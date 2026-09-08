//! Bounded observations for the populated canonical-Writer experiment.
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;

const BUCKETS: usize = 2048;
const RATIO: f64 = 1.02;

/// Fixed 2%-wide logarithmic nanosecond buckets, with zero included in the first bucket.
/// Quantiles are intervals, not exact samples.
#[derive(Clone)]
pub struct Histogram {
    bins: Box<[u64; BUCKETS]>,
    count: u64,
    sum_seconds: f64,
    max_seconds: f64,
    overflow: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            bins: Box::new([0; BUCKETS]),
            count: 0,
            sum_seconds: 0.0,
            max_seconds: 0.0,
            overflow: 0,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Distribution {
    pub count: u64,
    pub sum_seconds: f64,
    pub max_seconds: f64,
    pub p99_seconds_interval: Option<[f64; 2]>,
    pub bucket_relative_width: f64,
    pub overflow: u64,
}

impl Histogram {
    pub fn observe(&mut self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        let nanos = elapsed.as_nanos().max(1) as f64;
        let index = (nanos.ln() / RATIO.ln()).floor() as usize;
        if index >= BUCKETS {
            self.overflow += 1;
        } else {
            self.bins[index] += 1;
        }
        self.count += 1;
        self.sum_seconds += seconds;
        self.max_seconds = self.max_seconds.max(seconds);
    }

    pub fn merge(&mut self, other: &Self) {
        for (target, source) in self.bins.iter_mut().zip(other.bins.iter()) {
            *target += source;
        }
        self.count += other.count;
        self.sum_seconds += other.sum_seconds;
        self.max_seconds = self.max_seconds.max(other.max_seconds);
        self.overflow += other.overflow;
    }

    pub fn report(&self) -> Distribution {
        let p99_seconds_interval = if self.count >= 10_000 && self.overflow == 0 {
            let rank = (self.count * 99).div_ceil(100);
            let mut seen = 0;
            self.bins.iter().enumerate().find_map(|(index, count)| {
                seen += count;
                (seen >= rank).then(|| {
                    [
                        if index == 0 {
                            0.0
                        } else {
                            RATIO.powi(index as i32) * 1e-9
                        },
                        RATIO.powi(index as i32 + 1) * 1e-9,
                    ]
                })
            })
        } else {
            None
        };
        Distribution {
            count: self.count,
            sum_seconds: self.sum_seconds,
            max_seconds: self.max_seconds,
            p99_seconds_interval,
            bucket_relative_width: RATIO - 1.0,
            overflow: self.overflow,
        }
    }
}

#[derive(Default, Debug, Serialize)]
pub struct Stage {
    pub count: u64,
    pub sum_seconds: f64,
    pub max_seconds: f64,
    pub failed: u64,
}

#[derive(Default, Debug, Serialize)]
pub struct WriterObservation {
    pub stages: BTreeMap<&'static str, Stage>,
    pub dropped_at_start: u64,
    pub dropped_at_end: u64,
    pub queue_samples: u64,
    pub queue_nonempty_samples: u64,
    pub queue_max: usize,
    pub peak_wal_bytes: u64,
    pub checkpoint_attempts: u64,
    pub checkpoint_busy: u64,
    pub checkpoint_completed: u64,
}

impl WriterObservation {
    pub fn start(store: &cairn_meta::SqliteMetadataStore) -> Self {
        // The caller first drains and labels preparation. Boundary observations cannot be
        // silently thrown away and then presented as complete load service demand.
        Self {
            dropped_at_start: store.dropped_writer_stage_samples(),
            ..Self::default()
        }
    }

    pub fn drain(&mut self, store: &cairn_meta::SqliteMetadataStore) {
        for sample in store.drain_writer_stage_samples() {
            let stage = self.stages.entry(sample.stage).or_default();
            stage.count += 1;
            stage.sum_seconds += sample.seconds;
            stage.max_seconds = stage.max_seconds.max(sample.seconds);
            stage.failed += u64::from(!sample.success);
        }
        let depth = store.writer_queue_depth();
        self.queue_samples += 1;
        self.queue_nonempty_samples += u64::from(depth != 0);
        self.queue_max = self.queue_max.max(depth);
        self.dropped_at_end = store.dropped_writer_stage_samples();
    }

    pub fn serialized_observed_seconds(&self) -> f64 {
        ["begin", "apply", "commit"]
            .iter()
            .filter_map(|name| self.stages.get(name))
            .map(|stage| stage.sum_seconds)
            .sum()
    }

    pub fn complete(&self) -> bool {
        self.dropped_at_start == self.dropped_at_end
    }
}

/// The task's CPU counters identify actual Writer service independently from process CPU.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct WriterCpu {
    pub tid: u32,
    pub started_ticks: u64,
    pub user_ticks: u64,
    pub system_ticks: u64,
}

impl WriterCpu {
    pub fn discover() -> std::io::Result<Self> {
        let mut found = None;
        for entry in std::fs::read_dir("/proc/self/task")? {
            let entry = entry?;
            let name = std::fs::read_to_string(entry.path().join("comm"))?;
            if name.trim_end() == &"cairn-meta-writer"[..15] {
                if found.is_some() {
                    return Err(std::io::Error::other(
                        "capacity process has multiple Writer threads",
                    ));
                }
                let tid = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse().ok())
                    .ok_or_else(|| std::io::Error::other("invalid Writer task identity"))?;
                found = Some(Self::read(tid)?);
            }
        }
        found.ok_or_else(|| std::io::Error::other("canonical Writer task is unavailable"))
    }

    pub fn read(tid: u32) -> std::io::Result<Self> {
        let stat = std::fs::read_to_string(format!("/proc/self/task/{tid}/stat"))?;
        let fields: Vec<_> = stat
            .rsplit_once(')')
            .ok_or_else(|| std::io::Error::other("invalid task stat"))?
            .1
            .split_whitespace()
            .collect();
        let get = |index| {
            fields
                .get(index)
                .and_then(|value: &&str| value.parse::<u64>().ok())
                .ok_or_else(|| std::io::Error::other("missing task counter"))
        };
        Ok(Self {
            tid,
            started_ticks: get(19)?,
            user_ticks: get(11)?,
            system_ticks: get(12)?,
        })
    }

    pub fn elapsed_seconds(&self, later: &Self, ticks_per_second: u64) -> std::io::Result<f64> {
        if self.tid != later.tid
            || self.started_ticks != later.started_ticks
            || ticks_per_second == 0
        {
            return Err(std::io::Error::other(
                "Writer CPU samples do not identify one task",
            ));
        }
        let user = later.user_ticks.checked_sub(self.user_ticks);
        let system = later.system_ticks.checked_sub(self.system_ticks);
        let ticks = user
            .zip(system)
            .and_then(|(user, system)| user.checked_add(system))
            .ok_or_else(|| std::io::Error::other("Writer CPU counters moved backwards"))?;
        Ok(ticks as f64 / ticks_per_second as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_requires_success_count_and_reports_interval_containing_observation() {
        let mut histogram = Histogram::default();
        for _ in 0..9_999 {
            histogram.observe(Duration::from_micros(123));
        }
        assert!(histogram.report().p99_seconds_interval.is_none());
        histogram.observe(Duration::from_micros(123));
        let interval = histogram.report().p99_seconds_interval.unwrap();
        assert!(interval[0] <= 123e-6 && interval[1] >= 123e-6);
        assert!(interval[1] / interval[0] <= RATIO + 1e-12);
    }

    #[test]
    fn zero_duration_is_inside_the_first_quantile_interval() {
        let mut histogram = Histogram::default();
        for _ in 0..10_000 {
            histogram.observe(Duration::ZERO);
        }
        let interval = histogram.report().p99_seconds_interval.unwrap();
        assert_eq!(interval[0], 0.0);
        assert!(interval[1] > 0.0 && interval[1] <= 1.021e-9);
    }

    #[test]
    fn merge_preserves_counts_sum_tail_and_overflow_refuses_quantiles() {
        let mut first = Histogram::default();
        let mut second = Histogram::default();
        first.observe(Duration::from_secs(1));
        second.observe(Duration::from_secs(2));
        first.merge(&second);
        let report = first.report();
        assert_eq!(report.count, 2);
        assert_eq!(report.sum_seconds, 3.0);
        assert_eq!(report.max_seconds, 2.0);
        for _ in 0..10_000 {
            first.observe(Duration::MAX);
        }
        assert!(first.report().p99_seconds_interval.is_none());
        assert_eq!(first.report().overflow, 10_000);
    }

    #[test]
    fn admission_and_queue_wait_are_excluded_from_serialized_writer_occupancy() {
        let mut observed = WriterObservation::default();
        for (name, sum) in [
            ("admission", 900.0),
            ("queue", 800.0),
            ("begin", 1.0),
            ("apply", 2.0),
            ("commit", 3.0),
            ("checkpoint", 7.0),
        ] {
            observed.stages.insert(
                name,
                Stage {
                    sum_seconds: sum,
                    ..Stage::default()
                },
            );
        }
        assert_eq!(observed.serialized_observed_seconds(), 6.0);
        observed.dropped_at_end = 1;
        assert!(!observed.complete());
    }
}
