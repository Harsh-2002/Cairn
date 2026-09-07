//! Bounded, dependency-free samples mirrored by the server's existing metrics task.
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const CAPACITY: usize = 1024;

/// Fixed multipart completion stages; no bucket or object labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultipartStage {
    /// Waiting for the blob write permit.
    PermitWait,
    /// Staging creation, part reads, hashes, transforms, and final trailer writes.
    Assembly,
    /// Bucket creation, file sync, rename, and directory sync.
    Durability,
}

impl MultipartStage {
    /// Stable metrics label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PermitWait => "permit_wait",
            Self::Assembly => "assembly",
            Self::Durability => "durability",
        }
    }
}

/// One completed or interrupted stage, including errors and request cancellation.
#[derive(Debug, Clone, Copy)]
pub struct MultipartTiming {
    /// The operation being measured.
    pub stage: MultipartStage,
    /// Wall time spent in that stage, excluding later metadata commit.
    pub elapsed: Duration,
}

#[derive(Debug, Default)]
pub(crate) struct MultipartTimings {
    samples: Mutex<VecDeque<MultipartTiming>>,
    dropped: AtomicU64,
}

impl MultipartTimings {
    pub(crate) fn start(&self, stage: MultipartStage) -> StageTimer<'_> {
        StageTimer {
            timings: self,
            stage,
            start: Instant::now(),
        }
    }

    fn record(&self, sample: MultipartTiming) {
        let mut samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        if samples.len() == CAPACITY {
            samples.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        samples.push_back(sample);
    }

    pub(crate) fn drain(&self) -> Vec<MultipartTiming> {
        self.samples
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect()
    }

    pub(crate) fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(crate) struct StageTimer<'a> {
    timings: &'a MultipartTimings,
    stage: MultipartStage,
    start: Instant,
}

impl Drop for StageTimer<'_> {
    fn drop(&mut self) {
        self.timings.record(MultipartTiming {
            stage: self.stage,
            elapsed: self.start.elapsed(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paused_collection_is_bounded_and_reports_eviction() {
        let timings = MultipartTimings::default();
        for i in 0..CAPACITY + 7 {
            timings.record(MultipartTiming {
                stage: MultipartStage::Assembly,
                elapsed: Duration::from_millis(i as u64),
            });
        }
        let samples = timings.drain();
        assert_eq!(samples.len(), CAPACITY);
        assert_eq!(samples[0].elapsed, Duration::from_millis(7));
        assert_eq!(timings.dropped_total(), 7);
        assert!(timings.drain().is_empty());
    }

    #[tokio::test]
    async fn cancelling_a_stage_records_its_duration() {
        let timings = std::sync::Arc::new(MultipartTimings::default());
        let task_timings = timings.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _timer = task_timings.start(MultipartStage::Assembly);
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let samples = timings.drain();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].stage, MultipartStage::Assembly);
    }
}
