//! Bounded response-path timing for sampled ordinary PUT requests.
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const CAPACITY: usize = 1024;
const SAMPLE_EVERY: u64 = 32;

/// Exclusive waits and total handler time for one sampled PUT population.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutStage {
    /// The complete PUT handler, including the exclusive stages below.
    Total,
    /// Bucket/config reads and synchronous request preparation before storage admission.
    Preflight,
    /// Preparing a storage plan and awaiting its durable Writer admission.
    StorageAdmission,
    /// Awaiting the blob store's stage operation.
    BlobStage,
    /// Validation, row construction and replication intent before publication.
    PublicationPrep,
    /// Awaiting the authoritative metadata publication mutation.
    Publication,
    /// Awaiting best-effort event notification after publication.
    Notification,
    /// Awaiting best-effort activity recording after notification.
    Audit,
}

impl PutStage {
    /// Fixed low-cardinality metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Total => "total",
            Self::Preflight => "preflight",
            Self::StorageAdmission => "storage_admission",
            Self::BlobStage => "blob_stage",
            Self::PublicationPrep => "publication_prep",
            Self::Publication => "publication",
            Self::Notification => "notification",
            Self::Audit => "audit",
        }
    }
}

/// One completed or interrupted sampled PUT stage. Durations are wall time.
#[derive(Debug, Clone, Copy)]
pub struct PutTiming {
    /// Fixed stage name.
    pub stage: PutStage,
    /// Wall duration including scheduler and I/O waits, not exclusive CPU time.
    pub elapsed: Duration,
    /// False if the stage returned an error or was cancelled before completion.
    pub completed: bool,
}

/// One relaxed atomic per PUT; only selected requests take clocks and a ring lock.
#[derive(Debug, Default)]
pub(crate) struct PutTimings {
    next: AtomicU64,
    samples: Mutex<VecDeque<PutTiming>>,
    dropped: AtomicU64,
}

impl PutTimings {
    pub(crate) fn sample(&self) -> bool {
        self.next.fetch_add(1, Ordering::Relaxed) % SAMPLE_EVERY == 0
    }

    pub(crate) fn start(&self, stage: PutStage) -> PutTimer<'_> {
        PutTimer {
            timings: self,
            stage,
            start: Instant::now(),
            completed: false,
        }
    }

    pub(crate) fn drain(&self) -> Vec<PutTiming> {
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

pub(crate) struct PutTimer<'a> {
    timings: &'a PutTimings,
    stage: PutStage,
    start: Instant,
    completed: bool,
}

impl PutTimer<'_> {
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for PutTimer<'_> {
    fn drop(&mut self) {
        let mut samples = self
            .timings
            .samples
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if samples.len() == CAPACITY {
            samples.pop_front();
            self.timings.dropped.fetch_add(1, Ordering::Relaxed);
        }
        samples.push_back(PutTiming {
            stage: self.stage,
            elapsed: self.start.elapsed(),
            completed: self.completed,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_selects_one_in_thirty_two() {
        let timings = PutTimings::default();
        let selected: Vec<_> = (0..96).filter(|_| timings.sample()).collect();
        assert_eq!(selected, [0, 32, 64]);
    }

    #[test]
    fn completion_and_interruption_are_distinct() {
        let timings = PutTimings::default();
        timings.start(PutStage::StorageAdmission).complete();
        drop(timings.start(PutStage::Publication));
        let samples = timings.drain();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].stage, PutStage::StorageAdmission);
        assert!(samples[0].completed);
        assert_eq!(samples[1].stage, PutStage::Publication);
        assert!(!samples[1].completed);
        assert!(timings.drain().is_empty());
    }

    #[test]
    fn bounded_ring_counts_evictions() {
        let timings = PutTimings::default();
        for _ in 0..CAPACITY + 3 {
            timings.start(PutStage::Total).complete();
        }
        assert_eq!(timings.dropped_total(), 3);
        assert_eq!(timings.drain().len(), CAPACITY);
    }
}
