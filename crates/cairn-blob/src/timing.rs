//! Bounded, dependency-free samples mirrored by the server's existing metrics task.
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const CAPACITY: usize = 1024;
const OBJECT_WRITE_SAMPLE_EVERY: u64 = 32;

/// Fixed ordinary-object write stages; metadata admission/publication are outside this layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectWriteStage {
    /// Waiting for a blob write permit.
    PermitWait,
    /// Anchored path preparation, including parent-directory durability fences.
    Namespace,
    /// Time a sampled namespace job waited before a blocking worker began it.
    NamespaceQueue,
    /// Time spent executing sampled anchored namespace preparation on that worker.
    NamespaceExecution,
    /// Parent-directory durability barrier during sampled namespace preparation, per call.
    NamespaceParentSync,
    /// Creating the admitted staging file and applying placement hints.
    StagingCreate,
    /// Consuming the body, hashing, transforming and writing it.
    Body,
    /// Waiting for the next plaintext body chunk, including end of stream.
    BodyInputWait,
    /// Updating/finalizing mandatory plaintext hashes on the raw path.
    BodyHash,
    /// Awaiting buffered raw-path writes; residual flushing is in Finalize.
    BodySink,
    /// Flushing, syncing the file and renaming it into the final directory.
    Finalize,
    /// Awaiting the residual buffered write flush before file sync.
    FinalizeFlush,
    /// Waiting for the retained blocking file-finalization job to start.
    FinalizeQueue,
    /// Trimming unused preallocation and syncing the staged file.
    FinalizeSync,
    /// Best-effort cache release after a successful file sync.
    FinalizeAdvice,
    /// Renaming the synced staged file into its admitted final name.
    FinalizeRename,
    /// Waiting for the coalesced destination-directory durability fence.
    DirectorySync,
}

impl ObjectWriteStage {
    /// Stable low-cardinality metrics label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PermitWait => "permit_wait",
            Self::Namespace => "namespace",
            Self::NamespaceQueue => "namespace_queue",
            Self::NamespaceExecution => "namespace_execution",
            Self::NamespaceParentSync => "namespace_parent_sync",
            Self::StagingCreate => "staging_create",
            Self::Body => "body",
            Self::BodyInputWait => "body_input_wait",
            Self::BodyHash => "body_hash",
            Self::BodySink => "body_sink",
            Self::Finalize => "finalize",
            Self::FinalizeFlush => "finalize_flush",
            Self::FinalizeQueue => "finalize_queue",
            Self::FinalizeSync => "finalize_sync",
            Self::FinalizeAdvice => "finalize_advice",
            Self::FinalizeRename => "finalize_rename",
            Self::DirectorySync => "directory_sync",
        }
    }
}

/// One sampled completed or interrupted ordinary-object blob stage.
#[derive(Debug, Clone, Copy)]
pub struct ObjectWriteTiming {
    /// The stage measured.
    pub stage: ObjectWriteStage,
    /// Wall time including scheduler delay and I/O waiting, not exclusive CPU time.
    pub elapsed: Duration,
    /// False when the future returned an error or was cancelled before the stage finished.
    pub completed: bool,
}

/// One relaxed atomic per ordinary object write; only selected requests take clocks/ring lock.
#[derive(Debug, Default)]
pub(crate) struct ObjectWriteTimings {
    next: AtomicU64,
    samples: Mutex<VecDeque<ObjectWriteTiming>>,
    dropped: AtomicU64,
}

impl ObjectWriteTimings {
    pub(crate) fn sample(&self) -> bool {
        self.next.fetch_add(1, Ordering::Relaxed) % OBJECT_WRITE_SAMPLE_EVERY == 0
    }

    pub(crate) fn start(&self, stage: ObjectWriteStage) -> ObjectWriteTimer<'_> {
        ObjectWriteTimer {
            timings: self,
            stage,
            start: Instant::now(),
            completed: false,
        }
    }

    pub(crate) fn raw_body_breakdown(&self) -> RawBodyBreakdown<'_> {
        RawBodyBreakdown {
            timings: self,
            totals: [Duration::ZERO; 3],
            seen: [false; 3],
            interrupted: [false; 3],
            active: None,
        }
    }

    fn record(&self, sample: ObjectWriteTiming) {
        let mut samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        if samples.len() == CAPACITY {
            samples.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        samples.push_back(sample);
    }

    pub(crate) fn record_elapsed(
        &self,
        stage: ObjectWriteStage,
        elapsed: Duration,
        completed: bool,
    ) {
        self.record(ObjectWriteTiming {
            stage,
            elapsed,
            completed,
        });
    }

    pub(crate) fn drain(&self) -> Vec<ObjectWriteTiming> {
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
pub(crate) struct ObjectWriteTimer<'a> {
    timings: &'a ObjectWriteTimings,
    stage: ObjectWriteStage,
    start: Instant,
    completed: bool,
}

impl ObjectWriteTimer<'_> {
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for ObjectWriteTimer<'_> {
    fn drop(&mut self) {
        self.timings.record(ObjectWriteTiming {
            stage: self.stage,
            elapsed: self.start.elapsed(),
            completed: self.completed,
        });
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RawBodyPhase {
    InputWait,
    Hash,
    Sink,
}

impl RawBodyPhase {
    const fn index(self) -> usize {
        self as usize
    }

    const fn stage(self) -> ObjectWriteStage {
        match self {
            Self::InputWait => ObjectWriteStage::BodyInputWait,
            Self::Hash => ObjectWriteStage::BodyHash,
            Self::Sink => ObjectWriteStage::BodySink,
        }
    }
}

/// At most three ring entries per sampled raw object, regardless of its chunk count.
#[derive(Debug)]
pub(crate) struct RawBodyBreakdown<'a> {
    timings: &'a ObjectWriteTimings,
    totals: [Duration; 3],
    seen: [bool; 3],
    interrupted: [bool; 3],
    active: Option<(RawBodyPhase, Instant)>,
}

impl RawBodyBreakdown<'_> {
    pub(crate) fn begin(&mut self, phase: RawBodyPhase) {
        debug_assert!(self.active.is_none());
        self.active = Some((phase, Instant::now()));
    }

    pub(crate) fn end(&mut self, completed: bool) {
        if let Some((phase, started)) = self.active.take() {
            let index = phase.index();
            self.seen[index] = true;
            self.totals[index] = self.totals[index].saturating_add(started.elapsed());
            self.interrupted[index] |= !completed;
        }
    }
}

impl Drop for RawBodyBreakdown<'_> {
    fn drop(&mut self) {
        self.end(false);
        for phase in [
            RawBodyPhase::InputWait,
            RawBodyPhase::Hash,
            RawBodyPhase::Sink,
        ] {
            let index = phase.index();
            if self.seen[index] {
                self.timings.record(ObjectWriteTiming {
                    stage: phase.stage(),
                    elapsed: self.totals[index],
                    completed: !self.interrupted[index],
                });
            }
        }
    }
}

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
    fn object_write_sampling_is_fixed_and_interrupted_stages_are_retained() {
        let timings = ObjectWriteTimings::default();
        for index in 0..=OBJECT_WRITE_SAMPLE_EVERY {
            assert_eq!(timings.sample(), index % OBJECT_WRITE_SAMPLE_EVERY == 0);
        }
        timings.start(ObjectWriteStage::Namespace).complete();
        drop(timings.start(ObjectWriteStage::Body));
        let samples = timings.drain();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].stage, ObjectWriteStage::Namespace);
        assert!(samples[0].completed);
        assert_eq!(samples[1].stage, ObjectWriteStage::Body);
        assert!(!samples[1].completed);
    }

    #[test]
    fn object_write_collection_is_bounded_and_reports_eviction() {
        let timings = ObjectWriteTimings::default();
        for _ in 0..CAPACITY + 3 {
            timings.start(ObjectWriteStage::Finalize).complete();
        }
        assert_eq!(timings.drain().len(), CAPACITY);
        assert_eq!(timings.dropped_total(), 3);
    }

    #[test]
    fn raw_body_breakdown_aggregates_chunks_and_marks_only_active_cancelled_phase() {
        let timings = ObjectWriteTimings::default();
        {
            let mut detail = timings.raw_body_breakdown();
            for _ in 0..3 {
                detail.begin(RawBodyPhase::InputWait);
                detail.end(true);
                detail.begin(RawBodyPhase::Hash);
                detail.end(true);
            }
            detail.begin(RawBodyPhase::Sink);
        }
        let samples = timings.drain();
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].stage, ObjectWriteStage::BodyInputWait);
        assert!(samples[0].completed);
        assert_eq!(samples[1].stage, ObjectWriteStage::BodyHash);
        assert!(samples[1].completed);
        assert_eq!(samples[2].stage, ObjectWriteStage::BodySink);
        assert!(!samples[2].completed);
    }

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
