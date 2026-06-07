//! `cairn-replication` — the outbox-driven asynchronous bucket-replication engine (ARCH §20).
//!
//! Replication is eventually consistent with at-least-once delivery and idempotent
//! application. A durable outbox in the [`MetadataStore`] records what remains to be
//! replicated; this engine drains it. A pool of workers claims a batch of *due* entries,
//! and for each it loads the object version, streams its body from the [`BlobStore`], and
//! drives a [`ReplicationSink`] (an S3-compatible destination):
//!
//! * On success the entry is marked done and the version is stamped
//!   [`ReplicationStatus::Completed`].
//! * On a [`ReplicationError::Retryable`] failure the attempt count is bumped and the entry
//!   is re-scheduled with exponential [`next_backoff`] until `max_attempts` is reached, at
//!   which point it becomes terminal.
//! * On a [`ReplicationError::Terminal`] failure the entry is marked failed immediately with
//!   no further retry (`next_attempt_at = None`), surfaced for operator attention.
//!
//! Idempotency comes from the per-version identity: re-shipping a version overwrites the
//! destination with identical bytes, so a duplicate delivery is harmless. **Loop prevention:**
//! a version whose status is [`ReplicationStatus::Replica`] (it arrived here *via* replication)
//! is never re-replicated; such an entry is drained without contacting the sink. Per-key
//! ordering is preserved: a key's versions are processed oldest-first, and if an earlier
//! version of a key is left pending in a batch, later versions of that same key are skipped
//! until the next pass so writes never reorder at the destination.
//!
//! The engine is generic over the trait spine and is exercised entirely against the in-memory
//! doubles ([`FakeReplicationSink`], `InMemoryMetadataStore`, `InMemoryBlobStore`,
//! `TestClock`]) in the gate tests.
//!
//! [`FakeReplicationSink`]: cairn_types::testing::FakeReplicationSink

#![forbid(unsafe_code)]

mod backoff;
mod config;
mod route;
mod sink;

pub use backoff::next_backoff;
pub use config::{Destination, Filter, ReplicationConfig, ReplicationRule, parse_replication};
pub use route::BucketRoutedSink;
pub use sink::{HttpS3Sink, S3SinkConfig};

use std::collections::BTreeMap;
use std::sync::Arc;

use cairn_types::blob::ByteRange;
use cairn_types::error::{BlobError, MetaError, ReplicationError};
use cairn_types::id::{BucketName, ObjectKey, VersionId};
use cairn_types::meta::{Mutation, OutboxEntry, ReplicationOp, ReplicationStatus};
use cairn_types::object::ObjectVersionRow;
use cairn_types::replication::ReplicatedObject;
use cairn_types::time::Timestamp;
use cairn_types::traits::{BlobStore, Clock, MetadataStore};

/// Tunables governing a replication worker pass.
#[derive(Debug, Clone, Copy)]
pub struct ReplicationOpts {
    /// How many due outbox entries a single pass claims at once.
    pub batch_size: u32,
    /// The maximum number of attempts before a retryable failure becomes terminal.
    pub max_attempts: u32,
    /// The base backoff delay, in seconds, between retries.
    pub base_backoff_secs: u64,
    /// The ceiling on the (exponentially growing) backoff delay, in seconds.
    pub max_backoff_secs: u64,
}

impl Default for ReplicationOpts {
    fn default() -> Self {
        Self {
            batch_size: 64,
            max_attempts: 8,
            base_backoff_secs: 5,
            max_backoff_secs: 900,
        }
    }
}

/// A summary of what one [`ReplicationEngine::run_once`] pass did, for observability and to
/// let a run loop decide whether the queue was drained.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunReport {
    /// Entries claimed from the outbox in this pass.
    pub claimed: usize,
    /// Entries that replicated successfully (or were drained as already-done/replica).
    pub completed: usize,
    /// Entries re-scheduled after a retryable failure.
    pub retried: usize,
    /// Entries marked terminally failed (terminal error or attempts exhausted).
    pub failed: usize,
    /// Entries skipped this pass to preserve per-key ordering (an earlier version of the
    /// same key did not complete).
    pub deferred: usize,
}

impl RunReport {
    /// Whether this pass found and processed no work, so a run loop may back off/idle.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.claimed == 0
    }
}

/// The outbox-driven replication engine, generic over the metadata store, the source blob
/// store, the destination sink, and the clock.
///
/// It holds no mutable state of its own: the durable outbox is the source of truth, so an
/// engine is cheap to construct and safe to run from many workers concurrently.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReplicationEngine {
    opts: ReplicationOpts,
}

impl ReplicationEngine {
    /// Construct an engine with the given options.
    #[must_use]
    pub fn new(opts: ReplicationOpts) -> Self {
        Self { opts }
    }

    /// The options this engine runs with.
    #[must_use]
    pub fn opts(&self) -> ReplicationOpts {
        self.opts
    }

    /// Claim one batch of due outbox entries and drive each to the sink, honouring per-key
    /// ordering and loop prevention. Returns a [`RunReport`] of what happened.
    ///
    /// # Errors
    /// Returns a [`MetaError`] only if claiming the batch or submitting a status mutation to
    /// the metadata store fails; per-entry sink failures are recorded on the outbox (retried
    /// or failed) and never abort the pass.
    pub async fn run_once<M, S, B, C>(
        &self,
        meta: &M,
        sink: &S,
        blobs: &Arc<B>,
        clock: &C,
    ) -> Result<RunReport, MetaError>
    where
        M: MetadataStore + ?Sized,
        S: BucketRoutedSink + ?Sized,
        B: BlobStore + ?Sized,
        C: Clock + ?Sized,
    {
        let now = clock.now();
        let batch = meta
            .claim_replication_batch(self.opts.batch_size, now)
            .await?;

        let mut report = RunReport {
            claimed: batch.len(),
            ..RunReport::default()
        };

        // Group by key so a key's versions are processed strictly oldest-first, and so a
        // stalled earlier version blocks the later ones (per-key ordering). Version ids are
        // time-sortable (uuid v7), so ascending string order is chronological order.
        let mut by_key: BTreeMap<(String, String), Vec<OutboxEntry>> = BTreeMap::new();
        for entry in batch {
            by_key
                .entry((
                    entry.bucket.as_str().to_owned(),
                    entry.key.as_str().to_owned(),
                ))
                .or_default()
                .push(entry);
        }

        for entries in by_key.values_mut() {
            entries.sort_by(|a, b| a.version_id.as_str().cmp(b.version_id.as_str()));
            let mut blocked = false;
            for entry in entries.iter() {
                if blocked {
                    // An earlier version of this key did not complete this pass; defer the
                    // rest so writes never reorder at the destination.
                    report.deferred += 1;
                    continue;
                }
                match self.process_entry(meta, sink, blobs, now, entry).await? {
                    EntryOutcome::Completed => report.completed += 1,
                    EntryOutcome::Retried => {
                        report.retried += 1;
                        blocked = true;
                    }
                    EntryOutcome::Failed => {
                        report.failed += 1;
                        blocked = true;
                    }
                }
            }
        }

        Ok(report)
    }

    /// Drive the engine in a loop, draining the outbox a batch at a time until a pass finds
    /// no due work, then return the cumulative [`RunReport`]. This is the synchronous
    /// "catch up now" helper; a long-running worker wraps it with its own sleep/poll cadence
    /// using the clock.
    ///
    /// `max_passes` bounds the work so a perpetually-failing-and-retrying entry cannot spin
    /// forever within a single call (retries land in the future and are not re-claimed until
    /// the clock advances anyway).
    ///
    /// # Errors
    /// Propagates any [`MetaError`] from an underlying pass.
    pub async fn run_until_idle<M, S, B, C>(
        &self,
        meta: &M,
        sink: &S,
        blobs: &Arc<B>,
        clock: &C,
        max_passes: u32,
    ) -> Result<RunReport, MetaError>
    where
        M: MetadataStore + ?Sized,
        S: BucketRoutedSink + ?Sized,
        B: BlobStore + ?Sized,
        C: Clock + ?Sized,
    {
        let mut total = RunReport::default();
        for _ in 0..max_passes {
            let pass = self.run_once(meta, sink, blobs, clock).await?;
            total.claimed += pass.claimed;
            total.completed += pass.completed;
            total.retried += pass.retried;
            total.failed += pass.failed;
            total.deferred += pass.deferred;
            if pass.is_idle() {
                break;
            }
        }
        Ok(total)
    }

    /// Process exactly one outbox entry, contacting the sink and recording the result on the
    /// outbox. Returns how the entry resolved so the caller can preserve per-key ordering.
    async fn process_entry<M, S, B>(
        &self,
        meta: &M,
        sink: &S,
        blobs: &Arc<B>,
        now: Timestamp,
        entry: &OutboxEntry,
    ) -> Result<EntryOutcome, MetaError>
    where
        M: MetadataStore + ?Sized,
        S: BucketRoutedSink + ?Sized,
        B: BlobStore + ?Sized,
    {
        // Load the version this entry concerns.
        let row = meta
            .get_version(&entry.bucket, &entry.key, &entry.version_id)
            .await?;
        let Some(row) = row else {
            // The version was permanently deleted out from under us: nothing to ship and no
            // amount of retrying will bring it back. Terminate the entry.
            self.mark_failed(meta, entry, "object version no longer exists", None)
                .await?;
            return Ok(EntryOutcome::Failed);
        };

        // Loop prevention and idempotency drains: never re-replicate a replica, and treat an
        // already-completed version as a harmless duplicate — drain the entry without
        // re-contacting the sink.
        match row.replication_status {
            Some(ReplicationStatus::Replica) | Some(ReplicationStatus::Completed) => {
                meta.submit(Mutation::MarkReplicationDone(entry.id.clone()))
                    .await?;
                return Ok(EntryOutcome::Completed);
            }
            _ => {}
        }

        // Drive the sink for this operation. The source bucket (`entry.bucket`) is threaded
        // through so the sink can resolve the destination bucket per source bucket (per-rule
        // replication); a fixed single-destination sink ignores it.
        let sink_result = match entry.operation {
            ReplicationOp::ObjectCreate => self.put_object(sink, blobs, &row).await,
            ReplicationOp::DeleteMarker => {
                sink.delete_marker(&entry.bucket, &entry.key, &entry.version_id)
                    .await
            }
        };

        match sink_result {
            Ok(()) => {
                self.mark_done(meta, entry, &row).await?;
                Ok(EntryOutcome::Completed)
            }
            Err(ReplicationError::Retryable(msg)) => {
                // Exhausting the attempt budget turns a retryable failure terminal.
                if entry.attempts.saturating_add(1) >= self.opts.max_attempts {
                    self.mark_failed(meta, entry, &format!("max attempts exhausted: {msg}"), None)
                        .await?;
                    Ok(EntryOutcome::Failed)
                } else {
                    let delay = next_backoff(
                        entry.attempts.saturating_add(1),
                        self.opts.base_backoff_secs,
                        self.opts.max_backoff_secs,
                    );
                    let next = now.plus_secs(delay as i64);
                    self.mark_failed(meta, entry, &msg, Some(next)).await?;
                    Ok(EntryOutcome::Retried)
                }
            }
            Err(ReplicationError::Terminal(msg)) => {
                self.mark_failed(meta, entry, &msg, None).await?;
                Ok(EntryOutcome::Failed)
            }
        }
    }

    /// Open the source blob, assemble a [`ReplicatedObject`], and put it at the destination.
    async fn put_object<S, B>(
        &self,
        sink: &S,
        blobs: &Arc<B>,
        row: &ObjectVersionRow,
    ) -> Result<(), ReplicationError>
    where
        S: BucketRoutedSink + ?Sized,
        B: BlobStore + ?Sized,
    {
        let Some(path) = row.storage_path.as_ref() else {
            // An ObjectCreate entry must reference a blob; a row without one is malformed and
            // cannot be made to replicate, so it is terminal.
            return Err(ReplicationError::Terminal(
                "object-create entry has no source blob".to_owned(),
            ));
        };

        // Read the whole logical body. Opening the blob is local I/O: a failure here is
        // transient (the blob may be momentarily unavailable), so classify it retryable
        // unless the blob is genuinely gone.
        let range = Some(ByteRange {
            offset: 0,
            length: row.size_logical,
        });
        let handle = blobs.open(path, range).await.map_err(map_blob_err)?;

        let object = ReplicatedObject {
            key: row.key.clone(),
            version_id: row.version_id.clone(),
            content_type: row.content_type.clone(),
            user_metadata: row.user_metadata.clone(),
            etag: row.etag.clone(),
            size: row.size_logical,
            tags: Vec::new(),
            acl: row.acl.clone(),
            body: handle.body,
        };

        sink.put_object(&row.bucket, object).await
    }

    /// Mark the entry done and stamp the version [`ReplicationStatus::Completed`]. The
    /// version re-upsert carries `replication: None`, so it enqueues no new outbox entry and
    /// cannot cause a replication loop.
    async fn mark_done<M>(
        &self,
        meta: &M,
        entry: &OutboxEntry,
        row: &ObjectVersionRow,
    ) -> Result<(), MetaError>
    where
        M: MetadataStore + ?Sized,
    {
        meta.submit(Mutation::MarkReplicationDone(entry.id.clone()))
            .await?;
        stamp_version_status(meta, row, ReplicationStatus::Completed).await?;
        Ok(())
    }

    /// Record a failed delivery on the outbox (retry with `next_attempt_at`, or terminal when
    /// `None`). On a terminal failure the version is also stamped
    /// [`ReplicationStatus::Failed`] for operator visibility.
    async fn mark_failed<M>(
        &self,
        meta: &M,
        entry: &OutboxEntry,
        error: &str,
        next_attempt_at: Option<Timestamp>,
    ) -> Result<(), MetaError>
    where
        M: MetadataStore + ?Sized,
    {
        meta.submit(Mutation::MarkReplicationFailed {
            id: entry.id.clone(),
            error: error.to_owned(),
            next_attempt_at,
        })
        .await?;
        if next_attempt_at.is_none() {
            if let Some(row) = meta
                .get_version(&entry.bucket, &entry.key, &entry.version_id)
                .await?
            {
                stamp_version_status(meta, &row, ReplicationStatus::Failed).await?;
            }
        }
        Ok(())
    }
}

/// The disposition of a single processed entry, used to preserve per-key ordering.
enum EntryOutcome {
    Completed,
    Retried,
    Failed,
}

/// Re-upsert a version row with a new replication status, enqueueing no replication.
async fn stamp_version_status<M>(
    meta: &M,
    row: &ObjectVersionRow,
    status: ReplicationStatus,
) -> Result<(), MetaError>
where
    M: MetadataStore + ?Sized,
{
    // Idempotent: skip the write if the row already carries the target status.
    if row.replication_status == Some(status) {
        return Ok(());
    }
    let mut updated = row.clone();
    updated.replication_status = Some(status);
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(updated),
        precondition: cairn_types::meta::Precondition::default(),
        replication: None,
    })
    .await?;
    Ok(())
}

/// Classify a blob-store error opening the source body: a missing blob is terminal (it will
/// never reappear), everything else is transient and worth retrying.
fn map_blob_err(e: BlobError) -> ReplicationError {
    match e {
        BlobError::NotFound => ReplicationError::Terminal(format!("source blob missing: {e}")),
        other => ReplicationError::Retryable(format!("source blob read failed: {other}")),
    }
}

/// Build the replication outbox entry for a freshly-written version. A convenience for the
/// write path (and the tests' setup): callers attach the returned entry to the
/// [`Mutation::PutObjectVersion`] that commits the write, so the enqueue rides the same
/// transaction.
#[must_use]
pub fn outbox_entry_for(
    id: impl Into<String>,
    bucket: BucketName,
    key: ObjectKey,
    version_id: VersionId,
    operation: ReplicationOp,
    rule_id: impl Into<String>,
    due_at: Timestamp,
) -> OutboxEntry {
    OutboxEntry {
        id: id.into(),
        bucket,
        key,
        version_id,
        operation,
        rule_id: rule_id.into(),
        attempts: 0,
        next_attempt_at: due_at,
        status: ReplicationStatus::Pending,
        last_error: None,
    }
}
