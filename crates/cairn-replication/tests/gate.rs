//! Gate tests for the outbox-driven replication engine, run entirely against the in-memory
//! doubles + a controllable clock.

use std::sync::Arc;

use bytes::Bytes;
use cairn_types::blob::StageOptions;
use cairn_types::error::BodyError;
use cairn_types::id::{BucketName, ObjectKey, StoragePath, UserId, VersionId};
use cairn_types::meta::{Mutation, OutboxEntry, Precondition, ReplicationOp, ReplicationStatus};
use cairn_types::object::{
    ChecksumSet, CompressionDescriptor, ETag, ObjectVersionRow, StorageClass,
};
use cairn_types::testing::{
    FakeReplicationSink, InMemoryBlobStore, InMemoryMetadataStore, RecordedIntent, SinkBehavior,
    TestClock,
};
use cairn_types::time::Timestamp;
use cairn_types::traits::{BlobStore, Clock, MetadataStore};

use cairn_replication::{ReplicationEngine, ReplicationOpts, next_backoff, outbox_entry_for};

const BUCKET: &str = "repl-bucket";

fn bucket() -> BucketName {
    BucketName::parse(BUCKET).unwrap()
}

fn body(bytes: &'static [u8]) -> cairn_types::BodyStream {
    Box::pin(futures_util::stream::once(async move {
        Ok::<Bytes, BodyError>(Bytes::from_static(bytes))
    }))
}

/// Stage `data` as a blob and return its storage path so a version row can reference it.
async fn stage_blob(blobs: &InMemoryBlobStore, data: &'static [u8]) -> (StoragePath, ETag, u64) {
    let staged = blobs
        .stage(
            &bucket(),
            body(data),
            StageOptions {
                compression: None,
                extra_checksums: ChecksumSet::none(),
                size_ceiling: 1 << 30,
                content_type: "text/plain".to_owned(),
            },
        )
        .await
        .unwrap();
    (staged.storage_path, staged.etag, staged.size_logical)
}

#[allow(clippy::too_many_arguments)]
fn version_row(
    key: &str,
    version: &VersionId,
    storage_path: Option<StoragePath>,
    etag: ETag,
    size: u64,
    is_delete_marker: bool,
    status: ReplicationStatus,
    now: Timestamp,
) -> ObjectVersionRow {
    ObjectVersionRow {
        id: format!("row-{}-{}", key, version.as_str()),
        bucket: bucket(),
        key: ObjectKey::parse(key).unwrap(),
        version_id: version.clone(),
        is_latest: true,
        is_delete_marker,
        size_logical: size,
        size_physical: size,
        etag,
        content_type: "text/plain".to_owned(),
        storage_path,
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId("owner".to_owned()),
        user_metadata: vec![("k".to_owned(), "v".to_owned())],
        acl: None,
        checksums: Vec::new(),
        replication_status: Some(status),
        created_at: now,
        updated_at: now,
    }
}

/// Commit an ObjectCreate version with a pending, due outbox entry, staging its blob.
async fn put_with_outbox(
    meta: &InMemoryMetadataStore,
    blobs: &InMemoryBlobStore,
    entry_id: &str,
    key: &str,
    data: &'static [u8],
    due_at: Timestamp,
    now: Timestamp,
) -> VersionId {
    let (path, etag, size) = stage_blob(blobs, data).await;
    let version = VersionId::generate();
    let row = version_row(
        key,
        &version,
        Some(path),
        etag,
        size,
        false,
        ReplicationStatus::Pending,
        now,
    );
    let entry = outbox_entry_for(
        entry_id,
        bucket(),
        ObjectKey::parse(key).unwrap(),
        version.clone(),
        ReplicationOp::ObjectCreate,
        "rule-0",
        due_at,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: Some(entry),
    })
    .await
    .unwrap();
    version
}

/// How many due Pending entries the outbox would hand a worker at `now` (test introspection
/// via the public trait surface).
async fn due_entries(meta: &InMemoryMetadataStore, now: Timestamp) -> Vec<OutboxEntry> {
    meta.claim_replication_batch(1000, now).await.unwrap()
}

async fn version_status(
    meta: &InMemoryMetadataStore,
    key: &str,
    version: &VersionId,
) -> Option<ReplicationStatus> {
    meta.object_replication_status(&bucket(), &ObjectKey::parse(key).unwrap(), version)
        .await
        .unwrap()
}

fn engine() -> ReplicationEngine {
    ReplicationEngine::new(ReplicationOpts {
        batch_size: 64,
        max_attempts: 3,
        base_backoff_secs: 10,
        max_backoff_secs: 100,
    })
}

// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn object_create_replicates_and_completes() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let clock = TestClock::at_secs(1_000);
    let now = clock.now();

    let version = put_with_outbox(&meta, &blobs, "e1", "obj/a", b"hello world", now, now).await;

    let report = engine()
        .run_once(&meta, &sink, &blobs, &clock)
        .await
        .unwrap();

    assert_eq!(report.claimed, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(report.retried, 0);
    assert_eq!(report.failed, 0);

    // The fake sink recorded the Put intent with the right identity and size.
    let intents = sink.intents();
    assert_eq!(intents.len(), 1);
    match &intents[0] {
        RecordedIntent::Put {
            key,
            version_id,
            size,
        } => {
            assert_eq!(key.as_str(), "obj/a");
            assert_eq!(version_id, &version);
            assert_eq!(*size, b"hello world".len() as u64);
        }
        other => panic!("expected a Put intent, got {other:?}"),
    }

    // The outbox entry is marked done (no longer claimable, even far in the future).
    assert!(
        due_entries(&meta, now.plus_secs(1_000_000))
            .await
            .is_empty()
    );

    // The version's replication status is stamped Completed.
    assert_eq!(
        version_status(&meta, "obj/a", &version).await,
        Some(ReplicationStatus::Completed)
    );
}

#[tokio::test]
async fn retryable_failure_reschedules_then_succeeds_after_clock_advance() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let clock = TestClock::at_secs(2_000);
    let now = clock.now();

    let version = put_with_outbox(&meta, &blobs, "e2", "obj/b", b"payload", now, now).await;

    // First pass: the sink fails retryably.
    sink.set_behavior(SinkBehavior::Retryable);
    let report = engine()
        .run_once(&meta, &sink, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.claimed, 1);
    assert_eq!(report.retried, 1);
    assert_eq!(report.completed, 0);
    assert!(sink.intents().is_empty(), "no Put recorded on failure");

    // The entry is no longer due *now* (backoff pushed next_attempt_at into the future) but
    // is still pending, with the attempt count incremented.
    assert!(
        due_entries(&meta, now).await.is_empty(),
        "entry must not be due immediately after a retryable failure"
    );
    let future = now.plus_secs(10_000);
    let pending = due_entries(&meta, future).await;
    assert_eq!(pending.len(), 1, "entry is still pending in the future");
    assert_eq!(pending[0].attempts, 1, "attempt count incremented");
    assert!(
        pending[0].next_attempt_at > now,
        "next_attempt_at moved into the future"
    );
    assert!(pending[0].last_error.is_some());
    assert_eq!(pending[0].status, ReplicationStatus::Pending);

    // The version is not yet completed.
    assert_eq!(
        version_status(&meta, "obj/b", &version).await,
        Some(ReplicationStatus::Pending)
    );

    // Advance the clock past the backoff and let the sink succeed: the retry replicates.
    clock.set(future);
    sink.set_behavior(SinkBehavior::Succeed);
    let report = engine()
        .run_once(&meta, &sink, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.claimed, 1);
    assert_eq!(report.completed, 1);

    assert_eq!(sink.intents().len(), 1, "Put recorded on the retry");
    assert_eq!(
        version_status(&meta, "obj/b", &version).await,
        Some(ReplicationStatus::Completed)
    );
    assert!(
        due_entries(&meta, future.plus_secs(1_000_000))
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn exceeding_max_attempts_marks_failed() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let clock = TestClock::at_secs(3_000);
    let now = clock.now();
    let eng = engine(); // max_attempts = 3

    let version = put_with_outbox(&meta, &blobs, "e3", "obj/c", b"data", now, now).await;
    sink.set_behavior(SinkBehavior::Retryable);

    // Attempt 1 -> retry (attempts becomes 1).
    let r = eng.run_once(&meta, &sink, &blobs, &clock).await.unwrap();
    assert_eq!(r.retried, 1);
    // Advance past backoff, attempt 2 -> retry (attempts becomes 2).
    clock.advance_secs(10_000);
    let r = eng.run_once(&meta, &sink, &blobs, &clock).await.unwrap();
    assert_eq!(r.retried, 1);
    // Advance past backoff, attempt 3 -> attempts would become 3 == max_attempts: terminal.
    clock.advance_secs(10_000);
    let r = eng.run_once(&meta, &sink, &blobs, &clock).await.unwrap();
    assert_eq!(r.failed, 1);
    assert_eq!(r.retried, 0);

    // The entry is terminal: never claimable again, regardless of how far the clock advances.
    let far = clock.now().plus_secs(10_000_000);
    assert!(due_entries(&meta, far).await.is_empty());
    // And the version is stamped Failed.
    assert_eq!(
        version_status(&meta, "obj/c", &version).await,
        Some(ReplicationStatus::Failed)
    );
}

#[tokio::test]
async fn terminal_failure_marks_failed_immediately() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let clock = TestClock::at_secs(4_000);
    let now = clock.now();

    let version = put_with_outbox(&meta, &blobs, "e4", "obj/d", b"data", now, now).await;
    sink.set_behavior(SinkBehavior::Terminal);

    let report = engine()
        .run_once(&meta, &sink, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.failed, 1);
    assert_eq!(report.retried, 0);

    // Terminal on the first attempt: no retry scheduled, version Failed.
    let far = now.plus_secs(10_000_000);
    assert!(due_entries(&meta, far).await.is_empty());
    assert_eq!(
        version_status(&meta, "obj/d", &version).await,
        Some(ReplicationStatus::Failed)
    );
}

#[tokio::test]
async fn delete_marker_entry_drives_sink_delete_marker() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let clock = TestClock::at_secs(5_000);
    let now = clock.now();

    // Commit a delete-marker version (no blob) plus a delete-marker outbox entry.
    let version = VersionId::generate();
    let row = version_row(
        "obj/e",
        &version,
        None,
        ETag::from_string(String::new()),
        0,
        true,
        ReplicationStatus::Pending,
        now,
    );
    let entry = outbox_entry_for(
        "e5",
        bucket(),
        ObjectKey::parse("obj/e").unwrap(),
        version.clone(),
        ReplicationOp::DeleteMarker,
        "rule-0",
        now,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: Some(entry),
    })
    .await
    .unwrap();

    let report = engine()
        .run_once(&meta, &sink, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.completed, 1);

    let intents = sink.intents();
    assert_eq!(intents.len(), 1);
    match &intents[0] {
        RecordedIntent::DeleteMarker { key, version_id } => {
            assert_eq!(key.as_str(), "obj/e");
            assert_eq!(version_id, &version);
        }
        other => panic!("expected a DeleteMarker intent, got {other:?}"),
    }

    assert_eq!(
        version_status(&meta, "obj/e", &version).await,
        Some(ReplicationStatus::Completed)
    );
}

#[tokio::test]
async fn replica_status_is_never_re_replicated() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let clock = TestClock::at_secs(6_000);
    let now = clock.now();

    // A version that arrived via replication is marked Replica; enqueue an entry for it to
    // simulate a misfire / loop attempt.
    let (path, etag, size) = stage_blob(&blobs, b"replica-bytes").await;
    let version = VersionId::generate();
    let row = version_row(
        "obj/f",
        &version,
        Some(path),
        etag,
        size,
        false,
        ReplicationStatus::Replica,
        now,
    );
    let entry = outbox_entry_for(
        "e6",
        bucket(),
        ObjectKey::parse("obj/f").unwrap(),
        version.clone(),
        ReplicationOp::ObjectCreate,
        "rule-0",
        now,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: Some(entry),
    })
    .await
    .unwrap();

    let report = engine()
        .run_once(&meta, &sink, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.claimed, 1);
    assert_eq!(report.completed, 1, "the entry is drained...");
    // ...but the sink was never contacted: a replica is never re-replicated (loop prevention).
    assert!(sink.intents().is_empty());

    // The version stays a Replica (we did not overwrite its status), and the entry is drained.
    assert_eq!(
        version_status(&meta, "obj/f", &version).await,
        Some(ReplicationStatus::Replica)
    );
    assert!(
        due_entries(&meta, now.plus_secs(1_000_000))
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn redelivering_completed_version_is_idempotent() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let clock = TestClock::at_secs(7_000);
    let now = clock.now();

    let version = put_with_outbox(&meta, &blobs, "e7", "obj/g", b"once", now, now).await;

    // First delivery succeeds.
    engine()
        .run_once(&meta, &sink, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(sink.intents().len(), 1);
    assert_eq!(
        version_status(&meta, "obj/g", &version).await,
        Some(ReplicationStatus::Completed)
    );

    // Re-enqueue a *new* pending entry for the same (already completed) version, simulating a
    // duplicate delivery from at-least-once semantics.
    let dup = outbox_entry_for(
        "e7-dup",
        bucket(),
        ObjectKey::parse("obj/g").unwrap(),
        version.clone(),
        ReplicationOp::ObjectCreate,
        "rule-0",
        now,
    );
    // Push the duplicate through the outbox by attaching it to a no-op re-put of the (now
    // Completed) row so the entry lands due.
    let existing = meta
        .get_version(&bucket(), &ObjectKey::parse("obj/g").unwrap(), &version)
        .await
        .unwrap()
        .unwrap();
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(existing),
        precondition: Precondition::default(),
        replication: Some(dup),
    })
    .await
    .unwrap();

    // The duplicate is drained without a second Put: idempotent / harmless.
    let report = engine()
        .run_once(&meta, &sink, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.claimed, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(
        sink.intents().len(),
        1,
        "completed version is not shipped twice"
    );
}

#[tokio::test]
async fn per_key_ordering_defers_later_versions_when_earlier_one_stalls() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let clock = TestClock::at_secs(8_000);
    let now = clock.now();

    // Two versions of the same key, both due. The first (older) version id sorts before the
    // second because uuid v7 is time-ordered; enqueue v1 then v2.
    let v1 = put_with_outbox(&meta, &blobs, "ord1", "same/key", b"v1", now, now).await;
    // Ensure a distinct, later version id.
    let v2 = put_with_outbox(&meta, &blobs, "ord2", "same/key", b"v2", now, now).await;
    assert!(v1.as_str() < v2.as_str(), "v7 ids are time-ordered");

    // The sink fails retryably, so v1 stalls; v2 must be deferred (not shipped out of order).
    sink.set_behavior(SinkBehavior::Retryable);
    let report = engine()
        .run_once(&meta, &sink, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.claimed, 2);
    assert_eq!(report.retried, 1, "the earlier version is retried");
    assert_eq!(report.deferred, 1, "the later version is deferred");
    assert_eq!(report.completed, 0);
    assert!(sink.intents().is_empty());

    // v2 is still pending and was never shipped ahead of v1.
    let future = now.plus_secs(10_000);
    let pending = due_entries(&meta, future).await;
    let v2_attempts = pending
        .iter()
        .find(|e| e.version_id == v2)
        .map(|e| e.attempts);
    assert_eq!(v2_attempts, Some(0), "deferred entry untouched");
}

#[tokio::test]
async fn run_until_idle_drains_independent_keys() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let clock = TestClock::at_secs(9_000);
    let now = clock.now();

    for i in 0..5 {
        let id = format!("multi-{i}");
        let key = format!("k/{i}");
        // Distinct keys so there is no cross-key ordering constraint.
        let (path, etag, size) = stage_blob(&blobs, b"x").await;
        let version = VersionId::generate();
        let row = version_row(
            &key,
            &version,
            Some(path),
            etag,
            size,
            false,
            ReplicationStatus::Pending,
            now,
        );
        let entry = outbox_entry_for(
            id,
            bucket(),
            ObjectKey::parse(&key).unwrap(),
            version,
            ReplicationOp::ObjectCreate,
            "rule-0",
            now,
        );
        meta.submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: Some(entry),
        })
        .await
        .unwrap();
    }

    let total = engine()
        .run_until_idle(&meta, &sink, &blobs, &clock, 8)
        .await
        .unwrap();
    assert_eq!(total.completed, 5);
    assert_eq!(sink.intents().len(), 5);
    assert!(
        due_entries(&meta, now.plus_secs(1_000_000))
            .await
            .is_empty()
    );
}

#[test]
fn next_backoff_helper_is_exponential_and_capped() {
    // Exposed helper: deterministic exponential growth, capped.
    assert_eq!(next_backoff(0, 5, 100), 5);
    assert_eq!(next_backoff(1, 5, 100), 5);
    assert_eq!(next_backoff(2, 5, 100), 10);
    assert_eq!(next_backoff(3, 5, 100), 20);
    assert_eq!(next_backoff(4, 5, 100), 40);
    assert_eq!(next_backoff(5, 5, 100), 80);
    assert_eq!(next_backoff(6, 5, 100), 100); // capped (160 -> 100)
    assert_eq!(next_backoff(100, 5, 100), 100);
}
