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

use cairn_replication::{
    BucketRoutedSink, ReplicationEngine, ReplicationOpts, SingleSink, SinkRouter, next_backoff,
    outbox_entry_for,
};

/// A router that resolves NO sink for any target — models a transient target-resolve failure
/// (audit #20): the stored target's sink could not be built this drain.
struct NoSinkRouter;
impl SinkRouter for NoSinkRouter {
    fn sink_for<'a>(&'a self, _target_arn: Option<&str>) -> Option<&'a dyn BucketRoutedSink> {
        None
    }
}

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
                encryption: None,
                content_length: None,
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
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path,
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId("owner".to_owned()),
        user_metadata: vec![("k".to_owned(), "v".to_owned())],
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
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
        None,
        due_at,
        0,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: vec![entry],
    })
    .await
    .unwrap();
    version
}

/// Commit an ObjectCreate version with an *explicit* version id (so the test controls per-key
/// ordering) plus a pending, due outbox entry, staging its blob.
async fn enqueue_versioned(
    meta: &InMemoryMetadataStore,
    blobs: &InMemoryBlobStore,
    entry_id: &str,
    key: &str,
    version: &VersionId,
    data: &'static [u8],
    due_at: Timestamp,
) {
    let (path, etag, size) = stage_blob(blobs, data).await;
    let row = version_row(
        key,
        version,
        Some(path),
        etag,
        size,
        false,
        ReplicationStatus::Pending,
        due_at,
    );
    let entry = outbox_entry_for(
        entry_id,
        bucket(),
        ObjectKey::parse(key).unwrap(),
        version.clone(),
        ReplicationOp::ObjectCreate,
        "rule-0",
        None,
        due_at,
        0,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: vec![entry],
    })
    .await
    .unwrap();
}

/// How many due Pending entries the outbox would hand a worker at `now` (test introspection
/// via the public trait surface).
async fn due_entries(meta: &InMemoryMetadataStore, now: Timestamp) -> Vec<OutboxEntry> {
    // Read-only probe of what is due — must not claim, or it would steal entries from the engine
    // run under test.
    meta.list_due_replication(1000, now).await.unwrap()
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
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(1_000);
    let now = clock.now();

    let version = put_with_outbox(&meta, &blobs, "e1", "obj/a", b"hello world", now, now).await;

    let report = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();

    assert_eq!(report.claimed, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(report.retried, 0);
    assert_eq!(report.failed, 0);

    // The fake sink recorded the Put intent with the right identity and size.
    let intents = router.0.intents();
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
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(2_000);
    let now = clock.now();

    let version = put_with_outbox(&meta, &blobs, "e2", "obj/b", b"payload", now, now).await;

    // First pass: the sink fails retryably.
    router.0.set_behavior(SinkBehavior::Retryable);
    let report = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.claimed, 1);
    assert_eq!(report.retried, 1);
    assert_eq!(report.completed, 0);
    assert!(router.0.intents().is_empty(), "no Put recorded on failure");

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
    router.0.set_behavior(SinkBehavior::Succeed);
    let report = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.claimed, 1);
    assert_eq!(report.completed, 1);

    assert_eq!(router.0.intents().len(), 1, "Put recorded on the retry");
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

/// Per-key replication ordering is preserved *across* drain batches (audit #9). Within one batch
/// the engine already sorts a key's versions and blocks on the first that does not complete; the
/// gap is a later version claimed in a *separate* batch, with no in-batch sibling to block on.
/// Here v1 fails retryably and backs off out of the due set, then v2 arrives as the only due
/// entry with a healthy sink — it must defer behind the still-pending v1 rather than ship ahead
/// of it, then both ship in order once v1 recovers.
#[tokio::test]
async fn later_version_defers_until_earlier_version_replicates() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(1_000);
    let now = clock.now();

    let key = "obj/ordered";
    // Deterministic, lexicographically time-ordered ids (v1 < v2), matching uuidv7 semantics.
    let v1 = VersionId::from_string("v1".into());
    let v2 = VersionId::from_string("v2".into());

    // v1 is enqueued and fails retryably, so its backoff pushes it out of the due set — landing
    // it in a different batch from v2.
    enqueue_versioned(&meta, &blobs, "e-v1", key, &v1, b"first", now).await;
    router.0.set_behavior(SinkBehavior::Retryable);
    let r = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(r.retried, 1);
    assert!(
        due_entries(&meta, now).await.is_empty(),
        "v1 backed off and is no longer due"
    );

    // v2 now arrives as the only due entry, with a *healthy* sink. Absent the cross-batch guard it
    // would ship immediately and reorder ahead of v1; with the guard it defers.
    enqueue_versioned(&meta, &blobs, "e-v2", key, &v2, b"second", now).await;
    router.0.set_behavior(SinkBehavior::Succeed);
    let r = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(r.deferred, 1, "v2 must defer behind the un-replicated v1");
    assert_eq!(r.completed, 0);
    assert!(
        router.0.intents().is_empty(),
        "v2 must not ship while v1 is still owed to the destination"
    );
    assert_eq!(
        version_status(&meta, key, &v2).await,
        Some(ReplicationStatus::Pending)
    );

    // Recovery: advance past v1's backoff and v2's claim lease, heal the sink, and drain. v1 ships
    // first; once it completes, v2 is free to ship — in order, exactly once each.
    clock.set(now.plus_secs(100_000));
    for _ in 0..6 {
        let r = engine()
            .run_once(&meta, &router, &blobs, &clock)
            .await
            .unwrap();
        if r.claimed == 0 {
            break;
        }
    }
    assert_eq!(
        version_status(&meta, key, &v1).await,
        Some(ReplicationStatus::Completed)
    );
    assert_eq!(
        version_status(&meta, key, &v2).await,
        Some(ReplicationStatus::Completed)
    );
    let intents = router.0.intents();
    assert_eq!(intents.len(), 2, "both versions shipped exactly once");
    match (&intents[0], &intents[1]) {
        (
            RecordedIntent::Put {
                version_id: first, ..
            },
            RecordedIntent::Put {
                version_id: second, ..
            },
        ) => {
            assert_eq!(first, &v1, "v1 ships first");
            assert_eq!(second, &v2, "v2 ships after v1 — order preserved");
        }
        other => panic!("expected two ordered Put intents, got {other:?}"),
    }
}

/// A target that resolves to no sink — modelling a transient target-resolve fault when building
/// the router — must NOT terminally fail the entry. It retries with backoff, bounded by the
/// attempt budget (audit #20).
#[tokio::test]
async fn unresolved_target_is_retried_not_terminally_failed() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let clock = TestClock::at_secs(5_000);
    let now = clock.now();
    let _ = put_with_outbox(&meta, &blobs, "e-nosink", "obj/x", b"data", now, now).await;

    let report = engine()
        .run_once(&meta, &NoSinkRouter, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.retried, 1, "an unresolved target retries");
    assert_eq!(report.failed, 0, "and is NOT terminally failed");

    // Rescheduled with backoff (pending in the future), not terminal.
    assert!(
        due_entries(&meta, now).await.is_empty(),
        "backed off, not due now"
    );
    let later = due_entries(&meta, now.plus_secs(10_000)).await;
    assert_eq!(later.len(), 1, "still pending for a later attempt");
    assert_eq!(later[0].status, ReplicationStatus::Pending);
    assert_eq!(later[0].attempts, 1);
}

#[tokio::test]
async fn exceeding_max_attempts_marks_failed() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(3_000);
    let now = clock.now();
    let eng = engine(); // max_attempts = 3

    let version = put_with_outbox(&meta, &blobs, "e3", "obj/c", b"data", now, now).await;
    router.0.set_behavior(SinkBehavior::Retryable);

    // Attempt 1 -> retry (attempts becomes 1).
    let r = eng.run_once(&meta, &router, &blobs, &clock).await.unwrap();
    assert_eq!(r.retried, 1);
    // Advance past backoff, attempt 2 -> retry (attempts becomes 2).
    clock.advance_secs(10_000);
    let r = eng.run_once(&meta, &router, &blobs, &clock).await.unwrap();
    assert_eq!(r.retried, 1);
    // Advance past backoff, attempt 3 -> attempts would become 3 == max_attempts: terminal.
    clock.advance_secs(10_000);
    let r = eng.run_once(&meta, &router, &blobs, &clock).await.unwrap();
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
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(4_000);
    let now = clock.now();

    let version = put_with_outbox(&meta, &blobs, "e4", "obj/d", b"data", now, now).await;
    router.0.set_behavior(SinkBehavior::Terminal);

    let report = engine()
        .run_once(&meta, &router, &blobs, &clock)
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
    let router = SingleSink(sink);
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
        None,
        now,
        0,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: vec![entry],
    })
    .await
    .unwrap();

    let report = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.completed, 1);

    let intents = router.0.intents();
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
    let router = SingleSink(sink);
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
        None,
        now,
        0,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: vec![entry],
    })
    .await
    .unwrap();

    let report = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.claimed, 1);
    assert_eq!(report.completed, 1, "the entry is drained...");
    // ...but the sink was never contacted: a replica is never re-replicated (loop prevention).
    assert!(router.0.intents().is_empty());

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
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(7_000);
    let now = clock.now();

    let version = put_with_outbox(&meta, &blobs, "e7", "obj/g", b"once", now, now).await;

    // First delivery succeeds.
    engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(router.0.intents().len(), 1);
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
        None,
        now,
        0,
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
        replication: vec![dup],
    })
    .await
    .unwrap();

    // The duplicate re-ships (at-least-once): per-target idempotency is the durable claim's job, not
    // a version-level skip, so under fan-out a second target is never starved. A re-ship to the same
    // target is harmless — the destination overwrites identical bytes (ARCH 20.4).
    let report = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.claimed, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(
        router.0.intents().len(),
        2,
        "a re-enqueued duplicate re-ships idempotently (overwrites identical bytes)"
    );
}

#[tokio::test]
async fn fan_out_ships_every_target_for_one_version() {
    // Regression for the 1→N fan-out bug: a single object version with one outbox entry per distinct
    // target must ship to ALL of them. The first target to complete stamps the version `Completed`;
    // a version-level skip (the old behaviour) would wrongly starve the rest.
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    // SingleSink returns the one fake sink for any ARN, so two distinct-ARN entries both land on it.
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(8_000);
    let now = clock.now();

    // One version; its put_with_outbox entry plus a second entry to a distinct target.
    let version = put_with_outbox(&meta, &blobs, "ef", "obj/fan", b"data", now, now).await;
    let to_y = outbox_entry_for(
        "fan-y",
        bucket(),
        ObjectKey::parse("obj/fan").unwrap(),
        version.clone(),
        ReplicationOp::ObjectCreate,
        "rule-y",
        Some("arn:Y".to_owned()),
        now,
        0,
    );
    let existing = meta
        .get_version(&bucket(), &ObjectKey::parse("obj/fan").unwrap(), &version)
        .await
        .unwrap()
        .unwrap();
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(existing),
        precondition: Precondition::default(),
        replication: vec![to_y],
    })
    .await
    .unwrap();

    engine()
        .run_until_idle(&meta, &router, &blobs, &clock, 10)
        .await
        .unwrap();
    assert_eq!(
        router.0.intents().len(),
        2,
        "both targets of a fanned-out version receive the object"
    );
}

#[tokio::test]
async fn per_key_ordering_defers_later_versions_when_earlier_one_stalls() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(8_000);
    let now = clock.now();

    // Two versions of the same key, both due. The first (older) version id sorts before the
    // second because uuid v7 is time-ordered; enqueue v1 then v2.
    let v1 = put_with_outbox(&meta, &blobs, "ord1", "same/key", b"v1", now, now).await;
    // Ensure a distinct, later version id.
    let v2 = put_with_outbox(&meta, &blobs, "ord2", "same/key", b"v2", now, now).await;
    assert!(v1.as_str() < v2.as_str(), "v7 ids are time-ordered");

    // The sink fails retryably, so v1 stalls; v2 must be deferred (not shipped out of order).
    router.0.set_behavior(SinkBehavior::Retryable);
    let report = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.claimed, 2);
    assert_eq!(report.retried, 1, "the earlier version is retried");
    assert_eq!(report.deferred, 1, "the later version is deferred");
    assert_eq!(report.completed, 0);
    assert!(router.0.intents().is_empty());

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
    let router = SingleSink(sink);
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
            None,
            now,
            0,
        );
        meta.submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: vec![entry],
        })
        .await
        .unwrap();
    }

    let total = engine()
        .run_until_idle(&meta, &router, &blobs, &clock, 8)
        .await
        .unwrap();
    assert_eq!(total.completed, 5);
    assert_eq!(router.0.intents().len(), 5);
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
