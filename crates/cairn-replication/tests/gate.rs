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
    StubCrypto, TestClock,
};
use cairn_types::time::Timestamp;
use cairn_types::traits::{BlobStore, Clock, Crypto, MetadataStore};

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
    engine_with_crypto(Arc::new(StubCrypto))
}

fn engine_with_crypto(crypto: Arc<dyn Crypto>) -> ReplicationEngine {
    ReplicationEngine::new(
        ReplicationOpts {
            batch_size: 64,
            max_attempts: 3,
            base_backoff_secs: 10,
            max_backoff_secs: 100,
        },
        crypto,
    )
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

/// A target that is *unavailable* (transport error / 5xx) must keep its queued work indefinitely
/// WITHOUT consuming the attempt budget, so an extended outage never turns an owed object terminal
/// and the queue auto-resumes once the target returns. This is the crash/outage-recovery contract.
#[tokio::test]
async fn unavailable_target_retries_without_consuming_budget_then_resumes() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(2_000);
    let now = clock.now();
    let eng = engine(); // max_attempts = 3

    let version = put_with_outbox(&meta, &blobs, "e-down", "obj/down", b"data", now, now).await;
    router.0.set_behavior(SinkBehavior::Unavailable);

    // Drain far more times than max_attempts: the target is down on every pass, advancing the clock
    // past each unavailable re-check. NOT ONE attempt is consumed, so it never goes terminal.
    for _ in 0..8 {
        let r = eng.run_once(&meta, &router, &blobs, &clock).await.unwrap();
        assert_eq!(r.retried, 1, "an unavailable target reschedules");
        assert_eq!(r.failed, 0, "and is NEVER terminally failed");
        clock.advance_secs(60); // past UNAVAILABLE_RETRY_SECS so it is due again
    }
    // Still pending with a zero attempt count, and the version is NOT stamped Failed.
    let pending = due_entries(&meta, clock.now().plus_secs(60)).await;
    assert_eq!(pending.len(), 1, "the entry is still owed (pending)");
    assert_eq!(pending[0].attempts, 0, "no attempt was ever consumed");
    assert_eq!(
        version_status(&meta, "obj/down", &version).await,
        Some(ReplicationStatus::Pending),
    );

    // The target returns: the very next drain ships the backlog.
    router.0.set_behavior(SinkBehavior::Succeed);
    clock.advance_secs(60);
    let r = eng.run_once(&meta, &router, &blobs, &clock).await.unwrap();
    assert_eq!(
        r.completed, 1,
        "the queue auto-resumes when the target returns"
    );
    assert_eq!(
        version_status(&meta, "obj/down", &version).await,
        Some(ReplicationStatus::Completed),
    );
    assert_eq!(router.0.intents().len(), 1, "shipped exactly once");
}

/// A *terminally-failed* predecessor must NOT freeze newer versions of the same key+target forever.
/// Once v1 is settled `failed`, a later v2 is free to ship (best-effort / at-least-once, ARCH 20.4)
/// — the alternative (a silent permanent head-of-line stall) is worse.
#[tokio::test]
async fn terminally_failed_predecessor_does_not_block_successor() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(1_500);
    let now = clock.now();

    let key = "obj/stalled";
    let v1 = VersionId::from_string("v1".into());
    let v2 = VersionId::from_string("v2".into());

    // v1 fails terminally on the first attempt.
    enqueue_versioned(&meta, &blobs, "s-v1", key, &v1, b"first", now).await;
    router.0.set_behavior(SinkBehavior::Terminal);
    let r = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(r.failed, 1);
    assert_eq!(
        version_status(&meta, key, &v1).await,
        Some(ReplicationStatus::Failed)
    );

    // v2 arrives with a healthy sink: it must ship despite v1 being terminally failed.
    enqueue_versioned(&meta, &blobs, "s-v2", key, &v2, b"second", now).await;
    router.0.set_behavior(SinkBehavior::Succeed);
    let r = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(r.completed, 1, "v2 is not blocked by the terminal v1");
    assert_eq!(r.deferred, 0);
    assert_eq!(
        version_status(&meta, key, &v2).await,
        Some(ReplicationStatus::Completed)
    );
    let intents = router.0.intents();
    assert_eq!(
        intents.len(),
        1,
        "only v2 shipped (v1 stays terminally failed)"
    );
}

/// A successor deferred to preserve per-key ordering must *release its claim* (back to pending,
/// short re-check) rather than sit under the 300 s lease — so it ships within seconds of its
/// predecessor clearing, not minutes later. Proves the event-driven drain is not defeated.
#[tokio::test]
async fn deferred_successor_releases_claim_for_prompt_recheck() {
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(1_000);
    let now = clock.now();

    let key = "obj/defer";
    let v1 = VersionId::from_string("v1".into());
    let v2 = VersionId::from_string("v2".into());

    // v1 backs off into a future batch; v2 then arrives as the only due entry and must defer.
    enqueue_versioned(&meta, &blobs, "d-v1", key, &v1, b"first", now).await;
    router.0.set_behavior(SinkBehavior::Retryable);
    engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();

    enqueue_versioned(&meta, &blobs, "d-v2", key, &v2, b"second", now).await;
    router.0.set_behavior(SinkBehavior::Succeed);
    let r = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(r.deferred, 1, "v2 defers behind the un-replicated v1");

    // The claim was RELEASED: v2 is pending and becomes due again within a couple of seconds (the
    // ordering re-check), not locked under the multi-minute claim lease. Without the release it
    // would be `claimed` and absent from the due (pending) set here.
    let soon = due_entries(&meta, now.plus_secs(2)).await;
    assert!(
        soon.iter().any(|e| e.id == "d-v2"),
        "the deferred v2 is promptly re-claimable (claim released), got {soon:?}"
    );
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

#[tokio::test]
async fn completing_replication_does_not_demote_a_newer_version() {
    // Audit 2026-07: marking v1's replication done must NOT re-upsert v1's (pre-ship) row, which
    // would force is_latest and demote a v2 written during the ship window. mark_done now only stamps
    // the version's replication_status via a targeted update — never a whole-row PutObjectVersion.
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = FakeReplicationSink::new();
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(1_000);
    let now = clock.now();
    let key = ObjectKey::parse("k").unwrap();

    // v1: pending replication, latest at commit.
    let v1 = put_with_outbox(&meta, &blobs, "e1", "k", b"one", now, now).await;
    // v2: a newer version written with NO outbox entry — becomes latest, demoting v1. Stands in for a
    // client write landing during v1's ship window.
    let (path, etag, size) = stage_blob(&blobs, b"two").await;
    let v2 = VersionId::generate();
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(version_row(
            "k",
            &v2,
            Some(path),
            etag,
            size,
            false,
            ReplicationStatus::Completed,
            now,
        )),
        precondition: Precondition::default(),
        replication: vec![],
    })
    .await
    .unwrap();
    let before = meta.current_version(&bucket(), &key).await.unwrap();
    assert_eq!(
        before.map(|r| r.version_id),
        Some(v2.clone()),
        "v2 is latest before replication runs"
    );

    // Ship v1 and mark it done.
    engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();

    // v2 must STILL be latest — replication completion must not demote it.
    let after = meta.current_version(&bucket(), &key).await.unwrap();
    assert_eq!(
        after.map(|r| r.version_id),
        Some(v2),
        "a newer version must not be demoted by replication completion"
    );
    // v1's replication status is stamped Completed (targeted update).
    assert_eq!(
        version_status(&meta, "k", &v1).await,
        Some(ReplicationStatus::Completed)
    );
}

// --- encrypted source versions ------------------------------------------------------------
//
// Every test above this line ships a version whose `sse_descriptor` is `None`. That omission is
// exactly why the engine shipped raw ciphertext to mirrors for as long as it did: the doubles-based
// gate never exercised the encrypted arm at all.

/// A sink that keeps the bytes it was handed, so a test can assert the replica received the
/// PLAINTEXT rather than the stored ciphertext.
#[derive(Default)]
struct CapturingSink {
    bodies: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
    /// The `client_encrypted` classification each shipped object carried, so a test can pin that
    /// the sink is actually TOLD whether the plaintext it is about to put on the wire was
    /// encrypted at the client's request.
    client_encrypted: std::sync::Mutex<Vec<bool>>,
}

impl CapturingSink {
    fn bodies(&self) -> Vec<(String, Vec<u8>)> {
        self.bodies.lock().unwrap().clone()
    }

    fn client_encrypted_flags(&self) -> Vec<bool> {
        self.client_encrypted.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl cairn_types::traits::ReplicationSink for CapturingSink {
    async fn put_object(
        &self,
        object: cairn_types::replication::ReplicatedObject,
    ) -> Result<(), cairn_types::error::ReplicationError> {
        use futures_util::StreamExt;
        let key = object.key.as_str().to_owned();
        let mut body = object.body;
        let mut out = Vec::new();
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(
                &chunk
                    .map_err(|e| cairn_types::error::ReplicationError::Retryable(e.to_string()))?,
            );
        }
        self.client_encrypted
            .lock()
            .unwrap()
            .push(object.client_encrypted);
        self.bodies.lock().unwrap().push((key, out));
        Ok(())
    }

    async fn delete_marker(
        &self,
        _key: &ObjectKey,
        _version: &VersionId,
    ) -> Result<(), cairn_types::error::ReplicationError> {
        Ok(())
    }
}

/// A crypto double whose `open` always reports the sealing key as absent from the ring — the
/// mid-rotation / not-yet-loaded-key condition.
#[derive(Debug)]
struct UnknownKeyCrypto;

impl Crypto for UnknownKeyCrypto {
    fn seal(
        &self,
        _plaintext: &[u8],
    ) -> Result<cairn_types::crypto::Sealed, cairn_types::error::CryptoError> {
        Err(cairn_types::error::CryptoError::Encrypt)
    }
    fn open(
        &self,
        _ciphertext: &[u8],
        _nonce: &cairn_types::crypto::Nonce,
    ) -> Result<zeroize::Zeroizing<Vec<u8>>, cairn_types::error::CryptoError> {
        Err(cairn_types::error::CryptoError::UnknownKeyId)
    }
    fn ct_eq(&self, a: &[u8], b: &[u8]) -> bool {
        a == b
    }
}

/// A crypto double whose `open` reports authenticated-decryption failure — tampering, or a key
/// that is present but wrong. Unlike an unknown key id, this can never start working.
#[derive(Debug)]
struct TamperedCrypto;

impl Crypto for TamperedCrypto {
    fn seal(
        &self,
        _plaintext: &[u8],
    ) -> Result<cairn_types::crypto::Sealed, cairn_types::error::CryptoError> {
        Err(cairn_types::error::CryptoError::Encrypt)
    }
    fn open(
        &self,
        _ciphertext: &[u8],
        _nonce: &cairn_types::crypto::Nonce,
    ) -> Result<zeroize::Zeroizing<Vec<u8>>, cairn_types::error::CryptoError> {
        Err(cairn_types::error::CryptoError::Decrypt)
    }
    fn ct_eq(&self, a: &[u8], b: &[u8]) -> bool {
        a == b
    }
}

/// The DEK an encrypted fixture is staged under, and the `sse_descriptor` that seals it with
/// [`StubCrypto`] (which XORs, so the sealed form differs from the raw key).
fn encrypted_fixture_dek() -> ([u8; 32], String) {
    use base64::Engine;
    let dek = [0x11u8; 32];
    let sealed = StubCrypto.seal(&dek).unwrap();
    let json = format!(
        r#"{{"alg":"AES256-GCM","wrapped_dek_b64":"{}","nonce_b64":""}}"#,
        base64::engine::general_purpose::STANDARD.encode(&sealed.ciphertext)
    );
    (dek, json)
}

/// Commit an encrypted ObjectCreate version (blob staged under `dek`, row carrying the matching
/// `sse_descriptor`) with a pending, due outbox entry.
async fn put_encrypted_with_outbox(
    meta: &InMemoryMetadataStore,
    blobs: &InMemoryBlobStore,
    entry_id: &str,
    key: &str,
    data: &'static [u8],
    now: Timestamp,
) -> VersionId {
    let (dek, descriptor) = encrypted_fixture_dek();
    let staged = blobs
        .stage(
            &bucket(),
            body(data),
            StageOptions {
                compression: None,
                extra_checksums: ChecksumSet::none(),
                size_ceiling: 1 << 30,
                content_type: "text/plain".to_owned(),
                encryption: Some(dek),
                content_length: None,
            },
        )
        .await
        .unwrap();
    let version = VersionId::generate();
    let mut row = version_row(
        key,
        &version,
        Some(staged.storage_path.clone()),
        staged.etag.clone(),
        staged.size_logical,
        false,
        ReplicationStatus::Pending,
        now,
    );
    row.sse_descriptor = Some(descriptor);
    let entry = outbox_entry_for(
        entry_id,
        bucket(),
        ObjectKey::parse(key).unwrap(),
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
    version
}

#[tokio::test]
async fn encrypted_version_replicates_plaintext_not_ciphertext() {
    // THE incident. Before the fix the engine read the source blob with no DEK, so the replica
    // received the stored ciphertext at exactly the plaintext length — a mirror that answers 200
    // with garbage. Now the engine unseals `sse_descriptor` and ships the plaintext.
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let sink = CapturingSink::default();
    let router = SingleSink(sink);
    let clock = TestClock::at_secs(1_000);
    let now = clock.now();

    let version =
        put_encrypted_with_outbox(&meta, &blobs, "e1", "secret.txt", b"attack at dawn", now).await;

    let report = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.completed, 1, "the encrypted version must replicate");
    assert_eq!(
        version_status(&meta, "secret.txt", &version).await,
        Some(ReplicationStatus::Completed)
    );
    let bodies = router.0.bodies();
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0].1,
        b"attack at dawn".to_vec(),
        "the replica must receive the PLAINTEXT, not the stored ciphertext"
    );
}

#[tokio::test]
async fn an_unknown_key_id_is_unavailable_and_preserves_the_attempt_budget() {
    // A key that is merely not on the ring right now (mid-rotation) must NOT burn the attempt
    // budget: at max_attempts=3, three passes would otherwise stamp the version permanently failed
    // and silently stop the whole bucket replicating. And nothing may be shipped on the error path
    // — no ciphertext egress.
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let router = SingleSink(CapturingSink::default());
    let clock = TestClock::at_secs(1_000);
    let now = clock.now();

    let version =
        put_encrypted_with_outbox(&meta, &blobs, "e2", "secret.txt", b"attack at dawn", now).await;

    let engine = engine_with_crypto(Arc::new(UnknownKeyCrypto));
    for _ in 0..5 {
        let report = engine
            .run_once(&meta, &router, &blobs, &clock)
            .await
            .unwrap();
        assert_eq!(report.failed, 0, "a rotation window must never be terminal");
        clock.advance_secs(60);
    }
    assert!(
        router.0.bodies().is_empty(),
        "nothing may be shipped when the DEK cannot be resolved"
    );
    assert_ne!(
        version_status(&meta, "secret.txt", &version).await,
        Some(ReplicationStatus::Failed),
        "an unavailable key must leave the version retryable, not failed"
    );
    // Still queued, with the attempt budget intact (5 passes > max_attempts of 3).
    let due = due_entries(&meta, clock.now()).await;
    assert_eq!(due.len(), 1, "the entry must still be queued");
    assert_eq!(due[0].attempts, 0, "the attempt budget must be untouched");
    // The condition must be attributed to the SOURCE KEY, not the destination. Because
    // `Unavailable` never consumes the attempt budget, a permanently-removed key id retries here
    // forever and never lands in `failed` — so a message blaming the (healthy) destination would
    // send an operator to the wrong system indefinitely, and the pass counter below is the only
    // durable signal that such objects exist at all (review finding 4).
    let last = due[0].last_error.as_deref().unwrap_or_default();
    assert!(
        last.contains("source data key unavailable"),
        "last_error must name the local master ring, not the target: {last}"
    );
    assert!(
        !last.contains("target unavailable"),
        "a DEK failure must not be reported as a destination failure: {last}"
    );
    let report = engine
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(
        report.dek_resolve_failures, 1,
        "the pass must count the DEK-resolve failure (cairn_replication_dek_resolve_failed_total)"
    );
    assert_eq!(report.retried, 1, "and still count it as a reschedule");
}

#[tokio::test]
async fn client_requested_encryption_is_flagged_to_the_sink_but_at_rest_is_not() {
    // The sink refuses to put a CLIENT-encrypted plaintext body on an unauthenticated http://
    // endpoint, so the engine must classify the source correctly. `SseS3`/`Kms` = the client asked
    // for encryption (a contract); `AtRest` = transparent operator storage encryption, which is NOT
    // a client contract and must NOT gate replication that worked before this change.
    for (mode_json, expected) in [
        (None, true), // legacy descriptor: defaults to SSE-S3
        (Some(r#","mode":"sse-s3""#), true),
        (Some(r#","mode":"kms","kms_key_id":"k1""#), true),
        (Some(r#","mode":"at-rest""#), false),
    ] {
        let meta = InMemoryMetadataStore::new();
        let blobs = Arc::new(InMemoryBlobStore::new());
        let router = SingleSink(CapturingSink::default());
        let clock = TestClock::at_secs(1_000);
        let now = clock.now();

        let (dek, descriptor) = encrypted_fixture_dek();
        let descriptor = match mode_json {
            Some(extra) => format!("{}{extra}}}", descriptor.trim_end_matches('}')),
            None => descriptor,
        };
        let staged = blobs
            .stage(
                &bucket(),
                body(b"attack at dawn"),
                StageOptions {
                    compression: None,
                    extra_checksums: ChecksumSet::none(),
                    size_ceiling: 1 << 30,
                    content_type: "text/plain".to_owned(),
                    encryption: Some(dek),
                    content_length: None,
                },
            )
            .await
            .unwrap();
        let version = VersionId::generate();
        let mut row = version_row(
            "secret.txt",
            &version,
            Some(staged.storage_path.clone()),
            staged.etag.clone(),
            staged.size_logical,
            false,
            ReplicationStatus::Pending,
            now,
        );
        row.sse_descriptor = Some(descriptor.clone());
        let entry = outbox_entry_for(
            "m1",
            bucket(),
            ObjectKey::parse("secret.txt").unwrap(),
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
        assert_eq!(report.completed, 1, "descriptor {descriptor}");
        assert_eq!(
            router.0.client_encrypted_flags(),
            vec![expected],
            "descriptor {descriptor}"
        );
    }
}

#[tokio::test]
async fn an_unencrypted_version_is_not_flagged_client_encrypted() {
    // The common case must stay `false`, or every plaintext object would start being gated on an
    // http:// endpoint.
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let router = SingleSink(CapturingSink::default());
    let clock = TestClock::at_secs(1_000);
    let now = clock.now();
    put_with_outbox(&meta, &blobs, "p1", "plain.txt", b"hello", now, now).await;
    let report = engine()
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.completed, 1);
    assert_eq!(router.0.client_encrypted_flags(), vec![false]);
    assert_eq!(report.dek_resolve_failures, 0);
}

#[tokio::test]
async fn a_tampered_or_unopenable_dek_is_terminal() {
    // An AEAD failure can never succeed on retry: fail it fast and loudly rather than burning eight
    // attempts. Still fail-CLOSED — the sink is never contacted.
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let router = SingleSink(CapturingSink::default());
    let clock = TestClock::at_secs(1_000);
    let now = clock.now();

    let version =
        put_encrypted_with_outbox(&meta, &blobs, "e3", "secret.txt", b"attack at dawn", now).await;

    let report = engine_with_crypto(Arc::new(TamperedCrypto))
        .run_once(&meta, &router, &blobs, &clock)
        .await
        .unwrap();
    assert_eq!(report.failed, 1, "a tampered envelope is terminal at once");
    assert_eq!(
        version_status(&meta, "secret.txt", &version).await,
        Some(ReplicationStatus::Failed)
    );
    assert!(
        router.0.bodies().is_empty(),
        "nothing may be shipped when the DEK cannot be opened"
    );
}

#[tokio::test]
async fn a_malformed_sse_descriptor_is_terminal() {
    // A descriptor that does not parse is permanently unreadable — not a transient condition.
    let meta = InMemoryMetadataStore::new();
    let blobs = Arc::new(InMemoryBlobStore::new());
    let router = SingleSink(CapturingSink::default());
    let clock = TestClock::at_secs(1_000);
    let now = clock.now();

    let (path, etag, size) = stage_blob(&blobs, b"plain").await;
    let version = VersionId::generate();
    let mut row = version_row(
        "bad.txt",
        &version,
        Some(path),
        etag,
        size,
        false,
        ReplicationStatus::Pending,
        now,
    );
    row.sse_descriptor = Some("{not json".to_owned());
    let entry = outbox_entry_for(
        "e4",
        bucket(),
        ObjectKey::parse("bad.txt").unwrap(),
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
    assert_eq!(report.failed, 1);
    assert!(router.0.bodies().is_empty());
}
