//! Gate tests for the lifecycle scanner and the configuration parser. Every test is
//! deterministic: it drives the in-memory [`InMemoryMetadataStore`] / [`InMemoryBlobStore`]
//! doubles with a [`TestClock`] so all age and date math is reproducible.

use crate::{
    Action, BucketLifecycle, Expiration, Filter, LifecycleRule, LifecycleScanner, parse_lifecycle,
};
use cairn_types::testing::{InMemoryBlobStore, InMemoryMetadataStore, TestClock};
use cairn_types::{
    BlobStore, Bucket, BucketName, CompressionDescriptor, ListQuery, MetadataStore,
    MultipartSession, MultipartStatus, Mutation, ObjectKey, ObjectVersionRow, OwnershipMode,
    PartRecord, StorageClass, StoragePath, Timestamp, UploadId, UserId, VersionId, VersioningState,
};

// --------------------------------------------------------------------------------------------
// Fixtures
// --------------------------------------------------------------------------------------------

const DAY: i64 = 86_400;

fn owner() -> UserId {
    UserId("owner-1".to_owned())
}

fn bucket_name() -> BucketName {
    BucketName::parse("test-bucket").unwrap()
}

async fn make_bucket(meta: &InMemoryMetadataStore, versioning: VersioningState) {
    let bucket = Bucket {
        name: bucket_name(),
        owner_id: owner(),
        created_at: Timestamp::from_secs(0),
        versioning,
        ownership_mode: OwnershipMode::BucketOwnerEnforced,
        region: "us-east-1".to_owned(),
        compression: None,
    };
    meta.submit(Mutation::CreateBucket(Box::new(bucket)))
        .await
        .unwrap();
}

/// Stage a blob through the blob store and upsert a version row referencing it, so the object
/// is fully realized (metadata + bytes) exactly as the put path would leave it.
async fn put_object(
    meta: &InMemoryMetadataStore,
    blob: &InMemoryBlobStore,
    key: &str,
    body: &[u8],
    created_secs: i64,
    version_id: VersionId,
) -> StoragePath {
    let stream = once_body(body.to_vec());
    let staged = blob
        .stage(
            &bucket_name(),
            stream,
            cairn_types::StageOptions {
                compression: None,
                extra_checksums: cairn_types::ChecksumSet::none(),
                size_ceiling: 1 << 30,
                content_type: "application/octet-stream".to_owned(),
                encryption: None,
            },
        )
        .await
        .unwrap();
    let ts = Timestamp::from_secs(created_secs);
    let row = ObjectVersionRow {
        id: format!("row-{}-{}", key, version_id),
        bucket: bucket_name(),
        key: ObjectKey::parse(key).unwrap(),
        version_id,
        is_latest: true,
        is_delete_marker: false,
        size_logical: staged.size_logical,
        size_physical: staged.size_physical,
        etag: staged.etag.clone(),
        content_type: "application/octet-stream".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(staged.storage_path.clone()),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: owner(),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: None,
        created_at: ts,
        updated_at: ts,
    };
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: cairn_types::Precondition::default(),
        replication: None,
    })
    .await
    .unwrap();
    staged.storage_path
}

fn once_body(bytes: Vec<u8>) -> cairn_types::BodyStream {
    Box::pin(futures_util::stream::once(async move {
        Ok(bytes::Bytes::from(bytes))
    }))
}

/// Create a multipart session whose `created_at`/`updated_at` are the given time, and stage one
/// part for it so a successful abort must reclaim staged bytes.
async fn make_session(
    meta: &InMemoryMetadataStore,
    blob: &InMemoryBlobStore,
    key: &str,
    created_secs: i64,
) -> UploadId {
    let upload = UploadId::generate();
    let ts = Timestamp::from_secs(created_secs);
    let session = MultipartSession {
        upload_id: upload.clone(),
        bucket: bucket_name(),
        key: ObjectKey::parse(key).unwrap(),
        content_type: "application/octet-stream".to_owned(),
        status: MultipartStatus::Active,
        owner_id: owner(),
        intended_acl: None,
        user_metadata: Vec::new(),
        created_at: ts,
        updated_at: ts,
    };
    meta.submit(Mutation::CreateMultipart(Box::new(session)))
        .await
        .unwrap();
    // Stage a part and record it.
    let staged = blob
        .stage_part(&upload, 1, once_body(vec![1u8; 16]), 1 << 30)
        .await
        .unwrap();
    meta.submit(Mutation::RecordPart {
        upload_id: upload.clone(),
        part: PartRecord {
            part_number: 1,
            size: staged.size,
            etag: staged.md5_hex,
            storage_path: staged.storage_path,
            checksum: None,
        },
    })
    .await
    .unwrap();
    upload
}

fn cfg(rules: Vec<LifecycleRule>) -> Vec<BucketLifecycle> {
    vec![BucketLifecycle::new(bucket_name(), rules)]
}

fn expiration_rule(days: u32) -> LifecycleRule {
    LifecycleRule {
        id: "expire".to_owned(),
        enabled: true,
        filter: Filter::default(),
        actions: vec![Action::Expiration(Expiration::Days(days))],
    }
}

async fn count_versions(meta: &InMemoryMetadataStore) -> usize {
    meta.list_versions(
        &bucket_name(),
        &ListQuery {
            limit: 10_000,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .items
    .len()
}

async fn count_current(meta: &InMemoryMetadataStore) -> usize {
    meta.list_current(
        &bucket_name(),
        &ListQuery {
            limit: 10_000,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .items
    .len()
}

// --------------------------------------------------------------------------------------------
// Scanner gate tests
// --------------------------------------------------------------------------------------------

#[tokio::test]
async fn current_expiration_unversioned_deletes_and_reclaims_blob() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(0);
    make_bucket(&meta, VersioningState::Unversioned).await;

    let path = put_object(&meta, &blob, "doc.txt", b"hello", 0, VersionId::null()).await;
    assert_eq!(blob.blob_count(), 1);

    // 31 days later, a 30-day expiration is due.
    clock.set(Timestamp::from_secs(31 * DAY));
    let scanner = LifecycleScanner::new();
    let report = scanner
        .run_once(&meta, &blob, &clock, &cfg(vec![expiration_rule(30)]))
        .await
        .unwrap();

    assert_eq!(report.objects_expired, 1, "one object expired");
    assert_eq!(report.errors, 0);
    assert_eq!(count_current(&meta).await, 0, "object permanently gone");
    assert_eq!(count_versions(&meta).await, 0, "no versions remain");
    assert_eq!(blob.blob_count(), 0, "blob reclaimed");
    assert!(blob.get_bytes(&path).is_none());
}

#[tokio::test]
async fn current_expiration_not_due_before_threshold() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(0);
    make_bucket(&meta, VersioningState::Unversioned).await;
    put_object(&meta, &blob, "doc.txt", b"hello", 0, VersionId::null()).await;

    // Only 29 days have passed; a 30-day rule is not yet due.
    clock.set(Timestamp::from_secs(29 * DAY));
    let scanner = LifecycleScanner::new();
    let report = scanner
        .run_once(&meta, &blob, &clock, &cfg(vec![expiration_rule(30)]))
        .await
        .unwrap();

    assert_eq!(report.objects_expired, 0);
    assert_eq!(count_current(&meta).await, 1);
    assert_eq!(blob.blob_count(), 1);
}

#[tokio::test]
async fn current_expiration_versioned_inserts_delete_marker() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(0);
    make_bucket(&meta, VersioningState::Enabled).await;

    let path = put_object(&meta, &blob, "doc.txt", b"hello", 0, VersionId::generate()).await;

    clock.set(Timestamp::from_secs(31 * DAY));
    let scanner = LifecycleScanner::new();
    let report = scanner
        .run_once(&meta, &blob, &clock, &cfg(vec![expiration_rule(30)]))
        .await
        .unwrap();

    assert_eq!(report.objects_expired, 1);
    // The object vanishes from a plain (current) listing...
    assert_eq!(count_current(&meta).await, 0, "hidden by delete marker");
    // ...but the original version and a new delete marker both remain as versions.
    assert_eq!(count_versions(&meta).await, 2, "original + delete marker");
    // The data is NOT destroyed: the original blob is still present.
    assert_eq!(blob.blob_count(), 1, "data retained under versioning");
    assert!(blob.get_bytes(&path).is_some());
}

#[tokio::test]
async fn lifecycle_expiration_replicates_delete_marker() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(0);
    make_bucket(&meta, VersioningState::Enabled).await;

    // A replication rule that replicates delete markers to a stored target.
    let repl = r#"<ReplicationConfiguration><Role>arn:aws:iam::cairn:role/r</Role><Rule><ID>r1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><Filter><Prefix></Prefix></Filter><Destination><Bucket>arn:cairn:replication:us-east-1:t1:dest</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    meta.submit(Mutation::SetBucketConfig {
        bucket: bucket_name(),
        aspect: cairn_types::bucket::ConfigAspect::Replication,
        doc: Some(cairn_types::bucket::ConfigDoc(repl.to_owned())),
    })
    .await
    .unwrap();

    put_object(&meta, &blob, "doc.txt", b"hello", 0, VersionId::generate()).await;

    clock.set(Timestamp::from_secs(31 * DAY));
    let scanner = LifecycleScanner::new();
    let report = scanner
        .run_once(&meta, &blob, &clock, &cfg(vec![expiration_rule(30)]))
        .await
        .unwrap();
    assert_eq!(report.objects_expired, 1);

    // The lifecycle-created delete marker was enqueued for replication to the rule's target —
    // expirations propagate to the replica exactly like a client delete (ARCH §19.3/§20.3).
    let due = meta
        .list_due_replication(100, Timestamp::from_secs(31 * DAY))
        .await
        .unwrap();
    assert_eq!(due.len(), 1, "delete-marker replication enqueued");
    assert_eq!(
        due[0].operation,
        cairn_types::meta::ReplicationOp::DeleteMarker
    );
    assert_eq!(
        due[0].target_arn.as_deref(),
        Some("arn:cairn:replication:us-east-1:t1:dest")
    );
}

#[tokio::test]
async fn noncurrent_expiration_keeps_newest_and_deletes_old() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(0);
    make_bucket(&meta, VersioningState::Enabled).await;

    // Four versions of the same key, created on days 0,1,2,3. After each put the previous
    // becomes noncurrent. Versions are time-sortable (uuid v7).
    let mut paths = Vec::new();
    for day in 0..4 {
        let v = VersionId::generate();
        // Slight sleep keeps v7 ids monotonic across the loop.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let p = put_object(
            &meta,
            &blob,
            "doc.txt",
            format!("body-{day}").as_bytes(),
            day,
            v,
        )
        .await;
        paths.push(p);
    }
    assert_eq!(count_versions(&meta).await, 4);
    assert_eq!(blob.blob_count(), 4);

    // Rule: noncurrent versions older than 1 day, keep the newest 1 noncurrent version.
    let rule = LifecycleRule {
        id: "ncv".to_owned(),
        enabled: true,
        filter: Filter::default(),
        actions: vec![Action::NoncurrentVersionExpiration {
            days: 1,
            newer_noncurrent_versions: Some(1),
        }],
    };

    // Day 10: all noncurrent versions are far older than 1 day.
    clock.set(Timestamp::from_secs(10 * DAY));
    let scanner = LifecycleScanner::new();
    let report = scanner
        .run_once(&meta, &blob, &clock, &cfg(vec![rule]))
        .await
        .unwrap();

    // 3 noncurrent versions exist (days 0,1,2); keep newest 1 (day 2) -> delete days 0 and 1.
    assert_eq!(report.versions_expired, 2, "two oldest noncurrent deleted");
    assert_eq!(report.errors, 0);
    // Remaining: latest (day 3) + kept newest noncurrent (day 2) = 2 versions.
    assert_eq!(count_versions(&meta).await, 2);
    assert_eq!(count_current(&meta).await, 1, "latest still current");
    assert_eq!(blob.blob_count(), 2, "two blobs reclaimed");
}

#[tokio::test]
async fn abort_incomplete_multipart_removes_session_and_parts() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(0);
    make_bucket(&meta, VersioningState::Unversioned).await;

    let upload = make_session(&meta, &blob, "big.bin", 0).await;
    assert!(meta.get_multipart(&upload).await.unwrap().is_some());
    assert_eq!(
        meta.list_parts(&upload, 0, 100).await.unwrap().items.len(),
        1
    );

    let rule = LifecycleRule {
        id: "abort".to_owned(),
        enabled: true,
        filter: Filter::default(),
        actions: vec![Action::AbortIncompleteMultipartUpload {
            days_after_initiation: 7,
        }],
    };

    // 8 days later, the session is stale under a 7-day threshold.
    clock.set(Timestamp::from_secs(8 * DAY));
    let scanner = LifecycleScanner::new();
    let report = scanner
        .run_once(&meta, &blob, &clock, &cfg(vec![rule]))
        .await
        .unwrap();

    assert_eq!(report.uploads_aborted, 1);
    assert_eq!(report.errors, 0);
    assert!(
        meta.get_multipart(&upload).await.unwrap().is_none(),
        "session gone"
    );
    assert_eq!(
        meta.list_parts(&upload, 0, 100).await.unwrap().items.len(),
        0,
        "parts gone"
    );
}

#[tokio::test]
async fn abort_incomplete_multipart_respects_threshold() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(0);
    make_bucket(&meta, VersioningState::Unversioned).await;
    let upload = make_session(&meta, &blob, "big.bin", 0).await;

    let rule = LifecycleRule {
        id: "abort".to_owned(),
        enabled: true,
        filter: Filter::default(),
        actions: vec![Action::AbortIncompleteMultipartUpload {
            days_after_initiation: 7,
        }],
    };

    // Only 5 days have passed; not yet due.
    clock.set(Timestamp::from_secs(5 * DAY));
    let scanner = LifecycleScanner::new();
    let report = scanner
        .run_once(&meta, &blob, &clock, &cfg(vec![rule]))
        .await
        .unwrap();

    assert_eq!(report.uploads_aborted, 0);
    assert!(meta.get_multipart(&upload).await.unwrap().is_some());
}

#[tokio::test]
async fn expired_object_delete_marker_removed_when_sole_version() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(0);
    make_bucket(&meta, VersioningState::Enabled).await;

    // Put an object, then permanently delete its only version, then drop a delete marker so the
    // marker becomes the sole remaining version of the key.
    let v = VersionId::generate();
    put_object(&meta, &blob, "ghost.txt", b"x", 0, v.clone()).await;
    meta.submit(Mutation::DeleteVersion {
        bucket: bucket_name(),
        key: ObjectKey::parse("ghost.txt").unwrap(),
        version_id: v,
    })
    .await
    .unwrap();
    meta.submit(Mutation::CreateDeleteMarker {
        bucket: bucket_name(),
        key: ObjectKey::parse("ghost.txt").unwrap(),
        version_id: VersionId::generate(),
        owner_id: owner(),
        now: Timestamp::from_secs(0),
        replication: None,
    })
    .await
    .unwrap();
    assert_eq!(count_versions(&meta).await, 1, "only the delete marker");

    let rule = LifecycleRule {
        id: "dm".to_owned(),
        enabled: true,
        filter: Filter::default(),
        actions: vec![Action::ExpiredObjectDeleteMarker],
    };

    clock.set(Timestamp::from_secs(DAY));
    let scanner = LifecycleScanner::new();
    let report = scanner
        .run_once(&meta, &blob, &clock, &cfg(vec![rule]))
        .await
        .unwrap();

    assert_eq!(report.delete_markers_removed, 1);
    assert_eq!(count_versions(&meta).await, 0, "marker swept away");
}

#[tokio::test]
async fn expired_object_delete_marker_kept_when_other_versions_exist() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(0);
    make_bucket(&meta, VersioningState::Enabled).await;

    // A live version plus a delete marker: the marker is NOT the sole version, so it stays.
    put_object(&meta, &blob, "doc.txt", b"x", 0, VersionId::generate()).await;
    meta.submit(Mutation::CreateDeleteMarker {
        bucket: bucket_name(),
        key: ObjectKey::parse("doc.txt").unwrap(),
        version_id: VersionId::generate(),
        owner_id: owner(),
        now: Timestamp::from_secs(0),
        replication: None,
    })
    .await
    .unwrap();
    assert_eq!(count_versions(&meta).await, 2);

    let rule = LifecycleRule {
        id: "dm".to_owned(),
        enabled: true,
        filter: Filter::default(),
        actions: vec![Action::ExpiredObjectDeleteMarker],
    };
    clock.set(Timestamp::from_secs(DAY));
    let report = LifecycleScanner::new()
        .run_once(&meta, &blob, &clock, &cfg(vec![rule]))
        .await
        .unwrap();

    assert_eq!(report.delete_markers_removed, 0);
    assert_eq!(count_versions(&meta).await, 2, "nothing removed");
}

#[tokio::test]
async fn scanner_is_idempotent_running_twice_equals_once() {
    // Run a mixed workload, scan once, snapshot; scan again and confirm identical end state and
    // a zero second-pass tally for the now-no-op actions.
    let build = || async {
        let meta = InMemoryMetadataStore::new();
        let blob = InMemoryBlobStore::new();
        make_bucket(&meta, VersioningState::Enabled).await;

        // Two versions of a key (one becomes noncurrent), plus a stale upload.
        let v1 = VersionId::generate();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let v2 = VersionId::generate();
        put_object(&meta, &blob, "doc.txt", b"v1", 0, v1).await;
        put_object(&meta, &blob, "doc.txt", b"v2", 1, v2).await;
        make_session(&meta, &blob, "big.bin", 0).await;
        (meta, blob)
    };

    let rules = vec![
        expiration_rule(30),
        LifecycleRule {
            id: "ncv".to_owned(),
            enabled: true,
            filter: Filter::default(),
            actions: vec![Action::NoncurrentVersionExpiration {
                days: 1,
                newer_noncurrent_versions: None,
            }],
        },
        LifecycleRule {
            id: "abort".to_owned(),
            enabled: true,
            filter: Filter::default(),
            actions: vec![Action::AbortIncompleteMultipartUpload {
                days_after_initiation: 7,
            }],
        },
    ];

    let clock = TestClock::at_secs(40 * DAY);
    let scanner = LifecycleScanner::new();

    // --- Path A: scan once ---
    let (meta_a, blob_a) = build().await;
    scanner
        .run_once(&meta_a, &blob_a, &clock, &cfg(rules.clone()))
        .await
        .unwrap();
    let versions_a = count_versions(&meta_a).await;
    let current_a = count_current(&meta_a).await;
    let blobs_a = blob_a.blob_count();

    // --- Path B: scan twice ---
    let (meta_b, blob_b) = build().await;
    let first = scanner
        .run_once(&meta_b, &blob_b, &clock, &cfg(rules.clone()))
        .await
        .unwrap();
    let second = scanner
        .run_once(&meta_b, &blob_b, &clock, &cfg(rules.clone()))
        .await
        .unwrap();

    // End state identical regardless of run count.
    assert_eq!(
        count_versions(&meta_b).await,
        versions_a,
        "versions converge"
    );
    assert_eq!(count_current(&meta_b).await, current_a, "current converge");
    assert_eq!(blob_b.blob_count(), blobs_a, "blobs converge");

    // The second pass is a no-op: nothing more to expire/abort.
    assert_eq!(second.objects_expired, 0);
    assert_eq!(second.versions_expired, 0);
    assert_eq!(second.uploads_aborted, 0);
    assert_eq!(second.delete_markers_removed, 0);
    assert_eq!(second.errors, 0);
    // And the first pass actually did work, so the test is meaningful.
    assert!(
        first.objects_expired + first.versions_expired + first.uploads_aborted > 0,
        "first pass performed real work"
    );
}

#[tokio::test]
async fn disabled_rule_does_nothing() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(100 * DAY);
    make_bucket(&meta, VersioningState::Unversioned).await;
    put_object(&meta, &blob, "doc.txt", b"hello", 0, VersionId::null()).await;

    let mut rule = expiration_rule(1);
    rule.enabled = false;

    let report = LifecycleScanner::new()
        .run_once(&meta, &blob, &clock, &cfg(vec![rule]))
        .await
        .unwrap();

    assert_eq!(report.objects_expired, 0, "disabled rule applies nothing");
    assert_eq!(count_current(&meta).await, 1);
    assert_eq!(blob.blob_count(), 1);
}

#[tokio::test]
async fn prefix_filter_scopes_the_rule() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(100 * DAY);
    make_bucket(&meta, VersioningState::Unversioned).await;

    put_object(&meta, &blob, "logs/a.txt", b"a", 0, VersionId::null()).await;
    put_object(&meta, &blob, "data/b.txt", b"b", 0, VersionId::null()).await;

    let rule = LifecycleRule {
        id: "logs-only".to_owned(),
        enabled: true,
        filter: Filter {
            prefix: Some("logs/".to_owned()),
            ..Default::default()
        },
        actions: vec![Action::Expiration(Expiration::Days(1))],
    };

    let report = LifecycleScanner::new()
        .run_once(&meta, &blob, &clock, &cfg(vec![rule]))
        .await
        .unwrap();

    assert_eq!(report.objects_expired, 1, "only the logs/ object expired");
    let remaining = meta
        .list_current(
            &bucket_name(),
            &ListQuery {
                limit: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(remaining.items.len(), 1);
    assert_eq!(
        remaining.items[0].key.as_str(),
        "data/b.txt",
        "data/ survives"
    );
}

#[tokio::test]
async fn expiration_by_absolute_date() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(0);
    make_bucket(&meta, VersioningState::Unversioned).await;
    put_object(&meta, &blob, "doc.txt", b"hello", 0, VersionId::null()).await;

    // Expire on or after day 10.
    let rule = LifecycleRule {
        id: "date".to_owned(),
        enabled: true,
        filter: Filter::default(),
        actions: vec![Action::Expiration(Expiration::Date(10 * DAY))],
    };
    let scanner = LifecycleScanner::new();

    // Before the date: nothing.
    clock.set(Timestamp::from_secs(9 * DAY));
    let r = scanner
        .run_once(&meta, &blob, &clock, &cfg(vec![rule.clone()]))
        .await
        .unwrap();
    assert_eq!(r.objects_expired, 0);

    // On/after the date: expired.
    clock.set(Timestamp::from_secs(10 * DAY));
    let r = scanner
        .run_once(&meta, &blob, &clock, &cfg(vec![rule]))
        .await
        .unwrap();
    assert_eq!(r.objects_expired, 1);
    assert_eq!(blob.blob_count(), 0);
}

#[tokio::test]
async fn transition_action_is_a_documented_noop() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::at_secs(100 * DAY);
    make_bucket(&meta, VersioningState::Unversioned).await;
    put_object(&meta, &blob, "cold.txt", b"data", 0, VersionId::null()).await;

    let rule = LifecycleRule {
        id: "tier".to_owned(),
        enabled: true,
        filter: Filter::default(),
        actions: vec![Action::Transition(crate::Transition::Days(1))],
    };
    let report = LifecycleScanner::new()
        .run_once(&meta, &blob, &clock, &cfg(vec![rule]))
        .await
        .unwrap();

    // v1 transition is a no-op: nothing expired, object and blob untouched.
    assert_eq!(report.objects_expired, 0);
    assert_eq!(report.errors, 0);
    assert_eq!(count_current(&meta).await, 1);
    assert_eq!(blob.blob_count(), 1);
}

#[tokio::test]
async fn missing_bucket_is_skipped() {
    let meta = InMemoryMetadataStore::new();
    let blob = InMemoryBlobStore::new();
    let clock = TestClock::default();
    // No bucket created.
    let report = LifecycleScanner::new()
        .run_once(&meta, &blob, &clock, &cfg(vec![expiration_rule(1)]))
        .await
        .unwrap();
    assert_eq!(report, crate::LifecycleReport::default());
}

// --------------------------------------------------------------------------------------------
// Parser gate tests
// --------------------------------------------------------------------------------------------

#[test]
fn parse_lifecycle_round_trips_representative_config() {
    let xml = br#"
        <LifecycleConfiguration>
          <Rule>
            <ID>expire-logs</ID>
            <Status>Enabled</Status>
            <Filter>
              <And>
                <Prefix>logs/</Prefix>
                <Tag><Key>archive</Key><Value>yes</Value></Tag>
                <ObjectSizeGreaterThan>1024</ObjectSizeGreaterThan>
                <ObjectSizeLessThan>1048576</ObjectSizeLessThan>
              </And>
            </Filter>
            <Expiration><Days>30</Days></Expiration>
            <NoncurrentVersionExpiration>
              <NoncurrentDays>10</NoncurrentDays>
              <NewerNoncurrentVersions>3</NewerNoncurrentVersions>
            </NoncurrentVersionExpiration>
            <AbortIncompleteMultipartUpload>
              <DaysAfterInitiation>7</DaysAfterInitiation>
            </AbortIncompleteMultipartUpload>
          </Rule>
          <Rule>
            <ID>kill-markers</ID>
            <Status>Disabled</Status>
            <Filter><Prefix>tmp/</Prefix></Filter>
            <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>
          </Rule>
        </LifecycleConfiguration>
    "#;

    let rules = parse_lifecycle(xml).expect("valid config parses");
    assert_eq!(rules.len(), 2);

    let r0 = &rules[0];
    assert_eq!(r0.id, "expire-logs");
    assert!(r0.enabled);
    assert_eq!(r0.filter.prefix.as_deref(), Some("logs/"));
    assert_eq!(
        r0.filter.tags,
        vec![("archive".to_owned(), "yes".to_owned())]
    );
    assert_eq!(r0.filter.object_size_greater_than, Some(1024));
    assert_eq!(r0.filter.object_size_less_than, Some(1_048_576));
    assert!(
        r0.actions
            .contains(&Action::Expiration(Expiration::Days(30)))
    );
    assert!(r0.actions.contains(&Action::NoncurrentVersionExpiration {
        days: 10,
        newer_noncurrent_versions: Some(3),
    }));
    assert!(
        r0.actions
            .contains(&Action::AbortIncompleteMultipartUpload {
                days_after_initiation: 7,
            })
    );

    let r1 = &rules[1];
    assert_eq!(r1.id, "kill-markers");
    assert!(!r1.enabled, "Disabled status parsed");
    assert_eq!(r1.filter.prefix.as_deref(), Some("tmp/"));
    assert!(r1.actions.contains(&Action::ExpiredObjectDeleteMarker));
}

#[test]
fn parse_lifecycle_empty_config_is_empty_list() {
    assert_eq!(
        parse_lifecycle(b"<LifecycleConfiguration></LifecycleConfiguration>").unwrap(),
        Vec::new()
    );
    assert_eq!(
        parse_lifecycle(b"<LifecycleConfiguration/>").unwrap(),
        Vec::new()
    );
}

#[test]
fn parse_lifecycle_accepts_date_forms() {
    // ISO-8601 instant.
    let iso = br#"<LifecycleConfiguration><Rule><ID>d</ID><Status>Enabled</Status>
        <Expiration><Date>2026-01-01T00:00:00Z</Date></Expiration></Rule></LifecycleConfiguration>"#;
    let rules = parse_lifecycle(iso).unwrap();
    match rules[0].actions.first() {
        Some(Action::Expiration(Expiration::Date(secs))) => {
            // 2026-01-01T00:00:00Z = 1767225600 epoch seconds.
            assert_eq!(*secs, 1_767_225_600);
        }
        other => panic!("expected a date expiration, got {other:?}"),
    }

    // Bare epoch seconds.
    let epoch = br#"<LifecycleConfiguration><Rule><ID>d</ID><Status>Enabled</Status>
        <Expiration><Date>1767225600</Date></Expiration></Rule></LifecycleConfiguration>"#;
    let rules = parse_lifecycle(epoch).unwrap();
    assert_eq!(
        rules[0].actions.first(),
        Some(&Action::Expiration(Expiration::Date(1_767_225_600)))
    );
}

#[test]
fn parse_lifecycle_rejects_malformed_xml() {
    // Unbalanced tags.
    assert!(parse_lifecycle(b"<LifecycleConfiguration><Rule>").is_err());
    // Mismatched end tag.
    assert!(
        parse_lifecycle(b"<LifecycleConfiguration><Rule></Wrong></LifecycleConfiguration>")
            .is_err()
    );
    // Non-numeric Days.
    let bad_days = br#"<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status>
        <Expiration><Days>soon</Days></Expiration></Rule></LifecycleConfiguration>"#;
    assert!(parse_lifecycle(bad_days).is_err());
    // Unrecognized status.
    let bad_status = br#"<LifecycleConfiguration><Rule><ID>x</ID><Status>Paused</Status>
        <Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#;
    assert!(parse_lifecycle(bad_status).is_err());
    // Invalid UTF-8.
    assert!(parse_lifecycle(&[0xff, 0xfe, 0x00]).is_err());
}

#[test]
fn filter_matches_semantics() {
    let f = Filter {
        prefix: Some("a/".to_owned()),
        tags: vec![("k".to_owned(), "v".to_owned())],
        object_size_greater_than: Some(10),
        object_size_less_than: Some(100),
    };
    let tags = vec![("k".to_owned(), "v".to_owned())];
    assert!(f.matches("a/x", 50, &tags));
    assert!(!f.matches("b/x", 50, &tags), "wrong prefix");
    assert!(!f.matches("a/x", 5, &tags), "too small");
    assert!(!f.matches("a/x", 200, &tags), "too large");
    assert!(!f.matches("a/x", 50, &[]), "missing tag");
    assert!(
        Filter::default().matches("anything", 0, &[]),
        "empty filter matches all"
    );
}
