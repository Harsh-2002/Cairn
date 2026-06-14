//! Contract tests for the canonical in-memory doubles. These pin the semantics every real
//! backend must reproduce: durable staging, the put commit point, listing, conditional-write
//! atomicity, versioning/delete-marker bookkeeping, and bounded reconciliation.

use bytes::Bytes;
use cairn_types::testing::{InMemoryBlobStore, InMemoryMetadataStore, TestClock};
use cairn_types::*;

fn body(data: &'static [u8]) -> BodyStream {
    Box::pin(futures_util::stream::once(async move {
        Ok(Bytes::from_static(data))
    }))
}

fn row_from(
    staged: &StagedBlob,
    bucket: &BucketName,
    key: &ObjectKey,
    version: VersionId,
    owner: &UserId,
    now: Timestamp,
) -> ObjectVersionRow {
    ObjectVersionRow {
        id: uuid::Uuid::new_v4().simple().to_string(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version,
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
        owner_id: owner.clone(),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: None,
        created_at: now,
        updated_at: now,
    }
}

async fn read_all(handle: BlobReadHandle) -> Vec<u8> {
    use futures_util::StreamExt;
    let mut out = Vec::new();
    let mut body = handle.body;
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

#[tokio::test]
async fn put_get_list_delete_roundtrip() {
    let blob = InMemoryBlobStore::new();
    let meta = InMemoryMetadataStore::new();
    let clock = TestClock::default();
    let bucket = BucketName::parse("test-bucket").unwrap();
    let owner = UserId::generate();

    // Stage + commit two objects.
    for (k, data) in [("a/1.txt", &b"hello"[..]), ("a/2.txt", &b"world!!"[..])] {
        let key = ObjectKey::parse(k).unwrap();
        let staged = blob
            .stage(
                &bucket,
                Box::pin(futures_util::stream::once({
                    let d = data.to_vec();
                    async move { Ok(Bytes::from(d)) }
                })),
                StageOptions {
                    compression: None,
                    extra_checksums: ChecksumSet::none(),
                    size_ceiling: 1 << 20,
                    content_type: "text/plain".to_owned(),
                    encryption: None,
                    content_length: None,
                },
            )
            .await
            .unwrap();
        let row = row_from(
            &staged,
            &bucket,
            &key,
            VersionId::null(),
            &owner,
            clock.now(),
        );
        let outcome = meta
            .submit(Mutation::PutObjectVersion {
                row: Box::new(row),
                precondition: Precondition::default(),
                replication: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            MutationOutcome::Put {
                superseded: None,
                ..
            }
        ));
    }

    // GET back the first object's bytes.
    let current = meta
        .current_version(&bucket, &ObjectKey::parse("a/1.txt").unwrap())
        .await
        .unwrap()
        .unwrap();
    let handle = blob
        .open(current.storage_path.as_ref().unwrap(), None)
        .await
        .unwrap();
    assert_eq!(read_all(handle).await, b"hello");
    assert_eq!(current.etag.as_str(), "5d41402abc4b2a76b9719d911017c592"); // md5("hello")

    // LIST with a prefix returns both, sorted.
    let page = meta
        .list_current(
            &bucket,
            &ListQuery {
                prefix: Some("a/".to_owned()),
                limit: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].key.as_str(), "a/1.txt");
    assert!(!page.truncated);

    // DELETE the second; bucket is not empty (one remains).
    let key2 = ObjectKey::parse("a/2.txt").unwrap();
    let del = meta
        .submit(Mutation::DeleteVersion {
            bucket: bucket.clone(),
            key: key2.clone(),
            version_id: VersionId::null(),
        })
        .await
        .unwrap();
    let MutationOutcome::Deleted { freed: Some(_), .. } = del else {
        panic!("expected freed blob");
    };
    assert!(!meta.is_bucket_empty(&bucket).await.unwrap());
}

#[tokio::test]
async fn conditional_write_if_none_match_is_atomic() {
    let blob = InMemoryBlobStore::new();
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("cond-bucket").unwrap();
    let key = ObjectKey::parse("k").unwrap();
    let owner = UserId::generate();

    let staged = blob.stage(&bucket, body(b"v1"), opts()).await.unwrap();
    let row = row_from(
        &staged,
        &bucket,
        &key,
        VersionId::null(),
        &owner,
        Timestamp::EPOCH,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row.clone()),
        precondition: Precondition::default(),
        replication: None,
    })
    .await
    .unwrap();

    // If-None-Match: * must now fail because the object exists.
    let err = meta
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition {
                if_match: None,
                if_none_match: Some(IfNoneMatch::Any),
            },
            replication: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, MetaError::PreconditionFailed));
}

#[tokio::test]
async fn versioning_keeps_history_and_promotes_latest() {
    let blob = InMemoryBlobStore::new();
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("ver-bucket").unwrap();
    let key = ObjectKey::parse("doc").unwrap();
    let owner = UserId::generate();

    let mut versions = Vec::new();
    for data in [&b"one"[..], &b"two"[..], &b"three"[..]] {
        let staged = blob
            .stage(
                &bucket,
                Box::pin(futures_util::stream::once({
                    let d = data.to_vec();
                    async move { Ok(Bytes::from(d)) }
                })),
                opts(),
            )
            .await
            .unwrap();
        let v = VersionId::generate();
        versions.push(v.clone());
        let mut row = row_from(&staged, &bucket, &key, v, &owner, Timestamp::EPOCH);
        row.is_latest = true;
        meta.submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: None,
        })
        .await
        .unwrap();
        // version ids are time-sortable; give each a distinct timestamp
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    // All three versions are retained.
    let all = meta
        .list_versions(
            &bucket,
            &ListQuery {
                limit: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(all.items.len(), 3);

    // Latest is the third version; deleting it promotes the second.
    let latest = meta.current_version(&bucket, &key).await.unwrap().unwrap();
    assert_eq!(latest.version_id, *versions.last().unwrap());
    let del = meta
        .submit(Mutation::DeleteVersion {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: versions[2].clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        del,
        MutationOutcome::Deleted {
            promoted_latest: true,
            ..
        }
    ));
    let new_latest = meta.current_version(&bucket, &key).await.unwrap().unwrap();
    assert_eq!(new_latest.version_id, versions[1]);
}

#[tokio::test]
async fn reconcile_reclaims_orphan_blobs() {
    let blob = InMemoryBlobStore::new();
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("recon-bucket").unwrap();
    let key = ObjectKey::parse("kept").unwrap();
    let owner = UserId::generate();

    // One referenced blob...
    let kept = blob.stage(&bucket, body(b"keep"), opts()).await.unwrap();
    let row = row_from(
        &kept,
        &bucket,
        &key,
        VersionId::null(),
        &owner,
        Timestamp::EPOCH,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: None,
    })
    .await
    .unwrap();
    // ...and one orphan blob with no metadata row (a crash between durability and commit).
    let _orphan = blob.stage(&bucket, body(b"orphan"), opts()).await.unwrap();
    assert_eq!(blob.blob_count(), 2);

    let oracle = meta.oracle();
    let report = blob
        .reconcile(&oracle, ReconcileOpts::default())
        .await
        .unwrap();
    assert_eq!(report.orphans_reclaimed, 1);
    assert_eq!(blob.blob_count(), 1);
}

fn opts() -> StageOptions {
    StageOptions {
        compression: None,
        extra_checksums: ChecksumSet::none(),
        size_ceiling: 1 << 20,
        content_type: "application/octet-stream".to_owned(),
        encryption: None,
        content_length: None,
    }
}

/// Plant one replication-outbox entry by committing a version that carries it, returning its id.
async fn plant_outbox_entry(
    meta: &InMemoryMetadataStore,
    bucket: &BucketName,
    key: &ObjectKey,
    version: &VersionId,
    id: &str,
) {
    let entry = cairn_types::meta::OutboxEntry {
        id: id.to_owned(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        operation: cairn_types::meta::ReplicationOp::ObjectCreate,
        rule_id: "rule-1".to_owned(),
        target_arn: None,
        attempts: 0,
        next_attempt_at: Timestamp::EPOCH,
        status: cairn_types::meta::ReplicationStatus::Pending,
        last_error: None,
        priority: 0,
        lease_until: None,
    };
    let row = ObjectVersionRow {
        id: uuid::Uuid::new_v4().simple().to_string(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        is_latest: true,
        is_delete_marker: false,
        size_logical: 3,
        size_physical: 3,
        etag: cairn_types::object::ETag::from_string("e".to_owned()),
        content_type: "text/plain".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(StoragePath::from_string(format!(
            "{}/{}",
            bucket.as_str(),
            version.as_str()
        ))),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId::generate(),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: Some(cairn_types::meta::ReplicationStatus::Pending),
        created_at: Timestamp::EPOCH,
        updated_at: Timestamp::EPOCH,
    };
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: Some(entry),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn list_failed_replication_returns_only_terminal_entries() {
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("repl-bucket").unwrap();
    let key = ObjectKey::parse("k").unwrap();
    let v1 = VersionId::from_string("00000001".to_owned());
    let v2 = VersionId::from_string("00000002".to_owned());

    // Two outbox entries: one we will leave pending, one we will mark terminally failed.
    plant_outbox_entry(&meta, &bucket, &key, &v1, "pending-1").await;
    plant_outbox_entry(&meta, &bucket, &key, &v2, "doomed-1").await;

    // Nothing has failed yet.
    assert!(meta.list_failed_replication(100).await.unwrap().is_empty());

    // Mark the second entry terminal (next_attempt_at = None per the engine's terminal marking).
    meta.submit(Mutation::MarkReplicationFailed {
        id: "doomed-1".to_owned(),
        error: "destination unreachable".to_owned(),
        next_attempt_at: None,
    })
    .await
    .unwrap();

    let failed = meta.list_failed_replication(100).await.unwrap();
    assert_eq!(failed.len(), 1, "only the terminal entry is reported");
    assert_eq!(failed[0].id, "doomed-1");
    assert_eq!(failed[0].version_id, v2);
    assert_eq!(failed[0].attempts, 1);
    assert_eq!(
        failed[0].last_error.as_deref(),
        Some("destination unreachable")
    );

    // A retryable failure (next_attempt_at = Some) is NOT terminal and must not be listed.
    meta.submit(Mutation::MarkReplicationFailed {
        id: "pending-1".to_owned(),
        error: "transient".to_owned(),
        next_attempt_at: Some(Timestamp::from_secs(60)),
    })
    .await
    .unwrap();
    let failed = meta.list_failed_replication(100).await.unwrap();
    assert_eq!(
        failed.len(),
        1,
        "the retryable entry stays out of the failed list"
    );
    assert_eq!(failed[0].id, "doomed-1");

    // The limit is honoured.
    assert!(meta.list_failed_replication(0).await.unwrap().is_empty());
}

#[tokio::test]
async fn get_bucket_quota_round_trips_set_and_clear() {
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("quota-bucket").unwrap();

    // No quota set initially.
    assert_eq!(meta.get_bucket_quota(&bucket).await.unwrap(), None);

    // Setting a quota is readable back.
    meta.submit(Mutation::SetBucketQuota {
        bucket: bucket.clone(),
        quota_bytes: Some(1_048_576),
    })
    .await
    .unwrap();
    assert_eq!(
        meta.get_bucket_quota(&bucket).await.unwrap(),
        Some(1_048_576)
    );

    // Clearing it (None) returns to unlimited.
    meta.submit(Mutation::SetBucketQuota {
        bucket: bucket.clone(),
        quota_bytes: None,
    })
    .await
    .unwrap();
    assert_eq!(meta.get_bucket_quota(&bucket).await.unwrap(), None);
}

#[tokio::test]
async fn set_object_acl_replaces_the_version_acl() {
    use cairn_types::authz::{Acl, Grant, Grantee, Permission};

    let blob = InMemoryBlobStore::new();
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("acl-bucket").unwrap();
    let key = ObjectKey::parse("obj").unwrap();
    let owner = UserId::generate();

    let staged = blob.stage(&bucket, body(b"data"), opts()).await.unwrap();
    let version = VersionId::null();
    let row = row_from(
        &staged,
        &bucket,
        &key,
        version.clone(),
        &owner,
        Timestamp::EPOCH,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: None,
    })
    .await
    .unwrap();

    // The freshly-put object has no ACL.
    let got = meta
        .get_version(&bucket, &key, &version)
        .await
        .unwrap()
        .unwrap();
    assert!(got.acl.is_none());

    // Setting an ACL replaces the version's `acl` column.
    let acl = Acl {
        owner: owner.clone(),
        grants: vec![Grant {
            grantee: Grantee::AllUsers,
            permission: Permission::Read,
        }],
    };
    meta.submit(Mutation::SetObjectAcl {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        acl: Some(acl.clone()),
    })
    .await
    .unwrap();
    let got = meta
        .get_version(&bucket, &key, &version)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.acl, Some(acl));

    // Clearing it (None) removes the ACL again.
    meta.submit(Mutation::SetObjectAcl {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        acl: None,
    })
    .await
    .unwrap();
    let got = meta
        .get_version(&bucket, &key, &version)
        .await
        .unwrap()
        .unwrap();
    assert!(got.acl.is_none());
}
