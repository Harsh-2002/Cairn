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
        storage_path: Some(staged.storage_path.clone()),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: owner.clone(),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
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
    }
}
