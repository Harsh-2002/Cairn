use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::traits::MetadataStore;
use cairn_types::*;
use std::sync::Arc;
use tokio::sync::Barrier;

fn object_row(bucket: &BucketName, key: &ObjectKey, version_id: &VersionId) -> ObjectVersionRow {
    ObjectVersionRow {
        id: format!("race-{}-{}", key.as_str(), version_id.as_str()),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version_id.clone(),
        is_latest: true,
        is_delete_marker: false,
        size_logical: 1,
        size_physical: 1,
        etag: ETag::from_string("race-etag".to_owned()),
        content_type: "application/octet-stream".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(StoragePath::from_string(format!(
            "{}/race-{}",
            bucket.as_str(),
            key.as_str()
        ))),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId("race-owner".to_owned()),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: None,
        replicated_at: None,
        created_at: Timestamp(10),
        updated_at: Timestamp(10),
    }
}

async fn put_with_retention(
    store: &Arc<dyn MetadataStore>,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: &VersionId,
    retain_until: Timestamp,
) {
    store
        .submit(Mutation::PutObjectVersion {
            row: Box::new(object_row(bucket, key, version_id)),
            precondition: Precondition::default(),
            initial_state: InitialObjectState {
                tags: Vec::new(),
                lock_intent: ExplicitObjectLockIntent {
                    retention: Some(ObjectRetention {
                        mode: ObjectLockMode::Compliance,
                        retain_until,
                    }),
                    legal_hold: None,
                },
            },
            replication: Vec::new(),
        })
        .await
        .unwrap();
}

pub async fn assert_writer_lock_races(store: Arc<dyn MetadataStore>, bucket_label: &str) {
    let bucket = BucketName::parse(bucket_label).unwrap();
    store
        .submit(Mutation::CreateObjectLockBucket(Box::new(Bucket {
            name: bucket.clone(),
            owner_id: UserId("race-owner".to_owned()),
            created_at: Timestamp(1),
            versioning: VersioningState::Enabled,
            ownership_mode: OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".to_owned(),
            compression: None,
        })))
        .await
        .unwrap();

    // The old retention is expired at the race time. Either deletion wins and the extension sees a
    // missing version, or the extension wins and the delete observes the new COMPLIANCE deadline.
    let key = ObjectKey::parse("extend-vs-delete").unwrap();
    let version = VersionId::from_string("race-v1".to_owned());
    put_with_retention(&store, &bucket, &key, &version, Timestamp(100)).await;
    let barrier = Arc::new(Barrier::new(3));
    let extension = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = key.clone();
        let version = version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::SetObjectRetention {
                    bucket,
                    key,
                    version_id: version,
                    retention: Some(ObjectRetention {
                        mode: ObjectLockMode::Compliance,
                        retain_until: Timestamp(1_000),
                    }),
                    now: Timestamp(200),
                    bypass: GovernanceBypass::Denied,
                })
                .await
        })
    };
    let deletion = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = key.clone();
        let version = version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::DeleteVersion {
                    bucket,
                    key,
                    version_id: version,
                    expected_row_id: None,
                    expected_updated_at: None,
                    require_sole_key_version: false,
                    now: Timestamp(200),
                    bypass: GovernanceBypass::Authorized,
                })
                .await
        })
    };
    barrier.wait().await;
    let extension = extension.await.unwrap();
    let deletion = deletion.await.unwrap();
    match (&extension, &deletion) {
        (Ok(MutationOutcome::Ack), Ok(MutationOutcome::DeleteProtected)) => {
            assert_eq!(
                store
                    .get_object_lock(&bucket, &key, &version)
                    .await
                    .unwrap()
                    .retention,
                Some(ObjectRetention {
                    mode: ObjectLockMode::Compliance,
                    retain_until: Timestamp(1_000),
                })
            );
        }
        (Err(MetaError::ObjectVersionNotFound), Ok(MutationOutcome::Deleted { .. })) => {
            assert!(
                store
                    .get_version(&bucket, &key, &version)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        other => panic!("unsafe retention/delete serialization: {other:?}"),
    }

    // A legal hold racing deletion has the same safe serial orders: either the hold commits and
    // protects the version, or deletion commits and the hold setter observes a missing version.
    let key = ObjectKey::parse("hold-vs-delete").unwrap();
    let version = VersionId::from_string("race-hold".to_owned());
    store
        .submit(Mutation::PutObjectVersion {
            row: Box::new(object_row(&bucket, &key, &version)),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: Vec::new(),
        })
        .await
        .unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let holder = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = key.clone();
        let version = version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::SetObjectLegalHold {
                    bucket,
                    key,
                    version_id: version,
                    on: true,
                })
                .await
        })
    };
    let deletion = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = key.clone();
        let version = version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::DeleteVersion {
                    bucket,
                    key,
                    version_id: version,
                    expected_row_id: None,
                    expected_updated_at: None,
                    require_sole_key_version: false,
                    now: Timestamp(20),
                    bypass: GovernanceBypass::Authorized,
                })
                .await
        })
    };
    barrier.wait().await;
    let holder = holder.await.unwrap();
    let deletion = deletion.await.unwrap();
    match (&holder, &deletion) {
        (Ok(MutationOutcome::Ack), Ok(MutationOutcome::DeleteProtected)) => {
            assert!(
                store
                    .get_object_lock(&bucket, &key, &version)
                    .await
                    .unwrap()
                    .legal_hold
            );
        }
        (Err(MetaError::ObjectVersionNotFound), Ok(MutationOutcome::Deleted { .. })) => {
            assert!(
                store
                    .get_version(&bucket, &key, &version)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        other => panic!("unsafe legal-hold/delete serialization: {other:?}"),
    }

    // If the longer extension wins first, the shorter update must fail. If the shorter update wins
    // first, the longer one advances it. No serial order may finish at the shorter deadline.
    let key = ObjectKey::parse("retention-vs-retention").unwrap();
    let version = VersionId::from_string("race-v2".to_owned());
    put_with_retention(&store, &bucket, &key, &version, Timestamp(1_000)).await;
    let barrier = Arc::new(Barrier::new(3));
    let shorter = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = key.clone();
        let version = version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::SetObjectRetention {
                    bucket,
                    key,
                    version_id: version,
                    retention: Some(ObjectRetention {
                        mode: ObjectLockMode::Compliance,
                        retain_until: Timestamp(2_000),
                    }),
                    now: Timestamp(100),
                    bypass: GovernanceBypass::Denied,
                })
                .await
        })
    };
    let longer = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = key.clone();
        let version = version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::SetObjectRetention {
                    bucket,
                    key,
                    version_id: version,
                    retention: Some(ObjectRetention {
                        mode: ObjectLockMode::Compliance,
                        retain_until: Timestamp(3_000),
                    }),
                    now: Timestamp(100),
                    bypass: GovernanceBypass::Denied,
                })
                .await
        })
    };
    barrier.wait().await;
    let shorter = shorter.await.unwrap();
    let longer = longer.await.unwrap();
    assert!(
        matches!(shorter, Ok(MutationOutcome::Ack))
            || matches!(shorter, Err(MetaError::ObjectProtected))
    );
    assert!(matches!(longer, Ok(MutationOutcome::Ack)));
    assert_eq!(
        store
            .get_object_lock(&bucket, &key, &version)
            .await
            .unwrap()
            .retention,
        Some(ObjectRetention {
            mode: ObjectLockMode::Compliance,
            retain_until: Timestamp(3_000),
        })
    );
}
