//! The same observable counter contract runs against every backend and shard fan-out.
use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::testing::FixtureMetadataStore;
use cairn_types::traits::MetadataStore;
use cairn_types::*;

pub async fn exercise(store: &dyn MetadataStore) {
    let fixture = store.begin_fixture().await.unwrap();
    let mut legacy_claims = Vec::new();
    for name in ["alpha", "bravo", "charlie"] {
        let bucket = BucketName::parse(name).unwrap();
        store
            .submit(Mutation::CreateBucket(Box::new(Bucket {
                name: bucket.clone(),
                owner_id: UserId("owner".into()),
                created_at: Timestamp(1),
                versioning: VersioningState::Enabled,
                ownership_mode: OwnershipMode::BucketOwnerEnforced,
                region: "us-east-1".into(),
                compression: None,
            })))
            .await
            .unwrap();
        let key = ObjectKey::parse("object").unwrap();
        let version = VersionId::from_string("version".into());
        let replication = (0..1004)
            .map(|i| OutboxEntry {
                id: format!("{name}-{i}"),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version.clone(),
                operation: ReplicationOp::ObjectCreate,
                rule_id: "rule".into(),
                target_arn: match i {
                    1 => None,
                    2 => Some("arn:B".into()),
                    _ => Some("arn:A".into()),
                },
                attempts: 0,
                next_attempt_at: Timestamp(if i < 3 { 0 } else { 1_000_000 }),
                status: ReplicationStatus::Pending,
                last_error: None,
                priority: 0,
                lease_until: None,
                claim_token: None,
                enqueued_at: Timestamp(1),
            })
            .collect();
        let row = ObjectVersionRow {
            id: uuid::Uuid::new_v4().simple().to_string(),
            bucket: bucket.clone(),
            key,
            version_id: version,
            is_latest: true,
            is_delete_marker: false,
            size_logical: 1,
            size_physical: 1,
            etag: ETag::from_string("e".into()),
            content_type: "text/plain".into(),
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            storage_path: Some(StoragePath::generate(&bucket)),
            compression: CompressionDescriptor::Uncompressed,
            storage_class: StorageClass::Standard,
            cold_locator: None,
            owner_id: UserId("owner".into()),
            user_metadata: Vec::new(),
            acl: None,
            checksums: Vec::new(),
            sse_descriptor: None,
            replication_status: None,
            internal_sha256: None,
            replicated_at: None,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        };
        store
            .submit_fixture(
                &fixture,
                Mutation::PutObjectVersion {
                    row: Box::new(row),
                    precondition: Precondition::default(),
                    initial_state: InitialObjectState::default(),
                    replication,
                },
            )
            .await
            .unwrap();
        let claims = store
            .claim_replication_batch(3, Timestamp(10))
            .await
            .unwrap();
        assert_eq!(claims.len(), 3);
        for entry in claims {
            if entry.target_arn.as_deref() == Some("arn:B") {
                store
                    .submit(Mutation::MarkReplicationFailed {
                        id: entry.id,
                        claim_token: entry.claim_token.unwrap(),
                        now: Timestamp(11),
                        error: "terminal".into(),
                        next_attempt_at: None,
                    })
                    .await
                    .unwrap();
            } else if entry.target_arn.is_none() {
                legacy_claims.push(entry);
            }
        }
        let counts = store.replication_counts(Some(&bucket)).await.unwrap();
        assert_eq!(
            (counts.pending, counts.claimed, counts.failed),
            (1001, 2, 1)
        );
        assert_eq!(
            counts
                .by_target
                .iter()
                .find(|t| t.target_arn.is_none())
                .unwrap()
                .claimed,
            1
        );
    }
    let all = store.replication_counts(None).await.unwrap();
    assert_eq!((all.pending, all.claimed, all.failed), (3003, 6, 3));
    let active = all
        .by_target
        .iter()
        .find(|t| t.target_arn.as_deref() == Some("arn:A"))
        .unwrap();
    assert_eq!(
        (active.pending, active.claimed, active.failed),
        (3003, 3, 0)
    );
    let legacy = all
        .by_target
        .iter()
        .find(|t| t.target_arn.is_none())
        .unwrap();
    assert_eq!((legacy.pending, legacy.claimed, legacy.failed), (0, 3, 0));
    for entry in legacy_claims {
        store
            .submit(Mutation::MarkReplicationDone {
                id: entry.id,
                claim_token: entry.claim_token.unwrap(),
                now: Timestamp(12),
            })
            .await
            .unwrap();
    }
    let done = store.replication_counts(None).await.unwrap();
    assert_eq!((done.claimed, done.completed), (3, 3));
    assert!(done.by_target.iter().all(|t| t.target_arn.is_some()));
}
