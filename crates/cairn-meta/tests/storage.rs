#[tokio::test]
async fn sqlite_prepare_storage_restore_contract() {
    let store = cairn_meta::open_in_memory().unwrap();
    cairn_types::testing::assert_prepare_storage_restore(&store).await;
}

#[tokio::test]
async fn sharded_prepare_storage_restore_contract() {
    use cairn_types::storage::StorageToken;
    use cairn_types::{MetadataStore, Mutation, MutationOutcome};
    use std::sync::Arc;
    let shards: Vec<Arc<dyn MetadataStore>> = (0..4)
        .map(|_| Arc::new(cairn_meta::open_in_memory().unwrap()) as Arc<dyn MetadataStore>)
        .collect();
    let store = cairn_meta::ShardedMetadataStore::new(shards.clone());
    cairn_types::testing::assert_prepare_storage_restore(&store).await;
    let generation = StorageToken::generate();
    assert_eq!(
        store
            .submit(Mutation::PrepareStorageRestore {
                generation: generation.clone(),
            })
            .await
            .unwrap(),
        MutationOutcome::Ack
    );
    for shard in shards {
        assert!(
            shard
                .submit(Mutation::PrepareStorageRestore {
                    generation: generation.clone(),
                })
                .await
                .is_err(),
            "every physical shard must carry the prepared generation"
        );
    }
}

#[tokio::test]
async fn sqlite_storage_journal_contract() {
    let store = cairn_meta::open_in_memory().unwrap();
    cairn_types::testing::assert_storage_journal(&store).await;
}

#[tokio::test]
async fn sharded_storage_journal_contract() {
    use std::sync::Arc;
    let shards = (0..4)
        .map(|_| {
            Arc::new(cairn_meta::open_in_memory().unwrap()) as Arc<dyn cairn_types::MetadataStore>
        })
        .collect();
    let store = cairn_meta::ShardedMetadataStore::new(shards);
    cairn_types::testing::assert_storage_journal(&store).await;
}

#[tokio::test]
async fn sqlite_storage_baseline_contract() {
    let store = cairn_meta::open_in_memory().unwrap();
    cairn_types::testing::assert_storage_baseline(&store).await;
}

#[tokio::test]
async fn sqlite_storage_baseline_authority_contract() {
    let store = cairn_meta::open_in_memory().unwrap();
    cairn_types::testing::assert_storage_baseline_authority(&store).await;
}

#[tokio::test]
async fn sharded_storage_baseline_contract() {
    let shards = (0..4)
        .map(|_| {
            std::sync::Arc::new(cairn_meta::open_in_memory().unwrap())
                as std::sync::Arc<dyn cairn_types::MetadataStore>
        })
        .collect();
    let store = cairn_meta::ShardedMetadataStore::new(shards);
    cairn_types::testing::assert_storage_baseline(&store).await;
}

#[tokio::test]
async fn sharded_storage_baseline_authority_contract() {
    let shards = (0..4)
        .map(|_| {
            std::sync::Arc::new(cairn_meta::open_in_memory().unwrap())
                as std::sync::Arc<dyn cairn_types::MetadataStore>
        })
        .collect();
    let store = cairn_meta::ShardedMetadataStore::new(shards);
    cairn_types::testing::assert_storage_baseline_authority(&store).await;
}

#[tokio::test]
async fn sqlite_storage_baseline_native_aliases() {
    let store = cairn_meta::open_in_memory().unwrap();
    cairn_types::testing::assert_storage_baseline_native_aliases(&store).await;
}

#[tokio::test]
async fn sharded_storage_baseline_native_aliases() {
    let shards = (0..4)
        .map(|_| {
            std::sync::Arc::new(cairn_meta::open_in_memory().unwrap())
                as std::sync::Arc<dyn cairn_types::MetadataStore>
        })
        .collect();
    let store = cairn_meta::ShardedMetadataStore::new(shards);
    cairn_types::testing::assert_storage_baseline_native_aliases(&store).await;
}

#[tokio::test]
async fn sqlite_unused_index_spool_is_not_retired() {
    let store = cairn_meta::open_in_memory().unwrap();
    cairn_types::testing::assert_unused_index_spool_is_not_retired(&store).await;
}

#[tokio::test]
async fn sharded_unused_index_spool_is_not_retired() {
    use std::sync::Arc;
    let shards = (0..4)
        .map(|_| {
            Arc::new(cairn_meta::open_in_memory().unwrap()) as Arc<dyn cairn_types::MetadataStore>
        })
        .collect();
    let store = cairn_meta::ShardedMetadataStore::new(shards);
    cairn_types::testing::assert_unused_index_spool_is_not_retired(&store).await;
}

#[tokio::test]
async fn sqlite_publication_co_commits_activity() {
    let store = cairn_meta::open_in_memory().unwrap();
    cairn_types::testing::assert_publication_co_commits_activity(&store).await;
}

#[tokio::test]
async fn sharded_publication_co_commits_activity() {
    use std::sync::Arc;
    let shards = (0..4)
        .map(|_| {
            Arc::new(cairn_meta::open_in_memory().unwrap()) as Arc<dyn cairn_types::MetadataStore>
        })
        .collect();
    let store = cairn_meta::ShardedMetadataStore::new(shards);
    cairn_types::testing::assert_publication_co_commits_activity(&store).await;
}

/// A plan authorized against one bucket instance must not be admitted after that bucket was
/// deleted and recreated under another owner: name equality alone is not identity.
#[tokio::test]
async fn storage_admission_rejects_a_recreated_bucket_from_an_old_plan() {
    use cairn_types::authz::OwnershipMode;
    use cairn_types::bucket::VersioningState;
    use cairn_types::storage::{
        PlannedStorageWrite, StorageAdmission, StorageMutation, StorageToken, StorageWriteTarget,
    };
    use cairn_types::{
        Bucket, BucketName, MetadataStore, Mutation, MutationOutcome, ObjectKey, Timestamp, UserId,
        VersionId,
    };
    let store = cairn_meta::open_in_memory().unwrap();
    let bucket = BucketName::parse("recreate-race").unwrap();
    let owner_a = UserId::generate();
    let owner_b = UserId::generate();
    let new_bucket = |owner: &UserId| {
        Box::new(Bucket {
            name: bucket.clone(),
            owner_id: owner.clone(),
            created_at: Timestamp(1),
            versioning: VersioningState::Unversioned,
            ownership_mode: OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".to_owned(),
            compression: None,
        })
    };
    store
        .submit(Mutation::CreateBucket(new_bucket(&owner_a)))
        .await
        .unwrap();
    let generation = StorageToken::generate();
    store
        .submit(Mutation::BeginStorageGeneration {
            generation: generation.clone(),
        })
        .await
        .unwrap();
    let planned = PlannedStorageWrite::new(
        bucket.clone(),
        generation,
        StorageWriteTarget::Object {
            key: ObjectKey::parse("obj").unwrap(),
            version_id: VersionId::null(),
            row_id: StorageToken::generate().as_str().to_owned(),
        },
    )
    .unwrap()
    .with_bucket_snapshot(owner_a.clone(), Timestamp(1));
    let plan = planned.plan().clone();
    store
        .submit(Mutation::DeleteBucket(bucket.clone()))
        .await
        .unwrap();
    store
        .submit(Mutation::CreateBucket(new_bucket(&owner_b)))
        .await
        .unwrap();
    let outcome = store
        .submit(Mutation::Storage {
            bucket: bucket.clone(),
            operation: StorageMutation::Reserve {
                plan: Box::new(plan),
                now: Timestamp(2),
            },
        })
        .await
        .unwrap();
    assert!(
        matches!(
            outcome,
            MutationOutcome::StorageAdmission(StorageAdmission::NotApplied)
        ),
        "a plan authorized for the old bucket must not be admitted to a recreated one: {outcome:?}"
    );
}

#[tokio::test]
async fn sharded_admission_rejects_a_recreated_bucket() {
    use cairn_types::MetadataStore;
    use std::sync::Arc;
    let shards: Vec<Arc<dyn MetadataStore>> = (0..4)
        .map(|_| Arc::new(cairn_meta::open_in_memory().unwrap()) as Arc<dyn MetadataStore>)
        .collect();
    let store = cairn_meta::ShardedMetadataStore::new(shards);
    cairn_types::testing::assert_admission_rejects_recreated_bucket(&store).await;
}
