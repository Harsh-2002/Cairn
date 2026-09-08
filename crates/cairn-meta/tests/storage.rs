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
