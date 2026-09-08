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
