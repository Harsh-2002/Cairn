use cairn_types::traits::MetadataStore;
use std::sync::Arc;
#[path = "common/replication_counts.rs"]
mod contract;

#[tokio::test]
async fn replication_counters_include_active_work_in_sqlite_and_memory() {
    contract::exercise(&cairn_meta::open_in_memory().unwrap()).await;
    contract::exercise(&cairn_types::testing::InMemoryMetadataStore::new()).await;
}

#[tokio::test]
async fn replication_counters_merge_active_targets_across_shards() {
    let shards: Vec<Arc<dyn MetadataStore>> = (0..3)
        .map(|_| Arc::new(cairn_meta::open_in_memory().unwrap()) as Arc<dyn MetadataStore>)
        .collect();
    contract::exercise(&cairn_meta::ShardedMetadataStore::new(shards)).await;
}
