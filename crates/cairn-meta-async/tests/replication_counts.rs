#[path = "../../cairn-meta/tests/common/replication_counts.rs"]
mod contract;

#[tokio::test]
async fn replication_counters_include_active_work_in_libsql() {
    contract::exercise(&cairn_meta_async::open_libsql_in_memory().await.unwrap()).await;
}

#[tokio::test]
async fn replication_counters_include_active_work_in_turso() {
    contract::exercise(&cairn_meta_async::open_turso_in_memory().await.unwrap()).await;
}
