#[tokio::test]
async fn libsql_prepare_storage_restore_contract() {
    let store = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    cairn_types::testing::assert_prepare_storage_restore(&store).await;
}

#[tokio::test]
async fn turso_prepare_storage_restore_contract() {
    let store = cairn_meta_async::open_turso_in_memory().await.unwrap();
    cairn_types::testing::assert_prepare_storage_restore(&store).await;
}

#[tokio::test]
async fn libsql_storage_journal_contract() {
    let store = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    cairn_types::testing::assert_storage_journal(&store).await;
}

#[tokio::test]
async fn turso_storage_journal_contract() {
    let store = cairn_meta_async::open_turso_in_memory().await.unwrap();
    cairn_types::testing::assert_storage_journal(&store).await;
}

#[tokio::test]
async fn libsql_storage_baseline_contract() {
    let store = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    cairn_types::testing::assert_storage_baseline(&store).await;
}

#[tokio::test]
async fn libsql_storage_baseline_authority_contract() {
    let store = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    cairn_types::testing::assert_storage_baseline_authority(&store).await;
}

#[tokio::test]
async fn turso_storage_baseline_contract() {
    let store = cairn_meta_async::open_turso_in_memory().await.unwrap();
    cairn_types::testing::assert_storage_baseline(&store).await;
}

#[tokio::test]
async fn turso_storage_baseline_authority_contract() {
    let store = cairn_meta_async::open_turso_in_memory().await.unwrap();
    cairn_types::testing::assert_storage_baseline_authority(&store).await;
}

#[tokio::test]
async fn libsql_storage_baseline_native_aliases() {
    let store = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    cairn_types::testing::assert_storage_baseline_native_aliases(&store).await;
}

#[tokio::test]
async fn turso_storage_baseline_native_aliases() {
    let store = cairn_meta_async::open_turso_in_memory().await.unwrap();
    cairn_types::testing::assert_storage_baseline_native_aliases(&store).await;
}

#[tokio::test]
async fn libsql_unused_index_spool_is_not_retired() {
    let store = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    cairn_types::testing::assert_unused_index_spool_is_not_retired(&store).await;
}

#[tokio::test]
async fn turso_unused_index_spool_is_not_retired() {
    let store = cairn_meta_async::open_turso_in_memory().await.unwrap();
    cairn_types::testing::assert_unused_index_spool_is_not_retired(&store).await;
}
