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
