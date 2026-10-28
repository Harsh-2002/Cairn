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

#[tokio::test]
async fn libsql_publication_co_commits_activity() {
    let store = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    cairn_types::testing::assert_publication_co_commits_activity(&store).await;
}

#[tokio::test]
async fn turso_publication_co_commits_activity() {
    let store = cairn_meta_async::open_turso_in_memory().await.unwrap();
    cairn_types::testing::assert_publication_co_commits_activity(&store).await;
}

#[tokio::test]
async fn libsql_admission_rejects_a_recreated_bucket() {
    let store = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    cairn_types::testing::assert_admission_rejects_recreated_bucket(&store).await;
}

#[tokio::test]
async fn turso_admission_rejects_a_recreated_bucket() {
    let store = cairn_meta_async::open_turso_in_memory().await.unwrap();
    cairn_types::testing::assert_admission_rejects_recreated_bucket(&store).await;
}

#[tokio::test]
async fn libsql_object_publication_contract() {
    cairn_types::testing::assert_object_publication_rejects_recreated_bucket(
        &cairn_meta_async::open_libsql_in_memory().await.unwrap(),
    )
    .await;
    cairn_types::testing::assert_unadmitted_object_publication(
        &cairn_meta_async::open_libsql_in_memory().await.unwrap(),
    )
    .await;
    cairn_types::testing::assert_unadmitted_abandonment_reclaims_aliases(
        &cairn_meta_async::open_libsql_in_memory().await.unwrap(),
    )
    .await;
}

#[tokio::test]
async fn turso_object_publication_contract() {
    cairn_types::testing::assert_object_publication_rejects_recreated_bucket(
        &cairn_meta_async::open_turso_in_memory().await.unwrap(),
    )
    .await;
    cairn_types::testing::assert_unadmitted_object_publication(
        &cairn_meta_async::open_turso_in_memory().await.unwrap(),
    )
    .await;
    cairn_types::testing::assert_unadmitted_abandonment_reclaims_aliases(
        &cairn_meta_async::open_turso_in_memory().await.unwrap(),
    )
    .await;
}
