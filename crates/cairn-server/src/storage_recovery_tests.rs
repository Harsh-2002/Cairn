//! Real filesystem and metadata integration for exclusive protocol-2 recovery.

use crate::{node_lock::NodeLock, stack};
use bytes::Bytes;
use cairn_blob::LocalBlobStore;
use cairn_types::storage::io::StorageIoWatch;
use cairn_types::storage::{StorageAdmission, StorageMutation, StorageToken, StorageWriteTarget};
use cairn_types::{
    BlobStore, Bucket, BucketName, MetadataStore, Mutation, MutationOutcome, ObjectKey,
    OwnershipMode, ReconcileOpts, ReconcileOracle, StageOptions, StoragePath, Timestamp, UserId,
    VersionId, VersioningState,
};
use std::sync::Arc;

async fn create_bucket(meta: &dyn MetadataStore, bucket: &BucketName) {
    meta.submit(Mutation::CreateBucket(Box::new(Bucket {
        name: bucket.clone(),
        owner_id: UserId::generate(),
        created_at: Timestamp(1),
        versioning: VersioningState::Unversioned,
        ownership_mode: OwnershipMode::BucketOwnerEnforced,
        region: "us-east-1".into(),
        compression: None,
    })))
    .await
    .unwrap();
}

async fn stage_unpublished(
    meta: &dyn MetadataStore,
    blob: &dyn BlobStore,
    bucket: &BucketName,
    generation: &StorageToken,
    lifetime: Arc<NodeLock>,
) -> (cairn_types::storage::StorageWritePlan, StorageIoWatch) {
    let planned = blob
        .plan_write(
            bucket.clone(),
            generation.clone(),
            StorageWriteTarget::Object {
                key: ObjectKey::parse("pending").unwrap(),
                version_id: VersionId::null(),
                row_id: StorageToken::generate().as_str().to_owned(),
            },
        )
        .unwrap();
    let plan = planned.plan().clone();
    let MutationOutcome::StorageAdmission(receipt @ StorageAdmission::Granted(_)) = meta
        .submit(Mutation::Storage {
            bucket: bucket.clone(),
            operation: StorageMutation::Reserve {
                plan: Box::new(plan.clone()),
                now: Timestamp(1),
            },
        })
        .await
        .unwrap()
    else {
        panic!("admission must commit before the fixture creates bytes");
    };
    let (watch, lease) = StorageIoWatch::new(plan.attempt.clone(), generation.clone(), lifetime);
    let permit = planned.admit(receipt, lease).unwrap();
    let staged = blob
        .stage(
            permit,
            Box::pin(futures_util::stream::once(async {
                Ok(Bytes::from_static(b"unpublished but durably staged"))
            })),
            StageOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(&staged.storage_path, plan.final_path().unwrap());
    (plan, watch)
}

async fn assert_full_scan_protects_journal(
    meta: &dyn MetadataStore,
    oracle: &dyn ReconcileOracle,
    bucket: BucketName,
) {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().join("data");
    let node = Arc::new(NodeLock::acquire(&root, &workspace.path().join("meta.db")).unwrap());
    let generation = stack::begin_storage_generation(meta).await.unwrap();
    let blob = LocalBlobStore::open(&root, stack::maintenance_lease(&generation, node.clone()))
        .await
        .unwrap()
        .with_io_uring(false);
    create_bucket(meta, &bucket).await;
    let (plan, mut watch) =
        stage_unpublished(meta, &blob, &bucket, &generation, node.clone()).await;
    let proof = watch.quiescent().await;
    let paths: Vec<_> = plan.paths.iter().map(|path| path.path.clone()).collect();
    // Model a crash in scratch's create/unlink window. Its name was already admitted.
    let spool = plan.paths.last().unwrap().path.clone();
    std::fs::write(root.join(spool.as_str()), b"unfinished index").unwrap();
    let legacy_object = StoragePath::generate(&bucket);
    std::fs::write(root.join(legacy_object.as_str()), b"legacy orphan").unwrap();
    let legacy_tmp = root.join(format!(
        ".staging/{}.tmp",
        StorageToken::generate().as_str()
    ));
    std::fs::write(&legacy_tmp, b"legacy staging orphan").unwrap();

    assert_eq!(
        oracle.live_blobs(&paths).await.unwrap(),
        vec![true; paths.len()]
    );
    let options = ReconcileOpts {
        batch_size: 1,
        parallelism: 2,
        staging_safety_margin_secs: 0,
    };
    let report = blob
        .reconcile(
            oracle,
            options,
            stack::maintenance_lease(&generation, node.clone()),
        )
        .await
        .unwrap();
    assert_eq!(report.errors, 0);
    assert_eq!(report.orphans_reclaimed, 1);
    assert_eq!(report.staging_cleaned, 1);
    assert!(!legacy_tmp.exists());
    assert!(root.join(plan.final_path().unwrap().as_str()).exists());
    assert!(root.join(spool.as_str()).exists());

    // Routing and ownership survive deletion of the parent bucket, including on another shard.
    meta.submit(Mutation::DeleteBucket(bucket.clone()))
        .await
        .unwrap();
    meta.submit(Mutation::Storage {
        bucket: bucket.clone(),
        operation: StorageMutation::Resolve { quiescence: proof },
    })
    .await
    .unwrap();
    let MutationOutcome::StorageCleanupBatch(cleanups) = meta
        .submit(Mutation::ClaimStorageCleanup {
            generation: generation.clone(),
            limit: 16,
            now: Timestamp(1),
            lease_secs: 60,
        })
        .await
        .unwrap()
    else {
        panic!("expected exact cleanup claims");
    };
    assert_eq!(cleanups.len(), 3);
    assert_eq!(
        oracle.live_blobs(&paths).await.unwrap(),
        vec![true; paths.len()]
    );
    let report = blob
        .reconcile(
            oracle,
            options,
            stack::maintenance_lease(&generation, node.clone()),
        )
        .await
        .unwrap();
    assert_eq!(report.errors, 0);
    assert_eq!(report.orphans_reclaimed + report.staging_cleaned, 0);
    assert!(root.join(plan.final_path().unwrap().as_str()).exists());
    assert!(root.join(spool.as_str()).exists());

    for cleanup in cleanups {
        let (_, lease) = StorageIoWatch::new(cleanup.id.clone(), generation.clone(), node.clone());
        blob.cleanup_storage(&cleanup, lease).await.unwrap();
        assert_eq!(
            meta.submit(Mutation::Storage {
                bucket: bucket.clone(),
                operation: StorageMutation::FinishCleanup {
                    cleanup,
                    now: Timestamp(2)
                },
            })
            .await
            .unwrap(),
            MutationOutcome::StorageUpdated { applied: true }
        );
    }
    assert_eq!(
        oracle.live_blobs(&paths).await.unwrap(),
        vec![false; paths.len()]
    );
    for path in paths {
        assert!(!root.join(path.as_str()).exists());
    }
}

#[tokio::test]
async fn sqlite_full_scan_protects_intents_and_claimed_debt() {
    let store = cairn_meta::open_in_memory().unwrap();
    assert_full_scan_protects_journal(
        &store,
        &store.reconcile_oracle(),
        BucketName::parse("journal-oracle").unwrap(),
    )
    .await;
}

#[cfg(feature = "meta-async")]
#[tokio::test]
async fn libsql_full_scan_protects_intents_and_claimed_debt() {
    let store = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    assert_full_scan_protects_journal(
        &store,
        &store.reconcile_oracle(),
        BucketName::parse("journal-oracle").unwrap(),
    )
    .await;
}

#[cfg(feature = "meta-async")]
#[tokio::test]
async fn turso_full_scan_protects_intents_and_claimed_debt() {
    let store = cairn_meta_async::open_turso_in_memory().await.unwrap();
    assert_full_scan_protects_journal(
        &store,
        &store.reconcile_oracle(),
        BucketName::parse("journal-oracle").unwrap(),
    )
    .await;
}

#[tokio::test]
async fn sharded_full_scan_finds_staging_ownership_on_another_shard() {
    let mut stores = Vec::new();
    let mut oracles: Vec<Box<dyn ReconcileOracle + Send + Sync>> = Vec::new();
    for _ in 0..4 {
        let store = Arc::new(cairn_meta::open_in_memory().unwrap());
        oracles.push(Box::new(store.reconcile_oracle()));
        stores.push(store as Arc<dyn MetadataStore>);
    }
    let staging_shard = cairn_meta::shard_for_bucket(".staging", 4);
    let bucket = (0..100)
        .map(|index| format!("journal-{index}"))
        .find(|name| cairn_meta::shard_for_bucket(name, 4) != staging_shard)
        .unwrap();
    assert_full_scan_protects_journal(
        &cairn_meta::ShardedMetadataStore::new(stores),
        &cairn_meta::ShardedReconcileOracle::new(oracles),
        BucketName::parse(&bucket).unwrap(),
    )
    .await;
}

#[tokio::test]
async fn any_shard_baseline_hold_refuses_recovery_before_changing_any_generation() {
    use cairn_types::storage_baseline::{StorageBaselineToken, StorageBaselineTransition};
    let first = Arc::new(cairn_meta::open_in_memory().unwrap());
    let held = Arc::new(cairn_meta::open_in_memory().unwrap());
    let meta = cairn_meta::ShardedMetadataStore::new(vec![first.clone(), held.clone()]);
    let generation = stack::begin_storage_generation(&meta).await.unwrap();
    let token = StorageBaselineToken {
        generation,
        baseline_id: StorageToken::generate(),
    };
    assert_eq!(
        held.submit(Mutation::BeginStorageBaseline { token })
            .await
            .unwrap(),
        MutationOutcome::StorageBaselineUpdated(StorageBaselineTransition::Applied)
    );
    let before = meta.storage_baseline_states().await.unwrap();
    assert!(!before[0].legacy_accounting_hold);
    assert!(before[1].legacy_accounting_hold);
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().join("data");
    let node = Arc::new(NodeLock::acquire(&root, &workspace.path().join("meta.db")).unwrap());
    let blob = LocalBlobStore::open(
        &root,
        stack::maintenance_lease(&StorageToken::generate(), node.clone()),
    )
    .await
    .unwrap();
    let orphan = root.join(format!(
        ".staging/{}.tmp",
        StorageToken::generate().as_str()
    ));
    std::fs::write(&orphan, b"held legacy bytes").unwrap();
    let error = stack::recover_exclusive_storage(&meta, &blob, node.clone())
        .await
        .unwrap_err();
    assert!(error.contains("storage-baseline"), "{error}");
    assert!(
        stack::finish_exclusive_storage_scan(
            &meta,
            &blob,
            before[0].generation.as_ref().unwrap(),
            node,
        )
        .await
        .is_err()
    );
    assert_eq!(meta.storage_baseline_states().await.unwrap(), before);
    assert_eq!(std::fs::read(&orphan).unwrap(), b"held legacy bytes");
}

#[tokio::test]
async fn held_startup_refuses_before_blob_initialization_and_generation() {
    use cairn_types::storage_baseline::StorageBaselineToken;
    let workspace = tempfile::tempdir().unwrap();
    let cfg = crate::Config {
        data_dir: workspace.path().join("data"),
        db_path: workspace.path().join("metadata.db"),
        ..crate::Config::default()
    };
    let node = Arc::new(NodeLock::acquire(&cfg.data_dir, &cfg.db_path).unwrap());
    let store = cairn_meta::open(&cfg.db_path, &Default::default()).unwrap();
    let generation = stack::begin_storage_generation(&store).await.unwrap();
    store
        .submit(Mutation::BeginStorageBaseline {
            token: StorageBaselineToken {
                generation,
                baseline_id: StorageToken::generate(),
            },
        })
        .await
        .unwrap();
    let before = store.storage_baseline_states().await.unwrap();
    store.checkpoint_and_close().await.unwrap();
    assert!(!cfg.data_dir.join(".staging").exists());
    let error = match stack::build(&cfg, node).await {
        Ok(_) => panic!("held startup must fail"),
        Err(error) => error,
    };
    assert!(error.contains("storage-baseline"), "{error}");
    assert!(!cfg.data_dir.join(".staging").exists());
    let store = cairn_meta::open(&cfg.db_path, &Default::default()).unwrap();
    assert_eq!(store.storage_baseline_states().await.unwrap(), before);
    store.checkpoint_and_close().await.unwrap();
}

#[tokio::test]
async fn exclusive_recovery_refuses_a_generation_with_an_outstanding_file_lock() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().join("data");
    let node = Arc::new(NodeLock::acquire(&root, &workspace.path().join("meta.db")).unwrap());
    let store = cairn_meta::open_in_memory().unwrap();
    let generation = stack::begin_storage_generation(&store).await.unwrap();
    let blob = LocalBlobStore::open(&root, stack::maintenance_lease(&generation, node.clone()))
        .await
        .unwrap()
        .with_io_uring(false);
    let bucket = BucketName::parse("restart-lock").unwrap();
    create_bucket(&store, &bucket).await;
    let (plan, mut watch) =
        stage_unpublished(&store, &blob, &bucket, &generation, node.clone()).await;
    let _proof = watch.quiescent().await;
    let path = root.join(plan.final_path().unwrap().as_str());
    // This deterministic fixture models a retained open-description reference. The separate
    // privileged io_uring process-kill regression establishes the actual kernel teardown case.
    let outstanding = cairn_blob::open_readonly_nofollow(&path).unwrap();
    cairn_blob::try_lock_exclusive(&outstanding).unwrap();
    assert!(
        stack::recover_exclusive_storage(&store, &blob, node.clone())
            .await
            .is_err()
    );
    assert!(path.exists());
    drop(outstanding);
    let recovered = stack::recover_exclusive_storage(&store, &blob, node.clone())
        .await
        .unwrap();
    assert_ne!(generation, recovered);
    assert!(
        path.exists(),
        "resolution records debt; it does not unlink inline"
    );
    stack::drain_exclusive_storage_cleanup(&store, &blob, &recovered, node)
        .await
        .unwrap();
    assert!(!path.exists());
}
