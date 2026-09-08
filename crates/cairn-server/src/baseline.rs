//! Explicit offline coverage. A failed or interrupted run retains its accounting hold; ordinary
//! startup never substitutes a destructive full scan for resuming this command.

use crate::{Config, node_lock::NodeLock, stack};
use cairn_types::blob::StorageBaselineOptions;
use cairn_types::storage::StorageToken;
use cairn_types::storage_baseline::{StorageBaselineToken, StorageBaselineTransition};
use cairn_types::{BlobStore, Clock, MetadataStore, Mutation, MutationOutcome};
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

pub(crate) fn run(cfg: Config, backup: &Path, node: Arc<NodeLock>) -> ExitCode {
    if let Err(error) = crate::require_canonical_backup_topology(&cfg) {
        eprintln!("storage baseline refused: {error}");
        return ExitCode::from(2);
    }
    let runtime = match crate::runtime(&cfg) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to start storage baseline runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(execute(&cfg, backup, node)) {
        Ok(()) => {
            println!("storage baseline complete; full startup scans remain required");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("storage baseline incomplete: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(cfg: &Config, backup: &Path, node: Arc<NodeLock>) -> Result<(), String> {
    // The snapshot helper closes its source Writer on every path. No new generation or baseline
    // hold begins until the new image, manifest and copied authoritative references validate.
    let (snapshot, _) = crate::create_safety_snapshot(cfg, backup).await?;
    println!(
        "safety snapshot validated: {} referenced files checked at {}; no restore drill was run",
        snapshot.referenced_files,
        backup.display()
    );
    let store = cairn_meta::open(
        &cfg.db_path,
        &cairn_meta::OpenOptions {
            synchronous_full: true,
            read_pool_size: 1,
            ..Default::default()
        },
    )
    .map_err(|error| format!("open storage baseline Writer: {error}"))?;
    let result = establish(cfg, &store, node.clone()).await;
    let closed = store
        .checkpoint_and_close()
        .await
        .map_err(|error| format!("close storage baseline Writer: {error}"));
    result?;
    if closed?.busy {
        return Err("storage baseline WAL remains busy".into());
    }
    Ok(())
}

async fn establish(
    cfg: &Config,
    store: &cairn_meta::SqliteMetadataStore,
    node: Arc<NodeLock>,
) -> Result<(), String> {
    let crypto = stack::build_crypto(cfg)?;
    stack::preflight_key_state(&[store], &crypto, cfg).await?;
    let token = StorageBaselineToken {
        generation: StorageToken::generate(),
        baseline_id: StorageToken::generate(),
    };
    // This is the explicit resume path. The ordinary generation helper refuses any hold; this
    // Writer transition instead preserves it and invalidates authorization from an earlier run.
    match store
        .submit(Mutation::BeginStorageGeneration {
            generation: token.generation.clone(),
        })
        .await
        .map_err(|error| format!("begin baseline generation: {error}"))?
    {
        MutationOutcome::Ack => {}
        _ => return Err("unexpected baseline generation acknowledgement".into()),
    }
    require_transition(
        store
            .submit(Mutation::BeginStorageBaseline {
                token: token.clone(),
            })
            .await
            .map_err(|error| format!("begin storage baseline hold: {error}"))?,
    )?;
    stack::require_baseline_ownership(store, &token).await?;

    let options = baseline_options(cfg, &node, &token)?;
    let blob = cairn_blob::LocalBlobStore::open(
        &cfg.data_dir,
        stack::maintenance_lease(&token.generation, node.clone()),
    )
    .await
    .map_err(|error| format!("open baseline blob namespace: {error}"))?;

    // Complete the database-only classification first. A later unknown file cannot turn a
    // partially walked tree into authority for deleting earlier files or releasing legacy quota.
    let classification = blob
        .classify_storage_baseline(
            store,
            &options,
            stack::maintenance_lease(&token.generation, node.clone()),
        )
        .await
        .map_err(|error| format!("classify storage baseline: {error}"))?;
    stack::recover_baseline_storage(store, &blob, &token, node.clone()).await?;
    stack::drain_baseline_storage_cleanup(store, &blob, &token, node.clone()).await?;
    let proof = blob
        .verify_storage_baseline(
            store,
            &options,
            &classification,
            stack::maintenance_lease(&token.generation, node.clone()),
        )
        .await
        .map_err(|error| format!("verify storage baseline: {error}"))?;
    stack::require_baseline_ownership(store, &token).await?;
    require_transition(
        store
            .submit(Mutation::AuthorizeStorageBaselineRelease { proof })
            .await
            .map_err(|error| format!("authorize storage baseline legacy release: {error}"))?,
    )?;
    loop {
        stack::require_baseline_ownership(store, &token).await?;
        match store
            .submit(Mutation::FinalizeStorageBaselineLegacy {
                token: token.clone(),
                limit: 128,
            })
            .await
            .map_err(|error| format!("finalize storage baseline legacy accounting: {error}"))?
        {
            MutationOutcome::StorageBaselineLegacyPage {
                remaining: false, ..
            } => break,
            MutationOutcome::StorageBaselineLegacyPage {
                released,
                remaining: true,
            } if released > 0 => {}
            outcome => {
                return Err(format!(
                    "storage baseline legacy accounting did not advance: {outcome:?}"
                ));
            }
        }
    }
    require_transition(
        store
            .submit(Mutation::CompleteStorageBaseline {
                token,
                completed_at: cairn_crypto::SystemClock::new().now(),
            })
            .await
            .map_err(|error| format!("complete storage baseline: {error}"))?,
    )
}

fn baseline_options(
    cfg: &Config,
    node: &NodeLock,
    token: &StorageBaselineToken,
) -> Result<StorageBaselineOptions, String> {
    let mut root_artifacts = crate::database_artifact_names(&cfg.data_dir, &cfg.db_path)
        .map_err(|error| format!("identify baseline database artifacts: {error}"))?;
    root_artifacts.extend(
        node.root_artifact_names(&cfg.data_dir)
            .map_err(|error| format!("identify retained baseline locks: {error}"))?,
    );
    root_artifacts.sort();
    root_artifacts.dedup();
    Ok(StorageBaselineOptions {
        token: token.clone(),
        batch_size: 128,
        root_artifacts,
    })
}

fn require_transition(outcome: MutationOutcome) -> Result<(), String> {
    match outcome {
        MutationOutcome::StorageBaselineUpdated(
            StorageBaselineTransition::Applied | StorageBaselineTransition::AlreadyApplied,
        ) => Ok(()),
        outcome => Err(format!(
            "storage baseline transition was refused: {outcome:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::{BucketName, StoragePath};

    async fn fixture(root: &Path) -> (Config, Arc<NodeLock>) {
        let cfg = Config {
            data_dir: root.join("data"),
            db_path: root.join("data/metadata.db"),
            ..Config::default()
        };
        let node = Arc::new(NodeLock::acquire(&cfg.data_dir, &cfg.db_path).unwrap());
        cairn_meta::open(&cfg.db_path, &Default::default())
            .unwrap()
            .checkpoint_and_close()
            .await
            .unwrap();
        (cfg, node)
    }

    async fn states(cfg: &Config) -> Vec<cairn_types::storage_baseline::StorageBaselineState> {
        let store = cairn_meta::open(&cfg.db_path, &Default::default()).unwrap();
        let states = store.storage_baseline_states().await.unwrap();
        store.checkpoint_and_close().await.unwrap();
        states
    }

    #[test]
    fn command_requires_a_snapshot_destination() {
        use clap::Parser;
        assert!(crate::Cli::try_parse_from(["cairn", "storage-baseline"]).is_err());
        let cli = crate::Cli::try_parse_from(["cairn", "storage-baseline", "/backup/new"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(crate::Command::StorageBaseline { dir }) if dir == Path::new("/backup/new")
        ));
    }

    #[test]
    fn baseline_rejects_noncanonical_topology_before_snapshot_preparation() {
        let root = tempfile::tempdir().unwrap();
        let node = Arc::new(
            NodeLock::acquire(
                &root.path().join("guard-data"),
                &root.path().join("guard.db"),
            )
            .unwrap(),
        );
        for (backend, shards) in [("sqlite", 2), ("libsql", 1), ("turso", 1)] {
            let cfg = Config {
                meta_backend: backend.into(),
                meta_shards: shards,
                data_dir: root.path().join("untouched-data"),
                db_path: root.path().join("untouched.db"),
                ..Config::default()
            };
            let backup = root.path().join("untouched-backup");
            assert_eq!(run(cfg.clone(), &backup, node.clone()), ExitCode::from(2));
            assert!(!backup.exists());
            assert!(!cfg.data_dir.exists());
            assert!(!cfg.db_path.exists());
        }
    }

    #[tokio::test]
    async fn baseline_snapshot_failure_cannot_begin_or_replace_a_hold() {
        let root = tempfile::tempdir().unwrap();
        let (cfg, node) = fixture(root.path()).await;
        let snapshot = root.path().join("occupied");
        std::fs::create_dir(&snapshot).unwrap();
        std::fs::write(snapshot.join("keep"), b"previous generation").unwrap();
        for held in [false, true] {
            if held {
                let store = cairn_meta::open(&cfg.db_path, &Default::default()).unwrap();
                let token = StorageBaselineToken {
                    generation: stack::begin_storage_generation(&store).await.unwrap(),
                    baseline_id: StorageToken::generate(),
                };
                require_transition(
                    store
                        .submit(Mutation::BeginStorageBaseline { token })
                        .await
                        .unwrap(),
                )
                .unwrap();
                store.checkpoint_and_close().await.unwrap();
            }
            let before = states(&cfg).await;
            assert!(execute(&cfg, &snapshot, node.clone()).await.is_err());
            assert_eq!(states(&cfg).await, before);
            assert_eq!(
                std::fs::read(snapshot.join("keep")).unwrap(),
                b"previous generation"
            );
        }
    }

    #[tokio::test]
    async fn baseline_interruption_preserves_files_and_requires_a_fresh_snapshot_to_resume() {
        let root = tempfile::tempdir().unwrap();
        let (cfg, node) = fixture(root.path()).await;
        let bucket = BucketName::parse("legacy-orphans").unwrap();
        let orphan = StoragePath::generate(&bucket);
        let orphan_file = cfg.data_dir.join(orphan.as_str());
        std::fs::create_dir(orphan_file.parent().unwrap()).unwrap();
        std::fs::write(&orphan_file, b"unreferenced legacy bytes").unwrap();
        // Native backup recognizes the old broad lock suffix; strict coverage must use only the
        // actual retained lock names and refuse this unknown file without deleting earlier work.
        let unknown = cfg.data_dir.join(".unexpected.cairn-db.lock");
        std::fs::write(&unknown, b"unclassified").unwrap();
        let first_backup = root.path().join("first-backup");
        let error = execute(&cfg, &first_backup, node.clone())
            .await
            .unwrap_err();
        assert!(error.contains("classify storage baseline"), "{error}");
        crate::validate_snapshot(&first_backup).await.unwrap();
        let held = states(&cfg).await;
        assert!(held[0].legacy_accounting_hold);
        assert!(!held[0].legacy_release_authorized);
        assert!(orphan_file.exists());
        assert!(unknown.exists());
        let store = cairn_meta::open(&cfg.db_path, &Default::default()).unwrap();
        let error = stack::begin_storage_generation(&store).await.unwrap_err();
        assert!(error.contains("storage-baseline"), "{error}");
        assert_eq!(store.storage_baseline_states().await.unwrap(), held);
        store.checkpoint_and_close().await.unwrap();

        std::fs::remove_file(&unknown).unwrap();
        assert!(execute(&cfg, &first_backup, node.clone()).await.is_err());
        assert_eq!(states(&cfg).await, held);
        let second_backup = root.path().join("second-backup");
        execute(&cfg, &second_backup, node.clone()).await.unwrap();
        assert!(!orphan_file.exists());
        assert!(first_backup.join("blobs").join(orphan.as_str()).exists());
        assert!(second_backup.join("blobs").join(orphan.as_str()).exists());
        let completed = states(&cfg).await;
        assert!(!completed[0].legacy_accounting_hold);
        assert!(!completed[0].legacy_release_authorized);
        assert!(completed[0].coverage_identity.is_some());
        assert!(completed[0].completed_at.is_some());
        assert_ne!(completed[0].generation, held[0].generation);
        let store = cairn_meta::open(&cfg.db_path, &Default::default()).unwrap();
        assert!(!store.storage_baseline_pending().await.unwrap().any());
        stack::begin_storage_generation(&store).await.unwrap();
        store.checkpoint_and_close().await.unwrap();
    }

    #[tokio::test]
    async fn baseline_key_mismatch_refuses_before_changing_generation_or_hold() {
        use sha2::{Digest, Sha256};
        let root = tempfile::tempdir().unwrap();
        let (cfg, node) = fixture(root.path()).await;
        let store = cairn_meta::open(&cfg.db_path, &Default::default()).unwrap();
        store
            .key_ring_apply_config(vec![(1, hex::encode(Sha256::digest([1u8; 32])), true)], 11)
            .await
            .unwrap();
        store.key_ring_sync_seal_count(1, 73).await.unwrap();
        store.checkpoint_and_close().await.unwrap();
        let before = states(&cfg).await;
        let backup = root.path().join("snapshot");
        let error = execute(&cfg, &backup, node).await.unwrap_err();
        assert!(error.contains("same-id replacement"), "{error}");
        crate::validate_snapshot(&backup).await.unwrap();
        assert_eq!(states(&cfg).await, before);
    }

    #[tokio::test]
    async fn authorized_interruption_is_held_and_restore_invalidates_its_release_proof() {
        let root = tempfile::tempdir().unwrap();
        let (cfg, node) = fixture(root.path()).await;
        let store = cairn_meta::open(&cfg.db_path, &Default::default()).unwrap();
        let token = StorageBaselineToken {
            generation: stack::begin_storage_generation(&store).await.unwrap(),
            baseline_id: StorageToken::generate(),
        };
        require_transition(
            store
                .submit(Mutation::BeginStorageBaseline {
                    token: token.clone(),
                })
                .await
                .unwrap(),
        )
        .unwrap();
        let options = baseline_options(&cfg, &node, &token).unwrap();
        let blob = cairn_blob::LocalBlobStore::open(
            &cfg.data_dir,
            stack::maintenance_lease(&token.generation, node.clone()),
        )
        .await
        .unwrap();
        let classification = blob
            .classify_storage_baseline(
                &store,
                &options,
                stack::maintenance_lease(&token.generation, node.clone()),
            )
            .await
            .unwrap();
        let proof = blob
            .verify_storage_baseline(
                &store,
                &options,
                &classification,
                stack::maintenance_lease(&token.generation, node.clone()),
            )
            .await
            .unwrap();
        require_transition(
            store
                .submit(Mutation::AuthorizeStorageBaselineRelease { proof })
                .await
                .unwrap(),
        )
        .unwrap();
        let authorized = store.storage_baseline_states().await.unwrap();
        assert!(authorized[0].legacy_accounting_hold);
        assert!(authorized[0].legacy_release_authorized);
        assert!(stack::begin_storage_generation(&store).await.is_err());
        assert_eq!(store.storage_baseline_states().await.unwrap(), authorized);
        store.checkpoint_and_close().await.unwrap();
        drop(classification);
        drop(blob);

        let backup = root.path().join("resume-backup");
        execute(&cfg, &backup, node).await.unwrap();
        let completed = states(&cfg).await;
        assert!(!completed[0].legacy_accounting_hold);
        assert!(!completed[0].legacy_release_authorized);
        assert_ne!(completed[0].generation, authorized[0].generation);

        // The new safety snapshot captured the interrupted authorized state. Preparing its
        // restore must retain that hold/run while making its release authorization unusable.
        let snapshot = crate::validate_snapshot(&backup).await.unwrap();
        let target = root.path().join("held-restore.db");
        let staged =
            crate::stage_snapshot_database(&snapshot.database, &target, &snapshot.manifest)
                .await
                .unwrap();
        let prepared = crate::prepare_staged_database(staged, &cfg).await.unwrap();
        assert!(prepared.receipt.legacy_accounting_hold);
        assert_eq!(prepared.receipt.baseline_id, Some(token.baseline_id));
        let connection = crate::open_prepared_database(prepared.staged.path()).unwrap();
        let release: bool = connection
            .query_row(
                "SELECT legacy_release_authorized FROM storage_recovery_state WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!release);
        drop(connection);
    }
}
