//! Isolated conditional Fjall laboratory; no production engine registration.
mod actor;
mod apply;
mod facade;
mod journal;
mod kv;
mod model;
mod multipart;
mod outbox;
mod reads;
pub use actor::Store;

#[cfg(test)]
mod tests {
    use super::actor::Store;
    use crate::metadata_workload::{self as workload, Family};
    use cairn_types::{testing::PublicationFixture, *};
    use futures_util::{StreamExt, stream};
    use sha2::{Digest, Sha256};
    use std::{collections::BTreeSet, time::Duration};
    use tokio::time::Instant;

    async fn snapshot_without_quota(store: &Store) -> [u8; 32] {
        let mut digest = Sha256::new();
        for (key, value) in tiny_snapshot(store).await {
            if key.first() == Some(&super::model::QUOTA) {
                continue;
            }
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        }
        digest.finalize().into()
    }

    /// Called only by the bounded parent fixture, with an inherited ignored SIGXFSZ for EFBIG.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "owned subprocess helper for journal I/O and interrupted-commit fixtures"]
    async fn failure_child() {
        use std::io::Write;
        let directory = std::env::var_os("CAIRN_LAB_FJALL_CHILD_DIR").expect("child directory");
        let mode: u8 = std::env::var("CAIRN_LAB_FJALL_CHILD_MODE")
            .unwrap()
            .parse()
            .unwrap();
        let store = Store::open(std::path::Path::new(&directory)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        let fixture = workload::prepare(store.clone(), 1, 10, 99, deadline)
            .await
            .unwrap();
        fixture.verify(deadline).await.unwrap();
        store.checkpoint().await.unwrap();
        let bucket = BucketName::parse("capacity-00").unwrap();
        println!(
            "BASELINE:{}",
            serde_json::json!({"hash":snapshot_without_quota(&store).await,
            "quota":store.get_bucket_quota(&bucket).await.unwrap()})
        );
        std::io::stdout().flush().unwrap();
        if mode == 0 {
            rustix::process::setrlimit(
                rustix::process::Resource::Fsize,
                rustix::process::Rlimit {
                    current: Some(0),
                    maximum: Some(0),
                },
            )
            .unwrap();
        } else {
            super::actor::CRASH_POINT.store(mode, std::sync::atomic::Ordering::SeqCst);
        }
        assert!(
            store
                .submit(Mutation::SetBucketQuota {
                    bucket,
                    quota_bytes: Some(1_000_000)
                })
                .await
                .is_err(),
            "journal I/O failure must never acknowledge a mutation"
        );
        assert_eq!(mode, 0, "crash point did not terminate child");
        drop(fixture);
        let error = store.close().await.unwrap_err().to_string();
        assert!(
            error.contains("27") || error.contains("File too large"),
            "expected native EFBIG, received {error}"
        );
        println!("NATIVE_EFBIG_REFUSED");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_journal_error_and_commit_crashes_preserve_atomic_durable_state() {
        use std::{
            io::Read,
            os::unix::process::ExitStatusExt,
            process::{Command, Stdio},
        };
        for mode in 0..=2 {
            let directory = tempfile::tempdir().unwrap();
            let mut child = Command::new("sh")
                .args(["-c", "trap '' XFSZ; exec \"$@\"", "fjall-fault-child"])
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "metadata_fjall::tests::failure_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("CAIRN_LAB_FJALL_CHILD_DIR", directory.path())
                .env("CAIRN_LAB_FJALL_CHILD_MODE", mode.to_string())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            let status = loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                if std::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("owned fault child exceeded deadline");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            let mut output = String::new();
            child
                .stdout
                .take()
                .unwrap()
                .take(32_768)
                .read_to_string(&mut output)
                .unwrap();
            let mut errors = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .take(32_768)
                .read_to_string(&mut errors)
                .unwrap();
            if mode == 0 {
                assert!(
                    status.success() && output.contains("NATIVE_EFBIG_REFUSED"),
                    "{status}: {output} {errors}"
                );
            } else {
                assert_eq!(status.signal(), Some(9), "{output} {errors}");
            }
            let baseline: serde_json::Value = serde_json::from_str(
                output
                    .lines()
                    .find_map(|line| line.strip_prefix("BASELINE:"))
                    .expect("child baseline"),
            )
            .unwrap();
            let reopened = Store::open(directory.path()).unwrap();
            assert_eq!(
                serde_json::json!(snapshot_without_quota(&reopened).await),
                baseline["hash"]
            );
            let quota = reopened
                .get_bucket_quota(&BucketName::parse("capacity-00").unwrap())
                .await
                .unwrap();
            assert_eq!(
                serde_json::json!(quota),
                if mode == 2 {
                    serde_json::json!(1_000_000)
                } else {
                    baseline["quota"].clone()
                }
            );
            reopened.close().await.unwrap();
        }
    }

    async fn tiny_snapshot(store: &Store) -> std::collections::BTreeMap<Vec<u8>, Vec<u8>> {
        store
            .read(|view| {
                let mut rows = std::collections::BTreeMap::new();
                for table in 1..=34 {
                    let mut after = None;
                    loop {
                        let page = view.scan(&[table], after.as_deref(), super::kv::PAGE)?;
                        if page.is_empty() {
                            break;
                        }
                        after = page.last().map(|(key, _)| key.clone());
                        rows.extend(page);
                        assert!(rows.len() < 2000, "tiny fixture must remain bounded");
                    }
                }
                Ok(rows)
            })
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn late_outbox_failure_rolls_back_every_persisted_index_and_counter() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let fixture = workload::prepare(store.clone(), 1, 10, 91, deadline)
            .await
            .unwrap();
        let bucket = BucketName::parse("capacity-00").unwrap();
        let summary = store
            .list_current(
                &bucket,
                &ListQuery {
                    limit: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .items
            .remove(0);
        let mut row = store
            .get_version(&bucket, &summary.key, &summary.version_id)
            .await
            .unwrap()
            .unwrap();
        let publication = PublicationFixture::new();
        publication.begin(store.as_ref()).await.unwrap();
        row.id = cairn_types::storage::StorageToken::generate()
            .as_str()
            .to_owned();
        row.key = ObjectKey::parse("late-failure").unwrap();
        row.version_id = VersionId::from_string("00000000-0000-7000-8000-000000000001".into());
        row.storage_path = Some(StoragePath::from_string(format!(
            "{}/{}",
            bucket.as_str(),
            cairn_types::storage::StorageToken::generate().as_str()
        )));
        let entry = OutboxEntry {
            claim_token: None,
            id: "accepted-before-late-failure".into(),
            bucket: bucket.clone(),
            key: row.key.clone(),
            version_id: row.version_id.clone(),
            operation: ReplicationOp::ObjectCreate,
            rule_id: "test".into(),
            target_arn: None,
            attempts: 0,
            next_attempt_at: row.updated_at,
            status: ReplicationStatus::Pending,
            last_error: None,
            priority: 0,
            lease_until: None,
            enqueued_at: row.updated_at,
        };
        let mut invalid = entry.clone();
        invalid.id = "late-invalid-bucket".into();
        invalid.bucket = BucketName::parse("absent-bucket").unwrap();
        let mutation = publication
            .prepare_put(
                store.as_ref(),
                Mutation::PutObjectVersion {
                    row: Box::new(row.clone()),
                    precondition: Precondition::default(),
                    initial_state: InitialObjectState::default(),
                    replication: vec![entry, invalid],
                },
            )
            .await
            .unwrap();
        let before = tiny_snapshot(&store).await;
        assert!(store.submit(mutation).await.is_err());
        assert_eq!(tiny_snapshot(&store).await, before);
        assert!(
            store
                .current_version(&bucket, &row.key)
                .await
                .unwrap()
                .is_none()
        );
        drop(fixture);
        store.close().await.unwrap();
        let reopened = Store::open(directory.path()).unwrap();
        assert_eq!(tiny_snapshot(&reopened).await, before);
        reopened.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn populated_trace_has_all_families_and_fresh_reopen() {
        for buckets in [1, 16] {
            let directory = tempfile::tempdir().unwrap();
            let store = Store::open(directory.path()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(60);
            let fixture = workload::prepare(store.clone(), buckets, 100, 42, deadline)
                .await
                .unwrap();
            let mut seen = BTreeSet::new();
            for sequence in 0..10 {
                let result = fixture.operation(0, sequence, deadline).await.unwrap();
                for observation in result.observations {
                    assert_eq!(
                        observation.rejected,
                        observation.family == Family::ConditionalReject
                    );
                    seen.insert(observation.family);
                }
            }
            assert_eq!(seen, Family::ALL.into_iter().collect());
            assert_eq!(fixture.verify(deadline).await.unwrap(), fixture.expected());
            let detached = fixture.detach();
            assert!(store.stages().mutations > 100);
            store.close().await.unwrap();
            let reopened = Store::open(directory.path()).unwrap();
            let fixture = detached.attach(reopened.clone());
            assert_eq!(fixture.verify(deadline).await.unwrap(), fixture.expected());
            drop(fixture);
            reopened.close().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_128_owners_preserve_seed_and_accounting() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(90);
        let fixture = workload::prepare(store.clone(), 16, 100, 71, deadline)
            .await
            .unwrap();
        let results = stream::iter(0..128)
            .map(|worker| {
                let fixture = &fixture;
                async move {
                    for sequence in 0..5 {
                        fixture.operation(worker, sequence, deadline).await?;
                    }
                    Ok::<(), String>(())
                }
            })
            .buffer_unordered(128)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            result.unwrap();
        }
        assert_eq!(fixture.verify(deadline).await.unwrap(), fixture.expected());
        let detached = fixture.detach();
        store.close().await.unwrap();
        let reopened = Store::open(directory.path()).unwrap();
        let fixture = detached.attach(reopened.clone());
        assert_eq!(fixture.verify(deadline).await.unwrap(), fixture.expected());
        drop(fixture);
        reopened.close().await.unwrap();
    }
}
