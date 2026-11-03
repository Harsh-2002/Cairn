//! Offline backfill of missing protocol-3 metadata sidecars for assembled multipart objects.
//!
//! The command is dispatched by `main` after it has acquired the node lock, so no live writer can
//! supersede a version while its sidecar is staged. It never changes an index row.

use crate::{config::Config, node_lock, stack};
use cairn_types::object::ObjectLockState;
use cairn_types::storage::io::StorageIoWatch;
use cairn_types::storage::{
    ObjectMetadataRecord, PlannedStorageWrite, StorageToken, StorageWriteTarget,
    object_metadata_path,
};
use cairn_types::traits::Clock;
use cairn_types::{ExplicitObjectLockIntent, ListQuery, Timestamp};
use std::process::ExitCode;
use std::sync::Arc;

fn lock_intent(state: ObjectLockState, now: Timestamp) -> ExplicitObjectLockIntent {
    ExplicitObjectLockIntent {
        retention: state
            .retention
            .filter(|retention| retention.retain_until > now),
        legal_hold: state.legal_hold.then_some(true),
    }
}

/// Write the durable metadata sidecar for assembled multipart objects that predate it.
///
/// Offline by construction: `main` acquires the node lock for every node-local command, so no live
/// writer can supersede a version while its sidecar is staged. Only composite-ETag versions with a
/// flat `bucket/uuid` data path are candidates; a nested laboratory path is counted and skipped
/// because its parent directory is outside the permit's bucket directory.
pub(crate) fn run(
    cfg: Config,
    dry_run: bool,
    limit: Option<usize>,
    node_lock: Arc<node_lock::NodeLock>,
) -> ExitCode {
    let rt = match crate::runtime(&cfg) {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("failed to start runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        let (meta, _oracle) = match stack::open_meta_store(&cfg).await {
            Ok(pair) => pair,
            Err(error) => {
                eprintln!("failed to open metadata store: {error}");
                return ExitCode::FAILURE;
            }
        };
        let blob = match cairn_blob::LocalBlobStore::open(
            cfg.data_dir.clone(),
            stack::maintenance_lease(&StorageToken::generate(), node_lock.clone()),
        )
        .await
        {
            Ok(blob) => blob,
            Err(error) => {
                eprintln!("failed to open blob store: {error}");
                return ExitCode::FAILURE;
            }
        };
        let buckets = match meta.list_buckets(None).await {
            Ok(buckets) => buckets,
            Err(error) => {
                eprintln!("failed to list buckets: {error}");
                return ExitCode::FAILURE;
            }
        };
        let now = cairn_crypto::SystemClock::new().now();
        let mut scanned = 0u64;
        let mut candidates = 0u64;
        let mut written = 0u64;
        let mut existing = 0u64;
        let mut missing_data = 0u64;
        let mut nested = 0u64;
        let mut failed = 0u64;
        let mut limit_hit = false;
        'buckets: for bucket in buckets {
            let mut cursor = None;
            let mut version_marker = None;
            loop {
                let page = match meta
                    .list_versions(
                        &bucket.name,
                        &ListQuery {
                            prefix: None,
                            delimiter: None,
                            cursor: cursor.clone(),
                            version_id_marker: version_marker.clone(),
                            start_after: None,
                            limit: 1_000,
                            visible_at: None,
                        },
                    )
                    .await
                {
                    Ok(page) => page,
                    Err(error) => {
                        eprintln!(
                            "failed to list versions in bucket {}: {error}",
                            bucket.name.as_str()
                        );
                        return ExitCode::FAILURE;
                    }
                };
                for summary in page.items {
                    scanned += 1;
                    if summary.is_delete_marker || !summary.etag.is_multipart() {
                        continue;
                    }
                    let row = match meta
                        .get_version(&bucket.name, &summary.key, &summary.version_id)
                        .await
                    {
                        Ok(Some(row)) => row,
                        Ok(None) => continue,
                        Err(error) => {
                            failed += 1;
                            eprintln!(
                                "cannot read {}/{} version {}: {error}",
                                bucket.name.as_str(),
                                summary.key.as_str(),
                                summary.version_id.as_str()
                            );
                            continue;
                        }
                    };
                    if row.cold_locator.is_some() {
                        missing_data += 1;
                        continue;
                    }
                    let Some(data_path) = row.storage_path.clone() else {
                        missing_data += 1;
                        continue;
                    };
                    let Some(sidecar) = object_metadata_path(&data_path) else {
                        failed += 1;
                        eprintln!(
                            "object {} has no derivable sidecar path",
                            data_path.as_str()
                        );
                        continue;
                    };
                    match std::fs::symlink_metadata(cfg.data_dir.join(sidecar.as_str())) {
                        Ok(_) => {
                            existing += 1;
                            continue;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => {
                            failed += 1;
                            eprintln!(
                                "cannot inspect sidecar {}: {error}",
                                sidecar.as_str()
                            );
                            continue;
                        }
                    }
                    match std::fs::symlink_metadata(cfg.data_dir.join(data_path.as_str())) {
                        Ok(metadata) if metadata.file_type().is_file() => {}
                        _ => {
                            missing_data += 1;
                            continue;
                        }
                    }
                    if data_path.as_str().split('/').count() != 2 {
                        nested += 1;
                        continue;
                    }
                    if limit.is_some_and(|limit| candidates >= limit as u64) {
                        limit_hit = true;
                        break 'buckets;
                    }
                    candidates += 1;
                    if dry_run {
                        continue;
                    }
                    let tags = match meta
                        .get_object_tags(
                            &bucket.name,
                            &summary.key,
                            &summary.version_id,
                        )
                        .await
                    {
                        Ok(tags) => tags,
                        Err(error) => {
                            failed += 1;
                            eprintln!(
                                "cannot read tags for {}/{}: {error}",
                                bucket.name.as_str(),
                                summary.key.as_str()
                            );
                            continue;
                        }
                    };
                    let state = match meta
                        .get_object_lock(
                            &bucket.name,
                            &summary.key,
                            &summary.version_id,
                        )
                        .await
                    {
                        Ok(state) => state,
                        Err(error) => {
                            failed += 1;
                            eprintln!(
                                "cannot read Object Lock for {}/{}: {error}",
                                bucket.name.as_str(),
                                summary.key.as_str()
                            );
                            continue;
                        }
                    };
                    let record = match ObjectMetadataRecord::from_existing(
                        &row,
                        tags,
                        lock_intent(state, now),
                    ) {
                        Ok(record) => record,
                        Err(error) => {
                            failed += 1;
                            eprintln!(
                                "cannot build metadata for {}/{}: {error}",
                                bucket.name.as_str(),
                                summary.key.as_str()
                            );
                            continue;
                        }
                    };
                    let generation = StorageToken::generate();
                    let planned = match PlannedStorageWrite::new(
                        bucket.name.clone(),
                        generation.clone(),
                        StorageWriteTarget::Object {
                            key: row.key.clone(),
                            version_id: row.version_id.clone(),
                            // The permit only opens the bucket namespace; the sidecar record keeps
                            // the real row id. A pre-protocol-3 row id need not be a 32-hex token.
                            row_id: StorageToken::generate().as_str().to_owned(),
                        },
                    ) {
                        Ok(planned) => planned,
                        Err(error) => {
                            failed += 1;
                            eprintln!(
                                "cannot plan sidecar for {}/{}: {error}",
                                bucket.name.as_str(),
                                summary.key.as_str()
                            );
                            continue;
                        }
                    };
                    let attempt = planned.plan().attempt.clone();
                    let lifetime: Arc<dyn Send + Sync> = node_lock.clone();
                    let (_watch, lease) = StorageIoWatch::new(attempt, generation, lifetime);
                    let permit = match planned.into_object_permit(lease) {
                        Ok(permit) => permit,
                        Err(error) => {
                            failed += 1;
                            eprintln!(
                                "cannot admit sidecar for {}/{}: {error}",
                                bucket.name.as_str(),
                                summary.key.as_str()
                            );
                            continue;
                        }
                    };
                    match blob
                        .backfill_metadata_sidecar(permit, &data_path, &record)
                        .await
                    {
                        Ok(true) => written += 1,
                        Ok(false) => existing += 1,
                        Err(error) => {
                            failed += 1;
                            eprintln!(
                                "cannot write sidecar for {}: {error}",
                                data_path.as_str()
                            );
                        }
                    }
                }
                if !page.truncated {
                    break;
                }
                cursor = page.next_cursor;
                version_marker = page.next_version_id_marker;
                if cursor.is_none() {
                    break;
                }
            }
        }
        println!(
            "sidecar backfill: scanned={scanned} candidates={candidates} written={written} existing={existing} missing_data={missing_data} nested={nested} failed={failed}{}{}",
            if dry_run { " dry_run=true" } else { "" },
            if limit_hit {
                " limit_reached=true (rerun to continue)"
            } else {
                ""
            }
        );
        if failed == 0 {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::object::{ObjectLockMode, ObjectRetention};

    #[test]
    fn only_active_retention_and_legal_hold_become_explicit_intent() {
        let now = Timestamp(10);
        let retention = ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until: Timestamp(11),
        };
        assert_eq!(
            lock_intent(
                ObjectLockState {
                    retention: Some(retention),
                    legal_hold: true,
                },
                now,
            ),
            ExplicitObjectLockIntent {
                retention: Some(retention),
                legal_hold: Some(true),
            }
        );
        // An expired side row is not pinned as explicit intent: it would be ineffective anyway and
        // could be rejected as a non-future retention on rebuild.
        assert_eq!(
            lock_intent(
                ObjectLockState {
                    retention: Some(ObjectRetention {
                        retain_until: Timestamp(9),
                        ..retention
                    }),
                    legal_hold: false,
                },
                now,
            ),
            ExplicitObjectLockIntent::default()
        );
    }
}
