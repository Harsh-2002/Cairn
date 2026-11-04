//! Offline backfill of missing protocol-3 metadata sidecars for assembled multipart objects.
//!
//! The command is dispatched by `main` after it has acquired the node lock, so no live writer can
//! supersede a version while its sidecar is staged. It never changes an index row.

use crate::{config::Config, node_lock, stack};
use cairn_types::object::{ObjectLockState, ObjectVersionRow};
use cairn_types::storage::io::StorageIoWatch;
use cairn_types::storage::{
    OBJECT_METADATA_V1_FORMAT_VERSION, OBJECT_METADATA_V2_FORMAT_VERSION,
    OBJECT_METADATA_V3_FORMAT_VERSION, ObjectMetadataRecord, PlannedStorageWrite, StorageToken,
    StorageWriteTarget, object_metadata_path,
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

/// What the opt-in `--upgrade-v1` pass may do with an existing sidecar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingSidecar {
    /// A record the row already agrees with (normally v2): never rewrite it.
    Keep,
    /// A v1 record whose row has no part layout: there is nothing to upgrade.
    SkipV1,
    /// The sidecar and the index row disagree: leave the durable record untouched.
    Conflict,
    /// A v1 record that names the row exactly: replace it with the row's v2 record.
    Upgrade,
}

/// Classify an existing sidecar before the upgrade touches it.
///
/// The identity check is the safety gate: only a record that names the same row id, bucket, key,
/// version, storage path, ETag and sizes may be replaced. A mismatch is reported and skipped
/// rather than "fixed", because overwriting it could bless the wrong object.
fn classify_existing_sidecar(
    existing: &ObjectMetadataRecord,
    row: &ObjectVersionRow,
) -> ExistingSidecar {
    if !existing.matches_row_identity(row) {
        return ExistingSidecar::Conflict;
    }
    match existing.format_version {
        OBJECT_METADATA_V1_FORMAT_VERSION => {
            if row.multipart_parts.is_some() || row.internal_blake3.is_some() {
                ExistingSidecar::Upgrade
            } else {
                ExistingSidecar::SkipV1
            }
        }
        OBJECT_METADATA_V2_FORMAT_VERSION => {
            // A v2 record for a row that now carries the BLAKE3 baseline is upgraded to v3; the
            // row's layout must still match, or the record is not ours to rewrite.
            if existing.multipart_parts != row.multipart_parts {
                ExistingSidecar::Conflict
            } else if row.internal_blake3.is_some() {
                ExistingSidecar::Upgrade
            } else {
                ExistingSidecar::Keep
            }
        }
        OBJECT_METADATA_V3_FORMAT_VERSION => {
            if existing.multipart_parts == row.multipart_parts
                && existing.internal_blake3 == row.internal_blake3
            {
                ExistingSidecar::Keep
            } else {
                ExistingSidecar::Conflict
            }
        }
        _ => ExistingSidecar::Conflict,
    }
}

/// Write the durable metadata sidecar for assembled multipart objects that predate it.
///
/// Offline by construction: `main` acquires the node lock for every node-local command, so no live
/// writer can supersede a version while its sidecar is staged. Only composite-ETag versions with a
/// flat `bucket/uuid` data path are candidates; a nested laboratory path is counted and skipped
/// because its parent directory is outside the permit's bucket directory.
///
/// With `upgrade_v1`, an object assembled between the v1 sidecar stage and the v2 part-layout stage
/// also gets its existing v1 record replaced by the row's v2 record, after the record identity is
/// checked against the row. Without the flag an existing sidecar is never touched.
pub(crate) fn run(
    cfg: Config,
    dry_run: bool,
    limit: Option<usize>,
    upgrade_v1: bool,
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
        let mut upgraded = 0u64;
        let mut skipped_v1 = 0u64;
        let mut conflicts = 0u64;
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
                    let sidecar_file = match std::fs::symlink_metadata(
                        cfg.data_dir.join(sidecar.as_str()),
                    ) {
                        Ok(metadata) => Some(metadata.file_type().is_file()),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                        Err(error) => {
                            failed += 1;
                            eprintln!(
                                "cannot inspect sidecar {}: {error}",
                                sidecar.as_str()
                            );
                            continue;
                        }
                    };
                    // An existing sidecar is only rewritten by the opt-in upgrade, and only after
                    // its decoded record is proven to describe exactly this index row.
                    let mut replace = false;
                    if let Some(is_regular_file) = sidecar_file {
                        if !upgrade_v1 {
                            existing += 1;
                            continue;
                        }
                        if !is_regular_file {
                            conflicts += 1;
                            eprintln!(
                                "sidecar {} is not a regular file; left untouched",
                                sidecar.as_str()
                            );
                            continue;
                        }
                        let decoded = match crate::sidecar_rebuild::read_sidecar(
                            &cfg.data_dir.join(sidecar.as_str()),
                        ) {
                            Ok(record) => record,
                            Err(error) => {
                                failed += 1;
                                eprintln!(
                                    "cannot decode sidecar {}: {error}",
                                    sidecar.as_str()
                                );
                                continue;
                            }
                        };
                        match classify_existing_sidecar(&decoded, &row) {
                            ExistingSidecar::Keep => {
                                existing += 1;
                                continue;
                            }
                            ExistingSidecar::SkipV1 => {
                                skipped_v1 += 1;
                                continue;
                            }
                            ExistingSidecar::Conflict => {
                                conflicts += 1;
                                eprintln!(
                                    "sidecar {} disagrees with {}/{} version {}; left untouched",
                                    sidecar.as_str(),
                                    bucket.name.as_str(),
                                    summary.key.as_str(),
                                    summary.version_id.as_str()
                                );
                                continue;
                            }
                            ExistingSidecar::Upgrade => replace = true,
                        }
                    }
                    match std::fs::symlink_metadata(cfg.data_dir.join(data_path.as_str())) {
                        // An assembled object's data file is the payload or container itself; the
                        // record's physical size is its exact length. Never bless a truncated or
                        // replaced file with a fresh sidecar.
                        Ok(metadata)
                            if metadata.file_type().is_file()
                                && metadata.len() == row.size_physical => {}
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
                    let outcome = if replace {
                        blob.replace_metadata_sidecar(permit, &data_path, &record)
                            .await
                            .map(|()| true)
                    } else {
                        blob.backfill_metadata_sidecar(permit, &data_path, &record)
                            .await
                    };
                    match outcome {
                        Ok(true) if replace => upgraded += 1,
                        Ok(true) => written += 1,
                        Ok(false) => existing += 1,
                        Err(error) => {
                            failed += 1;
                            eprintln!(
                                "cannot {} sidecar for {}: {error}",
                                if replace { "upgrade" } else { "write" },
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
            "sidecar backfill: scanned={scanned} candidates={candidates} written={written} existing={existing} upgraded={upgraded} skipped_v1={skipped_v1} conflicts={conflicts} missing_data={missing_data} nested={nested} failed={failed}{}{}",
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
    use cairn_types::object::{MultipartPartRecord, ObjectLockMode, ObjectRetention};

    fn object_row(parts: Option<Vec<MultipartPartRecord>>) -> ObjectVersionRow {
        ObjectVersionRow {
            id: "a".repeat(32),
            bucket: cairn_types::BucketName::parse("bucket").unwrap(),
            key: cairn_types::ObjectKey::parse("assembled.bin").unwrap(),
            version_id: cairn_types::VersionId::null(),
            is_latest: true,
            is_delete_marker: false,
            size_logical: 11,
            size_physical: 11,
            etag: cairn_types::ETag::from_string("0123456789abcdef0123456789abcdef-2".to_owned()),
            content_type: "application/octet-stream".to_owned(),
            content_encoding: None,
            cache_control: Some("max-age=60".to_owned()),
            content_disposition: None,
            content_language: None,
            expires: None,
            expire_at: None,
            storage_path: Some(cairn_types::StoragePath::from_string(format!(
                "bucket/{}",
                "b".repeat(32)
            ))),
            compression: cairn_types::CompressionDescriptor::Uncompressed,
            storage_class: cairn_types::StorageClass::Standard,
            cold_locator: None,
            owner_id: cairn_types::UserId("owner".to_owned()),
            user_metadata: Vec::new(),
            acl: None,
            checksums: Vec::new(),
            sse_descriptor: None,
            internal_blake3: None,
            internal_sha256: None,
            replication_status: None,
            replicated_at: None,
            multipart_parts: parts,
            created_at: Timestamp(1),
            updated_at: Timestamp(2),
        }
    }

    fn parts() -> Vec<MultipartPartRecord> {
        vec![
            MultipartPartRecord {
                part_number: 1,
                size: 5,
            },
            MultipartPartRecord {
                part_number: 3,
                size: 6,
            },
        ]
    }

    #[test]
    fn only_a_matching_v1_record_with_a_row_layout_is_upgraded() {
        let row = object_row(Some(parts()));
        let v2 = ObjectMetadataRecord::from_existing(
            &row,
            Vec::new(),
            ExplicitObjectLockIntent::default(),
        )
        .unwrap();
        assert_eq!(classify_existing_sidecar(&v2, &row), ExistingSidecar::Keep);

        // The exact gap this command closes: a v1 record beside a row that records the layout.
        let mut v1 = v2.clone();
        v1.format_version = OBJECT_METADATA_V1_FORMAT_VERSION;
        v1.multipart_parts = None;
        assert_eq!(
            classify_existing_sidecar(&v1, &row),
            ExistingSidecar::Upgrade
        );

        // A legacy assembly with no recorded layout has nothing to upgrade.
        let layout_less = object_row(None);
        assert_eq!(
            classify_existing_sidecar(&v1, &layout_less),
            ExistingSidecar::SkipV1
        );

        // Any identity disagreement is a conflict, never a rewrite.
        let mut mismatched = v1.clone();
        mismatched.etag = cairn_types::ETag::from_string("other".to_owned());
        assert_eq!(
            classify_existing_sidecar(&mismatched, &row),
            ExistingSidecar::Conflict
        );
        let mut wrong_size = v1.clone();
        wrong_size.size_physical += 1;
        assert_eq!(
            classify_existing_sidecar(&wrong_size, &row),
            ExistingSidecar::Conflict
        );

        // A v2 record whose layout disagrees with the row is reported, not replaced.
        let mut different_layout = v2.clone();
        different_layout.multipart_parts = Some(vec![MultipartPartRecord {
            part_number: 1,
            size: 11,
        }]);
        assert_eq!(
            classify_existing_sidecar(&different_layout, &row),
            ExistingSidecar::Conflict
        );

        // A row with the BLAKE3 baseline classifies against the v3 generation. The same v2 record
        // becomes an upgrade target, and a v3 record with a stale baseline is a conflict.
        let mut blake3_row = row.clone();
        blake3_row.internal_blake3 = Some("ab".repeat(32));
        blake3_row.internal_sha256 = None;
        let v3 = ObjectMetadataRecord::from_existing(
            &blake3_row,
            Vec::new(),
            ExplicitObjectLockIntent::default(),
        )
        .unwrap();
        assert_eq!(
            classify_existing_sidecar(&v3, &blake3_row),
            ExistingSidecar::Keep
        );
        assert_eq!(
            classify_existing_sidecar(&v2, &blake3_row),
            ExistingSidecar::Upgrade
        );
        let mut stale = v3.clone();
        stale.internal_blake3 = Some("cd".repeat(32));
        assert_eq!(
            classify_existing_sidecar(&stale, &blake3_row),
            ExistingSidecar::Conflict
        );
    }

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
