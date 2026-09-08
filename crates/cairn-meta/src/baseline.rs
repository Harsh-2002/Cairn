//! Offline baseline accounting. Mutation functions execute only inside the canonical Writer.

use crate::storage::{integer, optional_string, q, string, text, x};
use cairn_types::storage::{StorageToken, validate_storage_path};
use cairn_types::storage_baseline::*;
use cairn_types::{BucketName, MetaError, MutationOutcome, StoragePath, Timestamp};
use rusqlite::Connection as DB;
use rusqlite::types::Value as Cell;

type R<T> = Result<T, MetaError>;
fn invalid(message: &str) -> MetaError {
    MetaError::Engine(message.into())
}
fn token(value: Option<&str>) -> R<Option<StorageToken>> {
    value
        .map(|value| StorageToken::try_from(value.to_owned()))
        .transpose()
}
fn updated(value: StorageBaselineTransition) -> MutationOutcome {
    MutationOutcome::StorageBaselineUpdated(value)
}

pub fn state(db: &DB) -> R<StorageBaselineState> {
    let rows = q(
        db,
        "SELECT generation,coverage_identity,baseline_completed_at,baseline_id,legacy_accounting_hold,legacy_release_authorized,coverage_state FROM storage_recovery_state WHERE singleton=1",
        vec![],
    )?;
    let row = rows
        .first()
        .ok_or_else(|| invalid("missing storage recovery state"))?;
    let hold = integer(row, 4)?;
    let authorized = integer(row, 5)?;
    let completed_at = match &row[2] {
        Cell::Null => None,
        Cell::Integer(value) if *value >= 0 => Some(Timestamp(*value)),
        _ => return Err(invalid("invalid storage baseline completion time")),
    };
    let state = StorageBaselineState {
        generation: token(optional_string(row, 0)?)?,
        coverage_identity: token(optional_string(row, 1)?)?,
        completed_at,
        baseline_id: token(optional_string(row, 3)?)?,
        legacy_accounting_hold: hold == 1,
        legacy_release_authorized: authorized == 1,
    };
    let coverage_valid = match string(row, 6)? {
        "incomplete" => state.coverage_identity.is_none() && state.completed_at.is_none(),
        "complete" => {
            state.coverage_identity.is_some()
                && state.completed_at.is_some()
                && hold == 0
                && state.generation.is_some()
        }
        _ => false,
    };
    if !coverage_valid
        || !matches!(hold, 0 | 1)
        || !matches!(authorized, 0 | 1)
        || (hold == 0 && (authorized != 0 || state.baseline_id.is_some()))
        || (hold == 1 && (state.baseline_id.is_none() || state.generation.is_none()))
    {
        return Err(invalid("invalid storage baseline state"));
    }
    Ok(state)
}

pub fn pending(db: &DB) -> R<StorageBaselinePending> {
    let rows = q(
        db,
        "SELECT EXISTS(SELECT 1 FROM storage_write_intents), EXISTS(SELECT 1 FROM storage_intent_paths), EXISTS(SELECT 1 FROM storage_cleanups), EXISTS(SELECT 1 FROM multipart_staging_cleanups WHERE storage_protocol IS NOT 1), EXISTS(SELECT 1 FROM multipart_part_reservations), EXISTS(SELECT 1 FROM multipart_staging_cleanups WHERE storage_protocol=1)",
        vec![],
    )?;
    let row = rows
        .first()
        .ok_or_else(|| invalid("missing storage pending state"))?;
    Ok(StorageBaselinePending {
        intents: integer(row, 0)? != 0,
        intent_paths: integer(row, 1)? != 0,
        exact_debt: integer(row, 2)? != 0,
        native_quota_debt: integer(row, 3)? != 0,
        legacy_reservations: integer(row, 4)? != 0,
        legacy_quota_debt: integer(row, 5)? != 0,
    })
}

pub fn held(db: &DB) -> R<Option<StorageToken>> {
    let state = state(db)?;
    Ok(if state.legacy_accounting_hold {
        state.baseline_id
    } else {
        None
    })
}

pub fn ensure_unheld(db: &DB) -> R<()> {
    if held(db)?.is_some() {
        return Err(invalid("storage baseline accounting is held"));
    }
    Ok(())
}

pub fn begin(db: &DB, token: &StorageBaselineToken) -> R<MutationOutcome> {
    let state = state(db)?;
    if state.generation.as_ref() != Some(&token.generation) {
        return Ok(updated(StorageBaselineTransition::Stale));
    }
    if state.matches(token) {
        return Ok(updated(if state.legacy_release_authorized {
            StorageBaselineTransition::Stale
        } else {
            StorageBaselineTransition::AlreadyApplied
        }));
    }
    x(
        db,
        "UPDATE storage_recovery_state SET coverage_state='incomplete',coverage_identity=NULL,baseline_completed_at=NULL,baseline_id=?1,legacy_accounting_hold=1,legacy_release_authorized=0 WHERE singleton=1",
        vec![text(token.baseline_id.as_str())],
    )?;
    Ok(updated(StorageBaselineTransition::Applied))
}

pub fn owners(db: &DB, paths: &[StoragePath]) -> R<Vec<StoragePathOwnership>> {
    if paths.len() > STORAGE_BASELINE_PAGE_LIMIT {
        return Err(invalid("storage ownership page exceeds bound"));
    }
    let mut result = Vec::with_capacity(paths.len());
    for path in paths {
        if path.as_str().len() > 256 {
            return Err(invalid("storage path exceeds bound"));
        }
        // Every lookup is an indexed exact path seek. LEFT JOIN exposes orphan authority/index
        // rows as errors instead of hiding them. Quota-owner aliases retain their routing bucket.
        let rows = q(
            db,
            "SELECT bucket_name,0 FROM object_versions WHERE storage_path=?1 UNION ALL SELECT u.bucket_name,0 FROM multipart_parts p LEFT JOIN multipart_uploads u ON u.id=p.upload_id WHERE p.storage_path=?1 UNION ALL SELECT i.bucket_name,1 FROM storage_intent_paths p LEFT JOIN storage_write_intents i ON i.attempt_id=p.attempt_id WHERE p.storage_path=?1 UNION ALL SELECT bucket_name,2 FROM storage_cleanups WHERE storage_path=?1 UNION ALL SELECT bucket_name,3 FROM multipart_staging_cleanups WHERE storage_path=?1 UNION ALL SELECT bucket_name,4 FROM storage_cleanups WHERE quota_owner_path=?1 LIMIT 130",
            vec![text(path.as_str())],
        )?;
        if rows.len() > STORAGE_BASELINE_PAGE_LIMIT {
            return Err(invalid("ambiguous storage path ownership exceeds bound"));
        }
        let mut owner = StoragePathOwnership {
            path: path.clone(),
            bucket: None,
            authoritative: false,
            intent: false,
            cleanup: false,
            legacy_debt: false,
        };
        for row in rows {
            let bucket = BucketName::parse(string(&row, 0)?)
                .map_err(|_| invalid("invalid retained storage bucket"))?;
            validate_storage_path(&bucket, path)?;
            if owner
                .bucket
                .as_ref()
                .is_some_and(|previous| previous != &bucket)
            {
                return Err(invalid("conflicting retained storage buckets"));
            }
            owner.bucket = Some(bucket);
            match integer(&row, 1)? {
                0 => owner.authoritative = true,
                1 => owner.intent = true,
                2 => owner.cleanup = true,
                3 => owner.legacy_debt = true,
                4 => {}
                _ => return Err(invalid("invalid storage ownership kind")),
            }
        }
        if let Some(upload) = path
            .as_str()
            .strip_prefix(".staging/multipart/")
            .and_then(|tail| tail.split_once('/').map(|(upload, _)| upload))
        {
            let retained = q(
                db,
                "SELECT bucket_name FROM multipart_uploads WHERE id=?1 UNION SELECT bucket_name FROM multipart_staging_cleanups WHERE upload_id=?1 UNION SELECT bucket_name FROM storage_write_intents WHERE upload_id=?1 LIMIT 2",
                vec![text(upload)],
            )?;
            for row in retained {
                let bucket = BucketName::parse(string(&row, 0)?)
                    .map_err(|_| invalid("invalid retained multipart bucket"))?;
                validate_storage_path(&bucket, path)?;
                if owner
                    .bucket
                    .as_ref()
                    .is_some_and(|previous| previous != &bucket)
                {
                    return Err(invalid("conflicting retained multipart buckets"));
                }
                owner.bucket = Some(bucket);
            }
        }
        result.push(owner);
    }
    Ok(result)
}

pub fn classify(
    db: &DB,
    bucket: &BucketName,
    token: &StorageBaselineToken,
    paths: &[StoragePath],
) -> R<MutationOutcome> {
    let state = state(db)?;
    if !state.matches(token) || state.legacy_release_authorized {
        return Ok(updated(StorageBaselineTransition::Stale));
    }
    if paths.is_empty() || paths.len() > STORAGE_BASELINE_PAGE_LIMIT {
        return Err(invalid("invalid storage classification page size"));
    }
    let owners = owners(db, paths)?;
    let mut dispositions = Vec::with_capacity(paths.len());
    for (path, owner) in paths.iter().zip(owners) {
        validate_storage_path(bucket, path)?;
        if owner.bucket.as_ref().is_some_and(|owner| owner != bucket) {
            return Err(invalid("storage classification routing mismatch"));
        }
        if owner.authoritative {
            dispositions.push(StorageBaselineDisposition::Authoritative);
        } else if owner.intent {
            dispositions.push(StorageBaselineDisposition::IntentOwned);
        } else {
            if !owner.cleanup {
                // A remaining native spool may already own this final path's quota. Preserve that
                // association when the scan rediscovers the final alias; never manufacture a
                // second quota charge or downgrade its native ownership to generic legacy debt.
                let linked = q(
                    db,
                    "SELECT DISTINCT quota_debt_id FROM storage_cleanups WHERE quota_owner_path=?1 AND quota_debt_id IS NOT NULL LIMIT 2",
                    vec![text(path.as_str())],
                )?;
                if linked.len() > 1 {
                    return Err(invalid("conflicting native storage quota owners"));
                }
                let quota = linked.first().map(|row| string(row, 0)).transpose()?;
                crate::storage::enqueue_owned(db, bucket, path, quota, quota.map(|_| path))?;
            }
            dispositions.push(StorageBaselineDisposition::CleanupRecorded);
        }
    }
    Ok(MutationOutcome::StorageBaselineClassified(dispositions))
}

pub fn authorize(db: &DB, token: &StorageBaselineToken) -> R<MutationOutcome> {
    let state = state(db)?;
    if !state.matches(token) {
        return Ok(updated(StorageBaselineTransition::Stale));
    }
    if pending(db)?.native_pending() {
        return Ok(updated(StorageBaselineTransition::Blocked));
    }
    if state.legacy_release_authorized {
        return Ok(updated(StorageBaselineTransition::AlreadyApplied));
    }
    x(
        db,
        "UPDATE storage_recovery_state SET legacy_release_authorized=1 WHERE singleton=1",
        vec![],
    )?;
    Ok(updated(StorageBaselineTransition::Applied))
}

pub fn finalize(db: &DB, token: &StorageBaselineToken, limit: u32) -> R<MutationOutcome> {
    let state = state(db)?;
    if !state.matches(token) || !state.legacy_release_authorized {
        return Ok(updated(StorageBaselineTransition::Stale));
    }
    if pending(db)?.native_pending() {
        return Ok(updated(StorageBaselineTransition::Blocked));
    }
    let limit = limit.clamp(1, 1000);
    let rows = q(
        db,
        "SELECT r.attempt_id,u.bucket_name,COALESCE(u.initiated_by,u.owner_id),r.reserved_bytes FROM multipart_part_reservations r LEFT JOIN multipart_uploads u ON u.id=r.upload_id ORDER BY r.created_at,r.attempt_id LIMIT ?1",
        vec![Cell::Integer(i64::from(limit))],
    )?;
    let mut released = 0;
    for row in rows {
        let id = string(&row, 0)?;
        let bucket = string(&row, 1)?;
        let principal = string(&row, 2)?;
        let bytes = integer(&row, 3)?;
        if bytes < 0 {
            return Err(invalid("invalid legacy multipart reservation charge"));
        }
        let changed = x(
            db,
            "DELETE FROM multipart_part_reservations WHERE attempt_id=?1",
            vec![text(id)],
        )?;
        if changed != 1 {
            return Err(invalid("legacy reservation deletion lost ownership"));
        }
        crate::apply::adjust_multipart_stats(db, bucket, principal, 0, -bytes)?;
        released += 1;
    }
    if released < limit {
        let rows = q(
            db,
            "SELECT id,bucket_name,principal_id,bytes FROM multipart_staging_cleanups WHERE storage_protocol=1 ORDER BY created_at,id LIMIT ?1",
            vec![Cell::Integer(i64::from(limit - released))],
        )?;
        for row in rows {
            let bytes = integer(&row, 3)?;
            if bytes < 0 {
                return Err(invalid("invalid legacy multipart cleanup charge"));
            }
            let changed = x(
                db,
                "DELETE FROM multipart_staging_cleanups WHERE id=?1 AND storage_protocol=1",
                vec![text(string(&row, 0)?)],
            )?;
            if changed != 1 {
                return Err(invalid("legacy cleanup deletion lost ownership"));
            }
            crate::apply::adjust_multipart_stats(
                db,
                string(&row, 1)?,
                string(&row, 2)?,
                0,
                -bytes,
            )?;
            released += 1;
        }
    }
    let pending = pending(db)?;
    Ok(MutationOutcome::StorageBaselineLegacyPage {
        released,
        remaining: pending.legacy_reservations || pending.legacy_quota_debt,
    })
}

pub fn complete(
    db: &DB,
    token: &StorageBaselineToken,
    completed_at: Timestamp,
) -> R<MutationOutcome> {
    let state = state(db)?;
    if !state.legacy_accounting_hold
        && state.generation.as_ref() == Some(&token.generation)
        && state.coverage_identity.as_ref() == Some(&token.baseline_id)
    {
        return Ok(updated(StorageBaselineTransition::AlreadyApplied));
    }
    if !state.matches(token) || !state.legacy_release_authorized {
        return Ok(updated(StorageBaselineTransition::Stale));
    }
    if pending(db)?.any() {
        return Ok(updated(StorageBaselineTransition::Blocked));
    }
    if completed_at.0 < 0 {
        return Err(invalid("invalid storage baseline completion time"));
    }
    x(
        db,
        "UPDATE storage_recovery_state SET coverage_state='complete',coverage_identity=?1,baseline_completed_at=?2,baseline_id=NULL,legacy_accounting_hold=0,legacy_release_authorized=0 WHERE singleton=1",
        vec![
            text(token.baseline_id.as_str()),
            Cell::Integer(completed_at.0),
        ],
    )?;
    Ok(updated(StorageBaselineTransition::Applied))
}

fn object_authority(db: &DB, after: &str, limit: u32) -> R<Vec<StorageAuthority>> {
    let mut statement = db
        .prepare_cached("SELECT * FROM object_versions WHERE id>?1 ORDER BY id LIMIT ?2")
        .map_err(crate::model::engine_err)?;
    let rows = statement
        .query_map(rusqlite::params![after, limit], |row| {
            let bucket: String = row.get("bucket_name")?;
            let logical: i64 = row.get("size_logical")?;
            let physical: i64 = row.get("size_physical")?;
            Ok((
                bucket,
                logical,
                physical,
                crate::model::object_version_from_row(row)?,
            ))
        })
        .map_err(crate::model::engine_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(crate::model::engine_err)?;
    rows.into_iter()
        .map(|(bucket, logical, physical, row)| {
            let bucket =
                BucketName::parse(&bucket).map_err(|_| invalid("invalid authority bucket"))?;
            if logical < 0 || physical < 0 {
                return Err(invalid("negative authority object length"));
            }
            if let Some(path) = &row.storage_path {
                validate_storage_path(&bucket, path)?;
            }
            Ok(StorageAuthority::Object(Box::new(row)))
        })
        .collect()
}

fn part_authority(db: &DB, after: &str, part: u16, limit: u32) -> R<Vec<StorageAuthority>> {
    let mut statement = db.prepare_cached("SELECT p.*,u.bucket_name AS baseline_bucket FROM multipart_parts p LEFT JOIN multipart_uploads u ON u.id=p.upload_id WHERE (p.upload_id,p.part_number)>(?1,?2) ORDER BY p.upload_id,p.part_number LIMIT ?3").map_err(crate::model::engine_err)?;
    let rows = statement
        .query_map(rusqlite::params![after, part, limit], |row| {
            let bucket: String = row.get("baseline_bucket")?;
            let upload: String = row.get("upload_id")?;
            let number: i64 = row.get("part_number")?;
            let size: i64 = row.get("size")?;
            Ok((
                bucket,
                upload,
                number,
                size,
                crate::model::part_from_row(row)?,
            ))
        })
        .map_err(crate::model::engine_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(crate::model::engine_err)?;
    rows.into_iter()
        .map(|(bucket, upload, number, size, part)| {
            let bucket = BucketName::parse(&bucket)
                .map_err(|_| invalid("invalid multipart authority bucket"))?;
            if !(1..=10000).contains(&number) || size < 0 {
                return Err(invalid("invalid multipart authority geometry"));
            }
            validate_storage_path(&bucket, &part.storage_path)?;
            Ok(StorageAuthority::Part {
                bucket,
                upload_id: cairn_types::UploadId::from_string(upload),
                part,
            })
        })
        .collect()
}

pub fn authority(
    db: &DB,
    cursor: Option<&StorageAuthorityCursor>,
    limit: u32,
) -> R<StorageAuthorityPage> {
    // The cursor starts before every valid id. Reject damaged NULL/empty primary keys instead
    // of silently skipping them at the first keyset seek.
    if !q(
        db,
        "SELECT 1 FROM object_versions WHERE id IS NULL OR id='' LIMIT 1",
        vec![],
    )?
    .is_empty()
    {
        return Err(invalid("invalid storage authority row identity"));
    }
    let mut cursor = cursor.cloned().unwrap_or_default();
    if cursor.shard != 0
        || cursor.last_id.len() > 256
        || (cursor.kind == StorageAuthorityKind::Objects && cursor.last_part != 0)
    {
        return Err(invalid("invalid storage authority cursor"));
    }
    let limit = limit.clamp(1, STORAGE_BASELINE_PAGE_LIMIT as u32);
    let mut items = Vec::with_capacity(limit as usize);
    loop {
        let remaining = limit - items.len() as u32;
        let mut rows = match cursor.kind {
            StorageAuthorityKind::Objects => object_authority(db, &cursor.last_id, remaining + 1)?,
            StorageAuthorityKind::Parts => {
                part_authority(db, &cursor.last_id, cursor.last_part, remaining + 1)?
            }
        };
        let more = rows.len() > remaining as usize;
        rows.truncate(remaining as usize);
        for row in &rows {
            match row {
                StorageAuthority::Object(row) => {
                    cursor.last_id = row.id.clone();
                    cursor.last_part = 0;
                }
                StorageAuthority::Part {
                    upload_id, part, ..
                } => {
                    cursor.last_id = upload_id.as_str().to_owned();
                    cursor.last_part = part.part_number;
                }
            }
        }
        items.extend(rows);
        if more {
            return Ok(StorageAuthorityPage {
                items,
                next: Some(cursor),
            });
        }
        if cursor.kind == StorageAuthorityKind::Parts {
            return Ok(StorageAuthorityPage { items, next: None });
        }
        cursor.kind = StorageAuthorityKind::Parts;
        cursor.last_id.clear();
        cursor.last_part = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::{MetadataStore, Mutation};

    fn legacy_fixture(db: &DB) -> StorageBaselineToken {
        crate::schema::run_migrations(db).unwrap();
        let token = StorageBaselineToken {
            generation: StorageToken::generate(),
            baseline_id: StorageToken::generate(),
        };
        crate::storage::begin(db, &token.generation).unwrap();
        x(db, "INSERT INTO multipart_staging_cleanups(id,upload_id,bucket_name,principal_id,bytes,storage_path,created_at) VALUES ('legacy-a','gone-upload','legacy-bucket','initiator',10,NULL,0),('legacy-b','gone-upload','legacy-bucket','initiator',20,NULL,1)",vec![]).unwrap();
        x(
            db,
            "INSERT INTO multipart_bucket_stats VALUES ('legacy-bucket',0,30)",
            vec![],
        )
        .unwrap();
        x(
            db,
            "INSERT INTO multipart_principal_stats VALUES ('initiator',0,30)",
            vec![],
        )
        .unwrap();
        assert_eq!(
            begin(db, &token).unwrap(),
            updated(StorageBaselineTransition::Applied)
        );
        token
    }

    fn legacy_snapshot(db: &DB) -> Vec<Vec<Cell>> {
        q(db,"SELECT id,bytes FROM multipart_staging_cleanups UNION ALL SELECT bucket_name,staged_bytes FROM multipart_bucket_stats UNION ALL SELECT principal_id,staged_bytes FROM multipart_principal_stats ORDER BY 1",vec![]).unwrap()
    }

    fn legacy_hold_contract(db: &DB) {
        let token = legacy_fixture(db);
        let before = legacy_snapshot(db);
        for mutation in [
            Mutation::ReleaseMultipartReservation {
                upload_id: cairn_types::UploadId::from_string("gone-upload".into()),
                attempt_id: "unknown".into(),
            },
            Mutation::ReleaseMultipartCleanup {
                cleanup_id: "legacy-a".into(),
            },
            Mutation::ReleaseMultipartUploadCleanups {
                upload_id: cairn_types::UploadId::from_string("gone-upload".into()),
            },
            Mutation::RecoverMultipartStagingAccounting { limit: 1000 },
        ] {
            assert_eq!(
                crate::apply::apply(db, mutation).unwrap(),
                MutationOutcome::MultipartAccountingHeld {
                    baseline_id: token.baseline_id.clone()
                }
            );
            assert_eq!(legacy_snapshot(db), before);
        }
        assert_eq!(
            authorize(db, &token).unwrap(),
            updated(StorageBaselineTransition::Applied)
        );
        assert_eq!(
            finalize(db, &token, 1).unwrap(),
            MutationOutcome::StorageBaselineLegacyPage {
                released: 1,
                remaining: true
            }
        );
        assert_eq!(
            legacy_snapshot(db),
            vec![
                vec![text("initiator"), Cell::Integer(20)],
                vec![text("legacy-b"), Cell::Integer(20)],
                vec![text("legacy-bucket"), Cell::Integer(20)]
            ]
        );
        assert!(state(db).unwrap().legacy_accounting_hold);
        assert_eq!(
            complete(db, &token, Timestamp(1)).unwrap(),
            updated(StorageBaselineTransition::Blocked)
        );
        assert_eq!(
            finalize(db, &token, 1).unwrap(),
            MutationOutcome::StorageBaselineLegacyPage {
                released: 1,
                remaining: false
            }
        );
        assert_eq!(
            complete(db, &token, Timestamp(1)).unwrap(),
            updated(StorageBaselineTransition::Applied)
        );
        crate::schema::run_migrations(db).unwrap();
        assert_eq!(
            q(db, "SELECT recovery_mode FROM storage_protocol", vec![]).unwrap(),
            vec![vec![text("full-scan")]]
        );
    }

    fn unconditional_pending_contract(db: &DB) {
        crate::schema::run_migrations(db).unwrap();
        // Corruption fixture: expose orphan rows that a damaged database may retain.
        x(db, "PRAGMA foreign_keys=OFF", vec![]).unwrap();
        let token = StorageBaselineToken {
            generation: StorageToken::generate(),
            baseline_id: StorageToken::generate(),
        };
        crate::storage::begin(db, &token.generation).unwrap();
        begin(db, &token).unwrap();
        let path = ".staging/11111111111111111111111111111111.tmp";
        x(db,"INSERT INTO storage_intent_paths(attempt_id,role,storage_path) VALUES ('missing-intent','temporary',?1)",vec![text(path)]).unwrap();
        assert!(pending(db).unwrap().intent_paths);
        assert!(!pending(db).unwrap().intents);
        assert!(owners(db, &[StoragePath::from_string(path.into())]).is_err());
        assert_eq!(
            authorize(db, &token).unwrap(),
            updated(StorageBaselineTransition::Blocked)
        );
        x(db, "DELETE FROM storage_intent_paths", vec![]).unwrap();
        x(db,"INSERT INTO multipart_staging_cleanups(id,upload_id,bucket_name,principal_id,bytes,created_at,storage_protocol) VALUES ('native','gone','retained-bucket','initiator',0,0,2)",vec![]).unwrap();
        assert!(pending(db).unwrap().native_quota_debt);
        assert_eq!(
            authorize(db, &token).unwrap(),
            updated(StorageBaselineTransition::Blocked)
        );
        x(
            db,
            "UPDATE multipart_staging_cleanups SET storage_protocol=99",
            vec![],
        )
        .unwrap();
        assert!(pending(db).unwrap().native_quota_debt);
        assert_eq!(
            authorize(db, &token).unwrap(),
            updated(StorageBaselineTransition::Blocked)
        );
        x(db, "DELETE FROM multipart_staging_cleanups", vec![]).unwrap();
        x(db,"INSERT INTO multipart_parts(upload_id,part_number,size,etag,storage_path) VALUES ('22222222222222222222222222222222',1,0,'etag','.staging/multipart/22222222222222222222222222222222/00001')",vec![]).unwrap();
        assert!(
            authority(db, None, 1).is_err(),
            "orphan multipart authority must not disappear in an inner join"
        );
    }

    #[test]
    fn sqlite_storage_baseline_legacy_hold_and_counters() {
        legacy_hold_contract(&DB::open_in_memory().unwrap());
    }
    #[test]
    fn sqlite_storage_baseline_unconditional_pending() {
        unconditional_pending_contract(&DB::open_in_memory().unwrap());
    }

    #[tokio::test]
    async fn sqlite_storage_baseline_corrupt_counter_rolls_back_writer_page() {
        let store = crate::open_in_memory().unwrap();
        let token = store
            .writer
            .run_exec(|db| {
                let token = legacy_fixture(db);
                authorize(db, &token)?;
                x(
                    db,
                    "UPDATE multipart_bucket_stats SET staged_bytes=0",
                    vec![],
                )?;
                Ok(token)
            })
            .await
            .unwrap();
        let before = store.with_read(|db| Ok(legacy_snapshot(db))).await.unwrap();
        assert!(
            store
                .submit(Mutation::FinalizeStorageBaselineLegacy {
                    token: token.clone(),
                    limit: 2
                })
                .await
                .is_err()
        );
        assert_eq!(
            store.with_read(|db| Ok(legacy_snapshot(db))).await.unwrap(),
            before
        );
        assert!(store.storage_baseline_states().await.unwrap()[0].matches(&token));
    }

    #[tokio::test]
    async fn sharded_storage_baseline_partial_hold_blocks_before_first_release() {
        use std::sync::Arc;
        let first = Arc::new(crate::open_in_memory().unwrap());
        let last = Arc::new(crate::open_in_memory().unwrap());
        let token = last
            .writer
            .run_exec(|db| Ok(legacy_fixture(db)))
            .await
            .unwrap();
        first.writer.run_exec(|db| {
            x(db,"INSERT INTO multipart_staging_cleanups(id,upload_id,bucket_name,principal_id,bytes,created_at) VALUES ('legacy-a','gone-upload','legacy-bucket','initiator',10,0)",vec![])?;
            x(db,"INSERT INTO multipart_bucket_stats VALUES ('legacy-bucket',0,10)",vec![])?;
            x(db,"INSERT INTO multipart_principal_stats VALUES ('initiator',0,10)",vec![])?;
            Ok(())
        }).await.unwrap();
        let before = first.with_read(|db| Ok(legacy_snapshot(db))).await.unwrap();
        let store = crate::ShardedMetadataStore::new(vec![first.clone(), last.clone()]);
        for mutation in [
            Mutation::ReleaseMultipartReservation {
                upload_id: cairn_types::UploadId::from_string("gone-upload".into()),
                attempt_id: "unknown".into(),
            },
            Mutation::ReleaseMultipartCleanup {
                cleanup_id: "legacy-a".into(),
            },
            Mutation::ReleaseMultipartUploadCleanups {
                upload_id: cairn_types::UploadId::from_string("gone-upload".into()),
            },
            Mutation::RecoverMultipartStagingAccounting { limit: 1000 },
        ] {
            assert_eq!(
                store.submit(mutation).await.unwrap(),
                MutationOutcome::MultipartAccountingHeld {
                    baseline_id: token.baseline_id.clone()
                }
            );
            assert_eq!(
                first.with_read(|db| Ok(legacy_snapshot(db))).await.unwrap(),
                before
            );
        }
    }
    #[tokio::test]
    async fn sharded_storage_baseline_partial_authorization_and_completion() {
        use cairn_types::blob::{
            StorageBaselineOptions, StorageBaselineProof, StorageClassificationCounts,
            StorageClassificationReport,
        };
        use std::sync::Arc;
        let first = Arc::new(crate::open_in_memory().unwrap());
        let last = Arc::new(crate::open_in_memory().unwrap());
        let store = crate::ShardedMetadataStore::new(vec![first.clone(), last.clone()]);
        let token = StorageBaselineToken {
            generation: StorageToken::generate(),
            baseline_id: StorageToken::generate(),
        };
        store
            .submit(Mutation::BeginStorageGeneration {
                generation: token.generation.clone(),
            })
            .await
            .unwrap();
        store
            .submit(Mutation::BeginStorageBaseline {
                token: token.clone(),
            })
            .await
            .unwrap();
        let proof = StorageBaselineProof::backend_verified(
            StorageClassificationReport::backend_completed(
                StorageBaselineOptions {
                    token: token.clone(),
                    batch_size: 128,
                    root_artifacts: Vec::new(),
                },
                1,
                1,
                StorageClassificationCounts::default(),
                Arc::new(()),
            ),
            0,
        );
        assert_eq!(
            first
                .submit(Mutation::AuthorizeStorageBaselineRelease {
                    proof: proof.clone()
                })
                .await
                .unwrap(),
            updated(StorageBaselineTransition::Applied)
        );
        let before = store.storage_baseline_states().await.unwrap();
        assert_eq!(
            store
                .submit(Mutation::FinalizeStorageBaselineLegacy {
                    token: token.clone(),
                    limit: 1
                })
                .await
                .unwrap(),
            updated(StorageBaselineTransition::Stale)
        );
        assert_eq!(
            store
                .submit(Mutation::CompleteStorageBaseline {
                    token: token.clone(),
                    completed_at: Timestamp(1)
                })
                .await
                .unwrap(),
            updated(StorageBaselineTransition::Stale)
        );
        assert_eq!(store.storage_baseline_states().await.unwrap(), before);
        assert_eq!(
            store
                .submit(Mutation::AuthorizeStorageBaselineRelease {
                    proof: proof.clone()
                })
                .await
                .unwrap(),
            updated(StorageBaselineTransition::Applied)
        );
        assert_eq!(
            store
                .submit(Mutation::AuthorizeStorageBaselineRelease { proof })
                .await
                .unwrap(),
            updated(StorageBaselineTransition::AlreadyApplied)
        );
        assert_eq!(
            first
                .submit(Mutation::CompleteStorageBaseline {
                    token: token.clone(),
                    completed_at: Timestamp(1)
                })
                .await
                .unwrap(),
            updated(StorageBaselineTransition::Applied)
        );
        let states = store.storage_baseline_states().await.unwrap();
        assert!(!states[0].legacy_accounting_hold);
        assert!(states[1].matches(&token));
        assert_eq!(
            store
                .submit(Mutation::RecoverMultipartStagingAccounting { limit: 1 })
                .await
                .unwrap(),
            MutationOutcome::MultipartAccountingHeld {
                baseline_id: token.baseline_id.clone()
            }
        );
        assert_eq!(
            store
                .submit(Mutation::CompleteStorageBaseline {
                    token: token.clone(),
                    completed_at: Timestamp(1)
                })
                .await
                .unwrap(),
            updated(StorageBaselineTransition::Applied)
        );
        assert!(
            store
                .storage_baseline_states()
                .await
                .unwrap()
                .iter()
                .all(|state| !state.legacy_accounting_hold
                    && state.coverage_identity.as_ref() == Some(&token.baseline_id))
        );
    }
}
