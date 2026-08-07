//! Applying one [`Mutation`] to the write connection. Each call runs inside its own savepoint
//! (managed by the writer), so returning `Err` rolls back only this mutation while its
//! batch-mates commit. Preconditions are evaluated here, inside the transaction, so the check
//! and the upsert are atomic with respect to every other writer (ARCH 11.6).

use crate::model::{self, engine_err, repl_op_str, repl_status_str, storage_class_str, to_json};
use cairn_types::MetaError;
use cairn_types::bucket::{
    ConfigAspect, DefaultRetention, ObjectLockConfiguration, RetentionPeriod, VersioningState,
};
use cairn_types::id::{BucketName, ObjectKey, StoragePath, VersionId};
use cairn_types::meta::{
    ClaimReleaseOutcome, IfNoneMatch, InitialObjectState, MAX_IMPORT_JOB_PRUNE_BATCH,
    MultipartCleanup, MultipartTerminalOutcome, Mutation, MutationOutcome, OutboxEntry,
    Precondition,
};
use cairn_types::object::{
    ETag, ExplicitObjectLockIntent, GovernanceBypass, ObjectLockMode, ObjectLockState,
    ObjectRetention, ObjectVersionRow,
};
use cairn_types::time::Timestamp;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;

type R<T> = Result<T, MetaError>;
type MultipartInitialColumns = (String, Option<String>, Option<i64>, Option<i64>, i64);

/// Apply a mutation, returning its typed outcome or a typed error.
pub fn apply(conn: &Connection, m: Mutation) -> R<MutationOutcome> {
    match m {
        Mutation::PutObjectVersion {
            row,
            precondition,
            initial_state,
            replication,
        } => put_version(conn, *row, &precondition, initial_state, replication),
        Mutation::ResolveObjectWrite {
            bucket,
            key,
            version_id,
            row_id,
            storage_path,
        } => {
            let referenced = conn
                .query_row(
                    "SELECT 1 FROM object_versions
                     WHERE bucket_name=?1 AND key=?2 AND version_id=?3
                       AND id=?4 AND storage_path=?5",
                    params![
                        bucket.as_str(),
                        key.as_str(),
                        version_id.as_str(),
                        row_id,
                        storage_path.as_str(),
                    ],
                    |_| Ok(()),
                )
                .optional()
                .map_err(engine_err)?
                .is_some();
            Ok(MutationOutcome::ObjectWriteResolved { referenced })
        }
        Mutation::CreateDeleteMarker {
            bucket,
            key,
            version_id,
            owner_id,
            now,
            bypass,
            expected_current,
            replication,
        } => {
            if let Some(expected) = expected_current {
                let still_current = conn
                    .query_row(
                        "SELECT 1 FROM object_versions
                         WHERE bucket_name=?1 AND key=?2 AND version_id=?3
                           AND updated_at=?4 AND is_latest=1 AND is_delete_marker=0",
                        params![
                            bucket.as_str(),
                            key.as_str(),
                            expected.version_id.as_str(),
                            expected.updated_at.0,
                        ],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(engine_err)?
                    .is_some();
                if !still_current {
                    return Ok(MutationOutcome::DeleteNotApplied);
                }
            }
            let row = ObjectVersionRow {
                id: uuid::Uuid::new_v4().simple().to_string(),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version_id.clone(),
                is_latest: true,
                is_delete_marker: true,
                size_logical: 0,
                size_physical: 0,
                etag: ETag::from_string(String::new()),
                content_type: String::new(),
                content_encoding: None,
                cache_control: None,
                content_disposition: None,
                content_language: None,
                expires: None,
                storage_path: None,
                compression: cairn_types::object::CompressionDescriptor::Uncompressed,
                storage_class: cairn_types::object::StorageClass::Standard,
                cold_locator: None,
                owner_id,
                user_metadata: Vec::new(),
                acl: None,
                checksums: Vec::new(),
                sse_descriptor: None,
                replication_status: None,
                replicated_at: None,
                created_at: now,
                updated_at: now,
            };
            if replacement_is_protected(conn, &row.bucket, &row.key, &row.version_id, now, bypass)?
            {
                return Ok(MutationOutcome::DeleteProtected);
            }
            let freed = upsert_version(conn, row)?;
            replace_object_tags(conn, &bucket, &key, &version_id, &[])?;
            write_object_lock_state(conn, &bucket, &key, &version_id, ObjectLockState::default())?;
            for e in &replication {
                enqueue(conn, e)?;
            }
            Ok(MutationOutcome::DeleteMarker { version_id, freed })
        }
        Mutation::DeleteVersion {
            bucket,
            key,
            version_id,
            expected_row_id,
            expected_updated_at,
            require_sole_key_version,
            now,
            bypass,
        } => delete_version(
            conn,
            &bucket,
            &key,
            &version_id,
            DeleteVersionGuard {
                expected_row_id,
                expected_updated_at,
                require_sole_key_version,
            },
            now,
            bypass,
        ),
        Mutation::CreateMultipart { session: s, limits } => {
            validate_multipart_lock_intent(conn, &s.bucket, s.lock_intent, s.created_at)?;
            let bucket_active = multipart_stat(
                conn,
                "multipart_bucket_stats",
                "bucket_name",
                s.bucket.as_str(),
                "active_uploads",
            )?;
            let principal_active = multipart_stat(
                conn,
                "multipart_principal_stats",
                "principal_id",
                s.initiated_by.0.as_str(),
                "active_uploads",
            )?;
            if bucket_active >= i64::from(limits.max_active_uploads_per_bucket)
                || principal_active >= i64::from(limits.max_active_uploads_per_principal)
            {
                return Err(MetaError::QuotaExceeded);
            }
            conn.execute(
                "INSERT INTO multipart_uploads
                 (id, bucket_name, key, content_type, status, owner_id, intended_acl, user_metadata,
                  sse_requested, encrypt_parts, sse_kms_requested, sse_kms_key_id,
                  sse_bucket_key_enabled, created_at, updated_at, initiated_by, initial_tags,
                  lock_mode, retain_until, legal_hold, object_lock_intent_known)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
                params![
                    s.upload_id.as_str(),
                    s.bucket.as_str(),
                    s.key.as_str(),
                    s.content_type,
                    model::mp_status_str(s.status),
                    s.owner_id.0,
                    s.intended_acl.as_ref().map(to_json),
                    to_json(&s.user_metadata),
                    s.sse_requested as i64,
                    s.encrypt_parts as i64,
                    s.sse_kms_requested as i64,
                    s.sse_kms_key_id,
                    s.sse_bucket_key_enabled as i64,
                    s.created_at.0,
                    s.updated_at.0,
                    s.initiated_by.0,
                    to_json(&s.initial_tags),
                    s.lock_intent
                        .retention
                        .map(|retention| model::lock_mode_str(retention.mode)),
                    s.lock_intent
                        .retention
                        .map(|retention| retention.retain_until.0),
                    s.lock_intent.legal_hold.map(i64::from),
                    1_i64,
                ],
            )
            .map_err(engine_err)?;
            adjust_multipart_stats(conn, s.bucket.as_str(), s.initiated_by.0.as_str(), 1, 0)?;
            Ok(MutationOutcome::MultipartCreated(s.upload_id))
        }
        Mutation::ReserveMultipartPart {
            upload_id,
            part_number,
            attempt_id,
            reserved_bytes,
            max_parts_per_upload,
            now,
        } => {
            let context = active_multipart_context(conn, &upload_id)?;
            let existing_attempt: Option<(String, i64, i64)> = conn
                .query_row(
                    "SELECT upload_id, part_number, reserved_bytes
                     FROM multipart_part_reservations WHERE attempt_id=?1",
                    params![attempt_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(engine_err)?;
            if let Some((existing_upload, existing_part, existing_bytes)) = existing_attempt {
                if existing_upload == upload_id.as_str()
                    && existing_part == i64::from(part_number)
                    && existing_bytes == reserved_bytes as i64
                {
                    return Ok(MutationOutcome::MultipartReserved);
                }
                return Err(MetaError::QuotaExceeded);
            }
            let distinct_parts: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM (
                         SELECT part_number FROM multipart_parts WHERE upload_id=?1
                         UNION
                         SELECT part_number FROM multipart_part_reservations WHERE upload_id=?1
                     )",
                    params![upload_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(engine_err)?;
            let number_exists: bool = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM multipart_parts WHERE upload_id=?1 AND part_number=?2
                         UNION ALL
                         SELECT 1 FROM multipart_part_reservations
                         WHERE upload_id=?1 AND part_number=?2
                     )",
                    params![upload_id.as_str(), part_number],
                    |row| row.get::<_, i64>(0),
                )
                .map(|value| value != 0)
                .map_err(engine_err)?;
            if !number_exists && distinct_parts >= i64::from(max_parts_per_upload) {
                return Err(MetaError::QuotaExceeded);
            }
            enforce_multipart_reservation_quota(conn, &context, reserved_bytes)?;
            conn.execute(
                "INSERT INTO multipart_part_reservations
                 (attempt_id, upload_id, part_number, reserved_bytes, created_at)
                 VALUES (?1,?2,?3,?4,?5)",
                params![
                    attempt_id,
                    upload_id.as_str(),
                    part_number,
                    reserved_bytes as i64,
                    now.0,
                ],
            )
            .map_err(|error| match engine_err(error) {
                MetaError::Conflict => MetaError::QuotaExceeded,
                other => other,
            })?;
            conn.execute(
                "UPDATE multipart_uploads SET updated_at=?2
                 WHERE id=?1 AND status='active'",
                params![upload_id.as_str(), now.0],
            )
            .map_err(engine_err)?;
            adjust_multipart_stats(
                conn,
                &context.bucket,
                &context.principal,
                0,
                reserved_bytes as i64,
            )?;
            Ok(MutationOutcome::MultipartReserved)
        }
        Mutation::ReleaseMultipartReservation {
            upload_id,
            attempt_id,
        } => {
            release_multipart_reservation(conn, &upload_id, &attempt_id)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RecordPart {
            upload_id,
            attempt_id,
            part,
        } => record_part(conn, &upload_id, &attempt_id, part),
        Mutation::ResolveMultipartPartWrite {
            upload_id,
            part_number,
            storage_path,
        } => {
            let referenced = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM multipart_parts
                         WHERE upload_id=?1 AND part_number=?2 AND storage_path=?3
                     )",
                    params![upload_id.as_str(), part_number, storage_path.as_str()],
                    |row| row.get::<_, i64>(0),
                )
                .map(|value| value != 0)
                .map_err(engine_err)?;
            Ok(MutationOutcome::MultipartPartWriteResolved { referenced })
        }
        Mutation::ReleaseMultipartCleanup { cleanup_id } => {
            release_multipart_cleanup(conn, &cleanup_id)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::ReleaseMultipartUploadCleanups { upload_id } => {
            release_multipart_upload_cleanups(conn, &upload_id)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RecoverMultipartStagingAccounting { limit } => {
            recover_multipart_staging_accounting(conn, limit)
        }
        Mutation::ClaimMultipart {
            upload_id,
            claim_token,
        } => claim_multipart(conn, &upload_id, &claim_token),
        Mutation::ReleaseMultipartClaim {
            upload_id,
            claim_token,
        } => {
            let released = conn
                .execute(
                    "UPDATE multipart_uploads
                     SET status='active', completion_claim_token=NULL
                     WHERE id=?1 AND status='completing' AND completion_claim_token=?2",
                    params![upload_id.as_str(), claim_token.as_str()],
                )
                .map_err(engine_err)?;
            Ok(MutationOutcome::MultipartClaimRelease(if released == 1 {
                ClaimReleaseOutcome::Released
            } else {
                ClaimReleaseOutcome::NotOwner
            }))
        }
        Mutation::RecoverMultipartClaims => {
            // No request survives a process restart, so every transient completion owner is an
            // orphan. Restore retryability without deleting its durable session or parts.
            conn.execute(
                "UPDATE multipart_uploads
                 SET status='active', completion_claim_token=NULL
                 WHERE status='completing'",
                [],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CompleteMultipart {
            upload_id,
            claim_token,
            row,
            precondition,
            replication,
        } => {
            let Some(initial_state) =
                multipart_initial_state(conn, &upload_id, &claim_token, &row.bucket, &row.key)?
            else {
                return Ok(MutationOutcome::MultipartTerminal(
                    MultipartTerminalOutcome::NotOwner,
                ));
            };
            let bucket = row.bucket.clone();
            let key = row.key.clone();
            check_precondition(conn, &bucket, &key, &precondition)?;
            enforce_bucket_quota(conn, &row)?;
            enforce_user_quota(conn, &row)?;
            if replacement_is_protected(
                conn,
                &bucket,
                &key,
                &row.version_id,
                row.created_at,
                GovernanceBypass::Denied,
            )? {
                return Err(MetaError::ObjectProtected);
            }
            let (tags, lock_state) = if row.is_delete_marker {
                (Vec::new(), ObjectLockState::default())
            } else {
                (
                    initial_state.tags,
                    resolve_initial_object_lock(
                        conn,
                        &bucket,
                        initial_state.lock_intent,
                        row.created_at,
                    )?,
                )
            };
            let version_id = row.version_id.clone();
            let superseded = upsert_version(conn, *row)?;
            replace_object_tags(conn, &bucket, &key, &version_id, &tags)?;
            write_object_lock_state(conn, &bucket, &key, &version_id, lock_state)?;
            retire_multipart_session(conn, &upload_id)?;
            for e in &replication {
                enqueue(conn, e)?;
            }
            Ok(MutationOutcome::MultipartTerminal(
                MultipartTerminalOutcome::Completed {
                    superseded,
                    version_id,
                },
            ))
        }
        Mutation::AbortMultipart(upload_id) => {
            let active = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM multipart_uploads WHERE id=?1 AND status='active'
                     )",
                    params![upload_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )
                .map(|value| value != 0)
                .map_err(engine_err)?;
            if active {
                retire_multipart_session(conn, &upload_id)?;
            }
            Ok(MutationOutcome::MultipartTerminal(if active {
                MultipartTerminalOutcome::Aborted
            } else {
                MultipartTerminalOutcome::NotOwner
            }))
        }
        Mutation::CreateBucket(b) => {
            insert_bucket(conn, &b)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CreateObjectLockBucket(b) => {
            if b.versioning != VersioningState::Enabled {
                return Err(MetaError::InvalidBucketState);
            }
            insert_bucket(conn, &b)?;
            let doc = to_json(&ObjectLockConfiguration {
                enabled: true,
                default_retention: None,
            });
            conn.execute(
                "INSERT INTO bucket_config (bucket_name, aspect, doc)
                 VALUES (?1, 'object_lock', ?2)",
                params![b.name.as_str(), doc],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeleteBucket(name) => {
            // Re-check emptiness INSIDE the savepoint so the check and the delete are atomic. The
            // protocol layer pre-checks too, but that read races a concurrent write (a PUT or
            // multipart initiate committing between the check and this delete would be orphaned:
            // stranded object_versions/multipart rows, corrupted counters, a leaked blob, and — since
            // rows are keyed by bucket name — cross-tenant exposure to a recreated same-name bucket).
            // Objects AND in-progress multipart uploads both keep a bucket non-empty (audit 2026-07).
            let non_empty: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM object_versions WHERE bucket_name=?1) \
                          OR EXISTS(SELECT 1 FROM multipart_uploads WHERE bucket_name=?1)",
                    params![name.as_str()],
                    |r| r.get(0),
                )
                .map_err(engine_err)?;
            if non_empty {
                return Err(MetaError::NotEmpty);
            }
            conn.execute(
                "DELETE FROM bucket_config WHERE bucket_name=?1",
                params![name.as_str()],
            )
            .map_err(engine_err)?;
            conn.execute("DELETE FROM buckets WHERE name=?1", params![name.as_str()])
                .map_err(engine_err)?;
            // A bucket is empty when deleted, so its roll-up row is already zero; drop it to keep
            // the counter table from accumulating tombstones for recreated bucket names.
            conn.execute(
                "DELETE FROM bucket_stats WHERE bucket_name=?1",
                params![name.as_str()],
            )
            .map_err(engine_err)?;
            // Take the bucket's usage-analytics with it: every per-bucket request_metrics row
            // (ARCH 26.5) keyed to this bucket is dropped in the same commit, so its history does not
            // linger and a recreated bucket of the same name never inherits the old series. Rows for
            // non-bucket operations (bucket_name '') are untouched.
            conn.execute(
                "DELETE FROM request_metrics WHERE bucket_name=?1",
                params![name.as_str()],
            )
            .map_err(engine_err)?;
            // Shares are account-global and intentionally have no cross-shard bucket FK. Delete
            // every capability for the name so deleting/recreating a bucket can never revive an
            // old link against new object bytes. The sharded adapter executes this same mutation
            // on shard 0 before the authoritative nonzero-shard bucket delete.
            conn.execute(
                "DELETE FROM object_shares WHERE bucket_name=?1",
                params![name.as_str()],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetBucketConfig {
            bucket,
            aspect,
            doc,
        } => {
            if aspect == ConfigAspect::ObjectLock {
                // Object Lock has specialized writer mutations because enablement is immutable and
                // coupled atomically to bucket versioning. The generic document seam must never be
                // able to delete, disable, or install a malformed WORM configuration.
                return Err(MetaError::InvalidBucketState);
            }
            let aspect_s = config_aspect_str(aspect);
            match doc {
                Some(d) => conn.execute(
                    "INSERT OR REPLACE INTO bucket_config (bucket_name, aspect, doc) VALUES (?1,?2,?3)",
                    params![bucket.as_str(), aspect_s, d.0],
                ),
                None => conn.execute(
                    "DELETE FROM bucket_config WHERE bucket_name=?1 AND aspect=?2",
                    params![bucket.as_str(), aspect_s],
                ),
            }
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::UpdateObjectLockConfiguration {
            bucket,
            default_retention,
        } => {
            if let Some(default) = default_retention {
                default
                    .validate()
                    .map_err(|_| MetaError::InvalidObjectLockState)?;
            }
            let versioning: Option<String> = conn
                .query_row(
                    "SELECT versioning_state FROM buckets WHERE name=?1",
                    params![bucket.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(engine_err)?;
            if versioning.as_deref() != Some("enabled") {
                return Err(MetaError::InvalidBucketState);
            }
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM bucket_config
                         WHERE bucket_name=?1 AND aspect='object_lock'
                     )",
                    params![bucket.as_str()],
                    |row| row.get::<_, i64>(0),
                )
                .map(|value| value != 0)
                .map_err(engine_err)?;
            if !exists {
                return Err(MetaError::InvalidBucketState);
            }
            let doc = to_json(&ObjectLockConfiguration {
                enabled: true,
                default_retention,
            });
            conn.execute(
                "UPDATE bucket_config SET doc=?2
                 WHERE bucket_name=?1 AND aspect='object_lock'",
                params![bucket.as_str(), doc],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetObjectRetention {
            bucket,
            key,
            version_id,
            retention,
            now,
            bypass,
        } => {
            require_object_version(conn, &bucket, &key, &version_id)?;
            require_object_lock_enabled(conn, &bucket)?;
            if retention.is_some_and(|value| value.retain_until <= now) {
                return Err(MetaError::InvalidObjectLockState);
            }
            let mut state = read_object_lock_state(conn, &bucket, &key, &version_id)?;
            enforce_retention_transition(state.retention, retention, now, bypass)?;
            state.retention = retention;
            write_object_lock_state(conn, &bucket, &key, &version_id, state)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetObjectLegalHold {
            bucket,
            key,
            version_id,
            on,
        } => {
            require_object_version(conn, &bucket, &key, &version_id)?;
            require_object_lock_enabled(conn, &bucket)?;
            let mut state = read_object_lock_state(conn, &bucket, &key, &version_id)?;
            state.legal_hold = on;
            write_object_lock_state(conn, &bucket, &key, &version_id, state)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetVersioning { bucket, state } => {
            if state != VersioningState::Enabled {
                let has_object_lock_row: bool = conn
                    .query_row(
                        "SELECT EXISTS(
                             SELECT 1 FROM bucket_config
                             WHERE bucket_name=?1 AND aspect='object_lock'
                         )",
                        params![bucket.as_str()],
                        |row| row.get::<_, i64>(0),
                    )
                    .map(|value| value != 0)
                    .map_err(engine_err)?;
                if has_object_lock_row {
                    return Err(MetaError::InvalidBucketState);
                }
            }
            conn.execute(
                "UPDATE buckets SET versioning_state=?2 WHERE name=?1",
                params![bucket.as_str(), model::versioning_str(state)],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetOwnership { bucket, mode } => {
            conn.execute(
                "UPDATE buckets SET ownership_mode=?2 WHERE name=?1",
                params![bucket.as_str(), model::ownership_str(mode)],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetBucketQuota {
            bucket,
            quota_bytes,
        } => {
            conn.execute(
                "UPDATE buckets SET quota_bytes=?2 WHERE name=?1",
                params![bucket.as_str(), quota_bytes.map(|q| q as i64)],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetBucketCompression { bucket, policy } => {
            conn.execute(
                "UPDATE buckets SET compression_policy=?2 WHERE name=?1",
                params![bucket.as_str(), policy.as_ref().map(to_json)],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetUserPolicy { user_id, policy } => {
            conn.execute(
                "UPDATE users SET policy=?2 WHERE id=?1",
                params![user_id.0.as_str(), policy],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetUserQuota {
            user_id,
            quota_bytes,
        } => {
            conn.execute(
                "UPDATE users SET quota_bytes=?2 WHERE id=?1",
                params![user_id.0.as_str(), quota_bytes.map(|q| q as i64)],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RetryFailedReplication { bucket, now } => {
            // Reset `attempts=0`: a terminally-failed entry sits at the max-attempts boundary, so
            // requeuing without clearing the count would re-fail on the very next attempt.
            match bucket {
                Some(b) => conn.execute(
                    "UPDATE replication_outbox SET status='pending', next_attempt_at=?2, attempts=0, lease_until=NULL \
                     WHERE status='failed' AND bucket_name=?1",
                    params![b.as_str(), now.0],
                ),
                None => conn.execute(
                    "UPDATE replication_outbox SET status='pending', next_attempt_at=?1, attempts=0, lease_until=NULL \
                     WHERE status='failed'",
                    params![now.0],
                ),
            }
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RequeueReplicationVersions {
            bucket,
            only_encrypted,
            now,
            after_key,
            limit,
        } => {
            // (0) THE KEY PAGE. Both DML statements below are driven from this one page, so the
            // outbox and the ledger can never disagree about a key.
            //
            // Paging by ROW instead of by key is a correctness bug, not a slower option: a bare
            // `WHERE rowid IN (SELECT rowid … LIMIT ?)` has no ORDER BY, so SQLite serves rows via
            // `idx_outbox_status_next`, which groups every `completed` row ahead of every `failed`
            // row. A key whose OLDER encrypted version is `failed` and whose NEWER version is
            // `completed` would get the newer row requeued pages before the older one, the
            // heartbeat would ship it in between, and the mirror would revert to the old bytes (or
            // resurrect a deleted key, when the newer row is a delete marker). See the
            // `Mutation::RequeueReplicationVersions` doc.
            //
            // The UNION is load-bearing: a key whose outbox row was already pruned by the retention
            // sweep still carries a `completed` ledger stamp that has to be reset. `after_key` makes
            // the sweep monotone forward — each pass resumes strictly past the previous page's last
            // key, so a full requeue is linear rather than quadratic. Migration v23's
            // `idx_outbox_bucket_key` is what makes the outbox arm of this an index seek; the
            // ledger arm rides the `UNIQUE (bucket_name, key, version_id)` auto-index.
            //
            // `only_encrypted` correlates on (bucket_name, key) ONLY — never on version_id, for the
            // same ordering reason. With it false the OR short-circuits and the EXISTS is never
            // evaluated.
            let page: Vec<String> = {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT u.k FROM ( \
                             SELECT DISTINCT o.key AS k FROM replication_outbox o \
                             WHERE o.bucket_name=?1 AND o.status IN ('completed','failed') \
                               AND (?2 IS NULL OR o.key > ?2) \
                             UNION \
                             SELECT DISTINCT v.key AS k FROM object_versions v \
                             WHERE v.bucket_name=?1 AND v.replication_status IN ('completed','failed') \
                               AND (?2 IS NULL OR v.key > ?2) \
                         ) AS u \
                         WHERE ?3 = 0 OR EXISTS ( \
                             SELECT 1 FROM object_versions ov \
                             WHERE ov.bucket_name=?1 AND ov.key=u.k AND ov.sse_descriptor IS NOT NULL) \
                         ORDER BY u.k LIMIT ?4",
                    )
                    .map_err(engine_err)?;
                let rows = stmt
                    .query_map(
                        params![
                            bucket.as_str(),
                            after_key.as_deref(),
                            i64::from(only_encrypted),
                            limit
                        ],
                        |r| r.get::<_, String>(0),
                    )
                    .map_err(engine_err)?;
                rows.collect::<rusqlite::Result<Vec<String>>>()
                    .map_err(engine_err)?
            };
            let Some(page_end) = page.last().cloned() else {
                // Drained: nothing at or past the cursor. The caller stops here.
                return Ok(MutationOutcome::RowsRequeued {
                    rows: 0,
                    page_end: None,
                });
            };
            // Both UPDATEs run over the CLOSED KEY RANGE the page covers, so every terminal row of
            // every key in the page moves in this one transaction together with its siblings. A key
            // is therefore never split across a batch boundary — including the pathological key with
            // an enormous version history, whose rows are deliberately NOT split (splitting them is
            // the bug this shape exists to prevent).
            //
            // (1) The outbox half. `EnqueueReplication`'s INSERT OR IGNORE cannot resurrect a
            // terminal row, so a repeat resync inside the retention window is a silent no-op; this
            // is the only way to re-ship a version whose entry already reads `completed`.
            // `attempts=0` matches `RetryFailedReplication`: a `failed` entry sits at the
            // max-attempts boundary and would re-fail on the very next attempt otherwise.
            let outbox = conn.execute(
                "UPDATE replication_outbox SET status='pending', next_attempt_at=?2, attempts=0, lease_until=NULL \
                 WHERE bucket_name=?1 AND status IN ('completed','failed') \
                   AND (?3 IS NULL OR key > ?3) AND key <= ?4 \
                   AND (?5 = 0 OR EXISTS ( \
                        SELECT 1 FROM object_versions ov \
                        WHERE ov.bucket_name = replication_outbox.bucket_name \
                          AND ov.key = replication_outbox.key \
                          AND ov.sse_descriptor IS NOT NULL))",
                params![
                    bucket.as_str(),
                    now.0,
                    after_key.as_deref(),
                    page_end,
                    i64::from(only_encrypted)
                ],
            )
            .map_err(engine_err)?;
            // (2) The ledger half, over the SAME key range: if a key's plaintext version is being
            // re-shipped, its ledger stamp must stop reading `completed` too, or the audit and the
            // queue disagree. Filtering on `IN ('completed','failed')` already excludes `replica`
            // (loop prevention, ARCH 20.4) and `pending`/`claimed` (live work), so the statement is
            // idempotent and can never resurrect an inbound replica for re-shipping.
            //
            // `replicated_at` is deliberately LEFT ALONE. It records when the version last shipped,
            // and it has not shipped again yet; clearing it here would make a repair in flight
            // indistinguishable from a version that never replicated, and re-stamping it would
            // declare success before the ship. `MarkReplicationDone` advances it, and only it.
            //
            // THE LEDGER HALF IS NARROWER THAN THE OUTBOX HALF, and that asymmetry is the accurate
            // rule rather than a workaround. `pending` in the ledger is a claim that *something*
            // will ship this version. Two populations can honour that claim:
            //
            //   * `is_latest = 1` — the resync backfill that follows a forced requeue enumerates
            //     `list_current`, so a current version gets a fresh `INSERT OR IGNORE` entry;
            //   * a version that still HAS an outbox row for this exact (bucket, key, version_id) —
            //     the outbox half above just moved it back to `pending`, so the queue holds it.
            //     (The existence test is status-agnostic on purpose: that UPDATE has already run in
            //     this same transaction, so these rows now read `pending`, not `completed`.)
            //
            // Everything else — a NON-CURRENT version whose outbox row the retention sweep already
            // pruned, which 24 h after an incident is essentially all of them — has no queue entry
            // and nothing that will ever create one. Flipping it to `pending` would make the ledger
            // claim queued work that no queue holds: the audit's `repair_pending` gauge would count
            // it forever, the runbook's `repair_pending == 0` done-state would be unreachable, and
            // an alert on it would fire permanently. Leaving it `completed` is the truth — it is
            // still a suspect replica, it is reported as `non_current_suspect`, and TRAP 2 already
            // tells the operator non-current versions are unrepairable without rebuilding the
            // destination bucket.
            let ledger = conn
                .execute(
                    "UPDATE object_versions SET replication_status=?2 \
                     WHERE bucket_name=?1 AND replication_status IN ('completed','failed') \
                       AND (?3 IS NULL OR key > ?3) AND key <= ?4 \
                       AND (is_latest = 1 OR EXISTS ( \
                            SELECT 1 FROM replication_outbox ob \
                            WHERE ob.bucket_name = object_versions.bucket_name \
                              AND ob.key = object_versions.key \
                              AND ob.version_id = object_versions.version_id)) \
                       AND (?5 = 0 OR EXISTS ( \
                            SELECT 1 FROM object_versions ov \
                            WHERE ov.bucket_name = object_versions.bucket_name \
                              AND ov.key = object_versions.key \
                              AND ov.sse_descriptor IS NOT NULL))",
                    params![
                        bucket.as_str(),
                        repl_status_str(cairn_types::meta::ReplicationStatus::Pending),
                        after_key.as_deref(),
                        page_end,
                        i64::from(only_encrypted)
                    ],
                )
                .map_err(engine_err)?;
            Ok(MutationOutcome::RowsRequeued {
                rows: (outbox + ledger) as u64,
                page_end: Some(page_end),
            })
        }
        Mutation::SetAccountPublicAccessBlock(bpa) => {
            conn.execute(
                "INSERT OR REPLACE INTO account_config (k, v) VALUES ('public_access_block', ?1)",
                params![to_json(&bpa)],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::PutObjectTags {
            bucket,
            key,
            version_id,
            tags,
        } => {
            replace_object_tags(conn, &bucket, &key, &version_id, &tags)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeleteObjectTags {
            bucket,
            key,
            version_id,
        } => {
            conn.execute(
                "DELETE FROM object_tags WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                params![bucket.as_str(), key.as_str(), version_id.as_str()],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetObjectAcl {
            bucket,
            key,
            version_id,
            acl,
        } => {
            // Replace the version row's `acl` JSON column; `None` stores SQL NULL (clears it).
            conn.execute(
                "UPDATE object_versions SET acl=?4 WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                params![
                    bucket.as_str(),
                    key.as_str(),
                    version_id.as_str(),
                    acl.as_ref().map(to_json),
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CreateUser(rec) => {
            let c = model::user_record_columns(&rec);
            conn.execute(
                "INSERT INTO users
                 (id, display_name, access_key_id, secret_hash, sigv4_access_key_id,
                  sigv4_secret_ciphertext, sigv4_secret_nonce, role, is_active, created_at, updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    c.id, c.display_name, c.access_key_id, c.secret_hash, c.sigv4_access_key_id,
                    c.sigv4_secret_ciphertext, c.sigv4_secret_nonce, c.role, c.is_active,
                    c.created_at, c.updated_at
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::UserCreated(rec.user.id.clone()))
        }
        Mutation::UpdateUser(rec) => {
            // A column-scoped UPDATE, NOT `INSERT OR REPLACE`: the latter delete-and-reinserts the
            // row, nulling the `policy` and `quota_bytes` columns it does not list — silently wiping
            // a user's identity policy and quota on every role/credential change (audit #10). This
            // UPDATE touches only the mutable identity columns and leaves policy/quota untouched.
            let c = model::user_record_columns(&rec);
            conn.execute(
                "UPDATE users SET
                   display_name=?2, access_key_id=?3, secret_hash=?4, sigv4_access_key_id=?5,
                   sigv4_secret_ciphertext=?6, sigv4_secret_nonce=?7, role=?8, is_active=?9,
                   created_at=?10, updated_at=?11
                 WHERE id=?1",
                params![
                    c.id,
                    c.display_name,
                    c.access_key_id,
                    c.secret_hash,
                    c.sigv4_access_key_id,
                    c.sigv4_secret_ciphertext,
                    c.sigv4_secret_nonce,
                    c.role,
                    c.is_active,
                    c.created_at,
                    c.updated_at
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeactivateUser(id) => {
            conn.execute("UPDATE users SET is_active=0 WHERE id=?1", params![id.0])
                .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeleteUser(id) => {
            // Remove everything that lets the user act, in one commit: their session credentials and
            // the user row itself (which carries the identity policy column). The authenticator's
            // cached principal is keyed off the now-gone record, so access is denied as soon as its
            // epoch is bumped. Bucket ownership is checked by the caller (a bucket cannot be
            // orphaned). We deliberately do NOT touch user_stats: objects the user uploaded into
            // other owners' buckets stay (their owner_id becomes a historical dangling id, which is
            // harmless), and `user_stats.logical_bytes` must stay equal to the sum of those
            // still-owned object sizes — an enforced integrity invariant — so deleting one of those
            // objects later decrements the existing row toward zero instead of re-creating it with a
            // negative balance.
            conn.execute(
                "DELETE FROM session_credentials WHERE parent_user_id=?1",
                params![id.0],
            )
            .map_err(engine_err)?;
            conn.execute("DELETE FROM users WHERE id=?1", params![id.0])
                .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CreateSessionCredential(rec) => {
            conn.execute(
                "INSERT INTO session_credentials
                 (access_key_id, parent_user_id, secret_ciphertext, secret_nonce,
                  session_token_hash, inline_policy, expires_at, created_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    rec.access_key_id,
                    rec.parent_user_id.0,
                    rec.secret_ciphertext,
                    rec.secret_nonce,
                    rec.session_token_hash,
                    rec.inline_policy,
                    rec.expires_at.0,
                    rec.created_at.0,
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeleteExpiredSessionCredentials { before } => {
            conn.execute(
                "DELETE FROM session_credentials WHERE expires_at < ?1",
                params![before.0],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeleteSessionCredential { access_key_id } => {
            conn.execute(
                "DELETE FROM session_credentials WHERE access_key_id = ?1",
                params![access_key_id],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CreateImportJob(rec) => {
            conn.execute(
                "INSERT INTO import_jobs
                 (id, source_endpoint, source_region, access_key_id, secret_ciphertext, secret_nonce,
                  ca_cert_pem, insecure_skip_verify, workers, state, buckets_json, objects_done,
                  objects_total, bytes_done, bytes_total, last_error, lease_until, created_at, updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
                params![
                    rec.id,
                    rec.source_endpoint,
                    rec.source_region,
                    rec.access_key_id,
                    rec.secret_ciphertext,
                    rec.secret_nonce,
                    rec.ca_cert_pem,
                    rec.insecure_skip_verify,
                    rec.workers as i64,
                    model::import_state_str(rec.state),
                    model::to_json(&rec.buckets),
                    rec.objects_done as i64,
                    rec.objects_total as i64,
                    rec.bytes_done as i64,
                    rec.bytes_total as i64,
                    rec.last_error,
                    rec.lease_until.map(|t| t.0),
                    rec.created_at.0,
                    rec.updated_at.0,
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::UpdateImportJobProgress {
            id,
            buckets,
            objects_done,
            objects_total,
            bytes_done,
            bytes_total,
            last_error,
            lease_until,
            updated_at,
        } => {
            // Column-scoped UPDATE (never touches `state`), mirroring the `UpdateUser` posture.
            conn.execute(
                "UPDATE import_jobs SET
                   buckets_json=?2, objects_done=?3, objects_total=?4, bytes_done=?5, bytes_total=?6,
                   last_error=?7, lease_until=?8, updated_at=?9
                 WHERE id=?1",
                params![
                    id,
                    model::to_json(&buckets),
                    objects_done as i64,
                    objects_total as i64,
                    bytes_done as i64,
                    bytes_total as i64,
                    last_error,
                    lease_until.map(|t| t.0),
                    updated_at.0,
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetImportJobState {
            id,
            state,
            last_error,
            lease_until,
            updated_at,
        } => {
            conn.execute(
                "UPDATE import_jobs SET state=?2, last_error=?3, lease_until=?4, updated_at=?5
                 WHERE id=?1",
                params![
                    id,
                    model::import_state_str(state),
                    last_error,
                    lease_until.map(|t| t.0),
                    updated_at.0,
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::PruneImportJobs { before_ms, limit } => {
            let limit = i64::from(limit.clamp(1, MAX_IMPORT_JOB_PRUNE_BATCH));
            let rows = conn
                .execute(
                    "DELETE FROM import_jobs
                 WHERE id IN (
                     SELECT id FROM import_jobs
                     WHERE state IN ('completed','failed','cancelled') AND updated_at < ?1
                     LIMIT ?2
                 )",
                    params![before_ms, limit],
                )
                .map_err(engine_err)?;
            Ok(MutationOutcome::ImportJobsPruned(rows as u64))
        }
        Mutation::ClaimReplicationBatch {
            limit,
            now,
            lease_secs,
        } => claim_replication_batch(conn, limit, now, lease_secs),
        Mutation::MarkReplicationDone { id, now } => {
            if let Some((bucket, key, version)) = conn
                .query_row(
                    "SELECT bucket_name, key, version_id FROM replication_outbox WHERE id=?1",
                    params![id],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(engine_err)?
            {
                // Preserve a `replica` marker: a version that arrived here via replication must keep
                // that status for loop prevention (ARCH 20.4) even when a stray outbox entry for it is
                // drained. `IS NOT` is NULL-safe (a NULL/other status is stamped Completed).
                //
                // `replicated_at` moves in the SAME statement as the status (schema v23). The status
                // alone is not enough for the replication audit: a version that shipped BEFORE a fix
                // and one that was force-requeued and has since re-shipped correctly both read
                // `completed`, and `created_at` is never rewritten — so without this stamp the audit
                // gauge can never fall back to zero after a successful repair. Deliberately not
                // `updated_at`, which feeds the client-visible S3 `LastModified`.
                conn.execute(
                    "UPDATE object_versions SET replication_status=?4, replicated_at=?5 \
                     WHERE bucket_name=?1 AND key=?2 AND version_id=?3 AND replication_status IS NOT 'replica'",
                    params![bucket, key, version, repl_status_str(cairn_types::meta::ReplicationStatus::Completed), now.0],
                )
                .map_err(engine_err)?;
            }
            conn.execute(
                "UPDATE replication_outbox SET status='completed' WHERE id=?1",
                params![id],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::MarkReplicationFailed {
            id,
            error,
            next_attempt_at,
        } => {
            match next_attempt_at {
                Some(t) => conn.execute(
                    "UPDATE replication_outbox SET attempts=attempts+1, last_error=?2, next_attempt_at=?3, status='pending' WHERE id=?1",
                    params![id, error, t.0],
                ),
                None => conn.execute(
                    "UPDATE replication_outbox SET attempts=attempts+1, last_error=?2, status='failed' WHERE id=?1",
                    params![id, error],
                ),
            }
            .map_err(engine_err)?;
            // On a TERMINAL failure, stamp the version's replication_status=failed for operator
            // visibility — via a SURGICAL update keyed to the outbox row's (bucket,key,version),
            // exactly like MarkReplicationDone. The replication engine must NOT re-upsert the whole
            // version row for this (the old stamp_version_status did), because that forces is_latest
            // and would demote a newer version written during the ship window or resurrect one
            // deleted meanwhile (audit 2026-07).
            if next_attempt_at.is_none() {
                if let Some((bucket, key, version)) = conn
                    .query_row(
                        "SELECT bucket_name, key, version_id FROM replication_outbox WHERE id=?1",
                        params![id],
                        |r| {
                            Ok((
                                r.get::<_, String>(0)?,
                                r.get::<_, String>(1)?,
                                r.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(engine_err)?
                {
                    conn.execute(
                        "UPDATE object_versions SET replication_status=?4 \
                         WHERE bucket_name=?1 AND key=?2 AND version_id=?3 AND replication_status IS NOT 'replica'",
                        params![bucket, key, version, repl_status_str(cairn_types::meta::ReplicationStatus::Failed)],
                    )
                    .map_err(engine_err)?;
                }
            }
            Ok(MutationOutcome::Ack)
        }
        Mutation::EnqueueReplication(e) => {
            // Idempotent: a repeated resync of the same (rule, key, version) — which produces the
            // same deterministic entry id — is a no-op rather than a duplicate or a PK error.
            conn.execute(
                "INSERT OR IGNORE INTO replication_outbox
                 (id, bucket_name, key, version_id, operation, rule_id, target_arn, attempts, next_attempt_at, status, last_error, priority, lease_until, enqueued_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                params![
                    e.id,
                    e.bucket.as_str(),
                    e.key.as_str(),
                    e.version_id.as_str(),
                    repl_op_str(e.operation),
                    e.rule_id,
                    e.target_arn,
                    e.attempts as i64,
                    e.next_attempt_at.0,
                    repl_status_str(e.status),
                    e.last_error,
                    e.priority,
                    e.lease_until.map(|t| t.0),
                    e.enqueued_at.0,
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeferReplication {
            id,
            next_attempt_at,
            last_error,
        } => {
            // Release the claim (lease_until=NULL, status='pending') and re-schedule WITHOUT
            // touching `attempts` — a deferral/unavailability is not a failure, so it must never
            // push the entry toward terminal. `COALESCE(?3, last_error)` keeps the prior error when
            // no new one is supplied (an ordering defer).
            conn.execute(
                "UPDATE replication_outbox \
                 SET status='pending', lease_until=NULL, next_attempt_at=?2, \
                     last_error=COALESCE(?3, last_error) \
                 WHERE id=?1",
                params![id, next_attempt_at.0, last_error],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RecoverClaimedReplication => {
            // Startup recovery: every `claimed` row is orphaned (no live worker holds it), so release
            // them to `pending` for immediate re-claim instead of waiting out the 300s lease.
            conn.execute(
                "UPDATE replication_outbox SET status='pending', lease_until=NULL WHERE status='claimed'",
                [],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::PruneReplicationOutbox { before_ms } => {
            // Reclaim terminal rows (completed/failed) older than the horizon; never touch
            // pending/claimed (outstanding work). Keeps the outbox bounded and auto-clears stale
            // failures. The per-key ordering check treats an absent row exactly like a completed
            // one, so dropping completed rows is safe.
            conn.execute(
                "DELETE FROM replication_outbox \
                 WHERE status IN ('completed','failed') AND enqueued_at < ?1",
                params![before_ms],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::PruneEventsOutbox { before_ms } => {
            // Reclaim terminal webhook rows (delivered rows are deleted on MarkWebhookDone, so only
            // 'failed' rows persist) older than the horizon; never touch pending/claimed work. Keeps
            // events_outbox bounded so a dead sink can't bloat the metadata DB (audit 2026-07).
            // events_outbox has no enqueued_at column, so age off the indexed next_attempt_at.
            conn.execute(
                "DELETE FROM events_outbox WHERE status='failed' AND next_attempt_at < ?1",
                params![before_ms],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::EnqueueWebhooks(entries) => {
            for e in &entries {
                enqueue_webhook(conn, e)?;
            }
            Ok(MutationOutcome::Ack)
        }
        Mutation::ClaimWebhookBatch {
            limit,
            now,
            lease_secs,
        } => claim_webhook_batch(conn, limit, now, lease_secs),
        Mutation::MarkWebhookDone(id) => {
            // A delivered (or dropped) entry has no further use — delete it outright rather than
            // leaving a `completed` row, so the common success path keeps `events_outbox` bounded
            // (only pending + terminally-failed rows persist).
            conn.execute("DELETE FROM events_outbox WHERE id=?1", params![id])
                .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::MarkWebhookFailed {
            id,
            error,
            next_attempt_at,
        } => {
            match next_attempt_at {
                Some(t) => conn.execute(
                    "UPDATE events_outbox SET attempts=attempts+1, last_error=?2, next_attempt_at=?3, status='pending' WHERE id=?1",
                    params![id, error, t.0],
                ),
                None => conn.execute(
                    "UPDATE events_outbox SET attempts=attempts+1, last_error=?2, status='failed' WHERE id=?1",
                    params![id, error],
                ),
            }
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RecordActivity(e) => {
            conn.prepare_cached(
                "INSERT INTO activity (id, action, bucket, key, size, etag, actor, at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            )
            .map_err(engine_err)?
            .execute(params![
                e.id,
                e.action,
                e.bucket,
                e.key,
                e.size.map(|s| s as i64),
                e.etag,
                e.actor,
                e.at.0
            ])
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CreateShare(s) => {
            conn.execute(
                "INSERT INTO object_shares
                 (id, token_hash, bucket_name, key, version_id, expires_at, disposition, filename, created_by, created_at, revoked_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    s.id,
                    s.token_hash.as_bytes().as_slice(),
                    s.bucket.as_str(),
                    s.key.as_str(),
                    s.version_id.as_ref().map(|v| v.as_str()),
                    s.expires_at.map(|t| t.0),
                    model::disposition_str(s.disposition),
                    s.filename,
                    s.created_by.0,
                    s.created_at.0,
                    s.revoked_at.map(|t| t.0),
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RevokeShare { id, now } => {
            // Idempotent: revoking an already-revoked or missing id is a no-op.
            conn.execute(
                "UPDATE object_shares SET revoked_at=?2 WHERE id=?1 AND revoked_at IS NULL",
                params![id, now.0],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RecordRequestMetrics { rows, prune_before } => {
            // Accumulate each window/op/bucket/status bucket; the composite PK upsert sums counts,
            // bytes, and latency histogram so repeated flushes never double-insert (ARCH 26.5).
            for r in &rows {
                conn.prepare_cached(
                    "INSERT INTO request_metrics
                     (ts_bucket, operation, bucket_name, status_class, count,
                      bytes_in, bytes_out, lat_sum_ms,
                      lat_le_5, lat_le_20, lat_le_50, lat_le_200, lat_le_1000, lat_gt_1000)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)
                     ON CONFLICT(ts_bucket, operation, bucket_name, status_class)
                     DO UPDATE SET
                        count       = count       + excluded.count,
                        bytes_in    = bytes_in    + excluded.bytes_in,
                        bytes_out   = bytes_out   + excluded.bytes_out,
                        lat_sum_ms  = lat_sum_ms  + excluded.lat_sum_ms,
                        lat_le_5    = lat_le_5    + excluded.lat_le_5,
                        lat_le_20   = lat_le_20   + excluded.lat_le_20,
                        lat_le_50   = lat_le_50   + excluded.lat_le_50,
                        lat_le_200  = lat_le_200  + excluded.lat_le_200,
                        lat_le_1000 = lat_le_1000 + excluded.lat_le_1000,
                        lat_gt_1000 = lat_gt_1000 + excluded.lat_gt_1000",
                )
                .map_err(engine_err)?
                .execute(params![
                    r.ts_bucket,
                    r.operation,
                    r.bucket,
                    r.status_class,
                    r.count as i64,
                    r.bytes_in as i64,
                    r.bytes_out as i64,
                    r.lat_sum_ms as i64,
                    r.lat_hist[0] as i64,
                    r.lat_hist[1] as i64,
                    r.lat_hist[2] as i64,
                    r.lat_hist[3] as i64,
                    r.lat_hist[4] as i64,
                    r.lat_hist[5] as i64,
                ])
                .map_err(engine_err)?;
            }
            if let Some(before) = prune_before {
                conn.prepare_cached("DELETE FROM request_metrics WHERE ts_bucket < ?1")
                    .map_err(engine_err)?
                    .execute(params![before])
                    .map_err(engine_err)?;
            }
            Ok(MutationOutcome::Ack)
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredObjectLockConfiguration {
    enabled: bool,
    default_retention: Option<StoredDefaultRetention>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDefaultRetention {
    mode: ObjectLockMode,
    period: RetentionPeriod,
}

fn bucket_object_lock_configuration(
    conn: &Connection,
    bucket: &BucketName,
) -> R<Option<ObjectLockConfiguration>> {
    let doc: Option<String> = conn
        .query_row(
            "SELECT doc FROM bucket_config
             WHERE bucket_name=?1 AND aspect='object_lock'",
            params![bucket.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(engine_err)?;
    let Some(doc) = doc else {
        return Ok(None);
    };
    let versioning: Option<String> = conn
        .query_row(
            "SELECT versioning_state FROM buckets WHERE name=?1",
            params![bucket.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(engine_err)?;
    if versioning.as_deref() != Some("enabled") {
        return Err(MetaError::InvalidBucketState);
    }
    let stored: StoredObjectLockConfiguration =
        serde_json::from_str(&doc).map_err(|_| MetaError::InvalidObjectLockState)?;
    if !stored.enabled {
        return Err(MetaError::InvalidObjectLockState);
    }
    let default_retention = stored.default_retention.map(|default| DefaultRetention {
        mode: default.mode,
        period: default.period,
    });
    if let Some(default) = default_retention {
        default
            .validate()
            .map_err(|_| MetaError::InvalidObjectLockState)?;
    }
    Ok(Some(ObjectLockConfiguration {
        enabled: true,
        default_retention,
    }))
}

fn require_object_lock_enabled(
    conn: &Connection,
    bucket: &BucketName,
) -> R<ObjectLockConfiguration> {
    bucket_object_lock_configuration(conn, bucket)?.ok_or(MetaError::InvalidBucketState)
}

fn resolve_initial_object_lock(
    conn: &Connection,
    bucket: &BucketName,
    intent: ExplicitObjectLockIntent,
    now: Timestamp,
) -> R<ObjectLockState> {
    let config = bucket_object_lock_configuration(conn, bucket)?;
    let Some(config) = config else {
        if intent.retention.is_some() || intent.legal_hold.is_some() {
            return Err(MetaError::InvalidBucketState);
        }
        return Ok(ObjectLockState::default());
    };
    if intent
        .retention
        .is_some_and(|retention| retention.retain_until <= now)
    {
        return Err(MetaError::InvalidObjectLockState);
    }
    let retention = match (intent.retention, config.default_retention) {
        (Some(retention), _) => Some(retention),
        (None, Some(default)) => Some(ObjectRetention {
            mode: default.mode,
            retain_until: default
                .retain_until(now)
                .map_err(|_| MetaError::InvalidObjectLockState)?,
        }),
        (None, None) => None,
    };
    Ok(ObjectLockState {
        retention,
        legal_hold: intent.legal_hold.unwrap_or(false),
    })
}

fn validate_multipart_lock_intent(
    conn: &Connection,
    bucket: &BucketName,
    intent: ExplicitObjectLockIntent,
    now: Timestamp,
) -> R<()> {
    let config = bucket_object_lock_configuration(conn, bucket)?;
    if config.is_none() {
        return if intent.retention.is_none() && intent.legal_hold.is_none() {
            Ok(())
        } else {
            Err(MetaError::InvalidBucketState)
        };
    }
    if intent
        .retention
        .is_some_and(|retention| retention.retain_until <= now)
    {
        return Err(MetaError::InvalidObjectLockState);
    }
    // The default is intentionally not resolved here. Initiation pins only explicit intent;
    // CompleteMultipart resolves the then-current default from the completion row's creation time.
    Ok(())
}

fn read_object_lock_state(
    conn: &Connection,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: &VersionId,
) -> R<ObjectLockState> {
    let row: Option<(Option<String>, Option<i64>, i64)> = conn
        .query_row(
            "SELECT lock_mode, retain_until, legal_hold FROM object_locks
             WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
            params![bucket.as_str(), key.as_str(), version_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(engine_err)?;
    row.map_or_else(
        || Ok(ObjectLockState::default()),
        |(mode, until, legal_hold)| model::object_lock_state_from_columns(mode, until, legal_hold),
    )
}

fn write_object_lock_state(
    conn: &Connection,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: &VersionId,
    state: ObjectLockState,
) -> R<()> {
    if state == ObjectLockState::default() {
        conn.execute(
            "DELETE FROM object_locks
             WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
            params![bucket.as_str(), key.as_str(), version_id.as_str()],
        )
        .map_err(engine_err)?;
        return Ok(());
    }
    let mode = state
        .retention
        .map(|retention| model::lock_mode_str(retention.mode));
    let until = state.retention.map(|retention| retention.retain_until.0);
    conn.execute(
        "INSERT INTO object_locks
         (bucket_name, key, version_id, lock_mode, retain_until, legal_hold)
         VALUES (?1,?2,?3,?4,?5,?6)
         ON CONFLICT(bucket_name,key,version_id) DO UPDATE SET
             lock_mode=excluded.lock_mode,
             retain_until=excluded.retain_until,
             legal_hold=excluded.legal_hold",
        params![
            bucket.as_str(),
            key.as_str(),
            version_id.as_str(),
            mode,
            until,
            i64::from(state.legal_hold),
        ],
    )
    .map_err(engine_err)?;
    Ok(())
}

fn require_object_version(
    conn: &Connection,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: &VersionId,
) -> R<()> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM object_versions
                 WHERE bucket_name=?1 AND key=?2 AND version_id=?3
             )",
            params![bucket.as_str(), key.as_str(), version_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| value != 0)
        .map_err(engine_err)?;
    if exists {
        Ok(())
    } else {
        Err(MetaError::ObjectVersionNotFound)
    }
}

fn replacement_is_protected(
    conn: &Connection,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: &VersionId,
    now: Timestamp,
    bypass: GovernanceBypass,
) -> R<bool> {
    // A malformed/disabled Object Lock document or an enabled document paired with suspended
    // versioning makes the bucket's protection policy unknowable. Every destructive/replacement
    // path fails closed until the specialized repair path restores a valid configuration.
    bucket_object_lock_configuration(conn, bucket)?;
    let state = read_object_lock_state(conn, bucket, key, version_id)?;
    if state.legal_hold {
        return Ok(true);
    }
    Ok(match state.retention {
        Some(retention) if retention.retain_until > now => match retention.mode {
            ObjectLockMode::Compliance => true,
            ObjectLockMode::Governance => bypass != GovernanceBypass::Authorized,
        },
        _ => false,
    })
}

fn enforce_retention_transition(
    current: Option<ObjectRetention>,
    requested: Option<ObjectRetention>,
    now: Timestamp,
    bypass: GovernanceBypass,
) -> R<()> {
    let Some(current) = current.filter(|retention| retention.retain_until > now) else {
        return Ok(());
    };
    let non_weakening = match (current.mode, requested) {
        (ObjectLockMode::Compliance, Some(next)) => {
            next.mode == ObjectLockMode::Compliance && next.retain_until >= current.retain_until
        }
        (ObjectLockMode::Governance, Some(next)) => next.retain_until >= current.retain_until,
        (_, None) => false,
    };
    if non_weakening
        || (current.mode == ObjectLockMode::Governance && bypass == GovernanceBypass::Authorized)
    {
        Ok(())
    } else {
        Err(MetaError::ObjectProtected)
    }
}

fn replace_object_tags(
    conn: &Connection,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: &VersionId,
    tags: &[(String, String)],
) -> R<()> {
    conn.execute(
        "DELETE FROM object_tags WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        params![bucket.as_str(), key.as_str(), version_id.as_str()],
    )
    .map_err(engine_err)?;
    for (tag_key, tag_value) in tags {
        conn.execute(
            "INSERT INTO object_tags
             (bucket_name, key, version_id, tag_key, tag_value)
             VALUES (?1,?2,?3,?4,?5)",
            params![
                bucket.as_str(),
                key.as_str(),
                version_id.as_str(),
                tag_key,
                tag_value,
            ],
        )
        .map_err(engine_err)?;
    }
    Ok(())
}

fn multipart_initial_state(
    conn: &Connection,
    upload_id: &cairn_types::UploadId,
    claim_token: &cairn_types::MultipartClaimToken,
    bucket: &BucketName,
    key: &ObjectKey,
) -> R<Option<InitialObjectState>> {
    let row: Option<MultipartInitialColumns> = conn
        .query_row(
            "SELECT initial_tags, lock_mode, retain_until, legal_hold, object_lock_intent_known
             FROM multipart_uploads
             WHERE id=?1 AND status='completing' AND completion_claim_token=?2
               AND bucket_name=?3 AND key=?4",
            params![
                upload_id.as_str(),
                claim_token.as_str(),
                bucket.as_str(),
                key.as_str()
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(engine_err)?;
    let Some((tags, mode, until, legal_hold, intent_known)) = row else {
        return Ok(None);
    };
    if intent_known == 0 {
        // A migrated pre-v27 session cannot prove that no explicit retention/legal-hold header was
        // supplied. Completion on an Object-Lock bucket must preserve the session and fail closed;
        // an ordinary bucket may consume only the migration's known-empty defaults.
        if bucket_object_lock_configuration(conn, bucket)?.is_some() {
            return Err(MetaError::InvalidObjectLockState);
        }
        let tags: Vec<(String, String)> =
            serde_json::from_str(&tags).map_err(|_| MetaError::InvalidObjectLockState)?;
        if !tags.is_empty() || mode.is_some() || until.is_some() || legal_hold.is_some() {
            return Err(MetaError::InvalidObjectLockState);
        }
        return Ok(Some(InitialObjectState::default()));
    }
    if intent_known != 1 {
        return Err(MetaError::InvalidObjectLockState);
    }
    let tags = serde_json::from_str(&tags)
        .map_err(|_| MetaError::Engine("invalid multipart initial tags".to_owned()))?;
    let retention = match (mode, until) {
        (None, None) => None,
        (Some(mode), Some(until)) => Some(ObjectRetention {
            mode: model::lock_mode_from(&mode)?,
            retain_until: Timestamp(until),
        }),
        _ => return Err(MetaError::InvalidObjectLockState),
    };
    let legal_hold = match legal_hold {
        None => None,
        Some(0) => Some(false),
        Some(1) => Some(true),
        Some(_) => return Err(MetaError::InvalidObjectLockState),
    };
    Ok(Some(InitialObjectState {
        tags,
        lock_intent: ExplicitObjectLockIntent {
            retention,
            legal_hold,
        },
    }))
}

fn insert_bucket(conn: &Connection, bucket: &cairn_types::bucket::Bucket) -> R<()> {
    // `compression_policy` is the spec column name (ARCH 34.1); `quota_bytes` defaults to NULL
    // (unlimited) because the domain row deliberately carries no quota field.
    conn.execute(
        "INSERT INTO buckets
         (name, owner_id, created_at, versioning_state, ownership_mode, region, compression_policy)
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![
            bucket.name.as_str(),
            bucket.owner_id.0,
            bucket.created_at.0,
            model::versioning_str(bucket.versioning),
            model::ownership_str(bucket.ownership_mode),
            bucket.region,
            bucket.compression.as_ref().map(to_json),
        ],
    )
    .map_err(engine_err)?;
    Ok(())
}

fn put_version(
    conn: &Connection,
    row: ObjectVersionRow,
    precondition: &Precondition,
    initial_state: InitialObjectState,
    replication: Vec<OutboxEntry>,
) -> R<MutationOutcome> {
    check_precondition(conn, &row.bucket, &row.key, precondition)?;
    enforce_bucket_quota(conn, &row)?;
    enforce_user_quota(conn, &row)?;
    if replacement_is_protected(
        conn,
        &row.bucket,
        &row.key,
        &row.version_id,
        row.created_at,
        GovernanceBypass::Denied,
    )? {
        return Err(MetaError::ObjectProtected);
    }
    let (tags, lock_state) = if row.is_delete_marker {
        (Vec::new(), ObjectLockState::default())
    } else {
        (
            initial_state.tags,
            resolve_initial_object_lock(
                conn,
                &row.bucket,
                initial_state.lock_intent,
                row.created_at,
            )?,
        )
    };
    let bucket = row.bucket.clone();
    let key = row.key.clone();
    let version_id = row.version_id.clone();
    let superseded = upsert_version(conn, row)?;
    replace_object_tags(conn, &bucket, &key, &version_id, &tags)?;
    write_object_lock_state(conn, &bucket, &key, &version_id, lock_state)?;
    for e in &replication {
        enqueue(conn, e)?;
    }
    Ok(MutationOutcome::Put {
        superseded,
        version_id,
    })
}

/// Enforce a bucket's optional byte quota inside the commit transaction (ARCH 27.5/28.2).
///
/// If the target bucket has a non-NULL `quota_bytes`, this rejects the write — with
/// [`MetaError::QuotaExceeded`], which rolls back only this mutation's savepoint — when the
/// bucket's resulting logical bytes would exceed the quota. The existing row at the same
/// (bucket, key, version_id), if any, is excluded from the current total because the upsert
/// replaces it. Delete markers carry no logical bytes, so they never trip the quota.
fn enforce_bucket_quota(conn: &Connection, row: &ObjectVersionRow) -> R<()> {
    let quota: Option<i64> = conn
        .prepare_cached("SELECT quota_bytes FROM buckets WHERE name=?1")
        .map_err(engine_err)?
        .query_row(params![row.bucket.as_str()], |r| r.get(0))
        .optional()
        .map_err(engine_err)?
        .flatten();
    let Some(quota) = quota else {
        return Ok(());
    };
    // Current logical bytes in the bucket, read O(1) from the maintained counter (Phase 2.1/2.2)
    // instead of summing every version, minus the row this upsert will replace (if present).
    let total: i64 = conn
        .prepare_cached("SELECT logical_bytes FROM bucket_stats WHERE bucket_name=?1")
        .map_err(engine_err)?
        .query_row(params![row.bucket.as_str()], |r| r.get(0))
        .optional()
        .map_err(engine_err)?
        .unwrap_or(0);
    let existing: i64 = conn
        .prepare_cached(
            "SELECT size_logical FROM object_versions
             WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        )
        .map_err(engine_err)?
        .query_row(
            params![
                row.bucket.as_str(),
                row.key.as_str(),
                row.version_id.as_str()
            ],
            |r| r.get(0),
        )
        .optional()
        .map_err(engine_err)?
        .unwrap_or(0);
    let current = (total - existing).max(0);
    // Saturating add in u128 so a pathological size can never wrap past the quota check.
    let projected = u128::from(current as u64) + u128::from(row.size_logical);
    if projected > u128::from(quota.max(0) as u64) {
        return Err(MetaError::QuotaExceeded);
    }
    Ok(())
}

/// Enforce the owning user's optional byte quota inside the commit transaction (ARCH 27.5).
///
/// Mirrors [`enforce_bucket_quota`] but scoped to the row's `owner_id`: if that user has a
/// non-NULL `quota_bytes`, the write is rejected with [`MetaError::QuotaExceeded`] when the
/// user's resulting logical bytes — summed over `size_logical` of every `object_versions` row
/// they own across all buckets — would exceed the quota. The existing row at the same
/// (bucket, key, version_id), if any, is excluded because the upsert replaces it. Delete
/// markers carry no logical bytes, so they never trip the quota.
fn enforce_user_quota(conn: &Connection, row: &ObjectVersionRow) -> R<()> {
    let quota: Option<i64> = conn
        .prepare_cached("SELECT quota_bytes FROM users WHERE id=?1")
        .map_err(engine_err)?
        .query_row(params![row.owner_id.0.as_str()], |r| r.get(0))
        .optional()
        .map_err(engine_err)?
        .flatten();
    let Some(quota) = quota else {
        return Ok(());
    };
    // Current logical bytes owned by this user across all buckets, read O(1) from the maintained
    // counter (Phase 2.1/2.2), minus the row this upsert replaces — but only when that existing
    // row is owned by THIS user (otherwise it is not part of this user's total to begin with).
    let total: i64 = conn
        .prepare_cached("SELECT logical_bytes FROM user_stats WHERE owner_id=?1")
        .map_err(engine_err)?
        .query_row(params![row.owner_id.0.as_str()], |r| r.get(0))
        .optional()
        .map_err(engine_err)?
        .unwrap_or(0);
    let existing: i64 = conn
        .prepare_cached(
            "SELECT size_logical FROM object_versions
             WHERE bucket_name=?1 AND key=?2 AND version_id=?3 AND owner_id=?4",
        )
        .map_err(engine_err)?
        .query_row(
            params![
                row.bucket.as_str(),
                row.key.as_str(),
                row.version_id.as_str(),
                row.owner_id.0.as_str()
            ],
            |r| r.get(0),
        )
        .optional()
        .map_err(engine_err)?
        .unwrap_or(0);
    let current = (total - existing).max(0);
    // Saturating add in u128 so a pathological size can never wrap past the quota check.
    let projected = u128::from(current as u64) + u128::from(row.size_logical);
    if projected > u128::from(quota.max(0) as u64) {
        return Err(MetaError::QuotaExceeded);
    }
    Ok(())
}

/// Replace any existing row at (bucket,key,version_id) — capturing its blob for reclamation —
/// demote the key's other versions, and insert the new latest row.
fn upsert_version(conn: &Connection, mut row: ObjectVersionRow) -> R<Option<StoragePath>> {
    // Read the row this upsert replaces (if any): its blob to reclaim, plus its owner and byte
    // sizes so the roll-up counters can be decremented for it before the replacement is inserted.
    let existing: Option<(Option<String>, String, i64, i64)> = conn
        .prepare_cached(
            "SELECT storage_path, owner_id, size_logical, size_physical
             FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        )
        .map_err(engine_err)?
        .query_row(
            params![
                row.bucket.as_str(),
                row.key.as_str(),
                row.version_id.as_str()
            ],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(engine_err)?;
    conn.prepare_cached(
        "DELETE FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
    )
    .map_err(engine_err)?
    .execute(params![
        row.bucket.as_str(),
        row.key.as_str(),
        row.version_id.as_str()
    ])
    .map_err(engine_err)?;
    let superseded = match &existing {
        Some((sp, owner, sl, sp_bytes)) => {
            // The replaced row leaves the table: subtract its version and bytes from the counters.
            adjust_stats(conn, row.bucket.as_str(), owner, -1, -sl, -sp_bytes)?;
            sp.clone()
        }
        None => None,
    };
    // A replicated write carries the SOURCE's (uuidv7, time-ordered) version id, which may be OLDER
    // than a version already present here (a local write, or an out-of-order / re-delivered replica).
    // It becomes the latest only if its id is the maximum for the key, so an older replica never
    // demotes a newer version (AWS S3 CRR-style version-id ordering, ARCH 20.4). A normal write
    // always carries a fresh (max) id, so it keeps the unconditional last-write-is-latest behaviour.
    // Replicas only occur in versioned buckets, so every id compared here is a uuidv7 hex (the
    // unversioned `null` sentinel is never a replica), and `MAX(version_id)` runs AFTER the same-id
    // delete above, so it reflects the other versions a re-delivery must not jump ahead of.
    let becomes_latest =
        if row.replication_status == Some(cairn_types::meta::ReplicationStatus::Replica) {
            let max_other: Option<String> = conn
                .prepare_cached(
                    "SELECT MAX(version_id) FROM object_versions WHERE bucket_name=?1 AND key=?2",
                )
                .map_err(engine_err)?
                .query_row(params![row.bucket.as_str(), row.key.as_str()], |r| r.get(0))
                .optional()
                .map_err(engine_err)?
                .flatten();
            max_other.is_none_or(|m| row.version_id.as_str() >= m.as_str())
        } else {
            true
        };
    if becomes_latest {
        demote_latest(conn, &row.bucket, &row.key)?;
    }
    row.is_latest = becomes_latest;
    insert_version(conn, &row)?;
    Ok(superseded.map(StoragePath::from_string))
}

fn demote_latest(conn: &Connection, bucket: &BucketName, key: &ObjectKey) -> R<()> {
    conn.prepare_cached(
        "UPDATE object_versions SET is_latest=0 WHERE bucket_name=?1 AND key=?2 AND is_latest=1",
    )
    .map_err(engine_err)?
    .execute(params![bucket.as_str(), key.as_str()])
    .map_err(engine_err)?;
    Ok(())
}

/// Apply a signed delta to the maintained roll-up counters (Phase 2.1, ARCH 30) for `bucket` and
/// `owner`. One accumulating upsert per table, run in the same transaction as the `object_versions`
/// row change that produced the delta, so the counters never diverge from the table across a commit
/// boundary. `versions`/byte totals sum over ALL versions, matching the prior scan semantics; the
/// current-visible `objects` count is not tracked here (it stays an index-only count).
fn adjust_stats(
    conn: &Connection,
    bucket: &str,
    owner: &str,
    d_versions: i64,
    d_logical: i64,
    d_physical: i64,
) -> R<()> {
    conn.prepare_cached(
        "INSERT INTO bucket_stats (bucket_name, versions, logical_bytes, physical_bytes)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bucket_name) DO UPDATE SET
            versions       = versions       + excluded.versions,
            logical_bytes  = logical_bytes  + excluded.logical_bytes,
            physical_bytes = physical_bytes + excluded.physical_bytes",
    )
    .map_err(engine_err)?
    .execute(params![bucket, d_versions, d_logical, d_physical])
    .map_err(engine_err)?;
    conn.prepare_cached(
        "INSERT INTO user_stats (owner_id, logical_bytes) VALUES (?1, ?2)
         ON CONFLICT(owner_id) DO UPDATE SET logical_bytes = logical_bytes + excluded.logical_bytes",
    )
    .map_err(engine_err)?
    .execute(params![owner, d_logical])
    .map_err(engine_err)?;
    Ok(())
}

fn insert_version(conn: &Connection, row: &ObjectVersionRow) -> R<()> {
    conn.prepare_cached(
        "INSERT INTO object_versions
         (id, bucket_name, key, version_id, is_latest, is_delete_marker, size_logical, size_physical,
          etag, content_type, content_encoding, cache_control, content_disposition, content_language,
          expires, storage_path, compression, storage_class, cold_locator, owner_id,
          user_metadata, acl, checksums, sse_descriptor, replication_status, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27)",
    )
    .map_err(engine_err)?
    .execute(params![
            row.id,
            row.bucket.as_str(),
            row.key.as_str(),
            row.version_id.as_str(),
            i64::from(row.is_latest),
            i64::from(row.is_delete_marker),
            row.size_logical as i64,
            row.size_physical as i64,
            row.etag.as_str(),
            row.content_type,
            row.content_encoding,
            row.cache_control,
            row.content_disposition,
            row.content_language,
            row.expires,
            row.storage_path.as_ref().map(|p| p.as_str().to_owned()),
            to_json(&row.compression),
            storage_class_str(row.storage_class),
            row.cold_locator,
            row.owner_id.0,
            to_json(&row.user_metadata),
            row.acl.as_ref().map(to_json),
            to_json(&row.checksums),
            row.sse_descriptor,
            row.replication_status.map(repl_status_str),
            row.created_at.0,
            row.updated_at.0,
        ])
    .map_err(engine_err)?;
    // Maintain the roll-up counters in lockstep: this new row adds one version and its bytes.
    adjust_stats(
        conn,
        row.bucket.as_str(),
        row.owner_id.0.as_str(),
        1,
        row.size_logical as i64,
        row.size_physical as i64,
    )?;
    Ok(())
}

struct DeleteVersionGuard {
    expected_row_id: Option<String>,
    expected_updated_at: Option<Timestamp>,
    require_sole_key_version: bool,
}

fn delete_version(
    conn: &Connection,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: &VersionId,
    guard: DeleteVersionGuard,
    now: Timestamp,
    bypass: GovernanceBypass,
) -> R<MutationOutcome> {
    // Lifecycle compare-and-delete guards are checked in the writer savepoint. The immutable row id
    // distinguishes two sentinel versions even when a concurrent overwrite lands in the same
    // timestamp tick; updated_at remains a defense-in-depth freshness check.
    if guard.expected_row_id.is_some() || guard.expected_updated_at.is_some() {
        let stored: Option<(String, i64)> = conn
            .query_row(
                "SELECT id, updated_at FROM object_versions
                 WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                params![bucket.as_str(), key.as_str(), version_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(engine_err)?;
        let matches = stored.as_ref().is_some_and(|(row_id, updated_at)| {
            guard
                .expected_row_id
                .as_ref()
                .is_none_or(|expected| expected == row_id)
                && guard
                    .expected_updated_at
                    .is_none_or(|expected| expected.0 == *updated_at)
        });
        if !matches {
            return Ok(MutationOutcome::DeleteNotApplied);
        }
    }
    if guard.require_sole_key_version {
        let (version_count, guarded_marker): (i64, i64) = conn
            .query_row(
                "SELECT COUNT(*),
                        COALESCE(MAX(CASE
                            WHEN version_id=?3 AND is_latest=1 AND is_delete_marker=1 THEN 1
                            ELSE 0
                        END), 0)
                 FROM object_versions
                 WHERE bucket_name=?1 AND key=?2",
                params![bucket.as_str(), key.as_str(), version_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(engine_err)?;
        if version_count != 1 || guarded_marker != 1 {
            return Ok(MutationOutcome::DeleteNotApplied);
        }
    }
    // Read the row's blob, latest flag, and owner/byte sizes before deleting it, so we can both
    // promote a successor and decrement the roll-up counters for the removed version.
    let existing: Option<(Option<String>, i64, String, i64, i64)> = conn
        .prepare_cached(
            "SELECT storage_path, is_latest, owner_id, size_logical, size_physical
             FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        )
        .map_err(engine_err)?
        .query_row(
            params![bucket.as_str(), key.as_str(), version_id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()
        .map_err(engine_err)?;
    let Some((storage_path, latest, owner, logical, physical)) = existing else {
        return Ok(MutationOutcome::DeleteNotApplied);
    };
    if replacement_is_protected(conn, bucket, key, version_id, now, bypass)? {
        return Ok(MutationOutcome::DeleteProtected);
    }
    let freed = storage_path.map(StoragePath::from_string);
    let was_latest = latest != 0;
    conn.prepare_cached(
        "DELETE FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
    )
    .map_err(engine_err)?
    .execute(params![bucket.as_str(), key.as_str(), version_id.as_str()])
    .map_err(engine_err)?;
    // Drop any Object Lock side-row for the removed version (a locked version can only reach here
    // once its retention has expired and no legal hold remains; see the protocol-layer enforcement).
    conn.prepare_cached(
        "DELETE FROM object_locks WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
    )
    .map_err(engine_err)?
    .execute(params![bucket.as_str(), key.as_str(), version_id.as_str()])
    .map_err(engine_err)?;
    // Drop the version's tags too. object_tags has no FK/ON DELETE CASCADE to object_versions
    // (schema.rs), so without this the tags outlive the version and, since an unversioned bucket
    // reuses the `null` version id, a later object re-created at the same key silently inherits the
    // dead object's tags — mis-firing tag lifecycle/replication rules. The in-memory reference double
    // already clears them; this brings both SQL engines into agreement (audit 2026-07).
    conn.prepare_cached(
        "DELETE FROM object_tags WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
    )
    .map_err(engine_err)?
    .execute(params![bucket.as_str(), key.as_str(), version_id.as_str()])
    .map_err(engine_err)?;
    // The deleted row leaves the table: subtract its version and bytes from the counters.
    adjust_stats(conn, bucket.as_str(), &owner, -1, -logical, -physical)?;
    let mut promoted = false;
    if was_latest {
        let promote: Option<String> = conn
            .prepare_cached(
                "SELECT id FROM object_versions WHERE bucket_name=?1 AND key=?2 ORDER BY version_id DESC LIMIT 1",
            )
            .map_err(engine_err)?
            .query_row(params![bucket.as_str(), key.as_str()], |r| r.get(0))
            .optional()
            .map_err(engine_err)?;
        if let Some(id) = promote {
            conn.prepare_cached("UPDATE object_versions SET is_latest=1 WHERE id=?1")
                .map_err(engine_err)?
                .execute(params![id])
                .map_err(engine_err)?;
            promoted = true;
        }
    }
    Ok(MutationOutcome::Deleted {
        freed,
        promoted_latest: promoted,
    })
}

struct MultipartContext {
    bucket: String,
    principal: String,
    updated_at: i64,
}

fn active_multipart_context(
    conn: &Connection,
    upload_id: &cairn_types::UploadId,
) -> R<MultipartContext> {
    conn.query_row(
        "SELECT bucket_name, COALESCE(initiated_by, owner_id), updated_at
         FROM multipart_uploads WHERE id=?1 AND status='active'",
        params![upload_id.as_str()],
        |row| {
            Ok(MultipartContext {
                bucket: row.get(0)?,
                principal: row.get(1)?,
                updated_at: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(engine_err)?
    .ok_or(MetaError::MultipartNotActive)
}

fn multipart_context(conn: &Connection, upload_id: &cairn_types::UploadId) -> R<MultipartContext> {
    conn.query_row(
        "SELECT bucket_name, COALESCE(initiated_by, owner_id), updated_at
         FROM multipart_uploads WHERE id=?1",
        params![upload_id.as_str()],
        |row| {
            Ok(MultipartContext {
                bucket: row.get(0)?,
                principal: row.get(1)?,
                updated_at: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(engine_err)?
    .ok_or(MetaError::MultipartNotActive)
}

fn multipart_stat(
    conn: &Connection,
    table: &str,
    key_column: &str,
    key: &str,
    value_column: &str,
) -> R<i64> {
    conn.query_row(
        &format!("SELECT {value_column} FROM {table} WHERE {key_column}=?1"),
        params![key],
        |row| row.get(0),
    )
    .optional()
    .map_err(engine_err)
    .map(|value| value.unwrap_or(0))
}

fn adjust_multipart_stats(
    conn: &Connection,
    bucket: &str,
    principal: &str,
    active_delta: i64,
    bytes_delta: i64,
) -> R<()> {
    conn.execute(
        "INSERT OR IGNORE INTO multipart_bucket_stats
         (bucket_name, active_uploads, staged_bytes) VALUES (?1,0,0)",
        params![bucket],
    )
    .map_err(engine_err)?;
    conn.execute(
        "UPDATE multipart_bucket_stats
         SET active_uploads=active_uploads+?2, staged_bytes=staged_bytes+?3
         WHERE bucket_name=?1",
        params![bucket, active_delta, bytes_delta],
    )
    .map_err(engine_err)?;
    conn.execute(
        "INSERT OR IGNORE INTO multipart_principal_stats
         (principal_id, active_uploads, staged_bytes) VALUES (?1,0,0)",
        params![principal],
    )
    .map_err(engine_err)?;
    conn.execute(
        "UPDATE multipart_principal_stats
         SET active_uploads=active_uploads+?2, staged_bytes=staged_bytes+?3
         WHERE principal_id=?1",
        params![principal, active_delta, bytes_delta],
    )
    .map_err(engine_err)?;
    Ok(())
}

fn enforce_multipart_reservation_quota(
    conn: &Connection,
    context: &MultipartContext,
    reserved_bytes: u64,
) -> R<()> {
    let bucket_quota: Option<i64> = conn
        .query_row(
            "SELECT quota_bytes FROM buckets WHERE name=?1",
            params![context.bucket],
            |row| row.get(0),
        )
        .optional()
        .map_err(engine_err)?
        .flatten();
    if let Some(quota) = bucket_quota {
        let committed = multipart_stat(
            conn,
            "bucket_stats",
            "bucket_name",
            &context.bucket,
            "logical_bytes",
        )?;
        let staged = multipart_stat(
            conn,
            "multipart_bucket_stats",
            "bucket_name",
            &context.bucket,
            "staged_bytes",
        )?;
        let projected = u128::from(committed.max(0) as u64)
            + u128::from(staged.max(0) as u64)
            + u128::from(reserved_bytes);
        if projected > u128::from(quota.max(0) as u64) {
            return Err(MetaError::QuotaExceeded);
        }
    }

    let principal_quota: Option<i64> = conn
        .query_row(
            "SELECT quota_bytes FROM users WHERE id=?1",
            params![context.principal],
            |row| row.get(0),
        )
        .optional()
        .map_err(engine_err)?
        .flatten();
    if let Some(quota) = principal_quota {
        let committed = multipart_stat(
            conn,
            "user_stats",
            "owner_id",
            &context.principal,
            "logical_bytes",
        )?;
        let staged = multipart_stat(
            conn,
            "multipart_principal_stats",
            "principal_id",
            &context.principal,
            "staged_bytes",
        )?;
        let projected = u128::from(committed.max(0) as u64)
            + u128::from(staged.max(0) as u64)
            + u128::from(reserved_bytes);
        if projected > u128::from(quota.max(0) as u64) {
            return Err(MetaError::QuotaExceeded);
        }
    }
    Ok(())
}

fn release_multipart_reservation(
    conn: &Connection,
    upload_id: &cairn_types::UploadId,
    attempt_id: &str,
) -> R<()> {
    let row: Option<(String, String, i64)> = conn
        .query_row(
            "SELECT u.bucket_name, COALESCE(u.initiated_by, u.owner_id), r.reserved_bytes
             FROM multipart_part_reservations r
             JOIN multipart_uploads u ON u.id=r.upload_id
             WHERE r.attempt_id=?1 AND r.upload_id=?2",
            params![attempt_id, upload_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(engine_err)?;
    let Some((bucket, principal, bytes)) = row else {
        return Ok(());
    };
    conn.execute(
        "DELETE FROM multipart_part_reservations WHERE attempt_id=?1 AND upload_id=?2",
        params![attempt_id, upload_id.as_str()],
    )
    .map_err(engine_err)?;
    adjust_multipart_stats(conn, &bucket, &principal, 0, -bytes)
}

fn record_part(
    conn: &Connection,
    upload_id: &cairn_types::UploadId,
    attempt_id: &str,
    part: cairn_types::meta::PartRecord,
) -> R<MutationOutcome> {
    let context = active_multipart_context(conn, upload_id)?;
    let reservation: Option<(i64, i64, i64)> = conn
        .query_row(
            "SELECT part_number, reserved_bytes, created_at FROM multipart_part_reservations
             WHERE attempt_id=?1 AND upload_id=?2",
            params![attempt_id, upload_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(engine_err)?;
    let Some((reserved_part, reserved_bytes, reserved_at)) = reservation else {
        return Err(MetaError::MultipartNotActive);
    };
    if reserved_part != i64::from(part.part_number) || part.size != reserved_bytes as u64 {
        return Err(MetaError::QuotaExceeded);
    }

    let previous: Option<(String, i64)> = conn
        .query_row(
            "SELECT storage_path, size FROM multipart_parts
             WHERE upload_id=?1 AND part_number=?2",
            params![upload_id.as_str(), part.part_number],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(engine_err)?;
    let cleanup = previous.map(|(storage_path, bytes)| MultipartCleanup {
        id: format!("part:{attempt_id}"),
        upload_id: upload_id.clone(),
        bucket: BucketName::parse(&context.bucket).expect("stored bucket name is validated"),
        principal_id: cairn_types::UserId(context.principal.clone()),
        bytes: bytes as u64,
        storage_path: Some(StoragePath::from_string(storage_path)),
        created_at: Timestamp(reserved_at),
    });
    if let Some(debt) = &cleanup {
        conn.execute(
            "INSERT INTO multipart_staging_cleanups
             (id, upload_id, bucket_name, principal_id, bytes, storage_path, created_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                debt.id,
                debt.upload_id.as_str(),
                debt.bucket.as_str(),
                debt.principal_id.0,
                debt.bytes as i64,
                debt.storage_path.as_ref().map(StoragePath::as_str),
                debt.created_at.0,
            ],
        )
        .map_err(engine_err)?;
        conn.execute(
            "UPDATE multipart_parts
             SET size=?3, etag=?4, storage_path=?5, checksum=?6, part_dek=?7
             WHERE upload_id=?1 AND part_number=?2",
            params![
                upload_id.as_str(),
                part.part_number,
                part.size as i64,
                part.etag,
                part.storage_path.as_str(),
                part.checksum.as_ref().map(to_json),
                part.part_dek,
            ],
        )
        .map_err(engine_err)?;
    } else {
        conn.execute(
            "INSERT INTO multipart_parts
             (upload_id, part_number, size, etag, storage_path, checksum, part_dek)
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                upload_id.as_str(),
                part.part_number,
                part.size as i64,
                part.etag,
                part.storage_path.as_str(),
                part.checksum.as_ref().map(to_json),
                part.part_dek,
            ],
        )
        .map_err(engine_err)?;
    }
    conn.execute(
        "DELETE FROM multipart_part_reservations WHERE attempt_id=?1",
        params![attempt_id],
    )
    .map_err(engine_err)?;
    adjust_multipart_stats(
        conn,
        &context.bucket,
        &context.principal,
        0,
        part.size as i64 - reserved_bytes,
    )?;
    Ok(MutationOutcome::PartRecorded { cleanup })
}

fn release_multipart_cleanup(conn: &Connection, cleanup_id: &str) -> R<()> {
    let row: Option<(String, String, i64)> = conn
        .query_row(
            "SELECT bucket_name, principal_id, bytes
             FROM multipart_staging_cleanups WHERE id=?1",
            params![cleanup_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(engine_err)?;
    let Some((bucket, principal, bytes)) = row else {
        return Ok(());
    };
    conn.execute(
        "DELETE FROM multipart_staging_cleanups WHERE id=?1",
        params![cleanup_id],
    )
    .map_err(engine_err)?;
    adjust_multipart_stats(conn, &bucket, &principal, 0, -bytes)
}

fn release_multipart_upload_cleanups(
    conn: &Connection,
    upload_id: &cairn_types::UploadId,
) -> R<()> {
    let row: Option<(String, String, i64)> = conn
        .query_row(
            "SELECT bucket_name, principal_id, COALESCE(SUM(bytes),0)
             FROM multipart_staging_cleanups WHERE upload_id=?1
             GROUP BY bucket_name, principal_id",
            params![upload_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(engine_err)?;
    let Some((bucket, principal, bytes)) = row else {
        return Ok(());
    };
    conn.execute(
        "DELETE FROM multipart_staging_cleanups WHERE upload_id=?1",
        params![upload_id.as_str()],
    )
    .map_err(engine_err)?;
    adjust_multipart_stats(conn, &bucket, &principal, 0, -bytes)
}

fn retire_multipart_session(conn: &Connection, upload_id: &cairn_types::UploadId) -> R<()> {
    let context = multipart_context(conn, upload_id)?;
    let part_bytes: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(size),0) FROM multipart_parts WHERE upload_id=?1",
            params![upload_id.as_str()],
            |row| row.get(0),
        )
        .map_err(engine_err)?;
    let reservation_bytes: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(reserved_bytes),0)
             FROM multipart_part_reservations WHERE upload_id=?1",
            params![upload_id.as_str()],
            |row| row.get(0),
        )
        .map_err(engine_err)?;
    let current_bytes = part_bytes + reservation_bytes;
    // Keep a session-directory cleanup token even when its recorded byte total is zero. A valid
    // zero-length part still creates a filesystem artifact, and a failed directory deletion must
    // remain retryable rather than disappearing merely because quota has no bytes to charge.
    conn.execute(
        "INSERT INTO multipart_staging_cleanups
         (id, upload_id, bucket_name, principal_id, bytes, storage_path, created_at)
         VALUES (?1,?2,?3,?4,?5,NULL,?6)",
        params![
            format!("session:{}", upload_id.as_str()),
            upload_id.as_str(),
            context.bucket,
            context.principal,
            current_bytes,
            context.updated_at,
        ],
    )
    .map_err(engine_err)?;
    conn.execute(
        "DELETE FROM multipart_uploads WHERE id=?1",
        params![upload_id.as_str()],
    )
    .map_err(engine_err)?;
    adjust_multipart_stats(conn, &context.bucket, &context.principal, -1, 0)
}

fn recover_multipart_staging_accounting(conn: &Connection, limit: u32) -> R<MutationOutcome> {
    let limit = i64::from(limit.clamp(1, 1_000));
    let reservations: Vec<(String, String)> = {
        let mut stmt = conn
            .prepare_cached(
                "SELECT attempt_id, upload_id FROM multipart_part_reservations
                 ORDER BY created_at, attempt_id LIMIT ?1",
            )
            .map_err(engine_err)?;
        stmt.query_map(params![limit], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(engine_err)?
            .collect::<rusqlite::Result<_>>()
            .map_err(engine_err)?
    };
    let mut released = 0u64;
    for (attempt_id, upload_id) in reservations {
        release_multipart_reservation(
            conn,
            &cairn_types::UploadId::from_string(upload_id),
            &attempt_id,
        )?;
        released += 1;
    }
    let remaining = limit.saturating_sub(released as i64);
    if remaining > 0 {
        let cleanup_ids: Vec<String> = {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id FROM multipart_staging_cleanups
                     ORDER BY created_at, id LIMIT ?1",
                )
                .map_err(engine_err)?;
            stmt.query_map(params![remaining], |row| row.get(0))
                .map_err(engine_err)?
                .collect::<rusqlite::Result<_>>()
                .map_err(engine_err)?
        };
        for cleanup_id in cleanup_ids {
            release_multipart_cleanup(conn, &cleanup_id)?;
            released += 1;
        }
    }
    Ok(MutationOutcome::MultipartAccountingReleased(released))
}

fn claim_multipart(
    conn: &Connection,
    upload_id: &cairn_types::UploadId,
    claim_token: &cairn_types::MultipartClaimToken,
) -> R<MutationOutcome> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM multipart_uploads WHERE id=?1",
            params![upload_id.as_str()],
            |r| r.get(0),
        )
        .optional()
        .map_err(engine_err)?;
    let outcome = match status.as_deref() {
        Some("active") => {
            conn.execute(
                "UPDATE multipart_uploads
                 SET status='completing', completion_claim_token=?2, updated_at=updated_at
                 WHERE id=?1",
                params![upload_id.as_str(), claim_token.as_str()],
            )
            .map_err(engine_err)?;
            let session = conn
                .query_row(
                    "SELECT * FROM multipart_uploads WHERE id=?1",
                    params![upload_id.as_str()],
                    model::multipart_from_row,
                )
                .map_err(engine_err)?;
            cairn_types::meta::ClaimOutcome::Claimed(Box::new(session))
        }
        Some(_) => cairn_types::meta::ClaimOutcome::AlreadyClaimed,
        None => cairn_types::meta::ClaimOutcome::NotFound,
    };
    Ok(MutationOutcome::MultipartClaim(outcome))
}

/// Evaluate a conditional-write precondition against the current latest non-delete-marker
/// version, inside the transaction.
fn check_precondition(
    conn: &Connection,
    bucket: &BucketName,
    key: &ObjectKey,
    pc: &Precondition,
) -> R<()> {
    if pc.is_unconditional() {
        return Ok(());
    }
    let current: Option<String> = conn
        .prepare_cached(
            "SELECT etag FROM object_versions
             WHERE bucket_name=?1 AND key=?2 AND is_latest=1 AND is_delete_marker=0",
        )
        .map_err(engine_err)?
        .query_row(params![bucket.as_str(), key.as_str()], |r| r.get(0))
        .optional()
        .map_err(engine_err)?;
    if let Some(want) = &pc.if_match {
        match &current {
            Some(e) if e == want.as_str() => {}
            _ => return Err(MetaError::PreconditionFailed),
        }
    }
    if let Some(inm) = &pc.if_none_match {
        match inm {
            IfNoneMatch::Any => {
                if current.is_some() {
                    return Err(MetaError::PreconditionFailed);
                }
            }
            IfNoneMatch::ETag(e) => {
                if current.as_deref() == Some(e.as_str()) {
                    return Err(MetaError::PreconditionFailed);
                }
            }
        }
    }
    Ok(())
}

fn enqueue(conn: &Connection, e: &OutboxEntry) -> R<()> {
    conn.prepare_cached(
        "INSERT INTO replication_outbox
         (id, bucket_name, key, version_id, operation, rule_id, target_arn, attempts, next_attempt_at, status, last_error, priority, lease_until, enqueued_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
    )
    .map_err(engine_err)?
    .execute(params![
            e.id,
            e.bucket.as_str(),
            e.key.as_str(),
            e.version_id.as_str(),
            repl_op_str(e.operation),
            e.rule_id,
            e.target_arn,
            e.attempts as i64,
            e.next_attempt_at.0,
            repl_status_str(e.status),
            e.last_error,
            e.priority,
            e.lease_until.map(|t| t.0),
            e.enqueued_at.0,
        ])
    .map_err(engine_err)?;
    Ok(())
}

/// Atomically claim up to `limit` due outbox entries: an entry is due when it is `pending`, or
/// `claimed` with an expired lease, and its `next_attempt_at` has passed. Claimed entries are
/// marked `status='claimed', lease_until = now + lease_secs` and returned. This runs inside the
/// writer's transaction, so the select-and-mark is atomic against other claimers.
fn claim_replication_batch(
    conn: &Connection,
    limit: u32,
    now: Timestamp,
    lease_secs: i64,
) -> R<MutationOutcome> {
    let lease_until = now.0 + lease_secs * 1000;
    let ids: Vec<String> = {
        let mut stmt = conn
            .prepare_cached(
                "SELECT id FROM replication_outbox
                 WHERE (status='pending' OR (status='claimed' AND lease_until < ?1))
                   AND next_attempt_at <= ?1
                 ORDER BY priority DESC, next_attempt_at LIMIT ?2",
            )
            .map_err(engine_err)?;
        stmt.query_map(params![now.0, i64::from(limit)], |r| r.get::<_, String>(0))
            .map_err(engine_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(engine_err)?
    };
    let mut claimed = Vec::with_capacity(ids.len());
    for id in &ids {
        conn.execute(
            "UPDATE replication_outbox SET status='claimed', lease_until=?2 WHERE id=?1",
            params![id, lease_until],
        )
        .map_err(engine_err)?;
        let entry = conn
            .query_row(
                "SELECT * FROM replication_outbox WHERE id=?1",
                params![id],
                model::outbox_from_row,
            )
            .map_err(engine_err)?;
        claimed.push(entry);
    }
    Ok(MutationOutcome::ReplicationBatch(claimed))
}

/// Idempotently insert one webhook-outbox entry (INSERT OR IGNORE on the deterministic id).
fn enqueue_webhook(conn: &Connection, e: &cairn_types::WebhookEntry) -> R<()> {
    conn.prepare_cached(
        "INSERT OR IGNORE INTO events_outbox
         (id, bucket_name, key, version_id, event_type, endpoint_id, payload, attempts,
          next_attempt_at, status, last_error, priority, lease_until)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
    )
    .map_err(engine_err)?
    .execute(params![
        e.id,
        e.bucket.as_str(),
        e.key.as_str(),
        e.version_id.as_str(),
        model::event_kind_str(e.event),
        e.endpoint_id,
        e.payload,
        e.attempts as i64,
        e.next_attempt_at.0,
        model::webhook_status_str(e.status),
        e.last_error,
        e.priority,
        e.lease_until.map(|t| t.0),
    ])
    .map_err(engine_err)?;
    Ok(())
}

/// Atomically claim due webhook-outbox entries — the select-and-mark mirrors `claim_replication_batch`.
fn claim_webhook_batch(
    conn: &Connection,
    limit: u32,
    now: Timestamp,
    lease_secs: i64,
) -> R<MutationOutcome> {
    let lease_until = now.0 + lease_secs * 1000;
    let ids: Vec<String> = {
        let mut stmt = conn
            .prepare_cached(
                "SELECT id FROM events_outbox
                 WHERE (status='pending' OR (status='claimed' AND lease_until < ?1))
                   AND next_attempt_at <= ?1
                 ORDER BY priority DESC, next_attempt_at LIMIT ?2",
            )
            .map_err(engine_err)?;
        stmt.query_map(params![now.0, i64::from(limit)], |r| r.get::<_, String>(0))
            .map_err(engine_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(engine_err)?
    };
    let mut claimed = Vec::with_capacity(ids.len());
    for id in &ids {
        conn.execute(
            "UPDATE events_outbox SET status='claimed', lease_until=?2 WHERE id=?1",
            params![id, lease_until],
        )
        .map_err(engine_err)?;
        let entry = conn
            .query_row(
                "SELECT * FROM events_outbox WHERE id=?1",
                params![id],
                model::webhook_from_row,
            )
            .map_err(engine_err)?;
        claimed.push(entry);
    }
    Ok(MutationOutcome::WebhookBatch(claimed))
}

fn config_aspect_str(a: cairn_types::bucket::ConfigAspect) -> &'static str {
    use cairn_types::bucket::ConfigAspect::*;
    match a {
        Policy => "policy",
        Acl => "acl",
        Cors => "cors",
        Lifecycle => "lifecycle",
        Replication => "replication",
        ReplicationTargets => "replication_targets",
        Tagging => "tagging",
        PublicAccessBlock => "public_access_block",
        Encryption => "encryption",
        ObjectLock => "object_lock",
        Notification => "notification",
    }
}

/// The string form of a config aspect (shared with the read path).
pub fn aspect_str(a: cairn_types::bucket::ConfigAspect) -> &'static str {
    config_aspect_str(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::id::UserId;
    use cairn_types::object::{CompressionDescriptor, StorageClass};
    use cairn_types::time::Timestamp;

    fn conn_with_schema() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::schema::run_migrations(&conn).unwrap();
        conn
    }

    fn seed_bucket(conn: &Connection, name: &str, quota: Option<i64>) {
        conn.execute(
            "INSERT INTO buckets (name, owner_id, created_at, versioning_state, ownership_mode, region, quota_bytes)
             VALUES (?1, 'owner', 0, 'enabled', 'BucketOwnerEnforced', 'us-east-1', ?2)",
            params![name, quota],
        )
        .unwrap();
    }

    fn seed_user(conn: &Connection, id: &str, quota: Option<i64>) {
        conn.execute(
            "INSERT INTO users
             (id, display_name, access_key_id, secret_hash, role, is_active, created_at, updated_at, quota_bytes)
             VALUES (?1, ?1, ?1, 'h', 'member', 1, 0, 0, ?2)",
            params![id, quota],
        )
        .unwrap();
    }

    fn obj_row_owned(
        bucket: &str,
        key: &str,
        version: &str,
        size: u64,
        owner: &str,
    ) -> ObjectVersionRow {
        ObjectVersionRow {
            owner_id: UserId(owner.to_owned()),
            ..obj_row(bucket, key, version, size)
        }
    }

    fn user_logical_bytes(conn: &Connection, owner: &str) -> i64 {
        conn.query_row(
            "SELECT COALESCE(SUM(size_logical),0) FROM object_versions WHERE owner_id=?1",
            params![owner],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn obj_row(bucket: &str, key: &str, version: &str, size: u64) -> ObjectVersionRow {
        ObjectVersionRow {
            id: uuid::Uuid::new_v4().simple().to_string(),
            bucket: BucketName::parse(bucket).unwrap(),
            key: ObjectKey::parse(key).unwrap(),
            version_id: VersionId::from_string(version.to_owned()),
            is_latest: true,
            is_delete_marker: false,
            size_logical: size,
            size_physical: size,
            etag: ETag::from_string("e".to_owned()),
            content_type: "text/plain".to_owned(),
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            storage_path: Some(StoragePath::from_string(format!("{bucket}/{version}"))),
            compression: CompressionDescriptor::Uncompressed,
            storage_class: StorageClass::Standard,
            cold_locator: None,
            owner_id: UserId("owner".to_owned()),
            user_metadata: Vec::new(),
            acl: None,
            checksums: Vec::new(),
            sse_descriptor: None,
            replication_status: None,
            replicated_at: None,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        }
    }

    fn put(row: ObjectVersionRow) -> Mutation {
        Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: Vec::new(),
        }
    }

    fn bucket_logical_bytes(conn: &Connection, bucket: &str) -> i64 {
        conn.query_row(
            "SELECT COALESCE(SUM(size_logical),0) FROM object_versions WHERE bucket_name=?1",
            params![bucket],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// Apply a mutation the way the writer does: inside a savepoint, rolling that savepoint back
    /// on error so a rejected op commits nothing while the surrounding transaction survives.
    fn apply_in_savepoint(conn: &Connection, m: Mutation) -> R<MutationOutcome> {
        conn.execute_batch("SAVEPOINT sp").unwrap();
        match apply(conn, m) {
            Ok(o) => {
                conn.execute_batch("RELEASE sp").unwrap();
                Ok(o)
            }
            Err(e) => {
                conn.execute_batch("ROLLBACK TO sp; RELEASE sp").unwrap();
                Err(e)
            }
        }
    }

    /// The maintained roll-up counters (Phase 2.1) must agree exactly with a fresh full scan of
    /// `object_versions` — the global sums catch any over- or under-counting, the per-bucket rows
    /// catch a misattributed delta.
    fn assert_counters_match_scan(conn: &Connection) {
        // Global: the counter sums equal the table's totals.
        let (sv, sl, sp): (i64, i64, i64) = conn
            .query_row(
                "SELECT COALESCE(SUM(versions),0), COALESCE(SUM(logical_bytes),0),
                        COALESCE(SUM(physical_bytes),0) FROM bucket_stats",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        let (tv, tl, tp): (i64, i64, i64) = conn
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(size_logical),0), COALESCE(SUM(size_physical),0)
                 FROM object_versions",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            (sv, sl, sp),
            (tv, tl, tp),
            "bucket_stats global sums must equal the object_versions scan"
        );
        let su: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(logical_bytes),0) FROM user_stats",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            su, tl,
            "user_stats logical sum must equal the object_versions scan"
        );

        // Per-bucket: every bucket that has rows must match its counter row exactly.
        let mut stmt = conn
            .prepare(
                "SELECT bucket_name, COUNT(*), COALESCE(SUM(size_logical),0),
                        COALESCE(SUM(size_physical),0) FROM object_versions GROUP BY bucket_name",
            )
            .unwrap();
        let scanned: Vec<(String, i64, i64, i64)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        for (b, v, l, p) in scanned {
            let (cv, cl, cp): (i64, i64, i64) = conn
                .query_row(
                    "SELECT versions, logical_bytes, physical_bytes FROM bucket_stats WHERE bucket_name=?1",
                    params![b],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap();
            assert_eq!(
                (cv, cl, cp),
                (v, l, p),
                "bucket_stats mismatch for bucket {b}"
            );
        }
    }

    #[test]
    fn stat_counters_stay_consistent_through_lifecycle() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", None);
        seed_bucket(&conn, "cct", None);
        assert_counters_match_scan(&conn);

        // Inserts across two buckets and two owners.
        apply(&conn, put(obj_row_owned("bkt", "k1", "v1", 10, "alice"))).unwrap();
        apply(&conn, put(obj_row_owned("bkt", "k2", "v1", 20, "bob"))).unwrap();
        apply(&conn, put(obj_row_owned("cct", "k1", "v1", 5, "alice"))).unwrap();
        assert_counters_match_scan(&conn);

        // A new version of k1 (history grows; both versions are counted).
        apply(&conn, put(obj_row_owned("bkt", "k1", "v2", 15, "alice"))).unwrap();
        assert_counters_match_scan(&conn);

        // Replace the same (key, version) — the upsert delete+insert path must net the size change.
        apply(&conn, put(obj_row_owned("bkt", "k2", "v1", 25, "bob"))).unwrap();
        assert_counters_match_scan(&conn);

        // A delete marker (a zero-byte version row).
        apply(
            &conn,
            Mutation::CreateDeleteMarker {
                bucket: BucketName::parse("bkt").unwrap(),
                key: ObjectKey::parse("k1").unwrap(),
                version_id: VersionId::from_string("v3".to_owned()),
                owner_id: UserId("alice".to_owned()),
                now: Timestamp(2),
                bypass: GovernanceBypass::Denied,
                expected_current: None,
                replication: Vec::new(),
            },
        )
        .unwrap();
        assert_counters_match_scan(&conn);

        // Delete a specific historical version.
        apply(
            &conn,
            Mutation::DeleteVersion {
                bucket: BucketName::parse("bkt").unwrap(),
                key: ObjectKey::parse("k1").unwrap(),
                version_id: VersionId::from_string("v1".to_owned()),
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(2),
                bypass: GovernanceBypass::Denied,
            },
        )
        .unwrap();
        assert_counters_match_scan(&conn);

        // Delete the current version (triggers a promotion of the predecessor).
        apply(
            &conn,
            Mutation::DeleteVersion {
                bucket: BucketName::parse("bkt").unwrap(),
                key: ObjectKey::parse("k1").unwrap(),
                version_id: VersionId::from_string("v3".to_owned()),
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(2),
                bypass: GovernanceBypass::Denied,
            },
        )
        .unwrap();
        assert_counters_match_scan(&conn);
    }

    #[test]
    fn deleting_an_owner_keeps_user_stats_consistent() {
        // Regression: DeleteUser must NOT drop an owner's user_stats while that owner still has
        // object rows. If it did, a later deletion of one of those objects would decrement a missing
        // row and re-create it with a NEGATIVE balance, breaking the enforced
        // `user_stats sum == object scan` invariant (and corrupting quota accounting). The presence
        // of an actual users row is irrelevant here — the bug is purely the stats/object interaction.
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", None);
        apply(&conn, put(obj_row_owned("bkt", "k1", "v1", 10, "alice"))).unwrap();
        apply(&conn, put(obj_row_owned("bkt", "k2", "v1", 20, "bob"))).unwrap();
        assert_counters_match_scan(&conn);

        // Delete alice while she still owns "k1": her objects — and so her stats — must survive.
        apply(&conn, Mutation::DeleteUser(UserId("alice".to_owned()))).unwrap();
        assert_counters_match_scan(&conn);

        // Removing alice's object now decrements her surviving stats row toward zero, rather than
        // resurrecting a negative one.
        apply(
            &conn,
            Mutation::DeleteVersion {
                bucket: BucketName::parse("bkt").unwrap(),
                key: ObjectKey::parse("k1").unwrap(),
                version_id: VersionId::from_string("v1".to_owned()),
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(2),
                bypass: GovernanceBypass::Denied,
            },
        )
        .unwrap();
        assert_counters_match_scan(&conn);
    }

    #[test]
    fn rejected_quota_write_leaves_counters_unchanged() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", Some(100));
        apply_in_savepoint(&conn, put(obj_row("bkt", "k1", "v1", 60))).unwrap();
        assert_counters_match_scan(&conn);
        // This put would exceed the quota: it is rolled back in its savepoint, and the counter
        // upserts — which run inside that savepoint — must be rolled back with it.
        let err = apply_in_savepoint(&conn, put(obj_row("bkt", "k2", "v1", 50))).unwrap_err();
        assert!(matches!(err, MetaError::QuotaExceeded));
        assert_counters_match_scan(&conn);
        let versions: i64 = conn
            .query_row(
                "SELECT versions FROM bucket_stats WHERE bucket_name='bkt'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(versions, 1, "only the first, accepted write is counted");
    }

    #[test]
    fn put_under_quota_succeeds() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", Some(100));
        apply(&conn, put(obj_row("bkt", "k", "v1", 60))).unwrap();
        assert_eq!(bucket_logical_bytes(&conn, "bkt"), 60);
    }

    #[test]
    fn put_exceeding_quota_rejected_and_commits_nothing() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", Some(100));
        // First put fits: 60 <= 100.
        apply_in_savepoint(&conn, put(obj_row("bkt", "k1", "v1", 60))).unwrap();
        // Second put would push the bucket to 60 + 50 = 110 > 100: rejected, rolled back.
        let err = apply_in_savepoint(&conn, put(obj_row("bkt", "k2", "v1", 50))).unwrap_err();
        assert!(matches!(err, MetaError::QuotaExceeded));
        // The rejected op left nothing behind: the bucket still holds exactly the first object.
        assert_eq!(bucket_logical_bytes(&conn, "bkt"), 60);
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM object_versions WHERE bucket_name='bkt'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn raising_quota_lets_the_put_through() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", Some(100));
        apply(&conn, put(obj_row("bkt", "k1", "v1", 60))).unwrap();
        let err = apply_in_savepoint(&conn, put(obj_row("bkt", "k2", "v1", 50))).unwrap_err();
        assert!(matches!(err, MetaError::QuotaExceeded));
        // Operator raises the quota; the previously-rejected size now fits.
        conn.execute("UPDATE buckets SET quota_bytes=200 WHERE name='bkt'", [])
            .unwrap();
        apply(&conn, put(obj_row("bkt", "k2", "v1", 50))).unwrap();
        assert_eq!(bucket_logical_bytes(&conn, "bkt"), 110);
    }

    #[test]
    fn null_quota_is_unlimited() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", None);
        apply(&conn, put(obj_row("bkt", "k", "v1", 1_000_000))).unwrap();
        assert_eq!(bucket_logical_bytes(&conn, "bkt"), 1_000_000);
    }

    #[test]
    fn overwriting_same_version_counts_only_the_new_size() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", Some(100));
        apply(&conn, put(obj_row("bkt", "k", "v1", 90))).unwrap();
        // Overwriting the same (key, version) with a 95-byte body replaces the old 90 bytes,
        // so the bucket total is 95 (not 185) and the quota of 100 is not exceeded.
        apply(&conn, put(obj_row("bkt", "k", "v1", 95))).unwrap();
        assert_eq!(bucket_logical_bytes(&conn, "bkt"), 95);
    }

    #[test]
    fn delete_marker_ignores_quota() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", Some(10));
        // Fill to the quota, then a delete marker (no logical bytes) must still be allowed.
        apply(&conn, put(obj_row("bkt", "k", "v1", 10))).unwrap();
        apply(
            &conn,
            Mutation::CreateDeleteMarker {
                bucket: BucketName::parse("bkt").unwrap(),
                key: ObjectKey::parse("k").unwrap(),
                version_id: VersionId::from_string("v2".to_owned()),
                owner_id: UserId("owner".to_owned()),
                now: Timestamp(2),
                bypass: GovernanceBypass::Denied,
                expected_current: None,
                replication: Vec::new(),
            },
        )
        .unwrap();
    }

    #[test]
    fn put_under_user_quota_succeeds() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", None);
        seed_user(&conn, "alice", Some(100));
        apply(&conn, put(obj_row_owned("bkt", "k", "v1", 60, "alice"))).unwrap();
        assert_eq!(user_logical_bytes(&conn, "alice"), 60);
    }

    #[test]
    fn put_exceeding_user_quota_rejected_and_commits_nothing() {
        let conn = conn_with_schema();
        // Two buckets with no bucket quota: the user quota must aggregate across both.
        seed_bucket(&conn, "bkt1", None);
        seed_bucket(&conn, "bkt2", None);
        seed_user(&conn, "alice", Some(100));
        apply_in_savepoint(&conn, put(obj_row_owned("bkt1", "k1", "v1", 60, "alice"))).unwrap();
        // 60 (in bkt1) + 50 (in bkt2) = 110 > 100: rejected and rolled back.
        let err = apply_in_savepoint(&conn, put(obj_row_owned("bkt2", "k2", "v1", 50, "alice")))
            .unwrap_err();
        assert!(matches!(err, MetaError::QuotaExceeded));
        assert_eq!(user_logical_bytes(&conn, "alice"), 60);
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM object_versions WHERE owner_id='alice'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn unset_user_quota_is_unlimited() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", None);
        // User row exists but quota_bytes is NULL -> no enforcement.
        seed_user(&conn, "alice", None);
        apply(
            &conn,
            put(obj_row_owned("bkt", "k", "v1", 1_000_000, "alice")),
        )
        .unwrap();
        assert_eq!(user_logical_bytes(&conn, "alice"), 1_000_000);
    }

    #[test]
    fn missing_user_row_is_unlimited() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", None);
        // No users row for the owner at all -> no enforcement.
        apply(
            &conn,
            put(obj_row_owned("bkt", "k", "v1", 1_000_000, "nobody")),
        )
        .unwrap();
        assert_eq!(user_logical_bytes(&conn, "nobody"), 1_000_000);
    }

    #[test]
    fn overwriting_same_version_counts_only_new_size_for_user_quota() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "bkt", None);
        seed_user(&conn, "alice", Some(100));
        apply(&conn, put(obj_row_owned("bkt", "k", "v1", 90, "alice"))).unwrap();
        // Replacing the same (bucket,key,version) with 95 bytes supersedes the old 90, so the
        // user's total is 95 (not 185) and the 100-byte quota is not exceeded.
        apply(&conn, put(obj_row_owned("bkt", "k", "v1", 95, "alice"))).unwrap();
        assert_eq!(user_logical_bytes(&conn, "alice"), 95);
    }

    fn object_lock_bucket(name: &str, versioning: VersioningState) -> cairn_types::Bucket {
        cairn_types::Bucket {
            name: BucketName::parse(name).unwrap(),
            owner_id: UserId("owner".to_owned()),
            created_at: Timestamp(1),
            versioning,
            ownership_mode: cairn_types::OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".to_owned(),
            compression: None,
        }
    }

    #[test]
    fn object_lock_bucket_creation_and_repair_are_writer_atomic() {
        let conn = conn_with_schema();
        let wrong = object_lock_bucket("wrong-state", VersioningState::Suspended);
        assert!(matches!(
            apply_in_savepoint(&conn, Mutation::CreateObjectLockBucket(Box::new(wrong))),
            Err(MetaError::InvalidBucketState)
        ));
        let wrong_rows: i64 = conn
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM buckets WHERE name='wrong-state')
                   + (SELECT COUNT(*) FROM bucket_config WHERE bucket_name='wrong-state')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            wrong_rows, 0,
            "wrong-state create must leave no half bucket"
        );

        seed_bucket(&conn, "repair", None);
        conn.execute(
            "INSERT INTO bucket_config (bucket_name, aspect, doc)
             VALUES ('repair','object_lock','{\"enabled\":false,\"default_retention\":null}')",
            [],
        )
        .unwrap();
        assert!(matches!(
            apply_in_savepoint(&conn, put(obj_row("repair", "k", "v1", 1))),
            Err(MetaError::InvalidObjectLockState)
        ));
        apply_in_savepoint(
            &conn,
            Mutation::UpdateObjectLockConfiguration {
                bucket: BucketName::parse("repair").unwrap(),
                default_retention: Some(DefaultRetention {
                    mode: ObjectLockMode::Compliance,
                    period: RetentionPeriod::Days(1),
                }),
            },
        )
        .unwrap();
        apply_in_savepoint(&conn, put(obj_row("repair", "k", "v1", 1))).unwrap();

        conn.execute(
            "UPDATE bucket_config
             SET doc='{\"enabled\":true,\"default_retention\":null,\"unknown\":1}'
             WHERE bucket_name='repair' AND aspect='object_lock'",
            [],
        )
        .unwrap();
        assert!(matches!(
            apply_in_savepoint(
                &conn,
                Mutation::DeleteVersion {
                    bucket: BucketName::parse("repair").unwrap(),
                    key: ObjectKey::parse("k").unwrap(),
                    version_id: VersionId::from_string("v1".to_owned()),
                    expected_row_id: None,
                    expected_updated_at: None,
                    require_sole_key_version: false,
                    now: Timestamp(i64::MAX),
                    bypass: GovernanceBypass::Authorized,
                }
            ),
            Err(MetaError::InvalidObjectLockState)
        ));
        apply_in_savepoint(
            &conn,
            Mutation::UpdateObjectLockConfiguration {
                bucket: BucketName::parse("repair").unwrap(),
                default_retention: None,
            },
        )
        .unwrap();
        assert!(matches!(
            apply_in_savepoint(
                &conn,
                Mutation::DeleteVersion {
                    bucket: BucketName::parse("repair").unwrap(),
                    key: ObjectKey::parse("k").unwrap(),
                    version_id: VersionId::from_string("v1".to_owned()),
                    expected_row_id: None,
                    expected_updated_at: None,
                    require_sole_key_version: false,
                    now: Timestamp(i64::MAX),
                    bypass: GovernanceBypass::Denied,
                }
            ),
            Ok(MutationOutcome::Deleted { .. })
        ));
    }

    #[test]
    fn object_lock_missing_targets_precede_config_and_corrupt_rows_fail_closed() {
        let conn = conn_with_schema();
        seed_bucket(&conn, "strict", None);
        conn.execute(
            "INSERT INTO bucket_config (bucket_name, aspect, doc)
             VALUES ('strict','object_lock','not-json')",
            [],
        )
        .unwrap();
        let bucket = BucketName::parse("strict").unwrap();
        let missing_key = ObjectKey::parse("missing").unwrap();
        let missing_version = VersionId::from_string("v".to_owned());
        assert!(matches!(
            apply_in_savepoint(
                &conn,
                Mutation::SetObjectRetention {
                    bucket: bucket.clone(),
                    key: missing_key.clone(),
                    version_id: missing_version.clone(),
                    retention: None,
                    now: Timestamp(1),
                    bypass: GovernanceBypass::Denied,
                }
            ),
            Err(MetaError::ObjectVersionNotFound)
        ));
        assert!(matches!(
            apply_in_savepoint(
                &conn,
                Mutation::SetObjectLegalHold {
                    bucket: bucket.clone(),
                    key: missing_key,
                    version_id: missing_version,
                    on: true,
                }
            ),
            Err(MetaError::ObjectVersionNotFound)
        ));

        conn.execute(
            "UPDATE bucket_config
             SET doc='{\"enabled\":true,\"default_retention\":null}'
             WHERE bucket_name='strict'",
            [],
        )
        .unwrap();
        apply_in_savepoint(&conn, put(obj_row("strict", "live", "v1", 1))).unwrap();
        apply_in_savepoint(
            &conn,
            Mutation::SetObjectLegalHold {
                bucket: bucket.clone(),
                key: ObjectKey::parse("live").unwrap(),
                version_id: VersionId::from_string("v1".to_owned()),
                on: true,
            },
        )
        .unwrap();
        conn.execute_batch(
            "PRAGMA ignore_check_constraints=ON;
             UPDATE object_locks SET lock_mode='GOVERNANCE', retain_until=NULL
             WHERE bucket_name='strict' AND key='live' AND version_id='v1';
             PRAGMA ignore_check_constraints=OFF;",
        )
        .unwrap();
        assert!(matches!(
            apply_in_savepoint(
                &conn,
                Mutation::DeleteVersion {
                    bucket,
                    key: ObjectKey::parse("live").unwrap(),
                    version_id: VersionId::from_string("v1".to_owned()),
                    expected_row_id: None,
                    expected_updated_at: None,
                    require_sole_key_version: false,
                    now: Timestamp(i64::MAX),
                    bypass: GovernanceBypass::Authorized,
                }
            ),
            Err(MetaError::InvalidObjectLockState)
        ));
    }

    #[test]
    fn legacy_multipart_intent_fails_closed_on_an_object_lock_bucket() {
        let conn = conn_with_schema();
        let bucket = object_lock_bucket("legacy-mpu-lock", VersioningState::Enabled);
        apply_in_savepoint(
            &conn,
            Mutation::CreateObjectLockBucket(Box::new(bucket.clone())),
        )
        .unwrap();
        let upload_id = cairn_types::UploadId::from_string("legacy-upload".to_owned());
        let key = ObjectKey::parse("assembled").unwrap();
        apply_in_savepoint(
            &conn,
            Mutation::CreateMultipart {
                session: Box::new(cairn_types::MultipartSession {
                    upload_id: upload_id.clone(),
                    bucket: bucket.name.clone(),
                    key: key.clone(),
                    content_type: "application/octet-stream".to_owned(),
                    status: cairn_types::MultipartStatus::Active,
                    owner_id: UserId("owner".to_owned()),
                    initiated_by: UserId("owner".to_owned()),
                    intended_acl: None,
                    user_metadata: Vec::new(),
                    initial_tags: Vec::new(),
                    lock_intent: ExplicitObjectLockIntent::default(),
                    sse_requested: false,
                    encrypt_parts: false,
                    sse_kms_requested: false,
                    sse_kms_key_id: None,
                    sse_bucket_key_enabled: false,
                    created_at: Timestamp(10),
                    updated_at: Timestamp(10),
                }),
                limits: cairn_types::meta::MultipartLimits::default(),
            },
        )
        .unwrap();
        let intent_known: i64 = conn
            .query_row(
                "SELECT object_lock_intent_known FROM multipart_uploads WHERE id=?1",
                params![upload_id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(intent_known, 1, "new sessions must pin a known intent");
        // New sessions write 1. Reset it to the migration default to model an in-flight v26 row
        // whose explicit retention/legal-hold headers were never persisted.
        conn.execute(
            "UPDATE multipart_uploads SET object_lock_intent_known=0 WHERE id=?1",
            params![upload_id.as_str()],
        )
        .unwrap();
        let claim_token = cairn_types::MultipartClaimToken::generate();
        apply_in_savepoint(
            &conn,
            Mutation::ClaimMultipart {
                upload_id: upload_id.clone(),
                claim_token: claim_token.clone(),
            },
        )
        .unwrap();

        let error = apply_in_savepoint(
            &conn,
            Mutation::CompleteMultipart {
                upload_id: upload_id.clone(),
                claim_token,
                row: Box::new(obj_row(
                    bucket.name.as_str(),
                    key.as_str(),
                    "assembled-v1",
                    1,
                )),
                precondition: Precondition::default(),
                replication: Vec::new(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaError::InvalidObjectLockState));
        let status: String = conn
            .query_row(
                "SELECT status FROM multipart_uploads WHERE id=?1",
                params![upload_id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            status, "completing",
            "the rejected legacy session must remain available for release/abort"
        );
        let versions: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM object_versions WHERE bucket_name=?1 AND key=?2",
                params![bucket.name.as_str(), key.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(versions, 0);
    }
}
