//! Applying one [`Mutation`] to the write driver, ported from `cairn-meta/src/apply.rs`. Each
//! call runs inside its own savepoint (managed by the async writer), so returning `Err` rolls
//! back only this mutation while its batch-mates commit. Preconditions are evaluated here, inside
//! the transaction, so the check and the upsert are atomic with respect to every other writer
//! (ARCH 11.6). The SQL, precondition logic, savepoint semantics, and outcomes are identical to
//! the rusqlite store.

use crate::driver::{AsyncSqlDriver, Value, query_one};
use crate::model::{self, opt_text, repl_op_str, repl_status_str, storage_class_str, to_json};
use cairn_types::MetaError;
use cairn_types::id::{BucketName, ObjectKey, StoragePath, VersionId};
use cairn_types::meta::{IfNoneMatch, Mutation, MutationOutcome, OutboxEntry, Precondition};
use cairn_types::object::{ETag, ObjectVersionRow};
use cairn_types::time::Timestamp;

type R<T> = Result<T, MetaError>;

/// Apply a mutation, returning its typed outcome or a typed error.
pub async fn apply(driver: &dyn AsyncSqlDriver, m: Mutation) -> R<MutationOutcome> {
    match m {
        Mutation::PutObjectVersion {
            row,
            precondition,
            replication,
        } => put_version(driver, *row, &precondition, replication).await,
        Mutation::CreateDeleteMarker {
            bucket,
            key,
            version_id,
            owner_id,
            now,
            replication,
        } => {
            let row = ObjectVersionRow {
                id: uuid::Uuid::new_v4().simple().to_string(),
                bucket,
                key,
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
                created_at: now,
                updated_at: now,
            };
            demote_latest(driver, &row.bucket, &row.key).await?;
            insert_version(driver, &row).await?;
            for e in &replication {
                enqueue(driver, e).await?;
            }
            Ok(MutationOutcome::DeleteMarker { version_id })
        }
        Mutation::DeleteVersion {
            bucket,
            key,
            version_id,
        } => delete_version(driver, &bucket, &key, &version_id).await,
        Mutation::CreateMultipart(s) => {
            driver
                .execute(
                    "INSERT INTO multipart_uploads
                     (id, bucket_name, key, content_type, status, owner_id, intended_acl, user_metadata, sse_requested, created_at, updated_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                    vec![
                        Value::Text(s.upload_id.as_str().to_owned()),
                        Value::Text(s.bucket.as_str().to_owned()),
                        Value::Text(s.key.as_str().to_owned()),
                        Value::Text(s.content_type.clone()),
                        Value::Text(model::mp_status_str(s.status).to_owned()),
                        Value::Text(s.owner_id.0.clone()),
                        opt_text(s.intended_acl.as_ref().map(to_json)),
                        Value::Text(to_json(&s.user_metadata)),
                        Value::Int(s.sse_requested as i64),
                        Value::Int(s.created_at.0),
                        Value::Int(s.updated_at.0),
                    ],
                )
                .await?;
            Ok(MutationOutcome::MultipartCreated(s.upload_id))
        }
        Mutation::RecordPart { upload_id, part } => {
            let superseded: Option<String> = query_one(
                driver,
                "SELECT storage_path FROM multipart_parts WHERE upload_id=?1 AND part_number=?2",
                vec![
                    Value::Text(upload_id.as_str().to_owned()),
                    Value::Int(i64::from(part.part_number)),
                ],
            )
            .await?
            .and_then(|r| r.get_opt_text(0));
            driver
                .execute(
                    "INSERT OR REPLACE INTO multipart_parts
                     (upload_id, part_number, size, etag, storage_path, checksum)
                     VALUES (?1,?2,?3,?4,?5,?6)",
                    vec![
                        Value::Text(upload_id.as_str().to_owned()),
                        Value::Int(i64::from(part.part_number)),
                        Value::Int(part.size as i64),
                        Value::Text(part.etag.clone()),
                        Value::Text(part.storage_path.as_str().to_owned()),
                        opt_text(part.checksum.as_ref().map(to_json)),
                    ],
                )
                .await?;
            Ok(MutationOutcome::PartRecorded {
                superseded: superseded.map(StoragePath::from_string),
            })
        }
        Mutation::ClaimMultipart(upload_id) => claim_multipart(driver, &upload_id).await,
        Mutation::CompleteMultipart {
            upload_id,
            row,
            precondition,
            replication,
        } => {
            let bucket = row.bucket.clone();
            let key = row.key.clone();
            check_precondition(driver, &bucket, &key, &precondition).await?;
            enforce_bucket_quota(driver, &row).await?;
            enforce_user_quota(driver, &row).await?;
            let version_id = row.version_id.clone();
            let superseded = upsert_version(driver, *row).await?;
            driver
                .execute(
                    "DELETE FROM multipart_uploads WHERE id=?1",
                    vec![Value::Text(upload_id.as_str().to_owned())],
                )
                .await?;
            for e in &replication {
                enqueue(driver, e).await?;
            }
            Ok(MutationOutcome::MultipartCompleted {
                superseded,
                version_id,
            })
        }
        Mutation::AbortMultipart(upload_id) => {
            driver
                .execute(
                    "DELETE FROM multipart_uploads WHERE id=?1",
                    vec![Value::Text(upload_id.as_str().to_owned())],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CreateBucket(b) => {
            // `compression_policy` is the spec column name (ARCH 34.1); `quota_bytes` defaults to
            // NULL (unlimited) since the frozen `Bucket` domain type carries no quota field.
            driver
                .execute(
                    "INSERT INTO buckets (name, owner_id, created_at, versioning_state, ownership_mode, region, compression_policy)
                     VALUES (?1,?2,?3,?4,?5,?6,?7)",
                    vec![
                        Value::Text(b.name.as_str().to_owned()),
                        Value::Text(b.owner_id.0.clone()),
                        Value::Int(b.created_at.0),
                        Value::Text(model::versioning_str(b.versioning).to_owned()),
                        Value::Text(model::ownership_str(b.ownership_mode).to_owned()),
                        Value::Text(b.region.clone()),
                        opt_text(b.compression.as_ref().map(to_json)),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeleteBucket(name) => {
            driver
                .execute(
                    "DELETE FROM bucket_config WHERE bucket_name=?1",
                    vec![Value::Text(name.as_str().to_owned())],
                )
                .await?;
            driver
                .execute(
                    "DELETE FROM buckets WHERE name=?1",
                    vec![Value::Text(name.as_str().to_owned())],
                )
                .await?;
            // A bucket is empty when deleted, so its roll-up row is already zero; drop it to keep
            // the counter table from accumulating tombstones for recreated bucket names.
            driver
                .execute(
                    "DELETE FROM bucket_stats WHERE bucket_name=?1",
                    vec![Value::Text(name.as_str().to_owned())],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetBucketConfig {
            bucket,
            aspect,
            doc,
        } => {
            let aspect_s = config_aspect_str(aspect);
            match doc {
                Some(d) => {
                    driver
                        .execute(
                            "INSERT OR REPLACE INTO bucket_config (bucket_name, aspect, doc) VALUES (?1,?2,?3)",
                            vec![
                                Value::Text(bucket.as_str().to_owned()),
                                Value::Text(aspect_s.to_owned()),
                                Value::Text(d.0),
                            ],
                        )
                        .await?;
                }
                None => {
                    driver
                        .execute(
                            "DELETE FROM bucket_config WHERE bucket_name=?1 AND aspect=?2",
                            vec![
                                Value::Text(bucket.as_str().to_owned()),
                                Value::Text(aspect_s.to_owned()),
                            ],
                        )
                        .await?;
                }
            }
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetObjectRetention {
            bucket,
            key,
            version_id,
            retention,
        } => {
            let mode = retention
                .as_ref()
                .map(|r| model::lock_mode_str(r.mode).to_owned());
            let until = match retention.as_ref().map(|r| r.retain_until.0) {
                Some(u) => Value::Int(u),
                None => Value::Null,
            };
            driver
                .execute(
                    "INSERT INTO object_locks (bucket_name, key, version_id, lock_mode, retain_until, legal_hold)
                     VALUES (?1,?2,?3,?4,?5,0)
                     ON CONFLICT(bucket_name,key,version_id)
                     DO UPDATE SET lock_mode=excluded.lock_mode, retain_until=excluded.retain_until",
                    vec![
                        Value::Text(bucket.as_str().to_owned()),
                        Value::Text(key.as_str().to_owned()),
                        Value::Text(version_id.as_str().to_owned()),
                        opt_text(mode),
                        until,
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetObjectLegalHold {
            bucket,
            key,
            version_id,
            on,
        } => {
            driver
                .execute(
                    "INSERT INTO object_locks (bucket_name, key, version_id, lock_mode, retain_until, legal_hold)
                     VALUES (?1,?2,?3,NULL,NULL,?4)
                     ON CONFLICT(bucket_name,key,version_id) DO UPDATE SET legal_hold=excluded.legal_hold",
                    vec![
                        Value::Text(bucket.as_str().to_owned()),
                        Value::Text(key.as_str().to_owned()),
                        Value::Text(version_id.as_str().to_owned()),
                        Value::Int(on as i64),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetVersioning { bucket, state } => {
            driver
                .execute(
                    "UPDATE buckets SET versioning_state=?2 WHERE name=?1",
                    vec![
                        Value::Text(bucket.as_str().to_owned()),
                        Value::Text(model::versioning_str(state).to_owned()),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetOwnership { bucket, mode } => {
            driver
                .execute(
                    "UPDATE buckets SET ownership_mode=?2 WHERE name=?1",
                    vec![
                        Value::Text(bucket.as_str().to_owned()),
                        Value::Text(model::ownership_str(mode).to_owned()),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetBucketQuota {
            bucket,
            quota_bytes,
        } => {
            driver
                .execute(
                    "UPDATE buckets SET quota_bytes=?2 WHERE name=?1",
                    vec![
                        Value::Text(bucket.as_str().to_owned()),
                        quota_bytes.map_or(Value::Null, |q| Value::Int(q as i64)),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetBucketCompression { bucket, policy } => {
            driver
                .execute(
                    "UPDATE buckets SET compression_policy=?2 WHERE name=?1",
                    vec![
                        Value::Text(bucket.as_str().to_owned()),
                        policy
                            .as_ref()
                            .map_or(Value::Null, |p| Value::Text(to_json(p))),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetUserPolicy { user_id, policy } => {
            driver
                .execute(
                    "UPDATE users SET policy=?2 WHERE id=?1",
                    vec![
                        Value::Text(user_id.0.as_str().to_owned()),
                        policy.map_or(Value::Null, Value::Text),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetUserQuota {
            user_id,
            quota_bytes,
        } => {
            driver
                .execute(
                    "UPDATE users SET quota_bytes=?2 WHERE id=?1",
                    vec![
                        Value::Text(user_id.0.as_str().to_owned()),
                        quota_bytes.map_or(Value::Null, |q| Value::Int(q as i64)),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RetryFailedReplication { bucket, now } => {
            match bucket {
                Some(b) => {
                    driver
                        .execute(
                            "UPDATE replication_outbox SET status='pending', next_attempt_at=?2, attempts=0, lease_until=NULL \
                             WHERE status='failed' AND bucket_name=?1",
                            vec![Value::Text(b.as_str().to_owned()), Value::Int(now.0)],
                        )
                        .await?;
                }
                None => {
                    driver
                        .execute(
                            "UPDATE replication_outbox SET status='pending', next_attempt_at=?1, attempts=0, lease_until=NULL \
                             WHERE status='failed'",
                            vec![Value::Int(now.0)],
                        )
                        .await?;
                }
            }
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetAccountPublicAccessBlock(bpa) => {
            driver
                .execute(
                    "INSERT OR REPLACE INTO account_config (k, v) VALUES ('public_access_block', ?1)",
                    vec![Value::Text(to_json(&bpa))],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::PutObjectTags {
            bucket,
            key,
            version_id,
            tags,
        } => {
            driver
                .execute(
                    "DELETE FROM object_tags WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                    vec![
                        Value::Text(bucket.as_str().to_owned()),
                        Value::Text(key.as_str().to_owned()),
                        Value::Text(version_id.as_str().to_owned()),
                    ],
                )
                .await?;
            for (k, v) in &tags {
                driver
                    .execute(
                        "INSERT INTO object_tags (bucket_name, key, version_id, tag_key, tag_value) VALUES (?1,?2,?3,?4,?5)",
                        vec![
                            Value::Text(bucket.as_str().to_owned()),
                            Value::Text(key.as_str().to_owned()),
                            Value::Text(version_id.as_str().to_owned()),
                            Value::Text(k.clone()),
                            Value::Text(v.clone()),
                        ],
                    )
                    .await?;
            }
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeleteObjectTags {
            bucket,
            key,
            version_id,
        } => {
            driver
                .execute(
                    "DELETE FROM object_tags WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                    vec![
                        Value::Text(bucket.as_str().to_owned()),
                        Value::Text(key.as_str().to_owned()),
                        Value::Text(version_id.as_str().to_owned()),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetObjectAcl {
            bucket,
            key,
            version_id,
            acl,
        } => {
            // Replace the version row's `acl` JSON column; `None` stores SQL NULL (clears it).
            driver
                .execute(
                    "UPDATE object_versions SET acl=?4 WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                    vec![
                        Value::Text(bucket.as_str().to_owned()),
                        Value::Text(key.as_str().to_owned()),
                        Value::Text(version_id.as_str().to_owned()),
                        opt_text(acl.as_ref().map(to_json)),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CreateUser(rec) => {
            driver
                .execute(
                    "INSERT INTO users
                     (id, display_name, access_key_id, secret_hash, sigv4_access_key_id,
                      sigv4_secret_ciphertext, sigv4_secret_nonce, role, is_active, created_at, updated_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                    model::user_record_values(&rec),
                )
                .await?;
            Ok(MutationOutcome::UserCreated(rec.user.id.clone()))
        }
        Mutation::UpdateUser(rec) => {
            // Column-scoped UPDATE (not INSERT OR REPLACE) so the unlisted `policy`/`quota_bytes`
            // columns are preserved across a role/credential change (audit #10). The positional
            // params from `user_record_values` are unchanged: ?1=id (the WHERE key), ?2..?11 the
            // identity columns in the same order.
            driver
                .execute(
                    "UPDATE users SET
                       display_name=?2, access_key_id=?3, secret_hash=?4, sigv4_access_key_id=?5,
                       sigv4_secret_ciphertext=?6, sigv4_secret_nonce=?7, role=?8, is_active=?9,
                       created_at=?10, updated_at=?11
                     WHERE id=?1",
                    model::user_record_values(&rec),
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeactivateUser(id) => {
            driver
                .execute(
                    "UPDATE users SET is_active=0 WHERE id=?1",
                    vec![Value::Text(id.0)],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CreateSessionCredential(rec) => {
            driver
                .execute(
                    "INSERT INTO session_credentials
                     (access_key_id, parent_user_id, secret_ciphertext, secret_nonce,
                      session_token_hash, inline_policy, expires_at, created_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                    vec![
                        Value::Text(rec.access_key_id.clone()),
                        Value::Text(rec.parent_user_id.0.clone()),
                        Value::Blob(rec.secret_ciphertext.clone()),
                        rec.secret_nonce.clone().map_or(Value::Null, Value::Blob),
                        Value::Text(rec.session_token_hash.clone()),
                        opt_text(rec.inline_policy.clone()),
                        Value::Int(rec.expires_at.0),
                        Value::Int(rec.created_at.0),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeleteExpiredSessionCredentials { before } => {
            driver
                .execute(
                    "DELETE FROM session_credentials WHERE expires_at < ?1",
                    vec![Value::Int(before.0)],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeleteSessionCredential { access_key_id } => {
            driver
                .execute(
                    "DELETE FROM session_credentials WHERE access_key_id = ?1",
                    vec![Value::Text(access_key_id.clone())],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::ClaimReplicationBatch {
            limit,
            now,
            lease_secs,
        } => claim_replication_batch(driver, limit, now, lease_secs).await,
        Mutation::MarkReplicationDone(id) => {
            if let Some(row) = query_one(
                driver,
                "SELECT bucket_name, key, version_id FROM replication_outbox WHERE id=?1",
                vec![Value::Text(id.clone())],
            )
            .await?
            {
                let (bucket, key, version) = (row.get_text(0), row.get_text(1), row.get_text(2));
                driver
                    .execute(
                        "UPDATE object_versions SET replication_status=?4 WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                        vec![
                            Value::Text(bucket),
                            Value::Text(key),
                            Value::Text(version),
                            Value::Text(
                                repl_status_str(cairn_types::meta::ReplicationStatus::Completed)
                                    .to_owned(),
                            ),
                        ],
                    )
                    .await?;
            }
            driver
                .execute(
                    "UPDATE replication_outbox SET status='completed' WHERE id=?1",
                    vec![Value::Text(id)],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::MarkReplicationFailed {
            id,
            error,
            next_attempt_at,
        } => {
            match next_attempt_at {
                Some(t) => {
                    driver
                        .execute(
                            "UPDATE replication_outbox SET attempts=attempts+1, last_error=?2, next_attempt_at=?3, status='pending' WHERE id=?1",
                            vec![Value::Text(id), Value::Text(error), Value::Int(t.0)],
                        )
                        .await?;
                }
                None => {
                    driver
                        .execute(
                            "UPDATE replication_outbox SET attempts=attempts+1, last_error=?2, status='failed' WHERE id=?1",
                            vec![Value::Text(id), Value::Text(error)],
                        )
                        .await?;
                }
            }
            Ok(MutationOutcome::Ack)
        }
        Mutation::EnqueueReplication(e) => {
            // Idempotent (INSERT OR IGNORE on the deterministic backfill id); see the sync store.
            driver
                .execute(
                    "INSERT OR IGNORE INTO replication_outbox
                     (id, bucket_name, key, version_id, operation, rule_id, target_arn, attempts, next_attempt_at, status, last_error, priority, lease_until, enqueued_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                    vec![
                        Value::Text(e.id.clone()),
                        Value::Text(e.bucket.as_str().to_owned()),
                        Value::Text(e.key.as_str().to_owned()),
                        Value::Text(e.version_id.as_str().to_owned()),
                        Value::Text(repl_op_str(e.operation).to_owned()),
                        Value::Text(e.rule_id.clone()),
                        opt_text(e.target_arn.clone()),
                        Value::Int(e.attempts as i64),
                        Value::Int(e.next_attempt_at.0),
                        Value::Text(repl_status_str(e.status).to_owned()),
                        opt_text(e.last_error.clone()),
                        Value::Int(e.priority),
                        e.lease_until.map_or(Value::Null, |t| Value::Int(t.0)),
                        Value::Int(e.enqueued_at.0),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeferReplication {
            id,
            next_attempt_at,
            last_error,
        } => {
            // Mirrors cairn-meta: release the claim and re-schedule without touching `attempts`
            // (a deferral/unavailability is not a failure). COALESCE keeps the prior error when no
            // new one is supplied.
            driver
                .execute(
                    "UPDATE replication_outbox \
                     SET status='pending', lease_until=NULL, next_attempt_at=?2, \
                         last_error=COALESCE(?3, last_error) \
                     WHERE id=?1",
                    vec![
                        Value::Text(id),
                        Value::Int(next_attempt_at.0),
                        opt_text(last_error),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RecoverClaimedReplication => {
            // Mirrors cairn-meta: release orphaned `claimed` rows to `pending` at startup.
            driver
                .execute(
                    "UPDATE replication_outbox SET status='pending', lease_until=NULL WHERE status='claimed'",
                    vec![],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::PruneReplicationOutbox { before_ms } => {
            // Mirrors cairn-meta: reclaim terminal (completed/failed) rows older than the horizon;
            // pending/claimed are never pruned.
            driver
                .execute(
                    "DELETE FROM replication_outbox \
                     WHERE status IN ('completed','failed') AND enqueued_at < ?1",
                    vec![Value::Int(before_ms)],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::EnqueueWebhooks(entries) => {
            for e in &entries {
                enqueue_webhook(driver, e).await?;
            }
            Ok(MutationOutcome::Ack)
        }
        Mutation::ClaimWebhookBatch {
            limit,
            now,
            lease_secs,
        } => claim_webhook_batch(driver, limit, now, lease_secs).await,
        Mutation::MarkWebhookDone(id) => {
            // Delete the delivered/dropped entry outright (see cairn-meta) so the success path keeps
            // the outbox bounded.
            driver
                .execute(
                    "DELETE FROM events_outbox WHERE id=?1",
                    vec![Value::Text(id)],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::MarkWebhookFailed {
            id,
            error,
            next_attempt_at,
        } => {
            match next_attempt_at {
                Some(t) => {
                    driver
                        .execute(
                            "UPDATE events_outbox SET attempts=attempts+1, last_error=?2, next_attempt_at=?3, status='pending' WHERE id=?1",
                            vec![Value::Text(id), Value::Text(error), Value::Int(t.0)],
                        )
                        .await?;
                }
                None => {
                    driver
                        .execute(
                            "UPDATE events_outbox SET attempts=attempts+1, last_error=?2, status='failed' WHERE id=?1",
                            vec![Value::Text(id), Value::Text(error)],
                        )
                        .await?;
                }
            }
            Ok(MutationOutcome::Ack)
        }
        Mutation::RecordActivity(e) => {
            driver
                .execute(
                    "INSERT INTO activity (id, action, bucket, key, size, etag, actor, at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                    vec![
                        Value::Text(e.id.clone()),
                        Value::Text(e.action.clone()),
                        opt_text(e.bucket.clone()),
                        opt_text(e.key.clone()),
                        e.size.map_or(Value::Null, |s| Value::Int(s as i64)),
                        opt_text(e.etag.clone()),
                        opt_text(e.actor.clone()),
                        Value::Int(e.at.0),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CreateShare(s) => {
            driver
                .execute(
                    "INSERT INTO object_shares
                     (token, bucket_name, key, version_id, expires_at, disposition, filename, created_by, created_at, revoked_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    vec![
                        Value::Text(s.token.clone()),
                        Value::Text(s.bucket.as_str().to_owned()),
                        Value::Text(s.key.as_str().to_owned()),
                        opt_text(s.version_id.as_ref().map(|v| v.as_str().to_owned())),
                        s.expires_at.map_or(Value::Null, |t| Value::Int(t.0)),
                        Value::Text(model::disposition_str(s.disposition).to_owned()),
                        opt_text(s.filename.clone()),
                        Value::Text(s.created_by.0.clone()),
                        Value::Int(s.created_at.0),
                        s.revoked_at.map_or(Value::Null, |t| Value::Int(t.0)),
                    ],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RevokeShare { token, now } => {
            driver
                .execute(
                    "UPDATE object_shares SET revoked_at=?2 WHERE token=?1 AND revoked_at IS NULL",
                    vec![Value::Text(token.clone()), Value::Int(now.0)],
                )
                .await?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::RecordRequestMetrics { rows, prune_before } => {
            // Accumulate each window/op/bucket/status bucket; the composite PK upsert sums counts,
            // bytes, and latency histogram so repeated flushes never double-insert (ARCH 26.5).
            for r in &rows {
                driver
                    .execute(
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
                        vec![
                            Value::Int(r.ts_bucket),
                            Value::Text(r.operation.clone()),
                            Value::Text(r.bucket.clone()),
                            Value::Text(r.status_class.clone()),
                            Value::Int(r.count as i64),
                            Value::Int(r.bytes_in as i64),
                            Value::Int(r.bytes_out as i64),
                            Value::Int(r.lat_sum_ms as i64),
                            Value::Int(r.lat_hist[0] as i64),
                            Value::Int(r.lat_hist[1] as i64),
                            Value::Int(r.lat_hist[2] as i64),
                            Value::Int(r.lat_hist[3] as i64),
                            Value::Int(r.lat_hist[4] as i64),
                            Value::Int(r.lat_hist[5] as i64),
                        ],
                    )
                    .await?;
            }
            if let Some(before) = prune_before {
                driver
                    .execute(
                        "DELETE FROM request_metrics WHERE ts_bucket < ?1",
                        vec![Value::Int(before)],
                    )
                    .await?;
            }
            Ok(MutationOutcome::Ack)
        }
    }
}

async fn put_version(
    driver: &dyn AsyncSqlDriver,
    row: ObjectVersionRow,
    precondition: &Precondition,
    replication: Vec<OutboxEntry>,
) -> R<MutationOutcome> {
    check_precondition(driver, &row.bucket, &row.key, precondition).await?;
    enforce_bucket_quota(driver, &row).await?;
    enforce_user_quota(driver, &row).await?;
    let version_id = row.version_id.clone();
    let superseded = upsert_version(driver, row).await?;
    for e in &replication {
        enqueue(driver, e).await?;
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
async fn enforce_bucket_quota(driver: &dyn AsyncSqlDriver, row: &ObjectVersionRow) -> R<()> {
    let quota: Option<i64> = query_one(
        driver,
        "SELECT quota_bytes FROM buckets WHERE name=?1",
        vec![Value::Text(row.bucket.as_str().to_owned())],
    )
    .await?
    .and_then(|r| r.get_opt_i64(0));
    let Some(quota) = quota else {
        return Ok(());
    };
    // Current logical bytes in the bucket, read O(1) from the maintained counter (Phase 2.1/2.2)
    // instead of summing every version, minus the row this upsert will replace (if present).
    let total: i64 = query_one(
        driver,
        "SELECT logical_bytes FROM bucket_stats WHERE bucket_name=?1",
        vec![Value::Text(row.bucket.as_str().to_owned())],
    )
    .await?
    .map_or(0, |r| r.get_i64(0));
    let existing: i64 = query_one(
        driver,
        "SELECT size_logical FROM object_versions
         WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        vec![
            Value::Text(row.bucket.as_str().to_owned()),
            Value::Text(row.key.as_str().to_owned()),
            Value::Text(row.version_id.as_str().to_owned()),
        ],
    )
    .await?
    .map_or(0, |r| r.get_i64(0));
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
async fn enforce_user_quota(driver: &dyn AsyncSqlDriver, row: &ObjectVersionRow) -> R<()> {
    let quota: Option<i64> = query_one(
        driver,
        "SELECT quota_bytes FROM users WHERE id=?1",
        vec![Value::Text(row.owner_id.0.clone())],
    )
    .await?
    .and_then(|r| r.get_opt_i64(0));
    let Some(quota) = quota else {
        return Ok(());
    };
    // Current logical bytes owned by this user across all buckets, read O(1) from the maintained
    // counter (Phase 2.1/2.2), minus the row this upsert replaces — but only when that existing row
    // is owned by THIS user (otherwise it is not part of this user's total to begin with).
    let total: i64 = query_one(
        driver,
        "SELECT logical_bytes FROM user_stats WHERE owner_id=?1",
        vec![Value::Text(row.owner_id.0.clone())],
    )
    .await?
    .map_or(0, |r| r.get_i64(0));
    let existing: i64 = query_one(
        driver,
        "SELECT size_logical FROM object_versions
         WHERE bucket_name=?1 AND key=?2 AND version_id=?3 AND owner_id=?4",
        vec![
            Value::Text(row.bucket.as_str().to_owned()),
            Value::Text(row.key.as_str().to_owned()),
            Value::Text(row.version_id.as_str().to_owned()),
            Value::Text(row.owner_id.0.clone()),
        ],
    )
    .await?
    .map_or(0, |r| r.get_i64(0));
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
/// Apply a signed delta to the maintained roll-up counters (Phase 2.1, ARCH 30) for `bucket` and
/// `owner`. Byte-identical SQL to the rusqlite store's `adjust_stats`; runs in the same transaction
/// as the row change so the counters never diverge from `object_versions`.
async fn adjust_stats(
    driver: &dyn AsyncSqlDriver,
    bucket: &str,
    owner: &str,
    d_versions: i64,
    d_logical: i64,
    d_physical: i64,
) -> R<()> {
    driver
        .execute(
            "INSERT INTO bucket_stats (bucket_name, versions, logical_bytes, physical_bytes)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(bucket_name) DO UPDATE SET
                versions       = versions       + excluded.versions,
                logical_bytes  = logical_bytes  + excluded.logical_bytes,
                physical_bytes = physical_bytes + excluded.physical_bytes",
            vec![
                Value::Text(bucket.to_owned()),
                Value::Int(d_versions),
                Value::Int(d_logical),
                Value::Int(d_physical),
            ],
        )
        .await?;
    driver
        .execute(
            "INSERT INTO user_stats (owner_id, logical_bytes) VALUES (?1, ?2)
             ON CONFLICT(owner_id) DO UPDATE SET logical_bytes = logical_bytes + excluded.logical_bytes",
            vec![Value::Text(owner.to_owned()), Value::Int(d_logical)],
        )
        .await?;
    Ok(())
}

async fn upsert_version(
    driver: &dyn AsyncSqlDriver,
    mut row: ObjectVersionRow,
) -> R<Option<StoragePath>> {
    // Read the row this upsert replaces (if any): its blob plus owner/byte sizes so the counters
    // can be decremented for it before the replacement is inserted.
    let existing = query_one(
        driver,
        "SELECT storage_path, owner_id, size_logical, size_physical
         FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        vec![
            Value::Text(row.bucket.as_str().to_owned()),
            Value::Text(row.key.as_str().to_owned()),
            Value::Text(row.version_id.as_str().to_owned()),
        ],
    )
    .await?;
    driver
        .execute(
            "DELETE FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
            vec![
                Value::Text(row.bucket.as_str().to_owned()),
                Value::Text(row.key.as_str().to_owned()),
                Value::Text(row.version_id.as_str().to_owned()),
            ],
        )
        .await?;
    let superseded = match existing {
        Some(r) => {
            let sp = r.get_opt_text(0);
            adjust_stats(
                driver,
                row.bucket.as_str(),
                &r.get_text(1),
                -1,
                -r.get_i64(2),
                -r.get_i64(3),
            )
            .await?;
            sp
        }
        None => None,
    };
    // Mirrors cairn-meta: a replica carries the source's (uuidv7-ordered) version id and is latest
    // only if its id is the max for the key, so an older/re-delivered replica never demotes a newer
    // version. A normal write keeps last-write-is-latest. MAX runs AFTER the same-id delete above.
    let becomes_latest =
        if row.replication_status == Some(cairn_types::meta::ReplicationStatus::Replica) {
            let m = query_one(
                driver,
                "SELECT MAX(version_id) FROM object_versions WHERE bucket_name=?1 AND key=?2",
                vec![
                    Value::Text(row.bucket.as_str().to_owned()),
                    Value::Text(row.key.as_str().to_owned()),
                ],
            )
            .await?;
            match m.and_then(|r| r.get_opt_text(0)) {
                Some(maxv) => row.version_id.as_str() >= maxv.as_str(),
                None => true,
            }
        } else {
            true
        };
    if becomes_latest {
        demote_latest(driver, &row.bucket, &row.key).await?;
    }
    row.is_latest = becomes_latest;
    insert_version(driver, &row).await?;
    Ok(superseded.map(StoragePath::from_string))
}

async fn demote_latest(driver: &dyn AsyncSqlDriver, bucket: &BucketName, key: &ObjectKey) -> R<()> {
    driver
        .execute(
            "UPDATE object_versions SET is_latest=0 WHERE bucket_name=?1 AND key=?2 AND is_latest=1",
            vec![
                Value::Text(bucket.as_str().to_owned()),
                Value::Text(key.as_str().to_owned()),
            ],
        )
        .await?;
    Ok(())
}

async fn insert_version(driver: &dyn AsyncSqlDriver, row: &ObjectVersionRow) -> R<()> {
    driver
        .execute(
            "INSERT INTO object_versions
             (id, bucket_name, key, version_id, is_latest, is_delete_marker, size_logical, size_physical,
              etag, content_type, storage_path, compression, storage_class, cold_locator, owner_id,
              user_metadata, acl, checksums, sse_descriptor, replication_status, created_at, updated_at,
              content_encoding, cache_control, content_disposition, content_language, expires)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27)",
            vec![
                Value::Text(row.id.clone()),
                Value::Text(row.bucket.as_str().to_owned()),
                Value::Text(row.key.as_str().to_owned()),
                Value::Text(row.version_id.as_str().to_owned()),
                Value::Int(i64::from(row.is_latest)),
                Value::Int(i64::from(row.is_delete_marker)),
                Value::Int(row.size_logical as i64),
                Value::Int(row.size_physical as i64),
                Value::Text(row.etag.as_str().to_owned()),
                Value::Text(row.content_type.clone()),
                opt_text(row.storage_path.as_ref().map(|p| p.as_str().to_owned())),
                Value::Text(to_json(&row.compression)),
                Value::Text(storage_class_str(row.storage_class).to_owned()),
                opt_text(row.cold_locator.clone()),
                Value::Text(row.owner_id.0.clone()),
                Value::Text(to_json(&row.user_metadata)),
                opt_text(row.acl.as_ref().map(to_json)),
                Value::Text(to_json(&row.checksums)),
                opt_text(row.sse_descriptor.clone()),
                opt_text(row.replication_status.map(|s| repl_status_str(s).to_owned())),
                Value::Int(row.created_at.0),
                Value::Int(row.updated_at.0),
                opt_text(row.content_encoding.clone()),
                opt_text(row.cache_control.clone()),
                opt_text(row.content_disposition.clone()),
                opt_text(row.content_language.clone()),
                opt_text(row.expires.clone()),
            ],
        )
        .await?;
    // Maintain the roll-up counters in lockstep: this new row adds one version and its bytes.
    adjust_stats(
        driver,
        row.bucket.as_str(),
        row.owner_id.0.as_str(),
        1,
        row.size_logical as i64,
        row.size_physical as i64,
    )
    .await?;
    Ok(())
}

async fn delete_version(
    driver: &dyn AsyncSqlDriver,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: &VersionId,
) -> R<MutationOutcome> {
    // Read the row's blob, latest flag, and owner/byte sizes before deleting, so we can promote a
    // successor and decrement the roll-up counters for the removed version.
    let existing = query_one(
        driver,
        "SELECT storage_path, is_latest, owner_id, size_logical, size_physical
         FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        vec![
            Value::Text(bucket.as_str().to_owned()),
            Value::Text(key.as_str().to_owned()),
            Value::Text(version_id.as_str().to_owned()),
        ],
    )
    .await?;
    let (freed, was_latest, removed) = match existing {
        Some(r) => (
            r.get_opt_text(0).map(StoragePath::from_string),
            r.get_i64(1) != 0,
            Some((r.get_text(2), r.get_i64(3), r.get_i64(4))),
        ),
        None => (None, false, None),
    };
    driver
        .execute(
            "DELETE FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
            vec![
                Value::Text(bucket.as_str().to_owned()),
                Value::Text(key.as_str().to_owned()),
                Value::Text(version_id.as_str().to_owned()),
            ],
        )
        .await?;
    // Drop any Object Lock side-row for the removed version (mirrors cairn-meta).
    driver
        .execute(
            "DELETE FROM object_locks WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
            vec![
                Value::Text(bucket.as_str().to_owned()),
                Value::Text(key.as_str().to_owned()),
                Value::Text(version_id.as_str().to_owned()),
            ],
        )
        .await?;
    if let Some((owner, sl, sp_bytes)) = removed {
        // The deleted row leaves the table: subtract its version and bytes from the counters.
        adjust_stats(driver, bucket.as_str(), &owner, -1, -sl, -sp_bytes).await?;
    }
    let mut promoted = false;
    if was_latest {
        let promote: Option<String> = query_one(
            driver,
            "SELECT id FROM object_versions WHERE bucket_name=?1 AND key=?2 ORDER BY version_id DESC LIMIT 1",
            vec![
                Value::Text(bucket.as_str().to_owned()),
                Value::Text(key.as_str().to_owned()),
            ],
        )
        .await?
        .map(|r| r.get_text(0));
        if let Some(id) = promote {
            driver
                .execute(
                    "UPDATE object_versions SET is_latest=1 WHERE id=?1",
                    vec![Value::Text(id)],
                )
                .await?;
            promoted = true;
        }
    }
    Ok(MutationOutcome::Deleted {
        freed,
        promoted_latest: promoted,
    })
}

async fn claim_multipart(
    driver: &dyn AsyncSqlDriver,
    upload_id: &cairn_types::UploadId,
) -> R<MutationOutcome> {
    let status: Option<String> = query_one(
        driver,
        "SELECT status FROM multipart_uploads WHERE id=?1",
        vec![Value::Text(upload_id.as_str().to_owned())],
    )
    .await?
    .map(|r| r.get_text(0));
    let outcome = match status.as_deref() {
        Some("active") => {
            driver
                .execute(
                    "UPDATE multipart_uploads SET status='completing', updated_at=updated_at WHERE id=?1",
                    vec![Value::Text(upload_id.as_str().to_owned())],
                )
                .await?;
            let row = query_one(
                driver,
                &format!(
                    "SELECT {} FROM multipart_uploads WHERE id=?1",
                    model::MULTIPART_COLS
                ),
                vec![Value::Text(upload_id.as_str().to_owned())],
            )
            .await?
            .ok_or_else(|| MetaError::Engine("multipart row vanished".to_owned()))?;
            let session = model::multipart_from_row(&row)?;
            cairn_types::meta::ClaimOutcome::Claimed(Box::new(session))
        }
        Some(_) => cairn_types::meta::ClaimOutcome::AlreadyClaimed,
        None => cairn_types::meta::ClaimOutcome::NotFound,
    };
    Ok(MutationOutcome::MultipartClaim(outcome))
}

/// Evaluate a conditional-write precondition against the current latest non-delete-marker
/// version, inside the transaction.
async fn check_precondition(
    driver: &dyn AsyncSqlDriver,
    bucket: &BucketName,
    key: &ObjectKey,
    pc: &Precondition,
) -> R<()> {
    if pc.is_unconditional() {
        return Ok(());
    }
    let current: Option<String> = query_one(
        driver,
        "SELECT etag FROM object_versions
         WHERE bucket_name=?1 AND key=?2 AND is_latest=1 AND is_delete_marker=0",
        vec![
            Value::Text(bucket.as_str().to_owned()),
            Value::Text(key.as_str().to_owned()),
        ],
    )
    .await?
    .map(|r| r.get_text(0));
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

async fn enqueue(driver: &dyn AsyncSqlDriver, e: &OutboxEntry) -> R<()> {
    driver
        .execute(
            "INSERT INTO replication_outbox
             (id, bucket_name, key, version_id, operation, rule_id, target_arn, attempts, next_attempt_at, status, last_error, priority, lease_until, enqueued_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            vec![
                Value::Text(e.id.clone()),
                Value::Text(e.bucket.as_str().to_owned()),
                Value::Text(e.key.as_str().to_owned()),
                Value::Text(e.version_id.as_str().to_owned()),
                Value::Text(repl_op_str(e.operation).to_owned()),
                Value::Text(e.rule_id.clone()),
                opt_text(e.target_arn.clone()),
                Value::Int(e.attempts as i64),
                Value::Int(e.next_attempt_at.0),
                Value::Text(repl_status_str(e.status).to_owned()),
                opt_text(e.last_error.clone()),
                Value::Int(e.priority),
                e.lease_until.map_or(Value::Null, |t| Value::Int(t.0)),
                Value::Int(e.enqueued_at.0),
            ],
        )
        .await?;
    Ok(())
}

/// Atomically claim up to `limit` due outbox entries: an entry is due when it is `pending`, or
/// `claimed` with an expired lease, and its `next_attempt_at` has passed. Claimed entries are
/// marked `status='claimed', lease_until = now + lease_secs` and returned. This runs inside the
/// writer's transaction, so the select-and-mark is atomic against other claimers.
async fn claim_replication_batch(
    driver: &dyn AsyncSqlDriver,
    limit: u32,
    now: Timestamp,
    lease_secs: i64,
) -> R<MutationOutcome> {
    let lease_until = now.0 + lease_secs * 1000;
    let id_rows = driver
        .query(
            "SELECT id FROM replication_outbox
             WHERE (status='pending' OR (status='claimed' AND lease_until < ?1))
               AND next_attempt_at <= ?1
             ORDER BY priority DESC, next_attempt_at LIMIT ?2",
            vec![Value::Int(now.0), Value::Int(i64::from(limit))],
        )
        .await?;
    let ids: Vec<String> = id_rows.iter().map(|r| r.get_text(0)).collect();
    let mut claimed = Vec::with_capacity(ids.len());
    for id in &ids {
        driver
            .execute(
                "UPDATE replication_outbox SET status='claimed', lease_until=?2 WHERE id=?1",
                vec![Value::Text(id.clone()), Value::Int(lease_until)],
            )
            .await?;
        let row = query_one(
            driver,
            &format!(
                "SELECT {} FROM replication_outbox WHERE id=?1",
                model::OUTBOX_COLS
            ),
            vec![Value::Text(id.clone())],
        )
        .await?
        .ok_or_else(|| MetaError::Engine("claimed outbox row vanished".to_owned()))?;
        claimed.push(model::outbox_from_row(&row)?);
    }
    Ok(MutationOutcome::ReplicationBatch(claimed))
}

async fn enqueue_webhook(driver: &dyn AsyncSqlDriver, e: &cairn_types::WebhookEntry) -> R<()> {
    driver
        .execute(
            "INSERT OR IGNORE INTO events_outbox
             (id, bucket_name, key, version_id, event_type, endpoint_id, payload, attempts, next_attempt_at, status, last_error, priority, lease_until)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            vec![
                Value::Text(e.id.clone()),
                Value::Text(e.bucket.as_str().to_owned()),
                Value::Text(e.key.as_str().to_owned()),
                Value::Text(e.version_id.as_str().to_owned()),
                Value::Text(model::event_kind_str(e.event).to_owned()),
                Value::Text(e.endpoint_id.clone()),
                Value::Text(e.payload.clone()),
                Value::Int(e.attempts as i64),
                Value::Int(e.next_attempt_at.0),
                Value::Text(model::webhook_status_str(e.status).to_owned()),
                opt_text(e.last_error.clone()),
                Value::Int(e.priority),
                e.lease_until.map_or(Value::Null, |t| Value::Int(t.0)),
            ],
        )
        .await?;
    Ok(())
}

async fn claim_webhook_batch(
    driver: &dyn AsyncSqlDriver,
    limit: u32,
    now: Timestamp,
    lease_secs: i64,
) -> R<MutationOutcome> {
    let lease_until = now.0 + lease_secs * 1000;
    let id_rows = driver
        .query(
            "SELECT id FROM events_outbox
             WHERE (status='pending' OR (status='claimed' AND lease_until < ?1))
               AND next_attempt_at <= ?1
             ORDER BY priority DESC, next_attempt_at LIMIT ?2",
            vec![Value::Int(now.0), Value::Int(i64::from(limit))],
        )
        .await?;
    let ids: Vec<String> = id_rows.iter().map(|r| r.get_text(0)).collect();
    let mut claimed = Vec::with_capacity(ids.len());
    for id in &ids {
        driver
            .execute(
                "UPDATE events_outbox SET status='claimed', lease_until=?2 WHERE id=?1",
                vec![Value::Text(id.clone()), Value::Int(lease_until)],
            )
            .await?;
        let row = query_one(
            driver,
            &format!(
                "SELECT {} FROM events_outbox WHERE id=?1",
                model::WEBHOOK_COLS
            ),
            vec![Value::Text(id.clone())],
        )
        .await?
        .ok_or_else(|| MetaError::Engine("claimed webhook row vanished".to_owned()))?;
        claimed.push(model::webhook_from_row(&row)?);
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
