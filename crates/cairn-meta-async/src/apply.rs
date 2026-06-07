//! Applying one [`Mutation`] to the write driver, ported from `cairn-meta/src/apply.rs`. Each
//! call runs inside its own savepoint (managed by the async writer), so returning `Err` rolls
//! back only this mutation while its batch-mates commit. Preconditions are evaluated here, inside
//! the transaction, so the check and the upsert are atomic with respect to every other writer
//! (ARCH §11.6). The SQL, precondition logic, savepoint semantics, and outcomes are identical to
//! the rusqlite store.

use crate::driver::{AsyncSqlDriver, Value, query_one};
use crate::model::{self, opt_text, repl_op_str, repl_status_str, storage_class_str, to_json};
use cairn_types::MetaError;
use cairn_types::id::{BucketName, ObjectKey, StoragePath, VersionId};
use cairn_types::meta::{IfNoneMatch, Mutation, MutationOutcome, OutboxEntry, Precondition};
use cairn_types::object::{ETag, ObjectVersionRow};

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
            if let Some(e) = replication {
                enqueue(driver, &e).await?;
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
                     (id, bucket_name, key, content_type, status, owner_id, intended_acl, user_metadata, created_at, updated_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    vec![
                        Value::Text(s.upload_id.as_str().to_owned()),
                        Value::Text(s.bucket.as_str().to_owned()),
                        Value::Text(s.key.as_str().to_owned()),
                        Value::Text(s.content_type.clone()),
                        Value::Text(model::mp_status_str(s.status).to_owned()),
                        Value::Text(s.owner_id.0.clone()),
                        opt_text(s.intended_acl.as_ref().map(to_json)),
                        Value::Text(to_json(&s.user_metadata)),
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
            let version_id = row.version_id.clone();
            let superseded = upsert_version(driver, *row).await?;
            driver
                .execute(
                    "DELETE FROM multipart_uploads WHERE id=?1",
                    vec![Value::Text(upload_id.as_str().to_owned())],
                )
                .await?;
            if let Some(e) = replication {
                enqueue(driver, &e).await?;
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
            // `compression_policy` is the spec column name (ARCH §34.1); `quota_bytes` defaults to
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
            driver
                .execute(
                    "INSERT OR REPLACE INTO users
                     (id, display_name, access_key_id, secret_hash, sigv4_access_key_id,
                      sigv4_secret_ciphertext, sigv4_secret_nonce, role, is_active, created_at, updated_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
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
    }
}

async fn put_version(
    driver: &dyn AsyncSqlDriver,
    row: ObjectVersionRow,
    precondition: &Precondition,
    replication: Option<OutboxEntry>,
) -> R<MutationOutcome> {
    check_precondition(driver, &row.bucket, &row.key, precondition).await?;
    enforce_bucket_quota(driver, &row).await?;
    let version_id = row.version_id.clone();
    let superseded = upsert_version(driver, row).await?;
    if let Some(e) = replication {
        enqueue(driver, &e).await?;
    }
    Ok(MutationOutcome::Put {
        superseded,
        version_id,
    })
}

/// Enforce a bucket's optional byte quota inside the commit transaction (ARCH §27.5/§28.2).
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
    // Current logical bytes in the bucket, excluding the row this upsert will replace.
    let current: i64 = query_one(
        driver,
        "SELECT COALESCE(SUM(size_logical), 0) FROM object_versions
         WHERE bucket_name=?1 AND NOT (key=?2 AND version_id=?3)",
        vec![
            Value::Text(row.bucket.as_str().to_owned()),
            Value::Text(row.key.as_str().to_owned()),
            Value::Text(row.version_id.as_str().to_owned()),
        ],
    )
    .await?
    .map_or(0, |r| r.get_i64(0));
    // Saturating add in u128 so a pathological size can never wrap past the quota check.
    let projected = u128::from(current.max(0) as u64) + u128::from(row.size_logical);
    if projected > u128::from(quota.max(0) as u64) {
        return Err(MetaError::QuotaExceeded);
    }
    Ok(())
}

/// Replace any existing row at (bucket,key,version_id) — capturing its blob for reclamation —
/// demote the key's other versions, and insert the new latest row.
async fn upsert_version(
    driver: &dyn AsyncSqlDriver,
    row: ObjectVersionRow,
) -> R<Option<StoragePath>> {
    let superseded: Option<String> = query_one(
        driver,
        "SELECT storage_path FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        vec![
            Value::Text(row.bucket.as_str().to_owned()),
            Value::Text(row.key.as_str().to_owned()),
            Value::Text(row.version_id.as_str().to_owned()),
        ],
    )
    .await?
    .and_then(|r| r.get_opt_text(0));
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
    demote_latest(driver, &row.bucket, &row.key).await?;
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
              user_metadata, acl, checksums, sse_descriptor, replication_status, created_at, updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
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
            ],
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
    let existing = query_one(
        driver,
        "SELECT storage_path, is_latest FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        vec![
            Value::Text(bucket.as_str().to_owned()),
            Value::Text(key.as_str().to_owned()),
            Value::Text(version_id.as_str().to_owned()),
        ],
    )
    .await?;
    let (freed, was_latest) = match existing {
        Some(r) => (
            r.get_opt_text(0).map(StoragePath::from_string),
            r.get_i64(1) != 0,
        ),
        None => (None, false),
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
             (id, bucket_name, key, version_id, operation, rule_id, attempts, next_attempt_at, status, last_error)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            vec![
                Value::Text(e.id.clone()),
                Value::Text(e.bucket.as_str().to_owned()),
                Value::Text(e.key.as_str().to_owned()),
                Value::Text(e.version_id.as_str().to_owned()),
                Value::Text(repl_op_str(e.operation).to_owned()),
                Value::Text(e.rule_id.clone()),
                Value::Int(e.attempts as i64),
                Value::Int(e.next_attempt_at.0),
                Value::Text(repl_status_str(e.status).to_owned()),
                opt_text(e.last_error.clone()),
            ],
        )
        .await?;
    Ok(())
}

fn config_aspect_str(a: cairn_types::bucket::ConfigAspect) -> &'static str {
    use cairn_types::bucket::ConfigAspect::*;
    match a {
        Policy => "policy",
        Acl => "acl",
        Cors => "cors",
        Lifecycle => "lifecycle",
        Replication => "replication",
        Tagging => "tagging",
        PublicAccessBlock => "public_access_block",
    }
}

/// The string form of a config aspect (shared with the read path).
pub fn aspect_str(a: cairn_types::bucket::ConfigAspect) -> &'static str {
    config_aspect_str(a)
}
