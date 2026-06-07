//! Applying one [`Mutation`] to the write connection. Each call runs inside its own savepoint
//! (managed by the writer), so returning `Err` rolls back only this mutation while its
//! batch-mates commit. Preconditions are evaluated here, inside the transaction, so the check
//! and the upsert are atomic with respect to every other writer (ARCH §11.6).

use crate::model::{self, engine_err, repl_op_str, repl_status_str, storage_class_str, to_json};
use cairn_types::MetaError;
use cairn_types::id::{BucketName, ObjectKey, StoragePath, VersionId};
use cairn_types::meta::{IfNoneMatch, Mutation, MutationOutcome, OutboxEntry, Precondition};
use cairn_types::object::{ETag, ObjectVersionRow};
use rusqlite::{Connection, OptionalExtension, params};

type R<T> = Result<T, MetaError>;

/// Apply a mutation, returning its typed outcome or a typed error.
pub fn apply(conn: &Connection, m: Mutation) -> R<MutationOutcome> {
    match m {
        Mutation::PutObjectVersion {
            row,
            precondition,
            replication,
        } => put_version(conn, *row, &precondition, replication),
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
                replication_status: None,
                created_at: now,
                updated_at: now,
            };
            demote_latest(conn, &row.bucket, &row.key)?;
            insert_version(conn, &row)?;
            if let Some(e) = replication {
                enqueue(conn, &e)?;
            }
            Ok(MutationOutcome::DeleteMarker { version_id })
        }
        Mutation::DeleteVersion {
            bucket,
            key,
            version_id,
        } => delete_version(conn, &bucket, &key, &version_id),
        Mutation::CreateMultipart(s) => {
            conn.execute(
                "INSERT INTO multipart_uploads
                 (id, bucket_name, key, content_type, status, owner_id, intended_acl, user_metadata, created_at, updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    s.upload_id.as_str(),
                    s.bucket.as_str(),
                    s.key.as_str(),
                    s.content_type,
                    model::mp_status_str(s.status),
                    s.owner_id.0,
                    s.intended_acl.as_ref().map(to_json),
                    to_json(&s.user_metadata),
                    s.created_at.0,
                    s.updated_at.0,
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::MultipartCreated(s.upload_id))
        }
        Mutation::RecordPart { upload_id, part } => {
            let superseded: Option<String> = conn
                .query_row(
                    "SELECT storage_path FROM multipart_parts WHERE upload_id=?1 AND part_number=?2",
                    params![upload_id.as_str(), part.part_number],
                    |r| r.get(0),
                )
                .optional()
                .map_err(engine_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO multipart_parts
                 (upload_id, part_number, size, etag, storage_path, checksum)
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    upload_id.as_str(),
                    part.part_number,
                    part.size as i64,
                    part.etag,
                    part.storage_path.as_str(),
                    part.checksum.as_ref().map(to_json),
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::PartRecorded {
                superseded: superseded.map(StoragePath::from_string),
            })
        }
        Mutation::ClaimMultipart(upload_id) => claim_multipart(conn, &upload_id),
        Mutation::CompleteMultipart {
            upload_id,
            row,
            precondition,
            replication,
        } => {
            let bucket = row.bucket.clone();
            let key = row.key.clone();
            check_precondition(conn, &bucket, &key, &precondition)?;
            enforce_bucket_quota(conn, &row)?;
            let version_id = row.version_id.clone();
            let superseded = upsert_version(conn, *row)?;
            conn.execute(
                "DELETE FROM multipart_uploads WHERE id=?1",
                params![upload_id.as_str()],
            )
            .map_err(engine_err)?;
            if let Some(e) = replication {
                enqueue(conn, &e)?;
            }
            Ok(MutationOutcome::MultipartCompleted {
                superseded,
                version_id,
            })
        }
        Mutation::AbortMultipart(upload_id) => {
            conn.execute(
                "DELETE FROM multipart_uploads WHERE id=?1",
                params![upload_id.as_str()],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::CreateBucket(b) => {
            // `compression_policy` is the spec column name (ARCH §34.1); `quota_bytes` defaults to
            // NULL (unlimited) since the frozen `Bucket` domain type carries no quota field.
            conn.execute(
                "INSERT INTO buckets (name, owner_id, created_at, versioning_state, ownership_mode, region, compression_policy)
                 VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    b.name.as_str(),
                    b.owner_id.0,
                    b.created_at.0,
                    model::versioning_str(b.versioning),
                    model::ownership_str(b.ownership_mode),
                    b.region,
                    b.compression.as_ref().map(to_json),
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeleteBucket(name) => {
            conn.execute(
                "DELETE FROM bucket_config WHERE bucket_name=?1",
                params![name.as_str()],
            )
            .map_err(engine_err)?;
            conn.execute("DELETE FROM buckets WHERE name=?1", params![name.as_str()])
                .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetBucketConfig {
            bucket,
            aspect,
            doc,
        } => {
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
        Mutation::SetVersioning { bucket, state } => {
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
            conn.execute(
                "DELETE FROM object_tags WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                params![bucket.as_str(), key.as_str(), version_id.as_str()],
            )
            .map_err(engine_err)?;
            for (k, v) in &tags {
                conn.execute(
                    "INSERT INTO object_tags (bucket_name, key, version_id, tag_key, tag_value) VALUES (?1,?2,?3,?4,?5)",
                    params![bucket.as_str(), key.as_str(), version_id.as_str(), k, v],
                )
                .map_err(engine_err)?;
            }
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
            let c = model::user_record_columns(&rec);
            conn.execute(
                "INSERT OR REPLACE INTO users
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
            Ok(MutationOutcome::Ack)
        }
        Mutation::DeactivateUser(id) => {
            conn.execute("UPDATE users SET is_active=0 WHERE id=?1", params![id.0])
                .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::MarkReplicationDone(id) => {
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
                    "UPDATE object_versions SET replication_status=?4 WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                    params![bucket, key, version, repl_status_str(cairn_types::meta::ReplicationStatus::Completed)],
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
            Ok(MutationOutcome::Ack)
        }
        Mutation::RecordActivity(e) => {
            conn.execute(
                "INSERT INTO activity (id, action, bucket, key, size, etag, actor, at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    e.id,
                    e.action,
                    e.bucket,
                    e.key,
                    e.size.map(|s| s as i64),
                    e.etag,
                    e.actor,
                    e.at.0
                ],
            )
            .map_err(engine_err)?;
            Ok(MutationOutcome::Ack)
        }
    }
}

fn put_version(
    conn: &Connection,
    row: ObjectVersionRow,
    precondition: &Precondition,
    replication: Option<OutboxEntry>,
) -> R<MutationOutcome> {
    check_precondition(conn, &row.bucket, &row.key, precondition)?;
    enforce_bucket_quota(conn, &row)?;
    let version_id = row.version_id.clone();
    let superseded = upsert_version(conn, row)?;
    if let Some(e) = replication {
        enqueue(conn, &e)?;
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
fn enforce_bucket_quota(conn: &Connection, row: &ObjectVersionRow) -> R<()> {
    let quota: Option<i64> = conn
        .query_row(
            "SELECT quota_bytes FROM buckets WHERE name=?1",
            params![row.bucket.as_str()],
            |r| r.get(0),
        )
        .optional()
        .map_err(engine_err)?
        .flatten();
    let Some(quota) = quota else {
        return Ok(());
    };
    // Current logical bytes in the bucket, excluding the row this upsert will replace.
    let current: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(size_logical), 0) FROM object_versions
             WHERE bucket_name=?1 AND NOT (key=?2 AND version_id=?3)",
            params![
                row.bucket.as_str(),
                row.key.as_str(),
                row.version_id.as_str()
            ],
            |r| r.get(0),
        )
        .map_err(engine_err)?;
    // Saturating add in u128 so a pathological size can never wrap past the quota check.
    let projected = u128::from(current.max(0) as u64) + u128::from(row.size_logical);
    if projected > u128::from(quota.max(0) as u64) {
        return Err(MetaError::QuotaExceeded);
    }
    Ok(())
}

/// Replace any existing row at (bucket,key,version_id) — capturing its blob for reclamation —
/// demote the key's other versions, and insert the new latest row.
fn upsert_version(conn: &Connection, row: ObjectVersionRow) -> R<Option<StoragePath>> {
    let superseded: Option<String> = conn
        .query_row(
            "SELECT storage_path FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
            params![row.bucket.as_str(), row.key.as_str(), row.version_id.as_str()],
            |r| r.get(0),
        )
        .optional()
        .map_err(engine_err)?
        .flatten();
    conn.execute(
        "DELETE FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        params![
            row.bucket.as_str(),
            row.key.as_str(),
            row.version_id.as_str()
        ],
    )
    .map_err(engine_err)?;
    demote_latest(conn, &row.bucket, &row.key)?;
    insert_version(conn, &row)?;
    Ok(superseded.map(StoragePath::from_string))
}

fn demote_latest(conn: &Connection, bucket: &BucketName, key: &ObjectKey) -> R<()> {
    conn.execute(
        "UPDATE object_versions SET is_latest=0 WHERE bucket_name=?1 AND key=?2 AND is_latest=1",
        params![bucket.as_str(), key.as_str()],
    )
    .map_err(engine_err)?;
    Ok(())
}

fn insert_version(conn: &Connection, row: &ObjectVersionRow) -> R<()> {
    conn.execute(
        "INSERT INTO object_versions
         (id, bucket_name, key, version_id, is_latest, is_delete_marker, size_logical, size_physical,
          etag, content_type, storage_path, compression, storage_class, cold_locator, owner_id,
          user_metadata, acl, checksums, replication_status, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
        params![
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
            row.storage_path.as_ref().map(|p| p.as_str().to_owned()),
            to_json(&row.compression),
            storage_class_str(row.storage_class),
            row.cold_locator,
            row.owner_id.0,
            to_json(&row.user_metadata),
            row.acl.as_ref().map(to_json),
            to_json(&row.checksums),
            row.replication_status.map(repl_status_str),
            row.created_at.0,
            row.updated_at.0,
        ],
    )
    .map_err(engine_err)?;
    Ok(())
}

fn delete_version(
    conn: &Connection,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: &VersionId,
) -> R<MutationOutcome> {
    let existing: Option<(Option<String>, i64)> = conn
        .query_row(
            "SELECT storage_path, is_latest FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
            params![bucket.as_str(), key.as_str(), version_id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(engine_err)?;
    let (freed, was_latest) = match existing {
        Some((sp, latest)) => (sp.map(StoragePath::from_string), latest != 0),
        None => (None, false),
    };
    conn.execute(
        "DELETE FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
        params![bucket.as_str(), key.as_str(), version_id.as_str()],
    )
    .map_err(engine_err)?;
    let mut promoted = false;
    if was_latest {
        let promote: Option<String> = conn
            .query_row(
                "SELECT id FROM object_versions WHERE bucket_name=?1 AND key=?2 ORDER BY version_id DESC LIMIT 1",
                params![bucket.as_str(), key.as_str()],
                |r| r.get(0),
            )
            .optional()
            .map_err(engine_err)?;
        if let Some(id) = promote {
            conn.execute(
                "UPDATE object_versions SET is_latest=1 WHERE id=?1",
                params![id],
            )
            .map_err(engine_err)?;
            promoted = true;
        }
    }
    Ok(MutationOutcome::Deleted {
        freed,
        promoted_latest: promoted,
    })
}

fn claim_multipart(conn: &Connection, upload_id: &cairn_types::UploadId) -> R<MutationOutcome> {
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
                "UPDATE multipart_uploads SET status='completing', updated_at=updated_at WHERE id=?1",
                params![upload_id.as_str()],
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
        .query_row(
            "SELECT etag FROM object_versions
             WHERE bucket_name=?1 AND key=?2 AND is_latest=1 AND is_delete_marker=0",
            params![bucket.as_str(), key.as_str()],
            |r| r.get(0),
        )
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
    conn.execute(
        "INSERT INTO replication_outbox
         (id, bucket_name, key, version_id, operation, rule_id, attempts, next_attempt_at, status, last_error)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            e.id,
            e.bucket.as_str(),
            e.key.as_str(),
            e.version_id.as_str(),
            repl_op_str(e.operation),
            e.rule_id,
            e.attempts as i64,
            e.next_attempt_at.0,
            repl_status_str(e.status),
            e.last_error,
        ],
    )
    .map_err(engine_err)?;
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
            storage_path: Some(StoragePath::from_string(format!("{bucket}/{version}"))),
            compression: CompressionDescriptor::Uncompressed,
            storage_class: StorageClass::Standard,
            cold_locator: None,
            owner_id: UserId("owner".to_owned()),
            user_metadata: Vec::new(),
            acl: None,
            checksums: Vec::new(),
            replication_status: None,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        }
    }

    fn put(row: ObjectVersionRow) -> Mutation {
        Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: None,
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
                replication: None,
            },
        )
        .unwrap();
    }
}
