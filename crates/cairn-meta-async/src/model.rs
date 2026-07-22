//! Conversions between driver [`Row`]s and domain types, and the enum<->text mappings, ported
//! from `cairn-meta/src/model.rs`. The rusqlite store maps rows by column name; the async
//! driver yields positional cells, so reads select an explicit, fixed column list (the `*_COLS`
//! constants below) and the mappers index those positions. The JSON column encoding, the
//! enum<->text strings, and the resulting domain values are identical to the rusqlite store.

use crate::driver::{Row, Value};
use cairn_types::MetaError;
use cairn_types::auth::Role;
use cairn_types::authz::{Acl, OwnershipMode};
use cairn_types::bucket::{Bucket, CompressionPolicy, VersioningState};
use cairn_types::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use cairn_types::meta::{
    ActivityEntry, ImportJob, ImportJobRecord, ImportState, MultipartSession, MultipartStatus,
    ObjectSummary, OutboxEntry, PartRecord, ReplicationOp, ReplicationStatus, ShareDisposition,
    ShareRow, User, UserRecord, UserSigV4Credentials, UserWithBearerHash, WebhookEntry,
    WebhookStatus,
};
use cairn_types::notification::EventKind;
use cairn_types::object::{
    ChecksumValue, CompressionDescriptor, ETag, ObjectVersionRow, StorageClass, UserMetadata,
};
use cairn_types::time::Timestamp;

// --- canonical column lists (fix the positional order the mappers index) ---

/// `object_versions` columns in mapper order.
pub const OBJECT_VERSION_COLS: &str = "id, bucket_name, key, version_id, is_latest, \
     is_delete_marker, size_logical, size_physical, etag, content_type, storage_path, \
     compression, storage_class, cold_locator, owner_id, user_metadata, acl, checksums, \
     sse_descriptor, replication_status, created_at, updated_at, content_encoding, cache_control, \
     content_disposition, content_language, expires";

/// `buckets` columns in mapper order.
pub const BUCKET_COLS: &str =
    "name, owner_id, created_at, versioning_state, ownership_mode, region, compression_policy";

/// `multipart_uploads` columns in mapper order.
pub const MULTIPART_COLS: &str = "id, bucket_name, key, content_type, status, owner_id, \
     intended_acl, user_metadata, created_at, updated_at, sse_requested, encrypt_parts";

/// `multipart_parts` columns in mapper order.
pub const PART_COLS: &str = "part_number, size, etag, storage_path, checksum, part_dek";

/// `users` columns in mapper order (with the secret hash for the bearer mapper).
pub const USER_COLS: &str = "id, display_name, access_key_id, secret_hash, sigv4_access_key_id, \
     sigv4_secret_ciphertext, sigv4_secret_nonce, role, is_active, created_at, updated_at, \
     quota_bytes";

/// `replication_outbox` columns in mapper order.
pub const OUTBOX_COLS: &str = "id, bucket_name, key, version_id, operation, rule_id, target_arn, \
     attempts, next_attempt_at, status, last_error, priority, lease_until, enqueued_at";

/// `events_outbox` (webhook) columns in mapper order.
pub const WEBHOOK_COLS: &str = "id, bucket_name, key, version_id, event_type, endpoint_id, payload, \
     attempts, next_attempt_at, status, last_error, priority, lease_until";

/// `activity` columns in mapper order.
pub const ACTIVITY_COLS: &str = "id, action, bucket, key, size, etag, actor, at";
pub const SHARE_COLS: &str = "token, bucket_name, key, version_id, expires_at, disposition, \
     filename, created_by, created_at, revoked_at";

/// `object_summary` listing columns in mapper order.
pub const SUMMARY_COLS: &str = "key, version_id, is_latest, is_delete_marker, etag, size_logical, \
     updated_at, storage_class, owner_id";

fn json_col<T: serde::de::DeserializeOwned>(s: &str) -> Result<T, MetaError> {
    serde_json::from_str(s).map_err(|e| MetaError::Engine(format!("json column decode: {e}")))
}

/// Serialize a value to a JSON column string.
pub fn to_json<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).expect("domain types serialize cleanly")
}

// --- enum <-> text (verbatim from cairn-meta/src/model.rs) ---

pub fn role_str(r: Role) -> &'static str {
    match r {
        Role::Administrator => "administrator",
        Role::Member => "member",
    }
}
pub fn role_from(s: &str) -> Role {
    match s {
        "administrator" => Role::Administrator,
        _ => Role::Member,
    }
}

pub fn versioning_str(v: VersioningState) -> &'static str {
    match v {
        VersioningState::Unversioned => "unversioned",
        VersioningState::Enabled => "enabled",
        VersioningState::Suspended => "suspended",
    }
}

pub fn versioning_from(s: &str) -> VersioningState {
    match s {
        "enabled" => VersioningState::Enabled,
        "suspended" => VersioningState::Suspended,
        _ => VersioningState::Unversioned,
    }
}

pub fn ownership_str(o: OwnershipMode) -> &'static str {
    match o {
        OwnershipMode::BucketOwnerEnforced => "BucketOwnerEnforced",
        OwnershipMode::BucketOwnerPreferred => "BucketOwnerPreferred",
        OwnershipMode::ObjectWriter => "ObjectWriter",
    }
}
pub fn ownership_from(s: &str) -> OwnershipMode {
    match s {
        "BucketOwnerPreferred" => OwnershipMode::BucketOwnerPreferred,
        "ObjectWriter" => OwnershipMode::ObjectWriter,
        _ => OwnershipMode::BucketOwnerEnforced,
    }
}

pub fn mp_status_str(s: MultipartStatus) -> &'static str {
    match s {
        MultipartStatus::Active => "active",
        MultipartStatus::Completing => "completing",
        MultipartStatus::Aborted => "aborted",
    }
}
pub fn mp_status_from(s: &str) -> MultipartStatus {
    match s {
        "completing" => MultipartStatus::Completing,
        "aborted" => MultipartStatus::Aborted,
        _ => MultipartStatus::Active,
    }
}

pub fn lock_mode_str(m: cairn_types::object::ObjectLockMode) -> &'static str {
    match m {
        cairn_types::object::ObjectLockMode::Governance => "GOVERNANCE",
        cairn_types::object::ObjectLockMode::Compliance => "COMPLIANCE",
    }
}
/// Parse a stored lock-mode string. Unknown values fail safe to the stricter `Compliance`.
pub fn lock_mode_from(s: &str) -> cairn_types::object::ObjectLockMode {
    match s {
        "GOVERNANCE" => cairn_types::object::ObjectLockMode::Governance,
        _ => cairn_types::object::ObjectLockMode::Compliance,
    }
}

pub fn repl_status_str(s: ReplicationStatus) -> &'static str {
    match s {
        ReplicationStatus::Pending => "pending",
        ReplicationStatus::Claimed => "claimed",
        ReplicationStatus::Completed => "completed",
        ReplicationStatus::Failed => "failed",
        ReplicationStatus::Replica => "replica",
    }
}
pub fn repl_status_from(s: &str) -> ReplicationStatus {
    match s {
        "claimed" => ReplicationStatus::Claimed,
        "completed" => ReplicationStatus::Completed,
        "failed" => ReplicationStatus::Failed,
        "replica" => ReplicationStatus::Replica,
        _ => ReplicationStatus::Pending,
    }
}

pub fn repl_op_str(o: ReplicationOp) -> &'static str {
    match o {
        ReplicationOp::ObjectCreate => "object_create",
        ReplicationOp::DeleteMarker => "delete_marker",
    }
}
pub fn repl_op_from(s: &str) -> ReplicationOp {
    match s {
        "delete_marker" => ReplicationOp::DeleteMarker,
        _ => ReplicationOp::ObjectCreate,
    }
}

pub fn storage_class_str(c: StorageClass) -> &'static str {
    match c {
        StorageClass::Standard => "standard",
        StorageClass::ColdTier => "cold_tier",
    }
}
pub fn storage_class_from(s: &str) -> StorageClass {
    match s {
        "cold_tier" => StorageClass::ColdTier,
        _ => StorageClass::Standard,
    }
}

// --- row -> domain (positional, per the *_COLS lists) ---

pub fn object_version_from_row(row: &Row) -> Result<ObjectVersionRow, MetaError> {
    let compression: CompressionDescriptor = json_col(&row.get_text(11))?;
    let user_metadata: UserMetadata = json_col(&row.get_text(15))?;
    let acl: Option<Acl> = match row.get_opt_text(16) {
        Some(s) => Some(json_col(&s)?),
        None => None,
    };
    let checksums: Vec<ChecksumValue> = json_col(&row.get_text(17))?;
    Ok(ObjectVersionRow {
        id: row.get_text(0),
        bucket: BucketName::parse(&row.get_text(1)).unwrap_or_else(|_| unreachable_bucket()),
        key: ObjectKey::parse(&row.get_text(2)).unwrap_or_else(|_| unreachable_key()),
        version_id: VersionId::from_string(row.get_text(3)),
        is_latest: row.get_i64(4) != 0,
        is_delete_marker: row.get_i64(5) != 0,
        size_logical: row.get_i64(6) as u64,
        size_physical: row.get_i64(7) as u64,
        etag: ETag::from_string(row.get_text(8)),
        content_type: row.get_text(9),
        content_encoding: row.get_opt_text(22),
        cache_control: row.get_opt_text(23),
        content_disposition: row.get_opt_text(24),
        content_language: row.get_opt_text(25),
        expires: row.get_opt_text(26),
        storage_path: row.get_opt_text(10).map(StoragePath::from_string),
        compression,
        storage_class: storage_class_from(&row.get_text(12)),
        cold_locator: row.get_opt_text(13),
        owner_id: UserId(row.get_text(14)),
        user_metadata,
        acl,
        checksums,
        sse_descriptor: row.get_opt_text(18),
        replication_status: row.get_opt_text(19).map(|s| repl_status_from(&s)),
        created_at: Timestamp(row.get_i64(20)),
        updated_at: Timestamp(row.get_i64(21)),
    })
}

pub fn object_summary_from_row(row: &Row) -> Result<ObjectSummary, MetaError> {
    Ok(ObjectSummary {
        key: ObjectKey::parse(&row.get_text(0)).unwrap_or_else(|_| unreachable_key()),
        version_id: VersionId::from_string(row.get_text(1)),
        is_latest: row.get_i64(2) != 0,
        is_delete_marker: row.get_i64(3) != 0,
        etag: ETag::from_string(row.get_text(4)),
        size: row.get_i64(5) as u64,
        last_modified: Timestamp(row.get_i64(6)),
        storage_class: storage_class_from(&row.get_text(7)),
        owner_id: UserId(row.get_text(8)),
    })
}

pub fn bucket_from_row(row: &Row) -> Result<Bucket, MetaError> {
    // The column is `compression_policy` per ARCH 34.1; the domain field stays `compression`.
    let compression: Option<CompressionPolicy> = match row.get_opt_text(6) {
        Some(s) => Some(json_col(&s)?),
        None => None,
    };
    Ok(Bucket {
        name: BucketName::parse(&row.get_text(0)).unwrap_or_else(|_| unreachable_bucket()),
        owner_id: UserId(row.get_text(1)),
        created_at: Timestamp(row.get_i64(2)),
        versioning: versioning_from(&row.get_text(3)),
        ownership_mode: ownership_from(&row.get_text(4)),
        region: row.get_text(5),
        compression,
    })
}

pub fn multipart_from_row(row: &Row) -> Result<MultipartSession, MetaError> {
    let intended_acl: Option<Acl> = match row.get_opt_text(6) {
        Some(s) => Some(json_col(&s)?),
        None => None,
    };
    let user_metadata: UserMetadata = json_col(&row.get_text(7))?;
    Ok(MultipartSession {
        upload_id: UploadId::from_string(row.get_text(0)),
        bucket: BucketName::parse(&row.get_text(1)).unwrap_or_else(|_| unreachable_bucket()),
        key: ObjectKey::parse(&row.get_text(2)).unwrap_or_else(|_| unreachable_key()),
        content_type: row.get_text(3),
        status: mp_status_from(&row.get_text(4)),
        owner_id: UserId(row.get_text(5)),
        intended_acl,
        user_metadata,
        created_at: Timestamp(row.get_i64(8)),
        updated_at: Timestamp(row.get_i64(9)),
        sse_requested: row.get_i64(10) != 0,
        encrypt_parts: row.get_i64(11) != 0,
    })
}

pub fn part_from_row(row: &Row) -> Result<PartRecord, MetaError> {
    let checksum: Option<ChecksumValue> = match row.get_opt_text(4) {
        Some(s) => Some(json_col(&s)?),
        None => None,
    };
    Ok(PartRecord {
        part_number: row.get_i64(0) as u16,
        size: row.get_i64(1) as u64,
        etag: row.get_text(2),
        storage_path: StoragePath::from_string(row.get_text(3)),
        checksum,
        part_dek: row.get_opt_text(5),
    })
}

pub fn user_from_row(row: &Row) -> Result<User, MetaError> {
    Ok(User {
        id: UserId(row.get_text(0)),
        display_name: row.get_text(1),
        access_key_id: row.get_text(2),
        sigv4_access_key_id: row.get_opt_text(4),
        role: role_from(&row.get_text(7)),
        is_active: row.get_i64(8) != 0,
        created_at: Timestamp(row.get_i64(9)),
        updated_at: Timestamp(row.get_i64(10)),
        quota_bytes: row.get_opt_i64(11).map(|q| q as u64),
    })
}

pub fn user_with_bearer_from_row(row: &Row) -> Result<UserWithBearerHash, MetaError> {
    Ok(UserWithBearerHash {
        user: user_from_row(row)?,
        secret_hash: row.get_text(3),
    })
}

pub fn user_sigv4_from_row(row: &Row) -> Result<Option<UserSigV4Credentials>, MetaError> {
    let ct = row.get_opt_blob(5);
    let nonce = row.get_opt_blob(6);
    match (ct, nonce) {
        (Some(secret_ciphertext), Some(secret_nonce)) => Ok(Some(UserSigV4Credentials {
            user: user_from_row(row)?,
            secret_ciphertext,
            secret_nonce,
        })),
        _ => Ok(None),
    }
}

// --- import jobs (ARCH 27) ---

/// The secret-free column list for `import_jobs` reads (excludes secret_ciphertext/secret_nonce), in
/// the positional order [`import_job_from_row`] expects.
pub const IMPORT_JOB_COLS: &str = "id, source_endpoint, source_region, access_key_id, ca_cert_pem, \
     insecure_skip_verify, workers, state, buckets_json, objects_done, objects_total, bytes_done, \
     bytes_total, last_error, created_at, updated_at";

pub fn import_state_str(s: ImportState) -> &'static str {
    match s {
        ImportState::Pending => "pending",
        ImportState::Running => "running",
        ImportState::Completed => "completed",
        ImportState::Failed => "failed",
        ImportState::Cancelled => "cancelled",
    }
}

pub fn import_state_from(s: &str) -> ImportState {
    match s {
        "running" => ImportState::Running,
        "completed" => ImportState::Completed,
        "failed" => ImportState::Failed,
        "cancelled" => ImportState::Cancelled,
        _ => ImportState::Pending,
    }
}

/// The FULL column list for an `import_jobs` record read (includes the sealed secret), positional.
pub const IMPORT_JOB_RECORD_COLS: &str = "id, source_endpoint, source_region, access_key_id, \
     secret_ciphertext, secret_nonce, ca_cert_pem, insecure_skip_verify, workers, state, \
     buckets_json, objects_done, objects_total, bytes_done, bytes_total, last_error, lease_until, \
     created_at, updated_at";

/// Map a full `import_jobs` row (selected via [`IMPORT_JOB_RECORD_COLS`]) to the [`ImportJobRecord`],
/// **including** the sealed secret. For the server-internal import worker only.
pub fn import_job_record_from_row(row: &Row) -> Result<ImportJobRecord, MetaError> {
    Ok(ImportJobRecord {
        id: row.get_text(0),
        source_endpoint: row.get_text(1),
        source_region: row.get_text(2),
        access_key_id: row.get_text(3),
        secret_ciphertext: row.get_opt_blob(4).unwrap_or_default(),
        secret_nonce: row.get_opt_blob(5),
        ca_cert_pem: row.get_opt_text(6),
        insecure_skip_verify: row.get_i64(7) != 0,
        workers: row.get_i64(8) as u32,
        state: import_state_from(&row.get_text(9)),
        buckets: json_col(&row.get_text(10))?,
        objects_done: row.get_i64(11) as u64,
        objects_total: row.get_i64(12) as u64,
        bytes_done: row.get_i64(13) as u64,
        bytes_total: row.get_i64(14) as u64,
        last_error: row.get_opt_text(15),
        lease_until: row.get_opt_i64(16).map(Timestamp),
        created_at: Timestamp(row.get_i64(17)),
        updated_at: Timestamp(row.get_i64(18)),
    })
}

/// Map an `import_jobs` row (selected via [`IMPORT_JOB_COLS`]) to the secret-free [`ImportJob`].
pub fn import_job_from_row(row: &Row) -> Result<ImportJob, MetaError> {
    Ok(ImportJob {
        id: row.get_text(0),
        source_endpoint: row.get_text(1),
        source_region: row.get_text(2),
        access_key_id: row.get_text(3),
        has_ca_cert: row.get_opt_text(4).is_some(),
        insecure_skip_verify: row.get_i64(5) != 0,
        workers: row.get_i64(6) as u32,
        state: import_state_from(&row.get_text(7)),
        buckets: json_col(&row.get_text(8))?,
        objects_done: row.get_i64(9) as u64,
        objects_total: row.get_i64(10) as u64,
        bytes_done: row.get_i64(11) as u64,
        bytes_total: row.get_i64(12) as u64,
        last_error: row.get_opt_text(13),
        created_at: Timestamp(row.get_i64(14)),
        updated_at: Timestamp(row.get_i64(15)),
    })
}

pub fn outbox_from_row(row: &Row) -> Result<OutboxEntry, MetaError> {
    Ok(OutboxEntry {
        id: row.get_text(0),
        bucket: BucketName::parse(&row.get_text(1)).unwrap_or_else(|_| unreachable_bucket()),
        key: ObjectKey::parse(&row.get_text(2)).unwrap_or_else(|_| unreachable_key()),
        version_id: VersionId::from_string(row.get_text(3)),
        operation: repl_op_from(&row.get_text(4)),
        rule_id: row.get_text(5),
        target_arn: row.get_opt_text(6),
        attempts: row.get_i64(7) as u32,
        next_attempt_at: Timestamp(row.get_i64(8)),
        status: repl_status_from(&row.get_text(9)),
        last_error: row.get_opt_text(10),
        priority: row.get_i64(11),
        lease_until: row.get_opt_i64(12).map(Timestamp),
        enqueued_at: Timestamp(row.get_i64(13)),
    })
}

// --- webhook event-notification outbox (mirrors the replication helpers) ---

pub fn webhook_status_str(s: WebhookStatus) -> &'static str {
    match s {
        WebhookStatus::Pending => "pending",
        WebhookStatus::Claimed => "claimed",
        WebhookStatus::Completed => "completed",
        WebhookStatus::Failed => "failed",
    }
}
pub fn webhook_status_from(s: &str) -> WebhookStatus {
    match s {
        "claimed" => WebhookStatus::Claimed,
        "completed" => WebhookStatus::Completed,
        "failed" => WebhookStatus::Failed,
        _ => WebhookStatus::Pending,
    }
}
pub fn event_kind_str(e: EventKind) -> &'static str {
    e.s3_name()
}
pub fn event_kind_from(s: &str) -> EventKind {
    match s {
        "s3:ObjectCreated:Copy" => EventKind::ObjectCreatedCopy,
        "s3:ObjectCreated:CompleteMultipartUpload" => {
            EventKind::ObjectCreatedCompleteMultipartUpload
        }
        "s3:ObjectRemoved:Delete" => EventKind::ObjectRemovedDelete,
        "s3:ObjectRemoved:DeleteMarkerCreated" => EventKind::ObjectRemovedDeleteMarkerCreated,
        _ => EventKind::ObjectCreatedPut,
    }
}
pub fn webhook_from_row(row: &Row) -> Result<WebhookEntry, MetaError> {
    Ok(WebhookEntry {
        id: row.get_text(0),
        bucket: BucketName::parse(&row.get_text(1)).unwrap_or_else(|_| unreachable_bucket()),
        key: ObjectKey::parse(&row.get_text(2)).unwrap_or_else(|_| unreachable_key()),
        version_id: VersionId::from_string(row.get_text(3)),
        event: event_kind_from(&row.get_text(4)),
        endpoint_id: row.get_text(5),
        payload: row.get_text(6),
        attempts: row.get_i64(7) as u32,
        next_attempt_at: Timestamp(row.get_i64(8)),
        status: webhook_status_from(&row.get_text(9)),
        last_error: row.get_opt_text(10),
        priority: row.get_i64(11),
        lease_until: row.get_opt_i64(12).map(Timestamp),
    })
}

pub fn activity_from_row(row: &Row) -> Result<ActivityEntry, MetaError> {
    Ok(ActivityEntry {
        id: row.get_text(0),
        action: row.get_text(1),
        bucket: row.get_opt_text(2),
        key: row.get_opt_text(3),
        size: row.get_opt_i64(4).map(|s| s as u64),
        etag: row.get_opt_text(5),
        actor: row.get_opt_text(6),
        at: Timestamp(row.get_i64(7)),
    })
}

pub fn disposition_str(d: ShareDisposition) -> &'static str {
    match d {
        ShareDisposition::Inline => "inline",
        ShareDisposition::Attachment => "attachment",
    }
}
pub fn disposition_from(s: &str) -> ShareDisposition {
    match s {
        "attachment" => ShareDisposition::Attachment,
        _ => ShareDisposition::Inline,
    }
}

pub fn share_from_row(row: &Row) -> Result<ShareRow, MetaError> {
    Ok(ShareRow {
        token: row.get_text(0),
        bucket: BucketName::parse(&row.get_text(1)).unwrap_or_else(|_| unreachable_bucket()),
        key: ObjectKey::parse(&row.get_text(2)).unwrap_or_else(|_| unreachable_key()),
        version_id: row.get_opt_text(3).map(VersionId::from_string),
        expires_at: row.get_opt_i64(4).map(Timestamp),
        disposition: disposition_from(&row.get_text(5)),
        filename: row.get_opt_text(6),
        created_by: UserId(row.get_text(7)),
        created_at: Timestamp(row.get_i64(8)),
        revoked_at: row.get_opt_i64(9).map(Timestamp),
    })
}

/// Build the full credential column values for inserts/updates, as bound [`Value`]s in the
/// `users` insert order.
pub fn user_record_values(rec: &UserRecord) -> Vec<Value> {
    vec![
        Value::Text(rec.user.id.0.clone()),
        Value::Text(rec.user.display_name.clone()),
        Value::Text(rec.user.access_key_id.clone()),
        Value::Text(rec.bearer_secret_hash.clone()),
        opt_text(rec.user.sigv4_access_key_id.clone()),
        opt_blob(rec.sigv4_secret_ciphertext.clone()),
        opt_blob(rec.sigv4_secret_nonce.clone()),
        Value::Text(role_str(rec.user.role).to_owned()),
        Value::Int(i64::from(rec.user.is_active)),
        Value::Int(rec.user.created_at.0),
        Value::Int(rec.user.updated_at.0),
    ]
}

/// `Some(text)` -> text value, `None` -> NULL.
pub fn opt_text(s: Option<String>) -> Value {
    s.map_or(Value::Null, Value::Text)
}

/// `Some(bytes)` -> blob value, `None` -> NULL.
pub fn opt_blob(b: Option<Vec<u8>>) -> Value {
    b.map_or(Value::Null, Value::Blob)
}

// These two are only reached if the database somehow holds an invalid name/key, which our own
// writes never produce; we keep listing/reads infallible rather than poisoning a page.
fn unreachable_bucket() -> BucketName {
    BucketName::parse("invalid-bucket").expect("placeholder is valid")
}
fn unreachable_key() -> ObjectKey {
    ObjectKey::parse("invalid-key").expect("placeholder is valid")
}
