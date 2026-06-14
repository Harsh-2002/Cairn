//! Conversions between SQL rows and domain types, and the enum<->text mappings. Complex
//! fields (compression descriptor, user metadata, ACL, checksums) are stored as JSON.

use cairn_types::auth::Role;
use cairn_types::authz::{Acl, OwnershipMode};
use cairn_types::bucket::{Bucket, CompressionPolicy, VersioningState};
use cairn_types::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use cairn_types::meta::{
    ActivityEntry, MultipartSession, MultipartStatus, ObjectSummary, OutboxEntry, PartRecord,
    ReplicationOp, ReplicationStatus, ShareDisposition, ShareRow, User, UserRecord,
    UserSigV4Credentials, UserWithBearerHash,
};
use cairn_types::object::{
    ChecksumValue, CompressionDescriptor, ETag, ObjectVersionRow, StorageClass, UserMetadata,
};
use cairn_types::time::Timestamp;
use rusqlite::Row;
use rusqlite::types::Type;

/// Map a rusqlite error to a domain metadata error.
pub fn engine_err(e: rusqlite::Error) -> cairn_types::MetaError {
    // Surface uniqueness/constraint violations as the typed conflict so callers can map them.
    if let rusqlite::Error::SqliteFailure(f, _) = &e {
        if f.code == rusqlite::ErrorCode::ConstraintViolation {
            return cairn_types::MetaError::Conflict;
        }
    }
    cairn_types::MetaError::Engine(e.to_string())
}

fn json_col<T: serde::de::DeserializeOwned>(s: &str) -> rusqlite::Result<T> {
    serde_json::from_str(s)
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(e)))
}

/// Serialize a value to a JSON column string.
pub fn to_json<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).expect("domain types serialize cleanly")
}

// --- enum <-> text ---

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

pub fn share_from_row(row: &Row) -> rusqlite::Result<ShareRow> {
    Ok(ShareRow {
        token: row.get("token")?,
        bucket: BucketName::parse(&row.get::<_, String>("bucket_name")?)
            .unwrap_or_else(|_| unreachable_bucket()),
        key: ObjectKey::parse(&row.get::<_, String>("key")?).unwrap_or_else(|_| unreachable_key()),
        version_id: row
            .get::<_, Option<String>>("version_id")?
            .map(VersionId::from_string),
        expires_at: row.get::<_, Option<i64>>("expires_at")?.map(Timestamp),
        disposition: disposition_from(&row.get::<_, String>("disposition")?),
        filename: row.get("filename")?,
        created_by: UserId(row.get("created_by")?),
        created_at: Timestamp(row.get("created_at")?),
        revoked_at: row.get::<_, Option<i64>>("revoked_at")?.map(Timestamp),
    })
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

// --- row -> domain ---

pub fn object_version_from_row(row: &Row) -> rusqlite::Result<ObjectVersionRow> {
    let compression: CompressionDescriptor = json_col(&row.get::<_, String>("compression")?)?;
    let user_metadata: UserMetadata = json_col(&row.get::<_, String>("user_metadata")?)?;
    let acl: Option<Acl> = match row.get::<_, Option<String>>("acl")? {
        Some(s) => Some(json_col(&s)?),
        None => None,
    };
    let checksums: Vec<ChecksumValue> = json_col(&row.get::<_, String>("checksums")?)?;
    Ok(ObjectVersionRow {
        id: row.get("id")?,
        bucket: BucketName::parse(&row.get::<_, String>("bucket_name")?)
            .unwrap_or_else(|_| unreachable_bucket()),
        key: ObjectKey::parse(&row.get::<_, String>("key")?).unwrap_or_else(|_| unreachable_key()),
        version_id: VersionId::from_string(row.get("version_id")?),
        is_latest: row.get::<_, i64>("is_latest")? != 0,
        is_delete_marker: row.get::<_, i64>("is_delete_marker")? != 0,
        size_logical: row.get::<_, i64>("size_logical")? as u64,
        size_physical: row.get::<_, i64>("size_physical")? as u64,
        etag: ETag::from_string(row.get("etag")?),
        content_type: row.get("content_type")?,
        content_encoding: row.get::<_, Option<String>>("content_encoding")?,
        cache_control: row.get::<_, Option<String>>("cache_control")?,
        content_disposition: row.get::<_, Option<String>>("content_disposition")?,
        content_language: row.get::<_, Option<String>>("content_language")?,
        expires: row.get::<_, Option<String>>("expires")?,
        storage_path: row
            .get::<_, Option<String>>("storage_path")?
            .map(StoragePath::from_string),
        compression,
        storage_class: storage_class_from(&row.get::<_, String>("storage_class")?),
        cold_locator: row.get("cold_locator")?,
        owner_id: UserId(row.get("owner_id")?),
        user_metadata,
        acl,
        checksums,
        sse_descriptor: row.get::<_, Option<String>>("sse_descriptor")?,
        replication_status: row
            .get::<_, Option<String>>("replication_status")?
            .map(|s| repl_status_from(&s)),
        created_at: Timestamp(row.get("created_at")?),
        updated_at: Timestamp(row.get("updated_at")?),
    })
}

pub fn object_summary_from_row(row: &Row) -> rusqlite::Result<ObjectSummary> {
    Ok(ObjectSummary {
        key: ObjectKey::parse(&row.get::<_, String>("key")?).unwrap_or_else(|_| unreachable_key()),
        version_id: VersionId::from_string(row.get("version_id")?),
        is_latest: row.get::<_, i64>("is_latest")? != 0,
        is_delete_marker: row.get::<_, i64>("is_delete_marker")? != 0,
        etag: ETag::from_string(row.get("etag")?),
        size: row.get::<_, i64>("size_logical")? as u64,
        last_modified: Timestamp(row.get("updated_at")?),
        storage_class: storage_class_from(&row.get::<_, String>("storage_class")?),
        owner_id: UserId(row.get("owner_id")?),
    })
}

pub fn bucket_from_row(row: &Row) -> rusqlite::Result<Bucket> {
    // The column is `compression_policy` per ARCH §34.1; the domain field stays `compression`.
    let compression: Option<CompressionPolicy> =
        match row.get::<_, Option<String>>("compression_policy")? {
            Some(s) => Some(json_col(&s)?),
            None => None,
        };
    Ok(Bucket {
        name: BucketName::parse(&row.get::<_, String>("name")?)
            .unwrap_or_else(|_| unreachable_bucket()),
        owner_id: UserId(row.get("owner_id")?),
        created_at: Timestamp(row.get("created_at")?),
        versioning: versioning_from(&row.get::<_, String>("versioning_state")?),
        ownership_mode: ownership_from(&row.get::<_, String>("ownership_mode")?),
        region: row.get("region")?,
        compression,
    })
}

pub fn multipart_from_row(row: &Row) -> rusqlite::Result<MultipartSession> {
    let intended_acl: Option<Acl> = match row.get::<_, Option<String>>("intended_acl")? {
        Some(s) => Some(json_col(&s)?),
        None => None,
    };
    let user_metadata: UserMetadata = json_col(&row.get::<_, String>("user_metadata")?)?;
    Ok(MultipartSession {
        upload_id: UploadId::from_string(row.get("id")?),
        bucket: BucketName::parse(&row.get::<_, String>("bucket_name")?)
            .unwrap_or_else(|_| unreachable_bucket()),
        key: ObjectKey::parse(&row.get::<_, String>("key")?).unwrap_or_else(|_| unreachable_key()),
        content_type: row.get("content_type")?,
        status: mp_status_from(&row.get::<_, String>("status")?),
        owner_id: UserId(row.get("owner_id")?),
        intended_acl,
        user_metadata,
        created_at: Timestamp(row.get("created_at")?),
        updated_at: Timestamp(row.get("updated_at")?),
    })
}

pub fn part_from_row(row: &Row) -> rusqlite::Result<PartRecord> {
    let checksum: Option<ChecksumValue> = match row.get::<_, Option<String>>("checksum")? {
        Some(s) => Some(json_col(&s)?),
        None => None,
    };
    Ok(PartRecord {
        part_number: row.get::<_, i64>("part_number")? as u16,
        size: row.get::<_, i64>("size")? as u64,
        etag: row.get("etag")?,
        storage_path: StoragePath::from_string(row.get("storage_path")?),
        checksum,
    })
}

pub fn user_from_row(row: &Row) -> rusqlite::Result<User> {
    Ok(User {
        id: UserId(row.get("id")?),
        display_name: row.get("display_name")?,
        access_key_id: row.get("access_key_id")?,
        sigv4_access_key_id: row.get("sigv4_access_key_id")?,
        role: role_from(&row.get::<_, String>("role")?),
        is_active: row.get::<_, i64>("is_active")? != 0,
        quota_bytes: row.get::<_, Option<i64>>("quota_bytes")?.map(|q| q as u64),
        created_at: Timestamp(row.get("created_at")?),
        updated_at: Timestamp(row.get("updated_at")?),
    })
}

pub fn user_with_bearer_from_row(row: &Row) -> rusqlite::Result<UserWithBearerHash> {
    Ok(UserWithBearerHash {
        user: user_from_row(row)?,
        secret_hash: row.get("secret_hash")?,
    })
}

pub fn user_sigv4_from_row(row: &Row) -> rusqlite::Result<Option<UserSigV4Credentials>> {
    let ct: Option<Vec<u8>> = row.get("sigv4_secret_ciphertext")?;
    let nonce: Option<Vec<u8>> = row.get("sigv4_secret_nonce")?;
    match (ct, nonce) {
        (Some(secret_ciphertext), Some(secret_nonce)) => Ok(Some(UserSigV4Credentials {
            user: user_from_row(row)?,
            secret_ciphertext,
            secret_nonce,
        })),
        _ => Ok(None),
    }
}

pub fn outbox_from_row(row: &Row) -> rusqlite::Result<OutboxEntry> {
    Ok(OutboxEntry {
        id: row.get("id")?,
        bucket: BucketName::parse(&row.get::<_, String>("bucket_name")?)
            .unwrap_or_else(|_| unreachable_bucket()),
        key: ObjectKey::parse(&row.get::<_, String>("key")?).unwrap_or_else(|_| unreachable_key()),
        version_id: VersionId::from_string(row.get("version_id")?),
        operation: repl_op_from(&row.get::<_, String>("operation")?),
        rule_id: row.get("rule_id")?,
        target_arn: row.get("target_arn")?,
        attempts: row.get::<_, i64>("attempts")? as u32,
        next_attempt_at: Timestamp(row.get("next_attempt_at")?),
        status: repl_status_from(&row.get::<_, String>("status")?),
        last_error: row.get("last_error")?,
        priority: row.get::<_, i64>("priority")?,
        lease_until: row.get::<_, Option<i64>>("lease_until")?.map(Timestamp),
    })
}

pub fn activity_from_row(row: &Row) -> rusqlite::Result<ActivityEntry> {
    Ok(ActivityEntry {
        id: row.get("id")?,
        action: row.get("action")?,
        bucket: row.get("bucket")?,
        key: row.get("key")?,
        size: row.get::<_, Option<i64>>("size")?.map(|s| s as u64),
        etag: row.get("etag")?,
        actor: row.get("actor")?,
        at: Timestamp(row.get("at")?),
    })
}

/// Build the full credential record for inserts/updates.
pub fn user_record_columns(rec: &UserRecord) -> UserColumns<'_> {
    UserColumns {
        id: rec.user.id.0.as_str(),
        display_name: rec.user.display_name.as_str(),
        access_key_id: rec.user.access_key_id.as_str(),
        secret_hash: rec.bearer_secret_hash.as_str(),
        sigv4_access_key_id: rec.user.sigv4_access_key_id.as_deref(),
        sigv4_secret_ciphertext: rec.sigv4_secret_ciphertext.as_deref(),
        sigv4_secret_nonce: rec.sigv4_secret_nonce.as_deref(),
        role: role_str(rec.user.role),
        is_active: i64::from(rec.user.is_active),
        created_at: rec.user.created_at.0,
        updated_at: rec.user.updated_at.0,
    }
}

/// Borrowed column values for a user upsert.
pub struct UserColumns<'a> {
    pub id: &'a str,
    pub display_name: &'a str,
    pub access_key_id: &'a str,
    pub secret_hash: &'a str,
    pub sigv4_access_key_id: Option<&'a str>,
    pub sigv4_secret_ciphertext: Option<&'a [u8]>,
    pub sigv4_secret_nonce: Option<&'a [u8]>,
    pub role: &'a str,
    pub is_active: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

// These two are only reached if the database somehow holds an invalid name/key, which our
// own writes never produce; we keep listing/reads infallible rather than poisoning a page.
fn unreachable_bucket() -> BucketName {
    BucketName::parse("invalid-bucket").expect("placeholder is valid")
}
fn unreachable_key() -> ObjectKey {
    ObjectKey::parse("invalid-key").expect("placeholder is valid")
}
