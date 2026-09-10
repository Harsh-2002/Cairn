//! Versioned laboratory records and transactionally maintained indexes.
use super::kv::{self, Overlay, View, get, key};
use cairn_types::authz::Acl;
use cairn_types::meta::MultipartReplicaIntent;
use cairn_types::storage::{StorageToken, StorageWritePlan};
use cairn_types::*;
use serde::{Deserialize, Serialize};

pub const META: u8 = 1;
pub const BUCKET: u8 = 2;
pub const STATS: u8 = 3;
pub const QUOTA: u8 = 4;
pub const PRINCIPAL: u8 = 5;
pub const CURRENT: u8 = 6;
pub const VERSION: u8 = 7;
pub const CURRENT_LIST: u8 = 8;
pub const VERSION_LIST: u8 = 9;
pub const REFERENCE: u8 = 10;
pub const SESSION: u8 = 11;
pub const SESSION_BUCKET: u8 = 12;
pub const PART: u8 = 13;
pub const RESERVATION: u8 = 14;
pub const RESERVATION_UPLOAD: u8 = 15;
pub const INTENT: u8 = 18;
pub const INTENT_PATH: u8 = 19;
pub const DEBT: u8 = 20;
pub const DEBT_PATH: u8 = 21;
pub const DEBT_READY: u8 = 22;
pub const QUOTA_DEBT: u8 = 24;
pub const OWNER_ALIAS: u8 = 25;
pub const OUTBOX: u8 = 26;
pub const OUTBOX_DUE: u8 = 27;
pub const OUTBOX_STATUS: u8 = 28;
pub const OUTBOX_BUCKET_KEY: u8 = 29;
pub const ROW_ID: u8 = 30;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Stats {
    pub objects: u64,
    pub versions: u64,
    pub logical: u64,
    pub physical: u64,
    pub active: u64,
    pub staged: u64,
    pub outbox: [u64; 4],
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Principal {
    pub logical: u64,
    pub active: u64,
    pub staged: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub row: ObjectVersionRow,
    pub internal_sha256: Option<String>,
}
impl From<ObjectVersionRow> for Row {
    fn from(row: ObjectVersionRow) -> Self {
        Self {
            internal_sha256: row.internal_sha256.clone(),
            row,
        }
    }
}
impl Row {
    pub fn into_row(mut self) -> ObjectVersionRow {
        self.row.internal_sha256 = self.internal_sha256;
        self.row
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Summary {
    pub row_id: String,
    pub summary: ObjectSummary,
}
impl Summary {
    pub fn from_row(row: &ObjectVersionRow) -> Self {
        Self {
            row_id: row.id.clone(),
            summary: ObjectSummary {
                row_id: row.id.clone(),
                key: row.key.clone(),
                version_id: row.version_id.clone(),
                is_latest: row.is_latest,
                is_delete_marker: row.is_delete_marker,
                etag: row.etag.clone(),
                size: row.size_logical,
                last_modified: row.updated_at,
                storage_class: row.storage_class,
                owner_id: row.owner_id.clone(),
            },
        }
    }
    pub fn into_summary(mut self) -> ObjectSummary {
        self.summary.row_id = self.row_id;
        self.summary
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Intent {
    pub plan: StorageWritePlan,
    pub cancelled: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Debt {
    pub id: StorageToken,
    pub bucket: BucketName,
    pub path: StoragePath,
    pub quota_id: Option<String>,
    pub quota_owner_path: Option<String>,
    pub claim: Option<Claim>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub token: StorageToken,
    pub generation: StorageToken,
    pub until: Timestamp,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaDebt {
    pub id: String,
    pub bucket: BucketName,
    pub principal: UserId,
    pub upload: UploadId,
    pub path: String,
    pub bytes: u64,
    pub links: u64,
    pub created_at: Timestamp,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reservation {
    pub attempt: String,
    pub upload: UploadId,
    pub part: u16,
    pub bytes: u64,
    pub bucket: BucketName,
    pub principal: UserId,
    pub created_at: Timestamp,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "MultipartSession")]
struct MultipartSessionCodec {
    pub upload_id: UploadId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub content_type: String,
    pub content_disposition: Option<String>,
    pub status: MultipartStatus,
    pub owner_id: UserId,
    pub initiated_by: UserId,
    pub intended_acl: Option<Acl>,
    pub replica_intent: Option<MultipartReplicaIntent>,
    pub user_metadata: UserMetadata,
    pub initial_tags: Vec<(String, String)>,
    #[serde(with = "LockIntentCodec")]
    pub lock_intent: ExplicitObjectLockIntent,
    pub sse_requested: bool,
    pub encrypt_parts: bool,
    pub sse_kms_requested: bool,
    pub sse_kms_key_id: Option<String>,
    pub sse_bucket_key_enabled: bool,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session(#[serde(with = "MultipartSessionCodec")] pub MultipartSession);

#[derive(Serialize, Deserialize)]
#[serde(remote = "PartRecord")]
struct PartRecordCodec {
    pub part_number: u16,
    pub size: u64,
    pub etag: String,
    pub storage_path: StoragePath,
    pub checksum: Option<ChecksumValue>,
    pub part_dek: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Part(#[serde(with = "PartRecordCodec")] pub PartRecord);

#[derive(Serialize, Deserialize)]
#[serde(remote = "OutboxEntry")]
struct OutboxEntryCodec {
    #[serde(with = "claim_codec")]
    pub claim_token: Option<cairn_types::id::ReplicationClaimToken>,
    pub id: String,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub operation: ReplicationOp,
    pub rule_id: String,
    pub target_arn: Option<String>,
    pub attempts: u32,
    pub next_attempt_at: Timestamp,
    pub status: ReplicationStatus,
    pub last_error: Option<String>,
    pub priority: i64,
    pub lease_until: Option<Timestamp>,
    pub enqueued_at: Timestamp,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Outbox(#[serde(with = "OutboxEntryCodec")] pub OutboxEntry);

pub fn version_key(bucket: &BucketName, object: &ObjectKey, version: &VersionId) -> Vec<u8> {
    key(
        VERSION,
        &[bucket.as_str(), object.as_str(), version.as_str()],
    )
}
pub fn version_index(bucket: &BucketName, object: &ObjectKey, version: &VersionId) -> Vec<u8> {
    let mut output = key(VERSION_LIST, &[bucket.as_str(), object.as_str()]);
    let mut suffix = Vec::new();
    kv::component(&mut suffix, version.as_str().as_bytes());
    output.extend(suffix.into_iter().map(|byte| !byte));
    output
}
pub fn current(
    view: &dyn View,
    bucket: &BucketName,
    object: &ObjectKey,
) -> Result<Option<ObjectVersionRow>, MetaError> {
    let id: Option<String> = get(view, &key(CURRENT, &[bucket.as_str(), object.as_str()]))?;
    match id {
        Some(id) => version(view, bucket, object, &VersionId::from_string(id)),
        None => Ok(None),
    }
}
pub fn version(
    view: &dyn View,
    bucket: &BucketName,
    object: &ObjectKey,
    id: &VersionId,
) -> Result<Option<ObjectVersionRow>, MetaError> {
    Ok(get::<Row>(view, &version_key(bucket, object, id))?.map(Row::into_row))
}
pub fn save_row(view: &mut Overlay<'_>, row: ObjectVersionRow) -> Result<(), MetaError> {
    view.put(
        version_index(&row.bucket, &row.key, &row.version_id),
        &Summary::from_row(&row),
    )?;
    view.put(
        version_key(&row.bucket, &row.key, &row.version_id),
        &Row::from(row),
    )
}
pub fn stats(view: &dyn View, bucket: &BucketName) -> Result<Stats, MetaError> {
    get(view, &key(STATS, &[bucket.as_str()]))?.ok_or(MetaError::Integrity)
}
pub fn principal(view: &dyn View, owner: &UserId) -> Result<Principal, MetaError> {
    Ok(get(view, &key(PRINCIPAL, &[&owner.0]))?.unwrap_or_default())
}
pub fn require_bucket(view: &dyn View, name: &BucketName) -> Result<Bucket, MetaError> {
    get(view, &key(BUCKET, &[name.as_str()]))?.ok_or(MetaError::Integrity)
}
pub fn change(value: u64, delta: i128) -> Result<u64, MetaError> {
    u64::try_from(i128::from(value) + delta).map_err(|_| MetaError::Integrity)
}
pub fn stage_delta(
    view: &mut Overlay<'_>,
    bucket: &BucketName,
    owner: &UserId,
    delta: i128,
) -> Result<(), MetaError> {
    let mut stats = stats(view, bucket)?;
    let mut principal = principal(view, owner)?;
    stats.staged = change(stats.staged, delta)?;
    principal.staged = change(principal.staged, delta)?;
    if delta > 0
        && let Some(quota) = get::<Option<u64>>(view, &key(QUOTA, &[bucket.as_str()]))?.flatten()
        && stats
            .logical
            .checked_add(stats.staged)
            .is_none_or(|total| total > quota)
    {
        return Err(MetaError::QuotaExceeded);
    }
    view.put(key(STATS, &[bucket.as_str()]), &stats)?;
    view.put(key(PRINCIPAL, &[&owner.0]), &principal)
}
pub fn status_index(status: ReplicationStatus) -> Result<usize, MetaError> {
    match status {
        ReplicationStatus::Pending => Ok(0),
        ReplicationStatus::Claimed => Ok(1),
        ReplicationStatus::Failed => Ok(2),
        ReplicationStatus::Completed => Ok(3),
        ReplicationStatus::Replica => Err(kv::error("replica is not a dispatch status")),
    }
}
pub fn time_key(table: u8, time: i64, id: &str) -> Vec<u8> {
    let mut output = vec![table];
    output.extend_from_slice(&((time as u64) ^ (1_u64 << 63)).to_be_bytes());
    kv::component(&mut output, id.as_bytes());
    output
}
pub fn claim_until(now: Timestamp, seconds: i64) -> Result<Timestamp, MetaError> {
    seconds
        .checked_mul(1000)
        .filter(|n| *n > 0)
        .and_then(|n| now.0.checked_add(n))
        .map(Timestamp)
        .ok_or_else(|| kv::error("invalid candidate lease"))
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ExplicitObjectLockIntent")]
struct LockIntentCodec {
    retention: Option<ObjectRetention>,
    legal_hold: Option<bool>,
}
mod claim_codec {
    use cairn_types::id::ReplicationClaimToken;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn serialize<S: Serializer>(
        value: &Option<ReplicationClaimToken>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(ReplicationClaimToken::as_str)
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<ReplicationClaimToken>, D::Error> {
        Ok(Option::<String>::deserialize(deserializer)?.map(ReplicationClaimToken::from_string))
    }
}
