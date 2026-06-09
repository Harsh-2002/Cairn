//! The JSON request/response shapes of the management API contract (ARCH §22). These are the
//! stable wire DTOs; the domain types in `cairn-types` are translated into and out of them
//! here so that the contract never drifts with internal representation changes.

use cairn_types::auth::Role;
use cairn_types::authz::OwnershipMode;
use cairn_types::bucket::VersioningState;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------------------
// Enum wire encodings (the contract fixes these lowercase strings explicitly)
// ---------------------------------------------------------------------------------------

/// The contract string for a bucket's versioning state.
#[must_use]
pub fn versioning_str(v: VersioningState) -> &'static str {
    match v {
        VersioningState::Unversioned => "unversioned",
        VersioningState::Enabled => "enabled",
        VersioningState::Suspended => "suspended",
    }
}

/// The contract string for a bucket's ownership mode.
#[must_use]
pub fn ownership_str(m: OwnershipMode) -> &'static str {
    match m {
        OwnershipMode::BucketOwnerEnforced => "bucket-owner-enforced",
        OwnershipMode::BucketOwnerPreferred => "bucket-owner-preferred",
        OwnershipMode::ObjectWriter => "object-writer",
    }
}

/// The contract string for a user's role.
#[must_use]
pub fn role_str(r: Role) -> &'static str {
    match r {
        Role::Administrator => "administrator",
        Role::Member => "member",
    }
}

/// Parse the contract role string back into a [`Role`].
#[must_use]
pub fn parse_role(s: &str) -> Option<Role> {
    match s {
        "administrator" => Some(Role::Administrator),
        "member" => Some(Role::Member),
        _ => None,
    }
}

/// Parse the versioning-state string used by `PUT /buckets/{name}/versioning` into a
/// [`VersioningState`]. The body uses the S3 capitalized spelling (`"Enabled"`, `"Suspended"`,
/// `"Unversioned"`).
#[must_use]
pub fn parse_versioning(s: &str) -> Option<VersioningState> {
    match s {
        "Enabled" => Some(VersioningState::Enabled),
        "Suspended" => Some(VersioningState::Suspended),
        "Unversioned" => Some(VersioningState::Unversioned),
        _ => None,
    }
}

// ---------------------------------------------------------------------------------------
// Health & overview
// ---------------------------------------------------------------------------------------

/// `GET /health` response.
#[derive(Debug, Serialize)]
pub struct HealthResp {
    /// Liveness marker (always `"ok"`).
    pub status: &'static str,
    /// Whether the store is ready to serve.
    pub ready: bool,
}

/// `GET /overview` response.
#[derive(Debug, Serialize)]
pub struct OverviewResp {
    /// Number of buckets.
    pub buckets: u64,
    /// Number of current objects.
    pub objects: u64,
    /// Number of object versions.
    pub versions: u64,
    /// Total logical bytes.
    pub logical_bytes: u64,
    /// Total physical bytes.
    pub physical_bytes: u64,
    /// Logical/physical compression ratio (`1.0` when nothing is stored).
    pub compression_ratio: f64,
}

/// `GET /system` response: node identity for the console's node card.
#[derive(Debug, Serialize)]
pub struct SystemResp {
    /// The server version (workspace `CARGO_PKG_VERSION`).
    pub version: String,
    /// Seconds since this process started.
    pub uptime_secs: u64,
    /// The S3 API listener address as configured.
    pub s3_addr: String,
    /// The web-UI listener address as configured (may be `off`).
    pub ui_addr: String,
    /// Whether TLS is enabled on the S3 listener.
    pub tls: bool,
    /// The data directory path.
    pub data_dir: String,
    /// Total bytes of the filesystem holding the data directory (`null` when unavailable).
    pub disk_total_bytes: Option<u64>,
    /// Bytes available to unprivileged writers on that filesystem (`null` when unavailable).
    pub disk_free_bytes: Option<u64>,
}

/// One entry in the `GET /overview/buckets` breakdown.
#[derive(Debug, Serialize)]
pub struct BucketUsageEntry {
    /// The bucket name.
    pub name: String,
    /// Number of current objects.
    pub objects: u64,
    /// Total logical bytes across all versions.
    pub logical_bytes: u64,
    /// Total physical bytes across all versions.
    pub physical_bytes: u64,
}

/// `GET /overview/buckets` response. Entries sum to the `GET /overview` totals.
#[derive(Debug, Serialize)]
pub struct OverviewBucketsResp {
    /// Per-bucket usage, sorted by name; empty buckets appear with zeros.
    pub buckets: Vec<BucketUsageEntry>,
}

// ---------------------------------------------------------------------------------------
// Buckets
// ---------------------------------------------------------------------------------------

/// One entry in the `GET /buckets` list.
#[derive(Debug, Serialize)]
pub struct BucketListEntry {
    /// The bucket name.
    pub name: String,
    /// The owning user id.
    pub owner_id: String,
    /// Creation time in epoch milliseconds.
    pub created_at_ms: i64,
    /// The versioning state.
    pub versioning: &'static str,
}

/// `GET /buckets` response.
#[derive(Debug, Serialize)]
pub struct BucketListResp {
    /// The buckets.
    pub buckets: Vec<BucketListEntry>,
}

/// `POST /buckets` request body.
#[derive(Debug, Deserialize)]
pub struct CreateBucketReq {
    /// The desired bucket name.
    pub name: String,
}

/// `POST /buckets` response body.
#[derive(Debug, Serialize)]
pub struct CreateBucketResp {
    /// The created bucket name.
    pub name: String,
}

/// `GET /buckets/{name}` response.
#[derive(Debug, Serialize)]
pub struct BucketDetailResp {
    /// The bucket name.
    pub name: String,
    /// The versioning state.
    pub versioning: &'static str,
    /// The ownership mode.
    pub ownership_mode: &'static str,
    /// The region label.
    pub region: String,
    /// The count of current objects (bounded per-bucket page count).
    pub object_count: u64,
    /// The logical bytes of current objects.
    pub logical_bytes: u64,
    /// The active compression algorithm (`"zstd"`/`"lz4"`), or `null` when compression is disabled.
    pub compression: Option<String>,
}

// ---------------------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------------------

/// One entry in the bucket object listing.
#[derive(Debug, Serialize)]
pub struct ObjectEntry {
    /// The object key.
    pub key: String,
    /// The logical size in bytes.
    pub size: u64,
    /// The unquoted ETag.
    pub etag: String,
    /// Last-modified time in epoch milliseconds.
    pub last_modified_ms: i64,
}

/// `GET /buckets/{name}/objects` response.
#[derive(Debug, Serialize)]
pub struct ObjectListResp {
    /// The objects in this page.
    pub objects: Vec<ObjectEntry>,
    /// The continuation cursor, or `null` if this is the last page.
    pub next: Option<String>,
}

// ---------------------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------------------

/// One entry in the `GET /users` list.
#[derive(Debug, Serialize)]
pub struct UserListEntry {
    /// The user id.
    pub id: String,
    /// The display name.
    pub display_name: String,
    /// The Bearer access-key id.
    pub access_key_id: String,
    /// The role.
    pub role: &'static str,
    /// Whether the user is active.
    pub is_active: bool,
}

/// `GET /users` response.
#[derive(Debug, Serialize)]
pub struct UserListResp {
    /// The users.
    pub users: Vec<UserListEntry>,
}

/// `POST /users` request body.
#[derive(Debug, Deserialize)]
pub struct CreateUserReq {
    /// The display name.
    pub display_name: String,
    /// The role (`"administrator"` or `"member"`).
    pub role: String,
}

/// `POST /users` response body. The secrets are shown exactly once.
#[derive(Debug, Serialize)]
pub struct CreateUserResp {
    /// The new user id.
    pub id: String,
    /// The Bearer access-key id.
    pub bearer_access_key_id: String,
    /// The Bearer secret (shown once; only its hash is retained server-side).
    pub bearer_secret: String,
    /// The SigV4 access-key id — the "S3 access key" a standard S3 client (boto3, aws-cli) uses.
    pub s3_access_key_id: String,
    /// The SigV4 secret — the "S3 secret key", shown exactly once (sealed at rest server-side).
    pub s3_secret_key: String,
}

/// `GET /users/{id}` response: the public user view plus its attached identity policy.
#[derive(Debug, Serialize)]
pub struct UserDetailResp {
    /// The user id.
    pub id: String,
    /// The display name.
    pub display_name: String,
    /// The Bearer access-key id.
    pub access_key_id: String,
    /// The SigV4 access-key id used by S3 clients (public; the secret is never returned after
    /// creation).
    pub sigv4_access_key_id: Option<String>,
    /// The role.
    pub role: &'static str,
    /// Whether the user is active.
    pub is_active: bool,
    /// The attached identity (per-user) policy document, or null if none.
    pub policy: Option<Value>,
}

/// `GET /users/{id}/policy` response.
#[derive(Debug, Serialize)]
pub struct UserPolicyResp {
    /// The attached identity (per-user) policy document, or null if none.
    pub policy: Option<Value>,
}

// ---------------------------------------------------------------------------------------
// Activity
// ---------------------------------------------------------------------------------------

/// One entry in the `GET /activity` log.
#[derive(Debug, Serialize)]
pub struct ActivityListEntry {
    /// The action performed.
    pub action: String,
    /// The bucket, if applicable.
    pub bucket: Option<String>,
    /// The key, if applicable.
    pub key: Option<String>,
    /// When it happened, in epoch milliseconds.
    pub at_ms: i64,
}

/// `GET /activity` response.
#[derive(Debug, Serialize)]
pub struct ActivityListResp {
    /// The entries, most recent first.
    pub entries: Vec<ActivityListEntry>,
}

// ---------------------------------------------------------------------------------------
// Bucket configuration
// ---------------------------------------------------------------------------------------

/// `GET /buckets/{name}/config` response. Each aspect is the parsed JSON document the store
/// holds for that aspect, or `null` when the aspect is unset. `quota_bytes` is the configured
/// per-bucket byte quota (`null` when the bucket has no quota), read via `get_bucket_quota`.
#[derive(Debug, Serialize)]
pub struct BucketConfigResp {
    /// The versioning state.
    pub versioning: &'static str,
    /// The ownership mode.
    pub ownership_mode: &'static str,
    /// The configured per-bucket byte quota, or `null` when the bucket has no quota set.
    pub quota_bytes: Option<u64>,
    /// The bucket policy document, or `null`.
    pub policy: Option<Value>,
    /// The CORS document, or `null`.
    pub cors: Option<Value>,
    /// The tag-set document, or `null`.
    pub tagging: Option<Value>,
    /// The lifecycle document, or `null`.
    pub lifecycle: Option<Value>,
    /// The ACL document, or `null`.
    pub acl: Option<Value>,
    /// The bucket-level public-access-block document, or `null`.
    pub public_access_block: Option<Value>,
}

/// `PUT /buckets/{name}/versioning` request body.
#[derive(Debug, Deserialize)]
pub struct SetVersioningReq {
    /// The desired state (`"Enabled"`, `"Suspended"`, or `"Unversioned"`).
    pub status: String,
}

/// `PUT /buckets/{name}/quota` request body.
#[derive(Debug, Deserialize)]
pub struct SetQuotaReq {
    /// The new byte quota, or `null` to remove the limit.
    pub quota_bytes: Option<u64>,
}

// ---------------------------------------------------------------------------------------
// User management
// ---------------------------------------------------------------------------------------

/// `PATCH /users/{id}` request body. Absent fields are left unchanged.
#[derive(Debug, Deserialize)]
pub struct PatchUserReq {
    /// Activate or deactivate the user.
    #[serde(default)]
    pub is_active: Option<bool>,
    /// Change the user's role (`"administrator"` or `"member"`).
    #[serde(default)]
    pub role: Option<String>,
}

/// `PATCH /users/{id}` response body (the updated public user view).
#[derive(Debug, Serialize)]
pub struct PatchUserResp {
    /// The user id.
    pub id: String,
    /// The display name.
    pub display_name: String,
    /// The Bearer access-key id.
    pub access_key_id: String,
    /// The role.
    pub role: &'static str,
    /// Whether the user is active.
    pub is_active: bool,
}

/// `POST /users/{id}/rotate-credentials` response body. The fresh Bearer secret is shown
/// exactly once; only its hash is retained server-side.
#[derive(Debug, Serialize)]
pub struct RotateCredentialsResp {
    /// The Bearer access-key id (unchanged by rotation).
    pub bearer_access_key_id: String,
    /// The freshly minted Bearer secret (shown once).
    pub bearer_secret: String,
}

// ---------------------------------------------------------------------------------------
// Replication operations
// ---------------------------------------------------------------------------------------

/// One entry in the failed-replication listing.
#[derive(Debug, Serialize)]
pub struct FailedReplicationEntry {
    /// The bucket.
    pub bucket: String,
    /// The key.
    pub key: String,
    /// The version id concerned.
    pub version_id: String,
    /// The last error recorded, if any.
    pub error: Option<String>,
    /// The retry attempt count.
    pub attempts: u32,
    /// When the entry is next due, in epoch milliseconds.
    pub next_attempt_at_ms: i64,
}

/// `GET /replication/failed` response.
#[derive(Debug, Serialize)]
pub struct FailedReplicationResp {
    /// The failed/terminal outbox entries.
    pub entries: Vec<FailedReplicationEntry>,
}

// ---------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------

/// The JSON error envelope used by every non-success response.
#[derive(Debug, Serialize)]
pub struct ErrorResp {
    /// A short, stable error message.
    pub error: String,
}
