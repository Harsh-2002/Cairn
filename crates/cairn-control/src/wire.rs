//! The JSON request/response shapes of the management API contract (ARCH §22). These are the
//! stable wire DTOs; the domain types in `cairn-types` are translated into and out of them
//! here so that the contract never drifts with internal representation changes.

use cairn_types::auth::Role;
use cairn_types::bucket::VersioningState;
use cairn_types::authz::OwnershipMode;
use serde::{Deserialize, Serialize};

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

/// `POST /users` response body. The Bearer secret is shown exactly once.
#[derive(Debug, Serialize)]
pub struct CreateUserResp {
    /// The new user id.
    pub id: String,
    /// The Bearer access-key id.
    pub bearer_access_key_id: String,
    /// The Bearer secret (shown once; only its hash is retained server-side).
    pub bearer_secret: String,
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
// Errors
// ---------------------------------------------------------------------------------------

/// The JSON error envelope used by every non-success response.
#[derive(Debug, Serialize)]
pub struct ErrorResp {
    /// A short, stable error message.
    pub error: String,
}
