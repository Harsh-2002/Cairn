//! The JSON request/response shapes of the management API contract (ARCH 22). These are the
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
    /// Whether the store is ready to serve. Reflects a real probe of the metadata store
    /// (`list_buckets` succeeds), not a hardcoded constant (ARCH 26.4).
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
// Request metrics
// ---------------------------------------------------------------------------------------

/// `GET /metrics/requests` response: the downsampled request timeline plus breakdowns by
/// operation, most-active bucket, and HTTP status class, with range-wide totals (bytes, errors,
/// latency average and p95, peak window, active buckets) and the timeline window.
#[derive(Debug, Serialize)]
pub struct RequestMetricsResp {
    /// The timeline downsampling window, in seconds (for the UI to derive req/s).
    pub window_secs: i64,
    /// Grand total requests in range.
    pub total: u64,
    /// Total error requests (4xx + 5xx) in range.
    pub total_errors: u64,
    /// Total received bytes in range.
    pub total_bytes_in: u64,
    /// Total sent bytes in range.
    pub total_bytes_out: u64,
    /// Range-wide average latency, milliseconds.
    pub latency_avg_ms: u64,
    /// Range-wide 95th-percentile latency, milliseconds.
    pub latency_p95_ms: u64,
    /// The busiest single window's request count (for a peak req/s stat).
    pub peak_window_count: u64,
    /// Number of distinct buckets that saw any traffic in range.
    pub active_buckets: u64,
    /// Requests over time, one point per downsampling window (ascending by `ts_ms`).
    pub timeline: Vec<MetricPoint>,
    /// Requests broken down by operation, descending by count.
    pub by_operation: Vec<MetricOp>,
    /// The most-active buckets, descending by count.
    pub top_buckets: Vec<MetricBucket>,
    /// The top buckets by bytes transferred (in + out), descending — a different ranking than
    /// `top_buckets`, so the console's "by data" panel is honest about what it shows.
    pub top_buckets_by_bytes: Vec<MetricBucket>,
    /// Requests broken down by HTTP status class.
    pub by_status: Vec<MetricStatus>,
}

/// One point on the requests-over-time timeline.
#[derive(Debug, Serialize)]
pub struct MetricPoint {
    /// Window start, epoch milliseconds.
    pub ts_ms: i64,
    /// Requests in the window.
    pub count: u64,
    /// Of which were errors (4xx + 5xx).
    pub errors: u64,
    /// Received bytes in the window.
    pub bytes_in: u64,
    /// Sent bytes in the window.
    pub bytes_out: u64,
    /// Average request latency in the window, milliseconds.
    pub latency_avg_ms: u64,
}

/// A request count attributed to one operation name.
#[derive(Debug, Serialize)]
pub struct MetricOp {
    /// The operation name.
    pub operation: String,
    /// Requests for this operation in range.
    pub count: u64,
    /// Total bytes (in + out) for this operation in range.
    pub bytes: u64,
    /// Average latency for this operation, milliseconds.
    pub latency_avg_ms: u64,
}

/// A request count attributed to one bucket.
#[derive(Debug, Serialize)]
pub struct MetricBucket {
    /// The bucket name.
    pub bucket: String,
    /// Requests against this bucket in range.
    pub count: u64,
    /// Total bytes (in + out) against this bucket in range.
    pub bytes: u64,
}

/// A request count attributed to one HTTP status class (`2xx`/`3xx`/`4xx`/`5xx`).
#[derive(Debug, Serialize)]
pub struct MetricStatus {
    /// The status class.
    pub status_class: String,
    /// Requests with this status class in range.
    pub count: u64,
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
    /// Enable Object Lock at creation (forces versioning Enabled; cannot be turned on later).
    #[serde(default)]
    pub object_lock: bool,
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
    /// Key groups folded at the requested `delimiter` (empty without one) — the "folders".
    pub common_prefixes: Vec<String>,
    /// The continuation cursor, or `null` if this is the last page.
    pub next: Option<String>,
}

/// `DELETE /buckets/{name}/objects?prefix=P` response: how many versions were permanently
/// deleted, any per-item failures, and whether more work remains (the page budget was exhausted
/// while items still matched the prefix, so the caller should re-invoke).
#[derive(Debug, Serialize)]
pub struct DeletePrefixResp {
    /// The number of versions (and delete markers) permanently deleted.
    pub deleted: u64,
    /// Per-item failures encountered; deletion continued past each one.
    pub errors: Vec<DeletePrefixError>,
    /// `true` when the page budget was exhausted with items still remaining (re-invoke to finish).
    pub more: bool,
}

/// One per-item failure in a prefix delete.
#[derive(Debug, Serialize)]
pub struct DeletePrefixError {
    /// The object key that failed to delete.
    pub key: String,
    /// The failure message.
    pub message: String,
}

// ---------------------------------------------------------------------------------------
// Object tag browsing (ARCH 17.2)
// ---------------------------------------------------------------------------------------

/// `GET /tags` response: the distinct object tags in use, descending by count.
#[derive(Debug, Serialize)]
pub struct TagSummaryResp {
    /// The distinct tags.
    pub tags: Vec<TagSummaryItem>,
}

/// One distinct object tag (`tag_key=tag_value`) with its current-object count.
#[derive(Debug, Serialize)]
pub struct TagSummaryItem {
    /// The tag key.
    pub tag_key: String,
    /// The tag value.
    pub tag_value: String,
    /// Number of current objects carrying this exact key=value.
    pub object_count: u64,
}

/// `GET /tags/objects` response: the current objects carrying a queried tag.
#[derive(Debug, Serialize)]
pub struct TagObjectsResp {
    /// The matching objects (bounded by the standard page limit).
    pub objects: Vec<TagObjectItem>,
}

/// One current object carrying a queried tag.
#[derive(Debug, Serialize)]
pub struct TagObjectItem {
    /// The bucket the object lives in.
    pub bucket: String,
    /// The object key.
    pub key: String,
    /// The current version id the tag is attached to.
    pub version_id: String,
    /// The object's logical size in bytes.
    pub size: u64,
    /// When the current version was last modified, in epoch milliseconds.
    pub last_modified_ms: i64,
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

/// `POST /credentials/temporary` request body: mint an STS-style temporary session credential.
#[derive(Debug, Deserialize)]
pub struct MintSessionReq {
    /// The credential lifetime in seconds (bounded 900..=43200 server-side).
    pub duration_secs: u64,
    /// The scoped inline policy document (a standard policy JSON object) — required. This is the
    /// session's entire effective permission set; it carries no implicit access.
    pub policy: serde_json::Value,
}

/// `POST /credentials/temporary` response: the temporary credential, shown exactly once.
#[derive(Debug, Serialize)]
pub struct MintSessionResp {
    /// The temporary access-key id (an `CAIRNTMP…` SigV4 key).
    pub access_key_id: String,
    /// The temporary secret access key (sealed at rest server-side; shown once).
    pub secret_access_key: String,
    /// The opaque session token the SDK presents as `X-Amz-Security-Token` (hashed at rest).
    pub session_token: String,
    /// When the credential expires, in epoch seconds.
    pub expiration_epoch_secs: i64,
}

/// `GET /credentials/temporary` response: the active (non-expired) session credentials.
#[derive(Debug, Serialize)]
pub struct ListSessionsResp {
    /// Active session credentials, newest first. No secret/token material.
    pub sessions: Vec<SessionView>,
}

/// One active session credential in the list (public summary; never any secret).
#[derive(Debug, Serialize)]
pub struct SessionView {
    /// The temporary access-key id (the public identifier; also the revoke key).
    pub access_key_id: String,
    /// The parent user id this session derives from.
    pub parent_user_id: String,
    /// Whether an inline policy scopes this session below the parent.
    pub scoped: bool,
    /// When it was minted, epoch milliseconds.
    pub created_at_ms: i64,
    /// When it expires, epoch milliseconds.
    pub expires_at_ms: i64,
}

/// `POST /users` request body.
#[derive(Debug, Deserialize)]
pub struct CreateUserReq {
    /// The display name.
    pub display_name: String,
    /// The role (`"administrator"` or `"member"`).
    pub role: String,
    /// When set, attach a canned **replication** identity policy to the new user scoped to this
    /// destination bucket: it grants `s3:ReplicateObject`, `s3:ReplicateDelete`, `s3:GetObject`,
    /// and `s3:PutObject` on `arn:aws:s3:::<bucket>/*`. This mints a dedicated destination
    /// credential in one step (ARCH 20.5). Absent/null leaves the user with no attached policy.
    #[serde(default)]
    pub replication_policy_bucket: Option<String>,
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
    /// The per-user byte quota (ARCH 27.5), or null when unset (no limit).
    pub quota_bytes: Option<u64>,
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
    /// The actor (the admin access-key id that performed it), if recorded.
    pub actor: Option<String>,
    /// When it happened, in epoch milliseconds.
    pub at_ms: i64,
}

/// `GET /activity` response.
#[derive(Debug, Serialize)]
pub struct ActivityListResp {
    /// The entries, most recent first.
    pub entries: Vec<ActivityListEntry>,
}

/// A persistent object-share, as returned by the management API (ARCH 15.8).
#[derive(Debug, Serialize)]
pub struct ShareRecord {
    /// The opaque token; also the `/p/{token}` path tail.
    pub token: String,
    /// The shared object's bucket.
    pub bucket: String,
    /// The shared object's key.
    pub key: String,
    /// A pinned version id, or null to follow the current version.
    pub version_id: Option<String>,
    /// Expiry in epoch ms, or null for a forever share.
    pub expires_at_ms: Option<i64>,
    /// When the share was minted, in epoch ms.
    pub created_at_ms: i64,
    /// The user id that minted it.
    pub created_by: String,
    /// `inline` or `attachment`.
    pub disposition: String,
    /// The download filename for `attachment`, or null.
    pub filename: Option<String>,
    /// Server-derived: `active`, `expired`, or `revoked`.
    pub status: String,
}

/// `GET /buckets/{b}/objects/shares` response.
#[derive(Debug, Serialize)]
pub struct ShareListResp {
    /// The shares, most recent first.
    pub shares: Vec<ShareRecord>,
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
    /// The default server-side-encryption document (`{"algorithm":"AES256"}`), or `null` when
    /// new uploads are stored unencrypted by default.
    pub encryption: Option<Value>,
}

/// `PUT /buckets/{name}/encryption` request body.
#[derive(Debug, Deserialize)]
pub struct SetEncryptionReq {
    /// `"AES256"` to encrypt new uploads by default (SSE-S3), `"none"` to turn the default off.
    pub algorithm: String,
    /// When true, the bucket *mandates* encryption: a client PUT whose resolved encryption is "none"
    /// is refused (an inbound replica is transparently encrypted instead). Defaults to false. Pairing
    /// it with `algorithm: "AES256"` makes header-less client uploads encrypt by default *and*
    /// guarantees no plaintext object can be stored.
    #[serde(default)]
    pub required: bool,
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

/// `PUT /users/{id}/quota` request body. The quota is enforced inside the writer's commit
/// transaction (ARCH 27.5); this endpoint only sets the configured value.
#[derive(Debug, Deserialize)]
pub struct SetUserQuotaReq {
    /// The new byte quota, or `null` to remove the limit.
    pub quota_bytes: Option<u64>,
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

/// `POST /buckets/{name}/replication/targets` request body. The `secret` is sealed under the
/// master key before it is stored and is **never** echoed back in any response.
#[derive(Debug, Deserialize)]
pub struct CreateReplicationTargetReq {
    /// The destination endpoint base URL, e.g. `https://s3.peer.example.com:9000`.
    pub endpoint: String,
    /// The SigV4 signing region for the destination.
    pub region: String,
    /// The destination bucket replicated into.
    pub dest_bucket: String,
    /// The destination access-key id (public; stored in the clear).
    pub access_key: String,
    /// The destination secret access key (plaintext; sealed at rest, never returned).
    pub secret: String,
    /// Optional CA certificate (PEM) to trust for an `https://` endpoint signed by a private or
    /// self-signed CA, instead of the public root set. Public material, stored in the clear.
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Skip TLS certificate verification for the destination — testing only (a self-signed
    /// endpoint). Mutually exclusive with `ca_cert`.
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

/// `POST /buckets/{name}/replication/targets` response body: just the minted ARN. The secret is
/// never returned.
#[derive(Debug, Serialize)]
pub struct CreateReplicationTargetResp {
    /// The stable target ARN that replication rules reference.
    pub arn: String,
}

/// One entry in the replication-target listing. Deliberately omits the sealed secret material:
/// only the public connection parameters and access-key id are surfaced.
#[derive(Debug, Serialize)]
pub struct ReplicationTargetEntry {
    /// The stable target ARN.
    pub arn: String,
    /// The destination endpoint base URL.
    pub endpoint: String,
    /// The SigV4 signing region.
    pub region: String,
    /// The destination bucket.
    pub dest_bucket: String,
    /// The destination access-key id (the secret is never returned).
    pub access_key_id: String,
    /// Whether TLS certificate verification is skipped for this destination (testing only).
    pub insecure_skip_verify: bool,
    /// Whether a custom CA certificate is trusted for this destination's TLS.
    pub has_ca_cert: bool,
}

/// `GET /buckets/{name}/replication/targets` response. Secrets are never included.
#[derive(Debug, Serialize)]
pub struct ReplicationTargetListResp {
    /// The configured targets, without any secret material.
    pub targets: Vec<ReplicationTargetEntry>,
}

/// One webhook endpoint as surfaced by the management API: the HMAC secret is reduced to a presence
/// flag (`has_secret`) so it is never echoed back.
#[derive(Debug, Serialize)]
pub struct WebhookEndpointView {
    /// The endpoint id.
    pub id: String,
    /// The destination URL.
    pub url: String,
    /// The subscribed event selectors.
    pub events: Vec<String>,
    /// The object-key prefix filter, if any.
    pub prefix: Option<String>,
    /// The object-key suffix filter, if any.
    pub suffix: Option<String>,
    /// Whether an HMAC signing secret is configured (the value is never returned).
    pub has_secret: bool,
}

impl From<cairn_types::WebhookEndpoint> for WebhookEndpointView {
    fn from(e: cairn_types::WebhookEndpoint) -> Self {
        Self {
            id: e.id,
            url: e.url,
            events: e.events,
            prefix: e.prefix,
            suffix: e.suffix,
            has_secret: e.secret.is_some(),
        }
    }
}

/// `GET /buckets/{name}/notifications` response: the webhook endpoint list (without secrets).
#[derive(Debug, Serialize)]
pub struct NotificationsResp {
    /// The configured webhook endpoints.
    pub endpoints: Vec<WebhookEndpointView>,
}

/// `POST /buckets/{name}/replication/retry` response: an acknowledgement carrying the count of
/// failed entries observed for the bucket just before the requeue was submitted.
#[derive(Debug, Serialize)]
pub struct ReplicationRetryResp {
    /// Always `true` once the requeue mutation is accepted.
    pub requeued: bool,
    /// How many failed entries for this bucket were observed prior to the requeue.
    pub failed_observed: u64,
}

/// `POST /buckets/{name}/replication/resync` response: the backfill has been accepted and runs as a
/// background task (HTTP 202). Progress is observable via the replication-status endpoint/metrics.
#[derive(Debug, Serialize)]
pub struct ReplicationResyncResp {
    /// Always `true` once the backfill task is spawned.
    pub started: bool,
}

/// `GET /buckets/{name}/replication/status` response: per-bucket replication counters plus the
/// most recent failed entries' errors. All figures are bounded by the standard page limit.
#[derive(Debug, Serialize)]
pub struct ReplicationStatusResp {
    /// The bucket the status pertains to.
    pub bucket: String,
    /// Count of entries currently due (pending and claimable) for this bucket, bounded.
    pub pending: u64,
    /// Count of terminally failed entries for this bucket (exact, not page-bounded).
    pub failed: u64,
    /// Age of the oldest still-pending enqueue for this bucket, in seconds (true lag, 0 when idle).
    pub lag_seconds: u64,
    /// Per-target pending/failed breakdown for this bucket.
    pub by_target: Vec<ReplicationTargetCount>,
    /// The most recent failed entries' errors for this bucket (bounded), newest first.
    pub recent_errors: Vec<ReplicationStatusError>,
}

/// One recent failed-replication error in the per-bucket status view.
#[derive(Debug, Serialize)]
pub struct ReplicationStatusError {
    /// The object key concerned.
    pub key: String,
    /// The version id concerned.
    pub version_id: String,
    /// The last error recorded, if any.
    pub error: Option<String>,
}

/// One target's pending/failed counts in a replication status/summary response.
#[derive(Debug, Serialize)]
pub struct ReplicationTargetCount {
    /// The remote-target ARN (`None` = the legacy env single-target path).
    pub target_arn: Option<String>,
    /// Entries pending to this target.
    pub pending: u64,
    /// Entries terminally failed to this target.
    pub failed: u64,
}

/// Store-wide replication summary (`GET /replication/summary`; also the SSE `replication` topic).
#[derive(Debug, Serialize)]
pub struct ReplicationSummaryResp {
    /// Entries awaiting their first/next attempt, store-wide.
    pub pending: u64,
    /// Entries leased by a worker.
    pub claimed: u64,
    /// Terminally failed entries.
    pub failed: u64,
    /// Completed entries (rows retained).
    pub completed: u64,
    /// Age of the oldest still-pending enqueue, in seconds (true lag, 0 when idle).
    pub lag_seconds: u64,
    /// Per-target pending/failed breakdown.
    pub by_target: Vec<ReplicationTargetCount>,
}

// ---------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------

/// The JSON error envelope used by every non-success response. The `request_id` mirrors the
/// `x-amz-request-id` response header so an operator can correlate a failed call with logs
/// (ARCH 25.1).
#[derive(Debug, Serialize)]
pub struct ErrorResp {
    /// A short, stable error message.
    pub error: String,
    /// The per-request id, also emitted as the `x-amz-request-id` response header.
    pub request_id: String,
}

// ---------------------------------------------------------------------------------------
// S3 import jobs (ARCH 27)
// ---------------------------------------------------------------------------------------

/// One source→destination bucket mapping in a create-import request. An empty `dest` means "same
/// name as the source".
#[derive(Debug, Clone, Deserialize)]
pub struct ImportBucketMap {
    /// The source bucket name.
    pub source: String,
    /// The destination bucket name (defaults to `source` when empty).
    #[serde(default)]
    pub dest: String,
}

/// Create an import job. The `secret` is sealed server-side and never returned. An empty `buckets`
/// list means "import every bucket the source credentials can see".
#[derive(Debug, Clone, Deserialize)]
pub struct CreateImportReq {
    /// The remote S3 endpoint base URL.
    pub source_endpoint: String,
    /// The SigV4 signing region for the source.
    pub source_region: String,
    /// The source admin access key.
    pub access_key: String,
    /// The source admin secret (sealed at rest, never echoed).
    pub secret: String,
    /// The buckets to import; empty = all.
    #[serde(default)]
    pub buckets: Vec<ImportBucketMap>,
    /// The object-worker count; `None` uses the server default (clamped to the server max).
    #[serde(default)]
    pub workers: Option<u32>,
    /// An optional PEM CA bundle to trust for an https source.
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Skip TLS verification for the source (testing only).
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

/// The create-import response: the job id only (the secret is never echoed).
#[derive(Debug, Clone, Serialize)]
pub struct CreateImportResp {
    /// The new job id.
    pub id: String,
}

/// Per-bucket progress in an import-job detail response.
#[derive(Debug, Clone, Serialize)]
pub struct ImportBucketProgressWire {
    /// The source bucket.
    pub source_bucket: String,
    /// The destination bucket.
    pub dest_bucket: String,
    /// This bucket's state.
    pub state: String,
    /// Objects copied.
    pub objects_done: u64,
    /// Objects seen.
    pub objects_total: u64,
    /// Bytes copied.
    pub bytes_done: u64,
    /// Bytes seen.
    pub bytes_total: u64,
    /// The most recent per-object error, if any.
    pub last_error: Option<String>,
}

/// An import job summary (secret-free) for the list view.
#[derive(Debug, Clone, Serialize)]
pub struct ImportJobEntry {
    /// The job id.
    pub id: String,
    /// The source endpoint.
    pub source_endpoint: String,
    /// The source region.
    pub source_region: String,
    /// The source access-key id (identifier, not a secret).
    pub access_key_id: String,
    /// Whether a custom CA certificate is configured (presence flag).
    pub has_ca_cert: bool,
    /// Whether TLS verification is skipped for the source.
    pub insecure_skip_verify: bool,
    /// The requested worker count (0 = server default).
    pub workers: u32,
    /// The job state.
    pub state: String,
    /// Aggregate objects copied.
    pub objects_done: u64,
    /// Aggregate objects seen.
    pub objects_total: u64,
    /// Aggregate bytes copied.
    pub bytes_done: u64,
    /// Aggregate bytes seen.
    pub bytes_total: u64,
    /// Creation time (epoch millis).
    pub created_at_ms: i64,
    /// Last-update time (epoch millis).
    pub updated_at_ms: i64,
}

/// An import job detail: the summary plus per-bucket progress and any job-level error.
#[derive(Debug, Clone, Serialize)]
pub struct ImportJobDetail {
    /// The summary fields.
    #[serde(flatten)]
    pub entry: ImportJobEntry,
    /// Per-bucket progress.
    pub buckets: Vec<ImportBucketProgressWire>,
    /// A job-level error/status message, if any.
    pub last_error: Option<String>,
}

/// The import-job list response.
#[derive(Debug, Clone, Serialize)]
pub struct ImportListResp {
    /// The jobs, newest first.
    pub jobs: Vec<ImportJobEntry>,
}

/// The contract string for an import state.
#[must_use]
pub fn import_state_str(s: cairn_types::meta::ImportState) -> &'static str {
    use cairn_types::meta::ImportState;
    match s {
        ImportState::Pending => "pending",
        ImportState::Running => "running",
        ImportState::Completed => "completed",
        ImportState::Failed => "failed",
        ImportState::Cancelled => "cancelled",
    }
}

impl From<&cairn_types::meta::ImportJob> for ImportJobEntry {
    fn from(j: &cairn_types::meta::ImportJob) -> Self {
        Self {
            id: j.id.clone(),
            source_endpoint: j.source_endpoint.clone(),
            source_region: j.source_region.clone(),
            access_key_id: j.access_key_id.clone(),
            has_ca_cert: j.has_ca_cert,
            insecure_skip_verify: j.insecure_skip_verify,
            workers: j.workers,
            state: import_state_str(j.state).to_owned(),
            objects_done: j.objects_done,
            objects_total: j.objects_total,
            bytes_done: j.bytes_done,
            bytes_total: j.bytes_total,
            created_at_ms: j.created_at.0,
            updated_at_ms: j.updated_at.0,
        }
    }
}

impl From<&cairn_types::meta::ImportJob> for ImportJobDetail {
    fn from(j: &cairn_types::meta::ImportJob) -> Self {
        Self {
            entry: ImportJobEntry::from(j),
            buckets: j
                .buckets
                .iter()
                .map(|b| ImportBucketProgressWire {
                    source_bucket: b.source_bucket.clone(),
                    dest_bucket: b.dest_bucket.clone(),
                    state: import_state_str(b.state).to_owned(),
                    objects_done: b.objects_done,
                    objects_total: b.objects_total,
                    bytes_done: b.bytes_done,
                    bytes_total: b.bytes_total,
                    last_error: b.last_error.clone(),
                })
                .collect(),
            last_error: j.last_error.clone(),
        }
    }
}
