//! Metadata-store value types: the typed mutation enum that rides the group-committing
//! writer, its outcomes, listing queries/pages, multipart sessions, the replication outbox,
//! and user/credential records.

use crate::authz::{Acl, OwnershipMode, PublicAccessBlock};
use crate::bucket::{Bucket, CompressionPolicy, ConfigAspect, ConfigDoc, VersioningState};
use crate::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use crate::object::{ChecksumValue, ETag, ObjectVersionRow, StorageClass, UserMetadata};
use crate::time::Timestamp;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------------------
// Conditional writes
// ---------------------------------------------------------------------------------------

/// A conditional-write precondition, evaluated inside the same savepoint that performs the
/// upsert so the check and the mutation are atomic.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Precondition {
    /// `If-Match`: the current version's ETag must equal this.
    pub if_match: Option<ETag>,
    /// `If-None-Match`: either the object must not exist (`Any`) or its ETag must differ.
    pub if_none_match: Option<IfNoneMatch>,
}

impl Precondition {
    /// Whether any precondition is set.
    #[must_use]
    pub fn is_unconditional(&self) -> bool {
        self.if_match.is_none() && self.if_none_match.is_none()
    }
}

/// The `If-None-Match` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IfNoneMatch {
    /// `*`: the object must not currently exist.
    Any,
    /// The current version's ETag must differ from this.
    ETag(ETag),
}

// ---------------------------------------------------------------------------------------
// Mutations (every write goes through the single group-committing writer)
// ---------------------------------------------------------------------------------------

/// A typed mutation submitted to the writer. Each is applied in its own savepoint within the
/// shared group-commit transaction, so one mutation's failure rolls back only itself.
#[derive(Debug, Clone)]
pub enum Mutation {
    /// Upsert an object version (the put commit point). Returns any superseded blob path.
    PutObjectVersion {
        /// The new version row.
        row: Box<ObjectVersionRow>,
        /// The conditional precondition.
        precondition: Precondition,
        /// A replication outbox entry to enqueue in the same transaction, if applicable.
        replication: Option<OutboxEntry>,
    },
    /// Insert a delete marker (a versioned plain delete).
    CreateDeleteMarker {
        /// Target bucket.
        bucket: BucketName,
        /// Target key.
        key: ObjectKey,
        /// The marker's version id.
        version_id: VersionId,
        /// The owner.
        owner_id: UserId,
        /// Creation time.
        now: Timestamp,
        /// Replication of the marker, if applicable.
        replication: Option<OutboxEntry>,
    },
    /// Permanently delete a specific version. Returns its freed blob path.
    DeleteVersion {
        /// Target bucket.
        bucket: BucketName,
        /// Target key.
        key: ObjectKey,
        /// The version to remove (sentinel for unversioned).
        version_id: VersionId,
    },
    /// Create a multipart session.
    CreateMultipart(Box<MultipartSession>),
    /// Record (or supersede) a part. Returns any superseded part blob path.
    RecordPart {
        /// The session.
        upload_id: UploadId,
        /// The part.
        part: PartRecord,
    },
    /// Atomically claim a session for completion (guards double completion).
    ClaimMultipart(UploadId),
    /// Complete a multipart upload: upsert the object and remove the session in one tx.
    CompleteMultipart {
        /// The session being completed.
        upload_id: UploadId,
        /// The assembled object version row.
        row: Box<ObjectVersionRow>,
        /// The conditional precondition.
        precondition: Precondition,
        /// Replication enqueue, if applicable.
        replication: Option<OutboxEntry>,
    },
    /// Abort a multipart session.
    AbortMultipart(UploadId),
    /// Create a bucket.
    CreateBucket(Box<Bucket>),
    /// Delete an (empty) bucket.
    DeleteBucket(BucketName),
    /// Set or clear (None) a bucket configuration aspect.
    SetBucketConfig {
        /// The bucket.
        bucket: BucketName,
        /// Which aspect.
        aspect: ConfigAspect,
        /// The document, or None to delete.
        doc: Option<ConfigDoc>,
    },
    /// Set a bucket's versioning state.
    SetVersioning {
        /// The bucket.
        bucket: BucketName,
        /// The new state.
        state: VersioningState,
    },
    /// Set a bucket's ownership mode.
    SetOwnership {
        /// The bucket.
        bucket: BucketName,
        /// The new mode.
        mode: OwnershipMode,
    },
    /// Set (or clear) a bucket's byte quota. The quota is enforced inside the commit transaction
    /// of subsequent object writes (ARCH §27.5).
    SetBucketQuota {
        /// The bucket.
        bucket: BucketName,
        /// The new quota in bytes, or `None` to remove the limit.
        quota_bytes: Option<u64>,
    },
    /// Set (or clear) a bucket's compression policy, applied to subsequent object writes.
    SetBucketCompression {
        /// The bucket.
        bucket: BucketName,
        /// The new compression policy, or `None` to disable compression.
        policy: Option<CompressionPolicy>,
    },
    /// Set (or clear) a user's attached identity policy (ARCH §15 / user-centric authz). The value
    /// is the validated policy JSON document, or `None` to detach.
    SetUserPolicy {
        /// The user.
        user_id: UserId,
        /// The validated policy JSON, or `None` to clear.
        policy: Option<String>,
    },
    /// Set (or clear) a user's byte quota. The quota is enforced inside the commit transaction of
    /// subsequent object writes the user owns (ARCH §27.5), mirroring [`Mutation::SetBucketQuota`].
    SetUserQuota {
        /// The user.
        user_id: UserId,
        /// The new quota in bytes, or `None` to remove the limit.
        quota_bytes: Option<u64>,
    },
    /// Set the account-wide Block Public Access singleton.
    SetAccountPublicAccessBlock(PublicAccessBlock),
    /// Replace an object version's tags.
    PutObjectTags {
        /// The bucket.
        bucket: BucketName,
        /// The key.
        key: ObjectKey,
        /// The version.
        version_id: VersionId,
        /// The new tag set.
        tags: Vec<(String, String)>,
    },
    /// Delete an object version's tags.
    DeleteObjectTags {
        /// The bucket.
        bucket: BucketName,
        /// The key.
        key: ObjectKey,
        /// The version.
        version_id: VersionId,
    },
    /// Set (or clear) an object version's ACL document (the `PutObjectAcl` commit point). The new
    /// ACL replaces the version row's stored `acl` column; `None` clears it (ARCH §13.3/§15.4).
    SetObjectAcl {
        /// The bucket.
        bucket: BucketName,
        /// The key.
        key: ObjectKey,
        /// The version whose ACL is replaced.
        version_id: VersionId,
        /// The new ACL document, or `None` to clear it.
        acl: Option<Acl>,
    },
    /// Create a user (with credentials).
    CreateUser(Box<UserRecord>),
    /// Update a user's mutable fields.
    UpdateUser(Box<UserRecord>),
    /// Deactivate a user.
    DeactivateUser(UserId),
    /// Atomically claim a batch of due replication-outbox entries, routed through the writer so
    /// the select-and-mark is one transaction (no two workers claim the same entry). Marks each
    /// claimed entry `status='claimed'` with `lease_until = now + lease_secs`, and returns them.
    ClaimReplicationBatch {
        /// Maximum entries to claim.
        limit: u32,
        /// The current time (the due-by cutoff and the lease base).
        now: Timestamp,
        /// The claim lease length in seconds.
        lease_secs: i64,
    },
    /// Mark a replication outbox entry done and stamp the version replicated.
    MarkReplicationDone(String),
    /// Mark a replication outbox entry failed/retry with backoff.
    MarkReplicationFailed {
        /// The entry id.
        id: String,
        /// The last error.
        error: String,
        /// When to next attempt (None = give up / terminal).
        next_attempt_at: Option<Timestamp>,
    },
    /// Requeue terminal (`status='failed'`) replication-outbox entries for another attempt: flips
    /// them back to `pending` with `next_attempt_at = now` so the worker picks them up on the next
    /// drain (ARCH §20.5). Scoped to one bucket when `bucket` is `Some`, else all failed entries.
    RetryFailedReplication {
        /// Restrict to this source bucket, or `None` for every failed entry.
        bucket: Option<BucketName>,
        /// The time to schedule the retry at (immediately due).
        now: Timestamp,
    },
    /// Enqueue a single replication-outbox entry idempotently (INSERT OR IGNORE on the entry id),
    /// used by existing-object backfill / resync (ARCH §20.5). Unlike the enqueue that rides a
    /// `PutObjectVersion`, this stands alone for objects written before replication was configured;
    /// the deterministic backfill id makes a repeated resync a no-op for already-queued versions.
    EnqueueReplication(Box<OutboxEntry>),
    /// Append an audit/activity entry.
    RecordActivity(Box<ActivityEntry>),
    /// Create a persistent object-share token (ARCH §15.8).
    CreateShare(Box<ShareRow>),
    /// Revoke a share token (idempotent; sets `revoked_at` if still active).
    RevokeShare {
        /// The token to revoke.
        token: String,
        /// The revocation time.
        now: Timestamp,
    },
}

/// The typed result of applying a [`Mutation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationOutcome {
    /// A put committed.
    Put {
        /// Any superseded blob to reclaim.
        superseded: Option<StoragePath>,
        /// The committed version id.
        version_id: VersionId,
    },
    /// A delete marker was inserted.
    DeleteMarker {
        /// The marker's version id.
        version_id: VersionId,
    },
    /// A version was permanently deleted.
    Deleted {
        /// The freed blob, if the version referenced one.
        freed: Option<StoragePath>,
        /// Whether a successor was promoted to latest.
        promoted_latest: bool,
    },
    /// A multipart session was created.
    MultipartCreated(UploadId),
    /// A part was recorded.
    PartRecorded {
        /// Any superseded part blob to reclaim.
        superseded: Option<StoragePath>,
    },
    /// A claim attempt resolved.
    MultipartClaim(ClaimOutcome),
    /// A multipart completion committed.
    MultipartCompleted {
        /// Any superseded object blob to reclaim.
        superseded: Option<StoragePath>,
        /// The committed version id.
        version_id: VersionId,
    },
    /// A batch of due replication entries was claimed.
    ReplicationBatch(Vec<OutboxEntry>),
    /// A user was created.
    UserCreated(UserId),
    /// A generic acknowledgement for mutations with no specific return value.
    Ack,
}

// ---------------------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------------------

/// A listing query over a bucket's keyspace.
#[derive(Debug, Clone, Default)]
pub struct ListQuery {
    /// Restrict to keys starting with this prefix.
    pub prefix: Option<String>,
    /// Group keys sharing a prefix up to this delimiter into common prefixes.
    pub delimiter: Option<String>,
    /// Continuation cursor (the last key returned).
    pub cursor: Option<String>,
    /// Version-id marker for version listings: resume strictly after `(cursor, version_id_marker)`
    /// so a key whose versions span a page boundary continues mid-key. Ignored unless `cursor` is
    /// also set (the key it pairs with). `None` resumes at the key boundary.
    pub version_id_marker: Option<String>,
    /// Start strictly after this key.
    pub start_after: Option<String>,
    /// Page size (clamped to the S3 ceiling by the caller).
    pub limit: u32,
}

/// One page of a bounded listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage<T> {
    /// The entries in this page.
    pub items: Vec<T>,
    /// Common prefixes grouped by the delimiter.
    pub common_prefixes: Vec<String>,
    /// The cursor to resume after, if truncated. For version listings this is the boundary key
    /// (paired with [`next_version_id_marker`](Self::next_version_id_marker)); for current-object
    /// listings it is the last key returned.
    pub next_cursor: Option<String>,
    /// The boundary version id to resume after, for a version listing truncated mid-key. Threads
    /// back as the next request's [`ListQuery::version_id_marker`] (paired with `next_cursor` as the
    /// key) so a key whose versions span a page boundary continues strictly after the last returned
    /// version. `None` for current-object listings and for version listings truncated on a key
    /// boundary (the next page resumes at the next key).
    pub next_version_id_marker: Option<String>,
    /// Whether more pages remain.
    pub truncated: bool,
}

impl<T> Default for ListPage<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            common_prefixes: Vec::new(),
            next_cursor: None,
            next_version_id_marker: None,
            truncated: false,
        }
    }
}

/// A summary of one object version for listing output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectSummary {
    /// The key.
    pub key: ObjectKey,
    /// The version id.
    pub version_id: VersionId,
    /// Whether this is the latest version.
    pub is_latest: bool,
    /// Whether this is a delete marker.
    pub is_delete_marker: bool,
    /// The ETag.
    pub etag: ETag,
    /// The logical size.
    pub size: u64,
    /// Last-modified time.
    pub last_modified: Timestamp,
    /// The storage class.
    pub storage_class: StorageClass,
    /// The owner.
    pub owner_id: UserId,
}

// ---------------------------------------------------------------------------------------
// Multipart
// ---------------------------------------------------------------------------------------

/// A multipart upload session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartSession {
    /// The upload id.
    pub upload_id: UploadId,
    /// The target bucket.
    pub bucket: BucketName,
    /// The target key.
    pub key: ObjectKey,
    /// The content type to apply on completion.
    pub content_type: String,
    /// The session status.
    pub status: MultipartStatus,
    /// The owner.
    pub owner_id: UserId,
    /// The ACL to apply on completion.
    pub intended_acl: Option<Acl>,
    /// The metadata to apply on completion.
    pub user_metadata: UserMetadata,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last-update time.
    pub updated_at: Timestamp,
}

/// A multipart session status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MultipartStatus {
    /// Accepting parts.
    Active,
    /// Claimed for completion.
    Completing,
    /// Aborted.
    Aborted,
}

/// How a shared object is delivered to the browser (the `Content-Disposition`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShareDisposition {
    /// `inline` — render in the browser when possible.
    Inline,
    /// `attachment` — force a download (optionally with a chosen filename).
    Attachment,
}

/// A persistent, revocable, optionally-forever object-share token (ARCH §15.8). The token is the
/// bearer capability served at `GET /p/{token}`; revoking flips `revoked_at` without rotating any
/// global key. Stored in the `object_shares` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareRow {
    /// The opaque bearer token (base64url of 32 CSPRNG bytes); the table primary key.
    pub token: String,
    /// The shared object's bucket.
    pub bucket: BucketName,
    /// The shared object's key.
    pub key: ObjectKey,
    /// A pinned version id, or `None` to always follow the current version.
    pub version_id: Option<VersionId>,
    /// Expiry, or `None` for a forever share (valid until revoked).
    pub expires_at: Option<Timestamp>,
    /// How the object is delivered (inline vs forced download).
    pub disposition: ShareDisposition,
    /// The download filename for `attachment`, or `None` to use the object's basename.
    pub filename: Option<String>,
    /// The user id that minted the share (for audit).
    pub created_by: UserId,
    /// When it was minted.
    pub created_at: Timestamp,
    /// When it was revoked, or `None` while active.
    pub revoked_at: Option<Timestamp>,
}

/// A recorded multipart part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartRecord {
    /// The part number (1..=10000).
    pub part_number: u16,
    /// The plaintext part size.
    pub size: u64,
    /// The part's hex MD5 (its part ETag).
    pub etag: String,
    /// The part blob path.
    pub storage_path: StoragePath,
    /// Any client-supplied checksum.
    pub checksum: Option<ChecksumValue>,
}

/// The outcome of claiming a multipart session for completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The session was claimed; here it is.
    Claimed(Box<MultipartSession>),
    /// Already being completed by another caller.
    AlreadyClaimed,
    /// No such (active) session.
    NotFound,
}

// ---------------------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------------------

/// The replication status of an object version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationStatus {
    /// Awaiting replication.
    Pending,
    /// Claimed by a worker under a lease; eligible for re-claim once the lease expires.
    Claimed,
    /// Replicated successfully.
    Completed,
    /// Replication failed after retries.
    Failed,
    /// This object arrived via replication (do not re-replicate).
    Replica,
}

/// What an outbox entry asks a worker to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationOp {
    /// Replicate an object creation.
    ObjectCreate,
    /// Propagate a delete marker.
    DeleteMarker,
}

/// A durable replication outbox entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEntry {
    /// Entry id.
    pub id: String,
    /// The bucket.
    pub bucket: BucketName,
    /// The key.
    pub key: ObjectKey,
    /// The version concerned.
    pub version_id: VersionId,
    /// The operation.
    pub operation: ReplicationOp,
    /// The replication rule id this belongs to.
    pub rule_id: String,
    /// The remote-target ARN this entry ships to, resolved from the matching rule at enqueue time
    /// and stamped on the entry so routing is a pure per-entry lookup at drain time (a later rule
    /// edit cannot misroute already-queued entries). `None` for entries enqueued before targets
    /// were stamped or routed via the legacy env single-target path.
    pub target_arn: Option<String>,
    /// Retry attempt count.
    pub attempts: u32,
    /// When the entry is next due.
    pub next_attempt_at: Timestamp,
    /// Current status.
    pub status: ReplicationStatus,
    /// The last error, if any.
    pub last_error: Option<String>,
    /// Dispatch priority; higher is claimed first (default 0).
    pub priority: i64,
    /// When the current claim lease expires; `None` when the entry is not claimed. A claimed
    /// entry whose lease has elapsed is eligible to be re-claimed.
    pub lease_until: Option<Timestamp>,
}

// ---------------------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------------------

/// A user record without secret material (for listing/management).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// User id.
    pub id: UserId,
    /// Display name.
    pub display_name: String,
    /// Bearer access-key id.
    pub access_key_id: String,
    /// SigV4 access-key id, if the user has SigV4 credentials.
    pub sigv4_access_key_id: Option<String>,
    /// Role.
    pub role: crate::auth::Role,
    /// Whether active.
    pub is_active: bool,
    /// The per-user byte quota (`users.quota_bytes`, ARCH §27.5), or `None` when unset (no limit).
    pub quota_bytes: Option<u64>,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last-update time.
    pub updated_at: Timestamp,
}

/// A full user record including secret material, for creation/update mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserRecord {
    /// The public user fields.
    pub user: User,
    /// The fast hash of the Bearer secret.
    pub bearer_secret_hash: String,
    /// The SigV4 secret ciphertext (envelope-encrypted), if any.
    pub sigv4_secret_ciphertext: Option<Vec<u8>>,
    /// The nonce for the SigV4 secret ciphertext.
    pub sigv4_secret_nonce: Option<Vec<u8>>,
}

/// A user looked up by Bearer key, with the stored secret hash for verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserWithBearerHash {
    /// The user.
    pub user: User,
    /// The stored secret hash.
    pub secret_hash: String,
}

/// A user looked up by SigV4 key, with the encrypted secret for the authenticator to
/// decrypt transiently via [`crate::Crypto`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserSigV4Credentials {
    /// The user.
    pub user: User,
    /// The SigV4 secret ciphertext.
    pub secret_ciphertext: Vec<u8>,
    /// The ciphertext nonce.
    pub secret_nonce: Vec<u8>,
}

// ---------------------------------------------------------------------------------------
// Audit & aggregates
// ---------------------------------------------------------------------------------------

/// An audit/activity log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityEntry {
    /// Entry id.
    pub id: String,
    /// The action performed.
    pub action: String,
    /// The bucket, if applicable.
    pub bucket: Option<String>,
    /// The key, if applicable.
    pub key: Option<String>,
    /// The size, if applicable.
    pub size: Option<u64>,
    /// The ETag, if applicable.
    pub etag: Option<String>,
    /// The actor's user id.
    pub actor: Option<String>,
    /// When it happened.
    pub at: Timestamp,
}

/// Aggregate store counts for the overview/metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreCounts {
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
}

/// Per-bucket aggregate counts for the overview's storage breakdown. Semantics mirror
/// [`StoreCounts`] sliced by bucket — `objects` counts latest non-delete-marker versions and the
/// byte totals sum over *all* versions — so the per-bucket rows add up to the store totals.
/// Buckets with no objects are included with zeros.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketCounts {
    /// The bucket name.
    pub bucket: String,
    /// Number of current objects.
    pub objects: u64,
    /// Total logical bytes across all versions.
    pub logical_bytes: u64,
    /// Total physical bytes across all versions.
    pub physical_bytes: u64,
}
