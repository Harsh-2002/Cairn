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
        /// Replication outbox entries to enqueue in the same transaction — one per matching
        /// destination target (fan-out); empty when the write does not replicate (ARCH 20).
        replication: Vec<OutboxEntry>,
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
        /// Replication of the marker — one entry per matching target; empty when not replicated.
        replication: Vec<OutboxEntry>,
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
        /// Replication enqueue — one entry per matching target; empty when not replicated.
        replication: Vec<OutboxEntry>,
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
    /// Set or clear the Object Lock retention on one object version (preserving any legal hold).
    /// The protocol layer enforces the retention policy (no shortening COMPLIANCE, governance bypass)
    /// before submitting this.
    SetObjectRetention {
        /// Target bucket.
        bucket: BucketName,
        /// Target key.
        key: ObjectKey,
        /// Target version.
        version_id: VersionId,
        /// The retention to apply, or None to clear it.
        retention: Option<crate::object::ObjectRetention>,
    },
    /// Set the Object Lock legal-hold flag on one object version (preserving any retention).
    SetObjectLegalHold {
        /// Target bucket.
        bucket: BucketName,
        /// Target key.
        key: ObjectKey,
        /// Target version.
        version_id: VersionId,
        /// Whether the legal hold is on.
        on: bool,
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
    /// of subsequent object writes (ARCH 27.5).
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
    /// Set (or clear) a user's attached identity policy (ARCH 15 / user-centric authz). The value
    /// is the validated policy JSON document, or `None` to detach.
    SetUserPolicy {
        /// The user.
        user_id: UserId,
        /// The validated policy JSON, or `None` to clear.
        policy: Option<String>,
    },
    /// Set (or clear) a user's byte quota. The quota is enforced inside the commit transaction of
    /// subsequent object writes the user owns (ARCH 27.5), mirroring [`Mutation::SetBucketQuota`].
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
    /// ACL replaces the version row's stored `acl` column; `None` clears it (ARCH 13.3/15.4).
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
    /// Mint an STS-style temporary session credential scoped to a parent user (ARCH 14).
    CreateSessionCredential(Box<SessionCredentialRecord>),
    /// Delete all session credentials that expired before `before` (the background cleanup sweep).
    DeleteExpiredSessionCredentials {
        /// The expiry cutoff (epoch ms): rows with `expires_at < before` are removed.
        before: Timestamp,
    },
    /// Revoke a single session credential early by its access-key id (idempotent: a no-op if the
    /// row is already gone). Deleting the row makes the next request authenticate as unknown.
    DeleteSessionCredential {
        /// The temporary access-key id to revoke.
        access_key_id: String,
    },
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
    /// drain (ARCH 20.5). Scoped to one bucket when `bucket` is `Some`, else all failed entries.
    RetryFailedReplication {
        /// Restrict to this source bucket, or `None` for every failed entry.
        bucket: Option<BucketName>,
        /// The time to schedule the retry at (immediately due).
        now: Timestamp,
    },
    /// Enqueue a single replication-outbox entry idempotently (INSERT OR IGNORE on the entry id),
    /// used by existing-object backfill / resync (ARCH 20.5). Unlike the enqueue that rides a
    /// `PutObjectVersion`, this stands alone for objects written before replication was configured;
    /// the deterministic backfill id makes a repeated resync a no-op for already-queued versions.
    EnqueueReplication(Box<OutboxEntry>),
    /// Reclaim terminal replication-outbox rows: delete `completed` and `failed` entries whose
    /// `enqueued_at` is older than `before_ms`. The outbox is a durable WORK queue, not a permanent
    /// per-object ledger — completed rows carry no further information (the object version row holds
    /// the replication status) and unbounded retention would grow the table with every replicated
    /// object. Pending/claimed entries (outstanding work) are never pruned. Bounds the table and
    /// auto-clears genuinely-stale failures (ARCH 20.3).
    PruneReplicationOutbox {
        /// Delete completed/failed entries enqueued before this wall-clock millis.
        before_ms: i64,
    },
    /// Release a *claimed* replication-outbox entry back to `pending` so it is promptly
    /// re-claimable, **without** consuming the terminal attempt budget. Used for two non-failure
    /// reschedules: (1) an entry deferred to preserve per-key ordering (an earlier version is still
    /// in flight) — re-checked after a short delay instead of waiting out the 300 s claim lease; and
    /// (2) an entry whose destination target is *unavailable* (transport error / 5xx) — re-tried at
    /// a bounded cadence so a target that is down for hours auto-resumes when it returns rather than
    /// exhausting to a terminal failure. Leaves `attempts` untouched; clears the lease and sets
    /// `next_attempt_at`. `last_error` records the reason when `Some` (an ordering defer passes
    /// `None`, leaving the prior error intact).
    DeferReplication {
        /// The entry id.
        id: String,
        /// When the entry next becomes due (claimable).
        next_attempt_at: Timestamp,
        /// Optional last-error string to record (None leaves the existing value).
        last_error: Option<String>,
    },
    /// Enqueue a batch of event-notification (webhook) outbox entries idempotently (INSERT OR
    /// IGNORE on the deterministic entry id). Emitted by the protocol layer right after an object
    /// commit succeeds; delivery is best-effort at-least-once (a crash in the gap drops the
    /// notification, never the object), matching S3's best-effort event-delivery contract.
    EnqueueWebhooks(Vec<WebhookEntry>),
    /// Atomically claim a batch of due webhook-outbox entries (select-and-mark in one transaction,
    /// so no two workers claim the same entry). Marks each `status='claimed'` with
    /// `lease_until = now + lease_secs`; returns them as [`MutationOutcome::WebhookBatch`].
    ClaimWebhookBatch {
        /// Maximum entries to claim.
        limit: u32,
        /// The current time (the due-by cutoff and the lease base).
        now: Timestamp,
        /// The claim lease length in seconds.
        lease_secs: i64,
    },
    /// Mark a webhook-outbox entry delivered (or dropped): the row is deleted outright, so the
    /// success path keeps `events_outbox` bounded — only pending and terminally-failed rows persist.
    MarkWebhookDone(String),
    /// Mark a webhook-outbox entry failed/retry: bump attempts, store the error, and either
    /// reschedule (`next_attempt_at = Some`) back to `pending` or give up (`None` = terminal `failed`).
    MarkWebhookFailed {
        /// The entry id.
        id: String,
        /// The last error.
        error: String,
        /// When to next attempt (None = give up / terminal).
        next_attempt_at: Option<Timestamp>,
    },
    /// Append an audit/activity entry.
    RecordActivity(Box<ActivityEntry>),
    /// Create a persistent object-share token (ARCH 15.8).
    CreateShare(Box<ShareRow>),
    /// Revoke a share token (idempotent; sets `revoked_at` if still active).
    RevokeShare {
        /// The token to revoke.
        token: String,
        /// The revocation time.
        now: Timestamp,
    },
    /// Flush a batch of accumulated request-metric rows (upsert-accumulate by composite key) and
    /// optionally prune rows older than `prune_before` (ARCH 26.5). One mutation = one transaction,
    /// so the request hot path never touches the DB; the in-process aggregator coalesces and the
    /// background flush submits this periodically.
    RecordRequestMetrics {
        /// The accumulated rows to upsert.
        rows: Vec<RequestMetricRow>,
        /// When set, delete rows whose `ts_bucket` is strictly less than this epoch-seconds bound.
        prune_before: Option<i64>,
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
    /// A batch of due webhook-notification entries was claimed.
    WebhookBatch(Vec<WebhookEntry>),
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
    /// Whether SSE-S3 was requested for this upload at initiate (via the request header or the
    /// bucket default-encryption setting). Captured at initiate because the `CompleteMultipartUpload`
    /// request carries no SSE header; honored at completion so the assembled object is encrypted at
    /// rest exactly like a single-part PUT (ARCH 27).
    pub sse_requested: bool,
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

/// A persistent, revocable, optionally-forever object-share token (ARCH 15.8). The token is the
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
    /// Wall-clock millis the entry was first enqueued, fixed at enqueue and never moved by a retry
    /// (unlike [`next_attempt_at`](Self::next_attempt_at)). Drives the true replication-lag gauge
    /// (age of the oldest still-pending enqueue). Rows migrated from before this column read `0`,
    /// which lag treats as "unknown".
    pub enqueued_at: Timestamp,
}

/// Aggregate replication-outbox counts, computed in a single indexed pass (never `PAGE_LIMIT`
/// bounded) for metrics and the control-plane status/summary. `bucket`-scoped or store-wide.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicationCounts {
    /// Entries awaiting their first/next attempt.
    pub pending: u64,
    /// Entries leased by a worker for the current pass.
    pub claimed: u64,
    /// Terminally failed entries (retries exhausted or a terminal error).
    pub failed: u64,
    /// Entries that completed replication (their outbox row is retained).
    pub completed: u64,
    /// The oldest still-`pending` entry's enqueue time in ms (`0` when nothing is pending, or when
    /// every pending row predates the `enqueued_at` column). The caller, which holds a [`Clock`],
    /// derives lag as `max(0, now - oldest_pending_at_ms)`.
    ///
    /// [`Clock`]: crate::traits::Clock
    pub oldest_pending_at_ms: i64,
    /// Per-target pending/failed breakdown; targets with neither are omitted.
    pub by_target: Vec<ReplicationTargetCounts>,
}

/// One target's pending/failed replication counts (part of [`ReplicationCounts`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicationTargetCounts {
    /// The remote-target ARN (`None` = the legacy env single-target path).
    pub target_arn: Option<String>,
    /// Entries pending to this target.
    pub pending: u64,
    /// Entries terminally failed to this target.
    pub failed: u64,
}

/// The delivery status of a webhook event-notification outbox entry. Mirrors
/// [`ReplicationStatus`] minus the inbound-`Replica` state (events are never inbound).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookStatus {
    /// Awaiting delivery.
    Pending,
    /// Claimed by a worker under a lease; eligible for re-claim once the lease expires.
    Claimed,
    /// Delivered successfully (endpoint returned 2xx).
    Completed,
    /// Delivery failed after the retry budget was exhausted (terminal).
    Failed,
}

/// A durable event-notification (webhook) outbox entry: one object event matched to one endpoint.
/// The S3-event-record JSON is pre-rendered into `payload` at enqueue time so delivery is a pure
/// HTTP POST that needs no further metadata lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookEntry {
    /// Entry id (deterministic: `{bucket}:{endpoint}:{version}:{event}`, so a re-enqueue is idempotent).
    pub id: String,
    /// The source bucket.
    pub bucket: BucketName,
    /// The object key.
    pub key: ObjectKey,
    /// The version concerned (the sentinel version id for unversioned buckets).
    pub version_id: VersionId,
    /// The event that fired.
    pub event: crate::notification::EventKind,
    /// The id of the bucket webhook endpoint this entry delivers to (resolved against the bucket's
    /// notification config at delivery time for the URL + signing secret).
    pub endpoint_id: String,
    /// The fully-rendered JSON body to POST.
    pub payload: String,
    /// Retry attempt count.
    pub attempts: u32,
    /// When the entry is next due.
    pub next_attempt_at: Timestamp,
    /// Current status.
    pub status: WebhookStatus,
    /// The last delivery error, if any.
    pub last_error: Option<String>,
    /// Dispatch priority; higher is claimed first (default 0).
    pub priority: i64,
    /// When the current claim lease expires; `None` when not claimed.
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
    /// The per-user byte quota (`users.quota_bytes`, ARCH 27.5), or `None` when unset (no limit).
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

/// The record persisted when minting an STS-style temporary session credential. The secret is
/// sealed under the master key exactly like a user's SigV4 secret; the session token is stored as a
/// hash (never plaintext). The credential is scoped to its parent user and expires at `expires_at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCredentialRecord {
    /// The temporary access-key id (the SigV4 lookup key).
    pub access_key_id: String,
    /// The parent user this session derives from (owns the buckets, ties the audit trail).
    pub parent_user_id: UserId,
    /// The sealed temporary secret (`CRK1` envelope; `secret_nonce` is `None`).
    pub secret_ciphertext: Vec<u8>,
    /// The legacy ciphertext nonce (`None` for a `CRK1` envelope).
    pub secret_nonce: Option<Vec<u8>>,
    /// The hash of the opaque session token the SDK must present (`X-Amz-Security-Token`).
    pub session_token_hash: String,
    /// An optional inline policy JSON scoping the session below the parent (the session's effective
    /// identity policy). `None` inherits the parent's attached policy.
    pub inline_policy: Option<String>,
    /// When the credential expires (epoch ms); requests after this are denied.
    pub expires_at: Timestamp,
    /// When it was minted.
    pub created_at: Timestamp,
}

/// A temporary session credential looked up by its access-key id, with everything the authenticator
/// needs to validate the request (decrypt the secret, check the token + expiry) and build a
/// least-privilege principal. The parent's identity is joined in for the principal and the
/// active-account check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserSessionCredentials {
    /// The parent user id (the principal's identity, for ownership + audit).
    pub parent_user_id: UserId,
    /// The parent's display name.
    pub parent_display_name: String,
    /// Whether the parent account is still active (a deactivated parent's sessions are denied).
    pub parent_is_active: bool,
    /// The sealed temporary secret.
    pub secret_ciphertext: Vec<u8>,
    /// The legacy ciphertext nonce (empty for a `CRK1` envelope).
    pub secret_nonce: Vec<u8>,
    /// The stored session-token hash to compare (constant-time) against the presented token.
    pub session_token_hash: String,
    /// The optional inline policy JSON scoping the session (the effective identity policy).
    pub inline_policy: Option<String>,
    /// When the credential expires (epoch ms).
    pub expires_at: Timestamp,
}

/// A non-secret summary of an active session credential, safe to surface in the console's
/// "active sessions" list. Carries no secret/nonce/token material — only the public identifier and
/// timing, plus whether an inline policy scopes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCredentialSummary {
    /// The temporary access-key id (the public identifier).
    pub access_key_id: String,
    /// The parent user this session derives from.
    pub parent_user_id: UserId,
    /// Whether an inline policy scopes this session below the parent.
    pub has_inline_policy: bool,
    /// When the credential was minted (epoch ms).
    pub created_at: Timestamp,
    /// When it expires (epoch ms).
    pub expires_at: Timestamp,
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

// ---------------------------------------------------------------------------------------
// Request metrics (usage analytics, ARCH 26.5)
// ---------------------------------------------------------------------------------------

/// The inclusive upper bounds (milliseconds) of the request-latency histogram buckets; the implicit
/// final bucket catches everything slower. Mirrors the `lat_le_*`/`lat_gt_1000` columns added in
/// schema migration v9. Used both by the ingestion aggregator (to bucket a sample) and by the query
/// path (to estimate percentiles). Keep in lockstep with the SQL column names.
pub const LATENCY_BUCKET_BOUNDS_MS: [u64; 5] = [5, 20, 50, 200, 1000];

/// The number of latency histogram buckets: one per bound plus the overflow bucket.
pub const LATENCY_BUCKETS: usize = LATENCY_BUCKET_BOUNDS_MS.len() + 1;

/// Map a latency sample (ms) to its histogram bucket index in `0..LATENCY_BUCKETS`.
pub fn latency_bucket_index(ms: u64) -> usize {
    for (i, bound) in LATENCY_BUCKET_BOUNDS_MS.iter().enumerate() {
        if ms <= *bound {
            return i;
        }
    }
    LATENCY_BUCKETS - 1
}

/// Estimate the `q`-quantile (e.g. 0.95) in milliseconds from aggregated histogram bucket counts,
/// linearly interpolating within the bucket that contains the quantile. The overflow bucket reports
/// its lower bound (we cannot bound it above). Returns 0 when there are no samples.
pub fn latency_quantile_ms(hist: &[u64; LATENCY_BUCKETS], q: f64) -> u64 {
    let total: u64 = hist.iter().sum();
    if total == 0 {
        return 0;
    }
    let target = (q * total as f64).ceil() as u64;
    let mut cumulative = 0u64;
    let mut lower = 0u64;
    for (i, &c) in hist.iter().enumerate() {
        let upper = LATENCY_BUCKET_BOUNDS_MS.get(i).copied();
        cumulative += c;
        if cumulative >= target {
            match upper {
                // Interpolate within [lower, upper] by how far into this bucket the target falls.
                Some(up) if c > 0 => {
                    let into = cumulative - target; // remaining above target within this bucket
                    let frac = 1.0 - (into as f64 / c as f64);
                    return lower + ((up - lower) as f64 * frac) as u64;
                }
                Some(up) => return up,
                None => return lower, // overflow bucket: report its lower bound
            }
        }
        if let Some(up) = upper {
            lower = up;
        }
    }
    lower
}

/// One accumulated request-metrics rollup row: counts, transferred bytes, and a latency histogram for
/// requests in window `ts_bucket` for a given operation, bucket (empty string for non-bucket ops),
/// and HTTP status class (ARCH 26.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestMetricRow {
    /// Epoch seconds floored to the rollup window.
    pub ts_bucket: i64,
    /// The classified operation name (e.g. `GetObject`, `PutObject`, `Management`).
    pub operation: String,
    /// The bucket the request targeted, or `""` for non-bucket operations.
    pub bucket: String,
    /// The HTTP status class: `2xx`, `3xx`, `4xx`, or `5xx`.
    pub status_class: String,
    /// Number of requests accumulated for this key.
    pub count: u64,
    /// Total request (received) bytes for these requests.
    pub bytes_in: u64,
    /// Total response (sent) bytes for these requests.
    pub bytes_out: u64,
    /// Sum of request latencies in milliseconds (divide by `count` for the average).
    pub lat_sum_ms: u64,
    /// Latency histogram bucket counts (see [`LATENCY_BUCKET_BOUNDS_MS`]).
    pub lat_hist: [u64; LATENCY_BUCKETS],
}

/// The time range the console asks for, which also fixes the query-time downsampling window so each
/// range returns a bounded number of points regardless of the underlying row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsRange {
    /// Last 24 hours, downsampled to 5-minute points.
    OneDay,
    /// Last 7 days, downsampled to hourly points.
    OneWeek,
    /// Last 14 days, downsampled to 3-hour points.
    TwoWeeks,
    /// Last ~31 days, downsampled to 6-hour points.
    OneMonth,
}

impl MetricsRange {
    /// Parse the wire token (`1d`/`1w`/`2w`/`1m`); unknown values fall back to [`Self::OneDay`].
    pub fn parse(s: &str) -> Self {
        match s {
            "1w" => Self::OneWeek,
            "2w" => Self::TwoWeeks,
            "1m" => Self::OneMonth,
            _ => Self::OneDay,
        }
    }

    /// The downsampling window, in seconds, that timeline points are bucketed into.
    pub fn window_secs(self) -> i64 {
        match self {
            Self::OneDay => 300,      // 5 minutes
            Self::OneWeek => 3_600,   // 1 hour
            Self::TwoWeeks => 10_800, // 3 hours
            Self::OneMonth => 21_600, // 6 hours
        }
    }

    /// The total span of the range, in seconds.
    pub fn span_secs(self) -> i64 {
        match self {
            Self::OneDay => 86_400,
            Self::OneWeek => 604_800,
            Self::TwoWeeks => 1_209_600,
            Self::OneMonth => 2_678_400, // 31 days
        }
    }

    /// The inclusive lower bound (epoch seconds) for rows in this range, given `now` (epoch seconds).
    pub fn since_secs(self, now: i64) -> i64 {
        now - self.span_secs()
    }
}

/// One point on the requests-over-time timeline: `ts` is the window start (epoch seconds). Each point
/// carries enough to drive the requests, errors, throughput, and latency charts from one series.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimePoint {
    /// Window start, epoch seconds.
    pub ts: i64,
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

/// A breakdown attributed to one operation name: request count, total bytes, and average latency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpCount {
    /// The operation name.
    pub operation: String,
    /// Requests for this operation in range.
    pub count: u64,
    /// Total bytes (in + out) for this operation in range.
    pub bytes: u64,
    /// Average latency for this operation, milliseconds.
    pub latency_avg_ms: u64,
}

/// A breakdown attributed to one bucket: request count and total bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRequestCount {
    /// The bucket name.
    pub bucket: String,
    /// Requests against this bucket in range.
    pub count: u64,
    /// Total bytes (in + out) against this bucket in range.
    pub bytes: u64,
}

/// A request count attributed to one HTTP status class (`2xx`/`3xx`/`4xx`/`5xx`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusCount {
    /// The status class.
    pub status_class: String,
    /// Requests with this status class in range.
    pub count: u64,
}

/// The aggregated request-metrics answer for a [`MetricsRange`]: a rich downsampled timeline plus
/// breakdowns by operation, bucket, and status class, and range-wide totals (bytes, errors, latency
/// average and p95, peak window, active buckets) — enough to drive the whole console dashboard.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestMetricsSeries {
    /// Requests over time, one point per downsampling window (ascending by `ts`).
    pub timeline: Vec<TimePoint>,
    /// Requests broken down by operation, descending by count.
    pub by_operation: Vec<OpCount>,
    /// The most-active buckets, descending by count (capped to a small N).
    pub top_buckets: Vec<BucketRequestCount>,
    /// The top buckets by bytes transferred (in + out), descending (capped to a small N). A genuinely
    /// different ranking than `top_buckets`: a backup target with one huge transfer outranks a chatty
    /// metadata bucket, so the console's "by data" panel must not reuse the by-count cohort.
    pub top_buckets_by_bytes: Vec<BucketRequestCount>,
    /// Requests broken down by HTTP status class.
    pub by_status: Vec<StatusCount>,
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
    /// Range-wide 95th-percentile latency, milliseconds (estimated from the histogram).
    pub latency_p95_ms: u64,
    /// The busiest single window's request count (for a peak req/s stat).
    pub peak_window_count: u64,
    /// Number of distinct buckets that saw any traffic in range.
    pub active_buckets: u64,
    /// The timeline downsampling window, in seconds (for the UI to derive req/s).
    pub window_secs: i64,
}

// ---------------------------------------------------------------------------------------
// Object tag browsing (ARCH 17.2)
// ---------------------------------------------------------------------------------------

/// One distinct object tag (`tag_key=tag_value`) in use, with how many current objects carry it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagSummary {
    /// The tag key.
    pub tag_key: String,
    /// The tag value.
    pub tag_value: String,
    /// Number of current objects (latest, non-delete-marker) carrying this exact key=value.
    pub object_count: u64,
}

/// A current object that carries a queried tag, with enough to render it and link into its browser.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaggedObject {
    /// The bucket the object lives in.
    pub bucket: String,
    /// The object key.
    pub key: String,
    /// The current version id the tag is attached to.
    pub version_id: String,
    /// The object's logical size in bytes.
    pub size: u64,
    /// When the current version was last modified.
    pub last_modified: Timestamp,
}
