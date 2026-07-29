//! Metadata-store value types: the typed mutation enum that rides the group-committing
//! writer, its outcomes, listing queries/pages, multipart sessions, the replication outbox,
//! and user/credential records.

use crate::authz::{Acl, OwnershipMode, PublicAccessBlock};
use crate::bucket::{
    Bucket, CompressionPolicy, ConfigAspect, ConfigDoc, DefaultRetention, VersioningState,
};
use crate::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use crate::object::{
    ChecksumValue, ETag, ExplicitObjectLockIntent, GovernanceBypass, ObjectVersionRow,
    StorageClass, UserMetadata,
};
use crate::time::Timestamp;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

/// Tags and explicit Object Lock intent installed atomically with a new object version.
///
/// Keeping these side rows inside the object commit mutation prevents a visible version from
/// escaping without its requested tags/retention/legal hold, and makes a late side-row failure roll
/// the version and replication outbox back together.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InitialObjectState {
    /// Initial object tags. Empty means replace the version's tag set with empty.
    pub tags: Vec<(String, String)>,
    /// Explicit retention/legal-hold request. Bucket defaults are resolved by the writer.
    pub lock_intent: ExplicitObjectLockIntent,
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
        /// Tags and explicit Object Lock state committed with the version.
        initial_state: InitialObjectState,
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
        /// Trusted governance-bypass decision for replacing a protected sentinel version.
        bypass: GovernanceBypass,
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
        /// Optional compare-and-delete guard: only delete if the stored version's `updated_at` still
        /// equals this value. `None` = unconditional (client `DeleteObject`). The lifecycle scanner
        /// captures a version's `updated_at` at enumeration and passes it here so a concurrent
        /// overwrite landing between the scan and the delete is a NO-OP rather than destroying the
        /// fresh object — the current-object-expiration TOCTOU (audit 2026-07). Evaluated inside the
        /// delete's savepoint, so the check and the delete are atomic.
        expected_updated_at: Option<Timestamp>,
        /// Trusted current time used to evaluate retention inside the writer savepoint.
        now: Timestamp,
        /// Trusted governance-bypass authorization.
        bypass: GovernanceBypass,
    },
    /// Create a multipart session, enforcing the active-session limits atomically with the insert.
    CreateMultipart {
        /// The session to create.
        session: Box<MultipartSession>,
        /// Bounded multipart cardinality limits.
        limits: MultipartLimits,
    },
    /// Reserve quota for one exact multipart-part attempt before any bytes are staged.
    ///
    /// `attempt_id` is also the blob store's deterministic part-attempt identifier, so a cleanup
    /// worker can unlink the intended artifact even when staging fails after file creation but
    /// before returning a [`StoragePath`].
    ReserveMultipartPart {
        /// The session.
        upload_id: UploadId,
        /// The S3 part number.
        part_number: u16,
        /// Unique identifier for this upload attempt.
        attempt_id: String,
        /// Exact declared plaintext payload bytes to reserve.
        reserved_bytes: u64,
        /// Maximum distinct part numbers allowed for the session.
        max_parts_per_upload: u16,
        /// Reservation creation time.
        now: Timestamp,
    },
    /// Release an uncommitted part reservation after its artifact is proven absent.
    ReleaseMultipartReservation {
        /// The session.
        upload_id: UploadId,
        /// The attempt reservation.
        attempt_id: String,
    },
    /// Record (or supersede) a part by consuming its pre-stage reservation.
    RecordPart {
        /// The session.
        upload_id: UploadId,
        /// The reservation/part-attempt identifier.
        attempt_id: String,
        /// The part.
        part: PartRecord,
    },
    /// Release one exact-path multipart cleanup debt after unlink succeeds.
    ReleaseMultipartCleanup {
        /// The cleanup debt id.
        cleanup_id: String,
    },
    /// Release every cleanup debt for one upload after its complete staging directory is removed.
    ReleaseMultipartUploadCleanups {
        /// The upload whose directory was removed.
        upload_id: UploadId,
    },
    /// Release a bounded page of crash-orphaned reservations and cleanup debts.
    ///
    /// This may run only after a complete, successful filesystem reconciliation. A partial or
    /// failed walk must never release accounting for bytes it did not prove absent.
    RecoverMultipartStagingAccounting {
        /// Maximum ledger rows to release in this writer transaction.
        limit: u32,
    },
    /// Atomically claim a session for completion (guards double completion).
    ClaimMultipart(UploadId),
    /// Release a failed completion claim (`completing` -> `active`) so the client can retry.
    ///
    /// The transition is conditional inside the writer savepoint: a caller that no longer owns a
    /// `completing` session cannot resurrect an aborted or already-completed upload.
    ReleaseMultipartClaim(UploadId),
    /// Recover every orphaned multipart completion claim (`completing` -> `active`).
    ///
    /// Run once before listeners bind. A newly-started process has no surviving request that could
    /// still own a `completing` session, so retaining that transient state would strand the upload
    /// forever. Global across shards, idempotent, and leaves parts/session metadata intact.
    RecoverMultipartClaims,
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
    /// Abort an active multipart session. A completion owner (`completing`) wins atomically.
    AbortMultipart(UploadId),
    /// Create a bucket.
    CreateBucket(Box<Bucket>),
    /// Atomically create an Object-Lock-enabled, versioning-enabled bucket and its immutable
    /// enabled configuration row.
    CreateObjectLockBucket(Box<Bucket>),
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
    /// Update the default retention of an already Object-Lock-enabled bucket.
    ///
    /// This specialized mutation deliberately repairs malformed/disabled legacy configuration
    /// rows but cannot enable Object Lock on a bucket that has no Object Lock row.
    UpdateObjectLockConfiguration {
        /// Target bucket.
        bucket: BucketName,
        /// The new default retention, or `None` for enabled Object Lock without a default.
        default_retention: Option<DefaultRetention>,
    },
    /// Set or clear the Object Lock retention on one object version (preserving any legal hold).
    ///
    /// The metadata writer is the final authority for future-date validation, COMPLIANCE
    /// non-weakening, and GOVERNANCE bypass. Callers pass only the trusted authorization result.
    SetObjectRetention {
        /// Target bucket.
        bucket: BucketName,
        /// Target key.
        key: ObjectKey,
        /// Target version.
        version_id: VersionId,
        /// The retention to apply, or None to clear it.
        retention: Option<crate::object::ObjectRetention>,
        /// Trusted current time used for future-date and weakening checks.
        now: Timestamp,
        /// Trusted governance-bypass authorization.
        bypass: GovernanceBypass,
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
    /// Permanently delete a user and everything that lets them act: their record (which carries the
    /// identity policy column) and their session credentials. Removing the record evicts the
    /// authenticator's cached principal, so access is denied immediately. The user's usage accounting
    /// (`user_stats`) is deliberately **preserved** — it must stay equal to the sum of the user's
    /// still-owned object sizes (an enforced integrity invariant), so deleting it would corrupt the
    /// quota ledger. Callers must first ensure the user owns no buckets (buckets cannot be orphaned)
    /// and is not the last administrator.
    DeleteUser(UserId),
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
    ///
    /// The version-row UPDATE sets `replication_status = Completed` **and**
    /// [`replicated_at`](crate::object::ObjectVersionRow::replicated_at) `= now` in the same
    /// statement (schema v23). They must move together: the status alone cannot distinguish a
    /// version that shipped before a fix from one that was force-requeued and has since re-shipped
    /// correctly, which is precisely what the replication audit has to decide. A `replica` row
    /// (inbound, loop prevention ARCH 20.4) is left untouched by both.
    MarkReplicationDone {
        /// The outbox entry id.
        id: String,
        /// The completion time to stamp on the version row.
        now: Timestamp,
    },
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
    /// Force a bucket's already-terminal replication work back into the queue so it is **re-shipped
    /// unconditionally** — the operator remediation for versions that replicated *successfully* but
    /// wrongly (ARCH 20.5, the pre-release-X SSE plaintext-seam incident: an encrypted version was
    /// shipped as raw ciphertext and the destination accepted it, so the replica exists, is the
    /// right size, answers 200, and is garbage).
    ///
    /// Two DML statements, no schema change:
    /// 1. every `completed`/`failed` outbox row for the bucket goes back to `pending` with
    ///    `attempts=0`, `next_attempt_at=now` and the lease cleared — this is what
    ///    [`EnqueueReplication`](Self::EnqueueReplication) *cannot* do, because its `INSERT OR
    ///    IGNORE` on the deterministic `backfill:{rule}:{key}:{version}` id silently no-ops while a
    ///    row for that id still exists (i.e. for the whole
    ///    `CAIRN_REPLICATION_RETENTION_SECS` window a second resync repairs nothing);
    /// 2. every matching version row's `replication_status` goes back to `pending`, so the durable
    ///    ledger stops claiming the object is replicated. `replica` rows are never touched (loop
    ///    prevention, ARCH 20.4).
    ///
    /// Rows whose outbox entry was already pruned are covered by the resync backfill that runs
    /// after this mutation — for those, `INSERT OR IGNORE` genuinely inserts. The two halves
    /// together are what make a repair pass complete.
    ///
    /// ## The LEDGER half is narrower than the OUTBOX half
    ///
    /// `pending` on a version row is a promise that something will ship that version, and the
    /// replication audit reports it as `repair_pending` — "repair in flight". Only two populations
    /// can honour that promise: rows that are **current** (the resync backfill enumerates current
    /// objects, so they get a fresh entry) and rows that still **have** an outbox entry for their
    /// exact `(bucket, key, version_id)` — which statement 1 has just requeued.
    ///
    /// A NON-CURRENT version whose outbox row the retention sweep already pruned has neither, and 24
    /// h after an incident that is essentially all of them. Marking it `pending` would make the
    /// ledger claim queued work that no queue holds: it would sit there forever, `repair_pending`
    /// could never reach zero, the runbook's done-state would be unreachable, and the alert
    /// `docs/operations.md` 8.7 prescribes on that gauge would fire permanently. So statement 2
    /// skips it and it stays `completed` — still reported as a suspect replica, which is the truth,
    /// and which the runbook's TRAP 2 already explains is unrepairable without rebuilding the
    /// destination bucket. Statement 1 is **not** narrowed: key atomicity there is a correctness
    /// property (see below).
    ///
    /// ## The scope is the KEY, never the single version
    ///
    /// `only_encrypted` selects **keys that have at least one encrypted terminal version**, and then
    /// requeues *every* terminal row of those keys. Filtering per version would be a correctness
    /// bug, not an optimisation:
    ///
    /// * key `k` with `v1` encrypted (`completed`) and a later **plaintext** `v2` (`completed`) —
    ///   e.g. `CAIRN_ENCRYPT_AT_REST` was turned off, or the key was rewritten without SSE. Requeue
    ///   only `v1` and it is PUT last at the destination, so the mirror's current object silently
    ///   becomes the **old** version;
    /// * key `k` with `v1` encrypted (`completed`) and a `v2` **delete marker** (`completed`, no
    ///   descriptor). Requeue only `v1` — and the resync backfill enumerates *current* objects, so
    ///   it never re-enqueues the marker — and the mirror **resurrects a deleted object**.
    ///
    /// With every version of the key queued, `has_unreplicated_predecessor` re-establishes the
    /// write-order guarantee (an earlier version outstanding defers the later one), which is exactly
    /// what makes the key-level scope the correct one rather than merely the wider one.
    ///
    /// ## Bounded — but the batch is a page of KEYS, never a page of rows
    ///
    /// An unbounded UPDATE on a large bucket would hold the group-commit transaction — and
    /// therefore every write on the node — for as long as a full-table scan takes, while growing the
    /// WAL. So the work is paged. **The page unit is the key, and this is a correctness
    /// requirement, not a tuning choice.**
    ///
    /// A row-bounded page (`WHERE rowid IN (SELECT rowid … LIMIT ?)`) reintroduces the very
    /// ordering bug the key-level scope exists to prevent. It has no `ORDER BY`, so SQLite serves
    /// rows in whatever order the cheapest index gives — `(status, next_attempt_at)`, which groups
    /// **all** `completed` rows ahead of **all** `failed` rows. A key whose OLDER encrypted version
    /// is `failed` and whose NEWER version is `completed` then gets the newer row requeued in an
    /// early page and the older one many pages later; the replication heartbeat ships the newer one
    /// in between, and the mirror reverts to the old bytes (or resurrects a deleted key, when the
    /// newer row is a delete marker). Only buckets larger than one page are affected — i.e.
    /// precisely the ones paging exists for.
    ///
    /// So each apply:
    /// 1. selects the next page of at most `limit` **distinct keys** with terminal work, ordered by
    ///    key, strictly after `after_key`, from the UNION of the outbox and the version ledger (a
    ///    key whose outbox row was already pruned still has a ledger stamp to reset);
    /// 2. runs BOTH UPDATEs over the closed key range `(after_key, page_end]` — so every terminal
    ///    row of every key in the page moves in the SAME transaction as its siblings, and the queue
    ///    and the ledger cannot disagree about a key;
    /// 3. returns [`MutationOutcome::RowsRequeued`] carrying the rows changed **and** `page_end`.
    ///
    /// The caller threads `page_end` back as `after_key`, so progress is monotone forward and no
    /// pass ever rescans what an earlier pass drained (re-deriving the page by scanning from the
    /// start each time would be quadratic on exactly the large buckets this exists for). A `None`
    /// `page_end` means the bucket is drained and the caller stops.
    ///
    /// Per-transaction write volume is bounded by `limit` keys × that key's version count. The one
    /// exception is a pathological single key with an enormous version history: its rows are
    /// **never** split across transactions, because splitting them is the bug above. That is a
    /// deliberate, documented unbounded case, not an oversight.
    RequeueReplicationVersions {
        /// The source bucket to requeue. Always bucket-scoped: this is a deliberately destructive
        /// re-ship, never a store-wide sweep.
        bucket: BucketName,
        /// Restrict to keys that have at least one version carrying an `sse_descriptor` (the
        /// encrypted-only blast radius of the incident). `false` requeues every terminal entry in
        /// the bucket. See the type doc for why this is key-scoped and not version-scoped.
        only_encrypted: bool,
        /// The time to schedule the requeued entries at (immediately due).
        now: Timestamp,
        /// Forward cursor: resume strictly **after** this key. `None` starts at the beginning of the
        /// bucket's keyspace. The caller feeds back the `page_end` of the previous pass.
        after_key: Option<String>,
        /// Maximum number of distinct **keys** this one transaction may page over (not rows — see
        /// the variant doc for why a row bound is a correctness bug).
        limit: u32,
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
    /// Reclaim terminally-`failed` webhook-outbox (`events_outbox`) rows older than `before_ms` (by
    /// `next_attempt_at`). Delivered/dropped entries are removed on `MarkWebhookDone`, so only
    /// `failed` rows accumulate; without this the table grows one permanent JSON-payload row per
    /// failed object event, bloating the metadata DB (the single source of truth) — an availability
    /// DoS reachable by a misconfigured sink over time. Mirrors [`PruneReplicationOutbox`] for the
    /// webhook engine (ARCH 20.3 bounded-work-queue contract).
    PruneEventsOutbox {
        /// Delete failed entries whose `next_attempt_at` is before this wall-clock millis.
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
    /// Reclaim ALL `claimed` replication-outbox entries back to `pending` (clearing the lease). Run
    /// ONCE at startup: a freshly-started process has no live workers, so every `claimed` row is an
    /// orphan left by a worker that crashed mid-ship. Without this, those entries would wait out the
    /// full 300 s claim lease before any worker could re-claim them — so a node that crashes
    /// mid-drain would take minutes to resume the objects it was actively shipping. Idempotent and
    /// safe because each node owns its own metadata store. Does not touch `attempts`.
    RecoverClaimedReplication,
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
    /// Create a persistent object share whose bearer token has already been reduced to a
    /// domain-separated lookup hash (ARCH 15.8).
    CreateShare(Box<ShareRow>),
    /// Revoke a share by its stable, non-secret management id (idempotent).
    RevokeShare {
        /// The stable share id to revoke.
        id: String,
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
    /// Create an S3 import job (its source secret already sealed under the master key), in state
    /// `Pending`. The background import loop claims and runs it. Returns [`MutationOutcome::Ack`]
    /// (the id is minted by the control handler, mirroring the replication-target ARN).
    CreateImportJob(Box<ImportJobRecord>),
    /// Persist an import job's progress: the per-bucket cursors/counters (`buckets`), the denormalized
    /// aggregate counters, and a renewed running lease. Emitted by the engine's throttled checkpoint
    /// callback while a job runs. Column-scoped `UPDATE ... WHERE id = ?` (never touches `state`).
    UpdateImportJobProgress {
        /// The job id.
        id: String,
        /// The per-bucket progress (serialized to the `buckets_json` column).
        buckets: Vec<ImportBucketProgress>,
        /// Denormalized total objects copied so far.
        objects_done: u64,
        /// Denormalized total objects to copy (best-effort; may grow as enumeration proceeds).
        objects_total: u64,
        /// Denormalized total bytes copied so far.
        bytes_done: u64,
        /// Denormalized total bytes to copy (best-effort).
        bytes_total: u64,
        /// The most recent per-object error sample, if any.
        last_error: Option<String>,
        /// Renewed claim lease (`None` clears it).
        lease_until: Option<Timestamp>,
        /// The update time.
        updated_at: Timestamp,
    },
    /// Transition an import job's lifecycle state: `Pending -> Running` (claim, stamps the lease),
    /// `-> Completed`/`Failed` (terminal), or `-> Cancelled`/back to `Pending` (operator resume).
    /// Column-scoped `UPDATE ... WHERE id = ?`.
    SetImportJobState {
        /// The job id.
        id: String,
        /// The new state.
        state: ImportState,
        /// An optional error/status message to record.
        last_error: Option<String>,
        /// The claim lease (`Some` when moving to `Running`, `None` otherwise).
        lease_until: Option<Timestamp>,
        /// The update time.
        updated_at: Timestamp,
    },
    /// Reclaim finished (`completed`/`failed`/`cancelled`) import jobs whose `updated_at` is before
    /// `before_ms`, keeping the table bounded. Running/pending jobs are never pruned. One mutation
    /// deletes at most `limit` rows so retention maintenance cannot monopolize the single writer.
    PruneImportJobs {
        /// Delete finished jobs updated before this wall-clock millis.
        before_ms: i64,
        /// Maximum rows to delete in this writer transaction (backend-clamped to
        /// [`MAX_IMPORT_JOB_PRUNE_BATCH`]).
        limit: u32,
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
        /// Any sentinel blob replaced by this marker.
        freed: Option<StoragePath>,
    },
    /// A version was permanently deleted.
    Deleted {
        /// The freed blob, if the version referenced one.
        freed: Option<StoragePath>,
        /// Whether a successor was promoted to latest.
        promoted_latest: bool,
    },
    /// The permanent-delete target was already absent or its compare-and-delete guard no longer
    /// matched, so no metadata changed. S3 DELETE remains idempotent, while maintenance callers
    /// can distinguish this lost race from a writer-confirmed deletion.
    DeleteNotApplied,
    /// A permanent delete or sentinel replacement was denied by Object Lock. No metadata changed.
    DeleteProtected,
    /// A multipart session was created.
    MultipartCreated(UploadId),
    /// Quota was reserved for a multipart part attempt before staging.
    MultipartReserved,
    /// A part was recorded.
    PartRecorded {
        /// Any superseded part cleanup debt. Its bytes remain charged until the path is unlinked
        /// and [`Mutation::ReleaseMultipartCleanup`] commits.
        cleanup: Option<MultipartCleanup>,
    },
    /// Rows released by one bounded [`Mutation::RecoverMultipartStagingAccounting`] transaction.
    MultipartAccountingReleased(u64),
    /// A claim attempt resolved.
    MultipartClaim(ClaimOutcome),
    /// A failed completion claim was released (or rejected because the caller was not its owner).
    MultipartClaimRelease(ClaimReleaseOutcome),
    /// An abort or completion terminal transition resolved.
    MultipartTerminal(MultipartTerminalOutcome),
    /// A batch of due replication entries was claimed.
    ReplicationBatch(Vec<OutboxEntry>),
    /// A batch of due webhook-notification entries was claimed.
    WebhookBatch(Vec<WebhookEntry>),
    /// A user was created.
    UserCreated(UserId),
    /// The result of one key-paged [`Mutation::RequeueReplicationVersions`] batch: how many rows it
    /// changed (both statements summed) and the last key it covered.
    ///
    /// `page_end` is the forward cursor — the caller feeds it back as `after_key` on the next pass,
    /// which is what makes progress monotone and keeps a full requeue linear rather than quadratic.
    /// `None` means the page was empty, i.e. the bucket is drained and the caller stops. `rows` is
    /// reporting only: a non-empty page with zero changed rows is possible in principle and must
    /// **not** terminate the loop, or the requeue silently stops half-way.
    RowsRequeued {
        /// Rows changed by this batch, outbox + ledger.
        rows: u64,
        /// The last key covered by this batch, or `None` when the page was empty (drained).
        page_end: Option<String>,
    },
    /// Rows deleted by one bounded [`Mutation::PruneImportJobs`] transaction.
    ImportJobsPruned(u64),
    /// A generic acknowledgement for mutations with no specific return value.
    Ack,
}

// ---------------------------------------------------------------------------------------
// Import (migrating buckets in from another S3-compatible store)
// ---------------------------------------------------------------------------------------

/// The lifecycle state of an import job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImportState {
    /// Created, awaiting a worker.
    Pending,
    /// Claimed and actively copying.
    Running,
    /// Finished; every selected bucket was enumerated and its objects copied (possibly with a
    /// non-zero per-object failed count — see [`ImportBucketProgress::last_error`]).
    Completed,
    /// Terminally failed (e.g. the source became unreachable past the retry budget).
    Failed,
    /// Cancelled by an operator; resumable back to `Pending`.
    Cancelled,
}

/// Per-bucket progress within an import job, serialized as an element of the job's `buckets_json`
/// column. `cursor` is the source `ListObjectsV2` continuation token to resume from, so a restart
/// picks up mid-bucket instead of re-scanning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportBucketProgress {
    /// The source bucket name.
    pub source_bucket: String,
    /// The destination bucket on this node.
    pub dest_bucket: String,
    /// Objects copied so far in this bucket.
    pub objects_done: u64,
    /// Objects seen so far (grows as enumeration proceeds; a running best-effort total).
    pub objects_total: u64,
    /// Bytes copied so far in this bucket.
    pub bytes_done: u64,
    /// Bytes seen so far.
    pub bytes_total: u64,
    /// The `ListObjectsV2` continuation token to resume enumeration from; `None` = not started or
    /// fully enumerated.
    pub cursor: Option<String>,
    /// This bucket's state.
    pub state: ImportState,
    /// A bounded sample of the most recent per-object error, if any.
    pub last_error: Option<String>,
}

/// A full import-job record including the sealed source secret, for the create/update mutations.
/// Mirrors the sealed-secret shape of [`SessionCredentialRecord`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportJobRecord {
    /// The job id (a uuid minted by the control plane).
    pub id: String,
    /// The remote S3 endpoint base URL.
    pub source_endpoint: String,
    /// The SigV4 signing region for the source.
    pub source_region: String,
    /// The source admin access-key id (public; not a secret).
    pub access_key_id: String,
    /// The sealed source secret (`CRK1` envelope; `secret_nonce` is `None`).
    pub secret_ciphertext: Vec<u8>,
    /// The legacy ciphertext nonce (`None` for a `CRK1` envelope).
    pub secret_nonce: Option<Vec<u8>>,
    /// An optional PEM CA bundle to trust for an `https://` source.
    pub ca_cert_pem: Option<String>,
    /// Whether to skip TLS verification for the source (testing only).
    pub insecure_skip_verify: bool,
    /// The requested object-worker count.
    pub workers: u32,
    /// The job state.
    pub state: ImportState,
    /// Per-bucket progress + cursors.
    pub buckets: Vec<ImportBucketProgress>,
    /// Denormalized aggregate: objects copied across all buckets (cheap list rendering).
    pub objects_done: u64,
    /// Denormalized aggregate: objects seen across all buckets.
    pub objects_total: u64,
    /// Denormalized aggregate: bytes copied.
    pub bytes_done: u64,
    /// Denormalized aggregate: bytes seen.
    pub bytes_total: u64,
    /// A job-level error/status message, if any.
    pub last_error: Option<String>,
    /// The running-job claim lease (`None` when not claimed); a stale lease is reclaimable at startup.
    pub lease_until: Option<Timestamp>,
    /// When the job was created.
    pub created_at: Timestamp,
    /// When the job was last updated.
    pub updated_at: Timestamp,
}

/// An import job as returned to the control plane — the **secret-free** view (no ciphertext/nonce).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportJob {
    /// The job id.
    pub id: String,
    /// The remote S3 endpoint base URL.
    pub source_endpoint: String,
    /// The SigV4 signing region for the source.
    pub source_region: String,
    /// The source admin access-key id.
    pub access_key_id: String,
    /// Whether a custom CA certificate is configured (presence flag; the PEM is not returned).
    pub has_ca_cert: bool,
    /// Whether TLS verification is skipped for the source.
    pub insecure_skip_verify: bool,
    /// The object-worker count.
    pub workers: u32,
    /// The job state.
    pub state: ImportState,
    /// Per-bucket progress.
    pub buckets: Vec<ImportBucketProgress>,
    /// Aggregate objects copied.
    pub objects_done: u64,
    /// Aggregate objects seen.
    pub objects_total: u64,
    /// Aggregate bytes copied.
    pub bytes_done: u64,
    /// Aggregate bytes seen.
    pub bytes_total: u64,
    /// A job-level error/status message, if any.
    pub last_error: Option<String>,
    /// When the job was created.
    pub created_at: Timestamp,
    /// When the job was last updated.
    pub updated_at: Timestamp,
}

/// An import job summary for history pages.
///
/// Unlike [`ImportJob`], this type deliberately has no per-bucket progress vector. History listing
/// therefore never selects or decodes `buckets_json`; callers fetch one job's detail explicitly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportJobSummary {
    /// The job id.
    pub id: String,
    /// The remote S3 endpoint base URL.
    pub source_endpoint: String,
    /// The SigV4 signing region for the source.
    pub source_region: String,
    /// The source admin access-key id.
    pub access_key_id: String,
    /// Whether a custom CA certificate is configured (presence flag; the PEM is not returned).
    pub has_ca_cert: bool,
    /// Whether TLS verification is skipped for the source.
    pub insecure_skip_verify: bool,
    /// The object-worker count.
    pub workers: u32,
    /// The job state.
    pub state: ImportState,
    /// Aggregate objects copied.
    pub objects_done: u64,
    /// Aggregate objects seen.
    pub objects_total: u64,
    /// Aggregate bytes copied.
    pub bytes_done: u64,
    /// Aggregate bytes seen.
    pub bytes_total: u64,
    /// A job-level error/status message, if any.
    pub last_error: Option<String>,
    /// When the job was created.
    pub created_at: Timestamp,
    /// When the job was last updated.
    pub updated_at: Timestamp,
}

impl ImportJobRecord {
    /// The secret-free [`ImportJob`] view: drops the sealed secret material and exposes only a
    /// `has_ca_cert` presence flag. Used by every read path so a secret can never leak to the API.
    #[must_use]
    pub fn to_view(&self) -> ImportJob {
        ImportJob {
            id: self.id.clone(),
            source_endpoint: self.source_endpoint.clone(),
            source_region: self.source_region.clone(),
            access_key_id: self.access_key_id.clone(),
            has_ca_cert: self.ca_cert_pem.is_some(),
            insecure_skip_verify: self.insecure_skip_verify,
            workers: self.workers,
            state: self.state,
            buckets: self.buckets.clone(),
            objects_done: self.objects_done,
            objects_total: self.objects_total,
            bytes_done: self.bytes_done,
            bytes_total: self.bytes_total,
            last_error: self.last_error.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    /// The secret-free, bucket-progress-free history summary.
    #[must_use]
    pub fn to_summary(&self) -> ImportJobSummary {
        ImportJobSummary {
            id: self.id.clone(),
            source_endpoint: self.source_endpoint.clone(),
            source_region: self.source_region.clone(),
            access_key_id: self.access_key_id.clone(),
            has_ca_cert: self.ca_cert_pem.is_some(),
            insecure_skip_verify: self.insecure_skip_verify,
            workers: self.workers,
            state: self.state,
            objects_done: self.objects_done,
            objects_total: self.objects_total,
            bytes_done: self.bytes_done,
            bytes_total: self.bytes_total,
            last_error: self.last_error.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

/// Hard ceiling for one import-history page at the metadata seam.
///
/// Enforcing this below the management API keeps every backend bounded even if a future internal
/// caller forgets to validate its requested limit.
pub const MAX_IMPORT_JOB_PAGE_SIZE: u32 = 1_000;

/// Hard ceiling on terminal import jobs removed by one writer transaction.
pub const MAX_IMPORT_JOB_PRUNE_BATCH: u32 = 1_000;

/// Stable keyset cursor for the import history's `(created_at DESC, id DESC)` order.
///
/// The id is the tie-breaker: timestamps are millisecond resolution, so creation time alone would
/// skip or duplicate jobs created in the same millisecond.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportJobCursor {
    /// Creation time of the last job returned by the previous page.
    pub created_at: Timestamp,
    /// Id of the last job returned by the previous page.
    pub id: String,
}

/// A bounded import-history listing query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportJobListQuery {
    /// Resume strictly after this `(created_at, id)` pair in newest-first order.
    pub cursor: Option<ImportJobCursor>,
    /// Requested page size. Backends clamp this to [`MAX_IMPORT_JOB_PAGE_SIZE`] and treat zero as
    /// one, preserving the bounded-read invariant independently of the caller.
    pub limit: u32,
}

impl ImportJobListQuery {
    /// The page size enforced by every metadata backend.
    #[must_use]
    pub fn bounded_limit(&self) -> u32 {
        self.limit.clamp(1, MAX_IMPORT_JOB_PAGE_SIZE)
    }
}

impl Default for ImportJobListQuery {
    fn default() -> Self {
        Self {
            cursor: None,
            limit: MAX_IMPORT_JOB_PAGE_SIZE,
        }
    }
}

/// One keyset-paginated page of secret-free import jobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportJobPage {
    /// Jobs in stable `(created_at DESC, id DESC)` order.
    pub items: Vec<ImportJobSummary>,
    /// Cursor derived from the last returned item when another page exists.
    pub next_cursor: Option<ImportJobCursor>,
}

impl ImportJobPage {
    /// Build a page from a backend result ordered newest first and fetched at `limit + 1`.
    ///
    /// Keeping the truncation/cursor rule here makes SQLite, libSQL/Turso, sharding, cache, and the
    /// in-memory double expose identical page boundaries.
    #[must_use]
    pub fn from_overfetch(mut items: Vec<ImportJobSummary>, limit: u32) -> Self {
        let limit = limit.clamp(1, MAX_IMPORT_JOB_PAGE_SIZE) as usize;
        let truncated = items.len() > limit;
        items.truncate(limit);
        let next_cursor = if truncated {
            items.last().map(|last| ImportJobCursor {
                created_at: last.created_at,
                id: last.id.clone(),
            })
        } else {
            None
        };
        Self { items, next_cursor }
    }
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
    /// Continuation cursor (the last key returned). For multipart-upload listings this is the S3
    /// `key-marker`.
    pub cursor: Option<String>,
    /// Secondary marker WITHIN the `cursor` key: resume strictly after `(cursor, marker)` so a key
    /// whose entries span a page boundary continues mid-key. The version id for version listings,
    /// the S3 `upload-id-marker` for multipart-upload listings. Ignored unless `cursor` is also set
    /// (the key it pairs with). `None` resumes at the key boundary.
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
    /// The boundary secondary marker to resume after, for a listing truncated mid-key: the version
    /// id for a version listing, the upload id (S3 `NextUploadIdMarker`) for a multipart-upload
    /// listing. Threads back as the next request's [`ListQuery::version_id_marker`] (paired with
    /// `next_cursor` as the key) so a key whose entries span a page boundary continues strictly
    /// after the last returned one. `None` for current-object listings and for version listings
    /// truncated on a key boundary (the next page resumes at the next key).
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

/// Writer-enforced cardinality limits for active multipart state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartLimits {
    /// Maximum active uploads in one bucket.
    pub max_active_uploads_per_bucket: u32,
    /// Maximum active uploads attributed to one initiating principal.
    pub max_active_uploads_per_principal: u32,
    /// Maximum distinct part numbers in one upload (never above S3's 10,000).
    pub max_parts_per_upload: u16,
}

impl Default for MultipartLimits {
    fn default() -> Self {
        Self {
            max_active_uploads_per_bucket: 1_000,
            max_active_uploads_per_principal: 1_000,
            max_parts_per_upload: 10_000,
        }
    }
}

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
    /// The authenticated principal that initiated the upload.
    ///
    /// This is deliberately distinct from `owner_id`: bucket ownership rules can make the final
    /// object belong to the bucket owner even when an administrator or delegated writer supplied
    /// the staged bytes. Multipart session limits and staged-byte user quota are attributed to the
    /// actual initiator.
    pub initiated_by: UserId,
    /// The ACL to apply on completion.
    pub intended_acl: Option<Acl>,
    /// The metadata to apply on completion.
    pub user_metadata: UserMetadata,
    /// Object tags pinned at initiation and installed atomically at completion.
    pub initial_tags: Vec<(String, String)>,
    /// Explicit Object Lock intent pinned at initiation. Bucket defaults are deliberately resolved
    /// from the then-current configuration at completion rather than pinned here.
    pub lock_intent: ExplicitObjectLockIntent,
    /// Whether SSE-S3 was requested for this upload at initiate (via the request header or the
    /// bucket default-encryption setting). Captured at initiate because the `CompleteMultipartUpload`
    /// request carries no SSE header; honored at completion so the assembled object is encrypted at
    /// rest exactly like a single-part PUT (ARCH 27).
    pub sse_requested: bool,
    /// The part-encryption decision pinned at initiate (ARCH 27, Increment 3a). When `true`, every
    /// `UploadPart`/`UploadPartCopy` of this session mints a fresh per-part DEK and stages the part
    /// as a CRNB `VERSION_ENCRYPTED` blob, so nothing plaintext hits disk; the assembled object is
    /// a decrypt-then-re-encrypt pass. Computed by a cheap predicate at initiate (explicit AES256, a
    /// bucket default of any mode, or `CAIRN_ENCRYPT_AT_REST`) that mints no DEK and never validates
    /// a KMS key id — distinct from `sse_requested`, which drives the object's advertised mode at
    /// complete. A pre-v21 in-flight session reads `false` and completes via the legacy
    /// plaintext-parts -> encrypt-at-assemble path.
    pub encrypt_parts: bool,
    /// Whether an explicit `x-amz-server-side-encryption: aws:kms` header was accepted at initiate
    /// (ARCH 27, Increment 3b). Captured because `CompleteMultipartUpload` carries no SSE header;
    /// when `true` the assembled object advertises `aws:kms` + its key id. Distinct from
    /// `sse_requested` (explicit SSE-S3) — a session sets at most one. A pre-v22 in-flight session
    /// reads `false` and completes via the SSE-S3 / bucket-default path unchanged.
    pub sse_kms_requested: bool,
    /// The validated KMS key-id label to advertise on the assembled object (`None` = the default
    /// key). The key id is validated at initiate (fail-closed) and is a LABEL only — the same
    /// master-sealed DEK is used for every id; no external KMS / network (ARCH 27).
    pub sse_kms_key_id: Option<String>,
    /// Whether to echo `x-amz-server-side-encryption-bucket-key-enabled: true` on the assembled
    /// object; only meaningful when `sse_kms_requested` is `true` (ARCH 27, Increment 3b).
    pub sse_bucket_key_enabled: bool,
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

/// Fixed-size lookup key derived from an object-share bearer token.
///
/// The domain separator prevents a token hash from being interchangeable with any other SHA-256
/// use in Cairn. This is an indexed lookup key, not a password verifier: share tokens carry 256
/// random bits, so a fast hash does not make offline guessing practical. `Debug` is deliberately
/// redacted so an enclosing domain value cannot disclose even the lookup material.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShareLookupHash([u8; Self::LEN]);

impl ShareLookupHash {
    /// Encoded width of the hash stored in SQLite/libSQL/Turso.
    pub const LEN: usize = 32;
    const DOMAIN: &'static [u8] = b"cairn:object-share-token:v1\0";

    /// Derive the lookup hash for a raw bearer token.
    #[must_use]
    pub fn for_token(token: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(Self::DOMAIN);
        hasher.update(token.as_bytes());
        Self(hasher.finalize().into())
    }

    /// Decode an exact-width database BLOB.
    #[must_use]
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok().map(Self)
    }

    /// Borrow the fixed-width database representation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; Self::LEN] {
        &self.0
    }
}

impl std::fmt::Debug for ShareLookupHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ShareLookupHash(<redacted>)")
    }
}

/// A persistent, revocable, optionally-forever object share (ARCH 15.8).
///
/// The raw bearer capability is returned only by the mint operation and is never placed in this
/// row. Redemption hashes `/share/{token}` and performs an indexed lookup by `token_hash`, while
/// management list/get/revoke operations use the stable non-secret `id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareRow {
    /// Stable, non-secret management identifier; the table primary key.
    pub id: String,
    /// Domain-separated SHA-256 lookup hash of the opaque bearer token.
    pub token_hash: ShareLookupHash,
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
    /// The part's 32-byte DEK when it was staged encrypted (ARCH 27, Increment 3a), sealed under the
    /// master ring (base64 CRK1 envelope) — opaque to the metadata layer. `None` = a plaintext part
    /// (legacy / pre-v21). Consumed at `CompleteMultipartUpload` to decrypt the part before it is
    /// re-encoded under the object DEK; it never enters the object rewrap stream.
    pub part_dek: Option<String>,
}

/// One durable pre-stage multipart byte reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartReservation {
    /// Unique attempt identifier, also used by the blob store to name the attempt artifact.
    pub attempt_id: String,
    /// The upload session.
    pub upload_id: UploadId,
    /// The S3 part number.
    pub part_number: u16,
    /// Reserved plaintext bytes.
    pub reserved_bytes: u64,
    /// When the reservation was created.
    pub created_at: Timestamp,
}

/// Durable accounting debt for multipart bytes whose metadata ownership ended before their
/// filesystem artifact was successfully reclaimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartCleanup {
    /// Stable cleanup id.
    pub id: String,
    /// The originating upload.
    pub upload_id: UploadId,
    /// Bucket charged for the bytes.
    pub bucket: BucketName,
    /// Initiating principal charged for the bytes.
    pub principal_id: UserId,
    /// Plaintext bytes that remain charged until cleanup succeeds.
    pub bytes: u64,
    /// Exact superseded part path, or `None` when the whole upload directory must be removed.
    pub storage_path: Option<StoragePath>,
    /// When the debt was created.
    pub created_at: Timestamp,
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

/// The outcome of releasing a failed multipart completion claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimReleaseOutcome {
    /// The owned `completing` session returned to `active` and is retryable.
    Released,
    /// The session was absent or was not in `completing`; no state changed.
    NotOwner,
}

/// The writer-atomic outcome of a multipart terminal transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartTerminalOutcome {
    /// Completion owned the `completing` session, committed the object, and consumed the session.
    Completed {
        /// Any superseded object blob to reclaim.
        superseded: Option<StoragePath>,
        /// The committed version id.
        version_id: VersionId,
    },
    /// Abort owned and removed an `active` session.
    Aborted,
    /// The session was absent or owned by the competing terminal operation; no state changed.
    NotOwner,
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
    /// identity policy). Current minting paths always store `Some`; a legacy `None` grants nothing
    /// and never inherits the parent's attached policy.
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
    /// `None` is inert; session authentication never loads the parent's policy.
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
    /// The timeline downsampling window, in seconds (for the web console to derive req/s).
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

#[cfg(test)]
mod share_lookup_hash_tests {
    use super::ShareLookupHash;

    #[test]
    fn share_lookup_hash_is_domain_stable_fixed_width_and_debug_redacted() {
        let sentinel = "cairn-share-debug-sentinel-029";
        let first = ShareLookupHash::for_token(sentinel);
        let second = ShareLookupHash::for_token(sentinel);
        let other = ShareLookupHash::for_token("another-token");

        assert_eq!(first, second);
        assert_ne!(first, other);
        assert_eq!(first.as_bytes().len(), ShareLookupHash::LEN);
        assert_eq!(ShareLookupHash::from_slice(first.as_bytes()), Some(first));
        assert!(ShareLookupHash::from_slice(&[0_u8; 31]).is_none());

        let debug = format!("{first:?}");
        assert_eq!(debug, "ShareLookupHash(<redacted>)");
        assert!(!debug.contains(sentinel));
    }
}
