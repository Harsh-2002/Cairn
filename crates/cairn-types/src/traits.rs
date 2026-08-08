//! The trait spine (ARCH 12). The protocol and control layers depend only on these
//! interfaces; every concrete backend (`cairn-meta`, `cairn-blob`, `cairn-crypto`,
//! `cairn-auth`, `cairn-authz`, `cairn-replication`) is replaceable, and the whole engine
//! is unit-testable against the in-memory doubles in [`crate::testing`].
//!
//! Async methods use `#[async_trait]` so the traits stay object-safe (dyn-compatible) and
//! their futures are `Send` on the multi-threaded runtime; the per-call boxing is negligible
//! against disk/network I/O. Zero-copy of object *bytes* is handled by the [`BlobReadHandle`]
//! fast-path hint, not by the control-flow futures.

use crate::auth::{AuthOutcome, RequestView};
use crate::authz::{AuthzInput, Decision, PublicAccessBlock};
use crate::blob::{
    BlobCipher, BlobProbe, BlobReadHandle, ByteRange, PartRef, ReconcileOpts, ReconcileReport,
    StageOptions, StagedBlob, StagedPart,
};
use crate::bucket::{Bucket, ConfigAspect, ConfigDoc};
use crate::crypto::{Nonce, Sealed, Signature};
use crate::error::{BlobError, CryptoError, MetaError, ReplicationError};
use crate::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use crate::meta::{
    ActivityEntry, BucketCounts, ImportJob, ImportJobListQuery, ImportJobPage, ImportJobRecord,
    ImportState, ListPage, ListQuery, MetricsRange, MultipartCleanup, MultipartReservation,
    MultipartSession, Mutation, MutationOutcome, ObjectSummary, OutboxEntry, PartRecord,
    ReplicationCounts, ReplicationStatus, RequestMetricsSeries, SessionCredentialSummary,
    ShareLookupHash, ShareRow, StoreCounts, TagSummary, TaggedObject, User, UserSessionCredentials,
    UserSigV4Credentials, UserWithBearerHash, WebhookEntry,
};
use crate::object::{ChecksumSet, CompressionDescriptor, ObjectVersionRow};
use crate::replication::ReplicatedObject;
use crate::secret::SecretKey32;
use crate::time::Timestamp;
use async_trait::async_trait;
use zeroize::Zeroizing;

/// The blob store owns object bytes on some medium and knows nothing of S3, identity, or
/// metadata. The local-filesystem implementation lives in `cairn-blob` and is the only place
/// that performs filesystem syscalls; the durable commit sequence (fsync file → rename →
/// fsync dir) is its invariant.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Stage a single object durably from a body stream, computing the plaintext MD5 and any
    /// requested checksums, applying compression, and enforcing the size ceiling. On `Ok` the
    /// blob is durable. Writes no metadata; does not verify client checksums.
    async fn stage(
        &self,
        bucket: &BucketName,
        body: crate::BodyStream,
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError>;

    /// Open a committed blob for reading, transparently decompressing and — when `cipher` is
    /// [`BlobCipher::LegacyV2`] or [`BlobCipher::AuthenticatedV3`] — transparently decrypting each
    /// AES-256-GCM block with the supplied raw 32-byte data-encryption key, optionally for a range
    /// expressed in logical (plaintext) coordinates.
    ///
    /// This is the **one** reader: there is no DEK-less `open`. The caller MUST name the cipher —
    /// [`BlobCipher::KnownPlaintext`] for a blob written in the clear, or one of the two named
    /// encrypted formats for ciphertext. That is deliberate. The former `open` forwarded to a DEK
    /// of `None`, so any
    /// caller that forgot the key streamed an encrypted container's raw ciphertext as if it were
    /// the plaintext body — silently, at exactly the plaintext length (it is how replication and
    /// the integrity scrub shipped ciphertext). Requiring the cipher by name removes that footgun.
    ///
    /// **Fail-closed (ARCH 27, SSE-S3):** a wrong DEK/format fails with
    /// [`BlobError::Corruption`] rather than yielding plaintext or ciphertext. A
    /// [`BlobCipher::KnownPlaintext`] read requires physical length to equal this call's trusted
    /// logical length; that catches the ordinary encrypted/uncompressed layout without sniffing
    /// and rejecting legitimate arbitrary plaintext bytes that happen to form a CRNB file.
    ///
    /// `compression` is the object version's stored compression descriptor, the source of truth
    /// for whether the blob is a self-describing CRNB block container. The reader trusts it (and
    /// the cipher — encryption also uses the container) rather than sniffing the trailer magic,
    /// which an uncompressed object's bytes can collide with (audit #18).
    ///
    /// `expected_logical_len` is the trusted plaintext size from the object-version row (or
    /// [`PartRef::size`] for multipart assembly). Legacy encrypted v2 did not authenticate its
    /// trailer/index, so the reader requires their logical total to equal this value before serving
    /// bytes. It must never be derived from the CRNB file itself.
    async fn open_raw(
        &self,
        path: &StoragePath,
        range: Option<ByteRange>,
        cipher: BlobCipher,
        compression: &CompressionDescriptor,
        expected_logical_len: u64,
    ) -> Result<BlobReadHandle, BlobError>;

    /// Cheaply answer whether a committed blob is PRESENT, plus basic framing — WITHOUT a DEK and
    /// WITHOUT decrypting. It stats the file / reads the fixed container header only; it never
    /// opens the body. Contract: a well-formed *encrypted* blob returns `Ok` (present), NOT
    /// [`BlobError::Corruption`] — presence is not decryptability. An absent blob is
    /// [`BlobError::NotFound`]; a real I/O failure is the corresponding `BlobError`.
    ///
    /// This is the presence probe the integrity `--repair` pass needs: it must distinguish a
    /// dangling row (blob missing) from a healthy encrypted object it holds no key for, and
    /// `open_raw` cannot serve that — a DEK-less open of an encrypted blob fails closed by design.
    async fn probe(&self, path: &StoragePath) -> Result<BlobProbe, BlobError>;

    /// Idempotently delete a committed blob (absence is success).
    async fn delete(&self, path: &StoragePath) -> Result<(), BlobError>;

    /// Stage one multipart part durably, reporting its plaintext size, MD5, and any supplementary
    /// `checksums` computed over the plaintext (empty when `checksums` is empty). The caller
    /// validates the returned checksums against any client-supplied `x-amz-checksum-*` header and
    /// persists the per-part value for composite/full-object composition at completion. When
    /// `encryption` is `Some`, the part is staged as a CRNB `VERSION_ENCRYPTED` blob under that DEK
    /// (ARCH 27); the MD5/checksums are still computed over the plaintext, so the ETag basis is
    /// unchanged. `None` stages the part in the clear (legacy).
    #[allow(clippy::too_many_arguments)]
    async fn stage_part(
        &self,
        upload: &UploadId,
        part_number: u16,
        attempt_id: &str,
        body: crate::BodyStream,
        checksums: ChecksumSet,
        size_ceiling: u64,
        encryption: Option<SecretKey32>,
    ) -> Result<StagedPart, BlobError>;

    /// Idempotently delete one deterministically-named multipart part attempt.
    ///
    /// The caller reserves `attempt_id` before staging. This exact deletion seam lets cleanup retry
    /// converge even if [`BlobStore::stage_part`] created the artifact but failed before returning
    /// its [`StagedPart`].
    async fn delete_part_attempt(
        &self,
        upload: &UploadId,
        part_number: u16,
        attempt_id: &str,
    ) -> Result<(), BlobError>;

    /// Assemble ordered parts into one durably-committed blob, applying compression during
    /// the assembly pass.
    async fn assemble(
        &self,
        bucket: &BucketName,
        parts: &[PartRef],
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError>;

    /// Idempotently delete all of a session's staged parts.
    async fn delete_session(&self, upload: &UploadId) -> Result<(), BlobError>;

    /// Reconcile on-disk blobs against the metadata, reclaiming orphans. Bounded in memory:
    /// it streams the filesystem and consults the batched membership `oracle`.
    async fn reconcile(
        &self,
        oracle: &dyn ReconcileOracle,
        opts: ReconcileOpts,
    ) -> Result<ReconcileReport, BlobError>;
}

/// A bounded membership oracle for reconciliation: tells the blob store which storage paths
/// and upload sessions the metadata still references, without materializing the keyspace.
#[async_trait]
pub trait ReconcileOracle: Send + Sync {
    /// For each candidate path, whether a metadata row references it.
    async fn live_blobs(&self, candidates: &[StoragePath]) -> Result<Vec<bool>, MetaError>;

    /// Whether an upload session still exists.
    async fn live_session(&self, upload: &UploadId) -> Result<bool, MetaError>;

    /// For each candidate multipart-part path, whether a committed part row references it.
    async fn live_multipart_parts(
        &self,
        candidates: &[StoragePath],
    ) -> Result<Vec<bool>, MetaError>;
}

/// The source-of-truth metadata store. All writes go through [`submit`](MetadataStore::submit)
/// (the single group-committing writer); all reads use the snapshot read pool. Every
/// enumeration is paged and bounded.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Submit a mutation to the group-committing writer. The returned future resolves only
    /// after the batch containing this mutation has been made durable.
    async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError>;

    /// Probe the snapshot-read path with a constant-cost query.
    ///
    /// This is the readiness seam: concrete stores must check out a real read-pool connection and
    /// execute a trivial query that does not enumerate application rows. Decorators must forward it
    /// without caching, and a sharded store must probe every bounded shard. Its cost must remain
    /// independent of bucket and object counts.
    async fn read_probe(&self) -> Result<(), MetaError>;

    // --- buckets ---
    /// Look up a bucket.
    async fn get_bucket(&self, name: &BucketName) -> Result<Option<Bucket>, MetaError>;
    /// List buckets, optionally only those owned by `owner`.
    async fn list_buckets(&self, owner: Option<&UserId>) -> Result<Vec<Bucket>, MetaError>;
    /// Get a bucket configuration aspect document.
    async fn get_bucket_config(
        &self,
        name: &BucketName,
        aspect: ConfigAspect,
    ) -> Result<Option<ConfigDoc>, MetaError>;
    /// Get the account-wide Block Public Access singleton.
    async fn get_account_public_access_block(&self) -> Result<PublicAccessBlock, MetaError>;
    /// Whether the bucket has no current objects (for delete).
    async fn is_bucket_empty(&self, name: &BucketName) -> Result<bool, MetaError>;

    // --- object versions ---
    /// Get the current version of a key (None if absent or hidden by a delete marker handled
    /// by the caller).
    async fn current_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<ObjectVersionRow>, MetaError>;
    /// Get a specific version.
    async fn get_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ObjectVersionRow>, MetaError>;
    /// Page current objects under a prefix (half-open range seek), excluding delete markers.
    /// Each summary carries the internal immutable metadata-row identity used by maintenance
    /// compare-and-delete; external serializers omit it.
    async fn list_current(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError>;
    /// Page all versions and delete markers under a prefix, with the same internal row identity.
    async fn list_versions(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError>;
    /// Page storage paths for reconciliation and force-empty (bounded).
    async fn enumerate_storage_paths(
        &self,
        bucket: &BucketName,
        cursor: Option<&str>,
        batch: u32,
    ) -> Result<ListPage<StoragePath>, MetaError>;
    /// Get an object version's tags.
    async fn get_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Vec<(String, String)>, MetaError>;
    /// Get the Object Lock state (retention + legal hold) of one object version, or the default
    /// (no retention, no legal hold) when none is set.
    async fn get_object_lock(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<crate::object::ObjectLockState, MetaError>;

    // --- multipart ---
    /// Get a multipart session.
    async fn get_multipart(&self, upload: &UploadId)
    -> Result<Option<MultipartSession>, MetaError>;
    /// List a session's parts (after `part_number_marker`).
    async fn list_parts(
        &self,
        upload: &UploadId,
        part_number_marker: u16,
        limit: u32,
    ) -> Result<ListPage<PartRecord>, MetaError>;
    /// List active multipart uploads under a prefix.
    async fn list_multipart_uploads(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<MultipartSession>, MetaError>;
    /// Page sessions older than `older_than` for the sweeper.
    async fn enumerate_stale_sessions(
        &self,
        older_than: Timestamp,
        batch: u32,
    ) -> Result<Vec<MultipartSession>, MetaError>;
    /// Page abandoned pre-stage part reservations older than `older_than`.
    async fn enumerate_stale_multipart_reservations(
        &self,
        older_than: Timestamp,
        batch: u32,
    ) -> Result<Vec<MultipartReservation>, MetaError>;
    /// List pending filesystem cleanup debts, exact paths first, under a hard row bound.
    async fn list_multipart_cleanups(&self, limit: u32)
    -> Result<Vec<MultipartCleanup>, MetaError>;

    // --- replication ---
    /// Get an object version's replication status.
    async fn object_replication_status(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ReplicationStatus>, MetaError>;
    /// Whether the outbox holds an earlier (lower `version_id`, i.e. created-before, since
    /// version ids are time-ordered uuidv7) entry for the same `(bucket, key, target)` that has not
    /// yet completed replication. The replication engine consults this before shipping an entry so
    /// it can defer a later version whose predecessor is still in flight in a *separate* drain batch,
    /// preserving per-key write order **per target** at the destination across batches (audit #9).
    /// `target` scopes the check to the entry's own destination ARN (`None` = the legacy env
    /// single-target path), so under fan-out a slow target never blocks a healthy one for the same
    /// key. Completed entries keep their outbox row with `status='completed'`, so they are excluded.
    async fn has_unreplicated_predecessor(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        before: &VersionId,
        target: Option<&str>,
    ) -> Result<bool, MetaError>;
    /// Claim a batch of due replication entries (a write; routed through the writer
    /// internally by the implementation, exposed here for the worker pool). Claiming marks the
    /// entries `Claimed` with a lease so a concurrent worker cannot also process them.
    async fn claim_replication_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError>;
    /// Read-only peek of the replication entries that are due (claimable) as of `now`, without
    /// claiming them. Used for observability — the per-bucket replication status view and tests —
    /// where a non-mutating probe is required.
    async fn list_due_replication(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError>;
    /// List replication-outbox entries the engine has marked terminal/failed (status `Failed`,
    /// i.e. retries exhausted with no further attempt scheduled), most recently due first, up to
    /// `limit`. The control plane surfaces these for operator attention (ARCH 20.5/22.2).
    async fn list_failed_replication(&self, limit: u32) -> Result<Vec<OutboxEntry>, MetaError>;
    /// Aggregate replication-outbox counts in a single indexed pass — totals by status, a per-target
    /// pending/failed breakdown, and the oldest still-pending entry's enqueue time (for true lag).
    /// Unlike counting a `list_*` result this is never bounded by a page limit, so it does not
    /// under-report on a busy node. `bucket` scopes to one source bucket; `None` is store-wide.
    async fn replication_counts(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<ReplicationCounts, MetaError>;

    // --- webhook event-notification outbox (mirrors the replication outbox) ---
    /// Atomically claim a batch of due webhook-notification entries (a write routed through the
    /// writer; the select-and-mark is one transaction so two workers never claim the same entry).
    async fn claim_webhook_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError>;
    /// Read-only peek of the webhook entries due as of `now`, for observability/tests.
    async fn list_due_webhooks(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError>;
    /// List webhook-outbox entries that exhausted their retry budget (status `Failed`), most
    /// recently due first, up to `limit`, for operator attention.
    async fn list_failed_webhooks(&self, limit: u32) -> Result<Vec<WebhookEntry>, MetaError>;

    // --- bucket quota ---
    /// Read a bucket's optional byte quota (`buckets.quota_bytes`), `None` when the bucket has
    /// no quota set. The quota is enforced inside the writer's commit transaction (ARCH 27.5);
    /// this reader exposes the configured value to the control plane.
    async fn get_bucket_quota(&self, bucket: &BucketName) -> Result<Option<u64>, MetaError>;

    // --- users ---
    /// Look up a user by Bearer access-key id (returns the stored secret hash).
    async fn user_by_bearer_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserWithBearerHash>, MetaError>;
    /// Look up a user by SigV4 access-key id (returns the encrypted secret).
    async fn user_by_sigv4_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserSigV4Credentials>, MetaError>;
    /// Look up a temporary STS-style session credential by its access-key id, joining the parent
    /// user's identity. Returns `None` when the key is not a session credential (the authenticator
    /// then treats it as an unknown key). The caller validates the token + expiry and fails closed.
    async fn user_by_session_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserSessionCredentials>, MetaError>;
    /// List active (not-yet-expired as of `now`) session credentials, newest first, as non-secret
    /// summaries — for the console's "active sessions" view. Carries no secret/token material.
    async fn list_session_credentials(
        &self,
        now: Timestamp,
    ) -> Result<Vec<SessionCredentialSummary>, MetaError>;
    /// Count users (for bootstrap gating).
    async fn count_users(&self) -> Result<u64, MetaError>;
    /// List all users.
    async fn list_users(&self) -> Result<Vec<User>, MetaError>;
    /// Fetch a user's attached identity-policy JSON document, or `None` if the user has none (or
    /// does not exist). The raw stored JSON is returned; the caller parses/validates it.
    async fn get_user_policy(&self, user_id: &UserId) -> Result<Option<String>, MetaError>;

    // --- import jobs (ARCH 27) ---
    /// List one bounded, keyset-paginated page of S3 import-job summaries. Neither the source
    /// secret (ciphertext/nonce) nor per-bucket `buckets_json` is selected. Results have stable
    /// `(created_at DESC, id DESC)` ordering.
    async fn list_import_jobs(
        &self,
        query: &ImportJobListQuery,
    ) -> Result<ImportJobPage, MetaError>;
    /// Select only the id of the oldest job in `state`, or `None` when none exists.
    ///
    /// This constant-row, indexed read is the scheduler/recovery primitive: it deliberately does
    /// not select or decode `buckets_json`, so terminal history cannot amplify worker memory.
    async fn next_import_job_id(&self, state: ImportState) -> Result<Option<String>, MetaError>;
    /// Fetch a single import job by id (secret-free), or `None` if no such job exists.
    async fn get_import_job(&self, id: &str) -> Result<Option<ImportJob>, MetaError>;
    /// Fetch a single import job's **full record**, including the sealed source secret and per-bucket
    /// resume cursors. For the trusted, server-internal import worker ONLY (it opens the secret to
    /// dial the source and resumes from the cursors) — never reachable through the management API,
    /// which uses the secret-free [`get_import_job`](Self::get_import_job).
    async fn get_import_job_record(&self, id: &str) -> Result<Option<ImportJobRecord>, MetaError>;

    // --- object shares (ARCH 15.8) ---
    /// Fetch a share by its stable, non-secret management id.
    async fn get_share_by_id(&self, id: &str) -> Result<Option<ShareRow>, MetaError>;
    /// Fetch a share by the fixed-width lookup hash derived from its bearer token. Redemption uses
    /// this indexed equality lookup and then checks revoked/expired state.
    async fn get_share_by_token_hash(
        &self,
        token_hash: &ShareLookupHash,
    ) -> Result<Option<ShareRow>, MetaError>;
    /// List a bucket's shares (most recent first), optionally narrowed to a single key.
    async fn list_shares(
        &self,
        bucket: &BucketName,
        key: Option<&ObjectKey>,
    ) -> Result<Vec<ShareRow>, MetaError>;

    // --- object tag browsing (ARCH 17.2) ---
    /// Summarize the distinct object tags in use (each `key=value` with a current-object count),
    /// descending by count. Scoped to `bucket` when set, else across all buckets.
    async fn list_tag_summary(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<Vec<TagSummary>, MetaError>;
    /// List the current objects (latest, non-delete-marker) carrying the exact `tag_key=tag_value`,
    /// up to `limit`. Scoped to `bucket` when set, else across all buckets.
    async fn list_objects_by_tag(
        &self,
        bucket: Option<&BucketName>,
        tag_key: &str,
        tag_value: &str,
        limit: u32,
    ) -> Result<Vec<TaggedObject>, MetaError>;

    // --- audit & aggregates ---
    /// List recent activity (most recent first), up to `limit`.
    async fn list_activity(&self, limit: u32) -> Result<Vec<ActivityEntry>, MetaError>;
    /// Aggregate store counts.
    async fn aggregate_counts(&self) -> Result<StoreCounts, MetaError>;
    /// Per-bucket aggregate counts (sorted by bucket name); empty buckets appear with zeros.
    async fn bucket_counts(&self) -> Result<Vec<BucketCounts>, MetaError>;

    // --- request metrics (usage analytics, ARCH 26.5) ---
    /// Query the request-metrics rollup for the given range, downsampling the timeline into the
    /// range's window. `now_secs` is the current epoch-seconds reference for the lower bound.
    async fn query_request_metrics(
        &self,
        range: MetricsRange,
        now_secs: i64,
    ) -> Result<RequestMetricsSeries, MetaError>;
}

/// An authenticator examines a library-neutral request view and yields one of three
/// outcomes. Implementations are composed into an ordered chain whose first applicable
/// outcome decides (ARCH 12.3, 14).
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Attempt authentication.
    async fn authenticate(&self, view: &RequestView<'_>) -> AuthOutcome;
}

/// The authorization engine: a pure function from fetched inputs to an allow/deny decision,
/// with the fixed evaluation order of ARCH 15.3. No I/O.
pub trait AuthorizationEngine: Send + Sync {
    /// Evaluate a request.
    fn evaluate(&self, input: &AuthzInput) -> Decision;
}

/// A replication destination abstracted as an S3-compatible client. A fake sink in tests
/// records intents and can simulate retryable/terminal failures.
#[async_trait]
pub trait ReplicationSink: Send + Sync {
    /// Put an object with its metadata, tags, and ACL as the rule dictates.
    async fn put_object(&self, object: ReplicatedObject) -> Result<(), ReplicationError>;

    /// Propagate a deletion or delete marker.
    async fn delete_marker(
        &self,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<(), ReplicationError>;
}

/// The cryptography facility: envelope encryption of secrets at rest and constant-time
/// comparison. Key handling is isolated here.
pub trait Crypto: Send + Sync {
    /// Envelope-encrypt a secret under the ACTIVE master key, returning a self-describing `CRK1`
    /// envelope in `Sealed.ciphertext` (`magic ‖ key_id ‖ nonce ‖ ct‖tag`, audit #29). The nonce
    /// is *inside* the envelope; `Sealed.nonce` is set to the same bytes for source-compat but
    /// callers persisting a `CRK1` envelope MUST store only `Sealed.ciphertext` and leave any
    /// separate nonce column empty/NULL.
    fn seal(&self, plaintext: &[u8]) -> Result<Sealed, CryptoError>;
    /// Decrypt a sealed secret (plaintext returned in a zeroizing container). Routes by content: a
    /// `CRK1`-magic `ciphertext` parses its own key_id + nonce and ignores `nonce`; a legacy blob
    /// (no magic) decrypts under the legacy key using the separate `nonce`. A missing key id or any
    /// tag/AAD failure is a hard error (fail-closed), never a fallback.
    fn open(&self, ciphertext: &[u8], nonce: &Nonce) -> Result<Zeroizing<Vec<u8>>, CryptoError>;
    /// Constant-time byte comparison.
    fn ct_eq(&self, a: &[u8], b: &[u8]) -> bool;
    /// The ring id new seals use. Defaulted to `1` so test doubles need no change (audit #29).
    fn active_key_id(&self) -> u16 {
        1
    }
}

/// The clock, injected wherever time governs behaviour so it is tested deterministically.
pub trait Clock: Send + Sync {
    /// The current time.
    fn now(&self) -> Timestamp;
}

/// Signing and verification of Cairn's signed public-read URLs (a Cairn extension).
pub trait PublicUrl: Send + Sync {
    /// Sign a public-read URL over the method, escaped path, and expiry.
    fn sign(&self, method: &str, escaped_path: &str, expiry: Timestamp) -> Signature;
    /// Verify a signed public-read URL in constant time, including the expiry check.
    fn verify(
        &self,
        method: &str,
        escaped_path: &str,
        expiry: Timestamp,
        signature: &Signature,
        now: Timestamp,
    ) -> bool;
}
