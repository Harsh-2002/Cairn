//! A read-through cache decorator for the hot, read-mostly config reads consulted on the
//! authorization path (ARCH 11.5 / finding F-10).
//!
//! [`CachedMetadataStore`] wraps any [`MetadataStore`] and memoises exactly three lookups —
//! [`get_bucket`](MetadataStore::get_bucket),
//! [`get_bucket_config`](MetadataStore::get_bucket_config), and
//! [`get_account_public_access_block`](MetadataStore::get_account_public_access_block) — that
//! every authorized request reconsults. Every other trait method forwards straight to the
//! inner store untouched. Writes flow through [`submit`](MetadataStore::submit), which the
//! decorator inspects to invalidate exactly the entries a mutation could have changed (when in
//! doubt it invalidates the whole affected bucket — correctness over precision).
//!
//! The cache is sharded into [`SHARDS`] independent `Mutex<HashMap<…>>` buckets chosen by key
//! hash, so concurrent authorizers for different buckets rarely contend on the same lock. A
//! coarse byte budget is enforced with an atomic running counter: an insert that would push a
//! shard over its slice of the budget first evicts an arbitrary existing entry from that shard.
//! Values are held behind [`Arc`] so a cache hit is a refcount bump, never a deep copy. Negative
//! results (`None`) are cached too, because "this bucket has no policy" is itself a hot answer.
//!
//! Dependency note: `metrics` is *not* a dependency of `cairn-meta`, and the task forbids adding
//! one gratuitously, so hit/miss accounting is exposed via [`CachedMetadataStore::stats`] (backed
//! by relaxed atomics) for the server to scrape into its own metrics registry rather than emitted
//! through the `metrics` crate from here.

use async_trait::async_trait;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use cairn_types::authz::PublicAccessBlock;
use cairn_types::bucket::{Bucket, ConfigAspect, ConfigDoc};
use cairn_types::error::MetaError;
use cairn_types::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use cairn_types::meta::{
    ActivityEntry, BucketCounts, ImportJob, ImportJobRecord, ListPage, ListQuery, MetricsRange,
    MultipartSession, Mutation, MutationOutcome, ObjectSummary, OutboxEntry, PartRecord,
    ReplicationCounts, ReplicationStatus, RequestMetricsSeries, SessionCredentialSummary, ShareRow,
    StoreCounts, TagSummary, TaggedObject, User, UserSessionCredentials, UserSigV4Credentials,
    UserWithBearerHash, WebhookEntry,
};
use cairn_types::object::ObjectVersionRow;
use cairn_types::time::Timestamp;
use cairn_types::traits::MetadataStore;

/// Number of lock shards. A power of two keeps the hash-to-shard reduction cheap and the
/// per-shard budget arithmetic exact.
const SHARDS: usize = 16;

/// A rough fixed overhead charged per cached entry on top of its payload, accounting for the
/// key, the `Arc` allocation, and `HashMap` bookkeeping. Only the *relative* budget pressure
/// matters here, so the constant need not be precise.
const ENTRY_OVERHEAD: u64 = 128;

/// Compute the shard index for a hashable key.
fn shard_of<K: Hash>(key: &K) -> usize {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) & (SHARDS - 1)
}

/// One cached config value: either a present document or a remembered absence. Both are hot
/// authorization answers worth caching.
type CachedConfig = Option<Arc<ConfigDoc>>;

/// One sharded cache over key `K` to `Arc<V>`-style value `Val`, with an approximate byte budget.
struct ShardedCache<K, Val> {
    shards: Vec<Mutex<HashMap<K, (Val, u64)>>>,
    /// Running approximate byte size across all shards.
    size: AtomicU64,
    /// Total budget in bytes; `0` disables caching entirely.
    budget: u64,
}

impl<K, Val> ShardedCache<K, Val>
where
    K: Hash + Eq + Clone,
    Val: Clone,
{
    fn new(budget: u64) -> Self {
        let mut shards = Vec::with_capacity(SHARDS);
        for _ in 0..SHARDS {
            shards.push(Mutex::new(HashMap::new()));
        }
        Self {
            shards,
            size: AtomicU64::new(0),
            budget,
        }
    }

    fn enabled(&self) -> bool {
        self.budget != 0
    }

    fn get(&self, key: &K) -> Option<Val> {
        if !self.enabled() {
            return None;
        }
        let shard = &self.shards[shard_of(key)];
        let guard = shard.lock().unwrap();
        guard.get(key).map(|(v, _)| v.clone())
    }

    /// Insert (or replace) an entry costing `cost` bytes, evicting within the shard to keep the
    /// running total under budget — but only if the invalidation `generation` has not advanced past
    /// `gen_snapshot` (taken by the caller before its inner read). Eviction is intentionally simple
    /// (drop an arbitrary entry): correctness of membership matters here, not eviction optimality.
    ///
    /// The generation re-check happens **inside the shard lock**, which the invalidation paths
    /// (`invalidate` / `invalidate_matching`) also take after bumping the generation. That closes
    /// the read-install TOCTOU: an invalidation racing our inner read either bumps the generation
    /// before we re-check (so we skip the install) or removes our freshly-inserted entry under the
    /// same lock afterward — a stale value can never be pinned past the invalidation.
    fn put_checked(&self, key: K, val: Val, cost: u64, gen_snapshot: u64, generation: &AtomicU64) {
        if !self.enabled() {
            return;
        }
        let cost = cost + ENTRY_OVERHEAD;
        let shard = &self.shards[shard_of(&key)];
        let mut guard = shard.lock().unwrap();

        if generation.load(Ordering::Acquire) != gen_snapshot {
            return;
        }

        // Replace: refund the prior cost first.
        if let Some((_, old_cost)) = guard.remove(&key) {
            self.size.fetch_sub(old_cost, Ordering::Relaxed);
        }

        // Evict arbitrary entries from this shard until adding `cost` fits the budget.
        while self.size.load(Ordering::Relaxed) + cost > self.budget {
            let victim = guard.keys().next().cloned();
            match victim {
                Some(k) => {
                    if let Some((_, c)) = guard.remove(&k) {
                        self.size.fetch_sub(c, Ordering::Relaxed);
                    }
                }
                // This shard is empty but we are still over budget (other shards hold the bytes).
                // Insert anyway rather than spin — the budget is approximate and per-insert
                // eviction keeps it bounded in aggregate.
                None => break,
            }
        }

        guard.insert(key, (val, cost));
        self.size.fetch_add(cost, Ordering::Relaxed);
    }

    /// Drop a single key, refunding its cost.
    fn invalidate(&self, key: &K) {
        if !self.enabled() {
            return;
        }
        let shard = &self.shards[shard_of(key)];
        let mut guard = shard.lock().unwrap();
        if let Some((_, c)) = guard.remove(key) {
            self.size.fetch_sub(c, Ordering::Relaxed);
        }
    }

    /// Drop every key matching `pred` across all shards (used to wipe all aspects of a bucket).
    fn invalidate_matching(&self, pred: impl Fn(&K) -> bool) {
        if !self.enabled() {
            return;
        }
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            let victims: Vec<K> = guard.keys().filter(|k| pred(k)).cloned().collect();
            for k in victims {
                if let Some((_, c)) = guard.remove(&k) {
                    self.size.fetch_sub(c, Ordering::Relaxed);
                }
            }
        }
    }
}

/// A read-through caching decorator over an inner [`MetadataStore`].
///
/// Construct with [`CachedMetadataStore::new`]; a `budget_bytes` of `0` yields a pure
/// pass-through that forwards every call. See the module docs for the caching and invalidation
/// model.
pub struct CachedMetadataStore {
    inner: Arc<dyn MetadataStore>,
    /// `get_bucket` cache, keyed by bucket name. The value caches absence too.
    bucket: ShardedCache<BucketName, Option<Arc<Bucket>>>,
    /// `get_bucket_config` cache, keyed by `(bucket, aspect)`.
    config: ShardedCache<(BucketName, ConfigAspect), CachedConfig>,
    /// `get_account_public_access_block` cache (a single account-wide value). The whole
    /// [`PublicAccessBlock`] is four booleans, so it packs into the low 8 bits (bit 0 = "present",
    /// bits 1-4 = the flags) of a lock-free `AtomicU64` whose high bits carry the `account_bpa_gen`
    /// the value was fetched at — a truly lock-free load/store on this very-hot read with no mutex.
    /// `0` means "not cached". A hit is served only when the packed generation still equals the live
    /// `account_bpa_gen`, so an invalidation that races the read-install can never be lost (a stale
    /// install carries an older generation and is simply not served).
    account_bpa: AtomicU64,
    /// Dedicated invalidation generation for `account_bpa`, bumped only on a public-access-block
    /// change. Kept separate from the global `generation` so an unrelated bucket-config invalidation
    /// does not spuriously evict this account-wide value from the very-hot authorization path.
    account_bpa_gen: AtomicU64,
    /// Whether caching is on at all (`budget_bytes != 0`).
    enabled: bool,
    /// Monotonic invalidation epoch, bumped on *every* cache invalidation (both around `submit`
    /// and whenever any aspect is dropped). A cached read snapshots this before calling `inner`
    /// and refuses to install its result if the epoch advanced meanwhile — closing the stale-read
    /// TOCTOU window where a concurrent writer commits while a reader is parked at its inner
    /// `.await` holding a pre-commit snapshot value. See [`CachedMetadataStore::get_bucket_config`].
    generation: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    /// A monotonic epoch shared with the authenticator's credential/policy cache (`cairn-auth`).
    /// Bumped on every user-identity mutation (create / update / deactivate / set-policy) so that
    /// cache — which never observes these mutations directly — drops any entry minted before the
    /// change. Independent of `enabled`: the auth cache must stay coherent even when this config
    /// cache is turned off (`budget_bytes == 0`).
    auth_epoch: Arc<AtomicU64>,
}

/// "Present" bit for the packed account-BPA byte; `0` means "not cached".
const BPA_PRESENT: u8 = 1;

/// Pack an `Option<PublicAccessBlock>` into the single byte stored in `account_bpa` (bit 0 present,
/// bits 1-4 the flags).
fn encode_bpa(v: Option<PublicAccessBlock>) -> u8 {
    match v {
        None => 0,
        Some(b) => {
            BPA_PRESENT
                | (u8::from(b.block_public_acls) << 1)
                | (u8::from(b.ignore_public_acls) << 2)
                | (u8::from(b.block_public_policy) << 3)
                | (u8::from(b.restrict_public_buckets) << 4)
        }
    }
}

/// Unpack a byte produced by [`encode_bpa`]; `None` for the "not cached" sentinel `0`.
fn decode_bpa(x: u8) -> Option<PublicAccessBlock> {
    if x & BPA_PRESENT == 0 {
        return None;
    }
    Some(PublicAccessBlock {
        block_public_acls: x & (1 << 1) != 0,
        ignore_public_acls: x & (1 << 2) != 0,
        block_public_policy: x & (1 << 3) != 0,
        restrict_public_buckets: x & (1 << 4) != 0,
    })
}

impl std::fmt::Debug for CachedMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedMetadataStore")
            .field("enabled", &self.enabled)
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl CachedMetadataStore {
    /// Wrap `inner`, caching the hot config reads within an approximate `budget_bytes`.
    ///
    /// `budget_bytes == 0` disables the cache entirely: every method forwards straight to
    /// `inner` with no memoisation and no invalidation bookkeeping.
    #[must_use]
    pub fn new(inner: Arc<dyn MetadataStore>, budget_bytes: u64) -> Self {
        // Split the byte budget across the two byte-counted caches. The account-BPA slot is a
        // single small `Copy` value, so it is not charged against the budget.
        let half = budget_bytes / 2;
        let bucket_budget = if budget_bytes == 0 { 0 } else { half.max(1) };
        let config_budget = if budget_bytes == 0 {
            0
        } else {
            (budget_bytes - half).max(1)
        };
        Self {
            inner,
            bucket: ShardedCache::new(bucket_budget),
            config: ShardedCache::new(config_budget),
            account_bpa: AtomicU64::new(0),
            account_bpa_gen: AtomicU64::new(0),
            enabled: budget_bytes != 0,
            generation: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            auth_epoch: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A handle to the shared user-mutation epoch, handed to the authenticator's credential/policy
    /// cache so it can treat any user-identity change as "drop my cached entries" without observing
    /// the mutation stream itself. See the `auth_epoch` field docs.
    #[must_use]
    pub fn auth_epoch_handle(&self) -> Arc<AtomicU64> {
        self.auth_epoch.clone()
    }

    /// Bump the shared auth epoch when `mutation` changes a user's credentials, active state, or
    /// identity policy — the inputs the authenticator caches. Runs regardless of `enabled` (the
    /// auth cache is independent of the config cache). `SetUserQuota` is intentionally excluded:
    /// quota is enforced at write time and is not part of any cached authentication answer.
    fn note_user_mutation(&self, mutation: &Mutation) {
        if matches!(
            mutation,
            Mutation::CreateUser(_)
                | Mutation::UpdateUser(_)
                | Mutation::DeactivateUser(_)
                | Mutation::DeleteUser(_)
                | Mutation::SetUserPolicy { .. }
        ) {
            self.auth_epoch.fetch_add(1, Ordering::Release);
        }
    }

    /// Cumulative `(hits, misses)` across the cached reads. Exposed in lieu of emitting through
    /// the `metrics` crate (which is not a dependency of this crate); the server scrapes this.
    #[must_use]
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }

    fn hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Read the current invalidation epoch (snapshot at the start of a cached read).
    ///
    /// Ordering: an `Acquire` load here pairs with the `Release` bump in [`Self::bump_generation`]
    /// so a reader that observes the pre-bump epoch is guaranteed to have started before the
    /// invalidation's effects became visible — the precondition for it being safe to cache.
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Advance the invalidation epoch. Called from every invalidation path so that any read in
    /// flight across this point will refuse to cache its (now possibly stale) inner result.
    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Approximate the cached byte cost of a config document.
    fn config_cost(doc: &CachedConfig) -> u64 {
        doc.as_ref().map_or(0, |d| d.0.len() as u64)
    }

    /// Approximate the cached byte cost of a bucket row.
    fn bucket_cost(b: &Option<Arc<Bucket>>) -> u64 {
        // A bucket row is small and fixed-ish; charge the name plus a flat estimate.
        b.as_ref()
            .map_or(0, |bk| bk.name.as_str().len() as u64 + 64)
    }

    /// Invalidate everything the cache holds about `bucket`: its `get_bucket` row and every
    /// cached config aspect for it.
    fn invalidate_bucket(&self, bucket: &BucketName) {
        self.bump_generation();
        self.bucket.invalidate(bucket);
        let b = bucket.clone();
        self.config.invalidate_matching(move |(name, _)| name == &b);
    }

    /// Invalidate one specific config aspect of a bucket (and the bucket row, cheaply, since a
    /// config change never alters the row but keeping them coherent costs nothing).
    fn invalidate_bucket_aspect(&self, bucket: &BucketName, aspect: ConfigAspect) {
        self.bump_generation();
        self.config.invalidate(&(bucket.clone(), aspect));
    }

    /// Public evict for an out-of-band writer that bypasses [`submit`](Self::submit): the #29
    /// key-rewrap worker re-seals a bucket's replication targets via a direct compare-and-swap on the
    /// raw store (to preserve the lost-update witness), so the decorator never sees it. Without this
    /// the cache keeps serving the pre-rewrap (old-key) targets doc, which the control plane can then
    /// re-persist — leaving old-key ciphertext in the DB after re-wrap "finished" (audit 2026-07).
    pub fn invalidate_config_aspect(&self, bucket: &BucketName, aspect: ConfigAspect) {
        if self.enabled {
            self.invalidate_bucket_aspect(bucket, aspect);
        }
    }

    /// Drop the cached account-wide public-access-block. Bumps the dedicated `account_bpa_gen`
    /// first, then clears the slot: a read-install that snapshotted the old generation will carry it
    /// in the packed value and so be rejected on the next hit, even if its `store` lands after this
    /// `store(0)` — the invalidation can never be lost.
    fn invalidate_account_bpa(&self) {
        if self.enabled {
            self.account_bpa_gen.fetch_add(1, Ordering::Release);
            self.account_bpa.store(0, Ordering::Release);
        }
    }

    /// Inspect a mutation and invalidate exactly the cache entries it could have affected.
    /// Conservative by construction: any mutation naming a bucket whose config *might* change
    /// drops that bucket's whole config set.
    fn invalidate_for(&self, mutation: &Mutation) {
        if !self.enabled {
            return;
        }
        match mutation {
            // --- mutations that change a bucket's row and/or config ---
            Mutation::CreateBucket(b) => self.invalidate_bucket(&b.name),
            Mutation::DeleteBucket(name) => self.invalidate_bucket(name),
            Mutation::SetBucketConfig { bucket, aspect, .. } => {
                // The exact aspect is known; drop just it, and refresh the bucket row defensively
                // (PublicAccessBlock-as-config never touches the row, but a stale row never hurts).
                self.invalidate_bucket_aspect(bucket, *aspect);
                self.bucket.invalidate(bucket);
            }
            Mutation::SetVersioning { bucket, .. }
            | Mutation::SetOwnership { bucket, .. }
            | Mutation::SetBucketQuota { bucket, .. }
            | Mutation::SetBucketCompression { bucket, .. } => {
                // These change the bucket row directly; aspects are untouched but invalidating
                // the whole bucket is cheap and unambiguously correct.
                self.invalidate_bucket(bucket);
            }

            // --- the account-wide public-access-block singleton ---
            Mutation::SetAccountPublicAccessBlock(_) => self.invalidate_account_bpa(),

            // --- mutations that never touch cached config reads: nothing to do here ---
            // (the user-identity mutations below are handled for the auth cache by
            // `note_user_mutation`, which bumps the shared auth epoch; the config cache holds no
            // user rows, so they are no-ops for *this* cache).
            Mutation::PutObjectVersion { .. }
            | Mutation::CreateDeleteMarker { .. }
            | Mutation::DeleteVersion { .. }
            | Mutation::CreateMultipart(_)
            | Mutation::RecordPart { .. }
            | Mutation::ClaimMultipart(_)
            | Mutation::CompleteMultipart { .. }
            | Mutation::AbortMultipart(_)
            | Mutation::SetUserPolicy { .. }
            | Mutation::SetUserQuota { .. }
            | Mutation::PutObjectTags { .. }
            | Mutation::DeleteObjectTags { .. }
            | Mutation::SetObjectAcl { .. }
            | Mutation::CreateUser(_)
            | Mutation::UpdateUser(_)
            | Mutation::DeactivateUser(_)
            | Mutation::DeleteUser(_)
            | Mutation::CreateSessionCredential(_)
            | Mutation::DeleteExpiredSessionCredentials { .. }
            | Mutation::DeleteSessionCredential { .. }
            | Mutation::ClaimReplicationBatch { .. }
            | Mutation::MarkReplicationDone { .. }
            | Mutation::MarkReplicationFailed { .. }
            | Mutation::RetryFailedReplication { .. }
            | Mutation::PruneReplicationOutbox { .. }
            | Mutation::PruneEventsOutbox { .. }
            | Mutation::DeferReplication { .. }
            | Mutation::RecoverClaimedReplication
            | Mutation::EnqueueReplication(_)
            // Touches only `replication_outbox` and `object_versions.replication_status`; this
            // cache holds bucket rows and config aspects, neither of which changes.
            | Mutation::RequeueReplicationVersions { .. }
            | Mutation::EnqueueWebhooks(_)
            | Mutation::ClaimWebhookBatch { .. }
            | Mutation::MarkWebhookDone(_)
            | Mutation::MarkWebhookFailed { .. }
            | Mutation::RecordActivity(_)
            | Mutation::CreateShare(_)
            | Mutation::RevokeShare { .. }
            | Mutation::SetObjectRetention { .. }
            | Mutation::SetObjectLegalHold { .. }
            | Mutation::RecordRequestMetrics { .. }
            | Mutation::CreateImportJob(_)
            | Mutation::UpdateImportJobProgress { .. }
            | Mutation::SetImportJobState { .. }
            | Mutation::PruneImportJobs { .. } => {}
        }
    }
}

#[async_trait]
impl MetadataStore for CachedMetadataStore {
    async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
        // Invalidate around the write so no reader can repopulate a stale entry from a snapshot
        // taken before the commit: drop before forwarding, and again after it lands. The auth
        // epoch is bumped on the same before/after schedule so the authenticator's cache cannot
        // serve a credential/policy minted from a pre-commit view.
        self.note_user_mutation(&mutation);
        self.invalidate_for(&mutation);
        let outcome = self.inner.submit(mutation.clone()).await;
        if outcome.is_ok() {
            self.note_user_mutation(&mutation);
            self.invalidate_for(&mutation);
        }
        outcome
    }

    // --- cached reads ---

    async fn get_bucket(&self, name: &BucketName) -> Result<Option<Bucket>, MetaError> {
        if let Some(cached) = self.bucket.get(name) {
            self.hit();
            return Ok(cached.as_deref().cloned());
        }
        self.miss();
        // Snapshot the invalidation epoch *before* reading inner. If a concurrent `submit`
        // invalidates this entry while we are parked at the `.await` below, the epoch advances
        // and we must not install our (now possibly pre-commit, stale) snapshot.
        let gen_before = self.generation();
        let fetched = self.inner.get_bucket(name).await?;
        let arc = fetched.clone().map(Arc::new);
        let cost = Self::bucket_cost(&arc);
        // The generation re-check is now atomic with the install (inside the shard lock), closing
        // the window where an invalidation between the check and the insert could pin a stale value.
        self.bucket
            .put_checked(name.clone(), arc, cost, gen_before, &self.generation);
        Ok(fetched)
    }

    async fn get_bucket_config(
        &self,
        name: &BucketName,
        aspect: ConfigAspect,
    ) -> Result<Option<ConfigDoc>, MetaError> {
        let key = (name.clone(), aspect);
        if let Some(cached) = self.config.get(&key) {
            self.hit();
            return Ok(cached.as_deref().cloned());
        }
        self.miss();
        // Snapshot the invalidation epoch *before* reading inner, mirroring `get_bucket`: if a
        // concurrent `submit` invalidates this aspect while we are parked at the `.await`, the
        // epoch advances and we must not install our (now possibly pre-commit, stale) snapshot.
        let gen_before = self.generation();
        let fetched = self.inner.get_bucket_config(name, aspect).await?;
        let arc: CachedConfig = fetched.clone().map(Arc::new);
        let cost = Self::config_cost(&arc);
        self.config
            .put_checked(key, arc, cost, gen_before, &self.generation);
        Ok(fetched)
    }

    async fn get_account_public_access_block(&self) -> Result<PublicAccessBlock, MetaError> {
        if self.enabled {
            // Lock-free load of the packed value; serve only while its generation is still current,
            // so a stale install (carrying an older generation) is never served.
            let packed = self.account_bpa.load(Ordering::Acquire);
            if packed != 0 && (packed >> 8) == self.account_bpa_gen.load(Ordering::Acquire) {
                if let Some(cached) = decode_bpa((packed & 0xff) as u8) {
                    self.hit();
                    return Ok(cached);
                }
            }
        }
        self.miss();
        // Snapshot the dedicated generation BEFORE the inner read, then tag the installed value with
        // it. An invalidation racing the read bumps the generation, so our tagged value carries the
        // stale generation and the hit path above refuses to serve it — the invalidation is never
        // lost, even though the store is lock-free and unconditional.
        let gen_before = self.account_bpa_gen.load(Ordering::Acquire);
        let fetched = self.inner.get_account_public_access_block().await?;
        if self.enabled {
            let packed = (gen_before << 8) | u64::from(encode_bpa(Some(fetched)));
            self.account_bpa.store(packed, Ordering::Release);
        }
        Ok(fetched)
    }

    // --- everything below forwards straight to the inner store ---

    async fn list_buckets(&self, owner: Option<&UserId>) -> Result<Vec<Bucket>, MetaError> {
        self.inner.list_buckets(owner).await
    }

    async fn is_bucket_empty(&self, name: &BucketName) -> Result<bool, MetaError> {
        self.inner.is_bucket_empty(name).await
    }

    async fn current_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        self.inner.current_version(bucket, key).await
    }

    async fn get_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        self.inner.get_version(bucket, key, version).await
    }

    async fn list_current(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        self.inner.list_current(bucket, query).await
    }

    async fn list_versions(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        self.inner.list_versions(bucket, query).await
    }

    async fn enumerate_storage_paths(
        &self,
        bucket: &BucketName,
        cursor: Option<&str>,
        batch: u32,
    ) -> Result<ListPage<StoragePath>, MetaError> {
        self.inner
            .enumerate_storage_paths(bucket, cursor, batch)
            .await
    }

    async fn get_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Vec<(String, String)>, MetaError> {
        self.inner.get_object_tags(bucket, key, version).await
    }

    async fn get_object_lock(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<cairn_types::object::ObjectLockState, MetaError> {
        self.inner.get_object_lock(bucket, key, version).await
    }

    async fn get_multipart(
        &self,
        upload: &UploadId,
    ) -> Result<Option<MultipartSession>, MetaError> {
        self.inner.get_multipart(upload).await
    }

    async fn list_parts(
        &self,
        upload: &UploadId,
        part_number_marker: u16,
        limit: u32,
    ) -> Result<ListPage<PartRecord>, MetaError> {
        self.inner
            .list_parts(upload, part_number_marker, limit)
            .await
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<MultipartSession>, MetaError> {
        self.inner.list_multipart_uploads(bucket, query).await
    }

    async fn enumerate_stale_sessions(
        &self,
        older_than: Timestamp,
        batch: u32,
    ) -> Result<Vec<MultipartSession>, MetaError> {
        self.inner.enumerate_stale_sessions(older_than, batch).await
    }

    async fn object_replication_status(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ReplicationStatus>, MetaError> {
        self.inner
            .object_replication_status(bucket, key, version)
            .await
    }

    async fn has_unreplicated_predecessor(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        before: &VersionId,
        target: Option<&str>,
    ) -> Result<bool, MetaError> {
        self.inner
            .has_unreplicated_predecessor(bucket, key, before, target)
            .await
    }

    async fn claim_replication_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        self.inner.claim_replication_batch(limit, now).await
    }

    async fn list_due_replication(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        self.inner.list_due_replication(limit, now).await
    }

    async fn list_failed_replication(&self, limit: u32) -> Result<Vec<OutboxEntry>, MetaError> {
        self.inner.list_failed_replication(limit).await
    }

    async fn replication_counts(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<ReplicationCounts, MetaError> {
        self.inner.replication_counts(bucket).await
    }

    async fn claim_webhook_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError> {
        self.inner.claim_webhook_batch(limit, now).await
    }

    async fn list_due_webhooks(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError> {
        self.inner.list_due_webhooks(limit, now).await
    }

    async fn list_failed_webhooks(&self, limit: u32) -> Result<Vec<WebhookEntry>, MetaError> {
        self.inner.list_failed_webhooks(limit).await
    }

    async fn get_bucket_quota(&self, bucket: &BucketName) -> Result<Option<u64>, MetaError> {
        self.inner.get_bucket_quota(bucket).await
    }

    async fn user_by_bearer_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserWithBearerHash>, MetaError> {
        self.inner.user_by_bearer_key(access_key_id).await
    }

    async fn user_by_sigv4_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserSigV4Credentials>, MetaError> {
        self.inner.user_by_sigv4_key(access_key_id).await
    }

    async fn user_by_session_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserSessionCredentials>, MetaError> {
        self.inner.user_by_session_key(access_key_id).await
    }

    async fn list_session_credentials(
        &self,
        now: Timestamp,
    ) -> Result<Vec<SessionCredentialSummary>, MetaError> {
        self.inner.list_session_credentials(now).await
    }

    async fn count_users(&self) -> Result<u64, MetaError> {
        self.inner.count_users().await
    }

    async fn list_users(&self) -> Result<Vec<User>, MetaError> {
        self.inner.list_users().await
    }

    async fn get_user_policy(&self, user_id: &UserId) -> Result<Option<String>, MetaError> {
        self.inner.get_user_policy(user_id).await
    }

    async fn list_import_jobs(&self) -> Result<Vec<ImportJob>, MetaError> {
        self.inner.list_import_jobs().await
    }

    async fn get_import_job(&self, id: &str) -> Result<Option<ImportJob>, MetaError> {
        self.inner.get_import_job(id).await
    }

    async fn get_import_job_record(&self, id: &str) -> Result<Option<ImportJobRecord>, MetaError> {
        self.inner.get_import_job_record(id).await
    }

    async fn list_activity(&self, limit: u32) -> Result<Vec<ActivityEntry>, MetaError> {
        self.inner.list_activity(limit).await
    }

    async fn get_share(&self, token: &str) -> Result<Option<ShareRow>, MetaError> {
        // Shares are uncached so a revoke takes effect immediately on the next fetch.
        self.inner.get_share(token).await
    }

    async fn list_shares(
        &self,
        bucket: &BucketName,
        key: Option<&ObjectKey>,
    ) -> Result<Vec<ShareRow>, MetaError> {
        self.inner.list_shares(bucket, key).await
    }

    async fn list_tag_summary(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<Vec<TagSummary>, MetaError> {
        self.inner.list_tag_summary(bucket).await
    }

    async fn list_objects_by_tag(
        &self,
        bucket: Option<&BucketName>,
        tag_key: &str,
        tag_value: &str,
        limit: u32,
    ) -> Result<Vec<TaggedObject>, MetaError> {
        self.inner
            .list_objects_by_tag(bucket, tag_key, tag_value, limit)
            .await
    }

    async fn aggregate_counts(&self) -> Result<StoreCounts, MetaError> {
        self.inner.aggregate_counts().await
    }

    async fn bucket_counts(&self) -> Result<Vec<BucketCounts>, MetaError> {
        self.inner.bucket_counts().await
    }

    async fn query_request_metrics(
        &self,
        range: MetricsRange,
        now_secs: i64,
    ) -> Result<RequestMetricsSeries, MetaError> {
        self.inner.query_request_metrics(range, now_secs).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::authz::OwnershipMode;
    use cairn_types::bucket::VersioningState;
    use cairn_types::testing::InMemoryMetadataStore;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn bpa_packing_round_trips_every_flag_combination() {
        // The "not cached" sentinel decodes to None.
        assert_eq!(decode_bpa(0), None);
        // Every one of the 16 flag combinations survives encode → decode, and "present" is set.
        for bits in 0u8..16 {
            let bpa = PublicAccessBlock {
                block_public_acls: bits & 1 != 0,
                ignore_public_acls: bits & 2 != 0,
                block_public_policy: bits & 4 != 0,
                restrict_public_buckets: bits & 8 != 0,
            };
            let encoded = encode_bpa(Some(bpa));
            assert_ne!(encoded & BPA_PRESENT, 0, "present bit must be set");
            assert_eq!(decode_bpa(encoded), Some(bpa), "round-trip must be exact");
        }
    }

    /// A `SetAccountPublicAccessBlock` that lands while a BPA read is parked between its generation
    /// snapshot and its install must NOT be lost: the next read must serve the new value, not the
    /// stale one the parked read installs. Guards the lock-free account-BPA fix (audit #6).
    #[tokio::test]
    async fn account_bpa_lost_invalidation_is_closed() {
        let counting = Arc::new(CountingStore::new());
        counting
            .submit(Mutation::SetAccountPublicAccessBlock(
                PublicAccessBlock::default(),
            ))
            .await
            .unwrap();
        let cache = Arc::new(CachedMetadataStore::new(counting.clone(), 64 * 1024));

        // Gate the next BPA read so it parks mid-flight (generation already snapshotted).
        counting.bpa_gated.store(true, Ordering::Release);
        let cache2 = cache.clone();
        let read =
            tokio::spawn(async move { cache2.get_account_public_access_block().await.unwrap() });

        // Once the read is parked inside the inner store, invalidate + flip the value to all-true.
        counting.bpa_entered.notified().await;
        cache
            .submit(Mutation::SetAccountPublicAccessBlock(PublicAccessBlock {
                block_public_acls: true,
                ignore_public_acls: true,
                block_public_policy: true,
                restrict_public_buckets: true,
            }))
            .await
            .unwrap();
        // Release the parked read; it installs the OLD value tagged with the now-stale generation.
        counting.bpa_gated.store(false, Ordering::Release);
        counting.bpa_release.notify_one();
        let observed = read.await.unwrap();
        assert!(
            !observed.block_public_acls,
            "the in-flight read returns the pre-change value"
        );

        // The next read must reflect the post-invalidation value, not the stale install.
        let fresh = cache.get_account_public_access_block().await.unwrap();
        assert!(
            fresh.block_public_acls,
            "lost-invalidation: a read after the racing invalidation must serve the NEW value"
        );
    }

    /// An inner store that forwards to an [`InMemoryMetadataStore`] while counting how often each
    /// cached read actually reaches it. Lets a test assert a hit served from the cache without a
    /// second inner call.
    struct CountingStore {
        inner: InMemoryMetadataStore,
        get_bucket: AtomicU64,
        get_config: AtomicU64,
        get_bpa: AtomicU64,
        // When `bpa_gated` is set, `get_account_public_access_block` signals `bpa_entered` and then
        // parks on `bpa_release`, letting a test interleave an invalidation mid-read.
        bpa_gated: std::sync::atomic::AtomicBool,
        bpa_entered: tokio::sync::Notify,
        bpa_release: tokio::sync::Notify,
    }

    impl CountingStore {
        fn new() -> Self {
            Self {
                inner: InMemoryMetadataStore::new(),
                get_bucket: AtomicU64::new(0),
                get_config: AtomicU64::new(0),
                get_bpa: AtomicU64::new(0),
                bpa_gated: std::sync::atomic::AtomicBool::new(false),
                bpa_entered: tokio::sync::Notify::new(),
                bpa_release: tokio::sync::Notify::new(),
            }
        }
    }

    impl std::fmt::Debug for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("CountingStore").finish_non_exhaustive()
        }
    }

    #[async_trait]
    impl MetadataStore for CountingStore {
        async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
            self.inner.submit(mutation).await
        }
        async fn get_bucket(&self, name: &BucketName) -> Result<Option<Bucket>, MetaError> {
            self.get_bucket.fetch_add(1, Ordering::Relaxed);
            self.inner.get_bucket(name).await
        }
        async fn list_buckets(&self, owner: Option<&UserId>) -> Result<Vec<Bucket>, MetaError> {
            self.inner.list_buckets(owner).await
        }
        async fn get_bucket_config(
            &self,
            name: &BucketName,
            aspect: ConfigAspect,
        ) -> Result<Option<ConfigDoc>, MetaError> {
            self.get_config.fetch_add(1, Ordering::Relaxed);
            self.inner.get_bucket_config(name, aspect).await
        }
        async fn get_account_public_access_block(&self) -> Result<PublicAccessBlock, MetaError> {
            self.get_bpa.fetch_add(1, Ordering::Relaxed);
            // Capture the value first so a change interleaved during the gate does not affect what
            // THIS in-flight read returns (modeling a read that observed the pre-change state).
            let v = self.inner.get_account_public_access_block().await?;
            if self.bpa_gated.load(Ordering::Acquire) {
                self.bpa_entered.notify_one();
                self.bpa_release.notified().await;
            }
            Ok(v)
        }
        async fn is_bucket_empty(&self, name: &BucketName) -> Result<bool, MetaError> {
            self.inner.is_bucket_empty(name).await
        }
        async fn current_version(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
        ) -> Result<Option<ObjectVersionRow>, MetaError> {
            self.inner.current_version(bucket, key).await
        }
        async fn get_version(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version: &VersionId,
        ) -> Result<Option<ObjectVersionRow>, MetaError> {
            self.inner.get_version(bucket, key, version).await
        }
        async fn list_current(
            &self,
            bucket: &BucketName,
            query: &ListQuery,
        ) -> Result<ListPage<ObjectSummary>, MetaError> {
            self.inner.list_current(bucket, query).await
        }
        async fn list_versions(
            &self,
            bucket: &BucketName,
            query: &ListQuery,
        ) -> Result<ListPage<ObjectSummary>, MetaError> {
            self.inner.list_versions(bucket, query).await
        }
        async fn enumerate_storage_paths(
            &self,
            bucket: &BucketName,
            cursor: Option<&str>,
            batch: u32,
        ) -> Result<ListPage<StoragePath>, MetaError> {
            self.inner
                .enumerate_storage_paths(bucket, cursor, batch)
                .await
        }
        async fn get_object_tags(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version: &VersionId,
        ) -> Result<Vec<(String, String)>, MetaError> {
            self.inner.get_object_tags(bucket, key, version).await
        }
        async fn get_object_lock(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version: &VersionId,
        ) -> Result<cairn_types::object::ObjectLockState, MetaError> {
            self.inner.get_object_lock(bucket, key, version).await
        }
        async fn get_multipart(
            &self,
            upload: &UploadId,
        ) -> Result<Option<MultipartSession>, MetaError> {
            self.inner.get_multipart(upload).await
        }
        async fn list_parts(
            &self,
            upload: &UploadId,
            part_number_marker: u16,
            limit: u32,
        ) -> Result<ListPage<PartRecord>, MetaError> {
            self.inner
                .list_parts(upload, part_number_marker, limit)
                .await
        }
        async fn list_multipart_uploads(
            &self,
            bucket: &BucketName,
            query: &ListQuery,
        ) -> Result<ListPage<MultipartSession>, MetaError> {
            self.inner.list_multipart_uploads(bucket, query).await
        }
        async fn enumerate_stale_sessions(
            &self,
            older_than: Timestamp,
            batch: u32,
        ) -> Result<Vec<MultipartSession>, MetaError> {
            self.inner.enumerate_stale_sessions(older_than, batch).await
        }
        async fn object_replication_status(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version: &VersionId,
        ) -> Result<Option<ReplicationStatus>, MetaError> {
            self.inner
                .object_replication_status(bucket, key, version)
                .await
        }
        async fn has_unreplicated_predecessor(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            before: &VersionId,
            target: Option<&str>,
        ) -> Result<bool, MetaError> {
            self.inner
                .has_unreplicated_predecessor(bucket, key, before, target)
                .await
        }
        async fn claim_replication_batch(
            &self,
            limit: u32,
            now: Timestamp,
        ) -> Result<Vec<OutboxEntry>, MetaError> {
            self.inner.claim_replication_batch(limit, now).await
        }
        async fn list_due_replication(
            &self,
            limit: u32,
            now: Timestamp,
        ) -> Result<Vec<OutboxEntry>, MetaError> {
            self.inner.list_due_replication(limit, now).await
        }
        async fn list_failed_replication(&self, limit: u32) -> Result<Vec<OutboxEntry>, MetaError> {
            self.inner.list_failed_replication(limit).await
        }
        async fn replication_counts(
            &self,
            bucket: Option<&BucketName>,
        ) -> Result<ReplicationCounts, MetaError> {
            self.inner.replication_counts(bucket).await
        }
        async fn claim_webhook_batch(
            &self,
            limit: u32,
            now: Timestamp,
        ) -> Result<Vec<WebhookEntry>, MetaError> {
            self.inner.claim_webhook_batch(limit, now).await
        }
        async fn list_due_webhooks(
            &self,
            limit: u32,
            now: Timestamp,
        ) -> Result<Vec<WebhookEntry>, MetaError> {
            self.inner.list_due_webhooks(limit, now).await
        }
        async fn list_failed_webhooks(&self, limit: u32) -> Result<Vec<WebhookEntry>, MetaError> {
            self.inner.list_failed_webhooks(limit).await
        }
        async fn get_bucket_quota(&self, bucket: &BucketName) -> Result<Option<u64>, MetaError> {
            self.inner.get_bucket_quota(bucket).await
        }
        async fn user_by_bearer_key(
            &self,
            access_key_id: &str,
        ) -> Result<Option<UserWithBearerHash>, MetaError> {
            self.inner.user_by_bearer_key(access_key_id).await
        }
        async fn user_by_sigv4_key(
            &self,
            access_key_id: &str,
        ) -> Result<Option<UserSigV4Credentials>, MetaError> {
            self.inner.user_by_sigv4_key(access_key_id).await
        }
        async fn user_by_session_key(
            &self,
            access_key_id: &str,
        ) -> Result<Option<UserSessionCredentials>, MetaError> {
            self.inner.user_by_session_key(access_key_id).await
        }
        async fn list_session_credentials(
            &self,
            now: Timestamp,
        ) -> Result<Vec<SessionCredentialSummary>, MetaError> {
            self.inner.list_session_credentials(now).await
        }
        async fn count_users(&self) -> Result<u64, MetaError> {
            self.inner.count_users().await
        }
        async fn list_users(&self) -> Result<Vec<User>, MetaError> {
            self.inner.list_users().await
        }
        async fn get_user_policy(&self, user_id: &UserId) -> Result<Option<String>, MetaError> {
            self.inner.get_user_policy(user_id).await
        }
        async fn list_import_jobs(&self) -> Result<Vec<ImportJob>, MetaError> {
            self.inner.list_import_jobs().await
        }
        async fn get_import_job(&self, id: &str) -> Result<Option<ImportJob>, MetaError> {
            self.inner.get_import_job(id).await
        }
        async fn get_import_job_record(
            &self,
            id: &str,
        ) -> Result<Option<ImportJobRecord>, MetaError> {
            self.inner.get_import_job_record(id).await
        }
        async fn list_activity(&self, limit: u32) -> Result<Vec<ActivityEntry>, MetaError> {
            self.inner.list_activity(limit).await
        }
        async fn get_share(&self, token: &str) -> Result<Option<ShareRow>, MetaError> {
            self.inner.get_share(token).await
        }
        async fn list_shares(
            &self,
            bucket: &BucketName,
            key: Option<&ObjectKey>,
        ) -> Result<Vec<ShareRow>, MetaError> {
            self.inner.list_shares(bucket, key).await
        }
        async fn list_tag_summary(
            &self,
            bucket: Option<&BucketName>,
        ) -> Result<Vec<TagSummary>, MetaError> {
            self.inner.list_tag_summary(bucket).await
        }
        async fn list_objects_by_tag(
            &self,
            bucket: Option<&BucketName>,
            tag_key: &str,
            tag_value: &str,
            limit: u32,
        ) -> Result<Vec<TaggedObject>, MetaError> {
            self.inner
                .list_objects_by_tag(bucket, tag_key, tag_value, limit)
                .await
        }
        async fn aggregate_counts(&self) -> Result<StoreCounts, MetaError> {
            self.inner.aggregate_counts().await
        }
        async fn bucket_counts(&self) -> Result<Vec<BucketCounts>, MetaError> {
            self.inner.bucket_counts().await
        }
        async fn query_request_metrics(
            &self,
            range: MetricsRange,
            now_secs: i64,
        ) -> Result<RequestMetricsSeries, MetaError> {
            self.inner.query_request_metrics(range, now_secs).await
        }
    }

    fn bucket_name(s: &str) -> BucketName {
        BucketName::parse(s).expect("valid bucket name")
    }

    fn make_bucket(name: &BucketName) -> Bucket {
        Bucket {
            name: name.clone(),
            owner_id: UserId("owner".into()),
            created_at: Timestamp::from_secs(1),
            versioning: VersioningState::Unversioned,
            ownership_mode: OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".into(),
            compression: None,
        }
    }

    async fn seed_bucket(store: &CountingStore, name: &BucketName) {
        store
            .submit(Mutation::CreateBucket(Box::new(make_bucket(name))))
            .await
            .expect("create bucket");
    }

    /// (a) A second read of the same config is served from the cache without re-hitting inner.
    #[tokio::test]
    async fn hit_serves_without_second_inner_call() {
        let counting = Arc::new(CountingStore::new());
        let name = bucket_name("hot-bucket");
        seed_bucket(&counting, &name).await;
        counting
            .submit(Mutation::SetBucketConfig {
                bucket: name.clone(),
                aspect: ConfigAspect::Policy,
                doc: Some(ConfigDoc("{\"v\":1}".into())),
            })
            .await
            .expect("set config");

        let cache = CachedMetadataStore::new(counting.clone(), 64 * 1024);

        let first = cache
            .get_bucket_config(&name, ConfigAspect::Policy)
            .await
            .unwrap();
        let second = cache
            .get_bucket_config(&name, ConfigAspect::Policy)
            .await
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(first, Some(ConfigDoc("{\"v\":1}".into())));
        // Only the first call reached the inner store.
        assert_eq!(counting.get_config.load(Ordering::Relaxed), 1);
        let (hits, misses) = cache.stats();
        assert_eq!((hits, misses), (1, 1));

        // Negative results cache too: a missing aspect is fetched once, then served from cache.
        let none1 = cache
            .get_bucket_config(&name, ConfigAspect::Cors)
            .await
            .unwrap();
        let none2 = cache
            .get_bucket_config(&name, ConfigAspect::Cors)
            .await
            .unwrap();
        assert_eq!(none1, None);
        assert_eq!(none2, None);
        // get_config saw exactly one more call (the first Cors fetch).
        assert_eq!(counting.get_config.load(Ordering::Relaxed), 2);

        // get_bucket and account-BPA also cache.
        let _ = cache.get_bucket(&name).await.unwrap();
        let _ = cache.get_bucket(&name).await.unwrap();
        assert_eq!(counting.get_bucket.load(Ordering::Relaxed), 1);
        let _ = cache.get_account_public_access_block().await.unwrap();
        let _ = cache.get_account_public_access_block().await.unwrap();
        assert_eq!(counting.get_bpa.load(Ordering::Relaxed), 1);
    }

    /// The public out-of-band evict (used by the #29 key-rewrap worker, which re-seals replication
    /// targets via a raw-store CAS that bypasses `submit`) drops the cached aspect so the next read
    /// re-fetches the freshly-re-sealed doc (audit 2026-07).
    #[tokio::test]
    async fn invalidate_config_aspect_evicts_out_of_band() {
        let counting = Arc::new(CountingStore::new());
        let name = bucket_name("rewrap-bucket");
        seed_bucket(&counting, &name).await;
        counting
            .submit(Mutation::SetBucketConfig {
                bucket: name.clone(),
                aspect: ConfigAspect::ReplicationTargets,
                doc: Some(ConfigDoc("old-key-doc".into())),
            })
            .await
            .expect("set config");

        let cache = CachedMetadataStore::new(counting.clone(), 64 * 1024);
        // Prime the cache: first read misses (reaches the store), second is served from cache.
        let _ = cache
            .get_bucket_config(&name, ConfigAspect::ReplicationTargets)
            .await
            .unwrap();
        let _ = cache
            .get_bucket_config(&name, ConfigAspect::ReplicationTargets)
            .await
            .unwrap();
        assert_eq!(counting.get_config.load(Ordering::Relaxed), 1);

        // Out-of-band evict — the next read must reach the inner store again.
        cache.invalidate_config_aspect(&name, ConfigAspect::ReplicationTargets);
        let _ = cache
            .get_bucket_config(&name, ConfigAspect::ReplicationTargets)
            .await
            .unwrap();
        assert_eq!(
            counting.get_config.load(Ordering::Relaxed),
            2,
            "after eviction the read re-fetches from the store"
        );
    }

    /// (b) A submit that changes a bucket's policy invalidates it so the next read re-fetches.
    #[tokio::test]
    async fn submit_invalidates_changed_bucket() {
        let counting = Arc::new(CountingStore::new());
        let name = bucket_name("mut-bucket");
        seed_bucket(&counting, &name).await;
        counting
            .submit(Mutation::SetBucketConfig {
                bucket: name.clone(),
                aspect: ConfigAspect::Policy,
                doc: Some(ConfigDoc("old".into())),
            })
            .await
            .expect("set config");

        let cache = CachedMetadataStore::new(counting.clone(), 64 * 1024);

        // Prime the cache.
        let v0 = cache
            .get_bucket_config(&name, ConfigAspect::Policy)
            .await
            .unwrap();
        assert_eq!(v0, Some(ConfigDoc("old".into())));
        assert_eq!(counting.get_config.load(Ordering::Relaxed), 1);

        // Change the policy through the decorator: this must invalidate the cached entry.
        cache
            .submit(Mutation::SetBucketConfig {
                bucket: name.clone(),
                aspect: ConfigAspect::Policy,
                doc: Some(ConfigDoc("new".into())),
            })
            .await
            .expect("update config");

        // Next read re-reads inner and sees the new value.
        let v1 = cache
            .get_bucket_config(&name, ConfigAspect::Policy)
            .await
            .unwrap();
        assert_eq!(v1, Some(ConfigDoc("new".into())));
        assert_eq!(
            counting.get_config.load(Ordering::Relaxed),
            2,
            "must have re-read inner after invalidation"
        );
    }

    /// (c) `budget_bytes == 0` is a pure pass-through: every read hits inner, nothing is cached.
    #[tokio::test]
    async fn zero_budget_always_forwards() {
        let counting = Arc::new(CountingStore::new());
        let name = bucket_name("nocache");
        seed_bucket(&counting, &name).await;
        counting
            .submit(Mutation::SetBucketConfig {
                bucket: name.clone(),
                aspect: ConfigAspect::Policy,
                doc: Some(ConfigDoc("x".into())),
            })
            .await
            .expect("set config");

        let cache = CachedMetadataStore::new(counting.clone(), 0);

        for _ in 0..3 {
            let _ = cache
                .get_bucket_config(&name, ConfigAspect::Policy)
                .await
                .unwrap();
            let _ = cache.get_bucket(&name).await.unwrap();
            let _ = cache.get_account_public_access_block().await.unwrap();
        }

        assert_eq!(counting.get_config.load(Ordering::Relaxed), 3);
        assert_eq!(counting.get_bucket.load(Ordering::Relaxed), 3);
        assert_eq!(counting.get_bpa.load(Ordering::Relaxed), 3);
        // Pass-through records misses, never hits.
        let (hits, misses) = cache.stats();
        assert_eq!(hits, 0);
        assert_eq!(misses, 9);
    }
}
