//! A short-lived, epoch-invalidated cache for the authentication hot path (Phase 1.3, ARCH §30).
//!
//! Every authenticated request otherwise pays two metadata reads — the credential lookup by
//! access-key-id and the identity-policy load by user-id — plus a JSON policy parse, all *before*
//! the request's own work begins. Under load those reads contend on the WAL read pool and the
//! parse burns CPU per request. This cache memoizes both, keyed by access-key-id (credentials) and
//! user-id (parsed policy), so a steady stream of requests from the same identity skips them.
//!
//! ## What is and is not cached
//! The plaintext SigV4 secret is **never** cached — only its sealed form (`secret_ciphertext` +
//! `secret_nonce`), exactly as it already lives in the SQLite page cache. The day-scoped signing
//! key is re-derived per request from that sealed secret, so the security-critical signature
//! verification math is byte-for-byte unchanged; only the *source* of the credential and policy
//! moves from the database to memory.
//!
//! ## Coherency
//! Each entry is tagged with the value of a shared **auth epoch** (an `AtomicU64` the metadata
//! layer bumps on every user-identity mutation) observed when it was fetched, and with a monotonic
//! expiry `Instant`. An entry is served only while its tag equals the current epoch and it has not
//! expired; a value is installed only if the epoch did not advance between the fetch snapshot and
//! the install. Together these close the same TOCTOU window the config cache guards: a credential
//! or policy change takes effect immediately (epoch bump drops every stale entry), and the TTL is
//! a belt-and-braces bound on staleness for entries no mutation ever touches.

use cairn_types::auth::Role;
use cairn_types::authz::Policy;
use cairn_types::id::UserId;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Lock shards. A power of two keeps the hash-to-shard reduction a cheap mask, mirroring the
/// metadata config cache's idiom so hot access-key lookups rarely contend.
const SHARDS: usize = 16;

/// Per-shard entry cap. User counts are small, so a coarse "clear the shard when full" bound keeps
/// memory capped without per-entry LRU bookkeeping; eviction here is effectively never hit.
const MAX_ENTRIES_PER_SHARD: usize = 1024;

/// The cached SigV4 identity for one access-key-id: the user fields a [`Principal`] needs, plus the
/// **sealed** secret (never the plaintext). The signing key is re-derived from the sealed secret
/// per request, so possessing this entry does not shortcut signature verification.
///
/// [`Principal`]: cairn_types::auth::Principal
#[derive(Clone)]
pub struct CachedSigv4 {
    /// The owning user's stable id.
    pub user_id: UserId,
    /// The user's display name (carried into the principal).
    pub display_name: String,
    /// The user's role.
    pub role: Role,
    /// The SigV4 secret sealed under the master key — decrypted per request, never stored in clear.
    pub secret_ciphertext: Vec<u8>,
    /// The nonce for [`Self::secret_ciphertext`].
    pub secret_nonce: Vec<u8>,
}

impl std::fmt::Debug for CachedSigv4 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the sealed secret material; surface only the non-sensitive identity fields.
        f.debug_struct("CachedSigv4")
            .field("user_id", &self.user_id)
            .field("display_name", &self.display_name)
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

/// The cached Bearer identity for one access-key-id: the user fields plus the stored secret hash
/// the presented token is compared against (a hash, never the secret).
#[derive(Clone)]
pub struct CachedBearer {
    /// The owning user's stable id.
    pub user_id: UserId,
    /// The user's display name (carried into the principal).
    pub display_name: String,
    /// The user's role.
    pub role: Role,
    /// The stored Bearer secret hash to compare the presented token against.
    pub secret_hash: String,
}

impl std::fmt::Debug for CachedBearer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the secret hash; surface only the non-sensitive identity fields.
        f.debug_struct("CachedBearer")
            .field("user_id", &self.user_id)
            .field("display_name", &self.display_name)
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

/// One cached value with its coherency tags.
struct Entry<V> {
    val: V,
    epoch: u64,
    expires: Instant,
}

/// A sharded `K -> V` map with epoch + TTL coherency, all values `Clone`-cheap (small structs or
/// `Arc`s).
struct Sharded<K, V> {
    shards: Vec<Mutex<HashMap<K, Entry<V>>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> Sharded<K, V> {
    fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
        }
    }

    fn shard(&self, key: &K) -> &Mutex<HashMap<K, Entry<V>>> {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        &self.shards[(h.finish() as usize) & (SHARDS - 1)]
    }

    /// Return the value iff it is tagged with the current `epoch` and has not expired; a stale or
    /// expired entry is removed in passing so the map self-prunes on access.
    fn get(&self, key: &K, epoch: u64, now: Instant) -> Option<V> {
        let mut g = self.shard(key).lock().unwrap();
        match g.get(key) {
            Some(e) if e.epoch == epoch && e.expires > now => Some(e.val.clone()),
            Some(_) => {
                g.remove(key);
                None
            }
            None => None,
        }
    }

    fn put(&self, key: K, val: V, epoch: u64, expires: Instant) {
        let mut g = self.shard(&key).lock().unwrap();
        if g.len() >= MAX_ENTRIES_PER_SHARD && !g.contains_key(&key) {
            g.clear();
        }
        g.insert(
            key,
            Entry {
                val,
                epoch,
                expires,
            },
        );
    }
}

/// The authentication cache: sealed-credential and parsed-policy memoization with shared-epoch +
/// TTL invalidation. A `ttl` of zero disables it (every lookup misses), so the authenticator
/// behaves exactly as before when the operator turns it off.
pub struct AuthCache {
    ttl: Duration,
    epoch: Arc<AtomicU64>,
    sigv4: Sharded<String, CachedSigv4>,
    bearer: Sharded<String, CachedBearer>,
    /// Keyed by user-id. The inner `Option` distinguishes "user has a (parsed) policy" from "user
    /// has no policy / a remembered-malformed one"; both are cached so neither re-reads the DB.
    policy: Sharded<UserId, Option<Arc<Policy>>>,
}

impl std::fmt::Debug for AuthCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthCache")
            .field("ttl", &self.ttl)
            .field("epoch", &self.epoch.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl AuthCache {
    /// Build a cache with time-to-live `ttl`, sharing user-mutation `epoch` with the metadata
    /// layer. `ttl == 0` yields a disabled cache.
    #[must_use]
    pub fn new(ttl: Duration, epoch: Arc<AtomicU64>) -> Self {
        Self {
            ttl,
            epoch,
            sigv4: Sharded::new(),
            bearer: Sharded::new(),
            policy: Sharded::new(),
        }
    }

    fn enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    /// Snapshot the current epoch — call this *before* a metadata fetch on a miss, then pass the
    /// returned value to the matching `put_*` so a value fetched against a now-superseded epoch is
    /// not installed.
    #[must_use]
    pub fn observe_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// A cached SigV4 identity for `access_key_id`, or `None` on miss/disabled.
    #[must_use]
    pub fn get_sigv4(&self, access_key_id: &str) -> Option<CachedSigv4> {
        if !self.enabled() {
            return None;
        }
        self.sigv4.get(
            &access_key_id.to_owned(),
            self.observe_epoch(),
            Instant::now(),
        )
    }

    /// Install a SigV4 identity, unless a user mutation advanced the epoch since `observed_epoch`.
    pub fn put_sigv4(&self, access_key_id: &str, creds: CachedSigv4, observed_epoch: u64) {
        if !self.enabled() || self.observe_epoch() != observed_epoch {
            return;
        }
        self.sigv4.put(
            access_key_id.to_owned(),
            creds,
            observed_epoch,
            Instant::now() + self.ttl,
        );
    }

    /// A cached Bearer identity for `access_key_id`, or `None` on miss/disabled.
    #[must_use]
    pub fn get_bearer(&self, access_key_id: &str) -> Option<CachedBearer> {
        if !self.enabled() {
            return None;
        }
        self.bearer.get(
            &access_key_id.to_owned(),
            self.observe_epoch(),
            Instant::now(),
        )
    }

    /// Install a Bearer identity, unless a user mutation advanced the epoch since `observed_epoch`.
    pub fn put_bearer(&self, access_key_id: &str, creds: CachedBearer, observed_epoch: u64) {
        if !self.enabled() || self.observe_epoch() != observed_epoch {
            return;
        }
        self.bearer.put(
            access_key_id.to_owned(),
            creds,
            observed_epoch,
            Instant::now() + self.ttl,
        );
    }

    /// A cached parsed policy for `user_id`. The outer `Option` is hit/miss; the inner is
    /// present/absent policy. `None` (miss) means "go read the DB".
    #[must_use]
    pub fn get_policy(&self, user_id: &UserId) -> Option<Option<Arc<Policy>>> {
        if !self.enabled() {
            return None;
        }
        self.policy
            .get(user_id, self.observe_epoch(), Instant::now())
    }

    /// Install a parsed policy (or a remembered absence), unless the epoch advanced since
    /// `observed_epoch`.
    pub fn put_policy(&self, user_id: &UserId, policy: Option<Arc<Policy>>, observed_epoch: u64) {
        if !self.enabled() || self.observe_epoch() != observed_epoch {
            return;
        }
        self.policy.put(
            user_id.clone(),
            policy,
            observed_epoch,
            Instant::now() + self.ttl,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> Policy {
        Policy {
            version: "2012-10-17".to_owned(),
            id: None,
            statements: Vec::new(),
        }
    }

    fn creds() -> CachedSigv4 {
        CachedSigv4 {
            user_id: UserId("u".to_owned()),
            display_name: "U".to_owned(),
            role: Role::Member,
            secret_ciphertext: vec![1, 2, 3],
            secret_nonce: vec![4, 5, 6],
        }
    }

    #[test]
    fn hit_then_epoch_bump_invalidates() {
        let epoch = Arc::new(AtomicU64::new(0));
        let cache = AuthCache::new(Duration::from_secs(60), epoch.clone());
        let observed = cache.observe_epoch();
        cache.put_sigv4("AK", creds(), observed);
        assert!(cache.get_sigv4("AK").is_some(), "fresh entry must hit");

        // A user mutation bumps the shared epoch; the entry must no longer be served.
        epoch.fetch_add(1, Ordering::Release);
        assert!(
            cache.get_sigv4("AK").is_none(),
            "entry tagged with the old epoch must miss after a bump"
        );
    }

    #[test]
    fn put_is_dropped_if_epoch_advanced_during_fetch() {
        let epoch = Arc::new(AtomicU64::new(0));
        let cache = AuthCache::new(Duration::from_secs(60), epoch.clone());
        // Snapshot, then a mutation lands before we install: the install must be refused so a
        // value fetched from a pre-mutation view is never cached.
        let observed = cache.observe_epoch();
        epoch.fetch_add(1, Ordering::Release);
        cache.put_sigv4("AK", creds(), observed);
        assert!(
            cache.get_sigv4("AK").is_none(),
            "a value observed at a superseded epoch must not be installed"
        );
    }

    #[test]
    fn ttl_zero_disables() {
        let epoch = Arc::new(AtomicU64::new(0));
        let cache = AuthCache::new(Duration::ZERO, epoch);
        let observed = cache.observe_epoch();
        cache.put_sigv4("AK", creds(), observed);
        assert!(
            cache.get_sigv4("AK").is_none(),
            "ttl=0 must disable caching"
        );
    }

    #[test]
    fn ttl_expiry_misses() {
        let epoch = Arc::new(AtomicU64::new(0));
        // A 1ns TTL is effectively already-expired by the time we read it back.
        let cache = AuthCache::new(Duration::from_nanos(1), epoch);
        let observed = cache.observe_epoch();
        cache.put_sigv4("AK", creds(), observed);
        std::thread::sleep(Duration::from_millis(2));
        assert!(cache.get_sigv4("AK").is_none(), "expired entry must miss");
    }

    #[test]
    fn policy_caches_presence_and_absence() {
        let epoch = Arc::new(AtomicU64::new(0));
        let cache = AuthCache::new(Duration::from_secs(60), epoch);
        let uid = UserId("u".to_owned());
        let observed = cache.observe_epoch();

        // Absence is cached and distinguished from a miss.
        cache.put_policy(&uid, None, observed);
        assert!(
            matches!(cache.get_policy(&uid), Some(None)),
            "absence hits as Some(None)"
        );

        // Presence is cached as a shared Arc.
        cache.put_policy(&uid, Some(Arc::new(policy())), observed);
        assert!(
            matches!(cache.get_policy(&uid), Some(Some(_))),
            "presence hits"
        );

        // An unknown user is a miss (outer None).
        assert!(cache.get_policy(&UserId("other".to_owned())).is_none());
    }
}
