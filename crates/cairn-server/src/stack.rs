//! Builds and owns the concrete engine stack — the only place that names concrete
//! implementations (ARCH 12.7). It opens the metadata store and blob store, wires the
//! authenticator chain and the S3 service, and runs startup reconciliation before serving.

use crate::config::Config;
use cairn_auth::AuthChain;
use cairn_blob::LocalBlobStore;
use cairn_crypto::{SystemClock, SystemCrypto};
use cairn_meta::{
    CachedMetadataStore, KeyHashBindingMatch, KeyRingStateRow, OpenOptions, SqliteMetadataStore,
    classify_key_hash_binding,
};
use cairn_protocol::S3Service;
use cairn_types::blob::ReconcileOpts;
use cairn_types::traits::{
    Authenticator, AuthorizationEngine, BlobStore, Clock, Crypto, MetadataStore, ReconcileOracle,
};
use std::sync::Arc;

/// What [`open_meta`] yields: the (possibly sharded) trait-object store, the boxed reconcile oracle,
/// and the per-shard typed sqlite handles for the WAL checkpointer (empty for libSQL/Turso).
type OpenedMeta = (
    Arc<dyn MetadataStore>,
    Box<dyn ReconcileOracle + Send + Sync>,
    Vec<Arc<SqliteMetadataStore>>,
);

/// One opened sqlite shard: its trait-object store, reconcile oracle, and typed handle.
type OpenedShard = (
    Arc<dyn MetadataStore>,
    Box<dyn ReconcileOracle + Send + Sync>,
    Arc<SqliteMetadataStore>,
);

/// The assembled runtime stack shared across requests.
pub struct AppStack {
    /// The S3 protocol service.
    pub s3: S3Service,
    /// The management JSON API service.
    pub control: cairn_control::ControlService,
    /// Shared release-update status, written by the background update-check loop and read live by
    /// `GET /system` (a clone is also held inside `SystemInfo` so the console sees the freshest value).
    pub update_status: Arc<std::sync::RwLock<cairn_control::UpdateStatus>>,
    /// The authenticator chain.
    pub auth: Arc<dyn Authenticator>,
    /// The same authenticator chain behind its concrete type, kept so the STS wire surface can call
    /// `AuthChain::authenticate_sts` (an inherent method that hosts an `sts`-scoped signature; the
    /// generic `Authenticator::authenticate` deliberately rejects a non-`s3` scope). Mirrors the
    /// `store: Vec<Arc<SqliteMetadataStore>>` concrete-handle-alongside-`dyn` pattern above; one
    /// extra startup `Arc` clone, zero per-request cost.
    pub auth_chain: Arc<AuthChain>,
    /// Whether the AWS-STS wire surface is served on the S3 data plane (`CAIRN_STS_ENABLED`, ARCH
    /// 14). When `false`, a form `POST /` on the S3 listener is not intercepted for STS.
    pub sts_enabled: bool,
    /// The metadata store behind its trait object, used by request handlers, the readiness
    /// probe, and the background subsystems (multipart sweeper, lifecycle scanner). Backend-
    /// agnostic: it is the sqlite, libSQL, or Turso store depending on `CAIRN_META_BACKEND`.
    pub meta: Arc<dyn MetadataStore>,
    /// The blob store. Held for the background subsystems (sweeper, periodic reconcile).
    #[allow(dead_code)]
    pub blob: Arc<dyn BlobStore>,
    /// The same local blob store behind its concrete type, kept so the metrics loop can scrape
    /// `plaintext_length_mismatch_total()` into `cairn_blob_plaintext_length_mismatch_total` — an inherent
    /// accessor, not part of the `BlobStore` trait object. Mirrors the `meta_cache` /
    /// `store: Vec<Arc<SqliteMetadataStore>>` concrete-handle-alongside-`dyn` pattern; one extra
    /// startup `Arc` clone, zero per-request cost.
    pub blob_local: Arc<LocalBlobStore>,
    /// The reconciliation oracle behind its trait object. Held for periodic out-of-band reconcile.
    /// Boxed because the concrete oracle type differs per backend (sqlite vs the shared async one).
    #[allow(dead_code)]
    pub oracle: Box<dyn ReconcileOracle + Send + Sync>,
    /// Typed handles to the concrete SQLite shard stores, **only populated for the `sqlite`
    /// backend** (one per `CAIRN_META_SHARDS`; a single entry when unsharded). The WAL
    /// checkpointer's `checkpoint()` and `wal_size_bytes()` are inherent methods on
    /// `SqliteMetadataStore`, not part of the `MetadataStore` trait object, so the concrete stores
    /// are threaded through here rather than reached via `meta` (ARCH 8.4/11.2). The libSQL and
    /// Turso engines self-manage their WAL, so this is **empty** for them and the WAL-checkpointer
    /// background loop does not run.
    pub store: Vec<Arc<SqliteMetadataStore>>,
    /// A typed handle to the read-through config cache wrapping `meta`, kept so the metrics loop can
    /// scrape its `(hits, misses)` counters into `cairn_meta_cache_hits_total`/`_misses_total`
    /// (ARCH 11.5). `meta` above is this same store behind the trait object; this handle exists
    /// only for the inherent `stats()` accessor, which is not part of the `MetadataStore` trait.
    pub meta_cache: Arc<CachedMetadataStore>,
    /// The master-key crypto facility, threaded to the replication drain so it can unseal stored
    /// per-bucket remote replication targets (`ConfigAspect::ReplicationTargets`, ARCH 20.5).
    pub crypto: Arc<SystemCrypto>,
    /// Pulsed by the S3 write path after a write commits replication outbox entries, so the
    /// replication worker drains immediately (event-driven) instead of waiting its poll heartbeat.
    pub replication_notify: Arc<tokio::sync::Notify>,
    /// Pulsed by the control plane when an import job is created/resumed, so the import worker claims
    /// it immediately instead of waiting its poll heartbeat (mirrors `replication_notify`).
    pub import_notify: Arc<tokio::sync::Notify>,
    /// Process-local queue that restores multipart `completing` claims and resolves staged
    /// PUT/Copy blobs when an owning request future is cancelled. Its producers are injected into
    /// `S3Service`; its single receiver is retained and shutdown-drained by the background
    /// supervisor.
    pub multipart_claim_recovery: Arc<crate::multipart_claim_recovery::MultipartClaimRecoveryQueue>,
    /// Short-lived, single-use tickets for the SSE live-update stream (`hash -> (expiry_ms,
    /// minting principal)`). EventSource cannot send an Authorization header, so the browser mints
    /// a ticket with its Bearer token then opens the stream with `?ticket=`. In-process and
    /// node-local; nothing durable.
    pub sse_tickets: crate::sse::SseTicketStore,
    /// The base domain for virtual-host-style S3 addressing (`CAIRN_S3_DOMAIN`, ARCH 13.1), e.g.
    /// `s3.example.com`. When set, a request whose `Host` is `<bucket>.<s3_domain>` routes to that
    /// bucket with the whole path as the key; `None` leaves path-style addressing as the only form.
    pub s3_domain: Option<String>,
    /// The SigV4 signing region (`CAIRN_REGION`), used when minting presigned URLs so the
    /// credential scope matches what the verifier derives.
    pub region: String,
    /// Whether operator-configured outbound dialers may reach internal addresses
    /// (`CAIRN_ALLOW_INTERNAL_ENDPOINTS`). Threaded into every replication/webhook/import sink so the
    /// SSRF guard's connect-time policy is server-global and consistent. Default `false` (enforcing).
    pub allow_internal_endpoints: bool,
    /// Whether a CLIENT-encrypted (SSE-S3 / SSE-KMS) object may be replicated — as the decrypted
    /// body, which is what replication ships — to a plaintext `http://` endpoint
    /// (`CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP`). Threaded into every replication sink
    /// (env single target, env named targets, and stored per-bucket targets) so the policy is
    /// server-global. Default `false` (refuse, and reschedule the object rather than fail it).
    pub replication_allow_plaintext_sse_over_http: bool,
    /// The public base URL (`CAIRN_PUBLIC_BASE_URL`) shares/presigned links are built against; when
    /// `None`, the minting request's own scheme + Host is used.
    pub public_base_url: Option<String>,
    /// The data-listener address. Without an explicit public base URL, control-plane URL minting
    /// combines this port with the request Host's hostname to keep object bytes on a distinct
    /// origin.
    pub data_listen_addr: std::net::SocketAddr,
    /// The in-process request-metrics aggregator (ARCH 26.5). Every completed request bumps a
    /// counter here (zero DB I/O on the hot path); the background flush loop drains it into a
    /// batched upsert through the single writer. Held behind an `Arc` so the request path and the
    /// flush loop share one accumulator.
    pub request_metrics: Arc<crate::metrics_agg::RequestMetricsAgg>,
}

impl std::fmt::Debug for AppStack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppStack").finish_non_exhaustive()
    }
}

/// Build the cryptography facility from the configured master key ring (or single key, or a
/// development key). With a ring (`CAIRN_MASTER_KEY_RING`, audit #29) new seals use the active key
/// (`CAIRN_MASTER_KEY_ACTIVE_ID`, default = highest id) and the legacy (pre-ring, no-magic) blobs
/// decrypt under the lowest id — the conventional original key (3.4.1). The Phase-E seal-count
/// base is primed later from durable state (`prime_seal_count`).
pub(crate) fn build_crypto(cfg: &Config) -> Result<SystemCrypto, String> {
    if let Some(ring_json) = &cfg.master_key_ring {
        let keys = crate::config::parse_key_ring(ring_json)
            .map_err(|e| format!("invalid CAIRN_MASTER_KEY_RING {e}"))?;
        let max_id = keys
            .iter()
            .map(|(id, _)| *id)
            .max()
            .expect("ring is non-empty");
        let min_id = keys
            .iter()
            .map(|(id, _)| *id)
            .min()
            .expect("ring is non-empty");
        let active = cfg.master_key_active_id.unwrap_or(max_id);
        if cfg.master_key.is_some() {
            tracing::debug!("CAIRN_MASTER_KEY_RING is set; CAIRN_MASTER_KEY is ignored");
        }
        SystemCrypto::from_ring(keys, active, min_id, 0)
            .map_err(|e| format!("invalid master key ring: {e}"))
    } else if let Some(hex) = &cfg.master_key {
        SystemCrypto::from_hex(hex).map_err(|e| format!("invalid master_key: {e}"))
    } else {
        tracing::warn!(
            "no master_key configured; using a fixed DEVELOPMENT key (insecure). Set CAIRN_MASTER_KEY in production."
        );
        Ok(SystemCrypto::new(cairn_types::SecretKey32::new([0u8; 32])))
    }
}

/// The `(id, key_hash, is_active)` rows durably bound in `key_ring_state` (audit #29).
///
/// The durable hash is the full 64-hex SHA-256(key) — never key material. The status API abbreviates
/// it separately. Unlike the old best-effort display seeding, deriving this identity cannot
/// silently fall back: invalid configuration is a startup error, and an existing id may never be
/// rebound to a different hash.
fn ring_for_state(cfg: &Config) -> Result<Vec<(u16, String, bool)>, String> {
    use sha2::{Digest, Sha256};
    let hash = |bytes: &[u8]| hex::encode(Sha256::digest(bytes));
    if let Some(ring_json) = &cfg.master_key_ring {
        let keys = crate::config::parse_key_ring(ring_json)
            .map_err(|e| format!("invalid CAIRN_MASTER_KEY_RING {e}"))?;
        let max_id = keys
            .iter()
            .map(|(id, _)| *id)
            .max()
            .ok_or_else(|| "CAIRN_MASTER_KEY_RING must not be empty".to_owned())?;
        let active = cfg.master_key_active_id.unwrap_or(max_id);
        return Ok(keys
            .iter()
            .map(|(id, k)| (*id, hash(k.expose_secret()), *id == active))
            .collect());
    }
    if let Some(mk) = &cfg.master_key {
        let bytes = hex::decode(mk.expose_secret())
            .map_err(|_| "invalid CAIRN_MASTER_KEY hex".to_owned())?;
        return Ok(vec![(1, hash(&bytes), true)]);
    }
    Ok(vec![(1, hash(&[0u8; 32]), true)]) // development key
}

/// Pure retire-gate decision for one shard (audit #29 / spec 5.4). Given the key ids this shard has
/// ever recorded, the current env ring ids, the active id, and the lowest `done_active_id` across
/// the re-wrap streams, return the removed ids whose data is NOT proven re-wrapped off them.
///
/// In the forward-rotation model (ids increase, `active` is the newest), a removed key K is unsafe
/// iff `K < active` (it is an older key the active one supersedes) AND the re-wrap has not swept past
/// it (`min_done <= K`). A removed key newer than `active`, or one fully swept (`min_done > K`), is
/// safe. An empty result means it is safe to start.
pub(crate) fn retire_gate_unsafe_ids(
    recorded_ids: &[u16],
    env_ids: &std::collections::HashSet<u16>,
    active: u16,
    min_done: u16,
) -> Vec<u16> {
    let mut bad: Vec<u16> = recorded_ids
        .iter()
        .copied()
        .filter(|id| !env_ids.contains(id) && *id < active && min_done <= *id)
        .collect();
    bad.sort_unstable();
    bad.dedup();
    bad
}

/// Validate one shard's durable key identity and retirement state.
///
/// The `Result` inputs are intentional: tests inject both state and progress read failures through
/// this exact decision seam. Either error is fatal; only successful reads may authorize startup.
fn validate_key_gate_reads(
    shard: usize,
    ring: &[(u16, String, bool)],
    env_ids: &std::collections::HashSet<u16>,
    active: u16,
    states: Result<Vec<KeyRingStateRow>, cairn_types::MetaError>,
    progress: Result<Vec<(String, u16)>, cairn_types::MetaError>,
) -> Result<Vec<KeyRingStateRow>, String> {
    let states = states
        .map_err(|e| format!("master-key gate: read key_ring_state on shard {shard}: {e}"))?;
    let progress = progress
        .map_err(|e| format!("master-key gate: read rewrap_progress on shard {shard}: {e}"))?;

    for (id, configured_hash, _) in ring {
        if let Some(stored) = states.iter().find(|row| row.id == *id) {
            match classify_key_hash_binding(&stored.key_hash, configured_hash) {
                KeyHashBindingMatch::Exact | KeyHashBindingMatch::LegacyPrefix => {}
                KeyHashBindingMatch::Mismatch => {
                    return Err(format!(
                        "master-key identity mismatch on shard {shard}: key id {id} is already \
                         bound to different key material or a malformed durable hash; refusing \
                         same-id replacement. Restore the original key bytes for id {id}, or \
                         rotate by adding a new id."
                    ));
                }
            }
        }
    }

    let dones: std::collections::HashMap<String, u16> = progress.into_iter().collect();
    let min_done = crate::key_rewrap::SEALED_SECRET_STREAMS
        .iter()
        .map(|stream| dones.get(stream.name()).copied().unwrap_or(0))
        .min()
        .unwrap_or(0);
    let recorded: Vec<u16> = states.iter().map(|row| row.id).collect();
    let unsafe_ids = retire_gate_unsafe_ids(&recorded, env_ids, active, min_done);
    if !unsafe_ids.is_empty() {
        return Err(format!(
            "audit #29 retire-gate: shard {shard} still holds data sealed under master key id(s) \
             {unsafe_ids:?} that were removed from CAIRN_MASTER_KEY_RING before re-wrap onto the \
             active key {active} completed (re-wrap reached id {min_done}). Restore those key \
             id(s) to the ring and wait for GET /api/v1/system/crypto-status to report them \
             retire_eligible before removing them; refusing to start to avoid unreadable data."
        ));
    }
    Ok(states)
}

/// Fail-closed master-key initialization for every SQLite shard.
///
/// Phase 1 reads and validates every shard — durable id→full-hash identity, re-wrap progress, and
/// the retire gate — before **any** key-state write occurs. A matching legacy eight-hex prefix is
/// accepted only so phase 2 can atomically upgrade it to the configured full hash through each
/// Writer. That write defensively rechecks the binding in one transaction; once upgraded, later
/// comparisons are exact. Every read/write error is fatal. A retry after a partial cross-shard
/// phase-2 failure accepts the already-full shards and upgrades the remaining legacy shards.
/// Finally, the active key's in-process seal counter is primed from the preflight snapshots.
async fn initialize_key_state(
    store: &[Arc<SqliteMetadataStore>],
    crypto: &SystemCrypto,
    cfg: &Config,
) -> Result<(), String> {
    let ring = ring_for_state(cfg)?;
    let env_ids: std::collections::HashSet<u16> = ring.iter().map(|(id, _, _)| *id).collect();
    let active = crypto.active_key_id();
    let mut snapshots = Vec::with_capacity(store.len());
    for (i, s) in store.iter().enumerate() {
        snapshots.push(validate_key_gate_reads(
            i,
            &ring,
            &env_ids,
            active,
            s.key_ring_states().await,
            s.rewrap_done_active_ids().await,
        )?);
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    for (i, s) in store.iter().enumerate() {
        s.key_ring_apply_config(ring.clone(), now)
            .await
            .map_err(|e| format!("master-key gate: bind configured ring on shard {i}: {e}"))?;
    }

    let base = snapshots
        .iter()
        .flatten()
        .filter(|row| row.id == active)
        .map(|row| row.sealed_count)
        .max()
        .unwrap_or(0);
    crypto.prime_seal_count(base);
    if !store.is_empty() {
        tracing::info!(
            active_key_id = active,
            primed_seal_count = base,
            ring_keys = ring.len(),
            "master-key identity and retirement gate passed"
        );
    }
    Ok(())
}

/// The on-disk path for shard `i`: shard 0 is `base` itself (so existing single-shard data is
/// shard 0 untouched), and shard `i>0` is a sibling `base.shard{i}`.
fn shard_db_path(base: &std::path::Path, i: usize) -> std::path::PathBuf {
    if i == 0 {
        base.to_path_buf()
    } else {
        let mut name = base.as_os_str().to_owned();
        name.push(format!(".shard{i}"));
        std::path::PathBuf::from(name)
    }
}

/// Open one sqlite shard at `db_path`, returning its trait-object store, reconcile oracle, and the
/// typed handle the WAL checkpointer drives.
fn open_sqlite_shard(db_path: &std::path::Path, opts: &OpenOptions) -> Result<OpenedShard, String> {
    let store = cairn_meta::open(db_path, opts)
        .map_err(|e| format!("open metadata store (sqlite) at {}: {e}", db_path.display()))?;
    let oracle: Box<dyn ReconcileOracle + Send + Sync> = Box::new(store.reconcile_oracle());
    let store = Arc::new(store);
    let meta: Arc<dyn MetadataStore> = store.clone();
    Ok((meta, oracle, store))
}

/// Open the metadata store for the configured backend (ARCH 12.7). Returns the trait-object store
/// (a [`cairn_meta::ShardedMetadataStore`] router when `meta_shards > 1`), the boxed reconcile
/// oracle, and — for the `sqlite` backend only — the typed `SqliteMetadataStore` handles the WAL
/// checkpointer drives (one per shard; empty for the self-WAL-managing libSQL/Turso engines).
async fn open_meta(cfg: &Config) -> Result<OpenedMeta, String> {
    // Throughput tuning from config (ARCH 28.2/30), applied identically to whichever backend is
    // selected. `cache_size` follows SQLite's convention: negative => KiB of page cache.
    let synchronous_full = cfg.meta_synchronous == "full";
    let group_commit_linger = (cfg.meta_group_commit_linger_micros > 0)
        .then(|| std::time::Duration::from_micros(cfg.meta_group_commit_linger_micros));
    let read_pool_size = cfg.meta_read_pool_size;
    let cache_size = -((cfg.meta_cache_bytes_per_conn / 1024) as i64);
    let mmap_bytes = cfg.meta_mmap_bytes as i64;

    match cfg.meta_backend.as_str() {
        "sqlite" => {
            // The default, byte-identical path: the rusqlite/bundled-C store. Migrations run
            // inside `open`. Typed handles are kept for the WAL checkpointer (one per shard).
            let opts = OpenOptions {
                synchronous_full,
                read_pool_size,
                group_commit_linger,
                busy_timeout_ms: 5000,
                mmap_bytes,
                cache_size,
            };
            if cfg.meta_shards <= 1 {
                let (meta, oracle, store) = open_sqlite_shard(&cfg.db_path, &opts)?;
                return Ok((meta, oracle, vec![store]));
            }
            // Sharded (Phase 3.2): open N shard databases, partition by bucket name through the
            // routing store, and route each storage path to its owning shard for reconcile.
            let mut metas: Vec<Arc<dyn MetadataStore>> = Vec::with_capacity(cfg.meta_shards);
            let mut oracles: Vec<Box<dyn ReconcileOracle + Send + Sync>> =
                Vec::with_capacity(cfg.meta_shards);
            let mut handles: Vec<Arc<SqliteMetadataStore>> = Vec::with_capacity(cfg.meta_shards);
            for i in 0..cfg.meta_shards {
                let (meta, oracle, store) =
                    open_sqlite_shard(&shard_db_path(&cfg.db_path, i), &opts)?;
                metas.push(meta);
                oracles.push(oracle);
                handles.push(store);
            }
            let meta: Arc<dyn MetadataStore> =
                Arc::new(cairn_meta::ShardedMetadataStore::new(metas));
            let oracle: Box<dyn ReconcileOracle + Send + Sync> =
                Box::new(cairn_meta::ShardedReconcileOracle::new(oracles));
            Ok((meta, oracle, handles))
        }
        #[cfg(feature = "meta-async")]
        "libsql" => {
            let opts = cairn_meta_async::OpenOptions {
                synchronous_full,
                read_pool_size,
                group_commit_linger,
                busy_timeout_ms: 5000,
                mmap_bytes,
                cache_size,
            };
            let store = cairn_meta_async::open_libsql(&cfg.db_path, &opts)
                .await
                .map_err(|e| format!("open metadata store (libsql): {e}"))?;
            let oracle = Box::new(store.reconcile_oracle());
            let meta: Arc<dyn MetadataStore> = Arc::new(store);
            Ok((meta, oracle, vec![]))
        }
        #[cfg(feature = "meta-async")]
        "turso" => {
            let opts = cairn_meta_async::OpenOptions {
                synchronous_full,
                read_pool_size,
                group_commit_linger,
                busy_timeout_ms: 5000,
                mmap_bytes,
                cache_size,
            };
            let store = cairn_meta_async::open_turso(&cfg.db_path, &opts)
                .await
                .map_err(|e| format!("turso backend unavailable: {e}"))?;
            let oracle = Box::new(store.reconcile_oracle());
            let meta: Arc<dyn MetadataStore> = Arc::new(store);
            Ok((meta, oracle, vec![]))
        }
        // The libSQL/Turso backends are compiled in only with the `meta-async` cargo feature, so the
        // default release binary links only the rusqlite engine (no dual-bundled-SQLite collision —
        // it builds cleanly on every linker, including the aarch64 cross path). This arm exists only
        // when the feature is OFF (otherwise the specific arms above match and this is unreachable).
        #[cfg(not(feature = "meta-async"))]
        backend @ ("libsql" | "turso") => Err(format!(
            "meta_backend {backend:?} requires a binary built with --features meta-async \
             (the default binary supports only sqlite)"
        )),
        // `Config::validate` already rejects any other value at load, so this is unreachable in
        // practice; it is kept as a defensive clear error rather than a panic.
        other => Err(format!(
            "unknown meta_backend {other:?} (expected sqlite|libsql|turso)"
        )),
    }
}

/// Open just the metadata store (and its reconcile oracle) for the configured backend, for the
/// node-local CLI commands (`bootstrap`, `integrity`). This honours `CAIRN_META_BACKEND` so an
/// operator who selects libSQL or Turso bootstraps and reconciles through that same engine, rather
/// than silently falling back to the rusqlite engine. Migrations run as part of opening.
///
/// # Errors
/// Returns a message if the store cannot be opened for the configured backend.
pub(crate) async fn open_meta_store(
    cfg: &Config,
) -> Result<
    (
        Arc<dyn MetadataStore>,
        Box<dyn ReconcileOracle + Send + Sync>,
    ),
    String,
> {
    let (meta, oracle, _store) = open_meta(cfg).await?;
    Ok((meta, oracle))
}

/// Open the stores, wire the stack, and run startup reconciliation.
///
/// # Errors
/// Returns a message if any store cannot be opened or the master key is invalid.
pub async fn build(cfg: &Config) -> Result<AppStack, String> {
    if cfg.replication_allow_plaintext_sse_over_http {
        // Loud, operator-visible warning: replication ships the DECRYPTED body, so this permits
        // data the client asked us to encrypt to cross an unauthenticated, unencrypted link.
        tracing::warn!(
            "CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP is set: objects the client encrypted \
             with SSE-S3/SSE-KMS will be replicated as DECRYPTED bodies over plaintext http:// \
             endpoints"
        );
    }
    if cfg.allow_internal_endpoints {
        // Loud, operator-visible warning: the SSRF guard is off, so a replication target, webhook,
        // import source, or update feed may reach loopback/private/cloud-metadata addresses.
        tracing::warn!(
            "CAIRN_ALLOW_INTERNAL_ENDPOINTS is set: outbound dialers (replication, webhook, import, \
             update check) may connect to internal/loopback/link-local addresses — the SSRF guard \
             is DISABLED"
        );
    }
    tokio::fs::create_dir_all(&cfg.data_dir)
        .await
        .map_err(|e| format!("create data_dir: {e}"))?;

    // Open the configured metadata backend. `inner_meta` is the raw trait-object store; `oracle`
    // is the boxed reconcile oracle; `store` is the typed sqlite handle for the WAL checkpointer
    // (None for the self-WAL-managing libSQL/Turso engines).
    let (inner_meta, oracle, store) = open_meta(cfg).await?;

    // Front the store with the read-through config cache (ARCH 11.5) before handing it to the S3
    // and control services, so the hot authorization config reads (policy/ACL/CORS/public-access)
    // are memoised instead of re-reading SQLite per request. `meta_cache_bytes == 0` yields a pure
    // pass-through. The typed `meta_cache` handle is kept so the metrics loop can scrape `stats()`.
    let meta_cache = Arc::new(CachedMetadataStore::new(inner_meta, cfg.meta_cache_bytes));
    let meta: Arc<dyn MetadataStore> = meta_cache.clone();

    let blob_impl = LocalBlobStore::open(cfg.data_dir.clone())
        .await
        .map_err(|e| format!("open blob store: {e}"))?
        .with_io_pool_size(cfg.blob_io_pool_size)
        .with_read_io_pool_size(cfg.blob_io_read_pool_size);

    // Fail fast if the data root and staging are on different filesystems: the commit protocol's
    // atomic rename would fail with EXDEV on every write (ARCH 2.4, 9.2, GAP medium #10).
    #[cfg(unix)]
    blob_impl
        .check_single_filesystem()
        .map_err(|e| format!("single-filesystem check failed: {e}"))?;

    let blob_local = Arc::new(blob_impl);
    let blob: Arc<dyn BlobStore> = blob_local.clone();

    // Keep the concrete `SystemCrypto` (the replication target unsealing needs the concrete type,
    // `seal_target`/`open_target` take `&SystemCrypto`) as well as the `dyn Crypto` view the rest
    // of the stack uses.
    let system_crypto = Arc::new(build_crypto(cfg)?);
    // The durable id→hash binding and retire gate are a precondition for using this crypto at all.
    // Run them immediately after construction, before root bootstrap, compatibility migrations, or
    // any other path can seal a secret. The two-phase helper reads every shard before writing one.
    initialize_key_state(&store, &system_crypto, cfg).await?;
    let crypto: Arc<dyn Crypto> = system_crypto.clone();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock::new());

    // The authentication cache (ARCH 30): credential + parsed-policy memoization keyed by
    // access-key-id / user-id, sharing the metadata cache's user-mutation epoch so a
    // create/update/deactivate/set-policy drops every cached entry immediately. The TTL is a
    // staleness backstop; `auth_cache_ttl_secs == 0` disables it.
    let auth_cache = Arc::new(cairn_auth::AuthCache::new(
        std::time::Duration::from_secs(cfg.auth_cache_ttl_secs),
        meta_cache.auth_epoch_handle(),
    ));
    // Build the chain once and keep both the concrete `Arc<AuthChain>` (for `authenticate_sts`) and
    // the `dyn Authenticator` view the rest of the stack uses.
    let auth_chain = Arc::new(AuthChain::new(
        meta.clone(),
        crypto.clone(),
        clock.clone(),
        auth_cache,
        cfg.dev_auth,
    ));
    let auth: Arc<dyn Authenticator> = auth_chain.clone();
    let authz: Arc<dyn AuthorizationEngine> = Arc::new(cairn_authz::PolicyEngine);
    let replication_notify = Arc::new(tokio::sync::Notify::new());
    let import_notify = Arc::new(tokio::sync::Notify::new());
    let multipart_claim_recovery = Arc::new(
        crate::multipart_claim_recovery::MultipartClaimRecoveryQueue::new(cfg.concurrency_limit),
    );
    let s3 = S3Service::new(
        meta.clone(),
        blob.clone(),
        authz,
        clock.clone(),
        crypto.clone(),
        cfg.region.clone(),
        cfg.max_object_size,
    )
    .with_encrypt_at_rest(cfg.encrypt_at_rest)
    .with_multipart_limits(cfg.multipart_limits())
    .with_key_provider(Arc::new(cairn_protocol::LocalRingProvider::new(
        crypto.clone(),
        cfg.parse_kms_key_ids(),
    )))
    .with_replication_wake({
        let n = replication_notify.clone();
        Arc::new(move || n.notify_one())
    })
    .with_multipart_claim_recovery(multipart_claim_recovery.callback())
    .with_object_write_recovery(multipart_claim_recovery.object_callback())
    .with_multipart_part_write_recovery(multipart_claim_recovery.part_callback())
    .with_storage_recovery_admission(multipart_claim_recovery.admission_callback());
    let update_status = Arc::new(std::sync::RwLock::new(
        cairn_control::UpdateStatus::default(),
    ));
    let control = cairn_control::ControlService::new(
        meta.clone(),
        blob.clone(),
        crypto.clone(),
        clock.clone(),
        cairn_control::SystemInfo {
            // The build-injected release/dev version (see `build.rs::emit_version`), so `GET /system`
            // and the console footer report the same string as `cairn --version`.
            version: crate::CAIRN_VERSION.to_owned(),
            s3_addr: cfg.listen_addr.to_string(),
            web_addr: cfg.web_addr.clone(),
            tls: cfg.tls_enabled(),
            data_dir: cfg.data_dir.clone(),
            started_at: std::time::Instant::now(),
            update_status: update_status.clone(),
        },
    )
    .with_replication_wake({
        let n = replication_notify.clone();
        Arc::new(move || n.notify_one())
    })
    .with_root_access_key(cfg.root_access_key.clone())
    .with_allow_internal_endpoints(cfg.allow_internal_endpoints)
    .with_import_timeouts(cfg.import_timeouts())
    .with_import_wake({
        let n = import_notify.clone();
        Arc::new(move || n.notify_one())
    });

    // Ensure the root administrator exists so the deployment is usable immediately: the same access
    // key + secret log into the web console, authenticate the management API, and sign S3 requests.
    ensure_root_admin(&meta, &crypto, &clock, cfg).await?;

    // Older releases stored notification HMAC keys as plaintext JSON. Seal those values through the
    // ordinary writer before any listener binds, on every metadata backend and even when periodic
    // key rewrap is disabled. Fail startup rather than knowingly serve with database-readable keys.
    let migrated_webhooks =
        crate::key_rewrap::migrate_legacy_webhook_secrets(&*meta, &*crypto).await?;
    if migrated_webhooks > 0 {
        tracing::info!(
            buckets = migrated_webhooks,
            "sealed legacy webhook signing secrets during startup"
        );
    }

    // Completion ownership is process-local: no request survives a restart. Restore every
    // transient `completing` claim before reconciliation or listener bind so the durable session
    // and its parts remain retryable instead of being stranded forever. This global mutation fans
    // out across all metadata shards and is idempotent.
    recover_orphaned_multipart_claims(&*meta).await?;

    // Startup reconciliation reclaims orphaned blobs from any crash window before serving. The
    // oracle is taken by `&dyn ReconcileOracle`, so the boxed oracle is borrowed via `as_ref`.
    // No request is in flight yet (the listener is not bound), so a crash-orphan is unambiguous —
    // reclaim it immediately (margin 0); the safety margin only matters for a reconcile that races
    // live PUTs, which startup never does.
    let report = require_startup_reconciliation(
        blob.reconcile(
            oracle.as_ref(),
            ReconcileOpts {
                staging_safety_margin_secs: 0,
                ..ReconcileOpts::default()
            },
        )
        .await,
    )?;
    tracing::info!(
        orphans = report.orphans_reclaimed,
        scanned = report.blobs_scanned,
        "startup reconciliation complete"
    );
    recover_multipart_staging_accounting(&*meta).await?;

    // Replication crash-recovery: release any `claimed` outbox entries left leased by a worker that
    // crashed mid-ship. A freshly-started process has no live workers, so every claimed row is an
    // orphan — reclaim them to `pending` now so they re-ship immediately, instead of waiting out the
    // 300s claim lease. Runs before the listener binds and the workers start, so it never races a
    // live claim. Failure is fatal: a sharded fan-out can recover early shards before a later shard
    // errors, and only a startup retry can safely complete that idempotent partial recovery.
    recover_orphaned_replication_claims(&*meta).await?;

    Ok(AppStack {
        s3,
        control,
        update_status,
        auth,
        auth_chain,
        sts_enabled: cfg.sts_enabled,
        meta,
        meta_cache,
        crypto: system_crypto,
        replication_notify,
        import_notify,
        multipart_claim_recovery,
        sse_tickets: crate::sse::SseTicketStore::default(),
        blob,
        blob_local,
        oracle,
        store,
        s3_domain: cfg.s3_domain.clone(),
        region: cfg.region.clone(),
        allow_internal_endpoints: cfg.allow_internal_endpoints,
        replication_allow_plaintext_sse_over_http: cfg.replication_allow_plaintext_sse_over_http,
        public_base_url: cfg.public_base_url.clone(),
        data_listen_addr: cfg.listen_addr,
        request_metrics: Arc::new(crate::metrics_agg::RequestMetricsAgg::new(
            cfg.request_metrics_bucket_secs,
        )),
    })
}

async fn recover_orphaned_multipart_claims(meta: &dyn MetadataStore) -> Result<(), String> {
    meta.submit(cairn_types::meta::Mutation::RecoverMultipartClaims)
        .await
        .map_err(|error| format!("recover orphaned multipart completion claims: {error}"))?;
    tracing::info!("orphaned multipart completion claims recovered");
    Ok(())
}

async fn recover_multipart_staging_accounting(meta: &dyn MetadataStore) -> Result<(), String> {
    let mut released = 0u64;
    loop {
        match meta
            .submit(cairn_types::meta::Mutation::RecoverMultipartStagingAccounting { limit: 1_000 })
            .await
            .map_err(|error| format!("recover multipart staging accounting: {error}"))?
        {
            cairn_types::meta::MutationOutcome::MultipartAccountingReleased(0) => break,
            cairn_types::meta::MutationOutcome::MultipartAccountingReleased(count) => {
                released += count;
            }
            outcome => {
                return Err(format!(
                    "recover multipart staging accounting returned unexpected outcome: {outcome:?}"
                ));
            }
        }
    }
    if released > 0 {
        tracing::info!(
            released,
            "released crash-orphaned multipart staging accounting"
        );
    }
    Ok(())
}

async fn recover_orphaned_replication_claims(meta: &dyn MetadataStore) -> Result<(), String> {
    meta.submit(cairn_types::meta::Mutation::RecoverClaimedReplication)
        .await
        .map_err(|error| format!("recover orphaned replication claims: {error}"))?;
    tracing::info!("orphaned replication claims recovered");
    Ok(())
}

/// Convert the pre-bind reconciliation result into the stack-construction contract.
///
/// Serving with unresolved metadata/blob divergence violates ARCH 8, so every reconciliation
/// failure is fatal. Keeping this mapping in a small pure seam makes the startup behavior directly
/// testable without weakening the filesystem-only blob boundary with a production fault injector.
fn require_startup_reconciliation(
    result: Result<cairn_types::blob::ReconcileReport, cairn_types::error::BlobError>,
) -> Result<cairn_types::blob::ReconcileReport, String> {
    let report = result.map_err(|error| format!("startup reconciliation failed: {error}"))?;
    if report.errors > 0 {
        return Err(format!(
            "startup reconciliation failed: {} filesystem reclamation operation(s) failed",
            report.errors
        ));
    }
    Ok(report)
}

#[cfg(test)]
mod multipart_recovery_tests {
    use super::recover_orphaned_multipart_claims;
    use cairn_types::id::{BucketName, MultipartClaimToken, ObjectKey, UploadId, UserId};
    use cairn_types::meta::{
        ClaimOutcome, MultipartSession, MultipartStatus, Mutation, MutationOutcome,
    };
    use cairn_types::testing::InMemoryMetadataStore;
    use cairn_types::time::Timestamp;
    use cairn_types::traits::MetadataStore;

    #[tokio::test]
    async fn pre_bind_recovery_restores_an_orphaned_completion_claim() {
        let store = InMemoryMetadataStore::new();
        let upload_id = UploadId::from_string("restart-recovery".to_owned());
        store
            .submit(Mutation::CreateMultipart {
                session: Box::new(MultipartSession {
                    upload_id: upload_id.clone(),
                    bucket: BucketName::parse("recovery-bucket").unwrap(),
                    key: ObjectKey::parse("large-object").unwrap(),
                    content_type: "application/octet-stream".to_owned(),
                    status: MultipartStatus::Active,
                    owner_id: UserId("owner".to_owned()),
                    initiated_by: UserId("owner".to_owned()),
                    intended_acl: None,
                    user_metadata: Vec::new(),
                    initial_tags: Vec::new(),
                    lock_intent: cairn_types::ExplicitObjectLockIntent::default(),
                    sse_requested: false,
                    encrypt_parts: false,
                    sse_kms_requested: false,
                    sse_kms_key_id: None,
                    sse_bucket_key_enabled: false,
                    created_at: Timestamp(1),
                    updated_at: Timestamp(1),
                }),
                limits: cairn_types::meta::MultipartLimits::default(),
            })
            .await
            .unwrap();
        assert!(matches!(
            store
                .submit(Mutation::ClaimMultipart {
                    upload_id: upload_id.clone(),
                    claim_token: MultipartClaimToken::generate(),
                })
                .await
                .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));

        recover_orphaned_multipart_claims(&store).await.unwrap();
        assert_eq!(
            store
                .get_multipart(&upload_id)
                .await
                .unwrap()
                .expect("session survives recovery")
                .status,
            MultipartStatus::Active
        );
        assert!(matches!(
            store
                .submit(Mutation::ClaimMultipart {
                    upload_id,
                    claim_token: MultipartClaimToken::generate(),
                })
                .await
                .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));
    }
}

#[cfg(test)]
mod reconciliation_tests {
    use super::require_startup_reconciliation;
    use cairn_types::blob::ReconcileReport;
    use cairn_types::error::BlobError;

    #[test]
    fn failed_blob_walk_is_fatal_before_listener_bind() {
        let error =
            require_startup_reconciliation(Err(BlobError::Io("sentinel walk failure".to_owned())))
                .expect_err("startup must not continue after reconciliation failure");

        assert!(error.contains("startup reconciliation failed"));
        assert!(error.contains("sentinel walk failure"));
    }

    #[test]
    fn successful_reconciliation_report_is_preserved() {
        let report = ReconcileReport {
            blobs_scanned: 7,
            orphans_reclaimed: 2,
            ..ReconcileReport::default()
        };

        assert_eq!(
            require_startup_reconciliation(Ok(report.clone())).unwrap(),
            report
        );
    }

    #[test]
    fn partial_reclamation_failure_is_fatal_before_accounting_recovery() {
        let report = ReconcileReport {
            blobs_scanned: 7,
            orphans_reclaimed: 2,
            errors: 1,
            ..ReconcileReport::default()
        };

        let error = require_startup_reconciliation(Ok(report))
            .expect_err("accounting must remain charged when a filesystem unlink failed");
        assert!(error.contains("startup reconciliation failed"));
        assert!(error.contains("1 filesystem reclamation operation"));
    }
}

/// Ensure an active administrator with the configured root access key exists, so the server is
/// usable out of the box. The same `CAIRN_ROOT_ACCESS_KEY` / `CAIRN_ROOT_SECRET_KEY` pair is valid
/// for the web console login, the management API (as a Bearer token `access.secret`), and the S3 API
/// (SigV4 — the access key is registered as the SigV4 key id too). Idempotent: created when absent,
/// secret/role refreshed when the env changed, left untouched when already in sync.
pub(crate) async fn ensure_root_admin(
    meta: &Arc<dyn MetadataStore>,
    crypto: &Arc<dyn Crypto>,
    clock: &Arc<dyn Clock>,
    cfg: &Config,
) -> Result<(), String> {
    use cairn_types::auth::Role;
    use cairn_types::id::UserId;
    use cairn_types::meta::{Mutation, User, UserRecord};

    let akid = cfg.root_access_key.clone();
    let want_hash = cairn_auth::hash_bearer_secret(&cfg.root_secret_key);

    let existing = meta
        .user_by_bearer_key(&akid)
        .await
        .map_err(|e| format!("root admin lookup: {e}"))?;

    // Already present, active, admin, and the secret matches the env — nothing to do.
    if let Some(ub) = &existing {
        if ub.user.is_active && ub.user.role == Role::Administrator && ub.secret_hash == want_hash {
            return Ok(());
        }
    }

    let now = clock.now();
    let sealed = crypto
        .seal(cfg.root_secret_key.as_bytes())
        .map_err(|e| format!("seal root secret: {e}"))?;
    let id = existing
        .as_ref()
        .map(|u| u.user.id.clone())
        .unwrap_or_else(UserId::generate);
    // Preserve the original creation time when re-affirming an existing root (e.g. a secret/role
    // refresh on restart); only a brand-new root is stamped `now`. `created_at` means "when created",
    // not "when last touched" — `updated_at` carries that.
    let created_at = existing.as_ref().map_or(now, |u| u.user.created_at);
    let record = UserRecord {
        user: User {
            id,
            display_name: "root".to_owned(),
            access_key_id: akid.clone(),
            sigv4_access_key_id: Some(akid.clone()),
            role: Role::Administrator,
            is_active: true,
            quota_bytes: None,
            created_at,
            updated_at: now,
        },
        bearer_secret_hash: want_hash,
        // CRK1 envelope (audit #29): the nonce is inside the ciphertext; store NULL nonce.
        sigv4_secret_ciphertext: Some(sealed.ciphertext),
        sigv4_secret_nonce: None,
    };
    let mutation = if existing.is_some() {
        Mutation::UpdateUser(Box::new(record))
    } else {
        Mutation::CreateUser(Box::new(record))
    };
    meta.submit(mutation)
        .await
        .map_err(|e| format!("seed root admin: {e}"))?;

    if cfg.root_access_key == "cairn" && cfg.root_secret_key == "cairnadmin" {
        tracing::warn!(
            access_key = %akid,
            "using DEFAULT root admin credentials (cairn / cairnadmin) — set CAIRN_ROOT_ACCESS_KEY \
             and CAIRN_ROOT_SECRET_KEY to secure this deployment"
        );
    } else {
        tracing::info!(access_key = %akid, "root administrator ensured");
    }
    Ok(())
}

#[cfg(test)]
mod retire_gate_tests {
    use super::{retire_gate_unsafe_ids, ring_for_state, validate_key_gate_reads};
    use crate::config::Config;
    use cairn_meta::KeyRingStateRow;
    use cairn_types::MetaError;
    use std::collections::HashSet;

    fn ids(xs: &[u16]) -> HashSet<u16> {
        xs.iter().copied().collect()
    }

    #[test]
    fn flags_only_unswept_removed_keys() {
        // Removed id=1 with NO re-wrap (min_done=0) under active 2 -> unsafe (the P4 brick case).
        assert_eq!(retire_gate_unsafe_ids(&[1, 2], &ids(&[2]), 2, 0), vec![1]);
        // Removed id=1 but re-wrap completed to id=2 -> safe to retire (the legitimate P3 flow).
        assert!(retire_gate_unsafe_ids(&[1, 2], &ids(&[2]), 2, 2).is_empty());
        // Multi-rotation: id=1 long-removed and swept to 2; now active=3 mid-pass -> still safe
        // (no false refusal just because the new rotation has not finished).
        assert!(retire_gate_unsafe_ids(&[1, 2, 3], &ids(&[2, 3]), 3, 2).is_empty());
        // Dangerous: active=3 but data only swept to 2, and id=2 (which still holds data) removed.
        assert_eq!(
            retire_gate_unsafe_ids(&[1, 2, 3], &ids(&[1, 3]), 3, 2),
            vec![2]
        );
        // A removed id newer than the active id (unusual/pinned active) is not flagged.
        assert!(retire_gate_unsafe_ids(&[1, 2, 3], &ids(&[1, 2]), 2, 2).is_empty());
        // No keys removed (every recorded id still in the ring) -> always safe.
        assert!(retire_gate_unsafe_ids(&[1, 2], &ids(&[1, 2]), 2, 0).is_empty());
    }

    fn state(id: u16, hash: &str) -> KeyRingStateRow {
        KeyRingStateRow {
            id,
            key_hash: hash.to_owned(),
            is_active: true,
            sealed_count: 0,
            created_at: 1,
        }
    }

    #[test]
    fn configured_key_identity_uses_the_full_sha256() {
        let ring = ring_for_state(&Config::default()).expect("development key is valid");
        assert_eq!(ring.len(), 1);
        assert_eq!(
            ring[0].1, "66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d0d5f2925",
            "the durable identity must retain all 256 SHA-256 bits"
        );
    }

    #[test]
    fn same_id_replacement_is_rejected_before_binding() {
        let configured = "11".repeat(32);
        let durable = "22".repeat(32);
        let err = validate_key_gate_reads(
            2,
            &[(7, configured, true)],
            &ids(&[7]),
            7,
            Ok(vec![state(7, &durable)]),
            Ok(Vec::new()),
        )
        .expect_err("same-id replacement must fail closed");
        assert!(err.contains("key id 7"));
        assert!(err.contains("same-id replacement"));
    }

    #[test]
    fn every_gate_read_error_is_fatal() {
        let full_hash = "11".repeat(32);
        let ring = [(7, full_hash.clone(), true)];
        let state_err = validate_key_gate_reads(
            0,
            &ring,
            &ids(&[7]),
            7,
            Err(MetaError::Engine("injected state read failure".to_owned())),
            Ok(Vec::new()),
        )
        .expect_err("key-state read failure must refuse startup");
        assert!(state_err.contains("read key_ring_state"));

        let progress_err = validate_key_gate_reads(
            1,
            &ring,
            &ids(&[7]),
            7,
            Ok(vec![state(7, &full_hash)]),
            Err(MetaError::Engine(
                "injected progress read failure".to_owned(),
            )),
        )
        .expect_err("re-wrap progress read failure must refuse startup");
        assert!(progress_err.contains("read rewrap_progress"));
    }

    #[test]
    fn partial_shard_legacy_upgrade_is_safe_to_retry() {
        let full_hash = format!("deadbeef{}", "11".repeat(28));
        let ring = [(7, full_hash.clone(), true)];
        for (shard, stored) in [(0, full_hash.as_str()), (1, "deadbeef")] {
            validate_key_gate_reads(
                shard,
                &ring,
                &ids(&[7]),
                7,
                Ok(vec![state(7, stored)]),
                Ok(Vec::new()),
            )
            .expect("both an already-upgraded shard and its matching legacy peer must preflight");
        }
    }
}

#[cfg(test)]
mod sharding_tests {
    use super::*;
    use cairn_types::authz::OwnershipMode;
    use cairn_types::bucket::{Bucket, VersioningState};
    use cairn_types::meta::{OutboxEntry, ReplicationOp, ReplicationStatus};
    use cairn_types::{BucketName, Mutation, ObjectKey, Timestamp, UserId, VersionId};

    fn bucket(name: &str) -> Mutation {
        Mutation::CreateBucket(Box::new(Bucket {
            name: BucketName::parse(name).unwrap(),
            owner_id: UserId("o".to_owned()),
            created_at: Timestamp(1),
            versioning: VersioningState::Enabled,
            ownership_mode: OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".to_owned(),
            compression: None,
        }))
    }

    #[tokio::test]
    async fn open_meta_shards_partition_buckets_across_db_files() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            data_dir: dir.path().to_path_buf(),
            db_path: dir.path().join("meta.db"),
            meta_backend: "sqlite".to_owned(),
            meta_shards: 3,
            ..Config::default()
        };
        assert!(cfg.validate().is_ok());

        let (meta, _oracle, handles) = open_meta(&cfg).await.unwrap();
        assert_eq!(handles.len(), 3, "one WAL-checkpointer handle per shard");

        for name in ["alpha", "bravo", "charlie", "delta", "echo"] {
            meta.submit(bucket(name)).await.unwrap();
        }

        // The router sees every bucket; each shard holds only the buckets that hash to it, with no
        // loss or duplication across the partition.
        assert_eq!(meta.list_buckets(None).await.unwrap().len(), 5);
        let mut total = 0;
        for (i, h) in handles.iter().enumerate() {
            let on_shard = h.list_buckets(None).await.unwrap();
            for b in &on_shard {
                assert_eq!(
                    cairn_meta::shard_for_bucket(b.name.as_str(), 3),
                    i,
                    "bucket {} must live on its hashed shard",
                    b.name.as_str()
                );
            }
            total += on_shard.len();
        }
        assert_eq!(total, 5, "buckets partitioned with no loss or duplication");

        // The sibling shard database files exist on disk.
        assert!(dir.path().join("meta.db").exists());
        assert!(dir.path().join("meta.db.shard1").exists());
        assert!(dir.path().join("meta.db.shard2").exists());
    }

    #[tokio::test]
    async fn startup_replication_recovery_releases_claims_on_every_physical_shard() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            data_dir: dir.path().to_path_buf(),
            db_path: dir.path().join("meta.db"),
            meta_backend: "sqlite".to_owned(),
            meta_shards: 3,
            ..Config::default()
        };
        let (meta, _oracle, handles) = open_meta(&cfg).await.unwrap();

        // Pick one valid bucket name for each physical shard, then enqueue through the router so
        // this fixture exercises exactly the same ownership classification as production.
        let mut bucket_for_shard: Vec<Option<BucketName>> = vec![None; handles.len()];
        for index in 0..1_000 {
            let name = format!("recovery-bucket-{index}");
            let shard = cairn_meta::shard_for_bucket(&name, handles.len());
            if bucket_for_shard[shard].is_none() {
                bucket_for_shard[shard] = Some(BucketName::parse(&name).unwrap());
            }
            if bucket_for_shard.iter().all(Option::is_some) {
                break;
            }
        }
        let bucket_for_shard: Vec<BucketName> = bucket_for_shard
            .into_iter()
            .map(|bucket| bucket.expect("candidate search covers every shard"))
            .collect();

        for (shard, bucket_name) in bucket_for_shard.iter().enumerate() {
            meta.submit(bucket(bucket_name.as_str())).await.unwrap();
            meta.submit(Mutation::EnqueueReplication(Box::new(OutboxEntry {
                id: format!("orphaned-claim-{shard}"),
                bucket: bucket_name.clone(),
                key: ObjectKey::parse("object").unwrap(),
                version_id: VersionId::from_string(format!("version-{shard}")),
                operation: ReplicationOp::ObjectCreate,
                rule_id: "rule".to_owned(),
                target_arn: None,
                attempts: 0,
                next_attempt_at: Timestamp(0),
                status: ReplicationStatus::Pending,
                last_error: None,
                priority: 0,
                lease_until: None,
                enqueued_at: Timestamp(0),
            })))
            .await
            .unwrap();
        }

        let claimed = meta
            .claim_replication_batch(handles.len() as u32, Timestamp(1_000))
            .await
            .unwrap();
        assert_eq!(claimed.len(), handles.len());
        for handle in &handles {
            let counts = handle.replication_counts(None).await.unwrap();
            assert_eq!(counts.claimed, 1);
            assert_eq!(counts.pending, 0);
        }

        // This is the exact pre-bind helper. Its router mutation must broadcast, not route to only
        // shard zero, because cancellation can strand leases on any bucket shard.
        recover_orphaned_replication_claims(meta.as_ref())
            .await
            .unwrap();
        for (shard, handle) in handles.iter().enumerate() {
            let counts = handle.replication_counts(None).await.unwrap();
            assert_eq!(counts.claimed, 0, "shard {shard} retained a stale lease");
            assert_eq!(counts.pending, 1, "shard {shard} was not recovered");
        }
    }
}
