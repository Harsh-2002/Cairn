//! Builds and owns the concrete engine stack — the only place that names concrete
//! implementations (ARCH 12.7). It opens the metadata store and blob store, wires the
//! authenticator chain and the S3 service, and runs startup reconciliation before serving.

use crate::config::Config;
use cairn_auth::AuthChain;
use cairn_blob::LocalBlobStore;
use cairn_crypto::{SystemClock, SystemCrypto};
use cairn_meta::{CachedMetadataStore, OpenOptions, SqliteMetadataStore};
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
    /// The authenticator chain.
    pub auth: Arc<dyn Authenticator>,
    /// The metadata store behind its trait object, used by request handlers, the readiness
    /// probe, and the background subsystems (multipart sweeper, lifecycle scanner). Backend-
    /// agnostic: it is the sqlite, libSQL, or Turso store depending on `CAIRN_META_BACKEND`.
    pub meta: Arc<dyn MetadataStore>,
    /// The blob store. Held for the background subsystems (sweeper, periodic reconcile).
    #[allow(dead_code)]
    pub blob: Arc<dyn BlobStore>,
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
    /// The base domain for virtual-host-style S3 addressing (`CAIRN_S3_DOMAIN`, ARCH 13.1), e.g.
    /// `s3.example.com`. When set, a request whose `Host` is `<bucket>.<s3_domain>` routes to that
    /// bucket with the whole path as the key; `None` leaves path-style addressing as the only form.
    pub s3_domain: Option<String>,
    /// The SigV4 signing region (`CAIRN_REGION`), used when minting presigned URLs so the
    /// credential scope matches what the verifier derives.
    pub region: String,
    /// The public base URL (`CAIRN_PUBLIC_BASE_URL`) shares/presigned links are built against; when
    /// `None`, the minting request's own scheme + Host is used.
    pub public_base_url: Option<String>,
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
        Ok(SystemCrypto::new([0u8; 32]))
    }
}

/// The `(id, key_hash, is_active)` rows to record in `key_ring_state` for operator display
/// (audit #29). The hash is the first 8 hex of SHA-256(key) — never key material. Re-derived from
/// config (the key bytes were scrubbed into ciphers by `build_crypto`).
fn ring_for_state(cfg: &Config) -> Vec<(u16, String, bool)> {
    use sha2::{Digest, Sha256};
    let hash = |bytes: &[u8]| hex::encode(&Sha256::digest(bytes)[..4]);
    if let Some(ring_json) = &cfg.master_key_ring {
        if let Ok(keys) = crate::config::parse_key_ring(ring_json) {
            let max_id = keys.iter().map(|(id, _)| *id).max().unwrap_or(1);
            let active = cfg.master_key_active_id.unwrap_or(max_id);
            return keys
                .iter()
                .map(|(id, k)| (*id, hash(k), *id == active))
                .collect();
        }
    }
    if let Some(mk) = &cfg.master_key {
        if let Ok(bytes) = hex::decode(mk) {
            return vec![(1, hash(&bytes), true)];
        }
    }
    vec![(1, hash(&[0u8; 32]), true)] // development key
}

/// Seed each sqlite shard's `key_ring_state` for the env ring keys and prime the in-process
/// active-key seal counter from the max durable count across shards (audit #29, Phase E).
async fn prime_key_state(store: &[Arc<SqliteMetadataStore>], crypto: &SystemCrypto, cfg: &Config) {
    let ring = ring_for_state(cfg);
    let active = crypto.active_key_id();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    let mut base = 0u64;
    for s in store {
        for (id, key_hash, is_active) in &ring {
            if let Err(e) = s
                .key_ring_upsert(*id, key_hash.clone(), *is_active, now)
                .await
            {
                tracing::warn!(error = %e, "key_ring_state upsert failed");
            }
        }
        if let Ok(states) = s.key_ring_states().await {
            if let Some(row) = states.iter().find(|r| r.id == active) {
                base = base.max(row.sealed_count);
            }
        }
    }
    crypto.prime_seal_count(base);
    if !store.is_empty() {
        tracing::info!(
            active_key_id = active,
            primed_seal_count = base,
            ring_keys = ring.len(),
            "master-key rotation state primed (audit #29)"
        );
    }
}

/// The set of ring ids in the current env config (audit #29 retire-gate).
fn env_ring_ids(cfg: &Config) -> std::collections::HashSet<u16> {
    ring_for_state(cfg)
        .into_iter()
        .map(|(id, _, _)| id)
        .collect()
}

/// The re-wrap streams whose completion gates a key retirement (audit #29).
const REWRAP_STREAMS: [&str; 3] = [
    "object_versions.sse_descriptor",
    "users.sigv4_secret",
    "bucket_config.replication_targets",
];

/// Pure retire-gate decision for one shard (audit #29 / spec 5.4). Given the key ids this shard has
/// ever recorded, the current env ring ids, the active id, and the lowest `done_active_id` across
/// the re-wrap streams, return the removed ids whose data is NOT proven re-wrapped off them.
///
/// In the forward-rotation model (ids increase, `active` is the newest), a removed key K is unsafe
/// iff `K < active` (it is an older key the active one supersedes) AND the re-wrap has not swept past
/// it (`min_done <= K`). A removed key newer than `active`, or one fully swept (`min_done > K`), is
/// safe. An empty result means it is safe to start.
fn retire_gate_unsafe_ids(
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

/// Enforce the retire-gate across every sqlite shard before the listener binds (audit #29). Returns
/// an `Err` (failing startup) naming the offending key id(s) and shard when a removed key still has
/// un-re-wrapped data; a read error on a shard is logged and skipped (it never wedges startup), and
/// the async backends (no concrete shard handles) are not gated.
async fn enforce_retire_gate(
    store: &[Arc<SqliteMetadataStore>],
    crypto: &SystemCrypto,
    cfg: &Config,
) -> Result<(), String> {
    let env_ids = env_ring_ids(cfg);
    let active = crypto.active_key_id();
    for (i, s) in store.iter().enumerate() {
        let recorded: Vec<u16> = match s.key_ring_states().await {
            Ok(rows) => rows.into_iter().map(|r| r.id).collect(),
            Err(e) => {
                tracing::warn!(error = %e, shard = i, "retire-gate: could not read key_ring_state; skipping");
                continue;
            }
        };
        let dones: std::collections::HashMap<String, u16> = match s.rewrap_done_active_ids().await {
            Ok(v) => v.into_iter().collect(),
            Err(e) => {
                tracing::warn!(error = %e, shard = i, "retire-gate: could not read rewrap progress; skipping");
                continue;
            }
        };
        // The lowest completion across the gated streams (0 = a stream never finished a clean pass).
        let min_done = REWRAP_STREAMS
            .iter()
            .map(|st| dones.get(*st).copied().unwrap_or(0))
            .min()
            .unwrap_or(0);
        let unsafe_ids = retire_gate_unsafe_ids(&recorded, &env_ids, active, min_done);
        if !unsafe_ids.is_empty() {
            return Err(format!(
                "audit #29 retire-gate: shard {i} still holds data sealed under master key id(s) \
                 {unsafe_ids:?} that were removed from CAIRN_MASTER_KEY_RING before re-wrap onto the \
                 active key {active} completed (re-wrap reached id {min_done}). Restore those key \
                 id(s) to the ring and wait for GET /api/v1/system/crypto-status to report them \
                 retire_eligible before removing them; refusing to start to avoid unreadable data."
            ));
        }
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
        .with_io_pool_size(cfg.blob_io_pool_size);

    // Fail fast if the data root and staging are on different filesystems: the commit protocol's
    // atomic rename would fail with EXDEV on every write (ARCH 2.4, 9.2, GAP medium #10).
    #[cfg(unix)]
    blob_impl
        .check_single_filesystem()
        .map_err(|e| format!("single-filesystem check failed: {e}"))?;

    let blob: Arc<dyn BlobStore> = Arc::new(blob_impl);

    // Keep the concrete `SystemCrypto` (the replication target unsealing needs the concrete type,
    // `seal_target`/`open_target` take `&SystemCrypto`) as well as the `dyn Crypto` view the rest
    // of the stack uses.
    let system_crypto = Arc::new(build_crypto(cfg)?);
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
    let auth: Arc<dyn Authenticator> = Arc::new(AuthChain::new(
        meta.clone(),
        crypto.clone(),
        clock.clone(),
        auth_cache,
        cfg.dev_auth,
    ));
    let authz: Arc<dyn AuthorizationEngine> = Arc::new(cairn_authz::PolicyEngine);
    let s3 = S3Service::new(
        meta.clone(),
        blob.clone(),
        authz,
        clock.clone(),
        crypto.clone(),
        cfg.region.clone(),
        cfg.max_object_size,
    );
    let control = cairn_control::ControlService::new(
        meta.clone(),
        blob.clone(),
        crypto.clone(),
        clock.clone(),
        cairn_control::SystemInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            s3_addr: cfg.listen_addr.to_string(),
            ui_addr: cfg.ui_addr.clone(),
            tls: cfg.tls_enabled(),
            data_dir: cfg.data_dir.clone(),
            started_at: std::time::Instant::now(),
        },
    );

    // Ensure the root administrator exists so the deployment is usable immediately: the same access
    // key + secret log into the web UI, authenticate the management API, and sign S3 requests.
    ensure_root_admin(&meta, &crypto, &clock, cfg).await?;

    // Startup reconciliation reclaims orphaned blobs from any crash window before serving. The
    // oracle is taken by `&dyn ReconcileOracle`, so the boxed oracle is borrowed via `as_ref`.
    // No request is in flight yet (the listener is not bound), so a crash-orphan is unambiguous —
    // reclaim it immediately (margin 0); the safety margin only matters for a reconcile that races
    // live PUTs, which startup never does.
    match blob
        .reconcile(
            oracle.as_ref(),
            ReconcileOpts {
                staging_safety_margin_secs: 0,
                ..ReconcileOpts::default()
            },
        )
        .await
    {
        Ok(report) => tracing::info!(
            orphans = report.orphans_reclaimed,
            scanned = report.blobs_scanned,
            "startup reconciliation complete"
        ),
        Err(e) => tracing::warn!(error = %e, "startup reconciliation failed"),
    }

    // Master-key rotation state (audit #29, Phase E): seed each sqlite shard's key_ring_state for
    // the env ring keys and prime the in-process seal counter from durable state so the seal-count
    // bound survives a restart. No-op for the async backends (no concrete shard handles).
    prime_key_state(&store, &system_crypto, cfg).await;

    // Retire-gate (audit #29 / spec 5.4): refuse to start if a master key was removed from the ring
    // before its data was re-wrapped onto the active key — otherwise that data (object DEKs, SigV4
    // secrets) is unreadable and the failure surfaces only as a confusing flood of per-request
    // errors. Fail fast with a diagnostic naming the key id(s) and shard instead.
    enforce_retire_gate(&store, &system_crypto, cfg).await?;

    Ok(AppStack {
        s3,
        control,
        auth,
        meta,
        meta_cache,
        crypto: system_crypto,
        blob,
        oracle,
        store,
        s3_domain: cfg.s3_domain.clone(),
        region: cfg.region.clone(),
        public_base_url: cfg.public_base_url.clone(),
        request_metrics: Arc::new(crate::metrics_agg::RequestMetricsAgg::new(
            cfg.request_metrics_bucket_secs,
        )),
    })
}

/// Ensure an active administrator with the configured root access key exists, so the server is
/// usable out of the box. The same `CAIRN_ROOT_ACCESS_KEY` / `CAIRN_ROOT_SECRET_KEY` pair is valid
/// for the web UI login, the management API (as a Bearer token `access.secret`), and the S3 API
/// (SigV4 — the access key is registered as the SigV4 key id too). Idempotent: created when absent,
/// secret/role refreshed when the env changed, left untouched when already in sync.
async fn ensure_root_admin(
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
    let record = UserRecord {
        user: User {
            id,
            display_name: "root".to_owned(),
            access_key_id: akid.clone(),
            sigv4_access_key_id: Some(akid.clone()),
            role: Role::Administrator,
            is_active: true,
            quota_bytes: None,
            created_at: now,
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
    use super::retire_gate_unsafe_ids;
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
}

#[cfg(test)]
mod sharding_tests {
    use super::*;
    use cairn_types::authz::OwnershipMode;
    use cairn_types::bucket::{Bucket, VersioningState};
    use cairn_types::{BucketName, Mutation, Timestamp, UserId};

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
}
