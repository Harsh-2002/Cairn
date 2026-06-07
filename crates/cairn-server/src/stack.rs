//! Builds and owns the concrete engine stack — the only place that names concrete
//! implementations (ARCH §12.7). It opens the metadata store and blob store, wires the
//! authenticator chain and the S3 service, and runs startup reconciliation before serving.

use crate::config::Config;
use cairn_auth::AuthChain;
use cairn_blob::LocalBlobStore;
use cairn_crypto::{HmacPublicUrl, SystemClock, SystemCrypto};
use cairn_meta::{OpenOptions, SqliteMetadataStore};
use cairn_s3::S3Service;
use cairn_types::blob::ReconcileOpts;
use cairn_types::traits::{
    Authenticator, AuthorizationEngine, BlobStore, Clock, Crypto, MetadataStore, PublicUrl,
    ReconcileOracle,
};
use std::sync::Arc;

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
    /// A typed handle to the concrete SQLite store, **only present for the `sqlite` backend**. The
    /// WAL checkpointer's `checkpoint()` and `wal_size_bytes()` are inherent methods on
    /// `SqliteMetadataStore`, not part of the `MetadataStore` trait object, so the concrete store
    /// is threaded through here rather than reached via `meta` (ARCH §8.4/§11.2). The libSQL and
    /// Turso engines self-manage their WAL, so this is `None` for them and the WAL-checkpointer
    /// background loop does not run.
    pub store: Option<Arc<SqliteMetadataStore>>,
    /// Signer/verifier for Cairn's signed public-read ("share") URLs (ARCH §14.5). Keyed off the
    /// master key so links stay valid across restarts when `CAIRN_MASTER_KEY` is set.
    pub public_url: Arc<dyn PublicUrl>,
}

impl std::fmt::Debug for AppStack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppStack").finish_non_exhaustive()
    }
}

/// Build the cryptography facility from the configured master key (or a development key).
pub(crate) fn build_crypto(cfg: &Config) -> Result<SystemCrypto, String> {
    match &cfg.master_key {
        Some(hex) => SystemCrypto::from_hex(hex).map_err(|e| format!("invalid master_key: {e}")),
        None => {
            tracing::warn!(
                "no master_key configured; using a fixed DEVELOPMENT key (insecure). Set CAIRN_MASTER_KEY in production."
            );
            Ok(SystemCrypto::new([0u8; 32]))
        }
    }
}

/// Open the metadata store for the configured backend (ARCH §12.7). Returns the trait-object
/// store, the boxed reconcile oracle, and — for the `sqlite` backend only — the typed
/// `SqliteMetadataStore` handle the WAL checkpointer drives (the libSQL and Turso engines
/// self-manage their WAL, so they return `None`). The on-disk database is the same SQLite file
/// format for all three engines; the backend only changes which engine drives it.
async fn open_meta(
    cfg: &Config,
) -> Result<
    (
        Arc<dyn MetadataStore>,
        Box<dyn ReconcileOracle + Send + Sync>,
        Option<Arc<SqliteMetadataStore>>,
    ),
    String,
> {
    match cfg.meta_backend.as_str() {
        "sqlite" => {
            // The default, byte-identical path: the rusqlite/bundled-C store. Migrations run
            // inside `open`. A typed handle is kept for the WAL checkpointer.
            let store = cairn_meta::open(&cfg.db_path, &OpenOptions::default())
                .map_err(|e| format!("open metadata store (sqlite): {e}"))?;
            let oracle = Box::new(store.reconcile_oracle());
            let store = Arc::new(store);
            let meta: Arc<dyn MetadataStore> = store.clone();
            Ok((meta, oracle, Some(store)))
        }
        #[cfg(feature = "meta-async")]
        "libsql" => {
            let store = cairn_meta_async::open_libsql(&cfg.db_path, &Default::default())
                .await
                .map_err(|e| format!("open metadata store (libsql): {e}"))?;
            let oracle = Box::new(store.reconcile_oracle());
            let meta: Arc<dyn MetadataStore> = Arc::new(store);
            Ok((meta, oracle, None))
        }
        #[cfg(feature = "meta-async")]
        "turso" => {
            let store = cairn_meta_async::open_turso(&cfg.db_path, &Default::default())
                .await
                .map_err(|e| format!("turso backend unavailable: {e}"))?;
            let oracle = Box::new(store.reconcile_oracle());
            let meta: Arc<dyn MetadataStore> = Arc::new(store);
            Ok((meta, oracle, None))
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

    // Open the configured metadata backend. `meta` is the trait-object store used everywhere;
    // `oracle` is the boxed reconcile oracle; `store` is the typed sqlite handle for the WAL
    // checkpointer (None for the self-WAL-managing libSQL/Turso engines).
    let (meta, oracle, store) = open_meta(cfg).await?;

    let blob_impl = LocalBlobStore::open(cfg.data_dir.clone())
        .await
        .map_err(|e| format!("open blob store: {e}"))?;

    // Fail fast if the data root and staging are on different filesystems: the commit protocol's
    // atomic rename would fail with EXDEV on every write (ARCH §2.4, §9.2, GAP medium #10).
    #[cfg(unix)]
    blob_impl
        .check_single_filesystem()
        .map_err(|e| format!("single-filesystem check failed: {e}"))?;

    let blob: Arc<dyn BlobStore> = Arc::new(blob_impl);

    let crypto: Arc<dyn Crypto> = Arc::new(build_crypto(cfg)?);
    let clock: Arc<dyn Clock> = Arc::new(SystemClock::new());

    let auth: Arc<dyn Authenticator> = Arc::new(AuthChain::new(
        meta.clone(),
        crypto.clone(),
        clock.clone(),
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
    );

    // Ensure the root administrator exists so the deployment is usable immediately: the same access
    // key + secret log into the web UI, authenticate the management API, and sign S3 requests.
    ensure_root_admin(&meta, &crypto, &clock, cfg).await?;

    // The public-read ("share") URL signer is keyed off the master key (its hex string as the HMAC
    // secret) so signed links survive restarts when CAIRN_MASTER_KEY is set; without it, a fixed
    // dev key is used (links are valid only within a process lifetime, like the dev master key).
    let pu_secret = cfg
        .master_key
        .clone()
        .unwrap_or_else(|| "cairn-development-public-url-key".to_owned());
    let public_url: Arc<dyn PublicUrl> = Arc::new(HmacPublicUrl::new(pu_secret.into_bytes()));

    // Startup reconciliation reclaims orphaned blobs from any crash window before serving. The
    // oracle is taken by `&dyn ReconcileOracle`, so the boxed oracle is borrowed via `as_ref`.
    match blob
        .reconcile(oracle.as_ref(), ReconcileOpts::default())
        .await
    {
        Ok(report) => tracing::info!(
            orphans = report.orphans_reclaimed,
            scanned = report.blobs_scanned,
            "startup reconciliation complete"
        ),
        Err(e) => tracing::warn!(error = %e, "startup reconciliation failed"),
    }

    Ok(AppStack {
        s3,
        control,
        auth,
        meta,
        blob,
        oracle,
        store,
        public_url,
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
            created_at: now,
            updated_at: now,
        },
        bearer_secret_hash: want_hash,
        sigv4_secret_ciphertext: Some(sealed.ciphertext),
        sigv4_secret_nonce: Some(sealed.nonce.0),
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
