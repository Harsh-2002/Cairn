//! Builds and owns the concrete engine stack — the only place that names concrete
//! implementations (ARCH §12.7). It opens the metadata store and blob store, wires the
//! authenticator chain and the S3 service, and runs startup reconciliation before serving.

use crate::config::Config;
use cairn_auth::AuthChain;
use cairn_blob::LocalBlobStore;
use cairn_crypto::{SystemClock, SystemCrypto};
use cairn_meta::{OpenOptions, SqliteMetadataStore, SqliteReconcileOracle};
use cairn_s3::S3Service;
use cairn_types::blob::ReconcileOpts;
use cairn_types::traits::{
    Authenticator, AuthorizationEngine, BlobStore, Clock, Crypto, MetadataStore,
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
    /// probe, and the background subsystems (multipart sweeper, lifecycle scanner).
    pub meta: Arc<dyn MetadataStore>,
    /// The blob store. Held for the background subsystems (sweeper, periodic reconcile).
    #[allow(dead_code)]
    pub blob: Arc<dyn BlobStore>,
    /// The reconciliation oracle. Held for periodic out-of-band reconcile.
    #[allow(dead_code)]
    pub oracle: SqliteReconcileOracle,
    /// A typed handle to the concrete SQLite store. The WAL checkpointer's `checkpoint()` and
    /// `wal_size_bytes()` are inherent methods on `SqliteMetadataStore`, not part of the
    /// `MetadataStore` trait object, so the concrete store is threaded through here rather than
    /// reached via `meta` (ARCH §8.4/§11.2).
    pub store: Arc<SqliteMetadataStore>,
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

/// Open the stores, wire the stack, and run startup reconciliation.
///
/// # Errors
/// Returns a message if any store cannot be opened or the master key is invalid.
pub async fn build(cfg: &Config) -> Result<AppStack, String> {
    tokio::fs::create_dir_all(&cfg.data_dir)
        .await
        .map_err(|e| format!("create data_dir: {e}"))?;

    let store = cairn_meta::open(&cfg.db_path, &OpenOptions::default())
        .map_err(|e| format!("open metadata store: {e}"))?;
    let oracle = store.reconcile_oracle();
    // Open + migrations already ran inside `open`; keep a typed handle for the WAL checkpointer
    // (its `checkpoint()`/`wal_size_bytes()` are inherent methods, not on the trait object) and
    // share the same store behind the trait object for everything else.
    let store = Arc::new(store);
    let meta: Arc<dyn MetadataStore> = store.clone();

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
        cfg.region.clone(),
        cfg.max_object_size,
    );
    let control = cairn_control::ControlService::new(
        meta.clone(),
        blob.clone(),
        crypto.clone(),
        clock.clone(),
    );

    // Startup reconciliation reclaims orphaned blobs from any crash window before serving.
    match blob.reconcile(&oracle, ReconcileOpts::default()).await {
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
    })
}
