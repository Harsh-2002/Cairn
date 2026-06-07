//! Builds and owns the concrete engine stack — the only place that names concrete
//! implementations (ARCH §12.7). It opens the metadata store and blob store, wires the
//! authenticator chain and the S3 service, and runs startup reconciliation before serving.

use crate::config::Config;
use cairn_auth::AuthChain;
use cairn_blob::LocalBlobStore;
use cairn_crypto::{SystemClock, SystemCrypto};
use cairn_meta::{OpenOptions, SqliteReconcileOracle};
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
    /// The authenticator chain.
    pub auth: Arc<dyn Authenticator>,
    // Held for the background subsystems wired in later waves (WAL checkpointer, multipart
    // sweeper, lifecycle scanner, replication workers, periodic reconcile).
    #[allow(dead_code)]
    pub meta: Arc<dyn MetadataStore>,
    #[allow(dead_code)]
    pub blob: Arc<dyn BlobStore>,
    #[allow(dead_code)]
    pub oracle: SqliteReconcileOracle,
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
    let meta: Arc<dyn MetadataStore> = Arc::new(store);

    let blob_impl = LocalBlobStore::open(cfg.data_dir.clone())
        .await
        .map_err(|e| format!("open blob store: {e}"))?;
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
        auth,
        meta,
        blob,
        oracle,
    })
}
