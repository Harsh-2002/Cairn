//! Background master-key re-wrap worker (audit #29, Phase D): re-seals stored secrets that are
//! sealed under a non-active ring key — or are legacy (pre-#29, no-magic/plaintext webhook)
//! values — onto the active key, so an old master key can eventually be retired. SQLite backend
//! only (one worker per shard). Resumable via the `rewrap_progress` table; idempotent — a blob
//! already sealed under the active key is skipped by a cheap byte check, never decrypted. A
//! re-seal that cannot open (e.g. its key was removed) is logged and skipped; data is never deleted
//! or corrupted.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use cairn_crypto::SystemCrypto;
use cairn_meta::{CachedMetadataStore, SqliteMetadataStore};
use cairn_types::bucket::{ConfigAspect, ConfigDoc};
use cairn_types::crypto::Nonce;
use cairn_types::error::MetaError;
use cairn_types::meta::Mutation;
use cairn_types::notification::{NotificationConfig, WebhookSecret};
use cairn_types::sse::SseDescriptor;
use cairn_types::traits::{Crypto, MetadataStore};
use std::sync::Arc;
use std::time::Duration;

/// Rows re-wrapped per page before persisting the cursor (resumability granularity).
const BATCH: u32 = 500;

/// Exhaustive registry of durable secret-envelope streams covered by automatic re-wrap and the
/// retirement gate.
///
/// This enum is deliberately matched exhaustively by [`run_once`], while the startup gate and
/// crypto-status endpoint iterate [`SEALED_SECRET_STREAMS`]. Adding a new durable secret location
/// therefore creates one compiler-visible registry entry instead of another drifting string list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SealedSecretStream {
    ObjectVersions,
    Users,
    ReplicationTargets,
    Notifications,
    SessionCredentials,
    ImportJobs,
}

impl SealedSecretStream {
    /// Stable persisted stream name in `rewrap_progress`.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::ObjectVersions => "object_versions.sse_descriptor",
            Self::Users => "users.sigv4_secret",
            Self::ReplicationTargets => "bucket_config.replication_targets",
            Self::Notifications => "bucket_config.notifications",
            Self::SessionCredentials => "session_credentials.secret",
            Self::ImportJobs => "import_jobs.secret",
        }
    }
}

/// The single sealed-secret registry consumed by the worker, startup retire gate, and status API.
pub(crate) const SEALED_SECRET_STREAMS: [SealedSecretStream; 6] = [
    SealedSecretStream::ObjectVersions,
    SealedSecretStream::Users,
    SealedSecretStream::ReplicationTargets,
    SealedSecretStream::Notifications,
    SealedSecretStream::SessionCredentials,
    SealedSecretStream::ImportJobs,
];

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Run the per-shard re-wrap loop. The caller owns the task handle and only calls this when the
/// configured interval is non-zero. The worker shares one `Arc<SystemCrypto>` (the ring) with the
/// rest of the stack and stops before starting another resumable pass after shutdown is signalled.
pub(crate) async fn rewrap_loop(
    store: Arc<SqliteMetadataStore>,
    crypto: Arc<SystemCrypto>,
    cache: Arc<CachedMetadataStore>,
    interval_secs: u64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let interval = Duration::from_secs(interval_secs);
    loop {
        if *shutdown.borrow() {
            return;
        }
        if let Err(e) = run_once(&store, &crypto, &cache).await {
            tracing::warn!(error = %e, "master-key re-wrap pass failed");
        }
        if !crate::background::wait_for_interval_or_shutdown(interval, &mut shutdown).await {
            return;
        }
    }
}

/// Run the per-shard active-key seal-count flush loop (audit #29, Phase E). The caller owns the
/// task handle and only calls this when the configured interval is non-zero.
pub(crate) async fn counter_sync_loop(
    store: Arc<SqliteMetadataStore>,
    crypto: Arc<SystemCrypto>,
    interval_secs: u64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let interval = Duration::from_secs(interval_secs);
    while crate::background::wait_for_interval_or_shutdown(interval, &mut shutdown).await {
        if let Err(e) = sync_seal_count(&store, &crypto).await {
            tracing::warn!(error = %e, "active-key seal-count flush failed");
        }
    }
}

/// Persist the active key's current process-wide seal counter on one SQLite shard.
///
/// Used by both the periodic worker and the ordered shutdown finalizer. The finalizer calls this
/// even when periodic synchronization is disabled, then checkpoints the WAL so the last observed
/// nonce count survives a clean restart.
pub(crate) async fn sync_seal_count(
    store: &SqliteMetadataStore,
    crypto: &SystemCrypto,
) -> Result<(), MetaError> {
    store
        .key_ring_sync_seal_count(crypto.active_key_id(), crypto.seal_count())
        .await
}

async fn run_once(
    store: &SqliteMetadataStore,
    crypto: &SystemCrypto,
    cache: &CachedMetadataStore,
) -> Result<(), MetaError> {
    for stream in SEALED_SECRET_STREAMS {
        match stream {
            SealedSecretStream::ObjectVersions => rewrap_sse(store, crypto).await?,
            SealedSecretStream::Users => rewrap_users(store, crypto).await?,
            SealedSecretStream::ReplicationTargets => rewrap_targets(store, crypto, cache).await?,
            SealedSecretStream::Notifications => rewrap_notifications(store, crypto, cache).await?,
            SealedSecretStream::SessionCredentials => {
                rewrap_session_credentials(store, crypto).await?
            }
            SealedSecretStream::ImportJobs => rewrap_import_jobs(store, crypto).await?,
        }
    }
    Ok(())
}

/// Seal every legacy plaintext webhook HMAC key before the listeners bind.
///
/// This compatibility migration is deliberately independent of the periodic SQLite-only rewrap
/// worker: operators may disable that worker, and libSQL/Turso do not run it. Startup is the one
/// cross-backend point with no in-process request concurrency, so replacing a legacy config through
/// the ordinary writer is safe and cache-coherent. Any read/parse/seal/write failure is fatal; Cairn
/// never knowingly starts while a database-readable webhook capability remains in plaintext.
pub(crate) async fn migrate_legacy_webhook_secrets(
    meta: &dyn MetadataStore,
    crypto: &dyn Crypto,
) -> Result<u64, String> {
    let buckets = meta
        .list_buckets(None)
        .await
        .map_err(|_| "list buckets for webhook-secret migration failed".to_owned())?;
    let mut migrated = 0u64;
    for bucket in buckets {
        let Some(doc) = meta
            .get_bucket_config(&bucket.name, ConfigAspect::Notification)
            .await
            .map_err(|_| {
                format!(
                    "read notification config for bucket {:?} during secret migration failed",
                    bucket.name.as_str()
                )
            })?
        else {
            continue;
        };
        let mut config: NotificationConfig = serde_json::from_str(&doc.0).map_err(|_| {
            format!(
                "notification config for bucket {:?} is invalid; refusing plaintext-secret migration",
                bucket.name.as_str()
            )
        })?;
        let mut changed = false;
        for endpoint in &mut config.endpoints {
            let Some(WebhookSecret::LegacyPlaintext(plaintext)) = endpoint.secret.as_ref() else {
                continue;
            };
            let sealed = crypto
                .seal(plaintext.expose_secret().as_bytes())
                .map_err(|_| {
                    format!(
                        "seal webhook secret for bucket {:?} failed",
                        bucket.name.as_str()
                    )
                })?;
            endpoint.secret = Some(WebhookSecret::from_sealed(sealed));
            changed = true;
        }
        if !changed {
            continue;
        }
        let replacement = serde_json::to_string(&config).map_err(|_| {
            format!(
                "serialize notification config for bucket {:?} failed",
                bucket.name.as_str()
            )
        })?;
        meta.submit(Mutation::SetBucketConfig {
            bucket: bucket.name.clone(),
            aspect: ConfigAspect::Notification,
            doc: Some(ConfigDoc(replacement)),
        })
        .await
        .map_err(|_| {
            format!(
                "store sealed notification config for bucket {:?} failed",
                bucket.name.as_str()
            )
        })?;
        migrated += 1;
    }
    Ok(migrated)
}

async fn rewrap_sse(store: &SqliteMetadataStore, crypto: &SystemCrypto) -> Result<(), MetaError> {
    let active = crypto.active_key_id();
    let stream = SealedSecretStream::ObjectVersions.name();
    let mut cursor = store.rewrap_cursor(stream.to_owned()).await?;
    let started_fresh = cursor.is_none();
    let mut pass_failed = 0u64;
    loop {
        let page = store.rewrap_sse_page(cursor.clone(), BATCH).await?;
        if page.is_empty() {
            break;
        }
        let n = page.len();
        let (mut done, mut failed) = (0u64, 0u64);
        let mut last = cursor.clone();
        for (pk, descriptor) in page {
            last = Some(pk.clone());
            match rewrap_sse_descriptor(crypto, &descriptor) {
                // Compare-and-swap on the descriptor we read: if a concurrent write changed the row
                // meanwhile, the CAS no-ops (it is already current) rather than clobbering it.
                Ok(Some(new_desc)) => {
                    match store.rewrap_set_sse(pk.clone(), descriptor, new_desc).await {
                        Ok(true) => done += 1,
                        Ok(false) => {} // CAS miss: row changed concurrently; leave it
                        Err(_) => failed += 1,
                    }
                }
                Ok(None) => {} // already active key — skip
                Err(()) => {
                    failed += 1;
                    tracing::warn!(version = %pk, "SSE re-wrap could not open the DEK; skipping");
                }
            }
        }
        pass_failed += failed;
        store
            .rewrap_set_progress(stream.to_owned(), last.clone(), done, failed, now_ms())
            .await?;
        cursor = last;
        if n < BATCH as usize {
            break;
        }
    }
    // Completion (clearing the cursor for a future rotation) records the active id ONLY for an
    // uninterrupted full pass (started at the head) with zero failures — so a key is never shown
    // retire-eligible before its data is actually re-wrapped (audit #29). A resumed pass or any
    // failure records 0, leaving the stream "not complete" until a clean full pass confirms it.
    let done_id = if started_fresh && pass_failed == 0 {
        active
    } else {
        0
    };
    store
        .rewrap_finish_pass(stream.to_owned(), done_id, now_ms())
        .await
}

/// Re-wrap one SSE descriptor's DEK onto the active key. `Ok(None)` if already active.
fn rewrap_sse_descriptor(crypto: &SystemCrypto, json: &str) -> Result<Option<String>, ()> {
    let mut d: SseDescriptor = serde_json::from_str(json).map_err(|_| ())?;
    let envelope = B64.decode(d.wrapped_dek_b64.as_bytes()).map_err(|_| ())?;
    if !crypto.needs_rewrap(&envelope) {
        return Ok(None);
    }
    let nonce = if d.nonce_b64.is_empty() {
        Vec::new()
    } else {
        B64.decode(d.nonce_b64.as_bytes()).map_err(|_| ())?
    };
    let dek = crypto.open(&envelope, &Nonce(nonce)).map_err(|_| ())?;
    let resealed = crypto.seal(&dek).map_err(|_| ())?;
    // Mutate IN PLACE — never reconstruct field-by-field. A rebuild silently drops any field this
    // binary does not name, which is exactly the label-drop the `extra` flatten exists to prevent;
    // in-place keeps every known and unknown field by construction.
    d.wrapped_dek_b64 = B64.encode(&resealed.ciphertext);
    d.nonce_b64.clear();
    serde_json::to_string(&d).map(Some).map_err(|_| ())
}

async fn rewrap_users(store: &SqliteMetadataStore, crypto: &SystemCrypto) -> Result<(), MetaError> {
    let active = crypto.active_key_id();
    let stream = SealedSecretStream::Users.name();
    let mut cursor = store.rewrap_cursor(stream.to_owned()).await?;
    let started_fresh = cursor.is_none();
    let mut pass_failed = 0u64;
    loop {
        let page = store.rewrap_users_page(cursor.clone(), BATCH).await?;
        if page.is_empty() {
            break;
        }
        let n = page.len();
        let (mut done, mut failed) = (0u64, 0u64);
        let mut last = cursor.clone();
        for (id, ct, nonce) in page {
            last = Some(id.clone());
            if !crypto.needs_rewrap(&ct) {
                continue;
            }
            match crypto
                .open(&ct, &Nonce(nonce.unwrap_or_default()))
                .and_then(|secret| crypto.seal(&secret))
            {
                // Compare-and-swap on the secret we read: a concurrent credential rotation (which
                // re-seals under the active key) is NOT clobbered — the CAS just no-ops.
                Ok(resealed) => {
                    match store
                        .rewrap_set_user_sigv4(id.clone(), ct, resealed.ciphertext)
                        .await
                    {
                        Ok(true) => done += 1,
                        Ok(false) => {} // CAS miss: rotated concurrently; the newer value stands
                        Err(_) => failed += 1,
                    }
                }
                Err(_) => {
                    failed += 1;
                    tracing::warn!(user = %id, "SigV4 secret re-wrap could not open; skipping");
                }
            }
        }
        pass_failed += failed;
        store
            .rewrap_set_progress(stream.to_owned(), last.clone(), done, failed, now_ms())
            .await?;
        cursor = last;
        if n < BATCH as usize {
            break;
        }
    }
    let done_id = if started_fresh && pass_failed == 0 {
        active
    } else {
        0
    };
    store
        .rewrap_finish_pass(stream.to_owned(), done_id, now_ms())
        .await
}

/// Re-seal every durable STS/session signing secret onto the active master key.
async fn rewrap_session_credentials(
    store: &SqliteMetadataStore,
    crypto: &SystemCrypto,
) -> Result<(), MetaError> {
    let active = crypto.active_key_id();
    let stream = SealedSecretStream::SessionCredentials.name();
    let mut cursor = store.rewrap_cursor(stream.to_owned()).await?;
    let started_fresh = cursor.is_none();
    let mut pass_failed = 0u64;
    loop {
        let page = store
            .rewrap_session_credentials_page(cursor.clone(), BATCH)
            .await?;
        if page.is_empty() {
            break;
        }
        let n = page.len();
        let (mut done, mut failed) = (0u64, 0u64);
        let mut last = cursor.clone();
        for (access_key_id, ciphertext, nonce) in page {
            last = Some(access_key_id.clone());
            if !crypto.needs_rewrap(&ciphertext) {
                continue;
            }
            match crypto
                .open(&ciphertext, &Nonce(nonce.unwrap_or_default()))
                .and_then(|secret| crypto.seal(&secret))
            {
                Ok(resealed) => {
                    match store
                        .rewrap_set_session_credential(
                            access_key_id.clone(),
                            ciphertext,
                            resealed.ciphertext,
                        )
                        .await
                    {
                        Ok(true) => done += 1,
                        // A concurrent revoke/expiry sweep is safe, but this pass did not prove the
                        // exact row set it read. Keep the gate closed until a later clean pass.
                        Ok(false) => failed += 1,
                        Err(_) => failed += 1,
                    }
                }
                Err(_) => {
                    failed += 1;
                    tracing::warn!(
                        access_key_id = %access_key_id,
                        "temporary-session secret re-wrap could not open; skipping"
                    );
                }
            }
        }
        pass_failed += failed;
        store
            .rewrap_set_progress(stream.to_owned(), last.clone(), done, failed, now_ms())
            .await?;
        cursor = last;
        if n < BATCH as usize {
            break;
        }
    }
    let done_id = if started_fresh && pass_failed == 0 {
        active
    } else {
        0
    };
    store
        .rewrap_finish_pass(stream.to_owned(), done_id, now_ms())
        .await
}

/// Re-seal source credentials for every durable import job, including terminal history retained
/// for operator inspection. The row is the secret's lifetime; only pruning removes it from the
/// retirement population.
async fn rewrap_import_jobs(
    store: &SqliteMetadataStore,
    crypto: &SystemCrypto,
) -> Result<(), MetaError> {
    let active = crypto.active_key_id();
    let stream = SealedSecretStream::ImportJobs.name();
    let mut cursor = store.rewrap_cursor(stream.to_owned()).await?;
    let started_fresh = cursor.is_none();
    let mut pass_failed = 0u64;
    loop {
        let page = store.rewrap_import_jobs_page(cursor.clone(), BATCH).await?;
        if page.is_empty() {
            break;
        }
        let n = page.len();
        let (mut done, mut failed) = (0u64, 0u64);
        let mut last = cursor.clone();
        for (job_id, ciphertext, nonce) in page {
            last = Some(job_id.clone());
            if !crypto.needs_rewrap(&ciphertext) {
                continue;
            }
            match crypto
                .open(&ciphertext, &Nonce(nonce.unwrap_or_default()))
                .and_then(|secret| crypto.seal(&secret))
            {
                Ok(resealed) => {
                    match store
                        .rewrap_set_import_job_secret(
                            job_id.clone(),
                            ciphertext,
                            resealed.ciphertext,
                        )
                        .await
                    {
                        Ok(true) => done += 1,
                        // A concurrent retention prune wins. Require another full pass rather than
                        // infer that a missed compare-and-swap is harmless.
                        Ok(false) => failed += 1,
                        Err(_) => failed += 1,
                    }
                }
                Err(_) => {
                    failed += 1;
                    tracing::warn!(
                        job_id = %job_id,
                        "import source secret re-wrap could not open; skipping"
                    );
                }
            }
        }
        pass_failed += failed;
        store
            .rewrap_set_progress(stream.to_owned(), last.clone(), done, failed, now_ms())
            .await?;
        cursor = last;
        if n < BATCH as usize {
            break;
        }
    }
    let done_id = if started_fresh && pass_failed == 0 {
        active
    } else {
        0
    };
    store
        .rewrap_finish_pass(stream.to_owned(), done_id, now_ms())
        .await
}

async fn rewrap_targets(
    store: &SqliteMetadataStore,
    crypto: &SystemCrypto,
    cache: &CachedMetadataStore,
) -> Result<(), MetaError> {
    let active = crypto.active_key_id();
    let mut pass_failed = 0u64;
    for b in store.list_buckets(None).await? {
        let Some(doc) = store
            .get_bucket_config(&b.name, ConfigAspect::ReplicationTargets)
            .await?
        else {
            continue;
        };
        // Keep the doc we read as the compare-and-swap witness, so a concurrently-added/edited
        // target list is never clobbered by our re-seal (audit #29 lost-update).
        let expected = doc.0;
        let Ok(mut targets) = cairn_replication::parse_targets(expected.as_bytes()) else {
            continue;
        };
        let mut changed = false;
        for t in &mut targets {
            if !crypto.needs_rewrap(&t.secret_ciphertext) {
                continue;
            }
            match crypto
                .open(&t.secret_ciphertext, &Nonce(t.nonce.clone()))
                .and_then(|secret| crypto.seal(&secret))
            {
                Ok(resealed) => {
                    t.secret_ciphertext = resealed.ciphertext;
                    t.nonce = Vec::new();
                    changed = true;
                }
                Err(_) => {
                    pass_failed += 1;
                    tracing::warn!(bucket = %b.name, "replication target re-wrap could not open; skipping");
                }
            }
        }
        if changed {
            let new_doc = cairn_replication::serialize_targets(&targets);
            match store
                .rewrap_set_bucket_config_cas(
                    b.name.to_string(),
                    ConfigAspect::ReplicationTargets,
                    expected,
                    new_doc,
                )
                .await
            {
                Ok(true) => {
                    // The re-seal committed on the RAW store, bypassing the read-through cache's
                    // decorator, so evict the now-stale cached targets doc — otherwise the control
                    // plane keeps serving (and can re-persist) the pre-rewrap old-key doc after the
                    // pass "finished" (audit 2026-07).
                    cache.invalidate_config_aspect(&b.name, ConfigAspect::ReplicationTargets);
                }
                Ok(false) => {
                    // A CAS miss means a concurrent target edit landed between our read and our
                    // write, so THIS bucket was not re-sealed this pass. Treat it exactly like an
                    // open failure (which already bumps `pass_failed`): the pass is incomplete and
                    // must NOT record completion under the active id. Audit 2026-07: recording the
                    // targets stream "done" after a CAS miss let the retire-gate delete a master key
                    // still sealing a sibling target's secret — silent, unrecoverable loss. The next
                    // pass re-attempts.
                    pass_failed += 1;
                    tracing::debug!(bucket = %b.name, "target re-wrap CAS miss; deferring completion to next pass");
                }
                Err(e) => return Err(e),
            }
        }
    }
    // Targets have no resume cursor (each pass scans every bucket), so only a pass with zero
    // failures AND zero CAS misses is a complete pass under the active key (audit #29).
    let done_id = bucket_config_pass_done_id(active, pass_failed);
    store
        .rewrap_finish_pass(
            SealedSecretStream::ReplicationTargets.name().to_owned(),
            done_id,
            now_ms(),
        )
        .await
}

/// Re-seal stored webhook HMAC keys, including the one-time migration from legacy plaintext JSON.
///
/// Like replication targets, notification configs have no resume cursor: each pass scans all
/// buckets and uses the raw document as a CAS witness so a concurrent operator edit always wins.
async fn rewrap_notifications(
    store: &SqliteMetadataStore,
    crypto: &SystemCrypto,
    cache: &CachedMetadataStore,
) -> Result<(), MetaError> {
    let active = crypto.active_key_id();
    let mut pass_failed = 0u64;
    for bucket in store.list_buckets(None).await? {
        let Some(doc) = store
            .get_bucket_config(&bucket.name, ConfigAspect::Notification)
            .await?
        else {
            continue;
        };
        let expected = doc.0;
        let new_doc = match rewrap_notification_config(crypto, &expected) {
            Ok(new_doc) => new_doc,
            Err(()) => {
                pass_failed += 1;
                tracing::warn!(
                    bucket = %bucket.name,
                    "webhook secret re-wrap could not parse/open/seal configuration; skipping"
                );
                continue;
            }
        };
        let Some(new_doc) = new_doc else {
            continue;
        };
        match store
            .rewrap_set_bucket_config_cas(
                bucket.name.to_string(),
                ConfigAspect::Notification,
                expected,
                new_doc,
            )
            .await
        {
            Ok(true) => {
                cache.invalidate_config_aspect(&bucket.name, ConfigAspect::Notification);
            }
            Ok(false) => {
                pass_failed += 1;
                tracing::debug!(
                    bucket = %bucket.name,
                    "notification re-wrap CAS miss; deferring completion to next pass"
                );
            }
            Err(error) => return Err(error),
        }
    }
    let done_id = bucket_config_pass_done_id(active, pass_failed);
    store
        .rewrap_finish_pass(
            SealedSecretStream::Notifications.name().to_owned(),
            done_id,
            now_ms(),
        )
        .await
}

/// Transform one stored notification document without changing any subscription fields.
///
/// `Ok(None)` means all secrets are already sealed under the active key. The whole document fails
/// atomically when any one secret cannot be opened/sealed; the caller then leaves the original JSON
/// untouched and keeps the retirement gate closed.
fn rewrap_notification_config(crypto: &SystemCrypto, json: &str) -> Result<Option<String>, ()> {
    let mut config: NotificationConfig = serde_json::from_str(json).map_err(|_| ())?;
    let mut changed = false;
    for endpoint in &mut config.endpoints {
        let Some(secret) = endpoint.secret.as_mut() else {
            continue;
        };
        let replacement = match secret {
            WebhookSecret::LegacyPlaintext(plaintext) => Some(
                crypto
                    .seal(plaintext.expose_secret().as_bytes())
                    .map_err(|_| ())?,
            ),
            WebhookSecret::Sealed(sealed) if crypto.needs_rewrap(&sealed.ciphertext) => {
                let plaintext = crypto
                    .open(&sealed.ciphertext, &Nonce(sealed.nonce.clone()))
                    .map_err(|_| ())?;
                Some(crypto.seal(&plaintext).map_err(|_| ())?)
            }
            WebhookSecret::Sealed(_) => None,
        };
        if let Some(resealed) = replacement {
            *secret = WebhookSecret::from_sealed(resealed);
            changed = true;
        }
    }
    if changed {
        serde_json::to_string(&config).map(Some).map_err(|_| ())
    } else {
        Ok(None)
    }
}

/// The `done_active_id` a bucket-config re-wrap pass records: the active id only if the pass
/// re-sealed every bucket with zero failures and zero CAS misses; otherwise 0, so the retire-gate
/// stays closed until a genuinely clean pass (audit #29 / 2026-07).
fn bucket_config_pass_done_id(active: u16, pass_failed: u64) -> u16 {
    if pass_failed == 0 { active } else { 0 }
}

#[cfg(test)]
mod tests {
    use super::{
        B64, Nonce, SEALED_SECRET_STREAMS, SystemCrypto, bucket_config_pass_done_id,
        migrate_legacy_webhook_secrets, rewrap_import_jobs, rewrap_notification_config,
        rewrap_notifications, rewrap_session_credentials, rewrap_sse, rewrap_sse_descriptor,
        rewrap_targets, rewrap_users,
    };
    use base64::Engine;
    use cairn_meta::CachedMetadataStore;
    use cairn_replication::RemoteTarget;
    use cairn_types::auth::Role;
    use cairn_types::authz::OwnershipMode;
    use cairn_types::bucket::{Bucket, ConfigAspect, ConfigDoc, VersioningState};
    use cairn_types::meta::{
        ImportJobRecord, ImportState, Mutation, Precondition, SessionCredentialRecord, User,
        UserRecord,
    };
    use cairn_types::notification::{NotificationConfig, WebhookEndpoint, WebhookSecret};
    use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
    use cairn_types::sse::SseDescriptor;
    use cairn_types::traits::{Crypto, MetadataStore};
    use cairn_types::{
        BucketName, ObjectKey, SecretString, StoragePath, Timestamp, UserId, VersionId,
    };
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn rewrap_preserves_mode_and_kms_key_id() {
        // A master-key rotation must reseal the DEK under the new active key WITHOUT dropping the
        // `mode`/`kms_key_id` labels — losing them would silently make an at-rest object (which
        // advertises nothing) start advertising AES256. Increment 0's flatten-preserve guards this.
        let (k1, k2, dek) = ([1u8; 32], [2u8; 32], [7u8; 32]);
        // Seal the DEK under key 1 (key 1 active).
        let old = SystemCrypto::from_ring(vec![(1, k1.into())], 1, 1, 0).unwrap();
        let sealed = old.seal(&dek).unwrap();
        let json = format!(
            r#"{{"alg":"AES256","wrapped_dek_b64":"{}","nonce_b64":"","mode":"at-rest","kms_key_id":"tenant-A"}}"#,
            B64.encode(&sealed.ciphertext)
        );
        // Now key 2 is active (key 1 retained for opening) — the descriptor needs a rewrap.
        let new = SystemCrypto::from_ring(vec![(1, k1.into()), (2, k2.into())], 2, 1, 0).unwrap();
        let out = rewrap_sse_descriptor(&new, &json)
            .unwrap()
            .expect("a key-1-sealed descriptor must need rewrap under active key 2");
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        // The labels survived the rotation.
        assert_eq!(v["mode"], "at-rest", "mode dropped on rewrap: {out}");
        assert_eq!(
            v["kms_key_id"], "tenant-A",
            "kms_key_id dropped on rewrap: {out}"
        );
        // The DEK was genuinely resealed under the active key and still round-trips.
        let env = B64.decode(v["wrapped_dek_b64"].as_str().unwrap()).unwrap();
        assert!(
            !new.needs_rewrap(&env),
            "resealed DEK must be under the active key"
        );
        assert_eq!(
            &new.open(&env, &Nonce(Vec::new())).unwrap()[..],
            &dek[..],
            "DEK must round-trip after rewrap"
        );
    }

    #[test]
    fn rewrap_preserves_unknown_fields_written_by_a_newer_node() {
        // The descriptor's `#[serde(flatten)] extra` is load-bearing: a field a NEWER node stamped
        // (and this binary has never heard of) must survive a master-key rotation untouched.
        // Without the flatten — or if the rewrap rebuilt the struct field-by-field — the rotation
        // would silently erase it.
        let (k1, k2, dek) = ([1u8; 32], [2u8; 32], [9u8; 32]);
        let old = SystemCrypto::from_ring(vec![(1, k1.into())], 1, 1, 0).unwrap();
        let sealed = old.seal(&dek).unwrap();
        let json = format!(
            r#"{{"alg":"AES256","wrapped_dek_b64":"{}","nonce_b64":"","future_field":{{"x":1}}}}"#,
            B64.encode(&sealed.ciphertext)
        );
        let new = SystemCrypto::from_ring(vec![(1, k1.into()), (2, k2.into())], 2, 1, 0).unwrap();
        let out = rewrap_sse_descriptor(&new, &json).unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            v["future_field"]["x"], 1,
            "an unknown descriptor field must survive a rewrap: {out}"
        );
    }

    #[test]
    fn incomplete_targets_pass_does_not_record_completion() {
        // A clean pass records the active id.
        assert_eq!(bucket_config_pass_done_id(3, 0), 3);
        // Any failure OR CAS miss (both feed pass_failed) records 0, keeping the retire-gate closed
        // (audit 2026-07: pre-fix a CAS miss still recorded `active`, so a key still sealing a
        // target secret could be retired).
        assert_eq!(bucket_config_pass_done_id(3, 1), 0);
        assert_eq!(bucket_config_pass_done_id(3, 5), 0);
    }

    fn webhook_config(secret: WebhookSecret) -> NotificationConfig {
        NotificationConfig {
            endpoints: vec![WebhookEndpoint {
                id: "audit".to_owned(),
                url: "https://example.test/hook".to_owned(),
                events: vec!["s3:ObjectCreated:*".to_owned()],
                prefix: Some("logs/".to_owned()),
                suffix: Some(".json".to_owned()),
                secret: Some(secret),
            }],
        }
    }

    async fn registry_min_done(store: &cairn_meta::SqliteMetadataStore) -> u16 {
        let done: std::collections::HashMap<String, u16> = store
            .rewrap_done_active_ids()
            .await
            .unwrap()
            .into_iter()
            .collect();
        SEALED_SECRET_STREAMS
            .iter()
            .map(|stream| done.get(stream.name()).copied().unwrap_or(0))
            .min()
            .unwrap_or(0)
    }

    fn retirement_is_blocked(min_done: u16) -> bool {
        !crate::stack::retire_gate_unsafe_ids(&[1, 2], &HashSet::from([2]), 2, min_done).is_empty()
    }

    #[test]
    fn legacy_webhook_plaintext_is_removed_and_sealed() {
        let crypto = SystemCrypto::from_ring(vec![(7, [7u8; 32].into())], 7, 7, 0).unwrap();
        let sentinel = "AUD015-plaintext-must-disappear";
        let json = serde_json::to_string(&webhook_config(WebhookSecret::LegacyPlaintext(
            SecretString::from(sentinel),
        )))
        .unwrap();
        assert!(json.contains(sentinel));

        let migrated = rewrap_notification_config(&crypto, &json)
            .unwrap()
            .expect("legacy plaintext must be migrated");
        assert!(
            !migrated.contains(sentinel),
            "the raw replacement document must not retain plaintext"
        );
        let parsed: NotificationConfig = serde_json::from_str(&migrated).unwrap();
        let WebhookSecret::Sealed(sealed) = parsed.endpoints[0].secret.as_ref().unwrap() else {
            panic!("migration must persist a sealed envelope");
        };
        assert_eq!(
            crypto
                .open(&sealed.ciphertext, &Nonce(sealed.nonce.clone()),)
                .unwrap()
                .as_slice(),
            sentinel.as_bytes()
        );
    }

    #[test]
    fn webhook_secret_is_resealed_under_active_key_without_field_loss() {
        let old = SystemCrypto::from_ring(vec![(1, [1u8; 32].into())], 1, 1, 0).unwrap();
        let old_sealed = old.seal(b"rotation-secret").unwrap();
        let json =
            serde_json::to_string(&webhook_config(WebhookSecret::from_sealed(old_sealed))).unwrap();
        let rotated =
            SystemCrypto::from_ring(vec![(1, [1u8; 32].into()), (2, [2u8; 32].into())], 2, 1, 0)
                .unwrap();

        let migrated = rewrap_notification_config(&rotated, &json)
            .unwrap()
            .expect("old-key secret must be rewrapped");
        let parsed: NotificationConfig = serde_json::from_str(&migrated).unwrap();
        let endpoint = &parsed.endpoints[0];
        assert_eq!(endpoint.prefix.as_deref(), Some("logs/"));
        assert_eq!(endpoint.suffix.as_deref(), Some(".json"));
        let WebhookSecret::Sealed(sealed) = endpoint.secret.as_ref().unwrap() else {
            panic!("rewrap must preserve sealed representation");
        };
        assert!(!rotated.needs_rewrap(&sealed.ciphertext));
        assert_eq!(
            rotated
                .open(&sealed.ciphertext, &Nonce(sealed.nonce.clone()),)
                .unwrap()
                .as_slice(),
            b"rotation-secret"
        );
    }

    #[tokio::test]
    async fn notification_rewrap_migrates_raw_doc_invalidates_cache_and_marks_stream_done() {
        let store = Arc::new(cairn_meta::open_in_memory().unwrap());
        let cache = CachedMetadataStore::new(store.clone() as Arc<dyn MetadataStore>, 1024 * 1024);
        let bucket = BucketName::parse("rewrap-hooks").unwrap();
        store
            .submit(Mutation::CreateBucket(Box::new(Bucket {
                name: bucket.clone(),
                owner_id: UserId("owner".to_owned()),
                created_at: Timestamp::from_secs(1),
                versioning: VersioningState::Unversioned,
                ownership_mode: OwnershipMode::BucketOwnerEnforced,
                region: "us-east-1".to_owned(),
                compression: None,
            })))
            .await
            .unwrap();
        let sentinel = "AUD015-live-legacy-config";
        store
            .submit(Mutation::SetBucketConfig {
                bucket: bucket.clone(),
                aspect: ConfigAspect::Notification,
                doc: Some(ConfigDoc(
                    serde_json::to_string(&webhook_config(WebhookSecret::LegacyPlaintext(
                        SecretString::from(sentinel),
                    )))
                    .unwrap(),
                )),
            })
            .await
            .unwrap();
        assert!(
            cache
                .get_bucket_config(&bucket, ConfigAspect::Notification)
                .await
                .unwrap()
                .unwrap()
                .0
                .contains(sentinel),
            "precondition: the legacy document is resident in the cache"
        );

        let crypto = SystemCrypto::from_ring(vec![(9, [9u8; 32].into())], 9, 9, 0).unwrap();
        rewrap_notifications(&store, &crypto, &cache).await.unwrap();

        let raw = store
            .get_bucket_config(&bucket, ConfigAspect::Notification)
            .await
            .unwrap()
            .unwrap();
        assert!(!raw.0.contains(sentinel));
        let reread = cache
            .get_bucket_config(&bucket, ConfigAspect::Notification)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !reread.0.contains(sentinel),
            "raw-store CAS must evict the cached plaintext document"
        );
        let progress: std::collections::HashMap<String, u16> = store
            .rewrap_done_active_ids()
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            progress.get(super::SealedSecretStream::Notifications.name()),
            Some(&9),
            "a clean notification pass must satisfy its retirement-gate stream"
        );
    }

    #[tokio::test]
    async fn startup_migration_seals_legacy_webhooks_without_periodic_worker() {
        let store = cairn_meta::open_in_memory().unwrap();
        let bucket = BucketName::parse("startup-hooks").unwrap();
        store
            .submit(Mutation::CreateBucket(Box::new(Bucket {
                name: bucket.clone(),
                owner_id: UserId("owner".to_owned()),
                created_at: Timestamp::from_secs(1),
                versioning: VersioningState::Unversioned,
                ownership_mode: OwnershipMode::BucketOwnerEnforced,
                region: "us-east-1".to_owned(),
                compression: None,
            })))
            .await
            .unwrap();
        let sentinel = "AUD015-startup-legacy-secret";
        store
            .submit(Mutation::SetBucketConfig {
                bucket: bucket.clone(),
                aspect: ConfigAspect::Notification,
                doc: Some(ConfigDoc(
                    serde_json::to_string(&webhook_config(WebhookSecret::LegacyPlaintext(
                        SecretString::from(sentinel),
                    )))
                    .unwrap(),
                )),
            })
            .await
            .unwrap();
        let crypto = SystemCrypto::from_ring(vec![(11, [11u8; 32].into())], 11, 11, 0).unwrap();

        assert_eq!(
            migrate_legacy_webhook_secrets(&store, &crypto)
                .await
                .unwrap(),
            1
        );
        let raw = store
            .get_bucket_config(&bucket, ConfigAspect::Notification)
            .await
            .unwrap()
            .unwrap();
        assert!(!raw.0.contains(sentinel));
        let parsed: NotificationConfig = serde_json::from_str(&raw.0).unwrap();
        let WebhookSecret::Sealed(sealed) = parsed.endpoints[0].secret.as_ref().unwrap() else {
            panic!("startup migration must persist an authenticated envelope");
        };
        assert_eq!(
            crypto
                .open(&sealed.ciphertext, &Nonce(sealed.nonce.clone()))
                .unwrap()
                .as_slice(),
            sentinel.as_bytes()
        );
        assert_eq!(
            migrate_legacy_webhook_secrets(&store, &crypto)
                .await
                .unwrap(),
            0,
            "the migration is idempotent once every key is sealed"
        );
    }

    #[tokio::test]
    async fn every_registered_durable_secret_blocks_retirement_until_rewrapped() {
        assert_eq!(
            SEALED_SECRET_STREAMS.len(),
            6,
            "the registry must name every durable re-wrap stream"
        );
        let names: HashSet<&str> = SEALED_SECRET_STREAMS
            .iter()
            .map(|stream| stream.name())
            .collect();
        assert_eq!(names.len(), SEALED_SECRET_STREAMS.len());

        let store = Arc::new(cairn_meta::open_in_memory().unwrap());
        let cache = CachedMetadataStore::new(store.clone() as Arc<dyn MetadataStore>, 1024 * 1024);
        let old = SystemCrypto::from_ring(vec![(1, [1u8; 32].into())], 1, 1, 0).unwrap();
        let rotated =
            SystemCrypto::from_ring(vec![(1, [1u8; 32].into()), (2, [2u8; 32].into())], 2, 1, 0)
                .unwrap();
        let owner = UserId("rewrap-owner".to_owned());
        let bucket = BucketName::parse("all-sealed-streams").unwrap();

        let user_secret = old.seal(b"user-secret").unwrap();
        store
            .submit(Mutation::CreateUser(Box::new(UserRecord {
                user: User {
                    id: owner.clone(),
                    display_name: "Rewrap Owner".to_owned(),
                    access_key_id: "owner-bearer".to_owned(),
                    sigv4_access_key_id: Some("owner-sigv4".to_owned()),
                    role: Role::Administrator,
                    is_active: true,
                    quota_bytes: None,
                    created_at: Timestamp(1),
                    updated_at: Timestamp(1),
                },
                bearer_secret_hash: "hash".to_owned(),
                sigv4_secret_ciphertext: Some(user_secret.ciphertext),
                sigv4_secret_nonce: None,
            })))
            .await
            .unwrap();
        store
            .submit(Mutation::CreateBucket(Box::new(Bucket {
                name: bucket.clone(),
                owner_id: owner.clone(),
                created_at: Timestamp(1),
                versioning: VersioningState::Unversioned,
                ownership_mode: OwnershipMode::BucketOwnerEnforced,
                region: "us-east-1".to_owned(),
                compression: None,
            })))
            .await
            .unwrap();

        let object_dek = old.seal(&[3u8; 32]).unwrap();
        let descriptor = serde_json::to_string(&SseDescriptor {
            alg: "AES256-GCM".to_owned(),
            wrapped_dek_b64: B64.encode(object_dek.ciphertext),
            ..SseDescriptor::default()
        })
        .unwrap();
        store
            .submit(Mutation::PutObjectVersion {
                row: Box::new(ObjectVersionRow {
                    id: "old-key-object".to_owned(),
                    bucket: bucket.clone(),
                    key: ObjectKey::parse("object").unwrap(),
                    version_id: VersionId::null(),
                    is_latest: true,
                    is_delete_marker: false,
                    size_logical: 1,
                    size_physical: 1,
                    etag: ETag::from_string("etag".to_owned()),
                    content_type: "application/octet-stream".to_owned(),
                    content_encoding: None,
                    cache_control: None,
                    content_disposition: None,
                    content_language: None,
                    expires: None,
                    storage_path: Some(StoragePath::generate(&bucket)),
                    compression: CompressionDescriptor::Uncompressed,
                    storage_class: StorageClass::Standard,
                    cold_locator: None,
                    owner_id: owner.clone(),
                    user_metadata: Vec::new(),
                    acl: None,
                    checksums: Vec::new(),
                    sse_descriptor: Some(descriptor),
                    replication_status: None,
                    replicated_at: None,
                    created_at: Timestamp(1),
                    updated_at: Timestamp(1),
                }),
                precondition: Precondition::default(),
                initial_state: cairn_types::InitialObjectState::default(),
                replication: Vec::new(),
            })
            .await
            .unwrap();

        let target_secret = old.seal(b"target-secret").unwrap();
        let target_doc = cairn_replication::serialize_targets(&[RemoteTarget {
            arn: "arn:cairn:replication:us-east-1:test:mirror".to_owned(),
            endpoint: "https://replica.example.test".to_owned(),
            region: "us-east-1".to_owned(),
            dest_bucket: "mirror".to_owned(),
            access_key_id: "target-access".to_owned(),
            secret_ciphertext: target_secret.ciphertext,
            nonce: Vec::new(),
            ca_cert_pem: None,
            insecure_skip_verify: false,
        }]);
        store
            .submit(Mutation::SetBucketConfig {
                bucket: bucket.clone(),
                aspect: ConfigAspect::ReplicationTargets,
                doc: Some(ConfigDoc(target_doc)),
            })
            .await
            .unwrap();

        let notification_secret = old.seal(b"notification-secret").unwrap();
        store
            .submit(Mutation::SetBucketConfig {
                bucket: bucket.clone(),
                aspect: ConfigAspect::Notification,
                doc: Some(ConfigDoc(
                    serde_json::to_string(&webhook_config(WebhookSecret::from_sealed(
                        notification_secret,
                    )))
                    .unwrap(),
                )),
            })
            .await
            .unwrap();

        let session_secret = old.seal(b"session-secret").unwrap();
        store
            .submit(Mutation::CreateSessionCredential(Box::new(
                SessionCredentialRecord {
                    access_key_id: "session-access".to_owned(),
                    parent_user_id: owner.clone(),
                    secret_ciphertext: session_secret.ciphertext,
                    secret_nonce: None,
                    session_token_hash: "token-hash".to_owned(),
                    inline_policy: Some(r#"{"Version":"2012-10-17","Statement":[]}"#.to_owned()),
                    expires_at: Timestamp(i64::MAX),
                    created_at: Timestamp(1),
                },
            )))
            .await
            .unwrap();

        let import_secret = old.seal(b"import-secret").unwrap();
        store
            .submit(Mutation::CreateImportJob(Box::new(ImportJobRecord {
                id: "import-job".to_owned(),
                source_endpoint: "https://source.example.test".to_owned(),
                source_region: "us-east-1".to_owned(),
                access_key_id: "source-access".to_owned(),
                secret_ciphertext: import_secret.ciphertext,
                secret_nonce: None,
                ca_cert_pem: None,
                insecure_skip_verify: false,
                workers: 1,
                state: ImportState::Completed,
                buckets: Vec::new(),
                objects_done: 0,
                objects_total: 0,
                bytes_done: 0,
                bytes_total: 0,
                last_error: None,
                lease_until: None,
                created_at: Timestamp(1),
                updated_at: Timestamp(1),
            })))
            .await
            .unwrap();

        assert!(retirement_is_blocked(registry_min_done(&store).await));
        rewrap_sse(&store, &rotated).await.unwrap();
        assert!(retirement_is_blocked(registry_min_done(&store).await));
        rewrap_users(&store, &rotated).await.unwrap();
        assert!(retirement_is_blocked(registry_min_done(&store).await));
        rewrap_targets(&store, &rotated, &cache).await.unwrap();
        assert!(retirement_is_blocked(registry_min_done(&store).await));
        rewrap_notifications(&store, &rotated, &cache)
            .await
            .unwrap();
        assert!(retirement_is_blocked(registry_min_done(&store).await));
        rewrap_session_credentials(&store, &rotated).await.unwrap();
        assert!(retirement_is_blocked(registry_min_done(&store).await));
        rewrap_import_jobs(&store, &rotated).await.unwrap();

        let min_done = registry_min_done(&store).await;
        assert_eq!(min_done, 2);
        assert!(
            !retirement_is_blocked(min_done),
            "the old id becomes removable only after every registered stream completed"
        );

        let object = store
            .get_version(
                &bucket,
                &ObjectKey::parse("object").unwrap(),
                &VersionId::null(),
            )
            .await
            .unwrap()
            .unwrap();
        let object_descriptor: SseDescriptor =
            serde_json::from_str(object.sse_descriptor.as_deref().unwrap()).unwrap();
        assert!(
            !rotated.needs_rewrap(
                &B64.decode(object_descriptor.wrapped_dek_b64.as_bytes())
                    .unwrap()
            )
        );
        let user = store
            .user_by_sigv4_key("owner-sigv4")
            .await
            .unwrap()
            .unwrap();
        assert!(!rotated.needs_rewrap(&user.secret_ciphertext));
        let target_doc = store
            .get_bucket_config(&bucket, ConfigAspect::ReplicationTargets)
            .await
            .unwrap()
            .unwrap();
        let targets = cairn_replication::parse_targets(target_doc.0.as_bytes()).unwrap();
        assert!(!rotated.needs_rewrap(&targets[0].secret_ciphertext));
        let notification_doc = store
            .get_bucket_config(&bucket, ConfigAspect::Notification)
            .await
            .unwrap()
            .unwrap();
        let notification: NotificationConfig = serde_json::from_str(&notification_doc.0).unwrap();
        let WebhookSecret::Sealed(notification_secret) =
            notification.endpoints[0].secret.as_ref().unwrap()
        else {
            panic!("notification secret must stay sealed");
        };
        assert!(!rotated.needs_rewrap(&notification_secret.ciphertext));
        let session = store
            .user_by_session_key("session-access")
            .await
            .unwrap()
            .unwrap();
        assert!(!rotated.needs_rewrap(&session.secret_ciphertext));
        let import = store
            .get_import_job_record("import-job")
            .await
            .unwrap()
            .unwrap();
        assert!(!rotated.needs_rewrap(&import.secret_ciphertext));
    }
}
