//! Background master-key re-wrap worker (audit #29, Phase D): re-seals stored secrets that are
//! sealed under a non-active ring key — or are legacy (pre-#29, no-magic) blobs — onto the active
//! key, so an old master key can eventually be retired. SQLite backend only (one worker per
//! shard). Resumable via the `rewrap_progress` table; idempotent — a blob already sealed under the
//! active key is skipped by a cheap byte check, never decrypted. A re-seal that cannot open
//! (e.g. its key was removed) is logged and skipped; data is never deleted or corrupted.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use cairn_crypto::SystemCrypto;
use cairn_meta::SqliteMetadataStore;
use cairn_types::bucket::{ConfigAspect, ConfigDoc};
use cairn_types::crypto::Nonce;
use cairn_types::error::MetaError;
use cairn_types::meta::Mutation;
use cairn_types::traits::{Crypto, MetadataStore};
use std::sync::Arc;
use std::time::Duration;

/// Rows re-wrapped per page before persisting the cursor (resumability granularity).
const BATCH: u32 = 500;
const SSE_STREAM: &str = "object_versions.sse_descriptor";
const USER_STREAM: &str = "users.sigv4_secret";

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// The stored SSE descriptor JSON shape (mirrors `cairn-protocol`'s private struct).
#[derive(serde::Serialize, serde::Deserialize)]
struct SseDesc {
    alg: String,
    wrapped_dek_b64: String,
    #[serde(default)]
    nonce_b64: String,
}

/// Spawn the per-shard re-wrap loop. `interval_secs == 0` disables it. The worker shares one
/// `Arc<SystemCrypto>` (the ring) with the rest of the stack.
pub fn spawn(store: Arc<SqliteMetadataStore>, crypto: Arc<SystemCrypto>, interval_secs: u64) {
    if interval_secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let interval = Duration::from_secs(interval_secs);
        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = run_once(&store, &crypto).await {
                tracing::warn!(error = %e, "master-key re-wrap pass failed");
            }
        }
    });
}

/// Spawn the per-shard active-key seal-count flush loop (audit #29, Phase E). Persists the
/// in-process counter to `key_ring_state` so the seal-count bound survives a restart.
/// `interval_secs == 0` disables it.
pub fn spawn_counter_sync(
    store: Arc<SqliteMetadataStore>,
    crypto: Arc<SystemCrypto>,
    interval_secs: u64,
) {
    if interval_secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let interval = Duration::from_secs(interval_secs);
        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = store
                .key_ring_sync_seal_count(crypto.active_key_id(), crypto.seal_count())
                .await
            {
                tracing::warn!(error = %e, "active-key seal-count flush failed");
            }
        }
    });
}

async fn run_once(store: &SqliteMetadataStore, crypto: &SystemCrypto) -> Result<(), MetaError> {
    rewrap_sse(store, crypto).await?;
    rewrap_users(store, crypto).await?;
    rewrap_targets(store, crypto).await?;
    Ok(())
}

async fn rewrap_sse(store: &SqliteMetadataStore, crypto: &SystemCrypto) -> Result<(), MetaError> {
    let mut cursor = store.rewrap_cursor(SSE_STREAM.to_owned()).await?;
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
                Ok(Some(new_desc)) => match store.rewrap_set_sse(pk.clone(), new_desc).await {
                    Ok(()) => done += 1,
                    Err(_) => failed += 1,
                },
                Ok(None) => {} // already active key — skip
                Err(()) => {
                    failed += 1;
                    tracing::warn!(version = %pk, "SSE re-wrap could not open the DEK; skipping");
                }
            }
        }
        store
            .rewrap_set_progress(SSE_STREAM.to_owned(), last.clone(), done, failed, now_ms())
            .await?;
        cursor = last;
        if n < BATCH as usize {
            break;
        }
    }
    // Pass complete: clear the cursor so a future rotation re-scans from the start.
    store
        .rewrap_set_progress(SSE_STREAM.to_owned(), None, 0, 0, now_ms())
        .await
}

/// Re-wrap one SSE descriptor's DEK onto the active key. `Ok(None)` if already active.
fn rewrap_sse_descriptor(crypto: &SystemCrypto, json: &str) -> Result<Option<String>, ()> {
    let d: SseDesc = serde_json::from_str(json).map_err(|_| ())?;
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
    let new = SseDesc {
        alg: d.alg,
        wrapped_dek_b64: B64.encode(&resealed.ciphertext),
        nonce_b64: String::new(),
    };
    serde_json::to_string(&new).map(Some).map_err(|_| ())
}

async fn rewrap_users(store: &SqliteMetadataStore, crypto: &SystemCrypto) -> Result<(), MetaError> {
    let mut cursor = store.rewrap_cursor(USER_STREAM.to_owned()).await?;
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
                Ok(resealed) => {
                    match store
                        .rewrap_set_user_sigv4(id.clone(), resealed.ciphertext)
                        .await
                    {
                        Ok(()) => done += 1,
                        Err(_) => failed += 1,
                    }
                }
                Err(_) => {
                    failed += 1;
                    tracing::warn!(user = %id, "SigV4 secret re-wrap could not open; skipping");
                }
            }
        }
        store
            .rewrap_set_progress(USER_STREAM.to_owned(), last.clone(), done, failed, now_ms())
            .await?;
        cursor = last;
        if n < BATCH as usize {
            break;
        }
    }
    store
        .rewrap_set_progress(USER_STREAM.to_owned(), None, 0, 0, now_ms())
        .await
}

async fn rewrap_targets(
    store: &SqliteMetadataStore,
    crypto: &SystemCrypto,
) -> Result<(), MetaError> {
    for b in store.list_buckets(None).await? {
        let Some(doc) = store
            .get_bucket_config(&b.name, ConfigAspect::ReplicationTargets)
            .await?
        else {
            continue;
        };
        let Ok(mut targets) = cairn_replication::parse_targets(doc.0.as_bytes()) else {
            continue;
        };
        let mut changed = false;
        for t in &mut targets {
            if !crypto.needs_rewrap(&t.secret_ciphertext) {
                continue;
            }
            if let Ok(secret) = crypto.open(&t.secret_ciphertext, &Nonce(t.nonce.clone())) {
                if let Ok(resealed) = crypto.seal(&secret) {
                    t.secret_ciphertext = resealed.ciphertext;
                    t.nonce = Vec::new();
                    changed = true;
                }
            }
        }
        if changed {
            store
                .submit(Mutation::SetBucketConfig {
                    bucket: b.name.clone(),
                    aspect: ConfigAspect::ReplicationTargets,
                    doc: Some(ConfigDoc(cairn_replication::serialize_targets(&targets))),
                })
                .await?;
        }
    }
    Ok(())
}
