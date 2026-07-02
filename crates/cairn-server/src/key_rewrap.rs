//! Background master-key re-wrap worker (audit #29, Phase D): re-seals stored secrets that are
//! sealed under a non-active ring key — or are legacy (pre-#29, no-magic) blobs — onto the active
//! key, so an old master key can eventually be retired. SQLite backend only (one worker per
//! shard). Resumable via the `rewrap_progress` table; idempotent — a blob already sealed under the
//! active key is skipped by a cheap byte check, never decrypted. A re-seal that cannot open
//! (e.g. its key was removed) is logged and skipped; data is never deleted or corrupted.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use cairn_crypto::SystemCrypto;
use cairn_meta::{CachedMetadataStore, SqliteMetadataStore};
use cairn_types::bucket::ConfigAspect;
use cairn_types::crypto::Nonce;
use cairn_types::error::MetaError;
use cairn_types::traits::{Crypto, MetadataStore};
use std::sync::Arc;
use std::time::Duration;

/// Rows re-wrapped per page before persisting the cursor (resumability granularity).
const BATCH: u32 = 500;
const SSE_STREAM: &str = "object_versions.sse_descriptor";
const USER_STREAM: &str = "users.sigv4_secret";
const TARGETS_STREAM: &str = "bucket_config.replication_targets";

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
pub fn spawn(
    store: Arc<SqliteMetadataStore>,
    crypto: Arc<SystemCrypto>,
    cache: Arc<CachedMetadataStore>,
    interval_secs: u64,
) {
    if interval_secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let interval = Duration::from_secs(interval_secs);
        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = run_once(&store, &crypto, &cache).await {
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

async fn run_once(
    store: &SqliteMetadataStore,
    crypto: &SystemCrypto,
    cache: &CachedMetadataStore,
) -> Result<(), MetaError> {
    rewrap_sse(store, crypto).await?;
    rewrap_users(store, crypto).await?;
    rewrap_targets(store, crypto, cache).await?;
    Ok(())
}

async fn rewrap_sse(store: &SqliteMetadataStore, crypto: &SystemCrypto) -> Result<(), MetaError> {
    let active = crypto.active_key_id();
    let mut cursor = store.rewrap_cursor(SSE_STREAM.to_owned()).await?;
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
            .rewrap_set_progress(SSE_STREAM.to_owned(), last.clone(), done, failed, now_ms())
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
        .rewrap_finish_pass(SSE_STREAM.to_owned(), done_id, now_ms())
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
    let active = crypto.active_key_id();
    let mut cursor = store.rewrap_cursor(USER_STREAM.to_owned()).await?;
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
            .rewrap_set_progress(USER_STREAM.to_owned(), last.clone(), done, failed, now_ms())
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
        .rewrap_finish_pass(USER_STREAM.to_owned(), done_id, now_ms())
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
    let done_id = targets_pass_done_id(active, pass_failed);
    store
        .rewrap_finish_pass(TARGETS_STREAM.to_owned(), done_id, now_ms())
        .await
}

/// The `done_active_id` a targets re-wrap pass records: the active id only if the pass re-sealed
/// every bucket with zero failures and zero CAS misses (`pass_failed == 0`); otherwise 0, so the
/// retire-gate stays closed until a genuinely clean pass (audit #29 / 2026-07).
fn targets_pass_done_id(active: u16, pass_failed: u64) -> u16 {
    if pass_failed == 0 { active } else { 0 }
}

#[cfg(test)]
mod tests {
    use super::targets_pass_done_id;

    #[test]
    fn incomplete_targets_pass_does_not_record_completion() {
        // A clean pass records the active id.
        assert_eq!(targets_pass_done_id(3, 0), 3);
        // Any failure OR CAS miss (both feed pass_failed) records 0, keeping the retire-gate closed
        // (audit 2026-07: pre-fix a CAS miss still recorded `active`, so a key still sealing a
        // target secret could be retired).
        assert_eq!(targets_pass_done_id(3, 1), 0);
        assert_eq!(targets_pass_done_id(3, 5), 0);
    }
}
