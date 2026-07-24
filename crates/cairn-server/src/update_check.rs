//! Periodic release-update check (ARCH 28, `CAIRN_UPDATE_CHECK_*`).
//!
//! Cairn is always self-hosted, so an operator only learns a new release shipped if the node tells
//! them. When enabled (the default), a background loop fetches the configured release feed (GitHub
//! Releases by default) through the SSRF-guarded outbound connector — the same one replication and
//! webhooks dial through, so a hijacked feed URL can never reach an internal address — and publishes
//! the result on `GET /system` for the console. It is strictly best-effort: any failure (air-gapped,
//! feed down, malformed JSON) leaves the last-known status and logs at debug, never an error, and
//! never blocks a request.

use bytes::Bytes;
use cairn_control::UpdateStatus;
use http_body_util::{BodyExt, Empty};
use hyper::{Request, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Cap on the feed body we will parse, so a hostile/huge response can't balloon memory.
const MAX_FEED_BYTES: usize = 1_000_000;

/// A newer release is available when this is a *release* build (not `-dev`) and the feed's latest
/// tag differs from the running version. The release workflow threads the identical calendar version
/// into both the binary and the git tag, and the feed's "latest" is authoritative for newest — so a
/// plain tag inequality is correct even when several releases share a calendar date (their git tags
/// still differ). A `-dev` build never reports an update.
pub fn compute_update_available(current_version: &str, latest_tag: &str) -> bool {
    !current_version.contains("-dev") && !latest_tag.is_empty() && latest_tag != current_version
}

/// Fetch the latest release `(tag, html_url)` from `url` through the SSRF-guarded connector. Returns
/// `None` on any error (unreachable, non-2xx, oversized, malformed) — the caller keeps prior state.
async fn fetch_latest_release(
    url: &str,
    allow_internal: bool,
    timeout: Duration,
) -> Option<(String, Option<String>)> {
    let uri: Uri = url.parse().ok()?;
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .wrap_connector(cairn_net::guarded_http_connector(
            cairn_net::GuardConfig::new(allow_internal),
        ));
    let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new()).build(https);
    // GitHub's API rejects requests without a User-Agent; the Accept pins the stable media type.
    let req = Request::builder()
        .uri(uri)
        .header("user-agent", "cairn-update-check")
        .header("accept", "application/vnd.github+json")
        .body(Empty::<Bytes>::new())
        .ok()?;
    let resp = tokio::time::timeout(timeout, client.request(req))
        .await
        .ok()?
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let collected = tokio::time::timeout(timeout, resp.into_body().collect())
        .await
        .ok()?
        .ok()?;
    let bytes = collected.to_bytes();
    if bytes.len() > MAX_FEED_BYTES {
        return None;
    }
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let tag = v.get("tag_name")?.as_str()?.trim().to_owned();
    if tag.is_empty() {
        return None;
    }
    let html_url = v
        .get("html_url")
        .and_then(|u| u.as_str())
        .map(|s| s.to_owned());
    Some((tag, html_url))
}

/// Run one check and publish the result into `status`. Best-effort: any failure leaves prior state.
pub async fn run_once(
    url: &str,
    current_version: &str,
    allow_internal: bool,
    timeout: Duration,
    status: &Arc<RwLock<UpdateStatus>>,
) {
    match fetch_latest_release(url, allow_internal, timeout).await {
        Some((tag, html_url)) => {
            let available = compute_update_available(current_version, &tag);
            if let Ok(mut s) = status.write() {
                s.latest_version = Some(tag);
                s.update_available = available;
                s.release_url = html_url;
            }
            tracing::debug!(
                update_available = available,
                "release update check complete"
            );
        }
        None => {
            tracing::debug!("release update check: feed unavailable; keeping prior status");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::compute_update_available;

    #[test]
    fn dev_build_never_reports_update() {
        assert!(!compute_update_available(
            "0.1.0-dev+g1a2b3c",
            "v2026.07.25"
        ));
    }

    #[test]
    fn same_version_is_up_to_date() {
        assert!(!compute_update_available("v2026.07.24", "v2026.07.24"));
    }

    #[test]
    fn a_different_latest_tag_is_an_update() {
        assert!(compute_update_available("v2026.07.24", "v2026.07.25"));
        // A second release on the SAME calendar day carries a distinct git tag, so a tag inequality
        // still correctly flags it — the case the plain calendar version could not order on its own.
        assert!(compute_update_available("v2026.07.24", "v2026.07.24-2"));
    }

    #[test]
    fn an_empty_or_missing_feed_tag_is_not_an_update() {
        assert!(!compute_update_available("v2026.07.24", ""));
    }
}
