//! The delivery sink: a `WebhookSink` POSTs a pre-rendered JSON event body to a webhook URL,
//! optionally carrying an HMAC-SHA256 `X-Cairn-Signature` header, and classifies the outcome as
//! success, retryable, or terminal so the engine knows whether to reschedule.

use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, Request, Uri};
use http_body_util::{BodyExt, Full, Limited};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use std::time::Duration;

/// The default per-request delivery timeout. Bounds a single POST so a hung endpoint cannot stall
/// the worker; on timeout the delivery is retryable (rescheduled with backoff).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The most of a webhook response body we will read before dropping the connection. The content is
/// irrelevant to us (we only need the status), so this only exists to bound memory — and, together
/// with the timeout now covering the drain, to stop a receiver that sends `200` headers then
/// trickles/never-ends the body from pinning the delivery future and wedging the whole outbox tick
/// (audit 2026-07). Mirrors `cairn-server`'s `MAX_API_BODY` posture: a fixed, generous cap.
const MAX_RESPONSE_BODY: usize = 64 * 1024;

/// Why a webhook delivery did not succeed.
#[derive(Debug, Clone)]
pub enum WebhookError {
    /// A transient failure (network error, 408/429, or 5xx): retry after backoff.
    Retryable(String),
    /// A permanent failure (a 4xx other than 408/429, or a malformed endpoint URL): give up.
    Terminal(String),
}

impl std::fmt::Display for WebhookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebhookError::Retryable(m) => write!(f, "retryable: {m}"),
            WebhookError::Terminal(m) => write!(f, "terminal: {m}"),
        }
    }
}

/// The delivery transport. Abstracted so tests can substitute a recording sink.
#[async_trait]
pub trait WebhookSink: Send + Sync {
    /// POST `body` to `url`, attaching `X-Cairn-Signature: sha256=<hex>` when `signature` is set.
    async fn deliver(
        &self,
        url: &str,
        body: &[u8],
        signature: Option<&str>,
    ) -> Result<(), WebhookError>;
}

/// An HTTP(S) webhook sink built on the same hyper/rustls client stack as the replication sink,
/// dialing through the SSRF-guarded resolver.
#[derive(Debug)]
pub struct HttpWebhookSink {
    client: Client<
        hyper_rustls::HttpsConnector<HttpConnector<cairn_net::GuardedResolver>>,
        Full<Bytes>,
    >,
    timeout: Duration,
}

impl Default for HttpWebhookSink {
    fn default() -> Self {
        Self::new(cairn_net::GuardConfig::default())
    }
}

impl HttpWebhookSink {
    /// Construct a sink with a connector that dials plaintext for `http://` and negotiates rustls
    /// (webpki roots) for `https://`, speaking HTTP/1.1, with the default per-request timeout.
    /// `guard` refuses a webhook endpoint that resolves to an internal address (ARCH 27).
    #[must_use]
    pub fn new(guard: cairn_net::GuardConfig) -> Self {
        Self::with_timeout(DEFAULT_TIMEOUT, guard)
    }

    /// As [`new`](Self::new) but with an explicit per-request timeout.
    #[must_use]
    pub fn with_timeout(timeout: Duration, guard: cairn_net::GuardConfig) -> Self {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .wrap_connector(cairn_net::guarded_http_connector(guard));
        let client = Client::builder(TokioExecutor::new()).build(https);
        Self { client, timeout }
    }
}

#[async_trait]
impl WebhookSink for HttpWebhookSink {
    async fn deliver(
        &self,
        url: &str,
        body: &[u8],
        signature: Option<&str>,
    ) -> Result<(), WebhookError> {
        let uri: Uri = url
            .parse()
            .map_err(|e| WebhookError::Terminal(format!("invalid webhook URL: {e}")))?;
        match uri.scheme_str() {
            Some("http" | "https") => {}
            other => {
                return Err(WebhookError::Terminal(format!(
                    "unsupported webhook scheme: {other:?}"
                )));
            }
        }
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-type", "application/json")
            .header("user-agent", "cairn-webhook/1");
        if let Some(sig) = signature {
            builder = builder.header("x-cairn-signature", format!("sha256={sig}"));
        }
        let req = builder
            .body(Full::new(Bytes::copy_from_slice(body)))
            .map_err(|e| WebhookError::Terminal(format!("request build failed: {e}")))?;

        // Bound the WHOLE delivery — sending the request, receiving the head, AND draining the body
        // — under one timeout so a hung endpoint cannot pin this future and (with bounded engine
        // concurrency) stall the outbox. The body drain must be inside the timeout: a receiver that
        // returns `200` headers then trickles or never-ends the body would otherwise hang forever
        // (audit 2026-07). The drain is also byte-capped so a large finite body can't OOM us.
        let deliver = async {
            let resp = self
                .client
                .request(req)
                .await
                .map_err(|e| WebhookError::Retryable(format!("connection failed: {e}")))?;
            let status = resp.status();
            // Drain (capped) so the connection can be reused; the content is irrelevant to us, and
            // an over-cap or errored drain is fine — we already have the status.
            let _ = Limited::new(resp.into_body(), MAX_RESPONSE_BODY)
                .collect()
                .await;
            Ok::<http::StatusCode, WebhookError>(status)
        };
        let status = match tokio::time::timeout(self.timeout, deliver).await {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(WebhookError::Retryable(format!(
                    "delivery timed out after {}s",
                    self.timeout.as_secs()
                )));
            }
        };

        if status.is_success() {
            Ok(())
        } else if status.is_server_error()
            || status == http::StatusCode::REQUEST_TIMEOUT
            || status == http::StatusCode::TOO_MANY_REQUESTS
        {
            Err(WebhookError::Retryable(format!(
                "endpoint returned {status}"
            )))
        } else {
            Err(WebhookError::Terminal(format!(
                "endpoint returned {status}"
            )))
        }
    }
}

#[cfg(test)]
mod sink_timeout_tests {
    use super::{HttpWebhookSink, WebhookError, WebhookSink};
    use std::io::{Read, Write};
    use std::time::Duration;

    /// Regression (audit 2026-07): a receiver that returns `200` headers then stalls the response
    /// body must NOT pin the delivery future — the timeout has to cover the body drain, not just the
    /// request head. Pre-fix the drain ran outside the timeout and `deliver` hung forever (the outer
    /// 5s guard would fire and fail the `.expect` below); post-fix it returns Retryable within the
    /// sink's 500ms timeout.
    #[tokio::test]
    async fn stalled_response_body_times_out_instead_of_hanging() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // A blocking server thread: send a 200 with a large Content-Length, a few body bytes, then
        // hold the connection open without ever finishing the body.
        std::thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf);
                let _ = sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\nabc");
                let _ = sock.flush();
                std::thread::sleep(Duration::from_secs(10));
            }
        });

        // The stall server runs on loopback, so opt out of the SSRF guard for this test.
        let sink = HttpWebhookSink::with_timeout(
            Duration::from_millis(500),
            cairn_net::GuardConfig::new(true),
        );
        let url = format!("http://{addr}/");
        let outcome = tokio::time::timeout(Duration::from_secs(5), sink.deliver(&url, b"{}", None))
            .await
            .expect(
                "deliver must return within 5s — pre-fix it hangs on the stalled response body",
            );
        assert!(
            matches!(outcome, Err(WebhookError::Retryable(_))),
            "a stalled response body should time out as Retryable, got {outcome:?}"
        );
    }
}
