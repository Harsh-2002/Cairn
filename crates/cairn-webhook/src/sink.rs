//! The delivery sink: a `WebhookSink` POSTs a pre-rendered JSON event body to a webhook URL,
//! optionally carrying an HMAC-SHA256 `X-Cairn-Signature` header, and classifies the outcome as
//! success, retryable, or terminal so the engine knows whether to reschedule.

use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use std::time::Duration;

/// The default per-request delivery timeout. Bounds a single POST so a hung endpoint cannot stall
/// the worker; on timeout the delivery is retryable (rescheduled with backoff).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

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

/// An HTTP(S) webhook sink built on the same hyper/rustls client stack as the replication sink.
#[derive(Debug)]
pub struct HttpWebhookSink {
    client: Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>,
    timeout: Duration,
}

impl Default for HttpWebhookSink {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpWebhookSink {
    /// Construct a sink with a connector that dials plaintext for `http://` and negotiates rustls
    /// (webpki roots) for `https://`, speaking HTTP/1.1, with the default per-request timeout.
    #[must_use]
    pub fn new() -> Self {
        Self::with_timeout(DEFAULT_TIMEOUT)
    }

    /// As [`new`](Self::new) but with an explicit per-request timeout.
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
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

        // Bound the whole request: a hung endpoint must not pin this future (and, with bounded
        // engine concurrency, stall the outbox). A timeout is retryable.
        let resp = match tokio::time::timeout(self.timeout, self.client.request(req)).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => {
                return Err(WebhookError::Retryable(format!("connection failed: {e}")));
            }
            Err(_) => {
                return Err(WebhookError::Retryable(format!(
                    "delivery timed out after {}s",
                    self.timeout.as_secs()
                )));
            }
        };
        let status = resp.status();
        // Drain the body so the connection can be reused; the content is irrelevant to us.
        let _ = resp.into_body().collect().await;

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
