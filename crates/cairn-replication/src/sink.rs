//! [`HttpS3Sink`] — a production [`ReplicationSink`] that ships objects to a remote
//! S3-compatible endpoint over HTTP(S) with SigV4-signed requests (ARCH §20.2).
//!
//! The engine drives this sink exactly like the test double: [`put_object`](HttpS3Sink::put_object)
//! `PUT`s the replicated body to `{endpoint}/{dest_bucket}/{key}`, carrying the content type,
//! the user metadata as `x-amz-meta-*` headers, and the `x-amz-meta-cairn-replica: true`
//! loop-prevention marker (so a destination that itself replicates back to us recognizes the
//! object as a replica and never re-ships it). [`delete_marker`](HttpS3Sink::delete_marker)
//! `DELETE`s the same path.
//!
//! Every request is signed with SigV4 using `cairn_auth`'s signing primitives
//! (`signing_key`/`canonical_request`/`string_to_sign`/`compute_signature`), with
//! `x-amz-content-sha256` set to the SHA-256 of the (buffered) payload. The body is read fully
//! into memory before signing because a signed-payload PUT must hash the bytes; a streaming
//! `UNSIGNED-PAYLOAD` variant is a future extension.
//!
//! ## Error classification
//! Network failures and `5xx` responses are [`ReplicationError::Retryable`]; `4xx` responses
//! are [`ReplicationError::Terminal`] except `408 Request Timeout` and `429 Too Many Requests`,
//! which are transient and therefore retryable. The engine's outbox machinery turns a retryable
//! failure into a backed-off re-attempt and a terminal one into operator-visible failure.
//!
//! ## Transport
//! The sink uses a `hyper-util` legacy client over a plain-HTTP connector, so `http://`
//! endpoints work out of the box. `https://` requires a TLS connector (`hyper-rustls` or a
//! hand-rolled `tokio-rustls` connector); that dependency is **not** declared in the workspace,
//! so an `https://` endpoint is rejected at construction with a clear error rather than failing
//! opaquely at connect time. See the crate README / remediation note.

use cairn_auth::{canonical_request, compute_signature, sha256_hex, signing_key, string_to_sign};
use cairn_types::error::ReplicationError;
use cairn_types::id::{ObjectKey, VersionId};
use cairn_types::replication::ReplicatedObject;
use cairn_types::time::Timestamp;
use cairn_types::traits::{Clock, ReplicationSink};
use futures_util::StreamExt;
use http::{Method, Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use std::sync::Arc;

/// The S3 service name in the SigV4 credential scope.
const SERVICE: &str = "s3";
/// The loop-prevention metadata key (sent as `x-amz-meta-cairn-replica`).
const REPLICA_MARKER_KEY: &str = "cairn-replica";

/// Connection parameters for a remote S3-compatible replication destination.
#[derive(Debug, Clone)]
pub struct S3SinkConfig {
    /// The endpoint base URL, e.g. `http://s3.us-east-1.example.com:9000`. Path-style addressing
    /// is used: requests target `{endpoint}/{dest_bucket}/{key}`.
    pub endpoint: String,
    /// The destination bucket name (path-style).
    pub dest_bucket: String,
    /// The SigV4 signing region.
    pub region: String,
    /// The destination access-key id.
    pub access_key_id: String,
    /// The destination secret access key.
    pub secret_access_key: String,
}

/// A production [`ReplicationSink`] issuing SigV4-signed S3 requests to a remote endpoint.
pub struct HttpS3Sink {
    config: S3SinkConfig,
    /// The scheme/authority of the endpoint, parsed once at construction (e.g. `s3.example.com:9000`).
    authority: String,
    /// The HTTP client. Plain HTTP only; see the module note on HTTPS.
    client: Client<HttpConnector, Full<bytes::Bytes>>,
    /// The clock supplying the SigV4 request time; injected so signing is deterministic in tests.
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for HttpS3Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpS3Sink")
            .field("endpoint", &self.config.endpoint)
            .field("dest_bucket", &self.config.dest_bucket)
            .field("region", &self.config.region)
            .field("access_key_id", &self.config.access_key_id)
            .finish_non_exhaustive()
    }
}

impl HttpS3Sink {
    /// Construct a sink from the destination connection parameters, using the operating-system
    /// wall clock for request signing.
    ///
    /// # Errors
    /// Returns [`ReplicationError::Terminal`] if the endpoint URL is malformed, or if it uses
    /// `https://` (which needs a TLS connector not available in this build — see the module
    /// note); a misconfiguration is a permanent, operator-actionable problem, not a transient
    /// one.
    pub fn new(config: S3SinkConfig) -> Result<Self, ReplicationError> {
        Self::with_clock(config, Arc::new(SystemClock))
    }

    /// Construct a sink with an injected [`Clock`]. Tests pass a fixed clock so the signed
    /// request is byte-for-byte deterministic.
    ///
    /// # Errors
    /// As [`HttpS3Sink::new`].
    pub fn with_clock(
        config: S3SinkConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ReplicationError> {
        let uri: Uri = config
            .endpoint
            .parse()
            .map_err(|e| ReplicationError::Terminal(format!("invalid endpoint URL: {e}")))?;
        match uri.scheme_str() {
            Some("http") => {}
            Some("https") => {
                return Err(ReplicationError::Terminal(
                    "https replication endpoints require a TLS connector not built into this \
                     crate (hyper-rustls is not a declared workspace dependency); use an http:// \
                     endpoint or add the connector"
                        .to_owned(),
                ));
            }
            other => {
                return Err(ReplicationError::Terminal(format!(
                    "unsupported endpoint scheme: {other:?}"
                )));
            }
        }
        let authority = uri
            .authority()
            .map(ToString::to_string)
            .ok_or_else(|| ReplicationError::Terminal("endpoint URL has no host".to_owned()))?;

        let client = Client::builder(TokioExecutor::new()).build(HttpConnector::new());

        Ok(Self {
            config,
            authority,
            client,
            clock,
        })
    }

    /// Build the canonical, percent-encoded request path `/{dest_bucket}/{key}` (S3 path-style).
    fn request_path(&self, key: &str) -> String {
        let mut path = String::from("/");
        path.push_str(&uri_encode_path(&self.config.dest_bucket));
        path.push('/');
        path.push_str(&uri_encode_path(key));
        path
    }

    /// Sign and send one request, classifying the outcome into the sink error taxonomy.
    async fn send_signed(
        &self,
        method: &Method,
        key: &str,
        body: bytes::Bytes,
        content_type: Option<&str>,
        user_headers: &[(String, String)],
    ) -> Result<(), ReplicationError> {
        let now = self.clock.now();
        let amz_date = format_amz_datetime(now);
        let scope_date = &amz_date[..8];
        let payload_hash = sha256_hex(&body);
        let path = self.request_path(key);

        // Assemble the headers that participate in (and accompany) the request. `host`,
        // `x-amz-content-sha256`, and `x-amz-date` are always signed; the content type and any
        // user-metadata headers join them. Names are lowercased for canonicalization.
        let mut signed: Vec<(String, String)> = vec![
            ("host".to_owned(), self.authority.clone()),
            ("x-amz-content-sha256".to_owned(), payload_hash.clone()),
            ("x-amz-date".to_owned(), amz_date.clone()),
        ];
        if let Some(ct) = content_type {
            signed.push(("content-type".to_owned(), ct.to_owned()));
        }
        for (name, value) in user_headers {
            signed.push((name.to_ascii_lowercase(), value.clone()));
        }
        signed.sort_by(|a, b| a.0.cmp(&b.0));

        let signed_names = signed
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(";");

        let canonical = canonical_request(
            method.as_str(),
            &path,
            "",
            &signed,
            &signed_names,
            &payload_hash,
        );
        let scope = format!("{scope_date}/{}/{SERVICE}/aws4_request", self.config.region);
        let sts = string_to_sign(&amz_date, &scope, &canonical);
        let key_bytes = signing_key(
            &self.config.secret_access_key,
            scope_date,
            &self.config.region,
            SERVICE,
        );
        let signature = compute_signature(&key_bytes, &sts);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_names}, \
             Signature={signature}",
            self.config.access_key_id
        );

        // Build the wire request. The endpoint authority is reused; only the path varies.
        let uri = format!("http://{}{path}", self.authority);
        let mut builder = Request::builder()
            .method(method.clone())
            .uri(&uri)
            .header(http::header::HOST, &self.authority)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &payload_hash)
            .header(http::header::AUTHORIZATION, &authorization)
            .header(http::header::CONTENT_LENGTH, body.len());
        if let Some(ct) = content_type {
            builder = builder.header(http::header::CONTENT_TYPE, ct);
        }
        for (name, value) in user_headers {
            builder = builder.header(name.as_str(), value);
        }
        let request = builder
            .body(Full::new(body))
            .map_err(|e| ReplicationError::Terminal(format!("failed to build request: {e}")))?;

        // A connection/transport failure is transient: classify retryable.
        let response = self
            .client
            .request(request)
            .await
            .map_err(|e| ReplicationError::Retryable(format!("transport error: {e}")))?;

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        // Drain the (bounded, error) body so the message can quote it, ignoring read errors.
        let detail = response
            .into_body()
            .collect()
            .await
            .map(|b| String::from_utf8_lossy(&b.to_bytes()).into_owned())
            .unwrap_or_default();
        Err(classify_status(status.as_u16(), &detail))
    }
}

/// Classify a non-2xx HTTP status into the sink error taxonomy: 5xx and the transient 408/429
/// are retryable; every other 4xx is terminal.
fn classify_status(code: u16, detail: &str) -> ReplicationError {
    let msg = if detail.trim().is_empty() {
        format!("destination returned HTTP {code}")
    } else {
        format!("destination returned HTTP {code}: {}", detail.trim())
    };
    if code >= 500 || code == 408 || code == 429 {
        ReplicationError::Retryable(msg)
    } else {
        ReplicationError::Terminal(msg)
    }
}

#[async_trait::async_trait]
impl ReplicationSink for HttpS3Sink {
    async fn put_object(&self, object: ReplicatedObject) -> Result<(), ReplicationError> {
        // Buffer the logical body so the payload can be hashed for the signed-payload PUT.
        let body = collect_body(object.body).await?;

        // User metadata becomes `x-amz-meta-*`, plus the loop-prevention marker. The marker is
        // appended unconditionally so a destination that mirrors back recognizes the replica.
        let mut user_headers: Vec<(String, String)> =
            Vec::with_capacity(object.user_metadata.len() + 1);
        for (k, v) in &object.user_metadata {
            user_headers.push((format!("x-amz-meta-{}", k.to_ascii_lowercase()), v.clone()));
        }
        user_headers.push((
            format!("x-amz-meta-{REPLICA_MARKER_KEY}"),
            "true".to_owned(),
        ));

        self.send_signed(
            &Method::PUT,
            object.key.as_str(),
            body,
            Some(&object.content_type),
            &user_headers,
        )
        .await
    }

    async fn delete_marker(
        &self,
        key: &ObjectKey,
        _version: &VersionId,
    ) -> Result<(), ReplicationError> {
        self.send_signed(
            &Method::DELETE,
            key.as_str(),
            bytes::Bytes::new(),
            None,
            &[],
        )
        .await
    }
}

/// Read a logical-byte blob stream fully into a contiguous buffer. A read error mid-stream is
/// transient (the source blob may be momentarily unavailable), so it is retryable.
async fn collect_body(
    mut stream: cairn_types::BlobStream,
) -> Result<bytes::Bytes, ReplicationError> {
    let mut buf = bytes::BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|e| ReplicationError::Retryable(format!("reading source body: {e}")))?;
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}

/// Percent-encode a path segment per SigV4 canonical-URI rules: unreserved characters and `/`
/// pass through; everything else is `%`-escaped in uppercase hex. The destination bucket and key
/// are joined into the path by the caller, so `/` is preserved here.
fn uri_encode_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(hex_upper(b >> 4));
                out.push(hex_upper(b & 0x0f));
            }
        }
    }
    out
}

/// One uppercase hex digit for a nibble in `0..=15`.
fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

/// Format a [`Timestamp`] as the SigV4 `x-amz-date` basic-ISO instant `YYYYMMDDTHHMMSSZ` (UTC).
fn format_amz_datetime(ts: Timestamp) -> String {
    let secs = ts.as_secs();
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = tod / 3600;
    let minute = (tod % 3600) / 60;
    let second = tod % 60;
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

/// Civil date `(year, month, day)` from days since the Unix epoch, by Howard Hinnant's
/// `civil_from_days` algorithm (the inverse of the lifecycle parser's `days_from_civil`).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The production [`Clock`] backed by the operating-system wall clock. Mirrors
/// `cairn_crypto::SystemClock`, replicated here so the sink has a default time source without
/// depending on a sibling crate outside the trait spine.
#[derive(Debug, Clone, Copy, Default)]
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        use std::time::{SystemTime, UNIX_EPOCH};
        let millis = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
            Err(e) => i64::try_from(e.duration().as_millis())
                .map(|m| -m)
                .unwrap_or(i64::MIN),
        };
        Timestamp(millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_amz_datetime_against_known_instant() {
        // 2015-08-30T12:36:00Z — the AWS get-vanilla vector's instant.
        let ts = Timestamp::from_secs(1_440_938_160);
        assert_eq!(format_amz_datetime(ts), "20150830T123600Z");
    }

    #[test]
    fn formats_epoch() {
        assert_eq!(
            format_amz_datetime(Timestamp::from_secs(0)),
            "19700101T000000Z"
        );
    }

    #[test]
    fn uri_encodes_reserved_characters() {
        assert_eq!(uri_encode_path("a/b c"), "a/b%20c");
        assert_eq!(uri_encode_path("plain-key_1.txt"), "plain-key_1.txt");
        assert_eq!(uri_encode_path("p+q"), "p%2Bq");
    }

    #[test]
    fn classify_status_partitions_retryable_and_terminal() {
        assert!(matches!(
            classify_status(500, ""),
            ReplicationError::Retryable(_)
        ));
        assert!(matches!(
            classify_status(503, "slow down"),
            ReplicationError::Retryable(_)
        ));
        assert!(matches!(
            classify_status(429, ""),
            ReplicationError::Retryable(_)
        ));
        assert!(matches!(
            classify_status(408, ""),
            ReplicationError::Retryable(_)
        ));
        assert!(matches!(
            classify_status(403, "AccessDenied"),
            ReplicationError::Terminal(_)
        ));
        assert!(matches!(
            classify_status(404, ""),
            ReplicationError::Terminal(_)
        ));
        assert!(matches!(
            classify_status(400, ""),
            ReplicationError::Terminal(_)
        ));
    }

    #[test]
    fn rejects_https_endpoint_with_terminal_error() {
        let cfg = S3SinkConfig {
            endpoint: "https://s3.example.com".to_owned(),
            dest_bucket: "dest".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKID".to_owned(),
            secret_access_key: "secret".to_owned(),
        };
        let err = HttpS3Sink::new(cfg).unwrap_err();
        assert!(matches!(err, ReplicationError::Terminal(_)));
    }

    #[test]
    fn rejects_malformed_endpoint() {
        let cfg = S3SinkConfig {
            endpoint: "not a url".to_owned(),
            dest_bucket: "dest".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKID".to_owned(),
            secret_access_key: "secret".to_owned(),
        };
        assert!(HttpS3Sink::new(cfg).is_err());
    }
}
