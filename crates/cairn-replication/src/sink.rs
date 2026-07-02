//! [`HttpS3Sink`] — a production [`ReplicationSink`] that ships objects to a remote
//! S3-compatible endpoint over HTTP(S) with SigV4-signed requests (ARCH 20.2).
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
//! The sink uses a `hyper-util` legacy client over a `hyper-rustls` [`HttpsConnector`] built with
//! `.https_or_http()`, so the **same** client serves both `http://` and `https://` endpoints:
//! plaintext endpoints connect directly, TLS endpoints negotiate rustls (aws-lc-rs provider).
//! There is no longer an https-rejection at construction (ARCH 20.2).
//!
//! ## TLS trust
//! Which root anchors a `https://` endpoint is verified against is per-target
//! ([`S3SinkConfig::ca_cert_path`] and [`S3SinkConfig::insecure_skip_verify`]):
//! * a custom **CA path** builds a [`RootCertStore`](rustls::RootCertStore) from that PEM bundle
//!   and trusts exactly those anchors — the way to replicate to a peer with a private CA;
//! * **`insecure_skip_verify`** installs a no-op [`ServerCertVerifier`] that accepts any
//!   certificate and logs a loud warning — for testing against a self-signed endpoint only;
//! * otherwise the built-in **webpki roots** are used, as before.
//!
//! `hyper-rustls` has no `.dangerous()` shortcut, so both non-default cases go through
//! [`HttpsConnectorBuilder::with_tls_config`](hyper_rustls::HttpsConnectorBuilder::with_tls_config)
//! with a hand-built [`ClientConfig`](rustls::ClientConfig).
//!
//! ## Per-source destination routing
//! A single sink can replicate many source buckets to many destination buckets. [`S3SinkConfig`]
//! carries a `source -> dest` [`HashMap`](std::collections::HashMap) plus a `dest_bucket` default;
//! [`HttpS3Sink::dest_for`] resolves the destination bucket for a given source bucket, falling
//! back to the default when the source has no explicit mapping. The [`BucketRoutedSink`] entry
//! points carry the source bucket so the destination is chosen per request. Constructing a sink
//! with an empty map and a single `dest_bucket` reproduces the original node->node behaviour.
//!
//! [`HttpsConnector`]: hyper_rustls::HttpsConnector
//! [`BucketRoutedSink`]: crate::BucketRoutedSink

use base64::Engine as _;
use cairn_auth::{canonical_request, compute_signature, sha256_hex, signing_key, string_to_sign};
use cairn_types::error::ReplicationError;
use cairn_types::id::{BucketName, ObjectKey, VersionId};
use cairn_types::replication::ReplicatedObject;
use cairn_types::time::Timestamp;
use cairn_types::traits::Clock;
use futures_util::StreamExt;
use http::{Method, Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::aws_lc_rs;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

/// The S3 service name in the SigV4 credential scope.
const SERVICE: &str = "s3";
/// The loop-prevention metadata key (sent as `x-amz-meta-cairn-replica`).
const REPLICA_MARKER_KEY: &str = "cairn-replica";
/// The source version-id metadata key (sent as `x-amz-meta-cairn-replica-version-id`). The
/// destination preserves this exact version id so a version has the same identity on every node and
/// re-delivery is an idempotent upsert (AWS S3 CRR semantics, ARCH 20.4).
const REPLICA_VERSION_ID_KEY: &str = "cairn-replica-version-id";
/// The source ACL metadata key (sent as `x-amz-meta-cairn-replica-acl`), carrying the object's ACL
/// as base64 of its JSON so the destination version reproduces the source's grants (ARCH 20.4). Sent
/// only when the rule replicates an ACL; base64 keeps the JSON's structural characters out of the
/// header value. The destination applies it fail-open (a malformed value is ignored, never a 4xx).
const REPLICA_ACL_KEY: &str = "cairn-replica-acl";

/// The most of a single object's logical body the buffered signed-payload PUT will hold in memory.
/// `HttpS3Sink` buffers the whole body to hash it for SigV4 signed-payload, so without a bound one
/// very large object (up to `CAIRN_MAX_OBJECT_SIZE`, default 5 TiB) times the worker concurrency
/// exhausts memory and OOM-kills the node — and, because the claimed outbox entry is re-leased on
/// restart, it re-buffers and OOMs again in a permanent crash loop (audit 2026-07). An object past
/// this cap fails replication terminally (parked, not retried). A future streaming
/// `UNSIGNED-PAYLOAD` PUT would remove the buffer entirely; until then this is a fixed bound rather
/// than an operator knob (mirrors the webhook response-body cap).
const MAX_BUFFERED_BODY_BYTES: usize = 2 * 1024 * 1024 * 1024; // 2 GiB

/// Connection parameters for a remote S3-compatible replication destination.
#[derive(Debug, Clone)]
pub struct S3SinkConfig {
    /// The endpoint base URL, e.g. `http://s3.us-east-1.example.com:9000` or
    /// `https://s3.example.com`. Path-style addressing is used: requests target
    /// `{endpoint}/{dest_bucket}/{key}`. Both `http://` and `https://` are supported.
    pub endpoint: String,
    /// The default destination bucket (path-style), used for any source bucket not present in
    /// [`dest_buckets`](Self::dest_buckets). With an empty map this is the single fixed
    /// destination (the original node->node behaviour).
    pub dest_bucket: String,
    /// Per-source-bucket destination overrides (`source bucket name -> dest bucket name`),
    /// resolved from each bucket's stored replication rule. A source bucket absent from the map
    /// replicates to [`dest_bucket`](Self::dest_bucket).
    pub dest_buckets: HashMap<String, String>,
    /// The SigV4 signing region.
    pub region: String,
    /// The destination access-key id.
    pub access_key_id: String,
    /// The destination secret access key.
    pub secret_access_key: String,
    /// An optional PEM bundle of CA certificates to trust for a `https://` endpoint, instead of
    /// the built-in webpki roots. Use this to replicate to a peer that presents a certificate
    /// signed by a private CA. Ignored for `http://` endpoints. Mutually exclusive with
    /// [`insecure_skip_verify`](Self::insecure_skip_verify).
    pub ca_cert_path: Option<PathBuf>,
    /// An optional CA certificate **as PEM text** (rather than a file path) to trust for a
    /// `https://` endpoint. Used by per-bucket replication targets configured through the console,
    /// where the operator pastes the peer's CA/self-signed certificate. Takes precedence over
    /// [`ca_cert_path`](Self::ca_cert_path); mutually exclusive with `insecure_skip_verify`.
    pub ca_cert_pem: Option<String>,
    /// When true, the server certificate of a `https://` endpoint is **not** verified: any
    /// certificate is accepted. This is dangerous and defeats TLS authentication; it exists only
    /// for testing against a self-signed endpoint and emits a loud warning when used.
    pub insecure_skip_verify: bool,
}

/// A production replication sink issuing SigV4-signed S3 requests to a remote endpoint over
/// HTTP or HTTPS. Implements [`BucketRoutedSink`](crate::BucketRoutedSink), choosing the
/// destination bucket per request from the source bucket.
pub struct HttpS3Sink {
    config: S3SinkConfig,
    /// The scheme of the endpoint (`http` or `https`), parsed once at construction. Reused when
    /// building each request URI so the connector dials the right transport.
    scheme: String,
    /// The scheme/authority of the endpoint, parsed once at construction (e.g. `s3.example.com:9000`).
    authority: String,
    /// The HTTP(S) client. The TLS-or-plaintext connector serves both schemes.
    client: Client<HttpsConnector<HttpConnector>, Full<bytes::Bytes>>,
    /// The clock supplying the SigV4 request time; injected so signing is deterministic in tests.
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for HttpS3Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpS3Sink")
            .field("endpoint", &self.config.endpoint)
            .field("dest_bucket", &self.config.dest_bucket)
            .field("dest_buckets", &self.config.dest_buckets)
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
    /// Returns [`ReplicationError::Terminal`] if the endpoint URL is malformed or names a scheme
    /// other than `http`/`https`; a misconfiguration is a permanent, operator-actionable problem,
    /// not a transient one.
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
        let scheme = match uri.scheme_str() {
            // Both schemes are served by the same TLS-or-plaintext connector below.
            Some(s @ ("http" | "https")) => s.to_owned(),
            other => {
                return Err(ReplicationError::Terminal(format!(
                    "unsupported endpoint scheme: {other:?}"
                )));
            }
        };
        let authority = uri
            .authority()
            .map(ToString::to_string)
            .ok_or_else(|| ReplicationError::Terminal("endpoint URL has no host".to_owned()))?;

        // One connector serves both transports: `.https_or_http()` dials plaintext for `http://`
        // and negotiates rustls for `https://`. `enable_http1()` matches the HTTP/1.1 protocol the
        // legacy client speaks. The TLS trust source is per-target (CA path / skip-verify /
        // webpki); see `build_tls_connector`.
        let builder = build_tls_connector_builder(&config)?;
        let https = builder.https_or_http().enable_http1().build();
        let client = Client::builder(TokioExecutor::new()).build(https);

        Ok(Self {
            config,
            scheme,
            authority,
            client,
            clock,
        })
    }

    /// Resolve the destination bucket for a source bucket: the per-source override if one is
    /// configured, otherwise the default [`dest_bucket`](S3SinkConfig::dest_bucket).
    #[must_use]
    pub fn dest_for(&self, source_bucket: &str) -> &str {
        self.config
            .dest_buckets
            .get(source_bucket)
            .map_or(self.config.dest_bucket.as_str(), String::as_str)
    }

    /// Build the canonical, percent-encoded request path `/{dest_bucket}/{key}` (S3 path-style).
    fn request_path(&self, dest_bucket: &str, key: &str) -> String {
        let mut path = String::from("/");
        path.push_str(&uri_encode_path(dest_bucket));
        path.push('/');
        path.push_str(&uri_encode_path(key));
        path
    }

    /// Sign and send one request to `dest_bucket`, classifying the outcome into the sink error
    /// taxonomy.
    async fn send_signed(
        &self,
        method: &Method,
        dest_bucket: &str,
        key: &str,
        body: bytes::Bytes,
        content_type: Option<&str>,
        user_headers: &[(String, String)],
    ) -> Result<(), ReplicationError> {
        let now = self.clock.now();
        let amz_date = format_amz_datetime(now);
        let scope_date = &amz_date[..8];
        let payload_hash = sha256_hex(&body);
        let path = self.request_path(dest_bucket, key);

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

        // Build the wire request. The endpoint scheme and authority are reused; only the path
        // varies. The scheme (`http`/`https`) selects the transport the connector dials.
        let uri = format!("{}://{}{path}", self.scheme, self.authority);
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

        // A connection/transport failure means the destination node is unreachable: classify it
        // `Unavailable` so the entry retries without burning the terminal attempt budget (a target
        // that is down for hours then returns must auto-resume, not exhaust to terminal).
        let response = self
            .client
            .request(request)
            .await
            .map_err(|e| ReplicationError::Unavailable(format!("transport error: {e}")))?;

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

/// Build an [`HttpS3Sink`] from an opened remote target (ARCH 20.5). The target supplies the
/// endpoint, region, destination bucket, and the unsealed credentials; `ca_path` /
/// `insecure_skip_verify` carry the per-target TLS-trust knobs. The resulting sink ships every
/// source bucket to the target's single `dest_bucket` (no per-source override map).
///
/// # Errors
/// Returns [`ReplicationError::Terminal`] if the endpoint URL is malformed or the TLS knobs
/// conflict (see [`HttpS3Sink::new`]).
pub fn sink_for_target(open: &crate::OpenTarget) -> Result<HttpS3Sink, ReplicationError> {
    HttpS3Sink::new(S3SinkConfig {
        endpoint: open.endpoint.clone(),
        dest_bucket: open.dest_bucket.clone(),
        dest_buckets: HashMap::new(),
        region: open.region.clone(),
        access_key_id: open.access_key_id.clone(),
        secret_access_key: open.secret.as_str().to_owned(),
        ca_cert_path: None,
        ca_cert_pem: open.ca_cert_pem.clone(),
        insecure_skip_verify: open.insecure_skip_verify,
    })
}

/// Build the `hyper-rustls` connector builder for a sink, selecting the TLS trust source from the
/// per-target knobs (ARCH 20.2):
///
/// * [`ca_cert_path`](S3SinkConfig::ca_cert_path) — trust exactly the CA anchors in that PEM file;
/// * [`insecure_skip_verify`](S3SinkConfig::insecure_skip_verify) — accept any certificate (logs a
///   loud warning);
/// * otherwise — the built-in webpki roots, the safe default.
///
/// The two non-default cases go through `with_tls_config` because `hyper-rustls` exposes no
/// `.dangerous()` shortcut. The returned builder is in the `WantsSchemes` state, identical to what
/// `with_webpki_roots()` produces, so the caller finishes it the same way for every variant.
fn build_tls_connector_builder(
    config: &S3SinkConfig,
) -> Result<
    hyper_rustls::HttpsConnectorBuilder<hyper_rustls::builderstates::WantsSchemes>,
    ReplicationError,
> {
    let base = hyper_rustls::HttpsConnectorBuilder::new();
    let custom_ca = config.ca_cert_pem.is_some() || config.ca_cert_path.is_some();
    if custom_ca && config.insecure_skip_verify {
        return Err(ReplicationError::Terminal(
            "a CA certificate and insecure_skip_verify are mutually exclusive".to_owned(),
        ));
    }
    if config.insecure_skip_verify {
        // A loud, operator-visible warning: skip-verify defeats TLS authentication entirely.
        tracing::warn!(
            endpoint = %config.endpoint,
            "replication TLS certificate verification DISABLED (insecure_skip_verify); the \
             destination's identity is NOT authenticated — use only for testing"
        );
        let verifier = Arc::new(NoVerification::new());
        let tls = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();
        return Ok(base.with_tls_config(tls));
    }
    // A pasted PEM certificate takes precedence over a file path; otherwise trust the built-in roots.
    let roots = if let Some(pem) = &config.ca_cert_pem {
        Some(load_roots_from_pem(
            pem.as_bytes(),
            "configured CA certificate",
        )?)
    } else if let Some(ca_path) = &config.ca_cert_path {
        Some(load_root_store(ca_path)?)
    } else {
        None
    };
    match roots {
        Some(roots) => {
            let tls = ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            Ok(base.with_tls_config(tls))
        }
        None => Ok(base.with_webpki_roots()),
    }
}

/// Read a PEM bundle of CA certificates from `path` into a fresh [`RootCertStore`]. A missing or
/// unreadable file, or a bundle that yields no usable certificates, is a permanent misconfiguration
/// (terminal), not a transient failure.
fn load_root_store(path: &Path) -> Result<RootCertStore, ReplicationError> {
    let pem = std::fs::read(path).map_err(|e| {
        ReplicationError::Terminal(format!("reading CA bundle {}: {e}", path.display()))
    })?;
    load_roots_from_pem(&pem, &format!("CA bundle {}", path.display()))
}

/// Build a [`RootCertStore`] from PEM certificate text. `source` is a human-readable label used in
/// error messages (a file path or "configured CA certificate").
fn load_roots_from_pem(pem: &[u8], source: &str) -> Result<RootCertStore, ReplicationError> {
    let mut reader = std::io::BufReader::new(pem);
    let mut roots = RootCertStore::empty();
    let mut added = 0usize;
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert =
            cert.map_err(|e| ReplicationError::Terminal(format!("parsing {source}: {e}")))?;
        roots
            .add(cert)
            .map_err(|e| ReplicationError::Terminal(format!("adding CA from {source}: {e}")))?;
        added += 1;
    }
    if added == 0 {
        return Err(ReplicationError::Terminal(format!(
            "{source} contained no certificates"
        )));
    }
    Ok(roots)
}

/// A [`ServerCertVerifier`] that accepts every certificate and signature without checking
/// anything. It backs [`insecure_skip_verify`](S3SinkConfig::insecure_skip_verify); installing it
/// is what makes TLS authentication a no-op, so it is constructed only on that explicit, warned
/// opt-in. Signature schemes are delegated to the aws-lc-rs provider (the one the sink's
/// `ClientConfig::builder()` uses) so the handshake still advertises a real, accepted set.
#[derive(Debug)]
struct NoVerification {
    schemes: Vec<SignatureScheme>,
}

impl NoVerification {
    fn new() -> Self {
        Self {
            schemes: aws_lc_rs::default_provider()
                .signature_verification_algorithms
                .supported_schemes(),
        }
    }
}

impl ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}

/// Classify a non-2xx HTTP status into the sink error taxonomy: `5xx` and the transient `408`/`429`
/// mean the destination is unavailable/overloaded (retry without consuming the attempt budget);
/// every other `4xx` is a per-request rejection and is terminal.
fn classify_status(code: u16, detail: &str) -> ReplicationError {
    let msg = if detail.trim().is_empty() {
        format!("destination returned HTTP {code}")
    } else {
        format!("destination returned HTTP {code}: {}", detail.trim())
    };
    if code >= 500 || code == 408 || code == 429 {
        ReplicationError::Unavailable(msg)
    } else {
        ReplicationError::Terminal(msg)
    }
}

impl HttpS3Sink {
    /// PUT a replicated object into the destination bucket resolved for `source_bucket`.
    async fn put_object_routed(
        &self,
        source_bucket: &str,
        object: ReplicatedObject,
    ) -> Result<(), ReplicationError> {
        let dest_bucket = self.dest_for(source_bucket).to_owned();

        // Buffer the logical body so the payload can be hashed for the signed-payload PUT, bounded so
        // one oversized object cannot OOM the node in a crash loop (audit 2026-07).
        let body = collect_body(object.body, MAX_BUFFERED_BODY_BYTES).await?;

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
        // Carry the source version id so the destination preserves it (version-id identity +
        // idempotent re-delivery).
        user_headers.push((
            format!("x-amz-meta-{REPLICA_VERSION_ID_KEY}"),
            object.version_id.as_str().to_owned(),
        ));
        // Carry the object's ACL (base64 of its JSON) when the rule replicates one, so the
        // destination reproduces the source's grants. Absent when there is no ACL (and never on the
        // delete-marker path). Serialization cannot realistically fail for an `Acl`; if it somehow
        // did we simply omit the header (the destination then keeps its default ownership).
        if let Some(acl) = &object.acl {
            if let Ok(json) = serde_json::to_vec(acl) {
                user_headers.push((
                    format!("x-amz-meta-{REPLICA_ACL_KEY}"),
                    base64::engine::general_purpose::STANDARD.encode(json),
                ));
            }
        }

        // Replicate the object's tag set via the standard `x-amz-tagging` header (form-urlencoded
        // `k=v&k=v`), so the destination version carries the same tags the source rule filtered on.
        // `uri_encode_path` percent-encodes the structural `&`/`=` which the destination's
        // `form_pct_decode` reverses.
        if !object.tags.is_empty() {
            let tagging = object
                .tags
                .iter()
                .map(|(k, v)| format!("{}={}", uri_encode_path(k), uri_encode_path(v)))
                .collect::<Vec<_>>()
                .join("&");
            user_headers.push(("x-amz-tagging".to_owned(), tagging));
        }

        self.send_signed(
            &Method::PUT,
            &dest_bucket,
            object.key.as_str(),
            body,
            Some(&object.content_type),
            &user_headers,
        )
        .await
    }

    /// DELETE a key in the destination bucket resolved for `source_bucket`.
    async fn delete_marker_routed(
        &self,
        source_bucket: &str,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<(), ReplicationError> {
        let dest_bucket = self.dest_for(source_bucket).to_owned();
        // Stamp the loop-prevention marker so the destination (a) authorizes this as a
        // `ReplicateDelete` for a dedicated replication user and (b) records the propagated marker
        // as a replica rather than re-replicating it (ARCH 20.4), mirroring the PUT path. The
        // source version id is carried so the destination preserves the marker's identity.
        let headers = [
            (
                format!("x-amz-meta-{REPLICA_MARKER_KEY}"),
                "true".to_owned(),
            ),
            (
                format!("x-amz-meta-{REPLICA_VERSION_ID_KEY}"),
                version.as_str().to_owned(),
            ),
        ];
        self.send_signed(
            &Method::DELETE,
            &dest_bucket,
            key.as_str(),
            bytes::Bytes::new(),
            None,
            &headers,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::route::BucketRoutedSink for HttpS3Sink {
    async fn put_object(
        &self,
        source_bucket: &BucketName,
        object: ReplicatedObject,
    ) -> Result<(), ReplicationError> {
        self.put_object_routed(source_bucket.as_str(), object).await
    }

    async fn delete_marker(
        &self,
        source_bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<(), ReplicationError> {
        self.delete_marker_routed(source_bucket.as_str(), key, version)
            .await
    }
}

/// Read a logical-byte blob stream fully into a contiguous buffer, bounded by `max_bytes`. A read
/// error mid-stream is transient (the source blob may be momentarily unavailable), so it is
/// retryable. Exceeding `max_bytes` is a *terminal* failure: the signed-payload PUT buffers the whole
/// body in memory, so an oversized object would OOM the node — and because a claimed outbox entry is
/// re-leased on restart, a retryable failure would re-buffer and OOM again in a permanent crash loop
/// (audit 2026-07). Terminal parks the entry for operator attention instead.
async fn collect_body(
    mut stream: cairn_types::BlobStream,
    max_bytes: usize,
) -> Result<bytes::Bytes, ReplicationError> {
    let mut buf = bytes::BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|e| ReplicationError::Retryable(format!("reading source body: {e}")))?;
        if buf.len().saturating_add(chunk.len()) > max_bytes {
            return Err(ReplicationError::Terminal(format!(
                "object body exceeds the {max_bytes}-byte replication buffer cap; the signed-payload \
                 PUT buffers the whole body in memory. This object will not replicate until streaming \
                 uploads land (audit 2026-07)."
            )));
        }
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
    fn classify_status_partitions_unavailable_and_terminal() {
        // 5xx and the transient 408/429 mean the destination is unavailable/overloaded — retried
        // without consuming the terminal attempt budget.
        assert!(matches!(
            classify_status(500, ""),
            ReplicationError::Unavailable(_)
        ));
        assert!(matches!(
            classify_status(503, "slow down"),
            ReplicationError::Unavailable(_)
        ));
        assert!(matches!(
            classify_status(429, ""),
            ReplicationError::Unavailable(_)
        ));
        assert!(matches!(
            classify_status(408, ""),
            ReplicationError::Unavailable(_)
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

    #[tokio::test]
    async fn collect_body_caps_oversize_terminally() {
        use bytes::Bytes;
        type Chunk = Result<Bytes, cairn_types::error::BlobError>;

        // A body over the cap fails TERMINAL (not Retryable) so the entry parks instead of
        // re-leasing on restart and OOM-looping (audit 2026-07).
        let over: Vec<Chunk> = vec![
            Ok(Bytes::from_static(&[0u8; 8])),
            Ok(Bytes::from_static(&[0u8; 8])),
        ];
        let stream = Box::pin(futures_util::stream::iter(over));
        let err = collect_body(stream, 10)
            .await
            .expect_err("a body over the cap must error");
        assert!(
            matches!(err, ReplicationError::Terminal(_)),
            "over-cap must be Terminal, got {err:?}"
        );

        // A body within the cap collects fine.
        let under: Vec<Chunk> = vec![Ok(Bytes::from_static(b"hello"))];
        let stream = Box::pin(futures_util::stream::iter(under));
        let body = collect_body(stream, 10).await.expect("within cap collects");
        assert_eq!(&body[..], b"hello");
    }

    fn cfg_for(endpoint: &str) -> S3SinkConfig {
        S3SinkConfig {
            endpoint: endpoint.to_owned(),
            dest_bucket: "dest".to_owned(),
            dest_buckets: HashMap::new(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKID".to_owned(),
            secret_access_key: "secret".to_owned(),
            ca_cert_path: None,
            ca_cert_pem: None,
            insecure_skip_verify: false,
        }
    }

    #[test]
    fn builds_for_https_endpoint() {
        // An https:// endpoint must construct cleanly now that the TLS connector is wired in
        // (the former terminal https-rejection is gone).
        let sink = HttpS3Sink::new(cfg_for("https://s3.example.com")).expect("https sink builds");
        assert_eq!(sink.scheme, "https");
        assert_eq!(sink.authority, "s3.example.com");
    }

    #[test]
    fn builds_for_http_endpoint() {
        // The same connector still serves plaintext http:// endpoints.
        let sink =
            HttpS3Sink::new(cfg_for("http://s3.example.com:9000")).expect("http sink builds");
        assert_eq!(sink.scheme, "http");
        assert_eq!(sink.authority, "s3.example.com:9000");
    }

    #[test]
    fn rejects_malformed_endpoint() {
        assert!(HttpS3Sink::new(cfg_for("not a url")).is_err());
    }

    #[test]
    fn rejects_unsupported_scheme() {
        let err = HttpS3Sink::new(cfg_for("ftp://s3.example.com")).unwrap_err();
        assert!(matches!(err, ReplicationError::Terminal(_)));
    }

    #[test]
    fn dest_for_resolves_per_source_with_default_fallback() {
        let mut dest_buckets = HashMap::new();
        dest_buckets.insert("logs-src".to_owned(), "logs-dst".to_owned());
        dest_buckets.insert("media-src".to_owned(), "media-dst".to_owned());
        let cfg = S3SinkConfig {
            endpoint: "http://s3.example.com".to_owned(),
            dest_bucket: "fallback-dst".to_owned(),
            dest_buckets,
            region: "us-east-1".to_owned(),
            access_key_id: "AKID".to_owned(),
            secret_access_key: "secret".to_owned(),
            ca_cert_path: None,
            ca_cert_pem: None,
            insecure_skip_verify: false,
        };
        let sink = HttpS3Sink::new(cfg).unwrap();
        // Mapped sources resolve to their explicit destinations.
        assert_eq!(sink.dest_for("logs-src"), "logs-dst");
        assert_eq!(sink.dest_for("media-src"), "media-dst");
        // An unmapped source falls back to the default destination bucket.
        assert_eq!(sink.dest_for("other-src"), "fallback-dst");
    }

    #[test]
    fn request_path_uses_resolved_dest_bucket() {
        let sink = HttpS3Sink::new(cfg_for("http://s3.example.com")).unwrap();
        assert_eq!(sink.request_path("dst", "a/b c"), "/dst/a/b%20c");
    }

    /// A self-signed CA certificate (PEM), used to exercise the custom-CA trust path. Generated
    /// once with `openssl req -x509` (CN=cairn-test-ca, 100-year validity) so the test is
    /// hermetic and needs no tooling at run time.
    const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIDEzCCAfugAwIBAgIUe4M5AXhgFTWt7qnOtCOt72yDEfMwDQYJKoZIhvcNAQEL\n\
BQAwGDEWMBQGA1UEAwwNY2Fpcm4tdGVzdC1jYTAgFw0yNjA2MDcxMTMyMjVaGA8y\n\
MTI2MDUxNDExMzIyNVowGDEWMBQGA1UEAwwNY2Fpcm4tdGVzdC1jYTCCASIwDQYJ\n\
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAM8wfaaCovY1pSPYotW+aXm4JvDQauQv\n\
UkwLZTNkyuG3/7N+jzSZIC1BS+tPej+ekQjm8us3zp0f4FDTEBsxc1pX144arIAT\n\
coJn1mH1mKNgGF/Nj+y35mWWIH7DRFja1Wf4rl12P4qRo705n406k6mRtwp6o++m\n\
kkW4VuO0X5GfSsx0ZGkZ2MAo2wTSyBKHgxv7tqzHNYrZdFmUNFs1K1eDP1kW61+Q\n\
Vj5eRHbOMOsKaDRyXmFs+6I1jMJa+4XlYxJ8BMFhIruX5PYcRyOjSUaPGI3y/Twm\n\
GJb6l3R0TTD82AP+TOdkAB1O/ivPZuaL/5tlpxb3R4EN5im28q4jZh0CAwEAAaNT\n\
MFEwHQYDVR0OBBYEFMALcto1SS3baEY/CfRjatZ5eIX4MB8GA1UdIwQYMBaAFMAL\n\
cto1SS3baEY/CfRjatZ5eIX4MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQEL\n\
BQADggEBALXMpLBStTKTWhAD8cbLWazTknzSkPAblHLpg6i11lqXl/F/KZ6kFXlw\n\
YsOAWDXJ/sRVjIYHw6383+wv2fDe5HFmZfiRAVrCgGciN6nEuj7uMIBBMWushgwB\n\
lKW7AYk2V0jamYhThbAyqUmu4JEvfJY7jQfv3S6kjVPLQtPe8N5qSMML44oC+bi2\n\
V+IAp6sZrU2TNVgeOnP18BtJWFoKmHXgSs5eJtDcmw41llD1CCUnVjSfUGPmHNSb\n\
hO1QwFPernIBHXfT8PObpNX2wryLTH1rMSJwHt50++2EPnR0Npi85smSQ4GglyTw\n\
+/7AJl5aMXyWTz4YhkL9aoTvLfbWrz8=\n\
-----END CERTIFICATE-----\n";

    /// An https endpoint with `insecure_skip_verify` builds: the connector is constructed through
    /// the `with_tls_config` path with the no-op verifier installed (no `.dangerous()` shortcut on
    /// hyper-rustls), so a sink against a self-signed endpoint comes up.
    #[test]
    fn builds_https_with_insecure_skip_verify() {
        let mut cfg = cfg_for("https://self-signed.example.com");
        cfg.insecure_skip_verify = true;
        let sink = HttpS3Sink::new(cfg).expect("insecure-skip-verify https sink builds");
        assert_eq!(sink.scheme, "https");
        assert_eq!(sink.authority, "self-signed.example.com");
    }

    /// A custom CA path is honoured: the PEM bundle is read and a sink built against it. A bundle
    /// that contains no certificates is rejected, and a path that conflicts with skip-verify is
    /// rejected, so the trust source is unambiguous.
    #[test]
    fn builds_https_with_custom_ca_path() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("ca.pem");
        std::fs::write(&ca, TEST_CA_PEM).unwrap();

        // The bundle parses into a non-empty root store.
        let roots = load_root_store(&ca).expect("CA bundle loads");
        assert_eq!(roots.len(), 1, "exactly the one test CA is trusted");

        // And a sink against an https endpoint with that CA constructs.
        let mut cfg = cfg_for("https://peer.example.com");
        cfg.ca_cert_path = Some(ca);
        let sink = HttpS3Sink::new(cfg).expect("custom-CA https sink builds");
        assert_eq!(sink.scheme, "https");
    }

    /// A CA bundle with no certificates in it is a misconfiguration, not a transient failure.
    #[test]
    fn empty_ca_bundle_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("empty.pem");
        std::fs::write(&ca, "not a certificate\n").unwrap();
        let err = load_root_store(&ca).unwrap_err();
        assert!(matches!(err, ReplicationError::Terminal(_)));
    }

    /// A missing CA file is terminal (the path is wrong; retrying will not fix it).
    #[test]
    fn missing_ca_file_is_rejected() {
        let err = load_root_store(Path::new("/no/such/ca.pem")).unwrap_err();
        assert!(matches!(err, ReplicationError::Terminal(_)));
        // ...and surfaces through sink construction too.
        let mut cfg = cfg_for("https://peer.example.com");
        cfg.ca_cert_path = Some(PathBuf::from("/no/such/ca.pem"));
        assert!(HttpS3Sink::new(cfg).is_err());
    }

    /// Setting both a CA path and skip-verify is contradictory and rejected at construction.
    #[test]
    fn ca_path_and_skip_verify_conflict_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("ca.pem");
        std::fs::write(&ca, TEST_CA_PEM).unwrap();
        let mut cfg = cfg_for("https://peer.example.com");
        cfg.ca_cert_path = Some(ca);
        cfg.insecure_skip_verify = true;
        let err = HttpS3Sink::new(cfg).unwrap_err();
        assert!(matches!(err, ReplicationError::Terminal(_)));
    }

    /// The no-op verifier accepts any certificate and signature, and advertises a non-empty,
    /// provider-backed scheme set (so the handshake offers a real list).
    #[test]
    fn no_verification_accepts_everything() {
        let v = NoVerification::new();
        assert!(!v.supported_verify_schemes().is_empty());
        // verify_server_cert returns Ok for an arbitrary (here empty) certificate.
        let cert = CertificateDer::from(vec![0u8; 4]);
        let name = ServerName::try_from("example.com").unwrap();
        assert!(
            v.verify_server_cert(&cert, &[], &name, &[], UnixTime::now())
                .is_ok()
        );
    }

    /// The plaintext `http://` path is unaffected by the trust knobs (they apply only to TLS), so
    /// the default single-target node->node path keeps building exactly as before.
    #[test]
    fn http_endpoint_unaffected_by_trust_defaults() {
        let sink = HttpS3Sink::new(cfg_for("http://s3.example.com:9000")).expect("http builds");
        assert_eq!(sink.scheme, "http");
    }
}
