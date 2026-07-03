//! [`HttpS3Source`]: the production [`SourceReader`](crate::SourceReader) — SigV4-signed `GET` /
//! `ListObjectsV2` / `ListBuckets` against a remote S3-compatible endpoint over http or https,
//! dialing through the SSRF-guarded connector. It reuses `cairn-auth`'s signing primitives (so a
//! signed request is byte-identical to what the verifier expects) and streams object bodies.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use http::{Method, Request};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rustls::ClientConfig;
use rustls::RootCertStore;

use cairn_auth::{canonical_request, compute_signature, signing_key, string_to_sign, uri_encode};
use cairn_net::GuardedResolver;

use crate::{ImportError, ObjectPage, RemoteObject, SourceObject, SourceReader};

const SERVICE: &str = "s3";
/// The sha256 of an empty body (all GETs Cairn issues carry no request body).
const EMPTY_PAYLOAD_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// Cap on a buffered *listing* response body (object GETs stream and are never buffered).
const MAX_LIST_BODY: usize = 16 * 1024 * 1024;

/// Connection parameters for the remote source.
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// The endpoint base URL, e.g. `https://s3.example.com:9000`. http and https are both supported.
    pub endpoint: String,
    /// The SigV4 signing region.
    pub region: String,
    /// The source access-key id.
    pub access_key_id: String,
    /// The source secret access key.
    pub secret_access_key: String,
    /// An optional PEM CA bundle to trust for an https endpoint (mutually exclusive with skip-verify).
    pub ca_cert_pem: Option<String>,
    /// Accept any TLS certificate (testing only).
    pub insecure_skip_verify: bool,
    /// Whether the SSRF guard permits internal addresses for this source.
    pub allow_internal_endpoints: bool,
}

type SourceClient = Client<HttpsConnector<HttpConnector<GuardedResolver>>, Full<Bytes>>;

/// A signed, streaming S3 read client for a remote source.
pub struct HttpS3Source {
    config: SourceConfig,
    scheme: String,
    authority: String,
    client: SourceClient,
}

impl std::fmt::Debug for HttpS3Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpS3Source")
            .field("endpoint", &self.config.endpoint)
            .field("region", &self.config.region)
            .field("access_key_id", &self.config.access_key_id)
            .finish_non_exhaustive()
    }
}

impl HttpS3Source {
    /// Build a source client, parsing/validating the endpoint URL and TLS trust.
    ///
    /// # Errors
    /// [`ImportError::Terminal`] if the endpoint is malformed, its scheme is not http/https, or the
    /// TLS knobs conflict.
    pub fn new(config: SourceConfig) -> Result<Self, ImportError> {
        let uri: http::Uri = config
            .endpoint
            .parse()
            .map_err(|e| ImportError::Terminal(format!("invalid source endpoint URL: {e}")))?;
        let scheme = match uri.scheme_str() {
            Some(s @ ("http" | "https")) => s.to_owned(),
            other => {
                return Err(ImportError::Terminal(format!(
                    "unsupported source endpoint scheme: {other:?}"
                )));
            }
        };
        let authority = uri
            .authority()
            .map(ToString::to_string)
            .ok_or_else(|| ImportError::Terminal("source endpoint has no host".to_owned()))?;

        let client = build_client(&config)?;
        Ok(Self {
            config,
            scheme,
            authority,
            client,
        })
    }

    /// Sign and send a GET, returning the raw response. `canonical_query` is the sorted,
    /// percent-encoded query string (empty for an object GET); `path` is the percent-encoded path.
    async fn signed_get(
        &self,
        path: &str,
        query: &str,
    ) -> Result<hyper::Response<Incoming>, ImportError> {
        let amz_date = format_amz_now();
        let scope_date = &amz_date[..8];

        let signed: Vec<(String, String)> = vec![
            ("host".to_owned(), self.authority.clone()),
            (
                "x-amz-content-sha256".to_owned(),
                EMPTY_PAYLOAD_HASH.to_owned(),
            ),
            ("x-amz-date".to_owned(), amz_date.clone()),
        ];
        // Already sorted by name (host < x-amz-content-sha256 < x-amz-date).
        let signed_names = "host;x-amz-content-sha256;x-amz-date";
        let canonical = canonical_request(
            "GET",
            path,
            query,
            &signed,
            signed_names,
            EMPTY_PAYLOAD_HASH,
        );
        let scope = format!("{scope_date}/{}/{SERVICE}/aws4_request", self.config.region);
        let sts = string_to_sign(&amz_date, &scope, &canonical);
        let key = signing_key(
            &self.config.secret_access_key,
            scope_date,
            &self.config.region,
            SERVICE,
        );
        let signature = compute_signature(&key, &sts);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_names}, \
             Signature={signature}",
            self.config.access_key_id
        );

        let uri = if query.is_empty() {
            format!("{}://{}{path}", self.scheme, self.authority)
        } else {
            format!("{}://{}{path}?{query}", self.scheme, self.authority)
        };
        let req = Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(http::header::HOST, &self.authority)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", EMPTY_PAYLOAD_HASH)
            .header(http::header::AUTHORIZATION, &authorization)
            .body(Full::new(Bytes::new()))
            .map_err(|e| ImportError::Terminal(format!("request build failed: {e}")))?;

        self.client
            .request(req)
            .await
            .map_err(|e| ImportError::Unavailable(format!("source connection failed: {e}")))
    }

    /// Buffer a (small) listing response body, capped so a hostile/huge listing can't OOM us.
    async fn read_list_body(resp: hyper::Response<Incoming>) -> Result<Bytes, ImportError> {
        let status = resp.status();
        let limited = http_body_util::Limited::new(resp.into_body(), MAX_LIST_BODY);
        let bytes = limited
            .collect()
            .await
            .map_err(|e| ImportError::Retryable(format!("reading listing body: {e}")))?
            .to_bytes();
        if !status.is_success() {
            return Err(classify_status(status.as_u16(), &bytes));
        }
        Ok(bytes)
    }
}

#[async_trait]
impl SourceReader for HttpS3Source {
    async fn list_buckets(&self) -> Result<Vec<String>, ImportError> {
        let resp = self.signed_get("/", "").await?;
        let body = Self::read_list_body(resp).await?;
        cairn_xml::parse_list_all_my_buckets(&body)
            .map_err(|e| ImportError::Terminal(format!("parsing ListBuckets response: {e}")))
    }

    async fn list_objects(
        &self,
        bucket: &str,
        cursor: Option<&str>,
        max_keys: u32,
    ) -> Result<ObjectPage, ImportError> {
        let path = format!("/{}", uri_encode(bucket, false));
        // Canonical query: keys sorted (continuation-token, list-type, max-keys), values encoded.
        let mut query = String::new();
        if let Some(tok) = cursor {
            query.push_str("continuation-token=");
            query.push_str(&uri_encode(tok, true));
            query.push('&');
        }
        query.push_str("list-type=2&max-keys=");
        query.push_str(&max_keys.to_string());

        let resp = self.signed_get(&path, &query).await?;
        let body = Self::read_list_body(resp).await?;
        let (objects, next_cursor, is_truncated) = cairn_xml::parse_list_objects_v2(&body)
            .map_err(|e| ImportError::Terminal(format!("parsing ListObjectsV2 response: {e}")))?;
        Ok(ObjectPage {
            objects: objects
                .into_iter()
                .map(|(key, size, etag)| RemoteObject { key, size, etag })
                .collect(),
            next_cursor,
            is_truncated,
        })
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<SourceObject, ImportError> {
        let path = format!("/{}/{}", uri_encode(bucket, false), uri_encode(key, false));
        let resp = self.signed_get(&path, "").await?;
        let status = resp.status();
        if !status.is_success() {
            // Read a bounded slice of the error body for context, then classify.
            let body = http_body_util::Limited::new(resp.into_body(), 8 * 1024)
                .collect()
                .await
                .map(|b| b.to_bytes())
                .unwrap_or_default();
            return Err(classify_status(status.as_u16(), &body));
        }

        let headers = resp.headers();
        let size = headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let hdr = |name: http::header::HeaderName| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        let etag = hdr(http::header::ETAG).map(|e| e.trim_matches('"').to_owned());
        let content_type = hdr(http::header::CONTENT_TYPE);
        let content_encoding = hdr(http::header::CONTENT_ENCODING);
        let cache_control = hdr(http::header::CACHE_CONTROL);
        let content_disposition = hdr(http::header::CONTENT_DISPOSITION);
        let content_language = hdr(http::header::CONTENT_LANGUAGE);
        // User metadata: x-amz-meta-* headers, key stored WITHOUT the prefix.
        let mut user_metadata = Vec::new();
        for (name, value) in headers.iter() {
            let n = name.as_str();
            if let Some(meta_key) = n.strip_prefix("x-amz-meta-") {
                if let Ok(v) = value.to_str() {
                    user_metadata.push((meta_key.to_owned(), v.to_owned()));
                }
            }
        }

        // Stream the body straight through — never buffered.
        let body = resp
            .into_body()
            .into_data_stream()
            .map(|r| r.map_err(|e| cairn_types::error::BlobError::Io(e.to_string())));
        let body: cairn_types::BlobStream = Box::pin(body);

        Ok(SourceObject {
            key: key.to_owned(),
            size,
            etag,
            content_type,
            user_metadata,
            content_encoding,
            cache_control,
            content_disposition,
            content_language,
            body,
        })
    }
}

/// Classify a non-2xx source status into the import error taxonomy: 5xx/408/429 are transient
/// (`Unavailable`), everything else is terminal for that request.
fn classify_status(status: u16, body: &[u8]) -> ImportError {
    let detail = String::from_utf8_lossy(&body[..body.len().min(512)]).to_string();
    if status >= 500 || status == 408 || status == 429 {
        ImportError::Unavailable(format!("source returned {status}: {detail}"))
    } else {
        ImportError::Terminal(format!("source returned {status}: {detail}"))
    }
}

/// Build the hyper client with the SSRF-guarded connector and per-source TLS trust (mirrors the
/// replication sink's connector).
fn build_client(config: &SourceConfig) -> Result<SourceClient, ImportError> {
    let base = hyper_rustls::HttpsConnectorBuilder::new();
    let custom_ca = config.ca_cert_pem.is_some();
    if custom_ca && config.insecure_skip_verify {
        return Err(ImportError::Terminal(
            "a CA certificate and insecure_skip_verify are mutually exclusive".to_owned(),
        ));
    }
    let builder = if config.insecure_skip_verify {
        tracing::warn!(
            endpoint = %config.endpoint,
            "import source TLS certificate verification DISABLED (insecure_skip_verify)"
        );
        let tls = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(no_verify::NoVerification::new()))
            .with_no_client_auth();
        base.with_tls_config(tls)
    } else if let Some(pem) = &config.ca_cert_pem {
        let roots = load_roots_from_pem(pem.as_bytes())?;
        let tls = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        base.with_tls_config(tls)
    } else {
        base.with_webpki_roots()
    };
    let guard = cairn_net::GuardConfig::new(config.allow_internal_endpoints);
    let https = builder
        .https_or_http()
        .enable_http1()
        .wrap_connector(cairn_net::guarded_http_connector(guard));
    Ok(Client::builder(TokioExecutor::new()).build(https))
}

fn load_roots_from_pem(pem: &[u8]) -> Result<RootCertStore, ImportError> {
    let mut reader = std::io::BufReader::new(pem);
    let mut roots = RootCertStore::empty();
    let mut added = 0usize;
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert =
            cert.map_err(|e| ImportError::Terminal(format!("parsing source CA certificate: {e}")))?;
        roots
            .add(cert)
            .map_err(|e| ImportError::Terminal(format!("adding source CA certificate: {e}")))?;
        added += 1;
    }
    if added == 0 {
        return Err(ImportError::Terminal(
            "the configured source CA certificate contained no certificates".to_owned(),
        ));
    }
    Ok(roots)
}

/// Format the current instant as the SigV4 `x-amz-date` (`YYYYMMDDTHHMMSSZ`, UTC).
fn format_amz_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

/// Civil `(year, month, day)` from days since the Unix epoch (Howard Hinnant's algorithm).
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

/// A `ServerCertVerifier` that accepts everything, backing `insecure_skip_verify` (testing only).
mod no_verify {
    use rustls::DigitallySignedStruct;
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::aws_lc_rs;
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{Error as TlsError, SignatureScheme};

    #[derive(Debug)]
    pub struct NoVerification {
        schemes: Vec<SignatureScheme>,
    }

    impl NoVerification {
        pub fn new() -> Self {
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
        ) -> Result<ServerCertVerified, TlsError> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, TlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, TlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.schemes.clone()
        }
    }
}
