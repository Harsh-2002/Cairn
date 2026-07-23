//! Integration tests for [`HttpS3Sink`] against a tiny in-process hyper server. The server
//! captures the request line and headers each call receives, so the tests can assert that the
//! sink emits a correctly-signed, correctly-shaped PUT/DELETE without contacting a real S3.

use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use cairn_types::error::{BlobError, ReplicationError};
use cairn_types::id::{BucketName, ObjectKey, VersionId};
use cairn_types::object::{ChecksumAlgorithm, ChecksumValue, ETag};
use cairn_types::replication::ReplicatedObject;
use cairn_types::time::Timestamp;
use cairn_types::traits::Clock;

use cairn_replication::{BucketRoutedSink, HttpS3Sink, S3SinkConfig};

use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

/// A clock pinned to a fixed instant so the signed request is byte-for-byte deterministic.
#[derive(Debug, Clone, Copy)]
struct FixedClock(i64);

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_secs(self.0)
    }
}

/// What one received request looked like on the wire.
#[derive(Debug, Clone, Default)]
struct Captured {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Captured {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// A response the test server should return for the next request.
#[derive(Clone, Copy)]
struct Reply {
    status: u16,
}

/// Spawn a one-connection-at-a-time hyper server that records each request into `captured` and
/// answers with `reply`. Returns the bound `host:port` authority.
async fn spawn_server(captured: Arc<Mutex<Vec<Captured>>>, reply: Reply) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let authority = format!("127.0.0.1:{}", addr.port());

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let captured = captured.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req: Request<Incoming>| {
                    let captured = captured.clone();
                    async move {
                        let method = req.method().to_string();
                        let path = req.uri().path().to_string();
                        let headers = req
                            .headers()
                            .iter()
                            .map(|(n, v)| {
                                (n.as_str().to_owned(), v.to_str().unwrap_or("").to_owned())
                            })
                            .collect();
                        let body = req.into_body().collect().await.unwrap().to_bytes().to_vec();
                        captured.lock().unwrap().push(Captured {
                            method,
                            path,
                            headers,
                            body,
                        });
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(StatusCode::from_u16(reply.status).unwrap())
                                .body(Full::new(Bytes::from_static(b"ok")))
                                .unwrap(),
                        )
                    }
                });
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await;
            });
        }
    });

    authority
}

fn body_stream(bytes: &'static [u8]) -> cairn_types::BlobStream {
    Box::pin(futures_util::stream::once(async move {
        Ok::<Bytes, BlobError>(Bytes::from_static(bytes))
    }))
}

fn sink_for(authority: &str, clock_secs: i64) -> HttpS3Sink {
    HttpS3Sink::with_clock(
        S3SinkConfig {
            endpoint: format!("http://{authority}"),
            dest_bucket: "dest-bucket".to_owned(),
            dest_buckets: std::collections::HashMap::new(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_owned(),
            ca_cert_path: None,
            ca_cert_pem: None,
            insecure_skip_verify: false,
            // The mock S3 server runs on loopback, so opt out of the SSRF guard for these tests.
            allow_internal_endpoints: true,
            allow_plaintext_sse_over_http: false,
        },
        Arc::new(FixedClock(clock_secs)),
    )
    .unwrap()
}

/// The source bucket each call replicates *from*. With the default (empty) map, `sink_for`
/// routes every source to `dest-bucket`, so the wire path is unaffected by the source name.
fn src() -> BucketName {
    BucketName::parse("source-bucket").unwrap()
}

#[tokio::test]
async fn put_object_issues_well_formed_signed_request() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured.clone(), Reply { status: 200 }).await;

    // 2015-08-30T12:36:00Z.
    let sink = sink_for(&authority, 1_440_938_160);

    let object = ReplicatedObject {
        key: ObjectKey::parse("logs/app.log").unwrap(),
        version_id: VersionId::from_string("v1".to_owned()),
        content_type: "text/plain".to_owned(),
        user_metadata: vec![("Owner".to_owned(), "alice".to_owned())],
        etag: ETag::from_string("\"abc\"".to_owned()),
        size: 5,
        tags: Vec::new(),
        acl: None,
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: cairn_types::object::StorageClass::Standard,
        checksums: Vec::new(),
        client_encrypted: false,
        body: body_stream(b"hello"),
    };

    sink.put_object(&src(), object).await.unwrap();

    let reqs = captured.lock().unwrap().clone();
    assert_eq!(reqs.len(), 1);
    let req = &reqs[0];

    assert_eq!(req.method, "PUT");
    assert_eq!(req.path, "/dest-bucket/logs/app.log");
    assert_eq!(req.body, b"hello");

    // Content metadata.
    assert_eq!(req.header("content-type"), Some("text/plain"));
    assert_eq!(req.header("content-length"), Some("5"));

    // User metadata is carried as x-amz-meta-*, lowercased.
    assert_eq!(req.header("x-amz-meta-owner"), Some("alice"));
    // The loop-prevention marker is always present.
    assert_eq!(req.header("x-amz-meta-cairn-replica"), Some("true"));
    // The source version id is carried so the destination preserves it (version-id identity).
    assert_eq!(
        req.header("x-amz-meta-cairn-replica-version-id"),
        Some("v1")
    );

    // SigV4 date and payload hash.
    assert_eq!(req.header("x-amz-date"), Some("20150830T123600Z"));
    // sha256("hello").
    assert_eq!(
        req.header("x-amz-content-sha256"),
        Some("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
    );

    // The Authorization header is a well-formed SigV4 header naming our credential, the signed
    // headers (including the signed user-metadata and host), and a 64-hex signature.
    let auth = req.header("authorization").expect("authorization header");
    assert!(auth.starts_with("AWS4-HMAC-SHA256 "), "auth = {auth}");
    assert!(
        auth.contains("Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request"),
        "auth = {auth}"
    );
    let signed_headers = auth
        .split("SignedHeaders=")
        .nth(1)
        .and_then(|s| s.split(',').next())
        .unwrap();
    for required in [
        "host",
        "x-amz-content-sha256",
        "x-amz-date",
        "content-type",
        "x-amz-meta-cairn-replica",
        "x-amz-meta-owner",
    ] {
        assert!(
            signed_headers.split(';').any(|h| h == required),
            "signed headers {signed_headers} missing {required}"
        );
    }
    // Signed-header list must be sorted.
    let names: Vec<&str> = signed_headers.split(';').collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "signed headers must be sorted");

    let signature = auth.split("Signature=").nth(1).unwrap();
    assert_eq!(signature.len(), 64, "signature must be 64 hex chars");
    assert!(signature.bytes().all(|b| b.is_ascii_hexdigit()));
}

/// A COMPOSITE multipart checksum-of-checksums (a `-N`-suffixed value) is NOT re-emitted on the
/// single-part replica PUT — the destination would reject it — while a whole-object FULL_OBJECT
/// checksum still ships so a checksum-mode GET of the replica matches the source.
#[tokio::test]
async fn composite_checksum_is_not_replicated_but_full_object_is() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured.clone(), Reply { status: 200 }).await;
    let sink = sink_for(&authority, 1_440_938_160);

    let object = ReplicatedObject {
        key: ObjectKey::parse("logs/app.log").unwrap(),
        version_id: VersionId::from_string("v1".to_owned()),
        content_type: "text/plain".to_owned(),
        user_metadata: Vec::new(),
        etag: ETag::from_string("\"abc-2\"".to_owned()),
        size: 5,
        tags: Vec::new(),
        acl: None,
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: cairn_types::object::StorageClass::Standard,
        checksums: vec![
            // A composite CRC32 (checksum-of-checksums over 2 parts) — must be skipped.
            ChecksumValue {
                algorithm: ChecksumAlgorithm::Crc32,
                value: "AAAAAA==-2".to_owned(),
            },
            // A whole-object SHA-256 — must still ship.
            ChecksumValue {
                algorithm: ChecksumAlgorithm::Sha256,
                value: "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=".to_owned(),
            },
        ],
        client_encrypted: false,
        body: body_stream(b"hello"),
    };

    sink.put_object(&src(), object).await.unwrap();

    let reqs = captured.lock().unwrap().clone();
    assert_eq!(reqs.len(), 1);
    let req = &reqs[0];
    assert_eq!(
        req.header("x-amz-checksum-crc32"),
        None,
        "a composite (-N) checksum must not be re-emitted to a single-part replica"
    );
    assert_eq!(
        req.header("x-amz-checksum-sha256"),
        Some("ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="),
        "a whole-object FULL_OBJECT checksum still replicates"
    );
}

/// The object ACL is replicated as a base64(JSON) `x-amz-meta-cairn-replica-acl` header when (and
/// only when) the replicated object carries one; it round-trips back to the same `Acl`.
#[tokio::test]
async fn put_object_emits_acl_header_only_when_present() {
    use base64::Engine as _;
    use cairn_types::authz::{Acl, Grant, Grantee, Permission};
    use cairn_types::id::UserId;

    let acl = Acl {
        owner: UserId("owner-1".to_owned()),
        grants: vec![
            Grant {
                grantee: Grantee::User(UserId("owner-1".to_owned())),
                permission: Permission::FullControl,
            },
            Grant {
                grantee: Grantee::AllUsers,
                permission: Permission::Read,
            },
        ],
    };

    // (a) With an ACL: the header is present and decodes back to the exact same Acl.
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured.clone(), Reply { status: 200 }).await;
    let sink = sink_for(&authority, 1_440_938_160);
    let object = ReplicatedObject {
        key: ObjectKey::parse("acl/obj").unwrap(),
        version_id: VersionId::from_string("v9".to_owned()),
        content_type: "application/octet-stream".to_owned(),
        user_metadata: Vec::new(),
        etag: ETag::from_string("\"e\"".to_owned()),
        size: 3,
        tags: Vec::new(),
        acl: Some(acl.clone()),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: cairn_types::object::StorageClass::Standard,
        checksums: Vec::new(),
        client_encrypted: false,
        body: body_stream(b"abc"),
    };
    sink.put_object(&src(), object).await.unwrap();
    let reqs = captured.lock().unwrap().clone();
    let hdr = reqs[0]
        .header("x-amz-meta-cairn-replica-acl")
        .expect("ACL header must be present when the object has an ACL");
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(hdr)
        .expect("ACL header is valid base64");
    let decoded: Acl = serde_json::from_slice(&bytes).expect("ACL header is valid JSON");
    assert_eq!(
        decoded, acl,
        "the replicated ACL round-trips through the header"
    );

    // (b) Without an ACL: no header at all.
    let captured2 = Arc::new(Mutex::new(Vec::new()));
    let authority2 = spawn_server(captured2.clone(), Reply { status: 200 }).await;
    let sink2 = sink_for(&authority2, 1_440_938_160);
    let object2 = ReplicatedObject {
        key: ObjectKey::parse("noacl/obj").unwrap(),
        version_id: VersionId::from_string("v10".to_owned()),
        content_type: "application/octet-stream".to_owned(),
        user_metadata: Vec::new(),
        etag: ETag::from_string("\"e\"".to_owned()),
        size: 0,
        tags: Vec::new(),
        acl: None,
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: cairn_types::object::StorageClass::Standard,
        checksums: Vec::new(),
        client_encrypted: false,
        body: body_stream(b""),
    };
    sink2.put_object(&src(), object2).await.unwrap();
    let reqs2 = captured2.lock().unwrap().clone();
    assert_eq!(
        reqs2[0].header("x-amz-meta-cairn-replica-acl"),
        None,
        "no ACL on the object → no ACL header"
    );
}

#[tokio::test]
async fn put_object_recomputes_signature_when_a_header_changes() {
    // The signature must actually depend on the request: a different key produces a different
    // signature, proving the canonical request is being signed (not a constant).
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured.clone(), Reply { status: 200 }).await;
    let sink = sink_for(&authority, 1_440_938_160);

    let make = |key: &'static str| ReplicatedObject {
        key: ObjectKey::parse(key).unwrap(),
        version_id: VersionId::from_string("v1".to_owned()),
        content_type: "text/plain".to_owned(),
        user_metadata: Vec::new(),
        etag: ETag::from_string("\"e\"".to_owned()),
        size: 5,
        tags: Vec::new(),
        acl: None,
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: cairn_types::object::StorageClass::Standard,
        checksums: Vec::new(),
        client_encrypted: false,
        body: body_stream(b"hello"),
    };

    sink.put_object(&src(), make("a")).await.unwrap();
    sink.put_object(&src(), make("b")).await.unwrap();

    let reqs = captured.lock().unwrap().clone();
    let sig = |r: &Captured| {
        r.header("authorization")
            .unwrap()
            .split("Signature=")
            .nth(1)
            .unwrap()
            .to_owned()
    };
    assert_ne!(sig(&reqs[0]), sig(&reqs[1]));
}

#[tokio::test]
async fn delete_marker_issues_signed_delete() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured.clone(), Reply { status: 204 }).await;
    let sink = sink_for(&authority, 1_440_938_160);

    sink.delete_marker(
        &src(),
        &ObjectKey::parse("logs/app.log").unwrap(),
        &VersionId::from_string("v9".to_owned()),
    )
    .await
    .unwrap();

    let reqs = captured.lock().unwrap().clone();
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].method, "DELETE");
    assert_eq!(reqs[0].path, "/dest-bucket/logs/app.log");
    assert!(reqs[0].header("authorization").is_some());
    assert!(reqs[0].body.is_empty());
}

#[tokio::test]
async fn server_5xx_is_unavailable() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured, Reply { status: 503 }).await;
    let sink = sink_for(&authority, 1_440_938_160);

    let object = ReplicatedObject {
        key: ObjectKey::parse("k").unwrap(),
        version_id: VersionId::from_string("v1".to_owned()),
        content_type: "application/octet-stream".to_owned(),
        user_metadata: Vec::new(),
        etag: ETag::from_string("\"e\"".to_owned()),
        size: 1,
        tags: Vec::new(),
        acl: None,
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: cairn_types::object::StorageClass::Standard,
        checksums: Vec::new(),
        client_encrypted: false,
        body: body_stream(b"x"),
    };
    let err = sink.put_object(&src(), object).await.unwrap_err();
    assert!(
        matches!(err, cairn_types::error::ReplicationError::Unavailable(_)),
        "503 means the destination is unavailable (retry without burning the budget), got {err:?}"
    );
}

#[tokio::test]
async fn server_4xx_is_terminal() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured, Reply { status: 403 }).await;
    let sink = sink_for(&authority, 1_440_938_160);

    let err = sink
        .delete_marker(
            &src(),
            &ObjectKey::parse("k").unwrap(),
            &VersionId::from_string("v1".to_owned()),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, cairn_types::error::ReplicationError::Terminal(_)),
        "403 should be terminal, got {err:?}"
    );
}

#[tokio::test]
async fn put_object_routes_to_per_source_destination_bucket() {
    // A sink configured with a source -> dest map must address the request to the destination
    // bucket resolved from the *source* bucket, and fall back to the default for unmapped
    // sources. The wire path is /{dest-bucket}/{key}.
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured.clone(), Reply { status: 200 }).await;

    let mut dest_buckets = std::collections::HashMap::new();
    dest_buckets.insert("alpha-src".to_owned(), "alpha-dst".to_owned());
    let sink = HttpS3Sink::with_clock(
        S3SinkConfig {
            endpoint: format!("http://{authority}"),
            dest_bucket: "fallback-dst".to_owned(),
            dest_buckets,
            region: "us-east-1".to_owned(),
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_owned(),
            ca_cert_path: None,
            ca_cert_pem: None,
            insecure_skip_verify: false,
            // The mock S3 server runs on loopback, so opt out of the SSRF guard for these tests.
            allow_internal_endpoints: true,
            allow_plaintext_sse_over_http: false,
        },
        Arc::new(FixedClock(1_440_938_160)),
    )
    .unwrap();

    let make = |key: &'static str| ReplicatedObject {
        key: ObjectKey::parse(key).unwrap(),
        version_id: VersionId::from_string("v1".to_owned()),
        content_type: "text/plain".to_owned(),
        user_metadata: Vec::new(),
        etag: ETag::from_string("\"e\"".to_owned()),
        size: 1,
        tags: Vec::new(),
        acl: None,
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: cairn_types::object::StorageClass::Standard,
        checksums: Vec::new(),
        client_encrypted: false,
        body: body_stream(b"x"),
    };

    // Mapped source routes to its destination bucket.
    sink.put_object(&BucketName::parse("alpha-src").unwrap(), make("k1"))
        .await
        .unwrap();
    // Unmapped source falls back to the default destination bucket.
    sink.put_object(&BucketName::parse("beta-src").unwrap(), make("k2"))
        .await
        .unwrap();
    // A delete marker from a mapped source also routes per source.
    sink.delete_marker(
        &BucketName::parse("alpha-src").unwrap(),
        &ObjectKey::parse("k3").unwrap(),
        &VersionId::from_string("v2".to_owned()),
    )
    .await
    .unwrap();

    let reqs = captured.lock().unwrap().clone();
    assert_eq!(reqs.len(), 3);
    assert_eq!(reqs[0].path, "/alpha-dst/k1");
    assert_eq!(reqs[1].path, "/fallback-dst/k2");
    assert_eq!(reqs[2].method, "DELETE");
    assert_eq!(reqs[2].path, "/alpha-dst/k3");
}

#[tokio::test]
async fn https_endpoint_negotiates_tls_not_plaintext() {
    // An https:// sink must drive the request over TLS through the wired hyper-rustls connector.
    // We point it at a plain-TCP listener that, on accept, immediately reads a few bytes and
    // closes: a TLS client opens with a ClientHello (the TLS record type byte 0x16), whereas a
    // plaintext HTTP client would send ASCII (`PUT ...`). Capturing the first byte proves the
    // connector actually negotiated TLS for the https scheme rather than falling back to
    // plaintext. The handshake then fails (no server cert), surfacing a Retryable transport
    // error — which also confirms the request was attempted, not rejected at construction.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let authority = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let first_byte = Arc::new(Mutex::new(None::<u8>));

    let fb = first_byte.clone();
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0u8; 1];
            if stream.read_exact(&mut buf).await.is_ok() {
                *fb.lock().unwrap() = Some(buf[0]);
            }
            // Drop the connection so the client's handshake fails fast.
        }
    });

    let sink = HttpS3Sink::with_clock(
        S3SinkConfig {
            endpoint: format!("https://{authority}"),
            dest_bucket: "dest-bucket".to_owned(),
            dest_buckets: std::collections::HashMap::new(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_owned(),
            ca_cert_path: None,
            ca_cert_pem: None,
            insecure_skip_verify: false,
            // The mock S3 server runs on loopback, so opt out of the SSRF guard for these tests.
            allow_internal_endpoints: true,
            allow_plaintext_sse_over_http: false,
        },
        Arc::new(FixedClock(1_440_938_160)),
    )
    .unwrap();

    let object = ReplicatedObject {
        key: ObjectKey::parse("k").unwrap(),
        version_id: VersionId::from_string("v1".to_owned()),
        content_type: "text/plain".to_owned(),
        user_metadata: Vec::new(),
        etag: ETag::from_string("\"e\"".to_owned()),
        size: 1,
        tags: Vec::new(),
        acl: None,
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: cairn_types::object::StorageClass::Standard,
        checksums: Vec::new(),
        client_encrypted: false,
        body: body_stream(b"x"),
    };
    // The handshake fails (server presents no certificate), so the call errors as unavailable.
    let err = sink.put_object(&src(), object).await.unwrap_err();
    assert!(
        matches!(err, cairn_types::error::ReplicationError::Unavailable(_)),
        "a failed TLS handshake is a transport error (target unavailable), got {err:?}"
    );

    // The byte the client first put on the wire is a TLS handshake record (0x16), proving the
    // connector negotiated TLS for the https scheme rather than speaking plaintext HTTP.
    let observed = *first_byte.lock().unwrap();
    assert_eq!(
        observed,
        Some(0x16),
        "https endpoint must open with a TLS ClientHello (0x16), got {observed:?}"
    );
}

/// Audit 2026-07: the replica must carry the source's system response headers, storage class, and
/// supplementary checksums (AWS CRR preserves these) — most sharply Content-Encoding.
#[tokio::test]
async fn put_object_replicates_system_headers_and_checksums() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured.clone(), Reply { status: 200 }).await;
    let sink = sink_for(&authority, 1_440_938_160);
    let object = ReplicatedObject {
        key: ObjectKey::parse("k").unwrap(),
        version_id: VersionId::from_string("v1".to_owned()),
        content_type: "text/plain".to_owned(),
        user_metadata: Vec::new(),
        etag: ETag::from_string("\"abc\"".to_owned()),
        size: 5,
        tags: Vec::new(),
        acl: None,
        content_encoding: Some("gzip".to_owned()),
        cache_control: Some("max-age=60".to_owned()),
        content_disposition: Some("attachment".to_owned()),
        content_language: Some("en".to_owned()),
        expires: Some("Wed, 21 Oct 2026 07:28:00 GMT".to_owned()),
        storage_class: cairn_types::object::StorageClass::ColdTier,
        checksums: vec![cairn_types::object::ChecksumValue {
            algorithm: cairn_types::object::ChecksumAlgorithm::Sha256,
            value: "aGFzaA==".to_owned(),
        }],
        client_encrypted: false,
        body: body_stream(b"hello"),
    };
    sink.put_object(&src(), object).await.unwrap();
    let reqs = captured.lock().unwrap().clone();
    let req = &reqs[0];
    assert_eq!(req.header("content-encoding"), Some("gzip"));
    assert_eq!(req.header("cache-control"), Some("max-age=60"));
    assert_eq!(req.header("content-disposition"), Some("attachment"));
    assert_eq!(req.header("content-language"), Some("en"));
    assert_eq!(req.header("expires"), Some("Wed, 21 Oct 2026 07:28:00 GMT"));
    assert_eq!(req.header("x-amz-storage-class"), Some("GLACIER"));
    assert_eq!(req.header("x-amz-checksum-sha256"), Some("aGFzaA=="));
}

// --- client-encrypted objects over a plaintext endpoint -------------------------------------
//
// Replication ships the DECRYPTED body (it must: the stored ciphertext is unreadable at the
// destination). For an object the client asked us to encrypt, sending that plaintext to an
// `http://` endpoint is new exposure created by the DEK fix itself — before it, such an object
// either never replicated or replicated as ciphertext. It is therefore gated.

/// Build a `ReplicatedObject` carrying `client_encrypted`.
fn encrypted_object(client_encrypted: bool) -> ReplicatedObject {
    ReplicatedObject {
        key: ObjectKey::parse("secret.txt").unwrap(),
        version_id: VersionId::from_string("v1".to_owned()),
        content_type: "text/plain".to_owned(),
        user_metadata: Vec::new(),
        etag: ETag::from_string("\"abc\"".to_owned()),
        size: 5,
        tags: Vec::new(),
        acl: None,
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: cairn_types::object::StorageClass::Standard,
        checksums: Vec::new(),
        client_encrypted,
        body: body_stream(b"hello"),
    }
}

#[tokio::test]
async fn client_encrypted_object_is_refused_over_http_and_never_dials() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured.clone(), Reply { status: 200 }).await;
    let sink = sink_for(&authority, 1_440_938_160);

    let err = sink
        .put_object(&src(), encrypted_object(true))
        .await
        .expect_err("a client-encrypted object must not be shipped in the clear");

    // Unavailable, NOT Terminal: this is an operator-fixable configuration condition, and
    // `Unavailable` reschedules without consuming the attempt budget — so the object ships by
    // itself once the endpoint becomes https:// or the opt-in is set, with no operator retry and
    // no bucket stamped permanently failed.
    let msg = match &err {
        ReplicationError::Unavailable(m) => m.clone(),
        other => panic!("must be Unavailable (rescheduled, budget preserved), got {other:?}"),
    };
    // The message must name the REAL cause. "target unavailable" would send an operator to a
    // destination that is perfectly healthy.
    assert!(msg.contains("client-encrypted"), "{msg}");
    assert!(msg.contains("http://"), "{msg}");
    assert!(
        msg.contains("CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP"),
        "the message must name the opt-in: {msg}"
    );
    // Fail closed: refused BEFORE the body is buffered or the endpoint dialled.
    assert!(
        captured.lock().unwrap().is_empty(),
        "nothing may reach the wire"
    );
}

#[tokio::test]
async fn a_non_client_encrypted_object_still_ships_over_http() {
    // At-rest-encrypted and unencrypted objects are `client_encrypted: false` and must be
    // completely unaffected — gating them would break every existing plaintext-endpoint
    // deployment for no security gain (at-rest is an operator storage property, not a client
    // contract).
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured.clone(), Reply { status: 200 }).await;
    let sink = sink_for(&authority, 1_440_938_160);
    sink.put_object(&src(), encrypted_object(false))
        .await
        .expect("an unflagged object ships over http exactly as before");
    assert_eq!(captured.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn the_opt_in_allows_a_client_encrypted_object_over_http() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured.clone(), Reply { status: 200 }).await;
    // The same rig as `sink_for`, but with the operator opt-in set.
    let cfg_sink = HttpS3Sink::with_clock(
        S3SinkConfig {
            endpoint: format!("http://{authority}"),
            dest_bucket: "dest-bucket".to_owned(),
            dest_buckets: std::collections::HashMap::new(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_owned(),
            ca_cert_path: None,
            ca_cert_pem: None,
            insecure_skip_verify: false,
            allow_internal_endpoints: true,
            allow_plaintext_sse_over_http: true,
        },
        Arc::new(FixedClock(1_440_938_160)),
    )
    .unwrap();
    cfg_sink
        .put_object(&src(), encrypted_object(true))
        .await
        .expect("the operator opted in");
    assert_eq!(captured.lock().unwrap().len(), 1);
}

/// Spawn a server that answers every request with `chunks`, emitted as SEPARATE body frames and
/// with no `Content-Length` (chunked transfer). Returns the bound authority.
///
/// The framing is the point: `stream_object` must hash what it is handed frame by frame and hold
/// none of it, and its byte cap must be enforced against what it has actually read rather than
/// against a header the destination controls.
async fn spawn_chunked_server(chunks: Vec<&'static [u8]>) -> String {
    use futures_util::stream;
    use http_body_util::StreamBody;
    use hyper::body::Frame;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let authority = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(async move {
        loop {
            let Ok((stream_io, _)) = listener.accept().await else {
                return;
            };
            let chunks = chunks.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream_io);
                let service = service_fn(move |_req: Request<Incoming>| {
                    let chunks = chunks.clone();
                    async move {
                        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> = chunks
                            .into_iter()
                            .map(|c: &'static [u8]| Ok(Frame::data(Bytes::from_static(c))))
                            .collect();
                        let frames = stream::iter(frames);
                        Ok::<_, std::convert::Infallible>(Response::new(StreamBody::new(frames)))
                    }
                });
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await;
            });
        }
    });
    authority
}

/// `--verify` compares an MD5, and a digest is incremental — so the replica body is hashed as it
/// arrives and NEVER buffered. This asserts the observable consequence: the caller sees the body one
/// frame at a time (not one 2 GiB `Bytes`), and the returned count is the bytes actually read.
#[tokio::test]
async fn stream_object_feeds_the_caller_frame_by_frame_without_buffering() {
    let authority = spawn_chunked_server(vec![b"alpha", b"beta", b"gamma"]).await;
    let sink = sink_for(&authority, 1_700_000_000);

    let mut seen: Vec<Vec<u8>> = Vec::new();
    let read = sink
        .stream_object("source-bucket", "k", 1_000, &mut |c: &[u8]| {
            seen.push(c.to_vec())
        })
        .await
        .expect("a 200 with a body streams");

    assert_eq!(read, 14, "the return value is the total bytes read");
    assert_eq!(
        seen,
        vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()],
        "each frame is handed over and dropped; nothing accumulates in the sink"
    );
}

/// The cap survives the switch to streaming, and it bounds the bytes actually READ rather than a
/// buffer size — the response above carries no `Content-Length`, so there is no header to trust.
/// A hostile or misconfigured destination that streams without end must terminate the audit, not
/// run until an operator kills it.
#[tokio::test]
async fn stream_object_stops_at_the_read_cap() {
    let authority = spawn_chunked_server(vec![b"alpha", b"beta", b"gamma"]).await;
    let sink = sink_for(&authority, 1_700_000_000);

    let mut read_back = 0u64;
    let err = sink
        .stream_object("source-bucket", "k", 6, &mut |c: &[u8]| {
            read_back += c.len() as u64
        })
        .await
        .expect_err("a body past the cap must error");
    assert!(
        matches!(err, ReplicationError::Terminal(_)),
        "over-cap is terminal (parked, not retried forever): {err:?}"
    );
    assert!(
        read_back <= 6,
        "the cap is checked BEFORE the frame is consumed, so at most `max_bytes` reach the caller"
    );
}

/// A 404 from the destination is the absent-replica population, and it must arrive as the
/// structural `NotFound` variant so the audit never has to sniff a message for digits.
#[tokio::test]
async fn stream_object_maps_a_404_to_not_found() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured, Reply { status: 404 }).await;
    let sink = sink_for(&authority, 1_700_000_000);

    let err = sink
        .stream_object("source-bucket", "k", 1_000, &mut |_: &[u8]| {})
        .await
        .expect_err("a 404 is an error");
    assert!(
        matches!(err, ReplicationError::NotFound(_)),
        "unexpected: {err:?}"
    );
}
