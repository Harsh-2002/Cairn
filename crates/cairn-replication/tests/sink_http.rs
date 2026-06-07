//! Integration tests for [`HttpS3Sink`] against a tiny in-process hyper server. The server
//! captures the request line and headers each call receives, so the tests can assert that the
//! sink emits a correctly-signed, correctly-shaped PUT/DELETE without contacting a real S3.

use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use cairn_types::error::BlobError;
use cairn_types::id::{ObjectKey, VersionId};
use cairn_types::object::ETag;
use cairn_types::replication::ReplicatedObject;
use cairn_types::time::Timestamp;
use cairn_types::traits::{Clock, ReplicationSink};

use cairn_replication::{HttpS3Sink, S3SinkConfig};

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
            region: "us-east-1".to_owned(),
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_owned(),
        },
        Arc::new(FixedClock(clock_secs)),
    )
    .unwrap()
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
        body: body_stream(b"hello"),
    };

    sink.put_object(object).await.unwrap();

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
        body: body_stream(b"hello"),
    };

    sink.put_object(make("a")).await.unwrap();
    sink.put_object(make("b")).await.unwrap();

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
async fn server_5xx_is_retryable() {
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
        body: body_stream(b"x"),
    };
    let err = sink.put_object(object).await.unwrap_err();
    assert!(
        matches!(err, cairn_types::error::ReplicationError::Retryable(_)),
        "503 should be retryable, got {err:?}"
    );
}

#[tokio::test]
async fn server_4xx_is_terminal() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let authority = spawn_server(captured, Reply { status: 403 }).await;
    let sink = sink_for(&authority, 1_440_938_160);

    let err = sink
        .delete_marker(
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
