//! End-to-end tests of the S3 service against the REAL backends (SQLite metadata + filesystem
//! blob), exercising the core bucket/object lifecycle including a SigV4-streaming (chunked) PUT.

use bytes::Bytes;
use cairn_protocol::{S3Body, S3Request, S3Response, S3Service};
use cairn_types::auth::{AuthMethod, Principal, Role};
use cairn_types::id::{BucketName, ObjectKey, UserId};
use cairn_types::traits::{BlobStore, MetadataStore};
use http::{Method, StatusCode};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

fn admin() -> Principal {
    Principal {
        user_id: UserId("admin".to_owned()),
        display_name: "admin".to_owned(),
        access_key_id: "k".to_owned(),
        role: Role::Administrator,
        method: AuthMethod::Bearer,
        chunk_signing: None,
        user_policy: None,
        is_session: false,
    }
}

/// A non-admin member principal identified by `user`; buckets it creates are owned by it.
fn member(user: &str) -> Principal {
    Principal {
        user_id: UserId(user.to_owned()),
        display_name: user.to_owned(),
        access_key_id: format!("{user}-key"),
        role: Role::Member,
        method: AuthMethod::Bearer,
        chunk_signing: None,
        user_policy: None,
        is_session: false,
    }
}

struct Harness {
    svc: S3Service,
    meta: Arc<dyn MetadataStore>,
    _dir: tempfile::TempDir,
}

async fn harness() -> Harness {
    harness_with_authz(Arc::new(cairn_types::testing::AllowAll)).await
}

async fn harness_with_authz(authz: Arc<dyn cairn_types::traits::AuthorizationEngine>) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> = Arc::new(cairn_meta::open_in_memory().unwrap());
    let blob: Arc<dyn BlobStore> =
        Arc::new(cairn_blob::LocalBlobStore::open(dir.path()).await.unwrap());
    let clock = Arc::new(cairn_types::testing::TestClock::default());
    let crypto: Arc<dyn cairn_types::traits::Crypto> =
        Arc::new(cairn_crypto::SystemCrypto::new([7u8; 32]));
    let svc = S3Service::new(
        meta.clone(),
        blob,
        authz,
        clock,
        crypto,
        "us-east-1".to_owned(),
        5 * 1024 * 1024 * 1024,
    );
    Harness {
        svc,
        meta,
        _dir: dir,
    }
}

/// A harness whose metadata store is a [`ShardedMetadataStore`] over `shards` in-memory shards, to
/// exercise the protocol layer end-to-end under metadata sharding.
async fn harness_sharded(shards: usize) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let inner: Vec<Arc<dyn MetadataStore>> = (0..shards)
        .map(|_| Arc::new(cairn_meta::open_in_memory().unwrap()) as Arc<dyn MetadataStore>)
        .collect();
    let meta: Arc<dyn MetadataStore> = Arc::new(cairn_meta::ShardedMetadataStore::new(inner));
    let blob: Arc<dyn BlobStore> =
        Arc::new(cairn_blob::LocalBlobStore::open(dir.path()).await.unwrap());
    let clock = Arc::new(cairn_types::testing::TestClock::default());
    let crypto: Arc<dyn cairn_types::traits::Crypto> =
        Arc::new(cairn_crypto::SystemCrypto::new([7u8; 32]));
    let svc = S3Service::new(
        meta.clone(),
        blob,
        Arc::new(cairn_types::testing::AllowAll),
        clock,
        crypto,
        "us-east-1".to_owned(),
        5 * 1024 * 1024 * 1024,
    );
    Harness {
        svc,
        meta,
        _dir: dir,
    }
}

/// A harness whose service has transparent at-rest encryption (`CAIRN_ENCRYPT_AT_REST`) enabled, so
/// every committed object with no SSE header / bucket default is still stored encrypted (`AtRest`).
async fn harness_encrypt_at_rest() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> = Arc::new(cairn_meta::open_in_memory().unwrap());
    let blob: Arc<dyn BlobStore> =
        Arc::new(cairn_blob::LocalBlobStore::open(dir.path()).await.unwrap());
    let clock = Arc::new(cairn_types::testing::TestClock::default());
    let crypto: Arc<dyn cairn_types::traits::Crypto> =
        Arc::new(cairn_crypto::SystemCrypto::new([7u8; 32]));
    let svc = S3Service::new(
        meta.clone(),
        blob,
        Arc::new(cairn_types::testing::AllowAll),
        clock,
        crypto,
        "us-east-1".to_owned(),
        5 * 1024 * 1024 * 1024,
    )
    .with_encrypt_at_rest(true);
    Harness {
        svc,
        meta,
        _dir: dir,
    }
}

fn req(
    method: Method,
    bucket: Option<&str>,
    key: Option<&str>,
    query: &[(&str, &str)],
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> (S3Request, cairn_types::BodyStream) {
    req_with_principal(method, bucket, key, query, headers, body, admin())
}

fn req_with_principal(
    method: Method,
    bucket: Option<&str>,
    key: Option<&str>,
    query: &[(&str, &str)],
    headers: &[(&str, &str)],
    body: Vec<u8>,
    principal: Principal,
) -> (S3Request, cairn_types::BodyStream) {
    let request = S3Request {
        method,
        bucket: bucket.map(|b| BucketName::parse(b).unwrap()),
        key: key.map(|k| ObjectKey::parse(k).unwrap()),
        query: query
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        headers: headers
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        principal: Some(principal),
        source: IpAddr::V4(Ipv4Addr::LOCALHOST),
        secure: false,
        request_id: "req-1".to_owned(),
    };
    let body: cairn_types::BodyStream =
        Box::pin(futures_util::stream::once(
            async move { Ok(Bytes::from(body)) },
        ));
    (request, body)
}

async fn send(svc: &S3Service, parts: (S3Request, cairn_types::BodyStream)) -> S3Response {
    svc.handle(parts.0, parts.1).await
}

async fn drain(resp: S3Response) -> (StatusCode, Vec<(String, String)>, Vec<u8>) {
    use futures_util::StreamExt;
    let body = match resp.body {
        S3Body::Empty => Vec::new(),
        S3Body::Bytes(b) => b.to_vec(),
        S3Body::Stream { mut stream, .. } | S3Body::ZeroCopy { mut stream, .. } => {
            let mut out = Vec::new();
            while let Some(c) = stream.next().await {
                out.extend_from_slice(&c.unwrap());
            }
            out
        }
    };
    (resp.status, resp.headers, body)
}

fn header<'a>(h: &'a [(String, String)], name: &str) -> Option<&'a str> {
    h.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

#[tokio::test]
async fn full_object_lifecycle() {
    let h = harness().await;

    // Create bucket.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("my-bucket"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Duplicate create -> 409.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("my-bucket"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);

    // PUT object (plain body).
    let put = req(
        Method::PUT,
        Some("my-bucket"),
        Some("docs/a.txt"),
        &[],
        &[("content-type", "text/plain")],
        b"hello world".to_vec(),
    );
    let (st, hdrs, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "etag"),
        Some("\"5eb63bbbe01eeed093cb22bb8f5acdc3\"")
    );

    // GET it back.
    let (st, hdrs, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, b"hello world");
    assert_eq!(header(&hdrs, "content-type"), Some("text/plain"));
    assert_eq!(header(&hdrs, "content-length"), Some("11"));

    // HEAD.
    let (st, hdrs, body) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(header(&hdrs, "content-length"), Some("11"));

    // Ranged GET -> 206.
    let (st, hdrs, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[("range", "bytes=0-4")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, b"hello");
    assert_eq!(header(&hdrs, "content-range"), Some("bytes 0-4/11"));

    // Conditional If-None-Match with the current ETag -> 304.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[("if-none-match", "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_MODIFIED);

    // LIST objects (v2).
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("my-bucket"),
                None,
                &[("list-type", "2")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(xml.contains("<Key>docs/a.txt</Key>"), "listing: {xml}");

    // DELETE.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);

    // GET after delete -> 404.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn streaming_chunked_put_is_deframed() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("strm"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // Build an aws-chunked body carrying "the quick brown fox" across two chunks.
    let payload = b"the quick brown fox";
    let mut chunked = Vec::new();
    chunked.extend_from_slice(format!("{:x}\r\n", 9).as_bytes());
    chunked.extend_from_slice(&payload[..9]);
    chunked.extend_from_slice(b"\r\n");
    chunked.extend_from_slice(format!("{:x}\r\n", payload.len() - 9).as_bytes());
    chunked.extend_from_slice(&payload[9..]);
    chunked.extend_from_slice(b"\r\n0\r\n\r\n");

    let put = req(
        Method::PUT,
        Some("strm"),
        Some("obj"),
        &[],
        &[
            // Unsigned streaming: de-frame without verifying a per-chunk signature chain.
            ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD"),
            ("content-type", "text/plain"),
        ],
        chunked,
    );
    let (st, hdrs, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK);
    // ETag is the MD5 of the DE-FRAMED plaintext, not the framed body.
    let want_etag = format!("\"{}\"", md5_hex(payload));
    assert_eq!(header(&hdrs, "etag"), Some(want_etag.as_str()));

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("strm"), Some("obj"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        body, payload,
        "streaming body must be de-framed to the original payload"
    );
}

/// `aws-chunked` is a TRANSFER coding describing the request-body framing, not a content coding of
/// the stored object. AWS strips it before storing; Cairn used to persist it verbatim, so every
/// GET advertised a framing the response body does not use — and the replication sink forwarded
/// the lie downstream (audit 2026-07).
#[tokio::test]
async fn aws_chunked_is_stripped_from_stored_content_encoding() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("cencode"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // A one-chunk aws-chunked framing of `payload`, reused by every streaming case below.
    let payload = b"the quick brown fox";
    let framed = || {
        let mut c = Vec::new();
        c.extend_from_slice(format!("{:x}\r\n", payload.len()).as_bytes());
        c.extend_from_slice(payload);
        c.extend_from_slice(b"\r\n0\r\n\r\n");
        c
    };

    // (key, sent content-encoding, expected stored/echoed value, streaming?)
    let cases: [(&str, &str, Option<&str>, bool); 4] = [
        // The exact boto3 flexible-checksum shape: the real coding survives, the token does not.
        ("mixed", "gzip,aws-chunked", Some("gzip"), true),
        // aws-chunked alone leaves NOTHING to store: the header must be absent, not empty.
        ("only", "aws-chunked", None, true),
        // Case-insensitive token match; the other codings keep their order.
        ("multi", "gzip, AWS-Chunked, br", Some("gzip, br"), true),
        // The verbatim fast path must not regress.
        ("plain", "gzip", Some("gzip"), false),
    ];
    for (key, sent, want, streaming) in cases {
        let mut headers: Vec<(&str, &str)> = vec![("content-encoding", sent)];
        let body = if streaming {
            headers.push(("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD"));
            framed()
        } else {
            payload.to_vec()
        };
        let (st, _, _) = drain(
            send(
                &h.svc,
                req(Method::PUT, Some("cencode"), Some(key), &[], &headers, body),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "PUT {key} must still succeed");
        let (st, hdrs, got) = drain(
            send(
                &h.svc,
                req(Method::GET, Some("cencode"), Some(key), &[], &[], vec![]),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(
            header(&hdrs, "content-encoding"),
            want,
            "stored content-encoding for {key} (sent {sent:?})"
        );
        // Normalizing the header must not disturb the F-5 de-framing of the body.
        assert_eq!(got, payload, "body round-trip for {key}");
    }
}

/// Build a SigV4 signed `aws-chunked` body for `payloads` and the matching signed-streaming
/// context, using cairn-auth's signing primitives the way a real client would. The returned
/// context seeds the decoder's per-chunk chain; `tamper` flips a payload byte AFTER signing so
/// the chain no longer verifies.
fn signed_streaming(
    payloads: &[&[u8]],
    tamper: bool,
) -> (Vec<u8>, cairn_types::ChunkSigningContext) {
    use sha2::Digest;
    let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    let amzdate = "20260101T000000Z";
    let scope = "20260101/us-east-1/s3/aws4_request";
    let seed = "0000000000000000000000000000000000000000000000000000000000000000";
    let key = cairn_auth::streaming_signing_key(secret, "20260101", "us-east-1");

    let mut prev = seed.to_owned();
    let mut body = Vec::new();
    // The wire stream carries each payload chunk then a terminating zero-size chunk.
    let mut chunks: Vec<&[u8]> = payloads.to_vec();
    chunks.push(b"");
    for p in &chunks {
        let hash = hex::encode(sha2::Sha256::digest(p));
        let sts = cairn_auth::chunk_string_to_sign(amzdate, scope, &prev, &hash);
        let sig = cairn_auth::compute_signature(&key, &sts);
        body.extend_from_slice(format!("{:x};chunk-signature={}\r\n", p.len(), sig).as_bytes());
        body.extend_from_slice(p);
        body.extend_from_slice(b"\r\n");
        prev = sig;
    }
    body.extend_from_slice(b"\r\n"); // trailer terminator

    if tamper {
        // Corrupt the first payload byte: signed, but the chunk hash no longer matches.
        let first = payloads.first().copied().unwrap_or(b"");
        if let Some(pos) = body.windows(first.len()).position(|w| w == first) {
            body[pos] ^= 0xff;
        }
    }

    let ctx = cairn_types::ChunkSigningContext {
        seed_signature: seed.to_owned(),
        signing_key: key,
        amz_date: amzdate.to_owned(),
        scope: scope.to_owned(),
    };
    (body, ctx)
}

fn signed_streaming_principal(ctx: cairn_types::ChunkSigningContext) -> Principal {
    let mut p = admin();
    p.chunk_signing = Some(ctx);
    p
}

#[tokio::test]
async fn signed_streaming_put_with_valid_chain_succeeds() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("sgn"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    let payload: &[u8] = b"the quick brown fox jumps over the lazy dog";
    let (body, ctx) = signed_streaming(&[payload], false);
    let put = req_with_principal(
        Method::PUT,
        Some("sgn"),
        Some("ok"),
        &[],
        &[
            ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
            ("content-type", "application/octet-stream"),
        ],
        body,
        signed_streaming_principal(ctx),
    );
    let (st, hdrs, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK, "valid signed chain must be accepted");
    assert_eq!(
        header(&hdrs, "etag"),
        Some(format!("\"{}\"", md5_hex(payload)).as_str())
    );

    // And the de-framed plaintext round-trips.
    let (st, _, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("sgn"), Some("ok"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(got, payload);
}

#[tokio::test]
async fn signed_streaming_put_with_wrong_chunk_signature_is_rejected() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("bad"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    let payload: &[u8] = b"the quick brown fox jumps over the lazy dog";
    let (body, ctx) = signed_streaming(&[payload], true);
    let put = req_with_principal(
        Method::PUT,
        Some("bad"),
        Some("tampered"),
        &[],
        &[
            ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
            ("content-type", "application/octet-stream"),
        ],
        body,
        signed_streaming_principal(ctx),
    );
    let (st, _, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "a tampered signed-streaming chunk must be rejected"
    );

    // The object must NOT have been stored.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("bad"), Some("tampered"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NOT_FOUND,
        "tampered upload must not be stored"
    );
}

#[tokio::test]
async fn signed_streaming_sentinel_without_context_is_rejected() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("noctx"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // A signed sentinel but no SigV4 streaming context on the principal (e.g. it never went
    // through the header path) is invalid and must be refused before any bytes are stored.
    let (body, _ctx) = signed_streaming(&[b"data"], false);
    let put = req_with_principal(
        Method::PUT,
        Some("noctx"),
        Some("k"),
        &[],
        &[("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")],
        body,
        admin(), // chunk_signing is None
    );
    let (st, _, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::FORBIDDEN);
}

// The test harness clock is fixed at 1_700_000_000s = Tue, 14 Nov 2023 22:13:20 GMT, so an
// object PUT through it has that last-modified. These bracket it for time-based conditionals.
const BEFORE_LM: &str = "Sat, 01 Jan 2022 00:00:00 GMT";
const AFTER_LM: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

/// PUT a small object into a fresh bucket, returning its quoted ETag.
async fn put_simple(h: &Harness, bucket: &str, key: &str) -> String {
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some(bucket), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some(bucket),
                Some(key),
                &[],
                &[],
                b"conditional".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    header(&hdrs, "etag").unwrap().to_owned()
}

#[tokio::test]
async fn conditional_get_modified_since_returns_304() {
    let h = harness().await;
    put_simple(&h, "cond", "obj").await;

    // Not modified since a date AFTER last-modified => 304.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("cond"),
                Some("obj"),
                &[],
                &[("if-modified-since", AFTER_LM)],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_MODIFIED);

    // Modified since a date BEFORE last-modified => normal 200.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("cond"),
                Some("obj"),
                &[],
                &[("if-modified-since", BEFORE_LM)],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, b"conditional");
}

#[tokio::test]
async fn conditional_get_unmodified_since_returns_412() {
    let h = harness().await;
    put_simple(&h, "cond2", "obj").await;

    // The object WAS modified after this date => If-Unmodified-Since fails => 412.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("cond2"),
                Some("obj"),
                &[],
                &[("if-unmodified-since", BEFORE_LM)],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED);
}

#[tokio::test]
async fn conditional_head_returns_304_and_412() {
    let h = harness().await;
    let etag = put_simple(&h, "cond3", "obj").await;

    // HEAD with If-None-Match matching the ETag => 304.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("cond3"),
                Some("obj"),
                &[],
                &[("if-none-match", etag.as_str())],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_MODIFIED, "conditional HEAD must 304");

    // HEAD with If-Match not matching => 412.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("cond3"),
                Some("obj"),
                &[],
                &[("if-match", "\"deadbeefdeadbeefdeadbeefdeadbeef\"")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::PRECONDITION_FAILED,
        "conditional HEAD must 412"
    );

    // HEAD with If-Modified-Since after last-modified => 304.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("cond3"),
                Some("obj"),
                &[],
                &[("if-modified-since", AFTER_LM)],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn copy_source_if_match_and_modified_since_preconditions() {
    let h = harness().await;
    let etag = put_simple(&h, "src", "k").await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("dst"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // Copy with a non-matching x-amz-copy-source-if-match => 412.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("dst"),
                Some("c1"),
                &[],
                &[
                    ("x-amz-copy-source", "/src/k"),
                    (
                        "x-amz-copy-source-if-match",
                        "\"00000000000000000000000000000000\"",
                    ),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED);

    // Copy with a matching if-match and a satisfied if-unmodified-since => succeeds.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("dst"),
                Some("c2"),
                &[],
                &[
                    ("x-amz-copy-source", "/src/k"),
                    ("x-amz-copy-source-if-match", etag.as_str()),
                    ("x-amz-copy-source-if-unmodified-since", AFTER_LM),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Copy with x-amz-copy-source-if-modified-since AFTER last-modified (not modified) => 412.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("dst"),
                Some("c3"),
                &[],
                &[
                    ("x-amz-copy-source", "/src/k"),
                    ("x-amz-copy-source-if-modified-since", AFTER_LM),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED);
}

fn sha256_b64(data: &[u8]) -> String {
    use base64::Engine;
    use sha2::Digest;
    base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(data))
}

#[tokio::test]
async fn put_with_matching_checksum_succeeds() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("cks"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    let payload = b"checksum me please".to_vec();
    let want = sha256_b64(&payload);
    let put = req(
        Method::PUT,
        Some("cks"),
        Some("good"),
        &[],
        &[("x-amz-checksum-sha256", want.as_str())],
        payload,
    );
    let (st, _, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK, "matching checksum must be accepted");
}

#[tokio::test]
async fn put_with_mismatching_checksum_is_bad_digest_and_not_stored() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("cks2"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    let payload = b"checksum me please".to_vec();
    // A checksum computed over different bytes -> mismatch.
    let wrong = sha256_b64(b"some other content entirely");
    let put = req(
        Method::PUT,
        Some("cks2"),
        Some("bad"),
        &[],
        &[("x-amz-checksum-sha256", wrong.as_str())],
        payload,
    );
    let (st, _, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "mismatching checksum must be BadDigest"
    );

    // The object must not have been stored.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("cks2"), Some("bad"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

// A modern SDK (boto3 >=1.36, aws-cli v2, JS/Go/Java v2) sends a checksum by default and validates
// the round-trip by reading `x-amz-checksum-{algo}` off the response. Cairn must echo the stored
// checksum on the PUT response, and on GET/HEAD when the client opts in with
// `x-amz-checksum-mode: ENABLED` — but never on a partial (Range/206) GET, and never unprompted.
#[tokio::test]
async fn stored_checksum_is_echoed_on_put_get_head_but_not_on_range_or_unprompted() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("ckecho"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    let payload = b"the quick brown fox jumps over the lazy dog".to_vec();
    let want_sha = sha256_b64(&payload);

    // PUT carrying a SHA-256 checksum: the response echoes it + the checksum type.
    let put = req(
        Method::PUT,
        Some("ckecho"),
        Some("obj"),
        &[],
        &[("x-amz-checksum-sha256", want_sha.as_str())],
        payload.clone(),
    );
    let (st, hdrs, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-checksum-sha256"),
        Some(want_sha.as_str()),
        "PUT response must echo the computed checksum"
    );
    assert_eq!(header(&hdrs, "x-amz-checksum-type"), Some("FULL_OBJECT"));

    // GET with checksum mode enabled echoes the stored checksum.
    let get = req(
        Method::GET,
        Some("ckecho"),
        Some("obj"),
        &[],
        &[("x-amz-checksum-mode", "ENABLED")],
        vec![],
    );
    let (st, hdrs, body) = drain(send(&h.svc, get).await).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, payload);
    assert_eq!(
        header(&hdrs, "x-amz-checksum-sha256"),
        Some(want_sha.as_str())
    );
    assert_eq!(header(&hdrs, "x-amz-checksum-type"), Some("FULL_OBJECT"));

    // GET without the mode header must NOT leak the checksum (S3 only echoes on opt-in).
    let get_plain = req(Method::GET, Some("ckecho"), Some("obj"), &[], &[], vec![]);
    let (_, hdrs, _) = drain(send(&h.svc, get_plain).await).await;
    assert_eq!(header(&hdrs, "x-amz-checksum-sha256"), None);

    // HEAD with the mode header echoes it.
    let head = req(
        Method::HEAD,
        Some("ckecho"),
        Some("obj"),
        &[],
        &[("x-amz-checksum-mode", "ENABLED")],
        vec![],
    );
    let (st, hdrs, _) = drain(send(&h.svc, head).await).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-checksum-sha256"),
        Some(want_sha.as_str())
    );

    // A Range GET (206) must NOT echo a whole-object checksum — it would not match the slice.
    let ranged = req(
        Method::GET,
        Some("ckecho"),
        Some("obj"),
        &[],
        &[("x-amz-checksum-mode", "ENABLED"), ("range", "bytes=0-3")],
        vec![],
    );
    let (st, hdrs, _) = drain(send(&h.svc, ranged).await).await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(header(&hdrs, "x-amz-checksum-sha256"), None);
}

// CRC-64/NVME is the AWS CLI v2 / CRT default flexible checksum. Cairn must recognize it, compute it
// server-side when requested, and echo it consistently across PUT and GET.
#[tokio::test]
async fn crc64nvme_checksum_is_computed_and_echoed() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("ck64"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    let payload = b"123456789".to_vec(); // canonical CRC-64/NVME check-vector input
    // The SDK asks the server to compute the checksum via the selector header (no value to verify).
    let put = req(
        Method::PUT,
        Some("ck64"),
        Some("obj"),
        &[],
        &[("x-amz-sdk-checksum-algorithm", "CRC64NVME")],
        payload.clone(),
    );
    let (st, hdrs, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK);
    // 0xAE8B14860A799888 big-endian, base64-encoded.
    let put_crc = header(&hdrs, "x-amz-checksum-crc64nvme")
        .expect("CRC64NVME echoed on PUT")
        .to_owned();
    assert_eq!(put_crc, "rosUhgp5mIg=");

    let get = req(
        Method::GET,
        Some("ck64"),
        Some("obj"),
        &[],
        &[("x-amz-checksum-mode", "ENABLED")],
        vec![],
    );
    let (st, hdrs, body) = drain(send(&h.svc, get).await).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, payload);
    assert_eq!(
        header(&hdrs, "x-amz-checksum-crc64nvme"),
        Some(put_crc.as_str())
    );
}

// Count regular files under the blob-store root, skipping the `.staging` area. Metadata is
// in-memory in these tests, so every remaining file is an object blob — used to prove a rejected
// PUT leaves no orphan.
fn blob_file_count(root: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, n: &mut usize) {
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                if e.file_name() != ".staging" {
                    walk(&p, n);
                }
            } else {
                *n += 1;
            }
        }
    }
    let mut n = 0;
    walk(root, &mut n);
    n
}

// H1: a syntactically invalid Content-MD5 must be rejected (InvalidDigest / 400) BEFORE anything is
// staged, so it can never leave an orphaned durable blob. The status is 400 even with the bug; the
// discriminating assertion is that no blob file remains on disk.
#[tokio::test]
async fn put_with_invalid_content_md5_is_invalid_digest_and_leaks_no_blob() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("md5b"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // "!!not-valid-base64!!" contains characters outside the standard base64 alphabet.
    let put = req(
        Method::PUT,
        Some("md5b"),
        Some("obj"),
        &[],
        &[("content-md5", "!!not-valid-base64!!")],
        b"some payload".to_vec(),
    );
    let (st, _, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "invalid Content-MD5 must be InvalidDigest (400)"
    );
    assert_eq!(
        blob_file_count(h._dir.path()),
        0,
        "invalid Content-MD5 leaked a staged blob (H1)"
    );

    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("md5b"), Some("obj"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn between(s: &str, start: &str, end: &str) -> String {
    let i = s.find(start).expect("start tag") + start.len();
    let j = s[i..].find(end).expect("end tag") + i;
    s[i..j].to_owned()
}

#[tokio::test]
async fn multipart_lifecycle() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("mpb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // Initiate.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpb"),
                Some("big.bin"),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );

    // Part 1 must be >= 5 MiB; part 2 is the small tail.
    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = b"the-tail".to_vec();
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpb"),
                Some("big.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[],
                part1.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let etag1 = header(&hdrs, "etag").unwrap().to_owned();
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpb"),
                Some("big.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "2")],
                &[],
                part2.clone(),
            ),
        )
        .await,
    )
    .await;
    let etag2 = header(&hdrs, "etag").unwrap().to_owned();

    // Complete.
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpb"),
                Some("big.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        String::from_utf8(body).unwrap().contains("-2"),
        "multipart ETag has part-count suffix"
    );

    // The assembled object is the concatenation of the parts.
    let (st, _, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("mpb"), Some("big.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(got.len(), expected.len());
    assert_eq!(got, expected);
}

/// Audit 2026-07: an uploadId is scoped to its (bucket, key). Completing it against a different key
/// must be NoSuchUpload — never a silent write to the wrong path — and must not brick the upload.
#[tokio::test]
async fn complete_multipart_against_wrong_key_is_no_such_upload() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("mpb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    // Initiate for key "right.bin".
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpb"),
                Some("right.bin"),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );
    // Upload a single part (the last part may be under 5 MiB).
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpb"),
                Some("right.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[],
                b"hello".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let etag1 = header(&hdrs, "etag").unwrap().to_owned();
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part></CompleteMultipartUpload>"
    );

    // Complete against a DIFFERENT key with the same uploadId -> NoSuchUpload (404).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpb"),
                Some("wrong.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.clone().into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NOT_FOUND,
        "completing against the wrong key must be NoSuchUpload"
    );

    // The upload is NOT bricked: completing against the RIGHT key still succeeds.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpb"),
                Some("right.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "the upload stays completable against its real key"
    );
}

/// Initiate a multipart upload on `bucket`/`key` with one 1-part body, returning
/// `(upload_id, part_etag)`. Shared by the uploadId-scoping regressions below.
async fn start_upload_with_part(h: &Harness, bucket: &str, key: &str) -> (String, String) {
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some(bucket), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some(bucket),
                Some(key),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some(bucket),
                Some(key),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[],
                b"hello".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let etag = header(&hdrs, "etag").unwrap().to_owned();
    (upload_id, etag)
}

/// Audit 2026-07 (critical): `abort_multipart` never checked that the uploadId belonged to the
/// request path. Any principal who could write SOME path of their own could destroy another
/// tenant's in-flight upload — session row AND staged part bytes — by id alone.
#[tokio::test]
async fn abort_multipart_against_wrong_key_is_no_such_upload() {
    let h = harness().await;
    let (upload_id, etag1) = start_upload_with_part(&h, "mpb2", "right.bin").await;

    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("mpb2"),
                Some("wrong.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NOT_FOUND,
        "aborting against the wrong key must be NoSuchUpload"
    );

    // The victim's upload SURVIVED — the real security assertion. A fix that 404s but still
    // deletes the session and its staged bytes fails here.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mpb2"),
                Some("right.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("<PartNumber>1</PartNumber>"), "{body}");
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpb2"),
                Some("right.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "the staged part must still be there");
}

/// Aborting an unknown uploadId used to return 204: `Mutation::AbortMultipart` is an unconditional
/// DELETE that cannot distinguish "removed" from "never existed". AWS answers NoSuchUpload.
#[tokio::test]
async fn abort_multipart_with_unknown_upload_id_is_no_such_upload() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("mpb3"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("mpb3"),
                Some("some.bin"),
                &[("uploadId", "does-not-exist")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NOT_FOUND,
        "an unknown uploadId must be NoSuchUpload, not a silent 204"
    );
}

/// `list_parts` checked existence but not ownership, so an unscoped id rendered another key's part
/// numbers, sizes and ETags (= per-part content MD5s) under the caller's own path.
#[tokio::test]
async fn list_parts_against_wrong_key_is_no_such_upload() {
    let h = harness().await;
    let (upload_id, _) = start_upload_with_part(&h, "mpb4", "right.bin").await;

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mpb4"),
                Some("wrong.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let body = String::from_utf8(body).unwrap();
    assert!(
        !body.contains("<PartNumber>"),
        "no part metadata may leak even in the error body: {body}"
    );

    // Not over-tight: the real path still lists.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mpb4"),
                Some("right.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        String::from_utf8(body)
            .unwrap()
            .contains("<PartNumber>1</PartNumber>")
    );
}

/// `upload_part` (and `upload_part_copy`) checked existence but not ownership, and
/// `Mutation::RecordPart` SUPERSEDES the part at that number — so an unscoped id let a caller
/// corrupt another key's in-flight upload with bytes of their choosing.
#[tokio::test]
async fn upload_part_against_wrong_key_is_no_such_upload() {
    let h = harness().await;
    let (upload_id, etag1) = start_upload_with_part(&h, "mpb5", "right.bin").await;

    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpb5"),
                Some("wrong.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[],
                b"evil".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // The victim's part was NOT superseded — a 404-but-still-recorded fix fails here.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mpb5"),
                Some("right.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    // The listing renders the part ETag quoted (XML-escaped); compare the bare hex.
    let want = etag1.trim_matches('"').to_owned();
    assert!(
        body.contains(&want),
        "part 1 was overwritten by the cross-key upload (want {want}): {body}"
    );
}

/// `upload_part_copy` is a SECOND, distinct call site of the same `Mutation::RecordPart` supersede
/// — and the more dangerous one, because it needs no request body at all: the attacker names bytes
/// that already exist. The copy source below is real and readable, so the ONLY thing that can stop
/// this request is the uploadId scope check; without it the copy resolves, stages, and supersedes
/// the victim's part 1.
#[tokio::test]
async fn upload_part_copy_against_wrong_key_is_no_such_upload() {
    let h = harness().await;
    let (upload_id, etag1) = start_upload_with_part(&h, "mpb6", "right.bin").await;

    // The attacker's own object, used as the copy source — a legitimate read for them.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpb6"),
                Some("evil-source.bin"),
                &[],
                &[],
                b"attacker-chosen-bytes".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpb6"),
                Some("wrong.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[("x-amz-copy-source", "/mpb6/evil-source.bin")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NOT_FOUND,
        "UploadPartCopy against the wrong key must be NoSuchUpload"
    );

    // The victim's part was NOT superseded — a 404-that-still-records fails here. The source bytes
    // differ from the staged `hello`, so a successful splice would change part 1's ETag.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mpb6"),
                Some("right.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    let want = etag1.trim_matches('"').to_owned();
    assert!(
        body.contains(&want),
        "part 1 was spliced by the cross-key UploadPartCopy (want {want}): {body}"
    );

    // Not over-tight: the same copy against the upload's REAL key still records.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpb6"),
                Some("right.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "2")],
                &[("x-amz-copy-source", "/mpb6/evil-source.bin")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
}

/// Initiate a multipart upload and return its id (no parts).
async fn start_upload(h: &Harness, bucket: &str, key: &str) -> String {
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some(bucket),
                Some(key),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    )
}

/// Audit 2026-07: `list_multipart_uploads` advertised a NextKeyMarker on a truncated page but
/// never READ `key-marker`, so a paginating client (rclone's multipart cleanup) got page 1 back
/// forever.
#[tokio::test]
async fn list_multipart_uploads_honours_key_marker() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("lmub"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    for k in ["a.bin", "b.bin", "c.bin"] {
        start_upload(&h, "lmub", k).await;
    }

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("lmub"),
                None,
                &[("uploads", ""), ("max-uploads", "2")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body1 = String::from_utf8(body).unwrap();
    assert!(body1.contains("<IsTruncated>true</IsTruncated>"), "{body1}");
    assert!(body1.contains("<Key>a.bin</Key>"), "{body1}");
    assert!(body1.contains("<Key>b.bin</Key>"), "{body1}");
    assert!(!body1.contains("<Key>c.bin</Key>"), "{body1}");
    let next_key = between(&body1, "<NextKeyMarker>", "</NextKeyMarker>");
    assert_eq!(next_key, "b.bin", "{body1}");
    let next_upload = between(&body1, "<NextUploadIdMarker>", "</NextUploadIdMarker>");
    assert!(!next_upload.is_empty(), "{body1}");

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("lmub"),
                None,
                &[
                    ("uploads", ""),
                    ("max-uploads", "2"),
                    ("key-marker", next_key.as_str()),
                    ("upload-id-marker", next_upload.as_str()),
                ],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body2 = String::from_utf8(body).unwrap();
    assert!(body2.contains("<Key>c.bin</Key>"), "{body2}");
    assert!(
        !body2.contains("<Key>a.bin</Key>") && !body2.contains("<Key>b.bin</Key>"),
        "page 2 must advance, not re-serve page 1: {body2}"
    );
    assert!(
        body2.contains("<IsTruncated>false</IsTruncated>"),
        "{body2}"
    );
    // The markers we sent are echoed back.
    assert!(body2.contains("<KeyMarker>b.bin</KeyMarker>"), "{body2}");
    assert!(
        body2.contains(&format!("<UploadIdMarker>{next_upload}</UploadIdMarker>")),
        "{body2}"
    );
}

/// The upload-id half of the marker: one key can hold many concurrent uploads, and without a
/// `(key, upload id)` pair such a key can never be paged past — every page is that same key, so
/// the NextKeyMarker never advances.
#[tokio::test]
async fn list_multipart_uploads_pages_within_one_key() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("lmuk"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let mut initiated = Vec::new();
    for _ in 0..3 {
        initiated.push(start_upload(&h, "lmuk", "same.bin").await);
    }
    initiated.sort();

    let mut seen: Vec<String> = Vec::new();
    let mut key_marker = String::new();
    let mut upload_marker = String::new();
    for page in 0..3 {
        let mut query: Vec<(&str, &str)> = vec![("uploads", ""), ("max-uploads", "1")];
        if page > 0 {
            query.push(("key-marker", key_marker.as_str()));
            query.push(("upload-id-marker", upload_marker.as_str()));
        }
        let (st, _, body) = drain(
            send(
                &h.svc,
                req(Method::GET, Some("lmuk"), None, &query, &[], vec![]),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let body = String::from_utf8(body).unwrap();
        let id = between(&body, "<UploadId>", "</UploadId>");
        assert!(
            !seen.contains(&id),
            "page {page} repeated upload {id}: {body}"
        );
        seen.push(id);
        if page < 2 {
            assert!(
                body.contains("<IsTruncated>true</IsTruncated>"),
                "page {page}: {body}"
            );
            key_marker = between(&body, "<NextKeyMarker>", "</NextKeyMarker>");
            upload_marker = between(&body, "<NextUploadIdMarker>", "</NextUploadIdMarker>");
        } else {
            assert!(
                body.contains("<IsTruncated>false</IsTruncated>"),
                "the last page must terminate: {body}"
            );
        }
    }
    seen.sort();
    assert_eq!(
        seen, initiated,
        "every initiated upload listed exactly once"
    );
}

/// Issue #3: `max-uploads` was left on the lenient `.parse().ok().unwrap_or(1000)` when `max-keys`
/// was tightened. Both halves of that defect:
///   * `max-uploads=0` is legal S3 and means literally zero uploads, but the store's `limit.max(1)`
///     progress guard rounded it up to a one-upload page;
///   * an unparseable `max-uploads` silently became the 1000 default — a silent, unbounded response
///     where the client asked for something it believed was small.
#[tokio::test]
async fn list_multipart_uploads_max_uploads_zero_and_non_integer() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("lmumax"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    for k in ["a.bin", "b.bin", "c.bin"] {
        start_upload(&h, "lmumax", k).await;
    }

    // `max-uploads=0`: zero uploads, not one, and a terminal page.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("lmumax"),
                None,
                &[("uploads", ""), ("max-uploads", "0")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "max-uploads=0 is valid, not a 400");
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("<MaxUploads>0</MaxUploads>"), "{body}");
    assert!(body.contains("<IsTruncated>false</IsTruncated>"), "{body}");
    assert!(!body.contains("<Upload>"), "no uploads at all: {body}");
    assert!(!body.contains("<NextKeyMarker>"), "{body}");

    // A non-integer / negative `max-uploads` is a 400 InvalidArgument naming `max-uploads` — not a
    // full 1000-upload page, and not an error text about `max-keys`.
    for bad in ["abc", "-1", "1e3", "1.5"] {
        let (st, _, body) = drain(
            send(
                &h.svc,
                req(
                    Method::GET,
                    Some("lmumax"),
                    None,
                    &[("uploads", ""), ("max-uploads", bad)],
                    &[],
                    vec![],
                ),
            )
            .await,
        )
        .await;
        assert_eq!(
            st,
            StatusCode::BAD_REQUEST,
            "max-uploads={bad} must be a 400"
        );
        let body = String::from_utf8(body).unwrap();
        assert!(body.contains("<Code>InvalidArgument</Code>"), "{body}");
        assert!(
            body.contains("max-uploads") && !body.contains("max-keys"),
            "the message must name the parameter the client sent: {body}"
        );
        assert!(
            !body.contains("<Upload>"),
            "must not fall through to a 1000-upload page: {body}"
        );
    }
}

/// Guard against over-correcting the strict parse: an ordinary `max-uploads` must still bound the
/// page and truncate, and one over the ceiling is silently CAPPED (AWS caps, it does not error).
#[tokio::test]
async fn list_multipart_uploads_max_uploads_still_paginates() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("lmupage"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    for k in ["a.bin", "b.bin", "c.bin"] {
        start_upload(&h, "lmupage", k).await;
    }

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("lmupage"),
                None,
                &[("uploads", ""), ("max-uploads", "2")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert_eq!(body.matches("<Upload>").count(), 2, "{body}");
    assert!(body.contains("<MaxUploads>2</MaxUploads>"), "{body}");
    assert!(body.contains("<IsTruncated>true</IsTruncated>"), "{body}");
    assert_eq!(
        between(&body, "<NextKeyMarker>", "</NextKeyMarker>"),
        "b.bin",
        "{body}"
    );

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("lmupage"),
                None,
                &[("uploads", ""), ("max-uploads", "5000")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("<MaxUploads>1000</MaxUploads>"), "{body}");
    assert_eq!(body.matches("<Upload>").count(), 3, "{body}");
}

/// Audit 2026-07: a CompleteMultipartUpload naming a part that was never uploaded must be
/// 400 InvalidPart, not 404 NoSuchUpload (which falsely tells the client the whole upload vanished).
#[tokio::test]
async fn complete_multipart_missing_part_is_invalid_part() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("mpb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (_, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpb"),
                Some("k"),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpb"),
                Some("k"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[],
                b"hello".to_vec(),
            ),
        )
        .await,
    )
    .await;
    let etag1 = header(&hdrs, "etag").unwrap().to_owned();
    // Complete requesting part 2 (never uploaded).
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>{etag1}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpb"),
                Some("k"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "missing part is 400, not 404");
    assert!(
        String::from_utf8(body).unwrap().contains("InvalidPart"),
        "error code is InvalidPart"
    );
}

/// Audit 2026-07: v1 ListObjects must emit a plain-key NextMarker that, echoed back as `marker`,
/// advances the listing. Pre-fix the NextMarker was base64-encoded but the incoming marker consumed
/// raw, so pagination looped (re-returned page 1) or skipped keys.
#[tokio::test]
async fn list_objects_v1_pagination_round_trips() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("listbkt"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    for k in ["file1", "file2", "file3", "file4"] {
        drain(
            send(
                &h.svc,
                req(
                    Method::PUT,
                    Some("listbkt"),
                    Some(k),
                    &[],
                    &[],
                    b"x".to_vec(),
                ),
            )
            .await,
        )
        .await;
    }

    // Page 1: v1 (no list-type), max-keys=2.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("listbkt"),
                None,
                &[("max-keys", "2")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body1 = String::from_utf8(body).unwrap();
    assert!(
        body1.contains("<Key>file1</Key>") && body1.contains("<Key>file2</Key>"),
        "page 1 has the first two keys"
    );
    assert!(body1.contains("<IsTruncated>true</IsTruncated>"));
    let next = between(&body1, "<NextMarker>", "</NextMarker>");
    assert!(
        !next.is_empty(),
        "a truncated v1 listing emits a NextMarker"
    );

    // Page 2: resume with marker = the emitted NextMarker.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("listbkt"),
                None,
                &[("max-keys", "2"), ("marker", next.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body2 = String::from_utf8(body).unwrap();
    assert!(
        body2.contains("<Key>file3</Key>") && body2.contains("<Key>file4</Key>"),
        "page 2 advances to the next keys, got: {body2}"
    );
    assert!(
        !body2.contains("<Key>file1</Key>") && !body2.contains("<Key>file2</Key>"),
        "page 2 must not repeat page 1 (no loop)"
    );
}

/// Audit 2026-07: ListObjectVersions must emit a PLAIN-key NextKeyMarker (paired with a plain
/// NextVersionIdMarker) that, echoed back as `key-marker`/`version-id-marker`, advances the
/// listing. Pre-fix the NextKeyMarker was base64-encoded and the incoming key-marker
/// base64-decoded, so a client passing a literal key had it silently dropped — which also dropped
/// the paired version-id marker — and pagination looped on page 1 forever.
#[tokio::test]
async fn list_object_versions_pagination_round_trips() {
    let h = harness().await;
    versioned_bucket(&h, "verbkt").await;
    for k in ["vk1", "vk2"] {
        for _ in 0..2 {
            drain(
                send(
                    &h.svc,
                    req(
                        Method::PUT,
                        Some("verbkt"),
                        Some(k),
                        &[],
                        &[],
                        b"x".to_vec(),
                    ),
                )
                .await,
            )
            .await;
        }
    }

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("verbkt"),
                None,
                &[("versions", ""), ("max-keys", "2")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body1 = String::from_utf8(body).unwrap();
    assert!(body1.contains("<IsTruncated>true</IsTruncated>"), "{body1}");
    let next_key = between(&body1, "<NextKeyMarker>", "</NextKeyMarker>");
    assert_eq!(
        next_key, "vk1",
        "NextKeyMarker must be the plain object key, not a base64 token: {body1}"
    );
    let next_vid = between(&body1, "<NextVersionIdMarker>", "</NextVersionIdMarker>");
    assert!(!next_vid.is_empty(), "{body1}");
    // Collect page 1's version ids so page 2 can be checked for repeats.
    let page1_vids: Vec<String> = body1
        .match_indices("<VersionId>")
        .map(|(i, _)| {
            let s = &body1[i + "<VersionId>".len()..];
            s[..s.find("</VersionId>").unwrap()].to_owned()
        })
        .collect();
    assert_eq!(page1_vids.len(), 2, "{body1}");
    assert_eq!(
        page1_vids.last().unwrap(),
        &next_vid,
        "the version-id half must name page 1's last version"
    );

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("verbkt"),
                None,
                &[
                    ("versions", ""),
                    ("max-keys", "2"),
                    ("key-marker", next_key.as_str()),
                    ("version-id-marker", next_vid.as_str()),
                ],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body2 = String::from_utf8(body).unwrap();
    // Compare the RETURNED versions, not the raw body: page 2 legitimately echoes the
    // `<VersionIdMarker>` we sent, which is by definition page 1's last version id. A naive
    // `body2.contains(..)` therefore reports a pagination loop that isn't there.
    let page2_vids: Vec<String> = body2
        .match_indices("<Version><Key>")
        .map(|(i, _)| {
            let s = &body2[i..];
            let v = &s[s.find("<VersionId>").unwrap() + "<VersionId>".len()..];
            v[..v.find("</VersionId>").unwrap()].to_owned()
        })
        .collect();
    assert_eq!(page2_vids.len(), 2, "{body2}");
    assert!(
        !page1_vids.iter().any(|v| page2_vids.contains(v)),
        "page 2 must not repeat page 1 (no pagination loop), got: {body2}"
    );
    assert!(body2.contains("<Key>vk2</Key>"), "{body2}");
    assert!(
        body2.contains("<IsTruncated>false</IsTruncated>"),
        "{body2}"
    );

    // A marker the CLIENT synthesized (no version-id half) must be honored too, and a BARE
    // key-marker is EXCLUSIVE: S3 begins listing after that key, so naming vk1 skips ALL of vk1's
    // versions and lands on vk2. (This block previously sent "vk2" and asserted vk2 came back,
    // documenting the store's then-inclusive seek "by design" — that was bug B from
    // conformance/listing.py, not a design choice. The paired-marker case, which must still resume
    // *within* a key, is covered by list_object_versions_paired_marker_still_resumes_within_a_key.)
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("verbkt"),
                None,
                &[("versions", ""), ("key-marker", "vk1")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body3 = String::from_utf8(body).unwrap();
    assert!(body3.contains("<Key>vk2</Key>"), "{body3}");
    assert!(
        !body3.contains("<Key>vk1</Key>"),
        "a bare key-marker is exclusive: naming vk1 must skip every vk1 version, got: {body3}"
    );
}

/// Audit 2026-07: `max-keys=0` is legal S3 and means literally zero keys. It used to reach the
/// store, whose `limit.max(1)` progress guard rounded it up, so a client asking for no keys got one
/// (KeyCount=1, IsTruncated=false).
#[tokio::test]
async fn list_objects_max_keys_zero_returns_no_keys() {
    let h = harness().await;
    versioned_bucket(&h, "maxkeys0").await;
    for k in ["a", "b", "c"] {
        drain(
            send(
                &h.svc,
                req(
                    Method::PUT,
                    Some("maxkeys0"),
                    Some(k),
                    &[],
                    &[],
                    b"x".to_vec(),
                ),
            )
            .await,
        )
        .await;
    }

    // v2.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("maxkeys0"),
                None,
                &[("list-type", "2"), ("max-keys", "0")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "max-keys=0 is valid, not a 400");
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("<KeyCount>0</KeyCount>"), "{body}");
    assert!(body.contains("<IsTruncated>false</IsTruncated>"), "{body}");
    assert!(body.contains("<MaxKeys>0</MaxKeys>"), "{body}");
    assert!(!body.contains("<Key>"), "no Contents at all: {body}");
    assert!(!body.contains("<NextContinuationToken>"), "{body}");

    // v1.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("maxkeys0"),
                None,
                &[("max-keys", "0")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("<IsTruncated>false</IsTruncated>"), "{body}");
    assert!(!body.contains("<Key>"), "{body}");
    assert!(!body.contains("<NextMarker>"), "{body}");

    // ListObjectVersions shares the parameter and the same store clamp.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("maxkeys0"),
                None,
                &[("versions", ""), ("max-keys", "0")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("<IsTruncated>false</IsTruncated>"), "{body}");
    assert!(!body.contains("<Version>"), "{body}");
    assert!(!body.contains("<Key>"), "{body}");
}

/// A non-integer or negative `max-keys` is a 400 InvalidArgument (AWS parity). Swallowing the parse
/// error into the 1000 default handed a client that asked for something small a full 1000-key page.
#[tokio::test]
async fn list_objects_rejects_non_integer_max_keys() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("maxkeysbad"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("maxkeysbad"),
                Some("only"),
                &[],
                &[],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;

    for bad in ["abc", "-1", "1e3", "1.5"] {
        let (st, _, body) = drain(
            send(
                &h.svc,
                req(
                    Method::GET,
                    Some("maxkeysbad"),
                    None,
                    &[("list-type", "2"), ("max-keys", bad)],
                    &[],
                    vec![],
                ),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "max-keys={bad} must be a 400");
        let body = String::from_utf8(body).unwrap();
        assert!(body.contains("<Code>InvalidArgument</Code>"), "{body}");
        assert!(
            !body.contains("<Key>"),
            "must not fall through to a 1000-key page: {body}"
        );
    }

    // Over the ceiling is silently CAPPED, not rejected.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("maxkeysbad"),
                None,
                &[("list-type", "2"), ("max-keys", "5000")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("<MaxKeys>1000</MaxKeys>"), "{body}");

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("maxkeysbad"),
                None,
                &[("list-type", "2"), ("max-keys", "1")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert_eq!(body.matches("<Key>").count(), 1, "{body}");
}

/// Regression: a ListObjectsV2 with an empty-but-present `delimiter=` (exactly what minio-go — and
/// therefore warp's recursive-list benchmark — sends on every request) must behave as "no
/// delimiter" and return the objects, not collapse them into a single CommonPrefix. Before the fix,
/// `str::find("")` matched at offset 0 for every key, so this returned zero `<Contents>` and warp
/// flagged every LIST op as an error.
#[tokio::test]
async fn list_objects_v2_empty_delimiter_lists_all() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("delimbkt"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    for k in ["a/1", "a/2", "c"] {
        drain(
            send(
                &h.svc,
                req(
                    Method::PUT,
                    Some("delimbkt"),
                    Some(k),
                    &[],
                    &[],
                    b"x".to_vec(),
                ),
            )
            .await,
        )
        .await;
    }

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("delimbkt"),
                None,
                &[("list-type", "2"), ("delimiter", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert!(
        body.contains("<Key>a/1</Key>")
            && body.contains("<Key>a/2</Key>")
            && body.contains("<Key>c</Key>"),
        "empty delimiter must list every object, got: {body}"
    );
    assert!(
        !body.contains("<CommonPrefixes>"),
        "empty delimiter must not synthesize a common prefix, got: {body}"
    );
}

/// A part-validation failure on CompleteMultipartUpload must leave the upload retryable rather than
/// bricking it in `completing` (audit #14): a first complete carrying a wrong ETag fails, and a
/// second complete with the correct ETags then succeeds.
#[tokio::test]
async fn complete_multipart_part_validation_failure_is_retryable() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("mpr"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpr"),
                Some("o"),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );

    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = b"tail".to_vec();
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpr"),
                Some("o"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[],
                part1.clone(),
            ),
        )
        .await,
    )
    .await;
    let etag1 = header(&hdrs, "etag").unwrap().to_owned();
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpr"),
                Some("o"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "2")],
                &[],
                part2.clone(),
            ),
        )
        .await,
    )
    .await;
    let etag2 = header(&hdrs, "etag").unwrap().to_owned();

    // First complete carries a wrong ETag for part 2 — validation must fail.
    let bad = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>\"deadbeefdeadbeefdeadbeefdeadbeef\"</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpr"),
                Some("o"),
                &[("uploadId", upload_id.as_str())],
                &[],
                bad.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_ne!(st, StatusCode::OK, "a bad-ETag complete must fail");

    // The upload is NOT bricked: a retry with the correct ETags succeeds.
    let good = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpr"),
                Some("o"),
                &[("uploadId", upload_id.as_str())],
                &[],
                good.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "retry after a part-validation failure must succeed (upload not bricked)"
    );
}

/// Object GET/HEAD must carry `x-content-type-options: nosniff` so attacker-uploaded bytes served
/// inline same-origin cannot be MIME-sniffed into executable content (audit #13).
#[tokio::test]
async fn object_get_sets_nosniff() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("snb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("snb"),
                Some("k"),
                &[],
                &[],
                b"hi".to_vec(),
            ),
        )
        .await,
    )
    .await;
    for method in [Method::GET, Method::HEAD] {
        let (st, hdrs, _) = drain(
            send(
                &h.svc,
                req(method.clone(), Some("snb"), Some("k"), &[], &[], vec![]),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(
            header(&hdrs, "x-content-type-options"),
            Some("nosniff"),
            "{method} must set nosniff"
        );
    }
}

/// A full multipart cycle must work under metadata sharding (audit #4): the upload id the client
/// receives from Initiate must carry the owning shard, so the later UploadPart / Complete route back
/// to the shard that holds the session. Before the fix the protocol returned the un-encoded local id
/// and UploadPart 404'd on the wrong shard.
#[tokio::test]
async fn multipart_cycle_works_under_sharding() {
    let h = harness_sharded(3).await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("mpshard"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // Initiate → the returned upload id is shard-encoded.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpshard"),
                Some("big"),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );

    // A single part (last part may be small).
    let part = vec![b'z'; 4096];
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpshard"),
                Some("big"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[],
                part.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "UploadPart must route to the upload's shard"
    );
    let etag = header(&hdrs, "etag").unwrap().to_owned();

    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpshard"),
                Some("big"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "CompleteMultipartUpload must route to the upload's shard"
    );

    let (st, _, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("mpshard"), Some("big"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(got, part);
}

#[tokio::test]
async fn copy_object_works() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("cpb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cpb"),
                Some("src.txt"),
                &[],
                &[("content-type", "text/plain")],
                b"original".to_vec(),
            ),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cpb"),
                Some("dst.txt"),
                &[],
                &[("x-amz-copy-source", "/cpb/src.txt")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("cpb"), Some("dst.txt"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, b"original");
}

/// The six system response headers of a copy source (ARCH 13.4 / 21.6). Under the default COPY
/// metadata directive they must all be carried onto the destination — audit 2026-07 found five of
/// them hard-coded `None` in `copy_object`'s row literal, so every copy silently lost them.
#[tokio::test]
async fn copy_object_preserves_system_headers_under_default_directive() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("cphdr"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let src_headers = [
        ("content-type", "text/plain"),
        ("content-encoding", "gzip"),
        ("cache-control", "max-age=99"),
        ("content-disposition", "attachment; filename=\"a.txt\""),
        ("content-language", "en-GB"),
        ("expires", "Thu, 01 Jan 2032 00:00:00 GMT"),
        ("x-amz-meta-foo", "bar"),
    ];
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cphdr"),
                Some("src.txt"),
                &[],
                &src_headers,
                b"original".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cphdr"),
                Some("dst.txt"),
                &[],
                &[("x-amz-copy-source", "/cphdr/src.txt")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("cphdr"),
                Some("dst.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    for (name, want) in src_headers {
        assert_eq!(header(&hdrs, name), Some(want), "copy lost {name}");
    }
}

/// REPLACE takes every system header from the copy REQUEST and drops the source's — a header the
/// request omits must be ABSENT on the destination, not inherited.
#[tokio::test]
async fn copy_object_replace_directive_replaces_system_headers() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("cprep"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cprep"),
                Some("src.txt"),
                &[],
                &[
                    ("content-type", "text/plain"),
                    ("content-encoding", "gzip"),
                    ("cache-control", "max-age=99"),
                    ("content-disposition", "attachment; filename=\"a.txt\""),
                    ("content-language", "en-GB"),
                    ("expires", "Thu, 01 Jan 2032 00:00:00 GMT"),
                    ("x-amz-meta-foo", "bar"),
                ],
                b"original".to_vec(),
            ),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cprep"),
                Some("dst.txt"),
                &[],
                &[
                    ("x-amz-copy-source", "/cprep/src.txt"),
                    ("x-amz-metadata-directive", "REPLACE"),
                    ("content-type", "application/json"),
                    ("cache-control", "no-store"),
                    ("x-amz-meta-new", "1"),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("cprep"),
                Some("dst.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(header(&hdrs, "content-type"), Some("application/json"));
    assert_eq!(header(&hdrs, "cache-control"), Some("no-store"));
    for absent in [
        "content-encoding",
        "content-disposition",
        "content-language",
        "expires",
        "x-amz-meta-foo",
    ] {
        assert_eq!(
            header(&hdrs, absent),
            None,
            "{absent} must not be inherited"
        );
    }
    assert_eq!(header(&hdrs, "x-amz-meta-new"), Some("1"));
}

/// `x-amz-tagging-directive` is independent of the metadata directive: COPY (the default) carries
/// the SOURCE version's stored tags, REPLACE takes the inline `x-amz-tagging`. Before audit
/// 2026-07 a copy persisted NO tags under either directive.
#[tokio::test]
async fn copy_object_tagging_directive() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("cptag"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cptag"),
                Some("src.txt"),
                &[],
                &[("x-amz-tagging", "a=1&b=2")],
                b"original".to_vec(),
            ),
        )
        .await,
    )
    .await;
    // (a) default directive: the source's tags come along.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cptag"),
                Some("dst.txt"),
                &[],
                &[("x-amz-copy-source", "/cptag/src.txt")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("cptag"),
                Some("dst.txt"),
                &[("tagging", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert!(
        body.contains("<Key>a</Key>") && body.contains("<Value>1</Value>"),
        "{body}"
    );
    assert!(
        body.contains("<Key>b</Key>") && body.contains("<Value>2</Value>"),
        "{body}"
    );

    // (b) REPLACE: exactly the inline tag set, and none of the source's.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cptag"),
                Some("dst2.txt"),
                &[],
                &[
                    ("x-amz-copy-source", "/cptag/src.txt"),
                    ("x-amz-tagging-directive", "REPLACE"),
                    ("x-amz-tagging", "c=3"),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("cptag"),
                Some("dst2.txt"),
                &[("tagging", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert!(
        body.contains("<Key>c</Key>") && body.contains("<Value>3</Value>"),
        "{body}"
    );
    assert!(
        !body.contains("<Key>a</Key>"),
        "source tags must not survive REPLACE: {body}"
    );
}

/// An unrecognized directive is `InvalidArgument`, not a silent degrade to COPY — the caller
/// believes a replace succeeded when it did not.
#[tokio::test]
async fn copy_object_rejects_unknown_directives() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("cpbad"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cpbad"),
                Some("src.txt"),
                &[],
                &[],
                b"original".to_vec(),
            ),
        )
        .await,
    )
    .await;
    for (name, value) in [
        ("x-amz-metadata-directive", "BOGUS"),
        ("x-amz-tagging-directive", "BOGUS"),
    ] {
        let (st, _, body) = drain(
            send(
                &h.svc,
                req(
                    Method::PUT,
                    Some("cpbad"),
                    Some("dst.txt"),
                    &[],
                    &[("x-amz-copy-source", "/cpbad/src.txt"), (name, value)],
                    vec![],
                ),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{name} must be validated");
        let body = String::from_utf8(body).unwrap();
        assert!(body.contains("<Code>InvalidArgument</Code>"), "{body}");
    }
    // An invalid inline tag set on a REPLACE copy is rejected before the blob is staged.
    let dup = "d=1&d=2";
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cpbad"),
                Some("dst.txt"),
                &[],
                &[
                    ("x-amz-copy-source", "/cpbad/src.txt"),
                    ("x-amz-tagging-directive", "REPLACE"),
                    ("x-amz-tagging", dup),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("<Code>InvalidTag</Code>"), "{body}");
}

/// A non-admin who owns a destination bucket must NOT be able to copy another tenant's object via
/// `x-amz-copy-source`: the SOURCE read is now authorized against the source bucket. Under the real
/// policy engine, the attacker owns the destination (so the write is allowed) but has no grant on
/// the victim's bucket, so the source read is denied — exactly the hole this fixes (before the fix
/// the copy succeeded with no source check).
#[tokio::test]
async fn copy_source_read_is_authorized_cross_tenant_denied() {
    let h = harness_with_authz(Arc::new(cairn_authz::PolicyEngine)).await;
    let victim = member("victim");
    let attacker = member("attacker");

    // victim owns victimbkt and writes a secret object (owner short-circuit allows both).
    drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("victimbkt"),
                None,
                &[],
                &[],
                vec![],
                victim.clone(),
            ),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("victimbkt"),
                Some("secret"),
                &[],
                &[("content-type", "text/plain")],
                b"top secret".to_vec(),
                victim.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    // attacker owns their own bucket.
    drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("attackerbkt"),
                None,
                &[],
                &[],
                vec![],
                attacker.clone(),
            ),
        )
        .await,
    )
    .await;

    // attacker copies victim's object into their own bucket → source read DENIED (403).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("attackerbkt"),
                Some("stolen"),
                &[],
                &[("x-amz-copy-source", "/victimbkt/secret")],
                vec![],
                attacker.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "cross-tenant copy source must be denied"
    );

    // Sanity: the victim CAN copy their own object within their own bucket (owner short-circuit).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("victimbkt"),
                Some("copy"),
                &[],
                &[("x-amz-copy-source", "/victimbkt/secret")],
                vec![],
                victim.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "owner copy within their own bucket must succeed"
    );
}

/// Version-scoped authorization (audit #33): a policy condition on `s3:ExistingObjectTag` must be
/// evaluated against the version named by `?versionId`, not the current version. An attacker must
/// not read a restricted OLD version merely because the CURRENT version carries the tag the policy
/// allows.
#[tokio::test]
async fn versioned_read_evaluates_existing_tag_against_named_version() {
    let h = harness_with_authz(Arc::new(cairn_authz::PolicyEngine)).await;
    let owner = member("owner-v");
    let reader = member("reader-v");

    // Owner creates a versioned bucket whose policy allows any principal to read `obj` ONLY when
    // its `tier` tag is `public`.
    drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("vbkt"),
                None,
                &[],
                &[],
                vec![],
                owner.clone(),
            ),
        )
        .await,
    )
    .await;
    let vcfg =
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("vbkt"),
                None,
                &[("versioning", "")],
                &[],
                vcfg,
                owner.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":"arn:aws:s3:::vbkt/obj","Condition":{"StringEquals":{"s3:ExistingObjectTag/tier":"public"}}}]}"#.to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("vbkt"),
                None,
                &[("policy", "")],
                &[],
                policy,
                owner.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);

    // v1 = restricted (tier=secret); then v2 (current) = public (tier=public).
    let (_, hv1, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("vbkt"),
                Some("obj"),
                &[],
                &[],
                b"v1".to_vec(),
                owner.clone(),
            ),
        )
        .await,
    )
    .await;
    let v1 = header(&hv1, "x-amz-version-id").unwrap().to_owned();
    let tag_secret =
        b"<Tagging><TagSet><Tag><Key>tier</Key><Value>secret</Value></Tag></TagSet></Tagging>"
            .to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("vbkt"),
                Some("obj"),
                &[("tagging", ""), ("versionId", v1.as_str())],
                &[],
                tag_secret,
                owner.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (_, hv2, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("vbkt"),
                Some("obj"),
                &[],
                &[],
                b"v2".to_vec(),
                owner.clone(),
            ),
        )
        .await,
    )
    .await;
    let v2 = header(&hv2, "x-amz-version-id").unwrap().to_owned();
    assert_ne!(v1, v2);
    let tag_public =
        b"<Tagging><TagSet><Tag><Key>tier</Key><Value>public</Value></Tag></TagSet></Tagging>"
            .to_vec();
    drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("vbkt"),
                Some("obj"),
                &[("tagging", ""), ("versionId", v2.as_str())],
                &[],
                tag_public,
                owner.clone(),
            ),
        )
        .await,
    )
    .await;

    // reader CAN read the public current version (condition met on v2's tier=public).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::GET,
                Some("vbkt"),
                Some("obj"),
                &[("versionId", v2.as_str())],
                &[],
                vec![],
                reader.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "the public version is readable (condition met)"
    );

    // reader must NOT read the restricted OLD version: the condition is evaluated against v1's
    // `tier=secret`, not the current v2's `tier=public`. Before #33 this leaked v1.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::GET,
                Some("vbkt"),
                Some("obj"),
                &[("versionId", v1.as_str())],
                &[],
                vec![],
                reader.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "the restricted version must be denied even though the current version is public (#33)"
    );
}

#[tokio::test]
async fn bulk_delete_works() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("bdb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    for k in ["a.txt", "b.txt", "c.txt"] {
        drain(
            send(
                &h.svc,
                req(Method::PUT, Some("bdb"), Some(k), &[], &[], b"x".to_vec()),
            )
            .await,
        )
        .await;
    }
    let del = "<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object></Delete>";
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("bdb"),
                None,
                &[("delete", "")],
                &[],
                del.as_bytes().to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(xml.contains("<Deleted>") && xml.contains("a.txt"));
    // a and b are gone, c remains.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("bdb"), Some("a.txt"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("bdb"), Some("c.txt"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
}

#[tokio::test]
async fn versioning_and_object_tagging() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("verb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // Enable versioning.
    let vcfg =
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("verb"),
                None,
                &[("versioning", "")],
                &[],
                vcfg,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Two puts create two versions.
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("verb"),
                Some("k"),
                &[],
                &[],
                b"v1".to_vec(),
            ),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("verb"),
                Some("k"),
                &[],
                &[],
                b"v2-newer".to_vec(),
            ),
        )
        .await,
    )
    .await;

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("verb"),
                None,
                &[("versions", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert_eq!(
        xml.matches("<Version>").count(),
        2,
        "two versions listed: {xml}"
    );

    // Latest GET returns the newer object.
    let (_, _, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("verb"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(body, b"v2-newer");

    // Object tagging round-trip.
    let tags = b"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"
        .to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("verb"),
                Some("k"),
                &[("tagging", "")],
                &[],
                tags,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (_, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("verb"),
                Some("k"),
                &[("tagging", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("env") && xml.contains("prod"),
        "tags returned: {xml}"
    );
}

/// Tag GET/PUT operate on the version named by `?versionId`, not always the current one (audit
/// #15): tagging an older version must not tag — nor be read from — the current version.
#[tokio::test]
async fn object_tagging_honors_version_id() {
    let h = harness().await;
    versioned_bucket(&h, "vtag").await;

    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("vtag"),
                Some("k"),
                &[],
                &[],
                b"one".to_vec(),
            ),
        )
        .await,
    )
    .await;
    let v1 = header(&hdrs, "x-amz-version-id").unwrap().to_owned();
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("vtag"),
                Some("k"),
                &[],
                &[],
                b"two".to_vec(),
            ),
        )
        .await,
    )
    .await;
    let v2 = header(&hdrs, "x-amz-version-id").unwrap().to_owned();
    assert_ne!(v1, v2);

    // Tag the OLDER version (v1) explicitly via ?versionId.
    let tags = b"<Tagging><TagSet><Tag><Key>which</Key><Value>v1</Value></Tag></TagSet></Tagging>"
        .to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("vtag"),
                Some("k"),
                &[("tagging", ""), ("versionId", v1.as_str())],
                &[],
                tags,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Reading v1's tags returns them.
    let (_, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("vtag"),
                Some("k"),
                &[("tagging", ""), ("versionId", v1.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let xml_v1 = String::from_utf8(body).unwrap();
    assert!(
        xml_v1.contains("which") && xml_v1.contains("v1"),
        "v1 tags: {xml_v1}"
    );

    // The CURRENT version (v2) must have NO tags — the tag did not leak onto it.
    let (_, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("vtag"),
                Some("k"),
                &[("tagging", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let xml_cur = String::from_utf8(body).unwrap();
    assert!(
        !xml_cur.contains("which"),
        "the current version must be untagged: {xml_cur}"
    );
}

/// A non-streaming PUT that signs a concrete payload hash must have its body verified against it
/// (audit #25): the correct sha256 succeeds, a wrong one is rejected.
#[tokio::test]
async fn put_verifies_signed_content_sha256() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("shab"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // sha256("hi").
    let good = "8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4";
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("shab"),
                Some("k"),
                &[],
                &[("x-amz-content-sha256", good)],
                b"hi".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "a body matching the signed sha256 is accepted"
    );

    // The same body with a wrong signed hash must be rejected.
    let bad = "0000000000000000000000000000000000000000000000000000000000000000";
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("shab"),
                Some("k2"),
                &[],
                &[("x-amz-content-sha256", bad)],
                b"hi".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "a body that does not match the signed sha256 is rejected"
    );
}

#[tokio::test]
async fn bucket_policy_roundtrip() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("polb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::polb/*"}]}"#.to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("polb"),
                None,
                &[("policy", "")],
                &[],
                policy,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("polb"),
                None,
                &[("policy", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(String::from_utf8(body).unwrap().contains("s3:GetObject"));

    // Malformed policy is rejected.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("polb"),
                None,
                &[("policy", "")],
                &[],
                b"not json".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Delete then 404.
    drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("polb"),
                None,
                &[("policy", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("polb"),
                None,
                &[("policy", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn put_object_acl_subresource_does_not_overwrite_body() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("aclb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("aclb"),
                Some("k"),
                &[],
                &[],
                b"real-body".to_vec(),
            ),
        )
        .await,
    )
    .await;

    // PUT ?acl must NOT fall through to put_object and overwrite the object with the ACL body.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("aclb"),
                Some("k"),
                &[("acl", "")],
                &[],
                b"<AccessControlPolicy/>".to_vec(),
            ),
        )
        .await,
    )
    .await;
    // Under BucketOwnerEnforced ownership ACLs are unsupported (400); the key point is that
    // PUT key?acl is never a body write (the GET below still returns the original body).
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "PUT key?acl must not be a body write"
    );

    // The object body is unchanged.
    let (_, _, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("aclb"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(body, b"real-body");

    // GET ?acl returns an AccessControlPolicy document.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("aclb"),
                Some("k"),
                &[("acl", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        String::from_utf8(body)
            .unwrap()
            .contains("AccessControlPolicy")
    );
}

#[tokio::test]
async fn unknown_subresource_is_not_implemented_not_misrouted() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("subb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    // An unrecognized bucket subresource must be 501, never a list/create fall-through.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("subb"),
                None,
                &[("website", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_IMPLEMENTED);
}

/// Audit 2026-07: a RECOGNIZED bucket subresource on a method we do not serve used to skip every
/// subresource arm and land on the bare verb — `DELETE /b?ownershipControls` ran `delete_bucket`
/// and destroyed the bucket. The guard's keyword list must cover partially-served keywords too.
#[tokio::test]
async fn unhandled_bucket_subresource_method_does_not_execute_bare_verb() {
    let h = harness().await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("subguard"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Every one of these reached `delete_bucket` before the fix.
    for kw in [
        "ownershipControls",
        "acl",
        "location",
        "uploads",
        "versions",
        "versioning",
    ] {
        let (st, _, _) = drain(
            send(
                &h.svc,
                req(
                    Method::DELETE,
                    Some("subguard"),
                    None,
                    &[(kw, "")],
                    &[],
                    vec![],
                ),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_IMPLEMENTED, "DELETE ?{kw} must be 501");
        // The load-bearing assertion: the bucket survived, i.e. `delete_bucket` did not run.
        let (st, _, _) = drain(
            send(
                &h.svc,
                req(Method::HEAD, Some("subguard"), None, &[], &[], vec![]),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "bucket destroyed by DELETE ?{kw}");
    }

    // The mutating half: a GET-only keyword on PUT must not reach `create_bucket`.
    for kw in ["location", "uploads", "versions"] {
        let (st, _, _) = drain(
            send(
                &h.svc,
                req(
                    Method::PUT,
                    Some("subguard"),
                    None,
                    &[(kw, "")],
                    &[],
                    vec![],
                ),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_IMPLEMENTED, "PUT ?{kw} must be 501");
    }

    // Guard against an over-broad list: every served method/selector pair still works.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("subguard"),
                None,
                &[("ownershipControls", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("subguard"),
                None,
                &[("acl", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("subguard"),
                None,
                &[("versioning", "")],
                &[],
                b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                    .to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("subguard"),
                None,
                &[("tagging", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    // `delete` IS in the guard list, so its one served pair (`POST ?delete`) has to be matched
    // ABOVE the guard — assert the arm move kept it routing.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("subguard"),
                None,
                &[("delete", "")],
                &[],
                b"<Delete><Object><Key>k</Key></Object></Delete>".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
}

/// Audit 2026-07: the object-side twin. `PUT key?attributes` used to fall through to `put_object`
/// and overwrite the object body; `DELETE key?uploads` fell through to `delete_object`.
#[tokio::test]
async fn unhandled_object_subresource_does_not_overwrite_or_delete() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("objguard"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("objguard"),
                Some("k"),
                &[],
                &[],
                b"original".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    for kw in ["attributes", "uploads"] {
        let (st, _, _) = drain(
            send(
                &h.svc,
                req(
                    Method::PUT,
                    Some("objguard"),
                    Some("k"),
                    &[(kw, "")],
                    &[],
                    b"clobbered".to_vec(),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_IMPLEMENTED, "PUT key?{kw} must be 501");
        let (st, _, body) = drain(
            send(
                &h.svc,
                req(Method::GET, Some("objguard"), Some("k"), &[], &[], vec![]),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "object deleted by PUT key?{kw}");
        assert_eq!(body, b"original", "object body clobbered by PUT key?{kw}");

        let (st, _, _) = drain(
            send(
                &h.svc,
                req(
                    Method::DELETE,
                    Some("objguard"),
                    Some("k"),
                    &[(kw, "")],
                    &[],
                    vec![],
                ),
            )
            .await,
        )
        .await;
        assert_eq!(
            st,
            StatusCode::NOT_IMPLEMENTED,
            "DELETE key?{kw} must be 501"
        );
        let (st, _, _) = drain(
            send(
                &h.svc,
                req(Method::GET, Some("objguard"), Some("k"), &[], &[], vec![]),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "object deleted by DELETE key?{kw}");
    }

    // The served multipart pairs must still route after the arm move.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("objguard"),
                Some("mk"),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("objguard"),
                Some("mk"),
                &[("uploadId", upload_id.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("objguard"),
                Some("mk"),
                &[("uploadId", upload_id.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
}

/// Audit 2026-07: `?delete` names the DeleteObjects POST body, not the bucket. Before the fix the
/// keyword was absent from the guard list, so `DELETE /b?delete` — a plausible client typo, and the
/// shape rclone/boto3 produce if the method is wrong — skipped every subresource arm and ran
/// `delete_bucket`, silently destroying an empty bucket the caller never asked to remove.
#[tokio::test]
async fn delete_verb_on_the_delete_subresource_does_not_destroy_the_bucket() {
    let h = harness().await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("delsub"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // The bucket is deliberately EMPTY here: `delete_bucket` refuses a non-empty bucket, so only an
    // empty one exposes the fall-through.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("delsub"),
                None,
                &[("delete", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NOT_IMPLEMENTED,
        "DELETE ?delete must be 501"
    );

    // The load-bearing assertion: the bucket survived, i.e. `delete_bucket` did not run.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::HEAD, Some("delsub"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "bucket destroyed by DELETE ?delete");

    // The regression risk of the fix: listing `delete` in the guard forced the real DeleteObjects
    // arm to move ABOVE the guard. Prove it still routes AND still deletes.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("delsub"),
                Some("gone.txt"),
                &[],
                &[],
                b"bye".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("delsub"),
                None,
                &[("delete", "")],
                &[],
                b"<Delete><Object><Key>gone.txt</Key></Object></Delete>".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "POST ?delete must still be DeleteObjects"
    );
    let xml = String::from_utf8(body).unwrap();
    assert!(xml.contains("<Deleted>"), "DeleteResult body: {xml}");
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("delsub"),
                Some("gone.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "the key was really deleted");
}

/// Audit 2026-07: HEAD sits ABOVE both subresource guards. Widening the keyword lists to cover
/// partially-served selectors would otherwise have 501'd `HEAD /b?versioning`, `HEAD /b?acl` and
/// `HEAD key?attributes` — a gratuitous deviation, since HEAD writes nothing and S3 answers
/// HeadBucket/HeadObject whatever `?subresource` rides along. The second half checks the exemption
/// did not punch a hole in the guard: the MUTATING verbs on those same keywords still 501.
#[tokio::test]
async fn head_is_exempt_from_the_subresource_guards() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("headsub"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("headsub"),
                Some("k"),
                &[],
                &[],
                b"original".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // HeadBucket answers regardless of the selector riding along.
    for kw in ["versioning", "acl"] {
        let (st, _, _) = drain(
            send(
                &h.svc,
                req(
                    Method::HEAD,
                    Some("headsub"),
                    None,
                    &[(kw, "")],
                    &[],
                    vec![],
                ),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "HEAD /b?{kw} must not be 501");
    }

    // HeadObject likewise — and it is a real HeadObject, not an empty 200: the content-length is
    // the object's.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("headsub"),
                Some("k"),
                &[("attributes", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "HEAD key?attributes must not be 501");
    assert_eq!(
        header(&hdrs, "content-length"),
        Some("8"),
        "HEAD key?attributes must answer for the object itself"
    );

    // The guard still fires for the mutating verbs on the very same keywords.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("headsub"),
                None,
                &[("versioning", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NOT_IMPLEMENTED,
        "DELETE ?versioning must be 501"
    );
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::HEAD, Some("headsub"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "bucket destroyed by DELETE ?versioning");

    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("headsub"),
                Some("k"),
                &[("attributes", "")],
                &[],
                b"clobbered".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NOT_IMPLEMENTED,
        "PUT key?attributes must be 501"
    );
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("headsub"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        body, b"original",
        "object body clobbered by PUT key?attributes"
    );
}

#[tokio::test]
async fn public_access_block_roundtrip() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("bpab"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let cfg = b"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>".to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("bpab"),
                None,
                &[("publicAccessBlock", "")],
                &[],
                cfg,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("bpab"),
                None,
                &[("publicAccessBlock", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("<BlockPublicAcls>true</BlockPublicAcls>"),
        "bpa: {xml}"
    );
    assert!(xml.contains("<BlockPublicPolicy>true</BlockPublicPolicy>"));
}

/// Create a bucket and enable versioning on it.
async fn versioned_bucket(h: &Harness, bucket: &str) {
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some(bucket), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let vcfg =
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some(bucket),
                None,
                &[("versioning", "")],
                &[],
                vcfg,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
}

#[tokio::test]
async fn versioning_delete_marker_signaling_and_405() {
    let h = harness().await;
    versioned_bucket(&h, "dmb").await;

    // PUT a version.
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("dmb"),
                Some("k"),
                &[],
                &[],
                b"v1".to_vec(),
            ),
        )
        .await,
    )
    .await;

    // Plain DELETE in an Enabled bucket inserts a delete marker and signals its identity.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::DELETE, Some("dmb"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    assert_eq!(header(&hdrs, "x-amz-delete-marker"), Some("true"));
    let marker_vid = header(&hdrs, "x-amz-version-id")
        .expect("delete response carries the new marker version id")
        .to_owned();

    // A plain GET of the now-deleted key returns 404 with the marker signaled.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("dmb"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(header(&hdrs, "x-amz-delete-marker"), Some("true"));
    assert_eq!(header(&hdrs, "x-amz-version-id"), Some(marker_vid.as_str()));

    // A plain HEAD behaves the same way.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::HEAD, Some("dmb"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(header(&hdrs, "x-amz-delete-marker"), Some("true"));

    // GET/HEAD naming the delete marker's OWN version id is 405 MethodNotAllowed, not 404.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("dmb"),
                Some("k"),
                &[("versionId", marker_vid.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(header(&hdrs, "x-amz-delete-marker"), Some("true"));

    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("dmb"),
                Some("k"),
                &[("versionId", marker_vid.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::METHOD_NOT_ALLOWED);

    // The original version is still retrievable by its own version id (not destroyed).
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("dmb"),
                None,
                &[("versions", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert_eq!(xml.matches("<Version>").count(), 1, "v1 retained: {xml}");
    assert_eq!(
        xml.matches("<DeleteMarker>").count(),
        1,
        "delete marker listed: {xml}"
    );
}

#[tokio::test]
async fn suspended_delete_inserts_null_marker_no_data_loss() {
    let h = harness().await;
    versioned_bucket(&h, "susb").await;

    // Write an identified version while Enabled.
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("susb"),
                Some("k"),
                &[],
                &[],
                b"keep-me".to_vec(),
            ),
        )
        .await,
    )
    .await;
    // Capture the surviving identified version id from the versions listing.
    let (_, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("susb"),
                None,
                &[("versions", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let xml = String::from_utf8(body).unwrap();
    let kept_vid = between(&xml, "<VersionId>", "</VersionId>");

    // Suspend versioning.
    let vcfg =
        b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>".to_vec();
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("susb"),
                None,
                &[("versioning", "")],
                &[],
                vcfg,
            ),
        )
        .await,
    )
    .await;

    // A plain DELETE in a Suspended bucket inserts a NULL-version delete marker rather than
    // permanently removing data; the response signals a delete marker.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::DELETE, Some("susb"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    assert_eq!(header(&hdrs, "x-amz-delete-marker"), Some("true"));

    // The current key now reads as deleted (a delete marker hides it).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("susb"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // But the earlier identified version is NOT lost: it is still retrievable by its version id.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("susb"),
                Some("k"),
                &[("versionId", kept_vid.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "suspended delete must not destroy data");
    assert_eq!(body, b"keep-me");
}

#[tokio::test]
async fn put_object_inline_tagging_header_is_persisted() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("tagb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // PUT with an inline x-amz-tagging header (form-encoded tag set).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("tagb"),
                Some("k"),
                &[],
                &[("x-amz-tagging", "team=storage&env=prod")],
                b"body".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // GET ?tagging returns the persisted tags.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("tagb"),
                Some("k"),
                &[("tagging", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("team") && xml.contains("storage"),
        "tags: {xml}"
    );
    assert!(xml.contains("env") && xml.contains("prod"), "tags: {xml}");
}

/// An authorization engine that denies DELETE of one specific object key and allows everything
/// else, used to exercise per-key authorization fidelity in bulk delete. (Reads of the key stay
/// allowed so the test can verify the key survived.)
#[derive(Debug)]
struct DenyKey(&'static str);

impl cairn_types::traits::AuthorizationEngine for DenyKey {
    fn evaluate(&self, input: &cairn_types::authz::AuthzInput) -> cairn_types::authz::Decision {
        use cairn_types::authz::{Action, Decision, DenyReason, Resource};
        let is_delete = matches!(
            input.action,
            Action::DeleteObject | Action::DeleteObjectVersion
        );
        if is_delete {
            if let Resource::Object { key, .. } = &input.resource {
                if key.as_str() == self.0 {
                    return Decision::Deny(DenyReason::DefaultDeny);
                }
            }
        }
        Decision::Allow
    }
}

#[tokio::test]
async fn bulk_delete_mixed_results_have_per_key_codes() {
    // Deny "denied.txt"; everything else is allowed.
    let h = harness_with_authz(Arc::new(DenyKey("denied.txt"))).await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("mxb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    for k in ["allowed.txt", "denied.txt"] {
        drain(
            send(
                &h.svc,
                req(Method::PUT, Some("mxb"), Some(k), &[], &[], b"x".to_vec()),
            )
            .await,
        )
        .await;
    }

    let del = "<Delete>\
        <Object><Key>allowed.txt</Key></Object>\
        <Object><Key>denied.txt</Key></Object>\
        <Object><Key>missing.txt</Key></Object>\
        </Delete>";
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mxb"),
                None,
                &[("delete", "")],
                &[],
                del.as_bytes().to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();

    // The denied key reports AccessDenied, NOT InternalError.
    assert!(
        xml.contains("<Key>denied.txt</Key>") && xml.contains("<Code>AccessDenied</Code>"),
        "denied key must surface its true code: {xml}"
    );
    assert!(
        !xml.contains("InternalError"),
        "no failure should collapse to InternalError: {xml}"
    );
    // The allowed key is deleted.
    assert!(xml.contains("<Deleted>") && xml.contains("allowed.txt"));

    // allowed.txt is gone; denied.txt remains (its delete was rejected).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mxb"),
                Some("allowed.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mxb"),
                Some("denied.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "denied key must NOT be deleted");
}

#[tokio::test]
async fn cors_preflight_match_and_no_match() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("corsb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // Configure a CORS rule allowing https://app.example for GET/PUT with any header.
    let cors = b"<CORSConfiguration><CORSRule>\
        <AllowedOrigin>https://app.example</AllowedOrigin>\
        <AllowedMethod>GET</AllowedMethod>\
        <AllowedMethod>PUT</AllowedMethod>\
        <AllowedHeader>*</AllowedHeader>\
        <ExposeHeader>ETag</ExposeHeader>\
        <MaxAgeSeconds>3000</MaxAgeSeconds>\
        </CORSRule></CORSConfiguration>"
        .to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("corsb"), None, &[("cors", "")], &[], cors),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);

    // A matching preflight returns 200 with the Access-Control-Allow-* headers and Vary: Origin.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::OPTIONS,
                Some("corsb"),
                None,
                &[],
                &[
                    ("origin", "https://app.example"),
                    ("access-control-request-method", "PUT"),
                    (
                        "access-control-request-headers",
                        "x-amz-meta-foo, content-type",
                    ),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "access-control-allow-origin"),
        Some("https://app.example")
    );
    let methods = header(&hdrs, "access-control-allow-methods").unwrap();
    assert!(methods.contains("PUT"), "methods echoed: {methods}");
    assert_eq!(
        header(&hdrs, "access-control-allow-headers"),
        Some("x-amz-meta-foo, content-type")
    );
    assert_eq!(header(&hdrs, "access-control-max-age"), Some("3000"));
    assert_eq!(header(&hdrs, "vary"), Some("Origin"));

    // A preflight from a disallowed origin is rejected with 403.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::OPTIONS,
                Some("corsb"),
                None,
                &[],
                &[
                    ("origin", "https://evil.example"),
                    ("access-control-request-method", "PUT"),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // A preflight for a disallowed METHOD is also rejected.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::OPTIONS,
                Some("corsb"),
                None,
                &[],
                &[
                    ("origin", "https://app.example"),
                    ("access-control-request-method", "DELETE"),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
}

/// Create a bucket and switch it out of `BucketOwnerEnforced` so ACLs are in force.
async fn acl_enabled_bucket(h: &Harness, bucket: &str) {
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some(bucket), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some(bucket),
                None,
                &[("ownershipControls", "")],
                &[],
                b"<OwnershipControls><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule></OwnershipControls>".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "ownership controls set");
}

/// Regression: authorize()'s object-metadata fast path (crates/cairn-protocol/src/service.rs,
/// `needs_object_meta`) must still load the object ACL — and let it actually decide the request —
/// for an ACL-enabled bucket evaluated by the REAL policy engine. Every other ACL test in this file
/// runs under the default `AllowAll` double, so none of them exercise a real Allow/Deny decision
/// driven by an ACL grant; this is that missing case.
#[tokio::test]
async fn acl_grant_is_enforced_by_the_real_policy_engine_under_the_fastpath_guard() {
    let h = harness_with_authz(Arc::new(cairn_authz::PolicyEngine)).await;
    let stranger = member("stranger");

    acl_enabled_bucket(&h, "aclfp").await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("aclfp"),
                Some("k"),
                &[],
                &[],
                b"secret".to_vec(),
            ),
        )
        .await,
    )
    .await;

    // No ACL grant beyond the owner yet: a non-owner member is denied.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::GET,
                Some("aclfp"),
                Some("k"),
                &[],
                &[],
                vec![],
                stranger.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "no ACL grant yet: a non-owner must be denied"
    );

    // The owner grants public-read (AllUsers READ) via a canned ACL.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("aclfp"),
                Some("k"),
                &[("acl", "")],
                &[("x-amz-acl", "public-read")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "owner sets a public-read canned ACL");

    // The SAME non-owner request now succeeds — proving the object ACL genuinely flows from
    // authorize() into a real Allow decision, not merely that the ACL document round-trips.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::GET,
                Some("aclfp"),
                Some("k"),
                &[],
                &[],
                vec![],
                stranger.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "the AllUsers READ grant must let a non-owner through"
    );
    assert_eq!(body, b"secret");

    // Revert to a private ACL (owner-only) and confirm enforcement flips back — the grant is truly
    // load-bearing, not a one-way fallback.
    let private_acl = b"<AccessControlPolicy>\
        <Owner><ID>admin</ID></Owner>\
        <AccessControlList>\
            <Grant><Grantee xsi:type=\"CanonicalUser\"><ID>admin</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>\
        </AccessControlList>\
        </AccessControlPolicy>"
        .to_vec();
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("aclfp"),
                Some("k"),
                &[("acl", "")],
                &[],
                private_acl,
            ),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::GET,
                Some("aclfp"),
                Some("k"),
                &[],
                &[],
                vec![],
                stranger,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "revoking the grant must deny the non-owner again"
    );
}

#[tokio::test]
async fn upload_part_copy_roundtrip() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("upcb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // A source object large enough that a copied range plus a tail still satisfy the 5 MiB part
    // floor. The first 5 MiB will be copied into part 1.
    let mut source = vec![b'a'; 5 * 1024 * 1024];
    source.extend_from_slice(&vec![b'b'; 1024]);
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("upcb"),
                Some("source.bin"),
                &[],
                &[],
                source.clone(),
            ),
        )
        .await,
    )
    .await;

    // Initiate a multipart upload for the destination.
    let (_, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("upcb"),
                Some("dest.bin"),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );

    // Part 1: UploadPartCopy of the first 5 MiB of the source object (bytes 0..5MiB-1).
    let copy_range = format!("bytes=0-{}", 5 * 1024 * 1024 - 1);
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("upcb"),
                Some("dest.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[
                    ("x-amz-copy-source", "/upcb/source.bin"),
                    ("x-amz-copy-source-range", copy_range.as_str()),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "UploadPartCopy returns 200");
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("<CopyPartResult"),
        "CopyPartResult body: {xml}"
    );
    let etag1 = between(&xml, "<ETag>", "</ETag>");

    // Part 2: a small regular body part (the tail).
    let part2 = b"the-copied-tail".to_vec();
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("upcb"),
                Some("dest.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "2")],
                &[],
                part2.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let etag2 = header(&hdrs, "etag").unwrap().to_owned();

    // Complete: part 1's ETag came back quoted in the XML; part 2's came back quoted in a header.
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("upcb"),
                Some("dest.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "complete multipart");

    // Verify the assembled object is the copied range followed by the body tail.
    let (st, _, got) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("upcb"),
                Some("dest.bin"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mut expected = vec![b'a'; 5 * 1024 * 1024];
    expected.extend_from_slice(&part2);
    assert_eq!(got.len(), expected.len());
    assert_eq!(got, expected, "UploadPartCopy bytes round-trip");
}

#[tokio::test]
async fn upload_part_copy_whole_object_no_range() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("upcw"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let part1 = vec![b'z'; 5 * 1024 * 1024];
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("upcw"),
                Some("whole.bin"),
                &[],
                &[],
                part1.clone(),
            ),
        )
        .await,
    )
    .await;
    let (_, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("upcw"),
                Some("out.bin"),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );
    // No x-amz-copy-source-range: the whole source object is copied into the part.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("upcw"),
                Some("out.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[("x-amz-copy-source", "/upcw/whole.bin")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let etag1 = between(&String::from_utf8(body).unwrap(), "<ETag>", "</ETag>");
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("upcw"),
                Some("out.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("upcw"), Some("out.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(got, part1);
}

#[tokio::test]
async fn get_object_attributes_returns_size_and_etag() {
    let h = harness().await;
    let etag = put_simple(&h, "attrb", "obj").await;
    let etag_bare = etag.trim_matches('"').to_owned();

    let (st, hdrs, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("attrb"),
                Some("obj"),
                &[("attributes", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("<GetObjectAttributesResponse"),
        "attributes body: {xml}"
    );
    // "conditional" is 11 bytes.
    assert!(xml.contains("<ObjectSize>11</ObjectSize>"), "{xml}");
    // The ETag is rendered UNQUOTED in GetObjectAttributes.
    assert!(
        xml.contains(&format!("<ETag>{etag_bare}</ETag>")),
        "etag {etag_bare} in {xml}"
    );
    assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));
    assert!(header(&hdrs, "last-modified").is_some());
}

#[tokio::test]
async fn put_object_acl_canned_and_body_roundtrip() {
    let h = harness().await;
    acl_enabled_bucket(&h, "oaclb").await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("oaclb"),
                Some("k"),
                &[],
                &[],
                b"the-body".to_vec(),
            ),
        )
        .await,
    )
    .await;

    // PUT ?acl with a canned header must NOT overwrite the body and must persist the ACL.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("oaclb"),
                Some("k"),
                &[("acl", "")],
                &[("x-amz-acl", "public-read")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "canned put_object_acl");

    // The object body is unchanged.
    let (_, _, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("oaclb"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(body, b"the-body");

    // GET ?acl reflects the canned public-read grant (AllUsers READ).
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("oaclb"),
                Some("k"),
                &[("acl", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("AllUsers"),
        "public-read AllUsers grant: {xml}"
    );
    assert!(xml.contains("<Permission>READ</Permission>"), "{xml}");

    // Now PUT ?acl with an AccessControlPolicy BODY (private — only the owner has FULL_CONTROL).
    let acl_body = b"<AccessControlPolicy>\
        <Owner><ID>admin</ID></Owner>\
        <AccessControlList>\
            <Grant><Grantee xsi:type=\"CanonicalUser\"><ID>admin</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>\
        </AccessControlList>\
        </AccessControlPolicy>"
        .to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("oaclb"),
                Some("k"),
                &[("acl", "")],
                &[],
                acl_body,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "body put_object_acl");

    // GET ?acl now reflects the private ACL — no AllUsers grant remains.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("oaclb"),
                Some("k"),
                &[("acl", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(
        !xml.contains("AllUsers"),
        "private ACL has no AllUsers: {xml}"
    );
    assert!(
        xml.contains("<Permission>FULL_CONTROL</Permission>"),
        "{xml}"
    );
}

#[tokio::test]
async fn put_object_acl_rejected_under_enforced_ownership() {
    let h = harness().await;
    // A default bucket is BucketOwnerEnforced: ACLs are not supported.
    let _ = put_simple(&h, "enfb", "k").await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("enfb"),
                Some("k"),
                &[("acl", "")],
                &[("x-amz-acl", "public-read")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "ACLs disabled under BucketOwnerEnforced (AccessControlListNotSupported)"
    );
}

#[tokio::test]
async fn put_bucket_acl_accepts_body_document() {
    let h = harness().await;
    acl_enabled_bucket(&h, "baclb").await;
    let acl_body = b"<AccessControlPolicy>\
        <Owner><ID>admin</ID></Owner>\
        <AccessControlList>\
            <Grant><Grantee xsi:type=\"CanonicalUser\"><ID>admin</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>\
            <Grant><Grantee xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>\
        </AccessControlList>\
        </AccessControlPolicy>"
        .to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("baclb"),
                None,
                &[("acl", "")],
                &[],
                acl_body,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "PUT bucket?acl with a body document");

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("baclb"),
                None,
                &[("acl", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(xml.contains("AllUsers"), "bucket ACL body persisted: {xml}");
    assert!(xml.contains("<Permission>READ</Permission>"));
}

/// SSE-S3 end to end against the real backends: a PUT carrying
/// `x-amz-server-side-encryption: AES256` stores the object encrypted, echoes the SSE header on the
/// PUT/GET/HEAD responses, returns byte-identical content on GET (including a ranged read), and the
/// ETag equals the plaintext MD5 — proving encryption is transparent to the entity tag (ARCH 27).
#[tokio::test]
async fn sse_s3_put_get_roundtrip_and_etag() {
    let h = harness().await;

    // Create the bucket.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("enc"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // A multi-block payload so the ranged read crosses encrypted block boundaries.
    let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 256) as u8).collect();
    let want_etag = format!("\"{}\"", md5_hex(&payload));

    // PUT with the SSE-S3 header.
    let put = req(
        Method::PUT,
        Some("enc"),
        Some("secret.bin"),
        &[],
        &[
            ("content-type", "application/octet-stream"),
            ("x-amz-server-side-encryption", "AES256"),
        ],
        payload.clone(),
    );
    let (st, hdrs, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK);
    // The ETag is the plaintext MD5 (byte-identical to the unencrypted case).
    assert_eq!(header(&hdrs, "etag"), Some(want_etag.as_str()));
    // The SSE algorithm is echoed on the PUT response.
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256")
    );

    // GET returns the identical bytes plus the SSE response header.
    let (st, hdrs, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("enc"),
                Some("secret.bin"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, payload);
    assert_eq!(header(&hdrs, "etag"), Some(want_etag.as_str()));
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256")
    );

    // HEAD also echoes the SSE header and carries no body.
    let (st, hdrs, body) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("enc"),
                Some("secret.bin"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256")
    );

    // A ranged GET decrypts only the overlapping blocks and returns the matching slice.
    let (st, hdrs, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("enc"),
                Some("secret.bin"),
                &[],
                &[("range", "bytes=10000-19999")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, &payload[10000..20000]);
    assert_eq!(
        header(&hdrs, "content-range"),
        Some("bytes 10000-19999/40000")
    );
}

/// An object PUT without the SSE header is stored unencrypted and never echoes the SSE response
/// header on GET, confirming the feature is opt-in.
#[tokio::test]
async fn put_without_sse_header_is_plaintext_and_no_header() {
    let h = harness().await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("pln"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("pln"),
                Some("a.txt"),
                &[],
                &[("content-type", "text/plain")],
                b"hello world".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(header(&hdrs, "x-amz-server-side-encryption").is_none());

    let (st, hdrs, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("pln"), Some("a.txt"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, b"hello world");
    assert!(header(&hdrs, "x-amz-server-side-encryption").is_none());
}

/// An unrecognized server-side-encryption algorithm (neither `AES256` nor `aws:kms`) is rejected
/// rather than silently stored unencrypted.
#[tokio::test]
async fn put_with_unsupported_sse_mode_is_rejected() {
    let h = harness().await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("kms"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("kms"),
                Some("a.txt"),
                &[],
                &[("x-amz-server-side-encryption", "rot13")],
                b"data".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
}

/// SSE-KMS (ARCH 27, Increment 2): a PUT with `x-amz-server-side-encryption: aws:kms` +
/// `...-aws-kms-key-id` + `...-bucket-key-enabled` stores a `Kms`-mode descriptor, echoes `aws:kms`
/// + the key id + BucketKeyEnabled on PUT/GET/HEAD, and round-trips the bytes byte-identically.
#[tokio::test]
async fn sse_kms_put_get_head_advertises_algorithm_key_id_and_bucket_key() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("kmsb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    let payload = b"kms-encrypted payload".to_vec();
    let put = req(
        Method::PUT,
        Some("kmsb"),
        Some("k.bin"),
        &[],
        &[
            ("x-amz-server-side-encryption", "aws:kms"),
            ("x-amz-server-side-encryption-aws-kms-key-id", "alias/app"),
            ("x-amz-server-side-encryption-bucket-key-enabled", "true"),
        ],
        payload.clone(),
    );
    let (st, hdrs, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("aws:kms")
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption-aws-kms-key-id"),
        Some("alias/app")
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption-bucket-key-enabled"),
        Some("true")
    );

    // Stored with a `kms`-mode descriptor.
    let desc = stored_descriptor(&h, "kmsb", "k.bin").await.unwrap();
    assert_eq!(descriptor_mode(&desc).as_deref(), Some("kms"));

    // GET round-trips the bytes and re-advertises everything.
    let (st, hdrs, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("kmsb"), Some("k.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, payload, "aws:kms encryption is transparent on read");
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("aws:kms")
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption-aws-kms-key-id"),
        Some("alias/app")
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption-bucket-key-enabled"),
        Some("true"),
        "BucketKeyEnabled must round-trip on GET"
    );

    // HEAD echoes the same SSE surface.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::HEAD, Some("kmsb"), Some("k.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("aws:kms")
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption-aws-kms-key-id"),
        Some("alias/app")
    );
}

/// A KMS key-id header WITHOUT `x-amz-server-side-encryption: aws:kms` is a malformed request
/// (`InvalidArgument`), not a silent plaintext write.
#[tokio::test]
async fn sse_kms_key_id_without_algorithm_is_invalid_argument() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("kmsb2"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("kmsb2"),
                Some("k.bin"),
                &[],
                &[("x-amz-server-side-encryption-aws-kms-key-id", "alias/app")],
                b"data".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert!(
        String::from_utf8_lossy(&body).contains("InvalidArgument"),
        "expected InvalidArgument, got {}",
        String::from_utf8_lossy(&body)
    );
    // Nothing was stored.
    assert!(
        h.meta
            .current_version(
                &BucketName::parse("kmsb2").unwrap(),
                &ObjectKey::parse("k.bin").unwrap(),
            )
            .await
            .unwrap()
            .is_none(),
        "a rejected KMS request must not store an object"
    );
}

/// With a `CAIRN_KMS_KEY_IDS` allow-list configured, an `aws:kms` PUT naming a key id NOT on the list
/// is rejected (fail-closed) and stores nothing; a key id ON the list is accepted.
#[tokio::test]
async fn sse_kms_key_id_allow_list_is_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> = Arc::new(cairn_meta::open_in_memory().unwrap());
    let blob: Arc<dyn BlobStore> =
        Arc::new(cairn_blob::LocalBlobStore::open(dir.path()).await.unwrap());
    let clock = Arc::new(cairn_types::testing::TestClock::default());
    let crypto: Arc<dyn cairn_types::traits::Crypto> =
        Arc::new(cairn_crypto::SystemCrypto::new([7u8; 32]));
    let svc = S3Service::new(
        meta.clone(),
        blob,
        Arc::new(cairn_types::testing::AllowAll),
        clock,
        crypto.clone(),
        "us-east-1".to_owned(),
        5 * 1024 * 1024 * 1024,
    )
    .with_key_provider(Arc::new(cairn_protocol::LocalRingProvider::new(
        crypto,
        Some(vec!["alias/allowed".to_owned()]),
    )));

    drain(
        send(
            &svc,
            req(Method::PUT, Some("albkt"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // A key id NOT on the allow-list is rejected, and nothing is stored.
    let (st, _, _) = drain(
        send(
            &svc,
            req(
                Method::PUT,
                Some("albkt"),
                Some("denied.bin"),
                &[],
                &[
                    ("x-amz-server-side-encryption", "aws:kms"),
                    ("x-amz-server-side-encryption-aws-kms-key-id", "alias/nope"),
                ],
                b"data".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert!(
        meta.current_version(
            &BucketName::parse("albkt").unwrap(),
            &ObjectKey::parse("denied.bin").unwrap(),
        )
        .await
        .unwrap()
        .is_none(),
        "a KMS write with a disallowed key id must store nothing"
    );

    // A key id ON the allow-list is accepted.
    let (st, hdrs, _) = drain(
        send(
            &svc,
            req(
                Method::PUT,
                Some("albkt"),
                Some("ok.bin"),
                &[],
                &[
                    ("x-amz-server-side-encryption", "aws:kms"),
                    (
                        "x-amz-server-side-encryption-aws-kms-key-id",
                        "alias/allowed",
                    ),
                ],
                b"data".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("aws:kms")
    );
}

/// A tampered `Kms` descriptor fails closed on GET (crypto rejects the corrupted DEK envelope): the
/// response is an error and never leaks the plaintext or zeros.
#[tokio::test]
async fn sse_kms_tampered_descriptor_fails_closed() {
    use base64::Engine;
    use cairn_types::meta::{Mutation, Precondition};

    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("tkms"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    const TOKEN: &[u8] = b"ZZKMSPLAINTEXTTOKENZZ";
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("tkms"),
                Some("obj.bin"),
                &[],
                &[
                    ("x-amz-server-side-encryption", "aws:kms"),
                    ("x-amz-server-side-encryption-aws-kms-key-id", "alias/app"),
                ],
                TOKEN.to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Corrupt the wrapped DEK in the stored `Kms` descriptor and rewrite the version row.
    let bucket = BucketName::parse("tkms").unwrap();
    let key = ObjectKey::parse("obj.bin").unwrap();
    let mut row = h
        .meta
        .current_version(&bucket, &key)
        .await
        .unwrap()
        .unwrap();
    let mut desc: serde_json::Value =
        serde_json::from_str(row.sse_descriptor.as_ref().unwrap()).unwrap();
    let wrapped = desc["wrapped_dek_b64"].as_str().unwrap();
    let mut raw = base64::engine::general_purpose::STANDARD
        .decode(wrapped.as_bytes())
        .unwrap();
    raw[0] ^= 0xff; // flip a byte so the GCM tag no longer verifies
    desc["wrapped_dek_b64"] =
        serde_json::json!(base64::engine::general_purpose::STANDARD.encode(&raw));
    // Confirm we kept the `kms` mode (still a Kms descriptor, just tampered).
    assert_eq!(desc["mode"], serde_json::json!("kms"));
    row.sse_descriptor = Some(desc.to_string());
    h.meta
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: Vec::new(),
        })
        .await
        .unwrap();

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("tkms"), Some("obj.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_ne!(
        st,
        StatusCode::OK,
        "a tampered KMS descriptor must fail the GET"
    );
    assert!(
        !body.windows(TOKEN.len()).any(|w| w == TOKEN),
        "a fail-closed GET must never leak plaintext, got {body:?}"
    );
}

/// `?encryption` subresource round-trip (ARCH 27): PutBucketEncryption(aws:kms, key id) →
/// GetBucketEncryption returns it → a header-less PUT into that bucket is encrypted + advertised as
/// `aws:kms` with the default key id → DeleteBucketEncryption → GetBucketEncryption is 404 and the
/// bucket SURVIVES (the routing fall-through guard keeps `DELETE ?encryption` off the bucket-delete).
#[tokio::test]
async fn bucket_encryption_subresource_round_trip_and_bucket_survives_delete() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("benc"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // PUT ?encryption with an aws:kms default + key id.
    let put_xml =
        cairn_xml::server_side_encryption_configuration("aws:kms", Some("alias/def"), false);
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("benc"),
                None,
                &[("encryption", "")],
                &[],
                put_xml.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // GET ?encryption returns the stored configuration.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("benc"),
                None,
                &[("encryption", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8_lossy(&body);
    assert!(
        xml.contains("<SSEAlgorithm>aws:kms</SSEAlgorithm>"),
        "{xml}"
    );
    assert!(
        xml.contains("<KMSMasterKeyID>alias/def</KMSMasterKeyID>"),
        "{xml}"
    );

    // A header-less PUT into that bucket picks up the KMS default and advertises it + the key id.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("benc"),
                Some("plain.bin"),
                &[],
                &[],
                b"default-kms bytes".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("aws:kms")
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption-aws-kms-key-id"),
        Some("alias/def"),
        "a bucket KMS default must supply the default key id"
    );

    // DELETE ?encryption clears the configuration.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("benc"),
                None,
                &[("encryption", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);

    // GetBucketEncryption is now a 404.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("benc"),
                None,
                &[("encryption", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // The bucket itself SURVIVED the `DELETE ?encryption` — a HEAD still finds it.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::HEAD, Some("benc"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "the bucket must survive DELETE ?encryption"
    );
}

/// Crypto-review F1 (downgrade): the S3 `?encryption` default-encryption surface shares the same
/// `Encryption` aspect doc as the management-plane mandatory `required` flag, so a `PutBucketEncryption`
/// or `DELETE ?encryption` from the data plane must NOT be able to silently clear that mandatory
/// control. This sets `required:true`, exercises both S3 verbs, and asserts a plaintext client PUT
/// stays refused throughout. Pre-fix, either verb overwrote/wiped the doc and a plaintext PUT then
/// succeeded — a security-control removal from the data plane.
#[tokio::test]
async fn s3_encryption_surface_preserves_mandatory_required_flag() {
    use cairn_types::bucket::{ConfigAspect, ConfigDoc};
    use cairn_types::meta::Mutation;
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("menc"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    h.meta
        .submit(Mutation::SetBucketConfig {
            bucket: BucketName::parse("menc").unwrap(),
            aspect: ConfigAspect::Encryption,
            doc: Some(ConfigDoc(r#"{"required":true}"#.to_owned())),
        })
        .await
        .unwrap();

    macro_rules! plaintext_put_status {
        () => {{
            let (st, _, _) = drain(
                send(
                    &h.svc,
                    req(
                        Method::PUT,
                        Some("menc"),
                        Some("k"),
                        &[],
                        &[],
                        b"x".to_vec(),
                    ),
                )
                .await,
            )
            .await;
            st
        }};
    }
    macro_rules! s3_encryption {
        ($m:expr, $body:expr) => {{
            let (st, _, _) = drain(
                send(
                    &h.svc,
                    req($m, Some("menc"), None, &[("encryption", "")], &[], $body),
                )
                .await,
            )
            .await;
            st
        }};
    }
    // Baseline: `required` with no default algorithm refuses a plaintext client PUT.
    assert_eq!(plaintext_put_status!(), StatusCode::BAD_REQUEST);

    // DeleteBucketEncryption drops only the (absent) default algorithm and must PRESERVE `required`,
    // so a plaintext PUT stays refused. Pre-fix, DELETE wiped the whole aspect and the next plaintext
    // PUT would have been accepted (200) — the downgrade.
    assert_eq!(
        s3_encryption!(Method::DELETE, vec![]),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        plaintext_put_status!(),
        StatusCode::BAD_REQUEST,
        "DeleteBucketEncryption must not clear the mandatory `required` flag"
    );

    // PutBucketEncryption(AES256) sets a default, so a header-less PUT is now ENCRYPTED (200), not
    // refused — the default applies, satisfying `required`. That the `required` flag survived the PUT
    // is proven by the NEXT DeleteBucketEncryption still refusing a plaintext PUT: if the PUT had
    // dropped `required`, this delete would preserve nothing and the plaintext PUT would be accepted.
    let put_xml = cairn_xml::server_side_encryption_configuration("AES256", None, false);
    assert_eq!(
        s3_encryption!(Method::PUT, put_xml.into_bytes()),
        StatusCode::OK
    );
    assert_eq!(
        plaintext_put_status!(),
        StatusCode::OK,
        "with a default algorithm a header-less PUT is encrypted, not refused"
    );
    assert_eq!(
        s3_encryption!(Method::DELETE, vec![]),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        plaintext_put_status!(),
        StatusCode::BAD_REQUEST,
        "PutBucketEncryption must not clear the mandatory `required` flag"
    );
}

/// Bucket default encryption (the `encryption` config aspect): a plain PUT with NO SSE header into
/// a bucket whose default is AES256 stores the object encrypted — the SSE header is echoed on the
/// PUT and GET responses and the bytes round-trip. A sibling bucket without the default stays
/// unencrypted, proving the setting is per-bucket.
#[tokio::test]
async fn bucket_default_encryption_applies_to_plain_puts() {
    use cairn_types::bucket::{ConfigAspect, ConfigDoc};
    use cairn_types::meta::Mutation;

    let h = harness().await;
    for b in ["encdef", "plain"] {
        let (st, _, _) =
            drain(send(&h.svc, req(Method::PUT, Some(b), None, &[], &[], vec![])).await).await;
        assert_eq!(st, StatusCode::OK);
    }

    // Configure the default the way the management API stores it.
    h.meta
        .submit(Mutation::SetBucketConfig {
            bucket: BucketName::parse("encdef").unwrap(),
            aspect: ConfigAspect::Encryption,
            doc: Some(ConfigDoc(r#"{"algorithm":"AES256"}"#.to_owned())),
        })
        .await
        .unwrap();

    let payload = b"default-encrypted bytes".to_vec();

    // No SSE header on the PUT — the bucket default applies.
    let put = req(
        Method::PUT,
        Some("encdef"),
        Some("k"),
        &[],
        &[("content-type", "application/octet-stream")],
        payload.clone(),
    );
    let (st, hdrs, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256"),
        "bucket default encryption must apply to header-less PUTs"
    );

    let (st, hdrs, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("encdef"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, payload);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256")
    );

    // The sibling bucket without the default stays unencrypted.
    let put = req(
        Method::PUT,
        Some("plain"),
        Some("k"),
        &[],
        &[("content-type", "application/octet-stream")],
        payload.clone(),
    );
    let (st, hdrs, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(header(&hdrs, "x-amz-server-side-encryption"), None);
}

/// A replication configuration with one enabled rule scoped to `prefix`, optionally enabling
/// delete-marker replication. The destination is a fixed remote bucket ARN.
fn replication_config_xml(prefix: &str, delete_marker: bool) -> Vec<u8> {
    let dmr = if delete_marker {
        "<DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>"
    } else {
        ""
    };
    format!(
        "<ReplicationConfiguration><Role>arn:aws:iam::1:role/r</Role>\
         <Rule><ID>r1</ID><Status>Enabled</Status><Priority>1</Priority>\
         <Filter><Prefix>{prefix}</Prefix></Filter>{dmr}\
         <Destination><Bucket>arn:aws:s3:::dest</Bucket></Destination></Rule>\
         </ReplicationConfiguration>"
    )
    .into_bytes()
}

async fn set_replication(h: &Harness, bucket: &str, prefix: &str, delete_marker: bool) {
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some(bucket),
                None,
                &[("replication", "")],
                &[],
                replication_config_xml(prefix, delete_marker),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn system_headers_round_trip_and_response_overrides() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("syshdr"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    // PUT with the five system headers (ARCH 13.4).
    let put = req(
        Method::PUT,
        Some("syshdr"),
        Some("k"),
        &[],
        &[
            ("content-type", "text/plain"),
            ("content-encoding", "gzip"),
            ("cache-control", "max-age=99"),
            ("content-disposition", "attachment; filename=a.txt"),
            ("content-language", "en-US"),
            ("expires", "Wed, 21 Oct 2026 07:28:00 GMT"),
        ],
        b"hello".to_vec(),
    );
    let (st, _, _) = drain(send(&h.svc, put).await).await;
    assert_eq!(st, StatusCode::OK);

    // GET echoes each stored system header.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("syshdr"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(header(&hdrs, "content-encoding"), Some("gzip"));
    assert_eq!(header(&hdrs, "cache-control"), Some("max-age=99"));
    assert_eq!(
        header(&hdrs, "content-disposition"),
        Some("attachment; filename=a.txt")
    );
    assert_eq!(header(&hdrs, "content-language"), Some("en-US"));
    assert_eq!(
        header(&hdrs, "expires"),
        Some("Wed, 21 Oct 2026 07:28:00 GMT")
    );

    // HEAD echoes them too.
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::HEAD, Some("syshdr"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(header(&hdrs, "content-encoding"), Some("gzip"));

    // GET response-* overrides REPLACE the corresponding headers (ARCH 21.2), with no duplicate.
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("syshdr"),
                Some("k"),
                &[
                    ("response-content-type", "application/xml"),
                    ("response-cache-control", "no-store"),
                    ("response-content-disposition", "inline"),
                ],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(header(&hdrs, "content-type"), Some("application/xml"));
    assert_eq!(header(&hdrs, "cache-control"), Some("no-store"));
    assert_eq!(header(&hdrs, "content-disposition"), Some("inline"));
    // Exactly one content-type header (override replaced, not appended).
    assert_eq!(
        hdrs.iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case("content-type"))
            .count(),
        1
    );
}

#[tokio::test]
async fn inbound_replica_marks_status_and_skips_outbox() {
    let h = harness().await;
    versioned_bucket(&h, "repl").await;
    set_replication(&h, "repl", "", false).await;

    // A normal PUT enqueues a replication outbox entry.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("repl"),
                Some("a"),
                &[],
                &[],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let _ = hdrs;
    let now = cairn_types::Timestamp::from_secs(4_000_000_000);
    let due = h.meta.list_due_replication(100, now).await.unwrap();
    assert_eq!(due.len(), 1, "normal PUT enqueues replication");

    // A replica PUT (x-amz-meta-cairn-replica: true) does NOT enqueue, and is marked Replica.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("repl"),
                Some("b"),
                &[],
                &[("x-amz-meta-cairn-replica", "true")],
                b"y".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let due = h.meta.list_due_replication(100, now).await.unwrap();
    assert_eq!(due.len(), 1, "replica PUT must not enqueue replication");

    let key_b = ObjectKey::parse("b").unwrap();
    let row = h
        .meta
        .current_version(&BucketName::parse("repl").unwrap(), &key_b)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.replication_status,
        Some(cairn_types::meta::ReplicationStatus::Replica)
    );

    // Audit #16: a non-admin (member) cannot forge the replica marker. The same header from a
    // member is ignored, so the write replicates normally and is NOT recorded as a Replica —
    // closing a replication-evasion / loop-prevention bypass.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("repl"),
                Some("c"),
                &[],
                &[("x-amz-meta-cairn-replica", "true")],
                b"z".to_vec(),
                member("intruder"),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let due = h.meta.list_due_replication(100, now).await.unwrap();
    assert_eq!(
        due.len(),
        2,
        "a member's replica header is ignored, so the write enqueues replication"
    );
    let key_c = ObjectKey::parse("c").unwrap();
    let row_c = h
        .meta
        .current_version(&BucketName::parse("repl").unwrap(), &key_c)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        row_c.replication_status,
        Some(cairn_types::meta::ReplicationStatus::Replica),
        "a member must not be able to self-classify a write as a Replica"
    );
}

/// Mandatory bucket encryption (the `encryption` aspect's `required` flag, ARCH 27): a client PUT
/// whose resolved encryption is "none" is refused (400); an explicit SSE-S3 PUT is accepted; an
/// inbound replica is transparently force-encrypted rather than refused (so enabling the policy can
/// never break replication); and a bucket that pairs `required` with a default algorithm encrypts
/// header-less client uploads instead of refusing them.
#[tokio::test]
async fn mandatory_sse_denies_plaintext_but_exempts_replicas() {
    use cairn_types::bucket::{ConfigAspect, ConfigDoc};
    use cairn_types::meta::Mutation;
    let h = harness().await;

    // A bucket that REQUIRES encryption but sets no default algorithm.
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("must"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    h.meta
        .submit(Mutation::SetBucketConfig {
            bucket: BucketName::parse("must").unwrap(),
            aspect: ConfigAspect::Encryption,
            doc: Some(ConfigDoc(r#"{"required":true}"#.to_owned())),
        })
        .await
        .unwrap();

    // (1) A header-less client PUT would land plaintext → refused with 400.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("must"),
                Some("plain"),
                &[],
                &[],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "an unencrypted client PUT to a mandatory-SSE bucket must be refused"
    );

    // (2) An explicit SSE-S3 PUT is accepted and stored encrypted.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("must"),
                Some("enc"),
                &[],
                &[("x-amz-server-side-encryption", "AES256")],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256")
    );

    // (3) SSE-KMS is now an advertised encryption mode, so it satisfies the mandatory-encryption
    // requirement: the PUT succeeds and advertises `aws:kms`.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("must"),
                Some("kms"),
                &[],
                &[("x-amz-server-side-encryption", "aws:kms")],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "SSE-KMS satisfies mandatory encryption");
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("aws:kms")
    );

    // (4) An inbound replica (admin + marker) without an SSE header is force-encrypted, NOT refused.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("must"),
                Some("rep"),
                &[],
                &[("x-amz-meta-cairn-replica", "true")],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "a replica PUT into a mandatory-SSE bucket must succeed (force-encrypted, not refused)"
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256"),
        "the replica is transparently encrypted"
    );
    let rep_row = h
        .meta
        .current_version(
            &BucketName::parse("must").unwrap(),
            &ObjectKey::parse("rep").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        rep_row.sse_descriptor.is_some(),
        "the replicated object is stored encrypted"
    );

    // (5) A bucket pairing `required` with a default algorithm encrypts a header-less client PUT
    //     (the default applies) rather than refusing it.
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("mustdef"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    h.meta
        .submit(Mutation::SetBucketConfig {
            bucket: BucketName::parse("mustdef").unwrap(),
            aspect: ConfigAspect::Encryption,
            doc: Some(ConfigDoc(
                r#"{"algorithm":"AES256","required":true}"#.to_owned(),
            )),
        })
        .await
        .unwrap();
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mustdef"),
                Some("k"),
                &[],
                &[],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256"),
        "with a default algorithm, a header-less PUT is encrypted, not refused"
    );
}

/// A `required:true`-without-algorithm bucket must STILL refuse a plaintext client PUT even when
/// `CAIRN_ENCRYPT_AT_REST` is on. Transparent at-rest encryption satisfies the data goal but not the
/// client-facing required-bucket contract (an `AtRest` object advertises nothing), so the reject must
/// fire — the client is required to send SSE. (Crypto-review F1: pre-fix, at-rest silently accepted
/// the PUT because the plan carried a DEK.)
#[tokio::test]
async fn mandatory_sse_still_refuses_plaintext_client_put_with_at_rest_on() {
    use cairn_types::bucket::{ConfigAspect, ConfigDoc};
    use cairn_types::meta::Mutation;
    let h = harness_encrypt_at_rest().await;

    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("must"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    h.meta
        .submit(Mutation::SetBucketConfig {
            bucket: BucketName::parse("must").unwrap(),
            aspect: ConfigAspect::Encryption,
            doc: Some(ConfigDoc(r#"{"required":true}"#.to_owned())),
        })
        .await
        .unwrap();

    // A header-less client PUT: at-rest would encrypt it as `AtRest`, but the required-bucket
    // contract demands the CLIENT send SSE — so it is still refused with 400.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("must"),
                Some("plain"),
                &[],
                &[],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "at-rest must NOT let a required bucket silently accept a plaintext client PUT"
    );

    // An explicit SSE-S3 PUT is accepted (satisfies the contract, advertised).
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("must"),
                Some("sealed"),
                &[],
                &[("x-amz-server-side-encryption", "AES256")],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256")
    );
}

/// Resource regression (ARCH 7.5, audit #11): a PUT declaring a `content-length` above the
/// configured object-size ceiling is refused on the HEADER alone — before any body is staged — so a
/// client cannot pin server memory or disk by declaring a huge object. The harness ceiling is 5 GiB.
#[tokio::test]
async fn oversize_content_length_is_refused_before_staging() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("big"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    // ~10 GB declared, a tiny actual body: the declared length alone must trip EntityTooLarge.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("big"),
                Some("k"),
                &[],
                &[("content-length", "9999999999")],
                b"tiny".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "a content-length above the object-size ceiling must be refused (EntityTooLarge)"
    );
    // The object must not have been created.
    assert!(
        h.meta
            .current_version(
                &BucketName::parse("big").unwrap(),
                &ObjectKey::parse("k").unwrap(),
            )
            .await
            .unwrap()
            .is_none(),
        "the over-ceiling PUT must not have staged or committed any object"
    );
}

/// ACL replication, inbound side (ARCH 20.4): an admin-gated replica PUT applies the SOURCE ACL
/// carried as a base64(JSON) `x-amz-meta-cairn-replica-acl` header; a malformed header fails OPEN
/// (no ACL, no 4xx — a 4xx would be terminal at the source and stall that key's outbox); a non-admin
/// cannot apply one (the replica marker is ignored); and `BucketOwnerEnforced` drops it entirely,
/// exactly as it drops a client `x-amz-acl`.
#[tokio::test]
async fn inbound_replica_applies_acl_fail_open_and_respects_ownership() {
    use base64::Engine as _;
    use cairn_types::authz::{Acl, Grant, Grantee, Permission};

    let h = harness().await;
    acl_enabled_bucket(&h, "racl").await;

    let acl = Acl {
        owner: UserId("admin".to_owned()),
        grants: vec![Grant {
            grantee: Grantee::AllUsers,
            permission: Permission::Read,
        }],
    };
    let acl_b64 =
        base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&acl).unwrap());
    let racl = BucketName::parse("racl").unwrap();

    // (1) A valid ACL header on an admin replica PUT is applied verbatim.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("racl"),
                Some("ok"),
                &[],
                &[
                    ("x-amz-meta-cairn-replica", "true"),
                    ("x-amz-meta-cairn-replica-acl", acl_b64.as_str()),
                ],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let ok_row = h
        .meta
        .current_version(&racl, &ObjectKey::parse("ok").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ok_row.acl.as_ref(),
        Some(&acl),
        "a valid replica ACL header is applied verbatim"
    );

    // (2) A malformed ACL header fails OPEN: 2xx and no ACL stored (never a 4xx that stalls the outbox).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("racl"),
                Some("bad"),
                &[],
                &[
                    ("x-amz-meta-cairn-replica", "true"),
                    ("x-amz-meta-cairn-replica-acl", "!!!not-base64!!!"),
                ],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "a malformed replica ACL must fail open, not error"
    );
    let bad_row = h
        .meta
        .current_version(&racl, &ObjectKey::parse("bad").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(
        bad_row.acl.is_none(),
        "a malformed replica ACL header stores no ACL (fail-open)"
    );

    // (3) A non-admin cannot apply a replica ACL: the marker is ignored, and with no x-amz-acl the
    //     object gets no ACL.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("racl"),
                Some("mem"),
                &[],
                &[
                    ("x-amz-meta-cairn-replica", "true"),
                    ("x-amz-meta-cairn-replica-acl", acl_b64.as_str()),
                ],
                b"x".to_vec(),
                member("intruder"),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mem_row = h
        .meta
        .current_version(&racl, &ObjectKey::parse("mem").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(
        mem_row.acl.is_none(),
        "a member's replica ACL header is ignored"
    );

    // (4) Under BucketOwnerEnforced (the default ownership) a replica ACL is dropped, just like a
    //     client x-amz-acl.
    versioned_bucket(&h, "boe").await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("boe"),
                Some("k"),
                &[],
                &[
                    ("x-amz-meta-cairn-replica", "true"),
                    ("x-amz-meta-cairn-replica-acl", acl_b64.as_str()),
                ],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let boe_row = h
        .meta
        .current_version(
            &BucketName::parse("boe").unwrap(),
            &ObjectKey::parse("k").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        boe_row.acl.is_none(),
        "BucketOwnerEnforced drops the replicated ACL"
    );
}

#[tokio::test]
async fn inbound_replica_preserves_version_id_idempotently() {
    let h = harness().await;
    versioned_bucket(&h, "rvp").await;
    let bkt = BucketName::parse("rvp").unwrap();
    let key = ObjectKey::parse("k").unwrap();
    let pinned = "00000000000000000000000000000abc";

    // An admin replica PUT carrying the preserved version id stores the version under THAT id.
    let do_replica = || async {
        drain(
            send(
                &h.svc,
                req(
                    Method::PUT,
                    Some("rvp"),
                    Some("k"),
                    &[],
                    &[
                        ("x-amz-meta-cairn-replica", "true"),
                        ("x-amz-meta-cairn-replica-version-id", pinned),
                    ],
                    b"replicated-bytes".to_vec(),
                ),
            )
            .await,
        )
        .await
    };
    let (st, _, _) = do_replica().await;
    assert_eq!(st, StatusCode::OK);
    let cur = h.meta.current_version(&bkt, &key).await.unwrap().unwrap();
    assert_eq!(
        cur.version_id.as_str(),
        pinned,
        "the source version id is preserved"
    );
    assert_eq!(
        cur.replication_status,
        Some(cairn_types::meta::ReplicationStatus::Replica)
    );

    // Re-delivery of the same (key, version id) is an idempotent upsert — still exactly one version.
    let (st, _, _) = do_replica().await;
    assert_eq!(st, StatusCode::OK);
    let versions = h
        .meta
        .list_versions(
            &bkt,
            &cairn_types::meta::ListQuery {
                limit: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        versions.items.len(),
        1,
        "re-delivery did not create a duplicate version"
    );

    // A NON-admin member cannot pin a version id (audit #16): the header is ignored and a fresh id
    // is minted, so the write is a normal (non-replica) version, not the pinned one.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("rvp"),
                Some("k2"),
                &[],
                &[
                    ("x-amz-meta-cairn-replica", "true"),
                    ("x-amz-meta-cairn-replica-version-id", pinned),
                ],
                b"forged".to_vec(),
                member("intruder"),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let cur2 = h
        .meta
        .current_version(&bkt, &ObjectKey::parse("k2").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        cur2.version_id.as_str(),
        pinned,
        "a member must not be able to pin an arbitrary version id"
    );
    assert_ne!(
        cur2.replication_status,
        Some(cairn_types::meta::ReplicationStatus::Replica)
    );
}

#[tokio::test]
async fn replication_config_requires_versioning() {
    let h = harness().await;
    // A plain, unversioned bucket.
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("rcv"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // Configuring replication on an unversioned bucket is refused (replication ships versions).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("rcv"),
                None,
                &[("replication", "")],
                &[],
                replication_config_xml("", false),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "replication config must require versioning"
    );

    // Enable versioning, and the same config is now accepted — so a rule can only exist on a
    // versioned bucket, keeping new-write replication and resync consistent.
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("rcv"),
                None,
                &[("versioning", "")],
                &[],
                b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                    .to_vec(),
            ),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("rcv"),
                None,
                &[("replication", "")],
                &[],
                replication_config_xml("", false),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NO_CONTENT,
        "replication config is accepted once the bucket is versioned"
    );
}

#[tokio::test]
async fn delete_marker_replication_enqueues_when_enabled() {
    let h = harness().await;
    versioned_bucket(&h, "dmr").await;
    set_replication(&h, "dmr", "", true).await;
    let now = cairn_types::Timestamp::from_secs(4_000_000_000);

    // PUT then plain DELETE inserts a delete marker; with delete_marker_replication on, the
    // marker is enqueued (one entry for the object create + one for the marker).
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("dmr"), Some("k"), &[], &[], b"x".to_vec()),
        )
        .await,
    )
    .await;
    let after_put = h.meta.list_due_replication(100, now).await.unwrap().len();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::DELETE, Some("dmr"), Some("k"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    let after_delete = h.meta.list_due_replication(100, now).await.unwrap();
    assert_eq!(
        after_delete.len(),
        after_put + 1,
        "delete-marker replication enqueues an entry when enabled"
    );
    assert!(
        after_delete
            .iter()
            .any(|e| e.operation == cairn_types::meta::ReplicationOp::DeleteMarker),
        "a DeleteMarker outbox op is present"
    );
}

/// A multipart-completed object on a replication-enabled bucket must enqueue replication exactly
/// like a single PUT (audit: CompleteMultipart previously hard-coded `replication: Vec::new()`, so
/// large multipart uploads — the objects you most want replicated — silently never shipped).
#[tokio::test]
async fn multipart_complete_enqueues_replication() {
    let h = harness().await;
    versioned_bucket(&h, "mprepl").await;
    set_replication(&h, "mprepl", "", false).await;
    let now = cairn_types::Timestamp::from_secs(4_000_000_000);

    // Initiate.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mprepl"),
                Some("big.bin"),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );

    // Two parts (part 1 must be >= 5 MiB).
    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = b"tail".to_vec();
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mprepl"),
                Some("big.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[],
                part1,
            ),
        )
        .await,
    )
    .await;
    let etag1 = header(&hdrs, "etag").unwrap().to_owned();
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mprepl"),
                Some("big.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "2")],
                &[],
                part2,
            ),
        )
        .await,
    )
    .await;
    let etag2 = header(&hdrs, "etag").unwrap().to_owned();

    // Nothing is owed yet — only the completion enqueues.
    assert_eq!(
        h.meta.list_due_replication(100, now).await.unwrap().len(),
        0
    );

    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mprepl"),
                Some("big.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let due = h.meta.list_due_replication(100, now).await.unwrap();
    assert_eq!(due.len(), 1, "multipart completion enqueues replication");
    assert_eq!(due[0].key.as_str(), "big.bin");
    assert_eq!(
        due[0].operation,
        cairn_types::meta::ReplicationOp::ObjectCreate
    );
}

/// A server-side copy into a replication-enabled bucket must enqueue replication for the new
/// destination version (audit: CopyObject previously hard-coded `replication: Vec::new()`, so
/// copied objects silently never propagated).
#[tokio::test]
async fn copy_object_enqueues_replication() {
    let h = harness().await;
    versioned_bucket(&h, "cprepl").await;
    set_replication(&h, "cprepl", "", false).await;
    let now = cairn_types::Timestamp::from_secs(4_000_000_000);

    // Source PUT enqueues one entry.
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cprepl"),
                Some("src.txt"),
                &[],
                &[("content-type", "text/plain")],
                b"original".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        h.meta.list_due_replication(100, now).await.unwrap().len(),
        1
    );

    // Copy src -> dst within the replicated bucket.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cprepl"),
                Some("dst.txt"),
                &[],
                &[("x-amz-copy-source", "/cprepl/src.txt")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let due = h.meta.list_due_replication(100, now).await.unwrap();
    assert_eq!(due.len(), 2, "the copy enqueues a second replication entry");
    assert!(
        due.iter().any(|e| e.key.as_str() == "dst.txt"
            && e.operation == cairn_types::meta::ReplicationOp::ObjectCreate),
        "the copied destination object is enqueued, got {due:?}"
    );
}

#[tokio::test]
async fn bulk_delete_reports_marker_and_rejects_oversize() {
    let h = harness().await;
    versioned_bucket(&h, "bdm").await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("bdm"), Some("k"), &[], &[], b"x".to_vec()),
        )
        .await,
    )
    .await;

    // A plain bulk delete in a versioned bucket inserts a marker; the result reports it.
    let del = "<Delete><Object><Key>k</Key></Object></Delete>";
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("bdm"),
                None,
                &[("delete", "")],
                &[],
                del.as_bytes().to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(xml.contains("<DeleteMarker>true</DeleteMarker>"), "{xml}");
    assert!(xml.contains("<DeleteMarkerVersionId>"), "{xml}");

    // A batch of >1000 keys is rejected as MalformedXML (400).
    let mut big = String::from("<Delete>");
    for i in 0..1001 {
        big.push_str(&format!("<Object><Key>k{i}</Key></Object>"));
    }
    big.push_str("</Delete>");
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("bdm"),
                None,
                &[("delete", "")],
                &[],
                big.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bulk_delete_suspended_inserts_null_marker_not_permanent() {
    let h = harness().await;
    versioned_bucket(&h, "bdsus").await;
    // Write an identified version while Enabled.
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("bdsus"),
                Some("k"),
                &[],
                &[],
                b"keep".to_vec(),
            ),
        )
        .await,
    )
    .await;
    // Suspend versioning.
    let vcfg =
        b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>".to_vec();
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("bdsus"),
                None,
                &[("versioning", "")],
                &[],
                vcfg,
            ),
        )
        .await,
    )
    .await;

    // A bulk delete in a Suspended bucket must insert a NULL-version delete marker — NOT permanently
    // remove the object (the prior identified version must survive). Previously the bulk path
    // collapsed Suspended to Unversioned and destroyed data.
    let del = "<Delete><Object><Key>k</Key></Object></Delete>";
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("bdsus"),
                None,
                &[("delete", "")],
                &[],
                del.as_bytes().to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("<DeleteMarker>true</DeleteMarker>"),
        "suspended bulk delete inserts a marker: {xml}"
    );

    // The earlier identified version is retained (data not destroyed): a versions listing still
    // shows a real <Version> entry alongside the new delete marker.
    let (_, _, vbody) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("bdsus"),
                None,
                &[("versions", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let vxml = String::from_utf8(vbody).unwrap();
    assert!(
        vxml.contains("<Version>"),
        "identified version retained under Suspended bulk delete: {vxml}"
    );
}

/// Regression: a DUPLICATE key (the same key listed twice) in one bulk delete against a
/// Suspended-versioning bucket must NOT race itself. Bulk-delete entries run concurrently across
/// DISTINCT keys for throughput, but a Suspended bare-key delete is a DeleteVersion-then-
/// CreateDeleteMarker two-step, and CreateDeleteMarker is a bare INSERT with no upsert — two
/// duplicate entries both running that two-step concurrently could have both DeleteVersion calls
/// land before either CreateDeleteMarker, so the second insert collides on the
/// (bucket, key, version_id) UNIQUE constraint and surfaces a spurious InternalError. Both entries
/// for the duplicate key must succeed with no error.
#[tokio::test]
async fn bulk_delete_duplicate_key_in_suspended_bucket_does_not_race() {
    let h = harness().await;
    versioned_bucket(&h, "bddup").await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("bddup"),
                Some("k"),
                &[],
                &[],
                b"v1".to_vec(),
            ),
        )
        .await,
    )
    .await;
    let vcfg =
        b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>".to_vec();
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("bddup"),
                None,
                &[("versioning", "")],
                &[],
                vcfg,
            ),
        )
        .await,
    )
    .await;

    // The SAME key listed twice in one request — permitted by the API.
    let del = "<Delete><Object><Key>k</Key></Object><Object><Key>k</Key></Object></Delete>";
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("bddup"),
                None,
                &[("delete", "")],
                &[],
                del.as_bytes().to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(
        !xml.contains("<Error>"),
        "a duplicate key must not race itself into a spurious error: {xml}"
    );
    assert_eq!(
        xml.matches("<DeleteMarker>true</DeleteMarker>").count(),
        2,
        "both duplicate entries must succeed with a delete-marker insert: {xml}"
    );
}

// ===========================================================================================
// Object Lock / WORM / retention / legal hold (ARCH 13/15)
// ===========================================================================================

/// Create an Object-Lock-enabled bucket, PUT one object, and return its `(version_id)`.
async fn lock_bucket_with_object(h: &Harness, bucket: &str, key: &str) -> String {
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some(bucket),
                None,
                &[],
                &[("x-amz-bucket-object-lock-enabled", "true")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "create object-lock bucket");
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some(bucket),
                Some(key),
                &[],
                &[],
                b"payload".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "put object");
    header(&hdrs, "x-amz-version-id")
        .expect("object-lock bucket is versioned, so a PUT returns a version id")
        .to_owned()
}

fn retention_body(mode: &str, until: &str) -> Vec<u8> {
    format!("<Retention><Mode>{mode}</Mode><RetainUntilDate>{until}</RetainUntilDate></Retention>")
        .into_bytes()
}

/// A bucket created with Object Lock is forced to versioning Enabled and reports its lock config.
#[tokio::test]
async fn object_lock_create_forces_versioning_and_reports_config() {
    let h = harness().await;
    let _v = lock_bucket_with_object(&h, "olbucket", "k").await;

    // Versioning is Enabled (forced by Object Lock).
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("olbucket"),
                None,
                &[("versioning", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("<Status>Enabled</Status>"),
        "versioning forced Enabled: {xml}"
    );

    // GET ?object-lock reports it enabled.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("olbucket"),
                None,
                &[("object-lock", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"),
        "{xml}"
    );
}

/// COMPLIANCE retention can never be deleted or weakened before it expires — not even with the
/// governance-bypass header.
#[tokio::test]
async fn object_lock_compliance_is_immutable() {
    let h = harness().await;
    let v = lock_bucket_with_object(&h, "compl", "doc").await;

    // Apply a COMPLIANCE retention far in the future.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("compl"),
                Some("doc"),
                &[("retention", ""), ("versionId", &v)],
                &[],
                retention_body("COMPLIANCE", "2099-01-01T00:00:00Z"),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "set compliance retention");

    // Permanent version delete is denied.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("compl"),
                Some("doc"),
                &[("versionId", &v)],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN, "compliance blocks delete");

    // Even WITH the bypass header it stays denied (compliance is never bypassable).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("compl"),
                Some("doc"),
                &[("versionId", &v)],
                &[("x-amz-bypass-governance-retention", "true")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "compliance ignores bypass header"
    );

    // Shortening the COMPLIANCE retention is denied too.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("compl"),
                Some("doc"),
                &[("retention", ""), ("versionId", &v)],
                &[],
                retention_body("COMPLIANCE", "2030-01-01T00:00:00Z"),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN, "compliance cannot be shortened");
}

/// GOVERNANCE retention blocks a permanent delete, but the bypass header (with permission) lifts it.
#[tokio::test]
async fn object_lock_governance_bypass() {
    let h = harness().await;
    let v = lock_bucket_with_object(&h, "gov", "doc").await;

    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("gov"),
                Some("doc"),
                &[("retention", ""), ("versionId", &v)],
                &[],
                retention_body("GOVERNANCE", "2099-01-01T00:00:00Z"),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Without the bypass header: denied.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("gov"),
                Some("doc"),
                &[("versionId", &v)],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "governance blocks without bypass"
    );

    // With the bypass header (AllowAll authz grants the action): permitted.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("gov"),
                Some("doc"),
                &[("versionId", &v)],
                &[("x-amz-bypass-governance-retention", "true")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT, "governance delete with bypass");
}

/// Legal hold blocks a permanent delete regardless of retention, and releasing it re-enables delete.
#[tokio::test]
async fn object_lock_legal_hold_blocks_then_releases() {
    let h = harness().await;
    let v = lock_bucket_with_object(&h, "lhold", "doc").await;

    // Turn legal hold ON.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("lhold"),
                Some("doc"),
                &[("legal-hold", ""), ("versionId", &v)],
                &[],
                b"<LegalHold><Status>ON</Status></LegalHold>".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Delete denied while held — even with the governance-bypass header.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("lhold"),
                Some("doc"),
                &[("versionId", &v)],
                &[("x-amz-bypass-governance-retention", "true")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN, "legal hold blocks delete");

    // Release it.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("lhold"),
                Some("doc"),
                &[("legal-hold", ""), ("versionId", &v)],
                &[],
                b"<LegalHold><Status>OFF</Status></LegalHold>".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Now the delete succeeds.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("lhold"),
                Some("doc"),
                &[("versionId", &v)],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NO_CONTENT,
        "delete after legal-hold release"
    );
}

/// A bucket default retention is stamped onto new objects, surfaced by GET ?retention and echoed
/// on HEAD; and an over-the-wire GET ?legal-hold round-trips.
#[tokio::test]
async fn object_lock_default_retention_stamped_and_echoed() {
    let h = harness().await;
    // Create lock-enabled bucket (no object yet).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("def"),
                None,
                &[],
                &[("x-amz-bucket-object-lock-enabled", "true")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Configure a default GOVERNANCE retention of 30 days.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("def"),
                None,
                &[("object-lock", "")],
                &[],
                b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                  <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>30</Days></DefaultRetention>\
                  </Rule></ObjectLockConfiguration>"
                    .to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "set default retention");

    // PUT an object — default retention is stamped.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("def"), Some("d"), &[], &[], b"x".to_vec()),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let v = header(&hdrs, "x-amz-version-id").unwrap().to_owned();

    // GET ?retention shows a GOVERNANCE retention.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("def"),
                Some("d"),
                &[("retention", ""), ("versionId", &v)],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("<Mode>GOVERNANCE</Mode>"),
        "default retention surfaced: {xml}"
    );

    // HEAD echoes the lock headers.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::HEAD, Some("def"), Some("d"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(header(&hdrs, "x-amz-object-lock-mode"), Some("GOVERNANCE"));
    assert_eq!(header(&hdrs, "x-amz-object-lock-legal-hold"), Some("OFF"));

    // And the default retention blocks an unbypassed permanent delete.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("def"),
                Some("d"),
                &[("versionId", &v)],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN, "default retention blocks delete");
}

/// Retention requires a lock-enabled bucket; object-lock config requires versioning.
#[tokio::test]
async fn object_lock_guards() {
    let h = harness().await;
    // A plain (non-lock) bucket.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("plain"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("plain"),
                Some("k"),
                &[],
                &[],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let _ = hdrs;

    // PUT ?retention on a non-lock bucket -> 400 InvalidRequest.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("plain"),
                Some("k"),
                &[("retention", "")],
                &[],
                retention_body("GOVERNANCE", "2099-01-01T00:00:00Z"),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "retention needs a lock-enabled bucket"
    );

    // PUT ?object-lock (enabled) on a non-versioned bucket -> 400.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("plain"),
                None,
                &[("object-lock", "")],
                &[],
                b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                  </ObjectLockConfiguration>"
                    .to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "object-lock needs versioning");
}

// ===========================================================================================
// Webhook event notifications (ARCH 20-style): emission on object events
// ===========================================================================================

use cairn_types::notification::{NotificationConfig, WebhookEndpoint};

async fn set_notifications(
    meta: &Arc<dyn MetadataStore>,
    bucket: &str,
    endpoints: Vec<WebhookEndpoint>,
) {
    let config = NotificationConfig { endpoints };
    meta.submit(cairn_types::Mutation::SetBucketConfig {
        bucket: BucketName::parse(bucket).unwrap(),
        aspect: cairn_types::bucket::ConfigAspect::Notification,
        doc: Some(cairn_types::bucket::ConfigDoc(
            serde_json::to_string(&config).unwrap(),
        )),
    })
    .await
    .unwrap();
}

fn endpoint(id: &str, events: &[&str], prefix: Option<&str>) -> WebhookEndpoint {
    WebhookEndpoint {
        id: id.to_owned(),
        url: "http://sink.test/hook".to_owned(),
        events: events.iter().map(|s| (*s).to_owned()).collect(),
        prefix: prefix.map(str::to_owned),
        suffix: None,
        secret: None,
    }
}

/// A PUT and a DELETE on a notification-configured bucket enqueue the matching events with the
/// S3-event-record JSON shape; a non-matching prefix is filtered out.
#[tokio::test]
async fn webhook_events_emitted_on_put_and_delete() {
    let h = harness().await;
    let now = cairn_types::Timestamp::from_secs(2_000_000_000); // TestClock default is fixed; due immediately.

    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("evbucket"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    // Two endpoints: one catches everything, one only the "logs/" prefix.
    set_notifications(
        &h.meta,
        "evbucket",
        vec![
            endpoint("all", &["s3:ObjectCreated:*", "s3:ObjectRemoved:*"], None),
            endpoint("logs", &["s3:ObjectCreated:*"], Some("logs/")),
        ],
    )
    .await;

    // PUT a key NOT under logs/ — only the "all" endpoint matches.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("evbucket"),
                Some("data.txt"),
                &[],
                &[],
                b"hi".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let due = h.meta.list_due_webhooks(100, now).await.unwrap();
    assert_eq!(
        due.len(),
        1,
        "only the catch-all endpoint matched the non-logs key"
    );
    let e = &due[0];
    assert_eq!(e.endpoint_id, "all");
    assert!(matches!(
        e.event,
        cairn_types::notification::EventKind::ObjectCreatedPut
    ));
    // Payload is the S3 event-record JSON.
    let v: serde_json::Value = serde_json::from_str(&e.payload).unwrap();
    assert_eq!(v["Records"][0]["eventName"], "s3:ObjectCreated:Put");
    assert_eq!(v["Records"][0]["s3"]["bucket"]["name"], "evbucket");
    assert_eq!(v["Records"][0]["s3"]["object"]["key"], "data.txt");

    // PUT under logs/ — BOTH endpoints match → two new entries.
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("evbucket"),
                Some("logs/a.log"),
                &[],
                &[],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    let due = h.meta.list_due_webhooks(100, now).await.unwrap();
    let logs_entries: Vec<_> = due
        .iter()
        .filter(|e| e.key.as_str() == "logs/a.log")
        .collect();
    assert_eq!(
        logs_entries.len(),
        2,
        "both endpoints matched the logs/ key"
    );

    // DELETE the data.txt key (unversioned bucket → ObjectRemoved:Delete) — only "all" subscribes.
    drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("evbucket"),
                Some("data.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let removed: Vec<_> = h
        .meta
        .list_due_webhooks(100, now)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| {
            matches!(
                e.event,
                cairn_types::notification::EventKind::ObjectRemovedDelete
            )
        })
        .collect();
    assert_eq!(removed.len(), 1, "delete emitted one ObjectRemoved event");
    assert_eq!(removed[0].endpoint_id, "all");
}

/// A bucket with no notification config emits nothing (the common, zero-overhead path).
#[tokio::test]
async fn no_webhook_config_emits_nothing() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("plainb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("plainb"),
                Some("k"),
                &[],
                &[],
                b"x".to_vec(),
            ),
        )
        .await,
    )
    .await;
    let due = h
        .meta
        .list_due_webhooks(100, cairn_types::Timestamp::from_secs(2_000_000_000))
        .await
        .unwrap();
    assert!(
        due.is_empty(),
        "no notifications configured → no events enqueued"
    );
}

// ===========================================================================================
// STS session credentials: the owner/admin short-circuit is suppressed (ARCH 14)
// ===========================================================================================

/// A session principal for `user` carrying an optional scoped policy (always least-privilege:
/// Member role, `is_session = true`).
fn session_of(user: &str, policy: Option<Box<cairn_types::authz::Policy>>) -> Principal {
    Principal {
        user_id: UserId(user.to_owned()),
        display_name: user.to_owned(),
        access_key_id: format!("{user}-session"),
        role: Role::Member,
        method: AuthMethod::SigV4Header,
        chunk_signing: None,
        user_policy: policy,
        is_session: true,
    }
}

/// A session derived from a bucket's owner does NOT inherit the owner short-circuit: with no scoped
/// grant it is denied, even though a plain (non-session) owner principal is allowed.
#[tokio::test]
async fn session_does_not_inherit_owner_bypass() {
    let h = harness_with_authz(Arc::new(cairn_authz::PolicyEngine)).await;
    let owner = member("owner");

    // Owner creates a bucket and writes an object (owner short-circuit allows both).
    drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("ownerbkt"),
                None,
                &[],
                &[],
                vec![],
                owner.clone(),
            ),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::PUT,
                Some("ownerbkt"),
                Some("obj"),
                &[],
                &[],
                b"data".to_vec(),
                owner.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // A session for the SAME owner identity, but with no scoped policy, is denied (the owner
    // short-circuit is suppressed for sessions — least privilege).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::GET,
                Some("ownerbkt"),
                Some("obj"),
                &[],
                &[],
                vec![],
                session_of("owner", None),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "session has no implicit owner access"
    );

    // Control: the plain (non-session) owner principal CAN read it (owner short-circuit applies).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::GET,
                Some("ownerbkt"),
                Some("obj"),
                &[],
                &[],
                vec![],
                owner.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "the real owner still has access");

    // A session WITH a scoped policy granting the read is allowed — exactly what it was granted.
    let policy: cairn_types::authz::Policy = cairn_authz::parse_user_policy(
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::ownerbkt/*"}]}"#,
    )
    .unwrap();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req_with_principal(
                Method::GET,
                Some("ownerbkt"),
                Some("obj"),
                &[],
                &[],
                vec![],
                session_of("owner", Some(Box::new(policy))),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "a session can do exactly what its scoped policy grants"
    );
}

/// Audit 2026-07: a real (non-preflight) cross-origin request carrying an Origin header must get
/// Access-Control-Allow-Origin/Expose-Headers/Vary on the RESPONSE, not just the OPTIONS preflight —
/// otherwise the browser blocks the response body even though the request succeeded.
#[tokio::test]
async fn cors_actual_request_gets_response_headers() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("corsc"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let cors = b"<CORSConfiguration><CORSRule>\
        <AllowedOrigin>https://app.example</AllowedOrigin>\
        <AllowedMethod>GET</AllowedMethod>\
        <ExposeHeader>ETag</ExposeHeader>\
        </CORSRule></CORSConfiguration>"
        .to_vec();
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("corsc"), None, &[("cors", "")], &[], cors),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    // Store an object.
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("corsc"),
                Some("k"),
                &[],
                &[],
                b"hi".to_vec(),
            ),
        )
        .await,
    )
    .await;

    // A real cross-origin GET must carry the CORS response headers.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("corsc"),
                Some("k"),
                &[],
                &[("origin", "https://app.example")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "access-control-allow-origin"),
        Some("https://app.example"),
        "actual cross-origin GET echoes the allowed origin"
    );
    assert_eq!(header(&hdrs, "vary"), Some("Origin"));
    assert_eq!(header(&hdrs, "access-control-expose-headers"), Some("ETag"));

    // A cross-origin request from a DISALLOWED origin gets no CORS headers.
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("corsc"),
                Some("k"),
                &[],
                &[("origin", "https://evil.example")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(header(&hdrs, "access-control-allow-origin"), None);
}

// =================================================================================================
// Listing marker semantics (conformance/listing.py). S3 defines `marker` (v1) and `start-after`
// (v2) as the SAME exclusive parameter — "Amazon S3 starts listing AFTER this specified key" — and
// v1's `NextMarker` as the LAST key the page returned. Cairn had both halves inverted: an inclusive
// marker paired with a NextMarker naming the first key NOT returned. That pair is self-consistent,
// so Cairn's own pagination loop was duplicate-free and the defect stayed invisible; a client
// following the AWS contract (resume from the last key it saw) re-read one key per page boundary.
// These tests pin BOTH halves, since moving only one skips or duplicates a key.
// =================================================================================================

/// Create `bucket` and PUT each key with a one-byte body.
async fn seed_keys(h: &Harness, bucket: &str, keys: &[&str]) {
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some(bucket), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    for k in keys {
        let (st, _, _) = drain(
            send(
                &h.svc,
                req(Method::PUT, Some(bucket), Some(k), &[], &[], b"x".to_vec()),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
    }
}

/// GET the bucket listing with `query` and return the response body as a string.
async fn list_body(h: &Harness, bucket: &str, query: &[(&str, &str)]) -> String {
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(Method::GET, Some(bucket), None, query, &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    String::from_utf8(body).unwrap()
}

/// Every `<Key>` in a listing body, in document order.
fn keys_of(body: &str) -> Vec<String> {
    body.match_indices("<Key>")
        .map(|(i, _)| {
            let rest = &body[i + "<Key>".len()..];
            rest[..rest.find("</Key>").unwrap()].to_owned()
        })
        .collect()
}

#[tokio::test]
async fn v1_marker_is_exclusive_and_next_marker_is_the_last_key_returned() {
    let h = harness().await;
    let keys: Vec<String> = (0..10).map(|i| format!("k{i:02}")).collect();
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    seed_keys(&h, "v1mark", &refs).await;

    // NextMarker names the LAST key of the page, not the first key withheld.
    let page1 = list_body(&h, "v1mark", &[("max-keys", "3")]).await;
    assert_eq!(keys_of(&page1), ["k00", "k01", "k02"]);
    assert!(page1.contains("<IsTruncated>true</IsTruncated>"));
    assert_eq!(
        between(&page1, "<NextMarker>", "</NextMarker>"),
        "k02",
        "NextMarker is the last key returned, not the first one withheld: {page1}"
    );

    // `marker` is EXCLUSIVE: listing resumes strictly after the named key.
    let after = list_body(&h, "v1mark", &[("marker", "k04")]).await;
    assert_eq!(
        keys_of(&after),
        ["k05", "k06", "k07", "k08", "k09"],
        "marker=k04 must start at k05"
    );

    // The sharpest evidence the old behavior was wrong: `marker` and its v2 sibling `start-after`
    // are the same S3 parameter, so they must agree exactly.
    let start_after = list_body(&h, "v1mark", &[("list-type", "2"), ("start-after", "k04")]).await;
    assert_eq!(
        keys_of(&after),
        keys_of(&start_after),
        "v1 marker and v2 start-after must agree on exclusivity"
    );

    // The whole loop still returns every key exactly once, in order — the property that must not
    // regress while both halves move.
    let mut seen: Vec<String> = Vec::new();
    let mut marker: Option<String> = None;
    for _ in 0..25 {
        let body = match &marker {
            Some(m) => list_body(&h, "v1mark", &[("max-keys", "3"), ("marker", m)]).await,
            None => list_body(&h, "v1mark", &[("max-keys", "3")]).await,
        };
        seen.extend(keys_of(&body));
        if !body.contains("<IsTruncated>true</IsTruncated>") {
            break;
        }
        marker = Some(between(&body, "<NextMarker>", "</NextMarker>"));
    }
    assert_eq!(seen, keys, "v1 pagination returns every key exactly once");
}

#[tokio::test]
async fn v1_delimiter_pagination_advances_past_a_common_prefix() {
    let h = harness().await;
    seed_keys(
        &h,
        "v1delim",
        &[
            "a/b/c1.txt",
            "a/b/c2.txt",
            "a/d.txt",
            "b/e.txt",
            "m::n",
            "m::o",
        ],
    )
    .await;

    // The page is all CommonPrefixes, so NextMarker is the last PREFIX — the last entry returned in
    // result order, keys and prefixes interleaved.
    let page1 = list_body(&h, "v1delim", &[("delimiter", "/"), ("max-keys", "2")]).await;
    assert!(page1.contains("<Prefix>a/</Prefix>") && page1.contains("<Prefix>b/</Prefix>"));
    assert!(page1.contains("<IsTruncated>true</IsTruncated>"));
    let next = between(&page1, "<NextMarker>", "</NextMarker>");
    assert_eq!(next, "b/", "NextMarker is the last CommonPrefix returned");

    // Resuming from a prefix marker must skip the WHOLE group, not land on its first member —
    // otherwise `b/` is re-emitted every page and the listing never advances.
    let page2 = list_body(
        &h,
        "v1delim",
        &[
            ("delimiter", "/"),
            ("max-keys", "2"),
            ("marker", next.as_str()),
        ],
    )
    .await;
    assert_eq!(keys_of(&page2), ["m::n", "m::o"], "got: {page2}");
    assert!(
        !page2.contains("<Prefix>b/</Prefix>"),
        "the grouped prefix must not repeat: {page2}"
    );
}

#[tokio::test]
async fn start_after_naming_a_real_key_that_ends_in_the_delimiter_is_not_a_group_skip() {
    let h = harness().await;
    // `photos/` is a real (folder-marker) object here, and under `prefix=photos/` nothing rolls up,
    // so it is an ordinary key marker. Treating any marker that merely ENDS in the delimiter as a
    // group would swallow `photos/a.jpg`.
    seed_keys(&h, "folder", &["photos/", "photos/a.jpg", "photos/b.jpg"]).await;
    let body = list_body(
        &h,
        "folder",
        &[
            ("list-type", "2"),
            ("prefix", "photos/"),
            ("delimiter", "/"),
            ("start-after", "photos/"),
        ],
    )
    .await;
    assert_eq!(
        keys_of(&body),
        ["photos/a.jpg", "photos/b.jpg"],
        "a key marker that ends in the delimiter is not a group: {body}"
    );
}

#[tokio::test]
async fn list_object_versions_bare_key_marker_is_exclusive() {
    let h = harness().await;
    seed_keys(&h, "vermark", &["v1", "v2", "v3"]).await;

    // A bare `key-marker` (no version-id-marker) begins the results AFTER that key.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("vermark"),
                None,
                &[("versions", ""), ("key-marker", "v1")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert_eq!(
        keys_of(&body),
        ["v2", "v3"],
        "a bare key-marker is exclusive: {body}"
    );
}

#[tokio::test]
async fn list_object_versions_paired_marker_still_resumes_within_a_key() {
    let h = harness().await;
    versioned_bucket(&h, "verpair").await;
    // Three versions of ONE key: paging must resume INSIDE the key, which is exactly what the
    // key-marker + version-id-marker PAIR means — and why the bare form's exclusivity must not
    // leak into it.
    for _ in 0..3 {
        drain(
            send(
                &h.svc,
                req(
                    Method::PUT,
                    Some("verpair"),
                    Some("k"),
                    &[],
                    &[],
                    b"x".to_vec(),
                ),
            )
            .await,
        )
        .await;
    }

    let page1 = list_body(&h, "verpair", &[("versions", ""), ("max-keys", "2")]).await;
    assert_eq!(keys_of(&page1).len(), 2, "page 1 holds two versions");
    let key_marker = between(&page1, "<NextKeyMarker>", "</NextKeyMarker>");
    let vid_marker = between(&page1, "<NextVersionIdMarker>", "</NextVersionIdMarker>");
    assert_eq!(key_marker, "k");
    assert!(!vid_marker.is_empty(), "the paired marker is emitted");

    let page2 = list_body(
        &h,
        "verpair",
        &[
            ("versions", ""),
            ("max-keys", "2"),
            ("key-marker", key_marker.as_str()),
            ("version-id-marker", vid_marker.as_str()),
        ],
    )
    .await;
    assert_eq!(
        keys_of(&page2).len(),
        1,
        "the paired marker resumes WITHIN the key, returning its last version: {page2}"
    );
}

/// An undecodable continuation token must be rejected, never folded into "no token": collapsing
/// invalid to absent silently restarts the listing at page 1, so a paginating consumer whose token
/// got truncated re-processes the bucket forever instead of failing.
#[tokio::test]
async fn undecodable_continuation_token_is_invalid_argument() {
    let h = harness().await;
    seed_keys(&h, "badtok", &["a", "b", "c"]).await;

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("badtok"),
                None,
                &[
                    ("list-type", "2"),
                    ("continuation-token", "!!!not-base64!!!"),
                ],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "got: {st}");
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("<Code>InvalidArgument</Code>"), "{body}");
    assert!(
        !body.contains("<Key>a</Key>"),
        "a bad token must not silently restart the listing: {body}"
    );
}

#[tokio::test]
async fn list_objects_v2_echoes_start_after() {
    let h = harness().await;
    seed_keys(&h, "echosa", &["k1", "k2", "k3"]).await;
    let body = list_body(&h, "echosa", &[("list-type", "2"), ("start-after", "k1")]).await;
    assert!(
        body.contains("<StartAfter>k1</StartAfter>"),
        "S3 echoes StartAfter: {body}"
    );
    // Absent start-after emits no element at all.
    let body = list_body(&h, "echosa", &[("list-type", "2")]).await;
    assert!(!body.contains("<StartAfter>"), "{body}");
}

/// `max-parts=0` must not advertise a page the client can never leave: `LIMIT 0+1` marks the page
/// truncated while the page itself is emptied, so `IsTruncated=true` ships with no
/// NextPartNumberMarker and a client looping on IsTruncated spins or restarts at part 1.
#[tokio::test]
async fn list_parts_max_parts_zero_is_not_an_unterminable_page() {
    let h = harness().await;
    let (upload_id, _) = start_upload_with_part(&h, "mpzero", "obj.bin").await;

    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mpzero"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str()), ("max-parts", "0")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert!(
        !body.contains("<Part>"),
        "max-parts=0 returns no parts: {body}"
    );
    assert!(
        !body.contains("<IsTruncated>true</IsTruncated>")
            || body.contains("<NextPartNumberMarker>"),
        "truncated with no marker is unterminable: {body}"
    );

    // The same strict parse the other page-size parameters get.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mpzero"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str()), ("max-parts", "abc")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert!(
        String::from_utf8(body)
            .unwrap()
            .contains("<Code>InvalidArgument</Code>")
    );
}

/// A version-scoped DELETE must report WHICH version it removed and whether that version was a
/// delete marker. Without both headers a client cannot confirm that the canonical undelete (DELETE
/// the delete marker and the object comes back) removed a marker rather than a data version.
#[tokio::test]
async fn version_scoped_delete_reports_delete_marker_and_version_id() {
    let h = harness().await;
    versioned_bucket(&h, "delhdr").await;
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("delhdr"),
                Some("obj"),
                &[],
                &[],
                b"data".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let data_version = header(&hdrs, "x-amz-version-id").unwrap().to_owned();

    // A plain DELETE creates a delete marker (this branch already set both headers).
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("delhdr"),
                Some("obj"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    assert_eq!(header(&hdrs, "x-amz-delete-marker"), Some("true"));
    let marker_version = header(&hdrs, "x-amz-version-id").unwrap().to_owned();

    // THE UNDELETE: removing the marker by version id must report that a MARKER was removed.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("delhdr"),
                Some("obj"),
                &[("versionId", marker_version.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    assert_eq!(
        header(&hdrs, "x-amz-delete-marker"),
        Some("true"),
        "the version removed WAS a delete marker"
    );
    assert_eq!(
        header(&hdrs, "x-amz-version-id"),
        Some(marker_version.as_str()),
        "the response names the version removed"
    );

    // Removing a real data version reports the same two headers, with the flag FALSE — the
    // distinction the client is asking for.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("delhdr"),
                Some("obj"),
                &[("versionId", data_version.as_str())],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    assert_eq!(
        header(&hdrs, "x-amz-delete-marker"),
        Some("false"),
        "a data version is not a delete marker"
    );
    assert_eq!(
        header(&hdrs, "x-amz-version-id"),
        Some(data_version.as_str())
    );
}

/// Part ordering is a property of the request document alone and S3 validates it first. With the
/// checks interleaved, parts [(2, small), (1, small)] passed the ordering test at entry 0 and then
/// tripped the undersized test there — reporting EntityTooSmall for a request whose actual defect
/// is the ordering the client must fix first.
#[tokio::test]
async fn complete_multipart_reports_invalid_part_order_before_entity_too_small() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("mporder"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mporder"),
                Some("obj.bin"),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let upload_id = between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    );

    // Two SMALL parts: each would independently trip the 5 MiB non-final-part rule.
    let mut etags = Vec::new();
    for pn in ["1", "2"] {
        let (st, hdrs, _) = drain(
            send(
                &h.svc,
                req(
                    Method::PUT,
                    Some("mporder"),
                    Some("obj.bin"),
                    &[("uploadId", upload_id.as_str()), ("partNumber", pn)],
                    &[],
                    b"tiny".to_vec(),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        etags.push(header(&hdrs, "etag").unwrap().to_owned());
    }

    // Ask for them BACKWARDS. Both defects are present; S3 reports the ordering one.
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>\
         <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etags[1], etags[0]
    );
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mporder"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                xml.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let body = String::from_utf8(body).unwrap();
    assert!(
        body.contains("<Code>InvalidPartOrder</Code>"),
        "ordering is reported before size, got: {body}"
    );
}

/// PR #1 stripped the `aws-chunked` TRANSFER coding from the stored `content-encoding` on the PUT
/// path; the copy path kept its own header read and was missed. A stored `aws-chunked` makes every
/// later GET advertise a coding the response does not use.
#[tokio::test]
async fn copy_object_strips_aws_chunked_from_stored_content_encoding() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("cpenc"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cpenc"),
                Some("src"),
                &[],
                &[],
                b"payload".to_vec(),
            ),
        )
        .await,
    )
    .await;

    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("cpenc"),
                Some("dst"),
                &[],
                &[
                    ("x-amz-copy-source", "/cpenc/src"),
                    ("x-amz-metadata-directive", "REPLACE"),
                    ("content-encoding", "aws-chunked,gzip"),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("cpenc"), Some("dst"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "content-encoding"),
        Some("gzip"),
        "the transfer coding is stripped, the real coding kept"
    );
}

/// RFC 7233: "If the selected representation is zero length, the byte-range-spec is unsatisfiable."
/// A suffix range selecting no bytes is equally unsatisfiable. Both answered 206 with a
/// self-contradictory `bytes 0-0/0` Content-Range before the fix.
#[tokio::test]
async fn suffix_range_selecting_no_bytes_is_invalid_range() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("rng"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("rng"),
                Some("empty.bin"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("rng"),
                Some("full.bin"),
                &[],
                &[],
                b"abcdefghij".to_vec(),
            ),
        )
        .await,
    )
    .await;

    // Any suffix against a zero-length representation.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("rng"),
                Some("empty.bin"),
                &[],
                &[("range", "bytes=-5")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::RANGE_NOT_SATISFIABLE, "suffix on empty");

    // A zero-length suffix against a non-empty object.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("rng"),
                Some("full.bin"),
                &[],
                &[("range", "bytes=-0")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::RANGE_NOT_SATISFIABLE, "bytes=-0");

    // The satisfiable suffix still works — the fix must not over-reject.
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("rng"),
                Some("full.bin"),
                &[],
                &[("range", "bytes=-3")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, b"hij");
}

/// ARCH 26.3: mutating S3 data-plane ops are recorded in the audit log (actor + action + resource +
/// salient attributes), while reads are NOT. A PutObject records a `PutObject` entry naming the
/// bucket/key/actor and carrying the size + ETag; GET/HEAD/ListObjects add nothing; a DeleteObject
/// records a `DeleteObject` entry. Fails outright without the handler wiring (the log stays empty).
#[tokio::test]
async fn mutating_s3_ops_are_audited_and_reads_are_not() {
    let h = harness().await;

    // Create the bucket (bucket creation itself is a control-plane concern, not audited here).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("audit-bkt"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        h.meta.list_activity(100).await.unwrap().is_empty(),
        "bucket create must not add an S3 audit entry"
    );

    // PUT an object -> one PutObject audit entry with the full salient attributes.
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("audit-bkt"),
                Some("docs/a.txt"),
                &[],
                &[("content-type", "text/plain")],
                b"hello world".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let log = h.meta.list_activity(100).await.unwrap();
    assert_eq!(
        log.len(),
        1,
        "PutObject must record exactly one audit entry"
    );
    let put = &log[0];
    assert_eq!(put.action, "PutObject");
    assert_eq!(put.bucket.as_deref(), Some("audit-bkt"));
    assert_eq!(put.key.as_deref(), Some("docs/a.txt"));
    assert_eq!(put.size, Some(11));
    // ETag is stored unquoted; "hello world" -> md5 5eb63bbbe01eeed093cb22bb8f5acdc3.
    assert_eq!(
        put.etag.as_deref(),
        Some("5eb63bbbe01eeed093cb22bb8f5acdc3")
    );
    // The actor is the requester's access-key id (the admin principal's is "k").
    assert_eq!(put.actor.as_deref(), Some("k"));

    // Reads must NOT be audited: a GET, a HEAD, and a ListObjects add nothing to the log.
    for parts in [
        req(
            Method::GET,
            Some("audit-bkt"),
            Some("docs/a.txt"),
            &[],
            &[],
            vec![],
        ),
        req(
            Method::HEAD,
            Some("audit-bkt"),
            Some("docs/a.txt"),
            &[],
            &[],
            vec![],
        ),
        req(Method::GET, Some("audit-bkt"), None, &[], &[], vec![]),
    ] {
        let (st, _, _) = drain(send(&h.svc, parts).await).await;
        assert_eq!(st, StatusCode::OK);
    }
    assert_eq!(
        h.meta.list_activity(100).await.unwrap().len(),
        1,
        "reads (GET/HEAD/ListObjects) must not add audit entries"
    );

    // DELETE the object -> a DeleteObject audit entry (unversioned bucket: permanent removal).
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::DELETE,
                Some("audit-bkt"),
                Some("docs/a.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);

    let log = h.meta.list_activity(100).await.unwrap();
    assert_eq!(log.len(), 2, "DeleteObject must add a second audit entry");
    let del = log
        .iter()
        .find(|e| e.action == "DeleteObject")
        .expect("a DeleteObject audit entry is recorded");
    assert_eq!(del.bucket.as_deref(), Some("audit-bkt"));
    assert_eq!(del.key.as_deref(), Some("docs/a.txt"));
    assert_eq!(del.actor.as_deref(), Some("k"));
}

// --- Composite multipart checksums (Phase 1) -------------------------------------------------

fn crc32_b64(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(crc32fast::hash(data).to_be_bytes())
}

/// Independently compose the object-level COMPOSITE CRC32 the same way AWS does: base64-decode each
/// per-part CRC32 digest, concatenate the raw bytes, CRC32 the concatenation, base64-encode, and
/// append the `-N` part-count suffix. Used to cross-check Cairn's composition (checksum-of-checksums,
/// not a whole-object hash).
fn expected_composite_crc32(part_b64: &[&str]) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut concat = Vec::new();
    for p in part_b64 {
        concat.extend_from_slice(&b64.decode(p).unwrap());
    }
    format!(
        "{}-{}",
        b64.encode(crc32fast::hash(&concat).to_be_bytes()),
        part_b64.len()
    )
}

/// Initiate a multipart upload on a freshly-created bucket, returning the upload id.
async fn init_mpu(h: &Harness, bucket: &str, key: &str) -> String {
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some(bucket), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some(bucket),
                Some(key),
                &[("uploads", "")],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    )
}

/// A part upload carrying a WRONG `x-amz-checksum-crc32` is rejected as BadDigest, and the just-staged
/// part is deleted so no orphan blob is leaked (the no-orphan invariant).
#[tokio::test]
async fn upload_part_wrong_checksum_is_bad_digest_and_no_orphan() {
    let h = harness().await;
    let upload_id = init_mpu(&h, "mpc-bad", "k.bin").await;
    let part = b"a part body".to_vec();
    let wrong = crc32_b64(b"different bytes entirely");
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-bad"),
                Some("k.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[("x-amz-checksum-crc32", wrong.as_str())],
                part,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "wrong part checksum must be BadDigest"
    );
    // The staged part blob must have been deleted before returning — the session's staging dir holds
    // no leftover part file (an orphan reconcile would otherwise have to reclaim).
    let session_dir = h
        ._dir
        .path()
        .join(".staging")
        .join("multipart")
        .join(&upload_id);
    let leftover = std::fs::read_dir(&session_dir)
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "a rejected part must leave no staged blob");
}

/// A part carrying more than one `x-amz-checksum-*` algorithm is rejected — exactly one algorithm may
/// be stored per part so object-level composition stays unambiguous.
#[tokio::test]
async fn upload_part_two_checksum_algorithms_is_invalid_request() {
    let h = harness().await;
    let upload_id = init_mpu(&h, "mpc-two", "k.bin").await;
    let part = b"body bytes".to_vec();
    let c32 = crc32_b64(&part);
    let s256 = sha256_b64(&part);
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-two"),
                Some("k.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[
                    ("x-amz-checksum-crc32", c32.as_str()),
                    ("x-amz-checksum-sha256", s256.as_str()),
                ],
                part,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "two algorithms per part must be InvalidRequest"
    );
}

/// A full 2-part CRC32 multipart upload composes an object-level COMPOSITE checksum: the Complete
/// response body carries `<ChecksumCRC32>` (the checksum-of-checksums with a `-2` suffix) and
/// `<ChecksumType>COMPOSITE</ChecksumType>`, and a checksum-mode GET echoes the same value + type.
#[tokio::test]
async fn multipart_crc32_composes_composite_checksum() {
    let h = harness().await;
    let upload_id = init_mpu(&h, "mpc-c32", "obj.bin").await;
    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = b"the-tail".to_vec();
    let c1 = crc32_b64(&part1);
    let c2 = crc32_b64(&part2);

    let (st, hdrs1, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-c32"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[("x-amz-checksum-crc32", c1.as_str())],
                part1,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs1, "x-amz-checksum-crc32"),
        Some(c1.as_str()),
        "part upload echoes its checksum"
    );
    let etag1 = header(&hdrs1, "etag").unwrap().to_owned();

    let (_, hdrs2, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-c32"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "2")],
                &[("x-amz-checksum-crc32", c2.as_str())],
                part2,
            ),
        )
        .await,
    )
    .await;
    let etag2 = header(&hdrs2, "etag").unwrap().to_owned();

    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, chdrs, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpc-c32"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let expected = expected_composite_crc32(&[c1.as_str(), c2.as_str()]);
    assert!(
        expected.ends_with("-2"),
        "composite carries the part-count suffix"
    );
    let body_s = String::from_utf8(body).unwrap();
    assert!(
        body_s.contains(&format!("<ChecksumCRC32>{expected}</ChecksumCRC32>")),
        "complete body carries the composite: {body_s}"
    );
    assert!(
        body_s.contains("<ChecksumType>COMPOSITE</ChecksumType>"),
        "{body_s}"
    );
    assert_eq!(
        header(&chdrs, "x-amz-checksum-crc32"),
        Some(expected.as_str())
    );
    assert_eq!(header(&chdrs, "x-amz-checksum-type"), Some("COMPOSITE"));

    // A checksum-mode GET echoes the composite value and type.
    let (st, ghdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mpc-c32"),
                Some("obj.bin"),
                &[],
                &[("x-amz-checksum-mode", "ENABLED")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&ghdrs, "x-amz-checksum-crc32"),
        Some(expected.as_str())
    );
    assert_eq!(header(&ghdrs, "x-amz-checksum-type"), Some("COMPOSITE"));
}

/// A CRC64NVME multipart upload composes a whole-object FULL_OBJECT checksum (CRC64NVME has no
/// composite form): the object checksum has NO `-N` suffix, its type is FULL_OBJECT, and it equals the
/// CRC64NVME of the same bytes uploaded as a single-part PUT.
#[tokio::test]
async fn multipart_crc64nvme_recomputes_full_object_checksum() {
    let h = harness().await;
    let upload_id = init_mpu(&h, "mpc-c64", "obj.bin").await;
    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = b"the-tail".to_vec();

    // The SDK selector header asks the server to COMPUTE crc64nvme over each part (no value to verify).
    let sel = [("x-amz-sdk-checksum-algorithm", "CRC64NVME")];
    let (st, hdrs1, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-c64"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &sel,
                part1.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let etag1 = header(&hdrs1, "etag").unwrap().to_owned();
    let (_, hdrs2, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-c64"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "2")],
                &sel,
                part2.clone(),
            ),
        )
        .await,
    )
    .await;
    let etag2 = header(&hdrs2, "etag").unwrap().to_owned();

    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpc-c64"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let body_s = String::from_utf8(body).unwrap();
    assert!(
        body_s.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
        "{body_s}"
    );

    let (_, ghdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mpc-c64"),
                Some("obj.bin"),
                &[],
                &[("x-amz-checksum-mode", "ENABLED")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    let obj_crc = header(&ghdrs, "x-amz-checksum-crc64nvme")
        .expect("object crc64nvme present")
        .to_owned();
    assert!(
        !obj_crc.contains('-'),
        "a FULL_OBJECT crc64nvme carries no -N suffix: {obj_crc}"
    );
    assert_eq!(header(&ghdrs, "x-amz-checksum-type"), Some("FULL_OBJECT"));

    // The same concatenated bytes as a single-part PUT must yield the identical whole-object crc64nvme.
    let mut whole = part1.clone();
    whole.extend_from_slice(&part2);
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-c64"),
                Some("single.bin"),
                &[],
                &sel,
                whole,
            ),
        )
        .await,
    )
    .await;
    let (_, shdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("mpc-c64"),
                Some("single.bin"),
                &[],
                &[("x-amz-checksum-mode", "ENABLED")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        header(&shdrs, "x-amz-checksum-crc64nvme"),
        Some(obj_crc.as_str()),
        "multipart FULL_OBJECT crc64nvme equals the single-part whole-object crc64nvme"
    );
}

/// A multipart upload where only SOME parts carry a checksum (simulating a mixed UploadPart +
/// UploadPartCopy session) completes with 200 and NO object-level checksum — never bricking an upload
/// just because a part lacked a checksum (backward-compatible degrade).
#[tokio::test]
async fn multipart_mixed_present_and_absent_checksums_has_no_object_checksum() {
    let h = harness().await;
    let upload_id = init_mpu(&h, "mpc-mix", "obj.bin").await;
    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = b"the-tail".to_vec();
    let c1 = crc32_b64(&part1);

    let (st, hdrs1, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-mix"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[("x-amz-checksum-crc32", c1.as_str())],
                part1,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let etag1 = header(&hdrs1, "etag").unwrap().to_owned();
    // Part 2 carries no checksum at all.
    let (_, hdrs2, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-mix"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "2")],
                &[],
                part2,
            ),
        )
        .await,
    )
    .await;
    let etag2 = header(&hdrs2, "etag").unwrap().to_owned();

    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, chdrs, body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpc-mix"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "a mixed-checksum upload must still complete"
    );
    assert!(header(&chdrs, "x-amz-checksum-crc32").is_none());
    assert!(!String::from_utf8(body).unwrap().contains("<ChecksumCRC32>"));
}

/// A multipart upload whose PRESENT part checksums use genuinely inconsistent algorithms is rejected
/// with InvalidRequest — composing across algorithms would produce undefined bytes.
#[tokio::test]
async fn multipart_inconsistent_present_algorithms_is_invalid_request() {
    let h = harness().await;
    let upload_id = init_mpu(&h, "mpc-inc", "obj.bin").await;
    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = b"the-tail".to_vec();
    let c1 = crc32_b64(&part1);
    let s2 = sha256_b64(&part2);

    let (st, hdrs1, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-inc"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[("x-amz-checksum-crc32", c1.as_str())],
                part1,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let etag1 = header(&hdrs1, "etag").unwrap().to_owned();
    let (_, hdrs2, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mpc-inc"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "2")],
                &[("x-amz-checksum-sha256", s2.as_str())],
                part2,
            ),
        )
        .await,
    )
    .await;
    let etag2 = header(&hdrs2, "etag").unwrap().to_owned();

    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("mpc-inc"),
                Some("obj.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "inconsistent present part algorithms must be InvalidRequest"
    );
}

/// Read the stored `sse_descriptor` (JSON) of a key's current version, or `None` if unencrypted.
async fn stored_descriptor(h: &Harness, bucket: &str, key: &str) -> Option<String> {
    h.meta
        .current_version(
            &BucketName::parse(bucket).unwrap(),
            &ObjectKey::parse(key).unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
        .sse_descriptor
}

/// The `mode` field of a stored descriptor JSON: `Some("at-rest")` for transparent at-rest, `None`
/// when the field is absent (a plain SSE-S3 descriptor serializes without it).
fn descriptor_mode(descriptor_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(descriptor_json)
        .unwrap()
        .get("mode")
        .and_then(|m| m.as_str())
        .map(str::to_owned)
}

/// With `CAIRN_ENCRYPT_AT_REST` on, a plain PUT (no SSE header) round-trips, is stored ENCRYPTED
/// with an `AtRest`-mode descriptor, and does NOT advertise `x-amz-server-side-encryption` on
/// PUT/GET/HEAD (transparent operator encryption). An explicit SSE-S3 PUT in the same bucket IS
/// advertised (`AES256`) and stores an SSE-S3 (mode-absent) descriptor.
#[tokio::test]
async fn at_rest_transparent_encryption_stores_encrypted_but_does_not_advertise() {
    let h = harness_encrypt_at_rest().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("enc"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // A plain PUT with no SSE header.
    let payload = b"transparent at-rest payload".to_vec();
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("enc"),
                Some("plain.txt"),
                &[],
                &[("content-type", "text/plain")],
                payload.clone(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        None,
        "a transparent at-rest object must NOT advertise SSE on the PUT response"
    );

    // Stored encrypted: the descriptor is present and its mode is `at-rest`.
    let desc = stored_descriptor(&h, "enc", "plain.txt")
        .await
        .expect("at-rest object stores an sse_descriptor");
    assert_eq!(descriptor_mode(&desc).as_deref(), Some("at-rest"));

    // GET round-trips the exact bytes and advertises nothing.
    let (st, hdrs, got) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("enc"),
                Some("plain.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(got, payload, "at-rest encryption is transparent on read");
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        None,
        "GET of a transparent at-rest object must NOT advertise SSE"
    );

    // HEAD likewise advertises nothing.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("enc"),
                Some("plain.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(header(&hdrs, "x-amz-server-side-encryption"), None);

    // An EXPLICIT SSE-S3 PUT in the same bucket IS advertised and stores an SSE-S3 descriptor.
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("enc"),
                Some("explicit.txt"),
                &[],
                &[("x-amz-server-side-encryption", "AES256")],
                b"explicit sse".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256"),
        "an explicitly-requested SSE-S3 object IS advertised"
    );
    let desc = stored_descriptor(&h, "enc", "explicit.txt").await.unwrap();
    assert_eq!(
        descriptor_mode(&desc),
        None,
        "an SSE-S3 descriptor serializes without a `mode` field (advertised as AES256)"
    );
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::GET,
                Some("enc"),
                Some("explicit.txt"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256"),
        "GET of an explicit SSE-S3 object advertises AES256"
    );
}

/// With at-rest encryption OFF (the default), a plain PUT stores PLAINTEXT — no descriptor, nothing
/// advertised — unchanged from before the feature.
#[tokio::test]
async fn at_rest_off_stores_plaintext() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("plainb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("plainb"),
                Some("k.txt"),
                &[],
                &[],
                b"unencrypted".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(header(&hdrs, "x-amz-server-side-encryption"), None);
    assert_eq!(
        stored_descriptor(&h, "plainb", "k.txt").await,
        None,
        "with at-rest off a plain PUT stores no sse_descriptor"
    );
}

/// Fails closed: an encrypted object whose DEK cannot be unwrapped (the master key that sealed it is
/// gone) errors on GET rather than returning plaintext or zeros. Modeled by writing under one master
/// key and reading through a service holding a different key, sharing the same metadata + blob.
#[tokio::test]
async fn encrypted_object_unopenable_dek_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> = Arc::new(cairn_meta::open_in_memory().unwrap());
    let blob: Arc<dyn BlobStore> =
        Arc::new(cairn_blob::LocalBlobStore::open(dir.path()).await.unwrap());
    let clock = Arc::new(cairn_types::testing::TestClock::default());
    let mk = |key: [u8; 32], at_rest: bool| {
        S3Service::new(
            meta.clone(),
            blob.clone(),
            Arc::new(cairn_types::testing::AllowAll),
            clock.clone(),
            Arc::new(cairn_crypto::SystemCrypto::new(key)) as Arc<dyn cairn_types::traits::Crypto>,
            "us-east-1".to_owned(),
            5 * 1024 * 1024 * 1024,
        )
        .with_encrypt_at_rest(at_rest)
    };
    let writer = mk([1u8; 32], true);
    let reader = mk([2u8; 32], false); // a different master key can never unwrap the DEK

    // A distinctive plaintext token that does NOT appear in the key/bucket names, so an error
    // response echoing the resource path can't be mistaken for a plaintext leak.
    const TOKEN: &[u8] = b"ZZUNIQUEPLAINTEXTTOKENZZ";
    drain(
        send(
            &writer,
            req(Method::PUT, Some("failc"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &writer,
            req(
                Method::PUT,
                Some("failc"),
                Some("obj.bin"),
                &[],
                &[],
                TOKEN.to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, body) = drain(
        send(
            &reader,
            req(
                Method::GET,
                Some("failc"),
                Some("obj.bin"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_ne!(st, StatusCode::OK, "an unopenable DEK must fail the GET");
    assert!(
        !body.windows(TOKEN.len()).any(|w| w == TOKEN),
        "a fail-closed GET must never leak plaintext, got {body:?}"
    );
    assert!(
        body.iter().any(|&b| b != 0),
        "a fail-closed GET must never return zeros"
    );
}

/// With at-rest encryption on, both a CopyObject destination and a completed multipart object are
/// stored encrypted (`AtRest` descriptor) and round-trip on GET.
#[tokio::test]
async fn at_rest_copy_and_multipart_are_encrypted() {
    let h = harness_encrypt_at_rest().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("encb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;

    // Seed a source object, then server-side copy it within the same at-rest bucket.
    let src_body = b"copy me while encrypting".to_vec();
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("encb"),
                Some("src.bin"),
                &[],
                &[],
                src_body.clone(),
            ),
        )
        .await,
    )
    .await;
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("encb"),
                Some("dst.bin"),
                &[],
                &[("x-amz-copy-source", "/encb/src.bin")],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let desc = stored_descriptor(&h, "encb", "dst.bin")
        .await
        .expect("copy destination stores an sse_descriptor under at-rest");
    assert_eq!(descriptor_mode(&desc).as_deref(), Some("at-rest"));
    let (st, _, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("encb"), Some("dst.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(got, src_body, "copied at-rest object round-trips");

    // Multipart: initiate, upload two parts, complete — the assembled object is encrypted.
    let body_s = String::from_utf8(
        drain(
            send(
                &h.svc,
                req(
                    Method::POST,
                    Some("encb"),
                    Some("mp.bin"),
                    &[("uploads", "")],
                    &[],
                    vec![],
                ),
            )
            .await,
        )
        .await
        .2,
    )
    .unwrap();
    let upload_id = between(&body_s, "<UploadId>", "</UploadId>");
    let part1 = vec![b'z'; 5 * 1024 * 1024];
    let part2 = b"-encrypted-tail".to_vec();
    let (_, h1, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("encb"),
                Some("mp.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "1")],
                &[],
                part1.clone(),
            ),
        )
        .await,
    )
    .await;
    let etag1 = header(&h1, "etag").unwrap().to_owned();
    let (_, h2, _) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("encb"),
                Some("mp.bin"),
                &[("uploadId", upload_id.as_str()), ("partNumber", "2")],
                &[],
                part2.clone(),
            ),
        )
        .await,
    )
    .await;
    let etag2 = header(&h2, "etag").unwrap().to_owned();
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("encb"),
                Some("mp.bin"),
                &[("uploadId", upload_id.as_str())],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        None,
        "a transparent at-rest multipart object does not advertise SSE"
    );
    let desc = stored_descriptor(&h, "encb", "mp.bin")
        .await
        .expect("assembled multipart object stores an sse_descriptor under at-rest");
    assert_eq!(descriptor_mode(&desc).as_deref(), Some("at-rest"));
    let (st, _, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("encb"), Some("mp.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mut expected = part1;
    expected.extend_from_slice(&part2);
    assert_eq!(got, expected, "assembled at-rest object round-trips");
}

// ---------------------------------------------------------------------------------------------
// Part-level SSE at rest (ARCH 27, Increment 3a). An SSE / bucket-default / at-rest multipart
// upload stages every part as ciphertext on disk; the assembled object round-trips byte-exact and
// the ETag stays the plaintext md5(concat(part_md5s))-N. Every ciphertext assertion targets the
// real LocalBlobStore staging files.
// ---------------------------------------------------------------------------------------------

fn once_body(data: Vec<u8>) -> cairn_types::BodyStream {
    Box::pin(futures_util::stream::once(
        async move { Ok(Bytes::from(data)) },
    ))
}

/// The staged part files of an in-flight upload, in part-number order.
fn staged_part_paths(root: &std::path::Path, upload_id: &str) -> Vec<std::path::PathBuf> {
    let dir = root.join(".staging").join("multipart").join(upload_id);
    let mut v: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .collect();
    v.sort();
    v
}

/// A CRNB blob's trailer is 34 bytes: magic(4) + version(1) at offset 4. VERSION_ENCRYPTED == 2.
fn is_version_encrypted(path: &std::path::Path) -> bool {
    let raw = std::fs::read(path).unwrap();
    raw.len() >= 34 && raw[raw.len() - 34 + 4] == 2
}

fn build_svc(meta: Arc<dyn MetadataStore>, blob: Arc<dyn BlobStore>, key: [u8; 32]) -> S3Service {
    let clock = Arc::new(cairn_types::testing::TestClock::default());
    let crypto: Arc<dyn cairn_types::traits::Crypto> =
        Arc::new(cairn_crypto::SystemCrypto::new(key));
    S3Service::new(
        meta,
        blob,
        Arc::new(cairn_types::testing::AllowAll),
        clock,
        crypto,
        "us-east-1".to_owned(),
        5 * 1024 * 1024 * 1024,
    )
}

/// Initiate a multipart upload with the given initiate headers and return the upload id.
async fn initiate(svc: &S3Service, bucket: &str, key: &str, headers: &[(&str, &str)]) -> String {
    let (st, _, body) = drain(
        send(
            svc,
            req(
                Method::POST,
                Some(bucket),
                Some(key),
                &[("uploads", "")],
                headers,
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "initiate multipart");
    between(
        &String::from_utf8(body).unwrap(),
        "<UploadId>",
        "</UploadId>",
    )
}

/// Upload one body part; returns its ETag header.
async fn upload_part(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    upload_id: &str,
    n: u16,
    body: Vec<u8>,
) -> String {
    let (st, hdrs, _) = drain(
        send(
            svc,
            req(
                Method::PUT,
                Some(bucket),
                Some(key),
                &[("uploadId", upload_id), ("partNumber", &n.to_string())],
                &[],
                body,
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "upload part {n}");
    header(&hdrs, "etag").unwrap().to_owned()
}

async fn complete(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    upload_id: &str,
    etags: &[(u16, &str)],
) -> (StatusCode, Vec<(String, String)>, Vec<u8>) {
    let parts: String = etags
        .iter()
        .map(|(n, e)| format!("<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>"))
        .collect();
    let complete = format!("<CompleteMultipartUpload>{parts}</CompleteMultipartUpload>");
    drain(
        send(
            svc,
            req(
                Method::POST,
                Some(bucket),
                Some(key),
                &[("uploadId", upload_id)],
                &[],
                complete.into_bytes(),
            ),
        )
        .await,
    )
    .await
}

/// An explicit-AES256 multipart upload stages BOTH parts as ciphertext on disk, then completes to an
/// object that GETs byte-exact, advertises AES256, and keeps the `-N` plaintext ETag. Fails before
/// 3a: parts were staged plaintext.
#[tokio::test]
async fn multipart_sse_parts_are_ciphertext_and_object_roundtrips() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("msse"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let uid = initiate(
        &h.svc,
        "msse",
        "big.bin",
        &[("x-amz-server-side-encryption", "AES256")],
    )
    .await;

    let part1 = vec![b'A'; 6 * 1024 * 1024];
    let part2 = b"the-tail".to_vec();
    let e1 = upload_part(&h.svc, "msse", "big.bin", &uid, 1, part1.clone()).await;
    let e2 = upload_part(&h.svc, "msse", "big.bin", &uid, 2, part2.clone()).await;

    // Both staged part files are ciphertext (VERSION_ENCRYPTED) BEFORE complete deletes the session.
    let staged = staged_part_paths(h._dir.path(), &uid);
    assert_eq!(staged.len(), 2, "two staged parts");
    for p in &staged {
        assert!(
            is_version_encrypted(p),
            "staged SSE part must be ciphertext: {p:?}"
        );
    }

    let (st, hdrs, body) = complete(&h.svc, "msse", "big.bin", &uid, &[(1, &e1), (2, &e2)]).await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        String::from_utf8(body).unwrap().contains("-2"),
        "ETag has -N suffix"
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256")
    );

    let (st, hdrs, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("msse"), Some("big.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(got, expected);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256")
    );
}

/// The HIGH-finding regression: an SSE session mixing one UploadPart and one UploadPartCopy stages
/// BOTH as ciphertext and the completed object round-trips. Fails if §5.3 (upload_part_copy) is
/// omitted — the copied part would be plaintext (or read raw), corrupting the object.
#[tokio::test]
async fn mixed_uploadpart_and_uploadpartcopy_sse_all_ciphertext() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("mix"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    // A plaintext source object; its first 5 MiB is copied into part 1.
    let mut source = vec![b'a'; 5 * 1024 * 1024];
    source.extend_from_slice(&vec![b'b'; 4096]);
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mix"),
                Some("src.bin"),
                &[],
                &[],
                source.clone(),
            ),
        )
        .await,
    )
    .await;

    let uid = initiate(
        &h.svc,
        "mix",
        "dest.bin",
        &[("x-amz-server-side-encryption", "AES256")],
    )
    .await;

    // Part 1: UploadPartCopy of the first 5 MiB.
    let copy_range = format!("bytes=0-{}", 5 * 1024 * 1024 - 1);
    let (st, _, body) = drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("mix"),
                Some("dest.bin"),
                &[("uploadId", uid.as_str()), ("partNumber", "1")],
                &[
                    ("x-amz-copy-source", "/mix/src.bin"),
                    ("x-amz-copy-source-range", copy_range.as_str()),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let e1 = between(&String::from_utf8(body).unwrap(), "<ETag>", "</ETag>");

    // Part 2: a regular body part.
    let part2 = b"copied-tail".to_vec();
    let e2 = upload_part(&h.svc, "mix", "dest.bin", &uid, 2, part2.clone()).await;

    // Both staged files — the copied one AND the body one — are ciphertext.
    let staged = staged_part_paths(h._dir.path(), &uid);
    assert_eq!(staged.len(), 2);
    for p in &staged {
        assert!(
            is_version_encrypted(p),
            "mixed staged part must be ciphertext: {p:?}"
        );
    }

    let (st, _, _) = complete(&h.svc, "mix", "dest.bin", &uid, &[(1, &e1), (2, &e2)]).await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("mix"), Some("dest.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mut expected = vec![b'a'; 5 * 1024 * 1024];
    expected.extend_from_slice(&part2);
    assert_eq!(got, expected);
}

/// With CAIRN_ENCRYPT_AT_REST and NO SSE header, multipart parts are still ciphertext on disk and the
/// object round-trips, but GET advertises nothing (transparent AtRest).
#[tokio::test]
async fn multipart_at_rest_parts_are_ciphertext() {
    let h = harness_encrypt_at_rest().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("atr"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let uid = initiate(&h.svc, "atr", "big.bin", &[]).await;
    let part1 = vec![b'C'; 6 * 1024 * 1024];
    let part2 = b"tail".to_vec();
    let e1 = upload_part(&h.svc, "atr", "big.bin", &uid, 1, part1.clone()).await;
    let e2 = upload_part(&h.svc, "atr", "big.bin", &uid, 2, part2.clone()).await;

    for p in &staged_part_paths(h._dir.path(), &uid) {
        assert!(
            is_version_encrypted(p),
            "at-rest staged part must be ciphertext"
        );
    }

    let (st, hdrs, _) = complete(&h.svc, "atr", "big.bin", &uid, &[(1, &e1), (2, &e2)]).await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        header(&hdrs, "x-amz-server-side-encryption").is_none(),
        "AtRest advertises nothing"
    );

    let (st, hdrs, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("atr"), Some("big.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(got, expected);
    assert!(header(&hdrs, "x-amz-server-side-encryption").is_none());
}

/// A bucket-default AES256 with no request header pins encrypt_parts at initiate: the staged parts are
/// ciphertext and complete advertises AES256.
#[tokio::test]
async fn bucket_default_sse_multipart_parts_ciphertext() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("bdef"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let put_xml = cairn_xml::server_side_encryption_configuration("AES256", None, false);
    drain(
        send(
            &h.svc,
            req(
                Method::PUT,
                Some("bdef"),
                None,
                &[("encryption", "")],
                &[],
                put_xml.into_bytes(),
            ),
        )
        .await,
    )
    .await;

    let uid = initiate(&h.svc, "bdef", "big.bin", &[]).await;
    let part1 = vec![b'D'; 6 * 1024 * 1024];
    let part2 = b"tail".to_vec();
    let e1 = upload_part(&h.svc, "bdef", "big.bin", &uid, 1, part1.clone()).await;
    let e2 = upload_part(&h.svc, "bdef", "big.bin", &uid, 2, part2.clone()).await;

    for p in &staged_part_paths(h._dir.path(), &uid) {
        assert!(
            is_version_encrypted(p),
            "bucket-default SSE staged part must be ciphertext"
        );
    }

    let (st, hdrs, _) = complete(&h.svc, "bdef", "big.bin", &uid, &[(1, &e1), (2, &e2)]).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256")
    );
    let (st, _, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("bdef"), Some("big.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(got, expected);
}

/// The protocol mints a DISTINCT per-part DEK on every staging: re-uploading the same part number
/// stores a different sealed `part_dek`. Guards against a future refactor to a session-shared key
/// (the blob unit test only proves the blob honors distinct keys).
#[tokio::test]
async fn reupload_part_mints_distinct_dek() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("reup"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let uid = initiate(
        &h.svc,
        "reup",
        "big.bin",
        &[("x-amz-server-side-encryption", "AES256")],
    )
    .await;

    upload_part(
        &h.svc,
        "reup",
        "big.bin",
        &uid,
        1,
        vec![b'x'; 6 * 1024 * 1024],
    )
    .await;
    let upload = cairn_types::id::UploadId::from_string(uid.clone());
    let first = h.meta.list_parts(&upload, 0, 100).await.unwrap().items[0]
        .part_dek
        .clone()
        .expect("part 1 has a sealed DEK");

    upload_part(
        &h.svc,
        "reup",
        "big.bin",
        &uid,
        1,
        vec![b'y'; 6 * 1024 * 1024],
    )
    .await;
    let second = h.meta.list_parts(&upload, 0, 100).await.unwrap().items[0]
        .part_dek
        .clone()
        .expect("re-uploaded part 1 has a sealed DEK");

    assert_ne!(first, second, "re-upload must mint a fresh per-part DEK");
}

/// Tampering with a staged part's on-disk ciphertext fails the completion closed (GCM auth error at
/// assemble) and writes no object version.
#[tokio::test]
async fn tampered_part_fails_complete() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("tmp"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let uid = initiate(
        &h.svc,
        "tmp",
        "big.bin",
        &[("x-amz-server-side-encryption", "AES256")],
    )
    .await;
    let e1 = upload_part(
        &h.svc,
        "tmp",
        "big.bin",
        &uid,
        1,
        vec![b'A'; 6 * 1024 * 1024],
    )
    .await;
    let e2 = upload_part(&h.svc, "tmp", "big.bin", &uid, 2, b"tail".to_vec()).await;

    // Flip a byte in part 1's block-0 ciphertext.
    let staged = staged_part_paths(h._dir.path(), &uid);
    let mut raw = std::fs::read(&staged[0]).unwrap();
    raw[0] ^= 0xff;
    std::fs::write(&staged[0], &raw).unwrap();

    let (st, _, _) = complete(&h.svc, "tmp", "big.bin", &uid, &[(1, &e1), (2, &e2)]).await;
    assert_ne!(st, StatusCode::OK, "a tampered part must fail completion");
    let (st, _, _) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("tmp"), Some("big.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "no object version written");
}

/// A wrong master ring at completion fails to open the part DEKs — closed, BEFORE claiming — so no
/// object is written and the upload stays retryable under the correct key.
#[tokio::test]
async fn wrong_master_key_fails_complete() {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> = Arc::new(cairn_meta::open_in_memory().unwrap());
    let blob: Arc<dyn BlobStore> =
        Arc::new(cairn_blob::LocalBlobStore::open(dir.path()).await.unwrap());
    let svc1 = build_svc(meta.clone(), blob.clone(), [7u8; 32]);
    let svc2 = build_svc(meta.clone(), blob.clone(), [8u8; 32]);

    drain(send(&svc1, req(Method::PUT, Some("wmk"), None, &[], &[], vec![])).await).await;
    let uid = initiate(
        &svc1,
        "wmk",
        "big.bin",
        &[("x-amz-server-side-encryption", "AES256")],
    )
    .await;
    let e1 = upload_part(
        &svc1,
        "wmk",
        "big.bin",
        &uid,
        1,
        vec![b'A'; 6 * 1024 * 1024],
    )
    .await;
    let e2 = upload_part(&svc1, "wmk", "big.bin", &uid, 2, b"tail".to_vec()).await;

    // Completing under the WRONG ring cannot open the sealed part DEKs -> fails closed, no object.
    let (st, _, _) = complete(&svc2, "wmk", "big.bin", &uid, &[(1, &e1), (2, &e2)]).await;
    assert_ne!(st, StatusCode::OK, "wrong master key must fail completion");
    let (st, _, _) = drain(
        send(
            &svc1,
            req(Method::GET, Some("wmk"), Some("big.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(
        st,
        StatusCode::NOT_FOUND,
        "no object after failed completion"
    );

    // The session stayed retryable: completing under the CORRECT ring succeeds.
    let (st, _, _) = complete(&svc1, "wmk", "big.bin", &uid, &[(1, &e1), (2, &e2)]).await;
    assert_eq!(
        st,
        StatusCode::OK,
        "the upload stayed retryable under the correct key"
    );
}

/// Back-compat: a pre-v21 in-flight session (encrypt_parts=0, sse_requested=1, plaintext parts with
/// NULL part_dek) still completes via the legacy plaintext-parts -> encrypt-at-assemble path, and the
/// object GETs byte-exact + advertises AES256.
#[tokio::test]
async fn pre_v21_session_completes_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> = Arc::new(cairn_meta::open_in_memory().unwrap());
    let blob: Arc<dyn BlobStore> =
        Arc::new(cairn_blob::LocalBlobStore::open(dir.path()).await.unwrap());
    let svc = build_svc(meta.clone(), blob.clone(), [7u8; 32]);
    drain(send(&svc, req(Method::PUT, Some("leg"), None, &[], &[], vec![])).await).await;

    // Fabricate a legacy session: sse_requested but NO part encryption, and plaintext parts.
    let upload = cairn_types::id::UploadId::generate();
    let bucket = BucketName::parse("leg").unwrap();
    let key = ObjectKey::parse("big.bin").unwrap();
    let session = cairn_types::meta::MultipartSession {
        upload_id: upload.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        content_type: "application/octet-stream".to_owned(),
        status: cairn_types::meta::MultipartStatus::Active,
        owner_id: UserId("admin".to_owned()),
        intended_acl: None,
        user_metadata: Vec::new(),
        sse_requested: true,
        encrypt_parts: false,
        sse_kms_requested: false,
        sse_kms_key_id: None,
        sse_bucket_key_enabled: false,
        created_at: cairn_types::time::Timestamp(0),
        updated_at: cairn_types::time::Timestamp(0),
    };
    meta.submit(cairn_types::meta::Mutation::CreateMultipart(Box::new(
        session,
    )))
    .await
    .unwrap();

    let part1 = vec![b'L'; 6 * 1024 * 1024];
    let part2 = b"legacy-tail".to_vec();
    let mut etags = Vec::new();
    for (n, body) in [(1u16, part1.clone()), (2u16, part2.clone())] {
        let staged = blob
            .stage_part(
                &upload,
                n,
                once_body(body),
                cairn_types::object::ChecksumSet::none(),
                1 << 30,
                None,
            )
            .await
            .unwrap();
        let part = cairn_types::meta::PartRecord {
            part_number: n,
            size: staged.size,
            etag: staged.md5_hex.clone(),
            storage_path: staged.storage_path.clone(),
            checksum: None,
            part_dek: None,
        };
        etags.push((n, staged.md5_hex.clone()));
        meta.submit(cairn_types::meta::Mutation::RecordPart {
            upload_id: upload.clone(),
            part,
        })
        .await
        .unwrap();
    }

    let refs: Vec<(u16, &str)> = etags.iter().map(|(n, e)| (*n, e.as_str())).collect();
    let (st, hdrs, _) = complete(&svc, "leg", "big.bin", upload.as_str(), &refs).await;
    assert_eq!(st, StatusCode::OK, "legacy session completes");
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("AES256")
    );

    let (st, _, got) = drain(
        send(
            &svc,
            req(Method::GET, Some("leg"), Some("big.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(got, expected);
}

/// SSE-KMS multipart (ARCH 27, Increment 3b): an explicit `x-amz-server-side-encryption: aws:kms` +
/// key id at initiate stages every part as ciphertext, and the assembled object advertises aws:kms +
/// The `CreateMultipartUpload` response itself echoes the requested SSE (ARCH 27): AWS surfaces the
/// algorithm + key id + BucketKeyEnabled on the initiate response so the SDK sees it before any part
/// upload. Fails before the initiate-echo fix: `create_multipart` returned only `x-amz-request-id`.
#[tokio::test]
async fn multipart_kms_initiate_response_advertises_aws_kms_and_key_id() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("kmpi"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let (st, hdrs, _body) = drain(
        send(
            &h.svc,
            req(
                Method::POST,
                Some("kmpi"),
                Some("obj"),
                &[("uploads", "")],
                &[
                    ("x-amz-server-side-encryption", "aws:kms"),
                    ("x-amz-server-side-encryption-aws-kms-key-id", "alias/app"),
                    ("x-amz-server-side-encryption-bucket-key-enabled", "true"),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("aws:kms"),
        "initiate advertises aws:kms"
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption-aws-kms-key-id"),
        Some("alias/app"),
        "initiate echoes the key id"
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption-bucket-key-enabled"),
        Some("true"),
        "initiate echoes BucketKeyEnabled"
    );
}

/// the key id at complete, keeps the `-N` plaintext ETag, and GETs byte-exact. Fails before 3b:
/// `create_multipart` returned `NotImplemented` for `aws:kms`.
#[tokio::test]
async fn multipart_kms_put_complete_advertises_aws_kms_and_key_id() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("kmpm"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let uid = initiate(
        &h.svc,
        "kmpm",
        "big.bin",
        &[
            ("x-amz-server-side-encryption", "aws:kms"),
            ("x-amz-server-side-encryption-aws-kms-key-id", "alias/app"),
        ],
    )
    .await;
    let part1 = vec![b'K'; 6 * 1024 * 1024];
    let part2 = b"kms-tail".to_vec();
    let e1 = upload_part(&h.svc, "kmpm", "big.bin", &uid, 1, part1.clone()).await;
    let e2 = upload_part(&h.svc, "kmpm", "big.bin", &uid, 2, part2.clone()).await;

    for p in &staged_part_paths(h._dir.path(), &uid) {
        assert!(
            is_version_encrypted(p),
            "an aws:kms multipart part must be staged as ciphertext"
        );
    }

    let (st, hdrs, body) = complete(&h.svc, "kmpm", "big.bin", &uid, &[(1, &e1), (2, &e2)]).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("aws:kms"),
        "complete advertises aws:kms"
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption-aws-kms-key-id"),
        Some("alias/app"),
        "complete echoes the KMS key id"
    );
    assert!(
        String::from_utf8_lossy(&body).contains("-2"),
        "multipart ETag keeps the part-count suffix"
    );

    // HEAD echoes the same SSE surface, and GET returns the exact concatenated plaintext.
    let (st, hhdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("kmpm"),
                Some("big.bin"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hhdrs, "x-amz-server-side-encryption"),
        Some("aws:kms")
    );
    assert_eq!(
        header(&hhdrs, "x-amz-server-side-encryption-aws-kms-key-id"),
        Some("alias/app")
    );
    let (st, _, got) = drain(
        send(
            &h.svc,
            req(Method::GET, Some("kmpm"), Some("big.bin"), &[], &[], vec![]),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(got, expected);
}

/// SSE-KMS multipart with bucket-key-enabled at initiate round-trips: complete advertises
/// `x-amz-server-side-encryption-bucket-key-enabled: true`. Fails before 3b (aws:kms rejected at
/// initiate).
#[tokio::test]
async fn multipart_kms_bucket_key_enabled_round_trips() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("kmpb"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let uid = initiate(
        &h.svc,
        "kmpb",
        "big.bin",
        &[
            ("x-amz-server-side-encryption", "aws:kms"),
            ("x-amz-server-side-encryption-aws-kms-key-id", "alias/app"),
            ("x-amz-server-side-encryption-bucket-key-enabled", "true"),
        ],
    )
    .await;
    let e1 = upload_part(
        &h.svc,
        "kmpb",
        "big.bin",
        &uid,
        1,
        vec![b'B'; 6 * 1024 * 1024],
    )
    .await;
    let e2 = upload_part(&h.svc, "kmpb", "big.bin", &uid, 2, b"tail".to_vec()).await;

    let (st, hdrs, _) = complete(&h.svc, "kmpb", "big.bin", &uid, &[(1, &e1), (2, &e2)]).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption"),
        Some("aws:kms")
    );
    assert_eq!(
        header(&hdrs, "x-amz-server-side-encryption-bucket-key-enabled"),
        Some("true"),
        "bucket-key-enabled survives initiate -> complete"
    );
}

/// SSE-KMS multipart is never downgraded: the assembled object's advertised mode is aws:kms, never
/// AES256 and never absent. Guards the fail-closed intent that an aws:kms initiate cannot silently
/// become SSE-S3/plaintext at complete.
#[tokio::test]
async fn multipart_kms_survives_and_is_not_downgraded() {
    let h = harness().await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("kmpd"), None, &[], &[], vec![]),
        )
        .await,
    )
    .await;
    let uid = initiate(
        &h.svc,
        "kmpd",
        "big.bin",
        &[
            ("x-amz-server-side-encryption", "aws:kms"),
            ("x-amz-server-side-encryption-aws-kms-key-id", "alias/app"),
        ],
    )
    .await;
    let e1 = upload_part(
        &h.svc,
        "kmpd",
        "big.bin",
        &uid,
        1,
        vec![b'D'; 6 * 1024 * 1024],
    )
    .await;
    let e2 = upload_part(&h.svc, "kmpd", "big.bin", &uid, 2, b"tail".to_vec()).await;
    let (st, _, _) = complete(&h.svc, "kmpd", "big.bin", &uid, &[(1, &e1), (2, &e2)]).await;
    assert_eq!(st, StatusCode::OK);

    let (st, hdrs, _) = drain(
        send(
            &h.svc,
            req(
                Method::HEAD,
                Some("kmpd"),
                Some("big.bin"),
                &[],
                &[],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let mode = header(&hdrs, "x-amz-server-side-encryption");
    assert_eq!(mode, Some("aws:kms"), "must stay aws:kms");
    assert_ne!(mode, Some("AES256"), "must not downgrade to SSE-S3");
    assert!(mode.is_some(), "must not downgrade to plaintext");
}

/// Fail-closed at INITIATE: with a `CAIRN_KMS_KEY_IDS` allow-list configured, an aws:kms initiate
/// naming a key id NOT on the list is rejected up front (`InvalidArgument`) and creates no session —
/// never a silent downgrade at complete. Fails before 3b for the opposite reason (aws:kms rejected as
/// `NotImplemented` regardless of the key id).
#[tokio::test]
async fn multipart_kms_unknown_key_id_rejected_at_initiate() {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> = Arc::new(cairn_meta::open_in_memory().unwrap());
    let blob: Arc<dyn BlobStore> =
        Arc::new(cairn_blob::LocalBlobStore::open(dir.path()).await.unwrap());
    let clock = Arc::new(cairn_types::testing::TestClock::default());
    let crypto: Arc<dyn cairn_types::traits::Crypto> =
        Arc::new(cairn_crypto::SystemCrypto::new([7u8; 32]));
    let svc = S3Service::new(
        meta.clone(),
        blob,
        Arc::new(cairn_types::testing::AllowAll),
        clock,
        crypto.clone(),
        "us-east-1".to_owned(),
        5 * 1024 * 1024 * 1024,
    )
    .with_key_provider(Arc::new(cairn_protocol::LocalRingProvider::new(
        crypto,
        Some(vec!["alias/allowed".to_owned()]),
    )));

    drain(send(&svc, req(Method::PUT, Some("kmpa"), None, &[], &[], vec![])).await).await;

    // An id NOT on the allow-list is rejected at initiate.
    let (st, _, body) = drain(
        send(
            &svc,
            req(
                Method::POST,
                Some("kmpa"),
                Some("denied.bin"),
                &[("uploads", "")],
                &[
                    ("x-amz-server-side-encryption", "aws:kms"),
                    ("x-amz-server-side-encryption-aws-kms-key-id", "alias/nope"),
                ],
                vec![],
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "initiate fails closed");
    assert!(
        String::from_utf8_lossy(&body).contains("InvalidArgument"),
        "expected InvalidArgument, got {}",
        String::from_utf8_lossy(&body)
    );

    // No session was created for the rejected upload.
    let uploads = meta
        .list_multipart_uploads(&BucketName::parse("kmpa").unwrap(), &Default::default())
        .await
        .unwrap();
    assert!(
        uploads.items.is_empty(),
        "a rejected aws:kms initiate must not create a session"
    );

    // A key id ON the allow-list is accepted at initiate.
    let uid = initiate(
        &svc,
        "kmpa",
        "ok.bin",
        &[
            ("x-amz-server-side-encryption", "aws:kms"),
            (
                "x-amz-server-side-encryption-aws-kms-key-id",
                "alias/allowed",
            ),
        ],
    )
    .await;
    assert!(!uid.is_empty(), "an allow-listed key id initiates");
}
