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

/// A part-validation failure on CompleteMultipartUpload must leave the upload retryable rather than
/// bricking it in `completing` (audit #14): a first complete carrying a wrong ETag fails, and a
/// second complete with the correct ETags then succeeds.
#[tokio::test]
async fn complete_multipart_part_validation_failure_is_retryable() {
    let h = harness().await;
    drain(send(&h.svc, req(Method::PUT, Some("mpr"), None, &[], &[], vec![])).await).await;

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
    let upload_id = between(&String::from_utf8(body).unwrap(), "<UploadId>", "</UploadId>");

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
    drain(send(&h.svc, req(Method::PUT, Some("snb"), None, &[], &[], vec![])).await).await;
    drain(
        send(
            &h.svc,
            req(Method::PUT, Some("snb"), Some("k"), &[], &[], b"hi".to_vec()),
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
            req(Method::PUT, Some("vtag"), Some("k"), &[], &[], b"one".to_vec()),
        )
        .await,
    )
    .await;
    let v1 = header(&hdrs, "x-amz-version-id").unwrap().to_owned();
    let (_, hdrs, _) = drain(
        send(
            &h.svc,
            req(Method::PUT, Some("vtag"), Some("k"), &[], &[], b"two".to_vec()),
        )
        .await,
    )
    .await;
    let v2 = header(&hdrs, "x-amz-version-id").unwrap().to_owned();
    assert_ne!(v1, v2);

    // Tag the OLDER version (v1) explicitly via ?versionId.
    let tags =
        b"<Tagging><TagSet><Tag><Key>which</Key><Value>v1</Value></Tag></TagSet></Tagging>".to_vec();
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
/// ETag equals the plaintext MD5 — proving encryption is transparent to the entity tag (ARCH §27).
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

/// An unsupported server-side-encryption mode (e.g. `aws:kms`) is rejected rather than silently
/// stored unencrypted.
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
                &[("x-amz-server-side-encryption", "aws:kms")],
                b"data".to_vec(),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
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
    // PUT with the five system headers (ARCH §13.4).
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

    // GET response-* overrides REPLACE the corresponding headers (ARCH §21.2), with no duplicate.
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
