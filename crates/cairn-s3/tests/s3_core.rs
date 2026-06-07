//! End-to-end tests of the S3 service against the REAL backends (SQLite metadata + filesystem
//! blob), exercising the core bucket/object lifecycle including a SigV4-streaming (chunked) PUT.

use bytes::Bytes;
use cairn_s3::{S3Body, S3Request, S3Response, S3Service};
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
    }
}

struct Harness {
    svc: S3Service,
    _dir: tempfile::TempDir,
}

async fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> = Arc::new(cairn_meta::open_in_memory().unwrap());
    let blob: Arc<dyn BlobStore> =
        Arc::new(cairn_blob::LocalBlobStore::open(dir.path()).await.unwrap());
    let clock = Arc::new(cairn_types::testing::TestClock::default());
    let authz = Arc::new(cairn_types::testing::AllowAll);
    let svc = S3Service::new(
        meta,
        blob,
        authz,
        clock,
        "us-east-1".to_owned(),
        5 * 1024 * 1024 * 1024,
    );
    Harness { svc, _dir: dir }
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
        S3Body::Stream { mut stream, .. } => {
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
    assert_eq!(
        st,
        StatusCode::NOT_IMPLEMENTED,
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
