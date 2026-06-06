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
    let svc = S3Service::new(
        meta,
        blob,
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
) -> S3Request {
    S3Request {
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
        principal: Some(admin()),
        source: IpAddr::V4(Ipv4Addr::LOCALHOST),
        secure: false,
        request_id: "req-1".to_owned(),
        body: Box::pin(futures_util::stream::once(
            async move { Ok(Bytes::from(body)) },
        )),
    }
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
        h.svc
            .handle(req(Method::PUT, Some("my-bucket"), None, &[], &[], vec![]))
            .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Duplicate create -> 409.
    let (st, _, _) = drain(
        h.svc
            .handle(req(Method::PUT, Some("my-bucket"), None, &[], &[], vec![]))
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
    let (st, hdrs, _) = drain(h.svc.handle(put).await).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        header(&hdrs, "etag"),
        Some("\"5eb63bbbe01eeed093cb22bb8f5acdc3\"")
    );

    // GET it back.
    let (st, hdrs, body) = drain(
        h.svc
            .handle(req(
                Method::GET,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[],
                vec![],
            ))
            .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, b"hello world");
    assert_eq!(header(&hdrs, "content-type"), Some("text/plain"));
    assert_eq!(header(&hdrs, "content-length"), Some("11"));

    // HEAD.
    let (st, hdrs, body) = drain(
        h.svc
            .handle(req(
                Method::HEAD,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[],
                vec![],
            ))
            .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(header(&hdrs, "content-length"), Some("11"));

    // Ranged GET -> 206.
    let (st, hdrs, body) = drain(
        h.svc
            .handle(req(
                Method::GET,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[("range", "bytes=0-4")],
                vec![],
            ))
            .await,
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, b"hello");
    assert_eq!(header(&hdrs, "content-range"), Some("bytes 0-4/11"));

    // Conditional If-None-Match with the current ETag -> 304.
    let (st, _, _) = drain(
        h.svc
            .handle(req(
                Method::GET,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[("if-none-match", "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"")],
                vec![],
            ))
            .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_MODIFIED);

    // LIST objects (v2).
    let (st, _, body) = drain(
        h.svc
            .handle(req(
                Method::GET,
                Some("my-bucket"),
                None,
                &[("list-type", "2")],
                &[],
                vec![],
            ))
            .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8(body).unwrap();
    assert!(xml.contains("<Key>docs/a.txt</Key>"), "listing: {xml}");

    // DELETE.
    let (st, _, _) = drain(
        h.svc
            .handle(req(
                Method::DELETE,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[],
                vec![],
            ))
            .await,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);

    // GET after delete -> 404.
    let (st, _, _) = drain(
        h.svc
            .handle(req(
                Method::GET,
                Some("my-bucket"),
                Some("docs/a.txt"),
                &[],
                &[],
                vec![],
            ))
            .await,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn streaming_chunked_put_is_deframed() {
    let h = harness().await;
    drain(
        h.svc
            .handle(req(Method::PUT, Some("strm"), None, &[], &[], vec![]))
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
            ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
            ("content-type", "text/plain"),
        ],
        chunked,
    );
    let (st, hdrs, _) = drain(h.svc.handle(put).await).await;
    assert_eq!(st, StatusCode::OK);
    // ETag is the MD5 of the DE-FRAMED plaintext, not the framed body.
    let want_etag = format!("\"{}\"", md5_hex(payload));
    assert_eq!(header(&hdrs, "etag"), Some(want_etag.as_str()));

    let (st, _, body) = drain(
        h.svc
            .handle(req(
                Method::GET,
                Some("strm"),
                Some("obj"),
                &[],
                &[],
                vec![],
            ))
            .await,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        body, payload,
        "streaming body must be de-framed to the original payload"
    );
}

fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    h.update(data);
    hex::encode(h.finalize())
}
