//! Integration tests for [`HttpS3Source::get_object`] against a tiny in-process hyper server,
//! proving the tag-fetch behavior end to end over a real signed HTTP round-trip: a tagged object's
//! tags are fetched and attached, an untagged object costs no error, and a broken tagging endpoint
//! degrades to no tags rather than failing the whole object.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use cairn_import::{HttpS3Source, ImportError, ImportTimeouts, SourceConfig, SourceReader};

/// Spawn a tiny router server: routes by exact `path?query`, serving canned (status, content-type,
/// body) triples. Unmatched requests get 404.
async fn spawn_router(routes: Vec<(String, u16, &'static str, Vec<u8>)>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let authority = format!("127.0.0.1:{}", addr.port());
    let routes = Arc::new(Mutex::new(routes));

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let routes = routes.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service =
                    service_fn(move |req: Request<Incoming>| {
                        let routes = routes.clone();
                        async move {
                            let path_and_query = req
                                .uri()
                                .path_and_query()
                                .map(|pq| pq.as_str().to_owned())
                                .unwrap_or_default();
                            let hit = routes
                                .lock()
                                .unwrap()
                                .iter()
                                .find(|(k, ..)| *k == path_and_query)
                                .cloned();
                            let (status, ctype, body) = hit
                                .map(|(_, s, c, b)| (s, c, b))
                                .unwrap_or((404, "text/plain", b"not found".to_vec()));
                            Ok::<_, std::convert::Infallible>(
                                Response::builder()
                                    .status(StatusCode::from_u16(status).unwrap())
                                    .header("content-type", ctype)
                                    .body(Full::new(Bytes::from(body)))
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

fn source(authority: &str) -> HttpS3Source {
    source_with_timeouts(authority, ImportTimeouts::default())
}

fn source_with_timeouts(authority: &str, timeouts: ImportTimeouts) -> HttpS3Source {
    HttpS3Source::new(SourceConfig {
        endpoint: format!("http://{authority}"),
        region: "us-east-1".to_owned(),
        access_key_id: "ak".to_owned(),
        secret_access_key: "sk".into(),
        ca_cert_pem: None,
        insecure_skip_verify: false,
        allow_internal_endpoints: true,
        timeouts,
    })
    .unwrap()
}

async fn drain(mut body: cairn_types::BlobStream) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

#[tokio::test]
async fn tagged_object_gets_its_tags() {
    let authority = spawn_router(vec![
        (
            "/bkt/tagged.txt".to_owned(),
            200,
            "text/plain",
            b"hello".to_vec(),
        ),
        (
            "/bkt/tagged.txt?tagging=".to_owned(),
            200,
            "application/xml",
            b"<Tagging><TagSet><Tag><Key>team</Key><Value>eng</Value></Tag></TagSet></Tagging>"
                .to_vec(),
        ),
    ])
    .await;
    let obj = source(&authority)
        .get_object("bkt", "tagged.txt")
        .await
        .unwrap();
    assert_eq!(obj.tags, vec![("team".to_owned(), "eng".to_owned())]);
    assert_eq!(drain(obj.body).await, b"hello");
}

#[tokio::test]
async fn untagged_object_has_no_tags_and_no_error() {
    let authority = spawn_router(vec![
        (
            "/bkt/untagged.txt".to_owned(),
            200,
            "text/plain",
            b"world".to_vec(),
        ),
        (
            "/bkt/untagged.txt?tagging=".to_owned(),
            200,
            "application/xml",
            b"<Tagging><TagSet/></Tagging>".to_vec(),
        ),
    ])
    .await;
    let obj = source(&authority)
        .get_object("bkt", "untagged.txt")
        .await
        .unwrap();
    assert!(obj.tags.is_empty());
    assert_eq!(drain(obj.body).await, b"world");
}

/// A source whose tagging endpoint is broken (a real-world case: a source that doesn't support
/// tagging, or is transiently failing) must NOT fail the object copy — the body already succeeded.
#[tokio::test]
async fn broken_tagging_endpoint_is_non_fatal() {
    let authority = spawn_router(vec![
        (
            "/bkt/failtag.txt".to_owned(),
            200,
            "text/plain",
            b"still here".to_vec(),
        ),
        (
            "/bkt/failtag.txt?tagging=".to_owned(),
            500,
            "text/plain",
            b"boom".to_vec(),
        ),
    ])
    .await;
    let obj = source(&authority)
        .get_object("bkt", "failtag.txt")
        .await
        .unwrap();
    assert!(
        obj.tags.is_empty(),
        "a broken tagging endpoint must degrade to no tags, not fail the object"
    );
    assert_eq!(drain(obj.body).await, b"still here");
}

/// A peer that accepts the request but never produces response headers must be classified
/// unavailable at the response-head deadline rather than wedging the import worker.
#[tokio::test]
async fn stalled_response_head_times_out() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let authority = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 2048];
        let _ = stream.read(&mut request).await.unwrap();
        std::future::pending::<()>().await;
    });
    let source = source_with_timeouts(
        &authority,
        ImportTimeouts {
            connect: Duration::from_secs(1),
            response_head: Duration::from_millis(100),
            body_idle: Duration::from_secs(1),
            whole_object: Duration::from_secs(2),
        },
    );

    let error = tokio::time::timeout(Duration::from_secs(2), source.list_buckets())
        .await
        .expect("source response-head deadline must return")
        .expect_err("stalled response head must fail");
    assert!(
        matches!(&error, ImportError::Unavailable(_)),
        "head timeout classification: {error}"
    );
    assert!(
        error.to_string().contains("response head timed out"),
        "{error}"
    );
    server.abort();
}

/// Serve one object byte, then trickle subsequent chunks more slowly than the configured idle
/// window. The body stream must surface a timeout without waiting for EOF; the independent tagging
/// request still succeeds so this isolates body-idle behavior.
#[tokio::test]
async fn slow_body_trickle_hits_idle_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let authority = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut chunk = [0u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let Ok(n) = stream.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..n]);
                }
                if String::from_utf8_lossy(&request).contains("?tagging=") {
                    let body = b"<Tagging><TagSet/></Tagging>";
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\
                         Content-Type: application/xml\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes()).await;
                    let _ = stream.write_all(body).await;
                    return;
                }

                let _ = stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
                          Transfer-Encoding: chunked\r\n\r\n1\r\nx\r\n",
                    )
                    .await;
                let _ = stream.flush().await;
                loop {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    if stream.write_all(b"1\r\ny\r\n").await.is_err() {
                        return;
                    }
                    let _ = stream.flush().await;
                }
            });
        }
    });
    let source = source_with_timeouts(
        &authority,
        ImportTimeouts {
            connect: Duration::from_secs(1),
            response_head: Duration::from_secs(1),
            body_idle: Duration::from_millis(100),
            whole_object: Duration::from_secs(2),
        },
    );

    let obj = source.get_object("bkt", "slow.bin").await.unwrap();
    let mut body = obj.body;
    assert_eq!(
        body.next().await.unwrap().unwrap(),
        Bytes::from_static(b"x")
    );
    let error = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .expect("body-idle deadline must return")
        .expect("timeout is reported as one terminal stream item")
        .expect_err("slow trickle must fail");
    assert!(
        error.to_string().contains("body idle timeout"),
        "unexpected body error: {error}"
    );
    server.abort();
}
