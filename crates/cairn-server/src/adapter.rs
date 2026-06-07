//! Adapts hyper's request/response to the library-neutral S3 request/response, performs
//! authentication, and routes path-style addressing into the S3 service.
//!
//! Object reads (`S3Body::Stream`) are forwarded to hyper as a streaming body so a large GET
//! flows blob -> socket with bounded memory (ARCH §7.4/§7.6/§7.8): no whole-object buffer is ever
//! materialised. Empty and in-memory (XML/error) bodies stay fully buffered, which is correct —
//! they are already small and bounded. Request bodies for object PUT are streamed separately via
//! [`incoming_to_stream`].

use crate::stack::AppStack;
use bytes::Bytes;
use cairn_s3::{S3Body, S3Request, S3Response, error_response};
use cairn_types::auth::{AuthOutcome, RequestView};
use cairn_types::error::{BodyError, Error};
use cairn_types::id::{BucketName, ObjectKey};
use futures_util::StreamExt;
use http_body_util::{BodyExt, BodyStream, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::{Request, Response};
use std::net::IpAddr;

/// The unified HTTP response body: either a fully-buffered in-memory body (empty, XML, errors,
/// UI assets, management JSON) or a blob stream forwarded frame-by-frame from the blob store.
/// Boxing both into one type lets every response path return a single concrete `Body`. It is an
/// `UnsyncBoxBody` rather than a `BoxBody` because the underlying blob stream is `Send` but not
/// `Sync`; hyper only requires the body to be `Send`, so dropping the `Sync` bound is correct and
/// avoids buffering the stream to satisfy it.
pub type ResponseBody = http_body_util::combinators::UnsyncBoxBody<Bytes, BodyError>;

/// Wrap a fully-buffered byte payload as a [`ResponseBody`].
pub(crate) fn full_body(bytes: Bytes) -> ResponseBody {
    Full::new(bytes)
        .map_err(|e: std::convert::Infallible| match e {})
        .boxed_unsync()
}

/// Handle an S3 (or anonymous) HTTP request end to end.
pub async fn handle(
    stack: &AppStack,
    req: Request<Incoming>,
    peer: IpAddr,
    secure: bool,
    request_id: String,
) -> Response<ResponseBody> {
    let method = req.method().clone();
    let raw_path = req.uri().path().to_owned();
    let query_str = req.uri().query().unwrap_or("").to_owned();
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_ascii_lowercase(),
                v.to_str().unwrap_or("").to_owned(),
            )
        })
        .collect();
    let host = headers
        .iter()
        .find(|(k, _)| k == "host")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();

    // Authenticate against a borrowed, library-neutral view.
    let principal = {
        let view = RequestView {
            method: method.as_str(),
            path: &raw_path,
            query: &query_str,
            headers: &headers,
            host: &host,
            source: peer,
            secure_transport: secure,
        };
        match stack.auth.authenticate(&view).await {
            AuthOutcome::Authenticated(p) => Some(p),
            AuthOutcome::NotApplicable => None,
            AuthOutcome::Denied(e) => {
                let resource = raw_path.clone();
                return render(error_response(&Error::from(e), &resource, &request_id));
            }
        }
    };

    // The management JSON API and the embedded web UI share the listener with the S3 surface.
    if let Some(subpath) = raw_path.strip_prefix("/api/v1") {
        let query = parse_query(&query_str);
        let body_bytes = match req.into_body().collect().await {
            Ok(c) => c.to_bytes(),
            Err(_) => Bytes::new(),
        };
        let resp = stack
            .control
            .handle(&method, subpath, &query, principal.as_ref(), body_bytes)
            .await;
        return Response::builder()
            .status(resp.status)
            .header("content-type", "application/json")
            .body(full_body(Bytes::from(resp.body)))
            .unwrap_or_else(|_| Response::new(full_body(Bytes::new())));
    }
    // The embedded management UI is served under `/web`. `/ui/*` (the former mount) redirects there
    // so existing links keep working.
    if raw_path == "/web" || raw_path.starts_with("/web/") {
        return serve_ui(&raw_path, "/web");
    }
    if raw_path == "/ui" || raw_path.starts_with("/ui/") {
        let target = raw_path.replacen("/ui", "/web", 1);
        return redirect(&target);
    }

    let (bucket, key) = route_path(&raw_path);
    let query = parse_query(&query_str);
    let body = incoming_to_stream(req.into_body());

    let s3req = S3Request {
        method,
        bucket,
        key,
        query,
        headers,
        principal,
        source: peer,
        secure,
        request_id,
    };
    render(stack.s3.handle(s3req, body).await)
}

/// Serve the embedded management UI mounted at `prefix` (e.g. `/web`). A bare prefix with no
/// trailing slash redirects to `prefix/`; any path that isn't an embedded asset falls back to the
/// SPA shell so client-side routing survives a reload.
fn serve_ui(path: &str, prefix: &str) -> Response<ResponseBody> {
    if path == prefix {
        return redirect(&format!("{prefix}/"));
    }
    let rel = path
        .strip_prefix(prefix)
        .and_then(|p| p.strip_prefix('/'))
        .unwrap_or("");
    let (content_type, bytes) = if rel.is_empty() {
        cairn_ui::spa_shell()
    } else {
        cairn_ui::asset(rel).unwrap_or_else(cairn_ui::spa_shell)
    };
    Response::builder()
        .status(200)
        .header("content-type", content_type)
        .body(full_body(Bytes::from(bytes.into_owned())))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// A 301 redirect to `location`.
fn redirect(location: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(301)
        .header("location", location)
        .body(full_body(Bytes::new()))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// Split a path-style request path into a bucket and key.
fn route_path(raw_path: &str) -> (Option<BucketName>, Option<ObjectKey>) {
    let p = raw_path.strip_prefix('/').unwrap_or(raw_path);
    if p.is_empty() {
        return (None, None);
    }
    let (bucket_seg, key_rest) = match p.split_once('/') {
        Some((b, k)) => (b, Some(k)),
        None => (p, None),
    };
    let bucket = BucketName::parse(&pct_decode(bucket_seg)).ok();
    let key = key_rest
        .filter(|k| !k.is_empty())
        .and_then(|k| ObjectKey::parse(&pct_decode(k)).ok());
    (bucket, key)
}

fn parse_query(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (pct_decode(k), pct_decode(v))
        })
        .collect()
}

fn incoming_to_stream(body: Incoming) -> cairn_types::BodyStream {
    Box::pin(BodyStream::new(body).filter_map(|res| async move {
        match res {
            Ok(frame) => frame.into_data().ok().map(Ok),
            Err(e) => Some(Err(BodyError::Transport(e.to_string()))),
        }
    }))
}

/// Render an [`S3Response`] onto the wire. `Empty`/`Bytes` bodies are already bounded and stay
/// buffered; a `Stream` body (object read) is forwarded to hyper as a `StreamBody` so bytes flow
/// from the blob store to the socket in bounded chunks with backpressure, never materialising the
/// whole object in memory (ARCH §7.4/§7.6/§7.8). The stream's `BlobError` is mapped onto the
/// body's `BodyError`; a mid-stream blob failure terminates the body, which surfaces to the
/// client as a truncated transfer (the status line is already sent by then).
fn render(resp: S3Response) -> Response<ResponseBody> {
    let body: ResponseBody = match resp.body {
        S3Body::Empty => full_body(Bytes::new()),
        S3Body::Bytes(b) => full_body(b),
        S3Body::Stream { stream, .. } => {
            let framed = stream.map(|chunk| {
                chunk
                    .map(Frame::data)
                    .map_err(|e| BodyError::Transport(e.to_string()))
            });
            BodyExt::boxed_unsync(StreamBody::new(framed))
        }
    };
    let mut builder = Response::builder().status(resp.status);
    for (k, v) in resp.headers {
        builder = builder.header(k, v);
    }
    builder
        .body(body)
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// Minimal percent-decoding for path/query segments.
fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex_val(b[i + 1]), hex_val(b[i + 2])) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::error::BlobError;
    use futures_util::stream;
    use http::StatusCode;

    /// A `Stream` response body is forwarded frame-by-frame, not drained into one buffer: the
    /// rendered body yields one HTTP data frame per source chunk and the bytes round-trip
    /// unchanged (ARCH §7.4/§7.6/§7.8, High #4).
    #[tokio::test]
    async fn stream_response_is_forwarded_chunk_by_chunk() {
        let chunks: Vec<Result<Bytes, BlobError>> = vec![
            Ok(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"streamed ")),
            Ok(Bytes::from_static(b"world")),
        ];
        let stream: cairn_types::BlobStream = Box::pin(stream::iter(chunks));
        let resp = S3Response {
            status: StatusCode::OK,
            headers: vec![("content-length".to_owned(), "20".to_owned())],
            body: S3Body::Stream { length: 20, stream },
        };

        let mut body = render(resp).into_body();
        let mut frames = 0usize;
        let mut collected = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.expect("frame ok");
            if let Ok(data) = frame.into_data() {
                frames += 1;
                collected.extend_from_slice(&data);
            }
        }
        assert_eq!(collected, b"hello streamed world");
        // Three source chunks must surface as three distinct data frames: proof the body streams
        // rather than collecting everything into a single buffer first.
        assert_eq!(frames, 3, "each source chunk must be its own frame");
    }

    /// A buffered (`Bytes`) response stays a single bounded body.
    #[tokio::test]
    async fn bytes_response_round_trips() {
        let resp = S3Response {
            status: StatusCode::OK,
            headers: Vec::new(),
            body: S3Body::Bytes(Bytes::from_static(b"<xml/>")),
        };
        let body = render(resp).into_body();
        let collected = body.collect().await.expect("collect").to_bytes();
        assert_eq!(&collected[..], b"<xml/>");
    }

    /// A mid-stream blob error terminates the body with a transport error rather than panicking
    /// or silently truncating without signal.
    #[tokio::test]
    async fn stream_error_surfaces_as_body_error() {
        let chunks: Vec<Result<Bytes, BlobError>> = vec![
            Ok(Bytes::from_static(b"partial")),
            Err(BlobError::Io("disk gone".to_owned())),
        ];
        let stream: cairn_types::BlobStream = Box::pin(stream::iter(chunks));
        let resp = S3Response {
            status: StatusCode::OK,
            headers: Vec::new(),
            body: S3Body::Stream { length: 7, stream },
        };
        let mut body = render(resp).into_body();
        let first = body.frame().await.expect("first frame").expect("ok");
        assert_eq!(
            first.into_data().expect("data"),
            Bytes::from_static(b"partial")
        );
        let second = body.frame().await.expect("second frame");
        assert!(second.is_err(), "blob error must surface as a body error");
    }
}
