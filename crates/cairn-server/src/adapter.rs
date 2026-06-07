//! Adapts hyper's request/response to the library-neutral S3 request/response, performs
//! authentication, and routes path-style addressing into the S3 service. Responses are
//! buffered in this first wiring; streaming the response body straight from the blob store is a
//! hardening-wave refinement.

use crate::stack::AppStack;
use bytes::Bytes;
use cairn_s3::{S3Body, S3Request, S3Response, error_response};
use cairn_types::auth::{AuthOutcome, RequestView};
use cairn_types::error::{BodyError, Error};
use cairn_types::id::{BucketName, ObjectKey};
use futures_util::StreamExt;
use http_body_util::{BodyExt, BodyStream, Full};
use hyper::body::Incoming;
use hyper::{Request, Response};
use std::net::IpAddr;

/// Handle an S3 (or anonymous) HTTP request end to end.
pub async fn handle(
    stack: &AppStack,
    req: Request<Incoming>,
    peer: IpAddr,
    secure: bool,
    request_id: String,
) -> Response<Full<Bytes>> {
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
                return collect(error_response(&Error::from(e), &resource, &request_id)).await;
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
            .body(Full::new(Bytes::from(resp.body)))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())));
    }
    if raw_path == "/ui" || raw_path.starts_with("/ui/") {
        return serve_ui(&raw_path);
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
    collect(stack.s3.handle(s3req, body).await).await
}

/// Serve the embedded management UI under `/ui/`.
fn serve_ui(path: &str) -> Response<Full<Bytes>> {
    if path == "/ui" {
        return Response::builder()
            .status(301)
            .header("location", "/ui/")
            .body(Full::new(Bytes::new()))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())));
    }
    let rel = path.strip_prefix("/ui/").unwrap_or("");
    let (content_type, bytes) = if rel.is_empty() {
        cairn_ui::spa_shell()
    } else {
        cairn_ui::asset(rel).unwrap_or_else(cairn_ui::spa_shell)
    };
    Response::builder()
        .status(200)
        .header("content-type", content_type)
        .body(Full::new(Bytes::from(bytes.into_owned())))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
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

async fn collect(resp: S3Response) -> Response<Full<Bytes>> {
    let body = match resp.body {
        S3Body::Empty => Bytes::new(),
        S3Body::Bytes(b) => b,
        S3Body::Stream { mut stream, .. } => {
            let mut buf = Vec::new();
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(b) => buf.extend_from_slice(&b),
                    Err(_) => break,
                }
            }
            Bytes::from(buf)
        }
    };
    let mut builder = Response::builder().status(resp.status);
    for (k, v) in resp.headers {
        builder = builder.header(k, v);
    }
    builder
        .body(Full::new(body))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
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
