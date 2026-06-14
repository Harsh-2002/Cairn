//! Plaintext HTTP/1.1 `sendfile` fast path for object GETs (`fast-io`, Linux only).
//!
//! Before a plaintext connection is handed to hyper, [`try_sendfile_get`] PEEKS the first request
//! (via `recv(MSG_PEEK)`, which does not consume it). For a clean `GET` of an uncompressed,
//! unencrypted object it serves the response head and then the body with a single `sendfile(2)`
//! (file → socket, no userspace copy) and closes the connection. EVERYTHING else — a non-GET, a
//! ranged or conditional GET, a compressed/encrypted object, a body, an upgrade, or anything it is
//! unsure about — is handed back to hyper with the socket UNTOUCHED (peek consumed nothing), so
//! hyper serves it exactly as if the fast path never ran.
//!
//! Security: the fast path authorizes through the SAME [`cairn_protocol::S3Service::handle`] as the normal
//! path (same authenticator, same bucket policy/ACL evaluation), and only diverges in HOW the bytes
//! of an already-authorized response are written. It never serves anything the normal path would not.

use crate::adapter::route_path;
use crate::stack::AppStack;
use cairn_protocol::{S3Body, S3Request};
use cairn_types::auth::{AuthOutcome, RequestView};
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use tokio::net::TcpStream;

/// Cap on the request head we will peek before giving up and falling back to hyper.
const MAX_HEAD: usize = 16 * 1024;

/// The result of the fast-path attempt.
pub enum Fast {
    /// The connection was fully served via `sendfile` and is now closed.
    Handled,
    /// Not eligible; hand the (untouched) connection back to hyper.
    Fallback { stream: TcpStream },
}

/// Index just past the `CRLFCRLF` head terminator, if present in `buf`.
fn head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

struct Head {
    target: String,
    headers: Vec<(String, String)>,
    is_get: bool,
}

/// Parse just enough of an HTTP/1.1 request head: the method, the request target, and the headers
/// (names lowercased). Returns `None` for anything that isn't a well-formed HTTP/1.1 head.
fn parse_head(bytes: &[u8]) -> Option<Head> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.split("\r\n");
    let mut rl = lines.next()?.split(' ');
    let method = rl.next()?;
    let target = rl.next()?.to_owned();
    if !rl.next()?.starts_with("HTTP/1.1") {
        return None;
    }
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (k, v) = line.split_once(':')?;
        headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
    }
    Some(Head {
        target,
        headers,
        is_get: method == "GET",
    })
}

/// Attempt to serve the first request on a plaintext connection via `sendfile`. See the module docs.
pub async fn try_sendfile_get(stream: TcpStream, stack: &AppStack, peer: SocketAddr) -> Fast {
    // Peek the head WITHOUT consuming it: `peek` always returns from the start of the socket buffer,
    // so on a fallback the socket is pristine for hyper.
    let mut buf = vec![0u8; MAX_HEAD];
    let head_len = loop {
        match stream.peek(&mut buf).await {
            Ok(0) => return Fast::Fallback { stream },
            Ok(n) => {
                if let Some(e) = head_end(&buf[..n]) {
                    break e;
                }
                if n >= MAX_HEAD {
                    return Fast::Fallback { stream };
                }
                // The head hasn't fully arrived yet; wait for more bytes, then peek again.
                if stream.readable().await.is_err() {
                    return Fast::Fallback { stream };
                }
            }
            Err(_) => return Fast::Fallback { stream },
        }
    };

    let Some(head) = parse_head(&buf[..head_len]) else {
        return Fast::Fallback { stream };
    };
    // Only a clean, full, unconditional GET with no body is eligible; everything else goes to hyper.
    let has = |n: &str| head.headers.iter().any(|(k, _)| k == n);
    if !head.is_get
        || has("content-length")
        || has("transfer-encoding")
        || has("range")
        || has("if-none-match")
        || has("if-modified-since")
        || has("if-match")
        || has("if-unmodified-since")
        || has("upgrade")
    {
        return Fast::Fallback { stream };
    }

    let (path, query) = match head.target.split_once('?') {
        Some((p, q)) => (p.to_owned(), q.to_owned()),
        None => (head.target.clone(), String::new()),
    };
    let (bucket, key) = route_path(&path);
    if bucket.is_none() || key.is_none() {
        return Fast::Fallback { stream };
    }
    let host = head
        .headers
        .iter()
        .find(|(k, _)| k == "host")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();

    // Authenticate through the same chain as the normal path; defer any auth error to hyper so it
    // renders the proper S3 error envelope.
    let principal = {
        let view = RequestView {
            method: "GET",
            path: &path,
            query: &query,
            headers: &head.headers,
            host: &host,
            source: peer.ip(),
            secure_transport: false,
        };
        match stack.auth.authenticate(&view).await {
            AuthOutcome::Authenticated(p) => Some(p),
            AuthOutcome::NotApplicable => None,
            AuthOutcome::Denied(_) => return Fast::Fallback { stream },
        }
    };

    let request_id = uuid::Uuid::new_v4().simple().to_string();
    let s3req = S3Request {
        method: hyper::Method::GET,
        bucket,
        key,
        query: query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (k, v) = p.split_once('=').unwrap_or((p, ""));
                (k.to_owned(), v.to_owned())
            })
            .collect(),
        headers: head.headers.clone(),
        principal,
        source: peer.ip(),
        secure: false,
        request_id,
    };

    // Authorize + resolve through the normal service. Only a zero-copy-eligible body is fast-pathed;
    // anything else (an error, a compressed/encrypted object) hands the untouched socket to hyper.
    let empty: cairn_types::BodyStream = Box::pin(futures_util::stream::empty());
    let resp = stack.s3.handle(s3req, empty).await;
    let S3Body::ZeroCopy {
        zero_copy, length, ..
    } = resp.body
    else {
        return Fast::Fallback { stream };
    };

    // Build the response head. `sendfile` writes the body, and we always close (no keep-alive on the
    // fast path), so drop any inbound connection header and append our own.
    let mut out = format!("HTTP/1.1 {} OK\r\n", resp.status.as_u16());
    for (k, v) in &resp.headers {
        if k.eq_ignore_ascii_case("connection") {
            continue;
        }
        out.push_str(k);
        out.push_str(": ");
        out.push_str(v);
        out.push_str("\r\n");
    }
    out.push_str("connection: close\r\n\r\n");

    // Hand the socket to a blocking thread: make it blocking, write the head, then `sendfile` the
    // body straight from the page cache. The unconsumed peeked request bytes are discarded when the
    // connection closes (`std_stream` drops) — correct for a bodyless GET we answer with `close`.
    let std_stream = match stream.into_std() {
        Ok(s) => s,
        Err(_) => return Fast::Handled,
    };
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        use std::io::Write;
        std_stream.set_nonblocking(false)?;
        let mut s = std_stream;
        s.write_all(out.as_bytes())?;
        crate::sendfile::sendfile_all(
            s.as_raw_fd(),
            zero_copy.file.as_raw_fd(),
            zero_copy.offset,
            length,
        )?;
        s.flush()
    })
    .await;
    match result {
        Ok(Ok(())) => metrics::counter!("cairn_sendfile_get_total", "result" => "ok").increment(1),
        Ok(Err(e)) => {
            metrics::counter!("cairn_sendfile_get_total", "result" => "error").increment(1);
            tracing::debug!(error = %e, "sendfile fast path failed mid-write");
        }
        Err(e) => tracing::debug!(error = %e, "sendfile blocking task panicked"),
    }
    Fast::Handled
}
