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
use std::time::Duration;
use tokio::net::TcpStream;

/// Cap on the request head we will peek before giving up and falling back to hyper.
const MAX_HEAD: usize = 16 * 1024;

/// Overall deadline for peeking a complete request head before falling back to hyper (audit #12).
/// Bounds a slow or stalled (slow-loris) head so the fast path never waits on it indefinitely.
const HEAD_TIMEOUT: Duration = Duration::from_secs(15);

/// Pause between head peeks when no new bytes have arrived. The fast path only PEEKs (never
/// consumes), so the socket reports readable as long as any bytes are buffered; sleeping briefly on
/// no-progress avoids a busy-spin while still waking promptly once more of the head lands (#12).
const HEAD_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// How long the blocking sender waits to drain the client's bytes during an orderly close before
/// giving up, so a stalled peer cannot pin the blocking thread (audit #23).
const FAST_LINGER: Duration = Duration::from_secs(2);

/// The result of the fast-path attempt.
pub enum Fast {
    /// The connection was fully served via `sendfile` and is now closed.
    Handled,
    /// Not eligible; hand the (untouched) connection back to hyper.
    Fallback { stream: TcpStream },
}

/// Hand the still-pristine socket back to hyper, recording *why* the fast path declined so the
/// engage-vs-fallback ratio (and its reasons) are observable. `reason` is a fixed, low-cardinality
/// label, never request-derived.
fn fallback(stream: TcpStream, reason: &'static str) -> Fast {
    metrics::counter!("cairn_sendfile_fallback_total", "reason" => reason).increment(1);
    Fast::Fallback { stream }
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

/// The single source of truth for header-level fast-path eligibility: a clean, unconditional,
/// bodyless `GET`. A ranged GET is allowed PAST this header gate, but currently always falls back:
/// [`cairn_protocol::S3Service::handle`] only produces a `ZeroCopy` body for a FULL-object read
/// (`service.rs`, `content_range.is_none()`), so a `Range` request is served via the portable
/// stream. Letting ranges through here (rather than rejecting them) keeps this the single header
/// gate, so a future `service.rs` change that zero-copies a single sub-range needs no edit here.
/// Conditional GETs (so a 304/412 short-circuit is never sendfile'd), request bodies, and protocol
/// upgrades are not eligible. Object/transport/at-rest eligibility (uncompressed, unencrypted,
/// plaintext, HTTP/1.1) is enforced elsewhere: the body type (`S3Body::ZeroCopy` is only produced
/// for a full read of an uncompressed+unencrypted object), the plaintext-only call site, and the
/// HTTP/1.1-only [`parse_head`].
fn head_eligible(head: &Head) -> bool {
    let has = |n: &str| head.headers.iter().any(|(k, _)| k == n);
    head.is_get
        && !has("content-length")
        && !has("transfer-encoding")
        && !has("if-none-match")
        && !has("if-modified-since")
        && !has("if-match")
        && !has("if-unmodified-since")
        && !has("upgrade")
}

/// Build the HTTP/1.1 response head for the fast path. `sendfile` writes the body and we always
/// close (no keep-alive on the fast path), so any inbound `connection` header is dropped and our own
/// `connection: close` appended. `x-amz-request-id` is appended for parity with the hyper path
/// (which adds it in `server::handle`, a layer the fast path bypasses); any value already present is
/// replaced. The status's canonical reason phrase is used for a correct status line (today the fast
/// path only serves a full-object `200`; the reason lookup keeps a future ranged `206` correct too).
fn format_head(
    status: hyper::StatusCode,
    headers: &[(String, String)],
    request_id: &str,
) -> String {
    let reason = status.canonical_reason().unwrap_or("OK");
    let mut out = format!("HTTP/1.1 {} {reason}\r\n", status.as_u16());
    for (k, v) in headers {
        if k.eq_ignore_ascii_case("connection") || k.eq_ignore_ascii_case("x-amz-request-id") {
            continue;
        }
        out.push_str(k);
        out.push_str(": ");
        out.push_str(v);
        out.push_str("\r\n");
    }
    out.push_str("x-amz-request-id: ");
    out.push_str(request_id);
    out.push_str("\r\nconnection: close\r\n\r\n");
    out
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
pub async fn try_sendfile_get(
    stream: TcpStream,
    stack: &AppStack,
    peer: SocketAddr,
    request_metrics_enabled: bool,
) -> Fast {
    // Time the whole fast-path attempt so a fast-pathed GET reports `cairn_request_duration_seconds`
    // exactly like the hyper path (which this path bypasses).
    let start = std::time::Instant::now();
    // Peek the head WITHOUT consuming it: `peek` always returns from the start of the socket buffer,
    // so on a fallback the socket is pristine for hyper.
    let mut buf = vec![0u8; MAX_HEAD];
    let head_len = {
        // Peek a complete request head under an overall deadline (audit #12). Because we only PEEK
        // (the socket must stay pristine for a hyper fallback), readiness alone cannot signal "new
        // bytes" — the same buffered bytes keep the socket readable — so awaiting it in a loop
        // hot-spins on a stalled head. Instead, sleep briefly when a peek makes no progress, and
        // bound the whole head read with a timeout.
        let peek = async {
            let mut last_n = 0usize;
            loop {
                match stream.peek(&mut buf).await {
                    Ok(0) => return None, // peer closed before sending a head
                    Ok(n) => {
                        if let Some(e) = head_end(&buf[..n]) {
                            return Some(e);
                        }
                        if n >= MAX_HEAD {
                            return None; // head larger than we will buffer; let hyper handle it
                        }
                        if n == last_n {
                            tokio::time::sleep(HEAD_POLL_INTERVAL).await;
                        } else {
                            last_n = n;
                            tokio::task::yield_now().await;
                        }
                    }
                    Err(_) => return None,
                }
            }
        };
        match tokio::time::timeout(HEAD_TIMEOUT, peek).await {
            Ok(Some(e)) => e,
            // Closed, oversize, a read error, or the head never completed in time: hand the
            // still-unconsumed socket back to hyper untouched.
            Ok(None) | Err(_) => return fallback(stream, "head"),
        }
    };

    let Some(head) = parse_head(&buf[..head_len]) else {
        return fallback(stream, "parse");
    };
    // See [`head_eligible`] for the single source of truth on header-level eligibility (a clean,
    // unconditional, bodyless GET; a ranged GET passes here but `handle` serves it via the stream
    // today — only a full-object read zero-copies).
    if !head_eligible(&head) {
        return fallback(stream, "ineligible");
    }

    let (path, query) = match head.target.split_once('?') {
        Some((p, q)) => (p.to_owned(), q.to_owned()),
        None => (head.target.clone(), String::new()),
    };
    let (bucket, key) = route_path(&path);
    if bucket.is_none() || key.is_none() {
        return fallback(stream, "not_object");
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
            AuthOutcome::Denied(_) => return fallback(stream, "denied"),
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
        request_id: request_id.clone(),
    };

    // Authorize + resolve through the normal service. Only a zero-copy-eligible body is fast-pathed;
    // anything else (an error, a compressed/encrypted object) hands the untouched socket to hyper.
    let empty: cairn_types::BodyStream = Box::pin(futures_util::stream::empty());
    let resp = stack.s3.handle(s3req, empty).await;
    let status = resp.status;
    let S3Body::ZeroCopy {
        zero_copy, length, ..
    } = resp.body
    else {
        return fallback(stream, "not_zerocopy");
    };

    // The body is exactly `length` bytes (the full object or one resolved sub-range); record it as
    // the response size for parity with the hyper path's content-length-based `cairn_bytes_sent_total`.
    let resp_bytes = length;
    let out = format_head(status, &resp.headers, &request_id);

    // Hand the socket to a blocking thread: make it blocking, write the head, then `sendfile` the
    // body straight from the page cache. The unconsumed peeked request bytes are discarded when the
    // connection closes (`std_stream` drops) — correct for a bodyless GET we answer with `close`.
    let std_stream = match stream.into_std() {
        Ok(s) => s,
        Err(_) => return Fast::Handled,
    };
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        use std::io::{Read, Write};
        std_stream.set_nonblocking(false)?;
        let mut s = std_stream;
        s.write_all(out.as_bytes())?;
        crate::sendfile::sendfile_all(
            s.as_raw_fd(),
            zero_copy.file.as_raw_fd(),
            zero_copy.offset,
            length,
        )?;
        s.flush()?;
        // Orderly close (audit #23): we PEEKED the request but never consumed it, so closing now
        // with unread bytes in the socket buffer makes Linux send a RST instead of a FIN — and a
        // RST can discard the response still in flight, truncating the body the client is reading.
        // Half-close our write side (flushing a FIN), then drain the client's pending bytes (the
        // un-consumed request and its FIN) under a bounded read timeout so the final close is an
        // orderly shutdown rather than a reset.
        let _ = s.shutdown(std::net::Shutdown::Write);
        s.set_read_timeout(Some(FAST_LINGER))?;
        let mut scratch = [0u8; 2048];
        loop {
            match s.read(&mut scratch) {
                Ok(0) => break,    // client closed its side — orderly
                Ok(_) => continue, // discard any pending request bytes
                Err(_) => break,   // read timeout or error — stop draining
            }
        }
        Ok(())
    })
    .await;
    match result {
        Ok(Ok(())) => {
            metrics::counter!("cairn_sendfile_get_total", "result" => "ok", "transport" => "plain")
                .increment(1);
        }
        Ok(Err(e)) => {
            metrics::counter!("cairn_sendfile_get_total", "result" => "error", "transport" => "plain")
                .increment(1);
            tracing::debug!(error = %e, "sendfile fast path failed mid-write");
        }
        Err(e) => tracing::debug!(error = %e, "sendfile blocking task panicked"),
    }

    // Parity with `server::handle` (ARCH 26): a fast-pathed GET bypasses that middleware, so emit the
    // same request/throughput metrics and usage-analytics ingestion here. The response was authorized
    // and its status/size are known regardless of whether the body transfer later truncated, exactly
    // like the hyper path counts at the (declared) header level rather than on body completion.
    let elapsed = start.elapsed();
    let status_code = status.as_u16();
    let route = crate::server::classify_route(&path);
    metrics::counter!(
        "cairn_requests_total",
        "method" => "GET",
        "status" => status_code.to_string(),
        "route" => route,
    )
    .increment(1);
    metrics::histogram!(
        "cairn_request_duration_seconds",
        "method" => "GET",
        "route" => route,
    )
    .record(elapsed.as_secs_f64());
    if resp_bytes > 0 {
        metrics::counter!("cairn_bytes_sent_total").increment(resp_bytes);
    }
    if request_metrics_enabled {
        if let Some((op, bucket)) =
            crate::server::classify_operation(&hyper::Method::GET, &path, &query)
        {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs() as i64);
            stack.request_metrics.record(
                &op,
                &bucket,
                status_code,
                elapsed.as_millis() as u64,
                0,
                resp_bytes,
                now_secs,
            );
        }
    }

    Fast::Handled
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(method: &str, headers: &[(&str, &str)]) -> Head {
        Head {
            target: "/bucket/key".to_owned(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), (*v).to_owned()))
                .collect(),
            is_get: method == "GET",
        }
    }

    #[test]
    fn parse_head_accepts_a_clean_http11_get() {
        let raw = b"GET /b/k?x=1 HTTP/1.1\r\nHost: example.com\r\nRange: bytes=0-9\r\n\r\n";
        let h = parse_head(raw).expect("a well-formed HTTP/1.1 GET head parses");
        assert!(h.is_get);
        assert_eq!(h.target, "/b/k?x=1");
        // Header names are lowercased; values preserved.
        assert!(
            h.headers
                .iter()
                .any(|(k, v)| k == "host" && v == "example.com")
        );
        assert!(
            h.headers
                .iter()
                .any(|(k, v)| k == "range" && v == "bytes=0-9")
        );
    }

    #[test]
    fn parse_head_rejects_http2_preface_and_non_http11() {
        // The HTTP/2 connection preface must never parse as an HTTP/1.1 head — the fast path can only
        // frame HTTP/1.1, so an h2 client must fall through to hyper.
        assert!(parse_head(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").is_none());
        // HTTP/1.0 (no keep-alive semantics we rely on) is not HTTP/1.1.
        assert!(parse_head(b"GET /b/k HTTP/1.0\r\nHost: h\r\n\r\n").is_none());
        // Garbage / truncated request lines.
        assert!(parse_head(b"not a request\r\n\r\n").is_none());
    }

    #[test]
    fn parse_head_marks_non_get_methods() {
        let h = parse_head(b"PUT /b/k HTTP/1.1\r\nHost: h\r\n\r\n").expect("parses");
        assert!(!h.is_get, "a PUT is parsed but not a GET");
    }

    #[test]
    fn head_eligible_accepts_plain_and_ranged_gets() {
        assert!(head_eligible(&head("GET", &[("host", "h")])));
        // A single-Range GET is eligible at the header layer; `handle` decides the body.
        assert!(head_eligible(&head("GET", &[("range", "bytes=0-99")])));
    }

    #[test]
    fn head_eligible_rejects_non_get_body_conditional_and_upgrade() {
        assert!(!head_eligible(&head("POST", &[])));
        assert!(!head_eligible(&head("GET", &[("content-length", "10")])));
        assert!(!head_eligible(&head(
            "GET",
            &[("transfer-encoding", "chunked")]
        )));
        assert!(!head_eligible(&head(
            "GET",
            &[("if-none-match", "\"abc\"")]
        )));
        assert!(!head_eligible(&head("GET", &[("if-modified-since", "x")])));
        assert!(!head_eligible(&head("GET", &[("if-match", "\"abc\"")])));
        assert!(!head_eligible(&head(
            "GET",
            &[("if-unmodified-since", "x")]
        )));
        assert!(!head_eligible(&head("GET", &[("upgrade", "h2c")])));
    }

    #[test]
    fn format_head_writes_status_line_close_and_request_id() {
        let headers = vec![
            ("content-length".to_owned(), "42".to_owned()),
            ("etag".to_owned(), "\"abc\"".to_owned()),
            // An inbound keep-alive must be dropped; the fast path always closes.
            ("connection".to_owned(), "keep-alive".to_owned()),
            // A stale request-id must be replaced, not duplicated.
            ("x-amz-request-id".to_owned(), "stale".to_owned()),
        ];
        let out = format_head(hyper::StatusCode::OK, &headers, "req-123");
        assert!(out.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(out.contains("content-length: 42\r\n"));
        assert!(out.contains("etag: \"abc\"\r\n"));
        assert!(out.ends_with("\r\n\r\n"));
        // Exactly one request-id, carrying our value, not the stale inbound one.
        assert_eq!(out.matches("x-amz-request-id:").count(), 1);
        assert!(out.contains("x-amz-request-id: req-123\r\n"));
        assert!(!out.contains("stale"));
        // The connection header is normalized to a single `close`.
        assert!(out.contains("connection: close\r\n"));
        assert!(!out.contains("keep-alive"));
        assert_eq!(out.to_ascii_lowercase().matches("connection:").count(), 1);
    }

    #[test]
    fn format_head_uses_the_canonical_206_reason_for_ranged_responses() {
        let headers = vec![("content-range".to_owned(), "bytes 0-9/100".to_owned())];
        let out = format_head(hyper::StatusCode::PARTIAL_CONTENT, &headers, "r");
        assert!(out.starts_with("HTTP/1.1 206 Partial Content\r\n"));
        assert!(out.contains("content-range: bytes 0-9/100\r\n"));
    }
}
