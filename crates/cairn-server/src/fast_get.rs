//! Plaintext HTTP/1.1 `sendfile` fast path for object GETs (`fast-io`, Linux only).
//!
//! On a plaintext connection, [`try_sendfile_get`] runs a small Cairn-owned HTTP/1.1 keep-alive
//! loop. Per request it CONSUMES the request head and, for a clean `GET` of an uncompressed,
//! unencrypted object — the full object OR a single byte-range — it writes the response head and
//! then the body with one `sendfile(2)` (file → socket, no userspace copy) and KEEPS the connection
//! alive for the next request. So a pooled client (boto3/warp) is served entirely on the zero-copy
//! path, not just its first request.
//!
//! EVERYTHING else — a non-GET, a conditional GET, a request with a body, a compressed/encrypted
//! object, a multi-range or unsatisfiable range, a body below the size floor, an upgrade, a
//! malformed/oversize head, or anything it is unsure about — is handed to hyper via [`Rewind`],
//! which replays the bytes already consumed so hyper serves the request exactly as the client sent
//! it. Once a connection hands off, hyper owns it for the rest of its life.
//!
//! Security: the fast path authorizes through the SAME [`cairn_protocol::S3Service::handle`] as the
//! normal path (same authenticator, same bucket policy/ACL evaluation), and only diverges in HOW the
//! bytes of an already-authorized response are written. It never serves anything the normal path
//! would not.

use crate::adapter::route_path;
use crate::stack::AppStack;
use cairn_protocol::{S3Body, S3Request};
use cairn_types::auth::{AuthOutcome, RequestView};
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// Cap on the request head we will read before giving up and falling back to hyper.
const MAX_HEAD: usize = 16 * 1024;

/// Per-request deadline for reading a complete request head before giving up (audit #12). Bounds a
/// slow or stalled (slow-loris) head, and — between keep-alive requests on the same connection — acts
/// as the idle timeout: a connection that sends no next request within this window is closed.
const HEAD_TIMEOUT: Duration = Duration::from_secs(15);

/// How long the blocking sender waits to drain the client's bytes during an orderly close before
/// giving up, so a stalled peer cannot pin the blocking thread (audit #23).
const FAST_LINGER: Duration = Duration::from_secs(2);

/// The result of the fast-path connection loop.
pub enum Fast {
    /// The connection was fully served (one or more `sendfile` GETs) and is now closed.
    Handled,
    /// A request the fast path cannot serve was encountered; hand the connection to hyper with the
    /// already-consumed bytes replayed first (see [`Rewind`]).
    Fallback { stream: Rewind },
}

/// A connection handed back to hyper after the fast-path loop consumed (read) some bytes from it. It
/// replays those `prefix` bytes — the unprocessed request the loop could not serve, plus anything it
/// read past that request — before reading from the live socket, so hyper sees the byte stream
/// exactly as the client sent it. Writes go straight to the socket. The fast path only ever consumes
/// COMPLETE bodyless GET heads itself, so the prefix is always a clean request boundary for hyper.
pub struct Rewind {
    prefix: Vec<u8>,
    pos: usize,
    inner: TcpStream,
}

impl Rewind {
    fn new(prefix: Vec<u8>, inner: TcpStream) -> Self {
        Self {
            prefix,
            pos: 0,
            inner,
        }
    }
}

impl AsyncRead for Rewind {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.pos < self.prefix.len() {
            let n = (self.prefix.len() - self.pos).min(buf.remaining());
            let start = self.pos;
            buf.put_slice(&self.prefix[start..start + n]);
            self.pos += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Rewind {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, data)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Hand the connection to hyper, replaying the bytes already consumed (`consumed`) so hyper sees the
/// request from its start. Records *why* the fast path declined so the engage-vs-fallback ratio (and
/// its reasons) stay observable; `reason` is a fixed, low-cardinality label, never request-derived.
fn fallback(stream: TcpStream, consumed: Vec<u8>, reason: &'static str) -> Fast {
    metrics::counter!("cairn_sendfile_fallback_total", "reason" => reason).increment(1);
    Fast::Fallback {
        stream: Rewind::new(consumed, stream),
    }
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
/// bodyless `GET`. A ranged GET passes this header gate too — [`cairn_protocol::S3Service::handle`]
/// zero-copies a single resolved byte-range (a 206, `content-range` relayed) exactly like a full
/// object, while a multi-range/unsatisfiable range or a compressed/encrypted object yields a
/// non-`ZeroCopy` body that falls back. Conditional GETs (so a 304/412 short-circuit is never
/// sendfile'd), request bodies, and protocol upgrades are not eligible. Object/transport/at-rest
/// eligibility (uncompressed, unencrypted, plaintext, HTTP/1.1) is enforced elsewhere: the body type
/// (`S3Body::ZeroCopy` is only produced for an uncompressed+unencrypted object — full or single
/// range), the plaintext-only call site, and the HTTP/1.1-only [`parse_head`].
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

/// Build the HTTP/1.1 response head for the fast path. `sendfile` writes the body; the response
/// carries an exact `content-length`, so the client knows where the body ends and the connection can
/// be reused. We emit `connection: keep-alive` to keep it open (looping for the next request) or
/// `connection: close` to close after, per `keep_alive` (which mirrors the request's intent). Any
/// inbound `connection` header is dropped and replaced. `x-amz-request-id` is appended for parity
/// with the hyper path (which adds it in `server::handle`, a layer the fast path bypasses); any value
/// already present is replaced. The status's canonical reason phrase makes the status line correct
/// for both a full-object `200` and a single-range `206 Partial Content`.
fn format_head(
    status: hyper::StatusCode,
    headers: &[(String, String)],
    request_id: &str,
    keep_alive: bool,
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
    out.push_str(if keep_alive {
        "\r\nconnection: keep-alive\r\n\r\n"
    } else {
        "\r\nconnection: close\r\n\r\n"
    });
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

/// The outcome of reading one request head off the connection (consuming).
enum HeadOutcome {
    /// A complete head terminated by `CRLFCRLF`; the value is the byte index just past it in `buf`.
    Complete(usize),
    /// The peer closed cleanly with no (further) request.
    Eof,
    /// The head was unterminated and hit the size cap, or a read error occurred.
    Incomplete,
}

/// Read bytes from `stream` into `buf` (which may already hold carried-over pipelined bytes) until a
/// complete HTTP head terminator is seen, the size cap is hit, or the peer closes. Unlike the old
/// peek, this CONSUMES the bytes, so after a keep-alive response the connection is positioned at the
/// next request. Bytes read past the head stay in `buf` for the caller to carry or replay.
async fn read_head(stream: &mut TcpStream, buf: &mut Vec<u8>) -> HeadOutcome {
    if let Some(e) = head_end(buf) {
        return HeadOutcome::Complete(e); // a pipelined head already sits in the carried bytes
    }
    let mut chunk = [0u8; 8192];
    loop {
        if buf.len() >= MAX_HEAD {
            return HeadOutcome::Incomplete; // head larger than we will buffer; let hyper handle it
        }
        match stream.read(&mut chunk).await {
            Ok(0) => return HeadOutcome::Eof,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(e) = head_end(buf) {
                    return HeadOutcome::Complete(e);
                }
            }
            Err(_) => return HeadOutcome::Incomplete,
        }
    }
}

/// Run the plaintext `sendfile` keep-alive loop on a connection. See the module docs. Serves eligible
/// GETs back-to-back via `sendfile`; the first request it cannot serve (or a clean close) ends the
/// loop — either returning [`Fast::Handled`] (closed) or [`Fast::Fallback`] (handed to hyper, with
/// the bytes already consumed replayed).
pub async fn try_sendfile_get(
    mut stream: TcpStream,
    stack: &AppStack,
    peer: SocketAddr,
    request_metrics_enabled: bool,
    min_bytes: u64,
) -> Fast {
    // Bytes read past one request's head (a pipelined next request) are carried into the next
    // iteration. The fast path only ever fully consumes bodyless GET heads, so `carry` is always a
    // clean request boundary.
    let mut carry: Vec<u8> = Vec::new();
    loop {
        let start = std::time::Instant::now();
        let mut buf = std::mem::take(&mut carry);

        // 1) Read a complete request head (consuming), bounded by HEAD_TIMEOUT — which also serves as
        //    the keep-alive idle timeout between requests on a reused connection.
        let head_len =
            match tokio::time::timeout(HEAD_TIMEOUT, read_head(&mut stream, &mut buf)).await {
                Ok(HeadOutcome::Complete(e)) => e,
                // A clean close (or idle timeout) with nothing buffered: we are done with this connection.
                Ok(HeadOutcome::Eof) if buf.is_empty() => return Fast::Handled,
                Err(_) if buf.is_empty() => return Fast::Handled,
                // A partial/oversize head, a read error, or a timeout with bytes already read: replay what
                // we have to hyper, which renders the proper error or frames the request correctly.
                Ok(HeadOutcome::Eof) | Ok(HeadOutcome::Incomplete) | Err(_) => {
                    return fallback(stream, buf, "head");
                }
            };

        let Some(head) = parse_head(&buf[..head_len]) else {
            return fallback(stream, buf, "parse");
        };
        // See [`head_eligible`] for the single source of truth on header-level eligibility.
        if !head_eligible(&head) {
            return fallback(stream, buf, "ineligible");
        }

        let (path, query) = match head.target.split_once('?') {
            Some((p, q)) => (p.to_owned(), q.to_owned()),
            None => (head.target.clone(), String::new()),
        };
        // An unparseable bucket/key is not a fast-path GET; hand the buffered head to hyper, which
        // renders the proper 400 through the normal adapter path.
        let Ok((bucket, key)) = route_path(&path) else {
            return fallback(stream, buf, "not_object");
        };
        if bucket.is_none() || key.is_none() {
            return fallback(stream, buf, "not_object");
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
                AuthOutcome::Denied(_) => return fallback(stream, buf, "denied"),
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

        // Authorize + resolve through the normal service. Only a zero-copy-eligible body is
        // fast-pathed; anything else (an error, a compressed/encrypted object, a multi-range) replays
        // to hyper.
        let empty: cairn_types::BodyStream = Box::pin(futures_util::stream::empty());
        let resp = stack.s3.handle(s3req, empty).await;
        let status = resp.status;
        let S3Body::ZeroCopy {
            zero_copy, length, ..
        } = resp.body
        else {
            return fallback(stream, buf, "not_zerocopy");
        };

        // Size floor (`CAIRN_FASTIO_MIN_BYTES`): below it, a GET is cheaper on the normal streamed
        // path. `min_bytes == 0` disables the floor.
        if length < min_bytes {
            return fallback(stream, buf, "below_floor");
        }

        // Keep the connection alive unless the client asked to close (HTTP/1.1 defaults to
        // keep-alive, and `parse_head` already requires HTTP/1.1). The response carries an exact
        // content-length, so the client frames the body without chunking and can reuse the socket.
        let keep_alive = !head
            .headers
            .iter()
            .any(|(k, v)| k == "connection" && v.eq_ignore_ascii_case("close"));
        // Bytes read past this (bodyless) GET head are the start of the next pipelined request; carry
        // them to the next iteration when we keep the connection alive.
        let leftover = buf[head_len..].to_vec();
        let resp_bytes = length;
        let out = format_head(status, &resp.headers, &request_id, keep_alive);

        // Write the head + `sendfile` on a blocking thread (the socket must be blocking for
        // `sendfile`). On keep-alive, return the socket — switched back to non-blocking — so the loop
        // can re-register it with tokio and read the next request; on close, do the orderly
        // half-close + drain here so the final close is a FIN, not a body-truncating RST.
        let std_stream = match stream.into_std() {
            Ok(s) => s,
            Err(_) => return Fast::Handled,
        };
        let blocking =
            tokio::task::spawn_blocking(move || -> std::io::Result<Option<std::net::TcpStream>> {
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
                if keep_alive {
                    s.set_nonblocking(true)?;
                    Ok(Some(s))
                } else {
                    // Orderly close (audit #23): half-close (FIN), then drain any pending client
                    // bytes under a bounded timeout so the final close is a FIN rather than a RST that
                    // could discard the response still in flight.
                    let _ = s.shutdown(std::net::Shutdown::Write);
                    s.set_read_timeout(Some(FAST_LINGER))?;
                    let mut scratch = [0u8; 2048];
                    loop {
                        match s.read(&mut scratch) {
                            Ok(0) => break,
                            Ok(_) => continue,
                            Err(_) => break,
                        }
                    }
                    Ok(None)
                }
            })
            .await;

        let (reused, ok) = match blocking {
            Ok(Ok(reused)) => (reused, true),
            Ok(Err(e)) => {
                tracing::debug!(error = %e, "sendfile fast path failed mid-write");
                (None, false)
            }
            Err(e) => {
                tracing::debug!(error = %e, "sendfile blocking task panicked");
                (None, false)
            }
        };
        metrics::counter!(
            "cairn_sendfile_get_total",
            "result" => if ok { "ok" } else { "error" },
            "transport" => "plain",
        )
        .increment(1);
        // Parity with `server::handle` (ARCH 26): the fast path bypasses that middleware, so emit the
        // same request/throughput metrics and usage-analytics ingestion here.
        record_request_metrics(
            stack,
            request_metrics_enabled,
            &path,
            &query,
            status,
            resp_bytes,
            start.elapsed(),
        );

        match reused {
            Some(s) if ok => match TcpStream::from_std(s) {
                // Keep-alive: serve the next request on the same connection (with any pipelined bytes).
                Ok(ns) => {
                    stream = ns;
                    carry = leftover;
                }
                Err(_) => return Fast::Handled,
            },
            // Closed (the client asked, the transfer errored, or we could not re-register the socket).
            _ => return Fast::Handled,
        }
    }
}

/// Emit the per-request metrics + usage-analytics ingestion a fast-pathed GET would otherwise miss by
/// bypassing `server::handle` (ARCH 26). The response was authorized and its status/size are known
/// regardless of whether the body transfer later truncated — the hyper path likewise counts at the
/// declared header level rather than on body completion.
fn record_request_metrics(
    stack: &AppStack,
    request_metrics_enabled: bool,
    path: &str,
    query: &str,
    status: hyper::StatusCode,
    resp_bytes: u64,
    elapsed: Duration,
) {
    let status_code = status.as_u16();
    let route = crate::server::classify_route(path);
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
            // The sendfile fast path is data-plane only (S3 object GET), never the console listener.
            crate::server::classify_operation(false, &hyper::Method::GET, path, query)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    fn format_head_keep_alive_and_close_variants() {
        let headers = vec![
            ("content-length".to_owned(), "42".to_owned()),
            ("etag".to_owned(), "\"abc\"".to_owned()),
            // An inbound connection header must be dropped and replaced by our framing decision.
            ("connection".to_owned(), "close".to_owned()),
            // A stale request-id must be replaced, not duplicated.
            ("x-amz-request-id".to_owned(), "stale".to_owned()),
        ];
        // keep-alive variant: the connection stays open for the next request.
        let ka = format_head(hyper::StatusCode::OK, &headers, "req-123", true);
        assert!(ka.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(ka.contains("content-length: 42\r\n"));
        assert!(ka.contains("etag: \"abc\"\r\n"));
        assert!(ka.ends_with("\r\n\r\n"));
        assert_eq!(ka.matches("x-amz-request-id:").count(), 1);
        assert!(ka.contains("x-amz-request-id: req-123\r\n"));
        assert!(!ka.contains("stale"));
        assert!(ka.contains("connection: keep-alive\r\n"));
        assert_eq!(ka.to_ascii_lowercase().matches("connection:").count(), 1);
        // close variant.
        let close = format_head(hyper::StatusCode::OK, &headers, "r", false);
        assert!(close.contains("connection: close\r\n"));
        assert!(!close.contains("keep-alive"));
    }

    #[test]
    fn format_head_uses_the_canonical_206_reason_for_ranged_responses() {
        let headers = vec![("content-range".to_owned(), "bytes 0-9/100".to_owned())];
        let out = format_head(hyper::StatusCode::PARTIAL_CONTENT, &headers, "r", true);
        assert!(out.starts_with("HTTP/1.1 206 Partial Content\r\n"));
        assert!(out.contains("content-range: bytes 0-9/100\r\n"));
    }

    /// The rewind handoff replays the already-consumed prefix bytes first, then the live socket, so
    /// hyper sees the request stream exactly as the client sent it — even across small reads.
    #[tokio::test]
    async fn rewind_replays_prefix_then_socket_bytes() {
        // A real socket pair so `Rewind` wraps an actual `TcpStream` (its inner type).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        // The client writes the "socket" portion; the prefix is the part the fast path already read.
        let mut client = client;
        client.write_all(b" world from the socket").await.unwrap();
        client.flush().await.unwrap();

        let mut rw = Rewind::new(b"hello".to_vec(), server);
        let mut got = vec![0u8; b"hello world from the socket".len()];
        rw.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"hello world from the socket");
    }

    #[tokio::test]
    async fn read_head_consumes_a_complete_head_and_keeps_the_remainder() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();

        // Two pipelined heads in one write; read_head must stop exactly at the first terminator.
        client
            .write_all(b"GET /a HTTP/1.1\r\nHost: h\r\n\r\nGET /b HTTP/1.1\r\nHost: h\r\n\r\n")
            .await
            .unwrap();
        client.flush().await.unwrap();

        let mut buf = Vec::new();
        let e = match read_head(&mut server, &mut buf).await {
            HeadOutcome::Complete(e) => e,
            _ => panic!("expected a complete head"),
        };
        // The first head ends at the first CRLFCRLF; the second head is carried in the remainder.
        assert_eq!(&buf[..e], b"GET /a HTTP/1.1\r\nHost: h\r\n\r\n");
        assert!(buf[e..].starts_with(b"GET /b HTTP/1.1"));
    }
}
