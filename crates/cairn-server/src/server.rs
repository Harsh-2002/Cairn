//! The HTTP serving loop, the outer middleware, and ordered graceful shutdown. In the
//! skeleton the router answers liveness, readiness, and metrics; later waves route the S3 and
//! management families here behind authentication and authorization.

use crate::adapter;
use crate::adapter::{ResponseBody, full_body};
use crate::config::Config;
use crate::stack::AppStack;
use bytes::Bytes;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use metrics_exporter_prometheus::PrometheusHandle;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tracing::Instrument;

/// Shared, cheaply-cloneable server state.
struct AppState {
    /// Readiness gate: false until migrations + reconciliation have completed.
    ready: AtomicBool,
    /// Global in-flight concurrency limiter.
    concurrency: Semaphore,
    /// Per-request timeout.
    request_timeout: Duration,
    /// Maximum time allowed to read a connection's complete request head (slowloris guard).
    header_read_timeout: Duration,
    /// Caps the number of concurrent TCP connections per listener; a connection past the cap is
    /// dropped so idle/slow sockets can't exhaust FDs ahead of the concurrency limiter.
    connection_limiter: Arc<Semaphore>,
    /// The Prometheus render handle.
    metrics: PrometheusHandle,
    /// Whether the request-metrics usage-analytics subsystem is enabled (`CAIRN_REQUEST_METRICS_*`,
    /// ARCH 26.5). When off, no per-request counters accumulate on the hot path.
    request_metrics_enabled: bool,
    /// Minimum GET-response size for the `sendfile` fast path (`CAIRN_FASTIO_MIN_BYTES`). Only read
    /// in a `fast-io` build; allowed to be dead in the default build where the fast path is cfg'd out.
    #[cfg_attr(not(all(feature = "fast-io", target_os = "linux")), allow(dead_code))]
    fastio_min_bytes: u64,
    /// The assembled S3/engine stack.
    stack: Arc<AppStack>,
}

/// Run the server until a shutdown signal is received, then drain in-flight work.
///
/// # Errors
/// Returns an I/O error if the listener cannot bind.
pub async fn serve(
    config: Config,
    metrics: PrometheusHandle,
    stack: Arc<AppStack>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    let local = listener.local_addr()?;
    // The web-console listener is a second, optional socket. It serves the same stack but additionally
    // serves the management console at the root path, so an operator can firewall it off from the
    // S3 data-plane port. `None` (CAIRN_WEB_ADDR empty/off) runs headless with only the S3 listener.
    let web_listener = match config.web_listen_addr().ok().flatten() {
        Some(addr) => Some(TcpListener::bind(addr).await?),
        None => None,
    };
    let web_local = web_listener.as_ref().and_then(|l| l.local_addr().ok());
    let state = Arc::new(AppState {
        ready: AtomicBool::new(false),
        concurrency: Semaphore::new(config.concurrency_limit),
        request_timeout: Duration::from_secs(config.request_timeout_secs),
        header_read_timeout: Duration::from_secs(config.header_read_timeout_secs),
        connection_limiter: Arc::new(Semaphore::new(config.max_connections)),
        metrics,
        request_metrics_enabled: config.request_metrics_enabled,
        fastio_min_bytes: config.fastio_min_bytes,
        stack,
    });

    // Optional native TLS. The served config lives behind a watch channel so a SIGHUP can
    // hot-reload the certificate/key from the same paths without dropping the listener
    // (ARCH 27.2): the accept loop reads the current config per connection, and the reload
    // handler atomically publishes a new one (a bad new cert is logged and the old config kept).
    let tls_rx = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert), Some(key)) => {
            let cfg = crate::tls::load_server_config(cert, key).map_err(std::io::Error::other)?;
            let (tx, rx) = watch::channel(cfg);
            tokio::spawn(reload_tls_on_sighup(tx, cert.clone(), key.clone()));
            Some(rx)
        }
        _ => None,
    };

    // Probe once whether the kernel can offload TLS record crypto (feature `fast-io`, Linux only).
    // The result gates the per-connection path: if kTLS is unavailable we never attempt the
    // offload and every TLS connection takes the unchanged userspace path. With the feature off
    // this is always `false` and the probe is a no-op.
    let ktls_ready = tls_rx.is_some() && ktls_available();
    if ktls_ready {
        tracing::info!("kTLS offload available; TLS connections will use kernel record crypto");
    }

    // Migrations and startup reconciliation already ran while building the stack; ready now.
    state.ready.store(true, Ordering::SeqCst);

    // The graceful-shutdown signal, created before the background pool so the replication workers
    // can watch it and stop claiming when shutdown begins.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(wait_for_signal(shutdown_tx));

    // Background subsystems: multipart sweeper, lifecycle scanner, WAL checkpointer, metrics, and
    // the replication worker pool (which takes the shutdown receiver).
    crate::background::spawn(state.stack.clone(), &config, shutdown_rx.clone());
    tracing::info!(s3_api = %local, web_console = ?web_local, tls = tls_rx.is_some(), "cairn listening");

    // Run the S3-API accept loop and (optionally) the web-console accept loop concurrently. The web console loop
    // sets `serve_web = true`, which makes its connections serve the console at the root path.
    let api = accept_loop(
        listener,
        state.clone(),
        tls_rx.clone(),
        ktls_ready,
        false,
        shutdown_rx.clone(),
    );
    match web_listener {
        Some(sock) => {
            let web = accept_loop(sock, state.clone(), tls_rx, ktls_ready, true, shutdown_rx);
            tokio::join!(api, web);
        }
        None => api.await,
    }
    state.ready.store(false, Ordering::SeqCst);
    tracing::info!("shutdown complete");
    Ok(())
}

/// Accept and serve connections on one listener until shutdown, then drain in-flight connections
/// within a bounded grace period. `serve_web` selects the listener's role: `true` adds the web
/// console at the root path; `false` is the pure S3 data-plane listener.
async fn accept_loop(
    listener: TcpListener,
    state: Arc<AppState>,
    tls_rx: Option<watch::Receiver<Arc<rustls::ServerConfig>>>,
    ktls_ready: bool,
    serve_web: bool,
    shutdown_rx: watch::Receiver<bool>,
) {
    let mut conns = tokio::task::JoinSet::new();
    let mut shutdown = shutdown_rx.clone();
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, peer) = match accept {
                    Ok(v) => v,
                    Err(e) => { tracing::warn!(error = %e, "accept failed"); continue; }
                };
                // Cap concurrent connections: acquire a permit held for the connection's lifetime, or
                // drop the connection immediately if we're at the cap. This bounds FD/memory use
                // against a flood of idle/slow sockets ahead of the per-request limiter (audit
                // 2026-07). A drop is counted, never silent.
                let permit = match state.connection_limiter.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        metrics::counter!("cairn_connections_rejected_total").increment(1);
                        tracing::debug!(%peer, "connection limit reached; dropping connection");
                        continue;
                    }
                };
                let st = state.clone();
                let conn_shutdown = shutdown_rx.clone();
                // Snapshot the *current* TLS config for this connection; a concurrent reload
                // affects only subsequently-accepted connections.
                let tls = tls_rx.as_ref().map(|rx| rx.borrow().clone());
                conns.spawn(async move {
                    let _permit = permit; // released when the connection task ends
                    match tls {
                        Some(cfg) => serve_tls(stream, cfg, ktls_ready, st, peer, serve_web, conn_shutdown).await,
                        None => serve_plaintext(stream, st, peer, serve_web, conn_shutdown).await,
                    }
                });
            }
            _ = shutdown.changed() => break,
        }
    }

    let drain = async { while conns.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(30), drain)
        .await
        .is_err()
    {
        conns.shutdown().await;
    }
}

/// Perform the TLS handshake for one accepted connection and serve it.
///
/// With the `fast-io` feature OFF (the default) `ktls_ready` is always `false` and this is exactly
/// the original path: handshake over the raw [`tokio::net::TcpStream`] and serve the userspace
/// [`tokio_rustls`] `TlsStream`. Nothing changes.
///
/// With `fast-io` ON on Linux and `ktls_ready` true, the socket is wrapped in [`ktls::CorkStream`]
/// before the handshake (the cork lets `ktls` drain rustls cleanly at a record boundary), and after
/// the handshake [`ktls::config_ktls_server`] extracts the negotiated traffic secrets from rustls
/// and installs them on the socket via `setsockopt(TLS_TX/TLS_RX)`. The kernel then performs the
/// symmetric record crypto and hyper serves over the resulting [`ktls::KtlsStream`] unchanged — the
/// win is CPU offload, the bytes on the wire are identical.
///
/// The always-on fallback is a *startup* decision: `ktls_ready` is the result of a one-time probe
/// (`ktls_available`). If the kernel cannot offload TLS at all, `ktls_ready` is false and every
/// connection takes the unchanged userspace path, so correctness and durability/crash semantics are
/// never affected — only where the crypto runs. A per-connection offload failure (rare, e.g. a
/// cipher the kernel build does not support) is logged; because `config_ktls_server` consumes the
/// stream while draining it, that one connection is dropped and the client retries, rather than
/// risking a half-drained userspace continuation.
async fn serve_tls(
    stream: tokio::net::TcpStream,
    cfg: Arc<rustls::ServerConfig>,
    ktls_ready: bool,
    state: Arc<AppState>,
    peer: std::net::SocketAddr,
    serve_web: bool,
    conn_shutdown: watch::Receiver<bool>,
) {
    // Console courtesy: on the web-console listener, a browser that connects in plaintext to the TLS port
    // gets a `308` to the `https://` URL rather than an opaque handshake failure. Peek the first byte
    // WITHOUT consuming it — a TLS ClientHello is a handshake record (`0x16`); any other first byte is
    // a plaintext HTTP request (`G`/`P`/… are all != 0x16). The S3 data-plane listener
    // (`serve_web == false`) deliberately skips this and stays TLS-only: redirecting a SigV4 request
    // would require first accepting its `Authorization`/presigned credentials over cleartext.
    if serve_web {
        // Bound the wait for the first byte: a client that connects and never sends one must not pin
        // this task (an unauthenticated slow-loris). A genuine TLS or HTTP client sends immediately,
        // so a short cap is invisible to real traffic and drops idle/hostile sockets.
        let mut first = [0u8; 1];
        match tokio::time::timeout(
            Duration::from_secs(PEEK_TIMEOUT_SECS),
            stream.peek(&mut first),
        )
        .await
        {
            Ok(Ok(n)) if n >= 1 && first[0] != TLS_HANDSHAKE_RECORD => {
                let fallback_host = stream
                    .local_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_default();
                redirect_plaintext_to_https(stream, fallback_host).await;
                return;
            }
            // A TLS ClientHello (0x16) or EOF — the peek consumed nothing, so the handshake sees it
            // whole. Fall through to the acceptor.
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::debug!(%peer, error = %e, "console listener peek failed");
                return;
            }
            Err(_) => {
                tracing::debug!(%peer, "console listener peek timed out; dropping idle connection");
                return;
            }
        }
    }

    let acceptor = tokio_rustls::TlsAcceptor::from(cfg);

    #[cfg(all(feature = "fast-io", target_os = "linux"))]
    if ktls_ready {
        let corked = ktls::CorkStream::new(stream);
        match acceptor.accept(corked).await {
            Ok(tls) => match ktls::config_ktls_server(tls).await {
                Ok(ktls_stream) => {
                    metrics::counter!("cairn_ktls_offload_total", "result" => "ok").increment(1);
                    tracing::debug!(%peer, "kTLS offload engaged");
                    serve_io(ktls_stream, state, peer, true, serve_web, conn_shutdown).await;
                }
                Err(e) => {
                    metrics::counter!("cairn_ktls_offload_total", "result" => "error").increment(1);
                    tracing::debug!(%peer, error = %e, "kTLS offload failed mid-connection");
                }
            },
            Err(e) => tracing::debug!(error = %e, "TLS handshake failed"),
        }
        return;
    }

    // Userspace path (feature off, non-Linux, or kTLS unavailable): the original behaviour.
    let _ = ktls_ready;
    match acceptor.accept(stream).await {
        Ok(tls) => serve_io(tls, state, peer, true, serve_web, conn_shutdown).await,
        Err(e) => tracing::debug!(error = %e, "TLS handshake failed"),
    }
}

/// TLS record ContentType for a handshake record — the first byte of a ClientHello. Any other first
/// byte on the console listener is a plaintext HTTP request we redirect to `https://`.
const TLS_HANDSHAKE_RECORD: u8 = 0x16;

/// How long to wait for the first byte on a console connection before giving up. A real TLS or HTTP
/// client sends immediately; a connection that sends nothing is idle or hostile and is dropped so it
/// cannot pin the accept task (an unauthenticated slow-loris vector).
const PEEK_TIMEOUT_SECS: u64 = 5;

/// Total deadline for reading the plaintext request head before we answer with the redirect. A bound
/// on the *whole* read — not per-read — so a client dribbling one byte at a time cannot hold the task
/// open indefinitely. We redirect from whatever head arrived before the deadline.
const REDIRECT_HEAD_TIMEOUT_SECS: u64 = 5;

/// Read the plaintext HTTP request head off a console connection that reached the TLS port and reply
/// with `308 Permanent Redirect` to the `https://` equivalent, then close. Bounded by size and a
/// total read deadline so a slow or hostile client cannot pin the task; `308` (not `301`) preserves
/// the method + body so a non-GET retries correctly over TLS.
async fn redirect_plaintext_to_https<S>(mut stream: S, fallback_host: String)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut head = Vec::with_capacity(1024);
    // One deadline for the entire head read, so a byte-at-a-time dribble cannot extend it: a per-read
    // timeout would reset on every trickled byte and never fire. We redirect from whatever arrived.
    let read_head = async {
        let mut chunk = [0u8; 1024];
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    head.extend_from_slice(&chunk[..n]);
                    // Stop once the head is complete, or it grows past anything a request line + Host
                    // needs — we never read the body (we are redirecting, not serving the request).
                    if head.windows(4).any(|w| w == b"\r\n\r\n") || head.len() >= 8192 {
                        break;
                    }
                }
                Err(_) => break, // read error: respond with whatever we have (likely a "/" redirect)
            }
        }
    };
    // Timeout is non-fatal: on expiry we still answer from the partial head we collected.
    let _ = tokio::time::timeout(Duration::from_secs(REDIRECT_HEAD_TIMEOUT_SECS), read_head).await;
    let resp = build_https_redirect(&head, &fallback_host);
    let _ = stream.write_all(resp.as_bytes()).await;
    let _ = stream.flush().await;
}

/// Build the response for a plaintext request that hit the TLS console port. Parses the request target
/// and `Host` from the (possibly partial) head: when a usable host resolves (the request's `Host` or,
/// failing that, `fallback_host`) it is a `308 Permanent Redirect` to the `https://` equivalent; with
/// no usable host at all it is a `400 Bad Request` rather than a malformed `https:///` Location. Target
/// and host are sanitised so a hostile request cannot inject header lines or a non-`https` scheme.
fn build_https_redirect(head: &[u8], fallback_host: &str) -> String {
    let text = String::from_utf8_lossy(head);
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    // Request target = the second token of "METHOD target HTTP/x"; must be an absolute path.
    let target = request_line
        .split(' ')
        .nth(1)
        .filter(|t| is_safe_target(t))
        .unwrap_or("/");
    // First sane `Host:` header value, else the fallback (local socket addr) when it too is sane.
    let host = lines
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            if k.trim().eq_ignore_ascii_case("host") {
                Some(v.trim())
            } else {
                None
            }
        })
        .filter(|h| is_safe_host(h))
        .or_else(|| is_safe_host(fallback_host).then_some(fallback_host));
    match host {
        Some(host) => format!(
            "HTTP/1.1 308 Permanent Redirect\r\n\
             Location: https://{host}{target}\r\n\
             Content-Length: 0\r\n\
             Connection: close\r\n\r\n"
        ),
        // No host we can trust to build an absolute `https://` URL — fail rather than emit `https:///`.
        None => {
            "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
        }
    }
}

/// A request target safe to echo into a `Location` header: an absolute path of printable, non-space
/// ASCII — so it cannot contain CR/LF, spaces, or control bytes that would split the header.
fn is_safe_target(t: &str) -> bool {
    t.starts_with('/') && t.bytes().all(|b| b.is_ascii_graphic())
}

/// A `Host` value safe to echo into a `Location` header: a non-empty hostname/IP[:port] of the
/// permitted charset only (alphanumerics, `.`, `-`, `:`, and `[` `]` for IPv6 literals).
fn is_safe_host(h: &str) -> bool {
    !h.is_empty()
        && h.len() <= 255
        && h.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'[' | b']'))
}

/// One-time probe of whether the kernel can offload TLS record crypto (feature `fast-io`, Linux).
///
/// kTLS needs the `tls` kernel ULP (upper-layer protocol). We test for it the cheapest correct way:
/// open a throwaway TCP socket and try `setsockopt(SOL_TCP, TCP_ULP, "tls")`. Success means the
/// machinery is present and the per-connection offload can be attempted; any failure (module not
/// loaded, container without the capability, older kernel) means we never try and every connection
/// uses the userspace path. The socket is closed immediately. With the feature off this is a
/// compile-time `false`.
fn ktls_available() -> bool {
    #[cfg(all(feature = "fast-io", target_os = "linux"))]
    {
        crate::sendfile::probe_tcp_ulp_tls()
    }
    #[cfg(not(all(feature = "fast-io", target_os = "linux")))]
    {
        false
    }
}

/// Serve one accepted connection (plaintext or TLS) with graceful shutdown.
/// Serve a plaintext (non-TLS) connection. With `fast-io` on Linux, the first request is offered to
/// the `sendfile` fast path ([`crate::fast_get`]): a qualifying object GET is served file→socket with
/// no userspace copy and the connection closes; anything else is replayed to hyper unchanged. With
/// `fast-io` off (the default) this is exactly the original path — hyper serves the raw socket.
async fn serve_plaintext(
    stream: tokio::net::TcpStream,
    state: Arc<AppState>,
    peer: std::net::SocketAddr,
    serve_web: bool,
    conn_shutdown: watch::Receiver<bool>,
) {
    // The sendfile fast path runs only on the S3 data-plane listener: the web console listener serves console
    // assets at paths that must be matched before S3 routing, so it always goes straight to hyper.
    #[cfg(all(feature = "fast-io", target_os = "linux"))]
    if !serve_web {
        match crate::fast_get::try_sendfile_get(
            stream,
            state.stack.as_ref(),
            peer,
            state.request_metrics_enabled,
            state.fastio_min_bytes,
        )
        .await
        {
            crate::fast_get::Fast::Handled => {}
            crate::fast_get::Fast::Fallback { stream } => {
                serve_io(stream, state, peer, false, serve_web, conn_shutdown).await;
            }
        }
        return;
    }
    serve_io(stream, state, peer, false, serve_web, conn_shutdown).await;
}

async fn serve_io<S>(
    stream: S,
    state: Arc<AppState>,
    peer: std::net::SocketAddr,
    secure: bool,
    serve_web: bool,
    mut conn_shutdown: watch::Receiver<bool>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let io = TokioIo::new(stream);
    let svc_shutdown = conn_shutdown.clone();
    // Capture before `state` is moved into the service closure (Duration is Copy).
    let header_read_timeout = state.header_read_timeout;
    let svc = service_fn(move |req| {
        handle(
            state.clone(),
            peer,
            secure,
            serve_web,
            req,
            svc_shutdown.clone(),
        )
    });
    let mut builder = auto::Builder::new(TokioExecutor::new());
    // Install a timer and a header-read timeout so a connection that dribbles or never finishes its
    // request head is dropped instead of pinning a task/FD forever (slowloris; audit 2026-07). The
    // per-request timeout only starts after the head is parsed, so this is the only bound on the
    // head-read phase. `header_read_timeout` requires the timer to be set (else it panics).
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(header_read_timeout);
    let conn = builder.serve_connection(io, svc);
    tokio::pin!(conn);
    tokio::select! {
        res = conn.as_mut() => {
            if let Err(e) = res { tracing::debug!(error = %e, "connection ended"); }
        }
        _ = conn_shutdown.changed() => {
            conn.as_mut().graceful_shutdown();
            let _ = conn.await;
        }
    }
}

/// Mint a per-request correlation id without per-request randomness. A request id only needs to be
/// unique (it correlates logs/headers, it is not a security token), so it is a one-time random
/// 64-bit process salt — drawn once at first use — concatenated with a monotonic atomic counter,
/// hex-encoded to the same 32-char width as the previous UUIDv4. This drops the per-request RNG
/// draw and string re-parse from the hot path while keeping ids collision-free across processes
/// and restarts (distinct salts) and within a process (distinct counters).
fn next_request_id() -> String {
    static SALT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let salt = *SALT.get_or_init(|| {
        // One UUIDv4 at startup seeds the salt — no new dependency, no per-request RNG.
        let b = *uuid::Uuid::new_v4().as_bytes();
        u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
    });
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{salt:016x}{seq:016x}")
}

/// Redact the object-share token from a path before it is logged. `GET /share/{token}` carries a 256-bit
/// revocable capability in the path, and the request span is recorded at info level — anyone with
/// access-log access (a broader, less-trusted audience than DB/filesystem access) could otherwise
/// extract and replay it until revoked (audit 2026-07). Correlation is preserved via the request_id.
fn redact_log_path(path: &str) -> &str {
    if path.starts_with("/share/") {
        "/share/<redacted>"
    } else {
        path
    }
}

/// The outer middleware: request id, tracing span, concurrency limit, timeout, and the
/// request/latency metrics, wrapping the router.
async fn handle(
    state: Arc<AppState>,
    peer: std::net::SocketAddr,
    secure: bool,
    serve_web: bool,
    req: Request<Incoming>,
    shutdown: watch::Receiver<bool>,
) -> Result<Response<ResponseBody>, Infallible> {
    let request_id = next_request_id();
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    // Capture the raw query before `req` is consumed by the router: the request-metrics operation
    // classifier needs it to distinguish e.g. `?uploads`/`?partNumber`/`?list-type` sub-resources.
    let query = req.uri().query().unwrap_or("").to_owned();
    // Approximate inbound payload size from the declared content-length (the body itself is streamed
    // and never fully buffered here, so the header is the cheapest available proxy).
    let req_bytes = content_length(req.headers());
    // Redact the share token from logs (see `redact_log_path`).
    let log_path = redact_log_path(&path);
    let span = tracing::info_span!(
        "request",
        request_id = %request_id,
        method = %method,
        path = %log_path,
        %peer,
    );

    let infra =
        method == Method::GET && matches!(path.as_str(), "/healthz" | "/readyz" | "/metrics");

    let response = async move {
        // Infra endpoints (`/healthz`, `/readyz`, `/metrics`) must answer even when the server is
        // shedding load, so a liveness/readiness probe or scrape never trips the concurrency
        // limiter and flaps the instance out of rotation (audit #21). Only real S3/web console work takes a
        // permit; the guard is held for the whole request via the `Option`.
        let _permit = if infra {
            None
        } else {
            match state.concurrency.try_acquire() {
                Ok(p) => Some(p),
                Err(_) => {
                    return error_response(StatusCode::SERVICE_UNAVAILABLE, "TooManyRequests");
                }
            }
        };
        let start = Instant::now();
        let work = async {
            if infra {
                route_infra(&state, &path).await
            } else {
                adapter::handle(
                    state.stack.clone(),
                    req,
                    peer.ip(),
                    secure,
                    serve_web,
                    request_id.clone(),
                    shutdown.clone(),
                )
                .await
            }
        };
        let mut resp = match tokio::time::timeout(state.request_timeout, work).await {
            Ok(r) => r,
            Err(_) => error_response(StatusCode::SERVICE_UNAVAILABLE, "RequestTimeout"),
        };
        let status = resp.status();
        let elapsed_dur = start.elapsed();
        let elapsed = elapsed_dur.as_secs_f64();
        // A low-cardinality `route` label (ARCH 26): the request is bucketed into a small fixed set
        // of route classes rather than the raw path, so the time series stay bounded.
        let route = classify_route(&path);
        metrics::counter!(
            "cairn_requests_total",
            "method" => method.to_string(),
            "status" => status.as_u16().to_string(),
            "route" => route,
        )
        .increment(1);
        metrics::histogram!(
            "cairn_request_duration_seconds",
            "method" => method.to_string(),
            "route" => route,
        )
        .record(elapsed);
        // Throughput counters (ARCH 26). Sizes are taken from the content-length declarations, the
        // only bounded-cost proxy at this layer (bodies stream past without being buffered).
        if req_bytes > 0 {
            metrics::counter!("cairn_bytes_received_total").increment(req_bytes);
        }
        let resp_bytes = content_length(resp.headers());
        if resp_bytes > 0 {
            metrics::counter!("cairn_bytes_sent_total").increment(resp_bytes);
        }
        // Usage-analytics ingestion (ARCH 26.5): count this completed request into the in-process
        // aggregator. This is a single sharded hashmap bump — zero DB I/O on the hot path; the
        // background flush loop drains it periodically. Gated on the subsystem being enabled, and
        // skipped for infra/web console/share/root paths the classifier returns `None` for.
        if state.request_metrics_enabled {
            // A successful bucket deletion — whether through the raw S3 path (`DELETE /{bucket}`) or
            // the management console/CLI (`DELETE /api/v1/buckets/{name}`) — removes the bucket and
            // its persisted analytics (cleared in the delete's own metadata commit). Evict the
            // bucket's not-yet-flushed in-memory counts too, or pending per-bucket counts from prior
            // S3 traffic (reads, the deletes that emptied it) would flush after the delete and
            // resurrect a per-bucket series. Both delete paths reach this shared handler, so one
            // check here covers console, CLI, bulk, and S3 uniformly.
            if status.is_success() {
                if let Some(deleted) = deleted_bucket_label(&method, &path) {
                    state.stack.request_metrics.forget_bucket(deleted);
                }
            }
            if let Some((op, mut bucket)) = classify_operation(serve_web, &method, &path, &query) {
                // The raw S3 DeleteBucket request itself: attribute it to the non-bucket sentinel so
                // it does not re-create a per-bucket row for the bucket just deleted. (The management
                // delete is already classified as Management/"".)
                if op == "DeleteBucket" && status.is_success() {
                    bucket.clear();
                }
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs() as i64);
                // Latency and byte counts reuse the exact values fed to the Prometheus
                // throughput/duration metrics above so the two views agree.
                let latency_ms = elapsed_dur.as_millis() as u64;
                state.stack.request_metrics.record(
                    &op,
                    &bucket,
                    status.as_u16(),
                    latency_ms,
                    req_bytes,
                    resp_bytes,
                    now_secs,
                );
            }
        }
        tracing::info!(
            status = status.as_u16(),
            elapsed_ms = elapsed * 1000.0,
            "handled"
        );
        if let Ok(v) = request_id.parse() {
            resp.headers_mut().insert("x-amz-request-id", v);
        }
        resp
    }
    .instrument(span)
    .await;

    Ok(response)
}

/// Read a `content-length` header as a byte count, or `0` when absent/unparseable. Used as the
/// bounded-cost proxy for the throughput counters (the bodies themselves stream past unbuffered).
fn content_length(headers: &hyper::HeaderMap) -> u64 {
    headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Bucket a request path into a small, fixed set of low-cardinality route classes for the metrics
/// `route` label (ARCH 26). The raw path (which embeds bucket/key names) would explode the time
/// series, so it is collapsed to a coarse family: the infra endpoints by name, the management API,
/// the web console assets, the signed share path, and otherwise the S3 data plane.
pub(crate) fn classify_route(path: &str) -> &'static str {
    match path {
        "/healthz" => "healthz",
        "/readyz" => "readyz",
        "/metrics" => "metrics",
        "/" => "web",
        _ if path.starts_with("/api/v1") => "api",
        _ if path.starts_with("/share/") => "share",
        _ if path.starts_with("/assets/") => "web",
        _ => "s3",
    }
}

/// Classify a completed request into a `(operation, bucket)` pair for usage-analytics ingestion
/// (ARCH 26.5), or `None` for paths that should not be counted.
///
/// `None` is returned for the infra endpoints (`/healthz`, `/readyz`, `/metrics`), the web console
/// and its assets, the signed-share redeem path (`/share/…`), and the bare root (`/`) — none of which
/// are an S3 or management API operation worth charting. Management API calls (`/api/v1/…`) collapse
/// to a single `Management` operation with no bucket. Everything else is treated as path-style S3
/// addressing: the first path segment is the bucket and the method + sub-resource query select the
/// S3 operation name. Virtual-host attribution is out of scope, so a request whose bucket cannot be
/// read from the path falls back to an empty bucket string.
///
/// `serve_web` is the listener role: on the web-console listener everything that is not `/api/v1` is
/// the SPA shell or a root-served static asset (e.g. `/favicon.svg`), not S3 traffic (which uses the
/// data-plane listener), so it is not charted — otherwise a console asset shows up as a phantom
/// path-style bucket.
pub(crate) fn classify_operation(
    serve_web: bool,
    method: &Method,
    path: &str,
    query: &str,
) -> Option<(String, String)> {
    // Not-counted families. Mirror `classify_route`'s buckets so the two stay consistent.
    match path {
        "/" | "/healthz" | "/readyz" | "/metrics" => return None,
        _ => {}
    }
    if path.starts_with("/share/") || path.starts_with("/assets/") {
        return None;
    }
    if path.starts_with("/api/v1") {
        return Some(("Management".to_owned(), String::new()));
    }
    // On the web-console listener, anything that is not the management API is the SPA shell or a
    // static asset served at the root (e.g. `/favicon.svg`) — not an S3 operation. Real S3
    // data-plane traffic uses the S3 listener, so don't misclassify a console asset as a phantom
    // path-style bucket — which is exactly how `/favicon.svg` surfaced as a bucket named
    // "favicon.svg" in the usage analytics.
    if serve_web {
        return None;
    }

    // Path-style S3 addressing: `/{bucket}` or `/{bucket}/{key}`. Take the first segment as the
    // bucket label (no validation — the classifier is a cheap string match, not the router) and
    // whether a key segment follows.
    let rest = path.strip_prefix('/').unwrap_or(path);
    if rest.is_empty() {
        return None;
    }
    let (bucket_seg, key_rest) = match rest.split_once('/') {
        Some((b, k)) => (b, k),
        None => (rest, ""),
    };
    let bucket = bucket_seg.to_owned();
    let has_key = !key_rest.is_empty();

    // Cheap sub-resource probes over the raw query string.
    let has = |name: &str| {
        query.split('&').any(|p| {
            let k = p.split('=').next().unwrap_or(p);
            k.eq_ignore_ascii_case(name)
        })
    };

    let op = if has_key {
        // Object-level operations.
        match *method {
            Method::GET => "GetObject",
            Method::HEAD => "HeadObject",
            Method::PUT => {
                if has("partNumber") {
                    "UploadPart"
                } else {
                    "PutObject"
                }
            }
            Method::POST => {
                if has("uploads") {
                    "CreateMultipartUpload"
                } else if has("uploadId") {
                    "CompleteMultipartUpload"
                } else {
                    "S3"
                }
            }
            Method::DELETE => {
                if has("uploadId") {
                    "AbortMultipartUpload"
                } else {
                    "DeleteObject"
                }
            }
            _ => "S3",
        }
    } else {
        // Bucket-level operations.
        match *method {
            Method::GET | Method::HEAD => "ListObjects",
            Method::PUT => "CreateBucket",
            Method::DELETE => "DeleteBucket",
            Method::POST if has("delete") => "DeleteObjects",
            _ => "S3",
        }
    };
    Some((op.to_owned(), bucket))
}

/// The bucket a request would delete, recognised on either listener so a bucket's in-process
/// request-metrics can be evicted when it is removed (see [`RequestMetricsAgg::forget_bucket`]).
/// Both the raw S3 path-style delete (`DELETE /{bucket}`, no key) and the management console/CLI
/// delete (`DELETE /api/v1/buckets/{name}`, no sub-resource) funnel through the same `DeleteBucket`
/// mutation; this recognises both and returns the (raw, undecoded) bucket label, which matches what
/// [`classify_operation`] records per-bucket S3 traffic under. Returns `None` for object deletes,
/// sub-resource deletes, list endpoints, and infra paths. The caller must additionally require a
/// successful (2xx) response — a failed delete leaves the bucket and its metrics intact.
fn deleted_bucket_label<'a>(method: &Method, path: &'a str) -> Option<&'a str> {
    if *method != Method::DELETE {
        return None;
    }
    // Management console / CLI: `/api/v1/buckets/{name}` — the bucket itself, not `/objects`,
    // `/policy`, `/replication/...`, etc. (which carry a further `/`).
    if let Some(name) = path.strip_prefix("/api/v1/buckets/") {
        return (!name.is_empty() && !name.contains('/')).then_some(name);
    }
    // Raw S3 path-style: `/{bucket}` with no key segment. Exclude the infra endpoints.
    let seg = path.strip_prefix('/').unwrap_or(path);
    if seg.is_empty() || seg.contains('/') {
        return None;
    }
    match seg {
        "healthz" | "readyz" | "metrics" => None,
        _ => Some(seg),
    }
}

/// Liveness, readiness, and metrics endpoints (the S3 and management families are dispatched
/// through the adapter).
async fn route_infra(state: &AppState, path: &str) -> Response<ResponseBody> {
    match path {
        "/healthz" => text(StatusCode::OK, "ok"),
        "/readyz" => {
            if is_ready(state).await {
                text(StatusCode::OK, "ready")
            } else {
                text(StatusCode::SERVICE_UNAVAILABLE, "not ready")
            }
        }
        "/metrics" => {
            let body = state.metrics.render();
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/plain; version=0.0.4")
                .body(full_body(Bytes::from(body)))
                .expect("valid metrics response")
        }
        _ => error_response(StatusCode::NOT_FOUND, "NotFound"),
    }
}

/// Readiness reflects real state (ARCH 6.4, 26.4): the process is ready only once startup
/// migrations and reconciliation have completed (the `ready` gate) AND both halves of the store are
/// responsive — a trivial indexed read on the read pool (`list_buckets(None)`) AND a cheap probe of
/// the single writer (it must be draining its queue, not wedged). `/healthz` stays pure liveness;
/// this probe must not falsely report ready when either the read pool or the writer is stuck. The
/// writer probe is available only for the concrete sqlite backend; the libSQL/Turso engines
/// self-manage their writer, so for them the read probe alone gates readiness.
async fn is_ready(state: &AppState) -> bool {
    if !state.ready.load(Ordering::SeqCst) {
        return false;
    }
    if state.stack.meta.list_buckets(None).await.is_err() {
        return false;
    }
    // Every sqlite shard's writer must be responsive (one entry when unsharded; none for the
    // self-WAL-managing libSQL/Turso backends).
    for store in &state.stack.store {
        if store.writer_probe().await.is_err() {
            return false;
        }
    }
    true
}

fn text(status: StatusCode, body: &'static str) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(full_body(Bytes::from_static(body.as_bytes())))
        .expect("valid text response")
}

fn error_response(status: StatusCode, code: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(full_body(Bytes::from(code.to_owned())))
        .expect("valid error response")
}

/// Reload the TLS certificate/key on every `SIGHUP`, publishing the new config into `tls_tx` so
/// subsequently-accepted connections use the rotated certificate (ARCH 27.2). A reload failure
/// (e.g. a half-written or invalid new cert) is logged and the previously-served config is kept,
/// so a rotation mistake never takes the listener down. Each successful reload is logged.
///
/// On platforms without `SIGHUP` (non-unix) this is a no-op task.
#[cfg(unix)]
async fn reload_tls_on_sighup(
    tls_tx: watch::Sender<std::sync::Arc<rustls::ServerConfig>>,
    cert_path: std::path::PathBuf,
    key_path: std::path::PathBuf,
) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut hup = match signal(SignalKind::hangup()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "cannot install SIGHUP handler; TLS hot-reload disabled");
            return;
        }
    };
    // Stop when every accept-side receiver is gone (the server is shutting down).
    while hup.recv().await.is_some() {
        if tls_tx.is_closed() {
            return;
        }
        match crate::tls::reload_into(&tls_tx, &cert_path, &key_path) {
            Ok(_) => tracing::info!(
                cert = %cert_path.display(),
                key = %key_path.display(),
                "TLS certificate reloaded on SIGHUP"
            ),
            Err(e) => tracing::error!(
                error = %e,
                "TLS reload failed; keeping the previously-served certificate"
            ),
        }
    }
}

#[cfg(not(unix))]
async fn reload_tls_on_sighup(
    _tls_tx: watch::Sender<std::sync::Arc<rustls::ServerConfig>>,
    _cert_path: std::path::PathBuf,
    _key_path: std::path::PathBuf,
) {
}

/// Resolve on the first of SIGINT or SIGTERM, broadcasting shutdown.
async fn wait_for_signal(tx: watch::Sender<bool>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => return,
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    let _ = tx.send(true);
}

// The infra endpoints and S3 dispatch are exercised by the live smoke test and the
// cairn-protocol real-stack integration tests.

/// End-to-end coverage of the `fast-io` kTLS path. These run only with the feature on and on Linux
/// (the only platform where kTLS exists). They prove the exact serving logic of [`serve_tls`] —
/// cork-wrap, handshake, attempt the kernel offload, serve hyper on whatever stream results —
/// produces a correct HTTP/1.1 response over a real TLS connection, whether the host kernel engages
/// kTLS or the offload is unavailable and we fall back to userspace TLS. A real client driving a
/// real handshake against the actual rustls config (with secret extraction enabled) is the
/// strongest portable check available without standing up the whole stack.
#[cfg(all(test, feature = "fast-io", target_os = "linux"))]
mod fast_io_tests {
    use super::*;
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const CERT: &str = include_str!("../testdata/tls_a.crt");
    const KEY: &str = include_str!("../testdata/tls_a.key");

    /// A rustls client verifier that accepts any server certificate. The test uses a self-signed
    /// cert with no SAN, so real verification is neither possible nor the point; we are testing the
    /// kTLS serving path, not PKI.
    #[derive(Debug)]
    struct AcceptAny;

    impl rustls::client::danger::ServerCertVerifier for AcceptAny {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::pki_types::CertificateDer<'_>,
            _intermediates: &[rustls::pki_types::CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            rustls::crypto::aws_lc_rs::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }

    /// Serve exactly one HTTP/1.1 request over the given accepted TLS connection, mirroring
    /// [`serve_tls`]: attempt the kTLS offload, fall back to userspace TLS if it fails, and answer
    /// `/healthz` with `200 ok`. Returns whether the kernel offload engaged.
    async fn serve_one(
        stream: tokio::net::TcpStream,
        cfg: Arc<rustls::ServerConfig>,
        ktls_ready: bool,
    ) -> bool {
        let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
        let svc = hyper::service::service_fn(|_req: Request<Incoming>| async {
            Ok::<_, std::convert::Infallible>(Response::new(full_body(Bytes::from_static(b"ok"))))
        });
        if ktls_ready {
            let corked = ktls::CorkStream::new(stream);
            let tls = acceptor.accept(corked).await.expect("server handshake");
            match ktls::config_ktls_server(tls).await {
                Ok(ks) => {
                    let io = TokioIo::new(ks);
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .await;
                    return true;
                }
                Err(_) => return false, // offload failed mid-connection; nothing to serve
            }
        }
        let tls = acceptor.accept(stream).await.expect("server handshake");
        let io = TokioIo::new(tls);
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(io, svc)
            .await;
        false
    }

    /// Drive a real rustls client GET `/healthz` against the serving path and assert `200 ok`.
    async fn roundtrip(ktls_ready: bool) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("c.crt");
        let key_path = dir.path().join("c.key");
        std::fs::write(&cert_path, CERT).unwrap();
        std::fs::write(&key_path, KEY).unwrap();
        let server_cfg = crate::tls::load_server_config(&cert_path, &key_path).unwrap();
        assert!(
            server_cfg.enable_secret_extraction,
            "fast-io build must enable secret extraction"
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            serve_one(sock, server_cfg, ktls_ready).await
        });

        let client_cfg = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAny))
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_cfg));
        let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut tls = connector
            .connect(server_name, tcp)
            .await
            .expect("client handshake");
        tls.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        tls.flush().await.unwrap();
        let mut buf = Vec::new();
        tls.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        assert!(
            text.starts_with("HTTP/1.1 200"),
            "expected 200, got: {text}"
        );
        assert!(
            text.trim_end().ends_with("ok"),
            "expected body 'ok', got: {text}"
        );

        let engaged = server.await.unwrap();
        // Whether the kernel actually offloaded is host-dependent; the response must be correct
        // either way. We only log the outcome so the result is visible in `--nocapture` runs.
        eprintln!("fast-io roundtrip ktls_ready={ktls_ready} kernel_offload_engaged={engaged}");
    }

    /// The userspace fallback path (kTLS *not* requested) serves a correct TLS response. This is the
    /// always-on path and must pass on every kernel.
    #[tokio::test]
    async fn tls_get_healthz_userspace_fallback() {
        roundtrip(false).await;
    }

    /// The kTLS path (offload requested) serves a correct TLS response. If the host kernel supports
    /// the `tls` ULP the kernel does the crypto; if not, `serve_one` reports no offload but the
    /// handshake/response still succeed via the cork-wrapped stream. Either way the client sees a
    /// correct `200 ok`, proving the offload attempt never corrupts the connection.
    #[tokio::test]
    async fn tls_get_healthz_with_ktls_offload_attempt() {
        // Only meaningful when the kernel advertises the ULP; otherwise the offload attempt would
        // consume the stream on failure (matching production), so gate on the probe.
        if !super::ktls_available() {
            eprintln!(
                "kernel kTLS unavailable; skipping offload roundtrip (fallback test covers correctness)"
            );
            return;
        }
        roundtrip(true).await;
    }

    /// A buffered response body still collects correctly when served over the kTLS-eligible path,
    /// guarding the `full_body` rendering the real `/healthz` uses.
    #[tokio::test]
    async fn full_body_collects() {
        let body = full_body(Bytes::from_static(b"ok"));
        let collected = body.collect().await.unwrap().to_bytes();
        assert_eq!(&collected[..], b"ok");
    }
}

#[cfg(test)]
mod delete_label_tests {
    use super::*;

    #[test]
    fn deleted_bucket_label_recognises_both_delete_paths() {
        let del = Method::DELETE;
        // Raw S3 path-style bucket delete.
        assert_eq!(deleted_bucket_label(&del, "/photos"), Some("photos"));
        // Management console / CLI bucket delete.
        assert_eq!(
            deleted_bucket_label(&del, "/api/v1/buckets/photos"),
            Some("photos")
        );

        // Not a bucket delete: object deletes, sub-resource deletes, listings, infra.
        assert_eq!(deleted_bucket_label(&del, "/photos/a/b.jpg"), None);
        assert_eq!(
            deleted_bucket_label(&del, "/api/v1/buckets/photos/objects"),
            None
        );
        assert_eq!(
            deleted_bucket_label(&del, "/api/v1/buckets/photos/policy"),
            None
        );
        assert_eq!(deleted_bucket_label(&del, "/api/v1/buckets"), None);
        assert_eq!(deleted_bucket_label(&del, "/"), None);
        assert_eq!(deleted_bucket_label(&del, "/healthz"), None);

        // Only DELETE counts — a GET/PUT to the same path is not a deletion.
        assert_eq!(deleted_bucket_label(&Method::GET, "/photos"), None);
        assert_eq!(
            deleted_bucket_label(&Method::PUT, "/api/v1/buckets/photos"),
            None
        );
    }

    #[test]
    fn console_assets_are_not_classified_as_s3_buckets() {
        let get = Method::GET;
        // On the web-console listener, a root-served asset like the favicon must NOT be charted as a
        // path-style S3 bucket named "favicon.svg" (the bug a fresh node made obvious).
        assert_eq!(classify_operation(true, &get, "/favicon.svg", ""), None);
        // The SPA shell / any other root path on the console listener is likewise not S3.
        assert_eq!(classify_operation(true, &get, "/anything", ""), None);
        // Management calls are still charted on the console listener.
        assert_eq!(
            classify_operation(true, &get, "/api/v1/buckets", ""),
            Some(("Management".to_owned(), String::new()))
        );
        // On the S3 data-plane listener the same path is a real path-style S3 op, unchanged.
        assert_eq!(
            classify_operation(false, &get, "/photos", ""),
            Some(("ListObjects".to_owned(), "photos".to_owned()))
        );
    }
}

#[cfg(test)]
mod redirect_tests {
    use super::*;

    fn location(head: &str, fallback: &str) -> String {
        build_https_redirect(head.as_bytes(), fallback)
            .lines()
            .find_map(|l| l.strip_prefix("Location: "))
            .unwrap()
            .to_owned()
    }

    #[test]
    fn redirect_preserves_host_and_target() {
        let resp = build_https_redirect(
            b"GET /console/metrics?range=1d HTTP/1.1\r\nHost: cairn.example:7374\r\n\r\n",
            "127.0.0.1:7374",
        );
        assert!(resp.starts_with("HTTP/1.1 308 "));
        assert!(resp.contains("Connection: close"));
        assert!(resp.contains("Location: https://cairn.example:7374/console/metrics?range=1d\r\n"));
    }

    #[test]
    fn redirect_falls_back_when_host_absent_or_unsafe() {
        // No Host header → use the local socket address.
        assert_eq!(
            location("GET / HTTP/1.1\r\n\r\n", "127.0.0.1:7374"),
            "https://127.0.0.1:7374/"
        );
        // A Host carrying anything outside the host charset is rejected → fallback.
        assert_eq!(
            location(
                "GET /x HTTP/1.1\r\nHost: ev il/path\r\n\r\n",
                "10.0.0.1:7374"
            ),
            "https://10.0.0.1:7374/x"
        );
    }

    #[test]
    fn redirect_sanitises_target_and_host_against_header_injection() {
        // A target that is not a clean absolute path falls back to "/".
        assert_eq!(
            location("GET nonsense HTTP/1.1\r\nHost: h\r\n\r\n", "fb:1"),
            "https://h/"
        );
        // is_safe_* reject CR/LF, spaces, and control bytes that could split the Location header.
        assert!(!is_safe_target("/ok\r\nSet-Cookie: x"));
        assert!(!is_safe_target("/has space"));
        assert!(is_safe_target("/ok/path?q=1&r=2"));
        assert!(!is_safe_host("h\r\nX: y"));
        assert!(!is_safe_host("has space"));
        assert!(is_safe_host("cairn.example:7374"));
        assert!(is_safe_host("[::1]:7374"));
    }

    #[test]
    fn redirect_returns_400_when_no_usable_host() {
        // No Host header AND an unusable fallback (e.g. local_addr() failed → empty string): there is
        // no host to build an absolute https:// URL from, so answer 400 rather than emit https:///.
        let resp = build_https_redirect(b"GET /x HTTP/1.1\r\n\r\n", "");
        assert!(resp.starts_with("HTTP/1.1 400 "), "got: {resp}");
        assert!(!resp.contains("Location:"), "got: {resp}");
        // An unsafe fallback is treated the same as no fallback.
        let resp = build_https_redirect(b"GET / HTTP/1.1\r\n\r\n", "bad host/with space");
        assert!(resp.starts_with("HTTP/1.1 400 "), "got: {resp}");
    }

    #[tokio::test]
    async fn redirect_reads_request_and_writes_308_over_a_stream() {
        let (mut client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(redirect_plaintext_to_https(
            server,
            "fallback:7374".to_owned(),
        ));
        client
            .write_all(b"GET /buckets HTTP/1.1\r\nHost: web.local:7374\r\nUser-Agent: x\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        client.read_to_end(&mut resp).await.unwrap();
        task.await.unwrap();
        let resp = String::from_utf8(resp).unwrap();
        assert!(resp.starts_with("HTTP/1.1 308 "), "got: {resp}");
        assert!(resp.contains("Location: https://web.local:7374/buckets\r\n"));
    }
}

#[cfg(test)]
mod redact_tests {
    use super::redact_log_path;

    #[test]
    fn share_token_is_redacted() {
        // Audit 2026-07: the share capability must never reach the access log.
        assert_eq!(
            redact_log_path("/share/abc123deadbeef"),
            "/share/<redacted>"
        );
        assert_eq!(redact_log_path("/share/"), "/share/<redacted>");
        // Other paths pass through unchanged.
        assert_eq!(redact_log_path("/bucket/key"), "/bucket/key");
        assert_eq!(redact_log_path("/healthz"), "/healthz");
    }
}
