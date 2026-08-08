//! The HTTP serving loop, the outer middleware, and ordered graceful shutdown. In the
//! skeleton the router answers liveness, readiness, and metrics; later waves route the S3 and
//! management families here behind authentication and authorization.

use crate::adapter;
use crate::adapter::{ListenerRole, ResponseBody, full_body};
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
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError, watch};
use tracing::Instrument;

/// The two socket bindings and their immutable route roles. The optional control binding is absent
/// in headless mode; the data binding never inherits its routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ListenerPlan {
    data_addr: std::net::SocketAddr,
    control_addr: Option<std::net::SocketAddr>,
}

fn listener_plan(config: &Config) -> std::io::Result<ListenerPlan> {
    let control_addr = config.web_listen_addr().map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid control listener configuration: {error}"),
        )
    })?;
    Ok(ListenerPlan {
        data_addr: config.listen_addr,
        control_addr,
    })
}

/// Infrastructure work has its own small, fixed budget so probes and scrapes stay independent of
/// saturated S3 traffic without becoming an unbounded unauthenticated work lane. Metrics rendering
/// has a smaller sub-budget, reserving capacity for health/readiness probes during a scrape flood.
const INFRA_CONCURRENCY_LIMIT: usize = 4;
const METRICS_CONCURRENCY_LIMIT: usize = 2;
/// One shared bound for draining accepted HTTP connections and cooperative background workers.
const SHUTDOWN_DRAIN_GRACE: Duration = Duration::from_secs(30);
/// After every request future has been joined or cancelled, give the retained storage-commit
/// recovery consumer a small separate window to drain cancellation callbacks queued by their
/// drops. A timeout makes shutdown incomplete; startup recovery is the final fallback.
const SHUTDOWN_REQUEST_TAIL_GRACE: Duration = Duration::from_secs(5);
/// A separate bound for the ordered final metrics/counter flush and SQLite checkpoints.
const SHUTDOWN_FINALIZE_GRACE: Duration = Duration::from_secs(30);

#[derive(Default, Debug, PartialEq, Eq)]
struct HttpDrainReport {
    completed: usize,
    cancelled: usize,
    failed: usize,
    deadline_exceeded: bool,
}

impl HttpDrainReport {
    fn merge(mut self, other: Self) -> Self {
        self.completed += other.completed;
        self.cancelled += other.cancelled;
        self.failed += other.failed;
        self.deadline_exceeded |= other.deadline_exceeded;
        self
    }

    fn is_complete(&self) -> bool {
        !self.deadline_exceeded && self.cancelled == 0 && self.failed == 0
    }
}

/// A process-lifetime auxiliary task that aborts on unexpected owner drop.
///
/// Tokio detaches a plain `JoinHandle` when it is dropped. This wrapper makes both the normal
/// shutdown path (explicit join) and cancellation of [`serve`] fail closed: the SIGHUP and signal
/// waiters cannot outlive the server future as orphaned tasks.
struct RetainedTask {
    name: &'static str,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl RetainedTask {
    fn spawn(name: &'static str, task: impl Future<Output = ()> + Send + 'static) -> Self {
        Self {
            name,
            handle: Some(tokio::spawn(task)),
        }
    }

    async fn join(mut self) -> bool {
        let Some(handle) = self.handle.take() else {
            return true;
        };
        match handle.await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(task = self.name, %error, "auxiliary task exited unexpectedly");
                false
            }
        }
    }

    async fn cancel_and_join(mut self) -> bool {
        let Some(handle) = self.handle.take() else {
            return true;
        };
        handle.abort();
        match handle.await {
            Ok(()) => true,
            Err(error) if error.is_cancelled() => true,
            Err(error) => {
                tracing::warn!(task = self.name, %error, "auxiliary task exited unexpectedly");
                false
            }
        }
    }
}

impl Drop for RetainedTask {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

/// Independent in-flight budgets for normal traffic and infrastructure endpoints.
struct RequestBudgets {
    general: Semaphore,
    infrastructure: Semaphore,
    metrics: Semaphore,
}

/// RAII permits held for a request's whole execution.
struct RequestPermits<'a> {
    _request: SemaphorePermit<'a>,
    _metrics: Option<SemaphorePermit<'a>>,
}

impl RequestBudgets {
    fn new(general: usize) -> Self {
        Self {
            general: Semaphore::new(general),
            infrastructure: Semaphore::new(INFRA_CONCURRENCY_LIMIT),
            metrics: Semaphore::new(METRICS_CONCURRENCY_LIMIT),
        }
    }

    /// Acquire without waiting. Metrics requests take both the shared infrastructure permit and a
    /// metrics sub-permit; therefore at most two of the four infrastructure lanes can render a
    /// scrape, permanently reserving the others for liveness/readiness.
    fn try_acquire(
        &self,
        infrastructure: bool,
        metrics: bool,
    ) -> Result<RequestPermits<'_>, TryAcquireError> {
        let metrics = if metrics {
            Some(self.metrics.try_acquire()?)
        } else {
            None
        };
        let request = if infrastructure {
            self.infrastructure.try_acquire()?
        } else {
            self.general.try_acquire()?
        };
        Ok(RequestPermits {
            _request: request,
            _metrics: metrics,
        })
    }
}

/// Shared, cheaply-cloneable server state.
struct AppState {
    /// Readiness gate: false until migrations + reconciliation have completed.
    ready: Arc<AtomicBool>,
    /// Independent normal-request and infrastructure concurrency budgets.
    budgets: RequestBudgets,
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
    /// Immediate peers whose validated forwarding metadata may establish control-plane transport
    /// provenance. It never changes S3 source-IP authorization in this audit phase.
    trusted_proxies: crate::proxy::TrustedProxies,
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
    let plan = listener_plan(&config)?;
    let trusted_proxies = config.trusted_proxy_allowlist().map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid trusted-proxy configuration: {error}"),
        )
    })?;
    let listener = TcpListener::bind(plan.data_addr).await?;
    let local = listener.local_addr()?;
    // The control listener is a second, optional socket with a disjoint route matrix. `None`
    // (`CAIRN_WEB_ADDR` empty/off) runs headless: no management socket is bound, and the data
    // listener's immutable role still rejects the management namespace.
    let web_listener = match plan.control_addr {
        Some(addr) => Some(TcpListener::bind(addr).await?),
        None => None,
    };
    let web_local = web_listener.as_ref().and_then(|l| l.local_addr().ok());
    let state = Arc::new(AppState {
        ready: Arc::new(AtomicBool::new(false)),
        budgets: RequestBudgets::new(config.concurrency_limit),
        request_timeout: Duration::from_secs(config.request_timeout_secs),
        header_read_timeout: Duration::from_secs(config.header_read_timeout_secs),
        connection_limiter: Arc::new(Semaphore::new(config.max_connections)),
        metrics,
        request_metrics_enabled: config.request_metrics_enabled,
        trusted_proxies,
        fastio_min_bytes: config.fastio_min_bytes,
        stack,
    });

    // Optional native TLS. The served config lives behind a watch channel so a SIGHUP can
    // hot-reload the certificate/key from the same paths without dropping the listener
    // (ARCH 27.2): the accept loop reads the current config per connection, and the reload
    // handler atomically publishes a new one (a bad new cert is logged and the old config kept).
    let (tls_rx, tls_reload_task) = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert), Some(key)) => {
            let cfg = crate::tls::load_server_config(cert, key).map_err(std::io::Error::other)?;
            let (tx, rx) = watch::channel(cfg);
            let task = RetainedTask::spawn(
                "TLS reload",
                reload_tls_on_sighup(tx, cert.clone(), key.clone()),
            );
            (Some(rx), Some(task))
        }
        _ => (None, None),
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
    let signal_task = RetainedTask::spawn(
        "shutdown signal",
        broadcast_shutdown(wait_for_signal(), Arc::clone(&state.ready), shutdown_tx),
    );

    // Background subsystems: multipart sweeper, lifecycle scanner, WAL checkpointer, metrics, and
    // the replication worker pool (which takes the shutdown receiver).
    let background = crate::background::spawn(state.stack.clone(), &config, shutdown_rx.clone());
    tracing::info!(s3_api = %local, web_console = ?web_local, tls = tls_rx.is_some(), "cairn listening");

    // Run the disjoint data and (optionally) control accept loops concurrently. Their roles are
    // values captured at wiring time, not a per-request decision.
    let listeners = async {
        let api = accept_loop(
            listener,
            state.clone(),
            tls_rx.clone(),
            ktls_ready,
            ListenerRole::Data,
            shutdown_rx.clone(),
        );
        let report = match web_listener {
            Some(sock) => {
                let web = accept_loop(
                    sock,
                    state.clone(),
                    tls_rx,
                    ktls_ready,
                    ListenerRole::Control,
                    shutdown_rx.clone(),
                );
                let (api_report, web_report) = tokio::join!(api, web);
                api_report.merge(web_report)
            }
            None => api.await,
        };
        // All accepted request futures have now returned or been force-cancelled, and therefore
        // every armed multipart or ordinary object-write drop guard has synchronously enqueued its
        // recovery record. The FIFO sentinel lets the retained consumer process those commands and
        // exit; it is joined before final persistence below.
        state.stack.multipart_claim_recovery.finish_requests();
        report
    };

    // Stop accepting/claiming concurrently. Only after BOTH HTTP and workers have drained do the
    // final request-metrics drain, seal-counter persistence, and WAL checkpoints run; otherwise an
    // in-flight request could increment the accumulator after its supposed final flush.
    let mut background_shutdown = shutdown_rx.clone();
    let shutdown_state = state.clone();
    let stop_background = async move {
        wait_for_shutdown(&mut background_shutdown).await;
        // The signal broadcaster withdraws readiness before publishing shutdown. Repeat the store
        // as a fail-safe for the sender-closed path so an auxiliary-task failure cannot leave a
        // stopped listener advertised ready.
        shutdown_state.ready.store(false, Ordering::SeqCst);
        background.stop(SHUTDOWN_DRAIN_GRACE).await
    };
    let (http_report, stopped_background) = tokio::join!(listeners, stop_background);
    let background_report = stopped_background
        .finalize(SHUTDOWN_REQUEST_TAIL_GRACE, SHUTDOWN_FINALIZE_GRACE)
        .await;

    // Neither auxiliary task is detached. The signal broadcaster has completed by definition; the
    // SIGHUP waiter may still be blocked in `recv`, so cancel and join it explicitly.
    let signal_joined = signal_task.join().await;
    let tls_joined = match tls_reload_task {
        Some(task) => task.cancel_and_join().await,
        None => true,
    };
    if http_report.is_complete() && background_report.is_complete() && signal_joined && tls_joined {
        tracing::info!("shutdown complete");
    } else {
        tracing::error!(
            http_drain_complete = http_report.is_complete(),
            background_shutdown_complete = background_report.is_complete(),
            signal_joined,
            tls_joined,
            "server stopped with incomplete shutdown work"
        );
    }
    Ok(())
}

/// Accept and serve connections on one listener until shutdown, then drain in-flight connections
/// within a bounded grace period. `role` is fixed when the listener is wired.
async fn accept_loop(
    listener: TcpListener,
    state: Arc<AppState>,
    tls_rx: Option<watch::Receiver<Arc<rustls::ServerConfig>>>,
    ktls_ready: bool,
    role: ListenerRole,
    shutdown_rx: watch::Receiver<bool>,
) -> HttpDrainReport {
    let mut conns = tokio::task::JoinSet::new();
    let mut shutdown = shutdown_rx.clone();
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
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
                        Some(cfg) => serve_tls(stream, cfg, ktls_ready, st, peer, role, conn_shutdown).await,
                        None => serve_plaintext(stream, st, peer, role, conn_shutdown).await,
                    }
                });
            }
        }
    }

    let report = drain_connections(conns, SHUTDOWN_DRAIN_GRACE).await;
    if report.is_complete() {
        tracing::info!(
            ?role,
            completed = report.completed,
            "HTTP connections drained"
        );
    } else {
        tracing::error!(
            ?role,
            completed = report.completed,
            cancelled = report.cancelled,
            failed = report.failed,
            deadline_exceeded = report.deadline_exceeded,
            "HTTP connection drain incomplete"
        );
    }
    report
}

async fn drain_connections(
    mut connections: tokio::task::JoinSet<()>,
    grace: Duration,
) -> HttpDrainReport {
    let mut report = HttpDrainReport::default();
    let cooperative = async {
        while let Some(result) = connections.join_next().await {
            match result {
                Ok(()) => report.completed += 1,
                Err(error) if error.is_cancelled() => {
                    report.cancelled += 1;
                    tracing::warn!(%error, "HTTP connection task was cancelled during drain");
                }
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(%error, "HTTP connection task failed during drain");
                }
            }
        }
    };

    if tokio::time::timeout(grace, cooperative).await.is_err() {
        report.deadline_exceeded = true;
        let remaining = connections.len();
        tracing::error!(
            remaining,
            timeout_seconds = grace.as_secs(),
            "HTTP connection drain deadline exceeded; aborting remaining tasks"
        );
        connections.abort_all();
        // `abort_all` only requests cancellation. Drain every result so no connection task is
        // detached and so panics/cancellations are represented in the process-level outcome.
        while let Some(result) = connections.join_next().await {
            match result {
                Ok(()) => report.completed += 1,
                Err(error) if error.is_cancelled() => report.cancelled += 1,
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(%error, "HTTP connection task failed while being cancelled");
                }
            }
        }
    }
    report
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
    role: ListenerRole,
    conn_shutdown: watch::Receiver<bool>,
) {
    // Console courtesy: on the web-console listener, a browser that connects in plaintext to the TLS port
    // gets a `308` to the `https://` URL rather than an opaque handshake failure. Peek the first byte
    // WITHOUT consuming it — a TLS ClientHello is a handshake record (`0x16`); any other first byte is
    // a plaintext HTTP request (`G`/`P`/… are all != 0x16). The S3 data-plane listener
    // (the data role) deliberately skips this and stays TLS-only: redirecting a SigV4 request
    // would require first accepting its `Authorization`/presigned credentials over cleartext.
    if role.is_control() {
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
                    serve_io(ktls_stream, state, peer, true, role, conn_shutdown).await;
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
        Ok(tls) => serve_io(tls, state, peer, true, role, conn_shutdown).await,
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
    role: ListenerRole,
    conn_shutdown: watch::Receiver<bool>,
) {
    // The sendfile fast path runs only on the S3 data-plane listener: the web console listener serves console
    // assets at paths that must be matched before S3 routing, so it always goes straight to hyper.
    #[cfg(all(feature = "fast-io", target_os = "linux"))]
    if role.is_data() {
        let mut fast_shutdown = conn_shutdown.clone();
        match crate::fast_get::try_sendfile_get(
            stream,
            state.stack.as_ref(),
            peer,
            &state.trusted_proxies,
            state.request_metrics_enabled,
            state.fastio_min_bytes,
            &mut fast_shutdown,
        )
        .await
        {
            crate::fast_get::Fast::Handled => {}
            crate::fast_get::Fast::Fallback { stream } => {
                serve_io(stream, state, peer, false, role, conn_shutdown).await;
            }
        }
        return;
    }
    serve_io(stream, state, peer, false, role, conn_shutdown).await;
}

async fn serve_io<S>(
    stream: S,
    state: Arc<AppState>,
    peer: std::net::SocketAddr,
    secure: bool,
    role: ListenerRole,
    mut conn_shutdown: watch::Receiver<bool>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let io = TokioIo::new(stream);
    let svc_shutdown = conn_shutdown.clone();
    // Capture before `state` is moved into the service closure (Duration is Copy).
    let header_read_timeout = state.header_read_timeout;
    let svc =
        service_fn(move |req| handle(state.clone(), peer, secure, role, req, svc_shutdown.clone()));
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
    if *conn_shutdown.borrow() {
        conn.as_mut().graceful_shutdown();
        let _ = conn.await;
        return;
    }
    tokio::select! {
        biased;
        _ = conn_shutdown.changed() => {
            conn.as_mut().graceful_shutdown();
            let _ = conn.await;
        }
        res = conn.as_mut() => {
            if let Err(e) = res { tracing::debug!(error = %e, "connection ended"); }
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
    role: ListenerRole,
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

    let infra = role.is_data()
        && method == Method::GET
        && matches!(path.as_str(), "/healthz" | "/readyz" | "/metrics");
    // Load-shed and timeout are the two failures a person is most likely to meet under load, and
    // both answer before the request ever reaches the S3 adapter. Decide here, while the head is
    // still in hand, whether this caller is a browser navigating (ARCH 25.1.1); machine clients keep
    // the exact plain-text body they have always received.
    let shed_wants_html = {
        let hdrs: Vec<(String, String)> = req
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_owned(),
                    v.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        crate::error_page::wants_html_pairs(&method, &hdrs)
    };

    let response = async move {
        // Infra endpoints use a small dedicated budget, independent of saturated S3 traffic but
        // still bounded against unauthenticated probe/scrape floods. Metrics has a sub-budget so
        // rendering scrapes cannot occupy every infrastructure lane and starve `/readyz`.
        let _permits = match state
            .budgets
            .try_acquire(infra, infra && path == "/metrics")
        {
            Ok(permits) => permits,
            Err(_) => {
                return shed_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "TooManyRequests",
                    shed_wants_html,
                    &request_id,
                );
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
                    adapter::RequestTransport::new(peer.ip(), secure, &state.trusted_proxies, role),
                    request_id.clone(),
                    shutdown.clone(),
                )
                .await
            }
        };
        let mut resp = match tokio::time::timeout(state.request_timeout, work).await {
            Ok(r) => r,
            Err(_) => shed_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "RequestTimeout",
                shed_wants_html,
                &request_id,
            ),
        };
        let status = resp.status();
        let elapsed_dur = start.elapsed();
        let elapsed = elapsed_dur.as_secs_f64();
        // A low-cardinality `route` label (ARCH 26): the request is bucketed into a small fixed set
        // of route classes rather than the raw path, so the time series stay bounded.
        let route = classify_route(role, &path);
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
            if let Some((op, mut bucket)) = classify_operation(role, &method, &path, &query) {
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
/// series, so it is collapsed to a coarse family. The listener role is part of the classification:
/// the same path can be an S3 bucket on the data listener and a rejected console path on control.
pub(crate) fn classify_route(role: ListenerRole, path: &str) -> &'static str {
    match role {
        ListenerRole::Data => match path {
            "/healthz" => "healthz",
            "/readyz" => "readyz",
            "/metrics" => "metrics",
            _ if adapter::is_control_path(path) => "rejected",
            _ if path.starts_with("/share/") => "share",
            _ => "s3",
        },
        ListenerRole::Control => {
            if adapter::is_control_path(path) {
                "api"
            } else {
                "web"
            }
        }
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
/// On the control listener, exact `/api/v1` requests are management operations and every other path
/// is console or rejected traffic. On the data listener, control paths, infrastructure, and shares
/// are excluded; everything else retains normal path-style S3 classification.
pub(crate) fn classify_operation(
    role: ListenerRole,
    method: &Method,
    path: &str,
    query: &str,
) -> Option<(String, String)> {
    if role.is_control() {
        return adapter::is_control_path(path).then(|| ("Management".to_owned(), String::new()));
    }

    // Not-counted families. Mirror `classify_route`'s buckets so the two stay consistent.
    match path {
        "/" | "/healthz" | "/readyz" | "/metrics" => return None,
        _ => {}
    }
    if path.starts_with("/share/") || adapter::is_control_path(path) {
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
/// responsive — a constant-cost `SELECT 1` through every read pool AND a cheap probe of
/// the single writer (it must be draining its queue, not wedged). `/healthz` stays pure liveness;
/// this probe must not falsely report ready when either the read pool or the writer is stuck. The
/// writer probe is available only for the concrete sqlite backend; the libSQL/Turso engines
/// self-manage their writer, so for them the read probe alone gates readiness.
async fn is_ready(state: &AppState) -> bool {
    if !state.ready.load(Ordering::SeqCst) {
        return false;
    }
    if state.stack.meta.read_probe().await.is_err() {
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

/// The load-shed / request-timeout answer. A browser navigation gets the same readable page every
/// other failure renders (ARCH 25.1.1); every machine client keeps the byte-identical plain-text
/// body, which the stress harnesses assert on.
fn shed_response(
    status: StatusCode,
    code: &str,
    wants_html: bool,
    request_id: &str,
) -> Response<ResponseBody> {
    if wants_html {
        let html = crate::error_page::render(status, code, "", request_id);
        if let Ok(resp) = Response::builder()
            .status(status)
            .header("content-type", "text/html; charset=utf-8")
            .header(
                "content-security-policy",
                "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'",
            )
            .header("x-content-type-options", "nosniff")
            .header("cache-control", "no-store")
            .header("vary", crate::error_page::VARY)
            .body(full_body(Bytes::from(html)))
        {
            return resp;
        }
    }
    error_response(status, code)
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

/// Publish a testable shutdown future onto the process-wide watch channel.
async fn broadcast_shutdown(
    signal: impl Future<Output = ()>,
    ready: Arc<AtomicBool>,
    tx: watch::Sender<bool>,
) {
    signal.await;
    // Ordering is part of the external shutdown contract: load balancers must see not-ready before
    // listeners stop accepting and workers stop claiming new work from the published watch value.
    ready.store(false, Ordering::SeqCst);
    let _ = tx.send(true);
}

/// Wait until shutdown is signalled or the sole sender exits unexpectedly. Sender closure is
/// treated as shutdown so workers and listeners cannot remain orphaned if the signal task fails.
async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}

/// Resolve on the first of SIGINT or SIGTERM.
async fn wait_for_signal() {
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
mod request_budget_tests {
    use super::*;

    #[test]
    fn infrastructure_budget_is_bounded_independent_and_reserves_probe_lanes() {
        let budgets = RequestBudgets::new(1);

        // Saturating normal S3/control work does not consume the infrastructure budget.
        let general = budgets.try_acquire(false, false).unwrap();
        assert!(budgets.try_acquire(false, false).is_err());

        // A scrape consumes one infrastructure lane plus one of the two metrics sub-lanes.
        let metrics_a = budgets.try_acquire(true, true).unwrap();
        let metrics_b = budgets.try_acquire(true, true).unwrap();
        assert!(
            budgets.try_acquire(true, true).is_err(),
            "a third concurrent scrape must fail fast"
        );

        // Two infrastructure lanes remain available for cheap health/readiness probes even while
        // both metrics lanes are busy. This prevents a scrape flood from starving `/readyz`.
        let probe_a = budgets.try_acquire(true, false).unwrap();
        let probe_b = budgets.try_acquire(true, false).unwrap();
        assert!(
            budgets.try_acquire(true, false).is_err(),
            "the fixed infrastructure budget must fail fast when full"
        );

        // Releasing any request returns its exact lane immediately.
        drop(metrics_a);
        let metrics_c = budgets.try_acquire(true, true).unwrap();
        drop((metrics_b, metrics_c, probe_a, probe_b, general));
        assert!(budgets.try_acquire(false, false).is_ok());
        assert!(budgets.try_acquire(true, false).is_ok());
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn shutdown_withdraws_readiness_before_waking_listeners_and_workers() {
        let ready = Arc::new(AtomicBool::new(true));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let signal_task = RetainedTask::spawn(
            "test shutdown signal",
            broadcast_shutdown(
                async move {
                    let _ = signal_rx.await;
                },
                Arc::clone(&ready),
                shutdown_tx,
            ),
        );

        assert!(ready.load(Ordering::SeqCst));
        assert!(!*shutdown_rx.borrow());
        signal_tx.send(()).unwrap();
        shutdown_rx.changed().await.unwrap();

        // `broadcast_shutdown` stores readiness=false before publishing the watch value. Seeing
        // that value therefore guarantees every listener/worker also sees readiness withdrawn.
        assert!(!ready.load(Ordering::SeqCst));
        assert!(*shutdown_rx.borrow());
        assert!(signal_task.join().await);
    }

    #[tokio::test]
    async fn retained_auxiliary_task_aborts_on_owner_drop_instead_of_detaching() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let dropped_task = Arc::clone(&dropped);
        let task = RetainedTask::spawn("TLS reload test", async move {
            let _drop_flag = DropFlag(dropped_task);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("auxiliary task started");

        drop(task);
        tokio::task::yield_now().await;
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn retained_auxiliary_cancel_path_aborts_and_joins() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let dropped_task = Arc::clone(&dropped);
        let task = RetainedTask::spawn("TLS reload test", async move {
            let _drop_flag = DropFlag(dropped_task);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("auxiliary task started");

        assert!(task.cancel_and_join().await);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn http_drain_timeout_aborts_awaits_and_reports_remaining_connections() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut connections = tokio::task::JoinSet::new();
        connections.spawn(async {});
        let dropped_task = Arc::clone(&dropped);
        connections.spawn(async move {
            let _drop_flag = DropFlag(dropped_task);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("connection task started");
        tokio::task::yield_now().await;

        let report = drain_connections(connections, Duration::from_millis(10)).await;
        assert_eq!(
            report,
            HttpDrainReport {
                completed: 1,
                cancelled: 1,
                failed: 0,
                deadline_exceeded: true,
            }
        );
        assert!(!report.is_complete());
        assert!(
            dropped.load(Ordering::SeqCst),
            "cancelled connection future must be dropped before drain returns"
        );
    }
}

#[cfg(test)]
mod delete_label_tests {
    use super::*;

    #[test]
    fn headless_listener_plan_has_no_control_plane() {
        let mut config = Config {
            web_addr: "off".to_owned(),
            ..Config::default()
        };
        let plan = listener_plan(&config).expect("headless plan");
        assert_eq!(plan.data_addr, config.listen_addr);
        assert_eq!(plan.control_addr, None);

        config.web_addr = "127.0.0.1:7374".to_owned();
        let plan = listener_plan(&config).expect("two-listener plan");
        assert_eq!(plan.control_addr, Some("127.0.0.1:7374".parse().unwrap()));
    }

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
        assert_eq!(
            classify_operation(ListenerRole::Control, &get, "/favicon.svg", ""),
            None
        );
        // The SPA shell / any other root path on the console listener is likewise not S3.
        assert_eq!(
            classify_operation(ListenerRole::Control, &get, "/anything", ""),
            None
        );
        // Management calls are still charted on the console listener.
        assert_eq!(
            classify_operation(ListenerRole::Control, &get, "/api/v1/buckets", ""),
            Some(("Management".to_owned(), String::new()))
        );
        // On the S3 data-plane listener the same path is a real path-style S3 op, unchanged.
        assert_eq!(
            classify_operation(ListenerRole::Data, &get, "/photos", ""),
            Some(("ListObjects".to_owned(), "photos".to_owned()))
        );
        assert_eq!(
            classify_operation(ListenerRole::Data, &get, "/favicon.svg", ""),
            Some(("ListObjects".to_owned(), "favicon.svg".to_owned()))
        );
        // Segment lookalikes remain S3 names on data; only the exact versioned namespace is blocked.
        assert_eq!(
            classify_operation(ListenerRole::Data, &get, "/api/v10", ""),
            Some(("GetObject".to_owned(), "api".to_owned()))
        );
        assert_eq!(
            classify_operation(ListenerRole::Data, &get, "/api/v1/buckets", ""),
            None
        );
    }

    #[test]
    fn route_metrics_follow_the_listener_matrix() {
        assert_eq!(classify_route(ListenerRole::Data, "/"), "s3");
        assert_eq!(classify_route(ListenerRole::Control, "/"), "web");
        assert_eq!(
            classify_route(ListenerRole::Data, "/api/v1/buckets"),
            "rejected"
        );
        assert_eq!(
            classify_route(ListenerRole::Control, "/api/v1/buckets"),
            "api"
        );
        assert_eq!(classify_route(ListenerRole::Data, "/api/v10"), "s3");
        assert_eq!(classify_route(ListenerRole::Control, "/api/v10"), "web");
        assert_eq!(classify_route(ListenerRole::Data, "/healthz"), "healthz");
        assert_eq!(classify_route(ListenerRole::Control, "/healthz"), "web");
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
