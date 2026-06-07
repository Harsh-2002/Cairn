//! The HTTP serving loop, the outer middleware, and ordered graceful shutdown. In the
//! skeleton the router answers liveness, readiness, and metrics; later waves route the S3 and
//! management families here behind authentication and authorization.

use crate::adapter;
use crate::config::Config;
use crate::stack::AppStack;
use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use metrics_exporter_prometheus::PrometheusHandle;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
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
    /// The Prometheus render handle.
    metrics: PrometheusHandle,
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
    let state = Arc::new(AppState {
        ready: AtomicBool::new(false),
        concurrency: Semaphore::new(config.concurrency_limit),
        request_timeout: Duration::from_secs(config.request_timeout_secs),
        metrics,
        stack,
    });

    // Optional native TLS.
    let tls = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert), Some(key)) => {
            let cfg = crate::tls::load_server_config(cert, key).map_err(std::io::Error::other)?;
            Some(cfg)
        }
        _ => None,
    };

    // Migrations and startup reconciliation already ran while building the stack; ready now.
    state.ready.store(true, Ordering::SeqCst);

    // Background subsystems: the multipart sweeper and the lifecycle scanner.
    crate::background::spawn(
        state.stack.clone(),
        Duration::from_secs(3600),
        86_400,
        Duration::from_secs(3600),
    );
    tracing::info!(addr = %local, tls = tls.is_some(), "cairn listening");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(wait_for_signal(shutdown_tx));

    let mut conns = tokio::task::JoinSet::new();
    let mut shutdown = shutdown_rx.clone();
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, peer) = match accept {
                    Ok(v) => v,
                    Err(e) => { tracing::warn!(error = %e, "accept failed"); continue; }
                };
                let st = state.clone();
                let conn_shutdown = shutdown_rx.clone();
                let tls = tls.clone();
                conns.spawn(async move {
                    match tls {
                        Some(cfg) => {
                            let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
                            match acceptor.accept(stream).await {
                                Ok(s) => serve_io(s, st, peer, true, conn_shutdown).await,
                                Err(e) => tracing::debug!(error = %e, "TLS handshake failed"),
                            }
                        }
                        None => serve_io(stream, st, peer, false, conn_shutdown).await,
                    }
                });
            }
            _ = shutdown.changed() => {
                tracing::info!("shutdown signal received; draining connections");
                state.ready.store(false, Ordering::SeqCst);
                break;
            }
        }
    }

    // Drain in-flight connections within a bounded grace period.
    let drain = async { while conns.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(30), drain)
        .await
        .is_err()
    {
        tracing::warn!("drain timed out; aborting remaining connections");
        conns.shutdown().await;
    }
    tracing::info!("shutdown complete");
    Ok(())
}

/// Serve one accepted connection (plaintext or TLS) with graceful shutdown.
async fn serve_io<S>(
    stream: S,
    state: Arc<AppState>,
    peer: std::net::SocketAddr,
    secure: bool,
    mut conn_shutdown: watch::Receiver<bool>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let io = TokioIo::new(stream);
    let svc = service_fn(move |req| handle(state.clone(), peer, secure, req));
    let builder = auto::Builder::new(TokioExecutor::new());
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

/// The outer middleware: request id, tracing span, concurrency limit, timeout, and the
/// request/latency metrics, wrapping the router.
async fn handle(
    state: Arc<AppState>,
    peer: std::net::SocketAddr,
    secure: bool,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let request_id = uuid::Uuid::new_v4().simple().to_string();
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let span = tracing::info_span!(
        "request",
        request_id = %request_id,
        method = %method,
        path = %path,
        %peer,
    );

    let infra =
        method == Method::GET && matches!(path.as_str(), "/healthz" | "/readyz" | "/metrics");

    let response = async move {
        let _permit = match state.concurrency.try_acquire() {
            Ok(p) => p,
            Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "TooManyRequests"),
        };
        let start = Instant::now();
        let work = async {
            if infra {
                route_infra(&state, &path)
            } else {
                adapter::handle(&state.stack, req, peer.ip(), secure, request_id.clone()).await
            }
        };
        let mut resp = match tokio::time::timeout(state.request_timeout, work).await {
            Ok(r) => r,
            Err(_) => error_response(StatusCode::SERVICE_UNAVAILABLE, "RequestTimeout"),
        };
        let status = resp.status();
        let elapsed = start.elapsed().as_secs_f64();
        metrics::counter!(
            "cairn_requests_total",
            "method" => method.to_string(),
            "status" => status.as_u16().to_string(),
        )
        .increment(1);
        metrics::histogram!("cairn_request_duration_seconds", "method" => method.to_string())
            .record(elapsed);
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

/// Liveness, readiness, and metrics endpoints (the S3 and management families are dispatched
/// through the adapter).
fn route_infra(state: &AppState, path: &str) -> Response<Full<Bytes>> {
    match path {
        "/healthz" => text(StatusCode::OK, "ok"),
        "/readyz" => {
            if state.ready.load(Ordering::SeqCst) {
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
                .body(Full::new(Bytes::from(body)))
                .expect("valid metrics response")
        }
        _ => error_response(StatusCode::NOT_FOUND, "NotFound"),
    }
}

fn text(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .expect("valid text response")
}

fn error_response(status: StatusCode, code: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(code.to_owned())))
        .expect("valid error response")
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
// cairn-s3 real-stack integration tests.
