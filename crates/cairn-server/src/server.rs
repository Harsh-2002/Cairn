//! The HTTP serving loop, the outer middleware, and ordered graceful shutdown. In the
//! skeleton the router answers liveness, readiness, and metrics; later waves route the S3 and
//! management families here behind authentication and authorization.

use crate::config::Config;
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
}

/// Run the server until a shutdown signal is received, then drain in-flight work.
///
/// # Errors
/// Returns an I/O error if the listener cannot bind.
pub async fn serve(config: Config, metrics: PrometheusHandle) -> std::io::Result<()> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    let local = listener.local_addr()?;
    let state = Arc::new(AppState {
        ready: AtomicBool::new(false),
        concurrency: Semaphore::new(config.concurrency_limit),
        request_timeout: Duration::from_secs(config.request_timeout_secs),
        metrics,
    });

    // The skeleton has no migrations/reconciliation yet; mark ready immediately. Later waves
    // gate readiness on those completing before the listener is considered serving.
    state.ready.store(true, Ordering::SeqCst);
    tracing::info!(addr = %local, "cairn listening");

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
                let mut conn_shutdown = shutdown_rx.clone();
                conns.spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req| handle(st.clone(), peer, req));
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

/// The outer middleware: request id, tracing span, concurrency limit, timeout, and the
/// request/latency metrics, wrapping the router.
async fn handle(
    state: Arc<AppState>,
    peer: std::net::SocketAddr,
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

    let response = async move {
        let _permit = match state.concurrency.try_acquire() {
            Ok(p) => p,
            Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "TooManyRequests"),
        };
        let start = Instant::now();
        let routed =
            tokio::time::timeout(state.request_timeout, route(&state, &method, &path)).await;
        let mut resp = match routed {
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

/// The skeleton router. Later waves dispatch the four request families here.
async fn route(state: &AppState, method: &Method, path: &str) -> Response<Full<Bytes>> {
    match (method, path) {
        (&Method::GET, "/healthz") => text(StatusCode::OK, "ok"),
        (&Method::GET, "/readyz") => {
            if state.ready.load(Ordering::SeqCst) {
                text(StatusCode::OK, "ready")
            } else {
                text(StatusCode::SERVICE_UNAVAILABLE, "not ready")
            }
        }
        (&Method::GET, "/metrics") => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn router_answers_health_and_readiness() {
        let metrics = metrics_exporter_prometheus::PrometheusBuilder::new()
            .build_recorder()
            .handle();
        let state = AppState {
            ready: AtomicBool::new(true),
            concurrency: Semaphore::new(8),
            request_timeout: Duration::from_secs(5),
            metrics,
        };
        let h = route(&state, &Method::GET, "/healthz").await;
        assert_eq!(h.status(), StatusCode::OK);
        let r = route(&state, &Method::GET, "/readyz").await;
        assert_eq!(r.status(), StatusCode::OK);
        state.ready.store(false, Ordering::SeqCst);
        let r = route(&state, &Method::GET, "/readyz").await;
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        let nf = route(&state, &Method::GET, "/nope").await;
        assert_eq!(nf.status(), StatusCode::NOT_FOUND);
    }
}
