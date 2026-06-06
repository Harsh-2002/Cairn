//! Structured tracing and the Prometheus metrics recorder (ARCH §26).

use crate::config::LogFormat;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;

fn filter(level: &str) -> EnvFilter {
    EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Initialise the global tracing subscriber. Call exactly once, before serving.
pub fn init_tracing(level: &str, format: LogFormat) {
    match format {
        LogFormat::Json => fmt().with_env_filter(filter(level)).json().init(),
        LogFormat::Text => fmt().with_env_filter(filter(level)).init(),
    }
}

/// Install the global Prometheus recorder and return a handle that renders the exposition
/// text for the `/metrics` endpoint.
///
/// # Panics
/// Panics if a global metrics recorder is already installed.
#[must_use]
pub fn init_metrics() -> PrometheusHandle {
    PrometheusBuilder::new()
        .install_recorder()
        .expect("installing the Prometheus recorder")
}
