//! Structured tracing and the Prometheus metrics recorder (ARCH 26).

use crate::config::LogFormat;
use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};
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
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("installing the Prometheus recorder");
    describe_metrics();
    handle
}

/// Register help text and units for the metric series the server emits (ARCH 26). Describing a
/// series is idempotent and independent of whether it has been observed yet, so the `/metrics`
/// exposition carries `# HELP`/`# TYPE` lines from the first scrape. Only the series introduced or
/// extended by this wave are described here; pre-existing store gauges keep their inline emission.
fn describe_metrics() {
    // Request-level series (now carrying a `route` label alongside method/status).
    describe_counter!(
        "cairn_requests_total",
        "Total HTTP requests, labelled by method, status, and coarse route class"
    );
    describe_histogram!(
        "cairn_request_duration_seconds",
        Unit::Seconds,
        "Request handling latency, labelled by method and route class"
    );

    // Throughput.
    describe_counter!(
        "cairn_bytes_received_total",
        Unit::Bytes,
        "Total request-body bytes received (from declared content-length)"
    );
    describe_counter!(
        "cairn_bytes_sent_total",
        Unit::Bytes,
        "Total response-body bytes sent (from declared content-length)"
    );

    // Metadata config-cache effectiveness (ARCH 11.5). Monotonic cumulative counts.
    describe_counter!(
        "cairn_meta_cache_hits_total",
        "Cumulative metadata config-cache hits"
    );
    describe_counter!(
        "cairn_meta_cache_misses_total",
        "Cumulative metadata config-cache misses"
    );

    // Writer backpressure (ARCH 26.2).
    describe_gauge!(
        "cairn_writer_queue_depth",
        "Inbound metadata-writer queue depth (submitted but not yet committed)"
    );
    describe_histogram!(
        "cairn_writer_commit_seconds",
        Unit::Seconds,
        "Wall time of a single metadata group-commit durability barrier (the fsync)"
    );
    describe_histogram!(
        "cairn_writer_batch_size",
        "Mutations coalesced into one metadata group-commit batch"
    );

    // Replication observability (ARCH 20/26).
    describe_gauge!(
        "cairn_replication_lag_seconds",
        Unit::Seconds,
        "Age of the oldest due replication outbox entry"
    );
    describe_gauge!(
        "cairn_replication_queue_depth",
        "Number of replication outbox entries currently due"
    );
    describe_counter!(
        "cairn_replication_bytes_total",
        Unit::Bytes,
        "Total logical bytes shipped by successful object replications"
    );
}
