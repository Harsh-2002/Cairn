//! Background subsystems (ARCH §6.4): the multipart-upload sweeper, the lifecycle scanner, the
//! WAL checkpointer, and the store-metrics refresher. Each runs on a configurable interval
//! against the shared engine stack. Replication workers are wired once a remote sink is
//! configured.

use crate::config::Config;
use crate::stack::AppStack;
use cairn_crypto::SystemClock;
use cairn_lifecycle::{BucketLifecycle, LifecycleScanner};
use cairn_types::bucket::ConfigAspect;
use cairn_types::meta::Mutation;
use cairn_types::traits::Clock;
use std::sync::Arc;
use std::time::Duration;

/// Spawn the background tasks, reading their intervals and the multipart lifetime from the
/// configured §28.2 knobs.
pub fn spawn(stack: Arc<AppStack>, cfg: &Config) {
    let sweep_interval = Duration::from_secs(cfg.multipart_sweep_interval_secs);
    #[allow(clippy::cast_possible_wrap)]
    let multipart_lifetime_secs = cfg.multipart_upload_lifetime_secs as i64;
    let lifecycle_interval = Duration::from_secs(cfg.lifecycle_interval_secs);
    let checkpoint_interval = Duration::from_secs(cfg.wal_checkpoint_interval_secs);

    tokio::spawn(sweeper_loop(
        stack.clone(),
        sweep_interval,
        multipart_lifetime_secs,
    ));
    tokio::spawn(lifecycle_loop(stack.clone(), lifecycle_interval));
    tokio::spawn(checkpoint_loop(stack.clone(), checkpoint_interval));

    // Replication worker: only active when a destination endpoint is configured (otherwise outbox
    // entries accumulate and are observable, never silently dropped — ARCH §20). The configured
    // `replication_dest_bucket` is the *default* destination; the actual destination per source
    // bucket is resolved each drain from that bucket's stored replication rule (see
    // `replication_loop`).
    if let (Some(endpoint), Some(dest_bucket), Some(access), Some(secret)) = (
        cfg.replication_endpoint.clone(),
        cfg.replication_dest_bucket.clone(),
        cfg.replication_access_key.clone(),
        cfg.replication_secret.clone(),
    ) {
        let sink_cfg = cairn_replication::S3SinkConfig {
            endpoint,
            dest_bucket,
            // Populated per drain from each source bucket's replication rule.
            dest_buckets: std::collections::HashMap::new(),
            region: cfg
                .replication_region
                .clone()
                .unwrap_or_else(|| cfg.region.clone()),
            access_key_id: access,
            secret_access_key: secret,
        };
        tokio::spawn(replication_loop(
            stack.clone(),
            sink_cfg,
            Duration::from_secs(cfg.replication_interval_secs),
        ));
        tracing::info!("replication worker enabled");
    }
    tokio::spawn(metrics_loop(stack));
}

/// Drain the replication outbox to the configured remote sink on an interval (ARCH §20).
///
/// `base_cfg` carries the endpoint, credentials, region, and the *default* destination bucket.
/// Before each drain the per-source-bucket destination map is rebuilt from every bucket's stored
/// replication rule (`ConfigAspect::Replication` → [`parse_replication`] → the rule's
/// `<Destination><Bucket>` with the `arn:aws:s3:::` prefix stripped), so each source bucket's
/// objects ship to the destination its own rule names; a bucket with no explicit destination
/// falls back to `replication_dest_bucket`. The sink is rebuilt per drain with the fresh map
/// (its connector is cheap to construct), keeping the node→node single-destination path working
/// when no per-bucket rule is present.
async fn replication_loop(
    stack: Arc<AppStack>,
    base_cfg: cairn_replication::S3SinkConfig,
    interval: Duration,
) {
    let engine =
        cairn_replication::ReplicationEngine::new(cairn_replication::ReplicationOpts::default());
    let clock = SystemClock::new();
    loop {
        tokio::time::sleep(interval).await;

        // Resolve the per-source destination map from each bucket's replication rule.
        let dest_buckets = resolve_dest_buckets(&stack).await;
        let mut sink_cfg = base_cfg.clone();
        sink_cfg.dest_buckets = dest_buckets;
        let sink = match cairn_replication::HttpS3Sink::new(sink_cfg) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "replication sink construction failed; skipping drain");
                continue;
            }
        };

        match engine
            .run_until_idle(&*stack.meta, &sink, &stack.blob, &clock, 50)
            .await
        {
            Ok(report) if !report.is_idle() => {
                metrics::counter!("cairn_replication_completed_total")
                    .increment(report.completed as u64);
                metrics::counter!("cairn_replication_failed_total").increment(report.failed as u64);
                tracing::info!(
                    completed = report.completed,
                    failed = report.failed,
                    "replication progressed"
                );
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "replication run failed"),
        }
    }
}

/// Build the `source bucket name -> destination bucket name` map by reading each bucket's stored
/// `ConfigAspect::Replication` document and taking the first enabled rule's destination bucket
/// (ARN prefix stripped). Buckets with no replication config, an unparseable document, or no
/// destination are simply omitted, so they fall back to the sink's default destination.
async fn resolve_dest_buckets(stack: &Arc<AppStack>) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let buckets = match stack.meta.list_buckets(None).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "replication: listing buckets for dest resolution failed");
            return map;
        }
    };
    for b in buckets {
        let Ok(Some(doc)) = stack
            .meta
            .get_bucket_config(&b.name, ConfigAspect::Replication)
            .await
        else {
            continue;
        };
        let Ok(cfg) = cairn_replication::parse_replication(doc.0.as_bytes()) else {
            continue;
        };
        // The first enabled rule that names a destination determines this bucket's target.
        if let Some(dest) = cfg
            .rules
            .iter()
            .find(|r| r.enabled)
            .and_then(|r| r.destination.bucket())
        {
            map.insert(b.name.as_str().to_owned(), dest.to_owned());
        }
    }
    map
}

/// Periodically run a truncating WAL checkpoint on the metadata store and publish the WAL size
/// and checkpoint stats as metrics (ARCH §8.4/§11.2, F-3). Without this the `-wal` file can grow
/// unbounded under sustained writes with a long-lived reader, inflating disk use and read
/// latency. `checkpoint()` runs on the writer thread (serialized with mutations, never
/// contending), and a `busy` result means a reader pinned the log so the truncation was
/// deferred — that is observable via `cairn_wal_checkpoints_busy_total`.
async fn checkpoint_loop(stack: Arc<AppStack>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        match stack.store.checkpoint().await {
            Ok(stats) => {
                metrics::counter!("cairn_wal_checkpoints_total").increment(1);
                if stats.busy {
                    metrics::counter!("cairn_wal_checkpoints_busy_total").increment(1);
                }
                metrics::counter!("cairn_wal_checkpointed_frames_total")
                    .increment(stats.checkpointed_frames);
                tracing::debug!(
                    busy = stats.busy,
                    log_frames = stats.log_frames,
                    checkpointed_frames = stats.checkpointed_frames,
                    "wal checkpoint complete"
                );
            }
            Err(e) => tracing::warn!(error = %e, "wal checkpoint failed"),
        }
        match stack.store.wal_size_bytes().await {
            Ok(bytes) => metrics::gauge!("cairn_wal_bytes").set(bytes as f64),
            Err(e) => tracing::warn!(error = %e, "wal size probe failed"),
        }
    }
}

/// Refresh the store gauges (object/bucket/byte counts and compression ratio) from the metadata
/// aggregate on a short interval, so `/metrics` reflects live state.
async fn metrics_loop(stack: Arc<AppStack>) {
    loop {
        tokio::time::sleep(Duration::from_secs(15)).await;
        if let Ok(c) = stack.meta.aggregate_counts().await {
            metrics::gauge!("cairn_buckets").set(c.buckets as f64);
            metrics::gauge!("cairn_objects").set(c.objects as f64);
            metrics::gauge!("cairn_versions").set(c.versions as f64);
            metrics::gauge!("cairn_logical_bytes").set(c.logical_bytes as f64);
            metrics::gauge!("cairn_physical_bytes").set(c.physical_bytes as f64);
            let ratio = if c.physical_bytes > 0 {
                c.logical_bytes as f64 / c.physical_bytes as f64
            } else {
                1.0
            };
            metrics::gauge!("cairn_compression_ratio").set(ratio);
        }
    }
}

/// Periodically abort multipart sessions idle beyond their lifetime and reclaim their parts.
async fn sweeper_loop(stack: Arc<AppStack>, interval: Duration, lifetime_secs: i64) {
    let clock = SystemClock::new();
    loop {
        tokio::time::sleep(interval).await;
        let cutoff = clock.now().plus_secs(-lifetime_secs);
        match stack.meta.enumerate_stale_sessions(cutoff, 1000).await {
            Ok(stale) => {
                let n = stale.len();
                for s in stale {
                    let _ = stack
                        .meta
                        .submit(Mutation::AbortMultipart(s.upload_id.clone()))
                        .await;
                    let _ = stack.blob.delete_session(&s.upload_id).await;
                }
                if n > 0 {
                    tracing::info!(aborted = n, "multipart sweeper reclaimed stale uploads");
                }
            }
            Err(e) => tracing::warn!(error = %e, "multipart sweep failed"),
        }
    }
}

/// Periodically apply each bucket's lifecycle rules.
async fn lifecycle_loop(stack: Arc<AppStack>, interval: Duration) {
    let scanner = LifecycleScanner::new();
    let clock = SystemClock::new();
    loop {
        tokio::time::sleep(interval).await;
        let buckets = match stack.meta.list_buckets(None).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "lifecycle: listing buckets failed");
                continue;
            }
        };
        let mut configs = Vec::new();
        for b in buckets {
            if let Ok(Some(doc)) = stack
                .meta
                .get_bucket_config(&b.name, ConfigAspect::Lifecycle)
                .await
            {
                if let Ok(rules) = cairn_lifecycle::parse_lifecycle(doc.0.as_bytes()) {
                    if !rules.is_empty() {
                        configs.push(BucketLifecycle::new(b.name, rules));
                    }
                }
            }
        }
        if configs.is_empty() {
            continue;
        }
        match scanner
            .run_once(&*stack.meta, &*stack.blob, &clock, &configs)
            .await
        {
            Ok(report) => tracing::info!(?report, "lifecycle scan complete"),
            Err(e) => tracing::warn!(error = %e, "lifecycle scan failed"),
        }
    }
}
