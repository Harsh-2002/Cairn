//! Background subsystems (ARCH §6.4): the multipart-upload sweeper and the lifecycle scanner.
//! Each runs on an interval against the shared engine stack. The WAL checkpointer is managed
//! inside `cairn-meta`; replication workers are wired once a remote sink is configured.

use crate::stack::AppStack;
use cairn_crypto::SystemClock;
use cairn_lifecycle::{BucketLifecycle, LifecycleScanner};
use cairn_types::bucket::ConfigAspect;
use cairn_types::meta::Mutation;
use cairn_types::traits::Clock;
use std::sync::Arc;
use std::time::Duration;

/// Spawn the background tasks.
pub fn spawn(
    stack: Arc<AppStack>,
    sweep_interval: Duration,
    multipart_lifetime_secs: i64,
    lifecycle_interval: Duration,
) {
    tokio::spawn(sweeper_loop(
        stack.clone(),
        sweep_interval,
        multipart_lifetime_secs,
    ));
    tokio::spawn(lifecycle_loop(stack.clone(), lifecycle_interval));
    tokio::spawn(metrics_loop(stack));
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
