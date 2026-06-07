//! Background subsystems (ARCH §6.4): the multipart-upload sweeper, the lifecycle scanner, the
//! WAL checkpointer, and the store-metrics refresher. Each runs on a configurable interval
//! against the shared engine stack. Replication workers are wired once a remote sink is
//! configured.

use crate::config::{Config, ReplicationTarget};
use crate::stack::AppStack;
use cairn_crypto::SystemClock;
use cairn_lifecycle::{BucketLifecycle, LifecycleScanner};
use cairn_replication::{BucketRoutedSink, HttpS3Sink};
use cairn_types::bucket::ConfigAspect;
use cairn_types::error::ReplicationError;
use cairn_types::id::{BucketName, ObjectKey, VersionId};
use cairn_types::meta::Mutation;
use cairn_types::replication::ReplicatedObject;
use cairn_types::traits::Clock;
use std::collections::HashMap;
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
    // The WAL checkpointer drives inherent methods on the concrete `SqliteMetadataStore`, so it
    // runs only for the `sqlite` backend (where `stack.store` is `Some`). The libSQL and Turso
    // engines self-manage their WAL, so the loop is simply not spawned for them.
    if stack.store.is_some() {
        tokio::spawn(checkpoint_loop(stack.clone(), checkpoint_interval));
    } else {
        tracing::info!(
            "WAL checkpointer disabled: the active metadata backend self-manages its WAL"
        );
    }

    // Replication worker. Two shapes, chosen by configuration:
    //
    //  * MULTI-TARGET — `CAIRN_REPLICATION_TARGETS` names a set of destinations, each with its own
    //    endpoint, credentials, and TLS trust. Each source bucket is routed to the target whose
    //    `dest_bucket` (or `name`) matches the bucket's replication rule, and shipped through that
    //    target's own sink. The single-target `CAIRN_REPLICATION_*` keys, if present, build a
    //    default sink used for any source bucket that matches no named target.
    //
    //  * SINGLE-TARGET (default) — the original node->node path: one endpoint + credentials, with
    //    the per-source destination *bucket* resolved each drain from each bucket's replication
    //    rule. Unchanged.
    //
    // In both shapes outbox entries accumulate (never silently dropped) until a sink is configured
    // (ARCH §20).
    let interval = Duration::from_secs(cfg.replication_interval_secs);
    let targets = cfg.parse_replication_targets().unwrap_or_default();
    if !targets.is_empty() {
        let default_cfg = single_target_sink_cfg(cfg);
        tokio::spawn(multi_target_replication_loop(
            stack.clone(),
            targets,
            default_cfg,
            interval,
        ));
        tracing::info!("replication worker enabled (multi-target)");
    } else if let Some(sink_cfg) = single_target_sink_cfg(cfg) {
        tokio::spawn(replication_loop(stack.clone(), sink_cfg, interval));
        tracing::info!("replication worker enabled");
    }
    tokio::spawn(metrics_loop(stack));
}

/// Build the single-target sink configuration from the `CAIRN_REPLICATION_*` keys, or `None` when
/// the endpoint/credentials triple is not fully configured. The `dest_bucket` is OPTIONAL because
/// the per-source destination is normally resolved from each bucket's replication rule each drain.
fn single_target_sink_cfg(cfg: &Config) -> Option<cairn_replication::S3SinkConfig> {
    match (
        cfg.replication_endpoint.clone(),
        cfg.replication_access_key.clone(),
        cfg.replication_secret.clone(),
    ) {
        (Some(endpoint), Some(access), Some(secret)) => Some(cairn_replication::S3SinkConfig {
            endpoint,
            dest_bucket: cfg.replication_dest_bucket.clone().unwrap_or_default(),
            // Populated per drain from each source bucket's replication rule.
            dest_buckets: HashMap::new(),
            region: cfg
                .replication_region
                .clone()
                .unwrap_or_else(|| cfg.region.clone()),
            access_key_id: access,
            secret_access_key: secret,
            ca_cert_path: None,
            insecure_skip_verify: false,
        }),
        _ => None,
    }
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
async fn resolve_dest_buckets(stack: &Arc<AppStack>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let buckets = match stack.meta.list_buckets(None).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "replication: listing buckets for dest resolution failed");
            return map;
        }
    };
    for b in buckets {
        if let Some(dest) = bucket_rule_dest(stack, &b.name).await {
            map.insert(b.name.as_str().to_owned(), dest);
        }
    }
    map
}

/// Read a single bucket's first enabled replication rule's destination bucket (ARN prefix
/// stripped), or `None` when the bucket has no replication config, an unparseable document, or no
/// enabled rule naming a destination.
async fn bucket_rule_dest(stack: &Arc<AppStack>, bucket: &BucketName) -> Option<String> {
    let doc = stack
        .meta
        .get_bucket_config(bucket, ConfigAspect::Replication)
        .await
        .ok()??;
    let cfg = cairn_replication::parse_replication(doc.0.as_bytes()).ok()?;
    cfg.rules
        .iter()
        .find(|r| r.enabled)
        .and_then(|r| r.destination.bucket())
        .map(ToOwned::to_owned)
}

/// Drain the replication outbox across many named targets on an interval (ARCH §20). Each source
/// bucket is routed to the target whose `dest_bucket` (or, failing that, `name`) matches the
/// bucket's stored replication rule; objects ship through that target's own sink (its endpoint,
/// credentials, and TLS trust). A source bucket matching no named target falls back to the
/// single-target `default_cfg` sink when one is configured.
///
/// Per-target sinks are built once up front (the connector is the only non-trivial cost and is
/// stable for a target's lifetime); only the cheap `source bucket -> target` routing map is
/// rebuilt each drain from the current bucket rules.
async fn multi_target_replication_loop(
    stack: Arc<AppStack>,
    targets: Vec<ReplicationTarget>,
    default_cfg: Option<cairn_replication::S3SinkConfig>,
    interval: Duration,
) {
    let engine =
        cairn_replication::ReplicationEngine::new(cairn_replication::ReplicationOpts::default());
    let clock = SystemClock::new();

    // Build a sink per named target once. A target whose sink fails to construct (a bad endpoint,
    // an unreadable CA bundle, conflicting trust knobs) is logged and dropped; the rest still run.
    let mut target_sinks: Vec<(ReplicationTarget, Arc<HttpS3Sink>)> = Vec::new();
    for target in targets {
        match HttpS3Sink::new(target_sink_cfg(&target)) {
            Ok(sink) => target_sinks.push((target, Arc::new(sink))),
            Err(e) => {
                tracing::error!(target = %target.name, error = %e,
                    "replication target sink construction failed; target disabled");
            }
        }
    }
    let default_sink = match default_cfg {
        Some(cfg) => match HttpS3Sink::new(cfg) {
            Ok(s) => Some(Arc::new(s)),
            Err(e) => {
                tracing::error!(error = %e, "default replication sink construction failed");
                None
            }
        },
        None => None,
    };

    if target_sinks.is_empty() && default_sink.is_none() {
        tracing::error!("no usable replication targets; replication worker idle");
        return;
    }

    loop {
        tokio::time::sleep(interval).await;

        // Resolve `source bucket -> target sink` from the current bucket rules each drain.
        let routes = resolve_target_routes(&stack, &target_sinks).await;
        let sink = MultiTargetSink {
            routes,
            default: default_sink.clone(),
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
                    "replication progressed (multi-target)"
                );
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "replication run failed"),
        }
    }
}

/// Convert a configured [`ReplicationTarget`] into the sink configuration for its dedicated
/// [`HttpS3Sink`]. The target is a single fixed destination, so `dest_buckets` stays empty and the
/// target's `dest_bucket` is the one destination; its TLS trust knobs are carried through.
fn target_sink_cfg(target: &ReplicationTarget) -> cairn_replication::S3SinkConfig {
    cairn_replication::S3SinkConfig {
        endpoint: target.endpoint.clone(),
        dest_bucket: target.dest_bucket.clone(),
        dest_buckets: HashMap::new(),
        region: target.region.clone(),
        access_key_id: target.access_key.clone(),
        secret_access_key: target.secret.clone(),
        ca_cert_path: target.ca_path.clone(),
        insecure_skip_verify: target.insecure_skip_verify,
    }
}

/// Resolve the `source bucket name -> target sink` routing for this drain. For each bucket with an
/// enabled replication rule, the destination bucket the rule names is matched against each target's
/// `dest_bucket` first, then its `name`; the first match wins. Buckets that match no target are
/// omitted so they fall back to the default sink.
async fn resolve_target_routes(
    stack: &Arc<AppStack>,
    target_sinks: &[(ReplicationTarget, Arc<HttpS3Sink>)],
) -> HashMap<String, Arc<HttpS3Sink>> {
    let mut routes = HashMap::new();
    let buckets = match stack.meta.list_buckets(None).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "replication: listing buckets for target routing failed");
            return routes;
        }
    };
    for b in buckets {
        let Some(dest) = bucket_rule_dest(stack, &b.name).await else {
            continue;
        };
        if let Some(sink) = match_target(target_sinks, &dest) {
            routes.insert(b.name.as_str().to_owned(), sink);
        } else {
            tracing::warn!(
                bucket = %b.name.as_str(),
                destination = %dest,
                "replication: no named target matches this bucket's destination; using default"
            );
        }
    }
    routes
}

/// Pick the target sink for a destination named by a bucket's replication rule: match the target's
/// `dest_bucket` first, then its `name`. Returns a clone of the matched sink handle, or `None`.
fn match_target(
    target_sinks: &[(ReplicationTarget, Arc<HttpS3Sink>)],
    dest: &str,
) -> Option<Arc<HttpS3Sink>> {
    target_sinks
        .iter()
        .find(|(t, _)| t.dest_bucket == dest || t.name == dest)
        .map(|(_, sink)| Arc::clone(sink))
}

/// A [`BucketRoutedSink`] over many per-target [`HttpS3Sink`]s plus an optional default. Each call
/// dispatches on the source bucket: a routed bucket ships through its target's sink, an unrouted
/// bucket through the default. With no route and no default the entry is a terminal failure (it
/// names a destination no configured target serves), surfaced for operator attention rather than
/// silently dropped.
struct MultiTargetSink {
    routes: HashMap<String, Arc<HttpS3Sink>>,
    default: Option<Arc<HttpS3Sink>>,
}

impl MultiTargetSink {
    /// Resolve the sink for a source bucket: its explicit route, else the default.
    fn sink_for(&self, source_bucket: &str) -> Result<&Arc<HttpS3Sink>, ReplicationError> {
        self.routes
            .get(source_bucket)
            .or(self.default.as_ref())
            .ok_or_else(|| {
                ReplicationError::Terminal(format!(
                    "no replication target for source bucket {source_bucket:?}"
                ))
            })
    }
}

#[async_trait::async_trait]
impl BucketRoutedSink for MultiTargetSink {
    async fn put_object(
        &self,
        source_bucket: &BucketName,
        object: ReplicatedObject,
    ) -> Result<(), ReplicationError> {
        self.sink_for(source_bucket.as_str())?
            .put_object(source_bucket, object)
            .await
    }

    async fn delete_marker(
        &self,
        source_bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<(), ReplicationError> {
        self.sink_for(source_bucket.as_str())?
            .delete_marker(source_bucket, key, version)
            .await
    }
}

/// Periodically run a truncating WAL checkpoint on the metadata store and publish the WAL size
/// and checkpoint stats as metrics (ARCH §8.4/§11.2, F-3). Without this the `-wal` file can grow
/// unbounded under sustained writes with a long-lived reader, inflating disk use and read
/// latency. `checkpoint()` runs on the writer thread (serialized with mutations, never
/// contending), and a `busy` result means a reader pinned the log so the truncation was
/// deferred — that is observable via `cairn_wal_checkpoints_busy_total`.
async fn checkpoint_loop(stack: Arc<AppStack>, interval: Duration) {
    // Only spawned when `store` is Some (the sqlite backend); bind the typed handle once.
    let Some(store) = stack.store.clone() else {
        return;
    };
    loop {
        tokio::time::sleep(interval).await;
        match store.checkpoint().await {
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
        match store.wal_size_bytes().await {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn target(name: &str, dest_bucket: &str, endpoint: &str) -> ReplicationTarget {
        ReplicationTarget {
            name: name.to_owned(),
            endpoint: endpoint.to_owned(),
            region: "us-east-1".to_owned(),
            dest_bucket: dest_bucket.to_owned(),
            access_key: "AKID".to_owned(),
            secret: "secret".to_owned(),
            ca_path: None,
            insecure_skip_verify: false,
        }
    }

    fn sinks(targets: &[ReplicationTarget]) -> Vec<(ReplicationTarget, Arc<HttpS3Sink>)> {
        targets
            .iter()
            .map(|t| {
                let sink = HttpS3Sink::new(target_sink_cfg(t)).expect("target sink builds");
                (t.clone(), Arc::new(sink))
            })
            .collect()
    }

    /// A bucket's destination is matched to a target by its `dest_bucket`, then by `name`.
    #[test]
    fn match_target_by_dest_bucket_then_name() {
        let targets = [
            target("west", "mirror-west", "https://s3.west.example"),
            target("east", "mirror-east", "http://s3.east.example:9000"),
        ];
        let built = sinks(&targets);

        // Match by destination bucket name.
        let m = match_target(&built, "mirror-west").expect("dest-bucket match");
        assert_eq!(m.dest_for("any"), "mirror-west");

        // Match by target name (a rule that names the target rather than the bucket).
        let m = match_target(&built, "east").expect("name match");
        assert_eq!(m.dest_for("any"), "mirror-east");

        // No match for an unknown destination.
        assert!(match_target(&built, "nowhere").is_none());
    }

    /// `target_sink_cfg` carries the target's endpoint, credentials, and TLS trust knobs through to
    /// the sink config, with an empty per-source map (a target is one fixed destination).
    #[test]
    fn target_sink_cfg_carries_trust_knobs() {
        let mut t = target("secure", "mirror", "https://s3.secure.example");
        t.insecure_skip_verify = true;
        let cfg = target_sink_cfg(&t);
        assert_eq!(cfg.endpoint, "https://s3.secure.example");
        assert_eq!(cfg.dest_bucket, "mirror");
        assert!(cfg.dest_buckets.is_empty());
        assert!(cfg.insecure_skip_verify);
        assert!(cfg.ca_cert_path.is_none());

        let mut t = target("ca", "mirror", "https://s3.ca.example");
        t.ca_path = Some(std::path::PathBuf::from("/etc/ca.pem"));
        let cfg = target_sink_cfg(&t);
        assert_eq!(
            cfg.ca_cert_path,
            Some(std::path::PathBuf::from("/etc/ca.pem"))
        );
        assert!(!cfg.insecure_skip_verify);
    }

    /// The multi-target sink routes a known source bucket to its target sink and an unknown one to
    /// the default; with neither, resolution is a terminal error (never a silent drop).
    #[test]
    fn multi_target_sink_routes_and_falls_back() {
        let targets = [target("west", "mirror-west", "https://s3.west.example")];
        let built = sinks(&targets);
        let west = Arc::clone(&built[0].1);
        let default = Arc::new(
            HttpS3Sink::new(cairn_replication::S3SinkConfig {
                endpoint: "http://default.example:9000".to_owned(),
                dest_bucket: "fallback".to_owned(),
                dest_buckets: HashMap::new(),
                region: "us-east-1".to_owned(),
                access_key_id: "AKID".to_owned(),
                secret_access_key: "secret".to_owned(),
                ca_cert_path: None,
                insecure_skip_verify: false,
            })
            .unwrap(),
        );

        let mut routes = HashMap::new();
        routes.insert("logs".to_owned(), Arc::clone(&west));
        let sink = MultiTargetSink {
            routes,
            default: Some(default),
        };

        // Routed bucket -> its target sink; unrouted -> the default sink.
        assert_eq!(sink.sink_for("logs").unwrap().dest_for("x"), "mirror-west");
        assert_eq!(sink.sink_for("other").unwrap().dest_for("x"), "fallback");

        // With no default, an unrouted bucket is a terminal failure.
        let sink = MultiTargetSink {
            routes: HashMap::new(),
            default: None,
        };
        let err = sink.sink_for("orphan").unwrap_err();
        assert!(matches!(err, ReplicationError::Terminal(_)));
    }

    /// `single_target_sink_cfg` yields `None` until the endpoint/credentials triple is complete,
    /// keeping the worker idle (outbox accumulating) rather than half-configured.
    #[test]
    fn single_target_cfg_requires_full_triple() {
        let mut cfg = Config::default();
        assert!(single_target_sink_cfg(&cfg).is_none());
        cfg.replication_endpoint = Some("http://backup:9000".to_owned());
        assert!(single_target_sink_cfg(&cfg).is_none());
        cfg.replication_access_key = Some("AKID".to_owned());
        assert!(single_target_sink_cfg(&cfg).is_none());
        cfg.replication_secret = Some("secret".to_owned());
        let built = single_target_sink_cfg(&cfg).expect("full triple yields a config");
        assert_eq!(built.endpoint, "http://backup:9000");
        // The TLS trust defaults are the safe webpki path for the single-target node->node case.
        assert!(built.ca_cert_path.is_none());
        assert!(!built.insecure_skip_verify);
    }
}
