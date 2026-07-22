//! Background subsystems (ARCH 6.4): the multipart-upload sweeper, the lifecycle scanner, the
//! WAL checkpointer, and the store-metrics refresher. Each runs on a configurable interval
//! against the shared engine stack. Replication workers are wired once a remote sink is
//! configured.

use crate::config::{Config, ReplicationTarget};
use crate::stack::AppStack;
use cairn_crypto::SystemClock;
use cairn_lifecycle::{BucketLifecycle, LifecycleScanner};
use cairn_replication::{BucketRoutedSink, HttpS3Sink, SinkRouter};
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
/// configured 28.2 knobs. `shutdown` is the server's graceful-shutdown signal; the replication
/// worker pool watches it *between* drain passes, so a worker stops claiming NEW work as soon as
/// shutdown begins. A pass already in flight is not joined on shutdown — the runtime drop may
/// cancel it mid-ship — but that is safe: an aborted claim is recovered by the outbox lease and the
/// object re-ships at-least-once on the next start (durability rests on the durable outbox, not on a
/// clean worker join).
pub fn spawn(stack: Arc<AppStack>, cfg: &Config, shutdown: tokio::sync::watch::Receiver<bool>) {
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
    tokio::spawn(webhook_loop(
        stack.clone(),
        Duration::from_secs(cfg.webhook_interval_secs),
    ));
    // The S3-import worker: claims pending import jobs and runs them into this node.
    tokio::spawn(crate::import_run::import_loop(
        stack.clone(),
        crate::import_run::ImportLoopConfig {
            poll_interval_secs: cfg.import_poll_interval_secs,
            default_workers: cfg.import_default_workers,
            max_workers: cfg.import_max_workers,
            global_max_inflight: cfg.import_global_max_inflight,
            root_access_key: cfg.root_access_key.clone(),
        },
        stack.import_notify.clone(),
        shutdown.clone(),
    ));
    // The integrity scrub is opt-in (I/O-heavy): only spawned when an interval is configured.
    if cfg.scrub_interval_secs > 0 {
        tokio::spawn(scrub_loop(
            stack.clone(),
            Duration::from_secs(cfg.scrub_interval_secs),
        ));
    }
    // The WAL checkpointer drives inherent methods on the concrete `SqliteMetadataStore`, so it
    // runs only for the `sqlite` backend (where `stack.store` holds one handle per shard). The
    // libSQL and Turso engines self-manage their WAL, so the loop is not spawned for them.
    if !stack.store.is_empty() {
        tokio::spawn(checkpoint_loop(
            stack.clone(),
            checkpoint_interval,
            cfg.wal_checkpoint_size_bytes,
        ));
        // Master-key re-wrap + seal-count flush (audit #29, Phase D/E), one per sqlite shard,
        // sharing the one master-key ring. Disabled when the interval is 0.
        for store in &stack.store {
            crate::key_rewrap::spawn(
                store.clone(),
                stack.crypto.clone(),
                stack.meta_cache.clone(),
                cfg.key_rewrap_interval_secs,
            );
            crate::key_rewrap::spawn_counter_sync(
                store.clone(),
                stack.crypto.clone(),
                cfg.key_counter_sync_secs,
            );
        }
    } else {
        tracing::info!(
            "WAL checkpointer disabled: the active metadata backend self-manages its WAL"
        );
    }

    // Replication worker POOL. Three shapes, chosen by configuration:
    //
    //  * MULTI-TARGET — `CAIRN_REPLICATION_TARGETS` names a set of destinations, each shipped
    //    through its own sink; the single-target `CAIRN_REPLICATION_*` keys, if present, build a
    //    default sink for any source bucket matching no named target.
    //  * SINGLE-TARGET — the original node->node path: one endpoint + credentials, per-source
    //    destination bucket resolved each drain from each bucket's rule.
    //  * PER-BUCKET STORED TARGETS (default) — no env sink; destinations come from each bucket's
    //    sealed `ConfigAspect::ReplicationTargets`, discovered fresh each drain.
    //
    // `replication_worker_concurrency` tasks run the chosen shape concurrently; per-key, per-target
    // ordering is preserved by the durable claim lease + predecessor check regardless of pool size.
    // Each worker is event-driven (a write-path pulse on `stack.replication_notify`) with the
    // interval as a safety-net heartbeat, and stops on the shutdown signal. Outbox entries accumulate
    // (never silently dropped) until a sink is configured (ARCH 20).
    let interval = Duration::from_secs(cfg.replication_interval_secs);
    let opts = cairn_replication::ReplicationOpts {
        batch_size: cfg.replication_batch_size,
        max_attempts: cfg.replication_max_attempts,
        base_backoff_secs: cfg.replication_base_backoff_secs,
        max_backoff_secs: cfg.replication_max_backoff_secs,
    };
    let concurrency = cfg.replication_worker_concurrency.max(1);
    let targets = cfg.parse_replication_targets().unwrap_or_default();
    let single_cfg = single_target_sink_cfg(cfg);
    let shape = if !targets.is_empty() {
        "multi-target"
    } else if single_cfg.is_some() {
        "single-target"
    } else {
        "per-bucket stored targets"
    };
    for _ in 0..concurrency {
        let notify = stack.replication_notify.clone();
        let sd = shutdown.clone();
        if !targets.is_empty() {
            tokio::spawn(multi_target_replication_loop(
                stack.clone(),
                targets.clone(),
                single_cfg.clone(),
                interval,
                notify,
                sd,
                opts,
            ));
        } else if let Some(sink_cfg) = single_cfg.clone() {
            tokio::spawn(replication_loop(
                stack.clone(),
                sink_cfg,
                interval,
                notify,
                sd,
                opts,
            ));
        } else {
            tokio::spawn(multi_target_replication_loop(
                stack.clone(),
                Vec::new(),
                None,
                interval,
                notify,
                sd,
                opts,
            ));
        }
    }
    tracing::info!(
        workers = concurrency,
        shape,
        "replication worker pool enabled"
    );
    // Reclaim terminal outbox rows so the table stays a bounded work queue (ARCH 20.3): completed
    // rows carry no further information and would otherwise accumulate one-per-replicated-object
    // forever, and genuinely-stale failures are auto-cleared.
    tokio::spawn(replication_prune_loop(
        stack.clone(),
        cfg.replication_retention_secs,
    ));
    // Reclaim terminally-failed webhook-outbox rows so a dead/misconfigured sink can't bloat the
    // metadata DB without bound (audit 2026-07; ARCH 20.3 bounded-work-queue contract).
    tokio::spawn(events_outbox_prune_loop(
        stack.clone(),
        cfg.events_outbox_retention_secs,
    ));
    // Request-metrics flush loop (ARCH 26.5). Gated on the subsystem being enabled: when off, the
    // hot path accumulates nothing and there is nothing to flush. Otherwise it periodically drains
    // the in-process aggregator into a batched upsert and prunes rows past the retention horizon.
    if cfg.request_metrics_enabled {
        let flush_interval = Duration::from_secs(cfg.request_metrics_flush_secs.max(1));
        #[allow(clippy::cast_possible_wrap)]
        let retention_secs = (cfg.request_metrics_retention_days as i64) * 86_400;
        tokio::spawn(request_metrics_flush_loop(
            stack.clone(),
            flush_interval,
            retention_secs,
        ));
        tracing::info!("request-metrics ingestion enabled");
    }

    tokio::spawn(metrics_loop(stack));
}

/// Periodically flush the in-process request-metrics aggregator into the rollup table and prune
/// rows past the retention horizon (ARCH 26.5). Each tick drains the accumulated counts and submits
/// a single `RecordRequestMetrics` mutation through the single writer — the only DB touch the
/// request-metrics subsystem makes, keeping the request hot path free of any DB I/O. `prune_before`
/// is always supplied so old rows are reclaimed even on idle ticks, but a submit is skipped entirely
/// when there is no traffic to flush to avoid a pointless write each interval.
async fn request_metrics_flush_loop(stack: Arc<AppStack>, interval: Duration, retention_secs: i64) {
    let clock = SystemClock::new();
    loop {
        tokio::time::sleep(interval).await;
        let rows = stack.request_metrics.drain();
        let now_secs = clock.now().as_secs();
        let prune_before = Some(now_secs - retention_secs);
        // Skip the write on a fully idle tick (no rows). The next tick with traffic carries the
        // prune, so retention is still bounded; a long-idle table simply prunes a little later.
        if rows.is_empty() {
            continue;
        }
        if let Err(e) = stack
            .meta
            .submit(Mutation::RecordRequestMetrics { rows, prune_before })
            .await
        {
            tracing::warn!(error = %e, "request metrics flush failed");
        }
    }
}

/// Periodically reclaim terminal replication-outbox rows (completed/failed) older than the retention
/// horizon, so the durable work queue stays bounded instead of growing one row per replicated object
/// forever (ARCH 20.3). Pending/claimed entries are never pruned. Runs on a calm cadence — the table
/// only needs to stay bounded, not be trimmed instantly — and pruning is idempotent, so a tick missed
/// on shutdown is harmless.
async fn replication_prune_loop(stack: Arc<AppStack>, retention_secs: u64) {
    let clock = SystemClock::new();
    let interval = Duration::from_secs(retention_secs.clamp(60, 3600));
    let retention_ms = (retention_secs as i64).saturating_mul(1000);
    loop {
        tokio::time::sleep(interval).await;
        let before_ms = clock.now().as_millis().saturating_sub(retention_ms);
        if let Err(e) = stack
            .meta
            .submit(Mutation::PruneReplicationOutbox { before_ms })
            .await
        {
            tracing::warn!(error = %e, "replication outbox prune failed");
        }
    }
}

/// Periodically reclaim terminally-failed webhook-outbox (`events_outbox`) rows older than the
/// retention horizon, so a misconfigured or decommissioned webhook sink cannot grow the metadata DB
/// one permanent failed-event row at a time (audit 2026-07; ARCH 20.3). Delivered rows are removed on
/// delivery and pending/claimed work is never pruned; the same calm-cadence, idempotent design as
/// [`replication_prune_loop`].
async fn events_outbox_prune_loop(stack: Arc<AppStack>, retention_secs: u64) {
    let clock = SystemClock::new();
    let interval = Duration::from_secs(retention_secs.clamp(60, 3600));
    let retention_ms = (retention_secs as i64).saturating_mul(1000);
    loop {
        tokio::time::sleep(interval).await;
        let before_ms = clock.now().as_millis().saturating_sub(retention_ms);
        if let Err(e) = stack
            .meta
            .submit(Mutation::PruneEventsOutbox { before_ms })
            .await
        {
            tracing::warn!(error = %e, "events outbox prune failed");
        }
    }
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
            ca_cert_pem: None,
            insecure_skip_verify: false,
            allow_internal_endpoints: cfg.allow_internal_endpoints,
        }),
        _ => None,
    }
}

/// Drain the replication outbox to the configured remote sink on an interval (ARCH 20).
///
/// `base_cfg` carries the endpoint, credentials, region, and the *default* destination bucket.
/// Before each drain the per-source-bucket destination map is rebuilt from every bucket's stored
/// replication rule (`ConfigAspect::Replication` → [`parse_replication`] → the rule's
/// `<Destination><Bucket>` with the `arn:aws:s3:::` prefix stripped), so each source bucket's
/// objects ship to the destination its own rule names; a bucket with no explicit destination
/// falls back to `replication_dest_bucket`. The sink is rebuilt per drain with the fresh map
/// (its connector is cheap to construct), keeping the node→node single-destination path working
/// when no per-bucket rule is present.
/// Block until the next replication drain pass is due: a write-path pulse (`notify`), the heartbeat
/// `interval`, or the shutdown signal. Returns `true` to drain, `false` to stop the worker.
async fn wait_for_drain_trigger(
    interval: Duration,
    notify: &tokio::sync::Notify,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    if *shutdown.borrow() {
        return false;
    }
    tokio::select! {
        () = notify.notified() => true,
        () = tokio::time::sleep(interval) => true,
        _ = shutdown.changed() => false,
    }
}

async fn replication_loop(
    stack: Arc<AppStack>,
    base_cfg: cairn_replication::S3SinkConfig,
    interval: Duration,
    notify: Arc<tokio::sync::Notify>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    opts: cairn_replication::ReplicationOpts,
) {
    let engine = cairn_replication::ReplicationEngine::new(opts);
    let clock = SystemClock::new();
    while wait_for_drain_trigger(interval, &notify, &mut shutdown).await {
        // Resolve the per-source destination map from each bucket's replication rule.
        let dest_buckets = resolve_dest_buckets(&stack).await;
        let mut sink_cfg = base_cfg.clone();
        sink_cfg.dest_buckets = dest_buckets;
        let default_sink = match cairn_replication::HttpS3Sink::new(sink_cfg) {
            Ok(s) => Some(Arc::new(s)),
            Err(e) => {
                tracing::error!(error = %e, "replication sink construction failed; skipping drain");
                continue;
            }
        };

        // Build the router for this drain: stored per-bucket remote targets take precedence; any
        // bucket without one falls back to this env-configured default sink (the unchanged
        // node->node path).
        let stored = resolve_stored_target_sinks(&stack).await;
        let router = build_router(default_sink, &stored);
        drain_with_router(&engine, &stack, &router, &clock).await;
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

/// Drain the replication outbox across many named targets on an interval (ARCH 20). Each source
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
    notify: Arc<tokio::sync::Notify>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    opts: cairn_replication::ReplicationOpts,
) {
    let engine = cairn_replication::ReplicationEngine::new(opts);
    let clock = SystemClock::new();

    // Build a sink per named target once. A target whose sink fails to construct (a bad endpoint,
    // an unreadable CA bundle, conflicting trust knobs) is logged and dropped; the rest still run.
    let mut target_sinks: Vec<(ReplicationTarget, Arc<HttpS3Sink>)> = Vec::new();
    for target in targets {
        match HttpS3Sink::new(target_sink_cfg(&target, stack.allow_internal_endpoints)) {
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
        // No env sinks — this is the stored-targets-only shape. Do NOT bail: per-bucket stored
        // remote targets are resolved from bucket config on every drain below (ARCH 20).
        tracing::debug!("no env replication sinks; serving per-bucket stored targets only");
    }

    while wait_for_drain_trigger(interval, &notify, &mut shutdown).await {
        // Resolve `source bucket -> target sink` from the current bucket rules each drain. Stored
        // per-bucket remote targets are layered on top and win over the env-named targets.
        let routes = resolve_target_routes(&stack, &target_sinks).await;
        let stored = resolve_stored_target_sinks(&stack).await;
        // `routes` are the env-named per-source routes; fold them in over the stored ones.
        let router = build_router(default_sink.clone(), &stored).with_env_routes(routes);
        drain_with_router(&engine, &stack, &router, &clock).await;
    }
}

/// Run one drain pass through the engine with the assembled router, publishing the replication
/// progress + bytes metrics. Centralises the run/report handling shared by both worker shapes.
async fn drain_with_router(
    engine: &cairn_replication::ReplicationEngine,
    stack: &Arc<AppStack>,
    router: &StoredTargetRouter,
    clock: &SystemClock,
) {
    match engine
        .run_until_idle(&*stack.meta, router, &stack.blob, clock, 50)
        .await
    {
        Ok(report) if !report.is_idle() => {
            metrics::counter!("cairn_replication_completed_total")
                .increment(report.completed as u64);
            metrics::counter!("cairn_replication_failed_total").increment(report.failed as u64);
            metrics::counter!("cairn_replication_bytes_total").increment(report.bytes);
            tracing::info!(
                completed = report.completed,
                failed = report.failed,
                bytes = report.bytes,
                "replication progressed"
            );
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(error = %e, "replication run failed"),
    }
}

/// Resolve the `target-ARN -> built sink` map from every bucket's stored remote replication targets
/// (`ConfigAspect::ReplicationTargets`, ARCH 20.5). Each stored [`RemoteTarget`] is unsealed under
/// the master key and built into an [`HttpS3Sink`] keyed by its ARN, so a drained outbox entry —
/// which carries the ARN its matching rule named at enqueue — routes to exactly its destination.
/// Keying by ARN (rather than by source bucket) is what lets one bucket fan out to several distinct
/// targets by rule/priority/filter.
///
/// Sinks are keyed and rebuilt per drain. Building a sink is cheap (the connector is the only real
/// cost) and the target set is small, so this keeps a fresh view of operator edits each pass without
/// a long-lived cache to invalidate. An entry whose ARN resolves to no sink here is terminated by
/// the engine (target removed), rather than silently misrouted.
async fn resolve_stored_target_sinks(stack: &Arc<AppStack>) -> HashMap<String, Arc<HttpS3Sink>> {
    let mut by_arn: HashMap<String, Arc<HttpS3Sink>> = HashMap::new();
    let buckets = match stack.meta.list_buckets(None).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "replication: listing buckets for stored-target resolution failed");
            return by_arn;
        }
    };
    for b in buckets {
        let doc = match stack
            .meta
            .get_bucket_config(&b.name, ConfigAspect::ReplicationTargets)
            .await
        {
            Ok(Some(doc)) => doc,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(bucket = %b.name.as_str(), error = %e,
                    "replication: reading stored targets failed");
                continue;
            }
        };
        let targets = match cairn_replication::parse_targets(doc.0.as_bytes()) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(bucket = %b.name.as_str(), error = %e,
                    "replication: parsing stored targets failed");
                continue;
            }
        };
        // Build a sink for every distinct target ARN (a bucket may name several).
        for target in &targets {
            if by_arn.contains_key(&target.arn) {
                continue;
            }
            let open = match cairn_replication::open_target(&stack.crypto, target) {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(arn = %target.arn, error = %e,
                        "replication: unsealing stored target failed");
                    continue;
                }
            };
            match cairn_replication::sink_for_target(&open, stack.allow_internal_endpoints) {
                Ok(sink) => {
                    by_arn.insert(target.arn.clone(), Arc::new(sink));
                }
                Err(e) => {
                    tracing::warn!(arn = %target.arn, error = %e,
                        "replication: building sink for stored target failed");
                }
            }
        }
    }
    by_arn
}

/// Read a single bucket's first enabled replication rule's remote-target ARN, or `None` when the
/// bucket has no replication config, an unparseable document, or no enabled rule naming a target.
/// Build the [`StoredTargetRouter`] for a drain from the stored per-ARN sinks plus the env default.
/// The multi-target worker shape folds its env-named per-source routes in afterwards via
/// [`StoredTargetRouter::with_env_routes`]; the single-target shape passes none.
fn build_router(
    default: Option<Arc<HttpS3Sink>>,
    by_arn: &HashMap<String, Arc<HttpS3Sink>>,
) -> StoredTargetRouter {
    StoredTargetRouter {
        by_arn: by_arn.clone(),
        env_routes: HashMap::new(),
        default,
    }
}

/// Convert a configured [`ReplicationTarget`] into the sink configuration for its dedicated
/// [`HttpS3Sink`]. The target is a single fixed destination, so `dest_buckets` stays empty and the
/// target's `dest_bucket` is the one destination; its TLS trust knobs are carried through.
fn target_sink_cfg(
    target: &ReplicationTarget,
    allow_internal_endpoints: bool,
) -> cairn_replication::S3SinkConfig {
    cairn_replication::S3SinkConfig {
        endpoint: target.endpoint.clone(),
        dest_bucket: target.dest_bucket.clone(),
        dest_buckets: HashMap::new(),
        region: target.region.clone(),
        access_key_id: target.access_key.clone(),
        secret_access_key: target.secret.clone(),
        ca_cert_path: target.ca_path.clone(),
        ca_cert_pem: None,
        insecure_skip_verify: target.insecure_skip_verify,
        allow_internal_endpoints,
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

/// The [`SinkRouter`] the engine drives, plus the [`BucketRoutedSink`] it routes every entry to.
///
/// The engine resolves an entry's rule->target binding through `sink_for(target_arn)`; the current
/// outbox entry does not carry an explicit target ARN (`cairn_replication`'s `entry_target_arn`
/// returns `None`), so this router returns **itself** for any ARN and performs the real routing per
/// **source bucket** inside [`BucketRoutedSink`]. Each call dispatches on the source bucket in
/// precedence order:
///
///  1. **stored** — a sink built from the bucket's stored per-bucket remote target
///     (`ConfigAspect::ReplicationTargets`, unsealed under the master key). This is the MinIO-model
///     per-bucket destination and takes precedence.
///  2. **env_routes** — the legacy `CAIRN_REPLICATION_TARGETS` named-target route for the bucket.
///  3. **default** — the single-target `CAIRN_REPLICATION_*` env sink.
///
/// Routing precedence: an entry carrying a stored-target ARN routes directly to that target's sink
/// (per-entry, so one bucket fans out to several distinct targets correctly). An entry with no ARN
/// (the legacy env path) routes by source bucket through the env named route, then the env default.
/// An entry whose ARN resolves to no sink, or a no-ARN entry with no env route/default, terminates
/// for operator attention rather than silently dropping (ARCH 20).
struct StoredTargetRouter {
    /// `target ARN -> sink` for the per-bucket stored remote targets — the primary path.
    by_arn: HashMap<String, Arc<HttpS3Sink>>,
    /// `source bucket -> sink` resolved from the env named targets (legacy path for ARN-less entries).
    env_routes: HashMap<String, Arc<HttpS3Sink>>,
    /// The env single-target default sink (legacy fallback for ARN-less entries).
    default: Option<Arc<HttpS3Sink>>,
}

impl StoredTargetRouter {
    /// Fold the env-named per-source routes in (used by the multi-target worker shape).
    fn with_env_routes(mut self, routes: HashMap<String, Arc<HttpS3Sink>>) -> Self {
        self.env_routes = routes;
        self
    }

    /// Resolve the sink for an ARN-less (legacy/env) entry by its source bucket: env route, then
    /// the env default.
    fn sink_for_bucket(&self, source_bucket: &str) -> Result<&Arc<HttpS3Sink>, ReplicationError> {
        self.env_routes
            .get(source_bucket)
            .or(self.default.as_ref())
            .ok_or_else(|| {
                ReplicationError::Terminal(format!(
                    "no replication target for source bucket {source_bucket:?}"
                ))
            })
    }
}

impl SinkRouter for StoredTargetRouter {
    fn sink_for<'a>(&'a self, target_arn: Option<&str>) -> Option<&'a dyn BucketRoutedSink> {
        match target_arn {
            // The entry names a stored target: route straight to that target's sink. An ARN with no
            // sink (target removed since enqueue) yields None → the engine terminates the entry.
            Some(arn) => self
                .by_arn
                .get(arn)
                .map(|s| s.as_ref() as &dyn BucketRoutedSink),
            // No ARN (legacy/env entry): route by source bucket inside `BucketRoutedSink`.
            None => Some(self),
        }
    }
}

#[async_trait::async_trait]
impl BucketRoutedSink for StoredTargetRouter {
    async fn put_object(
        &self,
        source_bucket: &BucketName,
        object: ReplicatedObject,
    ) -> Result<(), ReplicationError> {
        self.sink_for_bucket(source_bucket.as_str())?
            .put_object(source_bucket, object)
            .await
    }

    async fn delete_marker(
        &self,
        source_bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<(), ReplicationError> {
        self.sink_for_bucket(source_bucket.as_str())?
            .delete_marker(source_bucket, key, version)
            .await
    }
}

/// Periodically run a truncating WAL checkpoint on the metadata store and publish the WAL size
/// and checkpoint stats as metrics (ARCH 8.4/11.2, F-3). Without this the `-wal` file can grow
/// unbounded under sustained writes with a long-lived reader, inflating disk use and read
/// latency. `checkpoint()` runs on the writer thread (serialized with mutations, never
/// contending), and a `busy` result means a reader pinned the log so the truncation was
/// deferred — that is observable via `cairn_wal_checkpoints_busy_total`.
async fn checkpoint_loop(stack: Arc<AppStack>, interval: Duration, size_threshold_bytes: u64) {
    // Only spawned when there is at least one sqlite shard handle; bind them once. Under sharding
    // (Phase 3.2) there is one handle per shard, each with its own WAL to checkpoint.
    let stores = stack.store.clone();
    if stores.is_empty() {
        return;
    }
    // Sum the WAL footprint across all shards for the gauge and the size trigger.
    let total_wal_bytes = |stores: &[Arc<cairn_meta::SqliteMetadataStore>]| {
        let stores = stores.to_vec();
        async move {
            let mut total = 0u64;
            for s in &stores {
                match s.wal_size_bytes().await {
                    Ok(bytes) => total += bytes,
                    Err(e) => tracing::warn!(error = %e, "wal size probe failed"),
                }
            }
            total
        }
    };
    // Poll on a cadence fine enough to react to the size threshold between interval ticks, but
    // never longer than the interval itself. When the size trigger is disabled (threshold 0) the
    // poll cadence is just the interval, preserving the original interval-only behaviour.
    let poll = if size_threshold_bytes > 0 {
        interval
            .min(Duration::from_secs(10))
            .max(Duration::from_secs(1))
    } else {
        interval
    };
    let mut elapsed = Duration::ZERO;
    loop {
        tokio::time::sleep(poll).await;
        elapsed += poll;

        // Probe the total WAL size every tick so the gauge stays live and the size trigger can fire.
        let wal_bytes = total_wal_bytes(&stores).await;
        metrics::gauge!("cairn_wal_bytes").set(wal_bytes as f64);

        // Checkpoint when the interval has elapsed OR the combined WAL has grown past the configured
        // size threshold (ARCH 8.4) — the latter bounds `-wal` growth under sustained writes with a
        // long-lived reader rather than waiting out the whole interval.
        let interval_due = elapsed >= interval;
        let size_due = size_threshold_bytes > 0 && wal_bytes >= size_threshold_bytes;
        if !interval_due && !size_due {
            continue;
        }
        if size_due && !interval_due {
            tracing::debug!(
                wal_bytes,
                threshold = size_threshold_bytes,
                "wal size threshold exceeded; checkpointing early"
            );
        }
        elapsed = Duration::ZERO;

        // Checkpoint every shard's WAL.
        for store in &stores {
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
        }
        // Refresh the gauge post-checkpoint so a truncating checkpoint's effect is visible.
        metrics::gauge!("cairn_wal_bytes").set(total_wal_bytes(&stores).await as f64);
    }
}

/// Refresh the store gauges (object/bucket/byte counts and compression ratio) from the metadata
/// aggregate on a short interval, so `/metrics` reflects live state.
async fn metrics_loop(stack: Arc<AppStack>) {
    let clock = SystemClock::new();
    // The per-target label set emitted last tick. A target that drains to zero falls out of the
    // aggregate's `by_target`, so we must explicitly zero its gauges this tick — otherwise the
    // registry keeps its last (non-zero) value forever and a caught-up destination reads as
    // permanently lagging/failing (audit: stale per-target gauges).
    let mut prev_targets: std::collections::HashSet<String> = std::collections::HashSet::new();
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

        // Writer inbound queue depth (ARCH 26.2): the headline write-backpressure signal. Only the
        // concrete sqlite store exposes the writer handle; libSQL/Turso self-manage and have no
        // such gauge.
        if !stack.store.is_empty() {
            let depth: usize = stack.store.iter().map(|s| s.writer_queue_depth()).sum();
            metrics::gauge!("cairn_writer_queue_depth").set(depth as f64);

            // Group-commit health (ARCH 26.2): the writer buffers a sample per durable batch off the
            // fsync barrier; drain and record each into the histograms. commit_seconds climbing is a
            // stall; batch_size collapsing to 1 under load means the batching broke. Sqlite-only,
            // like the queue-depth gauge — libSQL/Turso self-manage and expose no writer handle.
            for s in &stack.store {
                for sample in s.drain_writer_commit_samples() {
                    metrics::histogram!("cairn_writer_commit_seconds")
                        .record(sample.commit_seconds);
                    metrics::histogram!("cairn_writer_batch_size").record(sample.batch_size as f64);
                }
            }
        }

        // Metadata config-cache effectiveness (ARCH 11.5). The cache is not a `metrics` dependency,
        // so it exposes cumulative counters we mirror into the registry here.
        // Cumulative monotonic totals: set the counters to their absolute values each tick.
        let (hits, misses) = stack.meta_cache.stats();
        metrics::counter!("cairn_meta_cache_hits_total").absolute(hits);
        metrics::counter!("cairn_meta_cache_misses_total").absolute(misses);

        // Replication health (ARCH 20/26) from the uncapped aggregate (no 10k probe cap). Lag is
        // the age of the oldest still-pending *enqueue* (not its backed-off next_attempt_at, which
        // a retry pushes into the future), so a fresh backlog raises lag immediately. Per-target
        // gauges let an operator see which destination is lagging/failing (target cardinality is
        // operator-bounded; per-bucket stays API-only to avoid unbounded label cardinality).
        let now = clock.now();
        match stack.meta.replication_counts(None).await {
            Ok(c) => {
                metrics::gauge!("cairn_replication_queue_depth").set(c.pending as f64);
                metrics::gauge!("cairn_replication_claimed").set(c.claimed as f64);
                metrics::gauge!("cairn_replication_failed").set(c.failed as f64);
                // The honest "anything owed or stuck" signal: pending + in-flight + terminally
                // failed. Unlike queue_depth/lag (which fall to 0 once a down target's backlog
                // exhausts to `failed`), this is non-zero whenever ANY object is un-replicated, so a
                // dashboard never reads "healthy" while objects are permanently un-shipped (audit:
                // metrics read 0 once entries reach terminal failed).
                metrics::gauge!("cairn_replication_unreplicated")
                    .set((c.pending + c.claimed + c.failed) as f64);
                let lag_secs = if c.oldest_pending_at_ms == 0 {
                    0.0
                } else {
                    ((now.as_millis() - c.oldest_pending_at_ms).max(0) as f64) / 1000.0
                };
                metrics::gauge!("cairn_replication_lag_seconds").set(lag_secs);
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                for t in &c.by_target {
                    let target = t.target_arn.as_deref().unwrap_or("env-default").to_owned();
                    seen.insert(target.clone());
                    metrics::gauge!("cairn_replication_pending", "target" => target.clone())
                        .set(t.pending as f64);
                    metrics::gauge!("cairn_replication_failed_by_target", "target" => target)
                        .set(t.failed as f64);
                }
                // Zero any target that was present last tick but has now fully drained out of the
                // aggregate, so its gauges do not stick at their last non-zero value.
                for stale in prev_targets.difference(&seen) {
                    metrics::gauge!("cairn_replication_pending", "target" => stale.clone())
                        .set(0.0);
                    metrics::gauge!("cairn_replication_failed_by_target", "target" => stale.clone())
                        .set(0.0);
                }
                prev_targets = seen;
            }
            Err(e) => tracing::debug!(error = %e, "replication counts probe failed"),
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
        // Reclaim expired STS-style session credentials on the same cadence (ARCH 14): an expired
        // credential is already denied at auth time, but pruning its row keeps the table bounded.
        let _ = stack
            .meta
            .submit(Mutation::DeleteExpiredSessionCredentials {
                before: clock.now(),
            })
            .await;
    }
}

/// Periodically re-read every committed blob and verify it against the recorded ETag, turning silent
/// on-disk bit-rot into an observable event (ARCH 8.6/26.4). Encrypted and compressed blobs are
/// already integrity-checked on every read (AES-GCM authentication / the self-describing CRNB block
/// format), so the scrub targets the otherwise-unverified uncompressed-plaintext path: it re-hashes
/// the bytes and compares the MD5 to the stored single-part ETag. Enumeration is paged so memory
/// stays flat regardless of store size.
async fn scrub_loop(stack: Arc<AppStack>, interval: Duration) {
    use cairn_types::meta::ListQuery;
    loop {
        tokio::time::sleep(interval).await;
        let started = std::time::Instant::now();
        let mut scanned: u64 = 0;
        let mut corrupt: u64 = 0;
        let buckets = match stack.meta.list_buckets(None).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "scrub: enumerating buckets failed");
                continue;
            }
        };
        for bucket in &buckets {
            let mut cursor: Option<String> = None;
            let mut vmarker: Option<String> = None;
            loop {
                let query = ListQuery {
                    prefix: None,
                    delimiter: None,
                    cursor: cursor.clone(),
                    version_id_marker: vmarker.clone(),
                    start_after: None,
                    limit: 500,
                };
                let page = match stack.meta.list_versions(&bucket.name, &query).await {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(bucket = %bucket.name.as_str(), error = %e, "scrub: listing versions failed");
                        break;
                    }
                };
                for s in &page.items {
                    if s.is_delete_marker {
                        continue;
                    }
                    let row = match stack
                        .meta
                        .get_version(&bucket.name, &s.key, &s.version_id)
                        .await
                    {
                        Ok(Some(r)) => r,
                        _ => continue,
                    };
                    let Some(path) = row.storage_path.clone() else {
                        continue;
                    };
                    // Encrypted blobs are AES-GCM-authenticated on every read, so a real GET already
                    // detects their corruption; the scrub closes the plaintext gap.
                    if row.sse_descriptor.is_some() {
                        continue;
                    }
                    scanned += 1;
                    if let Err(kind) =
                        scrub_one(&stack, &path, &row.compression, row.etag.as_str()).await
                    {
                        corrupt += 1;
                        metrics::counter!("cairn_scrub_corruption_total", "kind" => kind)
                            .increment(1);
                        tracing::error!(
                            bucket = %bucket.name.as_str(),
                            key = %s.key.as_str(),
                            version = %s.version_id.as_str(),
                            kind,
                            "scrub: blob failed integrity verification"
                        );
                    }
                }
                match page.next_cursor {
                    Some(c) => {
                        cursor = Some(c);
                        vmarker = page.next_version_id_marker;
                    }
                    None => break,
                }
            }
        }
        metrics::counter!("cairn_scrub_objects_total").increment(scanned);
        metrics::histogram!("cairn_scrub_pass_seconds").record(started.elapsed().as_secs_f64());
        tracing::info!(scanned, corrupt, "scrub pass complete");
    }
}

/// Re-read one unencrypted blob and verify it. The read decompresses a CRNB-compressed blob (so a
/// corrupt compressed blob fails here), and for a single-part object the recomputed plaintext MD5 is
/// compared to the stored ETag (so a bit flip in an uncompressed plaintext blob is caught). A
/// multipart ETag (`{md5}-{n}`) is not a whole-object hash, so those are verified for readability
/// only. Returns a static corruption-kind label on failure.
async fn scrub_one(
    stack: &Arc<AppStack>,
    path: &cairn_types::id::StoragePath,
    compression: &cairn_types::CompressionDescriptor,
    etag: &str,
) -> Result<(), &'static str> {
    use futures_util::StreamExt;
    use md5::{Digest, Md5};
    let handle = stack
        .blob
        .open_with_dek(path, None, None, compression)
        .await
        .map_err(|_| "open_failed")?;
    let mut hasher = Md5::new();
    let mut body = handle.body;
    while let Some(chunk) = body.next().await {
        let bytes = chunk.map_err(|_| "read_failed")?;
        hasher.update(&bytes);
    }
    // Single-part ETag is a bare hex MD5; multipart is "{md5}-{partcount}" and is not re-hashable.
    if !etag.contains('-') {
        let computed = hex::encode(hasher.finalize());
        if !computed.eq_ignore_ascii_case(etag.trim_matches('"')) {
            return Err("hash_mismatch");
        }
    }
    Ok(())
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

/// The webhook event-notification delivery worker: drains the `events_outbox` to the configured
/// per-bucket endpoints on a fixed interval (ARCH 20-style). The claim is a cheap indexed query, so
/// the loop is harmless when no bucket has notifications configured (it claims nothing and sleeps).
async fn webhook_loop(stack: Arc<AppStack>, interval: Duration) {
    let engine = cairn_webhook::WebhookEngine::new(cairn_webhook::WebhookOpts::default());
    let sink = cairn_webhook::HttpWebhookSink::new(cairn_net::GuardConfig::new(
        stack.allow_internal_endpoints,
    ));
    let clock = SystemClock::new();
    loop {
        tokio::time::sleep(interval).await;
        match engine.run_until_idle(&*stack.meta, &sink, &clock, 50).await {
            Ok(report) if !report.is_idle() => {
                metrics::counter!("cairn_webhook_delivered_total").increment(report.delivered);
                metrics::counter!("cairn_webhook_failed_total").increment(report.failed);
                metrics::counter!("cairn_webhook_dropped_total").increment(report.dropped);
                tracing::info!(
                    delivered = report.delivered,
                    failed = report.failed,
                    dropped = report.dropped,
                    "webhook delivery progressed"
                );
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "webhook drain failed"),
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
                let sink = HttpS3Sink::new(target_sink_cfg(t, false)).expect("target sink builds");
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
        let cfg = target_sink_cfg(&t, false);
        assert_eq!(cfg.endpoint, "https://s3.secure.example");
        assert_eq!(cfg.dest_bucket, "mirror");
        assert!(cfg.dest_buckets.is_empty());
        assert!(cfg.insecure_skip_verify);
        assert!(cfg.ca_cert_path.is_none());

        let mut t = target("ca", "mirror", "https://s3.ca.example");
        t.ca_path = Some(std::path::PathBuf::from("/etc/ca.pem"));
        let cfg = target_sink_cfg(&t, false);
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
                ca_cert_pem: None,
                insecure_skip_verify: false,
                allow_internal_endpoints: false,
            })
            .unwrap(),
        );

        let mut routes = HashMap::new();
        routes.insert("logs".to_owned(), Arc::clone(&west));
        let sink = StoredTargetRouter {
            by_arn: HashMap::new(),
            env_routes: routes,
            default: Some(default),
        };

        // Routed bucket -> its target sink; unrouted -> the default sink.
        assert_eq!(
            sink.sink_for_bucket("logs").unwrap().dest_for("x"),
            "mirror-west"
        );
        assert_eq!(
            sink.sink_for_bucket("other").unwrap().dest_for("x"),
            "fallback"
        );

        // With no default, an unrouted bucket is a terminal failure.
        let sink = StoredTargetRouter {
            by_arn: HashMap::new(),
            env_routes: HashMap::new(),
            default: None,
        };
        let err = sink.sink_for_bucket("orphan").unwrap_err();
        assert!(matches!(err, ReplicationError::Terminal(_)));
    }

    fn test_sink(endpoint: &str, dest: &str) -> Arc<HttpS3Sink> {
        Arc::new(
            HttpS3Sink::new(cairn_replication::S3SinkConfig {
                endpoint: endpoint.to_owned(),
                dest_bucket: dest.to_owned(),
                dest_buckets: HashMap::new(),
                region: "us-east-1".to_owned(),
                access_key_id: "AKID".to_owned(),
                secret_access_key: "secret".to_owned(),
                ca_cert_path: None,
                ca_cert_pem: None,
                insecure_skip_verify: false,
                allow_internal_endpoints: false,
            })
            .unwrap(),
        )
    }

    /// An entry routes to the sink for ITS target ARN (per-entry), so one source can fan out to
    /// several distinct targets; an ARN with no sink terminates; an ARN-less (env) entry falls back
    /// to the source-bucket route, then the env default (ARCH 20.4/20.5).
    #[test]
    fn router_routes_per_entry_arn_then_falls_back_to_env() {
        let target_a = test_sink("https://a.example:9000", "dest-a");
        let target_b = test_sink("https://b.example:9000", "dest-b");
        let env_sink = test_sink("https://env.example:9000", "env-dest");
        let default_sink = test_sink("https://default.example:9000", "default-dest");

        let mut by_arn = HashMap::new();
        by_arn.insert(
            "arn:cairn:replication:us-east-1:aaaa:dest-a".to_owned(),
            Arc::clone(&target_a),
        );
        by_arn.insert(
            "arn:cairn:replication:us-east-1:bbbb:dest-b".to_owned(),
            Arc::clone(&target_b),
        );
        let mut env_routes = HashMap::new();
        env_routes.insert("metrics".to_owned(), Arc::clone(&env_sink));
        let router = StoredTargetRouter {
            by_arn,
            env_routes,
            default: Some(Arc::clone(&default_sink)),
        };

        // Two different ARNs resolve (so one source fans out to several distinct targets), each to
        // its own correct destination. `sink_for` yields a trait object, so the destination is
        // asserted on the concrete `by_arn` sinks; resolution itself is asserted via `sink_for`.
        assert!(
            router
                .sink_for(Some("arn:cairn:replication:us-east-1:aaaa:dest-a"))
                .is_some()
        );
        assert!(
            router
                .sink_for(Some("arn:cairn:replication:us-east-1:bbbb:dest-b"))
                .is_some()
        );
        assert_eq!(
            router.by_arn["arn:cairn:replication:us-east-1:aaaa:dest-a"].dest_for("x"),
            "dest-a"
        );
        assert_eq!(
            router.by_arn["arn:cairn:replication:us-east-1:bbbb:dest-b"].dest_for("x"),
            "dest-b"
        );

        // An ARN with no built sink (target removed) does not resolve -> the engine terminates it.
        assert!(
            router
                .sink_for(Some("arn:cairn:replication:us-east-1:zzzz:gone"))
                .is_none()
        );

        // An ARN-less (legacy/env) entry routes by source bucket: env route, then the env default.
        assert!(router.sink_for(None).is_some());
        assert_eq!(
            router.sink_for_bucket("metrics").unwrap().dest_for("x"),
            "env-dest"
        );
        assert_eq!(
            router.sink_for_bucket("other").unwrap().dest_for("x"),
            "default-dest"
        );
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
