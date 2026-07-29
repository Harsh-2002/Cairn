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
use cairn_types::error::{MetaError, ReplicationError};
use cairn_types::id::{BucketName, ObjectKey, VersionId};
use cairn_types::meta::{MultipartTerminalOutcome, Mutation, MutationOutcome};
use cairn_types::replication::ReplicatedObject;
use cairn_types::traits::Clock;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Maximum batches one event-driven drain may claim before yielding to its heartbeat. Shutdown is
/// checked between every batch, so a signalled worker never starts another claim pass.
const MAX_DRAIN_PASSES: u32 = 50;
const MULTIPART_SWEEP_PAGE: u32 = 1_000;
const MULTIPART_SWEEP_MAX_ITEMS: usize = 10_000;
const MULTIPART_SWEEP_MAX_DURATION: Duration = Duration::from_secs(30);

struct ManagedTask {
    name: String,
    handle: JoinHandle<()>,
}

#[derive(Default, Debug, PartialEq, Eq)]
struct TaskShutdownReport {
    completed: usize,
    cancelled: usize,
    failed: usize,
}

#[derive(Default)]
struct TaskSet {
    tasks: Vec<ManagedTask>,
}

impl Drop for TaskSet {
    fn drop(&mut self) {
        // `JoinHandle` detaches when merely dropped. Abort first so cancellation of the outer
        // server future cannot accidentally orphan a worker outside the retained supervisor.
        for task in &self.tasks {
            task.handle.abort();
        }
    }
}

impl TaskSet {
    fn spawn(&mut self, name: impl Into<String>, task: impl Future<Output = ()> + Send + 'static) {
        self.tasks.push(ManagedTask {
            name: name.into(),
            handle: tokio::spawn(task),
        });
    }

    /// Join every cooperative worker until one shared deadline, then abort and join whatever
    /// remains. Tasks run concurrently while this method awaits them; sequentially observing their
    /// handles does not serialize their work.
    async fn join_or_abort(&mut self, grace: Duration) -> TaskShutdownReport {
        let deadline = tokio::time::Instant::now() + grace;
        let mut report = TaskShutdownReport::default();
        let mut joined = 0usize;

        while joined < self.tasks.len() {
            let task = &mut self.tasks[joined];
            match tokio::time::timeout_at(deadline, &mut task.handle).await {
                Ok(Ok(())) => report.completed += 1,
                Ok(Err(error)) => {
                    report.failed += 1;
                    tracing::warn!(
                        task = %task.name,
                        error = %error,
                        "background task exited unexpectedly during shutdown"
                    );
                }
                Err(_) => break,
            }
            joined += 1;
        }

        let remaining = self.tasks.split_off(joined);
        self.tasks.clear();
        for task in remaining {
            let was_running = !task.handle.is_finished();
            if was_running {
                task.handle.abort();
            }
            match task.handle.await {
                Ok(()) => report.completed += 1,
                Err(error) if error.is_cancelled() => {
                    report.cancelled += 1;
                    tracing::warn!(
                        task = %task.name,
                        "background task exceeded the shutdown deadline and was cancelled"
                    );
                }
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(
                        task = %task.name,
                        error = %error,
                        "background task exited unexpectedly during shutdown"
                    );
                }
            }
        }
        report
    }
}

impl TaskShutdownReport {
    fn is_complete(&self) -> bool {
        self.cancelled == 0 && self.failed == 0
    }
}

/// Outcome of the ordered final durability tail.
///
/// A report is complete only when every enabled flush/checkpoint finished successfully. Keeping
/// this explicit prevents the process-level shutdown log from claiming success after a best-effort
/// error or deadline cancellation.
#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct FinalizeReport {
    request_metric_flushes: usize,
    request_metric_failures: usize,
    seal_counter_flushes: usize,
    seal_counter_failures: usize,
    checkpoints: usize,
    checkpoint_busy: usize,
    checkpoint_failures: usize,
    timed_out: bool,
}

impl FinalizeReport {
    pub(crate) fn is_complete(&self) -> bool {
        !self.timed_out
            && self.request_metric_failures == 0
            && self.seal_counter_failures == 0
            && self.checkpoint_busy == 0
            && self.checkpoint_failures == 0
    }

    pub(crate) fn log_outcome(&self) {
        if self.is_complete() {
            tracing::info!(
                request_metric_flushes = self.request_metric_flushes,
                seal_counter_flushes = self.seal_counter_flushes,
                checkpoints = self.checkpoints,
                "final metrics/counter flush and SQLite checkpoints complete"
            );
        } else {
            tracing::error!(
                timed_out = self.timed_out,
                request_metric_failures = self.request_metric_failures,
                seal_counter_failures = self.seal_counter_failures,
                checkpoint_busy = self.checkpoint_busy,
                checkpoint_failures = self.checkpoint_failures,
                "final shutdown durability work incomplete"
            );
        }
    }
}

/// Retained ownership of every task spawned by [`spawn`].
///
/// On shutdown, workers first observe the shared signal and stop starting new passes. The
/// supervisor then joins them under one deadline, cancels any overrun, and only after no worker can
/// race a final drain does it flush in-memory metrics, persist master-key counters, and checkpoint
/// every concrete SQLite shard.
pub(crate) struct BackgroundTasks {
    tasks: TaskSet,
    stack: Arc<AppStack>,
    request_metrics_retention_secs: Option<i64>,
}

/// A stopped worker set that still owns the resources needed by the ordered final durability tail.
pub(crate) struct StoppedBackgroundTasks {
    worker_report: TaskShutdownReport,
    stack: Arc<AppStack>,
    request_metrics_retention_secs: Option<i64>,
}

/// The complete background-side shutdown result: cooperative worker drain plus final persistence.
pub(crate) struct BackgroundShutdownReport {
    worker_report: TaskShutdownReport,
    finalization: FinalizeReport,
}

impl BackgroundShutdownReport {
    pub(crate) fn is_complete(&self) -> bool {
        self.worker_report.is_complete() && self.finalization.is_complete()
    }
}

impl BackgroundTasks {
    fn new(stack: Arc<AppStack>, request_metrics_retention_secs: Option<i64>) -> Self {
        Self {
            tasks: TaskSet::default(),
            stack,
            request_metrics_retention_secs,
        }
    }

    fn spawn(&mut self, name: impl Into<String>, task: impl Future<Output = ()> + Send + 'static) {
        self.tasks.spawn(name, task);
    }

    /// Stop and join/cancel workers, retaining the durability resources needed for the final flush.
    pub(crate) async fn stop(mut self, worker_grace: Duration) -> StoppedBackgroundTasks {
        let report = self.tasks.join_or_abort(worker_grace).await;
        if report.is_complete() {
            tracing::info!(
                completed = report.completed,
                "background workers stopped cooperatively"
            );
        } else {
            tracing::error!(
                completed = report.completed,
                cancelled = report.cancelled,
                failed = report.failed,
                "background worker shutdown incomplete"
            );
        }
        StoppedBackgroundTasks {
            worker_report: report,
            stack: self.stack,
            request_metrics_retention_secs: self.request_metrics_retention_secs,
        }
    }
}

impl StoppedBackgroundTasks {
    /// Run only after both background workers and in-flight HTTP requests have drained. This order
    /// ensures the request-metrics accumulator cannot receive a late request after its final drain.
    pub(crate) async fn finalize(self, final_flush_grace: Duration) -> BackgroundShutdownReport {
        let finalization = match tokio::time::timeout(
            final_flush_grace,
            finalize_shutdown(&self.stack, self.request_metrics_retention_secs),
        )
        .await
        {
            Ok(report) => report,
            Err(_) => {
                tracing::error!(
                    timeout_seconds = final_flush_grace.as_secs(),
                    "final metrics/counter flush and WAL checkpoint exceeded the shutdown deadline"
                );
                FinalizeReport {
                    timed_out: true,
                    ..FinalizeReport::default()
                }
            }
        };
        finalization.log_outcome();
        BackgroundShutdownReport {
            worker_report: self.worker_report,
            finalization,
        }
    }
}

/// Spawn the background tasks, reading their intervals and the multipart lifetime from the
/// configured 28.2 knobs. Every handle is retained in the returned supervisor. `shutdown` is the
/// server's graceful-shutdown signal; workers check it before starting another pass or claim batch.
/// A pass that cannot finish within the server's bounded grace is cancelled, leaving its durable
/// lease/cursor for startup recovery.
pub(crate) fn spawn(
    stack: Arc<AppStack>,
    cfg: &Config,
    shutdown: watch::Receiver<bool>,
) -> BackgroundTasks {
    let sweep_interval = Duration::from_secs(cfg.multipart_sweep_interval_secs);
    #[allow(clippy::cast_possible_wrap)]
    let multipart_lifetime_secs = cfg.multipart_upload_lifetime_secs as i64;
    #[allow(clippy::cast_possible_wrap)]
    let multipart_reservation_lifetime_secs = cfg
        .request_timeout_secs
        .saturating_add(60)
        .min(i64::MAX as u64) as i64;
    let lifecycle_interval = Duration::from_secs(cfg.lifecycle_interval_secs);
    let checkpoint_interval = Duration::from_secs(cfg.wal_checkpoint_interval_secs);
    #[allow(clippy::cast_possible_wrap)]
    let request_metrics_retention_secs = cfg
        .request_metrics_enabled
        .then_some((cfg.request_metrics_retention_days as i64) * 86_400);
    let mut tasks = BackgroundTasks::new(stack.clone(), request_metrics_retention_secs);

    tasks.spawn(
        "multipart sweeper",
        sweeper_loop(
            stack.clone(),
            sweep_interval,
            multipart_lifetime_secs,
            multipart_reservation_lifetime_secs,
            shutdown.clone(),
        ),
    );
    tasks.spawn(
        "lifecycle scanner",
        lifecycle_loop(stack.clone(), lifecycle_interval, shutdown.clone()),
    );
    tasks.spawn(
        "webhook delivery",
        webhook_loop(
            stack.clone(),
            Duration::from_secs(cfg.webhook_interval_secs),
            shutdown.clone(),
        ),
    );
    // The S3-import worker: claims pending import jobs and runs them into this node.
    tasks.spawn(
        "S3 import worker",
        crate::import_run::import_loop(
            stack.clone(),
            crate::import_run::ImportLoopConfig {
                poll_interval_secs: cfg.import_poll_interval_secs,
                retention_secs: cfg.import_retention_secs,
                default_workers: cfg.import_default_workers,
                max_workers: cfg.import_max_workers,
                global_max_inflight: cfg.import_global_max_inflight,
                timeouts: cfg.import_timeouts(),
                root_access_key: cfg.root_access_key.clone(),
            },
            stack.import_notify.clone(),
            shutdown.clone(),
        ),
    );
    // The integrity scrub is opt-in (I/O-heavy): only spawned when an interval is configured.
    if cfg.scrub_interval_secs > 0 {
        tasks.spawn(
            "integrity scrub",
            scrub_loop(
                stack.clone(),
                Duration::from_secs(cfg.scrub_interval_secs),
                shutdown.clone(),
            ),
        );
    }
    // The release update check (ARCH 28): opt-out, best-effort. Dials the configured feed through the
    // SSRF guard on a slow cadence and publishes the result for `GET /system`. Never spawned when off.
    if cfg.update_check_enabled {
        tasks.spawn(
            "release update check",
            update_check_loop(
                stack.clone(),
                cfg.update_check_url.clone(),
                Duration::from_secs(cfg.update_check_interval_secs),
                cfg.allow_internal_endpoints,
                shutdown.clone(),
            ),
        );
    }
    // The WAL checkpointer drives inherent methods on the concrete `SqliteMetadataStore`, so it
    // runs only for the `sqlite` backend (where `stack.store` holds one handle per shard). The
    // libSQL and Turso engines self-manage their WAL, so the loop is not spawned for them.
    if !stack.store.is_empty() {
        tasks.spawn(
            "WAL checkpointer",
            checkpoint_loop(
                stack.clone(),
                checkpoint_interval,
                cfg.wal_checkpoint_size_bytes,
                shutdown.clone(),
            ),
        );
        // Master-key re-wrap + seal-count flush (audit #29, Phase D/E), one per sqlite shard,
        // sharing the one master-key ring. Disabled when the interval is 0.
        for (shard, store) in stack.store.iter().enumerate() {
            if cfg.key_rewrap_interval_secs > 0 {
                tasks.spawn(
                    format!("master-key re-wrap shard {shard}"),
                    crate::key_rewrap::rewrap_loop(
                        store.clone(),
                        stack.crypto.clone(),
                        stack.meta_cache.clone(),
                        cfg.key_rewrap_interval_secs,
                        shutdown.clone(),
                    ),
                );
            }
            if cfg.key_counter_sync_secs > 0 {
                tasks.spawn(
                    format!("master-key counter sync shard {shard}"),
                    crate::key_rewrap::counter_sync_loop(
                        store.clone(),
                        stack.crypto.clone(),
                        cfg.key_counter_sync_secs,
                        shutdown.clone(),
                    ),
                );
            }
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
    // Exactly one process-wide weighted byte budget is created here and cloned through every
    // worker and sink shape below. Worker count, destination fan-out, and per-drain sink rebuilds
    // therefore cannot multiply the configured replication memory allowance.
    let sink_runtime = cfg.replication_sink_runtime();
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
        let worker = ReplicationWorkerRuntime {
            interval,
            notify,
            shutdown: sd,
            opts,
            sink_runtime: sink_runtime.clone(),
        };
        if !targets.is_empty() {
            tasks.spawn(
                "replication worker",
                multi_target_replication_loop(
                    stack.clone(),
                    targets.clone(),
                    single_cfg.clone(),
                    worker,
                ),
            );
        } else if let Some(sink_cfg) = single_cfg.clone() {
            tasks.spawn(
                "replication worker",
                replication_loop(stack.clone(), sink_cfg, worker),
            );
        } else {
            tasks.spawn(
                "replication worker",
                multi_target_replication_loop(stack.clone(), Vec::new(), None, worker),
            );
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
    tasks.spawn(
        "replication outbox pruner",
        replication_prune_loop(
            stack.clone(),
            cfg.replication_retention_secs,
            shutdown.clone(),
        ),
    );
    // Reclaim terminally-failed webhook-outbox rows so a dead/misconfigured sink can't bloat the
    // metadata DB without bound (audit 2026-07; ARCH 20.3 bounded-work-queue contract).
    tasks.spawn(
        "webhook outbox pruner",
        events_outbox_prune_loop(
            stack.clone(),
            cfg.events_outbox_retention_secs,
            shutdown.clone(),
        ),
    );
    // Request-metrics flush loop (ARCH 26.5). Gated on the subsystem being enabled: when off, the
    // hot path accumulates nothing and there is nothing to flush. Otherwise it periodically drains
    // the in-process aggregator into a batched upsert and prunes rows past the retention horizon.
    if cfg.request_metrics_enabled {
        let flush_interval = Duration::from_secs(cfg.request_metrics_flush_secs.max(1));
        let retention_secs =
            request_metrics_retention_secs.expect("enabled request metrics have retention");
        tasks.spawn(
            "request metrics flush",
            request_metrics_flush_loop(
                stack.clone(),
                flush_interval,
                retention_secs,
                shutdown.clone(),
            ),
        );
        tracing::info!("request-metrics ingestion enabled");
    }

    // The encrypted-suspect audit gauges (ARCH 20.5/26.4) — OPT-IN, and off unless the operator has
    // supplied a cutoff via `CAIRN_REPLICATION_AUDIT_BEFORE`. Unset means the loop is never spawned:
    // no version walk, no gauges, no warning, zero cost. See `replication_audit_loop` for why a
    // cutoff-less gauge is not a conservative default but a broken signal.
    //
    // Deliberately NOT part of `metrics_loop` either way: it is a full version-row enumeration, and
    // `object_versions.replication_status` has no index (adding one is a migration, which this
    // remediation deliberately avoids).
    match cfg.replication_audit_before.as_deref() {
        None => tracing::debug!(
            "encrypted-suspect replication audit disabled (set CAIRN_REPLICATION_AUDIT_BEFORE to \
             the moment this node was upgraded past the SSE replication defect to enable it)"
        ),
        Some(raw) => match crate::replication_audit::parse_cutoff(raw) {
            // Config validation already rejected an unparseable value at load; this arm cannot fire
            // on a validated config, and refuses to guess a cutoff if it somehow does.
            Err(e) => {
                tracing::error!(error = %e, "CAIRN_REPLICATION_AUDIT_BEFORE is unusable; the \
                 encrypted-suspect audit will not run")
            }
            Ok(cutoff) => {
                tracing::info!(
                    created_before = cutoff.0,
                    "encrypted-suspect replication audit enabled"
                );
                tasks.spawn(
                    "encrypted replication audit",
                    replication_audit_loop(
                        stack.clone(),
                        cutoff,
                        cfg.replication_allow_plaintext_sse_over_http,
                        cfg.replication_endpoint.clone(),
                        shutdown.clone(),
                    ),
                );
            }
        },
    }

    tasks.spawn("metrics refresher", metrics_loop(stack, shutdown));
    tasks
}

/// Wait for one periodic interval unless shutdown wins. The biased select is deliberate: if the
/// timer and signal become ready together, shutdown wins and the caller does not start a new pass.
pub(crate) async fn wait_for_interval_or_shutdown(
    interval: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    if *shutdown.borrow() {
        return false;
    }
    tokio::select! {
        biased;
        changed = shutdown.changed() => {
            let _ = changed;
            false
        }
        () = tokio::time::sleep(interval) => true,
    }
}

/// Drain one request-metrics snapshot through the writer. Background and final shutdown flushes
/// share this seam so their retention and empty-drain behavior cannot drift.
async fn flush_request_metrics_once(
    stack: &AppStack,
    retention_secs: i64,
) -> Result<(), MetaError> {
    let rows = stack.request_metrics.drain();
    if rows.is_empty() {
        return Ok(());
    }
    let now_secs = SystemClock::new().now().as_secs();
    stack
        .meta
        .submit(Mutation::RecordRequestMetrics {
            rows,
            prune_before: Some(now_secs - retention_secs),
        })
        .await?;
    Ok(())
}

/// The final, ordered durability tail after every worker has joined or been cancelled:
///
/// 1. drain request metrics while no periodic flusher can race the accumulator;
/// 2. persist the active master-key seal counter on every SQLite shard;
/// 3. checkpoint every SQLite WAL after both preceding writer mutations.
async fn finalize_shutdown(
    stack: &AppStack,
    request_metrics_retention_secs: Option<i64>,
) -> FinalizeReport {
    let mut report = FinalizeReport::default();
    if let Some(retention_secs) = request_metrics_retention_secs {
        match flush_request_metrics_once(stack, retention_secs).await {
            Ok(()) => report.request_metric_flushes += 1,
            Err(error) => {
                report.request_metric_failures += 1;
                tracing::error!(%error, "final request-metrics flush failed");
            }
        }
    }

    for (shard, store) in stack.store.iter().enumerate() {
        match crate::key_rewrap::sync_seal_count(store, &stack.crypto).await {
            Ok(()) => report.seal_counter_flushes += 1,
            Err(error) => {
                report.seal_counter_failures += 1;
                tracing::error!(%error, shard, "final master-key seal-count flush failed");
            }
        }
    }

    for (shard, store) in stack.store.iter().enumerate() {
        match store.checkpoint().await {
            Ok(stats) => {
                if stats.busy {
                    report.checkpoint_busy += 1;
                    tracing::warn!(
                        shard,
                        log_frames = stats.log_frames,
                        checkpointed_frames = stats.checkpointed_frames,
                        "final SQLite checkpoint remained busy"
                    );
                } else {
                    report.checkpoints += 1;
                    tracing::info!(
                        shard,
                        checkpointed_frames = stats.checkpointed_frames,
                        "final SQLite checkpoint complete"
                    );
                }
            }
            Err(error) => {
                report.checkpoint_failures += 1;
                tracing::error!(%error, shard, "final SQLite checkpoint failed");
            }
        }
    }
    report
}

/// How long after startup the first encrypted-suspect audit pass runs. Long enough that it never
/// competes with the startup reconcile/readiness path (ARCH 8), short enough that an operator who
/// restarts a node to pick up the fix sees the number within the same maintenance window.
const AUDIT_FIRST_PASS_DELAY: Duration = Duration::from_secs(300);

/// The steady-state cadence of the encrypted-suspect audit pass: **every 6 hours**.
///
/// This is a deliberate choice, not a default. The pass walks every version of every bucket that has
/// an enabled replication rule and reads each candidate's row, and there is **no index** on
/// `object_versions.replication_status` — creating one would be a schema migration, which this
/// remediation explicitly does not take. Six hours keeps the cost off the per-scrape path entirely
/// (a Prometheus scrape must never trigger a table walk) while still being far faster than the
/// human loop it feeds: an operator reads this gauge on a dashboard, decides to run a repair, and
/// watches it fall over hours.
///
/// The cost is genuinely two-sided and both halves matter. Buckets without an enabled replication
/// rule are skipped before any version is read, so on a store that does not replicate — the
/// overwhelming majority — a pass costs one `list_buckets` plus one config read per bucket. A bucket
/// that **does** replicate costs a full version listing *plus one `get_version` point query per
/// listed version*, because the listing page carries no `sse_descriptor`. On a large replicated
/// bucket that per-version query is the pass, which is the other reason this is opt-in and slow.
const AUDIT_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

/// Periodically count the **encrypted + terminally replicated** version population created before
/// `created_before` and publish it as gauges, so the field damage from the pre-release-X plaintext
/// seam is visible on a dashboard rather than only from a CLI an operator has to know to run
/// (ARCH 20.5, 26.4). Spawned only when the operator has set `CAIRN_REPLICATION_AUDIT_BEFORE`.
///
/// `cairn_replication_unreplicated` does **not** cover this: those versions are `completed`: the
/// engine believes they shipped, and the replica exists — it is just the wrong bytes. That is
/// exactly why it needs its own signal.
///
/// Three gauges, because one cannot express convergence. A forced requeue moves damaged rows to
/// `pending`, which the suspect predicate excludes — so the suspect gauge falls to zero the moment
/// the repair is *queued*, before a byte re-ships, and would then appear to regress as entries
/// complete. `…_repair_pending_versions` counts the in-flight population, and it genuinely reaches
/// zero: a version is only ever moved to `pending` when something can actually ship it (it is
/// current, so the resync backfill re-enqueues it, or it still has an outbox row).
/// `…_non_current_suspect_versions` is the third, and it is the FLOOR of the suspect gauge —
/// versions the backfill cannot reach (TRAP 2). The reachable done-state is therefore
/// `repair_pending == 0` **and** `suspect == non_current_suspect`; demanding `suspect == 0` where
/// non-current suspects exist would be alerting on something only a destination rebuild can fix.
async fn replication_audit_loop(
    stack: Arc<AppStack>,
    created_before: cairn_types::time::Timestamp,
    allow_plaintext_sse_over_http: bool,
    env_endpoint: Option<String>,
    mut shutdown: watch::Receiver<bool>,
) {
    if !wait_for_interval_or_shutdown(AUDIT_FIRST_PASS_DELAY, &mut shutdown).await {
        return;
    }
    loop {
        let started = std::time::Instant::now();
        // `sample_limit = 0`: the gauge path wants counts, and materializes no sample list.
        match crate::replication_audit::audit_store(
            stack.meta.as_ref(),
            None,
            created_before,
            0,
            allow_plaintext_sse_over_http,
            env_endpoint.as_deref(),
            None,
        )
        .await
        {
            Ok(report) => {
                metrics::gauge!("cairn_replication_encrypted_suspect_versions")
                    .set(report.present_and_suspect as f64);
                metrics::gauge!("cairn_replication_encrypted_absent_versions")
                    .set(report.absent as f64);
                metrics::gauge!("cairn_replication_encrypted_repair_pending_versions")
                    .set(report.repair_pending as f64);
                // The FLOOR of the suspect gauge. Non-current suspects are unrepairable by any
                // command (TRAP 2 — the backfill enumerates current versions only), so without this
                // series an operator cannot write a convergent alert on the suspect count: it stops
                // falling here, not at zero.
                metrics::gauge!("cairn_replication_encrypted_non_current_suspect_versions")
                    .set(report.non_current_suspect as f64);
                metrics::gauge!("cairn_replication_audit_pass_seconds")
                    .set(started.elapsed().as_secs_f64());
                // Bounded by the cutoff, so this is a real, convergent alarm rather than a
                // permanent one: it stops firing when the repair finishes.
                if report.present_and_suspect > 0 {
                    tracing::warn!(
                        suspect = report.present_and_suspect,
                        absent = report.absent,
                        repair_pending = report.repair_pending,
                        created_before = created_before.0,
                        "encrypted versions created before the audit cutoff are stamped replicated \
                         but may hold CIPHERTEXT on the mirror; run `cairn replication audit` \
                         (docs/operations.md 8.7)"
                    );
                }
            }
            Err(e) => tracing::warn!(error = %e, "replication encrypted-suspect audit failed"),
        }
        if !wait_for_interval_or_shutdown(AUDIT_INTERVAL, &mut shutdown).await {
            return;
        }
    }
}

/// Periodically flush the in-process request-metrics aggregator into the rollup table and prune
/// rows past the retention horizon (ARCH 26.5). Each tick drains the accumulated counts and submits
/// a single `RecordRequestMetrics` mutation through the single writer — the only DB touch the
/// request-metrics subsystem makes, keeping the request hot path free of any DB I/O. `prune_before`
/// is always supplied so old rows are reclaimed even on idle ticks, but a submit is skipped entirely
/// when there is no traffic to flush to avoid a pointless write each interval.
async fn request_metrics_flush_loop(
    stack: Arc<AppStack>,
    interval: Duration,
    retention_secs: i64,
    mut shutdown: watch::Receiver<bool>,
) {
    while wait_for_interval_or_shutdown(interval, &mut shutdown).await {
        if let Err(e) = flush_request_metrics_once(&stack, retention_secs).await {
            tracing::warn!(error = %e, "request metrics flush failed");
        }
    }
}

/// Periodically reclaim terminal replication-outbox rows (completed/failed) older than the retention
/// horizon, so the durable work queue stays bounded instead of growing one row per replicated object
/// forever (ARCH 20.3). Pending/claimed entries are never pruned. Runs on a calm cadence — the table
/// only needs to stay bounded, not be trimmed instantly — and pruning is idempotent, so a tick missed
/// on shutdown is harmless.
async fn replication_prune_loop(
    stack: Arc<AppStack>,
    retention_secs: u64,
    mut shutdown: watch::Receiver<bool>,
) {
    let clock = SystemClock::new();
    let interval = Duration::from_secs(retention_secs.clamp(60, 3600));
    let retention_ms = (retention_secs as i64).saturating_mul(1000);
    while wait_for_interval_or_shutdown(interval, &mut shutdown).await {
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
async fn events_outbox_prune_loop(
    stack: Arc<AppStack>,
    retention_secs: u64,
    mut shutdown: watch::Receiver<bool>,
) {
    let clock = SystemClock::new();
    let interval = Duration::from_secs(retention_secs.clamp(60, 3600));
    let retention_ms = (retention_secs as i64).saturating_mul(1000);
    while wait_for_interval_or_shutdown(interval, &mut shutdown).await {
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
            allow_plaintext_sse_over_http: cfg.replication_allow_plaintext_sse_over_http,
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
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    if *shutdown.borrow() {
        return false;
    }
    tokio::select! {
        biased;
        changed = shutdown.changed() => {
            let _ = changed;
            false
        }
        () = notify.notified() => true,
        () = tokio::time::sleep(interval) => true,
    }
}

/// Per-worker controls plus clones of the two process-wide resources (notification pulse and sink
/// runtime). Grouping them keeps every worker shape on exactly the same admission/deadline policy.
struct ReplicationWorkerRuntime {
    interval: Duration,
    notify: Arc<tokio::sync::Notify>,
    shutdown: watch::Receiver<bool>,
    opts: cairn_replication::ReplicationOpts,
    sink_runtime: cairn_replication::ReplicationSinkRuntime,
}

async fn replication_loop(
    stack: Arc<AppStack>,
    base_cfg: cairn_replication::S3SinkConfig,
    worker: ReplicationWorkerRuntime,
) {
    let ReplicationWorkerRuntime {
        interval,
        notify,
        mut shutdown,
        opts,
        sink_runtime,
    } = worker;
    // The engine unseals each encrypted version's DEK through the shared master ring before
    // reading its body, so the replica receives plaintext rather than raw ciphertext.
    let engine = cairn_replication::ReplicationEngine::new(opts, stack.crypto.clone());
    let clock = SystemClock::new();
    while wait_for_drain_trigger(interval, &notify, &mut shutdown).await {
        // Resolve the per-source destination map from each bucket's replication rule.
        let dest_buckets = resolve_dest_buckets(&stack).await;
        let mut sink_cfg = base_cfg.clone();
        sink_cfg.dest_buckets = dest_buckets;
        let default_sink = match cairn_replication::HttpS3Sink::new(sink_cfg, sink_runtime.clone())
        {
            Ok(s) => Some(Arc::new(s)),
            Err(e) => {
                tracing::error!(error = %e, "replication sink construction failed; skipping drain");
                continue;
            }
        };

        // Build the router for this drain: stored per-bucket remote targets take precedence; any
        // bucket without one falls back to this env-configured default sink (the unchanged
        // node->node path).
        let stored = resolve_stored_target_sinks(&stack, &sink_runtime).await;
        let router = build_router(default_sink, &stored);
        drain_with_router(&engine, &stack, &router, &clock, &shutdown).await;
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
    worker: ReplicationWorkerRuntime,
) {
    let ReplicationWorkerRuntime {
        interval,
        notify,
        mut shutdown,
        opts,
        sink_runtime,
    } = worker;
    // The engine unseals each encrypted version's DEK through the shared master ring before
    // reading its body, so the replica receives plaintext rather than raw ciphertext.
    let engine = cairn_replication::ReplicationEngine::new(opts, stack.crypto.clone());
    let clock = SystemClock::new();

    // Build a sink per named target once. A target whose sink fails to construct (a bad endpoint,
    // an unreadable CA bundle, conflicting trust knobs) is logged and dropped; the rest still run.
    let mut target_sinks: Vec<(ReplicationTarget, Arc<HttpS3Sink>)> = Vec::new();
    for target in targets {
        match HttpS3Sink::new(
            target_sink_cfg(
                &target,
                stack.allow_internal_endpoints,
                stack.replication_allow_plaintext_sse_over_http,
            ),
            sink_runtime.clone(),
        ) {
            Ok(sink) => target_sinks.push((target, Arc::new(sink))),
            Err(e) => {
                tracing::error!(target = %target.name, error = %e,
                    "replication target sink construction failed; target disabled");
            }
        }
    }
    let default_sink = match default_cfg {
        Some(cfg) => match HttpS3Sink::new(cfg, sink_runtime.clone()) {
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
        let stored = resolve_stored_target_sinks(&stack, &sink_runtime).await;
        // `routes` are the env-named per-source routes; fold them in over the stored ones.
        let router = build_router(default_sink.clone(), &stored).with_env_routes(routes);
        drain_with_router(&engine, &stack, &router, &clock, &shutdown).await;
    }
}

/// Run one drain pass through the engine with the assembled router, publishing the replication
/// progress + bytes metrics. Centralises the run/report handling shared by both worker shapes.
async fn drain_with_router(
    engine: &cairn_replication::ReplicationEngine,
    stack: &Arc<AppStack>,
    router: &StoredTargetRouter,
    clock: &SystemClock,
    shutdown: &watch::Receiver<bool>,
) {
    for _ in 0..MAX_DRAIN_PASSES {
        // A completed batch may have made more work immediately claimable. Re-check the signal
        // before every subsequent claim so shutdown never starts another batch.
        if *shutdown.borrow() {
            return;
        }
        match engine
            .run_once(&*stack.meta, router, &stack.blob, clock)
            .await
        {
            Ok(report) if report.is_idle() => return,
            Ok(report) => {
                metrics::counter!("cairn_replication_completed_total")
                    .increment(report.completed as u64);
                metrics::counter!("cairn_replication_failed_total").increment(report.failed as u64);
                metrics::counter!("cairn_replication_bytes_total").increment(report.bytes);
                // Source-DEK resolve failures (ARCH 20/26/27). These are rescheduled as
                // *unavailable*, which never consumes the attempt budget — so an object whose key
                // id was permanently removed from the master ring retries forever and NEVER
                // appears in `failed`. This counter is the only durable signal that such objects
                // exist; a sustained non-zero rate means replication is silently stalled on local
                // key material, not on the destination.
                metrics::counter!("cairn_replication_dek_resolve_failed_total")
                    .increment(report.dek_resolve_failures as u64);
                tracing::info!(
                    completed = report.completed,
                    failed = report.failed,
                    bytes = report.bytes,
                    "replication progressed"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, "replication run failed");
                return;
            }
        }
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
async fn resolve_stored_target_sinks(
    stack: &Arc<AppStack>,
    sink_runtime: &cairn_replication::ReplicationSinkRuntime,
) -> HashMap<String, Arc<HttpS3Sink>> {
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
            match cairn_replication::sink_for_target(
                &open,
                stack.allow_internal_endpoints,
                stack.replication_allow_plaintext_sse_over_http,
                sink_runtime.clone(),
            ) {
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
    allow_plaintext_sse_over_http: bool,
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
        allow_plaintext_sse_over_http,
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
async fn checkpoint_loop(
    stack: Arc<AppStack>,
    interval: Duration,
    size_threshold_bytes: u64,
    mut shutdown: watch::Receiver<bool>,
) {
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
    while wait_for_interval_or_shutdown(poll, &mut shutdown).await {
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
async fn metrics_loop(stack: Arc<AppStack>, mut shutdown: watch::Receiver<bool>) {
    let clock = SystemClock::new();
    // The per-target label set emitted last tick. A target that drains to zero falls out of the
    // aggregate's `by_target`, so we must explicitly zero its gauges this tick — otherwise the
    // registry keeps its last (non-zero) value forever and a caught-up destination reads as
    // permanently lagging/failing (audit: stale per-target gauges).
    let mut prev_targets: std::collections::HashSet<String> = std::collections::HashSet::new();
    while wait_for_interval_or_shutdown(Duration::from_secs(15), &mut shutdown).await {
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

        // Fail-closed encrypted-read refusals (ARCH 27). `cairn-blob` takes no `metrics` dependency
        // either, so it exposes a cumulative count we mirror here. Non-zero means a read of an
        // encrypted blob arrived with no data key — either a caller lost a DEK it should have
        // resolved (the class of bug that had replication shipping ciphertext), or the documented
        // false positive: an object whose body IS the verbatim bytes of an encrypted blob file
        // (a `CAIRN_DATA_DIR` backed up into a bucket). Alertable; a log line is not.
        metrics::counter!("cairn_blob_encrypted_without_key_total")
            .absolute(stack.blob_local.encrypted_without_key_total());

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
async fn sweeper_loop(
    stack: Arc<AppStack>,
    interval: Duration,
    lifetime_secs: i64,
    reservation_lifetime_secs: i64,
    mut shutdown: watch::Receiver<bool>,
) {
    let clock = SystemClock::new();
    while wait_for_interval_or_shutdown(interval, &mut shutdown).await {
        let result = tokio::time::timeout(
            MULTIPART_SWEEP_MAX_DURATION,
            sweep_multipart_once(
                &stack,
                clock.now(),
                lifetime_secs,
                reservation_lifetime_secs,
                &shutdown,
            ),
        )
        .await;
        match result {
            Ok(report) if report.reclaimed > 0 || report.aborted > 0 => tracing::info!(
                reclaimed = report.reclaimed,
                aborted = report.aborted,
                attempted = report.attempted,
                "multipart sweeper reclaimed stale staging"
            ),
            Ok(_) => {}
            Err(_) => tracing::warn!(
                max_seconds = MULTIPART_SWEEP_MAX_DURATION.as_secs(),
                "multipart sweep reached its fixed time budget"
            ),
        }
        // Reclaim expired STS-style session credentials on the same cadence (ARCH 14): an expired
        // credential is already denied at auth time, but pruning its row keeps the table bounded.
        if *shutdown.borrow() {
            return;
        }
        let _ = stack
            .meta
            .submit(Mutation::DeleteExpiredSessionCredentials {
                before: clock.now(),
            })
            .await;
    }
}

#[derive(Default)]
struct MultipartSweepReport {
    attempted: usize,
    reclaimed: usize,
    aborted: usize,
}

async fn sweep_multipart_once(
    stack: &AppStack,
    now: cairn_types::Timestamp,
    lifetime_secs: i64,
    reservation_lifetime_secs: i64,
    shutdown: &watch::Receiver<bool>,
) -> MultipartSweepReport {
    let mut report = MultipartSweepReport::default();

    // Cleanup debt comes first. Exact superseded-part paths are ordered before whole-session
    // directories by the metadata store, so cheap unlinks cannot be starved by a large directory.
    let mut seen_cleanups = std::collections::HashSet::new();
    loop {
        if *shutdown.borrow() || report.attempted >= MULTIPART_SWEEP_MAX_ITEMS {
            return report;
        }
        let cleanups = match stack
            .meta
            .list_multipart_cleanups(MULTIPART_SWEEP_PAGE)
            .await
        {
            Ok(cleanups) => cleanups,
            Err(error) => {
                tracing::warn!(%error, "multipart sweeper could not list cleanup debt");
                break;
            }
        };
        let page_full = cleanups.len() == MULTIPART_SWEEP_PAGE as usize;
        let mut discovered = 0usize;
        for cleanup in cleanups {
            if *shutdown.borrow() || report.attempted >= MULTIPART_SWEEP_MAX_ITEMS {
                return report;
            }
            if !seen_cleanups.insert(cleanup.id.clone()) {
                continue;
            }
            discovered += 1;
            report.attempted += 1;
            let deletion = match &cleanup.storage_path {
                Some(path) => stack.blob.delete(path).await,
                None => stack.blob.delete_session(&cleanup.upload_id).await,
            };
            if let Err(error) = deletion {
                tracing::warn!(
                    cleanup_id = cleanup.id,
                    upload_id = %cleanup.upload_id,
                    %error,
                    "multipart cleanup debt unlink failed"
                );
                continue;
            }
            let release = match cleanup.storage_path {
                Some(_) => Mutation::ReleaseMultipartCleanup {
                    cleanup_id: cleanup.id.clone(),
                },
                None => Mutation::ReleaseMultipartUploadCleanups {
                    upload_id: cleanup.upload_id.clone(),
                },
            };
            match stack.meta.submit(release).await {
                Ok(MutationOutcome::Ack) => report.reclaimed += 1,
                Ok(outcome) => tracing::warn!(
                    ?outcome,
                    cleanup_id = cleanup.id,
                    "multipart cleanup release returned an unexpected outcome"
                ),
                Err(error) => tracing::warn!(
                    %error,
                    cleanup_id = cleanup.id,
                    "multipart cleanup accounting release failed"
                ),
            }
        }
        if !page_full || discovered == 0 {
            break;
        }
    }

    // A reservation can outlive a cancelled/timed-out request before RecordPart commits. Its
    // deterministic attempt name lets the sweep prove the artifact absent before releasing bytes.
    let reservation_cutoff = now.plus_secs(-reservation_lifetime_secs);
    let mut seen_reservations = std::collections::HashSet::new();
    loop {
        if *shutdown.borrow() || report.attempted >= MULTIPART_SWEEP_MAX_ITEMS {
            return report;
        }
        let reservations = match stack
            .meta
            .enumerate_stale_multipart_reservations(reservation_cutoff, MULTIPART_SWEEP_PAGE)
            .await
        {
            Ok(reservations) => reservations,
            Err(error) => {
                tracing::warn!(%error, "multipart sweeper could not list stale reservations");
                break;
            }
        };
        let page_full = reservations.len() == MULTIPART_SWEEP_PAGE as usize;
        let mut discovered = 0usize;
        for reservation in reservations {
            if *shutdown.borrow() || report.attempted >= MULTIPART_SWEEP_MAX_ITEMS {
                return report;
            }
            if !seen_reservations.insert(reservation.attempt_id.clone()) {
                continue;
            }
            discovered += 1;
            report.attempted += 1;
            if let Err(error) = stack
                .blob
                .delete_part_attempt(
                    &reservation.upload_id,
                    reservation.part_number,
                    &reservation.attempt_id,
                )
                .await
            {
                tracing::warn!(
                    attempt_id = reservation.attempt_id,
                    upload_id = %reservation.upload_id,
                    %error,
                    "multipart reservation artifact cleanup failed"
                );
                continue;
            }
            match stack
                .meta
                .submit(Mutation::ReleaseMultipartReservation {
                    upload_id: reservation.upload_id.clone(),
                    attempt_id: reservation.attempt_id.clone(),
                })
                .await
            {
                Ok(MutationOutcome::Ack) => report.reclaimed += 1,
                Ok(outcome) => tracing::warn!(
                    ?outcome,
                    attempt_id = reservation.attempt_id,
                    "multipart reservation release returned an unexpected outcome"
                ),
                Err(error) => tracing::warn!(
                    %error,
                    attempt_id = reservation.attempt_id,
                    "multipart reservation accounting release failed"
                ),
            }
        }
        if !page_full || discovered == 0 {
            break;
        }
    }

    let session_cutoff = now.plus_secs(-lifetime_secs);
    let mut seen_sessions = std::collections::HashSet::new();
    loop {
        if *shutdown.borrow() || report.attempted >= MULTIPART_SWEEP_MAX_ITEMS {
            return report;
        }
        let sessions = match stack
            .meta
            .enumerate_stale_sessions(session_cutoff, MULTIPART_SWEEP_PAGE)
            .await
        {
            Ok(sessions) => sessions,
            Err(error) => {
                tracing::warn!(%error, "multipart sweeper could not list stale sessions");
                break;
            }
        };
        let page_full = sessions.len() == MULTIPART_SWEEP_PAGE as usize;
        let mut discovered = 0usize;
        for session in sessions {
            if *shutdown.borrow() || report.attempted >= MULTIPART_SWEEP_MAX_ITEMS {
                return report;
            }
            if !seen_sessions.insert(session.upload_id.as_str().to_owned()) {
                continue;
            }
            discovered += 1;
            report.attempted += 1;
            match stack
                .meta
                .submit(Mutation::AbortMultipart(session.upload_id.clone()))
                .await
            {
                Ok(MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::Aborted)) => {
                    if let Err(error) = stack.blob.delete_session(&session.upload_id).await {
                        tracing::warn!(
                            upload_id = %session.upload_id,
                            %error,
                            "multipart sweeper failed to reclaim an aborted session"
                        );
                        continue;
                    }
                    match stack
                        .meta
                        .submit(Mutation::ReleaseMultipartUploadCleanups {
                            upload_id: session.upload_id.clone(),
                        })
                        .await
                    {
                        Ok(MutationOutcome::Ack) => {
                            report.reclaimed += 1;
                            report.aborted += 1;
                        }
                        Ok(outcome) => tracing::warn!(
                            upload_id = %session.upload_id,
                            ?outcome,
                            "multipart session cleanup release returned an unexpected outcome"
                        ),
                        Err(error) => tracing::warn!(
                            upload_id = %session.upload_id,
                            %error,
                            "multipart session cleanup accounting release failed"
                        ),
                    }
                }
                Ok(MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::NotOwner)) => {}
                Ok(outcome) => tracing::warn!(
                    upload_id = %session.upload_id,
                    ?outcome,
                    "multipart sweeper received an unexpected abort outcome"
                ),
                Err(error) => tracing::warn!(
                    upload_id = %session.upload_id,
                    %error,
                    "multipart sweeper failed to abort a stale session"
                ),
            }
        }
        if !page_full || discovered == 0 {
            break;
        }
    }
    report
}

/// Why the scrub could not fully verify a version it walked. Every skip is COUNTED and emitted —
/// a silent `continue` is the defect this enum exists to make impossible (ARCH 26.4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SkipReason {
    /// The version's DEK is sealed under a key that is not on this node's ring right now (a
    /// rotation window, a not-yet-loaded ring entry). NOT corruption — reporting a rotation as bit
    /// rot is a false alarm on a healthy store — so it is skipped and re-tried next pass.
    KeyUnavailable,
    /// A multipart ETag (`{md5}-{n}`) is a hash OF HASHES, not a whole-object digest, so the
    /// re-read bytes cannot be compared to it. The blob is still read end-to-end (readability +
    /// AEAD/CRNB authentication), but the content hash is NOT verified. Composite-ETag
    /// verification is deliberately out of scope for this pass.
    CompositeEtag,
    /// The row carries no `storage_path` (nothing on this node's disk to re-read).
    NoBlob,
    /// A delete marker: a tombstone with no content.
    DeleteMarker,
    /// The listed version could not be re-read from the metadata store (concurrently deleted, or a
    /// transient store error). Nothing to verify, but it is not a verified version either.
    MetadataUnavailable,
    /// A transient filesystem failure (`BlobError::Io`, `OutOfSpace`) — an fd exhaustion spike, a
    /// full disk, an EIO blip. NOT corruption, for the same reason [`Self::KeyUnavailable`] is not:
    /// the bytes are very likely fine and the condition clears, so paging an operator for bit rot
    /// here is a false alarm on a healthy store. Re-tried next pass. A blob that is *missing*
    /// (`BlobError::NotFound`) is a different thing and IS reported corrupt.
    IoError,
}

impl SkipReason {
    /// The stable metric-label / log-field slug.
    const fn label(self) -> &'static str {
        match self {
            Self::KeyUnavailable => "key_unavailable",
            Self::CompositeEtag => "composite_etag",
            Self::NoBlob => "no_blob",
            Self::DeleteMarker => "delete_marker",
            Self::MetadataUnavailable => "metadata_unavailable",
            Self::IoError => "io_error",
        }
    }
    /// Every variant, in tally order.
    const ALL: [Self; 6] = [
        Self::KeyUnavailable,
        Self::CompositeEtag,
        Self::NoBlob,
        Self::DeleteMarker,
        Self::MetadataUnavailable,
        Self::IoError,
    ];
    const fn index(self) -> usize {
        match self {
            Self::KeyUnavailable => 0,
            Self::CompositeEtag => 1,
            Self::NoBlob => 2,
            Self::DeleteMarker => 3,
            Self::MetadataUnavailable => 4,
            Self::IoError => 5,
        }
    }
}

/// The verdict for one walked version.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ScrubOutcome {
    /// Re-read and the recomputed plaintext MD5 matched the stored ETag.
    Verified,
    /// Read or hash verification FAILED — bit rot, a broken container, a failed AEAD tag, or a
    /// tampered/unopenable key envelope. Carries a stable corruption-kind label.
    Corrupt(&'static str),
    /// Not verifiable this pass; see [`SkipReason`].
    Skipped(SkipReason),
}

/// The per-pass accounting. **INVARIANT: `scanned + skipped()` equals every version the pass
/// walked.** `scanned` counts versions whose bytes were fully re-read AND hash-verified (`corrupt`
/// is the subset of those that failed); `skipped` counts everything else, always with a reason. A
/// pass that verifies nothing must be visibly distinguishable from a pass that verified everything
/// and found nothing wrong — `scanned=0 skipped=N` says the first, `scanned=N skipped=0 corrupt=0`
/// the second. Do not add an un-counted `continue`.
#[derive(Default, Clone, Copy, Debug)]
struct ScrubTally {
    scanned: u64,
    corrupt: u64,
    skipped: [u64; SkipReason::ALL.len()],
}

impl ScrubTally {
    fn record(&mut self, outcome: ScrubOutcome) {
        match outcome {
            ScrubOutcome::Verified => self.scanned += 1,
            ScrubOutcome::Corrupt(_) => {
                self.scanned += 1;
                self.corrupt += 1;
            }
            ScrubOutcome::Skipped(r) => self.skipped[r.index()] += 1,
        }
    }
    fn skipped_total(&self) -> u64 {
        self.skipped.iter().sum()
    }
    fn skipped_for(&self, r: SkipReason) -> u64 {
        self.skipped[r.index()]
    }
    /// Total versions walked — the invariant's left-hand side.
    fn walked(&self) -> u64 {
        self.scanned + self.skipped_total()
    }
}

/// Periodically re-read every committed blob and verify it against the recorded ETag, turning silent
/// on-disk bit-rot into an observable event (ARCH 8.6/26.4). ENCRYPTED versions are verified too:
/// the pass unseals the version's DEK (`cairn_types::sse`) and re-reads through it, so an at-rest /
/// SSE-S3 / SSE-KMS node is covered exactly like a plaintext one. (The pass previously skipped
/// every version carrying an `sse_descriptor` on the theory that a real GET authenticates them —
/// true only for objects someone GETs, which is precisely the population a background scrub is NOT
/// for; on a `CAIRN_ENCRYPT_AT_REST` node that skipped 100% of the store, silently.)
///
/// Whatever cannot be verified is COUNTED with a reason and emitted, never dropped — see
/// [`ScrubTally`]. Enumeration is paged so memory stays flat regardless of store size.
/// Periodically refresh the release-update status (ARCH 28). `tokio::time::interval` ticks
/// immediately, so the first check runs on startup; thereafter every `interval`. Best-effort:
/// `run_once` swallows every error, so an air-gapped node simply never populates the status and the
/// console shows no update hint. A short per-attempt timeout keeps a hung feed from wedging the loop.
async fn update_check_loop(
    stack: Arc<AppStack>,
    url: String,
    interval: Duration,
    allow_internal: bool,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    let attempt_timeout = Duration::from_secs(10);
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                return;
            }
            _ = ticker.tick() => {}
        }
        if *shutdown.borrow() {
            return;
        }
        crate::update_check::run_once(
            &url,
            crate::CAIRN_VERSION,
            allow_internal,
            attempt_timeout,
            &stack.update_status,
        )
        .await;
    }
}

async fn scrub_loop(stack: Arc<AppStack>, interval: Duration, mut shutdown: watch::Receiver<bool>) {
    use cairn_types::meta::ListQuery;
    while wait_for_interval_or_shutdown(interval, &mut shutdown).await {
        let started = std::time::Instant::now();
        let mut tally = ScrubTally::default();
        // Enumeration failures are coverage lost, not versions verified — count them so a partial
        // pass is never mistaken for a clean one. A bucket-list failure yields an empty walk and
        // STILL emits the pass line below (rather than `continue`-ing silently), so an operator
        // alerting on `scanned == 0` always has a line to see.
        let mut enum_errors: u64 = 0;
        let buckets = match stack.meta.list_buckets(None).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "scrub: enumerating buckets failed");
                metrics::counter!("cairn_scrub_enumeration_errors_total", "stage" => "buckets")
                    .increment(1);
                enum_errors += 1;
                Vec::new()
            }
        };
        for bucket in &buckets {
            if *shutdown.borrow() {
                return;
            }
            let mut cursor: Option<String> = None;
            let mut vmarker: Option<String> = None;
            loop {
                if *shutdown.borrow() {
                    return;
                }
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
                        metrics::counter!("cairn_scrub_enumeration_errors_total", "stage" => "versions")
                            .increment(1);
                        enum_errors += 1;
                        break;
                    }
                };
                for s in &page.items {
                    if *shutdown.borrow() {
                        return;
                    }
                    let outcome = if s.is_delete_marker {
                        ScrubOutcome::Skipped(SkipReason::DeleteMarker)
                    } else {
                        match stack
                            .meta
                            .get_version(&bucket.name, &s.key, &s.version_id)
                            .await
                        {
                            Ok(Some(row)) => {
                                scrub_version(&*stack.blob, &*stack.crypto, &row).await
                            }
                            Ok(None) => ScrubOutcome::Skipped(SkipReason::MetadataUnavailable),
                            Err(e) => {
                                tracing::warn!(
                                    bucket = %bucket.name.as_str(),
                                    key = ?s.key.as_str(),
                                    version = %s.version_id.as_str(),
                                    error = %e,
                                    "scrub: re-reading version row failed"
                                );
                                ScrubOutcome::Skipped(SkipReason::MetadataUnavailable)
                            }
                        }
                    };
                    tally.record(outcome);
                    match outcome {
                        ScrubOutcome::Corrupt(kind) => {
                            metrics::counter!("cairn_scrub_corruption_total", "kind" => kind)
                                .increment(1);
                            tracing::error!(
                                bucket = %bucket.name.as_str(),
                                key = ?s.key.as_str(),
                                version = %s.version_id.as_str(),
                                kind,
                                "scrub: blob failed integrity verification"
                            );
                        }
                        ScrubOutcome::Skipped(SkipReason::KeyUnavailable) => {
                            // Loud but not an error: the operator needs to know a rotation window
                            // is shrinking coverage, without a corruption page.
                            tracing::warn!(
                                bucket = %bucket.name.as_str(),
                                key = ?s.key.as_str(),
                                version = %s.version_id.as_str(),
                                "scrub: data key unavailable, version not verified this pass"
                            );
                        }
                        _ => {}
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
        metrics::counter!("cairn_scrub_objects_total").increment(tally.scanned);
        // Emitted for EVERY reason, including zeros, so the series exists before the first skip and
        // an alert on it never has to distinguish "no skips" from "no scrub".
        for r in SkipReason::ALL {
            metrics::counter!("cairn_scrub_skipped_total", "reason" => r.label())
                .increment(tally.skipped_for(r));
        }
        // Register both enumeration-stage series each pass (increment(0) is a no-op on the count)
        // so an alert on them exists before the first failure, exactly like the skip series above.
        for stage in ["buckets", "versions"] {
            metrics::counter!("cairn_scrub_enumeration_errors_total", "stage" => stage)
                .increment(0);
        }
        metrics::histogram!("cairn_scrub_pass_seconds").record(started.elapsed().as_secs_f64());
        tracing::info!(
            scanned = tally.scanned,
            corrupt = tally.corrupt,
            skipped = tally.skipped_total(),
            walked = tally.walked(),
            skipped_key_unavailable = tally.skipped_for(SkipReason::KeyUnavailable),
            skipped_composite_etag = tally.skipped_for(SkipReason::CompositeEtag),
            skipped_no_blob = tally.skipped_for(SkipReason::NoBlob),
            skipped_delete_marker = tally.skipped_for(SkipReason::DeleteMarker),
            skipped_metadata_unavailable = tally.skipped_for(SkipReason::MetadataUnavailable),
            skipped_io_error = tally.skipped_for(SkipReason::IoError),
            enumeration_errors = enum_errors,
            "scrub pass complete"
        );
    }
}

/// Re-read one object version and verify it, decrypting through its own DEK when it is encrypted.
///
/// The read decompresses a CRNB-compressed blob and authenticates an encrypted one (so a corrupt
/// container or a flipped ciphertext byte fails the AEAD tag here), and for a single-part object the
/// recomputed PLAINTEXT MD5 is compared to the stored ETag — the right comparand under encryption
/// too, because the blob store hashes the plaintext BEFORE compressing/encrypting it, so the ETag is
/// the plaintext digest either way.
///
/// Error classification (deliberate — see [`SkipReason::KeyUnavailable`]):
///   * DEK sealed under an off-ring key (`UnknownKeyId`/`Key`/`KeyRotationRequired`) → **skipped**,
///     counted, warned. A rotation window is not bit rot.
///   * A malformed descriptor or a tampered/undersized envelope (`Decrypt` and friends) → **corrupt**:
///     it can never succeed, and it means the row's key material has been damaged.
///   * Open/read/hash failure → **corrupt**. Fails closed: no path returns "verified" on an error.
async fn scrub_version(
    blobs: &dyn cairn_types::traits::BlobStore,
    crypto: &dyn cairn_types::traits::Crypto,
    row: &cairn_types::object::ObjectVersionRow,
) -> ScrubOutcome {
    use cairn_types::error::{BlobError, CryptoError};
    use futures_util::StreamExt;
    use md5::{Digest, Md5};

    let Some(path) = row.storage_path.as_ref() else {
        return ScrubOutcome::Skipped(SkipReason::NoBlob);
    };
    // Resolve the DEK BEFORE reading a byte: reading an encrypted blob with `None` yields raw
    // ciphertext at exactly the plaintext length, which would hash to a mismatch and be reported as
    // corruption on a perfectly healthy store. Reuses the one shared descriptor module.
    let dek = match row.sse_descriptor.as_deref() {
        None => None,
        Some(json) => {
            let opened = cairn_types::sse::parse_descriptor(json)
                .and_then(|d| cairn_types::sse::open_dek(crypto, &d));
            match opened {
                Ok(k) => Some(k),
                Err(
                    CryptoError::UnknownKeyId | CryptoError::Key | CryptoError::KeyRotationRequired,
                ) => {
                    return ScrubOutcome::Skipped(SkipReason::KeyUnavailable);
                }
                Err(_) => return ScrubOutcome::Corrupt("dek_unopenable"),
            }
        }
    };

    let handle = match blobs
        .open_raw(
            path,
            None,
            cairn_types::blob::BlobCipher::from_dek(dek),
            &row.compression,
        )
        .await
    {
        Ok(h) => h,
        // A blob missing from under its row is real damage (`cairn integrity --repair` would drop the
        // dangling row), reported as corruption with its own kind. But a transient FS failure — fd
        // exhaustion, a full or flaky disk — is not bit rot; skip-and-count so it does not page an
        // operator, and re-try next pass.
        Err(BlobError::NotFound) => return ScrubOutcome::Corrupt("missing_blob"),
        Err(BlobError::Io(_) | BlobError::OutOfSpace) => {
            return ScrubOutcome::Skipped(SkipReason::IoError);
        }
        Err(_) => return ScrubOutcome::Corrupt("open_failed"),
    };
    let mut hasher = Md5::new();
    let mut body = handle.body;
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(bytes) => hasher.update(&bytes),
            // A decrypt/CRNB failure mid-stream is corruption; a bare I/O read error is transient.
            Err(BlobError::Io(_) | BlobError::OutOfSpace) => {
                return ScrubOutcome::Skipped(SkipReason::IoError);
            }
            Err(_) => return ScrubOutcome::Corrupt("read_failed"),
        }
    }
    // Single-part ETag is a bare hex MD5; multipart is "{md5}-{partcount}" — a hash of hashes, not
    // re-hashable from the assembled bytes. Those got the readability/AEAD check above but NOT a
    // content-hash check, so they are reported skipped-with-reason rather than counted as verified.
    let etag = row.etag.as_str();
    if etag.contains('-') {
        return ScrubOutcome::Skipped(SkipReason::CompositeEtag);
    }
    let computed = hex::encode(hasher.finalize());
    if computed.eq_ignore_ascii_case(etag.trim_matches('"')) {
        ScrubOutcome::Verified
    } else {
        ScrubOutcome::Corrupt("hash_mismatch")
    }
}

/// Periodically apply each bucket's lifecycle rules.
async fn lifecycle_loop(
    stack: Arc<AppStack>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let scanner = LifecycleScanner::new();
    let clock = SystemClock::new();
    while wait_for_interval_or_shutdown(interval, &mut shutdown).await {
        if *shutdown.borrow() {
            return;
        }
        let buckets = match stack.meta.list_buckets(None).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "lifecycle: listing buckets failed");
                continue;
            }
        };
        let mut configs = Vec::new();
        for b in buckets {
            if *shutdown.borrow() {
                return;
            }
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
        if *shutdown.borrow() {
            return;
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
async fn webhook_loop(
    stack: Arc<AppStack>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let engine = cairn_webhook::WebhookEngine::new(cairn_webhook::WebhookOpts::default());
    let sink = cairn_webhook::HttpWebhookSink::new(cairn_net::GuardConfig::new(
        stack.allow_internal_endpoints,
    ));
    let clock = SystemClock::new();
    while wait_for_interval_or_shutdown(interval, &mut shutdown).await {
        let mut delivered = 0u64;
        let mut failed = 0u64;
        let mut dropped = 0u64;
        for _ in 0..MAX_DRAIN_PASSES {
            if *shutdown.borrow() {
                break;
            }
            match engine
                .run_until_idle(&*stack.meta, &sink, &*stack.crypto, &clock, 1)
                .await
            {
                Ok(report) if report.is_idle() => break,
                Ok(report) => {
                    delivered += report.delivered;
                    failed += report.failed;
                    dropped += report.dropped;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "webhook drain failed");
                    break;
                }
            }
        }
        if delivered + failed + dropped > 0 {
            metrics::counter!("cairn_webhook_delivered_total").increment(delivered);
            metrics::counter!("cairn_webhook_failed_total").increment(failed);
            metrics::counter!("cairn_webhook_dropped_total").increment(dropped);
            tracing::info!(delivered, failed, dropped, "webhook delivery progressed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn test_sink_runtime() -> cairn_replication::ReplicationSinkRuntime {
        Config::default().replication_sink_runtime()
    }

    #[tokio::test]
    async fn task_set_joins_cooperative_workers_then_aborts_and_awaits_overruns() {
        let cooperative_finished = Arc::new(AtomicBool::new(false));
        let overrun_dropped = Arc::new(AtomicBool::new(false));
        let (overrun_started_tx, overrun_started_rx) = tokio::sync::oneshot::channel();
        let mut tasks = TaskSet::default();

        let cooperative_finished_task = Arc::clone(&cooperative_finished);
        tasks.spawn("cooperative", async move {
            cooperative_finished_task.store(true, Ordering::SeqCst);
        });
        let overrun_dropped_task = Arc::clone(&overrun_dropped);
        tasks.spawn("overrun", async move {
            let _drop_flag = DropFlag(overrun_dropped_task);
            let _ = overrun_started_tx.send(());
            std::future::pending::<()>().await;
        });

        overrun_started_rx.await.expect("overrun task started");
        tokio::task::yield_now().await;
        assert!(cooperative_finished.load(Ordering::SeqCst));

        // Both tasks have reached deterministic states before the short shared deadline begins.
        // The overrun's drop flag proves `abort()` was followed by awaiting the cancelled handle.
        let report = tasks.join_or_abort(Duration::from_millis(10)).await;
        assert_eq!(
            report,
            TaskShutdownReport {
                completed: 1,
                cancelled: 1,
                failed: 0,
            }
        );
        assert!(overrun_dropped.load(Ordering::SeqCst));
        assert!(tasks.tasks.is_empty(), "no background handle may detach");
    }

    #[tokio::test]
    async fn dropping_task_set_aborts_workers_instead_of_detaching_them() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let dropped_task = Arc::clone(&dropped);
        let mut tasks = TaskSet::default();
        tasks.spawn("orphan guard", async move {
            let _drop_flag = DropFlag(dropped_task);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("worker started");

        drop(tasks);
        tokio::task::yield_now().await;
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn interval_wait_never_starts_a_pass_after_shutdown_is_visible() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).unwrap();

        assert!(!wait_for_interval_or_shutdown(Duration::from_secs(3_600), &mut shutdown_rx).await);
    }

    #[test]
    fn finalization_report_cannot_claim_success_after_any_incomplete_tail() {
        assert!(FinalizeReport::default().is_complete());
        assert!(
            !FinalizeReport {
                request_metric_failures: 1,
                ..FinalizeReport::default()
            }
            .is_complete()
        );
        assert!(
            !FinalizeReport {
                checkpoint_busy: 1,
                ..FinalizeReport::default()
            }
            .is_complete()
        );
        assert!(
            !FinalizeReport {
                timed_out: true,
                ..FinalizeReport::default()
            }
            .is_complete()
        );
        assert!(
            !BackgroundShutdownReport {
                worker_report: TaskShutdownReport {
                    cancelled: 1,
                    ..TaskShutdownReport::default()
                },
                finalization: FinalizeReport::default(),
            }
            .is_complete(),
            "a cancelled worker must reach the process-level shutdown result"
        );
    }

    fn target(name: &str, dest_bucket: &str, endpoint: &str) -> ReplicationTarget {
        ReplicationTarget {
            name: name.to_owned(),
            endpoint: endpoint.to_owned(),
            region: "us-east-1".to_owned(),
            dest_bucket: dest_bucket.to_owned(),
            access_key: "AKID".to_owned(),
            secret: "secret".into(),
            ca_path: None,
            insecure_skip_verify: false,
        }
    }

    fn sinks(targets: &[ReplicationTarget]) -> Vec<(ReplicationTarget, Arc<HttpS3Sink>)> {
        let runtime = test_sink_runtime();
        targets
            .iter()
            .map(|t| {
                let sink = HttpS3Sink::new(target_sink_cfg(t, false, false), runtime.clone())
                    .expect("target sink builds");
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
        let cfg = target_sink_cfg(&t, false, false);
        assert_eq!(cfg.endpoint, "https://s3.secure.example");
        assert_eq!(cfg.dest_bucket, "mirror");
        assert!(cfg.dest_buckets.is_empty());
        assert!(cfg.insecure_skip_verify);
        assert!(cfg.ca_cert_path.is_none());

        let mut t = target("ca", "mirror", "https://s3.ca.example");
        t.ca_path = Some(std::path::PathBuf::from("/etc/ca.pem"));
        let cfg = target_sink_cfg(&t, false, false);
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
            HttpS3Sink::new(
                cairn_replication::S3SinkConfig {
                    endpoint: "http://default.example:9000".to_owned(),
                    dest_bucket: "fallback".to_owned(),
                    dest_buckets: HashMap::new(),
                    region: "us-east-1".to_owned(),
                    access_key_id: "AKID".to_owned(),
                    secret_access_key: "secret".into(),
                    ca_cert_path: None,
                    ca_cert_pem: None,
                    insecure_skip_verify: false,
                    allow_internal_endpoints: false,
                    allow_plaintext_sse_over_http: false,
                },
                test_sink_runtime(),
            )
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
            HttpS3Sink::new(
                cairn_replication::S3SinkConfig {
                    endpoint: endpoint.to_owned(),
                    dest_bucket: dest.to_owned(),
                    dest_buckets: HashMap::new(),
                    region: "us-east-1".to_owned(),
                    access_key_id: "AKID".to_owned(),
                    secret_access_key: "secret".into(),
                    ca_cert_path: None,
                    ca_cert_pem: None,
                    insecure_skip_verify: false,
                    allow_internal_endpoints: false,
                    allow_plaintext_sse_over_http: false,
                },
                test_sink_runtime(),
            )
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
        cfg.replication_secret = Some("secret".into());
        let built = single_target_sink_cfg(&cfg).expect("full triple yields a config");
        assert_eq!(built.endpoint, "http://backup:9000");
        // The TLS trust defaults are the safe webpki path for the single-target node->node case.
        assert!(built.ca_cert_path.is_none());
        assert!(!built.insecure_skip_verify);
    }

    // ---------------------------------------------------------------------------------------
    // The integrity scrub (ARCH 8.6 / 26.4).
    //
    // These exercise `scrub_version` against a REAL local blob store and a REAL master-key ring,
    // because the whole defect class here is "the pass claims success without reading anything":
    // an in-memory double that ignores the DEK could not tell a verified encrypted object from a
    // skipped one. Every test below fails on the pre-fix code, which skipped any version carrying
    // an `sse_descriptor` outright.
    // ---------------------------------------------------------------------------------------
    mod scrub {
        use super::super::{ScrubOutcome, ScrubTally, SkipReason, scrub_version};
        use cairn_blob::LocalBlobStore;
        use cairn_crypto::SystemCrypto;
        use cairn_types::ChecksumSet;
        use cairn_types::blob::StageOptions;
        use cairn_types::id::{BucketName, ObjectKey, StoragePath, VersionId};
        use cairn_types::object::{ETag, ObjectVersionRow, StorageClass};
        use cairn_types::traits::{BlobStore, Crypto};
        use cairn_types::{CompressionDescriptor, Timestamp, UserId};

        fn crypto_with_key_id(id: u16, byte: u8) -> SystemCrypto {
            SystemCrypto::from_ring(vec![(id, [byte; 32].into())], id, id, 0).expect("ring builds")
        }

        /// Seal a DEK into the persisted descriptor exactly as the S3 write path does.
        fn descriptor_for(crypto: &dyn Crypto, dek: &[u8; 32]) -> String {
            use base64::Engine;
            let sealed = crypto.seal(dek).expect("seal dek");
            serde_json::to_string(&cairn_types::sse::SseDescriptor {
                alg: "AES256-GCM".to_owned(),
                wrapped_dek_b64: base64::engine::general_purpose::STANDARD
                    .encode(&sealed.ciphertext),
                ..cairn_types::sse::SseDescriptor::default()
            })
            .expect("descriptor serializes")
        }

        fn row_for(
            path: StoragePath,
            etag: ETag,
            size: u64,
            compression: CompressionDescriptor,
            sse_descriptor: Option<String>,
        ) -> ObjectVersionRow {
            ObjectVersionRow {
                id: "row-1".to_owned(),
                bucket: BucketName::parse("scrub-bucket").unwrap(),
                key: ObjectKey::parse("k").unwrap(),
                version_id: VersionId::null(),
                is_latest: true,
                is_delete_marker: false,
                size_logical: size,
                size_physical: size,
                etag,
                content_type: "application/octet-stream".to_owned(),
                content_encoding: None,
                cache_control: None,
                content_disposition: None,
                content_language: None,
                expires: None,
                storage_path: Some(path),
                compression,
                storage_class: StorageClass::Standard,
                cold_locator: None,
                owner_id: UserId("owner".to_owned()),
                user_metadata: Vec::new(),
                acl: None,
                checksums: Vec::new(),
                sse_descriptor,
                replication_status: None,
                replicated_at: None,
                created_at: Timestamp(0),
                updated_at: Timestamp(0),
            }
        }

        /// Stage a body (optionally encrypted / compressed) and return the row describing it.
        async fn staged_row(
            blobs: &LocalBlobStore,
            body: &[u8],
            dek: Option<cairn_types::SecretKey32>,
            descriptor: Option<String>,
            compression: Option<cairn_types::bucket::CompressionPolicy>,
        ) -> ObjectVersionRow {
            let bucket = BucketName::parse("scrub-bucket").unwrap();
            let bytes = bytes::Bytes::copy_from_slice(body);
            let stream: cairn_types::BodyStream =
                Box::pin(futures_util::stream::once(async move {
                    Ok::<_, cairn_types::error::BodyError>(bytes)
                }));
            let staged = blobs
                .stage(
                    &bucket,
                    stream,
                    StageOptions {
                        compression,
                        extra_checksums: ChecksumSet::none(),
                        size_ceiling: 1 << 30,
                        content_type: "application/octet-stream".to_owned(),
                        encryption: dek,
                        content_length: None,
                    },
                )
                .await
                .expect("stage");
            row_for(
                staged.storage_path,
                staged.etag,
                staged.size_logical,
                staged.compression,
                descriptor,
            )
        }

        /// Flip one byte of the blob's on-disk representation — simulated bit rot.
        fn flip_a_byte(root: &std::path::Path, row: &ObjectVersionRow) {
            let p = root.join(row.storage_path.as_ref().unwrap().as_str());
            let mut data = std::fs::read(&p).expect("read blob");
            let last = data.len() - 1;
            data[last] ^= 0xff;
            std::fs::write(&p, &data).expect("write blob");
        }

        /// The core of the fix: an ENCRYPTED version is actually re-read (through its DEK) and
        /// hash-verified. Pre-fix this version was skipped entirely and never counted.
        #[tokio::test]
        async fn an_encrypted_version_is_verified() {
            let dir = tempfile::tempdir().unwrap();
            let blobs = LocalBlobStore::open(dir.path()).await.unwrap();
            let crypto = crypto_with_key_id(1, 0xa1);
            let dek = [7u8; 32];
            let row = staged_row(
                &blobs,
                b"encrypted payload that the scrub must actually read",
                Some(dek.into()),
                Some(descriptor_for(&crypto, &dek)),
                None,
            )
            .await;
            assert_eq!(
                scrub_version(&blobs, &crypto, &row).await,
                ScrubOutcome::Verified
            );
        }

        /// Bit rot in an encrypted blob must be REPORTED, not skipped: the AEAD tag fails on the
        /// re-read, which is exactly the signal a cold-data scrub exists to surface.
        #[tokio::test]
        async fn a_corrupted_encrypted_blob_is_reported_corrupt() {
            let dir = tempfile::tempdir().unwrap();
            let blobs = LocalBlobStore::open(dir.path()).await.unwrap();
            let crypto = crypto_with_key_id(1, 0xa1);
            let dek = [9u8; 32];
            let row = staged_row(
                &blobs,
                b"encrypted payload that will be corrupted on disk",
                Some(dek.into()),
                Some(descriptor_for(&crypto, &dek)),
                None,
            )
            .await;
            flip_a_byte(dir.path(), &row);
            assert!(
                matches!(
                    scrub_version(&blobs, &crypto, &row).await,
                    ScrubOutcome::Corrupt(_)
                ),
                "a flipped ciphertext byte must fail closed, never verify"
            );
        }

        /// A COMPRESSED plaintext blob is verified as well (the read decompresses), and rot in it
        /// is reported.
        #[tokio::test]
        async fn a_compressed_version_is_verified_and_its_rot_reported() {
            let dir = tempfile::tempdir().unwrap();
            let blobs = LocalBlobStore::open(dir.path()).await.unwrap();
            let crypto = crypto_with_key_id(1, 0xa1);
            let body = "compress me ".repeat(4096);
            let row = staged_row(
                &blobs,
                body.as_bytes(),
                None,
                None,
                Some(cairn_types::bucket::CompressionPolicy::default()),
            )
            .await;
            assert_ne!(
                row.compression,
                CompressionDescriptor::Uncompressed,
                "the fixture must actually be compressed for this to test anything"
            );
            assert_eq!(
                scrub_version(&blobs, &crypto, &row).await,
                ScrubOutcome::Verified
            );
            flip_a_byte(dir.path(), &row);
            assert!(matches!(
                scrub_version(&blobs, &crypto, &row).await,
                ScrubOutcome::Corrupt(_)
            ));
        }

        /// A key that is merely off this node's ring right now (a rotation window) is SKIPPED with
        /// a reason — never reported as corruption. A false bit-rot page on a healthy store is its
        /// own incident.
        #[tokio::test]
        async fn an_unknown_key_id_is_skipped_not_corrupt() {
            let dir = tempfile::tempdir().unwrap();
            let blobs = LocalBlobStore::open(dir.path()).await.unwrap();
            let sealing = crypto_with_key_id(1, 0xa1);
            let dek = [3u8; 32];
            let row = staged_row(
                &blobs,
                b"sealed under a key this node no longer holds",
                Some(dek.into()),
                Some(descriptor_for(&sealing, &dek)),
                None,
            )
            .await;
            // A ring holding a DIFFERENT key id: `open` reports UnknownKeyId, not Decrypt.
            let other_ring = crypto_with_key_id(2, 0xb2);
            assert_eq!(
                scrub_version(&blobs, &other_ring, &row).await,
                ScrubOutcome::Skipped(SkipReason::KeyUnavailable)
            );
        }

        /// A tampered/undamaged-but-unparseable descriptor can never succeed: that is damage, not a
        /// rotation, so it is corruption.
        #[tokio::test]
        async fn a_malformed_descriptor_is_corruption() {
            let dir = tempfile::tempdir().unwrap();
            let blobs = LocalBlobStore::open(dir.path()).await.unwrap();
            let crypto = crypto_with_key_id(1, 0xa1);
            let dek = [4u8; 32];
            let mut row = staged_row(&blobs, b"body", Some(dek.into()), None, None).await;
            row.sse_descriptor = Some("{not-json".to_owned());
            assert_eq!(
                scrub_version(&blobs, &crypto, &row).await,
                ScrubOutcome::Corrupt("dek_unopenable")
            );
        }

        /// Plaintext coverage must not regress, and a plaintext bit flip is still a hash mismatch.
        #[tokio::test]
        async fn a_plaintext_version_is_verified_and_its_rot_reported() {
            let dir = tempfile::tempdir().unwrap();
            let blobs = LocalBlobStore::open(dir.path()).await.unwrap();
            let crypto = crypto_with_key_id(1, 0xa1);
            let row = staged_row(&blobs, b"plain bytes on disk", None, None, None).await;
            assert_eq!(
                scrub_version(&blobs, &crypto, &row).await,
                ScrubOutcome::Verified
            );
            flip_a_byte(dir.path(), &row);
            assert_eq!(
                scrub_version(&blobs, &crypto, &row).await,
                ScrubOutcome::Corrupt("hash_mismatch")
            );
        }

        /// A composite (multipart) ETag is not a whole-object digest, so the content hash is NOT
        /// checked — that must be visible as a counted skip, not silently pass as "verified".
        #[tokio::test]
        async fn a_composite_etag_is_skipped_with_a_reason() {
            let dir = tempfile::tempdir().unwrap();
            let blobs = LocalBlobStore::open(dir.path()).await.unwrap();
            let crypto = crypto_with_key_id(1, 0xa1);
            let mut row = staged_row(&blobs, b"assembled body", None, None, None).await;
            row.etag = ETag::from_md5_hex(format!("{}-2", "0".repeat(32)));
            assert_eq!(
                scrub_version(&blobs, &crypto, &row).await,
                ScrubOutcome::Skipped(SkipReason::CompositeEtag)
            );
        }

        /// A row with no blob is counted, not dropped.
        #[tokio::test]
        async fn a_row_without_a_blob_is_skipped_with_a_reason() {
            let dir = tempfile::tempdir().unwrap();
            let blobs = LocalBlobStore::open(dir.path()).await.unwrap();
            let crypto = crypto_with_key_id(1, 0xa1);
            let mut row = staged_row(&blobs, b"body", None, None, None).await;
            row.storage_path = None;
            assert_eq!(
                scrub_version(&blobs, &crypto, &row).await,
                ScrubOutcome::Skipped(SkipReason::NoBlob)
            );
        }

        /// A blob MISSING from under its row (present `storage_path`, absent file) is real damage —
        /// reported corrupt with its own kind — NOT the transient-I/O skip. This is the boundary the
        /// review flagged: only `NotFound` is corruption; an `Io`/`OutOfSpace` blip is skipped.
        #[tokio::test]
        async fn a_blob_missing_from_disk_is_reported_corrupt() {
            let dir = tempfile::tempdir().unwrap();
            let blobs = LocalBlobStore::open(dir.path()).await.unwrap();
            let crypto = crypto_with_key_id(1, 0xa1);
            let row = staged_row(&blobs, b"body that then vanishes", None, None, None).await;
            let p = dir.path().join(row.storage_path.as_ref().unwrap().as_str());
            std::fs::remove_file(&p).expect("remove blob");
            assert_eq!(
                scrub_version(&blobs, &crypto, &row).await,
                ScrubOutcome::Corrupt("missing_blob")
            );
        }

        /// THE ACCOUNTING INVARIANT: every walked version lands in exactly one bucket, and a pass
        /// that verified nothing is distinguishable from one that verified everything cleanly.
        #[test]
        fn scanned_plus_skipped_accounts_for_every_walked_version() {
            let mut t = ScrubTally::default();
            let outcomes = [
                ScrubOutcome::Verified,
                ScrubOutcome::Verified,
                ScrubOutcome::Corrupt("hash_mismatch"),
                ScrubOutcome::Skipped(SkipReason::KeyUnavailable),
                ScrubOutcome::Skipped(SkipReason::CompositeEtag),
                ScrubOutcome::Skipped(SkipReason::DeleteMarker),
                ScrubOutcome::Skipped(SkipReason::NoBlob),
                ScrubOutcome::Skipped(SkipReason::MetadataUnavailable),
                ScrubOutcome::Skipped(SkipReason::IoError),
            ];
            for o in outcomes {
                t.record(o);
            }
            assert_eq!(t.walked(), outcomes.len() as u64);
            assert_eq!(t.scanned, 3, "corrupt versions were still fully read");
            assert_eq!(t.corrupt, 1);
            assert_eq!(t.skipped_total(), 6);
            for r in SkipReason::ALL {
                assert_eq!(t.skipped_for(r), 1, "reason {} miscounted", r.label());
            }

            // The signal the defect erased: an all-skipped pass reports a non-zero denominator.
            let mut nothing = ScrubTally::default();
            nothing.record(ScrubOutcome::Skipped(SkipReason::KeyUnavailable));
            assert_eq!(nothing.scanned, 0);
            assert_eq!(nothing.walked(), 1);
        }
    }
}
