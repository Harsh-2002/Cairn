//! The background S3-import worker: claims pending import jobs and runs the [`cairn_import`] engine
//! into this node, persisting per-bucket progress + cursors as the resumable checkpoint. A single
//! claimer (one task) keeps job claiming race-free without a claim-outcome mutation.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cairn_crypto::SystemClock;
use cairn_import::{
    BucketPlan, HttpS3Source, ImportEngine, ImportOpts, ImportTimeouts, ProgressSink, SourceConfig,
    SourceReader,
};
use cairn_types::auth::Principal;
use cairn_types::crypto::Nonce;
use cairn_types::id::UserId;
use cairn_types::meta::{ImportBucketProgress, ImportJobRecord, ImportState};
use cairn_types::time::Timestamp;
use cairn_types::traits::{Clock, Crypto};
use cairn_types::{MAX_IMPORT_JOB_PRUNE_BATCH, Mutation, MutationOutcome};
use futures_util::StreamExt;

use crate::import_dest::{LocalDestWriter, import_principal};
use crate::stack::AppStack;

/// The lease a claimed (running) job holds; a job left running past this after a crash is reclaimed.
const IMPORT_LEASE_SECS: i64 = 300;
/// Maximum bounded writer transactions retention maintenance spends per heartbeat.
const IMPORT_PRUNE_BATCHES_PER_HEARTBEAT: usize = 4;

/// Config the loop needs (a subset of the server config, resolved once).
#[derive(Debug, Clone)]
pub struct ImportLoopConfig {
    /// Poll heartbeat between drains, in seconds.
    pub poll_interval_secs: u64,
    /// Retain terminal job history for this many seconds before pruning it.
    pub retention_secs: u64,
    /// Worker count to use when a job requests 0 (server default).
    pub default_workers: usize,
    /// Hard cap on a job's worker count.
    pub max_workers: usize,
    /// The global in-flight-copy ceiling.
    pub global_max_inflight: usize,
    /// Source/copy phase deadlines.
    pub timeouts: ImportTimeouts,
    /// The root admin access key, to build the write principal.
    pub root_access_key: String,
}

fn lease_from(now: Timestamp) -> Timestamp {
    Timestamp(now.0 + IMPORT_LEASE_SECS * 1000)
}

fn shutdown_observed(
    change: Result<(), tokio::sync::watch::error::RecvError>,
    shutdown: &tokio::sync::watch::Receiver<bool>,
) -> bool {
    change.is_err() || *shutdown.borrow()
}

/// The import worker loop: reclaim orphaned jobs at startup, then drain pending jobs, waking on the
/// import notify or the poll heartbeat.
pub async fn import_loop(
    stack: Arc<AppStack>,
    cfg: ImportLoopConfig,
    notify: Arc<tokio::sync::Notify>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if *shutdown.borrow() {
        return;
    }
    reclaim_running(&stack, &shutdown).await;
    if *shutdown.borrow() {
        return;
    }
    prune_import_history(&stack, cfg.retention_secs, &shutdown).await;
    let interval = Duration::from_secs(cfg.poll_interval_secs.max(1));
    let mut heartbeat = tokio::time::interval(interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // `interval`'s first tick is immediate. The explicit startup prune above already did that work;
    // consume it so the next maintenance pass happens after one configured interval.
    heartbeat.tick().await;
    let global = Arc::new(tokio::sync::Semaphore::new(cfg.global_max_inflight.max(1)));
    // Bound active job futures while keeping at least one later job runnable even when the operator
    // deliberately sets the object budget to one. Jobs share `global`, whose FIFO semaphore ensures
    // a timed-out/stalled job cannot monopolize the object slot forever (CAIRN-AUD-019).
    let max_active_jobs = cfg.global_max_inflight.max(2);
    let mut active = futures_util::stream::FuturesUnordered::new();

    loop {
        if *shutdown.borrow() {
            return;
        }

        while active.len() < max_active_jobs {
            if *shutdown.borrow() {
                // A claim racing the signal is durable and is recovered to pending on restart, but
                // never deliberately start another claim after observing shutdown.
                return;
            }
            let Some(id) = claim_next(&stack).await else {
                break;
            };
            active.push(run_one(stack.clone(), cfg.clone(), id, global.clone()));
        }

        if active.is_empty() {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if shutdown_observed(changed, &shutdown) {
                        return;
                    }
                }
                _ = notify.notified() => {}
                _ = heartbeat.tick() => {
                    prune_import_history(&stack, cfg.retention_secs, &shutdown).await;
                }
            }
        } else {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if shutdown_observed(changed, &shutdown) {
                        return;
                    }
                }
                _ = active.next() => {}
                _ = notify.notified() => {}
                _ = heartbeat.tick() => {
                    prune_import_history(&stack, cfg.retention_secs, &shutdown).await;
                }
            }
        }
    }
}

/// A freshly-started process has no live worker, so any job left `running` is a crash orphan — reset
/// it to `pending` so it resumes from its stored cursor (mirrors `RecoverClaimedReplication`).
async fn reclaim_running(stack: &Arc<AppStack>, shutdown: &tokio::sync::watch::Receiver<bool>) {
    let now = SystemClock::new().now();
    loop {
        if *shutdown.borrow() {
            return;
        }
        let id = match stack.meta.next_import_job_id(ImportState::Running).await {
            Ok(Some(id)) => id,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(%error, "failed to select orphaned import job");
                return;
            }
        };
        if *shutdown.borrow() {
            return;
        }
        if let Err(error) = stack
            .meta
            .submit(set_state(&id, ImportState::Pending, None, None, now))
            .await
        {
            tracing::warn!(job = %id, %error, "failed to reclaim orphaned import job");
            return;
        }
    }
}

async fn next_pending(stack: &Arc<AppStack>) -> Option<String> {
    stack
        .meta
        .next_import_job_id(ImportState::Pending)
        .await
        .ok()
        .flatten()
}

fn retention_cutoff(now: Timestamp, retention_secs: u64) -> i64 {
    let retention_secs = i64::try_from(retention_secs).unwrap_or(i64::MAX);
    now.plus_secs(-retention_secs).0
}

/// Prune terminal history on the import heartbeat. Pending/running rows are protected by the
/// mutation predicate, so an aggressive retention never removes runnable work.
async fn prune_import_history(
    stack: &Arc<AppStack>,
    retention_secs: u64,
    shutdown: &tokio::sync::watch::Receiver<bool>,
) {
    let before_ms = retention_cutoff(SystemClock::new().now(), retention_secs);
    for _ in 0..IMPORT_PRUNE_BATCHES_PER_HEARTBEAT {
        if *shutdown.borrow() {
            return;
        }
        match stack
            .meta
            .submit(Mutation::PruneImportJobs {
                before_ms,
                limit: MAX_IMPORT_JOB_PRUNE_BATCH,
            })
            .await
        {
            Ok(MutationOutcome::ImportJobsPruned(rows))
                if rows < u64::from(MAX_IMPORT_JOB_PRUNE_BATCH) =>
            {
                return;
            }
            Ok(MutationOutcome::ImportJobsPruned(_)) => {}
            Ok(other) => {
                tracing::warn!(?other, "unexpected import prune outcome");
                return;
            }
            Err(error) => {
                tracing::warn!(%error, "failed to prune terminal import history");
                return;
            }
        }
    }
}

/// Select and claim one job before its future is added to the active set. Claiming synchronously in
/// the sole scheduler prevents the same pending row from being selected more than once.
async fn claim_next(stack: &Arc<AppStack>) -> Option<String> {
    let id = next_pending(stack).await?;
    let now = SystemClock::new().now();
    stack
        .meta
        .submit(set_state(
            &id,
            ImportState::Running,
            None,
            Some(lease_from(now)),
            now,
        ))
        .await
        .ok()?;
    Some(id)
}

fn set_state(
    id: &str,
    state: ImportState,
    last_error: Option<String>,
    lease_until: Option<Timestamp>,
    updated_at: Timestamp,
) -> Mutation {
    Mutation::SetImportJobState {
        id: id.to_owned(),
        state,
        last_error,
        lease_until,
        updated_at,
    }
}

async fn fail_job(stack: &Arc<AppStack>, id: &str, msg: String) {
    let now = SystemClock::new().now();
    tracing::warn!(job = %id, error = %msg, "import job failed");
    let _ = stack
        .meta
        .submit(set_state(id, ImportState::Failed, Some(msg), None, now))
        .await;
}

async fn run_one(
    stack: Arc<AppStack>,
    cfg: ImportLoopConfig,
    id: String,
    global: Arc<tokio::sync::Semaphore>,
) {
    let clock = SystemClock::new();
    let Ok(Some(record)) = stack.meta.get_import_job_record(&id).await else {
        return;
    };

    // Open the sealed source secret (CRK1 → NULL nonce), used only here to sign source requests.
    let secret = match stack.crypto.open(
        &record.secret_ciphertext,
        &Nonce(record.secret_nonce.clone().unwrap_or_default()),
    ) {
        Ok(pt) => match String::from_utf8(pt.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                return fail_job(&stack, &id, "source secret is not valid UTF-8".to_owned()).await;
            }
        },
        Err(e) => {
            return fail_job(&stack, &id, format!("unsealing source secret failed: {e}")).await;
        }
    };

    let source = match HttpS3Source::new(SourceConfig {
        endpoint: record.source_endpoint.clone(),
        region: record.source_region.clone(),
        access_key_id: record.access_key_id.clone(),
        secret_access_key: secret.into(),
        ca_cert_pem: record.ca_cert_pem.clone(),
        insecure_skip_verify: record.insecure_skip_verify,
        allow_internal_endpoints: stack.allow_internal_endpoints,
        timeouts: cfg.timeouts,
    }) {
        Ok(s) => s,
        Err(e) => {
            return fail_job(&stack, &id, format!("building source client: {e}")).await;
        }
    };

    let plan = match build_plan(&source, &record).await {
        Ok(p) => p,
        Err(e) => return fail_job(&stack, &id, e).await,
    };

    let principal = build_principal(&stack, &cfg.root_access_key).await;
    let dest = LocalDestWriter::new(stack.clone(), principal);

    let workers = if record.workers == 0 {
        cfg.default_workers
    } else {
        (record.workers as usize).clamp(1, cfg.max_workers)
    };
    let opts = ImportOpts {
        object_workers: workers,
        bucket_concurrency: 4.min(plan.len().max(1)),
        global_max_inflight: cfg.global_max_inflight,
        whole_object_timeout: cfg.timeouts.whole_object,
        ..ImportOpts::default()
    };

    let progress = DbProgress {
        stack: stack.clone(),
        job_id: id.clone(),
    };
    let report = ImportEngine::new(opts)
        .with_global_semaphore(global)
        .run(&source, &dest, &plan, &progress)
        .await;

    // Terminal state: cancelled (the sink observed a cancel) or completed. Per-object failures are
    // recorded in the buckets and never fail the whole job.
    let now = clock.now();
    let final_state = if report.cancelled {
        ImportState::Cancelled
    } else {
        ImportState::Completed
    };
    let _ = stack
        .meta
        .submit(set_state(&id, final_state, None, None, now))
        .await;
    tracing::info!(
        job = %id, objects = report.objects_copied, bytes = report.bytes_copied,
        cancelled = report.cancelled, "import job finished"
    );
}

/// Build the per-bucket plan: use the job's stored buckets (with their resume cursors), or — when the
/// job listed no buckets — enumerate every source bucket and map each to a same-named destination.
async fn build_plan(
    source: &HttpS3Source,
    record: &ImportJobRecord,
) -> Result<Vec<BucketPlan>, String> {
    if !record.buckets.is_empty() {
        return Ok(record
            .buckets
            .iter()
            .filter(|b| b.state != ImportState::Completed)
            .map(|b| BucketPlan {
                source_bucket: b.source_bucket.clone(),
                dest_bucket: b.dest_bucket.clone(),
                cursor: b.cursor.clone(),
                objects_done: b.objects_done,
                bytes_done: b.bytes_done,
            })
            .collect());
    }
    let buckets = source
        .list_buckets()
        .await
        .map_err(|e| format!("listing source buckets: {e}"))?;
    Ok(buckets
        .into_iter()
        .map(|name| BucketPlan {
            source_bucket: name.clone(),
            dest_bucket: name,
            cursor: None,
            objects_done: 0,
            bytes_done: 0,
        })
        .collect())
}

/// The principal import writes as: the root administrator (so imported buckets are root-owned and the
/// writes authorize via the owner/admin short-circuit).
async fn build_principal(stack: &Arc<AppStack>, root_access_key: &str) -> Principal {
    match stack.meta.user_by_bearer_key(root_access_key).await {
        Ok(Some(u)) => import_principal(u.user.id, u.user.access_key_id),
        _ => import_principal(UserId::generate(), "import".to_owned()),
    }
}

/// Persists progress + renews the lease after every page, and reports cancellation requests.
struct DbProgress {
    stack: Arc<AppStack>,
    job_id: String,
}

#[async_trait]
impl ProgressSink for DbProgress {
    async fn report(&self, buckets: &[ImportBucketProgress]) -> bool {
        let now = SystemClock::new().now();
        let (objects_done, objects_total, bytes_done, bytes_total) =
            buckets
                .iter()
                .fold((0u64, 0u64, 0u64, 0u64), |(a, b, c, d), x| {
                    (
                        a + x.objects_done,
                        b + x.objects_total,
                        c + x.bytes_done,
                        d + x.bytes_total,
                    )
                });
        let last_error = buckets.iter().rev().find_map(|b| b.last_error.clone());
        let _ = self
            .stack
            .meta
            .submit(Mutation::UpdateImportJobProgress {
                id: self.job_id.clone(),
                buckets: buckets.to_vec(),
                objects_done,
                objects_total,
                bytes_done,
                bytes_total,
                last_error,
                lease_until: Some(lease_from(now)),
                updated_at: now,
            })
            .await;
        // Cancellation: the DELETE handler flips the job to Cancelled; the sink observes it here.
        !matches!(
            self.stack.meta.get_import_job(&self.job_id).await,
            Ok(Some(j)) if j.state == ImportState::Cancelled
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{retention_cutoff, shutdown_observed};
    use cairn_types::time::Timestamp;

    #[test]
    fn retention_cutoff_uses_configured_seconds_and_saturates() {
        assert_eq!(retention_cutoff(Timestamp(1_000_000), 60), 940_000);
        assert_eq!(
            retention_cutoff(Timestamp::EPOCH, u64::MAX),
            i64::MIN,
            "an extreme retention must keep everything rather than wrap into the future"
        );
    }

    #[tokio::test]
    async fn closed_shutdown_sender_is_a_stop_event_not_a_hot_loop() {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        drop(shutdown_tx);

        let change = shutdown_rx.changed().await;
        assert!(shutdown_observed(change, &shutdown_rx));
    }
}
