//! The single, serialized, group-committing writer (ARCH §7.2). All mutations are submitted
//! to one writer task that owns the only write connection. It drains its queue, applies every
//! waiting mutation in one transaction — each wrapped in its own savepoint so a logical
//! failure rolls back only itself — commits once with a single durability barrier, and only
//! then acknowledges every caller whose mutation was in that batch.

use crate::apply::apply;
use cairn_types::MetaError;
use cairn_types::meta::{Mutation, MutationOutcome};
use rusqlite::Connection;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

type Ack = oneshot::Sender<Result<MutationOutcome, MetaError>>;
type WriteRequest = (Mutation, Ack);

const MAX_BATCH: usize = 256;

/// The result of a `PRAGMA wal_checkpoint(TRUNCATE)` run on the writer thread (ARCH §8.4/§11.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCheckpointStats {
    /// `true` if the checkpoint could not complete because the WAL was in use by a reader.
    pub busy: bool,
    /// Total frames in the WAL at the start of the checkpoint.
    pub log_frames: u64,
    /// Frames successfully moved into the database file (and, for TRUNCATE, reset to zero).
    pub checkpointed_frames: u64,
}

/// A control message multiplexed onto the writer's queue so it runs on the writer thread,
/// serialized with — and never contending against — ordinary mutations.
enum Control {
    /// Run a truncating WAL checkpoint and report its frame counts.
    Checkpoint(oneshot::Sender<Result<WalCheckpointStats, MetaError>>),
    /// A liveness probe: the writer simply acks, proving its thread is draining the queue. Used by
    /// the readiness check so `/readyz` reflects a responsive writer, not just a readable pool.
    Probe(oneshot::Sender<()>),
}

/// One unit of work for the writer loop: either a batched mutation or a control message.
///
/// The `Write` variant is intrinsically large (it carries a `Mutation`) and is the hot path;
/// boxing it to equalise variant sizes would add a heap allocation per write for no benefit,
/// since control messages are rare. So the size disparity is accepted deliberately.
#[allow(clippy::large_enum_variant)]
enum Job {
    Write(WriteRequest),
    Control(Control),
}

/// A handle to the writer task. Cloneable; the writer shuts down when the last handle drops.
#[derive(Clone, Debug)]
pub struct Writer {
    tx: mpsc::Sender<Job>,
    /// Number of mutations enqueued but not yet drained into a commit batch. Incremented on
    /// `submit` and decremented as the writer loop pulls each job off the channel. Exposed via
    /// [`Writer::queue_depth`] for the `cairn_writer_queue_depth` gauge (ARCH §26.2). This is the
    /// inbound backlog signal — a sustained nonzero depth means writes are arriving faster than the
    /// single writer can commit them.
    queue_depth: Arc<AtomicUsize>,
}

impl Writer {
    /// Spawn the writer on a dedicated OS thread owning `conn`. `linger` optionally waits a
    /// short window to enlarge batches under bursty load (group-commit linger).
    pub fn spawn(conn: Connection, linger: Option<Duration>) -> Writer {
        let (tx, rx) = mpsc::channel::<Job>(4096);
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let loop_depth = queue_depth.clone();
        std::thread::Builder::new()
            .name("cairn-meta-writer".to_owned())
            .spawn(move || writer_loop(conn, rx, linger, &loop_depth))
            .expect("spawn writer thread");
        Writer { tx, queue_depth }
    }

    /// The current inbound write-queue depth: mutations submitted but not yet pulled into a commit
    /// batch by the writer loop. Published as the `cairn_writer_queue_depth` gauge.
    #[must_use]
    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed)
    }

    /// Submit a mutation; the returned future resolves only after the batch containing it has
    /// been made durable.
    pub async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        // Count the job as queued before it is sent; the writer loop decrements as it drains.
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
        if self.tx.send(Job::Write((mutation, ack_tx))).await.is_err() {
            // The send failed (writer gone): the job will never be drained, so undo the increment.
            self.queue_depth.fetch_sub(1, Ordering::Relaxed);
            return Err(MetaError::WriterClosed);
        }
        ack_rx.await.map_err(|_| MetaError::WriterClosed)?
    }

    /// Probe that the writer thread is alive and draining its queue. Enqueues a control message and
    /// awaits its ack; resolving proves the writer is responsive (the readiness check uses this so
    /// `/readyz` does not report ready while the writer is wedged). Cheap: no database work.
    ///
    /// # Errors
    /// Returns [`MetaError::WriterClosed`] if the writer has shut down.
    pub async fn probe(&self) -> Result<(), MetaError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Job::Control(Control::Probe(reply_tx)))
            .await
            .map_err(|_| MetaError::WriterClosed)?;
        reply_rx.await.map_err(|_| MetaError::WriterClosed)
    }

    /// Run a truncating WAL checkpoint on the writer thread — the only thread that owns the
    /// write connection — so it is serialized with mutations rather than racing them
    /// (ARCH §8.4/§11.2). Resolves with the checkpoint's frame counts.
    pub async fn checkpoint(&self) -> Result<WalCheckpointStats, MetaError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Job::Control(Control::Checkpoint(reply_tx)))
            .await
            .map_err(|_| MetaError::WriterClosed)?;
        reply_rx.await.map_err(|_| MetaError::WriterClosed)?
    }
}

fn writer_loop(
    conn: Connection,
    mut rx: mpsc::Receiver<Job>,
    linger: Option<Duration>,
    queue_depth: &AtomicUsize,
) {
    loop {
        // Block for the first job; None means every handle dropped — shut down.
        let Some(first) = rx.blocking_recv() else {
            break;
        };
        let first = match first {
            // A control message that arrives alone is handled directly, with no write batch.
            Job::Control(ctl) => {
                run_control(&conn, ctl);
                continue;
            }
            // This write job is now drained off the inbound queue.
            Job::Write(req) => {
                queue_depth.fetch_sub(1, Ordering::Relaxed);
                req
            }
        };

        let mut batch: Vec<WriteRequest> = Vec::with_capacity(MAX_BATCH);
        batch.push(first);
        // Control messages drained while assembling the batch are deferred until after the
        // commit, preserving submission order and keeping each off the write transaction.
        let mut deferred: Vec<Control> = Vec::new();

        // Opportunistically drain everything already queued.
        drain_available(&mut rx, &mut batch, &mut deferred, queue_depth);

        // Optional linger to enlarge the batch under bursty load.
        if let Some(d) = linger {
            if batch.len() < MAX_BATCH {
                std::thread::sleep(d);
                drain_available(&mut rx, &mut batch, &mut deferred, queue_depth);
            }
        }

        commit_batch(&conn, batch);
        for ctl in deferred {
            run_control(&conn, ctl);
        }
    }
}

fn drain_available(
    rx: &mut mpsc::Receiver<Job>,
    batch: &mut Vec<WriteRequest>,
    deferred: &mut Vec<Control>,
    queue_depth: &AtomicUsize,
) {
    while batch.len() < MAX_BATCH {
        match rx.try_recv() {
            Ok(Job::Write(req)) => {
                queue_depth.fetch_sub(1, Ordering::Relaxed);
                batch.push(req);
            }
            Ok(Job::Control(ctl)) => deferred.push(ctl),
            Err(_) => break,
        }
    }
}

/// Execute a control message on the writer thread, outside any write transaction.
fn run_control(conn: &Connection, ctl: Control) {
    match ctl {
        Control::Checkpoint(reply) => {
            let _ = reply.send(run_checkpoint(conn));
        }
        Control::Probe(reply) => {
            let _ = reply.send(());
        }
    }
}

/// Run `PRAGMA wal_checkpoint(TRUNCATE)`, which returns one row of `(busy, log, checkpointed)`.
fn run_checkpoint(conn: &Connection) -> Result<WalCheckpointStats, MetaError> {
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
        Ok(WalCheckpointStats {
            busy: r.get::<_, i64>(0)? != 0,
            log_frames: r.get::<_, i64>(1)?.max(0) as u64,
            checkpointed_frames: r.get::<_, i64>(2)?.max(0) as u64,
        })
    })
    .map_err(|e| MetaError::Engine(e.to_string()))
}

/// Apply a batch in one transaction with a savepoint per mutation, commit once, then ack.
fn commit_batch(conn: &Connection, batch: Vec<WriteRequest>) {
    if let Err(e) = conn.execute_batch("BEGIN IMMEDIATE") {
        // Could not even begin; fail the whole batch.
        let msg = e.to_string();
        for (_, ack) in batch {
            let _ = ack.send(Err(MetaError::Engine(msg.clone())));
        }
        return;
    }

    let mut acks: Vec<(Ack, Result<MutationOutcome, MetaError>)> = Vec::with_capacity(batch.len());
    let mut iter = batch.into_iter().enumerate();
    // A savepoint RELEASE/ROLLBACK that itself fails (audit #17) leaves the transaction's
    // per-mutation isolation untrustworthy: a failed ROLLBACK-TO can leave a failed mutation's
    // partial writes live, and committing would persist them. When that happens we record the
    // error and stop processing, so the block below aborts the WHOLE transaction instead of
    // committing suspect state.
    let abort: Option<String> = loop {
        let Some((idx, (mutation, ack))) = iter.next() else {
            break None;
        };
        let sp = format!("sp{idx}");
        if conn.execute_batch(&format!("SAVEPOINT {sp}")).is_err() {
            acks.push((
                ack,
                Err(MetaError::Engine("failed to open savepoint".to_owned())),
            ));
            continue;
        }
        match apply(conn, mutation) {
            Ok(outcome) => {
                if let Err(e) = conn.execute_batch(&format!("RELEASE {sp}")) {
                    let msg = e.to_string();
                    acks.push((
                        ack,
                        Err(MetaError::Engine(format!(
                            "savepoint release failed: {msg}"
                        ))),
                    ));
                    break Some(msg);
                }
                acks.push((ack, Ok(outcome)));
            }
            Err(e) => {
                // Roll back only this mutation; the rest of the batch is unaffected — unless the
                // rollback itself fails, in which case the whole batch must abort.
                if let Err(re) = conn.execute_batch(&format!("ROLLBACK TO {sp}; RELEASE {sp}")) {
                    let msg = re.to_string();
                    acks.push((
                        ack,
                        Err(MetaError::Engine(format!(
                            "savepoint rollback failed: {msg}"
                        ))),
                    ));
                    break Some(msg);
                }
                acks.push((ack, Err(e)));
            }
        }
    };

    if let Some(msg) = abort {
        // Abort the entire transaction and fail every submitter — those already applied and those
        // not yet reached (still in `iter`) — rather than commit a transaction whose savepoint
        // bookkeeping is broken (#17).
        let _ = conn.execute_batch("ROLLBACK");
        for (ack, _) in acks {
            let _ = ack.send(Err(MetaError::Engine(format!("batch aborted: {msg}"))));
        }
        for (_, (_, ack)) in iter {
            let _ = ack.send(Err(MetaError::Engine(format!("batch aborted: {msg}"))));
        }
        return;
    }

    // One commit = one durability barrier covering every surviving mutation in the batch.
    match conn.execute_batch("COMMIT") {
        Ok(()) => {
            for (ack, result) in acks {
                let _ = ack.send(result);
            }
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            let msg = e.to_string();
            for (ack, _) in acks {
                let _ = ack.send(Err(MetaError::Engine(format!("commit failed: {msg}"))));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_reports_frames_and_busy_state() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ckpt.sqlite");

        // A WAL writer connection that accumulates frames without auto-truncating.
        let writer = Connection::open(&db).unwrap();
        writer
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        writer
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);")
            .unwrap();
        for i in 0..200 {
            writer
                .execute("INSERT INTO t (id, v) VALUES (?1, zeroblob(4096))", [i])
                .unwrap();
        }

        // A second connection pins the WAL with an open read snapshot, so the WAL still holds
        // its frames and a TRUNCATE checkpoint reports itself busy rather than fully truncating.
        let reader = Connection::open(&db).unwrap();
        reader
            .execute_batch("BEGIN; SELECT COUNT(*) FROM t;")
            .unwrap();

        let pinned = run_checkpoint(&writer).unwrap();
        assert!(pinned.log_frames > 0, "WAL should hold frames: {pinned:?}");
        assert!(
            pinned.busy,
            "an open reader blocks the truncate: {pinned:?}"
        );

        // Release the reader; now the truncate can complete cleanly and report its frames.
        reader.execute_batch("COMMIT").unwrap();
        let clean = run_checkpoint(&writer).unwrap();
        assert!(!clean.busy, "no reader: truncate is uncontended: {clean:?}");
        assert!(
            clean.checkpointed_frames <= clean.log_frames,
            "checkpointed cannot exceed total frames: {clean:?}"
        );
    }
}
