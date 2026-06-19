//! The async single, serialized, group-committing writer (ARCH 7.2), the async analogue of
//! `cairn-meta/src/writer.rs`. All mutations are submitted to one writer task that owns the only
//! write driver. It drains its queue, applies every waiting mutation in one transaction — each
//! wrapped in its own savepoint so a logical failure rolls back only itself — commits once with a
//! single durability barrier, and only then acknowledges every caller whose mutation was in that
//! batch. The semantics match the rusqlite writer exactly; only the runtime differs (a tokio task
//! awaiting the async driver, rather than a dedicated OS thread doing blocking SQLite calls).

use crate::apply::apply;
use crate::driver::AsyncSqlDriver;
use cairn_types::MetaError;
use cairn_types::meta::{Mutation, MutationOutcome};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

type Ack = oneshot::Sender<Result<MutationOutcome, MetaError>>;
type WriteRequest = (Mutation, Ack);

const MAX_BATCH: usize = 256;

/// A handle to the async writer task. Cloneable; the writer shuts down when the last handle drops.
#[derive(Clone)]
pub struct Writer {
    tx: mpsc::Sender<WriteRequest>,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer").finish_non_exhaustive()
    }
}

impl Writer {
    /// Spawn the writer as a tokio task owning `driver`. `linger` optionally waits a short window
    /// to enlarge batches under bursty load (group-commit linger).
    pub fn spawn(driver: Arc<dyn AsyncSqlDriver>, linger: Option<Duration>) -> Writer {
        let (tx, rx) = mpsc::channel::<WriteRequest>(4096);
        tokio::spawn(writer_loop(driver, rx, linger));
        Writer { tx }
    }

    /// Submit a mutation; the returned future resolves only after the batch containing it has
    /// been made durable.
    pub async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send((mutation, ack_tx))
            .await
            .map_err(|_| MetaError::WriterClosed)?;
        ack_rx.await.map_err(|_| MetaError::WriterClosed)?
    }
}

async fn writer_loop(
    driver: Arc<dyn AsyncSqlDriver>,
    mut rx: mpsc::Receiver<WriteRequest>,
    linger: Option<Duration>,
) {
    loop {
        // Block for the first request; None means every handle dropped — shut down.
        let Some(first) = rx.recv().await else {
            break;
        };

        let mut batch: Vec<WriteRequest> = Vec::with_capacity(MAX_BATCH);
        batch.push(first);

        // Opportunistically drain everything already queued.
        drain_available(&mut rx, &mut batch);

        // Optional linger to enlarge the batch under bursty load.
        if let Some(d) = linger {
            if batch.len() < MAX_BATCH {
                tokio::time::sleep(d).await;
                drain_available(&mut rx, &mut batch);
            }
        }

        commit_batch(driver.as_ref(), batch).await;
    }
}

fn drain_available(rx: &mut mpsc::Receiver<WriteRequest>, batch: &mut Vec<WriteRequest>) {
    while batch.len() < MAX_BATCH {
        match rx.try_recv() {
            Ok(req) => batch.push(req),
            Err(_) => break,
        }
    }
}

/// Apply a batch in one transaction with a savepoint per mutation, commit once, then ack.
async fn commit_batch(driver: &dyn AsyncSqlDriver, batch: Vec<WriteRequest>) {
    if let Err(e) = driver.begin_immediate().await {
        // Could not even begin; fail the whole batch.
        for (_, ack) in batch {
            let _ = ack.send(Err(clone_err(&e)));
        }
        return;
    }

    let mut acks: Vec<(Ack, Result<MutationOutcome, MetaError>)> = Vec::with_capacity(batch.len());
    for (idx, (mutation, ack)) in batch.into_iter().enumerate() {
        let sp = format!("sp{idx}");
        if driver.savepoint(&sp).await.is_err() {
            acks.push((
                ack,
                Err(MetaError::Engine("failed to open savepoint".to_owned())),
            ));
            continue;
        }
        match apply(driver, mutation).await {
            Ok(outcome) => {
                let _ = driver.release(&sp).await;
                acks.push((ack, Ok(outcome)));
            }
            Err(e) => {
                // Roll back only this mutation; the rest of the batch is unaffected.
                let _ = driver.rollback_to(&sp).await;
                acks.push((ack, Err(e)));
            }
        }
    }

    // One commit = one durability barrier covering every surviving mutation in the batch.
    match driver.commit().await {
        Ok(()) => {
            for (ack, result) in acks {
                let _ = ack.send(result);
            }
        }
        Err(e) => {
            let _ = driver.rollback().await;
            let msg = e.to_string();
            for (ack, _) in acks {
                let _ = ack.send(Err(MetaError::Engine(format!("commit failed: {msg}"))));
            }
        }
    }
}

/// Clone a `MetaError` for fanning a single begin/commit failure out to every batch member.
fn clone_err(e: &MetaError) -> MetaError {
    MetaError::Engine(e.to_string())
}
