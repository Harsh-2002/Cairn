//! The single, serialized, group-committing writer (ARCH §7.2). All mutations are submitted
//! to one writer task that owns the only write connection. It drains its queue, applies every
//! waiting mutation in one transaction — each wrapped in its own savepoint so a logical
//! failure rolls back only itself — commits once with a single durability barrier, and only
//! then acknowledges every caller whose mutation was in that batch.

use crate::apply::apply;
use cairn_types::MetaError;
use cairn_types::meta::{Mutation, MutationOutcome};
use rusqlite::Connection;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

type Ack = oneshot::Sender<Result<MutationOutcome, MetaError>>;
type WriteRequest = (Mutation, Ack);

const MAX_BATCH: usize = 256;

/// A handle to the writer task. Cloneable; the writer shuts down when the last handle drops.
#[derive(Clone, Debug)]
pub struct Writer {
    tx: mpsc::Sender<WriteRequest>,
}

impl Writer {
    /// Spawn the writer on a dedicated OS thread owning `conn`. `linger` optionally waits a
    /// short window to enlarge batches under bursty load (group-commit linger).
    pub fn spawn(conn: Connection, linger: Option<Duration>) -> Writer {
        let (tx, rx) = mpsc::channel::<WriteRequest>(4096);
        std::thread::Builder::new()
            .name("cairn-meta-writer".to_owned())
            .spawn(move || writer_loop(conn, rx, linger))
            .expect("spawn writer thread");
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

fn writer_loop(conn: Connection, mut rx: mpsc::Receiver<WriteRequest>, linger: Option<Duration>) {
    loop {
        // Block for the first request; None means every handle dropped — shut down.
        let Some(first) = rx.blocking_recv() else {
            break;
        };
        let mut batch: Vec<WriteRequest> = Vec::with_capacity(MAX_BATCH);
        batch.push(first);

        // Opportunistically drain everything already queued.
        drain_available(&mut rx, &mut batch);

        // Optional linger to enlarge the batch under bursty load.
        if let Some(d) = linger {
            if batch.len() < MAX_BATCH {
                std::thread::sleep(d);
                drain_available(&mut rx, &mut batch);
            }
        }

        commit_batch(&conn, batch);
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
    for (idx, (mutation, ack)) in batch.into_iter().enumerate() {
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
                let _ = conn.execute_batch(&format!("RELEASE {sp}"));
                acks.push((ack, Ok(outcome)));
            }
            Err(e) => {
                // Roll back only this mutation; the rest of the batch is unaffected.
                let _ = conn.execute_batch(&format!("ROLLBACK TO {sp}; RELEASE {sp}"));
                acks.push((ack, Err(e)));
            }
        }
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
