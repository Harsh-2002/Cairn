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
            let _ = ack.send(Err(e.clone()));
        }
        return;
    }

    let mut acks: Vec<(Ack, Result<MutationOutcome, MetaError>)> = Vec::with_capacity(batch.len());
    let mut iter = batch.into_iter().enumerate();
    let abort = loop {
        let Some((idx, (mutation, ack))) = iter.next() else {
            break None;
        };
        let sp = format!("sp{idx}");
        if let Err(error) = driver.savepoint(&sp).await {
            acks.push((ack, Err(error.clone())));
            break Some(error);
        }
        match apply(driver, mutation).await {
            Ok(outcome) => {
                if let Err(error) = driver.release(&sp).await {
                    acks.push((ack, Err(error.clone())));
                    break Some(error);
                }
                acks.push((ack, Ok(outcome)));
            }
            Err(e) => {
                // A failed savepoint rollback makes isolation untrustworthy. Abort every member,
                // as the SQLite writer does, instead of committing any partial mutation.
                if let Err(rollback) = driver.rollback_to(&sp).await {
                    let error = rollback_failure(e, rollback);
                    acks.push((ack, Err(error.clone())));
                    break Some(error);
                }
                acks.push((ack, Err(e)));
            }
        }
    };
    if let Some(error) = abort {
        let error = match driver.rollback().await {
            Ok(()) => error,
            Err(rollback) => rollback_failure(error, rollback),
        };
        for (ack, _) in acks {
            let _ = ack.send(Err(error.clone()));
        }
        for (_, (_, ack)) in iter {
            let _ = ack.send(Err(error.clone()));
        }
        return;
    }

    // One commit = one durability barrier covering every surviving mutation in the batch.
    match driver.commit().await {
        Ok(()) => {
            for (ack, result) in acks {
                let _ = ack.send(result);
            }
        }
        Err(e) => {
            let error = match driver.rollback().await {
                Ok(()) => e,
                Err(rollback) => rollback_failure(e, rollback),
            };
            for (ack, _) in acks {
                let _ = ack.send(Err(error.clone()));
            }
        }
    }
}

/// Capacity is a failure classification, never proof of nonpublication. A later rollback error
/// must not erase the typed cause of a statement or commit failure.
fn rollback_failure(error: MetaError, rollback: MetaError) -> MetaError {
    if matches!(error, MetaError::OutOfSpace) || matches!(rollback, MetaError::OutOfSpace) {
        MetaError::OutOfSpace
    } else {
        MetaError::Engine(format!("{error}; rollback failed: {rollback}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{Row, Value};
    use async_trait::async_trait;
    use cairn_types::bucket::{ConfigAspect, ConfigDoc};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn config(aspect: ConfigAspect, doc: String) -> Mutation {
        Mutation::SetBucketConfig {
            bucket: cairn_types::BucketName::parse("capacity").unwrap(),
            aspect,
            doc: Some(ConfigDoc(doc)),
        }
    }

    async fn setup(driver: &dyn AsyncSqlDriver) {
        driver
            .execute_batch(
                "CREATE TABLE bucket_config (bucket_name TEXT, aspect TEXT, doc TEXT,
            PRIMARY KEY(bucket_name,aspect));
            INSERT INTO bucket_config VALUES ('capacity','policy','original');",
            )
            .await
            .unwrap();
    }

    async fn submit_batch(
        driver: &dyn AsyncSqlDriver,
        mutations: Vec<Mutation>,
    ) -> Vec<Result<MutationOutcome, MetaError>> {
        let mut batch = Vec::new();
        let mut replies = Vec::new();
        for mutation in mutations {
            let (ack, reply) = oneshot::channel();
            batch.push((mutation, ack));
            replies.push(reply);
        }
        commit_batch(driver, batch).await;
        let mut results = Vec::new();
        for reply in replies {
            results.push(reply.await.unwrap());
        }
        results
    }

    async fn engine_capacity_contract(driver: &dyn AsyncSqlDriver) {
        setup(driver).await;
        let pages = driver.query("PRAGMA page_count", vec![]).await.unwrap()[0].get_i64(0);
        driver
            .query(&format!("PRAGMA max_page_count={pages}"), vec![])
            .await
            .unwrap();
        let results = submit_batch(
            driver,
            vec![
                config(ConfigAspect::Policy, "first".into()),
                config(ConfigAspect::Cors, "x".repeat(256 * 1024)),
                config(ConfigAspect::Lifecycle, "last".into()),
            ],
        )
        .await;
        assert!(
            matches!(&results[1], Err(MetaError::OutOfSpace)),
            "{:?}",
            results[1]
        );
        let rows = driver
            .query("SELECT aspect,doc FROM bucket_config", vec![])
            .await
            .unwrap();
        assert!(!rows.iter().any(|row| row.get_text(0) == "cors"));
        // An engine may undo either the statement or the complete transaction on SQLITE_FULL.
        // Every acknowledgement must agree with the committed state in either case.
        for (index, aspect, before, after) in [
            (0, "policy", Some("original"), "first"),
            (2, "lifecycle", None, "last"),
        ] {
            let observed = rows
                .iter()
                .find(|row| row.get_text(0) == aspect)
                .map(|row| row.get_text(1));
            let expected = match &results[index] {
                Ok(MutationOutcome::Ack) => Some(after),
                Err(MetaError::OutOfSpace) => before,
                result => panic!("unexpected sibling result: {result:?}"),
            };
            assert_eq!(observed.as_deref(), expected);
        }
        driver
            .query("PRAGMA max_page_count=1024", vec![])
            .await
            .unwrap();
        let results = submit_batch(
            driver,
            vec![config(ConfigAspect::Policy, "recovered".into())],
        )
        .await;
        assert!(results[0].is_ok(), "{:?}", results[0]);
        assert_eq!(
            driver
                .query(
                    "SELECT doc FROM bucket_config WHERE aspect='policy'",
                    vec![]
                )
                .await
                .unwrap()[0]
                .get_text(0),
            "recovered"
        );
    }

    #[tokio::test]
    async fn libsql_full_preserves_capacity_and_transaction_state() {
        let db = libsql::Builder::new_local(":memory:")
            .build()
            .await
            .unwrap();
        engine_capacity_contract(&crate::libsql_driver::LibsqlDriver::new(
            db.connect().unwrap(),
        ))
        .await;
    }

    #[tokio::test]
    async fn turso_full_preserves_capacity_and_transaction_state() {
        let db = turso::Builder::new_local(":memory:").build().await.unwrap();
        engine_capacity_contract(&crate::turso_driver::TursoDriver::new(
            db.connect().unwrap(),
        ))
        .await;
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum FailurePoint {
        Begin,
        Savepoint,
        Release,
        CapacityThenRollback,
        RollbackCapacity,
        EngineThenRollback,
        Commit,
        CommitAcknowledgement,
    }

    struct FaultDriver {
        inner: crate::libsql_driver::LibsqlDriver,
        point: FailurePoint,
        statements: AtomicUsize,
    }

    #[async_trait]
    impl AsyncSqlDriver for FaultDriver {
        async fn execute(&self, sql: &str, params: Vec<Value>) -> Result<u64, MetaError> {
            let result = self.inner.execute(sql, params).await?;
            if self.statements.fetch_add(1, Ordering::Relaxed) == 1 {
                match self.point {
                    FailurePoint::CapacityThenRollback => return Err(MetaError::OutOfSpace),
                    FailurePoint::RollbackCapacity | FailurePoint::EngineThenRollback => {
                        return Err(MetaError::Engine(
                            "injected after the statement wrote".into(),
                        ));
                    }
                    _ => {}
                }
            }
            Ok(result)
        }

        async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Row>, MetaError> {
            self.inner.query(sql, params).await
        }

        async fn execute_batch(&self, sql: &str) -> Result<(), MetaError> {
            match (self.point, sql) {
                (FailurePoint::Begin, "BEGIN IMMEDIATE")
                | (FailurePoint::Savepoint, "SAVEPOINT sp1")
                | (FailurePoint::Release, "RELEASE sp1")
                | (FailurePoint::Commit, "COMMIT")
                | (FailurePoint::RollbackCapacity, "ROLLBACK TO sp1; RELEASE sp1") => {
                    return Err(MetaError::OutOfSpace);
                }
                (
                    FailurePoint::CapacityThenRollback | FailurePoint::EngineThenRollback,
                    "ROLLBACK TO sp1; RELEASE sp1",
                ) => {
                    return Err(MetaError::Engine(
                        "injected savepoint rollback failure".into(),
                    ));
                }
                (FailurePoint::CommitAcknowledgement, "COMMIT") => {
                    self.inner.execute_batch(sql).await?;
                    return Err(MetaError::OutOfSpace);
                }
                _ => {}
            }
            self.inner.execute_batch(sql).await
        }

        async fn scrub_legacy_share_storage(&self) -> Result<(), MetaError> {
            self.inner.scrub_legacy_share_storage().await
        }
    }

    #[tokio::test]
    async fn capacity_transaction_failures_preserve_batch_isolation_and_commit_ambiguity() {
        for point in [
            FailurePoint::Begin,
            FailurePoint::Savepoint,
            FailurePoint::Release,
            FailurePoint::CapacityThenRollback,
            FailurePoint::RollbackCapacity,
            FailurePoint::EngineThenRollback,
            FailurePoint::Commit,
            FailurePoint::CommitAcknowledgement,
        ] {
            let db = libsql::Builder::new_local(":memory:")
                .build()
                .await
                .unwrap();
            let driver = FaultDriver {
                inner: crate::libsql_driver::LibsqlDriver::new(db.connect().unwrap()),
                point,
                statements: AtomicUsize::new(0),
            };
            setup(&driver.inner).await;
            let results = submit_batch(
                &driver,
                vec![
                    config(ConfigAspect::Policy, "first".into()),
                    config(ConfigAspect::Cors, "second".into()),
                    config(ConfigAspect::Lifecycle, "last".into()),
                ],
            )
            .await;
            for result in results {
                match point {
                    FailurePoint::EngineThenRollback => assert!(
                        matches!(result, Err(MetaError::Engine(_))),
                        "{point:?}: {result:?}"
                    ),
                    _ => assert!(
                        matches!(result, Err(MetaError::OutOfSpace)),
                        "{point:?}: {result:?}"
                    ),
                }
            }
            let rows = driver
                .query(
                    "SELECT aspect,doc FROM bucket_config ORDER BY aspect",
                    vec![],
                )
                .await
                .unwrap();
            if point == FailurePoint::CommitAcknowledgement {
                // A failed capacity acknowledgement still does not establish nonpublication.
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[2].get_text(1), "first");
            } else {
                assert_eq!(rows.len(), 1, "partial mutation committed at {point:?}");
                assert_eq!(rows[0].get_text(1), "original", "{point:?}");
            }
        }
    }
}
