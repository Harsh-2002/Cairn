//! A single FULL-durability SQLite actor for the packing laboratory.
//!
//! Reads, descriptor acquisition and metadata retirement share the actor. File locks retain
//! readers after that serialized acquisition; no object or segment inventory lives in a heap map.

use super::model::{
    ArtifactIdentity, ArtifactKind, ArtifactPlan, CipherFormat, CleanupDebt, EncodedFormat,
    ExpectedCurrent, Location, MAX_RECORDS, PublishRecord, PublishedRecord, RecordMetadata,
    Rejection, Result,
};
use super::record::{self, CleanupReceipt, DurableArtifact, PinnedRecord, QuiescentArtifact};
use cairn_types::storage::StorageToken;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::collections::BTreeSet;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};

const SCHEMA_VERSION: i64 = 1;
const CHANNEL_CAPACITY: usize = 32;

/// Move-only evidence that the exact plan was durably reserved by this actor.
pub struct ArtifactAdmission {
    plan: ArtifactPlan,
    lifetime: Arc<dyn Send + Sync>,
    root_identity: (u64, u64),
}

impl std::fmt::Debug for ArtifactAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArtifactAdmission")
            .field("plan", &self.plan)
            .finish_non_exhaustive()
    }
}

impl ArtifactAdmission {
    pub fn plan(&self) -> &ArtifactPlan {
        &self.plan
    }

    pub fn lifetime(&self) -> Arc<dyn Send + Sync> {
        self.lifetime.clone()
    }

    pub fn root_identity(&self) -> (u64, u64) {
        self.root_identity
    }
}

/// Move-only exact ownership of one immutable cleanup alias.
pub struct CleanupClaim {
    debt: CleanupDebt,
    claim_token: StorageToken,
    generation: StorageToken,
    lifetime: Arc<dyn Send + Sync>,
    root_identity: (u64, u64),
}

impl std::fmt::Debug for CleanupClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CleanupClaim")
            .field("debt", &self.debt)
            .field("claim_token", &self.claim_token)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl CleanupClaim {
    pub fn id(&self) -> &StorageToken {
        &self.debt.id
    }

    pub fn artifact(&self) -> &ArtifactIdentity {
        &self.debt.artifact
    }

    pub fn kind(&self) -> ArtifactKind {
        self.debt.kind
    }

    pub fn path(&self) -> &Path {
        &self.debt.path
    }

    pub fn claim_token(&self) -> &StorageToken {
        &self.claim_token
    }

    pub fn generation(&self) -> &StorageToken {
        &self.generation
    }

    pub fn lifetime(&self) -> Arc<dyn Send + Sync> {
        self.lifetime.clone()
    }

    pub fn root_identity(&self) -> (u64, u64) {
        self.root_identity
    }
}

pub enum PublicationOutcome {
    Applied {
        records: usize,
    },
    Rejected {
        reason: Rejection,
        artifact: DurableArtifact,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteOutcome {
    Applied,
    Rejected(Rejection),
}

pub struct Pinned {
    pub record: PublishedRecord,
    pub pin: PinnedRecord,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreStats {
    pub records: u64,
    pub current: u64,
    pub history: u64,
    pub locked: u64,
    pub pending: u64,
    pub cleanup: u64,
}

type Reply<T> = oneshot::Sender<Result<T>>;

enum Command {
    Plan(ArtifactKind, u64, Reply<ArtifactAdmission>),
    Publish(
        DurableArtifact,
        Vec<PublishRecord>,
        Reply<PublicationOutcome>,
    ),
    Lookup(String, Reply<Option<PublishedRecord>>),
    Version(StorageToken, Reply<Option<PublishedRecord>>),
    Pin(String, bool, Reply<Option<Pinned>>),
    Delete(StorageToken, Location, Reply<DeleteOutcome>),
    Pending(Option<StorageToken>, usize, Reply<Vec<ArtifactPlan>>),
    Debt(Option<StorageToken>, usize, Reply<Vec<CleanupDebt>>),
    Abort(QuiescentArtifact, Reply<bool>),
    Recover(ArtifactPlan, Reply<bool>),
    Claim(usize, Reply<Vec<CleanupClaim>>),
    Finish(CleanupReceipt, Reply<bool>),
    Release(CleanupClaim, Reply<bool>),
    Stats(Reply<StoreStats>),
    Close(Reply<()>),
    #[cfg(test)]
    FailPublicationAfter(Option<usize>, Reply<()>),
}

struct Inner {
    sender: mpsc::Sender<Command>,
    thread: Mutex<Option<JoinHandle<()>>>,
    generation: StorageToken,
}

#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

impl Store {
    /// The caller exclusively owns this laboratory root. `lifetime` retains that node lock;
    /// admissions and pinned readers keep it alive until their actual filesystem work ends.
    pub fn open(root: &Path, lifetime: Arc<dyn Send + Sync>) -> Result<Self> {
        let root = root.to_owned();
        let generation = StorageToken::generate();
        let actor_generation = generation.clone();
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        let (ready, opened) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("packing-lab-writer".into())
            .spawn(
                move || match Actor::open(root, actor_generation, lifetime) {
                    Ok(mut actor) => {
                        if ready.send(Ok(())).is_ok() {
                            actor.run(receiver);
                        }
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error));
                    }
                },
            )?;
        if let Err(error) = opened.recv().map_err(|_| "packing actor failed to open")? {
            let _ = thread.join();
            return Err(error);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                sender,
                thread: Mutex::new(Some(thread)),
                generation,
            }),
        })
    }

    pub fn generation(&self) -> &StorageToken {
        &self.inner.generation
    }

    async fn request<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Result<T> {
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(command(reply))
            .await
            .map_err(|_| "packing actor is closed")?;
        response.await.map_err(|_| "packing actor dropped reply")?
    }

    pub async fn plan(&self, kind: ArtifactKind, max_length: u64) -> Result<ArtifactAdmission> {
        self.request(|reply| Command::Plan(kind, max_length, reply))
            .await
    }

    pub async fn publish(
        &self,
        artifact: DurableArtifact,
        records: Vec<PublishRecord>,
    ) -> Result<PublicationOutcome> {
        validate_limit(records.len())?;
        self.request(|reply| Command::Publish(artifact, records, reply))
            .await
    }

    /// Metadata-only lookup. Use `pin` when opening bytes, so retirement cannot race acquisition.
    pub async fn lookup(&self, key: &str) -> Result<Option<PublishedRecord>> {
        self.request(|reply| Command::Lookup(key.to_owned(), reply))
            .await
    }

    pub async fn get_version(&self, row_id: &StorageToken) -> Result<Option<PublishedRecord>> {
        self.request(|reply| Command::Version(row_id.clone(), reply))
            .await
    }

    pub async fn pin(&self, key: &str) -> Result<Option<Pinned>> {
        self.request(|reply| Command::Pin(key.to_owned(), false, reply))
            .await
    }

    pub async fn pin_version(&self, row_id: &StorageToken) -> Result<Option<Pinned>> {
        self.request(|reply| Command::Pin(row_id.as_str().to_owned(), true, reply))
            .await
    }

    pub async fn delete(
        &self,
        row_id: &StorageToken,
        location: &Location,
    ) -> Result<DeleteOutcome> {
        self.request(|reply| Command::Delete(row_id.clone(), location.clone(), reply))
            .await
    }

    pub async fn pending(
        &self,
        after: Option<StorageToken>,
        limit: usize,
    ) -> Result<Vec<ArtifactPlan>> {
        validate_limit(limit)?;
        self.request(|reply| Command::Pending(after, limit, reply))
            .await
    }

    pub async fn debt(
        &self,
        after: Option<StorageToken>,
        limit: usize,
    ) -> Result<Vec<CleanupDebt>> {
        validate_limit(limit)?;
        self.request(|reply| Command::Debt(after, limit, reply))
            .await
    }

    pub async fn abort(&self, artifact: QuiescentArtifact) -> Result<bool> {
        self.request(|reply| Command::Abort(artifact, reply)).await
    }

    /// Only prior-generation pending plans are eligible. Opening this actor required the
    /// exclusive root lock; a current-generation plan must instead present backend quiescence.
    pub async fn recover_pending(&self, plan: ArtifactPlan) -> Result<bool> {
        self.request(|reply| Command::Recover(plan, reply)).await
    }

    pub async fn claim_cleanup(&self, limit: usize) -> Result<Vec<CleanupClaim>> {
        validate_limit(limit)?;
        self.request(|reply| Command::Claim(limit, reply)).await
    }

    pub async fn finish_cleanup(&self, receipt: CleanupReceipt) -> Result<bool> {
        self.request(|reply| Command::Finish(receipt, reply)).await
    }

    pub async fn release_cleanup(&self, claim: CleanupClaim) -> Result<bool> {
        self.request(|reply| Command::Release(claim, reply)).await
    }

    pub async fn stats(&self) -> Result<StoreStats> {
        self.request(Command::Stats).await
    }

    pub async fn close(&self) -> Result<()> {
        self.request(Command::Close).await?;
        let thread = self
            .inner
            .thread
            .lock()
            .map_err(|_| "actor join lock poisoned")?
            .take();
        if let Some(thread) = thread {
            tokio::task::spawn_blocking(move || {
                thread.join().map_err(|_| "packing actor panicked")
            })
            .await??;
        }
        Ok(())
    }
}

struct Actor {
    connection: Connection,
    root: PathBuf,
    generation: StorageToken,
    lifetime: Arc<dyn Send + Sync>,
    root_identity: (u64, u64),
    #[cfg(test)]
    fail_publication_after: Option<usize>,
}

impl Actor {
    fn open(
        root: PathBuf,
        generation: StorageToken,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Result<Self> {
        let root_metadata = std::fs::symlink_metadata(&root)?;
        if !root_metadata.is_dir() {
            return Err("packing root is not a real directory".into());
        }
        let path = root.join("packing.sqlite3");
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if !metadata.is_file() || metadata.is_symlink() => {
                return Err("packing database is not a regular file".into());
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut connection = Connection::open(path)?;
        let tables: i64 = connection.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        if tables != 0 {
            let version: i64 = connection.query_row(
                "SELECT schema_version FROM packing_state WHERE singleton=1",
                [],
                |row| row.get(0),
            )?;
            if version != SCHEMA_VERSION {
                return Err("unsupported packing laboratory schema".into());
            }
        }
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
             PRAGMA wal_autocheckpoint=1000; PRAGMA cache_size=-8192; PRAGMA mmap_size=0;",
        )?;
        if tables == 0 {
            connection.execute_batch(SCHEMA)?;
        }
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO packing_state(singleton,schema_version,generation) VALUES(1,?1,?2)
             ON CONFLICT(singleton) DO UPDATE SET generation=excluded.generation",
            params![SCHEMA_VERSION, generation.as_str()],
        )?;
        transaction.execute(
            "UPDATE cleanup SET claim_token=NULL,claim_generation=NULL",
            [],
        )?;
        transaction.commit()?;
        Ok(Self {
            connection,
            root,
            generation,
            lifetime,
            root_identity: (root_metadata.dev(), root_metadata.ino()),
            #[cfg(test)]
            fail_publication_after: None,
        })
    }

    fn run(&mut self, mut receiver: mpsc::Receiver<Command>) {
        while let Some(command) = receiver.blocking_recv() {
            match command {
                Command::Plan(kind, length, reply) => {
                    let _ = reply.send(self.plan(kind, length));
                }
                Command::Publish(artifact, records, reply) => {
                    let _ = reply.send(self.publish(artifact, records));
                }
                Command::Lookup(key, reply) => {
                    let _ = reply.send(lookup(&self.connection, &key, false));
                }
                Command::Version(id, reply) => {
                    let _ = reply.send(lookup(&self.connection, id.as_str(), true));
                }
                Command::Pin(key, by_id, reply) => {
                    let _ = reply.send(self.pin(&key, by_id));
                }
                Command::Delete(id, location, reply) => {
                    let _ = reply.send(self.delete(&id, &location));
                }
                Command::Pending(after, limit, reply) => {
                    let _ = reply.send(pending(&self.connection, after.as_ref(), limit));
                }
                Command::Debt(after, limit, reply) => {
                    let _ = reply.send(debt(&self.connection, after.as_ref(), limit));
                }
                Command::Abort(artifact, reply) => {
                    let result = if artifact.plan().artifact().generation == self.generation {
                        retire_pending(&mut self.connection, artifact.plan())
                    } else {
                        Ok(false)
                    };
                    let _ = reply.send(result);
                }
                Command::Recover(plan, reply) => {
                    let result = if plan.artifact().generation != self.generation {
                        retire_pending(&mut self.connection, &plan)
                    } else {
                        Ok(false)
                    };
                    let _ = reply.send(result);
                }
                Command::Claim(limit, reply) => {
                    let _ = reply.send(self.claim_cleanup(limit));
                }
                Command::Finish(receipt, reply) => {
                    let claim = receipt.into_claim();
                    let _ = reply.send(self.settle_cleanup(&claim, true));
                }
                Command::Release(claim, reply) => {
                    let _ = reply.send(self.settle_cleanup(&claim, false));
                }
                Command::Stats(reply) => {
                    let _ = reply.send(stats(&self.connection));
                }
                Command::Close(reply) => {
                    let result = self
                        .connection
                        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                        .map_err(Into::into);
                    let _ = reply.send(result);
                    break;
                }
                #[cfg(test)]
                Command::FailPublicationAfter(count, reply) => {
                    self.fail_publication_after = count;
                    let _ = reply.send(Ok(()));
                }
            }
        }
    }

    fn plan(&mut self, kind: ArtifactKind, max_length: u64) -> Result<ArtifactAdmission> {
        let plan = ArtifactPlan::new(
            ArtifactIdentity {
                id: StorageToken::generate(),
                generation: self.generation.clone(),
            },
            kind,
            max_length,
        )?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO artifacts(id,generation,kind,max_length,state) VALUES(?1,?2,?3,?4,'pending')",
            params![plan.artifact().id.as_str(), plan.artifact().generation.as_str(), kind_name(kind), max_length as i64],
        )?;
        transaction.commit()?;
        Ok(ArtifactAdmission {
            plan,
            lifetime: self.lifetime.clone(),
            root_identity: self.root_identity,
        })
    }

    fn publish(
        &mut self,
        artifact: DurableArtifact,
        records: Vec<PublishRecord>,
    ) -> Result<PublicationOutcome> {
        validate_publication(&artifact, &records)?;
        let plan = artifact.plan();
        if plan.artifact().generation != self.generation {
            return Ok(PublicationOutcome::Rejected {
                reason: Rejection::Stale,
                artifact,
            });
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !is_pending(&transaction, plan)? {
            return Ok(PublicationOutcome::Rejected {
                reason: Rejection::Stale,
                artifact,
            });
        }
        for record in &records {
            let current = lookup(&transaction, &record.metadata.key, false)?;
            if !matches_expected(&record.expected, current.as_ref())
                || transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM row_ids WHERE id=?1)",
                    [record.metadata.row_id.as_str()],
                    |row| row.get::<_, bool>(0),
                )?
            {
                return Ok(PublicationOutcome::Rejected {
                    reason: Rejection::Conflict,
                    artifact,
                });
            }
            if !record.preserve_previous && current.as_ref().is_some_and(|old| old.metadata.locked)
            {
                return Ok(PublicationOutcome::Rejected {
                    reason: Rejection::Locked,
                    artifact,
                });
            }
        }
        transaction.execute(
            "UPDATE artifacts SET state='live',physical_length=?3,sha256=?4 WHERE id=?1 AND generation=?2",
            params![plan.artifact().id.as_str(), plan.artifact().generation.as_str(), artifact.physical_length() as i64, artifact.sha256().as_slice()],
        )?;
        for (index, record) in records.iter().enumerate() {
            if let Some(old) = lookup(&transaction, &record.metadata.key, false)? {
                if record.preserve_previous {
                    transaction.execute(
                        "UPDATE records SET is_current=0 WHERE row_id=?1",
                        [old.metadata.row_id.as_str()],
                    )?;
                } else {
                    transaction.execute(
                        "DELETE FROM records WHERE row_id=?1",
                        [old.metadata.row_id.as_str()],
                    )?;
                    retire_if_unreferenced(&transaction, old.location.artifact())?;
                }
            }
            insert_record(&transaction, record)?;
            #[cfg(test)]
            if self.fail_publication_after == Some(index + 1) {
                // A real SQLite constraint error after a row and retirement debt changed.
                // Dropping this transaction must undo every mutation in the batch.
                transaction.execute(
                    "INSERT INTO row_ids(id) VALUES(?1)",
                    [record.metadata.row_id.as_str()],
                )?;
            }
            #[cfg(not(test))]
            let _ = index;
        }
        enqueue_cleanup(&transaction, plan, plan.temporary_path())?;
        transaction.commit()?;
        Ok(PublicationOutcome::Applied {
            records: records.len(),
        })
    }

    fn pin(&self, key: &str, by_id: bool) -> Result<Option<Pinned>> {
        lookup(&self.connection, key, by_id)?
            .map(|record| {
                let pin = record::open_pinned(
                    &self.root,
                    &record.location,
                    record.metadata.encoded_sha256,
                    self.lifetime.clone(),
                )?;
                if pin.root_identity()? != self.root_identity {
                    return Err("packing root changed before reader acquisition".into());
                }
                Ok(Pinned { record, pin })
            })
            .transpose()
    }

    fn delete(&mut self, id: &StorageToken, location: &Location) -> Result<DeleteOutcome> {
        location.validate()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(record) = lookup(&transaction, id.as_str(), true)? else {
            return Ok(DeleteOutcome::Rejected(Rejection::Conflict));
        };
        if &record.location != location {
            return Ok(DeleteOutcome::Rejected(Rejection::Conflict));
        }
        if record.metadata.locked {
            return Ok(DeleteOutcome::Rejected(Rejection::Locked));
        }
        transaction.execute("DELETE FROM records WHERE row_id=?1", [id.as_str()])?;
        if record.is_current {
            transaction.execute(
                "UPDATE records SET is_current=1 WHERE row_id=(SELECT row_id FROM records WHERE key=?1 ORDER BY ordinal DESC LIMIT 1)",
                [&record.metadata.key],
            )?;
        }
        retire_if_unreferenced(&transaction, record.location.artifact())?;
        transaction.commit()?;
        Ok(DeleteOutcome::Applied)
    }

    fn claim_cleanup(&mut self, limit: usize) -> Result<Vec<CleanupClaim>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let candidates = {
            let mut statement = transaction.prepare(
                "SELECT c.id,c.artifact_id,c.artifact_generation,a.kind,c.path,0 FROM cleanup c
                 JOIN artifacts a ON a.id=c.artifact_id AND a.generation=c.artifact_generation
                 WHERE c.claim_token IS NULL AND (c.path LIKE '.pending-%' OR
                 (a.state='retired' AND NOT EXISTS(SELECT 1 FROM records r WHERE r.artifact_id=a.id AND r.artifact_generation=a.generation)))
                 ORDER BY c.id LIMIT ?1",
            )?;
            statement
                .query_map([limit as i64], decode_debt)?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut claims = Vec::with_capacity(candidates.len());
        for debt in candidates {
            let claim_token = StorageToken::generate();
            transaction.execute(
                "UPDATE cleanup SET claim_token=?2,claim_generation=?3 WHERE id=?1 AND claim_token IS NULL",
                params![debt.id.as_str(), claim_token.as_str(), self.generation.as_str()],
            )?;
            claims.push(CleanupClaim {
                debt,
                claim_token,
                generation: self.generation.clone(),
                lifetime: self.lifetime.clone(),
                root_identity: self.root_identity,
            });
        }
        transaction.commit()?;
        Ok(claims)
    }

    fn settle_cleanup(&mut self, claim: &CleanupClaim, finished: bool) -> Result<bool> {
        if claim.generation != self.generation {
            return Ok(false);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let sql = if finished {
            "DELETE FROM cleanup WHERE id=?1 AND artifact_id=?2 AND artifact_generation=?3 AND path=?4 AND claim_token=?5 AND claim_generation=?6"
        } else {
            "UPDATE cleanup SET claim_token=NULL,claim_generation=NULL WHERE id=?1 AND artifact_id=?2 AND artifact_generation=?3 AND path=?4 AND claim_token=?5 AND claim_generation=?6"
        };
        let changed = transaction.execute(
            sql,
            params![
                claim.id().as_str(),
                claim.artifact().id.as_str(),
                claim.artifact().generation.as_str(),
                path_text(claim.path())?,
                claim.claim_token().as_str(),
                claim.generation().as_str()
            ],
        )?;
        transaction.commit()?;
        Ok(changed == 1)
    }
}

fn validate_limit(limit: usize) -> Result<()> {
    if !(1..=MAX_RECORDS).contains(&limit) {
        return Err("page limit must be 1..=256".into());
    }
    Ok(())
}

fn validate_publication(artifact: &DurableArtifact, records: &[PublishRecord]) -> Result<()> {
    validate_limit(records.len())?;
    if artifact.spans().len() != records.len()
        || artifact.physical_length() > artifact.plan().max_length()
        || artifact.physical_length() > i64::MAX as u64
        || (artifact.plan().kind() == ArtifactKind::File && records.len() != 1)
    {
        return Err("publication does not match durable artifact geometry".into());
    }
    let mut keys = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for (record, span) in records.iter().zip(artifact.spans()) {
        record.metadata.validate()?;
        record.location.validate()?;
        if !keys.insert(&record.metadata.key)
            || !ids.insert(&record.metadata.row_id)
            || record.location.artifact() != artifact.plan().artifact()
            || record.location.kind() != artifact.plan().kind()
            || record.location.offset() != span.offset
            || record.location.length() != span.length
            || record.metadata.encoded_length != span.length
            || record.metadata.encoded_sha256 != span.sha256
            || span
                .offset
                .checked_add(span.length)
                .is_none_or(|end| end > artifact.physical_length())
        {
            return Err("record does not match exact durable span".into());
        }
    }
    Ok(())
}

fn matches_expected(expected: &ExpectedCurrent, current: Option<&PublishedRecord>) -> bool {
    match expected {
        ExpectedCurrent::Any => true,
        ExpectedCurrent::Absent => current.is_none(),
        ExpectedCurrent::Exact { row_id, location } => current.is_some_and(|record| {
            &record.metadata.row_id == row_id && &record.location == location
        }),
    }
}

fn is_pending(connection: &Connection, plan: &ArtifactPlan) -> Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM artifacts WHERE id=?1 AND generation=?2 AND kind=?3 AND max_length=?4 AND state='pending')",
        params![plan.artifact().id.as_str(), plan.artifact().generation.as_str(), kind_name(plan.kind()), plan.max_length() as i64],
        |row| row.get(0),
    )?)
}

fn retire_pending(connection: &mut Connection, plan: &ArtifactPlan) -> Result<bool> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !is_pending(&transaction, plan)? {
        return Ok(false);
    }
    transaction.execute(
        "UPDATE artifacts SET state='retired' WHERE id=?1 AND generation=?2",
        params![
            plan.artifact().id.as_str(),
            plan.artifact().generation.as_str()
        ],
    )?;
    enqueue_cleanup(&transaction, plan, plan.temporary_path())?;
    enqueue_cleanup(&transaction, plan, plan.final_path())?;
    transaction.commit()?;
    Ok(true)
}

fn retire_if_unreferenced(
    transaction: &Transaction<'_>,
    artifact: &ArtifactIdentity,
) -> Result<()> {
    let count: i64 = transaction.query_row(
        "SELECT count(*) FROM records WHERE artifact_id=?1 AND artifact_generation=?2",
        params![artifact.id.as_str(), artifact.generation.as_str()],
        |row| row.get(0),
    )?;
    if count != 0 {
        return Ok(());
    }
    let plan = artifact_plan(transaction, artifact)?.ok_or("missing referenced artifact")?;
    transaction.execute(
        "UPDATE artifacts SET state='retired' WHERE id=?1 AND generation=?2 AND state='live'",
        params![artifact.id.as_str(), artifact.generation.as_str()],
    )?;
    enqueue_cleanup(transaction, &plan, plan.final_path())?;
    Ok(())
}

fn enqueue_cleanup(transaction: &Transaction<'_>, plan: &ArtifactPlan, path: &Path) -> Result<()> {
    transaction.execute(
        "INSERT OR IGNORE INTO cleanup(id,artifact_id,artifact_generation,path) VALUES(?1,?2,?3,?4)",
        params![StorageToken::generate().as_str(), plan.artifact().id.as_str(), plan.artifact().generation.as_str(), path_text(path)?],
    )?;
    Ok(())
}

fn insert_record(transaction: &Transaction<'_>, record: &PublishRecord) -> Result<()> {
    transaction.execute(
        "INSERT INTO row_ids(id) VALUES(?1)",
        [record.metadata.row_id.as_str()],
    )?;
    transaction.execute(
        "INSERT INTO records(row_id,key,encoded_sha256,encoded_length,logical_size,format,locked,is_current,artifact_id,artifact_generation,location_kind,offset,length,compression,cipher)
         VALUES(?1,?2,?3,?4,?5,?6,?7,1,?8,?9,?10,?11,?12,?13,?14)",
        params![record.metadata.row_id.as_str(), record.metadata.key, record.metadata.encoded_sha256.as_slice(), record.metadata.encoded_length as i64, record.metadata.logical_size as i64,
            match record.metadata.format { EncodedFormat::Raw => "raw", EncodedFormat::Crnb => "crnb" }, record.metadata.locked,
            record.location.artifact().id.as_str(), record.location.artifact().generation.as_str(), kind_name(record.location.kind()), record.location.offset() as i64, record.location.length() as i64,
            serde_json::to_string(&record.metadata.compression)?, cipher_name(record.metadata.cipher)],
    )?;
    Ok(())
}

fn lookup(connection: &Connection, value: &str, by_id: bool) -> Result<Option<PublishedRecord>> {
    let suffix = if by_id {
        "row_id=?1"
    } else {
        "key=?1 AND is_current=1"
    };
    let sql = format!(
        "SELECT row_id,key,encoded_sha256,encoded_length,logical_size,format,locked,is_current,artifact_id,artifact_generation,location_kind,offset,length,compression,cipher FROM records WHERE {suffix}"
    );
    Ok(connection
        .query_row(&sql, [value], decode_record)
        .optional()?)
}

fn decode_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<PublishedRecord> {
    let hash: Vec<u8> = row.get(2)?;
    let format: String = row.get(5)?;
    let kind = decode_kind(row.get(10)?)?;
    let artifact = ArtifactIdentity {
        id: token(row.get(8)?)?,
        generation: token(row.get(9)?)?,
    };
    let offset = nonnegative(row.get(11)?)?;
    let length = nonnegative(row.get(12)?)?;
    let location = match kind {
        ArtifactKind::File if offset == 0 => Location::File { artifact, length },
        ArtifactKind::Segment => Location::Segment {
            artifact,
            offset,
            length,
        },
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    let metadata = RecordMetadata {
        row_id: token(row.get(0)?)?,
        key: row.get(1)?,
        encoded_sha256: hash.try_into().map_err(|_| rusqlite::Error::InvalidQuery)?,
        encoded_length: nonnegative(row.get(3)?)?,
        logical_size: nonnegative(row.get(4)?)?,
        format: match format.as_str() {
            "raw" => EncodedFormat::Raw,
            "crnb" => EncodedFormat::Crnb,
            _ => return Err(rusqlite::Error::InvalidQuery),
        },
        compression: serde_json::from_str(&row.get::<_, String>(13)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        cipher: match row.get::<_, String>(14)?.as_str() {
            "plaintext" => CipherFormat::Plaintext,
            "legacy-v2" => CipherFormat::LegacyV2,
            "authenticated-v3" => CipherFormat::AuthenticatedV3,
            _ => return Err(rusqlite::Error::InvalidQuery),
        },
        locked: strict_bool(row.get(6)?)?,
    };
    metadata
        .validate()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    location
        .validate()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    if metadata.encoded_length != location.length() {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(PublishedRecord {
        metadata,
        location,
        is_current: strict_bool(row.get(7)?)?,
    })
}

fn artifact_plan(
    connection: &Connection,
    artifact: &ArtifactIdentity,
) -> Result<Option<ArtifactPlan>> {
    Ok(connection
        .query_row(
            "SELECT id,generation,kind,max_length FROM artifacts WHERE id=?1 AND generation=?2",
            params![artifact.id.as_str(), artifact.generation.as_str()],
            decode_plan,
        )
        .optional()?)
}

fn decode_plan(row: &rusqlite::Row<'_>) -> rusqlite::Result<ArtifactPlan> {
    ArtifactPlan::new(
        ArtifactIdentity {
            id: token(row.get(0)?)?,
            generation: token(row.get(1)?)?,
        },
        decode_kind(row.get(2)?)?,
        nonnegative(row.get(3)?)?,
    )
    .map_err(|_| rusqlite::Error::InvalidQuery)
}

fn pending(
    connection: &Connection,
    after: Option<&StorageToken>,
    limit: usize,
) -> Result<Vec<ArtifactPlan>> {
    let mut statement = connection.prepare("SELECT id,generation,kind,max_length FROM artifacts WHERE state='pending' AND id>?1 ORDER BY id LIMIT ?2")?;
    Ok(statement
        .query_map(
            params![after.map_or("", StorageToken::as_str), limit as i64],
            decode_plan,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn debt(
    connection: &Connection,
    after: Option<&StorageToken>,
    limit: usize,
) -> Result<Vec<CleanupDebt>> {
    let mut statement = connection.prepare("SELECT c.id,c.artifact_id,c.artifact_generation,a.kind,c.path,c.claim_token IS NOT NULL FROM cleanup c JOIN artifacts a ON a.id=c.artifact_id AND a.generation=c.artifact_generation WHERE c.id>?1 ORDER BY c.id LIMIT ?2")?;
    Ok(statement
        .query_map(
            params![after.map_or("", StorageToken::as_str), limit as i64],
            decode_debt,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn decode_debt(row: &rusqlite::Row<'_>) -> rusqlite::Result<CleanupDebt> {
    let artifact = ArtifactIdentity {
        id: token(row.get(1)?)?,
        generation: token(row.get(2)?)?,
    };
    let kind = decode_kind(row.get(3)?)?;
    let path = PathBuf::from(row.get::<_, String>(4)?);
    let plan = ArtifactPlan::new(
        artifact.clone(),
        kind,
        if kind == ArtifactKind::Segment {
            super::model::MAX_SEGMENT_LENGTH
        } else {
            0
        },
    )
    .map_err(|_| rusqlite::Error::InvalidQuery)?;
    if path != plan.temporary_path() && path != plan.final_path() {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(CleanupDebt {
        id: token(row.get(0)?)?,
        artifact,
        kind,
        path,
        claimed: strict_bool(row.get(5)?)?,
    })
}

fn stats(connection: &Connection) -> Result<StoreStats> {
    let count = |sql| -> Result<u64> {
        Ok(nonnegative(
            connection.query_row(sql, [], |row| row.get(0))?,
        )?)
    };
    Ok(StoreStats {
        records: count("SELECT count(*) FROM records")?,
        current: count("SELECT count(*) FROM records WHERE is_current=1")?,
        history: count("SELECT count(*) FROM records WHERE is_current=0")?,
        locked: count("SELECT count(*) FROM records WHERE locked=1")?,
        pending: count("SELECT count(*) FROM artifacts WHERE state='pending'")?,
        cleanup: count("SELECT count(*) FROM cleanup")?,
    })
}

fn token(value: String) -> rusqlite::Result<StorageToken> {
    StorageToken::try_from(value).map_err(|_| rusqlite::Error::InvalidQuery)
}

fn nonnegative(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| rusqlite::Error::InvalidQuery)
}

fn strict_bool(value: i64) -> rusqlite::Result<bool> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn kind_name(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::File => "file",
        ArtifactKind::Segment => "segment",
    }
}

fn decode_kind(value: String) -> rusqlite::Result<ArtifactKind> {
    match value.as_str() {
        "file" => Ok(ArtifactKind::File),
        "segment" => Ok(ArtifactKind::Segment),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn cipher_name(cipher: CipherFormat) -> &'static str {
    match cipher {
        CipherFormat::Plaintext => "plaintext",
        CipherFormat::LegacyV2 => "legacy-v2",
        CipherFormat::AuthenticatedV3 => "authenticated-v3",
    }
}

fn path_text(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| "non-UTF8 planned artifact path".into())
}

const SCHEMA: &str = "
BEGIN IMMEDIATE;
CREATE TABLE packing_state(singleton INTEGER PRIMARY KEY CHECK(singleton=1),schema_version INTEGER NOT NULL,generation TEXT NOT NULL);
CREATE TABLE artifacts(id TEXT PRIMARY KEY,generation TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('file','segment')),max_length INTEGER NOT NULL CHECK(max_length>=0),state TEXT NOT NULL CHECK(state IN ('pending','live','retired')),physical_length INTEGER CHECK(physical_length>=0),sha256 BLOB CHECK(sha256 IS NULL OR length(sha256)=32),UNIQUE(id,generation));
CREATE INDEX artifacts_pending ON artifacts(state,id);
CREATE TABLE row_ids(id TEXT PRIMARY KEY);
CREATE TABLE records(ordinal INTEGER PRIMARY KEY AUTOINCREMENT,row_id TEXT NOT NULL UNIQUE REFERENCES row_ids(id),key TEXT NOT NULL,encoded_sha256 BLOB NOT NULL CHECK(length(encoded_sha256)=32),encoded_length INTEGER NOT NULL CHECK(encoded_length>=0),logical_size INTEGER NOT NULL CHECK(logical_size>=0),format TEXT NOT NULL CHECK(format IN ('raw','crnb')),compression TEXT NOT NULL,cipher TEXT NOT NULL CHECK(cipher IN ('plaintext','legacy-v2','authenticated-v3')),locked INTEGER NOT NULL CHECK(locked IN (0,1)),is_current INTEGER NOT NULL CHECK(is_current IN (0,1)),artifact_id TEXT NOT NULL,artifact_generation TEXT NOT NULL,location_kind TEXT NOT NULL CHECK(location_kind IN ('file','segment')),offset INTEGER NOT NULL CHECK(offset>=0),length INTEGER NOT NULL CHECK(length>=0),FOREIGN KEY(artifact_id,artifact_generation) REFERENCES artifacts(id,generation),CHECK(encoded_length=length),CHECK(location_kind!='file' OR offset=0));
CREATE UNIQUE INDEX records_current ON records(key) WHERE is_current=1;
CREATE INDEX records_history ON records(key,ordinal);
CREATE INDEX records_artifact ON records(artifact_id,artifact_generation);
CREATE TABLE cleanup(id TEXT PRIMARY KEY,artifact_id TEXT NOT NULL,artifact_generation TEXT NOT NULL,path TEXT NOT NULL,claim_token TEXT,claim_generation TEXT,UNIQUE(artifact_id,artifact_generation,path),FOREIGN KEY(artifact_id,artifact_generation) REFERENCES artifacts(id,generation),CHECK((claim_token IS NULL)=(claim_generation IS NULL)));
CREATE INDEX cleanup_claimable ON cleanup(claim_token,id);
COMMIT;
";

#[cfg(test)]
mod tests {
    use super::super::record::{CleanupResult, publish_file, publish_segment};
    use super::*;
    use cairn_types::CompressionDescriptor;
    use std::io::{Cursor, Read};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn open(root: &Path) -> Store {
        Store::open(root, Arc::new(())).unwrap()
    }

    fn records(artifact: &DurableArtifact, keys: &[&str]) -> Vec<PublishRecord> {
        artifact
            .spans()
            .iter()
            .zip(keys)
            .map(|(span, key)| {
                let identity = artifact.plan().artifact().clone();
                PublishRecord {
                    metadata: RecordMetadata {
                        row_id: StorageToken::generate(),
                        key: (*key).to_owned(),
                        encoded_sha256: span.sha256,
                        encoded_length: span.length,
                        logical_size: span.length,
                        format: EncodedFormat::Raw,
                        compression: CompressionDescriptor::Uncompressed,
                        cipher: CipherFormat::Plaintext,
                        locked: false,
                    },
                    location: match artifact.plan().kind() {
                        ArtifactKind::File => Location::File {
                            artifact: identity,
                            length: span.length,
                        },
                        ArtifactKind::Segment => Location::Segment {
                            artifact: identity,
                            offset: span.offset,
                            length: span.length,
                        },
                    },
                    expected: ExpectedCurrent::Absent,
                    preserve_previous: false,
                }
            })
            .collect()
    }

    async fn file(
        store: &Store,
        root: &Path,
        key: &str,
        data: &[u8],
    ) -> (DurableArtifact, Vec<PublishRecord>) {
        let admitted = store
            .plan(ArtifactKind::File, data.len() as u64)
            .await
            .unwrap();
        let artifact = publish_file(root, admitted, &mut Cursor::new(data)).unwrap();
        let updates = records(&artifact, &[key]);
        (artifact, updates)
    }

    async fn segment(
        store: &Store,
        root: &Path,
        keys: &[&str],
    ) -> (DurableArtifact, Vec<PublishRecord>) {
        let admitted = store
            .plan(
                ArtifactKind::Segment,
                super::super::model::MAX_SEGMENT_LENGTH,
            )
            .await
            .unwrap();
        let bytes = keys.iter().map(|key| key.as_bytes()).collect::<Vec<_>>();
        let artifact = publish_segment(root, admitted, &bytes).unwrap();
        let updates = records(&artifact, keys);
        (artifact, updates)
    }

    async fn apply(store: &Store, artifact: DurableArtifact, updates: Vec<PublishRecord>) {
        let expected = updates.len();
        assert!(
            matches!(store.publish(artifact, updates).await.unwrap(), PublicationOutcome::Applied { records } if records == expected)
        );
    }

    async fn reject(
        store: &Store,
        artifact: DurableArtifact,
        updates: Vec<PublishRecord>,
        expected: Rejection,
    ) -> DurableArtifact {
        match store.publish(artifact, updates).await.unwrap() {
            PublicationOutcome::Rejected { reason, artifact } => {
                assert_eq!(reason, expected);
                artifact
            }
            PublicationOutcome::Applied { .. } => panic!("rejected publication applied"),
        }
    }

    async fn drain(store: &Store, root: &Path) {
        for _ in 0..32 {
            let claims = store.claim_cleanup(MAX_RECORDS).await.unwrap();
            if claims.is_empty() {
                assert_eq!(store.stats().await.unwrap().cleanup, 0);
                return;
            }
            for claim in claims {
                match record::cleanup(root, claim).unwrap() {
                    CleanupResult::Removed(receipt) => {
                        assert!(store.finish_cleanup(receipt).await.unwrap())
                    }
                    CleanupResult::Pinned(_) => panic!("unexpected retained reader pin"),
                }
            }
        }
        panic!("bounded cleanup did not finish");
    }

    #[tokio::test]
    async fn admission_is_durable_before_creation_and_abort_owns_both_aliases() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let admitted = store.plan(ArtifactKind::File, 20).await.unwrap();
        let plan = admitted.plan().clone();
        assert_eq!(store.pending(None, 1).await.unwrap(), vec![plan.clone()]);
        assert!(!root.path().join(plan.temporary_path()).exists());
        assert!(!root.path().join(plan.final_path()).exists());
        assert!(
            !store.recover_pending(plan.clone()).await.unwrap(),
            "a current generation cannot manufacture quiescence"
        );
        assert!(store.abort(record::abort(admitted)).await.unwrap());
        let debts = store.debt(None, 2).await.unwrap();
        assert_eq!(debts.len(), 2);
        assert!(debts.iter().any(|debt| debt.path == plan.final_path()));
        assert!(debts.iter().any(|debt| debt.path == plan.temporary_path()));
        assert!(store.pending(None, 1).await.unwrap().is_empty());
        drain(&store, root.path()).await;
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn batch_conflict_checks_the_entire_prior_location_and_changes_no_rows() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let (artifact, updates) = file(&store, root.path(), "a", b"before").await;
        apply(&store, artifact, updates).await;
        let before = store.lookup("a").await.unwrap().unwrap();
        let (artifact, mut updates) = segment(&store, root.path(), &["b", "a"]).await;
        let mut stale = before.location.clone();
        match &mut stale {
            Location::File { artifact, .. } | Location::Segment { artifact, .. } => {
                artifact.generation = StorageToken::generate()
            }
        }
        updates[1].expected = ExpectedCurrent::Exact {
            row_id: before.metadata.row_id.clone(),
            location: stale,
        };
        let stats = store.stats().await.unwrap();
        let loser = reject(&store, artifact, updates, Rejection::Conflict).await;
        assert_eq!(store.stats().await.unwrap(), stats);
        assert_eq!(store.lookup("a").await.unwrap(), Some(before));
        assert!(store.lookup("b").await.unwrap().is_none());
        assert!(store.abort(loser.into_quiescent()).await.unwrap());
        drain(&store, root.path()).await;
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn locked_history_survives_overwrite_delete_and_restart() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let (artifact, mut updates) = file(&store, root.path(), "history", b"retained").await;
        updates[0].metadata.locked = true;
        apply(&store, artifact, updates).await;
        let first = store.lookup("history").await.unwrap().unwrap();
        let (artifact, mut updates) = file(&store, root.path(), "history", b"replacement").await;
        updates[0].expected = ExpectedCurrent::Any;
        let loser = reject(&store, artifact, updates, Rejection::Locked).await;
        assert!(store.abort(loser.into_quiescent()).await.unwrap());
        let (artifact, mut updates) = file(&store, root.path(), "history", b"replacement").await;
        updates[0].expected = ExpectedCurrent::Exact {
            row_id: first.metadata.row_id.clone(),
            location: first.location.clone(),
        };
        updates[0].preserve_previous = true;
        apply(&store, artifact, updates).await;
        let second = store.lookup("history").await.unwrap().unwrap();
        let mut historical = first.clone();
        historical.is_current = false;
        assert_eq!(
            store.get_version(&first.metadata.row_id).await.unwrap(),
            Some(historical.clone())
        );
        assert_eq!(
            store
                .delete(&first.metadata.row_id, &first.location)
                .await
                .unwrap(),
            DeleteOutcome::Rejected(Rejection::Locked)
        );
        assert_eq!(store.stats().await.unwrap().history, 1);
        drain(&store, root.path()).await;
        assert!(root.path().join(first.location.path()).exists());
        store.close().await.unwrap();
        let reopened = open(root.path());
        assert_eq!(
            reopened.get_version(&first.metadata.row_id).await.unwrap(),
            Some(historical)
        );
        assert_eq!(
            reopened
                .delete(&second.metadata.row_id, &second.location)
                .await
                .unwrap(),
            DeleteOutcome::Applied
        );
        assert_eq!(reopened.lookup("history").await.unwrap(), Some(first));
        drain(&reopened, root.path()).await;
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn encrypted_crnb_history_keeps_exact_interpretation_after_reopen() {
        let key = super::super::model::encryption_test_key();
        use cairn_types::testing::{FixtureBlobStore, fixture_storage_io};
        use cairn_types::{
            BlobCipher, BucketName, CompressionAlgorithm, CompressionPolicy, StageOptions,
        };

        let source = tempfile::tempdir().unwrap();
        let blobs = cairn_blob::LocalBlobStore::open(source.path(), fixture_storage_io())
            .await
            .unwrap();
        let plaintext = vec![b'x'; 65_537];
        let body = bytes::Bytes::from(plaintext.clone());
        let staged = blobs
            .stage_fixture(
                &BucketName::parse("metadata-history").unwrap(),
                Box::pin(futures_util::stream::once(async move { Ok(body) })),
                StageOptions {
                    compression: Some(CompressionPolicy {
                        algorithm: CompressionAlgorithm::Zstd,
                        block_size: 64 * 1024,
                    }),
                    encryption: Some(key.clone()),
                    size_ceiling: plaintext.len() as u64,
                    content_type: "text/plain".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let encoded = std::fs::read(source.path().join(staged.storage_path.as_str())).unwrap();
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let (artifact, mut updates) =
            file(&store, root.path(), "encrypted-history", &encoded).await;
        updates[0].metadata.format = EncodedFormat::Crnb;
        updates[0].metadata.compression = staged.compression;
        updates[0].metadata.cipher = CipherFormat::AuthenticatedV3;
        updates[0].metadata.logical_size = plaintext.len() as u64;
        updates[0].metadata.locked = true;
        apply(&store, artifact, updates).await;
        let first = store.lookup("encrypted-history").await.unwrap().unwrap();
        let (artifact, mut updates) =
            file(&store, root.path(), "encrypted-history", b"new head").await;
        updates[0].expected = ExpectedCurrent::Exact {
            row_id: first.metadata.row_id.clone(),
            location: first.location.clone(),
        };
        updates[0].preserve_previous = true;
        apply(&store, artifact, updates).await;
        store.close().await.unwrap();
        let store = open(root.path());
        let pinned = store
            .pin_version(&first.metadata.row_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!pinned.record.is_current);
        assert_eq!(pinned.record.metadata, first.metadata);
        assert_eq!(pinned.record.location, first.location);
        let mut reader = cairn_blob::compress::CompressedReader::open_with_dek(
            pinned.pin,
            BlobCipher::AuthenticatedV3(key.clone()),
            &pinned.record.metadata.compression,
            pinned.record.metadata.logical_size,
        )
        .unwrap();
        assert_eq!(
            reader.read_range(0, plaintext.len() as u64).unwrap(),
            plaintext
        );
        drop(reader);
        drain(&store, root.path()).await;
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_sql_error_rolls_back_the_entire_batch_including_debt_and_row_ids() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let (artifact, updates) = file(&store, root.path(), "a", b"old").await;
        apply(&store, artifact, updates).await;
        let before = store.lookup("a").await.unwrap().unwrap();
        let (artifact, mut updates) = segment(&store, root.path(), &["a", "b"]).await;
        updates[0].expected = ExpectedCurrent::Any;
        let attempt = artifact.plan().clone();
        let new_row_id = updates[0].metadata.row_id.clone();
        let before_stats = store.stats().await.unwrap();
        store
            .request(|reply| Command::FailPublicationAfter(Some(1), reply))
            .await
            .unwrap();
        assert!(store.publish(artifact, updates).await.is_err());
        store
            .request(|reply| Command::FailPublicationAfter(None, reply))
            .await
            .unwrap();
        assert_eq!(store.stats().await.unwrap(), before_stats);
        assert_eq!(store.lookup("a").await.unwrap(), Some(before));
        assert!(store.lookup("b").await.unwrap().is_none());
        assert!(store.get_version(&new_row_id).await.unwrap().is_none());
        assert_eq!(store.pending(None, 1).await.unwrap(), vec![attempt.clone()]);
        store.close().await.unwrap();
        let reopened = open(root.path());
        assert!(reopened.recover_pending(attempt.clone()).await.unwrap());
        assert!(!reopened.recover_pending(attempt).await.unwrap());
        drain(&reopened, root.path()).await;
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn stale_receipt_and_duplicate_row_identity_cannot_publish() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let (artifact, updates) = file(&store, root.path(), "a", b"stale").await;
        let stale_plan = artifact.plan().clone();
        store.close().await.unwrap();
        let store = open(root.path());
        let artifact = reject(&store, artifact, updates, Rejection::Stale).await;
        assert!(!store.abort(artifact.into_quiescent()).await.unwrap());
        assert!(store.recover_pending(stale_plan).await.unwrap());
        let (artifact, updates) = file(&store, root.path(), "a", b"one").await;
        apply(&store, artifact, updates).await;
        let first = store.lookup("a").await.unwrap().unwrap();
        assert_eq!(
            store
                .delete(&first.metadata.row_id, &first.location)
                .await
                .unwrap(),
            DeleteOutcome::Applied
        );
        let (artifact, mut updates) = file(&store, root.path(), "a", b"two").await;
        updates[0].metadata.row_id = first.metadata.row_id;
        let artifact = reject(&store, artifact, updates, Rejection::Conflict).await;
        assert!(store.lookup("a").await.unwrap().is_none());
        assert!(store.abort(artifact.into_quiescent()).await.unwrap());
        drain(&store, root.path()).await;
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_pinned_reader_blocks_physical_retirement_but_not_metadata_progress() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let (artifact, updates) = file(&store, root.path(), "a", b"reader survivor").await;
        apply(&store, artifact, updates).await;
        drain(&store, root.path()).await;
        let mut pinned = store.pin("a").await.unwrap().unwrap();
        assert_eq!(
            store
                .delete(&pinned.record.metadata.row_id, &pinned.record.location)
                .await
                .unwrap(),
            DeleteOutcome::Applied
        );
        assert!(store.pin("a").await.unwrap().is_none());
        let claim = store.claim_cleanup(1).await.unwrap().pop().unwrap();
        match record::cleanup(root.path(), claim).unwrap() {
            CleanupResult::Pinned(claim) => assert!(store.release_cleanup(claim).await.unwrap()),
            CleanupResult::Removed(_) => panic!("cleanup reclaimed a pinned reader"),
        }
        let mut bytes = Vec::new();
        pinned.pin.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"reader survivor");
        drop(pinned);
        drain(&store, root.path()).await;
        store.close().await.unwrap();
    }

    fn duplicate_claim(claim: &CleanupClaim) -> CleanupClaim {
        // Tests manufacture a retransmitted wire receipt; public capabilities remain move-only.
        CleanupClaim {
            debt: claim.debt.clone(),
            claim_token: claim.claim_token.clone(),
            generation: claim.generation.clone(),
            lifetime: claim.lifetime.clone(),
            root_identity: claim.root_identity,
        }
    }

    #[tokio::test]
    async fn stale_and_duplicate_cleanup_claims_cannot_settle_new_ownership() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let admission = store.plan(ArtifactKind::File, 0).await.unwrap();
        assert!(store.abort(record::abort(admission)).await.unwrap());
        let old = store.claim_cleanup(1).await.unwrap().pop().unwrap();
        let old_copy = duplicate_claim(&old);
        store.close().await.unwrap();
        let store = open(root.path());
        assert!(!store.release_cleanup(old).await.unwrap());
        let new = store.claim_cleanup(2).await.unwrap();
        assert!(
            new.iter().any(|claim| claim.id() == old_copy.id()
                && claim.claim_token() != old_copy.claim_token())
        );
        let CleanupResult::Removed(stale_receipt) = record::cleanup(root.path(), old_copy).unwrap()
        else {
            panic!("missing aliases cannot be pinned");
        };
        assert!(!store.finish_cleanup(stale_receipt).await.unwrap());
        for claim in new {
            let duplicate = duplicate_claim(&claim);
            let CleanupResult::Removed(receipt) = record::cleanup(root.path(), claim).unwrap()
            else {
                panic!("unexpected pin");
            };
            assert!(store.finish_cleanup(receipt).await.unwrap());
            let CleanupResult::Removed(receipt) = record::cleanup(root.path(), duplicate).unwrap()
            else {
                panic!("unexpected pin");
            };
            assert!(!store.finish_cleanup(receipt).await.unwrap());
        }
        assert_eq!(store.stats().await.unwrap().cleanup, 0);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_admission_reply_leaves_recoverable_pending_ownership() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let (reply, receiver) = oneshot::channel();
        drop(receiver);
        store
            .inner
            .sender
            .send(Command::Plan(ArtifactKind::File, 0, reply))
            .await
            .unwrap();
        assert_eq!(store.stats().await.unwrap().pending, 1);
        let plan = store.pending(None, 1).await.unwrap().pop().unwrap();
        assert!(!store.recover_pending(plan.clone()).await.unwrap());
        store.close().await.unwrap();
        let store = open(root.path());
        assert!(store.recover_pending(plan).await.unwrap());
        drain(&store, root.path()).await;
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_publication_reply_preserves_committed_records() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let (artifact, updates) = file(&store, root.path(), "reply-lost", b"committed").await;
        let expected = updates[0].clone();
        let (reply, receiver) = oneshot::channel();
        drop(receiver);
        store
            .inner
            .sender
            .send(Command::Publish(artifact, updates, reply))
            .await
            .unwrap();
        let stats = store.stats().await.unwrap();
        assert_eq!(stats.records, 1);
        assert_eq!(stats.pending, 0);
        store.close().await.unwrap();
        let store = open(root.path());
        let current = store.lookup("reply-lost").await.unwrap().unwrap();
        assert_eq!(current.metadata, expected.metadata);
        assert_eq!(current.location, expected.location);
        drain(&store, root.path()).await;
        assert!(root.path().join(current.location.path()).exists());
        store.close().await.unwrap();
    }

    struct Lifetime(Arc<AtomicBool>);
    impl Drop for Lifetime {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn node_lifetime_survives_actor_shutdown_until_pinned_reader_finishes() {
        let root = tempfile::tempdir().unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let store = Store::open(root.path(), Arc::new(Lifetime(dropped.clone()))).unwrap();
        let (artifact, updates) = file(&store, root.path(), "a", b"owned").await;
        apply(&store, artifact, updates).await;
        let pinned = store.pin("a").await.unwrap().unwrap();
        store.close().await.unwrap();
        assert!(!dropped.load(Ordering::SeqCst));
        drop(pinned);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn node_lifetime_survives_a_returned_admission_after_actor_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let store = Store::open(root.path(), Arc::new(Lifetime(dropped.clone()))).unwrap();
        let admission = store.plan(ArtifactKind::File, 0).await.unwrap();
        store.close().await.unwrap();
        assert!(!dropped.load(Ordering::SeqCst));
        drop(admission);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn pending_pages_and_publication_batches_have_hard_bounds() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        assert!(store.pending(None, 0).await.is_err());
        assert!(store.pending(None, MAX_RECORDS + 1).await.is_err());
        assert!(store.claim_cleanup(MAX_RECORDS + 1).await.is_err());
        assert!(
            store
                .plan(
                    ArtifactKind::Segment,
                    super::super::model::MAX_SEGMENT_LENGTH + 1
                )
                .await
                .is_err()
        );
        let admissions = [
            store.plan(ArtifactKind::File, 0).await.unwrap(),
            store.plan(ArtifactKind::File, 0).await.unwrap(),
        ];
        let first = store.pending(None, 1).await.unwrap();
        let next = store
            .pending(Some(first[0].artifact().id.clone()), 1)
            .await
            .unwrap();
        assert_eq!(next.len(), 1);
        assert_ne!(first[0], next[0]);
        for admission in admissions {
            assert!(store.abort(record::abort(admission)).await.unwrap());
        }
        drain(&store, root.path()).await;
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn unsupported_lab_schema_is_rejected_before_changing_generation() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let generation = store.generation().clone();
        store.close().await.unwrap();
        let connection = Connection::open(root.path().join("packing.sqlite3")).unwrap();
        connection
            .execute("UPDATE packing_state SET schema_version=2", [])
            .unwrap();
        assert!(Store::open(root.path(), Arc::new(())).is_err());
        let found: String = connection
            .query_row("SELECT generation FROM packing_state", [], |row| row.get(0))
            .unwrap();
        assert_eq!(found, generation.as_str());
    }
}
