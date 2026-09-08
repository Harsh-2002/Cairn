//! Bounded group commit with durable replies and owned shutdown.
use super::{
    apply,
    kv::{self, Native, Overlay, View},
};
use cairn_types::{MetaError, Mutation, MutationOutcome};
use fjall::AbstractTree;
use fjall::config::CompressionPolicy;
use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase, SingleWriterTxKeyspace};
use serde::Serialize;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Instant,
};
use tokio::sync::{RwLock, Semaphore, mpsc, oneshot};
#[cfg(test)]
pub(super) static CRASH_POINT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
#[cfg(test)]
fn crash_at(point: u8) {
    if CRASH_POINT.load(std::sync::atomic::Ordering::SeqCst) == point {
        rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::KILL)
            .expect("kill owned test child");
        unreachable!("SIGKILL returned");
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct EngineState {
    pub cache_capacity_bytes: u64,
    pub cache_resident_bytes: u64,
    pub write_buffer_bytes: u64,
    pub sealed_memtables: usize,
    pub journal_bytes: u64,
    pub journal_count: usize,
    pub live_tree_bytes: u64,
    pub live_tables: usize,
    pub level_zero_tables: usize,
    pub outstanding_flushes: usize,
    pub active_compactions: usize,
    pub completed_compactions: usize,
    pub compaction_seconds: f64,
    pub physical_row_table_bytes: u64,
    pub non_current_row_table_bytes: u64,
    pub physical_row_table_files: usize,
    pub disappearing_table_samples: u64,
    pub block_cache_loads: usize,
    pub block_cache_hits: usize,
    pub block_io_requested_bytes: u64,
}

#[derive(Clone, Default, Serialize)]
pub struct Stages {
    pub admission_ns: u64,
    pub queue_ns: u64,
    pub begin_ns: u64,
    pub apply_ns: u64,
    pub commit_ns: u64,
    pub mutations: u64,
    pub batches: u64,
    pub rejected: u64,
    pub writer_tid: u32,
}
fn ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}
struct Engine {
    db: SingleWriterTxDatabase,
    tree: SingleWriterTxKeyspace,
}
struct Write {
    mutation: Mutation,
    queued: Instant,
    reply: oneshot::Sender<Result<MutationOutcome, MetaError>>,
}
#[allow(clippy::large_enum_variant)]
enum Job {
    Write(Write),
    Checkpoint(oneshot::Sender<Result<EngineState, MetaError>>),
    Close,
}
pub struct Store {
    engine: Mutex<Option<Arc<Engine>>>,
    sender: mpsc::Sender<Job>,
    admission: RwLock<bool>,
    readers: Arc<Semaphore>,
    thread: Mutex<Option<JoinHandle<Result<(), MetaError>>>>,
    stages: Arc<Mutex<Stages>>,
}
impl Store {
    pub fn open(path: &Path) -> Result<Arc<Self>, MetaError> {
        Self::open_mode(path, false)
    }
    pub fn open_existing(path: &Path) -> Result<Arc<Self>, MetaError> {
        if !std::fs::symlink_metadata(path.join("version"))
            .map_err(kv::error)?
            .is_file()
        {
            return Err(kv::error(
                "candidate existing version marker is not a regular file",
            ));
        }
        Self::open_mode(path, true)
    }
    fn open_mode(path: &Path, existing: bool) -> Result<Arc<Self>, MetaError> {
        let db = SingleWriterTxDatabase::builder(path)
            .cache_size(72 * 1024 * 1024)
            .worker_threads(2)
            .max_cached_files(Some(64))
            .max_journaling_size(64 * 1024 * 1024)
            .open()
            .map_err(kv::error)?;
        if existing && !db.inner().keyspace_exists("rows") {
            return Err(kv::error("candidate row keyspace is absent on resume"));
        }
        let tree = db
            .keyspace("rows", || {
                KeyspaceCreateOptions::default()
                    .max_memtable_size(32 * 1024 * 1024)
                    .data_block_compression_policy(CompressionPolicy::disabled())
                    .index_block_compression_policy(CompressionPolicy::disabled())
            })
            .map_err(kv::error)?;
        let engine = Arc::new(Engine { db, tree });
        let (sender, receiver) = mpsc::channel(4096);
        let stages = Arc::new(Mutex::new(Stages::default()));
        let owned = engine.clone();
        let statistics = stages.clone();
        let thread = std::thread::Builder::new()
            .name("fjall-lab-writer".into())
            .spawn(move || run(owned, receiver, statistics))
            .map_err(kv::error)?;
        Ok(Arc::new(Self {
            engine: Mutex::new(Some(engine)),
            sender,
            admission: RwLock::new(true),
            readers: Arc::new(Semaphore::new(8)),
            thread: Mutex::new(Some(thread)),
            stages,
        }))
    }
    pub fn stages(&self) -> Stages {
        self.stages.lock().expect("stage mutex").clone()
    }
    pub async fn verify_generation(
        &self,
        generation: &cairn_types::storage::StorageToken,
    ) -> Result<(), MetaError> {
        let generation = generation.clone();
        self.read(move |view| {
            let stored: Option<cairn_types::storage::StorageToken> =
                kv::get(view, &kv::key(super::model::META, &["generation"]))?;
            if stored.as_ref() != Some(&generation) {
                return Err(kv::error(
                    "candidate generation differs from fixture identity",
                ));
            }
            Ok(())
        })
        .await
    }
    pub fn engine_state(&self) -> Result<EngineState, MetaError> {
        let engine = self.engine.lock().expect("engine mutex");
        state(
            engine
                .as_ref()
                .ok_or_else(|| kv::error("candidate closed"))?,
        )
    }
    pub async fn checkpoint(&self) -> Result<EngineState, MetaError> {
        let admission = self.admission.read().await;
        if !*admission {
            return Err(kv::error("candidate closed"));
        }
        let (reply, answer) = oneshot::channel();
        self.sender
            .send(Job::Checkpoint(reply))
            .await
            .map_err(kv::error)?;
        drop(admission);
        answer.await.map_err(kv::error)?
    }
    pub async fn verify_auxiliary(
        &self,
        expected: &crate::metadata_workload::ExpectedState,
    ) -> Result<(), MetaError> {
        let expected = expected.clone();
        self.read(move |view| {
            use super::model;
            let mut totals = [0_u64; 6];
            for (_, data) in view.scan(&[model::STATS], None, kv::PAGE)? {
                let stats: model::Stats = kv::decode(&data)?;
                for (sum, value) in totals.iter_mut().zip([
                    stats.objects,
                    stats.versions,
                    stats.logical,
                    stats.physical,
                    stats.active,
                    stats.staged,
                ]) {
                    *sum += value;
                }
            }
            if totals
                != [
                    expected.current_data_rows,
                    expected.seed_rows,
                    expected.seed_logical_bytes,
                    expected.seed_logical_bytes,
                    expected.auxiliary_sessions,
                    expected.auxiliary_part_bytes,
                ]
            {
                return Err(kv::error(
                    "candidate bucket quotas disagree with independent seed",
                ));
            }
            let mut totals = [0_u64; 3];
            for (_, data) in view.scan(&[model::PRINCIPAL], None, kv::PAGE)? {
                let principal: model::Principal = kv::decode(&data)?;
                for (sum, value) in
                    totals
                        .iter_mut()
                        .zip([principal.logical, principal.active, principal.staged])
                {
                    *sum += value;
                }
            }
            if totals
                != [
                    expected.seed_logical_bytes,
                    expected.auxiliary_sessions,
                    expected.auxiliary_part_bytes,
                ]
            {
                return Err(kv::error(
                    "candidate principal quotas disagree with independent seed",
                ));
            }
            Ok(())
        })
        .await
    }
    pub async fn write(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
        let start = Instant::now();
        let admission = self.admission.read().await;
        if !*admission {
            return Err(kv::error("candidate closed"));
        }
        let permit = self.sender.reserve().await.map_err(kv::error)?;
        let (reply, answer) = oneshot::channel();
        self.stages.lock().expect("stage mutex").admission_ns += ns(start);
        permit.send(Job::Write(Write {
            mutation,
            queued: Instant::now(),
            reply,
        }));
        drop(admission);
        answer.await.map_err(kv::error)?
    }
    pub async fn read<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&dyn View) -> Result<T, MetaError> + Send + 'static,
    ) -> Result<T, MetaError> {
        let admission = self.admission.read().await;
        if !*admission {
            return Err(kv::error("candidate closed"));
        }
        let permit = self
            .readers
            .clone()
            .acquire_owned()
            .await
            .map_err(kv::error)?;
        let engine = self
            .engine
            .lock()
            .expect("engine mutex")
            .as_ref()
            .ok_or_else(|| kv::error("candidate closed"))?
            .clone();
        let job = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let snapshot = engine.db.read_tx();
            operation(&Native {
                transaction: &snapshot,
                tree: &engine.tree,
            })
        });
        drop(admission);
        job.await.map_err(kv::error)?
    }
    /// The owned task survives cancellation of the caller and joins all physical I/O workers.
    pub async fn close(self: Arc<Self>) -> Result<(), MetaError> {
        tokio::spawn(async move {
            let mut admission = self.admission.write().await;
            if !*admission {
                return Err(kv::error("candidate already closing"));
            }
            *admission = false;
            let channel_closed = self.sender.send(Job::Close).await.is_err();
            let readers = self
                .readers
                .clone()
                .acquire_many_owned(8)
                .await
                .map_err(kv::error)?;
            let engine = self.engine.lock().expect("engine mutex").take();
            let thread = self
                .thread
                .lock()
                .expect("thread mutex")
                .take()
                .ok_or_else(|| kv::error("candidate writer missing"))?;
            drop(admission);
            tokio::task::spawn_blocking(move || {
                let result = thread
                    .join()
                    .map_err(|_| kv::error("candidate writer panicked"))?;
                drop(engine); // Last Fjall database handle joins its background workers.
                drop(readers);
                if channel_closed && result.is_ok() {
                    return Err(kv::error("candidate writer closed unexpectedly"));
                }
                result
            })
            .await
            .map_err(kv::error)?
        })
        .await
        .map_err(kv::error)?
    }
}
fn run(
    engine: Arc<Engine>,
    mut receiver: mpsc::Receiver<Job>,
    statistics: Arc<Mutex<Stages>>,
) -> Result<(), MetaError> {
    statistics.lock().expect("stage mutex").writer_tid = std::fs::read_link("/proc/thread-self")
        .map_err(kv::error)?
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| kv::error("writer TID unavailable"))?
        .parse()
        .map_err(kv::error)?;
    while let Some(first) = receiver.blocking_recv() {
        let first = match first {
            Job::Write(first) => first,
            Job::Close => break,
            Job::Checkpoint(reply) => {
                let _ = reply.send(checkpoint(&engine));
                continue;
            }
        };
        let mut jobs = Vec::with_capacity(256);
        jobs.push(first);
        let mut closing = false;
        let mut checkpoints = Vec::new();
        while jobs.len() < 256 {
            match receiver.try_recv() {
                Ok(Job::Write(job)) => jobs.push(job),
                Ok(Job::Close) => {
                    closing = true;
                    break;
                }
                Ok(Job::Checkpoint(reply)) => {
                    checkpoints.push(reply);
                    break;
                }
                Err(_) => break,
            }
        }
        let queue_ns = jobs.iter().map(|job| ns(job.queued)).sum::<u64>();
        let begin = Instant::now();
        let mut transaction = engine.db.write_tx().durability(Some(PersistMode::SyncAll));
        let begin_ns = ns(begin);
        let apply_start = Instant::now();
        let native = Native {
            transaction: &transaction,
            tree: &engine.tree,
        };
        let mut batch = Overlay::new(&native);
        let mut answers = Vec::with_capacity(jobs.len());
        for job in jobs {
            let mut member = Overlay::new(&batch);
            let outcome = apply::apply(&mut member, job.mutation);
            let edits = member.into_edits();
            let outcome = outcome.and_then(|outcome| {
                batch.apply_member(edits)?;
                Ok(outcome)
            });
            answers.push((job.reply, outcome));
        }
        for (key, value) in batch.into_edits() {
            if let Some(value) = value {
                transaction.insert(&engine.tree, key, value);
            } else {
                transaction.remove(&engine.tree, key);
            }
        }
        let apply_ns = ns(apply_start);
        let commit = Instant::now();
        #[cfg(test)]
        crash_at(1);
        transaction.commit().map_err(kv::error)?; // Failure closes admission by dropping the receiver.
        #[cfg(test)]
        crash_at(2);
        let commit_ns = ns(commit);
        {
            let mut stages = statistics.lock().expect("stage mutex");
            stages.queue_ns += queue_ns;
            stages.begin_ns += begin_ns;
            stages.apply_ns += apply_ns;
            stages.commit_ns += commit_ns;
            stages.mutations += answers.len() as u64;
            stages.batches += 1;
            stages.rejected += answers.iter().filter(|(_, result)| result.is_err()).count() as u64;
        }
        for (reply, outcome) in answers {
            let _ = reply.send(outcome);
        }
        for reply in checkpoints {
            let _ = reply.send(checkpoint(&engine));
        }
        if closing {
            break;
        }
    }
    engine.db.persist(PersistMode::SyncAll).map_err(kv::error)
}
fn state(engine: &Engine) -> Result<EngineState, MetaError> {
    let db = engine.db.inner();
    let tree = engine.tree.inner();
    // Hold one manifest version while classifying current versus obsolete/in-progress tables.
    // This short observation can temporarily retain a table; its cost belongs to this arm.
    let version = tree.tree.current_version();
    if version.table_count() > 4096 {
        return Err(kv::error("candidate table observation bound exceeded"));
    }
    let current: std::collections::HashSet<_> = version
        .iter_tables()
        .map(|table| table.path.as_ref().clone())
        .collect();
    let (
        mut physical_row_table_bytes,
        mut non_current_row_table_bytes,
        mut physical_row_table_files,
        mut disappearing_table_samples,
    ) = (0, 0, 0, 0);
    for entry in std::fs::read_dir(engine.tree.path().join("tables")).map_err(kv::error)? {
        let entry = entry.map_err(kv::error)?;
        let metadata = match entry.metadata() {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                disappearing_table_samples += 1;
                continue;
            }
            Err(error) => return Err(kv::error(error)),
        };
        if !metadata.is_file() {
            return Err(kv::error("unexpected candidate table entry"));
        }
        physical_row_table_files += 1;
        if physical_row_table_files > 4096 {
            return Err(kv::error(
                "candidate physical table observation bound exceeded",
            ));
        }
        physical_row_table_bytes += metadata.len();
        if !current.contains(&entry.path()) {
            non_current_row_table_bytes += metadata.len();
        }
    }
    let metrics = tree.tree.metrics();
    Ok(EngineState {
        cache_capacity_bytes: db.cache_capacity(),
        cache_resident_bytes: db.cache_size(),
        write_buffer_bytes: db.write_buffer_size(),
        sealed_memtables: tree.sealed_memtable_count(),
        journal_bytes: db.journal_disk_space().map_err(kv::error)?,
        journal_count: db.journal_count(),
        live_tree_bytes: tree.disk_space(),
        live_tables: tree.table_count(),
        level_zero_tables: tree.l0_table_count(),
        outstanding_flushes: db.outstanding_flushes(),
        active_compactions: db.active_compactions(),
        completed_compactions: db.compactions_completed(),
        compaction_seconds: db.time_compacting().as_secs_f64(),
        physical_row_table_bytes,
        non_current_row_table_bytes,
        physical_row_table_files,
        disappearing_table_samples,
        block_cache_loads: metrics.block_loads(),
        block_cache_hits: metrics.block_load_cached_count(),
        block_io_requested_bytes: metrics.block_io(),
    })
}
fn checkpoint(engine: &Engine) -> Result<EngineState, MetaError> {
    engine
        .tree
        .inner()
        .rotate_memtable_and_wait()
        .map_err(kv::error)?;
    engine.db.persist(PersistMode::SyncAll).map_err(kv::error)?;
    state(engine)
}
