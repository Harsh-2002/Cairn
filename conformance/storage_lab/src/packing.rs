//! Isolated completed-byte publication diagnostic; no production routing or adoption decision.
#[path = "packing/gc.rs"]
pub mod gc;
#[path = "packing/measurement.rs"]
mod measurement;
#[path = "packing/model.rs"]
pub mod model;
#[path = "packing/node.rs"]
pub mod node;
#[path = "packing/record.rs"]
pub mod record;
#[path = "packing/snapshot.rs"]
pub mod snapshot;
#[path = "packing/store.rs"]
pub mod store;

use cairn_types::CompressionDescriptor;
use cairn_types::storage::StorageToken;
use model::{
    ArtifactKind, CipherFormat, EncodedFormat, ExpectedCurrent, Location, PublishRecord,
    RecordMetadata, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use store::{PublicationOutcome, Store};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::{Instant, timeout_at};

const ADMISSION_BYTES: usize = 32 * 1024 * 1024;
const SCRATCH_BYTES: usize = 64 * 1024;
// The file writer and its final readback may have overlapping 64-KiB stack frames.
const IO_RESERVATION: usize = 2 * SCRATCH_BYTES;
const LINGER: Duration = Duration::from_millis(1);

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Files,
    Packed,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    root: PathBuf,
    mode: Mode,
    size: usize,
    objects: usize,
    concurrency: usize,
    seed: u64,
    known_length: bool,
    #[serde(default)]
    measurement: bool,
    deadline_seconds: u64,
}

impl Config {
    fn validate(&self) -> Result<()> {
        if !(1..=4 * 1024 * 1024).contains(&self.size)
            || !(1..=16_384).contains(&self.objects)
            || ![1, 4, 32, 128].contains(&self.concurrency)
            || !(1..=120).contains(&self.deadline_seconds)
            || !self.root.is_absolute()
            || (self.measurement
                && (!(4..=8192).contains(&self.objects) || !self.objects.is_multiple_of(4)))
        {
            return Err("invalid bounded packing configuration".into());
        }
        Ok(())
    }

    fn packed(&self) -> bool {
        self.mode == Mode::Packed
            && self.known_length
            && self.size as u64 <= record::MAX_PACKED_RECORD_LENGTH
    }
}

#[derive(Debug)]
struct Deadline;
impl std::fmt::Display for Deadline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("packing diagnostic deadline reached")
    }
}
impl std::error::Error for Deadline {}

fn check_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(Deadline.into())
    } else {
        Ok(())
    }
}

#[derive(Default)]
struct Counters {
    bytes: AtomicUsize,
    pending: AtomicUsize,
    peak_bytes: AtomicUsize,
    peak_pending: AtomicUsize,
    artifacts: AtomicUsize,
    packed: AtomicUsize,
    dedicated: AtomicUsize,
}

pub(crate) struct Budget {
    bytes: Arc<Semaphore>,
    count: Arc<Semaphore>,
    counters: Arc<Counters>,
    lifetime: Arc<dyn Send + Sync>,
}

pub(crate) struct Admission {
    _bytes: OwnedSemaphorePermit,
    _count: OwnedSemaphorePermit,
    charged: usize,
    counters: Arc<Counters>,
    _lifetime: Arc<dyn Send + Sync>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.counters
            .bytes
            .fetch_sub(self.charged, Ordering::SeqCst);
        self.counters.pending.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Budget {
    pub(crate) fn new(lifetime: Arc<dyn Send + Sync>) -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(ADMISSION_BYTES)),
            count: Arc::new(Semaphore::new(model::MAX_RECORDS)),
            counters: Arc::new(Counters::default()),
            lifetime,
        }
    }

    pub(crate) async fn acquire(&self, payload: usize, deadline: Instant) -> Result<Admission> {
        let charged = payload
            .checked_add(IO_RESERVATION)
            .filter(|n| *n <= ADMISSION_BYTES)
            .ok_or("request exceeds admission budget")?;
        let count = timeout_at(deadline, self.count.clone().acquire_owned())
            .await
            .map_err(|_| Deadline)??;
        let bytes = timeout_at(
            deadline,
            self.bytes.clone().acquire_many_owned(charged as u32),
        )
        .await
        .map_err(|_| Deadline)??;
        check_deadline(deadline)?;
        let current = self.counters.bytes.fetch_add(charged, Ordering::SeqCst) + charged;
        self.counters
            .peak_bytes
            .fetch_max(current, Ordering::SeqCst);
        let pending = self.counters.pending.fetch_add(1, Ordering::SeqCst) + 1;
        self.counters
            .peak_pending
            .fetch_max(pending, Ordering::SeqCst);
        Ok(Admission {
            _bytes: bytes,
            _count: count,
            charged,
            counters: self.counters.clone(),
            _lifetime: self.lifetime.clone(),
        })
    }
}

/// No request allocates fixture bytes until it holds both admission permits.
struct Request {
    index: usize,
    replacement: bool,
    payload: Option<Vec<u8>>,
    admission: Admission,
    reply: oneshot::Sender<std::result::Result<(), String>>,
}

fn acknowledge(request: Request, outcome: std::result::Result<(), String>) {
    let Request {
        payload,
        admission,
        reply,
        ..
    } = request;
    drop(payload);
    drop(admission);
    // A worker cannot admit its next object while this object's bytes remain resident.
    let _ = reply.send(outcome);
}

#[derive(Default)]
struct Builder {
    requests: Vec<Request>,
    physical: u64,
    first: Option<Instant>,
}

impl Builder {
    fn accepts(&self, length: usize) -> bool {
        self.requests.len() < model::MAX_RECORDS
            && self.physical.max(model::SEGMENT_HEADER_LENGTH)
                + model::RECORD_HEADER_LENGTH
                + length as u64
                <= model::MAX_SEGMENT_LENGTH
    }

    fn push(&mut self, request: Request, now: Instant) {
        let length = request.payload.as_ref().expect("packed request").len();
        assert!(self.accepts(length));
        if self.requests.is_empty() {
            self.first = Some(now);
            self.physical = model::SEGMENT_HEADER_LENGTH;
        }
        self.physical += model::RECORD_HEADER_LENGTH + length as u64;
        self.requests.push(request);
    }

    fn seal_at(&self) -> Option<Instant> {
        self.first.map(|first| first + LINGER)
    }
    fn full(&self) -> bool {
        self.requests.len() == model::MAX_RECORDS || self.physical == model::MAX_SEGMENT_LENGTH
    }
    fn take(&mut self) -> Vec<Request> {
        self.first = None;
        self.physical = 0;
        std::mem::take(&mut self.requests)
    }
}

/// Indexed SplitMix words make generation independent of read/chunk boundaries.
struct Fixture {
    seed: u64,
    index: usize,
    position: usize,
    length: usize,
}
impl Fixture {
    fn new(seed: u64, index: usize, length: usize) -> Self {
        Self {
            seed,
            index,
            position: 0,
            length,
        }
    }
    fn byte(&self, position: usize) -> u8 {
        let mut word = self
            .seed
            .wrapping_add((self.index as u64).wrapping_mul(0x9e3779b97f4a7c15))
            .wrapping_add((position as u64 / 8).wrapping_mul(0xd1b54a32d192ed03));
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d049bb133111eb);
        ((word ^ (word >> 31)) >> ((position % 8) * 8)) as u8
    }
}
impl Read for Fixture {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let count = output
            .len()
            .min(self.length - self.position)
            .min(SCRATCH_BYTES);
        for (offset, value) in output[..count].iter_mut().enumerate() {
            *value = self.byte(self.position + offset);
        }
        self.position += count;
        Ok(count)
    }
}

fn key(index: usize) -> String {
    format!("object-{index:08}")
}

fn persist(store: &Store, config: &Config, batch: &[Request], deadline: Instant) -> Result<()> {
    let runtime = tokio::runtime::Handle::current();
    let kind = if config.packed() {
        ArtifactKind::Segment
    } else {
        ArtifactKind::File
    };
    let length = if config.packed() {
        record::segment_length(
            batch
                .iter()
                .map(|r| r.payload.as_ref().expect("packed payload").len() as u64),
        )?
    } else {
        config.size as u64
    };
    // Requests and their admission guards remain in this owned job through every lookup,
    // physical operation and conditional Writer acknowledgement. Overwrites fence the exact
    // current row and complete old location, never an unconditional key replacement.
    let expected: Vec<_> = batch
        .iter()
        .map(|request| -> Result<ExpectedCurrent> {
            if request.replacement {
                check_deadline(deadline)?;
                let previous = runtime
                    .block_on(store.lookup(&key(request.index)))?
                    .ok_or("overwrite source is absent")?;
                Ok(ExpectedCurrent::Exact {
                    row_id: previous.metadata.row_id,
                    location: previous.location,
                })
            } else {
                Ok(ExpectedCurrent::Absent)
            }
        })
        .collect::<Result<_>>()?;
    let admission = runtime.block_on(store.plan(kind, length))?;
    if let Err(error) = check_deadline(deadline) {
        runtime.block_on(store.abort(record::abort(admission)))?;
        return Err(error);
    }
    let artifact = if config.packed() {
        let slices: Vec<&[u8]> = batch
            .iter()
            .map(|r| r.payload.as_ref().expect("packed payload").as_slice())
            .collect();
        record::publish_segment(&config.root, admission, &slices)
    } else {
        record::publish_file(
            &config.root,
            admission,
            &mut Fixture::new(
                config.seed,
                batch[0].index
                    + if batch[0].replacement {
                        config.objects
                    } else {
                        0
                    },
                config.size,
            ),
        )
    };
    let artifact = match artifact {
        Ok(artifact) => artifact,
        Err(error) => {
            let (error, quiescent) = error.into_parts();
            runtime.block_on(store.abort(quiescent))?;
            return Err(error.into());
        }
    };
    let identity = artifact.plan().artifact().clone();
    let records = batch
        .iter()
        .zip(artifact.spans())
        .zip(expected)
        .map(|((request, span), expected)| {
            let location = match kind {
                ArtifactKind::File => Location::File {
                    artifact: identity.clone(),
                    length: span.length,
                },
                ArtifactKind::Segment => Location::Segment {
                    artifact: identity.clone(),
                    offset: span.offset,
                    length: span.length,
                },
            };
            PublishRecord {
                metadata: RecordMetadata {
                    row_id: StorageToken::generate(),
                    key: key(request.index),
                    encoded_sha256: span.sha256,
                    encoded_length: span.length,
                    logical_size: span.length,
                    format: EncodedFormat::Raw,
                    compression: CompressionDescriptor::Uncompressed,
                    cipher: CipherFormat::Plaintext,
                    locked: false,
                },
                location,
                expected,
                preserve_previous: false,
            }
        })
        .collect();
    match runtime.block_on(store.publish(artifact, records))? {
        PublicationOutcome::Applied { records } if records == batch.len() => Ok(()),
        PublicationOutcome::Applied { .. } => Err("publication applied an incomplete batch".into()),
        PublicationOutcome::Rejected { reason, artifact } => {
            runtime.block_on(store.abort(artifact.into_quiescent()))?;
            Err(format!("unexpected conditional publication rejection: {reason:?}").into())
        }
    }
}

async fn publish_batch(
    store: Store,
    config: Arc<Config>,
    batch: Vec<Request>,
    counters: Arc<Counters>,
    deadline: Instant,
) -> Result<()> {
    let packed = config.packed();
    run_owned_batch(batch, counters, packed, deadline, move |batch| {
        persist(&store, &config, batch, deadline)
    })
    .await
}

async fn run_owned_batch(
    batch: Vec<Request>,
    counters: Arc<Counters>,
    packed: bool,
    deadline: Instant,
    work: impl FnOnce(&[Request]) -> Result<()> + Send + 'static,
) -> Result<()> {
    // The blocking closure owns the WHOLE batch through the SQLite response. Dropping or
    // cancelling this async waiter cannot free a live syscall's bytes, permits or root lock.
    tokio::task::spawn_blocking(move || {
        let batch: Vec<_> = batch
            .into_iter()
            .filter(|request| !request.reply.is_closed())
            .collect();
        if batch.is_empty() {
            return Ok(());
        }
        let outcome = check_deadline(deadline).and_then(|()| work(&batch));
        if outcome.is_ok() {
            counters.artifacts.fetch_add(1, Ordering::SeqCst);
            let records = if packed {
                &counters.packed
            } else {
                &counters.dedicated
            };
            records.fetch_add(batch.len(), Ordering::SeqCst);
        }
        let reply = outcome.as_ref().copied().map_err(ToString::to_string);
        for request in batch {
            acknowledge(request, reply.clone());
        }
        outcome
    })
    .await?
}

async fn build(
    mut receiver: mpsc::Receiver<Request>,
    store: Store,
    config: Arc<Config>,
    counters: Arc<Counters>,
    deadline: Instant,
) -> Result<()> {
    let mut builder = Builder::default();
    loop {
        if builder.full() {
            publish_batch(
                store.clone(),
                config.clone(),
                builder.take(),
                counters.clone(),
                deadline,
            )
            .await?;
        }
        let until = builder.seal_at().unwrap_or(deadline).min(deadline);
        // A fixed first-arrival timeout cannot slide under a continuously nonempty queue.
        let next = timeout_at(until, receiver.recv()).await;
        match next {
            Ok(Some(request)) => {
                if request.reply.is_closed() {
                    continue;
                }
                if Instant::now() >= until && !builder.requests.is_empty() {
                    publish_batch(
                        store.clone(),
                        config.clone(),
                        builder.take(),
                        counters.clone(),
                        deadline,
                    )
                    .await?;
                }
                check_deadline(deadline)?;
                let length = request.payload.as_ref().expect("packed payload").len();
                if !builder.accepts(length) {
                    publish_batch(
                        store.clone(),
                        config.clone(),
                        builder.take(),
                        counters.clone(),
                        deadline,
                    )
                    .await?;
                }
                builder.push(request, Instant::now());
            }
            Ok(None) => {
                if !builder.requests.is_empty() {
                    publish_batch(store, config, builder.take(), counters, deadline).await?;
                }
                return Ok(());
            }
            Err(_) => {
                check_deadline(deadline)?;
                publish_batch(
                    store.clone(),
                    config.clone(),
                    builder.take(),
                    counters.clone(),
                    deadline,
                )
                .await?;
            }
        }
    }
}

#[derive(Default, Serialize)]
struct Latency {
    count: usize,
    sum_seconds: f64,
    max_seconds: f64,
}
impl Latency {
    fn observe(&mut self, seconds: f64) {
        self.count += 1;
        self.sum_seconds += seconds;
        self.max_seconds = self.max_seconds.max(seconds);
    }
    fn merge(&mut self, other: Self) {
        self.count += other.count;
        self.sum_seconds += other.sum_seconds;
        self.max_seconds = self.max_seconds.max(other.max_seconds);
    }
}

#[derive(Clone, Copy)]
enum PublicationStage {
    Append,
    Overwrite,
}

async fn worker(
    worker: usize,
    stage: PublicationStage,
    config: Arc<Config>,
    budget: Arc<Budget>,
    sender: mpsc::Sender<Request>,
    store: Store,
    deadline: Instant,
) -> Result<Latency> {
    let mut latency = Latency::default();
    let replacement = matches!(stage, PublicationStage::Overwrite);
    let objects = if replacement {
        config.objects / 4
    } else {
        config.objects
    };
    for ordinal in (worker..objects).step_by(config.concurrency) {
        let index = if config.measurement && !replacement {
            measurement::initial_index(ordinal, config.objects)
        } else {
            ordinal
        };
        check_deadline(deadline)?;
        let start = Instant::now();
        let admission = budget
            .acquire(if config.packed() { config.size } else { 0 }, deadline)
            .await?;
        let payload = if config.packed() {
            let mut bytes = vec![0; config.size];
            Fixture::new(
                config.seed,
                index + if replacement { config.objects } else { 0 },
                config.size,
            )
            .read_exact(&mut bytes)?;
            Some(bytes)
        } else {
            None
        };
        let (reply, response) = oneshot::channel();
        let request = Request {
            index,
            replacement,
            payload,
            admission,
            reply,
        };
        if config.packed() {
            timeout_at(deadline, sender.send(request))
                .await
                .map_err(|_| Deadline)?
                .map_err(|_| "packed builder stopped")?;
        } else {
            publish_batch(
                store.clone(),
                config.clone(),
                vec![request],
                budget.counters.clone(),
                deadline,
            )
            .await?;
        }
        timeout_at(deadline, response)
            .await
            .map_err(|_| Deadline)?
            .map_err(|_| "publication acknowledgement lost")?
            .map_err(|error| -> model::Error { error.into() })?;
        latency.observe(start.elapsed().as_secs_f64());
    }
    Ok(latency)
}

async fn publish_phase(
    stage: PublicationStage,
    store: Store,
    config: Arc<Config>,
    budget: Arc<Budget>,
    deadline: Instant,
) -> Result<(Latency, f64)> {
    let start = Instant::now();
    let (sender, receiver) = mpsc::channel(model::MAX_RECORDS);
    let builder = tokio::spawn(build(
        receiver,
        store.clone(),
        config.clone(),
        budget.counters.clone(),
        deadline,
    ));
    let mut workers = Vec::with_capacity(config.concurrency);
    for index in 0..config.concurrency {
        workers.push(tokio::spawn(worker(
            index,
            stage,
            config.clone(),
            budget.clone(),
            sender.clone(),
            store.clone(),
            deadline,
        )));
    }
    drop(sender);
    let mut latency = Latency::default();
    let mut error = None;
    // Join every actual owner before closing SQLite; never abort live filesystem work.
    for worker in workers {
        match worker.await {
            Ok(Ok(value)) => latency.merge(value),
            Ok(Err(value)) => {
                error.get_or_insert(value);
            }
            Err(value) => {
                error.get_or_insert(value.into());
            }
        }
    }
    // Preserve the builder's typed cause ahead of peers that only observed its closed channel.
    builder.await??;
    if let Some(error) = error {
        return Err(error);
    }
    Ok((latency, start.elapsed().as_secs_f64()))
}

fn create_root(path: &Path) -> Result<Arc<node::Node>> {
    node::Node::create(path)
}

fn physical_budget(config: &Config) -> model::PhysicalBudget {
    // Reserve final bytes, worst-case one-frame segment overhead per object, and one complete
    // replacement segment. SQLite/WAL and observation headroom remain the coordinator's charge.
    model::PhysicalBudget {
        // The measurement retains originals, one-quarter overwrites and replacement segments
        // until its explicit cleanup phase. Reserve those bytes before any physical admission.
        limit_bytes: config.objects as u64
            * (config.size as u64 + 120)
            * if config.measurement { 2 } else { 1 }
            + model::MAX_SEGMENT_LENGTH,
    }
}

async fn verify(
    store: &Store,
    config: &Config,
    budget: &Budget,
    deadline: Instant,
) -> Result<usize> {
    let mut verified = 0;
    for index in 0..config.objects {
        check_deadline(deadline)?;
        let admission = budget.acquire(0, deadline).await?;
        let pinned = timeout_at(deadline, store.pin(&key(index)))
            .await
            .map_err(|_| Deadline)??
            .ok_or("published object is missing")?;
        let size = config.size;
        let seed = config.seed;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _admission = admission;
            let mut pin = pinned.pin;
            pin.verify_sha256(pinned.record.metadata.encoded_sha256)?;
            let mut actual = [0; SCRATCH_BYTES];
            let mut expected = [0; SCRATCH_BYTES];
            let mut fixture = Fixture::new(seed, index, size);
            let mut hash = Sha256::new();
            loop {
                check_deadline(deadline)?;
                let length = fixture.read(&mut expected)?;
                if length == 0 {
                    break;
                }
                pin.read_exact(&mut actual[..length])?;
                if actual[..length] != expected[..length] {
                    return Err("readback fixture byte mismatch".into());
                }
                hash.update(&expected[..length]);
            }
            if <[u8; 32]>::from(hash.finalize()) != pinned.record.metadata.encoded_sha256
                || pin.read(&mut actual[..1])? != 0
            {
                return Err("readback hash or exact length mismatch".into());
            }
            let offset = size / 3;
            let length = (size - offset).min(257);
            pin.seek(SeekFrom::Start(offset as u64))?;
            pin.read_exact(&mut actual[..length])?;
            let mut range = Fixture::new(seed, index, size);
            range.position = offset;
            range.read_exact(&mut expected[..length])?;
            if actual[..length] != expected[..length] {
                return Err("range readback mismatch".into());
            }
            Ok(())
        })
        .await??;
        verified += 1;
    }
    Ok(verified)
}

async fn cleanup(store: &Store, root: &Path, deadline: Instant) -> Result<()> {
    loop {
        check_deadline(deadline)?;
        let claims = timeout_at(deadline, store.claim_cleanup(model::MAX_RECORDS))
            .await
            .map_err(|_| Deadline)??;
        if claims.is_empty() {
            break;
        }
        let store = store.clone();
        let root = root.to_owned();
        // Once claims are transferred, complete or release every exact claim even if the
        // async caller disappears. An absent staging alias still requires its parent sync.
        tokio::task::spawn_blocking(move || -> Result<()> {
            let runtime = tokio::runtime::Handle::current();
            let mut first_error: Option<model::Error> = None;
            for claim in claims {
                if first_error.is_some() {
                    // Preserve the first failure, but return every unstarted claim to debt.
                    // A failed release itself remains durably claimed for fresh recovery.
                    let _ = runtime.block_on(store.release_cleanup(claim));
                    continue;
                }
                let outcome: Result<()> = match record::cleanup(&root, claim) {
                    Ok(record::CleanupResult::Removed(receipt)) => {
                        match runtime.block_on(store.finish_cleanup(receipt)) {
                            Ok(true) => Ok(()),
                            Ok(false) => Err("cleanup receipt lost exact ownership".into()),
                            Err(error) => Err(error),
                        }
                    }
                    Ok(record::CleanupResult::Pinned(claim)) => {
                        let _ = runtime.block_on(store.release_cleanup(claim));
                        Err("unexpected pinned cleanup alias after verification".into())
                    }
                    Err(error) => {
                        let (error, claim) = error.into_parts();
                        let _ = runtime.block_on(store.release_cleanup(claim));
                        Err(error.into())
                    }
                };
                if let Err(error) = outcome {
                    first_error = Some(error);
                }
            }
            if let Some(error) = first_error {
                return Err(error);
            }
            Ok(())
        })
        .await??;
    }
    let stats = store.stats().await?;
    if stats.pending != 0 || stats.cleanup != 0 {
        return Err("publication left pending artifacts or cleanup debt".into());
    }
    Ok(())
}

#[derive(Serialize)]
struct Report {
    status: &'static str,
    mode: Mode,
    size: usize,
    objects: usize,
    published: usize,
    verified: usize,
    artifact_count: usize,
    packed_records: usize,
    dedicated_records: usize,
    peak_admitted_bytes: usize,
    peak_pending_records: usize,
    publication_seconds: f64,
    publication_latency: Latency,
    admission_limit_bytes: usize,
    pending_limit_records: usize,
    timing_scope: &'static str,
    collection_check: gc::CollectionReport,
    #[serde(flatten)]
    measurement: Option<measurement::Metrics>,
}

async fn run(config: Config) -> Result<Report> {
    config.validate()?;
    if config.measurement {
        return measurement::run(config).await;
    }
    let deadline = Instant::now() + Duration::from_secs(config.deadline_seconds);
    let lifetime = create_root(&config.root)?;
    let store = Store::open(lifetime.clone(), physical_budget(&config))?;
    let budget = Arc::new(Budget::new(lifetime));
    let config = Arc::new(config);
    let result = async {
        let (latency, publication_seconds) = publish_phase(
            PublicationStage::Append, store.clone(), config.clone(), budget.clone(), deadline,
        ).await?;
        let verified = verify(&store, &config, &budget, deadline).await?;
        cleanup(&store, &config.root, deadline).await?;
        // This append-only diagnostic has no dead records. Exercise bounded enumeration and
        // require it to preserve every live artifact; churn comparisons have separate fixtures.
        let collection_check = gc::collect_pass(
            &config.root, store.clone(), budget.clone(), model::MAX_RECORDS,
            config.objects.div_ceil(model::MAX_RECORDS) + 1, deadline,
        ).await?;
        if !collection_check.completed || collection_check.candidates != 0 {
            return Err("fully live publication unexpectedly selected collection".into());
        }
        let stats = store.stats().await?;
        if stats.records != config.objects as u64 || stats.current != config.objects as u64
            || stats.history != 0 || stats.locked != 0 || latency.count != config.objects {
            return Err("authoritative publication counts disagree".into());
        }
        let counters = &budget.counters;
        if counters.bytes.load(Ordering::SeqCst) != 0 || counters.pending.load(Ordering::SeqCst) != 0 {
            return Err("admission ownership remained after worker joins".into());
        }
        Ok(Report { status: "PASS", mode: config.mode, size: config.size, objects: config.objects,
            published: latency.count, verified, artifact_count: counters.artifacts.load(Ordering::SeqCst),
            packed_records: counters.packed.load(Ordering::SeqCst), dedicated_records: counters.dedicated.load(Ordering::SeqCst),
            peak_admitted_bytes: counters.peak_bytes.load(Ordering::SeqCst), peak_pending_records: counters.peak_pending.load(Ordering::SeqCst),
            publication_seconds, publication_latency: latency, admission_limit_bytes: ADMISSION_BYTES,
            pending_limit_records: model::MAX_RECORDS, collection_check, measurement: None,
            timing_scope: "publication includes deterministic fixture generation, admission, filesystem durability/hash validation and SQLite acknowledgement; readback/cleanup excluded; no encoding or S3" })
    }.await;
    let closed = store.close().await;
    match result {
        Ok(report) => {
            closed?;
            Ok(report)
        }
        Err(error) => {
            closed?;
            Err(error)
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let result = async {
        let mut arguments = std::env::args_os().skip(1);
        let path = arguments
            .next()
            .ok_or("exactly one configuration path required")?;
        if arguments.next().is_some() {
            return Err("exactly one configuration path required".into());
        }
        let mut bytes = Vec::new();
        File::open(path)?
            .take(16 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 16 * 1024 {
            return Err("configuration exceeds bounded length".into());
        }
        let config: Config = serde_json::from_slice(&bytes)?;
        run(config).await
    }
    .await;
    match result {
        Ok(report) => println!("{}", serde_json::to_string(&report).expect("finite report")),
        Err(error) => {
            let timed_out = error.is::<Deadline>();
            println!(
                "{}",
                serde_json::json!({"status": if timed_out { "INCONCLUSIVE" } else { "FAIL" }, "reason": error.to_string()})
            );
            std::process::exit(if timed_out { 2 } else { 1 });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicBool;

    fn config(root: PathBuf) -> Config {
        Config {
            root,
            mode: Mode::Packed,
            size: 1024,
            objects: 16,
            concurrency: 4,
            seed: 0x5eed,
            known_length: true,
            measurement: false,
            deadline_seconds: 10,
        }
    }

    async fn request(
        budget: &Budget,
        size: usize,
    ) -> (Request, oneshot::Receiver<std::result::Result<(), String>>) {
        let admission = budget
            .acquire(size, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        let (reply, response) = oneshot::channel();
        (
            Request {
                index: 0,
                replacement: false,
                payload: Some(vec![0; size]),
                admission,
                reply,
            },
            response,
        )
    }

    #[test]
    fn placement_requires_known_small_encoded_length() {
        let mut config = config(PathBuf::from("/fresh"));
        config.size = record::MAX_PACKED_RECORD_LENGTH as usize;
        assert!(config.packed());
        config.size += 1;
        assert!(!config.packed());
        config.size = 1;
        config.known_length = false;
        assert!(!config.packed());
        config.known_length = true;
        config.mode = Mode::Files;
        assert!(!config.packed());
        config.objects = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn fresh_root_rejects_existing_and_symlinked_parent_without_touching_data() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("store");
        let lock = create_root(&root).unwrap();
        fs::write(root.join("canary"), b"preserve").unwrap();
        assert!(create_root(&root).is_err());
        assert_eq!(fs::read(root.join("canary")).unwrap(), b"preserve");
        let linked = temporary.path().join("alias");
        std::os::unix::fs::symlink(&root, &linked).unwrap();
        assert!(create_root(&linked.join("child")).is_err());
        assert!(!root.join("child").exists());
        drop(lock);
    }

    #[tokio::test]
    async fn builder_seals_at_exact_physical_bytes_including_headers() {
        let budget = Budget::new(Arc::new(()));
        let mut builder = Builder::default();
        let mut replies = Vec::new();
        for _ in 0..3 {
            let (request, reply) =
                request(&budget, record::MAX_PACKED_RECORD_LENGTH as usize).await;
            builder.push(request, Instant::now());
            replies.push(reply);
        }
        assert!(!builder.accepts(record::MAX_PACKED_RECORD_LENGTH as usize));
        let remainder =
            (model::MAX_SEGMENT_LENGTH - builder.physical - model::RECORD_HEADER_LENGTH) as usize;
        let (request, reply) = request(&budget, remainder).await;
        builder.push(request, Instant::now());
        replies.push(reply);
        assert_eq!(builder.physical, model::MAX_SEGMENT_LENGTH);
        assert!(builder.full());
        assert!(!builder.accepts(0));
        drop(builder.take());
        assert_eq!(budget.counters.pending.load(Ordering::SeqCst), 0);
        assert!(builder.seal_at().is_none());
    }

    #[tokio::test]
    async fn builder_count_cap_and_first_arrival_deadline_do_not_slide() {
        let budget = Budget::new(Arc::new(()));
        let first = Instant::now();
        let mut builder = Builder::default();
        let mut replies = Vec::new();
        for index in 0..model::MAX_RECORDS {
            // Empty encoded records are legal at the component layer; the fixed driver
            // separately requires size >=1. This isolates the record-count seal boundary.
            let (request, reply) = request(&budget, 0).await;
            builder.push(request, first + Duration::from_nanos(index as u64));
            replies.push(reply);
            assert_eq!(builder.seal_at(), Some(first + LINGER));
        }
        assert!(builder.full());
        assert!(!builder.accepts(0));
        assert_eq!(
            builder.physical,
            model::SEGMENT_HEADER_LENGTH + model::MAX_RECORDS as u64 * model::RECORD_HEADER_LENGTH
        );
        drop(builder);
        assert_eq!(budget.counters.bytes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn byte_backpressure_precedes_fixture_allocation() {
        let budget = Arc::new(Budget::new(Arc::new(())));
        let deadline = Instant::now() + Duration::from_secs(2);
        let full = budget
            .acquire(ADMISSION_BYTES - IO_RESERVATION, deadline)
            .await
            .unwrap();
        let allocated = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let budget = budget.clone();
            let allocated = allocated.clone();
            let started = started.clone();
            async move {
                started.store(true, Ordering::SeqCst);
                let admission = budget.acquire(1024, deadline).await.unwrap();
                let bytes = vec![1_u8; 1024];
                allocated.store(true, Ordering::SeqCst);
                (bytes, admission)
            }
        });
        while !started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        assert!(!allocated.load(Ordering::SeqCst));
        assert!(!task.is_finished());
        drop(full);
        let (bytes, admission) = task.await.unwrap();
        assert!(allocated.load(Ordering::SeqCst));
        drop(bytes);
        drop(admission);
        assert_eq!(budget.counters.pending.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_waiter_keeps_actual_blocking_bytes_permits_and_lock_alive() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("written-after-cancel");
        let file = File::create(temporary.path().join("lock")).unwrap();
        file.try_lock().unwrap();
        let lifetime: Arc<dyn Send + Sync> = Arc::new(file);
        let weak = Arc::downgrade(&lifetime);
        let budget = Budget::new(lifetime);
        let counters = budget.counters.clone();
        let (request, response) = request(&budget, 1024).await;
        let (started, entered) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new(Barrier::new(2));
        let task = tokio::spawn(run_owned_batch(
            vec![request],
            counters.clone(),
            true,
            Instant::now() + Duration::from_secs(5),
            {
                let release = release.clone();
                let path = path.clone();
                move |batch| {
                    started.send(()).unwrap();
                    release.wait();
                    fs::write(&path, batch[0].payload.as_ref().unwrap())?;
                    File::open(&path)?.sync_all()?;
                    Ok(())
                }
            },
        ));
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        task.abort();
        let outcome = task.await;
        drop(response);
        drop(budget);
        let lock_alive = weak.upgrade().is_some();
        let second_lock = File::open(temporary.path().join("lock")).unwrap();
        let lock_blocked = matches!(
            second_lock.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        );
        let pending = counters.pending.load(Ordering::SeqCst);
        let admitted = counters.bytes.load(Ordering::SeqCst);
        // Release the actual job before assertions so a regression cannot strand a barrier.
        release.wait();
        assert!(outcome.unwrap_err().is_cancelled());
        assert!(lock_alive && lock_blocked);
        assert_eq!(pending, 1);
        assert!(admitted >= 1024);
        tokio::time::timeout(Duration::from_secs(2), async {
            while counters.pending.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(fs::read(path).unwrap(), vec![0; 1024]);
        // Counters are decremented in Drop before the guard's root Arc field is dropped;
        // let the same actual destructor finish before checking the final lifetime.
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(counters.bytes.load(Ordering::SeqCst), 0);
        second_lock.try_lock().unwrap();
    }

    #[tokio::test]
    async fn cancelled_before_start_discards_without_calling_backend() {
        let budget = Budget::new(Arc::new(()));
        let (request, response) = request(&budget, 1024).await;
        drop(response);
        let called = Arc::new(AtomicBool::new(false));
        run_owned_batch(
            vec![request],
            budget.counters.clone(),
            true,
            Instant::now() + Duration::from_secs(2),
            {
                let called = called.clone();
                move |_| {
                    called.store(true, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await
        .unwrap();
        assert!(!called.load(Ordering::SeqCst));
        assert_eq!(budget.counters.pending.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn acknowledgement_observes_released_admission() {
        let budget = Budget::new(Arc::new(()));
        let (request, response) = request(&budget, 1024).await;
        run_owned_batch(
            vec![request],
            budget.counters.clone(),
            true,
            Instant::now() + Duration::from_secs(2),
            |_| Ok(()),
        )
        .await
        .unwrap();
        response.await.unwrap().unwrap();
        assert_eq!(budget.counters.bytes.load(Ordering::SeqCst), 0);
        assert_eq!(budget.counters.pending.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn fixture_bytes_are_stable_across_chunking_and_differ_by_seed_and_object() {
        let mut all = vec![0; SCRATCH_BYTES + 17];
        Fixture::new(7, 3, all.len()).read_exact(&mut all).unwrap();
        let mut split = vec![0; all.len()];
        let mut fixture = Fixture::new(7, 3, all.len());
        for chunk in split.chunks_mut(13) {
            fixture.read_exact(chunk).unwrap();
        }
        assert_eq!(all, split);
        assert_ne!(Fixture::new(8, 3, 1).byte(0), all[0]);
        assert_ne!(Fixture::new(7, 4, 1).byte(0), all[0]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tiny_files_packed_and_unknown_fallback_verify_and_leave_no_debt() {
        let temporary = tempfile::tempdir().unwrap();
        for (index, (mode, known_length)) in [
            (Mode::Files, true),
            (Mode::Packed, true),
            (Mode::Packed, false),
        ]
        .into_iter()
        .enumerate()
        {
            let mut config = config(temporary.path().join(format!("store-{index}")));
            config.mode = mode;
            config.known_length = known_length;
            let report = run(config).await.unwrap();
            assert_eq!(report.published, 16);
            assert_eq!(report.verified, 16);
            assert!(report.peak_admitted_bytes <= ADMISSION_BYTES);
            assert!(report.peak_pending_records <= 4);
            assert_eq!(
                report.packed_records,
                if mode == Mode::Packed && known_length {
                    16
                } else {
                    0
                }
            );
            assert_eq!(report.packed_records + report.dedicated_records, 16);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_post_plan_deadline_aborts_before_filesystem_creation() {
        let temporary = tempfile::tempdir().unwrap();
        let config = Arc::new(config(temporary.path().join("store")));
        let lifetime = create_root(&config.root).unwrap();
        let store = Store::open(lifetime.clone(), physical_budget(&config)).unwrap();
        let budget = Budget::new(lifetime);
        let (request, _response) = request(&budget, config.size).await;
        let result = tokio::task::spawn_blocking({
            let store = store.clone();
            let config = config.clone();
            move || {
                persist(
                    &store,
                    &config,
                    &[request],
                    Instant::now() - Duration::from_secs(1),
                )
            }
        })
        .await
        .unwrap();
        assert!(result.unwrap_err().is::<Deadline>());
        assert_eq!(store.stats().await.unwrap().pending, 0);
        assert!(fs::read_dir(&config.root).unwrap().all(|entry| {
            let name = entry.unwrap().file_name();
            let name = name.to_string_lossy();
            !name.starts_with(".pending-")
                && !name.starts_with("segment-")
                && !name.starts_with("file-")
        }));
        cleanup(
            &store,
            &config.root,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(store.stats().await.unwrap().cleanup, 0);
        store.close().await.unwrap();
    }
}
