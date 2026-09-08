//! Isolated namespace comparison. No production routing or placement configuration changes.
mod common;
// Compile the exact production descriptor/lease coordinator into the laboratory. Its internal
// syscall timings are unavailable here; do not substitute request waits for actual fsync counts.
#[path = "../../../crates/cairn-blob/src/commit.rs"]
mod commit;

use bytes::Bytes;
use cairn_blob::{LocalBlobStore, open_readonly_nofollow};
use cairn_types::storage::io::StorageIoLease;
use cairn_types::testing::{fixture_storage_cleanup, fixture_storage_io};
use cairn_types::traits::{BlobStore, ReconcileOracle};
use cairn_types::*;
use common::{distribution, emit, payload};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::OnceCell;

type Error = Box<dyn std::error::Error + Send + Sync>;
const PUBLICATION_VARIANT: &str = "raw_namespace_descriptor_coalescer_exact_cleanup_fixture_v3";

fn io_err(error: std::io::Error) -> BlobError {
    BlobError::Io(error.to_string())
}

#[cfg(test)]
mod owned_file {
    pub(crate) use cairn_types::testing::fixture_storage_io as test_lease;
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Layout {
    Flat,
    Fanout,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    root: PathBuf,
    layout: Layout,
    objects: usize,
    concurrency: usize,
    buckets: usize,
    size: usize,
    seed: u64,
}

impl Config {
    fn validate(&self) -> Result<(), Error> {
        if !(1..=128).contains(&self.buckets)
            || !(1..=128).contains(&self.concurrency)
            || !(4..=100_000).contains(&self.objects)
            || !self.objects.is_multiple_of(4 * self.buckets)
            || !(1024..=1_048_576).contains(&self.size)
            || !self.root.is_absolute()
            || self.root.exists()
        {
            return Err("invalid bounded fanout workload or nonempty root".into());
        }
        let parent = self.root.parent().ok_or("missing root parent")?;
        if parent.canonicalize()? != parent {
            return Err("root parent must be canonical".into());
        }
        Ok(())
    }

    fn identifier(&self, index: usize) -> String {
        // Seeded UUID-shaped identities, with a collision-free index in the low 62 bits.
        let mut mixed = self
            .seed
            .wrapping_add(index as u64)
            .wrapping_add(0x9e3779b97f4a7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
        mixed ^= mixed >> 31;
        mixed = (mixed & !0xf000) | 0x4000;
        format!("{mixed:016x}{:016x}", 0x8000_0000_0000_0000 | index as u64)
    }

    fn bucket(&self, index: usize) -> String {
        format!("lab-{:04}", index % self.buckets)
    }
    fn group(&self, index: usize) -> usize {
        index / self.buckets
    }
    fn location(&self, index: usize) -> StoragePath {
        self.location_for(index, &self.identifier(index))
    }
    fn location_for(&self, index: usize, id: &str) -> StoragePath {
        let bucket = self.bucket(index);
        StoragePath::from_string(match self.layout {
            Layout::Flat => format!("{bucket}/{id}"),
            Layout::Fanout => format!("{bucket}/{}/{id}", &id[..2]),
        })
    }
}

#[derive(Default)]
struct CreationMetrics {
    directories: AtomicU64,
    nanos: AtomicU64,
}

struct Publisher {
    config: Arc<Config>,
    buckets: Vec<OnceCell<PathBuf>>,
    leaves: Vec<OnceCell<PathBuf>>,
    coalescer: commit::DirSyncCoalescer,
    creation: Arc<CreationMetrics>,
}

async fn create_durable_directory(
    parent: PathBuf,
    path: PathBuf,
    metrics: Arc<CreationMetrics>,
) -> Result<PathBuf, Error> {
    let lease = fixture_storage_io();
    tokio::task::spawn_blocking(move || {
        let _lease = lease;
        let start = Instant::now();
        std::fs::create_dir(&path)?;
        File::open(parent)?.sync_all()?;
        metrics.directories.fetch_add(1, Ordering::Relaxed);
        metrics
            .nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        Ok(path)
    })
    .await?
}

struct Unpublished {
    temporary: PathBuf,
    final_path: PathBuf,
    armed: bool,
    lease: StorageIoLease,
}
impl Drop for Unpublished {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.temporary);
            let _ = std::fs::remove_file(&self.final_path);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    Create,
    Write,
    FileSync,
    Rename,
    DirectorySync,
}
fn inject(fault: Option<Fault>, point: Fault) -> Result<(), Error> {
    if fault == Some(point) {
        return Err("injected laboratory publication failure".into());
    }
    Ok(())
}

impl Publisher {
    fn new(config: Arc<Config>) -> Self {
        let buckets = (0..config.buckets).map(|_| OnceCell::new()).collect();
        let leaves = (0..if config.layout == Layout::Fanout {
            config.buckets * 256
        } else {
            0
        })
            .map(|_| OnceCell::new())
            .collect();
        Self {
            config,
            buckets,
            leaves,
            coalescer: commit::DirSyncCoalescer::spawn(),
            creation: Arc::new(CreationMetrics::default()),
        }
    }

    async fn directory(&self, index: usize, id: &str) -> Result<PathBuf, Error> {
        let bucket_number = index % self.config.buckets;
        let parent = self.buckets[bucket_number]
            .get_or_try_init(|| {
                create_durable_directory(
                    self.config.root.clone(),
                    self.config.root.join(self.config.bucket(index)),
                    self.creation.clone(),
                )
            })
            .await?
            .clone();
        if self.config.layout == Layout::Flat {
            return Ok(parent);
        }
        let prefix = &id[..2];
        let leaf_number = usize::from_str_radix(prefix, 16)?;
        self.leaves[bucket_number * 256 + leaf_number]
            .get_or_try_init(|| {
                create_durable_directory(parent.clone(), parent.join(prefix), self.creation.clone())
            })
            .await
            .cloned()
    }

    async fn publish(&self, index: usize, data: Bytes, fault: Option<Fault>) -> Result<(), Error> {
        let id = self.config.identifier(index);
        let directory = self.directory(index, &id).await?;
        // Both names and their cleanup owner exist before detached filesystem work can create.
        let owner = Unpublished {
            temporary: self
                .config
                .root
                .join(".staging")
                .join(format!("fanout-{id}.tmp")),
            final_path: self
                .config
                .root
                .join(self.config.location_for(index, &id).as_str()),
            armed: true,
            lease: fixture_storage_io(),
        };
        let mut owner = tokio::task::spawn_blocking(move || -> Result<Unpublished, Error> {
            let file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&owner.temporary)?;
            inject(fault, Fault::Create)?;
            let mut writer = BufWriter::with_capacity(256 * 1024, file);
            writer.write_all(&data)?;
            inject(fault, Fault::Write)?;
            let file = writer.into_inner()?;
            file.sync_data()?;
            inject(fault, Fault::FileSync)?;
            std::fs::rename(&owner.temporary, &owner.final_path)?;
            inject(fault, Fault::Rename)?;
            Ok(owner)
        })
        .await??;
        self.coalescer
            .sync_file(Arc::new(open_readonly_nofollow(&directory)?), &owner.lease)
            .await?;
        inject(fault, Fault::DirectorySync)?;
        owner.armed = false;
        Ok(())
    }
}

struct Oracle {
    config: Arc<Config>,
    all_live: bool,
}
#[async_trait::async_trait]
impl ReconcileOracle for Oracle {
    async fn live_blobs(&self, paths: &[StoragePath]) -> Result<Vec<bool>, MetaError> {
        paths
            .iter()
            .map(|path| {
                let id = path.as_str().rsplit('/').next().unwrap_or("");
                let index = id
                    .get(16..)
                    .and_then(|low| u64::from_str_radix(low, 16).ok())
                    .map(|low| (low & 0x3fff_ffff_ffff_ffff) as usize)
                    .ok_or_else(|| MetaError::Engine("invalid laboratory identity".into()))?;
                if index >= self.config.objects || self.config.location(index) != *path {
                    return Err(MetaError::Engine(
                        "unexpected laboratory storage location".into(),
                    ));
                }
                Ok(self.all_live || self.config.group(index) % 4 == 1)
            })
            .collect()
    }
    async fn live_session(&self, _: &UploadId) -> Result<bool, MetaError> {
        Ok(false)
    }
    async fn live_multipart_parts(&self, paths: &[StoragePath]) -> Result<Vec<bool>, MetaError> {
        if !paths.is_empty() {
            return Err(MetaError::Engine(
                "unexpected laboratory multipart artifact".into(),
            ));
        }
        Ok(Vec::new())
    }
}

async fn verify_read(
    store: &LocalBlobStore,
    config: &Config,
    index: usize,
    data: &[u8],
    ranged: bool,
) -> Result<(), Error> {
    let (offset, length) = if ranged {
        (config.size / 4, config.size / 2)
    } else {
        (0, config.size)
    };
    let range = ranged.then_some(ByteRange {
        offset: offset as u64,
        length: length as u64,
    });
    let mut body = store
        .open_raw(
            &config.location(index),
            range,
            BlobCipher::KnownPlaintext,
            &CompressionDescriptor::Uncompressed,
            config.size as u64,
        )
        .await?
        .body;
    let mut received = 0;
    while let Some(frame) = body.next().await {
        let frame = frame?;
        if received + frame.len() > length
            || data.get(offset + received..offset + received + frame.len()) != Some(frame.as_ref())
        {
            return Err("laboratory range/content mismatch".into());
        }
        received += frame.len();
    }
    if received != length {
        return Err("laboratory short read".into());
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Phase {
    Publish,
    Read,
    Delete,
    Verify,
}
impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Read => "read",
            Self::Delete => "delete",
            Self::Verify => "verify",
        }
    }
}

async fn phase(
    config: Arc<Config>,
    store: Arc<LocalBlobStore>,
    publisher: Arc<Publisher>,
    data: Bytes,
    phase: Phase,
) -> Result<serde_json::Value, Error> {
    emit(json!({"kind": "phase", "phase": phase.name()}));
    let start = Instant::now();
    let next = Arc::new(AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..config.concurrency {
        let (config, store, publisher, data, next) = (
            config.clone(),
            store.clone(),
            publisher.clone(),
            data.clone(),
            next.clone(),
        );
        tasks.spawn(async move {
            let mut samples = [Vec::new(), Vec::new()];
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= config.objects {
                    break;
                }
                let group = config.group(index);
                if matches!(phase, Phase::Delete) && !group.is_multiple_of(2) {
                    continue;
                }
                let start = Instant::now();
                let ranged = !group.is_multiple_of(2);
                match phase {
                    Phase::Publish => publisher.publish(index, data.clone(), None).await?,
                    Phase::Read => verify_read(&store, &config, index, &data, ranged).await?,
                    Phase::Delete => {
                        let (cleanup, lease) = fixture_storage_cleanup(
                            BucketName::parse(&config.bucket(index))?,
                            config.location(index),
                        );
                        store.cleanup_storage(&cleanup, lease).await?;
                    }
                    Phase::Verify => {
                        if group % 4 == 1 {
                            verify_read(&store, &config, index, &data, false).await?;
                        } else if !matches!(
                            store.probe(&config.location(index)).await,
                            Err(BlobError::NotFound)
                        ) {
                            return Err::<[Vec<f64>; 2], Error>(
                                "reclamation survivor set mismatch".into(),
                            );
                        }
                    }
                }
                samples[usize::from(matches!(phase, Phase::Read) && ranged)]
                    .push(start.elapsed().as_secs_f64());
            }
            Ok::<_, Error>(samples)
        });
    }
    let mut samples = [Vec::new(), Vec::new()];
    while let Some(result) = tasks.join_next().await {
        for (target, source) in samples.iter_mut().zip(result??) {
            target.extend(source);
        }
    }
    let seconds = start.elapsed().as_secs_f64();
    let count = samples.iter().map(Vec::len).sum::<usize>();
    let result = json!({"kind": "operations", "phase": phase.name(), "wall_seconds": seconds, "count": count,
        "successful_per_second": count as f64 / seconds, "latencies": samples.iter_mut().map(|s| distribution(s)).collect::<Vec<_>>()});
    emit(result.clone());
    Ok(result)
}

async fn reconcile(
    store: &LocalBlobStore,
    config: Arc<Config>,
    all_live: bool,
) -> Result<serde_json::Value, Error> {
    let name = if all_live {
        "live_scan"
    } else {
        "cleanup_scan"
    };
    emit(json!({"kind": "phase", "phase": name}));
    let start = Instant::now();
    let report = store
        .reconcile(
            &Oracle {
                config: config.clone(),
                all_live,
            },
            ReconcileOpts {
                batch_size: 1024,
                parallelism: 4,
                staging_safety_margin_secs: 0,
            },
            fixture_storage_io(),
        )
        .await?;
    let seconds = start.elapsed().as_secs_f64();
    if report.errors != 0
        || report.blobs_scanned
            != (if all_live {
                config.objects
            } else {
                config.objects / 2
            }) as u64
        || report.orphans_reclaimed != (if all_live { 0 } else { config.objects / 4 }) as u64
    {
        return Err(format!("unexpected reconcile counts: {report:?}").into());
    }
    let result = json!({"kind": "reconcile", "phase": name, "wall_seconds": seconds,
        "scanned": report.blobs_scanned, "reclaimed": report.orphans_reclaimed, "directories_pruned": report.dirs_pruned});
    emit(result.clone());
    Ok(result)
}

async fn run(config: Config) -> Result<(), Error> {
    config.validate()?;
    emit(
        json!({"kind": "start", "workload": config, "publication_variant": PUBLICATION_VARIANT,
        "metadata_admission": false, "scope": "isolated_raw_namespace_fixture_not_journal_adoption"}),
    );
    let config = Arc::new(config);
    let store = Arc::new(LocalBlobStore::open(&config.root, fixture_storage_io()).await?);
    let publisher = Arc::new(Publisher::new(config.clone()));
    let data = Bytes::from(payload(config.size, config.seed));
    phase(
        config.clone(),
        store.clone(),
        publisher.clone(),
        data.clone(),
        Phase::Publish,
    )
    .await?;
    let durability = json!({"kind": "durability", "directory_sync_requests": config.objects, "coalesced_directory_sync_calls": null,
        "coalesced_directory_sync_cumulative_seconds": null, "created_directories": publisher.creation.directories.load(Ordering::Relaxed),
        "publication_variant": PUBLICATION_VARIANT,
        "unavailable": "production_descriptor_coalescer_has_no_syscall_measurement_hook",
        "directory_creation_parent_sync_cumulative_seconds": publisher.creation.nanos.load(Ordering::Relaxed) as f64 / 1e9});
    emit(durability);
    phase(
        config.clone(),
        store.clone(),
        publisher.clone(),
        data.clone(),
        Phase::Read,
    )
    .await?;
    reconcile(&store, config.clone(), true).await?;
    phase(
        config.clone(),
        store.clone(),
        publisher.clone(),
        data.clone(),
        Phase::Delete,
    )
    .await?;
    reconcile(&store, config.clone(), false).await?;
    phase(config.clone(), store, publisher, data, Phase::Verify).await?;
    // A new process per arm makes the kernel high-water mark comparable; it is not a heap/leak diagnosis.
    let status = std::fs::read_to_string("/proc/self/status")?;
    let high_water_kib = status.lines().find_map(|line| {
        line.strip_prefix("VmHWM:")
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
    });
    emit(json!({"kind": "complete", "status": "PASS", "peak_rss_kib": high_water_kib}));
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Error> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("expected coordinator config path")?;
    run(serde_json::from_slice(&std::fs::read(path)?)?).await
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(root: PathBuf, layout: Layout) -> Config {
        Config {
            root,
            layout,
            objects: 16,
            concurrency: 4,
            buckets: 2,
            size: 1024,
            seed: 0x5eed,
        }
    }

    #[test]
    fn identity_is_seeded_unique_and_reversible_without_a_location_map() {
        let cfg = config(PathBuf::from("/unused"), Layout::Fanout);
        let first = cfg.identifier(0);
        assert_eq!(first.len(), 32);
        assert_ne!(first, cfg.identifier(1));
        let mut other = config(PathBuf::from("/unused"), Layout::Fanout);
        other.seed += 1;
        assert_ne!(first, other.identifier(0));
        assert_eq!(
            cfg.location(0).as_str().split('/').nth(1).unwrap(),
            &first[..2]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn both_layouts_have_identical_exact_survivor_bytes() {
        for layout in [Layout::Flat, Layout::Fanout] {
            let root = tempfile::tempdir().unwrap();
            run(config(root.path().join("data"), layout)).await.unwrap();
        }
    }

    #[tokio::test]
    async fn final_survivor_verification_checks_bytes_outside_the_measured_range() {
        for layout in [Layout::Flat, Layout::Fanout] {
            let root = tempfile::tempdir().unwrap();
            let mut cfg = config(root.path().join("data"), layout);
            cfg.objects = 4;
            cfg.buckets = 1;
            let cfg = Arc::new(cfg);
            let store = Arc::new(
                LocalBlobStore::open(&cfg.root, fixture_storage_io())
                    .await
                    .unwrap(),
            );
            let publisher = Arc::new(Publisher::new(cfg.clone()));
            let data = Bytes::from(vec![1; cfg.size]);
            publisher.publish(1, data.clone(), None).await.unwrap();
            let mut corrupted = data.to_vec();
            corrupted[0] = 2;
            std::fs::write(cfg.root.join(cfg.location(1).as_str()), corrupted).unwrap();
            assert!(
                phase(cfg, store, publisher, data, Phase::Verify)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn publication_failures_never_acknowledge_or_strand_a_file() {
        for point in [
            Fault::Create,
            Fault::Write,
            Fault::FileSync,
            Fault::Rename,
            Fault::DirectorySync,
        ] {
            let root = tempfile::tempdir().unwrap();
            let cfg = Arc::new(config(root.path().join("data"), Layout::Fanout));
            let _store = LocalBlobStore::open(&cfg.root, fixture_storage_io())
                .await
                .unwrap();
            let publisher = Publisher::new(cfg.clone());
            assert!(
                publisher
                    .publish(0, Bytes::from(vec![1; cfg.size]), Some(point))
                    .await
                    .is_err()
            );
            assert!(!cfg.root.join(cfg.location(0).as_str()).exists());
            assert!(
                std::fs::read_dir(cfg.root.join(".staging"))
                    .unwrap()
                    .all(|entry| entry.unwrap().file_name() == "multipart")
            );
        }
    }
}
