//! Fixed raw-record churn, collection and offline recovery measurement fixture.
//! All proposed performance decisions remain in the charged Python coordinator.
use super::model::Result;
use super::node::Node;
use super::store::{DeleteOutcome, Store, StoreStats};
use super::{
    ADMISSION_BYTES, Budget, CipherFormat, CompressionDescriptor, Config, EncodedFormat, Fixture,
    Latency, Ordering, PublicationStage, Report, SCRATCH_BYTES, check_deadline, cleanup, gc, key,
    model, physical_budget, publish_phase, snapshot, verify,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

// At most one bounded 256-row metadata page, catalog line and stream/hash scratch set.
// SQLite's cache and runtime allocations are separately observed by the process RSS peak.
const SNAPSHOT_RESERVATION: usize = 1024 * 1024;

#[derive(Serialize)]
pub(super) struct Metrics {
    measurement_protocol: u32,
    workload_seconds: f64,
    overwritten: usize,
    deleted: usize,
    final_records: u64,
    survivor_verified: usize,
    restored_verified: usize,
    reopened_verified: usize,
    overwrite_seconds: f64,
    delete_seconds: f64,
    readback_seconds: f64,
    range_read_seconds: f64,
    range_read_count: usize,
    range_read_bytes: u64,
    collection_seconds: f64,
    cleanup_seconds: f64,
    snapshot_seconds: f64,
    restore_seconds: f64,
    reopen_seconds: f64,
    pending_writes: u64,
    cleanup_debts: u64,
    live_physical_bytes: u64,
    peak_rss_kib: u64,
    snapshot_artifacts: usize,
    snapshot_database_bytes: u64,
    snapshot_catalog_bytes: u64,
}

struct Online {
    publication_latency: Latency,
    publication_seconds: f64,
    artifact_count: usize,
    packed_records: usize,
    dedicated_records: usize,
    verified: usize,
    workload_seconds: f64,
    overwritten: usize,
    deleted: usize,
    overwrite_seconds: f64,
    delete_seconds: f64,
    readback_seconds: f64,
    range_read_seconds: f64,
    survivor_verified: usize,
    ranges: Verification,
    collection_seconds: f64,
    cleanup_seconds: f64,
    collection_check: gc::CollectionReport,
    stats: StoreStats,
}

/// Fixed before measurement, identical in both layouts. Interleave all four key quarters
/// so the later fixed overwrite/delete intervals leave mixed live/dead immutable segments.
pub(super) fn initial_index(ordinal: usize, objects: usize) -> usize {
    (ordinal % 4) * (objects / 4) + ordinal / 4
}

fn deleted(index: usize, objects: usize) -> bool {
    (objects / 4..3 * objects / 4).contains(&index)
}

fn survivor_fixture(index: usize, objects: usize) -> usize {
    index + if index < objects / 4 { objects } else { 0 }
}

fn check_stats(stats: StoreStats, config: &Config) -> Result<()> {
    if stats.records != config.objects as u64 / 2
        || stats.current != stats.records
        || stats.history != 0
        || stats.locked != 0
        || stats.pending != 0
        || stats.cleanup != 0
        || stats.physical_charge == 0
        || stats.physical_charge > stats.physical_limit
    {
        return Err("churn survivor/accounting state differs from the exact fixture".into());
    }
    Ok(())
}

async fn delete_half(
    store: Store,
    config: Arc<Config>,
    budget: Arc<Budget>,
    deadline: Instant,
) -> Result<usize> {
    let mut workers = Vec::with_capacity(config.concurrency);
    for worker in 0..config.concurrency {
        let store = store.clone();
        let config = config.clone();
        let budget = budget.clone();
        workers.push(tokio::spawn(async move {
            let mut removed = 0;
            for offset in (worker..config.objects / 2).step_by(config.concurrency) {
                let index = config.objects / 4 + offset;
                let admission = budget.acquire(0, deadline).await?;
                let store = store.clone();
                // Keep admission through the actual actor lookup and exact delete even if
                // the awaiting task disappears. No per-object oracle remains in memory.
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let _admission = admission;
                    check_deadline(deadline)?;
                    let runtime = tokio::runtime::Handle::current();
                    let current = runtime
                        .block_on(store.lookup(&key(index)))?
                        .ok_or("permanent-delete source is absent")?;
                    check_deadline(deadline)?;
                    if runtime
                        .block_on(store.delete(&current.metadata.row_id, &current.location))?
                        != DeleteOutcome::Applied
                    {
                        return Err("permanent delete lost its exact row/location condition".into());
                    }
                    Ok(())
                })
                .await??;
                removed += 1;
            }
            Ok::<_, model::Error>(removed)
        }));
    }
    let mut removed = 0;
    let mut error = None;
    for worker in workers {
        match worker.await {
            Ok(Ok(count)) => removed += count,
            Ok(Err(value)) => {
                error.get_or_insert(value);
            }
            Err(value) => {
                error.get_or_insert(value.into());
            }
        }
    }
    if let Some(error) = error {
        return Err(error);
    }
    Ok(removed)
}

#[derive(Clone, Copy)]
enum ReadKind {
    Whole,
    Range,
}

#[derive(Default)]
struct Verification {
    records: usize,
    bytes: u64,
}

async fn verify_survivors(
    store: &Store,
    config: &Config,
    budget: &Budget,
    kind: ReadKind,
    deadline: Instant,
) -> Result<Verification> {
    let mut verified = Verification::default();
    for index in 0..config.objects {
        if deleted(index, config.objects) && matches!(kind, ReadKind::Range) {
            continue;
        }
        let admission = budget.acquire(0, deadline).await?;
        let store = store.clone();
        let size = config.size;
        let seed = config.seed;
        let objects = config.objects;
        let bytes = tokio::task::spawn_blocking(move || -> Result<Option<u64>> {
            let _admission = admission;
            check_deadline(deadline)?;
            let runtime = tokio::runtime::Handle::current();
            let pinned = runtime.block_on(store.pin(&key(index)))?;
            if deleted(index, objects) {
                if pinned.is_some() {
                    return Err("permanently deleted key reappeared".into());
                }
                return Ok(None);
            }
            let pinned = pinned.ok_or("acknowledged survivor disappeared")?;
            let metadata = pinned.record.metadata;
            if !pinned.record.is_current
                || metadata.key != key(index)
                || metadata.encoded_length != size as u64
                || metadata.logical_size != size as u64
                || metadata.format != EncodedFormat::Raw
                || metadata.compression != CompressionDescriptor::Uncompressed
                || metadata.cipher != CipherFormat::Plaintext
                || metadata.locked
            {
                return Err("survivor metadata differs from the raw fixture".into());
            }
            let mut pin = pinned.pin;
            let mut actual = [0; SCRATCH_BYTES];
            let mut expected = [0; SCRATCH_BYTES];
            let fixture_index = survivor_fixture(index, objects);
            match kind {
                ReadKind::Whole => {
                    pin.verify_sha256(metadata.encoded_sha256)?;
                    let mut fixture = Fixture::new(seed, fixture_index, size);
                    let mut digest = Sha256::new();
                    loop {
                        check_deadline(deadline)?;
                        let length = fixture.read(&mut expected)?;
                        if length == 0 {
                            break;
                        }
                        pin.read_exact(&mut actual[..length])?;
                        if actual[..length] != expected[..length] {
                            return Err("survivor fixture byte mismatch".into());
                        }
                        digest.update(&expected[..length]);
                    }
                    if <[u8; 32]>::from(digest.finalize()) != metadata.encoded_sha256
                        || pin.read(&mut actual[..1])? != 0
                    {
                        return Err("survivor hash or exact length mismatch".into());
                    }
                    Ok(Some(size as u64))
                }
                ReadKind::Range => {
                    // Whole verification ran separately. This timer reads only the selected
                    // bounded range, including metadata/pin acquisition and fixture checking.
                    let offset = size / 3;
                    let length = (size - offset).min(257);
                    pin.seek(SeekFrom::Start(offset as u64))?;
                    pin.read_exact(&mut actual[..length])?;
                    let mut fixture = Fixture::new(seed, fixture_index, size);
                    fixture.position = offset;
                    fixture.read_exact(&mut expected[..length])?;
                    if actual[..length] != expected[..length] {
                        return Err("survivor range byte mismatch".into());
                    }
                    Ok(Some(length as u64))
                }
            }
        })
        .await??;
        if let Some(bytes) = bytes {
            verified.records += 1;
            verified.bytes += bytes;
        }
    }
    Ok(verified)
}

async fn online(
    store: &Store,
    config: Arc<Config>,
    budget: Arc<Budget>,
    deadline: Instant,
) -> Result<Online> {
    let (publication_latency, publication_seconds) = publish_phase(
        PublicationStage::Append,
        store.clone(),
        config.clone(),
        budget.clone(),
        deadline,
    )
    .await?;
    let counters = &budget.counters;
    // These append counters are immutable report snapshots; the same actual builder and
    // counters continue to measure admission peaks during conditional overwrites and GC.
    let artifact_count = counters.artifacts.load(Ordering::SeqCst);
    let packed_records = counters.packed.load(Ordering::SeqCst);
    let dedicated_records = counters.dedicated.load(Ordering::SeqCst);
    let readback_start = Instant::now();
    let verified = verify(store, &config, &budget, deadline).await?;
    let initial_readback_seconds = readback_start.elapsed().as_secs_f64();
    let churn_start = Instant::now();
    let (overwrite_latency, overwrite_seconds) = publish_phase(
        PublicationStage::Overwrite,
        store.clone(),
        config.clone(),
        budget.clone(),
        deadline,
    )
    .await?;
    let delete_start = Instant::now();
    let removed = delete_half(store.clone(), config.clone(), budget.clone(), deadline).await?;
    let delete_seconds = delete_start.elapsed().as_secs_f64();
    let collection_start = Instant::now();
    let collection_check = gc::collect_pass(
        &config.root,
        store.clone(),
        budget.clone(),
        model::MAX_RECORDS,
        (2 * config.objects).div_ceil(model::MAX_RECORDS) + 1,
        deadline,
    )
    .await?;
    let collection_seconds = collection_start.elapsed().as_secs_f64();
    let cleanup_start = Instant::now();
    cleanup(store, &config.root, deadline).await?;
    let cleanup_seconds = cleanup_start.elapsed().as_secs_f64();
    let workload_seconds = publication_seconds + churn_start.elapsed().as_secs_f64();
    let stats = store.stats().await?;
    check_stats(stats, &config)?;
    if verified != config.objects
        || publication_latency.count != config.objects
        || overwrite_latency.count != config.objects / 4
        || removed != config.objects / 2
    {
        return Err("publication/churn count differs from the predeclared fixture".into());
    }
    let readback_start = Instant::now();
    let survivors = verify_survivors(store, &config, &budget, ReadKind::Whole, deadline).await?;
    let readback_seconds = initial_readback_seconds + readback_start.elapsed().as_secs_f64();
    let range_start = Instant::now();
    let ranges = verify_survivors(store, &config, &budget, ReadKind::Range, deadline).await?;
    let range_read_seconds = range_start.elapsed().as_secs_f64();
    if survivors.records != config.objects / 2 || ranges.records != survivors.records {
        return Err("survivor/range verification count mismatch".into());
    }
    Ok(Online {
        publication_latency,
        publication_seconds,
        artifact_count,
        packed_records,
        dedicated_records,
        verified,
        workload_seconds,
        overwritten: overwrite_latency.count,
        deleted: removed,
        overwrite_seconds,
        delete_seconds,
        readback_seconds,
        range_read_seconds,
        survivor_verified: survivors.records,
        ranges,
        collection_seconds,
        cleanup_seconds,
        collection_check,
        stats,
    })
}

fn limits(config: &Config) -> snapshot::Limits {
    let database = (config.objects as u64 * 256 * 1024).clamp(16 * 1024 * 1024, 1024 * 1024 * 1024);
    let catalog = 16 * 1024 * 1024;
    snapshot::Limits {
        max_artifacts: 2 * config.objects,
        max_records: config.objects,
        max_database_bytes: database,
        max_catalog_bytes: catalog,
        max_total_bytes: physical_budget(config).limit_bytes
            + database
            + catalog
            + (2 * config.objects as u64 + 3) * 16 * 1024
            + 2 * 1024 * 1024,
    }
}

fn snapshot_error(error: model::Error) -> model::Error {
    // The 4B component predates the driver's typed deadline. Translate only its exact
    // cooperative deadline cause; an I/O failure near the deadline remains a real failure.
    if error.to_string() == "snapshot deadline reached" {
        super::Deadline.into()
    } else {
        error
    }
}

async fn reopen_verified(
    node: Arc<Node>,
    config: &Config,
    budget: &Budget,
    original_generation: &cairn_types::storage::StorageToken,
    deadline: Instant,
) -> Result<usize> {
    check_deadline(deadline)?;
    let store = Store::open(node, physical_budget(config))?;
    let result = async {
        if store.generation() == original_generation {
            return Err("reopen/restore reused the old process generation".into());
        }
        let verified = verify_survivors(&store, config, budget, ReadKind::Whole, deadline).await?;
        check_stats(store.stats().await?, config)?;
        if verified.records != config.objects / 2 {
            return Err("reopened survivor count mismatch".into());
        }
        Ok(verified.records)
    }
    .await;
    let closed = store.close().await;
    closed?;
    result
}

fn peak_rss_kib() -> Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .filter(|value| *value > 0)
        .ok_or_else(|| "kernel peak RSS is unavailable".into())
}

pub(super) async fn run(mut config: Config) -> Result<Report> {
    config.validate()?;
    let deadline = Instant::now() + Duration::from_secs(config.deadline_seconds);
    // The wrapper is fresh and owned. Source, snapshot and restored directories are siblings
    // underneath it, so snapshot's non-overlap rule and coordinator teardown both hold.
    let wrapper = Node::create(&config.root)?;
    let snapshot_path = config.root.join("snapshot");
    let restored_path = config.root.join("restored");
    config.root = config.root.join("node");
    let node = Node::create(&config.root)?;
    let store = Store::open(node.clone(), physical_budget(&config))?;
    let generation = store.generation().clone();
    let budget = Arc::new(Budget::new(node.clone()));
    let config = Arc::new(config);
    let result = online(&store, config.clone(), budget.clone(), deadline).await;
    let snapshot_start = Instant::now();
    let closed = store.close().await;
    closed?;
    let online = result?;
    drop(store);
    if budget.counters.bytes.load(Ordering::SeqCst) != 0
        || budget.counters.pending.load(Ordering::SeqCst) != 0
    {
        return Err("actual online owners remained before offline snapshot".into());
    }
    let proof = node.offline()?;
    let snapshot_limits = limits(&config);
    let admission = budget.acquire(SNAPSHOT_RESERVATION, deadline).await?;
    let summary = tokio::task::spawn_blocking({
        let snapshot_path = snapshot_path.clone();
        let wrapper = wrapper.clone();
        move || {
            let _admission = admission;
            let _wrapper = wrapper;
            snapshot::create(proof, &snapshot_path, snapshot_limits, deadline.into_std())
                .map_err(snapshot_error)
        }
    })
    .await??;
    let snapshot_seconds = snapshot_start.elapsed().as_secs_f64();
    if summary.records != config.objects / 2
        || summary.artifact_bytes != online.stats.physical_charge
    {
        return Err("snapshot survivor bytes differ from settled physical accounting".into());
    }
    let restore_start = Instant::now();
    let admission = budget.acquire(SNAPSHOT_RESERVATION, deadline).await?;
    let (restored, restored_summary) = tokio::task::spawn_blocking({
        let wrapper = wrapper.clone();
        move || -> Result<_> {
            let _admission = admission;
            let _wrapper = wrapper;
            check_deadline(deadline)?;
            let restored = Node::create(&restored_path)?;
            let summary = snapshot::restore(
                &snapshot_path,
                restored.clone(),
                snapshot_limits,
                deadline.into_std(),
            )
            .map_err(snapshot_error)?;
            Ok((restored, summary))
        }
    })
    .await??;
    if restored_summary.records != summary.records
        || restored_summary.artifact_bytes != summary.artifact_bytes
    {
        return Err("restored snapshot summary differs from source".into());
    }
    let restored_verified =
        reopen_verified(restored, &config, &budget, &generation, deadline).await?;
    let restore_seconds = restore_start.elapsed().as_secs_f64();
    let reopen_start = Instant::now();
    let reopened_verified = reopen_verified(node, &config, &budget, &generation, deadline).await?;
    let reopen_seconds = reopen_start.elapsed().as_secs_f64();
    let counters = &budget.counters;
    if counters.bytes.load(Ordering::SeqCst) != 0 || counters.pending.load(Ordering::SeqCst) != 0 {
        return Err("measurement left live admission owners".into());
    }
    wrapper.validate()?;
    Ok(Report {
        status: "PASS",
        mode: config.mode,
        size: config.size,
        objects: config.objects,
        published: online.publication_latency.count,
        verified: online.verified,
        artifact_count: online.artifact_count,
        packed_records: online.packed_records,
        dedicated_records: online.dedicated_records,
        peak_admitted_bytes: counters.peak_bytes.load(Ordering::SeqCst),
        peak_pending_records: counters.peak_pending.load(Ordering::SeqCst),
        publication_seconds: online.publication_seconds,
        publication_latency: online.publication_latency,
        admission_limit_bytes: ADMISSION_BYTES,
        pending_limit_records: model::MAX_RECORDS,
        timing_scope: "raw fixture generation/admission/durable publication; primary includes append/overwrite/delete/collection/cleanup; readback/ranges/snapshot/restore/reopen separate; no S3 or encoding",
        collection_check: online.collection_check,
        measurement: Some(Metrics {
            measurement_protocol: 1,
            workload_seconds: online.workload_seconds,
            overwritten: online.overwritten,
            deleted: online.deleted,
            final_records: online.stats.records,
            survivor_verified: online.survivor_verified,
            restored_verified,
            reopened_verified,
            overwrite_seconds: online.overwrite_seconds,
            delete_seconds: online.delete_seconds,
            readback_seconds: online.readback_seconds,
            range_read_seconds: online.range_read_seconds,
            range_read_count: online.ranges.records,
            range_read_bytes: online.ranges.bytes,
            collection_seconds: online.collection_seconds,
            cleanup_seconds: online.cleanup_seconds,
            snapshot_seconds,
            restore_seconds,
            reopen_seconds,
            pending_writes: online.stats.pending,
            cleanup_debts: online.stats.cleanup,
            live_physical_bytes: summary.artifact_bytes,
            peak_rss_kib: peak_rss_kib()?,
            snapshot_artifacts: summary.artifacts,
            snapshot_database_bytes: summary.database_bytes,
            snapshot_catalog_bytes: summary.catalog_bytes,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::super::{Mode, RecordMetadata};
    use super::*;
    use cairn_types::storage::StorageToken;
    use std::path::PathBuf;

    fn config(root: PathBuf) -> Config {
        Config {
            root,
            mode: Mode::Packed,
            size: 1024,
            objects: 16,
            concurrency: 4,
            seed: 0x5eed,
            known_length: true,
            measurement: true,
            deadline_seconds: 30,
        }
    }

    #[test]
    fn measurement_is_explicit_bounded_and_initial_order_is_a_fixed_bijection() {
        let legacy: Config = serde_json::from_value(serde_json::json!({
            "root": "/fresh", "mode": "packed", "size": 1024, "objects": 16,
            "concurrency": 4, "seed": 24301, "known_length": true, "deadline_seconds": 30,
        }))
        .unwrap();
        assert!(!legacy.measurement);
        legacy.validate().unwrap();
        let mut config = config(PathBuf::from("/fresh"));
        for objects in [0, 1, 3, 6, 8196, 16_384] {
            config.objects = objects;
            assert!(config.validate().is_err(), "objects={objects}");
        }
        for objects in [4, 12, 256, 8192] {
            config.objects = objects;
            config.validate().unwrap();
            let mut indices: Vec<_> = (0..objects).map(|i| initial_index(i, objects)).collect();
            assert_eq!(
                &indices[..4],
                &[0, objects / 4, objects / 2, 3 * objects / 4]
            );
            indices.sort_unstable();
            assert_eq!(indices, (0..objects).collect::<Vec<_>>());
            assert_eq!(
                (0..objects).filter(|&i| deleted(i, objects)).count(),
                objects / 2
            );
            assert_eq!(
                (0..objects)
                    .filter(|&i| survivor_fixture(i, objects) >= objects)
                    .count(),
                objects / 4
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tiny_churn_files_packed_unknown_and_large_fallback_restore_exact_survivors() {
        let temporary = tempfile::tempdir().unwrap();
        for (index, (mode, known_length, size)) in [
            (Mode::Files, true, 1024),
            (Mode::Packed, true, 1024),
            (Mode::Packed, false, 1024),
            (Mode::Packed, true, 1024 * 1024 + 1),
        ]
        .into_iter()
        .enumerate()
        {
            let root = temporary.path().join(format!("measurement-{index}"));
            let mut config = config(root.clone());
            config.mode = mode;
            config.known_length = known_length;
            config.size = size;
            if size > 1024 {
                config.objects = 4;
            }
            let objects = config.objects;
            let report = super::super::run(config).await.unwrap();
            let metrics = report.measurement.as_ref().unwrap();
            assert_eq!(report.published, objects);
            assert_eq!(report.verified, objects);
            assert_eq!(report.packed_records + report.dedicated_records, objects);
            assert_eq!(metrics.overwritten, objects / 4);
            assert_eq!(metrics.deleted, objects / 2);
            assert_eq!(metrics.survivor_verified, objects / 2);
            assert_eq!(metrics.restored_verified, objects / 2);
            assert_eq!(metrics.reopened_verified, objects / 2);
            assert_eq!(metrics.range_read_count, objects / 2);
            assert_eq!(metrics.range_read_bytes, (objects / 2 * 257) as u64);
            assert_eq!(metrics.final_records, (objects / 2) as u64);
            assert_eq!(metrics.pending_writes, 0);
            assert_eq!(metrics.cleanup_debts, 0);
            assert!(metrics.live_physical_bytes >= (objects / 2 * size) as u64);
            assert!(metrics.peak_rss_kib > 0);
            assert!(report.collection_check.completed);
            assert!(report.peak_admitted_bytes <= ADMISSION_BYTES);
            assert!(report.peak_pending_records <= 4);
            assert!(
                metrics.workload_seconds
                    >= report.publication_seconds
                        + metrics.overwrite_seconds
                        + metrics.delete_seconds
                        + metrics.collection_seconds
                        + metrics.cleanup_seconds
            );
            for seconds in [
                metrics.readback_seconds,
                metrics.range_read_seconds,
                metrics.snapshot_seconds,
                metrics.restore_seconds,
                metrics.reopen_seconds,
            ] {
                assert!(seconds.is_finite() && seconds > 0.0);
            }
            let value = serde_json::to_value(&report).unwrap();
            assert_eq!(value["measurement_protocol"], 1);
            assert!(value.get("measurement").is_none());
            assert!(value.get("p99_seconds").is_none());
            // Every offline proof and actual descriptor is released before returning the report.
            let source = Node::open(&root.join("node")).unwrap();
            let restored = Node::open(&root.join("restored")).unwrap();
            assert!(root.join("snapshot/manifest.json").is_file());
            drop((source, restored));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn survivor_verifier_refuses_resurrection_wrong_fixture_and_metadata() {
        let temporary = tempfile::tempdir().unwrap();
        let config = Arc::new(config(temporary.path().join("node")));
        let node = Node::create(&config.root).unwrap();
        let store = Store::open(node.clone(), physical_budget(&config)).unwrap();
        let budget = Arc::new(Budget::new(node));
        let deadline = Instant::now() + Duration::from_secs(20);
        publish_phase(
            PublicationStage::Append,
            store.clone(),
            config.clone(),
            budget.clone(),
            deadline,
        )
        .await
        .unwrap();
        // Original first-quarter bytes are not the expected acknowledged overwrite fixture.
        let error = verify_survivors(&store, &config, &budget, ReadKind::Whole, deadline)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("fixture byte mismatch"));
        publish_phase(
            PublicationStage::Overwrite,
            store.clone(),
            config.clone(),
            budget.clone(),
            deadline,
        )
        .await
        .unwrap();
        // The disjoint deletion interval still exists, even though surviving bytes now match.
        let error = verify_survivors(&store, &config, &budget, ReadKind::Whole, deadline)
            .await
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("permanently deleted key reappeared")
        );
        delete_half(store.clone(), config.clone(), budget.clone(), deadline)
            .await
            .unwrap();
        assert_eq!(
            verify_survivors(&store, &config, &budget, ReadKind::Whole, deadline)
                .await
                .unwrap()
                .records,
            8
        );
        // An otherwise valid publication with a protected metadata bit must fail the fixture.
        let prior = store.lookup(&key(0)).await.unwrap().unwrap();
        let mut bytes = vec![0; config.size];
        Fixture::new(config.seed, config.objects, config.size)
            .read_exact(&mut bytes)
            .unwrap();
        let admission = store
            .plan(model::ArtifactKind::File, config.size as u64)
            .await
            .unwrap();
        let artifact =
            super::super::record::publish_file(&config.root, admission, &mut bytes.as_slice())
                .unwrap();
        let span = &artifact.spans()[0];
        let identity = artifact.plan().artifact().clone();
        let replacement = model::PublishRecord {
            metadata: RecordMetadata {
                row_id: StorageToken::generate(),
                locked: true,
                encoded_sha256: span.sha256,
                ..prior.metadata.clone()
            },
            location: model::Location::File {
                artifact: identity,
                length: span.length,
            },
            expected: model::ExpectedCurrent::Exact {
                row_id: prior.metadata.row_id,
                location: prior.location,
            },
            preserve_previous: false,
        };
        assert!(matches!(
            store.publish(artifact, vec![replacement]).await.unwrap(),
            super::super::store::PublicationOutcome::Applied { .. }
        ));
        let error = verify_survivors(&store, &config, &budget, ReadKind::Range, deadline)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("metadata differs"));
        store.close().await.unwrap();
    }

    #[test]
    fn only_the_snapshot_deadline_cause_is_translated() {
        assert!(snapshot_error("snapshot deadline reached".into()).is::<super::super::Deadline>());
        assert!(
            !snapshot_error("snapshot copy failed near deadline".into())
                .is::<super::super::Deadline>()
        );
    }
}
