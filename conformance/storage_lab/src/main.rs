//! Bounded layer drivers. These call the real blob store and canonical SQLite Writer/WAL pool.
mod common;
use bytes::Bytes;
use cairn_blob::LocalBlobStore;
use cairn_types::storage::{
    PlannedStorageWrite, StorageAdmission, StorageMutation, StorageToken, StorageWriteTarget,
};
use cairn_types::testing::{FixtureBlobStore, fixture_storage_cleanup, fixture_storage_io};
use cairn_types::traits::{BlobStore, MetadataStore};
use cairn_types::*;
use common::{distribution, emit, payload};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    root: PathBuf,
    layer: String,
    concurrency: usize,
    buckets: usize,
    size: usize,
    seed: u64,
    seconds: u64,
    idle: u64,
    cycles: usize,
    max_ops: u64,
}

fn row(
    bucket: &BucketName,
    key: ObjectKey,
    id: String,
    size: usize,
    path: StoragePath,
) -> ObjectVersionRow {
    ObjectVersionRow {
        id,
        bucket: bucket.clone(),
        key,
        version_id: VersionId::null(),
        is_latest: true,
        is_delete_marker: false,
        size_logical: size as u64,
        size_physical: size as u64,
        etag: ETag::from_string("lab-etag".into()),
        content_type: "application/octet-stream".into(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(path),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId("lab-owner".into()),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: None,
        internal_sha256: None,
        replicated_at: None,
        created_at: Timestamp(1),
        updated_at: Timestamp(1),
    }
}

async fn blob_operation(
    store: &LocalBlobStore,
    bucket: &BucketName,
    data: Bytes,
) -> Result<[f64; 3], Error> {
    let start = Instant::now();
    let sent = data.clone();
    let staged = store
        .stage_fixture(
            bucket,
            Box::pin(futures_util::stream::once(async move { Ok(sent) })),
            StageOptions {
                size_ceiling: data.len() as u64,
                content_type: "application/octet-stream".into(),
                ..Default::default()
            },
        )
        .await?;
    let stage = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let mut body = store
        .open_raw(
            &staged.storage_path,
            None,
            BlobCipher::KnownPlaintext,
            &staged.compression,
            staged.size_logical,
        )
        .await?
        .body;
    let mut offset = 0;
    while let Some(frame) = body.next().await {
        let frame = frame?;
        if data.get(offset..offset + frame.len()) != Some(frame.as_ref()) {
            return Err("blob checksum/length mismatch".into());
        }
        offset += frame.len();
    }
    if offset != data.len() {
        return Err("short blob read".into());
    }
    let read = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let (cleanup, lease) = fixture_storage_cleanup(bucket.clone(), staged.storage_path);
    store.cleanup_storage(&cleanup, lease).await?;
    Ok([stage, read, start.elapsed().as_secs_f64()])
}

async fn meta_operation(
    store: &cairn_meta::SqliteMetadataStore,
    generation: &StorageToken,
    bucket: &BucketName,
    worker: usize,
    sequence: u64,
    size: usize,
) -> Result<[f64; 3], Error> {
    // Each worker owns a bounded 16-key overwrite ring. The immutable row identity changes.
    let key = ObjectKey::parse(&format!("worker-{worker:04}/key-{:02}", sequence % 16))?;
    let id = StorageToken::generate().as_str().to_owned();
    let start = Instant::now();
    let planned = PlannedStorageWrite::new(
        bucket.clone(),
        generation.clone(),
        StorageWriteTarget::Object {
            key: key.clone(),
            version_id: VersionId::null(),
            row_id: id.clone(),
        },
    )?;
    let plan = planned.plan().clone();
    let version = row(
        bucket,
        key.clone(),
        id.clone(),
        size,
        plan.final_path()?.clone(),
    );
    let admission = store
        .submit(Mutation::Storage {
            bucket: bucket.clone(),
            operation: StorageMutation::Reserve {
                plan: Box::new(plan.clone()),
                now: Timestamp(1),
            },
        })
        .await?;
    if !matches!(admission, MutationOutcome::StorageAdmission(StorageAdmission::Granted(ref admitted)) if **admitted == plan)
    {
        return Err("metadata storage admission was not granted".into());
    }
    let outcome = store
        .submit(Mutation::PublishStorageWrite {
            plan: Box::new(plan),
            operation: Box::new(Mutation::PutObjectVersion {
                row: Box::new(version),
                precondition: Precondition::default(),
                initial_state: InitialObjectState::default(),
                replication: Vec::new(),
            }),
        })
        .await?;
    if !matches!(outcome, MutationOutcome::Put { .. }) {
        return Err("metadata publication did not apply".into());
    }
    let put = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let found = store
        .current_version(bucket, &key)
        .await?
        .ok_or("missing acknowledged metadata row")?;
    if found.id != id || found.size_logical != size as u64 {
        return Err("metadata row mismatch".into());
    }
    let read = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let page = store
        .list_current(
            bucket,
            &ListQuery {
                prefix: Some(format!("worker-{worker:04}/")),
                limit: 16,
                ..Default::default()
            },
        )
        .await?;
    if !page.items.iter().any(|item| item.key == key) {
        return Err("listing omitted acknowledged row".into());
    }
    Ok([put, read, start.elapsed().as_secs_f64()])
}

async fn run(config: Config) -> Result<(), Error> {
    if !["blob", "meta"].contains(&config.layer.as_str())
        || config.concurrency == 0
        || config.concurrency > 128
        || config.buckets == 0
        || config.buckets > 128
        || config.size == 0
        || config.size > 1024 * 1024
        || config.cycles != 3
        || config.seconds == 0
        || config.seconds > 60
        || config.idle > 30
        || config.max_ops < 3
        || config.max_ops > 300_000
    {
        return Err("invalid bounded laboratory configuration".into());
    }
    std::fs::create_dir(&config.root)?;
    let buckets = (0..config.buckets)
        .map(|n| BucketName::parse(&format!("lab-{n:04}")))
        .collect::<Result<Vec<_>, _>>()?;
    let blob = if config.layer == "blob" {
        Some(Arc::new(
            LocalBlobStore::open(&config.root, fixture_storage_io()).await?,
        ))
    } else {
        None
    };
    let generation = StorageToken::generate();
    let meta = if config.layer == "meta" {
        let store = Arc::new(cairn_meta::open(
            &config.root.join("metadata.db"),
            &cairn_meta::OpenOptions {
                synchronous_full: true,
                read_pool_size: 8,
                cache_size: -8192,
                mmap_bytes: 0,
                ..Default::default()
            },
        )?);
        if store
            .submit(Mutation::BeginStorageGeneration {
                generation: generation.clone(),
            })
            .await?
            != MutationOutcome::Ack
        {
            return Err("metadata storage generation was not initialized".into());
        }
        for name in &buckets {
            store
                .submit(Mutation::CreateBucket(Box::new(Bucket {
                    name: name.clone(),
                    owner_id: UserId("lab-owner".into()),
                    created_at: Timestamp(1),
                    versioning: VersioningState::Unversioned,
                    ownership_mode: OwnershipMode::BucketOwnerEnforced,
                    region: "us-east-1".into(),
                    compression: None,
                })))
                .await?;
        }
        Some(store)
    } else {
        None
    };
    emit(
        json!({"kind": "prepared", "layer": config.layer, "metadata": {"synchronous": "FULL", "read_pool": 8, "cache_bytes_per_connection": 8388608, "mmap_bytes": 0},
        "publication_variant": publication_variant(&config.layer),
        "metadata_cleanup": (config.layer == "meta").then_some("durable_debt_retained_for_teardown_no_physical_files"),
        "unavailable": ["application_cache_live_bytes", "blob_internal_stage_timings", "runtime_active_tasks", "in_flight_buffer_bytes"]}),
    );
    let data = Bytes::from(payload(config.size, config.seed));
    let buckets = Arc::new(buckets);
    for cycle in 0..config.cycles {
        let start = Instant::now();
        let deadline = start + Duration::from_secs(config.seconds);
        let admitted = Arc::new(AtomicU64::new(0));
        let cap = config.max_ops / config.cycles as u64;
        let mut tasks = tokio::task::JoinSet::new();
        emit(json!({"kind": "phase", "cycle": cycle, "phase": "load"}));
        for worker in 0..config.concurrency {
            let (blob, meta, buckets, data, admitted) = (
                blob.clone(),
                meta.clone(),
                buckets.clone(),
                data.clone(),
                admitted.clone(),
            );
            let size = config.size;
            let generation = generation.clone();
            tasks.spawn(async move {
                let mut samples = [Vec::new(), Vec::new(), Vec::new()];
                let mut bucket_counts = vec![0_u64; buckets.len()];
                while Instant::now() < deadline {
                    let sequence = admitted.fetch_add(1, Ordering::Relaxed);
                    if sequence >= cap {
                        break;
                    }
                    let bucket_index = sequence as usize % buckets.len();
                    let bucket = &buckets[bucket_index];
                    let elapsed = if let Some(store) = &blob {
                        blob_operation(store, bucket, data.clone()).await?
                    } else {
                        meta_operation(
                            meta.as_ref().ok_or("missing metadata driver")?,
                            &generation,
                            bucket,
                            worker,
                            sequence + cycle as u64 * cap,
                            size,
                        )
                        .await?
                    };
                    for (column, seconds) in samples.iter_mut().zip(elapsed) {
                        column.push(seconds);
                    }
                    bucket_counts[bucket_index] += 1;
                }
                Ok::<_, Error>((samples, bucket_counts))
            });
        }
        let mut all = [Vec::new(), Vec::new(), Vec::new()];
        let mut bucket_counts = vec![0_u64; buckets.len()];
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        while !tasks.is_empty() {
            tokio::select! {
                result = tasks.join_next() => {
                    if let Some(result) = result {
                        let (samples, counts) = result??;
                        for (target, samples) in all.iter_mut().zip(samples) { target.extend(samples); }
                        for (target, count) in bucket_counts.iter_mut().zip(counts) { *target += count; }
                    }
                }
                _ = tick.tick(), if meta.is_some() => {
                    if let Some(store) = &meta {
                        let mut stages: BTreeMap<&str, (u64, f64, f64, u64)> = BTreeMap::new();
                        for sample in store.drain_writer_stage_samples() {
                            let entry = stages.entry(sample.stage).or_default();
                            entry.0 += 1; entry.1 += sample.seconds; entry.2 = entry.2.max(sample.seconds); entry.3 += u64::from(!sample.success);
                        }
                        emit(json!({"kind": "writer", "cycle": cycle, "elapsed": start.elapsed().as_secs_f64(), "stages_count_sum_max_errors": stages,
                            "dropped_samples": store.dropped_writer_stage_samples(), "queue_depth": store.writer_queue_depth()}));
                    }
                }
            }
        }
        let elapsed = start.elapsed().as_secs_f64();
        emit(
            json!({"kind": "cycle", "cycle": cycle, "elapsed": elapsed, "successful_transactions": all[0].len(),
            "operation_names": if config.layer == "blob" { ["stage", "read", "delete"] } else { ["admit_publish", "read", "list"] },
            "publication_variant": publication_variant(&config.layer),
            "bucket_transactions": bucket_counts,
            "transactions_per_second": all[0].len() as f64 / elapsed,
            "operation_cap_reached": admitted.load(Ordering::Relaxed) >= cap,
            "operations": all.iter_mut().map(|samples| distribution(samples)).collect::<Vec<_>>()}),
        );
        emit(json!({"kind": "phase", "cycle": cycle, "phase": "idle"}));
        // Hold the same backend across all three idle periods. Driver latency arrays are
        // released before idle so their retained capacity cannot masquerade as server growth.
        drop(all);
        if let Some(store) = &meta {
            let start = Instant::now();
            let checkpoint = store.checkpoint().await?;
            emit(
                json!({"kind": "checkpoint", "cycle": cycle, "seconds": start.elapsed().as_secs_f64(), "busy": checkpoint.busy,
                "log_frames": checkpoint.log_frames, "checkpointed_frames": checkpoint.checkpointed_frames}),
            );
        }
        tokio::time::sleep(Duration::from_secs(config.idle)).await;
    }
    emit(json!({"kind": "complete", "status": "PASS"}));
    Ok(())
}

fn publication_variant(layer: &str) -> &'static str {
    if layer == "meta" {
        "writer_admission_exact_publication_v2"
    } else {
        "blob_only_fixture_permit_exact_cleanup_no_metadata_v3"
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Error> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("expected coordinator config path")?;
    let config = serde_json::from_slice(&std::fs::read(path)?)?;
    run(config).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deterministic_payload_and_sample_gate() {
        assert_eq!(payload(1024, 0x5eed), payload(1024, 0x5eed));
        assert_ne!(payload(1024, 1), payload(1024, 2));
        assert!(distribution(&mut [1.0, 2.0])["p99_seconds"].is_null());
        assert_eq!(distribution(&mut vec![0.5; 10_000])["p99_seconds"], 0.5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_layer_drivers_verify_small_capped_fixtures() {
        for layer in ["meta", "blob"] {
            let fixture = tempfile::tempdir().unwrap();
            run(Config {
                root: fixture.path().join("data"),
                layer: layer.into(),
                concurrency: 1,
                buckets: 2,
                size: 1024,
                seed: 0x5eed,
                seconds: 1,
                idle: 0,
                cycles: 3,
                max_ops: 6,
            })
            .await
            .unwrap();
            for number in 0..2 {
                let bucket = BucketName::parse(&format!("lab-{number:04}")).unwrap();
                if layer == "blob" {
                    assert!(
                        !fixture.path().join("data").join(bucket.as_str()).exists(),
                        "exact cleanup prunes the empty bucket namespace"
                    );
                } else {
                    let store = cairn_meta::open(
                        &fixture.path().join("data/metadata.db"),
                        &cairn_meta::OpenOptions::default(),
                    )
                    .unwrap();
                    assert!(
                        !store
                            .list_current(
                                &bucket,
                                &ListQuery {
                                    limit: 16,
                                    ..Default::default()
                                }
                            )
                            .await
                            .unwrap()
                            .items
                            .is_empty()
                    );
                }
            }
        }
    }
}
