//! Protocol-2 end-to-end metadata admission/publication cost vs shard count (Phase 3.2).
//! Spreads concurrent object inserts across buckets through `ShardedMetadataStore`, measuring
//! acknowledged admission + publication pairs for N = 1, 2, 4 shards on on-disk SQLite databases.
//! Includes routing, exact intent/path ownership, quota checks, upsert, roll-ups and cleanup-debt
//! recording. Excludes blob I/O and cleanup draining; this example creates metadata fixtures only.
//! Uses `OpenOptions::default()` (synchronous=NORMAL). These results are not comparable to the old
//! bare-`PutObjectVersion` throughput numbers. Run from the repo (ext4, not /tmp tmpfs):
//!   `cargo run --release --example bench_sharded -p cairn-meta`
//! Env: BENCH_CONC (submitters), BENCH_SECS, BENCH_BUCKETS, BENCH_SHARDS (comma list).

use cairn_meta::{OpenOptions, ShardedMetadataStore, open};
use cairn_types::authz::OwnershipMode;
use cairn_types::bucket::{Bucket, VersioningState};
use cairn_types::id::{BucketName, ObjectKey, UserId, VersionId};
use cairn_types::meta::{InitialObjectState, Mutation, MutationOutcome, Precondition};
use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::storage::{
    PlannedStorageWrite, StorageAdmission, StorageMutation, StorageToken, StorageWritePlan,
    StorageWriteTarget,
};
use cairn_types::time::Timestamp;
use cairn_types::traits::MetadataStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn row(
    bucket: &str,
    key: &str,
    t: usize,
    generation: &StorageToken,
) -> (ObjectVersionRow, StorageWritePlan) {
    let bucket = BucketName::parse(bucket).unwrap();
    let key = ObjectKey::parse(key).unwrap();
    let version_id = VersionId::from_string(format!("v{t}"));
    let id = StorageToken::generate().as_str().to_owned();
    let plan = PlannedStorageWrite::new(
        bucket.clone(),
        generation.clone(),
        StorageWriteTarget::Object {
            key: key.clone(),
            version_id: version_id.clone(),
            row_id: id.clone(),
        },
    )
    .unwrap()
    .plan()
    .clone();
    let row = ObjectVersionRow {
        id,
        bucket,
        key,
        version_id,
        is_latest: true,
        is_delete_marker: false,
        size_logical: 1024,
        size_physical: 1024,
        etag: ETag::from_string("e".to_owned()),
        content_type: "application/octet-stream".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(plan.final_path().unwrap().clone()),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId("owner".to_owned()),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: None,
        internal_sha256: None,
        replicated_at: None,
        created_at: Timestamp(1),
        updated_at: Timestamp(1),
    };
    (row, plan)
}

async fn bench(shards: usize, buckets: usize, conc: usize, secs: f64) -> f64 {
    let dir = tempfile::tempdir_in(".").unwrap();
    let opts = OpenOptions::default();
    let stores: Vec<Arc<dyn MetadataStore>> = (0..shards)
        .map(|i| {
            let p = dir.path().join(format!("shard{i}.db"));
            Arc::new(open(&p, &opts).unwrap()) as Arc<dyn MetadataStore>
        })
        .collect();
    let store: Arc<dyn MetadataStore> = Arc::new(ShardedMetadataStore::new(stores));

    // One broadcast establishes the same current generation on every shard before admission.
    // The databases are fresh, privately owned metadata fixtures; there is no prior backend I/O.
    let generation = StorageToken::generate();
    assert_eq!(
        store
            .submit(Mutation::BeginStorageGeneration {
                generation: generation.clone(),
            })
            .await
            .unwrap(),
        MutationOutcome::Ack,
    );

    // Pre-create the buckets (spread across shards by name).
    for b in 0..buckets {
        store
            .submit(Mutation::CreateBucket(Box::new(Bucket {
                name: BucketName::parse(&format!("bucket-{b}")).unwrap(),
                owner_id: UserId("owner".to_owned()),
                created_at: Timestamp(1),
                versioning: VersioningState::Enabled,
                ownership_mode: OwnershipMode::BucketOwnerEnforced,
                region: "us-east-1".to_owned(),
                compression: None,
            })))
            .await
            .unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs_f64(secs);
    let count = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for task in 0..conc {
        let store = store.clone();
        let count = count.clone();
        let generation = generation.clone();
        handles.push(tokio::spawn(async move {
            let mut i = task;
            while Instant::now() < deadline {
                // Distinct (bucket, key) per write so each is an insert; buckets chosen so writes
                // spread across shards.
                let b = format!("bucket-{}", i % buckets);
                let k = format!("k{task}-{i}");
                let (row, plan) = row(&b, &k, i, &generation);
                let admission = store
                    .submit(Mutation::Storage {
                        bucket: row.bucket.clone(),
                        operation: StorageMutation::Reserve {
                            plan: Box::new(plan.clone()),
                            now: Timestamp(1),
                        },
                    })
                    .await
                    .expect("protocol-2 metadata admission failed");
                assert!(matches!(
                    admission,
                    MutationOutcome::StorageAdmission(StorageAdmission::Granted(admitted))
                        if *admitted == plan
                ));
                let version_id = row.version_id.clone();
                let published = store
                    .submit(Mutation::PublishStorageWrite {
                        plan: Box::new(plan),
                        operation: Box::new(Mutation::PutObjectVersion {
                            row: Box::new(row),
                            precondition: Precondition::default(),
                            initial_state: InitialObjectState::default(),
                            replication: Vec::new(),
                        }),
                    })
                    .await
                    .expect("protocol-2 metadata publication failed");
                assert_eq!(
                    published,
                    MutationOutcome::Put {
                        superseded: None,
                        version_id
                    }
                );
                count.fetch_add(1, Ordering::Relaxed);
                i += conc;
            }
        }));
    }
    let t0 = Instant::now();
    for h in handles {
        h.await.unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    count.load(Ordering::Relaxed) as f64 / dt
}

#[tokio::main]
async fn main() {
    let conc: usize = std::env::var("BENCH_CONC")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let secs: f64 = std::env::var("BENCH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5.0);
    let buckets: usize = std::env::var("BENCH_BUCKETS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let shard_list: Vec<usize> = std::env::var("BENCH_SHARDS")
        .ok()
        .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1, 2, 4]);

    println!(
        "protocol-2 metadata admission+publication throughput: {conc} submitters, {buckets} buckets, {secs}s each, on {:?} ({} cores)",
        std::env::current_dir().unwrap(),
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
    );
    let mut base = 0.0;
    for (idx, &shards) in shard_list.iter().enumerate() {
        let rate = bench(shards, buckets, conc, secs).await;
        if idx == 0 {
            base = rate;
        }
        let speedup = if base > 0.0 { rate / base } else { 1.0 };
        println!(
            "  shards={shards:<2}  {rate:>10.0} admitted publications/s   ({speedup:.2}x vs shards={})",
            shard_list[0]
        );
    }
}
