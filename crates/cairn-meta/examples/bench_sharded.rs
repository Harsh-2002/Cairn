//! Object-write throughput vs shard count (Phase 3.2). Spreads concurrent `PutObjectVersion`s
//! across many buckets through the `ShardedMetadataStore` and measures puts/s for N = 1, 2, 4
//! shards on real on-disk databases. The full object-write path is exercised (routing → quota
//! check → upsert → roll-up counters). Run from the repo (ext4, not /tmp tmpfs):
//!   `cargo run --release --example bench_sharded -p cairn-meta`
//! Env: BENCH_CONC (submitters), BENCH_SECS, BENCH_BUCKETS, BENCH_SHARDS (comma list).

use cairn_meta::{OpenOptions, ShardedMetadataStore, open};
use cairn_types::authz::OwnershipMode;
use cairn_types::bucket::{Bucket, VersioningState};
use cairn_types::id::{BucketName, ObjectKey, UserId, VersionId};
use cairn_types::meta::{Mutation, Precondition};
use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::time::Timestamp;
use cairn_types::traits::MetadataStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn row(bucket: &str, key: &str, t: usize) -> ObjectVersionRow {
    let b = BucketName::parse(bucket).unwrap();
    ObjectVersionRow {
        id: format!("{bucket}-{key}-{t}"),
        bucket: b,
        key: ObjectKey::parse(key).unwrap(),
        version_id: VersionId::from_string(format!("v{t}")),
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
        storage_path: Some(cairn_types::id::StoragePath::from_string(format!(
            "{bucket}/{key}-{t}"
        ))),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId("owner".to_owned()),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: None,
        created_at: Timestamp(1),
        updated_at: Timestamp(1),
    }
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
        handles.push(tokio::spawn(async move {
            let mut i = task;
            while Instant::now() < deadline {
                // Distinct (bucket, key) per write so each is an insert; buckets chosen so writes
                // spread across shards.
                let b = format!("bucket-{}", i % buckets);
                let k = format!("k{task}-{i}");
                if store
                    .submit(Mutation::PutObjectVersion {
                        row: Box::new(row(&b, &k, i)),
                        precondition: Precondition::default(),
                        replication: Vec::new(),
                    })
                    .await
                    .is_ok()
                {
                    count.fetch_add(1, Ordering::Relaxed);
                }
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
        "sharded object-write throughput: {conc} submitters, {buckets} buckets, {secs}s each, on {:?} ({} cores)",
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
            "  shards={shards:<2}  {rate:>10.0} puts/s   ({speedup:.2}x vs shards={})",
            shard_list[0]
        );
    }
}
