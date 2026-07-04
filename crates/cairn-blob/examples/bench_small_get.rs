//! Small-object GET fast-path A/B micro-benchmark, isolating the blob read path from the
//! HTTP/auth/network layer. For each object size it stages one uncompressed blob, then serves it
//! repeatedly under concurrency two ways against the SAME file on disk:
//!
//! * FAST — the small-object fast path: one probe `open` reads the whole blob and serves it as a
//!   single `Bytes` (no second open, no I/O permit, no per-chunk streaming channel).
//! * STREAM — the general streamed read: probe `open` + a second `open` inside the spawned read
//!   task feeding an `mpsc`-backed body, holding a read I/O permit for the transfer.
//!
//! The STREAM leg is forced on every size via `with_small_read_max(0)`, so the two legs differ only
//! by the read path — same bytes, same page cache, same disk — which is exactly the cost the fast
//! path removes for a tiny GET (the one warp scenario Cairn loses, small-object GET). Ratio > 1 in
//! the RESULT column means the fast path serves that size that many times faster in-process.
//!
//! Run: `cargo run --release --example bench_small_get -p cairn-blob`
//! Knobs: `BENCH_CONC` (concurrent readers, default 32), `BENCH_SECS` (seconds/leg, default 3).

use bytes::Bytes;
use cairn_blob::LocalBlobStore;
use cairn_types::traits::BlobStore;
use cairn_types::{BodyStream, BucketName, CompressionDescriptor, StageOptions, StoragePath};
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn body(data: Vec<u8>) -> BodyStream {
    Box::pin(futures_util::stream::once(
        async move { Ok(Bytes::from(data)) },
    ))
}

/// Drive `store` at `conc` concurrent readers draining the whole body of `path` for `secs`,
/// returning completed GETs/second. Draining the body is the real hot path: warp's pooled clients
/// engage kernel sendfile ~0% of the time, so a small GET is served through the streaming body.
async fn bench_reads(
    store: Arc<LocalBlobStore>,
    path: StoragePath,
    compression: CompressionDescriptor,
    conc: usize,
    secs: f64,
) -> f64 {
    let deadline = Instant::now() + Duration::from_secs_f64(secs);
    let count = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..conc {
        let store = store.clone();
        let path = path.clone();
        let compression = compression.clone();
        let count = count.clone();
        handles.push(tokio::spawn(async move {
            while Instant::now() < deadline {
                let handle = store.open(&path, None, &compression).await.unwrap();
                let mut body = handle.body;
                let mut n = 0usize;
                while let Some(chunk) = body.next().await {
                    n += chunk.unwrap().len();
                }
                std::hint::black_box(n);
                count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    let t0 = Instant::now();
    for h in handles {
        h.await.unwrap();
    }
    count.load(Ordering::Relaxed) as f64 / t0.elapsed().as_secs_f64()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let conc: usize = std::env::var("BENCH_CONC")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32);
    let secs: f64 = std::env::var("BENCH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3.0);

    // tempdir on the cwd's filesystem (run from the repo on real disk, not /tmp tmpfs).
    let dir = tempfile::tempdir_in(".").unwrap();
    // Default store takes the fast path up to 256 KiB; the `slow` store forces the streamed read on
    // every size, so the two read the identical committed file and differ only by the read path.
    let fast = Arc::new(LocalBlobStore::open(dir.path()).await.unwrap());
    let slow = Arc::new(
        LocalBlobStore::open(dir.path())
            .await
            .unwrap()
            .with_small_read_max(0),
    );
    let bkt = BucketName::parse("bench").unwrap();

    println!(
        "small-object GET fast path: {conc} concurrent readers, {secs}s/leg, on {:?}\n",
        std::env::current_dir().unwrap()
    );
    println!(
        "  {:>9}   {:>13}   {:>13}   {:>7}",
        "size", "FAST get/s", "STREAM get/s", "speedup"
    );

    for &size in &[1usize, 4, 16, 64, 256] {
        let bytes = size * 1024;
        let staged = fast
            .stage(
                &bkt,
                body(vec![0x5a; bytes]),
                StageOptions {
                    compression: None, // uncompressed: the only path with a fast/stream fork
                    size_ceiling: 8 * 1024 * 1024,
                    content_type: "application/octet-stream".to_owned(),
                    ..StageOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            staged.compression,
            CompressionDescriptor::Uncompressed
        ));

        let f = bench_reads(
            fast.clone(),
            staged.storage_path.clone(),
            staged.compression.clone(),
            conc,
            secs,
        )
        .await;
        let s = bench_reads(
            slow.clone(),
            staged.storage_path.clone(),
            staged.compression.clone(),
            conc,
            secs,
        )
        .await;
        println!(
            "  {:>7} KiB   {f:>13.0}   {s:>13.0}   {:>6.2}x",
            size,
            f / s
        );
    }
}
