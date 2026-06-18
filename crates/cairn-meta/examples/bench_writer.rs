//! Direct writer-throughput micro-benchmark, isolating the single group-committing
//! writer + commit + fsync path from the HTTP/auth layer. Compares durability
//! settings (synchronous FULL vs NORMAL) and group-commit linger windows on a real
//! disk. Run: `cargo run --release --example bench_writer -p cairn-meta`.

use cairn_meta::{OpenOptions, open};
use cairn_types::meta::{ActivityEntry, Mutation};
use cairn_types::time::Timestamp;
use cairn_types::traits::MetadataStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

async fn bench(label: &str, opts: OpenOptions, conc: usize, secs: f64) {
    // tempdir on the cwd's filesystem (run from the repo on ext4, not /tmp tmpfs).
    let dir = tempfile::tempdir_in(".").unwrap();
    let db = dir.path().join("bench.db");
    let store = Arc::new(open(&db, &opts).unwrap());
    let deadline = Instant::now() + Duration::from_secs_f64(secs);
    let count = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for t in 0..conc {
        let store = store.clone();
        let count = count.clone();
        handles.push(tokio::spawn(async move {
            let mut i = t as u64;
            while Instant::now() < deadline {
                let e = ActivityEntry {
                    id: format!("{t}-{i}"),
                    action: "bench".to_owned(),
                    bucket: None,
                    key: None,
                    size: None,
                    etag: None,
                    actor: None,
                    at: Timestamp(i as i64),
                };
                if store
                    .submit(Mutation::RecordActivity(Box::new(e)))
                    .await
                    .is_ok()
                {
                    count.fetch_add(1, Ordering::Relaxed);
                }
                i += conc as u64;
            }
        }));
    }
    let t0 = Instant::now();
    for h in handles {
        h.await.unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    let n = count.load(Ordering::Relaxed);
    println!(
        "  {label:34} {:>10.0} commits/s   ({n} in {dt:.1}s)",
        n as f64 / dt
    );
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
    println!(
        "writer throughput: {conc} concurrent submitters, {secs}s each, on {:?}",
        std::env::current_dir().unwrap()
    );

    let full = |linger: Option<Duration>| OpenOptions {
        synchronous_full: true,
        group_commit_linger: linger,
        ..Default::default()
    };
    let normal = |linger: Option<Duration>| OpenOptions {
        synchronous_full: false,
        group_commit_linger: linger,
        ..Default::default()
    };

    bench(
        "FULL,  no linger  (current default)",
        full(None),
        conc,
        secs,
    )
    .await;
    bench(
        "FULL,  300us linger",
        full(Some(Duration::from_micros(300))),
        conc,
        secs,
    )
    .await;
    bench("NORMAL, no linger", normal(None), conc, secs).await;
    bench(
        "NORMAL, 300us linger",
        normal(Some(Duration::from_micros(300))),
        conc,
        secs,
    )
    .await;
    bench(
        "NORMAL, 1ms linger",
        normal(Some(Duration::from_millis(1))),
        conc,
        secs,
    )
    .await;
}
