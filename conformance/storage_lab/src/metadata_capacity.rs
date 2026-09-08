//! Populated canonical-Writer capacity laboratory. No physical object I/O is performed.
use cairn_storage_lab::{metadata_metrics as metrics, metadata_workload as workload};

use cairn_meta::{OpenOptions, SqliteMetadataStore};
use cairn_storage_lab::metadata_run::load;
use metrics::{WriterCpu, WriterObservation};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    root: PathBuf,
    buckets: usize,
    seed_rows: usize,
    seed: u64,
    concurrency: Vec<usize>,
    phase_seconds: u64,
    deadline_seconds: u64,
    ticks_per_second: u64,
}

impl Config {
    fn validate(&self) -> Result<(), Error> {
        if !self.root.is_absolute()
            || self.root.exists()
            || self
                .root
                .parent()
                .ok_or("missing root parent")?
                .canonicalize()?
                != self.root.parent().ok_or("missing root parent")?
            || ![1, 16].contains(&self.buckets)
            || !(100..=100_000).contains(&self.seed_rows)
            || !self.seed_rows.is_multiple_of(10)
            || self.concurrency.is_empty()
            || self.concurrency.len() > 5
            || self.concurrency.iter().any(|c| ![4, 32, 128].contains(c))
            || !(1..=60).contains(&self.phase_seconds)
            || !(1..=600).contains(&self.deadline_seconds)
            || !(1..=100_000).contains(&self.ticks_per_second)
        {
            return Err("invalid bounded metadata-capacity config or existing root".into());
        }
        Ok(())
    }
}

fn emit(value: Value) {
    println!("{value}");
}

fn options() -> OpenOptions {
    OpenOptions {
        synchronous_full: true,
        read_pool_size: 8,
        cache_size: -8192,
        mmap_bytes: 0,
        ..OpenOptions::default()
    }
}

fn wal_bytes(root: &Path) -> Result<u64, Error> {
    match std::fs::metadata(root.join("metadata.sqlite3-wal")) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

async fn periodic_checkpoint(
    store: &SqliteMetadataStore,
    observation: &mut WriterObservation,
) -> Result<(), Error> {
    observation.checkpoint_attempts += 1;
    let checkpoint = store.checkpoint().await?;
    if checkpoint.busy {
        // A live WAL reader may temporarily block truncation. Leave the next observation
        // eligible to retry; the coordinator still enforces the deadline and space ceiling.
        observation.checkpoint_busy += 1;
    } else {
        observation.checkpoint_completed += 1;
    }
    Ok(())
}

/// Observe while the actual operation remains owned. A deadline fails the run; it does not
/// turn an unfinished mutation into a completed latency sample or an acknowledged outcome.
async fn observed<F, T>(
    store: &SqliteMetadataStore,
    root: &Path,
    deadline: Instant,
    future: F,
) -> Result<(T, WriterObservation), Error>
where
    F: std::future::Future<Output = Result<T, String>>,
{
    tokio::pin!(future);
    let mut observation = WriterObservation::start(store);
    let mut deadline_reached = false;
    let mut failure: Option<Error> = None;
    let mut interval = tokio::time::interval(Duration::from_millis(20));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let output = loop {
        tokio::select! {
            biased;
            result = &mut future => break result,
            () = tokio::time::sleep_until(deadline), if !deadline_reached => deadline_reached = true,
            _ = interval.tick() => {
                observation.drain(store);
                let bytes = match wal_bytes(root) {
                    Ok(bytes) => bytes,
                    Err(error) => {failure.get_or_insert(error); 0}
                };
                observation.peak_wal_bytes = observation.peak_wal_bytes.max(bytes);
                // The server owns a periodic checkpoint loop. The lab must explicitly run
                // the same canonical Writer control seam to avoid unbounded WAL growth.
                if !deadline_reached && failure.is_none() && bytes >= 64 * 1024 * 1024
                    && let Err(error) = periodic_checkpoint(store, &mut observation).await {
                    failure = Some(error);
                }
            }
        }
    };
    observation.drain(store);
    observation.peak_wal_bytes = observation.peak_wal_bytes.max(wal_bytes(root)?);
    if deadline_reached || Instant::now() >= deadline {
        return Err("metadata phase deadline expired; admitted owners joined".into());
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok((
        output.map_err(|error| -> Error { error.into() })?,
        observation,
    ))
}

fn database_sizes(path: &Path) -> Result<Value, Error> {
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let (sqlite_version, sqlite_source_id): (String, String) =
        db.query_row("SELECT sqlite_version(), sqlite_source_id()", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
    let page_size: u64 = db.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let page_count: u64 = db.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let freelist: u64 = db.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
    // Aggregate at most schema-cardinality rows. This is outside the request timing window.
    let dbstat = (|| -> rusqlite::Result<Value> {
        let mut stmt = db.prepare("SELECT CASE WHEN type='index' THEN 'index' ELSE 'table' END, sum(s.pgsize) FROM dbstat s LEFT JOIN sqlite_schema m ON m.name=s.name GROUP BY 1")?;
        let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)))?;
        let sizes = rows.collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
        Ok(json!({"available":true,"bytes_by_kind":sizes}))
    })().unwrap_or_else(|error| json!({"available":false,"reason":error.to_string()}));
    Ok(
        json!({"sqlite_version":sqlite_version,"sqlite_source_id":sqlite_source_id,
        "database_file_bytes":std::fs::metadata(path)?.len(), "page_size":page_size,
        "page_count":page_count,"freelist_pages":freelist,"dbstat":dbstat}),
    )
}

fn verify_quota(path: &Path, expected: &workload::ExpectedState) -> Result<Value, Error> {
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut report = serde_json::Map::new();
    for table in ["multipart_bucket_stats", "multipart_principal_stats"] {
        let (sessions, bytes): (u64, u64) = db.query_row(
            &format!(
                "SELECT coalesce(sum(active_uploads),0), coalesce(sum(staged_bytes),0) FROM {table}"
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if sessions != expected.auxiliary_sessions || bytes != expected.auxiliary_part_bytes {
            return Err(
                "multipart quota roll-ups disagree with independently expected persistent fixtures"
                    .into(),
            );
        }
        report.insert(
            table.to_owned(),
            json!({"active_uploads":sessions,"staged_bytes":bytes}),
        );
    }
    let user_bytes: u64 = db.query_row(
        "SELECT coalesce(sum(logical_bytes),0) FROM user_stats",
        [],
        |row| row.get(0),
    )?;
    if user_bytes != expected.seed_logical_bytes {
        return Err("user logical-byte roll-up disagrees with seed".into());
    }
    report.insert("user_logical_bytes".to_owned(), json!(user_bytes));
    Ok(report.into())
}

async fn run(config: Config) -> Result<(), Error> {
    config.validate()?;
    let start = Instant::now();
    let deadline = start + Duration::from_secs(config.deadline_seconds);
    std::fs::create_dir(&config.root)?;
    let path = config.root.join("metadata.sqlite3");
    let store = Arc::new(cairn_meta::open(&path, &options())?);
    emit(
        json!({"event":"start","schema":1,"variant":"canonical_writer_full_populated_v1",
        "config":config,"cache":{"read_connections":8,"kib_per_connection":8192,"mmap_bytes":0,"application_cache":"absent"}}),
    );
    let prepare_start = Instant::now();
    let (fixture, prepare_observation) = observed(
        &store,
        &config.root,
        deadline,
        workload::prepare(
            store.clone(),
            config.buckets,
            config.seed_rows as u64,
            config.seed,
            deadline,
        ),
    )
    .await?;
    emit(
        json!({"event":"prepared","seconds":prepare_start.elapsed().as_secs_f64(),
        "expected":fixture.expected(),"writer":prepare_observation}),
    );
    let fixture = Arc::new(fixture);
    let (_, verification) =
        observed(&store, &config.root, deadline, fixture.verify(deadline)).await?;
    emit(
        json!({"event":"seed_verified","writer":verification,"quota":verify_quota(&path, &fixture.expected())?}),
    );
    let mut phases = Vec::new();
    for (phase, concurrency) in config.concurrency.iter().copied().enumerate() {
        let began = Instant::now();
        let before = WriterCpu::discover()?;
        let cleanup_claims_before = fixture.cleanup_claims_lost();
        let stop = began + Duration::from_secs(config.phase_seconds);
        if stop + Duration::from_secs(5) >= deadline {
            return Err("insufficient admitted phase/cleanup time".into());
        }
        let (stats, observation) = observed(
            &store,
            &config.root,
            deadline,
            load(fixture.clone(), concurrency, stop, deadline),
        )
        .await?;
        let seconds = began.elapsed().as_secs_f64();
        let cpu = before.elapsed_seconds(&WriterCpu::read(before.tid)?, config.ticks_per_second)?;
        let report = json!({"phase":phase,"concurrency":concurrency,"seconds":seconds,
            "completed_operation_bundles":stats.operations,"bundles_per_second":stats.operations as f64/seconds,
            "families":stats.report(),"writer_cpu_seconds":cpu,
            "cleanup_claims_lost":fixture.cleanup_claims_lost() - cleanup_claims_before,
            "serialized_observed_seconds":observation.serialized_observed_seconds(),
            "stage_samples_complete":observation.complete(),"writer":observation});
        emit(json!({"event":"phase","report":report}));
        phases.push(report);
        let (_, observation) =
            observed(&store, &config.root, deadline, fixture.verify(deadline)).await?;
        emit(
            json!({"event":"phase_verified","phase":phase,"writer":observation,"quota":verify_quota(&path, &fixture.expected())?}),
        );
    }
    let expected = serde_json::to_value(fixture.expected())?;
    let before_checkpoint = database_sizes(&path)?;
    let checkpoint_start = Instant::now();
    let checkpoint = tokio::time::timeout_at(deadline, store.checkpoint()).await??;
    if checkpoint.busy {
        return Err("final checkpoint busy".into());
    }
    let checkpoint_seconds = checkpoint_start.elapsed().as_secs_f64();
    let post_checkpoint_wal = wal_bytes(&config.root)?;
    let fixture = Arc::try_unwrap(fixture).map_err(|_| "fixture still shared after joined load")?;
    // Preserve the bounded expected-state recipe across an actual closed connection set.
    let recipe = fixture.detach();
    let store = Arc::try_unwrap(store).map_err(|_| "store still shared before close")?;
    let checkpoint = tokio::time::timeout_at(deadline, store.checkpoint_and_close()).await??;
    if checkpoint.busy {
        return Err("close checkpoint busy".into());
    }
    let reopen_start = Instant::now();
    let reopened = Arc::new(cairn_meta::open(&path, &options())?);
    let reopen_seconds = reopen_start.elapsed().as_secs_f64();
    // Reconstruct only the deterministic verification recipe; never reseed on reopen.
    let recipe = recipe.attach(reopened.clone());
    let (_, reopen_observation) =
        observed(&reopened, &config.root, deadline, recipe.verify(deadline)).await?;
    let reopen_quota = verify_quota(&path, &recipe.expected())?;
    drop(recipe);
    let checkpoint = tokio::time::timeout_at(deadline, reopened.checkpoint()).await??;
    if checkpoint.busy {
        return Err("final observation checkpoint busy".into());
    }
    // Observe while the store still owns WAL lifetime. A fresh read-only connection after
    // shutdown can recreate sidecars; the final close must remain the last database access.
    let final_database = database_sizes(&path)?;
    let reopened = Arc::try_unwrap(reopened).map_err(|_| "reopened store still shared")?;
    let checkpoint = tokio::time::timeout_at(deadline, reopened.checkpoint_and_close()).await??;
    if checkpoint.busy {
        return Err("reopened close checkpoint busy".into());
    }
    emit(
        json!({"event":"result","status":"complete","expected":expected,"phases":phases,
        "before_checkpoint":before_checkpoint,"checkpoint_seconds":checkpoint_seconds,
        "post_checkpoint_wal_bytes":post_checkpoint_wal,"reopen_seconds":reopen_seconds,
        "reopen_writer":reopen_observation,"reopen_quota":reopen_quota,"final_database":final_database,
        "cache_observations":{"application_cache":"absent","sqlite_hits":null,"sqlite_misses":null,
            "reason":"canonical store exposes configured cache sizes, not per-connection cache-status counters"},
        "total_seconds":start.elapsed().as_secs_f64(),"physical_objects":"not_created_metadata_only"}),
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Error> {
    use std::io::Read;
    let mut input = String::new();
    let path = std::env::args_os()
        .nth(1)
        .ok_or("config file argument required")?;
    std::fs::File::open(path)?
        .take(16_385)
        .read_to_string(&mut input)?;
    if input.len() > 16_384 {
        return Err("config exceeds bounded input".into());
    }
    match run(serde_json::from_str(&input)?).await {
        Ok(()) => Ok(()),
        Err(error) => {
            let reason = error.to_string();
            let status = if reason.contains("deadline") || reason.contains("insufficient admitted")
            {
                "INCONCLUSIVE"
            } else {
                "FAIL"
            };
            emit(json!({"event":"error","status":status,"reason":reason}));
            std::process::exit(if status == "INCONCLUSIVE" { 2 } else { 1 });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::{
        Bucket, BucketName, MetadataStore, Mutation, OwnershipMode, Timestamp, UserId,
        VersioningState,
    };

    #[tokio::test]
    async fn periodic_checkpoint_retries_after_a_real_wal_reader_releases_its_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("metadata.sqlite3");
        let store = cairn_meta::open(&path, &options()).unwrap();
        let bucket = |name: &str| {
            Mutation::CreateBucket(Box::new(Bucket {
                name: BucketName::parse(name).unwrap(),
                owner_id: UserId("checkpoint-owner".to_owned()),
                created_at: Timestamp(1),
                versioning: VersioningState::Enabled,
                ownership_mode: OwnershipMode::BucketOwnerEnforced,
                region: "us-east-1".to_owned(),
                compression: None,
            }))
        };
        store.submit(bucket("checkpoint-first")).await.unwrap();
        assert!(!store.checkpoint().await.unwrap().busy);
        let reader = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        reader.execute_batch("BEGIN").unwrap();
        let count = || {
            reader
                .query_row("SELECT count(*) FROM buckets", [], |row| {
                    row.get::<_, u64>(0)
                })
                .unwrap()
        };
        assert_eq!(count(), 1);
        store.submit(bucket("checkpoint-second")).await.unwrap();
        assert_eq!(count(), 1, "the reader must still own the old snapshot");
        assert!(wal_bytes(root.path()).unwrap() > 0);

        let mut observation = WriterObservation::start(&store);
        periodic_checkpoint(&store, &mut observation).await.unwrap();
        periodic_checkpoint(&store, &mut observation).await.unwrap();
        assert_eq!(observation.checkpoint_attempts, 2);
        assert_eq!(observation.checkpoint_busy, 2);
        assert_eq!(observation.checkpoint_completed, 0);
        reader.execute_batch("ROLLBACK").unwrap();
        drop(reader);
        periodic_checkpoint(&store, &mut observation).await.unwrap();
        assert_eq!(observation.checkpoint_attempts, 3);
        assert_eq!(observation.checkpoint_busy, 2);
        assert_eq!(observation.checkpoint_completed, 1);
        assert_eq!(wal_bytes(root.path()).unwrap(), 0);
        assert!(!store.checkpoint_and_close().await.unwrap().busy);
    }
}
