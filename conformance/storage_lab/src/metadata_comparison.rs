//! One owned fresh-process arm of the conditional hot-bucket comparison.
use cairn_meta::{OpenOptions, SqliteMetadataStore};
use cairn_storage_lab::{
    metadata_fjall,
    metadata_metrics::{WriterCpu, WriterObservation},
    metadata_run::load,
    metadata_workload::{self as workload, FixtureIdentity},
};
use cairn_types::MetadataStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;
type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Sqlite,
    Fjall,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Action {
    Prepare,
    Run,
    Verify,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    root: PathBuf,
    engine: Kind,
    action: Action,
    seed_rows: u64,
    seed: u64,
    phase_seconds: u64,
    deadline_seconds: u64,
    ticks_per_second: u64,
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        if !self.root.is_absolute()
            || self
                .root
                .parent()
                .ok_or("missing root parent")?
                .canonicalize()?
                != self.root.parent().ok_or("missing root parent")?
            || !(10..=100_000).contains(&self.seed_rows)
            || !self.seed_rows.is_multiple_of(10)
            || !(1..=10).contains(&self.phase_seconds)
            || !(1..=600).contains(&self.deadline_seconds)
            || !(1..=100_000).contains(&self.ticks_per_second)
            || (self.action == Action::Prepare && self.root.exists())
            || (self.action != Action::Prepare && self.root.canonicalize()? != self.root)
        {
            return Err("invalid bounded comparison config or root".into());
        }
        Ok(())
    }
}
enum Engine {
    Sqlite(Arc<SqliteMetadataStore>),
    Fjall(Arc<metadata_fjall::Store>),
}
impl Engine {
    fn open(config: &Config) -> Result<Self, Error> {
        if config.action != Action::Prepare
            && config.engine == Kind::Sqlite
            && !std::fs::symlink_metadata(config.root.join("metadata.sqlite3"))?.is_file()
        {
            return Err("SQLite seed database is not a regular file".into());
        }
        Ok(match config.engine {
            Kind::Sqlite => Self::Sqlite(Arc::new(cairn_meta::open(
                &config.root.join("metadata.sqlite3"),
                &OpenOptions {
                    synchronous_full: true,
                    read_pool_size: 8,
                    cache_size: -8192,
                    mmap_bytes: 0,
                    ..Default::default()
                },
            )?)),
            Kind::Fjall => Self::Fjall(if config.action == Action::Prepare {
                metadata_fjall::Store::open(&config.root.join("fjall"))?
            } else {
                metadata_fjall::Store::open_existing(&config.root.join("fjall"))?
            }),
        })
    }
    fn store(&self) -> Arc<dyn MetadataStore> {
        match self {
            Self::Sqlite(store) => store.clone(),
            Self::Fjall(store) => store.clone(),
        }
    }
    fn cpu(&self) -> Result<WriterCpu, Error> {
        Ok(match self {
            Self::Sqlite(_) => WriterCpu::discover()?,
            Self::Fjall(store) => WriterCpu::read(store.stages().writer_tid)?,
        })
    }
    async fn quota(
        &self,
        config: &Config,
        expected: &workload::ExpectedState,
        identity: &FixtureIdentity,
    ) -> Result<(), Error> {
        match self {
            Self::Fjall(store) => {
                store.verify_auxiliary(expected).await?;
                store.verify_generation(&identity.generation).await?;
            }
            Self::Sqlite(_) => {
                let db = rusqlite::Connection::open_with_flags(
                    config.root.join("metadata.sqlite3"),
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )?;
                for table in ["multipart_bucket_stats", "multipart_principal_stats"] {
                    let (sessions, bytes): (u64, u64) = db.query_row(&format!("SELECT coalesce(sum(active_uploads),0), coalesce(sum(staged_bytes),0) FROM {table}"), [], |row| Ok((row.get(0)?, row.get(1)?)))?;
                    if sessions != expected.auxiliary_sessions
                        || bytes != expected.auxiliary_part_bytes
                    {
                        return Err("SQLite multipart quota mismatch".into());
                    }
                }
                let bytes: u64 = db.query_row(
                    "SELECT coalesce(sum(logical_bytes),0) FROM user_stats",
                    [],
                    |row| row.get(0),
                )?;
                let generation: String = db.query_row(
                    "SELECT generation FROM storage_recovery_state WHERE singleton=1",
                    [],
                    |row| row.get(0),
                )?;
                if bytes != expected.seed_logical_bytes
                    || generation != identity.generation.as_str()
                {
                    return Err("SQLite principal or generation mismatch".into());
                }
            }
        }
        Ok(())
    }
    async fn checkpoint(&self) -> Result<Value, Error> {
        Ok(match self {
            Self::Sqlite(store) => {
                let result = store.checkpoint().await?;
                if result.busy {
                    return Err("SQLite checkpoint busy after joined owners".into());
                }
                json!({"busy":result.busy, "log_frames":result.log_frames, "checkpointed_frames":result.checkpointed_frames})
            }
            Self::Fjall(store) => serde_json::to_value(store.checkpoint().await?)?,
        })
    }
    async fn close(self) -> Result<(), Error> {
        match self {
            Self::Sqlite(store) => {
                let store = Arc::try_unwrap(store).map_err(|_| "SQLite owners remain at close")?;
                if store.checkpoint_and_close().await?.busy {
                    return Err("SQLite close checkpoint busy".into());
                }
            }
            Self::Fjall(store) => store.close().await?,
        }
        Ok(())
    }
}
fn wal_bytes(root: &Path) -> Result<u64, Error> {
    match std::fs::metadata(root.join("metadata.sqlite3-wal")) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}
fn stage_difference(before: &Value, after: &Value) -> Result<Value, Error> {
    let before = before
        .as_object()
        .ok_or("missing candidate stage baseline")?;
    let mut difference = serde_json::Map::new();
    for (key, value) in after
        .as_object()
        .ok_or("missing candidate stage counters")?
    {
        let value = value.as_u64().ok_or("nonnumeric stage counter")?;
        let previous = before
            .get(key)
            .and_then(Value::as_u64)
            .ok_or("stage baseline missing")?;
        difference.insert(
            key.clone(),
            json!(if key == "writer_tid" {
                value
            } else {
                value
                    .checked_sub(previous)
                    .ok_or("stage counter regressed")?
            }),
        );
    }
    Ok(difference.into())
}
async fn observed<F, T>(
    engine: &Engine,
    config: &Config,
    deadline: Instant,
    future: F,
) -> Result<(T, Value), Error>
where
    F: std::future::Future<Output = Result<T, String>>,
{
    tokio::pin!(future);
    let mut sqlite = match engine {
        Engine::Sqlite(store) => Some(WriterObservation::start(store)),
        _ => None,
    };
    let baseline = match engine {
        Engine::Fjall(store) => serde_json::to_value(store.stages())?,
        _ => Value::Null,
    };
    let mut maxima: BTreeMap<String, f64> = BTreeMap::new();
    let mut samples = 0_u64;
    let mut failure: Option<Error> = None;
    let mut interval = tokio::time::interval(Duration::from_millis(20));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let output = loop {
        tokio::select! {
            biased;
            result = &mut future => break result,
            _ = interval.tick(), if failure.is_none() => {
                let sampled: Result<(), Error> = async {
                samples += 1;
                match engine {
                    Engine::Sqlite(store) => {
                        let observation = sqlite.as_mut().ok_or("SQLite observer missing")?;
                        observation.drain(store);
                        let bytes = wal_bytes(&config.root)?;
                        observation.peak_wal_bytes = observation.peak_wal_bytes.max(bytes);
                        if bytes >= 64 * 1024 * 1024 && Instant::now() < deadline {
                            let result = store.checkpoint().await?;
                            observation.checkpoint_attempts += 1;
                            observation.checkpoint_busy += u64::from(result.busy);
                            observation.checkpoint_completed += u64::from(!result.busy);
                            observation.drain(store);
                        }
                    }
                    Engine::Fjall(store) => {
                        let state = serde_json::to_value(store.engine_state()?)?;
                        for (key, value) in state.as_object().ok_or("candidate counters missing")? {
                            let value = value.as_f64().ok_or("nonnumeric candidate counter")?;
                            let maximum = maxima.entry(key.clone()).or_default();
                            *maximum = maximum.max(value);
                        }
                    }
                }
                Ok(())
                }.await;
                if let Err(error) = sampled { failure = Some(error); }
            }
        }
    };
    if let Some(error) = failure {
        return Err(error);
    }
    let output = output.map_err(|error| -> Error { error.into() })?;
    if Instant::now() >= deadline {
        return Err("comparison deadline reached after joining admitted owners".into());
    }
    let observation = match engine {
        Engine::Sqlite(store) => {
            let mut observation = sqlite.ok_or("SQLite observer missing")?;
            observation.drain(store);
            observation.peak_wal_bytes = observation.peak_wal_bytes.max(wal_bytes(&config.root)?);
            if !observation.complete() {
                return Err("SQLite stage samples lost".into());
            }
            json!({"stages_complete":true,"serialized_observed_seconds":observation.serialized_observed_seconds(),"writer":observation})
        }
        Engine::Fjall(store) => {
            json!({"stages_complete":true,"writer":stage_difference(&baseline, &serde_json::to_value(store.stages())?)?,
            "engine_samples":samples,"engine_maxima":maxima,"engine_final":store.engine_state()?})
        }
    };
    Ok((output, observation))
}
fn bounded_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Error> {
    let mut input = String::new();
    std::fs::File::open(path)?
        .take(16_385)
        .read_to_string(&mut input)?;
    if input.len() > 16_384 {
        return Err("oversized comparison config or identity".into());
    }
    Ok(serde_json::from_str(&input)?)
}
fn save_identity(root: &Path, identity: &FixtureIdentity) -> Result<(), Error> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("fixture.json"))?;
    file.write_all(&serde_json::to_vec(identity)?)?;
    file.sync_all()?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}
fn disk_after_close(root: &Path) -> Result<Value, Error> {
    let mut pending = vec![(root.to_path_buf(), 0)];
    let (mut files, mut logical, mut allocated, mut table_bytes) = (0_u64, 0_u64, 0_u64, 0_u64);
    while let Some((directory, depth)) = pending.pop() {
        if depth > 8 {
            return Err("comparison directory depth exceeded".into());
        }
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if entry.file_type()?.is_symlink() {
                return Err("unexpected comparison symlink".into());
            }
            allocated += metadata.blocks() * 512;
            if metadata.is_dir() {
                pending.push((entry.path(), depth + 1));
            } else if metadata.is_file() {
                files += 1;
                logical += metadata.len();
                if entry
                    .path()
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == "tables")
                {
                    table_bytes += metadata.len();
                }
            } else {
                return Err("unexpected comparison entry type".into());
            }
            if files + pending.len() as u64 > 10_000 {
                return Err("comparison filesystem entry bound exceeded".into());
            }
        }
    }
    Ok(
        json!({"files":files,"logical_bytes":logical,"allocated_bytes":allocated,"physical_table_bytes":table_bytes}),
    )
}

fn process_io_after_close() -> Result<BTreeMap<String, u64>, Error> {
    let mut counters = BTreeMap::new();
    for line in std::fs::read_to_string("/proc/self/io")?.lines() {
        let (name, value) = line.split_once(':').ok_or("invalid process I/O counter")?;
        counters.insert(name.to_owned(), value.trim().parse()?);
    }
    Ok(counters)
}
async fn run(config: Config) -> Result<(), Error> {
    config.validate()?;
    let start = Instant::now();
    let deadline = start + Duration::from_secs(config.deadline_seconds);
    if config.action == Action::Prepare {
        std::fs::create_dir(&config.root)?;
    }
    let resume_identity = if config.action == Action::Prepare {
        None
    } else {
        let identity: FixtureIdentity = bounded_json(&config.root.join("fixture.json"))?;
        if identity.seed_rows != config.seed_rows
            || identity.seed != config.seed
            || identity.bucket_count != 1
        {
            return Err("resume identity disagrees with comparison config".into());
        }
        Some(identity)
    };
    let open = Instant::now();
    let engine = Engine::open(&config)?;
    let open_seconds = open.elapsed().as_secs_f64();
    println!(
        "{}",
        json!({"event":"opened","config":config,"open_seconds":open_seconds})
    );
    let fixture = if config.action == Action::Prepare {
        let began = Instant::now();
        let (fixture, observation) = observed(
            &engine,
            &config,
            deadline,
            workload::prepare(engine.store(), 1, config.seed_rows, config.seed, deadline),
        )
        .await?;
        save_identity(&config.root, &fixture.identity())?;
        println!(
            "{}",
            json!({"event":"prepared","seconds":began.elapsed().as_secs_f64(),"observation":observation,"expected":fixture.expected()})
        );
        fixture
    } else {
        workload::Fixture::resume(
            engine.store(),
            resume_identity.ok_or("resume identity absent")?,
        )?
    };
    let identity = fixture.identity();
    let expected = fixture.expected();
    let (verified, observation) =
        observed(&engine, &config, deadline, fixture.verify(deadline)).await?;
    engine.quota(&config, &expected, &identity).await?;
    println!(
        "{}",
        json!({"event":"verified_before","expected":verified,"observation":observation,"quota_verified":true})
    );
    let fixture = Arc::new(fixture);
    let mut phase = Value::Null;
    if config.action == Action::Run {
        let began = Instant::now();
        let before = engine.cpu()?;
        let stop = began + Duration::from_secs(config.phase_seconds);
        if stop + Duration::from_secs(3) >= deadline {
            return Err("insufficient comparison phase and cleanup time".into());
        }
        let (stats, observation) = observed(
            &engine,
            &config,
            deadline,
            load(fixture.clone(), 128, stop, deadline),
        )
        .await?;
        let seconds = began.elapsed().as_secs_f64();
        let writer_cpu_seconds = before.elapsed_seconds(&engine.cpu()?, config.ticks_per_second)?;
        phase = json!({"seconds":seconds,"concurrency":128,"completed_operation_bundles":stats.operations,
            "bundles_per_second":stats.operations as f64 / seconds,"families":stats.report(),"writer_cpu_seconds":writer_cpu_seconds,
            "cleanup_claims_lost":fixture.cleanup_claims_lost(),"observation":observation});
        println!("{}", json!({"event":"phase","report":phase}));
        let (verified, observation) =
            observed(&engine, &config, deadline, fixture.verify(deadline)).await?;
        engine.quota(&config, &expected, &identity).await?;
        println!(
            "{}",
            json!({"event":"verified_after","expected":verified,"observation":observation,"quota_verified":true})
        );
    }
    drop(fixture);
    let began = Instant::now();
    let checkpoint = engine.checkpoint().await?;
    let checkpoint_seconds = began.elapsed().as_secs_f64();
    let began = Instant::now();
    engine.close().await?;
    let close_seconds = began.elapsed().as_secs_f64();
    if Instant::now() >= deadline {
        return Err("comparison deadline expired during checked close".into());
    }
    println!(
        "{}",
        json!({"event":"result","status":"complete","expected":expected,"phase":phase,"open_seconds":open_seconds,
        "checkpoint_seconds":checkpoint_seconds,"checkpoint":checkpoint,"close_seconds":close_seconds,"close_complete":true,
        "disk_after_close":disk_after_close(&config.root)?,"total_seconds":start.elapsed().as_secs_f64(),
            "physical_objects":"not_created_metadata_only", "process_io_after_close":process_io_after_close()?})
    );
    Ok(())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Error> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("config file argument required")?;
    let result = run(bounded_json(Path::new(&path))?).await;
    if let Err(error) = result {
        println!(
            "{}",
            json!({"event":"result","status":"incomplete","reason":error.to_string()})
        );
        std::process::exit(2);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn observation_error_waits_for_owned_work_before_returning() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config {
            root: directory.path().to_path_buf(),
            engine: Kind::Sqlite,
            action: Action::Prepare,
            seed_rows: 100,
            seed: 1,
            phase_seconds: 1,
            deadline_seconds: 10,
            ticks_per_second: 100,
        };
        let engine = Engine::open(&config).unwrap();
        let completed = Arc::new(AtomicBool::new(false));
        let owner = completed.clone();
        let blocker = directory.path().join("not-a-directory");
        std::fs::write(&blocker, b"owned test fixture").unwrap();
        config.root = blocker;
        let result = observed(
            &engine,
            &config,
            Instant::now() + Duration::from_secs(5),
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                owner.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
        assert!(result.is_err());
        assert!(
            completed.load(Ordering::SeqCst),
            "observation failure must not drop admitted work"
        );
        engine.close().await.unwrap();
    }
}
