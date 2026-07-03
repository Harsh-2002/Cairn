//! The Cairn server binary entrypoint. Parses configuration, initialises observability, builds
//! the engine stack, and runs the HTTP server with ordered graceful shutdown. Also carries the
//! node-local commands that operate directly on the data dir from config: `bootstrap` (mint the
//! first administrator), `integrity` (on-demand reconciliation), `migrate` (run migrations and
//! report the schema version), and `backup`/`restore` (the ARCH 31.4 consistent snapshot and its
//! inverse). The full remote-admin CLI ships as `cairn-cli` in a later wave.

// The default (and every non-`fast-io`) build keeps the strongest posture: `forbid(unsafe_code)`
// makes it impossible to introduce `unsafe` anywhere in the crate. The experimental, Linux-only
// `fast-io` performance path needs a few raw syscalls (kTLS setsockopt probe, `sendfile(2)`), so
// under that feature we relax to `deny(unsafe_code)` — still rejecting every `unsafe` block by
// default, but allowing the individually reviewed, SAFETY-commented blocks in `sendfile.rs` to
// opt in with `#[allow(unsafe_code)]`. `forbid` cannot be locally overridden; `deny` can.
#![cfg_attr(not(feature = "fast-io"), forbid(unsafe_code))]
#![cfg_attr(feature = "fast-io", deny(unsafe_code))]

mod adapter;
mod background;
mod cli_remote;
mod config;
mod import_dest;
mod import_run;
mod key_rewrap;
mod metrics_agg;
mod observability;
mod server;
mod sse;
// Linux-only zero-copy syscall helpers for the `fast-io` perf path (kTLS probe + sendfile(2)).
// Gated to the feature *and* Linux so it is absent (and cannot warn) in every other build.
#[cfg(all(feature = "fast-io", target_os = "linux"))]
mod sendfile;
// The plaintext HTTP/1.1 sendfile fast path for object GETs; same gate as `sendfile`.
#[cfg(all(feature = "fast-io", target_os = "linux"))]
mod fast_get;
mod stack;
mod tls;

use clap::{Parser, Subcommand};
use config::Config;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

/// Cairn — a production-grade, S3-compatible object storage server. Configuration is
/// environment-only: set `CAIRN_*` variables (there is no configuration file).
#[derive(Debug, Parser)]
#[command(name = "cairn", version, about)]
struct Cli {
    /// The subcommand to run (defaults to `serve`).
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the server.
    Serve,
    /// Validate the configuration and exit.
    ValidateConfig,
    /// Ensure the single root administrator exists and print its credentials. Idempotent, and the
    /// same identity `serve` seeds — so a node always has exactly one default admin (root).
    Bootstrap,
    /// Run reconciliation on demand (reclaim orphaned blobs); a node-local integrity check.
    ///
    /// With `--repair`, additionally run in repair mode (ARCH 24.3/29.4): drop metadata rows
    /// whose backing blob is missing on disk, so the store can re-serve the remaining keys cleanly.
    Integrity {
        /// Also drop metadata rows whose backing blob is missing (destructive repair).
        #[arg(long)]
        repair: bool,
    },
    /// Open the store (running migrations) and report the applied schema version.
    Migrate,
    /// Take a consistent snapshot of the data dir into DIR (ARCH 31.4): checkpoint + copy the
    /// database, then copy the blob tree excluding the staging area.
    Backup {
        /// Destination directory for the snapshot (created if absent).
        dir: PathBuf,
    },
    /// Restore a snapshot from DIR into the configured data dir, then run reconciliation
    /// (ARCH 31.4): place the database and blobs, then reconcile.
    Restore {
        /// Source snapshot directory produced by `backup`.
        dir: PathBuf,
    },

    // --- Remote administration (ARCH 24.2): a thin client over a running server's management API
    //     and S3 data plane. These commands do not touch the local data dir or config; they are
    //     dispatched before `Config::load()`. Connection + output options come from the flattened
    //     `RemoteOpts` (flags or `CAIRN_*` env).
    /// Bucket operations against a running server's management API.
    Bucket {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::BucketCmd,
    },
    /// User operations against a running server's management API.
    User {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::UserCmd,
    },
    /// Replication operations against a running server's management API.
    Replication {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::ReplicationCmd,
    },
    /// Object operations over a running server's S3 data plane (same Bearer token).
    Object {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::ObjectCmd,
    },
    /// Object sharing on a running server: share links + presigned URLs.
    Share {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::ShareCmd,
    },
    /// Import buckets + objects from another S3-compatible store into a running server.
    Import {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::ImportCmd,
    },
    /// Print a running server's store overview.
    Overview {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Serve);

    // Remote-administration commands are a thin client over a running server's HTTP surfaces and
    // never read the local data dir or environment-only config; dispatch them before `Config::load`
    // so they work without a configured node (only `--endpoint`/`--access-key`/`--secret-key` or the
    // corresponding `CAIRN_*` vars matter).
    match command {
        Command::Bucket { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Bucket { cmd });
        }
        Command::User { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::User { cmd });
        }
        Command::Replication { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Replication { cmd });
        }
        Command::Object { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Object { cmd });
        }
        Command::Share { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Share { cmd });
        }
        Command::Import { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Import { cmd });
        }
        Command::Overview { opts } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Overview);
        }
        _ => {}
    }

    // Node-local commands need the environment-only config.
    let cfg = match Config::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e}");
            return ExitCode::from(2);
        }
    };

    match command {
        Command::ValidateConfig => {
            // The fields parsed; also enforce the serve-time deployment guardrail so an operator who
            // runs `validate-config` before deploying is told about an insecure public bind.
            if let Err(e) = cfg.refuse_insecure_public_bind() {
                eprintln!("configuration error: {e}");
                return ExitCode::from(2);
            }
            println!("configuration valid");
            ExitCode::SUCCESS
        }
        Command::Bootstrap => bootstrap(cfg),
        Command::Integrity { repair } => integrity(cfg, repair),
        Command::Migrate => migrate(cfg),
        Command::Backup { dir } => backup(cfg, &dir),
        Command::Restore { dir } => restore(cfg, &dir),
        Command::Serve => {
            if let Err(e) = cfg.refuse_insecure_public_bind() {
                eprintln!("configuration error: {e}");
                return ExitCode::from(2);
            }
            run_server(cfg)
        }
        // The remote-admin variants are handled and returned above.
        Command::Bucket { .. }
        | Command::User { .. }
        | Command::Replication { .. }
        | Command::Object { .. }
        | Command::Share { .. }
        | Command::Import { .. }
        | Command::Overview { .. } => unreachable!("remote commands dispatched above"),
    }
}

fn integrity(cfg: Config, repair: bool) -> ExitCode {
    use cairn_types::blob::ReconcileOpts;
    use cairn_types::traits::BlobStore;

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async {
        // Open through the configured backend (CAIRN_META_BACKEND) so reconciliation consults the
        // same engine the server serves from. Repair mode needs the metadata store itself (to drop
        // dangling rows), so keep both halves.
        let (meta, oracle) = match stack::open_meta_store(&cfg).await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("failed to open metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };
        let blob = match cairn_blob::LocalBlobStore::open(cfg.data_dir.clone()).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("failed to open blob store: {e}");
                return ExitCode::FAILURE;
            }
        };

        // First, the always-on forward pass: reclaim orphaned blobs (blobs with no metadata row).
        // `integrity` is an explicit, on-demand reconcile run against a quiesced store (no in-flight
        // writes), so reclaim crash-orphans immediately (margin 0) rather than honouring the live-
        // operation safety margin.
        let opts = ReconcileOpts {
            staging_safety_margin_secs: 0,
            ..ReconcileOpts::default()
        };
        match blob.reconcile(oracle.as_ref(), opts).await {
            Ok(r) => {
                println!(
                    "reconciliation complete: scanned={} orphans_reclaimed={} staging_cleaned={} sessions_cleaned={} errors={}",
                    r.blobs_scanned, r.orphans_reclaimed, r.staging_cleaned, r.sessions_cleaned, r.errors
                );
            }
            Err(e) => {
                eprintln!("reconciliation failed: {e}");
                return ExitCode::FAILURE;
            }
        }

        // Then, in repair mode, the inverse pass: drop metadata rows whose backing blob is missing
        // on disk (ARCH 24.3/29.4). The forward reconcile cannot detect these — it only walks the
        // blob tree — so repair walks the metadata instead, probes the blob store for each version's
        // backing object, and deletes the row when the blob is gone.
        if repair {
            match repair_dangling_rows(meta.as_ref(), &blob).await {
                Ok(dropped) => {
                    println!("repair complete: dangling_rows_dropped={dropped}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("repair failed: {e}");
                    ExitCode::FAILURE
                }
            }
        } else {
            ExitCode::SUCCESS
        }
    })
}

/// The page size used when walking metadata in repair mode; bounds memory per round.
const REPAIR_PAGE_LIMIT: u32 = 1000;
/// Upper bound on paging iterations per bucket, so a hostile/corrupt cursor can never spin forever.
const REPAIR_MAX_PAGES: u32 = 100_000;

/// Repair-mode reconciliation (ARCH 24.3/29.4): drop every metadata row whose backing blob is
/// missing on disk. Walks each bucket's versions, resolves each non-delete-marker version's
/// `storage_path`, probes the blob store for it, and submits a `DeleteVersion` mutation when the
/// blob is absent. Returns the count of rows dropped.
///
/// This composes only the public store/blob primitives (no privileged internals): it is the
/// node-local inverse of orphan reclamation and is deliberately destructive, so it runs only under
/// the explicit `--repair` flag.
async fn repair_dangling_rows(
    meta: &dyn cairn_types::traits::MetadataStore,
    blob: &cairn_blob::LocalBlobStore,
) -> Result<u64, String> {
    use cairn_types::error::BlobError;
    use cairn_types::meta::{ListQuery, Mutation, MutationOutcome};
    use cairn_types::traits::BlobStore;

    let buckets = meta.list_buckets(None).await.map_err(|e| e.to_string())?;
    let mut dropped = 0u64;

    for bucket in &buckets {
        let mut cursor: Option<String> = None;
        for _ in 0..REPAIR_MAX_PAGES {
            let query = ListQuery {
                cursor: cursor.clone(),
                limit: REPAIR_PAGE_LIMIT,
                ..Default::default()
            };
            let page = meta
                .list_versions(&bucket.name, &query)
                .await
                .map_err(|e| e.to_string())?;
            if page.items.is_empty() {
                break;
            }

            for item in &page.items {
                // Delete markers carry no blob, so they are never dangling.
                if item.is_delete_marker {
                    continue;
                }
                // Resolve the version's backing storage path. A row that has gone missing between
                // the listing and this read is simply skipped (nothing to repair).
                let row = match meta
                    .get_version(&bucket.name, &item.key, &item.version_id)
                    .await
                {
                    Ok(Some(r)) => r,
                    Ok(None) => continue,
                    Err(e) => return Err(e.to_string()),
                };
                let Some(path) = row.storage_path.clone() else {
                    continue;
                };

                // Probe the blob store. Opening a present blob succeeds (we read nothing); a
                // missing blob yields `NotFound`, which is exactly the dangling case we repair. Any
                // other error is surfaced rather than treated as "missing", so a transient I/O fault
                // never deletes good metadata.
                match blob.open(&path, None, &row.compression).await {
                    Ok(_) => {}
                    Err(BlobError::NotFound) => {
                        match meta
                            .submit(Mutation::DeleteVersion {
                                bucket: bucket.name.clone(),
                                key: item.key.clone(),
                                version_id: item.version_id.clone(),
                                expected_updated_at: None,
                            })
                            .await
                        {
                            Ok(MutationOutcome::Deleted { freed, .. }) => {
                                // Best-effort, idempotent: the blob is already gone, but reclaim any
                                // path the store reports freed so no surprise orphan remains.
                                if let Some(freed) = freed {
                                    let _ = blob.delete(&freed).await;
                                }
                                dropped += 1;
                            }
                            Ok(_) => {}
                            Err(e) => return Err(e.to_string()),
                        }
                    }
                    Err(e) => return Err(e.to_string()),
                }
            }

            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
    }

    Ok(dropped)
}

/// Open the metadata store (which runs any pending migrations) and report the resulting schema
/// version. The server runs the same migrations at startup; this command is for operators who
/// prefer to migrate explicitly (ARCH 11.2, 24.2).
fn migrate(cfg: Config) -> ExitCode {
    if let Some(parent) = cfg.db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    // `open` runs migrations on the write connection before returning (ARCH 11.2). We then read
    // the applied version directly from `schema_migrations` rather than holding the store, which
    // keeps this command a thin reporter over the migration the open already performed.
    match cairn_meta::open(&cfg.db_path, &cairn_meta::OpenOptions::default()) {
        Ok(_store) => {}
        Err(e) => {
            eprintln!("failed to open metadata store: {e}");
            return ExitCode::FAILURE;
        }
    }
    match schema_version(&cfg.db_path) {
        Ok(v) => {
            println!("migrations applied; schema version {v}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("failed to read schema version: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Read the highest applied migration version from the database file.
fn schema_version(db_path: &std::path::Path) -> Result<i64, String> {
    let conn = rusqlite::Connection::open(db_path).map_err(|e| e.to_string())?;
    conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |r| r.get::<_, i64>(0),
    )
    .map_err(|e| e.to_string())
}

/// Take a consistent snapshot into `dir` (ARCH 31.4). The database is snapshotted first
/// (checkpoint to fold the WAL into the main file, then copy it), and the blob tree is copied
/// second excluding the staging area. Taking the database first guarantees the copied blob set is
/// a superset of what the snapshot references, so restore finds a blob for every row; any extra
/// blobs from writes after the snapshot are reclaimed by reconciliation on restore. The master
/// key is deliberately not part of the data dir, so it is not disclosed by the snapshot.
fn backup(cfg: Config, dir: &std::path::Path) -> ExitCode {
    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        if let Err(e) = tokio::fs::create_dir_all(dir).await {
            eprintln!("failed to create backup dir: {e}");
            return ExitCode::FAILURE;
        }

        // 1. Database first: open (runs migrations), checkpoint to fold the WAL into the main
        //    file, then copy the now-self-contained database file.
        let store = match cairn_meta::open(&cfg.db_path, &cairn_meta::OpenOptions::default()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to open metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(e) = store.checkpoint().await {
            eprintln!("failed to checkpoint before snapshot: {e}");
            return ExitCode::FAILURE;
        }
        let db_name = match cfg.db_path.file_name() {
            Some(n) => n.to_owned(),
            None => {
                eprintln!("db_path has no file name: {}", cfg.db_path.display());
                return ExitCode::FAILURE;
            }
        };
        let db_dest = dir.join(&db_name);
        if let Err(e) = tokio::fs::copy(&cfg.db_path, &db_dest).await {
            eprintln!("failed to copy database: {e}");
            return ExitCode::FAILURE;
        }
        // Drop the store so its connections (and any -wal/-shm) are released before we finish.
        drop(store);

        // 2. Blobs second: copy every per-bucket directory, excluding the staging area.
        let blob_dest = dir.join("blobs");
        match copy_blob_tree(&cfg.data_dir, &blob_dest).await {
            Ok(n) => {
                println!(
                    "backup complete: database -> {} ({n} blob entries) -> {}",
                    db_dest.display(),
                    blob_dest.display()
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("failed to copy blob tree: {e}");
                ExitCode::FAILURE
            }
        }
    })
}

/// Restore a snapshot from `dir` into the configured data dir, then reconcile (ARCH 31.4). The
/// database and blob tree produced by `backup` are placed, and reconciliation reclaims any blobs
/// written after the snapshot was taken.
fn restore(cfg: Config, dir: &std::path::Path) -> ExitCode {
    use cairn_types::blob::ReconcileOpts;
    use cairn_types::traits::BlobStore;

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        let db_name = match cfg.db_path.file_name() {
            Some(n) => n.to_owned(),
            None => {
                eprintln!("db_path has no file name: {}", cfg.db_path.display());
                return ExitCode::FAILURE;
            }
        };
        let db_src = dir.join(&db_name);
        let blob_src = dir.join("blobs");
        if !db_src.exists() {
            eprintln!("snapshot is missing the database: {}", db_src.display());
            return ExitCode::FAILURE;
        }

        // 1. Place files: the blob tree into the data dir, then the database.
        if let Some(parent) = cfg.db_path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        if let Err(e) = tokio::fs::create_dir_all(&cfg.data_dir).await {
            eprintln!("failed to create data dir: {e}");
            return ExitCode::FAILURE;
        }
        if blob_src.exists() {
            if let Err(e) = copy_blob_tree(&blob_src, &cfg.data_dir).await {
                eprintln!("failed to restore blob tree: {e}");
                return ExitCode::FAILURE;
            }
        }
        if let Err(e) = tokio::fs::copy(&db_src, &cfg.db_path).await {
            eprintln!("failed to restore database: {e}");
            return ExitCode::FAILURE;
        }

        // 2. Reconcile: reclaim any blobs from writes after the snapshot (ARCH 31.4).
        let store = match cairn_meta::open(&cfg.db_path, &cairn_meta::OpenOptions::default()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to open restored metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };
        let oracle = store.reconcile_oracle();
        let blob = match cairn_blob::LocalBlobStore::open(cfg.data_dir.clone()).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("failed to open blob store: {e}");
                return ExitCode::FAILURE;
            }
        };
        match blob
            .reconcile(
                &oracle,
                ReconcileOpts {
                    staging_safety_margin_secs: 0,
                    ..ReconcileOpts::default()
                },
            )
            .await
        {
            Ok(r) => {
                println!(
                    "restore complete: reconciled scanned={} orphans_reclaimed={}",
                    r.blobs_scanned, r.orphans_reclaimed
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("restore placed files but reconciliation failed: {e}");
                ExitCode::FAILURE
            }
        }
    })
}

/// Recursively copy the per-bucket blob directories from `src` to `dst`, skipping the `.staging`
/// area (in-progress writes are not part of a consistent snapshot, ARCH 31.4) and any database
/// sidecar files. Returns the number of top-level entries copied.
async fn copy_blob_tree(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<u64> {
    tokio::fs::create_dir_all(dst).await?;
    let mut copied = 0u64;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        // Exclude the staging area and the database files; only committed per-bucket blob
        // directories belong in the snapshot.
        if name_str == ".staging"
            || name_str.ends_with(".db")
            || name_str.ends_with(".db-wal")
            || name_str.ends_with(".db-shm")
        {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        if entry.file_type().await?.is_dir() {
            Box::pin(copy_dir_recursive(&from, &to)).await?;
        } else {
            tokio::fs::copy(&from, &to).await?;
        }
        copied += 1;
    }
    Ok(copied)
}

/// Recursively copy a directory and its contents.
async fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(dst).await?;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(entry) = entries.next_entry().await? {
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type().await?.is_dir() {
            Box::pin(copy_dir_recursive(&from, &to)).await?;
        } else {
            tokio::fs::copy(&from, &to).await?;
        }
    }
    Ok(())
}

fn runtime(cfg: &Config) -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    // Size the blocking pool to cover the metadata read pool + blob I/O concurrency so neither
    // starves the other (ARCH 30); compute parallelism is pinned only when set explicitly.
    builder.max_blocking_threads(cfg.effective_max_blocking_threads());
    if let Some(workers) = cfg.effective_worker_threads() {
        builder.worker_threads(workers);
    }
    builder.build()
}

fn run_server(cfg: Config) -> ExitCode {
    observability::init_tracing(&cfg.log_level, cfg.log_format);
    let metrics = observability::init_metrics();

    // Arm the fault-injection registry from $FAILPOINTS (only in `failpoints` builds, used by the
    // crash-consistency harness). The scenario must outlive the server, so it is held here.
    #[cfg(feature = "failpoints")]
    let _fail_scenario = fail::FailScenario::setup();

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    rt.block_on(async {
        let stack = match stack::build(&cfg).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                tracing::error!(error = %e, "failed to build engine stack");
                return ExitCode::FAILURE;
            }
        };
        match server::serve(cfg, metrics, stack).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!(error = %e, "server exited with error");
                ExitCode::FAILURE
            }
        }
    })
}

fn bootstrap(cfg: Config) -> ExitCode {
    use cairn_types::traits::{Clock, Crypto};
    use std::sync::Arc;

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    rt.block_on(async {
        if let Some(parent) = cfg.db_path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let _ = tokio::fs::create_dir_all(&cfg.data_dir).await;

        // Open through the configured backend (CAIRN_META_BACKEND) so the first administrator is
        // written into the same engine the server will later serve from.
        let store = match stack::open_meta_store(&cfg).await {
            Ok((meta, _oracle)) => meta,
            Err(e) => {
                eprintln!("failed to open metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };
        let crypto: Arc<dyn Crypto> = match stack::build_crypto(&cfg) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };
        let clock: Arc<dyn Clock> = Arc::new(cairn_crypto::SystemClock::new());

        // Seed exactly one default administrator — the root identity (CAIRN_ROOT_ACCESS_KEY /
        // CAIRN_ROOT_SECRET_KEY) that `serve` also ensures on every startup. Bootstrapping the SAME
        // identity (rather than minting a separate random "administrator") means `bootstrap` + `serve`
        // converge on a single "root" admin instead of leaving the node with two default admins.
        // Idempotent: re-running just re-affirms the root admin.
        if let Err(e) = stack::ensure_root_admin(&store, &crypto, &clock, &cfg).await {
            eprintln!("failed to seed the root administrator: {e}");
            return ExitCode::FAILURE;
        }

        let insecure_defaults =
            cfg.root_access_key == "cairn" && cfg.root_secret_key == "cairnadmin";
        // Print both credential forms with their canonical labels so tooling and the conformance
        // harnesses parse them: the Bearer token off the "Authorization: Bearer" line, and the SigV4
        // pair off the "Access Key Id:" / "Secret Access Key:" lines (last field).
        println!("Root administrator ready — the single default admin for this node.\n");
        println!("  Bearer (web console + management API):");
        println!(
            "    Authorization: Bearer {}.{}",
            cfg.root_access_key, cfg.root_secret_key
        );
        println!("\n  SigV4 (S3 SDKs / aws-cli):");
        println!("    Access Key Id:     {}", cfg.root_access_key);
        println!("    Secret Access Key: {}", cfg.root_secret_key);
        println!("    Region:            {}", cfg.region);
        println!("\n  Create further users from the console or `cairn remote user create`.",);
        if insecure_defaults {
            println!(
                "\n  WARNING: these are the INSECURE defaults (cairn / cairnadmin). Set\n  \
                 CAIRN_ROOT_ACCESS_KEY and CAIRN_ROOT_SECRET_KEY before exposing this node."
            );
        }
        ExitCode::SUCCESS
    })
}

#[cfg(test)]
mod tests {
    use super::{copy_blob_tree, schema_version};

    /// `copy_blob_tree` copies committed per-bucket blob directories but skips the staging area
    /// and database sidecars, so a snapshot contains only durable blobs (ARCH 31.4).
    #[tokio::test]
    async fn backup_copies_blobs_but_excludes_staging_and_db() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let root = src.path();

        // A committed blob under a per-bucket directory, plus a staging artifact and a db file.
        tokio::fs::create_dir_all(root.join("bucket-a"))
            .await
            .unwrap();
        tokio::fs::write(root.join("bucket-a").join("blob1"), b"committed")
            .await
            .unwrap();
        tokio::fs::create_dir_all(root.join(".staging"))
            .await
            .unwrap();
        tokio::fs::write(root.join(".staging").join("inflight.tmp"), b"partial")
            .await
            .unwrap();
        tokio::fs::write(root.join("cairn.db"), b"db")
            .await
            .unwrap();
        tokio::fs::write(root.join("cairn.db-wal"), b"wal")
            .await
            .unwrap();

        let copied = copy_blob_tree(root, dst.path()).await.unwrap();

        assert!(dst.path().join("bucket-a").join("blob1").exists());
        assert_eq!(
            tokio::fs::read(dst.path().join("bucket-a").join("blob1"))
                .await
                .unwrap(),
            b"committed"
        );
        assert!(!dst.path().join(".staging").exists(), "staging excluded");
        assert!(!dst.path().join("cairn.db").exists(), "db excluded");
        assert!(!dst.path().join("cairn.db-wal").exists(), "wal excluded");
        assert_eq!(copied, 1, "only the one bucket directory is copied");
    }

    /// A backup of the blob tree, restored into a fresh data dir, reproduces every committed blob
    /// and nothing from the staging area (the core of the 31.4 round-trip).
    #[tokio::test]
    async fn backup_restore_blob_tree_round_trips() {
        let src = tempfile::tempdir().unwrap();
        let snap = tempfile::tempdir().unwrap();
        let restored = tempfile::tempdir().unwrap();

        tokio::fs::create_dir_all(src.path().join("b1"))
            .await
            .unwrap();
        tokio::fs::write(src.path().join("b1").join("x"), b"one")
            .await
            .unwrap();
        tokio::fs::create_dir_all(src.path().join("b2/sub"))
            .await
            .unwrap();
        tokio::fs::write(src.path().join("b2/sub/y"), b"two")
            .await
            .unwrap();
        tokio::fs::create_dir_all(src.path().join(".staging"))
            .await
            .unwrap();
        tokio::fs::write(src.path().join(".staging/tmp"), b"junk")
            .await
            .unwrap();

        copy_blob_tree(src.path(), snap.path()).await.unwrap();
        copy_blob_tree(snap.path(), restored.path()).await.unwrap();

        assert_eq!(
            tokio::fs::read(restored.path().join("b1/x")).await.unwrap(),
            b"one"
        );
        assert_eq!(
            tokio::fs::read(restored.path().join("b2/sub/y"))
                .await
                .unwrap(),
            b"two"
        );
        assert!(!restored.path().join(".staging").exists());
    }

    /// Opening the store runs migrations; the schema version is then a positive integer.
    #[test]
    fn migrate_reports_positive_schema_version() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("cairn.db");
        let _store = cairn_meta::open(&db, &cairn_meta::OpenOptions::default()).unwrap();
        let v = schema_version(&db).unwrap();
        assert!(v >= 1, "migrations should have advanced the schema version");
    }
}
