//! The Cairn server binary entrypoint. Parses configuration, initialises observability, builds
//! the engine stack, and runs the HTTP server with ordered graceful shutdown. Also carries the
//! node-local commands that operate directly on the data dir from config: `bootstrap` (mint the
//! first administrator), `integrity` (on-demand reconciliation), `migrate` (run migrations and
//! report the schema version), and `backup`/`restore` (the ARCH §31.4 consistent snapshot and its
//! inverse). The full remote-admin CLI ships as `cairn-cli` in a later wave.

#![forbid(unsafe_code)]

mod adapter;
mod background;
mod config;
mod observability;
mod server;
mod stack;
mod tls;

use clap::{Parser, Subcommand};
use config::Config;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

/// Cairn — a production-grade, S3-compatible object storage server.
#[derive(Debug, Parser)]
#[command(name = "cairn", version, about)]
struct Cli {
    /// Path to an optional TOML configuration file.
    #[arg(long, global = true, env = "CAIRN_CONFIG")]
    config: Option<PathBuf>,
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
    /// Create the first administrator into an empty store and print its credentials once.
    Bootstrap,
    /// Run reconciliation on demand (reclaim orphaned blobs); a node-local integrity check.
    Integrity,
    /// Open the store (running migrations) and report the applied schema version.
    Migrate,
    /// Take a consistent snapshot of the data dir into DIR (ARCH §31.4): checkpoint + copy the
    /// database, then copy the blob tree excluding the staging area.
    Backup {
        /// Destination directory for the snapshot (created if absent).
        dir: PathBuf,
    },
    /// Restore a snapshot from DIR into the configured data dir, then run reconciliation
    /// (ARCH §31.4): place the database and blobs, then reconcile.
    Restore {
        /// Source snapshot directory produced by `backup`.
        dir: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let cfg = match Config::load(cli.config.as_ref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e}");
            return ExitCode::from(2);
        }
    };

    match cli.command.unwrap_or(Command::Serve) {
        Command::ValidateConfig => {
            println!("configuration valid");
            ExitCode::SUCCESS
        }
        Command::Bootstrap => bootstrap(cfg),
        Command::Integrity => integrity(cfg),
        Command::Migrate => migrate(cfg),
        Command::Backup { dir } => backup(cfg, &dir),
        Command::Restore { dir } => restore(cfg, &dir),
        Command::Serve => run_server(cfg),
    }
}

fn integrity(cfg: Config) -> ExitCode {
    use cairn_types::blob::ReconcileOpts;
    use cairn_types::traits::BlobStore;

    let rt = match runtime() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async {
        let store = match cairn_meta::open(&cfg.db_path, &cairn_meta::OpenOptions::default()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to open metadata store: {e}");
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
        match blob.reconcile(&oracle, ReconcileOpts::default()).await {
            Ok(r) => {
                println!(
                    "reconciliation complete: scanned={} orphans_reclaimed={} staging_cleaned={} sessions_cleaned={} errors={}",
                    r.blobs_scanned, r.orphans_reclaimed, r.staging_cleaned, r.sessions_cleaned, r.errors
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("reconciliation failed: {e}");
                ExitCode::FAILURE
            }
        }
    })
}

/// Open the metadata store (which runs any pending migrations) and report the resulting schema
/// version. The server runs the same migrations at startup; this command is for operators who
/// prefer to migrate explicitly (ARCH §11.2, §24.2).
fn migrate(cfg: Config) -> ExitCode {
    if let Some(parent) = cfg.db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    // `open` runs migrations on the write connection before returning (ARCH §11.2). We then read
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

/// Take a consistent snapshot into `dir` (ARCH §31.4). The database is snapshotted first
/// (checkpoint to fold the WAL into the main file, then copy it), and the blob tree is copied
/// second excluding the staging area. Taking the database first guarantees the copied blob set is
/// a superset of what the snapshot references, so restore finds a blob for every row; any extra
/// blobs from writes after the snapshot are reclaimed by reconciliation on restore. The master
/// key is deliberately not part of the data dir, so it is not disclosed by the snapshot.
fn backup(cfg: Config, dir: &std::path::Path) -> ExitCode {
    let rt = match runtime() {
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

/// Restore a snapshot from `dir` into the configured data dir, then reconcile (ARCH §31.4). The
/// database and blob tree produced by `backup` are placed, and reconciliation reclaims any blobs
/// written after the snapshot was taken.
fn restore(cfg: Config, dir: &std::path::Path) -> ExitCode {
    use cairn_types::blob::ReconcileOpts;
    use cairn_types::traits::BlobStore;

    let rt = match runtime() {
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

        // 2. Reconcile: reclaim any blobs from writes after the snapshot (ARCH §31.4).
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
        match blob.reconcile(&oracle, ReconcileOpts::default()).await {
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
/// area (in-progress writes are not part of a consistent snapshot, ARCH §31.4) and any database
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

fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}

fn run_server(cfg: Config) -> ExitCode {
    observability::init_tracing(&cfg.log_level, cfg.log_format);
    let metrics = observability::init_metrics();

    // Arm the fault-injection registry from $FAILPOINTS (only in `failpoints` builds, used by the
    // crash-consistency harness). The scenario must outlive the server, so it is held here.
    #[cfg(feature = "failpoints")]
    let _fail_scenario = fail::FailScenario::setup();

    let rt = match runtime() {
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
    use cairn_types::auth::Role;
    use cairn_types::id::UserId;
    use cairn_types::meta::{Mutation, User, UserRecord};
    use cairn_types::traits::{Clock, Crypto, MetadataStore};

    let rt = match runtime() {
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

        let store = match cairn_meta::open(&cfg.db_path, &cairn_meta::OpenOptions::default()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to open metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };
        match store.count_users().await {
            Ok(0) => {}
            Ok(_) => {
                eprintln!("a user already exists; refusing to bootstrap again");
                return ExitCode::from(1);
            }
            Err(e) => {
                eprintln!("failed to query users: {e}");
                return ExitCode::FAILURE;
            }
        }

        let crypto = match stack::build_crypto(&cfg) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };
        let clock = cairn_crypto::SystemClock::new();
        let now = clock.now();

        let bearer_akid = format!("cairn_{}", uuid::Uuid::new_v4().simple());
        let bearer_secret = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let sigv4_akid = format!(
            "AKIA{}",
            &uuid::Uuid::new_v4().simple().to_string()[..16].to_uppercase()
        );
        let sigv4_secret = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );

        let sealed = match crypto.seal(sigv4_secret.as_bytes()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to seal SigV4 secret: {e}");
                return ExitCode::FAILURE;
            }
        };

        let record = UserRecord {
            user: User {
                id: UserId::generate(),
                display_name: "administrator".to_owned(),
                access_key_id: bearer_akid.clone(),
                sigv4_access_key_id: Some(sigv4_akid.clone()),
                role: Role::Administrator,
                is_active: true,
                created_at: now,
                updated_at: now,
            },
            bearer_secret_hash: cairn_auth::hash_bearer_secret(&bearer_secret),
            sigv4_secret_ciphertext: Some(sealed.ciphertext),
            sigv4_secret_nonce: Some(sealed.nonce.0),
        };

        if let Err(e) = store.submit(Mutation::CreateUser(Box::new(record))).await {
            eprintln!("failed to create administrator: {e}");
            return ExitCode::FAILURE;
        }

        println!("Administrator created. Save these credentials now — they are shown only once.\n");
        println!("  Bearer:");
        println!("    Authorization: Bearer {bearer_akid}.{bearer_secret}\n");
        println!("  SigV4 (S3 SDKs / aws-cli):");
        println!("    Access Key Id:     {sigv4_akid}");
        println!("    Secret Access Key: {sigv4_secret}");
        println!("    Region:            {}", cfg.region);
        ExitCode::SUCCESS
    })
}

#[cfg(test)]
mod tests {
    use super::{copy_blob_tree, schema_version};

    /// `copy_blob_tree` copies committed per-bucket blob directories but skips the staging area
    /// and database sidecars, so a snapshot contains only durable blobs (ARCH §31.4).
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
    /// and nothing from the staging area (the core of the §31.4 round-trip).
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
