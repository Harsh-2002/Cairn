//! `cairn-meta` — the SQLite [`MetadataStore`] implementation: one serialized,
//! group-committing writer plus a pool of read-only WAL connections (ARCH §7.2, §7.3, §11).
//! The metadata commit is the single linearization point of every mutation.

#![forbid(unsafe_code)]

mod apply;
mod model;
mod range;
mod schema;
mod store;
mod writer;

use cairn_types::MetaError;
use cairn_types::id::{StoragePath, UploadId};
use cairn_types::traits::ReconcileOracle;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Connection;
use std::path::Path;
use std::time::Duration;

pub use range::{prefix_upper_bound, successor};
pub use store::SqliteMetadataStore;
pub use writer::{WalCheckpointStats, Writer};

/// Tuning knobs for opening the store (ARCH §28).
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// `true` => `PRAGMA synchronous=FULL` (durable against power loss); `false` => `NORMAL`.
    pub synchronous_full: bool,
    /// Number of read-only WAL connections (≈ core count).
    pub read_pool_size: u32,
    /// Optional group-commit linger to enlarge batches under bursty load.
    pub group_commit_linger: Option<Duration>,
    /// Busy timeout as defense in depth (the single-writer design makes contention rare).
    pub busy_timeout_ms: u64,
    /// `mmap_size` bytes for read connections.
    pub mmap_bytes: i64,
    /// Negative => KiB of page cache; positive => number of pages (SQLite convention).
    pub cache_size: i64,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            synchronous_full: true,
            read_pool_size: 8,
            group_commit_linger: None,
            busy_timeout_ms: 5000,
            mmap_bytes: 256 * 1024 * 1024,
            cache_size: -64 * 1024, // 64 MiB
        }
    }
}

fn apply_common_pragmas(conn: &Connection, opts: &OpenOptions) -> rusqlite::Result<()> {
    conn.busy_timeout(Duration::from_millis(opts.busy_timeout_ms))?;
    conn.execute_batch(&format!(
        "PRAGMA foreign_keys=ON;
         PRAGMA mmap_size={};
         PRAGMA cache_size={};",
        opts.mmap_bytes, opts.cache_size
    ))?;
    Ok(())
}

/// Open (creating if absent) the metadata store at `db_path`, running migrations on the write
/// connection before returning. The parent directory must exist.
///
/// # Errors
/// Returns a [`MetaError`] if the database cannot be opened, configured, or migrated.
pub fn open(db_path: &Path, opts: &OpenOptions) -> Result<SqliteMetadataStore, MetaError> {
    let map = |e: rusqlite::Error| MetaError::Engine(e.to_string());

    // The single write connection, owned by the writer thread.
    let write_conn = Connection::open(db_path).map_err(map)?;
    write_conn
        .execute_batch(&format!(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous={};",
            if opts.synchronous_full {
                "FULL"
            } else {
                "NORMAL"
            }
        ))
        .map_err(map)?;
    apply_common_pragmas(&write_conn, opts).map_err(map)?;
    schema::run_migrations(&write_conn).map_err(map)?;
    let writer = Writer::spawn(write_conn, opts.group_commit_linger);

    // The read pool: WAL snapshot readers, query-only.
    let opts_for_init = opts.clone();
    let manager = SqliteConnectionManager::file(db_path).with_init(move |c| {
        c.execute_batch("PRAGMA query_only=ON;")?;
        apply_common_pragmas(c, &opts_for_init)?;
        Ok(())
    });
    let pool = Pool::builder()
        .max_size(opts.read_pool_size)
        .build(manager)
        .map_err(|e| MetaError::Engine(e.to_string()))?;

    Ok(SqliteMetadataStore {
        writer,
        pool,
        db_path: Some(db_path.to_owned()),
    })
}

/// Open an in-memory store (shared cache) for tests.
///
/// # Errors
/// Returns a [`MetaError`] on failure.
pub fn open_in_memory() -> Result<SqliteMetadataStore, MetaError> {
    // A uniquely-named shared-cache in-memory DB so the write conn and read pool see the same
    // data. Randomised to isolate concurrent tests.
    let name = format!(
        "file:cairn-mem-{}?mode=memory&cache=shared",
        uuid::Uuid::new_v4().simple()
    );
    let map = |e: rusqlite::Error| MetaError::Engine(e.to_string());
    let flags = rusqlite::OpenFlags::default() | rusqlite::OpenFlags::SQLITE_OPEN_URI;

    let write_conn = Connection::open_with_flags(&name, flags).map_err(map)?;
    write_conn
        .execute_batch("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;")
        .map_err(map)?;
    schema::run_migrations(&write_conn).map_err(map)?;
    let writer = Writer::spawn(write_conn, None);

    let manager = SqliteConnectionManager::file(&name)
        .with_flags(flags)
        .with_init(|c| c.execute_batch("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;"));
    let pool = Pool::builder()
        .max_size(4)
        .build(manager)
        .map_err(|e| MetaError::Engine(e.to_string()))?;
    Ok(SqliteMetadataStore {
        writer,
        pool,
        db_path: None,
    })
}

impl SqliteMetadataStore {
    /// A reconciliation oracle backed by this store, for the blob store's `reconcile`.
    #[must_use]
    pub fn reconcile_oracle(&self) -> SqliteReconcileOracle {
        SqliteReconcileOracle {
            pool: self.pool.clone(),
        }
    }
}

/// A [`ReconcileOracle`] answering membership questions against the live metadata.
#[derive(Clone)]
pub struct SqliteReconcileOracle {
    pool: Pool<SqliteConnectionManager>,
}

impl std::fmt::Debug for SqliteReconcileOracle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteReconcileOracle")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ReconcileOracle for SqliteReconcileOracle {
    async fn live_blobs(&self, candidates: &[StoragePath]) -> Result<Vec<bool>, MetaError> {
        let paths: Vec<String> = candidates.iter().map(|p| p.as_str().to_owned()).collect();
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(|e| MetaError::Engine(e.to_string()))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT EXISTS(SELECT 1 FROM object_versions WHERE storage_path=?1)",
                )
                .map_err(|e| MetaError::Engine(e.to_string()))?;
            paths
                .iter()
                .map(|p| {
                    stmt.query_row([p], |r| r.get::<_, i64>(0))
                        .map(|n| n != 0)
                        .map_err(|e| MetaError::Engine(e.to_string()))
                })
                .collect()
        })
        .await
        .map_err(|e| MetaError::Engine(e.to_string()))?
    }

    async fn live_session(&self, upload: &UploadId) -> Result<bool, MetaError> {
        let id = upload.as_str().to_owned();
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(|e| MetaError::Engine(e.to_string()))?;
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM multipart_uploads WHERE id=?1)",
                [id],
                |r| r.get::<_, i64>(0),
            )
            .map(|n| n != 0)
            .map_err(|e| MetaError::Engine(e.to_string()))
        })
        .await
        .map_err(|e| MetaError::Engine(e.to_string()))?
    }
}
