//! `cairn-meta-async` — an async [`MetadataStore`] implementation over the embedded **libSQL**
//! Rust driver (the mature SQLite-compatible fork). It reproduces `cairn-meta`'s behaviour
//! exactly — the same v1..v3 schema/migrations, the same `Mutation`->SQL `apply` with
//! per-mutation savepoint isolation and in-transaction precondition/quota checks, the same
//! half-open range-seek listing — but the writer is an **async** single group-committing task
//! and reads run async queries directly, all over libSQL's async `Connection` (ARCH 7.2/7.3,
//! 11). `cairn-meta` is left untouched; this is a parallel, additive backend behind the same
//! [`MetadataStore`] trait.
//!
//! libSQL is async: its `Connection::execute`/`query`/`execute_batch` are `async fn`s. The
//! [`AsyncSqlDriver`] seam captures the minimal surface the apply/writer/read logic needs, and
//! [`LibsqlDriver`] implements it over `libsql`.

#![forbid(unsafe_code)]

mod apply;
mod driver;
mod libsql_driver;
mod model;
mod range;
mod schema;
mod store;
mod turso_driver;
mod writer;

use cairn_types::MetaError;
use driver::AsyncSqlDriver;
use libsql::{Builder, Connection, Database};
use libsql_driver::LibsqlDriver;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use store::ReadPool;
use turso_driver::TursoDriver;
use writer::Writer;

pub use driver::{AsyncSqlDriver as Driver, Row, Value};
pub use libsql_driver::LibsqlDriver as RawLibsqlDriver;
pub use range::{prefix_upper_bound, successor};
pub use store::{AsyncMetadataStore, AsyncReconcileOracle};
pub use turso_driver::TursoDriver as RawTursoDriver;

/// The libSQL incarnation of the engine-agnostic [`AsyncMetadataStore`]. Opened by
/// [`open_libsql`]/[`open_libsql_in_memory`].
pub type LibsqlMetadataStore = AsyncMetadataStore;
/// The libSQL incarnation of the engine-agnostic [`AsyncReconcileOracle`].
pub type LibsqlReconcileOracle = AsyncReconcileOracle;
/// The Turso incarnation of the engine-agnostic [`AsyncMetadataStore`]. Opened by
/// [`open_turso`]/[`open_turso_in_memory`].
pub type TursoMetadataStore = AsyncMetadataStore;
/// The Turso incarnation of the engine-agnostic [`AsyncReconcileOracle`].
pub type TursoReconcileOracle = AsyncReconcileOracle;

/// Tuning knobs for opening the store (ARCH 28), mirroring `cairn-meta::OpenOptions`.
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
    /// `mmap_size` bytes for connections.
    pub mmap_bytes: i64,
    /// Negative => KiB of page cache; positive => number of pages (SQLite convention).
    pub cache_size: i64,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            // Throughput posture mirroring cairn-meta: WAL + NORMAL (no per-commit fsync, never
            // corrupts) by default; no linger (nothing to amortize under NORMAL). FULL is opt-in.
            synchronous_full: false,
            read_pool_size: 8,
            group_commit_linger: None,
            busy_timeout_ms: 5000,
            mmap_bytes: 256 * 1024 * 1024,
            cache_size: -64 * 1024, // 64 MiB
        }
    }
}

/// Apply the pragmas every connection shares (foreign keys, temp-store, busy timeout, mmap, cache).
async fn apply_common_pragmas(conn: &Connection, opts: &OpenOptions) -> Result<(), MetaError> {
    conn.busy_timeout(Duration::from_millis(opts.busy_timeout_ms))
        .map_err(|e| MetaError::Engine(e.to_string()))?;
    conn.execute_batch(&format!(
        "PRAGMA foreign_keys=ON;
         PRAGMA temp_store=MEMORY;
         PRAGMA mmap_size={};
         PRAGMA cache_size={};",
        opts.mmap_bytes, opts.cache_size
    ))
    .await
    .map_err(|e| MetaError::Engine(e.to_string()))?;
    Ok(())
}

/// Open (creating if absent) the libSQL metadata store at `db_path`, running migrations on the
/// write connection before returning. The parent directory must exist.
///
/// # Errors
/// Returns a [`MetaError`] if the database cannot be opened, configured, or migrated.
pub async fn open_libsql(
    db_path: &Path,
    opts: &OpenOptions,
) -> Result<LibsqlMetadataStore, MetaError> {
    let map = |e: libsql::Error| MetaError::Engine(e.to_string());

    // A single libSQL Database handle over the file; every connection opens the same file in WAL
    // mode (the rusqlite store likewise opens one write connection + an r2d2 read pool on the file).
    let db = Builder::new_local(db_path).build().await.map_err(map)?;

    // The single write connection, owned by the writer task. Mirrors the rusqlite store:
    // wal_autocheckpoint=0 hands all checkpointing to the background TRUNCATE loop (gated by the
    // server's W3 validation), journal_size_limit caps the WAL footprint, analysis_limit bounds the
    // periodic optimize.
    let write_conn = db.connect().map_err(map)?;
    write_conn
        .execute_batch(&format!(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous={};
             PRAGMA wal_autocheckpoint=0;
             PRAGMA journal_size_limit={};
             PRAGMA analysis_limit=400;",
            if opts.synchronous_full {
                "FULL"
            } else {
                "NORMAL"
            },
            64 * 1024 * 1024,
        ))
        .await
        .map_err(map)?;
    apply_common_pragmas(&write_conn, opts).await?;
    let write_driver: Arc<dyn AsyncSqlDriver> = Arc::new(LibsqlDriver::new(write_conn));
    schema::run_migrations(write_driver.as_ref()).await?;
    let writer = Writer::spawn(write_driver, opts.group_commit_linger);

    // The read pool: WAL snapshot readers, query-only.
    let mut readers: Vec<Arc<dyn AsyncSqlDriver>> = Vec::new();
    for _ in 0..opts.read_pool_size.max(1) {
        let conn = db.connect().map_err(map)?;
        conn.execute_batch("PRAGMA query_only=ON;")
            .await
            .map_err(map)?;
        apply_common_pragmas(&conn, opts).await?;
        readers.push(Arc::new(LibsqlDriver::new(conn)));
    }

    // Keep the Database handle alive for the store's lifetime by parking it in the pool guard.
    Ok(LibsqlMetadataStore::new(
        writer,
        ReadPool::new_with_keepalive(readers, Box::new(db)),
    ))
}

/// Open an in-memory store for tests.
///
/// Uses a uniquely-named shared-cache in-memory URI so the write connection and the read pool see
/// one database (the bundled libSQL SQLite is compiled with `SQLITE_USE_URI`, so the URI filename
/// is honoured), mirroring `cairn-meta::open_in_memory`.
///
/// # Errors
/// Returns a [`MetaError`] on failure.
pub async fn open_libsql_in_memory() -> Result<LibsqlMetadataStore, MetaError> {
    let map = |e: libsql::Error| MetaError::Engine(e.to_string());
    let name = format!(
        "file:cairn-libsql-mem-{}?mode=memory&cache=shared",
        uuid::Uuid::new_v4().simple()
    );

    // One Database over the shared-cache in-memory URI; connections from it share the same memory.
    #[allow(deprecated)]
    let db = Database::open(name).map_err(map)?;

    let write_conn = db.connect().map_err(map)?;
    write_conn
        .execute_batch("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;")
        .await
        .map_err(map)?;
    let write_driver: Arc<dyn AsyncSqlDriver> = Arc::new(LibsqlDriver::new(write_conn));
    schema::run_migrations(write_driver.as_ref()).await?;
    let writer = Writer::spawn(write_driver, None);

    let mut readers: Vec<Arc<dyn AsyncSqlDriver>> = Vec::new();
    for _ in 0..4 {
        let conn = db.connect().map_err(map)?;
        conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;")
            .await
            .map_err(map)?;
        readers.push(Arc::new(LibsqlDriver::new(conn)));
    }

    Ok(LibsqlMetadataStore::new(
        writer,
        ReadPool::new_with_keepalive(readers, Box::new(db)),
    ))
}

// ----------------------------------------------------------------------------------------------
// Turso backend (the pure-Rust SQLite rewrite; beta engine).
// ----------------------------------------------------------------------------------------------

/// Apply the per-connection settings every Turso connection shares. Turso (beta) does not yet
/// honour the full SQLite PRAGMA surface, so this is intentionally minimal and best-effort: the
/// busy timeout (a Turso connection method, not a PRAGMA) and a short batch of throughput PRAGMAs
/// whose individual failures are ignored so a PRAGMA the beta engine has not implemented cannot
/// abort startup. `synchronous` and `temp_store` are applied best-effort; `wal_autocheckpoint`
/// and `journal_size_limit` are deliberately NOT set — Turso self-manages its native WAL and has
/// no external checkpointer, so those are meaningless (and the W3 guardrail does not apply).
async fn apply_turso_pragmas(
    conn: &turso::Connection,
    opts: &OpenOptions,
) -> Result<(), MetaError> {
    conn.busy_timeout(std::time::Duration::from_millis(opts.busy_timeout_ms))
        .map_err(|e| MetaError::Engine(e.to_string()))?;
    // Best-effort: ignore an error so a PRAGMA the beta engine does not implement does not abort
    // startup. Turso enforces foreign keys for the multipart-parts cascade the store relies on.
    let sync = if opts.synchronous_full {
        "FULL"
    } else {
        "NORMAL"
    };
    let _ = conn.execute_batch("PRAGMA foreign_keys=ON;").await;
    let _ = conn
        .execute_batch(&format!("PRAGMA synchronous={sync};"))
        .await;
    let _ = conn.execute_batch("PRAGMA temp_store=MEMORY;").await;
    Ok(())
}

/// Open (creating if absent) the Turso metadata store at `db_path`, running migrations on the
/// write connection before returning. The parent directory must exist. Turso self-manages its WAL,
/// so unlike the rusqlite store there is no external WAL checkpointer for this backend.
///
/// # Errors
/// Returns a [`MetaError`] if the database cannot be opened, configured, or migrated.
pub async fn open_turso(
    db_path: &Path,
    opts: &OpenOptions,
) -> Result<TursoMetadataStore, MetaError> {
    let map = |e: turso::Error| MetaError::Engine(e.to_string());
    let path = db_path
        .to_str()
        .ok_or_else(|| MetaError::Engine("db_path is not valid UTF-8".to_owned()))?;

    // One Turso Database handle over the file; every connection opens the same file.
    let db = turso::Builder::new_local(path).build().await.map_err(map)?;

    // The single write connection, owned by the writer task.
    let write_conn = db.connect().map_err(map)?;
    apply_turso_pragmas(&write_conn, opts).await?;
    let write_driver: Arc<dyn AsyncSqlDriver> = Arc::new(TursoDriver::new(write_conn));
    schema::run_migrations(write_driver.as_ref()).await?;
    let writer = Writer::spawn(write_driver, opts.group_commit_linger);

    // The read pool: query-only WAL snapshot readers from the same Database handle.
    let mut readers: Vec<Arc<dyn AsyncSqlDriver>> = Vec::new();
    for _ in 0..opts.read_pool_size.max(1) {
        let conn = db.connect().map_err(map)?;
        apply_turso_pragmas(&conn, opts).await?;
        readers.push(Arc::new(TursoDriver::new(conn)));
    }

    Ok(TursoMetadataStore::new(
        writer,
        ReadPool::new_with_keepalive(readers, Box::new(db)),
    ))
}

/// Open an in-memory Turso store for tests.
///
/// All connections come from a single `:memory:` [`turso::Database`] handle; connections from one
/// Turso Database share the same in-memory database (asserted by the parity gate), mirroring
/// `cairn-meta::open_in_memory` and [`open_libsql_in_memory`].
///
/// # Errors
/// Returns a [`MetaError`] on failure.
pub async fn open_turso_in_memory() -> Result<TursoMetadataStore, MetaError> {
    let map = |e: turso::Error| MetaError::Engine(e.to_string());

    let db = turso::Builder::new_local(":memory:")
        .build()
        .await
        .map_err(map)?;

    let write_conn = db.connect().map_err(map)?;
    write_conn
        .busy_timeout(std::time::Duration::from_millis(5000))
        .map_err(map)?;
    let _ = write_conn.execute_batch("PRAGMA foreign_keys=ON;").await;
    let write_driver: Arc<dyn AsyncSqlDriver> = Arc::new(TursoDriver::new(write_conn));
    schema::run_migrations(write_driver.as_ref()).await?;
    let writer = Writer::spawn(write_driver, None);

    let mut readers: Vec<Arc<dyn AsyncSqlDriver>> = Vec::new();
    for _ in 0..4 {
        let conn = db.connect().map_err(map)?;
        conn.busy_timeout(std::time::Duration::from_millis(5000))
            .map_err(map)?;
        let _ = conn.execute_batch("PRAGMA foreign_keys=ON;").await;
        readers.push(Arc::new(TursoDriver::new(conn)));
    }

    Ok(TursoMetadataStore::new(
        writer,
        ReadPool::new_with_keepalive(readers, Box::new(db)),
    ))
}
