//! [`TursoDriver`]: the [`AsyncSqlDriver`] implemented over the embedded `turso` crate — the
//! pure-Rust SQLite rewrite (a beta engine). Each driver owns one `turso::Connection`; the store
//! holds one writer driver plus a pool of reader drivers, all connected from the same
//! `turso::Database` so they see one database, exactly like the libSQL driver (and the rusqlite
//! store's one write connection + r2d2 WAL read pool).
//!
//! The shape is intentionally identical to [`crate::libsql_driver::LibsqlDriver`]; only the
//! engine bindings differ. The apply/writer/read/store/range layers are engine-agnostic (written
//! against the [`AsyncSqlDriver`] seam and the driver [`Value`]/[`Row`] model) and are reused
//! unchanged.

use crate::driver::{AsyncSqlDriver, Row, Value};
use async_trait::async_trait;
use cairn_types::MetaError;
use turso::{Connection, Value as TValue};

/// One Turso connection behind the async driver seam.
pub struct TursoDriver {
    conn: Connection,
}

impl std::fmt::Debug for TursoDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TursoDriver").finish_non_exhaustive()
    }
}

impl TursoDriver {
    /// Wrap an already-connected Turso connection.
    #[must_use]
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }
}

/// Map a Turso error to a domain metadata error, surfacing constraint violations as the typed
/// [`MetaError::Conflict`] so callers map them exactly as the rusqlite store does (`engine_err`).
/// Turso reports a constraint failure with the dedicated [`turso::Error::Constraint`] variant
/// (there is no extended numeric code to inspect, unlike libSQL's `SqliteFailure`).
fn map_err(e: turso::Error) -> MetaError {
    match e {
        turso::Error::Constraint(_) => MetaError::Conflict,
        other => MetaError::Engine(other.to_string()),
    }
}

/// Convert a driver [`Value`] into a Turso value for binding.
fn to_turso(v: Value) -> TValue {
    match v {
        Value::Null => TValue::Null,
        Value::Int(n) => TValue::Integer(n),
        Value::Real(r) => TValue::Real(r),
        Value::Text(s) => TValue::Text(s),
        Value::Blob(b) => TValue::Blob(b),
    }
}

/// Convert a Turso cell back into a driver [`Value`].
fn from_turso(v: TValue) -> Value {
    match v {
        TValue::Null => Value::Null,
        TValue::Integer(n) => Value::Int(n),
        TValue::Real(r) => Value::Real(r),
        TValue::Text(s) => Value::Text(s),
        TValue::Blob(b) => Value::Blob(b),
    }
}

#[async_trait]
impl AsyncSqlDriver for TursoDriver {
    async fn execute(&self, sql: &str, params: Vec<Value>) -> Result<u64, MetaError> {
        let params: Vec<TValue> = params.into_iter().map(to_turso).collect();
        self.conn.execute(sql, params).await.map_err(map_err)
    }

    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Row>, MetaError> {
        let params: Vec<TValue> = params.into_iter().map(to_turso).collect();
        let mut rows = self.conn.query(sql, params).await.map_err(map_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(map_err)? {
            let n = row.column_count();
            let mut cells = Vec::with_capacity(n);
            for i in 0..n {
                cells.push(from_turso(row.get_value(i).map_err(map_err)?));
            }
            out.push(Row::new(cells));
        }
        Ok(out)
    }

    async fn execute_batch(&self, sql: &str) -> Result<(), MetaError> {
        self.conn.execute_batch(sql).await.map_err(map_err)?;
        Ok(())
    }
}
