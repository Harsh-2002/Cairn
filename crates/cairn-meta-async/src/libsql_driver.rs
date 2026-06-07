//! [`LibsqlDriver`]: the [`AsyncSqlDriver`] implemented over the embedded `libsql` crate. Each
//! driver owns one `libsql::Connection`; the store holds one writer driver plus a pool of reader
//! drivers, all connected from the same `libsql::Database` so they see one database (mirroring
//! the rusqlite store's one write connection + r2d2 WAL read pool).

use crate::driver::{AsyncSqlDriver, Row, Value};
use async_trait::async_trait;
use cairn_types::MetaError;
use libsql::{Connection, Value as LValue};

/// SQLite's primary result code for a constraint violation (`SQLITE_CONSTRAINT`). libSQL surfaces
/// it in `Error::SqliteFailure(code, _)`; the extended code is `19 | (sub << 8)`, so the primary
/// code is the low byte.
const SQLITE_CONSTRAINT: i32 = 19;

/// One libSQL connection behind the async driver seam.
pub struct LibsqlDriver {
    conn: Connection,
}

impl std::fmt::Debug for LibsqlDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LibsqlDriver").finish_non_exhaustive()
    }
}

impl LibsqlDriver {
    /// Wrap an already-connected libSQL connection.
    #[must_use]
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }
}

/// Map a libSQL error to a domain metadata error, surfacing constraint violations as the typed
/// [`MetaError::Conflict`] so callers map them exactly as the rusqlite store does (`engine_err`).
fn map_err(e: libsql::Error) -> MetaError {
    if let libsql::Error::SqliteFailure(code, _) = &e {
        if code & 0xff == SQLITE_CONSTRAINT {
            return MetaError::Conflict;
        }
    }
    MetaError::Engine(e.to_string())
}

/// Convert a driver [`Value`] into a libSQL value for binding.
fn to_libsql(v: Value) -> LValue {
    match v {
        Value::Null => LValue::Null,
        Value::Int(n) => LValue::Integer(n),
        Value::Real(r) => LValue::Real(r),
        Value::Text(s) => LValue::Text(s),
        Value::Blob(b) => LValue::Blob(b),
    }
}

/// Convert a libSQL cell back into a driver [`Value`].
fn from_libsql(v: LValue) -> Value {
    match v {
        LValue::Null => Value::Null,
        LValue::Integer(n) => Value::Int(n),
        LValue::Real(r) => Value::Real(r),
        LValue::Text(s) => Value::Text(s),
        LValue::Blob(b) => Value::Blob(b),
    }
}

#[async_trait]
impl AsyncSqlDriver for LibsqlDriver {
    async fn execute(&self, sql: &str, params: Vec<Value>) -> Result<u64, MetaError> {
        let params: Vec<LValue> = params.into_iter().map(to_libsql).collect();
        self.conn.execute(sql, params).await.map_err(map_err)
    }

    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Row>, MetaError> {
        let params: Vec<LValue> = params.into_iter().map(to_libsql).collect();
        let mut rows = self.conn.query(sql, params).await.map_err(map_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(map_err)? {
            let n = row.column_count();
            let mut cells = Vec::with_capacity(n as usize);
            for i in 0..n {
                cells.push(from_libsql(row.get_value(i).map_err(map_err)?));
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
