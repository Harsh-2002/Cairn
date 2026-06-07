//! The async driver seam (`AsyncSqlDriver`) the apply/writer/read logic is written against.
//! It exposes the minimal surface those layers need — parameterized `execute`/`query`, batch
//! DDL, and transaction control (begin-immediate / savepoint / release / rollback-to / commit /
//! rollback) — over a [`Value`]-typed parameter and cell model, so the metadata logic is
//! independent of any particular SQL engine. [`crate::libsql_driver::LibsqlDriver`] is the
//! concrete libSQL implementation.

use async_trait::async_trait;
use cairn_types::MetaError;

/// A single SQL value, used both for bound parameters and for returned [`Row`] cells. Mirrors
/// the storage classes SQLite exposes.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// SQL NULL.
    Null,
    /// A 64-bit signed integer.
    Int(i64),
    /// A 64-bit float.
    Real(f64),
    /// A UTF-8 text value.
    Text(String),
    /// A byte-string value.
    Blob(Vec<u8>),
}

impl Value {
    /// Borrow the value as text, if it is text.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

/// One returned row: an index-getable vector of cells. Columns are positional in the order the
/// `SELECT` lists them, so the row mappers select explicit column lists (or rely on the stable
/// `SELECT *` order of the schema) exactly as the rusqlite store does by name.
#[derive(Debug, Clone, Default)]
pub struct Row {
    cells: Vec<Value>,
}

impl Row {
    /// Build a row from its ordered cells.
    #[must_use]
    pub fn new(cells: Vec<Value>) -> Self {
        Self { cells }
    }

    /// The number of cells.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Whether the row has no cells.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// The raw value at `idx`, or [`Value::Null`] when out of range (so an absent column reads
    /// like SQL NULL rather than panicking).
    #[must_use]
    pub fn value(&self, idx: usize) -> &Value {
        self.cells.get(idx).unwrap_or(&Value::Null)
    }

    /// An `i64` at `idx` (0 for NULL/non-integer, matching SQLite's lenient affinity for the
    /// integer columns this store reads).
    #[must_use]
    pub fn get_i64(&self, idx: usize) -> i64 {
        match self.value(idx) {
            Value::Int(n) => *n,
            Value::Real(r) => *r as i64,
            _ => 0,
        }
    }

    /// A required text column at `idx`. An absent/NULL/non-text cell maps to the empty string,
    /// which only occurs for columns this store always writes as text.
    #[must_use]
    pub fn get_text(&self, idx: usize) -> String {
        match self.value(idx) {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        }
    }

    /// An optional text column at `idx`: `None` for SQL NULL, `Some` for text.
    #[must_use]
    pub fn get_opt_text(&self, idx: usize) -> Option<String> {
        match self.value(idx) {
            Value::Text(s) => Some(s.clone()),
            _ => None,
        }
    }

    /// An optional blob column at `idx`: `None` for SQL NULL, `Some` for a byte string.
    #[must_use]
    pub fn get_opt_blob(&self, idx: usize) -> Option<Vec<u8>> {
        match self.value(idx) {
            Value::Blob(b) => Some(b.clone()),
            _ => None,
        }
    }

    /// An optional integer column at `idx`: `None` for SQL NULL, `Some` for an integer.
    #[must_use]
    pub fn get_opt_i64(&self, idx: usize) -> Option<i64> {
        match self.value(idx) {
            Value::Int(n) => Some(*n),
            Value::Real(r) => Some(*r as i64),
            _ => None,
        }
    }
}

/// The async SQL driver seam. Implementations own a single connection; concurrency is provided
/// by holding several driver instances (one per writer, a pool for readers), exactly as the
/// rusqlite store holds one write connection plus an r2d2 read pool.
#[async_trait]
pub trait AsyncSqlDriver: Send + Sync {
    /// Execute a non-query statement with positional params, returning rows affected.
    async fn execute(&self, sql: &str, params: Vec<Value>) -> Result<u64, MetaError>;

    /// Run a query with positional params, returning all rows.
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Row>, MetaError>;

    /// Run a (possibly multi-statement) batch with no params (DDL, migrations, transaction
    /// control verbs).
    async fn execute_batch(&self, sql: &str) -> Result<(), MetaError>;

    // --- transaction control (the group-commit machinery drives these directly) ---

    /// `BEGIN IMMEDIATE`.
    async fn begin_immediate(&self) -> Result<(), MetaError> {
        self.execute_batch("BEGIN IMMEDIATE").await
    }
    /// `SAVEPOINT <name>`.
    async fn savepoint(&self, name: &str) -> Result<(), MetaError> {
        self.execute_batch(&format!("SAVEPOINT {name}")).await
    }
    /// `RELEASE <name>`.
    async fn release(&self, name: &str) -> Result<(), MetaError> {
        self.execute_batch(&format!("RELEASE {name}")).await
    }
    /// `ROLLBACK TO <name>; RELEASE <name>` — undo just this savepoint and discard it.
    async fn rollback_to(&self, name: &str) -> Result<(), MetaError> {
        self.execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"))
            .await
    }
    /// `COMMIT`.
    async fn commit(&self) -> Result<(), MetaError> {
        self.execute_batch("COMMIT").await
    }
    /// `ROLLBACK`.
    async fn rollback(&self) -> Result<(), MetaError> {
        self.execute_batch("ROLLBACK").await
    }
}

/// A single-cell helper for `query_row`-style reads: the first row of a query, if any.
pub async fn query_one(
    driver: &dyn AsyncSqlDriver,
    sql: &str,
    params: Vec<Value>,
) -> Result<Option<Row>, MetaError> {
    Ok(driver.query(sql, params).await?.into_iter().next())
}
