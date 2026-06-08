//! The SQLite schema and the migration runner (ARCH §34.1). Migrations run on the write
//! connection at startup, before any request is served, and are recorded so they apply
//! exactly once and in order.

use rusqlite::Connection;

/// An ordered migration: a monotonically increasing version, a name, and its SQL.
struct Migration {
    version: i64,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial schema",
        sql: r#"
CREATE TABLE users (
    id                      TEXT PRIMARY KEY,
    display_name            TEXT NOT NULL,
    access_key_id           TEXT NOT NULL UNIQUE,
    secret_hash             TEXT NOT NULL,
    sigv4_access_key_id     TEXT UNIQUE,
    sigv4_secret_ciphertext BLOB,
    sigv4_secret_nonce      BLOB,
    role                    TEXT NOT NULL CHECK (role IN ('administrator','member')),
    is_active               INTEGER NOT NULL DEFAULT 1,
    created_at              INTEGER NOT NULL,
    updated_at              INTEGER NOT NULL
);

CREATE TABLE buckets (
    name             TEXT PRIMARY KEY,
    owner_id         TEXT NOT NULL,
    created_at       INTEGER NOT NULL,
    versioning_state TEXT NOT NULL CHECK (versioning_state IN ('unversioned','enabled','suspended')),
    ownership_mode   TEXT NOT NULL,
    region           TEXT NOT NULL,
    compression      TEXT
);

CREATE TABLE bucket_config (
    bucket_name TEXT NOT NULL,
    aspect      TEXT NOT NULL,
    doc         TEXT NOT NULL,
    PRIMARY KEY (bucket_name, aspect)
);

CREATE TABLE account_config (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

CREATE TABLE object_versions (
    id                 TEXT PRIMARY KEY,
    bucket_name        TEXT NOT NULL,
    key                TEXT NOT NULL,
    version_id         TEXT NOT NULL,
    is_latest          INTEGER NOT NULL,
    is_delete_marker   INTEGER NOT NULL,
    size_logical       INTEGER NOT NULL,
    size_physical      INTEGER NOT NULL,
    etag               TEXT NOT NULL,
    content_type       TEXT NOT NULL,
    storage_path       TEXT,
    compression        TEXT NOT NULL,
    storage_class      TEXT NOT NULL,
    cold_locator       TEXT,
    owner_id           TEXT NOT NULL,
    user_metadata      TEXT NOT NULL,
    acl                TEXT,
    checksums          TEXT NOT NULL,
    replication_status TEXT,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL,
    UNIQUE (bucket_name, key, version_id)
);

-- The half-open range-seek index for current-version lookup and version listing.
CREATE INDEX idx_object_versions_bkv ON object_versions (bucket_name, key, version_id);
CREATE INDEX idx_object_versions_latest ON object_versions (bucket_name, key, is_latest);

CREATE TABLE object_tags (
    bucket_name TEXT NOT NULL,
    key         TEXT NOT NULL,
    version_id  TEXT NOT NULL,
    tag_key     TEXT NOT NULL,
    tag_value   TEXT NOT NULL,
    PRIMARY KEY (bucket_name, key, version_id, tag_key)
);

CREATE TABLE multipart_uploads (
    id            TEXT PRIMARY KEY,
    bucket_name   TEXT NOT NULL,
    key           TEXT NOT NULL,
    content_type  TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('active','completing','aborted')),
    owner_id      TEXT NOT NULL,
    intended_acl  TEXT,
    user_metadata TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);
CREATE INDEX idx_multipart_status_updated ON multipart_uploads (status, updated_at);

CREATE TABLE multipart_parts (
    upload_id    TEXT NOT NULL,
    part_number  INTEGER NOT NULL,
    size         INTEGER NOT NULL,
    etag         TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    checksum     TEXT,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads (id) ON DELETE CASCADE
);

CREATE TABLE replication_outbox (
    id              TEXT PRIMARY KEY,
    bucket_name     TEXT NOT NULL,
    key             TEXT NOT NULL,
    version_id      TEXT NOT NULL,
    operation       TEXT NOT NULL,
    rule_id         TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL,
    status          TEXT NOT NULL,
    last_error      TEXT
);
CREATE INDEX idx_outbox_status_next ON replication_outbox (status, next_attempt_at);

CREATE TABLE activity (
    id     TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    bucket TEXT,
    key    TEXT,
    size   INTEGER,
    etag   TEXT,
    actor  TEXT,
    at     INTEGER NOT NULL
);
CREATE INDEX idx_activity_at ON activity (at);
"#,
    },
    Migration {
        version: 2,
        name: "storage_path index, bucket quota, schema-name alignment (ARCH §8/§27.5/§34)",
        sql: r#"
-- F-8: a seek index over storage_path so reconcile's per-batch membership lookups and
-- enumerate_storage_paths range-seek instead of full-scanning object_versions, and so the
-- multipart parts table's paths are likewise seekable.
CREATE INDEX idx_object_versions_storage_path ON object_versions (storage_path);
CREATE INDEX idx_multipart_parts_storage_path ON multipart_parts (storage_path);

-- The (bucket_name, key, version_id) UNIQUE constraint already materialises an auto-index that
-- serves current-version lookup and version listing (ARCH §34.2), so this explicit duplicate is
-- redundant dead weight; drop it.
DROP INDEX idx_object_versions_bkv;

-- §27.5/§28.2: an optional per-bucket byte quota enforced inside the commit transaction.
-- NULL means unlimited.
ALTER TABLE buckets ADD COLUMN quota_bytes INTEGER;

-- §34.1/§34: the spec names this column compression_policy; the v1 column was compression.
ALTER TABLE buckets RENAME COLUMN compression TO compression_policy;
"#,
    },
    Migration {
        version: 3,
        name: "SSE-S3 object encryption descriptor (ARCH §27)",
        sql: r#"
-- §27 SSE-S3: a nullable per-version descriptor for server-side-encrypted object data. The JSON
-- document is {alg, wrapped_dek_b64, nonce_b64}: the algorithm, the data-encryption key sealed
-- under the master key (base64), and the wrapping nonce (base64). NULL means the object's data is
-- stored unencrypted. The raw DEK is never persisted; only its wrapped form lives here.
ALTER TABLE object_versions ADD COLUMN sse_descriptor TEXT;
"#,
    },
    Migration {
        version: 4,
        name: "per-user identity policy (ARCH §15 / user-centric authz)",
        sql: r#"
-- An AWS-IAM-style identity policy attached to a user, evaluated for that user's S3 requests in
-- union with bucket policy/ACL. The JSON document is a Principal-less policy (the principal IS this
-- user). NULL means the user has no identity policy (a non-admin then has no granted S3 access).
ALTER TABLE users ADD COLUMN policy TEXT;
"#,
    },
];

/// Run all pending migrations on the write connection, recording each as applied.
pub fn run_migrations(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            applied_at INTEGER NOT NULL
        );",
    )?;
    let applied: i64 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |r| r.get(0),
    )?;
    for m in MIGRATIONS {
        if m.version <= applied {
            continue;
        }
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(m.sql)?;
        tx.execute(
            "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![m.version, m.name, now_millis()],
        )?;
        tx.commit()?;
        tracing::info!(version = m.version, name = m.name, "applied migration");
    }
    Ok(())
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_apply_once_and_are_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        run_migrations(&conn).unwrap(); // second run is a no-op
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, MIGRATIONS.len() as i64);
        // a known table exists
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='object_versions'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
        conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name=?2",
            rusqlite::params![table, column],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    fn index_exists(conn: &Connection, name: &str) -> bool {
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
            rusqlite::params![name],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    #[test]
    fn migration_v2_renames_quota_and_index_changes() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();

        // The compression column was renamed to the spec name (ARCH §34.1) and the quota column
        // was added (ARCH §27.5).
        assert!(column_exists(&conn, "buckets", "compression_policy"));
        assert!(!column_exists(&conn, "buckets", "compression"));
        assert!(column_exists(&conn, "buckets", "quota_bytes"));

        // The storage_path seek indexes were created (F-8) and the redundant bkv index dropped.
        assert!(index_exists(&conn, "idx_object_versions_storage_path"));
        assert!(index_exists(&conn, "idx_multipart_parts_storage_path"));
        assert!(!index_exists(&conn, "idx_object_versions_bkv"));
        // The UNIQUE-constraint auto-index still serves bkv range seeks (ARCH §34.2).
        let auto: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type='index' AND tbl_name='object_versions' AND sql IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(auto >= 1, "the UNIQUE constraint's auto-index must remain");
    }

    #[test]
    fn migration_v3_adds_sse_descriptor_column() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        // The SSE-S3 descriptor column exists and is nullable (ARCH §27).
        assert!(column_exists(&conn, "object_versions", "sse_descriptor"));
        // It defaults to NULL when not supplied on insert.
        conn.execute_batch(
            "INSERT INTO object_versions
             (id, bucket_name, key, version_id, is_latest, is_delete_marker, size_logical,
              size_physical, etag, content_type, compression, storage_class, owner_id,
              user_metadata, checksums, created_at, updated_at)
             VALUES ('i','b','k','null',1,0,0,0,'e','text/plain','\"Uncompressed\"','Standard',
                     'o','[]','[]',0,0);",
        )
        .unwrap();
        let sse: Option<String> = conn
            .query_row(
                "SELECT sse_descriptor FROM object_versions WHERE id='i'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(sse.is_none());
    }

    #[test]
    fn migration_v4_adds_user_policy_column() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        // The nullable per-user identity policy column exists (ARCH §15 / user-centric authz).
        assert!(column_exists(&conn, "users", "policy"));
        conn.execute_batch(
            "INSERT INTO users
             (id, display_name, access_key_id, secret_hash, role, is_active, created_at, updated_at)
             VALUES ('u','n','ak','h','member',1,0,0);",
        )
        .unwrap();
        let policy: Option<String> = conn
            .query_row("SELECT policy FROM users WHERE id='u'", [], |r| r.get(0))
            .unwrap();
        assert!(policy.is_none());
    }
}
