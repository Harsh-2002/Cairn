//! The SQLite schema and the async migration runner (ARCH §34.1). The migration SQL is copied
//! verbatim from `cairn-meta/src/schema.rs` so the libSQL store materialises a byte-for-byte
//! identical schema (the same v1..v3 sequence, including the v3 `sse_descriptor` column).
//! Migrations run on the write connection at startup, before any request is served, and are
//! recorded so they apply exactly once and in order.

use crate::driver::{AsyncSqlDriver, Value};
use cairn_types::MetaError;

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
-- user). NULL means the user has no identity policy.
ALTER TABLE users ADD COLUMN policy TEXT;
"#,
    },
    Migration {
        version: 5,
        name: "object HTTP metadata, outbox priority/lease, user quota (Wave 1 spine)",
        sql: r#"
-- Standard S3 system-metadata headers persisted per object version, echoed back on GET/HEAD.
-- All nullable: absent means the header was not supplied on the write.
ALTER TABLE object_versions ADD COLUMN content_encoding TEXT;
ALTER TABLE object_versions ADD COLUMN cache_control TEXT;
ALTER TABLE object_versions ADD COLUMN content_disposition TEXT;
ALTER TABLE object_versions ADD COLUMN content_language TEXT;
ALTER TABLE object_versions ADD COLUMN expires TEXT;

-- Replication-outbox scheduling: a priority (higher first) and a claim lease. The status column
-- has no CHECK constraint, so an atomic claim can mark an entry 'claimed' with a lease_until that
-- lets a stalled lease be reclaimed once it expires.
ALTER TABLE replication_outbox ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
ALTER TABLE replication_outbox ADD COLUMN lease_until INTEGER;

-- An optional per-user byte quota. NULL means unlimited.
ALTER TABLE users ADD COLUMN quota_bytes INTEGER;
"#,
    },
    Migration {
        version: 6,
        name: "replication outbox target ARN (per-entry routing)",
        sql: r#"
-- The remote-target ARN an outbox entry ships to, stamped at enqueue from the matching rule so
-- drain-time routing is a pure per-entry lookup (multi-target buckets route correctly, and a later
-- rule edit cannot misroute already-queued entries). NULL routes via the legacy env single target.
ALTER TABLE replication_outbox ADD COLUMN target_arn TEXT;
"#,
    },
    Migration {
        version: 7,
        name: "object share tokens (persistent public sharing)",
        sql: r#"
-- Persistent, revocable, optionally-forever object-share tokens (ARCH §15.8). The opaque token is
-- the bearer capability served at GET /p/{token}; revoke flips revoked_at with no global key
-- rotation. version_id NULL follows the current version; expires_at NULL is a forever share.
CREATE TABLE object_shares (
    token        TEXT PRIMARY KEY,
    bucket_name  TEXT NOT NULL,
    key          TEXT NOT NULL,
    version_id   TEXT,
    expires_at   INTEGER,
    disposition  TEXT NOT NULL DEFAULT 'inline',
    filename     TEXT,
    created_by   TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    revoked_at   INTEGER
);
CREATE INDEX idx_object_shares_bucket_key ON object_shares (bucket_name, key);
CREATE INDEX idx_object_shares_created_by ON object_shares (created_by);
"#,
    },
];

/// Run all pending migrations on the write driver, recording each as applied. Each migration is
/// wrapped in its own transaction (begin/commit), matching the rusqlite runner's
/// `unchecked_transaction` per migration.
pub async fn run_migrations(driver: &dyn AsyncSqlDriver) -> Result<(), MetaError> {
    driver
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version    INTEGER PRIMARY KEY,
                name       TEXT NOT NULL,
                applied_at INTEGER NOT NULL
            );",
        )
        .await?;
    let applied: i64 = driver
        .query(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            vec![],
        )
        .await?
        .first()
        .map_or(0, |r| r.get_i64(0));
    for m in MIGRATIONS {
        if m.version <= applied {
            continue;
        }
        driver.execute_batch("BEGIN").await?;
        match apply_migration(driver, m).await {
            Ok(()) => {
                driver.execute_batch("COMMIT").await?;
                tracing::info!(version = m.version, name = m.name, "applied migration");
            }
            Err(e) => {
                let _ = driver.execute_batch("ROLLBACK").await;
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Apply one migration's DDL and record it, inside the caller's open transaction.
async fn apply_migration(driver: &dyn AsyncSqlDriver, m: &Migration) -> Result<(), MetaError> {
    driver.execute_batch(m.sql).await?;
    driver
        .execute(
            "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
            vec![
                Value::Int(m.version),
                Value::Text(m.name.to_owned()),
                Value::Int(now_millis()),
            ],
        )
        .await?;
    Ok(())
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
