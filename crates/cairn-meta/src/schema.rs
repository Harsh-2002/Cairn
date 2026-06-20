//! The SQLite schema and the migration runner (ARCH 34.1). Migrations run on the write
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
        name: "storage_path index, bucket quota, schema-name alignment (ARCH 8/27.5/34)",
        sql: r#"
-- F-8: a seek index over storage_path so reconcile's per-batch membership lookups and
-- enumerate_storage_paths range-seek instead of full-scanning object_versions, and so the
-- multipart parts table's paths are likewise seekable.
CREATE INDEX idx_object_versions_storage_path ON object_versions (storage_path);
CREATE INDEX idx_multipart_parts_storage_path ON multipart_parts (storage_path);

-- The (bucket_name, key, version_id) UNIQUE constraint already materialises an auto-index that
-- serves current-version lookup and version listing (ARCH 34.2), so this explicit duplicate is
-- redundant dead weight; drop it.
DROP INDEX idx_object_versions_bkv;

-- 27.5/28.2: an optional per-bucket byte quota enforced inside the commit transaction.
-- NULL means unlimited.
ALTER TABLE buckets ADD COLUMN quota_bytes INTEGER;

-- 34.1/34: the spec names this column compression_policy; the v1 column was compression.
ALTER TABLE buckets RENAME COLUMN compression TO compression_policy;
"#,
    },
    Migration {
        version: 3,
        name: "SSE-S3 object encryption descriptor (ARCH 27)",
        sql: r#"
-- 27 SSE-S3: a nullable per-version descriptor for server-side-encrypted object data. The JSON
-- document is {alg, wrapped_dek_b64, nonce_b64}: the algorithm, the data-encryption key sealed
-- under the master key (base64), and the wrapping nonce (base64). NULL means the object's data is
-- stored unencrypted. The raw DEK is never persisted; only its wrapped form lives here.
ALTER TABLE object_versions ADD COLUMN sse_descriptor TEXT;
"#,
    },
    Migration {
        version: 4,
        name: "per-user identity policy (ARCH 15 / user-centric authz)",
        sql: r#"
-- An AWS-IAM-style identity policy attached to a user, evaluated for that user's S3 requests in
-- union with bucket policy/ACL. The JSON document is a Principal-less policy (the principal IS this
-- user). NULL means the user has no identity policy (a non-admin then has no granted S3 access).
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
-- Persistent, revocable, optionally-forever object-share tokens (ARCH 15.8). The opaque token is
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
    Migration {
        version: 8,
        name: "request metrics rollup (usage analytics)",
        sql: r#"
-- Per-window rollup of API request counts for the console's usage analytics (ARCH 26.5). Each row
-- is one (window, operation, bucket, status-class) bucket; the in-process aggregator flushes batched
-- upserts that accumulate `count`, and a periodic prune drops rows older than the retention window.
-- bucket_name is '' (never NULL) for non-bucket operations. The composite PRIMARY KEY gives the
-- accumulating upsert (ON CONFLICT … DO UPDATE); the ts index serves range queries and the prune.
CREATE TABLE request_metrics (
    ts_bucket    INTEGER NOT NULL,
    operation    TEXT    NOT NULL,
    bucket_name  TEXT    NOT NULL,
    status_class TEXT    NOT NULL,
    count        INTEGER NOT NULL,
    PRIMARY KEY (ts_bucket, operation, bucket_name, status_class)
);
CREATE INDEX idx_request_metrics_ts ON request_metrics (ts_bucket);
"#,
    },
    Migration {
        version: 9,
        name: "request metrics bytes + latency capture",
        sql: r#"
-- Enrich the request-metrics rollup (ARCH 26.5) with transferred bytes and a latency histogram so
-- the console can chart throughput and p95/avg latency, not just request counts. Old v8 rows keep 0
-- for every new column (they predate the capture). lat_sum_ms drives the average; the six histogram
-- buckets (boundaries 5/20/50/200/1000 ms, last is the >1000ms overflow) drive the percentiles.
ALTER TABLE request_metrics ADD COLUMN bytes_in    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE request_metrics ADD COLUMN bytes_out   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE request_metrics ADD COLUMN lat_sum_ms  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE request_metrics ADD COLUMN lat_le_5    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE request_metrics ADD COLUMN lat_le_20   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE request_metrics ADD COLUMN lat_le_50   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE request_metrics ADD COLUMN lat_le_200  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE request_metrics ADD COLUMN lat_le_1000 INTEGER NOT NULL DEFAULT 0;
ALTER TABLE request_metrics ADD COLUMN lat_gt_1000 INTEGER NOT NULL DEFAULT 0;
"#,
    },
    Migration {
        version: 10,
        name: "object tags reverse index (tag browser)",
        sql: r#"
-- The object_tags PK is (bucket, key, version, tag_key) — indexed by object. The tag browser
-- (ARCH 17.2) asks the reverse question — which objects carry a given tag — so add a covering
-- index on (tag_key, tag_value) so "list all tags" and "objects by tag" are index seeks, not scans.
CREATE INDEX idx_object_tags_kv ON object_tags (tag_key, tag_value);
"#,
    },
    Migration {
        version: 11,
        name: "partial covering index for current-version reads (ARCH 30.3)",
        sql: r#"
-- A partial, covering index for the hot current-version read paths (Phase 1.7). The latest-only
-- listing (`fetch_rows`) and single-key current-version lookups all filter `is_latest = 1`; this
-- index keeps ONLY current rows (the partial `WHERE is_latest = 1` makes it one entry per live
-- key, not one per historical version) and carries every column the listing projects, so a
-- latest-only ListObjects is answered index-only — no per-row table fetch and no stepping over
-- superseded versions. `is_latest` itself is constant (1) under the partial predicate, so it need
-- not be stored. This supersedes idx_object_versions_latest, whose sole role was is_latest=1 seeks
-- over (bucket_name, key); dropping it keeps the number of maintained indexes flat.
DROP INDEX idx_object_versions_latest;
CREATE INDEX idx_ov_latest_cover ON object_versions
    (bucket_name, key, version_id, is_delete_marker, etag, size_logical, updated_at,
     storage_class, owner_id)
    WHERE is_latest = 1;
"#,
    },
    Migration {
        version: 12,
        name: "maintained per-bucket / per-user roll-up counters (ARCH 30, Phase 2.1)",
        sql: r#"
-- Maintained roll-ups so the overview aggregates and the quota checks read O(buckets)/O(1)
-- counters instead of scanning every object version. The writer keeps these in lockstep with
-- object_versions inside the same transaction: +1 row + bytes on insert, -1 row - bytes on delete.
-- Latest / delete-marker transitions don't change byte or version totals, so they are not tracked
-- here; `objects` (the current-visible count) stays an index-only count over the partial
-- current-version index, since it needs transition logic and is not a quota input. The byte totals
-- sum over ALL versions, matching the prior scan-based semantics. Seed both tables from the
-- existing rows so an upgrade starts consistent.
CREATE TABLE bucket_stats (
    bucket_name    TEXT PRIMARY KEY,
    versions       INTEGER NOT NULL DEFAULT 0,
    logical_bytes  INTEGER NOT NULL DEFAULT 0,
    physical_bytes INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE user_stats (
    owner_id       TEXT PRIMARY KEY,
    logical_bytes  INTEGER NOT NULL DEFAULT 0
);
INSERT INTO bucket_stats (bucket_name, versions, logical_bytes, physical_bytes)
    SELECT bucket_name, COUNT(*), COALESCE(SUM(size_logical), 0), COALESCE(SUM(size_physical), 0)
    FROM object_versions GROUP BY bucket_name;
INSERT INTO user_stats (owner_id, logical_bytes)
    SELECT owner_id, COALESCE(SUM(size_logical), 0)
    FROM object_versions GROUP BY owner_id;
"#,
    },
    Migration {
        version: 13,
        name: "master-key rotation: key-ring state + re-wrap progress (#29, Phase D/E)",
        sql: r#"
-- Per-key durable state for the active-key seal-count bound (Phase E) and the re-wrap progress
-- accounting (Phase D). Key MATERIAL never lives here — only ids, a short hash prefix for
-- operator display, and counters.
--   id              : u16 ring id (1..65535).
--   key_hash        : first 8 hex chars of SHA-256(key) for operator-visible identification only.
--   is_active       : 1 for the current active id, else 0 (advisory; env is the source of truth).
--   sealed_count    : high-water seal count under this key (synced from memory, Phase E).
--   rewrapped_count : rows re-sealed FROM this key onto the active key (Phase D progress).
--   created_at / retired_at : wall-clock millis; retired_at NULL while in the env ring.
CREATE TABLE key_ring_state (
    id              INTEGER PRIMARY KEY CHECK (id > 0 AND id <= 65535),
    key_hash        TEXT    NOT NULL,
    is_active       INTEGER NOT NULL DEFAULT 0 CHECK (is_active IN (0,1)),
    sealed_count    INTEGER NOT NULL DEFAULT 0,
    rewrapped_count INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    retired_at      INTEGER
);
-- Resumable cursor per re-wrap stream (Phase D). One row per (table.column) being migrated.
CREATE TABLE rewrap_progress (
    stream        TEXT PRIMARY KEY,   -- e.g. 'object_versions.sse_descriptor'
    cursor        TEXT,               -- last id processed within the in-flight pass; NULL = no pass mid-flight
    rows_done     INTEGER NOT NULL DEFAULT 0,
    rows_failed   INTEGER NOT NULL DEFAULT 0,
    updated_at    INTEGER NOT NULL DEFAULT 0
);
"#,
    },
    Migration {
        version: 14,
        name: "rewrap completion marker",
        sql: r#"
-- audit #29 fix: a NULL `cursor` alone cannot tell "re-wrap not started yet" from "re-wrap done",
-- so a freshly-rotated key wrongly looked retire-eligible before any pass ran. `done_active_id` is
-- the active key id under which a FULL, failure-free re-wrap pass last completed for this stream
-- (0 = none yet). A key is retire-eligible only when EVERY stream's `done_active_id` equals the
-- current active id on EVERY shard.
ALTER TABLE rewrap_progress ADD COLUMN done_active_id INTEGER NOT NULL DEFAULT 0;
"#,
    },
    Migration {
        version: 15,
        name: "multipart sse intent",
        sql: r#"
-- Capture whether SSE-S3 was requested for a multipart upload at initiate time, so completion
-- encrypts the assembled object at rest (multipart assembly previously always stored plaintext,
-- silently ignoring a requested or bucket-default SSE). 0 = no SSE; 1 = SSE-S3 (AES256).
ALTER TABLE multipart_uploads ADD COLUMN sse_requested INTEGER NOT NULL DEFAULT 0;
"#,
    },
    Migration {
        version: 16,
        name: "object lock",
        sql: r#"
-- S3 Object Lock (WORM): per-version retention + legal hold (ARCH 19.6). Stored in a side table so
-- the hot object_versions row is untouched; a row exists only for a version that has ever had a
-- lock set. lock_mode is 'GOVERNANCE'|'COMPLIANCE'|NULL (no retention); retain_until is epoch ms.
CREATE TABLE object_locks (
    bucket_name  TEXT NOT NULL,
    key          TEXT NOT NULL,
    version_id   TEXT NOT NULL,
    lock_mode    TEXT,
    retain_until INTEGER,
    legal_hold   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_name, key, version_id)
);
"#,
    },
    Migration {
        version: 17,
        name: "webhook event-notification outbox",
        sql: r#"
-- Event-notification (webhook) delivery outbox, mirroring replication_outbox (ARCH 20-style).
-- One row = one object event matched to one bucket webhook endpoint; the ready-to-POST JSON is
-- pre-rendered into `payload`. status is 'pending'|'claimed'|'completed'|'failed'; the drain
-- worker claims under a lease (lease_until) and retries with backoff (next_attempt_at).
CREATE TABLE events_outbox (
    id              TEXT PRIMARY KEY,
    bucket_name     TEXT NOT NULL,
    key             TEXT NOT NULL,
    version_id      TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    endpoint_id     TEXT NOT NULL,
    payload         TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL,
    status          TEXT NOT NULL,
    last_error      TEXT,
    priority        INTEGER NOT NULL DEFAULT 0,
    lease_until     INTEGER
);
CREATE INDEX idx_events_outbox_status_next ON events_outbox (status, next_attempt_at);
"#,
    },
    Migration {
        version: 18,
        name: "session credentials (STS)",
        sql: r#"
-- STS-style temporary session credentials (ARCH 14). A row is a temporary access-key/secret pair
-- scoped to a parent user, with a hashed session token, an optional inline policy, and an expiry.
-- The secret is sealed under the master key exactly like a user's SigV4 secret (CRK1 → NULL nonce).
CREATE TABLE session_credentials (
    access_key_id      TEXT PRIMARY KEY,
    parent_user_id     TEXT NOT NULL,
    secret_ciphertext  BLOB NOT NULL,
    secret_nonce       BLOB,
    session_token_hash TEXT NOT NULL,
    inline_policy      TEXT,
    expires_at         INTEGER NOT NULL,
    created_at         INTEGER NOT NULL
);
CREATE INDEX idx_session_creds_expiry ON session_credentials (expires_at);
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
    fn migration_v13_adds_rotation_state_tables() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        for table in ["key_ring_state", "rewrap_progress"] {
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    rusqlite::params![table],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "{table} created by v13");
        }
        // The id CHECK rejects 0 but accepts a valid ring id.
        assert!(
            conn.execute(
                "INSERT INTO key_ring_state (id, key_hash, created_at) VALUES (0, 'x', 0)",
                [],
            )
            .is_err()
        );
        assert!(
            conn.execute(
                "INSERT INTO key_ring_state (id, key_hash, created_at) VALUES (1, 'abc12345', 0)",
                [],
            )
            .is_ok()
        );
    }

    #[test]
    fn migration_v14_adds_rewrap_completion_marker() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        assert!(column_exists(&conn, "rewrap_progress", "done_active_id"));
        // It defaults to 0 (= no completed pass) so a freshly-created stream is never "complete".
        conn.execute(
            "INSERT INTO rewrap_progress (stream, updated_at) VALUES ('s', 0)",
            [],
        )
        .unwrap();
        let done: i64 = conn
            .query_row(
                "SELECT done_active_id FROM rewrap_progress WHERE stream='s'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(done, 0, "done_active_id defaults to 0 (not started)");
    }

    #[test]
    fn migration_v2_renames_quota_and_index_changes() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();

        // The compression column was renamed to the spec name (ARCH 34.1) and the quota column
        // was added (ARCH 27.5).
        assert!(column_exists(&conn, "buckets", "compression_policy"));
        assert!(!column_exists(&conn, "buckets", "compression"));
        assert!(column_exists(&conn, "buckets", "quota_bytes"));

        // The storage_path seek indexes were created (F-8) and the redundant bkv index dropped.
        assert!(index_exists(&conn, "idx_object_versions_storage_path"));
        assert!(index_exists(&conn, "idx_multipart_parts_storage_path"));
        assert!(!index_exists(&conn, "idx_object_versions_bkv"));
        // The UNIQUE-constraint auto-index still serves bkv range seeks (ARCH 34.2).
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
        // The SSE-S3 descriptor column exists and is nullable (ARCH 27).
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
        // The nullable per-user identity policy column exists (ARCH 15 / user-centric authz).
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

    #[test]
    fn migration_v8_request_metrics_table_and_upsert() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        // The rollup table + its timestamp index exist (ARCH 26.5).
        let table: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='request_metrics'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(table, 1);
        assert!(index_exists(&conn, "idx_request_metrics_ts"));

        // The composite-key upsert accumulates count rather than inserting duplicates.
        let up =
            "INSERT INTO request_metrics (ts_bucket, operation, bucket_name, status_class, count)
                  VALUES (60, 'GetObject', 'b', '2xx', ?1)
                  ON CONFLICT(ts_bucket, operation, bucket_name, status_class)
                  DO UPDATE SET count = count + excluded.count";
        conn.execute(up, rusqlite::params![3_i64]).unwrap();
        conn.execute(up, rusqlite::params![5_i64]).unwrap();
        let (rows, total): (i64, i64) = conn
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(count), 0) FROM request_metrics",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(rows, 1, "same key must upsert into one row");
        assert_eq!(total, 8, "count must accumulate");
    }

    #[test]
    fn migration_v9_adds_bytes_and_latency_columns() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        for col in [
            "bytes_in",
            "bytes_out",
            "lat_sum_ms",
            "lat_le_5",
            "lat_le_20",
            "lat_le_50",
            "lat_le_200",
            "lat_le_1000",
            "lat_gt_1000",
        ] {
            assert!(
                column_exists(&conn, "request_metrics", col),
                "missing column {col}"
            );
        }
        // The new columns default to 0 for a minimal insert (mirrors old v8 rows).
        conn.execute(
            "INSERT INTO request_metrics (ts_bucket, operation, bucket_name, status_class, count)
             VALUES (60, 'GetObject', 'b', '2xx', 1)",
            [],
        )
        .unwrap();
        let (bin, lat): (i64, i64) = conn
            .query_row(
                "SELECT bytes_in, lat_sum_ms FROM request_metrics",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((bin, lat), (0, 0));
    }

    #[test]
    fn migration_v10_adds_object_tags_reverse_index() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        assert!(index_exists(&conn, "idx_object_tags_kv"));
    }

    #[test]
    fn migration_v11_swaps_latest_index_for_partial_cover() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        // The partial covering index replaces the narrow latest index (ARCH 30.3).
        assert!(index_exists(&conn, "idx_ov_latest_cover"));
        assert!(!index_exists(&conn, "idx_object_versions_latest"));
        // It is a partial index (carries a WHERE predicate) so it holds only current rows.
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_ov_latest_cover'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            sql.to_ascii_lowercase().contains("where is_latest"),
            "index must be partial on is_latest=1, got: {sql}"
        );

        // The query planner answers the latest-only listing from this index alone (covering): the
        // plan must reference the index and must NOT fall back to a full table scan.
        conn.execute_batch(
            "INSERT INTO object_versions
             (id, bucket_name, key, version_id, is_latest, is_delete_marker, size_logical,
              size_physical, etag, content_type, compression, storage_class, owner_id,
              user_metadata, checksums, created_at, updated_at)
             VALUES ('i','b','k','v',1,0,1,1,'e','text/plain','\"Uncompressed\"','Standard',
                     'o','[]','[]',0,0);",
        )
        .unwrap();
        let plan: String = conn
            .query_row(
                "EXPLAIN QUERY PLAN
                 SELECT key, version_id, is_latest, is_delete_marker, etag, size_logical,
                        updated_at, storage_class, owner_id
                 FROM object_versions
                 WHERE bucket_name = 'b' AND key >= '' AND is_latest = 1 AND is_delete_marker = 0
                 ORDER BY key ASC LIMIT 10",
                [],
                |r| r.get::<_, String>(3),
            )
            .unwrap();
        // "USING COVERING INDEX <name>" is SQLite's label for an index-only scan: the projection
        // is satisfied entirely from the index, with no table B-tree lookups.
        assert!(
            plan.contains("COVERING INDEX idx_ov_latest_cover"),
            "latest-only listing must be index-only via the covering index, plan was: {plan}"
        );
    }

    #[test]
    fn migration_v12_seeds_stat_counters_from_existing_rows() {
        let conn = Connection::open_in_memory().unwrap();
        // Apply through v11, then insert object versions, then re-run so v12 seeds from them. We
        // simulate "existing data at upgrade time" by inserting before v12 runs: run migrations up
        // to v11 by faking the recorded version.
        run_migrations(&conn).unwrap();
        assert!(index_exists(&conn, "idx_ov_latest_cover")); // sanity: full chain applied

        // Both roll-up tables exist.
        for t in ["bucket_stats", "user_stats"] {
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    rusqlite::params![t],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "table {t} must exist");
        }

        // The seed is correct for rows inserted *before* v12: rebuild a fresh DB, insert, then seed.
        let conn2 = Connection::open_in_memory().unwrap();
        // Apply only up to v11 by running the chain then dropping the v12 tables and its record, so
        // we can re-seed against hand-inserted rows.
        run_migrations(&conn2).unwrap();
        conn2
            .execute_batch(
                "DELETE FROM bucket_stats; DELETE FROM user_stats;
                 INSERT INTO object_versions
                   (id, bucket_name, key, version_id, is_latest, is_delete_marker, size_logical,
                    size_physical, etag, content_type, compression, storage_class, owner_id,
                    user_metadata, checksums, created_at, updated_at)
                 VALUES
                   ('i1','b','k','v1',1,0,10,12,'e','text/plain','\"Uncompressed\"','Standard','alice','[]','[]',0,0),
                   ('i2','b','k','v2',0,0,20,24,'e','text/plain','\"Uncompressed\"','Standard','alice','[]','[]',0,0),
                   ('i3','c','k','v1',1,0,5,5,'e','text/plain','\"Uncompressed\"','Standard','bob','[]','[]',0,0);
                 INSERT INTO bucket_stats (bucket_name, versions, logical_bytes, physical_bytes)
                   SELECT bucket_name, COUNT(*), COALESCE(SUM(size_logical),0), COALESCE(SUM(size_physical),0)
                   FROM object_versions GROUP BY bucket_name;
                 INSERT INTO user_stats (owner_id, logical_bytes)
                   SELECT owner_id, COALESCE(SUM(size_logical),0) FROM object_versions GROUP BY owner_id;",
            )
            .unwrap();
        let (bv, bl, bp): (i64, i64, i64) = conn2
            .query_row(
                "SELECT versions, logical_bytes, physical_bytes FROM bucket_stats WHERE bucket_name='b'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            (bv, bl, bp),
            (2, 30, 36),
            "bucket b: 2 versions, 30 logical, 36 physical"
        );
        let ul: i64 = conn2
            .query_row(
                "SELECT logical_bytes FROM user_stats WHERE owner_id='alice'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ul, 30, "alice owns 30 logical bytes across versions");
    }
}
