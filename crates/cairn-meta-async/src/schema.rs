//! The SQLite schema and the async migration runner (ARCH 34.1). The migration SQL is copied
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
-- Persistent, revocable, optionally-forever object-share tokens (ARCH 15.8). The opaque token is
-- the bearer capability served at GET /share/{token}; revoke flips revoked_at with no global key
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
    // NOTE: versions 13 and 14 are intentionally absent here — they are the #29 key-rotation schema
    // (rewrap_progress/done_active_id), which the async backend does not implement (rotate-and-read
    // only). The runner applies any version > the current max, so the v12 -> v15 gap is fine and the
    // version number stays aligned with cairn-meta for the same logical migration.
    Migration {
        version: 15,
        name: "multipart sse intent",
        sql: r#"
-- Capture whether SSE-S3 was requested for a multipart upload at initiate time, so completion
-- encrypts the assembled object at rest (multipart assembly previously always stored plaintext).
-- Mirrors cairn-meta/src/schema.rs v15. 0 = no SSE; 1 = SSE-S3 (AES256).
ALTER TABLE multipart_uploads ADD COLUMN sse_requested INTEGER NOT NULL DEFAULT 0;
"#,
    },
    Migration {
        version: 16,
        name: "object lock",
        sql: r#"
-- S3 Object Lock side table (WORM): per-version retention + legal hold. Mirrors
-- cairn-meta/src/schema.rs v16. lock_mode is 'GOVERNANCE'|'COMPLIANCE'|NULL; retain_until is epoch ms.
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
-- Event-notification (webhook) delivery outbox. Mirrors cairn-meta/src/schema.rs v17.
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
-- STS-style temporary session credentials. Mirrors cairn-meta/src/schema.rs v18.
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
    Migration {
        version: 19,
        name: "replication outbox enqueue timestamp (true lag)",
        sql: r#"
-- Enqueue-time millis for true replication lag. Mirrors cairn-meta/src/schema.rs v19.
ALTER TABLE replication_outbox ADD COLUMN enqueued_at INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_outbox_status_enqueued ON replication_outbox (status, enqueued_at);
"#,
    },
    Migration {
        version: 20,
        name: "import jobs (S3 -> Cairn migration)",
        sql: r#"
-- S3 import jobs (ARCH 27). Mirrors cairn-meta/src/schema.rs v20 byte-for-byte (same version).
CREATE TABLE import_jobs (
    id                   TEXT PRIMARY KEY,
    source_endpoint      TEXT NOT NULL,
    source_region        TEXT NOT NULL,
    access_key_id        TEXT NOT NULL,
    secret_ciphertext    BLOB NOT NULL,
    secret_nonce         BLOB,
    ca_cert_pem          TEXT,
    insecure_skip_verify INTEGER NOT NULL DEFAULT 0,
    workers              INTEGER NOT NULL,
    state                TEXT NOT NULL,
    buckets_json         TEXT NOT NULL,
    objects_done         INTEGER NOT NULL DEFAULT 0,
    objects_total        INTEGER NOT NULL DEFAULT 0,
    bytes_done           INTEGER NOT NULL DEFAULT 0,
    bytes_total          INTEGER NOT NULL DEFAULT 0,
    last_error           TEXT,
    lease_until          INTEGER,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);
CREATE INDEX idx_import_jobs_state ON import_jobs (state, created_at);
"#,
    },
    Migration {
        version: 21,
        name: "multipart part-level encryption at rest",
        sql: r#"
-- Part-level SSE at rest (ARCH 27, Increment 3a). Two additive, back-compatible columns so an
-- SSE / bucket-default-SSE / at-rest multipart upload stages every PART as ciphertext (nothing
-- plaintext on disk); the assembled object is a decrypt-then-re-encrypt pass.
--   * multipart_uploads.encrypt_parts: the part-encryption decision PINNED at initiate (AWS captures
--     SSE intent at initiate). 1 => every UploadPart/UploadPartCopy mints a per-part DEK and stages a
--     CRNB VERSION_ENCRYPTED blob; 0 => stage plaintext (legacy). A pre-v21 in-flight row reads 0 and
--     still completes via the plaintext-parts -> encrypt-at-assemble legacy path.
--   * multipart_parts.part_dek: that part's 32-byte DEK, freshly random per staging, sealed under the
--     master ring (base64 CRK1 envelope). NULL = a plaintext part. Consumed and discarded at
--     CompleteMultipartUpload; never enters the object rewrap stream (ephemeral, GC'd with the session).
-- Mirrors cairn-meta/src/schema.rs v21 byte-for-byte (same version).
ALTER TABLE multipart_uploads ADD COLUMN encrypt_parts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE multipart_parts   ADD COLUMN part_dek       TEXT;
"#,
    },
    Migration {
        version: 22,
        name: "multipart kms intent",
        sql: r#"
-- Explicit SSE-KMS intent captured at CreateMultipartUpload (ARCH 27, Increment 3b). AWS pins the
-- x-amz-server-side-encryption header at initiate; these additive, back-compatible columns carry an
-- explicit `aws:kms` request from initiate to CompleteMultipartUpload so the assembled object
-- advertises aws:kms + its key id. The KMS key id is a validated LABEL only (same master-sealed DEK
-- per id) — no external KMS / network. A pre-v22 in-flight row reads sse_kms_requested=0 and
-- completes via the SSE-S3 / bucket-default path unchanged.
--   * multipart_uploads.sse_kms_requested: 1 => an explicit `aws:kms` header was accepted at initiate.
--   * multipart_uploads.sse_kms_key_id: the validated key-id label to advertise (NULL = default key).
--   * multipart_uploads.sse_bucket_key_enabled: echo x-amz-server-side-encryption-bucket-key-enabled.
-- Mirrors cairn-meta/src/schema.rs v22 byte-for-byte (same version).
ALTER TABLE multipart_uploads ADD COLUMN sse_kms_requested      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE multipart_uploads ADD COLUMN sse_kms_key_id         TEXT;
ALTER TABLE multipart_uploads ADD COLUMN sse_bucket_key_enabled INTEGER NOT NULL DEFAULT 0;
"#,
    },
    Migration {
        version: 23,
        name: "replication completion timestamp",
        sql: r#"
-- When a version was last SHIPPED (ARCH 20.5, the SSE plaintext-seam repair). `replication_status`
-- alone cannot tell a version that replicated BEFORE the fix from one that was force-requeued and
-- has since re-shipped CORRECTLY: both read `completed`, and `created_at` is never rewritten. The
-- audit's "is this replica suspect?" predicate therefore needs a second, replication-owned clock.
--   * object_versions.replicated_at: unix seconds at which MarkReplicationDone last stamped this
--     version `completed`. NULL = never successfully shipped from this node, OR shipped before this
--     migration — the two are indistinguishable on an upgraded node, and the audit deliberately
--     treats NULL as SUSPECT (over-report costs one wasted re-ship; under-report leaves a garbage
--     replica nobody looks for). It resolves itself as versions are re-shipped and stamped.
--     Deliberately NOT `updated_at`, which feeds the client-visible S3 `LastModified` — stamping
--     that on replication would silently mutate object metadata.
-- Mirrors cairn-meta/src/schema.rs v23 byte-for-byte (same version).
ALTER TABLE object_versions ADD COLUMN replicated_at INTEGER;

-- The forced-resync requeue pages the bucket's terminal outbox rows KEY BY KEY (a key's rows must
-- be requeued together, or an older version re-ships after a newer one). The existing indexes are
-- (status, next_attempt_at) and (status, enqueued_at), neither of which can order or seek by key —
-- so without this the per-page `key > ?cursor ORDER BY key` seek is a full scan per page, i.e.
-- quadratic on exactly the large buckets the paging exists for.
CREATE INDEX idx_outbox_bucket_key ON replication_outbox (bucket_name, key);
"#,
    },
    Migration {
        version: 24,
        name: "bounded import scheduling and history",
        sql: r#"
-- The import scheduler selects one oldest pending/running id without reading or decoding the
-- history-sized buckets_json payload. The id tie-breaker makes equal-millisecond creation times
-- deterministic and lets the query remain an index-only, constant-row seek.
-- Mirrors cairn-meta/src/schema.rs v24 byte-for-byte (same version).
CREATE INDEX idx_import_jobs_state_created_id
    ON import_jobs (state, created_at, id);

-- Management history is keyset-paged newest-first over this exact stable order.
CREATE INDEX idx_import_jobs_created_id
    ON import_jobs (created_at DESC, id DESC);

-- Terminal retention filters by lifecycle state and updated_at. Without this index every import
-- heartbeat scans the complete history merely to discover that nothing is old enough to prune.
CREATE INDEX idx_import_jobs_state_updated
    ON import_jobs (state, updated_at);
"#,
    },
    Migration {
        version: 25,
        name: "one-time object share capabilities",
        sql: r#"
-- Mirrors cairn-meta/src/schema.rs v25. Legacy bearer tokens are revoked and overwritten before
-- the table is rebuilt around a stable non-secret id and fixed-width SHA-256 lookup hash.
UPDATE object_shares
SET token = lower(hex(randomblob(32))),
    revoked_at = COALESCE(
        revoked_at,
        CAST(strftime('%s', 'now') AS INTEGER) * 1000
    );

DROP INDEX idx_object_shares_bucket_key;
DROP INDEX idx_object_shares_created_by;
ALTER TABLE object_shares RENAME TO object_shares_v24;

CREATE TABLE object_shares (
    id          TEXT PRIMARY KEY,
    token_hash  BLOB NOT NULL UNIQUE CHECK (length(token_hash) = 32),
    bucket_name TEXT NOT NULL,
    key         TEXT NOT NULL,
    version_id  TEXT,
    expires_at  INTEGER,
    disposition TEXT NOT NULL DEFAULT 'inline',
    filename    TEXT,
    created_by  TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    revoked_at  INTEGER
);

-- Shares are account-global on shard 0, while buckets are shard-local, so there is intentionally
-- no cross-database foreign key to `buckets`.
INSERT INTO object_shares
    (id, token_hash, bucket_name, key, version_id, expires_at, disposition, filename,
     created_by, created_at, revoked_at)
SELECT
    'legacy-' || lower(hex(randomblob(16))), randomblob(32), bucket_name, key, version_id,
    expires_at, disposition, filename, created_by, created_at, revoked_at
FROM object_shares_v24;

-- Durable retry marker: physical compaction happens after COMMIT, and startup must retry it if the
-- process dies or the backend returns an error. Fresh databases have no legacy rows and no marker.
CREATE TABLE share_capability_sanitation (
    id INTEGER PRIMARY KEY CHECK (id = 1)
);
INSERT INTO share_capability_sanitation (id)
SELECT 1 WHERE EXISTS (SELECT 1 FROM object_shares_v24);

DROP TABLE object_shares_v24;
CREATE INDEX idx_object_shares_bucket_key ON object_shares (bucket_name, key);
CREATE INDEX idx_object_shares_created_by ON object_shares (created_by);
"#,
    },
    Migration {
        version: 26,
        name: "bounded multipart staging reservations",
        sql: r#"
ALTER TABLE multipart_uploads ADD COLUMN initiated_by TEXT;
UPDATE multipart_uploads SET initiated_by=owner_id WHERE initiated_by IS NULL;

CREATE TABLE multipart_part_reservations (
    attempt_id     TEXT PRIMARY KEY,
    upload_id      TEXT NOT NULL,
    part_number    INTEGER NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    reserved_bytes INTEGER NOT NULL CHECK (reserved_bytes >= 0),
    created_at     INTEGER NOT NULL,
    UNIQUE (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads (id) ON DELETE CASCADE
);
CREATE INDEX idx_multipart_reservations_created
    ON multipart_part_reservations (created_at, attempt_id);

CREATE TABLE multipart_staging_cleanups (
    id           TEXT PRIMARY KEY,
    upload_id    TEXT NOT NULL,
    bucket_name  TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    bytes        INTEGER NOT NULL CHECK (bytes >= 0),
    storage_path TEXT,
    created_at   INTEGER NOT NULL
);
CREATE INDEX idx_multipart_cleanups_upload ON multipart_staging_cleanups (upload_id);
CREATE INDEX idx_multipart_cleanups_created ON multipart_staging_cleanups (created_at, id);

CREATE TABLE multipart_bucket_stats (
    bucket_name    TEXT PRIMARY KEY,
    active_uploads INTEGER NOT NULL DEFAULT 0 CHECK (active_uploads >= 0),
    staged_bytes   INTEGER NOT NULL DEFAULT 0 CHECK (staged_bytes >= 0)
);
CREATE TABLE multipart_principal_stats (
    principal_id   TEXT PRIMARY KEY,
    active_uploads INTEGER NOT NULL DEFAULT 0 CHECK (active_uploads >= 0),
    staged_bytes   INTEGER NOT NULL DEFAULT 0 CHECK (staged_bytes >= 0)
);
INSERT INTO multipart_bucket_stats (bucket_name, active_uploads, staged_bytes)
SELECT u.bucket_name, COUNT(DISTINCT u.id), COALESCE(SUM(p.size), 0)
FROM multipart_uploads u
LEFT JOIN multipart_parts p ON p.upload_id=u.id
GROUP BY u.bucket_name;
INSERT INTO multipart_principal_stats (principal_id, active_uploads, staged_bytes)
SELECT COALESCE(u.initiated_by, u.owner_id), COUNT(DISTINCT u.id), COALESCE(SUM(p.size), 0)
FROM multipart_uploads u
LEFT JOIN multipart_parts p ON p.upload_id=u.id
GROUP BY COALESCE(u.initiated_by, u.owner_id);
"#,
    },
    Migration {
        version: 27,
        name: "writer-authoritative object lock",
        sql: r#"
-- Object Lock enforcement is writer-authoritative. Rebuild the v16 side table with structural
-- checks and a composite foreign key so a lock can describe exactly one existing version and is
-- removed only as part of that version's writer-owned terminal mutation. Historical code could
-- leave an orphan side row after deleting its version; that row protects no bytes and is discarded.
-- Every row still attached to a version is copied through the constraints, so malformed protection
-- state fails startup closed rather than being silently weakened.
ALTER TABLE object_locks RENAME TO object_locks_v16;
CREATE TABLE object_locks (
    bucket_name  TEXT NOT NULL,
    key          TEXT NOT NULL,
    version_id   TEXT NOT NULL,
    lock_mode    TEXT CHECK (
        lock_mode IS NULL OR lock_mode IN ('GOVERNANCE', 'COMPLIANCE')
    ),
    retain_until INTEGER,
    legal_hold   INTEGER NOT NULL DEFAULT 0 CHECK (legal_hold IN (0, 1)),
    PRIMARY KEY (bucket_name, key, version_id),
    CHECK (
        (lock_mode IS NULL AND retain_until IS NULL)
        OR (lock_mode IS NOT NULL AND retain_until IS NOT NULL)
    ),
    FOREIGN KEY (bucket_name, key, version_id)
        REFERENCES object_versions (bucket_name, key, version_id)
        ON DELETE CASCADE
);
INSERT INTO object_locks
    (bucket_name, key, version_id, lock_mode, retain_until, legal_hold)
SELECT locks.bucket_name, locks.key, locks.version_id,
       locks.lock_mode, locks.retain_until, locks.legal_hold
FROM object_locks_v16 AS locks
WHERE EXISTS (
    SELECT 1
    FROM object_versions AS versions
    WHERE versions.bucket_name=locks.bucket_name
      AND versions.key=locks.key
      AND versions.version_id=locks.version_id
);
DROP TABLE object_locks_v16;

-- Initiation pins tags and only the explicit Object Lock request. NULL legal_hold means the header
-- was absent (distinct from explicit OFF). Bucket default retention is resolved from the current
-- strictly-parsed configuration inside CompleteMultipartUpload's writer savepoint.
ALTER TABLE multipart_uploads
    ADD COLUMN initial_tags TEXT NOT NULL DEFAULT '[]';
ALTER TABLE multipart_uploads
    ADD COLUMN lock_mode TEXT CHECK (
        lock_mode IS NULL OR lock_mode IN ('GOVERNANCE', 'COMPLIANCE')
    );
ALTER TABLE multipart_uploads ADD COLUMN retain_until INTEGER;
ALTER TABLE multipart_uploads
    ADD COLUMN legal_hold INTEGER CHECK (
        legal_hold IS NULL OR legal_hold IN (0, 1)
    );
-- Existing sessions cannot prove whether the pre-v27 initiate request carried explicit lock
-- headers. New writer-created sessions set this to 1; the 0 default makes every migrated legacy
-- session distinguishable so completion can fail closed on an Object-Lock bucket.
ALTER TABLE multipart_uploads
    ADD COLUMN object_lock_intent_known INTEGER NOT NULL DEFAULT 0 CHECK (
        object_lock_intent_known IN (0, 1)
    );
"#,
    },
    Migration {
        version: 28,
        name: "current-listing row identity cover",
        sql: r#"
-- Lifecycle compare-and-delete uses object_versions.id as the immutable identity of the exact row
-- returned by a listing. Keep the hot latest-only ListObjects projection index-only after adding
-- that internal field to ObjectSummary by replacing, rather than editing, migration v11's index.
DROP INDEX IF EXISTS idx_ov_latest_cover;
CREATE INDEX idx_ov_latest_cover ON object_versions
    (bucket_name, key, version_id, id, is_delete_marker, etag, size_logical, updated_at,
     storage_class, owner_id)
    WHERE is_latest = 1;
"#,
    },
    Migration {
        version: 29,
        name: "multipart completion claim tokens",
        sql: r#"
-- Completion ownership must survive a lost writer acknowledgement. A per-attempt token lets
-- cancellation recovery be armed before ClaimMultipart is acknowledged while remaining unable to
-- release or complete a newer attempt (the generationless form admitted an ABA race).
ALTER TABLE multipart_uploads ADD COLUMN completion_claim_token TEXT;
-- A process performing this migration has no surviving request that owns an old transient claim.
UPDATE multipart_uploads
SET status='active', completion_claim_token=NULL
WHERE status='completing';
"#,
    },
];

/// Run all pending migrations on the write driver, recording each as applied. Each migration is
/// wrapped in its own transaction (begin/commit), matching the rusqlite runner's
/// `unchecked_transaction` per migration.
pub async fn run_migrations(driver: &dyn AsyncSqlDriver) -> Result<(), MetaError> {
    // Ask SQLite-family engines to zero deleted b-tree content before v25 overwrites and rebuilds
    // the legacy plaintext-token table. Unsupported beta-engine PRAGMAs are harmless; the SQL
    // migration still destroys every redeemable value and VACUUM below rebuilds physical storage.
    let _ = driver.execute_batch("PRAGMA secure_delete=ON;").await;
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
    if share_sanitation_pending(driver).await? {
        // Run outside the migration transaction and before readers are opened. Each driver owns
        // the backend-specific physical compaction/checkpoint contract. The durable marker remains
        // if this fails, so the next startup retries instead of treating committed v25 as complete.
        driver.scrub_legacy_share_storage().await?;
        driver
            .execute("DELETE FROM share_capability_sanitation WHERE id=1", vec![])
            .await?;
    }
    Ok(())
}

async fn share_sanitation_pending(driver: &dyn AsyncSqlDriver) -> Result<bool, MetaError> {
    let table_exists = driver
        .query(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type='table' AND name='share_capability_sanitation'
             )",
            vec![],
        )
        .await?
        .first()
        .is_some_and(|row| row.get_i64(0) != 0);
    if !table_exists {
        return Ok(false);
    }
    Ok(driver
        .query(
            "SELECT EXISTS(
                 SELECT 1 FROM share_capability_sanitation WHERE id=1
             )",
            vec![],
        )
        .await?
        .first()
        .is_some_and(|row| row.get_i64(0) != 0))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::libsql_driver::LibsqlDriver;
    use crate::turso_driver::TursoDriver;
    use libsql::Database;
    use std::sync::Arc;

    const ASYNC_LEGACY_SHARE_SENTINEL: &str = "async-legacy-share-plaintext-sentinel-029";

    async fn migrated_driver() -> Arc<dyn AsyncSqlDriver> {
        let name = format!(
            "file:cairn-libsql-schema-{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4().simple()
        );
        #[allow(deprecated)]
        let db = Database::open(name).unwrap();
        let conn = db.connect().unwrap();
        let driver: Arc<dyn AsyncSqlDriver> = Arc::new(LibsqlDriver::new(conn));
        run_migrations(driver.as_ref()).await.unwrap();
        driver
    }

    async fn column_exists(driver: &dyn AsyncSqlDriver, table: &str, column: &str) -> bool {
        driver
            .query(
                "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name=?2",
                vec![
                    Value::Text(table.to_owned()),
                    Value::Text(column.to_owned()),
                ],
            )
            .await
            .unwrap()
            .first()
            .map_or(0, |r| r.get_i64(0))
            > 0
    }

    async fn assert_v25_legacy_share_is_revoked(driver: &dyn AsyncSqlDriver) {
        driver
            .execute_batch(
                "PRAGMA foreign_keys=ON;
                 CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY,
                     name TEXT NOT NULL,
                     applied_at INTEGER NOT NULL
                 );
                 INSERT INTO schema_migrations VALUES (24, 'legacy fixture', 0);
                 CREATE TABLE buckets (name TEXT PRIMARY KEY);
                 INSERT INTO buckets VALUES ('photos');
                 CREATE TABLE object_shares (
                     token TEXT PRIMARY KEY,
                     bucket_name TEXT NOT NULL,
                     key TEXT NOT NULL,
                     version_id TEXT,
                     expires_at INTEGER,
                     disposition TEXT NOT NULL DEFAULT 'inline',
                     filename TEXT,
                     created_by TEXT NOT NULL,
                     created_at INTEGER NOT NULL,
                     revoked_at INTEGER,
                     FOREIGN KEY (bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
                 );
                 CREATE INDEX idx_object_shares_bucket_key
                     ON object_shares (bucket_name, key);
                 CREATE INDEX idx_object_shares_created_by
                     ON object_shares (created_by);
                 -- Minimal v24-era tables needed by later append-only migrations. This fixture is
                 -- intentionally sparse because the test targets share sanitation, but every
                 -- migration after v25 must still be able to advance it to the current schema.
                 CREATE TABLE multipart_uploads (
                     id TEXT PRIMARY KEY,
                     bucket_name TEXT NOT NULL,
                     key TEXT NOT NULL,
                     content_type TEXT NOT NULL,
                     status TEXT NOT NULL,
                     owner_id TEXT NOT NULL,
                     user_metadata TEXT NOT NULL,
                     created_at INTEGER NOT NULL,
                     updated_at INTEGER NOT NULL
                 );
                 CREATE TABLE multipart_parts (
                     upload_id TEXT NOT NULL,
                     part_number INTEGER NOT NULL,
                     size INTEGER NOT NULL,
                     PRIMARY KEY (upload_id, part_number)
                 );
                 CREATE TABLE object_versions (
                     id TEXT PRIMARY KEY,
                     bucket_name TEXT NOT NULL,
                     key TEXT NOT NULL,
                     version_id TEXT NOT NULL,
                     is_latest INTEGER NOT NULL DEFAULT 1,
                     is_delete_marker INTEGER NOT NULL DEFAULT 0,
                     etag TEXT NOT NULL DEFAULT '',
                     size_logical INTEGER NOT NULL DEFAULT 0,
                     updated_at INTEGER NOT NULL DEFAULT 0,
                     storage_class TEXT NOT NULL DEFAULT 'Standard',
                     owner_id TEXT NOT NULL DEFAULT '',
                     UNIQUE (bucket_name, key, version_id)
                 );
                 CREATE INDEX idx_ov_latest_cover ON object_versions
                     (bucket_name, key, version_id, is_delete_marker, etag, size_logical,
                      updated_at, storage_class, owner_id)
                     WHERE is_latest = 1;
                 CREATE TABLE object_locks (
                     bucket_name TEXT NOT NULL,
                     key TEXT NOT NULL,
                     version_id TEXT NOT NULL,
                     lock_mode TEXT,
                     retain_until INTEGER,
                     legal_hold INTEGER NOT NULL DEFAULT 0,
                     PRIMARY KEY (bucket_name, key, version_id)
                 );",
            )
            .await
            .unwrap();
        driver
            .execute(
                "INSERT INTO object_shares
                 (token, bucket_name, key, disposition, created_by, created_at)
                 VALUES (?1, 'photos', 'private.jpg', 'inline', 'admin', 1)",
                vec![Value::Text(ASYNC_LEGACY_SHARE_SENTINEL.to_owned())],
            )
            .await
            .unwrap();

        run_migrations(driver).await.unwrap();
        assert!(!column_exists(driver, "object_shares", "token").await);
        assert!(column_exists(driver, "object_shares", "id").await);
        assert!(column_exists(driver, "object_shares", "token_hash").await);
        let rows = driver
            .query(
                "SELECT id, length(token_hash), revoked_at FROM object_shares",
                vec![],
            )
            .await
            .unwrap();
        let row = rows.first().unwrap();
        assert!(row.get_text(0).starts_with("legacy-"));
        assert_eq!(row.get_i64(1), 32);
        assert!(row.get_i64(2) > 1);

        let old_hash = cairn_types::ShareLookupHash::for_token(ASYNC_LEGACY_SHARE_SENTINEL);
        let matches = driver
            .query(
                "SELECT COUNT(*) FROM object_shares WHERE token_hash=?1",
                vec![Value::Blob(old_hash.as_bytes().to_vec())],
            )
            .await
            .unwrap();
        assert_eq!(matches.first().unwrap().get_i64(0), 0);
    }

    async fn assert_v27_discards_only_legacy_orphan_lock_rows(driver: &dyn AsyncSqlDriver) {
        driver
            .execute_batch(
                "PRAGMA foreign_keys=ON;
                 CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY,
                     name TEXT NOT NULL,
                     applied_at INTEGER NOT NULL
                 );
                 INSERT INTO schema_migrations VALUES (26, 'legacy fixture', 0);
                 CREATE TABLE object_versions (
                     id TEXT PRIMARY KEY,
                     bucket_name TEXT NOT NULL,
                     key TEXT NOT NULL,
                     version_id TEXT NOT NULL,
                     is_latest INTEGER NOT NULL DEFAULT 1,
                     is_delete_marker INTEGER NOT NULL DEFAULT 0,
                     etag TEXT NOT NULL DEFAULT '',
                     size_logical INTEGER NOT NULL DEFAULT 0,
                     updated_at INTEGER NOT NULL DEFAULT 0,
                     storage_class TEXT NOT NULL DEFAULT 'Standard',
                     owner_id TEXT NOT NULL DEFAULT '',
                     UNIQUE (bucket_name, key, version_id)
                 );
                 INSERT INTO object_versions (id, bucket_name, key, version_id)
                     VALUES ('row-live','b','live','v');
                 CREATE INDEX idx_ov_latest_cover ON object_versions
                     (bucket_name, key, version_id, is_delete_marker, etag, size_logical,
                      updated_at, storage_class, owner_id)
                     WHERE is_latest = 1;
                 CREATE TABLE object_locks (
                     bucket_name TEXT NOT NULL,
                     key TEXT NOT NULL,
                     version_id TEXT NOT NULL,
                     lock_mode TEXT,
                     retain_until INTEGER,
                     legal_hold INTEGER NOT NULL DEFAULT 0,
                     PRIMARY KEY (bucket_name, key, version_id)
                 );
                 INSERT INTO object_locks VALUES ('b','live','v','COMPLIANCE',100,0);
                 INSERT INTO object_locks VALUES ('b','orphan','v','GOVERNANCE',100,1);
                 CREATE TABLE multipart_uploads (
                     id TEXT PRIMARY KEY,
                     status TEXT NOT NULL
                 );
                 INSERT INTO multipart_uploads VALUES ('legacy-upload','active');",
            )
            .await
            .unwrap();

        run_migrations(driver).await.unwrap();
        let locks = driver
            .query(
                "SELECT key, lock_mode FROM object_locks ORDER BY key",
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].get_text(0), "live");
        assert_eq!(locks[0].get_text(1), "COMPLIANCE");
        let legacy = driver
            .query(
                "SELECT object_lock_intent_known FROM multipart_uploads
                 WHERE id='legacy-upload'",
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(legacy[0].get_i64(0), 0);
    }

    async fn assert_v28_adds_row_identity_to_current_listing_cover(driver: &dyn AsyncSqlDriver) {
        driver
            .execute_batch(
                "CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY,
                     name TEXT NOT NULL,
                     applied_at INTEGER NOT NULL
                 );
                 INSERT INTO schema_migrations VALUES (27, 'legacy fixture', 0);
                 CREATE TABLE object_versions (
                     id TEXT PRIMARY KEY,
                     bucket_name TEXT NOT NULL,
                     key TEXT NOT NULL,
                     version_id TEXT NOT NULL,
                     is_latest INTEGER NOT NULL,
                     is_delete_marker INTEGER NOT NULL,
                     etag TEXT NOT NULL,
                     size_logical INTEGER NOT NULL,
                     updated_at INTEGER NOT NULL,
                     storage_class TEXT NOT NULL,
                     owner_id TEXT NOT NULL
                 );
                 CREATE INDEX idx_ov_latest_cover ON object_versions
                     (bucket_name, key, version_id, is_delete_marker, etag, size_logical,
                      updated_at, storage_class, owner_id)
                     WHERE is_latest = 1;
                 CREATE TABLE multipart_uploads (
                     id TEXT PRIMARY KEY,
                     status TEXT NOT NULL
                 );",
            )
            .await
            .unwrap();

        run_migrations(driver).await.unwrap();
        let columns = driver
            .query(
                "SELECT name FROM pragma_index_info('idx_ov_latest_cover') ORDER BY seqno",
                vec![],
            )
            .await
            .unwrap()
            .iter()
            .map(|row| row.get_text(0))
            .collect::<Vec<_>>();
        assert_eq!(
            columns,
            [
                "bucket_name",
                "key",
                "version_id",
                "id",
                "is_delete_marker",
                "etag",
                "size_logical",
                "updated_at",
                "storage_class",
                "owner_id",
            ]
        );
    }

    async fn assert_v29_resets_unowned_legacy_completion_claims(driver: &dyn AsyncSqlDriver) {
        driver
            .execute_batch(
                "CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY,
                     name TEXT NOT NULL,
                     applied_at INTEGER NOT NULL
                 );
                 INSERT INTO schema_migrations VALUES (28, 'legacy fixture', 0);
                 CREATE TABLE multipart_uploads (
                     id TEXT PRIMARY KEY,
                     status TEXT NOT NULL
                 );
                 INSERT INTO multipart_uploads VALUES ('active-upload','active');
                 INSERT INTO multipart_uploads VALUES ('orphaned-completer','completing');",
            )
            .await
            .unwrap();

        run_migrations(driver).await.unwrap();
        let rows = driver
            .query(
                "SELECT id, status, completion_claim_token
                 FROM multipart_uploads ORDER BY id",
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get_text(0), "active-upload");
        assert_eq!(rows[0].get_text(1), "active");
        assert!(rows[0].get_opt_text(2).is_none());
        assert_eq!(rows[1].get_text(0), "orphaned-completer");
        assert_eq!(rows[1].get_text(1), "active");
        assert!(rows[1].get_opt_text(2).is_none());
    }

    #[tokio::test]
    async fn migration_v21_adds_multipart_part_encryption() {
        let driver = migrated_driver().await;
        assert!(column_exists(driver.as_ref(), "multipart_uploads", "encrypt_parts").await);
        assert!(column_exists(driver.as_ref(), "multipart_parts", "part_dek").await);
    }

    #[tokio::test]
    async fn migration_v22_adds_multipart_kms_intent() {
        let driver = migrated_driver().await;
        assert!(column_exists(driver.as_ref(), "multipart_uploads", "sse_kms_requested").await);
        assert!(column_exists(driver.as_ref(), "multipart_uploads", "sse_kms_key_id").await);
        assert!(
            column_exists(
                driver.as_ref(),
                "multipart_uploads",
                "sse_bucket_key_enabled"
            )
            .await
        );
    }

    #[tokio::test]
    async fn migration_v25_revokes_legacy_share_in_libsql() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("libsql-share-v25.db");
        let db = libsql::Builder::new_local(&db_path).build().await.unwrap();
        let conn = db.connect().unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .await
            .unwrap();
        let driver = LibsqlDriver::new(conn);
        assert_v25_legacy_share_is_revoked(&driver).await;
        drop(driver);
        drop(db);

        for path in [db_path.clone(), db_path.with_extension("db-wal")] {
            let bytes = std::fs::read(&path).unwrap_or_default();
            assert!(
                !bytes
                    .windows(ASYNC_LEGACY_SHARE_SENTINEL.len())
                    .any(|window| window == ASYNC_LEGACY_SHARE_SENTINEL.as_bytes()),
                "libSQL sanitation left the legacy bearer plaintext in {}",
                path.display()
            );
        }
    }

    #[tokio::test]
    async fn migration_v25_revokes_legacy_share_in_turso() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("turso-share-v25.db");
        let db = turso::Builder::new_local(db_path.to_str().unwrap())
            .experimental_vacuum(true)
            .build()
            .await
            .unwrap();
        let conn = db.connect().unwrap();
        let driver = TursoDriver::new(conn);
        assert_v25_legacy_share_is_revoked(&driver).await;
        drop(driver);
        drop(db);

        let bytes = std::fs::read(&db_path).unwrap();
        assert!(
            !bytes
                .windows(ASYNC_LEGACY_SHARE_SENTINEL.len())
                .any(|window| window == ASYNC_LEGACY_SHARE_SENTINEL.as_bytes()),
            "Turso VACUUM left the legacy bearer plaintext in the database image"
        );
    }

    #[tokio::test]
    async fn migration_v27_discards_only_legacy_orphan_locks_in_libsql() {
        let name = format!(
            "file:cairn-libsql-lock-v27-{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4().simple()
        );
        #[allow(deprecated)]
        let db = Database::open(name).unwrap();
        let conn = db.connect().unwrap();
        let driver = LibsqlDriver::new(conn);
        assert_v27_discards_only_legacy_orphan_lock_rows(&driver).await;
    }

    #[tokio::test]
    async fn migration_v27_discards_only_legacy_orphan_locks_in_turso() {
        let db = turso::Builder::new_local(":memory:").build().await.unwrap();
        let conn = db.connect().unwrap();
        let driver = TursoDriver::new(conn);
        assert_v27_discards_only_legacy_orphan_lock_rows(&driver).await;
    }

    #[tokio::test]
    async fn migration_v28_covers_row_identity_in_libsql() {
        let name = format!(
            "file:cairn-libsql-row-id-v28-{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4().simple()
        );
        #[allow(deprecated)]
        let db = Database::open(name).unwrap();
        let conn = db.connect().unwrap();
        let driver = LibsqlDriver::new(conn);
        assert_v28_adds_row_identity_to_current_listing_cover(&driver).await;
    }

    #[tokio::test]
    async fn migration_v28_covers_row_identity_in_turso() {
        let db = turso::Builder::new_local(":memory:").build().await.unwrap();
        let conn = db.connect().unwrap();
        let driver = TursoDriver::new(conn);
        assert_v28_adds_row_identity_to_current_listing_cover(&driver).await;
    }

    #[tokio::test]
    async fn migration_v29_resets_legacy_claims_in_libsql() {
        let name = format!(
            "file:cairn-libsql-claim-v29-{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4().simple()
        );
        #[allow(deprecated)]
        let db = Database::open(name).unwrap();
        let conn = db.connect().unwrap();
        let driver = LibsqlDriver::new(conn);
        assert_v29_resets_unowned_legacy_completion_claims(&driver).await;
    }

    #[tokio::test]
    async fn migration_v29_resets_legacy_claims_in_turso() {
        let db = turso::Builder::new_local(":memory:").build().await.unwrap();
        let conn = db.connect().unwrap();
        let driver = TursoDriver::new(conn);
        assert_v29_resets_unowned_legacy_completion_claims(&driver).await;
    }
}
