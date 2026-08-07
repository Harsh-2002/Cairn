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
    Migration {
        version: 19,
        name: "replication outbox enqueue timestamp (true lag)",
        sql: r#"
-- The wall-clock millis an entry was first enqueued, so replication lag is the age of the oldest
-- still-unreplicated entry's ENQUEUE time, not its backed-off next_attempt_at (which a retry moves
-- into the future and would under-report a fresh backlog). Rows predating this column default to 0;
-- the lag query treats 0 as "unknown" (MIN over NULLIF(enqueued_at,0)) so a one-time upgrade never
-- spikes lag to epoch-age. The (status, enqueued_at) index backs the per-status aggregate + lag.
ALTER TABLE replication_outbox ADD COLUMN enqueued_at INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_outbox_status_enqueued ON replication_outbox (status, enqueued_at);
"#,
    },
    Migration {
        version: 20,
        name: "import jobs (S3 -> Cairn migration)",
        sql: r#"
-- An import job (ARCH 27): copy buckets + objects from a remote S3-compatible store into this node.
-- One account-global row per job; per-bucket progress + resume cursors live in the buckets_json
-- column (Cairn's JSON-column convention). The source admin secret is sealed under the master key
-- exactly like a user's SigV4 secret (CRK1 -> NULL nonce); it is never returned by any endpoint.
-- The aggregate counters are denormalized from buckets_json for cheap list rendering. lease_until is
-- the running-job claim lease: a row left 'running' with a stale lease is reclaimed at startup.
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
-- Persistent share bearer tokens were historically the table primary key. Revoke every legacy
-- capability and overwrite its plaintext value before rebuilding the table; secure_delete is
-- enabled by the migration runner and a successful TRUNCATE checkpoint is forced before startup
-- opens readers or serves requests.
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

-- Preserve non-capability history for operator visibility. The replacement id/hash are random and
-- unrelated to the destroyed bearer token; every copied row is already revoked by the UPDATE.
-- Shares are account-global on shard 0, while buckets are shard-local, so this table deliberately
-- has no impossible cross-database foreign key to `buckets`.
INSERT INTO object_shares
    (id, token_hash, bucket_name, key, version_id, expires_at, disposition, filename,
     created_by, created_at, revoked_at)
SELECT
    'legacy-' || lower(hex(randomblob(16))), randomblob(32), bucket_name, key, version_id,
    expires_at, disposition, filename, created_by, created_at, revoked_at
FROM object_shares_v24;

-- A durable one-row marker makes physical sanitation retryable. The migration transaction commits
-- before the checkpoint can run; if startup dies or the checkpoint is busy, the next startup sees
-- this row and retries before opening readers. Empty/fresh databases insert no marker.
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
-- Attribute staged bytes and active-session cardinality to the authenticated initiator rather
-- than implicitly to the bucket owner. Existing sessions predate that distinction and therefore
-- inherit their owner as the safest compatible attribution.
ALTER TABLE multipart_uploads ADD COLUMN initiated_by TEXT;
UPDATE multipart_uploads SET initiated_by=owner_id WHERE initiated_by IS NULL;

-- One writer-owned reservation exists before any part-attempt bytes reach durable staging. The
-- unique (upload, part) key serializes same-part attempts, so concurrent replacements cannot
-- multiply disk use behind one logical part number.
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

-- Bytes remain charged after metadata ownership ends until their exact file (replacement) or
-- complete session directory (abort/complete) is actually gone. No upload FK is intentional:
-- terminal cleanup debt must survive deletion of multipart_uploads.
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

-- O(1) quota/cardinality checks maintained inside the same writer savepoint as every multipart
-- transition. `staged_bytes` includes committed parts, pending reservations, and cleanup debt.
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

/// Highest schema version understood by this build.
pub(crate) fn latest_version() -> i64 {
    MIGRATIONS.last().map_or(0, |migration| migration.version)
}

/// Run all pending migrations on the write connection, recording each as applied.
pub fn run_migrations(conn: &Connection) -> rusqlite::Result<()> {
    // A v25 upgrade destroys legacy plaintext bearer capabilities. This must be enabled before the
    // UPDATE/table rebuild so deleted b-tree cells are zeroed rather than left in free pages.
    conn.execute_batch("PRAGMA secure_delete=ON;")?;
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
    if share_sanitation_pending(conn)? {
        // No read pool exists yet. Move the scrubbed v25 pages into the main database and truncate
        // every older WAL frame before the process can redeem a share or expose a raw backup.
        let busy: i64 = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
        if busy != 0 {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                Some("legacy share sanitation checkpoint was busy".to_owned()),
            ));
        }
        conn.execute("DELETE FROM share_capability_sanitation WHERE id=1", [])?;
    }
    Ok(())
}

fn share_sanitation_pending(conn: &Connection) -> rusqlite::Result<bool> {
    let table_exists: bool = conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master
             WHERE type='table' AND name='share_capability_sanitation'
         )",
        [],
        |row| row.get(0),
    )?;
    if !table_exists {
        return Ok(false);
    }
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM share_capability_sanitation WHERE id=1)",
        [],
        |row| row.get(0),
    )
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
    fn migration_v21_adds_multipart_part_encryption() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        assert!(column_exists(&conn, "multipart_uploads", "encrypt_parts"));
        assert!(column_exists(&conn, "multipart_parts", "part_dek"));
        // encrypt_parts defaults to 0 (legacy plaintext parts); part_dek defaults to NULL.
        conn.execute(
            "INSERT INTO multipart_uploads (id, bucket_name, key, content_type, status, owner_id, user_metadata, created_at, updated_at) \
             VALUES ('u', 'b', 'k', 'application/octet-stream', 'active', 'o', '[]', 0, 0)",
            [],
        )
        .unwrap();
        let ep: i64 = conn
            .query_row(
                "SELECT encrypt_parts FROM multipart_uploads WHERE id='u'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ep, 0, "encrypt_parts defaults to 0");
    }

    #[test]
    fn migration_v22_adds_multipart_kms_intent() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        assert!(column_exists(
            &conn,
            "multipart_uploads",
            "sse_kms_requested"
        ));
        assert!(column_exists(&conn, "multipart_uploads", "sse_kms_key_id"));
        assert!(column_exists(
            &conn,
            "multipart_uploads",
            "sse_bucket_key_enabled"
        ));
        // sse_kms_requested / sse_bucket_key_enabled default to 0; sse_kms_key_id defaults to NULL.
        conn.execute(
            "INSERT INTO multipart_uploads (id, bucket_name, key, content_type, status, owner_id, user_metadata, created_at, updated_at) \
             VALUES ('u', 'b', 'k', 'application/octet-stream', 'active', 'o', '[]', 0, 0)",
            [],
        )
        .unwrap();
        let (req, bke): (i64, i64) = conn
            .query_row(
                "SELECT sse_kms_requested, sse_bucket_key_enabled FROM multipart_uploads WHERE id='u'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(req, 0, "sse_kms_requested defaults to 0");
        assert_eq!(bke, 0, "sse_bucket_key_enabled defaults to 0");
    }

    #[test]
    fn migration_v23_adds_replication_completion_stamp_and_outbox_key_index() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        assert!(column_exists(&conn, "object_versions", "replicated_at"));
        // NULL for every existing row, including ones that replicated fine before the upgrade —
        // the audit deliberately reads that as suspect and re-ships them once (see the module doc
        // on `replication_audit` and `docs/operations.md` 8.7).
        conn.execute(
            "INSERT INTO object_versions (id, bucket_name, key, version_id, is_latest, is_delete_marker, \
             size_logical, size_physical, etag, content_type, compression, storage_class, owner_id, \
             user_metadata, checksums, created_at, updated_at) \
             VALUES ('i', 'b', 'k', 'v', 1, 0, 0, 0, 'e', 'text/plain', '\"Uncompressed\"', 'standard', 'o', '[]', '[]', 0, 0)",
            [],
        )
        .unwrap();
        let stamp: Option<i64> = conn
            .query_row(
                "SELECT replicated_at FROM object_versions WHERE id='i'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stamp, None, "replicated_at backfills as NULL, never as 0");

        // The key-paged requeue seeks `(bucket_name, key)` on the outbox; without this index every
        // page is a full scan, i.e. quadratic on exactly the buckets paging exists for.
        let idx: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_outbox_bucket_key'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn migration_v24_indexes_bounded_import_reads_and_retention() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        for index in [
            "idx_import_jobs_state_created_id",
            "idx_import_jobs_created_id",
            "idx_import_jobs_state_updated",
        ] {
            let present: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                    [index],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(present, 1, "missing import index {index}");
        }
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

    #[test]
    fn migration_v25_revokes_and_physically_scrubs_legacy_share_tokens() {
        const SENTINEL: &str = "cairn-legacy-share-plaintext-sentinel-029";

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("legacy-share.db");

        // Build the exact v24 share surface with the sentinel checkpointed into the main database,
        // so the v25 assertion cannot pass merely because the test value lived only in memory.
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 PRAGMA foreign_keys=ON;
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
            .unwrap();
            conn.execute(
                "INSERT INTO object_shares
                 (token, bucket_name, key, disposition, created_by, created_at)
                 VALUES (?1, 'photos', 'private.jpg', 'inline', 'admin', 1)",
                rusqlite::params![SENTINEL],
            )
            .unwrap();
            let busy: i64 = conn
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
                .unwrap();
            assert_eq!(busy, 0);
        }
        assert!(
            std::fs::read(&db_path)
                .unwrap()
                .windows(SENTINEL.len())
                .any(|window| window == SENTINEL.as_bytes()),
            "fixture must prove the plaintext was physically present before migration"
        );

        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 PRAGMA foreign_keys=ON;",
            )
            .unwrap();
            run_migrations(&conn).unwrap();

            assert!(!column_exists(&conn, "object_shares", "token"));
            assert!(column_exists(&conn, "object_shares", "id"));
            assert!(column_exists(&conn, "object_shares", "token_hash"));
            let (id, hash_len, revoked_at): (String, i64, Option<i64>) = conn
                .query_row(
                    "SELECT id, length(token_hash), revoked_at FROM object_shares",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert!(id.starts_with("legacy-"));
            assert_eq!(hash_len, 32);
            assert!(revoked_at.is_some_and(|at| at > 1));

            let old_hash = cairn_types::ShareLookupHash::for_token(SENTINEL);
            let redeemable: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM object_shares WHERE token_hash=?1",
                    rusqlite::params![old_hash.as_bytes().as_slice()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                redeemable, 0,
                "the legacy capability must no longer resolve"
            );
        }

        for path in [db_path.clone(), db_path.with_extension("db-wal")] {
            let bytes = std::fs::read(&path).unwrap_or_default();
            assert!(
                !bytes
                    .windows(SENTINEL.len())
                    .any(|window| window == SENTINEL.as_bytes()),
                "legacy bearer plaintext remained in {}",
                path.display()
            );
        }
    }

    #[test]
    fn migration_v27_constrains_locks_and_pins_multipart_intent() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        run_migrations(&conn).unwrap();

        for column in [
            "initial_tags",
            "lock_mode",
            "retain_until",
            "legal_hold",
            "object_lock_intent_known",
        ] {
            assert!(
                column_exists(&conn, "multipart_uploads", column),
                "v27 multipart column {column} must exist"
            );
        }

        conn.execute_batch(
            "INSERT INTO object_versions
             (id, bucket_name, key, version_id, is_latest, is_delete_marker, size_logical,
              size_physical, etag, content_type, compression, storage_class, owner_id,
              user_metadata, checksums, created_at, updated_at)
             VALUES ('i','b','k','v',1,0,1,1,'e','text/plain','\"Uncompressed\"','Standard',
                     'o','[]','[]',0,0);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO object_locks
             (bucket_name,key,version_id,lock_mode,retain_until,legal_hold)
             VALUES ('b','k','v','COMPLIANCE',100,1)",
            [],
        )
        .unwrap();

        assert!(
            conn.execute(
                "INSERT INTO object_locks
                 (bucket_name,key,version_id,lock_mode,retain_until,legal_hold)
                 VALUES ('b','missing','v','GOVERNANCE',100,0)",
                [],
            )
            .is_err(),
            "a lock may not outlive or precede its object version"
        );
        assert!(
            conn.execute(
                "UPDATE object_locks SET lock_mode='GOVERNANCE', retain_until=NULL
                 WHERE bucket_name='b' AND key='k' AND version_id='v'",
                [],
            )
            .is_err(),
            "retention mode/date must remain a complete pair"
        );

        conn.execute(
            "DELETE FROM object_versions
             WHERE bucket_name='b' AND key='k' AND version_id='v'",
            [],
        )
        .unwrap();
        let locks: i64 = conn
            .query_row("SELECT COUNT(*) FROM object_locks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(locks, 0, "version deletion must cascade its lock side row");
    }

    #[test]
    fn migration_v27_discards_only_legacy_orphan_lock_rows() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
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
                 (bucket_name, key, version_id, is_delete_marker, etag, size_logical, updated_at,
                  storage_class, owner_id)
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
        .unwrap();

        run_migrations(&conn).unwrap();
        let locks: Vec<(String, String)> = conn
            .prepare("SELECT key, lock_mode FROM object_locks ORDER BY key")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            locks,
            vec![("live".to_owned(), "COMPLIANCE".to_owned())],
            "the attached protection survives while the no-version orphan is cleaned"
        );
        let intent_known: i64 = conn
            .query_row(
                "SELECT object_lock_intent_known FROM multipart_uploads
                 WHERE id='legacy-upload'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            intent_known, 0,
            "a migrated session must remain distinguishable from new writer-pinned intent"
        );
    }

    #[test]
    fn migration_v28_adds_row_identity_to_current_listing_cover() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
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
                 (bucket_name, key, version_id, is_delete_marker, etag, size_logical, updated_at,
                  storage_class, owner_id)
                 WHERE is_latest = 1;
             CREATE TABLE multipart_uploads (
                 id TEXT PRIMARY KEY,
                 status TEXT NOT NULL
             );",
        )
        .unwrap();

        run_migrations(&conn).unwrap();
        let columns = conn
            .prepare("SELECT name FROM pragma_index_info('idx_ov_latest_cover') ORDER BY seqno")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
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

        let plan: String = conn
            .query_row(
                "EXPLAIN QUERY PLAN
                 SELECT key, version_id, is_latest, is_delete_marker, etag, size_logical,
                        updated_at, storage_class, owner_id, id
                 FROM object_versions
                 WHERE bucket_name = 'b' AND key >= '' AND is_latest = 1
                       AND is_delete_marker = 0
                 ORDER BY key ASC LIMIT 10",
                [],
                |row| row.get(3),
            )
            .unwrap();
        assert!(
            plan.contains("COVERING INDEX idx_ov_latest_cover"),
            "row-identity listing must remain index-only, plan was: {plan}"
        );
    }

    #[test]
    fn migration_v29_resets_unowned_legacy_completion_claims() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
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
        .unwrap();

        run_migrations(&conn).unwrap();
        let rows = conn
            .prepare(
                "SELECT id, status, completion_claim_token
                 FROM multipart_uploads ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows,
            [
                ("active-upload".to_owned(), "active".to_owned(), None),
                ("orphaned-completer".to_owned(), "active".to_owned(), None),
            ]
        );
    }
}
