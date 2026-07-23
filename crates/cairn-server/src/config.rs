//! The configuration surface (ARCH 28). Configuration is **environment-only**: the whole config
//! is `Config::default()` overlaid with `CAIRN_*` environment variables, so the binary runs on a
//! bare host or inside a container configured purely by env with no file to mount. The config is
//! validated on load so an invalid configuration fails fast with a clear message rather than at
//! first use.

use figment::Figment;
use figment::providers::{Env, Serialized};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Whether logs are emitted as human-readable text or machine-readable JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable text.
    Text,
    /// Newline-delimited JSON.
    Json,
}

/// The full server configuration. A subset of the ARCH 28.2 surface is wired in the
/// skeleton; later waves extend it (compression, quotas, replication, lifecycle, TLS).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Where the **S3 API** listener binds (`CAIRN_LISTEN_ADDR`): the S3 protocol, the signed
    /// public-read share URLs (`/p/…`), and the liveness/readiness/metrics endpoints. This is the
    /// data-plane port you expose to S3 clients. Default `0.0.0.0:7373`.
    pub listen_addr: SocketAddr,
    /// Where the **web UI** listener binds (`CAIRN_UI_ADDR`): the management console served at the
    /// root path, the management API (`/api/v1`), and the S3 data plane the console drives. This is
    /// the control-plane port you can firewall off from the internet. Default `0.0.0.0:7374`.
    /// Set it empty (or `off`/`none`/`disabled`) to run headless with no UI listener.
    pub ui_addr: String,
    /// Root of the staging and per-bucket blob directories.
    pub data_dir: PathBuf,
    /// Location of the SQLite metadata file.
    pub db_path: PathBuf,
    /// Which metadata backend drives the store (`CAIRN_META_BACKEND`). One of `sqlite` (the
    /// default rusqlite/bundled-C store), `libsql` (the async embedded libSQL driver), or `turso`
    /// (the pure-Rust SQLite rewrite). The on-disk database file is the same SQLite format for all
    /// three, so the choice is purely which engine drives it.
    pub meta_backend: String,
    /// External base URL used when generating URLs behind ingress.
    pub public_base_url: Option<String>,
    /// TLS certificate path (enables built-in TLS when set together with the key).
    pub tls_cert_path: Option<PathBuf>,
    /// TLS private-key path.
    pub tls_key_path: Option<PathBuf>,
    /// Maximum number of in-flight requests.
    pub concurrency_limit: usize,
    /// Per-request timeout, in seconds.
    pub request_timeout_secs: u64,
    /// Maximum time a connection may take to send its complete request head
    /// (`CAIRN_HEADER_READ_TIMEOUT_SECS`), before the connection is dropped. Bounds a slowloris that
    /// dribbles or never finishes the request line/headers — the per-request timeout only starts
    /// once the head is fully parsed, so without this a partial-head connection is held forever
    /// (audit 2026-07). Applies to both listeners.
    pub header_read_timeout_secs: u64,
    /// Maximum number of concurrent TCP connections accepted per listener
    /// (`CAIRN_MAX_CONNECTIONS`). A connection past the cap is dropped immediately (counted as
    /// `cairn_connections_rejected_total`), so a flood of idle/slow sockets can't exhaust file
    /// descriptors and memory ahead of the per-request concurrency limiter (audit 2026-07).
    pub max_connections: usize,
    /// Hard per-object size ceiling, in bytes.
    pub max_object_size: u64,
    /// The region label returned by the location operation and used in SigV4 scope checks.
    pub region: String,
    /// The 32-byte master key (64 hex chars) for envelope-encrypting secrets at rest. Required
    /// in production; absent, a fixed development key is used (insecure, for local testing).
    /// Ignored when [`master_key_ring`](Self::master_key_ring) is set.
    pub master_key: Option<String>,
    /// A master-key RING for rotation (`CAIRN_MASTER_KEY_RING`, audit #29): a JSON array of
    /// `{"id":<u16 1..65535>,"key":"<64-hex>"}`. When set it replaces `master_key`; new secrets
    /// seal under the active key id and old keys stay available to open existing data. Leave
    /// unset for a single-key deployment (no new config required).
    pub master_key_ring: Option<String>,
    /// Which ring id NEW seals use (`CAIRN_MASTER_KEY_ACTIVE_ID`). Defaults to the highest id in
    /// the ring. Must name a key present in [`master_key_ring`](Self::master_key_ring).
    pub master_key_active_id: Option<u16>,
    /// Log verbosity filter (e.g. `info`, `cairn=debug`).
    pub log_level: String,
    /// Log output format.
    pub log_format: LogFormat,
    /// Enable the development authentication bypass (loopback only; debug builds).
    pub dev_auth: bool,
    /// Acknowledge running with insecure development defaults on a non-loopback interface
    /// (`CAIRN_ALLOW_INSECURE`). Off by default: startup refuses to bind a public address while the
    /// built-in development master key (no `CAIRN_MASTER_KEY`) or the default root secret is in use,
    /// so a hurried deploy cannot come up fully functional and fully insecure. Set `true` only on a
    /// trusted/closed network where those defaults are acceptable (e.g. a demo or an internal rig).
    pub allow_insecure: bool,
    /// How often the lifecycle scanner applies each bucket's rules, in seconds.
    pub lifecycle_interval_secs: u64,
    /// How often the webhook event-notification worker drains the delivery outbox to the configured
    /// per-bucket endpoints, in seconds (`CAIRN_WEBHOOK_INTERVAL_SECS`, ARCH 20-style). The claim is a
    /// cheap indexed query, so the loop is a no-op for buckets without notifications; default 15s.
    pub webhook_interval_secs: u64,
    /// How often the multipart sweeper reclaims stale upload sessions, in seconds.
    pub multipart_sweep_interval_secs: u64,
    /// How often the background integrity scrub re-reads stored blobs and verifies them against the
    /// recorded ETag, in seconds (`CAIRN_SCRUB_INTERVAL_SECS`, ARCH 8.6/26.4). `0` (default) disables
    /// it. It verifies EVERY version — plaintext, compressed, and encrypted (an encrypted version is
    /// re-read through its own unsealed DEK, so an at-rest/SSE-S3/SSE-KMS node is covered; read-path
    /// AEAD authentication only ever covers bytes somebody GETs, which is not what a scrub is for) —
    /// turning silent on-disk bit-rot into a logged `cairn_scrub_corruption_total` event instead of
    /// serving a corrupted byte. Whatever it cannot verify is counted as
    /// `cairn_scrub_skipped_total{reason}` (an off-ring key mid-rotation, a composite multipart
    /// ETag), never silently dropped. It is bounded (paged enumeration) but reads every blob, so it is
    /// I/O-heavy — schedule it for quiet periods. A checksumming filesystem remains defense-in-depth.
    pub scrub_interval_secs: u64,
    /// How often the master-key re-wrap worker re-seals secrets onto the active key, in seconds
    /// (`CAIRN_KEY_REWRAP_INTERVAL_SECS`, audit #29 Phase D; `0` disables). SQLite backend only.
    pub key_rewrap_interval_secs: u64,
    /// How often the active key's seal counter is flushed to durable state, in seconds
    /// (`CAIRN_KEY_COUNTER_SYNC_SECS`, audit #29 Phase E; `0` disables). SQLite backend only.
    pub key_counter_sync_secs: u64,
    /// How long an idle multipart upload session lives before the sweeper aborts it, in seconds.
    pub multipart_upload_lifetime_secs: u64,
    /// How often the WAL checkpointer runs a truncating checkpoint, in seconds.
    pub wal_checkpoint_interval_secs: u64,
    /// Size threshold, in bytes, above which a truncating WAL checkpoint is triggered between the
    /// regular interval ticks (`CAIRN_WAL_CHECKPOINT_SIZE_BYTES`, ARCH 8.4). `0` disables the
    /// size-based trigger, leaving only the interval. Default 64 MiB.
    pub wal_checkpoint_size_bytes: u64,
    /// Metadata write durability (`CAIRN_META_SYNCHRONOUS`): `full` (default) or `normal` (ARCH 30).
    /// The default `full` runs `PRAGMA synchronous=FULL` under WAL: an acknowledged write is durable
    /// — it survives power loss — at the cost of a per-commit fsync that the group-commit writer
    /// amortizes across concurrent writes (see `CAIRN_META_GROUP_COMMIT_LINGER_MICROS`). `normal` is
    /// an opt-in throughput mode (`PRAGMA synchronous=NORMAL`, ≈1.7× writer throughput, no per-commit
    /// fsync) that never corrupts but may lose the last few uncheckpointed transactions on power loss.
    pub meta_synchronous: String,
    /// Group-commit linger window in microseconds (`CAIRN_META_GROUP_COMMIT_LINGER_MICROS`): how
    /// long the single writer waits to coalesce more concurrent writes into one commit. Default `0`
    /// (off). Lingering amortizes the per-commit fsync under the default `synchronous=full` when many
    /// writes are concurrent (raise it for write-heavy concurrency); under `synchronous=normal` there
    /// is no per-commit fsync to amortize, so it only adds latency. Capped at 1000 (1 ms).
    pub meta_group_commit_linger_micros: u64,
    /// Number of read-only WAL connections in the metadata read pool
    /// (`CAIRN_META_READ_POOL_SIZE`). Default `max(8, cpu_count)`, capped at 64. Readers take
    /// independent WAL snapshots and never block the writer, so this scales concurrent read
    /// throughput; the cap bounds memory (each reader holds its own page cache, see below).
    pub meta_read_pool_size: u32,
    /// Page-cache budget per metadata connection, in bytes (`CAIRN_META_CACHE_BYTES_PER_CONN`).
    /// Default 64 MiB. Total provisioned cache is roughly this × `(read_pool_size + 1)`.
    pub meta_cache_bytes_per_conn: u64,
    /// Hard ceiling, in bytes, on the total metadata page cache across all connections
    /// (`CAIRN_META_CACHE_TOTAL_BUDGET_BYTES`). Default 2 GiB. Startup refuses a configuration whose
    /// `cache_bytes_per_conn × (read_pool_size + 1)` exceeds this, so a large pool cannot silently
    /// provision enough cache to OOM the process.
    pub meta_cache_total_budget_bytes: u64,
    /// `mmap_size` in bytes for metadata read connections (`CAIRN_META_MMAP_BYTES`). Default 256 MiB.
    pub meta_mmap_bytes: u64,
    /// Number of metadata shards (`CAIRN_META_SHARDS`, ARCH 30, Phase 3.2). Default `1` (the
    /// metadata lives in one database, as before). With `N > 1` the metadata is partitioned across
    /// N databases by bucket name — `db_path`, then `db_path.shard1`, `.shard2`, … — so disjoint
    /// buckets commit through N independent single-writers in parallel. This is a **deployment-time
    /// decision fixed at first init**: changing it on populated data would route a bucket to a shard
    /// that does not hold its rows. Supported on the `sqlite` backend only; capped at 64. User-quota
    /// enforcement becomes eventually-consistent under sharding (it cannot be atomic across shards).
    /// Each shard opens its **own** writer + read pool, so the SQLite page-cache and blocking-thread
    /// footprint scale by `N`: with `N > 1` you will likely need to raise
    /// `CAIRN_META_CACHE_TOTAL_BUDGET_BYTES` (or lower `CAIRN_META_CACHE_BYTES_PER_CONN`), since the
    /// startup budget check now accounts for the shard multiplier and refuses an over-provisioned ring.
    pub meta_shards: usize,
    /// The base domain for **virtual-host-style** S3 addressing (`CAIRN_S3_DOMAIN`), e.g.
    /// `s3.example.com`. When set, a request whose `Host` is `<bucket>.<domain>` is routed to that
    /// bucket with the whole path as the key; path-style addressing always remains supported. Unset
    /// disables virtual-host routing (path-style only). (ARCH 13.1)
    pub s3_domain: Option<String>,
    /// Byte budget for the in-memory metadata/configuration cache (`CAIRN_META_CACHE_BYTES`, ARCH
    /// 11.5). The cache fronts hot bucket-config reads (policy/ACL/CORS/public-access-block) so
    /// authorization does not re-read SQLite on every request. `0` disables the cache. Default
    /// 64 MiB.
    pub meta_cache_bytes: u64,
    /// Time-to-live, in seconds, for the authentication cache (`CAIRN_AUTH_CACHE_TTL_SECS`, ARCH
    /// 30). It memoizes the per-request credential lookup (sealed secret + the user fields a
    /// principal needs) keyed by access-key-id and the parsed identity policy keyed by user-id, so
    /// a stream of requests from one identity skips two metadata reads and a policy parse. Changes
    /// to a user's credentials, active state, or policy take effect immediately regardless of the
    /// TTL (a user mutation bumps a shared epoch that drops every cached entry); the TTL only bounds
    /// staleness for entries no mutation ever touches. `0` disables the cache. Default 30 s.
    pub auth_cache_ttl_secs: u64,
    /// Maximum number of concurrent blob transfers (`CAIRN_BLOB_IO_POOL_SIZE`, ARCH 7.4). Each
    /// object read/write/assemble holds one permit for its file I/O, so a flood of large transfers
    /// cannot exhaust the runtime's blocking-pool threads. Tune to the device's useful I/O
    /// concurrency: lower for a single spinning disk, higher for a fast NVMe array. Default 64.
    pub blob_io_pool_size: usize,
    /// Bound on concurrent blob **read** transfers (`CAIRN_BLOB_IO_READ_POOL_SIZE`), separate from
    /// the (write) `blob_io_pool_size`. A read holds its permit for the whole client-paced download,
    /// so a flood of slow readers can exhaust this pool — keeping it distinct means they starve only
    /// other reads, never writes (audit 2026-07). Default 64.
    pub blob_io_read_pool_size: usize,
    /// Tokio runtime worker (compute) threads (`CAIRN_RUNTIME_WORKER_THREADS`, ARCH 30). `0` lets
    /// the runtime pick the CPU count (the default). Set it to pin compute parallelism explicitly.
    pub runtime_worker_threads: usize,
    /// Tokio runtime max blocking threads (`CAIRN_RUNTIME_MAX_BLOCKING_THREADS`, ARCH 30): the cap
    /// on threads serving `spawn_blocking`, which is where every metadata read (the WAL read pool)
    /// and blob file transfer runs. `0` derives a safe value: `max(512, blob_io_pool_size +
    /// meta_read_pool_size + 64)`, so the blocking pool can never be starved below the concurrency
    /// those two pools demand. A non-zero value is validated to stay at or above that floor.
    pub runtime_max_blocking_threads: usize,
    /// Replication destination endpoint (e.g. `http://backup-host:9000`). When set, the
    /// replication worker ships outbox entries to this S3-compatible target (ARCH 20).
    pub replication_endpoint: Option<String>,
    /// Destination bucket at the replication endpoint (path-style).
    pub replication_dest_bucket: Option<String>,
    /// Destination access-key id.
    pub replication_access_key: Option<String>,
    /// Destination secret access key.
    pub replication_secret: Option<String>,
    /// Destination signing region (defaults to `region` when unset).
    pub replication_region: Option<String>,
    /// How often the replication worker drains the outbox, in seconds.
    pub replication_interval_secs: u64,
    /// A JSON array of named replication targets (`CAIRN_REPLICATION_TARGETS`). When present each
    /// source bucket's destination is resolved to the matching named target (by the target's
    /// `dest_bucket` or `name`) and shipped with that target's own endpoint, credentials, and TLS
    /// trust (ARCH 20). The single-target `CAIRN_REPLICATION_*` keys above remain as the default
    /// target used for any source bucket that does not match a named target. Each element is a
    /// [`ReplicationTarget`]; parsed with `serde_json` on load.
    pub replication_targets: Option<String>,
    /// How many due outbox entries one replication drain pass claims at once
    /// (`CAIRN_REPLICATION_BATCH_SIZE`).
    pub replication_batch_size: u32,
    /// How many replication worker tasks drain the outbox concurrently
    /// (`CAIRN_REPLICATION_WORKER_CONCURRENCY`). Per-key, per-target ordering is preserved by the
    /// durable claim lease and the predecessor check regardless of pool size.
    pub replication_worker_concurrency: usize,
    /// Max delivery attempts before a retryable replication failure becomes terminal
    /// (`CAIRN_REPLICATION_MAX_ATTEMPTS`).
    pub replication_max_attempts: u32,
    /// Base (and minimum) retry backoff in seconds (`CAIRN_REPLICATION_BASE_BACKOFF_SECS`).
    pub replication_base_backoff_secs: u64,
    /// Ceiling on the exponential retry backoff in seconds (`CAIRN_REPLICATION_MAX_BACKOFF_SECS`);
    /// must be `>=` the base backoff.
    pub replication_max_backoff_secs: u64,
    /// Retention for terminal replication-outbox rows in seconds
    /// (`CAIRN_REPLICATION_RETENTION_SECS`): completed and failed entries older than this are
    /// periodically reclaimed so the outbox stays a bounded work queue rather than a permanent
    /// per-object ledger. Pending/claimed entries (outstanding work) are never pruned.
    pub replication_retention_secs: u64,
    /// Allow replicating an object the CLIENT asked us to encrypt (SSE-S3 / SSE-KMS) to a plaintext
    /// `http://` replication endpoint (`CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP`, ARCH 20).
    ///
    /// Replication ships the **decrypted** body: the engine unseals the version's DEK and reads the
    /// plaintext, because shipping the stored ciphertext would leave the mirror holding bytes no
    /// destination can decrypt. For a client-encrypted object over an `http://` endpoint that puts
    /// data the client explicitly asked to have encrypted onto an unauthenticated, unencrypted link.
    ///
    /// Default `false`: such an object is refused per-object as *unavailable*, so it is rescheduled
    /// without consuming its attempt budget and ships by itself once the endpoint becomes `https://`
    /// (or this is set). Nothing else is affected — unencrypted objects, and objects encrypted only
    /// by transparent at-rest encryption (`CAIRN_ENCRYPT_AT_REST`, an operator storage property
    /// rather than a client contract), replicate over `http://` exactly as before.
    ///
    /// Set `true` only when the link is trusted by other means (an in-datacentre VLAN, a VPN, a
    /// service mesh doing the TLS). It emits a loud startup warning.
    pub replication_allow_plaintext_sse_over_http: bool,
    /// The cutoff for the background **encrypted-suspect audit** — an RFC-3339 timestamp
    /// (`2026-07-23T10:00:00Z`) or whole epoch seconds (`CAIRN_REPLICATION_AUDIT_BEFORE`, ARCH 20.5,
    /// 26.2). Set it to the moment this node was upgraded past the SSE replication defect.
    ///
    /// **Unset by default, and unset means the loop does not run at all**: no version walk, no
    /// gauges, no warning, zero cost. That is deliberate. The pass counts encrypted versions stamped
    /// terminally replicated, and *only* versions written by the pre-fix binary can be damaged — a
    /// version encrypted and replicated after the fix is equally encrypted and equally `completed`,
    /// and is perfectly healthy. Without a cutoff the count is permanently non-zero on every healthy
    /// node that uses SSE with replication: an alert on it would fire forever and a runbook that
    /// says "watch it fall to zero" would be describing something that cannot happen. A permanent
    /// false alarm shipped as a default is worse than no alarm, so the operator opts in by supplying
    /// the one fact the node cannot know.
    ///
    /// With it set, `cairn_replication_encrypted_suspect_versions` and
    /// `cairn_replication_encrypted_repair_pending_versions` genuinely converge to zero as a repair
    /// completes, and alerting on either is meaningful. The one-off CLI (`cairn replication audit`)
    /// takes the same value as `--before` and does not require this knob.
    ///
    /// Validated at load: an unparseable value fails startup rather than surfacing six hours later
    /// inside a background loop.
    pub replication_audit_before: Option<String>,
    /// Retention for terminally-failed webhook-outbox (`events_outbox`) rows in seconds
    /// (`CAIRN_EVENTS_OUTBOX_RETENTION_SECS`): failed entries older than this are periodically
    /// reclaimed so a misconfigured/decommissioned webhook sink cannot grow the metadata DB (the
    /// single source of truth) without bound. Pending/claimed entries (outstanding work) are never
    /// pruned; delivered entries are removed on delivery.
    pub events_outbox_retention_secs: u64,

    /// Whether operator-configured outbound dialers (replication targets, webhook endpoints, S3
    /// import sources) may connect to an internal address (`CAIRN_ALLOW_INTERNAL_ENDPOINTS`).
    /// Default `false`: the SSRF guard refuses a destination that resolves to a loopback, private
    /// (RFC1918), link-local (incl. the cloud-metadata `169.254.169.254`), ULA, or unspecified
    /// address, at connect time (ARCH 27). Set `true` only when Cairn must reach storage on a
    /// private network (e.g. an on-prem MinIO on `10.x`); it emits a loud startup warning.
    pub allow_internal_endpoints: bool,

    /// Default object-worker count for a new S3 import job when the request does not specify one
    /// (`CAIRN_IMPORT_DEFAULT_WORKERS`, ARCH 27).
    pub import_default_workers: usize,
    /// Hard cap on an import job's object-worker count (`CAIRN_IMPORT_MAX_WORKERS`); a request asking
    /// for more is clamped. Must be ≥ `import_default_workers`.
    pub import_max_workers: usize,
    /// The authoritative ceiling on total in-flight object copies across an import job
    /// (`CAIRN_IMPORT_GLOBAL_MAX_INFLIGHT`). Held **below** the blob-I/O permit pool so a bulk import
    /// cannot starve the node's live GET/PUT traffic. Validated `< 64`.
    pub import_global_max_inflight: usize,
    /// How often the import worker wakes to claim a pending/stale job, in seconds
    /// (`CAIRN_IMPORT_POLL_INTERVAL_SECS`).
    pub import_poll_interval_secs: u64,
    /// Retention for finished (completed/failed/cancelled) import-job rows in seconds
    /// (`CAIRN_IMPORT_RETENTION_SECS`); older rows are pruned so the table stays bounded.
    pub import_retention_secs: u64,

    /// Whether the request-metrics usage-analytics subsystem is enabled
    /// (`CAIRN_REQUEST_METRICS_ENABLED`). When off, no per-request counters accumulate and the
    /// flush loop is not spawned; the `/api/v1/metrics/requests` endpoint then returns empty series
    /// (ARCH 26.5).
    pub request_metrics_enabled: bool,
    /// How often the in-process request-metrics aggregator is flushed to the rollup table and pruned,
    /// in seconds (`CAIRN_REQUEST_METRICS_FLUSH_SECS`).
    pub request_metrics_flush_secs: u64,
    /// The rollup window granularity in seconds (`CAIRN_REQUEST_METRICS_BUCKET_SECS`): request counts
    /// are floored to this window before storage. Smaller is finer-grained but more rows.
    pub request_metrics_bucket_secs: u64,
    /// How many days of request-metrics rollup rows to retain
    /// (`CAIRN_REQUEST_METRICS_RETENTION_DAYS`); older rows are pruned on each flush.
    pub request_metrics_retention_days: u64,

    /// Whether the AWS-STS wire surface is served on the S3 data plane
    /// (`CAIRN_STS_ENABLED`, ARCH 14). On by default (opt-out) so the AWS SDK default
    /// credential-provider chain and Terraform's `assume_role{}` block obtain temporary credentials
    /// (`AssumeRole`/`GetSessionToken`) with zero configuration. When `false`, a form `POST /` on the
    /// S3 listener is not intercepted for STS and falls through to normal S3 routing; the
    /// management-plane mint (`POST /api/v1/credentials/temporary`) is unaffected either way. Set
    /// `false` to shrink the attack surface if no SDK/Terraform client needs the standard mint.
    pub sts_enabled: bool,

    /// Whether every committed object is transparently encrypted at rest, even when the client did
    /// not request server-side encryption (`CAIRN_ENCRYPT_AT_REST`, ARCH 27). Off by default. When
    /// on, a PUT/copy/multipart-completion with no SSE header and no bucket default-encryption still
    /// mints a per-object DEK (sealed under the master key) and stores the blob encrypted — but the
    /// object advertises **no** `x-amz-server-side-encryption` (it is an operator storage property,
    /// not an SSE contract the client asked for; the `AtRest` descriptor mode records this). An
    /// explicit client SSE header or a bucket default still takes priority and IS advertised.
    /// This is a confidentiality/throughput trade: an encrypted object can engage neither the
    /// sendfile zero-copy nor the small-object whole-read GET fast path, so it stays opt-in.
    pub encrypt_at_rest: bool,

    /// The SSE-KMS key-id allow-list (`CAIRN_KMS_KEY_IDS`, ARCH 27): a comma-separated list of key
    /// ids accepted on an `x-amz-server-side-encryption: aws:kms` write (or a bucket KMS default).
    /// **Unset (the default) accepts ANY key id** — the key id is a *label*, not cryptographic
    /// isolation: every DEK is sealed under the same node master-key ring regardless of the id, and
    /// this list gates **writes only** (removing an id does not lock existing objects, whose reads
    /// unwrap under the master key). Set it to constrain which ids clients may name. Parsed by
    /// [`parse_kms_key_ids`](Self::parse_kms_key_ids); blank entries are ignored.
    pub kms_key_ids: Option<String>,

    /// The root administrator's access key (`CAIRN_ROOT_ACCESS_KEY`). On every startup an active
    /// administrator with this access key is ensured in the store; the same access key + secret work
    /// for the web UI login, the management API (as a Bearer token `access.secret`), and the S3 API
    /// (SigV4). Defaults to a well-known value for out-of-the-box access — override in production.
    pub root_access_key: String,
    /// The root administrator's secret key (`CAIRN_ROOT_SECRET_KEY`). Paired with
    /// [`root_access_key`](Self::root_access_key); see its docs.
    pub root_secret_key: String,

    /// Minimum response size, in bytes, for the experimental `sendfile` GET fast path
    /// (`CAIRN_FASTIO_MIN_BYTES`; only consulted in a `fast-io` build). The fast path now keeps the
    /// connection alive across requests, but each fast-pathed GET still hands the socket to a
    /// blocking thread for `sendfile` (with two non-blocking-mode toggles); for a tiny body that
    /// per-request overhead can outweigh the zero-copy saving over the normal streamed path, so a GET
    /// whose body is below this floor falls back to hyper instead. `0` disables the floor (every
    /// eligible GET takes the fast path). Defaults to 256 KiB — large enough that the sendfile saving
    /// dominates. Has no effect without `fast-io`.
    pub fastio_min_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:7373".parse().expect("valid default addr"),
            ui_addr: "0.0.0.0:7374".to_owned(),
            data_dir: PathBuf::from("./data"),
            db_path: PathBuf::from("./data/cairn.db"),
            meta_backend: "sqlite".to_owned(),
            public_base_url: None,
            tls_cert_path: None,
            tls_key_path: None,
            concurrency_limit: 1024,
            request_timeout_secs: 300,
            header_read_timeout_secs: 15,
            max_connections: 8192,
            max_object_size: 5 * 1024 * 1024 * 1024 * 1024, // 5 TiB
            region: "us-east-1".to_owned(),
            master_key: None,
            master_key_ring: None,
            master_key_active_id: None,
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            dev_auth: false,
            allow_insecure: false,
            lifecycle_interval_secs: 3600,
            webhook_interval_secs: 15,
            multipart_sweep_interval_secs: 3600,
            scrub_interval_secs: 0,
            key_rewrap_interval_secs: 300,
            key_counter_sync_secs: 60,
            multipart_upload_lifetime_secs: 86_400,
            wal_checkpoint_interval_secs: 300,
            wal_checkpoint_size_bytes: 64 * 1024 * 1024,
            meta_synchronous: "full".to_owned(),
            meta_group_commit_linger_micros: 0,
            // Scale read concurrency with the host; floor 8 so a small box still parallelizes
            // reads, cap 64 so the cache budget stays bounded.
            meta_read_pool_size: std::thread::available_parallelism()
                .map(|n| (n.get() as u32).clamp(8, 64))
                .unwrap_or(8),
            meta_cache_bytes_per_conn: 64 * 1024 * 1024,
            meta_cache_total_budget_bytes: 2 * 1024 * 1024 * 1024,
            meta_mmap_bytes: 256 * 1024 * 1024,
            meta_shards: 1,
            s3_domain: None,
            meta_cache_bytes: 64 * 1024 * 1024,
            auth_cache_ttl_secs: 30,
            blob_io_pool_size: 64,
            blob_io_read_pool_size: 64,
            runtime_worker_threads: 0,
            runtime_max_blocking_threads: 0,
            replication_endpoint: None,
            replication_dest_bucket: None,
            replication_access_key: None,
            replication_secret: None,
            replication_region: None,
            replication_interval_secs: 30,
            replication_targets: None,
            replication_batch_size: 64,
            replication_worker_concurrency: 4,
            replication_max_attempts: 8,
            replication_base_backoff_secs: 5,
            replication_max_backoff_secs: 900,
            replication_retention_secs: 86_400,
            // Refuse by default: a correctness fix (replicating plaintext instead of ciphertext)
            // must not silently start putting client-encrypted bodies on an unencrypted link.
            replication_allow_plaintext_sse_over_http: false,
            // Unset: the encrypted-suspect audit loop does not run and emits no gauge until an
            // operator supplies the cutoff. See the field doc — a gauge with no cutoff cannot
            // converge, and a permanently-firing default alarm is worse than none.
            replication_audit_before: None,
            events_outbox_retention_secs: 86_400,
            allow_internal_endpoints: false,
            import_default_workers: 8,
            import_max_workers: 32,
            import_global_max_inflight: 24,
            import_poll_interval_secs: 30,
            import_retention_secs: 604_800,
            request_metrics_enabled: true,
            request_metrics_flush_secs: 15,
            request_metrics_bucket_secs: 60,
            request_metrics_retention_days: 31,
            sts_enabled: true,
            encrypt_at_rest: false,
            kms_key_ids: None,
            root_access_key: "cairn".to_owned(),
            root_secret_key: "cairnadmin".to_owned(),
            fastio_min_bytes: 256 * 1024,
        }
    }
}

/// One entry of the `CAIRN_REPLICATION_TARGETS` JSON array: a named replication destination with
/// its own endpoint, credentials, and TLS trust knobs (ARCH 20). A source bucket is routed to the
/// target whose `dest_bucket` (or, failing that, `name`) matches the bucket's replication rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationTarget {
    /// A stable name for the target, used to match a source bucket's replication rule when the
    /// rule names the target rather than a destination bucket.
    pub name: String,
    /// The endpoint base URL, e.g. `https://s3.us-west-2.example.com`.
    pub endpoint: String,
    /// The SigV4 signing region for this target.
    pub region: String,
    /// The destination bucket (path-style) at this target.
    pub dest_bucket: String,
    /// The destination access-key id.
    pub access_key: String,
    /// The destination secret access key.
    pub secret: String,
    /// An optional path to a PEM file of CA certificates to trust for this target's TLS endpoint,
    /// instead of the built-in webpki roots. Honoured only for `https://` endpoints.
    #[serde(default)]
    pub ca_path: Option<PathBuf>,
    /// When true, the target's TLS server certificate is **not** verified. Dangerous; intended
    /// only for testing against a self-signed endpoint, and logged loudly when used.
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

impl Config {
    /// Parse the `replication_targets` JSON document into the typed target list. Returns an empty
    /// vector when no targets are configured.
    ///
    /// # Errors
    /// Returns a [`ConfigError::Parse`] if the JSON is malformed or does not match the
    /// [`ReplicationTarget`] shape.
    /// Resolve the web-UI listener address from [`ui_addr`](Self::ui_addr): `Some(addr)` to bind a
    /// UI listener, or `None` for headless mode (empty / `off` / `none` / `disabled`).
    ///
    /// # Errors
    /// Returns a [`ConfigError::Invalid`] if a non-empty value does not parse as `host:port`.
    pub fn ui_listen_addr(&self) -> Result<Option<SocketAddr>, ConfigError> {
        let v = self.ui_addr.trim();
        if v.is_empty() || matches!(v.to_ascii_lowercase().as_str(), "off" | "none" | "disabled") {
            return Ok(None);
        }
        v.parse::<SocketAddr>().map(Some).map_err(|e| {
            ConfigError::Invalid(format!("CAIRN_UI_ADDR {v:?} is not a valid host:port: {e}"))
        })
    }

    pub fn parse_replication_targets(&self) -> Result<Vec<ReplicationTarget>, ConfigError> {
        match &self.replication_targets {
            None => Ok(Vec::new()),
            Some(json) => serde_json::from_str(json).map_err(|e| {
                ConfigError::Parse(format!("invalid CAIRN_REPLICATION_TARGETS JSON: {e}"))
            }),
        }
    }

    /// Parse the `CAIRN_KMS_KEY_IDS` allow-list (comma-separated) into the SSE-KMS key-id list, or
    /// `None` when unset/blank (accept-all — the key id is a label, not a gate, ARCH 27). Blank
    /// entries are dropped; a value with no non-blank entries collapses to `None`.
    #[must_use]
    pub fn parse_kms_key_ids(&self) -> Option<Vec<String>> {
        let raw = self.kms_key_ids.as_deref()?;
        let ids: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect();
        if ids.is_empty() { None } else { Some(ids) }
    }
}

impl Config {
    /// Load configuration from the environment only: the built-in [`Config::default`] overlaid
    /// with `CAIRN_*` environment variables, then validated. There is no configuration file — a
    /// Cairn host or container is configured purely by env (ARCH 28).
    ///
    /// # Errors
    /// Returns a [`ConfigError`] if the environment fails to parse or validation fails.
    pub fn load() -> Result<Self, ConfigError> {
        let cfg: Config = Figment::from(Serialized::defaults(Config::default()))
            .merge(Env::prefixed("CAIRN_"))
            .extract()
            .map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Deployment guardrail (release hardening): refuse to serve on a non-loopback interface while
    /// the built-in development master key (no `CAIRN_MASTER_KEY`/ring) or the well-known default
    /// root secret is in use, so a hurried public deploy cannot come up fully functional yet fully
    /// insecure. Loopback binds and an explicit `CAIRN_ALLOW_INSECURE=true` (a trusted/closed
    /// network) are allowed. Called by both `serve` and `validate-config`, kept separate from
    /// `validate` so field validation stays pure and the bare defaults still parse.
    ///
    /// # Errors
    /// Returns [`ConfigError::Invalid`] when an insecure default is in use on a public bind.
    pub fn refuse_insecure_public_bind(&self) -> Result<(), ConfigError> {
        if self.listen_addr.ip().is_loopback() || self.allow_insecure {
            return Ok(());
        }
        if self.master_key.is_none() && self.master_key_ring.is_none() {
            return Err(ConfigError::Invalid(
                "refusing to serve on a non-loopback address with the built-in development master \
                 key: set CAIRN_MASTER_KEY (or CAIRN_MASTER_KEY_RING), or CAIRN_ALLOW_INSECURE=true \
                 to override on a trusted network"
                    .into(),
            ));
        }
        if self.root_secret_key == "cairnadmin" {
            return Err(ConfigError::Invalid(
                "refusing to serve on a non-loopback address with the default root secret: set \
                 CAIRN_ROOT_SECRET_KEY, or CAIRN_ALLOW_INSECURE=true to override on a trusted network"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Whether built-in TLS is configured.
    #[must_use]
    pub fn tls_enabled(&self) -> bool {
        self.tls_cert_path.is_some() && self.tls_key_path.is_some()
    }

    /// The minimum blocking-pool size the metadata read pool + blob I/O pool require so neither
    /// starves the other's `spawn_blocking` work, plus headroom for incidental blocking calls.
    ///
    /// Under `CAIRN_META_SHARDS>1` each shard opens its own read pool of `meta_read_pool_size` WAL
    /// connections, and every metadata read runs inside its own `spawn_blocking` task, so the real
    /// read-side demand is `meta_read_pool_size × meta_shards` (audit 2026-07: the floor undercounted
    /// it by a factor of `meta_shards`).
    fn blocking_pool_floor(&self) -> usize {
        self.blob_io_pool_size
            + self.blob_io_read_pool_size
            + self.meta_read_pool_size as usize * self.meta_shards
            + 64
    }

    /// The blocking-thread cap to configure the runtime with: the explicit value, or a derived safe
    /// default of `max(512, floor)` when unset (`0`). Validation guarantees an explicit value is at
    /// or above the floor.
    #[must_use]
    pub fn effective_max_blocking_threads(&self) -> usize {
        if self.runtime_max_blocking_threads != 0 {
            self.runtime_max_blocking_threads
        } else {
            self.blocking_pool_floor().max(512)
        }
    }

    /// The explicit worker-thread count, or `None` to let the runtime default to the CPU count.
    #[must_use]
    pub fn effective_worker_threads(&self) -> Option<usize> {
        (self.runtime_worker_threads != 0).then_some(self.runtime_worker_threads)
    }

    /// Validate the configuration, rejecting the cases ARCH 28.2 enumerates.
    ///
    /// # Errors
    /// Returns a [`ConfigError`] describing the first invalid setting.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(ConfigError::Invalid("data_dir must not be empty".into()));
        }
        if self.db_path.as_os_str().is_empty() {
            return Err(ConfigError::Invalid("db_path must not be empty".into()));
        }
        if !matches!(self.meta_backend.as_str(), "sqlite" | "libsql" | "turso") {
            return Err(ConfigError::Invalid(format!(
                "meta_backend must be one of sqlite|libsql|turso, got {:?}",
                self.meta_backend
            )));
        }
        // --- metadata throughput tuning (ARCH 28.2/30) ---
        if !matches!(self.meta_synchronous.as_str(), "normal" | "full") {
            return Err(ConfigError::Invalid(format!(
                "meta_synchronous must be normal|full, got {:?}",
                self.meta_synchronous
            )));
        }
        if self.meta_group_commit_linger_micros > 1000 {
            return Err(ConfigError::Invalid(
                "meta_group_commit_linger_micros must be <= 1000 (1 ms)".into(),
            ));
        }
        if !(1..=64).contains(&self.meta_read_pool_size) {
            return Err(ConfigError::Invalid(
                "meta_read_pool_size must be between 1 and 64".into(),
            ));
        }
        // The auth cache's TTL is a staleness backstop, not the primary invalidation (user
        // mutations drop entries immediately via the shared epoch). Cap it at one hour so a
        // mis-set value can never let a stale credential/policy linger unboundedly, while still
        // allowing `0` to disable the cache.
        if self.auth_cache_ttl_secs > 3600 {
            return Err(ConfigError::Invalid(
                "auth_cache_ttl_secs must be <= 3600 (1 hour); 0 disables the cache".into(),
            ));
        }
        // Metadata sharding (Phase 3.2): 1..=64, and >1 only on the sqlite backend (the libSQL /
        // Turso engines self-manage their WAL and are not wired for the per-shard checkpointer).
        if !(1..=64).contains(&self.meta_shards) {
            return Err(ConfigError::Invalid(
                "meta_shards must be between 1 and 64".into(),
            ));
        }
        if self.meta_shards > 1 && self.meta_backend != "sqlite" {
            return Err(ConfigError::Invalid(format!(
                "meta_shards > 1 is supported only on the sqlite backend, not {:?}",
                self.meta_backend
            )));
        }
        // Runtime blocking-pool floor: `spawn_blocking` serves both the metadata read pool and blob
        // file I/O, so an explicit cap set below their combined concurrency would stall reads and
        // transfers. `0` auto-derives a safe value (see `effective_max_blocking_threads`).
        let blocking_floor = self.blocking_pool_floor();
        if self.runtime_max_blocking_threads != 0
            && self.runtime_max_blocking_threads < blocking_floor
        {
            return Err(ConfigError::Invalid(format!(
                "runtime_max_blocking_threads ({}) is below the floor {} required by the blob I/O \
                 pool ({}) + metadata read pool ({}); raise it or set 0 to auto-derive",
                self.runtime_max_blocking_threads,
                blocking_floor,
                self.blob_io_pool_size,
                self.meta_read_pool_size,
            )));
        }
        // Cache-budget clamp (R3 guardrail): the writer connection plus every reader each provision
        // `cache_bytes_per_conn`, so a large pool can silently OOM the host. Refuse at startup.
        // Under sharding every shard opens an independent writer + read pool with the same sizing, so
        // the true footprint is `(read_pool_size + 1) × meta_shards` connections (audit 2026-07: the
        // clamp ignored `meta_shards`, so a sharded node could provision N× the budget and OOM).
        let conns =
            (u64::from(self.meta_read_pool_size) + 1).saturating_mul(self.meta_shards as u64);
        let total_cache = self.meta_cache_bytes_per_conn.saturating_mul(conns);
        if total_cache > self.meta_cache_total_budget_bytes {
            return Err(ConfigError::Invalid(format!(
                "metadata cache budget exceeded: {} bytes/conn × {} conns ({} shards) = {} > total \
                 budget {} (lower CAIRN_META_CACHE_BYTES_PER_CONN / CAIRN_META_READ_POOL_SIZE / \
                 CAIRN_META_SHARDS or raise CAIRN_META_CACHE_TOTAL_BUDGET_BYTES)",
                self.meta_cache_bytes_per_conn,
                conns,
                self.meta_shards,
                total_cache,
                self.meta_cache_total_budget_bytes
            )));
        }
        // W3 guardrail: the store disables inline auto-checkpointing (PRAGMA wal_autocheckpoint=0),
        // so the background checkpointer is the WAL's only bound. A disabled size trigger plus a
        // long interval could let the WAL grow large between checkpoints; require a deterministic
        // bound (a size trigger, or a sub-minute interval) for the sqlite/libsql backends.
        if self.meta_backend != "turso"
            && self.wal_checkpoint_size_bytes == 0
            && self.wal_checkpoint_interval_secs > 60
        {
            return Err(ConfigError::Invalid(
                "with inline WAL auto-checkpointing disabled, set CAIRN_WAL_CHECKPOINT_SIZE_BYTES > 0 \
                 or CAIRN_WAL_CHECKPOINT_INTERVAL_SECS <= 60 to keep the WAL bounded".into(),
            ));
        }
        if let Some(url) = &self.public_base_url {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(ConfigError::Invalid(
                    "public_base_url must be an http(s) URL".into(),
                ));
            }
        }
        match (&self.tls_cert_path, &self.tls_key_path) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(ConfigError::Invalid(
                    "TLS requires both tls_cert_path and tls_key_path".into(),
                ));
            }
            _ => {}
        }
        if self.request_timeout_secs == 0 {
            return Err(ConfigError::Invalid(
                "request_timeout_secs must be positive".into(),
            ));
        }
        if self.concurrency_limit == 0 {
            return Err(ConfigError::Invalid(
                "concurrency_limit must be positive".into(),
            ));
        }
        if self.header_read_timeout_secs == 0 {
            return Err(ConfigError::Invalid(
                "header_read_timeout_secs must be positive".into(),
            ));
        }
        if self.max_connections == 0 {
            return Err(ConfigError::Invalid(
                "max_connections must be positive".into(),
            ));
        }
        if self.max_object_size == 0 {
            return Err(ConfigError::Invalid(
                "max_object_size must be positive".into(),
            ));
        }
        if self.dev_auth && !self.listen_addr.ip().is_loopback() {
            return Err(ConfigError::Invalid(
                "dev_auth is only permitted on a loopback listen_addr".into(),
            ));
        }
        if self.lifecycle_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "lifecycle_interval_secs must be positive".into(),
            ));
        }
        if self.webhook_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "webhook_interval_secs must be positive".into(),
            ));
        }
        if self.multipart_sweep_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "multipart_sweep_interval_secs must be positive".into(),
            ));
        }
        if self.multipart_upload_lifetime_secs == 0 {
            return Err(ConfigError::Invalid(
                "multipart_upload_lifetime_secs must be positive".into(),
            ));
        }
        if self.wal_checkpoint_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "wal_checkpoint_interval_secs must be positive".into(),
            ));
        }
        // The replication drain cadence was previously unvalidated; a `0` would busy-spin the worker.
        if self.replication_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "replication_interval_secs must be positive".into(),
            ));
        }
        if self.replication_batch_size == 0 {
            return Err(ConfigError::Invalid(
                "replication_batch_size must be positive".into(),
            ));
        }
        if !(1..=64).contains(&self.replication_worker_concurrency) {
            return Err(ConfigError::Invalid(
                "replication_worker_concurrency must be between 1 and 64".into(),
            ));
        }
        if self.replication_max_attempts == 0 {
            return Err(ConfigError::Invalid(
                "replication_max_attempts must be positive".into(),
            ));
        }
        if self.replication_base_backoff_secs == 0 {
            return Err(ConfigError::Invalid(
                "replication_base_backoff_secs must be positive".into(),
            ));
        }
        if self.replication_max_backoff_secs < self.replication_base_backoff_secs {
            return Err(ConfigError::Invalid(
                "replication_max_backoff_secs must be >= replication_base_backoff_secs".into(),
            ));
        }
        if self.replication_retention_secs == 0 {
            return Err(ConfigError::Invalid(
                "replication_retention_secs must be positive".into(),
            ));
        }
        // The plaintext-SSE-over-http opt-in is only meaningful for an `http://` destination. If the
        // ONLY configured destination is the env single target and it is `https://`, the knob is
        // inert — and a knob that reads as "encrypted bodies may go out in the clear" must never sit
        // in an environment doing nothing, because the next operator will assume it is load-bearing.
        // Only checked when there is no target list and no chance of a console-configured target
        // shape being misjudged (`replication_targets` unset), so this can never refuse a startup
        // whose real destinations live outside the env.
        if self.replication_allow_plaintext_sse_over_http
            && self.replication_targets.is_none()
            && self
                .replication_endpoint
                .as_deref()
                .is_some_and(|e| e.starts_with("https://"))
        {
            return Err(ConfigError::Invalid(
                "replication_allow_plaintext_sse_over_http is set but the only configured \
                 replication endpoint is https:// — the knob has no effect; unset it"
                    .into(),
            ));
        }
        // Fail fast on an unparseable audit cutoff. The value is only consumed by a background loop
        // that first ticks minutes after startup, so without this check a typo would look like a
        // clean boot and then silently produce no gauges at all.
        if let Some(raw) = self.replication_audit_before.as_deref() {
            if let Err(e) = crate::replication_audit::parse_cutoff(raw) {
                return Err(ConfigError::Invalid(format!(
                    "replication_audit_before: {e}"
                )));
            }
        }
        if self.events_outbox_retention_secs == 0 {
            return Err(ConfigError::Invalid(
                "events_outbox_retention_secs must be positive".into(),
            ));
        }
        // Import knobs: workers positive & default ≤ max, the global in-flight cap held below the
        // blob-I/O permit pool (so an import can't starve live traffic), and cadences positive.
        if self.import_default_workers == 0 || self.import_max_workers == 0 {
            return Err(ConfigError::Invalid(
                "import_default_workers and import_max_workers must be positive".into(),
            ));
        }
        if self.import_default_workers > self.import_max_workers {
            return Err(ConfigError::Invalid(
                "import_default_workers must not exceed import_max_workers".into(),
            ));
        }
        if !(1..64).contains(&self.import_global_max_inflight) {
            return Err(ConfigError::Invalid(
                "import_global_max_inflight must be between 1 and 63 (below the blob-I/O pool)"
                    .into(),
            ));
        }
        if self.import_poll_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "import_poll_interval_secs must be positive".into(),
            ));
        }
        if self.import_retention_secs == 0 {
            return Err(ConfigError::Invalid(
                "import_retention_secs must be positive".into(),
            ));
        }
        // Request-metrics cadences must be positive when the subsystem is enabled, else the flush
        // loop would busy-spin and the rollup window would divide by zero (ARCH 26.5).
        if self.request_metrics_enabled {
            if self.request_metrics_flush_secs == 0 {
                return Err(ConfigError::Invalid(
                    "request_metrics_flush_secs must be positive".into(),
                ));
            }
            if self.request_metrics_bucket_secs == 0 {
                return Err(ConfigError::Invalid(
                    "request_metrics_bucket_secs must be positive".into(),
                ));
            }
            if self.request_metrics_retention_days == 0 {
                return Err(ConfigError::Invalid(
                    "request_metrics_retention_days must be positive".into(),
                ));
            }
        }
        // Master key / ring (audit #29). A ring (`CAIRN_MASTER_KEY_RING`) replaces the single key;
        // validate its JSON, ids, and active id at load so a typo fails fast rather than at the
        // first secret seal/open. Otherwise a present single `master_key` must be 64 hex chars.
        // (There is no separate public-read signing secret — the share-URL key derives from the
        // master key.)
        // A pinned active id only makes sense with a ring; without one it would be silently ignored
        // (a single `master_key` is always id 1), so reject the combination rather than mislead.
        if self.master_key_ring.is_none() && self.master_key_active_id.is_some() {
            return Err(ConfigError::Invalid(
                "CAIRN_MASTER_KEY_ACTIVE_ID is only valid together with CAIRN_MASTER_KEY_RING"
                    .into(),
            ));
        }
        if let Some(ring_json) = &self.master_key_ring {
            let keys = parse_key_ring(ring_json)
                .map_err(|e| ConfigError::Invalid(format!("CAIRN_MASTER_KEY_RING {e}")))?;
            if let Some(active) = self.master_key_active_id {
                if !keys.iter().any(|(id, _)| *id == active) {
                    return Err(ConfigError::Invalid(format!(
                        "CAIRN_MASTER_KEY_ACTIVE_ID {active} is not present in CAIRN_MASTER_KEY_RING"
                    )));
                }
                // Forward-only rotation (audit #29 / 2026-07): the active key must be the NEWEST
                // (highest id) in the ring. The retire-gate assumes ids increase and `active` is the
                // newest, so it only flags a removed key with `id < active`; rolling `active` BACK
                // below a higher-id ring key would let that newer key (id > active) be retired while
                // it still seals data — silent, unrecoverable loss. Rotate forward by adding a
                // higher-id key, never by lowering the active id.
                let max_id = keys.iter().map(|(id, _)| *id).max().unwrap_or(active);
                if active != max_id {
                    return Err(ConfigError::Invalid(format!(
                        "CAIRN_MASTER_KEY_ACTIVE_ID {active} must be the highest id in \
                         CAIRN_MASTER_KEY_RING (found {max_id}); rotate forward by adding a \
                         higher-id key, never by lowering the active id (audit #29)"
                    )));
                }
            }
        } else if let Some(mk) = &self.master_key {
            if mk.len() != 64 || !mk.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(ConfigError::Invalid(
                    "master_key must be 64 hex characters (a 32-byte key)".into(),
                ));
            }
        }
        // A malformed replication-targets document is an operator error that must surface at load,
        // not when the first drain tries to route an object. Reject targets that set both a CA
        // path and skip-verify, since the two trust knobs are mutually exclusive.
        for target in self.parse_replication_targets()? {
            if target.ca_path.is_some() && target.insecure_skip_verify {
                return Err(ConfigError::Invalid(format!(
                    "replication target {:?} sets both ca_path and insecure_skip_verify",
                    target.name
                )));
            }
        }
        // Validate (but don't bind) the UI listener address.
        let ui = self.ui_listen_addr()?;
        if let Some(ui) = ui {
            if ui == self.listen_addr {
                return Err(ConfigError::Invalid(
                    "CAIRN_UI_ADDR must differ from the S3 API listener (CAIRN_LISTEN_ADDR)".into(),
                ));
            }
        }
        Ok(())
    }
}

/// One entry of the master-key ring (`CAIRN_MASTER_KEY_RING`, audit #29). Strict like [`Config`]:
/// an unexpected field (e.g. a typo'd key name) is rejected rather than silently ignored.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeySpec {
    id: u16,
    key: String,
}

/// Parse and validate the master-key ring JSON into `(id, 32-byte key)` pairs: non-empty, no id 0,
/// no duplicate id, each key exactly 64 hex chars. Decoded key bytes are returned to the caller
/// (and never logged). Returns a human-readable reason on failure.
pub(crate) fn parse_key_ring(json: &str) -> Result<Vec<(u16, [u8; 32])>, String> {
    let specs: Vec<KeySpec> =
        serde_json::from_str(json).map_err(|e| format!("is not a valid JSON ring: {e}"))?;
    if specs.is_empty() {
        return Err("must contain at least one key".to_owned());
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(specs.len());
    for s in specs {
        if s.id == 0 {
            return Err("key id 0 is reserved".to_owned());
        }
        if !seen.insert(s.id) {
            return Err(format!("duplicate key id {}", s.id));
        }
        if s.key.len() != 64 || !s.key.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("key id {} must be 64 hex characters", s.id));
        }
        let bytes = hex::decode(&s.key).map_err(|_| format!("key id {} has invalid hex", s.id))?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| format!("key id {} must decode to 32 bytes", s.id))?;
        out.push((s.id, arr));
    }
    Ok(out)
}

/// A configuration load/validation error.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// A configuration layer failed to parse.
    #[error("failed to parse configuration: {0}")]
    Parse(String),
    /// A value was invalid.
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

#[cfg(test)]
// `figment::Jail::expect_with` takes a closure returning `figment::Result<()>`, whose `Err`
// variant (`figment::Error`) is large; the type is dictated by figment's API, not ours, so the
// `result_large_err` lint is not actionable for these env-isolation tests.
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;

    fn base() -> Config {
        Config::default()
    }

    #[test]
    fn default_is_valid() {
        assert!(base().validate().is_ok());
    }

    /// The plaintext-SSE-over-http opt-in must be OFF by default — replication now ships DECRYPTED
    /// bodies, and a correctness fix must not silently start putting client-encrypted data on an
    /// unauthenticated link — and must not be able to sit in an environment doing nothing.
    #[test]
    fn plaintext_sse_over_http_opt_in_defaults_off_and_refuses_being_inert() {
        assert!(
            !Config::default().replication_allow_plaintext_sse_over_http,
            "the default MUST be refuse"
        );
        // Set with an http:// endpoint: meaningful, accepted.
        let mut c = base();
        c.replication_allow_plaintext_sse_over_http = true;
        c.replication_endpoint = Some("http://peer.example.com:9000".to_owned());
        assert!(c.validate().is_ok());
        // Set with only an https:// endpoint and no target list: provably inert, refused — a knob
        // reading "encrypted bodies may go out in the clear" must never be decorative.
        c.replication_endpoint = Some("https://peer.example.com".to_owned());
        assert!(c.validate().is_err());
        // ...unless a target list is configured, whose endpoints are not judged here.
        c.replication_targets = Some("[]".to_owned());
        assert!(c.validate().is_ok());
    }

    /// The encrypted-suspect audit cutoff is OFF by default (so the background loop never runs and
    /// emits no gauge), accepts both operator-facing forms, and fails startup on a typo rather than
    /// surfacing as "no gauges appeared" six hours into an incident.
    #[test]
    fn replication_audit_before_defaults_off_and_is_validated() {
        assert!(
            Config::default().replication_audit_before.is_none(),
            "the audit loop must be opt-in: an unbounded suspect gauge cannot converge, and a \
             permanently-firing default alarm is worse than no alarm"
        );
        let mut c = base();
        assert!(c.validate().is_ok());
        c.replication_audit_before = Some("2026-07-23T10:00:00Z".to_owned());
        assert!(c.validate().is_ok(), "RFC-3339 is accepted");
        c.replication_audit_before = Some("1753264800".to_owned());
        assert!(c.validate().is_ok(), "bare epoch seconds are accepted");
        c.replication_audit_before = Some("last tuesday".to_owned());
        let err = c.validate().unwrap_err().to_string();
        assert!(
            err.contains("replication_audit_before"),
            "the failure must name the knob, got {err:?}"
        );
    }

    fn hex64(b: u8) -> String {
        format!("{b:02x}").repeat(32)
    }

    #[test]
    fn parse_key_ring_accepts_a_valid_ring() {
        let json = format!(
            r#"[{{"id":1,"key":"{}"}},{{"id":2,"key":"{}"}}]"#,
            hex64(0xab),
            hex64(0xcd)
        );
        let keys = parse_key_ring(&json).expect("valid ring");
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].0, 1);
        assert_eq!(keys[1].0, 2);
        assert_eq!(keys[0].1, [0xabu8; 32]);
    }

    #[test]
    fn active_key_id_must_be_the_highest_ring_id() {
        // Audit #29 / 2026-07: rotation is forward-only. The active key must be the newest (highest
        // id) in the ring — otherwise a ring key with id > active could be retired while it still
        // seals data (the retire-gate only flags id < active), an unrecoverable loss.
        let ring = format!(
            r#"[{{"id":1,"key":"{}"}},{{"id":2,"key":"{}"}},{{"id":3,"key":"{}"}}]"#,
            hex64(1),
            hex64(2),
            hex64(3)
        );
        let mut c = base();
        c.master_key = None;
        c.master_key_ring = Some(ring);
        // active = the highest id validates.
        c.master_key_active_id = Some(3);
        assert!(c.validate().is_ok(), "active == max ring id is valid");
        // active rolled back below a higher-id ring key is refused.
        c.master_key_active_id = Some(2);
        assert!(
            c.validate().is_err(),
            "active below the highest ring id must be rejected (pre-fix this was accepted)"
        );
        // Omitting active (defaults to the highest id) is valid.
        c.master_key_active_id = None;
        assert!(
            c.validate().is_ok(),
            "defaulting active to the max id is valid"
        );
    }

    #[test]
    fn parse_key_ring_rejects_malformed_rings() {
        assert!(parse_key_ring("not json").is_err());
        assert!(parse_key_ring("[]").is_err(), "empty ring");
        assert!(
            parse_key_ring(&format!(r#"[{{"id":0,"key":"{}"}}]"#, hex64(1))).is_err(),
            "id 0 reserved"
        );
        let dup = format!(
            r#"[{{"id":1,"key":"{}"}},{{"id":1,"key":"{}"}}]"#,
            hex64(1),
            hex64(2)
        );
        assert!(parse_key_ring(&dup).is_err(), "duplicate id");
        assert!(
            parse_key_ring(r#"[{"id":1,"key":"abcd"}]"#).is_err(),
            "key not 64 hex chars"
        );
        assert!(
            parse_key_ring(&format!(r#"[{{"id":1,"key":"{}","oops":1}}]"#, hex64(1))).is_err(),
            "unknown field rejected (deny_unknown_fields)"
        );
    }

    #[test]
    fn rejects_active_id_without_a_ring() {
        let mut c = base();
        c.master_key = Some(hex64(7));
        c.master_key_active_id = Some(2);
        assert!(c.validate().is_err(), "active id requires a ring");
        // With a matching ring it validates.
        c.master_key_ring = Some(format!(r#"[{{"id":2,"key":"{}"}}]"#, hex64(7)));
        assert!(c.validate().is_ok());
        // An active id absent from the ring is rejected.
        c.master_key_active_id = Some(9);
        assert!(
            c.validate().is_err(),
            "active id must be present in the ring"
        );
    }

    #[test]
    fn rejects_incomplete_tls() {
        let mut c = base();
        c.tls_cert_path = Some(PathBuf::from("/x/cert.pem"));
        assert!(c.validate().is_err());
        c.tls_key_path = Some(PathBuf::from("/x/key.pem"));
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_zero_timeout_and_concurrency() {
        let mut c = base();
        c.request_timeout_secs = 0;
        assert!(c.validate().is_err());
        let mut c = base();
        c.concurrency_limit = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_zero_slowloris_guards() {
        // Audit 2026-07: the header-read timeout (slowloris) and the connection cap must be positive
        // — a zero would disable the very guard, so it is a misconfiguration, not "unlimited".
        let mut c = base();
        c.header_read_timeout_secs = 0;
        assert!(c.validate().is_err());
        let mut c = base();
        c.max_connections = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_bad_public_url() {
        let mut c = base();
        c.public_base_url = Some("ftp://nope".into());
        assert!(c.validate().is_err());
        c.public_base_url = Some("https://ok.example".into());
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_zero_background_intervals() {
        for mutate in [
            (|c: &mut Config| c.lifecycle_interval_secs = 0) as fn(&mut Config),
            |c: &mut Config| c.webhook_interval_secs = 0,
            |c: &mut Config| c.multipart_sweep_interval_secs = 0,
            |c: &mut Config| c.multipart_upload_lifetime_secs = 0,
            |c: &mut Config| c.wal_checkpoint_interval_secs = 0,
        ] {
            let mut c = base();
            mutate(&mut c);
            assert!(c.validate().is_err());
        }
    }

    #[test]
    fn rejects_zero_request_metrics_cadences_when_enabled() {
        for mutate in [
            (|c: &mut Config| c.request_metrics_flush_secs = 0) as fn(&mut Config),
            |c: &mut Config| c.request_metrics_bucket_secs = 0,
            |c: &mut Config| c.request_metrics_retention_days = 0,
        ] {
            let mut c = base();
            mutate(&mut c);
            assert!(c.validate().is_err());
        }
        // The same zeros are tolerated when the subsystem is disabled (nothing reads them).
        let mut c = base();
        c.request_metrics_enabled = false;
        c.request_metrics_flush_secs = 0;
        c.request_metrics_bucket_secs = 0;
        c.request_metrics_retention_days = 0;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn metadata_tuning_defaults_validate() {
        // The metadata defaults (synchronous=full for durable acked writes, linger 0, cpu-scaled
        // pool, 64 MiB/conn) must pass validation out of the box.
        assert!(base().validate().is_ok());
        // Durability is safe by default: acknowledged writes survive power loss unless an operator
        // explicitly opts into the `normal` throughput mode.
        assert_eq!(Config::default().meta_synchronous, "full");
    }

    #[test]
    fn rejects_bad_metadata_tuning() {
        let cases: [fn(&mut Config); 4] = [
            |c| c.meta_synchronous = "sometimes".to_owned(),
            |c| c.meta_group_commit_linger_micros = 2000, // > 1 ms cap
            |c| c.meta_read_pool_size = 0,
            |c| c.meta_read_pool_size = 128, // > 64 cap
        ];
        for mutate in cases {
            let mut c = base();
            mutate(&mut c);
            assert!(c.validate().is_err());
        }
        // Both normal and full are accepted.
        for s in ["normal", "full"] {
            let mut c = base();
            c.meta_synchronous = s.to_owned();
            assert!(c.validate().is_ok());
        }
    }

    #[test]
    fn meta_shards_bounds_and_backend() {
        // 1 (default) and any value up to 64 validate on sqlite. Give the cache budget ample
        // headroom so this exercises the shard-count bound, not the (separate, shard-scaled) cache
        // clamp — at 64 shards the default per-conn cache legitimately exceeds the default budget.
        for n in [1usize, 2, 16, 64] {
            let mut c = base();
            c.meta_shards = n;
            c.meta_cache_total_budget_bytes = 256 * 1024 * 1024 * 1024;
            assert!(c.validate().is_ok(), "shards {n} on sqlite must validate");
        }
        // 0 and >64 are rejected.
        for n in [0usize, 65] {
            let mut c = base();
            c.meta_shards = n;
            assert!(c.validate().is_err(), "shards {n} must be rejected");
        }
        // >1 is sqlite-only.
        let mut c = base();
        c.meta_shards = 4;
        c.meta_backend = "libsql".to_owned();
        assert!(
            c.validate().is_err(),
            "sharding requires the sqlite backend"
        );
    }

    #[test]
    fn runtime_blocking_pool_floor_is_enforced() {
        let mut c = base();
        c.blob_io_pool_size = 64;
        c.blob_io_read_pool_size = 32;
        c.meta_read_pool_size = 16;
        c.meta_shards = 1;
        // floor = blob_write(64) + blob_read(32) + meta_read(16)*shards(1) + 64 = 176.
        let floor = c.blocking_pool_floor();
        assert_eq!(floor, 176);
        // 0 auto-derives max(512, floor) and validates.
        c.runtime_max_blocking_threads = 0;
        assert!(c.validate().is_ok());
        assert_eq!(c.effective_max_blocking_threads(), 512);
        // An explicit value at or above the floor validates.
        c.runtime_max_blocking_threads = floor;
        assert!(c.validate().is_ok());
        assert_eq!(c.effective_max_blocking_threads(), floor);
        // Below the floor is rejected (would starve reads/transfers).
        c.runtime_max_blocking_threads = floor - 1;
        assert!(c.validate().is_err());
        // Worker threads: 0 = auto (None), explicit = pinned.
        c.runtime_max_blocking_threads = 0;
        assert_eq!(c.effective_worker_threads(), None);
        c.runtime_worker_threads = 8;
        assert_eq!(c.effective_worker_threads(), Some(8));
    }

    #[test]
    fn auth_cache_ttl_bounds() {
        // 0 (disabled) and any value up to the one-hour cap validate; above it is refused.
        for ttl in [0_u64, 1, 30, 3600] {
            let mut c = base();
            c.auth_cache_ttl_secs = ttl;
            assert!(c.validate().is_ok(), "ttl {ttl} must validate");
        }
        let mut c = base();
        c.auth_cache_ttl_secs = 3601;
        assert!(
            c.validate().is_err(),
            "ttl above the 1-hour cap must be rejected"
        );
    }

    #[test]
    fn rejects_metadata_cache_budget_overflow() {
        // 64 readers + writer × 64 MiB ≈ 4.1 GiB, over the 2 GiB default budget → refuse.
        let mut c = base();
        c.meta_read_pool_size = 64;
        c.meta_cache_bytes_per_conn = 64 * 1024 * 1024;
        assert!(c.validate().is_err());
        // Raising the budget (or shrinking per-conn cache) makes it valid again.
        c.meta_cache_total_budget_bytes = 8 * 1024 * 1024 * 1024;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn cache_budget_accounts_for_shards() {
        // Regression (audit 2026-07): each shard opens its own writer + read pool, so the budget
        // clamp must scale with CAIRN_META_SHARDS. These settings are within budget at 1 shard but
        // provision N× as much page cache when sharded — the clamp must reject the sharded case.
        let mut c = base();
        c.meta_backend = "sqlite".to_owned();
        c.meta_read_pool_size = 8; // 9 conns/shard
        c.meta_cache_bytes_per_conn = 64 * 1024 * 1024; // 9 × 64 MiB = 576 MiB < 2 GiB at 1 shard
        c.meta_shards = 1;
        assert!(c.validate().is_ok(), "single shard is within budget");
        c.meta_shards = 8; // 72 conns × 64 MiB ≈ 4.6 GiB > 2 GiB
        assert!(
            c.validate().is_err(),
            "8 shards must exceed the cache budget (pre-fix this passed)"
        );
    }

    #[test]
    fn rejects_unbounded_wal_when_autocheckpoint_disabled() {
        // wal_autocheckpoint is always 0 now; a disabled size trigger + a long interval would let
        // the WAL grow unbounded between checkpoints (sqlite/libsql) → refuse (the W3 guardrail).
        let mut c = base();
        c.wal_checkpoint_size_bytes = 0;
        c.wal_checkpoint_interval_secs = 3600;
        assert!(c.validate().is_err());
        // A sub-minute interval is a sufficient bound on its own.
        c.wal_checkpoint_interval_secs = 30;
        assert!(c.validate().is_ok());
        // turso self-manages its WAL, so the guard does not apply there.
        let mut t = base();
        t.meta_backend = "turso".to_owned();
        t.wal_checkpoint_size_bytes = 0;
        t.wal_checkpoint_interval_secs = 3600;
        assert!(t.validate().is_ok());
    }

    #[test]
    fn accepts_custom_background_intervals() {
        let mut c = base();
        c.lifecycle_interval_secs = 600;
        c.multipart_sweep_interval_secs = 600;
        c.multipart_upload_lifetime_secs = 7200;
        c.wal_checkpoint_interval_secs = 60;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn meta_backend_defaults_to_sqlite_and_validates_choices() {
        // Default is sqlite (the byte-identical, unchanged default path).
        assert_eq!(base().meta_backend, "sqlite");
        for ok in ["sqlite", "libsql", "turso"] {
            let mut c = base();
            c.meta_backend = ok.to_owned();
            assert!(c.validate().is_ok(), "{ok} must be accepted");
        }
        // An unknown backend is rejected at load.
        let mut c = base();
        c.meta_backend = "postgres".to_owned();
        assert!(c.validate().is_err());
    }

    /// `CAIRN_META_BACKEND` selects the backend from the environment; unset leaves the default.
    #[test]
    fn load_reads_meta_backend_from_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_META_BACKEND", "libsql");
            assert_eq!(Config::load().expect("loads").meta_backend, "libsql");
            Ok(())
        });
        figment::Jail::expect_with(|_jail| {
            assert_eq!(Config::load().expect("loads").meta_backend, "sqlite");
            Ok(())
        });
    }

    /// The sendfile size floor defaults to 256 KiB and is overridable from the environment,
    /// including `0` to disable it. It needs no validation — any byte count is valid.
    #[test]
    fn load_reads_fastio_min_bytes_from_env() {
        assert_eq!(Config::default().fastio_min_bytes, 256 * 1024);
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_FASTIO_MIN_BYTES", "0");
            assert_eq!(Config::load().expect("loads").fastio_min_bytes, 0);
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_FASTIO_MIN_BYTES", "1048576");
            assert_eq!(Config::load().expect("loads").fastio_min_bytes, 1_048_576);
            Ok(())
        });
    }

    /// The AWS-STS wire surface is on by default (opt-out) and can be disabled from the environment.
    #[test]
    fn load_reads_sts_enabled_from_env() {
        assert!(
            Config::default().sts_enabled,
            "STS is on by default (opt-out)"
        );
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_STS_ENABLED", "false");
            assert!(!Config::load().expect("loads").sts_enabled);
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_STS_ENABLED", "true");
            assert!(Config::load().expect("loads").sts_enabled);
            Ok(())
        });
    }

    /// `CAIRN_KMS_KEY_IDS` is unset by default (accept-all) and parses a comma-separated allow-list,
    /// dropping blank entries; an all-blank value collapses to `None`.
    #[test]
    fn parse_kms_key_ids_from_env() {
        assert_eq!(Config::default().parse_kms_key_ids(), None);
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_KMS_KEY_IDS", "alias/a, alias/b ,,");
            assert_eq!(
                Config::load().expect("loads").parse_kms_key_ids(),
                Some(vec!["alias/a".to_owned(), "alias/b".to_owned()])
            );
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_KMS_KEY_IDS", " , ");
            assert_eq!(Config::load().expect("loads").parse_kms_key_ids(), None);
            Ok(())
        });
    }

    /// The integrity scrub is off by default (`0`) and its interval is read from the environment.
    #[test]
    fn load_reads_scrub_interval_from_env() {
        assert_eq!(Config::default().scrub_interval_secs, 0);
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_SCRUB_INTERVAL_SECS", "86400");
            assert_eq!(Config::load().expect("loads").scrub_interval_secs, 86_400);
            Ok(())
        });
    }

    /// The insecure-default deployment guardrail: a public bind is refused while the built-in dev
    /// master key or the default root secret is in use, unless explicitly overridden.
    #[test]
    fn refuses_insecure_defaults_on_public_bind() {
        assert!(
            !Config::default().allow_insecure,
            "override is off by default"
        );
        let public = |f: fn(&mut Config)| {
            let mut c = base();
            c.listen_addr = "0.0.0.0:7373".parse().unwrap();
            f(&mut c);
            c.refuse_insecure_public_bind()
        };
        // Loopback is always allowed, even with the bare dev defaults.
        let mut lo = base();
        lo.listen_addr = "127.0.0.1:7373".parse().unwrap();
        assert!(lo.refuse_insecure_public_bind().is_ok());
        // Public bind with the built-in dev master key (none) -> refused.
        assert!(public(|_| {}).is_err());
        // A real master key but the default root secret -> still refused.
        assert!(public(|c| c.master_key = Some("ab".repeat(32))).is_err());
        // Real key + a non-default root secret -> allowed.
        assert!(
            public(|c| {
                c.master_key = Some("ab".repeat(32));
                c.root_secret_key = "a-real-secret".to_owned();
            })
            .is_ok()
        );
        // The explicit override permits it on a trusted/closed network.
        assert!(public(|c| c.allow_insecure = true).is_ok());
    }

    #[test]
    fn rejects_dev_auth_on_non_loopback() {
        let mut c = base();
        c.dev_auth = true;
        c.listen_addr = "0.0.0.0:9000".parse().unwrap();
        assert!(c.validate().is_err());
        c.listen_addr = "127.0.0.1:9000".parse().unwrap();
        assert!(c.validate().is_ok());
    }

    /// Environment-only loading: with no `CAIRN_*` set, `load` returns the validated defaults.
    /// `Jail` clears the ambient environment, so this also proves the loader needs no config file.
    #[test]
    fn load_env_only_returns_defaults_when_unset() {
        figment::Jail::expect_with(|_jail| {
            let cfg = Config::load().expect("defaults load and validate");
            assert_eq!(cfg.listen_addr, Config::default().listen_addr);
            assert_eq!(cfg.region, "us-east-1");
            assert!(cfg.replication_targets.is_none());
            Ok(())
        });
    }

    /// Environment variables override the defaults — the only configuration source there is.
    /// There is no longer a TOML layer: `load` takes no path and reads `CAIRN_*` exclusively.
    #[test]
    fn load_env_only_applies_overrides() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_REGION", "eu-west-1");
            jail.set_env("CAIRN_LISTEN_ADDR", "0.0.0.0:8080");
            jail.set_env("CAIRN_LOG_FORMAT", "json");
            jail.set_env("CAIRN_REPLICATION_INTERVAL_SECS", "7");
            jail.set_env("CAIRN_REQUEST_METRICS_RETENTION_DAYS", "14");
            jail.set_env("CAIRN_REQUEST_METRICS_FLUSH_SECS", "5");
            let cfg = Config::load().expect("env overrides load and validate");
            assert_eq!(cfg.region, "eu-west-1");
            assert_eq!(cfg.listen_addr, "0.0.0.0:8080".parse().unwrap());
            assert_eq!(cfg.log_format, LogFormat::Json);
            assert_eq!(cfg.replication_interval_secs, 7);
            assert_eq!(cfg.request_metrics_retention_days, 14);
            assert_eq!(cfg.request_metrics_flush_secs, 5);
            Ok(())
        });
    }

    /// A TOML file present on disk is ignored: configuration comes only from env (and defaults),
    /// proving the file-merge support is gone. The file would have changed `region` if honoured.
    #[test]
    fn load_ignores_any_toml_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("Cairn.toml", "region = \"from-toml\"\n")?;
            let cfg = Config::load().expect("loads without consulting the file");
            assert_eq!(cfg.region, "us-east-1", "the TOML file must not be read");
            Ok(())
        });
    }

    /// The single-target `CAIRN_REPLICATION_*` keys still load (the fallback/default target).
    #[test]
    fn load_keeps_single_target_replication_keys() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_REPLICATION_ENDPOINT", "http://backup:9000");
            jail.set_env("CAIRN_REPLICATION_DEST_BUCKET", "mirror");
            jail.set_env("CAIRN_REPLICATION_ACCESS_KEY", "AKID");
            jail.set_env("CAIRN_REPLICATION_SECRET", "shh");
            let cfg = Config::load().expect("single-target keys load");
            assert_eq!(
                cfg.replication_endpoint.as_deref(),
                Some("http://backup:9000")
            );
            assert_eq!(cfg.replication_dest_bucket.as_deref(), Some("mirror"));
            assert_eq!(cfg.replication_access_key.as_deref(), Some("AKID"));
            assert_eq!(cfg.replication_secret.as_deref(), Some("shh"));
            Ok(())
        });
    }

    /// `CAIRN_REPLICATION_TARGETS` carries a JSON array of named targets parsed with `serde_json`.
    #[test]
    fn load_parses_replication_targets_json() {
        figment::Jail::expect_with(|jail| {
            jail.set_env(
                "CAIRN_REPLICATION_TARGETS",
                r#"[
                    {"name":"west","endpoint":"https://s3.west.example","region":"us-west-2",
                     "dest_bucket":"mirror-west","access_key":"AKW","secret":"sw","ca_path":"/etc/ca.pem"},
                    {"name":"east","endpoint":"http://s3.east.example:9000","region":"us-east-1",
                     "dest_bucket":"mirror-east","access_key":"AKE","secret":"se",
                     "insecure_skip_verify":true}
                ]"#,
            );
            let cfg = Config::load().expect("targets JSON loads and validates");
            let targets = cfg.parse_replication_targets().expect("targets parse");
            assert_eq!(targets.len(), 2);
            assert_eq!(targets[0].name, "west");
            assert_eq!(targets[0].dest_bucket, "mirror-west");
            assert_eq!(targets[0].ca_path, Some(PathBuf::from("/etc/ca.pem")));
            assert!(!targets[0].insecure_skip_verify);
            assert_eq!(targets[1].name, "east");
            assert!(targets[1].insecure_skip_verify);
            assert!(targets[1].ca_path.is_none());
            Ok(())
        });
    }

    /// A malformed `CAIRN_REPLICATION_TARGETS` document fails fast at load.
    #[test]
    fn load_rejects_malformed_replication_targets() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_REPLICATION_TARGETS", "{ not an array");
            assert!(
                Config::load().is_err(),
                "malformed targets JSON must be rejected"
            );
            Ok(())
        });
    }

    /// A target may not request both a custom CA and skip-verify; the two trust knobs conflict.
    #[test]
    fn rejects_target_with_conflicting_trust_knobs() {
        let mut c = base();
        c.replication_targets = Some(
            r#"[{"name":"x","endpoint":"https://e","region":"r","dest_bucket":"d",
                 "access_key":"a","secret":"s","ca_path":"/ca.pem","insecure_skip_verify":true}]"#
                .to_owned(),
        );
        assert!(c.validate().is_err());
    }

    /// `parse_replication_targets` yields an empty list when unset.
    #[test]
    fn parse_targets_empty_when_unset() {
        assert!(base().parse_replication_targets().unwrap().is_empty());
    }

    #[test]
    fn rejects_zero_replication_interval() {
        let mut c = base();
        c.replication_interval_secs = 0;
        assert!(c.validate().is_err(), "a zero drain interval busy-spins");
        c.replication_interval_secs = 30;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_bad_import_knobs() {
        let mut c = base();
        assert!(c.validate().is_ok());
        // The global in-flight cap must stay below the blob-I/O pool (64) so it can't starve live I/O.
        c.import_global_max_inflight = 64;
        assert!(c.validate().is_err(), "global cap must be < 64");
        c.import_global_max_inflight = 24;
        // Default workers must not exceed the hard cap.
        c.import_default_workers = 40;
        c.import_max_workers = 32;
        assert!(c.validate().is_err(), "default must be <= max");
        c.import_default_workers = 8;
        assert!(c.validate().is_ok());
        // Cadences must be positive.
        c.import_poll_interval_secs = 0;
        assert!(c.validate().is_err(), "a zero poll interval busy-spins");
    }

    #[test]
    fn rejects_malformed_master_key() {
        let mut c = base();
        c.master_key = Some("not-hex".to_owned());
        assert!(c.validate().is_err(), "non-hex master key rejected");
        c.master_key = Some("ab".repeat(31)); // 62 hex chars — wrong length
        assert!(c.validate().is_err(), "wrong-length master key rejected");
        c.master_key = Some("zz".repeat(32)); // 64 chars but not hex digits
        assert!(c.validate().is_err(), "non-hex characters rejected");
        c.master_key = Some("ab".repeat(32)); // 64 valid hex chars = 32 bytes
        assert!(
            c.validate().is_ok(),
            "a valid 64-hex master key is accepted"
        );
        c.master_key = None;
        assert!(
            c.validate().is_ok(),
            "absent master key allowed (dev fallback)"
        );
    }
}
