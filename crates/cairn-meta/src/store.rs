//! [`SqliteMetadataStore`]: writes go through the single group-committing [`Writer`]; reads
//! run on the blocking pool against the WAL read-connection pool, never contending with the
//! writer. Listing is a half-open range seek with an efficient delimiter skip-scan.

use crate::model::{self, engine_err};
use crate::range::{prefix_upper_bound, successor};
use crate::writer::{WalCheckpointStats, Writer};
use cairn_types::MetaError;
use cairn_types::authz::PublicAccessBlock;
use cairn_types::bucket::{Bucket, ConfigAspect, ConfigDoc};
use cairn_types::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use cairn_types::meta::{
    ActivityEntry, BucketCounts, BucketRequestCount, LATENCY_BUCKETS, ListPage, ListQuery,
    MetricsRange, MultipartSession, Mutation, MutationOutcome, ObjectSummary, OpCount, OutboxEntry,
    PartRecord, ReplicationStatus, RequestMetricsSeries, ShareRow, StatusCount, StoreCounts,
    TagSummary, TaggedObject, TimePoint, User, UserSigV4Credentials, UserWithBearerHash,
    latency_quantile_ms,
};
use cairn_types::object::ObjectVersionRow;
use cairn_types::time::Timestamp;
use cairn_types::traits::MetadataStore;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OptionalExtension, params};

const LIST_BATCH: usize = 1024;

/// The claim-lease length for replication-outbox entries: a claimed entry whose lease elapses
/// (a stalled worker) becomes eligible for re-claim after this many seconds.
const REPLICATION_LEASE_SECS: i64 = 300;

/// The SQLite-backed metadata store.
#[derive(Clone)]
pub struct SqliteMetadataStore {
    pub(crate) writer: Writer,
    pub(crate) pool: Pool<SqliteConnectionManager>,
    /// The on-disk database path, used to stat the `-wal` sidecar. `None` for in-memory stores.
    pub(crate) db_path: Option<std::path::PathBuf>,
}

impl std::fmt::Debug for SqliteMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteMetadataStore")
            .finish_non_exhaustive()
    }
}

impl SqliteMetadataStore {
    /// Run a truncating WAL checkpoint on the writer thread (ARCH §8.4/§11.2).
    ///
    /// The writer owns the only write connection, so the checkpoint is submitted to it as a
    /// control message and runs serialized with mutations. Returns the checkpoint's frame
    /// counts: whether it found the WAL `busy` with a reader, the total `log_frames`, and the
    /// `checkpointed_frames` that were moved into the database file.
    ///
    /// # Errors
    /// Returns a [`MetaError`] if the writer has shut down or the checkpoint PRAGMA fails.
    pub async fn checkpoint(&self) -> Result<WalCheckpointStats, MetaError> {
        self.writer.checkpoint().await
    }

    /// The current size in bytes of the write-ahead log (`-wal`) sidecar file.
    ///
    /// Returns `0` for an in-memory store, or when the `-wal` file is absent (e.g. just after a
    /// truncating checkpoint, or before the first write). Stating the path is a fast `metadata`
    /// call run off the writer thread.
    ///
    /// # Errors
    /// Returns a [`MetaError`] if the `-wal` file exists but cannot be stat-ed.
    pub async fn wal_size_bytes(&self) -> Result<u64, MetaError> {
        let Some(db_path) = self.db_path.clone() else {
            return Ok(0);
        };
        let wal_path = wal_sidecar_path(&db_path);
        tokio::task::spawn_blocking(move || match std::fs::metadata(&wal_path) {
            Ok(meta) => Ok(meta.len()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(MetaError::Engine(e.to_string())),
        })
        .await
        .map_err(|e| MetaError::Engine(e.to_string()))?
    }

    /// The current inbound write-queue depth: mutations submitted to the single writer but not yet
    /// pulled into a commit batch (ARCH §26.2). Published as `cairn_writer_queue_depth`; a sustained
    /// nonzero value means writes are arriving faster than the writer can commit them.
    #[must_use]
    pub fn writer_queue_depth(&self) -> usize {
        self.writer.queue_depth()
    }

    /// Probe that the single writer is responsive (its thread is draining the queue). Used by the
    /// readiness check so `/readyz` does not report ready while the writer is wedged. Cheap: a
    /// control message acked on the writer thread, no database work.
    ///
    /// # Errors
    /// Returns a [`MetaError`] if the writer has shut down.
    pub async fn writer_probe(&self) -> Result<(), MetaError> {
        self.writer.probe().await
    }

    /// Run a read closure on the blocking pool with a pooled read connection.
    pub(crate) async fn with_read<F, T>(&self, f: F) -> Result<T, MetaError>
    where
        F: FnOnce(&Connection) -> Result<T, MetaError> + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(|e| MetaError::Engine(e.to_string()))?;
            f(&conn)
        })
        .await
        .map_err(|e| MetaError::Engine(e.to_string()))?
    }

    // --- master-key re-wrap support (audit #29, Phase D/E; sqlite-only) ---
    // (see `RewrapUserRow` below)

    /// Page `(object_version_pk, sse_descriptor)` for rows carrying an SSE descriptor, after
    /// `cursor` (exclusive), ordered by PK so the cursor advances deterministically.
    pub async fn rewrap_sse_page(
        &self,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<Vec<(String, String)>, MetaError> {
        self.with_read(move |conn| {
            let cur = cursor.unwrap_or_default();
            conn.prepare_cached(
                "SELECT id, sse_descriptor FROM object_versions
                 WHERE sse_descriptor IS NOT NULL AND id > ?1 ORDER BY id LIMIT ?2",
            )
            .map_err(engine_err)?
            .query_map(params![cur, limit], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(engine_err)?
            .collect::<rusqlite::Result<Vec<(String, String)>>>()
            .map_err(engine_err)
        })
        .await
    }

    /// Replace one object version's SSE descriptor with its re-wrapped form, but ONLY if the stored
    /// descriptor still equals what the worker read (`expected`). The compare-and-swap closes the
    /// re-wrap lost-update window: if anything changed the row meanwhile, the update is a no-op and
    /// returns `false` rather than clobbering the newer value (audit #29). Returns whether a row was
    /// updated.
    pub async fn rewrap_set_sse(
        &self,
        version_pk: String,
        expected: String,
        descriptor: String,
    ) -> Result<bool, MetaError> {
        self.writer
            .run_exec(move |conn| {
                let n = conn
                    .execute(
                        "UPDATE object_versions SET sse_descriptor=?1 WHERE id=?2 AND sse_descriptor=?3",
                        params![descriptor, version_pk, expected],
                    )
                    .map_err(engine_err)?;
                Ok(n > 0)
            })
            .await
    }

    /// Page `(user_id, ciphertext, nonce_opt)` for users with a sealed SigV4 secret, after
    /// `cursor` (exclusive).
    pub async fn rewrap_users_page(
        &self,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<Vec<RewrapUserRow>, MetaError> {
        self.with_read(move |conn| {
            let cur = cursor.unwrap_or_default();
            conn.prepare_cached(
                "SELECT id, sigv4_secret_ciphertext, sigv4_secret_nonce FROM users
                 WHERE sigv4_secret_ciphertext IS NOT NULL AND id > ?1 ORDER BY id LIMIT ?2",
            )
            .map_err(engine_err)?
            .query_map(params![cur, limit], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .map_err(engine_err)?
            .collect::<rusqlite::Result<Vec<(String, Vec<u8>, Option<Vec<u8>>)>>>()
            .map_err(engine_err)
        })
        .await
    }

    /// Replace one user's SigV4 secret ciphertext with its re-wrapped form (NULLing the legacy
    /// nonce), but ONLY if the stored ciphertext still equals what the worker read (`expected`). The
    /// compare-and-swap closes the lost-update window where a concurrent credential rotation would
    /// otherwise be reverted by the re-wrap write (audit #29). Returns whether a row was updated.
    pub async fn rewrap_set_user_sigv4(
        &self,
        user_id: String,
        expected: Vec<u8>,
        ciphertext: Vec<u8>,
    ) -> Result<bool, MetaError> {
        self.writer
            .run_exec(move |conn| {
                let n = conn
                    .execute(
                        "UPDATE users SET sigv4_secret_ciphertext=?1, sigv4_secret_nonce=NULL
                         WHERE id=?2 AND sigv4_secret_ciphertext=?3",
                        params![ciphertext, user_id, expected],
                    )
                    .map_err(engine_err)?;
                Ok(n > 0)
            })
            .await
    }

    /// Compare-and-swap a bucket-config aspect document: replace `aspect`'s doc with `new_doc` only
    /// if it still equals `expected`. Used by the re-wrap worker to update re-sealed replication
    /// targets without clobbering a concurrently-edited target list (audit #29). Returns whether a
    /// row was updated.
    pub async fn rewrap_set_bucket_config_cas(
        &self,
        bucket: String,
        aspect: ConfigAspect,
        expected: String,
        new_doc: String,
    ) -> Result<bool, MetaError> {
        let aspect_s = crate::apply::aspect_str(aspect);
        self.writer
            .run_exec(move |conn| {
                let n = conn
                    .execute(
                        "UPDATE bucket_config SET doc=?1 WHERE bucket_name=?2 AND aspect=?3 AND doc=?4",
                        params![new_doc, bucket, aspect_s, expected],
                    )
                    .map_err(engine_err)?;
                Ok(n > 0)
            })
            .await
    }

    /// The resume cursor for `stream`'s in-flight re-wrap pass (None = no pass mid-flight). This is
    /// only for resumability — it does NOT signal completion; see [`rewrap_done_active_ids`] for
    /// that (a NULL cursor means "not started" just as much as "finished", audit #29).
    ///
    /// [`rewrap_done_active_ids`]: Self::rewrap_done_active_ids
    pub async fn rewrap_cursor(&self, stream: String) -> Result<Option<String>, MetaError> {
        self.with_read(move |conn| {
            Ok(conn
                .query_row(
                    "SELECT cursor FROM rewrap_progress WHERE stream=?1",
                    params![stream],
                    |r| r.get::<_, Option<String>>(0),
                )
                .optional()
                .map_err(engine_err)?
                .flatten())
        })
        .await
    }

    /// Upsert the re-wrap cursor + accumulate the row counters for `stream`.
    pub async fn rewrap_set_progress(
        &self,
        stream: String,
        cursor: Option<String>,
        rows_done_delta: u64,
        rows_failed_delta: u64,
        now: i64,
    ) -> Result<(), MetaError> {
        self.writer
            .run_exec(move |conn| {
                conn.execute(
                    "INSERT INTO rewrap_progress (stream, cursor, rows_done, rows_failed, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(stream) DO UPDATE SET
                       cursor=excluded.cursor,
                       rows_done=rows_done+excluded.rows_done,
                       rows_failed=rows_failed+excluded.rows_failed,
                       updated_at=excluded.updated_at",
                    params![
                        stream,
                        cursor,
                        rows_done_delta as i64,
                        rows_failed_delta as i64,
                        now
                    ],
                )
                .map_err(engine_err)?;
                Ok(())
            })
            .await
    }

    /// Record the end of a re-wrap pass for `stream`: clear the resume cursor and set the active
    /// key id the pass completed under — `done_active_id` is the active id ONLY when a full,
    /// failure-free pass actually re-sealed (or confirmed) every row under it, else 0 (audit #29).
    /// Upserts, so a stream whose table held zero rows still records completion.
    pub async fn rewrap_finish_pass(
        &self,
        stream: String,
        done_active_id: u16,
        now: i64,
    ) -> Result<(), MetaError> {
        self.writer
            .run_exec(move |conn| {
                conn.execute(
                    "INSERT INTO rewrap_progress (stream, cursor, rows_done, rows_failed, updated_at, done_active_id)
                     VALUES (?1, NULL, 0, 0, ?2, ?3)
                     ON CONFLICT(stream) DO UPDATE SET cursor=NULL, updated_at=?2, done_active_id=?3",
                    params![stream, now, done_active_id as i64],
                )
                .map_err(engine_err)?;
                Ok(())
            })
            .await
    }

    /// The active key id under which each stream's last full re-wrap pass completed (0 = none yet).
    /// The crypto-status endpoint compares these against the live active id, on every shard, to
    /// decide whether a retired key's data is fully re-wrapped (audit #29).
    pub async fn rewrap_done_active_ids(&self) -> Result<Vec<(String, u16)>, MetaError> {
        self.with_read(|conn| {
            conn.prepare_cached("SELECT stream, done_active_id FROM rewrap_progress")
                .map_err(engine_err)?
                .query_map([], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u16))
                })
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<(String, u16)>>>()
                .map_err(engine_err)
        })
        .await
    }

    /// Upsert per-key ring state (id, key-hash prefix, active flag) for operator visibility.
    pub async fn key_ring_upsert(
        &self,
        id: u16,
        key_hash: String,
        is_active: bool,
        now: i64,
    ) -> Result<(), MetaError> {
        self.writer
            .run_exec(move |conn| {
                conn.execute(
                    "INSERT INTO key_ring_state (id, key_hash, is_active, created_at)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(id) DO UPDATE SET key_hash=excluded.key_hash, is_active=excluded.is_active",
                    params![id, key_hash, is_active as i64, now],
                )
                .map_err(engine_err)?;
                Ok(())
            })
            .await
    }

    /// Sync the high-water seal count for a key (Phase E durable counter).
    pub async fn key_ring_sync_seal_count(&self, id: u16, count: u64) -> Result<(), MetaError> {
        self.writer
            .run_exec(move |conn| {
                conn.execute(
                    "UPDATE key_ring_state SET sealed_count=MAX(sealed_count, ?2) WHERE id=?1",
                    params![id, count as i64],
                )
                .map_err(engine_err)?;
                Ok(())
            })
            .await
    }

    /// Read all ring-state rows (for the crypto-status endpoint + startup priming).
    pub async fn key_ring_states(&self) -> Result<Vec<KeyRingStateRow>, MetaError> {
        self.with_read(|conn| {
            conn.prepare_cached(
                "SELECT id, key_hash, is_active, sealed_count, created_at
                 FROM key_ring_state ORDER BY id",
            )
            .map_err(engine_err)?
            .query_map([], |r| {
                Ok(KeyRingStateRow {
                    id: r.get::<_, i64>(0)? as u16,
                    key_hash: r.get(1)?,
                    is_active: r.get::<_, i64>(2)? != 0,
                    sealed_count: r.get::<_, i64>(3)? as u64,
                    created_at: r.get(4)?,
                })
            })
            .map_err(engine_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(engine_err)
        })
        .await
    }
}

/// One paged user row for the re-wrap worker: `(user_id, ciphertext, nonce_opt)` (audit #29).
pub type RewrapUserRow = (String, Vec<u8>, Option<Vec<u8>>);

/// A row of `key_ring_state` for the crypto-status endpoint (audit #29).
#[derive(Debug, Clone)]
pub struct KeyRingStateRow {
    /// Ring id.
    pub id: u16,
    /// First 8 hex of SHA-256(key) for operator display (never key material).
    pub key_hash: String,
    /// Whether this is the active (sealing) key.
    pub is_active: bool,
    /// High-water seal count synced from the active key's in-process counter.
    pub sealed_count: u64,
    /// Wall-clock millis the row was first seen.
    pub created_at: i64,
}

/// SQLite names the write-ahead log sidecar `<database>-wal`; build that path.
fn wal_sidecar_path(db_path: &std::path::Path) -> std::path::PathBuf {
    let mut name = db_path.as_os_str().to_owned();
    name.push("-wal");
    std::path::PathBuf::from(name)
}

/// The efficient listing implementation: half-open range seek with delimiter skip-scan. The
/// continuation cursor is an inclusive lower bound (the first key NOT yet returned).
fn list_impl(
    conn: &Connection,
    bucket: &str,
    query: &ListQuery,
    latest_only: bool,
) -> Result<ListPage<ObjectSummary>, MetaError> {
    let prefix = query.prefix.clone().unwrap_or_default();
    let upper = prefix_upper_bound(&prefix);
    let limit = query.limit.max(1) as usize;

    // Inclusive lower bound = max(prefix, cursor, successor(start_after)).
    let mut seek = prefix.clone();
    if let Some(c) = &query.cursor {
        if c.as_str() > seek.as_str() {
            seek = c.clone();
        }
    }
    if let Some(sa) = &query.start_after {
        let s = successor(sa);
        if s > seek {
            seek = s;
        }
    }

    // For version listings, a version-id marker resumes strictly after `(cursor_key, marker)`
    // within the marker key. Versions sort `version_id DESC`, so entries already returned for that
    // key have `version_id >= marker`; we skip exactly those on the first key.
    let vid_marker = if latest_only {
        None
    } else {
        query
            .version_id_marker
            .as_deref()
            .zip(query.cursor.as_deref())
            .map(|(vid, key)| (key.to_owned(), vid.to_owned()))
    };

    let mut page: ListPage<ObjectSummary> = ListPage::default();
    let mut seen_cp = std::collections::HashSet::new();

    'outer: loop {
        let rows = fetch_rows(
            conn,
            bucket,
            &seek,
            upper.as_deref(),
            latest_only,
            LIST_BATCH + 1,
        )?;
        if rows.is_empty() {
            break;
        }
        let exhausted = rows.len() <= LIST_BATCH;
        for summary in rows.into_iter().take(LIST_BATCH) {
            if let Some((mk, mv)) = &vid_marker {
                if summary.key.as_str() == mk.as_str() && summary.version_id.as_str() >= mv.as_str()
                {
                    // Already returned on the previous page (or is the marker itself); skip it. The
                    // skipped versions of the marker key are a bounded prefix that fits in one batch.
                    continue;
                }
            }
            if page.items.len() + page.common_prefixes.len() >= limit {
                page.truncated = true;
                if latest_only {
                    // Current-object listing (one row per key): resume at the first unreturned key.
                    page.next_cursor = Some(summary.key.as_str().to_owned());
                } else if let Some(last) = page.items.last() {
                    // Version listing: resume at the LAST RETURNED (key, version_id). The next page
                    // re-seeks that key and the version-id marker skips the versions already
                    // returned (`version_id >= marker`), so paging within a multi-version key is
                    // gap-free and duplicate-free.
                    page.next_cursor = Some(last.key.as_str().to_owned());
                    page.next_version_id_marker = Some(last.version_id.as_str().to_owned());
                } else {
                    page.next_cursor = Some(summary.key.as_str().to_owned());
                }
                break 'outer;
            }
            let key = summary.key.as_str().to_owned();
            if let Some(delim) = query.delimiter.as_deref() {
                let rest = &key[prefix.len()..];
                if let Some(idx) = rest.find(delim) {
                    let cp = format!("{}{}{}", prefix, &rest[..idx], delim);
                    if seen_cp.insert(cp.clone()) {
                        page.common_prefixes.push(cp.clone());
                    }
                    // Skip every key under this common prefix.
                    match prefix_upper_bound(&cp) {
                        Some(next) => {
                            seek = next;
                            continue 'outer;
                        }
                        None => break 'outer,
                    }
                }
            }
            seek = successor(&key);
            page.items.push(summary);
        }
        if exhausted {
            break;
        }
    }
    Ok(page)
}

fn fetch_rows(
    conn: &Connection,
    bucket: &str,
    seek: &str,
    upper: Option<&str>,
    latest_only: bool,
    limit: usize,
) -> Result<Vec<ObjectSummary>, MetaError> {
    let mut sql = String::from(
        "SELECT key, version_id, is_latest, is_delete_marker, etag, size_logical, updated_at, \
         storage_class, owner_id FROM object_versions WHERE bucket_name = ?1 AND key >= ?2",
    );
    if latest_only {
        sql.push_str(" AND is_latest = 1 AND is_delete_marker = 0");
    }
    if upper.is_some() {
        sql.push_str(" AND key < ?3");
    }
    if latest_only {
        sql.push_str(" ORDER BY key ASC");
    } else {
        sql.push_str(" ORDER BY key ASC, version_id DESC");
    }
    sql.push_str(" LIMIT ?4");

    let mut stmt = conn.prepare_cached(&sql).map_err(engine_err)?;
    let limit = limit as i64;
    let map = model::object_summary_from_row;
    let rows = if let Some(ub) = upper {
        stmt.query_map(params![bucket, seek, ub, limit], map)
    } else {
        stmt.query_map(params![bucket, seek, rusqlite::types::Null, limit], map)
    }
    .map_err(engine_err)?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(engine_err)
}

#[async_trait::async_trait]
impl MetadataStore for SqliteMetadataStore {
    async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
        self.writer.submit(mutation).await
    }

    async fn get_bucket(&self, name: &BucketName) -> Result<Option<Bucket>, MetaError> {
        let name = name.as_str().to_owned();
        self.with_read(move |conn| {
            conn.query_row(
                "SELECT * FROM buckets WHERE name=?1",
                params![name],
                model::bucket_from_row,
            )
            .optional()
            .map_err(engine_err)
        })
        .await
    }

    async fn list_buckets(&self, owner: Option<&UserId>) -> Result<Vec<Bucket>, MetaError> {
        let owner = owner.map(|o| o.0.clone());
        self.with_read(move |conn| {
            let (sql, bind): (&str, Vec<String>) = match &owner {
                Some(o) => (
                    "SELECT * FROM buckets WHERE owner_id=?1 ORDER BY name",
                    vec![o.clone()],
                ),
                None => ("SELECT * FROM buckets ORDER BY name", vec![]),
            };
            let mut stmt = conn.prepare(sql).map_err(engine_err)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(bind), model::bucket_from_row)
                .map_err(engine_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)
        })
        .await
    }

    async fn get_bucket_config(
        &self,
        name: &BucketName,
        aspect: ConfigAspect,
    ) -> Result<Option<ConfigDoc>, MetaError> {
        let name = name.as_str().to_owned();
        let aspect = crate::apply::aspect_str(aspect);
        self.with_read(move |conn| {
            conn.query_row(
                "SELECT doc FROM bucket_config WHERE bucket_name=?1 AND aspect=?2",
                params![name, aspect],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(engine_err)
            .map(|o| o.map(ConfigDoc))
        })
        .await
    }

    async fn get_account_public_access_block(&self) -> Result<PublicAccessBlock, MetaError> {
        self.with_read(move |conn| {
            let v: Option<String> = conn
                .query_row(
                    "SELECT v FROM account_config WHERE k='public_access_block'",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(engine_err)?;
            Ok(v.and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default())
        })
        .await
    }

    async fn get_bucket_quota(&self, bucket: &BucketName) -> Result<Option<u64>, MetaError> {
        let name = bucket.as_str().to_owned();
        self.with_read(move |conn| {
            // `quota_bytes` is a nullable column added in migration v2; the outer `Option`
            // distinguishes "no such bucket", the inner `Option` distinguishes "no quota set".
            let quota: Option<Option<i64>> = conn
                .query_row(
                    "SELECT quota_bytes FROM buckets WHERE name=?1",
                    params![name],
                    |r| r.get(0),
                )
                .optional()
                .map_err(engine_err)?;
            // Both "no such bucket" and "quota NULL" present to the reader as "no quota". A stored
            // negative is clamped to 0 defensively (writes only ever store non-negative values).
            Ok(quota.flatten().map(|q| q.max(0) as u64))
        })
        .await
    }

    async fn is_bucket_empty(&self, name: &BucketName) -> Result<bool, MetaError> {
        let name = name.as_str().to_owned();
        self.with_read(move |conn| {
            let exists: bool = conn
                .query_row(
                    // Empty means NO object_versions rows at all — including non-current versions
                    // and delete markers — so S3 DeleteBucket correctly refuses a bucket that still
                    // holds version history (audit #3). A bucket with only old versions / delete
                    // markers is NOT deletable in S3, and deleting it would orphan those rows.
                    "SELECT EXISTS(SELECT 1 FROM object_versions WHERE bucket_name=?1)",
                    params![name],
                    |r| r.get::<_, i64>(0),
                )
                .map_err(engine_err)?
                != 0;
            Ok(!exists)
        })
        .await
    }

    async fn current_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        let (b, k) = (bucket.as_str().to_owned(), key.as_str().to_owned());
        self.with_read(move |conn| {
            conn.query_row(
                "SELECT * FROM object_versions WHERE bucket_name=?1 AND key=?2 AND is_latest=1",
                params![b, k],
                model::object_version_from_row,
            )
            .optional()
            .map_err(engine_err)
        })
        .await
    }

    async fn get_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        let (b, k, v) = (
            bucket.as_str().to_owned(),
            key.as_str().to_owned(),
            version.as_str().to_owned(),
        );
        self.with_read(move |conn| {
            conn.query_row(
                "SELECT * FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                params![b, k, v],
                model::object_version_from_row,
            )
            .optional()
            .map_err(engine_err)
        })
        .await
    }

    async fn list_current(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        let (b, q) = (bucket.as_str().to_owned(), query.clone());
        self.with_read(move |conn| list_impl(conn, &b, &q, true))
            .await
    }

    async fn list_versions(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        let (b, q) = (bucket.as_str().to_owned(), query.clone());
        self.with_read(move |conn| list_impl(conn, &b, &q, false))
            .await
    }

    async fn enumerate_storage_paths(
        &self,
        bucket: &BucketName,
        cursor: Option<&str>,
        batch: u32,
    ) -> Result<ListPage<StoragePath>, MetaError> {
        let (b, cursor) = (bucket.as_str().to_owned(), cursor.unwrap_or("").to_owned());
        self.with_read(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT storage_path FROM object_versions
                     WHERE bucket_name=?1 AND storage_path IS NOT NULL AND storage_path > ?2
                     ORDER BY storage_path LIMIT ?3",
                )
                .map_err(engine_err)?;
            let rows = stmt
                .query_map(params![b, cursor, i64::from(batch) + 1], |r| {
                    r.get::<_, String>(0)
                })
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)?;
            let truncated = rows.len() > batch as usize;
            let mut items: Vec<String> = rows;
            items.truncate(batch as usize);
            let next_cursor = if truncated {
                items.last().cloned()
            } else {
                None
            };
            Ok(ListPage {
                items: items.into_iter().map(StoragePath::from_string).collect(),
                common_prefixes: Vec::new(),
                next_cursor,
                next_version_id_marker: None,
                truncated,
            })
        })
        .await
    }

    async fn get_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Vec<(String, String)>, MetaError> {
        let (b, k, v) = (
            bucket.as_str().to_owned(),
            key.as_str().to_owned(),
            version.as_str().to_owned(),
        );
        self.with_read(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT tag_key, tag_value FROM object_tags WHERE bucket_name=?1 AND key=?2 AND version_id=?3 ORDER BY tag_key",
                )
                .map_err(engine_err)?;
            let rows = stmt
                .query_map(params![b, k, v], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)?;
            Ok(rows)
        })
        .await
    }

    async fn get_multipart(
        &self,
        upload: &UploadId,
    ) -> Result<Option<MultipartSession>, MetaError> {
        let id = upload.as_str().to_owned();
        self.with_read(move |conn| {
            conn.query_row(
                "SELECT * FROM multipart_uploads WHERE id=?1",
                params![id],
                model::multipart_from_row,
            )
            .optional()
            .map_err(engine_err)
        })
        .await
    }

    async fn list_parts(
        &self,
        upload: &UploadId,
        part_number_marker: u16,
        limit: u32,
    ) -> Result<ListPage<PartRecord>, MetaError> {
        let id = upload.as_str().to_owned();
        self.with_read(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT * FROM multipart_parts WHERE upload_id=?1 AND part_number>?2 ORDER BY part_number LIMIT ?3",
                )
                .map_err(engine_err)?;
            let rows = stmt
                .query_map(params![id, part_number_marker, i64::from(limit) + 1], model::part_from_row)
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)?;
            let truncated = rows.len() > limit as usize;
            let mut items = rows;
            items.truncate(limit as usize);
            let next_cursor = if truncated { items.last().map(|p| p.part_number.to_string()) } else { None };
            Ok(ListPage { items, common_prefixes: Vec::new(), next_cursor, next_version_id_marker: None, truncated })
        })
        .await
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<MultipartSession>, MetaError> {
        let (b, q) = (bucket.as_str().to_owned(), query.clone());
        self.with_read(move |conn| {
            let prefix = q.prefix.clone().unwrap_or_default();
            let upper = prefix_upper_bound(&prefix);
            // Resume strictly after the cursor key when one is supplied; otherwise seek to the
            // half-open prefix lower bound.
            let seek = match &q.cursor {
                Some(c) if c.as_str() > prefix.as_str() => c.clone(),
                _ => prefix.clone(),
            };
            let limit = q.limit.max(1) as usize;
            // Half-open `prefix_upper_bound` seek like the other listings, fetching one extra row to
            // detect truncation.
            let mut sql = String::from(
                "SELECT * FROM multipart_uploads WHERE bucket_name=?1 AND status='active' AND key>=?2",
            );
            if upper.is_some() {
                sql.push_str(" AND key<?3");
            }
            sql.push_str(" ORDER BY key, id LIMIT ?4");
            let mut stmt = conn.prepare_cached(&sql).map_err(engine_err)?;
            let batch = (limit + 1) as i64;
            let rows = if let Some(ub) = &upper {
                stmt.query_map(params![b, seek, ub, batch], model::multipart_from_row)
            } else {
                stmt.query_map(
                    params![b, seek, rusqlite::types::Null, batch],
                    model::multipart_from_row,
                )
            }
            .map_err(engine_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(engine_err)?;
            let mut items: Vec<MultipartSession> = rows;
            let truncated = items.len() > limit;
            items.truncate(limit);
            let next_cursor = if truncated {
                items.last().map(|s| s.key.as_str().to_owned())
            } else {
                None
            };
            Ok(ListPage { items, common_prefixes: Vec::new(), next_cursor, next_version_id_marker: None, truncated })
        })
        .await
    }

    async fn enumerate_stale_sessions(
        &self,
        older_than: Timestamp,
        batch: u32,
    ) -> Result<Vec<MultipartSession>, MetaError> {
        self.with_read(move |conn| {
            let mut stmt = conn
                .prepare_cached("SELECT * FROM multipart_uploads WHERE updated_at < ?1 LIMIT ?2")
                .map_err(engine_err)?;
            stmt.query_map(
                params![older_than.0, i64::from(batch)],
                model::multipart_from_row,
            )
            .map_err(engine_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(engine_err)
        })
        .await
    }

    async fn object_replication_status(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ReplicationStatus>, MetaError> {
        let (b, k, v) = (
            bucket.as_str().to_owned(),
            key.as_str().to_owned(),
            version.as_str().to_owned(),
        );
        self.with_read(move |conn| {
            let s: Option<String> = conn
                .query_row(
                    "SELECT replication_status FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                    params![b, k, v],
                    |r| r.get(0),
                )
                .optional()
                .map_err(engine_err)?
                .flatten();
            Ok(s.map(|s| model::repl_status_from(&s)))
        })
        .await
    }

    async fn has_unreplicated_predecessor(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        before: &VersionId,
    ) -> Result<bool, MetaError> {
        let (b, k, v) = (
            bucket.as_str().to_owned(),
            key.as_str().to_owned(),
            before.as_str().to_owned(),
        );
        self.with_read(move |conn| {
            // version_id is uuidv7 (time-ordered), so a strictly-lower id is an earlier write.
            // A completed entry keeps its row with status='completed'; anything else
            // (pending/claimed/failed) is still owed to the destination and must ship first.
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM replication_outbox \
                     WHERE bucket_name=?1 AND key=?2 AND version_id<?3 AND status!='completed')",
                    params![b, k, v],
                    |r| r.get(0),
                )
                .map_err(engine_err)?;
            Ok(exists)
        })
        .await
    }

    async fn claim_replication_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        // Claiming is a write (it marks entries `claimed` with a lease), routed through the
        // single writer so the select-and-mark is atomic against other workers.
        let outcome = self
            .submit(Mutation::ClaimReplicationBatch {
                limit,
                now,
                lease_secs: REPLICATION_LEASE_SECS,
            })
            .await?;
        match outcome {
            MutationOutcome::ReplicationBatch(entries) => Ok(entries),
            other => Err(MetaError::Engine(format!(
                "unexpected outcome for ClaimReplicationBatch: {other:?}"
            ))),
        }
    }

    async fn list_due_replication(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        // Read-only mirror of the claim predicate (pending, or claimed with an expired lease, that
        // has reached its next-attempt time), ordered as the claim orders. No mutation.
        self.with_read(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT * FROM replication_outbox \
                     WHERE next_attempt_at<=?1 \
                       AND (status='pending' OR (status='claimed' AND lease_until<?1)) \
                     ORDER BY priority DESC, next_attempt_at LIMIT ?2",
                )
                .map_err(engine_err)?;
            stmt.query_map(params![now.0, i64::from(limit)], model::outbox_from_row)
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)
        })
        .await
    }

    async fn list_failed_replication(&self, limit: u32) -> Result<Vec<OutboxEntry>, MetaError> {
        // Terminal entries are those the engine marked `status='failed'` (retries exhausted, no
        // further attempt scheduled — see `MarkReplicationFailed { next_attempt_at: None }`). The
        // `idx_outbox_status_next` index serves this status-prefixed range. Most-recently-due first.
        self.with_read(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT * FROM replication_outbox WHERE status='failed' ORDER BY next_attempt_at DESC LIMIT ?1",
                )
                .map_err(engine_err)?;
            stmt.query_map(params![i64::from(limit)], model::outbox_from_row)
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)
        })
        .await
    }

    async fn user_by_bearer_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserWithBearerHash>, MetaError> {
        let id = access_key_id.to_owned();
        self.with_read(move |conn| {
            conn.query_row(
                "SELECT * FROM users WHERE access_key_id=?1",
                params![id],
                model::user_with_bearer_from_row,
            )
            .optional()
            .map_err(engine_err)
        })
        .await
    }

    async fn user_by_sigv4_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserSigV4Credentials>, MetaError> {
        let id = access_key_id.to_owned();
        self.with_read(move |conn| {
            conn.query_row(
                "SELECT * FROM users WHERE sigv4_access_key_id=?1",
                params![id],
                model::user_sigv4_from_row,
            )
            .optional()
            .map_err(engine_err)
            .map(Option::flatten)
        })
        .await
    }

    async fn count_users(&self) -> Result<u64, MetaError> {
        self.with_read(move |conn| {
            conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get::<_, i64>(0))
                .map_err(engine_err)
                .map(|n| n as u64)
        })
        .await
    }

    async fn list_users(&self) -> Result<Vec<User>, MetaError> {
        self.with_read(move |conn| {
            let mut stmt = conn
                .prepare("SELECT * FROM users ORDER BY created_at")
                .map_err(engine_err)?;
            stmt.query_map([], model::user_from_row)
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)
        })
        .await
    }

    async fn get_user_policy(&self, user_id: &UserId) -> Result<Option<String>, MetaError> {
        let id = user_id.0.as_str().to_owned();
        self.with_read(move |conn| {
            conn.query_row("SELECT policy FROM users WHERE id=?1", params![id], |r| {
                r.get::<_, Option<String>>(0)
            })
            .optional()
            .map(Option::flatten)
            .map_err(engine_err)
        })
        .await
    }

    async fn list_activity(&self, limit: u32) -> Result<Vec<ActivityEntry>, MetaError> {
        self.with_read(move |conn| {
            let mut stmt = conn
                .prepare_cached("SELECT * FROM activity ORDER BY at DESC LIMIT ?1")
                .map_err(engine_err)?;
            stmt.query_map(params![i64::from(limit)], model::activity_from_row)
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)
        })
        .await
    }

    async fn get_share(&self, token: &str) -> Result<Option<ShareRow>, MetaError> {
        let token = token.to_owned();
        self.with_read(move |conn| {
            conn.query_row(
                "SELECT * FROM object_shares WHERE token=?1",
                params![token],
                model::share_from_row,
            )
            .optional()
            .map_err(engine_err)
        })
        .await
    }

    async fn list_shares(
        &self,
        bucket: &BucketName,
        key: Option<&ObjectKey>,
    ) -> Result<Vec<ShareRow>, MetaError> {
        let bucket = bucket.as_str().to_owned();
        let key = key.map(|k| k.as_str().to_owned());
        self.with_read(move |conn| match key {
            Some(k) => {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT * FROM object_shares WHERE bucket_name=?1 AND key=?2 ORDER BY created_at DESC",
                    )
                    .map_err(engine_err)?;
                stmt.query_map(params![bucket, k], model::share_from_row)
                    .map_err(engine_err)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(engine_err)
            }
            None => {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT * FROM object_shares WHERE bucket_name=?1 ORDER BY created_at DESC",
                    )
                    .map_err(engine_err)?;
                stmt.query_map(params![bucket], model::share_from_row)
                    .map_err(engine_err)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(engine_err)
            }
        })
        .await
    }

    async fn list_tag_summary(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<Vec<TagSummary>, MetaError> {
        let bucket = bucket.map(|b| b.as_str().to_owned());
        self.with_read(move |conn| {
            // Join to the current version so the count is of live objects, not historical versions.
            // `?1 IS NULL` makes the bucket filter optional in a single prepared statement.
            let mut stmt = conn
                .prepare_cached(
                    "SELECT ot.tag_key, ot.tag_value, COUNT(*) AS c
                     FROM object_tags ot
                     JOIN object_versions ov
                       ON ov.bucket_name = ot.bucket_name AND ov.key = ot.key
                          AND ov.version_id = ot.version_id
                     WHERE ov.is_latest = 1 AND ov.is_delete_marker = 0
                       AND (?1 IS NULL OR ot.bucket_name = ?1)
                     GROUP BY ot.tag_key, ot.tag_value
                     ORDER BY c DESC, ot.tag_key, ot.tag_value",
                )
                .map_err(engine_err)?;
            stmt.query_map(params![bucket], |r| {
                Ok(TagSummary {
                    tag_key: r.get(0)?,
                    tag_value: r.get(1)?,
                    object_count: r.get::<_, i64>(2)? as u64,
                })
            })
            .map_err(engine_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(engine_err)
        })
        .await
    }

    async fn list_objects_by_tag(
        &self,
        bucket: Option<&BucketName>,
        tag_key: &str,
        tag_value: &str,
        limit: u32,
    ) -> Result<Vec<TaggedObject>, MetaError> {
        let bucket = bucket.map(|b| b.as_str().to_owned());
        let (tag_key, tag_value) = (tag_key.to_owned(), tag_value.to_owned());
        self.with_read(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT ot.bucket_name, ot.key, ot.version_id, ov.size_logical, ov.updated_at
                     FROM object_tags ot
                     JOIN object_versions ov
                       ON ov.bucket_name = ot.bucket_name AND ov.key = ot.key
                          AND ov.version_id = ot.version_id
                     WHERE ot.tag_key = ?1 AND ot.tag_value = ?2
                       AND ov.is_latest = 1 AND ov.is_delete_marker = 0
                       AND (?3 IS NULL OR ot.bucket_name = ?3)
                     ORDER BY ot.bucket_name, ot.key
                     LIMIT ?4",
                )
                .map_err(engine_err)?;
            stmt.query_map(params![tag_key, tag_value, bucket, limit], |r| {
                Ok(TaggedObject {
                    bucket: r.get(0)?,
                    key: r.get(1)?,
                    version_id: r.get(2)?,
                    size: r.get::<_, i64>(3)? as u64,
                    last_modified: Timestamp(r.get::<_, i64>(4)?),
                })
            })
            .map_err(engine_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(engine_err)
        })
        .await
    }

    async fn aggregate_counts(&self) -> Result<StoreCounts, MetaError> {
        self.with_read(move |conn| {
            let buckets: i64 = conn
                .query_row("SELECT COUNT(*) FROM buckets", [], |r| r.get(0))
                .map_err(engine_err)?;
            // Versions and byte totals come from the maintained roll-up (O(buckets)), not a scan of
            // every object version (Phase 2.1).
            let (versions, logical, physical): (i64, i64, i64) = conn
                .query_row(
                    "SELECT COALESCE(SUM(versions),0), COALESCE(SUM(logical_bytes),0),
                            COALESCE(SUM(physical_bytes),0)
                     FROM bucket_stats",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .map_err(engine_err)?;
            // The current-visible object count is an index-only scan of the partial current-version
            // index (idx_ov_latest_cover), so it visits one entry per live object, not every version.
            let objects: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM object_versions WHERE is_latest=1 AND is_delete_marker=0",
                    [],
                    |r| r.get(0),
                )
                .map_err(engine_err)?;
            Ok(StoreCounts {
                buckets: buckets as u64,
                objects: objects as u64,
                versions: versions as u64,
                logical_bytes: logical as u64,
                physical_bytes: physical as u64,
            })
        })
        .await
    }

    async fn bucket_counts(&self) -> Result<Vec<BucketCounts>, MetaError> {
        self.with_read(move |conn| {
            // Byte totals come from the maintained roll-up (LEFT JOIN so buckets with no objects
            // still appear with zeros); the current-object count joins a GROUP BY over the partial
            // current-version index. Neither path scans historical versions for the byte sums.
            let mut stmt = conn
                .prepare_cached(
                    "SELECT b.name,
                        COALESCE(o.objects, 0),
                        COALESCE(s.logical_bytes, 0),
                        COALESCE(s.physical_bytes, 0)
                     FROM buckets b
                     LEFT JOIN bucket_stats s ON s.bucket_name = b.name
                     LEFT JOIN (
                        SELECT bucket_name, COUNT(*) AS objects FROM object_versions
                        WHERE is_latest=1 AND is_delete_marker=0 GROUP BY bucket_name
                     ) o ON o.bucket_name = b.name
                     ORDER BY b.name",
                )
                .map_err(engine_err)?;
            stmt.query_map([], |r| {
                let (objects, logical, physical): (i64, i64, i64) =
                    (r.get(1)?, r.get(2)?, r.get(3)?);
                Ok(BucketCounts {
                    bucket: r.get(0)?,
                    objects: objects as u64,
                    logical_bytes: logical as u64,
                    physical_bytes: physical as u64,
                })
            })
            .map_err(engine_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(engine_err)
        })
        .await
    }

    async fn query_request_metrics(
        &self,
        range: MetricsRange,
        now_secs: i64,
    ) -> Result<RequestMetricsSeries, MetaError> {
        let since = range.since_secs(now_secs);
        let window = range.window_secs().max(1);
        self.with_read(move |conn| {
            // Timeline: re-bucket the rollup into the range's downsampling window, carrying errors,
            // bytes, and average latency per window so one series drives every time chart.
            let mut stmt = conn
                .prepare_cached(
                    "SELECT (ts_bucket / ?2) * ?2 AS w,
                        COALESCE(SUM(count),0),
                        COALESCE(SUM(CASE WHEN status_class IN ('4xx','5xx') THEN count ELSE 0 END),0),
                        COALESCE(SUM(bytes_in),0), COALESCE(SUM(bytes_out),0),
                        COALESCE(SUM(lat_sum_ms),0)
                     FROM request_metrics WHERE ts_bucket >= ?1
                     GROUP BY w ORDER BY w",
                )
                .map_err(engine_err)?;
            let timeline: Vec<TimePoint> = stmt
                .query_map(params![since, window], |r| {
                    let count: i64 = r.get(1)?;
                    let lat_sum: i64 = r.get(5)?;
                    Ok(TimePoint {
                        ts: r.get(0)?,
                        count: count as u64,
                        errors: r.get::<_, i64>(2)? as u64,
                        bytes_in: r.get::<_, i64>(3)? as u64,
                        bytes_out: r.get::<_, i64>(4)? as u64,
                        latency_avg_ms: if count > 0 { (lat_sum / count) as u64 } else { 0 },
                    })
                })
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)?;

            // Breakdown by operation: count, bytes, average latency; descending by count.
            let mut stmt = conn
                .prepare_cached(
                    "SELECT operation, COALESCE(SUM(count),0) AS c,
                        COALESCE(SUM(bytes_in + bytes_out),0), COALESCE(SUM(lat_sum_ms),0)
                     FROM request_metrics WHERE ts_bucket >= ?1
                     GROUP BY operation ORDER BY c DESC",
                )
                .map_err(engine_err)?;
            let by_operation = stmt
                .query_map(params![since], |r| {
                    let count: i64 = r.get(1)?;
                    let lat_sum: i64 = r.get(3)?;
                    Ok(OpCount {
                        operation: r.get(0)?,
                        count: count as u64,
                        bytes: r.get::<_, i64>(2)? as u64,
                        latency_avg_ms: if count > 0 { (lat_sum / count) as u64 } else { 0 },
                    })
                })
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)?;

            // Most-active buckets (excluding the non-bucket sentinel), top 10.
            let mut stmt = conn
                .prepare_cached(
                    "SELECT bucket_name, COALESCE(SUM(count),0) AS c,
                        COALESCE(SUM(bytes_in + bytes_out),0)
                     FROM request_metrics WHERE ts_bucket >= ?1 AND bucket_name <> ''
                     GROUP BY bucket_name ORDER BY c DESC LIMIT 10",
                )
                .map_err(engine_err)?;
            let top_buckets = stmt
                .query_map(params![since], |r| {
                    Ok(BucketRequestCount {
                        bucket: r.get(0)?,
                        count: r.get::<_, i64>(1)? as u64,
                        bytes: r.get::<_, i64>(2)? as u64,
                    })
                })
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)?;

            // Breakdown by HTTP status class.
            let mut stmt = conn
                .prepare_cached(
                    "SELECT status_class, COALESCE(SUM(count),0) AS c
                     FROM request_metrics WHERE ts_bucket >= ?1
                     GROUP BY status_class ORDER BY c DESC",
                )
                .map_err(engine_err)?;
            let by_status = stmt
                .query_map(params![since], |r| {
                    Ok(StatusCount {
                        status_class: r.get(0)?,
                        count: r.get::<_, i64>(1)? as u64,
                    })
                })
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)?;

            // Range-wide totals + the latency histogram (for the average and p95).
            let (total_bytes_in, total_bytes_out, lat_sum, hist) = conn
                .query_row(
                    "SELECT COALESCE(SUM(bytes_in),0), COALESCE(SUM(bytes_out),0),
                        COALESCE(SUM(lat_sum_ms),0),
                        COALESCE(SUM(lat_le_5),0), COALESCE(SUM(lat_le_20),0),
                        COALESCE(SUM(lat_le_50),0), COALESCE(SUM(lat_le_200),0),
                        COALESCE(SUM(lat_le_1000),0), COALESCE(SUM(lat_gt_1000),0)
                     FROM request_metrics WHERE ts_bucket >= ?1",
                    params![since],
                    |r| {
                        let hist: [u64; LATENCY_BUCKETS] = [
                            r.get::<_, i64>(3)? as u64,
                            r.get::<_, i64>(4)? as u64,
                            r.get::<_, i64>(5)? as u64,
                            r.get::<_, i64>(6)? as u64,
                            r.get::<_, i64>(7)? as u64,
                            r.get::<_, i64>(8)? as u64,
                        ];
                        Ok((
                            r.get::<_, i64>(0)? as u64,
                            r.get::<_, i64>(1)? as u64,
                            r.get::<_, i64>(2)? as u64,
                            hist,
                        ))
                    },
                )
                .map_err(engine_err)?;

            // Distinct buckets that saw traffic (excluding the non-bucket sentinel).
            let active_buckets: i64 = conn
                .query_row(
                    "SELECT COUNT(DISTINCT bucket_name) FROM request_metrics
                     WHERE ts_bucket >= ?1 AND bucket_name <> ''",
                    params![since],
                    |r| r.get(0),
                )
                .map_err(engine_err)?;

            let total: u64 = by_operation.iter().map(|o| o.count).sum();
            let total_errors: u64 = timeline.iter().map(|p| p.errors).sum();
            let peak_window_count = timeline.iter().map(|p| p.count).max().unwrap_or(0);
            Ok(RequestMetricsSeries {
                timeline,
                by_operation,
                top_buckets,
                by_status,
                total,
                total_errors,
                total_bytes_in,
                total_bytes_out,
                latency_avg_ms: lat_sum.checked_div(total).unwrap_or(0),
                latency_p95_ms: latency_quantile_ms(&hist, 0.95),
                peak_window_count,
                active_buckets: active_buckets as u64,
                window_secs: window,
            })
        })
        .await
    }
}
