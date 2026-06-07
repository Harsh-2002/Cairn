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
    ActivityEntry, ListPage, ListQuery, MultipartSession, Mutation, MutationOutcome, ObjectSummary,
    OutboxEntry, PartRecord, ReplicationStatus, StoreCounts, User, UserSigV4Credentials,
    UserWithBearerHash,
};
use cairn_types::object::ObjectVersionRow;
use cairn_types::time::Timestamp;
use cairn_types::traits::MetadataStore;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OptionalExtension, params};

const LIST_BATCH: usize = 1024;

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

    let mut page = ListPage::default();
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
            if page.items.len() + page.common_prefixes.len() >= limit {
                page.truncated = true;
                page.next_cursor = Some(summary.key.as_str().to_owned());
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

    async fn is_bucket_empty(&self, name: &BucketName) -> Result<bool, MetaError> {
        let name = name.as_str().to_owned();
        self.with_read(move |conn| {
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM object_versions WHERE bucket_name=?1 AND is_latest=1 AND is_delete_marker=0)",
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
            Ok(ListPage { items, common_prefixes: Vec::new(), next_cursor, truncated })
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
            let mut stmt = conn
                .prepare_cached(
                    "SELECT * FROM multipart_uploads WHERE bucket_name=?1 AND status='active' AND key>=?2 ORDER BY key, id",
                )
                .map_err(engine_err)?;
            let rows = stmt
                .query_map(params![b, prefix], model::multipart_from_row)
                .map_err(engine_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(engine_err)?;
            let mut items: Vec<MultipartSession> =
                rows.into_iter().filter(|s| s.key.as_str().starts_with(&prefix)).collect();
            let truncated = items.len() > q.limit.max(1) as usize;
            items.truncate(q.limit.max(1) as usize);
            Ok(ListPage { items, common_prefixes: Vec::new(), next_cursor: None, truncated })
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

    async fn claim_replication_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        self.with_read(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT * FROM replication_outbox WHERE status='pending' AND next_attempt_at<=?1 ORDER BY next_attempt_at LIMIT ?2",
                )
                .map_err(engine_err)?;
            stmt.query_map(params![now.0, i64::from(limit)], model::outbox_from_row)
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

    async fn aggregate_counts(&self) -> Result<StoreCounts, MetaError> {
        self.with_read(move |conn| {
            let buckets: i64 =
                conn.query_row("SELECT COUNT(*) FROM buckets", [], |r| r.get(0)).map_err(engine_err)?;
            let (objects, logical, physical): (i64, i64, i64) = conn
                .query_row(
                    "SELECT
                        COALESCE(SUM(CASE WHEN is_latest=1 AND is_delete_marker=0 THEN 1 ELSE 0 END),0),
                        COALESCE(SUM(size_logical),0),
                        COALESCE(SUM(size_physical),0)
                     FROM object_versions",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .map_err(engine_err)?;
            let versions: i64 = conn
                .query_row("SELECT COUNT(*) FROM object_versions", [], |r| r.get(0))
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
}
