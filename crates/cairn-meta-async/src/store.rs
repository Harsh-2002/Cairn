//! [`LibsqlMetadataStore`]: writes go through the single async group-committing [`Writer`];
//! reads run async queries directly against a small pool of read-only driver connections, never
//! contending with the writer. Listing is a half-open range seek with an efficient delimiter
//! skip-scan — the same algorithm as `cairn-meta/src/store.rs`, ported to the async driver.

use crate::driver::{AsyncSqlDriver, Row, Value, query_one};
use crate::model::{
    self, ACTIVITY_COLS, BUCKET_COLS, MULTIPART_COLS, OBJECT_VERSION_COLS, OUTBOX_COLS, PART_COLS,
    SHARE_COLS, SUMMARY_COLS, USER_COLS, WEBHOOK_COLS,
};
use crate::range::{prefix_upper_bound, successor};
use crate::writer::Writer;
use cairn_types::MetaError;
use cairn_types::authz::PublicAccessBlock;
use cairn_types::bucket::{Bucket, ConfigAspect, ConfigDoc};
use cairn_types::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use cairn_types::meta::{
    ActivityEntry, BucketCounts, BucketRequestCount, LATENCY_BUCKETS, ListPage, ListQuery,
    MetricsRange, MultipartSession, Mutation, MutationOutcome, ObjectSummary, OpCount, OutboxEntry,
    PartRecord, ReplicationCounts, ReplicationStatus, ReplicationTargetCounts,
    RequestMetricsSeries, SessionCredentialSummary, ShareRow, StatusCount, StoreCounts, TagSummary,
    TaggedObject, TimePoint, User, UserSessionCredentials, UserSigV4Credentials,
    UserWithBearerHash, WebhookEntry, latency_quantile_ms,
};
use cairn_types::object::ObjectVersionRow;
use cairn_types::time::Timestamp;
use cairn_types::traits::MetadataStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, OwnedMutexGuard};

/// An exclusive read-connection checkout: a pooled driver connection held under its own lock for
/// the whole duration of one (possibly multi-query) read. Dereferences to the driver.
pub(crate) type ReadGuard = OwnedMutexGuard<Arc<dyn AsyncSqlDriver>>;

const LIST_BATCH: usize = 1024;

/// The claim-lease length for replication-outbox entries: a claimed entry whose lease elapses
/// (a stalled worker) becomes eligible for re-claim after this many seconds.
const REPLICATION_LEASE_SECS: i64 = 300;

/// A round-robin pool of read-only driver connections, each behind its own lock. A single
/// libSQL/Turso connection cannot serve two reads at once — concurrent queries would interleave on
/// the one connection and its cursors, returning wrong or leaked rows (audit #8) — so a read checks
/// out a connection *exclusively* for its whole duration. WAL readers take consistent snapshots and
/// never block the writer, so distinct connections still run fully in parallel.
///
/// The underlying engine's `Database` handle the connections were opened from is retained behind
/// an opaque keep-alive box so it (and, for a shared-cache in-memory database, the underlying
/// memory) outlives every connection. The box is engine-agnostic so the same pool serves any
/// [`AsyncSqlDriver`] backend (libSQL, Turso, …).
pub(crate) struct ReadPool {
    conns: Vec<Arc<Mutex<Arc<dyn AsyncSqlDriver>>>>,
    next: AtomicUsize,
    // Held only to keep the engine's database handle alive for the store's lifetime.
    _keepalive: Box<dyn std::any::Any + Send + Sync>,
}

impl ReadPool {
    pub(crate) fn new_with_keepalive(
        conns: Vec<Arc<dyn AsyncSqlDriver>>,
        keepalive: Box<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        assert!(!conns.is_empty(), "read pool must have at least one conn");
        Self {
            conns: conns.into_iter().map(|c| Arc::new(Mutex::new(c))).collect(),
            next: AtomicUsize::new(0),
            _keepalive: keepalive,
        }
    }

    /// Check out the next connection, awaiting exclusive access. Round-robin spreads load; if the
    /// chosen connection is busy the caller waits, so read concurrency is bounded by the pool size
    /// and no connection is ever driven by two reads at once (audit #8).
    async fn acquire(&self) -> ReadGuard {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        self.conns[i].clone().lock_owned().await
    }
}

/// The async-driver-backed metadata store. Engine-agnostic: it drives writes through the async
/// group-committing [`Writer`] and reads through the [`ReadPool`], both over the
/// [`AsyncSqlDriver`] seam, so the same store type serves any backend (libSQL, Turso, …). The
/// [`LibsqlMetadataStore`] alias is the libSQL incarnation.
#[derive(Clone)]
pub struct AsyncMetadataStore {
    pub(crate) writer: Writer,
    pub(crate) reads: Arc<ReadPool>,
}

impl std::fmt::Debug for AsyncMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncMetadataStore").finish_non_exhaustive()
    }
}

impl AsyncMetadataStore {
    pub(crate) fn new(writer: Writer, reads: ReadPool) -> Self {
        Self {
            writer,
            reads: Arc::new(reads),
        }
    }

    /// Check out a read-only driver connection from the pool, held exclusively for the whole read
    /// (audit #8). Bind the returned guard for the duration of the read and deref it (`&**guard`)
    /// to reach the driver.
    async fn reader(&self) -> ReadGuard {
        self.reads.acquire().await
    }

    /// A reconciliation oracle backed by this store, for the blob store's `reconcile`.
    #[must_use]
    pub fn reconcile_oracle(&self) -> AsyncReconcileOracle {
        AsyncReconcileOracle {
            reads: self.reads.clone(),
        }
    }
}

/// The efficient listing implementation: half-open range seek with delimiter skip-scan. The
/// continuation cursor is an inclusive lower bound (the first key NOT yet returned). Ported from
/// `cairn-meta/src/store.rs::list_impl`.
async fn list_impl(
    driver: &dyn AsyncSqlDriver,
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
            driver,
            bucket,
            &seek,
            upper.as_deref(),
            latest_only,
            LIST_BATCH + 1,
        )
        .await?;
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
                    page.next_cursor = Some(summary.key.as_str().to_owned());
                } else if let Some(last) = page.items.last() {
                    // Version listing resumes at the last returned (key, version_id); see the sync
                    // store for the rationale.
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

async fn fetch_rows(
    driver: &dyn AsyncSqlDriver,
    bucket: &str,
    seek: &str,
    upper: Option<&str>,
    latest_only: bool,
    limit: usize,
) -> Result<Vec<ObjectSummary>, MetaError> {
    let mut sql =
        format!("SELECT {SUMMARY_COLS} FROM object_versions WHERE bucket_name = ?1 AND key >= ?2");
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

    let limit = limit as i64;
    let params = vec![
        Value::Text(bucket.to_owned()),
        Value::Text(seek.to_owned()),
        upper.map_or(Value::Null, |u| Value::Text(u.to_owned())),
        Value::Int(limit),
    ];
    let rows = driver.query(&sql, params).await?;
    rows.iter().map(model::object_summary_from_row).collect()
}

#[async_trait::async_trait]
impl MetadataStore for AsyncMetadataStore {
    async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
        self.writer.submit(mutation).await
    }

    async fn get_bucket(&self, name: &BucketName) -> Result<Option<Bucket>, MetaError> {
        let row = query_one(
            &**self.reader().await,
            &format!("SELECT {BUCKET_COLS} FROM buckets WHERE name=?1"),
            vec![Value::Text(name.as_str().to_owned())],
        )
        .await?;
        row.as_ref().map(model::bucket_from_row).transpose()
    }

    async fn list_buckets(&self, owner: Option<&UserId>) -> Result<Vec<Bucket>, MetaError> {
        let (sql, params) = match owner {
            Some(o) => (
                format!("SELECT {BUCKET_COLS} FROM buckets WHERE owner_id=?1 ORDER BY name"),
                vec![Value::Text(o.0.clone())],
            ),
            None => (
                format!("SELECT {BUCKET_COLS} FROM buckets ORDER BY name"),
                vec![],
            ),
        };
        let rows = self.reader().await.query(&sql, params).await?;
        rows.iter().map(model::bucket_from_row).collect()
    }

    async fn get_bucket_config(
        &self,
        name: &BucketName,
        aspect: ConfigAspect,
    ) -> Result<Option<ConfigDoc>, MetaError> {
        let aspect = crate::apply::aspect_str(aspect);
        let row = query_one(
            &**self.reader().await,
            "SELECT doc FROM bucket_config WHERE bucket_name=?1 AND aspect=?2",
            vec![
                Value::Text(name.as_str().to_owned()),
                Value::Text(aspect.to_owned()),
            ],
        )
        .await?;
        Ok(row.and_then(|r| r.get_opt_text(0)).map(ConfigDoc))
    }

    async fn get_account_public_access_block(&self) -> Result<PublicAccessBlock, MetaError> {
        let row = query_one(
            &**self.reader().await,
            "SELECT v FROM account_config WHERE k='public_access_block'",
            vec![],
        )
        .await?;
        let v = row.and_then(|r| r.get_opt_text(0));
        Ok(v.and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default())
    }

    async fn get_bucket_quota(&self, bucket: &BucketName) -> Result<Option<u64>, MetaError> {
        // The query returns no row for "no such bucket"; a NULL cell for "no quota set". Both
        // present to the reader as "no quota". A stored negative is clamped to 0 defensively.
        let row = query_one(
            &**self.reader().await,
            "SELECT quota_bytes FROM buckets WHERE name=?1",
            vec![Value::Text(bucket.as_str().to_owned())],
        )
        .await?;
        Ok(row.and_then(|r| r.get_opt_i64(0)).map(|q| q.max(0) as u64))
    }

    async fn is_bucket_empty(&self, name: &BucketName) -> Result<bool, MetaError> {
        let row = query_one(
            &**self.reader().await,
            // Empty means NO object_versions rows at all (any version or delete marker), matching
            // S3 DeleteBucket semantics (audit #3).
            "SELECT EXISTS(SELECT 1 FROM object_versions WHERE bucket_name=?1)",
            vec![Value::Text(name.as_str().to_owned())],
        )
        .await?;
        Ok(row.is_none_or(|r| r.get_i64(0) == 0))
    }

    async fn current_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        let row = query_one(
            &**self.reader().await,
            &format!(
                "SELECT {OBJECT_VERSION_COLS} FROM object_versions WHERE bucket_name=?1 AND key=?2 AND is_latest=1"
            ),
            vec![
                Value::Text(bucket.as_str().to_owned()),
                Value::Text(key.as_str().to_owned()),
            ],
        )
        .await?;
        row.as_ref().map(model::object_version_from_row).transpose()
    }

    async fn get_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        let row = query_one(
            &**self.reader().await,
            &format!(
                "SELECT {OBJECT_VERSION_COLS} FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3"
            ),
            vec![
                Value::Text(bucket.as_str().to_owned()),
                Value::Text(key.as_str().to_owned()),
                Value::Text(version.as_str().to_owned()),
            ],
        )
        .await?;
        row.as_ref().map(model::object_version_from_row).transpose()
    }

    async fn list_current(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        list_impl(&**self.reader().await, bucket.as_str(), query, true).await
    }

    async fn list_versions(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        list_impl(&**self.reader().await, bucket.as_str(), query, false).await
    }

    async fn enumerate_storage_paths(
        &self,
        bucket: &BucketName,
        cursor: Option<&str>,
        batch: u32,
    ) -> Result<ListPage<StoragePath>, MetaError> {
        let rows = self
            .reader()
            .await
            .query(
                "SELECT storage_path FROM object_versions
                 WHERE bucket_name=?1 AND storage_path IS NOT NULL AND storage_path > ?2
                 ORDER BY storage_path LIMIT ?3",
                vec![
                    Value::Text(bucket.as_str().to_owned()),
                    Value::Text(cursor.unwrap_or("").to_owned()),
                    Value::Int(i64::from(batch) + 1),
                ],
            )
            .await?;
        let mut items: Vec<String> = rows.iter().map(|r| r.get_text(0)).collect();
        let truncated = items.len() > batch as usize;
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
    }

    async fn get_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Vec<(String, String)>, MetaError> {
        let rows = self
            .reader().await
            .query(
                "SELECT tag_key, tag_value FROM object_tags WHERE bucket_name=?1 AND key=?2 AND version_id=?3 ORDER BY tag_key",
                vec![
                    Value::Text(bucket.as_str().to_owned()),
                    Value::Text(key.as_str().to_owned()),
                    Value::Text(version.as_str().to_owned()),
                ],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| (r.get_text(0), r.get_text(1)))
            .collect())
    }

    async fn get_object_lock(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<cairn_types::object::ObjectLockState, MetaError> {
        let rows = self
            .reader().await
            .query(
                "SELECT lock_mode, retain_until, legal_hold FROM object_locks WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
                vec![
                    Value::Text(bucket.as_str().to_owned()),
                    Value::Text(key.as_str().to_owned()),
                    Value::Text(version.as_str().to_owned()),
                ],
            )
            .await?;
        let Some(r) = rows.first() else {
            return Ok(cairn_types::object::ObjectLockState::default());
        };
        // lock_mode and retain_until are written together, so retention is present iff lock_mode is.
        let retention = r
            .get_opt_text(0)
            .map(|m| cairn_types::object::ObjectRetention {
                mode: model::lock_mode_from(&m),
                retain_until: cairn_types::time::Timestamp(r.get_i64(1)),
            });
        Ok(cairn_types::object::ObjectLockState {
            retention,
            legal_hold: r.get_i64(2) != 0,
        })
    }

    async fn get_multipart(
        &self,
        upload: &UploadId,
    ) -> Result<Option<MultipartSession>, MetaError> {
        let row = query_one(
            &**self.reader().await,
            &format!("SELECT {MULTIPART_COLS} FROM multipart_uploads WHERE id=?1"),
            vec![Value::Text(upload.as_str().to_owned())],
        )
        .await?;
        row.as_ref().map(model::multipart_from_row).transpose()
    }

    async fn list_parts(
        &self,
        upload: &UploadId,
        part_number_marker: u16,
        limit: u32,
    ) -> Result<ListPage<PartRecord>, MetaError> {
        let rows = self
            .reader().await
            .query(
                &format!(
                    "SELECT {PART_COLS} FROM multipart_parts WHERE upload_id=?1 AND part_number>?2 ORDER BY part_number LIMIT ?3"
                ),
                vec![
                    Value::Text(upload.as_str().to_owned()),
                    Value::Int(i64::from(part_number_marker)),
                    Value::Int(i64::from(limit) + 1),
                ],
            )
            .await?;
        let mut items: Vec<PartRecord> = rows
            .iter()
            .map(model::part_from_row)
            .collect::<Result<_, _>>()?;
        let truncated = items.len() > limit as usize;
        items.truncate(limit as usize);
        let next_cursor = if truncated {
            items.last().map(|p| p.part_number.to_string())
        } else {
            None
        };
        Ok(ListPage {
            items,
            common_prefixes: Vec::new(),
            next_cursor,
            next_version_id_marker: None,
            truncated,
        })
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<MultipartSession>, MetaError> {
        let prefix = query.prefix.clone().unwrap_or_default();
        let upper = prefix_upper_bound(&prefix);
        // Resume strictly after the cursor key when one is supplied; otherwise seek to the
        // half-open prefix lower bound.
        let seek = match &query.cursor {
            Some(c) if c.as_str() > prefix.as_str() => c.clone(),
            _ => prefix.clone(),
        };
        let limit = query.limit.max(1) as usize;
        // Half-open `prefix_upper_bound` seek like the other listings, fetching one extra row to
        // detect truncation.
        let mut sql = format!(
            "SELECT {MULTIPART_COLS} FROM multipart_uploads WHERE bucket_name=?1 AND status='active' AND key>=?2"
        );
        if upper.is_some() {
            sql.push_str(" AND key<?3");
        }
        sql.push_str(" ORDER BY key, id LIMIT ?4");
        let params = vec![
            Value::Text(bucket.as_str().to_owned()),
            Value::Text(seek),
            upper.map_or(Value::Null, Value::Text),
            Value::Int((limit + 1) as i64),
        ];
        let rows = self.reader().await.query(&sql, params).await?;
        let mut items: Vec<MultipartSession> = rows
            .iter()
            .map(model::multipart_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let truncated = items.len() > limit;
        items.truncate(limit);
        let next_cursor = if truncated {
            items.last().map(|s| s.key.as_str().to_owned())
        } else {
            None
        };
        Ok(ListPage {
            items,
            common_prefixes: Vec::new(),
            next_cursor,
            next_version_id_marker: None,
            truncated,
        })
    }

    async fn enumerate_stale_sessions(
        &self,
        older_than: Timestamp,
        batch: u32,
    ) -> Result<Vec<MultipartSession>, MetaError> {
        let rows = self
            .reader()
            .await
            .query(
                &format!(
                    "SELECT {MULTIPART_COLS} FROM multipart_uploads WHERE updated_at < ?1 LIMIT ?2"
                ),
                vec![Value::Int(older_than.0), Value::Int(i64::from(batch))],
            )
            .await?;
        rows.iter().map(model::multipart_from_row).collect()
    }

    async fn object_replication_status(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ReplicationStatus>, MetaError> {
        let row = query_one(
            &**self.reader().await,
            "SELECT replication_status FROM object_versions WHERE bucket_name=?1 AND key=?2 AND version_id=?3",
            vec![
                Value::Text(bucket.as_str().to_owned()),
                Value::Text(key.as_str().to_owned()),
                Value::Text(version.as_str().to_owned()),
            ],
        )
        .await?;
        Ok(row
            .and_then(|r| r.get_opt_text(0))
            .map(|s| model::repl_status_from(&s)))
    }

    async fn has_unreplicated_predecessor(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        before: &VersionId,
        target: Option<&str>,
    ) -> Result<bool, MetaError> {
        // version_id is uuidv7 (time-ordered): a strictly-lower id is an earlier write. A
        // pending/claimed predecessor is still owed and must ship before this later version
        // (per-key ordering, audit #9). A `failed` predecessor is settled/terminal and must NOT
        // block successors forever — treat it like completed here so newer versions proceed
        // (best-effort/at-least-once, ARCH 20.4); mirrors cairn-meta. Scoped per target
        // (`target_arn IS ?4` is null-safe) so a slow target never blocks a healthy one.
        let row = query_one(
            &**self.reader().await,
            "SELECT EXISTS(SELECT 1 FROM replication_outbox \
             WHERE bucket_name=?1 AND key=?2 AND version_id<?3 AND target_arn IS ?4 \
             AND status NOT IN ('completed','failed'))",
            vec![
                Value::Text(bucket.as_str().to_owned()),
                Value::Text(key.as_str().to_owned()),
                Value::Text(before.as_str().to_owned()),
                target.map_or(Value::Null, |t| Value::Text(t.to_owned())),
            ],
        )
        .await?;
        Ok(row.is_some_and(|r| r.get_i64(0) != 0))
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
        // Read-only mirror of the claim predicate; no mutation (see the sync store for rationale).
        let rows = self
            .reader()
            .await
            .query(
                &format!(
                    "SELECT {OUTBOX_COLS} FROM replication_outbox \
                     WHERE next_attempt_at<=?1 \
                       AND (status='pending' OR (status='claimed' AND lease_until<?1)) \
                     ORDER BY priority DESC, next_attempt_at LIMIT ?2"
                ),
                vec![Value::Int(now.0), Value::Int(i64::from(limit))],
            )
            .await?;
        rows.iter().map(model::outbox_from_row).collect()
    }

    async fn list_failed_replication(&self, limit: u32) -> Result<Vec<OutboxEntry>, MetaError> {
        let rows = self
            .reader().await
            .query(
                &format!(
                    "SELECT {OUTBOX_COLS} FROM replication_outbox WHERE status='failed' ORDER BY next_attempt_at DESC LIMIT ?1"
                ),
                vec![Value::Int(i64::from(limit))],
            )
            .await?;
        rows.iter().map(model::outbox_from_row).collect()
    }

    async fn replication_counts(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<ReplicationCounts, MetaError> {
        let b = bucket.map_or(Value::Null, |x| Value::Text(x.as_str().to_owned()));
        let mut counts = ReplicationCounts::default();
        // Totals by status (`?1 IS NULL` makes the bucket filter optional).
        let rows = self
            .reader()
            .await
            .query(
                "SELECT status, COUNT(*) FROM replication_outbox \
                 WHERE (?1 IS NULL OR bucket_name = ?1) GROUP BY status",
                vec![b.clone()],
            )
            .await?;
        for row in &rows {
            let n = row.get_i64(1) as u64;
            match row.get_text(0).as_str() {
                "pending" => counts.pending = n,
                "claimed" => counts.claimed = n,
                "failed" => counts.failed = n,
                "completed" => counts.completed = n,
                _ => {}
            }
        }
        // Per-target pending/failed breakdown.
        let rows = self
            .reader()
            .await
            .query(
                "SELECT target_arn, \
                 SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END), \
                 SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) \
                 FROM replication_outbox WHERE (?1 IS NULL OR bucket_name = ?1) GROUP BY target_arn",
                vec![b.clone()],
            )
            .await?;
        for row in &rows {
            let pending = row.get_i64(1) as u64;
            let failed = row.get_i64(2) as u64;
            if pending > 0 || failed > 0 {
                counts.by_target.push(ReplicationTargetCounts {
                    target_arn: row.get_opt_text(0),
                    pending,
                    failed,
                });
            }
        }
        // Oldest still-pending enqueue time (0 = none / pre-migration unknowns).
        let rows = self
            .reader()
            .await
            .query(
                "SELECT COALESCE(MIN(NULLIF(enqueued_at, 0)), 0) FROM replication_outbox \
                 WHERE status='pending' AND (?1 IS NULL OR bucket_name = ?1)",
                vec![b],
            )
            .await?;
        counts.oldest_pending_at_ms = rows.first().map_or(0, |r| r.get_i64(0));
        Ok(counts)
    }

    async fn claim_webhook_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError> {
        let outcome = self
            .submit(Mutation::ClaimWebhookBatch {
                limit,
                now,
                lease_secs: REPLICATION_LEASE_SECS,
            })
            .await?;
        match outcome {
            MutationOutcome::WebhookBatch(entries) => Ok(entries),
            other => Err(MetaError::Engine(format!(
                "unexpected outcome for ClaimWebhookBatch: {other:?}"
            ))),
        }
    }

    async fn list_due_webhooks(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError> {
        let rows = self
            .reader()
            .await
            .query(
                &format!(
                    "SELECT {WEBHOOK_COLS} FROM events_outbox \
                     WHERE next_attempt_at<=?1 \
                       AND (status='pending' OR (status='claimed' AND lease_until<?1)) \
                     ORDER BY priority DESC, next_attempt_at LIMIT ?2"
                ),
                vec![Value::Int(now.0), Value::Int(i64::from(limit))],
            )
            .await?;
        rows.iter().map(model::webhook_from_row).collect()
    }

    async fn list_failed_webhooks(&self, limit: u32) -> Result<Vec<WebhookEntry>, MetaError> {
        let rows = self
            .reader().await
            .query(
                &format!(
                    "SELECT {WEBHOOK_COLS} FROM events_outbox WHERE status='failed' ORDER BY next_attempt_at DESC LIMIT ?1"
                ),
                vec![Value::Int(i64::from(limit))],
            )
            .await?;
        rows.iter().map(model::webhook_from_row).collect()
    }

    async fn user_by_bearer_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserWithBearerHash>, MetaError> {
        let row = query_one(
            &**self.reader().await,
            &format!("SELECT {USER_COLS} FROM users WHERE access_key_id=?1"),
            vec![Value::Text(access_key_id.to_owned())],
        )
        .await?;
        row.as_ref()
            .map(model::user_with_bearer_from_row)
            .transpose()
    }

    async fn user_by_sigv4_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserSigV4Credentials>, MetaError> {
        let row = query_one(
            &**self.reader().await,
            &format!("SELECT {USER_COLS} FROM users WHERE sigv4_access_key_id=?1"),
            vec![Value::Text(access_key_id.to_owned())],
        )
        .await?;
        match row {
            Some(r) => model::user_sigv4_from_row(&r),
            None => Ok(None),
        }
    }

    async fn user_by_session_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserSessionCredentials>, MetaError> {
        let row = query_one(
            &**self.reader().await,
            "SELECT s.parent_user_id, s.secret_ciphertext, s.secret_nonce, \
                    s.session_token_hash, s.inline_policy, s.expires_at, \
                    u.display_name, u.is_active \
             FROM session_credentials s \
             LEFT JOIN users u ON u.id = s.parent_user_id \
             WHERE s.access_key_id = ?1",
            vec![Value::Text(access_key_id.to_owned())],
        )
        .await?;
        Ok(row.map(|r| UserSessionCredentials {
            parent_user_id: cairn_types::id::UserId(r.get_text(0)),
            secret_ciphertext: r.get_opt_blob(1).unwrap_or_default(),
            secret_nonce: r.get_opt_blob(2).unwrap_or_default(),
            session_token_hash: r.get_text(3),
            inline_policy: r.get_opt_text(4),
            expires_at: Timestamp(r.get_i64(5)),
            parent_display_name: r.get_opt_text(6).unwrap_or_default(),
            // A session whose parent row is gone (LEFT JOIN NULL) is treated as inactive.
            parent_is_active: r.get_opt_i64(7).unwrap_or(0) != 0,
        }))
    }

    async fn list_session_credentials(
        &self,
        now: Timestamp,
    ) -> Result<Vec<SessionCredentialSummary>, MetaError> {
        let rows = self
            .reader()
            .await
            .query(
                "SELECT access_key_id, parent_user_id, inline_policy, created_at, expires_at \
                 FROM session_credentials WHERE expires_at > ?1 ORDER BY created_at DESC",
                vec![Value::Int(now.0)],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| SessionCredentialSummary {
                access_key_id: r.get_text(0),
                parent_user_id: cairn_types::id::UserId(r.get_text(1)),
                has_inline_policy: r.get_opt_text(2).is_some(),
                created_at: Timestamp(r.get_i64(3)),
                expires_at: Timestamp(r.get_i64(4)),
            })
            .collect())
    }

    async fn count_users(&self) -> Result<u64, MetaError> {
        let row = query_one(&**self.reader().await, "SELECT COUNT(*) FROM users", vec![]).await?;
        Ok(row.map_or(0, |r| r.get_i64(0)) as u64)
    }

    async fn list_users(&self) -> Result<Vec<User>, MetaError> {
        let rows = self
            .reader()
            .await
            .query(
                &format!("SELECT {USER_COLS} FROM users ORDER BY created_at"),
                vec![],
            )
            .await?;
        rows.iter().map(model::user_from_row).collect()
    }

    async fn get_user_policy(&self, user_id: &UserId) -> Result<Option<String>, MetaError> {
        let row = query_one(
            &**self.reader().await,
            "SELECT policy FROM users WHERE id=?1",
            vec![Value::Text(user_id.0.as_str().to_owned())],
        )
        .await?;
        Ok(row.and_then(|r| r.get_opt_text(0)))
    }

    async fn list_activity(&self, limit: u32) -> Result<Vec<ActivityEntry>, MetaError> {
        let rows = self
            .reader()
            .await
            .query(
                &format!("SELECT {ACTIVITY_COLS} FROM activity ORDER BY at DESC LIMIT ?1"),
                vec![Value::Int(i64::from(limit))],
            )
            .await?;
        rows.iter().map(model::activity_from_row).collect()
    }

    async fn get_share(&self, token: &str) -> Result<Option<ShareRow>, MetaError> {
        let rows = self
            .reader()
            .await
            .query(
                &format!("SELECT {SHARE_COLS} FROM object_shares WHERE token=?1"),
                vec![Value::Text(token.to_owned())],
            )
            .await?;
        rows.first().map(model::share_from_row).transpose()
    }

    async fn list_shares(
        &self,
        bucket: &BucketName,
        key: Option<&ObjectKey>,
    ) -> Result<Vec<ShareRow>, MetaError> {
        let guard = self.reader().await;
        let rows = match key {
            Some(k) => {
                guard
                    .query(
                        &format!(
                            "SELECT {SHARE_COLS} FROM object_shares WHERE bucket_name=?1 AND key=?2 ORDER BY created_at DESC"
                        ),
                        vec![
                            Value::Text(bucket.as_str().to_owned()),
                            Value::Text(k.as_str().to_owned()),
                        ],
                    )
                    .await?
            }
            None => {
                guard
                    .query(
                        &format!(
                            "SELECT {SHARE_COLS} FROM object_shares WHERE bucket_name=?1 ORDER BY created_at DESC"
                        ),
                        vec![Value::Text(bucket.as_str().to_owned())],
                    )
                    .await?
            }
        };
        rows.iter().map(model::share_from_row).collect()
    }

    async fn list_tag_summary(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<Vec<TagSummary>, MetaError> {
        let bucket_param = match bucket {
            Some(b) => Value::Text(b.as_str().to_owned()),
            None => Value::Null,
        };
        let rows = self
            .reader()
            .await
            .query(
                "SELECT ot.tag_key, ot.tag_value, COUNT(*) AS c
                 FROM object_tags ot
                 JOIN object_versions ov
                   ON ov.bucket_name = ot.bucket_name AND ov.key = ot.key
                      AND ov.version_id = ot.version_id
                 WHERE ov.is_latest = 1 AND ov.is_delete_marker = 0
                   AND (?1 IS NULL OR ot.bucket_name = ?1)
                 GROUP BY ot.tag_key, ot.tag_value
                 ORDER BY c DESC, ot.tag_key, ot.tag_value",
                vec![bucket_param],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| TagSummary {
                tag_key: r.get_text(0),
                tag_value: r.get_text(1),
                object_count: r.get_i64(2) as u64,
            })
            .collect())
    }

    async fn list_objects_by_tag(
        &self,
        bucket: Option<&BucketName>,
        tag_key: &str,
        tag_value: &str,
        limit: u32,
    ) -> Result<Vec<TaggedObject>, MetaError> {
        let bucket_param = match bucket {
            Some(b) => Value::Text(b.as_str().to_owned()),
            None => Value::Null,
        };
        let rows = self
            .reader()
            .await
            .query(
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
                vec![
                    Value::Text(tag_key.to_owned()),
                    Value::Text(tag_value.to_owned()),
                    bucket_param,
                    Value::Int(limit as i64),
                ],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| TaggedObject {
                bucket: r.get_text(0),
                key: r.get_text(1),
                version_id: r.get_text(2),
                size: r.get_i64(3) as u64,
                last_modified: Timestamp(r.get_i64(4)),
            })
            .collect())
    }

    async fn aggregate_counts(&self) -> Result<StoreCounts, MetaError> {
        let driver_guard = self.reader().await;
        let driver: &dyn AsyncSqlDriver = &**driver_guard;
        let buckets = query_one(driver, "SELECT COUNT(*) FROM buckets", vec![])
            .await?
            .map_or(0, |r| r.get_i64(0));
        // Versions and byte totals come from the maintained roll-up (O(buckets)), not a scan of
        // every object version (Phase 2.1).
        let agg = query_one(
            driver,
            "SELECT COALESCE(SUM(versions),0), COALESCE(SUM(logical_bytes),0),
                    COALESCE(SUM(physical_bytes),0)
             FROM bucket_stats",
            vec![],
        )
        .await?
        .unwrap_or_default();
        let (versions, logical, physical) = (agg.get_i64(0), agg.get_i64(1), agg.get_i64(2));
        // The current-visible object count is an index-only scan of the partial current-version
        // index (idx_ov_latest_cover): one entry per live object, not every version.
        let objects = query_one(
            driver,
            "SELECT COUNT(*) FROM object_versions WHERE is_latest=1 AND is_delete_marker=0",
            vec![],
        )
        .await?
        .map_or(0, |r| r.get_i64(0));
        Ok(StoreCounts {
            buckets: buckets as u64,
            objects: objects as u64,
            versions: versions as u64,
            logical_bytes: logical as u64,
            physical_bytes: physical as u64,
        })
    }

    async fn bucket_counts(&self) -> Result<Vec<BucketCounts>, MetaError> {
        // Byte totals from the maintained roll-up (LEFT JOIN so empty buckets show zeros); the
        // current-object count joins a GROUP BY over the partial current-version index. Neither
        // path scans historical versions for the byte sums.
        let rows = self
            .reader()
            .await
            .query(
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
                vec![],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| BucketCounts {
                bucket: r.get_text(0),
                objects: r.get_i64(1) as u64,
                logical_bytes: r.get_i64(2) as u64,
                physical_bytes: r.get_i64(3) as u64,
            })
            .collect())
    }

    async fn query_request_metrics(
        &self,
        range: MetricsRange,
        now_secs: i64,
    ) -> Result<RequestMetricsSeries, MetaError> {
        let since = range.since_secs(now_secs);
        let window = range.window_secs().max(1);
        let driver_guard = self.reader().await;
        let driver: &dyn AsyncSqlDriver = &**driver_guard;

        // Timeline: re-bucket into the range's window, carrying errors, bytes, and average latency.
        let timeline: Vec<TimePoint> = driver
            .query(
                "SELECT (ts_bucket / ?2) * ?2 AS w,
                    COALESCE(SUM(count),0),
                    COALESCE(SUM(CASE WHEN status_class IN ('4xx','5xx') THEN count ELSE 0 END),0),
                    COALESCE(SUM(bytes_in),0), COALESCE(SUM(bytes_out),0),
                    COALESCE(SUM(lat_sum_ms),0)
                 FROM request_metrics WHERE ts_bucket >= ?1
                 GROUP BY w ORDER BY w",
                vec![Value::Int(since), Value::Int(window)],
            )
            .await?
            .iter()
            .map(|r| {
                let count = r.get_i64(1) as u64;
                let lat_sum = r.get_i64(5) as u64;
                TimePoint {
                    ts: r.get_i64(0),
                    count,
                    errors: r.get_i64(2) as u64,
                    bytes_in: r.get_i64(3) as u64,
                    bytes_out: r.get_i64(4) as u64,
                    latency_avg_ms: lat_sum.checked_div(count).unwrap_or(0),
                }
            })
            .collect();

        // Breakdown by operation: count, bytes, average latency; descending.
        let by_operation: Vec<OpCount> = driver
            .query(
                "SELECT operation, COALESCE(SUM(count),0) AS c,
                    COALESCE(SUM(bytes_in + bytes_out),0), COALESCE(SUM(lat_sum_ms),0)
                 FROM request_metrics WHERE ts_bucket >= ?1
                 GROUP BY operation ORDER BY c DESC",
                vec![Value::Int(since)],
            )
            .await?
            .iter()
            .map(|r| {
                let count = r.get_i64(1) as u64;
                let lat_sum = r.get_i64(3) as u64;
                OpCount {
                    operation: r.get_text(0),
                    count,
                    bytes: r.get_i64(2) as u64,
                    latency_avg_ms: lat_sum.checked_div(count).unwrap_or(0),
                }
            })
            .collect();

        // Most-active buckets (excluding the non-bucket sentinel), top 10.
        let top_buckets = driver
            .query(
                "SELECT bucket_name, COALESCE(SUM(count),0) AS c,
                    COALESCE(SUM(bytes_in + bytes_out),0)
                 FROM request_metrics WHERE ts_bucket >= ?1 AND bucket_name <> ''
                 GROUP BY bucket_name ORDER BY c DESC LIMIT 10",
                vec![Value::Int(since)],
            )
            .await?
            .iter()
            .map(|r| BucketRequestCount {
                bucket: r.get_text(0),
                count: r.get_i64(1) as u64,
                bytes: r.get_i64(2) as u64,
            })
            .collect();

        // Top buckets by bytes transferred (in + out) — a different ranking than by count, so the
        // console's "by data" panel ranks on bytes directly rather than re-sorting the by-count top-N.
        let top_buckets_by_bytes = driver
            .query(
                "SELECT bucket_name, COALESCE(SUM(count),0),
                    COALESCE(SUM(bytes_in + bytes_out),0) AS b
                 FROM request_metrics WHERE ts_bucket >= ?1 AND bucket_name <> ''
                 GROUP BY bucket_name ORDER BY b DESC LIMIT 10",
                vec![Value::Int(since)],
            )
            .await?
            .iter()
            .map(|r| BucketRequestCount {
                bucket: r.get_text(0),
                count: r.get_i64(1) as u64,
                bytes: r.get_i64(2) as u64,
            })
            .collect();

        // Breakdown by HTTP status class.
        let by_status: Vec<StatusCount> = driver
            .query(
                "SELECT status_class, COALESCE(SUM(count),0) AS c
                 FROM request_metrics WHERE ts_bucket >= ?1
                 GROUP BY status_class ORDER BY c DESC",
                vec![Value::Int(since)],
            )
            .await?
            .iter()
            .map(|r| StatusCount {
                status_class: r.get_text(0),
                count: r.get_i64(1) as u64,
            })
            .collect();

        // Range-wide totals + latency histogram (for the average and p95).
        let agg = query_one(
            driver,
            "SELECT COALESCE(SUM(bytes_in),0), COALESCE(SUM(bytes_out),0),
                COALESCE(SUM(lat_sum_ms),0),
                COALESCE(SUM(lat_le_5),0), COALESCE(SUM(lat_le_20),0),
                COALESCE(SUM(lat_le_50),0), COALESCE(SUM(lat_le_200),0),
                COALESCE(SUM(lat_le_1000),0), COALESCE(SUM(lat_gt_1000),0)
             FROM request_metrics WHERE ts_bucket >= ?1",
            vec![Value::Int(since)],
        )
        .await?
        .unwrap_or_default();
        let total_bytes_in = agg.get_i64(0) as u64;
        let total_bytes_out = agg.get_i64(1) as u64;
        let lat_sum = agg.get_i64(2) as u64;
        let hist: [u64; LATENCY_BUCKETS] = [
            agg.get_i64(3) as u64,
            agg.get_i64(4) as u64,
            agg.get_i64(5) as u64,
            agg.get_i64(6) as u64,
            agg.get_i64(7) as u64,
            agg.get_i64(8) as u64,
        ];

        let active_buckets = query_one(
            driver,
            "SELECT COUNT(DISTINCT bucket_name) FROM request_metrics
             WHERE ts_bucket >= ?1 AND bucket_name <> ''",
            vec![Value::Int(since)],
        )
        .await?
        .map_or(0, |r| r.get_i64(0)) as u64;

        let total: u64 = by_operation.iter().map(|o| o.count).sum();
        let total_errors: u64 = timeline.iter().map(|p| p.errors).sum();
        let peak_window_count = timeline.iter().map(|p| p.count).max().unwrap_or(0);
        Ok(RequestMetricsSeries {
            timeline,
            by_operation,
            top_buckets,
            top_buckets_by_bytes,
            by_status,
            total,
            total_errors,
            total_bytes_in,
            total_bytes_out,
            latency_avg_ms: lat_sum.checked_div(total).unwrap_or(0),
            latency_p95_ms: latency_quantile_ms(&hist, 0.95),
            peak_window_count,
            active_buckets,
            window_secs: window,
        })
    }
}

/// A [`cairn_types::traits::ReconcileOracle`] answering membership questions against the live
/// metadata, backed by the same read pool as the store. Engine-agnostic; [`LibsqlReconcileOracle`]
/// is the libSQL alias.
#[derive(Clone)]
pub struct AsyncReconcileOracle {
    reads: Arc<ReadPool>,
}

impl std::fmt::Debug for AsyncReconcileOracle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncReconcileOracle")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl cairn_types::traits::ReconcileOracle for AsyncReconcileOracle {
    async fn live_blobs(&self, candidates: &[StoragePath]) -> Result<Vec<bool>, MetaError> {
        let guard = self.reads.acquire().await;
        let driver: &dyn AsyncSqlDriver = &**guard;
        let mut out = Vec::with_capacity(candidates.len());
        for p in candidates {
            let row: Option<Row> = query_one(
                driver,
                "SELECT EXISTS(SELECT 1 FROM object_versions WHERE storage_path=?1)",
                vec![Value::Text(p.as_str().to_owned())],
            )
            .await?;
            out.push(row.is_some_and(|r| r.get_i64(0) != 0));
        }
        Ok(out)
    }

    async fn live_session(&self, upload: &UploadId) -> Result<bool, MetaError> {
        let row = query_one(
            &**self.reads.acquire().await,
            "SELECT EXISTS(SELECT 1 FROM multipart_uploads WHERE id=?1)",
            vec![Value::Text(upload.as_str().to_owned())],
        )
        .await?;
        Ok(row.is_some_and(|r| r.get_i64(0) != 0))
    }
}
