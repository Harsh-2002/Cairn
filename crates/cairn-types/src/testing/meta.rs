//! An in-memory [`MetadataStore`] double backed by `BTreeMap`s. It implements the *semantics*
//! the real SQLite store must provide — atomic conditional check-and-set, last-writer-wins by
//! submission order, versioning/delete-marker bookkeeping, and bounded listing — without the
//! group-commit machinery (mutations apply atomically under one lock).

use crate::authz::PublicAccessBlock;
use crate::bucket::{Bucket, ConfigAspect, ConfigDoc};
use crate::error::MetaError;
use crate::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use crate::meta::{
    ActivityEntry, BucketCounts, BucketRequestCount, ClaimOutcome, IfNoneMatch, LATENCY_BUCKETS,
    ListPage, ListQuery, MetricsRange, MultipartSession, MultipartStatus, Mutation,
    MutationOutcome, ObjectSummary, OpCount, OutboxEntry, PartRecord, Precondition,
    ReplicationStatus, RequestMetricsSeries, ShareRow, StatusCount, StoreCounts, TagSummary,
    TaggedObject, TimePoint, User, UserRecord, UserSigV4Credentials, UserWithBearerHash,
    WebhookEntry, WebhookStatus, latency_quantile_ms,
};
use crate::object::{ETag, ObjectVersionRow};
use crate::time::Timestamp;
use crate::traits::{MetadataStore, ReconcileOracle};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

type VKey = (String, String, String); // (bucket, key, version_id)

#[derive(Default)]
struct State {
    buckets: BTreeMap<String, Bucket>,
    /// Per-bucket byte quota (`buckets.quota_bytes`), absent when unlimited. The double does not
    /// enforce the quota (that lives in the SQLite writer), but it records the configured value so
    /// `get_bucket_quota` round-trips a `SetBucketQuota`.
    ///
    /// The per-user byte quota (`users.quota_bytes`, ARCH 27.5) is likewise enforced only in the
    /// SQLite writer's commit transaction; like the bucket quota it is not enforced here, and —
    /// having no `SetUserQuota` mutation or reader on `MetadataStore` — there is nothing for the
    /// double to round-trip, so no user-quota state is modeled.
    bucket_quotas: HashMap<String, u64>,
    config: HashMap<(String, ConfigAspect), ConfigDoc>,
    account_bpa: PublicAccessBlock,
    versions: BTreeMap<VKey, ObjectVersionRow>,
    tags: HashMap<VKey, Vec<(String, String)>>,
    locks: HashMap<VKey, crate::object::ObjectLockState>,
    multipart: BTreeMap<String, MultipartSession>,
    parts: BTreeMap<(String, u16), PartRecord>,
    outbox: Vec<OutboxEntry>,
    webhook_outbox: Vec<WebhookEntry>,
    users: BTreeMap<String, UserRecord>,
    /// Per-user identity policy JSON (`users.policy`), keyed by user id. Absent when the user has no
    /// attached policy; mirrors the real stores' nullable `policy` column without touching the
    /// shared `UserRecord` type.
    user_policies: BTreeMap<String, String>,
    activity: Vec<ActivityEntry>,
    /// Object-share tokens (ARCH 15.8), keyed by token.
    shares: BTreeMap<String, ShareRow>,
    /// Request-metrics rollup (ARCH 26.5), keyed by (ts_bucket, operation, bucket, status_class).
    request_metrics: BTreeMap<(i64, String, String, String), MetricCell>,
}

/// The accumulated metrics for one rollup key in the in-memory double (mirrors the SQL columns).
#[derive(Default, Clone)]
struct MetricCell {
    count: u64,
    bytes_in: u64,
    bytes_out: u64,
    lat_sum_ms: u64,
    lat_hist: [u64; LATENCY_BUCKETS],
}

impl State {
    fn latest(&self, bucket: &str, key: &str) -> Option<&ObjectVersionRow> {
        self.versions
            .values()
            .filter(|r| r.bucket.as_str() == bucket && r.key.as_str() == key && r.is_latest)
            .max_by(|a, b| a.version_id.as_str().cmp(b.version_id.as_str()))
    }

    fn demote_all(&mut self, bucket: &str, key: &str) {
        for r in self.versions.values_mut() {
            if r.bucket.as_str() == bucket && r.key.as_str() == key {
                r.is_latest = false;
            }
        }
    }

    fn check_precondition(
        &self,
        bucket: &str,
        key: &str,
        pc: &Precondition,
    ) -> Result<(), MetaError> {
        if pc.is_unconditional() {
            return Ok(());
        }
        let current = self.latest(bucket, key).filter(|r| !r.is_delete_marker);
        if let Some(want) = &pc.if_match {
            match current {
                Some(r) if r.etag == *want => {}
                _ => return Err(MetaError::PreconditionFailed),
            }
        }
        if let Some(inm) = &pc.if_none_match {
            match inm {
                IfNoneMatch::Any => {
                    if current.is_some() {
                        return Err(MetaError::PreconditionFailed);
                    }
                }
                IfNoneMatch::ETag(e) => {
                    if let Some(r) = current {
                        if r.etag == *e {
                            return Err(MetaError::PreconditionFailed);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn upsert_version(&mut self, mut row: ObjectVersionRow) -> Option<StoragePath> {
        let vk: VKey = (
            row.bucket.as_str().to_owned(),
            row.key.as_str().to_owned(),
            row.version_id.as_str().to_owned(),
        );
        let superseded = self.versions.get(&vk).and_then(|r| r.storage_path.clone());
        self.demote_all(row.bucket.as_str(), row.key.as_str());
        row.is_latest = true;
        self.versions.insert(vk, row);
        superseded
    }
}

/// An in-memory metadata store.
#[derive(Default)]
pub struct InMemoryMetadataStore {
    state: Mutex<State>,
}

impl std::fmt::Debug for InMemoryMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryMetadataStore")
            .finish_non_exhaustive()
    }
}

impl InMemoryMetadataStore {
    /// A fresh empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the set of live storage paths and upload sessions, for building a
    /// [`SetReconcileOracle`].
    #[must_use]
    pub fn live_set(&self) -> (HashSet<String>, HashSet<String>) {
        let st = self.state.lock().unwrap();
        let paths = st
            .versions
            .values()
            .filter_map(|r| r.storage_path.as_ref().map(|p| p.as_str().to_owned()))
            .collect();
        let uploads = st.multipart.keys().cloned().collect();
        (paths, uploads)
    }

    /// Build an oracle reflecting the current live set.
    #[must_use]
    pub fn oracle(&self) -> SetReconcileOracle {
        let (paths, uploads) = self.live_set();
        SetReconcileOracle {
            live_paths: paths,
            live_uploads: uploads,
        }
    }
}

fn key_after(q: &ListQuery) -> Option<&str> {
    q.cursor.as_deref().or(q.start_after.as_deref())
}

fn summarize(r: &ObjectVersionRow) -> ObjectSummary {
    ObjectSummary {
        key: r.key.clone(),
        version_id: r.version_id.clone(),
        is_latest: r.is_latest,
        is_delete_marker: r.is_delete_marker,
        etag: r.etag.clone(),
        size: r.size_logical,
        last_modified: r.updated_at,
        storage_class: r.storage_class,
        owner_id: r.owner_id.clone(),
    }
}

/// Page a set of rows (already filtered) honouring prefix, delimiter, cursor, and limit.
fn page_rows(
    rows: Vec<&ObjectVersionRow>,
    q: &ListQuery,
    version_listing: bool,
) -> ListPage<ObjectSummary> {
    let prefix = q.prefix.clone().unwrap_or_default();
    let after = key_after(q).map(str::to_owned);
    // A version-id marker (paired with the cursor key) resumes strictly after `(key, marker)`
    // within that key. Versions sort `version_id DESC`, so entries already returned for the marker
    // key have `version_id >= marker`; we exclude exactly those, plus all rows at-or-before the key.
    let vid_marker: Option<(String, String)> = q
        .version_id_marker
        .as_deref()
        .zip(q.cursor.as_deref())
        .map(|(vid, key)| (key.to_owned(), vid.to_owned()));
    let mut ordered: Vec<&ObjectVersionRow> = rows
        .into_iter()
        .filter(|r| r.key.as_str().starts_with(&prefix))
        .filter(|r| match (&after, &vid_marker) {
            // When a version-id marker is in play, the marker key itself is not excluded by the
            // key cursor; its post-marker versions resume below.
            (Some(a), Some((mk, _))) => {
                r.key.as_str() > a.as_str() || r.key.as_str() == mk.as_str()
            }
            (Some(a), None) => r.key.as_str() > a.as_str(),
            (None, _) => true,
        })
        .filter(|r| match &vid_marker {
            Some((mk, mv)) if r.key.as_str() == mk.as_str() => r.version_id.as_str() < mv.as_str(),
            _ => true,
        })
        .collect();
    ordered.sort_by(|a, b| {
        a.key
            .as_str()
            .cmp(b.key.as_str())
            .then_with(|| b.version_id.as_str().cmp(a.version_id.as_str()))
    });

    let mut page = ListPage::default();
    let mut seen_cp: HashSet<String> = HashSet::new();
    let limit = q.limit.max(1) as usize;
    let mut last_key: Option<String> = None;
    let mut last_version: Option<String> = None;

    for r in ordered {
        let count = page.items.len() + page.common_prefixes.len();
        if count >= limit {
            page.truncated = true;
            page.next_cursor = last_key.clone();
            if version_listing {
                page.next_version_id_marker = last_version.clone();
            }
            break;
        }
        if let Some(delim) = q.delimiter.as_deref() {
            let rest = &r.key.as_str()[prefix.len()..];
            if let Some(idx) = rest.find(delim) {
                let cp = format!("{}{}{}", prefix, &rest[..idx], delim);
                if seen_cp.insert(cp.clone()) {
                    page.common_prefixes.push(cp);
                    last_key = Some(r.key.as_str().to_owned());
                }
                continue;
            }
        }
        page.items.push(summarize(r));
        last_key = Some(r.key.as_str().to_owned());
        last_version = Some(r.version_id.as_str().to_owned());
    }
    page
}

#[async_trait::async_trait]
impl MetadataStore for InMemoryMetadataStore {
    async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
        let mut st = self.state.lock().unwrap();
        match mutation {
            Mutation::PutObjectVersion {
                row,
                precondition,
                replication,
            } => {
                st.check_precondition(row.bucket.as_str(), row.key.as_str(), &precondition)?;
                let version_id = row.version_id.clone();
                let superseded = st.upsert_version(*row);
                if let Some(entry) = replication {
                    st.outbox.push(entry);
                }
                Ok(MutationOutcome::Put {
                    superseded,
                    version_id,
                })
            }
            Mutation::CreateDeleteMarker {
                bucket,
                key,
                version_id,
                owner_id,
                now,
                replication,
            } => {
                st.demote_all(bucket.as_str(), key.as_str());
                let row = ObjectVersionRow {
                    id: uuid_like(&version_id),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: version_id.clone(),
                    is_latest: true,
                    is_delete_marker: true,
                    size_logical: 0,
                    size_physical: 0,
                    etag: ETag::from_string(String::new()),
                    content_type: String::new(),
                    content_encoding: None,
                    cache_control: None,
                    content_disposition: None,
                    content_language: None,
                    expires: None,
                    storage_path: None,
                    compression: crate::object::CompressionDescriptor::Uncompressed,
                    storage_class: crate::object::StorageClass::Standard,
                    cold_locator: None,
                    owner_id,
                    user_metadata: Vec::new(),
                    acl: None,
                    checksums: Vec::new(),
                    sse_descriptor: None,
                    replication_status: None,
                    created_at: now,
                    updated_at: now,
                };
                let vk = (
                    bucket.as_str().to_owned(),
                    key.as_str().to_owned(),
                    version_id.as_str().to_owned(),
                );
                st.versions.insert(vk, row);
                if let Some(entry) = replication {
                    st.outbox.push(entry);
                }
                Ok(MutationOutcome::DeleteMarker { version_id })
            }
            Mutation::DeleteVersion {
                bucket,
                key,
                version_id,
            } => {
                let vk = (
                    bucket.as_str().to_owned(),
                    key.as_str().to_owned(),
                    version_id.as_str().to_owned(),
                );
                let removed = st.versions.remove(&vk);
                st.tags.remove(&vk);
                st.locks.remove(&vk);
                let freed = removed.as_ref().and_then(|r| r.storage_path.clone());
                let was_latest = removed.as_ref().is_some_and(|r| r.is_latest);
                let mut promoted = false;
                if was_latest {
                    if let Some(next) = st
                        .versions
                        .values_mut()
                        .filter(|r| {
                            r.bucket.as_str() == bucket.as_str() && r.key.as_str() == key.as_str()
                        })
                        .max_by(|a, b| a.version_id.as_str().cmp(b.version_id.as_str()))
                    {
                        next.is_latest = true;
                        promoted = true;
                    }
                }
                Ok(MutationOutcome::Deleted {
                    freed,
                    promoted_latest: promoted,
                })
            }
            Mutation::CreateMultipart(session) => {
                let id = session.upload_id.clone();
                st.multipart.insert(id.as_str().to_owned(), *session);
                Ok(MutationOutcome::MultipartCreated(id))
            }
            Mutation::RecordPart { upload_id, part } => {
                let pk = (upload_id.as_str().to_owned(), part.part_number);
                let superseded = st.parts.get(&pk).map(|p| p.storage_path.clone());
                st.parts.insert(pk, part);
                Ok(MutationOutcome::PartRecorded { superseded })
            }
            Mutation::ClaimMultipart(upload_id) => {
                let outcome = match st.multipart.get_mut(upload_id.as_str()) {
                    Some(s) if s.status == MultipartStatus::Active => {
                        s.status = MultipartStatus::Completing;
                        ClaimOutcome::Claimed(Box::new(s.clone()))
                    }
                    Some(_) => ClaimOutcome::AlreadyClaimed,
                    None => ClaimOutcome::NotFound,
                };
                Ok(MutationOutcome::MultipartClaim(outcome))
            }
            Mutation::CompleteMultipart {
                upload_id,
                row,
                precondition,
                replication,
            } => {
                st.check_precondition(row.bucket.as_str(), row.key.as_str(), &precondition)?;
                let version_id = row.version_id.clone();
                let superseded = st.upsert_version(*row);
                st.multipart.remove(upload_id.as_str());
                st.parts.retain(|(u, _), _| u != upload_id.as_str());
                if let Some(entry) = replication {
                    st.outbox.push(entry);
                }
                Ok(MutationOutcome::MultipartCompleted {
                    superseded,
                    version_id,
                })
            }
            Mutation::AbortMultipart(upload_id) => {
                st.multipart.remove(upload_id.as_str());
                st.parts.retain(|(u, _), _| u != upload_id.as_str());
                Ok(MutationOutcome::Ack)
            }
            Mutation::CreateBucket(bucket) => {
                if st.buckets.contains_key(bucket.name.as_str()) {
                    return Err(MetaError::Conflict);
                }
                st.buckets.insert(bucket.name.as_str().to_owned(), *bucket);
                Ok(MutationOutcome::Ack)
            }
            Mutation::DeleteBucket(name) => {
                st.buckets.remove(name.as_str());
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetBucketConfig {
                bucket,
                aspect,
                doc,
            } => {
                let k = (bucket.as_str().to_owned(), aspect);
                match doc {
                    Some(d) => {
                        st.config.insert(k, d);
                    }
                    None => {
                        st.config.remove(&k);
                    }
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetObjectRetention {
                bucket,
                key,
                version_id,
                retention,
            } => {
                let vk = (
                    bucket.as_str().to_owned(),
                    key.as_str().to_owned(),
                    version_id.as_str().to_owned(),
                );
                st.locks.entry(vk).or_default().retention = retention;
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetObjectLegalHold {
                bucket,
                key,
                version_id,
                on,
            } => {
                let vk = (
                    bucket.as_str().to_owned(),
                    key.as_str().to_owned(),
                    version_id.as_str().to_owned(),
                );
                st.locks.entry(vk).or_default().legal_hold = on;
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetVersioning { bucket, state } => {
                if let Some(b) = st.buckets.get_mut(bucket.as_str()) {
                    b.versioning = state;
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetOwnership { bucket, mode } => {
                if let Some(b) = st.buckets.get_mut(bucket.as_str()) {
                    b.ownership_mode = mode;
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetBucketQuota {
                bucket,
                quota_bytes,
            } => {
                // The double does not enforce the quota (that lives in the SQLite writer's commit
                // transaction); it records the configured value so `get_bucket_quota` reads it back.
                match quota_bytes {
                    Some(q) => {
                        st.bucket_quotas.insert(bucket.as_str().to_owned(), q);
                    }
                    None => {
                        st.bucket_quotas.remove(bucket.as_str());
                    }
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetBucketCompression { bucket, policy } => {
                if let Some(b) = st.buckets.get_mut(bucket.as_str()) {
                    b.compression = policy;
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetUserPolicy { user_id, policy } => {
                match policy {
                    Some(doc) => st.user_policies.insert(user_id.0.as_str().to_owned(), doc),
                    None => st.user_policies.remove(user_id.0.as_str()),
                };
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetUserQuota {
                user_id: _,
                quota_bytes: _,
            } => {
                // User-quota enforcement lives in the SQLite writer's commit transaction (like the
                // bucket quota); the double neither enforces nor — absent a user-quota reader on the
                // trait — round-trips it, so this is an accepted no-op.
                Ok(MutationOutcome::Ack)
            }
            Mutation::RetryFailedReplication { bucket, now } => {
                for e in &mut st.outbox {
                    if e.status == ReplicationStatus::Failed
                        && bucket
                            .as_ref()
                            .is_none_or(|b| e.bucket.as_str() == b.as_str())
                    {
                        e.status = ReplicationStatus::Pending;
                        e.next_attempt_at = now;
                        e.attempts = 0;
                        e.lease_until = None;
                    }
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetAccountPublicAccessBlock(bpa) => {
                st.account_bpa = bpa;
                Ok(MutationOutcome::Ack)
            }
            Mutation::PutObjectTags {
                bucket,
                key,
                version_id,
                tags,
            } => {
                st.tags.insert(
                    (
                        bucket.as_str().to_owned(),
                        key.as_str().to_owned(),
                        version_id.as_str().to_owned(),
                    ),
                    tags,
                );
                Ok(MutationOutcome::Ack)
            }
            Mutation::DeleteObjectTags {
                bucket,
                key,
                version_id,
            } => {
                st.tags.remove(&(
                    bucket.as_str().to_owned(),
                    key.as_str().to_owned(),
                    version_id.as_str().to_owned(),
                ));
                Ok(MutationOutcome::Ack)
            }
            Mutation::SetObjectAcl {
                bucket,
                key,
                version_id,
                acl,
            } => {
                let vk = (
                    bucket.as_str().to_owned(),
                    key.as_str().to_owned(),
                    version_id.as_str().to_owned(),
                );
                if let Some(row) = st.versions.get_mut(&vk) {
                    row.acl = acl;
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::CreateUser(rec) => {
                let id = rec.user.id.clone();
                st.users.insert(id.to_string(), *rec);
                Ok(MutationOutcome::UserCreated(id))
            }
            Mutation::UpdateUser(rec) => {
                st.users.insert(rec.user.id.to_string(), *rec);
                Ok(MutationOutcome::Ack)
            }
            Mutation::DeactivateUser(id) => {
                if let Some(u) = st.users.get_mut(&id.to_string()) {
                    u.user.is_active = false;
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::ClaimReplicationBatch {
                limit,
                now,
                lease_secs,
            } => {
                let lease_until = Timestamp(now.0 + lease_secs * 1000);
                // Due = pending, or claimed with an expired lease, and next_attempt_at has passed.
                let mut due: Vec<usize> = st
                    .outbox
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| {
                        e.next_attempt_at <= now
                            && (e.status == ReplicationStatus::Pending
                                || (e.status == ReplicationStatus::Claimed
                                    && e.lease_until.is_some_and(|l| l < now)))
                    })
                    .map(|(i, _)| i)
                    .collect();
                // Order by priority DESC, then next_attempt_at ASC (mirroring the SQL claim).
                due.sort_by(|&a, &b| {
                    let (ea, eb) = (&st.outbox[a], &st.outbox[b]);
                    eb.priority
                        .cmp(&ea.priority)
                        .then(ea.next_attempt_at.cmp(&eb.next_attempt_at))
                });
                due.truncate(limit as usize);
                let mut claimed = Vec::with_capacity(due.len());
                for i in due {
                    let e = &mut st.outbox[i];
                    e.status = ReplicationStatus::Claimed;
                    e.lease_until = Some(lease_until);
                    claimed.push(e.clone());
                }
                Ok(MutationOutcome::ReplicationBatch(claimed))
            }
            Mutation::MarkReplicationDone(id) => {
                if let Some(e) = st.outbox.iter_mut().find(|e| e.id == id) {
                    e.status = ReplicationStatus::Completed;
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::MarkReplicationFailed {
                id,
                error,
                next_attempt_at,
            } => {
                if let Some(e) = st.outbox.iter_mut().find(|e| e.id == id) {
                    e.attempts += 1;
                    e.last_error = Some(error);
                    match next_attempt_at {
                        // Reschedule: release the claim back to pending (mirrors the SQL
                        // `status='pending'` reset) so the entry is re-claimable after backoff.
                        Some(t) => {
                            e.next_attempt_at = t;
                            e.status = ReplicationStatus::Pending;
                            e.lease_until = None;
                        }
                        None => e.status = ReplicationStatus::Failed,
                    }
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::EnqueueReplication(entry) => {
                // Idempotent on the entry id (mirrors INSERT OR IGNORE in the SQLite stores).
                if !st.outbox.iter().any(|e| e.id == entry.id) {
                    st.outbox.push(*entry);
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::EnqueueWebhooks(entries) => {
                for entry in entries {
                    if !st.webhook_outbox.iter().any(|e| e.id == entry.id) {
                        st.webhook_outbox.push(entry);
                    }
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::ClaimWebhookBatch {
                limit,
                now,
                lease_secs,
            } => {
                let lease_until = Timestamp(now.0 + lease_secs * 1000);
                let mut due: Vec<usize> = st
                    .webhook_outbox
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| {
                        e.next_attempt_at <= now
                            && (e.status == WebhookStatus::Pending
                                || (e.status == WebhookStatus::Claimed
                                    && e.lease_until.is_some_and(|l| l < now)))
                    })
                    .map(|(i, _)| i)
                    .collect();
                due.sort_by(|&a, &b| {
                    let (ea, eb) = (&st.webhook_outbox[a], &st.webhook_outbox[b]);
                    eb.priority
                        .cmp(&ea.priority)
                        .then(ea.next_attempt_at.cmp(&eb.next_attempt_at))
                });
                due.truncate(limit as usize);
                let mut claimed = Vec::with_capacity(due.len());
                for i in due {
                    let e = &mut st.webhook_outbox[i];
                    e.status = WebhookStatus::Claimed;
                    e.lease_until = Some(lease_until);
                    claimed.push(e.clone());
                }
                Ok(MutationOutcome::WebhookBatch(claimed))
            }
            Mutation::MarkWebhookDone(id) => {
                if let Some(e) = st.webhook_outbox.iter_mut().find(|e| e.id == id) {
                    e.status = WebhookStatus::Completed;
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::MarkWebhookFailed {
                id,
                error,
                next_attempt_at,
            } => {
                if let Some(e) = st.webhook_outbox.iter_mut().find(|e| e.id == id) {
                    e.attempts += 1;
                    e.last_error = Some(error);
                    match next_attempt_at {
                        Some(t) => {
                            e.next_attempt_at = t;
                            e.status = WebhookStatus::Pending;
                            e.lease_until = None;
                        }
                        None => e.status = WebhookStatus::Failed,
                    }
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::CreateShare(s) => {
                st.shares.insert(s.token.clone(), *s);
                Ok(MutationOutcome::Ack)
            }
            Mutation::RevokeShare { token, now } => {
                if let Some(s) = st.shares.get_mut(&token) {
                    if s.revoked_at.is_none() {
                        s.revoked_at = Some(now);
                    }
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::RecordActivity(entry) => {
                st.activity.push(*entry);
                Ok(MutationOutcome::Ack)
            }
            Mutation::RecordRequestMetrics { rows, prune_before } => {
                for r in rows {
                    let cell = st
                        .request_metrics
                        .entry((r.ts_bucket, r.operation, r.bucket, r.status_class))
                        .or_default();
                    cell.count += r.count;
                    cell.bytes_in += r.bytes_in;
                    cell.bytes_out += r.bytes_out;
                    cell.lat_sum_ms += r.lat_sum_ms;
                    for i in 0..LATENCY_BUCKETS {
                        cell.lat_hist[i] += r.lat_hist[i];
                    }
                }
                if let Some(before) = prune_before {
                    st.request_metrics.retain(|(ts, ..), _| *ts >= before);
                }
                Ok(MutationOutcome::Ack)
            }
        }
    }

    async fn get_bucket(&self, name: &BucketName) -> Result<Option<Bucket>, MetaError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .buckets
            .get(name.as_str())
            .cloned())
    }

    async fn list_buckets(&self, owner: Option<&UserId>) -> Result<Vec<Bucket>, MetaError> {
        let st = self.state.lock().unwrap();
        Ok(st
            .buckets
            .values()
            .filter(|b| owner.is_none_or(|o| &b.owner_id == o))
            .cloned()
            .collect())
    }

    async fn get_bucket_config(
        &self,
        name: &BucketName,
        aspect: ConfigAspect,
    ) -> Result<Option<ConfigDoc>, MetaError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .config
            .get(&(name.as_str().to_owned(), aspect))
            .cloned())
    }

    async fn get_account_public_access_block(&self) -> Result<PublicAccessBlock, MetaError> {
        Ok(self.state.lock().unwrap().account_bpa)
    }

    async fn get_bucket_quota(&self, bucket: &BucketName) -> Result<Option<u64>, MetaError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .bucket_quotas
            .get(bucket.as_str())
            .copied())
    }

    async fn is_bucket_empty(&self, name: &BucketName) -> Result<bool, MetaError> {
        // Empty means NO versions at all (any version or delete marker), matching S3 DeleteBucket
        // semantics (audit #3).
        let st = self.state.lock().unwrap();
        Ok(!st
            .versions
            .values()
            .any(|r| r.bucket.as_str() == name.as_str()))
    }

    async fn current_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .latest(bucket.as_str(), key.as_str())
            .cloned())
    }

    async fn get_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        let vk = (
            bucket.as_str().to_owned(),
            key.as_str().to_owned(),
            version.as_str().to_owned(),
        );
        Ok(self.state.lock().unwrap().versions.get(&vk).cloned())
    }

    async fn list_current(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        let st = self.state.lock().unwrap();
        let rows: Vec<&ObjectVersionRow> = st
            .versions
            .values()
            .filter(|r| r.bucket.as_str() == bucket.as_str() && r.is_latest && !r.is_delete_marker)
            .collect();
        Ok(page_rows(rows, query, false))
    }

    async fn list_versions(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        let st = self.state.lock().unwrap();
        let rows: Vec<&ObjectVersionRow> = st
            .versions
            .values()
            .filter(|r| r.bucket.as_str() == bucket.as_str())
            .collect();
        Ok(page_rows(rows, query, true))
    }

    async fn enumerate_storage_paths(
        &self,
        bucket: &BucketName,
        cursor: Option<&str>,
        batch: u32,
    ) -> Result<ListPage<StoragePath>, MetaError> {
        let st = self.state.lock().unwrap();
        let mut paths: Vec<String> = st
            .versions
            .values()
            .filter(|r| r.bucket.as_str() == bucket.as_str())
            .filter_map(|r| r.storage_path.as_ref().map(|p| p.as_str().to_owned()))
            .filter(|p| cursor.is_none_or(|c| p.as_str() > c))
            .collect();
        paths.sort();
        let truncated = paths.len() > batch as usize;
        paths.truncate(batch as usize);
        let next_cursor = if truncated {
            paths.last().cloned()
        } else {
            None
        };
        Ok(ListPage {
            items: paths.into_iter().map(StoragePath::from_string).collect(),
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
        let vk = (
            bucket.as_str().to_owned(),
            key.as_str().to_owned(),
            version.as_str().to_owned(),
        );
        Ok(self
            .state
            .lock()
            .unwrap()
            .tags
            .get(&vk)
            .cloned()
            .unwrap_or_default())
    }

    async fn get_object_lock(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<crate::object::ObjectLockState, MetaError> {
        let vk = (
            bucket.as_str().to_owned(),
            key.as_str().to_owned(),
            version.as_str().to_owned(),
        );
        Ok(self
            .state
            .lock()
            .unwrap()
            .locks
            .get(&vk)
            .copied()
            .unwrap_or_default())
    }

    async fn get_multipart(
        &self,
        upload: &UploadId,
    ) -> Result<Option<MultipartSession>, MetaError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .multipart
            .get(upload.as_str())
            .cloned())
    }

    async fn list_parts(
        &self,
        upload: &UploadId,
        part_number_marker: u16,
        limit: u32,
    ) -> Result<ListPage<PartRecord>, MetaError> {
        let st = self.state.lock().unwrap();
        let mut items: Vec<PartRecord> = st
            .parts
            .iter()
            .filter(|((u, n), _)| u == upload.as_str() && *n > part_number_marker)
            .map(|(_, p)| p.clone())
            .collect();
        items.sort_by_key(|p| p.part_number);
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
        let st = self.state.lock().unwrap();
        let prefix = query.prefix.clone().unwrap_or_default();
        let after = query.cursor.as_deref();
        let mut items: Vec<MultipartSession> = st
            .multipart
            .values()
            .filter(|s| {
                s.bucket.as_str() == bucket.as_str()
                    && s.status == MultipartStatus::Active
                    && s.key.as_str().starts_with(&prefix)
                    && after.is_none_or(|c| s.key.as_str() > c)
            })
            .cloned()
            .collect();
        items.sort_by(|a, b| {
            a.key
                .as_str()
                .cmp(b.key.as_str())
                .then_with(|| a.upload_id.as_str().cmp(b.upload_id.as_str()))
        });
        let limit = query.limit.max(1) as usize;
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
        let st = self.state.lock().unwrap();
        Ok(st
            .multipart
            .values()
            .filter(|s| s.updated_at < older_than)
            .take(batch as usize)
            .cloned()
            .collect())
    }

    async fn object_replication_status(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ReplicationStatus>, MetaError> {
        let vk = (
            bucket.as_str().to_owned(),
            key.as_str().to_owned(),
            version.as_str().to_owned(),
        );
        Ok(self
            .state
            .lock()
            .unwrap()
            .versions
            .get(&vk)
            .and_then(|r| r.replication_status))
    }

    async fn has_unreplicated_predecessor(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        before: &VersionId,
    ) -> Result<bool, MetaError> {
        // version_id is uuidv7 (time-ordered); a strictly-lower id is an earlier write that has
        // not shipped unless its outbox row is `Completed` (audit #9).
        let st = self.state.lock().unwrap();
        Ok(st.outbox.iter().any(|e| {
            e.bucket.as_str() == bucket.as_str()
                && e.key.as_str() == key.as_str()
                && e.version_id.as_str() < before.as_str()
                && e.status != ReplicationStatus::Completed
        }))
    }

    async fn claim_replication_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        // Mirror the real stores: claiming is a write that marks entries `claimed` under a lease.
        match self
            .submit(Mutation::ClaimReplicationBatch {
                limit,
                now,
                lease_secs: 300,
            })
            .await?
        {
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
        // Read-only mirror of the claim predicate; no mutation.
        let st = self.state.lock().unwrap();
        let mut due: Vec<OutboxEntry> = st
            .outbox
            .iter()
            .filter(|e| {
                e.next_attempt_at <= now
                    && (e.status == ReplicationStatus::Pending
                        || (e.status == ReplicationStatus::Claimed
                            && e.lease_until.is_some_and(|l| l < now)))
            })
            .cloned()
            .collect();
        due.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.next_attempt_at.cmp(&b.next_attempt_at))
        });
        due.truncate(limit as usize);
        Ok(due)
    }

    async fn list_failed_replication(&self, limit: u32) -> Result<Vec<OutboxEntry>, MetaError> {
        let st = self.state.lock().unwrap();
        // Terminal entries are those the engine marked `Failed` (retries exhausted). Return them
        // most-recently-due first, matching the SQLite reader's `ORDER BY next_attempt_at DESC`.
        let mut failed: Vec<OutboxEntry> = st
            .outbox
            .iter()
            .filter(|e| e.status == ReplicationStatus::Failed)
            .cloned()
            .collect();
        failed.sort_by_key(|e| std::cmp::Reverse(e.next_attempt_at));
        failed.truncate(limit as usize);
        Ok(failed)
    }

    async fn claim_webhook_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError> {
        match self
            .submit(Mutation::ClaimWebhookBatch {
                limit,
                now,
                lease_secs: 300,
            })
            .await?
        {
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
        let st = self.state.lock().unwrap();
        let mut due: Vec<WebhookEntry> = st
            .webhook_outbox
            .iter()
            .filter(|e| {
                e.next_attempt_at <= now
                    && (e.status == WebhookStatus::Pending
                        || (e.status == WebhookStatus::Claimed
                            && e.lease_until.is_some_and(|l| l < now)))
            })
            .cloned()
            .collect();
        due.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.next_attempt_at.cmp(&b.next_attempt_at))
        });
        due.truncate(limit as usize);
        Ok(due)
    }

    async fn list_failed_webhooks(&self, limit: u32) -> Result<Vec<WebhookEntry>, MetaError> {
        let st = self.state.lock().unwrap();
        let mut failed: Vec<WebhookEntry> = st
            .webhook_outbox
            .iter()
            .filter(|e| e.status == WebhookStatus::Failed)
            .cloned()
            .collect();
        failed.sort_by_key(|e| std::cmp::Reverse(e.next_attempt_at));
        failed.truncate(limit as usize);
        Ok(failed)
    }

    async fn user_by_bearer_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserWithBearerHash>, MetaError> {
        let st = self.state.lock().unwrap();
        Ok(st
            .users
            .values()
            .find(|r| r.user.access_key_id == access_key_id)
            .map(|r| UserWithBearerHash {
                user: r.user.clone(),
                secret_hash: r.bearer_secret_hash.clone(),
            }))
    }

    async fn user_by_sigv4_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserSigV4Credentials>, MetaError> {
        let st = self.state.lock().unwrap();
        Ok(st
            .users
            .values()
            .find(|r| r.user.sigv4_access_key_id.as_deref() == Some(access_key_id))
            .and_then(|r| {
                Some(UserSigV4Credentials {
                    user: r.user.clone(),
                    secret_ciphertext: r.sigv4_secret_ciphertext.clone()?,
                    secret_nonce: r.sigv4_secret_nonce.clone()?,
                })
            }))
    }

    async fn count_users(&self) -> Result<u64, MetaError> {
        Ok(self.state.lock().unwrap().users.len() as u64)
    }

    async fn list_users(&self) -> Result<Vec<User>, MetaError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .users
            .values()
            .map(|r| r.user.clone())
            .collect())
    }

    async fn get_user_policy(&self, user_id: &UserId) -> Result<Option<String>, MetaError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .user_policies
            .get(user_id.0.as_str())
            .cloned())
    }

    async fn list_activity(&self, limit: u32) -> Result<Vec<ActivityEntry>, MetaError> {
        let st = self.state.lock().unwrap();
        Ok(st
            .activity
            .iter()
            .rev()
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn get_share(&self, token: &str) -> Result<Option<ShareRow>, MetaError> {
        let st = self.state.lock().unwrap();
        Ok(st.shares.get(token).cloned())
    }

    async fn list_shares(
        &self,
        bucket: &BucketName,
        key: Option<&ObjectKey>,
    ) -> Result<Vec<ShareRow>, MetaError> {
        let st = self.state.lock().unwrap();
        let mut out: Vec<ShareRow> = st
            .shares
            .values()
            .filter(|s| {
                s.bucket.as_str() == bucket.as_str()
                    && key.is_none_or(|k| s.key.as_str() == k.as_str())
            })
            .cloned()
            .collect();
        // Most recent first, matching the SQL stores.
        out.sort_by_key(|s| std::cmp::Reverse(s.created_at.0));
        Ok(out)
    }

    async fn list_tag_summary(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<Vec<TagSummary>, MetaError> {
        let st = self.state.lock().unwrap();
        let mut counts: BTreeMap<(String, String), u64> = BTreeMap::new();
        for (vkey, tag_list) in &st.tags {
            let Some(v) = st.versions.get(vkey) else {
                continue;
            };
            // Only current objects (latest, non-delete-marker), optionally bucket-scoped.
            if !v.is_latest || v.is_delete_marker {
                continue;
            }
            if bucket.is_some_and(|b| b.as_str() != vkey.0) {
                continue;
            }
            for (k, val) in tag_list {
                *counts.entry((k.clone(), val.clone())).or_insert(0) += 1;
            }
        }
        let mut out: Vec<TagSummary> = counts
            .into_iter()
            .map(|((tag_key, tag_value), object_count)| TagSummary {
                tag_key,
                tag_value,
                object_count,
            })
            .collect();
        out.sort_by(|a, b| {
            b.object_count
                .cmp(&a.object_count)
                .then(a.tag_key.cmp(&b.tag_key))
                .then(a.tag_value.cmp(&b.tag_value))
        });
        Ok(out)
    }

    async fn list_objects_by_tag(
        &self,
        bucket: Option<&BucketName>,
        tag_key: &str,
        tag_value: &str,
        limit: u32,
    ) -> Result<Vec<TaggedObject>, MetaError> {
        let st = self.state.lock().unwrap();
        let mut out: Vec<TaggedObject> = Vec::new();
        for (vkey, tag_list) in &st.tags {
            let Some(v) = st.versions.get(vkey) else {
                continue;
            };
            if !v.is_latest || v.is_delete_marker {
                continue;
            }
            if bucket.is_some_and(|b| b.as_str() != vkey.0) {
                continue;
            }
            if tag_list
                .iter()
                .any(|(k, val)| k == tag_key && val == tag_value)
            {
                out.push(TaggedObject {
                    bucket: vkey.0.clone(),
                    key: vkey.1.clone(),
                    version_id: vkey.2.clone(),
                    size: v.size_logical,
                    last_modified: v.updated_at,
                });
            }
        }
        out.sort_by(|a, b| a.bucket.cmp(&b.bucket).then(a.key.cmp(&b.key)));
        out.truncate(limit as usize);
        Ok(out)
    }

    async fn aggregate_counts(&self) -> Result<StoreCounts, MetaError> {
        let st = self.state.lock().unwrap();
        let mut c = StoreCounts {
            buckets: st.buckets.len() as u64,
            ..Default::default()
        };
        for r in st.versions.values() {
            c.versions += 1;
            if r.is_latest && !r.is_delete_marker {
                c.objects += 1;
            }
            c.logical_bytes += r.size_logical;
            c.physical_bytes += r.size_physical;
        }
        Ok(c)
    }

    async fn bucket_counts(&self) -> Result<Vec<BucketCounts>, MetaError> {
        let st = self.state.lock().unwrap();
        // `st.buckets` is a BTreeMap, so the seed map is already name-ordered with empty
        // buckets present at zero — matching the SQL LEFT JOIN ... GROUP BY semantics.
        let mut by_bucket: BTreeMap<String, BucketCounts> = st
            .buckets
            .keys()
            .map(|name| {
                (
                    name.clone(),
                    BucketCounts {
                        bucket: name.clone(),
                        ..Default::default()
                    },
                )
            })
            .collect();
        for r in st.versions.values() {
            let Some(c) = by_bucket.get_mut(r.bucket.as_str()) else {
                continue;
            };
            if r.is_latest && !r.is_delete_marker {
                c.objects += 1;
            }
            c.logical_bytes += r.size_logical;
            c.physical_bytes += r.size_physical;
        }
        Ok(by_bucket.into_values().collect())
    }

    async fn query_request_metrics(
        &self,
        range: MetricsRange,
        now_secs: i64,
    ) -> Result<RequestMetricsSeries, MetaError> {
        let since = range.since_secs(now_secs);
        let window = range.window_secs().max(1);
        let st = self.state.lock().unwrap();

        // (count, errors, bytes_in, bytes_out, lat_sum) accumulators per dimension.
        let mut tl: BTreeMap<i64, (u64, u64, u64, u64, u64)> = BTreeMap::new();
        let mut by_op: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new(); // count, bytes, lat_sum
        let mut by_bkt: BTreeMap<String, (u64, u64)> = BTreeMap::new(); // count, bytes
        let mut by_st: BTreeMap<String, u64> = BTreeMap::new();
        let (mut t_in, mut t_out, mut t_lat) = (0u64, 0u64, 0u64);
        let mut hist = [0u64; LATENCY_BUCKETS];

        for ((ts, op, bucket, status), c) in &st.request_metrics {
            if *ts < since {
                continue;
            }
            let is_err = status == "4xx" || status == "5xx";
            let bytes = c.bytes_in + c.bytes_out;
            let e = tl.entry((ts / window) * window).or_default();
            e.0 += c.count;
            e.1 += if is_err { c.count } else { 0 };
            e.2 += c.bytes_in;
            e.3 += c.bytes_out;
            e.4 += c.lat_sum_ms;
            let o = by_op.entry(op.clone()).or_default();
            o.0 += c.count;
            o.1 += bytes;
            o.2 += c.lat_sum_ms;
            if !bucket.is_empty() {
                let b = by_bkt.entry(bucket.clone()).or_default();
                b.0 += c.count;
                b.1 += bytes;
            }
            *by_st.entry(status.clone()).or_insert(0) += c.count;
            t_in += c.bytes_in;
            t_out += c.bytes_out;
            t_lat += c.lat_sum_ms;
            for (h, x) in hist.iter_mut().zip(c.lat_hist.iter()) {
                *h += *x;
            }
        }

        let timeline: Vec<TimePoint> = tl
            .into_iter()
            .map(|(ts, (count, errors, bi, bo, lat))| TimePoint {
                ts,
                count,
                errors,
                bytes_in: bi,
                bytes_out: bo,
                latency_avg_ms: lat.checked_div(count).unwrap_or(0),
            })
            .collect();

        let mut by_operation: Vec<OpCount> = by_op
            .into_iter()
            .map(|(operation, (count, bytes, lat))| OpCount {
                operation,
                count,
                bytes,
                latency_avg_ms: lat.checked_div(count).unwrap_or(0),
            })
            .collect();
        by_operation.sort_by(|a, b| b.count.cmp(&a.count).then(a.operation.cmp(&b.operation)));

        let mut top_buckets: Vec<BucketRequestCount> = by_bkt
            .into_iter()
            .map(|(bucket, (count, bytes))| BucketRequestCount {
                bucket,
                count,
                bytes,
            })
            .collect();
        top_buckets.sort_by(|a, b| b.count.cmp(&a.count).then(a.bucket.cmp(&b.bucket)));
        let active_buckets = top_buckets.len() as u64;
        top_buckets.truncate(10);

        let mut by_status: Vec<StatusCount> = by_st
            .into_iter()
            .map(|(status_class, count)| StatusCount {
                status_class,
                count,
            })
            .collect();
        by_status.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then(a.status_class.cmp(&b.status_class))
        });

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
            total_bytes_in: t_in,
            total_bytes_out: t_out,
            latency_avg_ms: t_lat.checked_div(total).unwrap_or(0),
            latency_p95_ms: latency_quantile_ms(&hist, 0.95),
            peak_window_count,
            active_buckets,
            window_secs: window,
        })
    }
}

fn uuid_like(seed: &VersionId) -> String {
    format!("dm-{}", seed.as_str())
}

/// A reconcile oracle backed by snapshot sets of live paths/sessions.
#[derive(Debug, Clone, Default)]
pub struct SetReconcileOracle {
    /// Storage paths a metadata row references.
    pub live_paths: HashSet<String>,
    /// Upload sessions that still exist.
    pub live_uploads: HashSet<String>,
}

#[async_trait::async_trait]
impl ReconcileOracle for SetReconcileOracle {
    async fn live_blobs(&self, candidates: &[StoragePath]) -> Result<Vec<bool>, MetaError> {
        Ok(candidates
            .iter()
            .map(|p| self.live_paths.contains(p.as_str()))
            .collect())
    }

    async fn live_session(&self, upload: &UploadId) -> Result<bool, MetaError> {
        Ok(self.live_uploads.contains(upload.as_str()))
    }
}
