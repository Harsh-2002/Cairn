//! An in-memory [`MetadataStore`] double backed by `BTreeMap`s. It implements the *semantics*
//! the real SQLite store must provide — atomic conditional check-and-set, last-writer-wins by
//! submission order, versioning/delete-marker bookkeeping, and bounded listing — without the
//! group-commit machinery (mutations apply atomically under one lock).

use crate::authz::PublicAccessBlock;
use crate::bucket::{Bucket, ConfigAspect, ConfigDoc};
use crate::error::MetaError;
use crate::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use crate::meta::{
    ActivityEntry, ClaimOutcome, IfNoneMatch, ListPage, ListQuery, MultipartSession,
    MultipartStatus, Mutation, MutationOutcome, ObjectSummary, OutboxEntry, PartRecord,
    Precondition, ReplicationStatus, StoreCounts, User, UserRecord, UserSigV4Credentials,
    UserWithBearerHash,
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
    config: HashMap<(String, ConfigAspect), ConfigDoc>,
    account_bpa: PublicAccessBlock,
    versions: BTreeMap<VKey, ObjectVersionRow>,
    tags: HashMap<VKey, Vec<(String, String)>>,
    multipart: BTreeMap<String, MultipartSession>,
    parts: BTreeMap<(String, u16), PartRecord>,
    outbox: Vec<OutboxEntry>,
    users: BTreeMap<String, UserRecord>,
    activity: Vec<ActivityEntry>,
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
fn page_rows(rows: Vec<&ObjectVersionRow>, q: &ListQuery) -> ListPage<ObjectSummary> {
    let prefix = q.prefix.clone().unwrap_or_default();
    let after = key_after(q).map(str::to_owned);
    let mut ordered: Vec<&ObjectVersionRow> = rows
        .into_iter()
        .filter(|r| r.key.as_str().starts_with(&prefix))
        .filter(|r| after.as_deref().is_none_or(|a| r.key.as_str() > a))
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

    for r in ordered {
        let count = page.items.len() + page.common_prefixes.len();
        if count >= limit {
            page.truncated = true;
            page.next_cursor = last_key.clone();
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
                    storage_path: None,
                    compression: crate::object::CompressionDescriptor::Uncompressed,
                    storage_class: crate::object::StorageClass::Standard,
                    cold_locator: None,
                    owner_id,
                    user_metadata: Vec::new(),
                    acl: None,
                    checksums: Vec::new(),
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
            Mutation::SetBucketQuota { .. } => {
                // The in-memory double does not model quota enforcement (that lives in the SQLite
                // writer's commit transaction); accept the mutation as a no-op.
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
                        Some(t) => e.next_attempt_at = t,
                        None => e.status = ReplicationStatus::Failed,
                    }
                }
                Ok(MutationOutcome::Ack)
            }
            Mutation::RecordActivity(entry) => {
                st.activity.push(*entry);
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

    async fn is_bucket_empty(&self, name: &BucketName) -> Result<bool, MetaError> {
        let st = self.state.lock().unwrap();
        Ok(!st
            .versions
            .values()
            .any(|r| r.bucket.as_str() == name.as_str() && r.is_latest && !r.is_delete_marker))
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
        Ok(page_rows(rows, query))
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
        Ok(page_rows(rows, query))
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
        let mut items: Vec<MultipartSession> = st
            .multipart
            .values()
            .filter(|s| {
                s.bucket.as_str() == bucket.as_str()
                    && s.status == MultipartStatus::Active
                    && s.key.as_str().starts_with(&prefix)
            })
            .cloned()
            .collect();
        items.sort_by(|a, b| a.key.as_str().cmp(b.key.as_str()));
        let truncated = items.len() > query.limit.max(1) as usize;
        items.truncate(query.limit.max(1) as usize);
        Ok(ListPage {
            items,
            common_prefixes: Vec::new(),
            next_cursor: None,
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

    async fn claim_replication_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        let st = self.state.lock().unwrap();
        Ok(st
            .outbox
            .iter()
            .filter(|e| e.status == ReplicationStatus::Pending && e.next_attempt_at <= now)
            .take(limit as usize)
            .cloned()
            .collect())
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
