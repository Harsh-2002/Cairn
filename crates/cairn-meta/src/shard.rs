//! [`ShardedMetadataStore`]: partitions the metadata across N independent stores by bucket name
//! (ARCH 30, Phase 3.2), so disjoint buckets commit through N independent single-writers in
//! parallel instead of contending on one. `CAIRN_META_SHARDS` selects N; the default `1` makes
//! this a pure pass-through (every route resolves to the one shard, every fan-out is over one
//! store), so a single-shard deployment is byte-for-byte the unsharded store.
//!
//! ## What shards by bucket and what does not
//! The **per-bucket** tables — `buckets`, `bucket_config`, `bucket_stats`, `object_versions`,
//! `object_tags`, and the per-object `replication_outbox` rows that ride the version write — live
//! on `shard_for_bucket(name)`. The **account-global** tables — `users`, `user_stats`,
//! `account_config`, `activity`, `request_metrics`, and object shares — live on shard 0, so the
//! identity/auth path and the analytics views consult one store. **Multipart** lives on the
//! bucket's shard (so `CompleteMultipart` inserts the version and deletes the session in one
//! transaction on one shard); since the multipart trait methods are keyed only by `upload_id`, the
//! owning shard is encoded into the id at `CreateMultipart` and decoded on every later op.
//!
//! ## Cross-shard operations
//! Reads that span buckets (`list_buckets`, `aggregate_counts`, `bucket_counts`, the global tag
//! browser, the stale-session sweep, the replication peeks) fan out to every shard and merge.
//! Claiming replication work draws from each shard up to the limit. Replication marks are
//! idempotent, so they fan out to every shard and only the owning shard's row changes. User quota,
//! whose total spans buckets on different shards, is necessarily **eventually-consistent** under
//! sharding (it cannot be enforced inside one shard's write transaction) — a documented relaxation
//! versus the single-shard store's exact enforcement.

use crate::SqliteMetadataStore;
use async_trait::async_trait;
use cairn_types::MetaError;
use cairn_types::authz::PublicAccessBlock;
use cairn_types::bucket::{Bucket, ConfigAspect, ConfigDoc};
use cairn_types::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use cairn_types::meta::{
    ActivityEntry, BucketCounts, ListPage, ListQuery, MetricsRange, MultipartSession, Mutation,
    MutationOutcome, ObjectSummary, OutboxEntry, PartRecord, ReplicationCounts, ReplicationStatus,
    ReplicationTargetCounts, RequestMetricsSeries, SessionCredentialSummary, ShareRow, StoreCounts,
    TagSummary, TaggedObject, User, UserSessionCredentials, UserSigV4Credentials,
    UserWithBearerHash, WebhookEntry,
};
use cairn_types::object::ObjectVersionRow;
use cairn_types::time::Timestamp;
use cairn_types::traits::{MetadataStore, ReconcileOracle};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Map a bucket name to its shard index with a stable FNV-1a hash. Stable across processes and
/// releases (NOT `std`'s `DefaultHasher`, whose internals may change), so a bucket always maps to
/// the same shard for the life of the data. Returns 0 for the single-shard (pass-through) case.
#[must_use]
pub fn shard_for_bucket(name: &str, shards: usize) -> usize {
    if shards <= 1 {
        return 0;
    }
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % shards as u64) as usize
}

/// Encode a multipart upload's owning shard into its id (`"{shard}~{id}"`) so the later
/// `upload_id`-keyed operations can route without carrying the bucket. A no-op for the
/// single-shard case, so a pass-through deployment never sees a changed id format.
fn encode_upload_shard(shard: usize, id: &str, shards: usize) -> String {
    if shards <= 1 {
        id.to_owned()
    } else {
        format!("{shard}~{id}")
    }
}

/// Decode the owning shard from an `upload_id` produced by [`encode_upload_shard`]. An id without a
/// numeric `"{n}~"` prefix (single-shard, or created before sharding) maps to shard 0.
fn decode_upload_shard(id: &str, shards: usize) -> usize {
    if shards <= 1 {
        return 0;
    }
    match id.split_once('~') {
        Some((n, _)) => n.parse::<usize>().map(|s| s.min(shards - 1)).unwrap_or(0),
        None => 0,
    }
}

/// A metadata store that routes each operation to one of N backing stores by bucket name. See the
/// module docs for the per-bucket / global / cross-shard partitioning.
pub struct ShardedMetadataStore {
    shards: Vec<Arc<dyn MetadataStore>>,
    /// Round-robin start cursor for the cross-shard replication claim fan-out, so a saturated
    /// low-index shard cannot starve higher shards. `Relaxed`: fairness/liveness only, not a
    /// correctness barrier (per-shard claiming is enforced by the SQL lease).
    replication_claim_cursor: AtomicUsize,
    /// The same, for the webhook claim fan-out. A separate cursor from replication on purpose: a
    /// single shared cursor degenerates when the two callers alternate against an even shard count.
    webhook_claim_cursor: AtomicUsize,
}

impl std::fmt::Debug for ShardedMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedMetadataStore")
            .field("shards", &self.shards.len())
            .finish()
    }
}

impl ShardedMetadataStore {
    /// Build a router over `shards` (at least one). The first element is the global shard.
    ///
    /// # Panics
    /// Panics if `shards` is empty — the caller (the server stack) always provides ≥1.
    #[must_use]
    pub fn new(shards: Vec<Arc<dyn MetadataStore>>) -> Self {
        assert!(
            !shards.is_empty(),
            "a sharded store needs at least one shard"
        );
        Self {
            shards,
            replication_claim_cursor: AtomicUsize::new(0),
            webhook_claim_cursor: AtomicUsize::new(0),
        }
    }

    fn n(&self) -> usize {
        self.shards.len()
    }

    /// The shard index to start the next cross-shard claim fan-out from, advancing `cursor`. Returns
    /// 0 for a single-shard store, so N=1 (pass-through) is byte-for-byte unchanged. Rotating the
    /// start point stops a perpetually-saturated low-index shard from starving higher shards.
    fn rotate_start(&self, cursor: &AtomicUsize) -> usize {
        if self.shards.len() <= 1 {
            return 0;
        }
        cursor.fetch_add(1, Ordering::Relaxed) % self.shards.len()
    }

    /// The shard owning `bucket`.
    fn for_bucket(&self, bucket: &str) -> &Arc<dyn MetadataStore> {
        &self.shards[shard_for_bucket(bucket, self.n())]
    }

    /// The global shard (users, account config, analytics, shares).
    fn global(&self) -> &Arc<dyn MetadataStore> {
        &self.shards[0]
    }

    /// The shard owning a multipart `upload_id` (decoded from its shard prefix).
    fn for_upload(&self, upload: &str) -> &Arc<dyn MetadataStore> {
        &self.shards[decode_upload_shard(upload, self.n())]
    }
}

#[async_trait]
impl MetadataStore for ShardedMetadataStore {
    async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
        match mutation {
            // --- per-bucket object/config mutations: route by the target bucket ---
            Mutation::PutObjectVersion { .. }
            | Mutation::CreateDeleteMarker { .. }
            | Mutation::DeleteVersion { .. }
            | Mutation::CreateBucket(_)
            | Mutation::DeleteBucket(_)
            | Mutation::SetBucketConfig { .. }
            | Mutation::SetVersioning { .. }
            | Mutation::SetOwnership { .. }
            | Mutation::SetBucketQuota { .. }
            | Mutation::SetBucketCompression { .. }
            | Mutation::PutObjectTags { .. }
            | Mutation::DeleteObjectTags { .. }
            | Mutation::SetObjectAcl { .. }
            | Mutation::SetObjectRetention { .. }
            | Mutation::SetObjectLegalHold { .. }
            | Mutation::EnqueueReplication(_) => {
                let bucket = mutation_bucket(&mutation).expect("per-bucket mutation has a bucket");
                self.for_bucket(&bucket).submit(mutation).await
            }

            // --- webhook outbox: one EnqueueWebhooks batch is built for a single object event,
            //     so all its entries share a bucket; route by the first (empty = no-op). ---
            Mutation::EnqueueWebhooks(ref entries) => match entries.first() {
                Some(e) => {
                    let bucket = e.bucket.as_str().to_owned();
                    self.for_bucket(&bucket).submit(mutation).await
                }
                None => Ok(MutationOutcome::Ack),
            },

            // --- multipart: route by the (shard-encoded) upload id; encode it at creation ---
            Mutation::CreateMultipart(mut s) => {
                let shard = shard_for_bucket(s.bucket.as_str(), self.n());
                let encoded = encode_upload_shard(shard, s.upload_id.as_str(), self.n());
                s.upload_id = UploadId::from_string(encoded);
                self.shards[shard]
                    .submit(Mutation::CreateMultipart(s))
                    .await
            }
            Mutation::RecordPart { upload_id, part } => {
                self.for_upload(upload_id.as_str())
                    .submit(Mutation::RecordPart { upload_id, part })
                    .await
            }
            Mutation::ClaimMultipart(upload_id) => {
                self.for_upload(upload_id.as_str())
                    .submit(Mutation::ClaimMultipart(upload_id))
                    .await
            }
            Mutation::CompleteMultipart {
                upload_id,
                row,
                precondition,
                replication,
            } => {
                // The encoded upload shard equals the row's bucket shard (the upload was created for
                // that bucket), so the version insert and the session delete land on one shard.
                self.for_upload(upload_id.as_str())
                    .submit(Mutation::CompleteMultipart {
                        upload_id,
                        row,
                        precondition,
                        replication,
                    })
                    .await
            }
            Mutation::AbortMultipart(upload_id) => {
                self.for_upload(upload_id.as_str())
                    .submit(Mutation::AbortMultipart(upload_id))
                    .await
            }

            // --- replication/webhook marks/retry: idempotent, so fan out; only the owner's row changes ---
            Mutation::MarkReplicationDone(_)
            | Mutation::MarkReplicationFailed { .. }
            | Mutation::RetryFailedReplication { .. }
            | Mutation::PruneReplicationOutbox { .. }
            | Mutation::PruneEventsOutbox { .. }
            | Mutation::DeferReplication { .. }
            | Mutation::RecoverClaimedReplication
            | Mutation::MarkWebhookDone(_)
            | Mutation::MarkWebhookFailed { .. } => {
                let mut outcome = MutationOutcome::Ack;
                for s in &self.shards {
                    outcome = s.submit(mutation.clone()).await?;
                }
                Ok(outcome)
            }
            // A claim submitted directly drains shard 0 only; the worker uses the fan-out method
            // `claim_replication_batch` / `claim_webhook_batch` for the real cross-shard claim.
            Mutation::ClaimReplicationBatch { .. } | Mutation::ClaimWebhookBatch { .. } => {
                self.global().submit(mutation).await
            }

            // --- account-global mutations: shard 0 ---
            Mutation::SetUserPolicy { .. }
            | Mutation::SetUserQuota { .. }
            | Mutation::SetAccountPublicAccessBlock(_)
            | Mutation::CreateUser(_)
            | Mutation::UpdateUser(_)
            | Mutation::DeactivateUser(_)
            | Mutation::DeleteUser(_)
            | Mutation::CreateSessionCredential(_)
            | Mutation::DeleteExpiredSessionCredentials { .. }
            | Mutation::DeleteSessionCredential { .. }
            | Mutation::RecordActivity(_)
            | Mutation::RecordRequestMetrics { .. }
            | Mutation::CreateShare(_)
            | Mutation::RevokeShare { .. } => self.global().submit(mutation).await,
        }
    }

    // --- buckets ---
    async fn get_bucket(&self, name: &BucketName) -> Result<Option<Bucket>, MetaError> {
        self.for_bucket(name.as_str()).get_bucket(name).await
    }

    async fn list_buckets(&self, owner: Option<&UserId>) -> Result<Vec<Bucket>, MetaError> {
        let mut all = Vec::new();
        for s in &self.shards {
            all.extend(s.list_buckets(owner).await?);
        }
        all.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));
        Ok(all)
    }

    async fn get_bucket_config(
        &self,
        name: &BucketName,
        aspect: ConfigAspect,
    ) -> Result<Option<ConfigDoc>, MetaError> {
        self.for_bucket(name.as_str())
            .get_bucket_config(name, aspect)
            .await
    }

    async fn get_account_public_access_block(&self) -> Result<PublicAccessBlock, MetaError> {
        self.global().get_account_public_access_block().await
    }

    async fn is_bucket_empty(&self, name: &BucketName) -> Result<bool, MetaError> {
        self.for_bucket(name.as_str()).is_bucket_empty(name).await
    }

    // --- object versions ---
    async fn current_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        self.for_bucket(bucket.as_str())
            .current_version(bucket, key)
            .await
    }

    async fn get_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        self.for_bucket(bucket.as_str())
            .get_version(bucket, key, version)
            .await
    }

    async fn list_current(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        self.for_bucket(bucket.as_str())
            .list_current(bucket, query)
            .await
    }

    async fn list_versions(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        self.for_bucket(bucket.as_str())
            .list_versions(bucket, query)
            .await
    }

    async fn enumerate_storage_paths(
        &self,
        bucket: &BucketName,
        cursor: Option<&str>,
        batch: u32,
    ) -> Result<ListPage<StoragePath>, MetaError> {
        self.for_bucket(bucket.as_str())
            .enumerate_storage_paths(bucket, cursor, batch)
            .await
    }

    async fn get_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Vec<(String, String)>, MetaError> {
        self.for_bucket(bucket.as_str())
            .get_object_tags(bucket, key, version)
            .await
    }

    async fn get_object_lock(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<cairn_types::object::ObjectLockState, MetaError> {
        self.for_bucket(bucket.as_str())
            .get_object_lock(bucket, key, version)
            .await
    }

    // --- multipart (on the bucket's shard, keyed by encoded upload id) ---
    async fn get_multipart(
        &self,
        upload: &UploadId,
    ) -> Result<Option<MultipartSession>, MetaError> {
        self.for_upload(upload.as_str()).get_multipart(upload).await
    }

    async fn list_parts(
        &self,
        upload: &UploadId,
        part_number_marker: u16,
        limit: u32,
    ) -> Result<ListPage<PartRecord>, MetaError> {
        self.for_upload(upload.as_str())
            .list_parts(upload, part_number_marker, limit)
            .await
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<MultipartSession>, MetaError> {
        self.for_bucket(bucket.as_str())
            .list_multipart_uploads(bucket, query)
            .await
    }

    async fn enumerate_stale_sessions(
        &self,
        older_than: Timestamp,
        batch: u32,
    ) -> Result<Vec<MultipartSession>, MetaError> {
        let mut all = Vec::new();
        for s in &self.shards {
            all.extend(s.enumerate_stale_sessions(older_than, batch).await?);
            if all.len() >= batch as usize {
                break;
            }
        }
        all.truncate(batch as usize);
        Ok(all)
    }

    // --- replication (outbox is per-shard, riding each bucket's version write) ---
    async fn object_replication_status(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ReplicationStatus>, MetaError> {
        self.for_bucket(bucket.as_str())
            .object_replication_status(bucket, key, version)
            .await
    }

    async fn has_unreplicated_predecessor(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        before: &VersionId,
        target: Option<&str>,
    ) -> Result<bool, MetaError> {
        // The outbox is a per-bucket table, so this key's predecessors live in the same shard.
        self.for_bucket(bucket.as_str())
            .has_unreplicated_predecessor(bucket, key, before, target)
            .await
    }

    async fn claim_replication_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        let n = self.shards.len();
        let start = self.rotate_start(&self.replication_claim_cursor);
        let mut claimed = Vec::new();
        for off in 0..n {
            if claimed.len() as u32 >= limit {
                break;
            }
            let remaining = limit - claimed.len() as u32;
            claimed.extend(
                self.shards[(start + off) % n]
                    .claim_replication_batch(remaining, now)
                    .await?,
            );
        }
        Ok(claimed)
    }

    async fn list_due_replication(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        let mut all = Vec::new();
        for s in &self.shards {
            all.extend(s.list_due_replication(limit, now).await?);
        }
        all.truncate(limit as usize);
        Ok(all)
    }

    async fn list_failed_replication(&self, limit: u32) -> Result<Vec<OutboxEntry>, MetaError> {
        let mut all = Vec::new();
        for s in &self.shards {
            all.extend(s.list_failed_replication(limit).await?);
        }
        all.truncate(limit as usize);
        Ok(all)
    }

    async fn replication_counts(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<ReplicationCounts, MetaError> {
        let mut acc = ReplicationCounts::default();
        let mut by_target: std::collections::HashMap<Option<String>, (u64, u64)> =
            std::collections::HashMap::new();
        for s in &self.shards {
            let c = s.replication_counts(bucket).await?;
            acc.pending += c.pending;
            acc.claimed += c.claimed;
            acc.failed += c.failed;
            acc.completed += c.completed;
            // The oldest pending across all shards is the min of the per-shard non-zero values.
            if c.oldest_pending_at_ms != 0
                && (acc.oldest_pending_at_ms == 0
                    || c.oldest_pending_at_ms < acc.oldest_pending_at_ms)
            {
                acc.oldest_pending_at_ms = c.oldest_pending_at_ms;
            }
            for t in c.by_target {
                let e = by_target.entry(t.target_arn).or_default();
                e.0 += t.pending;
                e.1 += t.failed;
            }
        }
        acc.by_target = by_target
            .into_iter()
            .filter(|(_, (p, f))| *p > 0 || *f > 0)
            .map(|(target_arn, (pending, failed))| ReplicationTargetCounts {
                target_arn,
                pending,
                failed,
            })
            .collect();
        Ok(acc)
    }

    async fn claim_webhook_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError> {
        let n = self.shards.len();
        let start = self.rotate_start(&self.webhook_claim_cursor);
        let mut claimed = Vec::new();
        for off in 0..n {
            if claimed.len() as u32 >= limit {
                break;
            }
            let remaining = limit - claimed.len() as u32;
            claimed.extend(
                self.shards[(start + off) % n]
                    .claim_webhook_batch(remaining, now)
                    .await?,
            );
        }
        Ok(claimed)
    }

    async fn list_due_webhooks(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError> {
        let mut all = Vec::new();
        for s in &self.shards {
            all.extend(s.list_due_webhooks(limit, now).await?);
        }
        all.truncate(limit as usize);
        Ok(all)
    }

    async fn list_failed_webhooks(&self, limit: u32) -> Result<Vec<WebhookEntry>, MetaError> {
        let mut all = Vec::new();
        for s in &self.shards {
            all.extend(s.list_failed_webhooks(limit).await?);
        }
        all.truncate(limit as usize);
        Ok(all)
    }

    // --- bucket quota ---
    async fn get_bucket_quota(&self, bucket: &BucketName) -> Result<Option<u64>, MetaError> {
        self.for_bucket(bucket.as_str())
            .get_bucket_quota(bucket)
            .await
    }

    // --- users (global, shard 0) ---
    async fn user_by_bearer_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserWithBearerHash>, MetaError> {
        self.global().user_by_bearer_key(access_key_id).await
    }

    async fn user_by_sigv4_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserSigV4Credentials>, MetaError> {
        self.global().user_by_sigv4_key(access_key_id).await
    }

    async fn user_by_session_key(
        &self,
        access_key_id: &str,
    ) -> Result<Option<UserSessionCredentials>, MetaError> {
        self.global().user_by_session_key(access_key_id).await
    }

    async fn list_session_credentials(
        &self,
        now: Timestamp,
    ) -> Result<Vec<SessionCredentialSummary>, MetaError> {
        self.global().list_session_credentials(now).await
    }

    async fn count_users(&self) -> Result<u64, MetaError> {
        self.global().count_users().await
    }

    async fn list_users(&self) -> Result<Vec<User>, MetaError> {
        self.global().list_users().await
    }

    async fn get_user_policy(&self, user_id: &UserId) -> Result<Option<String>, MetaError> {
        self.global().get_user_policy(user_id).await
    }

    // --- object shares (global, shard 0) ---
    async fn get_share(&self, token: &str) -> Result<Option<ShareRow>, MetaError> {
        self.global().get_share(token).await
    }

    async fn list_shares(
        &self,
        bucket: &BucketName,
        key: Option<&ObjectKey>,
    ) -> Result<Vec<ShareRow>, MetaError> {
        self.global().list_shares(bucket, key).await
    }

    // --- object tag browsing (fan out when unscoped) ---
    async fn list_tag_summary(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<Vec<TagSummary>, MetaError> {
        if let Some(b) = bucket {
            return self.for_bucket(b.as_str()).list_tag_summary(bucket).await;
        }
        // Merge per-shard summaries by (key, value), summing counts, then re-sort by count desc.
        let mut merged: HashMap<(String, String), u64> = HashMap::new();
        for s in &self.shards {
            for t in s.list_tag_summary(None).await? {
                *merged.entry((t.tag_key, t.tag_value)).or_insert(0) += t.object_count;
            }
        }
        let mut out: Vec<TagSummary> = merged
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
                .then_with(|| a.tag_key.cmp(&b.tag_key))
                .then_with(|| a.tag_value.cmp(&b.tag_value))
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
        if let Some(b) = bucket {
            return self
                .for_bucket(b.as_str())
                .list_objects_by_tag(bucket, tag_key, tag_value, limit)
                .await;
        }
        let mut all = Vec::new();
        for s in &self.shards {
            all.extend(
                s.list_objects_by_tag(None, tag_key, tag_value, limit)
                    .await?,
            );
        }
        all.sort_by(|a, b| a.bucket.cmp(&b.bucket).then_with(|| a.key.cmp(&b.key)));
        all.truncate(limit as usize);
        Ok(all)
    }

    // --- audit & aggregates (analytics on shard 0; counts fan out) ---
    async fn list_activity(&self, limit: u32) -> Result<Vec<ActivityEntry>, MetaError> {
        self.global().list_activity(limit).await
    }

    async fn aggregate_counts(&self) -> Result<StoreCounts, MetaError> {
        let mut total = StoreCounts::default();
        for s in &self.shards {
            let c = s.aggregate_counts().await?;
            total.buckets += c.buckets;
            total.objects += c.objects;
            total.versions += c.versions;
            total.logical_bytes += c.logical_bytes;
            total.physical_bytes += c.physical_bytes;
        }
        Ok(total)
    }

    async fn bucket_counts(&self) -> Result<Vec<BucketCounts>, MetaError> {
        let mut all = Vec::new();
        for s in &self.shards {
            all.extend(s.bucket_counts().await?);
        }
        all.sort_by(|a, b| a.bucket.cmp(&b.bucket));
        Ok(all)
    }

    async fn query_request_metrics(
        &self,
        range: MetricsRange,
        now_secs: i64,
    ) -> Result<RequestMetricsSeries, MetaError> {
        self.global().query_request_metrics(range, now_secs).await
    }
}

/// Extract the target bucket name from a per-bucket mutation, for shard routing.
fn mutation_bucket(m: &Mutation) -> Option<String> {
    let b = match m {
        Mutation::PutObjectVersion { row, .. } => row.bucket.as_str(),
        Mutation::CreateDeleteMarker { bucket, .. } => bucket.as_str(),
        Mutation::DeleteVersion { bucket, .. } => bucket.as_str(),
        Mutation::CreateBucket(b) => b.name.as_str(),
        Mutation::DeleteBucket(name) => name.as_str(),
        Mutation::SetBucketConfig { bucket, .. } => bucket.as_str(),
        Mutation::SetVersioning { bucket, .. } => bucket.as_str(),
        Mutation::SetOwnership { bucket, .. } => bucket.as_str(),
        Mutation::SetBucketQuota { bucket, .. } => bucket.as_str(),
        Mutation::SetBucketCompression { bucket, .. } => bucket.as_str(),
        Mutation::PutObjectTags { bucket, .. } => bucket.as_str(),
        Mutation::DeleteObjectTags { bucket, .. } => bucket.as_str(),
        Mutation::SetObjectAcl { bucket, .. } => bucket.as_str(),
        Mutation::SetObjectRetention { bucket, .. } => bucket.as_str(),
        Mutation::SetObjectLegalHold { bucket, .. } => bucket.as_str(),
        Mutation::EnqueueReplication(e) => e.bucket.as_str(),
        // Not per-bucket, so no single target bucket to route by: multipart routes by upload id,
        // webhooks by their first entry, replication/webhook marks & batch claims fan out or hit
        // shard 0, and every account-global table (users, sessions, activity, shares, metrics,
        // account BPA) lands on shard 0. Listed EXPLICITLY (no wildcard) so a new per-bucket
        // Mutation fails to compile here until it is classified — matching `submit`'s exhaustive
        // routing match, which is the other half of the +routing site (see the root CLAUDE.md).
        Mutation::CreateMultipart(_)
        | Mutation::RecordPart { .. }
        | Mutation::ClaimMultipart(_)
        | Mutation::CompleteMultipart { .. }
        | Mutation::AbortMultipart(_)
        | Mutation::SetUserPolicy { .. }
        | Mutation::SetUserQuota { .. }
        | Mutation::SetAccountPublicAccessBlock(_)
        | Mutation::CreateUser(_)
        | Mutation::UpdateUser(_)
        | Mutation::DeactivateUser(_)
        | Mutation::DeleteUser(_)
        | Mutation::CreateSessionCredential(_)
        | Mutation::DeleteExpiredSessionCredentials { .. }
        | Mutation::DeleteSessionCredential { .. }
        | Mutation::ClaimReplicationBatch { .. }
        | Mutation::MarkReplicationDone(_)
        | Mutation::MarkReplicationFailed { .. }
        | Mutation::RetryFailedReplication { .. }
        | Mutation::PruneReplicationOutbox { .. }
        | Mutation::PruneEventsOutbox { .. }
        | Mutation::DeferReplication { .. }
        | Mutation::RecoverClaimedReplication
        | Mutation::EnqueueWebhooks(_)
        | Mutation::ClaimWebhookBatch { .. }
        | Mutation::MarkWebhookDone(_)
        | Mutation::MarkWebhookFailed { .. }
        | Mutation::RecordActivity(_)
        | Mutation::CreateShare(_)
        | Mutation::RevokeShare { .. }
        | Mutation::RecordRequestMetrics { .. } => return None,
    };
    Some(b.to_owned())
}

/// A [`ReconcileOracle`] over N shard oracles: each storage path (`"{bucket}/{id}"`) is checked
/// against the oracle of its owning shard, so reconcile never reports a live blob as orphan because
/// it asked the wrong shard. Multipart sessions live on the bucket's shard but are keyed by the
/// shard-encoded upload id, which the per-shard oracle resolves; a sweep that cannot localize a
/// session falls back to asking every shard.
pub struct ShardedReconcileOracle {
    oracles: Vec<Box<dyn ReconcileOracle + Send + Sync>>,
}

impl std::fmt::Debug for ShardedReconcileOracle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedReconcileOracle")
            .field("shards", &self.oracles.len())
            .finish()
    }
}

impl ShardedReconcileOracle {
    /// Build the oracle from per-shard oracles (same order and count as the store's shards).
    #[must_use]
    pub fn new(oracles: Vec<Box<dyn ReconcileOracle + Send + Sync>>) -> Self {
        assert!(
            !oracles.is_empty(),
            "a sharded oracle needs at least one shard"
        );
        Self { oracles }
    }

    /// The shard index owning `storage_path`, parsed from its `"{bucket}/{id}"` prefix.
    fn shard_for_path(&self, storage_path: &str) -> usize {
        let bucket = storage_path.split('/').next().unwrap_or("");
        shard_for_bucket(bucket, self.oracles.len())
    }
}

#[async_trait]
impl ReconcileOracle for ShardedReconcileOracle {
    async fn live_blobs(&self, candidates: &[StoragePath]) -> Result<Vec<bool>, MetaError> {
        // Group candidates by owning shard, query each shard once, then scatter the answers back
        // into the original order so the caller's path/answer alignment is preserved.
        let n = self.oracles.len();
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, p) in candidates.iter().enumerate() {
            buckets[self.shard_for_path(p.as_str())].push(i);
        }
        let mut out = vec![false; candidates.len()];
        for (shard, idxs) in buckets.into_iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let subset: Vec<StoragePath> = idxs.iter().map(|&i| candidates[i].clone()).collect();
            let answers = self.oracles[shard].live_blobs(&subset).await?;
            for (k, &i) in idxs.iter().enumerate() {
                out[i] = answers[k];
            }
        }
        Ok(out)
    }

    async fn live_session(&self, upload: &UploadId) -> Result<bool, MetaError> {
        let shard = decode_upload_shard(upload.as_str(), self.oracles.len());
        if self.oracles[shard].live_session(upload).await? {
            return Ok(true);
        }
        // Fallback for an id without a decodable shard: ask every shard.
        for (i, o) in self.oracles.iter().enumerate() {
            if i != shard && o.live_session(upload).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Per-shard typed handles (the `sqlite` backend only) so the server can drive one WAL checkpointer
/// and one WAL-size reporter per shard. Empty for the libSQL/Turso backends, which self-manage.
pub type ShardHandles = Vec<Arc<SqliteMetadataStore>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_shard_is_passthrough_routing() {
        // Every route resolves to shard 0 when N=1, and the upload id is unchanged.
        assert_eq!(shard_for_bucket("anything", 1), 0);
        assert_eq!(encode_upload_shard(0, "abc", 1), "abc");
        assert_eq!(decode_upload_shard("abc", 1), 0);
    }

    #[test]
    fn mutation_bucket_classifies_per_bucket_and_global() {
        // L1: a per-bucket mutation must resolve to its target bucket so `submit` routes it (rather
        // than panicking on `.expect`), and an account-global one must be None. The `mutation_bucket`
        // match is now exhaustive (no wildcard), so a new Mutation variant fails to compile until it
        // is classified here — this asserts the classification itself is right for both kinds.
        let per_bucket = Mutation::SetObjectLegalHold {
            bucket: BucketName::parse("charlie").unwrap(),
            key: ObjectKey::parse("k").unwrap(),
            version_id: VersionId::null(),
            on: true,
        };
        assert_eq!(mutation_bucket(&per_bucket).as_deref(), Some("charlie"));
        assert!(mutation_bucket(&Mutation::RecoverClaimedReplication).is_none());
    }

    #[test]
    fn bucket_routing_is_stable_and_spread() {
        // Stable: same bucket → same shard across calls.
        for name in ["alpha", "bravo", "charlie", "delta", "echo"] {
            let a = shard_for_bucket(name, 8);
            let b = shard_for_bucket(name, 8);
            assert_eq!(a, b, "routing must be deterministic for {name}");
            assert!(a < 8, "shard in range");
        }
        // Spread: a handful of names hit more than one shard (not all in shard 0).
        let shards: std::collections::HashSet<usize> = (0..50)
            .map(|i| shard_for_bucket(&format!("bucket-{i}"), 8))
            .collect();
        assert!(shards.len() > 1, "names should distribute across shards");
    }

    #[test]
    fn upload_shard_round_trips() {
        for shards in [2usize, 4, 16] {
            for shard in 0..shards {
                let enc = encode_upload_shard(shard, "deadbeef", shards);
                assert_eq!(
                    decode_upload_shard(&enc, shards),
                    shard,
                    "shard {shard}/{shards}"
                );
            }
        }
        // A legacy unprefixed id decodes to shard 0.
        assert_eq!(decode_upload_shard("no-prefix", 4), 0);
    }
}
