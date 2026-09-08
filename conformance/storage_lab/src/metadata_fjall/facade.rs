//! Trait facade: unsupported operations fail closed; only the declared trace is implemented.
use super::{actor::Store, kv, model, reads};
use cairn_types::meta::*;
use cairn_types::*;
#[async_trait::async_trait]
impl MetadataStore for Store {
    async fn storage_baseline_pending(
        &self,
    ) -> Result<cairn_types::storage_baseline::StorageBaselinePending, MetaError> {
        self.read(reads::pending).await
    }
    async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
        self.write(mutation).await
    }
    async fn read_probe(&self) -> Result<(), MetaError> {
        self.read(|view| {
            view.get(&[model::META])?;
            Ok(())
        })
        .await
    }
    async fn get_bucket(&self, name: &BucketName) -> Result<Option<Bucket>, MetaError> {
        let name = name.clone();
        self.read(move |view| kv::get(view, &kv::key(model::BUCKET, &[name.as_str()])))
            .await
    }
    async fn list_buckets(&self, _owner: Option<&UserId>) -> Result<Vec<Bucket>, MetaError> {
        Err(kv::error(
            "list_buckets is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_bucket_config(
        &self,
        _name: &BucketName,
        _aspect: ConfigAspect,
    ) -> Result<Option<ConfigDoc>, MetaError> {
        Err(kv::error(
            "get_bucket_config is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_account_public_access_block(&self) -> Result<PublicAccessBlock, MetaError> {
        Err(kv::error(
            "get_account_public_access_block is outside the isolated Fjall workload contract",
        ))
    }
    async fn is_bucket_empty(&self, _name: &BucketName) -> Result<bool, MetaError> {
        Err(kv::error(
            "is_bucket_empty is outside the isolated Fjall workload contract",
        ))
    }
    async fn current_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        let bucket = bucket.clone();
        let key = key.clone();
        self.read(move |view| model::current(view, &bucket, &key))
            .await
    }
    async fn get_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<Option<ObjectVersionRow>, MetaError> {
        let bucket = bucket.clone();
        let key = key.clone();
        let version = version.clone();
        self.read(move |view| model::version(view, &bucket, &key, &version))
            .await
    }
    async fn list_current(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        let bucket = bucket.clone();
        let query = query.clone();
        self.read(move |view| reads::list(view, &bucket, &query, false))
            .await
    }
    async fn list_versions(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<ObjectSummary>, MetaError> {
        let bucket = bucket.clone();
        let query = query.clone();
        self.read(move |view| reads::list(view, &bucket, &query, true))
            .await
    }
    async fn enumerate_storage_paths(
        &self,
        _bucket: &BucketName,
        _cursor: Option<&str>,
        _batch: u32,
    ) -> Result<ListPage<StoragePath>, MetaError> {
        Err(kv::error(
            "enumerate_storage_paths is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_object_tags(
        &self,
        _bucket: &BucketName,
        _key: &ObjectKey,
        _version: &VersionId,
    ) -> Result<Vec<(String, String)>, MetaError> {
        Err(kv::error(
            "get_object_tags is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_object_lock(
        &self,
        _bucket: &BucketName,
        _key: &ObjectKey,
        _version: &VersionId,
    ) -> Result<cairn_types::object::ObjectLockState, MetaError> {
        Err(kv::error(
            "get_object_lock is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_multipart(
        &self,
        upload: &UploadId,
    ) -> Result<Option<MultipartSession>, MetaError> {
        let upload = upload.clone();
        self.read(move |view| {
            Ok(
                kv::get::<model::Session>(view, &kv::key(model::SESSION, &[upload.as_str()]))?
                    .map(|s| s.0),
            )
        })
        .await
    }
    async fn list_parts(
        &self,
        upload: &UploadId,
        part_number_marker: u16,
        limit: u32,
    ) -> Result<ListPage<PartRecord>, MetaError> {
        let upload = upload.clone();
        self.read(move |view| reads::parts(view, &upload, part_number_marker, limit))
            .await
    }
    async fn list_multipart_uploads(
        &self,
        bucket: &BucketName,
        query: &ListQuery,
    ) -> Result<ListPage<MultipartSession>, MetaError> {
        let bucket = bucket.clone();
        let query = query.clone();
        self.read(move |view| reads::sessions(view, &bucket, &query))
            .await
    }
    async fn enumerate_stale_sessions(
        &self,
        _older_than: Timestamp,
        _batch: u32,
    ) -> Result<Vec<MultipartSession>, MetaError> {
        Err(kv::error(
            "enumerate_stale_sessions is outside the isolated Fjall workload contract",
        ))
    }
    async fn enumerate_stale_multipart_reservations(
        &self,
        _older_than: Timestamp,
        _batch: u32,
    ) -> Result<Vec<MultipartReservation>, MetaError> {
        Err(kv::error(
            "enumerate_stale_multipart_reservations is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_multipart_cleanups(
        &self,
        _limit: u32,
    ) -> Result<Vec<MultipartCleanup>, MetaError> {
        Err(kv::error(
            "list_multipart_cleanups is outside the isolated Fjall workload contract",
        ))
    }
    async fn object_replication_status(
        &self,
        _bucket: &BucketName,
        _key: &ObjectKey,
        _version: &VersionId,
    ) -> Result<Option<ReplicationStatus>, MetaError> {
        Err(kv::error(
            "object_replication_status is outside the isolated Fjall workload contract",
        ))
    }
    async fn has_unreplicated_predecessor(
        &self,
        _bucket: &BucketName,
        _key: &ObjectKey,
        _before: &VersionId,
        _target: Option<&str>,
    ) -> Result<bool, MetaError> {
        Err(kv::error(
            "has_unreplicated_predecessor is outside the isolated Fjall workload contract",
        ))
    }
    async fn claim_replication_batch(
        &self,
        limit: u32,
        now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        match self
            .write(Mutation::ClaimReplicationBatch {
                limit,
                now,
                lease_secs: 300,
            })
            .await?
        {
            MutationOutcome::ReplicationBatch(entries) => Ok(entries),
            _ => Err(MetaError::Integrity),
        }
    }
    async fn list_due_replication(
        &self,
        _limit: u32,
        _now: Timestamp,
    ) -> Result<Vec<OutboxEntry>, MetaError> {
        Err(kv::error(
            "list_due_replication is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_failed_replication(&self, _limit: u32) -> Result<Vec<OutboxEntry>, MetaError> {
        Err(kv::error(
            "list_failed_replication is outside the isolated Fjall workload contract",
        ))
    }
    async fn replication_counts(
        &self,
        bucket: Option<&BucketName>,
    ) -> Result<ReplicationCounts, MetaError> {
        let bucket = bucket.cloned();
        self.read(move |view| reads::replication_counts(view, bucket.as_ref()))
            .await
    }
    async fn claim_webhook_batch(
        &self,
        _limit: u32,
        _now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError> {
        Err(kv::error(
            "claim_webhook_batch is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_due_webhooks(
        &self,
        _limit: u32,
        _now: Timestamp,
    ) -> Result<Vec<WebhookEntry>, MetaError> {
        Err(kv::error(
            "list_due_webhooks is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_failed_webhooks(&self, _limit: u32) -> Result<Vec<WebhookEntry>, MetaError> {
        Err(kv::error(
            "list_failed_webhooks is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_bucket_quota(&self, bucket: &BucketName) -> Result<Option<u64>, MetaError> {
        let bucket = bucket.clone();
        self.read(move |view| {
            Ok(kv::get::<Option<u64>>(view, &kv::key(model::QUOTA, &[bucket.as_str()]))?.flatten())
        })
        .await
    }
    async fn user_by_bearer_key(
        &self,
        _access_key_id: &str,
    ) -> Result<Option<UserWithBearerHash>, MetaError> {
        Err(kv::error(
            "user_by_bearer_key is outside the isolated Fjall workload contract",
        ))
    }
    async fn user_by_sigv4_key(
        &self,
        _access_key_id: &str,
    ) -> Result<Option<UserSigV4Credentials>, MetaError> {
        Err(kv::error(
            "user_by_sigv4_key is outside the isolated Fjall workload contract",
        ))
    }
    async fn user_by_session_key(
        &self,
        _access_key_id: &str,
    ) -> Result<Option<UserSessionCredentials>, MetaError> {
        Err(kv::error(
            "user_by_session_key is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_session_credentials(
        &self,
        _now: Timestamp,
    ) -> Result<Vec<SessionCredentialSummary>, MetaError> {
        Err(kv::error(
            "list_session_credentials is outside the isolated Fjall workload contract",
        ))
    }
    async fn count_users(&self) -> Result<u64, MetaError> {
        Err(kv::error(
            "count_users is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_users(&self) -> Result<Vec<User>, MetaError> {
        Err(kv::error(
            "list_users is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_user_policy(&self, _user_id: &UserId) -> Result<Option<String>, MetaError> {
        Err(kv::error(
            "get_user_policy is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_import_jobs(
        &self,
        _query: &ImportJobListQuery,
    ) -> Result<ImportJobPage, MetaError> {
        Err(kv::error(
            "list_import_jobs is outside the isolated Fjall workload contract",
        ))
    }
    async fn next_import_job_id(&self, _state: ImportState) -> Result<Option<String>, MetaError> {
        Err(kv::error(
            "next_import_job_id is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_import_job(&self, _id: &str) -> Result<Option<ImportJob>, MetaError> {
        Err(kv::error(
            "get_import_job is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_import_job_record(&self, _id: &str) -> Result<Option<ImportJobRecord>, MetaError> {
        Err(kv::error(
            "get_import_job_record is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_share_by_id(&self, _id: &str) -> Result<Option<ShareRow>, MetaError> {
        Err(kv::error(
            "get_share_by_id is outside the isolated Fjall workload contract",
        ))
    }
    async fn get_share_by_token_hash(
        &self,
        _token_hash: &ShareLookupHash,
    ) -> Result<Option<ShareRow>, MetaError> {
        Err(kv::error(
            "get_share_by_token_hash is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_shares(
        &self,
        _bucket: &BucketName,
        _key: Option<&ObjectKey>,
    ) -> Result<Vec<ShareRow>, MetaError> {
        Err(kv::error(
            "list_shares is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_tag_summary(
        &self,
        _bucket: Option<&BucketName>,
    ) -> Result<Vec<TagSummary>, MetaError> {
        Err(kv::error(
            "list_tag_summary is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_objects_by_tag(
        &self,
        _bucket: Option<&BucketName>,
        _tag_key: &str,
        _tag_value: &str,
        _limit: u32,
    ) -> Result<Vec<TaggedObject>, MetaError> {
        Err(kv::error(
            "list_objects_by_tag is outside the isolated Fjall workload contract",
        ))
    }
    async fn list_activity(&self, _limit: u32) -> Result<Vec<ActivityEntry>, MetaError> {
        Err(kv::error(
            "list_activity is outside the isolated Fjall workload contract",
        ))
    }
    async fn aggregate_counts(&self) -> Result<StoreCounts, MetaError> {
        Err(kv::error(
            "aggregate_counts is outside the isolated Fjall workload contract",
        ))
    }
    async fn bucket_counts(&self) -> Result<Vec<BucketCounts>, MetaError> {
        self.read(reads::bucket_counts).await
    }
    async fn query_request_metrics(
        &self,
        _range: MetricsRange,
        _now_secs: i64,
    ) -> Result<RequestMetricsSeries, MetaError> {
        Err(kv::error(
            "query_request_metrics is outside the isolated Fjall workload contract",
        ))
    }
}
