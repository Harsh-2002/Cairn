//! Bounded canonical-Writer workload. Payload names are metadata-only fixtures: no object I/O.
use cairn_meta::SqliteMetadataStore;
use cairn_types::meta::MultipartLimits;
use cairn_types::storage::{
    PlannedStorageWrite, StorageAdmission, StorageMutation, StorageToken, StorageWritePlan,
    StorageWriteTarget, io::StorageIoWatch,
};
use cairn_types::testing::PublicationFixture;
use cairn_types::*;
use futures_util::{StreamExt, stream};
use serde::Serialize;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::time::Instant;

const BYTES: u64 = 128;
const PART_BYTES: u64 = 64;
const NOW: Timestamp = Timestamp(1_800_000_000_000);
const MAX_WORKERS: usize = 128;
const PAGE: u32 = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Family {
    VersionAppend,
    ConditionalAccept,
    ConditionalReject,
    PermanentDelete,
    MarkerInsert,
    MarkerRemove,
    MultipartReserve,
    MultipartPublish,
    MultipartReplace,
    MultipartAbort,
    CurrentRead,
    VersionRead,
    PrefixList,
    DelimiterList,
    VersionList,
    JournalSettle,
    OutboxPublish,
    OutboxClaimSettle,
}
impl Family {
    pub const ALL: [Self; 18] = [
        Self::VersionAppend,
        Self::ConditionalAccept,
        Self::ConditionalReject,
        Self::PermanentDelete,
        Self::MarkerInsert,
        Self::MarkerRemove,
        Self::MultipartReserve,
        Self::MultipartPublish,
        Self::MultipartReplace,
        Self::MultipartAbort,
        Self::CurrentRead,
        Self::VersionRead,
        Self::PrefixList,
        Self::DelimiterList,
        Self::VersionList,
        Self::JournalSettle,
        Self::OutboxPublish,
        Self::OutboxClaimSettle,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VersionAppend => "version_append",
            Self::ConditionalAccept => "conditional_accept",
            Self::ConditionalReject => "conditional_reject",
            Self::PermanentDelete => "permanent_delete",
            Self::MarkerInsert => "marker_insert",
            Self::MarkerRemove => "marker_remove",
            Self::MultipartReserve => "multipart_reserve",
            Self::MultipartPublish => "multipart_publish",
            Self::MultipartReplace => "multipart_replace",
            Self::MultipartAbort => "multipart_abort",
            Self::CurrentRead => "current_read",
            Self::VersionRead => "version_read",
            Self::PrefixList => "prefix_list",
            Self::DelimiterList => "delimiter_list",
            Self::VersionList => "version_list",
            Self::JournalSettle => "journal_settle",
            Self::OutboxPublish => "outbox_publish",
            Self::OutboxClaimSettle => "outbox_claim_settle",
        }
    }
}
#[derive(Debug)]
pub struct Observation {
    pub family: Family,
    pub elapsed_ns: u64,
    pub rejected: bool,
}
#[derive(Debug, Default)]
pub struct OperationResult {
    pub observations: Vec<Observation>,
}
impl OperationResult {
    fn record(&mut self, family: Family, start: Instant, rejected: bool) {
        self.observations.push(Observation {
            family,
            elapsed_ns: start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            rejected,
        });
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ExpectedState {
    pub seed_rows: u64,
    pub current_data_rows: u64,
    pub historical_data_rows: u64,
    pub current_delete_markers: u64,
    pub seed_logical_bytes: u64,
    pub auxiliary_sessions: u64,
    pub auxiliary_parts: u64,
    pub auxiliary_part_bytes: u64,
}
#[derive(Clone)]
pub struct Fixture {
    store: Arc<SqliteMetadataStore>,
    publication: PublicationFixture,
    buckets: Vec<BucketName>,
    seed_rows: u64,
    seed: u64,
    cleanup_claims_lost: Arc<AtomicU64>,
}
pub struct DetachedFixture {
    publication: PublicationFixture,
    buckets: Vec<BucketName>,
    seed_rows: u64,
    seed: u64,
    cleanup_claims_lost: Arc<AtomicU64>,
}
impl DetachedFixture {
    pub fn attach(self, store: Arc<SqliteMetadataStore>) -> Fixture {
        Fixture {
            store,
            publication: self.publication,
            buckets: self.buckets,
            seed_rows: self.seed_rows,
            seed: self.seed,
            cleanup_claims_lost: self.cleanup_claims_lost,
        }
    }
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn require(ok: bool, message: &str) -> Result<(), String> {
    if ok { Ok(()) } else { Err(message.to_owned()) }
}
fn before(deadline: Instant) -> Result<(), String> {
    require(
        Instant::now() < deadline,
        "metadata workload deadline reached before admission",
    )
}
fn owner() -> UserId {
    UserId("metadata-capacity-owner".to_owned())
}
fn seed_key(index: u64) -> ObjectKey {
    ObjectKey::parse(&format!(
        "seed/p{:02}/d{:02}/object-{index:08}",
        index % 32,
        (index / 32) % 16
    ))
    .expect("bounded seed key")
}
fn seed_version(index: u64, revision: u64) -> VersionId {
    VersionId::from_string(format!("seed-{index:08}-{revision}"))
}
fn session_id(seed: u64, index: usize, auxiliary: bool) -> UploadId {
    UploadId::from_string(format!(
        "{seed:016x}{:016x}",
        index as u64 + if auxiliary { 1_000 } else { 1 }
    ))
}
fn row(bucket: &BucketName, key: ObjectKey, version_id: VersionId) -> ObjectVersionRow {
    ObjectVersionRow {
        id: StorageToken::generate().as_str().to_owned(),
        bucket: bucket.clone(),
        key,
        version_id,
        is_latest: true,
        is_delete_marker: false,
        size_logical: BYTES,
        size_physical: BYTES,
        etag: ETag::from_string("capacity-etag".to_owned()),
        content_type: "application/octet-stream".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(StoragePath::generate(bucket)),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: owner(),
        user_metadata: vec![("fixture".to_owned(), "metadata-only".to_owned())],
        acl: None,
        checksums: vec![],
        sse_descriptor: None,
        internal_sha256: None,
        replication_status: None,
        replicated_at: None,
        created_at: NOW,
        updated_at: NOW,
    }
}

/// Close fresh-key admission on the first failure, while retaining every started owner's
/// future until it settles. The flag is set inside the owner, before refilling the stream.
async fn seed_owners<F, Fut>(keys: u64, work: F) -> Result<(), String>
where
    F: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let failed = AtomicBool::new(false);
    let results = stream::iter(0..keys)
        .take_while(|_| std::future::ready(!failed.load(Ordering::Acquire)))
        .map(|index| {
            let failed = &failed;
            let work = &work;
            async move {
                // A slot can be buffered before another owner fails but not yet admitted.
                if failed.load(Ordering::Acquire) {
                    return Ok(());
                }
                let result = work(index).await;
                if result.is_err() {
                    failed.store(true, Ordering::Release);
                }
                result
            }
        })
        .buffer_unordered(32);
    tokio::pin!(results);
    let mut first_error = None;
    while let Some(result) = results.next().await {
        if let Err(error) = result {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

pub async fn prepare(
    store: Arc<SqliteMetadataStore>,
    bucket_count: usize,
    seed_rows: u64,
    seed: u64,
    deadline: Instant,
) -> Result<Fixture, String> {
    require(
        matches!(bucket_count, 1 | 16),
        "expected one or sixteen buckets",
    )?;
    require(
        (10..=100_000).contains(&seed_rows) && seed_rows.is_multiple_of(10),
        "seed rows must be a multiple of ten up to 100000",
    )?;
    before(deadline)?;
    let publication = PublicationFixture::new();
    publication.begin(store.as_ref()).await.map_err(err)?;
    let buckets = (0..bucket_count)
        .map(|i| BucketName::parse(&format!("capacity-{i:02}")).map_err(err))
        .collect::<Result<Vec<_>, _>>()?;
    let fixture = Fixture {
        store,
        publication,
        buckets,
        seed_rows,
        seed,
        cleanup_claims_lost: Arc::new(AtomicU64::new(0)),
    };
    for bucket in &fixture.buckets {
        before(deadline)?;
        fixture
            .submit(Mutation::CreateBucket(Box::new(Bucket {
                name: bucket.clone(),
                owner_id: owner(),
                created_at: NOW,
                versioning: VersioningState::Enabled,
                ownership_mode: OwnershipMode::BucketOwnerEnforced,
                region: "us-east-1".to_owned(),
                compression: None,
            })))
            .await?;
        fixture
            .submit(Mutation::SetBucketQuota {
                bucket: bucket.clone(),
                quota_bytes: Some(1_000_000_000),
            })
            .await?;
    }
    // Only 32 in-flight keys; no seed-row inventory survives preparation.
    seed_owners(seed_rows * 9 / 10, |index| {
        let fixture = &fixture;
        async move {
            before(deadline)?;
            let bucket = &fixture.buckets[index as usize % fixture.buckets.len()];
            let key = seed_key(index);
            if index >= seed_rows * 8 / 10 {
                fixture.marker(bucket, &key, seed_version(index, 0)).await?;
            } else {
                let mut observations = OperationResult::default();
                if index < seed_rows / 10 {
                    fixture
                        .put(
                            row(bucket, key.clone(), seed_version(index, 0)),
                            Precondition::default(),
                            false,
                            Family::VersionAppend,
                            deadline,
                            &mut observations,
                        )
                        .await?;
                }
                fixture
                    .put(
                        row(bucket, key, seed_version(index, 1)),
                        Precondition::default(),
                        false,
                        Family::VersionAppend,
                        deadline,
                        &mut observations,
                    )
                    .await?;
            }
            fixture.cleanup_page().await?;
            Ok(())
        }
    })
    .await?;
    for (index, bucket) in fixture.buckets.iter().enumerate() {
        before(deadline)?;
        let upload = session_id(seed, index, true);
        fixture
            .create_session(
                bucket,
                ObjectKey::parse("auxiliary/reopen-part").map_err(err)?,
                upload.clone(),
            )
            .await?;
        fixture
            .part(
                bucket,
                &upload,
                PART_BYTES,
                Family::MultipartPublish,
                deadline,
                &mut OperationResult::default(),
            )
            .await?;
    }
    fixture.drain_cleanup(deadline).await?;
    fixture.verify(deadline).await?;
    Ok(fixture)
}

impl Fixture {
    pub fn detach(self) -> DetachedFixture {
        DetachedFixture {
            publication: self.publication,
            buckets: self.buckets,
            seed_rows: self.seed_rows,
            seed: self.seed,
            cleanup_claims_lost: self.cleanup_claims_lost,
        }
    }
    pub fn cleanup_claims_lost(&self) -> u64 {
        self.cleanup_claims_lost.load(Ordering::Relaxed)
    }
    pub fn expected(&self) -> ExpectedState {
        ExpectedState {
            seed_rows: self.seed_rows,
            current_data_rows: self.seed_rows * 8 / 10,
            historical_data_rows: self.seed_rows / 10,
            current_delete_markers: self.seed_rows / 10,
            seed_logical_bytes: self.seed_rows * 9 / 10 * BYTES,
            auxiliary_sessions: self.buckets.len() as u64,
            auxiliary_parts: self.buckets.len() as u64,
            auxiliary_part_bytes: self.buckets.len() as u64 * PART_BYTES,
        }
    }
    async fn submit(&self, mutation: Mutation) -> Result<MutationOutcome, String> {
        self.store.submit(mutation).await.map_err(err)
    }
    async fn settle(&self, plan: &StorageWritePlan, cancelled: bool) -> Result<(), String> {
        if cancelled {
            self.submit(Mutation::Storage {
                bucket: plan.bucket.clone(),
                operation: StorageMutation::Cancel {
                    attempt: plan.attempt.clone(),
                    generation: plan.generation.clone(),
                },
            })
            .await?;
        }
        // No I/O is scheduled by this executable. Dropping its sole lease is an actual, immediate
        // quiescence barrier for this metadata-only backend; it proves nothing about object files.
        let (mut watch, lease) =
            StorageIoWatch::new(plan.attempt.clone(), plan.generation.clone(), Arc::new(()));
        drop(lease);
        let outcome = self
            .submit(Mutation::Storage {
                bucket: plan.bucket.clone(),
                operation: StorageMutation::Resolve {
                    quiescence: watch.quiescent().await,
                },
            })
            .await?;
        require(
            matches!(outcome, MutationOutcome::StorageUpdated { applied: true }),
            "exact storage resolution lost ownership",
        )
    }
    #[allow(clippy::too_many_arguments)]
    async fn put(
        &self,
        row: ObjectVersionRow,
        precondition: Precondition,
        replicate: bool,
        family: Family,
        deadline: Instant,
        observations: &mut OperationResult,
    ) -> Result<(), String> {
        before(deadline)?;
        let start = Instant::now();
        let plan = self.publication.object_plan(&row).map_err(err)?;
        let admitted = self
            .submit(Mutation::Storage {
                bucket: row.bucket.clone(),
                operation: StorageMutation::Reserve {
                    plan: Box::new(plan.clone()),
                    now: NOW,
                },
            })
            .await?;
        require(
            matches!(admitted, MutationOutcome::StorageAdmission(StorageAdmission::Granted(ref p)) if **p == plan),
            "object admission failed",
        )?;
        let replication = if replicate {
            vec![OutboxEntry {
                claim_token: None,
                id: StorageToken::generate().as_str().to_owned(),
                bucket: row.bucket.clone(),
                key: row.key.clone(),
                version_id: row.version_id.clone(),
                operation: ReplicationOp::ObjectCreate,
                rule_id: "capacity".to_owned(),
                target_arn: None,
                attempts: 0,
                next_attempt_at: NOW,
                status: ReplicationStatus::Pending,
                last_error: None,
                priority: 0,
                lease_until: None,
                enqueued_at: NOW,
            }]
        } else {
            vec![]
        };
        let outcome = self
            .store
            .submit(
                PublicationFixture::publication(
                    plan.clone(),
                    Mutation::PutObjectVersion {
                        row: Box::new(row),
                        precondition,
                        initial_state: InitialObjectState::default(),
                        replication,
                    },
                )
                .map_err(err)?,
            )
            .await;
        let rejected = matches!(outcome, Err(MetaError::PreconditionFailed));
        let published = matches!(outcome, Ok(MutationOutcome::Put { .. }));
        observations.record(family, start, rejected);
        let settle_start = Instant::now();
        if published {
            let StorageWriteTarget::Object {
                key,
                version_id,
                row_id,
            } = &plan.target
            else {
                return Err("wrong object target".to_owned());
            };
            require(
                matches!(
                    self.submit(Mutation::ResolveObjectWrite {
                        bucket: plan.bucket.clone(),
                        key: key.clone(),
                        version_id: version_id.clone(),
                        row_id: row_id.clone(),
                        storage_path: plan.final_path().map_err(err)?.clone()
                    })
                    .await?,
                    MutationOutcome::ObjectWriteResolved { referenced: true }
                ),
                "exact published object not referenced",
            )?;
        } else {
            self.settle(&plan, true).await?;
        }
        observations.record(Family::JournalSettle, settle_start, false);
        if family == Family::ConditionalReject {
            require(rejected, "expected conditional rejection")
        } else {
            require(published, &format!("unexpected put outcome: {outcome:?}"))
        }
    }
    async fn marker(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), String> {
        let outcome = self
            .submit(Mutation::CreateDeleteMarker {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                owner_id: owner(),
                now: NOW,
                bypass: GovernanceBypass::Denied,
                expected_current: None,
                replication: vec![],
            })
            .await?;
        require(
            matches!(outcome, MutationOutcome::DeleteMarker { .. }),
            "marker insertion failed",
        )
    }
    async fn delete(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        row_id: Option<String>,
    ) -> Result<(), String> {
        let outcome = self
            .submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                expected_row_id: row_id,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: NOW,
                bypass: GovernanceBypass::Denied,
            })
            .await?;
        require(
            matches!(outcome, MutationOutcome::Deleted { .. }),
            "permanent deletion failed",
        )
    }
    async fn create_session(
        &self,
        bucket: &BucketName,
        key: ObjectKey,
        upload_id: UploadId,
    ) -> Result<(), String> {
        let outcome = self
            .submit(Mutation::CreateMultipart {
                session: Box::new(MultipartSession {
                    upload_id,
                    bucket: bucket.clone(),
                    key,
                    content_type: "application/octet-stream".to_owned(),
                    status: MultipartStatus::Active,
                    owner_id: owner(),
                    initiated_by: owner(),
                    intended_acl: None,
                    replica_intent: None,
                    user_metadata: vec![],
                    initial_tags: vec![],
                    lock_intent: ExplicitObjectLockIntent::default(),
                    sse_requested: false,
                    encrypt_parts: false,
                    sse_kms_requested: false,
                    sse_kms_key_id: None,
                    sse_bucket_key_enabled: false,
                    created_at: NOW,
                    updated_at: NOW,
                }),
                limits: MultipartLimits {
                    max_active_uploads_per_bucket: 256,
                    max_active_uploads_per_principal: 256,
                    max_parts_per_upload: 2,
                },
            })
            .await?;
        require(
            matches!(outcome, MutationOutcome::MultipartCreated(_)),
            "multipart creation failed",
        )
    }
    async fn part(
        &self,
        bucket: &BucketName,
        upload: &UploadId,
        bytes: u64,
        family: Family,
        deadline: Instant,
        observations: &mut OperationResult,
    ) -> Result<(), String> {
        before(deadline)?;
        let start = Instant::now();
        let attempt = StorageToken::generate().as_str().to_owned();
        let plan = self
            .publication
            .part_plan(bucket, upload, 1, &attempt)
            .map_err(err)?;
        let admitted = self
            .submit(
                PublicationFixture::admission(
                    plan.clone(),
                    Mutation::ReserveMultipartPart {
                        upload_id: upload.clone(),
                        part_number: 1,
                        attempt_id: attempt.clone(),
                        reserved_bytes: bytes,
                        max_parts_per_upload: 2,
                        now: NOW,
                    },
                    NOW,
                )
                .map_err(err)?,
            )
            .await?;
        require(
            matches!(admitted, MutationOutcome::StorageAdmission(StorageAdmission::Granted(ref p)) if **p == plan),
            "part admission failed",
        )?;
        observations.record(Family::MultipartReserve, start, false);
        let start = Instant::now();
        let outcome = self
            .submit(
                PublicationFixture::publication(
                    plan.clone(),
                    Mutation::RecordPart {
                        upload_id: upload.clone(),
                        attempt_id: attempt,
                        part: PartRecord {
                            part_number: 1,
                            size: bytes,
                            etag: "part".to_owned(),
                            storage_path: plan.final_path().map_err(err)?.clone(),
                            checksum: None,
                            part_dek: None,
                        },
                    },
                )
                .map_err(err)?,
            )
            .await;
        let published = matches!(outcome, Ok(MutationOutcome::PartRecorded { .. }));
        observations.record(family, start, false);
        if published {
            require(
                matches!(
                    self.submit(Mutation::ResolveMultipartPartWrite {
                        upload_id: upload.clone(),
                        part_number: 1,
                        storage_path: plan.final_path().map_err(err)?.clone()
                    })
                    .await?,
                    MutationOutcome::MultipartPartWriteResolved { referenced: true }
                ),
                "exact published part not referenced",
            )?;
        } else {
            self.settle(&plan, true).await?;
        }
        require(
            published,
            &format!("unexpected multipart publication: {outcome:?}"),
        )
    }
    async fn cleanup_page(&self) -> Result<usize, String> {
        let MutationOutcome::StorageCleanupBatch(batch) = self
            .submit(Mutation::ClaimStorageCleanup {
                generation: self.publication.generation().clone(),
                limit: PAGE,
                now: NOW,
                lease_secs: 60,
            })
            .await?
        else {
            return Err("unexpected cleanup batch".to_owned());
        };
        let count = batch.len();
        for cleanup in batch {
            let outcome = self
                .submit(Mutation::Storage {
                    bucket: cleanup.bucket.clone(),
                    operation: StorageMutation::FinishCleanup { cleanup, now: NOW },
                })
                .await?;
            match outcome {
                MutationOutcome::StorageUpdated { applied: true } => {}
                MutationOutcome::StorageUpdated { applied: false } => {
                    // Multipart replacement can attach quota debt to this same scratch alias.
                    // The Writer invalidates the old claim; only a fresh claim can retire it.
                    self.cleanup_claims_lost.fetch_add(1, Ordering::Relaxed);
                }
                _ => return Err("unexpected cleanup settlement outcome".to_owned()),
            }
        }
        Ok(count)
    }
    pub async fn drain_cleanup(&self, deadline: Instant) -> Result<(), String> {
        loop {
            before(deadline)?;
            if self.cleanup_page().await? == 0 {
                return Ok(());
            }
        }
    }

    fn operation_location(&self, worker: usize, sequence: u64) -> (&BucketName, ObjectKey) {
        let slot = sequence % 8;
        let mut mixed = self.seed ^ (worker as u64 * 8 + slot).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        let index = (mixed ^ (mixed >> 31)) % (self.seed_rows * 8 / 10);
        let key = ObjectKey::parse(&format!(
            "{}-lab-op-worker{worker:03}-slot{slot:02}",
            seed_key(index)
        ))
        .expect("bounded deterministic operation key");
        (&self.buckets[index as usize % self.buckets.len()], key)
    }
    pub async fn operation(
        &self,
        worker: usize,
        sequence: u64,
        deadline: Instant,
    ) -> Result<OperationResult, String> {
        require(
            worker < MAX_WORKERS,
            "worker index exceeds deterministic ring",
        )?;
        before(deadline)?;
        let (bucket, key) = self.operation_location(worker, sequence);
        let upload = session_id(self.seed, worker, false);
        let mut observations = OperationResult::default();
        let result = self
            .cycle(
                bucket,
                &key,
                &upload,
                worker,
                sequence,
                deadline,
                &mut observations,
            )
            .await;
        // Always finish exact cleanup, including after deadline/error. The caller joins owners;
        // it must not timeout/drop an admitted operation or start another operation on this worker.
        let query = ListQuery {
            prefix: Some(key.as_str().to_owned()),
            limit: 8,
            ..Default::default()
        };
        let page = self
            .store
            .list_versions(bucket, &query)
            .await
            .map_err(err)?;
        require(
            !page.truncated,
            "worker history exceeded bounded operation ring",
        )?;
        for item in page.items {
            let start = Instant::now();
            self.delete(bucket, &item.key, item.version_id, Some(item.row_id))
                .await?;
            observations.record(
                if item.is_delete_marker {
                    Family::MarkerRemove
                } else {
                    Family::PermanentDelete
                },
                start,
                false,
            );
        }
        if self
            .store
            .get_multipart(&upload)
            .await
            .map_err(err)?
            .is_some()
        {
            let start = Instant::now();
            require(
                matches!(
                    self.submit(Mutation::AbortMultipart(upload)).await?,
                    MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::Aborted)
                ),
                "multipart abort failed",
            )?;
            observations.record(Family::MultipartAbort, start, false);
        }
        let start = Instant::now();
        self.cleanup_page().await?;
        observations.record(Family::JournalSettle, start, false);
        result?;
        require(
            observations.observations.len() <= 12,
            "observation bound exceeded",
        )?;
        Ok(observations)
    }
    #[allow(clippy::too_many_arguments)]
    async fn cycle(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload: &UploadId,
        worker: usize,
        sequence: u64,
        deadline: Instant,
        observations: &mut OperationResult,
    ) -> Result<(), String> {
        match sequence % 5 {
            0 => {
                self.put(
                    row(bucket, key.clone(), VersionId::generate()),
                    Precondition::default(),
                    false,
                    Family::VersionAppend,
                    deadline,
                    observations,
                )
                .await?;
                self.put(
                    row(bucket, key.clone(), VersionId::generate()),
                    Precondition {
                        if_match: Some(ETag::from_string("capacity-etag".to_owned())),
                        if_none_match: None,
                    },
                    false,
                    Family::ConditionalAccept,
                    deadline,
                    observations,
                )
                .await?;
                self.put(
                    row(bucket, key.clone(), VersionId::generate()),
                    Precondition {
                        if_match: None,
                        if_none_match: Some(IfNoneMatch::Any),
                    },
                    false,
                    Family::ConditionalReject,
                    deadline,
                    observations,
                )
                .await?;
            }
            1 => {
                self.put(
                    row(bucket, key.clone(), VersionId::generate()),
                    Precondition::default(),
                    false,
                    Family::VersionAppend,
                    deadline,
                    observations,
                )
                .await?;
                before(deadline)?;
                let start = Instant::now();
                let marker = VersionId::generate();
                self.marker(bucket, key, marker.clone()).await?;
                observations.record(Family::MarkerInsert, start, false);
                require(
                    self.store
                        .current_version(bucket, key)
                        .await
                        .map_err(err)?
                        .is_some_and(|r| r.is_delete_marker),
                    "marker did not hide current data",
                )?;
                let start = Instant::now();
                self.delete(bucket, key, marker, None).await?;
                observations.record(Family::MarkerRemove, start, false);
                require(
                    self.store
                        .current_version(bucket, key)
                        .await
                        .map_err(err)?
                        .is_some_and(|r| !r.is_delete_marker && r.size_logical == BYTES),
                    "marker removal did not promote data",
                )?;
            }
            2 => {
                self.create_session(bucket, key.clone(), upload.clone())
                    .await?;
                self.part(
                    bucket,
                    upload,
                    PART_BYTES,
                    Family::MultipartPublish,
                    deadline,
                    observations,
                )
                .await?;
                self.part(
                    bucket,
                    upload,
                    PART_BYTES + 1,
                    Family::MultipartReplace,
                    deadline,
                    observations,
                )
                .await?;
                let parts = self.store.list_parts(upload, 0, 2).await.map_err(err)?;
                require(
                    parts.items.len() == 1 && parts.items[0].size == PART_BYTES + 1,
                    "replacement part mismatch",
                )?;
            }
            3 => {
                self.reads(worker, sequence, observations).await?;
            }
            _ => {
                self.put(
                    row(bucket, key.clone(), VersionId::generate()),
                    Precondition::default(),
                    true,
                    Family::OutboxPublish,
                    deadline,
                    observations,
                )
                .await?;
                let start = Instant::now();
                let batch = self
                    .store
                    .claim_replication_batch(PAGE, NOW)
                    .await
                    .map_err(err)?;
                for entry in batch {
                    require(
                        matches!(
                            self.submit(Mutation::MarkReplicationDone {
                                claim_token: entry
                                    .claim_token
                                    .ok_or("missing replication claim token")?,
                                id: entry.id,
                                now: NOW
                            })
                            .await?,
                            MutationOutcome::ReplicationClaimUpdated { applied: true }
                        ),
                        "replication claim settlement failed",
                    )?;
                }
                self.submit(Mutation::PruneReplicationOutbox {
                    before_ms: NOW.0 + 1,
                })
                .await?;
                observations.record(Family::OutboxClaimSettle, start, false);
                // Exercise cancellation without publication, distinct from failed conditions.
                before(deadline)?;
                let plan = PlannedStorageWrite::new(
                    bucket.clone(),
                    self.publication.generation().clone(),
                    StorageWriteTarget::Object {
                        key: key.clone(),
                        version_id: VersionId::generate(),
                        row_id: StorageToken::generate().as_str().to_owned(),
                    },
                )
                .map_err(err)?
                .plan()
                .clone();
                let admitted = self
                    .submit(Mutation::Storage {
                        bucket: bucket.clone(),
                        operation: StorageMutation::Reserve {
                            plan: Box::new(plan.clone()),
                            now: NOW,
                        },
                    })
                    .await?;
                require(
                    matches!(
                        admitted,
                        MutationOutcome::StorageAdmission(StorageAdmission::Granted(_))
                    ),
                    "cancel fixture admission failed",
                )?;
                let start = Instant::now();
                self.settle(&plan, true).await?;
                observations.record(Family::JournalSettle, start, false);
            }
        }
        Ok(())
    }
    async fn reads(
        &self,
        worker: usize,
        sequence: u64,
        observations: &mut OperationResult,
    ) -> Result<(), String> {
        let index =
            (sequence.wrapping_mul(17) + worker as u64 + self.seed) % (self.seed_rows * 8 / 10);
        let bucket = &self.buckets[index as usize % self.buckets.len()];
        let key = seed_key(index);
        let start = Instant::now();
        let current = self
            .store
            .current_version(bucket, &key)
            .await
            .map_err(err)?
            .ok_or("seed current row absent")?;
        require(
            !current.is_delete_marker && current.version_id == seed_version(index, 1),
            "seed current row changed",
        )?;
        observations.record(Family::CurrentRead, start, false);
        let start = Instant::now();
        require(
            self.store
                .get_version(
                    bucket,
                    &key,
                    &seed_version(index, if index < self.seed_rows / 10 { 0 } else { 1 }),
                )
                .await
                .map_err(err)?
                .is_some(),
            "seed version absent",
        )?;
        observations.record(Family::VersionRead, start, false);
        for (family, delimiter, versions) in [
            (Family::PrefixList, None, false),
            (Family::DelimiterList, Some("/".to_owned()), false),
            (Family::VersionList, None, true),
        ] {
            let start = Instant::now();
            let mut query = ListQuery {
                prefix: Some(format!("seed/p{:02}/", index % 32)),
                limit: if delimiter.is_some() { 1 } else { 16 },
                delimiter,
                ..Default::default()
            };
            for _ in 0..2 {
                let page = if versions {
                    self.store.list_versions(bucket, &query).await
                } else {
                    self.store.list_current(bucket, &query).await
                }
                .map_err(err)?;
                require(
                    page.items.len() + page.common_prefixes.len() <= query.limit as usize,
                    "listing page exceeded bound",
                )?;
                if !page.truncated {
                    break;
                }
                query.cursor = page.next_cursor;
                query.version_id_marker = page.next_version_id_marker;
                require(query.cursor.is_some(), "truncated listing lacks cursor")?;
            }
            observations.record(family, start, false);
        }
        Ok(())
    }
    pub async fn verify(&self, deadline: Instant) -> Result<ExpectedState, String> {
        self.drain_cleanup(deadline).await?;
        let expected = self.expected();
        let (mut current, mut history, mut markers, mut bytes) = (0, 0, 0, 0);
        for bucket in &self.buckets {
            let mut query = ListQuery {
                limit: PAGE,
                ..Default::default()
            };
            loop {
                before(deadline)?;
                let page = self
                    .store
                    .list_versions(bucket, &query)
                    .await
                    .map_err(err)?;
                require(
                    page.items.len() <= PAGE as usize,
                    "verification page exceeded bound",
                )?;
                for item in page.items {
                    let index = item
                        .key
                        .as_str()
                        .rsplit_once("object-")
                        .and_then(|(_, suffix)| suffix.parse::<u64>().ok())
                        .ok_or("operation leaked an authoritative version")?;
                    require(
                        index < self.seed_rows * 9 / 10
                            && item.key == seed_key(index)
                            && bucket == &self.buckets[index as usize % self.buckets.len()]
                            && item.owner_id == owner(),
                        "seed row identity or ownership differs",
                    )?;
                    if item.is_delete_marker {
                        require(
                            index >= self.seed_rows * 8 / 10
                                && item.version_id == seed_version(index, 0)
                                && item.size == 0,
                            "seed marker identity differs",
                        )?;
                    } else {
                        require(
                            index < self.seed_rows * 8 / 10
                                && item.size == BYTES
                                && item.etag == ETag::from_string("capacity-etag".to_owned())
                                && if item.is_latest {
                                    item.version_id == seed_version(index, 1)
                                } else {
                                    index < self.seed_rows / 10
                                        && item.version_id == seed_version(index, 0)
                                },
                            "seed data version identity differs",
                        )?;
                    }
                    if item.is_delete_marker {
                        require(item.is_latest, "unexpected historical marker")?;
                        markers += 1;
                    } else {
                        bytes += item.size;
                        if item.is_latest {
                            current += 1;
                        } else {
                            history += 1;
                        }
                    }
                }
                if !page.truncated {
                    break;
                }
                let next = (page.next_cursor, page.next_version_id_marker);
                require(
                    next.0.is_some()
                        && next != (query.cursor.clone(), query.version_id_marker.clone()),
                    "verification cursor did not advance",
                )?;
                query.cursor = next.0;
                query.version_id_marker = next.1;
            }
        }
        require(
            (current, history, markers, bytes)
                == (
                    expected.current_data_rows,
                    expected.historical_data_rows,
                    expected.current_delete_markers,
                    expected.seed_logical_bytes,
                ),
            "seed current/history/marker/byte totals differ",
        )?;
        let counts = self.store.bucket_counts().await.map_err(err)?;
        require(
            counts.iter().map(|v| v.objects).sum::<u64>() == current
                && counts.iter().map(|v| v.logical_bytes).sum::<u64>() == bytes
                && counts.iter().map(|v| v.physical_bytes).sum::<u64>() == bytes,
            "writer object/quota rollups differ",
        )?;
        let mut sessions = 0;
        for (index, bucket) in self.buckets.iter().enumerate() {
            before(deadline)?;
            let listed = self
                .store
                .list_multipart_uploads(
                    bucket,
                    &ListQuery {
                        limit: PAGE,
                        ..Default::default()
                    },
                )
                .await
                .map_err(err)?;
            require(
                !listed.truncated
                    && listed.items.len() == 1
                    && listed.items[0].upload_id == session_id(self.seed, index, true),
                "auxiliary multipart session mismatch",
            )?;
            let parts = self
                .store
                .list_parts(&listed.items[0].upload_id, 0, PAGE)
                .await
                .map_err(err)?;
            require(
                !parts.truncated && parts.items.len() == 1 && parts.items[0].size == PART_BYTES,
                "auxiliary multipart part mismatch",
            )?;
            sessions += 1;
        }
        require(
            sessions == expected.auxiliary_sessions,
            "auxiliary session total mismatch",
        )?;
        require(
            !self
                .store
                .storage_baseline_pending()
                .await
                .map_err(err)?
                .any(),
            "intent, cleanup or reservation accounting leaked",
        )?;
        let replication = self.store.replication_counts(None).await.map_err(err)?;
        require(
            replication.pending + replication.claimed + replication.failed + replication.completed
                == 0,
            "replication outbox leaked rows",
        )?;
        Ok(expected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn populated_mix_restores_exact_seed_and_parts() {
        for buckets in [1, 16] {
            let store = Arc::new(cairn_meta::open_in_memory().unwrap());
            let deadline = Instant::now() + std::time::Duration::from_secs(60);
            let fixture = prepare(store, buckets, 100, 42, deadline).await.unwrap();
            let mut seen = std::collections::BTreeSet::new();
            let mut locations = std::collections::BTreeSet::new();
            for worker in 0..128 {
                for slot in 0..8 {
                    let (bucket, key) = fixture.operation_location(worker, slot);
                    let (again_bucket, again_key) = fixture.operation_location(worker, slot + 8);
                    assert_eq!((bucket, &key), (again_bucket, &again_key));
                    assert!(locations.insert(key.as_str().to_owned()));
                    let seed = key.as_str().split_once("-lab-op-").unwrap().0;
                    assert!(
                        fixture
                            .store
                            .current_version(bucket, &ObjectKey::parse(seed).unwrap())
                            .await
                            .unwrap()
                            .is_some()
                    );
                }
            }
            assert_eq!(locations.len(), 1024);
            for sequence in 0..10 {
                let result = fixture.operation(0, sequence, deadline).await.unwrap();
                for observation in result.observations {
                    if observation.family == Family::ConditionalReject {
                        assert!(observation.rejected);
                    } else {
                        assert!(!observation.rejected);
                    }
                    seen.insert(observation.family);
                }
            }
            assert_eq!(seen, Family::ALL.into_iter().collect());
            assert_eq!(fixture.verify(deadline).await.unwrap(), fixture.expected());
            assert_eq!(fixture.expected().seed_rows, 100);
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_owners_drain_before_full_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("metadata.db");
        let options = cairn_meta::OpenOptions {
            synchronous_full: true,
            read_pool_size: 8,
            cache_size: -8192,
            mmap_bytes: 0,
            ..Default::default()
        };
        let store = Arc::new(cairn_meta::open(&path, &options).unwrap());
        let deadline = Instant::now() + std::time::Duration::from_secs(60);
        let fixture = prepare(store.clone(), 16, 100, 71, deadline).await.unwrap();
        let results = stream::iter(0..128)
            .map(|worker| {
                let fixture = &fixture;
                async move {
                    for sequence in 0..5 {
                        fixture.operation(worker, sequence, deadline).await?;
                    }
                    Ok::<_, String>(())
                }
            })
            .buffer_unordered(128)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            result.unwrap();
        }
        let expected = fixture.verify(deadline).await.unwrap();
        let detached = fixture.detach();
        let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("all workload owners joined"));
        store.checkpoint_and_close().await.unwrap();
        let reopened = Arc::new(cairn_meta::open(&path, &options).unwrap());
        let fixture = detached.attach(reopened.clone());
        assert_eq!(fixture.verify(deadline).await.unwrap(), expected);
        drop(fixture);
        Arc::try_unwrap(reopened)
            .ok()
            .unwrap()
            .checkpoint_and_close()
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn seed_failure_closes_admission_and_joins_all_started_owners() {
        use futures_util::FutureExt;
        let started = AtomicU64::new(0);
        let settled = AtomicU64::new(0);
        let admitted = tokio::sync::Barrier::new(32);
        let release = tokio::sync::Semaphore::new(0);
        let mut preparation = Box::pin(seed_owners(90_000, |index| {
            let (started, settled, admitted, release) = (&started, &settled, &admitted, &release);
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                admitted.wait().await;
                if index == 0 {
                    settled.fetch_add(1, Ordering::SeqCst);
                    return Err("injected first seed failure".to_owned());
                }
                let _permit = release.acquire().await.unwrap();
                settled.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }));
        for _ in 0..64 {
            assert!(preparation.as_mut().now_or_never().is_none());
            if settled.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(started.load(Ordering::SeqCst), 32);
        assert_eq!(settled.load(Ordering::SeqCst), 1);
        // Failure must not return by dropping the other admitted owners. Release them only
        // after checking the pending state, then demand all acknowledgements before return.
        release.add_permits(31);
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), preparation)
            .await
            .expect("bounded mock owners should settle");
        assert_eq!(result, Err("injected first seed failure".to_owned()));
        assert_eq!(started.load(Ordering::SeqCst), 32);
        assert_eq!(settled.load(Ordering::SeqCst), 32);
    }

    #[tokio::test]
    async fn expired_admission_does_not_mutate_populated_fixture() {
        let store = Arc::new(cairn_meta::open_in_memory().unwrap());
        let deadline = Instant::now() + std::time::Duration::from_secs(30);
        let fixture = prepare(store, 1, 20, 7, deadline).await.unwrap();
        assert!(fixture.operation(0, 0, Instant::now()).await.is_err());
        assert_eq!(fixture.verify(deadline).await.unwrap(), fixture.expected());
    }
}
