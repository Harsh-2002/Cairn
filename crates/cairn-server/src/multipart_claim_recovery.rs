//! One bounded, retained recovery consumer for admitted storage writes (ARCH 8).
//!
//! Request Drop only cancels child-I/O admission and queues the exact plan. The worker waits for
//! actual userspace and kernel quiescence before resolving durable ownership through the Writer.

use cairn_protocol::{StorageRecoveryAdmission, StorageRecoveryPermit, StorageWriteRecovery};
use cairn_types::meta::{ClaimReleaseOutcome, Mutation, MutationOutcome};
use cairn_types::storage::io::StorageIoWatch;
use cairn_types::storage::{StorageMutation, StorageToken, StorageWriteTarget};
use cairn_types::traits::{BlobStore, Clock, MetadataStore};
use futures_util::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

enum Command {
    Resolve(Box<StorageWriteRecovery>),
    DrainAndStop,
}

/// Synchronous Drop remains nonblocking; a slot is held by the request, recovery record and all
/// actual backend jobs until their last owner stops. The FIFO sentinel follows the HTTP drain.
pub(crate) struct MultipartClaimRecoveryQueue {
    sender: UnboundedSender<Command>,
    receiver: Mutex<Option<UnboundedReceiver<Command>>>,
    stop_sent: AtomicBool,
    failed: Arc<AtomicBool>,
    slots: Arc<Semaphore>,
}

impl MultipartClaimRecoveryQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "storage recovery capacity must be positive");
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
            stop_sent: AtomicBool::new(false),
            failed: Arc::new(AtomicBool::new(false)),
            slots: Arc::new(Semaphore::new(capacity)),
        }
    }

    pub(crate) fn admission_callback(&self) -> StorageRecoveryAdmission {
        let slots = self.slots.clone();
        let sender = self.sender.clone();
        Arc::new(move || {
            let slots = slots.clone();
            let sender = sender.clone();
            Box::pin(async move {
                if sender.is_closed() {
                    return None;
                }
                let permit = slots.acquire_owned().await.ok()?;
                if sender.is_closed() {
                    return None;
                }
                Some(StorageRecoveryPermit::new(permit))
            })
        })
    }

    pub(crate) fn callback(&self) -> Arc<dyn Fn(StorageWriteRecovery) -> bool + Send + Sync> {
        let sender = self.sender.clone();
        let slots = self.slots.clone();
        let failed = self.failed.clone();
        Arc::new(move |record| {
            if sender.send(Command::Resolve(Box::new(record))).is_ok() {
                true
            } else {
                slots.close();
                failed.store(true, Ordering::Release);
                tracing::error!(
                    "storage recovery queue is unavailable; exclusive startup recovery is required"
                );
                false
            }
        })
    }

    pub(crate) fn worker(
        &self,
        meta: Arc<dyn MetadataStore>,
        blob: Arc<dyn BlobStore>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut receiver = self
            .receiver
            .lock()
            .expect("storage recovery receiver mutex poisoned")
            .take()
            .expect("storage recovery worker may be started only once");
        let failed = self.failed.clone();
        async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Resolve(record) => {
                        if let Err(error) = recover_one(&*meta, &*blob, *record).await {
                            failed.store(true, Ordering::Release);
                            tracing::error!(%error, "storage ownership remains unresolved; exclusive startup recovery is required");
                        }
                    }
                    Command::DrainAndStop => return,
                }
            }
        }
    }

    pub(crate) fn finish_requests(&self) {
        if self.stop_sent.swap(true, Ordering::AcqRel) {
            return;
        }
        self.slots.close();
        if self.sender.send(Command::DrainAndStop).is_err() {
            self.failed.store(true, Ordering::Release);
            tracing::error!("storage recovery worker stopped before drain");
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        !self.failed.load(Ordering::Acquire)
    }
}

async fn recover_one(
    meta: &dyn MetadataStore,
    blob: &dyn BlobStore,
    mut record: StorageWriteRecovery,
) -> Result<(), String> {
    let cancellation = meta
        .submit(Mutation::Storage {
            bucket: record.plan.bucket.clone(),
            operation: StorageMutation::Cancel {
                attempt: record.plan.attempt.clone(),
                generation: record.plan.generation.clone(),
            },
        })
        .await;
    // Even a failed metadata cancellation must wait for old actual I/O before its queue slot can
    // disappear or shutdown can report this record drained.
    let proof = record.io.quiescent().await;
    let (_probe, lease) = StorageIoWatch::new(
        record.plan.attempt.clone(),
        record.plan.generation.clone(),
        Arc::new(record.lifetime.clone()),
    );
    blob.confirm_storage_quiescence(&record.plan, lease)
        .await
        .map_err(|error| error.to_string())?;
    match cancellation.map_err(|error| error.to_string())? {
        MutationOutcome::StorageUpdated { .. } => {}
        _ => return Err("unexpected storage cancellation outcome".into()),
    }
    if let StorageWriteTarget::Completion {
        upload_id,
        claim_token,
        ..
    } = &record.plan.target
    {
        match meta
            .submit(Mutation::ReleaseMultipartClaim {
                upload_id: upload_id.clone(),
                claim_token: cairn_types::id::MultipartClaimToken::from_string(claim_token.clone()),
            })
            .await
            .map_err(|error| error.to_string())?
        {
            MutationOutcome::MultipartClaimRelease(
                ClaimReleaseOutcome::Released | ClaimReleaseOutcome::NotOwner,
            ) => {}
            _ => return Err("unexpected storage completion release outcome".into()),
        }
    }
    match meta
        .submit(Mutation::Storage {
            bucket: record.plan.bucket,
            operation: StorageMutation::Resolve { quiescence: proof },
        })
        .await
        .map_err(|error| error.to_string())?
    {
        MutationOutcome::StorageUpdated { .. } => Ok(()),
        _ => Err("unexpected storage resolution outcome".into()),
    }
}

pub(crate) struct StorageCleanupDrain {
    pub claimed: usize,
    pub retired: usize,
}

/// Drain one bounded batch through exact Writer claims. Errors and stale/expired acknowledgements
/// retain debt and quota; a physical ENOENT still passes through the blob's namespace sync barrier.
pub(crate) async fn drain_storage_cleanup(
    meta: &dyn MetadataStore,
    blob: &dyn BlobStore,
    generation: &StorageToken,
    node_lifetime: Arc<dyn Send + Sync>,
    limit: u32,
) -> Result<StorageCleanupDrain, String> {
    let clock = cairn_crypto::SystemClock::new();
    let MutationOutcome::StorageCleanupBatch(batch) = meta
        .submit(Mutation::ClaimStorageCleanup {
            generation: generation.clone(),
            limit,
            now: clock.now(),
            lease_secs: 60,
        })
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("unexpected storage cleanup claim outcome".into());
    };
    let claimed = batch.len();
    // Keep each physical cleanup and its Writer acknowledgement in the same bounded future.
    // Concurrent acknowledgements can group-commit; no detached task or extra executor is needed.
    let mut operations = futures_util::stream::iter(batch)
        .map(|cleanup| {
            let node_lifetime = node_lifetime.clone();
            let clock = &clock;
            async move {
                let (_watch, lease) = StorageIoWatch::new(
                    cleanup.id.clone(), cleanup.generation.clone(), node_lifetime,
                );
                if let Err(error) = blob.cleanup_storage(&cleanup, lease).await {
                    tracing::warn!(%error, cleanup_id = %cleanup.id.as_str(), "storage cleanup remains pending");
                    return Ok::<bool, String>(false);
                }
                match meta.submit(Mutation::Storage {
                    bucket: cleanup.bucket.clone(),
                    operation: StorageMutation::FinishCleanup { cleanup, now: clock.now() },
                }).await.map_err(|error| error.to_string())? {
                    MutationOutcome::StorageUpdated { applied } => Ok(applied),
                    _ => Err("unexpected storage cleanup retirement outcome".into()),
                }
            }
        })
        .buffer_unordered(8);
    let mut retired = 0;
    while let Some(result) = operations.next().await {
        retired += usize::from(result?);
    }
    Ok(StorageCleanupDrain { claimed, retired })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use cairn_meta::{ShardedMetadataStore, shard_for_bucket};
    use cairn_types::blob::{
        BlobCipher, BlobProbe, BlobReadHandle, ByteRange, PartRef, ReadMemoryBound, ReconcileOpts,
        ReconcileReport, StageOptions, StagedBlob, StagedPart,
    };
    use cairn_types::id::{
        BucketName, MultipartClaimToken, ObjectKey, StoragePath, UploadId, UserId, VersionId,
    };
    use cairn_types::meta::{
        ClaimOutcome, MultipartLimits, MultipartSession, MultipartStatus, PartRecord,
    };
    use cairn_types::storage::io::StorageIoLease;
    use cairn_types::storage::{
        StorageAdmission, StorageCleanup, StorageCreationPermit, StorageWritePlan,
    };
    use cairn_types::testing::{InMemoryBlobStore, InMemoryMetadataStore};
    use cairn_types::traits::ReconcileOracle;
    use cairn_types::{
        BlobError, BodyStream, ChecksumSet, CompressionDescriptor, SecretKey32, Timestamp,
    };
    use futures_util::FutureExt;
    use std::time::Duration;

    fn session(upload_id: UploadId, bucket: BucketName, key: &str) -> MultipartSession {
        MultipartSession {
            upload_id,
            bucket,
            key: ObjectKey::parse(key).unwrap(),
            content_type: "application/octet-stream".to_owned(),
            status: MultipartStatus::Active,
            owner_id: UserId("owner".to_owned()),
            initiated_by: UserId("owner".to_owned()),
            intended_acl: None,
            replica_intent: None,
            user_metadata: Vec::new(),
            initial_tags: Vec::new(),
            lock_intent: cairn_types::ExplicitObjectLockIntent::default(),
            sse_requested: false,
            encrypt_parts: false,
            sse_kms_requested: false,
            sse_kms_key_id: None,
            sse_bucket_key_enabled: false,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        }
    }

    fn object_row(
        plan: &cairn_types::storage::StorageWritePlan,
        owner: UserId,
    ) -> cairn_types::ObjectVersionRow {
        let (key, version_id, row_id) = match &plan.target {
            StorageWriteTarget::Object {
                key,
                version_id,
                row_id,
            }
            | StorageWriteTarget::Completion {
                key,
                version_id,
                row_id,
                ..
            } => (key.clone(), version_id.clone(), row_id.clone()),
            _ => panic!("object target required"),
        };
        cairn_types::ObjectVersionRow {
            id: row_id,
            bucket: plan.bucket.clone(),
            key,
            version_id,
            is_latest: true,
            is_delete_marker: false,
            size_logical: 10,
            size_physical: 10,
            etag: cairn_types::ETag::from_string("etag".into()),
            content_type: "application/octet-stream".into(),
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            storage_path: Some(plan.final_path().unwrap().clone()),
            compression: cairn_types::CompressionDescriptor::Uncompressed,
            storage_class: cairn_types::StorageClass::Standard,
            cold_locator: None,
            owner_id: owner,
            user_metadata: Vec::new(),
            acl: None,
            checksums: Vec::new(),
            sse_descriptor: None,
            replication_status: None,
            internal_sha256: Some("00".repeat(32)),
            replicated_at: None,
            created_at: Timestamp(0),
            updated_at: Timestamp(0),
        }
    }

    fn put(row: cairn_types::ObjectVersionRow) -> Mutation {
        Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Default::default(),
            initial_state: Default::default(),
            replication: Vec::new(),
        }
    }

    async fn initialize(meta: &dyn MetadataStore, bucket: &BucketName) -> StorageToken {
        meta.submit(Mutation::CreateBucket(Box::new(cairn_types::Bucket {
            name: bucket.clone(),
            owner_id: UserId("owner".into()),
            created_at: Timestamp(0),
            versioning: cairn_types::VersioningState::Unversioned,
            ownership_mode: cairn_types::OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".into(),
            compression: None,
        })))
        .await
        .unwrap();
        let generation = StorageToken::generate();
        meta.submit(Mutation::BeginStorageGeneration {
            generation: generation.clone(),
        })
        .await
        .unwrap();
        generation
    }

    async fn create_session(meta: &dyn MetadataStore, bucket: &BucketName, key: &str) -> UploadId {
        match meta
            .submit(Mutation::CreateMultipart {
                session: Box::new(session(UploadId::generate(), bucket.clone(), key)),
                limits: MultipartLimits::default(),
            })
            .await
            .unwrap()
        {
            MutationOutcome::MultipartCreated(upload_id) => upload_id,
            outcome => panic!("expected multipart creation, got {outcome:?}"),
        }
    }

    fn object_target(key: &str) -> StorageWriteTarget {
        StorageWriteTarget::Object {
            key: ObjectKey::parse(key).unwrap(),
            version_id: VersionId::null(),
            row_id: StorageToken::generate().as_str().to_owned(),
        }
    }

    fn completion_target(upload_id: &UploadId, key: &str) -> StorageWriteTarget {
        StorageWriteTarget::Completion {
            upload_id: upload_id.clone(),
            claim_token: MultipartClaimToken::generate().as_str().to_owned(),
            key: ObjectKey::parse(key).unwrap(),
            version_id: VersionId::null(),
            row_id: StorageToken::generate().as_str().to_owned(),
        }
    }

    fn part_target(upload_id: &UploadId) -> StorageWriteTarget {
        StorageWriteTarget::Part {
            upload_id: upload_id.clone(),
            part_number: 1,
            reservation_id: StorageToken::generate().as_str().to_owned(),
        }
    }

    // Use the actual Writer receipt to construct every creation permit. No fixture admission can
    // manufacture a path without its durable intent or the queue's bounded lifetime ownership.
    async fn admitted(
        queue: &MultipartClaimRecoveryQueue,
        meta: &dyn MetadataStore,
        blob: &dyn BlobStore,
        bucket: &BucketName,
        generation: &StorageToken,
        target: StorageWriteTarget,
    ) -> (StorageWriteRecovery, StorageCreationPermit) {
        let lifetime = (queue.admission_callback())().await.unwrap();
        let planned = blob
            .plan_write(bucket.clone(), generation.clone(), target)
            .unwrap();
        let plan = planned.plan().clone();
        let (io, lease) = StorageIoWatch::new(
            plan.attempt.clone(),
            generation.clone(),
            Arc::new(lifetime.clone()),
        );
        let operation = match &plan.target {
            StorageWriteTarget::Object { .. } => Mutation::Storage {
                bucket: bucket.clone(),
                operation: StorageMutation::Reserve {
                    plan: Box::new(plan.clone()),
                    now: Timestamp(1),
                },
            },
            StorageWriteTarget::Part {
                upload_id,
                part_number,
                reservation_id,
            } => Mutation::AdmitStorageWrite {
                plan: Box::new(plan.clone()),
                now: Timestamp(1),
                operation: Box::new(Mutation::ReserveMultipartPart {
                    upload_id: upload_id.clone(),
                    part_number: *part_number,
                    attempt_id: reservation_id.clone(),
                    reserved_bytes: 4,
                    max_parts_per_upload: 10_000,
                    now: Timestamp(1),
                }),
            },
            StorageWriteTarget::Completion {
                upload_id,
                claim_token,
                ..
            } => Mutation::AdmitStorageWrite {
                plan: Box::new(plan.clone()),
                now: Timestamp(1),
                operation: Box::new(Mutation::ClaimMultipart {
                    upload_id: upload_id.clone(),
                    claim_token: MultipartClaimToken::from_string(claim_token.clone()),
                }),
            },
        };
        let receipt = match meta.submit(operation).await.unwrap() {
            MutationOutcome::StorageAdmission(receipt) => receipt,
            MutationOutcome::StorageMultipartClaim {
                admission,
                claim: ClaimOutcome::Claimed(_),
            } => admission,
            outcome => panic!("expected successful storage admission, got {outcome:?}"),
        };
        assert!(matches!(receipt, StorageAdmission::Granted(_)));
        let permit = planned.admit(receipt, lease).unwrap();
        (StorageWriteRecovery { plan, io, lifetime }, permit)
    }

    fn body() -> BodyStream {
        Box::pin(futures_util::stream::once(async {
            Ok(Bytes::from_static(b"data"))
        }))
    }

    fn enqueue(queue: &MultipartClaimRecoveryQueue, record: StorageWriteRecovery) {
        record.io.cancel();
        assert!((queue.callback())(record));
    }

    async fn stop(
        queue: &MultipartClaimRecoveryQueue,
        meta: Arc<dyn MetadataStore>,
        blob: Arc<dyn BlobStore>,
    ) {
        queue.finish_requests();
        queue.finish_requests();
        tokio::time::timeout(Duration::from_secs(2), queue.worker(meta, blob))
            .await
            .expect("FIFO recovery drain");
        assert!(queue.is_complete());
    }

    async fn cleanup(
        meta: &dyn MetadataStore,
        blob: &dyn BlobStore,
        generation: &StorageToken,
    ) -> StorageCleanupDrain {
        drain_storage_cleanup(meta, blob, generation, Arc::new(()), 100)
            .await
            .unwrap()
    }

    async fn publish_part(
        meta: &dyn MetadataStore,
        record: &StorageWriteRecovery,
        staged: &StagedPart,
    ) {
        let StorageWriteTarget::Part {
            upload_id,
            part_number,
            reservation_id,
        } = &record.plan.target
        else {
            panic!("part target")
        };
        meta.submit(Mutation::PublishStorageWrite {
            plan: Box::new(record.plan.clone()),
            operation: Box::new(Mutation::RecordPart {
                upload_id: upload_id.clone(),
                attempt_id: reservation_id.clone(),
                part: PartRecord {
                    part_number: *part_number,
                    size: staged.size,
                    etag: staged.md5_hex.clone(),
                    storage_path: staged.storage_path.clone(),
                    checksum: None,
                    part_dek: None,
                },
            }),
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stop_sentinel_drains_every_admitted_completion_and_object_before_worker_exit() {
        let meta = Arc::new(InMemoryMetadataStore::new());
        let blob = Arc::new(InMemoryBlobStore::new());
        let bucket = BucketName::parse("recovery-bucket").unwrap();
        let generation = initialize(&*meta, &bucket).await;
        let queue = MultipartClaimRecoveryQueue::new(3);
        let first = create_session(&*meta, &bucket, "first").await;
        let second = create_session(&*meta, &bucket, "second").await;
        for (upload, key) in [(&first, "first"), (&second, "second")] {
            let (record, permit) = admitted(
                &queue,
                &*meta,
                &*blob,
                &bucket,
                &generation,
                completion_target(upload, key),
            )
            .await;
            blob.assemble(permit, &[], StageOptions::default())
                .await
                .unwrap();
            enqueue(&queue, record);
        }
        let (record, permit) = admitted(
            &queue,
            &*meta,
            &*blob,
            &bucket,
            &generation,
            object_target("put"),
        )
        .await;
        blob.stage(permit, body(), StageOptions::default())
            .await
            .unwrap();
        enqueue(&queue, record);
        stop(&queue, meta.clone(), blob.clone()).await;
        for upload in [first, second] {
            assert_eq!(
                meta.get_multipart(&upload).await.unwrap().unwrap().status,
                MultipartStatus::Active
            );
        }
        assert_eq!(
            blob.blob_count(),
            3,
            "resolution records debt before physical reclamation"
        );
        let drained = cleanup(&*meta, &*blob, &generation).await;
        assert!(drained.claimed >= 3);
        assert_eq!(drained.claimed, drained.retired);
        assert_eq!(blob.blob_count(), 0);
        assert_eq!(cleanup(&*meta, &*blob, &generation).await.claimed, 0);
    }

    #[tokio::test]
    async fn acknowledged_or_ack_lost_object_publication_preserves_referenced_path() {
        for lose_ack in [false, true] {
            let meta = Arc::new(InMemoryMetadataStore::new());
            let blob = Arc::new(InMemoryBlobStore::new());
            let bucket = BucketName::parse("committed-recovery").unwrap();
            let generation = initialize(&*meta, &bucket).await;
            let queue = MultipartClaimRecoveryQueue::new(1);
            let (record, permit) = admitted(
                &queue,
                &*meta,
                &*blob,
                &bucket,
                &generation,
                object_target("put"),
            )
            .await;
            let staged = blob
                .stage(permit, body(), StageOptions::default())
                .await
                .unwrap();
            let mut row = object_row(&record.plan, UserId("owner".into()));
            row.size_logical = staged.size_logical;
            row.size_physical = staged.size_physical;
            row.etag = staged.etag;
            row.internal_sha256 = Some(staged.internal_sha256);
            if lose_ack {
                meta.fail_next_object_put_ack();
            }
            let outcome = meta
                .submit(Mutation::PublishStorageWrite {
                    plan: Box::new(record.plan.clone()),
                    operation: Box::new(put(row)),
                })
                .await;
            assert_eq!(outcome.is_err(), lose_ack);
            enqueue(&queue, record);
            stop(&queue, meta.clone(), blob.clone()).await;
            cleanup(&*meta, &*blob, &generation).await;
            assert_eq!(blob.get_bytes(&staged.storage_path), Some(b"data".to_vec()));
            assert_eq!(
                meta.current_version(&bucket, &ObjectKey::parse("put").unwrap())
                    .await
                    .unwrap()
                    .unwrap()
                    .storage_path,
                Some(staged.storage_path)
            );
        }
    }

    #[tokio::test]
    async fn bounded_admission_and_sentinel_wait_for_actual_io_lease_drain() {
        let meta = Arc::new(InMemoryMetadataStore::new());
        let blob = Arc::new(InMemoryBlobStore::new());
        let bucket = BucketName::parse("bounded-recovery").unwrap();
        let generation = initialize(&*meta, &bucket).await;
        let queue = MultipartClaimRecoveryQueue::new(1);
        let (record, permit) = admitted(
            &queue,
            &*meta,
            &*blob,
            &bucket,
            &generation,
            object_target("put"),
        )
        .await;
        let (_, lease) = permit.into_parts();
        let actual_io = lease.try_child().unwrap();
        drop(lease);
        enqueue(&queue, record);
        let admission = queue.admission_callback();
        assert!(admission().now_or_never().is_none());
        let mut worker = Box::pin(queue.worker(meta.clone(), blob.clone()));
        assert!(worker.as_mut().now_or_never().is_none());
        assert!(
            actual_io.try_child().is_err(),
            "request cancellation fences new child I/O"
        );
        assert_eq!(
            cleanup(&*meta, &*blob, &generation).await.claimed,
            0,
            "unfinished I/O cannot become cleanup debt"
        );
        assert!(admission().now_or_never().is_none());
        queue.finish_requests();
        assert!(
            admission().await.is_none(),
            "shutdown closes request admission"
        );
        assert!(
            worker.as_mut().now_or_never().is_none(),
            "sentinel cannot pass outstanding I/O"
        );
        drop(actual_io);
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .unwrap();
        assert!(queue.is_complete());
        assert!(cleanup(&*meta, &*blob, &generation).await.retired > 0);
    }

    #[tokio::test]
    async fn part_resolution_retains_quota_debt_until_exact_physical_cleanup() {
        let meta = Arc::new(InMemoryMetadataStore::new());
        let blob = Arc::new(InMemoryBlobStore::new());
        let bucket = BucketName::parse("part-recovery").unwrap();
        let generation = initialize(&*meta, &bucket).await;
        meta.submit(Mutation::SetBucketQuota {
            bucket: bucket.clone(),
            quota_bytes: Some(4),
        })
        .await
        .unwrap();
        let upload = create_session(&*meta, &bucket, "part").await;
        let queue = MultipartClaimRecoveryQueue::new(1);
        let (record, permit) = admitted(
            &queue,
            &*meta,
            &*blob,
            &bucket,
            &generation,
            part_target(&upload),
        )
        .await;
        blob.stage_part(permit, body(), ChecksumSet::default(), 4, None)
            .await
            .unwrap();
        enqueue(&queue, record);
        stop(&queue, meta.clone(), blob.clone()).await;
        assert_eq!(blob.multipart_part_count(), 1);
        assert!(
            meta.enumerate_stale_multipart_reservations(Timestamp(3), 10)
                .await
                .unwrap()
                .is_empty()
        );
        let retry = blob
            .plan_write(bucket.clone(), generation.clone(), part_target(&upload))
            .unwrap();
        let StorageWriteTarget::Part {
            part_number,
            reservation_id,
            ..
        } = &retry.plan().target
        else {
            panic!("part target")
        };
        let reserve_retry = Mutation::AdmitStorageWrite {
            plan: Box::new(retry.plan().clone()),
            now: Timestamp(3),
            operation: Box::new(Mutation::ReserveMultipartPart {
                upload_id: upload.clone(),
                part_number: *part_number,
                attempt_id: reservation_id.clone(),
                reserved_bytes: 4,
                max_parts_per_upload: 10_000,
                now: Timestamp(3),
            }),
        };
        assert!(
            matches!(
                meta.submit(reserve_retry.clone()).await,
                Err(cairn_types::MetaError::QuotaExceeded)
            ),
            "unremoved part bytes remain charged after reservation resolution"
        );
        let drained = cleanup(&*meta, &*blob, &generation).await;
        assert_eq!(drained.claimed, drained.retired);
        assert_eq!(blob.multipart_part_count(), 0);
        assert!(
            matches!(
                meta.submit(reserve_retry).await.unwrap(),
                MutationOutcome::StorageAdmission(StorageAdmission::Granted(_))
            ),
            "physical cleanup and exact retirement release the quota charge"
        );
    }

    #[tokio::test]
    async fn delayed_old_part_recovery_cannot_delete_a_superseding_retry() {
        let meta = Arc::new(InMemoryMetadataStore::new());
        let blob = Arc::new(InMemoryBlobStore::new());
        let bucket = BucketName::parse("part-retry").unwrap();
        let generation = initialize(&*meta, &bucket).await;
        let upload = create_session(&*meta, &bucket, "part").await;
        let queue = MultipartClaimRecoveryQueue::new(2);
        let mut paths = Vec::new();
        for bytes in [b"old!", b"new!"] {
            let (record, permit) = admitted(
                &queue,
                &*meta,
                &*blob,
                &bucket,
                &generation,
                part_target(&upload),
            )
            .await;
            let staged = blob
                .stage_part(
                    permit,
                    Box::pin(futures_util::stream::once(async move {
                        Ok(Bytes::copy_from_slice(bytes))
                    })),
                    ChecksumSet::default(),
                    4,
                    None,
                )
                .await
                .unwrap();
            publish_part(&*meta, &record, &staged).await;
            paths.push(staged.storage_path);
            enqueue(&queue, record);
        }
        stop(&queue, meta.clone(), blob.clone()).await;
        cleanup(&*meta, &*blob, &generation).await;
        assert_eq!(blob.multipart_part_count(), 1);
        assert_eq!(
            meta.submit(Mutation::ResolveMultipartPartWrite {
                upload_id: upload.clone(),
                part_number: 1,
                storage_path: paths[1].clone()
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartPartWriteResolved { referenced: true }
        );
        let next_queue = MultipartClaimRecoveryQueue::new(1);
        let (record, permit) = admitted(
            &next_queue,
            &*meta,
            &*blob,
            &bucket,
            &generation,
            completion_target(&upload, "part"),
        )
        .await;
        let assembled = blob
            .assemble(
                permit,
                &[PartRef {
                    part_number: 1,
                    storage_path: paths[1].clone(),
                    size: 4,
                    cipher: BlobCipher::KnownPlaintext,
                }],
                StageOptions::default(),
            )
            .await
            .expect("new authoritative part remains physically readable");
        assert_eq!(
            blob.get_bytes(&assembled.storage_path),
            Some(b"new!".to_vec())
        );
        recover_one(&*meta, &*blob, record).await.unwrap();
    }

    #[tokio::test]
    async fn completion_ack_loss_preserves_published_assembly_and_reclaims_only_parts() {
        let meta = Arc::new(InMemoryMetadataStore::new());
        let blob = Arc::new(InMemoryBlobStore::new());
        let bucket = BucketName::parse("completion-ack-loss").unwrap();
        let generation = initialize(&*meta, &bucket).await;
        let upload = create_session(&*meta, &bucket, "complete").await;
        let queue = MultipartClaimRecoveryQueue::new(1);
        let (part, permit) = admitted(
            &queue,
            &*meta,
            &*blob,
            &bucket,
            &generation,
            part_target(&upload),
        )
        .await;
        let staged_part = blob
            .stage_part(permit, body(), ChecksumSet::default(), 4, None)
            .await
            .unwrap();
        publish_part(&*meta, &part, &staged_part).await;
        drop(part);
        let (record, permit) = admitted(
            &queue,
            &*meta,
            &*blob,
            &bucket,
            &generation,
            completion_target(&upload, "complete"),
        )
        .await;
        let staged = blob
            .assemble(
                permit,
                &[PartRef {
                    part_number: 1,
                    storage_path: staged_part.storage_path,
                    size: 4,
                    cipher: BlobCipher::KnownPlaintext,
                }],
                StageOptions::default(),
            )
            .await
            .unwrap();
        let mut row = object_row(&record.plan, UserId("owner".into()));
        row.size_logical = staged.size_logical;
        row.size_physical = staged.size_physical;
        row.etag = staged.etag;
        row.internal_sha256 = Some(staged.internal_sha256);
        let StorageWriteTarget::Completion { claim_token, .. } = &record.plan.target else {
            panic!("completion target")
        };
        meta.fail_next_multipart_complete_ack();
        assert!(
            meta.submit(Mutation::PublishStorageWrite {
                plan: Box::new(record.plan.clone()),
                operation: Box::new(Mutation::CompleteMultipart {
                    upload_id: upload.clone(),
                    claim_token: MultipartClaimToken::from_string(claim_token.clone()),
                    row: Box::new(row),
                    precondition: Default::default(),
                    replication: Vec::new(),
                }),
            })
            .await
            .is_err()
        );
        assert!(
            meta.get_multipart(&upload).await.unwrap().is_none(),
            "completion committed despite its lost acknowledgement"
        );
        enqueue(&queue, record);
        stop(&queue, meta.clone(), blob.clone()).await;
        cleanup(&*meta, &*blob, &generation).await;
        assert_eq!(blob.multipart_part_count(), 0);
        assert_eq!(blob.get_bytes(&staged.storage_path), Some(b"data".to_vec()));
        assert_eq!(
            meta.current_version(&bucket, &ObjectKey::parse("complete").unwrap())
                .await
                .unwrap()
                .unwrap()
                .storage_path,
            Some(staged.storage_path)
        );
    }

    #[tokio::test]
    async fn delayed_old_completion_token_cannot_release_a_new_owner() {
        let meta = Arc::new(InMemoryMetadataStore::new());
        let blob = Arc::new(InMemoryBlobStore::new());
        let bucket = BucketName::parse("completion-retry").unwrap();
        let generation = initialize(&*meta, &bucket).await;
        let upload = create_session(&*meta, &bucket, "complete").await;
        let queue = MultipartClaimRecoveryQueue::new(2);
        let (old, permit) = admitted(
            &queue,
            &*meta,
            &*blob,
            &bucket,
            &generation,
            completion_target(&upload, "complete"),
        )
        .await;
        blob.assemble(permit, &[], StageOptions::default())
            .await
            .unwrap();
        // Retain the old recovery record across a release/retry, modelling a lost release reply.
        recover_one(&*meta, &*blob, old.clone()).await.unwrap();
        let (new, permit) = admitted(
            &queue,
            &*meta,
            &*blob,
            &bucket,
            &generation,
            completion_target(&upload, "complete"),
        )
        .await;
        let staged = blob
            .assemble(permit, &[], StageOptions::default())
            .await
            .unwrap();
        enqueue(&queue, old);
        stop(&queue, meta.clone(), blob.clone()).await;
        cleanup(&*meta, &*blob, &generation).await;
        assert_eq!(
            meta.get_multipart(&upload).await.unwrap().unwrap().status,
            MultipartStatus::Completing
        );
        assert!(
            blob.get_bytes(&staged.storage_path).is_some(),
            "new completer still owns its assembly"
        );
        recover_one(&*meta, &*blob, new).await.unwrap();
        assert_eq!(
            meta.get_multipart(&upload).await.unwrap().unwrap().status,
            MultipartStatus::Active
        );
    }

    #[tokio::test]
    async fn retained_worker_routes_admission_release_and_cleanup_to_nonzero_shard() {
        let inner: Vec<Arc<InMemoryMetadataStore>> = (0..3)
            .map(|_| Arc::new(InMemoryMetadataStore::new()))
            .collect();
        let router = Arc::new(ShardedMetadataStore::new(
            inner
                .iter()
                .cloned()
                .map(|store| store as Arc<dyn MetadataStore>)
                .collect(),
        ));
        let bucket = BucketName::parse("charlie").unwrap();
        assert_eq!(shard_for_bucket(bucket.as_str(), 3), 1);
        let generation = initialize(&*router, &bucket).await;
        let upload = create_session(&*router, &bucket, "complete").await;
        let blob = Arc::new(InMemoryBlobStore::new());
        let queue = MultipartClaimRecoveryQueue::new(2);
        let (part, permit) = admitted(
            &queue,
            &*router,
            &*blob,
            &bucket,
            &generation,
            part_target(&upload),
        )
        .await;
        let staged = blob
            .stage_part(permit, body(), ChecksumSet::default(), 4, None)
            .await
            .unwrap();
        publish_part(&*router, &part, &staged).await;
        enqueue(&queue, part);
        let (record, permit) = admitted(
            &queue,
            &*router,
            &*blob,
            &bucket,
            &generation,
            completion_target(&upload, "complete"),
        )
        .await;
        drop(permit);
        enqueue(&queue, record);
        stop(&queue, router.clone(), blob.clone()).await;
        cleanup(&*router, &*blob, &generation).await;
        assert_eq!(
            inner[1]
                .get_multipart(&upload)
                .await
                .unwrap()
                .unwrap()
                .status,
            MultipartStatus::Active
        );
        assert!(inner[0].get_multipart(&upload).await.unwrap().is_none());
        assert!(inner[2].get_multipart(&upload).await.unwrap().is_none());
        assert_eq!(
            blob.multipart_part_count(),
            1,
            "committed part on its bucket shard survives recovery"
        );
    }

    // Recovery probes can outlive their async caller just like blocking/kernel operations. The
    // double retains the real lease in a detached task, so queue/admission assertions exercise
    // actual lifetime ownership, including a failed or cancelled probe future.
    struct ControlledProbeBlob {
        inner: InMemoryBlobStore,
        entered: tokio::sync::Notify,
        release: Arc<Semaphore>,
        fail_early: bool,
    }

    impl ControlledProbeBlob {
        fn new(fail_early: bool) -> Self {
            Self {
                inner: InMemoryBlobStore::new(),
                entered: tokio::sync::Notify::new(),
                release: Arc::new(Semaphore::new(0)),
                fail_early,
            }
        }
    }

    #[async_trait::async_trait]
    impl BlobStore for ControlledProbeBlob {
        fn read_memory_bound(
            &self,
            compression: &CompressionDescriptor,
            encrypted: bool,
            logical_len: u64,
        ) -> Result<ReadMemoryBound, BlobError> {
            self.inner
                .read_memory_bound(compression, encrypted, logical_len)
        }
        async fn stage(
            &self,
            permit: StorageCreationPermit,
            body: BodyStream,
            opts: StageOptions,
        ) -> Result<StagedBlob, BlobError> {
            self.inner.stage(permit, body, opts).await
        }
        async fn open_raw(
            &self,
            path: &StoragePath,
            range: Option<ByteRange>,
            cipher: BlobCipher,
            compression: &CompressionDescriptor,
            expected_logical_len: u64,
        ) -> Result<BlobReadHandle, BlobError> {
            self.inner
                .open_raw(path, range, cipher, compression, expected_logical_len)
                .await
        }
        async fn probe(&self, path: &StoragePath) -> Result<BlobProbe, BlobError> {
            self.inner.probe(path).await
        }
        async fn confirm_storage_quiescence(
            &self,
            plan: &StorageWritePlan,
            lease: StorageIoLease,
        ) -> Result<(), BlobError> {
            assert!(lease.owns(&plan.attempt, &plan.generation));
            let child = lease.try_child()?;
            let release = self.release.clone();
            let task = tokio::spawn(async move {
                let _actual_io = child;
                let _permit = release.acquire().await.unwrap();
            });
            self.entered.notify_one();
            if self.fail_early {
                return Err(BlobError::Io("quiescence probe failed".into()));
            }
            task.await.unwrap();
            self.inner.confirm_storage_quiescence(plan, lease).await
        }
        async fn cleanup_storage(
            &self,
            cleanup: &StorageCleanup,
            lease: StorageIoLease,
        ) -> Result<(), BlobError> {
            self.inner.cleanup_storage(cleanup, lease).await
        }
        async fn stage_part(
            &self,
            permit: StorageCreationPermit,
            body: BodyStream,
            checksums: ChecksumSet,
            size_ceiling: u64,
            encryption: Option<SecretKey32>,
        ) -> Result<StagedPart, BlobError> {
            self.inner
                .stage_part(permit, body, checksums, size_ceiling, encryption)
                .await
        }
        async fn assemble(
            &self,
            permit: StorageCreationPermit,
            parts: &[PartRef],
            opts: StageOptions,
        ) -> Result<StagedBlob, BlobError> {
            self.inner.assemble(permit, parts, opts).await
        }
        async fn reconcile(
            &self,
            oracle: &dyn ReconcileOracle,
            opts: ReconcileOpts,
            lease: StorageIoLease,
        ) -> Result<ReconcileReport, BlobError> {
            self.inner.reconcile(oracle, opts, lease).await
        }
    }

    #[tokio::test]
    async fn failed_probe_preserves_ambiguous_path_and_retains_capacity_until_actual_probe_stops() {
        let meta = Arc::new(InMemoryMetadataStore::new());
        let blob = Arc::new(ControlledProbeBlob::new(true));
        let bucket = BucketName::parse("failed-probe").unwrap();
        let generation = initialize(&*meta, &bucket).await;
        let queue = MultipartClaimRecoveryQueue::new(1);
        let (record, permit) = admitted(
            &queue,
            &*meta,
            &*blob,
            &bucket,
            &generation,
            object_target("put"),
        )
        .await;
        let staged = blob
            .stage(permit, body(), StageOptions::default())
            .await
            .unwrap();
        enqueue(&queue, record);
        let mut worker = Box::pin(queue.worker(meta.clone(), blob.clone()));
        assert!(worker.as_mut().now_or_never().is_none());
        assert!(
            !queue.is_complete(),
            "failed quiescence must make shutdown incomplete"
        );
        let admission = queue.admission_callback();
        assert!(
            admission().now_or_never().is_none(),
            "failed probe still has actual backend work"
        );
        assert_eq!(cleanup(&*meta, &*blob, &generation).await.claimed, 0);
        assert!(blob.inner.get_bytes(&staged.storage_path).is_some());
        blob.release.add_permits(1);
        let permit = tokio::time::timeout(Duration::from_secs(2), admission())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        queue.finish_requests();
        worker.await;
        assert!(!queue.is_complete());
    }

    #[tokio::test]
    async fn cancelled_recovery_probe_keeps_capacity_charged_until_detached_work_stops() {
        let meta = Arc::new(InMemoryMetadataStore::new());
        let blob = Arc::new(ControlledProbeBlob::new(false));
        let bucket = BucketName::parse("cancelled-probe").unwrap();
        let generation = initialize(&*meta, &bucket).await;
        let queue = MultipartClaimRecoveryQueue::new(1);
        let (record, permit) = admitted(
            &queue,
            &*meta,
            &*blob,
            &bucket,
            &generation,
            object_target("put"),
        )
        .await;
        drop(permit);
        enqueue(&queue, record);
        let worker = tokio::spawn(queue.worker(meta.clone(), blob.clone()));
        blob.entered.notified().await;
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert_eq!(
            queue.slots.available_permits(),
            0,
            "worker cancellation cannot uncharge a detached probe"
        );
        assert_eq!(cleanup(&*meta, &*blob, &generation).await.claimed, 0);
        blob.release.add_permits(1);
        let slot =
            tokio::time::timeout(Duration::from_secs(2), queue.slots.clone().acquire_owned())
                .await
                .unwrap()
                .unwrap();
        drop(slot);
        assert!(
            (queue.admission_callback())().await.is_none(),
            "dead consumer refuses new writes"
        );
        queue.finish_requests();
        assert!(!queue.is_complete());
    }
}
