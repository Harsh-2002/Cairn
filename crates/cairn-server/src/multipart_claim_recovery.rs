//! Process-local recovery for storage commits dropped by request cancellation.
//!
//! `cairn-protocol` deliberately has no Tokio dependency in its production graph. It therefore
//! owns only a synchronous callback in its request-local drop guard; this module turns that
//! callback into a Tokio queue consumed by one retained server worker.

use cairn_protocol::{
    MultipartClaimRecovery, MultipartPartWriteRecovery, ObjectWriteRecovery,
    StorageRecoveryAdmission, StorageRecoveryPermit,
};
use cairn_types::id::{StoragePath, UploadId};
use cairn_types::meta::{ClaimReleaseOutcome, Mutation, MutationOutcome};
use cairn_types::traits::{BlobStore, MetadataStore};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

enum Command {
    Release(MultipartClaimRecovery),
    ResolveObjectWrite(ObjectWriteRecovery),
    ResolveMultipartPartWrite(MultipartPartWriteRecovery),
    /// Sent only after every accepted HTTP request has finished or been cancelled. FIFO ordering
    /// then makes the worker consume every preceding recovery command before it exits.
    DrainAndStop,
}

/// The synchronous producer plus single-consumer receiver owned by the server stack.
///
/// Sending is non-blocking because it runs from `Drop`, but record cardinality is bounded by
/// `slots`. A request acquires an owned slot before staging/claiming and transfers that lease
/// through its guard into the queued command, so sequential waves cannot outrun the worker and
/// grow memory without bound.
pub(crate) struct MultipartClaimRecoveryQueue {
    sender: UnboundedSender<Command>,
    receiver: Mutex<Option<UnboundedReceiver<Command>>>,
    stop_sent: AtomicBool,
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
            slots: Arc::new(Semaphore::new(capacity)),
        }
    }

    /// Build async admission to one bounded slot held through worker resolution.
    pub(crate) fn admission_callback(&self) -> StorageRecoveryAdmission {
        let slots = self.slots.clone();
        Arc::new(move || {
            let slots = slots.clone();
            Box::pin(async move {
                slots
                    .acquire_owned()
                    .await
                    .ok()
                    .map(StorageRecoveryPermit::new)
            })
        })
    }

    /// Build the runtime-neutral callback injected into `S3Service`.
    pub(crate) fn callback(&self) -> Arc<dyn Fn(MultipartClaimRecovery) -> bool + Send + Sync> {
        let sender = self.sender.clone();
        Arc::new(
            move |recovery| match sender.send(Command::Release(recovery)) {
                Ok(()) => true,
                Err(_) => {
                    // This can happen only after the retained worker has exited unexpectedly or
                    // after the request-drain stop sentinel. Never panic from a request-future
                    // Drop; `false` lets an explicit error path use its direct-writer fallback.
                    tracing::error!(
                        "multipart completion claim recovery queue is unavailable; startup \
                         recovery is required"
                    );
                    false
                }
            },
        )
    }

    /// Build the runtime-neutral ordinary PUT/Copy recovery callback injected into `S3Service`.
    pub(crate) fn object_callback(&self) -> Arc<dyn Fn(ObjectWriteRecovery) -> bool + Send + Sync> {
        let sender = self.sender.clone();
        Arc::new(
            move |recovery| match sender.send(Command::ResolveObjectWrite(recovery)) {
                Ok(()) => true,
                Err(_) => {
                    tracing::error!(
                        "object-write recovery queue is unavailable; startup reconciliation is \
                         required"
                    );
                    false
                }
            },
        )
    }

    /// Build the runtime-neutral multipart-part recovery callback injected into `S3Service`.
    pub(crate) fn part_callback(
        &self,
    ) -> Arc<dyn Fn(MultipartPartWriteRecovery) -> bool + Send + Sync> {
        let sender = self.sender.clone();
        Arc::new(
            move |recovery| match sender.send(Command::ResolveMultipartPartWrite(recovery)) {
                Ok(()) => true,
                Err(_) => {
                    tracing::error!(
                        "multipart-part recovery queue is unavailable; startup reconciliation is \
                         required"
                    );
                    false
                }
            },
        )
    }

    /// Create the one worker future. Taking the receiver twice is a server wiring bug.
    pub(crate) fn worker(
        &self,
        meta: Arc<dyn MetadataStore>,
        blob: Arc<dyn BlobStore>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let receiver = self
            .receiver
            .lock()
            .expect("multipart claim recovery receiver mutex poisoned")
            .take()
            .expect("multipart claim recovery worker may be started only once");
        recovery_loop(meta, blob, receiver)
    }

    /// Tell the worker that all accepted request futures have been joined or cancelled.
    ///
    /// Every cancellation callback has returned before this send, so FIFO queue order makes the
    /// sentinel a consumption barrier. The retained worker is joined by the background supervisor.
    pub(crate) fn finish_requests(&self) {
        if self.stop_sent.swap(true, Ordering::AcqRel) {
            return;
        }
        if self.sender.send(Command::DrainAndStop).is_err() {
            tracing::error!("multipart completion claim recovery worker stopped before drain");
        }
    }
}

async fn recovery_loop(
    meta: Arc<dyn MetadataStore>,
    blob: Arc<dyn BlobStore>,
    mut receiver: UnboundedReceiver<Command>,
) {
    while let Some(command) = receiver.recv().await {
        match command {
            Command::Release(recovery) => {
                recover_one(&*blob, recovery, {
                    let meta = meta.clone();
                    move |mutation| async move { meta.submit(mutation).await }
                })
                .await;
            }
            Command::ResolveObjectWrite(recovery) => {
                recover_object_write(&*blob, recovery, {
                    let meta = meta.clone();
                    move |mutation| async move { meta.submit(mutation).await }
                })
                .await;
            }
            Command::ResolveMultipartPartWrite(recovery) => {
                recover_multipart_part_write(&*blob, recovery, {
                    let meta = meta.clone();
                    move |mutation| {
                        let meta = meta.clone();
                        async move { meta.submit(mutation).await }
                    }
                })
                .await;
            }
            Command::DrainAndStop => return,
        }
    }
}

/// Preserve a committed multipart part, or reclaim its exact attempt artifact and reservation only
/// after the FIFO writer proves that the current part row does not reference it.
async fn recover_multipart_part_write<F, Fut>(
    blob: &dyn BlobStore,
    recovery: MultipartPartWriteRecovery,
    submit: F,
) where
    F: Fn(Mutation) -> Fut,
    Fut: std::future::Future<Output = Result<MutationOutcome, cairn_types::error::MetaError>>,
{
    let upload_id = recovery.upload_id;
    let part_number = recovery.part_number;
    let attempt_id = recovery.attempt_id;
    let storage_path = recovery.storage_path;
    let _permit = recovery.permit;
    match submit(Mutation::ResolveMultipartPartWrite {
        upload_id: upload_id.clone(),
        part_number,
        storage_path: storage_path.clone(),
    })
    .await
    {
        Ok(MutationOutcome::MultipartPartWriteResolved { referenced: true }) => {
            tracing::debug!(%upload_id, part_number, %storage_path, "cancelled multipart part committed");
        }
        Ok(MutationOutcome::MultipartPartWriteResolved { referenced: false }) => {
            if let Err(error) = blob
                .delete_part_attempt(&upload_id, part_number, &attempt_id)
                .await
            {
                tracing::error!(
                    %upload_id,
                    part_number,
                    %storage_path,
                    %error,
                    "uncommitted multipart part could not be reclaimed; startup reconciliation \
                     is required"
                );
                return;
            }
            match submit(Mutation::ReleaseMultipartReservation {
                upload_id: upload_id.clone(),
                attempt_id,
            })
            .await
            {
                Ok(MutationOutcome::Ack) => {}
                Ok(outcome) => tracing::error!(
                    %upload_id,
                    part_number,
                    ?outcome,
                    "unexpected multipart reservation recovery outcome"
                ),
                Err(error) => tracing::error!(
                    %upload_id,
                    part_number,
                    %error,
                    "multipart reservation recovery is ambiguous; startup reconciliation is \
                     required"
                ),
            }
        }
        Ok(outcome) => tracing::error!(
            %upload_id,
            part_number,
            %storage_path,
            ?outcome,
            "unexpected multipart-part recovery outcome; startup reconciliation is required"
        ),
        Err(error) => tracing::error!(
            %upload_id,
            part_number,
            %storage_path,
            %error,
            "multipart-part recovery result is ambiguous; startup reconciliation is required"
        ),
    }
}

/// Reclaim an ordinary PUT/Copy blob only after the serialized writer proves its exact intended
/// row id and storage path are absent. A metadata error is ambiguous and deliberately preserves the
/// file for startup reconciliation.
async fn recover_object_write<F, Fut>(
    blob: &dyn BlobStore,
    recovery: ObjectWriteRecovery,
    submit: F,
) where
    F: FnOnce(Mutation) -> Fut,
    Fut: std::future::Future<Output = Result<MutationOutcome, cairn_types::error::MetaError>>,
{
    let bucket = recovery.bucket;
    let key = recovery.key;
    let version_id = recovery.version_id;
    let row_id = recovery.row_id;
    let storage_path = recovery.storage_path;
    let _permit = recovery.permit;
    match submit(Mutation::ResolveObjectWrite {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id,
        row_id,
        storage_path: storage_path.clone(),
    })
    .await
    {
        Ok(MutationOutcome::ObjectWriteResolved { referenced: true }) => {
            tracing::debug!(%bucket, %key, %storage_path, "cancelled object write committed");
        }
        Ok(MutationOutcome::ObjectWriteResolved { referenced: false }) => {
            if let Err(error) = blob.delete(&storage_path).await {
                tracing::error!(
                    %bucket,
                    %key,
                    %storage_path,
                    %error,
                    "uncommitted object-write blob could not be reclaimed; startup \
                     reconciliation is required"
                );
            }
        }
        Ok(outcome) => {
            tracing::error!(
                %bucket,
                %key,
                %storage_path,
                ?outcome,
                "unexpected object-write recovery outcome; startup reconciliation is required"
            );
        }
        Err(error) => {
            tracing::error!(
                %bucket,
                %key,
                %storage_path,
                %error,
                "object-write recovery result is ambiguous; startup reconciliation is required"
            );
        }
    }
}

/// Apply one conditional release, then reclaim an assembled blob only when that outcome proves the
/// completion transaction did not consume the session.
///
/// A metadata error remains ambiguous, so this worker leaves final resolution to the next startup's
/// global claim recovery and blob reconciliation. The persisted claim token makes a same-token
/// retry ownership-safe, but one retained attempt keeps shutdown bounded.
async fn recover_one<F, Fut>(blob: &dyn BlobStore, recovery: MultipartClaimRecovery, submit: F)
where
    F: FnOnce(Mutation) -> Fut,
    Fut: std::future::Future<Output = Result<MutationOutcome, cairn_types::error::MetaError>>,
{
    let upload_id = recovery.upload_id;
    let claim_token = recovery.claim_token;
    let assembled_blob = recovery.assembled_blob;
    let delete_blob_on_not_owner = recovery.delete_blob_on_not_owner;
    let _permit = recovery.permit;
    match submit(Mutation::ReleaseMultipartClaim {
        upload_id: upload_id.clone(),
        claim_token,
    })
    .await
    {
        Ok(MutationOutcome::MultipartClaimRelease(ClaimReleaseOutcome::Released)) => {
            tracing::debug!(%upload_id, "cancelled multipart completion claim released");
            delete_recovered_blob(blob, &upload_id, assembled_blob).await;
        }
        Ok(MutationOutcome::MultipartClaimRelease(ClaimReleaseOutcome::NotOwner)) => {
            // The completion transaction may have committed and consumed the session. Its object
            // row can now reference `assembled_blob`, so preserve the path unless the request saw a
            // typed non-commit outcome and explicitly supplied stronger cleanup proof.
            if delete_blob_on_not_owner {
                delete_recovered_blob(blob, &upload_id, assembled_blob).await;
            }
        }
        Ok(outcome) => {
            tracing::error!(
                %upload_id,
                ?outcome,
                "unexpected multipart completion claim recovery outcome; startup recovery is \
                 required"
            );
        }
        Err(error) => {
            tracing::error!(
                %upload_id,
                %error,
                "multipart completion claim recovery result is ambiguous; startup recovery is \
                 required"
            );
        }
    }
}

async fn delete_recovered_blob(
    blob: &dyn BlobStore,
    upload_id: &UploadId,
    path: Option<StoragePath>,
) {
    if let Some(path) = path
        && let Err(error) = blob.delete(&path).await
    {
        tracing::error!(
            %upload_id,
            %path,
            %error,
            "released multipart completion left an assembled orphan; startup reconciliation is \
             required"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use cairn_meta::{ShardedMetadataStore, shard_for_bucket};
    use cairn_types::blob::StageOptions;
    use cairn_types::error::MetaError;
    use cairn_types::id::{BucketName, MultipartClaimToken, ObjectKey, UploadId, UserId};
    use cairn_types::meta::{
        ClaimOutcome, MultipartLimits, MultipartSession, MultipartStatus, Mutation, PartRecord,
    };
    use cairn_types::testing::{InMemoryBlobStore, InMemoryMetadataStore};
    use cairn_types::time::Timestamp;
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

    async fn claimed(store: &InMemoryMetadataStore, id: &str) -> (UploadId, MultipartClaimToken) {
        let upload_id = UploadId::from_string(id.to_owned());
        let claim_token = MultipartClaimToken::generate();
        store
            .submit(Mutation::CreateMultipart {
                session: Box::new(session(
                    upload_id.clone(),
                    BucketName::parse("recovery-bucket").unwrap(),
                    id,
                )),
                limits: MultipartLimits::default(),
            })
            .await
            .unwrap();
        assert!(matches!(
            store
                .submit(Mutation::ClaimMultipart {
                    upload_id: upload_id.clone(),
                    claim_token: claim_token.clone(),
                })
                .await
                .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));
        (upload_id, claim_token)
    }

    #[tokio::test]
    async fn stop_sentinel_drains_every_queued_claim_before_worker_exit() {
        let concrete = Arc::new(InMemoryMetadataStore::new());
        let (first, first_token) = claimed(&concrete, "cancelled-one").await;
        let (second, second_token) = claimed(&concrete, "cancelled-two").await;
        let concrete_blob = Arc::new(InMemoryBlobStore::new());
        let staged = concrete_blob
            .stage(
                &BucketName::parse("recovery-bucket").unwrap(),
                Box::pin(futures_util::stream::once(async {
                    Ok(Bytes::from_static(b"uncommitted assembly"))
                })),
                StageOptions::default(),
            )
            .await
            .unwrap();
        let staged_object = concrete_blob
            .stage(
                &BucketName::parse("object-recovery").unwrap(),
                Box::pin(futures_util::stream::once(async {
                    Ok(Bytes::from_static(b"unsubmitted put"))
                })),
                StageOptions::default(),
            )
            .await
            .unwrap();
        let queue = MultipartClaimRecoveryQueue::new(3);
        let callback = queue.callback();
        let object_callback = queue.object_callback();

        // Enqueue real work before the retained worker starts, then put the stop sentinel behind it.
        // FIFO ordering must release both claims and reclaim the first request's assembled orphan
        // before shutdown reports the worker joined.
        assert!(callback(MultipartClaimRecovery {
            upload_id: first.clone(),
            claim_token: first_token,
            assembled_blob: Some(staged.storage_path),
            delete_blob_on_not_owner: false,
            permit: None,
        }));
        assert!(callback(MultipartClaimRecovery {
            upload_id: second.clone(),
            claim_token: second_token,
            assembled_blob: None,
            delete_blob_on_not_owner: false,
            permit: None,
        }));
        assert!(object_callback(ObjectWriteRecovery {
            bucket: BucketName::parse("object-recovery").unwrap(),
            key: ObjectKey::parse("object").unwrap(),
            version_id: cairn_types::VersionId::null(),
            row_id: "unsubmitted-row".to_owned(),
            storage_path: staged_object.storage_path,
            permit: None,
        }));
        queue.finish_requests();
        queue.finish_requests();

        let meta: Arc<dyn MetadataStore> = concrete.clone();
        let blob: Arc<dyn BlobStore> = concrete_blob.clone();
        tokio::time::timeout(Duration::from_secs(1), queue.worker(meta, blob))
            .await
            .expect("worker must drain pending commands through the stop sentinel");

        for upload_id in [first, second] {
            assert_eq!(
                concrete
                    .get_multipart(&upload_id)
                    .await
                    .unwrap()
                    .expect("recovered session remains")
                    .status,
                MultipartStatus::Active
            );
        }
        assert_eq!(
            concrete_blob.blob_count(),
            0,
            "a writer-confirmed release proves the assembled blob is orphaned"
        );
    }

    #[tokio::test]
    async fn object_write_recovery_preserves_referenced_and_ambiguous_paths() {
        let blob = InMemoryBlobStore::new();
        let bucket = BucketName::parse("object-recovery").unwrap();
        let referenced = blob
            .stage(
                &bucket,
                Box::pin(futures_util::stream::once(async {
                    Ok(Bytes::from_static(b"committed"))
                })),
                StageOptions::default(),
            )
            .await
            .unwrap();
        let ambiguous = blob
            .stage(
                &bucket,
                Box::pin(futures_util::stream::once(async {
                    Ok(Bytes::from_static(b"unknown"))
                })),
                StageOptions::default(),
            )
            .await
            .unwrap();
        let recovery = |storage_path| ObjectWriteRecovery {
            bucket: bucket.clone(),
            key: ObjectKey::parse("object").unwrap(),
            version_id: cairn_types::VersionId::null(),
            row_id: "row".to_owned(),
            storage_path,
            permit: None,
        };

        recover_object_write(
            &blob,
            recovery(referenced.storage_path.clone()),
            |_| async { Ok(MutationOutcome::ObjectWriteResolved { referenced: true }) },
        )
        .await;
        recover_object_write(&blob, recovery(ambiguous.storage_path.clone()), |_| async {
            Err(MetaError::Engine("writer unavailable".to_owned()))
        })
        .await;

        assert!(blob.get_bytes(&referenced.storage_path).is_some());
        assert!(blob.get_bytes(&ambiguous.storage_path).is_some());
    }

    #[tokio::test]
    async fn bounded_admission_is_held_until_the_worker_resolves_a_record() {
        let queue = MultipartClaimRecoveryQueue::new(1);
        let admission = queue.admission_callback();
        let permit = admission()
            .await
            .expect("first request acquires the only slot");
        let waiting = tokio::spawn({
            let admission = admission.clone();
            async move { admission().await }
        });
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "a sequential request cannot create another retained record while capacity is full"
        );

        let blob = Arc::new(InMemoryBlobStore::new());
        let staged = blob
            .stage(
                &BucketName::parse("bounded-recovery").unwrap(),
                Box::pin(futures_util::stream::once(async {
                    Ok(Bytes::from_static(b"uncommitted"))
                })),
                StageOptions::default(),
            )
            .await
            .unwrap();
        assert!(queue.object_callback()(ObjectWriteRecovery {
            bucket: BucketName::parse("bounded-recovery").unwrap(),
            key: ObjectKey::parse("object").unwrap(),
            version_id: cairn_types::VersionId::null(),
            row_id: "unsubmitted-row".to_owned(),
            storage_path: staged.storage_path,
            permit: Some(permit),
        }));
        queue.finish_requests();
        let meta: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
        let blob_store: Arc<dyn BlobStore> = blob;
        queue.worker(meta, blob_store).await;

        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .expect("worker completion releases the retained slot")
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn part_worker_reclaims_only_a_proven_unreferenced_attempt_and_reservation() {
        let concrete = Arc::new(InMemoryMetadataStore::new());
        let upload_id = UploadId::from_string("uncommitted-part".to_owned());
        concrete
            .submit(Mutation::CreateMultipart {
                session: Box::new(session(
                    upload_id.clone(),
                    BucketName::parse("recovery-bucket").unwrap(),
                    "object",
                )),
                limits: MultipartLimits::default(),
            })
            .await
            .unwrap();
        concrete
            .submit(Mutation::ReserveMultipartPart {
                upload_id: upload_id.clone(),
                part_number: 1,
                attempt_id: "attempt-a".to_owned(),
                reserved_bytes: 4,
                max_parts_per_upload: 10_000,
                now: Timestamp(2),
            })
            .await
            .unwrap();
        let blob = Arc::new(InMemoryBlobStore::new());
        let staged = blob
            .stage_part(
                &upload_id,
                1,
                "attempt-a",
                Box::pin(futures_util::stream::once(async {
                    Ok(Bytes::from_static(b"part"))
                })),
                cairn_types::ChecksumSet::default(),
                4,
                None,
            )
            .await
            .unwrap();
        assert_eq!(blob.multipart_part_count(), 1);

        let queue = MultipartClaimRecoveryQueue::new(1);
        let permit = (queue.admission_callback())().await.expect("recovery slot");
        assert!(queue.part_callback()(MultipartPartWriteRecovery {
            upload_id: upload_id.clone(),
            part_number: 1,
            attempt_id: "attempt-a".to_owned(),
            storage_path: staged.storage_path,
            permit: Some(permit),
        }));
        queue.finish_requests();
        let meta: Arc<dyn MetadataStore> = concrete.clone();
        let blob_store: Arc<dyn BlobStore> = blob.clone();
        queue.worker(meta, blob_store).await;

        assert_eq!(blob.multipart_part_count(), 0);
        assert!(
            concrete
                .enumerate_stale_multipart_reservations(Timestamp(3), 10)
                .await
                .unwrap()
                .is_empty(),
            "referenced:false cleanup must release the exact durable reservation"
        );
    }

    #[tokio::test]
    async fn delayed_old_part_recovery_cannot_delete_a_superseding_retry() {
        let concrete = Arc::new(InMemoryMetadataStore::new());
        let upload_id = UploadId::from_string("part-retry-aba".to_owned());
        concrete
            .submit(Mutation::CreateMultipart {
                session: Box::new(session(
                    upload_id.clone(),
                    BucketName::parse("recovery-bucket").unwrap(),
                    "object",
                )),
                limits: MultipartLimits::default(),
            })
            .await
            .unwrap();
        let blob = Arc::new(InMemoryBlobStore::new());
        let mut paths = Vec::new();
        for (attempt, bytes, now) in [
            ("attempt-old", Bytes::from_static(b"old!"), Timestamp(2)),
            ("attempt-new", Bytes::from_static(b"new!"), Timestamp(3)),
        ] {
            concrete
                .submit(Mutation::ReserveMultipartPart {
                    upload_id: upload_id.clone(),
                    part_number: 1,
                    attempt_id: attempt.to_owned(),
                    reserved_bytes: 4,
                    max_parts_per_upload: 10_000,
                    now,
                })
                .await
                .unwrap();
            let staged = blob
                .stage_part(
                    &upload_id,
                    1,
                    attempt,
                    Box::pin(futures_util::stream::once(async move { Ok(bytes) })),
                    cairn_types::ChecksumSet::default(),
                    4,
                    None,
                )
                .await
                .unwrap();
            concrete
                .submit(Mutation::RecordPart {
                    upload_id: upload_id.clone(),
                    attempt_id: attempt.to_owned(),
                    part: PartRecord {
                        part_number: 1,
                        size: 4,
                        etag: attempt.to_owned(),
                        storage_path: staged.storage_path.clone(),
                        checksum: None,
                        part_dek: None,
                    },
                })
                .await
                .unwrap();
            paths.push(staged.storage_path);
        }
        assert_eq!(blob.multipart_part_count(), 2);

        let queue = MultipartClaimRecoveryQueue::new(1);
        assert!(queue.part_callback()(MultipartPartWriteRecovery {
            upload_id: upload_id.clone(),
            part_number: 1,
            attempt_id: "attempt-old".to_owned(),
            storage_path: paths[0].clone(),
            permit: None,
        }));
        queue.finish_requests();
        let meta: Arc<dyn MetadataStore> = concrete.clone();
        let blob_store: Arc<dyn BlobStore> = blob.clone();
        queue.worker(meta, blob_store).await;

        assert_eq!(
            blob.multipart_part_count(),
            1,
            "delayed recovery must reclaim only the old attempt artifact"
        );
        assert_eq!(
            concrete
                .submit(Mutation::ResolveMultipartPartWrite {
                    upload_id,
                    part_number: 1,
                    storage_path: paths[1].clone(),
                })
                .await
                .unwrap(),
            MutationOutcome::MultipartPartWriteResolved { referenced: true }
        );
    }

    #[tokio::test]
    async fn delayed_old_token_release_cannot_affect_a_new_claim_owner() {
        let concrete = Arc::new(InMemoryMetadataStore::new());
        let (upload_id, first_token) = claimed(&concrete, "ambiguous-release").await;
        let blob = InMemoryBlobStore::new();
        let stale_recovery = MultipartClaimRecovery {
            upload_id: upload_id.clone(),
            claim_token: first_token,
            assembled_blob: None,
            delete_blob_on_not_owner: false,
            permit: None,
        };

        // Model a remote commit/ack ambiguity: the first submit really releases the old owner, but
        // its caller receives an engine error.
        recover_one(&blob, stale_recovery.clone(), {
            let concrete = concrete.clone();
            move |mutation| async move {
                assert!(matches!(
                    concrete.submit(mutation).await.unwrap(),
                    MutationOutcome::MultipartClaimRelease(ClaimReleaseOutcome::Released)
                ));
                Err(MetaError::Engine("commit acknowledgement lost".to_owned()))
            }
        })
        .await;

        // A retrying client now owns `completing` under a new token. A delayed at-least-once
        // recovery of the old token must be a harmless NotOwner, never an ABA release.
        let second_token = MultipartClaimToken::generate();
        assert!(matches!(
            concrete
                .submit(Mutation::ClaimMultipart {
                    upload_id: upload_id.clone(),
                    claim_token: second_token,
                })
                .await
                .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));
        recover_one(&blob, stale_recovery, {
            let concrete = concrete.clone();
            move |mutation| async move { concrete.submit(mutation).await }
        })
        .await;
        assert_eq!(
            concrete
                .get_multipart(&upload_id)
                .await
                .unwrap()
                .expect("new claimant owns the session")
                .status,
            MultipartStatus::Completing
        );
    }

    #[tokio::test]
    async fn not_owner_preserves_ambiguous_path_but_deletes_one_proven_unreferenced() {
        let blob = Arc::new(InMemoryBlobStore::new());
        let possibly_committed = blob
            .stage(
                &BucketName::parse("recovery-bucket").unwrap(),
                Box::pin(futures_util::stream::once(async {
                    Ok(Bytes::from_static(b"possibly committed"))
                })),
                StageOptions::default(),
            )
            .await
            .unwrap();
        let proven_unreferenced = blob
            .stage(
                &BucketName::parse("recovery-bucket").unwrap(),
                Box::pin(futures_util::stream::once(async {
                    Ok(Bytes::from_static(b"typed non-commit"))
                })),
                StageOptions::default(),
            )
            .await
            .unwrap();
        let queue = MultipartClaimRecoveryQueue::new(2);
        let callback = queue.callback();
        assert!(callback(MultipartClaimRecovery {
            upload_id: UploadId::from_string("already-terminal".to_owned()),
            claim_token: MultipartClaimToken::generate(),
            assembled_blob: Some(possibly_committed.storage_path.clone()),
            delete_blob_on_not_owner: false,
            permit: None,
        }));
        assert!(callback(MultipartClaimRecovery {
            upload_id: UploadId::from_string("typed-not-owner".to_owned()),
            claim_token: MultipartClaimToken::generate(),
            assembled_blob: Some(proven_unreferenced.storage_path.clone()),
            delete_blob_on_not_owner: true,
            permit: None,
        }));
        queue.finish_requests();

        let meta: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
        let blob_store: Arc<dyn BlobStore> = blob.clone();
        queue.worker(meta, blob_store).await;

        assert!(
            blob.get_bytes(&possibly_committed.storage_path).is_some(),
            "NotOwner can mean Complete committed, so recovery must preserve its blob"
        );
        assert!(
            blob.get_bytes(&proven_unreferenced.storage_path).is_none(),
            "typed non-commit proof permits cleanup despite NotOwner"
        );
    }

    #[tokio::test]
    async fn retained_worker_routes_encoded_upload_to_a_nonzero_shard() {
        let inner: Vec<Arc<InMemoryMetadataStore>> = (0..3)
            .map(|_| Arc::new(InMemoryMetadataStore::new()))
            .collect();
        let routed: Vec<Arc<dyn MetadataStore>> = inner
            .iter()
            .cloned()
            .map(|store| store as Arc<dyn MetadataStore>)
            .collect();
        let router = Arc::new(ShardedMetadataStore::new(routed));
        let bucket = BucketName::parse("charlie").unwrap();
        assert_eq!(shard_for_bucket(bucket.as_str(), 3), 1);
        let created = router
            .submit(Mutation::CreateMultipart {
                session: Box::new(session(UploadId::generate(), bucket, "nonzero")),
                limits: MultipartLimits::default(),
            })
            .await
            .unwrap();
        let upload_id = match created {
            MutationOutcome::MultipartCreated(upload_id) => upload_id,
            outcome => panic!("expected MultipartCreated, got {outcome:?}"),
        };
        router
            .submit(Mutation::ReserveMultipartPart {
                upload_id: upload_id.clone(),
                part_number: 1,
                attempt_id: "routed-attempt".to_owned(),
                reserved_bytes: 4,
                max_parts_per_upload: 10_000,
                now: Timestamp(2),
            })
            .await
            .unwrap();
        let routed_path = StoragePath::from_string(format!(
            ".staging/multipart/{}/00001-routed-attempt",
            upload_id.as_str()
        ));
        router
            .submit(Mutation::RecordPart {
                upload_id: upload_id.clone(),
                attempt_id: "routed-attempt".to_owned(),
                part: PartRecord {
                    part_number: 1,
                    size: 4,
                    etag: "etag".to_owned(),
                    storage_path: routed_path.clone(),
                    checksum: None,
                    part_dek: None,
                },
            })
            .await
            .unwrap();
        assert_eq!(
            router
                .submit(Mutation::ResolveMultipartPartWrite {
                    upload_id: upload_id.clone(),
                    part_number: 1,
                    storage_path: routed_path,
                })
                .await
                .unwrap(),
            MutationOutcome::MultipartPartWriteResolved { referenced: true },
            "encoded upload ids must route exact part resolution to their bucket shard"
        );
        let claim_token = MultipartClaimToken::generate();
        assert!(matches!(
            router
                .submit(Mutation::ClaimMultipart {
                    upload_id: upload_id.clone(),
                    claim_token: claim_token.clone(),
                })
                .await
                .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));

        let queue = MultipartClaimRecoveryQueue::new(1);
        assert!(queue.callback()(MultipartClaimRecovery {
            upload_id: upload_id.clone(),
            claim_token,
            assembled_blob: None,
            delete_blob_on_not_owner: false,
            permit: None,
        }));
        queue.finish_requests();
        let meta: Arc<dyn MetadataStore> = router.clone();
        let blob: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
        queue.worker(meta, blob).await;

        assert_eq!(
            inner[1]
                .get_multipart(&upload_id)
                .await
                .unwrap()
                .expect("session lives on nonzero shard")
                .status,
            MultipartStatus::Active
        );
        assert!(inner[0].get_multipart(&upload_id).await.unwrap().is_none());
        assert!(inner[2].get_multipart(&upload_id).await.unwrap().is_none());
    }
}
