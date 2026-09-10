//! The same exact admission/cleanup contract runs against every metadata backend and shard router.

use crate::id::{BucketName, ObjectKey, UserId, VersionId};
use crate::meta::{Mutation, MutationOutcome};
use crate::storage::io::StorageIoWatch;
use crate::storage::{
    PlannedStorageWrite, StorageAdmission, StorageCleanup, StorageMutation, StorageToken,
    StorageWriteTarget,
};
use crate::{Bucket, MetadataStore, OwnershipMode, Timestamp, VersioningState};
use std::sync::Arc;

pub(super) fn plan(bucket: &BucketName, generation: &StorageToken) -> PlannedStorageWrite {
    PlannedStorageWrite::new(
        bucket.clone(),
        generation.clone(),
        StorageWriteTarget::Object {
            key: ObjectKey::parse("journal/key").unwrap(),
            version_id: VersionId::null(),
            row_id: StorageToken::generate().as_str().to_owned(),
        },
    )
    .unwrap()
}

pub(super) async fn claim(
    meta: &dyn MetadataStore,
    generation: &StorageToken,
    limit: u32,
    now: i64,
) -> Vec<StorageCleanup> {
    let MutationOutcome::StorageCleanupBatch(batch) = meta
        .submit(Mutation::ClaimStorageCleanup {
            generation: generation.clone(),
            limit,
            now: Timestamp(now),
            lease_secs: 60,
        })
        .await
        .unwrap()
    else {
        panic!("storage cleanup batch expected")
    };
    assert!(batch.len() <= limit.clamp(1, 1000) as usize);
    batch
}

pub(super) async fn update(
    meta: &dyn MetadataStore,
    bucket: &BucketName,
    operation: StorageMutation,
    applied: bool,
) {
    assert_eq!(
        meta.submit(Mutation::Storage {
            bucket: bucket.clone(),
            operation
        })
        .await
        .unwrap(),
        MutationOutcome::StorageUpdated { applied }
    );
}

pub(super) async fn create_bucket(meta: &dyn MetadataStore, bucket: &BucketName) {
    meta.submit(Mutation::CreateBucket(Box::new(Bucket {
        name: bucket.clone(),
        owner_id: UserId::generate(),
        created_at: Timestamp(0),
        versioning: VersioningState::Unversioned,
        ownership_mode: OwnershipMode::BucketOwnerEnforced,
        region: "us-east-1".to_owned(),
        compression: None,
    })))
    .await
    .unwrap();
}

/// Check admission acknowledgements, cancellation/quiescence, generation fencing, exact cleanup
/// leases, bounded pages, routing after bucket deletion, and lost-admission restart resolution.
pub async fn assert_storage_journal(meta: &dyn MetadataStore) {
    let bucket = BucketName::parse("storage-journal-contract").unwrap();
    create_bucket(meta, &bucket).await;
    let generation = StorageToken::generate();
    let planned = plan(&bucket, &generation);
    let reserve = Mutation::Storage {
        bucket: bucket.clone(),
        operation: StorageMutation::Reserve {
            plan: Box::new(planned.plan().clone()),
            now: Timestamp(0),
        },
    };
    assert_eq!(
        meta.submit(reserve.clone()).await.unwrap(),
        MutationOutcome::StorageAdmission(StorageAdmission::NotApplied)
    );
    meta.submit(Mutation::BeginStorageGeneration {
        generation: generation.clone(),
    })
    .await
    .unwrap();
    let mut malformed = planned.plan().clone();
    malformed.paths[0].path = crate::StoragePath::from_string("../outside".to_owned());
    assert!(
        meta.submit(Mutation::Storage {
            bucket: bucket.clone(),
            operation: StorageMutation::Reserve {
                plan: Box::new(malformed),
                now: Timestamp(0),
            }
        })
        .await
        .is_err()
    );
    let MutationOutcome::StorageAdmission(receipt) = meta.submit(reserve.clone()).await.unwrap()
    else {
        panic!("admission expected")
    };
    assert_eq!(
        receipt,
        StorageAdmission::Granted(Box::new(planned.plan().clone()))
    );
    assert_eq!(
        meta.submit(reserve).await.unwrap(),
        MutationOutcome::StorageAdmission(StorageAdmission::NotApplied)
    );
    let paths = planned.plan().paths.clone();
    let attempt = planned.plan().attempt.clone();
    let (mut watch, lease) = StorageIoWatch::new(attempt.clone(), generation.clone(), Arc::new(()));
    let permit = planned.admit(receipt, lease).unwrap();
    assert!(claim(meta, &generation, 100, 0).await.is_empty());
    update(
        meta,
        &bucket,
        StorageMutation::Cancel {
            attempt,
            generation: generation.clone(),
        },
        true,
    )
    .await;
    assert!(
        claim(meta, &generation, 100, 1_000_000).await.is_empty(),
        "time cannot authorize an active intent's reclamation"
    );
    drop(permit);
    let proof = watch.quiescent().await;
    update(
        meta,
        &bucket,
        StorageMutation::Resolve {
            quiescence: proof.clone(),
        },
        true,
    )
    .await;
    update(
        meta,
        &bucket,
        StorageMutation::Resolve { quiescence: proof },
        false,
    )
    .await;
    meta.submit(Mutation::DeleteBucket(bucket.clone()))
        .await
        .unwrap();
    let batch = claim(meta, &generation, 1, 0).await;
    assert_eq!(batch.len(), 1);
    assert!(paths.iter().any(|path| path.path == batch[0].path));
    let old_claim = batch[0].clone();
    let mut wrong = old_claim.clone();
    wrong.claim_token = StorageToken::generate();
    update(
        meta,
        &bucket,
        StorageMutation::FinishCleanup {
            cleanup: wrong,
            now: Timestamp(1),
        },
        false,
    )
    .await;
    let next_generation = StorageToken::generate();
    meta.submit(Mutation::BeginStorageGeneration {
        generation: next_generation.clone(),
    })
    .await
    .unwrap();
    update(
        meta,
        &bucket,
        StorageMutation::FinishCleanup {
            cleanup: old_claim,
            now: Timestamp(1),
        },
        false,
    )
    .await;
    let batch = claim(meta, &next_generation, 100, 2).await;
    assert_eq!(
        batch.len(),
        3,
        "debt must survive bucket deletion and process generation changes"
    );
    for cleanup in batch {
        assert_eq!(cleanup.bucket, bucket);
        assert!(paths.iter().any(|path| path.path == cleanup.path));
        let mut wrong = cleanup.clone();
        wrong.lease_until = Timestamp(wrong.lease_until.0 + 1);
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup: wrong,
                now: Timestamp(3),
            },
            false,
        )
        .await;
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup: cleanup.clone(),
                now: Timestamp(3),
            },
            true,
        )
        .await;
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(3),
            },
            false,
        )
        .await;
    }
    assert!(claim(meta, &next_generation, 100, 4).await.is_empty());

    // Persist two admissions but deliberately discard their acknowledgements. There is no I/O;
    // exclusive restart still has to resolve every possible name, bounded by the requested page.
    create_bucket(meta, &bucket).await;
    for _ in 0..2 {
        let planned = plan(&bucket, &next_generation);
        assert!(matches!(
            meta.submit(Mutation::Storage {
                bucket: bucket.clone(),
                operation: StorageMutation::Reserve {
                    plan: Box::new(planned.plan().clone()),
                    now: Timestamp(10),
                }
            })
            .await
            .unwrap(),
            MutationOutcome::StorageAdmission(StorageAdmission::Granted(_))
        ));
    }
    let restarted = StorageToken::generate();
    meta.submit(Mutation::BeginStorageGeneration {
        generation: restarted.clone(),
    })
    .await
    .unwrap();
    for expected in [1, 1, 0] {
        let request = Mutation::ListStorageIntents {
            generation: restarted.clone(),
            limit: 1,
        };
        let MutationOutcome::StorageIntentBatch(plans) =
            meta.submit(request.clone()).await.unwrap()
        else {
            panic!("old storage intent page expected");
        };
        assert_eq!(plans.len(), expected);
        assert_eq!(
            meta.submit(request).await.unwrap(),
            MutationOutcome::StorageIntentBatch(plans.clone()),
            "listing or advancing the generation cannot establish backend quiescence"
        );
        if let Some(plan) = plans.first() {
            // This metadata-only fixture never creates files. Production obtains this proof only
            // after the exclusive restart consumer probes every planned physical alias.
            let (mut watch, lease) =
                StorageIoWatch::new(plan.attempt.clone(), plan.generation.clone(), Arc::new(()));
            drop(lease);
            update(
                meta,
                &plan.bucket,
                StorageMutation::ResolveRecovered {
                    current_generation: restarted.clone(),
                    quiescence: watch.quiescent().await,
                },
                true,
            )
            .await;
        }
    }
    let batch = claim(meta, &restarted, 100, 11).await;
    assert_eq!(batch.len(), 6);
    for cleanup in batch {
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(12),
            },
            true,
        )
        .await;
    }
    assert!(claim(meta, &restarted, 100, 13).await.is_empty());
    assert_storage_publication(meta).await;
    assert_storage_multipart_quota(meta).await;
    assert_storage_completion(meta).await;
}

pub(super) fn object_row(
    plan: &crate::storage::StorageWritePlan,
    owner: UserId,
) -> crate::ObjectVersionRow {
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
    crate::ObjectVersionRow {
        id: row_id,
        bucket: plan.bucket.clone(),
        key,
        version_id,
        is_latest: true,
        is_delete_marker: false,
        size_logical: 10,
        size_physical: 10,
        etag: crate::ETag::from_string("etag".into()),
        content_type: "application/octet-stream".into(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(plan.final_path().unwrap().clone()),
        compression: crate::CompressionDescriptor::Uncompressed,
        storage_class: crate::StorageClass::Standard,
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

pub(super) fn put(row: crate::ObjectVersionRow) -> Mutation {
    Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Default::default(),
        initial_state: Default::default(),
        replication: Vec::new(),
    }
}

pub(super) async fn admit_object(
    meta: &dyn MetadataStore,
    plan: &crate::storage::StorageWritePlan,
) {
    assert_eq!(
        meta.submit(Mutation::Storage {
            bucket: plan.bucket.clone(),
            operation: StorageMutation::Reserve {
                plan: Box::new(plan.clone()),
                now: Timestamp(0)
            }
        })
        .await
        .unwrap(),
        MutationOutcome::StorageAdmission(StorageAdmission::Granted(Box::new(plan.clone())))
    );
}

async fn assert_storage_publication(meta: &dyn MetadataStore) {
    let bucket = BucketName::parse("storage-publication-contract").unwrap();
    create_bucket(meta, &bucket).await;
    let owner = meta.get_bucket(&bucket).await.unwrap().unwrap().owner_id;
    let generation = StorageToken::generate();
    meta.submit(Mutation::BeginStorageGeneration {
        generation: generation.clone(),
    })
    .await
    .unwrap();
    let planned = plan(&bucket, &generation);
    let plan = planned.plan();
    let row = object_row(plan, owner.clone());
    let publication = Mutation::PublishStorageWrite {
        plan: Box::new(plan.clone()),
        operation: Box::new(put(row.clone())),
    };
    assert_eq!(
        meta.submit(publication.clone()).await.unwrap(),
        MutationOutcome::StoragePublicationNotApplied
    );
    assert!(
        meta.submit(put(row.clone())).await.is_err(),
        "bare physical publication must fail"
    );
    admit_object(meta, plan).await;
    let mut wrong = row.clone();
    wrong.id = StorageToken::generate().as_str().to_owned();
    assert!(
        meta.submit(Mutation::PublishStorageWrite {
            plan: Box::new(plan.clone()),
            operation: Box::new(put(wrong))
        })
        .await
        .is_err()
    );
    assert!(
        meta.submit(Mutation::PublishStorageWrite {
            plan: Box::new(plan.clone()),
            operation: Box::new(publication.clone())
        })
        .await
        .is_err()
    );
    assert!(claim(meta, &generation, 100, 0).await.is_empty());
    assert!(matches!(
        meta.submit(publication.clone()).await.unwrap(),
        MutationOutcome::Put { .. }
    ));
    assert_eq!(
        meta.submit(publication).await.unwrap(),
        MutationOutcome::StoragePublicationNotApplied
    );
    let aliases = claim(meta, &generation, 100, 0).await;
    assert_eq!(aliases.len(), 2);
    for cleanup in aliases {
        assert_ne!(&cleanup.path, plan.final_path().unwrap());
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(1),
            },
            true,
        )
        .await;
    }
    // Failed preconditions must preserve the entire admission, permitting the exact attempt to be
    // resolved later. A later successful publication consumes it together with supersession debt.
    let next = super::storage_contract::plan(&bucket, &generation);
    let next = next.plan();
    admit_object(meta, next).await;
    let row = object_row(next, owner.clone());
    let mut conditional = put(row.clone());
    if let Mutation::PutObjectVersion { precondition, .. } = &mut conditional {
        precondition.if_none_match = Some(crate::meta::IfNoneMatch::Any);
    }
    assert!(
        meta.submit(Mutation::PublishStorageWrite {
            plan: Box::new(next.clone()),
            operation: Box::new(conditional)
        })
        .await
        .is_err()
    );
    assert!(claim(meta, &generation, 100, 2).await.is_empty());
    assert!(matches!(
        meta.submit(Mutation::PublishStorageWrite {
            plan: Box::new(next.clone()),
            operation: Box::new(put(row))
        })
        .await
        .unwrap(),
        MutationOutcome::Put { .. }
    ));
    let aliases = claim(meta, &generation, 100, 2).await;
    assert_eq!(aliases.len(), 3);
    assert!(
        aliases
            .iter()
            .any(|cleanup| &cleanup.path == plan.final_path().unwrap())
    );
    for cleanup in aliases {
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(3),
            },
            true,
        )
        .await;
    }
    meta.submit(Mutation::CreateDeleteMarker {
        bucket: bucket.clone(),
        key: ObjectKey::parse("journal/key").unwrap(),
        version_id: VersionId::null(),
        owner_id: owner,
        now: Timestamp(4),
        bypass: crate::GovernanceBypass::Denied,
        expected_current: None,
        replication: Vec::new(),
    })
    .await
    .unwrap();
    let deleted = claim(meta, &generation, 100, 4).await;
    assert_eq!(deleted.len(), 1);
    assert_eq!(&deleted[0].path, next.final_path().unwrap());
    for cleanup in deleted {
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(5),
            },
            true,
        )
        .await;
    }
}

pub(super) async fn create_upload(
    meta: &dyn MetadataStore,
    bucket: &BucketName,
) -> crate::UploadId {
    let owner = meta.get_bucket(bucket).await.unwrap().unwrap().owner_id;
    let upload = crate::UploadId::generate();
    let outcome = meta
        .submit(Mutation::CreateMultipart {
            session: Box::new(crate::meta::MultipartSession {
                upload_id: upload.clone(),
                bucket: bucket.clone(),
                key: ObjectKey::parse("multipart/key").unwrap(),
                content_type: "application/octet-stream".into(),
                content_disposition: None,
                status: crate::meta::MultipartStatus::Active,
                owner_id: owner.clone(),
                initiated_by: owner,
                intended_acl: None,
                replica_intent: None,
                user_metadata: Vec::new(),
                initial_tags: Vec::new(),
                lock_intent: Default::default(),
                sse_requested: false,
                encrypt_parts: false,
                sse_kms_requested: false,
                sse_kms_key_id: None,
                sse_bucket_key_enabled: false,
                created_at: Timestamp(0),
                updated_at: Timestamp(0),
            }),
            limits: Default::default(),
        })
        .await
        .unwrap();
    let MutationOutcome::MultipartCreated(upload) = outcome else {
        panic!("multipart creation expected");
    };
    upload
}

pub(super) fn part_plan(
    bucket: &BucketName,
    generation: &StorageToken,
    upload: &crate::UploadId,
) -> crate::storage::StorageWritePlan {
    PlannedStorageWrite::new(
        bucket.clone(),
        generation.clone(),
        StorageWriteTarget::Part {
            upload_id: upload.clone(),
            part_number: 1,
            reservation_id: StorageToken::generate().as_str().to_owned(),
        },
    )
    .unwrap()
    .plan()
    .clone()
}

pub(super) fn reserve_part(plan: &crate::storage::StorageWritePlan) -> Mutation {
    let StorageWriteTarget::Part {
        upload_id,
        part_number,
        reservation_id,
    } = &plan.target
    else {
        panic!("part required");
    };
    Mutation::AdmitStorageWrite {
        plan: Box::new(plan.clone()),
        operation: Box::new(Mutation::ReserveMultipartPart {
            upload_id: upload_id.clone(),
            part_number: *part_number,
            attempt_id: reservation_id.clone(),
            reserved_bytes: 10,
            max_parts_per_upload: 10_000,
            now: Timestamp(0),
        }),
        now: Timestamp(0),
    }
}

pub(super) fn publish_part(plan: &crate::storage::StorageWritePlan) -> Mutation {
    let StorageWriteTarget::Part {
        upload_id,
        part_number,
        reservation_id,
    } = &plan.target
    else {
        panic!("part required");
    };
    Mutation::PublishStorageWrite {
        plan: Box::new(plan.clone()),
        operation: Box::new(Mutation::RecordPart {
            upload_id: upload_id.clone(),
            attempt_id: reservation_id.clone(),
            part: crate::meta::PartRecord {
                part_number: *part_number,
                size: 10,
                etag: "etag".into(),
                storage_path: plan.final_path().unwrap().clone(),
                checksum: None,
                part_dek: None,
            },
        }),
    }
}

/// Restore preparation fences copied claims without forgiving physical work or quota charges.
pub async fn assert_prepare_storage_restore(meta: &dyn MetadataStore) {
    let bucket = BucketName::parse("storage-restore-contract").unwrap();
    create_bucket(meta, &bucket).await;
    meta.submit(Mutation::SetBucketQuota {
        bucket: bucket.clone(),
        quota_bytes: Some(40),
    })
    .await
    .unwrap();
    let generation = StorageToken::generate();
    meta.submit(Mutation::BeginStorageGeneration {
        generation: generation.clone(),
    })
    .await
    .unwrap();
    let owner = meta.get_bucket(&bucket).await.unwrap().unwrap().owner_id;
    let object = plan(&bucket, &generation);
    let row = object_row(object.plan(), owner);
    admit_object(meta, object.plan()).await;
    assert!(matches!(
        meta.submit(Mutation::PublishStorageWrite {
            plan: Box::new(object.plan().clone()),
            operation: Box::new(put(row.clone())),
        })
        .await
        .unwrap(),
        MutationOutcome::Put { .. }
    ));
    let upload = create_upload(meta, &bucket).await;
    // One live object, one replaced part's cleanup debt, one live part and one pending
    // replacement each retain ten bytes. All four must still count after restore preparation.
    for _ in 0..2 {
        let part = part_plan(&bucket, &generation, &upload);
        assert!(matches!(
            meta.submit(reserve_part(&part)).await.unwrap(),
            MutationOutcome::StorageAdmission(StorageAdmission::Granted(_))
        ));
        assert!(matches!(
            meta.submit(publish_part(&part)).await.unwrap(),
            MutationOutcome::PartRecorded { .. }
        ));
    }
    let pending = part_plan(&bucket, &generation, &upload);
    assert!(matches!(
        meta.submit(reserve_part(&pending)).await.unwrap(),
        MutationOutcome::StorageAdmission(StorageAdmission::Granted(_))
    ));
    let session = meta.get_multipart(&upload).await.unwrap();
    let parts = meta.list_parts(&upload, 0, 100).await.unwrap();
    let reservations = meta
        .enumerate_stale_multipart_reservations(Timestamp(1), 100)
        .await
        .unwrap();
    let debts = meta.list_multipart_cleanups(100).await.unwrap();
    // Protocol-2 ownership excludes these rows from legacy time-based cleanup enumerators.
    assert!(reservations.is_empty());
    assert!(debts.is_empty());
    let old_claims = claim(meta, &generation, 100, 0).await;
    assert_eq!(old_claims.len(), 5);
    assert!(
        old_claims
            .iter()
            .any(|cleanup| cleanup.quota_debt_id.is_some())
    );
    assert!(matches!(
        meta.submit(reserve_part(&part_plan(&bucket, &generation, &upload)))
            .await,
        Err(crate::MetaError::QuotaExceeded)
    ));
    assert!(
        meta.submit(Mutation::PrepareStorageRestore {
            generation: generation.clone(),
        })
        .await
        .is_err()
    );
    assert!(
        claim(meta, &generation, 100, 1).await.is_empty(),
        "rejected preparation must retain every existing lease"
    );

    let restored = StorageToken::generate();
    assert_eq!(
        meta.submit(Mutation::PrepareStorageRestore {
            generation: restored.clone(),
        })
        .await
        .unwrap(),
        MutationOutcome::Ack
    );
    assert_eq!(meta.get_multipart(&upload).await.unwrap(), session);
    assert_eq!(meta.list_parts(&upload, 0, 100).await.unwrap(), parts);
    assert_eq!(
        meta.enumerate_stale_multipart_reservations(Timestamp(1), 100)
            .await
            .unwrap(),
        reservations
    );
    assert_eq!(meta.list_multipart_cleanups(100).await.unwrap(), debts);
    assert_eq!(
        meta.get_version(&bucket, &row.key, &row.version_id)
            .await
            .unwrap(),
        Some(row)
    );
    assert_eq!(
        meta.submit(Mutation::ListStorageIntents {
            generation: restored.clone(),
            limit: 100,
        })
        .await
        .unwrap(),
        MutationOutcome::StorageIntentBatch(vec![pending.clone()])
    );
    assert_eq!(
        meta.submit(publish_part(&pending)).await.unwrap(),
        MutationOutcome::StoragePublicationNotApplied
    );
    assert!(matches!(
        meta.submit(reserve_part(&part_plan(&bucket, &restored, &upload)))
            .await,
        Err(crate::MetaError::QuotaExceeded)
    ));
    for cleanup in &old_claims {
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup: cleanup.clone(),
                now: Timestamp(1),
            },
            false,
        )
        .await;
    }
    let new_claims = claim(meta, &restored, 100, 1).await;
    assert_eq!(new_claims.len(), old_claims.len());
    for cleanup in &new_claims {
        let old = old_claims.iter().find(|old| old.id == cleanup.id).unwrap();
        assert_eq!(cleanup.path, old.path);
        assert_eq!(cleanup.bucket, old.bucket);
        assert_eq!(cleanup.quota_debt_id, old.quota_debt_id);
        assert_ne!(cleanup.claim_token, old.claim_token);
        assert_eq!(cleanup.generation, restored);
    }
}

async fn assert_storage_multipart_quota(meta: &dyn MetadataStore) {
    let bucket = BucketName::parse("storage-part-quota-contract").unwrap();
    create_bucket(meta, &bucket).await;
    meta.submit(Mutation::SetBucketQuota {
        bucket: bucket.clone(),
        quota_bytes: Some(20),
    })
    .await
    .unwrap();
    let generation = StorageToken::generate();
    meta.submit(Mutation::BeginStorageGeneration {
        generation: generation.clone(),
    })
    .await
    .unwrap();
    let upload = create_upload(meta, &bucket).await;
    let first = part_plan(&bucket, &generation, &upload);
    assert!(matches!(
        meta.submit(reserve_part(&first)).await.unwrap(),
        MutationOutcome::StorageAdmission(StorageAdmission::Granted(_))
    ));
    assert!(matches!(
        meta.submit(publish_part(&first)).await.unwrap(),
        MutationOutcome::PartRecorded { .. }
    ));
    let mut aliases = claim(meta, &generation, 100, 0).await;
    assert_eq!(aliases.len(), 1);
    let old_alias = aliases.remove(0);
    assert!(old_alias.quota_debt_id.is_none());
    let second = part_plan(&bucket, &generation, &upload);
    assert!(matches!(
        meta.submit(reserve_part(&second)).await.unwrap(),
        MutationOutcome::StorageAdmission(StorageAdmission::Granted(_))
    ));
    assert!(matches!(
        meta.submit(publish_part(&second)).await.unwrap(),
        MutationOutcome::PartRecorded { .. }
    ));
    update(
        meta,
        &bucket,
        StorageMutation::FinishCleanup {
            cleanup: old_alias.clone(),
            now: Timestamp(1),
        },
        false,
    )
    .await;
    let third = part_plan(&bucket, &generation, &upload);
    assert!(matches!(
        meta.submit(reserve_part(&third)).await,
        Err(crate::MetaError::QuotaExceeded)
    ));
    let mut debts = claim(meta, &generation, 100, 2).await;
    assert_eq!(debts.len(), 3);
    let alias_index = debts
        .iter()
        .position(|row| row.path == old_alias.path)
        .unwrap();
    let alias = debts.remove(alias_index);
    assert_eq!(alias.id, old_alias.id);
    assert_ne!(alias.claim_token, old_alias.claim_token);
    assert!(alias.quota_debt_id.is_some());
    assert!(Timestamp(2) < old_alias.lease_until);
    assert!(claim(meta, &generation, 100, 2).await.is_empty());
    let terminal_alias_index = debts
        .iter()
        .position(|row| row.quota_debt_id.is_none())
        .unwrap();
    let terminal_alias = debts.remove(terminal_alias_index);
    for cleanup in debts {
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(3),
            },
            true,
        )
        .await;
    }
    assert!(
        matches!(
            meta.submit(reserve_part(&third)).await,
            Err(crate::MetaError::QuotaExceeded)
        ),
        "unlinking the part must not forgive its unsynchronized spool alias"
    );
    update(
        meta,
        &bucket,
        StorageMutation::FinishCleanup {
            cleanup: alias.clone(),
            now: Timestamp(4),
        },
        true,
    )
    .await;
    assert!(matches!(
        meta.submit(reserve_part(&third)).await.unwrap(),
        MutationOutcome::StorageAdmission(StorageAdmission::Granted(_))
    ));
    update(
        meta,
        &bucket,
        StorageMutation::FinishCleanup {
            cleanup: alias,
            now: Timestamp(5),
        },
        false,
    )
    .await;
    let another_upload = create_upload(meta, &bucket).await;
    let another_part = part_plan(&bucket, &generation, &another_upload);
    assert!(matches!(
        meta.submit(reserve_part(&another_part)).await,
        Err(crate::MetaError::QuotaExceeded)
    ));
    meta.submit(Mutation::AbortMultipart(another_upload))
        .await
        .unwrap();
    // Abort converts both the live part and the active reservation without releasing either charge.
    meta.submit(Mutation::AbortMultipart(upload)).await.unwrap();
    update(
        meta,
        &bucket,
        StorageMutation::FinishCleanup {
            cleanup: terminal_alias.clone(),
            now: Timestamp(6),
        },
        false,
    )
    .await;
    assert_eq!(
        meta.submit(Mutation::RecoverMultipartStagingAccounting { limit: 100 })
            .await
            .unwrap(),
        MutationOutcome::MultipartAccountingReleased(0)
    );
    let debts = claim(meta, &generation, 100, 7).await;
    assert_eq!(
        debts.len(),
        2,
        "active part intent must still protect all of its aliases"
    );
    let reclaimed_alias = debts
        .iter()
        .find(|row| row.path == terminal_alias.path)
        .unwrap();
    assert_ne!(reclaimed_alias.claim_token, terminal_alias.claim_token);
    assert!(reclaimed_alias.quota_debt_id.is_some());
    assert!(Timestamp(7) < terminal_alias.lease_until);
    for cleanup in debts {
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(8),
            },
            true,
        )
        .await;
    }
    let (mut watch, lease) =
        StorageIoWatch::new(third.attempt.clone(), generation.clone(), Arc::new(()));
    drop(lease);
    update(
        meta,
        &bucket,
        StorageMutation::Resolve {
            quiescence: watch.quiescent().await,
        },
        true,
    )
    .await;
    let debts = claim(meta, &generation, 100, 9).await;
    assert_eq!(debts.len(), 2);
    assert_eq!(debts[0].quota_debt_id, debts[1].quota_debt_id);
    for cleanup in debts {
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(10),
            },
            true,
        )
        .await;
    }
    assert!(claim(meta, &generation, 100, 11).await.is_empty());
}

fn completion_plan(
    bucket: &BucketName,
    generation: &StorageToken,
    upload: &crate::UploadId,
) -> crate::storage::StorageWritePlan {
    PlannedStorageWrite::new(
        bucket.clone(),
        generation.clone(),
        StorageWriteTarget::Completion {
            upload_id: upload.clone(),
            claim_token: crate::id::MultipartClaimToken::generate()
                .as_str()
                .to_owned(),
            key: ObjectKey::parse("multipart/key").unwrap(),
            version_id: VersionId::null(),
            row_id: StorageToken::generate().as_str().to_owned(),
        },
    )
    .unwrap()
    .plan()
    .clone()
}

fn claim_completion(plan: &crate::storage::StorageWritePlan) -> Mutation {
    let StorageWriteTarget::Completion {
        upload_id,
        claim_token,
        ..
    } = &plan.target
    else {
        panic!("completion required");
    };
    Mutation::AdmitStorageWrite {
        plan: Box::new(plan.clone()),
        now: Timestamp(0),
        operation: Box::new(Mutation::ClaimMultipart {
            upload_id: upload_id.clone(),
            claim_token: crate::id::MultipartClaimToken::from_string(claim_token.clone()),
        }),
    }
}

fn publish_completion(plan: &crate::storage::StorageWritePlan, owner: UserId) -> Mutation {
    let StorageWriteTarget::Completion {
        upload_id,
        claim_token,
        ..
    } = &plan.target
    else {
        panic!("completion required");
    };
    Mutation::PublishStorageWrite {
        plan: Box::new(plan.clone()),
        operation: Box::new(Mutation::CompleteMultipart {
            upload_id: upload_id.clone(),
            claim_token: crate::id::MultipartClaimToken::from_string(claim_token.clone()),
            row: Box::new(object_row(plan, owner)),
            precondition: Default::default(),
            replication: Vec::new(),
        }),
    }
}

async fn assert_storage_completion(meta: &dyn MetadataStore) {
    use crate::meta::{ClaimOutcome, ClaimReleaseOutcome, MultipartTerminalOutcome};
    let bucket = BucketName::parse("storage-completion-contract").unwrap();
    create_bucket(meta, &bucket).await;
    let owner = meta.get_bucket(&bucket).await.unwrap().unwrap().owner_id;
    let generation = StorageToken::generate();
    meta.submit(Mutation::BeginStorageGeneration {
        generation: generation.clone(),
    })
    .await
    .unwrap();
    let upload = create_upload(meta, &bucket).await;
    let part = part_plan(&bucket, &generation, &upload);
    meta.submit(reserve_part(&part)).await.unwrap();
    meta.submit(publish_part(&part)).await.unwrap();
    let first = completion_plan(&bucket, &generation, &upload);
    let mut wrong = first.clone();
    if let StorageWriteTarget::Completion { key, .. } = &mut wrong.target {
        *key = ObjectKey::parse("wrong-key").unwrap();
    }
    assert!(meta.submit(claim_completion(&wrong)).await.is_err());
    assert!(
        matches!(
            meta.submit(claim_completion(&first)).await.unwrap(),
            MutationOutcome::StorageMultipartClaim {
                admission: StorageAdmission::Granted(_),
                claim: ClaimOutcome::Claimed(_)
            }
        ),
        "a rejected routing target must roll the completion claim back too"
    );
    let second = completion_plan(&bucket, &generation, &upload);
    assert_eq!(
        meta.submit(claim_completion(&second)).await.unwrap(),
        MutationOutcome::StorageMultipartClaim {
            admission: StorageAdmission::NotApplied,
            claim: ClaimOutcome::AlreadyClaimed,
        }
    );
    let StorageWriteTarget::Completion { claim_token, .. } = &first.target else {
        panic!("completion required");
    };
    assert_eq!(
        meta.submit(Mutation::ReleaseMultipartClaim {
            upload_id: upload.clone(),
            claim_token: crate::id::MultipartClaimToken::from_string(claim_token.clone())
        })
        .await
        .unwrap(),
        MutationOutcome::MultipartClaimRelease(ClaimReleaseOutcome::Released)
    );
    assert_eq!(
        meta.submit(publish_completion(&first, owner.clone()))
            .await
            .unwrap(),
        MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::NotOwner)
    );
    assert!(
        matches!(
            meta.submit(claim_completion(&second)).await.unwrap(),
            MutationOutcome::StorageMultipartClaim {
                admission: StorageAdmission::Granted(_),
                claim: ClaimOutcome::Claimed(_)
            }
        ),
        "a lost joint claim must discard its unacknowledged intent so the exact proposal remains retryable"
    );
    let mut failed = publish_completion(&second, owner.clone());
    if let Mutation::PublishStorageWrite { operation, .. } = &mut failed
        && let Mutation::CompleteMultipart { precondition, .. } = operation.as_mut()
    {
        precondition.if_match = Some(crate::ETag::from_string("missing".into()));
    }
    assert!(meta.submit(failed).await.is_err());
    assert!(matches!(
        meta.submit(publish_completion(&second, owner.clone()))
            .await
            .unwrap(),
        MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::Completed { .. })
    ));
    assert_eq!(
        meta.submit(publish_completion(&first, owner))
            .await
            .unwrap(),
        MutationOutcome::StoragePublicationNotApplied
    );
    let debts = claim(meta, &generation, 100, 0).await;
    assert_eq!(
        debts.len(),
        4,
        "completed object aliases and retired part aliases must all remain durable debt"
    );
    for cleanup in debts {
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(1),
            },
            true,
        )
        .await;
    }
    let (mut watch, lease) =
        StorageIoWatch::new(first.attempt.clone(), generation.clone(), Arc::new(()));
    drop(lease);
    update(
        meta,
        &bucket,
        StorageMutation::Resolve {
            quiescence: watch.quiescent().await,
        },
        true,
    )
    .await;
    let debts = claim(meta, &generation, 100, 2).await;
    assert_eq!(debts.len(), 3);
    for cleanup in debts {
        update(
            meta,
            &bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(3),
            },
            true,
        )
        .await;
    }
    assert!(claim(meta, &generation, 100, 4).await.is_empty());
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn in_memory_prepare_storage_restore_contract() {
        super::assert_prepare_storage_restore(&crate::testing::InMemoryMetadataStore::new()).await;
    }

    #[tokio::test]
    async fn storage_admission_ack_loss_preserves_joint_reservation_and_intent() {
        use super::*;
        let meta = crate::testing::InMemoryMetadataStore::new();
        let bucket = BucketName::parse("admission-ack-loss").unwrap();
        create_bucket(&meta, &bucket).await;
        meta.submit(Mutation::SetBucketQuota {
            bucket: bucket.clone(),
            quota_bytes: Some(10),
        })
        .await
        .unwrap();
        let generation = StorageToken::generate();
        meta.submit(Mutation::BeginStorageGeneration {
            generation: generation.clone(),
        })
        .await
        .unwrap();
        let upload = create_upload(&meta, &bucket).await;
        let first = part_plan(&bucket, &generation, &upload);
        meta.fail_next_storage_admission_ack();
        assert!(meta.submit(reserve_part(&first)).await.is_err());
        assert!(claim(&meta, &generation, 100, 1_000_000).await.is_empty());
        assert_eq!(
            meta.submit(Mutation::RecoverMultipartStagingAccounting { limit: 100 })
                .await
                .unwrap(),
            MutationOutcome::MultipartAccountingReleased(0)
        );
        let retry = part_plan(&bucket, &generation, &upload);
        assert!(matches!(
            meta.submit(reserve_part(&retry)).await,
            Err(crate::MetaError::QuotaExceeded)
        ));
        let (mut watch, lease) =
            StorageIoWatch::new(first.attempt.clone(), generation.clone(), Arc::new(()));
        drop(lease);
        update(
            &meta,
            &bucket,
            StorageMutation::Resolve {
                quiescence: watch.quiescent().await,
            },
            true,
        )
        .await;
        let debts = claim(&meta, &generation, 100, 1_000_001).await;
        assert_eq!(debts.len(), 2);
        for cleanup in debts {
            update(
                &meta,
                &bucket,
                StorageMutation::FinishCleanup {
                    cleanup,
                    now: Timestamp(1_000_002),
                },
                true,
            )
            .await;
        }
        assert!(matches!(
            meta.submit(reserve_part(&retry)).await.unwrap(),
            MutationOutcome::StorageAdmission(StorageAdmission::Granted(_))
        ));
    }

    #[tokio::test]
    async fn in_memory_storage_journal_contract() {
        super::assert_storage_journal(&crate::testing::InMemoryMetadataStore::new()).await;
    }
}
