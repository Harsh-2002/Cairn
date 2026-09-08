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

fn plan(bucket: &BucketName, generation: &StorageToken) -> PlannedStorageWrite {
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

async fn claim(
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

async fn update(
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

async fn create_bucket(meta: &dyn MetadataStore, bucket: &BucketName) {
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
        assert_eq!(
            meta.submit(Mutation::RecoverStorageIntents {
                generation: restarted.clone(),
                limit: 1
            })
            .await
            .unwrap(),
            MutationOutcome::StorageRecovered(expected)
        );
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
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn in_memory_storage_journal_contract() {
        super::assert_storage_journal(&crate::testing::InMemoryMetadataStore::new()).await;
    }
}
