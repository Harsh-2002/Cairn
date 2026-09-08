//! The same held-accounting and bounded coverage contract runs on every backend and router.

use super::storage_contract::*;
use crate::blob::{
    StorageBaselineOptions, StorageBaselineProof, StorageClassificationCounts,
    StorageClassificationReport,
};
use crate::storage::io::StorageIoWatch;
use crate::storage::{PlannedStorageWrite, StorageMutation, StorageToken, StorageWriteTarget};
use crate::storage_baseline::*;
use crate::{
    BucketName, MetadataStore, Mutation, MutationOutcome, StoragePath, Timestamp, VersionId,
};
use std::sync::Arc;

fn proof(token: &StorageBaselineToken) -> StorageBaselineProof {
    // Metadata-only fixtures have no filesystem. Physical completion seams are exercised by the
    // owning blob tests; this test supplies their receipt to isolate exact Writer authorization.
    StorageBaselineProof::backend_verified(
        StorageClassificationReport::backend_completed(
            StorageBaselineOptions {
                token: token.clone(),
                batch_size: 128,
                root_artifacts: Vec::new(),
            },
            1,
            1,
            StorageClassificationCounts::default(),
            Arc::new(()),
        ),
        0,
    )
}

async fn transition(
    meta: &dyn MetadataStore,
    mutation: Mutation,
    expected: StorageBaselineTransition,
) {
    assert_eq!(
        meta.submit(mutation).await.unwrap(),
        MutationOutcome::StorageBaselineUpdated(expected)
    );
}

/// A partial/crashed baseline never forgives legacy bytes; only verified exact ownership can
/// authorize bounded release, and every fresh generation or restore invalidates that authority.
pub async fn assert_storage_baseline(meta: &dyn MetadataStore) {
    use StorageBaselineTransition::{AlreadyApplied, Applied, Blocked, Stale};
    let bucket = BucketName::parse("storage-baseline-contract").unwrap();
    create_bucket(meta, &bucket).await;
    meta.submit(Mutation::SetBucketQuota {
        bucket: bucket.clone(),
        quota_bytes: Some(30),
    })
    .await
    .unwrap();
    let old_generation = StorageToken::generate();
    meta.submit(Mutation::BeginStorageGeneration {
        generation: old_generation.clone(),
    })
    .await
    .unwrap();
    let upload = create_upload(meta, &bucket).await;
    let part = part_plan(&bucket, &old_generation, &upload);
    meta.submit(reserve_part(&part)).await.unwrap();
    meta.submit(publish_part(&part)).await.unwrap();
    for cleanup in claim(meta, &old_generation, 100, 0).await {
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
    let live_parts = meta.list_parts(&upload, 0, 100).await.unwrap();
    let legacy_ids = [StorageToken::generate(), StorageToken::generate()];
    for (index, id) in legacy_ids.iter().enumerate() {
        assert_eq!(
            meta.submit(Mutation::ReserveMultipartPart {
                upload_id: upload.clone(),
                part_number: index as u16 + 2,
                attempt_id: id.as_str().to_owned(),
                reserved_bytes: 10,
                max_parts_per_upload: 10000,
                now: Timestamp(0),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartReserved
        );
    }
    let interrupted = plan(&bucket, &old_generation);
    admit_object(meta, interrupted.plan()).await;
    let token = StorageBaselineToken {
        generation: StorageToken::generate(),
        baseline_id: StorageToken::generate(),
    };
    meta.submit(Mutation::BeginStorageGeneration {
        generation: token.generation.clone(),
    })
    .await
    .unwrap();
    transition(
        meta,
        Mutation::BeginStorageBaseline {
            token: token.clone(),
        },
        Applied,
    )
    .await;
    transition(
        meta,
        Mutation::BeginStorageBaseline {
            token: token.clone(),
        },
        AlreadyApplied,
    )
    .await;
    assert!(
        meta.storage_baseline_states()
            .await
            .unwrap()
            .iter()
            .all(|state| state.matches(&token) && !state.legacy_release_authorized)
    );
    let legacy = [
        Mutation::ReleaseMultipartReservation {
            upload_id: upload.clone(),
            attempt_id: legacy_ids[0].as_str().to_owned(),
        },
        Mutation::ReleaseMultipartCleanup {
            cleanup_id: "unrelated".into(),
        },
        Mutation::ReleaseMultipartUploadCleanups {
            upload_id: upload.clone(),
        },
        Mutation::RecoverMultipartStagingAccounting { limit: 1000 },
    ];
    for mutation in legacy {
        assert_eq!(
            meta.submit(mutation).await.unwrap(),
            MutationOutcome::MultipartAccountingHeld {
                baseline_id: token.baseline_id.clone()
            }
        );
    }
    assert!(
        meta.submit(reserve_part(&part_plan(
            &bucket,
            &token.generation,
            &upload
        )))
        .await
        .is_err()
    );
    let unknown_sibling = StoragePath::from_string(format!(
        ".staging/multipart/{upload}/00004-{}",
        StorageToken::generate().as_str()
    ));
    let sibling_owner = meta
        .storage_path_owners(std::slice::from_ref(&unknown_sibling))
        .await
        .unwrap();
    assert_eq!(sibling_owner[0].bucket, Some(bucket.clone()));
    assert!(!sibling_owner[0].authoritative);
    let orphan = StoragePath::from_string(format!(
        ".staging/{}.tmp",
        StorageToken::generate().as_str()
    ));
    let orphan_bucket = BucketName::parse("cairn-storage-orphans").unwrap();
    let duplicates = vec![orphan.clone(), orphan.clone()];
    assert!(
        meta.storage_path_owners(&duplicates)
            .await
            .unwrap()
            .iter()
            .all(|owner| owner.bucket.is_none())
    );
    assert_eq!(
        meta.submit(Mutation::ClassifyStorageBaseline {
            bucket: orphan_bucket.clone(),
            token: token.clone(),
            paths: duplicates.clone()
        })
        .await
        .unwrap(),
        MutationOutcome::StorageBaselineClassified(vec![
            StorageBaselineDisposition::CleanupRecorded;
            2
        ])
    );
    assert_eq!(
        meta.submit(Mutation::ClassifyStorageBaseline {
            bucket: orphan_bucket.clone(),
            token: token.clone(),
            paths: duplicates
        })
        .await
        .unwrap(),
        MutationOutcome::StorageBaselineClassified(vec![
            StorageBaselineDisposition::CleanupRecorded;
            2
        ])
    );
    assert_eq!(
        meta.submit(Mutation::ClassifyStorageBaseline {
            bucket: bucket.clone(),
            token: token.clone(),
            paths: vec![
                part.final_path().unwrap().clone(),
                interrupted.plan().paths[0].path.clone()
            ]
        })
        .await
        .unwrap(),
        MutationOutcome::StorageBaselineClassified(vec![
            StorageBaselineDisposition::Authoritative,
            StorageBaselineDisposition::IntentOwned
        ])
    );
    assert!(
        meta.storage_path_owners(&vec![orphan.clone(); 129])
            .await
            .is_err()
    );
    transition(
        meta,
        Mutation::AuthorizeStorageBaselineRelease {
            proof: proof(&token),
        },
        Blocked,
    )
    .await;
    let claimed = claim(meta, &token.generation, 100, 0).await;
    assert_eq!(claimed.len(), 1);
    assert!(
        meta.storage_baseline_pending().await.unwrap().exact_debt,
        "a future lease still blocks proof"
    );
    let (mut watch, lease) = StorageIoWatch::new(
        interrupted.plan().attempt.clone(),
        old_generation,
        Arc::new(()),
    );
    drop(lease);
    update(
        meta,
        &bucket,
        StorageMutation::ResolveRecovered {
            current_generation: token.generation.clone(),
            quiescence: watch.quiescent().await,
        },
        true,
    )
    .await;
    for cleanup in claimed {
        update(
            meta,
            &orphan_bucket,
            StorageMutation::FinishCleanup {
                cleanup,
                now: Timestamp(1),
            },
            true,
        )
        .await;
    }
    let recovered = claim(meta, &token.generation, 100, 0).await;
    assert_eq!(recovered.len(), 3);
    for cleanup in recovered {
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
    assert!(
        !meta
            .storage_baseline_pending()
            .await
            .unwrap()
            .native_pending()
    );
    transition(
        meta,
        Mutation::AuthorizeStorageBaselineRelease {
            proof: proof(&token),
        },
        Applied,
    )
    .await;
    transition(
        meta,
        Mutation::CompleteStorageBaseline {
            token: token.clone(),
            completed_at: Timestamp(2),
        },
        Blocked,
    )
    .await;
    assert_eq!(
        meta.submit(Mutation::FinalizeStorageBaselineLegacy {
            token: token.clone(),
            limit: 1
        })
        .await
        .unwrap(),
        MutationOutcome::StorageBaselineLegacyPage {
            released: 1,
            remaining: true
        }
    );
    assert!(
        meta.storage_baseline_states()
            .await
            .unwrap()
            .iter()
            .all(|state| state.matches(&token) && state.legacy_release_authorized)
    );
    let restarted = StorageToken::generate();
    meta.submit(Mutation::BeginStorageGeneration {
        generation: restarted.clone(),
    })
    .await
    .unwrap();
    assert!(
        meta.storage_baseline_states()
            .await
            .unwrap()
            .iter()
            .all(|state| state.legacy_accounting_hold
                && state.baseline_id.as_ref() == Some(&token.baseline_id)
                && !state.legacy_release_authorized)
    );
    transition(
        meta,
        Mutation::AuthorizeStorageBaselineRelease {
            proof: proof(&token),
        },
        Stale,
    )
    .await;
    transition(
        meta,
        Mutation::FinalizeStorageBaselineLegacy {
            token: token.clone(),
            limit: 1,
        },
        Stale,
    )
    .await;
    let restored = StorageToken::generate();
    meta.submit(Mutation::PrepareStorageRestore {
        generation: restored.clone(),
    })
    .await
    .unwrap();
    assert!(
        meta.storage_baseline_states()
            .await
            .unwrap()
            .iter()
            .all(|state| state.legacy_accounting_hold
                && state.baseline_id.as_ref() == Some(&token.baseline_id)
                && state.generation.as_ref() == Some(&restored)
                && !state.legacy_release_authorized)
    );
    let resumed = StorageBaselineToken {
        generation: restored,
        baseline_id: StorageToken::generate(),
    };
    transition(
        meta,
        Mutation::BeginStorageBaseline {
            token: resumed.clone(),
        },
        Applied,
    )
    .await;
    transition(
        meta,
        Mutation::AuthorizeStorageBaselineRelease {
            proof: proof(&resumed),
        },
        Applied,
    )
    .await;
    assert_eq!(
        meta.submit(Mutation::FinalizeStorageBaselineLegacy {
            token: resumed.clone(),
            limit: 1
        })
        .await
        .unwrap(),
        MutationOutcome::StorageBaselineLegacyPage {
            released: 1,
            remaining: false
        }
    );
    transition(
        meta,
        Mutation::CompleteStorageBaseline {
            token: resumed.clone(),
            completed_at: Timestamp(3),
        },
        Applied,
    )
    .await;
    transition(
        meta,
        Mutation::CompleteStorageBaseline {
            token: resumed.clone(),
            completed_at: Timestamp(3),
        },
        AlreadyApplied,
    )
    .await;
    assert!(
        meta.storage_baseline_states()
            .await
            .unwrap()
            .iter()
            .all(|state| !state.legacy_accounting_hold
                && !state.legacy_release_authorized
                && state.baseline_id.is_none()
                && state.coverage_identity.as_ref() == Some(&resumed.baseline_id)
                && state.completed_at == Some(Timestamp(3)))
    );
    assert_eq!(meta.list_parts(&upload, 0, 100).await.unwrap(), live_parts);
    assert!(!meta.storage_baseline_pending().await.unwrap().any());
    assert!(matches!(
        meta.submit(reserve_part(&part_plan(
            &bucket,
            &resumed.generation,
            &upload
        )))
        .await
        .unwrap(),
        MutationOutcome::StorageAdmission(crate::storage::StorageAdmission::Granted(_))
    ));
}

/// Coverage walks every historical object and live part with bounded, strictly advancing pages.
pub async fn assert_storage_baseline_authority(meta: &dyn MetadataStore) {
    let generation = StorageToken::generate();
    meta.submit(Mutation::BeginStorageGeneration {
        generation: generation.clone(),
    })
    .await
    .unwrap();
    let mut expected = 0;
    let mut historical = None;
    for index in 0..7 {
        let bucket = BucketName::parse(&format!("baseline-authority-{index}")).unwrap();
        create_bucket(meta, &bucket).await;
        meta.submit(Mutation::SetVersioning {
            bucket: bucket.clone(),
            state: crate::VersioningState::Enabled,
        })
        .await
        .unwrap();
        let owner = meta.get_bucket(&bucket).await.unwrap().unwrap().owner_id;
        for _ in 0..2 {
            let plan = PlannedStorageWrite::new(
                bucket.clone(),
                generation.clone(),
                StorageWriteTarget::Object {
                    key: crate::ObjectKey::parse("history/key").unwrap(),
                    version_id: VersionId::generate(),
                    row_id: StorageToken::generate().as_str().to_owned(),
                },
            )
            .unwrap();
            let row = object_row(plan.plan(), owner.clone());
            historical.get_or_insert(row.id.clone());
            admit_object(meta, plan.plan()).await;
            meta.submit(Mutation::PublishStorageWrite {
                plan: Box::new(plan.plan().clone()),
                operation: Box::new(put(row)),
            })
            .await
            .unwrap();
            expected += 1;
        }
        let upload = create_upload(meta, &bucket).await;
        let part = part_plan(&bucket, &generation, &upload);
        meta.submit(reserve_part(&part)).await.unwrap();
        meta.submit(publish_part(&part)).await.unwrap();
        expected += 1;
    }
    let mut cursor = None;
    let mut found = 0;
    let mut found_history = false;
    let mut paths = std::collections::BTreeSet::new();
    loop {
        let page = meta
            .enumerate_storage_authority(cursor.as_ref(), 2)
            .await
            .unwrap();
        assert!(page.items.len() <= 2);
        for item in page.items {
            found += 1;
            let path = match item {
                StorageAuthority::Object(row) => {
                    if Some(&row.id) == historical.as_ref() {
                        assert!(!row.is_latest);
                        found_history = true;
                    }
                    row.storage_path.unwrap()
                }
                StorageAuthority::Part { part, .. } => part.storage_path,
            };
            assert!(paths.insert(path));
        }
        let Some(next) = page.next else {
            break;
        };
        assert_ne!(cursor.as_ref(), Some(&next));
        cursor = Some(next);
    }
    assert_eq!(found, expected);
    assert!(found_history);
}

/// Classification preserves native alias debt identity and already-issued cleanup leases.
pub async fn assert_storage_baseline_native_aliases(meta: &dyn MetadataStore) {
    let bucket = BucketName::parse("baseline-native-aliases").unwrap();
    create_bucket(meta, &bucket).await;
    meta.submit(Mutation::SetBucketQuota {
        bucket: bucket.clone(),
        quota_bytes: Some(20),
    })
    .await
    .unwrap();
    let token = StorageBaselineToken {
        generation: StorageToken::generate(),
        baseline_id: StorageToken::generate(),
    };
    meta.submit(Mutation::BeginStorageGeneration {
        generation: token.generation.clone(),
    })
    .await
    .unwrap();
    let upload = create_upload(meta, &bucket).await;
    let first = part_plan(&bucket, &token.generation, &upload);
    let second = part_plan(&bucket, &token.generation, &upload);
    for part in [&first, &second] {
        meta.submit(reserve_part(part)).await.unwrap();
        meta.submit(publish_part(part)).await.unwrap();
    }
    assert!(matches!(
        meta.submit(reserve_part(&part_plan(
            &bucket,
            &token.generation,
            &upload
        )))
        .await,
        Err(crate::MetaError::QuotaExceeded)
    ));
    let claims = claim(meta, &token.generation, 100, 0).await;
    assert_eq!(claims.len(), 3);
    assert_eq!(
        claims
            .iter()
            .filter(|row| row.quota_debt_id.is_some())
            .count(),
        2
    );
    transition(
        meta,
        Mutation::BeginStorageBaseline {
            token: token.clone(),
        },
        StorageBaselineTransition::Applied,
    )
    .await;
    let paths = claims.iter().map(|row| row.path.clone()).collect();
    assert_eq!(
        meta.submit(Mutation::ClassifyStorageBaseline {
            bucket: bucket.clone(),
            token: token.clone(),
            paths
        })
        .await
        .unwrap(),
        MutationOutcome::StorageBaselineClassified(vec![
            StorageBaselineDisposition::CleanupRecorded;
            3
        ])
    );
    assert!(
        claim(meta, &token.generation, 100, 1).await.is_empty(),
        "classification must not rewrite a native quota owner or invalidate its lease"
    );
    let native: Vec<_> = claims
        .iter()
        .filter(|row| row.quota_debt_id.is_some())
        .cloned()
        .collect();
    update(
        meta,
        &bucket,
        StorageMutation::FinishCleanup {
            cleanup: native[0].clone(),
            now: Timestamp(1),
        },
        true,
    )
    .await;
    assert!(
        meta.storage_baseline_pending()
            .await
            .unwrap()
            .native_quota_debt,
        "one remaining native alias still owns the sole charge"
    );
    update(
        meta,
        &bucket,
        StorageMutation::FinishCleanup {
            cleanup: native[1].clone(),
            now: Timestamp(1),
        },
        true,
    )
    .await;
    assert!(
        !meta
            .storage_baseline_pending()
            .await
            .unwrap()
            .native_quota_debt
    );
    for cleanup in claims.into_iter().filter(|row| row.quota_debt_id.is_none()) {
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
    transition(
        meta,
        Mutation::AuthorizeStorageBaselineRelease {
            proof: proof(&token),
        },
        StorageBaselineTransition::Applied,
    )
    .await;
    transition(
        meta,
        Mutation::CompleteStorageBaseline {
            token: token.clone(),
            completed_at: Timestamp(2),
        },
        StorageBaselineTransition::Applied,
    )
    .await;
    assert_eq!(
        meta.list_parts(&upload, 0, 100).await.unwrap().items[0].storage_path,
        *second.final_path().unwrap()
    );
    assert!(matches!(
        meta.submit(reserve_part(&part_plan(
            &bucket,
            &token.generation,
            &upload
        )))
        .await
        .unwrap(),
        MutationOutcome::StorageAdmission(crate::storage::StorageAdmission::Granted(_))
    ));
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn in_memory_storage_baseline_contract() {
        super::assert_storage_baseline(&crate::testing::InMemoryMetadataStore::new()).await;
    }
    #[tokio::test]
    async fn in_memory_storage_baseline_authority() {
        super::assert_storage_baseline_authority(&crate::testing::InMemoryMetadataStore::new())
            .await;
    }
    #[tokio::test]
    async fn in_memory_storage_baseline_native_aliases() {
        super::assert_storage_baseline_native_aliases(&crate::testing::InMemoryMetadataStore::new()).await;
    }
}
