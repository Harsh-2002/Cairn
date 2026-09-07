//! Shared journal ownership/retention acceptance tests for every metadata backend.
use crate::id::{BucketName, ObjectKey, ReplicationClaimToken, VersionId};
use crate::meta::{Mutation, MutationOutcome, OutboxEntry, ReplicationOp, ReplicationStatus};
use crate::replication_upload::{
    RemoteMultipartDestination, RemoteMultipartUpload, ReplicationUploadBatch,
    ReplicationUploadMutation as Op,
};
use crate::{MetadataStore, Timestamp};

async fn submit(meta: &dyn MetadataStore, bucket: &BucketName, operation: Op, expected: bool) {
    assert_eq!(
        meta.submit(Mutation::ReplicationUpload {
            bucket: bucket.clone(),
            operation
        })
        .await
        .unwrap(),
        MutationOutcome::ReplicationClaimUpdated { applied: expected }
    );
}
async fn claim(meta: &dyn MetadataStore, now: i64) -> ReplicationUploadBatch {
    let MutationOutcome::ReplicationUploadBatch(batch) = meta
        .submit(Mutation::ClaimReplicationUploadCleanup {
            limit: 100,
            now: Timestamp(now),
            lease_secs: 300,
        })
        .await
        .unwrap()
    else {
        panic!("cleanup batch expected")
    };
    batch
}
async fn origin(
    meta: &dyn MetadataStore,
    bucket: &BucketName,
    suffix: &str,
    now: i64,
) -> OutboxEntry {
    let id = format!("journal-{}-{suffix}", bucket.as_str());
    meta.submit(Mutation::EnqueueReplication(Box::new(OutboxEntry {
        id: id.clone(),
        bucket: bucket.clone(),
        key: ObjectKey::parse("journal-key").unwrap(),
        version_id: VersionId::generate(),
        operation: ReplicationOp::ObjectCreate,
        rule_id: "journal-rule".to_owned(),
        target_arn: Some("journal-target".to_owned()),
        attempts: 0,
        next_attempt_at: Timestamp(now),
        status: ReplicationStatus::Pending,
        last_error: None,
        priority: 0,
        lease_until: None,
        enqueued_at: Timestamp(0),
        claim_token: None,
    })))
    .await
    .unwrap();
    meta.claim_replication_batch(1000, Timestamp(now))
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.id == id)
        .unwrap()
}
fn upload(entry: &OutboxEntry) -> RemoteMultipartUpload {
    RemoteMultipartUpload {
        id: format!("upload-{}", entry.id),
        outbox_id: entry.id.clone(),
        origin_token: entry.claim_token.clone().unwrap(),
        destination: RemoteMultipartDestination {
            bucket: entry.bucket.clone(),
            key: entry.key.clone(),
            target_arn: entry.target_arn.clone(),
            endpoint: "https://destination.example".to_owned(),
            destination_bucket: "destination".to_owned(),
        },
        upload_id: None,
        cleanup_token: None,
        lease_until: None,
        next_attempt_at: entry.next_attempt_at,
        orphan_reported: false,
        last_error: None,
    }
}
async fn finish_origin(meta: &dyn MetadataStore, entry: &OutboxEntry, now: i64) {
    assert_eq!(
        meta.submit(Mutation::MarkReplicationDone {
            id: entry.id.clone(),
            claim_token: entry.claim_token.clone().unwrap(),
            now: Timestamp(now),
        })
        .await
        .unwrap(),
        MutationOutcome::ReplicationClaimUpdated { applied: true }
    );
    meta.submit(Mutation::PruneReplicationOutbox { before_ms: 1 })
        .await
        .unwrap();
}

/// Prove exact cleanup leases, independent retention, missing receipts and late receipt recovery.
pub async fn assert_replication_upload_journal(meta: &dyn MetadataStore, bucket: &BucketName) {
    let entry = origin(meta, bucket, "known", 0).await;
    let row = upload(&entry);
    let mut invalid = row.clone();
    invalid.id.push_str("-invalid");
    invalid.last_error = Some("not an initial state".to_owned());
    assert!(
        meta.submit(Mutation::ReplicationUpload {
            bucket: bucket.clone(),
            operation: Op::Begin {
                upload: Box::new(invalid),
                now: Timestamp(0)
            }
        })
        .await
        .is_err()
    );
    let mut stale = row.clone();
    stale.id.push_str("-stale");
    stale.origin_token = ReplicationClaimToken::generate();
    submit(
        meta,
        bucket,
        Op::Begin {
            upload: Box::new(stale),
            now: Timestamp(0),
        },
        false,
    )
    .await;
    submit(
        meta,
        bucket,
        Op::Begin {
            upload: Box::new(row.clone()),
            now: Timestamp(0),
        },
        true,
    )
    .await;
    submit(
        meta,
        bucket,
        Op::RecordUploadId {
            id: row.id.clone(),
            origin_token: row.origin_token.clone(),
            upload_id: "remote-known".to_owned(),
            now: Timestamp(1),
        },
        true,
    )
    .await;
    assert!(
        claim(meta, 2).await.uploads.is_empty(),
        "a live delivery's upload cannot be aborted"
    );
    meta.submit(Mutation::RecoverClaimedReplication)
        .await
        .unwrap();
    let newer = meta
        .claim_replication_batch(1000, Timestamp(3))
        .await
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.id == entry.id)
        .unwrap();
    assert_ne!(entry.claim_token, newer.claim_token);
    let a = claim(meta, 4).await.uploads.pop().unwrap();
    finish_origin(meta, &newer, 5).await;
    assert_eq!(
        a.upload_id.as_deref(),
        Some("remote-known"),
        "outbox pruning must retain cleanup"
    );
    assert!(claim(meta, 5).await.uploads.is_empty());
    let b = claim(meta, 300005).await.uploads.pop().unwrap();
    let token_a = a.cleanup_token.unwrap();
    let token_b = b.cleanup_token.unwrap();
    assert_ne!(token_a, token_b);
    submit(
        meta,
        bucket,
        Op::RenewCleanup {
            id: row.id.clone(),
            cleanup_token: token_a.clone(),
            now: Timestamp(300006),
            lease_secs: 300,
        },
        false,
    )
    .await;
    submit(
        meta,
        bucket,
        Op::SettleCleanup {
            id: row.id.clone(),
            cleanup_token: token_a,
            now: Timestamp(300006),
            retry_at: Timestamp(0),
            error: None,
        },
        false,
    )
    .await;
    submit(
        meta,
        bucket,
        Op::RenewCleanup {
            id: row.id.clone(),
            cleanup_token: token_b.clone(),
            now: Timestamp(600000),
            lease_secs: 300,
        },
        true,
    )
    .await;
    submit(
        meta,
        bucket,
        Op::SettleCleanup {
            id: row.id.clone(),
            cleanup_token: token_b,
            now: Timestamp(600001),
            retry_at: Timestamp(0),
            error: None,
        },
        true,
    )
    .await;
    assert!(claim(meta, 600002).await.uploads.is_empty());

    let entry = origin(meta, bucket, "unknown", 700000).await;
    let row = upload(&entry);
    submit(
        meta,
        bucket,
        Op::Begin {
            upload: Box::new(row.clone()),
            now: Timestamp(700000),
        },
        true,
    )
    .await;
    finish_origin(meta, &entry, 700001).await;
    let missing = claim(meta, 700002).await;
    assert_eq!(missing.orphaned, 1);
    assert!(missing.uploads.is_empty());
    assert_eq!(
        claim(meta, 700003).await.orphaned,
        0,
        "unknown initiation reported once, retained for late receipt"
    );
    submit(
        meta,
        bucket,
        Op::RecordUploadId {
            id: row.id.clone(),
            origin_token: row.origin_token.clone(),
            upload_id: "late-receipt".to_owned(),
            now: Timestamp(700004),
        },
        false,
    )
    .await;
    let a = claim(meta, 700005).await.uploads.pop().unwrap();
    assert_eq!(a.upload_id.as_deref(), Some("late-receipt"));
    submit(
        meta,
        bucket,
        Op::SettleCleanup {
            id: row.id.clone(),
            cleanup_token: a.cleanup_token.unwrap(),
            now: Timestamp(700006),
            retry_at: Timestamp(701000),
            error: Some("target removed".to_owned()),
        },
        true,
    )
    .await;
    assert!(claim(meta, 700007).await.uploads.is_empty());
    let b = claim(meta, 701000).await.uploads.pop().unwrap();
    assert_eq!(b.last_error.as_deref(), Some("target removed"));
    submit(
        meta,
        bucket,
        Op::SettleCleanup {
            id: row.id.clone(),
            cleanup_token: b.cleanup_token.unwrap(),
            now: Timestamp(701001),
            retry_at: Timestamp(0),
            error: None,
        },
        true,
    )
    .await;
    assert!(claim(meta, 701002).await.uploads.is_empty());

    let removed = BucketName::parse(&format!("removed-{}", bucket.as_str())).unwrap();
    meta.submit(Mutation::CreateBucket(Box::new(crate::bucket::Bucket {
        name: removed.clone(),
        owner_id: crate::id::UserId::generate(),
        created_at: Timestamp(0),
        versioning: crate::bucket::VersioningState::Unversioned,
        ownership_mode: crate::authz::OwnershipMode::BucketOwnerEnforced,
        region: "us-east-1".to_owned(),
        compression: None,
    })))
    .await
    .unwrap();
    let entry = origin(meta, &removed, "deleted-bucket", 800000).await;
    let row = upload(&entry);
    submit(
        meta,
        &removed,
        Op::Begin {
            upload: Box::new(row.clone()),
            now: Timestamp(800000),
        },
        true,
    )
    .await;
    submit(
        meta,
        &removed,
        Op::RecordUploadId {
            id: row.id.clone(),
            origin_token: row.origin_token.clone(),
            upload_id: "deleted-source-receipt".to_owned(),
            now: Timestamp(800001),
        },
        true,
    )
    .await;
    finish_origin(meta, &entry, 800002).await;
    meta.submit(Mutation::DeleteBucket(removed.clone()))
        .await
        .unwrap();
    let retained = claim(meta, 800003).await.uploads.pop().unwrap();
    assert_eq!(
        retained.destination.bucket, removed,
        "source bucket deletion preserves original routing identity"
    );
    let old_token = retained.cleanup_token.unwrap();
    meta.submit(Mutation::RecoverClaimedReplication)
        .await
        .unwrap();
    let recovered = claim(meta, 800004).await.uploads.pop().unwrap();
    assert_ne!(
        recovered.cleanup_token.as_ref(),
        Some(&old_token),
        "restart invalidates orphaned cleanup ownership immediately"
    );
    submit(
        meta,
        &removed,
        Op::SettleCleanup {
            id: row.id.clone(),
            cleanup_token: old_token,
            now: Timestamp(800004),
            retry_at: Timestamp(0),
            error: None,
        },
        false,
    )
    .await;
    submit(
        meta,
        &removed,
        Op::SettleCleanup {
            id: row.id,
            cleanup_token: recovered.cleanup_token.unwrap(),
            now: Timestamp(800004),
            retry_at: Timestamp(0),
            error: None,
        },
        true,
    )
    .await;
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn remote_upload_journal_double_preserves_retention_and_ownership() {
        let meta = crate::testing::InMemoryMetadataStore::new();
        super::assert_replication_upload_journal(
            &meta,
            &crate::BucketName::parse("journal-test").unwrap(),
        )
        .await;
    }
}
