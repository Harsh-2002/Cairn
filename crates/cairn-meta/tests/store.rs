//! Gate tests for the SQLite metadata store: group-commit savepoint isolation, conditional
//! write atomicity, versioning bookkeeping, listing pagination/delimiter correctness, and the
//! reconcile oracle.

use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::traits::{MetadataStore, ReconcileOracle};
use cairn_types::*;

fn row(
    bucket: &BucketName,
    key: &str,
    version: VersionId,
    etag: &str,
    with_blob: bool,
) -> ObjectVersionRow {
    ObjectVersionRow {
        id: uuid::Uuid::new_v4().simple().to_string(),
        bucket: bucket.clone(),
        key: ObjectKey::parse(key).unwrap(),
        version_id: version,
        is_latest: true,
        is_delete_marker: false,
        size_logical: 3,
        size_physical: 3,
        etag: ETag::from_string(etag.to_owned()),
        content_type: "text/plain".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: with_blob.then(|| StoragePath::generate(bucket)),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId::generate(),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: None,
        created_at: Timestamp(1),
        updated_at: Timestamp(1),
    }
}

fn put(row: ObjectVersionRow, pc: Precondition) -> Mutation {
    Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: pc,
        replication: Vec::new(),
    }
}

#[tokio::test]
async fn user_policy_round_trips() {
    let store = cairn_meta::open_in_memory().unwrap();
    let id = UserId::generate();
    store
        .submit(Mutation::CreateUser(Box::new(UserRecord {
            user: User {
                id: id.clone(),
                display_name: "alice".to_owned(),
                access_key_id: "cairn_alice".to_owned(),
                sigv4_access_key_id: None,
                role: Role::Member,
                is_active: true,
                quota_bytes: None,
                created_at: Timestamp(1),
                updated_at: Timestamp(1),
            },
            bearer_secret_hash: "h".to_owned(),
            sigv4_secret_ciphertext: None,
            sigv4_secret_nonce: None,
        })))
        .await
        .unwrap();
    // No policy initially.
    assert_eq!(store.get_user_policy(&id).await.unwrap(), None);
    // Set → read back the exact stored JSON.
    let doc = r#"{"Version":"2012-10-17","Statement":[]}"#.to_owned();
    store
        .submit(Mutation::SetUserPolicy {
            user_id: id.clone(),
            policy: Some(doc.clone()),
        })
        .await
        .unwrap();
    assert_eq!(store.get_user_policy(&id).await.unwrap(), Some(doc));
    // Clear → back to None.
    store
        .submit(Mutation::SetUserPolicy {
            user_id: id.clone(),
            policy: None,
        })
        .await
        .unwrap();
    assert_eq!(store.get_user_policy(&id).await.unwrap(), None);
    // An unknown user has no policy.
    assert_eq!(
        store.get_user_policy(&UserId::generate()).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn object_shares_round_trip_and_revoke() {
    let store = cairn_meta::open_in_memory().unwrap();
    let bucket = BucketName::parse("photos").unwrap();
    let key = ObjectKey::parse("a/b.jpg").unwrap();
    let row = ShareRow {
        token: "tok-abc".to_owned(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: Some(VersionId::from_string("v1".to_owned())),
        expires_at: None, // forever
        disposition: ShareDisposition::Attachment,
        filename: Some("download.jpg".to_owned()),
        created_by: UserId("admin".to_owned()),
        created_at: Timestamp(100),
        revoked_at: None,
    };
    store
        .submit(Mutation::CreateShare(Box::new(row.clone())))
        .await
        .unwrap();

    // Round-trips by token, preserving every field including the forever (None) expiry.
    let got = store.get_share("tok-abc").await.unwrap().unwrap();
    assert_eq!(got, row);
    // Listed per-key and per-bucket.
    assert_eq!(
        store.list_shares(&bucket, Some(&key)).await.unwrap().len(),
        1
    );
    assert_eq!(store.list_shares(&bucket, None).await.unwrap().len(), 1);
    // Unknown token → None.
    assert!(store.get_share("nope").await.unwrap().is_none());

    // Revoke sets revoked_at; the row remains readable (the resolver checks the flag).
    store
        .submit(Mutation::RevokeShare {
            token: "tok-abc".to_owned(),
            now: Timestamp(200),
        })
        .await
        .unwrap();
    let revoked = store.get_share("tok-abc").await.unwrap().unwrap();
    assert_eq!(revoked.revoked_at, Some(Timestamp(200)));

    // Revoke is idempotent: a second revoke does not move the timestamp.
    store
        .submit(Mutation::RevokeShare {
            token: "tok-abc".to_owned(),
            now: Timestamp(999),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .get_share("tok-abc")
            .await
            .unwrap()
            .unwrap()
            .revoked_at,
        Some(Timestamp(200))
    );
}

#[tokio::test]
async fn put_is_visible_only_after_commit() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let k = ObjectKey::parse("k").unwrap();
    store
        .submit(put(
            row(&b, "k", VersionId::null(), "e1", true),
            Precondition::default(),
        ))
        .await
        .unwrap();
    // The submit future only resolved after the commit, so the row is immediately visible.
    let got = store.current_version(&b, &k).await.unwrap().unwrap();
    assert_eq!(got.etag.as_str(), "e1");
}

#[tokio::test]
async fn conditional_write_atomic() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("bkt").unwrap();
    store
        .submit(put(
            row(&b, "k", VersionId::null(), "e1", true),
            Precondition::default(),
        ))
        .await
        .unwrap();

    // If-None-Match: * must fail now that the object exists.
    let err = store
        .submit(put(
            row(&b, "k", VersionId::null(), "e2", true),
            Precondition {
                if_match: None,
                if_none_match: Some(IfNoneMatch::Any),
            },
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, MetaError::PreconditionFailed));

    // If-Match the current etag succeeds.
    store
        .submit(put(
            row(&b, "k", VersionId::null(), "e3", true),
            Precondition {
                if_match: Some(ETag::from_string("e1".into())),
                if_none_match: None,
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .current_version(&b, &ObjectKey::parse("k").unwrap())
            .await
            .unwrap()
            .unwrap()
            .etag
            .as_str(),
        "e3"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn group_commit_isolates_failed_mutations() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("bkt").unwrap();
    // Seed one object so the failing precondition has something to collide with.
    store
        .submit(put(
            row(&b, "exists", VersionId::null(), "e", true),
            Precondition::default(),
        ))
        .await
        .unwrap();

    // Fire many concurrent submits: 49 distinct successful puts + 1 doomed conditional put.
    let mut handles = Vec::new();
    for i in 0..49 {
        let s = store.clone();
        let bb = b.clone();
        handles.push(tokio::spawn(async move {
            s.submit(put(
                row(&bb, &format!("k{i:03}"), VersionId::null(), "e", true),
                Precondition::default(),
            ))
            .await
        }));
    }
    let s = store.clone();
    let bb = b.clone();
    let doomed = tokio::spawn(async move {
        s.submit(put(
            row(&bb, "exists", VersionId::null(), "e2", true),
            Precondition {
                if_match: None,
                if_none_match: Some(IfNoneMatch::Any),
            },
        ))
        .await
    });

    for h in handles {
        h.await.unwrap().expect("distinct puts must all commit");
    }
    assert!(matches!(
        doomed.await.unwrap(),
        Err(MetaError::PreconditionFailed)
    ));

    // The doomed mutation's rollback must not have touched its batch-mates: exactly 50 objects.
    let counts = store.aggregate_counts().await.unwrap();
    assert_eq!(
        counts.objects, 50,
        "all successful puts committed, the failed one isolated"
    );
}

#[tokio::test]
async fn versioning_history_and_promotion() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let k = ObjectKey::parse("doc").unwrap();
    let v1 = VersionId::from_string("00000001".into());
    let v2 = VersionId::from_string("00000002".into());
    let v3 = VersionId::from_string("00000003".into());
    for v in [&v1, &v2, &v3] {
        store
            .submit(put(
                row(&b, "doc", v.clone(), "e", true),
                Precondition::default(),
            ))
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .current_version(&b, &k)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        v3
    );

    let del = store
        .submit(Mutation::DeleteVersion {
            bucket: b.clone(),
            key: k.clone(),
            version_id: v3.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        del,
        MutationOutcome::Deleted {
            promoted_latest: true,
            ..
        }
    ));
    assert_eq!(
        store
            .current_version(&b, &k)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        v2
    );

    let all = store
        .list_versions(
            &b,
            &ListQuery {
                limit: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(all.items.len(), 2);
}

#[tokio::test]
async fn listing_prefix_delimiter_and_pagination() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("bkt").unwrap();
    for k in ["a/1", "a/2", "a/3", "b/1", "c"] {
        store
            .submit(put(
                row(&b, k, VersionId::null(), "e", true),
                Precondition::default(),
            ))
            .await
            .unwrap();
    }

    // Delimiter groups a/* and b/* into common prefixes; c is a direct object.
    let page = store
        .list_current(
            &b,
            &ListQuery {
                delimiter: Some("/".into()),
                limit: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.common_prefixes, vec!["a/".to_owned(), "b/".to_owned()]);
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].key.as_str(), "c");

    // Prefix a/ with no delimiter returns the three objects.
    let page = store
        .list_current(
            &b,
            &ListQuery {
                prefix: Some("a/".into()),
                limit: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 3);

    // Pagination: page size 2 across the full keyspace concatenates to the full listing.
    let mut all = Vec::new();
    let mut cursor = None;
    loop {
        let page = store
            .list_current(
                &b,
                &ListQuery {
                    cursor: cursor.clone(),
                    limit: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        all.extend(page.items.iter().map(|i| i.key.as_str().to_owned()));
        if !page.truncated {
            break;
        }
        cursor = page.next_cursor.clone();
        assert!(cursor.is_some());
    }
    assert_eq!(all, vec!["a/1", "a/2", "a/3", "b/1", "c"]);
}

#[tokio::test]
async fn reconcile_oracle_reports_membership() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let r = row(&b, "k", VersionId::null(), "e", true);
    let live_path = r.storage_path.clone().unwrap();
    store.submit(put(r, Precondition::default())).await.unwrap();

    let oracle = store.reconcile_oracle();
    let orphan = StoragePath::from_string("b/orphan".into());
    let answers = oracle.live_blobs(&[live_path, orphan]).await.unwrap();
    assert_eq!(answers, vec![true, false]);
}

#[tokio::test]
async fn checkpoint_truncates_wal_and_reports_frames() {
    // The checkpointer needs an on-disk database so a real -wal sidecar exists to stat/truncate.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("meta.sqlite");
    let store = cairn_meta::open(&db, &cairn_meta::OpenOptions::default()).unwrap();
    let b = BucketName::parse("bkt").unwrap();

    // Write enough versions to grow the WAL.
    for i in 0..64 {
        store
            .submit(put(
                row(&b, &format!("k{i:03}"), VersionId::null(), "e", true),
                Precondition::default(),
            ))
            .await
            .unwrap();
    }

    // The writes left frames in the -wal file.
    let before = store.wal_size_bytes().await.unwrap();
    assert!(before > 0, "writes should have grown the WAL");

    // A truncating checkpoint runs on the writer thread without error and reports its frame
    // counts. SQLite's implicit PASSIVE autocheckpoint may already have moved some frames into
    // the database (without shrinking the file), so `log_frames` is the count still present at
    // checkpoint time; what we require is that the run completes uncontended and the counts are
    // internally consistent.
    let stats = store.checkpoint().await.unwrap();
    assert!(
        !stats.busy,
        "the single-writer design means the checkpoint is never blocked, got {stats:?}"
    );
    assert!(
        stats.checkpointed_frames <= stats.log_frames,
        "cannot checkpoint more frames than the WAL holds, got {stats:?}"
    );

    // TRUNCATE resets the -wal file (the F-3 fix: the file would otherwise grow unbounded), so
    // it is now strictly smaller than before — typically zero.
    let after = store.wal_size_bytes().await.unwrap();
    assert!(
        after < before,
        "truncating checkpoint should shrink the WAL: before={before} after={after}"
    );

    // A second checkpoint immediately after a truncate finds an (almost) empty WAL and still
    // reports frames cleanly, proving the control path stays responsive.
    let again = store.checkpoint().await.unwrap();
    assert!(!again.busy, "follow-up checkpoint is also uncontended");

    // The store is still fully functional after the checkpoint.
    store
        .submit(put(
            row(&b, "after", VersionId::null(), "e", true),
            Precondition::default(),
        ))
        .await
        .unwrap();
    let counts = store.aggregate_counts().await.unwrap();
    assert_eq!(counts.objects, 65);
}

#[tokio::test]
async fn wal_size_is_zero_for_in_memory_store() {
    let store = cairn_meta::open_in_memory().unwrap();
    // An in-memory store has no -wal sidecar; the size is reported as zero rather than erroring.
    assert_eq!(store.wal_size_bytes().await.unwrap(), 0);
    // Checkpoint is still safe to call (it is a no-op against the in-memory journal).
    store.checkpoint().await.unwrap();
}

#[tokio::test]
async fn create_bucket_conflict() {
    let store = cairn_meta::open_in_memory().unwrap();
    let bucket = Bucket {
        name: BucketName::parse("dup").unwrap(),
        owner_id: UserId::generate(),
        created_at: Timestamp(1),
        versioning: VersioningState::Unversioned,
        ownership_mode: OwnershipMode::BucketOwnerEnforced,
        region: "us-east-1".to_owned(),
        compression: None,
    };
    store
        .submit(Mutation::CreateBucket(Box::new(bucket.clone())))
        .await
        .unwrap();
    let err = store
        .submit(Mutation::CreateBucket(Box::new(bucket)))
        .await
        .unwrap_err();
    assert!(matches!(err, MetaError::Conflict));
}

/// Create a versioning-enabled bucket so quota/ACL fixtures have a parent row to read/update.
fn bucket(name: &str) -> Bucket {
    Bucket {
        name: BucketName::parse(name).unwrap(),
        owner_id: UserId::generate(),
        created_at: Timestamp(1),
        versioning: VersioningState::Enabled,
        ownership_mode: OwnershipMode::BucketOwnerEnforced,
        region: "us-east-1".to_owned(),
        compression: None,
    }
}

/// Commit one object version carrying a replication outbox entry, returning the entry id.
async fn plant_outbox(
    store: &cairn_meta::SqliteMetadataStore,
    b: &BucketName,
    key: &str,
    version: VersionId,
    id: &str,
) {
    let entry = OutboxEntry {
        enqueued_at: Timestamp(0),
        id: id.to_owned(),
        bucket: b.clone(),
        key: ObjectKey::parse(key).unwrap(),
        version_id: version.clone(),
        operation: ReplicationOp::ObjectCreate,
        rule_id: "rule-1".to_owned(),
        target_arn: None,
        attempts: 0,
        next_attempt_at: Timestamp(0),
        status: ReplicationStatus::Pending,
        last_error: None,
        priority: 0,
        lease_until: None,
    };
    store
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row(b, key, version, "e", true)),
            precondition: Precondition::default(),
            replication: vec![entry],
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn list_failed_replication_reports_terminal_entries_only() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("bkt").unwrap();
    store
        .submit(Mutation::CreateBucket(Box::new(bucket("bkt"))))
        .await
        .unwrap();

    let v1 = VersionId::from_string("00000001".into());
    let v2 = VersionId::from_string("00000002".into());
    plant_outbox(&store, &b, "k1", v1.clone(), "pending-1").await;
    plant_outbox(&store, &b, "k2", v2.clone(), "doomed-1").await;

    // Nothing terminal yet.
    assert!(store.list_failed_replication(100).await.unwrap().is_empty());

    // Mark one entry terminally failed (next_attempt_at = None).
    store
        .submit(Mutation::MarkReplicationFailed {
            id: "doomed-1".to_owned(),
            error: "destination unreachable".to_owned(),
            next_attempt_at: None,
        })
        .await
        .unwrap();

    let failed = store.list_failed_replication(100).await.unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].id, "doomed-1");
    assert_eq!(failed[0].version_id, v2);
    assert_eq!(failed[0].attempts, 1);
    assert_eq!(
        failed[0].last_error.as_deref(),
        Some("destination unreachable")
    );

    // A retryable failure (next_attempt_at = Some) stays pending and out of the failed list.
    store
        .submit(Mutation::MarkReplicationFailed {
            id: "pending-1".to_owned(),
            error: "transient".to_owned(),
            next_attempt_at: Some(Timestamp(60_000)),
        })
        .await
        .unwrap();
    let failed = store.list_failed_replication(100).await.unwrap();
    assert_eq!(failed.len(), 1, "retryable entry is not terminal");
    assert_eq!(failed[0].id, "doomed-1");

    // The limit is honoured.
    assert!(store.list_failed_replication(0).await.unwrap().is_empty());
}

#[tokio::test]
async fn replication_counts_aggregates_by_status_and_target() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("rcbkt").unwrap();
    store
        .submit(Mutation::CreateBucket(Box::new(bucket("rcbkt"))))
        .await
        .unwrap();

    // (id, key, version, target, enqueued_at): three pending (X@300, X@100, Y@200) and one to X
    // that we then fail (@400). The fan-out routing key is the target ARN.
    for (id, key, vid, target, enq) in [
        ("x1", "k1", "00000001", "arn:X", 300_i64),
        ("x2", "k2", "00000002", "arn:X", 100),
        ("y1", "k3", "00000003", "arn:Y", 200),
        ("xf", "k4", "00000004", "arn:X", 400),
    ] {
        let v = VersionId::from_string(vid.to_owned());
        let entry = OutboxEntry {
            enqueued_at: Timestamp(enq),
            id: id.to_owned(),
            bucket: b.clone(),
            key: ObjectKey::parse(key).unwrap(),
            version_id: v.clone(),
            operation: ReplicationOp::ObjectCreate,
            rule_id: "r".to_owned(),
            target_arn: Some(target.to_owned()),
            attempts: 0,
            next_attempt_at: Timestamp(0),
            status: ReplicationStatus::Pending,
            last_error: None,
            priority: 0,
            lease_until: None,
        };
        store
            .submit(Mutation::PutObjectVersion {
                row: Box::new(row(&b, key, v, "e", true)),
                precondition: Precondition::default(),
                replication: vec![entry],
            })
            .await
            .unwrap();
    }
    store
        .submit(Mutation::MarkReplicationFailed {
            id: "xf".to_owned(),
            error: "x".to_owned(),
            next_attempt_at: None,
        })
        .await
        .unwrap();

    let c = store.replication_counts(Some(&b)).await.unwrap();
    assert_eq!(c.pending, 3);
    assert_eq!(c.failed, 1);
    assert_eq!(c.completed, 0);
    // Oldest still-pending enqueue time: min over pending (100), ignoring the failed entry (400).
    assert_eq!(c.oldest_pending_at_ms, 100);
    let mut by: Vec<(Option<&str>, u64, u64)> = c
        .by_target
        .iter()
        .map(|t| (t.target_arn.as_deref(), t.pending, t.failed))
        .collect();
    by.sort();
    assert_eq!(by, vec![(Some("arn:X"), 2, 1), (Some("arn:Y"), 1, 0)]);

    // Store-wide (None) matches here since there is a single bucket.
    let all = store.replication_counts(None).await.unwrap();
    assert_eq!((all.pending, all.failed), (3, 1));
}

#[tokio::test]
async fn prune_reclaims_old_terminal_entries_only() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("prunebkt").unwrap();
    store
        .submit(Mutation::CreateBucket(Box::new(bucket("prunebkt"))))
        .await
        .unwrap();
    let mk = |id: &str, status: ReplicationStatus, enq: i64| OutboxEntry {
        enqueued_at: Timestamp(enq),
        id: id.to_owned(),
        bucket: b.clone(),
        key: ObjectKey::parse("k").unwrap(),
        version_id: VersionId::from_string(id.to_owned()),
        operation: ReplicationOp::ObjectCreate,
        rule_id: "r".to_owned(),
        target_arn: None,
        attempts: 0,
        next_attempt_at: Timestamp(0),
        status,
        last_error: None,
        priority: 0,
        lease_until: None,
    };
    for e in [
        mk("old-done", ReplicationStatus::Completed, 1000),
        mk("old-fail", ReplicationStatus::Failed, 1000),
        mk("new-fail", ReplicationStatus::Failed, 9000),
        mk("old-pending", ReplicationStatus::Pending, 1000),
    ] {
        store
            .submit(Mutation::EnqueueReplication(Box::new(e)))
            .await
            .unwrap();
    }
    // Reclaim terminal rows enqueued before t=5000.
    store
        .submit(Mutation::PruneReplicationOutbox { before_ms: 5000 })
        .await
        .unwrap();

    let counts = store.replication_counts(Some(&b)).await.unwrap();
    assert_eq!(counts.completed, 0, "old completed pruned");
    assert_eq!(counts.failed, 1, "old failed pruned; recent failed kept");
    assert_eq!(
        counts.pending, 1,
        "pending is outstanding work and never pruned"
    );
    let failed = store.list_failed_replication(10).await.unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].id, "new-fail");
}

#[tokio::test]
async fn defer_releases_claim_without_consuming_attempts() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("deferbkt").unwrap();
    store
        .submit(Mutation::CreateBucket(Box::new(bucket("deferbkt"))))
        .await
        .unwrap();
    let v = VersionId::from_string("00000001".into());
    plant_outbox(&store, &b, "k", v.clone(), "d1").await;

    // Claim the entry: it goes `claimed` under a lease, so it leaves the due (pending) set.
    let claimed = store
        .claim_replication_batch(10, Timestamp(1_000))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(
        store
            .list_due_replication(10, Timestamp(1_000))
            .await
            .unwrap()
            .is_empty()
    );

    // Defer it: released back to pending, re-scheduled, attempts untouched, error recorded.
    store
        .submit(Mutation::DeferReplication {
            id: "d1".to_owned(),
            next_attempt_at: Timestamp(5_000),
            last_error: Some("target unavailable: down".to_owned()),
        })
        .await
        .unwrap();

    // Not due yet at its re-check time minus one, then due at the scheduled time.
    assert!(
        store
            .list_due_replication(10, Timestamp(4_999))
            .await
            .unwrap()
            .is_empty()
    );
    let due = store
        .list_due_replication(10, Timestamp(5_000))
        .await
        .unwrap();
    assert_eq!(due.len(), 1, "the deferred entry is promptly re-claimable");
    assert_eq!(due[0].id, "d1");
    assert_eq!(due[0].status, ReplicationStatus::Pending);
    assert_eq!(
        due[0].attempts, 0,
        "a deferral never consumes the attempt budget"
    );
    assert_eq!(due[0].lease_until, None, "the claim lease was cleared");
    assert_eq!(
        due[0].last_error.as_deref(),
        Some("target unavailable: down")
    );
}

#[tokio::test]
async fn replica_preserves_id_and_orders_by_version_id() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("rvbkt").unwrap();
    store
        .submit(Mutation::CreateBucket(Box::new(bucket("rvbkt"))))
        .await
        .unwrap();
    let k = ObjectKey::parse("k").unwrap();
    let replica = |bk: &BucketName, key, vid: VersionId, etag| {
        let mut r = row(bk, key, vid, etag, true);
        r.replication_status = Some(ReplicationStatus::Replica);
        r
    };
    async fn version_count(store: &cairn_meta::SqliteMetadataStore, b: &BucketName) -> usize {
        store
            .list_versions(
                b,
                &ListQuery {
                    limit: 100,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .items
            .len()
    }
    let v1 = VersionId::from_string("00000001".into());
    let v2 = VersionId::from_string("00000002".into());
    let v3 = VersionId::from_string("00000003".into());

    // A normal local write of v2 is the latest.
    store
        .submit(put(
            row(&b, "k", v2.clone(), "e2", true),
            Precondition::default(),
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .current_version(&b, &k)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        v2
    );

    // A replica carrying an OLDER id is preserved as a version but does NOT demote the newer latest.
    store
        .submit(put(
            replica(&b, "k", v1.clone(), "e1"),
            Precondition::default(),
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .current_version(&b, &k)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        v2,
        "an older replica must not become latest"
    );
    assert!(
        store.get_version(&b, &k, &v1).await.unwrap().is_some(),
        "v1 is stored"
    );

    // A replica carrying a NEWER id becomes the latest.
    store
        .submit(put(
            replica(&b, "k", v3.clone(), "e3"),
            Precondition::default(),
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .current_version(&b, &k)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        v3,
        "a newer replica becomes latest"
    );
    assert_eq!(
        version_count(&store, &b).await,
        3,
        "three distinct versions"
    );

    // Re-delivering v3 (the SAME id) is an idempotent upsert — no duplicate version.
    store
        .submit(put(
            replica(&b, "k", v3.clone(), "e3"),
            Precondition::default(),
        ))
        .await
        .unwrap();
    assert_eq!(
        version_count(&store, &b).await,
        3,
        "re-delivery did not duplicate"
    );
    assert_eq!(
        store
            .current_version(&b, &k)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        v3
    );
}

#[tokio::test]
async fn recover_claimed_resets_to_pending() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("recbkt").unwrap();
    store
        .submit(Mutation::CreateBucket(Box::new(bucket("recbkt"))))
        .await
        .unwrap();
    let v = VersionId::from_string("00000001".into());
    plant_outbox(&store, &b, "k", v, "r1").await;

    // Claim it: now `claimed` under a 300s lease, so it leaves the due (pending) set.
    store
        .claim_replication_batch(10, Timestamp(1_000))
        .await
        .unwrap();
    assert!(
        store
            .list_due_replication(10, Timestamp(2_000))
            .await
            .unwrap()
            .is_empty(),
        "a claimed entry is not in the due set before its lease expires"
    );

    // Startup recovery releases it immediately, without waiting out the lease.
    store
        .submit(Mutation::RecoverClaimedReplication)
        .await
        .unwrap();
    let due = store
        .list_due_replication(10, Timestamp(2_000))
        .await
        .unwrap();
    assert_eq!(due.len(), 1, "the orphaned claim was reclaimed to pending");
    assert_eq!(due[0].id, "r1");
    assert_eq!(due[0].status, ReplicationStatus::Pending);
}

#[tokio::test]
async fn get_bucket_quota_reads_the_column() {
    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("quotab").unwrap();

    // A bucket that does not exist reads as no-quota (None), not an error.
    assert_eq!(store.get_bucket_quota(&b).await.unwrap(), None);

    store
        .submit(Mutation::CreateBucket(Box::new(bucket("quotab"))))
        .await
        .unwrap();
    // A freshly created bucket has quota_bytes = NULL.
    assert_eq!(store.get_bucket_quota(&b).await.unwrap(), None);

    // Setting the quota is read back from the buckets.quota_bytes column.
    store
        .submit(Mutation::SetBucketQuota {
            bucket: b.clone(),
            quota_bytes: Some(4_096),
        })
        .await
        .unwrap();
    assert_eq!(store.get_bucket_quota(&b).await.unwrap(), Some(4_096));

    // Clearing it returns to NULL/None.
    store
        .submit(Mutation::SetBucketQuota {
            bucket: b.clone(),
            quota_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(store.get_bucket_quota(&b).await.unwrap(), None);
}

#[tokio::test]
async fn set_object_acl_updates_the_version_row() {
    use cairn_types::authz::{Acl, Grant, Grantee, Permission};

    let store = cairn_meta::open_in_memory().unwrap();
    let b = BucketName::parse("aclb").unwrap();
    let k = ObjectKey::parse("obj").unwrap();
    store
        .submit(Mutation::CreateBucket(Box::new(bucket("aclb"))))
        .await
        .unwrap();

    let v = VersionId::from_string("00000001".into());
    store
        .submit(put(
            row(&b, "obj", v.clone(), "e", true),
            Precondition::default(),
        ))
        .await
        .unwrap();
    assert!(
        store
            .get_version(&b, &k, &v)
            .await
            .unwrap()
            .unwrap()
            .acl
            .is_none()
    );

    let acl = Acl {
        owner: UserId::generate(),
        grants: vec![Grant {
            grantee: Grantee::AllUsers,
            permission: Permission::Read,
        }],
    };
    store
        .submit(Mutation::SetObjectAcl {
            bucket: b.clone(),
            key: k.clone(),
            version_id: v.clone(),
            acl: Some(acl.clone()),
        })
        .await
        .unwrap();
    let got = store.get_version(&b, &k, &v).await.unwrap().unwrap();
    assert_eq!(got.acl, Some(acl));

    // Clearing it stores SQL NULL and reads back as None.
    store
        .submit(Mutation::SetObjectAcl {
            bucket: b.clone(),
            key: k.clone(),
            version_id: v.clone(),
            acl: None,
        })
        .await
        .unwrap();
    assert!(
        store
            .get_version(&b, &k, &v)
            .await
            .unwrap()
            .unwrap()
            .acl
            .is_none()
    );
}

/// Audit #29 regression: re-wrap completion must NOT be inferred from a cleared cursor (which is
/// also the never-started state). `done_active_id` only reaches the active id after a real pass.
#[tokio::test]
async fn rewrap_completion_is_not_inferred_from_a_cleared_cursor() {
    let store = cairn_meta::open_in_memory().unwrap();
    let stream = "object_versions.sse_descriptor".to_owned();

    // Never started: no row at all -> the endpoint sees no done id, so it is NOT complete.
    assert!(store.rewrap_done_active_ids().await.unwrap().is_empty());
    assert!(store.rewrap_cursor(stream.clone()).await.unwrap().is_none());

    // Mid-pass: the cursor advances but completion stays 0 (not the active id).
    store
        .rewrap_set_progress(stream.clone(), Some("v-100".to_owned()), 50, 0, 1)
        .await
        .unwrap();
    assert_eq!(
        store.rewrap_cursor(stream.clone()).await.unwrap(),
        Some("v-100".to_owned())
    );
    let done: std::collections::HashMap<String, u16> = store
        .rewrap_done_active_ids()
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        done.get(stream.as_str()).copied(),
        Some(0),
        "mid-pass is not complete"
    );

    // A full pass under active id 2 finishes: cursor cleared AND done_active_id = 2.
    store
        .rewrap_finish_pass(stream.clone(), 2, 2)
        .await
        .unwrap();
    assert!(store.rewrap_cursor(stream.clone()).await.unwrap().is_none());
    let done: std::collections::HashMap<String, u16> = store
        .rewrap_done_active_ids()
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        done.get(stream.as_str()).copied(),
        Some(2),
        "complete under active id 2"
    );

    // A failed/partial pass records 0 again -> a later rotation is never falsely retire-eligible.
    store
        .rewrap_finish_pass(stream.clone(), 0, 3)
        .await
        .unwrap();
    let done: std::collections::HashMap<String, u16> = store
        .rewrap_done_active_ids()
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        done.get(stream.as_str()).copied(),
        Some(0),
        "a pass with failures is not complete"
    );
}

// Deleting a bucket must take its usage-analytics with it: the per-bucket request_metrics rows are
// dropped in the same commit as the bucket, so its history never lingers and a recreated bucket of
// the same name can't inherit the old series. Non-bucket roll-up rows (bucket_name "") survive, and
// because the delete is per-bucket it covers one, two, or N buckets the same way.
#[tokio::test]
async fn deleting_a_bucket_drops_its_request_metrics() {
    let store = cairn_meta::open_in_memory().unwrap();
    store
        .submit(Mutation::CreateBucket(Box::new(bucket("alpha"))))
        .await
        .unwrap();
    store
        .submit(Mutation::CreateBucket(Box::new(bucket("beta"))))
        .await
        .unwrap();

    let now = 100_000i64;
    let ts = now - 60;
    let metric = |b: &str, n: u64| RequestMetricRow {
        ts_bucket: ts,
        operation: if b.is_empty() {
            "Management".to_owned() // a non-bucket op (e.g. ListBuckets), keyed bucket_name ""
        } else {
            "GetObject".to_owned()
        },
        bucket: b.to_owned(),
        status_class: "2xx".to_owned(),
        count: n,
        bytes_in: 0,
        bytes_out: 0,
        lat_sum_ms: 0,
        lat_hist: [0u64; LATENCY_BUCKETS],
    };
    store
        .submit(Mutation::RecordRequestMetrics {
            rows: vec![metric("alpha", 10), metric("beta", 5), metric("", 3)],
            prune_before: None,
        })
        .await
        .unwrap();

    let names = |s: &RequestMetricsSeries| {
        s.top_buckets
            .iter()
            .map(|b| b.bucket.clone())
            .collect::<Vec<_>>()
    };

    // Before: both buckets appear in the analytics; the grand total counts every row.
    let before = store
        .query_request_metrics(MetricsRange::OneDay, now)
        .await
        .unwrap();
    assert_eq!(before.total, 18);
    assert!(names(&before).contains(&"alpha".to_owned()));
    assert!(names(&before).contains(&"beta".to_owned()));

    // Delete alpha → its per-bucket analytics go with it; beta and the non-bucket row remain.
    store
        .submit(Mutation::DeleteBucket(BucketName::parse("alpha").unwrap()))
        .await
        .unwrap();
    let after = store
        .query_request_metrics(MetricsRange::OneDay, now)
        .await
        .unwrap();
    assert_eq!(
        after.total, 8,
        "alpha's 10 are gone; beta (5) + non-bucket (3) remain"
    );
    assert!(
        !names(&after).contains(&"alpha".to_owned()),
        "alpha must not linger in the analytics after its bucket is deleted"
    );
    assert!(names(&after).contains(&"beta".to_owned()));

    // Deleting the rest clears their analytics too (the same per-bucket delete covers N buckets);
    // only the non-bucket ("") roll-up survives.
    store
        .submit(Mutation::DeleteBucket(BucketName::parse("beta").unwrap()))
        .await
        .unwrap();
    let last = store
        .query_request_metrics(MetricsRange::OneDay, now)
        .await
        .unwrap();
    assert_eq!(last.total, 3, "only the non-bucket roll-up remains");
    assert!(names(&last).is_empty());
}
