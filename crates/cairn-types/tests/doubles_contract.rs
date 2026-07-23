//! Contract tests for the canonical in-memory doubles. These pin the semantics every real
//! backend must reproduce: durable staging, the put commit point, listing, conditional-write
//! atomicity, versioning/delete-marker bookkeeping, and bounded reconciliation.

use bytes::Bytes;
use cairn_types::testing::{InMemoryBlobStore, InMemoryMetadataStore, TestClock};
use cairn_types::*;

fn body(data: &'static [u8]) -> BodyStream {
    Box::pin(futures_util::stream::once(async move {
        Ok(Bytes::from_static(data))
    }))
}

fn row_from(
    staged: &StagedBlob,
    bucket: &BucketName,
    key: &ObjectKey,
    version: VersionId,
    owner: &UserId,
    now: Timestamp,
) -> ObjectVersionRow {
    ObjectVersionRow {
        id: uuid::Uuid::new_v4().simple().to_string(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version,
        is_latest: true,
        is_delete_marker: false,
        size_logical: staged.size_logical,
        size_physical: staged.size_physical,
        etag: staged.etag.clone(),
        content_type: "application/octet-stream".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(staged.storage_path.clone()),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: owner.clone(),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: None,
        replicated_at: None,
        created_at: now,
        updated_at: now,
    }
}

async fn read_all(handle: BlobReadHandle) -> Vec<u8> {
    use futures_util::StreamExt;
    let mut out = Vec::new();
    let mut body = handle.body;
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

#[tokio::test]
async fn put_get_list_delete_roundtrip() {
    let blob = InMemoryBlobStore::new();
    let meta = InMemoryMetadataStore::new();
    let clock = TestClock::default();
    let bucket = BucketName::parse("test-bucket").unwrap();
    let owner = UserId::generate();

    // Stage + commit two objects.
    for (k, data) in [("a/1.txt", &b"hello"[..]), ("a/2.txt", &b"world!!"[..])] {
        let key = ObjectKey::parse(k).unwrap();
        let staged = blob
            .stage(
                &bucket,
                Box::pin(futures_util::stream::once({
                    let d = data.to_vec();
                    async move { Ok(Bytes::from(d)) }
                })),
                StageOptions {
                    compression: None,
                    extra_checksums: ChecksumSet::none(),
                    size_ceiling: 1 << 20,
                    content_type: "text/plain".to_owned(),
                    encryption: None,
                    content_length: None,
                },
            )
            .await
            .unwrap();
        let row = row_from(
            &staged,
            &bucket,
            &key,
            VersionId::null(),
            &owner,
            clock.now(),
        );
        let outcome = meta
            .submit(Mutation::PutObjectVersion {
                row: Box::new(row),
                precondition: Precondition::default(),
                replication: Vec::new(),
            })
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            MutationOutcome::Put {
                superseded: None,
                ..
            }
        ));
    }

    // GET back the first object's bytes.
    let current = meta
        .current_version(&bucket, &ObjectKey::parse("a/1.txt").unwrap())
        .await
        .unwrap()
        .unwrap();
    let handle = blob
        .open_raw(
            current.storage_path.as_ref().unwrap(),
            None,
            BlobCipher::KnownPlaintext,
            &current.compression,
        )
        .await
        .unwrap();
    assert_eq!(read_all(handle).await, b"hello");
    assert_eq!(current.etag.as_str(), "5d41402abc4b2a76b9719d911017c592"); // md5("hello")

    // LIST with a prefix returns both, sorted.
    let page = meta
        .list_current(
            &bucket,
            &ListQuery {
                prefix: Some("a/".to_owned()),
                limit: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].key.as_str(), "a/1.txt");
    assert!(!page.truncated);

    // DELETE the second; bucket is not empty (one remains).
    let key2 = ObjectKey::parse("a/2.txt").unwrap();
    let del = meta
        .submit(Mutation::DeleteVersion {
            bucket: bucket.clone(),
            key: key2.clone(),
            version_id: VersionId::null(),
            expected_updated_at: None,
        })
        .await
        .unwrap();
    let MutationOutcome::Deleted { freed: Some(_), .. } = del else {
        panic!("expected freed blob");
    };
    assert!(!meta.is_bucket_empty(&bucket).await.unwrap());
}

#[tokio::test]
async fn conditional_write_if_none_match_is_atomic() {
    let blob = InMemoryBlobStore::new();
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("cond-bucket").unwrap();
    let key = ObjectKey::parse("k").unwrap();
    let owner = UserId::generate();

    let staged = blob.stage(&bucket, body(b"v1"), opts()).await.unwrap();
    let row = row_from(
        &staged,
        &bucket,
        &key,
        VersionId::null(),
        &owner,
        Timestamp::EPOCH,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row.clone()),
        precondition: Precondition::default(),
        replication: Vec::new(),
    })
    .await
    .unwrap();

    // If-None-Match: * must now fail because the object exists.
    let err = meta
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition {
                if_match: None,
                if_none_match: Some(IfNoneMatch::Any),
            },
            replication: Vec::new(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, MetaError::PreconditionFailed));
}

#[tokio::test]
async fn versioning_keeps_history_and_promotes_latest() {
    let blob = InMemoryBlobStore::new();
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("ver-bucket").unwrap();
    let key = ObjectKey::parse("doc").unwrap();
    let owner = UserId::generate();

    let mut versions = Vec::new();
    for data in [&b"one"[..], &b"two"[..], &b"three"[..]] {
        let staged = blob
            .stage(
                &bucket,
                Box::pin(futures_util::stream::once({
                    let d = data.to_vec();
                    async move { Ok(Bytes::from(d)) }
                })),
                opts(),
            )
            .await
            .unwrap();
        let v = VersionId::generate();
        versions.push(v.clone());
        let mut row = row_from(&staged, &bucket, &key, v, &owner, Timestamp::EPOCH);
        row.is_latest = true;
        meta.submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: Vec::new(),
        })
        .await
        .unwrap();
        // version ids are time-sortable; give each a distinct timestamp
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    // All three versions are retained.
    let all = meta
        .list_versions(
            &bucket,
            &ListQuery {
                limit: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(all.items.len(), 3);

    // Latest is the third version; deleting it promotes the second.
    let latest = meta.current_version(&bucket, &key).await.unwrap().unwrap();
    assert_eq!(latest.version_id, *versions.last().unwrap());
    let del = meta
        .submit(Mutation::DeleteVersion {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: versions[2].clone(),
            expected_updated_at: None,
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
    let new_latest = meta.current_version(&bucket, &key).await.unwrap().unwrap();
    assert_eq!(new_latest.version_id, versions[1]);
}

#[tokio::test]
async fn reconcile_reclaims_orphan_blobs() {
    let blob = InMemoryBlobStore::new();
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("recon-bucket").unwrap();
    let key = ObjectKey::parse("kept").unwrap();
    let owner = UserId::generate();

    // One referenced blob...
    let kept = blob.stage(&bucket, body(b"keep"), opts()).await.unwrap();
    let row = row_from(
        &kept,
        &bucket,
        &key,
        VersionId::null(),
        &owner,
        Timestamp::EPOCH,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: Vec::new(),
    })
    .await
    .unwrap();
    // ...and one orphan blob with no metadata row (a crash between durability and commit).
    let _orphan = blob.stage(&bucket, body(b"orphan"), opts()).await.unwrap();
    assert_eq!(blob.blob_count(), 2);

    let oracle = meta.oracle();
    let report = blob
        .reconcile(&oracle, ReconcileOpts::default())
        .await
        .unwrap();
    assert_eq!(report.orphans_reclaimed, 1);
    assert_eq!(blob.blob_count(), 1);
}

fn opts() -> StageOptions {
    StageOptions {
        compression: None,
        extra_checksums: ChecksumSet::none(),
        size_ceiling: 1 << 20,
        content_type: "application/octet-stream".to_owned(),
        encryption: None,
        content_length: None,
    }
}

/// Plant one replication-outbox entry by committing a version that carries it, returning its id.
async fn plant_outbox_entry(
    meta: &InMemoryMetadataStore,
    bucket: &BucketName,
    key: &ObjectKey,
    version: &VersionId,
    id: &str,
) {
    plant_outbox_entry_at(meta, bucket, key, version, id, Timestamp::EPOCH).await;
}

/// As [`plant_outbox_entry`], but with an explicit `enqueued_at` — the clock
/// `PruneReplicationOutbox` ages rows off by, so a test can decide which entries the retention sweep
/// reclaims and which survive.
async fn plant_outbox_entry_at(
    meta: &InMemoryMetadataStore,
    bucket: &BucketName,
    key: &ObjectKey,
    version: &VersionId,
    id: &str,
    enqueued_at: Timestamp,
) {
    let entry = cairn_types::meta::OutboxEntry {
        enqueued_at,
        id: id.to_owned(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        operation: cairn_types::meta::ReplicationOp::ObjectCreate,
        rule_id: "rule-1".to_owned(),
        target_arn: None,
        attempts: 0,
        next_attempt_at: Timestamp::EPOCH,
        status: cairn_types::meta::ReplicationStatus::Pending,
        last_error: None,
        priority: 0,
        lease_until: None,
    };
    let row = ObjectVersionRow {
        id: uuid::Uuid::new_v4().simple().to_string(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        is_latest: true,
        is_delete_marker: false,
        size_logical: 3,
        size_physical: 3,
        etag: cairn_types::object::ETag::from_string("e".to_owned()),
        content_type: "text/plain".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(StoragePath::from_string(format!(
            "{}/{}",
            bucket.as_str(),
            version.as_str()
        ))),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId::generate(),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: Some(cairn_types::meta::ReplicationStatus::Pending),
        replicated_at: None,
        created_at: Timestamp::EPOCH,
        updated_at: Timestamp::EPOCH,
    };
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: vec![entry],
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn list_failed_replication_returns_only_terminal_entries() {
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("repl-bucket").unwrap();
    let key = ObjectKey::parse("k").unwrap();
    let v1 = VersionId::from_string("00000001".to_owned());
    let v2 = VersionId::from_string("00000002".to_owned());

    // Two outbox entries: one we will leave pending, one we will mark terminally failed.
    plant_outbox_entry(&meta, &bucket, &key, &v1, "pending-1").await;
    plant_outbox_entry(&meta, &bucket, &key, &v2, "doomed-1").await;

    // Nothing has failed yet.
    assert!(meta.list_failed_replication(100).await.unwrap().is_empty());

    // Mark the second entry terminal (next_attempt_at = None per the engine's terminal marking).
    meta.submit(Mutation::MarkReplicationFailed {
        id: "doomed-1".to_owned(),
        error: "destination unreachable".to_owned(),
        next_attempt_at: None,
    })
    .await
    .unwrap();

    let failed = meta.list_failed_replication(100).await.unwrap();
    assert_eq!(failed.len(), 1, "only the terminal entry is reported");
    assert_eq!(failed[0].id, "doomed-1");
    assert_eq!(failed[0].version_id, v2);
    assert_eq!(failed[0].attempts, 1);
    assert_eq!(
        failed[0].last_error.as_deref(),
        Some("destination unreachable")
    );

    // A retryable failure (next_attempt_at = Some) is NOT terminal and must not be listed.
    meta.submit(Mutation::MarkReplicationFailed {
        id: "pending-1".to_owned(),
        error: "transient".to_owned(),
        next_attempt_at: Some(Timestamp::from_secs(60)),
    })
    .await
    .unwrap();
    let failed = meta.list_failed_replication(100).await.unwrap();
    assert_eq!(
        failed.len(),
        1,
        "the retryable entry stays out of the failed list"
    );
    assert_eq!(failed[0].id, "doomed-1");

    // The limit is honoured.
    assert!(meta.list_failed_replication(0).await.unwrap().is_empty());
}

#[tokio::test]
async fn get_bucket_quota_round_trips_set_and_clear() {
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("quota-bucket").unwrap();

    // No quota set initially.
    assert_eq!(meta.get_bucket_quota(&bucket).await.unwrap(), None);

    // Setting a quota is readable back.
    meta.submit(Mutation::SetBucketQuota {
        bucket: bucket.clone(),
        quota_bytes: Some(1_048_576),
    })
    .await
    .unwrap();
    assert_eq!(
        meta.get_bucket_quota(&bucket).await.unwrap(),
        Some(1_048_576)
    );

    // Clearing it (None) returns to unlimited.
    meta.submit(Mutation::SetBucketQuota {
        bucket: bucket.clone(),
        quota_bytes: None,
    })
    .await
    .unwrap();
    assert_eq!(meta.get_bucket_quota(&bucket).await.unwrap(), None);
}

#[tokio::test]
async fn set_object_acl_replaces_the_version_acl() {
    use cairn_types::authz::{Acl, Grant, Grantee, Permission};

    let blob = InMemoryBlobStore::new();
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("acl-bucket").unwrap();
    let key = ObjectKey::parse("obj").unwrap();
    let owner = UserId::generate();

    let staged = blob.stage(&bucket, body(b"data"), opts()).await.unwrap();
    let version = VersionId::null();
    let row = row_from(
        &staged,
        &bucket,
        &key,
        version.clone(),
        &owner,
        Timestamp::EPOCH,
    );
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: Vec::new(),
    })
    .await
    .unwrap();

    // The freshly-put object has no ACL.
    let got = meta
        .get_version(&bucket, &key, &version)
        .await
        .unwrap()
        .unwrap();
    assert!(got.acl.is_none());

    // Setting an ACL replaces the version's `acl` column.
    let acl = Acl {
        owner: owner.clone(),
        grants: vec![Grant {
            grantee: Grantee::AllUsers,
            permission: Permission::Read,
        }],
    };
    meta.submit(Mutation::SetObjectAcl {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        acl: Some(acl.clone()),
    })
    .await
    .unwrap();
    let got = meta
        .get_version(&bucket, &key, &version)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.acl, Some(acl));

    // Clearing it (None) removes the ACL again.
    meta.submit(Mutation::SetObjectAcl {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        acl: None,
    })
    .await
    .unwrap();
    let got = meta
        .get_version(&bucket, &key, &version)
        .await
        .unwrap()
        .unwrap();
    assert!(got.acl.is_none());
}

/// An encrypted blob read WITHOUT its data-encryption key must fail closed — never plaintext,
/// never the raw ciphertext, never zeros.
///
/// This is a *contract* case rather than a double-only test on purpose: the in-memory double has
/// always refused a keyless encrypted read, while `LocalBlobStore` (which decides its framing from
/// the cipher argument, not from the bytes) happily streamed the ciphertext for an
/// encrypted-but-uncompressed blob — the exact divergence that let replication ship ciphertext to
/// mirrors while every doubles-based test stayed green. Now that `open_raw` requires the cipher be
/// named, `BlobCipher::KnownPlaintext` is the read that must fail closed. `cairn-blob` mirrors these
/// assertions
/// against the real store.
#[tokio::test]
async fn encrypted_blob_read_without_a_dek_fails_closed() {
    let blob = InMemoryBlobStore::new();
    let bucket = BucketName::parse("sse-bucket").unwrap();
    let dek = [9u8; 32];
    let staged = blob
        .stage(
            &bucket,
            body(b"top secret"),
            StageOptions {
                encryption: Some(dek),
                ..opts()
            },
        )
        .await
        .unwrap();

    // No key at all (KnownPlaintext): refused — never yields the plaintext bytes.
    let err = blob
        .open_raw(
            &staged.storage_path,
            None,
            BlobCipher::KnownPlaintext,
            &staged.compression,
        )
        .await
        .expect_err("a KnownPlaintext read of an encrypted blob must fail, not return bytes");
    assert!(
        matches!(err, BlobError::Corruption(_)),
        "unexpected: {err:?}"
    );

    // The wrong key: also refused.
    let err = blob
        .open_raw(
            &staged.storage_path,
            None,
            BlobCipher::Dek([1u8; 32]),
            &staged.compression,
        )
        .await
        .expect_err("a wrong-key read of an encrypted blob must fail");
    assert!(
        matches!(err, BlobError::Corruption(_)),
        "unexpected: {err:?}"
    );

    // The right key: the plaintext, byte-for-byte.
    let handle = blob
        .open_raw(
            &staged.storage_path,
            None,
            BlobCipher::Dek(dek),
            &staged.compression,
        )
        .await
        .unwrap();
    assert_eq!(read_all(handle).await, b"top secret".to_vec());
}

/// `probe` reports PRESENCE without a DEK and never decrypts: a well-formed ENCRYPTED blob probes
/// `Ok` (present) — not `Corruption` — a missing path is `NotFound`, and a plaintext blob's
/// physical length is reported. This is the seam the integrity `--repair` pass leans on to tell a
/// dangling row from a healthy encrypted object it holds no key for. `cairn-blob` mirrors it.
#[tokio::test]
async fn probe_reports_presence_without_a_dek() {
    let blob = InMemoryBlobStore::new();
    let bucket = BucketName::parse("probe-bucket").unwrap();

    // A plaintext blob: present, and its physical length is the byte length.
    let plain = blob
        .stage(&bucket, body(b"twelve bytes"), opts())
        .await
        .unwrap();
    let p = blob.probe(&plain.storage_path).await.unwrap();
    assert_eq!(p.physical_len, 12);

    // A well-formed ENCRYPTED blob probes present WITHOUT any DEK — presence is not decryptability.
    let enc = blob
        .stage(
            &bucket,
            body(b"top secret"),
            StageOptions {
                encryption: Some([9u8; 32]),
                ..opts()
            },
        )
        .await
        .unwrap();
    blob.probe(&enc.storage_path)
        .await
        .expect("an encrypted blob must probe present, never error");

    // A missing path is NotFound (the dangling-row case repair deletes).
    let missing = StoragePath::from_string(format!("{}/does-not-exist", bucket.as_str()));
    assert!(matches!(
        blob.probe(&missing).await,
        Err(BlobError::NotFound)
    ));
}

/// The in-memory double must implement `RequeueReplicationVersions` exactly like both SQL engines
/// (the 4(+1)-site rule): the outbox half is what makes a second repair pass real, the ledger half
/// is what stops the audit calling a corrupt object replicated, `only_encrypted` scopes it to the
/// incident's blast radius, and an inbound `Replica` stamp is never resurrected.
#[tokio::test]
async fn requeue_replication_versions_double_matches_the_engines() {
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("repl-bucket").unwrap();
    let enc_key = ObjectKey::parse("enc").unwrap();
    let plain_key = ObjectKey::parse("plain").unwrap();
    let v1 = VersionId::from_string("00000001".to_owned());
    let v2 = VersionId::from_string("00000002".to_owned());

    plant_outbox_entry(&meta, &bucket, &enc_key, &v1, "backfill:r1:enc:1").await;
    plant_outbox_entry(&meta, &bucket, &plain_key, &v2, "backfill:r1:plain:2").await;
    // Mark the first version encrypted by re-committing the row with a descriptor.
    let mut enc = meta
        .get_version(&bucket, &enc_key, &v1)
        .await
        .unwrap()
        .unwrap();
    enc.sse_descriptor =
        Some(r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":""}"#.to_owned());
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(enc),
        precondition: Precondition::default(),
        replication: Vec::new(),
    })
    .await
    .unwrap();

    // Both ship successfully; both entries and both version rows read `completed`.
    meta.claim_replication_batch(10, Timestamp::from_secs(1))
        .await
        .unwrap();
    for id in ["backfill:r1:enc:1", "backfill:r1:plain:2"] {
        meta.submit(Mutation::MarkReplicationDone {
            id: id.to_owned(),
            now: Timestamp(0),
        })
        .await
        .unwrap();
    }
    assert!(
        meta.claim_replication_batch(10, Timestamp::from_secs(2))
            .await
            .unwrap()
            .is_empty(),
        "a completed entry is never re-claimed — which is exactly why repair needs a requeue"
    );

    meta.submit(Mutation::RequeueReplicationVersions {
        bucket: bucket.clone(),
        only_encrypted: true,
        after_key: None,
        now: Timestamp::from_secs(10),
        limit: 1000,
    })
    .await
    .unwrap();

    let claimed = meta
        .claim_replication_batch(10, Timestamp::from_secs(20))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1, "only the encrypted version is requeued");
    assert_eq!(claimed[0].id, "backfill:r1:enc:1");
    assert_eq!(claimed[0].attempts, 0);
    assert_eq!(
        meta.object_replication_status(&bucket, &enc_key, &v1)
            .await
            .unwrap(),
        Some(cairn_types::meta::ReplicationStatus::Pending),
        "the durable ledger no longer claims this version is replicated"
    );
    assert_eq!(
        meta.object_replication_status(&bucket, &plain_key, &v2)
            .await
            .unwrap(),
        Some(cairn_types::meta::ReplicationStatus::Completed),
        "a plaintext version was never corrupt and must not be re-shipped"
    );
}

/// The double's `only_encrypted` scope must be KEY-level, exactly like both SQL engines' correlated
/// EXISTS (which omits `version_id`). Version-level filtering re-ships an encrypted `v1` while
/// leaving its later siblings settled, so `v1` lands at the destination last — reverting the
/// mirror's current object to the old version, or resurrecting a deleted one when the later sibling
/// is a delete marker. A double that disagrees with the engines here is worse than no double:
/// downstream crates trust it as the reference engine.
#[tokio::test]
async fn requeue_replication_versions_double_is_key_scoped() {
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("repl-bucket").unwrap();
    let k = ObjectKey::parse("k").unwrap();
    let p = ObjectKey::parse("p").unwrap();
    let v1 = VersionId::from_string("00000001".to_owned());
    let v2 = VersionId::from_string("00000002".to_owned());
    let v3 = VersionId::from_string("00000003".to_owned());

    // key `k`: an encrypted v1 and a later PLAINTEXT v2. key `p`: plaintext only.
    plant_outbox_entry(&meta, &bucket, &k, &v1, "k-1").await;
    plant_outbox_entry(&meta, &bucket, &k, &v2, "k-2").await;
    plant_outbox_entry(&meta, &bucket, &p, &v3, "p-3").await;
    let mut enc = meta.get_version(&bucket, &k, &v1).await.unwrap().unwrap();
    enc.sse_descriptor =
        Some(r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":""}"#.to_owned());
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(enc),
        precondition: Precondition::default(),
        replication: Vec::new(),
    })
    .await
    .unwrap();

    meta.claim_replication_batch(10, Timestamp::from_secs(1))
        .await
        .unwrap();
    for id in ["k-1", "k-2", "p-3"] {
        meta.submit(Mutation::MarkReplicationDone {
            id: id.to_owned(),
            now: Timestamp(0),
        })
        .await
        .unwrap();
    }

    meta.submit(Mutation::RequeueReplicationVersions {
        bucket: bucket.clone(),
        only_encrypted: true,
        after_key: None,
        now: Timestamp::from_secs(10),
        limit: 1000,
    })
    .await
    .unwrap();

    let claimed = meta
        .claim_replication_batch(10, Timestamp::from_secs(20))
        .await
        .unwrap();
    let mut ids: Vec<&str> = claimed.iter().map(|e| e.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec!["k-1", "k-2"],
        "both terminal entries of the key with an encrypted version are requeued; the \
         plaintext-only key is not"
    );
    assert_eq!(
        meta.object_replication_status(&bucket, &k, &v2)
            .await
            .unwrap(),
        Some(cairn_types::meta::ReplicationStatus::Pending),
        "the ledger half is key-scoped too, or the audit and the queue disagree"
    );
    assert_eq!(
        meta.object_replication_status(&bucket, &p, &v3)
            .await
            .unwrap(),
        Some(cairn_types::meta::ReplicationStatus::Completed)
    );
}

/// The double must page by KEY and thread the forward cursor exactly like both SQL engines, so a
/// caller's drain loop (`cairn-control`'s forced resync) terminates identically against it.
#[tokio::test]
async fn requeue_replication_versions_double_pages_by_key_and_threads_the_cursor() {
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("repl-bucket").unwrap();
    for i in 1..=5u32 {
        let key = ObjectKey::parse(&format!("k{i}")).unwrap();
        let v = VersionId::from_string(format!("0000000{i}"));
        plant_outbox_entry(&meta, &bucket, &key, &v, &format!("e{i}")).await;
    }
    meta.claim_replication_batch(10, Timestamp::from_secs(1))
        .await
        .unwrap();
    for i in 1..=5u32 {
        meta.submit(Mutation::MarkReplicationDone {
            id: format!("e{i}"),
            now: Timestamp(0),
        })
        .await
        .unwrap();
    }

    let mut total = 0u64;
    let mut passes = 0;
    let mut after_key: Option<String> = None;
    let mut ends: Vec<String> = Vec::new();
    loop {
        let outcome = meta
            .submit(Mutation::RequeueReplicationVersions {
                bucket: bucket.clone(),
                only_encrypted: false,
                after_key: after_key.clone(),
                now: Timestamp::from_secs(10),
                limit: 2,
            })
            .await
            .unwrap();
        let cairn_types::meta::MutationOutcome::RowsRequeued { rows, page_end } = outcome else {
            panic!("the requeue must report rows + a page cursor, got {outcome:?}");
        };
        total += rows;
        let Some(end) = page_end else { break };
        assert!(rows <= 4, "2 single-version keys per page, got {rows}");
        ends.push(end.clone());
        after_key = Some(end);
        passes += 1;
        assert!(passes < 100, "the loop must converge");
    }
    assert_eq!(total, 10, "5 outbox rows + 5 version rows");
    assert_eq!(
        ends,
        vec!["k2".to_owned(), "k4".to_owned(), "k5".to_owned()],
        "pages are ordered by key and resume strictly past the previous page"
    );
}

/// The double must be KEY-ATOMIC per page, not merely "the same answer on the easy case". Taking
/// the first `limit` ROWS in insertion order can requeue key `k`'s NEWER `completed` version in one
/// batch and its OLDER `failed` version in a later one; the replication heartbeat ships the newer
/// one in between and the mirror REVERTS to the old bytes. Downstream crates trust this double as
/// the reference engine, so it has to reproduce the guarantee, not just the outcome.
#[tokio::test]
async fn requeue_replication_versions_double_never_splits_a_key_across_pages() {
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("repl-bucket").unwrap();
    let a = ObjectKey::parse("a").unwrap();
    let k = ObjectKey::parse("k").unwrap();
    let v1 = VersionId::from_string("00000001".to_owned());
    let v2 = VersionId::from_string("00000002".to_owned());

    // Insertion order deliberately puts k's NEWER version in the outbox before its older one — the
    // shape a row-ordered `.take(limit)` gets wrong.
    plant_outbox_entry(&meta, &bucket, &k, &v2, "k:2").await;
    plant_outbox_entry(&meta, &bucket, &a, &v1, "a:1").await;
    plant_outbox_entry(&meta, &bucket, &k, &v1, "k:1").await;
    for (key, v) in [(&a, &v1), (&k, &v1)] {
        let mut enc = meta.get_version(&bucket, key, v).await.unwrap().unwrap();
        enc.sse_descriptor =
            Some(r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":""}"#.to_owned());
        meta.submit(Mutation::PutObjectVersion {
            row: Box::new(enc),
            precondition: Precondition::default(),
            replication: Vec::new(),
        })
        .await
        .unwrap();
    }

    meta.claim_replication_batch(10, Timestamp::from_secs(1))
        .await
        .unwrap();
    meta.submit(Mutation::MarkReplicationFailed {
        id: "k:1".to_owned(),
        error: "BadDigest".to_owned(),
        next_attempt_at: None,
    })
    .await
    .unwrap();
    for id in ["a:1", "k:2"] {
        meta.submit(Mutation::MarkReplicationDone {
            id: id.to_owned(),
            now: Timestamp::from_secs(2),
        })
        .await
        .unwrap();
    }

    let outcome = meta
        .submit(Mutation::RequeueReplicationVersions {
            bucket: bucket.clone(),
            only_encrypted: true,
            after_key: None,
            now: Timestamp::from_secs(10),
            limit: 1,
        })
        .await
        .unwrap();
    let cairn_types::meta::MutationOutcome::RowsRequeued { page_end, .. } = outcome else {
        panic!("expected a paged outcome, got {outcome:?}");
    };
    assert_eq!(page_end.as_deref(), Some("a"), "pages are ordered by key");
    let claimed = meta
        .claim_replication_batch(10, Timestamp::from_secs(11))
        .await
        .unwrap();
    let ids: Vec<&str> = claimed.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(
        ids,
        vec!["a:1"],
        "a page must never carry a partial key; got {ids:?}"
    );

    let outcome = meta
        .submit(Mutation::RequeueReplicationVersions {
            bucket: bucket.clone(),
            only_encrypted: true,
            after_key: page_end,
            now: Timestamp::from_secs(20),
            limit: 1,
        })
        .await
        .unwrap();
    let cairn_types::meta::MutationOutcome::RowsRequeued { page_end, .. } = outcome else {
        panic!("expected a paged outcome, got {outcome:?}");
    };
    assert_eq!(page_end.as_deref(), Some("k"));
    let claimed = meta
        .claim_replication_batch(10, Timestamp::from_secs(21))
        .await
        .unwrap();
    let mut ids: Vec<&str> = claimed.iter().map(|e| e.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec!["k:1", "k:2"],
        "both terminal rows of the key must move in the same batch"
    );
}

/// The double must stamp `replicated_at` on `MarkReplicationDone` (schema v23) like both engines,
/// leave `updated_at` (the client-visible S3 `LastModified`) alone, never stamp an inbound
/// `Replica`, and never advance the stamp on a mere requeue.
#[tokio::test]
async fn mark_replication_done_double_stamps_replicated_at() {
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("repl-bucket").unwrap();
    let k = ObjectKey::parse("k").unwrap();
    let v = VersionId::from_string("00000001".to_owned());
    plant_outbox_entry(&meta, &bucket, &k, &v, "e1").await;
    assert_eq!(
        meta.get_version(&bucket, &k, &v)
            .await
            .unwrap()
            .unwrap()
            .replicated_at,
        None
    );
    let before = meta.get_version(&bucket, &k, &v).await.unwrap().unwrap();

    meta.claim_replication_batch(10, Timestamp::from_secs(1))
        .await
        .unwrap();
    meta.submit(Mutation::MarkReplicationDone {
        id: "e1".to_owned(),
        now: Timestamp::from_secs(9_000),
    })
    .await
    .unwrap();
    let got = meta.get_version(&bucket, &k, &v).await.unwrap().unwrap();
    assert_eq!(
        got.replication_status,
        Some(cairn_types::meta::ReplicationStatus::Completed)
    );
    assert_eq!(got.replicated_at, Some(Timestamp::from_secs(9_000)));
    assert_eq!(
        got.updated_at, before.updated_at,
        "replication must not move the client-visible LastModified"
    );

    meta.submit(Mutation::RequeueReplicationVersions {
        bucket: bucket.clone(),
        only_encrypted: false,
        after_key: None,
        now: Timestamp::from_secs(9_500),
        limit: 100,
    })
    .await
    .unwrap();
    assert_eq!(
        meta.get_version(&bucket, &k, &v)
            .await
            .unwrap()
            .unwrap()
            .replicated_at,
        Some(Timestamp::from_secs(9_000)),
        "a requeue must not advance or clear the stamp — the re-ship has not happened yet"
    );

    // An inbound replica is never stamped as shipped from here.
    let rk = ObjectKey::parse("r").unwrap();
    let rv = VersionId::from_string("00000002".to_owned());
    plant_outbox_entry(&meta, &bucket, &rk, &rv, "r1").await;
    let mut inbound = meta.get_version(&bucket, &rk, &rv).await.unwrap().unwrap();
    inbound.replication_status = Some(cairn_types::meta::ReplicationStatus::Replica);
    meta.submit(Mutation::PutObjectVersion {
        row: Box::new(inbound),
        precondition: Precondition::default(),
        replication: Vec::new(),
    })
    .await
    .unwrap();
    meta.submit(Mutation::MarkReplicationDone {
        id: "r1".to_owned(),
        now: Timestamp::from_secs(9_900),
    })
    .await
    .unwrap();
    let got = meta.get_version(&bucket, &rk, &rv).await.unwrap().unwrap();
    assert_eq!(
        got.replication_status,
        Some(cairn_types::meta::ReplicationStatus::Replica)
    );
    assert_eq!(got.replicated_at, None);
}

/// DOUBLE PARITY for the narrowed LEDGER half (both SQL engines have the same rule).
///
/// `pending` on a version row is a claim that something will ship it. Only two populations honour
/// that claim: CURRENT versions (the resync backfill enumerates `list_current`, so they get a fresh
/// entry) and versions that still HAVE an outbox row. A non-current version whose row the retention
/// sweep pruned has neither — flipping it to `pending` makes the ledger claim queued work no queue
/// holds, and the replication audit's `repair_pending` gauge could then never reach zero, leaving
/// the runbook's done-state unreachable and its prescribed alert firing forever.
///
/// Downstream crates use this double as the reference engine, so a divergence here would hide the
/// bug rather than reveal it.
#[tokio::test]
async fn requeue_replication_versions_double_skips_unshippable_non_current_versions() {
    let meta = InMemoryMetadataStore::new();
    let bucket = BucketName::parse("repl-bucket").unwrap();
    let v1 = VersionId::from_string("00000001".to_owned());
    let v2 = VersionId::from_string("00000002".to_owned());

    // `pruned`'s v1 entry ages out of the outbox; `kept`'s survives. Both keys end up with a
    // non-current encrypted v1 and a current v2, all four stamped `completed`.
    for name in ["kept", "pruned"] {
        let key = ObjectKey::parse(name).unwrap();
        let enqueued = if name == "pruned" {
            Timestamp(0)
        } else {
            Timestamp(1_000)
        };
        plant_outbox_entry_at(&meta, &bucket, &key, &v1, &format!("{name}:1"), enqueued).await;
        let mut enc = meta.get_version(&bucket, &key, &v1).await.unwrap().unwrap();
        enc.sse_descriptor =
            Some(r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":""}"#.to_owned());
        meta.submit(Mutation::PutObjectVersion {
            row: Box::new(enc),
            precondition: Precondition::default(),
            replication: Vec::new(),
        })
        .await
        .unwrap();
        plant_outbox_entry_at(
            &meta,
            &bucket,
            &key,
            &v2,
            &format!("{name}:2"),
            Timestamp(1_000),
        )
        .await;
    }
    meta.claim_replication_batch(10, Timestamp::from_secs(1))
        .await
        .unwrap();
    for id in ["kept:1", "kept:2", "pruned:1", "pruned:2"] {
        meta.submit(Mutation::MarkReplicationDone {
            id: id.to_owned(),
            now: Timestamp(2),
        })
        .await
        .unwrap();
    }
    meta.submit(Mutation::PruneReplicationOutbox { before_ms: 500 })
        .await
        .unwrap();

    meta.submit(Mutation::RequeueReplicationVersions {
        bucket: bucket.clone(),
        only_encrypted: true,
        after_key: None,
        now: Timestamp::from_secs(10),
        limit: 1000,
    })
    .await
    .unwrap();

    let status = async |name: &str, v: &VersionId| {
        meta.object_replication_status(&bucket, &ObjectKey::parse(name).unwrap(), v)
            .await
            .unwrap()
    };
    assert_eq!(
        status("pruned", &v1).await,
        Some(cairn_types::meta::ReplicationStatus::Completed),
        "no queue can ever ship this version, so the ledger must not claim one will"
    );
    assert_eq!(
        status("pruned", &v2).await,
        Some(cairn_types::meta::ReplicationStatus::Pending),
        "the current version IS re-enqueued by the backfill that follows"
    );
    assert_eq!(
        status("kept", &v1).await,
        Some(cairn_types::meta::ReplicationStatus::Pending),
        "a non-current version whose outbox row survived is genuinely queued"
    );
    assert_eq!(
        status("kept", &v2).await,
        Some(cairn_types::meta::ReplicationStatus::Pending)
    );

    // The OUTBOX half is untouched by the narrowing: every surviving terminal row of a paged key
    // still moves in the same pass.
    let claimed = meta
        .claim_replication_batch(10, Timestamp::from_secs(20))
        .await
        .unwrap();
    let mut ids: Vec<&str> = claimed.iter().map(|e| e.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["kept:1", "kept:2", "pruned:2"]);
}
