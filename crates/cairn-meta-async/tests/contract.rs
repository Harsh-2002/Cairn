//! PARITY GATE: run a representative slice of `cairn-meta`'s own integration coverage against
//! `cairn_meta_async::open_libsql_in_memory()` and assert it behaves identically to the rusqlite
//! `cairn_meta::open_in_memory()` store. Each scenario executes the exact same mutation/read
//! sequence against both stores and asserts the observable results are equal, so any divergence in
//! the libSQL backend's SQL, savepoint semantics, listing, conditional writes, quota enforcement,
//! multipart, versioning, tags, replication outbox, users, or aggregates fails the gate.
//!
//! Covered: bucket CRUD; put/current_version/get_version; list_current + list_versions paging
//! with prefix/delimiter/markers; conditional writes If-Match/If-None-Match; multipart
//! create/record/complete; delete markers + versioning; tags; replication outbox claim/mark;
//! users; object-share capability hashing/id management; aggregate_counts; quota enforcement.

use cairn_types::authz::{Acl, Grant, Grantee, Permission};
use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::traits::{MetadataStore, ReconcileOracle};
use cairn_types::*;

#[path = "../../cairn-meta/tests/common/object_lock_races.rs"]
mod object_lock_races;

// ----------------------------------------------------------------------------------------------
// Fixtures shared by both backends.
// ----------------------------------------------------------------------------------------------

fn row(
    bucket: &BucketName,
    key: &str,
    version: VersionId,
    etag: &str,
    size: u64,
) -> ObjectVersionRow {
    ObjectVersionRow {
        // Deterministic id so cross-store comparisons of the row are stable.
        id: format!("{}-{}-{}", bucket.as_str(), key, version.as_str()),
        bucket: bucket.clone(),
        key: ObjectKey::parse(key).unwrap(),
        version_id: version,
        is_latest: true,
        is_delete_marker: false,
        size_logical: size,
        size_physical: size,
        etag: ETag::from_string(etag.to_owned()),
        content_type: "text/plain".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(StoragePath::from_string(format!(
            "{}/sp-{key}",
            bucket.as_str()
        ))),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId("owner".to_owned()),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: None,
        replicated_at: None,
        created_at: Timestamp(1),
        updated_at: Timestamp(1),
    }
}

#[tokio::test]
async fn writer_owned_object_lock_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bucket_name = BucketName::parse("locked").unwrap();
        s.submit(Mutation::CreateObjectLockBucket(Box::new(bucket(
            "locked",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();
        s.submit(Mutation::UpdateObjectLockConfiguration {
            bucket: bucket_name.clone(),
            default_retention: Some(DefaultRetention {
                mode: ObjectLockMode::Governance,
                period: RetentionPeriod::Days(1),
            }),
        })
        .await
        .unwrap();

        let key = ObjectKey::parse("object").unwrap();
        let version = VersionId::from_string("v1".into());
        let mut object = row(&bucket_name, "object", version.clone(), "original", 10);
        object.created_at = Timestamp(100);
        object.updated_at = Timestamp(100);
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(object),
            precondition: Precondition::default(),
            initial_state: InitialObjectState {
                tags: vec![("kind".to_owned(), "archive".to_owned())],
                lock_intent: ExplicitObjectLockIntent {
                    retention: None,
                    legal_hold: Some(true),
                },
            },
            replication: Vec::new(),
        })
        .await
        .unwrap();
        assert_eq!(
            s.get_object_tags(&bucket_name, &key, &version)
                .await
                .unwrap(),
            vec![("kind".to_owned(), "archive".to_owned())]
        );
        assert_eq!(
            s.get_object_lock(&bucket_name, &key, &version)
                .await
                .unwrap(),
            ObjectLockState {
                retention: Some(ObjectRetention {
                    mode: ObjectLockMode::Governance,
                    retain_until: Timestamp(86_400_100),
                }),
                legal_hold: true,
            }
        );
        assert!(matches!(
            s.submit(Mutation::SetObjectRetention {
                bucket: bucket_name.clone(),
                key: key.clone(),
                version_id: version.clone(),
                retention: Some(ObjectRetention {
                    mode: ObjectLockMode::Governance,
                    retain_until: Timestamp(86_400_099),
                }),
                now: Timestamp(200),
                bypass: GovernanceBypass::Denied,
            })
            .await,
            Err(MetaError::ObjectProtected)
        ));

        let mut replacement = row(&bucket_name, "object", version.clone(), "replacement", 11);
        replacement.created_at = Timestamp(200);
        replacement.updated_at = Timestamp(200);
        assert!(matches!(
            s.submit(Mutation::PutObjectVersion {
                row: Box::new(replacement),
                precondition: Precondition::default(),
                initial_state: InitialObjectState::default(),
                replication: Vec::new(),
            })
            .await,
            Err(MetaError::ObjectProtected)
        ));
        assert_eq!(
            s.get_version(&bucket_name, &key, &version)
                .await
                .unwrap()
                .unwrap()
                .etag
                .as_str(),
            "original"
        );

        assert_eq!(
            s.submit(Mutation::DeleteVersion {
                bucket: bucket_name.clone(),
                key: key.clone(),
                version_id: version.clone(),
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(200),
                bypass: GovernanceBypass::Authorized,
            })
            .await
            .unwrap(),
            MutationOutcome::DeleteProtected
        );
        s.submit(Mutation::SetObjectLegalHold {
            bucket: bucket_name.clone(),
            key: key.clone(),
            version_id: version.clone(),
            on: false,
        })
        .await
        .unwrap();
        assert!(matches!(
            s.submit(Mutation::DeleteVersion {
                bucket: bucket_name.clone(),
                key: key.clone(),
                version_id: version.clone(),
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(200),
                bypass: GovernanceBypass::Authorized,
            })
            .await
            .unwrap(),
            MutationOutcome::Deleted { .. }
        ));

        let marker_key = ObjectKey::parse("marker").unwrap();
        let marker_version = VersionId::from_string("v2".into());
        let mut marker = row(&bucket_name, "marker", marker_version.clone(), "ignored", 0);
        marker.is_delete_marker = true;
        marker.storage_path = None;
        marker.created_at = Timestamp(300);
        marker.updated_at = Timestamp(300);
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(marker),
            precondition: Precondition::default(),
            initial_state: InitialObjectState {
                tags: vec![("must".to_owned(), "drop".to_owned())],
                lock_intent: ExplicitObjectLockIntent {
                    retention: Some(ObjectRetention {
                        mode: ObjectLockMode::Compliance,
                        retain_until: Timestamp(10_000),
                    }),
                    legal_hold: Some(true),
                },
            },
            replication: Vec::new(),
        })
        .await
        .unwrap();
        assert!(
            s.get_object_tags(&bucket_name, &marker_key, &marker_version)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            s.get_object_lock(&bucket_name, &marker_key, &marker_version)
                .await
                .unwrap(),
            ObjectLockState::default()
        );

        let atomic_key = ObjectKey::parse("atomic").unwrap();
        let atomic_version = VersionId::from_string("v3".into());
        let mut atomic = row(&bucket_name, "atomic", atomic_version.clone(), "e", 1);
        atomic.created_at = Timestamp(400);
        atomic.updated_at = Timestamp(400);
        assert!(matches!(
            s.submit(Mutation::PutObjectVersion {
                row: Box::new(atomic),
                precondition: Precondition::default(),
                initial_state: InitialObjectState {
                    tags: vec![
                        ("duplicate".to_owned(), "one".to_owned()),
                        ("duplicate".to_owned(), "two".to_owned()),
                    ],
                    lock_intent: ExplicitObjectLockIntent::default(),
                },
                replication: Vec::new(),
            })
            .await,
            Err(MetaError::Conflict)
        ));
        assert!(
            s.get_version(&bucket_name, &atomic_key, &atomic_version)
                .await
                .unwrap()
                .is_none()
        );

        s.submit(Mutation::UpdateObjectLockConfiguration {
            bucket: bucket_name.clone(),
            default_retention: Some(DefaultRetention {
                mode: ObjectLockMode::Compliance,
                period: RetentionPeriod::Days(1),
            }),
        })
        .await
        .unwrap();
        let upload = UploadId::from_string("mp-lock".into());
        let multipart_key = ObjectKey::parse("multipart").unwrap();
        let multipart_version = VersionId::from_string("v4".into());
        s.submit(Mutation::CreateMultipart {
            session: Box::new(MultipartSession {
                upload_id: upload.clone(),
                bucket: bucket_name.clone(),
                key: multipart_key.clone(),
                content_type: "application/octet-stream".to_owned(),
                status: MultipartStatus::Active,
                owner_id: UserId("owner".to_owned()),
                initiated_by: UserId("owner".to_owned()),
                intended_acl: None,
                user_metadata: Vec::new(),
                initial_tags: vec![("kind".to_owned(), "multipart".to_owned())],
                lock_intent: ExplicitObjectLockIntent::default(),
                sse_requested: false,
                encrypt_parts: false,
                sse_kms_requested: false,
                sse_kms_key_id: None,
                sse_bucket_key_enabled: false,
                created_at: Timestamp(500),
                updated_at: Timestamp(500),
            }),
            limits: cairn_types::meta::MultipartLimits::default(),
        })
        .await
        .unwrap();
        let round_trip = s.get_multipart(&upload).await.unwrap().unwrap();
        assert_eq!(
            round_trip.initial_tags,
            vec![("kind".to_owned(), "multipart".to_owned())]
        );
        let claim_token = MultipartClaimToken::generate();
        assert!(matches!(
            s.submit(Mutation::ClaimMultipart {
                upload_id: upload.clone(),
                claim_token: claim_token.clone(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));
        let mut assembled = row(
            &bucket_name,
            "multipart",
            multipart_version.clone(),
            "assembled",
            20,
        );
        assembled.created_at = Timestamp(600);
        assembled.updated_at = Timestamp(600);
        assert!(matches!(
            s.submit(Mutation::CompleteMultipart {
                upload_id: upload,
                claim_token,
                row: Box::new(assembled),
                precondition: Precondition::default(),
                replication: Vec::new(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::Completed { .. })
        ));
        assert_eq!(
            s.get_object_lock(&bucket_name, &multipart_key, &multipart_version)
                .await
                .unwrap()
                .retention,
            Some(ObjectRetention {
                mode: ObjectLockMode::Compliance,
                retain_until: Timestamp(86_400_600),
            })
        );
        assert_eq!(
            s.get_object_tags(&bucket_name, &multipart_key, &multipart_version)
                .await
                .unwrap(),
            vec![("kind".to_owned(), "multipart".to_owned())]
        );

        assert!(matches!(
            s.submit(Mutation::SetVersioning {
                bucket: bucket_name.clone(),
                state: VersioningState::Suspended,
            })
            .await,
            Err(MetaError::InvalidBucketState)
        ));
        assert!(matches!(
            s.submit(Mutation::SetBucketConfig {
                bucket: bucket_name,
                aspect: ConfigAspect::ObjectLock,
                doc: None,
            })
            .await,
            Err(MetaError::InvalidBucketState)
        ));
    }
}

#[tokio::test]
async fn libsql_object_lock_races_have_safe_writer_serialization() {
    let store: std::sync::Arc<dyn MetadataStore> =
        std::sync::Arc::new(cairn_meta_async::open_libsql_in_memory().await.unwrap());
    object_lock_races::assert_writer_lock_races(store, "libsql-lock-races").await;
}

#[tokio::test]
async fn libsql_legacy_multipart_intent_fails_closed_and_preserves_session() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("legacy-object-lock-mpu.db");
    let store = cairn_meta_async::open_libsql(
        &db_path,
        &cairn_meta_async::OpenOptions {
            read_pool_size: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bucket_name = BucketName::parse("legacy-mpu").unwrap();
    let key = ObjectKey::parse("assembled").unwrap();
    let version = VersionId::from_string("v1".to_owned());
    let upload_id = UploadId::from_string("legacy-upload".to_owned());
    store
        .submit(Mutation::CreateObjectLockBucket(Box::new(bucket(
            bucket_name.as_str(),
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();
    store
        .submit(Mutation::CreateMultipart {
            session: Box::new(MultipartSession {
                upload_id: upload_id.clone(),
                bucket: bucket_name.clone(),
                key: key.clone(),
                content_type: "application/octet-stream".to_owned(),
                status: MultipartStatus::Active,
                owner_id: UserId("owner".to_owned()),
                initiated_by: UserId("owner".to_owned()),
                intended_acl: None,
                user_metadata: Vec::new(),
                initial_tags: Vec::new(),
                lock_intent: ExplicitObjectLockIntent::default(),
                sse_requested: false,
                encrypt_parts: false,
                sse_kms_requested: false,
                sse_kms_key_id: None,
                sse_bucket_key_enabled: false,
                created_at: Timestamp(10),
                updated_at: Timestamp(10),
            }),
            limits: cairn_types::meta::MultipartLimits::default(),
        })
        .await
        .unwrap();
    let claim_token = MultipartClaimToken::generate();
    store
        .submit(Mutation::ClaimMultipart {
            upload_id: upload_id.clone(),
            claim_token: claim_token.clone(),
        })
        .await
        .unwrap();

    let db = libsql::Builder::new_local(&db_path).build().await.unwrap();
    let conn = db.connect().unwrap();
    conn.execute(
        "UPDATE multipart_uploads SET object_lock_intent_known=0 WHERE id=?1",
        [upload_id.as_str()],
    )
    .await
    .unwrap();
    drop(conn);
    drop(db);

    assert!(matches!(
        store
            .submit(Mutation::CompleteMultipart {
                upload_id: upload_id.clone(),
                claim_token,
                row: Box::new(row(&bucket_name, key.as_str(), version, "assembled", 1)),
                precondition: Precondition::default(),
                replication: Vec::new(),
            })
            .await,
        Err(MetaError::InvalidObjectLockState)
    ));
    assert_eq!(
        store
            .get_multipart(&upload_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        MultipartStatus::Completing
    );
    assert!(
        store
            .current_version(&bucket_name, &key)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn libsql_object_lock_corrupt_configuration_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("object-lock-corrupt.db");
    let store = cairn_meta_async::open_libsql(
        &db_path,
        &cairn_meta_async::OpenOptions {
            read_pool_size: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bucket_name = BucketName::parse("strict").unwrap();
    let key = ObjectKey::parse("live").unwrap();
    let version = VersionId::from_string("v1".into());
    store
        .submit(Mutation::CreateObjectLockBucket(Box::new(bucket(
            "strict",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();
    store
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bucket_name, "live", version.clone(), "etag", 1)),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: Vec::new(),
        })
        .await
        .unwrap();

    let db = libsql::Builder::new_local(&db_path).build().await.unwrap();
    let conn = db.connect().unwrap();
    conn.execute_batch(
        "UPDATE bucket_config
         SET doc='{\"enabled\":true,\"default_retention\":null,\"unknown\":1}'
         WHERE bucket_name='strict' AND aspect='object_lock';",
    )
    .await
    .unwrap();
    drop(conn);
    drop(db);

    assert!(matches!(
        store
            .submit(Mutation::SetObjectLegalHold {
                bucket: bucket_name.clone(),
                key: ObjectKey::parse("missing").unwrap(),
                version_id: VersionId::from_string("missing".into()),
                on: true,
            })
            .await,
        Err(MetaError::ObjectVersionNotFound)
    ));
    assert!(matches!(
        store
            .submit(Mutation::DeleteVersion {
                bucket: bucket_name,
                key,
                version_id: version,
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(i64::MAX),
                bypass: GovernanceBypass::Authorized,
            })
            .await,
        Err(MetaError::InvalidObjectLockState)
    ));
}

fn put(row: ObjectVersionRow, pc: Precondition) -> Mutation {
    Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: pc,
        initial_state: InitialObjectState::default(),
        replication: Vec::new(),
    }
}

fn bucket(name: &str, versioning: VersioningState) -> Bucket {
    Bucket {
        name: BucketName::parse(name).unwrap(),
        owner_id: UserId("owner".to_owned()),
        created_at: Timestamp(1),
        versioning,
        ownership_mode: OwnershipMode::BucketOwnerEnforced,
        region: "us-east-1".to_owned(),
        compression: None,
    }
}

fn user_record(id: &str, akid: &str) -> UserRecord {
    UserRecord {
        user: User {
            id: UserId(id.to_owned()),
            display_name: format!("User {id}"),
            access_key_id: akid.to_owned(),
            sigv4_access_key_id: Some(format!("SIG-{akid}")),
            role: cairn_types::auth::Role::Member,
            is_active: true,
            quota_bytes: None,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        },
        bearer_secret_hash: "hash".to_owned(),
        sigv4_secret_ciphertext: Some(vec![1, 2, 3, 4]),
        sigv4_secret_nonce: Some(vec![9, 8, 7]),
    }
}

/// Open both backends. The libSQL store must be created inside the tokio runtime (its writer is a
/// spawned task); the rusqlite store spawns an OS-thread writer and is runtime-agnostic.
async fn both() -> (
    cairn_meta_async::LibsqlMetadataStore,
    cairn_meta::SqliteMetadataStore,
) {
    let a = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    let b = cairn_meta::open_in_memory().unwrap();
    (a, b)
}

// ----------------------------------------------------------------------------------------------
// Scenarios. Each runs the identical sequence on both stores via a generic closure and asserts
// equal observable output.
// ----------------------------------------------------------------------------------------------

#[tokio::test]
async fn object_share_capability_parity() {
    let (a, b) = both().await;
    for store in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bucket_name = BucketName::parse("shares").unwrap();
        let key = ObjectKey::parse("private.txt").unwrap();
        store
            .submit(Mutation::CreateBucket(Box::new(bucket(
                "shares",
                VersioningState::Unversioned,
            ))))
            .await
            .unwrap();

        let raw_token = "libsql-share-token-sentinel-029";
        let token_hash = ShareLookupHash::for_token(raw_token);
        let expected = ShareRow {
            id: "share-stable-id".to_owned(),
            token_hash,
            bucket: bucket_name.clone(),
            key: key.clone(),
            version_id: None,
            expires_at: Some(Timestamp(5_000)),
            disposition: ShareDisposition::Inline,
            filename: None,
            created_by: UserId("admin".to_owned()),
            created_at: Timestamp(100),
            revoked_at: None,
        };
        store
            .submit(Mutation::CreateShare(Box::new(expected.clone())))
            .await
            .unwrap();

        assert_eq!(
            store.get_share_by_id("share-stable-id").await.unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            store.get_share_by_token_hash(&token_hash).await.unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            store.list_shares(&bucket_name, Some(&key)).await.unwrap(),
            vec![expected]
        );
        assert!(
            store
                .get_share_by_token_hash(&ShareLookupHash::for_token("wrong"))
                .await
                .unwrap()
                .is_none()
        );

        store
            .submit(Mutation::RevokeShare {
                id: "share-stable-id".to_owned(),
                now: Timestamp(200),
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .get_share_by_id("share-stable-id")
                .await
                .unwrap()
                .unwrap()
                .revoked_at,
            Some(Timestamp(200))
        );
    }
}

#[tokio::test]
async fn bucket_crud_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        // Readiness exercises a real backend read connection without depending on table contents.
        s.read_probe().await.unwrap();
        let bk = BucketName::parse("bkt").unwrap();
        // Create.
        assert_eq!(
            s.submit(Mutation::CreateBucket(Box::new(bucket(
                "bkt",
                VersioningState::Enabled
            ))))
            .await
            .unwrap(),
            MutationOutcome::Ack
        );
        let got = s.get_bucket(&bk).await.unwrap().unwrap();
        assert_eq!(got.name, bk);
        assert_eq!(got.versioning, VersioningState::Enabled);
        s.read_probe().await.unwrap();

        // Duplicate => Conflict.
        let err = s
            .submit(Mutation::CreateBucket(Box::new(bucket(
                "bkt",
                VersioningState::Enabled,
            ))))
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::Conflict));

        // list_buckets.
        assert_eq!(s.list_buckets(None).await.unwrap().len(), 1);

        // SetVersioning / SetOwnership.
        s.submit(Mutation::SetVersioning {
            bucket: bk.clone(),
            state: VersioningState::Suspended,
        })
        .await
        .unwrap();
        assert_eq!(
            s.get_bucket(&bk).await.unwrap().unwrap().versioning,
            VersioningState::Suspended
        );

        // Config aspect set/get/clear.
        let doc = ConfigDoc("{\"hello\":true}".to_owned());
        s.submit(Mutation::SetBucketConfig {
            bucket: bk.clone(),
            aspect: ConfigAspect::Policy,
            doc: Some(doc.clone()),
        })
        .await
        .unwrap();
        assert_eq!(
            s.get_bucket_config(&bk, ConfigAspect::Policy)
                .await
                .unwrap(),
            Some(doc)
        );
        s.submit(Mutation::SetBucketConfig {
            bucket: bk.clone(),
            aspect: ConfigAspect::Policy,
            doc: None,
        })
        .await
        .unwrap();
        assert_eq!(
            s.get_bucket_config(&bk, ConfigAspect::Policy)
                .await
                .unwrap(),
            None
        );

        // Delete.
        assert!(s.is_bucket_empty(&bk).await.unwrap());
        s.submit(Mutation::DeleteBucket(bk.clone())).await.unwrap();
        assert!(s.get_bucket(&bk).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn put_and_get_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        let key = ObjectKey::parse("k").unwrap();
        let v1 = VersionId::from_string("00000001".into());
        let out = s
            .submit(put(
                row(&bk, "k", v1.clone(), "e1", 3),
                Precondition::default(),
            ))
            .await
            .unwrap();
        assert!(matches!(out, MutationOutcome::Put { .. }));

        let cur = s.current_version(&bk, &key).await.unwrap().unwrap();
        assert_eq!(cur.etag.as_str(), "e1");
        assert_eq!(cur.version_id, v1);

        let gv = s.get_version(&bk, &key, &v1).await.unwrap().unwrap();
        assert_eq!(gv.size_logical, 3);
        assert!(
            s.get_version(&bk, &key, &VersionId::from_string("nope".into()))
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn versioning_history_and_promotion_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        let k = ObjectKey::parse("doc").unwrap();
        let vs = ["00000001", "00000002", "00000003"].map(|v| VersionId::from_string(v.into()));
        for v in &vs {
            s.submit(put(
                row(&bk, "doc", v.clone(), "e", 3),
                Precondition::default(),
            ))
            .await
            .unwrap();
        }
        assert_eq!(
            s.current_version(&bk, &k)
                .await
                .unwrap()
                .unwrap()
                .version_id,
            vs[2]
        );

        let del = s
            .submit(Mutation::DeleteVersion {
                bucket: bk.clone(),
                key: k.clone(),
                version_id: vs[2].clone(),
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(i64::MAX),
                bypass: GovernanceBypass::Denied,
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
            s.current_version(&bk, &k)
                .await
                .unwrap()
                .unwrap()
                .version_id,
            vs[1]
        );

        let all = s
            .list_versions(
                &bk,
                &ListQuery {
                    limit: 100,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(all.items.len(), 2);
    }
}

#[tokio::test]
async fn object_write_resolution_exact_row_path_parity() {
    let (a, b) = both().await;
    for store in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bucket_name = BucketName::parse("write-resolution").unwrap();
        store
            .submit(Mutation::CreateBucket(Box::new(bucket(
                "write-resolution",
                VersioningState::Suspended,
            ))))
            .await
            .unwrap();
        let key = ObjectKey::parse("object").unwrap();
        let version_id = VersionId::null();
        let first = row(&bucket_name, "object", version_id.clone(), "first", 3);
        let first_id = first.id.clone();
        let first_path = first.storage_path.clone().unwrap();
        store
            .submit(put(first, Precondition::default()))
            .await
            .unwrap();
        let resolve = |row_id: String, storage_path: StoragePath| Mutation::ResolveObjectWrite {
            bucket: bucket_name.clone(),
            key: key.clone(),
            version_id: version_id.clone(),
            row_id,
            storage_path,
        };
        assert_eq!(
            store
                .submit(resolve(first_id.clone(), first_path.clone()))
                .await
                .unwrap(),
            MutationOutcome::ObjectWriteResolved { referenced: true }
        );
        assert_eq!(
            store
                .submit(resolve("wrong-row".to_owned(), first_path.clone()))
                .await
                .unwrap(),
            MutationOutcome::ObjectWriteResolved { referenced: false }
        );

        let mut second = row(&bucket_name, "object", version_id.clone(), "second", 4);
        second.id = "replacement-row".to_owned();
        second.storage_path = Some(StoragePath::from_string(
            "write-resolution/replacement-path".to_owned(),
        ));
        let second_path = second.storage_path.clone().unwrap();
        store
            .submit(put(second, Precondition::default()))
            .await
            .unwrap();
        assert_eq!(
            store.submit(resolve(first_id, first_path)).await.unwrap(),
            MutationOutcome::ObjectWriteResolved { referenced: false }
        );
        assert_eq!(
            store
                .submit(resolve("replacement-row".to_owned(), second_path))
                .await
                .unwrap(),
            MutationOutcome::ObjectWriteResolved { referenced: true }
        );
    }
}

#[tokio::test]
async fn delete_not_applied_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bucket = BucketName::parse("delete-guard").unwrap();
        let key = ObjectKey::parse("object").unwrap();
        let version = VersionId::from_string("v1".to_owned());
        let mut object = row(&bucket, key.as_str(), version.clone(), "etag", 3);
        object.id = "observed-row".to_owned();
        object.updated_at = Timestamp(100);
        s.submit(put(object, Precondition::default()))
            .await
            .unwrap();
        let observed = s
            .list_current(
                &bucket,
                &ListQuery {
                    limit: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .items
            .into_iter()
            .next()
            .unwrap();

        let stale_timestamp = s
            .submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version.clone(),
                expected_row_id: Some(observed.row_id.clone()),
                expected_updated_at: Some(Timestamp(99)),
                require_sole_key_version: false,
                now: Timestamp(i64::MAX),
                bypass: GovernanceBypass::Denied,
            })
            .await
            .unwrap();
        assert_eq!(stale_timestamp, MutationOutcome::DeleteNotApplied);
        assert!(
            s.get_version(&bucket, &key, &version)
                .await
                .unwrap()
                .is_some()
        );

        let mut replacement = row(&bucket, key.as_str(), version.clone(), "replacement", 4);
        replacement.id = "replacement-row".to_owned();
        replacement.updated_at = Timestamp(100);
        s.submit(put(replacement, Precondition::default()))
            .await
            .unwrap();
        let stale_row = s
            .submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version.clone(),
                expected_row_id: Some(observed.row_id),
                expected_updated_at: Some(Timestamp(100)),
                require_sole_key_version: false,
                now: Timestamp(i64::MAX),
                bypass: GovernanceBypass::Denied,
            })
            .await
            .unwrap();
        assert_eq!(stale_row, MutationOutcome::DeleteNotApplied);
        assert_eq!(
            s.get_version(&bucket, &key, &version)
                .await
                .unwrap()
                .unwrap()
                .id,
            "replacement-row"
        );

        let missing = s
            .submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_string("missing".to_owned()),
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(i64::MAX),
                bypass: GovernanceBypass::Denied,
            })
            .await
            .unwrap();
        assert_eq!(missing, MutationOutcome::DeleteNotApplied);

        let deleted = s
            .submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version.clone(),
                expected_row_id: Some("replacement-row".to_owned()),
                expected_updated_at: Some(Timestamp(100)),
                require_sole_key_version: false,
                now: Timestamp(i64::MAX),
                bypass: GovernanceBypass::Denied,
            })
            .await
            .unwrap();
        assert!(matches!(deleted, MutationOutcome::Deleted { .. }));
        assert!(
            s.get_version(&bucket, &key, &version)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn sole_delete_marker_guard_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bucket = BucketName::parse("marker-cleanup").unwrap();
        s.submit(Mutation::CreateBucket(Box::new(crate::bucket(
            "marker-cleanup",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();

        // The target is latest and is a delete marker, but it has an older sibling: lifecycle's
        // stale "sole marker" observation must not remove it and reveal the older object.
        let history_key = ObjectKey::parse("history").unwrap();
        let history_version = VersionId::from_string("history-v1".to_owned());
        let mut history = row(
            &bucket,
            history_key.as_str(),
            history_version.clone(),
            "history",
            3,
        );
        history.updated_at = Timestamp(100);
        s.submit(put(history, Precondition::default()))
            .await
            .unwrap();
        let history_marker = VersionId::from_string("history-marker".to_owned());
        s.submit(Mutation::CreateDeleteMarker {
            bucket: bucket.clone(),
            key: history_key.clone(),
            version_id: history_marker.clone(),
            owner_id: UserId("owner".to_owned()),
            now: Timestamp(200),
            bypass: GovernanceBypass::Denied,
            expected_current: None,
            replication: Vec::new(),
        })
        .await
        .unwrap();

        let stale_group = s
            .submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: history_key.clone(),
                version_id: history_marker.clone(),
                expected_row_id: None,
                expected_updated_at: Some(Timestamp(200)),
                require_sole_key_version: true,
                now: Timestamp(300),
                bypass: GovernanceBypass::Denied,
            })
            .await
            .unwrap();
        assert_eq!(stale_group, MutationOutcome::DeleteNotApplied);
        assert_eq!(
            s.current_version(&bucket, &history_key)
                .await
                .unwrap()
                .unwrap()
                .version_id,
            history_marker
        );
        assert!(
            s.get_version(&bucket, &history_key, &history_version)
                .await
                .unwrap()
                .is_some()
        );

        // The maintenance-only guard never applies to a data row, even when it is the sole/latest
        // version and its timestamp matches.
        let data_key = ObjectKey::parse("data").unwrap();
        let data_version = VersionId::from_string("data-v1".to_owned());
        let mut data = row(&bucket, data_key.as_str(), data_version.clone(), "data", 4);
        data.updated_at = Timestamp(400);
        s.submit(put(data, Precondition::default())).await.unwrap();
        assert_eq!(
            s.submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: data_key.clone(),
                version_id: data_version.clone(),
                expected_row_id: None,
                expected_updated_at: Some(Timestamp(400)),
                require_sole_key_version: true,
                now: Timestamp(500),
                bypass: GovernanceBypass::Denied,
            })
            .await
            .unwrap(),
            MutationOutcome::DeleteNotApplied
        );
        assert!(
            s.get_version(&bucket, &data_key, &data_version)
                .await
                .unwrap()
                .is_some()
        );

        // A genuinely sole/latest marker with the matching timestamp is removed.
        let sole_key = ObjectKey::parse("sole").unwrap();
        let sole_marker = VersionId::from_string("sole-marker".to_owned());
        s.submit(Mutation::CreateDeleteMarker {
            bucket: bucket.clone(),
            key: sole_key.clone(),
            version_id: sole_marker.clone(),
            owner_id: UserId("owner".to_owned()),
            now: Timestamp(600),
            bypass: GovernanceBypass::Denied,
            expected_current: None,
            replication: Vec::new(),
        })
        .await
        .unwrap();
        assert_eq!(
            s.submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: sole_key.clone(),
                version_id: sole_marker,
                expected_row_id: None,
                expected_updated_at: Some(Timestamp(600)),
                require_sole_key_version: true,
                now: Timestamp(700),
                bypass: GovernanceBypass::Denied,
            })
            .await
            .unwrap(),
            MutationOutcome::Deleted {
                freed: None,
                promoted_latest: false,
            }
        );
        assert!(
            s.current_version(&bucket, &sole_key)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn guarded_delete_marker_rejects_stale_current_without_side_effects_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bucket = BucketName::parse("marker-guard").unwrap();
        let key = ObjectKey::parse("object").unwrap();
        s.submit(Mutation::CreateBucket(Box::new(crate::bucket(
            "marker-guard",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();

        let observed_version = VersionId::from_string("observed".to_owned());
        let mut observed = row(
            &bucket,
            key.as_str(),
            observed_version.clone(),
            "observed",
            3,
        );
        observed.created_at = Timestamp(100);
        observed.updated_at = Timestamp(100);
        s.submit(put(observed, Precondition::default()))
            .await
            .unwrap();

        // A concurrent write becomes current after lifecycle enumerated `observed_version`.
        let fresh_version = VersionId::from_string("fresh".to_owned());
        let mut fresh = row(&bucket, key.as_str(), fresh_version.clone(), "fresh", 4);
        fresh.created_at = Timestamp(200);
        fresh.updated_at = Timestamp(200);
        s.submit(put(fresh, Precondition::default())).await.unwrap();

        let outbox = |id: &str, version: &VersionId| OutboxEntry {
            id: id.to_owned(),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: version.clone(),
            operation: ReplicationOp::DeleteMarker,
            rule_id: "lifecycle".to_owned(),
            target_arn: None,
            attempts: 0,
            next_attempt_at: Timestamp(300),
            status: ReplicationStatus::Pending,
            last_error: None,
            priority: 0,
            lease_until: None,
            enqueued_at: Timestamp(300),
        };

        let stale_version_marker = VersionId::from_string("marker-stale-version".to_owned());
        let stale_version = s
            .submit(Mutation::CreateDeleteMarker {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: stale_version_marker.clone(),
                owner_id: UserId("owner".to_owned()),
                now: Timestamp(300),
                bypass: GovernanceBypass::Denied,
                expected_current: Some(CurrentVersionGuard {
                    version_id: observed_version,
                    updated_at: Timestamp(100),
                }),
                replication: vec![outbox("marker-stale-version-outbox", &stale_version_marker)],
            })
            .await
            .unwrap();
        assert_eq!(stale_version, MutationOutcome::DeleteNotApplied);

        // Version identity alone is insufficient: the observed timestamp must still match too.
        let stale_time_marker = VersionId::from_string("marker-stale-time".to_owned());
        let stale_time = s
            .submit(Mutation::CreateDeleteMarker {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: stale_time_marker.clone(),
                owner_id: UserId("owner".to_owned()),
                now: Timestamp(301),
                bypass: GovernanceBypass::Denied,
                expected_current: Some(CurrentVersionGuard {
                    version_id: fresh_version.clone(),
                    updated_at: Timestamp(199),
                }),
                replication: vec![outbox("marker-stale-time-outbox", &stale_time_marker)],
            })
            .await
            .unwrap();
        assert_eq!(stale_time, MutationOutcome::DeleteNotApplied);

        let current = s.current_version(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(current.version_id, fresh_version);
        assert_eq!(current.updated_at, Timestamp(200));
        assert!(!current.is_delete_marker);
        assert!(
            s.get_version(&bucket, &key, &stale_version_marker)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            s.get_version(&bucket, &key, &stale_time_marker)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            s.list_due_replication(10, Timestamp(i64::MAX))
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn delete_marker_hides_current_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        let k = ObjectKey::parse("k").unwrap();
        s.submit(put(
            row(&bk, "k", VersionId::from_string("v1".into()), "e", 3),
            Precondition::default(),
        ))
        .await
        .unwrap();
        s.submit(Mutation::CreateDeleteMarker {
            bucket: bk.clone(),
            key: k.clone(),
            version_id: VersionId::from_string("v2".into()),
            owner_id: UserId("owner".to_owned()),
            now: Timestamp(2),
            bypass: GovernanceBypass::Denied,
            expected_current: None,
            replication: Vec::new(),
        })
        .await
        .unwrap();
        // The latest version is now the delete marker.
        let cur = s.current_version(&bk, &k).await.unwrap().unwrap();
        assert!(cur.is_delete_marker);
        // is_bucket_empty means "no rows at all" (S3 DeleteBucket semantics, audit #3): the prior
        // version v1 and the delete marker v2 both remain, so the bucket is NOT empty.
        assert!(!s.is_bucket_empty(&bk).await.unwrap());
        // list_current excludes the marker; list_versions includes both.
        assert_eq!(
            s.list_current(
                &bk,
                &ListQuery {
                    limit: 100,
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .items
            .len(),
            0
        );
        assert_eq!(
            s.list_versions(
                &bk,
                &ListQuery {
                    limit: 100,
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .items
            .len(),
            2
        );
    }
}

#[tokio::test]
async fn listing_prefix_delimiter_and_pagination_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        for k in ["a/1", "a/2", "a/3", "b/1", "c"] {
            s.submit(put(
                row(&bk, k, VersionId::null(), "e", 1),
                Precondition::default(),
            ))
            .await
            .unwrap();
        }
        let page = s
            .list_current(
                &bk,
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

        let page = s
            .list_current(
                &bk,
                &ListQuery {
                    prefix: Some("a/".into()),
                    limit: 100,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page.items.len(), 3);

        // Pagination across the keyspace.
        let mut all = Vec::new();
        let mut cursor = None;
        loop {
            let page = s
                .list_current(
                    &bk,
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

        // start_after.
        let page = s
            .list_current(
                &bk,
                &ListQuery {
                    start_after: Some("a/2".into()),
                    limit: 100,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|i| i.key.as_str().to_owned())
                .collect::<Vec<_>>(),
            vec!["a/3", "b/1", "c"]
        );
    }
}

/// A version continuation pair must be applied before the backend's internal SQL limit. If a
/// backend fetches a fixed batch first and only then drops rows at-or-above the marker, a key with
/// more history than that batch eventually returns an empty/short page and silently loses the
/// remaining versions.
#[tokio::test]
async fn deep_single_key_version_listing_makes_monotonic_progress_parity() {
    const TOTAL: usize = 2_053;
    const PAGE_SIZE: u32 = 1_000;

    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("deep-history").unwrap();
        for ordinal in 0..TOTAL {
            let version = VersionId::from_string(format!("version-{ordinal:08}"));
            let mut object = row(&bk, "one-key", version, "e", 1);
            object.storage_path = Some(StoragePath::from_string(format!(
                "{}/deep-{ordinal:08}",
                bk.as_str()
            )));
            s.submit(put(object, Precondition::default()))
                .await
                .unwrap();
        }

        let mut seen = Vec::with_capacity(TOTAL);
        let mut cursor: Option<String> = None;
        let mut version_marker: Option<String> = None;
        let mut previous_marker: Option<String> = None;
        let mut pages = 0usize;

        loop {
            pages += 1;
            assert!(pages <= 4, "listing cursor did not terminate");
            let page = s
                .list_versions(
                    &bk,
                    &ListQuery {
                        cursor: cursor.clone(),
                        version_id_marker: version_marker.clone(),
                        limit: PAGE_SIZE,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert!(
                !page.items.is_empty(),
                "a nonterminal page made no progress"
            );
            seen.extend(
                page.items
                    .iter()
                    .map(|item| item.version_id.as_str().to_owned()),
            );

            if !page.truncated {
                break;
            }
            let next_cursor = page
                .next_cursor
                .expect("a truncated version page carries its key marker");
            let next_marker = page
                .next_version_id_marker
                .expect("a page ending within one key carries its version marker");
            assert_eq!(next_cursor, "one-key");
            assert_eq!(
                page.items.last().unwrap().version_id.as_str(),
                next_marker,
                "the pair names the last returned entry"
            );
            if let Some(previous) = &previous_marker {
                assert!(
                    next_marker < *previous,
                    "the descending version marker must advance monotonically: \
                     previous={previous}, next={next_marker}"
                );
            }
            previous_marker = Some(next_marker.clone());
            cursor = Some(next_cursor);
            version_marker = Some(next_marker);
        }

        let expected: Vec<String> = (0..TOTAL)
            .rev()
            .map(|ordinal| format!("version-{ordinal:08}"))
            .collect();
        assert_eq!(pages, 3);
        assert_eq!(
            seen, expected,
            "every version is returned once, without gaps"
        );
    }
}

#[tokio::test]
async fn conditional_writes_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        let k = ObjectKey::parse("k").unwrap();
        s.submit(put(
            row(&bk, "k", VersionId::null(), "e1", 3),
            Precondition::default(),
        ))
        .await
        .unwrap();

        // If-None-Match * fails once the object exists.
        let err = s
            .submit(put(
                row(&bk, "k", VersionId::null(), "e2", 3),
                Precondition {
                    if_match: None,
                    if_none_match: Some(IfNoneMatch::Any),
                },
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::PreconditionFailed));

        // If-Match wrong etag fails.
        let err = s
            .submit(put(
                row(&bk, "k", VersionId::null(), "e3", 3),
                Precondition {
                    if_match: Some(ETag::from_string("WRONG".into())),
                    if_none_match: None,
                },
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::PreconditionFailed));

        // If-Match correct etag succeeds.
        s.submit(put(
            row(&bk, "k", VersionId::null(), "e3", 3),
            Precondition {
                if_match: Some(ETag::from_string("e1".into())),
                if_none_match: None,
            },
        ))
        .await
        .unwrap();
        assert_eq!(
            s.current_version(&bk, &k)
                .await
                .unwrap()
                .unwrap()
                .etag
                .as_str(),
            "e3"
        );
    }
}

#[tokio::test]
async fn quota_enforcement_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        s.submit(Mutation::CreateBucket(Box::new(bucket(
            "bkt",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();
        s.submit(Mutation::SetBucketQuota {
            bucket: bk.clone(),
            quota_bytes: Some(100),
        })
        .await
        .unwrap();
        assert_eq!(s.get_bucket_quota(&bk).await.unwrap(), Some(100));

        s.submit(put(
            row(&bk, "k1", VersionId::from_string("v1".into()), "e", 60),
            Precondition::default(),
        ))
        .await
        .unwrap();
        // 60 + 50 = 110 > 100 -> rejected, nothing committed.
        let err = s
            .submit(put(
                row(&bk, "k2", VersionId::from_string("v1".into()), "e", 50),
                Precondition::default(),
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::QuotaExceeded));
        assert_eq!(s.aggregate_counts().await.unwrap().logical_bytes, 60);

        // Raising the quota lets it through.
        s.submit(Mutation::SetBucketQuota {
            bucket: bk.clone(),
            quota_bytes: Some(200),
        })
        .await
        .unwrap();
        s.submit(put(
            row(&bk, "k2", VersionId::from_string("v1".into()), "e", 50),
            Precondition::default(),
        ))
        .await
        .unwrap();
        assert_eq!(s.aggregate_counts().await.unwrap().logical_bytes, 110);
    }
}

#[tokio::test]
async fn multipart_lifecycle_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        let upload = UploadId::from_string("upload-1".into());
        let session = MultipartSession {
            upload_id: upload.clone(),
            bucket: bk.clone(),
            key: ObjectKey::parse("big").unwrap(),
            content_type: "application/octet-stream".to_owned(),
            status: MultipartStatus::Active,
            owner_id: UserId("owner".to_owned()),
            initiated_by: UserId("owner".to_owned()),
            intended_acl: None,
            user_metadata: Vec::new(),
            initial_tags: Vec::new(),
            lock_intent: ExplicitObjectLockIntent::default(),
            sse_requested: false,
            encrypt_parts: false,
            sse_kms_requested: false,
            sse_kms_key_id: None,
            sse_bucket_key_enabled: false,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        };
        assert!(matches!(
            s.submit(Mutation::CreateMultipart {
                session: Box::new(session),
                limits: cairn_types::meta::MultipartLimits::default(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartCreated(_)
        ));
        assert!(s.get_multipart(&upload).await.unwrap().is_some());

        // Record two parts.
        for n in 1u16..=2 {
            let attempt_id = format!("part-{n}");
            let part = PartRecord {
                part_number: n,
                size: 5 * 1024 * 1024,
                etag: format!("petag{n}"),
                storage_path: StoragePath::from_string(format!("bkt/part-{n}")),
                checksum: None,
                part_dek: None,
            };
            s.submit(Mutation::ReserveMultipartPart {
                upload_id: upload.clone(),
                part_number: n,
                attempt_id: attempt_id.clone(),
                reserved_bytes: part.size,
                max_parts_per_upload: 10_000,
                now: Timestamp(2),
            })
            .await
            .unwrap();
            s.submit(Mutation::RecordPart {
                upload_id: upload.clone(),
                attempt_id,
                part,
            })
            .await
            .unwrap();
        }
        let parts = s.list_parts(&upload, 0, 100).await.unwrap();
        assert_eq!(parts.items.len(), 2);
        let first_path = StoragePath::from_string("bkt/part-1".to_owned());
        assert_eq!(
            s.submit(Mutation::ResolveMultipartPartWrite {
                upload_id: upload.clone(),
                part_number: 1,
                storage_path: first_path.clone(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartPartWriteResolved { referenced: true }
        );
        let retry_path = StoragePath::from_string("bkt/part-1-retry".to_owned());
        s.submit(Mutation::ReserveMultipartPart {
            upload_id: upload.clone(),
            part_number: 1,
            attempt_id: "part-1-retry".to_owned(),
            reserved_bytes: 5 * 1024 * 1024,
            max_parts_per_upload: 10_000,
            now: Timestamp(3),
        })
        .await
        .unwrap();
        s.submit(Mutation::RecordPart {
            upload_id: upload.clone(),
            attempt_id: "part-1-retry".to_owned(),
            part: PartRecord {
                part_number: 1,
                size: 5 * 1024 * 1024,
                etag: "petag1-retry".to_owned(),
                storage_path: retry_path.clone(),
                checksum: None,
                part_dek: None,
            },
        })
        .await
        .unwrap();
        assert_eq!(
            s.submit(Mutation::ResolveMultipartPartWrite {
                upload_id: upload.clone(),
                part_number: 1,
                storage_path: first_path,
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartPartWriteResolved { referenced: false }
        );
        assert_eq!(
            s.submit(Mutation::ResolveMultipartPartWrite {
                upload_id: upload.clone(),
                part_number: 1,
                storage_path: retry_path,
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartPartWriteResolved { referenced: true }
        );

        // list_multipart_uploads shows the active session.
        let active = s
            .list_multipart_uploads(
                &bk,
                &ListQuery {
                    limit: 100,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(active.items.len(), 1);

        // Audit 2026-07: paging on the (key-marker, upload-id-marker) PAIR must behave identically
        // in both engines, including within a single key. Add a second session on the SAME key so
        // a key-only marker could never advance past it.
        let upload2 = UploadId::from_string("upload-2".into());
        s.submit(Mutation::CreateMultipart {
            session: Box::new(MultipartSession {
                upload_id: upload2.clone(),
                bucket: bk.clone(),
                key: ObjectKey::parse("big").unwrap(),
                content_type: "application/octet-stream".to_owned(),
                status: MultipartStatus::Active,
                owner_id: UserId("owner".to_owned()),
                initiated_by: UserId("owner".to_owned()),
                intended_acl: None,
                user_metadata: Vec::new(),
                initial_tags: Vec::new(),
                lock_intent: ExplicitObjectLockIntent::default(),
                sse_requested: false,
                encrypt_parts: false,
                sse_kms_requested: false,
                sse_kms_key_id: None,
                sse_bucket_key_enabled: false,
                created_at: Timestamp(1),
                updated_at: Timestamp(1),
            }),
            limits: cairn_types::meta::MultipartLimits::default(),
        })
        .await
        .unwrap();
        let page1 = s
            .list_multipart_uploads(
                &bk,
                &ListQuery {
                    limit: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(page1.truncated);
        assert_eq!(page1.items.len(), 1);
        assert_eq!(page1.next_cursor.as_deref(), Some("big"));
        assert_eq!(
            page1.next_version_id_marker.as_deref(),
            Some(page1.items[0].upload_id.as_str()),
            "the upload-id half of the resume pair must be emitted"
        );
        let page2 = s
            .list_multipart_uploads(
                &bk,
                &ListQuery {
                    cursor: page1.next_cursor.clone(),
                    version_id_marker: page1.next_version_id_marker.clone(),
                    limit: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page2.items.len(), 1);
        assert_ne!(
            page2.items[0].upload_id, page1.items[0].upload_id,
            "the pair must resume mid-key, not re-serve page 1"
        );
        assert!(!page2.truncated);
        // Clean up so the rest of the lifecycle assertions see only `upload`.
        s.submit(Mutation::AbortMultipart(upload2)).await.unwrap();

        // Claim then complete.
        let initial_claim_token = MultipartClaimToken::generate();
        let claim = s
            .submit(Mutation::ClaimMultipart {
                upload_id: upload.clone(),
                claim_token: initial_claim_token,
            })
            .await
            .unwrap();
        assert!(matches!(
            claim,
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));
        // Re-claim is AlreadyClaimed (status now 'completing').
        assert!(matches!(
            s.submit(Mutation::ClaimMultipart {
                upload_id: upload.clone(),
                claim_token: MultipartClaimToken::generate(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::AlreadyClaimed)
        ));
        // A claimed completion owns the terminal transition. Abort loses without deleting the
        // session or parts, and a genuine failure can release that ownership for a retry.
        assert!(matches!(
            s.submit(Mutation::AbortMultipart(upload.clone()))
                .await
                .unwrap(),
            MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::NotOwner)
        ));
        assert_eq!(s.list_parts(&upload, 0, 100).await.unwrap().items.len(), 2);
        s.submit(Mutation::RecoverMultipartClaims).await.unwrap();
        assert_eq!(
            s.get_multipart(&upload)
                .await
                .unwrap()
                .expect("recovered multipart")
                .status,
            MultipartStatus::Active
        );
        let released_claim_token = MultipartClaimToken::generate();
        assert!(matches!(
            s.submit(Mutation::ClaimMultipart {
                upload_id: upload.clone(),
                claim_token: released_claim_token.clone(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));
        assert!(matches!(
            s.submit(Mutation::ReleaseMultipartClaim {
                upload_id: upload.clone(),
                claim_token: released_claim_token.clone(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartClaimRelease(ClaimReleaseOutcome::Released)
        ));
        let final_claim_token = MultipartClaimToken::generate();
        assert!(matches!(
            s.submit(Mutation::ClaimMultipart {
                upload_id: upload.clone(),
                claim_token: final_claim_token.clone(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));

        // A delayed recovery from the released attempt cannot ABA-release or complete the newer
        // owner. The exact final token remains authoritative until it completes.
        assert!(matches!(
            s.submit(Mutation::ReleaseMultipartClaim {
                upload_id: upload.clone(),
                claim_token: released_claim_token.clone(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartClaimRelease(ClaimReleaseOutcome::NotOwner)
        ));

        let assembled = row(
            &bk,
            "big",
            VersionId::from_string("v1".into()),
            "final-etag",
            10 * 1024 * 1024,
        );
        assert!(matches!(
            s.submit(Mutation::CompleteMultipart {
                upload_id: upload.clone(),
                claim_token: released_claim_token,
                row: Box::new(assembled.clone()),
                precondition: Precondition::default(),
                replication: Vec::new(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::NotOwner)
        ));
        assert_eq!(
            s.get_multipart(&upload)
                .await
                .unwrap()
                .expect("newer claimant still owns the session")
                .status,
            MultipartStatus::Completing
        );
        let out = s
            .submit(Mutation::CompleteMultipart {
                upload_id: upload.clone(),
                claim_token: final_claim_token,
                row: Box::new(assembled),
                precondition: Precondition::default(),
                replication: Vec::new(),
            })
            .await
            .unwrap();
        assert!(matches!(
            out,
            MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::Completed { .. })
        ));
        // The session is gone and the object exists.
        assert!(s.get_multipart(&upload).await.unwrap().is_none());
        assert_eq!(
            s.current_version(&bk, &ObjectKey::parse("big").unwrap())
                .await
                .unwrap()
                .unwrap()
                .etag
                .as_str(),
            "final-etag"
        );

        // The opposite ordering is equally atomic: Abort consumes active first, so a later claim
        // and even a direct completion mutation cannot create an object.
        let aborted_upload = UploadId::from_string("upload-abort-wins".into());
        s.submit(Mutation::CreateMultipart {
            session: Box::new(MultipartSession {
                upload_id: aborted_upload.clone(),
                bucket: bk.clone(),
                key: ObjectKey::parse("aborted").unwrap(),
                content_type: "application/octet-stream".to_owned(),
                status: MultipartStatus::Active,
                owner_id: UserId("owner".to_owned()),
                initiated_by: UserId("owner".to_owned()),
                intended_acl: None,
                user_metadata: Vec::new(),
                initial_tags: Vec::new(),
                lock_intent: ExplicitObjectLockIntent::default(),
                sse_requested: false,
                encrypt_parts: false,
                sse_kms_requested: false,
                sse_kms_key_id: None,
                sse_bucket_key_enabled: false,
                created_at: Timestamp(1),
                updated_at: Timestamp(1),
            }),
            limits: cairn_types::meta::MultipartLimits::default(),
        })
        .await
        .unwrap();
        assert!(matches!(
            s.submit(Mutation::AbortMultipart(aborted_upload.clone()))
                .await
                .unwrap(),
            MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::Aborted)
        ));
        let aborted_claim_token = MultipartClaimToken::generate();
        assert!(matches!(
            s.submit(Mutation::ClaimMultipart {
                upload_id: aborted_upload.clone(),
                claim_token: aborted_claim_token.clone(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::NotFound)
        ));
        assert!(matches!(
            s.submit(Mutation::CompleteMultipart {
                upload_id: aborted_upload,
                claim_token: aborted_claim_token,
                row: Box::new(row(
                    &bk,
                    "aborted",
                    VersionId::from_string("v1".into()),
                    "must-not-land",
                    1,
                )),
                precondition: Precondition::default(),
                replication: Vec::new(),
            })
            .await
            .unwrap(),
            MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::NotOwner)
        ));
        assert!(
            s.current_version(&bk, &ObjectKey::parse("aborted").unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }
}

/// v21 parity (ARCH 27, Increment 3a): `encrypt_parts` on the session and `part_dek` on a part must
/// round-trip identically through both backends. Guards the positional `MULTIPART_COLS[11]` /
/// `PART_COLS[5]` mirror + the v21 migration in `cairn-meta-async`.
#[tokio::test]
async fn multipart_part_encryption_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("enc").unwrap();
        let upload = UploadId::from_string("enc-upload".into());
        let session = MultipartSession {
            upload_id: upload.clone(),
            bucket: bk.clone(),
            key: ObjectKey::parse("big").unwrap(),
            content_type: "application/octet-stream".to_owned(),
            status: MultipartStatus::Active,
            owner_id: UserId("owner".to_owned()),
            initiated_by: UserId("owner".to_owned()),
            intended_acl: None,
            user_metadata: Vec::new(),
            initial_tags: Vec::new(),
            lock_intent: ExplicitObjectLockIntent::default(),
            sse_requested: true,
            encrypt_parts: true,
            sse_kms_requested: false,
            sse_kms_key_id: None,
            sse_bucket_key_enabled: false,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        };
        s.submit(Mutation::CreateMultipart {
            session: Box::new(session),
            limits: cairn_types::meta::MultipartLimits::default(),
        })
        .await
        .unwrap();
        // The pinned decision survives the round trip.
        assert!(
            s.get_multipart(&upload)
                .await
                .unwrap()
                .unwrap()
                .encrypt_parts
        );

        let part = PartRecord {
            part_number: 1,
            size: 5 * 1024 * 1024,
            etag: "petag".to_owned(),
            storage_path: StoragePath::from_string("enc/part-1".to_owned()),
            checksum: None,
            part_dek: Some("c2VhbGVkLWRlaw==".to_owned()),
        };
        s.submit(Mutation::ReserveMultipartPart {
            upload_id: upload.clone(),
            part_number: 1,
            attempt_id: "encrypted-part".to_owned(),
            reserved_bytes: part.size,
            max_parts_per_upload: 10_000,
            now: Timestamp(2),
        })
        .await
        .unwrap();
        s.submit(Mutation::RecordPart {
            upload_id: upload.clone(),
            attempt_id: "encrypted-part".to_owned(),
            part,
        })
        .await
        .unwrap();
        let parts = s.list_parts(&upload, 0, 100).await.unwrap();
        assert_eq!(parts.items.len(), 1);
        assert_eq!(parts.items[0].part_dek.as_deref(), Some("c2VhbGVkLWRlaw=="));

        // A part without a DEK reads back None (the legacy / plaintext-part case).
        let plain = PartRecord {
            part_number: 2,
            size: 5 * 1024 * 1024,
            etag: "petag2".to_owned(),
            storage_path: StoragePath::from_string("enc/part-2".to_owned()),
            checksum: None,
            part_dek: None,
        };
        s.submit(Mutation::ReserveMultipartPart {
            upload_id: upload.clone(),
            part_number: 2,
            attempt_id: "plain-part".to_owned(),
            reserved_bytes: plain.size,
            max_parts_per_upload: 10_000,
            now: Timestamp(3),
        })
        .await
        .unwrap();
        s.submit(Mutation::RecordPart {
            upload_id: upload.clone(),
            attempt_id: "plain-part".to_owned(),
            part: plain,
        })
        .await
        .unwrap();
        let parts = s.list_parts(&upload, 0, 100).await.unwrap();
        let p2 = parts.items.iter().find(|p| p.part_number == 2).unwrap();
        assert_eq!(p2.part_dek, None);
    }
}

/// v22 parity (ARCH 27, Increment 3b): the explicit-KMS intent fields (`sse_kms_requested`,
/// `sse_kms_key_id`, `sse_bucket_key_enabled`) on a multipart session must round-trip identically
/// through both backends. Guards the positional `MULTIPART_COLS[12..15]` mirror + the v22 migration
/// in `cairn-meta-async`.
#[tokio::test]
async fn multipart_kms_intent_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("kms").unwrap();
        let upload = UploadId::from_string("kms-upload".into());
        let session = MultipartSession {
            upload_id: upload.clone(),
            bucket: bk.clone(),
            key: ObjectKey::parse("big").unwrap(),
            content_type: "application/octet-stream".to_owned(),
            status: MultipartStatus::Active,
            owner_id: UserId("owner".to_owned()),
            initiated_by: UserId("owner".to_owned()),
            intended_acl: None,
            user_metadata: Vec::new(),
            initial_tags: Vec::new(),
            lock_intent: ExplicitObjectLockIntent::default(),
            sse_requested: false,
            encrypt_parts: true,
            sse_kms_requested: true,
            sse_kms_key_id: Some("alias/my-key".to_owned()),
            sse_bucket_key_enabled: true,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        };
        s.submit(Mutation::CreateMultipart {
            session: Box::new(session),
            limits: cairn_types::meta::MultipartLimits::default(),
        })
        .await
        .unwrap();
        let got = s.get_multipart(&upload).await.unwrap().unwrap();
        assert!(got.sse_kms_requested);
        assert_eq!(got.sse_kms_key_id.as_deref(), Some("alias/my-key"));
        assert!(got.sse_bucket_key_enabled);
        assert!(!got.sse_requested);
    }
}

#[tokio::test]
async fn tags_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        let k = ObjectKey::parse("k").unwrap();
        let v = VersionId::from_string("v1".into());
        s.submit(put(
            row(&bk, "k", v.clone(), "e", 3),
            Precondition::default(),
        ))
        .await
        .unwrap();

        s.submit(Mutation::PutObjectTags {
            bucket: bk.clone(),
            key: k.clone(),
            version_id: v.clone(),
            tags: vec![
                ("env".to_owned(), "prod".to_owned()),
                ("team".to_owned(), "core".to_owned()),
            ],
        })
        .await
        .unwrap();
        assert_eq!(
            s.get_object_tags(&bk, &k, &v).await.unwrap(),
            vec![
                ("env".to_owned(), "prod".to_owned()),
                ("team".to_owned(), "core".to_owned())
            ]
        );

        // Replace.
        s.submit(Mutation::PutObjectTags {
            bucket: bk.clone(),
            key: k.clone(),
            version_id: v.clone(),
            tags: vec![("only".to_owned(), "one".to_owned())],
        })
        .await
        .unwrap();
        assert_eq!(
            s.get_object_tags(&bk, &k, &v).await.unwrap(),
            vec![("only".to_owned(), "one".to_owned())]
        );

        // Delete.
        s.submit(Mutation::DeleteObjectTags {
            bucket: bk.clone(),
            key: k.clone(),
            version_id: v.clone(),
        })
        .await
        .unwrap();
        assert!(s.get_object_tags(&bk, &k, &v).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn object_acl_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        let k = ObjectKey::parse("obj").unwrap();
        let v = VersionId::from_string("v1".into());
        s.submit(put(
            row(&bk, "obj", v.clone(), "e", 3),
            Precondition::default(),
        ))
        .await
        .unwrap();
        assert!(
            s.get_version(&bk, &k, &v)
                .await
                .unwrap()
                .unwrap()
                .acl
                .is_none()
        );

        let acl = Acl {
            owner: UserId("owner".to_owned()),
            grants: vec![Grant {
                grantee: Grantee::AllUsers,
                permission: Permission::Read,
            }],
        };
        s.submit(Mutation::SetObjectAcl {
            bucket: bk.clone(),
            key: k.clone(),
            version_id: v.clone(),
            acl: Some(acl.clone()),
        })
        .await
        .unwrap();
        assert_eq!(
            s.get_version(&bk, &k, &v).await.unwrap().unwrap().acl,
            Some(acl)
        );

        s.submit(Mutation::SetObjectAcl {
            bucket: bk.clone(),
            key: k.clone(),
            version_id: v.clone(),
            acl: None,
        })
        .await
        .unwrap();
        assert!(
            s.get_version(&bk, &k, &v)
                .await
                .unwrap()
                .unwrap()
                .acl
                .is_none()
        );
    }
}

#[tokio::test]
async fn replication_outbox_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        s.submit(Mutation::CreateBucket(Box::new(bucket(
            "bkt",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();
        let v = VersionId::from_string("v1".into());
        let entry = OutboxEntry {
            enqueued_at: Timestamp(0),
            id: "out-1".to_owned(),
            bucket: bk.clone(),
            key: ObjectKey::parse("k").unwrap(),
            version_id: v.clone(),
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
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bk, "k", v.clone(), "e", 3)),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![entry],
        })
        .await
        .unwrap();

        // Claim due entries.
        let claimed = s.claim_replication_batch(10, Timestamp(1)).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, "out-1");

        // Mark done updates the version status to completed.
        s.submit(Mutation::MarkReplicationDone {
            id: "out-1".to_owned(),
            now: Timestamp(0),
        })
        .await
        .unwrap();
        assert_eq!(
            s.object_replication_status(&bk, &ObjectKey::parse("k").unwrap(), &v)
                .await
                .unwrap(),
            Some(ReplicationStatus::Completed)
        );

        // A terminal failure lands on the failed list; a retryable one does not.
        let v2 = VersionId::from_string("v2".into());
        let e2 = OutboxEntry {
            enqueued_at: Timestamp(0),
            id: "out-2".to_owned(),
            bucket: bk.clone(),
            key: ObjectKey::parse("k2").unwrap(),
            version_id: v2.clone(),
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
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bk, "k2", v2.clone(), "e", 3)),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![e2],
        })
        .await
        .unwrap();
        s.submit(Mutation::MarkReplicationFailed {
            id: "out-2".to_owned(),
            error: "down".to_owned(),
            next_attempt_at: None,
        })
        .await
        .unwrap();
        let failed = s.list_failed_replication(100).await.unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].id, "out-2");
        assert_eq!(failed[0].attempts, 1);
        assert_eq!(failed[0].last_error.as_deref(), Some("down"));
    }
}

#[tokio::test]
async fn users_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        assert_eq!(s.count_users().await.unwrap(), 0);
        let rec = user_record("u1", "AKIA1");
        assert!(matches!(
            s.submit(Mutation::CreateUser(Box::new(rec.clone())))
                .await
                .unwrap(),
            MutationOutcome::UserCreated(_)
        ));
        assert_eq!(s.count_users().await.unwrap(), 1);

        let by_bearer = s.user_by_bearer_key("AKIA1").await.unwrap().unwrap();
        assert_eq!(by_bearer.user.id.0, "u1");
        assert_eq!(by_bearer.secret_hash, "hash");

        let by_sig = s.user_by_sigv4_key("SIG-AKIA1").await.unwrap().unwrap();
        assert_eq!(by_sig.user.id.0, "u1");
        assert_eq!(by_sig.secret_ciphertext, vec![1, 2, 3, 4]);
        assert_eq!(by_sig.secret_nonce, vec![9, 8, 7]);

        // list_users.
        assert_eq!(s.list_users().await.unwrap().len(), 1);

        // Deactivate.
        s.submit(Mutation::DeactivateUser(UserId("u1".to_owned())))
            .await
            .unwrap();
        assert!(
            !s.user_by_bearer_key("AKIA1")
                .await
                .unwrap()
                .unwrap()
                .user
                .is_active
        );
    }
}

#[tokio::test]
async fn import_jobs_parity() {
    use cairn_types::meta::{
        ImportBucketProgress, ImportJobListQuery, ImportJobRecord, ImportState,
    };
    use cairn_types::time::Timestamp;
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        assert!(
            s.list_import_jobs(&Default::default())
                .await
                .unwrap()
                .items
                .is_empty()
        );
        let bucket = |done: u64, cursor: Option<&str>, st: ImportState| ImportBucketProgress {
            source_bucket: "src".to_owned(),
            dest_bucket: "dst".to_owned(),
            objects_done: done,
            objects_total: 10,
            bytes_done: done * 10,
            bytes_total: 100,
            cursor: cursor.map(str::to_owned),
            state: st,
            last_error: None,
        };
        let rec = ImportJobRecord {
            id: "job1".to_owned(),
            source_endpoint: "https://peer.example.com:9000".to_owned(),
            source_region: "us-east-1".to_owned(),
            access_key_id: "AKSRC".to_owned(),
            secret_ciphertext: vec![1, 2, 3, 4],
            secret_nonce: None,
            ca_cert_pem: Some("-----BEGIN CERTIFICATE-----".to_owned()),
            insecure_skip_verify: false,
            workers: 8,
            state: ImportState::Pending,
            buckets: vec![bucket(0, None, ImportState::Pending)],
            objects_done: 0,
            objects_total: 10,
            bytes_done: 0,
            bytes_total: 100,
            last_error: None,
            lease_until: None,
            created_at: Timestamp(1000),
            updated_at: Timestamp(1000),
        };
        s.submit(Mutation::CreateImportJob(Box::new(rec.clone())))
            .await
            .unwrap();

        // The scheduler primitive selects one oldest id without decoding the complete job history.
        // Equal creation timestamps resolve by id, consistently in both engines.
        let mut older_id = rec;
        older_id.id = "job0".to_owned();
        s.submit(Mutation::CreateImportJob(Box::new(older_id)))
            .await
            .unwrap();
        assert_eq!(
            s.next_import_job_id(ImportState::Pending).await.unwrap(),
            Some("job0".to_owned())
        );

        // list + get are secret-free (has_ca_cert flag, no ciphertext).
        let jobs = s
            .list_import_jobs(&ImportJobListQuery {
                cursor: None,
                limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(jobs.items.len(), 1);
        assert_eq!(jobs.items[0].id, "job1");
        assert!(jobs.items[0].has_ca_cert);
        assert_eq!(jobs.items[0].access_key_id, "AKSRC");
        let second = s
            .list_import_jobs(&ImportJobListQuery {
                cursor: jobs.next_cursor,
                limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(second.items[0].id, "job0");
        assert!(second.next_cursor.is_none());
        let got = s.get_import_job("job1").await.unwrap().unwrap();
        assert_eq!(got.state, ImportState::Pending);
        assert_eq!(got.objects_total, 10);
        assert_eq!(got.buckets.len(), 1);

        // Progress checkpoint (per-bucket cursor + counters + lease).
        s.submit(Mutation::UpdateImportJobProgress {
            id: "job1".to_owned(),
            buckets: vec![bucket(5, Some("tok"), ImportState::Running)],
            objects_done: 5,
            objects_total: 10,
            bytes_done: 50,
            bytes_total: 100,
            last_error: None,
            lease_until: Some(Timestamp(2000)),
            updated_at: Timestamp(1500),
        })
        .await
        .unwrap();
        let got = s.get_import_job("job1").await.unwrap().unwrap();
        assert_eq!(got.objects_done, 5);
        assert_eq!(got.buckets[0].cursor.as_deref(), Some("tok"));

        // Terminal state, then prune finished jobs past the horizon in bounded transactions.
        s.submit(Mutation::SetImportJobState {
            id: "job1".to_owned(),
            state: ImportState::Completed,
            last_error: None,
            lease_until: None,
            updated_at: Timestamp(3000),
        })
        .await
        .unwrap();
        assert_eq!(
            s.get_import_job("job1").await.unwrap().unwrap().state,
            ImportState::Completed
        );
        for id in ["job2", "job3"] {
            let mut terminal = s.get_import_job_record("job1").await.unwrap().unwrap();
            terminal.id = id.to_owned();
            s.submit(Mutation::CreateImportJob(Box::new(terminal)))
                .await
                .unwrap();
        }
        assert_eq!(
            s.submit(Mutation::PruneImportJobs {
                before_ms: 4000,
                limit: 1,
            })
            .await
            .unwrap(),
            MutationOutcome::ImportJobsPruned(1)
        );
        let mut remaining_terminal = 0;
        for id in ["job1", "job2", "job3"] {
            if s.get_import_job(id).await.unwrap().is_some() {
                remaining_terminal += 1;
            }
        }
        assert_eq!(
            remaining_terminal, 2,
            "one prune transaction must not exceed its row budget"
        );
        assert_eq!(
            s.submit(Mutation::PruneImportJobs {
                before_ms: 4000,
                limit: u32::MAX,
            })
            .await
            .unwrap(),
            MutationOutcome::ImportJobsPruned(2),
            "the backend cap still drains the remaining small batch"
        );
        for id in ["job1", "job2", "job3"] {
            assert!(s.get_import_job(id).await.unwrap().is_none());
        }
        assert_eq!(
            s.get_import_job("job0").await.unwrap().unwrap().state,
            ImportState::Pending,
            "retention must never prune runnable work"
        );
    }
}

#[tokio::test]
async fn aggregate_counts_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        s.submit(Mutation::CreateBucket(Box::new(bucket(
            "bkt",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();
        let bk = BucketName::parse("bkt").unwrap();
        s.submit(put(
            row(&bk, "k1", VersionId::from_string("v1".into()), "e", 10),
            Precondition::default(),
        ))
        .await
        .unwrap();
        s.submit(put(
            row(&bk, "k1", VersionId::from_string("v2".into()), "e", 20),
            Precondition::default(),
        ))
        .await
        .unwrap();
        s.submit(put(
            row(&bk, "k2", VersionId::from_string("v1".into()), "e", 30),
            Precondition::default(),
        ))
        .await
        .unwrap();

        let c = s.aggregate_counts().await.unwrap();
        assert_eq!(c.buckets, 1);
        assert_eq!(c.objects, 2); // two current keys
        assert_eq!(c.versions, 3); // three version rows
        assert_eq!(c.logical_bytes, 60);
    }
}

#[tokio::test]
async fn bucket_counts_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        for name in ["bkt", "empty"] {
            s.submit(Mutation::CreateBucket(Box::new(bucket(
                name,
                VersioningState::Enabled,
            ))))
            .await
            .unwrap();
        }
        let bk = BucketName::parse("bkt").unwrap();
        s.submit(put(
            row(&bk, "k1", VersionId::from_string("v1".into()), "e", 10),
            Precondition::default(),
        ))
        .await
        .unwrap();
        s.submit(put(
            row(&bk, "k1", VersionId::from_string("v2".into()), "e", 20),
            Precondition::default(),
        ))
        .await
        .unwrap();

        let counts = s.bucket_counts().await.unwrap();
        // Sorted by name; the empty bucket appears with zeros.
        assert_eq!(counts.len(), 2);
        assert_eq!(counts[0].bucket, "bkt");
        assert_eq!(counts[0].objects, 1); // one current key
        assert_eq!(counts[0].logical_bytes, 30); // both versions counted
        assert_eq!(counts[1].bucket, "empty");
        assert_eq!(counts[1].objects, 0);
        assert_eq!(counts[1].logical_bytes, 0);
    }
}

#[tokio::test]
async fn reconcile_oracle_parity() {
    let (a, b) = both().await;
    let bk = BucketName::parse("bkt").unwrap();
    let r = row(&bk, "k", VersionId::null(), "e", 3);
    let live = r.storage_path.clone().unwrap();
    let orphan = StoragePath::from_string("bkt/orphan".into());

    a.submit(put(r.clone(), Precondition::default()))
        .await
        .unwrap();
    b.submit(put(r, Precondition::default())).await.unwrap();

    let ans_a = a
        .reconcile_oracle()
        .live_blobs(&[live.clone(), orphan.clone()])
        .await
        .unwrap();
    let ans_b = b
        .reconcile_oracle()
        .live_blobs(&[live, orphan])
        .await
        .unwrap();
    assert_eq!(ans_a, vec![true, false]);
    assert_eq!(ans_a, ans_b);

    let up = UploadId::from_string("nope".into());
    assert_eq!(
        a.reconcile_oracle().live_session(&up).await.unwrap(),
        b.reconcile_oracle().live_session(&up).await.unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn group_commit_isolates_failed_mutations_parity() {
    // The async writer's per-mutation savepoint isolation must match the rusqlite writer's: a
    // doomed conditional put rolls back only itself while its concurrent batch-mates all commit.
    let store = cairn_meta_async::open_libsql_in_memory().await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    store
        .submit(put(
            row(&b, "exists", VersionId::null(), "e", 3),
            Precondition::default(),
        ))
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..49 {
        let s = store.clone();
        let bb = b.clone();
        handles.push(tokio::spawn(async move {
            s.submit(put(
                row(&bb, &format!("k{i:03}"), VersionId::null(), "e", 3),
                Precondition::default(),
            ))
            .await
        }));
    }
    let s = store.clone();
    let bb = b.clone();
    let doomed = tokio::spawn(async move {
        s.submit(put(
            row(&bb, "exists", VersionId::null(), "e2", 3),
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
    assert_eq!(store.aggregate_counts().await.unwrap().objects, 50);
}

/// PARITY: `RequeueReplicationVersions` (ARCH 20.5, the Stage-2 repair primitive) must behave
/// identically on both engines — the outbox rows go back to `pending` with the attempt budget reset,
/// the version-row ledger stops claiming `completed`, `only_encrypted` scopes it to versions
/// carrying an `sse_descriptor`, and an inbound `replica` stamp is untouched.
#[tokio::test]
async fn requeue_replication_versions_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        s.submit(Mutation::CreateBucket(Box::new(bucket(
            "bkt",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();

        let mk = |key: &str, v: &VersionId, id: &str| OutboxEntry {
            enqueued_at: Timestamp(0),
            id: id.to_owned(),
            bucket: bk.clone(),
            key: ObjectKey::parse(key).unwrap(),
            version_id: v.clone(),
            operation: ReplicationOp::ObjectCreate,
            rule_id: "r1".to_owned(),
            target_arn: None,
            attempts: 0,
            next_attempt_at: Timestamp(0),
            status: ReplicationStatus::Pending,
            last_error: None,
            priority: 0,
            lease_until: None,
        };

        // One encrypted version and one plaintext version, both shipped successfully.
        let venc = VersionId::from_string("00000001".into());
        let mut enc = row(&bk, "enc", venc.clone(), "e", 3);
        enc.sse_descriptor =
            Some(r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":""}"#.to_owned());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(enc),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![mk("enc", &venc, "backfill:r1:enc:1")],
        })
        .await
        .unwrap();
        let vplain = VersionId::from_string("00000002".into());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bk, "plain", vplain.clone(), "e", 3)),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![mk("plain", &vplain, "backfill:r1:plain:2")],
        })
        .await
        .unwrap();
        s.claim_replication_batch(10, Timestamp(1)).await.unwrap();
        for id in ["backfill:r1:enc:1", "backfill:r1:plain:2"] {
            s.submit(Mutation::MarkReplicationDone {
                id: id.to_owned(),
                now: Timestamp(0),
            })
            .await
            .unwrap();
        }
        assert!(
            s.claim_replication_batch(10, Timestamp(2))
                .await
                .unwrap()
                .is_empty(),
            "a completed entry is never re-claimed"
        );

        s.submit(Mutation::RequeueReplicationVersions {
            bucket: bk.clone(),
            only_encrypted: true,
            after_key: None,
            now: Timestamp(5000),
            limit: 1000,
        })
        .await
        .unwrap();

        let claimed = s
            .claim_replication_batch(10, Timestamp(6000))
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1, "only the encrypted version is requeued");
        assert_eq!(claimed[0].id, "backfill:r1:enc:1");
        assert_eq!(claimed[0].attempts, 0);
        assert_eq!(
            s.object_replication_status(&bk, &ObjectKey::parse("enc").unwrap(), &venc)
                .await
                .unwrap(),
            Some(ReplicationStatus::Pending)
        );
        assert_eq!(
            s.object_replication_status(&bk, &ObjectKey::parse("plain").unwrap(), &vplain)
                .await
                .unwrap(),
            Some(ReplicationStatus::Completed)
        );
    }
}

/// PARITY: the `only_encrypted` scope is KEY-level on both engines, never version-level.
///
/// A per-version filter requeues the encrypted `v1` of a key and leaves its later siblings settled,
/// so `v1` is PUT at the destination last: a later plaintext version means the mirror's current
/// object reverts to the old one, and a later delete marker means a deleted object is resurrected
/// there (the resync backfill enumerates current objects and never re-enqueues the marker). If the
/// two engines disagree about this, one of them silently corrupts the mirror.
#[tokio::test]
async fn requeue_replication_versions_is_key_scoped_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        s.submit(Mutation::CreateBucket(Box::new(bucket(
            "bkt",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();

        let mk = |key: &str, v: &VersionId, id: &str| OutboxEntry {
            enqueued_at: Timestamp(0),
            id: id.to_owned(),
            bucket: bk.clone(),
            key: ObjectKey::parse(key).unwrap(),
            version_id: v.clone(),
            operation: ReplicationOp::ObjectCreate,
            rule_id: "r1".to_owned(),
            target_arn: None,
            attempts: 0,
            next_attempt_at: Timestamp(0),
            status: ReplicationStatus::Pending,
            last_error: None,
            priority: 0,
            lease_until: None,
        };

        // key `k`: an ENCRYPTED v1, then a PLAINTEXT v2 that supersedes it.
        let v1 = VersionId::from_string("00000001".into());
        let mut enc = row(&bk, "k", v1.clone(), "e1", 3);
        enc.sse_descriptor =
            Some(r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":""}"#.to_owned());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(enc),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![mk("k", &v1, "backfill:r1:k:1")],
        })
        .await
        .unwrap();
        let v2 = VersionId::from_string("00000002".into());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bk, "k", v2.clone(), "e2", 3)),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![mk("k", &v2, "backfill:r1:k:2")],
        })
        .await
        .unwrap();
        // key `d`: an ENCRYPTED v1, then a DELETE MARKER v2 (no body, so no descriptor).
        let d1 = VersionId::from_string("00000003".into());
        let mut denc = row(&bk, "d", d1.clone(), "e3", 3);
        denc.sse_descriptor =
            Some(r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":""}"#.to_owned());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(denc),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![mk("d", &d1, "backfill:r1:d:3")],
        })
        .await
        .unwrap();
        let d2 = VersionId::from_string("00000004".into());
        s.submit(Mutation::CreateDeleteMarker {
            bucket: bk.clone(),
            key: ObjectKey::parse("d").unwrap(),
            version_id: d2.clone(),
            owner_id: UserId::generate(),
            now: Timestamp(2),
            bypass: GovernanceBypass::Denied,
            expected_current: None,
            replication: vec![OutboxEntry {
                operation: ReplicationOp::DeleteMarker,
                ..mk("d", &d2, "backfill:r1:d:4")
            }],
        })
        .await
        .unwrap();
        // key `p`: plaintext only — out of scope entirely, key-level or not.
        let p1 = VersionId::from_string("00000005".into());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bk, "p", p1.clone(), "e5", 3)),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![mk("p", &p1, "backfill:r1:p:5")],
        })
        .await
        .unwrap();

        s.claim_replication_batch(10, Timestamp(1)).await.unwrap();
        for id in [
            "backfill:r1:k:1",
            "backfill:r1:k:2",
            "backfill:r1:d:3",
            "backfill:r1:d:4",
            "backfill:r1:p:5",
        ] {
            s.submit(Mutation::MarkReplicationDone {
                id: id.to_owned(),
                now: Timestamp(0),
            })
            .await
            .unwrap();
        }

        s.submit(Mutation::RequeueReplicationVersions {
            bucket: bk.clone(),
            only_encrypted: true,
            after_key: None,
            now: Timestamp(5000),
            limit: 1000,
        })
        .await
        .unwrap();

        let claimed = s
            .claim_replication_batch(10, Timestamp(6000))
            .await
            .unwrap();
        let mut ids: Vec<&str> = claimed.iter().map(|e| e.id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![
                "backfill:r1:d:3",
                "backfill:r1:d:4",
                "backfill:r1:k:1",
                "backfill:r1:k:2"
            ],
            "every terminal entry of a key with an encrypted version is requeued (including the \
             later plaintext version and the delete marker); a plaintext-only key is not"
        );
        // The ledger half is key-scoped identically on both engines.
        assert_eq!(
            s.object_replication_status(&bk, &ObjectKey::parse("k").unwrap(), &v2)
                .await
                .unwrap(),
            Some(ReplicationStatus::Pending)
        );
        assert_eq!(
            s.object_replication_status(&bk, &ObjectKey::parse("p").unwrap(), &p1)
                .await
                .unwrap(),
            Some(ReplicationStatus::Completed)
        );
    }
}

/// A minimal sealed-DEK descriptor: enough for `sse_descriptor IS NOT NULL` to select the row.
const REQUEUE_ENC_DESCRIPTOR: &str =
    r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":""}"#;

/// A pending `ObjectCreate` outbox entry for (bucket, key, version) under a caller-chosen id.
fn requeue_entry(b: &BucketName, key: &str, version: VersionId, id: &str) -> OutboxEntry {
    OutboxEntry {
        enqueued_at: Timestamp(0),
        id: id.to_owned(),
        bucket: b.clone(),
        key: ObjectKey::parse(key).unwrap(),
        version_id: version,
        operation: ReplicationOp::ObjectCreate,
        rule_id: "r1".to_owned(),
        target_arn: None,
        attempts: 0,
        next_attempt_at: Timestamp(0),
        status: ReplicationStatus::Pending,
        last_error: None,
        priority: 0,
        lease_until: None,
    }
}

/// PARITY: the requeue pages by KEY, threads a forward cursor, and reports both halves identically
/// on every engine, so the caller's drain loop terminates the same way everywhere. An unbounded
/// UPDATE here would hold one group-commit transaction across a full-table scan.
#[tokio::test]
async fn requeue_replication_versions_batching_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        s.submit(Mutation::CreateBucket(Box::new(bucket(
            "bkt",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();
        for i in 1..=5u32 {
            let v = VersionId::from_string(format!("0000000{i}"));
            s.submit(Mutation::PutObjectVersion {
                row: Box::new(row(&bk, &format!("k{i}"), v.clone(), "e", 3)),
                precondition: Precondition::default(),
                initial_state: InitialObjectState::default(),
                replication: vec![requeue_entry(&bk, &format!("k{i}"), v, &format!("e{i}"))],
            })
            .await
            .unwrap();
        }
        s.claim_replication_batch(10, Timestamp(1)).await.unwrap();
        for i in 1..=5u32 {
            s.submit(Mutation::MarkReplicationDone {
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
            let outcome = s
                .submit(Mutation::RequeueReplicationVersions {
                    bucket: bk.clone(),
                    only_encrypted: false,
                    after_key: after_key.clone(),
                    now: Timestamp(5000),
                    limit: 2,
                })
                .await
                .unwrap();
            let MutationOutcome::RowsRequeued { rows, page_end } = outcome else {
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
            vec!["k2".to_owned(), "k4".to_owned(), "k5".to_owned()]
        );
    }
}

/// PARITY for the ordering defect the paging must not reintroduce: key `k` has an OLDER ENCRYPTED
/// version that is `failed` (the BadDigest population) and a NEWER version that is `completed`.
/// Every engine must requeue BOTH in the batch that covers `k` — a page that carries only the newer
/// row lets the heartbeat ship it first and REVERTS the mirror to the old bytes.
#[tokio::test]
async fn requeue_replication_versions_key_atomic_paging_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        s.submit(Mutation::CreateBucket(Box::new(bucket(
            "bkt",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();

        // "a" sorts first and exists only to force a page boundary at `limit: 1`.
        let av = VersionId::from_string("00000001".into());
        let mut arow = row(&bk, "a", av.clone(), "ea", 3);
        arow.sse_descriptor = Some(REQUEUE_ENC_DESCRIPTOR.to_owned());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(arow),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![requeue_entry(&bk, "a", av, "a:1")],
        })
        .await
        .unwrap();

        let v1 = VersionId::from_string("00000001".into());
        let mut enc = row(&bk, "k", v1.clone(), "e1", 3);
        enc.sse_descriptor = Some(REQUEUE_ENC_DESCRIPTOR.to_owned());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(enc),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![requeue_entry(&bk, "k", v1, "k:1")],
        })
        .await
        .unwrap();
        let v2 = VersionId::from_string("00000002".into());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bk, "k", v2.clone(), "e2", 3)),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![requeue_entry(&bk, "k", v2, "k:2")],
        })
        .await
        .unwrap();

        s.claim_replication_batch(10, Timestamp(1)).await.unwrap();
        s.submit(Mutation::MarkReplicationFailed {
            id: "k:1".to_owned(),
            error: "BadDigest".to_owned(),
            next_attempt_at: None,
        })
        .await
        .unwrap();
        for id in ["a:1", "k:2"] {
            s.submit(Mutation::MarkReplicationDone {
                id: id.to_owned(),
                now: Timestamp(2),
            })
            .await
            .unwrap();
        }

        let outcome = s
            .submit(Mutation::RequeueReplicationVersions {
                bucket: bk.clone(),
                only_encrypted: true,
                after_key: None,
                now: Timestamp(5000),
                limit: 1,
            })
            .await
            .unwrap();
        let MutationOutcome::RowsRequeued { page_end, .. } = outcome else {
            panic!("expected a paged outcome, got {outcome:?}");
        };
        assert_eq!(page_end.as_deref(), Some("a"));
        let claimed = s
            .claim_replication_batch(10, Timestamp(5001))
            .await
            .unwrap();
        let ids: Vec<&str> = claimed.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, vec!["a:1"], "a page must never carry a partial key");

        let outcome = s
            .submit(Mutation::RequeueReplicationVersions {
                bucket: bk.clone(),
                only_encrypted: true,
                after_key: page_end,
                now: Timestamp(6000),
                limit: 1,
            })
            .await
            .unwrap();
        let MutationOutcome::RowsRequeued { page_end, .. } = outcome else {
            panic!("expected a paged outcome, got {outcome:?}");
        };
        assert_eq!(page_end.as_deref(), Some("k"));
        let claimed = s
            .claim_replication_batch(10, Timestamp(6001))
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
}

/// PARITY: `MarkReplicationDone` stamps `replicated_at` (schema v23) in the same step as the status
/// on every engine, never touches `updated_at` (the client-visible S3 `LastModified`), and never
/// stamps an inbound `replica` row. A requeue leaves the stamp alone — the re-ship has not happened.
#[tokio::test]
async fn mark_replication_done_stamps_replicated_at_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        s.submit(Mutation::CreateBucket(Box::new(bucket(
            "bkt",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();
        let key = ObjectKey::parse("k").unwrap();
        let v = VersionId::from_string("00000001".into());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bk, "k", v.clone(), "e", 3)),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![requeue_entry(&bk, "k", v.clone(), "e1")],
        })
        .await
        .unwrap();
        assert_eq!(
            s.get_version(&bk, &key, &v)
                .await
                .unwrap()
                .unwrap()
                .replicated_at,
            None
        );

        s.claim_replication_batch(10, Timestamp(1)).await.unwrap();
        s.submit(Mutation::MarkReplicationDone {
            id: "e1".to_owned(),
            now: Timestamp(9_000),
        })
        .await
        .unwrap();
        let got = s.get_version(&bk, &key, &v).await.unwrap().unwrap();
        assert_eq!(got.replication_status, Some(ReplicationStatus::Completed));
        assert_eq!(got.replicated_at, Some(Timestamp(9_000)));
        assert_eq!(
            got.updated_at,
            Timestamp(1),
            "replication must not move the client-visible LastModified"
        );

        s.submit(Mutation::RequeueReplicationVersions {
            bucket: bk.clone(),
            only_encrypted: false,
            after_key: None,
            now: Timestamp(9_500),
            limit: 100,
        })
        .await
        .unwrap();
        assert_eq!(
            s.get_version(&bk, &key, &v)
                .await
                .unwrap()
                .unwrap()
                .replicated_at,
            Some(Timestamp(9_000)),
            "a requeue must not advance or clear the stamp"
        );

        // An inbound replica is never stamped as shipped from here.
        let rv = VersionId::from_string("00000002".into());
        let mut inbound = row(&bk, "r", rv.clone(), "e", 3);
        inbound.replication_status = Some(ReplicationStatus::Replica);
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(inbound),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![requeue_entry(&bk, "r", rv.clone(), "r1")],
        })
        .await
        .unwrap();
        s.submit(Mutation::MarkReplicationDone {
            id: "r1".to_owned(),
            now: Timestamp(9_900),
        })
        .await
        .unwrap();
        let got = s
            .get_version(&bk, &ObjectKey::parse("r").unwrap(), &rv)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.replication_status, Some(ReplicationStatus::Replica));
        assert_eq!(got.replicated_at, None);
    }
}

/// PARITY: the LEDGER half of the requeue is NARROWER than the outbox half, identically on every
/// engine — a divergence here is silent and only shows up as a gauge that never converges.
///
/// A non-current version whose outbox row the retention sweep already pruned cannot be shipped by
/// anything: the resync backfill that follows a forced requeue enumerates `list_current` only. If
/// an engine still flipped it to `pending`, the durable ledger would claim queued work that no
/// queue holds, the audit's `repair_pending` gauge could never fall to zero on that engine, and the
/// alert the runbook prescribes would fire forever. So `pending` is only for versions that are
/// CURRENT or that still HAVE an outbox row for their exact (bucket, key, version_id). The OUTBOX
/// half is unchanged: every surviving terminal row of a paged key still moves together.
#[tokio::test]
async fn requeue_ledger_skips_unshippable_non_current_versions_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        s.submit(Mutation::CreateBucket(Box::new(bucket(
            "bkt",
            VersioningState::Enabled,
        ))))
        .await
        .unwrap();
        let v1 = VersionId::from_string("00000001".into());
        let v2 = VersionId::from_string("00000002".into());

        // `pruned`: encrypted v1 whose outbox row ages out; `kept`: the same, but its row survives.
        for key in ["kept", "pruned"] {
            let mut enc = row(&bk, key, v1.clone(), "e1", 3);
            enc.sse_descriptor = Some(REQUEUE_ENC_DESCRIPTOR.to_owned());
            let old = if key == "pruned" {
                Timestamp(0)
            } else {
                Timestamp(1_000)
            };
            s.submit(Mutation::PutObjectVersion {
                row: Box::new(enc),
                precondition: Precondition::default(),
                initial_state: InitialObjectState::default(),
                replication: vec![OutboxEntry {
                    enqueued_at: old,
                    ..requeue_entry(&bk, key, v1.clone(), &format!("{key}:1"))
                }],
            })
            .await
            .unwrap();
            s.submit(Mutation::PutObjectVersion {
                row: Box::new(row(&bk, key, v2.clone(), "e2", 3)),
                precondition: Precondition::default(),
                initial_state: InitialObjectState::default(),
                replication: vec![OutboxEntry {
                    enqueued_at: Timestamp(1_000),
                    ..requeue_entry(&bk, key, v2.clone(), &format!("{key}:2"))
                }],
            })
            .await
            .unwrap();
        }
        s.claim_replication_batch(10, Timestamp(1)).await.unwrap();
        for id in ["kept:1", "kept:2", "pruned:1", "pruned:2"] {
            s.submit(Mutation::MarkReplicationDone {
                id: id.to_owned(),
                now: Timestamp(2),
            })
            .await
            .unwrap();
        }
        s.submit(Mutation::PruneReplicationOutbox { before_ms: 500 })
            .await
            .unwrap();

        s.submit(Mutation::RequeueReplicationVersions {
            bucket: bk.clone(),
            only_encrypted: true,
            after_key: None,
            now: Timestamp(5_000),
            limit: 1_000,
        })
        .await
        .unwrap();

        let mut got = Vec::new();
        for (key, v) in [
            ("kept", &v1),
            ("kept", &v2),
            ("pruned", &v1),
            ("pruned", &v2),
        ] {
            got.push(
                s.object_replication_status(&bk, &ObjectKey::parse(key).unwrap(), v)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(
            got,
            vec![
                Some(ReplicationStatus::Pending),
                Some(ReplicationStatus::Pending),
                // The one that no queue can ever ship stays as it was.
                Some(ReplicationStatus::Completed),
                Some(ReplicationStatus::Pending),
            ],
            "ledger scope diverged between engines"
        );

        let claimed = s
            .claim_replication_batch(10, Timestamp(5_001))
            .await
            .unwrap();
        let mut ids: Vec<&str> = claimed.iter().map(|e| e.id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec!["kept:1", "kept:2", "pruned:2"],
            "the OUTBOX half must still move every surviving terminal row of a paged key"
        );
    }
}
