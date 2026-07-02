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
//! users; aggregate_counts; quota enforcement.

use cairn_types::authz::{Acl, Grant, Grantee, Permission};
use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::traits::{MetadataStore, ReconcileOracle};
use cairn_types::*;

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
async fn bucket_crud_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
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
            intended_acl: None,
            user_metadata: Vec::new(),
            sse_requested: false,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        };
        assert!(matches!(
            s.submit(Mutation::CreateMultipart(Box::new(session)))
                .await
                .unwrap(),
            MutationOutcome::MultipartCreated(_)
        ));
        assert!(s.get_multipart(&upload).await.unwrap().is_some());

        // Record two parts.
        for n in 1u16..=2 {
            let part = PartRecord {
                part_number: n,
                size: 5 * 1024 * 1024,
                etag: format!("petag{n}"),
                storage_path: StoragePath::from_string(format!("bkt/part-{n}")),
                checksum: None,
            };
            s.submit(Mutation::RecordPart {
                upload_id: upload.clone(),
                part,
            })
            .await
            .unwrap();
        }
        let parts = s.list_parts(&upload, 0, 100).await.unwrap();
        assert_eq!(parts.items.len(), 2);

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

        // Claim then complete.
        let claim = s
            .submit(Mutation::ClaimMultipart(upload.clone()))
            .await
            .unwrap();
        assert!(matches!(
            claim,
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));
        // Re-claim is AlreadyClaimed (status now 'completing').
        assert!(matches!(
            s.submit(Mutation::ClaimMultipart(upload.clone()))
                .await
                .unwrap(),
            MutationOutcome::MultipartClaim(ClaimOutcome::AlreadyClaimed)
        ));

        let assembled = row(
            &bk,
            "big",
            VersionId::from_string("v1".into()),
            "final-etag",
            10 * 1024 * 1024,
        );
        let out = s
            .submit(Mutation::CompleteMultipart {
                upload_id: upload.clone(),
                row: Box::new(assembled),
                precondition: Precondition::default(),
                replication: Vec::new(),
            })
            .await
            .unwrap();
        assert!(matches!(out, MutationOutcome::MultipartCompleted { .. }));
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
            replication: vec![entry],
        })
        .await
        .unwrap();

        // Claim due entries.
        let claimed = s.claim_replication_batch(10, Timestamp(1)).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, "out-1");

        // Mark done updates the version status to completed.
        s.submit(Mutation::MarkReplicationDone("out-1".to_owned()))
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
