//! PARITY GATE (Turso): the same shape as `tests/contract.rs`, but against
//! `cairn_meta_async::open_turso_in_memory()` — the pure-Rust SQLite rewrite (a beta engine) —
//! asserting it behaves identically to the rusqlite `cairn_meta::open_in_memory()` store. Each
//! scenario runs the identical mutation/read sequence against both stores and asserts the
//! observable results are equal, so any divergence in Turso's SQL, savepoint semantics, listing,
//! conditional writes, quota enforcement, multipart, versioning, tags, replication outbox, users,
//! or aggregates fails the gate.
//!
//! Covered: bucket CRUD; put/current_version/get_version; list_current + list_versions paging with
//! prefix/delimiter/markers; conditional writes If-Match/If-None-Match; multipart
//! create/record/complete; delete markers + versioning; tags; replication outbox claim/mark;
//! users; aggregate_counts; quota enforcement; per-mutation savepoint isolation under concurrency.

use cairn_types::authz::{Acl, Grant, Grantee, Permission};
use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::traits::{MetadataStore, ReconcileOracle};
use cairn_types::*;

// ----------------------------------------------------------------------------------------------
// Fixtures shared by both backends (identical to tests/contract.rs).
// ----------------------------------------------------------------------------------------------

fn row(
    bucket: &BucketName,
    key: &str,
    version: VersionId,
    etag: &str,
    size: u64,
) -> ObjectVersionRow {
    ObjectVersionRow {
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

/// Open both backends. The Turso store must be created inside the tokio runtime (its writer is a
/// spawned task); the rusqlite store spawns an OS-thread writer and is runtime-agnostic.
async fn both() -> (
    cairn_meta_async::TursoMetadataStore,
    cairn_meta::SqliteMetadataStore,
) {
    let a = cairn_meta_async::open_turso_in_memory().await.unwrap();
    let b = cairn_meta::open_in_memory().unwrap();
    (a, b)
}

// ----------------------------------------------------------------------------------------------
// Scenarios. Each runs the identical sequence on both stores and asserts equal observable output.
// ----------------------------------------------------------------------------------------------

#[tokio::test]
async fn bucket_crud_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
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

        let err = s
            .submit(Mutation::CreateBucket(Box::new(bucket(
                "bkt",
                VersioningState::Enabled,
            ))))
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::Conflict));

        assert_eq!(s.list_buckets(None).await.unwrap().len(), 1);

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
        let cur = s.current_version(&bk, &k).await.unwrap().unwrap();
        assert!(cur.is_delete_marker);
        // is_bucket_empty means "no rows at all" (S3 DeleteBucket semantics, audit #3): the prior
        // version v1 and the delete marker v2 both remain, so the bucket is NOT empty.
        assert!(!s.is_bucket_empty(&bk).await.unwrap());
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
        let err = s
            .submit(put(
                row(&bk, "k2", VersionId::from_string("v1".into()), "e", 50),
                Precondition::default(),
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::QuotaExceeded));
        assert_eq!(s.aggregate_counts().await.unwrap().logical_bytes, 60);

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

        let claim = s
            .submit(Mutation::ClaimMultipart(upload.clone()))
            .await
            .unwrap();
        assert!(matches!(
            claim,
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
        ));
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

        let claimed = s.claim_replication_batch(10, Timestamp(1)).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, "out-1");

        s.submit(Mutation::MarkReplicationDone("out-1".to_owned()))
            .await
            .unwrap();
        assert_eq!(
            s.object_replication_status(&bk, &ObjectKey::parse("k").unwrap(), &v)
                .await
                .unwrap(),
            Some(ReplicationStatus::Completed)
        );

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

        assert_eq!(s.list_users().await.unwrap().len(), 1);

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
        assert_eq!(c.objects, 2);
        assert_eq!(c.versions, 3);
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
        assert_eq!(counts.len(), 2);
        assert_eq!(counts[0].bucket, "bkt");
        assert_eq!(counts[0].objects, 1);
        assert_eq!(counts[0].logical_bytes, 30);
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
    // The async writer's per-mutation savepoint isolation must hold on Turso exactly as on
    // rusqlite/libSQL: a doomed conditional put rolls back only itself while its concurrent
    // batch-mates all commit.
    let store = cairn_meta_async::open_turso_in_memory().await.unwrap();
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

/// Build a metric row carrying `count` samples that each had latency `lat_ms` and the given bytes,
/// landing the histogram in the correct bucket (mirrors what the ingestion aggregator produces).
#[allow(clippy::too_many_arguments)]
fn mrow(
    ts: i64,
    op: &str,
    bkt: &str,
    status: &str,
    count: u64,
    bytes_in: u64,
    bytes_out: u64,
    lat_ms: u64,
) -> RequestMetricRow {
    let mut lat_hist = [0u64; cairn_types::LATENCY_BUCKETS];
    lat_hist[cairn_types::latency_bucket_index(lat_ms)] = count;
    RequestMetricRow {
        ts_bucket: ts,
        operation: op.into(),
        bucket: bkt.into(),
        status_class: status.into(),
        count,
        bytes_in: bytes_in * count,
        bytes_out: bytes_out * count,
        lat_sum_ms: lat_ms * count,
        lat_hist,
    }
}

#[tokio::test]
async fn request_metrics_upsert_query_prune_parity() {
    let (a, b) = both().await;
    // `now` is fixed; windows are chosen so a 1-day query (5-minute windows) keeps the rows separate.
    let now: i64 = 10_000_000;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        // Two flushes into the same (window, op, bucket, status) key must accumulate, not duplicate;
        // bytes and latency accumulate alongside the count.
        s.submit(Mutation::RecordRequestMetrics {
            rows: vec![
                mrow(now - 100, "GetObject", "alpha", "2xx", 3, 10, 100, 8),
                mrow(now - 100, "PutObject", "beta", "2xx", 5, 200, 0, 40),
            ],
            prune_before: None,
        })
        .await
        .unwrap();
        s.submit(Mutation::RecordRequestMetrics {
            rows: vec![
                mrow(now - 100, "GetObject", "alpha", "2xx", 2, 10, 100, 8),
                mrow(now - 100, "ListObjects", "alpha", "4xx", 1, 0, 0, 2),
            ],
            prune_before: None,
        })
        .await
        .unwrap();

        let series = s
            .query_request_metrics(MetricsRange::OneDay, now)
            .await
            .unwrap();
        assert_eq!(series.total, 11, "3 + 2 + 5 + 1");
        assert_eq!(series.window_secs, 300);
        // by_operation is descending; GetObject accumulated to 5.
        let get = series
            .by_operation
            .iter()
            .find(|o| o.operation == "GetObject")
            .unwrap();
        assert_eq!(get.count, 5);
        assert_eq!(
            get.bytes,
            (10 + 100) * 5,
            "GetObject bytes in+out accumulate"
        );
        assert_eq!(get.latency_avg_ms, 8);
        // top_buckets excludes the non-bucket sentinel and ranks by count (alpha 6 > beta 5).
        assert_eq!(series.top_buckets.len(), 2);
        assert_eq!(series.top_buckets[0].bucket, "alpha");
        assert_eq!(series.active_buckets, 2);
        // top_buckets_by_bytes is a genuinely different ranking: beta moved (200+0)*5 = 1000 bytes
        // vs alpha's (10+100)*5 = 550, so beta leads by data even though it had fewer requests.
        // Regression guard for the console's "Top buckets by data" panel (it must NOT just re-sort
        // the by-count cohort, which would omit a low-traffic bucket that moved the most data).
        assert_eq!(series.top_buckets_by_bytes.len(), 2);
        assert_eq!(series.top_buckets_by_bytes[0].bucket, "beta");
        assert_ne!(
            series.top_buckets[0].bucket, series.top_buckets_by_bytes[0].bucket,
            "by-count and by-bytes rankings diverge for this data"
        );
        // status breakdown: one 4xx error out of 11.
        assert_eq!(series.total_errors, 1);
        assert_eq!(series.by_status.iter().map(|s| s.count).sum::<u64>(), 11);
        // bytes + latency totals.
        assert_eq!(series.total_bytes_in, 10 * 5 + 200 * 5);
        assert_eq!(series.total_bytes_out, 100 * 5);
        assert!(series.latency_avg_ms > 0);
        assert!(
            series.latency_p95_ms > 0,
            "p95 estimated from the histogram"
        );
        assert!(series.peak_window_count >= 11);

        // A row far in the past is excluded by the range lower bound, and pruned by prune_before.
        s.submit(Mutation::RecordRequestMetrics {
            rows: vec![mrow(now - 5_000_000, "GetObject", "", "2xx", 99, 1, 1, 5)],
            prune_before: Some(now - 1_000_000),
        })
        .await
        .unwrap();
        let after = s
            .query_request_metrics(MetricsRange::OneDay, now)
            .await
            .unwrap();
        assert_eq!(after.total, 11, "old row is outside the 1-day window");
        let month = s
            .query_request_metrics(MetricsRange::OneMonth, now)
            .await
            .unwrap();
        assert_eq!(
            month.total, 11,
            "pruned row must not resurface in any range"
        );
    }
}

#[tokio::test]
async fn tag_browser_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let ba = BucketName::parse("bucket-a").unwrap();
        let bb = BucketName::parse("bucket-b").unwrap();
        for name in ["bucket-a", "bucket-b"] {
            s.submit(Mutation::CreateBucket(Box::new(bucket(
                name,
                VersioningState::Enabled,
            ))))
            .await
            .unwrap();
        }
        // ba/a1 {env=prod}; ba/a2 {env=prod, team=core}; bb/b1 {env=dev}.
        let fixtures = [
            (&ba, "a1", vec![("env".into(), "prod".into())]),
            (
                &ba,
                "a2",
                vec![
                    ("env".into(), "prod".into()),
                    ("team".into(), "core".into()),
                ],
            ),
            (&bb, "b1", vec![("env".into(), "dev".into())]),
        ];
        for (bk, key, tags) in fixtures {
            let v = VersionId::from_string(format!("{key}-v1"));
            s.submit(put(
                row(bk, key, v.clone(), "e", 7),
                Precondition::default(),
            ))
            .await
            .unwrap();
            s.submit(Mutation::PutObjectTags {
                bucket: bk.clone(),
                key: ObjectKey::parse(key).unwrap(),
                version_id: v,
                tags,
            })
            .await
            .unwrap();
        }

        // Global summary: env=prod is the most-used (2 objects), then the singletons.
        let all = s.list_tag_summary(None).await.unwrap();
        assert_eq!(all[0].tag_key, "env");
        assert_eq!(all[0].tag_value, "prod");
        assert_eq!(all[0].object_count, 2);
        assert_eq!(all.len(), 3, "env=prod, env=dev, team=core");

        // Bucket-scoped summary excludes bb's env=dev.
        let in_ba = s.list_tag_summary(Some(&ba)).await.unwrap();
        assert!(
            in_ba
                .iter()
                .all(|t| !(t.tag_key == "env" && t.tag_value == "dev"))
        );
        assert_eq!(
            in_ba
                .iter()
                .find(|t| t.tag_value == "prod")
                .unwrap()
                .object_count,
            2
        );

        // Objects by tag: env=prod globally returns both ba objects, sorted by (bucket, key).
        let prod = s
            .list_objects_by_tag(None, "env", "prod", 100)
            .await
            .unwrap();
        assert_eq!(prod.len(), 2);
        assert_eq!(
            (prod[0].bucket.as_str(), prod[0].key.as_str()),
            ("bucket-a", "a1")
        );
        assert_eq!(prod[0].size, 7);

        // Bucket-scoped objects-by-tag respects the scope.
        let dev_in_ba = s
            .list_objects_by_tag(Some(&ba), "env", "dev", 100)
            .await
            .unwrap();
        assert!(dev_in_ba.is_empty(), "env=dev lives in bb, not ba");
        let dev_in_bb = s
            .list_objects_by_tag(Some(&bb), "env", "dev", 100)
            .await
            .unwrap();
        assert_eq!(dev_in_bb.len(), 1);
        assert_eq!(dev_in_bb[0].key, "b1");
    }
}

/// Concurrent reads must not interleave on a shared connection (audit #8). With the pool's
/// per-connection locking, many parallel listings — more readers than pool connections — each see
/// the full, uncorrupted result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reads_are_isolated_per_connection() {
    let s = cairn_meta_async::open_turso_in_memory().await.unwrap();
    let bk = BucketName::parse("conc").unwrap();
    s.submit(Mutation::CreateBucket(Box::new(bucket(
        "conc",
        VersioningState::Enabled,
    ))))
    .await
    .unwrap();
    const N: usize = 200;
    for i in 0..N {
        let key = format!("k{i:04}");
        s.submit(put(
            row(
                &bk,
                &key,
                VersionId::from_string(format!("v{i:04}")),
                "e",
                3,
            ),
            Precondition::default(),
        ))
        .await
        .unwrap();
    }

    // More concurrent readers than the pool's connections, each a multi-query paging listing.
    let mut handles = Vec::new();
    for _ in 0..32 {
        let s2 = s.clone();
        let bk2 = bk.clone();
        handles.push(tokio::spawn(async move {
            s2.list_current(
                &bk2,
                &ListQuery {
                    limit: 10_000,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .items
            .len()
        }));
    }
    for h in handles {
        assert_eq!(
            h.await.unwrap(),
            N,
            "every concurrent reader sees the full, uncorrupted listing"
        );
    }
}

#[tokio::test]
async fn object_lock_parity() {
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

        // No lock initially.
        let st = s.get_object_lock(&bk, &k, &v).await.unwrap();
        assert!(st.retention.is_none() && !st.legal_hold);

        // Set a COMPLIANCE retention.
        s.submit(Mutation::SetObjectRetention {
            bucket: bk.clone(),
            key: k.clone(),
            version_id: v.clone(),
            retention: Some(cairn_types::object::ObjectRetention {
                mode: cairn_types::object::ObjectLockMode::Compliance,
                retain_until: Timestamp::from_secs(2_000_000_000),
            }),
        })
        .await
        .unwrap();
        let st = s.get_object_lock(&bk, &k, &v).await.unwrap();
        let r = st.retention.expect("retention set");
        assert!(matches!(
            r.mode,
            cairn_types::object::ObjectLockMode::Compliance
        ));
        assert_eq!(r.retain_until, Timestamp::from_secs(2_000_000_000));
        assert!(!st.legal_hold);

        // Turn legal hold on (independent of retention).
        s.submit(Mutation::SetObjectLegalHold {
            bucket: bk.clone(),
            key: k.clone(),
            version_id: v.clone(),
            on: true,
        })
        .await
        .unwrap();
        let st = s.get_object_lock(&bk, &k, &v).await.unwrap();
        assert!(st.legal_hold);
        assert!(
            st.retention.is_some(),
            "retention preserved across legal-hold update"
        );

        // Release legal hold.
        s.submit(Mutation::SetObjectLegalHold {
            bucket: bk.clone(),
            key: k.clone(),
            version_id: v.clone(),
            on: false,
        })
        .await
        .unwrap();
        assert!(!s.get_object_lock(&bk, &k, &v).await.unwrap().legal_hold);

        // Deleting the version clears its lock row (no orphan).
        s.submit(Mutation::DeleteVersion {
            bucket: bk.clone(),
            key: k.clone(),
            version_id: v.clone(),
        })
        .await
        .unwrap();
        let st = s.get_object_lock(&bk, &k, &v).await.unwrap();
        assert!(
            st.retention.is_none() && !st.legal_hold,
            "lock cleared on version delete"
        );
    }
}

#[tokio::test]
async fn webhook_outbox_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        let bk = BucketName::parse("bkt").unwrap();
        let now = Timestamp::from_secs(1_000);
        let entry = cairn_types::WebhookEntry {
            id: "wh1".to_owned(),
            bucket: bk.clone(),
            key: ObjectKey::parse("k").unwrap(),
            version_id: VersionId::from_string("v1".into()),
            event: cairn_types::notification::EventKind::ObjectCreatedPut,
            endpoint_id: "ep1".to_owned(),
            payload: r#"{"Records":[]}"#.to_owned(),
            attempts: 0,
            next_attempt_at: now,
            status: cairn_types::WebhookStatus::Pending,
            last_error: None,
            priority: 0,
            lease_until: None,
        };
        s.submit(Mutation::EnqueueWebhooks(vec![entry.clone()]))
            .await
            .unwrap();
        // A second enqueue of the same id is idempotent (INSERT OR IGNORE).
        s.submit(Mutation::EnqueueWebhooks(vec![entry.clone()]))
            .await
            .unwrap();

        // Due as of `now`.
        let due = s.list_due_webhooks(10, now).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, "wh1");
        assert_eq!(due[0].payload, r#"{"Records":[]}"#);
        assert!(matches!(
            due[0].event,
            cairn_types::notification::EventKind::ObjectCreatedPut
        ));

        // Claim marks it claimed under a lease (no longer due at the same instant).
        let claimed = s.claim_webhook_batch(10, now).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert!(s.list_due_webhooks(10, now).await.unwrap().is_empty());

        // Mark failed terminally → it lands in the failed list.
        s.submit(Mutation::MarkWebhookFailed {
            id: "wh1".to_owned(),
            error: "boom".to_owned(),
            next_attempt_at: None,
        })
        .await
        .unwrap();
        let failed = s.list_failed_webhooks(10).await.unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].attempts, 1);
        assert_eq!(failed[0].last_error.as_deref(), Some("boom"));

        // Mark done clears it from failed.
        s.submit(Mutation::MarkWebhookDone("wh1".to_owned()))
            .await
            .unwrap();
        assert!(s.list_failed_webhooks(10).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn session_credentials_parity() {
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        // The parent user must exist for the join (display_name + is_active).
        let parent = cairn_types::meta::User {
            id: cairn_types::UserId("parent".into()),
            display_name: "Parent User".into(),
            access_key_id: "pk".into(),
            sigv4_access_key_id: None,
            role: cairn_types::auth::Role::Member,
            is_active: true,
            quota_bytes: None,
            created_at: Timestamp::from_secs(0),
            updated_at: Timestamp::from_secs(0),
        };
        s.submit(Mutation::CreateUser(Box::new(
            cairn_types::meta::UserRecord {
                user: parent,
                bearer_secret_hash: "h".into(),
                sigv4_secret_ciphertext: None,
                sigv4_secret_nonce: None,
            },
        )))
        .await
        .unwrap();

        // Absent before minting.
        assert!(s.user_by_session_key("CAIRNTMP1").await.unwrap().is_none());

        s.submit(Mutation::CreateSessionCredential(Box::new(
            cairn_types::SessionCredentialRecord {
                access_key_id: "CAIRNTMP1".into(),
                parent_user_id: cairn_types::UserId("parent".into()),
                secret_ciphertext: vec![1, 2, 3, 4],
                secret_nonce: None,
                session_token_hash: "tokhash".into(),
                inline_policy: Some(r#"{"Version":"2012-10-17","Statement":[]}"#.into()),
                expires_at: Timestamp::from_secs(2_000_000_000),
                created_at: Timestamp::from_secs(1_000),
            },
        )))
        .await
        .unwrap();

        let c = s
            .user_by_session_key("CAIRNTMP1")
            .await
            .unwrap()
            .expect("found");
        assert_eq!(c.parent_user_id.0, "parent");
        assert_eq!(c.parent_display_name, "Parent User");
        assert!(c.parent_is_active);
        assert_eq!(c.secret_ciphertext, vec![1, 2, 3, 4]);
        assert_eq!(c.session_token_hash, "tokhash");
        assert_eq!(c.expires_at, Timestamp::from_secs(2_000_000_000));
        assert!(c.inline_policy.is_some());

        // list_session_credentials returns the active session as a NON-secret summary, and excludes
        // sessions already expired as of the `now` cutoff.
        let active = s
            .list_session_credentials(Timestamp::from_secs(1_500_000_000))
            .await
            .unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].access_key_id, "CAIRNTMP1");
        assert_eq!(active[0].parent_user_id.0, "parent");
        assert!(active[0].has_inline_policy);
        assert_eq!(active[0].created_at, Timestamp::from_secs(1_000));
        assert_eq!(active[0].expires_at, Timestamp::from_secs(2_000_000_000));
        assert!(
            s.list_session_credentials(Timestamp::from_secs(2_000_000_001))
                .await
                .unwrap()
                .is_empty(),
            "expired sessions are excluded from the active list"
        );

        // The expiry sweep removes credentials that expired before the cutoff, keeps the rest.
        s.submit(Mutation::DeleteExpiredSessionCredentials {
            before: Timestamp::from_secs(1_999_999_999),
        })
        .await
        .unwrap();
        assert!(
            s.user_by_session_key("CAIRNTMP1").await.unwrap().is_some(),
            "not yet expired → retained"
        );
        s.submit(Mutation::DeleteExpiredSessionCredentials {
            before: Timestamp::from_secs(2_000_000_001),
        })
        .await
        .unwrap();
        assert!(
            s.user_by_session_key("CAIRNTMP1").await.unwrap().is_none(),
            "past expiry → swept"
        );

        // Explicit early revoke: mint, see it listed, delete it by access-key id, confirm it's gone
        // from both the active list and the auth lookup (so the next request is denied).
        s.submit(Mutation::CreateSessionCredential(Box::new(
            cairn_types::SessionCredentialRecord {
                access_key_id: "CAIRNTMP3".into(),
                parent_user_id: cairn_types::UserId("parent".into()),
                secret_ciphertext: vec![9],
                secret_nonce: None,
                session_token_hash: "h3".into(),
                inline_policy: None,
                expires_at: Timestamp::from_secs(2_000_000_000),
                created_at: Timestamp::from_secs(2_000),
            },
        )))
        .await
        .unwrap();
        assert_eq!(
            s.list_session_credentials(Timestamp::from_secs(1_500_000_000))
                .await
                .unwrap()
                .len(),
            1
        );
        s.submit(Mutation::DeleteSessionCredential {
            access_key_id: "CAIRNTMP3".into(),
        })
        .await
        .unwrap();
        assert!(
            s.list_session_credentials(Timestamp::from_secs(1_500_000_000))
                .await
                .unwrap()
                .is_empty(),
            "revoked session is no longer listed"
        );
        assert!(
            s.user_by_session_key("CAIRNTMP3").await.unwrap().is_none(),
            "revoked session is denied at auth"
        );
    }
}
