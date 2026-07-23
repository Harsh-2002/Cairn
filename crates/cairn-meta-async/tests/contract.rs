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
        replicated_at: None,
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
            encrypt_parts: false,
            sse_kms_requested: false,
            sse_kms_key_id: None,
            sse_bucket_key_enabled: false,
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
                part_dek: None,
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

        // Audit 2026-07: paging on the (key-marker, upload-id-marker) PAIR must behave identically
        // in both engines, including within a single key. Add a second session on the SAME key so
        // a key-only marker could never advance past it.
        let upload2 = UploadId::from_string("upload-2".into());
        s.submit(Mutation::CreateMultipart(Box::new(MultipartSession {
            upload_id: upload2.clone(),
            bucket: bk.clone(),
            key: ObjectKey::parse("big").unwrap(),
            content_type: "application/octet-stream".to_owned(),
            status: MultipartStatus::Active,
            owner_id: UserId("owner".to_owned()),
            intended_acl: None,
            user_metadata: Vec::new(),
            sse_requested: false,
            encrypt_parts: false,
            sse_kms_requested: false,
            sse_kms_key_id: None,
            sse_bucket_key_enabled: false,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        })))
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
            intended_acl: None,
            user_metadata: Vec::new(),
            sse_requested: true,
            encrypt_parts: true,
            sse_kms_requested: false,
            sse_kms_key_id: None,
            sse_bucket_key_enabled: false,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        };
        s.submit(Mutation::CreateMultipart(Box::new(session)))
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
        s.submit(Mutation::RecordPart {
            upload_id: upload.clone(),
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
        s.submit(Mutation::RecordPart {
            upload_id: upload.clone(),
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
            intended_acl: None,
            user_metadata: Vec::new(),
            sse_requested: false,
            encrypt_parts: true,
            sse_kms_requested: true,
            sse_kms_key_id: Some("alias/my-key".to_owned()),
            sse_bucket_key_enabled: true,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        };
        s.submit(Mutation::CreateMultipart(Box::new(session)))
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
    use cairn_types::meta::{ImportBucketProgress, ImportJobRecord, ImportState};
    use cairn_types::time::Timestamp;
    let (a, b) = both().await;
    for s in [&a as &dyn MetadataStore, &b as &dyn MetadataStore] {
        assert!(s.list_import_jobs().await.unwrap().is_empty());
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
        s.submit(Mutation::CreateImportJob(Box::new(rec)))
            .await
            .unwrap();

        // list + get are secret-free (has_ca_cert flag, no ciphertext).
        let jobs = s.list_import_jobs().await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, "job1");
        assert!(jobs[0].has_ca_cert);
        assert_eq!(jobs[0].access_key_id, "AKSRC");
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

        // Terminal state, then prune finished jobs past the horizon.
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
        s.submit(Mutation::PruneImportJobs { before_ms: 4000 })
            .await
            .unwrap();
        assert!(s.get_import_job("job1").await.unwrap().is_none());
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
            replication: vec![mk("enc", &venc, "backfill:r1:enc:1")],
        })
        .await
        .unwrap();
        let vplain = VersionId::from_string("00000002".into());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bk, "plain", vplain.clone(), "e", 3)),
            precondition: Precondition::default(),
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
            replication: vec![mk("k", &v1, "backfill:r1:k:1")],
        })
        .await
        .unwrap();
        let v2 = VersionId::from_string("00000002".into());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bk, "k", v2.clone(), "e2", 3)),
            precondition: Precondition::default(),
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
            replication: vec![requeue_entry(&bk, "k", v1, "k:1")],
        })
        .await
        .unwrap();
        let v2 = VersionId::from_string("00000002".into());
        s.submit(Mutation::PutObjectVersion {
            row: Box::new(row(&bk, "k", v2.clone(), "e2", 3)),
            precondition: Precondition::default(),
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
