//! Writer-authoritative Object Lock contract for the canonical in-memory metadata backend.

use cairn_types::testing::InMemoryMetadataStore;
use cairn_types::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct StartBarrier {
    parties: usize,
    arrived: AtomicUsize,
}

impl StartBarrier {
    fn new(parties: usize) -> Self {
        Self {
            parties,
            arrived: AtomicUsize::new(0),
        }
    }

    async fn wait(&self) {
        self.arrived.fetch_add(1, Ordering::AcqRel);
        while self.arrived.load(Ordering::Acquire) < self.parties {
            tokio::task::yield_now().await;
        }
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

fn row(bucket: &BucketName, key: &str, version: &str, created_at: Timestamp) -> ObjectVersionRow {
    ObjectVersionRow {
        id: format!("row-{version}"),
        bucket: bucket.clone(),
        key: ObjectKey::parse(key).unwrap(),
        version_id: VersionId::from_string(version.to_owned()),
        is_latest: true,
        is_delete_marker: false,
        size_logical: 3,
        size_physical: 3,
        etag: ETag::from_string(format!("etag-{version}")),
        content_type: "application/octet-stream".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(StoragePath::from_string(format!(
            "{}/blob-{version}",
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
        created_at,
        updated_at: created_at,
    }
}

async fn create_lock_bucket(store: &InMemoryMetadataStore, name: &str) -> BucketName {
    let bucket = bucket(name, VersioningState::Enabled);
    let name = bucket.name.clone();
    store
        .submit(Mutation::CreateObjectLockBucket(Box::new(bucket)))
        .await
        .unwrap();
    name
}

async fn put(
    store: &InMemoryMetadataStore,
    row: ObjectVersionRow,
    initial_state: InitialObjectState,
) -> Result<MutationOutcome, MetaError> {
    store
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            initial_state,
            replication: Vec::new(),
        })
        .await
}

fn session(
    bucket: &BucketName,
    upload: &str,
    key: &str,
    created_at: Timestamp,
    tags: Vec<(String, String)>,
    lock_intent: ExplicitObjectLockIntent,
) -> MultipartSession {
    MultipartSession {
        upload_id: UploadId::from_string(upload.to_owned()),
        bucket: bucket.clone(),
        key: ObjectKey::parse(key).unwrap(),
        content_type: "application/octet-stream".to_owned(),
        status: MultipartStatus::Active,
        owner_id: UserId("owner".to_owned()),
        initiated_by: UserId("writer".to_owned()),
        intended_acl: None,
        user_metadata: Vec::new(),
        initial_tags: tags,
        lock_intent,
        sse_requested: false,
        encrypt_parts: false,
        sse_kms_requested: false,
        sse_kms_key_id: None,
        sse_bucket_key_enabled: false,
        created_at,
        updated_at: created_at,
    }
}

fn outbox(bucket: &BucketName, key: &ObjectKey, version_id: &VersionId, id: &str) -> OutboxEntry {
    OutboxEntry {
        id: id.to_owned(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version_id.clone(),
        operation: ReplicationOp::ObjectCreate,
        rule_id: "rule".to_owned(),
        target_arn: None,
        attempts: 0,
        next_attempt_at: Timestamp(1),
        status: ReplicationStatus::Pending,
        last_error: None,
        priority: 0,
        lease_until: None,
        enqueued_at: Timestamp(1),
    }
}

#[tokio::test]
async fn object_lock_bucket_creation_and_configuration_are_writer_atomic_and_immutable() {
    let store = InMemoryMetadataStore::new();
    let invalid = bucket("bad-lock", VersioningState::Unversioned);
    assert!(matches!(
        store
            .submit(Mutation::CreateObjectLockBucket(Box::new(invalid.clone())))
            .await,
        Err(MetaError::InvalidBucketState)
    ));
    assert!(store.get_bucket(&invalid.name).await.unwrap().is_none());

    let name = create_lock_bucket(&store, "locked").await;
    let stored = store.get_bucket(&name).await.unwrap().unwrap();
    assert_eq!(stored.versioning, VersioningState::Enabled);
    let config: ObjectLockConfiguration = serde_json::from_str(
        &store
            .get_bucket_config(&name, ConfigAspect::ObjectLock)
            .await
            .unwrap()
            .unwrap()
            .0,
    )
    .unwrap();
    assert!(config.enabled);
    assert_eq!(config.default_retention, None);

    for doc in [
        None,
        Some(ConfigDoc(
            serde_json::to_string(&ObjectLockConfiguration::default()).unwrap(),
        )),
    ] {
        assert!(matches!(
            store
                .submit(Mutation::SetBucketConfig {
                    bucket: name.clone(),
                    aspect: ConfigAspect::ObjectLock,
                    doc,
                })
                .await,
            Err(MetaError::InvalidBucketState)
        ));
    }
    assert!(matches!(
        store
            .submit(Mutation::SetVersioning {
                bucket: name.clone(),
                state: VersioningState::Suspended,
            })
            .await,
        Err(MetaError::InvalidBucketState)
    ));

    let default = DefaultRetention {
        mode: ObjectLockMode::Governance,
        period: RetentionPeriod::Days(7),
    };
    store
        .submit(Mutation::UpdateObjectLockConfiguration {
            bucket: name,
            default_retention: Some(default),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn initial_tags_and_lock_are_atomic_and_protect_replacement_and_delete() {
    let store = InMemoryMetadataStore::new();
    let bucket = create_lock_bucket(&store, "protected").await;
    let key = ObjectKey::parse("key").unwrap();
    let version = VersionId::from_string("null".to_owned());
    let retention = ObjectRetention {
        mode: ObjectLockMode::Compliance,
        retain_until: Timestamp(1_000),
    };
    put(
        &store,
        row(&bucket, key.as_str(), version.as_str(), Timestamp(100)),
        InitialObjectState {
            tags: vec![("class".to_owned(), "records".to_owned())],
            lock_intent: ExplicitObjectLockIntent {
                retention: Some(retention),
                legal_hold: Some(false),
            },
        },
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .get_object_tags(&bucket, &key, &version)
            .await
            .unwrap(),
        vec![("class".to_owned(), "records".to_owned())]
    );
    assert_eq!(
        store
            .get_object_lock(&bucket, &key, &version)
            .await
            .unwrap(),
        ObjectLockState {
            retention: Some(retention),
            legal_hold: false,
        }
    );

    assert!(matches!(
        put(
            &store,
            row(&bucket, key.as_str(), version.as_str(), Timestamp(200)),
            InitialObjectState::default(),
        )
        .await,
        Err(MetaError::ObjectProtected)
    ));
    assert_eq!(
        store
            .get_object_tags(&bucket, &key, &version)
            .await
            .unwrap(),
        vec![("class".to_owned(), "records".to_owned())],
        "a rejected replacement must not clear side rows"
    );
    assert_eq!(
        store
            .submit(Mutation::CreateDeleteMarker {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version.clone(),
                owner_id: UserId("owner".to_owned()),
                now: Timestamp(200),
                bypass: GovernanceBypass::Authorized,
                expected_current: None,
                replication: Vec::new(),
            })
            .await
            .unwrap(),
        MutationOutcome::DeleteProtected
    );
    assert_eq!(
        store
            .submit(Mutation::DeleteVersion {
                bucket,
                key,
                version_id: version,
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(200),
                bypass: GovernanceBypass::Authorized,
            })
            .await
            .unwrap(),
        MutationOutcome::DeleteProtected,
        "COMPLIANCE cannot be bypassed"
    );
}

#[tokio::test]
async fn object_commit_exactly_replaces_side_rows_and_markers_never_gain_state() {
    let store = InMemoryMetadataStore::new();
    let bucket = create_lock_bucket(&store, "exact-side-state").await;
    let key = ObjectKey::parse("key").unwrap();
    let version = VersionId::from_string("null".to_owned());

    put(
        &store,
        row(&bucket, key.as_str(), version.as_str(), Timestamp(10)),
        InitialObjectState {
            tags: vec![("old".to_owned(), "tag".to_owned())],
            lock_intent: ExplicitObjectLockIntent::default(),
        },
    )
    .await
    .unwrap();
    put(
        &store,
        row(&bucket, key.as_str(), version.as_str(), Timestamp(20)),
        InitialObjectState::default(),
    )
    .await
    .unwrap();
    assert!(
        store
            .get_object_tags(&bucket, &key, &version)
            .await
            .unwrap()
            .is_empty(),
        "an empty initial tag set must clear a replaced sentinel's stale tags"
    );

    store
        .submit(Mutation::UpdateObjectLockConfiguration {
            bucket: bucket.clone(),
            default_retention: Some(DefaultRetention {
                mode: ObjectLockMode::Compliance,
                period: RetentionPeriod::Days(1),
            }),
        })
        .await
        .unwrap();
    let mut marker = row(&bucket, key.as_str(), version.as_str(), Timestamp(30));
    marker.is_delete_marker = true;
    marker.storage_path = None;
    marker.size_logical = 0;
    marker.size_physical = 0;
    put(
        &store,
        marker,
        InitialObjectState {
            tags: vec![("must".to_owned(), "drop".to_owned())],
            lock_intent: ExplicitObjectLockIntent {
                retention: Some(ObjectRetention {
                    mode: ObjectLockMode::Governance,
                    retain_until: Timestamp(1_000),
                }),
                legal_hold: Some(true),
            },
        },
    )
    .await
    .unwrap();
    assert!(
        store
            .get_object_tags(&bucket, &key, &version)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .get_object_lock(&bucket, &key, &version)
            .await
            .unwrap(),
        ObjectLockState::default(),
        "delete markers receive neither default nor explicit Object Lock state"
    );
}

#[tokio::test]
async fn late_outbox_conflict_rolls_back_version_tags_and_lock_together() {
    let store = InMemoryMetadataStore::new();
    let bucket = create_lock_bucket(&store, "late-conflict").await;
    let seed_key = ObjectKey::parse("seed").unwrap();
    let seed_version = VersionId::from_string("seed-v1".to_owned());
    store
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row(
                &bucket,
                seed_key.as_str(),
                seed_version.as_str(),
                Timestamp(10),
            )),
            precondition: Precondition::default(),
            initial_state: InitialObjectState::default(),
            replication: vec![outbox(
                &bucket,
                &seed_key,
                &seed_version,
                "duplicate-outbox",
            )],
        })
        .await
        .unwrap();

    let failed_key = ObjectKey::parse("failed").unwrap();
    let failed_version = VersionId::from_string("failed-v1".to_owned());
    assert!(matches!(
        store
            .submit(Mutation::PutObjectVersion {
                row: Box::new(row(
                    &bucket,
                    failed_key.as_str(),
                    failed_version.as_str(),
                    Timestamp(20),
                )),
                precondition: Precondition::default(),
                initial_state: InitialObjectState {
                    tags: vec![("must".to_owned(), "rollback".to_owned())],
                    lock_intent: ExplicitObjectLockIntent {
                        retention: Some(ObjectRetention {
                            mode: ObjectLockMode::Compliance,
                            retain_until: Timestamp(1_000),
                        }),
                        legal_hold: Some(true),
                    },
                },
                replication: vec![outbox(
                    &bucket,
                    &failed_key,
                    &failed_version,
                    "duplicate-outbox",
                )],
            })
            .await,
        Err(MetaError::Conflict)
    ));
    assert!(
        store
            .get_version(&bucket, &failed_key, &failed_version)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_object_tags(&bucket, &failed_key, &failed_version)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .get_object_lock(&bucket, &failed_key, &failed_version)
            .await
            .unwrap(),
        ObjectLockState::default()
    );
}

#[tokio::test]
async fn governance_legal_hold_retention_updates_and_corrupt_rows_fail_closed() {
    let store = InMemoryMetadataStore::new();
    let bucket = create_lock_bucket(&store, "governed").await;
    let key = ObjectKey::parse("key").unwrap();
    let version = VersionId::from_string("v1".to_owned());
    put(
        &store,
        row(&bucket, key.as_str(), version.as_str(), Timestamp(10)),
        InitialObjectState {
            tags: Vec::new(),
            lock_intent: ExplicitObjectLockIntent {
                retention: Some(ObjectRetention {
                    mode: ObjectLockMode::Governance,
                    retain_until: Timestamp(100),
                }),
                legal_hold: Some(true),
            },
        },
    )
    .await
    .unwrap();

    for bypass in [GovernanceBypass::Denied, GovernanceBypass::Authorized] {
        assert_eq!(
            store
                .submit(Mutation::DeleteVersion {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: version.clone(),
                    expected_row_id: None,
                    expected_updated_at: None,
                    require_sole_key_version: false,
                    now: Timestamp(20),
                    bypass,
                })
                .await
                .unwrap(),
            MutationOutcome::DeleteProtected,
            "legal hold cannot be bypassed"
        );
    }
    store
        .submit(Mutation::SetObjectLegalHold {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: version.clone(),
            on: false,
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .submit(Mutation::SetObjectRetention {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version.clone(),
                retention: None,
                now: Timestamp(20),
                bypass: GovernanceBypass::Denied,
            })
            .await,
        Err(MetaError::ObjectProtected)
    ));
    store
        .submit(Mutation::SetObjectRetention {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: version.clone(),
            retention: None,
            now: Timestamp(20),
            bypass: GovernanceBypass::Authorized,
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version.clone(),
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(20),
                bypass: GovernanceBypass::Denied,
            })
            .await
            .unwrap(),
        MutationOutcome::Deleted { freed: Some(_), .. }
    ));
    assert!(matches!(
        store
            .submit(Mutation::SetObjectRetention {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_string("missing".to_owned()),
                retention: Some(ObjectRetention {
                    mode: ObjectLockMode::Governance,
                    retain_until: Timestamp(100),
                }),
                now: Timestamp(20),
                bypass: GovernanceBypass::Denied,
            })
            .await,
        Err(MetaError::ObjectVersionNotFound)
    ));

    let corrupt_version = VersionId::from_string("corrupt".to_owned());
    put(
        &store,
        row(
            &bucket,
            key.as_str(),
            corrupt_version.as_str(),
            Timestamp(30),
        ),
        InitialObjectState::default(),
    )
    .await
    .unwrap();
    store.corrupt_object_lock_row(&bucket, &key, &corrupt_version);
    assert!(matches!(
        store.get_object_lock(&bucket, &key, &corrupt_version).await,
        Err(MetaError::InvalidObjectLockState)
    ));
    assert!(matches!(
        store
            .submit(Mutation::DeleteVersion {
                bucket,
                key,
                version_id: corrupt_version,
                expected_row_id: None,
                expected_updated_at: None,
                require_sole_key_version: false,
                now: Timestamp(40),
                bypass: GovernanceBypass::Authorized,
            })
            .await,
        Err(MetaError::InvalidObjectLockState)
    ));
}

#[tokio::test]
async fn concurrent_lock_mutations_and_deletes_have_only_safe_serial_orders() {
    let store = Arc::new(InMemoryMetadataStore::new());
    let bucket = create_lock_bucket(store.as_ref(), "lock-races").await;

    // An expired retention may be extended concurrently with a permanent delete. If the extension
    // linearizes first, deletion observes the new protection; if deletion wins, the extension must
    // observe a missing target. Both succeeding is forbidden.
    let key = ObjectKey::parse("retention-vs-delete").unwrap();
    let version = VersionId::from_string("race-v1".to_owned());
    put(
        store.as_ref(),
        row(&bucket, key.as_str(), version.as_str(), Timestamp(10)),
        InitialObjectState {
            tags: Vec::new(),
            lock_intent: ExplicitObjectLockIntent {
                retention: Some(ObjectRetention {
                    mode: ObjectLockMode::Compliance,
                    retain_until: Timestamp(100),
                }),
                legal_hold: None,
            },
        },
    )
    .await
    .unwrap();
    let barrier = Arc::new(StartBarrier::new(3));
    let setter = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = key.clone();
        let version = version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::SetObjectRetention {
                    bucket,
                    key,
                    version_id: version,
                    retention: Some(ObjectRetention {
                        mode: ObjectLockMode::Compliance,
                        retain_until: Timestamp(1_000),
                    }),
                    now: Timestamp(200),
                    bypass: GovernanceBypass::Denied,
                })
                .await
        })
    };
    let deleter = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = key.clone();
        let version = version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::DeleteVersion {
                    bucket,
                    key,
                    version_id: version,
                    expected_row_id: None,
                    expected_updated_at: None,
                    require_sole_key_version: false,
                    now: Timestamp(200),
                    bypass: GovernanceBypass::Authorized,
                })
                .await
        })
    };
    barrier.wait().await;
    let set_result = setter.await.unwrap();
    let delete_result = deleter.await.unwrap();
    match (&set_result, &delete_result) {
        (Ok(MutationOutcome::Ack), Ok(MutationOutcome::DeleteProtected)) => {
            assert_eq!(
                store
                    .get_object_lock(&bucket, &key, &version)
                    .await
                    .unwrap()
                    .retention,
                Some(ObjectRetention {
                    mode: ObjectLockMode::Compliance,
                    retain_until: Timestamp(1_000),
                })
            );
        }
        (
            Err(MetaError::ObjectVersionNotFound),
            Ok(MutationOutcome::Deleted {
                promoted_latest: false,
                ..
            }),
        ) => {
            assert!(
                store
                    .get_version(&bucket, &key, &version)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        other => panic!("unsafe retention/delete race outcome: {other:?}"),
    }

    // The same serialization rule applies to a legal hold racing deletion.
    let hold_key = ObjectKey::parse("hold-vs-delete").unwrap();
    let hold_version = VersionId::from_string("race-v2".to_owned());
    put(
        store.as_ref(),
        row(
            &bucket,
            hold_key.as_str(),
            hold_version.as_str(),
            Timestamp(10),
        ),
        InitialObjectState::default(),
    )
    .await
    .unwrap();
    let barrier = Arc::new(StartBarrier::new(3));
    let holder = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = hold_key.clone();
        let version = hold_version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::SetObjectLegalHold {
                    bucket,
                    key,
                    version_id: version,
                    on: true,
                })
                .await
        })
    };
    let deleter = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = hold_key.clone();
        let version = hold_version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::DeleteVersion {
                    bucket,
                    key,
                    version_id: version,
                    expected_row_id: None,
                    expected_updated_at: None,
                    require_sole_key_version: false,
                    now: Timestamp(20),
                    bypass: GovernanceBypass::Authorized,
                })
                .await
        })
    };
    barrier.wait().await;
    let hold_result = holder.await.unwrap();
    let delete_result = deleter.await.unwrap();
    match (&hold_result, &delete_result) {
        (Ok(MutationOutcome::Ack), Ok(MutationOutcome::DeleteProtected)) => {
            assert!(
                store
                    .get_object_lock(&bucket, &hold_key, &hold_version)
                    .await
                    .unwrap()
                    .legal_hold
            );
        }
        (
            Err(MetaError::ObjectVersionNotFound),
            Ok(MutationOutcome::Deleted {
                promoted_latest: false,
                ..
            }),
        ) => {
            assert!(
                store
                    .get_version(&bucket, &hold_key, &hold_version)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        other => panic!("unsafe legal-hold/delete race outcome: {other:?}"),
    }

    // Two concurrent COMPLIANCE extensions must converge on the longest deadline. The shorter
    // request may win first, but it may never weaken a longer value that already committed.
    let update_key = ObjectKey::parse("retention-vs-retention").unwrap();
    let update_version = VersionId::from_string("race-v3".to_owned());
    put(
        store.as_ref(),
        row(
            &bucket,
            update_key.as_str(),
            update_version.as_str(),
            Timestamp(10),
        ),
        InitialObjectState {
            tags: Vec::new(),
            lock_intent: ExplicitObjectLockIntent {
                retention: Some(ObjectRetention {
                    mode: ObjectLockMode::Compliance,
                    retain_until: Timestamp(1_000),
                }),
                legal_hold: None,
            },
        },
    )
    .await
    .unwrap();
    let barrier = Arc::new(StartBarrier::new(3));
    let shorter = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = update_key.clone();
        let version = update_version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::SetObjectRetention {
                    bucket,
                    key,
                    version_id: version,
                    retention: Some(ObjectRetention {
                        mode: ObjectLockMode::Compliance,
                        retain_until: Timestamp(2_000),
                    }),
                    now: Timestamp(100),
                    bypass: GovernanceBypass::Denied,
                })
                .await
        })
    };
    let longer = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let bucket = bucket.clone();
        let key = update_key.clone();
        let version = update_version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .submit(Mutation::SetObjectRetention {
                    bucket,
                    key,
                    version_id: version,
                    retention: Some(ObjectRetention {
                        mode: ObjectLockMode::Compliance,
                        retain_until: Timestamp(3_000),
                    }),
                    now: Timestamp(100),
                    bypass: GovernanceBypass::Denied,
                })
                .await
        })
    };
    barrier.wait().await;
    let shorter_result = shorter.await.unwrap();
    let longer_result = longer.await.unwrap();
    assert!(
        matches!(shorter_result, Ok(MutationOutcome::Ack))
            || matches!(shorter_result, Err(MetaError::ObjectProtected))
    );
    assert!(matches!(longer_result, Ok(MutationOutcome::Ack)));
    assert_eq!(
        store
            .get_object_lock(&bucket, &update_key, &update_version)
            .await
            .unwrap()
            .retention,
        Some(ObjectRetention {
            mode: ObjectLockMode::Compliance,
            retain_until: Timestamp(3_000),
        })
    );
}

#[tokio::test]
async fn corrupt_configuration_is_repairable_and_multipart_defaults_resolve_at_completion() {
    let store = InMemoryMetadataStore::new();
    let bucket = create_lock_bucket(&store, "multipart-lock").await;
    store.install_raw_object_lock_configuration(
        &bucket,
        r#"{"enabled":true,"default_retention":null,"unknown":1}"#.to_owned(),
    );
    assert!(matches!(
        put(
            &store,
            row(&bucket, "unknown-field", "v0", Timestamp(10)),
            InitialObjectState::default(),
        )
        .await,
        Err(MetaError::InvalidObjectLockState)
    ));
    store.install_raw_object_lock_configuration(&bucket, r#"{"enabled":false}"#.to_owned());
    assert!(matches!(
        put(
            &store,
            row(&bucket, "blocked", "v0", Timestamp(10)),
            InitialObjectState::default(),
        )
        .await,
        Err(MetaError::InvalidObjectLockState)
    ));

    let initial_default = DefaultRetention {
        mode: ObjectLockMode::Governance,
        period: RetentionPeriod::Days(1),
    };
    store
        .submit(Mutation::UpdateObjectLockConfiguration {
            bucket: bucket.clone(),
            default_retention: Some(initial_default),
        })
        .await
        .unwrap();
    let upload = UploadId::from_string("mp-default".to_owned());
    store
        .submit(Mutation::CreateMultipart {
            session: Box::new(session(
                &bucket,
                upload.as_str(),
                "assembled",
                Timestamp(100),
                vec![("source".to_owned(), "multipart".to_owned())],
                ExplicitObjectLockIntent {
                    retention: None,
                    legal_hold: Some(true),
                },
            )),
            limits: cairn_types::meta::MultipartLimits::default(),
        })
        .await
        .unwrap();

    let completion_default = DefaultRetention {
        mode: ObjectLockMode::Compliance,
        period: RetentionPeriod::Days(2),
    };
    store
        .submit(Mutation::UpdateObjectLockConfiguration {
            bucket: bucket.clone(),
            default_retention: Some(completion_default),
        })
        .await
        .unwrap();
    let claim_token = MultipartClaimToken::generate();
    assert!(matches!(
        store
            .submit(Mutation::ClaimMultipart {
                upload_id: upload.clone(),
                claim_token: claim_token.clone(),
            })
            .await
            .unwrap(),
        MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(_))
    ));
    let completed = row(&bucket, "assembled", "v-complete", Timestamp(1_000));
    store
        .submit(Mutation::CompleteMultipart {
            upload_id: upload,
            claim_token,
            row: Box::new(completed.clone()),
            precondition: Precondition::default(),
            replication: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .get_object_tags(&bucket, &completed.key, &completed.version_id)
            .await
            .unwrap(),
        vec![("source".to_owned(), "multipart".to_owned())]
    );
    assert_eq!(
        store
            .get_object_lock(&bucket, &completed.key, &completed.version_id)
            .await
            .unwrap(),
        ObjectLockState {
            retention: Some(ObjectRetention {
                mode: ObjectLockMode::Compliance,
                retain_until: Timestamp(1_000 + 2 * 86_400_000),
            }),
            legal_hold: true,
        }
    );

    let expiring = UploadId::from_string("mp-explicit".to_owned());
    store
        .submit(Mutation::CreateMultipart {
            session: Box::new(session(
                &bucket,
                expiring.as_str(),
                "expires",
                Timestamp(10),
                Vec::new(),
                ExplicitObjectLockIntent {
                    retention: Some(ObjectRetention {
                        mode: ObjectLockMode::Governance,
                        retain_until: Timestamp(50),
                    }),
                    legal_hold: None,
                },
            )),
            limits: cairn_types::meta::MultipartLimits::default(),
        })
        .await
        .unwrap();
    let expiring_claim_token = MultipartClaimToken::generate();
    store
        .submit(Mutation::ClaimMultipart {
            upload_id: expiring.clone(),
            claim_token: expiring_claim_token.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .submit(Mutation::CompleteMultipart {
                upload_id: expiring.clone(),
                claim_token: expiring_claim_token,
                row: Box::new(row(&bucket, "expires", "v-expired", Timestamp(60))),
                precondition: Precondition::default(),
                replication: Vec::new(),
            })
            .await,
        Err(MetaError::InvalidObjectLockState)
    ));
    assert_eq!(
        store
            .get_multipart(&expiring)
            .await
            .unwrap()
            .unwrap()
            .status,
        MultipartStatus::Completing,
        "a failed completion must leave the owned session available for claim release/retry"
    );
}
