use cairn_types::meta::MultipartReplicaIntent;
use cairn_types::*;

pub async fn preserves_replica_intent(store: &dyn MetadataStore) {
    let bucket = BucketName::parse("replica-session").unwrap();
    let owner = UserId::generate();
    store
        .submit(Mutation::CreateBucket(Box::new(Bucket {
            name: bucket.clone(),
            owner_id: owner.clone(),
            created_at: Timestamp(1),
            versioning: VersioningState::Enabled,
            ownership_mode: OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".to_owned(),
            compression: None,
        })))
        .await
        .unwrap();
    let upload_id = UploadId::generate();
    let intent = MultipartReplicaIntent {
        version_id: VersionId::generate(),
        content_encoding: Some("gzip".to_owned()),
        cache_control: Some("max-age=60".to_owned()),
        content_disposition: None,
        content_language: Some("en".to_owned()),
        expires: None,
        checksums: vec![cairn_types::object::ChecksumValue {
            algorithm: cairn_types::object::ChecksumAlgorithm::Sha256,
            value: "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=".to_owned(),
        }],
    };
    store
        .submit(Mutation::CreateMultipart {
            session: Box::new(MultipartSession {
                upload_id: upload_id.clone(),
                bucket: bucket.clone(),
                key: ObjectKey::parse("key").unwrap(),
                content_type: "text/plain".to_owned(),
                status: MultipartStatus::Active,
                owner_id: owner.clone(),
                initiated_by: owner,
                intended_acl: None,
                replica_intent: Some(intent.clone()),
                user_metadata: vec![],
                initial_tags: vec![],
                lock_intent: cairn_types::object::ExplicitObjectLockIntent::default(),
                sse_requested: true,
                encrypt_parts: true,
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
    assert_eq!(
        store
            .get_multipart(&upload_id)
            .await
            .unwrap()
            .unwrap()
            .replica_intent,
        Some(intent.clone())
    );
    let token = MultipartClaimToken::generate();
    match store
        .submit(Mutation::ClaimMultipart {
            upload_id: upload_id.clone(),
            claim_token: token,
        })
        .await
        .unwrap()
    {
        MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(session)) => {
            assert_eq!(session.replica_intent, Some(intent.clone()))
        }
        other => panic!("unexpected claim: {other:?}"),
    }
    store
        .submit(Mutation::RecoverMultipartClaims)
        .await
        .unwrap();
    let session = store.get_multipart(&upload_id).await.unwrap().unwrap();
    assert_eq!(session.status, MultipartStatus::Active);
    assert_eq!(session.replica_intent, Some(intent));
}
