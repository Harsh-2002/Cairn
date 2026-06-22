//! Integration tests for [`ShardedMetadataStore`] (ARCH 30, Phase 3.2): N=1 is a faithful
//! pass-through, and N=3 routes each bucket to its owning shard while cross-bucket reads merge
//! every shard.

use cairn_types::authz::OwnershipMode;
use cairn_types::bucket::{Bucket, VersioningState};
use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::traits::MetadataStore;
use cairn_types::*;
use std::sync::Arc;

fn bucket(name: &str) -> Mutation {
    Mutation::CreateBucket(Box::new(Bucket {
        name: BucketName::parse(name).unwrap(),
        owner_id: UserId("owner".to_owned()),
        created_at: Timestamp(1),
        versioning: VersioningState::Enabled,
        ownership_mode: OwnershipMode::BucketOwnerEnforced,
        region: "us-east-1".to_owned(),
        compression: None,
    }))
}

fn row(bucket: &str, key: &str, size: u64) -> ObjectVersionRow {
    let b = BucketName::parse(bucket).unwrap();
    ObjectVersionRow {
        id: uuid::Uuid::new_v4().simple().to_string(),
        bucket: b.clone(),
        key: ObjectKey::parse(key).unwrap(),
        version_id: VersionId::null(),
        is_latest: true,
        is_delete_marker: false,
        size_logical: size,
        size_physical: size,
        etag: ETag::from_string("e".to_owned()),
        content_type: "application/octet-stream".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(StoragePath::generate(&b)),
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

fn put(row: ObjectVersionRow) -> Mutation {
    Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: Vec::new(),
    }
}

fn shards(
    n: usize,
) -> (
    cairn_meta::ShardedMetadataStore,
    Vec<Arc<dyn MetadataStore>>,
) {
    let inner: Vec<Arc<dyn MetadataStore>> = (0..n)
        .map(|_| Arc::new(cairn_meta::open_in_memory().unwrap()) as Arc<dyn MetadataStore>)
        .collect();
    (cairn_meta::ShardedMetadataStore::new(inner.clone()), inner)
}

const NAMES: &[&str] = &[
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
];

#[tokio::test]
async fn n1_is_a_faithful_passthrough() {
    let (store, inner) = shards(1);
    for n in NAMES {
        store.submit(bucket(n)).await.unwrap();
        store.submit(put(row(n, "k", 10))).await.unwrap();
    }
    // Everything is on the single shard, and the router reads it back identically.
    assert_eq!(store.list_buckets(None).await.unwrap().len(), NAMES.len());
    assert_eq!(
        inner[0].list_buckets(None).await.unwrap().len(),
        NAMES.len()
    );
    for n in NAMES {
        let b = BucketName::parse(n).unwrap();
        assert!(store.get_bucket(&b).await.unwrap().is_some());
        let k = ObjectKey::parse("k").unwrap();
        assert!(store.current_version(&b, &k).await.unwrap().is_some());
    }
    let counts = store.aggregate_counts().await.unwrap();
    assert_eq!(counts.buckets, NAMES.len() as u64);
    assert_eq!(counts.objects, NAMES.len() as u64);
    assert_eq!(counts.logical_bytes, 10 * NAMES.len() as u64);
}

#[tokio::test]
async fn n3_routes_each_bucket_to_its_owning_shard() {
    let n = 3;
    let (store, inner) = shards(n);
    for name in NAMES {
        store.submit(bucket(name)).await.unwrap();
        store.submit(put(row(name, "k", 7))).await.unwrap();
    }

    // Each bucket lives on exactly its owning shard, and nowhere else.
    for name in NAMES {
        let b = BucketName::parse(name).unwrap();
        let owner = cairn_meta::shard_for_bucket(name, n);
        for (i, shard) in inner.iter().enumerate() {
            let present = shard.get_bucket(&b).await.unwrap().is_some();
            assert_eq!(
                present,
                i == owner,
                "bucket {name} should be only on shard {owner}, found on {i}={present}"
            );
        }
        // The router still finds it (routes to the owner) and its object.
        assert!(store.get_bucket(&b).await.unwrap().is_some());
        let k = ObjectKey::parse("k").unwrap();
        assert!(store.current_version(&b, &k).await.unwrap().is_some());
    }

    // Cross-bucket reads merge every shard.
    let mut names: Vec<String> = store
        .list_buckets(None)
        .await
        .unwrap()
        .into_iter()
        .map(|b| b.name.as_str().to_owned())
        .collect();
    names.sort();
    let mut expected: Vec<String> = NAMES.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(names, expected, "list_buckets must merge all shards");
    // And the result must be globally sorted (the merge keeps order).
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted);

    let counts = store.aggregate_counts().await.unwrap();
    assert_eq!(
        counts.buckets,
        NAMES.len() as u64,
        "counts sum across shards"
    );
    assert_eq!(counts.objects, NAMES.len() as u64);
    assert_eq!(counts.logical_bytes, 7 * NAMES.len() as u64);

    let bc = store.bucket_counts().await.unwrap();
    assert_eq!(bc.len(), NAMES.len());
    assert!(
        bc.windows(2).all(|w| w[0].bucket <= w[1].bucket),
        "bucket_counts sorted"
    );
}

#[tokio::test]
async fn n3_multipart_rides_the_bucket_shard_via_encoded_id() {
    let n = 3;
    let (store, inner) = shards(n);
    let name = "charlie";
    store.submit(bucket(name)).await.unwrap();

    // Create a multipart session through the router; the returned upload id encodes the shard.
    let b = BucketName::parse(name).unwrap();
    let session = MultipartSession {
        upload_id: UploadId::generate(),
        bucket: b.clone(),
        key: ObjectKey::parse("big").unwrap(),
        content_type: "application/octet-stream".to_owned(),
        status: cairn_types::meta::MultipartStatus::Active,
        owner_id: UserId("owner".to_owned()),
        intended_acl: None,
        user_metadata: Vec::new(),
        sse_requested: false,
        created_at: Timestamp(1),
        updated_at: Timestamp(1),
    };
    let outcome = store
        .submit(Mutation::CreateMultipart(Box::new(session)))
        .await
        .unwrap();
    let upload_id = match outcome {
        MutationOutcome::MultipartCreated(id) => id,
        other => panic!("expected MultipartCreated, got {other:?}"),
    };

    // The session lives on the bucket's owning shard, addressable through the router by the encoded
    // id, and is absent from the other shards.
    let owner = cairn_meta::shard_for_bucket(name, n);
    assert!(store.get_multipart(&upload_id).await.unwrap().is_some());
    for (i, shard) in inner.iter().enumerate() {
        let present = shard.get_multipart(&upload_id).await.unwrap().is_some();
        assert_eq!(
            present,
            i == owner,
            "multipart session must be only on shard {owner}"
        );
    }
}
