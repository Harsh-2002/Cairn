//! Gate tests for the management API, exercised against the in-memory trait doubles.

use bytes::Bytes;
use cairn_control::{ControlResponse, ControlService};
use cairn_types::auth::{AuthMethod, Principal, Role};
use cairn_types::blob::StageOptions;
use cairn_types::bucket::VersioningState;
use cairn_types::id::{BucketName, ObjectKey, UserId, VersionId};
use cairn_types::meta::{Mutation, Precondition};
use cairn_types::object::ChecksumSet;
use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::testing::{InMemoryBlobStore, InMemoryMetadataStore, StubCrypto, TestClock};
use cairn_types::traits::{BlobStore, Clock, Crypto, MetadataStore};
use http::{Method, StatusCode};
use std::sync::Arc;

fn admin() -> Principal {
    Principal {
        user_id: UserId("admin".to_owned()),
        display_name: "admin".to_owned(),
        access_key_id: "cairn_admin".to_owned(),
        role: Role::Administrator,
        method: AuthMethod::Bearer,
    }
}

fn member() -> Principal {
    Principal {
        user_id: UserId("bob".to_owned()),
        display_name: "bob".to_owned(),
        access_key_id: "cairn_bob".to_owned(),
        role: Role::Member,
        method: AuthMethod::Bearer,
    }
}

struct Harness {
    svc: ControlService,
    meta: Arc<InMemoryMetadataStore>,
    blob: Arc<InMemoryBlobStore>,
    clock: Arc<TestClock>,
}

fn harness() -> Harness {
    let meta = Arc::new(InMemoryMetadataStore::new());
    let blob = Arc::new(InMemoryBlobStore::new());
    let crypto = Arc::new(StubCrypto);
    let clock = Arc::new(TestClock::default());
    let svc = ControlService::new(
        meta.clone() as Arc<dyn MetadataStore>,
        blob.clone() as Arc<dyn BlobStore>,
        crypto as Arc<dyn Crypto>,
        clock.clone() as Arc<dyn Clock>,
    );
    Harness {
        svc,
        meta,
        blob,
        clock,
    }
}

fn body_stream(data: &'static [u8]) -> cairn_types::BodyStream {
    let bytes = Bytes::from_static(data);
    Box::pin(futures_util::stream::once(async move {
        Ok::<Bytes, cairn_types::error::BodyError>(bytes)
    }))
}

fn json(resp: &ControlResponse) -> serde_json::Value {
    serde_json::from_slice(&resp.body).expect("response body is JSON")
}

/// Put an object through the metadata + blob doubles, the way the data plane would, so the
/// overview/counts reflect it.
async fn put_object(h: &Harness, bucket: &str, key: &str, data: &'static [u8]) {
    let bname = BucketName::parse(bucket).unwrap();
    let staged = h
        .blob
        .stage(
            &bname,
            body_stream(data),
            StageOptions {
                compression: None,
                extra_checksums: ChecksumSet::none(),
                size_ceiling: 1 << 30,
                content_type: "application/octet-stream".to_owned(),
            },
        )
        .await
        .unwrap();
    let now = h.clock.now();
    let row = ObjectVersionRow {
        id: uuid::Uuid::new_v4().simple().to_string(),
        bucket: bname,
        key: ObjectKey::parse(key).unwrap(),
        version_id: VersionId::null(),
        is_latest: true,
        is_delete_marker: false,
        size_logical: staged.size_logical,
        size_physical: staged.size_physical,
        etag: ETag::from_md5_hex(staged.md5_hex.clone()),
        content_type: "application/octet-stream".to_owned(),
        storage_path: Some(staged.storage_path.clone()),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId("admin".to_owned()),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        replication_status: None,
        created_at: now,
        updated_at: now,
    };
    h.meta
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: None,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn health_is_unauthenticated_and_ok() {
    let h = harness();
    let resp = h
        .svc
        .handle(&Method::GET, "/health", &[], None, Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert_eq!(v["status"], "ok");
    assert_eq!(v["ready"], true);
}

#[tokio::test]
async fn non_admin_is_forbidden_on_overview() {
    let h = harness();
    let m = member();
    let resp = h
        .svc
        .handle(&Method::GET, "/overview", &[], Some(&m), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
    assert_eq!(json(&resp)["error"], "forbidden");
}

#[tokio::test]
async fn anonymous_is_forbidden_on_overview() {
    let h = harness();
    let resp = h
        .svc
        .handle(&Method::GET, "/overview", &[], None, Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn admin_bucket_lifecycle() {
    let h = harness();
    let a = admin();

    // Create.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/buckets",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"name":"photos"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    assert_eq!(json(&resp)["name"], "photos");

    // Duplicate create -> 409.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/buckets",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"name":"photos"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::CONFLICT);

    // List shows it.
    let resp = h
        .svc
        .handle(&Method::GET, "/buckets", &[], Some(&a), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let names: Vec<&str> = v["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"photos"));
    assert_eq!(v["buckets"][0]["versioning"], "unversioned");

    // Detail.
    let resp = h
        .svc
        .handle(&Method::GET, "/buckets/photos", &[], Some(&a), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert_eq!(v["name"], "photos");
    assert_eq!(v["versioning"], "unversioned");
    assert_eq!(v["ownership_mode"], "bucket-owner-enforced");
    assert_eq!(v["object_count"], 0);

    // Detail of a missing bucket -> 404.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/missing",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);

    // Delete -> 204.
    let resp = h
        .svc
        .handle(
            &Method::DELETE,
            "/buckets/photos",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    assert!(resp.body.is_empty());

    // Gone now.
    let resp = h
        .svc
        .handle(&Method::GET, "/buckets/photos", &[], Some(&a), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn force_delete_empties_populated_bucket() {
    let h = harness();
    let a = admin();
    h.svc
        .handle(
            &Method::POST,
            "/buckets",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"name":"data"}"#),
        )
        .await;
    put_object(&h, "data", "a.txt", b"hello").await;
    put_object(&h, "data", "b.txt", b"world!!").await;
    assert_eq!(h.blob.blob_count(), 2);

    let resp = h
        .svc
        .handle(
            &Method::DELETE,
            "/buckets/data",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    // Bucket gone, blobs reclaimed.
    assert!(
        h.meta
            .get_bucket(&BucketName::parse("data").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(h.blob.blob_count(), 0);
}

#[tokio::test]
async fn create_user_returns_secret_once_and_lists() {
    let h = harness();
    let a = admin();

    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/users",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"display_name":"Alice","role":"member"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let v = json(&resp);
    let key_id = v["bearer_access_key_id"].as_str().unwrap().to_owned();
    let secret = v["bearer_secret"].as_str().unwrap().to_owned();
    assert!(key_id.starts_with("cairn_"));
    assert!(!secret.is_empty());

    // The created user is listable and its stored hash matches the returned secret.
    let resp = h
        .svc
        .handle(&Method::GET, "/users", &[], Some(&a), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let users = v["users"].as_array().unwrap();
    let found = users
        .iter()
        .find(|u| u["access_key_id"] == key_id.as_str())
        .expect("new user is listed");
    assert_eq!(found["display_name"], "Alice");
    assert_eq!(found["role"], "member");
    assert_eq!(found["is_active"], true);

    // The persisted bearer hash matches what cairn-auth computes from the returned secret.
    let stored = h.meta.user_by_bearer_key(&key_id).await.unwrap().unwrap();
    assert_eq!(stored.secret_hash, cairn_auth::hash_bearer_secret(&secret));

    // Bad role -> 400.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/users",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"display_name":"X","role":"root"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Malformed body -> 400.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/users",
            &[],
            Some(&a),
            Bytes::from_static(b"not json"),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn overview_reflects_counts_after_put() {
    let h = harness();
    let a = admin();
    h.svc
        .handle(
            &Method::POST,
            "/buckets",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"name":"vault"}"#),
        )
        .await;
    put_object(&h, "vault", "k1", b"12345").await; // 5 bytes
    put_object(&h, "vault", "k2", b"678").await; // 3 bytes

    let resp = h
        .svc
        .handle(&Method::GET, "/overview", &[], Some(&a), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert_eq!(v["buckets"], 1);
    assert_eq!(v["objects"], 2);
    assert_eq!(v["versions"], 2);
    assert_eq!(v["logical_bytes"], 8);
    assert!(v["compression_ratio"].as_f64().unwrap() > 0.0);

    // The per-bucket detail reports the same per-bucket totals.
    let resp = h
        .svc
        .handle(&Method::GET, "/buckets/vault", &[], Some(&a), Bytes::new())
        .await;
    let v = json(&resp);
    assert_eq!(v["object_count"], 2);
    assert_eq!(v["logical_bytes"], 8);
}

#[tokio::test]
async fn list_objects_with_prefix_and_limit() {
    let h = harness();
    let a = admin();
    h.svc
        .handle(
            &Method::POST,
            "/buckets",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"name":"media"}"#),
        )
        .await;
    put_object(&h, "media", "img/1.png", b"a").await;
    put_object(&h, "media", "img/2.png", b"bb").await;
    put_object(&h, "media", "doc/1.txt", b"ccc").await;

    let q = vec![("prefix".to_owned(), "img/".to_owned())];
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/media/objects",
            &q,
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let objs = v["objects"].as_array().unwrap();
    assert_eq!(objs.len(), 2);
    for o in objs {
        assert!(o["key"].as_str().unwrap().starts_with("img/"));
    }

    // Objects on a missing bucket -> 404.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/nope/objects",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn activity_records_mutations() {
    let h = harness();
    let a = admin();
    h.svc
        .handle(
            &Method::POST,
            "/buckets",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"name":"logged"}"#),
        )
        .await;

    let resp = h
        .svc
        .handle(&Method::GET, "/activity", &[], Some(&a), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let entries = v["entries"].as_array().unwrap();
    assert!(entries.iter().any(|e| e["action"] == "CreateBucket"));
}

#[tokio::test]
async fn unknown_subpath_is_404() {
    let h = harness();
    let a = admin();
    let resp = h
        .svc
        .handle(&Method::GET, "/nope/nowhere", &[], Some(&a), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert_eq!(json(&resp)["error"], "not found");
}

#[tokio::test]
async fn bad_create_bucket_body_is_400() {
    let h = harness();
    let a = admin();
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/buckets",
            &[],
            Some(&a),
            Bytes::from_static(b"{ not json"),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Invalid bucket name -> 400.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/buckets",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"name":"A"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

// Keep the versioning enum referenced so a future contract change is caught at compile time.
#[test]
fn versioning_variants_exist() {
    let _ = (
        VersioningState::Unversioned,
        VersioningState::Enabled,
        VersioningState::Suspended,
    );
}
