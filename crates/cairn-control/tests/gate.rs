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
        chunk_signing: None,
    }
}

fn member() -> Principal {
    Principal {
        user_id: UserId("bob".to_owned()),
        display_name: "bob".to_owned(),
        access_key_id: "cairn_bob".to_owned(),
        role: Role::Member,
        method: AuthMethod::Bearer,
        chunk_signing: None,
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

async fn make_bucket(h: &Harness, a: &Principal, name: &str) {
    let body = format!(r#"{{"name":"{name}"}}"#);
    let resp = h
        .svc
        .handle(&Method::POST, "/buckets", &[], Some(a), Bytes::from(body))
        .await;
    assert_eq!(resp.status, StatusCode::CREATED);
}

#[tokio::test]
async fn bucket_config_get_reflects_aspects() {
    let h = harness();
    let a = admin();
    make_bucket(&h, &a, "cfg").await;

    // Initially every aspect is null and state is the bucket defaults.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/cfg/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert_eq!(v["versioning"], "unversioned");
    assert_eq!(v["ownership_mode"], "bucket-owner-enforced");
    assert!(v["quota_bytes"].is_null());
    assert!(v["policy"].is_null());
    assert!(v["cors"].is_null());
    assert!(v["tagging"].is_null());
    assert!(v["lifecycle"].is_null());
    assert!(v["acl"].is_null());
    assert!(v["public_access_block"].is_null());

    // Missing bucket -> 404.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/missing/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);

    // Non-admin -> 403.
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/cfg/config",
            &[],
            Some(&m),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn set_versioning_updates_state() {
    let h = harness();
    let a = admin();
    make_bucket(&h, &a, "vers").await;

    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/vers/versioning",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"status":"Enabled"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    assert!(resp.body.is_empty());

    let b = h
        .meta
        .get_bucket(&BucketName::parse("vers").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b.versioning, VersioningState::Enabled);

    // Suspended round-trips through the config view.
    h.svc
        .handle(
            &Method::PUT,
            "/buckets/vers/versioning",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"status":"Suspended"}"#),
        )
        .await;
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/vers/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(json(&resp)["versioning"], "suspended");

    // Bad status -> 400.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/vers/versioning",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"status":"On"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Missing bucket -> 404.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/nope/versioning",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"status":"Enabled"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);

    // Non-admin -> 403.
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/vers/versioning",
            &[],
            Some(&m),
            Bytes::from_static(br#"{"status":"Enabled"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn set_quota_is_accepted() {
    let h = harness();
    let a = admin();
    make_bucket(&h, &a, "quota").await;

    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/quota/quota",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"quota_bytes":1048576}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    // Clearing the quota (null) is also accepted.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/quota/quota",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"quota_bytes":null}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    // Bad body -> 400.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/quota/quota",
            &[],
            Some(&a),
            Bytes::from_static(b"not json"),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn policy_put_validates_and_get_round_trips() {
    let h = harness();
    let a = admin();
    make_bucket(&h, &a, "pol").await;

    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::pol/*"}]}"#;
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/pol/policy",
            &[],
            Some(&a),
            Bytes::from_static(policy),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    // The stored policy is surfaced as structured JSON by the config view.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/pol/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    let v = json(&resp);
    assert_eq!(v["policy"]["Version"], "2012-10-17");

    // A malformed policy is rejected at the edge -> 400, and is not stored.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/pol/policy",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"not":"a policy"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/pol/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    // The earlier valid policy still stands.
    assert_eq!(json(&resp)["policy"]["Version"], "2012-10-17");

    // Delete clears it.
    let resp = h
        .svc
        .handle(
            &Method::DELETE,
            "/buckets/pol/policy",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/pol/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert!(json(&resp)["policy"].is_null());

    // Non-admin cannot set a policy.
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/pol/policy",
            &[],
            Some(&m),
            Bytes::from_static(policy),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

/// Create a user via the API and return (id, access_key_id).
async fn create_member(h: &Harness, a: &Principal) -> (String, String) {
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/users",
            &[],
            Some(a),
            Bytes::from_static(br#"{"display_name":"Carol","role":"member"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let v = json(&resp);
    (
        v["id"].as_str().unwrap().to_owned(),
        v["bearer_access_key_id"].as_str().unwrap().to_owned(),
    )
}

#[tokio::test]
async fn patch_user_changes_role_and_deactivates() {
    let h = harness();
    let a = admin();
    let (id, key_id) = create_member(&h, &a).await;

    // Promote to administrator.
    let resp = h
        .svc
        .handle(
            &Method::PATCH,
            &format!("/users/{id}"),
            &[],
            Some(&a),
            Bytes::from_static(br#"{"role":"administrator"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert_eq!(v["role"], "administrator");
    assert_eq!(v["is_active"], true);

    // The change is durable and preserved the credential (bearer key unchanged).
    let stored = h.meta.user_by_bearer_key(&key_id).await.unwrap().unwrap();
    assert_eq!(stored.user.role, Role::Administrator);

    // Deactivate.
    let resp = h
        .svc
        .handle(
            &Method::PATCH,
            &format!("/users/{id}"),
            &[],
            Some(&a),
            Bytes::from_static(br#"{"is_active":false}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(json(&resp)["is_active"], false);
    let stored = h.meta.user_by_bearer_key(&key_id).await.unwrap().unwrap();
    assert!(!stored.user.is_active);

    // Empty patch -> 400.
    let resp = h
        .svc
        .handle(
            &Method::PATCH,
            &format!("/users/{id}"),
            &[],
            Some(&a),
            Bytes::from_static(b"{}"),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Bad role -> 400.
    let resp = h
        .svc
        .handle(
            &Method::PATCH,
            &format!("/users/{id}"),
            &[],
            Some(&a),
            Bytes::from_static(br#"{"role":"root"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Unknown user -> 404.
    let resp = h
        .svc
        .handle(
            &Method::PATCH,
            "/users/does-not-exist",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"is_active":false}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);

    // Non-admin -> 403.
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::PATCH,
            &format!("/users/{id}"),
            &[],
            Some(&m),
            Bytes::from_static(br#"{"is_active":true}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn rotate_credentials_mints_new_secret() {
    let h = harness();
    let a = admin();
    let (id, key_id) = create_member(&h, &a).await;
    let before = h.meta.user_by_bearer_key(&key_id).await.unwrap().unwrap();

    let resp = h
        .svc
        .handle(
            &Method::POST,
            &format!("/users/{id}/rotate-credentials"),
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert_eq!(v["bearer_access_key_id"], key_id.as_str());
    let new_secret = v["bearer_secret"].as_str().unwrap();
    assert!(!new_secret.is_empty());

    // The stored hash now matches the freshly returned secret, and differs from before.
    let after = h.meta.user_by_bearer_key(&key_id).await.unwrap().unwrap();
    assert_eq!(
        after.secret_hash,
        cairn_auth::hash_bearer_secret(new_secret)
    );
    assert_ne!(after.secret_hash, before.secret_hash);

    // Unknown user -> 404.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/users/nobody/rotate-credentials",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);

    // Non-admin -> 403.
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::POST,
            &format!("/users/{id}/rotate-credentials"),
            &[],
            Some(&m),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn failed_replication_lists_empty_and_is_gated() {
    let h = harness();
    let a = admin();

    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/replication/failed",
            &[("limit".to_owned(), "10".to_owned())],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert!(v["entries"].as_array().unwrap().is_empty());

    // Non-admin -> 403.
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/replication/failed",
            &[],
            Some(&m),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn failed_replication_reflects_a_planted_terminal_entry() {
    let h = harness();
    let a = admin();

    // Plant a replication outbox entry by committing a version that carries it, then mark it
    // terminally failed the way the replication engine does (next_attempt_at = None).
    let bucket = BucketName::parse("repl").unwrap();
    let key = ObjectKey::parse("photo.jpg").unwrap();
    let version = VersionId::from_string("00000001".to_owned());
    let entry = cairn_types::meta::OutboxEntry {
        id: "outbox-1".to_owned(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        operation: cairn_types::meta::ReplicationOp::ObjectCreate,
        rule_id: "rule-1".to_owned(),
        attempts: 0,
        next_attempt_at: cairn_types::time::Timestamp(0),
        status: cairn_types::meta::ReplicationStatus::Pending,
        last_error: None,
    };
    let now = h.clock.now();
    let row = ObjectVersionRow {
        id: uuid::Uuid::new_v4().simple().to_string(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        is_latest: true,
        is_delete_marker: false,
        size_logical: 4,
        size_physical: 4,
        etag: ETag::from_md5_hex("deadbeef".to_owned()),
        content_type: "image/jpeg".to_owned(),
        storage_path: Some(cairn_types::id::StoragePath::from_string(
            "repl/00000001".to_owned(),
        )),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId("admin".to_owned()),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        replication_status: Some(cairn_types::meta::ReplicationStatus::Pending),
        created_at: now,
        updated_at: now,
    };
    h.meta
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: Some(entry),
        })
        .await
        .unwrap();
    h.meta
        .submit(Mutation::MarkReplicationFailed {
            id: "outbox-1".to_owned(),
            error: "destination unreachable".to_owned(),
            next_attempt_at: None,
        })
        .await
        .unwrap();

    // The endpoint now reports the terminal entry with its real fields.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/replication/failed",
            &[("limit".to_owned(), "10".to_owned())],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let entries = v["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["bucket"], "repl");
    assert_eq!(entries[0]["key"], "photo.jpg");
    assert_eq!(entries[0]["version_id"], "00000001");
    assert_eq!(entries[0]["error"], "destination unreachable");
    assert_eq!(entries[0]["attempts"], 1);
    assert!(entries[0]["next_attempt_at_ms"].is_i64());
}

#[tokio::test]
async fn config_reports_a_set_quota() {
    let h = harness();
    let a = admin();
    make_bucket(&h, &a, "limited").await;

    // Before any quota is set the config view reports null.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/limited/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert!(json(&resp)["quota_bytes"].is_null());

    // Set a quota through the management endpoint.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/limited/quota",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"quota_bytes":1048576}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    // The config view now reports the configured quota via get_bucket_quota.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/limited/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(json(&resp)["quota_bytes"], 1_048_576);

    // Clearing the quota returns the config view to null.
    h.svc
        .handle(
            &Method::PUT,
            "/buckets/limited/quota",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"quota_bytes":null}"#),
        )
        .await;
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/limited/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert!(json(&resp)["quota_bytes"].is_null());
}

#[tokio::test]
async fn config_mutations_record_activity() {
    let h = harness();
    let a = admin();
    make_bucket(&h, &a, "audit").await;
    h.svc
        .handle(
            &Method::PUT,
            "/buckets/audit/versioning",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"status":"Enabled"}"#),
        )
        .await;

    let resp = h
        .svc
        .handle(&Method::GET, "/activity", &[], Some(&a), Bytes::new())
        .await;
    let v = json(&resp);
    let actions: Vec<&str> = v["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert!(actions.contains(&"SetVersioning"));
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
