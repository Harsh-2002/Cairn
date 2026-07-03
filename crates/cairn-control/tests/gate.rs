//! Gate tests for the management API, exercised against the in-memory trait doubles.

use bytes::Bytes;
use cairn_control::{ControlResponse, ControlService, SystemInfo};
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
        user_policy: None,
        is_session: false,
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
        user_policy: None,
        is_session: false,
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
        SystemInfo {
            version: "test".to_owned(),
            s3_addr: "127.0.0.1:7373".to_owned(),
            ui_addr: "127.0.0.1:7374".to_owned(),
            tls: false,
            data_dir: std::env::temp_dir(),
            started_at: std::time::Instant::now(),
        },
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
                encryption: None,
                content_length: Some(data.len() as u64),
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
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: Some(staged.storage_path.clone()),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId("admin".to_owned()),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        sse_descriptor: None,
        replication_status: None,
        created_at: now,
        updated_at: now,
    };
    h.meta
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: Vec::new(),
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
async fn overview_buckets_breakdown_sums_to_totals() {
    let h = harness();
    let a = admin();
    for name in [r#"{"name":"vault"}"#, r#"{"name":"empty"}"#] {
        h.svc
            .handle(
                &Method::POST,
                "/buckets",
                &[],
                Some(&a),
                Bytes::from(name.as_bytes().to_vec()),
            )
            .await;
    }
    put_object(&h, "vault", "k1", b"12345").await; // 5 bytes
    put_object(&h, "vault", "k2", b"678").await; // 3 bytes

    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/overview/buckets",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let buckets = v["buckets"].as_array().unwrap();
    // Sorted by name; the empty bucket is present with zeros.
    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0]["name"], "empty");
    assert_eq!(buckets[0]["objects"], 0);
    assert_eq!(buckets[0]["logical_bytes"], 0);
    assert_eq!(buckets[1]["name"], "vault");
    assert_eq!(buckets[1]["objects"], 2);
    assert_eq!(buckets[1]["logical_bytes"], 8);

    // The breakdown sums to the /overview totals.
    let resp = h
        .svc
        .handle(&Method::GET, "/overview", &[], Some(&a), Bytes::new())
        .await;
    let totals = json(&resp);
    let sum: u64 = buckets
        .iter()
        .map(|b| b["logical_bytes"].as_u64().unwrap())
        .sum();
    assert_eq!(totals["logical_bytes"].as_u64().unwrap(), sum);
}

#[tokio::test]
async fn overview_buckets_requires_admin() {
    let h = harness();
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/overview/buckets",
            &[],
            Some(&m),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn system_requires_admin() {
    let h = harness();
    let m = member();
    for principal in [Some(&m), None] {
        let resp = h
            .svc
            .handle(&Method::GET, "/system", &[], principal, Bytes::new())
            .await;
        assert_eq!(resp.status, StatusCode::FORBIDDEN);
        assert_eq!(json(&resp)["error"], "forbidden");
    }
}

#[tokio::test]
async fn system_reports_identity_and_disk() {
    let h = harness();
    let a = admin();
    let resp = h
        .svc
        .handle(&Method::GET, "/system", &[], Some(&a), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert_eq!(v["version"], "test");
    assert_eq!(v["s3_addr"], "127.0.0.1:7373");
    assert_eq!(v["ui_addr"], "127.0.0.1:7374");
    assert_eq!(v["tls"], false);
    assert!(!v["data_dir"].as_str().unwrap().is_empty());
    assert!(v["uptime_secs"].as_u64().is_some());
    #[cfg(unix)]
    {
        let total = v["disk_total_bytes"].as_u64().expect("disk total on unix");
        let free = v["disk_free_bytes"].as_u64().expect("disk free on unix");
        assert!(total > 0);
        assert!(free <= total);
    }
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

    // Seed a second active administrator so deactivating `id` below isn't blocked by the
    // last-active-admin break-glass guard (which now also covers PATCH).
    let (id2, _) = create_member(&h, &a).await;
    let resp = h
        .svc
        .handle(
            &Method::PATCH,
            &format!("/users/{id2}"),
            &[],
            Some(&a),
            Bytes::from_static(br#"{"role":"administrator"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);

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

/// Audit 2026-07: PATCH /users must enforce the same break-glass guards as DELETE — a deactivation
/// or demotion can strand the control plane exactly like a delete. Before the fix patch_user had
/// none of these guards.
#[tokio::test]
async fn patch_user_cannot_strand_the_control_plane() {
    let h = harness();
    let a = admin();

    // Promote a member so it is the only active administrator in the store.
    let (id, _) = create_member(&h, &a).await;
    let path = format!("/users/{id}");
    let patch = |body: &'static [u8]| {
        h.svc.handle(
            &Method::PATCH,
            &path,
            &[],
            Some(&a),
            Bytes::from_static(body),
        )
    };
    assert_eq!(
        patch(br#"{"role":"administrator"}"#).await.status,
        StatusCode::OK
    );

    // Deactivating the last active administrator is refused.
    assert_eq!(
        patch(br#"{"is_active":false}"#).await.status,
        StatusCode::BAD_REQUEST,
        "deactivating the last admin must be blocked"
    );
    // Demoting the last active administrator to member is refused.
    assert_eq!(
        patch(br#"{"role":"member"}"#).await.status,
        StatusCode::BAD_REQUEST,
        "demoting the last admin must be blocked"
    );

    // A self-patch that removes the caller's own admin access is refused (self-lockout). Build a
    // principal whose user_id matches the target and drive a self-deactivation.
    let self_principal = Principal {
        user_id: UserId(id.clone()),
        display_name: "self".to_owned(),
        access_key_id: "cairn_self".to_owned(),
        role: Role::Administrator,
        method: AuthMethod::Bearer,
        chunk_signing: None,
        user_policy: None,
        is_session: false,
    };
    let resp = h
        .svc
        .handle(
            &Method::PATCH,
            &format!("/users/{id}"),
            &[],
            Some(&self_principal),
            Bytes::from_static(br#"{"is_active":false}"#),
        )
        .await;
    assert_eq!(
        resp.status,
        StatusCode::BAD_REQUEST,
        "self-deactivation must be blocked"
    );

    // The target is still an active administrator — none of the rejected patches took effect.
    let users = h.meta.list_users().await.unwrap();
    let target = users
        .iter()
        .find(|u| u.id == UserId(id.clone()))
        .expect("target user still present");
    assert!(target.is_active && target.role == Role::Administrator);
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
        enqueued_at: cairn_types::time::Timestamp(0),
        id: "outbox-1".to_owned(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        operation: cairn_types::meta::ReplicationOp::ObjectCreate,
        rule_id: "rule-1".to_owned(),
        target_arn: None,
        attempts: 0,
        next_attempt_at: cairn_types::time::Timestamp(0),
        status: cairn_types::meta::ReplicationStatus::Pending,
        last_error: None,
        priority: 0,
        lease_until: None,
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
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
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
        sse_descriptor: None,
        replication_status: Some(cairn_types::meta::ReplicationStatus::Pending),
        created_at: now,
        updated_at: now,
    };
    h.meta
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: vec![entry],
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

#[tokio::test]
async fn health_reflects_store_readiness() {
    let h = harness();
    // A working store probes ready.
    let resp = h
        .svc
        .handle(&Method::GET, "/health", &[], None, Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert_eq!(v["status"], "ok");
    assert_eq!(v["ready"], true);

    // Every control response, including the unauthenticated health probe, carries a request id.
    assert!(!resp.request_id.is_empty());
}

#[tokio::test]
async fn error_envelope_and_response_carry_request_id() {
    let h = harness();
    let a = admin();

    // An error path: a 404 envelope carries a non-empty request_id that matches the response field.
    let resp = h
        .svc
        .handle(&Method::GET, "/nope/nowhere", &[], Some(&a), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert!(!resp.request_id.is_empty());
    let v = json(&resp);
    assert_eq!(v["error"], "not found");
    assert_eq!(v["request_id"], resp.request_id.as_str());

    // A success path also carries a request id (on the response, for the header).
    let resp = h
        .svc
        .handle(&Method::GET, "/health", &[], Some(&a), Bytes::new())
        .await;
    assert!(!resp.request_id.is_empty());

    // Two requests get distinct ids.
    let r1 = h
        .svc
        .handle(&Method::GET, "/health", &[], None, Bytes::new())
        .await;
    let r2 = h
        .svc
        .handle(&Method::GET, "/health", &[], None, Bytes::new())
        .await;
    assert_ne!(r1.request_id, r2.request_id);
}

#[tokio::test]
async fn record_activity_populates_actor() {
    let h = harness();
    let a = admin();
    make_bucket(&h, &a, "audited").await;

    // The recorded activity entry names the acting administrator by access-key id.
    let entries = h.meta.list_activity(100).await.unwrap();
    let create = entries
        .iter()
        .find(|e| e.action == "CreateBucket")
        .expect("CreateBucket activity entry");
    assert_eq!(create.actor.as_deref(), Some("cairn_admin"));
}

#[tokio::test]
async fn set_user_quota_is_accepted_and_gated() {
    let h = harness();
    let a = admin();
    let (id, _key) = create_member(&h, &a).await;

    // Set a quota.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            &format!("/users/{id}/quota"),
            &[],
            Some(&a),
            Bytes::from_static(br#"{"quota_bytes":1048576}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    // Clear it (null).
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            &format!("/users/{id}/quota"),
            &[],
            Some(&a),
            Bytes::from_static(br#"{"quota_bytes":null}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    // Unknown user -> 404.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/users/nobody/quota",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"quota_bytes":1}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);

    // Bad body -> 400.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            &format!("/users/{id}/quota"),
            &[],
            Some(&a),
            Bytes::from_static(b"not json"),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Non-admin -> 403.
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            &format!("/users/{id}/quota"),
            &[],
            Some(&m),
            Bytes::from_static(br#"{"quota_bytes":1}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn create_user_with_replication_policy_attaches_it() {
    let h = harness();
    let a = admin();
    make_bucket(&h, &a, "mirror").await;

    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/users",
            &[],
            Some(&a),
            Bytes::from_static(
                br#"{"display_name":"Replicator","role":"member","replication_policy_bucket":"mirror"}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let id = json(&resp)["id"].as_str().unwrap().to_owned();

    // The canned replication policy is attached and grants the replication actions on the bucket.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            &format!("/users/{id}/policy"),
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let actions: Vec<&str> = v["policy"]["Statement"][0]["Action"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect();
    assert!(actions.contains(&"s3:ReplicateObject"));
    assert!(actions.contains(&"s3:ReplicateDelete"));
    assert!(actions.contains(&"s3:GetObject"));
    assert!(actions.contains(&"s3:PutObject"));
    let resources: Vec<&str> = v["policy"]["Statement"][0]["Resource"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect();
    assert!(resources.contains(&"arn:aws:s3:::mirror/*"));

    // A bad destination bucket name -> 400 (and no user is half-provisioned).
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/users",
            &[],
            Some(&a),
            Bytes::from_static(
                br#"{"display_name":"X","role":"member","replication_policy_bucket":"A"}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn import_job_lifecycle_and_secret_never_echoed() {
    let h = harness();
    let a = admin();

    // Create an import job — the source secret is sealed and must never appear in any response.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/imports",
            &[],
            Some(&a),
            Bytes::from_static(
                br#"{"source_endpoint":"https://minio.example:9000","source_region":"us-east-1","access_key":"AKSRC","secret":"super-secret-import","buckets":[{"source":"photos","dest":"gallery"}],"workers":8}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let id = json(&resp)["id"].as_str().unwrap().to_owned();
    let leaked = |body: &[u8]| {
        body.windows(b"super-secret-import".len())
            .any(|w| w == b"super-secret-import")
    };
    assert!(!leaked(&resp.body), "create response leaked the secret");

    // List: secret-free, shows the source access key + pending state.
    let resp = h
        .svc
        .handle(&Method::GET, "/imports", &[], Some(&a), Bytes::new())
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let jobs = v["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["id"], id.as_str());
    assert_eq!(jobs[0]["access_key_id"], "AKSRC");
    assert_eq!(jobs[0]["state"], "pending");
    assert!(!leaked(&resp.body), "list response leaked the secret");

    // Detail: per-bucket mapping present (dest override honoured).
    let resp = h
        .svc
        .handle(
            &Method::GET,
            &format!("/imports/{id}"),
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let buckets = v["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["source_bucket"], "photos");
    assert_eq!(buckets[0]["dest_bucket"], "gallery");

    // Cancel → the job moves to cancelled.
    let resp = h
        .svc
        .handle(
            &Method::DELETE,
            &format!("/imports/{id}"),
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
            &format!("/imports/{id}"),
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(json(&resp)["state"], "cancelled");

    // An internal source endpoint is refused up front (SSRF guard).
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/imports",
            &[],
            Some(&a),
            Bytes::from_static(
                br#"{"source_endpoint":"http://169.254.169.254","source_region":"us-east-1","access_key":"AK","secret":"x","buckets":[]}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn probe_source_buckets_validates_and_guards() {
    // The "Fetch buckets" probe reuses the create-import connection validation: it rejects missing
    // fields, the CA-⊕-skip-verify conflict, and an internal endpoint (SSRF) — all before any
    // outbound dial — and never leaks the transient secret in the rejection body.
    let h = harness();
    let a = admin();
    let leaked = |body: &[u8]| body.windows(b"leak-me".len()).any(|w| w == b"leak-me");

    // Missing required fields → 400.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/imports/source/buckets",
            &[],
            Some(&a),
            Bytes::from_static(
                br#"{"source_endpoint":"","source_region":"us-east-1","access_key":"AK","secret":"leak-me"}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert!(!leaked(&resp.body), "probe validation leaked the secret");

    // CA certificate and skip-verify are mutually exclusive → 400.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/imports/source/buckets",
            &[],
            Some(&a),
            Bytes::from_static(
                br#"{"source_endpoint":"https://s3.example:9000","source_region":"us-east-1","access_key":"AK","secret":"leak-me","ca_cert":"-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----","insecure_skip_verify":true}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert!(
        !leaked(&resp.body),
        "probe conflict check leaked the secret"
    );

    // An internal endpoint literal is refused by the SSRF guard, before any dial.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/imports/source/buckets",
            &[],
            Some(&a),
            Bytes::from_static(
                br#"{"source_endpoint":"http://169.254.169.254","source_region":"us-east-1","access_key":"AK","secret":"leak-me"}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert!(
        !leaked(&resp.body),
        "probe SSRF rejection leaked the secret"
    );

    // The probe is admin-gated for free: an anonymous caller is refused.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/imports/source/buckets",
            &[],
            None,
            Bytes::from_static(
                br#"{"source_endpoint":"https://s3.example:9000","source_region":"us-east-1","access_key":"AK","secret":"leak-me"}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn replication_target_rejects_internal_endpoint() {
    // The SSRF guard (enforcing by default) must refuse a target whose endpoint is an internal IP
    // literal — otherwise it could be pointed at the node's own loopback or the cloud-metadata
    // service (ARCH 27). The secret must not leak in the rejection.
    let h = harness();
    let a = admin();
    make_bucket(&h, &a, "src").await;
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/buckets/src/replication/targets",
            &[],
            Some(&a),
            Bytes::from_static(
                br#"{"endpoint":"http://169.254.169.254","region":"us-east-1","dest_bucket":"mirror","access_key":"AK","secret":"sekret-cred"}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert!(
        !resp
            .body
            .windows(b"sekret-cred".len())
            .any(|w| w == b"sekret-cred")
    );
}

#[tokio::test]
async fn replication_target_add_list_hides_secret_and_delete_round_trip() {
    let h = harness();
    let a = admin();
    make_bucket(&h, &a, "src").await;

    // Add a target.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/buckets/src/replication/targets",
            &[],
            Some(&a),
            Bytes::from_static(
                br#"{"endpoint":"https://peer.example.com:9000","region":"us-west-2","dest_bucket":"mirror","access_key":"AKIDPEER","secret":"super-secret-key"}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let v = json(&resp);
    let arn = v["arn"].as_str().unwrap().to_owned();
    assert!(arn.starts_with("arn:cairn:replication:us-west-2:"));
    assert!(arn.ends_with(":mirror"));
    // The creation response must NOT echo the secret.
    assert!(
        !resp
            .body
            .windows(b"super-secret-key".len())
            .any(|w| w == b"super-secret-key")
    );

    // List shows the target WITHOUT any secret material.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/src/replication/targets",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let targets = v["targets"].as_array().unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0]["arn"], arn.as_str());
    assert_eq!(targets[0]["endpoint"], "https://peer.example.com:9000");
    assert_eq!(targets[0]["region"], "us-west-2");
    assert_eq!(targets[0]["dest_bucket"], "mirror");
    assert_eq!(targets[0]["access_key_id"], "AKIDPEER");
    // No secret field of any name leaks into the listing.
    assert!(targets[0].get("secret").is_none());
    assert!(targets[0].get("secret_ciphertext").is_none());
    assert!(
        !resp
            .body
            .windows(b"super-secret-key".len())
            .any(|w| w == b"super-secret-key")
    );

    // Delete by ARN -> 204, and the list is empty again.
    let resp = h
        .svc
        .handle(
            &Method::DELETE,
            &format!("/buckets/src/replication/targets/{arn}"),
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
            "/buckets/src/replication/targets",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert!(json(&resp)["targets"].as_array().unwrap().is_empty());

    // Deleting an unknown ARN -> 404.
    let resp = h
        .svc
        .handle(
            &Method::DELETE,
            "/buckets/src/replication/targets/arn:cairn:replication:x:y:z",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);

    // Missing bucket -> 404; non-admin -> 403.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/nope/replication/targets",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/buckets/src/replication/targets",
            &[],
            Some(&m),
            Bytes::from_static(
                br#"{"endpoint":"e","region":"r","dest_bucket":"d","access_key":"k","secret":"s"}"#,
            ),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn replication_retry_endpoint_requeues_failed_for_bucket() {
    let h = harness();
    let a = admin();

    // Plant a terminally-failed outbox entry for bucket "repl".
    let bucket = BucketName::parse("repl").unwrap();
    let key = ObjectKey::parse("photo.jpg").unwrap();
    let version = VersionId::from_string("00000001".to_owned());
    let entry = cairn_types::meta::OutboxEntry {
        enqueued_at: cairn_types::time::Timestamp(0),
        id: "outbox-1".to_owned(),
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version.clone(),
        operation: cairn_types::meta::ReplicationOp::ObjectCreate,
        rule_id: "rule-1".to_owned(),
        target_arn: None,
        attempts: 0,
        next_attempt_at: cairn_types::time::Timestamp(0),
        status: cairn_types::meta::ReplicationStatus::Pending,
        last_error: None,
        priority: 0,
        lease_until: None,
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
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
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
        sse_descriptor: None,
        replication_status: Some(cairn_types::meta::ReplicationStatus::Pending),
        created_at: now,
        updated_at: now,
    };
    h.meta
        .submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: vec![entry],
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
    // The bucket row must exist for require_bucket to pass.
    make_bucket(&h, &a, "repl").await;

    // Status reflects the one failed entry with its error.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/repl/replication/status",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert_eq!(v["bucket"], "repl");
    assert_eq!(v["failed"], 1);
    assert_eq!(v["recent_errors"][0]["key"], "photo.jpg");
    assert_eq!(v["recent_errors"][0]["error"], "destination unreachable");

    // Retry requeues it and reports the observed failed count.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/buckets/repl/replication/retry",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    assert_eq!(v["requeued"], true);
    assert_eq!(v["failed_observed"], 1);

    // After the requeue, the failed list is empty (the entry is pending again).
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/repl/replication/status",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(json(&resp)["failed"], 0);

    // Non-admin -> 403.
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/buckets/repl/replication/retry",
            &[],
            Some(&m),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
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

#[tokio::test]
async fn user_policy_routes_validate_store_and_surface() {
    let h = harness();
    let a = admin();

    // Create a member, capture its id.
    let resp = h
        .svc
        .handle(
            &Method::POST,
            "/users",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"display_name":"Bob","role":"member"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::CREATED);
    let id = json(&resp)["id"].as_str().unwrap().to_owned();

    // No policy initially: detail surfaces null.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            &format!("/users/{id}"),
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    assert!(json(&resp)["policy"].is_null());

    // Malformed policy JSON is rejected at the edge.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            &format!("/users/{id}/policy"),
            &[],
            Some(&a),
            Bytes::from_static(b"{ not json"),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // A valid Principal-less identity policy is stored and surfaced.
    let doc = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#;
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            &format!("/users/{id}/policy"),
            &[],
            Some(&a),
            Bytes::from_static(doc),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    let resp = h
        .svc
        .handle(
            &Method::GET,
            &format!("/users/{id}/policy"),
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(
        json(&resp)["policy"]["Statement"][0]["Action"],
        "s3:GetObject"
    );
    let resp = h
        .svc
        .handle(
            &Method::GET,
            &format!("/users/{id}"),
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(json(&resp)["policy"]["Statement"][0]["Effect"], "Allow");

    // Detaching clears it.
    let resp = h
        .svc
        .handle(
            &Method::DELETE,
            &format!("/users/{id}/policy"),
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
            &format!("/users/{id}/policy"),
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert!(json(&resp)["policy"].is_null());

    // A non-admin (member) is forbidden from the policy routes.
    let m = member();
    let resp = h
        .svc
        .handle(
            &Method::GET,
            &format!("/users/{id}/policy"),
            &[],
            Some(&m),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);

    // An unknown user is a 404.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/users/nope/policy",
            &[],
            Some(&a),
            Bytes::from_static(doc),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn bucket_encryption_default_round_trips() {
    let h = harness();
    let a = admin();
    h.svc
        .handle(
            &Method::POST,
            "/buckets",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"name":"enc"}"#),
        )
        .await;

    // Turn the default on.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/enc/encryption",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"algorithm":"AES256"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/enc/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    let v = json(&resp);
    assert_eq!(v["encryption"]["algorithm"], "AES256");

    // Unknown algorithm -> 400.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/enc/encryption",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"algorithm":"ROT13"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Turn it off again.
    let resp = h
        .svc
        .handle(
            &Method::PUT,
            "/buckets/enc/encryption",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"algorithm":"none"}"#),
        )
        .await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/enc/config",
            &[],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert!(json(&resp)["encryption"].is_null());
}

#[tokio::test]
async fn list_objects_with_delimiter_folds_folders() {
    let h = harness();
    let a = admin();
    h.svc
        .handle(
            &Method::POST,
            "/buckets",
            &[],
            Some(&a),
            Bytes::from_static(br#"{"name":"tree"}"#),
        )
        .await;
    put_object(&h, "tree", "docs/a.txt", b"a").await;
    put_object(&h, "tree", "docs/b.txt", b"b").await;
    put_object(&h, "tree", "img/c.png", b"c").await;
    put_object(&h, "tree", "root.txt", b"r").await;

    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/tree/objects",
            &[("delimiter".to_owned(), "/".to_owned())],
            Some(&a),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status, StatusCode::OK);
    let v = json(&resp);
    let prefixes: Vec<&str> = v["common_prefixes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap())
        .collect();
    assert_eq!(prefixes, vec!["docs/", "img/"]);
    let keys: Vec<&str> = v["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["root.txt"]);

    // Drilling into a folder: prefix + delimiter lists only that level.
    let resp = h
        .svc
        .handle(
            &Method::GET,
            "/buckets/tree/objects",
            &[
                ("prefix".to_owned(), "docs/".to_owned()),
                ("delimiter".to_owned(), "/".to_owned()),
            ],
            Some(&a),
            Bytes::new(),
        )
        .await;
    let v = json(&resp);
    let keys: Vec<&str> = v["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["docs/a.txt", "docs/b.txt"]);
    assert!(v["common_prefixes"].as_array().unwrap().is_empty());
}
