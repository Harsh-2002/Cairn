//! The control service: routing and the endpoint handlers of the management API.
//!
//! [`ControlService::handle`] takes the part of the path after `/api/v1` and dispatches to the
//! contract endpoints. Every endpoint except `/health` requires an administrator principal;
//! members and anonymous callers receive `403 { "error": "forbidden" }`.

use crate::wire;
use bytes::Bytes;
use cairn_types::auth::{Principal, Role};
use cairn_types::bucket::{
    Bucket, CompressionAlgorithm, CompressionPolicy, ConfigAspect, ConfigDoc, VersioningState,
};
use cairn_types::id::{BucketName, UserId};
use cairn_types::meta::{ListQuery, Mutation, MutationOutcome, StoreCounts, User, UserRecord};
use cairn_types::traits::{BlobStore, Clock, Crypto, MetadataStore};
use http::{Method, StatusCode};
use serde::Serialize;
use std::sync::Arc;

/// The bound on a single management-API listing page, and the batch size used when paging a
/// bucket for force-delete or per-bucket aggregation. Keeps every loop bounded in memory.
const PAGE_LIMIT: u32 = 1000;

/// The hard upper bound on iterations of any internal paging loop (force-delete, per-bucket
/// aggregation), so a hostile or corrupt cursor can never spin forever.
const MAX_PAGES: u32 = 100_000;

/// A management-API response: an HTTP status and a JSON body. The caller sets
/// `content-type: application/json`.
#[derive(Debug, Clone)]
pub struct ControlResponse {
    /// The HTTP status code.
    pub status: StatusCode,
    /// The JSON-encoded body.
    pub body: Vec<u8>,
}

impl ControlResponse {
    /// A response with a serialized JSON body at `status`. Serialization is infallible for the
    /// crate's own DTOs; a failure degrades to a `500` error envelope rather than panicking.
    fn json<T: Serialize>(status: StatusCode, value: &T) -> Self {
        match serde_json::to_vec(value) {
            Ok(body) => Self { status, body },
            Err(e) => Self::error_internal(&e.to_string()),
        }
    }

    /// An error envelope `{ "error": <message> }` at `status`.
    fn error(status: StatusCode, message: &str) -> Self {
        let body = serde_json::to_vec(&wire::ErrorResp {
            error: message.to_owned(),
        })
        .unwrap_or_else(|_| br#"{"error":"internal error"}"#.to_vec());
        Self { status, body }
    }

    fn forbidden() -> Self {
        Self::error(StatusCode::FORBIDDEN, "forbidden")
    }

    fn not_found() -> Self {
        Self::error(StatusCode::NOT_FOUND, "not found")
    }

    fn bad_request(message: &str) -> Self {
        Self::error(StatusCode::BAD_REQUEST, message)
    }

    fn error_internal(message: &str) -> Self {
        Self::error(StatusCode::INTERNAL_SERVER_ERROR, message)
    }

    /// An empty `204 No Content` response.
    fn no_content() -> Self {
        Self {
            status: StatusCode::NO_CONTENT,
            body: Vec::new(),
        }
    }
}

/// The JSON management API service. It owns shared handles to the trait spine and is otherwise
/// stateless; clone the `Arc`s freely.
#[derive(Clone)]
pub struct ControlService {
    meta: Arc<dyn MetadataStore>,
    blob: Arc<dyn BlobStore>,
    crypto: Arc<dyn Crypto>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for ControlService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlService").finish_non_exhaustive()
    }
}

impl ControlService {
    /// Construct a service over the given backends.
    #[must_use]
    pub fn new(
        meta: Arc<dyn MetadataStore>,
        blob: Arc<dyn BlobStore>,
        crypto: Arc<dyn Crypto>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            meta,
            blob,
            crypto,
            clock,
        }
    }

    /// Route a management-API request. `subpath` is the path *after* `/api/v1` (e.g.
    /// `"/overview"`, `"/buckets"`, `"/buckets/photos"`, `"/buckets/photos/objects"`).
    ///
    /// Every endpoint except `/health` requires `principal.role == Administrator`; otherwise a
    /// `403` is returned. Unknown paths yield `404`.
    pub async fn handle(
        &self,
        method: &Method,
        subpath: &str,
        query: &[(String, String)],
        principal: Option<&Principal>,
        body: Bytes,
    ) -> ControlResponse {
        let segments = split_path(subpath);

        // /health is the only unauthenticated endpoint.
        if matches!(segments.as_slice(), ["health"]) {
            return self.health(method);
        }

        // Everything else is admin-gated.
        if !is_admin(principal) {
            return ControlResponse::forbidden();
        }

        match (method, segments.as_slice()) {
            (&Method::GET, ["overview"]) => self.overview().await,

            (&Method::GET, ["buckets"]) => self.list_buckets().await,
            (&Method::POST, ["buckets"]) => self.create_bucket(&body).await,
            (&Method::GET, ["buckets", name]) => self.bucket_detail(name).await,
            (&Method::DELETE, ["buckets", name]) => self.delete_bucket(name).await,
            (&Method::GET, ["buckets", name, "objects"]) => self.list_objects(name, query).await,

            (&Method::GET, ["buckets", name, "config"]) => self.bucket_config(name).await,
            (&Method::PUT, ["buckets", name, "versioning"]) => {
                self.set_versioning(name, &body).await
            }
            (&Method::PUT, ["buckets", name, "quota"]) => self.set_quota(name, &body).await,
            (&Method::PUT, ["buckets", name, "compression"]) => {
                self.set_compression(name, &body).await
            }
            (&Method::PUT, ["buckets", name, "policy"]) => self.set_policy(name, &body).await,
            (&Method::DELETE, ["buckets", name, "policy"]) => self.delete_policy(name).await,

            (&Method::GET, ["users"]) => self.list_users().await,
            (&Method::POST, ["users"]) => self.create_user(&body).await,
            (&Method::PATCH, ["users", id]) => self.patch_user(id, &body).await,
            (&Method::POST, ["users", id, "rotate-credentials"]) => {
                self.rotate_credentials(id).await
            }

            (&Method::GET, ["replication", "failed"]) => self.failed_replication(query).await,

            (&Method::GET, ["activity"]) => self.activity(query).await,

            _ => ControlResponse::not_found(),
        }
    }

    // -----------------------------------------------------------------------------------
    // Health & overview
    // -----------------------------------------------------------------------------------

    fn health(&self, method: &Method) -> ControlResponse {
        if method != Method::GET {
            return ControlResponse::not_found();
        }
        ControlResponse::json(
            StatusCode::OK,
            &wire::HealthResp {
                status: "ok",
                ready: true,
            },
        )
    }

    async fn overview(&self) -> ControlResponse {
        let counts = match self.meta.aggregate_counts().await {
            Ok(c) => c,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        ControlResponse::json(
            StatusCode::OK,
            &wire::OverviewResp {
                buckets: counts.buckets,
                objects: counts.objects,
                versions: counts.versions,
                logical_bytes: counts.logical_bytes,
                physical_bytes: counts.physical_bytes,
                compression_ratio: compression_ratio(&counts),
            },
        )
    }

    // -----------------------------------------------------------------------------------
    // Buckets
    // -----------------------------------------------------------------------------------

    async fn list_buckets(&self) -> ControlResponse {
        let buckets = match self.meta.list_buckets(None).await {
            Ok(b) => b,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        let entries = buckets
            .into_iter()
            .map(|b| wire::BucketListEntry {
                name: b.name.as_str().to_owned(),
                owner_id: b.owner_id.to_string(),
                created_at_ms: b.created_at.as_millis(),
                versioning: wire::versioning_str(b.versioning),
            })
            .collect();
        ControlResponse::json(StatusCode::OK, &wire::BucketListResp { buckets: entries })
    }

    async fn create_bucket(&self, body: &Bytes) -> ControlResponse {
        let req: wire::CreateBucketReq = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };
        let name = match BucketName::parse(&req.name) {
            Ok(n) => n,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };

        // Distinguish "already exists" (409) from a creation attempt up front: the store
        // returns a generic Conflict that we surface as 409 per the contract.
        if matches!(self.meta.get_bucket(&name).await, Ok(Some(_))) {
            return ControlResponse::error(StatusCode::CONFLICT, "bucket already exists");
        }

        let owner_id = UserId("admin".to_owned());
        let bucket = Bucket {
            name: name.clone(),
            owner_id,
            created_at: self.clock.now(),
            versioning: VersioningState::Unversioned,
            ownership_mode: cairn_types::authz::OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".to_owned(),
            compression: None,
        };

        match self
            .meta
            .submit(Mutation::CreateBucket(Box::new(bucket)))
            .await
        {
            Ok(MutationOutcome::Ack) => {}
            Ok(_) => return ControlResponse::error_internal("unexpected create-bucket outcome"),
            Err(cairn_types::error::MetaError::Conflict) => {
                return ControlResponse::error(StatusCode::CONFLICT, "bucket already exists");
            }
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        }

        self.record_activity("CreateBucket", Some(name.as_str()), None)
            .await;

        ControlResponse::json(
            StatusCode::CREATED,
            &wire::CreateBucketResp {
                name: name.as_str().to_owned(),
            },
        )
    }

    async fn bucket_detail(&self, name: &str) -> ControlResponse {
        let bucket_name = match BucketName::parse(name) {
            Ok(n) => n,
            Err(_) => return ControlResponse::not_found(),
        };
        let bucket = match self.meta.get_bucket(&bucket_name).await {
            Ok(Some(b)) => b,
            Ok(None) => return ControlResponse::not_found(),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };

        let (object_count, logical_bytes) = match self.bucket_current_totals(&bucket_name).await {
            Ok(t) => t,
            Err(e) => return ControlResponse::error_internal(&e),
        };

        ControlResponse::json(
            StatusCode::OK,
            &wire::BucketDetailResp {
                name: bucket.name.as_str().to_owned(),
                versioning: wire::versioning_str(bucket.versioning),
                ownership_mode: wire::ownership_str(bucket.ownership_mode),
                region: bucket.region,
                object_count,
                logical_bytes,
                compression: bucket.compression.as_ref().map(|p| {
                    match p.algorithm {
                        CompressionAlgorithm::Zstd => "zstd",
                        CompressionAlgorithm::Lz4 => "lz4",
                        CompressionAlgorithm::None => "none",
                    }
                    .to_owned()
                }),
            },
        )
    }

    /// Page the current objects of a bucket with a bounded loop, summing count and logical
    /// bytes. Bounded by [`MAX_PAGES`] iterations.
    async fn bucket_current_totals(&self, bucket: &BucketName) -> Result<(u64, u64), String> {
        let mut object_count = 0u64;
        let mut logical_bytes = 0u64;
        let mut cursor: Option<String> = None;

        for _ in 0..MAX_PAGES {
            let query = ListQuery {
                cursor: cursor.clone(),
                limit: PAGE_LIMIT,
                ..Default::default()
            };
            let page = self
                .meta
                .list_current(bucket, &query)
                .await
                .map_err(|e| e.to_string())?;
            for item in &page.items {
                object_count += 1;
                logical_bytes += item.size;
            }
            if page.truncated {
                match page.next_cursor {
                    Some(c) => cursor = Some(c),
                    None => break,
                }
            } else {
                break;
            }
        }
        Ok((object_count, logical_bytes))
    }

    async fn delete_bucket(&self, name: &str) -> ControlResponse {
        let bucket_name = match BucketName::parse(name) {
            Ok(n) => n,
            Err(_) => return ControlResponse::not_found(),
        };
        match self.meta.get_bucket(&bucket_name).await {
            Ok(Some(_)) => {}
            Ok(None) => return ControlResponse::not_found(),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        }

        // Force-empty: page every version (and delete marker), permanently delete each,
        // reclaiming any referenced blob. We re-list from the start each round because each
        // deletion removes rows the cursor was anchored against.
        for _ in 0..MAX_PAGES {
            let query = ListQuery {
                limit: PAGE_LIMIT,
                ..Default::default()
            };
            let page = match self.meta.list_versions(&bucket_name, &query).await {
                Ok(p) => p,
                Err(e) => return ControlResponse::error_internal(&e.to_string()),
            };
            if page.items.is_empty() {
                break;
            }
            for item in &page.items {
                let key = item.key.clone();
                let version_id = item.version_id.clone();
                match self
                    .meta
                    .submit(Mutation::DeleteVersion {
                        bucket: bucket_name.clone(),
                        key,
                        version_id,
                    })
                    .await
                {
                    Ok(MutationOutcome::Deleted { freed, .. }) => {
                        if let Some(path) = freed {
                            // Blob reclamation is best-effort and idempotent.
                            let _ = self.blob.delete(&path).await;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => return ControlResponse::error_internal(&e.to_string()),
                }
            }
        }

        if let Err(e) = self
            .meta
            .submit(Mutation::DeleteBucket(bucket_name.clone()))
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity("DeleteBucket", Some(bucket_name.as_str()), None)
            .await;

        ControlResponse::no_content()
    }

    async fn list_objects(&self, name: &str, query: &[(String, String)]) -> ControlResponse {
        let bucket_name = match BucketName::parse(name) {
            Ok(n) => n,
            Err(_) => return ControlResponse::not_found(),
        };
        match self.meta.get_bucket(&bucket_name).await {
            Ok(Some(_)) => {}
            Ok(None) => return ControlResponse::not_found(),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        }

        let prefix = find_query(query, "prefix").map(str::to_owned);
        let limit = find_query(query, "limit")
            .and_then(|v| v.parse::<u32>().ok())
            .map_or(PAGE_LIMIT, |v| v.clamp(1, PAGE_LIMIT));
        let cursor = find_query(query, "cursor").map(str::to_owned);

        let list_query = ListQuery {
            prefix,
            cursor,
            limit,
            ..Default::default()
        };
        let page = match self.meta.list_current(&bucket_name, &list_query).await {
            Ok(p) => p,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };

        let objects = page
            .items
            .into_iter()
            .map(|o| wire::ObjectEntry {
                key: o.key.as_str().to_owned(),
                size: o.size,
                etag: o.etag.as_str().to_owned(),
                last_modified_ms: o.last_modified.as_millis(),
            })
            .collect();

        ControlResponse::json(
            StatusCode::OK,
            &wire::ObjectListResp {
                objects,
                next: page.next_cursor,
            },
        )
    }

    // -----------------------------------------------------------------------------------
    // Bucket configuration (ARCH §22.2)
    // -----------------------------------------------------------------------------------

    /// `GET /buckets/{name}/config`: the bucket's versioning + ownership state alongside each
    /// configuration aspect document (parsed back to JSON, or `null` when unset).
    async fn bucket_config(&self, name: &str) -> ControlResponse {
        let bucket_name = match BucketName::parse(name) {
            Ok(n) => n,
            Err(_) => return ControlResponse::not_found(),
        };
        let bucket = match self.meta.get_bucket(&bucket_name).await {
            Ok(Some(b)) => b,
            Ok(None) => return ControlResponse::not_found(),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };

        // Each aspect is an opaque stored document; surface it as parsed JSON so the UI can
        // render it, falling back to a JSON string if it is not itself JSON.
        let (policy, cors, tagging, lifecycle, acl, public_access_block) =
            match self.read_aspects(&bucket_name).await {
                Ok(aspects) => aspects,
                Err(e) => return ControlResponse::error_internal(&e),
            };

        // The byte quota is enforced inside the writer's commit transaction; the dedicated
        // reader surfaces the configured `buckets.quota_bytes` value (null when unlimited).
        let quota_bytes = match self.meta.get_bucket_quota(&bucket_name).await {
            Ok(q) => q,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };

        ControlResponse::json(
            StatusCode::OK,
            &wire::BucketConfigResp {
                versioning: wire::versioning_str(bucket.versioning),
                ownership_mode: wire::ownership_str(bucket.ownership_mode),
                quota_bytes,
                policy,
                cors,
                tagging,
                lifecycle,
                acl,
                public_access_block,
            },
        )
    }

    /// Read the six exposed config aspects of a bucket, each rendered as JSON (or `None` when
    /// unset). Returns the documents in the response's declared order, or an error string on a
    /// store failure.
    #[allow(clippy::type_complexity)]
    async fn read_aspects(
        &self,
        bucket: &BucketName,
    ) -> Result<
        (
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
        ),
        String,
    > {
        let read = |aspect: ConfigAspect| async move {
            match self.meta.get_bucket_config(bucket, aspect).await {
                Ok(Some(doc)) => Ok(Some(config_doc_to_json(&doc))),
                Ok(None) => Ok(None),
                Err(e) => Err(e.to_string()),
            }
        };
        Ok((
            read(ConfigAspect::Policy).await?,
            read(ConfigAspect::Cors).await?,
            read(ConfigAspect::Tagging).await?,
            read(ConfigAspect::Lifecycle).await?,
            read(ConfigAspect::Acl).await?,
            read(ConfigAspect::PublicAccessBlock).await?,
        ))
    }

    /// `PUT /buckets/{name}/versioning`: set the bucket's versioning state.
    async fn set_versioning(&self, name: &str, body: &Bytes) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        let req: wire::SetVersioningReq = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };
        let state = match wire::parse_versioning(&req.status) {
            Some(s) => s,
            None => {
                return ControlResponse::bad_request(
                    "status must be Enabled, Suspended, or Unversioned",
                );
            }
        };

        if let Err(e) = self
            .meta
            .submit(Mutation::SetVersioning {
                bucket: bucket_name.clone(),
                state,
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity("SetVersioning", Some(bucket_name.as_str()), None)
            .await;
        ControlResponse::no_content()
    }

    /// `PUT /buckets/{name}/quota`: set or clear the bucket's byte quota.
    async fn set_quota(&self, name: &str, body: &Bytes) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        let req: wire::SetQuotaReq = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };

        if let Err(e) = self
            .meta
            .submit(Mutation::SetBucketQuota {
                bucket: bucket_name.clone(),
                quota_bytes: req.quota_bytes,
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity("SetBucketQuota", Some(bucket_name.as_str()), None)
            .await;
        ControlResponse::no_content()
    }

    /// `PUT /buckets/{name}/compression`: set or disable the bucket's compression policy, applied to
    /// subsequent object writes. Body: `{"algorithm": "zstd"|"lz4"|"none", "block_size": 65536}`.
    async fn set_compression(&self, name: &str, body: &Bytes) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        #[derive(serde::Deserialize)]
        struct Req {
            algorithm: String,
            #[serde(default)]
            block_size: Option<u32>,
        }
        let req: Req = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };
        let block_size = req.block_size.unwrap_or(65_536);
        let policy = match req.algorithm.to_ascii_lowercase().as_str() {
            "none" | "off" | "" => None,
            "zstd" => Some(CompressionPolicy {
                algorithm: CompressionAlgorithm::Zstd,
                block_size,
            }),
            "lz4" => Some(CompressionPolicy {
                algorithm: CompressionAlgorithm::Lz4,
                block_size,
            }),
            other => {
                return ControlResponse::bad_request(&format!(
                    "unknown algorithm {other:?} (expected zstd|lz4|none)"
                ));
            }
        };
        if policy.is_some() && !(1024..=16 * 1024 * 1024).contains(&block_size) {
            return ControlResponse::bad_request("block_size must be between 1 KiB and 16 MiB");
        }

        if let Err(e) = self
            .meta
            .submit(Mutation::SetBucketCompression {
                bucket: bucket_name.clone(),
                policy,
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity("SetBucketCompression", Some(bucket_name.as_str()), None)
            .await;
        ControlResponse::no_content()
    }

    /// `PUT /buckets/{name}/policy`: validate the raw policy JSON via `cairn_authz::parse_policy`
    /// and store it as the bucket's `Policy` config aspect.
    async fn set_policy(&self, name: &str, body: &Bytes) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };

        // The body is the raw policy JSON document. Validate it before storing so a malformed
        // policy is rejected at the edge rather than failing open later (ARCH §15.5).
        let policy_json = match std::str::from_utf8(body) {
            Ok(s) => s,
            Err(_) => return ControlResponse::bad_request("policy must be valid UTF-8 JSON"),
        };
        if let Err(e) = cairn_authz::parse_policy(policy_json) {
            return ControlResponse::bad_request(&format!("invalid policy: {e}"));
        }

        if let Err(e) = self
            .meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket_name.clone(),
                aspect: ConfigAspect::Policy,
                doc: Some(ConfigDoc(policy_json.to_owned())),
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity("PutBucketPolicy", Some(bucket_name.as_str()), None)
            .await;
        ControlResponse::no_content()
    }

    /// `DELETE /buckets/{name}/policy`: clear the bucket's policy.
    async fn delete_policy(&self, name: &str) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };

        if let Err(e) = self
            .meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket_name.clone(),
                aspect: ConfigAspect::Policy,
                doc: None,
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity("DeleteBucketPolicy", Some(bucket_name.as_str()), None)
            .await;
        ControlResponse::no_content()
    }

    /// Parse a bucket name and confirm the bucket exists, mapping the two failure modes to the
    /// shared `404` responses used across the config endpoints.
    async fn require_bucket(&self, name: &str) -> Result<BucketName, ControlResponse> {
        let bucket_name = BucketName::parse(name).map_err(|_| ControlResponse::not_found())?;
        match self.meta.get_bucket(&bucket_name).await {
            Ok(Some(_)) => Ok(bucket_name),
            Ok(None) => Err(ControlResponse::not_found()),
            Err(e) => Err(ControlResponse::error_internal(&e.to_string())),
        }
    }

    // -----------------------------------------------------------------------------------
    // Users
    // -----------------------------------------------------------------------------------

    async fn list_users(&self) -> ControlResponse {
        let users = match self.meta.list_users().await {
            Ok(u) => u,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        let entries = users
            .into_iter()
            .map(|u: User| wire::UserListEntry {
                id: u.id.to_string(),
                display_name: u.display_name,
                access_key_id: u.access_key_id,
                role: wire::role_str(u.role),
                is_active: u.is_active,
            })
            .collect();
        ControlResponse::json(StatusCode::OK, &wire::UserListResp { users: entries })
    }

    async fn create_user(&self, body: &Bytes) -> ControlResponse {
        let req: wire::CreateUserReq = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };
        let role = match wire::parse_role(&req.role) {
            Some(r) => r,
            None => return ControlResponse::bad_request("role must be administrator or member"),
        };
        if req.display_name.trim().is_empty() {
            return ControlResponse::bad_request("display_name must not be empty");
        }

        // Mint the Bearer credential. The access-key id is public; only the hash of the secret
        // is persisted, so the plaintext secret is returned exactly once below.
        let user_id = UserId::generate();
        let access_key_id = format!("cairn_{}", uuid::Uuid::new_v4().simple());
        let secret = generate_secret();
        let bearer_secret_hash = cairn_auth::hash_bearer_secret(&secret);
        let now = self.clock.now();

        // Also provision a SigV4 access-key pair so the user can use the S3 surface. The
        // SigV4 secret is envelope-encrypted at rest via the crypto facility (the SigV4
        // authenticator decrypts it transiently); it is never returned over the wire.
        let sigv4_access_key_id = format!(
            "CAIRN{}",
            uuid::Uuid::new_v4().simple().to_string().to_uppercase()
        );
        let sigv4_secret = generate_secret();
        let (sigv4_secret_ciphertext, sigv4_secret_nonce) =
            match self.crypto.seal(sigv4_secret.as_bytes()) {
                Ok(sealed) => (Some(sealed.ciphertext), Some(sealed.nonce.0)),
                Err(e) => return ControlResponse::error_internal(&e.to_string()),
            };

        let record = UserRecord {
            user: User {
                id: user_id.clone(),
                display_name: req.display_name,
                access_key_id: access_key_id.clone(),
                sigv4_access_key_id: Some(sigv4_access_key_id),
                role,
                is_active: true,
                created_at: now,
                updated_at: now,
            },
            bearer_secret_hash,
            sigv4_secret_ciphertext,
            sigv4_secret_nonce,
        };

        match self
            .meta
            .submit(Mutation::CreateUser(Box::new(record)))
            .await
        {
            Ok(MutationOutcome::UserCreated(_)) | Ok(MutationOutcome::Ack) => {}
            Ok(_) => return ControlResponse::error_internal("unexpected create-user outcome"),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        }

        self.record_activity("CreateUser", None, None).await;

        ControlResponse::json(
            StatusCode::CREATED,
            &wire::CreateUserResp {
                id: user_id.to_string(),
                bearer_access_key_id: access_key_id,
                bearer_secret: secret,
            },
        )
    }

    /// `PATCH /users/{id}`: update a user's mutable fields (activation and/or role). Absent
    /// fields are left unchanged. Returns the updated public user view.
    async fn patch_user(&self, id: &str, body: &Bytes) -> ControlResponse {
        let req: wire::PatchUserReq = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };
        let new_role = match req.role.as_deref().map(wire::parse_role) {
            Some(Some(r)) => Some(r),
            Some(None) => {
                return ControlResponse::bad_request("role must be administrator or member");
            }
            None => None,
        };
        if req.is_active.is_none() && new_role.is_none() {
            return ControlResponse::bad_request("nothing to update");
        }

        let user_id = UserId(id.to_owned());
        // Load the current full record so the unchanged fields (display name, credentials,
        // SigV4 material) survive the update intact.
        let mut record = match self.load_user_record(&user_id).await {
            Ok(Some(r)) => r,
            Ok(None) => return ControlResponse::not_found(),
            Err(resp) => return resp,
        };

        if let Some(role) = new_role {
            record.user.role = role;
        }
        if let Some(active) = req.is_active {
            record.user.is_active = active;
        }
        record.user.updated_at = self.clock.now();

        // A pure deactivation (no role change) goes through the dedicated mutation; any field
        // that the record-preserving path must carry goes through UpdateUser.
        if new_role.is_none() && req.is_active == Some(false) {
            if let Err(e) = self
                .meta
                .submit(Mutation::DeactivateUser(user_id.clone()))
                .await
            {
                return ControlResponse::error_internal(&e.to_string());
            }
        } else if let Err(e) = self
            .meta
            .submit(Mutation::UpdateUser(Box::new(record.clone())))
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity("UpdateUser", None, None).await;

        ControlResponse::json(
            StatusCode::OK,
            &wire::PatchUserResp {
                id: record.user.id.to_string(),
                display_name: record.user.display_name,
                access_key_id: record.user.access_key_id,
                role: wire::role_str(record.user.role),
                is_active: record.user.is_active,
            },
        )
    }

    /// `POST /users/{id}/rotate-credentials`: mint a fresh Bearer secret for the existing user,
    /// persist only its hash, and return the plaintext exactly once.
    async fn rotate_credentials(&self, id: &str) -> ControlResponse {
        let user_id = UserId(id.to_owned());
        let mut record = match self.load_user_record(&user_id).await {
            Ok(Some(r)) => r,
            Ok(None) => return ControlResponse::not_found(),
            Err(resp) => return resp,
        };

        let secret = generate_secret();
        record.bearer_secret_hash = cairn_auth::hash_bearer_secret(&secret);
        record.user.updated_at = self.clock.now();
        let access_key_id = record.user.access_key_id.clone();

        if let Err(e) = self
            .meta
            .submit(Mutation::UpdateUser(Box::new(record)))
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity("RotateCredentials", None, None).await;

        ControlResponse::json(
            StatusCode::OK,
            &wire::RotateCredentialsResp {
                bearer_access_key_id: access_key_id,
                bearer_secret: secret,
            },
        )
    }

    /// Reconstruct a full [`UserRecord`] for `id` from the read surface the trait spine exposes:
    /// the public [`User`] fields come from the user listing, the Bearer secret hash from the
    /// Bearer-key lookup, and any SigV4 secret material from the SigV4-key lookup. The trait has
    /// no by-id record reader, so this is the faithful reconstruction the update path needs.
    ///
    /// Returns `Ok(None)` when no such user exists, and an error [`ControlResponse`] on a store
    /// failure.
    async fn load_user_record(&self, id: &UserId) -> Result<Option<UserRecord>, ControlResponse> {
        let users = self
            .meta
            .list_users()
            .await
            .map_err(|e| ControlResponse::error_internal(&e.to_string()))?;
        let Some(user) = users.into_iter().find(|u: &User| &u.id == id) else {
            return Ok(None);
        };

        let bearer = self
            .meta
            .user_by_bearer_key(&user.access_key_id)
            .await
            .map_err(|e| ControlResponse::error_internal(&e.to_string()))?;
        let Some(bearer) = bearer else {
            // The user is listed but its credential row is missing; treat as not found rather
            // than fabricating a hash.
            return Ok(None);
        };

        let (sigv4_secret_ciphertext, sigv4_secret_nonce) =
            match user.sigv4_access_key_id.as_deref() {
                Some(key) => {
                    let creds = self
                        .meta
                        .user_by_sigv4_key(key)
                        .await
                        .map_err(|e| ControlResponse::error_internal(&e.to_string()))?;
                    match creds {
                        Some(c) => (Some(c.secret_ciphertext), Some(c.secret_nonce)),
                        None => (None, None),
                    }
                }
                None => (None, None),
            };

        Ok(Some(UserRecord {
            user,
            bearer_secret_hash: bearer.secret_hash,
            sigv4_secret_ciphertext,
            sigv4_secret_nonce,
        }))
    }

    // -----------------------------------------------------------------------------------
    // Replication operations (ARCH §22.2)
    // -----------------------------------------------------------------------------------

    /// `GET /replication/failed`: list outbox entries the engine has marked terminal/failed,
    /// most recently due first, bounded by `?limit=` (default and ceiling [`PAGE_LIMIT`]).
    async fn failed_replication(&self, query: &[(String, String)]) -> ControlResponse {
        let limit = find_query(query, "limit")
            .and_then(|v| v.parse::<u32>().ok())
            .map_or(PAGE_LIMIT, |v| v.clamp(1, PAGE_LIMIT));

        let entries = match self.meta.list_failed_replication(limit).await {
            Ok(e) => e,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        let entries = entries
            .into_iter()
            .map(|e| wire::FailedReplicationEntry {
                bucket: e.bucket.as_str().to_owned(),
                key: e.key.as_str().to_owned(),
                version_id: e.version_id.as_str().to_owned(),
                error: e.last_error,
                attempts: e.attempts,
                next_attempt_at_ms: e.next_attempt_at.as_millis(),
            })
            .collect();

        ControlResponse::json(StatusCode::OK, &wire::FailedReplicationResp { entries })
    }

    // -----------------------------------------------------------------------------------
    // Activity
    // -----------------------------------------------------------------------------------

    async fn activity(&self, query: &[(String, String)]) -> ControlResponse {
        let limit = find_query(query, "limit")
            .and_then(|v| v.parse::<u32>().ok())
            .map_or(PAGE_LIMIT, |v| v.clamp(1, PAGE_LIMIT));
        let entries = match self.meta.list_activity(limit).await {
            Ok(e) => e,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        let list = entries
            .into_iter()
            .map(|e| wire::ActivityListEntry {
                action: e.action,
                bucket: e.bucket,
                key: e.key,
                at_ms: e.at.as_millis(),
            })
            .collect();
        ControlResponse::json(StatusCode::OK, &wire::ActivityListResp { entries: list })
    }

    // -----------------------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------------------

    /// Append an audit/activity entry for a mutating endpoint (best-effort).
    async fn record_activity(&self, action: &str, bucket: Option<&str>, key: Option<&str>) {
        let entry = cairn_types::meta::ActivityEntry {
            id: uuid::Uuid::new_v4().simple().to_string(),
            action: action.to_owned(),
            bucket: bucket.map(str::to_owned),
            key: key.map(str::to_owned),
            size: None,
            etag: None,
            actor: None,
            at: self.clock.now(),
        };
        let _ = self
            .meta
            .submit(Mutation::RecordActivity(Box::new(entry)))
            .await;
    }
}

/// Split a subpath into non-empty segments, ignoring a leading/trailing slash.
fn split_path(subpath: &str) -> Vec<&str> {
    subpath.split('/').filter(|s| !s.is_empty()).collect()
}

/// Whether the principal is an administrator.
fn is_admin(principal: Option<&Principal>) -> bool {
    principal.is_some_and(|p| p.role == Role::Administrator)
}

/// The first value for `name` in the parsed query pairs.
fn find_query<'a>(query: &'a [(String, String)], name: &str) -> Option<&'a str> {
    query
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// The logical/physical compression ratio, defined as `1.0` when nothing is stored.
fn compression_ratio(counts: &StoreCounts) -> f64 {
    if counts.physical_bytes == 0 {
        1.0
    } else {
        counts.logical_bytes as f64 / counts.physical_bytes as f64
    }
}

/// Render a stored config document as JSON: if the document text is itself JSON, return the
/// parsed value so the UI sees structured data; otherwise return it as a JSON string so the
/// response is always valid JSON.
fn config_doc_to_json(doc: &ConfigDoc) -> serde_json::Value {
    serde_json::from_str(&doc.0).unwrap_or_else(|_| serde_json::Value::String(doc.0.clone()))
}

/// A high-entropy URL-safe Bearer secret (32 random bytes, hex-encoded).
fn generate_secret() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
