//! The control service: routing and the endpoint handlers of the management API.
//!
//! [`ControlService::handle`] takes the part of the path after `/api/v1` and dispatches to the
//! contract endpoints. Every endpoint except `/health` requires an administrator principal;
//! members and anonymous callers receive `403 { "error": "forbidden" }`.

use crate::wire;
use bytes::Bytes;
use cairn_types::auth::{Principal, Role};
use cairn_types::bucket::{Bucket, VersioningState};
use cairn_types::id::{BucketName, UserId};
use cairn_types::meta::{
    ListQuery, Mutation, MutationOutcome, StoreCounts, User, UserRecord,
};
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

            (&Method::GET, ["users"]) => self.list_users().await,
            (&Method::POST, ["users"]) => self.create_user(&body).await,

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

        self.record_activity("CreateBucket", Some(name.as_str()), None).await;

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
            },
        )
    }

    /// Page the current objects of a bucket with a bounded loop, summing count and logical
    /// bytes. Bounded by [`MAX_PAGES`] iterations.
    async fn bucket_current_totals(
        &self,
        bucket: &BucketName,
    ) -> Result<(u64, u64), String> {
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

        self.record_activity("DeleteBucket", Some(bucket_name.as_str()), None).await;

        ControlResponse {
            status: StatusCode::NO_CONTENT,
            body: Vec::new(),
        }
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

        let record = UserRecord {
            user: User {
                id: user_id.clone(),
                display_name: req.display_name,
                access_key_id: access_key_id.clone(),
                sigv4_access_key_id: None,
                role,
                is_active: true,
                created_at: now,
                updated_at: now,
            },
            bearer_secret_hash,
            sigv4_secret_ciphertext: None,
            sigv4_secret_nonce: None,
        };

        match self.meta.submit(Mutation::CreateUser(Box::new(record))).await {
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

/// A high-entropy URL-safe Bearer secret (32 random bytes, hex-encoded).
fn generate_secret() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
