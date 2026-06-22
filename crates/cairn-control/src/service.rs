//! The control service: routing and the endpoint handlers of the management API.
//!
//! [`ControlService::handle`] takes the part of the path after `/api/v1` and dispatches to the
//! contract endpoints. Every endpoint except `/health` requires an administrator principal;
//! members and anonymous callers receive `403 { "error": "forbidden" }`.

use crate::wire;
use bytes::Bytes;
use cairn_replication::{
    RemoteTarget, RemoteTargetInput, parse_targets, resolve_target, serialize_targets,
};
use cairn_types::auth::{Principal, Role};
use cairn_types::bucket::{
    Bucket, CompressionAlgorithm, CompressionPolicy, ConfigAspect, ConfigDoc, VersioningState,
};
use cairn_types::id::{BucketName, ObjectKey, UserId};
use cairn_types::meta::{
    ListQuery, MetricsRange, Mutation, MutationOutcome, ShareDisposition, ShareRow, StoreCounts,
    TagSummary, TaggedObject, User, UserRecord,
};
use cairn_types::time::Timestamp;
use cairn_types::traits::{BlobStore, Clock, Crypto, MetadataStore};
use http::{Method, StatusCode};
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

/// The bound on a single management-API listing page, and the batch size used when paging a
/// bucket for force-delete or per-bucket aggregation. Keeps every loop bounded in memory.
const PAGE_LIMIT: u32 = 1000;

/// The hard upper bound on iterations of any internal paging loop (force-delete, per-bucket
/// aggregation), so a hostile or corrupt cursor can never spin forever.
const MAX_PAGES: u32 = 100_000;

/// Cap on the per-item error list a single force-delete-prefix response carries, so a prefix that
/// fails to delete cannot accumulate an unbounded error list in memory (audit #26).
const MAX_DELETE_PREFIX_ERRORS: usize = 1_000;

/// A management-API response: an HTTP status, a JSON body, and a per-request id. The caller sets
/// `content-type: application/json` and emits `request_id` as the `x-amz-request-id` header on
/// every response, success or error (ARCH 25.1).
#[derive(Debug, Clone)]
pub struct ControlResponse {
    /// The HTTP status code.
    pub status: StatusCode,
    /// The JSON-encoded body.
    pub body: Vec<u8>,
    /// The per-request id. Mirrored into the `x-amz-request-id` response header and, for error
    /// envelopes, into the body's `request_id` field. Constructors leave this empty; [`handle`]
    /// stamps the real id into the response (and re-renders error envelopes) before returning.
    ///
    /// [`handle`]: ControlService::handle
    pub request_id: String,
}

impl ControlResponse {
    /// A response with a serialized JSON body at `status`. Serialization is infallible for the
    /// crate's own DTOs; a failure degrades to a `500` error envelope rather than panicking.
    fn json<T: Serialize>(status: StatusCode, value: &T) -> Self {
        match serde_json::to_vec(value) {
            Ok(body) => Self {
                status,
                body,
                request_id: String::new(),
            },
            Err(e) => Self::error_internal(&e.to_string()),
        }
    }

    /// An error envelope `{ "error": <message>, "request_id": "" }` at `status`. The request id is
    /// filled in centrally by [`ControlResponse::stamp_request_id`] once it is known, so every
    /// error path carries the same id as the response header.
    fn error(status: StatusCode, message: &str) -> Self {
        let body = serde_json::to_vec(&wire::ErrorResp {
            error: message.to_owned(),
            request_id: String::new(),
        })
        .unwrap_or_else(|_| br#"{"error":"internal error","request_id":""}"#.to_vec());
        Self {
            status,
            body,
            request_id: String::new(),
        }
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
            request_id: String::new(),
        }
    }

    /// Stamp the resolved request id onto this response: it always lands in `request_id` (the
    /// header source), and for an error envelope it is also rewritten into the body's
    /// `request_id` field so the body and header agree. Success bodies are left byte-for-byte
    /// intact. This is the single choke point through which every response acquires its id.
    fn stamp_request_id(mut self, id: &str) -> Self {
        self.request_id = id.to_owned();
        // Only error envelopes carry a `request_id` in the body. Detect one structurally rather
        // than by status so a non-2xx success (e.g. 204) is never mis-rewritten: an error body is
        // a JSON object with an `error` string and an (empty) `request_id` placeholder.
        if !self.body.is_empty() {
            if let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(&self.body) {
                if let Some(obj) = v.as_object_mut() {
                    if obj.contains_key("error") && obj.contains_key("request_id") {
                        obj.insert(
                            "request_id".to_owned(),
                            serde_json::Value::String(id.to_owned()),
                        );
                        if let Ok(bytes) = serde_json::to_vec(&v) {
                            self.body = bytes;
                        }
                    }
                }
            }
        }
        self
    }
}

/// Static facts about the running node, surfaced by `GET /system` for the console's node card.
/// Assembled once at startup from the server config; the service never re-reads config.
#[derive(Debug, Clone)]
pub struct SystemInfo {
    /// The server version (workspace `CARGO_PKG_VERSION`).
    pub version: String,
    /// The S3 API listener address as configured.
    pub s3_addr: String,
    /// The web-UI listener address as configured (may be `off`).
    pub ui_addr: String,
    /// Whether TLS is enabled on the S3 listener.
    pub tls: bool,
    /// The data directory (also the statvfs target for disk figures).
    pub data_dir: std::path::PathBuf,
    /// Process start instant, for uptime.
    pub started_at: std::time::Instant,
}

/// The JSON management API service. It owns shared handles to the trait spine and is otherwise
/// stateless; clone the `Arc`s freely.
#[derive(Clone)]
pub struct ControlService {
    meta: Arc<dyn MetadataStore>,
    blob: Arc<dyn BlobStore>,
    crypto: Arc<dyn Crypto>,
    clock: Arc<dyn Clock>,
    system: Arc<SystemInfo>,
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
        system: SystemInfo,
    ) -> Self {
        Self {
            meta,
            blob,
            crypto,
            clock,
            system: Arc::new(system),
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
        // One id per control request, carried into the JSON error envelope and emitted as the
        // `x-amz-request-id` response header on every response (ARCH 25.1).
        let request_id = Uuid::new_v4().simple().to_string();
        let resp = self.route(method, subpath, query, principal, body).await;
        resp.stamp_request_id(&request_id)
    }

    /// The inner router: produces a [`ControlResponse`] whose request id is stamped on by
    /// [`handle`]. Splitting this out keeps the request-id stamping a single choke point that
    /// every routed path — including the `404` fall-through and the unauthenticated `/health`
    /// branch — passes through.
    ///
    /// [`handle`]: ControlService::handle
    async fn route(
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
            return self.health(method).await;
        }

        // Everything else is admin-gated.
        if !is_admin(principal) {
            return ControlResponse::forbidden();
        }

        match (method, segments.as_slice()) {
            (&Method::GET, ["overview"]) => self.overview().await,
            (&Method::GET, ["overview", "buckets"]) => self.overview_buckets().await,
            (&Method::GET, ["system"]) => self.system(),
            (&Method::GET, ["metrics", "requests"]) => self.request_metrics(query).await,

            (&Method::GET, ["tags"]) => self.list_tags(query).await,
            (&Method::GET, ["tags", "objects"]) => self.list_tag_objects(query).await,

            (&Method::GET, ["buckets"]) => self.list_buckets().await,
            (&Method::POST, ["buckets"]) => self.create_bucket(&body, principal).await,
            (&Method::GET, ["buckets", name]) => self.bucket_detail(name).await,
            (&Method::DELETE, ["buckets", name]) => self.delete_bucket(name, principal).await,
            (&Method::GET, ["buckets", name, "objects"]) => self.list_objects(name, query).await,
            (&Method::DELETE, ["buckets", name, "objects"]) => {
                self.delete_prefix(name, query, principal).await
            }

            // Persistent object-share management (ARCH 15.8). Minting is handled in the server
            // adapter (it streams object bytes on redemption); listing and revoking are pure
            // metadata operations and live here.
            (&Method::GET, ["buckets", name, "objects", "shares"]) => {
                self.list_object_shares(name, query).await
            }
            (&Method::GET, ["buckets", name, "objects", "shares", token]) => {
                self.get_object_share(name, token).await
            }
            (&Method::DELETE, ["buckets", name, "objects", "shares", token]) => {
                self.revoke_object_share(name, token, principal).await
            }

            (&Method::GET, ["buckets", name, "config"]) => self.bucket_config(name).await,
            (&Method::PUT, ["buckets", name, "versioning"]) => {
                self.set_versioning(name, &body, principal).await
            }
            (&Method::PUT, ["buckets", name, "quota"]) => {
                self.set_quota(name, &body, principal).await
            }
            (&Method::PUT, ["buckets", name, "compression"]) => {
                self.set_compression(name, &body, principal).await
            }
            (&Method::PUT, ["buckets", name, "encryption"]) => {
                self.set_encryption(name, &body, principal).await
            }
            (&Method::GET, ["buckets", name, "notifications"]) => {
                self.get_notifications(name).await
            }
            (&Method::PUT, ["buckets", name, "notifications"]) => {
                self.set_notifications(name, &body, principal).await
            }
            (&Method::DELETE, ["buckets", name, "notifications"]) => {
                self.clear_notifications(name, principal).await
            }
            (&Method::PUT, ["buckets", name, "policy"]) => {
                self.set_policy(name, &body, principal).await
            }
            (&Method::DELETE, ["buckets", name, "policy"]) => {
                self.delete_policy(name, principal).await
            }

            (&Method::POST, ["buckets", name, "replication", "targets"]) => {
                self.add_replication_target(name, &body, principal).await
            }
            (&Method::GET, ["buckets", name, "replication", "targets"]) => {
                self.list_replication_targets(name).await
            }
            (&Method::DELETE, ["buckets", name, "replication", "targets", arn]) => {
                self.delete_replication_target(name, arn, principal).await
            }
            (&Method::POST, ["buckets", name, "replication", "retry"]) => {
                self.retry_replication(name, principal).await
            }
            (&Method::POST, ["buckets", name, "replication", "resync"]) => {
                self.resync_replication(name, principal).await
            }
            (&Method::GET, ["buckets", name, "replication", "status"]) => {
                self.replication_status(name).await
            }

            (&Method::POST, ["credentials", "temporary"]) => {
                self.mint_session_credential(&body, principal).await
            }
            (&Method::GET, ["credentials", "temporary"]) => self.list_session_credentials().await,
            (&Method::DELETE, ["credentials", "temporary", id]) => {
                self.revoke_session_credential(id, principal).await
            }
            (&Method::GET, ["users"]) => self.list_users().await,
            (&Method::POST, ["users"]) => self.create_user(&body, principal).await,
            (&Method::GET, ["users", id]) => self.user_detail(id).await,
            (&Method::PATCH, ["users", id]) => self.patch_user(id, &body, principal).await,
            (&Method::POST, ["users", id, "rotate-credentials"]) => {
                self.rotate_credentials(id, principal).await
            }
            (&Method::PUT, ["users", id, "quota"]) => {
                self.set_user_quota(id, &body, principal).await
            }
            (&Method::GET, ["users", id, "policy"]) => self.get_user_policy(id).await,
            (&Method::PUT, ["users", id, "policy"]) => {
                self.set_user_policy(id, &body, principal).await
            }
            (&Method::DELETE, ["users", id, "policy"]) => {
                self.delete_user_policy(id, principal).await
            }

            (&Method::GET, ["replication", "failed"]) => self.failed_replication(query).await,
            (&Method::GET, ["replication", "summary"]) => self.replication_summary().await,

            (&Method::GET, ["activity"]) => self.activity(query).await,

            _ => ControlResponse::not_found(),
        }
    }

    // -----------------------------------------------------------------------------------
    // Health & overview
    // -----------------------------------------------------------------------------------

    /// `GET /health`: liveness is unconditional (`status: "ok"`); readiness reflects a real probe
    /// of the metadata store — a bounded `list_buckets` call — rather than a hardcoded constant
    /// (ARCH 26.4). The endpoint is always `200`; an unready store surfaces as `ready: false` so
    /// a load balancer can drain the node without the probe itself erroring.
    async fn health(&self, method: &Method) -> ControlResponse {
        if method != Method::GET {
            return ControlResponse::not_found();
        }
        let ready = self.meta.list_buckets(None).await.is_ok();
        ControlResponse::json(
            StatusCode::OK,
            &wire::HealthResp {
                status: "ok",
                ready,
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

    async fn overview_buckets(&self) -> ControlResponse {
        let counts = match self.meta.bucket_counts().await {
            Ok(c) => c,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        ControlResponse::json(
            StatusCode::OK,
            &wire::OverviewBucketsResp {
                buckets: counts
                    .into_iter()
                    .map(|c| wire::BucketUsageEntry {
                        name: c.bucket,
                        objects: c.objects,
                        logical_bytes: c.logical_bytes,
                        physical_bytes: c.physical_bytes,
                    })
                    .collect(),
            },
        )
    }

    fn system(&self) -> ControlResponse {
        let (disk_total_bytes, disk_free_bytes) = match disk_stats(&self.system.data_dir) {
            Some((total, free)) => (Some(total), Some(free)),
            None => (None, None),
        };
        ControlResponse::json(
            StatusCode::OK,
            &wire::SystemResp {
                version: self.system.version.clone(),
                uptime_secs: self.system.started_at.elapsed().as_secs(),
                s3_addr: self.system.s3_addr.clone(),
                ui_addr: self.system.ui_addr.clone(),
                tls: self.system.tls,
                data_dir: self.system.data_dir.display().to_string(),
                disk_total_bytes,
                disk_free_bytes,
            },
        )
    }

    /// `GET /metrics/requests?range=1d|1w|2w|1m`: the request-metrics series for the console's
    /// activity dashboard — a downsampled timeline plus breakdowns by operation, most-active
    /// bucket, and HTTP status class, with range-wide totals (bytes, errors, latency average and
    /// p95, peak window, active buckets). An unknown or absent `range` falls back to `1d`. Timeline
    /// timestamps are converted from the store's epoch *seconds* into epoch *milliseconds* (`ts_ms`)
    /// for the UI.
    async fn request_metrics(&self, query: &[(String, String)]) -> ControlResponse {
        let range = MetricsRange::parse(find_query(query, "range").unwrap_or("1d"));
        let now_secs = self.clock.now().as_secs();
        let series = match self.meta.query_request_metrics(range, now_secs).await {
            Ok(s) => s,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        ControlResponse::json(
            StatusCode::OK,
            &wire::RequestMetricsResp {
                window_secs: series.window_secs,
                total: series.total,
                total_errors: series.total_errors,
                total_bytes_in: series.total_bytes_in,
                total_bytes_out: series.total_bytes_out,
                latency_avg_ms: series.latency_avg_ms,
                latency_p95_ms: series.latency_p95_ms,
                peak_window_count: series.peak_window_count,
                active_buckets: series.active_buckets,
                timeline: series
                    .timeline
                    .into_iter()
                    .map(|p| wire::MetricPoint {
                        ts_ms: p.ts * 1000,
                        count: p.count,
                        errors: p.errors,
                        bytes_in: p.bytes_in,
                        bytes_out: p.bytes_out,
                        latency_avg_ms: p.latency_avg_ms,
                    })
                    .collect(),
                by_operation: series
                    .by_operation
                    .into_iter()
                    .map(|o| wire::MetricOp {
                        operation: o.operation,
                        count: o.count,
                        bytes: o.bytes,
                        latency_avg_ms: o.latency_avg_ms,
                    })
                    .collect(),
                top_buckets: series
                    .top_buckets
                    .into_iter()
                    .map(|b| wire::MetricBucket {
                        bucket: b.bucket,
                        count: b.count,
                        bytes: b.bytes,
                    })
                    .collect(),
                top_buckets_by_bytes: series
                    .top_buckets_by_bytes
                    .into_iter()
                    .map(|b| wire::MetricBucket {
                        bucket: b.bucket,
                        count: b.count,
                        bytes: b.bytes,
                    })
                    .collect(),
                by_status: series
                    .by_status
                    .into_iter()
                    .map(|s| wire::MetricStatus {
                        status_class: s.status_class,
                        count: s.count,
                    })
                    .collect(),
            },
        )
    }

    // -----------------------------------------------------------------------------------
    // Object tag browsing (ARCH 17.2)
    // -----------------------------------------------------------------------------------

    /// `GET /tags[?bucket=<name>]`: the distinct object tags in use (`key=value`, each with a
    /// current-object count), optionally scoped to one bucket. An invalid `bucket` is a `400`.
    async fn list_tags(&self, query: &[(String, String)]) -> ControlResponse {
        let bucket = match find_query(query, "bucket") {
            Some(b) => match BucketName::parse(b) {
                Ok(n) => Some(n),
                Err(_) => return ControlResponse::bad_request("invalid bucket"),
            },
            None => None,
        };
        let summary = match self.meta.list_tag_summary(bucket.as_ref()).await {
            Ok(s) => s,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        ControlResponse::json(
            StatusCode::OK,
            &wire::TagSummaryResp {
                tags: summary
                    .into_iter()
                    .map(|t: TagSummary| wire::TagSummaryItem {
                        tag_key: t.tag_key,
                        tag_value: t.tag_value,
                        object_count: t.object_count,
                    })
                    .collect(),
            },
        )
    }

    /// `GET /tags/objects?key=<k>&value=<v>[&bucket=<name>]`: the current objects carrying the
    /// exact `key=value` tag, bounded by [`PAGE_LIMIT`]. Missing `key`/`value` is a `400`; an
    /// invalid `bucket` is a `400`.
    async fn list_tag_objects(&self, query: &[(String, String)]) -> ControlResponse {
        let (Some(key), Some(value)) = (find_query(query, "key"), find_query(query, "value"))
        else {
            return ControlResponse::bad_request("key and value are required");
        };
        let bucket = match find_query(query, "bucket") {
            Some(b) => match BucketName::parse(b) {
                Ok(n) => Some(n),
                Err(_) => return ControlResponse::bad_request("invalid bucket"),
            },
            None => None,
        };
        let objects = match self
            .meta
            .list_objects_by_tag(bucket.as_ref(), key, value, PAGE_LIMIT)
            .await
        {
            Ok(o) => o,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        ControlResponse::json(
            StatusCode::OK,
            &wire::TagObjectsResp {
                objects: objects
                    .into_iter()
                    .map(|o: TaggedObject| wire::TagObjectItem {
                        bucket: o.bucket,
                        key: o.key,
                        version_id: o.version_id,
                        size: o.size,
                        last_modified_ms: o.last_modified.as_millis(),
                    })
                    .collect(),
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

    async fn create_bucket(&self, body: &Bytes, principal: Option<&Principal>) -> ControlResponse {
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
        // Object Lock can only be turned on at creation and forces versioning Enabled (a WORM
        // guarantee is meaningless without it), mirroring the S3 create path.
        let bucket = Bucket {
            name: name.clone(),
            owner_id,
            created_at: self.clock.now(),
            versioning: if req.object_lock {
                VersioningState::Enabled
            } else {
                VersioningState::Unversioned
            },
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

        if req.object_lock {
            let config = cairn_types::ObjectLockConfiguration {
                enabled: true,
                default_retention: None,
            };
            if let Ok(text) = serde_json::to_string(&config) {
                let _ = self
                    .meta
                    .submit(Mutation::SetBucketConfig {
                        bucket: name.clone(),
                        aspect: ConfigAspect::ObjectLock,
                        doc: Some(ConfigDoc(text)),
                    })
                    .await;
            }
        }

        self.record_activity("CreateBucket", Some(name.as_str()), None, principal)
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

    async fn delete_bucket(&self, name: &str, principal: Option<&Principal>) -> ControlResponse {
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

        self.record_activity("DeleteBucket", Some(bucket_name.as_str()), None, principal)
            .await;

        ControlResponse::no_content()
    }

    /// `DELETE /buckets/{name}/objects?prefix=P`: permanently delete every version (and delete
    /// marker) under `prefix` — the proven force-empty path of [`delete_bucket`], scoped to a
    /// prefix. We re-list from the start each round because each deletion removes the rows the
    /// cursor was anchored against, so the loop converges. If [`MAX_PAGES`] is exhausted while
    /// items remain, `more = true` signals the UI to re-invoke.
    async fn delete_prefix(
        &self,
        name: &str,
        query: &[(String, String)],
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let prefix = match find_query(query, "prefix") {
            Some(p) if !p.is_empty() => p,
            _ => return ControlResponse::bad_request("prefix is required"),
        };

        let bucket_name = match BucketName::parse(name) {
            Ok(n) => n,
            Err(_) => return ControlResponse::not_found(),
        };
        match self.meta.get_bucket(&bucket_name).await {
            Ok(Some(_)) => {}
            Ok(None) => return ControlResponse::not_found(),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        }

        let mut deleted: u64 = 0;
        let mut errors: Vec<wire::DeletePrefixError> = Vec::new();
        let mut more = true;

        for _ in 0..MAX_PAGES {
            let query = ListQuery {
                prefix: Some(prefix.to_owned()),
                limit: PAGE_LIMIT,
                ..Default::default()
            };
            let page = match self.meta.list_versions(&bucket_name, &query).await {
                Ok(p) => p,
                Err(e) => return ControlResponse::error_internal(&e.to_string()),
            };
            if page.items.is_empty() {
                more = false;
                break;
            }
            let deleted_before = deleted;
            for item in &page.items {
                match self
                    .meta
                    .submit(Mutation::DeleteVersion {
                        bucket: bucket_name.clone(),
                        key: item.key.clone(),
                        version_id: item.version_id.clone(),
                    })
                    .await
                {
                    Ok(MutationOutcome::Deleted { freed, .. }) => {
                        if let Some(path) = freed {
                            // Blob reclamation is best-effort and idempotent.
                            let _ = self.blob.delete(&path).await;
                        }
                        deleted += 1;
                    }
                    Ok(_) => {
                        deleted += 1;
                    }
                    Err(e) => {
                        // Record the per-item failure but keep going so one bad row does not
                        // strand the rest of the prefix. The list is capped (audit #26) so a
                        // wholesale failure cannot grow it without bound.
                        if errors.len() < MAX_DELETE_PREFIX_ERRORS {
                            errors.push(wire::DeletePrefixError {
                                key: item.key.as_str().to_owned(),
                                message: e.to_string(),
                            });
                        }
                    }
                }
            }
            // The list query is re-anchored at the prefix each page, so it only converges when a
            // page actually deletes rows. A page that deleted nothing (every item errored) would
            // recur identically until MAX_PAGES — stop early instead of spinning (audit #26).
            if deleted == deleted_before {
                break;
            }
        }

        self.record_activity(
            "DeletePrefix",
            Some(bucket_name.as_str()),
            Some(prefix),
            principal,
        )
        .await;

        ControlResponse::json(
            StatusCode::OK,
            &wire::DeletePrefixResp {
                deleted,
                errors,
                more,
            },
        )
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
        // A delimiter folds keys into common prefixes ("folders"), exactly like S3 listing.
        let delimiter = find_query(query, "delimiter")
            .filter(|d| !d.is_empty())
            .map(str::to_owned);

        let list_query = ListQuery {
            prefix,
            delimiter,
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
                common_prefixes: page.common_prefixes,
                next: page.next_cursor,
            },
        )
    }

    // -----------------------------------------------------------------------------------
    // Bucket configuration (ARCH 22.2)
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
        let (policy, cors, tagging, lifecycle, acl, public_access_block, encryption) =
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
                encryption,
            },
        )
    }

    /// Read the seven exposed config aspects of a bucket, each rendered as JSON (or `None` when
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
            read(ConfigAspect::Encryption).await?,
        ))
    }

    /// `PUT /buckets/{name}/versioning`: set the bucket's versioning state.
    async fn set_versioning(
        &self,
        name: &str,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
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

        self.record_activity("SetVersioning", Some(bucket_name.as_str()), None, principal)
            .await;
        ControlResponse::no_content()
    }

    /// `PUT /buckets/{name}/quota`: set or clear the bucket's byte quota.
    async fn set_quota(
        &self,
        name: &str,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
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

        self.record_activity(
            "SetBucketQuota",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
        .await;
        ControlResponse::no_content()
    }

    /// `PUT /buckets/{name}/compression`: set or disable the bucket's compression policy, applied to
    /// subsequent object writes. Body: `{"algorithm": "zstd"|"lz4"|"none", "block_size": 65536}`.
    async fn set_compression(
        &self,
        name: &str,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
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

        self.record_activity(
            "SetBucketCompression",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
        .await;
        ControlResponse::no_content()
    }

    /// `PUT /buckets/{name}/encryption`: set or clear the bucket's default server-side encryption.
    /// `"AES256"` makes every new upload SSE-S3-encrypted (unless the request carries its own
    /// `x-amz-server-side-encryption` header); `"none"` returns to unencrypted-by-default.
    /// Existing objects are never rewritten.
    async fn set_encryption(
        &self,
        name: &str,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        let req: wire::SetEncryptionReq = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };
        let doc = match req.algorithm.to_ascii_uppercase().as_str() {
            "NONE" | "OFF" | "" => None,
            "AES256" => Some(ConfigDoc(r#"{"algorithm":"AES256"}"#.to_owned())),
            other => {
                return ControlResponse::bad_request(&format!(
                    "unknown algorithm {other:?} (expected AES256|none)"
                ));
            }
        };

        if let Err(e) = self
            .meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket_name.clone(),
                aspect: ConfigAspect::Encryption,
                doc,
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity(
            "SetBucketEncryption",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
        .await;
        ControlResponse::no_content()
    }

    /// `GET /buckets/{name}/notifications`: the bucket's webhook event-notification configuration
    /// (the endpoint list). Secrets are returned only as a presence flag, never the value.
    async fn get_notifications(&self, name: &str) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        let config = match self
            .meta
            .get_bucket_config(&bucket_name, ConfigAspect::Notification)
            .await
        {
            Ok(Some(doc)) => {
                serde_json::from_str::<cairn_types::NotificationConfig>(&doc.0).unwrap_or_default()
            }
            Ok(None) => cairn_types::NotificationConfig::default(),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        let endpoints: Vec<wire::WebhookEndpointView> = config
            .endpoints
            .into_iter()
            .map(wire::WebhookEndpointView::from)
            .collect();
        ControlResponse::json(StatusCode::OK, &wire::NotificationsResp { endpoints })
    }

    /// `PUT /buckets/{name}/notifications`: replace the bucket's webhook endpoint list. The body is
    /// a `NotificationConfig` JSON document; each endpoint is validated (non-empty id, http(s) URL,
    /// at least one event selector) before storing. Secrets are stored as-is (sealed at rest is not
    /// required — they are HMAC signing keys, not credentials) and never echoed back.
    async fn set_notifications(
        &self,
        name: &str,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        let mut config: cairn_types::NotificationConfig = match serde_json::from_slice(body) {
            Ok(c) => c,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };
        // Preserve existing signing secrets: since secrets are write-only (GET never returns them)
        // and a PUT replaces the whole endpoint list, an incoming endpoint that omits its secret
        // (`null`) inherits the stored secret for the same id — so editing one endpoint's URL does
        // not silently wipe another's HMAC key. To clear a secret, send an empty string.
        let existing: Option<cairn_types::NotificationConfig> = match self
            .meta
            .get_bucket_config(&bucket_name, ConfigAspect::Notification)
            .await
        {
            Ok(Some(doc)) => serde_json::from_str(&doc.0).ok(),
            _ => None,
        };
        if let Some(prev) = &existing {
            for ep in &mut config.endpoints {
                if ep.secret.is_none() {
                    if let Some(p) = prev.endpoints.iter().find(|p| p.id == ep.id) {
                        ep.secret = p.secret.clone();
                    }
                } else if ep.secret.as_deref() == Some("") {
                    ep.secret = None; // explicit clear
                }
            }
        } else {
            for ep in &mut config.endpoints {
                if ep.secret.as_deref() == Some("") {
                    ep.secret = None;
                }
            }
        }
        // Validate every endpoint before storing so a bad config is rejected at the edge.
        let mut seen = std::collections::HashSet::new();
        for ep in &config.endpoints {
            if ep.id.trim().is_empty() {
                return ControlResponse::bad_request("endpoint id must not be empty");
            }
            if !seen.insert(ep.id.as_str()) {
                return ControlResponse::bad_request(&format!("duplicate endpoint id {:?}", ep.id));
            }
            if !(ep.url.starts_with("http://") || ep.url.starts_with("https://")) {
                return ControlResponse::bad_request(&format!(
                    "endpoint {:?} url must be http(s)",
                    ep.id
                ));
            }
            if ep.events.is_empty() {
                return ControlResponse::bad_request(&format!(
                    "endpoint {:?} must subscribe to at least one event",
                    ep.id
                ));
            }
        }
        let doc = match serde_json::to_string(&config) {
            Ok(s) => ConfigDoc(s),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        if let Err(e) = self
            .meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket_name.clone(),
                aspect: ConfigAspect::Notification,
                doc: Some(doc),
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }
        self.record_activity(
            "SetBucketNotifications",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
        .await;
        ControlResponse::no_content()
    }

    /// `DELETE /buckets/{name}/notifications`: remove all webhook endpoints for the bucket.
    async fn clear_notifications(
        &self,
        name: &str,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        if let Err(e) = self
            .meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket_name.clone(),
                aspect: ConfigAspect::Notification,
                doc: None,
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }
        self.record_activity(
            "ClearBucketNotifications",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
        .await;
        ControlResponse::no_content()
    }

    /// `PUT /buckets/{name}/policy`: validate the raw policy JSON via `cairn_authz::parse_policy`
    /// and store it as the bucket's `Policy` config aspect.
    async fn set_policy(
        &self,
        name: &str,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };

        // The body is the raw policy JSON document. Validate it before storing so a malformed
        // policy is rejected at the edge rather than failing open later (ARCH 15.5).
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

        self.record_activity(
            "PutBucketPolicy",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
        .await;
        ControlResponse::no_content()
    }

    /// `DELETE /buckets/{name}/policy`: clear the bucket's policy.
    async fn delete_policy(&self, name: &str, principal: Option<&Principal>) -> ControlResponse {
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

        self.record_activity(
            "DeleteBucketPolicy",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
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

    /// `POST /api/v1/credentials/temporary`: mint an STS-style temporary session credential scoped
    /// to the calling admin by a required inline policy (ARCH 14). Returns the access key, secret,
    /// opaque session token, and expiry exactly once. The credential is least-privilege: it carries
    /// the parent's identity for ownership/audit but never the owner/admin short-circuit, so it can
    /// do exactly what its policy grants until it expires.
    async fn mint_session_credential(
        &self,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let req: wire::MintSessionReq = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };
        // Bound the lifetime: 15 minutes .. 12 hours, mirroring AWS STS session limits.
        if !(900..=43_200).contains(&req.duration_secs) {
            return ControlResponse::bad_request(
                "duration_secs must be between 900 (15m) and 43200 (12h)",
            );
        }
        // The scoped policy is required and validated up front (a session has no implicit access).
        let policy_json = match serde_json::to_string(&req.policy) {
            Ok(s) => s,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };
        if let Err(e) = cairn_authz::parse_user_policy(&policy_json) {
            return ControlResponse::bad_request(&format!("invalid policy: {e}"));
        }
        let Some(parent) = principal.map(|p| p.user_id.clone()) else {
            return ControlResponse::forbidden();
        };

        let now = self.clock.now();
        let access_key_id = format!(
            "CAIRNTMP{}",
            uuid::Uuid::new_v4().simple().to_string().to_uppercase()
        );
        let secret = generate_secret();
        // The opaque session token the SDK must present as `X-Amz-Security-Token`; only its hash is
        // stored. High-entropy machine token, so a fast hash + constant-time compare suffices.
        let session_token = generate_secret();
        let (secret_ciphertext, secret_nonce) = match self.crypto.seal(secret.as_bytes()) {
            // CRK1 envelope (audit #29): the nonce is inside the ciphertext; store NULL nonce.
            Ok(sealed) => (sealed.ciphertext, None),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        let expires_at = cairn_types::Timestamp(now.0 + (req.duration_secs as i64) * 1000);

        let record = cairn_types::SessionCredentialRecord {
            access_key_id: access_key_id.clone(),
            parent_user_id: parent,
            secret_ciphertext,
            secret_nonce,
            session_token_hash: cairn_auth::hash_session_token(&session_token),
            inline_policy: Some(policy_json),
            expires_at,
            created_at: now,
        };
        if let Err(e) = self
            .meta
            .submit(Mutation::CreateSessionCredential(Box::new(record)))
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }
        self.record_activity("MintSessionCredential", None, None, principal)
            .await;
        ControlResponse::json(
            StatusCode::OK,
            &wire::MintSessionResp {
                access_key_id,
                secret_access_key: secret,
                session_token,
                expiration_epoch_secs: expires_at.as_secs(),
            },
        )
    }

    /// `GET /credentials/temporary`: the active (non-expired) session credentials as a public
    /// summary (no secret/token material) for the console's "active sessions" view.
    async fn list_session_credentials(&self) -> ControlResponse {
        match self.meta.list_session_credentials(self.clock.now()).await {
            Ok(rows) => ControlResponse::json(
                StatusCode::OK,
                &wire::ListSessionsResp {
                    sessions: rows
                        .into_iter()
                        .map(|s| wire::SessionView {
                            access_key_id: s.access_key_id,
                            parent_user_id: s.parent_user_id.0,
                            scoped: s.has_inline_policy,
                            created_at_ms: s.created_at.0,
                            expires_at_ms: s.expires_at.0,
                        })
                        .collect(),
                },
            ),
            Err(e) => ControlResponse::error_internal(&e.to_string()),
        }
    }

    /// `DELETE /credentials/temporary/{id}`: revoke a session credential early by its access-key id.
    /// Idempotent — revoking an unknown or already-expired id still returns 204.
    async fn revoke_session_credential(
        &self,
        access_key_id: &str,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        if let Err(e) = self
            .meta
            .submit(Mutation::DeleteSessionCredential {
                access_key_id: access_key_id.to_owned(),
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }
        self.record_activity("RevokeSessionCredential", None, None, principal)
            .await;
        ControlResponse::no_content()
    }

    async fn create_user(&self, body: &Bytes, principal: Option<&Principal>) -> ControlResponse {
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

        // If a canned replication policy was requested, validate the destination bucket name and
        // build the policy JSON up front — before any state is created — so a bad bucket name is a
        // clean `400` rather than a half-provisioned user (ARCH 20.5).
        let replication_policy = match req.replication_policy_bucket.as_deref() {
            Some(b) => match replication_policy_for_bucket(b) {
                Ok(json) => Some(json),
                Err(resp) => return resp,
            },
            None => None,
        };

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
                // CRK1 envelope (audit #29): the nonce is inside the ciphertext; store NULL nonce.
                Ok(sealed) => (Some(sealed.ciphertext), None),
                Err(e) => return ControlResponse::error_internal(&e.to_string()),
            };

        let record = UserRecord {
            user: User {
                id: user_id.clone(),
                display_name: req.display_name,
                access_key_id: access_key_id.clone(),
                sigv4_access_key_id: Some(sigv4_access_key_id.clone()),
                role,
                is_active: true,
                quota_bytes: None,
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

        // Attach the canned replication identity policy when requested, so the dedicated
        // destination credential is minted ready to receive replicated writes (ARCH 20.5). The
        // policy JSON was validated above before any state was touched.
        if let Some(policy) = replication_policy {
            if let Err(e) = self
                .meta
                .submit(Mutation::SetUserPolicy {
                    user_id: user_id.clone(),
                    policy: Some(policy),
                })
                .await
            {
                return ControlResponse::error_internal(&e.to_string());
            }
        }

        self.record_activity("CreateUser", None, None, principal)
            .await;

        ControlResponse::json(
            StatusCode::CREATED,
            &wire::CreateUserResp {
                id: user_id.to_string(),
                bearer_access_key_id: access_key_id,
                bearer_secret: secret,
                s3_access_key_id: sigv4_access_key_id,
                s3_secret_key: sigv4_secret,
            },
        )
    }

    /// `PATCH /users/{id}`: update a user's mutable fields (activation and/or role). Absent
    /// fields are left unchanged. Returns the updated public user view.
    async fn patch_user(
        &self,
        id: &str,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
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

        self.record_activity("UpdateUser", None, None, principal)
            .await;

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
    async fn rotate_credentials(&self, id: &str, principal: Option<&Principal>) -> ControlResponse {
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

        self.record_activity("RotateCredentials", None, None, principal)
            .await;

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

    /// `GET /users/{id}`: the public user view plus its attached identity (per-user) policy.
    async fn user_detail(&self, id: &str) -> ControlResponse {
        let user_id = UserId(id.to_owned());
        let record = match self.load_user_record(&user_id).await {
            Ok(Some(r)) => r,
            Ok(None) => return ControlResponse::not_found(),
            Err(resp) => return resp,
        };
        let policy = match self.load_user_policy_value(&user_id).await {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        ControlResponse::json(
            StatusCode::OK,
            &wire::UserDetailResp {
                id: record.user.id.to_string(),
                display_name: record.user.display_name,
                access_key_id: record.user.access_key_id,
                sigv4_access_key_id: record.user.sigv4_access_key_id,
                role: wire::role_str(record.user.role),
                is_active: record.user.is_active,
                quota_bytes: record.user.quota_bytes,
                policy,
            },
        )
    }

    /// `GET /users/{id}/policy`: the attached identity policy document (or null).
    async fn get_user_policy(&self, id: &str) -> ControlResponse {
        let user_id = UserId(id.to_owned());
        // Confirm the user exists so a missing user is a 404, not a null policy.
        match self.load_user_record(&user_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return ControlResponse::not_found(),
            Err(resp) => return resp,
        }
        let policy = match self.load_user_policy_value(&user_id).await {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        ControlResponse::json(StatusCode::OK, &wire::UserPolicyResp { policy })
    }

    /// `PUT /users/{id}/policy`: validate the raw identity-policy JSON via `parse_user_policy`
    /// (Principal-less; the principal is this user) and attach it. Rejected at the edge if malformed.
    async fn set_user_policy(
        &self,
        id: &str,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let user_id = UserId(id.to_owned());
        match self.load_user_record(&user_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return ControlResponse::not_found(),
            Err(resp) => return resp,
        }
        let policy_json = match std::str::from_utf8(body) {
            Ok(s) => s,
            Err(_) => return ControlResponse::bad_request("policy must be valid UTF-8 JSON"),
        };
        if let Err(e) = cairn_authz::parse_user_policy(policy_json) {
            return ControlResponse::bad_request(&format!("invalid policy: {e}"));
        }
        if let Err(e) = self
            .meta
            .submit(Mutation::SetUserPolicy {
                user_id,
                policy: Some(policy_json.to_owned()),
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }
        self.record_activity("SetUserPolicy", None, None, principal)
            .await;
        ControlResponse::no_content()
    }

    /// `DELETE /users/{id}/policy`: detach the user's identity policy.
    async fn delete_user_policy(&self, id: &str, principal: Option<&Principal>) -> ControlResponse {
        let user_id = UserId(id.to_owned());
        match self.load_user_record(&user_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return ControlResponse::not_found(),
            Err(resp) => return resp,
        }
        if let Err(e) = self
            .meta
            .submit(Mutation::SetUserPolicy {
                user_id,
                policy: None,
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }
        self.record_activity("DeleteUserPolicy", None, None, principal)
            .await;
        ControlResponse::no_content()
    }

    /// Load a user's attached identity policy as a parsed JSON value (or null), failing closed on a
    /// store error. A stored doc is JSON (validated on write), so a parse miss surfaces as null.
    async fn load_user_policy_value(
        &self,
        id: &UserId,
    ) -> Result<Option<serde_json::Value>, ControlResponse> {
        match self.meta.get_user_policy(id).await {
            Ok(Some(raw)) => Ok(serde_json::from_str(&raw).ok()),
            Ok(None) => Ok(None),
            Err(e) => Err(ControlResponse::error_internal(&e.to_string())),
        }
    }

    /// `PUT /users/{id}/quota`: set or clear a user's byte quota. The quota is enforced inside the
    /// writer's commit transaction for the objects the user owns (ARCH 27.5); this endpoint only
    /// persists the configured value (there is no by-id user-quota reader to echo it back, so the
    /// set is the contract — see the wire DTO note). Returns `204` on success.
    async fn set_user_quota(
        &self,
        id: &str,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let user_id = UserId(id.to_owned());
        // Confirm the user exists so a missing user is a clean 404 rather than a silent no-op.
        match self.load_user_record(&user_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return ControlResponse::not_found(),
            Err(resp) => return resp,
        }
        let req: wire::SetUserQuotaReq = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };

        if let Err(e) = self
            .meta
            .submit(Mutation::SetUserQuota {
                user_id,
                quota_bytes: req.quota_bytes,
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity("SetUserQuota", None, None, principal)
            .await;
        ControlResponse::no_content()
    }

    // -----------------------------------------------------------------------------------
    // Replication operations (ARCH 22.2)
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
    // Per-bucket remote replication targets (ARCH 20.5)
    // -----------------------------------------------------------------------------------

    /// Read and parse a bucket's stored `ReplicationTargets` config-aspect document into the typed
    /// target list. An unset aspect parses to an empty list. Maps a store/parse failure to a
    /// `500` error response.
    async fn load_replication_targets(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<RemoteTarget>, ControlResponse> {
        let doc = self
            .meta
            .get_bucket_config(bucket, ConfigAspect::ReplicationTargets)
            .await
            .map_err(|e| ControlResponse::error_internal(&e.to_string()))?;
        let bytes = doc.map(|d| d.0).unwrap_or_default();
        parse_targets(bytes.as_bytes()).map_err(|e| ControlResponse::error_internal(&e.to_string()))
    }

    /// Serialize the target set and store it back as the bucket's `ReplicationTargets` aspect.
    async fn store_replication_targets(
        &self,
        bucket: &BucketName,
        targets: &[RemoteTarget],
    ) -> Result<(), ControlResponse> {
        let doc = serialize_targets(targets);
        self.meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket.clone(),
                aspect: ConfigAspect::ReplicationTargets,
                doc: Some(ConfigDoc(doc)),
            })
            .await
            .map_err(|e| ControlResponse::error_internal(&e.to_string()))?;
        Ok(())
    }

    /// `POST /buckets/{name}/replication/targets`: seal the destination secret under the master
    /// key, mint a stable ARN, append the target to the bucket's stored set, and persist it. The
    /// response returns only the minted ARN — the secret is never echoed back (ARCH 20.5).
    async fn add_replication_target(
        &self,
        name: &str,
        body: &Bytes,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        let req: wire::CreateReplicationTargetReq = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return ControlResponse::bad_request(&e.to_string()),
        };
        if req.endpoint.trim().is_empty()
            || req.region.trim().is_empty()
            || req.dest_bucket.trim().is_empty()
            || req.access_key.trim().is_empty()
            || req.secret.is_empty()
        {
            return ControlResponse::bad_request(
                "endpoint, region, dest_bucket, access_key, and secret are all required",
            );
        }

        let input = RemoteTargetInput {
            endpoint: req.endpoint,
            region: req.region,
            dest_bucket: req.dest_bucket,
            access_key_id: req.access_key,
            secret: req.secret,
        };
        // Seal the destination secret under the master key via the Crypto trait spine and mint the
        // stable ARN, exactly as `cairn_replication::seal_target` does (it needs the concrete
        // `SystemCrypto`; the control plane only holds `Arc<dyn Crypto>`, so it seals here).
        let target = match self.seal_remote_target(input) {
            Ok(t) => t,
            Err(resp) => return resp,
        };
        let arn = target.arn.clone();

        let mut targets = match self.load_replication_targets(&bucket_name).await {
            Ok(t) => t,
            Err(resp) => return resp,
        };
        targets.push(target);
        if let Err(resp) = self.store_replication_targets(&bucket_name, &targets).await {
            return resp;
        }

        self.record_activity(
            "AddReplicationTarget",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
        .await;
        ControlResponse::json(
            StatusCode::CREATED,
            &wire::CreateReplicationTargetResp { arn },
        )
    }

    /// `GET /buckets/{name}/replication/targets`: list the bucket's targets WITHOUT any secret
    /// material — only the ARN, endpoint, region, destination bucket, and access-key id.
    async fn list_replication_targets(&self, name: &str) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        let targets = match self.load_replication_targets(&bucket_name).await {
            Ok(t) => t,
            Err(resp) => return resp,
        };
        let targets = targets
            .into_iter()
            .map(|t| wire::ReplicationTargetEntry {
                arn: t.arn,
                endpoint: t.endpoint,
                region: t.region,
                dest_bucket: t.dest_bucket,
                access_key_id: t.access_key_id,
            })
            .collect();
        ControlResponse::json(StatusCode::OK, &wire::ReplicationTargetListResp { targets })
    }

    /// `DELETE /buckets/{name}/replication/targets/{arn}`: remove the target with the given ARN
    /// from the bucket's stored set and persist the remainder. A `404` when no such ARN exists.
    async fn delete_replication_target(
        &self,
        name: &str,
        arn: &str,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        // The ARN arrives as a single path segment; it may have been percent-encoded by the
        // client (it contains colons), so decode before matching.
        let arn = percent_decode(arn);
        let mut targets = match self.load_replication_targets(&bucket_name).await {
            Ok(t) => t,
            Err(resp) => return resp,
        };
        if resolve_target(&targets, &arn).is_none() {
            return ControlResponse::not_found();
        }
        targets.retain(|t| t.arn != arn);
        if let Err(resp) = self.store_replication_targets(&bucket_name, &targets).await {
            return resp;
        }

        self.record_activity(
            "DeleteReplicationTarget",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
        .await;
        ControlResponse::no_content()
    }

    /// `POST /buckets/{name}/replication/retry`: requeue this bucket's terminally-failed outbox
    /// entries for another attempt (ARCH 20.5). Observes the failed count first (bounded) so the
    /// ack can report how many entries were requeued, then submits the retry mutation.
    async fn retry_replication(
        &self,
        name: &str,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };

        // Count this bucket's failed entries before the requeue, so the ack reports a real figure.
        let failed_observed = match self.meta.list_failed_replication(PAGE_LIMIT).await {
            Ok(entries) => entries
                .iter()
                .filter(|e| e.bucket.as_str() == bucket_name.as_str())
                .count() as u64,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };

        if let Err(e) = self
            .meta
            .submit(Mutation::RetryFailedReplication {
                bucket: Some(bucket_name.clone()),
                now: self.clock.now(),
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }

        self.record_activity(
            "RetryFailedReplication",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
        .await;
        ControlResponse::json(
            StatusCode::OK,
            &wire::ReplicationRetryResp {
                requeued: true,
                failed_observed,
            },
        )
    }

    /// `POST /buckets/{name}/replication/resync`: trigger existing-object backfill (ARCH 20.5).
    /// Requires the bucket to have a replication configuration with at least one enabled rule whose
    /// `ExistingObjectReplication` is enabled. The actual enumeration runs as a spawned background
    /// task (a large bucket must not block the request); the entries it enqueues are idempotent, so
    /// re-running a resync is safe and already-replicated versions are not re-shipped.
    async fn resync_replication(
        &self,
        name: &str,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        let doc = match self
            .meta
            .get_bucket_config(&bucket_name, ConfigAspect::Replication)
            .await
        {
            Ok(Some(d)) => d,
            Ok(None) => {
                return ControlResponse::bad_request("bucket has no replication configuration");
            }
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        let cfg = match cairn_replication::parse_replication(doc.0.as_bytes()) {
            Ok(c) => c,
            Err(e) => {
                return ControlResponse::bad_request(&format!("invalid replication config: {e}"));
            }
        };
        if !cfg
            .rules
            .iter()
            .any(|r| r.enabled && r.existing_object_replication)
        {
            return ControlResponse::bad_request(
                "no enabled rule has existing-object replication (set ExistingObjectReplication=Enabled)",
            );
        }

        // Spawn the bounded backfill; the request returns immediately.
        let meta = self.meta.clone();
        let clock = self.clock.clone();
        let bucket = bucket_name.clone();
        tokio::spawn(async move {
            backfill_replication(meta, clock, bucket, cfg).await;
        });

        self.record_activity(
            "ResyncReplication",
            Some(bucket_name.as_str()),
            None,
            principal,
        )
        .await;
        ControlResponse::json(
            StatusCode::ACCEPTED,
            &wire::ReplicationResyncResp { started: true },
        )
    }

    /// `GET /buckets/{name}/replication/status`: per-bucket replication counters — `pending` (due
    /// entries filtered to this bucket) and `failed` (terminal entries filtered to this bucket) —
    /// plus the most recent failed entries' errors. Every figure is bounded by [`PAGE_LIMIT`].
    async fn replication_status(&self, name: &str) -> ControlResponse {
        let bucket_name = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };

        let now = self.clock.now();
        // Exact counts + true lag in one indexed pass (not PAGE_LIMIT-bounded).
        let counts = match self.meta.replication_counts(Some(&bucket_name)).await {
            Ok(c) => c,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        // `recent_errors` is a bounded human-readable sample of this bucket's terminal failures.
        let recent_errors = match self.meta.list_failed_replication(PAGE_LIMIT).await {
            Ok(entries) => entries
                .into_iter()
                .filter(|e| e.bucket.as_str() == bucket_name.as_str())
                .map(|e| wire::ReplicationStatusError {
                    key: e.key.as_str().to_owned(),
                    version_id: e.version_id.as_str().to_owned(),
                    error: e.last_error,
                })
                .collect(),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };

        ControlResponse::json(
            StatusCode::OK,
            &wire::ReplicationStatusResp {
                bucket: bucket_name.as_str().to_owned(),
                pending: counts.pending,
                failed: counts.failed,
                lag_seconds: replication_lag_seconds(now, counts.oldest_pending_at_ms),
                by_target: counts
                    .by_target
                    .into_iter()
                    .map(target_count_wire)
                    .collect(),
                recent_errors,
            },
        )
    }

    /// `GET /replication/summary`: store-wide replication health (exact counts, true lag, per-target
    /// breakdown) in one indexed pass — the console overview's live snapshot.
    async fn replication_summary(&self) -> ControlResponse {
        let now = self.clock.now();
        let counts = match self.meta.replication_counts(None).await {
            Ok(c) => c,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        ControlResponse::json(
            StatusCode::OK,
            &wire::ReplicationSummaryResp {
                pending: counts.pending,
                claimed: counts.claimed,
                failed: counts.failed,
                completed: counts.completed,
                lag_seconds: replication_lag_seconds(now, counts.oldest_pending_at_ms),
                by_target: counts
                    .by_target
                    .into_iter()
                    .map(target_count_wire)
                    .collect(),
            },
        )
    }

    /// Seal a [`RemoteTargetInput`] into a storable [`RemoteTarget`] through the `Crypto` trait
    /// spine the control plane holds (an `Arc<dyn Crypto>`), minting the same
    /// `arn:cairn:replication:<region>:<uuid>:<dest_bucket>` ARN shape as
    /// `cairn_replication::seal_target`. The standalone function takes the concrete `SystemCrypto`,
    /// so the seal is performed here against the trait object instead.
    fn seal_remote_target(
        &self,
        input: RemoteTargetInput,
    ) -> Result<RemoteTarget, ControlResponse> {
        let sealed = self
            .crypto
            .seal(input.secret.as_bytes())
            .map_err(|e| ControlResponse::error_internal(&e.to_string()))?;
        let arn = format!(
            "arn:cairn:replication:{}:{}:{}",
            input.region,
            Uuid::new_v4().simple(),
            input.dest_bucket
        );
        Ok(RemoteTarget {
            arn,
            endpoint: input.endpoint,
            region: input.region,
            dest_bucket: input.dest_bucket,
            access_key_id: input.access_key_id,
            // CRK1 envelope (audit #29): the nonce is inside the ciphertext; store an empty nonce.
            secret_ciphertext: sealed.ciphertext,
            nonce: Vec::new(),
        })
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
                actor: e.actor,
                at_ms: e.at.as_millis(),
            })
            .collect();
        ControlResponse::json(StatusCode::OK, &wire::ActivityListResp { entries: list })
    }

    /// `GET /buckets/{name}/objects/shares[?key=]`: list a bucket's shares (or one key's).
    async fn list_object_shares(&self, name: &str, query: &[(String, String)]) -> ControlResponse {
        let bucket = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        let key = find_query(query, "key").and_then(|k| ObjectKey::parse(k).ok());
        let shares = match self.meta.list_shares(&bucket, key.as_ref()).await {
            Ok(s) => s,
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        };
        let now = self.clock.now();
        let records = shares.into_iter().map(|s| share_record(s, now)).collect();
        ControlResponse::json(StatusCode::OK, &wire::ShareListResp { shares: records })
    }

    /// `GET /buckets/{name}/objects/shares/{token}`: one share by token.
    async fn get_object_share(&self, name: &str, token: &str) -> ControlResponse {
        if let Err(resp) = self.require_bucket(name).await {
            return resp;
        }
        match self.meta.get_share(token).await {
            Ok(Some(s)) if s.bucket.as_str() == name => {
                ControlResponse::json(StatusCode::OK, &share_record(s, self.clock.now()))
            }
            // Absent, or belonging to another bucket: 404 either way — a share is only visible
            // through its own bucket's path, and we never reveal cross-bucket token existence
            // (audit #27).
            Ok(_) => ControlResponse::not_found(),
            Err(e) => ControlResponse::error_internal(&e.to_string()),
        }
    }

    /// `DELETE /buckets/{name}/objects/shares/{token}`: revoke a share. Idempotent.
    async fn revoke_object_share(
        &self,
        name: &str,
        token: &str,
        principal: Option<&Principal>,
    ) -> ControlResponse {
        let bucket = match self.require_bucket(name).await {
            Ok(n) => n,
            Err(resp) => return resp,
        };
        // Scope the token to the path bucket (audit #27): only revoke a share that belongs to this
        // bucket, so an admin for one bucket cannot revoke another bucket's share by token. A token
        // that is absent or belongs to another bucket is an idempotent no-op, not a cross-bucket
        // revoke.
        match self.meta.get_share(token).await {
            Ok(Some(s)) if s.bucket.as_str() == name => {}
            Ok(_) => return ControlResponse::no_content(),
            Err(e) => return ControlResponse::error_internal(&e.to_string()),
        }
        if let Err(e) = self
            .meta
            .submit(Mutation::RevokeShare {
                token: token.to_owned(),
                now: self.clock.now(),
            })
            .await
        {
            return ControlResponse::error_internal(&e.to_string());
        }
        self.record_activity("RevokeShare", Some(bucket.as_str()), None, principal)
            .await;
        ControlResponse::no_content()
    }

    // -----------------------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------------------

    /// Append an audit/activity entry for a mutating endpoint (best-effort). The `actor` is the
    /// authenticated administrator's Bearer access-key id — a stable, human-recognizable identity
    /// that survives display-name changes — so the audit trail names who performed each mutation
    /// (ARCH 26.3). `None` only when no principal was threaded in (which the admin gate prevents
    /// for every mutating route).
    async fn record_activity(
        &self,
        action: &str,
        bucket: Option<&str>,
        key: Option<&str>,
        principal: Option<&Principal>,
    ) {
        let entry = cairn_types::meta::ActivityEntry {
            id: uuid::Uuid::new_v4().simple().to_string(),
            action: action.to_owned(),
            bucket: bucket.map(str::to_owned),
            key: key.map(str::to_owned),
            size: None,
            etag: None,
            actor: principal.map(|p| p.access_key_id.clone()),
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

/// Map a stored [`ShareRow`] to the wire view, deriving the `active`/`expired`/`revoked` status.
fn share_record(s: ShareRow, now: Timestamp) -> wire::ShareRecord {
    let status = if s.revoked_at.is_some() {
        "revoked"
    } else if s.expires_at.is_some_and(|e| now.as_millis() > e.0) {
        "expired"
    } else {
        "active"
    };
    wire::ShareRecord {
        token: s.token,
        bucket: s.bucket.as_str().to_owned(),
        key: s.key.as_str().to_owned(),
        version_id: s.version_id.map(|v| v.as_str().to_owned()),
        expires_at_ms: s.expires_at.map(|t| t.0),
        created_at_ms: s.created_at.0,
        created_by: s.created_by.0,
        disposition: match s.disposition {
            ShareDisposition::Attachment => "attachment",
            ShareDisposition::Inline => "inline",
        }
        .to_owned(),
        filename: s.filename,
        status: status.to_owned(),
    }
}

/// Percent-decode a single URL path segment (used for the replication-target ARN, which contains
/// colons a client may encode). Unrecognized escapes are passed through unchanged.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_owned())
}

/// Build the canned **replication** identity-policy JSON for a destination bucket: a Principal-less
/// per-user policy granting the four actions a dedicated destination credential needs to receive
/// replicated writes — `s3:ReplicateObject`, `s3:ReplicateDelete`, `s3:GetObject`, `s3:PutObject` —
/// scoped to `arn:aws:s3:::<bucket>/*` (and the bucket ARN itself, for listing) (ARCH 20.5).
///
/// The destination bucket name is validated up front (a `400` on a bad name); the produced JSON is
/// re-validated through `parse_user_policy` so a future action-spelling drift is caught here rather
/// than failing open. Returns a `500` only if that self-check unexpectedly fails.
fn replication_policy_for_bucket(bucket: &str) -> Result<String, ControlResponse> {
    let bucket = BucketName::parse(bucket).map_err(|e| {
        ControlResponse::bad_request(&format!("invalid replication_policy_bucket: {e}"))
    })?;
    let b = bucket.as_str();
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "CairnReplicationDestination",
            "Effect": "Allow",
            "Action": [
                "s3:ReplicateObject",
                "s3:ReplicateDelete",
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                format!("arn:aws:s3:::{b}"),
                format!("arn:aws:s3:::{b}/*")
            ]
        }]
    });
    let json = serde_json::to_string(&policy)
        .unwrap_or_else(|_| "{\"Version\":\"2012-10-17\",\"Statement\":[]}".to_owned());
    // Self-check: the canned document must be a valid identity policy. This guards against a future
    // action-name change silently producing a document the engine would reject.
    cairn_authz::parse_user_policy(&json)
        .map_err(|e| ControlResponse::error_internal(&format!("canned replication policy: {e}")))?;
    Ok(json)
}

/// Total and unprivileged-available bytes of the filesystem holding `path`, or `None` when the
/// platform or the syscall cannot answer. `f_frsize` is the fragment size the totals are counted
/// in; some filesystems report it as zero, in which case `f_bsize` applies.
#[cfg(unix)]
fn disk_stats(path: &std::path::Path) -> Option<(u64, u64)> {
    let v = rustix::fs::statvfs(path).ok()?;
    let frsize = if v.f_frsize > 0 {
        v.f_frsize
    } else {
        v.f_bsize
    };
    Some((v.f_blocks * frsize, v.f_bavail * frsize))
}

#[cfg(not(unix))]
fn disk_stats(_path: &std::path::Path) -> Option<(u64, u64)> {
    None
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

/// Page a bucket's current object versions and idempotently enqueue a replication-outbox entry for
/// each one a backfill-enabled rule selects (existing-object replication / resync, ARCH 20.5).
///
/// The entry id is deterministic (`backfill:{rule}:{key}:{version}`) and the enqueue is
/// `INSERT OR IGNORE`, so a repeated resync is a no-op for already-queued versions and an
/// already-completed entry is never re-shipped. Enumeration is bounded paging (metadata-only — the
/// byte transfer is the worker's job), run on a spawned task so a large bucket never blocks a
/// request. Tag predicates are honoured by loading an object's tags only when the matched rule
/// actually carries them.
/// Seconds since the oldest still-pending enqueue (`0` when nothing is pending or the time is
/// unknown — a pre-migration row). The replication-lag figure for status/summary responses.
fn replication_lag_seconds(now: cairn_types::time::Timestamp, oldest_pending_at_ms: i64) -> u64 {
    if oldest_pending_at_ms == 0 {
        0
    } else {
        u64::try_from((now.as_millis() - oldest_pending_at_ms).max(0) / 1000).unwrap_or(0)
    }
}

/// Map a store-layer per-target count into its wire DTO.
fn target_count_wire(
    t: cairn_types::meta::ReplicationTargetCounts,
) -> wire::ReplicationTargetCount {
    wire::ReplicationTargetCount {
        target_arn: t.target_arn,
        pending: t.pending,
        failed: t.failed,
    }
}

async fn backfill_replication(
    meta: Arc<dyn MetadataStore>,
    clock: Arc<dyn Clock>,
    bucket: BucketName,
    cfg: cairn_replication::ReplicationConfig,
) {
    let mut cursor: Option<String> = None;
    let mut enqueued: u64 = 0;
    loop {
        let query = ListQuery {
            prefix: None,
            delimiter: None,
            cursor: cursor.clone(),
            version_id_marker: None,
            start_after: None,
            limit: PAGE_LIMIT,
        };
        let page = match meta.list_current(&bucket, &query).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(bucket = %bucket.as_str(), error = %e,
                    "replication backfill: listing current objects failed");
                return;
            }
        };
        // Load tags only when some enabled backfill rule actually filters on them.
        let needs_tags = cfg.backfill_needs_tags();
        for summary in &page.items {
            let key = &summary.key;
            let tags = if needs_tags {
                meta.get_object_tags(&bucket, key, &summary.version_id)
                    .await
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            // Fan-out: one entry per distinct target whose highest-priority enabled
            // existing-object-replication rule matches this key+tags. The id is scoped by rule so
            // N targets never collide on one primary key, and `INSERT OR IGNORE` keeps resync
            // idempotent per target.
            for rule in cfg.backfill_rules_for_all(key.as_str(), &tags) {
                let id = format!(
                    "backfill:{}:{}:{}",
                    rule.id,
                    key.as_str(),
                    summary.version_id.as_str()
                );
                let entry = cairn_replication::outbox_entry_for(
                    id,
                    bucket.clone(),
                    key.clone(),
                    summary.version_id.clone(),
                    cairn_types::meta::ReplicationOp::ObjectCreate,
                    rule.id.clone(),
                    rule.target_arn.clone(),
                    clock.now(),
                    rule.priority,
                );
                if let Err(e) = meta
                    .submit(Mutation::EnqueueReplication(Box::new(entry)))
                    .await
                {
                    tracing::warn!(bucket = %bucket.as_str(), error = %e,
                        "replication backfill: enqueue failed");
                    return;
                }
                enqueued += 1;
            }
        }
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    tracing::info!(bucket = %bucket.as_str(), enqueued, "replication backfill complete");
}
