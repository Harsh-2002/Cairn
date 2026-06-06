//! The S3 service: dispatch and the core request lifecycles (ARCH §21) for buckets and
//! objects. Handlers depend only on the trait spine; the authorization wiring here is the
//! owner/admin baseline (the full policy/ACL/anonymous pipeline lands in Wave 3).

use crate::chunked::{ChunkDecoder, decode_stream};
use crate::error_map::error_response;
use crate::httpdate::http_date;
use crate::request::{S3Body, S3Request, S3Response};
use base64::Engine;
use cairn_types::auth::{Principal, Role};
use cairn_types::blob::{ByteRange, StageOptions};
use cairn_types::bucket::{Bucket, VersioningState};
use cairn_types::error::Error;
use cairn_types::id::VersionId;
use cairn_types::meta::{IfNoneMatch, ListQuery, Mutation, MutationOutcome, Precondition};
use cairn_types::object::{
    ChecksumAlgorithm, ChecksumSet, CompressionDescriptor, ETag, ObjectVersionRow, StorageClass,
};
use cairn_types::traits::{BlobStore, Clock, MetadataStore};
use http::{Method, StatusCode};
use std::sync::Arc;

type Result<T> = std::result::Result<T, Error>;

/// The S3 protocol service, wiring the storage backends behind the trait spine.
#[derive(Clone)]
pub struct S3Service {
    meta: Arc<dyn MetadataStore>,
    blob: Arc<dyn BlobStore>,
    clock: Arc<dyn Clock>,
    region: String,
    max_object_size: u64,
}

impl std::fmt::Debug for S3Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Service")
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

impl S3Service {
    /// Construct the service.
    pub fn new(
        meta: Arc<dyn MetadataStore>,
        blob: Arc<dyn BlobStore>,
        clock: Arc<dyn Clock>,
        region: String,
        max_object_size: u64,
    ) -> Self {
        Self {
            meta,
            blob,
            clock,
            region,
            max_object_size,
        }
    }

    /// Handle a routed request, translating any error to an S3 XML error response.
    pub async fn handle(&self, req: S3Request) -> S3Response {
        let request_id = req.request_id.clone();
        let resource = resource_path(&req);
        match self.dispatch(req).await {
            Ok(resp) => resp,
            Err(e) => error_response(&e, &resource, &request_id),
        }
    }

    async fn dispatch(&self, req: S3Request) -> Result<S3Response> {
        match (&req.method, req.bucket.is_some(), req.key.is_some()) {
            (&Method::GET, false, _) => self.list_buckets(&req).await,
            (_, true, false) => self.bucket_op(req).await,
            (_, true, true) => self.object_op(req).await,
            _ => Err(Error::NotImplemented),
        }
    }

    async fn bucket_op(&self, req: S3Request) -> Result<S3Response> {
        match req.method {
            Method::PUT => self.create_bucket(&req).await,
            Method::DELETE => self.delete_bucket(&req).await,
            Method::HEAD => self.head_bucket(&req).await,
            Method::GET if req.has_query("location") => self.get_bucket_location(&req).await,
            Method::GET => self.list_objects(&req).await,
            _ => Err(Error::NotImplemented),
        }
    }

    async fn object_op(&self, req: S3Request) -> Result<S3Response> {
        match req.method {
            Method::PUT => self.put_object(req).await,
            Method::GET => self.get_object(&req).await,
            Method::HEAD => self.head_object(&req).await,
            Method::DELETE => self.delete_object(&req).await,
            _ => Err(Error::NotImplemented),
        }
    }

    // --- service / bucket operations ---

    async fn list_buckets(&self, req: &S3Request) -> Result<S3Response> {
        let principal = self.require_principal(req)?;
        let owner = (principal.role != Role::Administrator).then(|| principal.user_id.clone());
        let buckets = self.meta.list_buckets(owner.as_ref()).await?;
        let body = cairn_xml::list_buckets(&principal.user_id.0, &principal.display_name, &buckets);
        Ok(S3Response::xml(StatusCode::OK, body).with_header("x-amz-request-id", &req.request_id))
    }

    async fn create_bucket(&self, req: &S3Request) -> Result<S3Response> {
        let principal = self.require_principal(req)?;
        let name = req.bucket.clone().expect("bucket present");
        let bucket = Bucket {
            name: name.clone(),
            owner_id: principal.user_id.clone(),
            created_at: self.clock.now(),
            versioning: VersioningState::Unversioned,
            ownership_mode: cairn_types::authz::OwnershipMode::BucketOwnerEnforced,
            region: self.region.clone(),
            compression: None,
        };
        match self
            .meta
            .submit(Mutation::CreateBucket(Box::new(bucket)))
            .await
        {
            Ok(_) => Ok(S3Response::status(StatusCode::OK)
                .with_header("location", format!("/{}", name.as_str()))
                .with_header("x-amz-request-id", &req.request_id)),
            Err(cairn_types::MetaError::Conflict) => Err(Error::BucketAlreadyOwnedByYou),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete_bucket(&self, req: &S3Request) -> Result<S3Response> {
        let (bucket, _) = self.authorized_bucket(req).await?;
        if !self.meta.is_bucket_empty(&bucket.name).await? {
            return Err(Error::BucketNotEmpty);
        }
        self.meta
            .submit(Mutation::DeleteBucket(bucket.name.clone()))
            .await?;
        Ok(S3Response::status(StatusCode::NO_CONTENT)
            .with_header("x-amz-request-id", &req.request_id))
    }

    async fn head_bucket(&self, req: &S3Request) -> Result<S3Response> {
        let _ = self.authorized_bucket(req).await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    async fn get_bucket_location(&self, req: &S3Request) -> Result<S3Response> {
        let (bucket, _) = self.authorized_bucket(req).await?;
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>",
            bucket.region
        );
        Ok(S3Response::xml(StatusCode::OK, body))
    }

    async fn list_objects(&self, req: &S3Request) -> Result<S3Response> {
        let (bucket, _) = self.authorized_bucket(req).await?;
        let v1 = req.query("list-type").map(|v| v != "2").unwrap_or(true);
        let prefix = req.query("prefix").map(str::to_owned);
        let delimiter = req.query("delimiter").map(str::to_owned);
        let max_keys = req
            .query("max-keys")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000u32)
            .min(1000);

        // The continuation token / marker is the opaque base64 of the store cursor.
        let cursor = if v1 {
            req.query("marker").map(str::to_owned)
        } else {
            req.query("continuation-token").and_then(decode_token)
        };
        let start_after = req.query("start-after").map(str::to_owned);

        let query = ListQuery {
            prefix: prefix.clone(),
            delimiter: delimiter.clone(),
            cursor,
            start_after,
            limit: max_keys,
        };
        let mut page = self.meta.list_current(&bucket.name, &query).await?;
        // Re-encode the next cursor opaquely.
        page.next_cursor = page.next_cursor.map(|c| encode_token(&c));

        let body = if v1 {
            cairn_xml::list_objects_v1(
                bucket.name.as_str(),
                prefix.as_deref(),
                delimiter.as_deref(),
                max_keys,
                &page,
                req.query("marker"),
            )
        } else {
            cairn_xml::list_objects_v2(
                bucket.name.as_str(),
                prefix.as_deref(),
                delimiter.as_deref(),
                max_keys,
                &page,
                req.query("continuation-token"),
            )
        };
        Ok(S3Response::xml(StatusCode::OK, body).with_header("x-amz-request-id", &req.request_id))
    }

    // --- object operations ---

    async fn put_object(&self, req: S3Request) -> Result<S3Response> {
        let bucket = self.authorized_bucket_owned(&req).await?;
        let key = req.key.clone().expect("key present");

        if let Some(cl) = req
            .header("content-length")
            .and_then(|s| s.parse::<u64>().ok())
        {
            if cl > self.max_object_size {
                return Err(Error::EntityTooLarge);
            }
        }
        let content_type = req
            .header("content-type")
            .unwrap_or("application/octet-stream")
            .to_owned();
        let extra = requested_checksums(&req);
        let precond = precondition(&req);
        let user_metadata = user_metadata(&req);
        let content_md5 = req.header("content-md5").map(str::to_owned);

        // De-frame SigV4 streaming bodies (the F-5 fix); plain bodies pass through.
        let streaming = req
            .header("x-amz-content-sha256")
            .map(|v| v.starts_with("STREAMING"))
            .unwrap_or(false);
        let request_id = req.request_id.clone();
        let body = if streaming {
            decode_stream(req.body, ChunkDecoder::unsigned(self.max_object_size))
        } else {
            req.body
        };

        let opts = StageOptions {
            compression: bucket.compression,
            extra_checksums: extra,
            size_ceiling: self.max_object_size,
            content_type: content_type.clone(),
        };
        let staged = self.blob.stage(&bucket.name, body, opts).await?;

        // Verify any client-supplied Content-MD5 against the computed plaintext MD5.
        if let Some(cm) = content_md5 {
            let want = base64::engine::general_purpose::STANDARD
                .decode(cm.trim())
                .map_err(|_| Error::InvalidDigest)?;
            if hex::decode(staged.md5_hex.as_bytes()).ok().as_deref() != Some(&want) {
                let _ = self.blob.delete(&staged.storage_path).await;
                return Err(Error::BadDigest);
            }
        }

        let versioned = bucket.versioning == VersioningState::Enabled;
        let version_id = if versioned {
            VersionId::generate()
        } else {
            VersionId::null()
        };
        let now = self.clock.now();
        let row = ObjectVersionRow {
            id: uuid::Uuid::new_v4().simple().to_string(),
            bucket: bucket.name.clone(),
            key: key.clone(),
            version_id: version_id.clone(),
            is_latest: true,
            is_delete_marker: false,
            size_logical: staged.size_logical,
            size_physical: staged.size_physical,
            etag: staged.etag.clone(),
            content_type,
            storage_path: Some(staged.storage_path.clone()),
            compression: staged.compression.clone(),
            storage_class: StorageClass::Standard,
            cold_locator: None,
            owner_id: bucket.owner_id.clone(),
            user_metadata,
            acl: None,
            checksums: staged.checksums.clone(),
            replication_status: None,
            created_at: now,
            updated_at: now,
        };

        match self
            .meta
            .submit(Mutation::PutObjectVersion {
                row: Box::new(row),
                precondition: precond,
                replication: None,
            })
            .await
        {
            Ok(MutationOutcome::Put {
                superseded,
                version_id,
            }) => {
                if let Some(old) = superseded {
                    let _ = self.blob.delete(&old).await;
                }
                let mut resp = S3Response::status(StatusCode::OK)
                    .with_header("etag", quoted(&staged.etag))
                    .with_header("x-amz-request-id", &request_id);
                if versioned {
                    resp = resp.with_header("x-amz-version-id", version_id.as_str());
                }
                Ok(resp)
            }
            Ok(_) => {
                let _ = self.blob.delete(&staged.storage_path).await;
                Err(Error::Internal("unexpected put outcome".to_owned()))
            }
            Err(e) => {
                let _ = self.blob.delete(&staged.storage_path).await;
                Err(e.into())
            }
        }
    }

    async fn get_object(&self, req: &S3Request) -> Result<S3Response> {
        let (row, _bucket) = self.resolve_object(req).await?;
        if let Some(resp) = conditional_short_circuit(req, &row) {
            return Ok(resp);
        }
        let range = parse_range(req.header("range"), row.size_logical)?;
        let storage = row.storage_path.clone().ok_or(Error::NoSuchKey)?;
        let handle = self.blob.open(&storage, range).await?;
        let status = if handle.content_range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };

        let mut resp = S3Response {
            status,
            headers: Vec::new(),
            body: S3Body::Stream {
                length: handle.logical_len,
                stream: handle.body,
            },
        };
        resp = object_headers(resp, &row)
            .with_header("content-length", handle.logical_len.to_string());
        if let Some(cr) = handle.content_range {
            resp = resp.with_header(
                "content-range",
                format!("bytes {}-{}/{}", cr.start, cr.end, cr.total),
            );
        }
        Ok(resp.with_header("x-amz-request-id", &req.request_id))
    }

    async fn head_object(&self, req: &S3Request) -> Result<S3Response> {
        let (row, _bucket) = self.resolve_object(req).await?;
        let resp = object_headers(S3Response::status(StatusCode::OK), &row)
            .with_header("content-length", row.size_logical.to_string())
            .with_header("x-amz-request-id", &req.request_id);
        Ok(resp)
    }

    async fn delete_object(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.authorized_bucket_owned(req).await?;
        let key = req.key.clone().expect("key present");
        let now = self.clock.now();

        let outcome = if let Some(vid) = req.query("versionId") {
            self.meta
                .submit(Mutation::DeleteVersion {
                    bucket: bucket.name.clone(),
                    key,
                    version_id: VersionId::from_string(vid.to_owned()),
                })
                .await?
        } else if bucket.versioning == VersioningState::Enabled {
            self.meta
                .submit(Mutation::CreateDeleteMarker {
                    bucket: bucket.name.clone(),
                    key,
                    version_id: VersionId::generate(),
                    owner_id: bucket.owner_id.clone(),
                    now,
                    replication: None,
                })
                .await?
        } else {
            self.meta
                .submit(Mutation::DeleteVersion {
                    bucket: bucket.name.clone(),
                    key,
                    version_id: VersionId::null(),
                })
                .await?
        };
        if let MutationOutcome::Deleted {
            freed: Some(path), ..
        } = outcome
        {
            let _ = self.blob.delete(&path).await;
        }
        Ok(S3Response::status(StatusCode::NO_CONTENT)
            .with_header("x-amz-request-id", &req.request_id))
    }

    // --- helpers ---

    fn require_principal<'a>(&self, req: &'a S3Request) -> Result<&'a Principal> {
        req.principal.as_ref().ok_or(Error::AccessDenied)
    }

    /// Fetch the bucket and apply the owner/admin authorization baseline.
    async fn authorized_bucket(&self, req: &S3Request) -> Result<(Bucket, Principal)> {
        let principal = self.require_principal(req)?.clone();
        let name = req.bucket.clone().expect("bucket present");
        let bucket = self
            .meta
            .get_bucket(&name)
            .await?
            .ok_or(Error::NoSuchBucket)?;
        if principal.role != Role::Administrator && principal.user_id != bucket.owner_id {
            return Err(Error::AccessDenied);
        }
        Ok((bucket, principal))
    }

    async fn authorized_bucket_owned(&self, req: &S3Request) -> Result<Bucket> {
        Ok(self.authorized_bucket(req).await?.0)
    }

    /// Resolve the current (or hidden-by-delete-marker) object version for a read.
    async fn resolve_object(&self, req: &S3Request) -> Result<(ObjectVersionRow, Bucket)> {
        let (bucket, _) = self.authorized_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        let row = match req.query("versionId") {
            Some(vid) => self
                .meta
                .get_version(&bucket.name, &key, &VersionId::from_string(vid.to_owned()))
                .await?
                .ok_or(Error::NoSuchVersion)?,
            None => self
                .meta
                .current_version(&bucket.name, &key)
                .await?
                .ok_or(Error::NoSuchKey)?,
        };
        if row.is_delete_marker {
            return Err(Error::NoSuchKey);
        }
        Ok((row, bucket))
    }
}

fn resource_path(req: &S3Request) -> String {
    match (&req.bucket, &req.key) {
        (Some(b), Some(k)) => format!("/{}/{}", b.as_str(), k.as_str()),
        (Some(b), None) => format!("/{}", b.as_str()),
        _ => "/".to_owned(),
    }
}

fn quoted(etag: &ETag) -> String {
    format!("\"{}\"", etag.as_str())
}

fn object_headers(resp: S3Response, row: &ObjectVersionRow) -> S3Response {
    let mut resp = resp
        .with_header("etag", quoted(&row.etag))
        .with_header("content-type", row.content_type.clone())
        .with_header("last-modified", http_date(row.updated_at))
        .with_header("accept-ranges", "bytes");
    if !row.version_id.is_null() {
        resp = resp.with_header("x-amz-version-id", row.version_id.as_str());
    }
    if let CompressionDescriptor::Compressed { .. } = row.compression {
        // The physical form is hidden from clients; nothing leaks here.
    }
    for (k, v) in &row.user_metadata {
        resp = resp.with_header(&format!("x-amz-meta-{k}"), v.clone());
    }
    resp
}

/// For GET: return a 304/412 short-circuit when conditional headers dictate it.
fn conditional_short_circuit(req: &S3Request, row: &ObjectVersionRow) -> Option<S3Response> {
    let etag = row.etag.as_str();
    if let Some(inm) = req.header("if-none-match") {
        if inm.trim() == "*" || inm.split(',').any(|t| t.trim().trim_matches('"') == etag) {
            return Some(S3Response::status(StatusCode::NOT_MODIFIED));
        }
    }
    if let Some(im) = req.header("if-match") {
        if im.trim() != "*" && !im.split(',').any(|t| t.trim().trim_matches('"') == etag) {
            return Some(S3Response::status(StatusCode::PRECONDITION_FAILED));
        }
    }
    None
}

fn precondition(req: &S3Request) -> Precondition {
    let if_match = req
        .header("if-match")
        .map(|v| ETag::from_string(v.trim().trim_matches('"').to_owned()));
    let if_none_match = req.header("if-none-match").map(|v| {
        let v = v.trim();
        if v == "*" {
            IfNoneMatch::Any
        } else {
            IfNoneMatch::ETag(ETag::from_string(v.trim_matches('"').to_owned()))
        }
    });
    Precondition {
        if_match,
        if_none_match,
    }
}

fn parse_range(header: Option<&str>, total: u64) -> Result<Option<ByteRange>> {
    let Some(h) = header else { return Ok(None) };
    let spec = h.trim().strip_prefix("bytes=").ok_or(Error::InvalidRange)?;
    let (start_s, end_s) = spec.split_once('-').ok_or(Error::InvalidRange)?;
    let (offset, length) = match (start_s.trim(), end_s.trim()) {
        ("", "") => return Err(Error::InvalidRange),
        ("", suffix) => {
            let n: u64 = suffix.parse().map_err(|_| Error::InvalidRange)?;
            let n = n.min(total);
            (total - n, n)
        }
        (start, "") => {
            let s: u64 = start.parse().map_err(|_| Error::InvalidRange)?;
            if s >= total {
                return Err(Error::InvalidRange);
            }
            (s, total - s)
        }
        (start, end) => {
            let s: u64 = start.parse().map_err(|_| Error::InvalidRange)?;
            let e: u64 = end.parse().map_err(|_| Error::InvalidRange)?;
            if s > e || s >= total {
                return Err(Error::InvalidRange);
            }
            (s, (e.min(total - 1) - s) + 1)
        }
    };
    Ok(Some(ByteRange { offset, length }))
}

fn requested_checksums(req: &S3Request) -> ChecksumSet {
    let mut algos = Vec::new();
    if let Some(a) = req.header("x-amz-sdk-checksum-algorithm") {
        if let Some(alg) = checksum_algo(a) {
            algos.push(alg);
        }
    }
    for (k, _) in &req.headers {
        if let Some(name) = k.strip_prefix("x-amz-checksum-") {
            if let Some(alg) = checksum_algo(name) {
                if !algos.contains(&alg) {
                    algos.push(alg);
                }
            }
        }
    }
    ChecksumSet(algos)
}

fn checksum_algo(name: &str) -> Option<ChecksumAlgorithm> {
    match name.to_ascii_lowercase().as_str() {
        "crc32" => Some(ChecksumAlgorithm::Crc32),
        "crc32c" => Some(ChecksumAlgorithm::Crc32c),
        "sha1" => Some(ChecksumAlgorithm::Sha1),
        "sha256" => Some(ChecksumAlgorithm::Sha256),
        _ => None,
    }
}

fn user_metadata(req: &S3Request) -> Vec<(String, String)> {
    req.headers
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix("x-amz-meta-")
                .map(|n| (n.to_owned(), v.clone()))
        })
        .collect()
}

fn encode_token(cursor: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(cursor)
}

fn decode_token(token: &str) -> Option<String> {
    base64::engine::general_purpose::STANDARD
        .decode(token)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
}
