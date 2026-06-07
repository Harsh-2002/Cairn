//! The S3 service: dispatch and the core request lifecycles (ARCH §21) for buckets and
//! objects. Handlers depend only on the trait spine; the authorization wiring here is the
//! owner/admin baseline (the full policy/ACL/anonymous pipeline lands in Wave 3).

use crate::chunked::{ChunkDecoder, decode_stream};
use crate::error_map::error_response;
use crate::httpdate::http_date;
use crate::request::{S3Body, S3Request, S3Response};
use base64::Engine;
use cairn_types::auth::{Principal, RequesterClass, Role};
use cairn_types::authz::{
    Action, AuthzInput, Decision, PublicAccessBlock, RequestContext, Resource,
};
use cairn_types::blob::{ByteRange, PartRef, StageOptions};
use cairn_types::bucket::{Bucket, ConfigAspect, ConfigDoc, VersioningState};
use cairn_types::error::Error;
use cairn_types::id::{BucketName, ObjectKey, UploadId, VersionId};
use cairn_types::meta::{
    ClaimOutcome, IfNoneMatch, ListQuery, MultipartSession, Mutation, MutationOutcome, Precondition,
};
use cairn_types::object::{
    ChecksumAlgorithm, ChecksumSet, CompressionDescriptor, ETag, ObjectVersionRow, StorageClass,
};
use cairn_types::traits::{AuthorizationEngine, BlobStore, Clock, MetadataStore};
use http::{Method, StatusCode};
use std::sync::Arc;

type Result<T> = std::result::Result<T, Error>;

/// The S3 protocol service, wiring the storage backends behind the trait spine.
#[derive(Clone)]
pub struct S3Service {
    meta: Arc<dyn MetadataStore>,
    blob: Arc<dyn BlobStore>,
    authz: Arc<dyn AuthorizationEngine>,
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
        authz: Arc<dyn AuthorizationEngine>,
        clock: Arc<dyn Clock>,
        region: String,
        max_object_size: u64,
    ) -> Self {
        Self {
            meta,
            blob,
            authz,
            clock,
            region,
            max_object_size,
        }
    }

    /// Handle a routed request, translating any error to an S3 XML error response. The body is
    /// passed separately from the request head so the head stays `Sync` for borrowing across
    /// awaits; only body-consuming operations (object PUT) receive it.
    pub async fn handle(&self, req: S3Request, body: cairn_types::BodyStream) -> S3Response {
        let request_id = req.request_id.clone();
        let resource = resource_path(&req);
        match self.dispatch(req, body).await {
            Ok(resp) => resp,
            Err(e) => error_response(&e, &resource, &request_id),
        }
    }

    async fn dispatch(&self, req: S3Request, body: cairn_types::BodyStream) -> Result<S3Response> {
        match (&req.method, req.bucket.is_some(), req.key.is_some()) {
            (&Method::GET, false, _) => self.list_buckets(&req).await,
            (_, true, false) => self.bucket_op(req, body).await,
            (_, true, true) => self.object_op(req, body).await,
            _ => Err(Error::NotImplemented),
        }
    }

    async fn bucket_op(&self, req: S3Request, body: cairn_types::BodyStream) -> Result<S3Response> {
        // Authorize centrally: map the operation to an action, then evaluate the engine.
        let action = bucket_action(&req)?;
        if action == Action::CreateBucket {
            self.require_principal(&req)?;
        } else {
            let bucket = self.fetch_bucket(&req).await?;
            self.authorize(&req, &bucket, action, Resource::Bucket(bucket.name.clone()))
                .await?;
        }
        match req.method {
            // Subresources first (they share the bucket path with the plain operations).
            Method::GET if req.has_query("location") => self.get_bucket_location(&req).await,
            Method::GET if req.has_query("uploads") => self.list_multipart_uploads(&req).await,
            Method::GET if req.has_query("versions") => self.list_object_versions(&req).await,
            Method::GET if req.has_query("versioning") => self.get_bucket_versioning(&req).await,
            Method::PUT if req.has_query("versioning") => {
                self.put_bucket_versioning(req, body).await
            }
            Method::GET if req.has_query("tagging") => {
                self.get_bucket_doc(&req, ConfigAspect::Tagging, "NoSuchTagSet")
                    .await
            }
            Method::PUT if req.has_query("tagging") => {
                self.put_bucket_config(req, body, ConfigAspect::Tagging)
                    .await
            }
            Method::DELETE if req.has_query("tagging") => {
                self.clear_bucket_config(&req, ConfigAspect::Tagging).await
            }
            Method::GET if req.has_query("cors") => {
                self.get_bucket_doc(&req, ConfigAspect::Cors, "NoSuchCORSConfiguration")
                    .await
            }
            Method::PUT if req.has_query("cors") => {
                self.put_bucket_config(req, body, ConfigAspect::Cors).await
            }
            Method::DELETE if req.has_query("cors") => {
                self.clear_bucket_config(&req, ConfigAspect::Cors).await
            }
            Method::GET if req.has_query("policy") => {
                self.get_bucket_doc(&req, ConfigAspect::Policy, "NoSuchBucketPolicy")
                    .await
            }
            Method::PUT if req.has_query("policy") => {
                self.put_bucket_config(req, body, ConfigAspect::Policy)
                    .await
            }
            Method::DELETE if req.has_query("policy") => {
                self.clear_bucket_config(&req, ConfigAspect::Policy).await
            }
            Method::GET if req.has_query("lifecycle") => {
                self.get_bucket_doc(
                    &req,
                    ConfigAspect::Lifecycle,
                    "NoSuchLifecycleConfiguration",
                )
                .await
            }
            Method::PUT if req.has_query("lifecycle") => {
                self.put_bucket_config(req, body, ConfigAspect::Lifecycle)
                    .await
            }
            Method::DELETE if req.has_query("lifecycle") => {
                self.clear_bucket_config(&req, ConfigAspect::Lifecycle)
                    .await
            }
            Method::GET if req.has_query("replication") => {
                self.get_bucket_doc(
                    &req,
                    ConfigAspect::Replication,
                    "ReplicationConfigurationNotFoundError",
                )
                .await
            }
            Method::PUT if req.has_query("replication") => {
                self.put_bucket_config(req, body, ConfigAspect::Replication)
                    .await
            }
            Method::DELETE if req.has_query("replication") => {
                self.clear_bucket_config(&req, ConfigAspect::Replication)
                    .await
            }
            Method::PUT => self.create_bucket(&req).await,
            Method::DELETE => self.delete_bucket(&req).await,
            Method::HEAD => self.head_bucket(&req).await,
            Method::POST if req.has_query("delete") => self.delete_objects(&req, body).await,
            Method::GET => self.list_objects(&req).await,
            _ => Err(Error::NotImplemented),
        }
    }

    async fn object_op(&self, req: S3Request, body: cairn_types::BodyStream) -> Result<S3Response> {
        // Authorize centrally against the object resource.
        let action = object_action(&req)?;
        let bucket = self.fetch_bucket(&req).await?;
        let key = req.key.clone().ok_or(Error::NoSuchKey)?;
        self.authorize(
            &req,
            &bucket,
            action,
            Resource::Object {
                bucket: bucket.name.clone(),
                key,
            },
        )
        .await?;
        match req.method {
            Method::PUT if req.has_query("uploadId") && req.query("partNumber").is_some() => {
                self.upload_part(req, body).await
            }
            Method::PUT if req.header("x-amz-copy-source").is_some() => {
                self.copy_object(&req).await
            }
            Method::PUT if req.has_query("tagging") => self.put_object_tagging(req, body).await,
            Method::GET if req.has_query("tagging") => self.get_object_tagging(&req).await,
            Method::DELETE if req.has_query("tagging") => self.delete_object_tagging(&req).await,
            Method::PUT => self.put_object(req, body).await,
            Method::POST if req.has_query("uploads") => self.create_multipart(&req).await,
            Method::POST if req.has_query("uploadId") => self.complete_multipart(req, body).await,
            Method::GET if req.has_query("uploadId") => self.list_parts(&req).await,
            Method::GET => self.get_object(&req).await,
            Method::HEAD => self.head_object(&req).await,
            Method::DELETE if req.has_query("uploadId") => self.abort_multipart(&req).await,
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
        let bucket = self.fetch_bucket(req).await?;
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
        let _ = self.fetch_bucket(req).await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    async fn get_bucket_location(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>",
            bucket.region
        );
        Ok(S3Response::xml(StatusCode::OK, body))
    }

    async fn list_objects(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
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

    async fn put_object(
        &self,
        req: S3Request,
        raw_body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
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
            decode_stream(raw_body, ChunkDecoder::unsigned(self.max_object_size))
        } else {
            raw_body
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
        let bucket = self.fetch_bucket(req).await?;
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

    // --- multipart ---

    async fn create_multipart(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        let upload_id = UploadId::generate();
        let now = self.clock.now();
        let session = MultipartSession {
            upload_id: upload_id.clone(),
            bucket: bucket.name.clone(),
            key: key.clone(),
            content_type: req
                .header("content-type")
                .unwrap_or("application/octet-stream")
                .to_owned(),
            status: cairn_types::meta::MultipartStatus::Active,
            owner_id: bucket.owner_id.clone(),
            intended_acl: None,
            user_metadata: user_metadata(req),
            created_at: now,
            updated_at: now,
        };
        self.meta
            .submit(Mutation::CreateMultipart(Box::new(session)))
            .await?;
        let body = cairn_xml::initiate_multipart_result(
            bucket.name.as_str(),
            key.as_str(),
            upload_id.as_str(),
        );
        Ok(S3Response::xml(StatusCode::OK, body).with_header("x-amz-request-id", &req.request_id))
    }

    async fn upload_part(
        &self,
        req: S3Request,
        raw_body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let _bucket = self.fetch_bucket(&req).await?;
        let upload_id = UploadId::from_string(req.query("uploadId").unwrap_or_default().to_owned());
        let part_number: u16 = req
            .query("partNumber")
            .and_then(|s| s.parse().ok())
            .filter(|n| (1..=10_000).contains(n))
            .ok_or_else(|| Error::InvalidArgument("partNumber out of range".to_owned()))?;
        if self.meta.get_multipart(&upload_id).await?.is_none() {
            return Err(Error::NoSuchUpload);
        }
        let streaming = req
            .header("x-amz-content-sha256")
            .map(|v| v.starts_with("STREAMING"))
            .unwrap_or(false);
        let body = if streaming {
            decode_stream(raw_body, ChunkDecoder::unsigned(self.max_object_size))
        } else {
            raw_body
        };
        let staged = self
            .blob
            .stage_part(&upload_id, part_number, body, self.max_object_size)
            .await?;
        let part = cairn_types::meta::PartRecord {
            part_number,
            size: staged.size,
            etag: staged.md5_hex.clone(),
            storage_path: staged.storage_path.clone(),
            checksum: None,
        };
        if let MutationOutcome::PartRecorded {
            superseded: Some(old),
        } = self
            .meta
            .submit(Mutation::RecordPart { upload_id, part })
            .await?
        {
            let _ = self.blob.delete(&old).await;
        }
        Ok(S3Response::status(StatusCode::OK)
            .with_header("etag", format!("\"{}\"", staged.md5_hex))
            .with_header("x-amz-request-id", &req.request_id))
    }

    async fn complete_multipart(
        &self,
        req: S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
        let key = req.key.clone().expect("key present");
        let upload_id = UploadId::from_string(req.query("uploadId").unwrap_or_default().to_owned());
        let xml = drain_body(body, 8 * 1024 * 1024).await?;
        let requested = cairn_xml::parse_complete_multipart(&xml)?;
        if requested.is_empty() {
            return Err(Error::InvalidArgument("no parts specified".to_owned()));
        }

        let session = match self
            .meta
            .submit(Mutation::ClaimMultipart(upload_id.clone()))
            .await?
        {
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(s)) => *s,
            _ => return Err(Error::NoSuchUpload),
        };

        let stored: std::collections::HashMap<u16, cairn_types::meta::PartRecord> = self
            .meta
            .list_parts(&upload_id, 0, 10_000)
            .await?
            .items
            .into_iter()
            .map(|p| (p.part_number, p))
            .collect();

        let mut refs = Vec::with_capacity(requested.len());
        let mut part_md5s = Vec::with_capacity(requested.len());
        let mut last_pn = 0u16;
        for (i, (pn, etag)) in requested.iter().enumerate() {
            if *pn <= last_pn {
                return Err(Error::InvalidArgument(
                    "parts not in ascending order".to_owned(),
                ));
            }
            last_pn = *pn;
            let rec = stored.get(pn).ok_or(Error::NoSuchUpload)?;
            if strip_quotes(etag) != rec.etag {
                return Err(Error::InvalidArgument(format!("part {pn} etag mismatch")));
            }
            if i + 1 < requested.len() && rec.size < 5 * 1024 * 1024 {
                return Err(Error::InvalidArgument(format!(
                    "part {pn} smaller than 5 MiB"
                )));
            }
            refs.push(PartRef {
                part_number: *pn,
                storage_path: rec.storage_path.clone(),
                size: rec.size,
            });
            part_md5s.push(rec.etag.clone());
        }

        let opts = StageOptions {
            compression: bucket.compression,
            extra_checksums: ChecksumSet::none(),
            size_ceiling: self.max_object_size,
            content_type: session.content_type.clone(),
        };
        let staged = self.blob.assemble(&bucket.name, &refs, opts).await?;
        let etag = multipart_etag(&part_md5s);

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
            etag: etag.clone(),
            content_type: session.content_type.clone(),
            storage_path: Some(staged.storage_path.clone()),
            compression: staged.compression.clone(),
            storage_class: StorageClass::Standard,
            cold_locator: None,
            owner_id: bucket.owner_id.clone(),
            user_metadata: session.user_metadata.clone(),
            acl: None,
            checksums: Vec::new(),
            replication_status: None,
            created_at: now,
            updated_at: now,
        };

        match self
            .meta
            .submit(Mutation::CompleteMultipart {
                upload_id: upload_id.clone(),
                row: Box::new(row),
                precondition: Precondition::default(),
                replication: None,
            })
            .await
        {
            Ok(MutationOutcome::MultipartCompleted { superseded, .. }) => {
                if let Some(old) = superseded {
                    let _ = self.blob.delete(&old).await;
                }
                let _ = self.blob.delete_session(&upload_id).await;
                let location = format!("/{}/{}", bucket.name.as_str(), key.as_str());
                let body = cairn_xml::complete_multipart_result(
                    &location,
                    bucket.name.as_str(),
                    key.as_str(),
                    &etag,
                );
                let mut resp = S3Response::xml(StatusCode::OK, body)
                    .with_header("x-amz-request-id", &req.request_id);
                if versioned {
                    resp = resp.with_header("x-amz-version-id", version_id.as_str());
                }
                Ok(resp)
            }
            Ok(_) => Err(Error::Internal("unexpected completion outcome".to_owned())),
            Err(e) => {
                let _ = self.blob.delete(&staged.storage_path).await;
                Err(e.into())
            }
        }
    }

    async fn abort_multipart(&self, req: &S3Request) -> Result<S3Response> {
        let _bucket = self.fetch_bucket(req).await?;
        let upload_id = UploadId::from_string(req.query("uploadId").unwrap_or_default().to_owned());
        self.meta
            .submit(Mutation::AbortMultipart(upload_id.clone()))
            .await?;
        let _ = self.blob.delete_session(&upload_id).await;
        Ok(S3Response::status(StatusCode::NO_CONTENT)
            .with_header("x-amz-request-id", &req.request_id))
    }

    async fn list_parts(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        let upload_id = UploadId::from_string(req.query("uploadId").unwrap_or_default().to_owned());
        if self.meta.get_multipart(&upload_id).await?.is_none() {
            return Err(Error::NoSuchUpload);
        }
        let marker: u16 = req
            .query("part-number-marker")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let max: u32 = req
            .query("max-parts")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000)
            .min(1000);
        let page = self.meta.list_parts(&upload_id, marker, max).await?;
        let body = cairn_xml::list_parts_result(
            bucket.name.as_str(),
            key.as_str(),
            upload_id.as_str(),
            &page,
            &bucket.owner_id.0,
            marker,
            max,
        );
        Ok(S3Response::xml(StatusCode::OK, body).with_header("x-amz-request-id", &req.request_id))
    }

    async fn list_multipart_uploads(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let prefix = req.query("prefix").map(str::to_owned);
        let max: u32 = req
            .query("max-uploads")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000)
            .min(1000);
        let query = ListQuery {
            prefix: prefix.clone(),
            limit: max,
            ..Default::default()
        };
        let page = self
            .meta
            .list_multipart_uploads(&bucket.name, &query)
            .await?;
        let body = cairn_xml::list_multipart_uploads_result(
            bucket.name.as_str(),
            prefix.as_deref(),
            None,
            &page,
            None,
            None,
            max,
        );
        Ok(S3Response::xml(StatusCode::OK, body).with_header("x-amz-request-id", &req.request_id))
    }

    // --- copy & bulk delete ---

    async fn copy_object(&self, req: &S3Request) -> Result<S3Response> {
        let dest_bucket = self.fetch_bucket(req).await?;
        let dest_key = req.key.clone().expect("key present");
        let raw_source = req.header("x-amz-copy-source").unwrap_or_default();
        let (src_bucket_s, src_key_s, src_version) = parse_copy_source(raw_source)
            .ok_or_else(|| Error::InvalidArgument("bad copy source".to_owned()))?;
        let src_bucket = BucketName::parse(&src_bucket_s)?;
        let src_key = ObjectKey::parse(&src_key_s)?;

        let src_row = match src_version {
            Some(v) => self
                .meta
                .get_version(&src_bucket, &src_key, &VersionId::from_string(v))
                .await?
                .ok_or(Error::NoSuchVersion)?,
            None => self
                .meta
                .current_version(&src_bucket, &src_key)
                .await?
                .ok_or(Error::NoSuchKey)?,
        };
        if src_row.is_delete_marker {
            return Err(Error::NoSuchKey);
        }
        let src_path = src_row.storage_path.clone().ok_or(Error::NoSuchKey)?;

        let replace = req
            .header("x-amz-metadata-directive")
            .map(|d| d.eq_ignore_ascii_case("REPLACE"))
            .unwrap_or(false);
        let content_type = if replace {
            req.header("content-type")
                .unwrap_or("application/octet-stream")
                .to_owned()
        } else {
            src_row.content_type.clone()
        };
        let user_meta = if replace {
            user_metadata(req)
        } else {
            src_row.user_metadata.clone()
        };

        let handle = self.blob.open(&src_path, None).await?;
        // Re-tag the blob read errors as body errors so the source can feed `stage`.
        let src_stream: cairn_types::BodyStream =
            {
                use futures_util::StreamExt;
                Box::pin(handle.body.map(|r| {
                    r.map_err(|e| cairn_types::error::BodyError::Transport(e.to_string()))
                }))
            };
        let opts = StageOptions {
            compression: dest_bucket.compression,
            extra_checksums: ChecksumSet::none(),
            size_ceiling: self.max_object_size,
            content_type: content_type.clone(),
        };
        let staged = self.blob.stage(&dest_bucket.name, src_stream, opts).await?;

        let versioned = dest_bucket.versioning == VersioningState::Enabled;
        let version_id = if versioned {
            VersionId::generate()
        } else {
            VersionId::null()
        };
        let now = self.clock.now();
        let row = ObjectVersionRow {
            id: uuid::Uuid::new_v4().simple().to_string(),
            bucket: dest_bucket.name.clone(),
            key: dest_key,
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
            owner_id: dest_bucket.owner_id.clone(),
            user_metadata: user_meta,
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
                precondition: Precondition::default(),
                replication: None,
            })
            .await
        {
            Ok(MutationOutcome::Put { superseded, .. }) => {
                if let Some(old) = superseded {
                    let _ = self.blob.delete(&old).await;
                }
                let body = cairn_xml::copy_object_result(&staged.etag, now);
                let mut resp = S3Response::xml(StatusCode::OK, body)
                    .with_header("x-amz-request-id", &req.request_id);
                if versioned {
                    resp = resp.with_header("x-amz-version-id", version_id.as_str());
                }
                Ok(resp)
            }
            Ok(_) | Err(_) => {
                let _ = self.blob.delete(&staged.storage_path).await;
                Err(Error::Internal("copy commit failed".to_owned()))
            }
        }
    }

    async fn delete_objects(
        &self,
        req: &S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let xml = drain_body(body, 8 * 1024 * 1024).await?;
        let (quiet, keys) = cairn_xml::parse_delete(&xml)?;
        let versioned = bucket.versioning == VersioningState::Enabled;
        let now = self.clock.now();

        let mut deleted: Vec<(String, Option<String>)> = Vec::new();
        let mut errors: Vec<(String, String, String)> = Vec::new();
        for (key_s, version) in keys {
            let Ok(key) = ObjectKey::parse(&key_s) else {
                errors.push((
                    key_s,
                    "InvalidArgument".to_owned(),
                    "invalid key".to_owned(),
                ));
                continue;
            };
            let mutation = match (&version, versioned) {
                (Some(v), _) => Mutation::DeleteVersion {
                    bucket: bucket.name.clone(),
                    key,
                    version_id: VersionId::from_string(v.clone()),
                },
                (None, true) => Mutation::CreateDeleteMarker {
                    bucket: bucket.name.clone(),
                    key,
                    version_id: VersionId::generate(),
                    owner_id: bucket.owner_id.clone(),
                    now,
                    replication: None,
                },
                (None, false) => Mutation::DeleteVersion {
                    bucket: bucket.name.clone(),
                    key,
                    version_id: VersionId::null(),
                },
            };
            match self.meta.submit(mutation).await {
                Ok(MutationOutcome::Deleted { freed, .. }) => {
                    if let Some(p) = freed {
                        let _ = self.blob.delete(&p).await;
                    }
                    if !quiet {
                        deleted.push((key_s, version));
                    }
                }
                Ok(_) => {
                    if !quiet {
                        deleted.push((key_s, version));
                    }
                }
                Err(e) => errors.push((key_s, "InternalError".to_owned(), e.to_string())),
            }
        }
        let body = cairn_xml::delete_result(&deleted, &errors);
        Ok(S3Response::xml(StatusCode::OK, body).with_header("x-amz-request-id", &req.request_id))
    }

    // --- versioning ---

    async fn get_bucket_versioning(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        Ok(S3Response::xml(
            StatusCode::OK,
            cairn_xml::versioning_configuration(bucket.versioning),
        )
        .with_header("x-amz-request-id", &req.request_id))
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
        let doc = drain_body(body, 64 * 1024).await?;
        let state = cairn_xml::parse_versioning_configuration(&doc)?;
        self.meta
            .submit(Mutation::SetVersioning {
                bucket: bucket.name,
                state,
            })
            .await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    async fn list_object_versions(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let prefix = req.query("prefix").map(str::to_owned);
        let delimiter = req.query("delimiter").map(str::to_owned);
        let max = req
            .query("max-keys")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000u32)
            .min(1000);
        let cursor = req.query("key-marker").and_then(decode_token);
        let query = ListQuery {
            prefix: prefix.clone(),
            delimiter: delimiter.clone(),
            cursor,
            start_after: None,
            limit: max,
        };
        let mut page = self.meta.list_versions(&bucket.name, &query).await?;
        page.next_cursor = page.next_cursor.map(|c| encode_token(&c));
        let body = cairn_xml::list_object_versions(
            bucket.name.as_str(),
            prefix.as_deref(),
            delimiter.as_deref(),
            max,
            &page,
            req.query("key-marker"),
            req.query("version-id-marker"),
        );
        Ok(S3Response::xml(StatusCode::OK, body).with_header("x-amz-request-id", &req.request_id))
    }

    // --- bucket configuration documents (tagging, cors, policy) ---

    async fn get_bucket_doc(
        &self,
        req: &S3Request,
        aspect: ConfigAspect,
        not_found_code: &str,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        match self.meta.get_bucket_config(&bucket.name, aspect).await? {
            Some(doc) => Ok(S3Response::xml(StatusCode::OK, doc.0)
                .with_header("x-amz-request-id", &req.request_id)),
            None => Ok(S3Response::xml(
                StatusCode::NOT_FOUND,
                cairn_xml::error_document(
                    not_found_code,
                    "The requested configuration does not exist",
                    &resource_path(req),
                    &req.request_id,
                ),
            )),
        }
    }

    async fn put_bucket_config(
        &self,
        req: S3Request,
        body: cairn_types::BodyStream,
        aspect: ConfigAspect,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
        let doc = drain_body(body, 1024 * 1024).await?;
        let text = String::from_utf8_lossy(&doc).into_owned();
        // Validate per aspect before storing.
        match aspect {
            ConfigAspect::Policy => {
                cairn_authz::parse_policy(&text)?;
            }
            ConfigAspect::Cors => {
                cairn_xml::parse_cors_configuration(&doc)?;
            }
            ConfigAspect::Tagging => {
                cairn_xml::parse_tagging(&doc)?;
            }
            _ => {}
        }
        self.meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket.name,
                aspect,
                doc: Some(ConfigDoc(text)),
            })
            .await?;
        Ok(S3Response::status(StatusCode::NO_CONTENT)
            .with_header("x-amz-request-id", &req.request_id))
    }

    async fn clear_bucket_config(
        &self,
        req: &S3Request,
        aspect: ConfigAspect,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        self.meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket.name,
                aspect,
                doc: None,
            })
            .await?;
        Ok(S3Response::status(StatusCode::NO_CONTENT)
            .with_header("x-amz-request-id", &req.request_id))
    }

    // --- object tagging ---

    async fn get_object_tagging(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        let row = self
            .meta
            .current_version(&bucket.name, &key)
            .await?
            .ok_or(Error::NoSuchKey)?;
        let tags = self
            .meta
            .get_object_tags(&bucket.name, &key, &row.version_id)
            .await?;
        Ok(S3Response::xml(StatusCode::OK, cairn_xml::tagging(&tags))
            .with_header("x-amz-request-id", &req.request_id))
    }

    async fn put_object_tagging(
        &self,
        req: S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
        let key = req.key.clone().expect("key present");
        let row = self
            .meta
            .current_version(&bucket.name, &key)
            .await?
            .ok_or(Error::NoSuchKey)?;
        let doc = drain_body(body, 64 * 1024).await?;
        let tags = cairn_xml::parse_tagging(&doc)?;
        self.meta
            .submit(Mutation::PutObjectTags {
                bucket: bucket.name,
                key,
                version_id: row.version_id,
                tags,
            })
            .await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    async fn delete_object_tagging(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        let row = self
            .meta
            .current_version(&bucket.name, &key)
            .await?
            .ok_or(Error::NoSuchKey)?;
        self.meta
            .submit(Mutation::DeleteObjectTags {
                bucket: bucket.name,
                key,
                version_id: row.version_id,
            })
            .await?;
        Ok(S3Response::status(StatusCode::NO_CONTENT)
            .with_header("x-amz-request-id", &req.request_id))
    }

    // --- helpers ---

    fn require_principal<'a>(&self, req: &'a S3Request) -> Result<&'a Principal> {
        req.principal.as_ref().ok_or(Error::AccessDenied)
    }

    /// Fetch the target bucket (NoSuchBucket if absent). Authorization is applied centrally in
    /// `bucket_op`/`object_op`, so handlers only fetch.
    async fn fetch_bucket(&self, req: &S3Request) -> Result<Bucket> {
        let name = req.bucket.clone().expect("bucket present");
        self.meta
            .get_bucket(&name)
            .await?
            .ok_or(Error::NoSuchBucket)
    }

    /// Evaluate the full authorization decision (ARCH §15): assemble the requester class, the
    /// account/bucket Block Public Access settings, the bucket policy, and the request context,
    /// then run the pure policy engine.
    async fn authorize(
        &self,
        req: &S3Request,
        bucket: &Bucket,
        action: Action,
        resource: Resource,
    ) -> Result<()> {
        let requester = match req.principal.as_ref() {
            Some(p) if p.role == Role::Administrator || p.user_id == bucket.owner_id => {
                RequesterClass::OwnerOrAdmin
            }
            Some(p) => RequesterClass::AuthenticatedMember(p.user_id.clone()),
            None => RequesterClass::Anonymous,
        };
        let account_bpa = self.meta.get_account_public_access_block().await?;
        let bucket_bpa = match self
            .meta
            .get_bucket_config(&bucket.name, ConfigAspect::PublicAccessBlock)
            .await?
        {
            Some(doc) => serde_json::from_str(&doc.0).unwrap_or_default(),
            None => PublicAccessBlock::default(),
        };
        let policy = match self
            .meta
            .get_bucket_config(&bucket.name, ConfigAspect::Policy)
            .await?
        {
            Some(doc) => cairn_authz::parse_policy(&doc.0).ok(),
            None => None,
        };
        let input = AuthzInput {
            requester,
            action,
            resource,
            bucket_owner: bucket.owner_id.clone(),
            account_bpa,
            bucket_bpa,
            policy,
            bucket_acl: None,
            object_acl: None,
            ownership_mode: bucket.ownership_mode,
            context: build_context(req, self.clock.now()),
        };
        match self.authz.evaluate(&input) {
            Decision::Allow => Ok(()),
            Decision::Deny(_) => Err(Error::AccessDenied),
        }
    }

    /// Resolve the current (or hidden-by-delete-marker) object version for a read.
    async fn resolve_object(&self, req: &S3Request) -> Result<(ObjectVersionRow, Bucket)> {
        let bucket = self.fetch_bucket(req).await?;
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

/// Assemble the authorization condition/request context from the request.
fn build_context(req: &S3Request, now: cairn_types::Timestamp) -> RequestContext {
    RequestContext {
        source: req.source,
        secure_transport: req.secure,
        referer: req.header("referer").map(str::to_owned),
        user_agent: req.header("user-agent").map(str::to_owned),
        now,
        prefix: req.query("prefix").map(str::to_owned),
        delimiter: req.query("delimiter").map(str::to_owned),
        max_keys: req.query("max-keys").and_then(|s| s.parse().ok()),
        canned_acl: req.header("x-amz-acl").map(str::to_owned),
        content_sha256: req.header("x-amz-content-sha256").map(str::to_owned),
        version_id: req
            .query("versionId")
            .map(|v| VersionId::from_string(v.to_owned())),
        existing_tags: Vec::new(),
        request_tags: Vec::new(),
    }
}

/// Map a bucket-level request to the S3 action it requires.
fn bucket_action(req: &S3Request) -> Result<Action> {
    use Action::*;
    let q = |s: &str| req.has_query(s);
    Ok(match req.method {
        Method::PUT if q("versioning") => PutBucketVersioning,
        Method::PUT if q("tagging") => PutBucketTagging,
        Method::PUT if q("cors") => PutBucketCors,
        Method::PUT if q("policy") => PutBucketPolicy,
        Method::PUT if q("lifecycle") => PutLifecycleConfiguration,
        Method::PUT if q("replication") => PutReplicationConfiguration,
        Method::PUT => CreateBucket,
        Method::DELETE if q("tagging") => PutBucketTagging,
        Method::DELETE if q("cors") => PutBucketCors,
        Method::DELETE if q("policy") => PutBucketPolicy,
        Method::DELETE if q("lifecycle") => PutLifecycleConfiguration,
        Method::DELETE if q("replication") => PutReplicationConfiguration,
        Method::DELETE => DeleteBucket,
        Method::HEAD => ListBucket,
        Method::GET if q("location") => GetBucketLocation,
        Method::GET if q("uploads") => ListBucketMultipartUploads,
        Method::GET if q("versions") => ListBucketVersions,
        Method::GET if q("versioning") => GetBucketVersioning,
        Method::GET if q("tagging") => GetBucketTagging,
        Method::GET if q("cors") => GetBucketCors,
        Method::GET if q("policy") => GetBucketPolicy,
        Method::GET if q("lifecycle") => GetLifecycleConfiguration,
        Method::GET if q("replication") => GetReplicationConfiguration,
        Method::POST if q("delete") => DeleteObject,
        Method::GET => ListBucket,
        _ => return Err(Error::NotImplemented),
    })
}

/// Map an object-level request to the S3 action it requires.
fn object_action(req: &S3Request) -> Result<Action> {
    use Action::*;
    let q = |s: &str| req.has_query(s);
    Ok(match req.method {
        Method::PUT if q("tagging") => PutObjectTagging,
        Method::GET if q("tagging") => GetObjectTagging,
        Method::DELETE if q("tagging") => DeleteObjectTagging,
        Method::GET if q("uploadId") => ListMultipartUploadParts,
        Method::DELETE if q("uploadId") => AbortMultipartUpload,
        Method::PUT | Method::POST => PutObject,
        Method::GET | Method::HEAD => GetObject,
        Method::DELETE => DeleteObject,
        _ => return Err(Error::NotImplemented),
    })
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

/// Buffer a (small, XML) request body up to `limit` bytes.
async fn drain_body(body: cairn_types::BodyStream, limit: usize) -> Result<Vec<u8>> {
    use futures_util::StreamExt;
    let mut body = body;
    let mut buf = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|e| Error::InvalidArgument(e.to_string()))?;
        if buf.len() + chunk.len() > limit {
            return Err(Error::EntityTooLarge);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// The multipart ETag: MD5 of the concatenated per-part binary MD5 digests, suffixed with the
/// part count (ARCH §10.2).
fn multipart_etag(part_md5_hexes: &[String]) -> ETag {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    for hex_md5 in part_md5_hexes {
        if let Ok(bytes) = hex::decode(hex_md5) {
            h.update(&bytes);
        }
    }
    ETag::multipart(hex::encode(h.finalize()), part_md5_hexes.len())
}

fn strip_quotes(s: &str) -> &str {
    s.trim().trim_matches('"')
}

/// Parse `x-amz-copy-source`: `/bucket/key` or `bucket/key`, optionally `?versionId=...`.
fn parse_copy_source(raw: &str) -> Option<(String, String, Option<String>)> {
    let s = raw.strip_prefix('/').unwrap_or(raw);
    let (path, version) = match s.split_once("?versionId=") {
        Some((p, v)) => (p, Some(copy_pct_decode(v))),
        None => (s, None),
    };
    let (bucket, key) = path.split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((copy_pct_decode(bucket), copy_pct_decode(key), version))
}

fn copy_pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let hi = (b[i + 1] as char).to_digit(16);
            let lo = (b[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
