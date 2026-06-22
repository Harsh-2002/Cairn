//! The S3 service: dispatch and the core request lifecycles (ARCH 21) for buckets and
//! objects. Handlers depend only on the trait spine; the authorization wiring here is the
//! owner/admin baseline (the full policy/ACL/anonymous pipeline lands in Wave 3).

use crate::chunked::{ChunkDecoder, ChunkVerifier, decode_stream};
use crate::error_map::error_response;
use crate::httpdate::{http_date, parse_http_date};
use crate::request::{S3Body, S3Request, S3Response};
use base64::Engine;
use cairn_types::auth::{Principal, RequesterClass, Role};
use cairn_types::authz::{
    Acl, Action, AuthzInput, Decision, Grant, Grantee, OwnershipMode, Permission,
    PublicAccessBlock, RequestContext, Resource,
};
use cairn_types::blob::{ByteRange, PartRef, StageOptions};
use cairn_types::bucket::{
    Bucket, ConfigAspect, ConfigDoc, ObjectLockConfiguration, VersioningState,
};
use cairn_types::crypto::Nonce;
use cairn_types::error::Error;
use cairn_types::id::{BucketName, ObjectKey, UploadId, VersionId};
use cairn_types::meta::{
    ClaimOutcome, IfNoneMatch, ListQuery, MultipartSession, Mutation, MutationOutcome, OutboxEntry,
    Precondition, ReplicationOp, WebhookEntry, WebhookStatus,
};
use cairn_types::notification::{EventKind, NotificationConfig};
use cairn_types::object::{
    ChecksumAlgorithm, ChecksumSet, ChecksumValue, CompressionDescriptor, ETag, ObjectLockMode,
    ObjectRetention, ObjectVersionRow, StorageClass,
};
use cairn_types::traits::{AuthorizationEngine, BlobStore, Clock, Crypto, MetadataStore};
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
    /// The cryptography facility, used to wrap (seal) and unwrap (open) per-object SSE-S3
    /// data-encryption keys under the master key (ARCH 27).
    crypto: Arc<dyn Crypto>,
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
        crypto: Arc<dyn Crypto>,
        region: String,
        max_object_size: u64,
    ) -> Self {
        Self {
            meta,
            blob,
            authz,
            clock,
            crypto,
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
        // A CORS preflight (OPTIONS) is evaluated against the bucket's stored CORS configuration
        // before any authentication/authorization — a browser sends preflight without credentials
        // (ARCH 18.2, Medium #3).
        if req.method == Method::OPTIONS {
            return self.cors_preflight(&req).await;
        }
        match (&req.method, req.bucket.is_some(), req.key.is_some()) {
            (&Method::GET, false, _) => self.list_buckets(&req).await,
            (_, true, false) => self.bucket_op(req, body).await,
            (_, true, true) => self.object_op(req, body).await,
            _ => Err(Error::NotImplemented),
        }
    }

    /// Handle a CORS preflight (`OPTIONS` with `Origin` + `Access-Control-Request-Method`):
    /// evaluate the request against the bucket's stored CORS rules and, on a match, return 200
    /// with the `Access-Control-Allow-*`/`Vary: Origin` headers; on no match return 403 (ARCH
    /// 18.2, Medium #3).
    async fn cors_preflight(&self, req: &S3Request) -> Result<S3Response> {
        let Some(origin) = req.header("origin") else {
            // A bare OPTIONS without an Origin is not a CORS preflight.
            return Err(Error::AccessDenied);
        };
        let Some(method) = req.header("access-control-request-method") else {
            return Err(Error::AccessDenied);
        };
        // The requested headers are a comma-separated list (may be absent).
        let requested_headers: Vec<String> = req
            .header("access-control-request-headers")
            .map(|h| {
                h.split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let bucket = self.fetch_bucket(req).await?;
        let rules = match self
            .meta
            .get_bucket_config(&bucket.name, ConfigAspect::Cors)
            .await?
        {
            Some(doc) => cairn_xml::parse_cors_configuration(doc.0.as_bytes())?,
            None => Vec::new(),
        };

        match cors_match(&rules, origin, method, &requested_headers) {
            Some(resp) => Ok(resp.with_header("x-amz-request-id", &req.request_id)),
            None => Err(Error::AccessDenied),
        }
    }

    async fn bucket_op(&self, req: S3Request, body: cairn_types::BodyStream) -> Result<S3Response> {
        // Authorize centrally: map the operation to an action, then evaluate the engine.
        let action = bucket_action(&req)?;
        if action == Action::CreateBucket {
            self.require_principal(&req)?;
        } else {
            let bucket = self.fetch_bucket(&req).await?;
            self.authorize(
                &req,
                &bucket,
                action,
                Resource::Bucket(bucket.name.clone()),
                None,
            )
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
            Method::GET if req.has_query("acl") => self.get_bucket_acl(&req).await,
            Method::PUT if req.has_query("acl") => self.put_bucket_acl(req, body).await,
            Method::GET if req.has_query("publicAccessBlock") => {
                self.get_public_access_block(&req).await
            }
            Method::PUT if req.has_query("publicAccessBlock") => {
                self.put_public_access_block(req, body).await
            }
            Method::DELETE if req.has_query("publicAccessBlock") => {
                self.clear_bucket_config(&req, ConfigAspect::PublicAccessBlock)
                    .await
            }
            Method::GET if req.has_query("ownershipControls") => {
                self.get_ownership_controls(&req).await
            }
            Method::PUT if req.has_query("ownershipControls") => {
                self.put_ownership_controls(req, body).await
            }
            Method::GET if req.has_query("object-lock") => self.get_bucket_object_lock(&req).await,
            Method::PUT if req.has_query("object-lock") => {
                self.put_bucket_object_lock(req, body).await
            }
            // An UNRECOGNIZED bucket subresource must not fall through to list/create/delete.
            _ if unhandled_bucket_subresource(&req) => Err(Error::NotImplemented),
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
        // When the request names a `?versionId`, gate version-scoped policy conditions
        // (s3:ExistingObjectTag, object ACL) on THAT version, not the current one (audit #33).
        let req_version = req
            .query("versionId")
            .map(|v| VersionId::from_string(v.to_owned()));
        self.authorize(
            &req,
            &bucket,
            action,
            Resource::Object {
                bucket: bucket.name.clone(),
                key,
            },
            req_version.as_ref(),
        )
        .await?;
        match req.method {
            // A copy-source part is UploadPartCopy: stage a ranged copy of the source object as a
            // part rather than treating the (empty) request body as the part content.
            Method::PUT
                if req.has_query("uploadId")
                    && req.query("partNumber").is_some()
                    && req.header("x-amz-copy-source").is_some() =>
            {
                self.upload_part_copy(&req).await
            }
            Method::PUT if req.has_query("uploadId") && req.query("partNumber").is_some() => {
                self.upload_part(req, body).await
            }
            Method::PUT if req.header("x-amz-copy-source").is_some() => {
                self.copy_object(&req).await
            }
            Method::PUT if req.has_query("tagging") => self.put_object_tagging(req, body).await,
            Method::GET if req.has_query("tagging") => self.get_object_tagging(&req).await,
            Method::DELETE if req.has_query("tagging") => self.delete_object_tagging(&req).await,
            Method::GET if req.has_query("acl") => self.get_object_acl(&req).await,
            Method::PUT if req.has_query("acl") => self.put_object_acl(&req, body).await,
            Method::GET if req.has_query("retention") => self.get_object_retention(&req).await,
            Method::PUT if req.has_query("retention") => self.put_object_retention(req, body).await,
            Method::GET if req.has_query("legal-hold") => self.get_object_legal_hold(&req).await,
            Method::PUT if req.has_query("legal-hold") => {
                self.put_object_legal_hold(req, body).await
            }
            Method::GET if req.has_query("attributes") => self.get_object_attributes(&req).await,
            // An UNRECOGNIZED object subresource must never fall through to a data-plane handler
            // (a PUT object?acl must not overwrite the object body). Answer NotImplemented.
            _ if unhandled_object_subresource(&req) => Err(Error::NotImplemented),
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
        // Object Lock can only be turned on at creation (S3 semantics): `x-amz-bucket-object-lock-enabled`
        // forces versioning Enabled and stores an enabled (no default-retention) lock config.
        let object_lock = req
            .header("x-amz-bucket-object-lock-enabled")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let bucket = Bucket {
            name: name.clone(),
            owner_id: principal.user_id.clone(),
            created_at: self.clock.now(),
            versioning: if object_lock {
                VersioningState::Enabled
            } else {
                VersioningState::Unversioned
            },
            ownership_mode: cairn_types::authz::OwnershipMode::BucketOwnerEnforced,
            region: self.region.clone(),
            compression: None,
        };
        match self
            .meta
            .submit(Mutation::CreateBucket(Box::new(bucket)))
            .await
        {
            Ok(_) => {
                if object_lock {
                    let config = ObjectLockConfiguration {
                        enabled: true,
                        default_retention: None,
                    };
                    let text = serde_json::to_string(&config).map_err(|_| {
                        Error::Internal("config (de)serialization failed".to_owned())
                    })?;
                    self.meta
                        .submit(Mutation::SetBucketConfig {
                            bucket: name.clone(),
                            aspect: ConfigAspect::ObjectLock,
                            doc: Some(ConfigDoc(text)),
                        })
                        .await?;
                }
                Ok(S3Response::status(StatusCode::OK)
                    .with_header("location", format!("/{}", name.as_str()))
                    .with_header("x-amz-request-id", &req.request_id))
            }
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
            version_id_marker: None,
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
        // audit #25: when the client SIGNED a concrete payload hash (a real hex digest, not
        // UNSIGNED-PAYLOAD or a STREAMING-* sentinel), the body must actually hash to it — the hash
        // is part of the SigV4 signature, so an unverified body is not truly authenticated. Compute
        // SHA-256 over the body during staging only when there is a hash to verify; don't persist it
        // unless the client also asked to store a SHA-256 checksum.
        let signed_sha256 = req.header("x-amz-content-sha256").and_then(|v| {
            let v = v.trim();
            (v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit()))
                .then(|| v.to_ascii_lowercase())
        });
        let store_sha256 = extra.0.contains(&ChecksumAlgorithm::Sha256);
        let mut stage_checksums = extra;
        if signed_sha256.is_some() && !store_sha256 {
            stage_checksums.0.push(ChecksumAlgorithm::Sha256);
        }

        // Reject an invalid inline tag set up front — before any blob is staged — so a bad
        // `x-amz-tagging` header (over the limit, bad charset, duplicate/aws: key) returns
        // `InvalidTag` without leaving an orphaned staged blob (ARCH 17.1).
        cairn_xml::validate_tags(
            &parse_tagging_header(req.header("x-amz-tagging")),
            cairn_xml::MAX_TAGS_OBJECT,
        )?;

        // De-frame SigV4 streaming bodies (the F-5 fix); plain bodies pass through. A signed
        // sentinel selects the signature-verifying decoder seeded from the principal's context.
        let request_id = req.request_id.clone();
        let body = streaming_body(&req, raw_body, self.max_object_size)?;

        // SSE-S3 (ARCH 27): when the client requests AES256 server-side encryption, mint a fresh
        // random DEK, hand it to the blob store (which compress-then-encrypts each block), and seal
        // it under the master key for the metadata row. The plaintext-MD5 ETag is unaffected
        // because the blob store hashes the plaintext before any transform. Without a header, the
        // bucket's default-encryption setting (the `encryption` config aspect) decides.
        let want_sse = match requested_sse(&req) {
            Some(Ok(())) => true,
            Some(Err(e)) => return Err(e),
            None => self.bucket_default_sse(&bucket.name).await?,
        };
        let (sse_dek, sse_descriptor) = if want_sse {
            let (dek, descriptor) = self.new_sse_dek()?;
            (Some(dek), Some(descriptor))
        } else {
            (None, None)
        };

        // The declared payload length for preallocation (ARCH 7.5): for a SigV4-streaming upload
        // the client sends the real payload length in `x-amz-decoded-content-length` (the
        // `content-length` is the larger framed size), so prefer it; a plain PUT uses
        // `content-length`. An absent/unparseable value simply skips preallocation.
        let content_length = req
            .header("x-amz-decoded-content-length")
            .or_else(|| req.header("content-length"))
            .and_then(|v| v.parse::<u64>().ok());
        let opts = StageOptions {
            compression: bucket.compression,
            extra_checksums: stage_checksums,
            size_ceiling: self.max_object_size,
            content_type: content_type.clone(),
            encryption: sse_dek,
            content_length,
        };
        let mut staged = self
            .blob
            .stage(&bucket.name, body, opts)
            .await
            .map_err(map_stage_err)?;

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

        // Verify the signed SHA-256 payload hash against the body actually staged (audit #25).
        if let Some(want_hex) = &signed_sha256 {
            let got_hex = staged
                .checksums
                .iter()
                .find(|c| c.algorithm == ChecksumAlgorithm::Sha256)
                .and_then(|c| {
                    base64::engine::general_purpose::STANDARD
                        .decode(c.value.as_bytes())
                        .ok()
                })
                .map(hex::encode);
            if got_hex.as_deref() != Some(want_hex.as_str()) {
                let _ = self.blob.delete(&staged.storage_path).await;
                return Err(Error::BadDigest);
            }
            // Drop the verification-only checksum so it is not persisted/advertised unless the
            // client explicitly requested a SHA-256 checksum.
            if !store_sha256 {
                staged
                    .checksums
                    .retain(|c| c.algorithm != ChecksumAlgorithm::Sha256);
            }
        }

        // Verify any client-supplied x-amz-checksum-* against the computed checksum (21.1).
        if let Err(e) = verify_client_checksums(&req, &staged.checksums) {
            let _ = self.blob.delete(&staged.storage_path).await;
            return Err(e);
        }

        let versioned = bucket.versioning == VersioningState::Enabled;
        let version_id = if versioned {
            VersionId::generate()
        } else {
            VersionId::null()
        };
        let now = self.clock.now();
        // Honor a canned x-amz-acl when the bucket's ownership mode keeps ACLs in force.
        let acl = match req.header("x-amz-acl") {
            Some(canned) if bucket.ownership_mode != OwnershipMode::BucketOwnerEnforced => {
                cairn_authz::expand_canned_acl(canned, &bucket.owner_id)
            }
            _ => None,
        };
        // An inbound replica is a PUT carrying `x-amz-meta-cairn-replica: true` (ARCH 20.4): mark
        // the version a `Replica` so it is never re-replicated, and skip the outbox entirely (loop
        // prevention). The marker is matched case-insensitively on the value.
        // Only a privileged replication principal may classify a write as an inbound replica
        // (audit #16). Honoring the bare client header would let any writer mark their write a
        // `Replica` to suppress its own replication (skip the outbox) or otherwise downgrade how
        // it is handled. Replication ships under the destination's Administrator-role credential,
        // so gate the marker on that role; a normal member's header is ignored and the write
        // replicates normally.
        let is_replica = req
            .header("x-amz-meta-cairn-replica")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
            && req
                .principal
                .as_ref()
                .is_some_and(|p| p.role == Role::Administrator);
        let replication_status =
            is_replica.then_some(cairn_types::meta::ReplicationStatus::Replica);
        // The system response headers (ARCH 13.4) are stored verbatim from the request.
        let header_owned = |name: &str| req.header(name).map(str::to_owned);
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
            content_encoding: header_owned("content-encoding"),
            cache_control: header_owned("cache-control"),
            content_disposition: header_owned("content-disposition"),
            content_language: header_owned("content-language"),
            expires: header_owned("expires"),
            storage_path: Some(staged.storage_path.clone()),
            compression: staged.compression.clone(),
            storage_class: StorageClass::Standard,
            cold_locator: None,
            owner_id: bucket.owner_id.clone(),
            user_metadata,
            acl,
            checksums: staged.checksums.clone(),
            sse_descriptor: sse_descriptor.clone(),
            replication_status,
            created_at: now,
            updated_at: now,
        };

        // An inbound replica must NOT itself enqueue replication (loop prevention, ARCH 20.4);
        // a normal create enqueues when an enabled rule matches the key and the object's inline
        // tags (the `x-amz-tagging` header tag set, applied to this version below).
        let replication = if is_replica {
            Vec::new()
        } else {
            let inline_tags = parse_tagging_header(req.header("x-amz-tagging"));
            self.replication_outbox(
                &bucket,
                &key,
                &version_id,
                ReplicationOp::ObjectCreate,
                &inline_tags,
            )
            .await
        };
        match self
            .meta
            .submit(Mutation::PutObjectVersion {
                row: Box::new(row),
                precondition: precond,
                replication,
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
                // Persist the inline `x-amz-tagging` header as the object's initial tag set
                // (ARCH 17.1, Medium #5). Tags are a separate mutation; commit them after the
                // object version exists so they attach to the just-written version.
                let initial_tags = parse_tagging_header(req.header("x-amz-tagging"));
                if !initial_tags.is_empty() {
                    let _ = self
                        .meta
                        .submit(Mutation::PutObjectTags {
                            bucket: bucket.name.clone(),
                            key: key.clone(),
                            version_id: version_id.clone(),
                            tags: initial_tags,
                        })
                        .await;
                }
                // Stamp Object Lock retention / legal hold onto the new version (default or header).
                self.apply_object_lock_on_put(&req, &bucket, &key, &version_id, now)
                    .await?;
                // Emit an ObjectCreated:Put event notification (best-effort).
                self.emit_events(
                    &bucket.name,
                    &key,
                    &version_id,
                    EventKind::ObjectCreatedPut,
                    Some(staged.size_logical),
                    Some(staged.etag.as_str()),
                    now,
                )
                .await;
                let mut resp = S3Response::status(StatusCode::OK)
                    .with_header("etag", quoted(&staged.etag))
                    .with_header("x-amz-request-id", &request_id);
                if versioned {
                    resp = resp.with_header("x-amz-version-id", version_id.as_str());
                }
                // Echo the SSE algorithm on the PUT response for an encrypted object (ARCH 27).
                if sse_descriptor.is_some() {
                    resp = resp.with_header("x-amz-server-side-encryption", SSE_AES256);
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
        let row = match self.resolve_read_target(req).await? {
            ReadTarget::Object(row) => *row,
            ReadTarget::DeleteMarker(resp) => return Ok(resp),
        };
        if let Some(resp) = conditional_short_circuit(req, &row) {
            return Ok(resp.with_header("x-amz-request-id", &req.request_id));
        }
        let range = parse_range(req.header("range"), row.size_logical)?;
        let storage = row.storage_path.clone().ok_or(Error::NoSuchKey)?;
        // SSE-S3 (ARCH 27): if the version is encrypted, unwrap its DEK under the master key and
        // decrypt transparently while reading. An unencrypted version passes `None`.
        let dek = match row.sse_descriptor.as_deref() {
            Some(d) => Some(self.open_sse_dek(d)?),
            None => None,
        };
        let handle = self
            .blob
            .open_with_dek(&storage, range, dek, &row.compression)
            .await?;
        let status = if handle.content_range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };

        let logical_len = handle.logical_len;
        let content_range = handle.content_range;
        // A GET of an uncompressed, unencrypted blob carries the zero-copy hint up so the connection
        // layer can `sendfile` it on the fast path. This covers a full object AND a single resolved
        // byte-range: `open_with_dek` set the hint's `offset`/`len` to the sub-range and `logical_len`
        // to the range length, so the kernel transfers exactly the requested bytes (a 206 with the
        // `content-range` header added below). It still carries the portable stream, so every non-fast
        // path serves it identically. A compressed/encrypted blob yields `zero_copy == None`, and a
        // multi-range/unsatisfiable request never reaches here as a single `ByteRange`.
        let body = match handle.zero_copy {
            Some(zc) => S3Body::ZeroCopy {
                length: logical_len,
                zero_copy: zc,
                stream: handle.body,
            },
            None => S3Body::Stream {
                length: logical_len,
                stream: handle.body,
            },
        };

        let mut resp = S3Response {
            status,
            headers: Vec::new(),
            body,
        };
        resp = object_headers(resp, &row).with_header("content-length", logical_len.to_string());
        // Response-header overrides (ARCH 21.2): the `response-*` query params override the
        // corresponding response headers for this GET, applied after `object_headers` so they win.
        resp = apply_response_overrides(resp, req);
        if let Some(cr) = content_range {
            resp = resp.with_header(
                "content-range",
                format!("bytes {}-{}/{}", cr.start, cr.end, cr.total),
            );
        }
        let bucket_name = req.bucket.clone().expect("bucket present");
        let key = req.key.clone().expect("key present");
        resp = self
            .append_lock_headers(resp, &bucket_name, &key, &row.version_id)
            .await?;
        Ok(resp.with_header("x-amz-request-id", &req.request_id))
    }

    async fn head_object(&self, req: &S3Request) -> Result<S3Response> {
        let row = match self.resolve_read_target(req).await? {
            ReadTarget::Object(row) => *row,
            ReadTarget::DeleteMarker(resp) => return Ok(resp),
        };
        if let Some(resp) = conditional_short_circuit(req, &row) {
            // Conditional HEAD returns the bare 304/412 status (no body, per S3).
            return Ok(resp.with_header("x-amz-request-id", &req.request_id));
        }
        let resp = object_headers(S3Response::status(StatusCode::OK), &row)
            .with_header("content-length", row.size_logical.to_string())
            .with_header("x-amz-request-id", &req.request_id);
        // S3 applies the `response-*` header overrides to HEAD as well as GET.
        let resp = apply_response_overrides(resp, req);
        let bucket_name = req.bucket.clone().expect("bucket present");
        let key = req.key.clone().expect("key present");
        let resp = self
            .append_lock_headers(resp, &bucket_name, &key, &row.version_id)
            .await?;
        Ok(resp)
    }

    async fn delete_object(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        // A copy kept for event-notification emission, since `key` is moved into the mutation below.
        let event_key = key.clone();
        let now = self.clock.now();
        // An inbound replicated delete-marker propagation carries the loop-prevention marker; the
        // marker it creates must NOT itself be re-enqueued for replication (ARCH 20.4), which
        // matters for two-way replication where the destination bucket also has a rule.
        // Only a privileged replication principal may classify a write as an inbound replica
        // (audit #16). Honoring the bare client header would let any writer mark their write a
        // `Replica` to suppress its own replication (skip the outbox) or otherwise downgrade how
        // it is handled. Replication ships under the destination's Administrator-role credential,
        // so gate the marker on that role; a normal member's header is ignored and the write
        // replicates normally.
        let is_replica = req
            .header("x-amz-meta-cairn-replica")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
            && req
                .principal
                .as_ref()
                .is_some_and(|p| p.role == Role::Administrator);

        // A versioned DELETE (?versionId) permanently removes that version (no delete marker). A
        // plain DELETE in an Enabled bucket inserts a new identified delete marker; in a Suspended
        // bucket it inserts a NULL-version delete marker that replaces the null version (avoiding
        // the silent permanent removal of the null version — ARCH 16.1/16.3, Medium #4); in an
        // Unversioned bucket it removes the sentinel version.
        if let Some(vid) = req.query("versionId") {
            let version_id = VersionId::from_string(vid.to_owned());
            // Object Lock: a version still under retention or legal hold cannot be permanently
            // deleted (a `?versionId` DELETE is the only permanent-removal path for objects).
            self.enforce_unlocked_for_delete(req, &bucket, &key, &version_id, now)
                .await?;
            let event_version = version_id.clone();
            let outcome = self
                .meta
                .submit(Mutation::DeleteVersion {
                    bucket: bucket.name.clone(),
                    key,
                    version_id,
                })
                .await?;
            if let MutationOutcome::Deleted {
                freed: Some(path), ..
            } = outcome
            {
                let _ = self.blob.delete(&path).await;
            }
            self.emit_events(
                &bucket.name,
                &event_key,
                &event_version,
                EventKind::ObjectRemovedDelete,
                None,
                None,
                now,
            )
            .await;
            return Ok(S3Response::status(StatusCode::NO_CONTENT)
                .with_header("x-amz-request-id", &req.request_id));
        }

        match bucket.versioning {
            VersioningState::Enabled => {
                let marker_id = VersionId::generate();
                let replication = if is_replica {
                    Vec::new()
                } else {
                    self.replication_outbox(
                        &bucket,
                        &key,
                        &marker_id,
                        ReplicationOp::DeleteMarker,
                        &[],
                    )
                    .await
                };
                self.meta
                    .submit(Mutation::CreateDeleteMarker {
                        bucket: bucket.name.clone(),
                        key,
                        version_id: marker_id.clone(),
                        owner_id: bucket.owner_id.clone(),
                        now,
                        replication,
                    })
                    .await?;
                self.emit_events(
                    &bucket.name,
                    &event_key,
                    &marker_id,
                    EventKind::ObjectRemovedDeleteMarkerCreated,
                    None,
                    None,
                    now,
                )
                .await;
                // Signal the newly-created delete marker's identity to the client (Medium #4).
                Ok(S3Response::status(StatusCode::NO_CONTENT)
                    .with_header("x-amz-delete-marker", "true")
                    .with_header("x-amz-version-id", marker_id.as_str())
                    .with_header("x-amz-request-id", &req.request_id))
            }
            VersioningState::Suspended => {
                // Replace any existing null version with a NULL-version delete marker. Removing the
                // null version first keeps a single null entry per key without disturbing older
                // identified versions, and avoids a unique-constraint conflict on the insert.
                if let Ok(MutationOutcome::Deleted {
                    freed: Some(path), ..
                }) = self
                    .meta
                    .submit(Mutation::DeleteVersion {
                        bucket: bucket.name.clone(),
                        key: key.clone(),
                        version_id: VersionId::null(),
                    })
                    .await
                {
                    let _ = self.blob.delete(&path).await;
                }
                // A Suspended bucket inserts a NULL-version marker; replication requires
                // Enabled versioning, so `replication_outbox` returns None here by construction.
                let replication = self
                    .replication_outbox(
                        &bucket,
                        &key,
                        &VersionId::null(),
                        ReplicationOp::DeleteMarker,
                        &[],
                    )
                    .await;
                self.meta
                    .submit(Mutation::CreateDeleteMarker {
                        bucket: bucket.name.clone(),
                        key,
                        version_id: VersionId::null(),
                        owner_id: bucket.owner_id.clone(),
                        now,
                        replication,
                    })
                    .await?;
                self.emit_events(
                    &bucket.name,
                    &event_key,
                    &VersionId::null(),
                    EventKind::ObjectRemovedDeleteMarkerCreated,
                    None,
                    None,
                    now,
                )
                .await;
                Ok(S3Response::status(StatusCode::NO_CONTENT)
                    .with_header("x-amz-delete-marker", "true")
                    .with_header("x-amz-request-id", &req.request_id))
            }
            VersioningState::Unversioned => {
                let outcome = self
                    .meta
                    .submit(Mutation::DeleteVersion {
                        bucket: bucket.name.clone(),
                        key,
                        version_id: VersionId::null(),
                    })
                    .await?;
                if let MutationOutcome::Deleted {
                    freed: Some(path), ..
                } = outcome
                {
                    let _ = self.blob.delete(&path).await;
                }
                self.emit_events(
                    &bucket.name,
                    &event_key,
                    &VersionId::null(),
                    EventKind::ObjectRemovedDelete,
                    None,
                    None,
                    now,
                )
                .await;
                Ok(S3Response::status(StatusCode::NO_CONTENT)
                    .with_header("x-amz-request-id", &req.request_id))
            }
        }
    }

    // --- multipart ---

    async fn create_multipart(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        let upload_id = UploadId::generate();
        let now = self.clock.now();
        // Capture the SSE-S3 intent now (ARCH 27): the `CompleteMultipartUpload` request carries no
        // SSE header, so the decision — an explicit `x-amz-server-side-encryption` header here, else
        // the bucket default-encryption setting — must be recorded on the session and applied when
        // the object is assembled, exactly mirroring a single-part PUT.
        let want_sse = match requested_sse(req) {
            Some(Ok(())) => true,
            Some(Err(e)) => return Err(e),
            None => self.bucket_default_sse(&bucket.name).await?,
        };
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
            sse_requested: want_sse,
            created_at: now,
            updated_at: now,
        };
        // Use the upload id the STORE returns, not the locally-generated one: under metadata
        // sharding the store encodes the owning shard into the id so every later part/complete/abort
        // op routes back to the same shard. Returning the un-encoded local id would strand the
        // upload on the wrong shard (audit #4).
        let upload_id = match self
            .meta
            .submit(Mutation::CreateMultipart(Box::new(session)))
            .await?
        {
            MutationOutcome::MultipartCreated(id) => id,
            _ => return Err(Error::Internal("unexpected multipart outcome".to_owned())),
        };
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
        let body = streaming_body(&req, raw_body, self.max_object_size)?;
        let staged = self
            .blob
            .stage_part(&upload_id, part_number, body, self.max_object_size)
            .await
            .map_err(map_stage_err)?;
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

    /// `UploadPartCopy` (ARCH 21.3, 34.3): stage a part from a range of an existing object
    /// rather than from a request body. The copy source (`/bucket/key[?versionId]`) and the
    /// optional `x-amz-copy-source-range` are parsed; the source range is opened from the blob
    /// store (logical coordinates), restaged as a part, recorded, and a `CopyPartResult` carrying
    /// the part ETag and last-modified time is returned.
    async fn upload_part_copy(&self, req: &S3Request) -> Result<S3Response> {
        let _dest_bucket = self.fetch_bucket(req).await?;
        let upload_id = UploadId::from_string(req.query("uploadId").unwrap_or_default().to_owned());
        let part_number: u16 = req
            .query("partNumber")
            .and_then(|s| s.parse().ok())
            .filter(|n| (1..=10_000).contains(n))
            .ok_or_else(|| Error::InvalidArgument("partNumber out of range".to_owned()))?;
        if self.meta.get_multipart(&upload_id).await?.is_none() {
            return Err(Error::NoSuchUpload);
        }

        // Resolve the copy source object version (reusing the copy-source parser and the
        // copy-source-if-* precondition checks shared with CopyObject).
        let raw_source = req.header("x-amz-copy-source").unwrap_or_default();
        let (src_bucket_s, src_key_s, src_version) = parse_copy_source(raw_source)
            .ok_or_else(|| Error::InvalidArgument("bad copy source".to_owned()))?;
        let src_bucket = BucketName::parse(&src_bucket_s)?;
        let src_key = ObjectKey::parse(&src_key_s)?;
        let src_versioned = src_version.is_some();
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
        check_copy_source_conditions(req, &src_row)?;
        // Authorize the SOURCE read (see copy_object): the requester must hold s3:GetObject(Version)
        // on the source object, evaluated against the source bucket — not the destination upload's.
        let src_bucket_obj = self
            .meta
            .get_bucket(&src_bucket)
            .await?
            .ok_or(Error::NoSuchBucket)?;
        let src_action = if src_versioned {
            Action::GetObjectVersion
        } else {
            Action::GetObject
        };
        self.authorize(
            req,
            &src_bucket_obj,
            src_action,
            Resource::Object {
                bucket: src_bucket.clone(),
                key: src_key.clone(),
            },
            Some(&src_row.version_id),
        )
        .await?;
        let src_path = src_row.storage_path.clone().ok_or(Error::NoSuchKey)?;

        // Parse the optional copy-source range (logical bytes of the source object). An absent
        // header copies the whole object.
        let range =
            parse_copy_source_range(req.header("x-amz-copy-source-range"), src_row.size_logical)?;

        // Open the source range and feed its logical bytes into the part stager, re-tagging blob
        // read errors as body errors so the source can drive `stage_part`. An SSE-encrypted source
        // is decrypted transparently via its unwrapped DEK (ARCH 27).
        let src_dek = match src_row.sse_descriptor.as_deref() {
            Some(d) => Some(self.open_sse_dek(d)?),
            None => None,
        };
        let handle = self
            .blob
            .open_with_dek(&src_path, range, src_dek, &src_row.compression)
            .await?;
        let src_stream: cairn_types::BodyStream =
            {
                use futures_util::StreamExt;
                Box::pin(handle.body.map(|r| {
                    r.map_err(|e| cairn_types::error::BodyError::Transport(e.to_string()))
                }))
            };
        let staged = self
            .blob
            .stage_part(&upload_id, part_number, src_stream, self.max_object_size)
            .await
            .map_err(map_stage_err)?;

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

        let etag = ETag::from_md5_hex(staged.md5_hex.clone());
        let body = cairn_xml::copy_part_result(&etag, self.clock.now());
        let mut resp =
            S3Response::xml(StatusCode::OK, body).with_header("x-amz-request-id", &req.request_id);
        if !src_row.version_id.is_null() {
            resp = resp.with_header("x-amz-copy-source-version-id", src_row.version_id.as_str());
        }
        Ok(resp)
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

        // Validate the requested parts against what was uploaded BEFORE claiming the session, so a
        // part-validation failure (the common client error: wrong ETag, gap, out-of-order, or an
        // undersized part) leaves the upload `active` and retryable rather than bricking it in
        // `completing` with no way to retry or abort (audit #14). Listing and validating parts is
        // read-only and independent of the session status; only once every part checks out do we
        // claim the session and assemble.
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

        // Every requested part validated: now claim the session (active -> completing) and
        // assemble. A failure past this point is server-side (assembly/commit), not a client part
        // error, so the narrow `completing` window is acceptable.
        let session = match self
            .meta
            .submit(Mutation::ClaimMultipart(upload_id.clone()))
            .await?
        {
            MutationOutcome::MultipartClaim(ClaimOutcome::Claimed(s)) => *s,
            _ => return Err(Error::NoSuchUpload),
        };

        // SSE-S3 (ARCH 27): honor the encryption intent captured at initiate — mint a fresh DEK and
        // hand it to `assemble`, which compress-then-encrypts each block, then seal the descriptor for
        // the row, so a multipart object is encrypted at rest exactly like a single-part PUT.
        let (sse_dek, sse_descriptor) = if session.sse_requested {
            let (dek, descriptor) = self.new_sse_dek()?;
            (Some(dek), Some(descriptor))
        } else {
            (None, None)
        };
        let opts = StageOptions {
            compression: bucket.compression,
            extra_checksums: ChecksumSet::none(),
            size_ceiling: self.max_object_size,
            // `assemble` preallocates from the sum of the parts' sizes itself.
            content_length: None,
            content_type: session.content_type.clone(),
            encryption: sse_dek,
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
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            storage_path: Some(staged.storage_path.clone()),
            compression: staged.compression.clone(),
            storage_class: StorageClass::Standard,
            cold_locator: None,
            owner_id: bucket.owner_id.clone(),
            user_metadata: session.user_metadata.clone(),
            acl: None,
            checksums: Vec::new(),
            sse_descriptor: sse_descriptor.clone(),
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
                replication: Vec::new(),
            })
            .await
        {
            Ok(MutationOutcome::MultipartCompleted { superseded, .. }) => {
                if let Some(old) = superseded {
                    let _ = self.blob.delete(&old).await;
                }
                let _ = self.blob.delete_session(&upload_id).await;
                // Emit an ObjectCreated:CompleteMultipartUpload event (best-effort).
                self.emit_events(
                    &bucket.name,
                    &key,
                    &version_id,
                    EventKind::ObjectCreatedCompleteMultipartUpload,
                    Some(staged.size_logical),
                    Some(etag.as_str()),
                    now,
                )
                .await;
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
                if sse_descriptor.is_some() {
                    resp = resp.with_header("x-amz-server-side-encryption", "AES256");
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
        // A copy source that names an explicit versionId requires s3:GetObjectVersion; otherwise
        // s3:GetObject. Capture this before `src_version` is consumed below.
        let src_versioned = src_version.is_some();

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
        // Evaluate any x-amz-copy-source-if-* preconditions against the source version (21.6).
        check_copy_source_conditions(req, &src_row)?;
        // Authorize the SOURCE read: a copy reads the source object's bytes, so the requester must
        // hold s3:GetObject(Version) on the SOURCE, evaluated against the source bucket's policy /
        // ownership / ACLs (not the destination's). Without this, a user who merely owns the
        // destination bucket could exfiltrate any other tenant's object via x-amz-copy-source.
        let src_bucket_obj = self
            .meta
            .get_bucket(&src_bucket)
            .await?
            .ok_or(Error::NoSuchBucket)?;
        let src_action = if src_versioned {
            Action::GetObjectVersion
        } else {
            Action::GetObject
        };
        self.authorize(
            req,
            &src_bucket_obj,
            src_action,
            Resource::Object {
                bucket: src_bucket.clone(),
                key: src_key.clone(),
            },
            Some(&src_row.version_id),
        )
        .await?;
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

        // An SSE-encrypted source is decrypted transparently via its unwrapped DEK (ARCH 27).
        let src_dek = match src_row.sse_descriptor.as_deref() {
            Some(d) => Some(self.open_sse_dek(d)?),
            None => None,
        };
        let handle = self
            .blob
            .open_with_dek(&src_path, None, src_dek, &src_row.compression)
            .await?;
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
            encryption: None,
            // The source object's plaintext size is known, so preallocate the destination blob.
            content_length: Some(src_row.size_logical),
        };
        let staged = self.blob.stage(&dest_bucket.name, src_stream, opts).await?;

        let versioned = dest_bucket.versioning == VersioningState::Enabled;
        let version_id = if versioned {
            VersionId::generate()
        } else {
            VersionId::null()
        };
        let now = self.clock.now();
        let dest_key_for_event = dest_key.clone();
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
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            storage_path: Some(staged.storage_path.clone()),
            compression: staged.compression.clone(),
            storage_class: StorageClass::Standard,
            cold_locator: None,
            owner_id: dest_bucket.owner_id.clone(),
            user_metadata: user_meta,
            acl: None,
            checksums: staged.checksums.clone(),
            // The copy destination is stored unencrypted (SSE on copy is out of scope here).
            sse_descriptor: None,
            replication_status: None,
            created_at: now,
            updated_at: now,
        };
        match self
            .meta
            .submit(Mutation::PutObjectVersion {
                row: Box::new(row),
                precondition: Precondition::default(),
                replication: Vec::new(),
            })
            .await
        {
            Ok(MutationOutcome::Put { superseded, .. }) => {
                if let Some(old) = superseded {
                    let _ = self.blob.delete(&old).await;
                }
                // Object Lock: a copy creates a new version like a PUT, so stamp the bucket default
                // retention / any explicit `x-amz-object-lock-*` headers onto it too (WORM coverage
                // at EVERY object-creating path).
                self.apply_object_lock_on_put(
                    req,
                    &dest_bucket,
                    &dest_key_for_event,
                    &version_id,
                    now,
                )
                .await?;
                // Emit an ObjectCreated:Copy event for the destination (best-effort).
                self.emit_events(
                    &dest_bucket.name,
                    &dest_key_for_event,
                    &version_id,
                    EventKind::ObjectCreatedCopy,
                    Some(staged.size_logical),
                    Some(staged.etag.as_str()),
                    now,
                )
                .await;
                let body = cairn_xml::copy_object_result(&staged.etag, now);
                let mut resp = S3Response::xml(StatusCode::OK, body)
                    .with_header("x-amz-request-id", &req.request_id);
                if versioned {
                    resp = resp.with_header("x-amz-version-id", version_id.as_str());
                }
                Ok(resp)
            }
            // Surface the commit's TRUE error (PreconditionFailed, InsufficientStorage, ...) rather
            // than collapsing every failure to Internal(500) (Medium #7).
            Ok(_) => {
                let _ = self.blob.delete(&staged.storage_path).await;
                Err(Error::Internal("unexpected copy outcome".to_owned()))
            }
            Err(e) => {
                let _ = self.blob.delete(&staged.storage_path).await;
                Err(e.into())
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
        // S3 caps a multi-object delete at 1000 keys per request (ARCH 21.5); reject a larger
        // batch as a malformed request rather than processing it partially.
        if keys.len() > 1000 {
            return Err(Error::MalformedXml);
        }
        let now = self.clock.now();

        // Request-level replica gate (audit #16), mirroring the single-object DELETE: an
        // Administrator carrying `x-amz-meta-cairn-replica` is propagating a replicated delete, so
        // its delete markers must NOT be re-enqueued (loop prevention, ARCH 20.4) — and under
        // fan-out would otherwise re-ship to every target. A normal member's header is ignored.
        let is_replica = req
            .header("x-amz-meta-cairn-replica")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
            && req
                .principal
                .as_ref()
                .is_some_and(|p| p.role == Role::Administrator);

        // Each entry is (key, version_id, is_delete_marker, delete_marker_version_id) so the
        // result can surface `<DeleteMarker>`/`<DeleteMarkerVersionId>` for a marker insert.
        let mut deleted: Vec<(String, Option<String>, bool, Option<String>)> = Vec::new();
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
            // Authorize each key individually (Medium #7): a bulk delete is N independent
            // DeleteObject (or DeleteObjectVersion, for a versioned entry) decisions, not one
            // bucket-level grant. A per-key denial becomes a per-key error, not a whole-request
            // failure, so the remaining keys still proceed.
            let action = if version.is_some() {
                Action::DeleteObjectVersion
            } else {
                Action::DeleteObject
            };
            // Evaluate version-scoped policy conditions against the named version (audit #33).
            let acl_version = version.as_ref().map(|v| VersionId::from_string(v.clone()));
            if let Err(e) = self
                .authorize(
                    req,
                    &bucket,
                    action,
                    Resource::Object {
                        bucket: bucket.name.clone(),
                        key: key.clone(),
                    },
                    acl_version.as_ref(),
                )
                .await
            {
                let (_, code) = crate::error_map::map(&e);
                errors.push((key_s, code.to_owned(), e.to_string()));
                continue;
            }
            // Object Lock: a permanent version delete (?versionId) of a still-protected version is
            // denied per-key (the rest of the batch proceeds). A plain delete-marker insert (no
            // versionId) creates a NEW version and never destroys a locked one, so it is unaffected.
            if let Some(v) = &version {
                let version_id = VersionId::from_string(v.clone());
                if let Err(e) = self
                    .enforce_unlocked_for_delete(req, &bucket, &key, &version_id, now)
                    .await
                {
                    let (_, code) = crate::error_map::map(&e);
                    errors.push((key_s, code.to_owned(), e.to_string()));
                    continue;
                }
            }
            // Event-notification context kept before `key`/`version` are moved into the mutation.
            let event_key = key.clone();
            let event_version_req = version.clone();
            // Per-key delete semantics by versioning state, mirroring the single-object DELETE:
            //  - a versioned delete (?versionId) permanently removes that version;
            //  - Enabled: insert a delete marker (replicated where the rule calls for it);
            //  - Suspended: replace any existing null version with a null-version delete marker
            //    (NOT a permanent removal — Medium #4 applied to the bulk path);
            //  - Unversioned: remove the sentinel version outright.
            let result = if let Some(v) = &version {
                self.meta
                    .submit(Mutation::DeleteVersion {
                        bucket: bucket.name.clone(),
                        key,
                        version_id: VersionId::from_string(v.clone()),
                    })
                    .await
            } else {
                match bucket.versioning {
                    VersioningState::Enabled => {
                        let mid = VersionId::generate();
                        let replication = if is_replica {
                            Vec::new()
                        } else {
                            self.replication_outbox(
                                &bucket,
                                &key,
                                &mid,
                                ReplicationOp::DeleteMarker,
                                &[],
                            )
                            .await
                        };
                        self.meta
                            .submit(Mutation::CreateDeleteMarker {
                                bucket: bucket.name.clone(),
                                key,
                                version_id: mid,
                                owner_id: bucket.owner_id.clone(),
                                now,
                                replication,
                            })
                            .await
                    }
                    VersioningState::Suspended => {
                        // Remove any existing null version (reclaiming its blob), then insert a
                        // null-version delete marker that replaces it.
                        if let Ok(MutationOutcome::Deleted { freed: Some(p), .. }) = self
                            .meta
                            .submit(Mutation::DeleteVersion {
                                bucket: bucket.name.clone(),
                                key: key.clone(),
                                version_id: VersionId::null(),
                            })
                            .await
                        {
                            let _ = self.blob.delete(&p).await;
                        }
                        self.meta
                            .submit(Mutation::CreateDeleteMarker {
                                bucket: bucket.name.clone(),
                                key,
                                version_id: VersionId::null(),
                                owner_id: bucket.owner_id.clone(),
                                now,
                                replication: Vec::new(),
                            })
                            .await
                    }
                    VersioningState::Unversioned => {
                        self.meta
                            .submit(Mutation::DeleteVersion {
                                bucket: bucket.name.clone(),
                                key,
                                version_id: VersionId::null(),
                            })
                            .await
                    }
                }
            };
            match result {
                Ok(MutationOutcome::Deleted { freed, .. }) => {
                    if let Some(p) = freed {
                        let _ = self.blob.delete(&p).await;
                    }
                    let ev = match &event_version_req {
                        Some(v) => VersionId::from_string(v.clone()),
                        None => VersionId::null(),
                    };
                    self.emit_events(
                        &bucket.name,
                        &event_key,
                        &ev,
                        EventKind::ObjectRemovedDelete,
                        None,
                        None,
                        now,
                    )
                    .await;
                    if !quiet {
                        deleted.push((key_s, version, false, None));
                    }
                }
                Ok(MutationOutcome::DeleteMarker { version_id: mv }) => {
                    self.emit_events(
                        &bucket.name,
                        &event_key,
                        &mv,
                        EventKind::ObjectRemovedDeleteMarkerCreated,
                        None,
                        None,
                        now,
                    )
                    .await;
                    if !quiet {
                        // S3 surfaces a marker insert with DeleteMarker=true and the marker id in
                        // DeleteMarkerVersionId; the request's VersionId field stays empty here.
                        let dmv = (!mv.is_null()).then(|| mv.as_str().to_owned());
                        deleted.push((key_s, None, true, dmv));
                    }
                }
                Ok(_) => {
                    if !quiet {
                        deleted.push((key_s, version, false, None));
                    }
                }
                // Map each per-key failure to its TRUE S3 code via the total error map, rather
                // than collapsing every failure to InternalError (Medium #7).
                Err(e) => {
                    let err: Error = e.into();
                    let (_, code) = crate::error_map::map(&err);
                    errors.push((key_s, code.to_owned(), err.to_string()));
                }
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
        // Object Lock requires versioning to stay Enabled: suspending it would let a null-version
        // overwrite permanently destroy locked bytes in place, breaking the WORM guarantee. S3
        // rejects suspending versioning on an Object-Lock-enabled bucket (InvalidBucketState).
        if state == VersioningState::Suspended
            && self.bucket_object_lock_config(&bucket.name).await?.enabled
        {
            return Err(Error::InvalidRequest(
                "Cannot suspend versioning on a bucket with Object Lock enabled".to_owned(),
            ));
        }
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
        let version_id_marker = req.query("version-id-marker").map(str::to_owned);
        let query = ListQuery {
            prefix: prefix.clone(),
            delimiter: delimiter.clone(),
            cursor,
            version_id_marker,
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
                let tags = cairn_xml::parse_tagging(&doc)?;
                cairn_xml::validate_tags(&tags, cairn_xml::MAX_TAGS_BUCKET)?;
            }
            ConfigAspect::Lifecycle => {
                // Validate the configuration on write (well-formed rules), and REJECT storage-class
                // transitions/tiering: Cairn does not move object data to a cold tier, so silently
                // accepting a `Transition` rule would mislead an operator expecting cost savings.
                // Fail loudly with NotImplemented instead of storing a no-op (release fix 0.3).
                let rules = cairn_lifecycle::parse_lifecycle(&doc)?;
                if rules
                    .iter()
                    .flat_map(|r| r.actions.iter())
                    .any(|a| matches!(a, cairn_lifecycle::Action::Transition(_)))
                {
                    return Err(Error::NotImplemented);
                }
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

    // --- ACL / Block Public Access / Object Ownership subresources ---

    async fn get_bucket_acl(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let acl = match self
            .meta
            .get_bucket_config(&bucket.name, ConfigAspect::Acl)
            .await?
        {
            Some(doc) => serde_json::from_str(&doc.0)
                .map_err(|_| Error::Internal("config (de)serialization failed".to_owned()))?,
            None => default_owner_acl(&bucket.owner_id),
        };
        Ok(S3Response::xml(StatusCode::OK, acl_to_xml(&acl))
            .with_header("x-amz-request-id", &req.request_id))
    }

    /// `PutBucketAcl` (ARCH 13.2, 15.4): set the bucket ACL from either a canned `x-amz-acl`
    /// header or a parsed `AccessControlPolicy` request body. ACLs are disabled under
    /// `BucketOwnerEnforced` ownership (S3: `AccessControlListNotSupported`).
    async fn put_bucket_acl(
        &self,
        req: S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
        // ACLs are disabled under BucketOwnerEnforced (S3: AccessControlListNotSupported, 400).
        if bucket.ownership_mode == OwnershipMode::BucketOwnerEnforced {
            return Err(Error::AclNotSupported);
        }
        let acl = self.resolve_acl_input(&req, body, &bucket.owner_id).await?;
        let doc = serde_json::to_string(&acl)
            .map_err(|_| Error::Internal("config (de)serialization failed".to_owned()))?;
        self.meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket.name,
                aspect: ConfigAspect::Acl,
                doc: Some(ConfigDoc(doc)),
            })
            .await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    async fn get_object_acl(&self, req: &S3Request) -> Result<S3Response> {
        let (row, bucket) = self.resolve_object(req).await?;
        let acl = row
            .acl
            .unwrap_or_else(|| default_owner_acl(&bucket.owner_id));
        Ok(S3Response::xml(StatusCode::OK, acl_to_xml(&acl))
            .with_header("x-amz-request-id", &req.request_id))
    }

    /// `GetObjectAttributes` (ARCH 21.3, 34.3): return an object's metadata attributes —
    /// `ETag`, `ObjectSize`, `StorageClass`, any stored `Checksum`, and `ObjectParts` when the
    /// object is multipart and its parts can still be enumerated. Cairn discards a session's part
    /// rows on completion, so `ObjectParts` is omitted for completed multipart objects (S3 also
    /// returns it only while the parts are retrievable).
    async fn get_object_attributes(&self, req: &S3Request) -> Result<S3Response> {
        let (row, _bucket) = self.resolve_object(req).await?;
        let body = cairn_xml::get_object_attributes(
            &row.etag,
            row.size_logical,
            row.storage_class,
            &row.checksums,
            None,
        );
        let mut resp =
            S3Response::xml(StatusCode::OK, body).with_header("x-amz-request-id", &req.request_id);
        if !row.version_id.is_null() {
            resp = resp.with_header("x-amz-version-id", row.version_id.as_str());
        }
        resp = resp.with_header("last-modified", http_date(row.updated_at));
        Ok(resp)
    }

    /// `PutObjectAcl` (ARCH 13.3, 15.4): replace the current (or `?versionId`) object version's
    /// ACL with either a canned `x-amz-acl` header or a parsed `AccessControlPolicy` request body.
    /// ACLs are disabled under `BucketOwnerEnforced` ownership (S3: `AccessControlListNotSupported`).
    async fn put_object_acl(
        &self,
        req: &S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let (row, bucket) = self.resolve_object(req).await?;
        // Under BucketOwnerEnforced ownership ACLs are disabled; reject with the
        // AccessControlListNotSupported semantics (400), as PutBucketAcl does.
        if bucket.ownership_mode == OwnershipMode::BucketOwnerEnforced {
            return Err(Error::AclNotSupported);
        }
        let acl = self.resolve_acl_input(req, body, &bucket.owner_id).await?;
        self.meta
            .submit(Mutation::SetObjectAcl {
                bucket: bucket.name,
                key: row.key,
                version_id: row.version_id,
                acl: Some(acl),
            })
            .await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    /// Resolve the ACL an ACL-setting request asks for: a canned `x-amz-acl` header takes
    /// precedence; otherwise an `AccessControlPolicy` request body is parsed. An empty body with
    /// no canned header is rejected as a malformed request.
    async fn resolve_acl_input(
        &self,
        req: &S3Request,
        body: cairn_types::BodyStream,
        owner: &cairn_types::id::UserId,
    ) -> Result<Acl> {
        if let Some(canned) = req.header("x-amz-acl") {
            return cairn_authz::expand_canned_acl(canned, owner)
                .ok_or_else(|| Error::InvalidArgument("invalid canned ACL".to_owned()));
        }
        let doc = drain_body(body, 256 * 1024).await?;
        if doc.iter().all(u8::is_ascii_whitespace) {
            return Err(Error::InvalidArgument(
                "missing x-amz-acl or AccessControlPolicy body".to_owned(),
            ));
        }
        cairn_xml::parse_access_control_policy(&doc)
    }

    async fn get_public_access_block(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        match self
            .meta
            .get_bucket_config(&bucket.name, ConfigAspect::PublicAccessBlock)
            .await?
        {
            Some(doc) => {
                let bpa: PublicAccessBlock = serde_json::from_str(&doc.0)
                    .map_err(|_| Error::Internal("config (de)serialization failed".to_owned()))?;
                Ok(
                    S3Response::xml(StatusCode::OK, public_access_block_to_xml(&bpa))
                        .with_header("x-amz-request-id", &req.request_id),
                )
            }
            None => Ok(S3Response::xml(
                StatusCode::NOT_FOUND,
                cairn_xml::error_document(
                    "NoSuchPublicAccessBlockConfiguration",
                    "The public access block configuration was not found",
                    &resource_path(req),
                    &req.request_id,
                ),
            )),
        }
    }

    async fn put_public_access_block(
        &self,
        req: S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
        let doc = drain_body(body, 64 * 1024).await?;
        let bpa = parse_public_access_block(&doc);
        let json = serde_json::to_string(&bpa)
            .map_err(|_| Error::Internal("config (de)serialization failed".to_owned()))?;
        self.meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket.name,
                aspect: ConfigAspect::PublicAccessBlock,
                doc: Some(ConfigDoc(json)),
            })
            .await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    async fn get_ownership_controls(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        Ok(
            S3Response::xml(StatusCode::OK, ownership_to_xml(bucket.ownership_mode))
                .with_header("x-amz-request-id", &req.request_id),
        )
    }

    async fn put_ownership_controls(
        &self,
        req: S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
        let doc = drain_body(body, 64 * 1024).await?;
        let mode = parse_ownership(&doc)
            .ok_or_else(|| Error::InvalidArgument("invalid request body".to_owned()))?;
        self.meta
            .submit(Mutation::SetOwnership {
                bucket: bucket.name,
                mode,
            })
            .await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    // --- object tagging ---

    async fn get_object_tagging(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        let row = match req.query("versionId") {
            // Tag GET/PUT/DELETE operate on the named version when `?versionId` is given, not
            // always the current one (audit #15) — matching PutObjectAcl's resolution.
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
        let row = match req.query("versionId") {
            // Tag GET/PUT/DELETE operate on the named version when `?versionId` is given, not
            // always the current one (audit #15) — matching PutObjectAcl's resolution.
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
        let doc = drain_body(body, 64 * 1024).await?;
        let tags = cairn_xml::parse_tagging(&doc)?;
        cairn_xml::validate_tags(&tags, cairn_xml::MAX_TAGS_OBJECT)?;
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
        let row = match req.query("versionId") {
            // Tag GET/PUT/DELETE operate on the named version when `?versionId` is given, not
            // always the current one (audit #15) — matching PutObjectAcl's resolution.
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

    // --- object lock: retention / legal hold (WORM, ARCH 13/15) ---

    /// Resolve the version a `?retention`/`?legal-hold` request targets: the named `?versionId`
    /// when present, otherwise the current version (matching tagging/ACL resolution).
    async fn resolve_lock_target(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        req: &S3Request,
    ) -> Result<ObjectVersionRow> {
        match req.query("versionId") {
            Some(vid) => self
                .meta
                .get_version(bucket, key, &VersionId::from_string(vid.to_owned()))
                .await?
                .ok_or(Error::NoSuchVersion),
            None => self
                .meta
                .current_version(bucket, key)
                .await?
                .ok_or(Error::NoSuchKey),
        }
    }

    /// Read a bucket's Object Lock configuration (default = disabled when unset/corrupt-but-absent).
    async fn bucket_object_lock_config(
        &self,
        bucket: &BucketName,
    ) -> Result<ObjectLockConfiguration> {
        match self
            .meta
            .get_bucket_config(bucket, ConfigAspect::ObjectLock)
            .await?
        {
            Some(doc) => serde_json::from_str(&doc.0)
                .map_err(|_| Error::Internal("config (de)serialization failed".to_owned())),
            None => Ok(ObjectLockConfiguration::default()),
        }
    }

    async fn get_object_retention(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        let row = self.resolve_lock_target(&bucket.name, &key, req).await?;
        let state = self
            .meta
            .get_object_lock(&bucket.name, &key, &row.version_id)
            .await?;
        Ok(S3Response::xml(
            StatusCode::OK,
            cairn_xml::retention_to_xml(state.retention.as_ref()),
        )
        .with_header("x-amz-request-id", &req.request_id))
    }

    async fn put_object_retention(
        &self,
        req: S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
        let key = req.key.clone().expect("key present");
        // Retention can only be set on a bucket that has Object Lock enabled.
        if !self.bucket_object_lock_config(&bucket.name).await?.enabled {
            return Err(Error::InvalidRequest(
                "Bucket is missing Object Lock Configuration".to_owned(),
            ));
        }
        let row = self.resolve_lock_target(&bucket.name, &key, &req).await?;
        let doc = drain_body(body, 64 * 1024).await?;
        let new = cairn_xml::parse_retention(&doc)?;
        let now = self.clock.now();
        // A retain-until in the past would be an immediately-expired no-op "lock" — reject it rather
        // than store a misleading row.
        if new.retain_until <= now {
            return Err(Error::InvalidArgument(
                "x-amz-object-lock retain-until-date must be in the future".to_owned(),
            ));
        }
        // Lowering or removing an active retention is governed: COMPLIANCE is never weakenable;
        // GOVERNANCE requires `s3:BypassGovernanceRetention` + the bypass header.
        let current = self
            .meta
            .get_object_lock(&bucket.name, &key, &row.version_id)
            .await?;
        if let Some(cur) = current.retention {
            if cur.retain_until > now && retention_is_weaker(&cur, &new) {
                match cur.mode {
                    ObjectLockMode::Compliance => return Err(Error::AccessDenied),
                    ObjectLockMode::Governance => {
                        self.require_governance_bypass(&req, &bucket, &key, &row.version_id)
                            .await?;
                    }
                }
            }
        }
        self.meta
            .submit(Mutation::SetObjectRetention {
                bucket: bucket.name,
                key,
                version_id: row.version_id,
                retention: Some(new),
            })
            .await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    async fn get_object_legal_hold(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        let row = self.resolve_lock_target(&bucket.name, &key, req).await?;
        let state = self
            .meta
            .get_object_lock(&bucket.name, &key, &row.version_id)
            .await?;
        Ok(S3Response::xml(
            StatusCode::OK,
            cairn_xml::legal_hold_to_xml(state.legal_hold),
        )
        .with_header("x-amz-request-id", &req.request_id))
    }

    async fn put_object_legal_hold(
        &self,
        req: S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
        let key = req.key.clone().expect("key present");
        if !self.bucket_object_lock_config(&bucket.name).await?.enabled {
            return Err(Error::InvalidRequest(
                "Bucket is missing Object Lock Configuration".to_owned(),
            ));
        }
        let row = self.resolve_lock_target(&bucket.name, &key, &req).await?;
        let doc = drain_body(body, 64 * 1024).await?;
        let on = cairn_xml::parse_legal_hold(&doc)?;
        self.meta
            .submit(Mutation::SetObjectLegalHold {
                bucket: bucket.name,
                key,
                version_id: row.version_id,
                on,
            })
            .await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    /// Enforce the GOVERNANCE-bypass gate: the `x-amz-bypass-governance-retention: true` header
    /// AND `s3:BypassGovernanceRetention` on the object. COMPLIANCE retention is never bypassable;
    /// this path is only ever reached for GOVERNANCE-mode locks.
    async fn require_governance_bypass(
        &self,
        req: &S3Request,
        bucket: &Bucket,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<()> {
        let header_set = req
            .header("x-amz-bypass-governance-retention")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        if !header_set {
            return Err(Error::AccessDenied);
        }
        self.authorize(
            req,
            bucket,
            Action::BypassGovernanceRetention,
            Resource::Object {
                bucket: bucket.name.clone(),
                key: key.clone(),
            },
            Some(version),
        )
        .await
    }

    /// Deny a permanent delete (or permanent overwrite-away) of a still-protected version
    /// (ARCH 15, WORM). Legal hold always denies; an active COMPLIANCE retention always denies;
    /// an active GOVERNANCE retention denies unless the request carries `s3:BypassGovernanceRetention`
    /// plus the `x-amz-bypass-governance-retention: true` header. A version with no active lock
    /// passes. This is the single chokepoint every permanent-removal path funnels through.
    async fn enforce_unlocked_for_delete(
        &self,
        req: &S3Request,
        bucket: &Bucket,
        key: &ObjectKey,
        version: &VersionId,
        now: cairn_types::Timestamp,
    ) -> Result<()> {
        let lock = self
            .meta
            .get_object_lock(&bucket.name, key, version)
            .await?;
        if lock.legal_hold {
            return Err(Error::AccessDenied);
        }
        if let Some(ret) = lock.retention {
            if ret.retain_until > now {
                match ret.mode {
                    ObjectLockMode::Compliance => return Err(Error::AccessDenied),
                    ObjectLockMode::Governance => {
                        self.require_governance_bypass(req, bucket, key, version)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn get_bucket_object_lock(&self, req: &S3Request) -> Result<S3Response> {
        let bucket = self.fetch_bucket(req).await?;
        let config = self.bucket_object_lock_config(&bucket.name).await?;
        if !config.enabled {
            return Ok(S3Response::xml(
                StatusCode::NOT_FOUND,
                cairn_xml::error_document(
                    "ObjectLockConfigurationNotFoundError",
                    "Object Lock configuration does not exist for this bucket",
                    &resource_path(req),
                    &req.request_id,
                ),
            ));
        }
        Ok(S3Response::xml(
            StatusCode::OK,
            cairn_xml::object_lock_configuration_to_xml(&config),
        )
        .with_header("x-amz-request-id", &req.request_id))
    }

    async fn put_bucket_object_lock(
        &self,
        req: S3Request,
        body: cairn_types::BodyStream,
    ) -> Result<S3Response> {
        let bucket = self.fetch_bucket(&req).await?;
        let doc = drain_body(body, 64 * 1024).await?;
        let config = cairn_xml::parse_object_lock_configuration(&doc)?;
        // Object Lock requires versioning: a WORM guarantee is meaningless if an overwrite can
        // destroy the protected bytes in place.
        if config.enabled && bucket.versioning != VersioningState::Enabled {
            return Err(Error::InvalidRequest(
                "Object Lock requires bucket versioning to be enabled".to_owned(),
            ));
        }
        let text = serde_json::to_string(&config)
            .map_err(|_| Error::Internal("config (de)serialization failed".to_owned()))?;
        self.meta
            .submit(Mutation::SetBucketConfig {
                bucket: bucket.name,
                aspect: ConfigAspect::ObjectLock,
                doc: Some(ConfigDoc(text)),
            })
            .await?;
        Ok(S3Response::status(StatusCode::OK).with_header("x-amz-request-id", &req.request_id))
    }

    /// Apply Object Lock protections to a freshly-written version (ARCH 15): explicit
    /// `x-amz-object-lock-*` request headers win; otherwise the bucket's default retention (if any)
    /// is stamped. A no-op (one cached config read) unless the bucket has Object Lock enabled.
    async fn apply_object_lock_on_put(
        &self,
        req: &S3Request,
        bucket: &Bucket,
        key: &ObjectKey,
        version_id: &VersionId,
        now: cairn_types::Timestamp,
    ) -> Result<()> {
        let config = self.bucket_object_lock_config(&bucket.name).await?;
        if !config.enabled {
            return Ok(());
        }
        // Explicit retention headers take precedence over the bucket default; mode and
        // retain-until-date must be supplied together.
        let retention = match (
            req.header("x-amz-object-lock-mode"),
            req.header("x-amz-object-lock-retain-until-date"),
        ) {
            (Some(m), Some(d)) => {
                let mode = cairn_xml::parse_lock_mode(m)?;
                let retain_until = cairn_xml::parse_iso8601(d).ok_or_else(|| {
                    Error::InvalidArgument("invalid x-amz-object-lock-retain-until-date".to_owned())
                })?;
                if retain_until <= now {
                    return Err(Error::InvalidArgument(
                        "x-amz-object-lock-retain-until-date must be in the future".to_owned(),
                    ));
                }
                Some(ObjectRetention { mode, retain_until })
            }
            (None, None) => config.default_retention.map(|d| ObjectRetention {
                mode: d.mode,
                retain_until: d.retain_until(now),
            }),
            _ => {
                return Err(Error::InvalidRequest(
                    "x-amz-object-lock-mode and x-amz-object-lock-retain-until-date must be \
                     supplied together"
                        .to_owned(),
                ));
            }
        };
        if let Some(retention) = retention {
            self.meta
                .submit(Mutation::SetObjectRetention {
                    bucket: bucket.name.clone(),
                    key: key.clone(),
                    version_id: version_id.clone(),
                    retention: Some(retention),
                })
                .await?;
        }
        if req
            .header("x-amz-object-lock-legal-hold")
            .is_some_and(|h| h.eq_ignore_ascii_case("ON"))
        {
            self.meta
                .submit(Mutation::SetObjectLegalHold {
                    bucket: bucket.name.clone(),
                    key: key.clone(),
                    version_id: version_id.clone(),
                    on: true,
                })
                .await?;
        }
        Ok(())
    }

    /// Append `x-amz-object-lock-*` headers to a GET/HEAD response when the bucket has Object Lock
    /// enabled and the version carries protection. Buckets without Object Lock pay only one cached
    /// config read; the per-version lock read happens only for lock-enabled buckets.
    async fn append_lock_headers(
        &self,
        resp: S3Response,
        bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<S3Response> {
        if !self.bucket_object_lock_config(bucket).await?.enabled {
            return Ok(resp);
        }
        let state = self.meta.get_object_lock(bucket, key, version).await?;
        let mut resp = resp;
        if let Some(ret) = state.retention {
            resp = resp
                .with_header("x-amz-object-lock-mode", cairn_xml::lock_mode_str(ret.mode))
                .with_header(
                    "x-amz-object-lock-retain-until-date",
                    cairn_xml::format_iso8601(ret.retain_until),
                );
        }
        Ok(resp.with_header(
            "x-amz-object-lock-legal-hold",
            if state.legal_hold { "ON" } else { "OFF" },
        ))
    }

    /// Emit event notifications for an object event (ARCH 20-style). Reads the bucket's webhook
    /// configuration (one cached read; a no-op for buckets without notifications), matches each
    /// endpoint by event + prefix/suffix, renders the S3-event-record JSON, and enqueues a delivery
    /// entry per match. Best-effort: a notification failure never fails the object operation.
    #[allow(clippy::too_many_arguments)]
    async fn emit_events(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: &VersionId,
        event: EventKind,
        size: Option<u64>,
        etag: Option<&str>,
        now: cairn_types::Timestamp,
    ) {
        if let Err(e) = self
            .try_emit_events(bucket, key, version_id, event, size, etag, now)
            .await
        {
            tracing::warn!(bucket = %bucket.as_str(), key = %key.as_str(), error = ?e, "event notification enqueue failed (best-effort)");
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_emit_events(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: &VersionId,
        event: EventKind,
        size: Option<u64>,
        etag: Option<&str>,
        now: cairn_types::Timestamp,
    ) -> Result<()> {
        let config = match self
            .meta
            .get_bucket_config(bucket, ConfigAspect::Notification)
            .await?
        {
            Some(doc) => serde_json::from_str::<NotificationConfig>(&doc.0).unwrap_or_default(),
            None => return Ok(()),
        };
        if config.is_empty() {
            return Ok(());
        }
        let mut entries = Vec::new();
        for ep in &config.endpoints {
            if !ep.matches(event, key.as_str()) {
                continue;
            }
            let payload = build_event_payload(
                &self.region,
                bucket,
                key,
                version_id,
                event,
                size,
                etag,
                now,
            );
            entries.push(WebhookEntry {
                // A unique id per emitted event: each distinct object event is one at-least-once
                // delivery. (A version-derived id would collide across keys in an unversioned
                // bucket, where every object shares the `null` sentinel version.) The endpoint id
                // prefix keeps the row readable in the outbox.
                id: format!("{}-{}", ep.id, uuid::Uuid::new_v4().simple()),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version_id.clone(),
                event,
                endpoint_id: ep.id.clone(),
                payload,
                attempts: 0,
                next_attempt_at: now,
                status: WebhookStatus::Pending,
                last_error: None,
                priority: 0,
                lease_until: None,
            });
        }
        if !entries.is_empty() {
            self.meta.submit(Mutation::EnqueueWebhooks(entries)).await?;
        }
        Ok(())
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

    /// Evaluate the full authorization decision (ARCH 15): assemble the requester class, the
    /// account/bucket Block Public Access settings, the bucket policy, and the request context,
    /// then run the pure policy engine.
    async fn authorize(
        &self,
        req: &S3Request,
        bucket: &Bucket,
        action: Action,
        resource: Resource,
        // The object version whose ACL + tags the policy conditions evaluate against, when a
        // specific version is being acted on (a `?versionId` read, or a versioned copy source).
        // `None` selects the current version (audit #33).
        acl_version: Option<&VersionId>,
    ) -> Result<()> {
        let requester = match req.principal.as_ref() {
            // A temporary session credential is least-privilege: it never inherits the owner/admin
            // short-circuit, so its access is exactly what its scoped policy grants (ARCH 14).
            Some(p) if p.is_session => RequesterClass::AuthenticatedMember(p.user_id.clone()),
            Some(p) if p.role == Role::Administrator || p.user_id == bucket.owner_id => {
                RequesterClass::OwnerOrAdmin
            }
            Some(p) => RequesterClass::AuthenticatedMember(p.user_id.clone()),
            None => RequesterClass::Anonymous,
        };
        let account_bpa = self.meta.get_account_public_access_block().await?;
        // Corrupt security configs MUST fail closed (ARCH 15.3/15.5): a BPA doc that does not
        // parse must not silently open access, and an unparseable policy must not silently drop
        // its (possibly Deny) statements.
        let bucket_bpa = match self
            .meta
            .get_bucket_config(&bucket.name, ConfigAspect::PublicAccessBlock)
            .await?
        {
            Some(doc) => serde_json::from_str(&doc.0)
                .map_err(|_| Error::Internal("config (de)serialization failed".to_owned()))?,
            None => PublicAccessBlock::default(),
        };
        let policy = match self
            .meta
            .get_bucket_config(&bucket.name, ConfigAspect::Policy)
            .await?
        {
            Some(doc) => Some(
                cairn_authz::parse_policy(&doc.0)
                    .map_err(|_| Error::Internal("config (de)serialization failed".to_owned()))?,
            ),
            None => None,
        };
        // Resolve the version whose ACL + tags the policy conditions evaluate against (audit #33):
        // the specific version when one is named (a `?versionId` read, or a versioned copy source),
        // otherwise the current version. Evaluating against the current version when a *different*
        // version is being acted on would gate `s3:ExistingObjectTag` / object-ACL conditions on
        // the wrong version's metadata. Loaded once and shared by the ACL and tag paths below.
        let acl_row = match &resource {
            Resource::Object { key, .. } => match acl_version {
                Some(vid) => self.meta.get_version(&bucket.name, key, vid).await?,
                None => self.meta.current_version(&bucket.name, key).await?,
            },
            Resource::Bucket(_) => None,
        };
        // Load ACLs only when ownership mode keeps them enabled; under BucketOwnerEnforced
        // (the default) ACLs are disabled, so this stays a no-op on the hot path.
        let (bucket_acl, object_acl) =
            if bucket.ownership_mode == cairn_types::authz::OwnershipMode::BucketOwnerEnforced {
                (None, None)
            } else {
                let bucket_acl = match self
                    .meta
                    .get_bucket_config(&bucket.name, ConfigAspect::Acl)
                    .await?
                {
                    Some(doc) => Some(serde_json::from_str(&doc.0).map_err(|_| {
                        Error::Internal("config (de)serialization failed".to_owned())
                    })?),
                    None => None,
                };
                (bucket_acl, acl_row.as_ref().and_then(|row| row.acl.clone()))
            };
        // Load the existing object's tags and parse any request-supplied tags so policies
        // conditioned on s3:ExistingObjectTag/aws:RequestTag evaluate correctly (ARCH 15.6,
        // Medium #5) — against the resolved version (audit #33). The request tags come from the
        // inline `x-amz-tagging` header (a `PutObject`/copy form-encoded tag set).
        let existing_tags = match (&resource, &acl_row) {
            (Resource::Object { key, .. }, Some(row)) => {
                self.meta
                    .get_object_tags(&bucket.name, key, &row.version_id)
                    .await?
            }
            _ => Vec::new(),
        };
        let request_tags = parse_tagging_header(req.header("x-amz-tagging"));
        let mut context = build_context(req, self.clock.now());
        context.existing_tags = existing_tags;
        context.request_tags = request_tags;
        // The requester's attached identity policy (loaded at auth time), evaluated in union with
        // the bucket policy. `None` for anonymous requesters and users without one.
        let user_policy = req
            .principal
            .as_ref()
            .and_then(|p| p.user_policy.as_deref().cloned());
        let input = AuthzInput {
            requester,
            action,
            resource,
            bucket_owner: bucket.owner_id.clone(),
            account_bpa,
            bucket_bpa,
            policy,
            user_policy,
            bucket_acl,
            object_acl,
            ownership_mode: bucket.ownership_mode,
            context,
        };
        match self.authz.evaluate(&input) {
            Decision::Allow => Ok(()),
            Decision::Deny(_) => Err(Error::AccessDenied),
        }
    }

    /// Build a replication outbox entry for a write when the bucket has an enabled replication
    /// rule matching the key (ARCH 20). Replication requires versioning. The entry rides the same
    /// commit transaction as the write, so enqueue is atomic with the version.
    ///
    /// For an object-create the rule must match the key AND the object's `tags` (the full
    /// [`Filter::matches`] predicate, ARCH 20). For a [`ReplicationOp::DeleteMarker`] the rule
    /// must additionally have `delete_marker_replication` enabled (ARCH 20.3/21.5); delete
    /// markers carry no tags, so an empty tag set is used for the prefix-only match.
    async fn replication_outbox(
        &self,
        bucket: &Bucket,
        key: &ObjectKey,
        version_id: &VersionId,
        op: ReplicationOp,
        tags: &[(String, String)],
    ) -> Vec<OutboxEntry> {
        if bucket.versioning != VersioningState::Enabled {
            return Vec::new();
        }
        let Some(doc) = self
            .meta
            .get_bucket_config(&bucket.name, ConfigAspect::Replication)
            .await
            .ok()
            .flatten()
        else {
            return Vec::new();
        };
        let Ok(cfg) = cairn_replication::parse_replication(doc.0.as_bytes()) else {
            return Vec::new();
        };
        let is_marker = op == ReplicationOp::DeleteMarker;
        // Fan-out (ARCH 20.2): one outbox entry per distinct destination target whose
        // highest-priority rule matches. A single matching target — or keyspace-partitioned rules
        // with disjoint prefixes — yields exactly one entry (the prior single-winner behaviour).
        let now = self.clock.now();
        cfg.matching_rules_for_all(key.as_str(), tags, is_marker)
            .into_iter()
            .map(|rule| {
                cairn_replication::outbox_entry_for(
                    uuid::Uuid::new_v4().simple().to_string(),
                    bucket.name.clone(),
                    key.clone(),
                    version_id.clone(),
                    op,
                    rule.id.clone(),
                    rule.target_arn.clone(),
                    now,
                    rule.priority,
                )
            })
            .collect()
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

    /// Resolve a GET/HEAD read target with full delete-marker fidelity (ARCH 16, Medium #4).
    ///
    /// - A plain read (no `?versionId`) of a key whose latest version is a delete marker returns a
    ///   404 carrying `x-amz-delete-marker: true` and the marker's `x-amz-version-id`.
    /// - A read that names a delete marker's OWN `?versionId` returns 405 `MethodNotAllowed`
    ///   (a delete marker has no retrievable content), not 404.
    /// - Otherwise the live object version is returned.
    async fn resolve_read_target(&self, req: &S3Request) -> Result<ReadTarget> {
        let bucket = self.fetch_bucket(req).await?;
        let key = req.key.clone().expect("key present");
        match req.query("versionId") {
            Some(vid) => {
                let row = self
                    .meta
                    .get_version(&bucket.name, &key, &VersionId::from_string(vid.to_owned()))
                    .await?
                    .ok_or(Error::NoSuchVersion)?;
                if row.is_delete_marker {
                    // Naming a delete marker's own version is a 405, not a 404.
                    return Ok(ReadTarget::DeleteMarker(method_not_allowed_marker(
                        req, &row,
                    )));
                }
                Ok(ReadTarget::Object(Box::new(row)))
            }
            None => {
                let row = self
                    .meta
                    .current_version(&bucket.name, &key)
                    .await?
                    .ok_or(Error::NoSuchKey)?;
                if row.is_delete_marker {
                    return Ok(ReadTarget::DeleteMarker(not_found_marker(req, &row)));
                }
                Ok(ReadTarget::Object(Box::new(row)))
            }
        }
    }
}

/// The outcome of resolving a GET/HEAD target: a live object version, or a delete-marker response
/// (a 404 for a hidden current version, or a 405 for a directly-named marker version).
enum ReadTarget {
    Object(Box<ObjectVersionRow>),
    DeleteMarker(S3Response),
}

/// The 404 returned when the latest version of a key is a delete marker: the marker's identity is
/// signaled via `x-amz-delete-marker` and `x-amz-version-id` (ARCH 16.1, Medium #4).
fn not_found_marker(req: &S3Request, marker: &ObjectVersionRow) -> S3Response {
    let body = cairn_xml::error_document(
        "NoSuchKey",
        "The specified key does not exist.",
        &resource_path(req),
        &req.request_id,
    );
    let mut resp = S3Response::xml(StatusCode::NOT_FOUND, body)
        .with_header("x-amz-delete-marker", "true")
        .with_header("x-amz-request-id", &req.request_id);
    if !marker.version_id.is_null() {
        resp = resp.with_header("x-amz-version-id", marker.version_id.as_str());
    }
    resp
}

/// The 405 returned when a GET/HEAD names a delete marker's own version id (ARCH 16.1, Medium #4).
fn method_not_allowed_marker(req: &S3Request, marker: &ObjectVersionRow) -> S3Response {
    let body = cairn_xml::error_document(
        "MethodNotAllowed",
        "The specified method is not allowed against this resource.",
        &resource_path(req),
        &req.request_id,
    );
    let mut resp = S3Response::xml(StatusCode::METHOD_NOT_ALLOWED, body)
        .with_header("x-amz-delete-marker", "true")
        .with_header("x-amz-request-id", &req.request_id);
    if !marker.version_id.is_null() {
        resp = resp.with_header("x-amz-version-id", marker.version_id.as_str());
    }
    resp
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

/// Known S3 object subresource selectors that we do NOT serve via a handler; their presence must
/// route to NotImplemented rather than fall through to a data-plane handler (which would let
/// `PUT key?acl` overwrite the object body). `acl` (GET/PUT) and `attributes` (GET) are handled
/// earlier; an `acl` DELETE — which S3 has no operation for — lands here.
const UNHANDLED_OBJECT_SUBRESOURCES: &[&str] = &[
    "acl",
    "retention",
    "legal-hold",
    "torrent",
    "restore",
    "select",
];

/// Known S3 bucket subresource selectors we do not serve (handled ones are matched earlier).
const UNHANDLED_BUCKET_SUBRESOURCES: &[&str] = &[
    "accelerate",
    "analytics",
    "encryption",
    "intelligent-tiering",
    "inventory",
    "logging",
    "metrics",
    "notification",
    "object-lock",
    "policyStatus",
    "requestPayment",
    "website",
];

fn unhandled_object_subresource(req: &S3Request) -> bool {
    UNHANDLED_OBJECT_SUBRESOURCES
        .iter()
        .any(|k| req.has_query(k))
}

fn unhandled_bucket_subresource(req: &S3Request) -> bool {
    UNHANDLED_BUCKET_SUBRESOURCES
        .iter()
        .any(|k| req.has_query(k))
}

/// The default private ACL: the owner has full control.
fn default_owner_acl(owner: &cairn_types::id::UserId) -> Acl {
    Acl {
        owner: owner.clone(),
        grants: vec![Grant {
            grantee: Grantee::User(owner.clone()),
            permission: Permission::FullControl,
        }],
    }
}

/// Serialize an ACL to an S3 `AccessControlPolicy` document.
fn acl_to_xml(acl: &Acl) -> String {
    let mut grants = String::new();
    for g in &acl.grants {
        let (gtype, ident) = match &g.grantee {
            Grantee::User(u) => ("CanonicalUser", format!("<ID>{}</ID>", u.0)),
            Grantee::AllUsers => (
                "Group",
                "<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>".to_owned(),
            ),
            Grantee::AuthenticatedUsers => (
                "Group",
                "<URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI>".to_owned(),
            ),
            Grantee::LogDelivery => (
                "Group",
                "<URI>http://acs.amazonaws.com/groups/s3/LogDelivery</URI>".to_owned(),
            ),
        };
        let perm = match g.permission {
            Permission::FullControl => "FULL_CONTROL",
            Permission::Read => "READ",
            Permission::Write => "WRITE",
            Permission::ReadAcp => "READ_ACP",
            Permission::WriteAcp => "WRITE_ACP",
        };
        grants.push_str(&format!(
            "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"{gtype}\">{ident}</Grantee><Permission>{perm}</Permission></Grant>"
        ));
    }
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner><ID>{}</ID></Owner><AccessControlList>{grants}</AccessControlList></AccessControlPolicy>",
        acl.owner.0
    )
}

/// Parse the four toggles of a `PublicAccessBlockConfiguration` document (tolerant scan).
fn parse_public_access_block(doc: &[u8]) -> PublicAccessBlock {
    let s = String::from_utf8_lossy(doc);
    let flag = |tag: &str| -> bool {
        match s.find(&format!("<{tag}>")) {
            Some(i) => s[i + tag.len() + 2..]
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("true"),
            None => false,
        }
    };
    PublicAccessBlock {
        block_public_acls: flag("BlockPublicAcls"),
        ignore_public_acls: flag("IgnorePublicAcls"),
        block_public_policy: flag("BlockPublicPolicy"),
        restrict_public_buckets: flag("RestrictPublicBuckets"),
    }
}

fn public_access_block_to_xml(b: &PublicAccessBlock) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><PublicAccessBlockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><BlockPublicAcls>{}</BlockPublicAcls><IgnorePublicAcls>{}</IgnorePublicAcls><BlockPublicPolicy>{}</BlockPublicPolicy><RestrictPublicBuckets>{}</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
        b.block_public_acls, b.ignore_public_acls, b.block_public_policy, b.restrict_public_buckets
    )
}

fn ownership_to_xml(mode: OwnershipMode) -> String {
    let m = match mode {
        OwnershipMode::BucketOwnerEnforced => "BucketOwnerEnforced",
        OwnershipMode::BucketOwnerPreferred => "BucketOwnerPreferred",
        OwnershipMode::ObjectWriter => "ObjectWriter",
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><OwnershipControls xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ObjectOwnership>{m}</ObjectOwnership></Rule></OwnershipControls>"
    )
}

fn parse_ownership(doc: &[u8]) -> Option<OwnershipMode> {
    let s = String::from_utf8_lossy(doc);
    if s.contains("BucketOwnerEnforced") {
        Some(OwnershipMode::BucketOwnerEnforced)
    } else if s.contains("BucketOwnerPreferred") {
        Some(OwnershipMode::BucketOwnerPreferred)
    } else if s.contains("ObjectWriter") {
        Some(OwnershipMode::ObjectWriter)
    } else {
        None
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
        Method::PUT if q("acl") => PutBucketAcl,
        Method::PUT if q("publicAccessBlock") => PutBucketPublicAccessBlock,
        Method::PUT if q("ownershipControls") => PutBucketOwnershipControls,
        Method::PUT if q("object-lock") => PutBucketObjectLockConfiguration,
        Method::PUT => CreateBucket,
        Method::DELETE if q("tagging") => PutBucketTagging,
        Method::DELETE if q("cors") => PutBucketCors,
        Method::DELETE if q("policy") => PutBucketPolicy,
        Method::DELETE if q("lifecycle") => PutLifecycleConfiguration,
        Method::DELETE if q("replication") => PutReplicationConfiguration,
        Method::DELETE if q("publicAccessBlock") => PutBucketPublicAccessBlock,
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
        Method::GET if q("acl") => GetBucketAcl,
        Method::GET if q("publicAccessBlock") => GetBucketPublicAccessBlock,
        Method::GET if q("ownershipControls") => GetBucketOwnershipControls,
        Method::GET if q("object-lock") => GetBucketObjectLockConfiguration,
        Method::POST if q("delete") => DeleteObject,
        Method::GET => ListBucket,
        _ => return Err(Error::NotImplemented),
    })
}

/// Render the S3-event-notification record JSON for one object event. The shape mirrors S3's
/// `Records[].s3` envelope so existing webhook consumers parse it unchanged; `eventSource` is
/// `aws:s3` for that drop-in compatibility while `awsRegion` carries Cairn's configured region.
/// `ObjectRemoved:*` events omit size/eTag (as S3 does).
#[allow(clippy::too_many_arguments)]
fn build_event_payload(
    region: &str,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: &VersionId,
    event: EventKind,
    size: Option<u64>,
    etag: Option<&str>,
    now: cairn_types::Timestamp,
) -> String {
    let mut object = serde_json::Map::new();
    object.insert("key".to_owned(), serde_json::json!(key.as_str()));
    if let Some(sz) = size {
        object.insert("size".to_owned(), serde_json::json!(sz));
    }
    if let Some(tag) = etag {
        object.insert("eTag".to_owned(), serde_json::json!(tag));
    }
    object.insert(
        "versionId".to_owned(),
        serde_json::json!(version_id.as_str()),
    );
    // The sequencer is an opaque hex ordering token consumers use to collate at-least-once
    // deliveries. The event time (epoch ms) is monotonic and meaningful even for unversioned /
    // version-less events, where the version id is the `null` sentinel and orders nothing.
    object.insert(
        "sequencer".to_owned(),
        serde_json::json!(format!("{:016X}", now.0)),
    );
    let record = serde_json::json!({
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": region,
        "eventTime": cairn_xml::format_iso8601(now),
        "eventName": event.s3_name(),
        "s3": {
            "s3SchemaVersion": "1.0",
            "bucket": {
                "name": bucket.as_str(),
                "arn": format!("arn:aws:s3:::{}", bucket.as_str()),
            },
            "object": serde_json::Value::Object(object),
        },
    });
    serde_json::json!({ "Records": [record] }).to_string()
}

/// Whether replacing `current` retention with `new` weakens protection — a shorter retain-until
/// date or a `COMPLIANCE`→`GOVERNANCE` mode downgrade. Strengthening (a later date, or
/// `GOVERNANCE`→`COMPLIANCE`) is always allowed; weakening is governed (ARCH 15).
fn retention_is_weaker(current: &ObjectRetention, new: &ObjectRetention) -> bool {
    new.retain_until < current.retain_until
        || (matches!(current.mode, ObjectLockMode::Compliance)
            && matches!(new.mode, ObjectLockMode::Governance))
}

/// Map an object-level request to the S3 action it requires.
///
/// A read or delete that names a `?versionId` maps to the version-scoped action
/// (`GetObjectVersion`/`DeleteObjectVersion`) so a policy written against those distinct actions
/// grants or denies as written (ARCH 34.4, Medium #9). The multipart lifecycle
/// (create/complete/upload-part) has no distinct `Action` variant in `cairn_types::authz::Action`
/// — only `AbortMultipartUpload` and `ListMultipartUploadParts` exist — so those stay mapped to
/// the closest existing action (`PutObject`). NOTE: add `CreateMultipartUpload`/`UploadPart` etc.
/// to the action catalogue if finer-grained multipart policy is required.
fn object_action(req: &S3Request) -> Result<Action> {
    use Action::*;
    let q = |s: &str| req.has_query(s);
    let versioned = req.query("versionId").is_some();
    Ok(match req.method {
        Method::PUT if q("tagging") => PutObjectTagging,
        Method::GET if q("tagging") => GetObjectTagging,
        Method::DELETE if q("tagging") => DeleteObjectTagging,
        Method::GET if q("acl") => GetObjectAcl,
        Method::PUT if q("acl") => PutObjectAcl,
        Method::GET if q("retention") => GetObjectRetention,
        Method::PUT if q("retention") => PutObjectRetention,
        Method::GET if q("legal-hold") => GetObjectLegalHold,
        Method::PUT if q("legal-hold") => PutObjectLegalHold,
        Method::GET if q("attributes") => GetObjectAttributes,
        Method::GET if q("uploadId") => ListMultipartUploadParts,
        Method::DELETE if q("uploadId") => AbortMultipartUpload,
        // An inbound replica PUT (`x-amz-meta-cairn-replica: true`, ARCH 20.4) authorizes against
        // `s3:ReplicateObject`, so a dedicated replication user scoped to that action is allowed
        // (and a normal writer's `s3:PutObject` grant does not silently accept replica traffic).
        // Only a genuine body PUT is reclassified — a copy (which carries `x-amz-copy-source`) stays
        // a normal write so the replica marker cannot launder a copy past `s3:PutObject`.
        Method::PUT
            if req.header("x-amz-copy-source").is_none()
                && req
                    .header("x-amz-meta-cairn-replica")
                    .is_some_and(|v| v.eq_ignore_ascii_case("true")) =>
        {
            ReplicateObject
        }
        // The multipart lifecycle (initiate/complete/upload-part) has no distinct action variant;
        // it maps to PutObject, the closest catalogued action.
        Method::PUT | Method::POST => PutObject,
        Method::GET | Method::HEAD if versioned => GetObjectVersion,
        Method::GET | Method::HEAD => GetObject,
        // An inbound replicated delete-marker propagation (`x-amz-meta-cairn-replica: true`,
        // ARCH 20.4) authorizes against `s3:ReplicateDelete`, matching the dedicated replication
        // user's policy rather than requiring full `s3:DeleteObject`.
        Method::DELETE
            if !versioned
                && req
                    .header("x-amz-meta-cairn-replica")
                    .is_some_and(|v| v.eq_ignore_ascii_case("true")) =>
        {
            ReplicateDelete
        }
        Method::DELETE if versioned => DeleteObjectVersion,
        Method::DELETE => DeleteObject,
        _ => return Err(Error::NotImplemented),
    })
}

fn quoted(etag: &ETag) -> String {
    format!("\"{}\"", etag.as_str())
}

/// The S3 algorithm token for SSE-S3 (the only server-side-encryption mode Cairn implements).
const SSE_AES256: &str = "AES256";
/// The algorithm recorded in the per-object SSE descriptor.
const SSE_DESCRIPTOR_ALG: &str = "AES256-GCM";

/// The JSON `sse_descriptor` persisted on an encrypted object version: the algorithm, the
/// data-encryption key sealed under the master key (base64), and the wrapping nonce (base64). The
/// raw DEK is never stored — only this wrapped form (ARCH 27, SSE-S3).
#[derive(serde::Serialize, serde::Deserialize)]
struct SseDescriptor {
    alg: String,
    wrapped_dek_b64: String,
    nonce_b64: String,
}

/// Whether the request asks for SSE-S3 via `x-amz-server-side-encryption: AES256`. Any other value
/// (e.g. `aws:kms`) is rejected as unsupported by the caller.
fn requested_sse(req: &S3Request) -> Option<Result<()>> {
    req.header("x-amz-server-side-encryption").map(|v| {
        if v.eq_ignore_ascii_case(SSE_AES256) {
            Ok(())
        } else {
            Err(Error::InvalidArgument(format!(
                "unsupported server-side encryption: {v}"
            )))
        }
    })
}

impl S3Service {
    /// Whether the bucket has default server-side encryption configured (the `encryption` config
    /// aspect with algorithm AES256). A malformed stored document counts as "off" rather than
    /// failing the upload — the setting is operator-managed and validated on write.
    async fn bucket_default_sse(&self, bucket: &BucketName) -> Result<bool> {
        let doc = self
            .meta
            .get_bucket_config(bucket, ConfigAspect::Encryption)
            .await?;
        Ok(doc.is_some_and(|d| {
            serde_json::from_str::<serde_json::Value>(&d.0)
                .ok()
                .and_then(|v| {
                    v.get("algorithm")
                        .and_then(|a| a.as_str())
                        .map(|a| a.eq_ignore_ascii_case(SSE_AES256))
                })
                .unwrap_or(false)
        }))
    }

    /// Generate a fresh random 32-byte DEK, seal it under the master key, and return both the raw
    /// DEK (for staging) and the JSON descriptor string (for the metadata row).
    fn new_sse_dek(&self) -> Result<([u8; 32], String)> {
        use rand::RngCore;
        let mut dek = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut dek);
        let sealed = self.crypto.seal(&dek)?;
        let descriptor = SseDescriptor {
            alg: SSE_DESCRIPTOR_ALG.to_owned(),
            // The CRK1 envelope (audit #29) is self-describing — it carries its own key id and
            // nonce — so the whole envelope goes in `wrapped_dek_b64` and the separate nonce field
            // is left empty. Legacy descriptors keep a populated `nonce_b64`; `open_sse_dek` routes
            // on its presence.
            wrapped_dek_b64: base64::engine::general_purpose::STANDARD.encode(&sealed.ciphertext),
            nonce_b64: String::new(),
        };
        let json = serde_json::to_string(&descriptor)
            .map_err(|e| Error::Internal(format!("serialize sse descriptor: {e}")))?;
        Ok((dek, json))
    }

    /// Unwrap the raw 32-byte DEK from a stored `sse_descriptor` JSON document by opening the
    /// sealed key under the master key.
    fn open_sse_dek(&self, descriptor_json: &str) -> Result<[u8; 32]> {
        let d: SseDescriptor = serde_json::from_str(descriptor_json)
            .map_err(|e| Error::Internal(format!("parse sse descriptor: {e}")))?;
        let ciphertext = base64::engine::general_purpose::STANDARD
            .decode(d.wrapped_dek_b64.as_bytes())
            .map_err(|_| Error::Internal("sse descriptor: bad wrapped key base64".to_owned()))?;
        // A CRK1 envelope leaves `nonce_b64` empty (its nonce is inside the envelope and `open`
        // ignores this argument for it); a legacy descriptor carries the nonce separately (#29).
        let nonce_bytes = if d.nonce_b64.is_empty() {
            Vec::new()
        } else {
            base64::engine::general_purpose::STANDARD
                .decode(d.nonce_b64.as_bytes())
                .map_err(|_| Error::Internal("sse descriptor: bad nonce base64".to_owned()))?
        };
        let raw = self.crypto.open(&ciphertext, &Nonce(nonce_bytes))?;
        raw.as_slice().try_into().map_err(|_| {
            Error::Internal("sse descriptor: unwrapped key is not 32 bytes".to_owned())
        })
    }
}

fn object_headers(resp: S3Response, row: &ObjectVersionRow) -> S3Response {
    let mut resp = resp
        .with_header("etag", quoted(&row.etag))
        .with_header("content-type", row.content_type.clone())
        .with_header("last-modified", http_date(row.updated_at))
        .with_header("accept-ranges", "bytes")
        // Never let a browser MIME-sniff object bytes into an executable type (audit #13): an
        // attacker-uploaded object served inline same-origin (S3 GET, or a `/p/` share) could
        // otherwise be sniffed into HTML/JS and run as stored XSS. The declared content-type is
        // honored verbatim; nothing is sniffed.
        .with_header("x-content-type-options", "nosniff");
    if !row.version_id.is_null() {
        resp = resp.with_header("x-amz-version-id", row.version_id.as_str());
    }
    // Emit the stored system response headers when present (ARCH 13.4).
    if let Some(v) = &row.content_encoding {
        resp = resp.with_header("content-encoding", v.clone());
    }
    if let Some(v) = &row.cache_control {
        resp = resp.with_header("cache-control", v.clone());
    }
    if let Some(v) = &row.content_disposition {
        resp = resp.with_header("content-disposition", v.clone());
    }
    if let Some(v) = &row.content_language {
        resp = resp.with_header("content-language", v.clone());
    }
    if let Some(v) = &row.expires {
        resp = resp.with_header("expires", v.clone());
    }
    // Echo the SSE algorithm on GET/HEAD for a server-side-encrypted object (ARCH 27).
    if row.sse_descriptor.is_some() {
        resp = resp.with_header("x-amz-server-side-encryption", SSE_AES256);
    }
    if let CompressionDescriptor::Compressed { .. } = row.compression {
        // The physical form is hidden from clients; nothing leaks here.
    }
    for (k, v) in &row.user_metadata {
        resp = resp.with_header(&format!("x-amz-meta-{k}"), v.clone());
    }
    resp
}

/// Apply the GET `response-*` query-parameter header overrides (ARCH 21.2): each present param
/// REPLACES the corresponding response header (set rather than append, so no duplicate is emitted).
/// Applied after `object_headers`, so a client override always wins over the stored value.
fn apply_response_overrides(resp: S3Response, req: &S3Request) -> S3Response {
    const OVERRIDES: &[(&str, &str)] = &[
        ("response-content-type", "content-type"),
        ("response-content-disposition", "content-disposition"),
        ("response-content-encoding", "content-encoding"),
        ("response-content-language", "content-language"),
        ("response-cache-control", "cache-control"),
        ("response-expires", "expires"),
    ];
    let mut resp = resp;
    for (param, header) in OVERRIDES {
        if let Some(v) = req.query(param) {
            // Replace any existing value for this header, then set the override.
            resp.headers
                .retain(|(k, _)| !k.eq_ignore_ascii_case(header));
            resp = resp.with_header(header, v.to_owned());
        }
    }
    resp
}

/// For GET and HEAD: return a 304/412 short-circuit when conditional headers dictate it (21.2).
/// Evaluates If-Match / If-Unmodified-Since (412 conditions) and If-None-Match / If-Modified-Since
/// (304 conditions). Per RFC 7232 / S3, If-Match takes precedence over If-Unmodified-Since and
/// If-None-Match over If-Modified-Since; the time comparisons use the object's last-modified
/// (`updated_at`). A malformed date header is ignored.
fn conditional_short_circuit(req: &S3Request, row: &ObjectVersionRow) -> Option<S3Response> {
    let etag = row.etag.as_str();
    let last_modified = row.updated_at;

    // 412 group: If-Match, else If-Unmodified-Since.
    if let Some(im) = req.header("if-match") {
        if im.trim() != "*" && !im.split(',').any(|t| t.trim().trim_matches('"') == etag) {
            return Some(S3Response::status(StatusCode::PRECONDITION_FAILED));
        }
    } else if let Some(date) = req.header("if-unmodified-since").and_then(parse_http_date) {
        // Fail if the object was modified after the supplied date.
        if last_modified.as_secs() > date.as_secs() {
            return Some(S3Response::status(StatusCode::PRECONDITION_FAILED));
        }
    }

    // 304 group: If-None-Match, else If-Modified-Since.
    if let Some(inm) = req.header("if-none-match") {
        if inm.trim() == "*" || inm.split(',').any(|t| t.trim().trim_matches('"') == etag) {
            return Some(S3Response::status(StatusCode::NOT_MODIFIED));
        }
    } else if let Some(date) = req.header("if-modified-since").and_then(parse_http_date) {
        // Not modified since the supplied date => 304.
        if last_modified.as_secs() <= date.as_secs() {
            return Some(S3Response::status(StatusCode::NOT_MODIFIED));
        }
    }
    None
}

/// Evaluate the `x-amz-copy-source-if-*` preconditions against the source object version (21.6).
/// Every failed precondition fails the copy with [`Error::PreconditionFailed`] (412); a copy never
/// returns 304. Time comparisons use the source's last-modified (`updated_at`); a malformed date
/// header is ignored.
fn check_copy_source_conditions(req: &S3Request, src: &ObjectVersionRow) -> Result<()> {
    let etag = src.etag.as_str();
    let last_modified = src.updated_at;

    if let Some(im) = req.header("x-amz-copy-source-if-match") {
        if im.trim() != "*" && !im.split(',').any(|t| t.trim().trim_matches('"') == etag) {
            return Err(Error::PreconditionFailed);
        }
    }
    if let Some(inm) = req.header("x-amz-copy-source-if-none-match") {
        if inm.trim() == "*" || inm.split(',').any(|t| t.trim().trim_matches('"') == etag) {
            return Err(Error::PreconditionFailed);
        }
    }
    if let Some(date) = req
        .header("x-amz-copy-source-if-unmodified-since")
        .and_then(parse_http_date)
    {
        if last_modified.as_secs() > date.as_secs() {
            return Err(Error::PreconditionFailed);
        }
    }
    if let Some(date) = req
        .header("x-amz-copy-source-if-modified-since")
        .and_then(parse_http_date)
    {
        if last_modified.as_secs() <= date.as_secs() {
            return Err(Error::PreconditionFailed);
        }
    }
    Ok(())
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

/// Parse an `x-amz-copy-source-range` header into a [`ByteRange`] over the source object's
/// logical bytes. The header form is `bytes=first-last` with both bounds present and inclusive
/// (S3 does not accept the open-ended or suffix forms here). An absent header copies the whole
/// object (returns `None`).
fn parse_copy_source_range(header: Option<&str>, total: u64) -> Result<Option<ByteRange>> {
    let Some(h) = header else { return Ok(None) };
    let spec = h.trim().strip_prefix("bytes=").ok_or(Error::InvalidRange)?;
    let (start_s, end_s) = spec.split_once('-').ok_or(Error::InvalidRange)?;
    let start: u64 = start_s.trim().parse().map_err(|_| Error::InvalidRange)?;
    let end: u64 = end_s.trim().parse().map_err(|_| Error::InvalidRange)?;
    if start > end || start >= total {
        return Err(Error::InvalidRange);
    }
    let length = (end.min(total - 1) - start) + 1;
    Ok(Some(ByteRange {
        offset: start,
        length,
    }))
}

/// The SigV4 signed-streaming sentinel: the body is an `aws-chunked` stream whose per-chunk
/// signature chain must be verified.
const SIGNED_STREAMING_SENTINEL: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// Map a staging error to an S3 error, surfacing a signed-streaming chunk-signature mismatch as
/// an authentication failure rather than a generic internal error (the staged blob is already
/// reclaimed by the stager when its body stream errors).
fn map_stage_err(e: cairn_types::error::BlobError) -> Error {
    use cairn_types::error::{BlobError, BodyError};
    if let BlobError::Body(BodyError::Transport(msg)) = &e {
        if crate::chunked::is_signature_failure(msg) {
            return Error::SignatureDoesNotMatch;
        }
    }
    e.into()
}

/// Select the body decoder for a (possibly streaming) upload (the F-5 fix). For the signed
/// sentinel, build a signature-verifying decoder seeded from the principal's signed-streaming
/// context — a signed sentinel with no SigV4 streaming context is invalid and is rejected. Other
/// `STREAMING-*` sentinels (`STREAMING-UNSIGNED-PAYLOAD[-TRAILER]`) de-frame without verifying.
/// Non-streaming bodies pass through unchanged.
fn streaming_body(
    req: &S3Request,
    raw_body: cairn_types::BodyStream,
    max_payload: u64,
) -> Result<cairn_types::BodyStream> {
    let sentinel = req.header("x-amz-content-sha256");
    let streaming = sentinel
        .map(|v| v.starts_with("STREAMING"))
        .unwrap_or(false);
    if !streaming {
        return Ok(raw_body);
    }
    if sentinel == Some(SIGNED_STREAMING_SENTINEL) {
        // Signed streaming: the per-chunk chain is seeded by the request signature carried on the
        // principal. Without that context the signed sentinel cannot be verified, so refuse it.
        let ctx = req
            .principal
            .as_ref()
            .and_then(|p| p.chunk_signing.as_ref())
            .ok_or(Error::SignatureDoesNotMatch)?;
        let verifier = ChunkVerifier {
            key: ctx.signing_key,
            amzdate: ctx.amz_date.clone(),
            scope: ctx.scope.clone(),
            prev_signature: ctx.seed_signature.clone(),
        };
        Ok(decode_stream(
            raw_body,
            ChunkDecoder::signed(max_payload, verifier),
        ))
    } else {
        Ok(decode_stream(raw_body, ChunkDecoder::unsigned(max_payload)))
    }
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

/// Compare each client-supplied `x-amz-checksum-{algo}` header (base64) against the corresponding
/// computed checksum from the staged result (21.1). Returns [`Error::BadDigest`] on mismatch and
/// [`Error::InvalidDigest`] when a checksum header has no computed counterpart (its algorithm was
/// not staged — it should have been requested via `extra_checksums`). The `x-amz-checksum-algorithm`
/// selector header (no per-algorithm value) is ignored here.
fn verify_client_checksums(req: &S3Request, computed: &[ChecksumValue]) -> Result<()> {
    for (name, supplied) in &req.headers {
        let Some(algo_name) = name.strip_prefix("x-amz-checksum-") else {
            continue;
        };
        // `x-amz-checksum-algorithm` is a selector, not a digest value.
        if algo_name.eq_ignore_ascii_case("algorithm") {
            continue;
        }
        let Some(algo) = checksum_algo(algo_name) else {
            continue;
        };
        let got = computed
            .iter()
            .find(|c| c.algorithm == algo)
            .ok_or(Error::InvalidDigest)?;
        // S3 carries these base64-encoded; compare the trimmed base64 strings directly.
        if got.value != supplied.trim() {
            return Err(Error::BadDigest);
        }
    }
    Ok(())
}

/// Evaluate a CORS preflight against the bucket's rules (ARCH 18.2). Returns the 200 preflight
/// response (with `Access-Control-Allow-*` and `Vary: Origin`) for the first rule that allows the
/// `origin` + `method` + every requested header, or `None` if no rule matches.
fn cors_match(
    rules: &[cairn_xml::CorsRule],
    origin: &str,
    method: &str,
    requested_headers: &[String],
) -> Option<S3Response> {
    for rule in rules {
        let Some(allow_origin) = rule
            .allowed_origins
            .iter()
            .find_map(|pat| cors_origin_match(pat, origin))
        else {
            continue;
        };
        // Methods compare case-sensitively (S3 uses uppercase HTTP method tokens).
        if !rule.allowed_methods.iter().any(|m| m == method) {
            continue;
        }
        // Every requested header must be covered by an AllowedHeader pattern.
        if !requested_headers.iter().all(|h| {
            rule.allowed_headers
                .iter()
                .any(|pat| cors_header_match(pat, h))
        }) {
            continue;
        }

        let mut resp = S3Response::status(StatusCode::OK)
            .with_header("access-control-allow-origin", allow_origin)
            .with_header(
                "access-control-allow-methods",
                rule.allowed_methods.join(", "),
            )
            .with_header("vary", "Origin");
        if !requested_headers.is_empty() {
            resp = resp.with_header("access-control-allow-headers", requested_headers.join(", "));
        }
        if let Some(max_age) = rule.max_age_seconds {
            resp = resp.with_header("access-control-max-age", max_age.to_string());
        }
        if !rule.expose_headers.is_empty() {
            resp = resp.with_header(
                "access-control-expose-headers",
                rule.expose_headers.join(", "),
            );
        }
        return Some(resp);
    }
    None
}

/// Match an `AllowedOrigin` pattern against the request `Origin`. A bare `*` allows any origin and
/// echoes `*`; an exact match echoes the origin; a single embedded `*` is a wildcard segment. On a
/// match the value to echo in `Access-Control-Allow-Origin` is returned.
fn cors_origin_match(pattern: &str, origin: &str) -> Option<String> {
    if pattern == "*" {
        return Some("*".to_owned());
    }
    if pattern == origin {
        return Some(origin.to_owned());
    }
    if let Some((prefix, suffix)) = pattern.split_once('*') {
        if origin.len() >= prefix.len() + suffix.len()
            && origin.starts_with(prefix)
            && origin.ends_with(suffix)
        {
            return Some(origin.to_owned());
        }
    }
    None
}

/// Match an `AllowedHeader` pattern (case-insensitive) against a requested header name. `*` allows
/// any header; a trailing `*` is a prefix wildcard.
fn cors_header_match(pattern: &str, header: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase();
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return header.starts_with(prefix);
    }
    pattern == header
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

/// Parse the inline `x-amz-tagging` header, a URL-encoded `Key=Value&Key2=Value2` tag set as used
/// by `PutObject` and copy (ARCH 17.1, Medium #5). An empty/absent header yields no tags; a
/// segment with no `=` is treated as a key with an empty value.
fn parse_tagging_header(header: Option<&str>) -> Vec<(String, String)> {
    let Some(raw) = header else {
        return Vec::new();
    };
    raw.split('&')
        .filter(|seg| !seg.is_empty())
        .map(|seg| match seg.split_once('=') {
            Some((k, v)) => (form_pct_decode(k), form_pct_decode(v)),
            None => (form_pct_decode(seg), String::new()),
        })
        .collect()
}

/// Decode an `application/x-www-form-urlencoded` component: `+` becomes a space and `%XX`
/// becomes its byte. Used for the inline `x-amz-tagging` tag set.
fn form_pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < b.len() => {
                let hi = (b[i + 1] as char).to_digit(16);
                let lo = (b[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                } else {
                    out.push(b[i]);
                    i += 1;
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
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
/// part count (ARCH 10.2).
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
