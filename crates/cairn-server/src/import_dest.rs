//! [`LocalDestWriter`]: the [`DestWriter`] the server-side import loop uses. It writes each imported
//! object **through Cairn's real S3 put path** — `S3Service::handle` with a trusted root-admin
//! principal — so encryption (SSE), compression, quota, versioning, event notifications, and
//! outbound replication all apply exactly as for a normal upload. There is no raw blob/metadata
//! bypass; the admin principal simply short-circuits the authorization chokepoint.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use async_trait::async_trait;
use cairn_import::{DestWriter, ImportError, SourceObject};
use cairn_protocol::S3Request;
use cairn_types::auth::{AuthMethod, Principal, Role};
use cairn_types::error::BodyError;
use cairn_types::id::{BucketName, ObjectKey, UserId};
use futures_util::StreamExt;
use http::Method;

use crate::stack::AppStack;

/// A [`DestWriter`] that lands imported objects on this node via the S3 service.
pub struct LocalDestWriter {
    stack: Arc<AppStack>,
    principal: Principal,
}

/// Build the trusted principal imports write as: the deployment's root administrator (so imported
/// buckets are owned by root and the write is authorized via the owner/admin short-circuit). Falls
/// back to a synthetic Administrator identity if the root user cannot be read.
pub fn import_principal(root_user_id: UserId, access_key_id: String) -> Principal {
    Principal {
        user_id: root_user_id,
        display_name: "import".to_owned(),
        access_key_id,
        role: Role::Administrator,
        method: AuthMethod::Bearer,
        chunk_signing: None,
        user_policy: None,
        is_session: false,
    }
}

impl LocalDestWriter {
    /// Construct a writer that submits to `stack`'s S3 service as `principal`.
    #[must_use]
    pub fn new(stack: Arc<AppStack>, principal: Principal) -> Self {
        Self { stack, principal }
    }

    fn request(
        &self,
        method: Method,
        bucket: BucketName,
        key: Option<ObjectKey>,
        headers: Vec<(String, String)>,
    ) -> S3Request {
        S3Request {
            method,
            bucket: Some(bucket),
            key,
            query: Vec::new(),
            headers,
            principal: Some(self.principal.clone()),
            source: IpAddr::V4(Ipv4Addr::LOCALHOST),
            secure: true,
            request_id: format!("import-{}", cairn_types::id::VersionId::generate().as_str()),
        }
    }
}

#[async_trait]
impl DestWriter for LocalDestWriter {
    async fn ensure_bucket(&self, bucket: &str) -> Result<(), ImportError> {
        let bn = BucketName::parse(bucket).map_err(|e| {
            ImportError::Terminal(format!("invalid destination bucket {bucket}: {e}"))
        })?;
        let req = self.request(Method::PUT, bn, None, Vec::new());
        let resp = self.stack.s3.handle(req, empty_body()).await;
        match resp.status.as_u16() {
            // Created, or already present (owned by us) — both fine for an idempotent ensure.
            200 | 204 | 409 => Ok(()),
            s if s >= 500 => Err(ImportError::Unavailable(format!(
                "creating destination bucket {bucket}: HTTP {s}"
            ))),
            s => Err(ImportError::Terminal(format!(
                "creating destination bucket {bucket}: HTTP {s}"
            ))),
        }
    }

    async fn put_object(&self, bucket: &str, obj: SourceObject) -> Result<(), ImportError> {
        let bn = BucketName::parse(bucket).map_err(|e| {
            ImportError::Terminal(format!("invalid destination bucket {bucket}: {e}"))
        })?;
        let key = ObjectKey::parse(&obj.key)
            .map_err(|e| ImportError::Terminal(format!("invalid object key {:?}: {e}", obj.key)))?;

        // Replay the object's metadata as request headers; the put path preserves them. The
        // content-length drives the (clamped) preallocation and read.
        let mut headers = vec![("content-length".to_owned(), obj.size.to_string())];
        let mut push = |name: &str, v: &Option<String>| {
            if let Some(v) = v {
                headers.push((name.to_owned(), v.clone()));
            }
        };
        push("content-type", &obj.content_type);
        push("content-encoding", &obj.content_encoding);
        push("cache-control", &obj.cache_control);
        push("content-disposition", &obj.content_disposition);
        push("content-language", &obj.content_language);
        for (k, v) in &obj.user_metadata {
            headers.push((format!("x-amz-meta-{k}"), v.clone()));
        }
        // The inline `x-amz-tagging` header (form-urlencoded `k=v&k=v`) is what a normal PUT-with-
        // tags uses, so the real put path persists this as the object's initial tag set with no
        // further plumbing (`put_object`, cairn-protocol/service.rs). `uri_encode` percent-encodes
        // the structural `&`/`=` inside a key or value, mirroring the replication sink's same need.
        if !obj.tags.is_empty() {
            let tagging = obj
                .tags
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        cairn_auth::uri_encode(k, true),
                        cairn_auth::uri_encode(v, true)
                    )
                })
                .collect::<Vec<_>>()
                .join("&");
            headers.push(("x-amz-tagging".to_owned(), tagging));
        }

        let req = self.request(Method::PUT, bn, Some(key), headers);
        // Adapt the source body stream (BlobError) to the request body stream (BodyError).
        let body: cairn_types::BodyStream = Box::pin(
            obj.body
                .map(|r| r.map_err(|e| BodyError::Transport(e.to_string()))),
        );
        let resp = self.stack.s3.handle(req, body).await;
        if resp.status.is_success() {
            Ok(())
        } else if resp.status.as_u16() >= 500 {
            Err(ImportError::Unavailable(format!(
                "writing object {:?}: HTTP {}",
                obj.key, resp.status
            )))
        } else {
            Err(ImportError::Terminal(format!(
                "writing object {:?}: HTTP {}",
                obj.key, resp.status
            )))
        }
    }
}

/// An empty request body for a no-body op (bucket create).
fn empty_body() -> cairn_types::BodyStream {
    Box::pin(futures_util::stream::empty())
}
