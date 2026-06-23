//! Adapts hyper's request/response to the library-neutral S3 request/response, performs
//! authentication, and routes path-style addressing into the S3 service.
//!
//! Object reads (`S3Body::Stream`) are forwarded to hyper as a streaming body so a large GET
//! flows blob -> socket with bounded memory (ARCH 7.4/7.6/7.8): no whole-object buffer is ever
//! materialised. Empty and in-memory (XML/error) bodies stay fully buffered, which is correct —
//! they are already small and bounded. Request bodies for object PUT are streamed separately via
//! [`incoming_to_stream`].

use crate::stack::AppStack;
use bytes::Bytes;
use cairn_crypto::{SystemClock, SystemCrypto};
use cairn_protocol::{S3Body, S3Request, S3Response, error_response};
use cairn_types::auth::{AuthMethod, AuthOutcome, Principal, RequestView, Role};
use cairn_types::crypto::Nonce;
use cairn_types::error::{BodyError, Error};
use cairn_types::id::{BucketName, ObjectKey, UserId, VersionId};
use cairn_types::meta::{ActivityEntry, Mutation, ShareDisposition, ShareRow};
use cairn_types::time::Timestamp;
use cairn_types::traits::{Clock, Crypto};
use futures_util::StreamExt;
use http_body_util::{BodyExt, BodyStream, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::{Method, Request, Response};
use std::net::IpAddr;
use zeroize::Zeroizing;

/// The unified HTTP response body: either a fully-buffered in-memory body (empty, XML, errors,
/// UI assets, management JSON) or a blob stream forwarded frame-by-frame from the blob store.
/// Boxing both into one type lets every response path return a single concrete `Body`. It is an
/// `UnsyncBoxBody` rather than a `BoxBody` because the underlying blob stream is `Send` but not
/// `Sync`; hyper only requires the body to be `Send`, so dropping the `Sync` bound is correct and
/// avoids buffering the stream to satisfy it.
pub type ResponseBody = http_body_util::combinators::UnsyncBoxBody<Bytes, BodyError>;

/// Wrap a fully-buffered byte payload as a [`ResponseBody`].
pub(crate) fn full_body(bytes: Bytes) -> ResponseBody {
    Full::new(bytes)
        .map_err(|e: std::convert::Infallible| match e {})
        .boxed_unsync()
}

/// Handle an S3 (or anonymous) HTTP request end to end.
pub async fn handle(
    stack: std::sync::Arc<AppStack>,
    req: Request<Incoming>,
    peer: IpAddr,
    secure: bool,
    serve_ui: bool,
    request_id: String,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Response<ResponseBody> {
    let method = req.method().clone();
    let raw_path = req.uri().path().to_owned();
    let query_str = req.uri().query().unwrap_or("").to_owned();
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_ascii_lowercase(),
                v.to_str().unwrap_or("").to_owned(),
            )
        })
        .collect();
    let host = headers
        .iter()
        .find(|(k, _)| k == "host")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();

    // Authenticate against a borrowed, library-neutral view.
    let principal = {
        let view = RequestView {
            method: method.as_str(),
            path: &raw_path,
            query: &query_str,
            headers: &headers,
            host: &host,
            source: peer,
            secure_transport: secure,
        };
        match stack.auth.authenticate(&view).await {
            AuthOutcome::Authenticated(p) => Some(p),
            AuthOutcome::NotApplicable => None,
            AuthOutcome::Denied(e) => {
                let resource = raw_path.clone();
                return render(error_response(&Error::from(e), &resource, &request_id));
            }
        }
    };

    // The management JSON API and the embedded web UI share the listener with the S3 surface.
    if let Some(subpath) = raw_path.strip_prefix("/api/v1") {
        let query = parse_query(&query_str);
        // Bound the management-API request body (audit #11). The whole body is buffered for JSON
        // parsing, so an unbounded request would let a client pin arbitrary server memory. Cap it
        // and refuse oversize bodies with 413 instead of buffering them.
        const MAX_API_BODY: usize = 8 * 1024 * 1024;
        let body_bytes = match http_body_util::Limited::new(req.into_body(), MAX_API_BODY)
            .collect()
            .await
        {
            Ok(c) => c.to_bytes(),
            Err(e)
                if e.downcast_ref::<http_body_util::LengthLimitError>()
                    .is_some() =>
            {
                let mut builder = Response::builder()
                    .status(http::StatusCode::PAYLOAD_TOO_LARGE)
                    .header("content-type", "application/json");
                if let Ok(v) = http::HeaderValue::from_str(&request_id) {
                    builder = builder.header("x-amz-request-id", v);
                }
                return builder
                    .body(full_body(Bytes::from_static(
                        br#"{"error":"RequestEntityTooLarge","message":"request body exceeds the maximum allowed size"}"#,
                    )))
                    .unwrap_or_else(|_| Response::new(full_body(Bytes::new())));
            }
            // A transient read error (e.g. the client hung up): preserve the prior behavior of
            // proceeding with an empty body rather than synthesizing a misleading 413.
            Err(_) => Bytes::new(),
        };
        // Master-key rotation status (audit #29, Phase E): an admin-only operator surface served
        // from the server stack because it reads the concrete per-shard handles + the key ring.
        if method == Method::GET && subpath == "/system/crypto-status" {
            return crypto_status_response(&stack, principal.as_ref(), &request_id).await;
        }
        // Minting a persistent public-read ("share") URL is handled here, not in cairn-control,
        // because it streams object bytes through the server stack on redemption:
        // POST /api/v1/buckets/{bucket}/objects/share.
        if method == Method::POST {
            if let Some(bucket) = subpath
                .strip_prefix("/buckets/")
                .and_then(|r| r.strip_suffix("/objects/share"))
            {
                return create_share(&stack, bucket, &body_bytes, principal.as_ref()).await;
            }
            // Mint an interoperable S3 presigned URL (GET download / PUT upload). Lives here
            // because it opens the requester's sealed SigV4 secret from the server stack.
            if let Some(bucket) = subpath
                .strip_prefix("/buckets/")
                .and_then(|r| r.strip_suffix("/objects/presign"))
            {
                return presign(
                    &stack,
                    bucket,
                    &body_bytes,
                    principal.as_ref(),
                    &host,
                    secure,
                )
                .await;
            }
        }
        // Live-update channel (SSE): a single-use ticket mint (the browser's EventSource cannot
        // send an Authorization header, so it POSTs with its Bearer token for a short-lived ticket)
        // and the multiplexed event stream. Both live here because the stream is a long-lived
        // streaming body the buffered control plane cannot produce.
        if method == Method::POST && subpath == "/events/ticket" {
            return crate::sse::mint_ticket(&stack, principal.as_ref());
        }
        if method == Method::GET && subpath == "/events/stream" {
            let ticket = query
                .iter()
                .find(|(k, _)| k == "ticket")
                .map(|(_, v)| v.as_str());
            let topics = query
                .iter()
                .find(|(k, _)| k == "topics")
                .map(|(_, v)| v.as_str())
                .unwrap_or("");
            return crate::sse::events_stream(stack.clone(), ticket, topics, shutdown);
        }
        let resp = stack
            .control
            .handle(&method, subpath, &query, principal.as_ref(), body_bytes)
            .await;
        // Emit the per-request id as `x-amz-request-id` on every control response, success or
        // error, so an operator can correlate a call with logs and the error envelope (ARCH 25.1).
        let mut builder = Response::builder()
            .status(resp.status)
            .header("content-type", "application/json");
        if let Ok(v) = http::HeaderValue::from_str(&resp.request_id) {
            builder = builder.header("x-amz-request-id", v);
        }
        return builder
            .body(full_body(Bytes::from(resp.body)))
            .unwrap_or_else(|_| Response::new(full_body(Bytes::new())));
    }
    // On the web-UI listener only, serve the management console at the ROOT path and its embedded
    // assets BEFORE S3 routing (so `/assets/...` can never be shadowed by a bucket named `assets`).
    // Any path that is not the root or a known embedded asset falls through to the S3/data routing,
    // which is what the console's own object operations and the API listener rely on. The former
    // `/web` and `/ui` mounts redirect to the root for back-compat.
    if serve_ui && method == Method::GET {
        if raw_path == "/" {
            let (content_type, bytes) = cairn_ui::spa_shell();
            return ui_asset_response(content_type, bytes.into_owned());
        }
        if raw_path == "/web"
            || raw_path.starts_with("/web/")
            || raw_path == "/ui"
            || raw_path.starts_with("/ui/")
        {
            return redirect("/");
        }
        if let Some(rel) = raw_path.strip_prefix('/').filter(|r| !r.is_empty()) {
            if let Some((content_type, bytes)) = cairn_ui::asset(rel) {
                return ui_asset_response(content_type, bytes.into_owned());
            }
        }
    }

    // Persistent public-read ("share") URLs: GET|HEAD /p/{token} — unauthenticated, resolved by an
    // opaque registry token (ARCH 15.8). The token is a single path segment.
    if (method == Method::GET || method == Method::HEAD) && raw_path.starts_with("/p/") {
        let token = &raw_path[3..]; // after "/p/"
        if token.is_empty() || token.contains('/') {
            return json_status(404, r#"{"error":"not found"}"#);
        }
        return serve_share(&stack, token, method, &headers, peer, secure, request_id).await;
    }

    // Virtual-host-style addressing (ARCH 13.1): when `CAIRN_S3_DOMAIN` is configured and the
    // request Host is `<bucket>.<s3_domain>`, the bucket is taken from the Host and the entire path
    // is the key. Otherwise fall back to path-style routing (`/<bucket>/<key>`).
    let (bucket, key) = route_request(stack.s3_domain.as_deref(), &host, &raw_path);
    let query = parse_query(&query_str);
    let body = incoming_to_stream(req.into_body());

    let s3req = S3Request {
        method,
        bucket,
        key,
        query,
        headers,
        principal,
        source: peer,
        secure,
        request_id,
    };
    render(stack.s3.handle(s3req, body).await)
}

/// Build a 200 response for an embedded UI asset with its content type.
fn ui_asset_response(content_type: String, bytes: Vec<u8>) -> Response<ResponseBody> {
    Response::builder()
        .status(200)
        .header("content-type", content_type)
        .body(full_body(Bytes::from(bytes)))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// A 301 redirect to `location`.
fn redirect(location: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(301)
        .header("location", location)
        .body(full_body(Bytes::new()))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// Build a JSON response with the given status.
fn json_status(status: u16, body: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(full_body(Bytes::from(body.to_owned())))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// Strip header-injection and quoting characters from a download filename before it goes into
/// `Content-Disposition`.
fn sanitize_filename(s: &str) -> String {
    s.chars()
        .filter(|c| !matches!(c, '"' | '\\' | '\r' | '\n'))
        .collect()
}

/// A 256-bit opaque share token (two v4 UUIDs of hex), URL-safe and unguessable. Matches the
/// bootstrap secret construction; the row's existence is the capability.
fn generate_share_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Mint a persistent public-read ("share") token for an object (ARCH 15.8). Admin-only. Body:
/// `{"key", "expires_in_secs"?: null=forever, "disposition"?: "inline"|"attachment", "filename"?,
/// "version_id"?}`. Returns `{"token","url":"/p/{token}","expires_at_ms": ms|null}`.
async fn create_share(
    stack: &AppStack,
    bucket: &str,
    body: &Bytes,
    principal: Option<&Principal>,
) -> Response<ResponseBody> {
    if principal.map(|p| p.role) != Some(Role::Administrator) {
        return json_status(403, r#"{"error":"forbidden"}"#);
    }
    let bname = match BucketName::parse(bucket) {
        Ok(b) => b,
        Err(_) => return json_status(404, r#"{"error":"no such bucket"}"#),
    };
    #[derive(serde::Deserialize)]
    struct ShareReq {
        key: String,
        #[serde(default)]
        expires_in_secs: Option<u64>,
        #[serde(default)]
        disposition: Option<String>,
        #[serde(default)]
        filename: Option<String>,
        #[serde(default)]
        version_id: Option<String>,
    }
    let req: ShareReq = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(_) => return json_status(400, r#"{"error":"invalid request body"}"#),
    };
    let key = match ObjectKey::parse(&req.key) {
        Ok(k) if !req.key.is_empty() => k,
        _ => return json_status(400, r#"{"error":"a valid key is required"}"#),
    };
    let now = SystemClock::new().now();
    // null/absent expiry = forever (admin-minted, revocable, audited).
    let expires_at = req
        .expires_in_secs
        .map(|s| Timestamp(now.as_millis() + (s as i64) * 1000));
    let disposition = match req.disposition.as_deref() {
        Some("attachment") => ShareDisposition::Attachment,
        _ => ShareDisposition::Inline,
    };
    let token = generate_share_token();
    let row = ShareRow {
        token: token.clone(),
        bucket: bname.clone(),
        key: key.clone(),
        version_id: req.version_id.map(VersionId::from_string),
        expires_at,
        disposition,
        filename: req.filename,
        created_by: principal
            .map(|p| p.user_id.clone())
            .unwrap_or_else(UserId::generate),
        created_at: now,
        revoked_at: None,
    };
    if stack
        .meta
        .submit(Mutation::CreateShare(Box::new(row)))
        .await
        .is_err()
    {
        return json_status(500, r#"{"error":"could not create share"}"#);
    }
    // Audit the mint (best-effort; never blocks the response).
    let _ = stack
        .meta
        .submit(Mutation::RecordActivity(Box::new(ActivityEntry {
            id: uuid::Uuid::new_v4().simple().to_string(),
            action: "CreateShare".to_owned(),
            bucket: Some(bname.as_str().to_owned()),
            key: Some(key.as_str().to_owned()),
            size: None,
            etag: None,
            actor: principal.map(|p| p.access_key_id.clone()),
            at: now,
        })))
        .await;
    let expires_json = expires_at.map_or_else(|| "null".to_owned(), |t| t.0.to_string());
    json_status(
        200,
        &format!(r#"{{"token":"{token}","url":"/p/{token}","expires_at_ms":{expires_json}}}"#),
    )
}

/// Mint an interoperable S3 presigned URL (ARCH 14.2). Admin-only. Body: `{"key","method"?:
/// "GET"|"PUT","expires_in_secs":1..=604800,"version_id"?,"response_content_disposition"?,
/// "response_content_type"?,"content_type"?}`. Signs with the requester's own SigV4 secret.
async fn presign(
    stack: &AppStack,
    bucket: &str,
    body: &Bytes,
    principal: Option<&Principal>,
    host: &str,
    secure: bool,
) -> Response<ResponseBody> {
    let p = match principal {
        Some(p) if p.role == Role::Administrator => p,
        _ => return json_status(403, r#"{"error":"forbidden"}"#),
    };
    if BucketName::parse(bucket).is_err() {
        return json_status(404, r#"{"error":"no such bucket"}"#);
    }
    #[derive(serde::Deserialize)]
    struct PresignReq {
        key: String,
        #[serde(default)]
        method: Option<String>,
        expires_in_secs: i64,
        #[serde(default)]
        version_id: Option<String>,
        #[serde(default)]
        response_content_disposition: Option<String>,
        #[serde(default)]
        response_content_type: Option<String>,
        #[serde(default)]
        content_type: Option<String>,
    }
    let req: PresignReq = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(_) => return json_status(400, r#"{"error":"invalid request body"}"#),
    };
    if req.key.is_empty() {
        return json_status(400, r#"{"error":"key is required"}"#);
    }
    let http_method = req.method.as_deref().unwrap_or("GET").to_ascii_uppercase();
    if http_method != "GET" && http_method != "PUT" {
        return json_status(400, r#"{"error":"method must be GET or PUT"}"#);
    }
    // Reject over-cap at mint (vs the verifier's silent clamp) so the operator-visible expiry is
    // the real one; "forever" is only available via persistent shares, never presigned.
    if !(1..=604_800).contains(&req.expires_in_secs) {
        return json_status(
            400,
            r#"{"error":"expires_in_secs must be between 1 and 604800 (7 days)"}"#,
        );
    }
    // Open the requesting admin's own SigV4 secret transiently.
    let users = match stack.meta.list_users().await {
        Ok(u) => u,
        Err(_) => return json_status(500, r#"{"error":"internal error"}"#),
    };
    let Some(user) = users.into_iter().find(|u| u.id == p.user_id) else {
        return json_status(403, r#"{"error":"forbidden"}"#);
    };
    let Some(sigv4_key) = user.sigv4_access_key_id else {
        return json_status(
            400,
            r#"{"error":"this user has no S3 (SigV4) credential to sign with"}"#,
        );
    };
    let creds = match stack.meta.user_by_sigv4_key(&sigv4_key).await {
        Ok(Some(c)) => c,
        _ => return json_status(500, r#"{"error":"internal error"}"#),
    };
    let secret = match stack
        .crypto
        .open(&creds.secret_ciphertext, &Nonce(creds.secret_nonce.clone()))
    {
        // `open` returns the plaintext already in a zeroizing buffer; wrap the derived String too so
        // it is scrubbed promptly rather than lingering in freed heap (F-15).
        Ok(b) => Zeroizing::new(String::from_utf8_lossy(&b).into_owned()),
        Err(_) => return json_status(500, r#"{"error":"internal error"}"#),
    };

    let (scheme, signed_host) = base_scheme_host(stack.public_base_url.as_deref(), host, secure);
    let mut extra_query: Vec<(String, String)> = Vec::new();
    if let Some(v) = &req.version_id {
        extra_query.push(("versionId".to_owned(), v.clone()));
    }
    if http_method == "GET" {
        if let Some(d) = &req.response_content_disposition {
            extra_query.push(("response-content-disposition".to_owned(), d.clone()));
        }
        if let Some(t) = &req.response_content_type {
            extra_query.push(("response-content-type".to_owned(), t.clone()));
        }
    }
    let mut extra_signed_headers: Vec<(String, String)> = Vec::new();
    if http_method == "PUT" {
        if let Some(ct) = &req.content_type {
            extra_signed_headers.push(("content-type".to_owned(), ct.clone()));
        }
    }
    let now = SystemClock::new().now();
    let amz_date = format_amz_date(now);
    let path_query = cairn_auth::mint_presigned(&cairn_auth::PresignRequest {
        method: &http_method,
        host: &signed_host,
        bucket,
        key: &req.key,
        access_key_id: &sigv4_key,
        secret: &secret,
        region: &stack.region,
        expires_secs: req.expires_in_secs,
        amz_date: &amz_date,
        extra_query,
        extra_signed_headers,
    });
    let expires_at = now.as_millis() + req.expires_in_secs * 1000;
    let url = format!("{scheme}://{signed_host}{path_query}");
    json_status(
        200,
        &format!(r#"{{"url":"{url}","expires_at_ms":{expires_at},"absolute":true}}"#),
    )
}

/// Resolve the `(scheme, host)` for an absolute share/presigned URL: the configured public base URL
/// when set, else this request's transport + Host.
fn base_scheme_host(
    public_base_url: Option<&str>,
    req_host: &str,
    secure: bool,
) -> (String, String) {
    if let Some(base) = public_base_url {
        if let Some(rest) = base.strip_prefix("https://") {
            return (
                "https".to_owned(),
                rest.split('/').next().unwrap_or(rest).to_owned(),
            );
        }
        if let Some(rest) = base.strip_prefix("http://") {
            return (
                "http".to_owned(),
                rest.split('/').next().unwrap_or(rest).to_owned(),
            );
        }
    }
    (
        if secure { "https" } else { "http" }.to_owned(),
        req_host.to_owned(),
    )
}

/// Format an instant as the SigV4 basic date `YYYYMMDDTHHMMSSZ`.
fn format_amz_date(ts: Timestamp) -> String {
    let ms = ts.as_millis();
    let days = ms.div_euclid(86_400_000);
    let ms_of_day = ms.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    let hh = ms_of_day / 3_600_000;
    let mm = (ms_of_day % 3_600_000) / 60_000;
    let ss = (ms_of_day % 60_000) / 1_000;
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

/// Civil (year, month, day) from a count of days since the Unix epoch (Howard Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Serve a persistent share by its token: look it up, reject revoked/expired (`410`) or unknown
/// (`404`), then stream the object through the normal S3 read path under a least-privilege synthetic
/// principal scoped to read-only of the one key. Version-pinned shares serve the pinned version;
/// the server sets `Content-Disposition` from the share and `Referrer-Policy: no-referrer`.
async fn serve_share(
    stack: &AppStack,
    token: &str,
    method: Method,
    in_headers: &[(String, String)],
    peer: IpAddr,
    secure: bool,
    request_id: String,
) -> Response<ResponseBody> {
    let row = match stack.meta.get_share(token).await {
        Ok(Some(r)) => r,
        Ok(None) => return json_status(404, r#"{"error":"not found"}"#),
        Err(_) => return json_status(500, r#"{"error":"internal error"}"#),
    };
    if row.revoked_at.is_some() {
        return json_status(410, r#"{"error":"this share has been revoked"}"#);
    }
    if let Some(exp) = row.expires_at {
        if SystemClock::new().now().as_millis() > exp.0 {
            return json_status(410, r#"{"error":"this share has expired"}"#);
        }
    }

    // A least-privilege synthetic principal: a member whose ONLY grant is reading this one key. As
    // an identity (not public) grant it bypasses Block Public Access — the intended per-object
    // share semantics — yet it can never reach another object or a write, even if a downstream bug
    // let it try. A fresh random user id matches no named policy/ACL statement.
    let resource = format!("arn:aws:s3:::{}/{}", row.bucket.as_str(), row.key.as_str());
    let policy = cairn_types::authz::Policy {
        version: "2012-10-17".to_owned(),
        id: None,
        statements: vec![cairn_types::authz::Statement {
            sid: None,
            effect: cairn_types::Effect::Allow,
            principals: cairn_types::authz::PrincipalSpec::Any,
            actions: cairn_types::authz::ActionMatch::In(vec![
                cairn_types::authz::ActionPattern::Exact("s3:GetObject".to_owned()),
                cairn_types::authz::ActionPattern::Exact("s3:GetObjectVersion".to_owned()),
            ]),
            resources: cairn_types::authz::ResourceMatch::In(vec![resource]),
            conditions: Vec::new(),
        }],
    };
    let principal = Principal {
        user_id: UserId::generate(),
        display_name: "object-share".to_owned(),
        access_key_id: "object-share".to_owned(),
        role: Role::Member,
        method: AuthMethod::Bearer,
        chunk_signing: None,
        user_policy: Some(Box::new(policy)),
        is_session: false,
    };

    // Pin the version when the share is version-pinned; forward only safe read-shaping headers.
    let mut query: Vec<(String, String)> = Vec::new();
    if let Some(v) = &row.version_id {
        query.push(("versionId".to_owned(), v.as_str().to_owned()));
    }
    let headers: Vec<(String, String)> = in_headers
        .iter()
        .filter(|(k, _)| {
            matches!(
                k.as_str(),
                "range"
                    | "if-none-match"
                    | "if-modified-since"
                    | "if-match"
                    | "if-unmodified-since"
            )
        })
        .cloned()
        .collect();

    let s3req = S3Request {
        method,
        bucket: Some(row.bucket.clone()),
        key: Some(row.key.clone()),
        query,
        headers,
        principal: Some(principal),
        source: peer,
        secure,
        request_id,
    };
    let empty: cairn_types::BodyStream = Box::pin(futures_util::stream::empty());
    let mut resp = render(stack.s3.handle(s3req, empty).await);

    // Server-controlled delivery + privacy: override any object-set disposition, and never leak the
    // token through a referer.
    let disp = match (row.disposition, row.filename.as_deref()) {
        (ShareDisposition::Attachment, Some(name)) => {
            format!("attachment; filename=\"{}\"", sanitize_filename(name))
        }
        (ShareDisposition::Attachment, None) => "attachment".to_owned(),
        (ShareDisposition::Inline, _) => "inline".to_owned(),
    };
    let h = resp.headers_mut();
    if let Ok(v) = http::HeaderValue::from_str(&disp) {
        h.insert(http::header::CONTENT_DISPOSITION, v);
    }
    h.insert(
        "referrer-policy",
        http::HeaderValue::from_static("no-referrer"),
    );
    resp
}

/// Build the admin-only `GET /api/v1/system/crypto-status` JSON (audit #29, Phase E): the active
/// master key, its seal count vs the thresholds, the ring key states (aggregated across shards),
/// and per-stream re-wrap completion. Contains NO key material — only ids, key-hash prefixes, and
/// counters — so an operator can tell when a retired key's data is fully re-wrapped.
async fn crypto_status_response(
    stack: &AppStack,
    principal: Option<&Principal>,
    request_id: &str,
) -> Response<ResponseBody> {
    if principal.map(|p| p.role) != Some(Role::Administrator) {
        return json_status(403, r#"{"error":"forbidden"}"#);
    }
    // Aggregate ring states across shards: union of ids, active if any shard says so, max count.
    let mut keys: std::collections::BTreeMap<u16, (String, bool, u64)> =
        std::collections::BTreeMap::new();
    for s in &stack.store {
        if let Ok(rows) = s.key_ring_states().await {
            for r in rows {
                let e = keys.entry(r.id).or_insert((String::new(), false, 0));
                e.0 = r.key_hash;
                e.1 = e.1 || r.is_active;
                e.2 = e.2.max(r.sealed_count);
            }
        }
    }
    // Per-stream re-wrap completion: a stream is complete ONLY when every shard recorded a full,
    // failure-free re-wrap pass under the CURRENT active key (audit #29). A cleared cursor alone is
    // also the never-started state, so it must not read as complete — we compare the persisted
    // `done_active_id` against the live active id instead. With no sqlite shards (async backends,
    // which do not auto-re-wrap) nothing is verifiable, so nothing is ever reported complete.
    let active = stack.crypto.active_key_id();
    let has_shards = !stack.store.is_empty();
    let streams = [
        "object_versions.sse_descriptor",
        "users.sigv4_secret",
        "bucket_config.replication_targets",
    ];
    let mut complete_by_stream: std::collections::BTreeMap<&str, bool> =
        streams.iter().map(|s| (*s, has_shards)).collect();
    for s in &stack.store {
        let done: std::collections::HashMap<String, u16> = s
            .rewrap_done_active_ids()
            .await
            .unwrap_or_default()
            .into_iter()
            .collect();
        for stream in streams {
            if done.get(stream).copied() != Some(active) {
                complete_by_stream.insert(stream, false);
            }
        }
    }
    let all_complete = has_shards && complete_by_stream.values().all(|&c| c);
    let rewrap: Vec<_> = streams
        .iter()
        .map(|stream| serde_json::json!({ "stream": stream, "complete": complete_by_stream[stream] }))
        .collect();
    let keys_json: Vec<_> = keys
        .into_iter()
        .map(|(id, (hash, is_active, count))| {
            serde_json::json!({
                "id": id,
                "key_hash": hash,
                "active": is_active,
                "sealed_count": count,
                "retire_eligible": !is_active && all_complete,
            })
        })
        .collect();
    let body = serde_json::json!({
        "active_key_id": stack.crypto.active_key_id(),
        "seal_count": stack.crypto.seal_count(),
        "nonce_ceiling": SystemCrypto::nonce_ceiling(),
        "alert_threshold": SystemCrypto::seal_alert_threshold(),
        "hard_stop_threshold": SystemCrypto::seal_stop_threshold(),
        "keys": keys_json,
        "rewrap": rewrap,
    })
    .to_string();
    let mut builder = Response::builder()
        .status(200)
        .header("content-type", "application/json");
    if let Ok(v) = http::HeaderValue::from_str(request_id) {
        builder = builder.header("x-amz-request-id", v);
    }
    builder
        .body(full_body(Bytes::from(body)))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// Route a request to a `(bucket, key)`, preferring virtual-host-style addressing when configured.
///
/// When `s3_domain` is `Some` and the request `Host` (port stripped) is `<bucket>.<s3_domain>`, the
/// bucket is the leading Host label and the **entire** request path (sans the leading `/`) is the
/// key (ARCH 13.1). Any other Host — including a bare `<s3_domain>` with no bucket label, or a Host
/// that is not under the domain — falls through to path-style [`route_path`]. With `s3_domain`
/// `None`, routing is always path-style.
pub(crate) fn route_request(
    s3_domain: Option<&str>,
    host: &str,
    raw_path: &str,
) -> (Option<BucketName>, Option<ObjectKey>) {
    if let Some(domain) = s3_domain {
        if let Some(bucket) = vhost_bucket(host, domain) {
            if let Ok(b) = BucketName::parse(&bucket) {
                let key = raw_path.strip_prefix('/').unwrap_or(raw_path).to_owned();
                let key = (!key.is_empty())
                    .then(|| ObjectKey::parse(&pct_decode(&key)).ok())
                    .flatten();
                return (Some(b), key);
            }
        }
    }
    route_path(raw_path)
}

/// Extract the bucket label from a virtual-host `Host` of the form `<bucket>.<s3_domain>`, with any
/// `:port` stripped and matching done case-insensitively. Returns `None` when the Host is not a
/// strict `<label>.<domain>` (e.g. a bare domain, a mismatched domain, or an empty label).
fn vhost_bucket(host: &str, domain: &str) -> Option<String> {
    let host = host.split(':').next().unwrap_or(host);
    let host_l = host.to_ascii_lowercase();
    let domain_l = domain.to_ascii_lowercase();
    let suffix = format!(".{domain_l}");
    let bucket = host_l.strip_suffix(&suffix)?;
    // A single leading label only — `a.b.<domain>` is not a Cairn virtual-host bucket.
    if bucket.is_empty() || bucket.contains('.') {
        return None;
    }
    Some(bucket.to_owned())
}

/// Split a path-style request path into a bucket and key.
pub(crate) fn route_path(raw_path: &str) -> (Option<BucketName>, Option<ObjectKey>) {
    let p = raw_path.strip_prefix('/').unwrap_or(raw_path);
    if p.is_empty() {
        return (None, None);
    }
    let (bucket_seg, key_rest) = match p.split_once('/') {
        Some((b, k)) => (b, Some(k)),
        None => (p, None),
    };
    let bucket = BucketName::parse(&pct_decode(bucket_seg)).ok();
    let key = key_rest
        .filter(|k| !k.is_empty())
        .and_then(|k| ObjectKey::parse(&pct_decode(k)).ok());
    (bucket, key)
}

fn parse_query(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (pct_decode(k), pct_decode(v))
        })
        .collect()
}

fn incoming_to_stream(body: Incoming) -> cairn_types::BodyStream {
    Box::pin(BodyStream::new(body).filter_map(|res| async move {
        match res {
            Ok(frame) => frame.into_data().ok().map(Ok),
            Err(e) => Some(Err(BodyError::Transport(e.to_string()))),
        }
    }))
}

/// Render an [`S3Response`] onto the wire. `Empty`/`Bytes` bodies are already bounded and stay
/// buffered; a `Stream` body (object read) is forwarded to hyper as a `StreamBody` so bytes flow
/// from the blob store to the socket in bounded chunks with backpressure, never materialising the
/// whole object in memory (ARCH 7.4/7.6/7.8). The stream's `BlobError` is mapped onto the
/// body's `BodyError`; a mid-stream blob failure terminates the body, which surfaces to the
/// client as a truncated transfer (the status line is already sent by then).
fn render(resp: S3Response) -> Response<ResponseBody> {
    let body: ResponseBody = match resp.body {
        S3Body::Empty => full_body(Bytes::new()),
        S3Body::Bytes(b) => full_body(b),
        // ZeroCopy bodies fall back to their portable stream here: the fast `sendfile` path is taken
        // (when enabled) before hyper renders the response, so reaching `render` means this response
        // is being served the normal streamed way (TLS, default build, or a non-eligible connection).
        S3Body::Stream { stream, .. } | S3Body::ZeroCopy { stream, .. } => {
            let framed = stream.map(|chunk| {
                chunk
                    .map(Frame::data)
                    .map_err(|e| BodyError::Transport(e.to_string()))
            });
            BodyExt::boxed_unsync(StreamBody::new(framed))
        }
    };
    let mut builder = Response::builder().status(resp.status);
    for (k, v) in resp.headers {
        builder = builder.header(k, v);
    }
    builder
        .body(body)
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// Minimal percent-decoding for path/query segments.
fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex_val(b[i + 1]), hex_val(b[i + 2])) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::error::BlobError;
    use futures_util::stream;
    use http::StatusCode;

    /// A `Stream` response body is forwarded frame-by-frame, not drained into one buffer: the
    /// rendered body yields one HTTP data frame per source chunk and the bytes round-trip
    /// unchanged (ARCH 7.4/7.6/7.8, High #4).
    #[tokio::test]
    async fn stream_response_is_forwarded_chunk_by_chunk() {
        let chunks: Vec<Result<Bytes, BlobError>> = vec![
            Ok(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"streamed ")),
            Ok(Bytes::from_static(b"world")),
        ];
        let stream: cairn_types::BlobStream = Box::pin(stream::iter(chunks));
        let resp = S3Response {
            status: StatusCode::OK,
            headers: vec![("content-length".to_owned(), "20".to_owned())],
            body: S3Body::Stream { length: 20, stream },
        };

        let mut body = render(resp).into_body();
        let mut frames = 0usize;
        let mut collected = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.expect("frame ok");
            if let Ok(data) = frame.into_data() {
                frames += 1;
                collected.extend_from_slice(&data);
            }
        }
        assert_eq!(collected, b"hello streamed world");
        // Three source chunks must surface as three distinct data frames: proof the body streams
        // rather than collecting everything into a single buffer first.
        assert_eq!(frames, 3, "each source chunk must be its own frame");
    }

    /// Virtual-host addressing: with `CAIRN_S3_DOMAIN` set and a `<bucket>.<domain>` Host, the
    /// bucket comes from the Host and the entire path is the key (ARCH 13.1).
    #[test]
    fn route_request_virtual_host_takes_bucket_from_host() {
        let (b, k) = route_request(
            Some("s3.example.com"),
            "photos.s3.example.com",
            "/a/b/c.jpg",
        );
        assert_eq!(b.unwrap().as_str(), "photos");
        assert_eq!(k.unwrap().as_str(), "a/b/c.jpg");

        // Port on the Host is stripped; matching is case-insensitive.
        let (b, _) = route_request(Some("s3.example.com"), "Photos.S3.Example.com:9000", "/x");
        assert_eq!(b.unwrap().as_str(), "photos");

        // A bucket-only request (path is just "/") yields the bucket with no key.
        let (b, k) = route_request(Some("s3.example.com"), "logs.s3.example.com", "/");
        assert_eq!(b.unwrap().as_str(), "logs");
        assert!(k.is_none());
    }

    /// A bare domain Host (no bucket label) or a non-matching Host falls back to path-style routing,
    /// and an unset domain is always path-style.
    #[test]
    fn route_request_falls_back_to_path_style() {
        // Bare domain (no leading bucket label) -> path-style: `/bucket/key`.
        let (b, k) = route_request(Some("s3.example.com"), "s3.example.com", "/mybucket/obj");
        assert_eq!(b.unwrap().as_str(), "mybucket");
        assert_eq!(k.unwrap().as_str(), "obj");

        // Multi-label host under the domain is not a vhost bucket -> path-style.
        let (b, _) = route_request(
            Some("s3.example.com"),
            "a.b.s3.example.com",
            "/mybucket/obj",
        );
        assert_eq!(b.unwrap().as_str(), "mybucket");

        // A Host not under the domain -> path-style.
        let (b, _) = route_request(Some("s3.example.com"), "other.host.net", "/mybucket/obj");
        assert_eq!(b.unwrap().as_str(), "mybucket");

        // No domain configured -> always path-style even for a domain-shaped Host.
        let (b, k) = route_request(None, "photos.s3.example.com", "/mybucket/obj");
        assert_eq!(b.unwrap().as_str(), "mybucket");
        assert_eq!(k.unwrap().as_str(), "obj");
    }

    /// A buffered (`Bytes`) response stays a single bounded body.
    #[tokio::test]
    async fn bytes_response_round_trips() {
        let resp = S3Response {
            status: StatusCode::OK,
            headers: Vec::new(),
            body: S3Body::Bytes(Bytes::from_static(b"<xml/>")),
        };
        let body = render(resp).into_body();
        let collected = body.collect().await.expect("collect").to_bytes();
        assert_eq!(&collected[..], b"<xml/>");
    }

    /// A mid-stream blob error terminates the body with a transport error rather than panicking
    /// or silently truncating without signal.
    #[tokio::test]
    async fn stream_error_surfaces_as_body_error() {
        let chunks: Vec<Result<Bytes, BlobError>> = vec![
            Ok(Bytes::from_static(b"partial")),
            Err(BlobError::Io("disk gone".to_owned())),
        ];
        let stream: cairn_types::BlobStream = Box::pin(stream::iter(chunks));
        let resp = S3Response {
            status: StatusCode::OK,
            headers: Vec::new(),
            body: S3Body::Stream { length: 7, stream },
        };
        let mut body = render(resp).into_body();
        let first = body.frame().await.expect("first frame").expect("ok");
        assert_eq!(
            first.into_data().expect("data"),
            Bytes::from_static(b"partial")
        );
        let second = body.frame().await.expect("second frame");
        assert!(second.is_err(), "blob error must surface as a body error");
    }
}
