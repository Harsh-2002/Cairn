//! Adapts hyper's request/response to the library-neutral S3 request/response, performs
//! authentication, and routes path-style addressing into the S3 service.
//!
//! Object reads (`S3Body::Stream`) are forwarded to hyper as a streaming body so a large GET
//! flows blob -> socket with bounded memory (ARCH 7.4/7.6/7.8): no whole-object buffer is ever
//! materialised. Empty and in-memory (XML/error) bodies stay fully buffered, which is correct —
//! they are already small and bounded. Request bodies for object PUT are streamed separately via
//! [`shared_body_stream`], which keeps the incoming body reachable so an early error can drain
//! whatever the service left unread rather than poisoning the pooled keep-alive connection (issue #5).

use crate::stack::AppStack;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use bytes::{Buf, Bytes};
use cairn_crypto::{SystemClock, SystemCrypto};
use cairn_protocol::{S3Body, S3Request, S3Response, error_response};
use cairn_types::SecretString;
use cairn_types::auth::{AuthMethod, AuthOutcome, ClientSource, Principal, RequestView, Role};
use cairn_types::crypto::Nonce;
use cairn_types::error::{BodyError, Error};
use cairn_types::id::{BucketName, InvalidName, MAX_KEY_LEN, ObjectKey, UserId, VersionId};
use cairn_types::meta::{
    ActivityEntry, Mutation, SessionCredentialRecord, ShareDisposition, ShareLookupHash, ShareRow,
};
use cairn_types::time::Timestamp;
use cairn_types::traits::{Clock, Crypto};
use futures_util::StreamExt;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use std::net::IpAddr;
use zeroize::Zeroizing;

/// The unified HTTP response body: either a fully-buffered in-memory body (empty, XML, errors,
/// web console assets, management JSON) or a blob stream forwarded frame-by-frame from the blob store.
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

/// The immutable route role assigned when a listener is wired.
///
/// Keeping this as an enum rather than a boolean makes the trust boundary explicit at every
/// serving layer: a connection cannot accidentally gain both planes through a fall-through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListenerRole {
    /// S3, STS, signed public shares, and infrastructure endpoints.
    Data,
    /// The embedded console and versioned management API.
    Control,
}

impl ListenerRole {
    pub(crate) const fn is_data(self) -> bool {
        matches!(self, Self::Data)
    }

    pub(crate) const fn is_control(self) -> bool {
        matches!(self, Self::Control)
    }
}

/// Immutable connection provenance supplied by the accept loop.
///
/// Keeping the direct peer/TLS facts beside the trusted-proxy policy prevents later consumers from
/// accepting a forwarded value without also carrying its socket-level trust anchor.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestTransport<'a> {
    peer: IpAddr,
    direct_secure: bool,
    trusted_proxies: &'a crate::proxy::TrustedProxies,
    listener_role: ListenerRole,
}

impl<'a> RequestTransport<'a> {
    pub(crate) const fn new(
        peer: IpAddr,
        direct_secure: bool,
        trusted_proxies: &'a crate::proxy::TrustedProxies,
        listener_role: ListenerRole,
    ) -> Self {
        Self {
            peer,
            direct_secure,
            trusted_proxies,
            listener_role,
        }
    }
}

/// The route family selected before authentication or body processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListenerRoute {
    Data,
    PublicShare,
    ControlApi,
    ConsoleAsset,
    NotFound,
}

/// Apply the listener route matrix. No branch may fall through from one plane into the other.
fn listener_route(role: ListenerRole, method: &Method, path: &str) -> ListenerRoute {
    match role {
        ListenerRole::Data => {
            if is_control_path(path) {
                ListenerRoute::NotFound
            } else if (*method == Method::GET || *method == Method::HEAD)
                && path.starts_with("/share/")
            {
                ListenerRoute::PublicShare
            } else {
                ListenerRoute::Data
            }
        }
        ListenerRole::Control => {
            if is_control_path(path) {
                ListenerRoute::ControlApi
            } else if *method == Method::GET && is_console_asset(path) {
                ListenerRoute::ConsoleAsset
            } else {
                ListenerRoute::NotFound
            }
        }
    }
}

/// The root shell and concrete files embedded in the console bundle are the entire console route
/// family. The console uses hash routing, so client-side route fragments never reach the server.
fn is_console_asset(path: &str) -> bool {
    if path == "/" {
        return true;
    }
    path.strip_prefix('/')
        .filter(|relative| !relative.is_empty())
        .is_some_and(|relative| cairn_web::asset(relative).is_some())
}

/// Handle one request under the immutable role of the listener that accepted it.
pub async fn handle(
    stack: std::sync::Arc<AppStack>,
    req: Request<Incoming>,
    transport: RequestTransport<'_>,
    request_id: String,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Response<ResponseBody> {
    let secure = transport.direct_secure;
    let listener_role = transport.listener_role;
    let method = req.method().clone();
    let raw_path = req.uri().path().to_owned();
    let query_str = req.uri().query().unwrap_or("").to_owned();
    let mut headers: Vec<(String, String)> = req
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
    // Resolve client-address provenance once and carry that typed result through authentication and
    // authorization. An untrusted peer's forwarding headers are ignored; a trusted peer that omits
    // or contradicts them produces `Unavailable`, never the proxy's address.
    let source = request_client_source(transport, &headers);
    // The socket's TLS state remains authoritative for `aws:SecureTransport`. On a plaintext
    // control connection, validated forwarding provenance affects only the browser cookie's
    // externally-visible scheme.
    let cookie_secure = control_cookie_is_secure(transport, &headers);

    // Enforce the listener boundary before authentication or body collection. In particular this
    // makes `CAIRN_WEB_ADDR=off` genuinely headless, prevents `/api/v1` from leaking onto the S3
    // port, and prevents the control port from interpreting an unknown path as S3.
    let route = listener_route(listener_role, &method, &raw_path);
    if route == ListenerRoute::NotFound {
        let response = json_status(404, r#"{"error":"not found"}"#);
        return drain_or_close(response, request_has_body(&headers), req.into_body()).await;
    }
    // A browser preflights the exact presigned data URL with OPTIONS, which cannot itself verify a
    // signature minted for the eventual GET/PUT/etc. The signed console-origin query marker grants
    // only CORS metadata (never S3 authorization), so answer this narrow preflight before auth.
    if listener_role.is_data() && method == Method::OPTIONS {
        if let Some(response) = console_presign_preflight(&raw_path, &query_str, &headers) {
            return drain_or_close(response, request_has_body(&headers), req.into_body()).await;
        }
    }

    // Console session cookie → Bearer. On the web-console listener only, a request carrying the
    // `cairn_session` httpOnly cookie (and no explicit Authorization header) is authenticated as if
    // it sent the Bearer token the cookie holds — so the console never has to keep the credential in
    // JS-readable storage. Gated on the control role because cookies are NOT port-isolated: a cookie
    // set by the console on :7374 is also sent to the S3 data plane on :7373, which must ignore it.
    // The login endpoint (POST /api/v1/session) is exempt so a stale/invalid cookie cannot turn a
    // fresh sign-in into a 401 before the body credentials are even checked.
    let is_login = method == Method::POST && raw_path == "/api/v1/session";
    if listener_role.is_control() && !is_login && !headers.iter().any(|(k, _)| k == "authorization")
    {
        if let Some(token) = session_cookie_token(&headers) {
            headers.push(("authorization".to_owned(), format!("Bearer {token}")));
        }
    }

    // AWS-STS wire surface (ARCH 14): a form `POST /` on the **S3 data plane** carries an STS mint
    // request (`AssumeRole`/`GetSessionToken`). It must be intercepted BEFORE the generic
    // authenticate block, which would reject the `sts`-scoped signature as Malformed. Gated strictly
    // to the data listener, the root path, and the form content type, so no normal S3
    // request is captured; disabled entirely by `CAIRN_STS_ENABLED=false`.
    if stack.sts_enabled
        && listener_role.is_data()
        && method == Method::POST
        && raw_path == "/"
        && content_type_is_form(&headers)
    {
        return handle_sts(&stack, req, headers, host, source, secure, request_id).await;
    }

    // Authenticate against a borrowed, library-neutral view.
    let principal = {
        let view = RequestView {
            method: method.as_str(),
            path: &raw_path,
            query: &query_str,
            headers: &headers,
            host: &host,
            source,
            secure_transport: secure,
        };
        match stack.auth.authenticate(&view).await {
            AuthOutcome::Authenticated(p) => Some(p),
            AuthOutcome::NotApplicable => None,
            AuthOutcome::Denied(e) => {
                let resource = raw_path.clone();
                let response = render_negotiated(
                    error_response(&Error::from(e), &resource, &request_id),
                    crate::error_page::wants_html_pairs(&method, &headers),
                    &resource,
                );
                // Authentication rejects before any handler touches the body, so a body-bearing
                // request (e.g. a bad-signature PUT) leaves its bytes entirely unread. Drain them
                // (bounded) so the client can finish sending and reliably receive this 4xx, else
                // close — otherwise the leftover bytes mis-frame the next pooled request (issue #5).
                return drain_or_close(response, request_has_body(&headers), req.into_body()).await;
            }
        }
    };

    // The management API exists only on the control listener. `listener_route` already checked the
    // exact segment boundary, so stripping this prefix cannot capture `/api/v10`.
    if route == ListenerRoute::ControlApi {
        let subpath = raw_path
            .strip_prefix("/api/v1")
            .expect("control route has the /api/v1 prefix");
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
                    .header("content-type", "application/json")
                    // We stopped reading at the cap, so the rest of the oversize body is still in
                    // flight; close rather than let those bytes mis-frame the next pooled request (#5).
                    .header(http::header::CONNECTION, "close");
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
        // POST /api/v1/buckets/{bucket}/objects/shares (the plural collection).
        if method == Method::POST {
            if let Some(bucket) = subpath
                .strip_prefix("/buckets/")
                .and_then(|r| r.strip_suffix("/objects/shares"))
            {
                return create_share(
                    &stack,
                    bucket,
                    &body_bytes,
                    principal.as_ref(),
                    &host,
                    secure,
                )
                .await;
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
        // Console session: login (POST), whoami (GET), and logout (DELETE). Lives here, not in
        // cairn-control, because it sets/clears the httpOnly `Set-Cookie` and validates login against
        // the server auth chain — both transport concerns the JSON control plane does not own.
        if subpath == "/session" {
            let session_transport = SessionTransport {
                host: &host,
                source,
                direct_secure: secure,
                cookie_secure,
            };
            return session_endpoint(
                &stack,
                &method,
                &body_bytes,
                principal.as_ref(),
                session_transport,
            )
            .await;
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
    // The control listener serves only the root shell and concrete embedded assets. Unknown paths
    // were rejected above and can never fall through into S3.
    if route == ListenerRoute::ConsoleAsset {
        if raw_path == "/" {
            let (content_type, bytes) = cairn_web::spa_shell();
            return web_asset_response(content_type, bytes.into_owned());
        }
        if let Some(rel) = raw_path.strip_prefix('/').filter(|r| !r.is_empty()) {
            if let Some((content_type, bytes)) = cairn_web::asset(rel) {
                return web_asset_response(content_type, bytes.into_owned());
            }
        }
        // The classifier checked the same immutable embedded bundle; keep a fail-closed fallback in
        // case that invariant ever changes.
        return json_status(404, r#"{"error":"not found"}"#);
    }

    // Persistent public-read ("share") URLs: GET|HEAD /share/{token} — unauthenticated, resolved by
    // an opaque registry token (ARCH 15.8). They exist only on the data listener.
    if route == ListenerRoute::PublicShare {
        let token = &raw_path["/share/".len()..]; // after "/share/"
        if token.is_empty() || token.contains('/') {
            return json_status(404, r#"{"error":"not found"}"#);
        }
        return serve_share(&stack, token, method, &headers, source, secure, request_id).await;
    }

    // Virtual-host-style addressing (ARCH 13.1): when `CAIRN_S3_DOMAIN` is configured and the
    // request Host is `<bucket>.<s3_domain>`, the bucket is taken from the Host and the entire path
    // is the key. Otherwise fall back to path-style routing (`/<bucket>/<key>`).
    // A malformed bucket/key must be REJECTED here, not silently dropped: a `None` segment is how
    // `dispatch` decides an operation is bucket- or root-level, so an unparseable key would re-route
    // an object request to the bucket handler (audit 2026-07).
    // Whether the request declares a body: only a body-bearing request can poison a pooled
    // keep-alive connection when an early error leaves its bytes unread (issue #5).
    let body_bearing = request_has_body(&headers);
    let (bucket, key) = match route_request(stack.s3_domain.as_deref(), &host, &raw_path) {
        Ok(bk) => bk,
        Err(e) => {
            // Routing rejected the request before it reached a handler, so its body is unread.
            let response = render_negotiated(
                error_response(&e, &raw_path, &request_id),
                crate::error_page::wants_html_pairs(&method, &headers),
                &raw_path,
            );
            return drain_or_close(response, body_bearing, req.into_body()).await;
        }
    };
    let query = parse_query(&query_str);
    // Share the incoming body: the service streams object-PUT bytes out of it during `handle`, but
    // if it returns before consuming the body (an early reject) we still need to reach the leftover
    // bytes to drain them — hence the shared handle rather than moving the body away wholesale (#5).
    let shared = std::sync::Arc::new(tokio::sync::Mutex::new(SharedIncoming {
        incoming: req.into_body(),
        ended: false,
    }));
    let body = shared_body_stream(shared.clone());

    // Decide browser-vs-machine now: the request head moves into `S3Request` on the next line.
    let wants_html = crate::error_page::wants_html_pairs(&method, &headers);
    let console_cors_origin = principal
        .as_ref()
        .filter(|principal| principal.is_session)
        .and_then(|_| console_actual_cors_origin(&query_str, &headers));
    let s3req = S3Request {
        method,
        bucket,
        key,
        query,
        headers,
        principal,
        source,
        secure,
        request_id,
    };
    let mut response = render_negotiated(stack.s3.handle(s3req, body).await, wants_html, &raw_path);
    if let Some(origin) = console_cors_origin {
        if let Ok(value) = origin.parse() {
            response
                .headers_mut()
                .insert("access-control-allow-origin", value);
        }
        merge_csv_header(response.headers_mut(), "vary", "Origin");
        merge_csv_header(
            response.headers_mut(),
            "access-control-expose-headers",
            "ETag, Content-Length, Content-Range, Content-Type, Last-Modified, x-amz-version-id, x-amz-request-id",
        );
    }
    // If the service returned before consuming the body — e.g. an UploadPart rejected for an unknown
    // uploadId, scoped ahead of `stage_part` — the unread request bytes are still in flight and would
    // mis-frame the next request on the pooled HTTP/1.1 connection (issue #5). Drain a bounded amount
    // so the client can finish sending and reliably RECEIVE this response; past the cap, close instead
    // of mis-framing. A fully-consumed body (the normal PUT path) drains to nothing here and keeps
    // keep-alive intact.
    finish_body_hygiene(response, body_bearing, &shared).await
}

/// Whether `path` is the versioned management namespace. The segment boundary matters:
/// `/api/v10` remains an ordinary S3 path rather than being captured as `/api/v1`.
pub(crate) fn is_control_path(path: &str) -> bool {
    path == "/api/v1" || path.starts_with("/api/v1/")
}

/// Whether the request declares an `application/x-www-form-urlencoded` content type — the strict
/// gate that distinguishes an STS form `POST /` from any other root request on the S3 listener.
fn content_type_is_form(headers: &[(String, String)]) -> bool {
    headers
        .iter()
        .find(|(k, _)| k == "content-type")
        .is_some_and(|(_, v)| {
            v.trim()
                .to_ascii_lowercase()
                .starts_with("application/x-www-form-urlencoded")
        })
}

/// The maximum STS form body Cairn will buffer. The params (`Action`/`DurationSeconds`/`Policy`/…)
/// are tiny; anything larger is not an STS request and is refused rather than buffered (DoS bound).
const STS_MAX_BODY: usize = 16 * 1024;

/// Serve an STS mint request (ARCH 14): buffer the bounded form body, bind it to the signature
/// (hash + `authenticate_sts`, genuine SigV4 only — no dev bypass, no session chaining), then
/// dispatch on `Action`. Terminal — always returns an STS XML document (success or the
/// query-protocol error shape). The plaintext secret + token appear once in the success body and are
/// never logged (this response is not routed through any body-logging path).
async fn handle_sts(
    stack: &std::sync::Arc<AppStack>,
    req: Request<Incoming>,
    headers: Vec<(String, String)>,
    host: String,
    source: ClientSource,
    secure: bool,
    request_id: String,
) -> Response<ResponseBody> {
    // Buffer the (small, bounded) form body. Oversize is not an STS request; refuse it.
    let body_bytes = match http_body_util::Limited::new(req.into_body(), STS_MAX_BODY)
        .collect()
        .await
    {
        Ok(c) => c.to_bytes(),
        Err(_) => {
            return sts_response(
                crate::sts::StsHttpResponse {
                    status: 400,
                    body: cairn_xml::sts_error_document(
                        "InvalidRequest",
                        "the STS request body is malformed or too large",
                        &request_id,
                    ),
                },
                &request_id,
            );
        }
    };
    // Bind the body to the signature: the STS SDK signer folds sha256(body) into the canonical
    // request (there is no trusted `x-amz-content-sha256` header to read for non-S3 services), so
    // authentication verifies a genuine `sts`-scoped SigV4 signature over that same body hash —
    // Action/DurationSeconds/Policy are all signature-bound.
    let body_sha256 = cairn_auth::sha256_hex(&body_bytes);
    let view = RequestView {
        method: "POST",
        path: "/",
        query: "",
        headers: &headers,
        host: &host,
        source,
        secure_transport: secure,
    };
    let principal = match stack.auth_chain.authenticate_sts(&view, &body_sha256).await {
        AuthOutcome::Authenticated(p) => p,
        AuthOutcome::Denied(e) => {
            return sts_response(
                crate::sts::auth_error_response(&e, &request_id),
                &request_id,
            );
        }
        AuthOutcome::NotApplicable => {
            return sts_response(
                crate::sts::StsHttpResponse {
                    status: 403,
                    body: cairn_xml::sts_error_document(
                        "AccessDenied",
                        "the request is not authenticated",
                        &request_id,
                    ),
                },
                &request_id,
            );
        }
    };
    let resp = crate::sts::handle(stack, &body_bytes, &principal, &request_id).await;
    sts_response(resp, &request_id)
}

/// Render an [`crate::sts::StsHttpResponse`] onto the wire with `text/xml` and the request id.
fn sts_response(resp: crate::sts::StsHttpResponse, request_id: &str) -> Response<ResponseBody> {
    let mut builder = Response::builder()
        .status(resp.status)
        .header("content-type", "text/xml");
    if let Ok(v) = http::HeaderValue::from_str(request_id) {
        builder = builder.header("x-amz-request-id", v);
    }
    builder
        .body(full_body(Bytes::from(resp.body)))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// Build a 200 response for an embedded web console asset with its content type.
fn web_asset_response(content_type: String, bytes: Vec<u8>) -> Response<ResponseBody> {
    Response::builder()
        .status(200)
        .header("content-type", content_type)
        .body(full_body(Bytes::from(bytes)))
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

/// Build a JSON response with the given status, body, and extra response headers (e.g. `Set-Cookie`).
fn json_status_with_headers(
    status: u16,
    body: &str,
    extra: &[(&str, String)],
) -> Response<ResponseBody> {
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", "application/json");
    for (k, v) in extra {
        builder = builder.header(*k, v.as_str());
    }
    builder
        .body(full_body(Bytes::from(body.to_owned())))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// Name of the console's httpOnly session cookie (set on the web-console listener only).
const SESSION_COOKIE: &str = "cairn_session";
/// How long the browser keeps the session cookie before it must sign in again.
const SESSION_COOKIE_MAX_AGE_SECS: u64 = 43_200; // 12 hours

/// Extract the Bearer token carried by the console session cookie, if present and well-formed.
fn session_cookie_token(headers: &[(String, String)]) -> Option<String> {
    let cookie = headers
        .iter()
        .find(|(k, _)| k == "cookie")
        .map(|(_, v)| v.as_str())?;
    let b64 = cookie_value(cookie, SESSION_COOKIE)?;
    let bytes = B64URL.decode(b64).ok()?;
    String::from_utf8(bytes).ok()
}

/// Read a single named cookie's value out of a `Cookie:` request-header string.
fn cookie_value(cookie_header: &str, name: &str) -> Option<String> {
    cookie_header.split(';').find_map(|pair| {
        let (k, v) = pair.trim().split_once('=')?;
        (k.trim() == name).then(|| v.trim().to_owned())
    })
}

/// `Set-Cookie` value that stores `token` in the httpOnly session cookie. `Secure` is added when the
/// externally-visible control transport is authenticated as HTTPS (direct TLS or validated trusted
/// proxy provenance); a direct loopback HTTP dev listener can still store it. `SameSite=Strict`
/// keeps the cookie off every cross-site request, which is the CSRF defense for the
/// cookie-authenticated API.
fn set_session_cookie(token: &str, secure: bool) -> String {
    let value = B64URL.encode(token.as_bytes());
    let mut c = format!(
        "{SESSION_COOKIE}={value}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_COOKIE_MAX_AGE_SECS}"
    );
    if secure {
        c.push_str("; Secure");
    }
    c
}

/// `Set-Cookie` value that immediately expires the session cookie (logout).
fn clear_session_cookie(secure: bool) -> String {
    let mut c = format!("{SESSION_COOKIE}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0");
    if secure {
        c.push_str("; Secure");
    }
    c
}

fn control_cookie_is_secure(transport: RequestTransport<'_>, headers: &[(String, String)]) -> bool {
    transport.listener_role.is_control()
        && crate::proxy::effective_scheme(
            transport.direct_secure,
            transport.peer,
            headers,
            transport.trusted_proxies,
        )
        .is_https()
}

fn request_client_source(
    transport: RequestTransport<'_>,
    headers: &[(String, String)],
) -> ClientSource {
    crate::proxy::client_source(transport.peer, headers, transport.trusted_proxies)
}

#[derive(Debug, Clone, Copy)]
struct SessionTransport<'a> {
    host: &'a str,
    source: ClientSource,
    direct_secure: bool,
    cookie_secure: bool,
}

/// The console session endpoint: `POST` signs in (validates `{access_key, secret_key}` via the auth
/// chain and sets the httpOnly cookie), `GET` reports the current session (so the SPA can decide
/// whether to show the console or the login screen without ever reading the token), and `DELETE`
/// signs out (expires the cookie). Admin-only sign-in: the console is an administrator surface.
async fn session_endpoint(
    stack: &AppStack,
    method: &Method,
    body: &Bytes,
    principal: Option<&Principal>,
    transport: SessionTransport<'_>,
) -> Response<ResponseBody> {
    match *method {
        Method::POST => {
            #[derive(serde::Deserialize)]
            struct LoginReq {
                access_key: String,
                secret_key: String,
            }
            let req: LoginReq = match serde_json::from_slice(body) {
                Ok(r) => r,
                Err(_) => return json_status(400, r#"{"error":"invalid request body"}"#),
            };
            if req.access_key.is_empty() || req.secret_key.is_empty() {
                return json_status(400, r#"{"error":"access_key and secret_key are required"}"#);
            }
            // The console credential IS the Bearer token `<access_key>.<secret_key>`; validate it
            // through the same auth chain the API uses by synthesizing the header it expects.
            let token = format!("{}.{}", req.access_key, req.secret_key);
            let auth_headers = vec![
                ("authorization".to_owned(), format!("Bearer {token}")),
                ("host".to_owned(), transport.host.to_owned()),
            ];
            let view = RequestView {
                method: "POST",
                path: "/api/v1/session",
                query: "",
                headers: &auth_headers,
                host: transport.host,
                source: transport.source,
                secure_transport: transport.direct_secure,
            };
            match stack.auth.authenticate(&view).await {
                AuthOutcome::Authenticated(p) if p.role == Role::Administrator => {
                    // The body never carries the secret — only the cookie (httpOnly) does.
                    let body = serde_json::json!({
                        "access_key_id": p.access_key_id,
                        "display_name": p.display_name,
                        "role": "administrator",
                    })
                    .to_string();
                    json_status_with_headers(
                        200,
                        &body,
                        &[(
                            "set-cookie",
                            set_session_cookie(&token, transport.cookie_secure),
                        )],
                    )
                }
                AuthOutcome::Authenticated(_) => json_status(
                    403,
                    r#"{"error":"That credential is not an administrator. Only an admin can use the console."}"#,
                ),
                _ => json_status(401, r#"{"error":"Access key or secret key is incorrect."}"#),
            }
        }
        Method::GET => match principal {
            Some(p) => {
                let role = if p.role == Role::Administrator {
                    "administrator"
                } else {
                    "member"
                };
                let body = serde_json::json!({
                    "access_key_id": p.access_key_id,
                    "display_name": p.display_name,
                    "role": role,
                })
                .to_string();
                json_status(200, &body)
            }
            None => json_status(401, r#"{"error":"not authenticated"}"#),
        },
        Method::DELETE => json_status_with_headers(
            200,
            r#"{"ok":true}"#,
            &[("set-cookie", clear_session_cookie(transport.cookie_secure))],
        ),
        _ => json_status(405, r#"{"error":"method not allowed"}"#),
    }
}

/// Strip header-injection and quoting characters from a download filename before it goes into
/// `Content-Disposition`.
fn sanitize_filename(s: &str) -> String {
    s.chars()
        .filter(|c| !matches!(c, '"' | '\\' | '\r' | '\n'))
        .collect()
}

/// A 256-bit opaque token (two v4 UUIDs of hex), URL-safe and unguessable. Persistent-share
/// callers immediately wrap it in [`SecretString`] and persist only [`ShareLookupHash`].
fn generate_share_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Mint a persistent public-read ("share") token for an object (ARCH 15.8). Admin-only. Body:
/// `{"key", "expires_in_secs"?: null=forever, "disposition"?: "inline"|"attachment", "filename"?,
/// "version_id"?}`. Returns `{"id","token","url","expires_at_ms"}` exactly once; subsequent
/// management responses contain only the stable non-secret `id`.
async fn create_share(
    stack: &AppStack,
    bucket: &str,
    body: &Bytes,
    principal: Option<&Principal>,
    request_host: &str,
    secure: bool,
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
    let id = uuid::Uuid::new_v4().simple().to_string();
    let token = SecretString::new(generate_share_token());
    let row = ShareRow {
        id: id.clone(),
        token_hash: ShareLookupHash::for_token(token.expose_secret()),
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
    let (scheme, data_host) = data_scheme_host(stack, request_host, secure);
    let url = format!("{scheme}://{data_host}/share/{}", token.expose_secret());
    #[derive(serde::Serialize)]
    struct ShareCreateResponse<'a> {
        id: &'a str,
        token: &'a SecretString,
        url: &'a str,
        expires_at_ms: Option<i64>,
    }
    let response = ShareCreateResponse {
        id: &id,
        token: &token,
        url: &url,
        expires_at_ms: expires_at.map(|t| t.0),
    };
    match serde_json::to_string(&response) {
        Ok(json) => json_status(200, &json),
        Err(_) => json_status(500, r#"{"error":"could not encode share"}"#),
    }
}

#[derive(serde::Deserialize)]
struct PresignSessionHandle {
    access_key_id: String,
    session_token: SecretString,
}

#[derive(serde::Serialize)]
struct PresignSessionResponse {
    access_key_id: String,
    session_token: SecretString,
    expires_at_ms: i64,
}

#[derive(serde::Serialize)]
struct PresignResponse {
    url: String,
    expires_at_ms: i64,
    absolute: bool,
    session: PresignSessionResponse,
}

struct ConsoleSigningCredential {
    access_key_id: String,
    secret: Zeroizing<String>,
    session_token: SecretString,
    expires_at: Timestamp,
}

/// Mint an interoperable S3 presigned URL (ARCH 14.2). Admin-only.
///
/// The signing key is a durable, bucket-scoped temporary session credential, never the
/// administrator's long-lived SigV4 secret. The browser receives only the public access-key id and
/// opaque session token, which it may return to reuse the same sealed credential; the temporary
/// signing secret remains server-side. Generic `query`/`headers` fields support the console's S3
/// calls, while the legacy object-share fields remain accepted by the CLI and share dialog.
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
        #[serde(default)]
        key: String,
        #[serde(default)]
        method: Option<String>,
        expires_in_secs: i64,
        #[serde(default)]
        query: Vec<(String, String)>,
        #[serde(default)]
        headers: Vec<(String, String)>,
        #[serde(default)]
        origin: Option<String>,
        #[serde(default)]
        session: Option<PresignSessionHandle>,
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
    // An empty key means a bucket-level request. Object-level callers still receive a normal S3
    // error if they choose a method/query combination that requires a key.
    if !req.key.is_empty() && ObjectKey::parse(&req.key).is_err() {
        return json_status(400, r#"{"error":"key is invalid"}"#);
    }
    // WHATWG URL parsing removes literal and percent-encoded `.` / `..` path segments before an
    // HTTP request is sent. A presigned browser URL for such a key can therefore target a different
    // canonical path than the one the operator selected. There is no lossless path-style surrogate
    // in this protocol, so fail closed; direct SDK/CLI S3 requests remain available.
    if presign_key_has_browser_dot_segment(&req.key) {
        return json_status(
            400,
            r#"{"error":"keys containing '.' or '..' path segments cannot be presigned safely for a browser; use a direct S3 client or the Cairn CLI"}"#,
        );
    }
    let http_method = req.method.as_deref().unwrap_or("GET").to_ascii_uppercase();
    if !matches!(
        http_method.as_str(),
        "GET" | "HEAD" | "PUT" | "POST" | "DELETE"
    ) {
        return json_status(
            400,
            r#"{"error":"method must be GET, HEAD, PUT, POST, or DELETE"}"#,
        );
    }
    // The backing temporary credential has the same 12-hour ceiling as every other session.
    if !(1..=43_200).contains(&req.expires_in_secs) {
        return json_status(
            400,
            r#"{"error":"expires_in_secs must be between 1 and 43200 (12 hours)"}"#,
        );
    }

    if req.query.len() > 32 || req.headers.len() > 16 {
        return json_status(
            400,
            r#"{"error":"too many signed query parameters or headers"}"#,
        );
    }
    let mut extra_query = Vec::with_capacity(req.query.len() + 4);
    for (name, value) in req.query {
        if name.is_empty()
            || name.len() > 128
            || value.len() > 4096
            || name.to_ascii_lowercase().starts_with("x-amz-")
            || name.eq_ignore_ascii_case("x-cairn-console-origin")
        {
            return json_status(400, r#"{"error":"invalid signed query parameter"}"#);
        }
        extra_query.push((name, value));
    }
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
    let mut extra_signed_headers = Vec::with_capacity(req.headers.len() + 1);
    for (name, value) in req.headers {
        let name = name.to_ascii_lowercase();
        if !matches!(
            name.as_str(),
            "content-type" | "range" | "x-amz-copy-source" | "x-amz-bypass-governance-retention"
        ) || value.len() > 4096
            || extra_signed_headers
                .iter()
                .any(|(existing, _)| existing == &name)
        {
            return json_status(400, r#"{"error":"invalid or duplicate signed header"}"#);
        }
        extra_signed_headers.push((name, value));
    }
    if http_method == "PUT" {
        if let Some(ct) = &req.content_type {
            if extra_signed_headers
                .iter()
                .any(|(name, _)| name == "content-type")
            {
                return json_status(400, r#"{"error":"duplicate content-type header"}"#);
            }
            extra_signed_headers.push(("content-type".to_owned(), ct.clone()));
        }
    }

    let (scheme, signed_host) = data_scheme_host(stack, host, secure);
    if let Some(origin) = req.origin.as_deref() {
        let Some(origin) = normalize_origin(origin) else {
            return json_status(400, r#"{"error":"origin must be an http(s) origin"}"#);
        };
        if origin == format!("{scheme}://{signed_host}") {
            return json_status(
                400,
                r#"{"error":"console and data origins must be distinct"}"#,
            );
        }
        extra_query.push(("X-Cairn-Console-Origin".to_owned(), origin));
    }

    let now = SystemClock::new().now();
    let required_expiry = Timestamp(now.0 + req.expires_in_secs * 1000);
    let credential =
        match console_signing_credential(stack, p, bucket, req.session, required_expiry, now).await
        {
            Ok(credential) => credential,
            Err(response) => return response,
        };
    extra_query.push((
        "X-Amz-Security-Token".to_owned(),
        credential.session_token.expose_secret().to_owned(),
    ));
    let amz_date = format_amz_date(now);
    let path_query = cairn_auth::mint_presigned(&cairn_auth::PresignRequest {
        method: &http_method,
        host: &signed_host,
        bucket,
        key: &req.key,
        access_key_id: &credential.access_key_id,
        secret: &credential.secret,
        region: &stack.region,
        expires_secs: req.expires_in_secs,
        amz_date: &amz_date,
        extra_query,
        extra_signed_headers,
    });
    let expires_at = now.as_millis() + req.expires_in_secs * 1000;
    let url = format!("{scheme}://{signed_host}{path_query}");
    let response = PresignResponse {
        url,
        expires_at_ms: expires_at,
        absolute: true,
        session: PresignSessionResponse {
            access_key_id: credential.access_key_id,
            session_token: credential.session_token,
            expires_at_ms: credential.expires_at.0,
        },
    };
    match serde_json::to_string(&response) {
        Ok(body) => json_status(200, &body),
        Err(_) => json_status(500, r#"{"error":"internal error"}"#),
    }
}

async fn console_signing_credential(
    stack: &AppStack,
    principal: &Principal,
    bucket: &str,
    supplied: Option<PresignSessionHandle>,
    required_expiry: Timestamp,
    now: Timestamp,
) -> Result<ConsoleSigningCredential, Response<ResponseBody>> {
    // This is an administrator-derived session, so its bucket Allow is only the requested boundary:
    // retain every current explicit Deny from the parent identity policy. Otherwise an admin with
    // a Deny boundary could use the console transfer session to escape it (AUD-038).
    let boundary = console_session_policy(bucket);
    let policy =
        crate::sts::administrator_bounded_policy(&stack.meta, &principal.user_id, &boundary)
            .await
            .map_err(|_| json_status(500, r#"{"error":"internal error"}"#))?;
    if let Some(supplied) = supplied {
        let lookup = stack
            .meta
            .user_by_session_key(&supplied.access_key_id)
            .await
            .map_err(|_| json_status(500, r#"{"error":"internal error"}"#))?;
        if let Some(creds) = lookup {
            let presented_hash =
                cairn_auth::hash_session_token(supplied.session_token.expose_secret());
            let token_matches = stack.crypto.ct_eq(
                presented_hash.as_bytes(),
                creds.session_token_hash.as_bytes(),
            );
            if creds.parent_user_id == principal.user_id
                && creds.parent_is_active
                && creds.expires_at >= required_expiry
                && creds.inline_policy.as_deref() == Some(policy.as_str())
                && token_matches
            {
                let opened = stack
                    .crypto
                    .open(&creds.secret_ciphertext, &Nonce(creds.secret_nonce))
                    .map_err(|_| json_status(500, r#"{"error":"internal error"}"#))?;
                return Ok(ConsoleSigningCredential {
                    access_key_id: supplied.access_key_id,
                    secret: Zeroizing::new(String::from_utf8_lossy(&opened).into_owned()),
                    session_token: supplied.session_token,
                    expires_at: creds.expires_at,
                });
            }
        }
        // An expired/mismatched handle is not an authentication decision — the management request
        // is already authenticated. Mint a fresh scoped session so a reloaded tab recovers cleanly.
    }

    let access_key_id = format!(
        "CAIRNTMP{}",
        uuid::Uuid::new_v4().simple().to_string().to_uppercase()
    );
    let secret = Zeroizing::new(generate_share_token());
    let session_token = SecretString::new(generate_share_token());
    let sealed = stack
        .crypto
        .seal(secret.as_bytes())
        .map_err(|_| json_status(500, r#"{"error":"internal error"}"#))?;
    let minimum_expiry = Timestamp(now.0 + 900_000);
    let expires_at = std::cmp::max(required_expiry, minimum_expiry);
    let record = SessionCredentialRecord {
        access_key_id: access_key_id.clone(),
        parent_user_id: principal.user_id.clone(),
        secret_ciphertext: sealed.ciphertext,
        secret_nonce: None,
        session_token_hash: cairn_auth::hash_session_token(session_token.expose_secret()),
        inline_policy: Some(policy),
        expires_at,
        created_at: now,
    };
    stack
        .meta
        .submit(Mutation::CreateSessionCredential(Box::new(record)))
        .await
        .map_err(|_| json_status(500, r#"{"error":"internal error"}"#))?;
    let _ = stack
        .meta
        .submit(Mutation::RecordActivity(Box::new(ActivityEntry {
            id: uuid::Uuid::new_v4().simple().to_string(),
            action: "MintConsoleTransferSession".to_owned(),
            bucket: Some(bucket.to_owned()),
            key: None,
            size: None,
            etag: None,
            actor: Some(principal.access_key_id.clone()),
            at: now,
        })))
        .await;
    Ok(ConsoleSigningCredential {
        access_key_id,
        secret,
        session_token,
        expires_at,
    })
}

fn console_session_policy(bucket: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "CairnConsoleTransfer",
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                format!("arn:aws:s3:::{bucket}"),
                format!("arn:aws:s3:::{bucket}/*")
            ]
        }]
    })
    .to_string()
}

/// Whether a raw object key contains a segment a browser can interpret as `.` or `..`.
///
/// API callers send raw keys, but reject a small number of percent-encoded aliases too: this keeps
/// the boundary fail-closed if a caller accidentally submits an encoded path instead of a raw key,
/// and covers mixed forms such as `.%2e`. Two decoding passes also catch a doubly encoded alias
/// without attempting unbounded recursive decoding.
fn presign_key_has_browser_dot_segment(key: &str) -> bool {
    key.split('/').any(|segment| {
        let mut candidate = segment.to_owned();
        for _ in 0..=2 {
            if matches!(candidate.as_str(), "." | "..") {
                return true;
            }
            let decoded = pct_decode(&candidate);
            if decoded == candidate {
                return false;
            }
            candidate = decoded;
        }
        matches!(candidate.as_str(), "." | "..")
    })
}

/// Resolve the data-plane `(scheme, host)` for an absolute share/presigned URL. An explicit public
/// base URL wins. Otherwise retain the control request's hostname but replace its port with the
/// configured data-listener port, which makes local/default deployments work without co-locating
/// object bytes on the console origin.
fn data_scheme_host(stack: &AppStack, req_host: &str, secure: bool) -> (String, String) {
    if let Some(base) = stack.public_base_url.as_deref() {
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
    let host = req_host
        .parse::<http::uri::Authority>()
        .ok()
        .map(|authority| authority.host().to_owned())
        .filter(|hostname| !hostname.is_empty())
        .unwrap_or_else(|| stack.data_listen_addr.ip().to_string());
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host
    };
    (
        if secure { "https" } else { "http" }.to_owned(),
        format!("{host}:{}", stack.data_listen_addr.port()),
    )
}

fn normalize_origin(origin: &str) -> Option<String> {
    let uri = origin.parse::<http::Uri>().ok()?;
    let scheme = uri.scheme_str()?;
    if !matches!(scheme, "http" | "https") || uri.query().is_some() {
        return None;
    }
    let authority = uri.authority()?.as_str();
    if authority.is_empty() || !matches!(uri.path(), "" | "/") {
        return None;
    }
    Some(format!("{scheme}://{authority}"))
}

/// Extract a console CORS marker only from a complete presigned-session query. The marker itself is
/// signature-bound on the actual request; merely forging these public parameter names can at most
/// obtain a preflight response and never authorizes S3 access.
fn console_origin_query_marker(query: &str) -> Option<String> {
    let params = parse_query(query);
    for required in [
        "X-Amz-Algorithm",
        "X-Amz-Credential",
        "X-Amz-Security-Token",
        "X-Amz-Signature",
    ] {
        if !params
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case(required) && !value.is_empty())
        {
            return None;
        }
    }
    params
        .into_iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("X-Cairn-Console-Origin"))
        .and_then(|(_, value)| normalize_origin(&value))
}

fn request_header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// Merge comma-delimited response metadata without discarding a handler's existing value. Console
/// CORS is applied after S3/error rendering, whose `Vary` and exposed-header entries remain
/// security/cache contracts of their own.
fn merge_csv_header(headers: &mut hyper::HeaderMap, name: &'static str, additions: &str) {
    let mut values = headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map_or_else(Vec::new, |value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        });
    for addition in additions
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if !values
            .iter()
            .any(|value| value.eq_ignore_ascii_case(addition))
        {
            values.push(addition.to_owned());
        }
    }
    if let Ok(value) = values.join(", ").parse() {
        headers.insert(name, value);
    }
}

fn console_presign_preflight(
    path: &str,
    query: &str,
    headers: &[(String, String)],
) -> Option<Response<ResponseBody>> {
    // Console presigns always target a bucket/key path. Keep the public-share and service roots out
    // of this special CORS lane even though reflecting CORS headers alone would grant no access.
    if path == "/" || path.starts_with("/share/") {
        return None;
    }
    let marker = console_origin_query_marker(query)?;
    let origin = normalize_origin(request_header(headers, "origin")?)?;
    if marker != origin {
        return None;
    }
    let requested_method = request_header(headers, "access-control-request-method")?
        .trim()
        .to_ascii_uppercase();
    if !matches!(
        requested_method.as_str(),
        "GET" | "HEAD" | "PUT" | "POST" | "DELETE"
    ) {
        return None;
    }

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("access-control-allow-origin", origin)
        .header("access-control-allow-methods", requested_method)
        .header(
            "vary",
            "Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
        )
        .header("access-control-max-age", "300");
    if let Some(requested_headers) = request_header(headers, "access-control-request-headers") {
        builder = builder.header("access-control-allow-headers", requested_headers);
    }
    Some(
        builder
            .body(full_body(Bytes::new()))
            .unwrap_or_else(|_| Response::new(full_body(Bytes::new()))),
    )
}

fn console_actual_cors_origin(query: &str, headers: &[(String, String)]) -> Option<String> {
    let marker = console_origin_query_marker(query)?;
    let origin = normalize_origin(request_header(headers, "origin")?)?;
    (marker == origin).then_some(origin)
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
    source: ClientSource,
    secure: bool,
    request_id: String,
) -> Response<ResponseBody> {
    // A share link is opened by a person far more often than by a program, so a dead link explains
    // itself as a page. The share token is a credential and is deliberately NOT echoed into the
    // page (the resource is left blank) — it already rides the URL, and `Referrer-Policy:
    // no-referrer` keeps it out of onward requests.
    let wants_html = crate::error_page::wants_html_pairs(&method, in_headers);
    let share_err = |status: u16, json: &'static str| -> Response<ResponseBody> {
        let code = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST);
        if wants_html {
            html_error_response(code, "", "", &request_id, &[])
        } else {
            json_status(status, json)
        }
    };

    let token_hash = ShareLookupHash::for_token(token);
    let row = match stack.meta.get_share_by_token_hash(&token_hash).await {
        Ok(Some(r)) => r,
        Ok(None) => return share_err(404, r#"{"error":"not found"}"#),
        Err(_) => return share_err(500, r#"{"error":"internal error"}"#),
    };
    if row.revoked_at.is_some() {
        return share_err(410, r#"{"error":"this share has been revoked"}"#);
    }
    if let Some(exp) = row.expires_at {
        if SystemClock::new().now().as_millis() > exp.0 {
            return share_err(410, r#"{"error":"this share has expired"}"#);
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
        source,
        secure,
        request_id,
    };
    let empty: cairn_types::BodyStream = Box::pin(futures_util::stream::empty());
    // A share link is the most browser-facing surface Cairn has: a failure here (revoked, expired,
    // or a deleted object) should read as a page, not as XML.
    let resp_raw = stack.s3.handle(s3req, empty).await;
    let mut resp = render_negotiated(resp_raw, wants_html, "");

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

/// Abbreviate the durable full SHA-256 identity for operator display.
///
/// The database keeps all 64 hex characters for collision-resistant same-id binding. This API
/// deliberately preserves the historical short, non-secret fingerprint and never exposes the
/// durable comparison value.
fn key_hash_for_status(durable_hash: &str) -> String {
    durable_hash.chars().take(8).collect()
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
        let rows = match s.key_ring_states().await {
            Ok(rows) => rows,
            Err(_) => return json_status(500, r#"{"error":"internal error"}"#),
        };
        for r in rows {
            let e = keys.entry(r.id).or_insert((String::new(), false, 0));
            e.0 = r.key_hash;
            e.1 = e.1 || r.is_active;
            e.2 = e.2.max(r.sealed_count);
        }
    }
    // Per-stream re-wrap completion: a stream is complete ONLY when every shard recorded a full,
    // failure-free re-wrap pass under the CURRENT active key (audit #29). A cleared cursor alone is
    // also the never-started state, so it must not read as complete — we compare the persisted
    // `done_active_id` against the live active id instead. With no sqlite shards (async backends,
    // which do not auto-re-wrap) nothing is verifiable, so nothing is ever reported complete.
    let active = stack.crypto.active_key_id();
    let has_shards = !stack.store.is_empty();
    let streams = crate::key_rewrap::SEALED_SECRET_STREAMS;
    let mut complete_by_stream: std::collections::BTreeMap<&str, bool> = streams
        .iter()
        .map(|stream| (stream.name(), has_shards))
        .collect();
    for s in &stack.store {
        let done: std::collections::HashMap<String, u16> = match s.rewrap_done_active_ids().await {
            Ok(done) => done.into_iter().collect(),
            Err(_) => return json_status(500, r#"{"error":"internal error"}"#),
        };
        for stream in streams {
            if done.get(stream.name()).copied() != Some(active) {
                complete_by_stream.insert(stream.name(), false);
            }
        }
    }
    let all_complete = has_shards && complete_by_stream.values().all(|&c| c);
    let rewrap: Vec<_> = streams
        .iter()
        .map(|stream| {
            let name = stream.name();
            serde_json::json!({ "stream": name, "complete": complete_by_stream[name] })
        })
        .collect();
    let keys_json: Vec<_> = keys
        .into_iter()
        .map(|(id, (hash, is_active, count))| {
            serde_json::json!({
                "id": id,
                "key_hash": key_hash_for_status(&hash),
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
///
/// # Errors
/// [`Error::InvalidArgument`] when a segment is present but unparseable — on the virtual-host path
/// that is the key (the whole request path), which suffered the same lossy `.ok()` collapse as
/// [`route_path`] and let an over-long key re-route to the bucket handler (audit 2026-07). A Host
/// whose leading label is not a valid bucket name is *not* an error: it simply is not a Cairn
/// virtual-host request and falls through to path-style.
pub(crate) fn route_request(
    s3_domain: Option<&str>,
    host: &str,
    raw_path: &str,
) -> Result<(Option<BucketName>, Option<ObjectKey>), Error> {
    if let Some(domain) = s3_domain {
        if let Some(bucket) = vhost_bucket(host, domain) {
            if let Ok(b) = BucketName::parse(&bucket) {
                let key = raw_path.strip_prefix('/').unwrap_or(raw_path);
                let key = match (!key.is_empty()).then_some(key) {
                    Some(k) => Some(parse_key(&pct_decode(k))?),
                    None => None,
                };
                return Ok((Some(b), key));
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
///
/// A segment that is **present but invalid** is a third state, distinct from *absent*, and is
/// returned as an error rather than collapsed into `None`: `None` means "the request names no
/// bucket/key", which is what steers `dispatch` to the bucket-level (or root-level) operation.
/// Collapsing an invalid segment let `DELETE /b/<1025-byte-key>` re-route to **DeleteBucket** and
/// destroy the bucket, and `GET /UPPERCASE/k` re-route to ListBuckets (audit 2026-07).
///
/// # Errors
/// [`Error::InvalidArgument`] (400 `InvalidArgument`) when the bucket segment is not a valid
/// bucket name or the key segment is not a valid object key.
pub(crate) fn route_path(raw_path: &str) -> Result<(Option<BucketName>, Option<ObjectKey>), Error> {
    let p = raw_path.strip_prefix('/').unwrap_or(raw_path);
    if p.is_empty() {
        return Ok((None, None));
    }
    let (bucket_seg, key_rest) = match p.split_once('/') {
        Some((b, k)) => (b, Some(k)),
        None => (p, None),
    };
    let bucket = BucketName::parse(&pct_decode(bucket_seg)).map_err(invalid_bucket)?;
    // An EMPTY key segment (`/bucket/`) is genuinely absent — S3 treats it as a bucket-level
    // request — so it is filtered out before parsing rather than rejected.
    let key = match key_rest.filter(|k| !k.is_empty()) {
        Some(k) => Some(parse_key(&pct_decode(k))?),
        None => None,
    };
    Ok((Some(bucket), key))
}

/// Map a rejected bucket segment onto the wire error. AWS answers `InvalidBucketName`/400 here;
/// Cairn's error tree folds every `InvalidName` into `InvalidArgument`, which is also 400
/// (ARCH 25.2), so the status matches and only the code string differs.
fn invalid_bucket(_: InvalidName) -> Error {
    Error::InvalidArgument("invalid bucket name".to_owned())
}

/// Parse an object key, distinguishing the over-long case (AWS: `KeyTooLongError`/400) in the
/// message so an operator can tell a 1 MiB path from a control character.
fn parse_key(decoded: &str) -> Result<ObjectKey, Error> {
    ObjectKey::parse(decoded).map_err(|e| match e {
        InvalidName::Length => {
            Error::InvalidArgument(format!("object key exceeds the {MAX_KEY_LEN} byte maximum"))
        }
        _ => Error::InvalidArgument("invalid object key".to_owned()),
    })
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

/// Bytes we are willing to drain from an unconsumed request body after an early error so the client
/// can finish sending and reliably receive the response (issue #5). Past this we stop reading and
/// close the connection instead — a rejected endpoint must not be forced to read an unbounded
/// upload. Sized to cover a typical rejected `UploadPart` while staying bounded (it matches the
/// management-API body cap above).
const EARLY_ERROR_DRAIN_CAP: usize = 8 * 1024 * 1024;

/// The request's incoming body, kept reachable after it is handed to the S3 service so an early
/// error can drain the bytes the service left unread (issue #5). `ended` records that the body has
/// reached EOF — nothing remains to drain and the connection is safe to keep alive.
struct SharedIncoming {
    incoming: Incoming,
    ended: bool,
}

/// Build the [`cairn_types::BodyStream`] handed to the S3 service from a shared incoming body,
/// preserving the data-frame-only contract of the old direct adapter (trailer frames are skipped).
/// The shared handle lets [`finish_body_hygiene`] drain any bytes the service leaves unread.
fn shared_body_stream(
    shared: std::sync::Arc<tokio::sync::Mutex<SharedIncoming>>,
) -> cairn_types::BodyStream {
    Box::pin(futures_util::stream::unfold(shared, |shared| async move {
        let mut guard = shared.lock().await;
        if guard.ended {
            return None;
        }
        loop {
            match guard.incoming.frame().await {
                // A non-data (trailer) frame falls through and keeps reading, matching the prior
                // data-only stream so downstream chunk decoding is unchanged.
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        drop(guard);
                        return Some((Ok(data), shared));
                    }
                }
                Some(Err(e)) => {
                    guard.ended = true;
                    drop(guard);
                    return Some((Err(BodyError::Transport(e.to_string())), shared));
                }
                None => {
                    guard.ended = true;
                    return None;
                }
            }
        }
    }))
}

/// Whether the request declares a payload body: a positive `Content-Length` or a chunked
/// `Transfer-Encoding`. Only a body-bearing request can poison a pooled keep-alive connection when
/// an early error leaves its bytes unread (issue #5); bodyless `GET`/`HEAD`/`DELETE` cannot.
fn request_has_body(headers: &[(String, String)]) -> bool {
    let mut chunked = false;
    for (k, v) in headers {
        match k.as_str() {
            "content-length" => {
                if v.trim().parse::<u64>().is_ok_and(|n| n > 0) {
                    return true;
                }
            }
            "transfer-encoding" if v.to_ascii_lowercase().contains("chunked") => {
                chunked = true;
            }
            _ => {}
        }
    }
    chunked
}

/// Read and discard up to `cap` bytes of `body`, returning whether it was fully drained (reached
/// EOF or the peer hung up) rather than cut off at the cap. Bounded so a rejected endpoint cannot be
/// forced to read an unbounded upload. Generic over the body type so it is unit-testable without a
/// live socket (a real `Incoming` cannot be constructed in-process).
async fn drain_frames<B>(body: &mut B, cap: usize) -> bool
where
    B: hyper::body::Body + Unpin,
{
    let mut drained: usize = 0;
    while drained <= cap {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    drained = drained.saturating_add(data.remaining());
                }
            }
            // The peer hung up or the transport failed mid-drain: nothing more will arrive, so no
            // leftover bytes can poison the connection.
            Some(Err(_)) => return true,
            None => return true,
        }
    }
    false
}

/// Ensure `close`-or-drain hygiene for an early error on a request whose body is still fully unread
/// (owned here, e.g. auth-denied or routing-rejected). Drains a bounded prefix so the client can
/// finish sending and receive `response`; if the body outruns the cap, marks the connection to close.
async fn drain_or_close(
    mut response: Response<ResponseBody>,
    body_bearing: bool,
    mut body: Incoming,
) -> Response<ResponseBody> {
    if body_bearing && !drain_frames(&mut body, EARLY_ERROR_DRAIN_CAP).await {
        set_connection_close(&mut response);
    }
    response
}

/// Ensure `close`-or-drain hygiene after the S3 service returned, for the body it may have left
/// unread. A fully-consumed body (`ended`, or draining to EOF immediately) keeps keep-alive; a body
/// still in flight past the drain cap forces the connection closed so it is not mis-framed (issue #5).
async fn finish_body_hygiene(
    mut response: Response<ResponseBody>,
    body_bearing: bool,
    shared: &std::sync::Arc<tokio::sync::Mutex<SharedIncoming>>,
) -> Response<ResponseBody> {
    if !body_bearing {
        return response;
    }
    let mut guard = shared.lock().await;
    if guard.ended {
        return response;
    }
    let fully = drain_frames(&mut guard.incoming, EARLY_ERROR_DRAIN_CAP).await;
    guard.ended = true;
    if !fully {
        set_connection_close(&mut response);
    }
    response
}

/// Mark a response so hyper ends the HTTP/1.1 connection after it, rather than reusing a connection
/// whose framing we can no longer guarantee. Ignored by hyper on HTTP/2 (no per-connection framing).
fn set_connection_close(response: &mut Response<ResponseBody>) {
    response.headers_mut().insert(
        http::header::CONNECTION,
        http::HeaderValue::from_static("close"),
    );
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

/// Render an [`S3Response`], swapping the machine-readable error body for a human-readable HTML
/// page when — and only when — the request is a browser top-level navigation (ARCH 25).
///
/// An object store is addressed by both programs and people. A presigned link that 404s in an SDK
/// must keep returning the exact `<Error>` XML the SDK parses; the same link pasted into a browser
/// should explain itself. `error_page::wants_html` draws that line (GET + `Accept: text/html` +
/// `Sec-Fetch-Dest: document`), so every machine client — SDK, CLI, `fetch()`, conformance suite —
/// receives a byte-identical response to before.
///
/// Only a buffered `Bytes` error body is ever swapped: a `Stream` body is a partially-sent object
/// read, and a `ZeroCopy`/`Empty` body carries nothing to replace.
/// `wants_html` is decided by the caller (via [`crate::error_page::wants_html`]) while the request
/// head is still in scope — the head is moved into `S3Request` before the response exists.
fn render_negotiated(resp: S3Response, wants_html: bool, resource: &str) -> Response<ResponseBody> {
    let is_error = resp.status.is_client_error() || resp.status.is_server_error();
    if !is_error {
        return render(resp);
    }
    // The body shape of an error now depends on request headers, so every error response must say
    // so — otherwise a shared cache can serve an SDK the browser's HTML (or the reverse).
    let mut resp = resp;
    resp.headers
        .push(("vary".to_owned(), crate::error_page::VARY.to_owned()));
    if !wants_html {
        return render(resp);
    }
    let S3Body::Bytes(ref body) = resp.body else {
        return render(resp);
    };
    // Our own error document, so a scan for the element is sufficient — no XML parser needed.
    let xml = std::str::from_utf8(body).unwrap_or_default();
    let code = xml
        .split_once("<Code>")
        .and_then(|(_, rest)| rest.split_once("</Code>"))
        .map(|(c, _)| c)
        .unwrap_or_default()
        .to_owned();
    let request_id = resp
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-request-id"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default();

    html_error_response(resp.status, &code, resource, &request_id, &resp.headers)
}

/// Build the HTML error page response: the page itself plus the headers that make it safe to serve
/// attacker-influenced text (a `default-src 'none'` CSP and `nosniff`) and uncacheable.
fn html_error_response(
    status: StatusCode,
    code: &str,
    resource: &str,
    request_id: &str,
    carry: &[(String, String)],
) -> Response<ResponseBody> {
    let html = crate::error_page::render(status, code, resource, request_id);
    let mut builder = Response::builder().status(status);
    // Carry over everything except the body-describing headers we are replacing.
    for (k, v) in carry {
        if k.eq_ignore_ascii_case("content-type") || k.eq_ignore_ascii_case("content-length") {
            continue;
        }
        builder = builder.header(k, v);
    }
    builder
        .header("content-type", "text/html; charset=utf-8")
        // The page interpolates an attacker-controlled path. It is escaped, but a page that can
        // load nothing and run nothing cannot be turned into a reflected-XSS sink even if that
        // escaping ever regressed.
        .header(
            "content-security-policy",
            "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'",
        )
        .header("x-content-type-options", "nosniff")
        .header("cache-control", "no-store")
        .header("vary", crate::error_page::VARY)
        // A share token rides the URL; keep it out of any onward request this page could trigger.
        // (It has no links or subresources, but the header costs nothing and the share error branch
        // returns before the normal share headers are applied.)
        .header("referrer-policy", "no-referrer")
        .body(full_body(Bytes::from(html)))
        .unwrap_or_else(|_| render_fallback(status))
}

/// A last-resort empty response if the HTML response somehow fails to build.
fn render_fallback(status: StatusCode) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .body(full_body(Bytes::new()))
        .expect("status-only response is always valid")
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

    #[test]
    fn crypto_status_abbreviates_the_durable_full_key_identity() {
        let durable = format!("deadbeef{}", "42".repeat(28));
        assert_eq!(durable.len(), 64);
        assert_eq!(key_hash_for_status(&durable), "deadbeef");
    }

    #[test]
    fn management_namespace_matches_only_the_versioned_path_segment() {
        assert!(is_control_path("/api/v1"));
        assert!(is_control_path("/api/v1/session"));
        assert!(is_control_path("/api/v1/buckets/photos"));
        assert!(!is_control_path("/api/v10"));
        assert!(!is_control_path("/api/v1-preview"));
        assert!(!is_control_path("/photos/api/v1"));
    }

    #[test]
    fn data_listener_never_selects_control_or_console_handlers() {
        let get = Method::GET;
        assert_eq!(
            listener_route(ListenerRole::Data, &get, "/api/v1"),
            ListenerRoute::NotFound
        );
        assert_eq!(
            listener_route(ListenerRole::Data, &get, "/api/v1/buckets"),
            ListenerRoute::NotFound
        );
        // Segment boundaries remain exact: these are ordinary path-style S3 names, not API paths.
        assert_eq!(
            listener_route(ListenerRole::Data, &get, "/api/v10"),
            ListenerRoute::Data
        );
        assert_eq!(
            listener_route(ListenerRole::Data, &get, "/api/v1-preview"),
            ListenerRoute::Data
        );
        // A concrete embedded filename on this port is still data-plane routing. The data listener
        // never reads or returns the embedded console bundle.
        assert!(cairn_web::asset("favicon.svg").is_some());
        assert_eq!(
            listener_route(ListenerRole::Data, &get, "/favicon.svg"),
            ListenerRoute::Data
        );
        assert_eq!(
            listener_route(ListenerRole::Data, &get, "/share/token"),
            ListenerRoute::PublicShare
        );
    }

    #[test]
    fn control_listener_never_falls_through_to_s3_or_public_shares() {
        let get = Method::GET;
        assert_eq!(
            listener_route(ListenerRole::Control, &get, "/api/v1"),
            ListenerRoute::ControlApi
        );
        assert_eq!(
            listener_route(ListenerRole::Control, &get, "/api/v1/session"),
            ListenerRoute::ControlApi
        );
        assert_eq!(
            listener_route(ListenerRole::Control, &get, "/"),
            ListenerRoute::ConsoleAsset
        );
        assert_eq!(
            listener_route(ListenerRole::Control, &get, "/favicon.svg"),
            ListenerRoute::ConsoleAsset
        );
        for path in [
            "/bucket",
            "/bucket/key",
            "/share/token",
            "/healthz",
            "/readyz",
            "/metrics",
            "/api/v10",
        ] {
            assert_eq!(
                listener_route(ListenerRole::Control, &get, path),
                ListenerRoute::NotFound,
                "{path} must not escape the control-plane matrix"
            );
        }
        assert_eq!(
            listener_route(ListenerRole::Control, &Method::PUT, "/favicon.svg"),
            ListenerRoute::NotFound
        );
    }

    fn console_presign_query(origin: &str) -> String {
        [
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            ("X-Amz-Credential", "CAIRNTMP/example"),
            ("X-Amz-Security-Token", "opaque-token"),
            ("X-Amz-Signature", "0123456789abcdef"),
            ("X-Cairn-Console-Origin", origin),
        ]
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
    }

    #[test]
    fn console_presign_cors_requires_a_matching_signed_origin_marker() {
        let query = console_presign_query("https://console.example.test:7374");
        let headers = vec![
            (
                "origin".to_owned(),
                "https://console.example.test:7374".to_owned(),
            ),
            ("access-control-request-method".to_owned(), "PUT".to_owned()),
            (
                "access-control-request-headers".to_owned(),
                "content-type,x-amz-copy-source".to_owned(),
            ),
        ];
        let response = console_presign_preflight("/photos/a.jpg", &query, &headers)
            .expect("complete matching console presign preflight");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .unwrap(),
            "https://console.example.test:7374"
        );
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-methods")
                .unwrap(),
            "PUT"
        );

        let wrong_origin = vec![
            (
                "origin".to_owned(),
                "https://attacker.example.test".to_owned(),
            ),
            ("access-control-request-method".to_owned(), "PUT".to_owned()),
        ];
        assert!(console_presign_preflight("/photos/a.jpg", &query, &wrong_origin).is_none());
        assert!(console_actual_cors_origin(&query, &wrong_origin).is_none());
    }

    #[test]
    fn console_presign_cors_rejects_incomplete_or_non_data_requests() {
        let origin = "https://console.example.test:7374";
        let headers = vec![
            ("origin".to_owned(), origin.to_owned()),
            ("access-control-request-method".to_owned(), "GET".to_owned()),
        ];
        let incomplete = format!("X-Cairn-Console-Origin={origin}&X-Amz-Signature=fake");
        assert!(console_presign_preflight("/photos/a.jpg", &incomplete, &headers).is_none());

        let complete = console_presign_query(origin);
        assert!(console_presign_preflight("/", &complete, &headers).is_none());
        assert!(console_presign_preflight("/share/token", &complete, &headers).is_none());

        let actual = vec![("origin".to_owned(), origin.to_owned())];
        assert_eq!(
            console_actual_cors_origin(&complete, &actual).as_deref(),
            Some(origin)
        );
    }

    #[test]
    fn console_origin_is_an_origin_not_an_arbitrary_url() {
        assert_eq!(
            normalize_origin("https://console.example.test:7374/").as_deref(),
            Some("https://console.example.test:7374")
        );
        assert!(normalize_origin("javascript:alert(1)").is_none());
        assert!(normalize_origin("https://console.example.test/app").is_none());
        assert!(normalize_origin("https://console.example.test/?token=secret").is_none());
    }

    #[test]
    fn console_transfer_policy_is_bucket_scoped() {
        let policy: serde_json::Value =
            serde_json::from_str(&console_session_policy("photos")).expect("valid policy JSON");
        let resources = policy["Statement"][0]["Resource"]
            .as_array()
            .expect("resource array");
        assert_eq!(
            resources,
            &[
                serde_json::Value::String("arn:aws:s3:::photos".to_owned()),
                serde_json::Value::String("arn:aws:s3:::photos/*".to_owned()),
            ]
        );
    }

    #[test]
    fn presign_rejects_browser_normalized_dot_segments_including_encoded_aliases() {
        for key in [
            ".",
            "..",
            "a/./b",
            "a/../b",
            "a/%2e/b",
            "a/%2E%2e/b",
            "a/.%2e/b",
            "a/%252E/b",
            "a/%252e%252e/b",
        ] {
            assert!(
                presign_key_has_browser_dot_segment(key),
                "{key:?} can normalize to a different browser path"
            );
        }
        for key in ["", "...", "a/.hidden/b", "a/name../b", "a/%2f/b", "a/%25/b"] {
            assert!(
                !presign_key_has_browser_dot_segment(key),
                "{key:?} is not a pure dot segment"
            );
        }
    }

    #[tokio::test]
    async fn console_transfer_boundary_preserves_parent_denies_and_fails_closed() {
        use cairn_types::traits::MetadataStore;

        let store = std::sync::Arc::new(cairn_types::testing::InMemoryMetadataStore::new());
        let meta: std::sync::Arc<dyn MetadataStore> = store.clone();
        meta.submit(Mutation::SetUserPolicy {
            user_id: UserId("admin".to_owned()),
            policy: Some(
                r#"{"Version":"2012-10-17","Statement":[{
                    "Effect":"Deny",
                    "Action":"s3:DeleteObject",
                    "Resource":"arn:aws:s3:::photos/private/*"
                }]}"#
                    .to_owned(),
            ),
        })
        .await
        .expect("store parent policy");

        let policy = crate::sts::administrator_bounded_policy(
            &meta,
            &UserId("admin".to_owned()),
            &console_session_policy("photos"),
        )
        .await
        .expect("compose console boundary");
        let document: serde_json::Value =
            serde_json::from_str(&policy).expect("combined policy JSON");
        let statements = document["Statement"].as_array().expect("statement array");
        assert!(
            statements.iter().any(|statement| {
                statement["Effect"] == "Deny"
                    && statement["Action"] == "s3:DeleteObject"
                    && statement["Resource"] == "arn:aws:s3:::photos/private/*"
            }),
            "the durable console session policy must retain the parent's explicit Deny"
        );

        store.set_fail_user_policy_reads(true);
        assert!(
            crate::sts::administrator_bounded_policy(
                &meta,
                &UserId("admin".to_owned()),
                &console_session_policy("photos"),
            )
            .await
            .is_err(),
            "a parent-policy read failure must abort console session minting"
        );
    }

    #[test]
    fn console_cors_metadata_preserves_existing_vary_and_expose_contracts() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            "vary",
            http::HeaderValue::from_static("accept, sec-fetch-dest"),
        );
        headers.insert(
            "access-control-expose-headers",
            http::HeaderValue::from_static("x-custom"),
        );
        merge_csv_header(&mut headers, "vary", "Origin, Accept");
        merge_csv_header(
            &mut headers,
            "access-control-expose-headers",
            "ETag, x-custom",
        );
        assert_eq!(
            headers.get("vary").unwrap(),
            "accept, sec-fetch-dest, Origin"
        );
        assert_eq!(
            headers.get("access-control-expose-headers").unwrap(),
            "x-custom, ETag"
        );
    }

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

    /// The session cookie round-trips a Bearer token through base64url, and a missing/garbled cookie
    /// yields `None` (never a panic) so a hostile `Cookie:` header degrades to "unauthenticated".
    #[test]
    fn session_cookie_round_trips_and_rejects_garbage() {
        let token = "cairn_abc123.s3cr3t-value_with.dots";
        let set = set_session_cookie(token, true);
        assert!(set.contains("HttpOnly"));
        assert!(set.contains("SameSite=Strict"));
        assert!(set.contains("Secure"));
        assert!(set.contains("Path=/"));
        // Pull the cookie value back out of the Set-Cookie string and decode it.
        let cookie_val = set
            .split(';')
            .next()
            .unwrap()
            .strip_prefix("cairn_session=")
            .unwrap();
        let headers = vec![("cookie".to_owned(), format!("cairn_session={cookie_val}"))];
        assert_eq!(session_cookie_token(&headers).as_deref(), Some(token));

        // Other cookies alongside ours are ignored; ours is found regardless of position/spacing.
        let headers = vec![(
            "cookie".to_owned(),
            format!("theme=dark; cairn_session={cookie_val} ; other=1"),
        )];
        assert_eq!(session_cookie_token(&headers).as_deref(), Some(token));

        // No cookie header, wrong name, and non-base64 garbage all yield None (no panic).
        assert_eq!(session_cookie_token(&[]), None);
        assert_eq!(
            session_cookie_token(&[("cookie".to_owned(), "theme=dark".to_owned())]),
            None
        );
        assert_eq!(
            session_cookie_token(&[("cookie".to_owned(), "cairn_session=!!!not_b64".to_owned())]),
            None
        );
    }

    /// `Secure` is omitted on a plaintext transport (so a dev HTTP listener can still store the
    /// cookie) and the clear variant expires it immediately.
    #[test]
    fn session_cookie_secure_flag_and_clear() {
        assert!(!set_session_cookie("t", false).contains("Secure"));
        assert!(set_session_cookie("t", true).contains("Secure"));
        let cleared = clear_session_cookie(false);
        assert!(cleared.contains("Max-Age=0"));
        assert!(cleared.starts_with("cairn_session=;"));
    }

    #[test]
    fn control_cookie_secure_flag_uses_only_authenticated_transport_provenance() {
        let no_proxies = crate::proxy::TrustedProxies::default();
        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(
            !control_cookie_is_secure(
                RequestTransport::new(loopback, false, &no_proxies, ListenerRole::Control),
                &[],
            ),
            "direct loopback plaintext remains usable for explicit local development"
        );
        assert!(
            control_cookie_is_secure(
                RequestTransport::new(
                    "203.0.113.9".parse().unwrap(),
                    true,
                    &no_proxies,
                    ListenerRole::Control,
                ),
                &[("x-forwarded-proto".to_owned(), "http".to_owned())],
            ),
            "direct TLS is authoritative"
        );

        let trusted = crate::proxy::TrustedProxies::parse(Some("10.0.0.9")).unwrap();
        let forwarded_https = vec![(
            "forwarded".to_owned(),
            "for=203.0.113.7;proto=https".to_owned(),
        )];
        assert!(control_cookie_is_secure(
            RequestTransport::new(
                "10.0.0.9".parse().unwrap(),
                false,
                &trusted,
                ListenerRole::Control,
            ),
            &forwarded_https,
        ));
        assert!(
            !control_cookie_is_secure(
                RequestTransport::new(
                    "203.0.113.9".parse().unwrap(),
                    false,
                    &trusted,
                    ListenerRole::Control,
                ),
                &forwarded_https,
            ),
            "an untrusted immediate peer cannot assert HTTPS"
        );
        assert!(
            !control_cookie_is_secure(
                RequestTransport::new(
                    "10.0.0.9".parse().unwrap(),
                    true,
                    &trusted,
                    ListenerRole::Data,
                ),
                &forwarded_https,
            ),
            "the data listener never owns the administrator cookie"
        );
    }

    #[test]
    fn adapter_source_provenance_never_substitutes_a_trusted_proxy() {
        let trusted = crate::proxy::TrustedProxies::parse(Some("10.0.0.9")).unwrap();
        let transport = RequestTransport::new(
            "10.0.0.9".parse().unwrap(),
            false,
            &trusted,
            ListenerRole::Data,
        );
        assert_eq!(
            request_client_source(transport, &[]),
            ClientSource::Unavailable
        );
        assert_eq!(
            request_client_source(
                transport,
                &[
                    ("forwarded".to_owned(), "for=203.0.113.7".to_owned()),
                    ("x-forwarded-for".to_owned(), "203.0.113.7".to_owned(),),
                ],
            ),
            ClientSource::Forwarded("203.0.113.7".parse().unwrap())
        );
    }

    /// Virtual-host addressing: with `CAIRN_S3_DOMAIN` set and a `<bucket>.<domain>` Host, the
    /// bucket comes from the Host and the entire path is the key (ARCH 13.1).
    #[test]
    fn route_request_virtual_host_takes_bucket_from_host() {
        let (b, k) = route_request(
            Some("s3.example.com"),
            "photos.s3.example.com",
            "/a/b/c.jpg",
        )
        .expect("valid route");
        assert_eq!(b.unwrap().as_str(), "photos");
        assert_eq!(k.unwrap().as_str(), "a/b/c.jpg");

        // Port on the Host is stripped; matching is case-insensitive.
        let (b, _) = route_request(Some("s3.example.com"), "Photos.S3.Example.com:9000", "/x")
            .expect("valid route");
        assert_eq!(b.unwrap().as_str(), "photos");

        // A bucket-only request (path is just "/") yields the bucket with no key.
        let (b, k) =
            route_request(Some("s3.example.com"), "logs.s3.example.com", "/").expect("valid route");
        assert_eq!(b.unwrap().as_str(), "logs");
        assert!(k.is_none());
    }

    /// A bare domain Host (no bucket label) or a non-matching Host falls back to path-style routing,
    /// and an unset domain is always path-style.
    #[test]
    fn route_request_falls_back_to_path_style() {
        // Bare domain (no leading bucket label) -> path-style: `/bucket/key`.
        let (b, k) = route_request(Some("s3.example.com"), "s3.example.com", "/mybucket/obj")
            .expect("valid route");
        assert_eq!(b.unwrap().as_str(), "mybucket");
        assert_eq!(k.unwrap().as_str(), "obj");

        // Multi-label host under the domain is not a vhost bucket -> path-style.
        let (b, _) = route_request(
            Some("s3.example.com"),
            "a.b.s3.example.com",
            "/mybucket/obj",
        )
        .expect("valid route");
        assert_eq!(b.unwrap().as_str(), "mybucket");

        // A Host not under the domain -> path-style.
        let (b, _) = route_request(Some("s3.example.com"), "other.host.net", "/mybucket/obj")
            .expect("valid route");
        assert_eq!(b.unwrap().as_str(), "mybucket");

        // No domain configured -> always path-style even for a domain-shaped Host.
        let (b, k) =
            route_request(None, "photos.s3.example.com", "/mybucket/obj").expect("valid route");
        assert_eq!(b.unwrap().as_str(), "mybucket");
        assert_eq!(k.unwrap().as_str(), "obj");
    }

    /// **The critical routing-fallthrough regression** (audit 2026-07). A path segment that is
    /// *present but invalid* must be rejected, never collapsed into `None`: `None` is how
    /// `dispatch` decides an operation is bucket- or root-level, so an unparseable key silently
    /// demoted an object request to a **bucket** request. Verified live before the fix:
    /// `DELETE /b/<1025-byte-key>` executed DeleteBucket and destroyed an empty bucket (and
    /// answered `BucketNotEmpty` 409 on a non-empty one — proof it reached `delete_bucket`), while
    /// the same GET returned 200 with a `ListBucketResult` body.
    #[test]
    fn route_path_rejects_invalid_segments_instead_of_dropping_them() {
        // Over-long key: the exact input that reached DeleteBucket. It must be an error, and the
        // wire mapping must be 400 `InvalidArgument` — not a bucket-level route.
        let err = route_path(&format!("/mybucket/{}", "a".repeat(MAX_KEY_LEN + 1)))
            .expect_err("over-long key must be rejected");
        assert!(matches!(err, Error::InvalidArgument(_)));
        assert_eq!(
            cairn_protocol::error_map::map(&err),
            (StatusCode::BAD_REQUEST, "InvalidArgument")
        );

        // The boundary did not move: a key of exactly MAX_KEY_LEN is still a valid object route.
        let (b, k) = route_path(&format!("/mybucket/{}", "a".repeat(MAX_KEY_LEN)))
            .expect("a key at the limit is valid");
        assert!(b.is_some() && k.is_some());

        // An invalid BUCKET segment must not become `None` either — that turned `GET /UPPERCASE/obj`
        // into ListBuckets, i.e. cross-bucket enumeration.
        for path in ["/UPPERCASE/obj", "/ab", "//obj"] {
            let err = route_path(path).expect_err("invalid bucket segment must be rejected");
            assert!(matches!(err, Error::InvalidArgument(_)), "path {path}");
        }

        // The charset branch of the key check, not just the length branch: a NUL is XML-illegal.
        let err = route_path("/mybucket/a\u{0}b").expect_err("control character must be rejected");
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    /// The other half of the contract: rejecting invalid segments must not start rejecting *absent*
    /// ones. `/bucket/` is a bucket-level request in S3 (empty key segment = no key), and a bare
    /// `/` is the root ListBuckets request — both must still route, or the fix breaks the API.
    #[test]
    fn route_path_preserves_absent_segments() {
        for path in ["/", ""] {
            let (b, k) = route_path(path).expect("root is a valid route");
            assert!(b.is_none() && k.is_none(), "path {path:?}");
        }

        // Bucket-level: no key, both with and without the trailing slash.
        for path in ["/mybucket", "/mybucket/"] {
            let (b, k) = route_path(path).expect("bucket-level is a valid route");
            assert_eq!(b.unwrap().as_str(), "mybucket");
            assert!(k.is_none(), "path {path:?}");
        }

        let (b, k) = route_path("/mybucket/a/b/c.jpg").expect("object is a valid route");
        assert_eq!(b.unwrap().as_str(), "mybucket");
        assert_eq!(k.unwrap().as_str(), "a/b/c.jpg");
    }

    /// The virtual-host branch had the same lossy `.ok()` collapse as [`route_path`], so the
    /// escalation reproduced with `Host: <bucket>.<domain>` and an over-long path — the key
    /// vanished and the request became a bucket operation.
    #[test]
    fn route_request_vhost_rejects_invalid_key() {
        let err = route_request(
            Some("s3.example.com"),
            "photos.s3.example.com",
            &format!("/{}", "a".repeat(MAX_KEY_LEN + 1)),
        )
        .expect_err("over-long vhost key must be rejected");
        assert!(matches!(err, Error::InvalidArgument(_)));

        // A genuine bucket-level vhost request (path "/") still carries no key.
        let (b, k) =
            route_request(Some("s3.example.com"), "photos.s3.example.com", "/").expect("valid");
        assert_eq!(b.unwrap().as_str(), "photos");
        assert!(k.is_none());
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

    /// A body under the drain cap is reported fully drained, so the early-error path leaves the
    /// keep-alive connection intact rather than closing it (issue #5). This is the mechanism that
    /// lets a rejected `UploadPart` still deliver its 404 and keep the pooled connection usable.
    #[tokio::test]
    async fn drain_frames_reports_full_drain_within_cap() {
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> = vec![
            Ok(Frame::data(Bytes::from_static(b"hello "))),
            Ok(Frame::data(Bytes::from_static(b"world"))),
        ];
        let mut body = StreamBody::new(stream::iter(frames));
        assert!(
            drain_frames(&mut body, 1024).await,
            "a body under the cap must report a full drain"
        );
    }

    /// A body larger than the cap is reported *not* fully drained: the caller must then close the
    /// connection rather than read an unbounded upload from a rejected endpoint (issue #5).
    #[tokio::test]
    async fn drain_frames_stops_and_reports_partial_past_cap() {
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> =
            vec![Ok(Frame::data(Bytes::from(vec![b'z'; 20])))];
        let mut body = StreamBody::new(stream::iter(frames));
        assert!(
            !drain_frames(&mut body, 4).await,
            "a body past the cap must be reported not-fully-drained so the caller closes"
        );
    }

    /// Trailer frames (e.g. `x-amz-checksum-*` on an aws-chunked upload) carry no payload bytes:
    /// they must not count toward the drain budget, and the body still drains fully.
    #[tokio::test]
    async fn drain_frames_skips_trailer_frames() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert(
            "x-amz-checksum-crc32",
            http::HeaderValue::from_static("AAAAAA=="),
        );
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> = vec![
            Ok(Frame::data(Bytes::from_static(b"data"))),
            Ok(Frame::trailers(trailers)),
        ];
        let mut body = StreamBody::new(stream::iter(frames));
        assert!(
            drain_frames(&mut body, 1024).await,
            "a data+trailer body drains fully; the trailer is not payload"
        );
    }

    /// The scoping predicate for the fix: only a positive `Content-Length` or a chunked
    /// `Transfer-Encoding` is body-bearing. A bodyless `GET`/`HEAD`/`DELETE` (and `Content-Length: 0`)
    /// must NOT trigger the drain/close path, so keep-alive is never regressed for them (issue #5).
    #[test]
    fn request_has_body_classifies_body_bearing_requests() {
        let cl = |v: &str| vec![("content-length".to_owned(), v.to_owned())];
        assert!(
            request_has_body(&cl("5")),
            "a positive length is body-bearing"
        );
        assert!(!request_has_body(&cl("0")), "content-length: 0 is bodyless");
        assert!(
            !request_has_body(&cl("")),
            "an unparseable length is not counted as a body"
        );
        assert!(
            request_has_body(&[("transfer-encoding".to_owned(), "Chunked".to_owned())]),
            "a chunked transfer-encoding is body-bearing (case-insensitive)"
        );
        assert!(
            !request_has_body(&[("host".to_owned(), "example".to_owned())]),
            "a request with no length and no transfer-encoding has no body"
        );
    }
}
