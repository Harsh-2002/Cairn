//! The remote-administration CLI client (ARCH 24.2).
//!
//! This module is a thin HTTP client over the running server's two surfaces:
//!
//!  * the **management JSON API** under `/api/v1` (ARCH 22) — admin-gated, authenticated with a
//!    first-party Bearer token `Authorization: Bearer <access>.<secret>` (ARCH 14.4); and
//!  * the **S3 data plane** at `/{bucket}/{key}` for object get/put/delete, which accepts the very
//!    same Bearer token (the authenticator chain treats `Bearer …` uniformly across both surfaces),
//!    so object operations need no SigV4 signing here.
//!
//! It carries no privileged logic of its own: every command maps to one (occasionally two) HTTP
//! calls, prints a concise human summary by default or the raw JSON under `--json`, and exits
//! non-zero on any non-2xx response, surfacing the management API's `{error, request_id}` envelope.
//!
//! The wire response shapes are mirrored here as minimal `Deserialize` structs rather than reused
//! from `cairn_control::wire`, whose `wire` module is private to that crate; mirroring keeps this
//! client decoupled and avoids widening another crate's surface.

use clap::{Args, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;

use bytes::Bytes;
use http::{Method, Request};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;

/// The default management/data endpoint: the web-UI listener, where `/api/v1` and the S3 data
/// plane are both served (the two-listener model; UI on 7374). Overridable via `--endpoint` /
/// `CAIRN_ENDPOINT`.
pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7374";

// ---------------------------------------------------------------------------------------
// Subcommand tree
// ---------------------------------------------------------------------------------------

/// Connection + output options shared by every remote subcommand. Each is sourced from a flag or,
/// when absent, the corresponding `CAIRN_*` environment variable.
#[derive(Debug, Clone, Args)]
pub struct RemoteOpts {
    /// The server endpoint base URL (the web-UI listener that serves `/api/v1` and S3).
    #[arg(long, env = "CAIRN_ENDPOINT", default_value = DEFAULT_ENDPOINT, global = true)]
    pub endpoint: String,
    /// The Bearer access-key id (the part before the dot in the token).
    #[arg(long, env = "CAIRN_ACCESS_KEY", global = true)]
    pub access_key: Option<String>,
    /// The Bearer secret (the part after the dot in the token).
    #[arg(long, env = "CAIRN_SECRET_KEY", global = true)]
    pub secret_key: Option<String>,
    /// Emit machine-readable JSON instead of the concise human summary.
    #[arg(long, global = true)]
    pub json: bool,
}

/// The remote-administration command groups.
#[derive(Debug, Subcommand)]
pub enum RemoteCommand {
    /// Bucket operations.
    Bucket {
        #[command(subcommand)]
        cmd: BucketCmd,
    },
    /// User operations.
    User {
        #[command(subcommand)]
        cmd: UserCmd,
    },
    /// Replication operations.
    Replication {
        #[command(subcommand)]
        cmd: ReplicationCmd,
    },
    /// Object operations (over the S3 data plane, authenticated with the same Bearer token).
    Object {
        #[command(subcommand)]
        cmd: ObjectCmd,
    },
    /// Object sharing: persistent share links + interoperable presigned URLs (ARCH 15.8).
    Share {
        #[command(subcommand)]
        cmd: ShareCmd,
    },
    /// Import buckets + objects from another S3-compatible store into this node (ARCH 27.7).
    Import {
        #[command(subcommand)]
        cmd: ImportCmd,
    },
    /// Print the store overview (`GET /api/v1/overview`).
    Overview,
}

#[derive(Debug, Subcommand)]
pub enum ImportCmd {
    /// Start an import job from a remote S3 source, then tail its progress.
    Run {
        /// The source S3 endpoint base URL.
        #[arg(long)]
        source_endpoint: String,
        /// The SigV4 signing region for the source.
        #[arg(long, default_value = "us-east-1")]
        region: String,
        /// The source admin access-key id.
        #[arg(long)]
        source_key: String,
        /// The source admin secret (or set `CAIRN_IMPORT_SOURCE_SECRET`).
        #[arg(long, env = "CAIRN_IMPORT_SOURCE_SECRET")]
        source_secret: String,
        /// A bucket to import, as `SRC` or `SRC:DEST` (repeatable). Omit to import every source bucket.
        #[arg(long = "bucket", value_name = "SRC[:DEST]")]
        buckets: Vec<String>,
        /// Object-copy concurrency (defaults to the server's setting).
        #[arg(long)]
        workers: Option<u32>,
        /// Path to a PEM CA bundle to trust for an `https` source.
        #[arg(long)]
        ca_cert: Option<std::path::PathBuf>,
        /// Accept any TLS certificate from the source (testing only).
        #[arg(long)]
        insecure_skip_verify: bool,
        /// Print the request that would be sent without creating a job.
        #[arg(long)]
        dry_run: bool,
        /// Create the job and return immediately instead of tailing progress.
        #[arg(long)]
        detach: bool,
    },
    /// List import jobs.
    Ls,
    /// Show one import job's status.
    Status {
        /// The job id.
        id: String,
    },
    /// Cancel an import job.
    Cancel {
        /// The job id.
        id: String,
    },
    /// Resume a failed or cancelled import job.
    Resume {
        /// The job id.
        id: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum ShareCmd {
    /// Create a persistent, revocable share link for an object.
    Create {
        /// The bucket.
        bucket: String,
        /// The object key.
        key: String,
        /// Validity, e.g. `24h`, `7d`, `3600s`. Defaults to 24h; use `--forever` for no expiry.
        #[arg(long)]
        expires: Option<String>,
        /// Never expire (works until revoked).
        #[arg(long, conflicts_with = "expires")]
        forever: bool,
        /// Force download instead of viewing inline.
        #[arg(long)]
        download: bool,
        /// Download filename (with `--download`).
        #[arg(long)]
        filename: Option<String>,
        /// Pin to a specific version id (default: always the current version).
        #[arg(long)]
        version: Option<String>,
    },
    /// Mint an interoperable S3 presigned URL (download by default; `--upload` for PUT).
    Presign {
        /// The bucket.
        bucket: String,
        /// The object key.
        key: String,
        /// Validity, e.g. `1h`, `7d` (max 7 days).
        #[arg(long)]
        expires: String,
        /// Make an upload (PUT) link instead of a download (GET) link.
        #[arg(long)]
        upload: bool,
        /// Pin the content type for an upload link.
        #[arg(long)]
        content_type: Option<String>,
    },
    /// List a bucket's shares, or one object's.
    List {
        /// The bucket.
        bucket: String,
        /// Restrict to one object key.
        #[arg(long)]
        key: Option<String>,
    },
    /// Revoke a share by its token.
    Revoke {
        /// The bucket.
        bucket: String,
        /// The share token.
        token: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum BucketCmd {
    /// List buckets.
    Ls,
    /// Create a bucket.
    Create {
        /// The bucket name.
        name: String,
    },
    /// Delete a bucket (force-empties it first, then removes it).
    Rm {
        /// The bucket name.
        name: String,
    },
    /// Force-empty a bucket. The management API's delete both empties and removes the bucket, so
    /// this is an alias for `rm` (there is no empty-but-keep endpoint).
    ForceEmpty {
        /// The bucket name.
        name: String,
    },
    /// Bucket configuration.
    Config {
        #[command(subcommand)]
        cmd: BucketConfigCmd,
    },
}

#[derive(Debug, Subcommand)]
pub enum BucketConfigCmd {
    /// Read a configuration aspect (one of: policy, cors, lifecycle, replication, tagging).
    Get {
        /// The bucket name.
        name: String,
        /// The aspect to read.
        aspect: String,
    },
    /// Set a configuration aspect from a JSON file body.
    Set {
        /// The bucket name.
        name: String,
        /// The aspect to write.
        aspect: String,
        /// The JSON document to send as the aspect body.
        #[arg(long)]
        file: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
pub enum UserCmd {
    /// List users.
    Ls,
    /// Create a user and print its one-time credentials.
    Create {
        /// The display name.
        name: String,
        /// Attach a canned replication identity policy scoped to this destination bucket.
        #[arg(long)]
        replication_policy_bucket: Option<String>,
    },
    /// Rotate a user's Bearer credentials, printing the fresh secret once.
    Rotate {
        /// The user id.
        id: String,
    },
    /// Permanently delete a user, revoking all of its access immediately. Refused for the root
    /// administrator, the last administrator, yourself, and a user that still owns buckets.
    Rm {
        /// The user id.
        id: String,
    },
    /// Set or clear a user's byte quota.
    Quota {
        /// The user id.
        id: String,
        /// The new quota in bytes, or the literal `none` to remove the limit.
        value: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum ReplicationCmd {
    /// Replication target operations.
    Target {
        #[command(subcommand)]
        cmd: ReplicationTargetCmd,
    },
    /// Show per-bucket replication status.
    Status {
        /// The source bucket.
        bucket: String,
    },
    /// Requeue this bucket's failed replication entries.
    Retry {
        /// The source bucket.
        bucket: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum ReplicationTargetCmd {
    /// Add a replication target to a bucket.
    Add {
        /// The source bucket.
        bucket: String,
        /// The destination endpoint base URL (distinct from the connection `--endpoint`).
        #[arg(long = "target-endpoint")]
        endpoint: String,
        /// The SigV4 signing region for the destination.
        #[arg(long)]
        region: String,
        /// The destination bucket.
        #[arg(long)]
        dest_bucket: String,
        /// The destination access-key id.
        #[arg(long)]
        access_key: String,
        /// The destination secret access key.
        #[arg(long)]
        secret: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum ObjectCmd {
    /// List objects in a bucket.
    Ls {
        /// The bucket.
        bucket: String,
        /// Restrict to keys under this prefix.
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Download an object to stdout or a file.
    Get {
        /// The bucket.
        bucket: String,
        /// The object key.
        key: String,
        /// Write to this file instead of stdout.
        #[arg(short = 'o', long)]
        output: Option<PathBuf>,
    },
    /// Upload a file as an object.
    Put {
        /// The bucket.
        bucket: String,
        /// The object key.
        key: String,
        /// The local file to upload.
        #[arg(long)]
        file: PathBuf,
    },
    /// Delete an object.
    Rm {
        /// The bucket.
        bucket: String,
        /// The object key.
        key: String,
    },
}

// ---------------------------------------------------------------------------------------
// Wire DTOs (minimal mirrors of cairn_control::wire response shapes)
// ---------------------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ErrorResp {
    error: String,
    #[serde(default)]
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct BucketListEntry {
    name: String,
    versioning: String,
    #[serde(default)]
    owner_id: String,
}

#[derive(Debug, Deserialize)]
struct BucketListResp {
    buckets: Vec<BucketListEntry>,
}

#[derive(Debug, Deserialize)]
struct CreateBucketResp {
    name: String,
}

#[derive(Debug, Deserialize)]
struct ObjectEntry {
    key: String,
    size: u64,
    #[serde(default)]
    etag: String,
}

#[derive(Debug, Deserialize)]
struct ObjectListResp {
    objects: Vec<ObjectEntry>,
    #[serde(default)]
    common_prefixes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct UserListEntry {
    id: String,
    display_name: String,
    access_key_id: String,
    role: String,
    is_active: bool,
}

#[derive(Debug, Deserialize)]
struct UserListResp {
    users: Vec<UserListEntry>,
}

#[derive(Debug, Deserialize)]
struct CreateUserResp {
    id: String,
    bearer_access_key_id: String,
    bearer_secret: String,
    s3_access_key_id: String,
    s3_secret_key: String,
}

#[derive(Debug, Deserialize)]
struct RotateCredentialsResp {
    bearer_access_key_id: String,
    bearer_secret: String,
}

#[derive(Debug, Deserialize)]
struct CreateReplicationTargetResp {
    arn: String,
}

#[derive(Debug, Deserialize)]
struct ReplicationStatusError {
    key: String,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReplicationStatusResp {
    bucket: String,
    pending: u64,
    failed: u64,
    #[serde(default)]
    recent_errors: Vec<ReplicationStatusError>,
}

#[derive(Debug, Deserialize)]
struct ReplicationRetryResp {
    requeued: bool,
    failed_observed: u64,
}

#[derive(Debug, Deserialize)]
struct OverviewResp {
    buckets: u64,
    objects: u64,
    versions: u64,
    logical_bytes: u64,
    physical_bytes: u64,
    compression_ratio: f64,
}

// ---------------------------------------------------------------------------------------
// Config resolution and URL/header construction (the unit-tested core)
// ---------------------------------------------------------------------------------------

/// The resolved client configuration: a normalized endpoint (no trailing slash), the Bearer token,
/// and the output mode. Built once from [`RemoteOpts`].
#[derive(Debug, Clone)]
struct ClientConfig {
    /// The endpoint base URL without a trailing slash.
    endpoint: String,
    /// The full Bearer token value (`<access>.<secret>`), or `None` when credentials are absent.
    token: Option<String>,
    /// Whether to emit raw JSON.
    json: bool,
}

impl ClientConfig {
    /// Resolve the options into a client config: trim the endpoint's trailing slash and, when both
    /// halves are present, join the access key and secret into the Bearer token.
    fn resolve(opts: &RemoteOpts) -> Self {
        let endpoint = opts.endpoint.trim_end_matches('/').to_owned();
        let token = bearer_token(opts.access_key.as_deref(), opts.secret_key.as_deref());
        Self {
            endpoint,
            token,
            json: opts.json,
        }
    }

    /// Build the absolute URL for a management-API subpath (which must start with `/`).
    fn api_url(&self, subpath: &str) -> String {
        format!("{}/api/v1{subpath}", self.endpoint)
    }

    /// Build the absolute URL for an S3 data-plane object at `/{bucket}/{key}`.
    fn object_url(&self, bucket: &str, key: &str) -> String {
        format!(
            "{}/{}/{}",
            self.endpoint,
            pct_encode_segment(bucket),
            pct_encode_path(key)
        )
    }
}

/// Build the Bearer token value from the two credential halves. Returns `None` unless *both* are
/// present, so an incomplete pair never produces a half-formed header.
fn bearer_token(access: Option<&str>, secret: Option<&str>) -> Option<String> {
    match (access, secret) {
        (Some(a), Some(s)) if !a.is_empty() && !s.is_empty() => Some(format!("{a}.{s}")),
        _ => None,
    }
}

/// Build the `Authorization: Bearer …` header value from a token.
fn authorization_value(token: &str) -> String {
    format!("Bearer {token}")
}

/// Percent-encode one path segment (a bucket or key component), keeping the unreserved set; `/` is
/// encoded so a single segment cannot inject a path separator.
// --- import-job response mirrors (a subset of the server DTOs; serde ignores extra fields) ---

#[derive(Debug, Deserialize)]
struct CreateImportResp {
    id: String,
}

#[derive(Debug, Deserialize)]
struct ImportJobEntry {
    id: String,
    source_endpoint: String,
    state: String,
    objects_done: u64,
    objects_total: u64,
    bytes_done: u64,
}

#[derive(Debug, Deserialize)]
struct ImportListResp {
    jobs: Vec<ImportJobEntry>,
}

#[derive(Debug, Deserialize)]
struct ImportBucketProgressWire {
    source_bucket: String,
    dest_bucket: String,
    state: String,
    objects_done: u64,
    objects_total: u64,
    last_error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ImportJobDetail {
    #[serde(flatten)]
    entry: ImportJobEntry,
    buckets: Vec<ImportBucketProgressWire>,
    last_error: Option<String>,
}

/// Parse a `--bucket` value: `SRC` or `SRC:DEST`. An empty or missing dest defaults to the source.
fn parse_bucket_map(s: &str) -> (String, String) {
    match s.split_once(':') {
        Some((src, dst)) if !dst.is_empty() => (src.to_owned(), dst.to_owned()),
        _ => {
            let src = s.trim_end_matches(':').to_owned();
            (src.clone(), src)
        }
    }
}

fn pct_encode_segment(s: &str) -> String {
    pct_encode(s, false)
}

/// Percent-encode an object key for the wire, keeping the unreserved set and `/` (the S3 data plane
/// is path-style, so embedded slashes are real key separators).
fn pct_encode_path(s: &str) -> String {
    pct_encode(s, true)
}

fn pct_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b'/' if keep_slash => out.push('/'),
            _ => {
                out.push('%');
                out.push(hex_upper(b >> 4));
                out.push(hex_upper(b & 0x0f));
            }
        }
    }
    out
}

fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

/// Map a management-API config aspect to its `GET /buckets/{name}/config` JSON field. The contract
/// returns every aspect under one document, so `config get` reads that document and projects the
/// requested field. Returns `None` for an unknown aspect.
fn config_aspect_field(aspect: &str) -> Option<&'static str> {
    match aspect {
        "policy" => Some("policy"),
        "cors" => Some("cors"),
        "lifecycle" => Some("lifecycle"),
        "tagging" => Some("tagging"),
        "replication" => Some("replication"),
        _ => None,
    }
}

/// Map a config aspect to its `PUT` management-API subpath under the bucket. Only `policy` is
/// settable through a dedicated endpoint in the current contract.
fn config_set_subpath(name: &str, aspect: &str) -> Option<String> {
    match aspect {
        "policy" => Some(format!("/buckets/{}/policy", pct_encode_segment(name))),
        _ => None,
    }
}

// ---------------------------------------------------------------------------------------
// HTTP transport
// ---------------------------------------------------------------------------------------

type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// A completed HTTP response: status plus the fully-buffered body.
struct HttpResponse {
    status: u16,
    body: Bytes,
}

/// Build the shared HTTP client. One connector serves both transports: `https_or_http()` dials
/// plaintext for `http://` and negotiates rustls for `https://`, matching the replication sink's
/// construction so the binary reuses the HTTP stack it already links.
fn build_client() -> HttpClient {
    // `webpki-roots` is the trust source enabled for `hyper-rustls` in the workspace; it bundles the
    // Mozilla root set, which is the right default for a CLI dialing a TLS endpoint. Plaintext
    // `http://` endpoints (the common loopback case) bypass TLS entirely via `https_or_http()`.
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(https)
}

/// Send one request and buffer the full response body. The Bearer header is attached when a token
/// is configured.
async fn send(
    client: &HttpClient,
    cfg: &ClientConfig,
    method: Method,
    url: &str,
    content_type: Option<&str>,
    body: Bytes,
) -> Result<HttpResponse, String> {
    let mut builder = Request::builder().method(method).uri(url);
    if let Some(token) = &cfg.token {
        builder = builder.header(http::header::AUTHORIZATION, authorization_value(token));
    }
    if let Some(ct) = content_type {
        builder = builder.header(http::header::CONTENT_TYPE, ct);
    }
    let req = builder
        .body(Full::new(body))
        .map_err(|e| format!("failed to build request: {e}"))?;
    let resp = client
        .request(req)
        .await
        .map_err(|e| format!("transport error talking to {url}: {e}"))?;
    let status = resp.status().as_u16();
    let body = resp
        .into_body()
        .collect()
        .await
        .map(|b| b.to_bytes())
        .map_err(|e| format!("failed to read response body: {e}"))?;
    Ok(HttpResponse { status, body })
}

/// Render a non-2xx management-API response as a one-line error string, preferring the structured
/// `{error, request_id}` envelope and falling back to the raw body.
fn format_api_error(resp: &HttpResponse) -> String {
    if let Ok(err) = serde_json::from_slice::<ErrorResp>(&resp.body) {
        if err.request_id.is_empty() {
            format!("server error ({}): {}", resp.status, err.error)
        } else {
            format!(
                "server error ({}): {} (request id {})",
                resp.status, err.error, err.request_id
            )
        }
    } else {
        let text = String::from_utf8_lossy(&resp.body);
        format!("server error ({}): {}", resp.status, text.trim())
    }
}

/// Decode a successful JSON body into `T`, mapping a decode failure to a printable error.
fn decode<T: for<'de> Deserialize<'de>>(resp: &HttpResponse) -> Result<T, String> {
    serde_json::from_slice(&resp.body).map_err(|e| format!("malformed server response: {e}"))
}

/// Pretty-print a JSON byte body, falling back to the raw bytes if it is not valid JSON.
fn print_json_body(body: &[u8]) {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(v) => println!(
            "{}",
            serde_json::to_string_pretty(&v)
                .unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned())
        ),
        Err(_) => println!("{}", String::from_utf8_lossy(body)),
    }
}

// ---------------------------------------------------------------------------------------
// Entry point + dispatch
// ---------------------------------------------------------------------------------------

/// Run a remote-admin command. Spins a small current-thread tokio runtime (the CLI is one-shot, so
/// a multi-thread pool would be wasteful) and drives the single async request to completion.
pub fn run(opts: &RemoteOpts, command: RemoteCommand) -> ExitCode {
    let cfg = ClientConfig::resolve(opts);
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        let client = build_client();
        match dispatch(&client, &cfg, command).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        }
    })
}

async fn dispatch(
    client: &HttpClient,
    cfg: &ClientConfig,
    command: RemoteCommand,
) -> Result<(), String> {
    match command {
        RemoteCommand::Bucket { cmd } => bucket(client, cfg, cmd).await,
        RemoteCommand::User { cmd } => user(client, cfg, cmd).await,
        RemoteCommand::Replication { cmd } => replication(client, cfg, cmd).await,
        RemoteCommand::Object { cmd } => object(client, cfg, cmd).await,
        RemoteCommand::Share { cmd } => share(client, cfg, cmd).await,
        RemoteCommand::Import { cmd } => import(client, cfg, cmd).await,
        RemoteCommand::Overview => overview(client, cfg).await,
    }
}

// ---------------------------------------------------------------------------------------
// Bucket commands
// ---------------------------------------------------------------------------------------

async fn bucket(client: &HttpClient, cfg: &ClientConfig, cmd: BucketCmd) -> Result<(), String> {
    match cmd {
        BucketCmd::Ls => {
            let resp = api_get(client, cfg, "/buckets").await?;
            if cfg.json {
                print_json_body(&resp.body);
                return Ok(());
            }
            let list: BucketListResp = decode(&resp)?;
            if list.buckets.is_empty() {
                println!("(no buckets)");
            } else {
                for b in &list.buckets {
                    println!("{}\t{}\t{}", b.name, b.versioning, b.owner_id);
                }
            }
            Ok(())
        }
        BucketCmd::Create { name } => {
            let body = serde_json::json!({ "name": name }).to_string();
            let resp = api_send(
                client,
                cfg,
                Method::POST,
                "/buckets",
                Some(Bytes::from(body)),
            )
            .await?;
            if cfg.json {
                print_json_body(&resp.body);
            } else {
                let created: CreateBucketResp = decode(&resp)?;
                println!("created bucket {}", created.name);
            }
            Ok(())
        }
        BucketCmd::Rm { name } | BucketCmd::ForceEmpty { name } => {
            let subpath = format!("/buckets/{}", pct_encode_segment(&name));
            let resp = api_send(client, cfg, Method::DELETE, &subpath, None).await?;
            if cfg.json {
                print_json_body(if resp.body.is_empty() {
                    br#"{"deleted":true}"#
                } else {
                    &resp.body
                });
            } else {
                println!("deleted bucket {name}");
            }
            Ok(())
        }
        BucketCmd::Config { cmd } => bucket_config(client, cfg, cmd).await,
    }
}

async fn bucket_config(
    client: &HttpClient,
    cfg: &ClientConfig,
    cmd: BucketConfigCmd,
) -> Result<(), String> {
    match cmd {
        BucketConfigCmd::Get { name, aspect } => {
            let field = config_aspect_field(&aspect).ok_or_else(|| {
                format!("unknown config aspect '{aspect}' (expected one of: policy, cors, lifecycle, replication, tagging)")
            })?;
            let subpath = format!("/buckets/{}/config", pct_encode_segment(&name));
            let resp = api_get(client, cfg, &subpath).await?;
            let doc: serde_json::Value = decode(&resp)?;
            let value = doc.get(field).cloned().unwrap_or(serde_json::Value::Null);
            if cfg.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&value).unwrap_or_else(|_| "null".to_owned())
                );
            } else if value.is_null() {
                println!("{aspect}: (unset)");
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
                );
            }
            Ok(())
        }
        BucketConfigCmd::Set { name, aspect, file } => {
            let subpath = config_set_subpath(&name, &aspect).ok_or_else(|| {
                format!(
                    "setting config aspect '{aspect}' is not supported by the management API \
                     (only 'policy' can be set)"
                )
            })?;
            let body = std::fs::read(&file)
                .map_err(|e| format!("failed to read {}: {e}", file.display()))?;
            let resp =
                api_send(client, cfg, Method::PUT, &subpath, Some(Bytes::from(body))).await?;
            if cfg.json {
                print_json_body(if resp.body.is_empty() {
                    br#"{"updated":true}"#
                } else {
                    &resp.body
                });
            } else {
                println!("set {aspect} on bucket {name}");
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------------------
// User commands
// ---------------------------------------------------------------------------------------

async fn user(client: &HttpClient, cfg: &ClientConfig, cmd: UserCmd) -> Result<(), String> {
    match cmd {
        UserCmd::Ls => {
            let resp = api_get(client, cfg, "/users").await?;
            if cfg.json {
                print_json_body(&resp.body);
                return Ok(());
            }
            let list: UserListResp = decode(&resp)?;
            if list.users.is_empty() {
                println!("(no users)");
            } else {
                for u in &list.users {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        u.id,
                        u.display_name,
                        u.role,
                        if u.is_active { "active" } else { "inactive" },
                        u.access_key_id
                    );
                }
            }
            Ok(())
        }
        UserCmd::Create {
            name,
            replication_policy_bucket,
        } => {
            let mut body = serde_json::json!({ "display_name": name, "role": "member" });
            if let Some(b) = replication_policy_bucket {
                body["replication_policy_bucket"] = serde_json::Value::String(b);
            }
            let resp = api_send(
                client,
                cfg,
                Method::POST,
                "/users",
                Some(Bytes::from(body.to_string())),
            )
            .await?;
            if cfg.json {
                print_json_body(&resp.body);
            } else {
                let created: CreateUserResp = decode(&resp)?;
                println!(
                    "Created user {}. Save these credentials now — shown only once.\n",
                    created.id
                );
                println!("  Bearer access key:  {}", created.bearer_access_key_id);
                println!("  Bearer secret:      {}", created.bearer_secret);
                println!(
                    "  Bearer token:       {}.{}",
                    created.bearer_access_key_id, created.bearer_secret
                );
                println!("  S3 access key id:   {}", created.s3_access_key_id);
                println!("  S3 secret key:      {}", created.s3_secret_key);
            }
            Ok(())
        }
        UserCmd::Rotate { id } => {
            let subpath = format!("/users/{}/rotate-credentials", pct_encode_segment(&id));
            let resp = api_send(client, cfg, Method::POST, &subpath, None).await?;
            if cfg.json {
                print_json_body(&resp.body);
            } else {
                let rotated: RotateCredentialsResp = decode(&resp)?;
                println!("Rotated. The new Bearer secret is shown only once.\n");
                println!("  Bearer access key:  {}", rotated.bearer_access_key_id);
                println!("  Bearer secret:      {}", rotated.bearer_secret);
                println!(
                    "  Bearer token:       {}.{}",
                    rotated.bearer_access_key_id, rotated.bearer_secret
                );
            }
            Ok(())
        }
        UserCmd::Quota { id, value } => {
            let quota_bytes = parse_quota(&value)?;
            let body = serde_json::json!({ "quota_bytes": quota_bytes });
            let subpath = format!("/users/{}/quota", pct_encode_segment(&id));
            let resp = api_send(
                client,
                cfg,
                Method::PUT,
                &subpath,
                Some(Bytes::from(body.to_string())),
            )
            .await?;
            if cfg.json {
                print_json_body(if resp.body.is_empty() {
                    br#"{"updated":true}"#
                } else {
                    &resp.body
                });
            } else {
                match quota_bytes {
                    Some(n) => println!("set quota for user {id} to {n} bytes"),
                    None => println!("removed quota for user {id}"),
                }
            }
            Ok(())
        }
        UserCmd::Rm { id } => {
            let subpath = format!("/users/{}", pct_encode_segment(&id));
            let resp = api_send(client, cfg, Method::DELETE, &subpath, None).await?;
            if cfg.json {
                print_json_body(if resp.body.is_empty() {
                    br#"{"deleted":true}"#
                } else {
                    &resp.body
                });
            } else {
                println!("deleted user {id}");
            }
            Ok(())
        }
    }
}

/// Parse a quota argument: a non-negative integer of bytes, or the literal `none` for no limit.
fn parse_quota(value: &str) -> Result<Option<u64>, String> {
    if value.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| format!("invalid quota '{value}': expected a byte count or 'none'"))
}

// ---------------------------------------------------------------------------------------
// Replication commands
// ---------------------------------------------------------------------------------------

/// Render a byte count in a compact human form.
fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

async fn import(client: &HttpClient, cfg: &ClientConfig, cmd: ImportCmd) -> Result<(), String> {
    match cmd {
        ImportCmd::Run {
            source_endpoint,
            region,
            source_key,
            source_secret,
            buckets,
            workers,
            ca_cert,
            insecure_skip_verify,
            dry_run,
            detach,
        } => {
            let bucket_maps: Vec<serde_json::Value> = buckets
                .iter()
                .map(|b| {
                    let (s, d) = parse_bucket_map(b);
                    serde_json::json!({ "source": s, "dest": d })
                })
                .collect();
            let ca_cert_pem = match &ca_cert {
                Some(p) => Some(
                    std::fs::read_to_string(p)
                        .map_err(|e| format!("reading CA certificate {}: {e}", p.display()))?,
                ),
                None => None,
            };
            let mut body = serde_json::json!({
                "source_endpoint": source_endpoint,
                "source_region": region,
                "access_key": source_key,
                "secret": source_secret,
                "buckets": bucket_maps,
                "insecure_skip_verify": insecure_skip_verify,
            });
            if let Some(w) = workers {
                body["workers"] = serde_json::json!(w);
            }
            if let Some(pem) = ca_cert_pem {
                body["ca_cert"] = serde_json::json!(pem);
            }
            if dry_run {
                // Redact the secret in the printed request.
                let mut shown = body.clone();
                shown["secret"] = serde_json::json!("<redacted>");
                println!("dry run — would POST /api/v1/imports:");
                print_json_body(shown.to_string().as_bytes());
                return Ok(());
            }
            let resp = api_send(
                client,
                cfg,
                Method::POST,
                "/imports",
                Some(Bytes::from(body.to_string())),
            )
            .await?;
            let created: CreateImportResp = decode(&resp)?;
            if cfg.json {
                print_json_body(&resp.body);
            } else {
                println!("started import job {}", created.id);
            }
            if detach {
                return Ok(());
            }
            poll_import(client, cfg, &created.id).await
        }
        ImportCmd::Ls => {
            let resp = api_get(client, cfg, "/imports").await?;
            if cfg.json {
                print_json_body(&resp.body);
                return Ok(());
            }
            let list: ImportListResp = decode(&resp)?;
            if list.jobs.is_empty() {
                println!("no import jobs");
            }
            for j in &list.jobs {
                println!(
                    "{}  {:<10} {}  {}/{} objects  {}",
                    j.id,
                    j.state,
                    j.source_endpoint,
                    j.objects_done,
                    j.objects_total,
                    human_bytes(j.bytes_done)
                );
            }
            Ok(())
        }
        ImportCmd::Status { id } => {
            let resp = api_get(
                client,
                cfg,
                &format!("/imports/{}", pct_encode_segment(&id)),
            )
            .await?;
            if cfg.json {
                print_json_body(&resp.body);
                return Ok(());
            }
            let d: ImportJobDetail = decode(&resp)?;
            println!(
                "job {}  state={}  {}/{} objects  {}",
                d.entry.id,
                d.entry.state,
                d.entry.objects_done,
                d.entry.objects_total,
                human_bytes(d.entry.bytes_done)
            );
            for b in &d.buckets {
                println!(
                    "  {} -> {}  {:<10} {}/{} objects{}",
                    b.source_bucket,
                    b.dest_bucket,
                    b.state,
                    b.objects_done,
                    b.objects_total,
                    b.last_error
                        .as_deref()
                        .map(|e| format!("  (error: {e})"))
                        .unwrap_or_default()
                );
            }
            if let Some(e) = &d.last_error {
                println!("  error: {e}");
            }
            Ok(())
        }
        ImportCmd::Cancel { id } => {
            api_send(
                client,
                cfg,
                Method::DELETE,
                &format!("/imports/{}", pct_encode_segment(&id)),
                None,
            )
            .await?;
            println!("cancelled import job {id}");
            Ok(())
        }
        ImportCmd::Resume { id } => {
            api_send(
                client,
                cfg,
                Method::POST,
                &format!("/imports/{}/resume", pct_encode_segment(&id)),
                None,
            )
            .await?;
            println!("resumed import job {id}");
            Ok(())
        }
    }
}

/// Poll a job's status every couple of seconds, printing a progress line, until it reaches a
/// terminal state. A failed job is surfaced as an error.
async fn poll_import(client: &HttpClient, cfg: &ClientConfig, id: &str) -> Result<(), String> {
    loop {
        let resp = api_get(client, cfg, &format!("/imports/{}", pct_encode_segment(id))).await?;
        let d: ImportJobDetail = decode(&resp)?;
        println!(
            "  {:<10} {}/{} objects  {}",
            d.entry.state,
            d.entry.objects_done,
            d.entry.objects_total,
            human_bytes(d.entry.bytes_done)
        );
        match d.entry.state.as_str() {
            "completed" | "cancelled" => return Ok(()),
            "failed" => {
                return Err(format!(
                    "import failed: {}",
                    d.last_error.as_deref().unwrap_or("(no error recorded)")
                ));
            }
            _ => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
        }
    }
}

async fn replication(
    client: &HttpClient,
    cfg: &ClientConfig,
    cmd: ReplicationCmd,
) -> Result<(), String> {
    match cmd {
        ReplicationCmd::Target {
            cmd:
                ReplicationTargetCmd::Add {
                    bucket,
                    endpoint,
                    region,
                    dest_bucket,
                    access_key,
                    secret,
                },
        } => {
            let body = serde_json::json!({
                "endpoint": endpoint,
                "region": region,
                "dest_bucket": dest_bucket,
                "access_key": access_key,
                "secret": secret,
            });
            let subpath = format!(
                "/buckets/{}/replication/targets",
                pct_encode_segment(&bucket)
            );
            let resp = api_send(
                client,
                cfg,
                Method::POST,
                &subpath,
                Some(Bytes::from(body.to_string())),
            )
            .await?;
            if cfg.json {
                print_json_body(&resp.body);
            } else {
                let created: CreateReplicationTargetResp = decode(&resp)?;
                println!("added replication target {}", created.arn);
            }
            Ok(())
        }
        ReplicationCmd::Status { bucket } => {
            let subpath = format!(
                "/buckets/{}/replication/status",
                pct_encode_segment(&bucket)
            );
            let resp = api_get(client, cfg, &subpath).await?;
            if cfg.json {
                print_json_body(&resp.body);
                return Ok(());
            }
            let status: ReplicationStatusResp = decode(&resp)?;
            println!(
                "bucket {}: pending={} failed={}",
                status.bucket, status.pending, status.failed
            );
            for e in &status.recent_errors {
                println!(
                    "  {} — {}",
                    e.key,
                    e.error.as_deref().unwrap_or("(no error recorded)")
                );
            }
            Ok(())
        }
        ReplicationCmd::Retry { bucket } => {
            let subpath = format!("/buckets/{}/replication/retry", pct_encode_segment(&bucket));
            let resp = api_send(client, cfg, Method::POST, &subpath, None).await?;
            if cfg.json {
                print_json_body(&resp.body);
            } else {
                let retried: ReplicationRetryResp = decode(&resp)?;
                println!(
                    "requeued={} (failed entries observed: {})",
                    retried.requeued, retried.failed_observed
                );
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------------------
// Object commands (S3 data plane)
// ---------------------------------------------------------------------------------------

async fn object(client: &HttpClient, cfg: &ClientConfig, cmd: ObjectCmd) -> Result<(), String> {
    match cmd {
        ObjectCmd::Ls { bucket, prefix } => {
            // Object listing is served by the management API, which folds delimiters and pages.
            let mut subpath = format!("/buckets/{}/objects", pct_encode_segment(&bucket));
            if let Some(p) = &prefix {
                subpath.push_str("?prefix=");
                subpath.push_str(&pct_encode_segment(p));
            }
            let resp = api_get(client, cfg, &subpath).await?;
            if cfg.json {
                print_json_body(&resp.body);
                return Ok(());
            }
            let list: ObjectListResp = decode(&resp)?;
            for p in &list.common_prefixes {
                println!("{p}");
            }
            for o in &list.objects {
                println!("{}\t{}\t{}", o.key, o.size, o.etag);
            }
            if list.objects.is_empty() && list.common_prefixes.is_empty() {
                println!("(no objects)");
            }
            Ok(())
        }
        ObjectCmd::Get {
            bucket,
            key,
            output,
        } => {
            let url = cfg.object_url(&bucket, &key);
            let resp = send(client, cfg, Method::GET, &url, None, Bytes::new()).await?;
            if !(200..300).contains(&resp.status) {
                return Err(format_api_error(&resp));
            }
            match output {
                Some(path) => {
                    std::fs::write(&path, &resp.body)
                        .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
                    if !cfg.json {
                        eprintln!("wrote {} bytes to {}", resp.body.len(), path.display());
                    }
                }
                None => {
                    use std::io::Write;
                    std::io::stdout()
                        .write_all(&resp.body)
                        .map_err(|e| format!("failed to write to stdout: {e}"))?;
                }
            }
            Ok(())
        }
        ObjectCmd::Put { bucket, key, file } => {
            let body = std::fs::read(&file)
                .map_err(|e| format!("failed to read {}: {e}", file.display()))?;
            let len = body.len();
            let url = cfg.object_url(&bucket, &key);
            let resp = send(
                client,
                cfg,
                Method::PUT,
                &url,
                Some("application/octet-stream"),
                Bytes::from(body),
            )
            .await?;
            if !(200..300).contains(&resp.status) {
                return Err(format_api_error(&resp));
            }
            if cfg.json {
                println!("{{\"uploaded\":true,\"bytes\":{len}}}");
            } else {
                println!("uploaded {len} bytes to {bucket}/{key}");
            }
            Ok(())
        }
        ObjectCmd::Rm { bucket, key } => {
            let url = cfg.object_url(&bucket, &key);
            let resp = send(client, cfg, Method::DELETE, &url, None, Bytes::new()).await?;
            if !(200..300).contains(&resp.status) {
                return Err(format_api_error(&resp));
            }
            if cfg.json {
                println!("{{\"deleted\":true}}");
            } else {
                println!("deleted {bucket}/{key}");
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------------------
// Share
// ---------------------------------------------------------------------------------------

async fn share(client: &HttpClient, cfg: &ClientConfig, cmd: ShareCmd) -> Result<(), String> {
    match cmd {
        ShareCmd::Create {
            bucket,
            key,
            expires,
            forever,
            download,
            filename,
            version,
        } => {
            let expires_in_secs = if forever {
                serde_json::Value::Null
            } else {
                let secs = match &expires {
                    Some(e) => {
                        parse_duration(e).ok_or_else(|| format!("invalid --expires: {e}"))?
                    }
                    None => 86_400,
                };
                serde_json::Value::from(secs)
            };
            let body = serde_json::json!({
                "key": key,
                "expires_in_secs": expires_in_secs,
                "disposition": if download { "attachment" } else { "inline" },
                "filename": filename,
                "version_id": version,
            })
            .to_string();
            let resp = api_send(
                client,
                cfg,
                Method::POST,
                &format!("/buckets/{bucket}/objects/share"),
                Some(Bytes::from(body)),
            )
            .await?;
            print_share_url(&resp, cfg)
        }
        ShareCmd::Presign {
            bucket,
            key,
            expires,
            upload,
            content_type,
        } => {
            let secs =
                parse_duration(&expires).ok_or_else(|| format!("invalid --expires: {expires}"))?;
            if !(1..=604_800).contains(&secs) {
                return Err("--expires must be between 1s and 7d for a presigned URL".to_owned());
            }
            let body = serde_json::json!({
                "key": key,
                "method": if upload { "PUT" } else { "GET" },
                "expires_in_secs": secs,
                "content_type": content_type,
            })
            .to_string();
            let resp = api_send(
                client,
                cfg,
                Method::POST,
                &format!("/buckets/{bucket}/objects/presign"),
                Some(Bytes::from(body)),
            )
            .await?;
            print_share_url(&resp, cfg)
        }
        ShareCmd::List { bucket, key } => {
            let subpath = match &key {
                Some(k) => format!("/buckets/{bucket}/objects/shares?key={}", encode_query(k)),
                None => format!("/buckets/{bucket}/objects/shares"),
            };
            let resp = api_get(client, cfg, &subpath).await?;
            if cfg.json {
                print_json_body(&resp.body);
            } else {
                let v: serde_json::Value =
                    serde_json::from_slice(&resp.body).map_err(|e| e.to_string())?;
                let shares = v.get("shares").and_then(|s| s.as_array());
                match shares {
                    Some(s) if !s.is_empty() => {
                        for sh in s {
                            let g = |k: &str| sh.get(k).and_then(|x| x.as_str()).unwrap_or("");
                            println!("{:8}  {}  {}", g("status"), g("token"), g("key"));
                        }
                    }
                    _ => println!("no shares"),
                }
            }
            Ok(())
        }
        ShareCmd::Revoke { bucket, token } => {
            api_send(
                client,
                cfg,
                Method::DELETE,
                &format!("/buckets/{bucket}/objects/shares/{token}"),
                None,
            )
            .await?;
            if cfg.json {
                println!(r#"{{"revoked":true}}"#);
            } else {
                println!("revoked");
            }
            Ok(())
        }
    }
}

/// Print the `url` from a share/presign response (or the raw JSON with `--json`).
fn print_share_url(resp: &HttpResponse, cfg: &ClientConfig) -> Result<(), String> {
    if cfg.json {
        print_json_body(&resp.body);
        return Ok(());
    }
    let v: serde_json::Value = serde_json::from_slice(&resp.body).map_err(|e| e.to_string())?;
    if let Some(url) = v.get("url").and_then(|u| u.as_str()) {
        // A persistent share returns a path (/p/{token}); make it absolute against the endpoint.
        if let Some(path) = url.strip_prefix('/') {
            let base = cfg.endpoint.trim_end_matches('/');
            println!("{base}/{path}");
        } else {
            println!("{url}");
        }
    }
    Ok(())
}

/// Parse a duration like `24h`, `7d`, `30m`, `3600s`, or bare seconds → seconds.
fn parse_duration(s: &str) -> Option<i64> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix('d') {
        (n, 86_400)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1)
    } else {
        (s, 1)
    };
    let v: i64 = num.trim().parse().ok()?;
    (v >= 0).then_some(v * mult)
}

/// Percent-encode a query value (the unreserved set passes through).
fn encode_query(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

// ---------------------------------------------------------------------------------------
// Overview
// ---------------------------------------------------------------------------------------

async fn overview(client: &HttpClient, cfg: &ClientConfig) -> Result<(), String> {
    let resp = api_get(client, cfg, "/overview").await?;
    if cfg.json {
        print_json_body(&resp.body);
        return Ok(());
    }
    let o: OverviewResp = decode(&resp)?;
    println!("buckets:           {}", o.buckets);
    println!("objects:           {}", o.objects);
    println!("versions:          {}", o.versions);
    println!("logical bytes:     {}", o.logical_bytes);
    println!("physical bytes:    {}", o.physical_bytes);
    println!("compression ratio: {:.3}", o.compression_ratio);
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Management-API request helpers (status-checked)
// ---------------------------------------------------------------------------------------

/// Issue a `GET` to a management-API subpath, returning the response only on a 2xx status (mapping
/// any non-2xx to the structured error string).
async fn api_get(
    client: &HttpClient,
    cfg: &ClientConfig,
    subpath: &str,
) -> Result<HttpResponse, String> {
    api_send(client, cfg, Method::GET, subpath, None).await
}

/// Issue a request to a management-API subpath with an optional JSON body, returning the response
/// only on a 2xx status.
async fn api_send(
    client: &HttpClient,
    cfg: &ClientConfig,
    method: Method,
    subpath: &str,
    body: Option<Bytes>,
) -> Result<HttpResponse, String> {
    let url = cfg.api_url(subpath);
    let (ct, payload) = match body {
        Some(b) => (Some("application/json"), b),
        None => (None, Bytes::new()),
    };
    let resp = send(client, cfg, method, &url, ct, payload).await?;
    if (200..300).contains(&resp.status) {
        Ok(resp)
    } else {
        Err(format_api_error(&resp))
    }
}

// ---------------------------------------------------------------------------------------
// Tests (no live server: arg parsing, URL construction, header building, helpers)
// ---------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(endpoint: &str, ak: Option<&str>, sk: Option<&str>, json: bool) -> RemoteOpts {
        RemoteOpts {
            endpoint: endpoint.to_owned(),
            access_key: ak.map(str::to_owned),
            secret_key: sk.map(str::to_owned),
            json,
        }
    }

    #[test]
    fn bearer_token_requires_both_halves() {
        assert_eq!(bearer_token(Some("a"), Some("s")), Some("a.s".to_owned()));
        assert_eq!(bearer_token(Some("a"), None), None);
        assert_eq!(bearer_token(None, Some("s")), None);
        assert_eq!(bearer_token(Some(""), Some("s")), None);
        assert_eq!(bearer_token(Some("a"), Some("")), None);
    }

    #[test]
    fn authorization_header_is_scheme_prefixed() {
        assert_eq!(
            authorization_value("cairn_abc.secret"),
            "Bearer cairn_abc.secret"
        );
    }

    #[test]
    fn config_resolve_trims_trailing_slash_and_builds_token() {
        let cfg = ClientConfig::resolve(&opts("http://h:7374/", Some("ak"), Some("sk"), true));
        assert_eq!(cfg.endpoint, "http://h:7374");
        assert_eq!(cfg.token.as_deref(), Some("ak.sk"));
        assert!(cfg.json);
    }

    #[test]
    fn config_resolve_without_creds_has_no_token() {
        let cfg = ClientConfig::resolve(&opts("http://127.0.0.1:7374", None, None, false));
        assert_eq!(cfg.token, None);
        assert!(!cfg.json);
    }

    #[test]
    fn api_url_inserts_versioned_prefix() {
        let cfg = ClientConfig::resolve(&opts(DEFAULT_ENDPOINT, None, None, false));
        assert_eq!(
            cfg.api_url("/buckets"),
            "http://127.0.0.1:7374/api/v1/buckets"
        );
        assert_eq!(
            cfg.api_url("/users/u1/quota"),
            "http://127.0.0.1:7374/api/v1/users/u1/quota"
        );
    }

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("1h"), Some(3600));
        assert_eq!(parse_duration("7d"), Some(604_800));
        assert_eq!(parse_duration("30m"), Some(1800));
        assert_eq!(parse_duration("3600s"), Some(3600));
        assert_eq!(parse_duration("42"), Some(42)); // bare seconds
        assert_eq!(parse_duration("-1h"), None);
        assert_eq!(parse_duration("abc"), None);
    }

    #[test]
    fn object_url_is_path_style_and_encodes() {
        let cfg = ClientConfig::resolve(&opts("https://s3.example.com", None, None, false));
        assert_eq!(
            cfg.object_url("photos", "a/b c.jpg"),
            "https://s3.example.com/photos/a/b%20c.jpg"
        );
        // A bucket segment never carries an embedded separator.
        assert_eq!(
            cfg.object_url("my/bucket", "k"),
            "https://s3.example.com/my%2Fbucket/k"
        );
    }

    #[test]
    fn pct_encoding_keeps_slash_only_for_keys() {
        assert_eq!(pct_encode_path("a/b+c"), "a/b%2Bc");
        assert_eq!(pct_encode_segment("a/b+c"), "a%2Fb%2Bc");
        assert_eq!(pct_encode_path("plain-Key_1.2~"), "plain-Key_1.2~");
    }

    #[test]
    fn quota_parses_bytes_and_none() {
        assert_eq!(parse_quota("1024").unwrap(), Some(1024));
        assert_eq!(parse_quota("none").unwrap(), None);
        assert_eq!(parse_quota("NONE").unwrap(), None);
        assert!(parse_quota("-5").is_err());
        assert!(parse_quota("big").is_err());
    }

    #[test]
    fn config_aspect_field_maps_known_aspects() {
        assert_eq!(config_aspect_field("policy"), Some("policy"));
        assert_eq!(config_aspect_field("cors"), Some("cors"));
        assert_eq!(config_aspect_field("lifecycle"), Some("lifecycle"));
        assert_eq!(config_aspect_field("tagging"), Some("tagging"));
        assert_eq!(config_aspect_field("replication"), Some("replication"));
        assert_eq!(config_aspect_field("bogus"), None);
    }

    #[test]
    fn config_set_subpath_only_policy_supported() {
        assert_eq!(
            config_set_subpath("b", "policy").as_deref(),
            Some("/buckets/b/policy")
        );
        assert_eq!(config_set_subpath("b", "cors"), None);
    }

    #[test]
    fn format_api_error_prefers_envelope_with_request_id() {
        let resp = HttpResponse {
            status: 404,
            body: Bytes::from(r#"{"error":"not found","request_id":"abc123"}"#),
        };
        let msg = format_api_error(&resp);
        assert!(msg.contains("404"));
        assert!(msg.contains("not found"));
        assert!(msg.contains("abc123"));
    }

    #[test]
    fn format_api_error_falls_back_to_raw_body() {
        let resp = HttpResponse {
            status: 502,
            body: Bytes::from("upstream boom"),
        };
        let msg = format_api_error(&resp);
        assert!(msg.contains("502"));
        assert!(msg.contains("upstream boom"));
    }

    #[test]
    fn parse_bucket_map_handles_src_and_src_dest() {
        assert_eq!(
            parse_bucket_map("photos"),
            ("photos".to_owned(), "photos".to_owned())
        );
        assert_eq!(
            parse_bucket_map("photos:gallery"),
            ("photos".to_owned(), "gallery".to_owned())
        );
        // A trailing colon with no dest falls back to the source name.
        assert_eq!(
            parse_bucket_map("photos:"),
            ("photos".to_owned(), "photos".to_owned())
        );
    }

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(1024 * 1024 * 5), "5.0 MiB");
    }
}
