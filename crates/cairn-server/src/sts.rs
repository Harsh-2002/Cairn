//! The AWS-STS wire surface (ARCH 14): `Action=AssumeRole` / `Action=GetSessionToken` served on the
//! S3 data-plane port as a form `POST /`, returning AWS-STS XML. This is the standard **minting**
//! protocol over the existing `Mutation::CreateSessionCredential`; the **consumption** path
//! (`authenticate_session`, the `is_session` least-privilege semantics) is unchanged. It lets the
//! AWS SDK default credential-provider chain and Terraform's `assume_role{}` obtain temporary creds.
//!
//! Authentication is a dedicated, genuinely-signed `sts`-scoped SigV4 verification
//! (`AuthChain::authenticate_sts`, no dev bypass, no session chaining); this module then computes
//! the session's inline policy, mints the `CAIRNTMP…` credential, and renders the response XML.
//!
//! Policy semantics (no policy-intersection engine exists, so nothing here can mint broader than the
//! caller):
//! - **`GetSessionToken`** inherits the caller's *effective* access — their identity policy plus a
//!   synthesized `Allow s3:*` over the buckets they own (never bucket-policy grants to the parent,
//!   matching the engine's session rule). An Administrator gets a full-S3 policy.
//! - **`AssumeRole`** requires `RoleArn`/`RoleSessionName` syntactically (recorded for audit only —
//!   Cairn has no IAM roles); an inline `Policy` becomes the session policy **only** for an
//!   Administrator, and a non-admin supplying one is denied (fail closed, no subset proof).
//!
//! Admin-derived sessions carry `Allow s3:*` but are structurally `is_session` — they never receive
//! the owner/admin short-circuit (`cairn-protocol`'s authorize step), so a session can never reach
//! the management API or bypass an explicit Deny.

use crate::stack::AppStack;
use cairn_crypto::SystemClock;
use cairn_types::auth::{Principal, Role};
use cairn_types::id::UserId;
use cairn_types::meta::{ActivityEntry, Mutation, SessionCredentialRecord};
use cairn_types::time::Timestamp;
use cairn_types::traits::{Clock, Crypto, MetadataStore};
use serde_json::Value;
use std::sync::Arc;

/// The bounds on a session credential's lifetime (15 minutes .. 12 hours), mirroring AWS STS and the
/// management-plane mint.
const MIN_DURATION_SECS: i64 = 900;
const MAX_DURATION_SECS: i64 = 43_200;
/// Per-action `DurationSeconds` defaults when omitted (AWS parity).
const ASSUME_ROLE_DEFAULT_SECS: i64 = 3_600;
const GET_SESSION_TOKEN_DEFAULT_SECS: i64 = 43_200;

/// A rendered STS response: the HTTP status and the XML body the adapter writes with a
/// `text/xml` content type.
pub(crate) struct StsHttpResponse {
    pub status: u16,
    pub body: String,
}

impl StsHttpResponse {
    fn error(status: u16, code: &str, message: &str, request_id: &str) -> Self {
        Self {
            status,
            body: cairn_xml::sts_error_document(code, message, request_id),
        }
    }
}

/// A deferred STS failure: the status + code + message, rendered to the wire (with the request id)
/// at the call site. Lets the pure helpers (`duration_secs`) and the store-touching `mint` signal a
/// failure without threading the request id through every layer.
struct StsError {
    status: u16,
    code: &'static str,
    message: String,
}

impl StsError {
    fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn render(&self, request_id: &str) -> StsHttpResponse {
        StsHttpResponse::error(self.status, self.code, &self.message, request_id)
    }
}

/// Map an authentication failure onto the STS query-protocol error shape (the SDK keys retry/refresh
/// off these codes). Every case is a client 4xx.
pub(crate) fn auth_error_response(
    e: &cairn_types::error::AuthError,
    request_id: &str,
) -> StsHttpResponse {
    use cairn_types::error::AuthError;
    let (status, code, msg): (u16, &str, &str) = match e {
        AuthError::UnknownKey => (
            403,
            "InvalidClientTokenId",
            "the security token included in the request is invalid",
        ),
        AuthError::SignatureMismatch | AuthError::ChunkSignatureMismatch => (
            403,
            "SignatureDoesNotMatch",
            "the request signature does not match",
        ),
        AuthError::SkewedClock => (
            403,
            "SignatureDoesNotMatch",
            "the request timestamp is outside the permitted skew window",
        ),
        AuthError::Expired => (403, "ExpiredToken", "the request has expired"),
        AuthError::Malformed => (
            400,
            "IncompleteSignature",
            "the request signature is missing or malformed",
        ),
    };
    StsHttpResponse::error(status, code, msg, request_id)
}

/// Handle a buffered STS form `POST /`. The caller has already verified the body hash against the
/// signature and authenticated `principal` via `AuthChain::authenticate_sts`. Dispatches on the
/// `Action` field and returns the rendered response (success or STS error XML).
pub(crate) async fn handle(
    stack: &AppStack,
    body: &[u8],
    principal: &Principal,
    request_id: &str,
) -> StsHttpResponse {
    let form = parse_form(body);
    match form_get(&form, "Action") {
        Some("GetSessionToken") => get_session_token(stack, &form, principal, request_id).await,
        Some("AssumeRole") => assume_role(stack, &form, principal, request_id).await,
        Some(other) => StsHttpResponse::error(
            400,
            "InvalidAction",
            &format!("unsupported STS action: {other}"),
            request_id,
        ),
        None => StsHttpResponse::error(
            400,
            "InvalidAction",
            "the request is missing the Action parameter",
            request_id,
        ),
    }
}

/// `Action=GetSessionToken`: mint a session inheriting the caller's effective access.
async fn get_session_token(
    stack: &AppStack,
    form: &[(String, String)],
    principal: &Principal,
    request_id: &str,
) -> StsHttpResponse {
    let duration = match duration_secs(form, GET_SESSION_TOKEN_DEFAULT_SECS) {
        Ok(d) => d,
        Err(e) => return e.render(request_id),
    };
    let policy_json = if principal.role == Role::Administrator {
        full_s3_policy()
    } else {
        effective_access_policy(&stack.meta, &principal.user_id).await
    };
    match mint(stack, principal, &policy_json, duration).await {
        Ok(minted) => StsHttpResponse {
            status: 200,
            body: cairn_xml::get_session_token_response(
                &minted.access_key_id,
                &minted.secret,
                &minted.session_token,
                minted.expires_at,
                request_id,
            ),
        },
        Err(e) => e.render(request_id),
    }
}

/// `Action=AssumeRole`: mint a session for the (audit-only) role. `RoleArn`/`RoleSessionName` are
/// required syntactically; an inline `Policy` is honoured only for an Administrator.
async fn assume_role(
    stack: &AppStack,
    form: &[(String, String)],
    principal: &Principal,
    request_id: &str,
) -> StsHttpResponse {
    // RoleArn + RoleSessionName are required by the SDK/Terraform contract even though Cairn has no
    // IAM roles; validate them syntactically (recorded only in the echoed AssumedRoleUser + audit).
    let Some(role_arn) = form_get(form, "RoleArn") else {
        return StsHttpResponse::error(400, "ValidationError", "RoleArn is required", request_id);
    };
    if !(20..=2048).contains(&role_arn.len()) {
        return StsHttpResponse::error(
            400,
            "ValidationError",
            "RoleArn must be between 20 and 2048 characters",
            request_id,
        );
    }
    let Some(session_name) = form_get(form, "RoleSessionName") else {
        return StsHttpResponse::error(
            400,
            "ValidationError",
            "RoleSessionName is required",
            request_id,
        );
    };
    if !valid_role_session_name(session_name) {
        return StsHttpResponse::error(
            400,
            "ValidationError",
            "RoleSessionName must match [\\w+=,.@-]{2,64}",
            request_id,
        );
    }
    let duration = match duration_secs(form, ASSUME_ROLE_DEFAULT_SECS) {
        Ok(d) => d,
        Err(e) => return e.render(request_id),
    };

    let is_admin = principal.role == Role::Administrator;
    let policy_json = match assume_role_policy_outcome(is_admin, form_get(form, "Policy")) {
        PolicyOutcome::Ready(json) => json,
        // No policy: inherit the caller's effective access (only reachable for a non-admin here).
        PolicyOutcome::Effective => effective_access_policy(&stack.meta, &principal.user_id).await,
        // A non-admin cannot supply an inline Policy: with no intersection engine we cannot prove it
        // is a subset of their access, so we fail closed rather than risk widening.
        PolicyOutcome::DeniedNonAdminPolicy => {
            return StsHttpResponse::error(
                403,
                "AccessDenied",
                "an inline session Policy on AssumeRole is permitted only for an administrator; \
                 omit it to inherit your effective access, or use GetSessionToken",
                request_id,
            );
        }
        PolicyOutcome::Malformed(e) => {
            return StsHttpResponse::error(
                400,
                "MalformedPolicyDocument",
                &format!("invalid session Policy: {e}"),
                request_id,
            );
        }
    };

    match mint(stack, principal, &policy_json, duration).await {
        Ok(minted) => {
            let role_name = role_arn.rsplit('/').next().unwrap_or(role_arn);
            let assumed_role_id = format!("{}:{session_name}", minted.access_key_id);
            let assumed_role_arn =
                format!("arn:aws:sts::cairn:assumed-role/{role_name}/{session_name}");
            StsHttpResponse {
                status: 200,
                body: cairn_xml::assume_role_response(
                    &minted.access_key_id,
                    &minted.secret,
                    &minted.session_token,
                    minted.expires_at,
                    &assumed_role_id,
                    &assumed_role_arn,
                    request_id,
                ),
            }
        }
        Err(e) => e.render(request_id),
    }
}

/// A freshly minted temporary credential (secret + token appear here once, then only the hash is
/// stored).
struct Minted {
    access_key_id: String,
    secret: String,
    session_token: String,
    expires_at: Timestamp,
}

/// Mint a `CAIRNTMP…` session credential with `policy_json` as its scoped inline policy: seal the
/// secret (CRK1), persist the row via the single writer, and audit the mint identically to the
/// management-plane mint (`MintSessionCredential`). On a crypto/store failure returns a
/// `500 InternalFailure` STS error (fail closed — never a partial credential).
async fn mint(
    stack: &AppStack,
    principal: &Principal,
    policy_json: &str,
    duration_secs: i64,
) -> Result<Minted, StsError> {
    let now = SystemClock::new().now();
    let access_key_id = format!(
        "CAIRNTMP{}",
        uuid::Uuid::new_v4().simple().to_string().to_uppercase()
    );
    let secret = generate_secret();
    // The opaque token the SDK presents as `X-Amz-Security-Token`; only its hash is stored.
    let session_token = generate_secret();
    let (secret_ciphertext, secret_nonce) = match stack.crypto.seal(secret.as_bytes()) {
        // CRK1 envelope (audit #29): the nonce is inside the ciphertext; store NULL nonce.
        Ok(sealed) => (sealed.ciphertext, None),
        Err(_) => {
            return Err(StsError::new(
                500,
                "InternalFailure",
                "could not seal credential",
            ));
        }
    };
    let expires_at = Timestamp(now.0 + duration_secs * 1000);
    let record = SessionCredentialRecord {
        access_key_id: access_key_id.clone(),
        parent_user_id: principal.user_id.clone(),
        secret_ciphertext,
        secret_nonce,
        session_token_hash: cairn_auth::hash_session_token(&session_token),
        inline_policy: Some(policy_json.to_owned()),
        expires_at,
        created_at: now,
    };
    if stack
        .meta
        .submit(Mutation::CreateSessionCredential(Box::new(record)))
        .await
        .is_err()
    {
        return Err(StsError::new(
            500,
            "InternalFailure",
            "could not mint credential",
        ));
    }
    // Audit the mint (best-effort) with the same action string as the management-plane mint, so an
    // operator sees STS mints identically. `actor` is the long-term caller's access key.
    let _ = stack
        .meta
        .submit(Mutation::RecordActivity(Box::new(ActivityEntry {
            id: uuid::Uuid::new_v4().simple().to_string(),
            action: "MintSessionCredential".to_owned(),
            bucket: None,
            key: None,
            size: None,
            etag: None,
            actor: Some(principal.access_key_id.clone()),
            at: now,
        })))
        .await;
    Ok(Minted {
        access_key_id,
        secret,
        session_token,
        expires_at,
    })
}

/// The policy the session receives on `AssumeRole`, decided purely from the admin flag and the
/// presence of an inline `Policy` (extracted so the escalation guard is unit-testable).
enum PolicyOutcome {
    /// A ready inline-policy JSON string (an admin's validated `Policy`, or admin full-S3).
    Ready(String),
    /// Inherit the caller's effective access (a non-admin with no inline `Policy`).
    Effective,
    /// A non-admin supplied an inline `Policy` — denied (fail closed; no subset proof).
    DeniedNonAdminPolicy,
    /// An admin's inline `Policy` did not parse.
    Malformed(String),
}

/// Decide the `AssumeRole` session policy. Admin-only inline `Policy`; a non-admin with a `Policy`
/// is denied; otherwise inherit effective access.
fn assume_role_policy_outcome(is_admin: bool, inline_policy: Option<&str>) -> PolicyOutcome {
    match (is_admin, inline_policy) {
        (true, Some(raw)) => match cairn_authz::parse_user_policy(raw) {
            Ok(_) => PolicyOutcome::Ready(raw.to_owned()),
            Err(e) => PolicyOutcome::Malformed(e.to_string()),
        },
        (true, None) => PolicyOutcome::Ready(full_s3_policy()),
        (false, Some(_)) => PolicyOutcome::DeniedNonAdminPolicy,
        (false, None) => PolicyOutcome::Effective,
    }
}

/// The caller's *effective* access as a session inline policy (ARCH 14): the raw identity-policy
/// statements (if any) plus a synthesized `Allow s3:*` over every bucket the caller owns. It never
/// widens — every statement is one the caller already holds — and mirrors the engine's session rule
/// (bucket-policy grants to the parent are NOT inherited). A caller with no identity policy and no
/// owned buckets gets an empty (inert) statement list, which is the honest reflection of their
/// session-scoped access.
async fn effective_access_policy(meta: &Arc<dyn MetadataStore>, user_id: &UserId) -> String {
    let mut statements: Vec<Value> = Vec::new();
    // Identity-policy statements, copied verbatim (the store validated them at SetUserPolicy time).
    if let Ok(Some(raw)) = meta.get_user_policy(user_id).await {
        if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(&raw) {
            match map.get("Statement") {
                Some(Value::Array(arr)) => statements.extend(arr.iter().cloned()),
                Some(obj @ Value::Object(_)) => statements.push(obj.clone()),
                _ => {}
            }
        }
    }
    // A synthesized owner grant over the caller's owned buckets.
    if let Some(synth) = owned_buckets_statement(meta, user_id).await {
        statements.push(synth);
    }
    let doc = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": statements,
    })
    .to_string();
    // Defensive: the merged document must be a valid user policy (the consumption path parses it). If
    // an unexpected identity statement broke it, fall back to owned-buckets-only rather than store a
    // policy that would fail closed to nothing.
    if cairn_authz::parse_user_policy(&doc).is_ok() {
        doc
    } else {
        let statements: Vec<Value> = owned_buckets_statement(meta, user_id)
            .await
            .into_iter()
            .collect();
        serde_json::json!({ "Version": "2012-10-17", "Statement": statements }).to_string()
    }
}

/// The synthesized `Allow s3:*` statement over the caller's owned buckets (`None` when they own
/// none), the owner half of the effective-access policy.
async fn owned_buckets_statement(meta: &Arc<dyn MetadataStore>, user_id: &UserId) -> Option<Value> {
    let buckets = meta.list_buckets(Some(user_id)).await.ok()?;
    let mut resources: Vec<String> = Vec::with_capacity(buckets.len() * 2);
    for b in &buckets {
        resources.push(format!("arn:aws:s3:::{}", b.name.as_str()));
        resources.push(format!("arn:aws:s3:::{}/*", b.name.as_str()));
    }
    if resources.is_empty() {
        return None;
    }
    Some(serde_json::json!({
        "Effect": "Allow",
        "Action": "s3:*",
        "Resource": resources,
    }))
}

/// A full-S3 inline policy (`Allow s3:* on *`) for an administrator-derived session. The session is
/// still structurally `is_session`, so it never gets the owner/admin short-circuit.
fn full_s3_policy() -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{ "Effect": "Allow", "Action": "s3:*", "Resource": "*" }],
    })
    .to_string()
}

/// Parse `DurationSeconds` (per-action default when absent), enforcing the `900..=43200` bound. A
/// non-integer or out-of-range value is an `InvalidParameterValue` STS error.
fn duration_secs(form: &[(String, String)], default_secs: i64) -> Result<i64, StsError> {
    let duration = match form_get(form, "DurationSeconds") {
        Some(s) => s.parse::<i64>().map_err(|_| {
            StsError::new(
                400,
                "InvalidParameterValue",
                "DurationSeconds must be an integer",
            )
        })?,
        None => default_secs,
    };
    if !(MIN_DURATION_SECS..=MAX_DURATION_SECS).contains(&duration) {
        return Err(StsError::new(
            400,
            "InvalidParameterValue",
            "DurationSeconds must be between 900 (15m) and 43200 (12h)",
        ));
    }
    Ok(duration)
}

/// A high-entropy URL-safe secret (two v4 UUIDs of hex). Matches the share-token construction; the
/// row's existence is the capability, and only the hash of the token is ever stored.
fn generate_secret() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Whether `s` matches AWS's `RoleSessionName` pattern `[\w+=,.@-]{2,64}` (`\w` = ASCII letters,
/// digits, underscore).
fn valid_role_session_name(s: &str) -> bool {
    (2..=64).contains(&s.len())
        && s.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'_' | b'+' | b'=' | b',' | b'.' | b'@' | b'-')
        })
}

/// First value for `key` in a parsed form (case-sensitive, matching the AWS query protocol).
fn form_get<'a>(form: &'a [(String, String)], key: &str) -> Option<&'a str> {
    form.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

/// Parse an `application/x-www-form-urlencoded` body into decoded `(name, value)` pairs.
pub(crate) fn parse_form(body: &[u8]) -> Vec<(String, String)> {
    let s = String::from_utf8_lossy(body);
    s.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (form_decode(k), form_decode(v))
        })
        .collect()
}

/// Decode one form component: `+` is a space, then percent-decode.
fn form_decode(s: &str) -> String {
    let spaced: String = s.chars().map(|c| if c == '+' { ' ' } else { c }).collect();
    percent_decode(&spaced)
}

/// Minimal percent-decoding (`%XX`). Invalid escapes are left verbatim.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_form_handles_plus_and_percent_and_repeats() {
        let form =
            parse_form(b"Action=GetSessionToken&DurationSeconds=3600&Name=a+b&Enc=%2Fx%2F&Empty=");
        assert_eq!(form_get(&form, "Action"), Some("GetSessionToken"));
        assert_eq!(form_get(&form, "DurationSeconds"), Some("3600"));
        assert_eq!(form_get(&form, "Name"), Some("a b"));
        assert_eq!(form_get(&form, "Enc"), Some("/x/"));
        assert_eq!(form_get(&form, "Empty"), Some(""));
        assert_eq!(form_get(&form, "Missing"), None);
    }

    #[test]
    fn parse_form_missing_action() {
        let form = parse_form(b"Version=2011-06-15");
        assert_eq!(form_get(&form, "Action"), None);
    }

    #[test]
    fn duration_defaults_and_bounds() {
        // Default applies when absent.
        assert_eq!(duration_secs(&[], 3600).ok(), Some(3600));
        // In-range explicit value is taken.
        let form = vec![("DurationSeconds".to_owned(), "900".to_owned())];
        assert_eq!(duration_secs(&form, 3600).ok(), Some(900));
        // Out-of-range (below and above) is rejected.
        let low = vec![("DurationSeconds".to_owned(), "899".to_owned())];
        assert!(duration_secs(&low, 3600).is_err());
        let high = vec![("DurationSeconds".to_owned(), "43201".to_owned())];
        assert!(duration_secs(&high, 3600).is_err());
        // Non-integer is rejected.
        let bad = vec![("DurationSeconds".to_owned(), "abc".to_owned())];
        assert!(duration_secs(&bad, 3600).is_err());
    }

    #[test]
    fn role_session_name_validation() {
        assert!(valid_role_session_name("deploy"));
        assert!(valid_role_session_name("a.b-c_d@e+f=g,h"));
        assert!(!valid_role_session_name("x")); // too short
        assert!(!valid_role_session_name(&"a".repeat(65))); // too long
        assert!(!valid_role_session_name("has space")); // space not allowed
        assert!(!valid_role_session_name("slash/no")); // '/' not allowed
    }

    #[test]
    fn full_s3_policy_parses_and_is_broad() {
        let doc = full_s3_policy();
        let p = cairn_authz::parse_user_policy(&doc).expect("valid user policy");
        assert_eq!(p.statements.len(), 1);
    }

    #[test]
    fn assume_role_policy_outcomes() {
        // A non-admin supplying an inline Policy is denied (fail closed — no subset proof).
        assert!(matches!(
            assume_role_policy_outcome(false, Some("{}")),
            PolicyOutcome::DeniedNonAdminPolicy
        ));
        // A non-admin without a Policy inherits their effective access.
        assert!(matches!(
            assume_role_policy_outcome(false, None),
            PolicyOutcome::Effective
        ));
        // An admin without a Policy gets full-S3.
        assert!(matches!(
            assume_role_policy_outcome(true, None),
            PolicyOutcome::Ready(_)
        ));
        // An admin with a valid inline Policy uses it verbatim.
        let valid = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#;
        match assume_role_policy_outcome(true, Some(valid)) {
            PolicyOutcome::Ready(json) => assert_eq!(json, valid),
            other => panic!("expected Ready, got a different outcome: {}", other.name()),
        }
        // An admin's malformed inline Policy is rejected.
        assert!(matches!(
            assume_role_policy_outcome(true, Some("not json")),
            PolicyOutcome::Malformed(_)
        ));
    }

    impl PolicyOutcome {
        fn name(&self) -> &'static str {
            match self {
                PolicyOutcome::Ready(_) => "Ready",
                PolicyOutcome::Effective => "Effective",
                PolicyOutcome::DeniedNonAdminPolicy => "DeniedNonAdminPolicy",
                PolicyOutcome::Malformed(_) => "Malformed",
            }
        }
    }

    fn owned_bucket(name: &str, owner: &str) -> Mutation {
        use cairn_types::authz::OwnershipMode;
        use cairn_types::bucket::{Bucket, VersioningState};
        Mutation::CreateBucket(Box::new(Bucket {
            name: cairn_types::BucketName::parse(name).unwrap(),
            owner_id: UserId(owner.to_owned()),
            created_at: Timestamp(1),
            versioning: VersioningState::Unversioned,
            ownership_mode: OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".to_owned(),
            compression: None,
        }))
    }

    /// Finding-1 fix: a member's session inherits an `Allow s3:*` over the buckets they own, so a
    /// `GetSessionToken` session is NOT silently narrowed to inert (the copy-only bug).
    #[tokio::test]
    async fn effective_access_member_with_owned_bucket_synthesizes_owner_grant() {
        let meta: Arc<dyn MetadataStore> =
            Arc::new(cairn_types::testing::InMemoryMetadataStore::new());
        meta.submit(owned_bucket("mybucket", "member"))
            .await
            .unwrap();
        // A bucket owned by someone else must NOT appear.
        meta.submit(owned_bucket("theirs", "someone-else"))
            .await
            .unwrap();
        let policy = effective_access_policy(&meta, &UserId("member".to_owned())).await;
        let parsed = cairn_authz::parse_user_policy(&policy).expect("valid user policy");
        assert_eq!(
            parsed.statements.len(),
            1,
            "one synthesized owner statement"
        );
        assert!(policy.contains("arn:aws:s3:::mybucket"));
        assert!(policy.contains("arn:aws:s3:::mybucket/*"));
        assert!(
            !policy.contains("theirs"),
            "must not grant another owner's bucket"
        );
    }

    #[tokio::test]
    async fn effective_access_no_buckets_no_policy_is_inert() {
        let meta: Arc<dyn MetadataStore> =
            Arc::new(cairn_types::testing::InMemoryMetadataStore::new());
        let policy = effective_access_policy(&meta, &UserId("nobody".to_owned())).await;
        let parsed = cairn_authz::parse_user_policy(&policy).expect("valid user policy");
        assert!(
            parsed.statements.is_empty(),
            "no owned buckets and no identity policy => an honestly inert session"
        );
    }

    #[tokio::test]
    async fn effective_access_merges_identity_policy_and_owned_buckets() {
        let meta: Arc<dyn MetadataStore> =
            Arc::new(cairn_types::testing::InMemoryMetadataStore::new());
        meta.submit(owned_bucket("owned", "member")).await.unwrap();
        meta.submit(Mutation::SetUserPolicy {
            user_id: UserId("member".to_owned()),
            policy: Some(
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::other/*"}]}"#
                    .to_owned(),
            ),
        })
        .await
        .unwrap();
        let policy = effective_access_policy(&meta, &UserId("member".to_owned())).await;
        let parsed = cairn_authz::parse_user_policy(&policy).expect("valid user policy");
        assert_eq!(
            parsed.statements.len(),
            2,
            "the identity statement plus the owned-bucket synth"
        );
        assert!(
            policy.contains("arn:aws:s3:::other/*"),
            "identity grant preserved"
        );
        assert!(
            policy.contains("arn:aws:s3:::owned/*"),
            "owned-bucket synth present"
        );
    }
}
