//! Integration tests for the authenticator chain against the in-memory doubles: a real SigV4
//! signed request round-trips, a tampered signature is denied, and Bearer works end to end.

use cairn_auth::{AuthCache, AuthChain, compute_signature, hash_bearer_secret, signing_key};
use cairn_types::Timestamp;
use cairn_types::auth::{AuthMethod, AuthOutcome, RequestView, Role};
use cairn_types::meta::{Mutation, User, UserRecord};
use cairn_types::testing::{InMemoryMetadataStore, StubCrypto, TestClock};
use cairn_types::traits::{Authenticator, Crypto, MetadataStore};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const AKID: &str = "AKIDEXAMPLE";

async fn setup() -> (AuthChain, Arc<InMemoryMetadataStore>) {
    let meta = Arc::new(InMemoryMetadataStore::new());
    let crypto = Arc::new(StubCrypto);
    let clock = Arc::new(TestClock::at_secs(1_440_938_160)); // 2015-08-30T12:36:00Z

    // Seal the SigV4 secret as the store would hold it.
    let sealed = crypto.seal(SECRET.as_bytes()).unwrap();
    let user = User {
        id: cairn_types::UserId("u1".to_owned()),
        display_name: "alice".to_owned(),
        access_key_id: "bearer-key".to_owned(),
        sigv4_access_key_id: Some(AKID.to_owned()),
        role: Role::Member,
        is_active: true,
        quota_bytes: None,
        created_at: Timestamp(0),
        updated_at: Timestamp(0),
    };
    meta.submit(Mutation::CreateUser(Box::new(UserRecord {
        user,
        bearer_secret_hash: hash_bearer_secret("topsecret"),
        sigv4_secret_ciphertext: Some(sealed.ciphertext),
        sigv4_secret_nonce: Some(sealed.nonce.0),
    })))
    .await
    .unwrap();

    // Exercise the auth path through an enabled cache (a fresh epoch; this suite performs no user
    // mutations after the chain is built, so the cached entries stay valid).
    let cache = Arc::new(AuthCache::new(
        Duration::from_secs(60),
        Arc::new(AtomicU64::new(0)),
    ));
    let chain = AuthChain::new(meta.clone(), crypto, clock, cache, false);
    (chain, meta)
}

/// Like [`setup`] but returns the shared auth epoch so a test can simulate what the metadata layer
/// does on a user mutation (bump the epoch), proving it gates cache invalidation.
async fn setup_with_epoch() -> (AuthChain, Arc<InMemoryMetadataStore>, Arc<AtomicU64>) {
    let meta = Arc::new(InMemoryMetadataStore::new());
    let crypto = Arc::new(StubCrypto);
    let clock = Arc::new(TestClock::at_secs(1_440_938_160));
    let sealed = crypto.seal(SECRET.as_bytes()).unwrap();
    let user = User {
        id: cairn_types::UserId("u1".to_owned()),
        display_name: "alice".to_owned(),
        access_key_id: "bearer-key".to_owned(),
        sigv4_access_key_id: Some(AKID.to_owned()),
        role: Role::Member,
        is_active: true,
        quota_bytes: None,
        created_at: Timestamp(0),
        updated_at: Timestamp(0),
    };
    meta.submit(Mutation::CreateUser(Box::new(UserRecord {
        user,
        bearer_secret_hash: hash_bearer_secret("topsecret"),
        sigv4_secret_ciphertext: Some(sealed.ciphertext),
        sigv4_secret_nonce: Some(sealed.nonce.0),
    })))
    .await
    .unwrap();

    let epoch = Arc::new(AtomicU64::new(0));
    let cache = Arc::new(AuthCache::new(Duration::from_secs(60), epoch.clone()));
    let chain = AuthChain::new(meta.clone(), crypto, clock, cache, false);
    (chain, meta, epoch)
}

/// Sign a request the way a client would, returning the Authorization header value.
fn sign(method: &str, path: &str, headers: &[(String, String)], payload_hash: &str) -> String {
    let amzdate = "20150830T123600Z";
    let scope_date = "20150830";
    let region = "us-east-1";
    let mut signed: Vec<(String, String)> = headers.to_vec();
    signed.sort();
    let names: Vec<String> = signed.iter().map(|(n, _)| n.clone()).collect();
    let signed_names = names.join(";");
    let cr = cairn_auth::canonical_request(method, path, "", &signed, &signed_names, payload_hash);
    let sts = cairn_auth::string_to_sign(
        amzdate,
        &format!("{scope_date}/{region}/s3/aws4_request"),
        &cr,
    );
    let key = signing_key(SECRET, scope_date, region, "s3");
    let sig = compute_signature(&key, &sts);
    format!(
        "AWS4-HMAC-SHA256 Credential={AKID}/{scope_date}/{region}/s3/aws4_request, \
         SignedHeaders={signed_names}, Signature={sig}"
    )
}

fn view<'a>(headers: &'a [(String, String)], host: &'a str) -> RequestView<'a> {
    RequestView {
        method: "GET",
        path: "/bucket/key",
        query: "",
        headers,
        host,
        source: IpAddr::V4(Ipv4Addr::LOCALHOST),
        secure_transport: false,
    }
}

#[tokio::test]
async fn sigv4_header_roundtrip_authenticates() {
    let (chain, _) = setup().await;
    let host = "s3.example.com";
    let payload = cairn_auth::sha256_hex(b"");
    let base = vec![
        ("host".to_owned(), host.to_owned()),
        ("x-amz-date".to_owned(), "20150830T123600Z".to_owned()),
        ("x-amz-content-sha256".to_owned(), payload.clone()),
    ];
    let auth = sign("GET", "/bucket/key", &base, &payload);

    let mut headers = base.clone();
    headers.push(("authorization".to_owned(), auth));
    let v = view(&headers, host);

    match chain.authenticate(&v).await {
        AuthOutcome::Authenticated(p) => {
            assert_eq!(p.access_key_id, AKID);
            assert_eq!(p.method, AuthMethod::SigV4Header);
            assert_eq!(p.display_name, "alice");
            // A non-streaming body carries no signed-streaming context.
            assert!(p.chunk_signing.is_none());
        }
        other => panic!("expected authenticated, got {other:?}"),
    }
}

#[tokio::test]
async fn deactivation_takes_effect_only_after_epoch_bump() {
    // The security-critical property of the auth cache: a credential change (here, deactivation)
    // is honored the moment the shared epoch is bumped, and the epoch is the load-bearing signal.
    let (chain, meta, epoch) = setup_with_epoch().await;
    let host = "s3.example.com";
    let payload = cairn_auth::sha256_hex(b"");
    let base = vec![
        ("host".to_owned(), host.to_owned()),
        ("x-amz-date".to_owned(), "20150830T123600Z".to_owned()),
        ("x-amz-content-sha256".to_owned(), payload.clone()),
    ];
    let auth = sign("GET", "/bucket/key", &base, &payload);
    let mut headers = base.clone();
    headers.push(("authorization".to_owned(), auth));
    let v = view(&headers, host);

    // First request authenticates and populates the cache.
    assert!(matches!(
        chain.authenticate(&v).await,
        AuthOutcome::Authenticated(_)
    ));

    // Deactivate the user in the store but do NOT bump the epoch: the cached (active) credential is
    // still served, demonstrating the cache is real and the epoch is what gates it.
    meta.submit(Mutation::DeactivateUser(cairn_types::UserId(
        "u1".to_owned(),
    )))
    .await
    .unwrap();
    assert!(
        matches!(chain.authenticate(&v).await, AuthOutcome::Authenticated(_)),
        "without an epoch bump the cached credential is still served"
    );

    // Bump the epoch as the metadata layer does on a user mutation: the stale entry is dropped, the
    // fresh lookup finds an inactive user, and authentication is now denied.
    epoch.fetch_add(1, std::sync::atomic::Ordering::Release);
    assert!(
        matches!(chain.authenticate(&v).await, AuthOutcome::Denied(_)),
        "after the epoch bump the deactivation must take effect"
    );
}

#[tokio::test]
async fn sigv4_streaming_header_populates_chunk_signing_context() {
    let (chain, _) = setup().await;
    let host = "s3.example.com";
    // The streaming sentinel is what is signed as the canonical payload hash.
    let sentinel = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
    let base = vec![
        ("host".to_owned(), host.to_owned()),
        ("x-amz-date".to_owned(), "20150830T123600Z".to_owned()),
        ("x-amz-content-sha256".to_owned(), sentinel.to_owned()),
    ];
    let auth = sign("PUT", "/bucket/key", &base, sentinel);

    let mut headers = base.clone();
    headers.push(("authorization".to_owned(), auth.clone()));
    let mut v = view(&headers, host);
    v.method = "PUT";

    match chain.authenticate(&v).await {
        AuthOutcome::Authenticated(p) => {
            let ctx = p
                .chunk_signing
                .expect("streaming sentinel should populate chunk_signing");
            // The seed is the request signature (the chain's first prev_signature) and the scope
            // and amz-date thread through verbatim.
            assert_eq!(ctx.scope, "20150830/us-east-1/s3/aws4_request");
            assert_eq!(ctx.amz_date, "20150830T123600Z");
            assert_eq!(
                ctx.signing_key,
                cairn_auth::streaming_signing_key(SECRET, "20150830", "us-east-1")
            );
            // The seed signature is the same value carried in the Authorization header.
            assert!(auth.ends_with(&format!("Signature={}", ctx.seed_signature)));
        }
        other => panic!("expected authenticated, got {other:?}"),
    }
}

#[tokio::test]
async fn tampered_signature_is_denied() {
    let (chain, _) = setup().await;
    let host = "s3.example.com";
    let payload = cairn_auth::sha256_hex(b"");
    let base = vec![
        ("host".to_owned(), host.to_owned()),
        ("x-amz-date".to_owned(), "20150830T123600Z".to_owned()),
        ("x-amz-content-sha256".to_owned(), payload.clone()),
    ];
    let mut auth = sign("GET", "/bucket/key", &base, &payload);
    auth.pop();
    auth.push('0'); // corrupt the last signature hex digit

    let mut headers = base.clone();
    headers.push(("authorization".to_owned(), auth));
    let v = view(&headers, host);
    assert!(matches!(
        chain.authenticate(&v).await,
        AuthOutcome::Denied(_)
    ));
}

#[tokio::test]
async fn bearer_roundtrip_and_anonymous() {
    let (chain, _) = setup().await;
    let headers = vec![(
        "authorization".to_owned(),
        "Bearer bearer-key.topsecret".to_owned(),
    )];
    let v = view(&headers, "s3.example.com");
    match chain.authenticate(&v).await {
        AuthOutcome::Authenticated(p) => {
            assert_eq!(p.method, AuthMethod::Bearer);
            assert!(p.chunk_signing.is_none());
        }
        other => panic!("expected bearer auth, got {other:?}"),
    }

    // No credentials => anonymous (NotApplicable).
    let none: Vec<(String, String)> = vec![];
    let v = view(&none, "s3.example.com");
    assert!(matches!(
        chain.authenticate(&v).await,
        AuthOutcome::NotApplicable
    ));

    // Wrong bearer secret => denied.
    let headers = vec![(
        "authorization".to_owned(),
        "Bearer bearer-key.wrong".to_owned(),
    )];
    let v = view(&headers, "s3.example.com");
    assert!(matches!(
        chain.authenticate(&v).await,
        AuthOutcome::Denied(_)
    ));
}

// ===========================================================================================
// STS-style temporary session credentials (ARCH 14)
// ===========================================================================================

const SESSION_AKID: &str = "CAIRNTMPEXAMPLE";
const SESSION_SECRET: &str = "sessionSecretsessionSecretsessionSecret0";
const SESSION_TOKEN: &str = "opaque-session-token-value-1234567890ABC";
// The scoped inline policy: a single statement (proves the session uses THIS, not the parent's).
const SESSION_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::scoped/*"}]}"#;
// The parent's distinct policy has TWO statements, so a session that wrongly inherited it would be
// observable by statement count.
const PARENT_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"},{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}"#;

async fn setup_session(expires_at: Timestamp, parent_active: bool) -> AuthChain {
    use cairn_types::UserId;
    let meta = Arc::new(InMemoryMetadataStore::new());
    let crypto = Arc::new(StubCrypto);
    let clock = Arc::new(TestClock::at_secs(1_440_938_160)); // 2015-08-30T12:36:00Z

    let parent = User {
        id: UserId("parent".to_owned()),
        display_name: "parent".to_owned(),
        access_key_id: "parent-bearer".to_owned(),
        sigv4_access_key_id: None,
        role: Role::Administrator, // an ADMIN parent — the session must still be least-privilege.
        is_active: parent_active,
        quota_bytes: None,
        created_at: Timestamp(0),
        updated_at: Timestamp(0),
    };
    meta.submit(Mutation::CreateUser(Box::new(UserRecord {
        user: parent,
        bearer_secret_hash: hash_bearer_secret("x"),
        sigv4_secret_ciphertext: None,
        sigv4_secret_nonce: None,
    })))
    .await
    .unwrap();
    meta.submit(Mutation::SetUserPolicy {
        user_id: UserId("parent".to_owned()),
        policy: Some(PARENT_POLICY.to_owned()),
    })
    .await
    .unwrap();

    let sealed = crypto.seal(SESSION_SECRET.as_bytes()).unwrap();
    meta.submit(Mutation::CreateSessionCredential(Box::new(
        cairn_types::SessionCredentialRecord {
            access_key_id: SESSION_AKID.to_owned(),
            parent_user_id: UserId("parent".to_owned()),
            secret_ciphertext: sealed.ciphertext,
            secret_nonce: Some(sealed.nonce.0),
            session_token_hash: cairn_auth::hash_session_token(SESSION_TOKEN),
            inline_policy: Some(SESSION_POLICY.to_owned()),
            expires_at,
            created_at: Timestamp(0),
        },
    )))
    .await
    .unwrap();

    let cache = Arc::new(AuthCache::new(
        Duration::from_secs(60),
        Arc::new(AtomicU64::new(0)),
    ));
    AuthChain::new(meta, crypto, clock, cache, false)
}

/// Build a presigned URL signed with the session secret, folding the security token into the
/// (signed) canonical query. `token` is the value placed in `X-Amz-Security-Token` (None omits it).
fn session_presigned_view<'a>(buf: &'a mut String, token: Option<&str>) -> RequestView<'a> {
    use cairn_auth::{PresignRequest, mint_presigned};
    let extra_query = match token {
        Some(t) => vec![("X-Amz-Security-Token".to_owned(), t.to_owned())],
        None => vec![],
    };
    let url = mint_presigned(&PresignRequest {
        method: "GET",
        host: "cairn.example.com",
        bucket: "scoped",
        key: "obj.txt",
        access_key_id: SESSION_AKID,
        secret: SESSION_SECRET,
        region: "us-east-1",
        expires_secs: 3600,
        amz_date: "20150830T123600Z",
        extra_query,
        extra_signed_headers: vec![],
    });
    *buf = url;
    let (path, query) = buf.split_once('?').unwrap();
    RequestView {
        method: "GET",
        path,
        query,
        headers: &[],
        host: "cairn.example.com",
        source: IpAddr::V4(Ipv4Addr::LOCALHOST),
        secure_transport: true,
    }
}

#[tokio::test]
async fn session_credential_authenticates_least_privilege() {
    let chain = setup_session(Timestamp::from_secs(9_999_999_999), true).await;
    let mut buf = String::new();
    let view = session_presigned_view(&mut buf, Some(SESSION_TOKEN));
    match chain.authenticate(&view).await {
        AuthOutcome::Authenticated(p) => {
            assert!(p.is_session, "marked as a session principal");
            assert_eq!(
                p.role,
                Role::Member,
                "role capped to Member (never the admin parent's)"
            );
            assert_eq!(
                p.user_id.0, "parent",
                "carries the parent identity for ownership/audit"
            );
            // The session carries its OWN scoped policy (1 statement), not the parent's (2).
            let pol = p.user_policy.expect("scoped policy attached");
            assert_eq!(
                pol.statements.len(),
                1,
                "session uses the inline policy, not the parent's"
            );
        }
        other => panic!("expected authenticated session, got {other:?}"),
    }
}

#[tokio::test]
async fn session_credential_denied_without_token() {
    let chain = setup_session(Timestamp::from_secs(9_999_999_999), true).await;
    let mut buf = String::new();
    let view = session_presigned_view(&mut buf, None);
    assert!(
        matches!(chain.authenticate(&view).await, AuthOutcome::Denied(_)),
        "a session key with no security token is denied"
    );
}

#[tokio::test]
async fn session_credential_denied_wrong_token() {
    let chain = setup_session(Timestamp::from_secs(9_999_999_999), true).await;
    let mut buf = String::new();
    // The wrong token is signed into the query (so the signature verifies), but its hash mismatches.
    let view = session_presigned_view(&mut buf, Some("not-the-real-token"));
    assert!(
        matches!(chain.authenticate(&view).await, AuthOutcome::Denied(_)),
        "a mismatched security token is denied"
    );
}

#[tokio::test]
async fn session_credential_denied_after_expiry() {
    // Session expired one second before the (fixed) clock; the presign itself is still in-window.
    let chain = setup_session(Timestamp::from_secs(1_440_938_159), true).await;
    let mut buf = String::new();
    let view = session_presigned_view(&mut buf, Some(SESSION_TOKEN));
    assert!(
        matches!(chain.authenticate(&view).await, AuthOutcome::Denied(_)),
        "an expired session credential is denied"
    );
}

#[tokio::test]
async fn session_credential_denied_when_parent_deactivated() {
    let chain = setup_session(Timestamp::from_secs(9_999_999_999), false).await;
    let mut buf = String::new();
    let view = session_presigned_view(&mut buf, Some(SESSION_TOKEN));
    assert!(
        matches!(chain.authenticate(&view).await, AuthOutcome::Denied(_)),
        "a session whose parent account is deactivated is denied"
    );
}

// -------------------------------------------------------------------------------------------
// AWS-STS minting auth (`authenticate_sts`, ARCH 14)
// -------------------------------------------------------------------------------------------

/// The STS form body a client would POST (the exact bytes are what the signature binds).
const STS_BODY: &str = "Action=GetSessionToken&Version=2011-06-15&DurationSeconds=3600";

/// Sign a `POST /` STS request the way a non-S3 SDK signer does: the credential scope service is
/// `sts` and the payload hash (the sha256 of the form body) is folded into the canonical request
/// **without** an `x-amz-content-sha256` header. Returns the Authorization header value. `service`
/// is a parameter only so a test can forge an `s3`-scoped signature onto the STS path.
fn sign_sts(headers: &[(String, String)], body: &[u8], service: &str) -> String {
    let amzdate = "20150830T123600Z";
    let scope_date = "20150830";
    let region = "us-east-1";
    let payload_hash = cairn_auth::sha256_hex(body);
    let mut signed: Vec<(String, String)> = headers.to_vec();
    signed.sort();
    let names: Vec<String> = signed.iter().map(|(n, _)| n.clone()).collect();
    let signed_names = names.join(";");
    let cr = cairn_auth::canonical_request("POST", "/", "", &signed, &signed_names, &payload_hash);
    let sts = cairn_auth::string_to_sign(
        amzdate,
        &format!("{scope_date}/{region}/{service}/aws4_request"),
        &cr,
    );
    let key = signing_key(SECRET, scope_date, region, service);
    let sig = compute_signature(&key, &sts);
    format!(
        "AWS4-HMAC-SHA256 Credential={AKID}/{scope_date}/{region}/{service}/aws4_request, \
         SignedHeaders={signed_names}, Signature={sig}"
    )
}

/// A `POST /` STS request view carrying `headers`.
fn sts_view<'a>(headers: &'a [(String, String)], host: &'a str) -> RequestView<'a> {
    RequestView {
        method: "POST",
        path: "/",
        query: "",
        headers,
        host,
        source: IpAddr::V4(Ipv4Addr::LOCALHOST),
        secure_transport: false,
    }
}

fn sts_signed_headers(host: &str) -> Vec<(String, String)> {
    vec![
        (
            "content-type".to_owned(),
            "application/x-www-form-urlencoded".to_owned(),
        ),
        ("host".to_owned(), host.to_owned()),
        ("x-amz-date".to_owned(), "20150830T123600Z".to_owned()),
    ]
}

#[tokio::test]
async fn authenticate_sts_accepts_sts_scoped_long_term_key() {
    let (chain, _) = setup().await;
    let host = "sts.example.com";
    let base = sts_signed_headers(host);
    let auth = sign_sts(&base, STS_BODY.as_bytes(), "sts");
    let mut headers = base.clone();
    headers.push(("authorization".to_owned(), auth));
    let v = sts_view(&headers, host);
    match chain
        .authenticate_sts(&v, &cairn_auth::sha256_hex(STS_BODY.as_bytes()))
        .await
    {
        AuthOutcome::Authenticated(p) => {
            assert_eq!(p.access_key_id, AKID);
            assert!(
                !p.is_session,
                "the STS caller is a long-term principal, not a session"
            );
        }
        other => panic!("expected authenticated, got {other:?}"),
    }
}

#[tokio::test]
async fn authenticate_sts_rejects_tampered_body() {
    // The signature binds the buffered body hash; presenting a different body hash must fail closed.
    let (chain, _) = setup().await;
    let host = "sts.example.com";
    let base = sts_signed_headers(host);
    let auth = sign_sts(&base, STS_BODY.as_bytes(), "sts");
    let mut headers = base.clone();
    headers.push(("authorization".to_owned(), auth));
    let v = sts_view(&headers, host);
    let tampered = cairn_auth::sha256_hex(b"Action=AssumeRole&DurationSeconds=43200");
    assert!(
        matches!(
            chain.authenticate_sts(&v, &tampered).await,
            AuthOutcome::Denied(_)
        ),
        "a body hash that does not match the signature is denied"
    );
}

#[tokio::test]
async fn authenticate_sts_rejects_s3_scope() {
    // An s3-scoped signature must not authenticate on the STS surface.
    let (chain, _) = setup().await;
    let host = "sts.example.com";
    let base = sts_signed_headers(host);
    let auth = sign_sts(&base, STS_BODY.as_bytes(), "s3");
    let mut headers = base.clone();
    headers.push(("authorization".to_owned(), auth));
    let v = sts_view(&headers, host);
    assert!(matches!(
        chain
            .authenticate_sts(&v, &cairn_auth::sha256_hex(STS_BODY.as_bytes()))
            .await,
        AuthOutcome::Denied(cairn_types::error::AuthError::Malformed)
    ));
}

#[tokio::test]
async fn authenticate_sts_rejects_unsigned_host() {
    // `host` must be among the signed headers (binding the request to the authority).
    let (chain, _) = setup().await;
    let host = "sts.example.com";
    let base = vec![
        (
            "content-type".to_owned(),
            "application/x-www-form-urlencoded".to_owned(),
        ),
        ("x-amz-date".to_owned(), "20150830T123600Z".to_owned()),
    ];
    let auth = sign_sts(&base, STS_BODY.as_bytes(), "sts");
    let mut headers = base.clone();
    headers.push(("host".to_owned(), host.to_owned()));
    headers.push(("authorization".to_owned(), auth));
    let v = sts_view(&headers, host);
    assert!(matches!(
        chain
            .authenticate_sts(&v, &cairn_auth::sha256_hex(STS_BODY.as_bytes()))
            .await,
        AuthOutcome::Denied(cairn_types::error::AuthError::Malformed)
    ));
}

#[tokio::test]
async fn authenticate_sts_rejects_session_key_no_chaining() {
    // A CAIRNTMP session credential is NOT a long-term key: `authenticate_sts` never consults the
    // session-key table, so a session cannot mint another session (no credential chaining).
    let chain = setup_session(Timestamp::from_secs(9_999_999_999), true).await;
    let host = "sts.example.com";
    let amzdate = "20150830T123600Z";
    let scope_date = "20150830";
    let region = "us-east-1";
    let base = sts_signed_headers(host);
    let mut signed = base.clone();
    signed.sort();
    let names: Vec<String> = signed.iter().map(|(n, _)| n.clone()).collect();
    let signed_names = names.join(";");
    let payload_hash = cairn_auth::sha256_hex(STS_BODY.as_bytes());
    let cr = cairn_auth::canonical_request("POST", "/", "", &signed, &signed_names, &payload_hash);
    let sts = cairn_auth::string_to_sign(
        amzdate,
        &format!("{scope_date}/{region}/sts/aws4_request"),
        &cr,
    );
    let key = signing_key(SESSION_SECRET, scope_date, region, "sts");
    let sig = compute_signature(&key, &sts);
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={SESSION_AKID}/{scope_date}/{region}/sts/aws4_request, \
         SignedHeaders={signed_names}, Signature={sig}"
    );
    let mut headers = base.clone();
    headers.push(("authorization".to_owned(), auth));
    let v = sts_view(&headers, host);
    assert!(matches!(
        chain.authenticate_sts(&v, &payload_hash).await,
        AuthOutcome::Denied(cairn_types::error::AuthError::UnknownKey)
    ));
}

#[tokio::test]
async fn sts_scope_on_generic_s3_chain_is_denied() {
    // The dangerous refactor direction: an sts-scoped signature must NEVER authenticate a normal S3
    // operation through the generic chain (`authenticate`). It is rejected as Malformed.
    let (chain, _) = setup().await;
    let host = "s3.example.com";
    let payload = cairn_auth::sha256_hex(b"");
    let base = vec![
        ("host".to_owned(), host.to_owned()),
        ("x-amz-date".to_owned(), "20150830T123600Z".to_owned()),
        ("x-amz-content-sha256".to_owned(), payload.clone()),
    ];
    // Sign a GET /bucket/key but with the `sts` service in the credential scope.
    let amzdate = "20150830T123600Z";
    let scope_date = "20150830";
    let region = "us-east-1";
    let mut signed = base.clone();
    signed.sort();
    let names: Vec<String> = signed.iter().map(|(n, _)| n.clone()).collect();
    let signed_names = names.join(";");
    let cr =
        cairn_auth::canonical_request("GET", "/bucket/key", "", &signed, &signed_names, &payload);
    let sts = cairn_auth::string_to_sign(
        amzdate,
        &format!("{scope_date}/{region}/sts/aws4_request"),
        &cr,
    );
    let key = signing_key(SECRET, scope_date, region, "sts");
    let sig = compute_signature(&key, &sts);
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={AKID}/{scope_date}/{region}/sts/aws4_request, \
         SignedHeaders={signed_names}, Signature={sig}"
    );
    let mut headers = base.clone();
    headers.push(("authorization".to_owned(), auth));
    let v = view(&headers, host);
    assert!(matches!(
        chain.authenticate(&v).await,
        AuthOutcome::Denied(cairn_types::error::AuthError::Malformed)
    ));
}
