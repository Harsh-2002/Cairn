//! Integration tests for the authenticator chain against the in-memory doubles: a real SigV4
//! signed request round-trips, a tampered signature is denied, and Bearer works end to end.

use cairn_auth::{AuthChain, compute_signature, hash_bearer_secret, signing_key};
use cairn_types::Timestamp;
use cairn_types::auth::{AuthMethod, AuthOutcome, RequestView, Role};
use cairn_types::meta::{Mutation, User, UserRecord};
use cairn_types::testing::{InMemoryMetadataStore, StubCrypto, TestClock};
use cairn_types::traits::{Authenticator, Crypto, MetadataStore};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

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

    let chain = AuthChain::new(meta.clone(), crypto, clock, false);
    (chain, meta)
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
