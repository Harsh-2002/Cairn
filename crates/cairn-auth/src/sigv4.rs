//! SigV4 canonicalization, signing, and verification for the header and presigned-query forms
//! (ARCH §14.1, §14.2). The signing pipeline is validated against the AWS published
//! `get-vanilla` test vector so the canonical request, string-to-sign, signing-key derivation,
//! and signature are all exercised end to end.

use crate::crypto_util::{hmac_sha256, parse_amz_date, percent_decode, sha256_hex, uri_encode};
use cairn_types::auth::{AuthMethod, ChunkSigningContext, Principal, RequestView};
use cairn_types::error::AuthError;
use cairn_types::time::Timestamp;
use subtle::ConstantTimeEq;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SKEW_SECS: i64 = 900;

/// Derive the SigV4 signing key.
#[must_use]
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k = hmac_sha256(&k, region.as_bytes());
    let k = hmac_sha256(&k, service.as_bytes());
    hmac_sha256(&k, b"aws4_request")
}

/// Build the canonical request string. `signed` is the sorted (lowercased-name, value) list.
#[must_use]
pub fn canonical_request(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    signed: &[(String, String)],
    signed_names: &str,
    payload_hash: &str,
) -> String {
    let mut headers = String::new();
    for (n, v) in signed {
        headers.push_str(n);
        headers.push(':');
        headers.push_str(&collapse_ws(v));
        headers.push('\n');
    }
    format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{headers}\n{signed_names}\n{payload_hash}"
    )
}

/// Build the string to sign.
#[must_use]
pub fn string_to_sign(amzdate: &str, scope: &str, canonical_req: &str) -> String {
    format!(
        "{ALGORITHM}\n{amzdate}\n{scope}\n{}",
        sha256_hex(canonical_req.as_bytes())
    )
}

/// Compute the hex signature.
#[must_use]
pub fn compute_signature(key: &[u8; 32], string_to_sign: &str) -> String {
    hex::encode(hmac_sha256(key, string_to_sign.as_bytes()))
}

fn collapse_ws(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    let mut prev_space = false;
    for ch in v.trim().chars() {
        if ch == ' ' || ch == '\t' {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out
}

/// The canonical query string: each parameter name/value decoded then re-encoded, sorted,
/// excluding `exclude` (used to drop `X-Amz-Signature` for presigned verification).
#[must_use]
pub fn canonical_query(query: &str, exclude: Option<&str>) -> String {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for part in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        let kd = percent_decode(k);
        if exclude == Some(kd.as_str()) {
            continue;
        }
        pairs.push((uri_encode(&kd, true), uri_encode(&percent_decode(v), true)));
    }
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// A parsed SigV4 `Authorization` header (or presigned-query equivalent).
#[derive(Debug, Clone)]
pub struct ParsedSig {
    pub access_key_id: String,
    pub scope_date: String,
    pub region: String,
    pub service: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

impl ParsedSig {
    /// The credential scope string `date/region/service/aws4_request`.
    #[must_use]
    pub fn scope(&self) -> String {
        format!(
            "{}/{}/{}/aws4_request",
            self.scope_date, self.region, self.service
        )
    }
}

/// Parse an `AWS4-HMAC-SHA256` Authorization header.
#[must_use]
pub fn parse_authorization_header(header: &str) -> Option<ParsedSig> {
    let rest = header.strip_prefix(ALGORITHM)?.trim_start();
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for field in rest.split(',') {
        let field = field.trim();
        if let Some(v) = field.strip_prefix("Credential=") {
            credential = Some(v.to_owned());
        } else if let Some(v) = field.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v.to_owned());
        } else if let Some(v) = field.strip_prefix("Signature=") {
            signature = Some(v.to_owned());
        }
    }
    let cred = credential?;
    let parts: Vec<&str> = cred.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return None;
    }
    Some(ParsedSig {
        access_key_id: parts[0].to_owned(),
        scope_date: parts[1].to_owned(),
        region: parts[2].to_owned(),
        service: parts[3].to_owned(),
        signed_headers: signed_headers?.split(';').map(str::to_owned).collect(),
        signature: signature?,
    })
}

/// Parse the presigned-query parameters into a [`ParsedSig`] plus the expiry seconds.
#[must_use]
pub fn parse_presigned(query: &str) -> Option<(ParsedSig, i64)> {
    let mut params = std::collections::HashMap::new();
    for part in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        params.insert(percent_decode(k), percent_decode(v));
    }
    if params.get("X-Amz-Algorithm").map(String::as_str) != Some(ALGORITHM) {
        return None;
    }
    let cred = params.get("X-Amz-Credential")?;
    let parts: Vec<&str> = cred.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return None;
    }
    let expires: i64 = params.get("X-Amz-Expires")?.parse().ok()?;
    Some((
        ParsedSig {
            access_key_id: parts[0].to_owned(),
            scope_date: parts[1].to_owned(),
            region: parts[2].to_owned(),
            service: parts[3].to_owned(),
            signed_headers: params
                .get("X-Amz-SignedHeaders")?
                .split(';')
                .map(str::to_owned)
                .collect(),
            signature: params.get("X-Amz-Signature")?.clone(),
        },
        expires,
    ))
}

/// Collect the signed (name, value) header pairs in sorted order, from the request view.
fn signed_header_pairs(view: &RequestView<'_>, names: &[String]) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = names
        .iter()
        .map(|n| {
            let name = n.to_ascii_lowercase();
            let value = if name == "host" {
                view.header("host")
                    .map(str::to_owned)
                    .unwrap_or_else(|| view.host.to_owned())
            } else {
                view.header(&name).unwrap_or("").to_owned()
            };
            (name, value)
        })
        .collect();
    pairs.sort();
    pairs
}

/// The streaming-payload sentinel: the body is an `aws-chunked` stream whose per-chunk signature
/// chain is seeded by this request's header signature (ARCH §14.3, §21.7).
const STREAMING_SENTINEL: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// The outcome of a successful header-form SigV4 verification: the auth method and, when the
/// body is a signed chunk stream, the context the ingest decoder seeds its chain from.
#[derive(Debug)]
pub struct HeaderAuth {
    /// The established auth method.
    pub method: AuthMethod,
    /// The signed-streaming context, when `x-amz-content-sha256` is the streaming sentinel.
    pub chunk_signing: Option<ChunkSigningContext>,
}

/// Verify a header-form SigV4 request, given the (decrypted) secret. Returns the established
/// auth method on success, plus a signed-streaming context when the request body is a
/// `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` chunk stream.
pub fn verify_header(
    view: &RequestView<'_>,
    parsed: &ParsedSig,
    secret: &str,
    now: Timestamp,
) -> Result<HeaderAuth, AuthError> {
    if parsed.service != "s3" {
        return Err(AuthError::Malformed);
    }
    if !parsed
        .signed_headers
        .iter()
        .any(|h| h.eq_ignore_ascii_case("host"))
    {
        return Err(AuthError::Malformed);
    }
    let amzdate = view.header("x-amz-date").ok_or(AuthError::Malformed)?;
    check_skew(amzdate, now)?;
    if !amzdate.starts_with(&parsed.scope_date) {
        return Err(AuthError::Malformed);
    }
    let payload_hash = view
        .header("x-amz-content-sha256")
        .unwrap_or("UNSIGNED-PAYLOAD")
        .to_owned();
    let signed = signed_header_pairs(view, &parsed.signed_headers);
    let signed_names = sorted_names(&parsed.signed_headers);
    let cr = canonical_request(
        view.method,
        &uri_encode(&percent_decode(view.path), false),
        &canonical_query(view.query, None),
        &signed,
        &signed_names,
        &payload_hash,
    );
    let sts = string_to_sign(amzdate, &parsed.scope(), &cr);
    let key = signing_key(secret, &parsed.scope_date, &parsed.region, &parsed.service);
    let expected = compute_signature(&key, &sts);
    if expected
        .as_bytes()
        .ct_eq(parsed.signature.as_bytes())
        .into()
    {
        // For the signed-streaming sentinel, hand the ingest decoder the seed signature and the
        // derived streaming signing key so it can verify the rolling per-chunk chain.
        let chunk_signing = (payload_hash == STREAMING_SENTINEL).then(|| ChunkSigningContext {
            seed_signature: expected,
            signing_key: crate::streaming_signing_key(secret, &parsed.scope_date, &parsed.region),
            amz_date: amzdate.to_owned(),
            scope: parsed.scope(),
        });
        Ok(HeaderAuth {
            method: AuthMethod::SigV4Header,
            chunk_signing,
        })
    } else {
        Err(AuthError::SignatureMismatch)
    }
}

/// Verify a presigned-query SigV4 request.
pub fn verify_presigned(
    view: &RequestView<'_>,
    parsed: &ParsedSig,
    expires: i64,
    secret: &str,
    now: Timestamp,
) -> Result<AuthMethod, AuthError> {
    if parsed.service != "s3" {
        return Err(AuthError::Malformed);
    }
    let amzdate = find_query(view.query, "X-Amz-Date").ok_or(AuthError::Malformed)?;
    let start = parse_amz_date(&amzdate).ok_or(AuthError::Malformed)?;
    let expiry = start + expires.clamp(1, 604_800) * 1000;
    if now.as_millis() > expiry {
        return Err(AuthError::Expired);
    }
    if start - now.as_millis() > SKEW_SECS * 1000 {
        return Err(AuthError::SkewedClock);
    }
    let signed = signed_header_pairs(view, &parsed.signed_headers);
    let signed_names = sorted_names(&parsed.signed_headers);
    let cr = canonical_request(
        view.method,
        &uri_encode(&percent_decode(view.path), false),
        &canonical_query(view.query, Some("X-Amz-Signature")),
        &signed,
        &signed_names,
        "UNSIGNED-PAYLOAD",
    );
    let sts = string_to_sign(&amzdate, &parsed.scope(), &cr);
    let key = signing_key(secret, &parsed.scope_date, &parsed.region, &parsed.service);
    let expected = compute_signature(&key, &sts);
    if expected
        .as_bytes()
        .ct_eq(parsed.signature.as_bytes())
        .into()
    {
        Ok(AuthMethod::SigV4Presigned)
    } else {
        Err(AuthError::SignatureMismatch)
    }
}

fn sorted_names(names: &[String]) -> String {
    let mut n: Vec<String> = names.iter().map(|s| s.to_ascii_lowercase()).collect();
    n.sort();
    n.join(";")
}

fn check_skew(amzdate: &str, now: Timestamp) -> Result<(), AuthError> {
    let t = parse_amz_date(amzdate).ok_or(AuthError::Malformed)?;
    if (t - now.as_millis()).abs() > SKEW_SECS * 1000 {
        return Err(AuthError::SkewedClock);
    }
    Ok(())
}

fn find_query(query: &str, key: &str) -> Option<String> {
    for part in query.split('&') {
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        if percent_decode(k) == key {
            return Some(percent_decode(v));
        }
    }
    None
}

/// Build a principal from a verified request and the looked-up user fields. `chunk_signing`
/// carries the signed-streaming context for header-form SigV4 with the streaming sentinel, and is
/// `None` for presigned, non-streaming, or Bearer auth.
#[must_use]
pub fn principal(
    user_id: cairn_types::id::UserId,
    display_name: String,
    access_key_id: String,
    role: cairn_types::auth::Role,
    method: AuthMethod,
    chunk_signing: Option<ChunkSigningContext>,
) -> Principal {
    Principal {
        user_id,
        display_name,
        access_key_id,
        role,
        method,
        chunk_signing,
        // Filled in by `AuthChain::attach_policy` at the authenticate() chokepoint.
        user_policy: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // AWS SigV4 test suite: get-vanilla.
    #[test]
    fn get_vanilla_vector() {
        let signed = vec![
            ("host".to_owned(), "example.amazonaws.com".to_owned()),
            ("x-amz-date".to_owned(), "20150830T123600Z".to_owned()),
        ];
        let cr = canonical_request("GET", "/", "", &signed, "host;x-amz-date", &sha256_hex(b""));
        let sts = string_to_sign(
            "20150830T123600Z",
            "20150830/us-east-1/service/aws4_request",
            &cr,
        );
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "service",
        );
        let sig = compute_signature(&key, &sts);
        assert_eq!(
            sig,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[test]
    fn parse_authorization_header_works() {
        let h = "AWS4-HMAC-SHA256 Credential=AKID/20150830/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;x-amz-date, Signature=abc123";
        let p = parse_authorization_header(h).unwrap();
        assert_eq!(p.access_key_id, "AKID");
        assert_eq!(p.service, "s3");
        assert_eq!(p.signed_headers, vec!["host", "x-amz-date"]);
        assert_eq!(p.signature, "abc123");
        assert_eq!(p.scope(), "20150830/us-east-1/s3/aws4_request");
    }
}
