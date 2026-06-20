//! Identity and the authenticator contract inputs/outputs. The [`crate::Authenticator`]
//! trait itself lives in `traits.rs`; these are the values that cross its boundary.

use crate::error::AuthError;
use crate::id::UserId;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// A user's role. An administrator is implicitly permitted (subject to an explicit policy
/// deny); a member's access is governed by bucket policy and ACL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Full administrative access to the control plane and all buckets.
    Administrator,
    /// A regular user; access governed by resource policy/ACL.
    Member,
}

/// How a principal authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthMethod {
    /// SigV4 in `Authorization` header form.
    SigV4Header,
    /// SigV4 in presigned-query form.
    SigV4Presigned,
    /// First-party Bearer scheme.
    Bearer,
    /// The development bypass (debug builds, loopback only).
    Development,
    /// A Cairn signed public-read URL.
    PublicUrl,
}

/// The SigV4 signed-streaming context a header-form principal carries when the request body is a
/// `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` chunk stream. The ingest decoder seeds its rolling
/// per-chunk signature chain from [`seed_signature`] and verifies each chunk with the derived
/// [`signing_key`]. Present only for SigV4 header auth with the streaming sentinel; `None`
/// otherwise (presigned, non-streaming, or Bearer).
///
/// [`seed_signature`]: ChunkSigningContext::seed_signature
/// [`signing_key`]: ChunkSigningContext::signing_key
#[derive(Clone, PartialEq, Eq)]
pub struct ChunkSigningContext {
    /// The request signature just computed/verified, seeding the per-chunk chain.
    pub seed_signature: String,
    /// The SigV4 streaming signing key derived from the secret, scope date, and region.
    pub signing_key: [u8; 32],
    /// The request `X-Amz-Date` header value (the `amz-date` each chunk is signed under).
    pub amz_date: String,
    /// The credential scope string `<date>/<region>/s3/aws4_request`.
    pub scope: String,
}

impl std::fmt::Debug for ChunkSigningContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the signing key (it is derived from the user's secret).
        f.debug_struct("ChunkSigningContext")
            .field("seed_signature", &self.seed_signature)
            .field("signing_key", &"<redacted>")
            .field("amz_date", &self.amz_date)
            .field("scope", &self.scope)
            .finish()
    }
}

/// An authenticated identity carried through authorization and into handlers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// The user's stable identifier.
    pub user_id: UserId,
    /// Human-readable display name.
    pub display_name: String,
    /// The access-key id used for this request.
    pub access_key_id: String,
    /// The user's role.
    pub role: Role,
    /// How authentication succeeded.
    pub method: AuthMethod,
    /// The SigV4 signed-streaming context, when the request body is a signed chunk stream.
    pub chunk_signing: Option<ChunkSigningContext>,
    /// The user's attached identity (per-user) policy, parsed and loaded at authentication time
    /// (ARCH 15 / user-centric authz). `None` if the user has no attached policy. Carried into
    /// authorization, where it is evaluated in union with the bucket policy. Boxed so the common
    /// `None` case keeps [`Principal`] (and thus `AuthOutcome`) small.
    pub user_policy: Option<Box<crate::authz::Policy>>,
    /// Whether this principal authenticated with an STS-style **temporary session credential**
    /// (ARCH 14). A session is least-privilege by construction: it NEVER receives the owner or
    /// administrator short-circuit in authorization (even though `user_id`/`role` carry the parent's
    /// identity for ownership and audit), so its access is exactly what its scoped policy grants.
    pub is_session: bool,
}

/// The class of requester, decided by the pipeline before authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequesterClass {
    /// The bucket owner or an administrator (implicitly permitted, save explicit deny).
    OwnerOrAdmin,
    /// An authenticated non-owner member.
    AuthenticatedMember(UserId),
    /// An anonymous (unauthenticated) requester.
    Anonymous,
}

/// The outcome of one authenticator in the chain (the three-valued contract).
#[derive(Debug)]
pub enum AuthOutcome {
    /// This scheme does not apply; the chain tries the next authenticator.
    NotApplicable,
    /// This scheme applied and established a principal.
    Authenticated(Principal),
    /// This scheme applied and failed; the request is denied and the chain stops.
    Denied(AuthError),
}

/// A borrowed, library-neutral view of a request given to authenticators so that no HTTP
/// library type leaks into the auth layer.
#[derive(Debug, Clone)]
pub struct RequestView<'a> {
    /// The HTTP method (uppercase).
    pub method: &'a str,
    /// The decoded request path (the URI path component).
    pub path: &'a str,
    /// Raw query string (without the leading `?`), if any.
    pub query: &'a str,
    /// Request headers as (lowercased-name, value) pairs.
    pub headers: &'a [(String, String)],
    /// The `Host` header value.
    pub host: &'a str,
    /// The source address of the requester (post proxy-header resolution).
    pub source: IpAddr,
    /// Whether the request arrived over a secure transport.
    pub secure_transport: bool,
}

impl RequestView<'_> {
    /// First header value matching `name` (case-insensitive; `name` must be lowercase).
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}
