//! `cairn-auth` — the authenticator chain (ARCH 14). It composes the Bearer, SigV4 header,
//! SigV4 presigned, and (debug-only) development schemes into an ordered chain whose first
//! applicable outcome decides. SigV4 and Bearer verification live here; the streaming
//! chunk-signature primitives are consumed by the ingest decoder in `cairn-protocol`.

#![forbid(unsafe_code)]

mod bearer;
mod cache;
mod chunked;
mod crypto_util;
mod sigv4;

pub use bearer::{hash_bearer_secret, parse_bearer};
pub use cache::AuthCache;
pub use chunked::{chunk_string_to_sign, next_chunk_signature, streaming_signing_key};
pub use crypto_util::sha256_hex;
pub use sigv4::{
    ParsedSig, PresignRequest, canonical_request, compute_signature, mint_presigned, signing_key,
    string_to_sign,
};

use async_trait::async_trait;
use cache::{CachedBearer, CachedSigv4};
use cairn_types::auth::{AuthMethod, AuthOutcome, Principal, RequestView, Role};
use cairn_types::crypto::Nonce;
use cairn_types::error::AuthError;
use cairn_types::id::UserId;
use cairn_types::traits::{Authenticator, Clock, Crypto, MetadataStore};
use std::sync::Arc;

/// The composed authenticator chain. Holds the metadata store (for credential lookup), the
/// crypto facility (to decrypt SigV4 secrets), the clock (for skew validation), and the
/// short-lived authentication cache (credential + parsed-policy memoization, ARCH 30).
#[derive(Clone)]
pub struct AuthChain {
    meta: Arc<dyn MetadataStore>,
    crypto: Arc<dyn Crypto>,
    clock: Arc<dyn Clock>,
    cache: Arc<AuthCache>,
    dev_enabled: bool,
}

impl std::fmt::Debug for AuthChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthChain")
            .field("dev_enabled", &self.dev_enabled)
            .field("cache", &self.cache)
            .finish_non_exhaustive()
    }
}

impl AuthChain {
    /// Build a chain. `dev_enabled` only has effect when the crate is built with the `dev-auth`
    /// feature (release builds compile the bypass out entirely). `cache` memoizes the per-request
    /// credential lookup and parsed identity policy; pass an [`AuthCache`] with a zero TTL to
    /// disable it.
    pub fn new(
        meta: Arc<dyn MetadataStore>,
        crypto: Arc<dyn Crypto>,
        clock: Arc<dyn Clock>,
        cache: Arc<AuthCache>,
        dev_enabled: bool,
    ) -> Self {
        Self {
            meta,
            crypto,
            clock,
            cache,
            dev_enabled,
        }
    }

    /// The SigV4 identity for `access_key_id`, preferring the cache and falling back to a metadata
    /// read. Returns `None` for an unknown or inactive key (the caller maps that to `UnknownKey`).
    /// Only active users are cached; deactivation is handled by the shared epoch invalidation.
    async fn sigv4_creds(&self, access_key_id: &str) -> Option<CachedSigv4> {
        if let Some(c) = self.cache.get_sigv4(access_key_id) {
            return Some(c);
        }
        let observed = self.cache.observe_epoch();
        let fetched = match self.meta.user_by_sigv4_key(access_key_id).await {
            Ok(Some(c)) if c.user.is_active => c,
            _ => return None,
        };
        let cached = CachedSigv4 {
            user_id: fetched.user.id,
            display_name: fetched.user.display_name,
            role: fetched.user.role,
            secret_ciphertext: fetched.secret_ciphertext,
            secret_nonce: fetched.secret_nonce,
        };
        self.cache
            .put_sigv4(access_key_id, cached.clone(), observed);
        Some(cached)
    }

    /// The Bearer identity for `access_key_id`, preferring the cache. Returns `None` for an unknown
    /// or inactive key. Only active users are cached.
    async fn bearer_creds(&self, access_key_id: &str) -> Option<CachedBearer> {
        if let Some(c) = self.cache.get_bearer(access_key_id) {
            return Some(c);
        }
        let observed = self.cache.observe_epoch();
        let fetched = match self.meta.user_by_bearer_key(access_key_id).await {
            Ok(Some(c)) if c.user.is_active => c,
            _ => return None,
        };
        let cached = CachedBearer {
            user_id: fetched.user.id,
            display_name: fetched.user.display_name,
            role: fetched.user.role,
            secret_hash: fetched.secret_hash,
        };
        self.cache
            .put_bearer(access_key_id, cached.clone(), observed);
        Some(cached)
    }

    async fn verify_sigv4_header(&self, view: &RequestView<'_>, header: &str) -> AuthOutcome {
        let Some(parsed) = sigv4::parse_authorization_header(header) else {
            return AuthOutcome::Denied(AuthError::Malformed);
        };
        let Some(creds) = self.sigv4_creds(&parsed.access_key_id).await else {
            return AuthOutcome::Denied(AuthError::UnknownKey);
        };
        // Decrypt the sealed secret per request (the plaintext is never cached) and re-derive the
        // signing key inside `verify_header`, so the verification math is unchanged by caching.
        let secret = match self
            .crypto
            .open(&creds.secret_ciphertext, &Nonce(creds.secret_nonce.clone()))
        {
            Ok(s) => s,
            Err(_) => return AuthOutcome::Denied(AuthError::UnknownKey),
        };
        let secret = String::from_utf8_lossy(&secret).into_owned();
        match sigv4::verify_header(view, &parsed, &secret, self.clock.now()) {
            Ok(auth) => AuthOutcome::Authenticated(sigv4::principal(
                creds.user_id,
                creds.display_name,
                parsed.access_key_id,
                creds.role,
                auth.method,
                auth.chunk_signing,
            )),
            Err(e) => AuthOutcome::Denied(e),
        }
    }

    async fn verify_sigv4_presigned(&self, view: &RequestView<'_>) -> AuthOutcome {
        let Some((parsed, expires)) = sigv4::parse_presigned(view.query) else {
            return AuthOutcome::Denied(AuthError::Malformed);
        };
        let Some(creds) = self.sigv4_creds(&parsed.access_key_id).await else {
            return AuthOutcome::Denied(AuthError::UnknownKey);
        };
        let secret = match self
            .crypto
            .open(&creds.secret_ciphertext, &Nonce(creds.secret_nonce.clone()))
        {
            Ok(s) => s,
            Err(_) => return AuthOutcome::Denied(AuthError::UnknownKey),
        };
        let secret = String::from_utf8_lossy(&secret).into_owned();
        match sigv4::verify_presigned(view, &parsed, expires, &secret, self.clock.now()) {
            // Presigned requests sign a fixed payload hash (`UNSIGNED-PAYLOAD`); they never carry
            // a streaming chunk chain, so there is no signed-streaming context.
            Ok(method) => AuthOutcome::Authenticated(sigv4::principal(
                creds.user_id,
                creds.display_name,
                parsed.access_key_id,
                creds.role,
                method,
                None,
            )),
            Err(e) => AuthOutcome::Denied(e),
        }
    }

    async fn verify_bearer(&self, header: &str) -> AuthOutcome {
        let Some((id, secret)) = parse_bearer(header) else {
            return AuthOutcome::Denied(AuthError::Malformed);
        };
        let Some(creds) = self.bearer_creds(&id).await else {
            return AuthOutcome::Denied(AuthError::UnknownKey);
        };
        let computed = hash_bearer_secret(&secret);
        if self
            .crypto
            .ct_eq(computed.as_bytes(), creds.secret_hash.as_bytes())
        {
            AuthOutcome::Authenticated(Principal {
                user_id: creds.user_id,
                display_name: creds.display_name,
                access_key_id: id,
                role: creds.role,
                method: AuthMethod::Bearer,
                // Bearer auth has no SigV4 streaming chain.
                chunk_signing: None,
                // Filled in by `attach_policy` at the authenticate() chokepoint.
                user_policy: None,
            })
        } else {
            AuthOutcome::Denied(AuthError::SignatureMismatch)
        }
    }
}

#[async_trait]
impl Authenticator for AuthChain {
    async fn authenticate(&self, view: &RequestView<'_>) -> AuthOutcome {
        // Every successful authentication is funnelled through one chokepoint that loads the user's
        // identity policy, so each auth method (bearer, SigV4 header/presigned, dev) gets it.
        match self.classify(view).await {
            AuthOutcome::Authenticated(p) => {
                AuthOutcome::Authenticated(self.attach_policy(p).await)
            }
            other => other,
        }
    }
}

impl AuthChain {
    /// Decide the auth outcome by method, without loading the identity policy (that is done once by
    /// [`Authenticator::authenticate`]). Preserves the original dispatch precedence.
    async fn classify(&self, view: &RequestView<'_>) -> AuthOutcome {
        if let Some(header) = view.header("authorization") {
            if header.starts_with("AWS4-HMAC-SHA256") {
                return self.verify_sigv4_header(view, header).await;
            }
            if header.starts_with("Bearer ") {
                return self.verify_bearer(header).await;
            }
        }
        if view.query.contains("X-Amz-Algorithm") {
            return self.verify_sigv4_presigned(view).await;
        }
        // The development bypass: compiled in only with `dev-auth`, and only on loopback.
        if cfg!(feature = "dev-auth") && self.dev_enabled && view.source.is_loopback() {
            return AuthOutcome::Authenticated(dev_principal());
        }
        AuthOutcome::NotApplicable
    }

    /// Load and attach the user's identity (per-user) policy (ARCH 15 / user-centric authz). A
    /// malformed stored policy, or a load error, fails closed — the principal proceeds with no
    /// identity policy (no grant), never a silently widened one.
    async fn attach_policy(&self, mut principal: Principal) -> Principal {
        // Cache hit: attach a clone of the shared parsed policy. The downstream `AuthzInput`
        // deep-clones the policy anyway (`.as_deref().cloned()`), so a `Box` clone here costs no
        // more than today while skipping both the metadata read and the JSON parse.
        if let Some(cached) = self.cache.get_policy(&principal.user_id) {
            principal.user_policy = cached.as_ref().map(|p| Box::new((**p).clone()));
            return principal;
        }
        let observed = self.cache.observe_epoch();
        match self.meta.get_user_policy(&principal.user_id).await {
            Ok(Some(raw)) => match cairn_authz::parse_user_policy(&raw) {
                Ok(policy) => {
                    let arc = Arc::new(policy);
                    self.cache
                        .put_policy(&principal.user_id, Some(arc.clone()), observed);
                    principal.user_policy = Some(Box::new((*arc).clone()));
                }
                Err(_) => {
                    // A malformed stored policy fails closed (no grant) and is remembered as an
                    // absence so a known-bad doc is not re-parsed every request; an operator fix
                    // is a `SetUserPolicy` mutation, which bumps the epoch and drops this entry.
                    tracing::warn!(
                        user_id = %principal.user_id,
                        "ignoring malformed stored user policy (fail-closed)"
                    );
                    self.cache.put_policy(&principal.user_id, None, observed);
                }
            },
            Ok(None) => self.cache.put_policy(&principal.user_id, None, observed),
            // A transient load error must not poison the cache: proceed with no policy and cache
            // nothing, so the next request retries the read.
            Err(e) => tracing::warn!(
                user_id = %principal.user_id, error = ?e,
                "failed to load user policy; proceeding with none"
            ),
        }
        principal
    }
}

fn dev_principal() -> Principal {
    Principal {
        user_id: UserId("dev".to_owned()),
        display_name: "development".to_owned(),
        access_key_id: "dev".to_owned(),
        role: Role::Administrator,
        method: AuthMethod::Development,
        chunk_signing: None,
        user_policy: None,
    }
}
