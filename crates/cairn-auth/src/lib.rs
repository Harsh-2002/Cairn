//! `cairn-auth` — the authenticator chain (ARCH §14). It composes the Bearer, SigV4 header,
//! SigV4 presigned, and (debug-only) development schemes into an ordered chain whose first
//! applicable outcome decides. SigV4 and Bearer verification live here; the streaming
//! chunk-signature primitives are consumed by the ingest decoder in `cairn-s3`.

#![forbid(unsafe_code)]

mod bearer;
mod chunked;
mod crypto_util;
mod sigv4;

pub use bearer::{hash_bearer_secret, parse_bearer};
pub use chunked::{chunk_string_to_sign, next_chunk_signature, streaming_signing_key};
pub use crypto_util::sha256_hex;
pub use sigv4::{ParsedSig, canonical_request, compute_signature, signing_key, string_to_sign};

use async_trait::async_trait;
use cairn_types::auth::{AuthMethod, AuthOutcome, Principal, RequestView, Role};
use cairn_types::crypto::Nonce;
use cairn_types::error::AuthError;
use cairn_types::id::UserId;
use cairn_types::traits::{Authenticator, Clock, Crypto, MetadataStore};
use std::sync::Arc;

/// The composed authenticator chain. Holds the metadata store (for credential lookup), the
/// crypto facility (to decrypt SigV4 secrets), and the clock (for skew validation).
#[derive(Clone)]
pub struct AuthChain {
    meta: Arc<dyn MetadataStore>,
    crypto: Arc<dyn Crypto>,
    clock: Arc<dyn Clock>,
    dev_enabled: bool,
}

impl std::fmt::Debug for AuthChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthChain")
            .field("dev_enabled", &self.dev_enabled)
            .finish_non_exhaustive()
    }
}

impl AuthChain {
    /// Build a chain. `dev_enabled` only has effect when the crate is built with the `dev-auth`
    /// feature (release builds compile the bypass out entirely).
    pub fn new(
        meta: Arc<dyn MetadataStore>,
        crypto: Arc<dyn Crypto>,
        clock: Arc<dyn Clock>,
        dev_enabled: bool,
    ) -> Self {
        Self {
            meta,
            crypto,
            clock,
            dev_enabled,
        }
    }

    async fn verify_sigv4_header(&self, view: &RequestView<'_>, header: &str) -> AuthOutcome {
        let Some(parsed) = sigv4::parse_authorization_header(header) else {
            return AuthOutcome::Denied(AuthError::Malformed);
        };
        let creds = match self.meta.user_by_sigv4_key(&parsed.access_key_id).await {
            Ok(Some(c)) if c.user.is_active => c,
            Ok(_) => return AuthOutcome::Denied(AuthError::UnknownKey),
            Err(_) => return AuthOutcome::Denied(AuthError::UnknownKey),
        };
        let secret = match self
            .crypto
            .open(&creds.secret_ciphertext, &Nonce(creds.secret_nonce))
        {
            Ok(s) => s,
            Err(_) => return AuthOutcome::Denied(AuthError::UnknownKey),
        };
        let secret = String::from_utf8_lossy(&secret).into_owned();
        match sigv4::verify_header(view, &parsed, &secret, self.clock.now()) {
            Ok(method) => AuthOutcome::Authenticated(sigv4::principal(
                creds.user.id,
                creds.user.display_name,
                parsed.access_key_id,
                creds.user.role,
                method,
            )),
            Err(e) => AuthOutcome::Denied(e),
        }
    }

    async fn verify_sigv4_presigned(&self, view: &RequestView<'_>) -> AuthOutcome {
        let Some((parsed, expires)) = sigv4::parse_presigned(view.query) else {
            return AuthOutcome::Denied(AuthError::Malformed);
        };
        let creds = match self.meta.user_by_sigv4_key(&parsed.access_key_id).await {
            Ok(Some(c)) if c.user.is_active => c,
            Ok(_) => return AuthOutcome::Denied(AuthError::UnknownKey),
            Err(_) => return AuthOutcome::Denied(AuthError::UnknownKey),
        };
        let secret = match self
            .crypto
            .open(&creds.secret_ciphertext, &Nonce(creds.secret_nonce))
        {
            Ok(s) => s,
            Err(_) => return AuthOutcome::Denied(AuthError::UnknownKey),
        };
        let secret = String::from_utf8_lossy(&secret).into_owned();
        match sigv4::verify_presigned(view, &parsed, expires, &secret, self.clock.now()) {
            Ok(method) => AuthOutcome::Authenticated(sigv4::principal(
                creds.user.id,
                creds.user.display_name,
                parsed.access_key_id,
                creds.user.role,
                method,
            )),
            Err(e) => AuthOutcome::Denied(e),
        }
    }

    async fn verify_bearer(&self, header: &str) -> AuthOutcome {
        let Some((id, secret)) = parse_bearer(header) else {
            return AuthOutcome::Denied(AuthError::Malformed);
        };
        match self.meta.user_by_bearer_key(&id).await {
            Ok(Some(ub)) if ub.user.is_active => {
                let computed = hash_bearer_secret(&secret);
                if self
                    .crypto
                    .ct_eq(computed.as_bytes(), ub.secret_hash.as_bytes())
                {
                    AuthOutcome::Authenticated(Principal {
                        user_id: ub.user.id,
                        display_name: ub.user.display_name,
                        access_key_id: id,
                        role: ub.user.role,
                        method: AuthMethod::Bearer,
                    })
                } else {
                    AuthOutcome::Denied(AuthError::SignatureMismatch)
                }
            }
            Ok(_) => AuthOutcome::Denied(AuthError::UnknownKey),
            Err(_) => AuthOutcome::Denied(AuthError::UnknownKey),
        }
    }
}

#[async_trait]
impl Authenticator for AuthChain {
    async fn authenticate(&self, view: &RequestView<'_>) -> AuthOutcome {
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
}

fn dev_principal() -> Principal {
    Principal {
        user_id: UserId("dev".to_owned()),
        display_name: "development".to_owned(),
        access_key_id: "dev".to_owned(),
        role: Role::Administrator,
        method: AuthMethod::Development,
    }
}
