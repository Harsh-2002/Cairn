//! The SSE-KMS key provider (ARCH 27, Increment 2).
//!
//! A [`KeyProvider`] maps a KMS key id to the cryptographic material used to seal (and later open)
//! an object's data-encryption key, and validates that a key id is permitted for a write. v1 ships
//! only [`LocalRingProvider`], which seals every DEK under the node master-key ring regardless of
//! the key id — the key id is a **label**, not cryptographic isolation:
//!
//! * every DEK is wrapped by the same master ring, so removing a key id from the allow-list does
//!   **not** lock existing objects (a read unwraps under the master key, ignoring the key id);
//! * the allow-list (`CAIRN_KMS_KEY_IDS`) gates **writes only**, and when unset accepts any id. It
//!   gates a *named* id: an `aws:kms` write with NO key id names nothing to gate and is accepted
//!   regardless of the allow-list. Under label-only this is exactly equivalent to any other DEK
//!   (same master-ring envelope), so it is not a confidentiality gap; a real per-key-material
//!   provider would additionally reject a no-id `aws:kms` write when an allow-list is active.
//!
//! The trait is shaped so a real external provider (AWS KMS, Vault) can slot in later without
//! touching the S3 surface: such a provider would return per-key material from [`crypto_for`] and
//! enforce genuine isolation and revocation in [`validate_key_id`]. (One exception, documented at
//! `open_sse_dek`: per-key material would require the open path to resolve crypto via the provider.)
//!
//! [`crypto_for`]: KeyProvider::crypto_for
//! [`validate_key_id`]: KeyProvider::validate_key_id

use cairn_types::error::Error;
use cairn_types::traits::Crypto;
use std::sync::Arc;

/// Resolves a KMS key id to the DEK-sealing crypto, and validates a requested key id.
pub trait KeyProvider: Send + Sync {
    /// Validate that `key_id` is acceptable for a write. Returns `Err(InvalidArgument)` to reject
    /// the write (fail-closed) — e.g. when an allow-list is configured and the id is not on it.
    fn validate_key_id(&self, key_id: &str) -> Result<(), Error>;

    /// The crypto used to seal (and, for the local provider, later open) a DEK for `key_id`. The
    /// local provider returns the master ring for **every** id (label-only); an external provider
    /// would return per-key material.
    fn crypto_for(&self, key_id: &str) -> Result<Arc<dyn Crypto>, Error>;
}

/// The v1 [`KeyProvider`]: seals every DEK under the node master-key ring (label-only), with an
/// optional write-time allow-list of accepted key ids.
pub struct LocalRingProvider {
    crypto: Arc<dyn Crypto>,
    /// The `CAIRN_KMS_KEY_IDS` allow-list. `None` ⇒ accept **any** key id (matches the
    /// label-not-gate framing); `Some(list)` ⇒ reject a write whose key id is not present.
    allowed_key_ids: Option<Vec<String>>,
}

impl std::fmt::Debug for LocalRingProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The crypto material is never printed; only the allow-list shape is surfaced.
        f.debug_struct("LocalRingProvider")
            .field("allowed_key_ids", &self.allowed_key_ids)
            .finish_non_exhaustive()
    }
}

impl LocalRingProvider {
    /// Build a local provider over the master-ring `crypto`. `allowed_key_ids` is the
    /// `CAIRN_KMS_KEY_IDS` allow-list — `None` accepts any id, `Some(list)` gates writes to it.
    #[must_use]
    pub fn new(crypto: Arc<dyn Crypto>, allowed_key_ids: Option<Vec<String>>) -> Self {
        Self {
            crypto,
            allowed_key_ids,
        }
    }
}

impl KeyProvider for LocalRingProvider {
    fn validate_key_id(&self, key_id: &str) -> Result<(), Error> {
        match &self.allowed_key_ids {
            // Unset allow-list: accept any id (the key id is a label, not a gate).
            None => Ok(()),
            Some(list) if list.iter().any(|k| k == key_id) => Ok(()),
            Some(_) => Err(Error::InvalidArgument(format!(
                "KMS key id is not in the configured allow-list: {key_id}"
            ))),
        }
    }

    fn crypto_for(&self, _key_id: &str) -> Result<Arc<dyn Crypto>, Error> {
        // Label-only: the same master ring seals every id. The object's `open_sse_dek` path unwraps
        // under this same ring, so sealing and opening stay symmetric for v1.
        Ok(self.crypto.clone())
    }
}
