//! Supporting types for the cryptography, clock, and public-URL traits (defined in
//! `traits.rs`). Production implementations live in `cairn-crypto`; test doubles in
//! `crate::testing`.

use serde::{Deserialize, Serialize};

/// An authenticated-encryption nonce for envelope-encrypted secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Nonce(pub Vec<u8>);

/// Ciphertext produced by sealing a secret (nonce kept alongside).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sealed {
    /// The ciphertext (including the AEAD tag).
    pub ciphertext: Vec<u8>,
    /// The nonce used.
    pub nonce: Nonce,
}

/// A keyed signature over a Cairn signed public-read URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature(pub String);
