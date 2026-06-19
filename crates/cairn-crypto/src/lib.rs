//! `cairn-crypto` — the production cryptography facility for Cairn (ARCH 12.6, 27).
//!
//! This crate realises three frozen traits from [`cairn_types`]:
//!
//! * [`SystemCrypto`] implements [`Crypto`]: AEAD envelope encryption of secrets at rest
//!   (SigV4 secrets, replication destination credentials) under an out-of-band 32-byte
//!   master key, using AES-256-GCM. Decrypted plaintext is held only transiently in a
//!   [`zeroize::Zeroizing`] container so it is scrubbed promptly (F-15).
//! * [`SystemClock`] implements [`Clock`]: the current time from the operating system as a
//!   Unix-millis [`Timestamp`].
//! * [`HmacPublicUrl`] implements [`PublicUrl`]: signing and verification of Cairn's signed
//!   public-read URLs with HMAC-SHA256 over a canonical string, verified in constant time
//!   with an expiry check.
//!
//! No primitive is rolled by hand: AES-GCM comes from `aes-gcm`, HMAC-SHA256 from `hmac` +
//! `sha2`, constant-time comparison from `subtle`, and randomness from `rand`.

#![forbid(unsafe_code)]

mod base64;
mod clock;
mod public_url;
mod system_crypto;

pub use clock::SystemClock;
pub use public_url::HmacPublicUrl;
pub use system_crypto::{KeyError, NONCE_LEN, SystemCrypto};

// Re-export the trait surface this crate implements so downstream wiring can name it from
// one place.
pub use cairn_types::traits::{Clock, Crypto, PublicUrl};
pub use cairn_types::{Nonce, Sealed, Signature, Timestamp};
