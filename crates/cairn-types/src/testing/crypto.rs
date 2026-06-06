//! Stub cryptography and public-URL doubles. These are NOT secure — they exist only to
//! exercise the seam round-trips deterministically.

use crate::crypto::{Nonce, Sealed, Signature};
use crate::error::CryptoError;
use crate::time::Timestamp;
use crate::traits::{Crypto, PublicUrl};

/// A stub [`Crypto`] that "encrypts" by XOR with a fixed byte so that ciphertext differs
/// from plaintext and round-trips. Do not use outside tests.
#[derive(Debug, Default)]
pub struct StubCrypto;

const XOR_BYTE: u8 = 0x5a;

impl Crypto for StubCrypto {
    fn seal(&self, plaintext: &[u8]) -> Result<Sealed, CryptoError> {
        let ciphertext = plaintext.iter().map(|b| b ^ XOR_BYTE).collect();
        Ok(Sealed {
            ciphertext,
            nonce: Nonce(vec![0; 12]),
        })
    }

    fn open(&self, ciphertext: &[u8], _nonce: &Nonce) -> Result<Vec<u8>, CryptoError> {
        Ok(ciphertext.iter().map(|b| b ^ XOR_BYTE).collect())
    }

    fn ct_eq(&self, a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut diff = 0u8;
        for (x, y) in a.iter().zip(b.iter()) {
            diff |= x ^ y;
        }
        diff == 0
    }
}

/// A stub [`PublicUrl`] whose signature is a deterministic string over the inputs.
#[derive(Debug, Default)]
pub struct StubPublicUrl;

impl PublicUrl for StubPublicUrl {
    fn sign(&self, method: &str, escaped_path: &str, expiry: Timestamp) -> Signature {
        Signature(format!(
            "stub:{method}:{escaped_path}:{}",
            expiry.as_millis()
        ))
    }

    fn verify(
        &self,
        method: &str,
        escaped_path: &str,
        expiry: Timestamp,
        signature: &Signature,
        now: Timestamp,
    ) -> bool {
        if now > expiry {
            return false;
        }
        self.sign(method, escaped_path, expiry) == *signature
    }
}
