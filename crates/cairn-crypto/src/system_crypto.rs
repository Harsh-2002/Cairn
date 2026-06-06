//! AES-256-GCM envelope encryption of secrets at rest ([`Crypto`]).

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce as GcmNonce};
use cairn_types::crypto::{Nonce, Sealed};
use cairn_types::error::CryptoError;
use cairn_types::traits::Crypto;
use rand::RngCore;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

use crate::base64;

/// The AES-GCM nonce length in bytes (96 bits — the standard, recommended GCM nonce size).
pub const NONCE_LEN: usize = 12;

/// The master-key length in bytes (AES-256).
const KEY_LEN: usize = 32;

/// A failure constructing a [`SystemCrypto`] from an encoded master key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    /// The encoded text was not valid for the declared encoding.
    Malformed,
    /// The decoded key was not exactly 32 bytes.
    WrongLength,
}

impl core::fmt::Display for KeyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            KeyError::Malformed => f.write_str("master key encoding is malformed"),
            KeyError::WrongLength => f.write_str("master key must decode to exactly 32 bytes"),
        }
    }
}

impl std::error::Error for KeyError {}

impl From<KeyError> for CryptoError {
    fn from(_: KeyError) -> Self {
        CryptoError::Key
    }
}

/// Production [`Crypto`]: AES-256-GCM envelope encryption of secrets under a 32-byte master
/// key supplied out of band. The cipher is built once from the key; each [`seal`](Crypto::seal)
/// draws a fresh random 96-bit nonce so that sealing the same plaintext twice yields distinct
/// ciphertexts.
///
/// The master key is held in a [`Zeroizing`] buffer so it is scrubbed on drop, and the
/// `Debug` impl never prints key material.
pub struct SystemCrypto {
    cipher: Aes256Gcm,
}

impl core::fmt::Debug for SystemCrypto {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SystemCrypto")
            .field("cipher", &"Aes256Gcm(<redacted>)")
            .finish()
    }
}

impl SystemCrypto {
    /// Construct from a raw 32-byte master key. The key bytes are scrubbed from the caller's
    /// move and from the intermediate buffer after the cipher is initialised.
    #[must_use]
    pub fn new(key: [u8; KEY_LEN]) -> Self {
        let mut key = Zeroizing::new(key);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key.as_slice()));
        key.zeroize();
        Self { cipher }
    }

    /// Construct from a hex-encoded 32-byte master key (64 hex digits).
    ///
    /// # Errors
    /// Returns [`KeyError::Malformed`] if `s` is not valid hex and [`KeyError::WrongLength`]
    /// if it does not decode to exactly 32 bytes.
    pub fn from_hex(s: &str) -> Result<Self, KeyError> {
        let mut bytes = Zeroizing::new(hex::decode(s.trim()).map_err(|_| KeyError::Malformed)?);
        let key = into_key(&bytes)?;
        bytes.zeroize();
        Ok(Self::new(key))
    }

    /// Construct from a standard base64-encoded 32-byte master key (with or without `=`
    /// padding).
    ///
    /// # Errors
    /// Returns [`KeyError::Malformed`] if `s` is not valid base64 and [`KeyError::WrongLength`]
    /// if it does not decode to exactly 32 bytes.
    pub fn from_base64(s: &str) -> Result<Self, KeyError> {
        let mut bytes = Zeroizing::new(base64::decode(s.trim()).map_err(|_| KeyError::Malformed)?);
        let key = into_key(&bytes)?;
        bytes.zeroize();
        Ok(Self::new(key))
    }
}

/// Copy a decoded buffer into a fixed-size key array, validating the length.
fn into_key(bytes: &[u8]) -> Result<[u8; KEY_LEN], KeyError> {
    let arr: [u8; KEY_LEN] = bytes.try_into().map_err(|_| KeyError::WrongLength)?;
    Ok(arr)
}

impl Crypto for SystemCrypto {
    fn seal(&self, plaintext: &[u8]) -> Result<Sealed, CryptoError> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = GcmNonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|_| CryptoError::Encrypt)?;

        Ok(Sealed {
            ciphertext,
            nonce: Nonce(nonce_bytes.to_vec()),
        })
    }

    fn open(&self, ciphertext: &[u8], nonce: &Nonce) -> Result<Vec<u8>, CryptoError> {
        if nonce.0.len() != NONCE_LEN {
            return Err(CryptoError::Decrypt);
        }
        let gcm_nonce = GcmNonce::from_slice(&nonce.0);

        // The decrypted plaintext lives only transiently in a zeroizing container so it is
        // scrubbed promptly and is less likely to surface in a core dump (ARCH §27, F-15).
        let plaintext = Zeroizing::new(
            self.cipher
                .decrypt(gcm_nonce, ciphertext)
                .map_err(|_| CryptoError::Decrypt)?,
        );

        // The trait contract returns an owned `Vec<u8>`; the caller is responsible for its
        // own scrubbing. The internal zeroizing copy above is scrubbed when this scope ends.
        Ok(plaintext.to_vec())
    }

    fn ct_eq(&self, a: &[u8], b: &[u8]) -> bool {
        // `subtle::ConstantTimeEq` on slices already folds in a length check and compares the
        // contents in constant time for equal-length inputs.
        a.ct_eq(b).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crypto() -> SystemCrypto {
        SystemCrypto::new([7u8; KEY_LEN])
    }

    #[test]
    fn seal_open_round_trips() {
        let c = crypto();
        let plaintext = b"a SigV4 secret access key";
        let sealed = c.seal(plaintext).expect("seal");
        let opened = c.open(&sealed.ciphertext, &sealed.nonce).expect("open");
        assert_eq!(opened, plaintext);
    }

    #[test]
    fn seal_open_round_trips_empty() {
        let c = crypto();
        let sealed = c.seal(b"").expect("seal empty");
        let opened = c
            .open(&sealed.ciphertext, &sealed.nonce)
            .expect("open empty");
        assert!(opened.is_empty());
    }

    #[test]
    fn nonce_is_twelve_bytes() {
        let c = crypto();
        let sealed = c.seal(b"x").expect("seal");
        assert_eq!(sealed.nonce.0.len(), NONCE_LEN);
        // ciphertext is plaintext + 16-byte GCM tag.
        assert_eq!(sealed.ciphertext.len(), 1 + 16);
    }

    #[test]
    fn tampered_ciphertext_byte_fails_decrypt() {
        let c = crypto();
        let mut sealed = c.seal(b"tamper me please").expect("seal");
        // Flip a bit in the first ciphertext byte.
        sealed.ciphertext[0] ^= 0x01;
        let err = c
            .open(&sealed.ciphertext, &sealed.nonce)
            .expect_err("must fail");
        assert!(matches!(err, CryptoError::Decrypt));
    }

    #[test]
    fn tampered_tag_byte_fails_decrypt() {
        let c = crypto();
        let mut sealed = c.seal(b"tag tamper").expect("seal");
        let last = sealed.ciphertext.len() - 1;
        sealed.ciphertext[last] ^= 0x80;
        let err = c
            .open(&sealed.ciphertext, &sealed.nonce)
            .expect_err("must fail");
        assert!(matches!(err, CryptoError::Decrypt));
    }

    #[test]
    fn tampered_nonce_fails_decrypt() {
        let c = crypto();
        let sealed = c.seal(b"nonce tamper").expect("seal");
        let mut bad = sealed.nonce.clone();
        bad.0[0] ^= 0xff;
        let err = c.open(&sealed.ciphertext, &bad).expect_err("must fail");
        assert!(matches!(err, CryptoError::Decrypt));
    }

    #[test]
    fn wrong_length_nonce_fails_decrypt() {
        let c = crypto();
        let sealed = c.seal(b"len").expect("seal");
        let short = Nonce(vec![0u8; NONCE_LEN - 1]);
        assert!(matches!(
            c.open(&sealed.ciphertext, &short),
            Err(CryptoError::Decrypt)
        ));
        let long = Nonce(vec![0u8; NONCE_LEN + 1]);
        assert!(matches!(
            c.open(&sealed.ciphertext, &long),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn wrong_key_fails_decrypt() {
        let a = SystemCrypto::new([1u8; KEY_LEN]);
        let b = SystemCrypto::new([2u8; KEY_LEN]);
        let sealed = a.seal(b"cross key").expect("seal");
        let err = b
            .open(&sealed.ciphertext, &sealed.nonce)
            .expect_err("must fail under a different key");
        assert!(matches!(err, CryptoError::Decrypt));
    }

    #[test]
    fn two_seals_of_same_plaintext_differ() {
        let c = crypto();
        let p = b"deterministic plaintext";
        let s1 = c.seal(p).expect("seal 1");
        let s2 = c.seal(p).expect("seal 2");
        // Random nonce -> distinct nonces and distinct ciphertexts with overwhelming prob.
        assert_ne!(s1.nonce, s2.nonce, "nonces must differ");
        assert_ne!(s1.ciphertext, s2.ciphertext, "ciphertexts must differ");
        // Both still decrypt to the original.
        assert_eq!(c.open(&s1.ciphertext, &s1.nonce).expect("open 1"), p);
        assert_eq!(c.open(&s2.ciphertext, &s2.nonce).expect("open 2"), p);
    }

    #[test]
    fn ct_eq_true_for_equal() {
        let c = crypto();
        assert!(c.ct_eq(b"abcdef", b"abcdef"));
        assert!(c.ct_eq(b"", b""));
    }

    #[test]
    fn ct_eq_false_for_unequal_same_length() {
        let c = crypto();
        assert!(!c.ct_eq(b"abcdef", b"abcdeg"));
    }

    #[test]
    fn ct_eq_false_for_different_lengths() {
        let c = crypto();
        assert!(!c.ct_eq(b"abc", b"abcd"));
        assert!(!c.ct_eq(b"", b"x"));
    }

    #[test]
    fn from_hex_round_trips() {
        let key = [0xABu8; KEY_LEN];
        let encoded = hex::encode(key);
        let c = SystemCrypto::from_hex(&encoded).expect("from_hex");
        let reference = SystemCrypto::new(key);
        let sealed = reference.seal(b"hk").expect("seal");
        // The hex-built crypto must decrypt what the array-built crypto sealed.
        assert_eq!(
            c.open(&sealed.ciphertext, &sealed.nonce).expect("open"),
            b"hk"
        );
    }

    #[test]
    fn from_hex_rejects_bad_input() {
        assert_eq!(
            SystemCrypto::from_hex("zz").err(),
            Some(KeyError::Malformed)
        );
        // Valid hex but only 16 bytes.
        let short = hex::encode([0u8; 16]);
        assert_eq!(
            SystemCrypto::from_hex(&short).err(),
            Some(KeyError::WrongLength)
        );
    }

    #[test]
    fn from_base64_round_trips() {
        let key = [0x5Cu8; KEY_LEN];
        let encoded = crate::base64::encode(&key);
        let c = SystemCrypto::from_base64(&encoded).expect("from_base64");
        let reference = SystemCrypto::new(key);
        let sealed = reference.seal(b"bk").expect("seal");
        assert_eq!(
            c.open(&sealed.ciphertext, &sealed.nonce).expect("open"),
            b"bk"
        );
    }

    #[test]
    fn from_base64_rejects_bad_input() {
        assert_eq!(
            SystemCrypto::from_base64("!!!!").err(),
            Some(KeyError::Malformed)
        );
        let short = crate::base64::encode(&[0u8; 16]);
        assert_eq!(
            SystemCrypto::from_base64(&short).err(),
            Some(KeyError::WrongLength)
        );
    }

    #[test]
    fn debug_redacts_key() {
        let c = crypto();
        let s = format!("{c:?}");
        assert!(s.contains("redacted"));
    }
}
