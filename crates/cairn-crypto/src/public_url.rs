//! HMAC-SHA256 signing and verification of Cairn's signed public-read URLs ([`PublicUrl`]).

use cairn_types::crypto::Signature;
use cairn_types::time::Timestamp;
use cairn_types::traits::PublicUrl;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

type HmacSha256 = Hmac<Sha256>;

/// Production [`PublicUrl`]: signs Cairn's signed public-read URLs (a Cairn extension, not an
/// S3 feature) with HMAC-SHA256 over a canonical string of
/// `method "\n" escaped_path "\n" expiry-millis`, hex-encoding the MAC into a [`Signature`].
///
/// Verification recomputes the MAC and compares it to the presented signature in constant
/// time, and independently enforces `now <= expiry`. The signing secret is held in a
/// zeroizing buffer so it is scrubbed on drop, and the `Debug` impl never prints it.
pub struct HmacPublicUrl {
    secret: Zeroizing<Vec<u8>>,
}

impl core::fmt::Debug for HmacPublicUrl {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HmacPublicUrl")
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl HmacPublicUrl {
    /// Construct from a signing secret. Any non-empty byte string is accepted; HMAC keys are
    /// of arbitrary length.
    #[must_use]
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: Zeroizing::new(secret.into()),
        }
    }

    /// Derive the signing secret from the 32-byte master key via a domain-separated
    /// HMAC-SHA256 PRF (`HMAC(master_key, "cairn/public-url/v1")`). This makes the public-URL
    /// key independent of every other use of the master key, and — crucially — keys off the
    /// raw key *bytes*, not the master key's hex encoding.
    #[must_use]
    pub fn from_master_key(master_key: &[u8; 32]) -> Self {
        let mut mac =
            HmacSha256::new_from_slice(master_key).expect("HMAC accepts a key of any length");
        mac.update(b"cairn/public-url/v1");
        let subkey = Zeroizing::new(mac.finalize().into_bytes().to_vec());
        Self::new(subkey.to_vec())
    }

    /// Derive from a hex-encoded 32-byte master key (64 hex digits), as held in config.
    ///
    /// # Errors
    /// [`KeyError::Malformed`] if not valid hex; [`KeyError::WrongLength`] if it does not
    /// decode to exactly 32 bytes.
    pub fn from_master_key_hex(hex_key: &str) -> Result<Self, crate::KeyError> {
        let bytes =
            Zeroizing::new(hex::decode(hex_key.trim()).map_err(|_| crate::KeyError::Malformed)?);
        let arr: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| crate::KeyError::WrongLength)?;
        // `bytes` is a Zeroizing buffer; it is scrubbed on drop at function return.
        Ok(Self::from_master_key(&arr))
    }

    /// A random per-process signing secret, used when no master key is configured. Links are
    /// valid only within this process (like the development master key) and — unlike the old
    /// hardcoded constant — are NOT forgeable from the source.
    #[must_use]
    pub fn ephemeral() -> Self {
        use rand::RngCore;
        let mut secret = Zeroizing::new(vec![0u8; 32]);
        rand::thread_rng().fill_bytes(&mut secret);
        Self::new(secret.to_vec())
    }

    /// The canonical string that is MAC'd: method, escaped path, and expiry millis joined by
    /// newlines. Keeping this in one place guarantees `sign` and `verify` agree.
    fn canonical(method: &str, escaped_path: &str, expiry: Timestamp) -> String {
        format!("{method}\n{escaped_path}\n{}", expiry.as_millis())
    }

    /// Compute the raw HMAC-SHA256 tag over the canonical string.
    fn mac(&self, method: &str, escaped_path: &str, expiry: Timestamp) -> Vec<u8> {
        // `Hmac::new_from_slice` accepts a key of any length, so this never fails.
        let mut mac =
            HmacSha256::new_from_slice(&self.secret).expect("HMAC accepts a key of any length");
        mac.update(Self::canonical(method, escaped_path, expiry).as_bytes());
        mac.finalize().into_bytes().to_vec()
    }
}

impl PublicUrl for HmacPublicUrl {
    fn sign(&self, method: &str, escaped_path: &str, expiry: Timestamp) -> Signature {
        Signature(hex::encode(self.mac(method, escaped_path, expiry)))
    }

    fn verify(
        &self,
        method: &str,
        escaped_path: &str,
        expiry: Timestamp,
        signature: &Signature,
        now: Timestamp,
    ) -> bool {
        // Expiry first: an expired URL is never valid regardless of the signature.
        if now > expiry {
            return false;
        }
        // Decode the presented hex signature; a malformed signature cannot match.
        let Ok(presented) = hex::decode(signature.0.as_bytes()) else {
            return false;
        };
        let expected = self.mac(method, escaped_path, expiry);
        // Constant-time compare of the raw MAC bytes (folds in the length check).
        expected.ct_eq(&presented).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url() -> HmacPublicUrl {
        HmacPublicUrl::new(b"super-secret-signing-key".to_vec())
    }

    const NOW: Timestamp = Timestamp(1_700_000_000_000);
    const EXPIRY: Timestamp = Timestamp(1_700_000_300_000); // +300s

    #[test]
    fn sign_then_verify_is_true() {
        let u = url();
        let sig = u.sign("GET", "/bucket/object%20key.txt", EXPIRY);
        assert!(u.verify("GET", "/bucket/object%20key.txt", EXPIRY, &sig, NOW));
    }

    #[test]
    fn master_key_derivation_is_deterministic_and_domain_separated() {
        let key = [7u8; 32];
        let a = HmacPublicUrl::from_master_key(&key);
        let b = HmacPublicUrl::from_master_key(&key);
        let sig_a = a.sign("GET", "/b/k", EXPIRY);
        // Same master key → same signer → same signature (links survive restarts).
        assert_eq!(sig_a, b.sign("GET", "/b/k", EXPIRY));
        // A different master key yields a different (non-verifying) signature.
        let other = HmacPublicUrl::from_master_key(&[8u8; 32]);
        assert!(!other.verify("GET", "/b/k", EXPIRY, &sig_a, NOW));
        // The derived key is the HMAC-PRF subkey, NOT the raw master key used directly.
        let raw = HmacPublicUrl::new(key.to_vec());
        assert!(!raw.verify("GET", "/b/k", EXPIRY, &sig_a, NOW));
    }

    #[test]
    fn from_hex_matches_from_bytes_and_ephemeral_is_random() {
        let key = [0xABu8; 32];
        let hex = hex::encode(key);
        let from_hex = HmacPublicUrl::from_master_key_hex(&hex).unwrap();
        let sig = HmacPublicUrl::from_master_key(&key).sign("GET", "/b/k", EXPIRY);
        // Hex path keys off the decoded bytes, agreeing with the byte path.
        assert!(from_hex.verify("GET", "/b/k", EXPIRY, &sig, NOW));
        assert!(HmacPublicUrl::from_master_key_hex("nothex").is_err());
        // Two ephemeral signers almost surely differ (random per-process key).
        let e1 = HmacPublicUrl::ephemeral().sign("GET", "/b/k", EXPIRY);
        let e2 = HmacPublicUrl::ephemeral().sign("GET", "/b/k", EXPIRY);
        assert_ne!(e1, e2);
    }

    #[test]
    fn signature_is_lowercase_hex_of_32_bytes() {
        let u = url();
        let Signature(s) = u.sign("GET", "/a", EXPIRY);
        assert_eq!(s.len(), 64, "SHA-256 MAC is 32 bytes => 64 hex chars");
        assert!(
            s.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn verify_false_for_wrong_signature() {
        let u = url();
        let bad = Signature(hex::encode([0u8; 32]));
        assert!(!u.verify("GET", "/bucket/key", EXPIRY, &bad, NOW));
    }

    #[test]
    fn verify_false_for_tampered_path() {
        let u = url();
        let sig = u.sign("GET", "/bucket/key", EXPIRY);
        assert!(!u.verify("GET", "/bucket/OTHER", EXPIRY, &sig, NOW));
    }

    #[test]
    fn verify_false_for_tampered_method() {
        let u = url();
        let sig = u.sign("GET", "/bucket/key", EXPIRY);
        assert!(!u.verify("PUT", "/bucket/key", EXPIRY, &sig, NOW));
    }

    #[test]
    fn verify_false_for_tampered_expiry() {
        let u = url();
        let sig = u.sign("GET", "/bucket/key", EXPIRY);
        let other_expiry = Timestamp(EXPIRY.as_millis() + 1);
        // Still unexpired (now < other_expiry) but the MAC was computed over EXPIRY.
        assert!(!u.verify("GET", "/bucket/key", other_expiry, &sig, NOW));
    }

    #[test]
    fn verify_false_for_expired_timestamp() {
        let u = url();
        let sig = u.sign("GET", "/bucket/key", EXPIRY);
        let after_expiry = Timestamp(EXPIRY.as_millis() + 1);
        assert!(
            !u.verify("GET", "/bucket/key", EXPIRY, &sig, after_expiry),
            "now > expiry must reject even a correct signature"
        );
    }

    #[test]
    fn verify_true_exactly_at_expiry() {
        let u = url();
        let sig = u.sign("GET", "/bucket/key", EXPIRY);
        // now == expiry is still valid (the check is now > expiry).
        assert!(u.verify("GET", "/bucket/key", EXPIRY, &sig, EXPIRY));
    }

    #[test]
    fn verify_false_for_malformed_signature_hex() {
        let u = url();
        let garbage = Signature("not-hex-zz".to_owned());
        assert!(!u.verify("GET", "/bucket/key", EXPIRY, &garbage, NOW));
    }

    #[test]
    fn verify_false_under_a_different_secret() {
        let signer = HmacPublicUrl::new(b"key-one".to_vec());
        let other = HmacPublicUrl::new(b"key-two".to_vec());
        let sig = signer.sign("GET", "/bucket/key", EXPIRY);
        assert!(!other.verify("GET", "/bucket/key", EXPIRY, &sig, NOW));
    }

    #[test]
    fn sign_is_deterministic() {
        let u = url();
        let a = u.sign("GET", "/p", EXPIRY);
        let b = u.sign("GET", "/p", EXPIRY);
        assert_eq!(a, b);
    }

    #[test]
    fn debug_redacts_secret() {
        let u = HmacPublicUrl::new(b"top-secret".to_vec());
        let s = format!("{u:?}");
        assert!(!s.contains("top-secret"));
        assert!(s.contains("redacted"));
    }
}
