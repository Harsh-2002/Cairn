//! AES-256-GCM envelope encryption of secrets at rest ([`Crypto`]).
//!
//! Secrets are sealed under a RING of 32-byte master keys (audit #29). Each seal writes a
//! self-describing `CRK1` envelope (`magic ‖ key_id ‖ nonce ‖ ct‖tag`) under the active key,
//! binding `magic ‖ key_id` as GCM associated data so a swapped version/key id fails
//! authentication. Opening routes by the magic; legacy (pre-#29, no-magic) blobs decrypt under
//! the configured legacy key using a separately-stored nonce.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce as GcmNonce};
use cairn_types::crypto::{Nonce, Sealed};
use cairn_types::error::CryptoError;
use cairn_types::traits::Crypto;
use rand::RngCore;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

use crate::base64;

/// The AES-GCM nonce length in bytes (96 bits — the standard, recommended GCM nonce size).
pub const NONCE_LEN: usize = 12;

/// The master-key length in bytes (AES-256).
const KEY_LEN: usize = 32;

/// The `CRK1` envelope magic ("Cairn Rotation Key, v1").
const MAGIC: [u8; 4] = *b"CRK1";
/// Width of the big-endian key id in the envelope header.
const KEY_ID_LEN: usize = 2;
/// Envelope header = magic ‖ key_id; bound as GCM associated data.
const HEADER_LEN: usize = MAGIC.len() + KEY_ID_LEN;
/// Bytes before the ciphertext in a versioned envelope = header ‖ nonce.
const VERSIONED_PREFIX: usize = HEADER_LEN + NONCE_LEN;

/// The AES-GCM random-nonce ceiling per key (NIST SP 800-38D): keep seals well under 2^32.
const NONCE_CEILING: u64 = 1 << 32;
/// Warn the operator to rotate once the active key passes 75% of the ceiling.
const ALERT_THRESHOLD: u64 = NONCE_CEILING / 4 * 3;
/// Refuse NEW seals once the active key passes 95% of the ceiling (opens are never blocked).
const STOP_THRESHOLD: u64 = NONCE_CEILING / 100 * 95;

/// A failure constructing a [`SystemCrypto`] from an encoded master key or key ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    /// The encoded text was not valid for the declared encoding, or the ring was invalid
    /// (empty, an id of 0, a duplicate id, or an active/legacy id absent from the ring).
    Malformed,
    /// The decoded key was not exactly 32 bytes.
    WrongLength,
}

impl core::fmt::Display for KeyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            KeyError::Malformed => f.write_str("master key encoding or ring is malformed"),
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

/// Production [`Crypto`]: AES-256-GCM envelope encryption of secrets under a ring of 32-byte
/// master keys (audit #29). Each [`seal`](Crypto::seal) writes a self-describing `CRK1` envelope
/// under the ACTIVE key with `magic ‖ key_id` bound as associated data; [`open`](Crypto::open)
/// routes by the magic (a `CRK1` blob decrypts under the ring key its envelope names; a legacy,
/// pre-#29, no-magic blob decrypts under the legacy key using a separately-stored nonce). A fresh
/// random 96-bit nonce per seal means sealing the same plaintext twice yields distinct
/// ciphertexts.
///
/// Key material lives only in the per-key ciphers (input key buffers are scrubbed after the
/// cipher is built); the `Debug` impl never prints key material.
pub struct SystemCrypto {
    /// id -> cipher. The active key seals; any ring key can open a blob that names it.
    ring: HashMap<u16, Aes256Gcm>,
    /// The id NEW seals use.
    active_id: u16,
    /// The id legacy (no-magic) blobs decrypt under — the single key that existed pre-ring.
    legacy_id: u16,
    /// Seals under the active key since this process started/primed (Phase E auto-bound).
    seal_count: AtomicU64,
    /// The seal count loaded from durable state at startup, added to `seal_count` (Phase E).
    seal_count_base: AtomicU64,
}

impl core::fmt::Debug for SystemCrypto {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SystemCrypto")
            .field("keys", &self.ring.len())
            .field("active_id", &self.active_id)
            .field("ring", &"<redacted>")
            .finish()
    }
}

impl SystemCrypto {
    /// Construct from a single raw 32-byte master key — a one-key ring at id 1 (active + legacy).
    /// The key bytes are scrubbed from the caller's move and the intermediate buffer.
    #[must_use]
    pub fn new(key: [u8; KEY_LEN]) -> Self {
        let mut ring = HashMap::with_capacity(1);
        ring.insert(1u16, cipher_from(key));
        Self {
            ring,
            active_id: 1,
            legacy_id: 1,
            seal_count: AtomicU64::new(0),
            seal_count_base: AtomicU64::new(0),
        }
    }

    /// Construct from a hex-encoded 32-byte master key (64 hex digits) — a one-key ring.
    ///
    /// # Errors
    /// [`KeyError::Malformed`] if `s` is not valid hex; [`KeyError::WrongLength`] if it does not
    /// decode to exactly 32 bytes.
    pub fn from_hex(s: &str) -> Result<Self, KeyError> {
        let mut bytes = Zeroizing::new(hex::decode(s.trim()).map_err(|_| KeyError::Malformed)?);
        let key = into_key(&bytes)?;
        bytes.zeroize();
        Ok(Self::new(key))
    }

    /// Construct from a standard base64-encoded 32-byte master key — a one-key ring.
    ///
    /// # Errors
    /// [`KeyError::Malformed`] if `s` is not valid base64; [`KeyError::WrongLength`] if it does
    /// not decode to exactly 32 bytes.
    pub fn from_base64(s: &str) -> Result<Self, KeyError> {
        let mut bytes = Zeroizing::new(base64::decode(s.trim()).map_err(|_| KeyError::Malformed)?);
        let key = into_key(&bytes)?;
        bytes.zeroize();
        Ok(Self::new(key))
    }

    /// Construct from a key ring (audit #29). `active_id` seals; `legacy_id` opens pre-ring
    /// (no-magic) blobs; `seal_count_base` primes the Phase-E counter from durable state.
    ///
    /// # Errors
    /// [`KeyError::Malformed`] if the ring is empty, contains id 0 or a duplicate id, or if
    /// `active_id`/`legacy_id` is not present in the ring.
    pub fn from_ring(
        keys: Vec<(u16, [u8; KEY_LEN])>,
        active_id: u16,
        legacy_id: u16,
        seal_count_base: u64,
    ) -> Result<Self, KeyError> {
        if keys.is_empty() {
            return Err(KeyError::Malformed);
        }
        let mut ring = HashMap::with_capacity(keys.len());
        for (id, key) in keys {
            if id == 0 {
                return Err(KeyError::Malformed);
            }
            if ring.insert(id, cipher_from(key)).is_some() {
                return Err(KeyError::Malformed); // duplicate id
            }
        }
        if !ring.contains_key(&active_id) || !ring.contains_key(&legacy_id) {
            return Err(KeyError::Malformed);
        }
        Ok(Self {
            ring,
            active_id,
            legacy_id,
            seal_count: AtomicU64::new(0),
            seal_count_base: AtomicU64::new(seal_count_base),
        })
    }

    /// The id NEW seals use.
    #[must_use]
    pub fn active_key_id(&self) -> u16 {
        self.active_id
    }

    /// The current high-water seal count under the active key (durable base + in-process).
    #[must_use]
    pub fn seal_count(&self) -> u64 {
        self.seal_count_base
            .load(Ordering::Relaxed)
            .saturating_add(self.seal_count.load(Ordering::Relaxed))
    }

    /// Prime the durable seal-count base at startup from persisted state (Phase E). Safe behind
    /// `Arc` (no `&mut`) so it can be called once before the listener binds.
    pub fn prime_seal_count(&self, base: u64) {
        self.seal_count_base.store(base, Ordering::Relaxed);
    }

    /// Decrypt `ct` under `cipher` with `nonce_bytes` (must be exactly 12 bytes) and optional AAD,
    /// scrubbing the transient plaintext (ARCH §27, F-15). A wrong nonce length fails closed.
    fn decrypt(
        &self,
        cipher: &Aes256Gcm,
        ct: &[u8],
        nonce_bytes: &[u8],
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>, CryptoError> {
        if nonce_bytes.len() != NONCE_LEN {
            return Err(CryptoError::Decrypt);
        }
        let gcm_nonce = GcmNonce::from_slice(nonce_bytes);
        let plaintext = Zeroizing::new(
            match aad {
                Some(aad) => cipher.decrypt(gcm_nonce, Payload { msg: ct, aad }),
                None => cipher.decrypt(gcm_nonce, ct),
            }
            .map_err(|_| CryptoError::Decrypt)?,
        );
        Ok(plaintext.to_vec())
    }
}

/// Build an AES-256-GCM cipher from a key, scrubbing the key buffer afterwards.
fn cipher_from(key: [u8; KEY_LEN]) -> Aes256Gcm {
    let mut key = Zeroizing::new(key);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key.as_slice()));
    key.zeroize();
    cipher
}

/// Copy a decoded buffer into a fixed-size key array, validating the length.
fn into_key(bytes: &[u8]) -> Result<[u8; KEY_LEN], KeyError> {
    let arr: [u8; KEY_LEN] = bytes.try_into().map_err(|_| KeyError::WrongLength)?;
    Ok(arr)
}

impl Crypto for SystemCrypto {
    fn seal(&self, plaintext: &[u8]) -> Result<Sealed, CryptoError> {
        // Phase-E auto-bound: count seals under the active key; refuse new seals at the hard stop
        // (opens are never affected), and warn once at the alert threshold.
        let count = self.seal_count.fetch_add(1, Ordering::Relaxed)
            + 1
            + self.seal_count_base.load(Ordering::Relaxed);
        if count >= STOP_THRESHOLD {
            return Err(CryptoError::KeyRotationRequired);
        }
        if count == ALERT_THRESHOLD {
            tracing::warn!(
                active_key_id = self.active_id,
                seal_count = count,
                "active master key at 75% of the AES-GCM random-nonce ceiling; rotate it soon (audit #29)"
            );
        }

        let mut env = Vec::with_capacity(VERSIONED_PREFIX + plaintext.len() + 16);
        env.extend_from_slice(&MAGIC);
        env.extend_from_slice(&self.active_id.to_be_bytes());
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        env.extend_from_slice(&nonce_bytes);
        // Bind magic ‖ key_id as associated data: a tampered version/key id fails authentication.
        let aad: [u8; HEADER_LEN] = env[..HEADER_LEN].try_into().expect("header is HEADER_LEN");
        let cipher = self.ring.get(&self.active_id).ok_or(CryptoError::Key)?;
        let ct = cipher
            .encrypt(
                GcmNonce::from_slice(&nonce_bytes),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| CryptoError::Encrypt)?;
        env.extend_from_slice(&ct);
        // `nonce` is redundant for a CRK1 envelope (the nonce is inside `ciphertext`); it is set
        // for source-compat but MUST NOT be persisted to a separate column (see the trait docs).
        Ok(Sealed {
            ciphertext: env,
            nonce: Nonce(nonce_bytes.to_vec()),
        })
    }

    fn open(&self, ciphertext: &[u8], nonce: &Nonce) -> Result<Vec<u8>, CryptoError> {
        // Versioned (CRK1) envelope: self-describing — parse key_id + nonce, verify AAD, decrypt
        // under the named ring key. A missing key id or any tag/AAD failure is hard (no fallback).
        if ciphertext.len() >= VERSIONED_PREFIX && ciphertext[..MAGIC.len()] == MAGIC {
            let key_id = u16::from_be_bytes([ciphertext[4], ciphertext[5]]);
            let nonce_bytes = &ciphertext[HEADER_LEN..VERSIONED_PREFIX];
            let ct = &ciphertext[VERSIONED_PREFIX..];
            let aad = &ciphertext[..HEADER_LEN];
            let cipher = self.ring.get(&key_id).ok_or(CryptoError::Decrypt)?;
            return self.decrypt(cipher, ct, nonce_bytes, Some(aad));
        }
        // Legacy (pre-#29) blob: no magic, nonce stored separately, no AAD, legacy key only.
        let cipher = self.ring.get(&self.legacy_id).ok_or(CryptoError::Decrypt)?;
        self.decrypt(cipher, ciphertext, &nonce.0, None)
    }

    fn ct_eq(&self, a: &[u8], b: &[u8]) -> bool {
        // `subtle::ConstantTimeEq` on slices already folds in a length check and compares the
        // contents in constant time for equal-length inputs.
        a.ct_eq(b).into()
    }

    fn active_key_id(&self) -> u16 {
        self.active_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crypto() -> SystemCrypto {
        SystemCrypto::new([7u8; KEY_LEN])
    }

    /// A legacy (pre-#29) sealed blob: raw AES-GCM with no AAD and a separately-stored nonce.
    fn legacy_blob(key: [u8; KEY_LEN], nonce: [u8; NONCE_LEN], plaintext: &[u8]) -> Vec<u8> {
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));
        cipher
            .encrypt(GcmNonce::from_slice(&nonce), plaintext)
            .expect("legacy seal")
    }

    #[test]
    fn versioned_round_trips_and_envelope_shape() {
        let c = crypto();
        let plaintext = b"a SigV4 secret access key";
        let sealed = c.seal(plaintext).expect("seal");
        assert_eq!(&sealed.ciphertext[..4], &MAGIC, "CRK1 magic prefix");
        assert_eq!(
            u16::from_be_bytes([sealed.ciphertext[4], sealed.ciphertext[5]]),
            1,
            "active key id in envelope"
        );
        assert_eq!(
            sealed.ciphertext.len(),
            VERSIONED_PREFIX + plaintext.len() + 16
        );
        let opened = c.open(&sealed.ciphertext, &sealed.nonce).expect("open");
        assert_eq!(opened, plaintext);
    }

    #[test]
    fn versioned_round_trips_empty() {
        let c = crypto();
        let sealed = c.seal(b"").expect("seal empty");
        assert_eq!(sealed.ciphertext.len(), VERSIONED_PREFIX + 16);
        assert!(
            c.open(&sealed.ciphertext, &sealed.nonce)
                .expect("open")
                .is_empty()
        );
    }

    #[test]
    fn two_seals_of_same_plaintext_differ() {
        let c = crypto();
        let p = b"deterministic plaintext";
        let s1 = c.seal(p).expect("seal 1");
        let s2 = c.seal(p).expect("seal 2");
        assert_ne!(
            s1.ciphertext, s2.ciphertext,
            "fresh nonce -> distinct envelopes"
        );
        assert_eq!(c.open(&s1.ciphertext, &s1.nonce).expect("open 1"), p);
        assert_eq!(c.open(&s2.ciphertext, &s2.nonce).expect("open 2"), p);
    }

    #[test]
    fn legacy_blob_round_trips_under_legacy_key() {
        // A blob sealed the old way (no magic) opens via the legacy path; the nonce is supplied
        // separately exactly as the old storage held it.
        let key = [7u8; KEY_LEN];
        let nonce = [9u8; NONCE_LEN];
        let ct = legacy_blob(key, nonce, b"legacy secret");
        let c = SystemCrypto::new(key); // legacy_id = 1; key 1 == this key
        let opened = c.open(&ct, &Nonce(nonce.to_vec())).expect("legacy open");
        assert_eq!(opened, b"legacy secret");
    }

    #[test]
    fn aad_binds_key_id_so_repointing_fails() {
        // Two-key ring; seal under id 1, then repoint the envelope's key_id to 2. Decryption
        // under key 2 with the now-mismatched AAD must fail (no silent reinterpretation).
        let c = SystemCrypto::from_ring(vec![(1, [1u8; KEY_LEN]), (2, [2u8; KEY_LEN])], 1, 1, 0)
            .expect("ring");
        let mut sealed = c.seal(b"bound").expect("seal");
        assert_eq!(sealed.ciphertext[5], 1);
        sealed.ciphertext[5] = 2; // repoint key_id 1 -> 2 (a live ring id)
        assert!(matches!(
            c.open(&sealed.ciphertext, &sealed.nonce),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn tampered_magic_routes_legacy_and_fails() {
        let c = crypto();
        let mut sealed = c.seal(b"x").expect("seal");
        sealed.ciphertext[0] ^= 0x01; // no longer "CRK1" -> legacy path
        // Legacy path uses the separate nonce + no AAD; the bytes are not a valid legacy ct.
        assert!(matches!(
            c.open(&sealed.ciphertext, &sealed.nonce),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn missing_key_id_fails_closed_no_fallback() {
        let c = crypto(); // single key id 1
        let mut sealed = c.seal(b"y").expect("seal");
        sealed.ciphertext[4] = 0x03;
        sealed.ciphertext[5] = 0xE7; // key_id 999, absent from the ring
        assert!(matches!(
            c.open(&sealed.ciphertext, &sealed.nonce),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn legacy_prefix_collision_is_fail_closed() {
        // A random legacy ct could (2^-32) start with "CRK1". Such a false-positive is classified
        // versioned, its random 2-byte id is almost never live, and it fails closed regardless.
        let c = crypto();
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&999u16.to_be_bytes());
        buf.extend_from_slice(&[0u8; NONCE_LEN + 16]); // nonce + a 16-byte "tag"
        assert!(matches!(
            c.open(&buf, &Nonce(vec![])),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn tampered_ciphertext_and_tag_fail() {
        let c = crypto();
        let mut s = c.seal(b"tamper me please").expect("seal");
        s.ciphertext[VERSIONED_PREFIX] ^= 0x01; // first ct byte
        assert!(matches!(
            c.open(&s.ciphertext, &s.nonce),
            Err(CryptoError::Decrypt)
        ));
        let mut s2 = c.seal(b"tag tamper").expect("seal");
        let last = s2.ciphertext.len() - 1;
        s2.ciphertext[last] ^= 0x80;
        assert!(matches!(
            c.open(&s2.ciphertext, &s2.nonce),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn wrong_legacy_nonce_len_fails() {
        let key = [3u8; KEY_LEN];
        let nonce = [4u8; NONCE_LEN];
        let ct = legacy_blob(key, nonce, b"len");
        let c = SystemCrypto::new(key);
        assert!(matches!(
            c.open(&ct, &Nonce(vec![0u8; NONCE_LEN - 1])),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn wrong_key_fails() {
        let a = SystemCrypto::new([1u8; KEY_LEN]);
        let b = SystemCrypto::new([2u8; KEY_LEN]);
        let sealed = a.seal(b"cross key").expect("seal");
        // b's ring has key id 1 but with different bytes -> tag fails.
        assert!(matches!(
            b.open(&sealed.ciphertext, &sealed.nonce),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn seal_count_increments_and_primes() {
        let c = crypto();
        assert_eq!(c.seal_count(), 0);
        c.seal(b"a").expect("seal");
        c.seal(b"b").expect("seal");
        assert_eq!(c.seal_count(), 2);
        c.prime_seal_count(100);
        assert_eq!(c.seal_count(), 102, "base + in-process");
    }

    #[test]
    fn seal_hard_stops_at_ceiling() {
        let c = crypto();
        // Prime just below the hard stop; the next seal crosses it.
        c.prime_seal_count(STOP_THRESHOLD - 1);
        assert!(matches!(
            c.seal(b"z"),
            Err(CryptoError::KeyRotationRequired)
        ));
        // Opens are never blocked even past the stop.
        let key = [7u8; KEY_LEN];
        let nonce = [1u8; NONCE_LEN];
        let ct = legacy_blob(key, nonce, b"still readable");
        assert_eq!(
            c.open(&ct, &Nonce(nonce.to_vec())).expect("open"),
            b"still readable"
        );
    }

    #[test]
    fn from_ring_validation() {
        let k = |b: u8| (b as u16 + 1, [b; KEY_LEN]);
        assert!(SystemCrypto::from_ring(vec![], 1, 1, 0).is_err(), "empty");
        assert!(
            SystemCrypto::from_ring(vec![(0, [1u8; KEY_LEN])], 0, 0, 0).is_err(),
            "id 0"
        );
        assert!(
            SystemCrypto::from_ring(vec![k(0), k(0)], 1, 1, 0).is_err(),
            "dup id"
        );
        assert!(
            SystemCrypto::from_ring(vec![k(0)], 9, 1, 0).is_err(),
            "active absent"
        );
        assert!(
            SystemCrypto::from_ring(vec![k(0)], 1, 9, 0).is_err(),
            "legacy absent"
        );
        assert!(SystemCrypto::from_ring(vec![k(0), k(1)], 2, 1, 0).is_ok());
    }

    #[test]
    fn ring_seals_under_active_and_opens_legacy_under_legacy() {
        // active=2, legacy=1. New seals carry key_id 2; a legacy blob made with key 1 opens.
        let key1 = [1u8; KEY_LEN];
        let c =
            SystemCrypto::from_ring(vec![(1, key1), (2, [2u8; KEY_LEN])], 2, 1, 0).expect("ring");
        let sealed = c.seal(b"new").expect("seal");
        assert_eq!(sealed.ciphertext[5], 2, "sealed under active id 2");
        assert_eq!(
            c.open(&sealed.ciphertext, &sealed.nonce).expect("open"),
            b"new"
        );
        let nonce = [5u8; NONCE_LEN];
        let legacy = legacy_blob(key1, nonce, b"old");
        assert_eq!(
            c.open(&legacy, &Nonce(nonce.to_vec())).expect("legacy"),
            b"old"
        );
    }

    #[test]
    fn debug_redacts_key_material() {
        let c = SystemCrypto::from_ring(vec![(1, [0xAB; KEY_LEN]), (2, [0xCD; KEY_LEN])], 2, 1, 0)
            .expect("ring");
        let s = format!("{c:?}");
        assert!(s.contains("redacted"));
        assert!(
            !s.contains("abab") && !s.contains("cdcd"),
            "no key bytes leak: {s}"
        );
    }

    #[test]
    fn ct_eq_semantics() {
        let c = crypto();
        assert!(c.ct_eq(b"abcdef", b"abcdef"));
        assert!(c.ct_eq(b"", b""));
        assert!(!c.ct_eq(b"abcdef", b"abcdeg"));
        assert!(!c.ct_eq(b"abc", b"abcd"));
    }

    #[test]
    fn from_hex_round_trips_and_rejects_bad() {
        let key = [0xABu8; KEY_LEN];
        let c = SystemCrypto::from_hex(&hex::encode(key)).expect("from_hex");
        let reference = SystemCrypto::new(key);
        let sealed = reference.seal(b"hk").expect("seal");
        assert_eq!(
            c.open(&sealed.ciphertext, &sealed.nonce).expect("open"),
            b"hk"
        );
        assert_eq!(
            SystemCrypto::from_hex("zz").err(),
            Some(KeyError::Malformed)
        );
        assert_eq!(
            SystemCrypto::from_hex("abcd").err(),
            Some(KeyError::WrongLength)
        );
    }
}
