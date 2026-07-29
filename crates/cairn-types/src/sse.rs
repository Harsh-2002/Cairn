//! The persisted server-side-encryption descriptor and the one place that knows how to reopen a
//! sealed data-encryption key (ARCH 27).
//!
//! The `sse_descriptor` JSON on an [`ObjectVersionRow`](crate::object::ObjectVersionRow) is written
//! by `cairn-protocol`, re-sealed by the `cairn-server` master-key re-wrap worker, and read by
//! every consumer that needs the object's plaintext (S3 GET, copy, replication). It used to be
//! hand-copied in three crates; a consumer that did not know about it read **raw ciphertext** and
//! shipped it. The format and [`open_dek`] live here so there is exactly one copy.

use crate::SecretKey32;
use crate::blob::BlobCipher;
use crate::crypto::Nonce;
use crate::error::CryptoError;
use crate::traits::Crypto;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use std::collections::BTreeMap;

/// The authenticated encrypted CRNB format emitted by every current write.
pub const AUTHENTICATED_BLOB_FORMAT_VERSION: u8 = 3;
/// Prefix on a current multipart part's sealed-DEK metadata. Standard base64 never contains `:`,
/// so an unprefixed value is unambiguously a legacy CRNB-v2 part.
pub const AUTHENTICATED_PART_DEK_PREFIX: &str = "crnb3:";

/// How an object version came to be encrypted, which decides what (if anything) GET/HEAD advertise
/// to the client. The DEK envelope is identical for all three (CRK1-under-master) — this is a
/// *labelling* discriminator, not distinct key material (SSE-KMS v1 is label-only, ARCH 27).
///   * `SseS3`  — the client asked for `x-amz-server-side-encryption: AES256`; advertise `AES256`.
///   * `AtRest` — transparent server-side-at-rest encryption the client did NOT request; advertise
///     nothing (it is an operator storage property, not an SSE contract the client can rely on).
///   * `Kms`    — the client asked for `aws:kms`; advertise `aws:kms` + the key id.
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq, Debug, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SseMode {
    /// The client asked for `AES256` (or a bucket default did).
    #[default]
    SseS3,
    /// Transparent operator at-rest encryption; advertised to nobody.
    AtRest,
    /// The client asked for `aws:kms` (label-only in v1).
    Kms,
}

fn is_default_sse_mode(m: &SseMode) -> bool {
    *m == SseMode::SseS3
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// The JSON `sse_descriptor` persisted on an encrypted object version: the algorithm, the
/// data-encryption key sealed under the master key (base64), and the wrapping nonce (base64). The
/// raw DEK is never stored — only this wrapped form (ARCH 27, SSE-S3).
///
/// `mode`/`kms_key_id`/`blob_format_version` are additive and skipped when absent/default. In
/// particular, an absent format marker is the trusted compatibility signal for a descriptor written
/// before authenticated CRNB v3; current writes persist `3`. The reader requires that metadata-backed
/// expectation instead of trusting the mutable version byte inside the blob itself.
///
/// [`extra`](Self::extra) is load-bearing, not decoration: the re-wrap worker deserializes,
/// re-seals, and re-serializes this struct, so any field written by a *newer* node that this binary
/// does not know about would be erased by a rotation without it.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct SseDescriptor {
    /// The content-encryption algorithm (`AES256-GCM`).
    pub alg: String,
    /// The DEK sealed under the master ring, base64. For a CRK1 envelope this is the whole
    /// self-describing envelope (key id + nonce inside).
    pub wrapped_dek_b64: String,
    /// The wrapping nonce, base64 — empty for a CRK1 envelope, populated for a legacy (pre-#29)
    /// one. [`open_dek`] routes on its presence.
    #[serde(default)]
    pub nonce_b64: String,
    /// The advertising discriminator.
    #[serde(default, skip_serializing_if = "is_default_sse_mode")]
    pub mode: SseMode,
    /// The SSE-KMS key id label (SSE-KMS only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kms_key_id: Option<String>,
    /// The `x-amz-server-side-encryption-bucket-key-enabled` flag, echoed on read so a round-trip
    /// GET does not drop it (SSE-KMS only). Additive: absent/false serializes away, so a legacy or
    /// SSE-S3 descriptor is byte-identical.
    #[serde(default, skip_serializing_if = "is_false")]
    pub bucket_key_enabled: bool,
    /// Expected encrypted CRNB container version. `None` means a descriptor written by the legacy
    /// v2 writer; current writes set `Some(3)`. Any other explicit value fails closed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_format_version: Option<u8>,
    /// Every field this binary does not know about, captured verbatim and re-emitted unchanged so a
    /// re-wrap (or any other read-modify-write) can never drop a label written by a newer node.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// Unwrap the raw 32-byte DEK from a stored [`SseDescriptor`] by opening the sealed key under the
/// master ring.
///
/// v1 is label-only: every DEK (`SseS3`, `AtRest`, `Kms`) is sealed under the SAME master ring, so
/// opening on the node's crypto is correct and symmetric with the seal path. A FUTURE external key
/// provider with per-key material would resolve the crypto from `kms_key_id` first — until then
/// this fails **closed** (a typed [`CryptoError`], never plaintext, never zeros).
///
/// # Errors
/// [`CryptoError::Decrypt`] for a malformed envelope, bad base64, a tampered tag, or an unwrapped
/// key that is not 32 bytes; [`CryptoError::UnknownKeyId`] when the sealing key is simply not on
/// this node's ring (a rotation window — retryable, not tampering).
pub fn open_dek(crypto: &dyn Crypto, d: &SseDescriptor) -> Result<SecretKey32, CryptoError> {
    let ciphertext = B64
        .decode(d.wrapped_dek_b64.as_bytes())
        .map_err(|_| CryptoError::Decrypt)?;
    // A CRK1 envelope leaves `nonce_b64` empty (its nonce is inside the envelope and `open`
    // ignores this argument for it); a legacy descriptor carries the nonce separately (#29).
    let nonce_bytes = if d.nonce_b64.is_empty() {
        Vec::new()
    } else {
        B64.decode(d.nonce_b64.as_bytes())
            .map_err(|_| CryptoError::Decrypt)?
    };
    let raw = crypto.open(&ciphertext, &Nonce(nonce_bytes))?;
    SecretKey32::from_slice(raw.as_slice()).ok_or(CryptoError::Decrypt)
}

/// Open an object's DEK together with the encrypted CRNB format its persisted descriptor authorizes.
///
/// A missing marker is deliberately interpreted as legacy v2 for backward compatibility. A marker
/// can never be inferred from the on-disk trailer: doing so would let a current authenticated blob
/// select the unauthenticated legacy parser after its framing was modified.
pub fn open_blob_cipher(crypto: &dyn Crypto, d: &SseDescriptor) -> Result<BlobCipher, CryptoError> {
    let legacy = match d.blob_format_version {
        None => true,
        Some(AUTHENTICATED_BLOB_FORMAT_VERSION) => false,
        Some(_) => return Err(CryptoError::Decrypt),
    };
    let dek = open_dek(crypto, d)?;
    Ok(if legacy {
        BlobCipher::LegacyV2(dek)
    } else {
        BlobCipher::AuthenticatedV3(dek)
    })
}

/// Parse and open an encrypted object's persisted descriptor into the only blob-read declaration
/// accepted by the storage layer.
pub fn parse_and_open_blob_cipher(
    crypto: &dyn Crypto,
    json: &str,
) -> Result<BlobCipher, CryptoError> {
    let descriptor = parse_descriptor(json)?;
    open_blob_cipher(crypto, &descriptor)
}

/// Seal a newly-staged multipart part's DEK and stamp the metadata with the current authenticated
/// CRNB version. Legacy rows contain only the base64 envelope and remain readable as v2.
pub fn seal_part_blob_cipher(
    crypto: &dyn Crypto,
    dek: &SecretKey32,
) -> Result<String, CryptoError> {
    let sealed = crypto.seal(dek.expose_secret())?;
    Ok(format!(
        "{AUTHENTICATED_PART_DEK_PREFIX}{}",
        B64.encode(sealed.ciphertext)
    ))
}

/// Open a persisted multipart-part DEK and return its metadata-backed CRNB expectation.
///
/// Unprefixed standard base64 is the legacy-v2 representation. The current `crnb3:` prefix selects
/// authenticated v3, while any other colon-bearing prefix is rejected so future/garbled formats
/// cannot silently fall back to legacy behavior.
pub fn open_part_blob_cipher(
    crypto: &dyn Crypto,
    persisted: &str,
) -> Result<BlobCipher, CryptoError> {
    let (encoded, authenticated) =
        if let Some(encoded) = persisted.strip_prefix(AUTHENTICATED_PART_DEK_PREFIX) {
            (encoded, true)
        } else if persisted.contains(':') {
            return Err(CryptoError::Decrypt);
        } else {
            (persisted, false)
        };
    let ciphertext = B64
        .decode(encoded.as_bytes())
        .map_err(|_| CryptoError::Decrypt)?;
    let raw = crypto.open(&ciphertext, &Nonce(Vec::new()))?;
    let dek = SecretKey32::from_slice(raw.as_slice()).ok_or(CryptoError::Decrypt)?;
    Ok(if authenticated {
        BlobCipher::AuthenticatedV3(dek)
    } else {
        BlobCipher::LegacyV2(dek)
    })
}

/// Parse a stored `sse_descriptor` JSON document.
///
/// # Errors
/// [`CryptoError::Decrypt`] when the document is not a valid descriptor — a malformed descriptor is
/// permanently unreadable, never a transient condition.
pub fn parse_descriptor(json: &str) -> Result<SseDescriptor, CryptoError> {
    serde_json::from_str(json).map_err(|_| CryptoError::Decrypt)
}

#[cfg(test)]
mod tests {
    use super::{
        AUTHENTICATED_BLOB_FORMAT_VERSION, AUTHENTICATED_PART_DEK_PREFIX, SseDescriptor, SseMode,
        open_blob_cipher, open_dek, open_part_blob_cipher, parse_descriptor, seal_part_blob_cipher,
    };
    use crate::blob::BlobCipher;
    use crate::error::CryptoError;
    use crate::traits::Crypto;

    /// A ring-less crypto double: `seal` XORs, `open` XORs back only when the envelope carries the
    /// expected marker byte, otherwise it reports the key id as absent from the ring.
    struct RingCrypto(u8);

    impl Crypto for RingCrypto {
        fn seal(&self, plaintext: &[u8]) -> Result<crate::crypto::Sealed, CryptoError> {
            let mut ct = vec![self.0];
            ct.extend(plaintext.iter().map(|b| b ^ 0x5a));
            Ok(crate::crypto::Sealed {
                ciphertext: ct,
                nonce: crate::crypto::Nonce(Vec::new()),
            })
        }
        fn open(
            &self,
            ciphertext: &[u8],
            _nonce: &crate::crypto::Nonce,
        ) -> Result<zeroize::Zeroizing<Vec<u8>>, CryptoError> {
            match ciphertext.split_first() {
                Some((id, rest)) if *id == self.0 => Ok(zeroize::Zeroizing::new(
                    rest.iter().map(|b| b ^ 0x5a).collect(),
                )),
                Some(_) => Err(CryptoError::UnknownKeyId),
                None => Err(CryptoError::Decrypt),
            }
        }
        fn ct_eq(&self, a: &[u8], b: &[u8]) -> bool {
            a == b
        }
    }

    fn descriptor_for(crypto: &RingCrypto, dek: &[u8; 32]) -> SseDescriptor {
        use base64::Engine;
        SseDescriptor {
            alg: "AES256-GCM".to_owned(),
            wrapped_dek_b64: super::B64.encode(&crypto.seal(dek).unwrap().ciphertext),
            ..SseDescriptor::default()
        }
    }

    #[test]
    fn open_dek_round_trips_and_fails_closed_on_the_wrong_ring() {
        let dek = [7u8; 32];
        let ring_a = RingCrypto(1);
        let d = descriptor_for(&ring_a, &dek);
        assert_eq!(open_dek(&ring_a, &d).unwrap().expose_secret(), &dek);
        // A DIFFERENT ring must not yield plaintext, zeros, or partial data — and must say
        // "unknown key id", not "decrypt failure", so callers can treat a rotation window as
        // transient instead of stamping the object permanently corrupt.
        let ring_b = RingCrypto(2);
        assert!(matches!(
            open_dek(&ring_b, &d),
            Err(CryptoError::UnknownKeyId)
        ));
    }

    #[test]
    fn bad_base64_and_short_keys_fail_closed() {
        let mut d = descriptor_for(&RingCrypto(1), &[3u8; 32]);
        d.wrapped_dek_b64 = "not base64!!".to_owned();
        assert!(matches!(
            open_dek(&RingCrypto(1), &d),
            Err(CryptoError::Decrypt)
        ));
        // An envelope that opens to the wrong length is never a usable DEK.
        use base64::Engine;
        d.wrapped_dek_b64 = super::B64.encode(RingCrypto(1).seal(b"short").unwrap().ciphertext);
        assert!(matches!(
            open_dek(&RingCrypto(1), &d),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn legacy_descriptor_round_trips_byte_identically() {
        // The additive fields must serialize away entirely, so a legacy (pre-mode) descriptor is
        // re-emitted byte-for-byte. Anything else rewrites every row on the first rotation.
        let json = r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":""}"#;
        let d: SseDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(d.mode, SseMode::SseS3);
        assert_eq!(d.blob_format_version, None);
        assert_eq!(serde_json::to_string(&d).unwrap(), json);
    }

    #[test]
    fn descriptor_marker_selects_one_exact_encrypted_format() {
        let crypto = RingCrypto(1);
        let mut d = descriptor_for(&crypto, &[4u8; 32]);
        assert!(matches!(
            open_blob_cipher(&crypto, &d),
            Ok(BlobCipher::LegacyV2(_))
        ));

        d.blob_format_version = Some(AUTHENTICATED_BLOB_FORMAT_VERSION);
        assert!(matches!(
            open_blob_cipher(&crypto, &d),
            Ok(BlobCipher::AuthenticatedV3(_))
        ));

        d.blob_format_version = Some(99);
        assert!(matches!(
            open_blob_cipher(&crypto, &d),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn multipart_part_marker_is_additive_and_unknown_prefixes_fail_closed() {
        use base64::Engine;

        let crypto = RingCrypto(1);
        let dek = crate::SecretKey32::new([8u8; 32]);
        let current = seal_part_blob_cipher(&crypto, &dek).unwrap();
        assert!(current.starts_with(AUTHENTICATED_PART_DEK_PREFIX));
        assert!(matches!(
            open_part_blob_cipher(&crypto, &current),
            Ok(BlobCipher::AuthenticatedV3(_))
        ));

        let legacy = super::B64.encode(crypto.seal(dek.expose_secret()).unwrap().ciphertext);
        assert!(matches!(
            open_part_blob_cipher(&crypto, &legacy),
            Ok(BlobCipher::LegacyV2(_))
        ));
        assert!(matches!(
            open_part_blob_cipher(&crypto, "crnb4:AAAA"),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn unknown_fields_survive_a_round_trip() {
        // `extra` is why a rotation performed by an OLDER node cannot erase a field a newer node
        // wrote. Dropping the flatten would silently discard `future_label` here.
        let json = r#"{"alg":"A","wrapped_dek_b64":"B","nonce_b64":"","mode":"at-rest","kms_key_id":"k","bucket_key_enabled":true,"future_label":"keep-me"}"#;
        let d = parse_descriptor(json).unwrap();
        assert_eq!(d.mode, SseMode::AtRest);
        assert_eq!(d.kms_key_id.as_deref(), Some("k"));
        assert!(d.bucket_key_enabled);
        let out = serde_json::to_string(&d).unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["future_label"], "keep-me", "unknown field dropped: {out}");
    }

    #[test]
    fn a_malformed_descriptor_is_not_transient() {
        assert!(matches!(parse_descriptor("{"), Err(CryptoError::Decrypt)));
    }
}
