//! Streaming content hashing: always the MD5 that becomes the ETag, plus the internal SHA-256
//! for durable objects and client-requested checksum algorithms, computed once over the
//! plaintext. Multipart parts drop the internal SHA-256 because their baseline is recomputed
//! over the assembled object.

use crate::crc64nvme::Crc64Nvme;
use aws_lc_rs::digest::{Context as Sha256Context, SHA256};
use base64::Engine;
use cairn_types::object::{ChecksumAlgorithm, ChecksumSet, ChecksumValue};
use md5::{Digest, Md5};
use sha1::Sha1;

/// Accumulates MD5, internal SHA-256, and requested supplementary checksums over plaintext.
pub struct Hashers {
    md5: Md5,
    crc32: Option<crc32fast::Hasher>,
    crc32c: Option<u32>,
    crc64nvme: Option<Crc64Nvme>,
    sha1: Option<Sha1>,
    /// The internal-baseline SHA-256, or the client-requested SHA-256. `None` only on a multipart
    /// part where neither the object baseline nor an exposed checksum needs it.
    sha256: Option<Sha256Context>,
    expose_sha256: bool,
}

impl std::fmt::Debug for Hashers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hashers").finish_non_exhaustive()
    }
}

impl Hashers {
    /// Build always-on MD5/SHA-256 plus requested supplementary algorithms.
    #[must_use]
    pub fn new(set: &ChecksumSet) -> Self {
        let has = |a| set.0.contains(&a);
        Self {
            md5: Md5::new(),
            crc32: has(ChecksumAlgorithm::Crc32).then(crc32fast::Hasher::new),
            crc32c: has(ChecksumAlgorithm::Crc32c).then_some(0),
            crc64nvme: has(ChecksumAlgorithm::Crc64Nvme).then(Crc64Nvme::new),
            sha1: has(ChecksumAlgorithm::Sha1).then(Sha1::new),
            sha256: Some(Sha256Context::new(&SHA256)),
            expose_sha256: has(ChecksumAlgorithm::Sha256),
        }
    }

    /// Build the hasher for a multipart part. A part's internal SHA-256 is discarded when the
    /// object is assembled, so compute it only when the client requested SHA-256 (or the signed
    /// payload must be verified); MD5 and every requested checksum are unaffected.
    #[must_use]
    pub fn new_for_part(set: &ChecksumSet) -> Self {
        let mut hashers = Self::new(set);
        if !hashers.expose_sha256 {
            hashers.sha256 = None;
        }
        hashers
    }

    /// Feed plaintext bytes.
    pub fn update(&mut self, data: &[u8]) {
        self.md5.update(data);
        if let Some(h) = &mut self.crc32 {
            h.update(data);
        }
        if let Some(c) = &mut self.crc32c {
            *c = crc32c::crc32c_append(*c, data);
        }
        if let Some(h) = &mut self.crc64nvme {
            h.update(data);
        }
        if let Some(h) = &mut self.sha1 {
            h.update(data);
        }
        if let Some(h) = &mut self.sha256 {
            h.update(data);
        }
    }

    /// Finalize, returning the hex MD5 (for the ETag and Content-MD5) and the base64-encoded
    /// supplementary checksums, followed by the internal hex SHA-256. The latter never changes
    /// which supplementary algorithms appear in the returned vector.
    #[must_use]
    pub fn finalize(self) -> (String, Vec<ChecksumValue>, String) {
        let b64 = base64::engine::general_purpose::STANDARD;
        let md5_hex = hex::encode(self.md5.finalize());
        let mut checksums = Vec::new();
        if let Some(h) = self.crc32 {
            checksums.push(ChecksumValue {
                algorithm: ChecksumAlgorithm::Crc32,
                value: b64.encode(h.finalize().to_be_bytes()),
            });
        }
        if let Some(c) = self.crc32c {
            checksums.push(ChecksumValue {
                algorithm: ChecksumAlgorithm::Crc32c,
                value: b64.encode(c.to_be_bytes()),
            });
        }
        if let Some(h) = self.crc64nvme {
            checksums.push(ChecksumValue {
                algorithm: ChecksumAlgorithm::Crc64Nvme,
                value: b64.encode(h.finalize()),
            });
        }
        if let Some(h) = self.sha1 {
            checksums.push(ChecksumValue {
                algorithm: ChecksumAlgorithm::Sha1,
                value: b64.encode(h.finalize()),
            });
        }
        let sha256 = self.sha256.map(|h| h.finish());
        if self.expose_sha256 {
            let digest = sha256
                .as_ref()
                .expect("expose_sha256 implies the SHA-256 hasher is present");
            checksums.push(ChecksumValue {
                algorithm: ChecksumAlgorithm::Sha256,
                value: b64.encode(digest.as_ref()),
            });
        }
        let internal_sha256 = sha256
            .as_ref()
            .map(|digest| hex::encode(digest.as_ref()))
            .unwrap_or_default();
        (md5_hex, checksums, internal_sha256)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_sha256_is_always_recorded_without_exposing_a_client_checksum() {
        let mut h = Hashers::new(&ChecksumSet::none());
        h.update(b"a");
        h.update(b"bc");
        let (_, checks, internal) = h.finalize();
        assert!(checks.is_empty());
        assert_eq!(
            internal,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn supplementary_checksums_present_when_requested() {
        let set = ChecksumSet(vec![ChecksumAlgorithm::Sha256, ChecksumAlgorithm::Crc32]);
        let mut h = Hashers::new(&set);
        h.update(b"abc");
        let (md5, checks, internal) = h.finalize();
        assert_eq!(
            internal,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(md5, "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(checks.len(), 2);
        // SHA-256("abc") base64
        let sha = checks
            .iter()
            .find(|c| c.algorithm == ChecksumAlgorithm::Sha256)
            .unwrap();
        assert_eq!(sha.value, "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=");
    }

    #[test]
    fn part_hasher_skips_the_unused_internal_sha256() {
        let mut h = Hashers::new_for_part(&ChecksumSet::none());
        h.update(b"abc");
        let (md5, checks, internal) = h.finalize();
        assert_eq!(md5, "900150983cd24fb0d6963f7d28e17f72");
        assert!(checks.is_empty());
        assert!(internal.is_empty());
    }

    #[test]
    fn part_hasher_keeps_a_requested_sha256() {
        let set = ChecksumSet(vec![ChecksumAlgorithm::Sha256]);
        let mut h = Hashers::new_for_part(&set);
        h.update(b"abc");
        let (_, checks, internal) = h.finalize();
        assert_eq!(
            internal,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(checks.len(), 1);
    }
}
