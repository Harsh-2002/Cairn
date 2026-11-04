//! Streaming content hashing: always the MD5 that becomes the ETag, the internal BLAKE3 baseline
//! for new objects, and client-requested checksum algorithms, computed once over the plaintext.
//!
//! Multipart parts drop the internal baseline because their baseline is recomputed over the
//! assembled object. Legacy SHA-256 is computed only when the client requested SHA-256 (or the
//! signed payload needs verification) and when scrubbing a row that predates BLAKE3.

use crate::crc64nvme::Crc64Nvme;
use aws_lc_rs::digest::{Context as Sha256Context, SHA256};
use base64::Engine;
use blake3::Hasher as Blake3Hasher;
use cairn_types::object::{ChecksumAlgorithm, ChecksumSet, ChecksumValue};
use md5::{Digest, Md5};
use sha1::Sha1;

/// The digests produced by one streaming pass. Exactly the internal baseline that the caller needs
/// is present: BLAKE3 for a new whole object, SHA-256 for a legacy scrub or a client request, and
/// never a value silently promoted from one algorithm to the other.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct FinalizedHashes {
    /// The plaintext MD5 (ETag basis), absent only when the caller asked to skip it.
    pub md5_hex: Option<String>,
    /// Requested supplementary checksums.
    pub checksums: Vec<ChecksumValue>,
    /// The internal plaintext BLAKE3 baseline (hex), when computed.
    pub internal_blake3: Option<String>,
    /// The internal or client-requested plaintext SHA-256 (hex), when computed.
    pub internal_sha256: Option<String>,
}

/// Accumulates MD5, internal BLAKE3, optional SHA-256, and requested supplementary checksums over
/// plaintext.
pub struct Hashers {
    md5: Option<Md5>,
    crc32: Option<crc32fast::Hasher>,
    crc32c: Option<u32>,
    crc64nvme: Option<Crc64Nvme>,
    sha1: Option<Sha1>,
    /// The always-on internal BLAKE3 baseline for a new whole object. `None` on a multipart part.
    blake3: Option<Blake3Hasher>,
    /// The internal-baseline SHA-256 (legacy scrub), or the client-requested SHA-256. `None` when
    /// neither a legacy row nor an exposed checksum needs it.
    sha256: Option<Sha256Context>,
    expose_sha256: bool,
}

impl std::fmt::Debug for Hashers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hashers").finish_non_exhaustive()
    }
}

impl Hashers {
    /// Build always-on MD5/BLAKE3 plus requested supplementary algorithms. SHA-256 is computed only
    /// when the client requested it (or a signed payload needs verification, which the protocol
    /// expresses by adding SHA-256 to the requested set).
    #[must_use]
    pub fn new(set: &ChecksumSet) -> Self {
        let has = |a| set.0.contains(&a);
        Self {
            md5: Some(Md5::new()),
            crc32: has(ChecksumAlgorithm::Crc32).then(crc32fast::Hasher::new),
            crc32c: has(ChecksumAlgorithm::Crc32c).then_some(0),
            crc64nvme: has(ChecksumAlgorithm::Crc64Nvme).then(Crc64Nvme::new),
            sha1: has(ChecksumAlgorithm::Sha1).then(Sha1::new),
            blake3: Some(Blake3Hasher::new()),
            sha256: has(ChecksumAlgorithm::Sha256).then(|| Sha256Context::new(&SHA256)),
            expose_sha256: has(ChecksumAlgorithm::Sha256),
        }
    }

    /// Build the hasher for a multipart part. A part's internal baseline is discarded when the
    /// object is assembled, so neither BLAKE3 nor SHA-256 is computed unless the client requested
    /// SHA-256 (or the signed payload must be verified); MD5 and every requested checksum are
    /// unaffected.
    #[must_use]
    pub fn new_for_part(set: &ChecksumSet) -> Self {
        let mut hashers = Self::new(set);
        hashers.blake3 = None;
        if !hashers.expose_sha256 {
            hashers.sha256 = None;
        }
        hashers
    }

    /// Build the hasher for assembly. The composite multipart ETag is computed from the per-part
    /// MD5s, not from the concatenated bytes, so an assembly with an explicit ETag skips the
    /// whole-object MD5 pass while still computing the internal BLAKE3 baseline and requested
    /// checksums.
    #[must_use]
    pub fn new_for_assembly(set: &ChecksumSet, compute_md5: bool) -> Self {
        let mut hashers = Self::new(set);
        if !compute_md5 {
            hashers.md5 = None;
        }
        hashers
    }

    /// Build the hasher for scrub verification. BLAKE3 is always computed for the new baseline;
    /// SHA-256 is also computed when the row carries only a legacy SHA-256 baseline. A row that
    /// somehow carries both is resolved by the caller in favour of BLAKE3.
    #[must_use]
    pub fn new_for_scrub(set: &ChecksumSet, legacy_sha256: bool) -> Self {
        let mut hashers = Self::new(set);
        if legacy_sha256 && hashers.sha256.is_none() {
            hashers.sha256 = Some(Sha256Context::new(&SHA256));
        }
        hashers
    }

    /// Feed plaintext bytes.
    pub fn update(&mut self, data: &[u8]) {
        if let Some(md5) = &mut self.md5 {
            md5.update(data);
        }
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
        if let Some(h) = &mut self.blake3 {
            h.update(data);
        }
        if let Some(h) = &mut self.sha256 {
            h.update(data);
        }
    }

    /// Finalize, returning the MD5 when computed, the base64-encoded supplementary checksums, the
    /// BLAKE3 internal baseline, and the optional SHA-256. Neither internal digest ever changes
    /// which supplementary algorithms appear in the returned vector.
    #[must_use]
    pub fn finalize(self) -> FinalizedHashes {
        let b64 = base64::engine::general_purpose::STANDARD;
        let md5_hex = self.md5.map(|md5| hex::encode(md5.finalize()));
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
        let internal_blake3 = self
            .blake3
            .map(|hasher| hasher.finalize().to_hex().to_string());
        let internal_sha256 = sha256.as_ref().map(|digest| hex::encode(digest.as_ref()));
        FinalizedHashes {
            md5_hex,
            checksums,
            internal_blake3,
            internal_sha256,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// BLAKE3 of "abc" (known vector):
    /// <https://github.com/BLAKE3-team/BLAKE3/blob/master/test_vectors/test_vectors.json>
    const BLAKE3_ABC: &str = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85";

    #[test]
    fn blake3_known_vector_is_the_new_internal_baseline() {
        let mut h = Hashers::new(&ChecksumSet::none());
        h.update(b"a");
        h.update(b"bc");
        let finalized = h.finalize();
        assert!(finalized.checksums.is_empty());
        assert_eq!(finalized.internal_blake3.as_deref(), Some(BLAKE3_ABC));
        // SHA-256 is not computed unless requested: the baseline is BLAKE3 only.
        assert_eq!(finalized.internal_sha256, None);
    }

    #[test]
    fn chunked_and_one_shot_blake3_agree() {
        let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
        let mut one = Hashers::new(&ChecksumSet::none());
        one.update(&data);
        let one = one.finalize();
        let mut chunked = Hashers::new(&ChecksumSet::none());
        for chunk in data.chunks(4097) {
            chunked.update(chunk);
        }
        let chunked = chunked.finalize();
        assert_eq!(chunked.internal_blake3, one.internal_blake3);
    }

    #[test]
    fn supplementary_checksums_present_when_requested() {
        let set = ChecksumSet(vec![ChecksumAlgorithm::Sha256, ChecksumAlgorithm::Crc32]);
        let mut h = Hashers::new(&set);
        h.update(b"abc");
        let finalized = h.finalize();
        assert_eq!(
            finalized.internal_sha256.as_deref(),
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
        assert_eq!(finalized.internal_blake3.as_deref(), Some(BLAKE3_ABC));
        assert_eq!(
            finalized.md5_hex.as_deref(),
            Some("900150983cd24fb0d6963f7d28e17f72")
        );
        assert_eq!(finalized.checksums.len(), 2);
        // SHA-256("abc") base64
        let sha = finalized
            .checksums
            .iter()
            .find(|c| c.algorithm == ChecksumAlgorithm::Sha256)
            .unwrap();
        assert_eq!(sha.value, "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=");
    }

    #[test]
    fn part_hasher_skips_the_unused_internal_baselines() {
        let mut h = Hashers::new_for_part(&ChecksumSet::none());
        h.update(b"abc");
        let finalized = h.finalize();
        assert_eq!(
            finalized.md5_hex.as_deref(),
            Some("900150983cd24fb0d6963f7d28e17f72")
        );
        assert!(finalized.checksums.is_empty());
        assert_eq!(finalized.internal_blake3, None);
        assert_eq!(finalized.internal_sha256, None);
    }

    #[test]
    fn assembly_hasher_can_skip_the_unused_md5() {
        let set = ChecksumSet(vec![ChecksumAlgorithm::Crc32]);
        let mut h = Hashers::new_for_assembly(&set, false);
        h.update(b"abc");
        let finalized = h.finalize();
        assert_eq!(finalized.md5_hex, None);
        assert_eq!(finalized.checksums.len(), 1);
        assert_eq!(finalized.internal_blake3.as_deref(), Some(BLAKE3_ABC));
    }

    #[test]
    fn part_hasher_keeps_a_requested_sha256() {
        let set = ChecksumSet(vec![ChecksumAlgorithm::Sha256]);
        let mut h = Hashers::new_for_part(&set);
        h.update(b"abc");
        let finalized = h.finalize();
        assert_eq!(
            finalized.internal_sha256.as_deref(),
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
        assert_eq!(finalized.checksums.len(), 1);
        assert_eq!(finalized.internal_blake3, None);
    }

    #[test]
    fn scrub_hasher_computes_both_algorithms_when_a_legacy_row_needs_sha256() {
        let mut h = Hashers::new_for_scrub(&ChecksumSet::none(), true);
        h.update(b"abc");
        let finalized = h.finalize();
        assert_eq!(finalized.internal_blake3.as_deref(), Some(BLAKE3_ABC));
        assert_eq!(
            finalized.internal_sha256.as_deref(),
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
    }
}
