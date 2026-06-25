//! Streaming content hashing: always the MD5 that becomes the ETag, plus any client-requested
//! checksum algorithms, computed once over the plaintext.

use crate::crc64nvme::Crc64Nvme;
use base64::Engine;
use cairn_types::object::{ChecksumAlgorithm, ChecksumSet, ChecksumValue};
use md5::{Digest, Md5};
use sha1::Sha1;
use sha2::Sha256;

/// Accumulates the MD5 and any requested supplementary checksums over a byte stream.
pub struct Hashers {
    md5: Md5,
    crc32: Option<crc32fast::Hasher>,
    crc32c: Option<u32>,
    crc64nvme: Option<Crc64Nvme>,
    sha1: Option<Sha1>,
    sha256: Option<Sha256>,
}

impl Hashers {
    /// Build hashers for the always-on MD5 plus the requested supplementary algorithms.
    #[must_use]
    pub fn new(set: &ChecksumSet) -> Self {
        let has = |a| set.0.contains(&a);
        Self {
            md5: Md5::new(),
            crc32: has(ChecksumAlgorithm::Crc32).then(crc32fast::Hasher::new),
            crc32c: has(ChecksumAlgorithm::Crc32c).then_some(0),
            crc64nvme: has(ChecksumAlgorithm::Crc64Nvme).then(Crc64Nvme::new),
            sha1: has(ChecksumAlgorithm::Sha1).then(Sha1::new),
            sha256: has(ChecksumAlgorithm::Sha256).then(Sha256::new),
        }
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
    /// supplementary checksums.
    #[must_use]
    pub fn finalize(self) -> (String, Vec<ChecksumValue>) {
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
        if let Some(h) = self.sha256 {
            checksums.push(ChecksumValue {
                algorithm: ChecksumAlgorithm::Sha256,
                value: b64.encode(h.finalize()),
            });
        }
        (md5_hex, checksums)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supplementary_checksums_present_when_requested() {
        let set = ChecksumSet(vec![ChecksumAlgorithm::Sha256, ChecksumAlgorithm::Crc32]);
        let mut h = Hashers::new(&set);
        h.update(b"abc");
        let (md5, checks) = h.finalize();
        assert_eq!(md5, "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(checks.len(), 2);
        // SHA-256("abc") base64
        let sha = checks
            .iter()
            .find(|c| c.algorithm == ChecksumAlgorithm::Sha256)
            .unwrap();
        assert_eq!(sha.value, "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=");
    }
}
