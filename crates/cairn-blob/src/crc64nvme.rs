//! CRC-64/NVME — the default flexible checksum of the AWS CLI v2 and CRT-based SDKs (ARCH 21.1).
//!
//! Parameters (Rocksoft, per the NVMe spec): width 64, normal poly `0xAD93D23594C93659`,
//! init `0xFFFF_FFFF_FFFF_FFFF`, reflected in/out, final XOR `0xFFFF_FFFF_FFFF_FFFF`. We compute it
//! the reflected way (LSB-first), so the table is built from the bit-reversed polynomial
//! `0x9A6C_9329_AC4B_C9B5`. Dependency-free on purpose: a single hand-verified table (checked against
//! the standard `0xAE8B_1486_0A79_9888` vector for `"123456789"`) is far less risk than hand-rolling
//! a CRC, and avoids pulling a new crate into the static build.

/// The bit-reversed CRC-64/NVME polynomial (reflected algorithm).
const POLY_REFLECTED: u64 = 0x9A6C_9329_AC4B_C9B5;

/// The 256-entry lookup table, generated at compile time from [`POLY_REFLECTED`].
const TABLE: [u64; 256] = {
    let mut table = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u64;
        let mut bit = 0;
        while bit < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY_REFLECTED;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// A streaming CRC-64/NVME accumulator. The running state holds the *un-finalized* CRC (the final
/// XOR is applied only by [`Crc64Nvme::finalize`]), so it composes correctly across many `update`s.
#[derive(Clone)]
pub struct Crc64Nvme {
    crc: u64,
}

impl Default for Crc64Nvme {
    fn default() -> Self {
        Self { crc: u64::MAX }
    }
}

impl Crc64Nvme {
    /// A fresh accumulator seeded with the init value.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed bytes.
    pub fn update(&mut self, data: &[u8]) {
        let mut crc = self.crc;
        for &b in data {
            let idx = ((crc ^ u64::from(b)) & 0xff) as usize;
            crc = TABLE[idx] ^ (crc >> 8);
        }
        self.crc = crc;
    }

    /// Finalize, returning the 8-byte big-endian digest (as S3 transmits it before base64).
    #[must_use]
    pub fn finalize(self) -> [u8; 8] {
        (self.crc ^ u64::MAX).to_be_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical CRC-64/NVME check value for the ASCII string "123456789".
    #[test]
    fn matches_the_standard_check_vector() {
        let mut c = Crc64Nvme::new();
        c.update(b"123456789");
        assert_eq!(u64::from_be_bytes(c.finalize()), 0xAE8B_1486_0A79_9888);
    }

    #[test]
    fn streaming_in_pieces_matches_one_shot() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let mut one = Crc64Nvme::new();
        one.update(data);
        let mut split = Crc64Nvme::new();
        for chunk in data.chunks(7) {
            split.update(chunk);
        }
        assert_eq!(one.finalize(), split.finalize());
    }

    #[test]
    fn empty_input_is_zero() {
        // CRC-64/NVME of the empty string is 0 (init ^ xorout cancel through the table).
        assert_eq!(u64::from_be_bytes(Crc64Nvme::new().finalize()), 0);
    }
}
