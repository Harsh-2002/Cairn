//! CRC-64/NVME — the default flexible checksum of the AWS CLI v2 and CRT-based SDKs (ARCH 21.1).
//!
//! Parameters (Rocksoft, per the NVMe spec): width 64, normal poly `0xAD93D23594C93659`,
//! init `0xFFFF_FFFF_FFFF_FFFF`, reflected in/out, final XOR `0xFFFF_FFFF_FFFF_FFFF`. We compute it
//! the reflected way (LSB-first), so the tables are built from the bit-reversed polynomial
//! `0x9A6C_9329_AC4B_C9B5`. Dependency-free on purpose: eight hand-verified tables (checked against
//! the standard `0xAE8B_1486_0A79_9888` vector for `"123456789"`) are far less risk than
//! hand-rolling a CRC, and avoid pulling a new crate into the static build.
//!
//! The update loops over eight bytes at a time. The byte-at-a-time table walked one byte per
//! iteration at roughly 370 MiB/s on the reference host, which is significant next to the SHA-256
//! a completion already computes over the same plaintext (a requested CRC64NVME cost 135 ms of a
//! 50 MiB assembly). Slicing by eight consumes a 64-bit word per iteration and stays exact: the
//! same tables, the same polynomial, the same digest.

/// The bit-reversed CRC-64/NVME polynomial (reflected algorithm).
const POLY_REFLECTED: u64 = 0x9A6C_9329_AC4B_C9B5;

/// Eight 256-entry lookup tables for the slice-by-eight update, generated at compile time from
/// [`POLY_REFLECTED`]. `TABLES[0]` is the classic byte-at-a-time table; `TABLES[k][i]` is the state
/// contributed by byte `i` shifted through `k` further zero bytes.
const TABLES: [[u64; 256]; 8] = {
    let mut tables = [[0u64; 256]; 8];
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
        tables[0][i] = crc;
        i += 1;
    }
    let mut slice = 1;
    while slice < 8 {
        let mut i = 0;
        while i < 256 {
            let previous = tables[slice - 1][i];
            tables[slice][i] = (previous >> 8) ^ tables[0][(previous & 0xff) as usize];
            i += 1;
        }
        slice += 1;
    }
    tables
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

    /// Feed bytes. Eight at a time where the input allows; the same digest as the byte-at-a-time
    /// recurrence because the slice tables are derived from it.
    pub fn update(&mut self, data: &[u8]) {
        let mut crc = self.crc;
        let mut words = data.chunks_exact(8);
        for word in &mut words {
            crc ^= u64::from_le_bytes(word.try_into().expect("chunks_exact(8) yields eight bytes"));
            crc = TABLES[7][(crc & 0xff) as usize]
                ^ TABLES[6][((crc >> 8) & 0xff) as usize]
                ^ TABLES[5][((crc >> 16) & 0xff) as usize]
                ^ TABLES[4][((crc >> 24) & 0xff) as usize]
                ^ TABLES[3][((crc >> 32) & 0xff) as usize]
                ^ TABLES[2][((crc >> 40) & 0xff) as usize]
                ^ TABLES[1][((crc >> 48) & 0xff) as usize]
                ^ TABLES[0][(crc >> 56) as usize];
        }
        for &b in words.remainder() {
            let idx = ((crc ^ u64::from(b)) & 0xff) as usize;
            crc = TABLES[0][idx] ^ (crc >> 8);
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

    /// The slice-by-eight loop must equal the byte-at-a-time recurrence for every input length and
    /// every streaming split, including the sub-word remainder.
    #[test]
    fn slice_by_eight_matches_the_byte_recurrence() {
        fn reference(data: &[u8]) -> u64 {
            let mut crc = u64::MAX;
            for &b in data {
                crc = TABLES[0][((crc ^ u64::from(b)) & 0xff) as usize] ^ (crc >> 8);
            }
            crc ^ u64::MAX
        }
        let data: Vec<u8> = (0..1027u32).map(|i| (i * 31 % 251) as u8).collect();
        for len in 0..=data.len() {
            let one = {
                let mut c = Crc64Nvme::new();
                c.update(&data[..len]);
                u64::from_be_bytes(c.finalize())
            };
            assert_eq!(one, reference(&data[..len]), "one-shot length {len}");
            for split in [1usize, 3, 7, 8, 9, 15, 64] {
                if split > len {
                    continue;
                }
                let mut c = Crc64Nvme::new();
                for chunk in data[..len].chunks(split) {
                    c.update(chunk);
                }
                assert_eq!(
                    u64::from_be_bytes(c.finalize()),
                    one,
                    "length {len} split {split}"
                );
            }
        }
    }

    #[test]
    fn empty_input_is_zero() {
        // CRC-64/NVME of the empty string is 0 (init ^ xorout cancel through the table).
        assert_eq!(u64::from_be_bytes(Crc64Nvme::new().finalize()), 0);
    }
}
