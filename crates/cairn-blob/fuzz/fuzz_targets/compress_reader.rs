#![no_main]
//! Fuzz the at-rest block-compression reader (ARCH 10, 29.3). `CompressedReader` parses an
//! attacker-influenceable (bit-rotted or otherwise corrupt) CRNB trailer + index and then serves
//! ranged reads. It must NEVER panic and never allocate without bound on malformed input — a bad
//! `block_count`/`index_len`/`logical_len`, a truncated trailer or index, a lying physical length,
//! or an out-of-range read must all fold to a typed `BlobError`, not an unwind or OOM.
//!
//! The whole input is treated as the blob file, so a real CRNB blob (see `fuzz/corpus/`) is a valid
//! seed the fuzzer mutates from. We open it both without and with a DEK (exercising the encrypted
//! per-block decrypt path) and drive `read_range` across in-range and just-past-the-end bounds.

use cairn_blob::compress::CompressedReader;
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    for dek in [None, Some([7u8; 32])] {
        let Ok(mut reader) = CompressedReader::open_with_dek(Cursor::new(data), dek) else {
            continue;
        };
        let ll = reader.logical_len();
        // Full read, a boundary-crossing read, a mid read, and a just-past-the-end read — each must
        // either decode or return a typed error, never panic. Bounds stay within a sane range so we
        // test the reader's own arithmetic, not a contrived caller-impossible u64 overflow.
        let _ = reader.read_range(0, ll);
        if ll > 0 {
            let _ = reader.read_range(ll - 1, 4);
            let _ = reader.read_range(ll / 2, ll / 2 + 1);
            let _ = reader.read_range(ll, 1);
        }
        let _ = reader.read_range(0, 1);
    }
});
