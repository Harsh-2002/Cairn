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
        let bs = reader.block_size().max(1);
        // Read block-by-block exactly as the production read path does — it only ever calls
        // `read_range` with a length within one block (`hi - lo <= block_size`), never the whole
        // object in one allocation. We mirror that so the fuzzer exercises the real usage pattern;
        // each call must decode or return a typed error, never panic. The loop is bounded so a
        // many-block trailer cannot make a single fuzz unit run unbounded.
        let mut off = 0u64;
        let mut scanned = 0u64;
        while off < ll && scanned < 8192 {
            let len = bs.min(ll - off);
            let _ = reader.read_range(off, len);
            off = off.saturating_add(len.max(1));
            scanned += 1;
        }
        // A few boundary reads (each within a block): just past the end, the last byte, the start.
        let _ = reader.read_range(0, 1);
        if ll > 0 {
            let _ = reader.read_range(ll - 1, 4);
            let _ = reader.read_range(ll, 1);
        }
    }
});
