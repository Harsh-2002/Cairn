#![no_main]
//! The foremost fuzz target (ARCH §29.3): the SigV4 streaming chunked decoder fed arbitrary
//! bytes with arbitrary read-boundary splits. The decoder must never panic and never buffer
//! without bound; it either decodes or returns a typed error.

use arbitrary::Arbitrary;
use cairn_protocol::ChunkDecoder;
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
struct Input {
    /// The raw chunked body bytes.
    body: Vec<u8>,
    /// Read-boundary split sizes; the decoder must be agnostic to how the body is fragmented.
    splits: Vec<u16>,
}

fuzz_target!(|input: Input| {
    let mut decoder = ChunkDecoder::unsigned(1 << 20);
    let mut out = Vec::new();
    let mut offset = 0usize;
    let mut splits = input.splits.iter().copied().cycle();
    while offset < input.body.len() {
        let take = (splits.next().unwrap_or(1) as usize).max(1).min(input.body.len() - offset);
        if decoder.push(&input.body[offset..offset + take], &mut out).is_err() {
            return;
        }
        offset += take;
    }
    let _ = decoder.finish();
});
