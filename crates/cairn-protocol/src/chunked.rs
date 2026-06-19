//! The SigV4 streaming `aws-chunked` decoder (ARCH 21.7) — the single highest-risk ingest
//! component (F-5: getting it wrong corrupts objects silently). It is a streaming state machine
//! over the raw body that emits ONLY de-framed payload bytes, never the framing. It keeps a
//! bounded parse buffer so it is correct when the transport splits a header or payload across
//! reads, enforces a payload size ceiling, and — for signed streaming — verifies each chunk's
//! signature against the rolling chain seeded by the request's seed signature, failing the
//! stream immediately on a mismatch rather than storing a tampered body.
//!
//! Chunk framing: `<hex-size>[;chunk-signature=<hex>]\r\n<payload>\r\n` repeated, ended by a
//! zero-size chunk and an optional trailer section terminated by a blank line.

use bytes::Bytes;
use sha2::{Digest, Sha256};

/// Maximum length of a single chunk header line (defense against a non-terminating header).
const MAX_HEADER_LINE: usize = 16 * 1024;

/// A streaming chunk-signature verifier context (signed streaming only).
#[derive(Clone)]
pub struct ChunkVerifier {
    /// The derived SigV4 signing key.
    pub key: [u8; 32],
    /// The request timestamp (`amz-date`).
    pub amzdate: String,
    /// The credential scope (`date/region/s3/aws4_request`).
    pub scope: String,
    /// The previous signature in the chain (seeded with the request's seed signature).
    pub prev_signature: String,
}

impl std::fmt::Debug for ChunkVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the signing key and rolling signature.
        f.debug_struct("ChunkVerifier")
            .field("amzdate", &self.amzdate)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// Errors the decoder can raise. None panic; malformed input is always a typed error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// A chunk header line was malformed.
    #[error("malformed chunk header")]
    BadHeader,
    /// The chunk size was not valid hexadecimal.
    #[error("invalid chunk size")]
    BadHex,
    /// A header line exceeded the bound without terminating.
    #[error("chunk header too long")]
    HeaderTooLong,
    /// A CRLF separator was missing where required.
    #[error("missing CRLF separator")]
    MissingCrlf,
    /// The total payload exceeded the configured ceiling.
    #[error("payload exceeds size ceiling")]
    SizeExceeded,
    /// A signed chunk's signature did not verify.
    #[error("chunk signature mismatch")]
    SignatureMismatch,
    /// The stream ended before the terminating zero-size chunk.
    #[error("incomplete chunk stream")]
    Incomplete,
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Header,
    Data,
    DataCr,
    DataLf,
    Trailer,
    Done,
}

/// The streaming `aws-chunked` decoder.
pub struct ChunkDecoder {
    state: State,
    header: Vec<u8>,
    remaining: u64,
    emitted: u64,
    max_payload: u64,
    verifier: Option<ChunkVerifier>,
    chunk_signature: Option<String>,
    chunk_hash: Sha256,
    trailer: Vec<u8>,
}

impl std::fmt::Debug for ChunkDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkDecoder")
            .field("state", &self.state)
            .field("emitted", &self.emitted)
            .field("signed", &self.verifier.is_some())
            .finish_non_exhaustive()
    }
}

impl ChunkDecoder {
    /// A decoder for unsigned streaming (framing only, no signature verification).
    #[must_use]
    pub fn unsigned(max_payload: u64) -> Self {
        Self::new(max_payload, None)
    }

    /// A decoder for signed streaming, verifying the rolling chunk-signature chain.
    #[must_use]
    pub fn signed(max_payload: u64, verifier: ChunkVerifier) -> Self {
        Self::new(max_payload, Some(verifier))
    }

    fn new(max_payload: u64, verifier: Option<ChunkVerifier>) -> Self {
        Self {
            state: State::Header,
            header: Vec::new(),
            remaining: 0,
            emitted: 0,
            max_payload,
            verifier,
            chunk_signature: None,
            chunk_hash: Sha256::new(),
            trailer: Vec::new(),
        }
    }

    /// Total payload bytes emitted so far.
    #[must_use]
    pub fn emitted(&self) -> u64 {
        self.emitted
    }

    /// Whether the stream terminated cleanly.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    /// Feed raw bytes; append any de-framed payload to `out`. Safe to call with arbitrary
    /// slices, including ones that split a header or payload across calls.
    pub fn push(&mut self, mut input: &[u8], out: &mut Vec<Bytes>) -> Result<(), DecodeError> {
        while !input.is_empty() {
            match self.state {
                State::Header => {
                    let (line_end, consumed) = find_lf(input);
                    if self.header.len() + consumed > MAX_HEADER_LINE {
                        return Err(DecodeError::HeaderTooLong);
                    }
                    self.header.extend_from_slice(&input[..consumed]);
                    input = &input[consumed..];
                    if line_end {
                        self.parse_header()?;
                        // size==0 chunk: verify (empty payload) and move to the trailer.
                        if self.remaining == 0 {
                            self.finish_chunk()?;
                            self.state = State::Trailer;
                        } else {
                            self.state = State::Data;
                        }
                    }
                }
                State::Data => {
                    let take = (self.remaining as usize).min(input.len());
                    let (data, rest) = input.split_at(take);
                    self.emitted += take as u64;
                    if self.emitted > self.max_payload {
                        return Err(DecodeError::SizeExceeded);
                    }
                    if self.verifier.is_some() {
                        self.chunk_hash.update(data);
                    }
                    out.push(Bytes::copy_from_slice(data));
                    self.remaining -= take as u64;
                    input = rest;
                    if self.remaining == 0 {
                        self.state = State::DataCr;
                    }
                }
                State::DataCr => {
                    if input[0] != b'\r' {
                        return Err(DecodeError::MissingCrlf);
                    }
                    input = &input[1..];
                    self.state = State::DataLf;
                }
                State::DataLf => {
                    if input[0] != b'\n' {
                        return Err(DecodeError::MissingCrlf);
                    }
                    input = &input[1..];
                    self.finish_chunk()?;
                    self.state = State::Header;
                }
                State::Trailer => {
                    let (line_end, consumed) = find_lf(input);
                    if self.trailer.len() + consumed > MAX_HEADER_LINE {
                        return Err(DecodeError::HeaderTooLong);
                    }
                    self.trailer.extend_from_slice(&input[..consumed]);
                    input = &input[consumed..];
                    if line_end {
                        let line = strip_cr(&self.trailer);
                        let empty = line.is_empty();
                        self.trailer.clear();
                        if empty {
                            self.state = State::Done;
                        }
                    }
                }
                State::Done => {
                    // Trailing bytes after the terminating chunk are ignored.
                    input = &[];
                }
            }
        }
        Ok(())
    }

    /// Assert the stream terminated at a chunk boundary (called when the body ends).
    pub fn finish(&self) -> Result<(), DecodeError> {
        match self.state {
            State::Done => Ok(()),
            // A producer that omits the final trailer blank line but sent the zero chunk has
            // reached the trailer state with nothing pending; accept that as terminated.
            State::Trailer if self.trailer.is_empty() => Ok(()),
            _ => Err(DecodeError::Incomplete),
        }
    }

    fn parse_header(&mut self) -> Result<(), DecodeError> {
        let line = strip_cr(&self.header);
        // size is the hex prefix up to the first ';' (extensions) or end of line.
        let size_end = line.iter().position(|&b| b == b';').unwrap_or(line.len());
        let size_str =
            std::str::from_utf8(&line[..size_end]).map_err(|_| DecodeError::BadHeader)?;
        let size = u64::from_str_radix(size_str.trim(), 16).map_err(|_| DecodeError::BadHex)?;

        // Extract chunk-signature extension if present (signed streaming).
        self.chunk_signature = None;
        if self.verifier.is_some() {
            let rest = &line[size_end..];
            let sig = extract_extension(rest, b"chunk-signature=");
            self.chunk_signature = Some(sig.ok_or(DecodeError::BadHeader)?);
        }
        self.remaining = size;
        self.chunk_hash = Sha256::new();
        self.header.clear();
        Ok(())
    }

    fn finish_chunk(&mut self) -> Result<(), DecodeError> {
        if let Some(v) = &mut self.verifier {
            let hash_hex = hex::encode(std::mem::take(&mut self.chunk_hash).finalize());
            let sts = cairn_auth::chunk_string_to_sign(
                &v.amzdate,
                &v.scope,
                &v.prev_signature,
                &hash_hex,
            );
            let expected = cairn_auth::compute_signature(&v.key, &sts);
            let declared = self.chunk_signature.take().ok_or(DecodeError::BadHeader)?;
            if !ct_eq(expected.as_bytes(), declared.as_bytes()) {
                return Err(DecodeError::SignatureMismatch);
            }
            v.prev_signature = expected;
        }
        Ok(())
    }
}

/// Scan `input` for a `\n`; return (found, bytes_consumed_including_lf_if_found).
fn find_lf(input: &[u8]) -> (bool, usize) {
    match input.iter().position(|&b| b == b'\n') {
        Some(i) => (true, i + 1),
        None => (false, input.len()),
    }
}

fn strip_cr(line: &[u8]) -> &[u8] {
    let end = line.len();
    let end = if end > 0 && line[end - 1] == b'\n' {
        end - 1
    } else {
        end
    };
    let end = if end > 0 && line[end - 1] == b'\r' {
        end - 1
    } else {
        end
    };
    &line[..end]
}

fn extract_extension(rest: &[u8], name: &[u8]) -> Option<String> {
    // rest looks like ";chunk-signature=<hex>" possibly with more ';'-separated extensions.
    for part in rest.split(|&b| b == b';') {
        if let Some(val) = part.strip_prefix(name) {
            return std::str::from_utf8(val).ok().map(str::to_owned);
        }
    }
    None
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

/// The marker prefix the body stream carries when a signed chunk fails its signature check, so
/// the ingest path can map the failure to an authentication error (`SignatureDoesNotMatch`)
/// rather than a generic internal error. See [`is_signature_failure`].
pub const CHUNK_SIGNATURE_FAILURE_MARKER: &str = "aws-chunked: chunk signature mismatch";

/// Whether a propagated body-error message denotes a signed-streaming chunk-signature failure.
#[must_use]
pub fn is_signature_failure(message: &str) -> bool {
    message.contains(CHUNK_SIGNATURE_FAILURE_MARKER)
}

/// Wrap a raw request body stream in `decoder`, yielding a stream of de-framed payload bytes.
/// A decode error terminates the stream with a [`BodyError`]; the downstream blob stager treats
/// that as a failed upload and reclaims the staging artifact. A signed-chunk signature mismatch
/// is tagged with [`CHUNK_SIGNATURE_FAILURE_MARKER`] so the ingest path can surface it as an
/// authentication failure.
///
/// [`BodyError`]: cairn_types::error::BodyError
pub fn decode_stream(
    body: cairn_types::BodyStream,
    decoder: ChunkDecoder,
) -> cairn_types::BodyStream {
    use cairn_types::error::BodyError;
    use futures_util::StreamExt;
    use std::collections::VecDeque;

    struct St {
        body: cairn_types::BodyStream,
        decoder: ChunkDecoder,
        pending: VecDeque<Bytes>,
        done: bool,
    }

    let st = St {
        body,
        decoder,
        pending: VecDeque::new(),
        done: false,
    };
    Box::pin(futures_util::stream::unfold(st, |mut st| async move {
        loop {
            if let Some(b) = st.pending.pop_front() {
                return Some((Ok(b), st));
            }
            if st.done {
                return None;
            }
            match st.body.next().await {
                Some(Ok(chunk)) => {
                    let mut out = Vec::new();
                    if let Err(e) = st.decoder.push(&chunk, &mut out) {
                        st.done = true;
                        let msg = if e == DecodeError::SignatureMismatch {
                            CHUNK_SIGNATURE_FAILURE_MARKER.to_owned()
                        } else {
                            e.to_string()
                        };
                        return Some((Err(BodyError::Transport(msg)), st));
                    }
                    st.pending.extend(out);
                }
                Some(Err(e)) => {
                    st.done = true;
                    return Some((Err(e), st));
                }
                None => {
                    st.done = true;
                    if st.decoder.finish().is_err() {
                        return Some((Err(BodyError::Truncated), st));
                    }
                    // Any payload buffered before the terminator was already drained above.
                }
            }
        }
    }))
}

/// Decode an entire signed/unsigned chunked body in memory (test/helper convenience).
#[cfg(test)]
pub fn decode_all(decoder: &mut ChunkDecoder, input: &[u8]) -> Result<Vec<u8>, DecodeError> {
    let mut out = Vec::new();
    decoder.push(input, &mut out)?;
    decoder.finish()?;
    Ok(cat(&out))
}

#[cfg(test)]
fn cat(parts: &[Bytes]) -> Vec<u8> {
    let mut buf = Vec::new();
    for p in parts {
        buf.extend_from_slice(p);
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unsigned_body(chunks: &[&[u8]]) -> Vec<u8> {
        let mut body = Vec::new();
        for c in chunks {
            body.extend_from_slice(format!("{:x}\r\n", c.len()).as_bytes());
            body.extend_from_slice(c);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(b"0\r\n\r\n");
        body
    }

    #[test]
    fn decodes_single_chunk() {
        let body = unsigned_body(&[b"hello world"]);
        let mut d = ChunkDecoder::unsigned(1 << 20);
        assert_eq!(decode_all(&mut d, &body).unwrap(), b"hello world");
    }

    #[test]
    fn decodes_multiple_chunks_concatenated() {
        let body = unsigned_body(&[b"part-one-", b"part-two"]);
        let mut d = ChunkDecoder::unsigned(1 << 20);
        assert_eq!(decode_all(&mut d, &body).unwrap(), b"part-one-part-two");
    }

    #[test]
    fn split_at_every_boundary_yields_same_payload() {
        let body = unsigned_body(&[b"abcdefghij", b"klmno"]);
        // Feed the body one byte at a time: split headers, payload, and CRLFs arbitrarily.
        for split in 1..body.len() {
            let mut d = ChunkDecoder::unsigned(1 << 20);
            let mut out = Vec::new();
            d.push(&body[..split], &mut out).unwrap();
            d.push(&body[split..], &mut out).unwrap();
            d.finish().unwrap();
            assert_eq!(cat(&out), b"abcdefghijklmno", "split at {split}");
        }
        // And one byte at a time.
        let mut d = ChunkDecoder::unsigned(1 << 20);
        let mut out = Vec::new();
        for b in &body {
            d.push(std::slice::from_ref(b), &mut out).unwrap();
        }
        d.finish().unwrap();
        assert_eq!(cat(&out), b"abcdefghijklmno");
    }

    #[test]
    fn enforces_size_ceiling() {
        let body = unsigned_body(&[b"0123456789"]);
        let mut d = ChunkDecoder::unsigned(5);
        let mut out = Vec::new();
        assert_eq!(d.push(&body, &mut out), Err(DecodeError::SizeExceeded));
    }

    #[test]
    fn rejects_missing_crlf_and_bad_hex() {
        let mut d = ChunkDecoder::unsigned(1 << 20);
        let mut out = Vec::new();
        // bad hex size
        assert_eq!(d.push(b"zz\r\n", &mut out), Err(DecodeError::BadHex));

        let mut d = ChunkDecoder::unsigned(1 << 20);
        let mut out = Vec::new();
        // size says 3 but data is followed by garbage instead of CRLF
        assert_eq!(
            d.push(b"3\r\nabcXX", &mut out),
            Err(DecodeError::MissingCrlf)
        );
    }

    #[test]
    fn incomplete_stream_is_an_error() {
        let mut d = ChunkDecoder::unsigned(1 << 20);
        let mut out = Vec::new();
        d.push(b"5\r\nab", &mut out).unwrap(); // mid-chunk
        assert_eq!(d.finish(), Err(DecodeError::Incomplete));
    }

    #[test]
    fn signed_chunk_chain_verifies_and_detects_tampering() {
        // Build a signed body the way a client would, using the same primitives the decoder
        // verifies against, then confirm a good body decodes and a tampered one is rejected.
        let key = cairn_auth::streaming_signing_key("secret", "20260101", "us-east-1");
        let scope = "20260101/us-east-1/s3/aws4_request".to_owned();
        let amzdate = "20260101T000000Z".to_owned();
        let seed = "0000000000000000000000000000000000000000000000000000000000000000".to_owned();

        let payloads: [&[u8]; 2] = [b"the quick brown fox", b""];
        let mut prev = seed.clone();
        let mut body = Vec::new();
        for p in payloads {
            let hash = hex::encode(Sha256::digest(p));
            let sts = cairn_auth::chunk_string_to_sign(&amzdate, &scope, &prev, &hash);
            let sig = cairn_auth::compute_signature(&key, &sts);
            body.extend_from_slice(format!("{:x};chunk-signature={}\r\n", p.len(), sig).as_bytes());
            body.extend_from_slice(p);
            body.extend_from_slice(b"\r\n");
            prev = sig;
        }
        body.extend_from_slice(b"\r\n"); // trailer terminator

        let verifier = ChunkVerifier {
            key,
            amzdate: amzdate.clone(),
            scope: scope.clone(),
            prev_signature: seed.clone(),
        };
        let mut d = ChunkDecoder::signed(1 << 20, verifier.clone());
        assert_eq!(decode_all(&mut d, &body).unwrap(), b"the quick brown fox");

        // Flip one payload byte: the chunk signature no longer matches.
        let mut tampered = body.clone();
        let pos = tampered.windows(3).position(|w| w == b"fox").unwrap();
        tampered[pos] = b'F';
        let mut d = ChunkDecoder::signed(1 << 20, verifier);
        let mut out = Vec::new();
        assert_eq!(
            d.push(&tampered, &mut out),
            Err(DecodeError::SignatureMismatch)
        );
    }
}

#[cfg(test)]
mod fuzz_props {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // The decoder must NEVER panic on arbitrary bytes delivered with arbitrary read
        // boundaries, and must never emit more payload than the input length nor exceed the
        // ceiling — the F-5 robustness guarantee.
        #![proptest_config(ProptestConfig::with_cases(2048))]
        #[test]
        fn never_panics_on_arbitrary_input(data: Vec<u8>, piece_sizes in prop::collection::vec(1usize..=17, 0..64)) {
            let ceiling = 1u64 << 16;
            let mut d = ChunkDecoder::unsigned(ceiling);
            let mut out = Vec::new();
            let mut offset = 0;
            let mut errored = false;
            let mut sizes = piece_sizes.into_iter().cycle();
            while offset < data.len() && !errored {
                let take = sizes.next().unwrap_or(1).min(data.len() - offset);
                if d.push(&data[offset..offset + take], &mut out).is_err() {
                    errored = true;
                }
                offset += take;
            }
            if !errored {
                let _ = d.finish();
            }
            let emitted: usize = out.iter().map(bytes::Bytes::len).sum();
            prop_assert!(emitted as u64 <= ceiling);
            prop_assert!(emitted <= data.len());
        }
    }
}
