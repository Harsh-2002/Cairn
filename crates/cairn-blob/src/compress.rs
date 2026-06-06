//! The self-describing, block-based compressed blob format (ARCH §9.3, §10.3). An object is a
//! sequence of independently (de)compressible fixed-size logical blocks, followed by an index
//! and a fixed trailer, so a ranged read decompresses only the blocks overlapping the range.
//! Each block is stored compressed only if it actually shrinks, so incompressible data never
//! grows (the per-block incompressibility fallback).
//!
//! Layout: `[block 0 phys][block 1 phys]...[block N-1 phys][index][trailer]`
//! Index entry (9 bytes LE): `phys_len: u32`, `logical_len: u32`, `compressed: u8`.
//! Trailer (34 bytes): magic(4) `CRNB`, version(1), algo(1), block_size(4), logical_len(8),
//! block_count(4), index_offset(8), index_len(4).

use cairn_types::bucket::CompressionAlgorithm;
use cairn_types::error::BlobError;
use std::io::{Read, Seek, SeekFrom};

const MAGIC: &[u8; 4] = b"CRNB";
const VERSION: u8 = 1;
const TRAILER_LEN: u64 = 34;
const INDEX_ENTRY_LEN: usize = 9;

fn algo_code(a: CompressionAlgorithm) -> u8 {
    match a {
        CompressionAlgorithm::None => 0,
        CompressionAlgorithm::Zstd => 1,
        CompressionAlgorithm::Lz4 => 2,
    }
}
fn algo_from(code: u8) -> CompressionAlgorithm {
    match code {
        1 => CompressionAlgorithm::Zstd,
        2 => CompressionAlgorithm::Lz4,
        _ => CompressionAlgorithm::None,
    }
}

/// Content types whose data is already compressed; storing them uncompressed avoids wasting
/// CPU for no gain (the whole-object heuristic).
#[must_use]
pub fn is_precompressed(content_type: &str) -> bool {
    let ct = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    matches!(
        ct.as_str(),
        "application/zip"
            | "application/gzip"
            | "application/x-gzip"
            | "application/x-7z-compressed"
            | "application/x-rar-compressed"
            | "application/x-bzip2"
            | "application/x-xz"
            | "application/zstd"
    ) || ct.starts_with("image/")
        || ct.starts_with("video/")
        || ct.starts_with("audio/")
}

struct IndexEntry {
    phys_len: u32,
    logical_len: u32,
    compressed: bool,
}

fn compress_block(algo: CompressionAlgorithm, logical: &[u8]) -> (Vec<u8>, bool) {
    let compressed = match algo {
        CompressionAlgorithm::Zstd => zstd::bulk::compress(logical, 3).ok(),
        CompressionAlgorithm::Lz4 => Some(lz4_flex::compress(logical)),
        CompressionAlgorithm::None => None,
    };
    match compressed {
        // Keep the compressed form only if it actually shrinks (per-block fallback).
        Some(c) if c.len() < logical.len() => (c, true),
        _ => (logical.to_vec(), false),
    }
}

fn decompress_block(
    algo: CompressionAlgorithm,
    phys: &[u8],
    logical_len: usize,
    compressed: bool,
) -> Result<Vec<u8>, BlobError> {
    if !compressed {
        return Ok(phys.to_vec());
    }
    match algo {
        CompressionAlgorithm::Zstd => zstd::bulk::decompress(phys, logical_len)
            .map_err(|e| BlobError::Corruption(format!("zstd: {e}"))),
        CompressionAlgorithm::Lz4 => lz4_flex::decompress(phys, logical_len)
            .map_err(|e| BlobError::Corruption(format!("lz4: {e}"))),
        CompressionAlgorithm::None => {
            Err(BlobError::Corruption("raw block flagged compressed".into()))
        }
    }
}

/// Streaming block encoder. Feed logical bytes; it emits physical bytes for completed blocks
/// and, on finish, the last block plus the index and trailer. Bounded memory: at most one
/// block plus its compressed form is buffered.
pub struct BlockEncoder {
    algo: CompressionAlgorithm,
    block_size: usize,
    buf: Vec<u8>,
    index: Vec<IndexEntry>,
    logical_len: u64,
    phys_len: u64,
}

impl BlockEncoder {
    /// A new encoder for the given algorithm and logical block size.
    #[must_use]
    pub fn new(algo: CompressionAlgorithm, block_size: u32) -> Self {
        Self {
            algo,
            block_size: block_size.max(1) as usize,
            buf: Vec::new(),
            index: Vec::new(),
            logical_len: 0,
            phys_len: 0,
        }
    }

    /// Feed plaintext; returns physical bytes to append for any blocks completed.
    pub fn feed(&mut self, data: &[u8]) -> Vec<u8> {
        self.logical_len += data.len() as u64;
        self.buf.extend_from_slice(data);
        let mut out = Vec::new();
        while self.buf.len() >= self.block_size {
            let block: Vec<u8> = self.buf.drain(..self.block_size).collect();
            self.emit_block(&block, &mut out);
        }
        out
    }

    fn emit_block(&mut self, logical: &[u8], out: &mut Vec<u8>) {
        let (phys, compressed) = compress_block(self.algo, logical);
        self.index.push(IndexEntry {
            phys_len: phys.len() as u32,
            logical_len: logical.len() as u32,
            compressed,
        });
        self.phys_len += phys.len() as u64;
        out.extend_from_slice(&phys);
    }

    /// Flush the final partial block and append the index and trailer; returns those bytes.
    pub fn finish(mut self) -> Vec<u8> {
        let mut out = Vec::new();
        if !self.buf.is_empty() {
            let block = std::mem::take(&mut self.buf);
            self.emit_block(&block, &mut out);
        }
        let index_offset = self.phys_len;
        let mut index_bytes = Vec::with_capacity(self.index.len() * INDEX_ENTRY_LEN);
        for e in &self.index {
            index_bytes.extend_from_slice(&e.phys_len.to_le_bytes());
            index_bytes.extend_from_slice(&e.logical_len.to_le_bytes());
            index_bytes.push(u8::from(e.compressed));
        }
        out.extend_from_slice(&index_bytes);

        let mut trailer = Vec::with_capacity(TRAILER_LEN as usize);
        trailer.extend_from_slice(MAGIC);
        trailer.push(VERSION);
        trailer.push(algo_code(self.algo));
        trailer.extend_from_slice(&(self.block_size as u32).to_le_bytes());
        trailer.extend_from_slice(&self.logical_len.to_le_bytes());
        trailer.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        trailer.extend_from_slice(&index_offset.to_le_bytes());
        trailer.extend_from_slice(&(index_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&trailer);
        out
    }
}

/// A random-access reader over a compressed blob file.
pub struct CompressedReader<R: Read + Seek> {
    inner: R,
    algo: CompressionAlgorithm,
    block_size: u64,
    logical_len: u64,
    block_offsets: Vec<u64>,
    index: Vec<IndexEntry>,
}

impl<R: Read + Seek> CompressedReader<R> {
    /// Read the trailer and index from a compressed blob.
    pub fn open(mut inner: R) -> Result<Self, BlobError> {
        let io = |e: std::io::Error| BlobError::Io(e.to_string());
        let total = inner.seek(SeekFrom::End(0)).map_err(io)?;
        if total < TRAILER_LEN {
            return Err(BlobError::Corruption("file shorter than trailer".into()));
        }
        inner
            .seek(SeekFrom::End(-(TRAILER_LEN as i64)))
            .map_err(io)?;
        let mut t = [0u8; TRAILER_LEN as usize];
        inner.read_exact(&mut t).map_err(io)?;
        if &t[0..4] != MAGIC {
            return Err(BlobError::Corruption("bad magic".into()));
        }
        let algo = algo_from(t[5]);
        let block_size = u32::from_le_bytes(t[6..10].try_into().unwrap()) as u64;
        let logical_len = u64::from_le_bytes(t[10..18].try_into().unwrap());
        let block_count = u32::from_le_bytes(t[18..22].try_into().unwrap()) as usize;
        let index_offset = u64::from_le_bytes(t[22..30].try_into().unwrap());
        let index_len = u32::from_le_bytes(t[30..34].try_into().unwrap()) as usize;

        if index_len != block_count * INDEX_ENTRY_LEN {
            return Err(BlobError::Corruption("index length mismatch".into()));
        }
        inner.seek(SeekFrom::Start(index_offset)).map_err(io)?;
        let mut idx = vec![0u8; index_len];
        inner.read_exact(&mut idx).map_err(io)?;

        let mut index = Vec::with_capacity(block_count);
        let mut block_offsets = Vec::with_capacity(block_count);
        let mut offset = 0u64;
        for chunk in idx.chunks_exact(INDEX_ENTRY_LEN) {
            let phys_len = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
            let logical = u32::from_le_bytes(chunk[4..8].try_into().unwrap());
            let compressed = chunk[8] != 0;
            block_offsets.push(offset);
            offset += u64::from(phys_len);
            index.push(IndexEntry {
                phys_len,
                logical_len: logical,
                compressed,
            });
        }
        Ok(Self {
            inner,
            algo,
            block_size,
            logical_len,
            block_offsets,
            index,
        })
    }

    /// The logical (plaintext) length of the object.
    #[must_use]
    pub fn logical_len(&self) -> u64 {
        self.logical_len
    }

    /// The logical block size.
    #[must_use]
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// Decompress and return the logical bytes for `[offset, offset+len)`, decompressing only
    /// the overlapping blocks.
    pub fn read_range(&mut self, offset: u64, len: u64) -> Result<Vec<u8>, BlobError> {
        let io = |e: std::io::Error| BlobError::Io(e.to_string());
        let end = offset.saturating_add(len).min(self.logical_len);
        if offset >= end || self.block_size == 0 {
            return Ok(Vec::new());
        }
        let first = (offset / self.block_size) as usize;
        let last = ((end - 1) / self.block_size) as usize;
        let mut out = Vec::with_capacity((end - offset) as usize);
        for b in first..=last {
            let entry = &self.index[b];
            self.inner
                .seek(SeekFrom::Start(self.block_offsets[b]))
                .map_err(io)?;
            let mut phys = vec![0u8; entry.phys_len as usize];
            self.inner.read_exact(&mut phys).map_err(io)?;
            let logical = decompress_block(
                self.algo,
                &phys,
                entry.logical_len as usize,
                entry.compressed,
            )?;
            let block_start = b as u64 * self.block_size;
            let from = offset.saturating_sub(block_start) as usize;
            let to = (end - block_start).min(logical.len() as u64) as usize;
            if from < to {
                out.extend_from_slice(&logical[from..to]);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn encode(algo: CompressionAlgorithm, block_size: u32, data: &[u8]) -> Vec<u8> {
        let mut enc = BlockEncoder::new(algo, block_size);
        let mut out = enc.feed(data);
        out.extend_from_slice(&enc.finish());
        out
    }

    #[test]
    fn roundtrip_full_and_ranges() {
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let blob = encode(CompressionAlgorithm::Zstd, 1024, &data);
        let mut r = CompressedReader::open(Cursor::new(blob)).unwrap();
        assert_eq!(r.logical_len(), 5000);
        // full read
        assert_eq!(r.read_range(0, 5000).unwrap(), data);
        // a range that starts mid-block near the end (the case block compression exists for)
        assert_eq!(r.read_range(4096, 500).unwrap(), &data[4096..4596]);
        // a range spanning a block boundary
        assert_eq!(r.read_range(1000, 100).unwrap(), &data[1000..1100]);
    }

    #[test]
    fn incompressible_data_does_not_grow_blocks() {
        // Pseudo-random, incompressible payload: each block falls back to raw storage when
        // compression would not shrink it, so the on-disk block bytes never exceed plaintext.
        let data: Vec<u8> = (0..4096u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
            .collect();
        let blob = encode(CompressionAlgorithm::Zstd, 1024, &data);
        // Only the small index + trailer overhead is added; the block payload never grows.
        let overhead = 4 * INDEX_ENTRY_LEN as u64 + TRAILER_LEN;
        assert!((blob.len() as u64) <= data.len() as u64 + overhead);
        let mut r = CompressedReader::open(Cursor::new(blob)).unwrap();
        assert_eq!(r.read_range(0, 4096).unwrap(), data);
        assert!(r.index.iter().all(|e| e.phys_len <= e.logical_len));
    }

    #[test]
    fn compressible_data_actually_shrinks() {
        let data = vec![b'a'; 10_000];
        let blob = encode(CompressionAlgorithm::Zstd, 1024, &data);
        assert!(
            (blob.len() as u64) < 10_000,
            "highly compressible data must shrink on disk"
        );
        let mut r = CompressedReader::open(Cursor::new(blob)).unwrap();
        assert_eq!(r.read_range(0, 10_000).unwrap(), data);
    }

    #[test]
    fn lz4_roundtrip() {
        let data = vec![b'x'; 3000];
        let blob = encode(CompressionAlgorithm::Lz4, 1024, &data);
        let mut r = CompressedReader::open(Cursor::new(blob)).unwrap();
        assert_eq!(r.read_range(0, 3000).unwrap(), data);
    }

    #[test]
    fn precompressed_detection() {
        assert!(is_precompressed("image/jpeg"));
        assert!(is_precompressed("video/mp4"));
        assert!(is_precompressed("application/zip"));
        assert!(!is_precompressed("text/plain"));
        assert!(!is_precompressed("application/json"));
    }
}
