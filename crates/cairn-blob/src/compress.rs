//! The self-describing, block-based compressed blob format (ARCH 9.3, 10.3). An object is a
//! sequence of independently (de)compressible fixed-size logical blocks, followed by an index
//! and a fixed trailer, so a ranged read decompresses only the blocks overlapping the range.
//! Each block is stored compressed only if it actually shrinks, so incompressible data never
//! grows (the per-block incompressibility fallback).
//!
//! Layout (current encrypted format):
//! `[block 0 phys][block 1 phys]...[block N-1 phys][index][metadata MAC][trailer]`
//! Index entry (9 bytes LE): `phys_len: u32`, `logical_len: u32`, `compressed: u8`.
//! Trailer (34 bytes): magic(4) `CRNB`, version(1), algo(1), block_size(4), logical_len(8),
//! block_count(4), index_offset(8), index_len(4).
//!
//! **SSE-S3 (ARCH 27).** When a data-encryption key (DEK) is supplied, the format version is
//! [`VERSION_ENCRYPTED`] and each block is encrypted with AES-256-GCM *after* compression
//! (compress-then-encrypt, since ciphertext is incompressible). The per-block 12-byte nonce is
//! derived deterministically from `(DEK, block_index)` as the first 12 bytes of
//! `HMAC-SHA256(DEK, block_index_le_u64)`, and the 16-byte GCM tag is appended to the block's
//! physical bytes, so `phys_len` covers ciphertext + tag. Range reads decrypt only the blocks
//! overlapping the range. Format v3 additionally appends a domain-separated HMAC-SHA256 over the
//! complete index and trailer, under the DEK, so compression flags, lengths, offsets, algorithm,
//! and version semantics are authenticated before the reader trusts them. Encrypted v2 blobs
//! (which have no metadata MAC) remain readable only when trusted metadata explicitly selects the
//! legacy reader, with strict structural and post-decompression checks. The on-disk version byte
//! cannot select its own parser. Unencrypted blobs keep [`VERSION_PLAIN`] and are byte-for-byte
//! identical to the pre-SSE format, so old blobs read unchanged.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce as GcmNonce};
use cairn_types::SecretKey32;
use cairn_types::blob::BlobCipher;
pub use cairn_types::bucket::CompressionAlgorithm;
use cairn_types::error::BlobError;
pub use cairn_types::object::CompressionDescriptor;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom};

const MAGIC: &[u8; 4] = b"CRNB";
/// Format version for an unencrypted blob (byte-identical to the pre-SSE format).
const VERSION_PLAIN: u8 = 1;
/// Legacy per-block AES-256-GCM format. Blocks are authenticated, but the index/trailer are not.
const VERSION_ENCRYPTED_V2: u8 = 2;
/// Current encrypted format: v2 block encryption plus an authenticated index/trailer.
const VERSION_ENCRYPTED: u8 = 3;
const TRAILER_LEN: u64 = 34;
const INDEX_ENTRY_LEN: usize = 9;
/// HMAC-SHA256 tag appended between the index and trailer for encrypted format v3.
const METADATA_TAG_LEN: usize = 32;
/// Domain separation from the HMAC invocation used to derive per-block GCM nonces.
const METADATA_MAC_DOMAIN: &[u8] = b"cairn/crnb/v3/metadata";
/// Upper bound on a trailer's `block_size`, enforced at open. The writer uses ≤256 KiB; this cap is
/// far above that yet bounds the per-block `read_range`/decompression allocation a corrupt or
/// bit-rotted trailer could otherwise demand (the read path works one block at a time).
const MAX_BLOCK_SIZE: u64 = 16 * 1024 * 1024;
/// Maximum index bytes emitted or accepted before authentication. This limits encoded logical
/// size according to block geometry; the raw-file object ceiling is independent.
const MAX_INDEX_LEN: usize = 64 * 1024 * 1024;
/// A whole number of nine-byte entries, just below 64 KiB.
const INDEX_PAGE_ENTRIES: usize = 7281;
const INDEX_PAGE_BYTES: usize = INDEX_PAGE_ENTRIES * INDEX_ENTRY_LEN;
/// Codec workspace plus fixed reader/channel bookkeeping, separate from block/page buffers.
const READER_FIXED_BYTES: u64 = 1024 * 1024;

/// The AES-GCM nonce length (96 bits — the recommended GCM nonce size).
const GCM_NONCE_LEN: usize = 12;
/// AES-256-GCM appends a 16-byte authentication tag to every encrypted block.
const GCM_TAG_LEN: u64 = 16;

fn index_len_for_blocks(blocks: usize, limit: usize) -> Option<usize> {
    blocks.checked_mul(INDEX_ENTRY_LEN).filter(|&n| n <= limit)
}

fn check_encoded_len(logical_len: u64, block_size: u64, limit: usize) -> Result<(), BlobError> {
    if block_size == 0 || block_size > MAX_BLOCK_SIZE {
        return Err(BlobError::SizeExceeded);
    }
    let blocks =
        usize::try_from(logical_len.div_ceil(block_size)).map_err(|_| BlobError::SizeExceeded)?;
    index_len_for_blocks(blocks, limit).ok_or(BlobError::SizeExceeded)?;
    Ok(())
}

pub(crate) fn validate_encoded_len(logical_len: u64, block_size: u32) -> Result<(), BlobError> {
    check_encoded_len(logical_len, u64::from(block_size), MAX_INDEX_LEN)
}

/// Derive a block's deterministic 96-bit GCM nonce from `(dek, block_index)` as the first 12
/// bytes of `HMAC-SHA256(dek, block_index_le_u64)`. Distinct blocks get distinct nonces, and the
/// nonce never repeats for a fixed key within a blob, satisfying GCM's nonce-uniqueness
/// requirement without storing per-block nonces on disk.
fn block_nonce(dek: &[u8; 32], block_index: u64) -> [u8; GCM_NONCE_LEN] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(dek).expect("HMAC accepts any key length");
    mac.update(&block_index.to_le_bytes());
    let tag = mac.finalize().into_bytes();
    let mut nonce = [0u8; GCM_NONCE_LEN];
    nonce.copy_from_slice(&tag[..GCM_NONCE_LEN]);
    nonce
}

/// Encrypt one block's (already compressed-or-raw) physical bytes in place-by-return, appending
/// the 16-byte GCM tag. Used only on the encrypted-write path.
fn encrypt_block(
    dek: &[u8; 32],
    block_index: u64,
    plain_phys: &[u8],
) -> Result<Vec<u8>, BlobError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(dek));
    let nonce = block_nonce(dek, block_index);
    cipher
        .encrypt(GcmNonce::from_slice(&nonce), plain_phys)
        .map_err(|_| BlobError::Corruption("SSE block encryption failed".into()))
}

/// Decrypt one block's physical bytes (ciphertext + appended GCM tag), returning the
/// compressed-or-raw plaintext. A wrong DEK or tampered block fails authentication and yields
/// [`BlobError::Corruption`] rather than plaintext.
fn decrypt_block(
    dek: &[u8; 32],
    block_index: u64,
    cipher_phys: &[u8],
) -> Result<Vec<u8>, BlobError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(dek));
    let nonce = block_nonce(dek, block_index);
    cipher
        .decrypt(GcmNonce::from_slice(&nonce), cipher_phys)
        .map_err(|_| BlobError::Corruption("SSE block authentication failed".into()))
}

/// Authenticate the complete plaintext index and fixed trailer. The fixed domain label makes this
/// HMAC invocation disjoint from the per-block nonce derivation, which feeds only an eight-byte
/// block index to HMAC under the same DEK.
#[cfg(test)]
fn metadata_tag(dek: &[u8; 32], index: &[u8], trailer: &[u8]) -> [u8; METADATA_TAG_LEN] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(dek).expect("HMAC accepts any key length");
    mac.update(METADATA_MAC_DOMAIN);
    mac.update(index);
    mac.update(trailer);
    mac.finalize().into_bytes().into()
}

fn algo_code(a: CompressionAlgorithm) -> u8 {
    match a {
        CompressionAlgorithm::None => 0,
        CompressionAlgorithm::Zstd => 1,
        CompressionAlgorithm::Lz4 => 2,
    }
}
fn algo_from(code: u8) -> Result<CompressionAlgorithm, BlobError> {
    match code {
        0 => Ok(CompressionAlgorithm::None),
        1 => Ok(CompressionAlgorithm::Zstd),
        2 => Ok(CompressionAlgorithm::Lz4),
        other => Err(BlobError::Corruption(format!(
            "unsupported compression algorithm {other}"
        ))),
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

impl IndexEntry {
    fn parse(bytes: &[u8]) -> Result<Self, BlobError> {
        let compressed = match bytes[8] {
            0 => false,
            1 => true,
            other => {
                return Err(BlobError::Corruption(format!(
                    "invalid compressed flag {other}"
                )));
            }
        };
        Ok(Self {
            phys_len: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
            logical_len: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            compressed,
        })
    }
}

/// These summaries become trusted only after the complete index and trailer validate.
struct IndexPageSummary {
    fingerprint: [u8; 32],
    physical_offset: u64,
}

fn trusted_geometry(
    compression: &CompressionDescriptor,
) -> Result<(CompressionAlgorithm, u64), BlobError> {
    let (algorithm, block_size) = match *compression {
        CompressionDescriptor::Uncompressed => (
            CompressionAlgorithm::None,
            crate::DEFAULT_ENCRYPTED_BLOCK_SIZE,
        ),
        CompressionDescriptor::Compressed {
            algorithm,
            block_size,
        } => (algorithm, block_size),
    };
    if block_size == 0 || u64::from(block_size) > MAX_BLOCK_SIZE {
        return Err(BlobError::Corruption(
            "trusted block size is outside the supported range".into(),
        ));
    }
    Ok((algorithm, u64::from(block_size)))
}

fn index_memory_bound(block_count: usize) -> u64 {
    let pages = block_count.div_ceil(INDEX_PAGE_ENTRIES);
    let entries = block_count.min(INDEX_PAGE_ENTRIES);
    (pages * std::mem::size_of::<IndexPageSummary>()
        + entries * (INDEX_ENTRY_LEN + std::mem::size_of::<u64>())) as u64
}

pub(crate) fn read_memory_bound(
    compression: &CompressionDescriptor,
    logical_len: u64,
) -> Result<cairn_types::blob::ReadMemoryBound, BlobError> {
    let (_, block_size) = trusted_geometry(compression)?;
    let blocks = usize::try_from(logical_len.div_ceil(block_size))
        .map_err(|_| BlobError::Corruption("block count overflows".into()))?;
    index_len_for_blocks(blocks, MAX_INDEX_LEN)
        .ok_or_else(|| BlobError::Corruption("index length exceeds the maximum".into()))?;
    // Four queued frames, the delivered frame and overlapping range/decrypt/decompress buffers.
    // The fixed allowance covers codec workspace and the small reader/channel bookkeeping.
    // Index accounting uses the same page geometry and summary layout as the actual reader.
    Ok(cairn_types::blob::ReadMemoryBound {
        buffer_bytes: index_memory_bound(blocks)
            + 10 * (block_size + GCM_TAG_LEN)
            + READER_FIXED_BYTES,
        max_frame_bytes: block_size,
    })
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
/// and retains only index entries not yet drained by the staging adapter. That adapter limits
/// feed chunks and spools entries before finalization. With a DEK
/// ([`new_encrypted`](BlockEncoder::new_encrypted)), each block is AES-256-GCM-encrypted after
/// compression and the trailer records [`VERSION_ENCRYPTED`].
pub(crate) struct BlockEncoder {
    algo: CompressionAlgorithm,
    block_size: usize,
    buf: Vec<u8>,
    index: Vec<u8>,
    metadata_mac: Option<Hmac<Sha256>>,
    index_limit: usize,
    logical_len: u64,
    phys_len: u64,
    /// The raw 32-byte DEK when this is an SSE-S3 (encrypted) encoder; `None` stores plaintext.
    dek: Option<SecretKey32>,
    /// The next block index to emit (drives the deterministic per-block nonce).
    block_index: u64,
    /// Set if a block encryption failed; surfaced from [`finish_parts`](BlockEncoder::finish_parts).
    error: Option<BlobError>,
}

impl BlockEncoder {
    /// A new plaintext encoder for the given algorithm and logical block size.
    #[must_use]
    pub fn new(algo: CompressionAlgorithm, block_size: u32) -> Self {
        Self::with_dek(algo, block_size, None)
    }

    /// A new SSE-S3 encoder that compresses then AES-256-GCM-encrypts each block under `dek`.
    #[must_use]
    pub fn new_encrypted(algo: CompressionAlgorithm, block_size: u32, dek: SecretKey32) -> Self {
        Self::with_dek(algo, block_size, Some(dek))
    }

    fn with_dek(algo: CompressionAlgorithm, block_size: u32, dek: Option<SecretKey32>) -> Self {
        Self {
            algo,
            block_size: block_size as usize,
            buf: Vec::new(),
            index: Vec::new(),
            metadata_mac: dek.as_ref().map(|key| {
                let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key.expose_secret())
                    .expect("HMAC accepts any key length");
                mac.update(METADATA_MAC_DOMAIN);
                mac
            }),
            index_limit: MAX_INDEX_LEN,
            logical_len: 0,
            phys_len: 0,
            dek,
            block_index: 0,
            error: None,
        }
    }

    /// Feed plaintext; returns physical bytes to append for any blocks completed.
    ///
    /// # Errors
    /// Returns [`BlobError::SizeExceeded`] before copying input whose index cannot be read.
    /// A rejected feed also makes finalization fail, so a caller cannot publish a prefix.
    pub fn feed(&mut self, data: &[u8]) -> Result<Vec<u8>, BlobError> {
        let next = self.logical_len.checked_add(data.len() as u64);
        if next.is_none_or(|len| {
            check_encoded_len(len, self.block_size as u64, self.index_limit).is_err()
        }) {
            self.error = Some(BlobError::SizeExceeded);
            return Err(BlobError::SizeExceeded);
        }
        self.logical_len = next.ok_or(BlobError::SizeExceeded)?;
        let mut remaining = data;
        let mut out = Vec::new();
        while !remaining.is_empty() {
            let n = remaining.len().min(self.block_size - self.buf.len());
            self.buf.extend_from_slice(&remaining[..n]);
            remaining = &remaining[n..];
            if self.buf.len() == self.block_size {
                let mut block = std::mem::take(&mut self.buf);
                self.emit_block(&block, &mut out);
                block.clear();
                self.buf = block;
            }
        }
        Ok(out)
    }

    fn emit_block(&mut self, logical: &[u8], out: &mut Vec<u8>) {
        let (mut phys, compressed) = compress_block(self.algo, logical);
        if let Some(dek) = self.dek.as_ref() {
            match encrypt_block(dek.expose_secret(), self.block_index, &phys) {
                Ok(ciphertext) => phys = ciphertext,
                // Record the first failure; `finish` turns it into an `Err`. An encryption failure
                // here is effectively unreachable (AES-GCM only fails on absurd sizes).
                Err(e) => {
                    self.error.get_or_insert(e);
                }
            }
        }
        let mut entry = [0u8; INDEX_ENTRY_LEN];
        entry[..4].copy_from_slice(&(phys.len() as u32).to_le_bytes());
        entry[4..8].copy_from_slice(&(logical.len() as u32).to_le_bytes());
        entry[8] = u8::from(compressed);
        self.index.extend_from_slice(&entry);
        if let Some(mac) = &mut self.metadata_mac {
            mac.update(&entry);
        }
        self.phys_len += phys.len() as u64;
        self.block_index += 1;
        out.extend_from_slice(&phys);
    }

    /// Drain serialized entries after each bounded feed, before accepting more input.
    pub(crate) fn take_index(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.index)
    }

    /// Flush the final partial block and return the remaining index and fixed footer separately.
    /// All earlier drained index entries must precede this footer in the final blob.
    pub(crate) fn finish_parts(mut self) -> Result<EncodedTail, BlobError> {
        if let Some(e) = self.error.take() {
            return Err(e);
        }
        check_encoded_len(self.logical_len, self.block_size as u64, self.index_limit)?;
        let mut out = Vec::new();
        if !self.buf.is_empty() {
            let block = std::mem::take(&mut self.buf);
            self.emit_block(&block, &mut out);
        }
        if let Some(e) = self.error.take() {
            return Err(e);
        }
        let index_offset = self.phys_len;
        let index_len = index_len_for_blocks(self.block_index as usize, self.index_limit)
            .ok_or(BlobError::SizeExceeded)?;
        let block_count = u32::try_from(self.block_index).map_err(|_| BlobError::SizeExceeded)?;
        let index_len_u32 = u32::try_from(index_len).map_err(|_| BlobError::SizeExceeded)?;
        let version = if self.dek.is_some() {
            VERSION_ENCRYPTED
        } else {
            VERSION_PLAIN
        };
        let mut trailer = Vec::with_capacity(TRAILER_LEN as usize);
        trailer.extend_from_slice(MAGIC);
        trailer.push(version);
        trailer.push(algo_code(self.algo));
        trailer.extend_from_slice(&(self.block_size as u32).to_le_bytes());
        trailer.extend_from_slice(&self.logical_len.to_le_bytes());
        trailer.extend_from_slice(&block_count.to_le_bytes());
        trailer.extend_from_slice(&index_offset.to_le_bytes());
        trailer.extend_from_slice(&index_len_u32.to_le_bytes());

        let mut footer = Vec::with_capacity(METADATA_TAG_LEN + TRAILER_LEN as usize);
        if let Some(mut mac) = self.metadata_mac {
            mac.update(&trailer);
            footer.extend_from_slice(&mac.finalize().into_bytes());
        }
        footer.extend_from_slice(&trailer);
        Ok(EncodedTail {
            payload: out,
            index: self.index,
            footer,
        })
    }
}

/// Final payload and index bytes precede the fixed authentication/trailer footer on disk.
pub(crate) struct EncodedTail {
    pub(crate) payload: Vec<u8>,
    pub(crate) index: Vec<u8>,
    pub(crate) footer: Vec<u8>,
}

#[cfg(test)]
impl BlockEncoder {
    fn finish(self) -> Result<Vec<u8>, BlobError> {
        let tail = self.finish_parts()?;
        let mut out = tail.payload;
        out.extend(tail.index);
        out.extend(tail.footer);
        Ok(out)
    }
}

/// The fixed CRNB trailer length, exported for diagnostics and tests that identify stored
/// encrypted artifacts without attempting to read their object bytes.
pub const TRAILER_BYTES: usize = TRAILER_LEN as usize;

/// Return whether a final [`TRAILER_BYTES`]-byte slice is structurally an encrypted CRNB trailer.
///
/// This helper is diagnostic only. The read path must never use body sniffing to choose framing;
/// object/part metadata supplies that authority.
#[must_use]
pub fn is_encrypted_container_trailer(trailer: &[u8], total: u64) -> bool {
    if trailer.len() != TRAILER_BYTES || total < TRAILER_LEN {
        return false;
    }
    if &trailer[0..4] != MAGIC || !matches!(trailer[4], VERSION_ENCRYPTED_V2 | VERSION_ENCRYPTED) {
        return false;
    }
    let metadata_tag_len = if trailer[4] == VERSION_ENCRYPTED {
        METADATA_TAG_LEN as u64
    } else {
        0
    };
    let block_count = u32::from_le_bytes(trailer[18..22].try_into().unwrap()) as u64;
    let index_offset = u64::from_le_bytes(trailer[22..30].try_into().unwrap());
    let index_len = u64::from(u32::from_le_bytes(trailer[30..34].try_into().unwrap()));
    index_len == block_count * INDEX_ENTRY_LEN as u64
        && index_offset
            .checked_add(index_len)
            .and_then(|n| n.checked_add(metadata_tag_len))
            .and_then(|n| n.checked_add(TRAILER_LEN))
            == Some(total)
}

/// DEK-free geometry check for an already anchored encrypted multipart file. Parts are always
/// uncompressed, so trusted plaintext length and format determine their physical extent exactly.
/// This reads only the fixed trailer; it neither authenticates metadata nor checks payload bytes.
pub(crate) fn verify_encrypted_part_geometry(
    file: &mut (impl Read + Seek),
    total: u64,
    logical_len: u64,
    authenticated: bool,
) -> Result<(), BlobError> {
    let error = || BlobError::Corruption("encrypted part framing does not match metadata".into());
    if total < TRAILER_LEN {
        return Err(error());
    }
    validate_encoded_len(logical_len, crate::DEFAULT_ENCRYPTED_BLOCK_SIZE)?;
    file.seek(SeekFrom::End(-(TRAILER_LEN as i64)))
        .map_err(crate::io_err)?;
    let mut trailer = [0; TRAILER_BYTES];
    file.read_exact(&mut trailer).map_err(crate::io_err)?;
    let block_size = u64::from(crate::DEFAULT_ENCRYPTED_BLOCK_SIZE);
    let blocks = logical_len.div_ceil(block_size);
    let payload_len = blocks
        .checked_mul(GCM_TAG_LEN)
        .and_then(|tags| logical_len.checked_add(tags))
        .ok_or_else(error)?;
    let version = if authenticated {
        VERSION_ENCRYPTED
    } else {
        VERSION_ENCRYPTED_V2
    };
    if !is_encrypted_container_trailer(&trailer, total)
        || trailer[4] != version
        || trailer[5] != algo_code(CompressionAlgorithm::None)
        || u64::from(u32::from_le_bytes(trailer[6..10].try_into().unwrap())) != block_size
        || u64::from_le_bytes(trailer[10..18].try_into().unwrap()) != logical_len
        || u64::from(u32::from_le_bytes(trailer[18..22].try_into().unwrap())) != blocks
        || u64::from_le_bytes(trailer[22..30].try_into().unwrap()) != payload_len
    {
        return Err(error());
    }
    Ok(())
}

/// A random-access reader over a compressed (and optionally SSE-S3-encrypted) blob file.
pub struct CompressedReader<R: Read + Seek> {
    inner: R,
    algo: CompressionAlgorithm,
    block_size: u64,
    logical_len: u64,
    index_offset: u64,
    index_len: usize,
    pages: Vec<IndexPageSummary>,
    page_bytes: Vec<u8>,
    page_offsets: Vec<u64>,
    cached_page: Option<usize>,
    /// `true` when the trailer version is [`VERSION_ENCRYPTED`]; reads then require a DEK.
    encrypted: bool,
    /// The raw 32-byte DEK supplied by the caller, if any.
    dek: Option<SecretKey32>,
}

impl<R: Read + Seek> CompressedReader<R> {
    /// Read the trailer and index under the caller's metadata-backed cipher declaration.
    ///
    /// The expected CRNB version is part of [`BlobCipher`], not inferred from the file: current v3
    /// metadata can therefore never be downgraded into the legacy-v2 parser by changing on-disk
    /// framing. `compression` is the independently stored logical compression descriptor; every format
    /// must match its trailer algorithm and geometry to that trusted expectation.
    /// `expected_logical_len` is likewise trusted object/part metadata and binds the
    /// trailer and index total. A wrong version, size/compression expectation, absent key, or bad key
    /// fails closed before object bytes are returned.
    pub fn open_with_dek(
        mut inner: R,
        cipher: BlobCipher,
        compression: &CompressionDescriptor,
        expected_logical_len: u64,
    ) -> Result<Self, BlobError> {
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
        let version = t[4];
        let expected_version = match &cipher {
            BlobCipher::KnownPlaintext => VERSION_PLAIN,
            BlobCipher::LegacyV2(_) => VERSION_ENCRYPTED_V2,
            BlobCipher::AuthenticatedV3(_) => VERSION_ENCRYPTED,
        };
        if version != expected_version {
            return Err(BlobError::Corruption(format!(
                "blob format version {version} does not match metadata expectation {expected_version}"
            )));
        }
        let dek = cipher.dek();
        let (encrypted, authenticated_metadata) = match version {
            VERSION_PLAIN => (false, false),
            VERSION_ENCRYPTED_V2 => (true, false),
            VERSION_ENCRYPTED => (true, true),
            other => {
                return Err(BlobError::Corruption(format!(
                    "unsupported blob format version {other}"
                )));
            }
        };
        if encrypted && dek.is_none() {
            return Err(BlobError::Corruption(
                "blob is SSE-S3 encrypted but no data-encryption key was supplied".into(),
            ));
        }
        let block_count = u32::from_le_bytes(t[18..22].try_into().unwrap()) as usize;
        let index_offset = u64::from_le_bytes(t[22..30].try_into().unwrap());
        let index_len = u32::from_le_bytes(t[30..34].try_into().unwrap()) as usize;

        // `block_count` and `index_len` come straight from the (possibly bit-rotted or otherwise
        // corrupt) trailer, so validate them BEFORE allocating: a `checked_mul` avoids a usize
        // overflow, and bounding the index against the actual file size stops a trailer claiming a
        // gigabyte index on a tiny file from forcing a multi-GB `vec![0u8; index_len]` allocation
        // (an out-of-memory DoS the `read_exact` below would only catch after the allocation).
        let expected_index_len = index_len_for_blocks(block_count, MAX_INDEX_LEN)
            .ok_or_else(|| BlobError::Corruption("index length exceeds the maximum".into()))?;
        if index_len != expected_index_len {
            return Err(BlobError::Corruption("index length mismatch".into()));
        }
        if index_len > MAX_INDEX_LEN {
            return Err(BlobError::Corruption(
                "index length exceeds the maximum".into(),
            ));
        }
        // The persisted compression descriptor and object/part logical size are independent of
        // this on-disk trailer. Use them to bound and cross-check the block count before allocating
        // the unauthenticated index. Besides the absolute cap above, this rejects a corrupt large
        // file whose trailer invents more entries than the authoritative row can address.
        let (expected_algo, trusted_block_size) = trusted_geometry(compression)?;
        let trusted_block_count = if expected_logical_len == 0 {
            0
        } else {
            expected_logical_len.div_ceil(trusted_block_size)
        };
        if block_count as u64 != trusted_block_count {
            return Err(BlobError::Corruption(
                "block count does not match trusted metadata".into(),
            ));
        }
        // The index, optional v3 metadata tag, and trailer are contiguous at the end of the file.
        // Validate their exact layout before allocating or seeking. Besides bounding the index
        // allocation by the file, exactness makes changing v3's version byte to legacy v2 fail
        // closed: the unexplained 32-byte tag cannot be treated as block or index data.
        let index_end = index_offset
            .checked_add(index_len as u64)
            .ok_or_else(|| BlobError::Corruption("index end overflows".into()))?;
        let metadata_tag_len = if authenticated_metadata {
            METADATA_TAG_LEN as u64
        } else {
            0
        };
        let metadata_end = index_end
            .checked_add(metadata_tag_len)
            .ok_or_else(|| BlobError::Corruption("metadata end overflows".into()))?;
        if metadata_end != total - TRAILER_LEN {
            return Err(BlobError::Corruption(
                "index and metadata tag do not exactly precede the trailer".into(),
            ));
        }
        // Geometry is still provisional here. Bind it to trusted metadata before allocating
        // bounded page state; neither it nor a page summary escapes before final authentication.
        let algo = algo_from(t[5])?;
        let block_size = u64::from(u32::from_le_bytes(t[6..10].try_into().unwrap()));
        let logical_len = u64::from_le_bytes(t[10..18].try_into().unwrap());
        if logical_len != expected_logical_len {
            return Err(BlobError::Corruption(
                "blob logical length does not match its trusted metadata expectation".into(),
            ));
        }
        if algo != expected_algo || block_size != trusted_block_size {
            return Err(BlobError::Corruption(
                "blob compression metadata does not match its trusted metadata expectation".into(),
            ));
        }
        let mut mac = authenticated_metadata.then(|| {
            let key = dek.as_ref().expect("encrypted formats require a DEK");
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key.expose_secret())
                .expect("HMAC accepts any key length");
            mac.update(METADATA_MAC_DOMAIN);
            mac
        });
        let mut pages = Vec::with_capacity(block_count.div_ceil(INDEX_PAGE_ENTRIES));
        let mut page_bytes = vec![0u8; index_len.min(INDEX_PAGE_BYTES)];
        let mut physical_offset = 0u64;
        let mut logical_offset = 0u64;
        let tag_len = if encrypted { GCM_TAG_LEN } else { 0 };
        inner.seek(SeekFrom::Start(index_offset)).map_err(io)?;
        for page_start in (0..index_len).step_by(INDEX_PAGE_BYTES) {
            let n = (index_len - page_start).min(INDEX_PAGE_BYTES);
            let bytes = &mut page_bytes[..n];
            inner.read_exact(bytes).map_err(io)?;
            if let Some(mac) = &mut mac {
                mac.update(bytes);
            }
            pages.push(IndexPageSummary {
                fingerprint: Sha256::digest(&*bytes).into(),
                physical_offset,
            });
            for chunk in bytes.chunks_exact(INDEX_ENTRY_LEN) {
                let entry = IndexEntry::parse(chunk)?;
                // Non-final blocks are full; only the final one may be partial. The trusted
                // count already pins exactly how many entries must cover the logical length.
                let expected = logical_len
                    .checked_sub(logical_offset)
                    .ok_or_else(|| {
                        BlobError::Corruption("logical block geometry underflows".into())
                    })?
                    .min(block_size);
                if expected == 0 || u64::from(entry.logical_len) != expected {
                    return Err(BlobError::Corruption(
                        "index block length does not match the fixed block geometry".into(),
                    ));
                }
                logical_offset = logical_offset.checked_add(expected).ok_or_else(|| {
                    BlobError::Corruption("logical block geometry overflows".into())
                })?;
                // Raw and compressed lengths are disjoint, including legacy v2's GCM tag.
                // Enforce this before a later block read can allocate from a physical length.
                let payload_len =
                    u64::from(entry.phys_len)
                        .checked_sub(tag_len)
                        .ok_or_else(|| {
                            BlobError::Corruption(
                                "block is shorter than its authentication tag".into(),
                            )
                        })?;
                let valid = if entry.compressed {
                    payload_len > 0 && payload_len < expected
                } else {
                    payload_len == expected
                };
                if !valid {
                    return Err(BlobError::Corruption(
                        "block physical length contradicts its compression flag".into(),
                    ));
                }
                physical_offset = physical_offset
                    .checked_add(u64::from(entry.phys_len))
                    .ok_or_else(|| BlobError::Corruption("block offset overflows".into()))?;
            }
        }
        if physical_offset != index_offset {
            return Err(BlobError::Corruption(
                "block physical lengths do not fill the block region".into(),
            ));
        }
        if logical_offset != logical_len {
            return Err(BlobError::Corruption(
                "index logical lengths do not sum to the logical length".into(),
            ));
        }
        if let Some(mut mac) = mac {
            let mut tag = [0u8; METADATA_TAG_LEN];
            inner.read_exact(&mut tag).map_err(io)?;
            mac.update(&t);
            mac.verify_slice(&tag).map_err(|_| {
                BlobError::Corruption("encrypted blob metadata authentication failed".into())
            })?;
        }
        // Only now can the caller obtain the page fingerprints and starting physical offsets.
        // One page plus its offsets is retained; the original index-sized buffers are gone.
        Ok(Self {
            inner,
            algo,
            block_size,
            logical_len,
            index_offset,
            index_len,
            pages,
            page_bytes,
            page_offsets: Vec::with_capacity(block_count.min(INDEX_PAGE_ENTRIES)),
            cached_page: None,
            encrypted,
            dek,
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

    /// Re-read through the retained descriptor and authenticate the entire page before decoding
    /// any entry. Cached bytes are already verified; no mutable trailer field is loaded again.
    fn entry(&mut self, block: usize) -> Result<(IndexEntry, u64), BlobError> {
        let page = block / INDEX_PAGE_ENTRIES;
        let within = block % INDEX_PAGE_ENTRIES;
        if self.cached_page != Some(page) {
            // An interrupted/failed load must not leave a previous page marked usable while its
            // buffer now contains a partial replacement.
            self.cached_page = None;
            let summary = self.pages.get(page).ok_or_else(|| {
                BlobError::Corruption("block is outside the verified index".into())
            })?;
            let start = page * INDEX_PAGE_BYTES;
            let n = (self.index_len - start).min(INDEX_PAGE_BYTES);
            let bytes = &mut self.page_bytes[..n];
            self.inner
                .seek(SeekFrom::Start(self.index_offset + start as u64))
                .map_err(crate::io_err)?;
            self.inner.read_exact(bytes).map_err(crate::io_err)?;
            let fingerprint: [u8; 32] = Sha256::digest(&*bytes).into();
            if fingerprint != summary.fingerprint {
                return Err(BlobError::Corruption(
                    "index page changed after initial verification".into(),
                ));
            }
            self.page_offsets.clear();
            let mut offset = summary.physical_offset;
            for chunk in bytes.chunks_exact(INDEX_ENTRY_LEN) {
                let entry = IndexEntry::parse(chunk)?;
                self.page_offsets.push(offset);
                offset = offset
                    .checked_add(u64::from(entry.phys_len))
                    .ok_or_else(|| BlobError::Corruption("block offset overflows".into()))?;
            }
            self.cached_page = Some(page);
        }
        let offset = *self
            .page_offsets
            .get(within)
            .ok_or_else(|| BlobError::Corruption("block is outside the verified page".into()))?;
        let start = within * INDEX_ENTRY_LEN;
        Ok((
            IndexEntry::parse(&self.page_bytes[start..start + INDEX_ENTRY_LEN])?,
            offset,
        ))
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
            let (entry, physical_offset) = self.entry(b)?;
            self.inner
                .seek(SeekFrom::Start(physical_offset))
                .map_err(io)?;
            let mut phys = vec![0u8; entry.phys_len as usize];
            self.inner.read_exact(&mut phys).map_err(io)?;
            // SSE-S3: decrypt the block before decompression (compress-then-encrypt is reversed on
            // read). A wrong/absent DEK or a tampered block fails authentication here.
            if self.encrypted {
                let dek = self.dek.as_ref().ok_or_else(|| {
                    BlobError::Corruption("encrypted blob read without a DEK".into())
                })?;
                phys = decrypt_block(dek.expose_secret(), b as u64, &phys)?;
            }
            let logical = decompress_block(
                self.algo,
                &phys,
                entry.logical_len as usize,
                entry.compressed,
            )?;
            // Legacy encrypted v2 did not authenticate the index. In particular, flipping a
            // compressed flag to false used to return the decrypted compressed representation.
            // Whether decompressed or raw, a block must produce exactly its recorded logical size.
            if logical.len() != entry.logical_len as usize {
                return Err(BlobError::Corruption(
                    "block output length does not match authenticated metadata".into(),
                ));
            }
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
        let mut out = enc.feed(data).unwrap();
        out.extend_from_slice(&enc.finish().unwrap());
        out
    }

    fn encode_encrypted(
        algo: CompressionAlgorithm,
        block_size: u32,
        dek: [u8; 32],
        data: &[u8],
    ) -> Vec<u8> {
        let mut enc = BlockEncoder::new_encrypted(algo, block_size, dek.into());
        let mut out = enc.feed(data).unwrap();
        out.extend_from_slice(&enc.finish().unwrap());
        out
    }

    /// Convert a current v3 fixture to the legacy v2 layout. Block encryption is identical; v2
    /// simply omitted the metadata tag and carried version byte 2.
    fn encode_encrypted_v2(
        algo: CompressionAlgorithm,
        block_size: u32,
        dek: [u8; 32],
        data: &[u8],
    ) -> Vec<u8> {
        let mut blob = encode_encrypted(algo, block_size, dek, data);
        let trailer_start = blob.len() - TRAILER_LEN as usize;
        assert_eq!(blob[trailer_start + 4], VERSION_ENCRYPTED);
        blob.drain(trailer_start - METADATA_TAG_LEN..trailer_start);
        let trailer_start = blob.len() - TRAILER_LEN as usize;
        blob[trailer_start + 4] = VERSION_ENCRYPTED_V2;
        blob
    }

    /// Build a raw 34-byte CRNB trailer with the given fields, for malformed-input tests.
    fn trailer(
        version: u8,
        algo: u8,
        block_size: u32,
        logical_len: u64,
        block_count: u32,
        index_offset: u64,
        index_len: u32,
    ) -> Vec<u8> {
        let mut t = Vec::with_capacity(TRAILER_LEN as usize);
        t.extend_from_slice(MAGIC);
        t.push(version);
        t.push(algo);
        t.extend_from_slice(&block_size.to_le_bytes());
        t.extend_from_slice(&logical_len.to_le_bytes());
        t.extend_from_slice(&block_count.to_le_bytes());
        t.extend_from_slice(&index_offset.to_le_bytes());
        t.extend_from_slice(&index_len.to_le_bytes());
        assert_eq!(t.len(), TRAILER_LEN as usize);
        t
    }

    /// Regression (fuzz-found, `compress_reader`): a trailer claiming a positive `logical_len` that
    /// the index does not cover (here zero blocks) must be rejected at OPEN, rather than panicking
    /// on a read that maps an offset to a block index past the (empty) index.
    #[test]
    fn open_rejects_logical_len_not_covered_by_index() {
        let blob = trailer(VERSION_PLAIN, 1, 100, 1000, 0, 0, 0);
        let err = CompressedReader::open_with_dek(
            Cursor::new(blob),
            BlobCipher::KnownPlaintext,
            &CompressionDescriptor::Uncompressed,
            1000,
        );
        assert!(
            err.is_err(),
            "logical_len uncovered by the index must be rejected"
        );
    }

    /// Regression: a corrupt trailer claiming a gigantic index on a tiny file must be rejected
    /// before the index allocation — bounding it by the file size prevents an out-of-memory DoS.
    #[test]
    fn open_rejects_oversized_index_without_allocating() {
        let block_count: u32 = 100_000_000; // index_len = 900 MB, but the file is 34 bytes
        let blob = trailer(
            VERSION_PLAIN,
            1,
            4096,
            4096,
            block_count,
            0,
            block_count * 9,
        );
        let err = CompressedReader::open_with_dek(
            Cursor::new(blob),
            BlobCipher::KnownPlaintext,
            &CompressionDescriptor::Uncompressed,
            4096,
        );
        assert!(
            err.is_err(),
            "an index larger than the file must be rejected"
        );
    }

    /// Regression (fuzz-found, OOM): a trailer claiming a block size above the cap must be rejected
    /// at open, so the per-block read path cannot be driven to allocate an outsized buffer.
    #[test]
    fn open_rejects_block_size_over_the_cap() {
        // A minimal valid 1-block blob: [1 raw byte][index entry][trailer], but with a block size
        // just over MAX_BLOCK_SIZE. Every other field is internally consistent, so only the cap
        // rejects it.
        let mut blob = vec![0u8]; // one raw block byte
        blob.extend_from_slice(&1u32.to_le_bytes()); // index: phys_len = 1
        blob.extend_from_slice(&1u32.to_le_bytes()); // index: logical_len = 1
        blob.push(0); // index: compressed = false
        let over_cap = (MAX_BLOCK_SIZE + 1) as u32;
        blob.extend_from_slice(&trailer(VERSION_PLAIN, 0, over_cap, 1, 1, 1, 9));
        assert!(
            CompressedReader::open_with_dek(
                Cursor::new(blob),
                BlobCipher::KnownPlaintext,
                &CompressionDescriptor::Uncompressed,
                1,
            )
            .is_err(),
            "a block size over the cap must be rejected"
        );
    }

    /// Regression (fuzz-found, OOM): an index entry claiming a huge physical length (`phys_len` is a
    /// u32, up to ~4 GiB) must be rejected at open — the per-block physical lengths must sum to the
    /// index offset — so a read never allocates a multi-gigabyte `phys` buffer for a corrupt block.
    #[test]
    fn open_rejects_oversized_phys_len() {
        // [1 raw byte][index entry claiming phys_len = ~3 GiB][trailer]. The block region is really 1
        // byte, so the claimed phys_len cannot sum to index_offset (1) — rejected before any read.
        let mut blob = vec![0u8]; // one real block byte
        blob.extend_from_slice(&3_000_000_000u32.to_le_bytes()); // index: phys_len = ~3 GiB (a lie)
        blob.extend_from_slice(&1u32.to_le_bytes()); // index: logical_len = 1
        blob.push(0); // index: compressed = false
        blob.extend_from_slice(&trailer(VERSION_PLAIN, 0, 4096, 1, 1, 1, 9));
        assert!(
            CompressedReader::open_with_dek(
                Cursor::new(blob),
                BlobCipher::KnownPlaintext,
                &CompressionDescriptor::Uncompressed,
                1,
            )
            .is_err(),
            "a block phys_len that overruns the block region must be rejected"
        );
    }

    /// An empty blob with a non-zero block count, and a too-short file, are both rejected cleanly.
    #[test]
    fn open_rejects_inconsistent_empty_and_short() {
        let blob = trailer(VERSION_PLAIN, 1, 4096, 0, 3, 0, 27);
        assert!(
            CompressedReader::open_with_dek(
                Cursor::new(blob),
                BlobCipher::KnownPlaintext,
                &CompressionDescriptor::Uncompressed,
                0,
            )
            .is_err()
        );
        assert!(
            CompressedReader::open_with_dek(
                Cursor::new(vec![0u8; 10]),
                BlobCipher::KnownPlaintext,
                &CompressionDescriptor::Uncompressed,
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn roundtrip_full_and_ranges() {
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let blob = encode(CompressionAlgorithm::Zstd, 1024, &data);
        let mut r = CompressedReader::open_with_dek(
            Cursor::new(blob),
            BlobCipher::KnownPlaintext,
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Zstd,
                block_size: 1024,
            },
            data.len() as u64,
        )
        .unwrap();
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
        let mut r = CompressedReader::open_with_dek(
            Cursor::new(blob),
            BlobCipher::KnownPlaintext,
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Zstd,
                block_size: 1024,
            },
            data.len() as u64,
        )
        .unwrap();
        assert_eq!(r.read_range(0, 4096).unwrap(), data);
        for block in 0..4 {
            let (entry, _) = r.entry(block).unwrap();
            assert!(entry.phys_len <= entry.logical_len);
        }
    }

    #[test]
    fn compressible_data_actually_shrinks() {
        let data = vec![b'a'; 10_000];
        let blob = encode(CompressionAlgorithm::Zstd, 1024, &data);
        assert!(
            (blob.len() as u64) < 10_000,
            "highly compressible data must shrink on disk"
        );
        let mut r = CompressedReader::open_with_dek(
            Cursor::new(blob),
            BlobCipher::KnownPlaintext,
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Zstd,
                block_size: 1024,
            },
            data.len() as u64,
        )
        .unwrap();
        assert_eq!(r.read_range(0, 10_000).unwrap(), data);
    }

    #[test]
    fn lz4_roundtrip() {
        let data = vec![b'x'; 3000];
        let blob = encode(CompressionAlgorithm::Lz4, 1024, &data);
        let mut r = CompressedReader::open_with_dek(
            Cursor::new(blob),
            BlobCipher::KnownPlaintext,
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Lz4,
                block_size: 1024,
            },
            data.len() as u64,
        )
        .unwrap();
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

    /// A compressed+encrypted blob round-trips: full read and a mid-block ranged read both return
    /// the original plaintext when opened with the correct DEK (SSE-S3, ARCH 27).
    #[test]
    fn encrypted_roundtrip_full_and_ranges() {
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let dek = [0x42u8; 32];
        let blob = encode_encrypted(CompressionAlgorithm::Zstd, 1024, dek, &data);
        let mut r = CompressedReader::open_with_dek(
            Cursor::new(blob),
            BlobCipher::AuthenticatedV3(dek.into()),
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Zstd,
                block_size: 1024,
            },
            data.len() as u64,
        )
        .unwrap();
        assert_eq!(r.logical_len(), 5000);
        assert_eq!(r.read_range(0, 5000).unwrap(), data);
        // A range that starts mid-block near the end: only the overlapping blocks are decrypted.
        assert_eq!(r.read_range(4096, 500).unwrap(), &data[4096..4596]);
        // A range spanning a block boundary.
        assert_eq!(r.read_range(1000, 100).unwrap(), &data[1000..1100]);
    }

    /// AUD-024 reproduction: the encrypted block authenticates its compressed bytes, but format
    /// v2 did not authenticate the plaintext index. Flipping only the `compressed` flag therefore
    /// made the reader return the decrypted zstd representation as object bytes. Every encrypted
    /// format written after the fix must reject the same metadata-only mutation.
    #[test]
    fn encrypted_index_compression_flag_tamper_is_corruption() {
        let data = vec![b'a'; 1024];
        let dek = [0x24u8; 32];
        let mut blob = encode_encrypted(CompressionAlgorithm::Zstd, 1024, dek, &data);
        let trailer_start = blob.len() - TRAILER_LEN as usize;
        let index_offset = u64::from_le_bytes(
            blob[trailer_start + 22..trailer_start + 30]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(blob[index_offset + 8], 1, "fixture block must compress");
        blob[index_offset + 8] = 0;

        let result = CompressedReader::open_with_dek(
            Cursor::new(blob),
            BlobCipher::AuthenticatedV3(dek.into()),
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Zstd,
                block_size: 1024,
            },
            data.len() as u64,
        )
        .and_then(|mut reader| reader.read_range(0, data.len() as u64));
        assert!(
            matches!(result, Err(BlobError::Corruption(_))),
            "metadata-only tampering must never return decrypted compressed bytes"
        );
    }

    /// Every byte of the v3 index, metadata tag, and trailer is authenticated (or is a structural
    /// field needed to locate that authentication). Mutating any one byte must fail before object
    /// bytes are returned.
    #[test]
    fn encrypted_v3_authenticates_every_metadata_byte() {
        let data = vec![b'm'; 3000];
        let dek = [0x19u8; 32];
        let blob = encode_encrypted(CompressionAlgorithm::Zstd, 1024, dek, &data);
        let trailer_start = blob.len() - TRAILER_LEN as usize;
        let index_offset = u64::from_le_bytes(
            blob[trailer_start + 22..trailer_start + 30]
                .try_into()
                .unwrap(),
        ) as usize;

        for position in index_offset..blob.len() {
            let mut mutated = blob.clone();
            mutated[position] ^= 1;
            let result = CompressedReader::open_with_dek(
                Cursor::new(mutated),
                BlobCipher::AuthenticatedV3(dek.into()),
                &CompressionDescriptor::Compressed {
                    algorithm: CompressionAlgorithm::Zstd,
                    block_size: 1024,
                },
                data.len() as u64,
            )
            .and_then(|mut reader| reader.read_range(0, data.len() as u64));
            assert!(
                matches!(result, Err(BlobError::Corruption(_))),
                "metadata byte {position} was mutable without a corruption error"
            );
        }
    }

    /// Legacy v2 encrypted blobs remain readable, but strict output-length validation closes the
    /// reproduced compression-flag attack even though those historical files have no metadata MAC.
    #[test]
    fn legacy_encrypted_v2_is_readable_and_rejects_flag_tampering() {
        let data = vec![b'v'; 2048];
        let dek = [0x82u8; 32];
        let blob = encode_encrypted_v2(CompressionAlgorithm::Zstd, 1024, dek, &data);
        let trailer_start = blob.len() - TRAILER_LEN as usize;
        assert_eq!(blob[trailer_start + 4], VERSION_ENCRYPTED_V2);
        let mut reader = CompressedReader::open_with_dek(
            Cursor::new(blob.clone()),
            BlobCipher::LegacyV2(dek.into()),
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Zstd,
                block_size: 1024,
            },
            data.len() as u64,
        )
        .unwrap();
        assert_eq!(reader.read_range(0, data.len() as u64).unwrap(), data);

        let index_offset = u64::from_le_bytes(
            blob[trailer_start + 22..trailer_start + 30]
                .try_into()
                .unwrap(),
        ) as usize;
        let mut tampered = blob;
        assert_eq!(tampered[index_offset + 8], 1);
        tampered[index_offset + 8] = 0;
        let result = CompressedReader::open_with_dek(
            Cursor::new(tampered),
            BlobCipher::LegacyV2(dek.into()),
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Zstd,
                block_size: 1024,
            },
            data.len() as u64,
        )
        .and_then(|mut reader| reader.read_range(0, data.len() as u64));
        assert!(matches!(result, Err(BlobError::Corruption(_))));
    }

    /// Exact-head regression: these six raw plaintext bytes are also a valid LZ4 stream that
    /// decompresses to a different six-byte value. Legacy v2 authenticates the bytes but not the
    /// index flag, so output-length validation alone cannot distinguish the two interpretations.
    #[test]
    fn legacy_v2_rejects_same_length_lz4_polyglot_flag_flip() {
        let data = vec![0x10, b'A', 1, 0, 0x10, b'B'];
        let alternate = lz4_flex::decompress(&data, data.len()).unwrap();
        assert_eq!(alternate.len(), data.len());
        assert_ne!(alternate, data);

        let dek = [0x31u8; 32];
        let mut blob =
            encode_encrypted_v2(CompressionAlgorithm::Lz4, data.len() as u32, dek, &data);
        let trailer_start = blob.len() - TRAILER_LEN as usize;
        let index_offset = u64::from_le_bytes(
            blob[trailer_start + 22..trailer_start + 30]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(
            blob[index_offset + 8],
            0,
            "the legitimate fixture must use its raw fallback"
        );
        let descriptor = CompressionDescriptor::Compressed {
            algorithm: CompressionAlgorithm::Lz4,
            block_size: data.len() as u32,
        };
        let mut reader = CompressedReader::open_with_dek(
            Cursor::new(blob.clone()),
            BlobCipher::LegacyV2(dek.into()),
            &descriptor,
            data.len() as u64,
        )
        .unwrap();
        assert_eq!(reader.read_range(0, data.len() as u64).unwrap(), data);

        blob[index_offset + 8] = 1;
        assert!(matches!(
            CompressedReader::open_with_dek(
                Cursor::new(blob),
                BlobCipher::LegacyV2(dek.into()),
                &descriptor,
                data.len() as u64,
            ),
            Err(BlobError::Corruption(_))
        ));
    }

    /// Legacy v2's unauthenticated trailer cannot override the algorithm or logical block geometry
    /// recorded in trusted metadata. The encryption-only descriptor has one canonical physical
    /// expectation: algorithm None with the default encrypted block size.
    #[test]
    fn legacy_v2_algorithm_and_geometry_must_match_compression_descriptor() {
        let data = vec![b'g'; 128];
        let dek = [0x47u8; 32];
        let blob = encode_encrypted_v2(
            CompressionAlgorithm::None,
            crate::DEFAULT_ENCRYPTED_BLOCK_SIZE,
            dek,
            &data,
        );
        let mut reader = CompressedReader::open_with_dek(
            Cursor::new(blob.clone()),
            BlobCipher::LegacyV2(dek.into()),
            &CompressionDescriptor::Uncompressed,
            data.len() as u64,
        )
        .unwrap();
        assert_eq!(reader.read_range(0, data.len() as u64).unwrap(), data);

        let trailer_start = blob.len() - TRAILER_LEN as usize;
        let mut wrong_algorithm = blob.clone();
        wrong_algorithm[trailer_start + 5] = algo_code(CompressionAlgorithm::Lz4);
        assert!(matches!(
            CompressedReader::open_with_dek(
                Cursor::new(wrong_algorithm),
                BlobCipher::LegacyV2(dek.into()),
                &CompressionDescriptor::Uncompressed,
                data.len() as u64,
            ),
            Err(BlobError::Corruption(_))
        ));

        let mut wrong_geometry = blob;
        wrong_geometry[trailer_start + 6..trailer_start + 10]
            .copy_from_slice(&(crate::DEFAULT_ENCRYPTED_BLOCK_SIZE * 2).to_le_bytes());
        assert!(matches!(
            CompressedReader::open_with_dek(
                Cursor::new(wrong_geometry),
                BlobCipher::LegacyV2(dek.into()),
                &CompressionDescriptor::Uncompressed,
                data.len() as u64,
            ),
            Err(BlobError::Corruption(_))
        ));

        // The same binding applies when metadata names an actual compression policy.
        let compressed = encode_encrypted_v2(CompressionAlgorithm::Zstd, 1024, dek, &data);
        let descriptor = CompressionDescriptor::Compressed {
            algorithm: CompressionAlgorithm::Zstd,
            block_size: 1024,
        };
        CompressedReader::open_with_dek(
            Cursor::new(compressed.clone()),
            BlobCipher::LegacyV2(dek.into()),
            &descriptor,
            data.len() as u64,
        )
        .unwrap();
        let trailer_start = compressed.len() - TRAILER_LEN as usize;
        let mut wrong_algorithm = compressed.clone();
        wrong_algorithm[trailer_start + 5] = algo_code(CompressionAlgorithm::Lz4);
        assert!(
            CompressedReader::open_with_dek(
                Cursor::new(wrong_algorithm),
                BlobCipher::LegacyV2(dek.into()),
                &descriptor,
                data.len() as u64,
            )
            .is_err()
        );
        let mut wrong_geometry = compressed;
        wrong_geometry[trailer_start + 6..trailer_start + 10]
            .copy_from_slice(&2048u32.to_le_bytes());
        assert!(
            CompressedReader::open_with_dek(
                Cursor::new(wrong_geometry),
                BlobCipher::LegacyV2(dek.into()),
                &descriptor,
                data.len() as u64,
            )
            .is_err()
        );
    }

    /// The file cannot choose its own compatibility parser. A current object whose persisted
    /// descriptor requires v3 must reject otherwise-valid legacy framing, and a legacy descriptor
    /// must not accept a current v3 container.
    #[test]
    fn encrypted_format_must_match_metadata_expectation() {
        let data = vec![b'f'; 2048];
        let dek = [0x53u8; 32];
        let v3 = encode_encrypted(CompressionAlgorithm::Zstd, 1024, dek, &data);
        let v2 = encode_encrypted_v2(CompressionAlgorithm::Zstd, 1024, dek, &data);

        assert!(matches!(
            CompressedReader::open_with_dek(
                Cursor::new(v2),
                BlobCipher::AuthenticatedV3(dek.into()),
                &CompressionDescriptor::Compressed {
                    algorithm: CompressionAlgorithm::Zstd,
                    block_size: 1024,
                },
                data.len() as u64,
            ),
            Err(BlobError::Corruption(_))
        ));
        assert!(matches!(
            CompressedReader::open_with_dek(
                Cursor::new(v3),
                BlobCipher::LegacyV2(dek.into()),
                &CompressionDescriptor::Compressed {
                    algorithm: CompressionAlgorithm::Zstd,
                    block_size: 1024,
                },
                data.len() as u64,
            ),
            Err(BlobError::Corruption(_))
        ));
    }

    /// Each encrypted block carries a 16-byte GCM tag, and v3 adds one 32-byte metadata tag before
    /// the fixed trailer.
    #[test]
    fn encrypted_trailer_marks_version_and_tag_overhead() {
        let data = vec![b'a'; 3000]; // 3 blocks at block_size 1024 (1024,1024,952).
        let dek = [9u8; 32];
        let blob = encode_encrypted(CompressionAlgorithm::Zstd, 1024, dek, &data);
        // The version byte sits at offset 4 of the 34-byte trailer at the end of the file.
        let trailer = &blob[blob.len() - TRAILER_LEN as usize..];
        assert_eq!(&trailer[0..4], MAGIC);
        assert_eq!(trailer[4], VERSION_ENCRYPTED);
        let index_offset = u64::from_le_bytes(trailer[22..30].try_into().unwrap()) as usize;
        let index_len = u32::from_le_bytes(trailer[30..34].try_into().unwrap()) as usize;
        assert_eq!(
            blob.len() - TRAILER_LEN as usize - (index_offset + index_len),
            METADATA_TAG_LEN
        );
        // Opening without a DEK fails fast because the blob is flagged encrypted.
        let opened = CompressedReader::open_with_dek(
            Cursor::new(blob),
            BlobCipher::KnownPlaintext,
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Zstd,
                block_size: 1024,
            },
            data.len() as u64,
        );
        assert!(matches!(opened, Err(BlobError::Corruption(_))));
    }

    /// Reading an encrypted blob with the wrong DEK fails authentication rather than returning
    /// plaintext or garbage.
    #[test]
    fn wrong_dek_fails_to_decrypt() {
        let data: Vec<u8> = (0..4096u32).map(|i| (i % 97) as u8).collect();
        let dek = [1u8; 32];
        let wrong = [2u8; 32];
        let blob = encode_encrypted(CompressionAlgorithm::Lz4, 1024, dek, &data);
        let result = CompressedReader::open_with_dek(
            Cursor::new(blob),
            BlobCipher::AuthenticatedV3(wrong.into()),
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Lz4,
                block_size: 1024,
            },
            data.len() as u64,
        )
        .and_then(|mut reader| reader.read_range(0, 4096));
        assert!(matches!(result, Err(BlobError::Corruption(_))));
    }

    /// An unencrypted (version 1) blob still reads with the explicit plaintext declaration, while an
    /// encrypted declaration fails closed instead of silently ignoring its format expectation.
    #[test]
    fn old_plain_blob_reads_unchanged() {
        let data: Vec<u8> = (0..2048u32).map(|i| (i % 211) as u8).collect();
        let blob = encode(CompressionAlgorithm::Zstd, 512, &data);
        // The version byte is the plaintext version.
        let trailer = &blob[blob.len() - TRAILER_LEN as usize..];
        assert_eq!(trailer[4], VERSION_PLAIN);
        let mut r = CompressedReader::open_with_dek(
            Cursor::new(blob.clone()),
            BlobCipher::KnownPlaintext,
            &CompressionDescriptor::Compressed {
                algorithm: CompressionAlgorithm::Zstd,
                block_size: 512,
            },
            data.len() as u64,
        )
        .unwrap();
        assert_eq!(r.read_range(0, 2048).unwrap(), data);
        assert!(
            CompressedReader::open_with_dek(
                Cursor::new(blob),
                BlobCipher::AuthenticatedV3([7u8; 32].into()),
                &CompressionDescriptor::Compressed {
                    algorithm: CompressionAlgorithm::Zstd,
                    block_size: 512,
                },
                data.len() as u64,
            )
            .is_err()
        );
    }

    /// A corrupt plaintext index must not turn one 64-KiB logical block into an allocation sized
    /// from a much larger physical file. Reject it while opening, before any block payload read.
    #[test]
    fn plaintext_oversized_physical_block_is_rejected_before_read() {
        let logical = 64 * 1024u32;
        let physical = 8 * 1024 * 1024u32;
        for compressed in [false, true] {
            let mut blob = vec![0; physical as usize];
            blob.extend_from_slice(&physical.to_le_bytes());
            blob.extend_from_slice(&logical.to_le_bytes());
            blob.push(u8::from(compressed));
            blob.extend_from_slice(&trailer(
                VERSION_PLAIN,
                algo_code(CompressionAlgorithm::Lz4),
                logical,
                u64::from(logical),
                1,
                u64::from(physical),
                INDEX_ENTRY_LEN as u32,
            ));
            let result = CompressedReader::open_with_dek(
                Cursor::new(blob),
                BlobCipher::KnownPlaintext,
                &CompressionDescriptor::Compressed {
                    algorithm: CompressionAlgorithm::Lz4,
                    block_size: logical,
                },
                u64::from(logical),
            );
            assert!(matches!(result, Err(BlobError::Corruption(_))));
        }
    }

    /// Equal block counts do not prove equal geometry: one forged block can be much larger than
    /// the caller's trusted block allowance while passing the pre-allocation index count check.
    #[test]
    fn plaintext_block_geometry_must_match_trusted_metadata() {
        let data = vec![1; 128 * 1024];
        let blob = encode(CompressionAlgorithm::Lz4, 128 * 1024, &data);
        for (block_size, logical_len, algorithm) in [
            (64 * 1024, 64 * 1024, CompressionAlgorithm::Lz4),
            (256 * 1024, 128 * 1024, CompressionAlgorithm::Lz4),
            (128 * 1024, 128 * 1024, CompressionAlgorithm::Zstd),
        ] {
            let result = CompressedReader::open_with_dek(
                Cursor::new(blob.clone()),
                BlobCipher::KnownPlaintext,
                &CompressionDescriptor::Compressed {
                    algorithm,
                    block_size,
                },
                logical_len,
            );
            assert!(matches!(result, Err(BlobError::Corruption(_))));
        }
    }

    /// The per-block nonce is deterministic in `(dek, block_index)` and distinct across blocks, so
    /// GCM's nonce-uniqueness requirement holds without storing nonces on disk.
    #[test]
    fn block_nonce_is_deterministic_and_distinct() {
        let dek: [u8; 32] = Aes256Gcm::generate_key(&mut aes_gcm::aead::OsRng).into();
        let mut other_dek = dek;
        other_dek[0] ^= 1;
        assert_eq!(block_nonce(&dek, 0), block_nonce(&dek, 0));
        assert_ne!(block_nonce(&dek, 0), block_nonce(&dek, 1));
        // A different key yields a different nonce for the same block index.
        assert_ne!(block_nonce(&dek, 0), block_nonce(&other_dek, 0));
    }

    #[test]
    fn encoded_length_boundaries_use_reader_geometry_without_allocating_payload() {
        for block in [1024_u32, 64 * 1024, 256 * 1024] {
            let limit = (MAX_INDEX_LEN / INDEX_ENTRY_LEN) as u64 * u64::from(block);
            assert!(validate_encoded_len(limit, block).is_ok());
            assert!(validate_encoded_len(limit - 1, block).is_ok());
            assert!(matches!(
                validate_encoded_len(limit + 1, block),
                Err(BlobError::SizeExceeded)
            ));
        }
        assert!(validate_encoded_len(0, 1024).is_ok());
        for (len, block) in [(0, 0), (1, u32::MAX), (u64::MAX, 1024)] {
            assert!(matches!(
                validate_encoded_len(len, block),
                Err(BlobError::SizeExceeded)
            ));
        }
        assert!(index_len_for_blocks(usize::MAX, MAX_INDEX_LEN).is_none());
    }

    #[test]
    fn encoder_rejects_extra_block_before_copy_and_cannot_finalize_a_prefix() {
        for chunk_size in [1, 1023, 1024, 3072] {
            let mut enc = BlockEncoder::new(CompressionAlgorithm::Zstd, 1024);
            enc.index_limit = 3 * INDEX_ENTRY_LEN;
            let mut out = Vec::new();
            let data = vec![42; 3072];
            for chunk in data.chunks(chunk_size) {
                out.extend(enc.feed(chunk).unwrap());
            }
            assert_eq!(enc.logical_len, 3072);
            assert_eq!(enc.index.len(), 3 * INDEX_ENTRY_LEN);
            assert!(enc.buf.is_empty());
            assert!(matches!(enc.feed(&[42]), Err(BlobError::SizeExceeded)));
            assert_eq!(enc.logical_len, 3072);
            assert_eq!(enc.index.len(), 3 * INDEX_ENTRY_LEN);
            assert!(enc.buf.is_empty());
            assert!(matches!(enc.finish(), Err(BlobError::SizeExceeded)));
        }
    }

    #[test]
    fn bounded_encoder_boundary_and_partial_final_block_roundtrip() {
        for len in [2049, 3072] {
            for encrypted in [false, true] {
                let dek = Aes256Gcm::generate_key(&mut aes_gcm::aead::OsRng);
                let cipher = if encrypted {
                    BlobCipher::AuthenticatedV3(SecretKey32::from_slice(&dek).unwrap())
                } else {
                    BlobCipher::KnownPlaintext
                };
                let mut enc = match cipher.dek() {
                    Some(key) => BlockEncoder::new_encrypted(CompressionAlgorithm::Zstd, 1024, key),
                    None => BlockEncoder::new(CompressionAlgorithm::Zstd, 1024),
                };
                enc.index_limit = 3 * INDEX_ENTRY_LEN;
                let data = vec![42; len];
                let mut out = enc.feed(&data).unwrap();
                out.extend(enc.finish().unwrap());
                let mut reader = CompressedReader::open_with_dek(
                    std::io::Cursor::new(out),
                    cipher,
                    &CompressionDescriptor::Compressed {
                        algorithm: CompressionAlgorithm::Zstd,
                        block_size: 1024,
                    },
                    len as u64,
                )
                .unwrap();
                assert_eq!(reader.read_range(0, len as u64).unwrap(), data);
            }
        }
    }

    #[test]
    fn encoder_checks_length_addition_and_finish_geometry() {
        let mut enc = BlockEncoder::new(CompressionAlgorithm::Zstd, 1024);
        enc.logical_len = u64::MAX;
        assert!(matches!(enc.feed(&[1]), Err(BlobError::SizeExceeded)));
        assert!(enc.buf.is_empty());
        assert!(enc.index.is_empty());
        assert!(matches!(enc.finish(), Err(BlobError::SizeExceeded)));
        assert!(matches!(
            BlockEncoder::new(CompressionAlgorithm::Zstd, 0).finish(),
            Err(BlobError::SizeExceeded)
        ));
    }
    /// A small reference serializer deliberately keeps the pre-spool wire algorithm: complete
    /// index, then one-shot MAC over index + trailer. Chunk draining must be byte-identical.
    #[test]
    fn drained_encoder_preserves_reference_wire_bytes() {
        for algo in [
            CompressionAlgorithm::None,
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Lz4,
        ] {
            for encrypted in [false, true] {
                let key = Aes256Gcm::generate_key(&mut aes_gcm::aead::OsRng);
                let key: [u8; 32] = key.into();
                for len in [0usize, 1024, 3073] {
                    let data: Vec<u8> = (0..len).map(|n| (n % 251) as u8).collect();
                    let mut expected = Vec::new();
                    let mut index = Vec::new();
                    for (number, logical) in data.chunks(1024).enumerate() {
                        let (mut physical, compressed) = compress_block(algo, logical);
                        if encrypted {
                            physical = encrypt_block(&key, number as u64, &physical).unwrap();
                        }
                        index.extend_from_slice(&(physical.len() as u32).to_le_bytes());
                        index.extend_from_slice(&(logical.len() as u32).to_le_bytes());
                        index.push(u8::from(compressed));
                        expected.extend(physical);
                    }
                    let mut trailer = Vec::from(*MAGIC);
                    trailer.extend([if encrypted { 3 } else { 1 }, algo_code(algo)]);
                    trailer.extend(1024u32.to_le_bytes());
                    trailer.extend((len as u64).to_le_bytes());
                    trailer.extend((len.div_ceil(1024) as u32).to_le_bytes());
                    trailer.extend((expected.len() as u64).to_le_bytes());
                    trailer.extend((index.len() as u32).to_le_bytes());
                    expected.extend(&index);
                    if encrypted {
                        expected.extend(metadata_tag(&key, &index, &trailer));
                    }
                    expected.extend(trailer);

                    for chunk_size in [1, 1023, 1024, 2049] {
                        let mut encoder =
                            BlockEncoder::with_dek(algo, 1024, encrypted.then(|| key.into()));
                        let mut actual = Vec::new();
                        let mut spooled = Vec::new();
                        for chunk in data.chunks(chunk_size) {
                            actual.extend(encoder.feed(chunk).unwrap());
                            spooled.extend(encoder.take_index());
                            assert!(encoder.index.is_empty());
                            assert!(encoder.buf.len() < 1024);
                        }
                        let tail = encoder.finish_parts().unwrap();
                        actual.extend(tail.payload);
                        spooled.extend(tail.index);
                        actual.extend(spooled);
                        actual.extend(tail.footer);
                        assert_eq!(actual, expected);
                    }
                }
            }
        }
    }

    /// Small block geometry keeps three real index pages below a MiB of logical fixture data.
    /// Alternate compressible and incompressible pages so swapping them changes their meaning.
    fn paged_fixture(version: u8) -> (Vec<u8>, CompressedReader<Cursor<Vec<u8>>>) {
        let block_size = 32;
        let len = (2 * INDEX_PAGE_ENTRIES + 5) * block_size - 7;
        let mut seed = 0x5eed_u64;
        let data: Vec<u8> = (0..len)
            .map(|i| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                if i / (block_size * INDEX_PAGE_ENTRIES) == 1 {
                    (seed >> 32) as u8
                } else {
                    b'a'
                }
            })
            .collect();
        let key: [u8; 32] = Aes256Gcm::generate_key(&mut aes_gcm::aead::OsRng).into();
        let algo = CompressionAlgorithm::Zstd;
        let (blob, cipher) = match version {
            VERSION_PLAIN => (
                encode(algo, block_size as u32, &data),
                BlobCipher::KnownPlaintext,
            ),
            VERSION_ENCRYPTED_V2 => (
                encode_encrypted_v2(algo, block_size as u32, key, &data),
                BlobCipher::LegacyV2(key.into()),
            ),
            VERSION_ENCRYPTED => (
                encode_encrypted(algo, block_size as u32, key, &data),
                BlobCipher::AuthenticatedV3(key.into()),
            ),
            _ => unreachable!(),
        };
        let reader = CompressedReader::open_with_dek(
            Cursor::new(blob),
            cipher,
            &CompressionDescriptor::Compressed {
                algorithm: algo,
                block_size: block_size as u32,
            },
            data.len() as u64,
        )
        .unwrap();
        (data, reader)
    }

    #[test]
    fn verified_pages_roundtrip_boundaries_and_bound_retained_index_memory() {
        assert_eq!(INDEX_PAGE_BYTES, 65_529);
        let max_blocks = MAX_INDEX_LEN / INDEX_ENTRY_LEN;
        assert!(index_memory_bound(max_blocks) < 192 * 1024);
        for version in [VERSION_PLAIN, VERSION_ENCRYPTED_V2, VERSION_ENCRYPTED] {
            let (data, mut reader) = paged_fixture(version);
            let page_logical = INDEX_PAGE_ENTRIES as u64 * reader.block_size();
            assert_eq!(reader.pages.len(), 3);
            assert_eq!(reader.index_len % INDEX_PAGE_BYTES, 5 * INDEX_ENTRY_LEN);
            for (offset, len) in [
                (0, 1),
                (page_logical - 9, 19),
                (2 * page_logical - 3, 11),
                (data.len() as u64 - 17, 100),
                (0, data.len() as u64),
            ] {
                let end = (offset + len).min(data.len() as u64);
                assert_eq!(
                    reader.read_range(offset, len).unwrap(),
                    &data[offset as usize..end as usize]
                );
                let retained = reader.pages.capacity() * std::mem::size_of::<IndexPageSummary>()
                    + reader.page_bytes.capacity()
                    + reader.page_offsets.capacity() * std::mem::size_of::<u64>();
                assert!(
                    retained as u64 <= index_memory_bound(data.len().div_ceil(32)),
                    "reader retained more than its advertised index bound"
                );
            }
        }
    }

    #[test]
    fn changed_index_page_is_rejected_before_its_entries_are_interpreted() {
        for version in [VERSION_PLAIN, VERSION_ENCRYPTED_V2, VERSION_ENCRYPTED] {
            let (_, mut reader) = paged_fixture(version);
            let offset = reader.index_offset as usize + INDEX_PAGE_BYTES;
            reader.inner.get_mut()[offset + 8] = 2; // invalid flag, never reached by entry parsing
            let error = reader
                .read_range(INDEX_PAGE_ENTRIES as u64 * 32, 1)
                .unwrap_err();
            assert!(matches!(error, BlobError::Corruption(ref message)
                if message.contains("index page changed")));
            assert!(reader.cached_page.is_none());
        }
    }

    #[test]
    fn swapped_pages_cannot_reuse_another_pages_fingerprint_or_physical_offset() {
        for version in [VERSION_PLAIN, VERSION_ENCRYPTED_V2, VERSION_ENCRYPTED] {
            let (_, mut reader) = paged_fixture(version);
            assert_ne!(reader.pages[0].fingerprint, reader.pages[1].fingerprint);
            let start = reader.index_offset as usize;
            let bytes = reader.inner.get_mut();
            let first = bytes[start..start + INDEX_PAGE_BYTES].to_vec();
            bytes.copy_within(
                start + INDEX_PAGE_BYTES..start + 2 * INDEX_PAGE_BYTES,
                start,
            );
            bytes[start + INDEX_PAGE_BYTES..start + 2 * INDEX_PAGE_BYTES].copy_from_slice(&first);
            assert!(
                matches!(reader.read_range(0, 1), Err(BlobError::Corruption(ref message))
                if message.contains("index page changed"))
            );
        }
    }

    #[test]
    fn evicted_page_is_verified_again_and_failed_load_does_not_poison_other_pages() {
        let (data, mut reader) = paged_fixture(VERSION_ENCRYPTED);
        let second_page = INDEX_PAGE_ENTRIES as u64 * 32;
        assert_eq!(reader.read_range(0, 1).unwrap(), &data[..1]);
        assert_eq!(
            reader.read_range(second_page, 1).unwrap(),
            &data[second_page as usize..][..1]
        );
        let first = reader.index_offset as usize;
        reader.inner.get_mut()[first] ^= 1;
        assert!(matches!(
            reader.read_range(0, 1),
            Err(BlobError::Corruption(_))
        ));
        assert!(reader.cached_page.is_none());
        assert_eq!(
            reader.read_range(second_page, 1).unwrap(),
            &data[second_page as usize..][..1]
        );
    }

    #[test]
    fn final_partial_page_is_covered_by_structural_validation_and_v3_authentication() {
        let (_, reader) = paged_fixture(VERSION_ENCRYPTED);
        let logical_len = reader.logical_len;
        let key = reader.dek.unwrap();
        let compression = CompressionDescriptor::Compressed {
            algorithm: CompressionAlgorithm::Zstd,
            block_size: 32,
        };
        let last_page = reader.index_offset as usize + 2 * INDEX_PAGE_BYTES;
        let original = reader.inner.into_inner();
        for preserve_total in [false, true] {
            let mut bytes = original.clone();
            let first = u32::from_le_bytes(bytes[last_page..last_page + 4].try_into().unwrap());
            bytes[last_page..last_page + 4].copy_from_slice(&(first - 1).to_le_bytes());
            if preserve_total {
                // Both compressed payload lengths remain structurally valid and their total is
                // unchanged. Only whole-index authentication can reject this offset alteration.
                let next = last_page + INDEX_ENTRY_LEN;
                let length = u32::from_le_bytes(bytes[next..next + 4].try_into().unwrap());
                bytes[next..next + 4].copy_from_slice(&(length + 1).to_le_bytes());
            }
            let result = CompressedReader::open_with_dek(
                Cursor::new(bytes),
                BlobCipher::AuthenticatedV3(key.clone()),
                &compression,
                logical_len,
            );
            let expected = if preserve_total {
                "metadata authentication failed"
            } else {
                "physical lengths"
            };
            assert!(
                matches!(result, Err(BlobError::Corruption(ref message)) if message.contains(expected))
            );
        }
    }

    #[test]
    fn page_reads_use_validated_geometry_without_reloading_mutated_trailer() {
        let (data, mut reader) = paged_fixture(VERSION_ENCRYPTED);
        let trailer = reader.inner.get_ref().len() - TRAILER_BYTES;
        reader.inner.get_mut()[trailer..].fill(0);
        let offset = data.len() as u64 - 7;
        assert_eq!(
            reader.read_range(offset, 7).unwrap(),
            &data[offset as usize..]
        );
    }

    #[test]
    fn backend_container_read_bound_retains_format_limits_without_per_block_heap_growth() {
        let compression = CompressionDescriptor::Compressed {
            algorithm: CompressionAlgorithm::Zstd,
            block_size: 1024,
        };
        let maximum = (MAX_INDEX_LEN / INDEX_ENTRY_LEN) as u64 * 1024;
        let bound = read_memory_bound(&compression, maximum).unwrap();
        assert!(bound.buffer_bytes < 2 * 1024 * 1024);
        assert_eq!(bound.max_frame_bytes, 1024);
        for invalid in [maximum + 1, u64::MAX] {
            assert!(matches!(
                read_memory_bound(&compression, invalid),
                Err(BlobError::Corruption(_))
            ));
        }
        for block_size in [0, u32::MAX] {
            assert!(
                read_memory_bound(
                    &CompressionDescriptor::Compressed {
                        algorithm: CompressionAlgorithm::Zstd,
                        block_size,
                    },
                    0
                )
                .is_err()
            );
        }
    }

    #[test]
    fn bulk_codec_workspace_fits_the_backend_read_allowance() {
        // zstd::bulk uses this same safe DCtx decompression operation. Its native context is
        // outside Rust Vec capacity accounting, so pin its allowance across dependency updates.
        for len in [1024, 256 * 1024, MAX_BLOCK_SIZE as usize] {
            let data = vec![42; len];
            let encoded = zstd::bulk::compress(&data, 3).unwrap();
            let mut context = zstd::zstd_safe::DCtx::create();
            let mut decoded = Vec::with_capacity(len);
            context.decompress(&mut decoded, &encoded).unwrap();
            assert_eq!(decoded.len(), len);
            assert!(decoded.iter().all(|&byte| byte == 42));
            assert!(context.sizeof() as u64 + 128 * 1024 < READER_FIXED_BYTES);
        }
    }
}
