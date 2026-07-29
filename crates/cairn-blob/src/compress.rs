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
use cairn_types::bucket::CompressionAlgorithm;
use cairn_types::error::BlobError;
use hmac::{Hmac, Mac};
use sha2::Sha256;
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
/// The AES-GCM nonce length (96 bits — the recommended GCM nonce size).
const GCM_NONCE_LEN: usize = 12;

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
fn metadata_tag(dek: &[u8; 32], index: &[u8], trailer: &[u8]) -> [u8; METADATA_TAG_LEN] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(dek).expect("HMAC accepts any key length");
    mac.update(METADATA_MAC_DOMAIN);
    mac.update(index);
    mac.update(trailer);
    mac.finalize().into_bytes().into()
}

fn verify_metadata_tag(
    dek: &[u8; 32],
    index: &[u8],
    trailer: &[u8],
    tag: &[u8],
) -> Result<(), BlobError> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(dek).expect("HMAC accepts any key length");
    mac.update(METADATA_MAC_DOMAIN);
    mac.update(index);
    mac.update(trailer);
    mac.verify_slice(tag)
        .map_err(|_| BlobError::Corruption("encrypted blob metadata authentication failed".into()))
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
/// block plus its compressed form is buffered. When constructed with a DEK
/// ([`new_encrypted`](BlockEncoder::new_encrypted)), each block is AES-256-GCM-encrypted after
/// compression and the trailer records [`VERSION_ENCRYPTED`].
pub struct BlockEncoder {
    algo: CompressionAlgorithm,
    block_size: usize,
    buf: Vec<u8>,
    index: Vec<IndexEntry>,
    logical_len: u64,
    phys_len: u64,
    /// The raw 32-byte DEK when this is an SSE-S3 (encrypted) encoder; `None` stores plaintext.
    dek: Option<SecretKey32>,
    /// The next block index to emit (drives the deterministic per-block nonce).
    block_index: u64,
    /// Set if a block encryption failed; surfaced from [`finish`](BlockEncoder::finish).
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
            block_size: block_size.max(1) as usize,
            buf: Vec::new(),
            index: Vec::new(),
            logical_len: 0,
            phys_len: 0,
            dek,
            block_index: 0,
            error: None,
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
        let (mut phys, compressed) = compress_block(self.algo, logical);
        if let Some(dek) = self.dek.as_ref() {
            match encrypt_block(dek.expose_secret(), self.block_index, &phys) {
                Ok(ciphertext) => phys = ciphertext,
                // Record the first failure; `finish` turns it into an `Err`. We cannot return an
                // error from `feed` without changing the streaming signature, and an encryption
                // failure here is effectively unreachable (AES-GCM only fails on absurd sizes).
                Err(e) => {
                    self.error.get_or_insert(e);
                }
            }
        }
        self.index.push(IndexEntry {
            phys_len: phys.len() as u32,
            logical_len: logical.len() as u32,
            compressed,
        });
        self.phys_len += phys.len() as u64;
        self.block_index += 1;
        out.extend_from_slice(&phys);
    }

    /// Flush the final partial block and append the index and trailer; returns those bytes, or an
    /// error if any block failed to encrypt.
    ///
    /// # Errors
    /// Returns [`BlobError::Corruption`] if a block's AES-256-GCM encryption failed (practically
    /// unreachable; GCM only rejects inputs larger than the format ever produces).
    pub fn finish(mut self) -> Result<Vec<u8>, BlobError> {
        let mut out = Vec::new();
        if !self.buf.is_empty() {
            let block = std::mem::take(&mut self.buf);
            self.emit_block(&block, &mut out);
        }
        if let Some(e) = self.error.take() {
            return Err(e);
        }
        let index_offset = self.phys_len;
        let mut index_bytes = Vec::with_capacity(self.index.len() * INDEX_ENTRY_LEN);
        for e in &self.index {
            index_bytes.extend_from_slice(&e.phys_len.to_le_bytes());
            index_bytes.extend_from_slice(&e.logical_len.to_le_bytes());
            index_bytes.push(u8::from(e.compressed));
        }
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
        trailer.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        trailer.extend_from_slice(&index_offset.to_le_bytes());
        trailer.extend_from_slice(&(index_bytes.len() as u32).to_le_bytes());

        out.extend_from_slice(&index_bytes);
        if let Some(dek) = self.dek.as_ref() {
            out.extend_from_slice(&metadata_tag(dek.expose_secret(), &index_bytes, &trailer));
        }
        out.extend_from_slice(&trailer);
        Ok(out)
    }
}

/// The length of the CRNB trailer, exported so a caller can read exactly the bytes
/// [`is_encrypted_container_trailer`] needs.
pub const TRAILER_BYTES: usize = TRAILER_LEN as usize;

/// Whether `trailer` (the last [`TRAILER_BYTES`] bytes of a `total`-byte file) is a **fully
/// self-consistent** CRNB trailer marking an *encrypted* container.
///
/// This is deliberately NOT the trailer sniffing audit #18 removed: framing is still decided from
/// the caller's stored descriptor, and this predicate is only ever used to **refuse** a read — never
/// to parse a blob as a container. Every field must agree (magic, the encrypted version byte, the
/// `index_len == block_count * INDEX_ENTRY_LEN` identity, and the
/// `index_offset + index_len + TRAILER_LEN == total` layout identity), so a plaintext blob whose
/// bytes merely end in `CRNB` is not matched: the odds of a plaintext blob satisfying all four are
/// negligible, and the consequence of one is a single refused read, not a misread body.
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
    if index_len != block_count * INDEX_ENTRY_LEN as u64 {
        return false;
    }
    index_offset
        .checked_add(index_len)
        .and_then(|n| n.checked_add(metadata_tag_len))
        .and_then(|n| n.checked_add(TRAILER_LEN))
        == Some(total)
}

/// A random-access reader over a compressed (and optionally SSE-S3-encrypted) blob file.
pub struct CompressedReader<R: Read + Seek> {
    inner: R,
    algo: CompressionAlgorithm,
    block_size: u64,
    logical_len: u64,
    block_offsets: Vec<u64>,
    index: Vec<IndexEntry>,
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
    /// framing. A wrong version, absent key, or bad key fails closed before object bytes are returned.
    pub fn open_with_dek(mut inner: R, cipher: BlobCipher) -> Result<Self, BlobError> {
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
        let expected_index_len = block_count
            .checked_mul(INDEX_ENTRY_LEN)
            .ok_or_else(|| BlobError::Corruption("block count overflows index length".into()))?;
        if index_len != expected_index_len {
            return Err(BlobError::Corruption("index length mismatch".into()));
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
        inner.seek(SeekFrom::Start(index_offset)).map_err(io)?;
        let mut idx = vec![0u8; index_len];
        inner.read_exact(&mut idx).map_err(io)?;

        // V3 authenticates every byte whose semantics the reader will trust. Verification occurs
        // before parsing the algorithm, logical geometry, or index entries. The small location
        // fields used above are treated only as bounded offsets until this succeeds.
        if authenticated_metadata {
            let mut tag = [0u8; METADATA_TAG_LEN];
            inner.seek(SeekFrom::Start(index_end)).map_err(io)?;
            inner.read_exact(&mut tag).map_err(io)?;
            let dek = dek.as_ref().expect("encrypted formats require a DEK");
            verify_metadata_tag(dek.expose_secret(), &idx, &t, &tag)?;
        }

        let algo = algo_from(t[5])?;
        let block_size = u32::from_le_bytes(t[6..10].try_into().unwrap()) as u64;
        let logical_len = u64::from_le_bytes(t[10..18].try_into().unwrap());
        let mut index = Vec::with_capacity(block_count);
        let mut block_offsets = Vec::with_capacity(block_count);
        let mut offset = 0u64;
        for chunk in idx.chunks_exact(INDEX_ENTRY_LEN) {
            let phys_len = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
            let logical = u32::from_le_bytes(chunk[4..8].try_into().unwrap());
            let compressed = match chunk[8] {
                0 => false,
                1 => true,
                other => {
                    return Err(BlobError::Corruption(format!(
                        "invalid compressed flag {other}"
                    )));
                }
            };
            block_offsets.push(offset);
            offset = offset
                .checked_add(u64::from(phys_len))
                .ok_or_else(|| BlobError::Corruption("block offset overflows".into()))?;
            index.push(IndexEntry {
                phys_len,
                logical_len: logical,
                compressed,
            });
        }
        // The blocks occupy exactly `[0, index_offset)` on disk, so the per-block physical lengths
        // must sum to where the index begins. Enforcing it rejects an index whose `phys_len` entries
        // point outside the block region AND bounds every `phys_len` by the file size — so a read
        // can never be asked to allocate a multi-gigabyte `phys` buffer for a corrupt block (OOM
        // guard; `phys_len` is a `u32` up to ~4 GiB and is otherwise unbounded).
        if offset != index_offset {
            return Err(BlobError::Corruption(
                "block physical lengths do not fill the block region".into(),
            ));
        }
        // Cross-validate the trailer's `logical_len` against the index BEFORE serving reads:
        // `read_range` maps a logical offset to a block via `logical_len`/`block_size`, so a trailer
        // claiming a logical length the index does not actually cover (e.g. `logical_len > 0` with
        // `block_count == 0`) would index past `self.index` and panic. A non-empty blob must have a
        // positive block size, exactly `ceil(logical_len / block_size)` blocks, and per-block logical
        // lengths that sum to `logical_len`; an empty blob must have no blocks. Reject any mismatch as
        // corruption so every value the reader trusts on read is established here.
        if logical_len == 0 {
            if block_count != 0 {
                return Err(BlobError::Corruption(
                    "empty blob with a non-zero block count".into(),
                ));
            }
        } else {
            if block_size == 0 {
                return Err(BlobError::Corruption(
                    "non-empty blob with a zero block size".into(),
                ));
            }
            // Cap the block size: the read path calls `read_range` (and `zstd`/`lz4` decompression)
            // with a length bounded by ONE block, so a corrupt trailer claiming a multi-gigabyte
            // block size would make the *server* allocate that per read. The writer uses ≤256 KiB;
            // this cap is generously above that and bounds every per-block allocation.
            if block_size > MAX_BLOCK_SIZE {
                return Err(BlobError::Corruption(
                    "block size exceeds the maximum".into(),
                ));
            }
            if logical_len.div_ceil(block_size) != block_count as u64 {
                return Err(BlobError::Corruption(
                    "block count does not cover the logical length".into(),
                ));
            }
        }
        let index_logical_sum: u64 = index.iter().map(|e| u64::from(e.logical_len)).sum();
        if index_logical_sum != logical_len {
            return Err(BlobError::Corruption(
                "index logical lengths do not sum to the logical length".into(),
            ));
        }
        // All non-final blocks are exactly `block_size`; only the final block may be shorter. This
        // is both the writer's framing invariant and a legacy-v2 hardening check: an unauthenticated
        // trailer cannot alter block geometry while still mapping ranges to different boundaries.
        for (position, entry) in index.iter().enumerate() {
            let expected = if position + 1 < block_count {
                block_size
            } else {
                let preceding = (block_count as u64)
                    .saturating_sub(1)
                    .checked_mul(block_size)
                    .ok_or_else(|| {
                        BlobError::Corruption("logical block geometry overflows".into())
                    })?;
                logical_len.checked_sub(preceding).ok_or_else(|| {
                    BlobError::Corruption("logical block geometry underflows".into())
                })?
            };
            if u64::from(entry.logical_len) != expected {
                return Err(BlobError::Corruption(
                    "index block length does not match the fixed block geometry".into(),
                ));
            }
        }
        Ok(Self {
            inner,
            algo,
            block_size,
            logical_len,
            block_offsets,
            index,
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
        let mut out = enc.feed(data);
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
        let mut out = enc.feed(data);
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
        let err = CompressedReader::open_with_dek(Cursor::new(blob), BlobCipher::KnownPlaintext);
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
        let err = CompressedReader::open_with_dek(Cursor::new(blob), BlobCipher::KnownPlaintext);
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
            CompressedReader::open_with_dek(Cursor::new(blob), BlobCipher::KnownPlaintext).is_err(),
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
            CompressedReader::open_with_dek(Cursor::new(blob), BlobCipher::KnownPlaintext).is_err(),
            "a block phys_len that overruns the block region must be rejected"
        );
    }

    /// An empty blob with a non-zero block count, and a too-short file, are both rejected cleanly.
    #[test]
    fn open_rejects_inconsistent_empty_and_short() {
        let blob = trailer(VERSION_PLAIN, 1, 4096, 0, 3, 0, 27);
        assert!(
            CompressedReader::open_with_dek(Cursor::new(blob), BlobCipher::KnownPlaintext).is_err()
        );
        assert!(
            CompressedReader::open_with_dek(
                Cursor::new(vec![0u8; 10]),
                BlobCipher::KnownPlaintext,
            )
            .is_err()
        );
    }

    #[test]
    fn roundtrip_full_and_ranges() {
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let blob = encode(CompressionAlgorithm::Zstd, 1024, &data);
        let mut r =
            CompressedReader::open_with_dek(Cursor::new(blob), BlobCipher::KnownPlaintext).unwrap();
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
        let mut r =
            CompressedReader::open_with_dek(Cursor::new(blob), BlobCipher::KnownPlaintext).unwrap();
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
        let mut r =
            CompressedReader::open_with_dek(Cursor::new(blob), BlobCipher::KnownPlaintext).unwrap();
        assert_eq!(r.read_range(0, 10_000).unwrap(), data);
    }

    #[test]
    fn lz4_roundtrip() {
        let data = vec![b'x'; 3000];
        let blob = encode(CompressionAlgorithm::Lz4, 1024, &data);
        let mut r =
            CompressedReader::open_with_dek(Cursor::new(blob), BlobCipher::KnownPlaintext).unwrap();
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
        )
        .and_then(|mut reader| reader.read_range(0, data.len() as u64));
        assert!(matches!(result, Err(BlobError::Corruption(_))));
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
            ),
            Err(BlobError::Corruption(_))
        ));
        assert!(matches!(
            CompressedReader::open_with_dek(Cursor::new(v3), BlobCipher::LegacyV2(dek.into()),),
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
        let opened = CompressedReader::open_with_dek(Cursor::new(blob), BlobCipher::KnownPlaintext);
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
        let mut r =
            CompressedReader::open_with_dek(Cursor::new(blob.clone()), BlobCipher::KnownPlaintext)
                .unwrap();
        assert_eq!(r.read_range(0, 2048).unwrap(), data);
        assert!(
            CompressedReader::open_with_dek(
                Cursor::new(blob),
                BlobCipher::AuthenticatedV3([7u8; 32].into()),
            )
            .is_err()
        );
    }

    /// The per-block nonce is deterministic in `(dek, block_index)` and distinct across blocks, so
    /// GCM's nonce-uniqueness requirement holds without storing nonces on disk.
    #[test]
    fn block_nonce_is_deterministic_and_distinct() {
        let dek = [5u8; 32];
        assert_eq!(block_nonce(&dek, 0), block_nonce(&dek, 0));
        assert_ne!(block_nonce(&dek, 0), block_nonce(&dek, 1));
        // A different key yields a different nonce for the same block index.
        assert_ne!(block_nonce(&dek, 0), block_nonce(&[6u8; 32], 0));
    }
}
