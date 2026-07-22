//! Gate tests for the local blob store: durable round-trips, compression fidelity and ETag
//! invariance, range reads against compressed blobs, the incompressibility heuristic, the size
//! ceiling, multipart assembly, path safety, and bounded reconciliation.

use bytes::Bytes;
use cairn_blob::LocalBlobStore;
use cairn_types::bucket::{CompressionAlgorithm, CompressionPolicy};
use cairn_types::testing::SetReconcileOracle;
use cairn_types::*;

fn body(data: Vec<u8>) -> BodyStream {
    Box::pin(futures_util::stream::once(
        async move { Ok(Bytes::from(data)) },
    ))
}

/// A body delivered in several chunks, to exercise the streaming/compression boundary logic.
fn chunked_body(data: Vec<u8>, chunk: usize) -> BodyStream {
    let chunks: Vec<Bytes> = data
        .chunks(chunk.max(1))
        .map(Bytes::copy_from_slice)
        .collect();
    Box::pin(futures_util::stream::iter(chunks.into_iter().map(Ok)))
}

fn opts(compression: Option<CompressionPolicy>, content_type: &str) -> StageOptions {
    StageOptions {
        compression,
        size_ceiling: 100 * 1024 * 1024,
        content_type: content_type.to_owned(),
        ..StageOptions::default()
    }
}

/// Stage options that compress *and* encrypt under the given DEK (SSE-S3 over the block format).
fn opts_encrypted(
    compression: Option<CompressionPolicy>,
    content_type: &str,
    dek: [u8; 32],
) -> StageOptions {
    StageOptions {
        compression,
        size_ceiling: 100 * 1024 * 1024,
        content_type: content_type.to_owned(),
        encryption: Some(dek),
        ..StageOptions::default()
    }
}

async fn read_all(
    store: &LocalBlobStore,
    path: &StoragePath,
    range: Option<ByteRange>,
    compression: &CompressionDescriptor,
) -> Vec<u8> {
    use futures_util::StreamExt;
    let handle = store.open(path, range, compression).await.unwrap();
    let mut out = Vec::new();
    let mut body = handle.body;
    while let Some(c) = body.next().await {
        out.extend_from_slice(&c.unwrap());
    }
    out
}

#[tokio::test]
async fn uncompressed_roundtrip_and_etag() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let staged = store
        .stage(&b, body(b"hello world".to_vec()), opts(None, "text/plain"))
        .await
        .unwrap();
    assert_eq!(staged.etag.as_str(), "5eb63bbbe01eeed093cb22bb8f5acdc3"); // md5("hello world")
    assert_eq!(staged.size_logical, 11);
    assert!(matches!(
        staged.compression,
        CompressionDescriptor::Uncompressed
    ));
    assert_eq!(
        read_all(&store, &staged.storage_path, None, &staged.compression).await,
        b"hello world"
    );
    // A small uncompressed object is served inline by the small-object fast path (read whole in the
    // single probe open, no second open and no streaming channel), so it exposes NO zero-copy hint —
    // and its body still reads back byte-exact (verified above via `read_all`).
    let handle = store
        .open(&staged.storage_path, None, &staged.compression)
        .await
        .unwrap();
    assert!(
        handle.zero_copy.is_none(),
        "a below-floor object takes the small-object fast path, not the zero-copy path"
    );

    // A larger uncompressed object (above the small-read floor) still exposes the zero-copy hint and
    // reads back byte-exact.
    let big = vec![7u8; 300 * 1024];
    let staged_big = store
        .stage(
            &b,
            body(big.clone()),
            opts(None, "application/octet-stream"),
        )
        .await
        .unwrap();
    assert_eq!(
        read_all(
            &store,
            &staged_big.storage_path,
            None,
            &staged_big.compression
        )
        .await,
        big
    );
    let handle_big = store
        .open(&staged_big.storage_path, None, &staged_big.compression)
        .await
        .unwrap();
    assert!(
        handle_big.zero_copy.is_some(),
        "an above-floor uncompressed object still exposes the zero-copy hint"
    );
}

/// Ranged reads served by the small-object fast path (the `Bytes::slice` of the whole-file buffer)
/// must return exactly the same bytes and `Content-Range` as the streamed path — including the edge
/// cases the slice arithmetic can get wrong: a range flush to EOF, a length clamped past the end, a
/// zero-length range, and a range whose offset sits at the very end.
#[tokio::test]
async fn small_object_fast_path_ranged_reads_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    // A distinct byte pattern so a mis-sliced range would produce visibly wrong bytes.
    let data: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let staged = store
        .stage(
            &b,
            body(data.clone()),
            opts(None, "application/octet-stream"),
        )
        .await
        .unwrap();

    // The whole read is byte-exact and offers no zero-copy hint (fast path, not sendfile).
    let full = store
        .open(&staged.storage_path, None, &staged.compression)
        .await
        .unwrap();
    assert!(full.zero_copy.is_none());
    assert!(full.content_range.is_none());

    // (offset, requested length, expected served length): mid-object, flush-to-EOF, over-long length
    // clamped to the remaining bytes, a single byte at the very end, and a zero-length range.
    let cases = [
        (100u64, 200u64, 200u64),
        (4000, 96, 96),     // exactly to EOF
        (4000, 10_000, 96), // length clamped to what remains
        (4095, 1, 1),       // last byte
        (500, 0, 0),        // empty range
    ];
    for (offset, length, want_len) in cases {
        let range = ByteRange { offset, length };
        let got = read_all(
            &store,
            &staged.storage_path,
            Some(range),
            &staged.compression,
        )
        .await;
        assert_eq!(
            got,
            &data[offset as usize..(offset + want_len) as usize],
            "fast-path range offset={offset} length={length} returned wrong bytes"
        );
        let cr = store
            .open(&staged.storage_path, Some(range), &staged.compression)
            .await
            .unwrap()
            .content_range
            .expect("a ranged read reports a Content-Range");
        assert_eq!(cr.start, offset);
        assert_eq!(cr.total, data.len() as u64);
        // `end` is inclusive and never precedes `start` (an empty range collapses to start..start).
        assert_eq!(cr.end, offset + want_len.saturating_sub(1));
    }
}

#[tokio::test]
async fn uncompressed_blob_ending_in_crnb_magic_is_not_misdetected() {
    // Audit #18: an uncompressed object whose trailing bytes collide with the 34-byte CRNB
    // block-container trailer magic must NOT be misread as a compressed container — the stored
    // descriptor is authoritative, not a trailer sniff.
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    // Plant "CRNB" exactly at offset len-34, where the trailer magic would sit — the worst case.
    let mut data = vec![0u8; 64];
    let pos = data.len() - 34;
    data[pos..pos + 4].copy_from_slice(b"CRNB");
    let staged = store
        .stage(
            &b,
            body(data.clone()),
            opts(None, "application/octet-stream"),
        )
        .await
        .unwrap();
    assert!(matches!(
        staged.compression,
        CompressionDescriptor::Uncompressed
    ));
    assert_eq!(
        read_all(&store, &staged.storage_path, None, &staged.compression).await,
        data,
        "an uncompressed blob colliding with the CRNB trailer must read back intact"
    );
}

#[tokio::test]
async fn preallocated_write_roundtrips_and_size_is_exact() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();

    // A >1 MiB object so the preallocation/fadvise fast path runs (ARCH 7.5). The blob must
    // round-trip byte-identically and report its exact size — preallocation must never pad it.
    let data: Vec<u8> = (0..2 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let opts = StageOptions {
        size_ceiling: 100 * 1024 * 1024,
        content_length: Some(data.len() as u64),
        ..StageOptions::default()
    };
    let staged = store
        .stage(&b, chunked_body(data.clone(), 64 * 1024), opts)
        .await
        .unwrap();
    assert_eq!(
        staged.size_logical,
        data.len() as u64,
        "preallocation must not change the logical size"
    );
    assert_eq!(
        read_all(&store, &staged.storage_path, None, &staged.compression).await,
        data,
        "byte-identical round-trip through the preallocated path"
    );

    // KEEP_SIZE safety: an OVER-declared content length (e.g. a client that sends a larger
    // Content-Length than the body) must leave the stored blob exactly the bytes written — the
    // reserved-but-unused blocks must not appear as zero padding.
    let short = b"short body".to_vec();
    let opts2 = StageOptions {
        size_ceiling: 100 * 1024 * 1024,
        content_length: Some(8 * 1024 * 1024),
        ..StageOptions::default()
    };
    let staged2 = store.stage(&b, body(short.clone()), opts2).await.unwrap();
    assert_eq!(
        staged2.size_logical,
        short.len() as u64,
        "an over-declared content length must not pad the blob"
    );
    assert_eq!(
        read_all(&store, &staged2.storage_path, None, &staged2.compression).await,
        short
    );
}

#[tokio::test]
async fn compression_is_transparent_and_etag_invariant() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let data: Vec<u8> = b"the quick brown fox "
        .iter()
        .copied()
        .cycle()
        .take(10_000)
        .collect();
    let policy = CompressionPolicy {
        algorithm: CompressionAlgorithm::Zstd,
        block_size: 1024,
    };

    let plain = store
        .stage(&b, body(data.clone()), opts(None, "text/plain"))
        .await
        .unwrap();
    let comp = store
        .stage(
            &b,
            chunked_body(data.clone(), 333),
            opts(Some(policy), "text/plain"),
        )
        .await
        .unwrap();

    // ETag is the plaintext MD5 either way (ARCH 10.2).
    assert_eq!(plain.etag.as_str(), comp.etag.as_str());
    assert!(matches!(
        comp.compression,
        CompressionDescriptor::Compressed { .. }
    ));
    // Compressible data shrinks on disk.
    assert!(comp.size_physical < comp.size_logical);
    assert_eq!(comp.size_logical, data.len() as u64);

    // Full read is transparent.
    assert_eq!(
        read_all(&store, &comp.storage_path, None, &comp.compression).await,
        data
    );
    // A range starting mid-block near the end (the case block compression exists for).
    let range = ByteRange {
        offset: 5000,
        length: 1234,
    };
    assert_eq!(
        read_all(&store, &comp.storage_path, Some(range), &comp.compression).await,
        &data[5000..6234]
    );
    // A compressed read offers no zero-copy hint.
    assert!(
        store
            .open(&comp.storage_path, Some(range), &comp.compression)
            .await
            .unwrap()
            .zero_copy
            .is_none()
    );
}

async fn read_all_dek(
    store: &LocalBlobStore,
    path: &StoragePath,
    range: Option<ByteRange>,
    dek: Option<[u8; 32]>,
    compression: &CompressionDescriptor,
) -> Result<Vec<u8>, cairn_types::error::BlobError> {
    use futures_util::StreamExt;
    let handle = store.open_with_dek(path, range, dek, compression).await?;
    let mut out = Vec::new();
    let mut body = handle.body;
    while let Some(c) = body.next().await {
        out.extend_from_slice(&c?);
    }
    Ok(out)
}

/// SSE-S3 over the real store: an encrypted+compressed object round-trips when read back with the
/// same DEK, its ETag equals the plaintext MD5 (encryption is transparent to the ETag), and a
/// ranged read of the encrypted blob returns the matching plaintext slice.
#[tokio::test]
async fn encrypted_roundtrip_etag_invariant_and_ranged() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let data: Vec<u8> = b"the quick brown fox "
        .iter()
        .copied()
        .cycle()
        .take(10_000)
        .collect();
    let policy = CompressionPolicy {
        algorithm: CompressionAlgorithm::Zstd,
        block_size: 1024,
    };
    let dek = [0x11u8; 32];

    // The same plaintext, staged plain and staged encrypted, must share the plaintext-MD5 ETag.
    let plain = store
        .stage(&b, body(data.clone()), opts(None, "text/plain"))
        .await
        .unwrap();
    let enc = store
        .stage(
            &b,
            chunked_body(data.clone(), 333),
            opts_encrypted(Some(policy), "text/plain", dek),
        )
        .await
        .unwrap();
    assert_eq!(
        plain.etag.as_str(),
        enc.etag.as_str(),
        "ETag is plaintext MD5"
    );
    assert_eq!(enc.size_logical, data.len() as u64);

    // Full read with the correct DEK returns the original bytes.
    assert_eq!(
        read_all_dek(&store, &enc.storage_path, None, Some(dek), &enc.compression)
            .await
            .unwrap(),
        data
    );
    // A ranged read decrypts only the overlapping blocks.
    let range = ByteRange {
        offset: 5000,
        length: 1234,
    };
    assert_eq!(
        read_all_dek(
            &store,
            &enc.storage_path,
            Some(range),
            Some(dek),
            &enc.compression
        )
        .await
        .unwrap(),
        &data[5000..6234]
    );
    // An encrypted blob never offers a zero-copy hint (the kernel cannot decrypt).
    assert!(
        store
            .open_with_dek(&enc.storage_path, None, Some(dek), &enc.compression)
            .await
            .unwrap()
            .zero_copy
            .is_none()
    );
}

/// The on-disk form of an encrypted blob is the CRNB **encrypted** variant, not merely a
/// transformed plaintext blob: the stored file's trailer version byte is `VERSION_ENCRYPTED`, a
/// DEK-less open through the store FAILS (the reader refuses an encrypted container without a key),
/// and the read handle offers NO zero-copy hint (so ciphertext can never reach the sendfile fast
/// path). This is stronger than "physical != logical" — a compressed *plaintext* blob also differs
/// from its logical bytes, so size/version framing alone would not prove encryption.
#[tokio::test]
async fn encrypted_blob_on_disk_is_encrypted_variant_and_no_zero_copy() {
    // The 34-byte CRNB trailer sits at the end of the file; the version byte is at trailer offset 4
    // and `2` marks the encrypted variant (see `cairn-blob/src/compress.rs`).
    const TRAILER_LEN: usize = 34;
    const VERSION_ENCRYPTED: u8 = 2;

    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let data: Vec<u8> = b"encrypt me at rest "
        .iter()
        .copied()
        .cycle()
        .take(9_000)
        .collect();
    let policy = CompressionPolicy {
        algorithm: CompressionAlgorithm::Zstd,
        block_size: 1024,
    };
    let dek = [0x5au8; 32];

    let enc = store
        .stage(
            &b,
            chunked_body(data.clone(), 512),
            opts_encrypted(Some(policy), "text/plain", dek),
        )
        .await
        .unwrap();

    // (1) Opening with the correct DEK decrypts back to the exact plaintext.
    assert_eq!(
        read_all_dek(&store, &enc.storage_path, None, Some(dek), &enc.compression)
            .await
            .unwrap(),
        data
    );

    // (2) The stored file on disk is the CRNB encrypted variant (version byte == 2).
    let on_disk = dir.path().join(enc.storage_path.as_str());
    let raw = std::fs::read(&on_disk).unwrap();
    let trailer = &raw[raw.len() - TRAILER_LEN..];
    assert_eq!(&trailer[0..4], b"CRNB", "stored blob is a CRNB container");
    assert_eq!(
        trailer[4], VERSION_ENCRYPTED,
        "stored blob is the ENCRYPTED CRNB variant, not a compressed-plaintext one"
    );

    // (3) A DEK-less open FAILS — the reader refuses an encrypted container with no key rather than
    // handing back ciphertext or plaintext (fails closed).
    let no_dek = read_all_dek(&store, &enc.storage_path, None, None, &enc.compression).await;
    assert!(
        matches!(no_dek, Err(cairn_types::error::BlobError::Corruption(_))),
        "a DEK-less open of an encrypted blob must fail, got {no_dek:?}"
    );

    // (4) An encrypted blob offers no zero-copy hint: ciphertext can never reach the sendfile path.
    assert!(
        store
            .open_with_dek(&enc.storage_path, None, Some(dek), &enc.compression)
            .await
            .unwrap()
            .zero_copy
            .is_none()
    );
}

/// An encrypted object encrypts even when the bucket has no compression policy: SSE flows through
/// the block container with `CompressionAlgorithm::None`, and the wrong DEK (or no DEK) fails to
/// decrypt rather than leaking plaintext.
#[tokio::test]
async fn encrypted_without_compression_and_wrong_dek_fails() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let data: Vec<u8> = (0..20_000u32).map(|i| (i % 256) as u8).collect();
    let dek = [0x22u8; 32];

    let enc = store
        .stage(
            &b,
            chunked_body(data.clone(), 4096),
            opts_encrypted(None, "application/octet-stream", dek),
        )
        .await
        .unwrap();
    // Even with no compression policy, the object is stored encrypted (uncompressed descriptor).
    assert!(matches!(
        enc.compression,
        CompressionDescriptor::Uncompressed
    ));

    // Correct DEK reads the original bytes.
    assert_eq!(
        read_all_dek(&store, &enc.storage_path, None, Some(dek), &enc.compression)
            .await
            .unwrap(),
        data
    );
    // The wrong DEK fails authentication.
    let wrong = read_all_dek(
        &store,
        &enc.storage_path,
        None,
        Some([0x23u8; 32]),
        &enc.compression,
    )
    .await;
    assert!(matches!(
        wrong,
        Err(cairn_types::error::BlobError::Corruption(_))
    ));
    // With the descriptor+DEK contract (audit #18) the blob layer trusts the caller's signals
    // instead of sniffing the trailer. Reading this (uncompressed-descriptor) encrypted blob with
    // NO DEK therefore yields the stored CRNB-container bytes as-is rather than erroring — but
    // never the plaintext. This cannot occur on the production GET path, which always supplies the
    // DEK derived from the object's SSE descriptor when the object is encrypted.
    let none = read_all_dek(&store, &enc.storage_path, None, None, &enc.compression)
        .await
        .expect("trusting the descriptor, a no-DEK read returns the raw stored bytes");
    assert_ne!(none, data, "a no-DEK read must never yield plaintext");
}

/// An old-format (unencrypted) blob still reads unchanged through both `open` and `open_with_dek`
/// after the format gained encryption, confirming the version gate keeps backward compatibility.
#[tokio::test]
async fn old_unencrypted_blob_reads_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let data: Vec<u8> = b"plaintext payload that compresses "
        .iter()
        .copied()
        .cycle()
        .take(8000)
        .collect();
    let policy = CompressionPolicy {
        algorithm: CompressionAlgorithm::Zstd,
        block_size: 512,
    };
    let staged = store
        .stage(&b, body(data.clone()), opts(Some(policy), "text/plain"))
        .await
        .unwrap();
    // Reads via the legacy `open` and via `open_with_dek(None)` both succeed and match.
    assert_eq!(
        read_all(&store, &staged.storage_path, None, &staged.compression).await,
        data
    );
    assert_eq!(
        read_all_dek(
            &store,
            &staged.storage_path,
            None,
            None,
            &staged.compression
        )
        .await
        .unwrap(),
        data
    );
}

#[tokio::test]
async fn precompressed_content_type_stored_raw() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let policy = CompressionPolicy {
        algorithm: CompressionAlgorithm::Zstd,
        block_size: 1024,
    };
    // Even with a compression policy, image/* is stored uncompressed.
    let staged = store
        .stage(&b, body(vec![1u8; 5000]), opts(Some(policy), "image/jpeg"))
        .await
        .unwrap();
    assert!(matches!(
        staged.compression,
        CompressionDescriptor::Uncompressed
    ));
}

#[tokio::test]
async fn size_ceiling_aborts() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let mut o = opts(None, "application/octet-stream");
    o.size_ceiling = 100;
    let err = store.stage(&b, body(vec![0u8; 500]), o).await.unwrap_err();
    assert!(matches!(err, BlobError::SizeExceeded));
    // The aborted staging artifact is cleaned up.
    let staging = dir.path().join(".staging");
    let mut count = 0;
    let mut rd = tokio::fs::read_dir(&staging).await.unwrap();
    while let Some(e) = rd.next_entry().await.unwrap() {
        if e.file_type().await.unwrap().is_file() {
            count += 1;
        }
    }
    assert_eq!(count, 0, "staging temp file removed on failure");
}

#[tokio::test]
async fn multipart_assembly_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let upload = UploadId::generate();
    let p1 = store
        .stage_part(
            &upload,
            1,
            body(b"part-one-".to_vec()),
            ChecksumSet::none(),
            1 << 20,
            None,
        )
        .await
        .unwrap();
    let p2 = store
        .stage_part(
            &upload,
            2,
            body(b"part-two".to_vec()),
            ChecksumSet::none(),
            1 << 20,
            None,
        )
        .await
        .unwrap();
    let refs = vec![
        PartRef {
            part_number: 1,
            storage_path: p1.storage_path.clone(),
            size: p1.size,
            dek: None,
        },
        PartRef {
            part_number: 2,
            storage_path: p2.storage_path.clone(),
            size: p2.size,
            dek: None,
        },
    ];
    let assembled = store
        .assemble(&b, &refs, opts(None, "text/plain"))
        .await
        .unwrap();
    assert_eq!(assembled.size_logical, (p1.size + p2.size));
    assert_eq!(
        read_all(
            &store,
            &assembled.storage_path,
            None,
            &assembled.compression
        )
        .await,
        b"part-one-part-two"
    );
    store.delete_session(&upload).await.unwrap();
    store.delete_session(&upload).await.unwrap(); // idempotent
}

#[tokio::test]
async fn assemble_enforces_size_ceiling() {
    // Audit 2026-07: the multipart total must be bounded by size_ceiling, exactly as a single PUT is.
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let upload = UploadId::generate();
    let p1 = store
        .stage_part(
            &upload,
            1,
            body(b"part-one-".to_vec()),
            ChecksumSet::none(),
            1 << 20,
            None,
        )
        .await
        .unwrap();
    let p2 = store
        .stage_part(
            &upload,
            2,
            body(b"part-two".to_vec()),
            ChecksumSet::none(),
            1 << 20,
            None,
        )
        .await
        .unwrap();
    let refs = vec![
        PartRef {
            part_number: 1,
            storage_path: p1.storage_path.clone(),
            size: p1.size,
            dek: None,
        },
        PartRef {
            part_number: 2,
            storage_path: p2.storage_path.clone(),
            size: p2.size,
            dek: None,
        },
    ];
    // The parts total 17 bytes; a 10-byte ceiling must reject the assembly.
    let tight = StageOptions {
        size_ceiling: 10,
        content_type: "text/plain".to_owned(),
        ..StageOptions::default()
    };
    assert!(matches!(
        store.assemble(&b, &refs, tight).await,
        Err(BlobError::SizeExceeded)
    ));
    // A generous ceiling assembles fine.
    assert!(
        store
            .assemble(&b, &refs, opts(None, "text/plain"))
            .await
            .is_ok()
    );
    store.delete_session(&upload).await.unwrap();
}

#[tokio::test]
async fn reconcile_reclaims_orphans_only() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let keep = store
        .stage(&b, body(b"keep".to_vec()), opts(None, "text/plain"))
        .await
        .unwrap();
    let orphan = store
        .stage(&b, body(b"orphan".to_vec()), opts(None, "text/plain"))
        .await
        .unwrap();

    // The oracle says only `keep` is referenced by metadata.
    let mut live = std::collections::HashSet::new();
    live.insert(keep.storage_path.as_str().to_owned());
    let oracle = SetReconcileOracle {
        live_paths: live,
        live_uploads: Default::default(),
    };

    let report = store
        .reconcile(
            &oracle,
            ReconcileOpts {
                staging_safety_margin_secs: 0,
                ..ReconcileOpts::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(report.orphans_reclaimed, 1);
    // keep is still readable, orphan is gone.
    assert_eq!(
        read_all(&store, &keep.storage_path, None, &keep.compression).await,
        b"keep"
    );
    assert!(matches!(
        store
            .open(&orphan.storage_path, None, &orphan.compression)
            .await,
        Err(BlobError::NotFound)
    ));
}

/// A blob younger than the staging safety margin must NOT be reclaimed even when the oracle reports
/// it as not-live: it may be an in-flight PUT whose metadata row has not yet committed (audit #7).
#[tokio::test]
async fn reconcile_skips_recent_orphan_within_safety_margin() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let orphan = store
        .stage(&b, body(b"fresh".to_vec()), opts(None, "text/plain"))
        .await
        .unwrap();
    // The oracle says nothing is live, but the just-written blob is within the safety margin.
    let oracle = SetReconcileOracle {
        live_paths: Default::default(),
        live_uploads: Default::default(),
    };
    let report = store
        .reconcile(
            &oracle,
            ReconcileOpts {
                staging_safety_margin_secs: 3600,
                ..ReconcileOpts::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        report.orphans_reclaimed, 0,
        "a blob younger than the safety margin must not be reclaimed"
    );
    assert_eq!(
        read_all(&store, &orphan.storage_path, None, &orphan.compression).await,
        b"fresh"
    );
}

#[tokio::test]
async fn reconcile_prunes_emptied_bucket_dir() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("doomed").unwrap();
    // One bucket whose only blob is an orphan, and a second bucket whose blob is live.
    store
        .stage(&b, body(b"orphan".to_vec()), opts(None, "text/plain"))
        .await
        .unwrap();
    let kb = BucketName::parse("kept").unwrap();
    let keep = store
        .stage(&kb, body(b"keep".to_vec()), opts(None, "text/plain"))
        .await
        .unwrap();

    let mut live = std::collections::HashSet::new();
    live.insert(keep.storage_path.as_str().to_owned());
    let oracle = SetReconcileOracle {
        live_paths: live,
        live_uploads: Default::default(),
    };

    let report = store
        .reconcile(
            &oracle,
            ReconcileOpts {
                staging_safety_margin_secs: 0,
                ..ReconcileOpts::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(report.orphans_reclaimed, 1);
    // The emptied bucket directory is pruned; the populated one survives.
    assert_eq!(report.dirs_pruned, 1, "emptied bucket dir is pruned");
    assert!(
        !tokio::fs::try_exists(dir.path().join("doomed"))
            .await
            .unwrap(),
        "doomed bucket dir removed once empty"
    );
    assert!(
        tokio::fs::try_exists(dir.path().join("kept"))
            .await
            .unwrap(),
        "kept bucket dir preserved"
    );
}

#[tokio::test]
async fn reconcile_honours_parallelism_across_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    // Several buckets, each with a single orphan blob, reconciled with bounded concurrency.
    for n in 0..6 {
        let b = BucketName::parse(&format!("bucket-{n}")).unwrap();
        store
            .stage(&b, body(vec![n as u8; 8]), opts(None, "text/plain"))
            .await
            .unwrap();
    }
    let oracle = SetReconcileOracle::default(); // nothing is live
    let opts = ReconcileOpts {
        parallelism: 3,
        // margin 0 so the just-staged orphans are reclaimed immediately (this test exercises
        // parallel reclamation, not the safety margin).
        staging_safety_margin_secs: 0,
        ..ReconcileOpts::default()
    };
    let report = store.reconcile(&oracle, opts).await.unwrap();
    assert_eq!(report.blobs_scanned, 6);
    assert_eq!(report.orphans_reclaimed, 6);
    assert_eq!(report.dirs_pruned, 6, "every emptied bucket dir is pruned");
}

#[cfg(unix)]
#[tokio::test]
async fn single_filesystem_check_passes() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    // Data root and its in-root staging dir share a filesystem, so the startup check is Ok.
    store.check_single_filesystem().unwrap();
}

#[tokio::test]
async fn delete_is_idempotent_and_paths_are_safe() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let staged = store
        .stage(&b, body(b"x".to_vec()), opts(None, "text/plain"))
        .await
        .unwrap();
    store.delete(&staged.storage_path).await.unwrap();
    store.delete(&staged.storage_path).await.unwrap(); // idempotent: absence is success

    // A traversal path is rejected structurally.
    let evil = StoragePath::from_string("../../../etc/passwd".to_owned());
    assert!(
        store
            .open(&evil, None, &CompressionDescriptor::Uncompressed)
            .await
            .is_err()
    );
}

/// `stage_part` computes the requested supplementary checksum over the part's plaintext (composite
/// multipart checksums, Phase 1): a CRC32 request returns the documented base64 CRC32 of the part's
/// bytes, and no checksum is returned when none is requested. Fails before this change (the field and
/// the `checksums` parameter did not exist).
#[tokio::test]
async fn stage_part_computes_requested_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let upload = UploadId::generate();
    // CRC32("abc") = 0x352441C2; base64 of the big-endian bytes is "NSRBwg==".
    let staged = store
        .stage_part(
            &upload,
            1,
            body(b"abc".to_vec()),
            ChecksumSet(vec![ChecksumAlgorithm::Crc32]),
            1 << 20,
            None,
        )
        .await
        .unwrap();
    assert_eq!(staged.checksums.len(), 1);
    assert_eq!(staged.checksums[0].algorithm, ChecksumAlgorithm::Crc32);
    assert_eq!(staged.checksums[0].value, "NSRBwg==");

    // No algorithm requested -> no supplementary checksum computed.
    let plain = store
        .stage_part(
            &upload,
            2,
            body(b"abc".to_vec()),
            ChecksumSet::none(),
            1 << 20,
            None,
        )
        .await
        .unwrap();
    assert!(plain.checksums.is_empty());
}

/// `assemble` honors `opts.extra_checksums`, recomputing a whole-object checksum over the assembled
/// plaintext: the CRC64NVME of two assembled parts equals the CRC64NVME of the same concatenated
/// bytes staged as a single object. Fails before this change (`assemble` returned `Vec::new()`).
#[tokio::test]
async fn assemble_honors_extra_checksums_whole_object() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let upload = UploadId::generate();
    let p1 = store
        .stage_part(
            &upload,
            1,
            body(b"part-one-".to_vec()),
            ChecksumSet::none(),
            1 << 20,
            None,
        )
        .await
        .unwrap();
    let p2 = store
        .stage_part(
            &upload,
            2,
            body(b"part-two".to_vec()),
            ChecksumSet::none(),
            1 << 20,
            None,
        )
        .await
        .unwrap();
    let refs = vec![
        PartRef {
            part_number: 1,
            storage_path: p1.storage_path.clone(),
            size: p1.size,
            dek: None,
        },
        PartRef {
            part_number: 2,
            storage_path: p2.storage_path.clone(),
            size: p2.size,
            dek: None,
        },
    ];
    let assemble_opts = StageOptions {
        extra_checksums: ChecksumSet(vec![ChecksumAlgorithm::Crc64Nvme]),
        size_ceiling: 100 * 1024 * 1024,
        content_type: "text/plain".to_owned(),
        ..StageOptions::default()
    };
    let assembled = store.assemble(&b, &refs, assemble_opts).await.unwrap();
    let assembled_crc = assembled
        .checksums
        .iter()
        .find(|c| c.algorithm == ChecksumAlgorithm::Crc64Nvme)
        .expect("crc64nvme present");

    // The same concatenated bytes staged as a single object must carry the identical whole-object
    // CRC64NVME — proving assembly recomputes over the plaintext, not a checksum-of-checksums.
    let whole = StageOptions {
        extra_checksums: ChecksumSet(vec![ChecksumAlgorithm::Crc64Nvme]),
        size_ceiling: 100 * 1024 * 1024,
        content_type: "text/plain".to_owned(),
        ..StageOptions::default()
    };
    let single = store
        .stage(&b, body(b"part-one-part-two".to_vec()), whole)
        .await
        .unwrap();
    let single_crc = single
        .checksums
        .iter()
        .find(|c| c.algorithm == ChecksumAlgorithm::Crc64Nvme)
        .expect("crc64nvme present");
    assert_eq!(assembled_crc.value, single_crc.value);
}

/// An object staged through the io_uring write path reads back byte-for-byte identically and with
/// the same ETag as the same bytes staged through the default `tokio::fs` path. This exercises the
/// dedicated io_uring executor end to end: create the staging tmp, stream the payload, fsync,
/// rename, fsync the bucket dir (the F-1 ordering), then read the committed blob back. Multi-chunk
/// and large bodies confirm the running-offset positional writes reassemble correctly.
#[cfg(feature = "io-uring")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn io_uring_staged_object_reads_back_identically() {
    let dir = tempfile::tempdir().unwrap();
    // Two stores over the same root: one forced onto the io_uring path, one onto tokio::fs.
    let uring = LocalBlobStore::open(dir.path())
        .await
        .unwrap()
        .with_io_uring(true);
    let epoll = LocalBlobStore::open(dir.path())
        .await
        .unwrap()
        .with_io_uring(false);
    let b = BucketName::parse("bkt").unwrap();

    // A payload large enough to span many write chunks, delivered in small chunks.
    let data: Vec<u8> = (0..(1u32 << 18))
        .map(|i| i.wrapping_mul(2_654_435_761) as u8)
        .collect();

    let via_uring = uring
        .stage(
            &b,
            chunked_body(data.clone(), 7000),
            opts(None, "application/octet-stream"),
        )
        .await
        .unwrap();
    let via_epoll = epoll
        .stage(
            &b,
            chunked_body(data.clone(), 7000),
            opts(None, "application/octet-stream"),
        )
        .await
        .unwrap();

    // Same plaintext MD5/ETag and size from both backends.
    assert_eq!(via_uring.etag.as_str(), via_epoll.etag.as_str());
    assert_eq!(via_uring.size_logical, data.len() as u64);
    assert_eq!(via_uring.size_physical, via_epoll.size_physical);

    // The blob committed by the io_uring path reads back byte-for-byte.
    assert_eq!(
        read_all(
            &uring,
            &via_uring.storage_path,
            None,
            &via_uring.compression
        )
        .await,
        data
    );
    // And it is fully readable through a store using the default backend, too — the on-disk
    // artifact is identical regardless of which path wrote it.
    assert_eq!(
        read_all(
            &epoll,
            &via_uring.storage_path,
            None,
            &via_uring.compression
        )
        .await,
        data
    );

    // A ranged read of the io_uring-staged blob returns the matching slice.
    let range = ByteRange {
        offset: 100_000,
        length: 4096,
    };
    assert_eq!(
        read_all(
            &uring,
            &via_uring.storage_path,
            Some(range),
            &via_uring.compression
        )
        .await,
        &data[100_000..104_096]
    );
}

/// The io_uring path honours the durable-commit ordering across compression, encryption, and
/// multipart assembly: each variant staged through io_uring round-trips to the original bytes.
#[cfg(feature = "io-uring")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn io_uring_compressed_encrypted_and_multipart_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path())
        .await
        .unwrap()
        .with_io_uring(true);
    let b = BucketName::parse("bkt").unwrap();
    let data: Vec<u8> = b"the quick brown fox "
        .iter()
        .copied()
        .cycle()
        .take(40_000)
        .collect();
    let policy = CompressionPolicy {
        algorithm: CompressionAlgorithm::Zstd,
        block_size: 4096,
    };
    let dek = [0x5au8; 32];

    // Compressed + encrypted single-shot stage via io_uring.
    let enc = store
        .stage(
            &b,
            chunked_body(data.clone(), 1234),
            opts_encrypted(Some(policy), "text/plain", dek),
        )
        .await
        .unwrap();
    assert!(enc.size_physical < enc.size_logical, "compressed on disk");
    assert_eq!(
        read_all_dek(&store, &enc.storage_path, None, Some(dek), &enc.compression)
            .await
            .unwrap(),
        data
    );

    // Multipart parts staged + assembled via io_uring.
    let upload = UploadId::generate();
    let p1 = store
        .stage_part(
            &upload,
            1,
            body(b"uring-part-one-".to_vec()),
            ChecksumSet::none(),
            1 << 20,
            None,
        )
        .await
        .unwrap();
    let p2 = store
        .stage_part(
            &upload,
            2,
            body(b"uring-part-two".to_vec()),
            ChecksumSet::none(),
            1 << 20,
            None,
        )
        .await
        .unwrap();
    let refs = vec![
        PartRef {
            part_number: 1,
            storage_path: p1.storage_path.clone(),
            size: p1.size,
            dek: None,
        },
        PartRef {
            part_number: 2,
            storage_path: p2.storage_path.clone(),
            size: p2.size,
            dek: None,
        },
    ];
    let assembled = store
        .assemble(&b, &refs, opts(None, "text/plain"))
        .await
        .unwrap();
    assert_eq!(
        read_all(
            &store,
            &assembled.storage_path,
            None,
            &assembled.compression
        )
        .await,
        b"uring-part-one-uring-part-two"
    );
    store.delete_session(&upload).await.unwrap();
}

/// A lightweight, opt-in throughput probe comparing the io_uring staging-write backend against the
/// default `tokio::fs` backend over the same data root. It is `#[ignore]`d so it never runs in the
/// normal gate (it does real fsyncs and depends on the host's storage), but can be invoked with
/// `cargo test -p cairn-blob --features io-uring -- --ignored --nocapture uring_vs_epoll`.
/// Reports MB/s for each backend and the relative delta. Not an assertion of a performance target;
/// io_uring's win is workload- and kernel-dependent (it shines most under high concurrency, which
/// a single-threaded loop understates).
#[cfg(feature = "io-uring")]
#[ignore = "benchmark: run explicitly with --ignored"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uring_vs_epoll_staging_throughput() {
    use std::time::Instant;

    async fn run(store: &LocalBlobStore, payload: &[u8], iters: u32) -> f64 {
        let b = BucketName::parse("bench").unwrap();
        // Warm up so directory-creation and first-touch costs don't skew the timed loop.
        for _ in 0..4 {
            store
                .stage(
                    &b,
                    body(payload.to_vec()),
                    opts(None, "application/octet-stream"),
                )
                .await
                .unwrap();
        }
        let start = Instant::now();
        for _ in 0..iters {
            store
                .stage(
                    &b,
                    body(payload.to_vec()),
                    opts(None, "application/octet-stream"),
                )
                .await
                .unwrap();
        }
        let elapsed = start.elapsed().as_secs_f64();
        let total_bytes = (payload.len() as f64) * (iters as f64);
        (total_bytes / (1024.0 * 1024.0)) / elapsed
    }

    let payload = vec![0xABu8; 1 << 20]; // 1 MiB objects
    let iters = 200u32;

    let dir_u = tempfile::tempdir().unwrap();
    let uring = LocalBlobStore::open(dir_u.path())
        .await
        .unwrap()
        .with_io_uring(true);
    let dir_e = tempfile::tempdir().unwrap();
    let epoll = LocalBlobStore::open(dir_e.path())
        .await
        .unwrap()
        .with_io_uring(false);

    let uring_mibs = run(&uring, &payload, iters).await;
    let epoll_mibs = run(&epoll, &payload, iters).await;
    let delta_pct = (uring_mibs - epoll_mibs) / epoll_mibs * 100.0;
    println!(
        "staging throughput ({iters}x1MiB): io_uring={uring_mibs:.1} MiB/s, \
         tokio::fs={epoll_mibs:.1} MiB/s, delta={delta_pct:+.1}%"
    );
}

/// Concurrent staging throughput: io_uring's advantage shows when many writes overlap (it batches
/// submissions to the kernel and overlaps fsync/rename), not on a serial loop. Run with multiple
/// io_uring reactor threads: `CAIRN_URING_THREADS=4 cargo test -p cairn-blob --release --features
/// io-uring -- --ignored --nocapture uring_vs_epoll_concurrent`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
#[cfg(feature = "io-uring")]
async fn uring_vs_epoll_concurrent_staging() {
    use std::sync::Arc;
    use std::time::Instant;

    async fn run(store: Arc<LocalBlobStore>, payload: Arc<Vec<u8>>, conc: usize, per: u32) -> f64 {
        let b = BucketName::parse("bench").unwrap();
        for _ in 0..4 {
            store
                .stage(
                    &b,
                    body(payload.to_vec()),
                    opts(None, "application/octet-stream"),
                )
                .await
                .unwrap();
        }
        let start = Instant::now();
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..conc {
            let (s, p) = (store.clone(), payload.clone());
            set.spawn(async move {
                let b = BucketName::parse("bench").unwrap();
                for _ in 0..per {
                    s.stage(&b, body(p.to_vec()), opts(None, "application/octet-stream"))
                        .await
                        .unwrap();
                }
            });
        }
        while set.join_next().await.is_some() {}
        let elapsed = start.elapsed().as_secs_f64();
        let total = payload.len() as f64 * conc as f64 * per as f64;
        (total / (1024.0 * 1024.0)) / elapsed
    }

    let payload = Arc::new(vec![0xABu8; 256 * 1024]); // 256 KiB
    let (conc, per) = (32usize, 16u32);

    let du = tempfile::tempdir().unwrap();
    let uring = Arc::new(
        LocalBlobStore::open(du.path())
            .await
            .unwrap()
            .with_io_uring(true),
    );
    let de = tempfile::tempdir().unwrap();
    let epoll = Arc::new(
        LocalBlobStore::open(de.path())
            .await
            .unwrap()
            .with_io_uring(false),
    );

    let u = run(uring, payload.clone(), conc, per).await;
    let e = run(epoll, payload, conc, per).await;
    println!(
        "CONCURRENT staging ({conc} workers x {per} x 256KiB): io_uring={u:.1} MiB/s, \
         tokio::fs={e:.1} MiB/s, delta={:+.1}%",
        (u - e) / e * 100.0
    );
}

// ---------------------------------------------------------------------------------------------
// Part-level SSE at rest (ARCH 27, Increment 3a). A part staged with a per-part DEK is ciphertext
// on disk; `assemble` decrypts it on read (via `PartRef.dek`) before hashing + re-encoding. All
// real ciphertext/nonce assertions run here against the on-disk `LocalBlobStore`, never a double.
// ---------------------------------------------------------------------------------------------

/// Read a staged/committed blob file's raw on-disk bytes given its logical `StoragePath`.
fn raw_blob_bytes(dir: &std::path::Path, path: &StoragePath) -> Vec<u8> {
    std::fs::read(dir.join(path.as_str())).unwrap()
}

fn part_ref(part_number: u16, staged: &StagedPart, dek: Option<[u8; 32]>) -> PartRef {
    PartRef {
        part_number,
        storage_path: staged.storage_path.clone(),
        size: staged.size,
        dek,
    }
}

/// Two encrypted parts assembled with their matching per-part DEKs reproduce the plaintext concat
/// byte-for-byte, and the object ETag is the plaintext MD5. Fails before this change: `stage_part`
/// had no `encryption` param and `assemble_into` never decrypted a part.
#[tokio::test]
async fn stage_part_encrypted_roundtrips_via_assemble() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let upload = UploadId::generate();
    let dek1 = [7u8; 32];
    let dek2 = [9u8; 32];
    let plain1 = vec![b'A'; 6 * 1024 * 1024];
    let plain2 = b"tail-bytes".to_vec();
    let p1 = store
        .stage_part(
            &upload,
            1,
            body(plain1.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some(dek1),
        )
        .await
        .unwrap();
    let p2 = store
        .stage_part(
            &upload,
            2,
            body(plain2.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some(dek2),
        )
        .await
        .unwrap();
    let refs = vec![part_ref(1, &p1, Some(dek1)), part_ref(2, &p2, Some(dek2))];
    let assembled = store
        .assemble(&b, &refs, opts(None, "application/octet-stream"))
        .await
        .unwrap();
    let mut expected = plain1.clone();
    expected.extend_from_slice(&plain2);
    assert_eq!(assembled.size_logical, expected.len() as u64);
    assert_eq!(
        read_all(
            &store,
            &assembled.storage_path,
            None,
            &assembled.compression
        )
        .await,
        expected
    );
    // ETag/MD5 basis stays the PLAINTEXT digest (unchanged by per-part encryption): it matches the
    // same concatenated bytes staged as a single plaintext object.
    let single = store
        .stage(
            &b,
            body(expected.clone()),
            opts(None, "application/octet-stream"),
        )
        .await
        .unwrap();
    assert_eq!(assembled.md5_hex, single.md5_hex);
}

/// A staged encrypted part is a `VERSION_ENCRYPTED` CRNB container on disk, and it cannot be opened
/// without the DEK — proving nothing plaintext is written. Fails before: parts were staged plaintext.
#[tokio::test]
async fn staged_part_file_is_ciphertext() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let upload = UploadId::generate();
    let dek = [3u8; 32];
    let plain = vec![b'Z'; 4096];
    let p = store
        .stage_part(
            &upload,
            1,
            body(plain.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some(dek),
        )
        .await
        .unwrap();
    let raw = raw_blob_bytes(dir.path(), &p.storage_path);
    // The CRNB trailer is 34 bytes: magic(4) + version(1) at offset 4. VERSION_ENCRYPTED == 2.
    let ver = raw[raw.len() - 34 + 4];
    assert_eq!(ver, 2, "staged part trailer must be VERSION_ENCRYPTED");
    // A known plaintext run is absent from the on-disk ciphertext.
    assert!(
        raw.windows(16).all(|w| w != [b'Z'; 16]),
        "plaintext run leaked into the staged part file"
    );
    // The container refuses to open without a DEK (fails closed).
    let f = std::fs::File::open(dir.path().join(p.storage_path.as_str())).unwrap();
    assert!(cairn_blob::compress::CompressedReader::open_with_dek(f, None).is_err());
}

/// Identical plaintext staged as two parts under two independent random DEKs yields DIFFERENT
/// block-0 ciphertext on disk — the nonce-reuse-catastrophe guard (block_index restarts at 0 per
/// blob, so distinct keys are what keep `(key, nonce)` unique). Also covers the re-upload case: the
/// same part number staged twice under fresh keys must differ. Fails under a shared-key strategy.
#[tokio::test]
async fn two_identical_parts_differ_in_block0_ciphertext() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let upload = UploadId::generate();
    let plain = vec![b'Q'; 4096];
    let a = store
        .stage_part(
            &upload,
            1,
            body(plain.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some([1u8; 32]),
        )
        .await
        .unwrap();
    let bpart = store
        .stage_part(
            &upload,
            2,
            body(plain.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some([2u8; 32]),
        )
        .await
        .unwrap();
    let ra = raw_blob_bytes(dir.path(), &a.storage_path);
    let rb = raw_blob_bytes(dir.path(), &bpart.storage_path);
    assert_ne!(
        ra[..16],
        rb[..16],
        "identical plaintext must differ in block-0 ciphertext"
    );

    // Re-upload variant: the same part number staged twice under fresh keys must also differ.
    let first = store
        .stage_part(
            &upload,
            3,
            body(plain.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some([4u8; 32]),
        )
        .await
        .unwrap();
    let second = store
        .stage_part(
            &upload,
            3,
            body(plain.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some([5u8; 32]),
        )
        .await
        .unwrap();
    let rf = raw_blob_bytes(dir.path(), &first.storage_path);
    let rs = raw_blob_bytes(dir.path(), &second.storage_path);
    assert_ne!(
        rf[..16],
        rs[..16],
        "re-uploaded staging must differ in block-0 ciphertext"
    );
}

/// Assembling an encrypted part with the WRONG DEK fails GCM auth → `BlobError::Corruption`, and no
/// orphan tmp is left in `.staging`. Fails-closed: never plaintext/zeros/partial.
#[tokio::test]
async fn assemble_wrong_part_dek_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let upload = UploadId::generate();
    let plain = vec![b'W'; 6 * 1024 * 1024];
    let p = store
        .stage_part(
            &upload,
            1,
            body(plain.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some([1u8; 32]),
        )
        .await
        .unwrap();
    // Assemble with a DIFFERENT key than the part was staged under.
    let refs = vec![part_ref(1, &p, Some([2u8; 32]))];
    let err = store
        .assemble(&b, &refs, opts(None, "application/octet-stream"))
        .await;
    assert!(
        matches!(err, Err(BlobError::Corruption(_))),
        "wrong part DEK must be Corruption"
    );
    // No orphan staging tmp remains.
    let staging = dir.path().join(".staging");
    let leaked: Vec<_> = std::fs::read_dir(&staging)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(
        leaked.is_empty(),
        "failed assembly left an orphan staging tmp"
    );
}

/// Encrypted parts are physically larger on disk than their plaintext (per-block GCM tag + index +
/// trailer), but `assemble` preallocates from the parts' LOGICAL sizes; the committed object still
/// reads back byte-exact with `size_logical == Σ plaintext`.
#[tokio::test]
async fn preallocation_holds_for_encrypted_parts() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let upload = UploadId::generate();
    let plain1 = vec![b'M'; 6 * 1024 * 1024];
    let plain2 = vec![b'N'; 5 * 1024 * 1024];
    let p1 = store
        .stage_part(
            &upload,
            1,
            body(plain1.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some([1u8; 32]),
        )
        .await
        .unwrap();
    let p2 = store
        .stage_part(
            &upload,
            2,
            body(plain2.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some([2u8; 32]),
        )
        .await
        .unwrap();
    // The staged part file is larger than the plaintext (GCM overhead).
    assert!(raw_blob_bytes(dir.path(), &p1.storage_path).len() > plain1.len());
    let refs = vec![
        part_ref(1, &p1, Some([1u8; 32])),
        part_ref(2, &p2, Some([2u8; 32])),
    ];
    let assembled = store
        .assemble(&b, &refs, opts(None, "application/octet-stream"))
        .await
        .unwrap();
    let mut expected = plain1.clone();
    expected.extend_from_slice(&plain2);
    assert_eq!(assembled.size_logical, expected.len() as u64);
    assert_eq!(
        read_all(
            &store,
            &assembled.storage_path,
            None,
            &assembled.compression
        )
        .await,
        expected
    );
}

/// The MD5/checksum basis is the PLAINTEXT, computed BEFORE the encrypt transform, at BOTH the
/// `stage_part` (per-part) and `assemble` (whole-object) seams. Staging identical plaintext with a DEK
/// yields the SAME per-part CRC32 (and MD5) as staging it in the clear — even though the DEK path writes
/// a ciphertext CRNB container to disk. And `assemble` over the encrypted parts (decrypt-on-read)
/// recomputes the SAME whole-object CRC64NVME as `assemble` over the plaintext parts. A refactor that
/// hashed ciphertext would silently break SDK download integrity for every encrypted-bucket object.
#[tokio::test]
async fn stage_part_checksum_and_assemble_checksum_are_over_plaintext() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).await.unwrap();
    let b = BucketName::parse("bkt").unwrap();
    let dek1 = [0x41u8; 32];
    let dek2 = [0x42u8; 32];
    let plain1 = vec![b'a'; 6 * 1024 * 1024];
    let plain2 = b"the-tail".to_vec();

    // --- per-part: encrypted staging computes the SAME CRC32 (and MD5) as plaintext staging ---
    let enc_upload = UploadId::generate();
    let plain_upload = UploadId::generate();
    let enc_p1 = store
        .stage_part(
            &enc_upload,
            1,
            body(plain1.clone()),
            ChecksumSet(vec![ChecksumAlgorithm::Crc32]),
            1 << 30,
            Some(dek1),
        )
        .await
        .unwrap();
    let plain_p1 = store
        .stage_part(
            &plain_upload,
            1,
            body(plain1.clone()),
            ChecksumSet(vec![ChecksumAlgorithm::Crc32]),
            1 << 30,
            None,
        )
        .await
        .unwrap();
    // The DEK path really did write a ciphertext CRNB container (VERSION_ENCRYPTED trailer)...
    let enc_raw = raw_blob_bytes(dir.path(), &enc_p1.storage_path);
    assert_eq!(
        enc_raw[enc_raw.len() - 34 + 4],
        2,
        "the encrypted part is ciphertext on disk"
    );
    // ...yet its per-part CRC32 and MD5 are byte-identical to the plaintext staging.
    assert!(!enc_p1.checksums.is_empty());
    assert_eq!(
        enc_p1.checksums, plain_p1.checksums,
        "per-part CRC32 must be over plaintext, identical encrypted vs plaintext"
    );
    assert_eq!(
        enc_p1.md5_hex, plain_p1.md5_hex,
        "per-part MD5/ETag basis must be over plaintext"
    );

    // --- whole-object: assemble over encrypted parts == assemble over plaintext parts (CRC64NVME) ---
    let enc_p2 = store
        .stage_part(
            &enc_upload,
            2,
            body(plain2.clone()),
            ChecksumSet::none(),
            1 << 30,
            Some(dek2),
        )
        .await
        .unwrap();
    let plain_p2 = store
        .stage_part(
            &plain_upload,
            2,
            body(plain2.clone()),
            ChecksumSet::none(),
            1 << 30,
            None,
        )
        .await
        .unwrap();
    let asm_opts = StageOptions {
        extra_checksums: ChecksumSet(vec![ChecksumAlgorithm::Crc64Nvme]),
        size_ceiling: 100 * 1024 * 1024,
        content_type: "application/octet-stream".to_owned(),
        ..StageOptions::default()
    };
    let enc_asm = store
        .assemble(
            &b,
            &[
                part_ref(1, &enc_p1, Some(dek1)),
                part_ref(2, &enc_p2, Some(dek2)),
            ],
            asm_opts.clone(),
        )
        .await
        .unwrap();
    let plain_asm = store
        .assemble(
            &b,
            &[part_ref(1, &plain_p1, None), part_ref(2, &plain_p2, None)],
            asm_opts,
        )
        .await
        .unwrap();
    let enc_crc = enc_asm
        .checksums
        .iter()
        .find(|c| c.algorithm == ChecksumAlgorithm::Crc64Nvme)
        .expect("crc64nvme present");
    let plain_crc = plain_asm
        .checksums
        .iter()
        .find(|c| c.algorithm == ChecksumAlgorithm::Crc64Nvme)
        .expect("crc64nvme present");
    assert_eq!(
        enc_crc.value, plain_crc.value,
        "whole-object CRC64NVME must be recomputed over plaintext (decrypt-on-read), not ciphertext"
    );
    assert_eq!(
        enc_asm.md5_hex, plain_asm.md5_hex,
        "whole-object MD5 over plaintext"
    );
}
