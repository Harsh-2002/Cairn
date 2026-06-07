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

async fn read_all(store: &LocalBlobStore, path: &StoragePath, range: Option<ByteRange>) -> Vec<u8> {
    use futures_util::StreamExt;
    let handle = store.open(path, range).await.unwrap();
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
        read_all(&store, &staged.storage_path, None).await,
        b"hello world"
    );
    // The uncompressed path exposes a zero-copy hint.
    let handle = store.open(&staged.storage_path, None).await.unwrap();
    assert!(handle.zero_copy.is_some());
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

    // ETag is the plaintext MD5 either way (ARCH §10.2).
    assert_eq!(plain.etag.as_str(), comp.etag.as_str());
    assert!(matches!(
        comp.compression,
        CompressionDescriptor::Compressed { .. }
    ));
    // Compressible data shrinks on disk.
    assert!(comp.size_physical < comp.size_logical);
    assert_eq!(comp.size_logical, data.len() as u64);

    // Full read is transparent.
    assert_eq!(read_all(&store, &comp.storage_path, None).await, data);
    // A range starting mid-block near the end (the case block compression exists for).
    let range = ByteRange {
        offset: 5000,
        length: 1234,
    };
    assert_eq!(
        read_all(&store, &comp.storage_path, Some(range)).await,
        &data[5000..6234]
    );
    // A compressed read offers no zero-copy hint.
    assert!(
        store
            .open(&comp.storage_path, Some(range))
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
) -> Result<Vec<u8>, cairn_types::error::BlobError> {
    use futures_util::StreamExt;
    let handle = store.open_with_dek(path, range, dek).await?;
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
        read_all_dek(&store, &enc.storage_path, None, Some(dek))
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
        read_all_dek(&store, &enc.storage_path, Some(range), Some(dek))
            .await
            .unwrap(),
        &data[5000..6234]
    );
    // An encrypted blob never offers a zero-copy hint (the kernel cannot decrypt).
    assert!(
        store
            .open_with_dek(&enc.storage_path, None, Some(dek))
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
        read_all_dek(&store, &enc.storage_path, None, Some(dek))
            .await
            .unwrap(),
        data
    );
    // The wrong DEK fails authentication.
    let wrong = read_all_dek(&store, &enc.storage_path, None, Some([0x23u8; 32])).await;
    assert!(matches!(
        wrong,
        Err(cairn_types::error::BlobError::Corruption(_))
    ));
    // Opening an encrypted blob with no DEK at all fails fast.
    let none = store.open_with_dek(&enc.storage_path, None, None).await;
    assert!(matches!(
        none,
        Err(cairn_types::error::BlobError::Corruption(_))
    ));
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
    assert_eq!(read_all(&store, &staged.storage_path, None).await, data);
    assert_eq!(
        read_all_dek(&store, &staged.storage_path, None, None)
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
        .stage_part(&upload, 1, body(b"part-one-".to_vec()), 1 << 20)
        .await
        .unwrap();
    let p2 = store
        .stage_part(&upload, 2, body(b"part-two".to_vec()), 1 << 20)
        .await
        .unwrap();
    let refs = vec![
        PartRef {
            part_number: 1,
            storage_path: p1.storage_path.clone(),
            size: p1.size,
        },
        PartRef {
            part_number: 2,
            storage_path: p2.storage_path.clone(),
            size: p2.size,
        },
    ];
    let assembled = store
        .assemble(&b, &refs, opts(None, "text/plain"))
        .await
        .unwrap();
    assert_eq!(assembled.size_logical, (p1.size + p2.size));
    assert_eq!(
        read_all(&store, &assembled.storage_path, None).await,
        b"part-one-part-two"
    );
    store.delete_session(&upload).await.unwrap();
    store.delete_session(&upload).await.unwrap(); // idempotent
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
        .reconcile(&oracle, ReconcileOpts::default())
        .await
        .unwrap();
    assert_eq!(report.orphans_reclaimed, 1);
    // keep is still readable, orphan is gone.
    assert_eq!(read_all(&store, &keep.storage_path, None).await, b"keep");
    assert!(matches!(
        store.open(&orphan.storage_path, None).await,
        Err(BlobError::NotFound)
    ));
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
        .reconcile(&oracle, ReconcileOpts::default())
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
    assert!(store.open(&evil, None).await.is_err());
}
