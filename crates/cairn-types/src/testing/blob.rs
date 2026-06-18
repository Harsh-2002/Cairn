//! An in-memory [`BlobStore`] double. It models the storage *semantics* (opaque paths,
//! durable-on-return, idempotent delete, bounded reconciliation) without a filesystem or
//! compression; it computes the real plaintext MD5 so ETags are faithful.

use crate::blob::{
    BlobReadHandle, ByteRange, ContentRange, PartRef, ReconcileOpts, ReconcileReport, StageOptions,
    StagedBlob, StagedPart,
};
use crate::error::BlobError;
use crate::id::{BucketName, StoragePath, UploadId};
use crate::object::{CompressionDescriptor, ETag};
use crate::traits::{BlobStore, ReconcileOracle};
use bytes::Bytes;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// A stored blob: its plaintext bytes and the optional data-encryption key it was staged under.
/// The double models SSE-S3 semantics without real cryptography: a blob staged with a DEK can
/// only be reopened with the *same* DEK, so the wrong-key-fails property is faithful.
#[derive(Debug)]
struct StoredBlob {
    bytes: Arc<Vec<u8>>,
    dek: Option<[u8; 32]>,
}

type BlobMap = HashMap<String, StoredBlob>;
type PartMap = HashMap<(String, u16), Arc<Vec<u8>>>;

/// An in-memory blob store.
#[derive(Debug, Default)]
pub struct InMemoryBlobStore {
    blobs: Mutex<BlobMap>,
    parts: Mutex<PartMap>,
}

impl InMemoryBlobStore {
    /// A fresh empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of committed blobs (test introspection).
    #[must_use]
    pub fn blob_count(&self) -> usize {
        self.blobs.lock().unwrap().len()
    }

    /// Directly read a committed blob's (plaintext) bytes (test introspection).
    #[must_use]
    pub fn get_bytes(&self, path: &StoragePath) -> Option<Vec<u8>> {
        self.blobs
            .lock()
            .unwrap()
            .get(path.as_str())
            .map(|b| b.bytes.as_ref().clone())
    }

    async fn drain(mut body: crate::BodyStream, ceiling: u64) -> Result<Vec<u8>, BlobError> {
        let mut buf = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            if buf.len() as u64 + chunk.len() as u64 > ceiling {
                return Err(BlobError::SizeExceeded);
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }
}

#[async_trait::async_trait]
impl BlobStore for InMemoryBlobStore {
    async fn stage(
        &self,
        bucket: &BucketName,
        body: crate::BodyStream,
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        let buf = Self::drain(body, opts.size_ceiling).await?;
        // The MD5/ETag is computed over the plaintext, before any (modelled) encryption — exactly
        // as the real store computes it pre-transform — so the ETag is identical with or without a
        // DEK (ARCH §21.1, SSE-S3).
        let md5 = md5_hex(&buf);
        let path = StoragePath::generate(bucket);
        let len = buf.len() as u64;
        self.blobs.lock().unwrap().insert(
            path.as_str().to_owned(),
            StoredBlob {
                bytes: Arc::new(buf),
                dek: opts.encryption,
            },
        );
        Ok(StagedBlob {
            storage_path: path,
            size_logical: len,
            size_physical: len,
            etag: ETag::from_md5_hex(md5.clone()),
            md5_hex: md5,
            checksums: Vec::new(),
            compression: CompressionDescriptor::Uncompressed,
        })
    }

    async fn open_with_dek(
        &self,
        path: &StoragePath,
        range: Option<ByteRange>,
        dek: Option<[u8; 32]>,
        _compression: &CompressionDescriptor,
    ) -> Result<BlobReadHandle, BlobError> {
        // The in-memory double stores logical bytes directly (no CRNB container), so the stored
        // compression descriptor is irrelevant to reads here.
        let data = {
            let blobs = self.blobs.lock().unwrap();
            let stored = blobs.get(path.as_str()).ok_or(BlobError::NotFound)?;
            // Model SSE-S3: a blob staged under a DEK is readable only with the same DEK, and a
            // blob staged in the clear ignores any supplied DEK.
            if stored.dek != dek && stored.dek.is_some() {
                return Err(BlobError::Corruption(
                    "blob is encrypted; wrong or missing data-encryption key".into(),
                ));
            }
            stored.bytes.clone()
        };
        let total = data.len() as u64;
        let (slice, content_range, logical_len) = match range {
            Some(r) => {
                let start = r.offset.min(total);
                let end = r.offset.saturating_add(r.length).min(total);
                let bytes = Bytes::copy_from_slice(&data[start as usize..end as usize]);
                let cr = ContentRange {
                    start,
                    end: end.saturating_sub(1).max(start),
                    total,
                };
                (bytes, Some(cr), end - start)
            }
            None => (Bytes::copy_from_slice(&data), None, total),
        };
        let body: crate::BlobStream =
            Box::pin(futures_util::stream::once(async move { Ok(slice) }));
        Ok(BlobReadHandle {
            logical_len,
            content_range,
            body,
            zero_copy: None,
        })
    }

    async fn delete(&self, path: &StoragePath) -> Result<(), BlobError> {
        self.blobs.lock().unwrap().remove(path.as_str());
        Ok(())
    }

    async fn stage_part(
        &self,
        upload: &UploadId,
        part_number: u16,
        body: crate::BodyStream,
        size_ceiling: u64,
    ) -> Result<StagedPart, BlobError> {
        let buf = Self::drain(body, size_ceiling).await?;
        let md5 = md5_hex(&buf);
        let size = buf.len() as u64;
        let path = StoragePath::from_string(format!("{}/part-{}", upload, part_number));
        self.parts
            .lock()
            .unwrap()
            .insert((upload.as_str().to_owned(), part_number), Arc::new(buf));
        Ok(StagedPart {
            storage_path: path,
            size,
            md5_hex: md5,
        })
    }

    async fn assemble(
        &self,
        bucket: &BucketName,
        parts: &[PartRef],
        _opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        let mut buf = Vec::new();
        {
            let parts_map = self.parts.lock().unwrap();
            for p in parts {
                // The double keys parts by (upload, part_number) embedded in the path.
                let key = p.storage_path.as_str();
                let pn = key.rsplit('-').next().and_then(|n| n.parse::<u16>().ok());
                let upload = key.split('/').next().unwrap_or_default().to_owned();
                if let Some(pn) = pn {
                    if let Some(bytes) = parts_map.get(&(upload, pn)) {
                        buf.extend_from_slice(bytes);
                        continue;
                    }
                }
                return Err(BlobError::NotFound);
            }
        }
        let md5 = md5_hex(&buf);
        let path = StoragePath::generate(bucket);
        let len = buf.len() as u64;
        self.blobs.lock().unwrap().insert(
            path.as_str().to_owned(),
            StoredBlob {
                bytes: Arc::new(buf),
                dek: _opts.encryption,
            },
        );
        Ok(StagedBlob {
            storage_path: path,
            size_logical: len,
            size_physical: len,
            etag: ETag::from_md5_hex(md5.clone()),
            md5_hex: md5,
            checksums: Vec::new(),
            compression: CompressionDescriptor::Uncompressed,
        })
    }

    async fn delete_session(&self, upload: &UploadId) -> Result<(), BlobError> {
        self.parts
            .lock()
            .unwrap()
            .retain(|(u, _), _| u != upload.as_str());
        Ok(())
    }

    async fn reconcile(
        &self,
        oracle: &dyn ReconcileOracle,
        opts: ReconcileOpts,
    ) -> Result<ReconcileReport, BlobError> {
        let mut report = ReconcileReport::default();
        let paths: Vec<StoragePath> = {
            let blobs = self.blobs.lock().unwrap();
            blobs
                .keys()
                .map(|k| StoragePath::from_string(k.clone()))
                .collect()
        };
        report.blobs_scanned = paths.len() as u64;
        for batch in paths.chunks(opts.batch_size.max(1) as usize) {
            let live = oracle
                .live_blobs(batch)
                .await
                .map_err(|e| BlobError::Io(e.to_string()))?;
            for (path, is_live) in batch.iter().zip(live) {
                if !is_live {
                    self.blobs.lock().unwrap().remove(path.as_str());
                    report.orphans_reclaimed += 1;
                }
            }
        }
        Ok(report)
    }
}
