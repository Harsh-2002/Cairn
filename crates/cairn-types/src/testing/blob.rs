//! An in-memory [`BlobStore`] double. It models the storage *semantics* (opaque paths,
//! durable-on-return, idempotent delete, bounded reconciliation) without a filesystem or
//! compression; it computes the real plaintext MD5 so ETags are faithful.

use crate::blob::{
    BlobCipher, BlobProbe, BlobReadHandle, ByteRange, ContentRange, PartRef, ReconcileOpts,
    ReconcileReport, StageOptions, StagedBlob, StagedPart,
};
use crate::error::BlobError;
use crate::id::{BucketName, StoragePath, UploadId};
use crate::object::{CompressionDescriptor, ETag};
use crate::secret::SecretKey32;
use crate::storage::{StorageCreationPermit, StorageWriteTarget};
use crate::traits::{BlobStore, ReconcileOracle};
use bytes::Bytes;
use futures_util::StreamExt;
use sha2::Digest;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Actual-operation ownership for one isolated fixture with no serving process/node lock.
/// Production constructors must instead retain the real exclusive node guard in their lease.
pub fn fixture_storage_io() -> crate::storage::io::StorageIoLease {
    crate::storage::io::StorageIoWatch::new(
        crate::storage::StorageToken::generate(),
        crate::storage::StorageToken::generate(),
        Arc::new(()),
    )
    .1
}

/// Synthetic exact cleanup receipt and matching I/O ownership for isolated blob fixtures.
/// This creates no metadata claim; production callers must use their Writer-issued receipt.
pub fn fixture_storage_cleanup(
    bucket: BucketName,
    path: StoragePath,
) -> (
    crate::storage::StorageCleanup,
    crate::storage::io::StorageIoLease,
) {
    use crate::storage::{StorageCleanup, StorageToken, io::StorageIoWatch};
    let cleanup = StorageCleanup {
        id: StorageToken::generate(),
        bucket,
        path,
        quota_debt_id: None,
        claim_token: StorageToken::generate(),
        generation: StorageToken::generate(),
        lease_until: crate::Timestamp(i64::MAX),
    };
    let (_, lease) =
        StorageIoWatch::new(cleanup.id.clone(), cleanup.generation.clone(), Arc::new(()));
    (cleanup, lease)
}

fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// A stored blob: its plaintext bytes and declared storage cipher. The double models SSE-S3
/// semantics without real cryptography: a blob staged with a DEK is a current authenticated-v3
/// object and can only be reopened with the same key and format expectation.
#[derive(Debug)]
struct StoredBlob {
    bytes: Arc<Vec<u8>>,
    cipher: BlobCipher,
}

type BlobMap = HashMap<String, StoredBlob>;
/// A staged part: its plaintext bytes and the optional DEK it was staged under. The double models
/// SSE-S3 part semantics with a key *label* (no real cryptography): `assemble` checks each
/// `PartRef.cipher` against the label a part was staged with, so wrong-key and wrong-format failures
/// are faithful. All real ciphertext/nonce assertions must run against the on-disk `LocalBlobStore`.
type PartMap = HashMap<String, (Arc<Vec<u8>>, BlobCipher)>;

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

    /// Number of staged multipart-part artifacts (test introspection).
    #[must_use]
    pub fn multipart_part_count(&self) -> usize {
        self.parts.lock().unwrap().len()
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
    fn read_memory_bound(
        &self,
        _compression: &CompressionDescriptor,
        _encrypted: bool,
        logical_len: u64,
    ) -> Result<crate::blob::ReadMemoryBound, BlobError> {
        // This double copies the requested range into a single frame; it has no CRNB decoder.
        Ok(crate::blob::ReadMemoryBound {
            buffer_bytes: logical_len.saturating_add(128 * 1024),
            max_frame_bytes: logical_len.max(1),
        })
    }

    async fn stage(
        &self,
        permit: StorageCreationPermit,
        body: crate::BodyStream,
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        let (plan, lease) = permit.into_parts();
        if !matches!(plan.target, StorageWriteTarget::Object { .. }) {
            return Err(BlobError::Io(
                "storage target is not an object write".into(),
            ));
        }
        let _operation = lease.try_child()?;
        let buf = Self::drain(body, opts.size_ceiling).await?;
        // The MD5/ETag is computed over the plaintext, before any (modelled) encryption — exactly
        // as the real store computes it pre-transform — so the ETag is identical with or without a
        // DEK (ARCH 21.1, SSE-S3).
        let md5 = md5_hex(&buf);
        let internal_sha256 = hex::encode(sha2::Sha256::digest(&buf));
        let path = plan
            .final_path()
            .map_err(|e| BlobError::Io(e.to_string()))?
            .clone();
        let len = buf.len() as u64;
        self.blobs.lock().unwrap().insert(
            path.as_str().to_owned(),
            StoredBlob {
                bytes: Arc::new(buf),
                cipher: match opts.encryption {
                    Some(dek) => BlobCipher::AuthenticatedV3(dek),
                    None => BlobCipher::KnownPlaintext,
                },
            },
        );
        Ok(StagedBlob {
            storage_path: path,
            size_logical: len,
            size_physical: len,
            etag: ETag::from_md5_hex(md5.clone()),
            md5_hex: md5,
            checksums: Vec::new(),
            internal_sha256,
            compression: CompressionDescriptor::Uncompressed,
        })
    }

    async fn open_raw(
        &self,
        path: &StoragePath,
        range: Option<ByteRange>,
        cipher: BlobCipher,
        _compression: &CompressionDescriptor,
        expected_logical_len: u64,
    ) -> Result<BlobReadHandle, BlobError> {
        // The in-memory double stores logical bytes directly (no CRNB container), so the stored
        // compression descriptor is irrelevant to reads here.
        let data = {
            let blobs = self.blobs.lock().unwrap();
            let stored = blobs.get(path.as_str()).ok_or(BlobError::NotFound)?;
            // Model SSE-S3: a blob staged under a DEK is readable only with the same DEK, and a
            // blob staged in the clear ignores any supplied DEK. A KnownPlaintext open of an
            // encrypted blob fails closed here — never yields the plaintext bytes.
            if stored.cipher != cipher {
                return Err(BlobError::Corruption(
                    "blob cipher or format does not match stored metadata".into(),
                ));
            }
            stored.bytes.clone()
        };
        let total = data.len() as u64;
        if total != expected_logical_len {
            return Err(BlobError::Corruption(
                "blob logical length does not match trusted metadata".into(),
            ));
        }
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

    async fn probe(&self, path: &StoragePath) -> Result<BlobProbe, BlobError> {
        // Presence + basic framing, no DEK, no decrypt: an encrypted blob probes present. The
        // double stores plaintext bytes directly, so the stored length IS the physical length
        // (there is no CRNB container here). Absence is NotFound.
        let blobs = self.blobs.lock().unwrap();
        let stored = blobs.get(path.as_str()).ok_or(BlobError::NotFound)?;
        Ok(BlobProbe {
            physical_len: stored.bytes.len() as u64,
        })
    }

    async fn confirm_storage_quiescence(
        &self,
        plan: &crate::storage::StorageWritePlan,
        lease: crate::storage::io::StorageIoLease,
    ) -> Result<(), BlobError> {
        plan.validate()
            .map_err(|error| BlobError::Io(error.to_string()))?;
        let _operation = lease.try_child()?;
        if !lease.owns(&plan.attempt, &plan.generation) {
            return Err(BlobError::Io("storage recovery ownership mismatch".into()));
        }
        Ok(())
    }

    async fn cleanup_storage(
        &self,
        cleanup: &crate::storage::StorageCleanup,
        lease: crate::storage::io::StorageIoLease,
    ) -> Result<(), BlobError> {
        crate::storage::validate_storage_path(&cleanup.bucket, &cleanup.path)
            .map_err(|error| BlobError::Io(error.to_string()))?;
        let _operation = lease.try_child()?;
        if !lease.owns(&cleanup.id, &cleanup.generation) {
            return Err(BlobError::Io("storage cleanup ownership mismatch".into()));
        }
        self.blobs.lock().unwrap().remove(cleanup.path.as_str());
        self.parts.lock().unwrap().remove(cleanup.path.as_str());
        Ok(())
    }

    async fn stage_part(
        &self,
        permit: StorageCreationPermit,
        body: crate::BodyStream,
        _checksums: crate::object::ChecksumSet,
        size_ceiling: u64,
        encryption: Option<SecretKey32>,
    ) -> Result<StagedPart, BlobError> {
        let (plan, lease) = permit.into_parts();
        if !matches!(plan.target, StorageWriteTarget::Part { .. }) {
            return Err(BlobError::Io("storage target is not a part write".into()));
        }
        let _operation = lease.try_child()?;
        let buf = Self::drain(body, size_ceiling).await?;
        let md5 = md5_hex(&buf);
        let size = buf.len() as u64;
        let path = plan
            .final_path()
            .map_err(|e| BlobError::Io(e.to_string()))?
            .clone();
        self.parts.lock().unwrap().insert(
            path.as_str().to_owned(),
            (
                Arc::new(buf),
                match encryption {
                    Some(dek) => BlobCipher::AuthenticatedV3(dek),
                    None => BlobCipher::KnownPlaintext,
                },
            ),
        );
        // Like `stage`/`assemble` here, the double models storage semantics without computing the
        // supplementary checksums (no hash engine in `cairn-types`); it returns the faithful MD5 and
        // an empty checksum set, so callers exercise the field's plumbing without a real digest.
        Ok(StagedPart {
            storage_path: path,
            size,
            md5_hex: md5,
            checksums: Vec::new(),
        })
    }

    async fn assemble(
        &self,
        permit: StorageCreationPermit,
        parts: &[PartRef],
        _opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        let (plan, lease) = permit.into_parts();
        if !matches!(plan.target, StorageWriteTarget::Completion { .. }) {
            return Err(BlobError::Io(
                "storage target is not multipart assembly".into(),
            ));
        }
        let _operation = lease.try_child()?;
        let mut buf = Vec::new();
        {
            let parts_map = self.parts.lock().unwrap();
            for p in parts {
                if let Some((bytes, staged_dek)) = parts_map.get(p.storage_path.as_str()) {
                    // Model the decrypt-on-read: a part staged under a DEK is readable only with
                    // the same DEK (mirrors `open_raw`); a wrong/missing key fails closed.
                    if staged_dek != &p.cipher {
                        return Err(BlobError::Corruption(
                            "part cipher or format does not match staged metadata".into(),
                        ));
                    }
                    buf.extend_from_slice(bytes);
                    continue;
                }
                return Err(BlobError::NotFound);
            }
        }
        let md5 = md5_hex(&buf);
        let internal_sha256 = hex::encode(sha2::Sha256::digest(&buf));
        let path = plan
            .final_path()
            .map_err(|e| BlobError::Io(e.to_string()))?
            .clone();
        let len = buf.len() as u64;
        self.blobs.lock().unwrap().insert(
            path.as_str().to_owned(),
            StoredBlob {
                bytes: Arc::new(buf),
                cipher: match _opts.encryption {
                    Some(dek) => BlobCipher::AuthenticatedV3(dek),
                    None => BlobCipher::KnownPlaintext,
                },
            },
        );
        Ok(StagedBlob {
            storage_path: path,
            size_logical: len,
            size_physical: len,
            etag: ETag::from_md5_hex(md5.clone()),
            md5_hex: md5,
            checksums: Vec::new(),
            internal_sha256,
            compression: CompressionDescriptor::Uncompressed,
        })
    }

    async fn reconcile(
        &self,
        oracle: &dyn ReconcileOracle,
        opts: ReconcileOpts,
        _lease: crate::storage::io::StorageIoLease,
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
        let parts: Vec<StoragePath> = self
            .parts
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .map(StoragePath::from_string)
            .collect();
        for batch in parts.chunks(opts.batch_size.max(1) as usize) {
            let live = oracle
                .live_multipart_parts(batch)
                .await
                .map_err(|error| BlobError::Io(error.to_string()))?;
            for (path, is_live) in batch.iter().zip(live) {
                let upload = path.as_str().split('/').nth(2).unwrap_or_default();
                let session_live = oracle
                    .live_session(&UploadId::from_string(upload.to_owned()))
                    .await
                    .map_err(|error| BlobError::Io(error.to_string()))?;
                if !session_live || !is_live {
                    self.parts.lock().unwrap().remove(path.as_str());
                    report.staging_cleaned += 1;
                }
            }
        }
        Ok(report)
    }
}

/// Explicit blob-only fixture setup. These helpers model the Writer receipt for tests and
/// microbenchmarks that have no metadata engine; production handlers use `BlobStore` directly.
#[async_trait::async_trait]
pub trait FixtureBlobStore: BlobStore {
    async fn reconcile_fixture(
        &self,
        oracle: &dyn ReconcileOracle,
        opts: ReconcileOpts,
    ) -> Result<ReconcileReport, BlobError> {
        let (_watch, lease) = crate::storage::io::StorageIoWatch::new(
            crate::storage::StorageToken::generate(),
            crate::storage::StorageToken::generate(),
            Arc::new(()),
        );
        self.reconcile(oracle, opts, lease).await
    }

    async fn stage_fixture(
        &self,
        bucket: &BucketName,
        body: crate::BodyStream,
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        self.stage(
            fixture_permit(
                bucket.clone(),
                StorageWriteTarget::Object {
                    key: crate::ObjectKey::parse("fixture").expect("valid fixture key"),
                    version_id: crate::VersionId::null(),
                    row_id: crate::storage::StorageToken::generate().as_str().to_owned(),
                },
            )?,
            body,
            opts,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_part_fixture(
        &self,
        upload: &UploadId,
        part_number: u16,
        attempt_id: &str,
        body: crate::BodyStream,
        checksums: crate::object::ChecksumSet,
        size_ceiling: u64,
        encryption: Option<SecretKey32>,
    ) -> Result<StagedPart, BlobError> {
        self.stage_part(
            fixture_permit(
                BucketName::parse("fixture-bucket").expect("valid bucket"),
                StorageWriteTarget::Part {
                    upload_id: upload.clone(),
                    part_number,
                    reservation_id: attempt_id.to_owned(),
                },
            )?,
            body,
            checksums,
            size_ceiling,
            encryption,
        )
        .await
    }

    async fn assemble_fixture(
        &self,
        bucket: &BucketName,
        parts: &[PartRef],
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        let upload_id = parts
            .first()
            .and_then(|part| part.storage_path.as_str().split('/').nth(2))
            .map(|id| UploadId::from_string(id.to_owned()))
            .unwrap_or_else(UploadId::generate);
        self.assemble(
            fixture_permit(
                bucket.clone(),
                StorageWriteTarget::Completion {
                    upload_id,
                    claim_token: crate::storage::StorageToken::generate().as_str().to_owned(),
                    key: crate::ObjectKey::parse("fixture").expect("valid fixture key"),
                    version_id: crate::VersionId::null(),
                    row_id: crate::storage::StorageToken::generate().as_str().to_owned(),
                },
            )?,
            parts,
            opts,
        )
        .await
    }
}

impl<T: BlobStore + ?Sized> FixtureBlobStore for T {}

fn fixture_permit(
    bucket: BucketName,
    target: StorageWriteTarget,
) -> Result<StorageCreationPermit, BlobError> {
    use crate::storage::{PlannedStorageWrite, StorageAdmission, StorageToken, io::StorageIoWatch};
    let planned = PlannedStorageWrite::new(bucket, StorageToken::generate(), target)
        .map_err(|error| BlobError::Io(error.to_string()))?;
    let plan = planned.plan().clone();
    let (_watch, lease) =
        StorageIoWatch::new(plan.attempt.clone(), plan.generation.clone(), Arc::new(()));
    planned.admit(StorageAdmission::Granted(Box::new(plan)), lease)
}
