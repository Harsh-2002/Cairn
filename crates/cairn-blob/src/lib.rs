//! `cairn-blob` — the local-filesystem [`BlobStore`]. This is the ONLY crate that performs
//! filesystem syscalls, and it owns the durable commit sequence (ARCH §8.2): stream to a
//! staging file, fsync the file, rename it into the per-bucket directory, fsync that
//! directory (the F-1 fix), and only then return — so a committed blob is durable before any
//! metadata references it. Object bytes live under opaque identifiers, never under the key, so
//! key-based path traversal is structurally impossible.

#![forbid(unsafe_code)]

mod compress;
mod hash;

use crate::compress::{BlockEncoder, CompressedReader, is_precompressed};
use crate::hash::Hashers;
use async_trait::async_trait;
use bytes::Bytes;
use cairn_types::blob::{
    BlobReadHandle, ByteRange, ContentRange, PartRef, ReconcileOpts, ReconcileReport, StageOptions,
    StagedBlob, StagedPart, ZeroCopyRead,
};
use cairn_types::error::BlobError;
use cairn_types::id::{BucketName, StoragePath, UploadId};
use cairn_types::object::{CompressionDescriptor, ETag};
use cairn_types::traits::{BlobStore, ReconcileOracle};
use futures_util::StreamExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufWriter};

const STAGING: &str = ".staging";
const READ_CHUNK: usize = 64 * 1024;

fn io_err(e: std::io::Error) -> BlobError {
    if e.kind() == std::io::ErrorKind::StorageFull || e.raw_os_error() == Some(28) {
        BlobError::OutOfSpace
    } else {
        BlobError::Io(e.to_string())
    }
}

/// The local-filesystem blob store rooted at one data directory. The database, the staging
/// area, and the per-bucket directories must all share this filesystem so atomic rename works.
#[derive(Debug, Clone)]
pub struct LocalBlobStore {
    data_root: Arc<PathBuf>,
}

impl LocalBlobStore {
    /// Open (creating the staging area) a blob store rooted at `data_root`.
    ///
    /// # Errors
    /// Returns a [`BlobError`] if the staging directory cannot be created.
    pub async fn open(data_root: impl Into<PathBuf>) -> Result<Self, BlobError> {
        let data_root = data_root.into();
        tokio::fs::create_dir_all(data_root.join(STAGING).join("multipart"))
            .await
            .map_err(io_err)?;
        Ok(Self {
            data_root: Arc::new(data_root),
        })
    }

    fn resolve(&self, sp: &StoragePath) -> Result<PathBuf, BlobError> {
        let rel = Path::new(sp.as_str());
        if rel.is_absolute()
            || rel.components().any(|c| {
                matches!(
                    c,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(BlobError::Io("unsafe storage path".into()));
        }
        Ok(self.data_root.join(rel))
    }
}

async fn fsync_dir(dir: &Path) -> Result<(), BlobError> {
    let d = tokio::fs::File::open(dir).await.map_err(io_err)?;
    d.sync_all().await.map_err(io_err)?;
    Ok(())
}

/// Detect the compressed format by its self-describing trailer magic.
async fn is_compressed_blob(path: &Path) -> Result<bool, BlobError> {
    let meta = tokio::fs::metadata(path).await.map_err(io_err)?;
    if meta.len() < 34 {
        return Ok(false);
    }
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(&path).map_err(io_err)?;
        f.seek(SeekFrom::End(-34)).map_err(io_err)?;
        let mut magic = [0u8; 4];
        f.read_exact(&mut magic).map_err(io_err)?;
        Ok(&magic == b"CRNB")
    })
    .await
    .map_err(|e| BlobError::Io(e.to_string()))?
}

/// Stream a body into a staging file, applying compression and hashing in one pass.
async fn write_staged(
    file: &mut BufWriter<tokio::fs::File>,
    mut body: cairn_types::BodyStream,
    opts: &StageOptions,
) -> Result<
    (
        u64,
        u64,
        String,
        Vec<cairn_types::object::ChecksumValue>,
        CompressionDescriptor,
    ),
    BlobError,
> {
    let compress = match opts.compression {
        Some(pol) if !is_precompressed(&opts.content_type) => Some(pol),
        _ => None,
    };
    let mut hashers = Hashers::new(&opts.extra_checksums);
    let mut logical: u64 = 0;
    let mut physical: u64 = 0;

    if let Some(pol) = compress {
        let mut enc = BlockEncoder::new(pol.algorithm, pol.block_size);
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            logical += chunk.len() as u64;
            if logical > opts.size_ceiling {
                return Err(BlobError::SizeExceeded);
            }
            hashers.update(&chunk);
            let phys = enc.feed(&chunk);
            file.write_all(&phys).await.map_err(io_err)?;
            physical += phys.len() as u64;
        }
        let tail = enc.finish();
        file.write_all(&tail).await.map_err(io_err)?;
        physical += tail.len() as u64;
        let (md5, checks) = hashers.finalize();
        Ok((
            logical,
            physical,
            md5,
            checks,
            CompressionDescriptor::Compressed {
                algorithm: pol.algorithm,
                block_size: pol.block_size,
            },
        ))
    } else {
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            logical += chunk.len() as u64;
            if logical > opts.size_ceiling {
                return Err(BlobError::SizeExceeded);
            }
            hashers.update(&chunk);
            file.write_all(&chunk).await.map_err(io_err)?;
            physical += chunk.len() as u64;
        }
        let (md5, checks) = hashers.finalize();
        Ok((
            logical,
            physical,
            md5,
            checks,
            CompressionDescriptor::Uncompressed,
        ))
    }
}

/// Stream a read of `[offset, offset+len)` logical bytes from a blob file, decompressing only
/// the overlapping blocks. Runs the blocking file work off the reactor and yields chunks.
fn read_stream(path: PathBuf, compressed: bool, offset: u64, len: u64) -> cairn_types::BlobStream {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, BlobError>>(4);
    tokio::task::spawn_blocking(move || {
        let result = (|| -> Result<(), BlobError> {
            use std::io::{Read, Seek, SeekFrom};
            if compressed {
                let f = std::fs::File::open(&path).map_err(io_err)?;
                let mut reader = CompressedReader::open(f)?;
                let bs = reader.block_size();
                let end = offset.saturating_add(len).min(reader.logical_len());
                if bs == 0 || offset >= end {
                    return Ok(());
                }
                let first = offset / bs;
                let last = (end - 1) / bs;
                for b in first..=last {
                    let bstart = b * bs;
                    let lo = offset.max(bstart);
                    let hi = end.min(bstart + bs);
                    let data = reader.read_range(lo, hi - lo)?;
                    if !data.is_empty() && tx.blocking_send(Ok(Bytes::from(data))).is_err() {
                        return Ok(());
                    }
                }
            } else {
                let mut f = std::fs::File::open(&path).map_err(io_err)?;
                f.seek(SeekFrom::Start(offset)).map_err(io_err)?;
                let mut remaining = len;
                let mut buf = vec![0u8; READ_CHUNK];
                while remaining > 0 {
                    let want = (remaining as usize).min(READ_CHUNK);
                    let n = f.read(&mut buf[..want]).map_err(io_err)?;
                    if n == 0 {
                        break;
                    }
                    if tx
                        .blocking_send(Ok(Bytes::copy_from_slice(&buf[..n])))
                        .is_err()
                    {
                        return Ok(());
                    }
                    remaining -= n as u64;
                }
            }
            Ok(())
        })();
        if let Err(e) = result {
            let _ = tx.blocking_send(Err(e));
        }
    });
    Box::pin(futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

#[async_trait]
impl BlobStore for LocalBlobStore {
    async fn stage(
        &self,
        bucket: &BucketName,
        body: cairn_types::BodyStream,
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let staging = self.data_root.join(STAGING).join(format!("{id}.tmp"));
        let bucket_dir = self.data_root.join(bucket.as_str());
        let final_path = bucket_dir.join(&id);
        let storage_path = StoragePath::from_string(format!("{}/{}", bucket.as_str(), id));

        let file = tokio::fs::File::create(&staging).await.map_err(io_err)?;
        let mut writer = BufWriter::new(file);
        let outcome = write_staged(&mut writer, body, &opts).await;
        let (logical, physical, md5, checksums, descriptor) = match outcome {
            Ok(v) => v,
            Err(e) => {
                let _ = tokio::fs::remove_file(&staging).await;
                return Err(e);
            }
        };
        writer.flush().await.map_err(io_err)?;
        let file = writer.into_inner();
        // 1) fsync the staged file, 2) rename it in, 3) fsync the destination directory.
        file.sync_all().await.map_err(io_err)?;
        tokio::fs::create_dir_all(&bucket_dir)
            .await
            .map_err(io_err)?;
        tokio::fs::rename(&staging, &final_path)
            .await
            .map_err(io_err)?;
        fsync_dir(&bucket_dir).await?;
        // The crash window the durability ordering protects: the blob is now durable but no
        // metadata row references it yet. A crash here leaves an orphan that reconcile reclaims.
        fail::fail_point!("blob_after_durable");

        Ok(StagedBlob {
            storage_path,
            size_logical: logical,
            size_physical: physical,
            etag: ETag::from_md5_hex(md5.clone()),
            md5_hex: md5,
            checksums,
            compression: descriptor,
        })
    }

    async fn open(
        &self,
        path: &StoragePath,
        range: Option<ByteRange>,
    ) -> Result<BlobReadHandle, BlobError> {
        let file_path = self.resolve(path)?;
        if !tokio::fs::try_exists(&file_path).await.map_err(io_err)? {
            return Err(BlobError::NotFound);
        }
        let compressed = is_compressed_blob(&file_path).await?;
        let logical_len = if compressed {
            let p = file_path.clone();
            tokio::task::spawn_blocking(move || {
                let f = std::fs::File::open(&p).map_err(io_err)?;
                CompressedReader::open(f).map(|r| r.logical_len())
            })
            .await
            .map_err(|e| BlobError::Io(e.to_string()))??
        } else {
            tokio::fs::metadata(&file_path).await.map_err(io_err)?.len()
        };

        let (offset, len, content_range) = match range {
            Some(r) => {
                let offset = r.offset.min(logical_len);
                let len = r.length.min(logical_len - offset);
                let cr = ContentRange {
                    start: offset,
                    end: (offset + len).saturating_sub(1).max(offset),
                    total: logical_len,
                };
                (offset, len, Some(cr))
            }
            None => (0, logical_len, None),
        };

        let body = read_stream(file_path.clone(), compressed, offset, len);
        // Uncompressed, plaintext blobs may take the kernel file-to-socket fast path.
        let zero_copy = if compressed {
            None
        } else {
            match std::fs::File::open(&file_path) {
                Ok(f) => Some(ZeroCopyRead {
                    file: Arc::new(f),
                    offset,
                    len,
                }),
                Err(_) => None,
            }
        };

        Ok(BlobReadHandle {
            logical_len: len,
            content_range,
            body,
            zero_copy,
        })
    }

    async fn delete(&self, path: &StoragePath) -> Result<(), BlobError> {
        let file_path = self.resolve(path)?;
        match tokio::fs::remove_file(&file_path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err(e)),
        }
    }

    async fn stage_part(
        &self,
        upload: &UploadId,
        part_number: u16,
        body: cairn_types::BodyStream,
        size_ceiling: u64,
    ) -> Result<StagedPart, BlobError> {
        let dir = self
            .data_root
            .join(STAGING)
            .join("multipart")
            .join(upload.as_str());
        tokio::fs::create_dir_all(&dir).await.map_err(io_err)?;
        let id = format!("{part_number:05}-{}", uuid::Uuid::new_v4().simple());
        let path = dir.join(&id);
        let opts = StageOptions {
            compression: None,
            extra_checksums: cairn_types::object::ChecksumSet::none(),
            size_ceiling,
            content_type: String::new(),
        };
        let file = tokio::fs::File::create(&path).await.map_err(io_err)?;
        let mut writer = BufWriter::new(file);
        let (logical, _phys, md5, _checks, _desc) =
            match write_staged(&mut writer, body, &opts).await {
                Ok(v) => v,
                Err(e) => {
                    let _ = tokio::fs::remove_file(&path).await;
                    return Err(e);
                }
            };
        writer.flush().await.map_err(io_err)?;
        writer.into_inner().sync_all().await.map_err(io_err)?;
        Ok(StagedPart {
            storage_path: StoragePath::from_string(format!(
                "{}/multipart/{}/{}",
                STAGING,
                upload.as_str(),
                id
            )),
            size: logical,
            md5_hex: md5,
        })
    }

    async fn assemble(
        &self,
        bucket: &BucketName,
        parts: &[PartRef],
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let staging = self.data_root.join(STAGING).join(format!("{id}.tmp"));
        let bucket_dir = self.data_root.join(bucket.as_str());
        let final_path = bucket_dir.join(&id);
        let storage_path = StoragePath::from_string(format!("{}/{}", bucket.as_str(), id));

        let compress = match opts.compression {
            Some(pol) if !is_precompressed(&opts.content_type) => Some(pol),
            _ => None,
        };
        let file = tokio::fs::File::create(&staging).await.map_err(io_err)?;
        let mut writer = BufWriter::new(file);
        use md5::Digest;
        let mut hasher = md5::Md5::new();
        let mut logical: u64 = 0;
        let mut physical: u64 = 0;
        let mut enc = compress.map(|p| BlockEncoder::new(p.algorithm, p.block_size));

        for part in parts {
            let part_path = self.resolve(&part.storage_path)?;
            let mut f = tokio::fs::File::open(&part_path)
                .await
                .map_err(|_| BlobError::NotFound)?;
            let mut buf = vec![0u8; READ_CHUNK];
            use tokio::io::AsyncReadExt;
            loop {
                let n = f.read(&mut buf).await.map_err(io_err)?;
                if n == 0 {
                    break;
                }
                logical += n as u64;
                hasher.update(&buf[..n]);
                match &mut enc {
                    Some(e) => {
                        let phys = e.feed(&buf[..n]);
                        writer.write_all(&phys).await.map_err(io_err)?;
                        physical += phys.len() as u64;
                    }
                    None => {
                        writer.write_all(&buf[..n]).await.map_err(io_err)?;
                        physical += n as u64;
                    }
                }
            }
        }
        let descriptor = if let Some(e) = enc {
            let tail = e.finish();
            writer.write_all(&tail).await.map_err(io_err)?;
            physical += tail.len() as u64;
            let pol = compress.expect("encoder implies a policy");
            CompressionDescriptor::Compressed {
                algorithm: pol.algorithm,
                block_size: pol.block_size,
            }
        } else {
            CompressionDescriptor::Uncompressed
        };

        writer.flush().await.map_err(io_err)?;
        let file = writer.into_inner();
        file.sync_all().await.map_err(io_err)?;
        tokio::fs::create_dir_all(&bucket_dir)
            .await
            .map_err(io_err)?;
        tokio::fs::rename(&staging, &final_path)
            .await
            .map_err(io_err)?;
        fsync_dir(&bucket_dir).await?;
        fail::fail_point!("blob_after_assemble");

        let md5_hex = hex::encode(hasher.finalize());
        Ok(StagedBlob {
            storage_path,
            size_logical: logical,
            size_physical: physical,
            etag: ETag::from_md5_hex(md5_hex.clone()),
            md5_hex,
            checksums: Vec::new(),
            compression: descriptor,
        })
    }

    async fn delete_session(&self, upload: &UploadId) -> Result<(), BlobError> {
        let dir = self
            .data_root
            .join(STAGING)
            .join("multipart")
            .join(upload.as_str());
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err(e)),
        }
    }

    async fn reconcile(
        &self,
        oracle: &dyn ReconcileOracle,
        opts: ReconcileOpts,
    ) -> Result<ReconcileReport, BlobError> {
        let mut report = ReconcileReport::default();
        let mut entries = tokio::fs::read_dir(&*self.data_root)
            .await
            .map_err(io_err)?;
        while let Some(entry) = entries.next_entry().await.map_err(io_err)? {
            let name = entry.file_name();
            let name = name.to_string_lossy().to_string();
            let ft = entry.file_type().await.map_err(io_err)?;
            if !ft.is_dir() {
                continue;
            }
            if name == STAGING {
                reconcile_staging(&entry.path(), oracle, &mut report).await?;
                continue;
            }
            // A per-bucket directory: reclaim blobs no metadata row references.
            reconcile_bucket(
                &entry.path(),
                &name,
                oracle,
                opts.batch_size.max(1),
                &mut report,
            )
            .await?;
        }
        Ok(report)
    }
}

async fn reconcile_bucket(
    dir: &Path,
    bucket: &str,
    oracle: &dyn ReconcileOracle,
    batch_size: u32,
    report: &mut ReconcileReport,
) -> Result<(), BlobError> {
    let mut rd = tokio::fs::read_dir(dir).await.map_err(io_err)?;
    let mut batch: Vec<(PathBuf, StoragePath)> = Vec::new();
    loop {
        let next = rd.next_entry().await.map_err(io_err)?;
        let is_end = next.is_none();
        if let Some(entry) = next {
            if entry.file_type().await.map_err(io_err)?.is_file() {
                let file = entry.file_name().to_string_lossy().to_string();
                let sp = StoragePath::from_string(format!("{bucket}/{file}"));
                batch.push((entry.path(), sp));
            }
        }
        if (is_end || batch.len() >= batch_size as usize) && !batch.is_empty() {
            report.blobs_scanned += batch.len() as u64;
            let paths: Vec<StoragePath> = batch.iter().map(|(_, sp)| sp.clone()).collect();
            let live = oracle
                .live_blobs(&paths)
                .await
                .map_err(|e| BlobError::Io(e.to_string()))?;
            for ((path, _), is_live) in batch.drain(..).zip(live) {
                if !is_live {
                    if tokio::fs::remove_file(&path).await.is_ok() {
                        report.orphans_reclaimed += 1;
                    } else {
                        report.errors += 1;
                    }
                }
            }
        }
        if is_end {
            break;
        }
    }
    Ok(())
}

async fn reconcile_staging(
    staging: &Path,
    oracle: &dyn ReconcileOracle,
    report: &mut ReconcileReport,
) -> Result<(), BlobError> {
    let mut rd = match tokio::fs::read_dir(staging).await {
        Ok(rd) => rd,
        Err(_) => return Ok(()),
    };
    while let Some(entry) = rd.next_entry().await.map_err(io_err)? {
        let name = entry.file_name().to_string_lossy().to_string();
        let ft = entry.file_type().await.map_err(io_err)?;
        if ft.is_file() {
            // A leftover single-part staging artifact from a crash.
            if tokio::fs::remove_file(entry.path()).await.is_ok() {
                report.staging_cleaned += 1;
            }
        } else if ft.is_dir() && name == "multipart" {
            let mut sessions = tokio::fs::read_dir(entry.path()).await.map_err(io_err)?;
            while let Some(s) = sessions.next_entry().await.map_err(io_err)? {
                let upload = UploadId::from_string(s.file_name().to_string_lossy().to_string());
                let live = oracle
                    .live_session(&upload)
                    .await
                    .map_err(|e| BlobError::Io(e.to_string()))?;
                if !live && tokio::fs::remove_dir_all(s.path()).await.is_ok() {
                    report.sessions_cleaned += 1;
                }
            }
        }
    }
    Ok(())
}
