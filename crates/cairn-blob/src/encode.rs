//! Bounded staging adapter for CRNB encoding. Index bytes are temporary input to the final blob,
//! never a separately committed artifact. Small indexes stay within one fixed buffer; larger ones
//! spill to an immediately unlinked file on the data filesystem, then stream into the final sink.

use crate::compress::BlockEncoder;
use crate::io_err;
use crate::staging::{Staging, UncommittedBlobCleanup};
use cairn_types::{SecretKey32, bucket::CompressionPolicy, error::BlobError};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};

const BATCH_BYTES: usize = 64 * 1024;

struct IndexSpool {
    dir: PathBuf,
    buffered: Vec<u8>,
    file: Option<BufWriter<tokio::fs::File>>,
    len: u64,
}

impl IndexSpool {
    fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_owned(),
            buffered: Vec::new(),
            file: None,
            len: 0,
        }
    }

    async fn append(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        if self.file.is_none() && self.buffered.len() + bytes.len() <= BATCH_BYTES {
            let needed = self.buffered.len() + bytes.len();
            if needed > self.buffered.capacity() {
                self.buffered.reserve_exact(needed - self.buffered.len());
            }
            self.buffered.extend_from_slice(bytes);
        } else {
            if self.file.is_none() {
                let path = self
                    .dir
                    .join(format!("{}.index.tmp", uuid::Uuid::new_v4().simple()));
                let file = tokio::task::spawn_blocking(move || {
                    // Arm after create_new: a collision must never delete somebody else's path.
                    // The guard and descriptor live in the blocking task, so cancellation cannot
                    // race creation. Unlink before returning: even detached later writes have no
                    // name to resurrect. A process crash between create/unlink leaves an ordinary
                    // .staging artifact for the mandatory startup reconciliation.
                    let file = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create_new(true)
                        .open(&path)
                        .map_err(io_err)?;
                    let mut cleanup = UncommittedBlobCleanup::staging_only(path.clone());
                    std::fs::remove_file(path).map_err(io_err)?;
                    cleanup.disarm();
                    Ok::<_, BlobError>(file)
                })
                .await
                .map_err(|e| BlobError::Io(e.to_string()))??;
                let mut writer =
                    BufWriter::with_capacity(BATCH_BYTES, tokio::fs::File::from_std(file));
                writer.write_all(&self.buffered).await.map_err(io_err)?;
                self.buffered = Vec::new();
                self.file = Some(writer);
            }
            self.file
                .as_mut()
                .expect("spool initialized")
                .write_all(bytes)
                .await
                .map_err(io_err)?;
        }
        self.len += bytes.len() as u64;
        Ok(())
    }

    async fn append_to(mut self, sink: &mut Staging) -> Result<u64, BlobError> {
        if let Some(mut writer) = self.file.take() {
            // Flush userspace bytes, then rewind. No durability barrier belongs to this scratch
            // file; the caller syncs the complete final blob after index + MAC + trailer append.
            writer.flush().await.map_err(io_err)?;
            let mut file = writer.into_inner();
            file.rewind().await.map_err(io_err)?;
            let mut buf = vec![0u8; BATCH_BYTES];
            let mut remaining = self.len;
            while remaining != 0 {
                let n = remaining.min(BATCH_BYTES as u64) as usize;
                file.read_exact(&mut buf[..n]).await.map_err(io_err)?;
                sink.write_all(&buf[..n]).await?;
                remaining -= n as u64;
            }
        } else {
            sink.write_all(&self.buffered).await?;
        }
        Ok(self.len)
    }
}

/// Owns the bounded encoder state and its index spool for one upload/assembly.
pub(crate) struct StagedEncoder {
    encoder: BlockEncoder,
    index: IndexSpool,
}

impl StagedEncoder {
    pub(crate) fn new(policy: CompressionPolicy, dek: Option<SecretKey32>, dir: &Path) -> Self {
        let encoder = match dek {
            Some(key) => BlockEncoder::new_encrypted(policy.algorithm, policy.block_size, key),
            None => BlockEncoder::new(policy.algorithm, policy.block_size),
        };
        Self {
            encoder,
            index: IndexSpool::new(dir),
        }
    }

    pub(crate) async fn feed(&mut self, data: &[u8], sink: &mut Staging) -> Result<u64, BlobError> {
        let mut physical = 0;
        // Never copy the caller's entire chunk into the encoder or aggregate its physical output.
        // A block may be larger than a batch; only that trusted block's buffer spans batches.
        for chunk in data.chunks(BATCH_BYTES) {
            let payload = self.encoder.feed(chunk)?;
            let index = self.encoder.take_index();
            self.index.append(&index).await?;
            sink.write_all(&payload).await?;
            physical += payload.len() as u64;
        }
        Ok(physical)
    }

    pub(crate) async fn finish(mut self, sink: &mut Staging) -> Result<u64, BlobError> {
        let tail = self.encoder.finish_parts()?;
        self.index.append(&tail.index).await?;
        sink.write_all(&tail.payload).await?;
        let index_len = self.index.append_to(sink).await?;
        sink.write_all(&tail.footer).await?;
        Ok(tail.payload.len() as u64 + index_len + tail.footer.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::{CompressedReader, CompressionAlgorithm};
    use cairn_types::{blob::BlobCipher, object::CompressionDescriptor};

    #[tokio::test]
    async fn spill_threshold_roundtrip_and_no_named_scratch() {
        for len in [
            0,
            BATCH_BYTES - 1,
            BATCH_BYTES,
            BATCH_BYTES + 1,
            3 * BATCH_BYTES + 7,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let data: Vec<u8> = (0..len).map(|n| (n % 251) as u8).collect();
            let mut spool = IndexSpool::new(dir.path());
            for chunk in data.chunks(733) {
                spool.append(chunk).await.unwrap();
                assert!(spool.buffered.capacity() <= BATCH_BYTES);
            }
            assert_eq!(spool.file.is_some(), len > BATCH_BYTES);
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
            let path = dir.path().join("final.tmp");
            let mut sink = Staging::create(path.clone(), false, None).await.unwrap();
            assert_eq!(spool.append_to(&mut sink).await.unwrap(), len as u64);
            sink.fsync_in_place().await.unwrap();
            assert_eq!(std::fs::read(path).unwrap(), data);
        }
    }

    #[tokio::test]
    async fn cancelled_spilled_writer_has_no_named_scratch() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = IndexSpool::new(dir.path());
        spool.append(&vec![1; BATCH_BYTES + 1]).await.unwrap();
        let (ready, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _spool = spool;
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn spool_create_and_read_errors_propagate_before_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut missing = IndexSpool::new(&dir.path().join("missing"));
        assert!(matches!(
            missing.append(&vec![1; BATCH_BYTES + 1]).await,
            Err(BlobError::Io(_))
        ));
        let mut truncated = IndexSpool::new(dir.path());
        truncated.append(&vec![2; BATCH_BYTES + 1]).await.unwrap();
        let writer = truncated.file.as_mut().unwrap();
        writer.flush().await.unwrap();
        writer.get_mut().set_len(1).await.unwrap();
        let mut sink = Staging::create(dir.path().join("output.tmp"), false, None)
            .await
            .unwrap();
        assert!(matches!(
            truncated.append_to(&mut sink).await,
            Err(BlobError::Io(_))
        ));
        sink.abort().await;
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn spool_enospc_is_out_of_space() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = IndexSpool::new(dir.path());
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open("/dev/full")
            .await
            .unwrap();
        spool.file = Some(BufWriter::with_capacity(BATCH_BYTES, file));
        // /dev/full supplies real ENOSPC without filling a filesystem. Tokio may surface it
        // on write or on the subsequent flush; either must prevent a successful finalization.
        let mut sink = Staging::create(dir.path().join("output.tmp"), false, None)
            .await
            .unwrap();
        let outcome = match spool.append(&vec![3; 2 * BATCH_BYTES]).await {
            Err(err) => Err(err),
            Ok(()) => spool.append_to(&mut sink).await,
        };
        assert!(matches!(outcome, Err(BlobError::OutOfSpace)));
        sink.abort().await;
    }

    #[tokio::test]
    async fn encoded_spill_roundtrips_large_caller_chunk_and_final_partial_block() {
        use aes_gcm::aead::KeyInit;
        for encrypted in [false, true] {
            let key = aes_gcm::Aes256Gcm::generate_key(&mut aes_gcm::aead::OsRng);
            let cipher = if encrypted {
                BlobCipher::AuthenticatedV3(SecretKey32::from_slice(&key).unwrap())
            } else {
                BlobCipher::KnownPlaintext
            };
            let dir = tempfile::tempdir().unwrap();
            let policy = CompressionPolicy {
                algorithm: CompressionAlgorithm::Zstd,
                block_size: 1024,
            };
            // >7,281 entries forces the actual encoded-index spill; about 8 MiB of logical data.
            let data = vec![42; 8 * 1024 * 1024 + 3];
            let path = dir.path().join("output.tmp");
            let mut sink = Staging::create(path.clone(), false, None).await.unwrap();
            let mut encoder = StagedEncoder::new(policy, cipher.dek(), dir.path());
            let physical = encoder.feed(&data, &mut sink).await.unwrap();
            assert!(encoder.index.file.is_some());
            assert!(encoder.encoder.take_index().is_empty());
            let physical = physical + encoder.finish(&mut sink).await.unwrap();
            sink.fsync_in_place().await.unwrap();
            assert_eq!(std::fs::metadata(&path).unwrap().len(), physical);
            let mut reader = CompressedReader::open_with_dek(
                std::fs::File::open(path).unwrap(),
                cipher,
                &CompressionDescriptor::Compressed {
                    algorithm: policy.algorithm,
                    block_size: policy.block_size,
                },
                data.len() as u64,
            )
            .unwrap();
            assert_eq!(reader.read_range(0, data.len() as u64).unwrap(), data);
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        }
    }
}
