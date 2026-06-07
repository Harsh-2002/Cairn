//! The staging-write seam: a small backend-agnostic handle for the durable single-object write
//! path. Both the default `tokio::fs` backend and the optional io_uring backend (feature
//! `io-uring`) implement the same shape — create a staging tmp, stream physical bytes into it,
//! then either commit it durably (fsync file → rename into the bucket dir → fsync that dir; the
//! F-1 ordering, ARCH §8.2) or abort it (unlink the tmp). The shared streaming transform in
//! `write_staged` is written against this handle, so the compression/encryption/hashing logic is
//! identical on both paths and only the raw file syscalls differ.

use crate::io_err;
use cairn_types::error::BlobError;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncWriteExt, BufWriter};

/// A staging file open for writing, dispatching to the selected I/O backend. Construct with
/// [`Staging::create`], feed it with [`Staging::write_all`], then finish with either
/// [`Staging::commit`] (durable) or [`Staging::abort`] (discard).
pub(crate) enum Staging {
    /// The default backend: a buffered `tokio::fs` writer over the staging file.
    Tokio {
        writer: BufWriter<tokio::fs::File>,
        staging_path: PathBuf,
    },
    /// The io_uring backend: file ops run on the dedicated io_uring executor.
    #[cfg(feature = "io-uring")]
    Uring(crate::uring::UringStaging),
}

impl Staging {
    /// Create the staging tmp file using the active backend.
    pub(crate) async fn create(staging_path: PathBuf, use_uring: bool) -> Result<Self, BlobError> {
        if use_uring {
            #[cfg(feature = "io-uring")]
            {
                let s = crate::uring::UringStaging::create(staging_path).await?;
                return Ok(Staging::Uring(s));
            }
            #[cfg(not(feature = "io-uring"))]
            {
                // The flag can only be set when the feature is compiled in; this arm is dead, but
                // keeping it explicit means a stray `true` degrades to the safe default rather
                // than failing to compile.
                let _ = &staging_path;
            }
        }
        let file = tokio::fs::File::create(&staging_path)
            .await
            .map_err(io_err)?;
        Ok(Staging::Tokio {
            writer: BufWriter::new(file),
            staging_path,
        })
    }

    /// Append `buf` to the staging file. On the `tokio::fs` path this fills the `BufWriter`; on
    /// the io_uring path it dispatches a positional write to the executor and awaits it.
    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> Result<(), BlobError> {
        match self {
            Staging::Tokio { writer, .. } => writer.write_all(buf).await.map_err(io_err),
            #[cfg(feature = "io-uring")]
            Staging::Uring(s) => s.write_all(buf).await,
        }
    }

    /// Commit the staged file durably into `final_path` inside `bucket_dir`, preserving the F-1
    /// ordering: fsync the file, rename it in, then fsync the destination directory. The caller is
    /// responsible for having created `bucket_dir` (and fsynced its parent when newly created)
    /// *before* calling this, exactly as on the legacy path.
    pub(crate) async fn commit(
        self,
        final_path: &Path,
        bucket_dir: &Path,
    ) -> Result<(), BlobError> {
        match self {
            Staging::Tokio {
                writer,
                staging_path,
            } => {
                let mut writer = writer;
                writer.flush().await.map_err(io_err)?;
                let file = writer.into_inner();
                // 1) fsync the staged file, 2) rename it in, 3) fsync the destination directory.
                file.sync_all().await.map_err(io_err)?;
                tokio::fs::rename(&staging_path, final_path)
                    .await
                    .map_err(io_err)?;
                crate::fsync_dir(bucket_dir).await?;
                Ok(())
            }
            #[cfg(feature = "io-uring")]
            Staging::Uring(s) => {
                s.commit(final_path.to_path_buf(), bucket_dir.to_path_buf())
                    .await
            }
        }
    }

    /// Flush and fsync the staged file *in place* (no rename), leaving it where it was created.
    /// Used for multipart parts, which are durable intermediate artifacts that `assemble` later
    /// reads and which are not renamed into a bucket directory.
    pub(crate) async fn fsync_in_place(self) -> Result<(), BlobError> {
        match self {
            Staging::Tokio { writer, .. } => {
                let mut writer = writer;
                writer.flush().await.map_err(io_err)?;
                writer.into_inner().sync_all().await.map_err(io_err)?;
                Ok(())
            }
            #[cfg(feature = "io-uring")]
            Staging::Uring(s) => s.fsync_in_place().await,
        }
    }

    /// Discard the staged file (best-effort unlink). Used on a streaming failure before commit.
    pub(crate) async fn abort(self) {
        match self {
            Staging::Tokio { staging_path, .. } => {
                let _ = tokio::fs::remove_file(&staging_path).await;
            }
            #[cfg(feature = "io-uring")]
            Staging::Uring(s) => s.abort().await,
        }
    }
}
