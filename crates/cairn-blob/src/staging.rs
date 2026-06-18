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
        /// When preallocation ran (a known-large write), the reserved length, so `commit` can
        /// advise the kernel to drop the written pages afterwards (ARCH §7.5). `None` skips it.
        release_len: Option<u64>,
    },
    /// The io_uring backend: file ops run on the dedicated io_uring executor.
    #[cfg(feature = "io-uring")]
    Uring(crate::uring::UringStaging),
}

impl Staging {
    /// Create the staging tmp file using the active backend. When `prealloc` is `Some(len)` and the
    /// length clears the hint threshold, the `tokio::fs` backend reserves blocks and advises
    /// sequential access up front (ARCH §7.5), all on the blocking pool in one hop.
    pub(crate) async fn create(
        staging_path: PathBuf,
        use_uring: bool,
        prealloc: Option<u64>,
    ) -> Result<Self, BlobError> {
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
        let release_len = prealloc.filter(|&n| n >= crate::raw_io::HINT_THRESHOLD);
        // Create the file and apply the placement hints in a single blocking hop, then wrap the
        // std handle for the async streamed write.
        let sp = staging_path.clone();
        let file = tokio::task::spawn_blocking(move || -> Result<std::fs::File, BlobError> {
            let file = std::fs::File::create(&sp).map_err(io_err)?;
            if let Some(len) = release_len {
                crate::raw_io::preallocate_sequential(&file, len);
            }
            Ok(file)
        })
        .await
        .map_err(|e| BlobError::Io(e.to_string()))??;
        Ok(Staging::Tokio {
            writer: BufWriter::new(tokio::fs::File::from_std(file)),
            staging_path,
            release_len,
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

    /// Commit the staged file durably into `final_path`, preserving the F-1 ordering up to the
    /// rename: fdatasync the file, then rename it in. The caller must have created the destination
    /// (bucket) directory beforehand, and must fsync that directory *after* this returns — through
    /// [`crate::commit::DirSyncCoalescer`], which coalesces concurrent same-directory PUTs into one
    /// fsync — before treating the blob as durable (ARCH §8.2).
    pub(crate) async fn commit(self, final_path: &Path) -> Result<(), BlobError> {
        match self {
            Staging::Tokio {
                writer,
                staging_path,
                release_len,
            } => {
                let mut writer = writer;
                writer.flush().await.map_err(io_err)?;
                let file = writer.into_inner();
                // 1) fdatasync the staged file, 2) rename it in. The destination-directory fsync is
                // the caller's coalesced step. `sync_data` (fdatasync) persists the bytes and the
                // size needed to read them back, skipping only the inode timestamps we never depend
                // on — one fewer metadata-journal write per PUT than `sync_all` (ARCH §8.2).
                file.sync_data().await.map_err(io_err)?;
                // For a known-large write, drop the just-written pages so a stream of bulk uploads
                // does not evict the page cache hot reads depend on (ARCH §7.5). Best-effort.
                if let Some(len) = release_len {
                    let std_file = file.into_std().await;
                    let _ = tokio::task::spawn_blocking(move || {
                        crate::raw_io::release_pages(&std_file, len);
                    })
                    .await;
                }
                tokio::fs::rename(&staging_path, final_path)
                    .await
                    .map_err(io_err)?;
                Ok(())
            }
            #[cfg(feature = "io-uring")]
            Staging::Uring(s) => s.commit(final_path.to_path_buf()).await,
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
                // fdatasync: the part's bytes and size must be durable for `assemble` to read it;
                // its timestamps are irrelevant, so skip the extra metadata flush (ARCH §8.2).
                writer.into_inner().sync_data().await.map_err(io_err)?;
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
