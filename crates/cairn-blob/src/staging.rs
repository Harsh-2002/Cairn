//! The staging-write seam: a small backend-agnostic handle for the durable single-object write
//! path. Both the default `tokio::fs` backend and the optional io_uring backend (feature
//! `io-uring`) implement the same shape — create a staging tmp, stream physical bytes into it,
//! then either commit it durably (fsync file → rename into the bucket dir → fsync that dir; the
//! F-1 ordering, ARCH 8.2) or abort it (unlink the tmp). The shared streaming transform in
//! `write_staged` is written against this handle, so the compression/encryption/hashing logic is
//! identical on both paths and only the raw file syscalls differ.

use crate::io_err;
use cairn_types::error::BlobError;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncWriteExt, BufWriter};

/// Owns the provisional filesystem names for a blob until the caller has received its
/// [`cairn_types::blob::StagedBlob`].
///
/// Async filesystem operations may continue on their backend after the request future that
/// awaited them is dropped. Keeping an armed copy in both the request and those detached handoffs
/// closes both sides of that race: whichever side observes cancellation after creating/renaming
/// the file unlinks it. The synchronous unlink is deliberate and restricted to this exceptional
/// `Drop` path; POSIX permits unlinking an open file, so a canceled writer cannot keep a named,
/// quota-unaccounted artifact behind.
#[derive(Debug)]
pub(crate) struct UncommittedBlobCleanup {
    staging_path: PathBuf,
    final_path: Option<PathBuf>,
    armed: bool,
}

impl UncommittedBlobCleanup {
    pub(crate) fn new(staging_path: PathBuf, final_path: PathBuf) -> Self {
        Self {
            staging_path,
            final_path: Some(final_path),
            armed: true,
        }
    }

    fn staging_only(staging_path: PathBuf) -> Self {
        Self {
            staging_path,
            final_path: None,
            armed: true,
        }
    }

    /// Transfer ownership of both paths to the caller (or to committed metadata).
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }

    fn unlink(path: &Path) {
        if let Err(error) = std::fs::remove_file(path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %path.display(),
                %error,
                "failed to unlink canceled provisional blob"
            );
        }
    }
}

impl Drop for UncommittedBlobCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        Self::unlink(&self.staging_path);
        if let Some(final_path) = &self.final_path {
            Self::unlink(final_path);
        }
    }
}

/// The staging writer's internal buffer size. `tokio::io::BufWriter::new` defaults to 8 KiB
/// (`tokio::io::util::DEFAULT_BUF_SIZE`), tuned for small, interactive writes: for a large object it
/// flushes to the file roughly every 8 KiB regardless of how large the incoming HTTP chunks are —
/// about a thousand `write()` syscalls for an 8 MiB PUT, versus a few dozen at this size. 256 KiB
/// (matching the small-object GET fast-path floor, `SMALL_READ_MAX`) measured a modest, real
/// improvement in isolation (~138 -> ~142 MiB/s for a bare 8 MiB `stage()`, no HTTP/auth) — most of an
/// 8 MiB PUT's cost is the durability barrier (fdatasync + rename), not the streaming writes, so this
/// is a small, free, zero-risk win rather than the whole story. The extra one-time allocation is
/// bounded by `write_permits` and negligible even for many concurrent small PUTs.
const STAGING_WRITE_BUF: usize = 256 * 1024;

/// A staging file open for writing, dispatching to the selected I/O backend. Construct with
/// [`Staging::create`], feed it with [`Staging::write_all`], then finish with either
/// [`Staging::commit`] (durable) or [`Staging::abort`] (discard).
pub(crate) enum Staging {
    /// The default backend: a buffered `tokio::fs` writer over the staging file.
    Tokio {
        writer: BufWriter<tokio::fs::File>,
        staging_path: PathBuf,
        /// When preallocation ran (a known-large write), the reserved length, so `commit` can
        /// advise the kernel to drop the written pages afterwards (ARCH 7.5). `None` skips it.
        release_len: Option<u64>,
    },
    /// The io_uring backend: file ops run on the dedicated io_uring executor.
    #[cfg(feature = "io-uring")]
    Uring(crate::uring::UringStaging),
}

impl Staging {
    /// Create the staging tmp file using the active backend. When `prealloc` is `Some(len)` and the
    /// length clears the hint threshold, the `tokio::fs` backend reserves blocks and advises
    /// sequential access up front (ARCH 7.5), all on the blocking pool in one hop.
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
        // std handle for the async streamed write. `spawn_blocking` itself is not canceled when
        // its awaiting request is dropped, so its result carries a staging-only cleanup guard. If
        // the join output has no receiver, dropping that output unlinks the file after creation;
        // on the ordinary path ownership transfers to the request-level two-path guard.
        let sp = staging_path.clone();
        let (file, mut create_cleanup) = tokio::task::spawn_blocking(
            move || -> Result<(std::fs::File, UncommittedBlobCleanup), BlobError> {
                let cleanup = UncommittedBlobCleanup::staging_only(sp.clone());
                let file = std::fs::File::create(&sp).map_err(io_err)?;
                if let Some(len) = release_len {
                    crate::raw_io::preallocate_sequential(&file, len);
                }
                Ok((file, cleanup))
            },
        )
        .await
        .map_err(|e| BlobError::Io(e.to_string()))??;
        create_cleanup.disarm();
        Ok(Staging::Tokio {
            writer: BufWriter::with_capacity(STAGING_WRITE_BUF, tokio::fs::File::from_std(file)),
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
    /// fsync — before treating the blob as durable (ARCH 8.2).
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
                // on — one fewer metadata-journal write per PUT than `sync_all` (ARCH 8.2).
                file.sync_data().await.map_err(io_err)?;
                // For a known-large write, drop the just-written pages so a stream of bulk uploads
                // does not evict the page cache hot reads depend on (ARCH 7.5). Best-effort.
                if let Some(len) = release_len {
                    let std_file = file.into_std().await;
                    let _ = tokio::task::spawn_blocking(move || {
                        crate::raw_io::release_pages(&std_file, len);
                    })
                    .await;
                }
                // Like create, Tokio implements filesystem rename on work that can outlive a
                // canceled await. Return an armed cleanup guard as the blocking task's output: if
                // the caller disappears, the abandoned output removes whichever of the tmp/final
                // names exists after rename; if it receives the output, the request-level guard
                // resumes ownership through the following directory-fsync await.
                let rename_source = staging_path.clone();
                let rename_dest = final_path.to_owned();
                let mut rename_cleanup = tokio::task::spawn_blocking(
                    move || -> Result<UncommittedBlobCleanup, BlobError> {
                        let cleanup =
                            UncommittedBlobCleanup::new(rename_source.clone(), rename_dest.clone());
                        std::fs::rename(&rename_source, &rename_dest).map_err(io_err)?;
                        Ok(cleanup)
                    },
                )
                .await
                .map_err(|e| BlobError::Io(e.to_string()))??;
                rename_cleanup.disarm();
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
                // its timestamps are irrelevant, so skip the extra metadata flush (ARCH 8.2).
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn cancel_with_guard(guard: UncommittedBlobCleanup) {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = guard;
            let _ = ready_tx.send(());
            std::future::pending::<()>().await;
        });
        ready_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }

    /// Dropping a request future cleans the unique artifact on either side of the atomic rename.
    /// This exercises the exact RAII path used by both `stage` and `assemble`, without timing a
    /// real filesystem operation or relying on a sleep.
    #[tokio::test]
    async fn cancellation_cleanup_covers_pre_and_post_rename_paths() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("object.tmp");
        let final_path = dir.path().join("object");

        std::fs::write(&staging, b"pre-rename").unwrap();
        cancel_with_guard(UncommittedBlobCleanup::new(
            staging.clone(),
            final_path.clone(),
        ))
        .await;
        assert!(
            !staging.exists(),
            "cancellation must unlink the staging tmp"
        );
        assert!(!final_path.exists());

        std::fs::write(&staging, b"post-rename").unwrap();
        let guard = UncommittedBlobCleanup::new(staging.clone(), final_path.clone());
        std::fs::rename(&staging, &final_path).unwrap();
        cancel_with_guard(guard).await;
        assert!(!staging.exists());
        assert!(
            !final_path.exists(),
            "cancellation after rename must unlink the unreturned final blob"
        );

        std::fs::write(&final_path, b"returned").unwrap();
        let mut guard = UncommittedBlobCleanup::new(staging, final_path.clone());
        guard.disarm();
        drop(guard);
        assert!(
            final_path.exists(),
            "disarming immediately before success must preserve the returned blob"
        );
    }
}
