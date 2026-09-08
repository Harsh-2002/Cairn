//! The admitted staging writer. Failed or cancelled writes retain their exact journal-owned
//! names; only the retained recovery consumer can turn quiescence into physical cleanup.

use crate::io_err;
use crate::namespace::AnchoredPath;
use crate::owned_file::{FileOwner, OwnedFile};
use cairn_types::{error::BlobError, storage::io::StorageIoLease};
use tokio::io::{AsyncWriteExt, BufWriter};

const STAGING_WRITE_BUF: usize = 256 * 1024;

pub(crate) enum Staging {
    Tokio {
        writer: BufWriter<OwnedFile>,
        staging: AnchoredPath,
        release_len: Option<u64>,
    },
    #[cfg(feature = "io-uring")]
    Uring(crate::uring::UringStaging),
}

impl Staging {
    pub(crate) async fn create(
        staging: AnchoredPath,
        use_uring: bool,
        prealloc: Option<u64>,
        lease: StorageIoLease,
    ) -> Result<Self, BlobError> {
        if use_uring {
            #[cfg(feature = "io-uring")]
            {
                return Ok(Self::Uring(
                    crate::uring::UringStaging::create(staging, lease).await?,
                ));
            }
        }
        let release_len = prealloc.filter(|&n| n >= crate::raw_io::HINT_THRESHOLD);
        let path = staging.clone();
        let operation = lease.try_child()?;
        let owner = tokio::task::spawn_blocking(move || {
            let _operation = operation;
            let file = path.create_new().map_err(io_err)?;
            if let Some(len) = release_len {
                crate::raw_io::preallocate_sequential(&file, len);
            }
            Ok::<_, BlobError>(FileOwner::new(file, lease))
        })
        .await
        .map_err(|error| BlobError::Io(error.to_string()))??;
        Ok(Self::Tokio {
            writer: BufWriter::with_capacity(STAGING_WRITE_BUF, OwnedFile::new(owner)),
            staging,
            release_len,
        })
    }

    pub(crate) async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        match self {
            Self::Tokio { writer, .. } => writer.write_all(bytes).await.map_err(io_err),
            #[cfg(feature = "io-uring")]
            Self::Uring(writer) => writer.write_all(bytes).await,
        }
    }

    /// File sync precedes the exclusive rename. The caller then synchronizes the exact destination
    /// directory through the shared coalescer before reporting the blob durable (ARCH 8.2).
    pub(crate) async fn commit(self, destination: &AnchoredPath) -> Result<(), BlobError> {
        match self {
            Self::Tokio {
                mut writer,
                staging,
                release_len,
            } => {
                writer.flush().await.map_err(io_err)?;
                let owner = writer.into_inner().into_owner().map_err(io_err)?;
                let destination = destination.clone();
                owner
                    .run(move |file| {
                        file.sync_data()?;
                        if let Some(len) = release_len {
                            crate::raw_io::release_pages(file, len);
                        }
                        staging.rename_to(&destination)
                    })
                    .await
                    .map_err(io_err)
            }
            #[cfg(feature = "io-uring")]
            Self::Uring(writer) => writer.commit(destination.clone()).await,
        }
    }

    pub(crate) async fn fsync_in_place(self) -> Result<(), BlobError> {
        match self {
            Self::Tokio { mut writer, .. } => {
                writer.flush().await.map_err(io_err)?;
                writer
                    .into_inner()
                    .into_owner()
                    .map_err(io_err)?
                    .run(|file| file.sync_data())
                    .await
                    .map_err(io_err)
            }
            #[cfg(feature = "io-uring")]
            Self::Uring(writer) => writer.fsync_in_place().await,
        }
    }

    /// Stop producing bytes. Abandoned blocking work still owns its lease, and all provisional
    /// names remain owned by the durable intent until the retained consumer proves quiescence.
    pub(crate) async fn abort(self) {
        #[cfg(feature = "io-uring")]
        if let Self::Uring(writer) = self {
            writer.abort().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::storage::{StorageToken, io::StorageIoWatch};
    use futures_util::FutureExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn abort_retains_admitted_artifact_and_collision_preserves_existing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part");
        let (mut watch, lease) = StorageIoWatch::new(
            StorageToken::generate(),
            StorageToken::generate(),
            Arc::new(()),
        );
        let mut staging = Staging::create(AnchoredPath::fixture(&path), false, None, lease)
            .await
            .unwrap();
        staging.write_all(b"owned bytes").await.unwrap();
        staging.fsync_in_place().await.unwrap();
        watch.quiescent().await;
        let collision = Staging::create(
            AnchoredPath::fixture(&path),
            false,
            None,
            crate::owned_file::test_lease(),
        )
        .await;
        assert!(collision.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"owned bytes");

        let other = dir.path().join("aborted");
        let staging = Staging::create(
            AnchoredPath::fixture(&other),
            false,
            None,
            crate::owned_file::test_lease(),
        )
        .await
        .unwrap();
        staging.abort().await;
        assert!(
            other.exists(),
            "abort cannot discard the journal's physical ownership"
        );
    }

    #[test]
    fn cancelled_queued_create_cannot_report_quiescence_before_late_creation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("queued");
            let anchored = AnchoredPath::fixture(&path);
            let (started, ready) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
            });
            ready.await.unwrap();
            let (mut watch, lease) = StorageIoWatch::new(
                StorageToken::generate(),
                StorageToken::generate(),
                Arc::new(()),
            );
            assert!(
                Staging::create(anchored, false, None, lease)
                    .now_or_never()
                    .is_none()
            );
            assert!(watch.quiescent().now_or_never().is_none());
            assert!(!path.exists());
            release.send(()).unwrap();
            blocker.await.unwrap();
            watch.quiescent().await;
            assert!(
                path.exists(),
                "late creation stays covered by the retained journal intent"
            );
        });
    }
}
