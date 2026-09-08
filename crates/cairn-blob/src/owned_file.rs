//! Blocking file operations retain storage ownership through actual completion, including an
//! abandoned join result. Tokio's filesystem wrapper cannot carry that ownership into its jobs.

use cairn_types::storage::io::StorageIoLease;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read, Seek, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use tokio::io::AsyncWrite;
use tokio::task::JoinHandle;

const WRITE_BATCH: usize = 256 * 1024;

/// Field order closes the descriptor before releasing its node-lock ownership.
pub(crate) struct FileOwner {
    pub(crate) file: Arc<File>,
    lease: StorageIoLease,
}

impl FileOwner {
    pub(crate) fn new(file: File, lease: StorageIoLease) -> Self {
        Self {
            file: Arc::new(file),
            lease,
        }
    }

    pub(crate) fn child(&self) -> io::Result<Self> {
        let lease = self.lease.try_child().map_err(io::Error::other)?;
        Ok(Self {
            file: self.file.clone(),
            lease,
        })
    }

    /// The result owns the lease too: cancelling the await cannot expose a still-open descriptor
    /// or retained buffer after the recovery observer reports quiescence.
    pub(crate) async fn run<T, F>(&self, operation: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&File) -> io::Result<T> + Send + 'static,
    {
        let owner = self.child()?;
        let (result, _owner) = tokio::task::spawn_blocking(move || {
            let result = operation(&owner.file);
            (result, owner)
        })
        .await
        .map_err(io::Error::other)?;
        result
    }
}

type WriteResult = (io::Result<usize>, FileOwner);

/// One bounded outstanding write per handle. Every queued closure owns a child lease, rather
/// than relying on the lifetime of the async future that happens to await it.
pub(crate) struct OwnedFile {
    pending: Option<JoinHandle<WriteResult>>,
    pub(crate) owner: FileOwner,
}

impl OwnedFile {
    pub(crate) fn new(owner: FileOwner) -> Self {
        Self {
            pending: None,
            owner,
        }
    }

    /// Only after an async flush has drained the last physical write.
    pub(crate) fn into_owner(self) -> io::Result<FileOwner> {
        if self.pending.is_some() {
            return Err(io::Error::other("storage file still has a pending write"));
        }
        Ok(self.owner)
    }

    pub(crate) async fn read_chunk(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if self.pending.is_some() || len > WRITE_BATCH {
            return Err(io::Error::other("invalid storage scratch read"));
        }
        self.owner
            .run(move |mut file| {
                file.seek(io::SeekFrom::Start(offset))?;
                let mut bytes = vec![0; len];
                file.read_exact(&mut bytes)?;
                Ok(bytes)
            })
            .await
    }

    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let Some(pending) = self.pending.as_mut() else {
            return Poll::Ready(Ok(0));
        };
        let joined = ready!(Pin::new(pending).poll(cx));
        self.pending = None;
        Poll::Ready(
            joined
                .map_err(io::Error::other)
                .and_then(|(result, _owner)| result),
        )
    }
}

impl AsyncWrite for OwnedFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.pending.is_none() {
            let owner = self.owner.child()?;
            let bytes = bytes[..bytes.len().min(WRITE_BATCH)].to_vec();
            self.pending = Some(tokio::task::spawn_blocking(move || {
                let result = (&*owner.file).write(&bytes);
                (result, owner)
            }));
        }
        self.poll_pending(cx)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_pending(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
pub(crate) fn test_lease() -> StorageIoLease {
    use cairn_types::storage::{StorageToken, io::StorageIoWatch};
    StorageIoWatch::new(
        StorageToken::generate(),
        StorageToken::generate(),
        Arc::new(()),
    )
    .1
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::storage::{StorageToken, io::StorageIoWatch};
    use futures_util::FutureExt;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn bounded_writes_preserve_bytes_and_cancellation_prevents_new_work() {
        let file = tempfile::tempfile().unwrap();
        let (mut watch, lease) = StorageIoWatch::new(
            StorageToken::generate(),
            StorageToken::generate(),
            Arc::new(()),
        );
        let mut writer = OwnedFile::new(FileOwner::new(file, lease));
        let bytes = vec![37; WRITE_BATCH * 2 + 71];
        writer.write_all(&bytes).await.unwrap();
        writer.flush().await.unwrap();
        assert_eq!(
            writer
                .read_chunk(WRITE_BATCH as u64, WRITE_BATCH)
                .await
                .unwrap(),
            vec![37; WRITE_BATCH]
        );
        let owner = writer.into_owner().unwrap();
        assert_eq!(owner.file.metadata().unwrap().len(), bytes.len() as u64);
        watch.cancel();
        assert!(owner.run(|file| file.sync_data()).await.is_err());
        assert!(watch.quiescent().now_or_never().is_none());
        drop(owner);
        assert!(watch.quiescent().now_or_never().is_some());
    }

    #[test]
    fn cancelled_await_keeps_executing_operation_and_node_lifetime_owned() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let lifetime = Arc::new(());
            let weak = Arc::downgrade(&lifetime);
            let (mut watch, lease) =
                StorageIoWatch::new(StorageToken::generate(), StorageToken::generate(), lifetime);
            let owner = FileOwner::new(tempfile::tempfile().unwrap(), lease);
            let (started, ready) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let task = tokio::spawn(async move {
                owner
                    .run(move |mut file| {
                        started.send(()).unwrap();
                        wait.recv().unwrap();
                        file.write_all(b"late physical write")
                    })
                    .await
            });
            ready.await.unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(watch.quiescent().now_or_never().is_none());
            assert!(weak.upgrade().is_some());
            release.send(()).unwrap();
            watch.quiescent().await;
            drop(watch);
            // The observer can wake between the last counter decrement and that lease's Arc
            // destructor returning. The sole worker barrier waits for that harmless tail too.
            tokio::task::spawn_blocking(|| ()).await.unwrap();
            assert!(weak.upgrade().is_none());
        });
    }

    #[test]
    fn cancelled_await_keeps_queued_operation_owned() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
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
            let mut writer = OwnedFile::new(FileOwner::new(tempfile::tempfile().unwrap(), lease));
            assert!(writer.write_all(b"queued").now_or_never().is_none());
            drop(writer);
            assert!(watch.quiescent().now_or_never().is_none());
            release.send(()).unwrap();
            blocker.await.unwrap();
            watch.quiescent().await;
        });
    }
}
