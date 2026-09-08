//! Directory-fsync coalescing for the durable commit path (ARCH 8.2, Phase 1.5).
//!
//! The commit sequence is: fdatasync the staged file, rename it into the per-bucket directory,
//! then fsync that directory so the new entry survives a crash. Under concurrent writes to the
//! same bucket, every PUT otherwise issues its own directory fsync even though one fsync makes
//! *all* renames into that directory durable at once. This coordinator batches those: callers hand
//! it the directory to sync and await a single shared fsync.
//!
//! ## Correctness
//! A caller enqueues its request **only after its rename has returned**, so by the time the
//! coordinator issues a directory fsync for a batch, every batched request's directory entry is
//! already present and will be flushed. The coordinator therefore never acknowledges a caller
//! before that caller's rename is durable — the same barrier the per-PUT fsync gave, with fewer
//! syscalls. Distinct directories within one batch are fsynced concurrently, so coalescing same-
//! directory writes never serializes unrelated buckets.

use cairn_types::{error::BlobError, storage::io::StorageIoLease};
use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

struct SyncRequest {
    dir: Arc<File>,
    identity: (u64, u64),
    done: oneshot::Sender<Result<(), String>>,
    lease: StorageIoLease,
}

#[derive(Debug)]
pub(crate) struct DirSyncCoalescer {
    tx: mpsc::UnboundedSender<SyncRequest>,
}

impl DirSyncCoalescer {
    pub(crate) fn spawn() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(rx));
        Self { tx }
    }

    /// Sync the exact directory used by the completed rename. Coalescing by device/inode avoids
    /// acknowledging an old descriptor by syncing a newly recreated directory at the same name.
    pub(crate) async fn sync_file(
        &self,
        dir: Arc<File>,
        lease: &StorageIoLease,
    ) -> Result<(), BlobError> {
        let owned = lease.try_child()?;
        let metadata = dir.metadata().map_err(crate::io_err)?;
        if !metadata.is_dir() {
            return Err(BlobError::Io(
                "storage sync target is not a directory".into(),
            ));
        }
        let (done, result) = oneshot::channel();
        if let Err(error) = self.tx.send(SyncRequest {
            dir: dir.clone(),
            identity: (metadata.dev(), metadata.ino()),
            done,
            lease: owned,
        }) {
            return direct_sync(error.0.dir, error.0.lease).await;
        }
        match result.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(BlobError::Io(error)),
            Err(_) => direct_sync(dir, lease.try_child()?).await,
        }
    }
}

async fn run(mut requests: mpsc::UnboundedReceiver<SyncRequest>) {
    while let Some(first) = requests.recv().await {
        let mut batch = vec![first];
        while let Ok(request) = requests.try_recv() {
            batch.push(request);
        }
        let mut by_dir: HashMap<(u64, u64), Vec<SyncRequest>> = HashMap::new();
        for request in batch {
            by_dir.entry(request.identity).or_default().push(request);
        }
        let syncs = by_dir.into_values().map(|waiters| async move {
            // The closure owns every waiter and lease through actual fsync completion. Neither
            // request cancellation nor dropping this coordinator future can release them early.
            let _ = tokio::task::spawn_blocking(move || {
                let result = waiters[0].dir.sync_all().map_err(|error| error.to_string());
                for waiter in waiters {
                    let _ = waiter.done.send(result.clone());
                }
            })
            .await;
        });
        futures_util::future::join_all(syncs).await;
    }
}

async fn direct_sync(dir: Arc<File>, lease: StorageIoLease) -> Result<(), BlobError> {
    tokio::task::spawn_blocking(move || {
        let _lease = lease;
        let result = dir.sync_all().map_err(crate::io_err);
        drop(dir);
        result
    })
    .await
    .map_err(|error| BlobError::Io(error.to_string()))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_file::test_lease;
    use cairn_types::storage::{StorageToken, io::StorageIoWatch};
    use futures_util::FutureExt;

    #[tokio::test]
    async fn coalesces_concurrent_same_dir_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let file = Arc::new(crate::open_readonly_nofollow(dir.path()).unwrap());
        let coalescer = DirSyncCoalescer::spawn();
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let file = file.clone();
            let tx = coalescer.tx.clone();
            tasks.push(tokio::spawn(async move {
                DirSyncCoalescer { tx }.sync_file(file, &test_lease()).await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn fallback_syncs_the_same_directory_and_refuses_a_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = Arc::new(crate::open_readonly_nofollow(dir.path()).unwrap());
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let coalescer = DirSyncCoalescer { tx };
        coalescer.sync_file(file, &test_lease()).await.unwrap();
        assert!(
            coalescer
                .sync_file(Arc::new(tempfile::tempfile().unwrap()), &test_lease())
                .await
                .is_err()
        );
    }

    #[test]
    fn cancellation_of_waiter_and_coordinator_retains_queued_fsync_ownership() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let file = Arc::new(crate::open_readonly_nofollow(dir.path()).unwrap());
            let (started, ready) = oneshot::channel();
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
            let (tx, rx) = mpsc::unbounded_channel();
            let coalescer = DirSyncCoalescer { tx };
            assert!(coalescer.sync_file(file, &lease).now_or_never().is_none());
            assert!(run(rx).now_or_never().is_none());
            drop(lease);
            assert!(watch.quiescent().now_or_never().is_none());
            release.send(()).unwrap();
            blocker.await.unwrap();
            watch.quiescent().await;
        });
    }
}
