//! Directory-fsync coalescing for the durable commit path (ARCH §8.2, Phase 1.5).
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

use cairn_types::error::BlobError;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::{mpsc, oneshot};

/// One request to make a directory's pending renames durable. The reply is a stringified result so
/// a single fsync outcome can fan out to every coalesced waiter (`BlobError` is not `Clone`).
struct SyncRequest {
    dir: PathBuf,
    done: oneshot::Sender<Result<(), String>>,
}

/// A handle to the directory-fsync coalescing coordinator. Cloning the owning [`LocalBlobStore`]
/// shares one coordinator task via the inner `Arc`.
///
/// [`LocalBlobStore`]: crate::LocalBlobStore
#[derive(Debug)]
pub(crate) struct DirSyncCoalescer {
    tx: mpsc::UnboundedSender<SyncRequest>,
}

impl DirSyncCoalescer {
    /// Spawn the coordinator task and return a handle. The task lives until every handle is
    /// dropped (the store and all its clones), then exits when its channel closes.
    pub(crate) fn spawn() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(rx));
        Self { tx }
    }

    /// Make every rename into `dir` that completed before this call durable, coalescing concurrent
    /// callers for the same directory into one fsync. Resolves only after that fsync completes, so
    /// the caller must not have proceeded past the durability barrier before awaiting this.
    ///
    /// If the coordinator is gone (only at shutdown) the call falls back to a direct fsync, so
    /// durability is preserved regardless.
    pub(crate) async fn sync_dir(&self, dir: &Path) -> Result<(), BlobError> {
        let (done, rx) = oneshot::channel();
        if self
            .tx
            .send(SyncRequest {
                dir: dir.to_owned(),
                done,
            })
            .is_err()
        {
            return crate::fsync_dir(dir).await;
        }
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(BlobError::Io(e)),
            // Coordinator dropped the reply mid-flight (shutdown race): fall back so we never
            // report success without a real fsync.
            Err(_) => crate::fsync_dir(dir).await,
        }
    }
}

/// The coordinator loop: block for the next request, drain every request already queued into one
/// batch, then fsync each distinct directory once (distinct directories concurrently) and reply to
/// every waiter. Requests that arrive while a batch's fsyncs are in flight form the next batch, so
/// under load batches grow and coalescing increases — the self-tuning group-commit behavior.
async fn run(mut rx: mpsc::UnboundedReceiver<SyncRequest>) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        while let Ok(r) = rx.try_recv() {
            batch.push(r);
        }
        let mut by_dir: HashMap<PathBuf, Vec<oneshot::Sender<Result<(), String>>>> = HashMap::new();
        for r in batch {
            by_dir.entry(r.dir).or_default().push(r.done);
        }
        let syncs = by_dir.into_iter().map(|(dir, waiters)| async move {
            let result = crate::fsync_dir(&dir).await.map_err(|e| e.to_string());
            for w in waiters {
                let _ = w.send(result.clone());
            }
        });
        futures_util::future::join_all(syncs).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn coalesces_concurrent_same_dir_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let coalescer = DirSyncCoalescer::spawn();
        // Many concurrent syncs of the same directory all succeed (and share fsyncs under the
        // hood). Each call returns only after a real directory fsync.
        let mut handles = Vec::new();
        for _ in 0..32 {
            let path = dir.path().to_owned();
            let tx = coalescer.tx.clone();
            handles.push(tokio::spawn(async move {
                let c = DirSyncCoalescer { tx };
                c.sync_dir(&path).await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("each coalesced sync must succeed");
        }
    }

    #[tokio::test]
    async fn syncs_distinct_dirs() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let coalescer = DirSyncCoalescer::spawn();
        coalescer.sync_dir(a.path()).await.unwrap();
        coalescer.sync_dir(b.path()).await.unwrap();
    }

    #[tokio::test]
    async fn fallback_on_missing_dir_reports_error() {
        let coalescer = DirSyncCoalescer::spawn();
        let missing = std::path::Path::new("/nonexistent/cairn/dir/xyz");
        assert!(
            coalescer.sync_dir(missing).await.is_err(),
            "a fsync of a missing directory must surface an error to the waiter"
        );
    }
}
