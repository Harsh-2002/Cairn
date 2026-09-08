//! The io_uring blob-I/O backend (feature `io-uring`).
//!
//! `tokio-uring` requires a *current-thread* io_uring runtime: its file operations submit to a
//! per-thread `io_uring` instance and cannot run on the server's multi-threaded work-stealing
//! tokio runtime. We therefore stand up a **dedicated io_uring executor** — one (or more) OS
//! threads, each running a `tokio_uring` runtime — and dispatch the durable staging file ops to
//! it, bridging results back to the async caller over a oneshot channel. The caller's runtime
//! keeps consuming the request body and doing compression/encryption/hashing exactly as before;
//! data writes and file syncs run on the io_uring threads. Namespace operations use leased
//! synchronous syscalls with anchored descriptors; cleanup is exclusively journal-owned.
//!
//! The durable-commit ordering is preserved byte-for-byte with the `tokio::fs` path: write the
//! payload, **fsync the file**, **rename** it into the bucket directory, then **fsync that
//! directory** (the F-1 ordering, ARCH 8.2). Data writes and file sync use io_uring; the anchored
//! rename runs in a leased blocking job, followed by the shared directory-sync coalescer.

use crate::namespace::AnchoredPath;
use crate::owned_file::FileOwner;
use cairn_types::{error::BlobError, storage::io::StorageIoLease};
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use tokio::sync::oneshot;

/// Map a `std::io::Error` from an io_uring op into a [`BlobError`], preserving the `OutOfSpace`
/// classification the rest of the crate relies on (ENOSPC == raw errno 28).
pub(crate) fn io_err(e: std::io::Error) -> BlobError {
    if e.kind() == std::io::ErrorKind::StorageFull || e.raw_os_error() == Some(28) {
        BlobError::OutOfSpace
    } else {
        BlobError::Io(e.to_string())
    }
}

/// A unit of work handed to the io_uring executor. It is a boxed closure that, when run *on* the
/// executor thread (inside the `tokio_uring` runtime), produces a future. The future is `'static`
/// because it is spawned with `tokio_uring::spawn` (i.e. `spawn_local`) and owns everything it
/// touches; results travel back over a oneshot the closure captures.
type Job = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + Send>;

/// Handle to the dedicated io_uring executor. Cloneable and cheap; all clones share one set of
/// runtime threads. The process holds a single lazily-started executor (see [`executor`]).
#[derive(Clone)]
pub(crate) struct UringExecutor {
    tx: tokio::sync::mpsc::UnboundedSender<Job>,
}

impl std::fmt::Debug for UringExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringExecutor").finish_non_exhaustive()
    }
}

impl UringExecutor {
    /// Start the executor: spawn `threads` OS threads, each running a `tokio_uring` runtime that
    /// drains the shared job queue and runs each job as a local task. The number of threads is
    /// kept small (it is I/O, not CPU, bound); one thread is sufficient for correctness, more add
    /// submission-side parallelism for many concurrent staging commits.
    fn start(threads: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Job>();
        let rx = std::sync::Arc::new(tokio::sync::Mutex::new(rx));
        for i in 0..threads.max(1) {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("cairn-uring-{i}"))
                .spawn(move || {
                    tokio_uring::start(async move {
                        loop {
                            // Lock only to dequeue one job, then release so sibling threads can
                            // pull the next one while this job's io_uring ops are in flight.
                            let job = {
                                let mut guard = rx.lock().await;
                                guard.recv().await
                            };
                            match job {
                                Some(job) => {
                                    // Run each commit as a local task so multiple in-flight
                                    // commits on this thread overlap their io_uring submissions.
                                    tokio_uring::spawn(job());
                                }
                                None => break, // all senders dropped: shut the runtime down
                            }
                        }
                    });
                })
                .expect("spawn io_uring executor thread");
        }
        Self { tx }
    }

    /// Spawn a long-lived future-producing closure onto an executor thread *without* awaiting its
    /// completion. The closure runs as a local task inside the `tokio_uring` runtime and drives
    /// itself; this is used for the per-staging-file writer task, which lives until it receives a
    /// terminal command and therefore must not block the caller. The caller learns the task is
    /// ready (and its eventual result) through channels the closure captures, not through this
    /// call's return.
    fn spawn_detached<F, Fut>(&self, f: F) -> Result<(), BlobError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let job: Job = Box::new(move || Box::pin(f()));
        self.tx
            .send(job)
            .map_err(|_| BlobError::Io("io_uring executor stopped".into()))
    }
}

/// The process-wide io_uring executor, started on first use. A single shared executor avoids
/// spawning a fresh runtime per `LocalBlobStore` (which clones freely), and its threads live for
/// the process lifetime — appropriate for a long-running server data plane.
static EXECUTOR: OnceLock<UringExecutor> = OnceLock::new();

/// Number of io_uring executor threads. One is correct; a small fixed count gives submission
/// parallelism for concurrent commits without oversubscribing. Overridable for tests/tuning via
/// `CAIRN_URING_THREADS`.
fn executor_threads() -> usize {
    std::env::var("CAIRN_URING_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(1)
}

pub(crate) fn executor() -> &'static UringExecutor {
    EXECUTOR.get_or_init(|| UringExecutor::start(executor_threads()))
}

/// A staging file being written through io_uring. Created on an executor thread; bytes are
/// streamed to it; finally committed (fsync → rename → dir-fsync) or aborted (unlink) — all on the
/// executor.
///
/// The transform that produces the physical bytes (consuming the body, compress/encrypt/hash)
/// stays on the caller's multi-threaded runtime, where the body lives. The `tokio_uring::fs::File`
/// is *not* held on the caller side; it is owned for its whole life by one long-lived task on the
/// io_uring executor ([`writer_task`]). This type is the caller-side front end: it sends chunk /
/// commit / abort / fsync commands to that task over a channel and awaits the result of each, so a
/// write error (e.g. ENOSPC) surfaces at exactly the point it would on the `tokio::fs` path. The
/// executor task appends each chunk at a running offset, keeping the on-disk layout identical to a
/// sequential `BufWriter`.
pub(crate) struct UringStaging {
    /// Sends chunks (and the terminal commit/abort command) to the executor-side writer task.
    cmd_tx: tokio::sync::mpsc::Sender<WriteCmd>,
    owner: FileOwner,
    /// Receives the result of the terminal command (commit/abort/fsync) so the caller can confirm
    /// the writer task wound down.
    final_rx: Option<oneshot::Receiver<Result<(), BlobError>>>,
}

enum WriteCmd {
    Chunk(Vec<u8>, oneshot::Sender<Result<(), BlobError>>),
    Commit(AnchoredPath, oneshot::Sender<Result<(), BlobError>>),
    FsyncInPlace(oneshot::Sender<Result<(), BlobError>>),
    Abort(oneshot::Sender<Result<(), BlobError>>),
}

impl UringStaging {
    /// Namespace creation is synchronous on the blocking pool. Kernel-ring operations are limited
    /// to this locked file's data and durability, so process death cannot leave a late create or
    /// rename queued behind the release of the node lock.
    pub(crate) async fn create(
        staging: AnchoredPath,
        lease: StorageIoLease,
    ) -> Result<Self, BlobError> {
        let path = staging.clone();
        let operation = lease.try_child()?;
        let owner = tokio::task::spawn_blocking(move || {
            let _operation = operation;
            Ok::<_, BlobError>(FileOwner::new(path.create_new().map_err(io_err)?, lease))
        })
        .await
        .map_err(|error| BlobError::Io(error.to_string()))??;
        let worker_owner = owner.child().map_err(io_err)?;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(8);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (final_tx, final_rx) = oneshot::channel();
        executor().spawn_detached(move || async move {
            writer_task(staging, worker_owner, cmd_rx, ready_tx, final_tx).await;
        })?;
        ready_rx
            .await
            .map_err(|_| BlobError::Io("io_uring staging task ended early".into()))??;
        Ok(Self {
            cmd_tx,
            owner,
            final_rx: Some(final_rx),
        })
    }

    pub(crate) async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        for bytes in bytes.chunks(256 * 1024) {
            let _operation = self.owner.child().map_err(io_err)?;
            let (reply, result) = oneshot::channel();
            self.cmd_tx
                .send(WriteCmd::Chunk(bytes.to_vec(), reply))
                .await
                .map_err(|_| BlobError::Io("io_uring staging writer stopped".into()))?;
            result
                .await
                .map_err(|_| BlobError::Io("io_uring write acknowledgement lost".into()))??;
        }
        Ok(())
    }

    pub(crate) async fn commit(mut self, destination: AnchoredPath) -> Result<(), BlobError> {
        let _operation = self.owner.child().map_err(io_err)?;
        let (reply, result) = oneshot::channel();
        self.cmd_tx
            .send(WriteCmd::Commit(destination, reply))
            .await
            .map_err(|_| BlobError::Io("io_uring staging writer stopped".into()))?;
        result
            .await
            .map_err(|_| BlobError::Io("io_uring commit acknowledgement lost".into()))??;
        self.terminal().await
    }

    pub(crate) async fn fsync_in_place(mut self) -> Result<(), BlobError> {
        let _operation = self.owner.child().map_err(io_err)?;
        let (reply, result) = oneshot::channel();
        self.cmd_tx
            .send(WriteCmd::FsyncInPlace(reply))
            .await
            .map_err(|_| BlobError::Io("io_uring staging writer stopped".into()))?;
        result
            .await
            .map_err(|_| BlobError::Io("io_uring sync acknowledgement lost".into()))??;
        self.terminal().await
    }

    pub(crate) async fn abort(mut self) {
        let (reply, result) = oneshot::channel();
        if self.cmd_tx.send(WriteCmd::Abort(reply)).await.is_ok() {
            let _ = result.await;
        }
        let _ = self.terminal().await;
    }

    async fn terminal(&mut self) -> Result<(), BlobError> {
        if let Some(result) = self.final_rx.take() {
            result
                .await
                .map_err(|_| BlobError::Io("io_uring staging task ended early".into()))?
        } else {
            Ok(())
        }
    }
}

/// This detached task drives every submitted operation to completion even after the request's
/// channel closes. The original locked descriptor and its node lifetime remain in `owner`.
async fn writer_task(
    staging: AnchoredPath,
    owner: FileOwner,
    mut commands: tokio::sync::mpsc::Receiver<WriteCmd>,
    ready: oneshot::Sender<Result<(), BlobError>>,
    final_result: oneshot::Sender<Result<(), BlobError>>,
) {
    let file = match owner.file.try_clone() {
        Ok(file) => tokio_uring::fs::File::from_std(file),
        Err(error) => {
            let _ = ready.send(Err(io_err(error)));
            return;
        }
    };
    let _ = ready.send(Ok(()));
    let mut offset = 0;
    while let Some(command) = commands.recv().await {
        match command {
            WriteCmd::Chunk(bytes, reply) => {
                let len = bytes.len() as u64;
                let (result, _bytes) = file.write_all_at(bytes, offset).await;
                if result.is_ok() {
                    offset += len;
                }
                let _ = reply.send(result.map_err(io_err));
            }
            WriteCmd::Commit(destination, reply) => {
                let synced = file.sync_data().await.map_err(io_err);
                let closed = file.close().await.map_err(io_err);
                let result = match synced.and(closed) {
                    Ok(()) => owner
                        .run(move |_| staging.rename_to(&destination))
                        .await
                        .map_err(io_err),
                    Err(error) => {
                        drop(staging);
                        drop(destination);
                        Err(error)
                    }
                };
                // The completed task must not retain file/directory fences after waking callers
                // which can immediately publish and retire their final cleanup debt.
                let _lease = owner.into_lease();
                let _ = reply.send(result.clone_shallow());
                let _ = final_result.send(result);
                return;
            }
            WriteCmd::FsyncInPlace(reply) => {
                let result = file.sync_data().await.map_err(io_err);
                let closed = file.close().await.map_err(io_err);
                let result = result.and(closed);
                drop(staging);
                let _lease = owner.into_lease();
                let _ = reply.send(result.clone_shallow());
                let _ = final_result.send(result);
                return;
            }
            WriteCmd::Abort(reply) => {
                let result = file.close().await.map_err(io_err);
                drop(staging);
                let _lease = owner.into_lease();
                let _ = reply.send(result.clone_shallow());
                let _ = final_result.send(result);
                return;
            }
        }
    }
    let result = file.close().await.map_err(io_err);
    drop(staging);
    let _lease = owner.into_lease();
    let _ = final_result.send(result);
}

/// `BlobError` is not `Clone`; this gives us a cheap shallow clone for the two-sink fan-out
/// (per-command reply + terminal channel) without adding a `Clone` impl to the shared error type.
trait CloneShallow {
    fn clone_shallow(&self) -> Self;
}
impl CloneShallow for Result<(), BlobError> {
    fn clone_shallow(&self) -> Self {
        match self {
            Ok(()) => Ok(()),
            Err(e) => Err(clone_blob_error(e)),
        }
    }
}

fn clone_blob_error(e: &BlobError) -> BlobError {
    match e {
        BlobError::Io(s) => BlobError::Io(s.clone()),
        BlobError::OutOfSpace => BlobError::OutOfSpace,
        BlobError::SizeExceeded => BlobError::SizeExceeded,
        BlobError::NotFound => BlobError::NotFound,
        BlobError::Corruption(s) => BlobError::Corruption(s.clone()),
        // Body errors never originate from the executor-side commit; map to Io defensively.
        other => BlobError::Io(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::storage::{StorageToken, io::StorageIoWatch};
    use futures_util::FutureExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Wake, Waker};

    #[tokio::test]
    async fn terminal_acknowledgements_release_file_and_directory_fences_before_waking() {
        enum Terminal {
            Commit,
            Sync,
            Abort,
            ChannelClosed,
        }
        struct Probe {
            directories: Vec<std::path::PathBuf>,
            file: std::path::PathBuf,
            watch: Mutex<StorageIoWatch>,
            wakes: AtomicUsize,
        }
        impl Wake for Probe {
            fn wake(self: Arc<Self>) {
                for path in self.directories.iter().chain(std::iter::once(&self.file)) {
                    let file = crate::open_readonly_nofollow(path).unwrap();
                    crate::try_lock_exclusive(&file)
                        .expect("terminal acknowledgement retained a completed descriptor fence");
                }
                assert!(
                    self.watch
                        .lock()
                        .unwrap()
                        .quiescent()
                        .now_or_never()
                        .is_none(),
                    "the terminal task must retain its I/O lease through the acknowledgement"
                );
                self.wakes.fetch_add(1, Ordering::SeqCst);
            }
        }
        for terminal in [
            Terminal::Commit,
            Terminal::Sync,
            Terminal::Abort,
            Terminal::ChannelClosed,
        ] {
            let root = tempfile::tempdir().unwrap();
            let session = root.path().join("session");
            let bucket = root.path().join("bucket");
            std::fs::create_dir(&session).unwrap();
            std::fs::create_dir(&bucket).unwrap();
            let source_path = session.join("source");
            let final_path = bucket.join("final");
            let staging = AnchoredPath::fixture(&source_path);
            rustix::fs::flock(&*staging.parent, rustix::fs::FlockOperation::LockShared).unwrap();
            let file = staging.create_new().unwrap();
            let (mut watch, lease) = StorageIoWatch::new(
                StorageToken::generate(),
                StorageToken::generate(),
                Arc::new(()),
            );
            let owner = FileOwner::new(file, lease);
            let is_commit = matches!(terminal, Terminal::Commit);
            let is_closed = matches!(terminal, Terminal::ChannelClosed);
            let probe = Arc::new(Probe {
                directories: vec![session, bucket],
                file: if is_commit {
                    final_path.clone()
                } else {
                    source_path
                },
                watch: Mutex::new(watch.clone()),
                wakes: AtomicUsize::new(0),
            });
            let waker = Waker::from(probe.clone());
            let (commands, receiver) = tokio::sync::mpsc::channel(1);
            let (ready, ready_rx) = oneshot::channel();
            let (done, mut done_rx) = oneshot::channel();
            let (ended, ended_rx) = oneshot::channel();
            assert!(
                Pin::new(&mut done_rx)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            executor()
                .spawn_detached(move || async move {
                    writer_task(staging, owner, receiver, ready, done).await;
                    ended.send(()).unwrap();
                })
                .unwrap();
            ready_rx.await.unwrap().unwrap();
            let (reply, mut reply_rx) = oneshot::channel();
            if !is_closed {
                assert!(
                    Pin::new(&mut reply_rx)
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
            }
            let command = match terminal {
                Terminal::Commit => {
                    let destination = AnchoredPath::fixture(&final_path);
                    rustix::fs::flock(&*destination.parent, rustix::fs::FlockOperation::LockShared)
                        .unwrap();
                    Some(WriteCmd::Commit(destination, reply))
                }
                Terminal::Sync => Some(WriteCmd::FsyncInPlace(reply)),
                Terminal::Abort => Some(WriteCmd::Abort(reply)),
                Terminal::ChannelClosed => {
                    drop(reply);
                    None
                }
            };
            if let Some(command) = command {
                commands.send(command).await.unwrap();
            }
            drop(commands);
            // Do not poll either terminal receiver again before the worker finishes: their
            // custom wakers inspect the exact synchronous send boundary on the ring executor.
            ended_rx.await.unwrap();
            done_rx.try_recv().unwrap().unwrap();
            if !is_closed {
                reply_rx.try_recv().unwrap().unwrap();
            }
            assert_eq!(
                probe.wakes.load(Ordering::SeqCst),
                if is_closed { 1 } else { 2 }
            );
            assert!(watch.quiescent().now_or_never().is_some());
        }
    }
}
