//! The io_uring blob-I/O backend (feature `io-uring`).
//!
//! `tokio-uring` requires a *current-thread* io_uring runtime: its file operations submit to a
//! per-thread `io_uring` instance and cannot run on the server's multi-threaded work-stealing
//! tokio runtime. We therefore stand up a **dedicated io_uring executor** — one (or more) OS
//! threads, each running a `tokio_uring` runtime — and dispatch the durable staging file ops to
//! it, bridging results back to the async caller over a oneshot channel. The caller's runtime
//! keeps consuming the request body and doing compression/encryption/hashing exactly as before;
//! only the raw file syscalls (create, write, fsync, rename, dir-fsync, unlink) move onto the
//! io_uring threads.
//!
//! The durable-commit ordering is preserved byte-for-byte with the `tokio::fs` path: write the
//! payload, **fsync the file**, **rename** it into the bucket directory, then **fsync that
//! directory** (the F-1 ordering, ARCH §8.2). All of those steps are issued as io_uring ops on
//! the executor thread that owns the staging file's fd.

use cairn_types::error::BlobError;
use std::future::Future;
use std::path::{Path, PathBuf};
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
    /// Receives the result of the terminal command (commit/abort/fsync) so the caller can confirm
    /// the writer task wound down.
    final_rx: Option<oneshot::Receiver<Result<(), BlobError>>>,
}

enum WriteCmd {
    /// Append these bytes at the current offset; ack over the bundled sender.
    Chunk(Vec<u8>, oneshot::Sender<Result<(), BlobError>>),
    /// fdatasync file → rename(tmp,dst); the staging task ends after this. The destination-directory
    /// fsync is the caller's coalesced step (see [`crate::commit::DirSyncCoalescer`]).
    Commit {
        final_path: PathBuf,
        reply: oneshot::Sender<Result<(), BlobError>>,
    },
    /// fsync the file in place (no rename); the staging task ends after this.
    FsyncInPlace(oneshot::Sender<Result<(), BlobError>>),
    /// Unlink the staging file; the staging task ends after this.
    Abort(oneshot::Sender<Result<(), BlobError>>),
}

impl UringStaging {
    /// Create the staging tmp file on an executor thread and start its long-lived writer task.
    pub(crate) async fn create(staging: PathBuf) -> Result<Self, BlobError> {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<WriteCmd>(8);
        let (ready_tx, ready_rx) = oneshot::channel::<Result<(), BlobError>>();
        let (final_tx, final_rx) = oneshot::channel::<Result<(), BlobError>>();

        let exec = executor();
        // Spawn one long-lived task on the executor that owns the staging fd for its whole life.
        // It is *detached* (not awaited here): it runs until a terminal commit/abort command, and
        // reports readiness/results over the channels below.
        exec.spawn_detached(move || async move {
            writer_task(staging, cmd_rx, ready_tx, final_tx).await;
        })?;

        // Wait for the file to actually be created before returning success.
        ready_rx
            .await
            .map_err(|_| BlobError::Io("io_uring staging task ended early".into()))??;
        Ok(Self {
            cmd_tx,
            final_rx: Some(final_rx),
        })
    }

    /// Append `buf` to the staging file (positional write at the running offset), awaiting its
    /// completion so write errors (e.g. ENOSPC) propagate at the same point they would on the
    /// `tokio::fs` path.
    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> Result<(), BlobError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(WriteCmd::Chunk(buf.to_vec(), ack_tx))
            .await
            .map_err(|_| BlobError::Io("io_uring staging writer stopped".into()))?;
        ack_rx
            .await
            .map_err(|_| BlobError::Io("io_uring staging writer dropped a write".into()))?
    }

    /// Commit the staged file durably up to the rename: fdatasync the file, then rename it into
    /// `final_path` — matching the `tokio::fs` path. The caller issues the coalesced
    /// destination-directory fsync afterward (see [`crate::commit::DirSyncCoalescer`]).
    pub(crate) async fn commit(mut self, final_path: PathBuf) -> Result<(), BlobError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(WriteCmd::Commit {
                final_path,
                reply: reply_tx,
            })
            .await
            .map_err(|_| BlobError::Io("io_uring staging writer stopped".into()))?;
        let _ = reply_rx
            .await
            .map_err(|_| BlobError::Io("io_uring staging writer dropped the commit".into()))?;
        // Also await the terminal channel so the writer task is fully wound down.
        if let Some(final_rx) = self.final_rx.take() {
            return final_rx
                .await
                .map_err(|_| BlobError::Io("io_uring staging task ended early".into()))?;
        }
        Ok(())
    }

    /// Flush+fsync the staged file in place (no rename), for multipart parts.
    pub(crate) async fn fsync_in_place(mut self) -> Result<(), BlobError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(WriteCmd::FsyncInPlace(reply_tx))
            .await
            .map_err(|_| BlobError::Io("io_uring staging writer stopped".into()))?;
        let res = reply_rx
            .await
            .map_err(|_| BlobError::Io("io_uring staging writer dropped the fsync".into()))?;
        if let Some(final_rx) = self.final_rx.take() {
            let _ = final_rx.await;
        }
        res
    }

    /// Abort the staged write: unlink the tmp file (best-effort on the executor).
    pub(crate) async fn abort(mut self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.cmd_tx.send(WriteCmd::Abort(reply_tx)).await.is_ok() {
            let _ = reply_rx.await;
        }
        if let Some(final_rx) = self.final_rx.take() {
            let _ = final_rx.await;
        }
    }
}

/// The long-lived executor-side task that owns one staging file's fd. It creates the file, acks
/// readiness, then serves chunk-append/commit/abort commands until a terminal command arrives.
async fn writer_task(
    staging: PathBuf,
    mut cmd_rx: tokio::sync::mpsc::Receiver<WriteCmd>,
    ready_tx: oneshot::Sender<Result<(), BlobError>>,
    final_tx: oneshot::Sender<Result<(), BlobError>>,
) {
    let file = match tokio_uring::fs::File::create(&staging).await {
        Ok(f) => {
            let _ = ready_tx.send(Ok(()));
            f
        }
        Err(e) => {
            let _ = ready_tx.send(Err(io_err(e)));
            let _ = final_tx.send(Ok(()));
            return;
        }
    };
    let mut offset: u64 = 0;

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            WriteCmd::Chunk(buf, reply) => {
                let len = buf.len() as u64;
                // write_all_at submits the buffer and retries short writes internally, returning
                // the buffer back to us (io_uring requires owned buffers).
                let (res, _buf) = file.write_all_at(buf, offset).await;
                match res {
                    Ok(()) => {
                        offset += len;
                        let _ = reply.send(Ok(()));
                    }
                    Err(e) => {
                        let _ = reply.send(Err(io_err(e)));
                    }
                }
            }
            WriteCmd::Commit { final_path, reply } => {
                let result = commit_on_executor(&staging, file, &final_path).await;
                let _ = reply.send(result.clone_shallow());
                let _ = final_tx.send(result);
                return;
            }
            WriteCmd::FsyncInPlace(reply) => {
                // fdatasync: the part's bytes + size must be durable; timestamps need not be.
                let res = file.sync_data().await.map_err(io_err);
                let _ = file.close().await;
                let _ = reply.send(res.clone_shallow());
                let _ = final_tx.send(res);
                return;
            }
            WriteCmd::Abort(reply) => {
                // Close then unlink; both best-effort, but surface the unlink result.
                let _ = file.close().await;
                let res = tokio_uring::fs::remove_file(&staging).await.map_err(io_err);
                let _ = reply.send(res.clone_shallow());
                let _ = final_tx.send(Ok(()));
                return;
            }
        }
    }
    // Sender dropped without a terminal command: best-effort clean up the orphaned tmp.
    let _ = file.close().await;
    let _ = tokio_uring::fs::remove_file(&staging).await;
    let _ = final_tx.send(Ok(()));
}

/// The durable commit up to the rename, issued as io_uring ops on the executor thread that owns
/// `file`: fdatasync the staged file, then rename it into place. The destination-directory fsync
/// (F-1 step 3) is the caller's coalesced step (see [`crate::commit::DirSyncCoalescer`]); this
/// matches the `tokio::fs` path step for step.
async fn commit_on_executor(
    staging: &Path,
    file: tokio_uring::fs::File,
    final_path: &Path,
) -> Result<(), BlobError> {
    // 1) fdatasync the staged file: persist its bytes and size, skipping the timestamp-only
    //    metadata `sync_all` would also flush — one fewer journal write per PUT (ARCH §8.2).
    file.sync_data().await.map_err(io_err)?;
    file.close().await.map_err(io_err)?;
    // 2) rename the staged file into the (already-ensured) bucket directory.
    tokio_uring::fs::rename(staging, final_path)
        .await
        .map_err(io_err)?;
    Ok(())
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
