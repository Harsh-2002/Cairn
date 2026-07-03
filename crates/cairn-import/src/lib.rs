//! The S3 → Cairn import engine (ARCH 27).
//!
//! Copies buckets and their objects from a remote S3-compatible store (MinIO / Garage / R2 / AWS /
//! another Cairn) **into this node**. It is trait-generic — like `cairn-replication`, it holds no
//! backend, only [`ImportOpts`] — so it is exercised entirely against in-memory doubles in tests and
//! wired to concrete impls by `cairn-server`.
//!
//! Three seams:
//! - [`SourceReader`] — the remote read side: list buckets, page objects, and **stream** an object's
//!   body. The production impl is [`HttpS3Source`] (signed GET/List over the SSRF-guarded connector).
//! - [`DestWriter`] — where objects land. The server's `LocalDestWriter` drives Cairn's real object
//!   write path (so encryption / compression / quota / versioning apply); tests use a recorder.
//! - [`ProgressSink`] — the engine reports per-bucket progress + cursors after each page so the job
//!   is durably resumable, and the sink returns `false` to request cooperative cancellation.
//!
//! ## Scale is bounded by construction
//! Enumeration streams (paged `ListObjectsV2`, never list-all-into-memory); each page's objects copy
//! under a per-bucket `object_workers` fan-out **and** a single global [`Semaphore`] whose permit
//! ceiling caps total in-flight work across every bucket — so peak memory ≈ `global_max_inflight ×
//! one streamed body`, independent of object count. Object bodies stream source→dest and are never
//! buffered whole. Resume is one cursor per `(job, bucket)`, carried in [`BucketPlan`].

mod source;

pub use source::{HttpS3Source, SourceConfig};

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use cairn_types::meta::{ImportBucketProgress, ImportState};
use futures_util::StreamExt;
use tokio::sync::Semaphore;

/// Why a copy step did not succeed. The three classes drive the retry policy, mirroring
/// `cairn-replication`'s taxonomy: `Retryable`/`Unavailable` back off and retry up to the attempt
/// budget; `Terminal` fails that object immediately (the job continues past it).
#[derive(Debug, Clone)]
pub enum ImportError {
    /// A transient failure (a read/network error): retry after backoff.
    Retryable(String),
    /// The source or destination is unavailable (transport error / 5xx / 429): retry with patience.
    Unavailable(String),
    /// A permanent failure for this object (a 4xx, a missing object, a malformed response).
    Terminal(String),
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportError::Retryable(m) => write!(f, "retryable: {m}"),
            ImportError::Unavailable(m) => write!(f, "unavailable: {m}"),
            ImportError::Terminal(m) => write!(f, "terminal: {m}"),
        }
    }
}

impl std::error::Error for ImportError {}

/// One object as summarized by a source listing.
#[derive(Debug, Clone)]
pub struct RemoteObject {
    /// The object key.
    pub key: String,
    /// The object size in bytes.
    pub size: u64,
    /// The source ETag, if the listing exposed it.
    pub etag: Option<String>,
}

/// One page of a source object listing.
#[derive(Debug, Clone)]
pub struct ObjectPage {
    /// The objects on this page.
    pub objects: Vec<RemoteObject>,
    /// The continuation token to fetch the next page, if [`is_truncated`](Self::is_truncated).
    pub next_cursor: Option<String>,
    /// Whether more pages follow.
    pub is_truncated: bool,
}

/// A source object with its metadata and a **streamed** body. The engine never materializes `body`;
/// it hands the stream straight to [`DestWriter::put_object`].
pub struct SourceObject {
    /// The object key.
    pub key: String,
    /// The object size in bytes (from the source `Content-Length`).
    pub size: u64,
    /// The source ETag, if present (for integrity checks by the destination).
    pub etag: Option<String>,
    /// The `Content-Type`, if present.
    pub content_type: Option<String>,
    /// User metadata (`x-amz-meta-*`), keys **without** the prefix, preserved on import.
    pub user_metadata: Vec<(String, String)>,
    /// `Content-Encoding`, if present.
    pub content_encoding: Option<String>,
    /// `Cache-Control`, if present.
    pub cache_control: Option<String>,
    /// `Content-Disposition`, if present.
    pub content_disposition: Option<String>,
    /// `Content-Language`, if present.
    pub content_language: Option<String>,
    /// The lazily-streamed object body.
    pub body: cairn_types::BlobStream,
}

impl std::fmt::Debug for SourceObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceObject")
            .field("key", &self.key)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

/// The remote read side of an import.
#[async_trait]
pub trait SourceReader: Send + Sync {
    /// List the buckets the source credentials can see.
    async fn list_buckets(&self) -> Result<Vec<String>, ImportError>;
    /// List one page of a bucket's objects (`ListObjectsV2`), resuming from `cursor`.
    async fn list_objects(
        &self,
        bucket: &str,
        cursor: Option<&str>,
        max_keys: u32,
    ) -> Result<ObjectPage, ImportError>;
    /// Fetch an object with its metadata and a streamed body.
    async fn get_object(&self, bucket: &str, key: &str) -> Result<SourceObject, ImportError>;
}

/// Where imported objects land. The server's impl drives Cairn's real object write path.
#[async_trait]
pub trait DestWriter: Send + Sync {
    /// Ensure the destination bucket exists (create it if absent; a no-op if it already exists).
    async fn ensure_bucket(&self, bucket: &str) -> Result<(), ImportError>;
    /// Write one object (streaming its body). The whole get+put is retried as a unit, so this may be
    /// called more than once for the same key with a fresh body.
    async fn put_object(&self, bucket: &str, obj: SourceObject) -> Result<(), ImportError>;
}

/// The engine's progress + cancellation channel. `report` is called after every page with the full
/// per-bucket snapshot (the server persists it as the resume checkpoint); returning `false` asks the
/// engine to stop cleanly at the next page boundary (operator cancel).
#[async_trait]
pub trait ProgressSink: Send + Sync {
    /// Persist `buckets` as the job's progress/checkpoint. Return `false` to request cancellation.
    async fn report(&self, buckets: &[ImportBucketProgress]) -> bool;
}

/// One bucket to import: a source→dest mapping plus the resume cursor and prior counters (so a
/// restarted job continues rather than re-copying).
#[derive(Debug, Clone)]
pub struct BucketPlan {
    /// The source bucket name.
    pub source_bucket: String,
    /// The destination bucket name on this node.
    pub dest_bucket: String,
    /// The `ListObjectsV2` continuation token to resume from; `None` starts at the beginning.
    pub cursor: Option<String>,
    /// Objects already copied in a prior run (for cumulative progress).
    pub objects_done: u64,
    /// Bytes already copied in a prior run.
    pub bytes_done: u64,
}

/// Engine tunables.
#[derive(Debug, Clone)]
pub struct ImportOpts {
    /// How many buckets to import concurrently.
    pub bucket_concurrency: usize,
    /// How many objects to copy concurrently within a bucket's page.
    pub object_workers: usize,
    /// The authoritative ceiling on total in-flight object copies across ALL buckets. Held below the
    /// blob-I/O permit pool so an import can't starve the node's live traffic.
    pub global_max_inflight: usize,
    /// `ListObjectsV2` page size (max-keys).
    pub list_page_size: u32,
    /// Per-object attempt budget before it is recorded failed.
    pub max_attempts: u32,
    /// Base backoff seconds (doubles per attempt).
    pub base_backoff_secs: u64,
    /// Backoff ceiling seconds.
    pub max_backoff_secs: u64,
}

impl Default for ImportOpts {
    fn default() -> Self {
        Self {
            bucket_concurrency: 4,
            object_workers: 16,
            global_max_inflight: 24,
            list_page_size: 1000,
            max_attempts: 8,
            base_backoff_secs: 5,
            max_backoff_secs: 900,
        }
    }
}

/// The outcome of an import run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportReport {
    /// Objects successfully copied.
    pub objects_copied: u64,
    /// Objects that failed after the attempt budget (recorded, not fatal to the job).
    pub objects_failed: u64,
    /// Bytes successfully copied.
    pub bytes_copied: u64,
    /// Whether the run stopped early because cancellation was requested.
    pub cancelled: bool,
}

/// Deterministic exponential backoff: `base * 2^(attempt-1)`, clamped to `max`.
fn next_backoff(attempt: u32, base_secs: u64, max_secs: u64) -> Duration {
    let shift = attempt.saturating_sub(1).min(32);
    let secs = base_secs.saturating_mul(1u64 << shift).min(max_secs);
    Duration::from_secs(secs)
}

/// The stateless import engine.
#[derive(Debug, Clone)]
pub struct ImportEngine {
    opts: ImportOpts,
}

/// Per-page copy tally, merged into the shared progress once per page (avoids per-object locking).
#[derive(Default)]
struct PageTally {
    objects_done: u64,
    bytes_done: u64,
    objects_failed: u64,
    last_error: Option<String>,
}

impl ImportEngine {
    /// Construct an engine with the given options.
    #[must_use]
    pub fn new(opts: ImportOpts) -> Self {
        Self { opts }
    }

    /// Run the import: copy every bucket in `plan` from `source` into `dest`, reporting progress via
    /// `progress` after each page. Returns an aggregate [`ImportReport`]. Per-object failures are
    /// recorded and never abort the job; a cancellation request (a `false` from `progress.report`)
    /// stops cleanly at the next page boundary.
    pub async fn run(
        &self,
        source: &dyn SourceReader,
        dest: &dyn DestWriter,
        plan: &[BucketPlan],
        progress: &dyn ProgressSink,
    ) -> ImportReport {
        // Shared, per-bucket progress snapshot — the checkpoint the sink persists. Seeded from the
        // plan so a resumed job's prior counters carry forward.
        let shared: Arc<Mutex<Vec<ImportBucketProgress>>> = Arc::new(Mutex::new(
            plan.iter()
                .map(|p| ImportBucketProgress {
                    source_bucket: p.source_bucket.clone(),
                    dest_bucket: p.dest_bucket.clone(),
                    objects_done: p.objects_done,
                    objects_total: p.objects_done,
                    bytes_done: p.bytes_done,
                    bytes_total: p.bytes_done,
                    cursor: p.cursor.clone(),
                    state: ImportState::Pending,
                    last_error: None,
                })
                .collect(),
        ));
        let global = Arc::new(Semaphore::new(self.opts.global_max_inflight.max(1)));
        let cancelled = Arc::new(AtomicBool::new(false));

        futures_util::stream::iter(plan.iter().enumerate())
            .for_each_concurrent(self.opts.bucket_concurrency.max(1), |(idx, plan)| {
                let shared = shared.clone();
                let global = global.clone();
                let cancelled = cancelled.clone();
                async move {
                    self.copy_bucket(
                        idx, plan, source, dest, progress, &shared, &global, &cancelled,
                    )
                    .await;
                }
            })
            .await;

        // Fold the final snapshot into the report.
        let snap = shared.lock().unwrap();
        let mut report = ImportReport {
            cancelled: cancelled.load(Ordering::SeqCst),
            ..Default::default()
        };
        for b in snap.iter() {
            report.objects_copied += b.objects_done;
            report.bytes_copied += b.bytes_done;
        }
        report
    }

    #[allow(clippy::too_many_arguments)]
    async fn copy_bucket(
        &self,
        idx: usize,
        plan: &BucketPlan,
        source: &dyn SourceReader,
        dest: &dyn DestWriter,
        progress: &dyn ProgressSink,
        shared: &Arc<Mutex<Vec<ImportBucketProgress>>>,
        global: &Arc<Semaphore>,
        cancelled: &Arc<AtomicBool>,
    ) {
        self.set_bucket_state(shared, idx, ImportState::Running);

        // Ensure the destination bucket exists first; a failure here fails the whole bucket.
        if let Err(e) = dest.ensure_bucket(&plan.dest_bucket).await {
            self.fail_bucket(shared, idx, format!("create destination bucket: {e}"));
            self.report(progress, shared).await;
            return;
        }

        let mut cursor = plan.cursor.clone();
        loop {
            if cancelled.load(Ordering::SeqCst) {
                self.set_bucket_state(shared, idx, ImportState::Cancelled);
                break;
            }

            // Page the source listing, retrying transient/unavailable errors.
            let page = match self
                .with_retry(|| {
                    source.list_objects(
                        &plan.source_bucket,
                        cursor.as_deref(),
                        self.opts.list_page_size,
                    )
                })
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    self.fail_bucket(shared, idx, format!("list objects: {e}"));
                    break;
                }
            };

            // Copy this page's objects under the per-bucket fan-out + the global in-flight cap.
            let page_bytes_total: u64 = page.objects.iter().map(|o| o.size).sum();
            let tally: PageTally = futures_util::stream::iter(page.objects)
                .map(|obj| {
                    let global = global.clone();
                    async move {
                        self.copy_one(
                            source,
                            dest,
                            &plan.source_bucket,
                            &plan.dest_bucket,
                            &obj,
                            &global,
                        )
                        .await
                    }
                })
                .buffer_unordered(self.opts.object_workers.max(1))
                .fold(PageTally::default(), |mut acc, r| async move {
                    match r {
                        Ok(bytes) => {
                            acc.objects_done += 1;
                            acc.bytes_done += bytes;
                        }
                        Err(e) => {
                            acc.objects_failed += 1;
                            acc.last_error = Some(e);
                        }
                    }
                    acc
                })
                .await;

            cursor = page.next_cursor.clone();
            self.advance_bucket(shared, idx, &tally, page_bytes_total, cursor.clone());

            // Persist the checkpoint; a `false` return is a cancellation request.
            if !self.report(progress, shared).await {
                cancelled.store(true, Ordering::SeqCst);
                self.set_bucket_state(shared, idx, ImportState::Cancelled);
                break;
            }

            if !page.is_truncated {
                self.set_bucket_state(shared, idx, ImportState::Completed);
                break;
            }
        }
        self.report(progress, shared).await;
    }

    /// Copy one object as an atomic get→put unit, retrying the whole unit on transient failure so a
    /// consumed body is re-fetched fresh. Returns the copied byte count, or a terminal error string.
    async fn copy_one(
        &self,
        source: &dyn SourceReader,
        dest: &dyn DestWriter,
        src_bucket: &str,
        dest_bucket: &str,
        obj: &RemoteObject,
        global: &Arc<Semaphore>,
    ) -> Result<u64, String> {
        let _permit = global.acquire().await.expect("global semaphore not closed");
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let res = async {
                let src = source.get_object(src_bucket, &obj.key).await?;
                let size = src.size;
                dest.put_object(dest_bucket, src).await?;
                Ok::<u64, ImportError>(size)
            }
            .await;
            match res {
                Ok(bytes) => return Ok(bytes),
                Err(ImportError::Terminal(e)) => return Err(e),
                Err(e) => {
                    if attempt >= self.opts.max_attempts {
                        return Err(e.to_string());
                    }
                    tokio::time::sleep(next_backoff(
                        attempt,
                        self.opts.base_backoff_secs,
                        self.opts.max_backoff_secs,
                    ))
                    .await;
                }
            }
        }
    }

    /// Retry a listing op on transient/unavailable errors up to the attempt budget.
    async fn with_retry<T, F, Fut>(&self, mut op: F) -> Result<T, ImportError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, ImportError>>,
    {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match op().await {
                Ok(v) => return Ok(v),
                Err(ImportError::Terminal(e)) => return Err(ImportError::Terminal(e)),
                Err(e) => {
                    if attempt >= self.opts.max_attempts {
                        return Err(e);
                    }
                    tokio::time::sleep(next_backoff(
                        attempt,
                        self.opts.base_backoff_secs,
                        self.opts.max_backoff_secs,
                    ))
                    .await;
                }
            }
        }
    }

    fn set_bucket_state(
        &self,
        shared: &Arc<Mutex<Vec<ImportBucketProgress>>>,
        idx: usize,
        state: ImportState,
    ) {
        if let Some(b) = shared.lock().unwrap().get_mut(idx) {
            b.state = state;
        }
    }

    fn fail_bucket(
        &self,
        shared: &Arc<Mutex<Vec<ImportBucketProgress>>>,
        idx: usize,
        error: String,
    ) {
        if let Some(b) = shared.lock().unwrap().get_mut(idx) {
            b.state = ImportState::Failed;
            b.last_error = Some(error);
        }
    }

    fn advance_bucket(
        &self,
        shared: &Arc<Mutex<Vec<ImportBucketProgress>>>,
        idx: usize,
        tally: &PageTally,
        page_bytes_total: u64,
        cursor: Option<String>,
    ) {
        if let Some(b) = shared.lock().unwrap().get_mut(idx) {
            b.objects_done += tally.objects_done;
            b.bytes_done += tally.bytes_done;
            // objects_total/bytes_total track everything SEEN so far (a running best-effort total).
            b.objects_total += tally.objects_done + tally.objects_failed;
            b.bytes_total += page_bytes_total;
            b.cursor = cursor;
            if tally.last_error.is_some() {
                b.last_error = tally.last_error.clone();
            }
        }
    }

    async fn report(
        &self,
        progress: &dyn ProgressSink,
        shared: &Arc<Mutex<Vec<ImportBucketProgress>>>,
    ) -> bool {
        let snapshot = shared.lock().unwrap().clone();
        progress.report(&snapshot).await
    }
}

#[cfg(test)]
mod tests;
