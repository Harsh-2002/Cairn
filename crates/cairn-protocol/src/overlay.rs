//! Bounded in-memory overlay for acknowledged object writes whose SQLite fold is still pending.
//!
//! An ordinary PUT acknowledges after the data file (and its metadata) is durable. The index fold
//! follows asynchronously so the acknowledgement never waits for the Writer. Until that fold lands,
//! reads resolve the newest row from this overlay and every mutation that needs the index drains it
//! first. The overlay is bounded; when it is full the caller falls back to the synchronous path.
//!
//! The fold task is the only writer of overlay entries into the metadata store, and it processes
//! the FIFO queue in order, so version visibility and the index's eventual state match the order in
//! which objects were acknowledged.

use cairn_types::error::MetaError;
use cairn_types::meta::{Mutation, MutationOutcome};
use cairn_types::object::ObjectVersionRow;
use cairn_types::traits::MetadataStore;
use cairn_types::{Bucket, BucketName, ObjectKey, Timestamp};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// The bucket incarnation an acknowledged row belongs to. A bucket deleted and re-created under
/// the same name is a different incarnation; a row from the old one must never be served for the
/// new one, even if its fold has not been resolved yet.
struct Latest {
    created_at: Timestamp,
    row: Arc<ObjectVersionRow>,
}

/// One acknowledged write waiting for its index fold.
struct FoldItem {
    bucket: BucketName,
    key: ObjectKey,
    row_id: String,
    mutation: Mutation,
    /// When the write was acknowledged, so the fold histogram measures acknowledgement-to-index.
    enqueued_at: Instant,
}

/// A request to wait until every prior write has been folded.
struct Barrier(tokio::sync::oneshot::Sender<Result<(), MetaError>>);

enum QueueItem {
    // Boxed: a FoldItem carries a full publication mutation (256+ bytes), and the queue is a
    // single-producer channel whose entries are almost always this variant.
    Write(Box<FoldItem>),
    Barrier(Barrier),
}

/// Striped per-key serialization. Every object mutation and every index-dependent read takes the
/// key's stripe, so a conditional write, a delete, or a drain cannot interleave with an
/// acknowledgement of the same key.
const STRIPES: usize = 256;

/// The overlay itself.
pub struct Overlay {
    latest: RwLock<HashMap<(BucketName, ObjectKey), Latest>>,
    tx: tokio::sync::mpsc::Sender<QueueItem>,
    stripes: Box<[tokio::sync::Mutex<()>]>,
    /// Rows whose fold exhausted its retries. A retained row is still served from `latest`, so a
    /// barrier that covers it must report failure instead of pretending the index is complete.
    failed: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for Overlay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Overlay").finish_non_exhaustive()
    }
}

impl Overlay {
    /// Start the fold task and return the shared overlay.
    #[must_use]
    pub fn start(meta: Arc<dyn MetadataStore>, capacity: usize) -> Arc<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel::<QueueItem>(capacity.max(1));
        let overlay = Arc::new(Self {
            latest: RwLock::new(HashMap::new()),
            tx,
            stripes: (0..STRIPES)
                .map(|_| tokio::sync::Mutex::new(()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            failed: std::sync::atomic::AtomicU64::new(0),
        });
        let fold = Arc::downgrade(&overlay);
        tokio::spawn(async move {
            fold_loop(meta, fold, rx).await;
        });
        overlay
    }

    /// Wait until this key has no acknowledged-but-unfolded row. A synchronous publication for the
    /// same key uses this instead of draining the whole queue, so one slow fold never stalls every
    /// other key's writes.
    pub async fn wait_key(&self, bucket: &Bucket, key: &ObjectKey) -> Result<(), MetaError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while self.latest(bucket, key).is_some() {
            if tokio::time::Instant::now() >= deadline {
                return Err(MetaError::Engine(
                    "object index fold did not catch up for this key within 30s".into(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok(())
    }

    /// The key's stripe, held across the acknowledgement or a drain.
    pub async fn stripe(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> tokio::sync::MutexGuard<'_, ()> {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in bucket.as_str().bytes().chain(key.as_str().bytes()) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        let index = (hash % STRIPES as u64) as usize;
        self.stripes[index].lock().await
    }

    /// Rows acknowledged but not yet folded (unique keys, not counting the in-flight window).
    fn record_pending(&self) {
        let pending = self
            .latest
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        metrics::gauge!("cairn_overlay_pending").set(pending as f64);
    }

    /// The newest acknowledged row for a key that is not yet folded, provided it belongs to the
    /// caller's bucket incarnation.
    #[must_use]
    pub fn latest(&self, bucket: &Bucket, key: &ObjectKey) -> Option<Arc<ObjectVersionRow>> {
        self.latest
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(bucket.name.clone(), key.clone()))
            .filter(|entry| entry.created_at == bucket.created_at)
            .map(|entry| Arc::clone(&entry.row))
    }

    /// Acknowledge `row` and queue its fold. The caller holds the key's stripe.
    pub async fn acknowledge(
        &self,
        bucket: &Bucket,
        key: &ObjectKey,
        row: Arc<ObjectVersionRow>,
        mutation: Mutation,
    ) {
        self.latest
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                (bucket.name.clone(), key.clone()),
                Latest {
                    created_at: bucket.created_at,
                    row: Arc::clone(&row),
                },
            );
        self.record_pending();
        let item = QueueItem::Write(Box::new(FoldItem {
            bucket: bucket.name.clone(),
            key: key.clone(),
            row_id: row.id.clone(),
            mutation,
            enqueued_at: Instant::now(),
        }));
        if self.tx.send(item).await.is_err() {
            // The fold task is gone; the write stays in the overlay and the startup rebuild
            // repairs the index. Never fail an already-durable acknowledgement for this.
            tracing::error!(
                bucket = bucket.name.as_str(),
                key = key.as_str(),
                "object index fold queue is closed"
            );
        }
    }

    /// Wait until every acknowledged write has been folded. Mutations and LIST call this before
    /// consulting the index so they never act on a stale view.
    pub async fn drain(&self) -> Result<(), MetaError> {
        let failed_before = self.failed.load(std::sync::atomic::Ordering::Acquire);
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.tx.send(QueueItem::Barrier(Barrier(tx))).await.is_err() {
            return Err(MetaError::Engine(
                "object index fold queue is closed".into(),
            ));
        }
        let outcome = match tokio::time::timeout(Duration::from_secs(30), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(MetaError::Engine("object index fold aborted".into())),
            Err(_) => Err(MetaError::Engine(
                "object index fold did not catch up within 30s".into(),
            )),
        };
        outcome?;
        if self.failed.load(std::sync::atomic::Ordering::Acquire) != failed_before {
            return Err(MetaError::Engine(
                "object index fold failed; the index is incomplete until the next rebuild".into(),
            ));
        }
        Ok(())
    }

    fn forget(&self, bucket: &BucketName, key: &ObjectKey, row_id: &str) {
        let mut latest = self
            .latest
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if latest
            .get(&(bucket.clone(), key.clone()))
            .is_some_and(|entry| entry.row.id == row_id)
        {
            latest.remove(&(bucket.clone(), key.clone()));
        }
        drop(latest);
        self.record_pending();
    }
}

/// Drive the index fold as a continuous pipeline.
///
/// The previous shape assembled a batch of up to 64 items, submitted them together, and waited for
/// every ack before reading the next batch. That left the Writer idle between batches: it applied a
/// batch, the fold task woke, rebuilt the batch, and only then did new work arrive. Under a steady
/// write load the Writer therefore never stayed saturated.
///
/// This loop keeps a bounded window of submissions in flight at all times. Completions are harvested
/// as they arrive, so one slow row never delays the rows behind it, and the Writer always has queued
/// work to coalesce into its next commit.
///
/// Submission order is still strict FIFO, which the index depends on for same-key overwrites:
/// `FuturesUnordered`'s ready queue is a FIFO MPSC queue, so a freshly pushed future is polled (and
/// therefore sends its mutation) before every later one, and the Writer's bounded channel hands out
/// permits through a FIFO-fair semaphore, so even a send that parks under backpressure cannot be
/// overtaken. One task plus one channel is what makes that hold.
///
/// A barrier stops admission, waits for the window to empty, and only then reports, so nothing
/// queued after it can fold before it completes.
async fn fold_loop(
    meta: Arc<dyn MetadataStore>,
    overlay: std::sync::Weak<Overlay>,
    mut rx: tokio::sync::mpsc::Receiver<QueueItem>,
) {
    use futures_util::StreamExt;
    use futures_util::stream::FuturesUnordered;

    let mut inflight: FuturesUnordered<Fold> = FuturesUnordered::new();
    let mut barriers: std::collections::VecDeque<Barrier> = std::collections::VecDeque::new();
    let mut closed = false;
    loop {
        if let Some(barrier) = barriers.pop_front() {
            while let Some((item, attempts, result)) = inflight.next().await {
                settle(&meta, &overlay, &mut inflight, item, attempts, result).await;
            }
            let _ = barrier.0.send(Ok(()));
            continue;
        }
        if closed {
            if let Some((item, attempts, result)) = inflight.next().await {
                settle(&meta, &overlay, &mut inflight, item, attempts, result).await;
                continue;
            }
            return;
        }
        if inflight.len() >= MAX_IN_FLIGHT {
            let (item, attempts, result) = inflight
                .next()
                .await
                .expect("in-flight window is not empty");
            settle(&meta, &overlay, &mut inflight, item, attempts, result).await;
            continue;
        }
        tokio::select! {
            biased;
            // Harvest first: a completed fold frees a slot and keeps the window moving.
            Some((item, attempts, result)) = inflight.next(), if !inflight.is_empty() => {
                settle(&meta, &overlay, &mut inflight, item, attempts, result).await;
            }
            maybe = rx.recv() => match maybe {
                Some(QueueItem::Write(item)) => inflight.push(submit(&meta, *item, 0)),
                Some(QueueItem::Barrier(barrier)) => barriers.push_back(barrier),
                None => closed = true,
            },
        }
    }
}

/// Rows submitted but not yet acknowledged. Large enough to keep the Writer's own 256-row batch
/// full several times over, small enough to bound memory while the fold stalls.
const MAX_IN_FLIGHT: usize = 1024;
/// A row that keeps failing is retried a few times without blocking its neighbours, then left in
/// the overlay for reads and repaired by the startup rebuild.
const MAX_ATTEMPTS: u32 = 4;

type Fold = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = (FoldItem, u32, Result<MutationOutcome, MetaError>)>
            + Send,
    >,
>;

fn submit(meta: &Arc<dyn MetadataStore>, item: FoldItem, attempts: u32) -> Fold {
    let meta = Arc::clone(meta);
    let mutation = item.mutation.clone();
    Box::pin(async move { (item, attempts, meta.submit(mutation).await) })
}

/// Apply one fold outcome: forget the overlay row on success, or requeue a bounded retry.
async fn settle(
    meta: &Arc<dyn MetadataStore>,
    overlay: &std::sync::Weak<Overlay>,
    inflight: &mut futures_util::stream::FuturesUnordered<Fold>,
    item: FoldItem,
    attempts: u32,
    result: Result<MutationOutcome, MetaError>,
) {
    let forget = match result {
        Ok(MutationOutcome::StoragePublicationNotApplied) => {
            // A vanished bucket can never apply, and neither can a row from a previous incarnation
            // of a bucket that was deleted and re-created under the same name; a conflicting path
            // inside the same incarnation is retried instead.
            match meta.get_bucket(&item.bucket).await {
                Ok(None) => true,
                Ok(Some(current)) => match &item.mutation {
                    Mutation::PublishObjectWrite { plan, .. } => {
                        plan.bucket_created_at != Some(current.created_at)
                            || plan.bucket_owner.as_ref() != Some(&current.owner_id)
                    }
                    _ => false,
                },
                Err(_) => false,
            }
        }
        Ok(_) => true,
        Err(error) => {
            tracing::warn!(
                bucket = item.bucket.as_str(),
                key = item.key.as_str(),
                %error,
                "object index fold failed; retrying"
            );
            false
        }
    };
    if forget {
        if let Some(overlay) = overlay.upgrade() {
            overlay.forget(&item.bucket, &item.key, &item.row_id);
        }
        metrics::histogram!("cairn_overlay_fold_seconds")
            .record(item.enqueued_at.elapsed().as_secs_f64());
        return;
    }
    if attempts + 1 < MAX_ATTEMPTS {
        inflight.push(submit(meta, item, attempts + 1));
        return;
    }
    if let Some(overlay) = overlay.upgrade() {
        overlay
            .failed
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }
    metrics::counter!("cairn_overlay_fold_failures_total").increment(1);
    tracing::error!(
        bucket = item.bucket.as_str(),
        key = item.key.as_str(),
        "object index fold failed after retries; retaining the overlay entry"
    );
}
