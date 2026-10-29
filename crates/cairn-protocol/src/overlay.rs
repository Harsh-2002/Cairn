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
use cairn_types::{BucketName, ObjectKey};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// One acknowledged write waiting for its index fold.
struct FoldItem {
    bucket: BucketName,
    key: ObjectKey,
    row_id: String,
    mutation: Mutation,
}

/// A request to wait until every prior write has been folded.
struct Barrier(tokio::sync::oneshot::Sender<Result<(), MetaError>>);

enum QueueItem {
    Write(FoldItem),
    Barrier(Barrier),
}

/// Striped per-key serialization. Every object mutation and every index-dependent read takes the
/// key's stripe, so a conditional write, a delete, or a drain cannot interleave with an
/// acknowledgement of the same key.
const STRIPES: usize = 256;

/// The overlay itself.
pub struct Overlay {
    latest: RwLock<HashMap<(BucketName, ObjectKey), Arc<ObjectVersionRow>>>,
    tx: tokio::sync::mpsc::Sender<QueueItem>,
    stripes: Box<[tokio::sync::Mutex<()>]>,
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<QueueItem>(capacity.max(1));
        let overlay = Arc::new(Self {
            latest: RwLock::new(HashMap::new()),
            tx,
            stripes: (0..STRIPES)
                .map(|_| tokio::sync::Mutex::new(()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        });
        let fold = Arc::downgrade(&overlay);
        tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut items: Vec<FoldItem> = Vec::with_capacity(64);
                let mut barriers: Vec<Barrier> = Vec::new();
                match first {
                    QueueItem::Barrier(barrier) => {
                        let _ = barrier.0.send(Ok(()));
                        continue;
                    }
                    QueueItem::Write(item) => items.push(item),
                }
                // Fill one batch from the queue. A barrier ends the batch: it may only complete
                // after every write enqueued before it has folded.
                while items.len() < 64 {
                    match rx.try_recv() {
                        Ok(QueueItem::Write(item)) => items.push(item),
                        Ok(QueueItem::Barrier(barrier)) => {
                            barriers.push(barrier);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                fold_batch(&*meta, &fold, items).await;
                for barrier in barriers {
                    let _ = barrier.0.send(Ok(()));
                }
            }
        });
        overlay
    }

    /// Wait until this key has no acknowledged-but-unfolded row. A synchronous publication for the
    /// same key uses this instead of draining the whole queue, so one slow fold never stalls every
    /// other key's writes.
    pub async fn wait_key(&self, bucket: &BucketName, key: &ObjectKey) -> Result<(), MetaError> {
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

    /// The newest acknowledged row for a key that is not yet folded.
    #[must_use]
    pub fn latest(&self, bucket: &BucketName, key: &ObjectKey) -> Option<Arc<ObjectVersionRow>> {
        self.latest
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(bucket.clone(), key.clone()))
            .cloned()
    }

    /// Acknowledge `row` and queue its fold. The caller holds the key's stripe.
    pub async fn acknowledge(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        row: Arc<ObjectVersionRow>,
        mutation: Mutation,
    ) {
        self.latest
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert((bucket.clone(), key.clone()), Arc::clone(&row));
        let item = QueueItem::Write(FoldItem {
            bucket: bucket.clone(),
            key: key.clone(),
            row_id: row.id.clone(),
            mutation,
        });
        if self.tx.send(item).await.is_err() {
            // The fold task is gone; the write stays in the overlay and the startup rebuild
            // repairs the index. Never fail an already-durable acknowledgement for this.
            tracing::error!(
                bucket = bucket.as_str(),
                key = key.as_str(),
                "object index fold queue is closed"
            );
        }
    }

    /// Wait until every acknowledged write has been folded. Mutations and LIST call this before
    /// consulting the index so they never act on a stale view.
    pub async fn drain(&self) -> Result<(), MetaError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.tx.send(QueueItem::Barrier(Barrier(tx))).await.is_err() {
            return Err(MetaError::Engine(
                "object index fold queue is closed".into(),
            ));
        }
        match tokio::time::timeout(Duration::from_secs(30), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(MetaError::Engine("object index fold aborted".into())),
            Err(_) => Err(MetaError::Engine(
                "object index fold did not catch up within 30s".into(),
            )),
        }
    }

    fn forget(&self, bucket: &BucketName, key: &ObjectKey, row_id: &str) {
        let mut latest = self
            .latest
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if latest
            .get(&(bucket.clone(), key.clone()))
            .is_some_and(|row| row.id == row_id)
        {
            latest.remove(&(bucket.clone(), key.clone()));
        }
    }
}

/// Fold a batch of acknowledged writes. The futures are polled in queue order, so they enter the
/// Writer's channel in that order and the Writer applies them in the same order while amortising
/// one commit across the whole batch.
async fn fold_batch(
    meta: &dyn MetadataStore,
    overlay: &std::sync::Weak<Overlay>,
    items: Vec<FoldItem>,
) {
    use futures_util::future::join_all;
    let mut retry: Vec<FoldItem> = Vec::new();
    let futures = items.iter().map(|item| meta.submit(item.mutation.clone()));
    let results = join_all(futures).await;
    for (item, result) in items.into_iter().zip(results) {
        match result {
            Ok(MutationOutcome::Put { .. }) => {
                if let Some(overlay) = overlay.upgrade() {
                    overlay.forget(&item.bucket, &item.key, &item.row_id);
                }
            }
            Ok(MutationOutcome::StoragePublicationNotApplied) => {
                // A vanished bucket can never apply; a conflicting path is retried a few times.
                match meta.get_bucket(&item.bucket).await {
                    Ok(None) => {
                        if let Some(overlay) = overlay.upgrade() {
                            overlay.forget(&item.bucket, &item.key, &item.row_id);
                        }
                    }
                    _ => retry.push(item),
                }
            }
            Ok(_) => {
                if let Some(overlay) = overlay.upgrade() {
                    overlay.forget(&item.bucket, &item.key, &item.row_id);
                }
            }
            Err(error) => {
                tracing::warn!(
                    bucket = item.bucket.as_str(),
                    key = item.key.as_str(),
                    %error,
                    "object index fold failed; retrying"
                );
                retry.push(item);
            }
        }
    }
    // Bounded retry of the failed tail; a persistent failure keeps its overlay entry for reads and
    // is repaired by the startup rebuild.
    for attempt in 1..=20u32 {
        if retry.is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(u64::from(attempt.min(10)) * 2)).await;
        let mut next = Vec::new();
        for item in retry.drain(..) {
            match meta.submit(item.mutation.clone()).await {
                Ok(MutationOutcome::Put { .. })
                | Ok(MutationOutcome::StoragePublicationNotApplied) => {
                    if let Some(overlay) = overlay.upgrade() {
                        overlay.forget(&item.bucket, &item.key, &item.row_id);
                    }
                }
                Ok(_) => {
                    if let Some(overlay) = overlay.upgrade() {
                        overlay.forget(&item.bucket, &item.key, &item.row_id);
                    }
                }
                Err(_) => next.push(item),
            }
        }
        retry = next;
    }
    for item in retry {
        tracing::error!(
            bucket = item.bucket.as_str(),
            key = item.key.as_str(),
            "object index fold failed after retries; retaining the overlay entry"
        );
    }
}
