//! Explicit, bounded laboratory collection. No background worker or filesystem scan establishes
//! liveness: the SQLite actor selects exact records and conditionally relocates their locations.

use super::model::{ArtifactKind, GcCandidate, Location, MAX_RECORDS, RelocateRecord, Result};
use super::record;
use super::store::{RelocationOutcome, Store};
use crate::{Budget, check_deadline};
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;
use tokio::time::Instant;

// This covers up to 256 metadata rows (including 1024-byte keys), mappings and small receipts.
// Budget::acquire adds the existing 128-KiB allowance for overlapping I/O scratch buffers.
const METADATA_RESERVATION: usize = 512 * 1024;

#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub struct CollectionReport {
    pub examined: u64,
    pub candidates: u64,
    pub skipped: u64,
    pub copied_records: u64,
    pub copied_encoded_bytes: u64,
    pub source_physical_bytes: u64,
    pub source_dead_bytes: u64,
    pub replacement_physical_bytes: u64,
    pub moved_records: u64,
    pub lost_records: u64,
    pub moved_encoded_bytes: u64,
    pub lost_encoded_bytes: u64,
    pub retired_sources: u64,
    pub completed: bool,
}

impl CollectionReport {
    fn add(&mut self, next: &Self) {
        self.candidates += next.candidates;
        self.skipped += next.skipped;
        self.copied_records += next.copied_records;
        self.copied_encoded_bytes += next.copied_encoded_bytes;
        self.source_physical_bytes += next.source_physical_bytes;
        self.source_dead_bytes += next.source_dead_bytes;
        self.replacement_physical_bytes += next.replacement_physical_bytes;
        self.moved_records += next.moved_records;
        self.lost_records += next.lost_records;
        self.moved_encoded_bytes += next.moved_encoded_bytes;
        self.lost_encoded_bytes += next.lost_encoded_bytes;
        self.retired_sources += next.retired_sources;
    }
}

#[derive(Default)]
struct Hooks {
    #[cfg(test)]
    after_copy: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Hooks {
    fn copied(&self) {
        #[cfg(test)]
        if let Some(hook) = &self.after_copy {
            hook();
        }
    }
}

pub(crate) async fn collect_one(
    root: &Path,
    store: Store,
    budget: Arc<Budget>,
    candidate: &GcCandidate,
    deadline: Instant,
) -> Result<CollectionReport> {
    collect_one_with(root, store, budget, candidate, deadline, Hooks::default()).await
}

async fn collect_one_with(
    root: &Path,
    store: Store,
    budget: Arc<Budget>,
    candidate: &GcCandidate,
    deadline: Instant,
    hooks: Hooks,
) -> Result<CollectionReport> {
    let admission = budget.acquire(METADATA_RESERVATION, deadline).await?;
    let root = root.to_owned();
    let identity = candidate.artifact.clone();
    let runtime = tokio::runtime::Handle::current();
    // Once running, this closure owns memory admission, source descriptors and the node lifetime
    // through the Writer result, even if the awaiting task is cancelled or its deadline passes.
    tokio::task::spawn_blocking(move || {
        let _admission = admission;
        check_deadline(deadline)?;
        let Some(source) = runtime.block_on(store.pin_gc(&identity))? else {
            return Ok(CollectionReport {
                candidates: 1,
                skipped: 1,
                completed: true,
                ..Default::default()
            });
        };
        let length =
            record::segment_length(source.records.iter().map(|row| row.location.length()))?;
        if source.records.len() != source.candidate.records
            || length != source.candidate.retained_length
            || source
                .candidate
                .physical_length
                .checked_sub(length)
                .is_none_or(|dead| dead < source.candidate.physical_length.div_ceil(2))
        {
            return Err("GC candidate geometry differs from pinned authority".into());
        }
        check_deadline(deadline)?;
        let plan = runtime.block_on(store.plan(ArtifactKind::Segment, length))?;
        if let Err(error) = check_deadline(deadline) {
            runtime.block_on(store.abort(record::abort(plan)))?;
            return Err(error);
        }
        let artifact = match record::copy_segment(&root, plan, &source.pin, &source.records) {
            Ok(artifact) => artifact,
            Err(error) => {
                let (error, quiescent) = error.into_parts();
                runtime.block_on(store.abort(quiescent))?;
                return Err(error.into());
            }
        };
        let mut report = CollectionReport {
            candidates: 1,
            copied_records: source.records.len() as u64,
            copied_encoded_bytes: source.records.iter().map(|row| row.location.length()).sum(),
            source_physical_bytes: source.candidate.physical_length,
            source_dead_bytes: source.candidate.physical_length - length,
            replacement_physical_bytes: artifact.physical_length(),
            completed: true,
            ..Default::default()
        };
        let mappings = source
            .records
            .iter()
            .zip(artifact.spans())
            .map(|(row, span)| RelocateRecord {
                row_id: row.metadata.row_id.clone(),
                expected: row.location.clone(),
                replacement: Location::Segment {
                    artifact: artifact.plan().artifact().clone(),
                    offset: span.offset,
                    length: span.length,
                },
            })
            .collect();
        hooks.copied();
        // A completed durable copy is resolved through the actor even after a deadline. Returning
        // early here would strand a known finished operation merely because its caller stopped.
        match runtime.block_on(store.relocate(artifact, mappings))? {
            RelocationOutcome::Applied {
                moved,
                lost,
                moved_bytes,
                lost_bytes,
                source_retired,
            } => {
                report.moved_records = moved as u64;
                report.lost_records = lost as u64;
                report.moved_encoded_bytes = moved_bytes;
                report.lost_encoded_bytes = lost_bytes;
                report.retired_sources = u64::from(source_retired);
            }
            RelocationOutcome::Rejected { reason, artifact } => {
                runtime.block_on(store.abort(artifact.into_quiescent()))?;
                return Err(format!("GC relocation rejected: {reason:?}").into());
            }
        }
        drop(source);
        Ok(report)
    })
    .await?
}

/// One cursor pass with explicit artifact/page ceilings. Physical cleanup remains the existing
/// exact-debt drain, whose unrelated work must not be attributed to collection's copied bytes.
pub(crate) async fn collect_pass(
    root: &Path,
    store: Store,
    budget: Arc<Budget>,
    page_size: usize,
    max_pages: usize,
    deadline: Instant,
) -> Result<CollectionReport> {
    if !(1..=MAX_RECORDS).contains(&page_size) || !(1..=MAX_RECORDS).contains(&max_pages) {
        return Err("invalid bounded collection pass".into());
    }
    let mut report = CollectionReport::default();
    let mut cursor = None;
    for _ in 0..max_pages {
        check_deadline(deadline)?;
        let page_guard = budget.acquire(METADATA_RESERVATION, deadline).await?;
        let page_store = store.clone();
        let page_cursor = cursor.clone();
        let runtime = tokio::runtime::Handle::current();
        // The actor may finish a cancelled request later. Keep the page reservation with the
        // real request and its returned rows, rather than only with the awaiting future.
        let (page, _page_guard) = tokio::task::spawn_blocking(move || {
            let page =
                runtime.block_on(page_store.gc_candidates(page_cursor.as_ref(), page_size))?;
            Ok::<_, super::model::Error>((page, page_guard))
        })
        .await??;
        if page.examined > page_size
            || page.candidates.len() > page.examined
            || page
                .after
                .as_ref()
                .is_some_and(|next| cursor.as_ref().is_some_and(|prior| next <= prior))
        {
            return Err("invalid collection candidate page".into());
        }
        report.examined += page.examined as u64;
        for candidate in page.candidates {
            let collected =
                collect_one(root, store.clone(), budget.clone(), &candidate, deadline).await?;
            report.add(&collected);
        }
        cursor = page.after;
        if page.examined < page_size || cursor.is_none() {
            report.completed = true;
            break;
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::super::model::{
        CipherFormat, EncodedFormat, ExpectedCurrent, PhysicalBudget, PublishRecord,
        PublishedRecord, RecordMetadata,
    };
    use super::super::node::Node;
    use super::super::store::{DeleteOutcome, PublicationOutcome};
    use super::*;
    use cairn_types::CompressionDescriptor;
    use cairn_types::storage::StorageToken;
    use std::io::Read;
    use std::sync::Mutex;
    use std::time::Duration;

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(30)
    }

    async fn publish(root: &Path, store: &Store, lengths: &[usize]) -> Vec<PublishedRecord> {
        let payloads: Vec<_> = lengths
            .iter()
            .enumerate()
            .map(|(index, length)| vec![index as u8 + 31; *length])
            .collect();
        let slices: Vec<_> = payloads.iter().map(Vec::as_slice).collect();
        let length = record::segment_length(lengths.iter().map(|length| *length as u64)).unwrap();
        let admission = store.plan(ArtifactKind::Segment, length).await.unwrap();
        let artifact = record::publish_segment(root, admission, &slices).unwrap();
        let records: Vec<_> = artifact
            .spans()
            .iter()
            .enumerate()
            .map(|(index, span)| PublishedRecord {
                metadata: RecordMetadata {
                    row_id: StorageToken::generate(),
                    key: format!("record-{index}"),
                    encoded_sha256: span.sha256,
                    encoded_length: span.length,
                    logical_size: span.length,
                    format: EncodedFormat::Raw,
                    compression: CompressionDescriptor::Uncompressed,
                    cipher: CipherFormat::Plaintext,
                    locked: false,
                },
                location: Location::Segment {
                    artifact: artifact.plan().artifact().clone(),
                    offset: span.offset,
                    length: span.length,
                },
                is_current: true,
            })
            .collect();
        let publications = records
            .iter()
            .map(|row| PublishRecord {
                metadata: row.metadata.clone(),
                location: row.location.clone(),
                expected: ExpectedCurrent::Absent,
                preserve_previous: false,
            })
            .collect();
        assert!(matches!(
            store.publish(artifact, publications).await.unwrap(),
            PublicationOutcome::Applied { .. }
        ));
        records
    }

    async fn remove(store: &Store, row: &PublishedRecord) {
        assert_eq!(
            store
                .delete(&row.metadata.row_id, &row.location)
                .await
                .unwrap(),
            DeleteOutcome::Applied
        );
    }

    async fn candidate(store: &Store) -> GcCandidate {
        let page = store.gc_candidates(None, 256).await.unwrap();
        assert_eq!(page.candidates.len(), 1);
        page.candidates.into_iter().next().unwrap()
    }

    async fn drain(root: &Path, store: &Store) -> usize {
        let mut pinned = 0;
        for claim in store.claim_cleanup(256).await.unwrap() {
            match record::cleanup(root, claim).unwrap() {
                record::CleanupResult::Removed(receipt) => {
                    assert!(store.finish_cleanup(receipt).await.unwrap())
                }
                record::CleanupResult::Pinned(claim) => {
                    pinned += 1;
                    assert!(store.release_cleanup(claim).await.unwrap());
                }
            }
        }
        pinned
    }

    #[tokio::test]
    async fn candidates_use_physical_dead_bytes_including_record_headers_at_exact_half() {
        for dead in [99, 100] {
            let root = tempfile::tempdir().unwrap();
            let node = Node::open(root.path()).unwrap();
            let store = Store::open(node, Default::default()).unwrap();
            let rows = publish(root.path(), &store, &[20, dead]).await;
            remove(&store, &rows[1]).await;
            let page = store.gc_candidates(None, 1).await.unwrap();
            assert_eq!(page.examined, 1);
            assert_eq!(page.candidates.len(), usize::from(dead == 100));
            if let Some(candidate) = page.candidates.first() {
                assert_eq!(candidate.physical_length, 280);
                assert_eq!(candidate.retained_length, 140);
            }
            store.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn replacement_budget_refusal_happens_before_creation_or_source_changes() {
        let root = tempfile::tempdir().unwrap();
        let node = Node::open(root.path()).unwrap();
        // Source=650, replacement=210; one byte below the complete reservation required.
        let store = Store::open(node.clone(), PhysicalBudget { limit_bytes: 859 }).unwrap();
        let rows = publish(root.path(), &store, &[20, 30, 400]).await;
        remove(&store, &rows[2]).await;
        let candidate = candidate(&store).await;
        let before = store.stats().await.unwrap();
        let budget = Arc::new(Budget::new(node));
        assert!(
            collect_one(
                root.path(),
                store.clone(),
                budget.clone(),
                &candidate,
                deadline()
            )
            .await
            .is_err()
        );
        assert_eq!(store.stats().await.unwrap(), before);
        for row in &rows[..2] {
            assert_eq!(
                store
                    .get_version(&row.metadata.row_id)
                    .await
                    .unwrap()
                    .unwrap(),
                *row
            );
        }
        assert_eq!(
            budget
                .counters
                .bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn collection_handles_partial_and_all_lost_cas_without_recreating_deleted_rows() {
        for losses in 0..=2 {
            let root = tempfile::tempdir().unwrap();
            let node = Node::open(root.path()).unwrap();
            let store = Store::open(node.clone(), Default::default()).unwrap();
            let rows = publish(root.path(), &store, &[20, 30, 400]).await;
            remove(&store, &rows[2]).await;
            let candidate = candidate(&store).await;
            let old_path = root
                .path()
                .join(candidate.artifact.file_name(ArtifactKind::Segment));
            let held = store
                .pin_version(&rows[0].metadata.row_id)
                .await
                .unwrap()
                .unwrap();
            let (entered, wait_entered) = tokio::sync::oneshot::channel();
            let entered = Mutex::new(Some(entered));
            let (release, wait_release) = std::sync::mpsc::channel();
            let wait_release = Mutex::new(wait_release);
            let hooks = Hooks {
                after_copy: Some(Arc::new(move || {
                    entered.lock().unwrap().take().unwrap().send(()).unwrap();
                    wait_release.lock().unwrap().recv().unwrap();
                })),
            };
            let path = root.path().to_owned();
            let copied_store = store.clone();
            let budget = Arc::new(Budget::new(node.clone()));
            let task = tokio::spawn(async move {
                collect_one_with(&path, copied_store, budget, &candidate, deadline(), hooks).await
            });
            wait_entered.await.unwrap();
            for row in rows.iter().take(losses) {
                remove(&store, row).await;
            }
            release.send(()).unwrap();
            let report = task.await.unwrap().unwrap();
            assert_eq!(report.moved_records, (2 - losses) as u64);
            assert_eq!(report.lost_records, losses as u64);
            assert_eq!(report.copied_encoded_bytes, 50);
            assert_eq!(report.moved_encoded_bytes + report.lost_encoded_bytes, 50);
            assert_eq!(report.source_dead_bytes, 440);
            assert_eq!(report.replacement_physical_bytes, 210);
            assert_eq!(drain(root.path(), &store).await, 1);
            assert!(old_path.exists());
            drop(held);
            assert_eq!(drain(root.path(), &store).await, 0);
            assert!(!old_path.exists());
            store.close().await.unwrap();
            drop(store);
            let reopened = Store::open(node, Default::default()).unwrap();
            for (index, row) in rows[..2].iter().enumerate() {
                let current = reopened.pin_version(&row.metadata.row_id).await.unwrap();
                if index < losses {
                    assert!(current.is_none());
                } else {
                    let mut current = current.unwrap();
                    assert_eq!(current.record.metadata, row.metadata);
                    assert_eq!(current.record.is_current, row.is_current);
                    assert_ne!(current.record.location, row.location);
                    current
                        .pin
                        .verify_sha256(row.metadata.encoded_sha256)
                        .unwrap();
                    let mut bytes = Vec::new();
                    current.pin.read_to_end(&mut bytes).unwrap();
                    assert_eq!(
                        bytes,
                        vec![index as u8 + 31; row.metadata.encoded_length as usize]
                    );
                }
            }
            assert_eq!(reopened.stats().await.unwrap().pending, 0);
            assert_eq!(reopened.stats().await.unwrap().cleanup, 0);
            reopened.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_collector_retains_actual_job_admission_and_source_pin() {
        let root = tempfile::tempdir().unwrap();
        let node = Node::open(root.path()).unwrap();
        let store = Store::open(node.clone(), Default::default()).unwrap();
        let rows = publish(root.path(), &store, &[20, 30, 400]).await;
        remove(&store, &rows[2]).await;
        let candidate = candidate(&store).await;
        let old_path = root
            .path()
            .join(candidate.artifact.file_name(ArtifactKind::Segment));
        let budget = Arc::new(Budget::new(node));
        let (entered, wait_entered) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered));
        let (release, wait_release) = std::sync::mpsc::channel();
        let wait_release = Mutex::new(wait_release);
        let hooks = Hooks {
            after_copy: Some(Arc::new(move || {
                entered.lock().unwrap().take().unwrap().send(()).unwrap();
                wait_release.lock().unwrap().recv().unwrap();
            })),
        };
        let (finished, wait_finished) = tokio::sync::oneshot::channel();
        struct Finished(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Finished {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        // The sentinel is captured by the hook, which belongs to the actual blocking job.
        let sentinel = Arc::new(Finished(Some(finished)));
        let previous = hooks.after_copy.unwrap();
        let hooks = Hooks {
            after_copy: Some(Arc::new(move || {
                let _retained = &sentinel;
                previous();
            })),
        };
        let path = root.path().to_owned();
        let copied_store = store.clone();
        let copied_budget = budget.clone();
        let task = tokio::spawn(async move {
            collect_one_with(
                &path,
                copied_store,
                copied_budget,
                &candidate,
                deadline(),
                hooks,
            )
            .await
        });
        wait_entered.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            budget
                .counters
                .bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            METADATA_RESERVATION + 128 * 1024
        );
        for row in &rows[..2] {
            remove(&store, row).await;
        }
        assert_eq!(drain(root.path(), &store).await, 1);
        assert!(old_path.exists());
        release.send(()).unwrap();
        wait_finished.await.unwrap();
        assert_eq!(
            budget
                .counters
                .bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(drain(root.path(), &store).await, 0);
        assert_eq!(store.stats().await.unwrap().records, 0);
        assert_eq!(store.stats().await.unwrap().pending, 0);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn bounded_pass_advances_over_ineligible_pages_and_skips_stale_candidates() {
        let root = tempfile::tempdir().unwrap();
        let node = Node::open(root.path()).unwrap();
        let store = Store::open(node.clone(), Default::default()).unwrap();
        let rows = publish(root.path(), &store, &[20, 30, 400]).await;
        let budget = Arc::new(Budget::new(node));
        let empty = collect_pass(root.path(), store.clone(), budget.clone(), 1, 2, deadline())
            .await
            .unwrap();
        assert_eq!(empty.examined, 1);
        assert_eq!(empty.candidates, 0);
        assert!(empty.completed);
        remove(&store, &rows[2]).await;
        let stale = candidate(&store).await;
        let pass = collect_pass(root.path(), store.clone(), budget.clone(), 1, 4, deadline())
            .await
            .unwrap();
        assert_eq!(pass.moved_records, 2);
        assert!(pass.completed);
        assert_eq!(
            budget
                .counters
                .peak_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            2 * (METADATA_RESERVATION + 128 * 1024)
        );
        assert_eq!(
            budget
                .counters
                .bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        let skipped = collect_one(root.path(), store.clone(), budget, &stale, deadline())
            .await
            .unwrap();
        assert_eq!(skipped.skipped, 1);
        store.close().await.unwrap();
    }
}
