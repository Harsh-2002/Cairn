//! Engine orchestration tests against in-memory doubles: copy-all, pagination, transient-retry,
//! resume-from-cursor, and cooperative cancellation. No network.

use super::*;
use bytes::Bytes;
use cairn_types::error::BlobError;
use futures_util::StreamExt;
use std::collections::BTreeMap;
use std::sync::Mutex as StdMutex;

fn body_of(data: Vec<u8>) -> cairn_types::BlobStream {
    Box::pin(futures_util::stream::once(async move {
        Ok::<Bytes, BlobError>(Bytes::from(data))
    }))
}

/// A fake source with a fixed object set per bucket, cursor = a stringified offset. It can be told
/// to fail `get_object` for a key a number of times (transiently) before succeeding.
struct FakeSource {
    data: BTreeMap<String, Vec<(String, Vec<u8>)>>,
    page_size: usize,
    fail_get: StdMutex<BTreeMap<String, u32>>,
}

impl FakeSource {
    fn new(page_size: usize) -> Self {
        Self {
            data: BTreeMap::new(),
            page_size,
            fail_get: StdMutex::new(BTreeMap::new()),
        }
    }
    fn with_bucket(mut self, name: &str, objects: &[(&str, &[u8])]) -> Self {
        self.data.insert(
            name.to_owned(),
            objects
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.to_vec()))
                .collect(),
        );
        self
    }
}

#[async_trait]
impl SourceReader for FakeSource {
    async fn list_buckets(&self) -> Result<Vec<String>, ImportError> {
        Ok(self.data.keys().cloned().collect())
    }
    async fn list_objects(
        &self,
        bucket: &str,
        cursor: Option<&str>,
        _max_keys: u32,
    ) -> Result<ObjectPage, ImportError> {
        let objs = self
            .data
            .get(bucket)
            .ok_or_else(|| ImportError::Terminal(format!("no such source bucket {bucket}")))?;
        let offset: usize = cursor.and_then(|c| c.parse().ok()).unwrap_or(0);
        let end = (offset + self.page_size).min(objs.len());
        let page: Vec<RemoteObject> = objs[offset..end]
            .iter()
            .map(|(k, v)| RemoteObject {
                key: k.clone(),
                size: v.len() as u64,
                etag: None,
            })
            .collect();
        let is_truncated = end < objs.len();
        Ok(ObjectPage {
            objects: page,
            next_cursor: is_truncated.then(|| end.to_string()),
            is_truncated,
        })
    }
    async fn get_object(&self, bucket: &str, key: &str) -> Result<SourceObject, ImportError> {
        {
            let mut fails = self.fail_get.lock().unwrap();
            if let Some(n) = fails.get_mut(key) {
                if *n > 0 {
                    *n -= 1;
                    return Err(ImportError::Retryable(
                        "scripted transient failure".to_owned(),
                    ));
                }
            }
        }
        let objs = self.data.get(bucket).unwrap();
        let (_, bytes) = objs.iter().find(|(k, _)| k == key).unwrap();
        Ok(SourceObject {
            key: key.to_owned(),
            size: bytes.len() as u64,
            etag: None,
            content_type: Some("application/octet-stream".to_owned()),
            user_metadata: vec![],
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            body: body_of(bytes.clone()),
        })
    }
}

/// Records every written object (bucket, key, bytes) after draining its stream.
#[derive(Default)]
struct RecordingDest {
    buckets: StdMutex<Vec<String>>,
    objects: StdMutex<BTreeMap<(String, String), Vec<u8>>>,
}

#[async_trait]
impl DestWriter for RecordingDest {
    async fn ensure_bucket(&self, bucket: &str) -> Result<(), ImportError> {
        self.buckets.lock().unwrap().push(bucket.to_owned());
        Ok(())
    }
    async fn put_object(&self, bucket: &str, obj: SourceObject) -> Result<(), ImportError> {
        let mut body = obj.body;
        let mut buf = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| ImportError::Retryable(e.to_string()))?;
            buf.extend_from_slice(&chunk);
        }
        self.objects
            .lock()
            .unwrap()
            .insert((bucket.to_owned(), obj.key), buf);
        Ok(())
    }
}

/// Collects progress snapshots; can request cancellation after `cancel_after` reports.
struct CollectProgress {
    snapshots: StdMutex<Vec<Vec<ImportBucketProgress>>>,
    cancel_after: Option<usize>,
}

impl CollectProgress {
    fn new() -> Self {
        Self {
            snapshots: StdMutex::new(Vec::new()),
            cancel_after: None,
        }
    }
    fn cancelling_after(n: usize) -> Self {
        Self {
            snapshots: StdMutex::new(Vec::new()),
            cancel_after: Some(n),
        }
    }
}

#[async_trait]
impl ProgressSink for CollectProgress {
    async fn report(&self, buckets: &[ImportBucketProgress]) -> bool {
        let mut snaps = self.snapshots.lock().unwrap();
        snaps.push(buckets.to_vec());
        match self.cancel_after {
            Some(n) => snaps.len() < n,
            None => true,
        }
    }
}

fn plan(src: &str, dst: &str) -> BucketPlan {
    BucketPlan {
        source_bucket: src.to_owned(),
        dest_bucket: dst.to_owned(),
        cursor: None,
        objects_done: 0,
        bytes_done: 0,
    }
}

fn engine() -> ImportEngine {
    ImportEngine::new(ImportOpts {
        base_backoff_secs: 0,
        max_backoff_secs: 0,
        ..ImportOpts::default()
    })
}

#[tokio::test]
async fn copies_all_objects_across_pages() {
    let source = FakeSource::new(2).with_bucket(
        "src",
        &[
            ("a.txt", b"aaa"),
            ("b.txt", b"bbbb"),
            ("c.txt", b"cc"),
            ("d.txt", b"d"),
            ("e.txt", b"eeeee"),
        ],
    );
    let dest = RecordingDest::default();
    let progress = CollectProgress::new();
    let report = engine()
        .run(&source, &dest, &[plan("src", "dst")], &progress)
        .await;

    assert_eq!(report.objects_copied, 5);
    assert_eq!(report.bytes_copied, 3 + 4 + 2 + 1 + 5);
    assert!(!report.cancelled);
    let objs = dest.objects.lock().unwrap();
    assert_eq!(objs.len(), 5);
    assert_eq!(objs[&("dst".to_owned(), "a.txt".to_owned())], b"aaa");
    assert_eq!(objs[&("dst".to_owned(), "e.txt".to_owned())], b"eeeee");
    // The destination bucket was ensured.
    assert_eq!(&*dest.buckets.lock().unwrap(), &["dst".to_owned()]);
    // The final snapshot marks the bucket completed.
    let last = progress.snapshots.lock().unwrap().last().unwrap().clone();
    assert_eq!(last[0].state, ImportState::Completed);
    assert_eq!(last[0].objects_done, 5);
}

#[tokio::test]
async fn retries_transient_get_failures() {
    let source = FakeSource::new(10).with_bucket("src", &[("flaky.txt", b"payload")]);
    // Fail the first two GETs, then succeed.
    source
        .fail_get
        .lock()
        .unwrap()
        .insert("flaky.txt".to_owned(), 2);
    let dest = RecordingDest::default();
    let report = engine()
        .run(
            &source,
            &dest,
            &[plan("src", "dst")],
            &CollectProgress::new(),
        )
        .await;
    assert_eq!(report.objects_copied, 1);
    assert_eq!(report.objects_failed, 0);
    assert_eq!(
        dest.objects.lock().unwrap()[&("dst".to_owned(), "flaky.txt".to_owned())],
        b"payload"
    );
}

#[tokio::test]
async fn gives_up_after_attempt_budget_but_job_continues() {
    let source = FakeSource::new(10).with_bucket("src", &[("bad.txt", b"x"), ("good.txt", b"yy")]);
    // bad.txt fails more times than the (small) attempt budget; good.txt still lands.
    source
        .fail_get
        .lock()
        .unwrap()
        .insert("bad.txt".to_owned(), 99);
    let dest = RecordingDest::default();
    let eng = ImportEngine::new(ImportOpts {
        max_attempts: 3,
        base_backoff_secs: 0,
        max_backoff_secs: 0,
        ..ImportOpts::default()
    });
    let progress = CollectProgress::new();
    let report = eng
        .run(&source, &dest, &[plan("src", "dst")], &progress)
        .await;
    assert_eq!(report.objects_copied, 1); // good.txt
    let objs = dest.objects.lock().unwrap();
    assert!(objs.contains_key(&("dst".to_owned(), "good.txt".to_owned())));
    assert!(!objs.contains_key(&("dst".to_owned(), "bad.txt".to_owned())));
    // The bucket records the failure but still completed (one poison object never fails the job).
    let last = progress.snapshots.lock().unwrap().last().unwrap().clone();
    assert_eq!(last[0].state, ImportState::Completed);
    assert!(last[0].last_error.is_some());
}

#[tokio::test]
async fn resumes_from_cursor() {
    let source = FakeSource::new(2)
        .with_bucket("src", &[("a", b"1"), ("b", b"2"), ("c", b"3"), ("d", b"4")]);
    let dest = RecordingDest::default();
    // Start at offset "2" (skip a,b) with 2 already done from a prior run.
    let mut p = plan("src", "dst");
    p.cursor = Some("2".to_owned());
    p.objects_done = 2;
    p.bytes_done = 2;
    let report = engine()
        .run(&source, &dest, &[p], &CollectProgress::new())
        .await;
    // Only c and d copied this run, but cumulative progress reflects the prior two.
    let objs = dest.objects.lock().unwrap();
    assert_eq!(objs.len(), 2);
    assert!(objs.contains_key(&("dst".to_owned(), "c".to_owned())));
    assert!(objs.contains_key(&("dst".to_owned(), "d".to_owned())));
    assert!(!objs.contains_key(&("dst".to_owned(), "a".to_owned())));
    assert_eq!(report.objects_copied, 4); // 2 prior + 2 this run
}

#[tokio::test]
async fn cancellation_stops_cleanly() {
    // 3 pages of 1 object; cancel after the first report.
    let source = FakeSource::new(1).with_bucket("src", &[("a", b"1"), ("b", b"2"), ("c", b"3")]);
    let dest = RecordingDest::default();
    let progress = CollectProgress::cancelling_after(1);
    let report = engine()
        .run(&source, &dest, &[plan("src", "dst")], &progress)
        .await;
    assert!(report.cancelled);
    // Fewer than all three objects were copied, and the bucket is marked cancelled.
    assert!(dest.objects.lock().unwrap().len() < 3);
    let last = progress.snapshots.lock().unwrap().last().unwrap().clone();
    assert_eq!(last[0].state, ImportState::Cancelled);
}
