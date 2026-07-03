//! Engine tests against the in-memory metadata double and a recording sink: delivery marks the
//! entry done, a retryable failure reschedules until the budget is exhausted then parks it failed,
//! a missing subscription drops the entry, and HMAC signing is stable.

use super::*;
use async_trait::async_trait;
use cairn_types::bucket::{Bucket, ConfigAspect, ConfigDoc};
use cairn_types::id::{BucketName, ObjectKey, UserId, VersionId};
use cairn_types::notification::{EventKind, NotificationConfig, WebhookEndpoint};
use cairn_types::testing::{InMemoryMetadataStore, TestClock};
use cairn_types::traits::MetadataStore;
use cairn_types::{Mutation, WebhookStatus};
use std::sync::Mutex;

/// One recorded delivery: `(url, body, signature)`.
type Call = (String, Vec<u8>, Option<String>);

/// A sink that records every delivery and returns a scripted result.
struct RecordingSink {
    result: Mutex<Result<(), WebhookError>>,
    calls: Mutex<Vec<Call>>,
}

impl RecordingSink {
    fn new(result: Result<(), WebhookError>) -> Self {
        Self {
            result: Mutex::new(result),
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl WebhookSink for RecordingSink {
    async fn deliver(
        &self,
        url: &str,
        body: &[u8],
        signature: Option<&str>,
    ) -> Result<(), WebhookError> {
        self.calls.lock().unwrap().push((
            url.to_owned(),
            body.to_vec(),
            signature.map(str::to_owned),
        ));
        self.result.lock().unwrap().clone()
    }
}

fn bucket_name() -> BucketName {
    BucketName::parse("wh-bucket").unwrap()
}

async fn setup(meta: &InMemoryMetadataStore, endpoints: Vec<WebhookEndpoint>) {
    let bucket = Bucket {
        name: bucket_name(),
        owner_id: UserId("o".into()),
        created_at: Timestamp::from_secs(0),
        versioning: cairn_types::bucket::VersioningState::Enabled,
        ownership_mode: cairn_types::authz::OwnershipMode::BucketOwnerEnforced,
        region: "us-east-1".into(),
        compression: None,
    };
    meta.submit(Mutation::CreateBucket(Box::new(bucket)))
        .await
        .unwrap();
    let config = NotificationConfig { endpoints };
    meta.submit(Mutation::SetBucketConfig {
        bucket: bucket_name(),
        aspect: ConfigAspect::Notification,
        doc: Some(ConfigDoc(serde_json::to_string(&config).unwrap())),
    })
    .await
    .unwrap();
}

fn entry(endpoint_id: &str) -> WebhookEntry {
    WebhookEntry {
        id: format!("wh-{endpoint_id}-1"),
        bucket: bucket_name(),
        key: ObjectKey::parse("k.txt").unwrap(),
        version_id: VersionId::from_string("v1".into()),
        event: EventKind::ObjectCreatedPut,
        endpoint_id: endpoint_id.to_owned(),
        payload: r#"{"Records":[]}"#.to_owned(),
        attempts: 0,
        next_attempt_at: Timestamp::from_secs(0),
        status: WebhookStatus::Pending,
        last_error: None,
        priority: 0,
        lease_until: None,
    }
}

fn endpoint(id: &str, secret: Option<&str>) -> WebhookEndpoint {
    WebhookEndpoint {
        id: id.to_owned(),
        url: "http://example.test/hook".to_owned(),
        events: vec!["s3:ObjectCreated:*".to_owned()],
        prefix: None,
        suffix: None,
        secret: secret.map(str::to_owned),
    }
}

#[tokio::test]
async fn delivers_and_marks_done_with_signature() {
    let meta = InMemoryMetadataStore::new();
    setup(&meta, vec![endpoint("ep1", Some("topsecret"))]).await;
    meta.submit(Mutation::EnqueueWebhooks(vec![entry("ep1")]))
        .await
        .unwrap();

    let sink = RecordingSink::new(Ok(()));
    let clock = TestClock::at_secs(100);
    let engine = WebhookEngine::new(WebhookOpts::default());
    let report = engine
        .run_until_idle(&meta, &sink, &clock, 10)
        .await
        .unwrap();

    assert_eq!(report.delivered, 1);
    assert_eq!(report.failed, 0);
    // Snapshot the recorded call and release the guard before any await below.
    let (url, _body, sig) = {
        let calls = sink.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        calls[0].clone()
    };
    assert_eq!(url, "http://example.test/hook");
    // Signature present and equal to the documented HMAC.
    assert_eq!(
        sig.as_deref(),
        Some(sign("topsecret", br#"{"Records":[]}"#).as_str())
    );
    // The entry is gone from the due set (marked completed).
    assert!(
        meta.list_due_webhooks(10, TestClock::at_secs(1_000).now())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn unsigned_when_no_secret() {
    let meta = InMemoryMetadataStore::new();
    setup(&meta, vec![endpoint("ep1", None)]).await;
    meta.submit(Mutation::EnqueueWebhooks(vec![entry("ep1")]))
        .await
        .unwrap();
    let sink = RecordingSink::new(Ok(()));
    let clock = TestClock::at_secs(100);
    WebhookEngine::new(WebhookOpts::default())
        .run_until_idle(&meta, &sink, &clock, 10)
        .await
        .unwrap();
    assert_eq!(sink.calls.lock().unwrap()[0].2, None);
}

#[tokio::test]
async fn retryable_failure_reschedules_then_parks_failed() {
    let meta = InMemoryMetadataStore::new();
    setup(&meta, vec![endpoint("ep1", None)]).await;
    meta.submit(Mutation::EnqueueWebhooks(vec![entry("ep1")]))
        .await
        .unwrap();

    let sink = RecordingSink::new(Err(WebhookError::Retryable("503".into())));
    // A tight budget so the entry exhausts quickly.
    let opts = WebhookOpts {
        max_attempts: 2,
        base_backoff_secs: 10,
        max_backoff_secs: 100,
        batch_size: 64,
        max_concurrency: 8,
    };
    let engine = WebhookEngine::new(opts);

    // First pass at t=100: attempt 1 fails → rescheduled (not terminal).
    let r1 = engine
        .run_until_idle(&meta, &sink, &TestClock::at_secs(100), 1)
        .await
        .unwrap();
    assert_eq!(r1.failed, 1);
    // Not yet due at t=100 (backoff pushed it out); it is due well after the backoff.
    assert!(
        meta.list_due_webhooks(10, TestClock::at_secs(101).now())
            .await
            .unwrap()
            .is_empty(),
        "rescheduled entry is not immediately due"
    );

    // Second pass after the backoff: attempt 2 fails → terminal (max_attempts reached).
    let r2 = engine
        .run_until_idle(&meta, &sink, &TestClock::at_secs(1_000), 1)
        .await
        .unwrap();
    assert_eq!(r2.failed, 1);
    let failed = meta.list_failed_webhooks(10).await.unwrap();
    assert_eq!(failed.len(), 1, "entry parked as terminally failed");
    assert_eq!(failed[0].attempts, 2);
}

#[tokio::test]
async fn missing_subscription_drops_entry() {
    let meta = InMemoryMetadataStore::new();
    // Endpoint "gone" is referenced by the entry but absent from the config.
    setup(&meta, vec![endpoint("other", None)]).await;
    meta.submit(Mutation::EnqueueWebhooks(vec![entry("gone")]))
        .await
        .unwrap();
    let sink = RecordingSink::new(Ok(()));
    let report = WebhookEngine::new(WebhookOpts::default())
        .run_until_idle(&meta, &sink, &TestClock::at_secs(100), 10)
        .await
        .unwrap();
    assert_eq!(report.dropped, 1);
    assert_eq!(report.delivered, 0);
    assert!(
        sink.calls.lock().unwrap().is_empty(),
        "no delivery attempted"
    );
}

#[test]
fn backoff_is_exponential_and_capped() {
    assert_eq!(next_backoff(1, 5, 900), 5);
    assert_eq!(next_backoff(2, 5, 900), 10);
    assert_eq!(next_backoff(3, 5, 900), 20);
    assert_eq!(next_backoff(100, 5, 900), 900);
}

/// A hung endpoint (accepts the connection but never responds) must not pin the delivery: the
/// per-request timeout converts it into a retryable failure promptly. This is the regression guard
/// for the head-of-line-blocking blocker — combined with the engine's bounded concurrency, one
/// slow endpoint can no longer stall the outbox.
#[tokio::test]
async fn delivery_times_out_against_a_hung_endpoint() {
    use std::time::{Duration, Instant};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Accept connections and hold them open without ever writing a response.
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            if let Ok((s, _)) = listener.accept().await {
                held.push(s);
            }
        }
    });

    // Loopback test server — opt out of the SSRF guard.
    let sink = HttpWebhookSink::with_timeout(
        Duration::from_millis(300),
        cairn_net::GuardConfig::new(true),
    );
    let start = Instant::now();
    let res = sink
        .deliver(&format!("http://{addr}/hook"), b"{}", None)
        .await;
    assert!(
        matches!(res, Err(WebhookError::Retryable(_))),
        "a hung endpoint is a retryable failure, not an indefinite hang: {res:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "the timeout bounded the request (elapsed {:?})",
        start.elapsed()
    );
}
