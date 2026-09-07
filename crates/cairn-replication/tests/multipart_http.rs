//! Bounded real-HTTP multipart delivery: range geometry, signed query/payload and 200/Error.
use bytes::Bytes;
use cairn_replication::{BucketRoutedSink, HttpS3Sink, ReplicationSinkRuntime, S3SinkConfig};
use cairn_types::replication::{ReplicatedObject, ReplicationSource};
use cairn_types::{BucketName, ObjectKey, VersionId};
use http::{Method, Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Journal(Mutex<Vec<&'static str>>);

#[async_trait::async_trait]
impl cairn_types::replication::ReplicationMultipartJournal for Journal {
    async fn begin(
        &self,
        _endpoint: &str,
        bucket: &str,
    ) -> Result<String, cairn_types::ReplicationError> {
        assert_eq!(bucket, "dest");
        self.0.lock().unwrap().push("begin");
        Ok("attempt".to_owned())
    }
    async fn record_upload_id(
        &self,
        attempt: &str,
        receipt: &str,
    ) -> Result<(), cairn_types::ReplicationError> {
        assert_eq!(attempt, "attempt");
        assert_eq!(receipt, "opaque/id+with&symbols");
        self.0.lock().unwrap().push("receipt");
        Ok(())
    }
    async fn retire(&self, attempt: &str) -> Result<(), cairn_types::ReplicationError> {
        assert_eq!(attempt, "attempt");
        self.0.lock().unwrap().push("retire");
        Ok(())
    }
}

#[derive(Debug)]
struct Observed {
    method: Method,
    query: String,
    size: u64,
}

async fn peer(
    error_at_complete: bool,
    reject_initiation: bool,
) -> (
    String,
    Arc<Mutex<Vec<Observed>>>,
    tokio::task::JoinHandle<()>,
) {
    peer_with_cleanup(
        error_at_complete,
        reject_initiation,
        (404, "<Error><Code>NoSuchUpload</Code></Error>"),
    )
    .await
}

async fn peer_with_cleanup(
    error_at_complete: bool,
    reject_initiation: bool,
    listing: (u16, &'static str),
) -> (
    String,
    Arc<Mutex<Vec<Observed>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let observed = Arc::new(Mutex::new(Vec::new()));
    let events = observed.clone();
    let task = tokio::spawn(async move {
        loop {
            let (io, _) = listener.accept().await.unwrap();
            let events = events.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                    let events = events.clone();
                    async move {
                        let (head, mut body) = request.into_parts();
                        let mut hash = Sha256::new();
                        let mut size = 0u64;
                        let mut xml = Vec::new();
                        while let Some(frame) = body.frame().await {
                            let frame = frame.unwrap();
                            if let Ok(bytes) = frame.into_data() {
                                size += bytes.len() as u64;
                                hash.update(&bytes);
                                if head.method == Method::POST {
                                    assert!(
                                        size <= 1024 * 1024,
                                        "control payload must stay bounded"
                                    );
                                    xml.extend_from_slice(&bytes);
                                }
                            }
                        }
                        assert_eq!(
                            head.headers["x-amz-content-sha256"].to_str().unwrap(),
                            hex::encode(hash.finalize()),
                            "every request uses a real signed payload digest"
                        );
                        let authorization = head.headers["authorization"].to_str().unwrap();
                        let fields = authorization
                            .strip_prefix("AWS4-HMAC-SHA256 ")
                            .unwrap()
                            .split(", ")
                            .map(|field| field.split_once('=').unwrap())
                            .collect::<HashMap<_, _>>();
                        let (access_key, scope) = fields["Credential"].split_once('/').unwrap();
                        assert_eq!(access_key, "AKID");
                        let names = fields["SignedHeaders"];
                        let signed = names
                            .split(';')
                            .map(|name| {
                                (
                                    name.to_owned(),
                                    head.headers[name].to_str().unwrap().trim().to_owned(),
                                )
                            })
                            .collect::<Vec<_>>();
                        let canonical = cairn_auth::canonical_request(
                            head.method.as_str(),
                            head.uri.path(),
                            head.uri.query().unwrap_or_default(),
                            &signed,
                            names,
                            head.headers["x-amz-content-sha256"].to_str().unwrap(),
                        );
                        let date = head.headers["x-amz-date"].to_str().unwrap();
                        let signing_key =
                            cairn_auth::signing_key("test-secret", &date[..8], "us-east-1", "s3");
                        assert_eq!(
                            fields["Signature"],
                            cairn_auth::compute_signature(
                                &signing_key,
                                &cairn_auth::string_to_sign(date, scope, &canonical)
                            ),
                            "the signature must cover the actual encoded multipart query"
                        );
                        let query = head.uri.query().unwrap_or_default().to_owned();
                        if query != "uploads=" {
                            assert!(
                                query.contains("uploadId=opaque%2Fid%2Bwith%26symbols"),
                                "query values must encode slash and punctuation: {query}"
                            );
                        }
                        let mut status = 200;
                        let reply = if query == "uploads=" {
                            assert_eq!(head.method, Method::POST);
                            assert_eq!(head.headers["x-amz-meta-cairn-replica"], "true");
                            if reject_initiation {
                                status = 403;
                                "<Error><Code>AccessDenied</Code></Error>"
                            } else {
                                "<InitiateMultipartUploadResult><UploadId>opaque/id+with&amp;symbols</UploadId></InitiateMultipartUploadResult>"
                            }
                        } else if head.method == Method::PUT {
                            ""
                        } else if head.method == Method::DELETE {
                            assert_eq!(head.headers["x-amz-meta-cairn-replica"], "true");
                            status = 204;
                            ""
                        } else if head.method == Method::GET {
                            assert!(query.contains("max-parts=1"));
                            assert_eq!(head.headers["x-amz-meta-cairn-replica"], "true");
                            status = listing.0;
                            listing.1
                        } else {
                            let xml = String::from_utf8(xml).unwrap();
                            assert!(xml.contains("<PartNumber>1</PartNumber>"));
                            assert!(xml.contains("<PartNumber>2</PartNumber>"));
                            if error_at_complete {
                                "<Error><Code>InternalError</Code></Error>"
                            } else {
                                "<CompleteMultipartUploadResult><ETag>&quot;done&quot;</ETag></CompleteMultipartUploadResult>"
                            }
                        };
                        events.lock().unwrap().push(Observed {
                            method: head.method,
                            query,
                            size,
                        });
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(status)
                                .header("etag", "\"0123456789abcdef0123456789abcdef\"")
                                .body(Full::new(Bytes::from_static(reply.as_bytes())))
                                .unwrap(),
                        )
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(io), service)
                    .await;
            });
        }
    });
    (endpoint, observed, task)
}

fn source<'a>(size: u64, reads: Arc<Mutex<Vec<(u64, u64)>>>) -> ReplicationSource<'a> {
    ReplicationSource {
        buffer_bytes: 1024 * 1024,
        max_frame_bytes: 64 * 1024,
        open: Box::new(move |range, _lease| {
            assert!(range.offset + range.length <= size);
            reads.lock().unwrap().push((range.offset, range.length));
            Box::pin(async move {
                Ok(Box::pin(futures_util::stream::unfold(
                    (range.offset, range.offset + range.length),
                    |(offset, end)| async move {
                        if offset == end {
                            return None;
                        }
                        let len = (end - offset).min(64 * 1024);
                        let bytes = (offset..offset + len)
                            .map(|position| (position % 251) as u8)
                            .collect::<Vec<_>>();
                        Some((Ok(Bytes::from(bytes)), (offset + len, end)))
                    },
                )) as cairn_types::BlobStream)
            })
        }),
    }
}

#[tokio::test]
async fn multipart_uses_two_bounded_passes_and_parses_embedded_errors() {
    let part = cairn_replication::SINGLE_PUT_MAX_BYTES;
    for error_at_complete in [false, true] {
        let (endpoint, observed, task) = peer(error_at_complete, false).await;
        let sink = sink(endpoint, Duration::from_secs(60));
        let reads = Arc::new(Mutex::new(Vec::new()));
        let journal = Journal::default();
        let object = object(part + 17, reads.clone(), Some(&journal));
        let result = sink
            .put_object(&BucketName::parse("source").unwrap(), object)
            .await;
        assert_eq!(
            result.is_err(),
            error_at_complete,
            "HTTP200 Error cannot become completion: {result:?}"
        );
        assert_eq!(*journal.0.lock().unwrap(), ["begin", "receipt", "retire"]);
        let events = observed.lock().unwrap();
        let parts = events
            .iter()
            .filter(|event| event.method == Method::PUT)
            .collect::<Vec<_>>();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].size, part);
        assert_eq!(parts[1].size, 17);
        assert!(parts[0].query.starts_with("partNumber=1&"));
        assert_eq!(
            events.iter().any(|event| event.method == Method::DELETE),
            error_at_complete
        );
        assert_eq!(
            *reads.lock().unwrap(),
            [(0, part), (0, part), (part, 17), (part, 17)]
        );
        drop(events);
        task.abort();
    }
}

fn sink(endpoint: String, timeout: Duration) -> HttpS3Sink {
    let runtime = ReplicationSinkRuntime::new(64 * 1024 * 1024, timeout).unwrap();
    HttpS3Sink::new(
        S3SinkConfig {
            endpoint,
            dest_bucket: "dest".to_owned(),
            dest_buckets: HashMap::new(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKID".to_owned(),
            secret_access_key: "test-secret".into(),
            ca_cert_path: None,
            ca_cert_pem: None,
            insecure_skip_verify: false,
            allow_internal_endpoints: true,
            allow_plaintext_sse_over_http: false,
        },
        runtime,
    )
    .unwrap()
}

fn object<'a>(
    size: u64,
    reads: Arc<Mutex<Vec<(u64, u64)>>>,
    journal: Option<&'a dyn cairn_types::replication::ReplicationMultipartJournal>,
) -> ReplicatedObject<'a> {
    ReplicatedObject {
        journal,
        key: ObjectKey::parse("key").unwrap(),
        version_id: VersionId::generate(),
        content_type: "application/octet-stream".to_owned(),
        user_metadata: vec![],
        etag: cairn_types::ETag::from_string("source-etag".to_owned()),
        size,
        tags: vec![],
        acl: None,
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: cairn_types::StorageClass::Standard,
        checksums: vec![],
        client_encrypted: false,
        source: source(size, reads),
    }
}

#[tokio::test]
async fn multipart_requires_journal_and_timeout_retains_known_receipt() {
    let size = cairn_replication::SINGLE_PUT_MAX_BYTES + 1;
    let bucket = BucketName::parse("source").unwrap();
    let (endpoint, observed, task) = peer(false, false).await;
    let sink = sink(endpoint, Duration::from_millis(500));
    let reads = Arc::new(Mutex::new(Vec::new()));
    assert!(
        sink.put_object(&bucket, object(size, reads.clone(), None))
            .await
            .is_err()
    );
    assert!(observed.lock().unwrap().is_empty());
    assert!(reads.lock().unwrap().is_empty());

    let journal = Journal::default();
    let mut object = object(size, reads, Some(&journal));
    // Stall the first hash pass after the initiation receipt is durably recorded.
    object.source.open = Box::new(|_, _lease| Box::pin(std::future::pending()));
    let result = sink.put_object(&bucket, object).await;
    assert!(matches!(
        result,
        Err(cairn_types::ReplicationError::Unavailable(_))
    ));
    assert_eq!(*journal.0.lock().unwrap(), ["begin", "receipt"]);
    assert_eq!(observed.lock().unwrap().len(), 1);
    task.abort();
}

#[tokio::test]
async fn rejected_initiation_does_not_leave_a_false_orphan_incident() {
    let (endpoint, observed, task) = peer(false, true).await;
    let sink = sink(endpoint, Duration::from_secs(5));
    let journal = Journal::default();
    let reads = Arc::new(Mutex::new(Vec::new()));
    let result = sink
        .put_object(
            &BucketName::parse("source").unwrap(),
            object(
                cairn_replication::SINGLE_PUT_MAX_BYTES + 1,
                reads.clone(),
                Some(&journal),
            ),
        )
        .await;
    assert!(matches!(
        result,
        Err(cairn_types::ReplicationError::Terminal(_))
    ));
    assert_eq!(*journal.0.lock().unwrap(), ["begin", "retire"]);
    assert!(reads.lock().unwrap().is_empty());
    assert_eq!(observed.lock().unwrap().len(), 1);
    task.abort();
}

#[tokio::test]
async fn cancelled_reader_retains_shared_admission_until_its_work_ends() {
    let size = cairn_replication::SINGLE_PUT_MAX_BYTES + 1;
    let bucket = BucketName::parse("source").unwrap();
    let (endpoint, observed, task) = peer(false, false).await;
    let sink = sink(endpoint, Duration::from_millis(500));
    let journal = Journal::default();
    let held = Arc::new(Mutex::new(Vec::new()));
    let retained = held.clone();
    let reads = Arc::new(Mutex::new(Vec::new()));
    let mut first = object(size, reads.clone(), Some(&journal));
    first.source.open = Box::new(move |_, lease| {
        // Model a blocking reader that survives cancellation of the async delivery future.
        retained.lock().unwrap().push(lease);
        Box::pin(std::future::pending())
    });
    assert!(sink.put_object(&bucket, first).await.is_err());
    assert_eq!(held.lock().unwrap().len(), 1);
    assert_eq!(observed.lock().unwrap().len(), 1);
    // Each transfer reserves 33 MiB from 64 MiB. A cancelled reader still occupies its share.
    assert!(
        sink.put_object(&bucket, object(size, reads.clone(), Some(&journal)))
            .await
            .is_err()
    );
    assert_eq!(
        observed.lock().unwrap().len(),
        1,
        "second delivery must not reach initiation"
    );
    held.lock().unwrap().clear();
    // With the last reader lease released, admission proceeds to the missing-journal guard.
    let error = sink
        .put_object(&bucket, object(size, reads, None))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires durable upload journaling")
    );
    task.abort();
}

#[tokio::test]
async fn abort_keeps_journal_until_bounded_listing_confirms_cleanup() {
    for (listing, retired) in [
        (
            (
                200,
                "<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber></Part></ListPartsResult>",
            ),
            false,
        ),
        (
            (
                200,
                "<ListPartsResult><IsTruncated>true</IsTruncated></ListPartsResult>",
            ),
            false,
        ),
        (
            (
                200,
                "<ListPartsResult><IsTruncated>false</IsTruncated><Part/></ListPartsResult>",
            ),
            false,
        ),
        (
            (200, "<ListPartsResult><IsTruncated>false</IsTruncated>"),
            false,
        ),
        ((200, "<Error><Code>InternalError</Code></Error>"), false),
        (
            (
                200,
                "<ListPartsResult><IsTruncated>false</IsTruncated><Error/></ListPartsResult>",
            ),
            false,
        ),
        ((200, "<ListPartsResult/>"), false),
        (
            (
                200,
                "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult><Extra/>",
            ),
            false,
        ),
        (
            (
                200,
                "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult><![CDATA[extra]]>",
            ),
            false,
        ),
        ((403, "<Error><Code>AccessDenied</Code></Error>"), false),
        ((503, "unavailable"), false),
        (
            (
                200,
                "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>",
            ),
            true,
        ),
        ((404, "<Error><Code>NoSuchUpload</Code></Error>"), true),
    ] {
        let (endpoint, events, task) = peer_with_cleanup(false, false, listing).await;
        let sink = sink(endpoint, Duration::from_secs(5));
        let journal = Journal::default();
        let mut object = object(
            cairn_replication::SINGLE_PUT_MAX_BYTES + 1,
            Arc::new(Mutex::new(Vec::new())),
            Some(&journal),
        );
        // Fail after persisting the receipt, then model a peer that acknowledges abort while a
        // previously in-flight part remains visible. The journal must survive the first 204.
        object.source.open = Box::new(|_, _lease| {
            Box::pin(async {
                Err(cairn_types::ReplicationError::Retryable(
                    "source read failed".to_owned(),
                ))
            })
        });
        assert!(
            sink.put_object(&BucketName::parse("source").unwrap(), object)
                .await
                .is_err()
        );
        let expected = if retired {
            vec!["begin", "receipt", "retire"]
        } else {
            vec!["begin", "receipt"]
        };
        assert_eq!(*journal.0.lock().unwrap(), expected, "listing: {listing:?}");
        let events = events.lock().unwrap();
        assert_eq!(
            events.iter().map(|e| e.method.clone()).collect::<Vec<_>>(),
            [Method::POST, Method::DELETE, Method::GET]
        );
        drop(events);
        task.abort();
    }
}
