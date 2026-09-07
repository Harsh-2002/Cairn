//! Durable remote multipart attempts and cleanup, driven by the existing replication workers.
use crate::{ReplicationEngine, SinkRouter};
use async_trait::async_trait;
use cairn_types::meta::{Mutation, MutationOutcome, OutboxEntry};
use cairn_types::replication::ReplicationMultipartJournal;
use cairn_types::replication_upload::{
    RemoteMultipartDestination, RemoteMultipartUpload, ReplicationUploadMutation,
};
use cairn_types::traits::{Clock, MetadataStore};
use cairn_types::{MetaError, ReplicationError};

pub(crate) struct UploadJournal<'a, M: ?Sized, C: ?Sized> {
    pub meta: &'a M,
    pub clock: &'a C,
    pub entry: &'a OutboxEntry,
}

fn ownership(outcome: MutationOutcome) -> Result<(), MetaError> {
    match outcome {
        MutationOutcome::ReplicationClaimUpdated { applied: true } => Ok(()),
        MutationOutcome::ReplicationClaimUpdated { applied: false } => Err(MetaError::Engine(
            "remote multipart ownership lost".to_owned(),
        )),
        _ => Err(MetaError::Engine(
            "unexpected remote multipart journal outcome".to_owned(),
        )),
    }
}
fn unavailable(error: MetaError) -> ReplicationError {
    ReplicationError::Unavailable(format!("remote multipart journal unavailable: {error}"))
}

#[async_trait]
impl<M: MetadataStore + ?Sized, C: Clock + ?Sized> ReplicationMultipartJournal
    for UploadJournal<'_, M, C>
{
    async fn begin(
        &self,
        endpoint: &str,
        destination_bucket: &str,
    ) -> Result<String, ReplicationError> {
        let origin_token =
            self.entry.claim_token.clone().ok_or_else(|| {
                ReplicationError::Unavailable("replication claim missing".to_owned())
            })?;
        let id = uuid::Uuid::new_v4().simple().to_string();
        let now = self.clock.now();
        let upload = RemoteMultipartUpload {
            id: id.clone(),
            outbox_id: self.entry.id.clone(),
            origin_token,
            destination: RemoteMultipartDestination {
                bucket: self.entry.bucket.clone(),
                key: self.entry.key.clone(),
                target_arn: self.entry.target_arn.clone(),
                endpoint: endpoint.to_owned(),
                destination_bucket: destination_bucket.to_owned(),
            },
            upload_id: None,
            cleanup_token: None,
            lease_until: None,
            next_attempt_at: now,
            orphan_reported: false,
            last_error: None,
        };
        let outcome = self
            .meta
            .submit(Mutation::ReplicationUpload {
                bucket: self.entry.bucket.clone(),
                operation: ReplicationUploadMutation::Begin {
                    upload: Box::new(upload),
                    now,
                },
            })
            .await
            .map_err(unavailable)?;
        ownership(outcome).map_err(unavailable)?;
        Ok(id)
    }
    async fn record_upload_id(
        &self,
        attempt: &str,
        remote_id: &str,
    ) -> Result<(), ReplicationError> {
        let origin_token =
            self.entry.claim_token.clone().ok_or_else(|| {
                ReplicationError::Unavailable("replication claim missing".to_owned())
            })?;
        let outcome = self
            .meta
            .submit(Mutation::ReplicationUpload {
                bucket: self.entry.bucket.clone(),
                operation: ReplicationUploadMutation::RecordUploadId {
                    id: attempt.to_owned(),
                    origin_token,
                    upload_id: remote_id.to_owned(),
                    now: self.clock.now(),
                },
            })
            .await
            .map_err(unavailable)?;
        ownership(outcome).map_err(unavailable)
    }
    async fn retire(&self, attempt: &str) -> Result<(), ReplicationError> {
        let origin_token =
            self.entry.claim_token.clone().ok_or_else(|| {
                ReplicationError::Unavailable("replication claim missing".to_owned())
            })?;
        let outcome = self
            .meta
            .submit(Mutation::ReplicationUpload {
                bucket: self.entry.bucket.clone(),
                operation: ReplicationUploadMutation::Retire {
                    id: attempt.to_owned(),
                    origin_token,
                },
            })
            .await
            .map_err(unavailable)?;
        ownership(outcome).map_err(unavailable)
    }
}

impl ReplicationEngine {
    pub(crate) async fn cleanup_uploads<
        M: MetadataStore + ?Sized,
        R: SinkRouter + ?Sized,
        C: Clock + ?Sized,
    >(
        &self,
        meta: &M,
        router: &R,
        clock: &C,
    ) -> Result<(), MetaError> {
        use std::sync::atomic::Ordering;
        // One opportunity per pass preserves delivery fairness even when cleanup is slow.
        {
            let MutationOutcome::ReplicationUploadBatch(batch) = meta
                .submit(Mutation::ClaimReplicationUploadCleanup {
                    limit: 1,
                    now: clock.now(),
                    lease_secs: 300,
                })
                .await?
            else {
                return Err(MetaError::Engine(
                    "unexpected remote cleanup batch".to_owned(),
                ));
            };
            self.claim_failures
                .orphan_initiation
                .fetch_add(u64::from(batch.orphaned), Ordering::Relaxed);
            let Some(upload) = batch.uploads.into_iter().next() else {
                return Ok(());
            };
            let token = upload
                .cleanup_token
                .clone()
                .ok_or_else(|| MetaError::Engine("remote cleanup claim missing".to_owned()))?;
            let result = {
                let work = async {
                    match router.sink_for(upload.destination.target_arn.as_deref()) {
                        Some(sink) => sink.abort_multipart(&upload).await,
                        None => Err(ReplicationError::Unavailable(
                            "saved replication target removed; remote cleanup retained".to_owned(),
                        )),
                    }
                };
                let renew = async {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                        ownership(
                            meta.submit(Mutation::ReplicationUpload {
                                bucket: upload.destination.bucket.clone(),
                                operation: ReplicationUploadMutation::RenewCleanup {
                                    id: upload.id.clone(),
                                    cleanup_token: token.clone(),
                                    now: clock.now(),
                                    lease_secs: 300,
                                },
                            })
                            .await?,
                        )?;
                    }
                    #[allow(unreachable_code)]
                    Ok::<(), MetaError>(())
                };
                futures_util::pin_mut!(work, renew);
                match futures_util::future::select(work, renew).await {
                    futures_util::future::Either::Left((result, _)) => result,
                    futures_util::future::Either::Right((result, _)) => {
                        self.claim_failures
                            .cleanup_failed
                            .fetch_add(1, Ordering::Relaxed);
                        result?;
                        return Err(MetaError::Engine(
                            "remote cleanup heartbeat ended".to_owned(),
                        ));
                    }
                }
            };
            // The heartbeat has stopped before settlement, so delayed acknowledgement is safe.
            let error = result.err().map(|error| error.to_string());
            if error.is_some() {
                self.claim_failures
                    .cleanup_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
            let now = clock.now();
            let retry_at = cairn_types::time::Timestamp(now.0.saturating_add(60_000));
            meta.submit(Mutation::ReplicationUpload {
                bucket: upload.destination.bucket.clone(),
                operation: ReplicationUploadMutation::SettleCleanup {
                    id: upload.id,
                    cleanup_token: token,
                    now,
                    retry_at,
                    error,
                },
            })
            .await
            .and_then(ownership)
            .inspect_err(|_| {
                self.claim_failures
                    .cleanup_failed
                    .fetch_add(1, Ordering::Relaxed);
            })?;
        }
        Ok(())
    }
}
