//! A fake replication sink that records intents and can simulate failures.

use crate::error::ReplicationError;
use crate::id::{ObjectKey, VersionId};
use crate::replication::ReplicatedObject;
use crate::traits::ReplicationSink;
use std::sync::Mutex;

/// What the fake sink should do on the next call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SinkBehavior {
    /// Succeed.
    #[default]
    Succeed,
    /// Fail retryably.
    Retryable,
    /// The target is unreachable (does not consume the attempt budget).
    Unavailable,
    /// Fail terminally.
    Terminal,
}

/// A recorded replication intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedIntent {
    /// An object was put.
    Put {
        /// The key.
        key: ObjectKey,
        /// The version.
        version_id: VersionId,
        /// The logical size.
        size: u64,
    },
    /// A delete marker was propagated.
    DeleteMarker {
        /// The key.
        key: ObjectKey,
        /// The version.
        version_id: VersionId,
    },
}

/// A fake [`ReplicationSink`] capturing what would have been replicated.
#[derive(Debug, Default)]
pub struct FakeReplicationSink {
    behavior: Mutex<SinkBehavior>,
    intents: Mutex<Vec<RecordedIntent>>,
}

impl FakeReplicationSink {
    /// A sink that always succeeds.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the behavior for subsequent calls.
    pub fn set_behavior(&self, behavior: SinkBehavior) {
        *self.behavior.lock().unwrap() = behavior;
    }

    /// The recorded intents so far.
    #[must_use]
    pub fn intents(&self) -> Vec<RecordedIntent> {
        self.intents.lock().unwrap().clone()
    }

    fn check(&self) -> Result<(), ReplicationError> {
        match *self.behavior.lock().unwrap() {
            SinkBehavior::Succeed => Ok(()),
            SinkBehavior::Retryable => Err(ReplicationError::Retryable("simulated".to_owned())),
            SinkBehavior::Unavailable => {
                Err(ReplicationError::Unavailable("simulated down".to_owned()))
            }
            SinkBehavior::Terminal => Err(ReplicationError::Terminal("simulated".to_owned())),
        }
    }
}

#[async_trait::async_trait]
impl ReplicationSink for FakeReplicationSink {
    async fn put_object(&self, object: ReplicatedObject) -> Result<(), ReplicationError> {
        self.check()?;
        self.intents.lock().unwrap().push(RecordedIntent::Put {
            key: object.key.clone(),
            version_id: object.version_id.clone(),
            size: object.size,
        });
        Ok(())
    }

    async fn delete_marker(
        &self,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<(), ReplicationError> {
        self.check()?;
        self.intents
            .lock()
            .unwrap()
            .push(RecordedIntent::DeleteMarker {
                key: key.clone(),
                version_id: version.clone(),
            });
        Ok(())
    }
}

/// Tracks real writer-issued claims while tests arrange settled outbox fixtures.
#[derive(Default, Debug)]
pub struct ReplicationClaims(
    std::collections::BTreeMap<String, (crate::id::ReplicationClaimToken, crate::Timestamp)>,
);

impl ReplicationClaims {
    /// Claim a batch and retain its exact tokens for later fixture settlement.
    pub async fn claim<M: crate::MetadataStore + ?Sized>(
        &mut self,
        meta: &M,
        limit: u32,
        now: crate::Timestamp,
    ) -> Result<Vec<crate::OutboxEntry>, crate::MetaError> {
        let entries = meta.claim_replication_batch(limit, now).await?;
        for entry in &entries {
            self.0.insert(
                entry.id.clone(),
                (
                    entry.claim_token.clone().expect("claimed entry token"),
                    entry.lease_until.expect("claimed entry lease"),
                ),
            );
        }
        Ok(entries)
    }

    /// Obtain ownership of one fixture, returning other newly claimed rows to pending.
    pub async fn take<M: crate::MetadataStore + ?Sized>(
        &mut self,
        meta: &M,
        id: &str,
        now: crate::Timestamp,
    ) -> crate::id::ReplicationClaimToken {
        if let Some((token, until)) = self.0.remove(id) {
            if until >= now {
                return token;
            }
        }
        let entries = meta
            .claim_replication_batch(1000, now)
            .await
            .expect("claim fixture");
        let mut token = None;
        for entry in entries {
            let claim_token = entry.claim_token.expect("claimed entry token");
            if entry.id == id {
                token = Some(claim_token);
            } else {
                meta.submit(crate::Mutation::DeferReplication {
                    id: entry.id,
                    claim_token,
                    now,
                    next_attempt_at: entry.next_attempt_at,
                    last_error: entry.last_error,
                })
                .await
                .expect("release unrelated fixture");
            }
        }
        token.expect("requested fixture must be due or already claimed")
    }
}

/// Exercise stale, expired, recovered and renewed ownership against any metadata backend.
/// The caller supplies an already persisted object row.
pub async fn assert_replication_claim_fencing<M: crate::MetadataStore + ?Sized>(
    meta: &M,
    row: &crate::ObjectVersionRow,
) {
    use crate::{
        Mutation, MutationOutcome, OutboxEntry, ReplicationOp, ReplicationStatus, Timestamp,
    };
    for action in 0..4 {
        let id = format!("claim-fence-{}-{action}", row.bucket.as_str());
        meta.submit(Mutation::EnqueueReplication(Box::new(OutboxEntry {
            id: id.clone(),
            bucket: row.bucket.clone(),
            key: row.key.clone(),
            version_id: row.version_id.clone(),
            operation: ReplicationOp::ObjectCreate,
            rule_id: "rule".to_owned(),
            target_arn: None,
            attempts: 0,
            next_attempt_at: Timestamp(0),
            status: ReplicationStatus::Pending,
            last_error: None,
            priority: 0,
            lease_until: None,
            enqueued_at: Timestamp(0),
            claim_token: None,
        })))
        .await
        .unwrap();
        let first = meta
            .claim_replication_batch(100, Timestamp(0))
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.id == id)
            .unwrap();
        let token_a = first.claim_token.unwrap();
        let rejected = |outcome| {
            assert!(matches!(
                outcome,
                MutationOutcome::ReplicationClaimUpdated { applied: false }
            ))
        };
        let accepted = |outcome| {
            assert!(matches!(
                outcome,
                MutationOutcome::ReplicationClaimUpdated { applied: true }
            ))
        };
        // Expiry fences a worker even before another worker takes over.
        rejected(
            meta.submit(Mutation::RenewReplicationClaim {
                id: id.clone(),
                claim_token: token_a.clone(),
                now: Timestamp(300_001),
                lease_secs: 300,
            })
            .await
            .unwrap(),
        );
        let second = meta
            .claim_replication_batch(100, Timestamp(300_001))
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.id == id)
            .unwrap();
        let token_b = second.claim_token.clone().unwrap();
        assert_ne!(token_a, token_b);
        let before = meta
            .get_version(&row.bucket, &row.key, &row.version_id)
            .await
            .unwrap()
            .unwrap();
        for stale in [
            Mutation::MarkReplicationDone {
                id: id.clone(),
                claim_token: token_a.clone(),
                now: Timestamp(300_002),
            },
            Mutation::MarkReplicationFailed {
                id: id.clone(),
                claim_token: token_a.clone(),
                now: Timestamp(300_002),
                error: "stale terminal".to_owned(),
                next_attempt_at: None,
            },
            Mutation::MarkReplicationFailed {
                id: id.clone(),
                claim_token: token_a.clone(),
                now: Timestamp(300_002),
                error: "stale retry".to_owned(),
                next_attempt_at: Some(Timestamp(900_000)),
            },
            Mutation::DeferReplication {
                id: id.clone(),
                claim_token: token_a.clone(),
                now: Timestamp(300_002),
                next_attempt_at: Timestamp(900_000),
                last_error: Some("stale defer".to_owned()),
            },
            Mutation::RenewReplicationClaim {
                id: id.clone(),
                claim_token: token_a.clone(),
                now: Timestamp(300_002),
                lease_secs: 1000,
            },
        ] {
            rejected(meta.submit(stale).await.unwrap());
        }
        let after = meta
            .get_version(&row.bucket, &row.key, &row.version_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.replication_status, after.replication_status);
        assert_eq!(before.replicated_at, after.replicated_at);
        let unchanged = meta
            .list_due_replication(100, Timestamp(900_000))
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.id == id)
            .unwrap();
        assert_eq!(unchanged.claim_token, second.claim_token);
        assert_eq!(unchanged.lease_until, second.lease_until);
        assert_eq!(unchanged.attempts, 0);
        assert_eq!(unchanged.last_error, None);
        assert_eq!(unchanged.next_attempt_at, Timestamp(0));
        accepted(
            meta.submit(Mutation::RenewReplicationClaim {
                id: id.clone(),
                claim_token: token_b.clone(),
                now: Timestamp(500_000),
                lease_secs: 300,
            })
            .await
            .unwrap(),
        );
        let settlement = match action {
            0 => Mutation::MarkReplicationDone {
                id: id.clone(),
                claim_token: token_b.clone(),
                now: Timestamp(700_000),
            },
            1 => Mutation::MarkReplicationFailed {
                id: id.clone(),
                claim_token: token_b.clone(),
                now: Timestamp(700_000),
                error: "current terminal".to_owned(),
                next_attempt_at: None,
            },
            2 => Mutation::MarkReplicationFailed {
                id: id.clone(),
                claim_token: token_b.clone(),
                now: Timestamp(700_000),
                error: "current retry".to_owned(),
                next_attempt_at: Some(Timestamp(1_000_000)),
            },
            _ => Mutation::DeferReplication {
                id: id.clone(),
                claim_token: token_b.clone(),
                now: Timestamp(700_000),
                next_attempt_at: Timestamp(1_000_000),
                last_error: None,
            },
        };
        accepted(meta.submit(settlement).await.unwrap());
        rejected(
            meta.submit(Mutation::MarkReplicationDone {
                id: id.clone(),
                claim_token: token_b,
                now: Timestamp(700_001),
            })
            .await
            .unwrap(),
        );
    }
    // Startup recovery invalidates ownership even when the lease has not expired.
    let entries = meta
        .claim_replication_batch(100, Timestamp(1_000_000))
        .await
        .unwrap();
    assert!(!entries.is_empty());
    meta.submit(Mutation::RecoverClaimedReplication)
        .await
        .unwrap();
    for entry in entries {
        assert!(matches!(
            meta.submit(Mutation::MarkReplicationDone {
                id: entry.id,
                claim_token: entry.claim_token.unwrap(),
                now: Timestamp(1_000_001),
            })
            .await
            .unwrap(),
            MutationOutcome::ReplicationClaimUpdated { applied: false }
        ));
    }
}
