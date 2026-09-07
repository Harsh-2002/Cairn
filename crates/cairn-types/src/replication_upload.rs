//! Durable remote multipart attempts. These rows deliberately outlive source buckets and outboxes.

use crate::id::{BucketName, ObjectKey, ReplicationClaimToken};
use crate::time::Timestamp;

/// Immutable routing identity; credentials are resolved separately and never stored here.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteMultipartDestination {
    /// Original source bucket, also the stable metadata shard key.
    pub bucket: BucketName,
    /// Original source object key.
    pub key: ObjectKey,
    /// Configured replication target identity.
    pub target_arn: Option<String>,
    /// Exact endpoint used to initiate the upload.
    pub endpoint: String,
    /// Exact remote bucket used to initiate the upload.
    pub destination_bucket: String,
}

/// One attempt, including its independently leased cleanup ownership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteMultipartUpload {
    /// Random attempt identity, known before contacting the destination.
    pub id: String,
    /// Originating outbox identity; no foreign key or cascading deletion.
    pub outbox_id: String,
    /// Originating delivery attempt.
    pub origin_token: ReplicationClaimToken,
    /// Saved routing identity.
    pub destination: RemoteMultipartDestination,
    /// Receipt from the destination; absent means an explicit unknown initiation incident.
    pub upload_id: Option<String>,
    /// Claimed cleanup worker, if any.
    pub cleanup_token: Option<ReplicationClaimToken>,
    /// Cleanup claim expiry.
    pub lease_until: Option<Timestamp>,
    /// Next cleanup opportunity.
    pub next_attempt_at: Timestamp,
    /// Whether a missing initiation receipt has already been reported.
    pub orphan_reported: bool,
    /// Operator-visible reason cleanup remains outstanding.
    pub last_error: Option<String>,
}

/// Per-bucket writer operations for a remote multipart journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationUploadMutation {
    /// Persist before initiate; rejects a stale originating claim.
    Begin {
        upload: Box<RemoteMultipartUpload>,
        now: Timestamp,
    },
    /// Persist a late receipt even after ownership loss; `applied` reports origin ownership.
    RecordUploadId {
        id: String,
        origin_token: ReplicationClaimToken,
        upload_id: String,
        now: Timestamp,
    },
    /// Forget only after a confirmed complete/abort response for this exact attempt.
    Retire {
        id: String,
        origin_token: ReplicationClaimToken,
    },
    /// Extend a still-owned cleanup lease.
    RenewCleanup {
        id: String,
        cleanup_token: ReplicationClaimToken,
        now: Timestamp,
        lease_secs: i64,
    },
    /// Settle an exact cleanup lease; absent error means confirmed remote abort.
    SettleCleanup {
        id: String,
        cleanup_token: ReplicationClaimToken,
        now: Timestamp,
        retry_at: Timestamp,
        error: Option<String>,
    },
}

/// Bounded cleanup claim result. Unknown receipt incidents consume the same page budget.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicationUploadBatch {
    /// Known receipts safe to abort, each carrying an exact cleanup lease.
    pub uploads: Vec<RemoteMultipartUpload>,
    /// Newly discovered initiation receipts missing after delivery ownership ended.
    pub orphaned: u32,
}
