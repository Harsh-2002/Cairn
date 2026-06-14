//! [`BucketRoutedSink`] — a source-bucket-aware view over a replication destination.
//!
//! The cross-crate [`ReplicationSink`](cairn_types::traits::ReplicationSink) trait ships an
//! object or a delete marker to a *fixed* destination: its method signatures do not carry the
//! source bucket. Per-rule replication, however, needs the destination bucket to be chosen from
//! the **source** bucket (each source bucket's stored replication rule names its own
//! `<Destination><Bucket>`). This module closes that gap entirely within `cairn-replication`,
//! without touching the shared trait spine.
//!
//! [`BucketRoutedSink`] is the engine's sink boundary. It mirrors [`ReplicationSink`] but threads
//! the source [`BucketName`] into each call so the implementation can route per source bucket:
//!
//! * [`HttpS3Sink`](crate::HttpS3Sink) implements it directly, resolving the destination bucket
//!   from its `source -> dest` map (with a default fallback).
//! * Every plain [`ReplicationSink`] gets a blanket implementation that ignores the source
//!   bucket and delegates straight through, so the in-memory test double
//!   ([`FakeReplicationSink`](cairn_types::testing::FakeReplicationSink)) and any other fixed
//!   single-destination sink keep working unchanged (preserving node->node behaviour).

use async_trait::async_trait;
use cairn_types::error::ReplicationError;
use cairn_types::id::{BucketName, ObjectKey, VersionId};
use cairn_types::replication::ReplicatedObject;
use cairn_types::traits::ReplicationSink;

/// A replication destination that chooses where to ship from the **source** bucket.
///
/// This is the trait the [`ReplicationEngine`](crate::ReplicationEngine) drives. It is identical
/// in spirit to [`ReplicationSink`] but receives the source bucket so the destination bucket can
/// be resolved per request.
#[async_trait]
pub trait BucketRoutedSink: Send + Sync {
    /// Put an object that originated in `source_bucket`, choosing the destination from it.
    async fn put_object(
        &self,
        source_bucket: &BucketName,
        object: ReplicatedObject,
    ) -> Result<(), ReplicationError>;

    /// Propagate a deletion/delete marker for a key in `source_bucket`.
    async fn delete_marker(
        &self,
        source_bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<(), ReplicationError>;
}

/// Chooses which [`BucketRoutedSink`] ships a given outbox entry, keyed by the entry's rule's
/// remote-target ARN (ARCH §20.5).
///
/// Per-bucket remote targets mean different rules can ship to different destinations; the engine
/// asks a router for the sink that serves a particular target ARN. A `None` ARN is the legacy
/// fixed-destination form (a rule without a remote target). A router that does not recognise the
/// ARN returns `None`, and the engine treats the entry as undeliverable (terminal: its target is
/// unknown).
pub trait SinkRouter: Send + Sync {
    /// The sink that ships entries for `target_arn`, or `None` if no sink serves it.
    fn sink_for<'a>(&'a self, target_arn: Option<&str>) -> Option<&'a dyn BucketRoutedSink>;
}

/// The trivial single-destination router: it returns its one wrapped sink for **any** target ARN
/// (including `None`). This preserves the original single-sink engine seam — tests wrap their fake
/// in `SingleSink(FakeReplicationSink)` and the engine routes every entry to it.
#[derive(Debug, Clone)]
pub struct SingleSink<S>(pub S);

impl<S> SinkRouter for SingleSink<S>
where
    S: BucketRoutedSink,
{
    fn sink_for<'a>(&'a self, _target_arn: Option<&str>) -> Option<&'a dyn BucketRoutedSink> {
        Some(&self.0)
    }
}

/// Blanket adapter: any fixed-destination [`ReplicationSink`] is a [`BucketRoutedSink`] that
/// ignores the source bucket. This keeps every existing sink (notably the in-memory test double)
/// usable by the engine with no changes, and preserves the single-destination node->node path.
///
/// Note: [`HttpS3Sink`](crate::HttpS3Sink) deliberately does **not** implement
/// [`ReplicationSink`]; it implements [`BucketRoutedSink`] directly so it can route per source
/// bucket, which also keeps this blanket implementation coherent (no overlap).
#[async_trait]
impl<T> BucketRoutedSink for T
where
    T: ReplicationSink + ?Sized,
{
    async fn put_object(
        &self,
        _source_bucket: &BucketName,
        object: ReplicatedObject,
    ) -> Result<(), ReplicationError> {
        ReplicationSink::put_object(self, object).await
    }

    async fn delete_marker(
        &self,
        _source_bucket: &BucketName,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<(), ReplicationError> {
        ReplicationSink::delete_marker(self, key, version).await
    }
}
