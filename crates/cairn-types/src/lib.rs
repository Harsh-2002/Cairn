//! `cairn-types` — the shared domain types, the typed error tree, and the trait spine that
//! every other Cairn crate is written against (ARCH 12). This crate depends on no engine
//! implementation, so freezing it freezes the seams: the protocol and control layers consume
//! only these traits, and the canonical in-memory [`testing`] doubles make the whole engine
//! unit-testable without a disk or a database.

#![forbid(unsafe_code)]

use bytes::Bytes;
use std::pin::Pin;

pub mod auth;
pub mod authz;
pub mod blob;
pub mod bucket;
pub mod crypto;
pub mod error;
pub mod id;
pub mod meta;
pub mod notification;
pub mod object;
pub mod replication;
pub mod secret;
pub mod sse;
pub mod time;
pub mod traits;

#[cfg(feature = "testing")]
pub mod testing;

/// A streaming request body of payload bytes (post chunk-decoding). Errors carry client
/// body failures.
pub type BodyStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<Bytes, error::BodyError>> + Send>>;

/// A streaming blob read of logical (decompressed) bytes.
pub type BlobStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<Bytes, error::BlobError>> + Send>>;

// --- Convenience re-exports of the most-used items ---
pub use auth::{
    AuthMethod, AuthOutcome, ChunkSigningContext, ClientSource, Principal, RequestView,
    RequesterClass, Role,
};
pub use authz::{
    Acl, Action, AuthzInput, Decision, DenyReason, Effect, Grant, Grantee, OwnershipMode,
    Permission, Policy, PublicAccessBlock, RequestContext, Resource, Statement,
};
pub use blob::{
    BlobCipher, BlobProbe, BlobReadHandle, ByteRange, ContentRange, PartRef, ReconcileOpts,
    ReconcileReport, StageOptions, StagedBlob, StagedPart, ZeroCopyRead,
};
pub use bucket::{
    Bucket, CompressionAlgorithm, CompressionPolicy, ConfigAspect, ConfigDoc, DefaultRetention,
    DefaultRetentionError, ObjectLockConfiguration, RetentionPeriod, VersioningState,
};
pub use crypto::{Nonce, Sealed, Signature};
pub use error::{
    AuthError, BlobError, BodyError, ConfigError, CryptoError, Error, MetaError, ReplicationError,
    Result,
};
pub use id::{
    BucketName, InvalidName, MultipartClaimToken, ObjectKey, StoragePath, UploadId, UserId,
    VersionId,
};
pub use meta::{
    ActivityEntry, BucketCounts, BucketRequestCount, ClaimOutcome, ClaimReleaseOutcome,
    CurrentVersionGuard, IfNoneMatch, ImportBucketProgress, ImportJob, ImportJobCursor,
    ImportJobListQuery, ImportJobPage, ImportJobRecord, ImportJobSummary, ImportState,
    InitialObjectState, LATENCY_BUCKET_BOUNDS_MS, LATENCY_BUCKETS, ListPage, ListQuery,
    MAX_IMPORT_JOB_PAGE_SIZE, MAX_IMPORT_JOB_PRUNE_BATCH, MetricsRange, MultipartSession,
    MultipartStatus, MultipartTerminalOutcome, Mutation, MutationOutcome, ObjectSummary, OpCount,
    OutboxEntry, PartRecord, Precondition, ReplicationOp, ReplicationStatus, RequestMetricRow,
    RequestMetricsSeries, SessionCredentialRecord, SessionCredentialSummary, ShareDisposition,
    ShareLookupHash, ShareRow, StatusCount, StoreCounts, TagSummary, TaggedObject, TimePoint, User,
    UserRecord, UserSessionCredentials, UserSigV4Credentials, UserWithBearerHash, WebhookEntry,
    WebhookStatus, latency_bucket_index, latency_quantile_ms,
};
pub use notification::{
    EventKind, NotificationConfig, SealedWebhookSecret, WebhookEndpoint, WebhookSecret,
};
pub use object::{
    ChecksumAlgorithm, ChecksumSet, ChecksumValue, CompressionDescriptor, ETag,
    ExplicitObjectLockIntent, GovernanceBypass, ObjectLockMode, ObjectLockState, ObjectRetention,
    ObjectVersionRow, StorageClass, UserMetadata,
};
pub use replication::ReplicatedObject;
pub use secret::{SecretKey32, SecretString};
pub use time::Timestamp;
pub use traits::{
    Authenticator, AuthorizationEngine, BlobStore, Clock, Crypto, MetadataStore, PublicUrl,
    ReconcileOracle, ReplicationSink,
};
