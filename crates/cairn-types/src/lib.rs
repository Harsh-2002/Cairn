//! `cairn-types` — the shared domain types, the typed error tree, and the trait spine that
//! every other Cairn crate is written against (ARCH §12). This crate depends on no engine
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
pub mod object;
pub mod replication;
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
    AuthMethod, AuthOutcome, ChunkSigningContext, Principal, RequestView, RequesterClass, Role,
};
pub use authz::{
    Acl, Action, AuthzInput, Decision, DenyReason, Effect, Grant, Grantee, OwnershipMode,
    Permission, Policy, PublicAccessBlock, RequestContext, Resource, Statement,
};
pub use blob::{
    BlobReadHandle, ByteRange, ContentRange, PartRef, ReconcileOpts, ReconcileReport, StageOptions,
    StagedBlob, StagedPart, ZeroCopyRead,
};
pub use bucket::{
    Bucket, CompressionAlgorithm, CompressionPolicy, ConfigAspect, ConfigDoc, VersioningState,
};
pub use crypto::{Nonce, Sealed, Signature};
pub use error::{
    AuthError, BlobError, BodyError, ConfigError, CryptoError, Error, MetaError, ReplicationError,
    Result,
};
pub use id::{BucketName, InvalidName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
pub use meta::{
    ActivityEntry, BucketCounts, BucketRequestCount, ClaimOutcome, IfNoneMatch, ListPage,
    ListQuery, MetricsRange, MultipartSession, MultipartStatus, Mutation, MutationOutcome,
    ObjectSummary, OpCount, OutboxEntry, PartRecord, Precondition, ReplicationOp,
    ReplicationStatus, RequestMetricRow, RequestMetricsSeries, ShareDisposition, ShareRow,
    StoreCounts, TimePoint, User, UserRecord, UserSigV4Credentials, UserWithBearerHash,
};
pub use object::{
    ChecksumAlgorithm, ChecksumSet, ChecksumValue, CompressionDescriptor, ETag, ObjectVersionRow,
    StorageClass, UserMetadata,
};
pub use replication::ReplicatedObject;
pub use time::Timestamp;
pub use traits::{
    Authenticator, AuthorizationEngine, BlobStore, Clock, Crypto, MetadataStore, PublicUrl,
    ReconcileOracle, ReplicationSink,
};
