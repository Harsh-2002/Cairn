//! The typed error tree. Each subsystem returns its own domain error from its trait
//! methods; the protocol/control layers fold these into the canonical [`Error`], which a
//! single translator (in `cairn-protocol` / `cairn-control`) maps totally to an S3 XML or JSON
//! response (ARCH 25). Keeping every wire-mappable condition in one enum is what makes
//! that translator total and testable.

use crate::id::InvalidName;
use thiserror::Error;

/// An error reading a client request body stream (disconnect, timeout, declared-length
/// mismatch). Surfaced by the body stream the blob store consumes.
#[derive(Debug, Error)]
pub enum BodyError {
    /// The underlying transport failed mid-stream.
    #[error("client body transport error: {0}")]
    Transport(String),
    /// The body ended before the declared content length.
    #[error("client body ended prematurely")]
    Truncated,
    /// A per-request timeout elapsed while reading the body.
    #[error("client body read timed out")]
    Timeout,
}

/// Failures of the blob store (the local filesystem data plane).
#[derive(Debug, Error)]
pub enum BlobError {
    /// An underlying I/O error.
    #[error("blob io error: {0}")]
    Io(String),
    /// The data filesystem is out of space.
    #[error("out of space on the data filesystem")]
    OutOfSpace,
    /// The object exceeded the configured hard size ceiling while streaming.
    #[error("object exceeds the configured size ceiling")]
    SizeExceeded,
    /// The blob (or part/session) does not exist.
    #[error("blob not found")]
    NotFound,
    /// A stored blob failed its self-describing-format or hash integrity check.
    #[error("blob integrity check failed: {0}")]
    Corruption(String),
    /// A client body error propagated through staging.
    #[error("client body error: {0}")]
    Body(#[from] BodyError),
}

/// Failures of the metadata store.
#[derive(Debug, Error)]
pub enum MetaError {
    /// An underlying storage-engine error.
    #[error("metadata store error: {0}")]
    Engine(String),
    /// A uniqueness constraint was violated (e.g. bucket/key/version already exists).
    #[error("metadata uniqueness conflict")]
    Conflict,
    /// A foreign-key or referential-integrity constraint was violated.
    #[error("metadata referential-integrity violation")]
    Integrity,
    /// A conditional-write precondition evaluated false inside the commit transaction.
    #[error("conditional precondition failed")]
    PreconditionFailed,
    /// The single-writer task has shut down and can no longer accept mutations.
    #[error("metadata writer has shut down")]
    WriterClosed,
    /// A configured byte quota would be exceeded by this mutation.
    #[error("quota exceeded")]
    QuotaExceeded,
    /// A container still holds rows and cannot be deleted (e.g. a bucket with objects or in-progress
    /// multipart uploads), enforced *inside* the commit transaction so the check is atomic with the
    /// delete.
    #[error("container not empty")]
    NotEmpty,
}

/// Failures of authentication.
#[derive(Debug, Error)]
pub enum AuthError {
    /// Credentials were missing or unparseable.
    #[error("missing or malformed credentials")]
    Malformed,
    /// The named access key does not exist or is inactive.
    #[error("unknown or inactive access key")]
    UnknownKey,
    /// The presented signature did not match.
    #[error("signature mismatch")]
    SignatureMismatch,
    /// The request timestamp was outside the allowed skew window.
    #[error("request timestamp outside the allowed skew window")]
    SkewedClock,
    /// The presigned URL has expired.
    #[error("presigned url expired")]
    Expired,
    /// A streaming chunk signature failed verification.
    #[error("streaming chunk signature mismatch")]
    ChunkSignatureMismatch,
}

/// Failures of the cryptography facility.
#[derive(Debug, Error)]
pub enum CryptoError {
    /// Authenticated decryption failed (tampering or wrong key).
    #[error("authenticated decryption failed")]
    Decrypt,
    /// Encryption failed.
    #[error("encryption failed")]
    Encrypt,
    /// The master key was required but absent or malformed.
    #[error("master key missing or malformed")]
    Key,
    /// The envelope names a key id that is not on this node's ring — a rotation/retirement window
    /// or a misconfigured ring, NOT tampering. Distinct from [`Decrypt`](Self::Decrypt) so callers
    /// can treat it as *transiently unavailable* (retry, preserve the budget) instead of
    /// permanently corrupt: conflating the two lets one mid-rotation pass stamp whole buckets
    /// terminally failed.
    #[error("no master key with that key id is on the ring")]
    UnknownKeyId,
    /// The active master key reached its seal-count hard stop; rotate to a new active key
    /// before sealing more secrets (audit #29, Phase E). Opens are never affected.
    #[error("active master key reached its seal-count limit; rotate the master key")]
    KeyRotationRequired,
}

/// Failures driving a replication sink.
#[derive(Debug, Error)]
pub enum ReplicationError {
    /// A transient *per-object* failure (e.g. a momentary source-read hiccup); the entry is
    /// retried with backoff and **consumes the attempt budget**, turning terminal once exhausted.
    #[error("retryable replication failure: {0}")]
    Retryable(String),
    /// The destination *target* is unreachable — a connection failure, request timeout, `5xx`, or
    /// throttle (`408`/`429`). The entry is rescheduled with backoff but does **not** consume the
    /// attempt budget, so a target that is down for an extended period keeps its queued work and
    /// auto-resumes when it returns, instead of exhausting to a terminal failure that needs an
    /// operator retry. (A genuinely-removed target still terminates via the no-sink path, which
    /// does consume the budget.)
    #[error("replication target unavailable: {0}")]
    Unavailable(String),
    /// A permanent failure; the entry should be marked failed for operator attention.
    #[error("terminal replication failure: {0}")]
    Terminal(String),
}

/// Configuration validation failures (fail-fast on load).
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A required value was missing or empty.
    #[error("config: {0}")]
    Invalid(String),
}

/// The canonical, wire-mappable error. Every condition in the ARCH 25.2 mapping table
/// is a variant here; the single translator maps each to an HTTP status + S3/JSON code.
#[derive(Debug, Error)]
pub enum Error {
    /// The named bucket does not exist.
    #[error("no such bucket")]
    NoSuchBucket,
    /// The named object key does not exist.
    #[error("no such key")]
    NoSuchKey,
    /// The named object version does not exist.
    #[error("no such version")]
    NoSuchVersion,
    /// A bucket with this name already exists globally.
    #[error("bucket already exists")]
    BucketAlreadyExists,
    /// The caller already owns a bucket with this name.
    #[error("bucket already owned by you")]
    BucketAlreadyOwnedByYou,
    /// The bucket is not empty and cannot be deleted.
    #[error("bucket not empty")]
    BucketNotEmpty,
    /// The multipart upload session does not exist or is no longer active.
    #[error("no such upload")]
    NoSuchUpload,
    /// A conditional precondition failed.
    #[error("precondition failed")]
    PreconditionFailed,
    /// The object exceeds the configured maximum size.
    #[error("entity too large")]
    EntityTooLarge,
    /// The data filesystem is out of space.
    #[error("insufficient storage")]
    InsufficientStorage,
    /// A supplied checksum did not match the computed one.
    #[error("bad digest")]
    BadDigest,
    /// A supplied checksum or content-MD5 was malformed.
    #[error("invalid digest")]
    InvalidDigest,
    /// A request/XML body was malformed.
    #[error("malformed xml")]
    MalformedXml,
    /// A tag set violated the S3 tag limits (count, key/value length, charset, duplicates, or a
    /// reserved `aws:` key prefix) — distinct from a malformed body (ARCH 17.1).
    #[error("invalid tag: {0}")]
    InvalidTag(String),
    /// A policy document was malformed.
    #[error("malformed policy")]
    MalformedPolicy,
    /// A request argument was invalid.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// A request was invalid for the bucket's configuration (e.g. setting an ACL while
    /// Object Ownership disables ACLs).
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// Authorization was denied (policy, ACL, or Block Public Access).
    #[error("access denied")]
    AccessDenied,
    /// The access key id is unknown.
    #[error("invalid access key id")]
    InvalidAccessKeyId,
    /// The request signature did not match.
    #[error("signature does not match")]
    SignatureDoesNotMatch,
    /// The request's signed timestamp fell outside the allowed clock-skew window. Distinct from a
    /// generic bad argument: SDKs key their clock-resync-and-retry logic off exactly this S3 code,
    /// so it must not be folded into `InvalidArgument`.
    #[error("The difference between the request time and the current time is too large")]
    RequestTimeTooSkewed,
    /// The requested byte range is not satisfiable.
    #[error("invalid range")]
    InvalidRange,
    /// The requested operation is not implemented. This message reaches the client verbatim (it is
    /// a client-facing capability answer, not an internal fault — see `error_map::error_response`),
    /// so it is phrased for an operator reading it out of an SDK exception.
    #[error("The requested operation is not implemented by this server")]
    NotImplemented,
    /// An ACL was supplied for a bucket whose Object Ownership disables ACLs.
    #[error("The bucket does not allow ACLs")]
    AclNotSupported,
    /// A CompleteMultipartUpload named a part that was never uploaded, or whose ETag did not match.
    #[error("invalid part")]
    InvalidPart,
    /// A CompleteMultipartUpload listed its parts out of ascending part-number order.
    #[error("invalid part order")]
    InvalidPartOrder,
    /// A non-final multipart part was smaller than the 5 MiB minimum.
    #[error("entity too small")]
    EntityTooSmall,
    /// An unexpected internal failure.
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<InvalidName> for Error {
    fn from(_: InvalidName) -> Self {
        Error::InvalidArgument("invalid bucket name or object key".to_owned())
    }
}

impl From<CryptoError> for Error {
    fn from(e: CryptoError) -> Self {
        Error::Internal(format!("crypto: {e}"))
    }
}

impl From<ConfigError> for Error {
    fn from(e: ConfigError) -> Self {
        Error::InvalidArgument(e.to_string())
    }
}

impl From<BlobError> for Error {
    fn from(e: BlobError) -> Self {
        match e {
            BlobError::OutOfSpace => Error::InsufficientStorage,
            BlobError::SizeExceeded => Error::EntityTooLarge,
            BlobError::NotFound => Error::NoSuchKey,
            BlobError::Body(BodyError::Truncated) => {
                Error::InvalidArgument("client body ended prematurely".to_owned())
            }
            other => Error::Internal(other.to_string()),
        }
    }
}

impl From<MetaError> for Error {
    fn from(e: MetaError) -> Self {
        match e {
            // NOTE: `MetaError::Conflict` is intentionally NOT mapped to a specific S3 error here. It
            // is a generic uniqueness/constraint violation (`engine_err` folds every SQLite constraint
            // into it), and only bucket creation should surface it as `BucketAlreadyExists` — the two
            // bucket-create paths (cairn-protocol `create_bucket`, cairn-control) intercept `Conflict`
            // explicitly BEFORE this `From` runs. Any other `Conflict` reaching here is an unexpected
            // constraint surprise, so it becomes an opaque `Internal` (5xx) rather than a misleading
            // 409 `BucketAlreadyExists`; a new user-collidable constraint must be mapped at its own
            // call site.
            MetaError::PreconditionFailed => Error::PreconditionFailed,
            MetaError::QuotaExceeded => Error::InsufficientStorage,
            MetaError::NotEmpty => Error::BucketNotEmpty,
            other => Error::Internal(other.to_string()),
        }
    }
}

impl From<AuthError> for Error {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::Malformed => Error::InvalidArgument("malformed credentials".to_owned()),
            AuthError::UnknownKey => Error::InvalidAccessKeyId,
            AuthError::SignatureMismatch | AuthError::ChunkSignatureMismatch => {
                Error::SignatureDoesNotMatch
            }
            AuthError::SkewedClock => Error::RequestTimeTooSkewed,
            AuthError::Expired => Error::AccessDenied,
        }
    }
}

/// The crate-wide result alias over the canonical [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_is_not_bucket_already_exists() {
        // M3: a stray metadata Conflict must surface as an opaque Internal (5xx), not a misleading
        // 409 BucketAlreadyExists. Only the bucket-create call sites map Conflict, before this From.
        assert!(matches!(
            Error::from(MetaError::Conflict),
            Error::Internal(_)
        ));
    }

    #[test]
    fn skewed_clock_is_its_own_variant() {
        // A stale/future signed date is a distinct S3 condition (403 RequestTimeTooSkewed), not a
        // generic 400 InvalidArgument: SDKs resync their clock and retry off that code alone.
        assert!(matches!(
            Error::from(AuthError::SkewedClock),
            Error::RequestTimeTooSkewed
        ));
    }

    #[test]
    fn not_implemented_message_is_client_facing() {
        // The 501 message is rendered verbatim to the client, so it must read as a permanent
        // capability answer and must never look like a retryable internal fault. Asserting the
        // DESCRIPTIVE shape, not merely the substring "not implemented" — the terse pre-fix
        // message satisfied that and this test would have passed without the fix.
        let m = Error::NotImplemented.to_string();
        assert!(m.contains("not implemented"), "{m}");
        assert!(!m.to_lowercase().contains("internal error"), "{m}");
        assert!(
            m.contains("requested operation") && m.contains("server"),
            "the 501 body must name what was refused and by whom, got: {m}"
        );
    }
}
