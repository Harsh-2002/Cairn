//! Authorization model types: S3 actions, resources, the bucket-policy language, ACLs,
//! Block Public Access, Object Ownership, and the pure evaluation input/output. The engine
//! that consumes these lives in `cairn-authz`; keeping the types here lets the protocol
//! layer assemble inputs without depending on the engine implementation.

use crate::id::{BucketName, ObjectKey, UserId, VersionId};
use crate::time::Timestamp;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// An S3 action that a policy statement can allow or deny, and that each operation maps to.
/// `as_s3_name`/`from_s3_name` bridge to the `s3:*` namespace used in policy documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Action {
    // service / bucket existence
    ListAllMyBuckets,
    CreateBucket,
    DeleteBucket,
    GetBucketLocation,
    // bucket listing
    ListBucket,
    ListBucketVersions,
    ListBucketMultipartUploads,
    // object data
    GetObject,
    GetObjectVersion,
    PutObject,
    DeleteObject,
    DeleteObjectVersion,
    // object subresources
    GetObjectAcl,
    PutObjectAcl,
    GetObjectTagging,
    PutObjectTagging,
    DeleteObjectTagging,
    GetObjectAttributes,
    // object lock / retention (WORM)
    GetObjectRetention,
    PutObjectRetention,
    GetObjectLegalHold,
    PutObjectLegalHold,
    /// Permits shortening or removing a `GOVERNANCE`-mode retention (or deleting a version still
    /// under it) when paired with the `x-amz-bypass-governance-retention: true` header.
    BypassGovernanceRetention,
    // multipart
    AbortMultipartUpload,
    ListMultipartUploadParts,
    // bucket config (read)
    GetBucketPolicy,
    GetBucketAcl,
    GetBucketCors,
    GetBucketVersioning,
    GetBucketTagging,
    GetLifecycleConfiguration,
    GetReplicationConfiguration,
    GetBucketOwnershipControls,
    GetBucketPublicAccessBlock,
    GetBucketObjectLockConfiguration,
    // bucket config (write)
    PutBucketPolicy,
    PutBucketAcl,
    PutBucketCors,
    PutBucketVersioning,
    PutBucketTagging,
    PutLifecycleConfiguration,
    PutReplicationConfiguration,
    PutBucketOwnershipControls,
    PutBucketPublicAccessBlock,
    PutBucketObjectLockConfiguration,
    // replication (cross-bucket propagation)
    ReplicateObject,
    ReplicateDelete,
}

impl Action {
    /// The `s3:Xxx` name used in policy documents.
    #[must_use]
    pub fn as_s3_name(self) -> &'static str {
        use Action::*;
        match self {
            ListAllMyBuckets => "s3:ListAllMyBuckets",
            CreateBucket => "s3:CreateBucket",
            DeleteBucket => "s3:DeleteBucket",
            GetBucketLocation => "s3:GetBucketLocation",
            ListBucket => "s3:ListBucket",
            ListBucketVersions => "s3:ListBucketVersions",
            ListBucketMultipartUploads => "s3:ListBucketMultipartUploads",
            GetObject => "s3:GetObject",
            GetObjectVersion => "s3:GetObjectVersion",
            PutObject => "s3:PutObject",
            DeleteObject => "s3:DeleteObject",
            DeleteObjectVersion => "s3:DeleteObjectVersion",
            GetObjectAcl => "s3:GetObjectAcl",
            PutObjectAcl => "s3:PutObjectAcl",
            GetObjectTagging => "s3:GetObjectTagging",
            PutObjectTagging => "s3:PutObjectTagging",
            DeleteObjectTagging => "s3:DeleteObjectTagging",
            GetObjectAttributes => "s3:GetObjectAttributes",
            GetObjectRetention => "s3:GetObjectRetention",
            PutObjectRetention => "s3:PutObjectRetention",
            GetObjectLegalHold => "s3:GetObjectLegalHold",
            PutObjectLegalHold => "s3:PutObjectLegalHold",
            BypassGovernanceRetention => "s3:BypassGovernanceRetention",
            AbortMultipartUpload => "s3:AbortMultipartUpload",
            ListMultipartUploadParts => "s3:ListMultipartUploadParts",
            GetBucketPolicy => "s3:GetBucketPolicy",
            GetBucketAcl => "s3:GetBucketAcl",
            GetBucketCors => "s3:GetBucketCORS",
            GetBucketVersioning => "s3:GetBucketVersioning",
            GetBucketTagging => "s3:GetBucketTagging",
            GetLifecycleConfiguration => "s3:GetLifecycleConfiguration",
            GetReplicationConfiguration => "s3:GetReplicationConfiguration",
            GetBucketOwnershipControls => "s3:GetBucketOwnershipControls",
            GetBucketPublicAccessBlock => "s3:GetBucketPublicAccessBlock",
            GetBucketObjectLockConfiguration => "s3:GetBucketObjectLockConfiguration",
            PutBucketPolicy => "s3:PutBucketPolicy",
            PutBucketAcl => "s3:PutBucketAcl",
            PutBucketCors => "s3:PutBucketCORS",
            PutBucketVersioning => "s3:PutBucketVersioning",
            PutBucketTagging => "s3:PutBucketTagging",
            PutLifecycleConfiguration => "s3:PutLifecycleConfiguration",
            PutReplicationConfiguration => "s3:PutReplicationConfiguration",
            PutBucketOwnershipControls => "s3:PutBucketOwnershipControls",
            PutBucketPublicAccessBlock => "s3:PutBucketPublicAccessBlock",
            PutBucketObjectLockConfiguration => "s3:PutBucketObjectLockConfiguration",
            ReplicateObject => "s3:ReplicateObject",
            ReplicateDelete => "s3:ReplicateDelete",
        }
    }
}

/// A resource an action targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resource {
    /// A bucket-level resource.
    Bucket(BucketName),
    /// An object-level resource.
    Object {
        /// The bucket.
        bucket: BucketName,
        /// The object key.
        key: ObjectKey,
    },
}

/// A policy statement effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    /// Grants the matched request.
    Allow,
    /// Denies the matched request (overrides any allow).
    Deny,
}

/// A bucket policy: a version marker, an optional id, and a list of statements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    /// The policy language version marker (e.g. `2012-10-17`).
    pub version: String,
    /// An optional policy id.
    #[serde(default)]
    pub id: Option<String>,
    /// The statements, evaluated together.
    pub statements: Vec<Statement>,
}

/// One policy statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Statement {
    /// An optional statement id.
    #[serde(default)]
    pub sid: Option<String>,
    /// Allow or deny.
    pub effect: Effect,
    /// Principals the statement applies to (a positive `Principal` or a negated `NotPrincipal`).
    pub principals: PrincipalSpec,
    /// Actions the statement governs: a positive `Action` set or a negated `NotAction` set.
    pub actions: ActionMatch,
    /// Resources the statement scopes to: a positive `Resource` set or a negated `NotResource` set.
    pub resources: ResourceMatch,
    /// Conditions that must all hold for the statement to match.
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// A statement's action clause: the positive `Action` list, or the negated `NotAction` list.
///
/// `NotAction` matches every action *except* those listed — the IAM/S3 negated form (15.5). A
/// statement carries exactly one of the two (the parser rejects both-present and neither-present),
/// so the type makes the invalid "both at once" state unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionMatch {
    /// `Action`: the request action must match one of these patterns.
    In(Vec<ActionPattern>),
    /// `NotAction`: the request action must match *none* of these patterns.
    NotIn(Vec<ActionPattern>),
}

/// A statement's resource clause: the positive `Resource` list, or the negated `NotResource` list.
///
/// `NotResource` matches every resource *except* those listed (15.5). As with [`ActionMatch`], a
/// statement carries exactly one of the two forms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceMatch {
    /// `Resource`: the request resource must match one of these ARN patterns.
    In(Vec<String>),
    /// `NotResource`: the request resource must match *none* of these ARN patterns.
    NotIn(Vec<String>),
}

/// Whom a statement applies to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrincipalSpec {
    /// `Principal: "*"` — the wildcard: anyone, including anonymous.
    Any,
    /// `Principal: {"AWS": ...}` — a specific set of Cairn users.
    Users(Vec<UserId>),
    /// `NotPrincipal: {"AWS": ...}` — everyone *except* the listed users (15.5). A powerful,
    /// rarely-needed negated form; in an `Allow` it grants broadly and is therefore treated as a
    /// public grant for Block Public Access purposes.
    NotUsers(Vec<UserId>),
}

/// An action pattern in a policy: exact, the `*` wildcard, or a `prefix*` form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionPattern {
    /// Matches every action.
    All,
    /// Matches actions whose `s3:` name starts with this prefix (the part before `*`).
    Prefix(String),
    /// Matches one exact action name.
    Exact(String),
}

/// A condition: an operator applied to a key and a set of expected values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Condition {
    /// The comparison operator.
    pub operator: ConditionOperator,
    /// The condition key being matched.
    pub key: String,
    /// The expected values (any-match semantics within a key).
    pub values: Vec<String>,
    /// Whether the `IfExists` qualifier applies (pass when the key is absent).
    #[serde(default)]
    pub if_exists: bool,
}

/// Supported condition operators (the families from ARCH 15.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionOperator {
    /// `StringEquals`
    StringEquals,
    /// `StringNotEquals`
    StringNotEquals,
    /// `StringLike` (wildcard match)
    StringLike,
    /// `Bool`
    Bool,
    /// `IpAddress`
    IpAddress,
    /// `NotIpAddress`
    NotIpAddress,
    /// `NumericEquals` / `NumericLessThan` / ... encoded with a comparator
    Numeric(NumericOp),
    /// `DateGreaterThan` / `DateLessThan` / ...
    Date(NumericOp),
    /// `Null` (existence) — values are `"true"`/`"false"`.
    Null,
}

/// A numeric/date comparator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NumericOp {
    /// `==`
    Equals,
    /// `!=`
    NotEquals,
    /// `<`
    LessThan,
    /// `<=`
    LessThanEquals,
    /// `>`
    GreaterThan,
    /// `>=`
    GreaterThanEquals,
}

/// An access-control list: an owner and a set of grants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acl {
    /// The resource owner.
    pub owner: UserId,
    /// The grants.
    pub grants: Vec<Grant>,
}

/// One ACL grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    /// Who is granted.
    pub grantee: Grantee,
    /// What permission.
    pub permission: Permission,
}

/// An ACL grantee.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Grantee {
    /// A specific user.
    User(UserId),
    /// The all-users group (anonymous and any requester).
    AllUsers,
    /// The authenticated-users group.
    AuthenticatedUsers,
    /// The log-delivery group.
    LogDelivery,
}

/// An ACL permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Permission {
    /// Full control (implies all others).
    FullControl,
    /// Read object data / list bucket.
    Read,
    /// Write/overwrite/delete objects (bucket-level).
    Write,
    /// Read the ACL.
    ReadAcp,
    /// Write the ACL.
    WriteAcp,
}

/// The Object Ownership mode governing whether ACLs participate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OwnershipMode {
    /// ACLs disabled; the bucket owner owns every object (the recommended default).
    BucketOwnerEnforced,
    /// ACLs in force; objects written by others are owned by the bucket owner.
    BucketOwnerPreferred,
    /// ACLs in force; objects are owned by their writer.
    ObjectWriter,
}

/// Block Public Access settings (four independent toggles), at account or bucket level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PublicAccessBlock {
    /// Reject `PutBucketAcl`/`PutObjectAcl` that grant public access.
    pub block_public_acls: bool,
    /// Ignore public ACL grants when evaluating access.
    pub ignore_public_acls: bool,
    /// Reject bucket policies that grant public access.
    pub block_public_policy: bool,
    /// Restrict access to buckets with public policies to authorized principals only.
    pub restrict_public_buckets: bool,
}

/// The condition/request context the pipeline assembles for evaluation.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Source network address.
    pub source: IpAddr,
    /// Whether the transport was secure.
    pub secure_transport: bool,
    /// The `Referer` header, if present.
    pub referer: Option<String>,
    /// The `User-Agent` header, if present.
    pub user_agent: Option<String>,
    /// The current time (from the injected clock).
    pub now: Timestamp,
    /// Listing prefix, for list operations.
    pub prefix: Option<String>,
    /// Listing delimiter, for list operations.
    pub delimiter: Option<String>,
    /// Listing max-keys, for list operations.
    pub max_keys: Option<u32>,
    /// The canned-ACL header supplied on a write.
    pub canned_acl: Option<String>,
    /// The `x-amz-content-sha256` header value.
    pub content_sha256: Option<String>,
    /// The targeted version id, if any.
    pub version_id: Option<VersionId>,
    /// The existing object's tags (for object actions).
    pub existing_tags: Vec<(String, String)>,
    /// The tags supplied on the request.
    pub request_tags: Vec<(String, String)>,
}

/// The full input to the authorization engine (a pure function over these).
#[derive(Debug, Clone)]
pub struct AuthzInput {
    /// The requester class.
    pub requester: RequesterClass,
    /// The action requested.
    pub action: Action,
    /// The resource targeted.
    pub resource: Resource,
    /// The bucket owner.
    pub bucket_owner: UserId,
    /// Account-wide Block Public Access.
    pub account_bpa: PublicAccessBlock,
    /// Bucket-level Block Public Access.
    pub bucket_bpa: PublicAccessBlock,
    /// The bucket policy, if any.
    pub policy: Option<Policy>,
    /// The requester's attached identity (per-user) policy, if any (ARCH 15 / user-centric authz).
    /// Evaluated in union with the bucket policy; its statements have no principal (the requester is
    /// implicitly the principal), and its grants are never public, so they survive Block Public
    /// Access. `None` for anonymous requesters and users without an attached policy.
    pub user_policy: Option<Policy>,
    /// The bucket ACL.
    pub bucket_acl: Option<Acl>,
    /// The object ACL (for object actions).
    pub object_acl: Option<Acl>,
    /// The bucket's ownership mode.
    pub ownership_mode: OwnershipMode,
    /// The condition/request context.
    pub context: RequestContext,
}

/// The authorization decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The request is allowed.
    Allow,
    /// The request is denied, with a reason for audit/debugging.
    Deny(DenyReason),
}

/// Why a request was denied (also which stage of the fixed evaluation order denied it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenyReason {
    /// An explicit `Deny` statement matched.
    ExplicitPolicyDeny,
    /// Block Public Access suppressed the only grant that would have allowed it.
    BlockPublicAccess,
    /// Nothing granted the request (default deny).
    DefaultDeny,
    /// Setting an ACL while ownership disables ACLs.
    AclsDisabled,
}

use crate::auth::RequesterClass;
