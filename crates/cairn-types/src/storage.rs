//! Durable physical-write ownership (storage protocol 2, ARCH 8). Plans contain names, never
//! file handles. Only an acknowledged admission can turn a fresh plan into a creation permit.

use crate::authz::Acl;
use crate::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use crate::object::{
    ChecksumSet, ChecksumValue, CompressionDescriptor, ETag, ExplicitObjectLockIntent,
    MultipartPartRecord, ObjectVersionRow, StorageClass, UserMetadata,
};
use crate::time::Timestamp;
use crate::{BlobError, MetaError};
use serde::{Deserialize, Serialize};

pub mod io;

/// Fresh opaque identity for a generation, write attempt or cleanup claim.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct StorageToken(String);

impl StorageToken {
    /// Mint an identity; identities are never deliberately reused.
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }

    /// Canonical database representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for StorageToken {
    type Error = MetaError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if uuid_name(&value) {
            Ok(Self(value))
        } else {
            Err(invalid("invalid storage ownership token"))
        }
    }
}

impl From<StorageToken> for String {
    fn from(value: StorageToken) -> Self {
        value.0
    }
}

fn uuid_name(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn upload_name(value: &str) -> bool {
    if let Some((shard, id)) = value.split_once('~') {
        !shard.is_empty()
            && shard.len() <= 5
            && shard.bytes().all(|b| b.is_ascii_digit())
            && uuid_name(id)
    } else {
        uuid_name(value)
    }
}

/// The durable per-object metadata sidecar path for one canonical object data path
/// (`<bucket>/<uuid>` or `<bucket>/<2hex>/<uuid>`), or `None` for any other path.
///
/// Protocol 3 writes the sidecar beside its data file; protocol 2 and legacy objects have none.
#[must_use]
pub fn object_metadata_path(data: &StoragePath) -> Option<StoragePath> {
    let (directory, name) = data.as_str().rsplit_once('/')?;
    if !uuid_name(name) {
        return None;
    }
    Some(StoragePath::from_string(format!("{directory}/{name}.meta")))
}

/// The data path a canonical object metadata sidecar belongs to, or `None` for other names.
#[must_use]
pub fn object_data_path_for_metadata(metadata: &StoragePath) -> Option<StoragePath> {
    let (directory, name) = metadata.as_str().rsplit_once('/')?;
    let id = name.strip_suffix(".meta")?;
    if !uuid_name(id) {
        return None;
    }
    Some(StoragePath::from_string(format!("{directory}/{id}")))
}

/// The path whose liveness owns `path`. A protocol-3 sidecar alias — the final `<data>.meta` or a
/// staging `<attempt>.meta.tmp` — is live exactly when its data alias is; every other name owns
/// itself. Reconciliation and recovery ask the metadata oracle about this owner.
#[must_use]
pub fn storage_alias_owner(path: &StoragePath) -> StoragePath {
    if let Some(data) = object_data_path_for_metadata(path) {
        return data;
    }
    if let Some((directory, name)) = path.as_str().rsplit_once('/')
        && directory == ".staging"
        && let Some(attempt) = name.strip_suffix(".meta.tmp")
        && uuid_name(attempt)
    {
        return StoragePath::from_string(format!(".staging/{attempt}.tmp"));
    }
    path.clone()
}

fn object_name(value: &str) -> bool {
    uuid_name(value) || value.strip_suffix(".meta").is_some_and(uuid_name)
}

/// Closed grammar for exact cleanup, including legacy flat/nested objects, protocol-3 object
/// metadata sidecars and multipart parts. Unknown shapes remain for explicit reconciliation; the
/// blob backend also rejects symlinks.
pub fn validate_storage_path(bucket: &BucketName, path: &StoragePath) -> Result<(), MetaError> {
    BucketName::parse(bucket.as_str()).map_err(|_| invalid("invalid storage routing bucket"))?;
    if path.as_str().len() > 256 {
        return Err(invalid("unsupported exact storage path"));
    }
    let components: Vec<_> = path.as_str().split('/').collect();
    let valid = match components.as_slice() {
        [directory, name] if *directory == bucket.as_str() => object_name(name),
        [directory, prefix, name] if *directory == bucket.as_str() => {
            object_name(name) && prefix.len() == 2 && name.starts_with(prefix)
        }
        [".staging", name] => name
            .strip_suffix(".index.tmp")
            .or_else(|| name.strip_suffix(".meta.tmp"))
            .or_else(|| name.strip_suffix(".tmp"))
            .is_some_and(uuid_name),
        [".staging", "multipart", upload, name] if upload_name(upload) => {
            let (part, attempt) = name
                .split_once('-')
                .map_or((*name, None), |(part, attempt)| (part, Some(attempt)));
            part.len() == 5
                && part.bytes().all(|b| b.is_ascii_digit())
                && part
                    .parse::<u16>()
                    .is_ok_and(|part| (1..=10_000).contains(&part))
                && attempt.is_none_or(|id| {
                    !id.is_empty()
                        && id.len() <= 64
                        && id
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
                })
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid("unsupported exact storage path"))
    }
}

/// Exact publication target. The routing bucket is retained separately even after deletion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum StorageWriteTarget {
    /// Ordinary PUT, Copy, import or replication ingest.
    Object {
        key: ObjectKey,
        version_id: VersionId,
        row_id: String,
    },
    /// One quota-reserved multipart attempt.
    Part {
        upload_id: UploadId,
        part_number: u16,
        /// Existing v26 quota reservation identity; the physical part name retains this token.
        reservation_id: String,
    },
    /// Assembly owned by one exact multipart completion claim.
    Completion {
        upload_id: UploadId,
        claim_token: String,
        key: ObjectKey,
        version_id: VersionId,
        row_id: String,
    },
}

/// Purpose of a bounded intent path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoragePathRole {
    Temporary,
    Final,
    IndexSpool,
    /// Durable protocol-3 per-object metadata sidecar (`<final>.meta`).
    ObjectMetadata,
}

impl StoragePathRole {
    /// The protocol-2 journal name, or `None` for a protocol-3-only role that has no schema row.
    #[must_use]
    pub fn journal_name(self) -> Option<&'static str> {
        match self {
            Self::Temporary => Some("temporary"),
            Self::Final => Some("final"),
            Self::IndexSpool => Some("index_spool"),
            Self::ObjectMetadata => None,
        }
    }
}

/// One exact relative filename. Syntax validation does not replace backend no-follow traversal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageIntentPath {
    pub role: StoragePathRole,
    pub path: StoragePath,
}

impl StorageIntentPath {
    /// Protocol-3 sidecar aliases are derived exactly from the target and are intentionally not
    /// journaled: schema v39's role check predates them, and the deterministic name already binds
    /// them to the admitted object attempt.
    #[must_use]
    pub fn is_object_metadata_sidecar(&self) -> bool {
        match self.role {
            StoragePathRole::ObjectMetadata => true,
            StoragePathRole::Temporary => self
                .path
                .as_str()
                .rsplit_once('/')
                .is_some_and(|(_, name)| name.ends_with(".meta.tmp")),
            StoragePathRole::Final | StoragePathRole::IndexSpool => false,
        }
    }
}

/// Serializable admission request. The Writer revalidates its bounded, canonical names.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageWritePlan {
    pub attempt: StorageToken,
    pub generation: StorageToken,
    pub bucket: BucketName,
    /// Owner recorded from the bucket snapshot the request was authorized against. Admission or
    /// publication refuses a plan whose name now resolves to a different owner (a delete/recreate
    /// race across tenants). `None` for recovery plans, where the admission already exists.
    #[serde(default)]
    pub bucket_owner: Option<UserId>,
    /// Bucket incarnation the request was authorized against. A same-name delete/recreate by the
    /// same owner is still a different bucket, so an ordinary object publication compares this
    /// value as well as the owner. `None` for plans recovered from an older journal, where the
    /// comparison is skipped.
    #[serde(default)]
    pub bucket_created_at: Option<Timestamp>,
    pub target: StorageWriteTarget,
    pub paths: Vec<StorageIntentPath>,
}

impl StorageWritePlan {
    /// Refuse aliases, wrong routing and unplanned paths before recording admission.
    pub fn validate(&self) -> Result<(), MetaError> {
        BucketName::parse(self.bucket.as_str()).map_err(|_| invalid("invalid storage bucket"))?;
        let expected = self.expected_paths()?;
        // A protocol-2 object intent persisted before sidecars existed remains recoverable. New
        // plans always carry the sidecar aliases; `PlannedStorageWrite::new` cannot mint this shape.
        let legacy = self.legacy_object_paths();
        if self.paths != expected && legacy.as_ref() != Some(&self.paths) {
            return Err(invalid("storage plan paths do not match its exact target"));
        }
        for path in &self.paths {
            validate_storage_path(&self.bucket, &path.path)?;
        }
        Ok(())
    }

    /// Paths recorded in the protocol-2 intent journal. Protocol-3 sidecars are exact functions of
    /// the object attempt, so recovery derives them from the plan instead of adding schema rows.
    pub fn journaled_paths(&self) -> impl Iterator<Item = &StorageIntentPath> {
        self.paths
            .iter()
            .filter(|path| !path.is_object_metadata_sidecar())
    }

    /// Restrict joint admission to the exact quota reservation or completion owner.
    pub fn validate_admission(&self, operation: &crate::meta::Mutation) -> Result<(), MetaError> {
        use crate::meta::Mutation;
        self.validate()?;
        let valid = match (&self.target, operation) {
            (
                StorageWriteTarget::Part {
                    upload_id,
                    part_number,
                    reservation_id,
                },
                Mutation::ReserveMultipartPart {
                    upload_id: actual_upload,
                    part_number: actual_part,
                    attempt_id,
                    ..
                },
            ) => {
                upload_id == actual_upload
                    && part_number == actual_part
                    && reservation_id == attempt_id
            }
            (
                StorageWriteTarget::Completion {
                    upload_id,
                    claim_token,
                    ..
                },
                Mutation::ClaimMultipart {
                    upload_id: actual_upload,
                    claim_token: actual_token,
                },
            ) => upload_id == actual_upload && claim_token == actual_token.as_str(),
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(invalid("storage admission target mismatch"))
        }
    }

    /// The durable blob result and metadata identity must match the admitted target exactly.
    pub fn validate_publication(&self, operation: &crate::meta::Mutation) -> Result<(), MetaError> {
        use crate::meta::Mutation;
        let final_path = self.final_path()?;
        let matches_row = |row: &crate::object::ObjectVersionRow,
                           key: &ObjectKey,
                           version_id: &VersionId,
                           row_id: &str| {
            row.bucket == self.bucket
                && &row.key == key
                && &row.version_id == version_id
                && row.id == row_id
                && !row.is_delete_marker
                && row.storage_path.as_ref() == Some(final_path)
        };
        let valid = match (&self.target, operation) {
            (
                StorageWriteTarget::Object {
                    key,
                    version_id,
                    row_id,
                },
                Mutation::PutObjectVersion { row, .. },
            ) => matches_row(row, key, version_id, row_id),
            (
                StorageWriteTarget::Part {
                    upload_id,
                    part_number,
                    reservation_id,
                },
                Mutation::RecordPart {
                    upload_id: actual_upload,
                    attempt_id,
                    part,
                },
            ) => {
                upload_id == actual_upload
                    && reservation_id == attempt_id
                    && *part_number == part.part_number
                    && &part.storage_path == final_path
            }
            (
                StorageWriteTarget::Completion {
                    upload_id,
                    claim_token,
                    key,
                    version_id,
                    row_id,
                },
                Mutation::CompleteMultipart {
                    upload_id: actual_upload,
                    claim_token: actual_token,
                    row,
                    ..
                },
            ) => {
                upload_id == actual_upload
                    && claim_token == actual_token.as_str()
                    && matches_row(row, key, version_id, row_id)
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(invalid("storage publication target mismatch"))
        }
    }

    /// Publication of an ordinary object write that was never journaled: only an exact object
    /// target and its [`crate::meta::Mutation::PutObjectVersion`] are accepted, and the plan must
    /// carry the authorized bucket snapshot the publication savepoint re-verifies.
    pub fn validate_object_publication(
        &self,
        operation: &crate::meta::Mutation,
    ) -> Result<(), MetaError> {
        if !matches!(self.target, StorageWriteTarget::Object { .. }) {
            return Err(invalid(
                "unadmitted storage publication requires an object target",
            ));
        }
        if self.bucket_owner.is_none() || self.bucket_created_at.is_none() {
            return Err(invalid(
                "unadmitted storage publication requires a bucket snapshot",
            ));
        }
        self.validate_publication(operation)
    }

    fn expected_paths(&self) -> Result<Vec<StorageIntentPath>, MetaError> {
        let mut paths = Vec::with_capacity(5);
        let id = self.attempt.as_str();
        let object = matches!(
            self.target,
            StorageWriteTarget::Object { .. } | StorageWriteTarget::Completion { .. }
        );
        let final_path = match &self.target {
            StorageWriteTarget::Part {
                upload_id,
                part_number,
                reservation_id,
            } => {
                if !upload_name(upload_id.as_str())
                    || !(1..=10_000).contains(part_number)
                    || reservation_id.is_empty()
                    || reservation_id.len() > 64
                    || !reservation_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
                {
                    return Err(invalid("invalid storage part target"));
                }
                format!(".staging/multipart/{upload_id}/{part_number:05}-{reservation_id}")
            }
            StorageWriteTarget::Object { key, row_id, .. }
            | StorageWriteTarget::Completion { key, row_id, .. } => {
                ObjectKey::parse(key.as_str())
                    .map_err(|_| invalid("invalid storage object key"))?;
                if !uuid_name(row_id) {
                    return Err(invalid("invalid storage object row identity"));
                }
                if let StorageWriteTarget::Completion {
                    upload_id,
                    claim_token,
                    ..
                } = &self.target
                    && (!upload_name(upload_id.as_str()) || !uuid_name(claim_token))
                {
                    return Err(invalid("invalid storage completion target"));
                }
                paths.push(StorageIntentPath {
                    role: StoragePathRole::Temporary,
                    path: StoragePath::from_string(format!(".staging/{id}.tmp")),
                });
                if object {
                    paths.push(StorageIntentPath {
                        role: StoragePathRole::Temporary,
                        path: StoragePath::from_string(format!(".staging/{id}.meta.tmp")),
                    });
                }
                format!("{}/{id}", self.bucket)
            }
        };
        paths.push(StorageIntentPath {
            role: StoragePathRole::Final,
            path: StoragePath::from_string(final_path.clone()),
        });
        if object {
            paths.push(StorageIntentPath {
                role: StoragePathRole::ObjectMetadata,
                path: StoragePath::from_string(format!("{final_path}.meta")),
            });
        }
        // A small/plain write may never create this optional alias. Reserving it unconditionally
        // keeps the encoder's later spill decision inside the original bounded admission.
        paths.push(StorageIntentPath {
            role: StoragePathRole::IndexSpool,
            path: StoragePath::from_string(format!(".staging/{id}.index.tmp")),
        });
        Ok(paths)
    }

    /// The protocol-2 object path set, still accepted so pre-sidecar intents can be recovered.
    fn legacy_object_paths(&self) -> Option<Vec<StorageIntentPath>> {
        if !matches!(
            self.target,
            StorageWriteTarget::Object { .. } | StorageWriteTarget::Completion { .. }
        ) {
            return None;
        }
        let id = self.attempt.as_str();
        Some(vec![
            StorageIntentPath {
                role: StoragePathRole::Temporary,
                path: StoragePath::from_string(format!(".staging/{id}.tmp")),
            },
            StorageIntentPath {
                role: StoragePathRole::Final,
                path: StoragePath::from_string(format!("{}/{id}", self.bucket)),
            },
            StorageIntentPath {
                role: StoragePathRole::IndexSpool,
                path: StoragePath::from_string(format!(".staging/{id}.index.tmp")),
            },
        ])
    }

    /// Exact final filename after validation.
    pub fn final_path(&self) -> Result<&StoragePath, MetaError> {
        self.validate()?;
        self.paths
            .iter()
            .find(|path| path.role == StoragePathRole::Final)
            .map(|path| &path.path)
            .ok_or_else(|| invalid("missing storage final path"))
    }
}

/// Versioned, canonical per-object metadata sidecar format (protocol 3, P1 dual-write).
///
/// The sidecar sits beside the data file as `<final>.meta`. It is serialized JSON, bounded by
/// [`OBJECT_METADATA_MAX_BYTES`], and carries no secret plaintext: an SSE descriptor is the same
/// sealed record SQLite stores, never a raw data-encryption key.
///
/// v2 adds the optional completed-multipart part layout. v1 records remain readable; a v2 record
/// must carry the layout and a v1 record must not, so a silently downgraded record cannot lose it.
pub const OBJECT_METADATA_V2_FORMAT_VERSION: u32 = 2;

/// The latest record generation this binary understands.
pub const OBJECT_METADATA_FORMAT_VERSION: u32 = OBJECT_METADATA_V2_FORMAT_VERSION;

/// The original metadata record generation, still written for objects without a part layout.
pub const OBJECT_METADATA_V1_FORMAT_VERSION: u32 = 1;

/// Hard bound on one encoded metadata record.
///
/// The record lives in an unauthenticated file trailer that startup recovery reads into memory, so
/// its size must be bounded independently of the object it describes. One MiB is far above the S3
/// object limits this record mirrors (2 KiB user metadata, 10 tags, one ACL) while keeping a
/// corrupt length from turning restart into a large allocation.
pub const OBJECT_METADATA_MAX_BYTES: usize = 1024 * 1024;

/// One durable object metadata sidecar record. SQLite remains the ACK authority in P1; the record
/// is written with the data file so a later protocol-3 phase can rebuild a missing index row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectMetadataRecord {
    /// Format generation: v1, or v2 when `multipart_parts` is present.
    pub format_version: u32,
    /// The owning bucket.
    pub bucket: BucketName,
    /// The object key.
    pub key: ObjectKey,
    /// The version id.
    pub version_id: VersionId,
    /// The immutable object-version row identity.
    pub row_id: String,
    /// Whether this record describes a delete marker (always false for a staged object).
    pub is_delete_marker: bool,
    /// The plaintext length.
    pub size_logical: u64,
    /// The ordered part layout of a completed multipart object, absent for an ordinary object.
    ///
    /// This is what keeps `GET`/`HEAD ?partNumber=N` available after a rebuild from the data
    /// files alone. It is skipped when absent so an ordinary record stays byte-compatible with v1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_parts: Option<Vec<MultipartPartRecord>>,
    /// The on-disk length after compression/encryption framing.
    pub size_physical: u64,
    /// The entity tag.
    pub etag: ETag,
    /// The MIME content type.
    pub content_type: String,
    /// The stored `Content-Encoding`.
    pub content_encoding: Option<String>,
    /// The stored `Cache-Control`.
    pub cache_control: Option<String>,
    /// The stored `Content-Disposition`.
    pub content_disposition: Option<String>,
    /// The stored `Content-Language`.
    pub content_language: Option<String>,
    /// The stored `Expires`.
    pub expires: Option<String>,
    /// The absolute object-TTL deadline, when the extension is in use.
    pub expire_at: Option<Timestamp>,
    /// The opaque data path (`<bucket>/<uuid>`).
    pub storage_path: StoragePath,
    /// The physical compression descriptor.
    pub compression: CompressionDescriptor,
    /// The storage class.
    pub storage_class: StorageClass,
    /// The version owner.
    pub owner_id: UserId,
    /// User-defined metadata.
    pub user_metadata: UserMetadata,
    /// The object ACL when ownership keeps ACLs in force.
    pub acl: Option<Acl>,
    /// The supplementary checksums persisted on the row.
    pub checksums: Vec<ChecksumValue>,
    /// The sealed SSE descriptor, when the data is server-side encrypted.
    pub sse_descriptor: Option<String>,
    /// The internal plaintext SHA-256 baseline.
    pub internal_sha256: Option<String>,
    /// Creation time captured before staging.
    pub created_at: Timestamp,
    /// Last-update time captured before staging.
    pub updated_at: Timestamp,
    /// The initial object tag set.
    pub tags: Vec<(String, String)>,
    /// The explicit Object Lock intent; bucket defaults remain Writer-authoritative.
    pub lock_intent: ExplicitObjectLockIntent,
}

/// Fields known before the body is staged. The blob fills sizes, ETag, checksums, compression,
/// storage path and internal SHA-256 after the streaming pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMetadataSeed {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    /// Optional ETag override for an assembled multipart object, whose composite ETag is not the
    /// MD5 of the concatenated bytes that `assemble` hashes.
    pub etag: Option<ETag>,
    pub row_id: String,
    pub is_delete_marker: bool,
    /// The completed-multipart part layout, when the write is an assembly.
    pub multipart_parts: Option<Vec<MultipartPartRecord>>,
    pub content_type: String,
    pub content_encoding: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
    pub expire_at: Option<Timestamp>,
    pub storage_class: StorageClass,
    pub owner_id: UserId,
    pub user_metadata: UserMetadata,
    pub acl: Option<Acl>,
    /// Checksum algorithms that belong on the row. Verification-only digests are computed but not
    /// recorded, matching the protocol's post-stage filtering.
    pub stored_checksums: ChecksumSet,
    /// Exact checksums to record instead of selecting them from the staged blob. Multipart
    /// COMPOSITE values are composed from the part digests before assembly, so `assemble` never
    /// computes them and the sidecar must carry the completed value explicitly.
    pub checksums: Option<Vec<ChecksumValue>>,
    pub sse_descriptor: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub tags: Vec<(String, String)>,
    pub lock_intent: ExplicitObjectLockIntent,
}

/// Trailer magic: `payload || metadata JSON || u32 meta_len || u64 payload_len || magic`.
pub const INLINE_TRAILER_MAGIC: [u8; 4] = *b"CRNI";

/// Fixed trailer size after the metadata JSON (`meta_len`, `payload_len`, magic).
pub const INLINE_TRAILER_FIXED: usize = 4 + 8 + 4;

impl ObjectMetadataRecord {
    /// Encode the metadata JSON and its trailer for a file that already holds `payload_len` bytes.
    pub fn encode_inline_trailer(&self, payload_len: u64) -> Result<Vec<u8>, MetaError> {
        let json = self.encode()?;
        let meta_len = u32::try_from(json.len())
            .map_err(|_| MetaError::Engine("inline metadata exceeds 4 GiB".into()))?;
        let mut out = Vec::with_capacity(json.len() + INLINE_TRAILER_FIXED);
        out.extend_from_slice(&json);
        out.extend_from_slice(&meta_len.to_le_bytes());
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&INLINE_TRAILER_MAGIC);
        Ok(out)
    }

    /// Decode the metadata from a whole inline file (`payload || metadata || trailer`).
    pub fn decode_inline_file(bytes: &[u8]) -> Result<Option<(Self, u64)>, MetaError> {
        if bytes.len() < INLINE_TRAILER_FIXED {
            return Ok(None);
        }
        let fixed = &bytes[bytes.len() - INLINE_TRAILER_FIXED..];
        if fixed[12..16] != INLINE_TRAILER_MAGIC {
            return Ok(None);
        }
        let meta_len = u32::from_le_bytes(fixed[0..4].try_into().expect("four bytes")) as usize;
        let total = meta_len
            .checked_add(INLINE_TRAILER_FIXED)
            .filter(|total| *total <= bytes.len())
            .ok_or_else(|| MetaError::Engine("inline metadata length exceeds its file".into()))?;
        Self::decode_inline_suffix(&bytes[bytes.len() - total..])
    }

    /// Decode the metadata from the exact trailer suffix (`metadata JSON || fixed trailer`).
    pub fn decode_inline_suffix(suffix: &[u8]) -> Result<Option<(Self, u64)>, MetaError> {
        if suffix.len() < INLINE_TRAILER_FIXED {
            return Ok(None);
        }
        let fixed = &suffix[suffix.len() - INLINE_TRAILER_FIXED..];
        if fixed[12..16] != INLINE_TRAILER_MAGIC {
            return Ok(None);
        }
        let meta_len = u32::from_le_bytes(fixed[0..4].try_into().expect("four bytes")) as usize;
        let payload_len = u64::from_le_bytes(fixed[4..12].try_into().expect("eight bytes"));
        if suffix.len() != meta_len + INLINE_TRAILER_FIXED {
            return Err(MetaError::Engine("inline metadata length mismatch".into()));
        }
        // A trailer larger than the record bound was not written by this format; it is not our
        // metadata rather than a record to parse.
        if meta_len > OBJECT_METADATA_MAX_BYTES {
            return Ok(None);
        }
        let record = Self::decode(&suffix[..meta_len])?;
        Ok(Some((record, payload_len)))
    }
}

impl ObjectMetadataRecord {
    /// Bind the pre-stage fields to the exact staged blob.
    #[must_use]
    pub fn from_staged(seed: &ObjectMetadataSeed, staged: &crate::blob::StagedBlob) -> Self {
        // Only a record that actually carries a layout advertises v2, so v1 stays the common case
        // and an older binary can still read every ordinary object's sidecar.
        let format_version = if seed.multipart_parts.is_some() {
            OBJECT_METADATA_V2_FORMAT_VERSION
        } else {
            OBJECT_METADATA_V1_FORMAT_VERSION
        };
        Self {
            format_version,
            bucket: seed.bucket.clone(),
            key: seed.key.clone(),
            version_id: seed.version_id.clone(),
            row_id: seed.row_id.clone(),
            is_delete_marker: seed.is_delete_marker,
            size_logical: staged.size_logical,
            multipart_parts: seed.multipart_parts.clone(),
            size_physical: staged.size_physical,
            etag: seed.etag.clone().unwrap_or_else(|| staged.etag.clone()),
            content_type: seed.content_type.clone(),
            content_encoding: seed.content_encoding.clone(),
            cache_control: seed.cache_control.clone(),
            content_disposition: seed.content_disposition.clone(),
            content_language: seed.content_language.clone(),
            expires: seed.expires.clone(),
            expire_at: seed.expire_at,
            storage_path: staged.storage_path.clone(),
            compression: staged.compression.clone(),
            storage_class: seed.storage_class,
            owner_id: seed.owner_id.clone(),
            user_metadata: seed.user_metadata.clone(),
            acl: seed.acl.clone(),
            checksums: seed.checksums.clone().unwrap_or_else(|| {
                staged
                    .checksums
                    .iter()
                    .filter(|checksum| seed.stored_checksums.0.contains(&checksum.algorithm))
                    .cloned()
                    .collect()
            }),
            sse_descriptor: seed.sse_descriptor.clone(),
            internal_sha256: Some(staged.internal_sha256.clone()),
            created_at: seed.created_at,
            updated_at: seed.updated_at,
            tags: seed.tags.clone(),
            lock_intent: seed.lock_intent,
        }
    }

    /// Build the sidecar record for an object version that predates the sidecar.
    ///
    /// The row is SQLite truth at the moment of the call; callers must hold the node lock so no
    /// live writer can supersede the version while the record is staged. Tags and Object Lock live
    /// in side tables, so they are supplied separately. A version with a recorded part layout gets
    /// a v2 record; a legacy assembled object without one gets v1 and keeps the fail-closed
    /// part-number behavior.
    pub fn from_existing(
        row: &ObjectVersionRow,
        tags: Vec<(String, String)>,
        lock_intent: ExplicitObjectLockIntent,
    ) -> Result<Self, MetaError> {
        if row.is_delete_marker {
            return Err(invalid("a delete marker has no object metadata record"));
        }
        let storage_path = row
            .storage_path
            .clone()
            .ok_or_else(|| invalid("object version has no storage path"))?;
        let format_version = if row.multipart_parts.is_some() {
            OBJECT_METADATA_V2_FORMAT_VERSION
        } else {
            OBJECT_METADATA_V1_FORMAT_VERSION
        };
        Ok(Self {
            format_version,
            bucket: row.bucket.clone(),
            key: row.key.clone(),
            version_id: row.version_id.clone(),
            row_id: row.id.clone(),
            is_delete_marker: false,
            size_logical: row.size_logical,
            multipart_parts: row.multipart_parts.clone(),
            size_physical: row.size_physical,
            etag: row.etag.clone(),
            content_type: row.content_type.clone(),
            content_encoding: row.content_encoding.clone(),
            cache_control: row.cache_control.clone(),
            content_disposition: row.content_disposition.clone(),
            content_language: row.content_language.clone(),
            expires: row.expires.clone(),
            expire_at: row.expire_at,
            storage_path,
            compression: row.compression.clone(),
            storage_class: row.storage_class,
            owner_id: row.owner_id.clone(),
            user_metadata: row.user_metadata.clone(),
            acl: row.acl.clone(),
            checksums: row.checksums.clone(),
            sse_descriptor: row.sse_descriptor.clone(),
            internal_sha256: row.internal_sha256.clone(),
            created_at: row.created_at,
            updated_at: row.updated_at,
            tags,
            lock_intent,
        })
    }

    /// Whether both records name the same object version and its exact staged file.
    ///
    /// The offline v1 → v2 sidecar upgrade compares the durable record against the record rebuilt
    /// from the index row before it replaces anything. A mismatch means the two disagree about the
    /// object and the existing sidecar must be left untouched.
    #[must_use]
    pub fn identity_matches(&self, other: &Self) -> bool {
        self.row_id == other.row_id
            && self.bucket == other.bucket
            && self.key == other.key
            && self.version_id == other.version_id
            && self.storage_path == other.storage_path
            && self.etag == other.etag
            && self.size_logical == other.size_logical
            && self.size_physical == other.size_physical
    }

    /// Whether this record names exactly the object version in `row`.
    #[must_use]
    pub fn matches_row_identity(&self, row: &ObjectVersionRow) -> bool {
        self.row_id == row.id
            && self.bucket == row.bucket
            && self.key == row.key
            && self.version_id == row.version_id
            && row.storage_path.as_ref() == Some(&self.storage_path)
            && self.etag == row.etag
            && self.size_logical == row.size_logical
            && self.size_physical == row.size_physical
    }

    /// Canonical bytes for the sidecar file.
    pub fn encode(&self) -> Result<Vec<u8>, MetaError> {
        let bytes =
            serde_json::to_vec(self).map_err(|error| MetaError::Engine(error.to_string()))?;
        if bytes.len() > OBJECT_METADATA_MAX_BYTES {
            return Err(invalid("object metadata record exceeds its 1 MiB bound"));
        }
        Ok(bytes)
    }

    /// Parse one bounded record and reject an unsupported or inconsistent format generation.
    pub fn decode(bytes: &[u8]) -> Result<Self, MetaError> {
        if bytes.len() > OBJECT_METADATA_MAX_BYTES {
            return Err(invalid("object metadata record exceeds its 1 MiB bound"));
        }
        let record: Self =
            serde_json::from_slice(bytes).map_err(|error| MetaError::Engine(error.to_string()))?;
        match record.format_version {
            OBJECT_METADATA_V1_FORMAT_VERSION if record.multipart_parts.is_some() => Err(invalid(
                "object metadata v1 must not carry a multipart part layout",
            )),
            OBJECT_METADATA_V2_FORMAT_VERSION if record.multipart_parts.is_none() => Err(invalid(
                "object metadata v2 requires a multipart part layout",
            )),
            OBJECT_METADATA_V1_FORMAT_VERSION | OBJECT_METADATA_V2_FORMAT_VERSION => Ok(record),
            _ => Err(invalid("unsupported object metadata format version")),
        }
    }
}

/// File-free, move-only backend plan. Deserializing its DTO cannot recreate this capability.
#[derive(Debug)]
pub struct PlannedStorageWrite(StorageWritePlan);

impl PlannedStorageWrite {
    /// Bind the authorized bucket snapshot (owner and incarnation) to this plan. Ordinary object
    /// publication re-establishes both inside its own savepoint.
    #[must_use]
    pub fn with_bucket_snapshot(mut self, owner: UserId, created_at: Timestamp) -> Self {
        self.0.bucket_owner = Some(owner);
        self.0.bucket_created_at = Some(created_at);
        self
    }
}

/// Typed Writer acknowledgement. An engine constructs this only for a committed admission.
#[derive(Debug, PartialEq, Eq)]
pub enum StorageAdmission {
    Granted(Box<StorageWritePlan>),
    NotApplied,
}

/// Per-bucket journal transactions, routed using the bucket retained by the outer mutation.
#[derive(Clone, Debug)]
pub enum StorageMutation {
    /// Ordinary object admission. Multipart admission shares the existing reserve/claim savepoint.
    Reserve {
        plan: Box<StorageWritePlan>,
        now: Timestamp,
    },
    /// Fence publication without claiming that queued or executing I/O has stopped.
    Cancel {
        attempt: StorageToken,
        generation: StorageToken,
    },
    /// Resolve only after the matching backend has stopped. Live paths remain authoritative;
    /// every other planned alias becomes exact durable cleanup debt.
    Resolve { quiescence: io::StorageQuiescence },
    /// Resolve one prior-generation plan after exclusive restart has confirmed actual backend
    /// quiescence, including outstanding kernel I/O. A generation change alone is insufficient.
    ResolveRecovered {
        current_generation: StorageToken,
        quiescence: io::StorageQuiescence,
    },
    /// Retire only after successful unlink and namespace synchronization, matching this exact
    /// still-owned claim. A failed/expired claim leaves both physical debt and quota intact.
    FinishCleanup {
        cleanup: StorageCleanup,
        now: Timestamp,
    },
    /// Retire a bounded set of rows whose physical reclamation already succeeded, in one Writer
    /// submission. Every row must belong to the outer [`Mutation::Storage`] bucket and share the
    /// same storage generation; the applier rejects a mixed batch as a caller defect rather than
    /// silently discarding debt. Each row still passes the exact claim check of
    /// [`Self::FinishCleanup`] on its own, so a stale, re-claimed or expired row retires nothing
    /// while the remaining rows still settle. A batch therefore never overcounts and never
    /// refunds a quota charge twice.
    FinishCleanupBatch {
        cleanups: Vec<StorageCleanup>,
        now: Timestamp,
    },
}

/// Exact immutable physical cleanup work. No reference to a deletable bucket/session row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageCleanup {
    pub id: StorageToken,
    pub bucket: BucketName,
    pub path: StoragePath,
    pub quota_debt_id: Option<String>,
    pub claim_token: StorageToken,
    pub generation: StorageToken,
    pub lease_until: Timestamp,
}

impl PlannedStorageWrite {
    /// Select fresh flat-layout paths without touching the filesystem.
    pub fn new(
        bucket: BucketName,
        generation: StorageToken,
        target: StorageWriteTarget,
    ) -> Result<Self, MetaError> {
        let mut plan = StorageWritePlan {
            attempt: StorageToken::generate(),
            generation,
            bucket,
            bucket_owner: None,
            bucket_created_at: None,
            target,
            paths: Vec::new(),
        };
        plan.paths = plan.expected_paths()?;
        plan.validate()?;
        Ok(Self(plan))
    }

    /// Copyable metadata input; it carries no permission to create files.
    #[must_use]
    pub fn plan(&self) -> &StorageWritePlan {
        &self.0
    }

    /// Consume this unique plan only after the Writer acknowledges this exact admission.
    pub fn admit(
        self,
        receipt: StorageAdmission,
        lease: io::StorageIoLease,
    ) -> Result<StorageCreationPermit, BlobError> {
        if !matches!(&receipt, StorageAdmission::Granted(plan) if plan.as_ref() == &self.0)
            || !lease.owns(&self.0.attempt, &self.0.generation)
            || lease.is_cancelled()
        {
            return Err(BlobError::Io("storage admission ownership lost".to_owned()));
        }
        Ok(StorageCreationPermit {
            plan: self.0,
            lease,
        })
    }

    /// Consume an object plan as a file-free creation permit. This is deliberately **not** a
    /// Writer acknowledgement: the publication savepoint re-establishes the current generation,
    /// the authorized bucket incarnation and exact path availability before any row becomes
    /// visible (ARCH 8.2). Multipart writes still require [`Self::admit`].
    pub fn into_object_permit(
        self,
        lease: io::StorageIoLease,
    ) -> Result<ObjectCreationPermit, BlobError> {
        if !matches!(self.0.target, StorageWriteTarget::Object { .. })
            || !lease.owns(&self.0.attempt, &self.0.generation)
            || lease.is_cancelled()
        {
            return Err(BlobError::Io("object creation ownership lost".to_owned()));
        }
        Ok(ObjectCreationPermit {
            plan: self.0,
            lease,
        })
    }
}

/// Move-only permission consumed by one ordinary object staging operation. It carries a bounded,
/// canonical plan and live I/O ownership, but no Writer intent; publication is the durable
/// authority. A stronger, Writer-admitted permit converts into this one because admission implies
/// everything it promises.
#[derive(Debug)]
pub struct ObjectCreationPermit {
    plan: StorageWritePlan,
    lease: io::StorageIoLease,
}

impl ObjectCreationPermit {
    /// The bounded plan this request may create.
    #[must_use]
    pub fn plan(&self) -> &StorageWritePlan {
        &self.plan
    }

    /// Transfer ownership to backend handles; each detached operation must obtain its own lease.
    #[must_use]
    pub fn into_parts(self) -> (StorageWritePlan, io::StorageIoLease) {
        (self.plan, self.lease)
    }
}

impl From<StorageCreationPermit> for ObjectCreationPermit {
    fn from(permit: StorageCreationPermit) -> Self {
        Self {
            plan: permit.plan,
            lease: permit.lease,
        }
    }
}

/// Move-only permission consumed by a staging or assembly operation.
#[derive(Debug)]
pub struct StorageCreationPermit {
    plan: StorageWritePlan,
    lease: io::StorageIoLease,
}

impl StorageCreationPermit {
    /// Inspect the immutable admitted target before accepting the request stream.
    #[must_use]
    pub fn plan(&self) -> &StorageWritePlan {
        &self.plan
    }

    /// Transfer ownership to backend handles; each detached operation must obtain its own lease.
    #[must_use]
    pub fn into_parts(self) -> (StorageWritePlan, io::StorageIoLease) {
        (self.plan, self.lease)
    }
}

fn invalid(message: &str) -> MetaError {
    MetaError::Engine(message.to_owned())
}

/// Protocol-2 publication has no bare mutation escape hatch. Metadata-only marker rows do not
/// create physical references and remain valid without a file admission.
pub fn validate_unadmitted_mutation(operation: &crate::meta::Mutation) -> Result<(), MetaError> {
    use crate::meta::Mutation;
    let physical = match operation {
        Mutation::PutObjectVersion { row, .. } | Mutation::CompleteMultipart { row, .. } => {
            row.storage_path.is_some()
        }
        Mutation::RecordPart { .. } => true,
        _ => false,
    };
    if physical {
        Err(invalid("physical publication requires storage admission"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{ChecksumAlgorithm, ObjectLockMode, ObjectRetention};
    use std::sync::Arc;

    fn metadata_seed(parts: Option<Vec<MultipartPartRecord>>) -> ObjectMetadataSeed {
        ObjectMetadataSeed {
            bucket: BucketName::parse("bucket").unwrap(),
            key: ObjectKey::parse("key").unwrap(),
            version_id: VersionId::null(),
            etag: None,
            row_id: "row-1".to_owned(),
            is_delete_marker: false,
            multipart_parts: parts,
            content_type: "application/octet-stream".to_owned(),
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            expire_at: None,
            storage_class: StorageClass::Standard,
            owner_id: UserId("owner".to_owned()),
            user_metadata: Vec::new(),
            acl: None,
            stored_checksums: ChecksumSet::none(),
            checksums: None,
            sse_descriptor: None,
            created_at: Timestamp(1),
            updated_at: Timestamp(2),
            tags: Vec::new(),
            lock_intent: ExplicitObjectLockIntent::default(),
        }
    }

    fn object_row(
        parts: Option<Vec<MultipartPartRecord>>,
        is_delete_marker: bool,
        storage_path: Option<StoragePath>,
    ) -> ObjectVersionRow {
        ObjectVersionRow {
            id: "a".repeat(32),
            bucket: BucketName::parse("bucket").unwrap(),
            key: ObjectKey::parse("assembled.bin").unwrap(),
            version_id: VersionId::null(),
            is_latest: true,
            is_delete_marker,
            size_logical: 11,
            size_physical: 11,
            etag: ETag::from_string("0123456789abcdef0123456789abcdef-2".to_owned()),
            content_type: "application/octet-stream".to_owned(),
            content_encoding: None,
            cache_control: Some("max-age=60".to_owned()),
            content_disposition: None,
            content_language: None,
            expires: None,
            expire_at: Some(Timestamp(99)),
            storage_path,
            compression: CompressionDescriptor::Uncompressed,
            storage_class: StorageClass::Standard,
            cold_locator: None,
            owner_id: UserId("owner".to_owned()),
            user_metadata: vec![("colour".to_owned(), "blue".to_owned())],
            acl: None,
            checksums: vec![ChecksumValue {
                algorithm: ChecksumAlgorithm::Crc32,
                value: "AAAAAA==-2".to_owned(),
            }],
            sse_descriptor: Some("sealed".to_owned()),
            internal_sha256: Some("00".repeat(32)),
            replication_status: None,
            replicated_at: None,
            multipart_parts: parts,
            created_at: Timestamp(1),
            updated_at: Timestamp(2),
        }
    }

    #[test]
    fn an_existing_version_reconstructs_its_sidecar_record() {
        let path = StoragePath::generate(&BucketName::parse("bucket").unwrap());
        let parts = vec![
            MultipartPartRecord {
                part_number: 1,
                size: 5,
            },
            MultipartPartRecord {
                part_number: 3,
                size: 6,
            },
        ];
        let tags = vec![("team".to_owned(), "storage".to_owned())];
        let retention = ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until: Timestamp(100),
        };
        let intent = ExplicitObjectLockIntent {
            retention: Some(retention),
            legal_hold: Some(true),
        };
        let record = ObjectMetadataRecord::from_existing(
            &object_row(Some(parts.clone()), false, Some(path.clone())),
            tags.clone(),
            intent,
        )
        .unwrap();
        assert_eq!(record.format_version, OBJECT_METADATA_V2_FORMAT_VERSION);
        assert_eq!(record.multipart_parts.as_deref(), Some(parts.as_slice()));
        assert_eq!(record.storage_path, path);
        assert_eq!(record.etag.as_str(), "0123456789abcdef0123456789abcdef-2");
        assert_eq!(record.checksums.len(), 1);
        assert_eq!(record.internal_sha256, Some("00".repeat(32)));
        assert_eq!(record.tags, tags);
        assert_eq!(record.lock_intent, intent);
        assert_eq!(record.expire_at, Some(Timestamp(99)));
        assert_eq!(
            ObjectMetadataRecord::decode(&record.encode().unwrap()).unwrap(),
            record
        );

        // A legacy assembly without a recorded layout stays v1: the row is restorable, and
        // part-number reads fail closed rather than inventing offsets.
        let legacy = ObjectMetadataRecord::from_existing(
            &object_row(None, false, Some(path)),
            Vec::new(),
            ExplicitObjectLockIntent::default(),
        )
        .unwrap();
        assert_eq!(legacy.format_version, OBJECT_METADATA_V1_FORMAT_VERSION);
        assert!(legacy.multipart_parts.is_none());

        assert!(
            ObjectMetadataRecord::from_existing(
                &object_row(None, true, None),
                Vec::new(),
                ExplicitObjectLockIntent::default()
            )
            .is_err()
        );
        assert!(
            ObjectMetadataRecord::from_existing(
                &object_row(None, false, None),
                Vec::new(),
                ExplicitObjectLockIntent::default()
            )
            .is_err()
        );
    }

    #[test]
    fn a_sidecar_identity_must_agree_with_the_row() {
        let path = StoragePath::generate(&BucketName::parse("bucket").unwrap());
        let row = object_row(None, false, Some(path));
        let record = ObjectMetadataRecord::from_existing(
            &row,
            Vec::new(),
            ExplicitObjectLockIntent::default(),
        )
        .unwrap();
        assert!(record.matches_row_identity(&row));
        assert!(record.identity_matches(&record));

        fn assert_conflict(
            record: &ObjectMetadataRecord,
            row: &ObjectVersionRow,
            mutate: impl Fn(&mut ObjectMetadataRecord),
        ) {
            let mut changed = record.clone();
            mutate(&mut changed);
            assert!(!changed.matches_row_identity(row));
            assert!(!record.identity_matches(&changed));
        }

        // Every identity-bearing field can independently invalidate the record: the offline
        // upgrade must never overwrite a sidecar that disagrees with the index row.
        assert_conflict(&record, &row, |record| record.row_id = "b".repeat(32));
        assert_conflict(&record, &row, |record| {
            record.bucket = BucketName::parse("other").unwrap();
        });
        assert_conflict(&record, &row, |record| {
            record.key = ObjectKey::parse("other.bin").unwrap();
        });
        assert_conflict(&record, &row, |record| {
            record.version_id = VersionId::from_string("v1".to_owned());
        });
        assert_conflict(&record, &row, |record| {
            record.storage_path = StoragePath::generate(&record.bucket);
        });
        assert_conflict(&record, &row, |record| {
            record.etag = ETag::from_string("other".into());
        });
        assert_conflict(&record, &row, |record| record.size_logical += 1);
        assert_conflict(&record, &row, |record| record.size_physical += 1);
    }

    fn staged_blob() -> crate::blob::StagedBlob {
        let path = StoragePath::generate(&BucketName::parse("bucket").unwrap());
        crate::blob::StagedBlob {
            storage_path: path,
            size_logical: 11,
            size_physical: 11,
            etag: ETag::from_md5_hex("5eb63bbbe01eeed093cb22bb8f5acdc3".to_owned()),
            md5_hex: Some("5eb63bbbe01eeed093cb22bb8f5acdc3".to_owned()),
            internal_sha256: "00".repeat(32),
            checksums: Vec::new(),
            compression: CompressionDescriptor::Uncompressed,
            index_spool: crate::blob::IndexSpoolState::NeverCreated,
        }
    }

    #[test]
    fn v1_records_stay_plain_and_v2_records_carry_the_part_layout() {
        let ordinary = ObjectMetadataRecord::from_staged(&metadata_seed(None), &staged_blob());
        assert_eq!(ordinary.format_version, OBJECT_METADATA_V1_FORMAT_VERSION);
        let encoded = ordinary.encode().unwrap();
        assert!(!String::from_utf8_lossy(&encoded).contains("multipart_parts"));
        assert_eq!(ObjectMetadataRecord::decode(&encoded).unwrap(), ordinary);

        let parts = vec![
            MultipartPartRecord {
                part_number: 1,
                size: 5,
            },
            MultipartPartRecord {
                part_number: 3,
                size: 6,
            },
        ];
        let assembled =
            ObjectMetadataRecord::from_staged(&metadata_seed(Some(parts.clone())), &staged_blob());
        assert_eq!(assembled.format_version, OBJECT_METADATA_V2_FORMAT_VERSION);
        assert_eq!(assembled.multipart_parts.as_deref(), Some(parts.as_slice()));
        let encoded = assembled.encode().unwrap();
        let decoded = ObjectMetadataRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, assembled);
        assert_eq!(decoded.multipart_parts.as_deref(), Some(parts.as_slice()));

        // A pre-composed checksum (multipart COMPOSITE, or a replica's pinned value) is recorded
        // verbatim instead of being looked up among the staged digests.
        let mut seeded = metadata_seed(None);
        seeded.checksums = Some(vec![ChecksumValue {
            algorithm: ChecksumAlgorithm::Crc32,
            value: "AAAAAA==-2".to_owned(),
        }]);
        let with_composite = ObjectMetadataRecord::from_staged(&seeded, &staged_blob());
        assert_eq!(with_composite.checksums, seeded.checksums.clone().unwrap());

        // The layout also survives the inline trailer codec.
        let trailer = assembled.encode_inline_trailer(11).unwrap();
        let (inline, payload) = ObjectMetadataRecord::decode_inline_suffix(&trailer)
            .unwrap()
            .unwrap();
        assert_eq!(payload, 11);
        assert_eq!(inline, assembled);
    }

    #[test]
    fn a_generation_that_disagrees_with_its_layout_is_rejected() {
        let ordinary = ObjectMetadataRecord::from_staged(&metadata_seed(None), &staged_blob());
        let mut v1_with_parts: serde_json::Value =
            serde_json::from_slice(&ordinary.encode().unwrap()).unwrap();
        v1_with_parts["multipart_parts"] = serde_json::json!([{"part_number": 1, "size": 11}]);
        assert!(
            ObjectMetadataRecord::decode(&serde_json::to_vec(&v1_with_parts).unwrap()).is_err()
        );

        let mut v2_without_parts: serde_json::Value =
            serde_json::from_slice(&ordinary.encode().unwrap()).unwrap();
        v2_without_parts["format_version"] = serde_json::json!(OBJECT_METADATA_V2_FORMAT_VERSION);
        assert!(
            ObjectMetadataRecord::decode(&serde_json::to_vec(&v2_without_parts).unwrap()).is_err()
        );

        let mut unknown: serde_json::Value =
            serde_json::from_slice(&ordinary.encode().unwrap()).unwrap();
        unknown["format_version"] = serde_json::json!(99);
        assert!(ObjectMetadataRecord::decode(&serde_json::to_vec(&unknown).unwrap()).is_err());
    }

    #[test]
    fn an_oversized_record_is_refused_on_both_encode_and_decode() {
        let mut seed = metadata_seed(None);
        seed.user_metadata.push((
            "oversized".to_owned(),
            "x".repeat(OBJECT_METADATA_MAX_BYTES),
        ));
        let record = ObjectMetadataRecord::from_staged(&seed, &staged_blob());
        assert!(record.encode().is_err());
        assert!(record.encode_inline_trailer(11).is_err());
        assert!(ObjectMetadataRecord::decode(&vec![b' '; OBJECT_METADATA_MAX_BYTES + 1]).is_err());

        // A trailer that declares an over-bound record is not our metadata: skip it, never parse it.
        let meta_len = OBJECT_METADATA_MAX_BYTES + 1;
        let mut suffix = vec![0u8; meta_len + INLINE_TRAILER_FIXED];
        suffix[meta_len..meta_len + 4].copy_from_slice(&(meta_len as u32).to_le_bytes());
        suffix[meta_len + 12..].copy_from_slice(&INLINE_TRAILER_MAGIC);
        assert!(
            ObjectMetadataRecord::decode_inline_suffix(&suffix)
                .unwrap()
                .is_none()
        );
    }

    fn object_plan() -> PlannedStorageWrite {
        PlannedStorageWrite::new(
            BucketName::parse("bucket").unwrap(),
            StorageToken::generate(),
            StorageWriteTarget::Object {
                key: ObjectKey::parse("key/../is-not-a-file-path").unwrap(),
                version_id: VersionId::null(),
                row_id: StorageToken::generate().as_str().to_owned(),
            },
        )
        .unwrap()
    }

    #[test]
    fn alias_liveness_resolves_to_the_data_owner() {
        let bucket = BucketName::parse("bucket").unwrap();
        let data = StoragePath::generate(&bucket);
        let final_sidecar = object_metadata_path(&data).unwrap();
        assert_eq!(storage_alias_owner(&final_sidecar), data);

        let attempt = StorageToken::generate();
        let staged = StoragePath::from_string(format!(".staging/{}.meta.tmp", attempt.as_str()));
        assert_eq!(
            storage_alias_owner(&staged),
            StoragePath::from_string(format!(".staging/{}.tmp", attempt.as_str()))
        );
        // A data path and unrelated names own themselves.
        assert_eq!(storage_alias_owner(&data), data);
        let other = StoragePath::from_string("bucket/unrelated".to_owned());
        assert_eq!(storage_alias_owner(&other), other);
    }

    #[test]
    fn plan_is_bounded_exact_and_roundtrips_without_creation_authority() {
        let planned = object_plan();
        let plan = planned.plan();
        assert_eq!(plan.paths.len(), 5);
        assert_eq!(
            plan.paths
                .iter()
                .find(|path| path.role == StoragePathRole::ObjectMetadata)
                .map(|path| path.path.as_str()),
            Some(format!("{}.meta", plan.final_path().unwrap().as_str()).as_str())
        );
        let encoded = serde_json::to_vec(plan).unwrap();
        let decoded: StorageWritePlan = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(&decoded, plan);
        assert!(decoded.validate().is_ok());
        assert!(!plan.final_path().unwrap().as_str().contains("key"));
        let other = object_plan();
        assert_ne!(plan.attempt, other.plan().attempt);
        for path in &plan.paths {
            assert!(
                !other
                    .plan()
                    .paths
                    .iter()
                    .any(|other| path.path == other.path)
            );
        }
        for bad in [
            "/etc/passwd",
            "bucket/../other",
            "bucket//file",
            "bucket/file",
            "",
        ] {
            let mut forged = plan.clone();
            forged.paths[0].path = StoragePath::from_string(bad.to_owned());
            assert!(forged.validate().is_err(), "accepted {bad}");
        }
        let mut duplicate = plan.clone();
        duplicate.paths.push(duplicate.paths[0].clone());
        assert!(duplicate.validate().is_err());
        let mut rerouted = plan.clone();
        rerouted.bucket = BucketName::parse("other-bucket").unwrap();
        assert!(rerouted.validate().is_err());
    }

    #[test]
    fn protocol_two_object_plans_remain_recoverable_but_are_not_journaled_twice() {
        let planned = object_plan();
        let plan = planned.plan();
        assert_eq!(plan.journaled_paths().count(), 3);
        let mut legacy = plan.clone();
        legacy.paths.retain(|path| {
            path.role != StoragePathRole::ObjectMetadata
                && !(path.role == StoragePathRole::Temporary
                    && path.path.as_str().ends_with(".meta.tmp"))
        });
        assert_eq!(legacy.paths.len(), 3);
        assert!(
            legacy.validate().is_ok(),
            "a pre-sidecar journal row must still decode"
        );
        let mut widened = legacy.clone();
        widened.paths.push(StorageIntentPath {
            role: StoragePathRole::ObjectMetadata,
            path: StoragePath::from_string(format!("{}.meta", plan.final_path().unwrap().as_str())),
        });
        assert!(
            widened.validate().is_err(),
            "a partial sidecar set is not a legal plan"
        );
    }

    #[test]
    fn multipart_plans_accept_shard_routing_and_reject_unsafe_identifiers() {
        let bucket = BucketName::parse("bucket").unwrap();
        let generation = StorageToken::generate();
        for upload in [
            StorageToken::generate().as_str().to_owned(),
            format!("7~{}", StorageToken::generate().as_str()),
        ] {
            let planned = PlannedStorageWrite::new(
                bucket.clone(),
                generation.clone(),
                StorageWriteTarget::Part {
                    upload_id: UploadId::from_string(upload),
                    part_number: 10_000,
                    reservation_id: StorageToken::generate().as_str().to_owned(),
                },
            )
            .unwrap();
            assert_eq!(planned.plan().paths.len(), 2);
        }
        for upload in [
            "../bucket",
            "",
            "../../",
            "7~../other",
            "/root",
            "0~invalid",
        ] {
            assert!(
                PlannedStorageWrite::new(
                    bucket.clone(),
                    generation.clone(),
                    StorageWriteTarget::Part {
                        upload_id: UploadId::from_string(upload.to_owned()),
                        part_number: 1,
                        reservation_id: StorageToken::generate().as_str().to_owned(),
                    },
                )
                .is_err()
            );
        }
    }

    #[test]
    fn only_exact_acknowledgement_and_live_matching_io_lease_admit_creation() {
        for mismatch in 0..4 {
            let planned = object_plan();
            let plan = planned.plan().clone();
            let (watch, lease) = io::StorageIoWatch::new(
                if mismatch == 2 {
                    StorageToken::generate()
                } else {
                    plan.attempt.clone()
                },
                plan.generation.clone(),
                Arc::new(()),
            );
            if mismatch == 3 {
                watch.cancel();
            }
            let receipt = match mismatch {
                0 => StorageAdmission::NotApplied,
                1 => StorageAdmission::Granted(Box::new(object_plan().plan().clone())),
                _ => StorageAdmission::Granted(Box::new(plan)),
            };
            assert!(planned.admit(receipt, lease).is_err());
        }
        let planned = object_plan();
        let receipt = planned.plan().clone();
        let (_watch, lease) = io::StorageIoWatch::new(
            receipt.attempt.clone(),
            receipt.generation.clone(),
            Arc::new(()),
        );
        assert!(
            planned
                .admit(StorageAdmission::Granted(Box::new(receipt)), lease)
                .is_ok()
        );
    }
}
