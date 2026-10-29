//! Durable physical-write ownership (storage protocol 2, ARCH 8). Plans contain names, never
//! file handles. Only an acknowledged admission can turn a fresh plan into a creation permit.

use crate::authz::Acl;
use crate::id::{BucketName, ObjectKey, StoragePath, UploadId, UserId, VersionId};
use crate::object::{
    ChecksumSet, ChecksumValue, CompressionDescriptor, ETag, ExplicitObjectLockIntent,
    StorageClass, UserMetadata,
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
        let object = matches!(self.target, StorageWriteTarget::Object { .. });
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
        if !matches!(self.target, StorageWriteTarget::Object { .. }) {
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
/// The sidecar sits beside the data file as `<final>.meta`. It is serialized JSON in v1, bounded by
/// the existing object-metadata limits, and carries no secret plaintext: an SSE descriptor is the
/// same sealed record SQLite stores, never a raw data-encryption key.
pub const OBJECT_METADATA_FORMAT_VERSION: u32 = 1;

/// One durable object metadata sidecar record. SQLite remains the ACK authority in P1; the record
/// is written with the data file so a later protocol-3 phase can rebuild a missing index row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectMetadataRecord {
    /// Format generation; v1 is the only accepted value in this phase.
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
    pub row_id: String,
    pub is_delete_marker: bool,
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
    pub sse_descriptor: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub tags: Vec<(String, String)>,
    pub lock_intent: ExplicitObjectLockIntent,
}

/// Largest logical object stored as one inline file (payload + metadata trailer).
pub const INLINE_MAX_BYTES: u64 = 128 * 1024;

/// Trailer magic: `payload || metadata JSON || u32 meta_len || u64 payload_len || magic`.
pub const INLINE_TRAILER_MAGIC: [u8; 4] = *b"CRNI";

/// Fixed trailer size after the metadata JSON (`meta_len`, `payload_len`, magic).
pub const INLINE_TRAILER_FIXED: usize = 4 + 8 + 4;

impl ObjectMetadataRecord {
    /// Encode the metadata JSON and its trailer for a file that already holds `payload_len` bytes.
    pub fn encode_inline_trailer(&self, payload_len: u64) -> Result<Vec<u8>, MetaError> {
        let json =
            serde_json::to_vec(self).map_err(|error| MetaError::Engine(error.to_string()))?;
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
        let record: Self = serde_json::from_slice(&suffix[..meta_len])
            .map_err(|error| MetaError::Engine(error.to_string()))?;
        Ok(Some((record, payload_len)))
    }
}

impl ObjectMetadataRecord {
    /// Bind the pre-stage fields to the exact staged blob.
    #[must_use]
    pub fn from_staged(seed: &ObjectMetadataSeed, staged: &crate::blob::StagedBlob) -> Self {
        Self {
            format_version: OBJECT_METADATA_FORMAT_VERSION,
            bucket: seed.bucket.clone(),
            key: seed.key.clone(),
            version_id: seed.version_id.clone(),
            row_id: seed.row_id.clone(),
            is_delete_marker: seed.is_delete_marker,
            size_logical: staged.size_logical,
            size_physical: staged.size_physical,
            etag: staged.etag.clone(),
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
            checksums: staged
                .checksums
                .iter()
                .filter(|checksum| seed.stored_checksums.0.contains(&checksum.algorithm))
                .cloned()
                .collect(),
            sse_descriptor: seed.sse_descriptor.clone(),
            internal_sha256: Some(staged.internal_sha256.clone()),
            created_at: seed.created_at,
            updated_at: seed.updated_at,
            tags: seed.tags.clone(),
            lock_intent: seed.lock_intent,
        }
    }

    /// Canonical bytes for the sidecar file.
    pub fn encode(&self) -> Result<Vec<u8>, MetaError> {
        serde_json::to_vec(self).map_err(|error| MetaError::Engine(error.to_string()))
    }

    /// Parse and reject an unsupported format generation.
    pub fn decode(bytes: &[u8]) -> Result<Self, MetaError> {
        let record: Self =
            serde_json::from_slice(bytes).map_err(|error| MetaError::Engine(error.to_string()))?;
        if record.format_version != OBJECT_METADATA_FORMAT_VERSION {
            return Err(invalid("unsupported object metadata format version"));
        }
        Ok(record)
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
    use std::sync::Arc;

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
