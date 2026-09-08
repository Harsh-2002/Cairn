//! Durable physical-write ownership (storage protocol 2, ARCH 8). Plans contain names, never
//! file handles. Only an acknowledged admission can turn a fresh plan into a creation permit.

use crate::id::{BucketName, ObjectKey, StoragePath, UploadId, VersionId};
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

/// Closed grammar for exact cleanup, including legacy flat/nested objects and multipart parts.
/// Unknown shapes remain for explicit reconciliation; the blob backend also rejects symlinks.
pub fn validate_storage_path(bucket: &BucketName, path: &StoragePath) -> Result<(), MetaError> {
    BucketName::parse(bucket.as_str()).map_err(|_| invalid("invalid storage routing bucket"))?;
    if path.as_str().len() > 256 {
        return Err(invalid("unsupported exact storage path"));
    }
    let components: Vec<_> = path.as_str().split('/').collect();
    let valid = match components.as_slice() {
        [directory, name] if *directory == bucket.as_str() => uuid_name(name),
        [directory, prefix, name] if *directory == bucket.as_str() => {
            uuid_name(name) && prefix.len() == 2 && name.starts_with(prefix)
        }
        [".staging", name] => name
            .strip_suffix(".index.tmp")
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
}

/// One exact relative filename. Syntax validation does not replace backend no-follow traversal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageIntentPath {
    pub role: StoragePathRole,
    pub path: StoragePath,
}

/// Serializable admission request. The Writer revalidates its bounded, canonical names.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageWritePlan {
    pub attempt: StorageToken,
    pub generation: StorageToken,
    pub bucket: BucketName,
    pub target: StorageWriteTarget,
    pub paths: Vec<StorageIntentPath>,
}

impl StorageWritePlan {
    /// Refuse aliases, wrong routing and unplanned paths before recording admission.
    pub fn validate(&self) -> Result<(), MetaError> {
        BucketName::parse(self.bucket.as_str()).map_err(|_| invalid("invalid storage bucket"))?;
        let expected = self.expected_paths()?;
        if self.paths != expected {
            return Err(invalid("storage plan paths do not match its exact target"));
        }
        for path in &self.paths {
            validate_storage_path(&self.bucket, &path.path)?;
        }
        Ok(())
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

    fn expected_paths(&self) -> Result<Vec<StorageIntentPath>, MetaError> {
        let mut paths = Vec::with_capacity(3);
        let id = self.attempt.as_str();
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
                format!("{}/{id}", self.bucket)
            }
        };
        paths.push(StorageIntentPath {
            role: StoragePathRole::Final,
            path: StoragePath::from_string(final_path),
        });
        // A small/plain write may never create this optional alias. Reserving it unconditionally
        // keeps the encoder's later spill decision inside the original bounded admission.
        paths.push(StorageIntentPath {
            role: StoragePathRole::IndexSpool,
            path: StoragePath::from_string(format!(".staging/{id}.index.tmp")),
        });
        Ok(paths)
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

/// File-free, move-only backend plan. Deserializing its DTO cannot recreate this capability.
#[derive(Debug)]
pub struct PlannedStorageWrite(StorageWritePlan);

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
    fn plan_is_bounded_exact_and_roundtrips_without_creation_authority() {
        let planned = object_plan();
        let plan = planned.plan();
        assert_eq!(plan.paths.len(), 3);
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
