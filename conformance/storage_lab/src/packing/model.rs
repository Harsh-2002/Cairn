//! Explicit identities and locations for the isolated packing laboratory.
//!
//! These are not production storage types or a production-format proposal.

use cairn_types::CompressionDescriptor;
use cairn_types::storage::StorageToken;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub use super::store::{ArtifactAdmission, CleanupClaim};

pub const MAX_RECORDS: usize = 256;
pub const MAX_SEGMENT_LENGTH: u64 = 4 * 1024 * 1024;
pub const SEGMENT_HEADER_LENGTH: u64 = 80;
pub const RECORD_HEADER_LENGTH: u64 = 40;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactIdentity {
    pub id: StorageToken,
    pub generation: StorageToken,
}

impl ArtifactIdentity {
    pub fn file_name(&self, kind: ArtifactKind) -> String {
        let (prefix, extension) = match kind {
            ArtifactKind::File => ("file", "blob"),
            ArtifactKind::Segment => ("segment", "pack"),
        };
        format!(
            "{prefix}-{}-{}.{extension}",
            self.id.as_str(),
            self.generation.as_str()
        )
    }

    fn temporary_name(&self) -> String {
        format!(
            ".pending-{}-{}.tmp",
            self.id.as_str(),
            self.generation.as_str()
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactKind {
    File,
    Segment,
}

/// A descriptive database plan. Only [`ArtifactAdmission`] authorizes creation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPlan {
    artifact: ArtifactIdentity,
    kind: ArtifactKind,
    temporary_path: PathBuf,
    final_path: PathBuf,
    max_length: u64,
}

impl ArtifactPlan {
    pub(super) fn new(
        artifact: ArtifactIdentity,
        kind: ArtifactKind,
        max_length: u64,
    ) -> Result<Self> {
        if max_length > i64::MAX as u64
            || (kind == ArtifactKind::Segment
                && !(SEGMENT_HEADER_LENGTH..=MAX_SEGMENT_LENGTH).contains(&max_length))
        {
            return Err("invalid bounded artifact length".into());
        }
        Ok(Self {
            temporary_path: PathBuf::from(artifact.temporary_name()),
            final_path: PathBuf::from(artifact.file_name(kind)),
            artifact,
            kind,
            max_length,
        })
    }

    pub fn artifact(&self) -> &ArtifactIdentity {
        &self.artifact
    }

    pub fn kind(&self) -> ArtifactKind {
        self.kind
    }

    pub fn temporary_path(&self) -> &Path {
        &self.temporary_path
    }

    pub fn final_path(&self) -> &Path {
        &self.final_path
    }

    pub fn max_length(&self) -> u64 {
        self.max_length
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Location {
    File {
        artifact: ArtifactIdentity,
        length: u64,
    },
    Segment {
        artifact: ArtifactIdentity,
        offset: u64,
        length: u64,
    },
}

impl Location {
    pub fn artifact(&self) -> &ArtifactIdentity {
        match self {
            Self::File { artifact, .. } | Self::Segment { artifact, .. } => artifact,
        }
    }

    pub fn kind(&self) -> ArtifactKind {
        match self {
            Self::File { .. } => ArtifactKind::File,
            Self::Segment { .. } => ArtifactKind::Segment,
        }
    }

    pub fn offset(&self) -> u64 {
        match self {
            Self::File { .. } => 0,
            Self::Segment { offset, .. } => *offset,
        }
    }

    pub fn length(&self) -> u64 {
        match self {
            Self::File { length, .. } | Self::Segment { length, .. } => *length,
        }
    }

    pub fn path(&self) -> PathBuf {
        PathBuf::from(self.artifact().file_name(self.kind()))
    }

    pub(super) fn validate(&self) -> Result<()> {
        if self.length() > i64::MAX as u64
            || self.offset() > i64::MAX as u64
            || self.offset().checked_add(self.length()).is_none()
            || (self.kind() == ArtifactKind::Segment
                && (self.offset() < SEGMENT_HEADER_LENGTH + RECORD_HEADER_LENGTH
                    || self.offset() + self.length() > MAX_SEGMENT_LENGTH))
        {
            return Err("invalid artifact record bounds".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncodedFormat {
    Raw,
    Crnb,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CipherFormat {
    Plaintext,
    LegacyV2,
    AuthenticatedV3,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordMetadata {
    pub row_id: StorageToken,
    pub key: String,
    pub encoded_sha256: [u8; 32],
    pub encoded_length: u64,
    pub logical_size: u64,
    pub format: EncodedFormat,
    pub compression: CompressionDescriptor,
    pub cipher: CipherFormat,
    /// A conservative laboratory Object Lock fixture: it cannot be weakened in place.
    pub locked: bool,
}

impl RecordMetadata {
    pub(super) fn validate(&self) -> Result<()> {
        if self.key.is_empty()
            || self.key.len() > 1024
            || self.encoded_length > i64::MAX as u64
            || self.logical_size > i64::MAX as u64
            || (self.format == EncodedFormat::Raw
                && (self.encoded_length != self.logical_size
                    || self.compression != CompressionDescriptor::Uncompressed
                    || self.cipher != CipherFormat::Plaintext))
        {
            return Err("invalid record metadata".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExpectedCurrent {
    /// Explicit unconditional laboratory publication.
    Any,
    Absent,
    Exact {
        row_id: StorageToken,
        location: Location,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishRecord {
    pub metadata: RecordMetadata,
    pub location: Location,
    pub expected: ExpectedCurrent,
    pub preserve_previous: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedRecord {
    pub metadata: RecordMetadata,
    pub location: Location,
    pub is_current: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    Conflict,
    Locked,
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupDebt {
    pub id: StorageToken,
    pub artifact: ArtifactIdentity,
    pub kind: ArtifactKind,
    pub path: PathBuf,
    pub claimed: bool,
}

#[cfg(test)]
pub(super) fn encryption_test_key() -> cairn_types::SecretKey32 {
    use std::io::Read;
    let mut key = [0; 32];
    std::fs::File::open("/dev/urandom")
        .unwrap()
        .read_exact(&mut key)
        .unwrap();
    cairn_types::SecretKey32::new(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_derive_exact_single_component_paths() {
        let artifact = ArtifactIdentity {
            id: StorageToken::generate(),
            generation: StorageToken::generate(),
        };
        let plan = ArtifactPlan::new(artifact.clone(), ArtifactKind::File, 0).unwrap();
        assert_eq!(plan.final_path().components().count(), 1);
        assert_eq!(plan.temporary_path().components().count(), 1);
        assert_ne!(plan.final_path(), plan.temporary_path());
        assert_eq!(
            plan.final_path(),
            Location::File {
                artifact,
                length: 0
            }
            .path()
        );
        assert!(StorageToken::try_from("../escape".to_owned()).is_err());
    }

    #[test]
    fn geometry_rejects_overflow_and_header_locations() {
        let artifact = ArtifactIdentity {
            id: StorageToken::generate(),
            generation: StorageToken::generate(),
        };
        for (offset, length) in [(u64::MAX, 2), (79, 1), (MAX_SEGMENT_LENGTH, 1)] {
            assert!(
                Location::Segment {
                    artifact: artifact.clone(),
                    offset,
                    length,
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            ArtifactPlan::new(artifact, ArtifactKind::Segment, MAX_SEGMENT_LENGTH + 1).is_err()
        );
    }
}
