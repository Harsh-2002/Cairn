//! Types crossing the replication sink boundary. The sink is an S3-compatible destination;
//! a fake sink in tests records intents and simulates failures.

use crate::authz::Acl;
use crate::id::{ObjectKey, VersionId};
use crate::object::{ChecksumValue, ETag, StorageClass, UserMetadata};

/// An object to put at a replication destination. Its body is a logical-byte stream read
/// from the source blob store.
pub struct ReplicatedObject {
    /// The destination key.
    pub key: ObjectKey,
    /// The source version id (the idempotency identity).
    pub version_id: VersionId,
    /// The content type.
    pub content_type: String,
    /// User-defined metadata.
    pub user_metadata: UserMetadata,
    /// The ETag (for verification at the destination).
    pub etag: ETag,
    /// The logical size.
    pub size: u64,
    /// The object's tags.
    pub tags: Vec<(String, String)>,
    /// The object ACL to apply, if the rule dictates.
    pub acl: Option<Acl>,
    /// Stored system response headers, replicated so the destination serves identical headers and a
    /// gzip'd object still auto-decompresses on the replica (audit 2026-07; AWS CRR preserves these).
    pub content_encoding: Option<String>,
    /// `Cache-Control` header.
    pub cache_control: Option<String>,
    /// `Content-Disposition` header.
    pub content_disposition: Option<String>,
    /// `Content-Language` header.
    pub content_language: Option<String>,
    /// `Expires` header.
    pub expires: Option<String>,
    /// The object's storage class, re-emitted on the replica.
    pub storage_class: StorageClass,
    /// Client-supplied flexible checksums, re-emitted as `x-amz-checksum-*` so a checksum-mode GET of
    /// the replica matches the source (audit 2026-07; AWS CRR preserves additional checksums).
    pub checksums: Vec<ChecksumValue>,
    /// The logical-byte body stream.
    pub body: crate::BlobStream,
}

impl std::fmt::Debug for ReplicatedObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicatedObject")
            .field("key", &self.key)
            .field("version_id", &self.version_id)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}
