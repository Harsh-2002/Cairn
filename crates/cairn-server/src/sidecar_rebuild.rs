//! Rebuild the object index from durable protocol-3 metadata files after an unclean shutdown.
//!
//! Ordinary object publications acknowledge after the data file and its metadata sidecar are
//! durable, before the SQLite index commit is guaranteed. A power loss can therefore drop the tail
//! of acknowledged index rows while the sidecars remain. Startup walks every bucket directory,
//! verifies each sidecar against its data file, and inserts the missing rows before the node
//! serves traffic. A corrupt or mismatched sidecar is reported and skipped: this pass never invents
//! an object, and it never deletes anything.

use cairn_types::blob::IndexSpoolState;
use cairn_types::error::MetaError;
use cairn_types::id::StoragePath;
use cairn_types::meta::{Mutation, MutationOutcome};
use cairn_types::object::ObjectVersionRow;
use cairn_types::storage::{
    ObjectMetadataRecord, StorageIntentPath, StoragePathRole, StorageToken, StorageWritePlan,
    StorageWriteTarget,
};
use cairn_types::traits::MetadataStore;
use cairn_types::{Bucket, InitialObjectState, Precondition};
use std::path::{Path, PathBuf};

/// What one rebuild pass did.
#[derive(Debug, Default)]
pub(crate) struct RebuildReport {
    pub scanned: u64,
    pub inserted: u64,
    pub already_indexed: u64,
    pub skipped: u64,
    pub failed: u64,
}

/// Insert index rows for durable sidecars whose row is missing. Runs before the node serves and
/// before any background reconciliation, so a sidecar is never mistaken for an orphan.
pub(crate) async fn rebuild_missing_object_rows(
    meta: &dyn MetadataStore,
    data_dir: &Path,
    generation: &StorageToken,
) -> RebuildReport {
    let mut report = RebuildReport::default();
    let mut records = Vec::new();
    let Ok(buckets) = std::fs::read_dir(data_dir) else {
        return report;
    };
    for bucket_entry in buckets.flatten() {
        let path = bucket_entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(bucket_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if bucket_name.starts_with('.') {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("meta") {
                report.scanned += 1;
                match read_sidecar(&path) {
                    Ok(record) => records.push(record),
                    Err(error) => {
                        report.failed += 1;
                        tracing::warn!(path = %path.display(), %error, "unreadable object metadata sidecar");
                    }
                }
                continue;
            }
            // A plaintext object carries its record as a trailer in the data file and has no
            // `.meta` companion, whatever its size. Compressed/encrypted objects always have one,
            // so absence plus the magic is the discriminator.
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.len() != 32 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            let mut companion = path.clone().into_os_string();
            companion.push(".meta");
            if std::path::Path::new(&companion).exists() {
                continue;
            }
            report.scanned += 1;
            match read_inline_record(&path) {
                Ok(Some(record)) => records.push(record),
                Ok(None) => report.skipped += 1,
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(path = %path.display(), %error, "unreadable inline metadata trailer");
                }
            }
        }
    }

    // Version ids are uuidv7 (time-ordered), so replaying ascending preserves which version the
    // index reports as current. Unversioned objects have at most one surviving sidecar per key.
    records.sort_by(|a, b| {
        (a.bucket.as_str(), a.key.as_str(), a.version_id.as_str()).cmp(&(
            b.bucket.as_str(),
            b.key.as_str(),
            b.version_id.as_str(),
        ))
    });

    let mut buckets: std::collections::HashMap<String, Option<Bucket>> =
        std::collections::HashMap::new();
    for record in records {
        let bucket_key = record.bucket.as_str().to_owned();
        let bucket = match buckets.entry(bucket_key) {
            std::collections::hash_map::Entry::Occupied(slot) => slot.get().clone(),
            std::collections::hash_map::Entry::Vacant(slot) => {
                let loaded = match meta.get_bucket(&record.bucket).await {
                    Ok(found) => found,
                    Err(error) => {
                        report.failed += 1;
                        tracing::warn!(bucket = record.bucket.as_str(), %error, "cannot read bucket");
                        continue;
                    }
                };
                slot.insert(loaded.clone());
                loaded
            }
        };
        let Some(bucket) = bucket else {
            report.skipped += 1;
            continue;
        };
        match meta
            .get_version(&record.bucket, &record.key, &record.version_id)
            .await
        {
            Ok(Some(existing)) => {
                if existing.id == record.row_id {
                    report.already_indexed += 1;
                    continue;
                }
                if !record_supersedes(&record, &existing) {
                    // A newer row owns this version: the file is a superseded generation.
                    report.skipped += 1;
                    continue;
                }
                // The index holds an older generation whose replacement commit was lost with the
                // relaxed WAL tail. Republish the durable newer file: on an unversioned bucket every
                // write shares the `null` version id, and skipping here would silently revert an
                // acknowledged overwrite to the previous object's bytes.
            }
            Ok(None) => {}
            Err(error) => {
                report.failed += 1;
                tracing::warn!(key = record.key.as_str(), %error, "cannot read object version");
                continue;
            }
        }
        if !data_file_exists(data_dir, &record.storage_path) {
            report.skipped += 1;
            tracing::warn!(
                path = record.storage_path.as_str(),
                "metadata sidecar has no data file; leaving it for reconciliation"
            );
            continue;
        }
        match publish_rebuilt(meta, generation, &bucket, &record).await {
            Ok(true) => report.inserted += 1,
            Ok(false) => report.skipped += 1,
            Err(error) => {
                report.failed += 1;
                tracing::warn!(
                    key = record.key.as_str(),
                    version = record.version_id.as_str(),
                    %error,
                    "could not rebuild object index row"
                );
            }
        }
    }
    report
}

/// The exact five-name path set of an ordinary object write (ARCH 8/9).
fn sidecar_rebuild_paths(
    storage_path: &StoragePath,
    attempt: &StorageToken,
) -> Vec<StorageIntentPath> {
    let id = attempt.as_str();
    vec![
        StorageIntentPath {
            role: StoragePathRole::Temporary,
            path: StoragePath::from_string(format!(".staging/{id}.tmp")),
        },
        StorageIntentPath {
            role: StoragePathRole::Temporary,
            path: StoragePath::from_string(format!(".staging/{id}.meta.tmp")),
        },
        StorageIntentPath {
            role: StoragePathRole::Final,
            path: storage_path.clone(),
        },
        StorageIntentPath {
            role: StoragePathRole::ObjectMetadata,
            path: StoragePath::from_string(format!("{}.meta", storage_path.as_str())),
        },
        StorageIntentPath {
            role: StoragePathRole::IndexSpool,
            path: StoragePath::from_string(format!(".staging/{id}.index.tmp")),
        },
    ]
}

/// Whether a durable record is a newer generation than the index row that owns the same version.
/// Row ids are UUIDv7 (time-ordered), so the pair orders writes even when the commit clock is
/// coarse; the `null` version id of an unversioned bucket is exactly where this matters.
fn record_supersedes(record: &ObjectMetadataRecord, existing: &ObjectVersionRow) -> bool {
    supersedes(
        record.updated_at,
        &record.row_id,
        existing.updated_at,
        &existing.id,
    )
}

fn supersedes(
    record_updated: cairn_types::Timestamp,
    record_id: &str,
    existing_updated: cairn_types::Timestamp,
    existing_id: &str,
) -> bool {
    (record_updated, record_id) > (existing_updated, existing_id)
}

/// Read an inline object's metadata from the trailer of its data file, if present.
fn read_inline_record(path: &Path) -> Result<Option<ObjectMetadataRecord>, MetaError> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file =
        std::fs::File::open(path).map_err(|error| MetaError::Engine(error.to_string()))?;
    let len = file
        .metadata()
        .map_err(|error| MetaError::Engine(error.to_string()))?
        .len();
    const FIXED: u64 = 16;
    if len < FIXED {
        return Ok(None);
    }
    let mut fixed = [0u8; 16];
    file.seek(SeekFrom::Start(len - FIXED))
        .and_then(|_| file.read_exact(&mut fixed))
        .map_err(|error| MetaError::Engine(error.to_string()))?;
    if fixed[12..16] != cairn_types::storage::INLINE_TRAILER_MAGIC {
        return Ok(None);
    }
    let meta_len = u32::from_le_bytes(fixed[0..4].try_into().expect("four bytes")) as u64;
    // The trailer lives in bytes an unauthenticated writer can choose, so an implausible declared
    // length is not our metadata: skip it instead of allocating up to the object size on every
    // restart.
    if meta_len > cairn_types::storage::OBJECT_METADATA_MAX_BYTES as u64 {
        return Ok(None);
    }
    let suffix_len = meta_len
        .checked_add(FIXED)
        .filter(|total| *total <= len)
        .ok_or_else(|| MetaError::Engine("inline trailer length exceeds its file".into()))?;
    let mut suffix = vec![0u8; suffix_len as usize];
    file.seek(SeekFrom::Start(len - suffix_len))
        .and_then(|_| file.read_exact(&mut suffix))
        .map_err(|error| MetaError::Engine(error.to_string()))?;
    let Some((record, trailer_payload)) = ObjectMetadataRecord::decode_inline_suffix(&suffix)
        .map_err(|error| MetaError::Engine(format!("inline metadata trailer: {error}")))?
    else {
        return Ok(None);
    };
    if trailer_payload != record.size_physical {
        return Err(MetaError::Engine(
            "inline trailer payload length disagrees with its record".into(),
        ));
    }
    Ok(Some(record))
}

fn read_sidecar(path: &Path) -> Result<ObjectMetadataRecord, MetaError> {
    // Check the bound before reading: a corrupt or hostile sidecar must not make startup allocate
    // an arbitrarily large buffer just to reject it.
    let len = std::fs::metadata(path)
        .map_err(|error| MetaError::Engine(error.to_string()))?
        .len();
    if len > cairn_types::storage::OBJECT_METADATA_MAX_BYTES as u64 {
        return Err(MetaError::Engine(
            "object metadata sidecar exceeds its 1 MiB bound".into(),
        ));
    }
    let bytes = std::fs::read(path).map_err(|error| MetaError::Engine(error.to_string()))?;
    ObjectMetadataRecord::decode(&bytes)
}

fn data_file_exists(data_dir: &Path, storage_path: &StoragePath) -> bool {
    let mut full = PathBuf::from(data_dir);
    for component in storage_path.as_str().split('/') {
        full.push(component);
    }
    full.is_file()
}

/// Build the exact five-name plan the ordinary planner would have produced and publish the row
/// through the same savepoint, so counters, visibility, tags and Object Lock state are identical
/// to a live write. The publication re-verifies that every planned name is unreferenced.
async fn publish_rebuilt(
    meta: &dyn MetadataStore,
    generation: &StorageToken,
    bucket: &Bucket,
    record: &ObjectMetadataRecord,
) -> Result<bool, MetaError> {
    let name = record
        .storage_path
        .as_str()
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_owned();
    let attempt = StorageToken::try_from(name)?;
    let paths = sidecar_rebuild_paths(&record.storage_path, &attempt);
    let plan = StorageWritePlan {
        attempt,
        generation: generation.clone(),
        bucket: bucket.name.clone(),
        bucket_owner: Some(bucket.owner_id.clone()),
        bucket_created_at: Some(bucket.created_at),
        target: StorageWriteTarget::Object {
            key: record.key.clone(),
            version_id: record.version_id.clone(),
            row_id: record.row_id.clone(),
        },
        paths,
    };
    plan.validate_object_publication(&rebuilt_operation(record))?;
    let row = rebuilt_row(record);
    let operation = Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        initial_state: InitialObjectState {
            tags: record.tags.clone(),
            lock_intent: record.lock_intent,
        },
        replication: Vec::new(),
    };
    match meta
        .submit(Mutation::PublishObjectWrite {
            plan: Box::new(plan),
            operation: Box::new(operation),
            index_spool: IndexSpoolState::NeverCreated,
            activity: None,
            // A rebuild must restore an acknowledged row; full checks could reject it if a quota
            // was configured after the acknowledgement.
            relaxed: true,
        })
        .await?
    {
        MutationOutcome::Put { .. } => Ok(true),
        // The plan's names are owned by an intent, a cleanup claim, or another row. The live
        // recovery/reconcile path owns that state; leave it untouched.
        MutationOutcome::StoragePublicationNotApplied => Ok(false),
        other => Err(MetaError::Engine(format!(
            "unexpected rebuild publication outcome: {other:?}"
        ))),
    }
}

fn rebuilt_operation(record: &ObjectMetadataRecord) -> Mutation {
    Mutation::PutObjectVersion {
        row: Box::new(rebuilt_row(record)),
        precondition: Precondition::default(),
        initial_state: InitialObjectState {
            tags: record.tags.clone(),
            lock_intent: record.lock_intent,
        },
        replication: Vec::new(),
    }
}

fn rebuilt_row(record: &ObjectMetadataRecord) -> ObjectVersionRow {
    ObjectVersionRow {
        id: record.row_id.clone(),
        bucket: record.bucket.clone(),
        key: record.key.clone(),
        version_id: record.version_id.clone(),
        // `put_version` recomputes current-version visibility against the other rows.
        is_latest: true,
        is_delete_marker: false,
        size_logical: record.size_logical,
        size_physical: record.size_physical,
        etag: record.etag.clone(),
        content_type: record.content_type.clone(),
        content_encoding: record.content_encoding.clone(),
        cache_control: record.cache_control.clone(),
        content_disposition: record.content_disposition.clone(),
        content_language: record.content_language.clone(),
        expires: record.expires.clone(),
        expire_at: record.expire_at,
        storage_path: Some(record.storage_path.clone()),
        compression: record.compression.clone(),
        storage_class: record.storage_class,
        cold_locator: None,
        owner_id: record.owner_id.clone(),
        user_metadata: record.user_metadata.clone(),
        acl: record.acl.clone(),
        checksums: record.checksums.clone(),
        sse_descriptor: record.sse_descriptor.clone(),
        // Replication intent is not part of the sidecar; a rebuilt row is not a replica, and the
        // pre-crash tail may need a replication rescan. Tracked as a known limitation.
        replication_status: None,
        internal_sha256: record.internal_sha256.clone(),
        replicated_at: None,
        created_at: record.created_at,
        updated_at: record.updated_at,
        // A completed multipart object's v2 record carries its part layout, so `?partNumber`
        // survives a rebuild. Older v1 assembled-object sidecars restore the row without it.
        multipart_parts: record.multipart_parts.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_newer_generation_supersedes_an_older_row_with_the_same_version() {
        use cairn_types::Timestamp;
        // Later commit time wins.
        assert!(supersedes(Timestamp(2), "b", Timestamp(1), "a"));
        assert!(!supersedes(Timestamp(1), "a", Timestamp(2), "b"));
        // Row ids are UUIDv7, so an equal timestamp falls back to their order.
        assert!(supersedes(Timestamp(5), "b", Timestamp(5), "a"));
        assert!(!supersedes(Timestamp(5), "a", Timestamp(5), "b"));
        // The same row never supersedes itself.
        assert!(!supersedes(Timestamp(5), "a", Timestamp(5), "a"));
    }

    #[test]
    fn a_rebuilt_multipart_row_keeps_the_part_layout() {
        use cairn_types::object::{
            ChecksumSet, CompressionDescriptor, ETag, ExplicitObjectLockIntent,
            MultipartPartRecord, StorageClass,
        };
        use cairn_types::storage::{
            OBJECT_METADATA_V2_FORMAT_VERSION, ObjectMetadataRecord, ObjectMetadataSeed,
        };
        use cairn_types::{BucketName, ObjectKey, StoragePath, UserId, VersionId};

        let bucket = BucketName::parse("bucket").unwrap();
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
        let seed = ObjectMetadataSeed {
            bucket: bucket.clone(),
            key: ObjectKey::parse("assembled.bin").unwrap(),
            version_id: VersionId::null(),
            etag: Some(ETag::from_md5_hex("0".repeat(32))),
            row_id: "row-1".to_owned(),
            is_delete_marker: false,
            multipart_parts: Some(parts.clone()),
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
            created_at: cairn_types::Timestamp(1),
            updated_at: cairn_types::Timestamp(2),
            tags: Vec::new(),
            lock_intent: ExplicitObjectLockIntent::default(),
        };
        let staged = cairn_types::StagedBlob {
            storage_path: StoragePath::generate(&bucket),
            size_logical: 11,
            size_physical: 11,
            etag: ETag::from_md5_hex("0".repeat(32)),
            md5_hex: "0".repeat(32),
            internal_sha256: "0".repeat(64),
            checksums: Vec::new(),
            compression: CompressionDescriptor::Uncompressed,
            index_spool: IndexSpoolState::NeverCreated,
        };
        let record = ObjectMetadataRecord::from_staged(&seed, &staged);
        assert_eq!(record.format_version, OBJECT_METADATA_V2_FORMAT_VERSION);

        let row = rebuilt_row(&record);
        assert_eq!(row.multipart_parts.as_deref(), Some(parts.as_slice()));

        // A v1 record has no layout; it restores the row without inventing parts.
        let mut legacy = record.clone();
        legacy.format_version = cairn_types::storage::OBJECT_METADATA_V1_FORMAT_VERSION;
        legacy.multipart_parts = None;
        assert!(rebuilt_row(&legacy).multipart_parts.is_none());
    }

    #[tokio::test]
    async fn rebuild_restores_a_multipart_row_and_its_part_layout_end_to_end() {
        use cairn_types::object::{
            ChecksumAlgorithm, ChecksumSet, ChecksumValue, CompressionDescriptor, ETag,
            ExplicitObjectLockIntent, MultipartPartRecord, StorageClass,
        };
        use cairn_types::storage::{ObjectMetadataRecord, ObjectMetadataSeed};
        use cairn_types::testing::InMemoryMetadataStore;
        use cairn_types::{
            BucketName, Mutation, MutationOutcome, ObjectKey, OwnershipMode, StoragePath,
            Timestamp, UserId, VersionId, VersioningState,
        };

        let bucket = cairn_types::Bucket {
            name: BucketName::parse("rebuilt-bucket").unwrap(),
            owner_id: UserId("owner".to_owned()),
            created_at: Timestamp(0),
            versioning: VersioningState::Unversioned,
            ownership_mode: OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".to_owned(),
            compression: None,
        };
        let meta = InMemoryMetadataStore::new();
        meta.submit(Mutation::CreateBucket(Box::new(bucket.clone())))
            .await
            .unwrap();
        let generation = StorageToken::generate();
        assert_eq!(
            meta.submit(Mutation::BeginStorageGeneration {
                generation: generation.clone(),
            })
            .await
            .unwrap(),
            MutationOutcome::Ack
        );

        let key = ObjectKey::parse("assembled.bin").unwrap();
        let version_id = VersionId::null();
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
        let storage_path = StoragePath::from_string(format!(
            "{}/{}",
            bucket.name.as_str(),
            StorageToken::generate().as_str()
        ));
        let checksums = vec![ChecksumValue {
            algorithm: ChecksumAlgorithm::Crc32,
            value: "AAAAAA==-2".to_owned(),
        }];
        let seed = ObjectMetadataSeed {
            bucket: bucket.name.clone(),
            key: key.clone(),
            version_id: version_id.clone(),
            etag: Some(ETag::from_md5_hex("0".repeat(32))),
            row_id: StorageToken::generate().as_str().to_owned(),
            is_delete_marker: false,
            multipart_parts: Some(parts.clone()),
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
            checksums: Some(checksums.clone()),
            sse_descriptor: None,
            created_at: Timestamp(1),
            updated_at: Timestamp(2),
            tags: Vec::new(),
            lock_intent: ExplicitObjectLockIntent::default(),
        };
        let staged = cairn_types::StagedBlob {
            storage_path: storage_path.clone(),
            size_logical: 11,
            size_physical: 11,
            etag: ETag::from_md5_hex("0".repeat(32)),
            md5_hex: "0".repeat(32),
            internal_sha256: "0".repeat(64),
            checksums: Vec::new(),
            compression: CompressionDescriptor::Uncompressed,
            index_spool: IndexSpoolState::NeverCreated,
        };
        let record = ObjectMetadataRecord::from_staged(&seed, &staged);
        let dir = tempfile::tempdir().unwrap();
        let data_file = dir.path().join(storage_path.as_str());
        std::fs::create_dir_all(data_file.parent().unwrap()).unwrap();
        std::fs::write(&data_file, b"hello world").unwrap();
        std::fs::write(
            dir.path().join(format!("{}.meta", storage_path.as_str())),
            record.encode().unwrap(),
        )
        .unwrap();

        let report = rebuild_missing_object_rows(&meta, dir.path(), &generation).await;
        assert_eq!(report.inserted, 1, "{report:?}");
        let row = meta
            .get_version(&bucket.name, &key, &version_id)
            .await
            .unwrap()
            .expect("rebuilt row");
        assert_eq!(row.id, record.row_id);
        assert_eq!(row.etag, record.etag);
        assert_eq!(row.multipart_parts.as_deref(), Some(parts.as_slice()));
        assert_eq!(row.checksums, checksums);
    }

    #[test]
    fn an_implausible_trailer_length_is_skipped_without_allocating() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("object");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(b"payload").unwrap();
        // meta_len = 1 GiB, payload_len = 7, magic = CRNI.
        file.write_all(&(1024u32 * 1024 * 1024).to_le_bytes())
            .unwrap();
        file.write_all(&7u64.to_le_bytes()).unwrap();
        file.write_all(&cairn_types::storage::INLINE_TRAILER_MAGIC)
            .unwrap();
        file.flush().unwrap();
        assert!(read_inline_record(&path).unwrap().is_none());
    }
}
