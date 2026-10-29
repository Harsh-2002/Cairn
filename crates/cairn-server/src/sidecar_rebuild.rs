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
            let sidecar = entry.path();
            if sidecar.extension().and_then(|e| e.to_str()) != Some("meta") {
                continue;
            }
            report.scanned += 1;
            match read_sidecar(&sidecar) {
                Ok(record) => records.push(record),
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(path = %sidecar.display(), %error, "unreadable object metadata sidecar");
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
                } else {
                    // A different row owns this version: the sidecar is a superseded generation.
                    report.skipped += 1;
                }
                continue;
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

fn read_sidecar(path: &Path) -> Result<ObjectMetadataRecord, MetaError> {
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
            lock_intent: record.lock_intent.clone(),
        },
        replication: Vec::new(),
    };
    match meta
        .submit(Mutation::PublishObjectWrite {
            plan: Box::new(plan),
            operation: Box::new(operation),
            index_spool: IndexSpoolState::NeverCreated,
            activity: None,
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
            lock_intent: record.lock_intent.clone(),
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
        // Replication intent is not part of the v1 sidecar; a rebuilt row is not a replica, and the
        // pre-crash tail may need a replication rescan. Tracked as a known limitation.
        replication_status: None,
        internal_sha256: record.internal_sha256.clone(),
        replicated_at: None,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}
