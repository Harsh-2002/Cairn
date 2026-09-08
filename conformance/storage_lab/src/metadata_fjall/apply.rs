//! The candidate implements only the versioned, owner-only capacity workload and fails closed
//! outside that declared surface. Every operation runs in its own bounded member overlay.
use super::{
    journal,
    kv::{self, Overlay, View, get, key},
    model::*,
    multipart, outbox,
};
use cairn_types::{
    storage::{StorageAdmission, StorageWriteTarget},
    *,
};

fn account(view: &mut Overlay<'_>, row: &ObjectVersionRow, sign: i128) -> Result<(), MetaError> {
    let mut stats = stats(view, &row.bucket)?;
    let mut principal = principal(view, &row.owner_id)?;
    stats.versions = change(stats.versions, sign)?;
    stats.logical = change(stats.logical, i128::from(row.size_logical) * sign)?;
    stats.physical = change(stats.physical, i128::from(row.size_physical) * sign)?;
    principal.logical = change(principal.logical, i128::from(row.size_logical) * sign)?;
    if sign > 0
        && let Some(quota) =
            get::<Option<u64>>(view, &key(QUOTA, &[row.bucket.as_str()]))?.flatten()
        && stats
            .logical
            .checked_add(stats.staged)
            .is_none_or(|total| total > quota)
    {
        return Err(MetaError::QuotaExceeded);
    }
    view.put(key(STATS, &[row.bucket.as_str()]), &stats)?;
    view.put(key(PRINCIPAL, &[&row.owner_id.0]), &principal)
}
fn visibility(
    view: &mut Overlay<'_>,
    bucket: &BucketName,
    before: bool,
    after: bool,
) -> Result<(), MetaError> {
    let mut stats = stats(view, bucket)?;
    stats.objects = change(stats.objects, i128::from(after) - i128::from(before))?;
    view.put(key(STATS, &[bucket.as_str()]), &stats)
}
fn check_condition(
    current: Option<&ObjectVersionRow>,
    condition: &Precondition,
) -> Result<(), MetaError> {
    let current = current.filter(|row| !row.is_delete_marker);
    if condition
        .if_match
        .as_ref()
        .is_some_and(|etag| current.is_none_or(|row| &row.etag != etag))
        || condition
            .if_none_match
            .as_ref()
            .is_some_and(|expected| match expected {
                IfNoneMatch::Any => current.is_some(),
                IfNoneMatch::ETag(etag) => current.is_some_and(|row| &row.etag == etag),
            })
    {
        return Err(MetaError::PreconditionFailed);
    }
    Ok(())
}
fn put(
    view: &mut Overlay<'_>,
    mut row: ObjectVersionRow,
    precondition: Precondition,
    initial_state: InitialObjectState,
    replication: Vec<OutboxEntry>,
) -> Result<MutationOutcome, MetaError> {
    let bucket = require_bucket(view, &row.bucket)?;
    if bucket.versioning != VersioningState::Enabled
        || row.version_id.is_null()
        || initial_state != InitialObjectState::default()
        || row.is_delete_marker
        || row.storage_path.is_none()
        || row.owner_id != bucket.owner_id
    {
        return Err(kv::error(
            "publication exceeds candidate versioned owner-only contract",
        ));
    }
    let previous_current = current(view, &row.bucket, &row.key)?;
    check_condition(previous_current.as_ref(), &precondition)?;
    let before = previous_current
        .as_ref()
        .is_some_and(|r| !r.is_delete_marker);
    let address = version_key(&row.bucket, &row.key, &row.version_id);
    if get::<Vec<u8>>(view, &key(ROW_ID, &[&row.id]))?.is_some_and(|old| old != address) {
        return Err(MetaError::Conflict);
    }
    let mut superseded = None;
    if let Some(previous) = version(view, &row.bucket, &row.key, &row.version_id)? {
        account(view, &previous, -1)?;
        view.remove(key(ROW_ID, &[&previous.id]))?;
        if previous.storage_path != row.storage_path
            && let Some(path) = previous.storage_path
        {
            view.remove(key(REFERENCE, &[path.as_str()]))?;
            journal::enqueue(view, &row.bucket, &path, None, None)?;
            superseded = Some(path);
        }
    }
    if let Some(mut previous) = previous_current
        && previous.version_id != row.version_id
    {
        previous.is_latest = false;
        save_row(view, previous)?;
    }
    let path = row.storage_path.as_ref().ok_or(MetaError::Integrity)?;
    if get::<Vec<u8>>(view, &key(REFERENCE, &[path.as_str()]))?.is_some_and(|old| old != address) {
        return Err(MetaError::Conflict);
    }
    row.is_latest = true;
    if !replication.is_empty() && row.replication_status != Some(ReplicationStatus::Replica) {
        row.replication_status = Some(ReplicationStatus::Pending);
    }
    account(view, &row, 1)?;
    visibility(view, &row.bucket, before, true)?;
    view.put(key(REFERENCE, &[path.as_str()]), &address)?;
    view.put(key(ROW_ID, &[&row.id]), &address)?;
    view.put(
        key(CURRENT, &[row.bucket.as_str(), row.key.as_str()]),
        &row.version_id.as_str(),
    )?;
    view.put(
        key(CURRENT_LIST, &[row.bucket.as_str(), row.key.as_str()]),
        &Summary::from_row(&row),
    )?;
    let version_id = row.version_id.clone();
    save_row(view, row)?;
    // Late outbox conflicts must discard the object, index/counter changes and publication work.
    outbox::enqueue(view, &replication)?;
    Ok(MutationOutcome::Put {
        superseded,
        version_id,
    })
}
#[allow(clippy::too_many_arguments)]
fn marker(
    view: &mut Overlay<'_>,
    bucket: BucketName,
    object: ObjectKey,
    id: VersionId,
    owner: UserId,
    now: Timestamp,
    expected: Option<CurrentVersionGuard>,
    replication: Vec<OutboxEntry>,
) -> Result<MutationOutcome, MetaError> {
    let stored = require_bucket(view, &bucket)?;
    if stored.versioning != VersioningState::Enabled || id.is_null() || owner != stored.owner_id {
        return Err(kv::error("marker exceeds candidate workload contract"));
    }
    let previous = current(view, &bucket, &object)?;
    if let Some(expected) = expected
        && previous.as_ref().is_none_or(|row| {
            row.version_id != expected.version_id || row.updated_at != expected.updated_at
        })
    {
        return Ok(MutationOutcome::DeleteNotApplied);
    }
    if version(view, &bucket, &object, &id)?.is_some() {
        return Err(MetaError::Conflict);
    }
    let before = previous.as_ref().is_some_and(|row| !row.is_delete_marker);
    if let Some(mut previous) = previous {
        previous.is_latest = false;
        save_row(view, previous)?;
    }
    let row = ObjectVersionRow {
        id: cairn_types::storage::StorageToken::generate()
            .as_str()
            .to_owned(),
        bucket: bucket.clone(),
        key: object.clone(),
        version_id: id.clone(),
        is_latest: true,
        is_delete_marker: true,
        size_logical: 0,
        size_physical: 0,
        etag: ETag::from_string(String::new()),
        content_type: String::new(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_path: None,
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: owner,
        user_metadata: vec![],
        acl: None,
        checksums: vec![],
        sse_descriptor: None,
        internal_sha256: None,
        replication_status: None,
        replicated_at: None,
        created_at: now,
        updated_at: now,
    };
    account(view, &row, 1)?;
    visibility(view, &bucket, before, false)?;
    view.put(key(ROW_ID, &[&row.id]), &version_key(&bucket, &object, &id))?;
    view.put(
        key(CURRENT, &[bucket.as_str(), object.as_str()]),
        &id.as_str(),
    )?;
    view.remove(key(CURRENT_LIST, &[bucket.as_str(), object.as_str()]))?;
    save_row(view, row)?;
    outbox::enqueue(view, &replication)?;
    Ok(MutationOutcome::DeleteMarker {
        version_id: id,
        freed: None,
    })
}
#[allow(clippy::too_many_arguments)]
fn delete(
    view: &mut Overlay<'_>,
    bucket: BucketName,
    object: ObjectKey,
    id: VersionId,
    expected_row: Option<String>,
    expected_updated: Option<Timestamp>,
    sole: bool,
) -> Result<MutationOutcome, MetaError> {
    let Some(row) = version(view, &bucket, &object, &id)? else {
        return Ok(MutationOutcome::DeleteNotApplied);
    };
    if expected_row
        .as_ref()
        .is_some_and(|expected| expected != &row.id)
        || expected_updated.is_some_and(|expected| expected != row.updated_at)
        || (sole
            && view
                .scan(
                    &key(VERSION_LIST, &[bucket.as_str(), object.as_str()]),
                    None,
                    2,
                )?
                .len()
                != 1)
    {
        return Ok(MutationOutcome::DeleteNotApplied);
    }
    account(view, &row, -1)?;
    view.remove(version_key(&bucket, &object, &id))?;
    view.remove(version_index(&bucket, &object, &id))?;
    view.remove(key(ROW_ID, &[&row.id]))?;
    let freed = row.storage_path.clone();
    if let Some(path) = &freed {
        view.remove(key(REFERENCE, &[path.as_str()]))?;
        journal::enqueue(view, &bucket, path, None, None)?;
    }
    let mut promoted_latest = false;
    if row.is_latest {
        view.remove(key(CURRENT, &[bucket.as_str(), object.as_str()]))?;
        view.remove(key(CURRENT_LIST, &[bucket.as_str(), object.as_str()]))?;
        let next = view.scan(
            &key(VERSION_LIST, &[bucket.as_str(), object.as_str()]),
            None,
            1,
        )?;
        let mut visible = false;
        if let Some((_, summary)) = next.first() {
            let summary = kv::decode::<Summary>(summary)?.into_summary();
            let mut promoted = version(view, &bucket, &object, &summary.version_id)?
                .ok_or(MetaError::Integrity)?;
            promoted.is_latest = true;
            visible = !promoted.is_delete_marker;
            view.put(
                key(CURRENT, &[bucket.as_str(), object.as_str()]),
                &promoted.version_id.as_str(),
            )?;
            if visible {
                view.put(
                    key(CURRENT_LIST, &[bucket.as_str(), object.as_str()]),
                    &Summary::from_row(&promoted),
                )?;
            }
            save_row(view, promoted)?;
            promoted_latest = true;
        }
        visibility(view, &bucket, !row.is_delete_marker, visible)?;
    }
    Ok(MutationOutcome::Deleted {
        freed,
        promoted_latest,
    })
}
pub fn apply(view: &mut Overlay<'_>, mutation: Mutation) -> Result<MutationOutcome, MetaError> {
    match mutation {
        Mutation::BeginStorageGeneration { generation } => journal::begin(view, generation),
        Mutation::CreateBucket(bucket) => {
            if bucket.versioning != VersioningState::Enabled
                || bucket.ownership_mode != OwnershipMode::BucketOwnerEnforced
            {
                return Err(kv::error("bucket exceeds candidate workload contract"));
            }
            if view.get(&key(BUCKET, &[bucket.name.as_str()]))?.is_some() {
                return Err(MetaError::Conflict);
            }
            view.put(key(STATS, &[bucket.name.as_str()]), &Stats::default())?;
            view.put(key(BUCKET, &[bucket.name.as_str()]), bucket.as_ref())?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::SetBucketQuota {
            bucket,
            quota_bytes,
        } => {
            require_bucket(view, &bucket)?;
            view.put(key(QUOTA, &[bucket.as_str()]), &quota_bytes)?;
            Ok(MutationOutcome::Ack)
        }
        Mutation::Storage { bucket, operation } => journal::apply(view, &bucket, operation),
        Mutation::AdmitStorageWrite {
            plan, operation, ..
        } => {
            plan.validate_admission(&operation)?;
            let StorageWriteTarget::Part { reservation_id, .. } = &plan.target else {
                return Err(kv::error("candidate completion admission is unsupported"));
            };
            let reservation_id = reservation_id.clone();
            let outcome = journal::reserve(view, &plan.bucket, (*plan).clone())?;
            if matches!(
                outcome,
                MutationOutcome::StorageAdmission(StorageAdmission::NotApplied)
            ) {
                return Ok(outcome);
            }
            let Mutation::ReserveMultipartPart {
                upload_id,
                part_number,
                attempt_id,
                reserved_bytes,
                max_parts_per_upload,
                now,
            } = *operation
            else {
                return Err(MetaError::Integrity);
            };
            multipart::reserve(
                view,
                upload_id,
                part_number,
                attempt_id,
                reserved_bytes,
                max_parts_per_upload,
                now,
            )?;
            multipart::remember_plan(view, &reservation_id, &plan)?;
            Ok(outcome)
        }
        Mutation::PublishStorageWrite { plan, operation } => {
            plan.validate_publication(&operation)?;
            if !journal::publication_allowed(view, &plan)? {
                return Ok(MutationOutcome::StoragePublicationNotApplied);
            }
            let outcome = match *operation {
                Mutation::PutObjectVersion {
                    row,
                    precondition,
                    initial_state,
                    replication,
                } => put(view, *row, precondition, initial_state, replication)?,
                Mutation::RecordPart {
                    upload_id,
                    attempt_id,
                    part,
                } => multipart::record(view, upload_id, attempt_id, part)?,
                _ => return Err(kv::error("candidate publication shape is unsupported")),
            };
            journal::resolve(view, &plan)?;
            Ok(outcome)
        }
        Mutation::ResolveObjectWrite {
            bucket,
            key: object,
            version_id,
            row_id,
            storage_path,
        } => Ok(MutationOutcome::ObjectWriteResolved {
            referenced: version(view, &bucket, &object, &version_id)?.is_some_and(|row| {
                row.id == row_id && row.storage_path.as_ref() == Some(&storage_path)
            }),
        }),
        Mutation::ClaimStorageCleanup {
            generation,
            limit,
            now,
            lease_secs,
        } => journal::claim(view, generation, limit, now, lease_secs),
        Mutation::CreateDeleteMarker {
            bucket,
            key,
            version_id,
            owner_id,
            now,
            expected_current,
            replication,
            ..
        } => marker(
            view,
            bucket,
            key,
            version_id,
            owner_id,
            now,
            expected_current,
            replication,
        ),
        Mutation::DeleteVersion {
            bucket,
            key,
            version_id,
            expected_row_id,
            expected_updated_at,
            require_sole_key_version,
            ..
        } => delete(
            view,
            bucket,
            key,
            version_id,
            expected_row_id,
            expected_updated_at,
            require_sole_key_version,
        ),
        Mutation::CreateMultipart { session, limits } => multipart::create(view, *session, limits),
        Mutation::ResolveMultipartPartWrite {
            upload_id,
            part_number,
            storage_path,
        } => Ok(MutationOutcome::MultipartPartWriteResolved {
            referenced: get::<Part>(view, &multipart::part_key(&upload_id, part_number))?
                .is_some_and(|part| part.0.storage_path == storage_path),
        }),
        Mutation::AbortMultipart(upload) => multipart::abort(view, upload),
        Mutation::ClaimReplicationBatch {
            limit,
            now,
            lease_secs,
        } => outbox::claim(view, limit, now, lease_secs),
        Mutation::MarkReplicationDone {
            claim_token,
            id,
            now,
        } => outbox::done(view, id, claim_token, now),
        Mutation::PruneReplicationOutbox { before_ms } => outbox::prune(view, before_ms),
        _ => Err(kv::error(
            "mutation is outside the isolated Fjall workload contract",
        )),
    }
}
