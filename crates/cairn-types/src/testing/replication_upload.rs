use crate::MetaError;
use crate::id::{BucketName, ReplicationClaimToken};
use crate::meta::MutationOutcome;
use crate::replication_upload::{
    RemoteMultipartUpload, ReplicationUploadBatch, ReplicationUploadMutation,
};
use crate::time::Timestamp;

type R<T> = Result<T, MetaError>;

fn lease(now: Timestamp, seconds: i64) -> R<i64> {
    if seconds <= 0 {
        return Err(MetaError::Engine("invalid remote cleanup lease".to_owned()));
    }
    seconds
        .checked_mul(1000)
        .and_then(|duration| now.0.checked_add(duration))
        .ok_or_else(|| MetaError::Engine("remote cleanup lease overflow".to_owned()))
}

pub(super) fn apply(
    db: &mut DB,
    bucket: &BucketName,
    operation: ReplicationUploadMutation,
) -> R<MutationOutcome> {
    let applied = match operation {
        ReplicationUploadMutation::Begin { upload, now } => {
            if &upload.destination.bucket != bucket
                || upload.upload_id.is_some()
                || upload.cleanup_token.is_some()
                || upload.lease_until.is_some()
                || upload.orphan_reported
                || upload.last_error.is_some()
            {
                return Err(MetaError::Engine(
                    "invalid initial remote upload record".to_owned(),
                ));
            }
            if !origin_owned(db, bucket, &upload.outbox_id, &upload.origin_token, now)? {
                false
            } else {
                insert(db, &upload)?;
                true
            }
        }
        ReplicationUploadMutation::RecordUploadId {
            id,
            origin_token,
            upload_id,
            now,
        } => {
            if upload_id.is_empty() || upload_id.len() > 8192 {
                return Err(MetaError::Engine(
                    "invalid remote upload receipt".to_owned(),
                ));
            }
            if let Some(mut row) = load(db, bucket, &id)? {
                if row.origin_token != origin_token {
                    false
                } else {
                    if row
                        .upload_id
                        .as_ref()
                        .is_some_and(|existing| existing != &upload_id)
                    {
                        return Err(MetaError::Conflict);
                    }
                    let owned = origin_owned(db, bucket, &row.outbox_id, &origin_token, now)?;
                    // Preserve a receipt arriving after cancellation: the caller errors only AFTER
                    // this write commits, and no part may follow an ownership rejection.
                    row.upload_id = Some(upload_id);
                    row.orphan_reported = false;
                    row.next_attempt_at = now;
                    save(db, &row)?;
                    owned
                }
            } else {
                false
            }
        }
        ReplicationUploadMutation::Retire { id, origin_token } => {
            if let Some(row) = load(db, bucket, &id)? {
                if row.origin_token == origin_token {
                    remove(db, &id)?;
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }
        ReplicationUploadMutation::RenewCleanup {
            id,
            cleanup_token,
            now,
            lease_secs,
        } => {
            let until = lease(now, lease_secs)?;
            if let Some(mut row) = load(db, bucket, &id)? {
                if owns_cleanup(&row, &cleanup_token, now) {
                    row.lease_until = Some(Timestamp(until));
                    save(db, &row)?;
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }
        ReplicationUploadMutation::SettleCleanup {
            id,
            cleanup_token,
            now,
            retry_at,
            error,
        } => {
            if let Some(mut row) = load(db, bucket, &id)? {
                if owns_cleanup(&row, &cleanup_token, now) {
                    if let Some(error) = error {
                        row.last_error = Some(error.chars().take(8192).collect());
                        row.next_attempt_at = retry_at;
                        row.cleanup_token = None;
                        row.lease_until = None;
                        save(db, &row)?;
                    } else {
                        remove(db, &id)?;
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }
    };
    Ok(MutationOutcome::ReplicationClaimUpdated { applied })
}

fn owns_cleanup(
    row: &RemoteMultipartUpload,
    token: &ReplicationClaimToken,
    now: Timestamp,
) -> bool {
    row.cleanup_token.as_ref() == Some(token) && row.lease_until.is_some_and(|until| until >= now)
}

pub(super) fn claim(
    db: &mut DB,
    limit: u32,
    now: Timestamp,
    lease_secs: i64,
) -> R<MutationOutcome> {
    let until = lease(now, lease_secs)?;
    let mut batch = ReplicationUploadBatch::default();
    for mut row in due(db, limit.clamp(1, 1000), now)? {
        if row.upload_id.is_none() {
            row.orphan_reported = true;
            row.last_error = Some("remote multipart initiation receipt unknown; destination lifecycle cleanup required".to_owned());
            batch.orphaned += 1;
        } else {
            row.cleanup_token = Some(ReplicationClaimToken::generate());
            row.lease_until = Some(Timestamp(until));
            batch.uploads.push(row.clone());
        }
        save(db, &row)?;
    }
    Ok(MutationOutcome::ReplicationUploadBatch(batch))
}
use super::State;
type DB = State;
fn load(db: &DB, bucket: &BucketName, id: &str) -> R<Option<RemoteMultipartUpload>> {
    Ok(db
        .replication_uploads
        .get(id)
        .filter(|row| &row.destination.bucket == bucket)
        .cloned())
}
fn origin_owned(
    db: &DB,
    bucket: &BucketName,
    id: &str,
    token: &ReplicationClaimToken,
    now: Timestamp,
) -> R<bool> {
    Ok(db.outbox.iter().any(|row| {
        row.id == id
            && &row.bucket == bucket
            && row.status == crate::meta::ReplicationStatus::Claimed
            && row.claim_token.as_ref() == Some(token)
            && row.lease_until.is_some_and(|until| until >= now)
    }))
}
fn insert(db: &mut DB, row: &RemoteMultipartUpload) -> R<()> {
    if db.replication_uploads.contains_key(&row.id) {
        return Err(MetaError::Conflict);
    }
    db.replication_uploads.insert(row.id.clone(), row.clone());
    Ok(())
}
fn save(db: &mut DB, row: &RemoteMultipartUpload) -> R<()> {
    db.replication_uploads.insert(row.id.clone(), row.clone());
    Ok(())
}
fn remove(db: &mut DB, id: &str) -> R<()> {
    db.replication_uploads.remove(id);
    Ok(())
}
fn due(db: &DB, limit: u32, now: Timestamp) -> R<Vec<RemoteMultipartUpload>> {
    let mut rows = Vec::new();
    for row in db.replication_uploads.values() {
        if !row.orphan_reported
            && row.next_attempt_at <= now
            && (row.cleanup_token.is_none() || row.lease_until.is_some_and(|until| until < now))
            && !origin_owned(
                db,
                &row.destination.bucket,
                &row.outbox_id,
                &row.origin_token,
                now,
            )?
        {
            rows.push(row.clone());
        }
    }
    rows.sort_by(|a, b| (a.next_attempt_at, &a.id).cmp(&(b.next_attempt_at, &b.id)));
    rows.truncate(limit as usize);
    Ok(rows)
}
