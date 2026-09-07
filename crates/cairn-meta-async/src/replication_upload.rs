use cairn_types::MetaError;
use cairn_types::id::{BucketName, ReplicationClaimToken};
use cairn_types::meta::MutationOutcome;
use cairn_types::replication_upload::{
    RemoteMultipartUpload, ReplicationUploadBatch, ReplicationUploadMutation,
};
use cairn_types::time::Timestamp;

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

pub async fn apply(
    db: &DB<'_>,
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
            if !origin_owned(db, bucket, &upload.outbox_id, &upload.origin_token, now).await? {
                false
            } else {
                insert(db, &upload).await?;
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
            if let Some(mut row) = load(db, bucket, &id).await? {
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
                    let owned =
                        origin_owned(db, bucket, &row.outbox_id, &origin_token, now).await?;
                    // Preserve a receipt arriving after cancellation: the caller errors only AFTER
                    // this write commits, and no part may follow an ownership rejection.
                    row.upload_id = Some(upload_id);
                    row.orphan_reported = false;
                    row.next_attempt_at = now;
                    save(db, &row).await?;
                    owned
                }
            } else {
                false
            }
        }
        ReplicationUploadMutation::Retire { id, origin_token } => {
            if let Some(row) = load(db, bucket, &id).await? {
                if row.origin_token == origin_token {
                    remove(db, &id).await?;
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
            if let Some(mut row) = load(db, bucket, &id).await? {
                if owns_cleanup(&row, &cleanup_token, now) {
                    row.lease_until = Some(Timestamp(until));
                    save(db, &row).await?;
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
            if let Some(mut row) = load(db, bucket, &id).await? {
                if owns_cleanup(&row, &cleanup_token, now) {
                    if let Some(error) = error {
                        row.last_error = Some(error.chars().take(8192).collect());
                        row.next_attempt_at = retry_at;
                        row.cleanup_token = None;
                        row.lease_until = None;
                        save(db, &row).await?;
                    } else {
                        remove(db, &id).await?;
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

pub async fn claim(db: &DB<'_>, limit: u32, now: Timestamp, lease_secs: i64) -> R<MutationOutcome> {
    let until = lease(now, lease_secs)?;
    let mut batch = ReplicationUploadBatch::default();
    for mut row in due(db, limit.clamp(1, 1000), now).await? {
        if row.upload_id.is_none() {
            row.orphan_reported = true;
            row.last_error = Some("remote multipart initiation receipt unknown; destination lifecycle cleanup required".to_owned());
            batch.orphaned += 1;
        } else {
            row.cleanup_token = Some(ReplicationClaimToken::generate());
            row.lease_until = Some(Timestamp(until));
            batch.uploads.push(row.clone());
        }
        save(db, &row).await?;
    }
    Ok(MutationOutcome::ReplicationUploadBatch(batch))
}
use crate::driver::{AsyncSqlDriver, Row, Value};
type DB<'a> = dyn AsyncSqlDriver + 'a;
const COLS: &str = "id,outbox_id,origin_token,destination,upload_id,cleanup_token,lease_until,next_attempt_at,orphan_reported,last_error";
fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}
fn optional(value: Option<&str>) -> Value {
    value.map_or(Value::Null, text)
}
fn decode(row: &Row) -> R<RemoteMultipartUpload> {
    Ok(RemoteMultipartUpload {
        id: row.get_text(0),
        outbox_id: row.get_text(1),
        origin_token: ReplicationClaimToken::from_string(row.get_text(2)),
        destination: serde_json::from_str(&row.get_text(3))
            .map_err(|error| MetaError::Engine(error.to_string()))?,
        upload_id: row.get_opt_text(4),
        cleanup_token: row.get_opt_text(5).map(ReplicationClaimToken::from_string),
        lease_until: row.get_opt_i64(6).map(Timestamp),
        next_attempt_at: Timestamp(row.get_i64(7)),
        orphan_reported: row.get_i64(8) != 0,
        last_error: row.get_opt_text(9),
    })
}
async fn load(db: &DB<'_>, bucket: &BucketName, id: &str) -> R<Option<RemoteMultipartUpload>> {
    db.query(
        &format!("SELECT {COLS} FROM replication_uploads WHERE id=?1 AND bucket_name=?2"),
        vec![text(id), text(bucket.as_str())],
    )
    .await?
    .first()
    .map(decode)
    .transpose()
}
async fn origin_owned(
    db: &DB<'_>,
    bucket: &BucketName,
    id: &str,
    token: &ReplicationClaimToken,
    now: Timestamp,
) -> R<bool> {
    Ok(!db.query("SELECT 1 FROM replication_outbox WHERE id=?1 AND bucket_name=?2 AND status='claimed' AND claim_token=?3 AND lease_until>=?4",vec![text(id),text(bucket.as_str()),text(token.as_str()),Value::Int(now.0)]).await?.is_empty())
}
async fn insert(db: &DB<'_>, row: &RemoteMultipartUpload) -> R<()> {
    let destination = serde_json::to_string(&row.destination)
        .map_err(|error| MetaError::Engine(error.to_string()))?;
    db.execute("INSERT INTO replication_uploads(id,bucket_name,outbox_id,origin_token,destination,next_attempt_at) VALUES (?1,?2,?3,?4,?5,?6)", vec![text(&row.id),text(row.destination.bucket.as_str()),text(&row.outbox_id),text(row.origin_token.as_str()),Value::Text(destination),Value::Int(row.next_attempt_at.0)]).await?;
    Ok(())
}
async fn save(db: &DB<'_>, row: &RemoteMultipartUpload) -> R<()> {
    db.execute("UPDATE replication_uploads SET upload_id=?2,cleanup_token=?3,lease_until=?4,next_attempt_at=?5,orphan_reported=?6,last_error=?7 WHERE id=?1",vec![text(&row.id),optional(row.upload_id.as_deref()),optional(row.cleanup_token.as_ref().map(ReplicationClaimToken::as_str)),row.lease_until.map_or(Value::Null,|time|Value::Int(time.0)),Value::Int(row.next_attempt_at.0),Value::Int(i64::from(row.orphan_reported)),optional(row.last_error.as_deref())]).await?;
    Ok(())
}
async fn remove(db: &DB<'_>, id: &str) -> R<()> {
    db.execute(
        "DELETE FROM replication_uploads WHERE id=?1",
        vec![text(id)],
    )
    .await?;
    Ok(())
}
async fn due(db: &DB<'_>, limit: u32, now: Timestamp) -> R<Vec<RemoteMultipartUpload>> {
    db.query(&format!("SELECT {COLS} FROM replication_uploads AS u WHERE orphan_reported=0 AND next_attempt_at<=?1 AND (cleanup_token IS NULL OR lease_until<?1) AND NOT EXISTS (SELECT 1 FROM replication_outbox AS o WHERE o.id=u.outbox_id AND o.bucket_name=u.bucket_name AND o.status='claimed' AND o.claim_token=u.origin_token AND o.lease_until>=?1) ORDER BY next_attempt_at,id LIMIT ?2"),vec![Value::Int(now.0),Value::Int(i64::from(limit))]).await?.iter().map(decode).collect()
}
