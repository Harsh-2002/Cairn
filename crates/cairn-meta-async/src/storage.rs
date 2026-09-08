//! Storage protocol 2 transactions. Every function runs inside the canonical Writer savepoint.
//! Keep the SQL and outcomes identical in cairn-meta-async/src/storage.rs.

use crate::driver::{AsyncSqlDriver, Value as Cell};
use cairn_types::MetaError;
use cairn_types::id::{BucketName, StoragePath};
use cairn_types::meta::MutationOutcome;
use cairn_types::storage::{
    StorageAdmission, StorageCleanup, StorageMutation, StoragePathRole, StorageToken,
    StorageWritePlan, StorageWriteTarget,
};
use cairn_types::time::Timestamp;
type DB<'a> = dyn AsyncSqlDriver + 'a;

type R<T> = Result<T, MetaError>;
type Row = Vec<Cell>;

fn invalid(message: &str) -> MetaError {
    MetaError::Engine(message.to_owned())
}

fn text(value: &str) -> Cell {
    Cell::Text(value.to_owned())
}

fn string(row: &Row, index: usize) -> R<&str> {
    match row.get(index) {
        Some(Cell::Text(value)) => Ok(value),
        _ => Err(invalid("invalid storage journal text")),
    }
}

fn optional_string(row: &Row, index: usize) -> R<Option<&str>> {
    match row.get(index) {
        Some(Cell::Null) => Ok(None),
        Some(Cell::Text(value)) => Ok(Some(value)),
        _ => Err(invalid("invalid optional storage journal text")),
    }
}

async fn current(db: &DB<'_>, generation: &StorageToken) -> R<bool> {
    Ok(!q(
        db,
        "SELECT 1 FROM storage_recovery_state WHERE singleton=1 AND generation=?1",
        vec![text(generation.as_str())],
    )
    .await?
    .is_empty())
}

pub async fn begin(db: &DB<'_>, generation: &StorageToken) -> R<MutationOutcome> {
    // The caller owns the node lock and has drained the previous backend. Generation alone is
    // not proof that a live process or outstanding kernel operation has stopped.
    x(
        db,
        "UPDATE storage_recovery_state SET generation=?1 WHERE singleton=1",
        vec![text(generation.as_str())],
    )
    .await?;
    x(
        db,
        "UPDATE storage_cleanups SET claim_token=NULL,claim_generation=NULL,lease_until=NULL",
        vec![],
    )
    .await?;
    Ok(MutationOutcome::Ack)
}

pub async fn apply(
    db: &DB<'_>,
    bucket: &BucketName,
    operation: StorageMutation,
) -> R<MutationOutcome> {
    let applied = match operation {
        StorageMutation::Reserve { plan, now } => {
            if !matches!(plan.target, StorageWriteTarget::Object { .. }) {
                return Err(invalid("multipart storage admission requires its reserve or claim"));
            }
            return Ok(MutationOutcome::StorageAdmission(reserve(db, bucket, *plan, now).await?));
        }
        StorageMutation::Cancel { attempt, generation } => {
            current(db, &generation).await? && x(db,
                "UPDATE storage_write_intents SET cancelled=1 WHERE attempt_id=?1 AND generation=?2 AND bucket_name=?3",
                vec![text(attempt.as_str()),text(generation.as_str()),text(bucket.as_str())]).await? != 0
        }
        StorageMutation::Resolve { quiescence } => {
            if !current(db, quiescence.generation()).await? {
                false
            } else if let Some(plan) = load(db, bucket, quiescence.attempt(), quiescence.generation()).await? {
                resolve(db, &plan).await?;
                true
            } else {
                false
            }
        }
        StorageMutation::FinishCleanup { cleanup, now } => {
            if &cleanup.bucket != bucket || !current(db, &cleanup.generation).await? {
                false
            } else {
                x(db, "DELETE FROM storage_cleanups WHERE id=?1 AND bucket_name=?2 AND storage_path=?3 AND claim_token=?4 AND claim_generation=?5 AND lease_until=?6 AND lease_until>=?7 AND quota_debt_id IS ?8",
                    vec![text(cleanup.id.as_str()),text(bucket.as_str()),text(cleanup.path.as_str()),text(cleanup.claim_token.as_str()),text(cleanup.generation.as_str()),Cell::Int(cleanup.lease_until.0),Cell::Int(now.0),cleanup.quota_debt_id.as_deref().map_or(Cell::Null,text)]).await? != 0
            }
        }
    };
    Ok(MutationOutcome::StorageUpdated { applied })
}

/// Shared admission prefix for ordinary writes and joint multipart reserve/claim transactions.
pub async fn reserve(
    db: &DB<'_>,
    bucket: &BucketName,
    plan: StorageWritePlan,
    now: Timestamp,
) -> R<StorageAdmission> {
    plan.validate()?;
    if &plan.bucket != bucket {
        return Err(invalid("storage admission routing mismatch"));
    }
    if !current(db, &plan.generation).await?
        || q(
            db,
            "SELECT 1 FROM buckets WHERE name=?1",
            vec![text(bucket.as_str())],
        )
        .await?
        .is_empty()
        || !q(
            db,
            "SELECT 1 FROM storage_write_intents WHERE attempt_id=?1",
            vec![text(plan.attempt.as_str())],
        )
        .await?
        .is_empty()
    {
        return Ok(StorageAdmission::NotApplied);
    }
    for path in &plan.paths {
        if referenced(db, &path.path).await? || !q(db,
            "SELECT 1 FROM storage_intent_paths WHERE storage_path=?1 UNION ALL SELECT 1 FROM storage_cleanups WHERE storage_path=?1 LIMIT 1",
            vec![text(path.path.as_str())]).await?.is_empty()
        {
            return Ok(StorageAdmission::NotApplied);
        }
    }
    let encoded =
        serde_json::to_string(&plan).map_err(|error| MetaError::Engine(error.to_string()))?;
    x(db, "INSERT INTO storage_write_intents(attempt_id,generation,bucket_name,plan,created_at) VALUES (?1,?2,?3,?4,?5)",
        vec![text(plan.attempt.as_str()),text(plan.generation.as_str()),text(bucket.as_str()),Cell::Text(encoded),Cell::Int(now.0)]).await?;
    for path in &plan.paths {
        let role = match path.role {
            StoragePathRole::Temporary => "temporary",
            StoragePathRole::Final => "final",
            StoragePathRole::IndexSpool => "index_spool",
        };
        x(
            db,
            "INSERT INTO storage_intent_paths(attempt_id,role,storage_path) VALUES (?1,?2,?3)",
            vec![
                text(plan.attempt.as_str()),
                text(role),
                text(path.path.as_str()),
            ],
        )
        .await?;
    }
    Ok(StorageAdmission::Granted(Box::new(plan)))
}

async fn load(
    db: &DB<'_>,
    bucket: &BucketName,
    attempt: &StorageToken,
    generation: &StorageToken,
) -> R<Option<StorageWritePlan>> {
    q(db, "SELECT plan,attempt_id,generation,bucket_name FROM storage_write_intents WHERE attempt_id=?1 AND generation=?2 AND bucket_name=?3",
        vec![text(attempt.as_str()),text(generation.as_str()),text(bucket.as_str())]).await?
        .first().map(decode_plan).transpose()
}

fn decode_plan(row: &Row) -> R<StorageWritePlan> {
    let plan: StorageWritePlan = serde_json::from_str(string(row, 0)?)
        .map_err(|error| MetaError::Engine(error.to_string()))?;
    plan.validate()?;
    if plan.attempt.as_str() != string(row, 1)?
        || plan.generation.as_str() != string(row, 2)?
        || plan.bucket.as_str() != string(row, 3)?
    {
        return Err(invalid(
            "storage plan identity differs from its journal row",
        ));
    }
    Ok(plan)
}

async fn referenced(db: &DB<'_>, path: &StoragePath) -> R<bool> {
    Ok(!q(db,
        "SELECT 1 FROM object_versions WHERE storage_path=?1 UNION ALL SELECT 1 FROM multipart_parts WHERE storage_path=?1 LIMIT 1",
        vec![text(path.as_str())]).await?.is_empty())
}

/// Enqueue aliases even when rename/unlink probably removed them: retirement still needs the
/// containing namespace barrier. Reference checks are Writer-serialized, never stale WAL reads.
async fn resolve(db: &DB<'_>, plan: &StorageWritePlan) -> R<()> {
    for path in &plan.paths {
        if !referenced(db, &path.path).await? {
            enqueue(db, &plan.bucket, &path.path, None).await?;
        }
    }
    x(
        db,
        "DELETE FROM storage_intent_paths WHERE attempt_id=?1",
        vec![text(plan.attempt.as_str())],
    )
    .await?;
    x(
        db,
        "DELETE FROM storage_write_intents WHERE attempt_id=?1",
        vec![text(plan.attempt.as_str())],
    )
    .await?;
    Ok(())
}

pub async fn enqueue(
    db: &DB<'_>,
    bucket: &BucketName,
    path: &StoragePath,
    quota: Option<&str>,
) -> R<()> {
    cairn_types::storage::validate_storage_path(bucket, path)?;
    let existing = q(
        db,
        "SELECT bucket_name,quota_debt_id FROM storage_cleanups WHERE storage_path=?1",
        vec![text(path.as_str())],
    )
    .await?;
    if let Some(row) = existing.first() {
        if string(row, 0)? != bucket.as_str() || optional_string(row, 1)? != quota {
            return Err(invalid("conflicting exact storage cleanup ownership"));
        }
        return Ok(());
    }
    x(db, "INSERT INTO storage_cleanups(id,storage_path,bucket_name,quota_debt_id) VALUES (?1,?2,?3,?4)",
        vec![text(StorageToken::generate().as_str()),text(path.as_str()),text(bucket.as_str()),quota.map_or(Cell::Null,text)]).await?;
    Ok(())
}

pub async fn recover(db: &DB<'_>, generation: &StorageToken, limit: u32) -> R<MutationOutcome> {
    if !current(db, generation).await? {
        return Ok(MutationOutcome::StorageRecovered(0));
    }
    let plans = q(db, "SELECT plan,attempt_id,generation,bucket_name FROM storage_write_intents WHERE generation<>?1 ORDER BY generation,attempt_id LIMIT ?2",
        vec![text(generation.as_str()),Cell::Int(i64::from(limit.clamp(1,1000)))]).await?;
    for row in &plans {
        resolve(db, &decode_plan(row)?).await?;
    }
    Ok(MutationOutcome::StorageRecovered(plans.len() as u32))
}

pub async fn claim(
    db: &DB<'_>,
    generation: &StorageToken,
    limit: u32,
    now: Timestamp,
    lease_secs: i64,
) -> R<MutationOutcome> {
    let until = lease_secs
        .checked_mul(1000)
        .filter(|duration| *duration > 0)
        .and_then(|duration| now.0.checked_add(duration))
        .ok_or_else(|| invalid("invalid storage cleanup lease"))?;
    if !current(db, generation).await? {
        return Ok(MutationOutcome::StorageCleanupBatch(Vec::new()));
    }
    let rows = q(db, "SELECT id,bucket_name,storage_path,quota_debt_id FROM storage_cleanups AS c WHERE (lease_until IS NULL OR lease_until<?1) AND NOT EXISTS (SELECT 1 FROM object_versions WHERE storage_path=c.storage_path) AND NOT EXISTS (SELECT 1 FROM multipart_parts WHERE storage_path=c.storage_path) AND NOT EXISTS (SELECT 1 FROM storage_intent_paths WHERE storage_path=c.storage_path) ORDER BY lease_until,id LIMIT ?2",
        vec![Cell::Int(now.0),Cell::Int(i64::from(limit.clamp(1,1000)))]).await?;
    let mut batch = Vec::with_capacity(rows.len());
    for row in rows {
        let cleanup = StorageCleanup {
            id: StorageToken::try_from(string(&row, 0)?.to_owned())?,
            bucket: BucketName::parse(string(&row, 1)?)
                .map_err(|_| invalid("invalid storage cleanup bucket"))?,
            path: StoragePath::from_string(string(&row, 2)?.to_owned()),
            quota_debt_id: optional_string(&row, 3)?.map(str::to_owned),
            claim_token: StorageToken::generate(),
            generation: generation.clone(),
            lease_until: Timestamp(until),
        };
        cairn_types::storage::validate_storage_path(&cleanup.bucket, &cleanup.path)?;
        x(db, "UPDATE storage_cleanups SET claim_token=?2,claim_generation=?3,lease_until=?4 WHERE id=?1",
            vec![text(cleanup.id.as_str()),text(cleanup.claim_token.as_str()),text(generation.as_str()),Cell::Int(until)]).await?;
        batch.push(cleanup);
    }
    Ok(MutationOutcome::StorageCleanupBatch(batch))
}

async fn x(db: &DB<'_>, sql: &str, parameters: Vec<Cell>) -> R<u64> {
    db.execute(sql, parameters).await
}

async fn q(db: &DB<'_>, sql: &str, parameters: Vec<Cell>) -> R<Vec<Row>> {
    Ok(db
        .query(sql, parameters)
        .await?
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|index| row.value(index).clone())
                .collect()
        })
        .collect())
}
