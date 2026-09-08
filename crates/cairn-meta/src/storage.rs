//! Storage protocol 2 transactions. Every function runs inside the canonical Writer savepoint.
//! Keep the SQL and outcomes identical in cairn-meta-async/src/storage.rs.

use cairn_types::MetaError;
use cairn_types::id::{BucketName, StoragePath};
use cairn_types::meta::MutationOutcome;
use cairn_types::storage::{
    StorageAdmission, StorageCleanup, StorageMutation, StoragePathRole, StorageToken,
    StorageWritePlan, StorageWriteTarget,
};
use cairn_types::time::Timestamp;
use rusqlite::Connection as DB;
use rusqlite::types::Value as Cell;

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

fn integer(row: &Row, index: usize) -> R<i64> {
    match row.get(index) {
        Some(Cell::Integer(value)) => Ok(*value),
        _ => Err(invalid("invalid storage journal integer")),
    }
}

fn optional_string(row: &Row, index: usize) -> R<Option<&str>> {
    match row.get(index) {
        Some(Cell::Null) => Ok(None),
        Some(Cell::Text(value)) => Ok(Some(value)),
        _ => Err(invalid("invalid optional storage journal text")),
    }
}

fn current(db: &DB, generation: &StorageToken) -> R<bool> {
    Ok(!q(
        db,
        "SELECT 1 FROM storage_recovery_state WHERE singleton=1 AND generation=?1",
        vec![text(generation.as_str())],
    )?
    .is_empty())
}

pub fn cancel_bucket(db: &DB, bucket: &BucketName) -> R<()> {
    x(
        db,
        "UPDATE storage_write_intents SET cancelled=1 WHERE bucket_name=?1",
        vec![text(bucket.as_str())],
    )?;
    Ok(())
}

pub fn begin(db: &DB, generation: &StorageToken) -> R<MutationOutcome> {
    // The caller owns the node lock and has drained the previous backend. Generation alone is
    // not proof that a live process or outstanding kernel operation has stopped.
    x(
        db,
        "UPDATE storage_recovery_state SET generation=?1 WHERE singleton=1",
        vec![text(generation.as_str())],
    )?;
    x(
        db,
        "UPDATE storage_cleanups SET claim_token=NULL,claim_generation=NULL,lease_until=NULL",
        vec![],
    )?;
    Ok(MutationOutcome::Ack)
}

pub fn prepare_restore(db: &DB, generation: &StorageToken) -> R<MutationOutcome> {
    if x(
        db,
        "UPDATE storage_recovery_state SET generation=?1,coverage_state='incomplete',coverage_identity=NULL,baseline_completed_at=NULL WHERE singleton=1 AND generation IS NOT ?1",
        vec![text(generation.as_str())],
    )? != 1
    {
        return Err(invalid(
            "storage restore requires a fresh generation and recovery state",
        ));
    }
    x(
        db,
        "UPDATE storage_cleanups SET claim_token=NULL,claim_generation=NULL,lease_until=NULL",
        vec![],
    )?;
    Ok(MutationOutcome::Ack)
}

pub fn apply(db: &DB, bucket: &BucketName, operation: StorageMutation) -> R<MutationOutcome> {
    let applied = match operation {
        StorageMutation::Reserve { plan, now } => {
            if !matches!(plan.target, StorageWriteTarget::Object { .. }) {
                return Err(invalid(
                    "multipart storage admission requires its reserve or claim",
                ));
            }
            return Ok(MutationOutcome::StorageAdmission(reserve(
                db, bucket, *plan, now,
            )?));
        }
        StorageMutation::Cancel {
            attempt,
            generation,
        } => {
            current(db, &generation)?
                && x(
                    db,
                    "UPDATE storage_write_intents SET cancelled=1 WHERE attempt_id=?1 AND generation=?2 AND bucket_name=?3",
                    vec![
                        text(attempt.as_str()),
                        text(generation.as_str()),
                        text(bucket.as_str()),
                    ],
                )? != 0
        }
        StorageMutation::Resolve { quiescence } => {
            if !current(db, quiescence.generation())? {
                false
            } else if let Some(plan) =
                load(db, bucket, quiescence.attempt(), quiescence.generation())?
            {
                resolve(db, &plan)?;
                true
            } else {
                false
            }
        }
        StorageMutation::ResolveRecovered {
            current_generation,
            quiescence,
        } => {
            if current_generation == *quiescence.generation() || !current(db, &current_generation)?
            {
                false
            } else if let Some(plan) =
                load(db, bucket, quiescence.attempt(), quiescence.generation())?
            {
                resolve(db, &plan)?;
                true
            } else {
                false
            }
        }
        StorageMutation::FinishCleanup { cleanup, now } => {
            if &cleanup.bucket != bucket || !current(db, &cleanup.generation)? {
                false
            } else {
                let deleted = x(
                    db,
                    "DELETE FROM storage_cleanups WHERE id=?1 AND bucket_name=?2 AND storage_path=?3 AND claim_token=?4 AND claim_generation=?5 AND lease_until=?6 AND lease_until>=?7 AND quota_debt_id IS ?8",
                    vec![
                        text(cleanup.id.as_str()),
                        text(bucket.as_str()),
                        text(cleanup.path.as_str()),
                        text(cleanup.claim_token.as_str()),
                        text(cleanup.generation.as_str()),
                        Cell::Integer(cleanup.lease_until.0),
                        Cell::Integer(now.0),
                        cleanup.quota_debt_id.as_deref().map_or(Cell::Null, text),
                    ],
                )? != 0;
                if deleted && let Some(debt) = &cleanup.quota_debt_id {
                    retire_quota(db, debt)?;
                }
                deleted
            }
        }
    };
    Ok(MutationOutcome::StorageUpdated { applied })
}

fn multipart_identity(plan: &StorageWritePlan) -> (Option<&str>, Option<&str>) {
    match &plan.target {
        StorageWriteTarget::Part {
            upload_id,
            reservation_id,
            ..
        } => (Some(upload_id.as_str()), Some(reservation_id)),
        StorageWriteTarget::Completion { upload_id, .. } => (Some(upload_id.as_str()), None),
        StorageWriteTarget::Object { .. } => (None, None),
    }
}

/// Validate the normalized ownership index as well as the JSON plan. A missing or changed
/// path row must never turn a live writer's alias into reclaimable debt.
fn validate_index(db: &DB, plan: &StorageWritePlan) -> R<()> {
    let identity = q(
        db,
        "SELECT upload_id,reservation_id FROM storage_write_intents WHERE attempt_id=?1",
        vec![text(plan.attempt.as_str())],
    )?;
    let row = identity
        .first()
        .ok_or_else(|| invalid("missing storage intent identity"))?;
    let (upload, reservation) = multipart_identity(plan);
    if optional_string(row, 0)? != upload || optional_string(row, 1)? != reservation {
        return Err(invalid("storage intent multipart identity mismatch"));
    }

    let rows = q(
        db,
        "SELECT role,storage_path FROM storage_intent_paths WHERE attempt_id=?1",
        vec![text(plan.attempt.as_str())],
    )?;
    if rows.len() != plan.paths.len() {
        return Err(invalid("storage intent path count mismatch"));
    }
    for expected in &plan.paths {
        let role = match expected.role {
            StoragePathRole::Temporary => "temporary",
            StoragePathRole::Final => "final",
            StoragePathRole::IndexSpool => "index_spool",
        };
        let mut found = false;
        for row in &rows {
            if string(row, 0)? == role && string(row, 1)? == expected.path.as_str() {
                found = true;
            }
        }
        if !found {
            return Err(invalid("storage intent path identity mismatch"));
        }
    }
    Ok(())
}

pub fn owns_publication(db: &DB, plan: &StorageWritePlan) -> R<bool> {
    if !current(db, &plan.generation)? {
        return Ok(false);
    }
    let Some(stored) = load(db, &plan.bucket, &plan.attempt, &plan.generation)? else {
        return Ok(false);
    };
    if stored != *plan {
        return Ok(false);
    }
    Ok(!q(
        db,
        "SELECT 1 FROM storage_write_intents WHERE attempt_id=?1 AND cancelled=0",
        vec![text(plan.attempt.as_str())],
    )?
    .is_empty())
}

/// Only for a joint reserve/claim which has not returned an admission acknowledgement. There
/// cannot yet be physical work; removing this freshly inserted intent is part of that savepoint.
pub fn discard_unacknowledged(db: &DB, plan: &StorageWritePlan) -> R<()> {
    x(
        db,
        "DELETE FROM storage_intent_paths WHERE attempt_id=?1",
        vec![text(plan.attempt.as_str())],
    )?;
    x(
        db,
        "DELETE FROM storage_write_intents WHERE attempt_id=?1",
        vec![text(plan.attempt.as_str())],
    )?;
    Ok(())
}

pub fn consume_published(db: &DB, plan: &StorageWritePlan) -> R<()> {
    resolve(db, plan)
}

/// Shared admission prefix for ordinary writes and joint multipart reserve/claim transactions.
pub fn reserve(
    db: &DB,
    bucket: &BucketName,
    plan: StorageWritePlan,
    now: Timestamp,
) -> R<StorageAdmission> {
    plan.validate()?;
    if &plan.bucket != bucket {
        return Err(invalid("storage admission routing mismatch"));
    }
    if !current(db, &plan.generation)?
        || q(
            db,
            "SELECT 1 FROM buckets WHERE name=?1",
            vec![text(bucket.as_str())],
        )?
        .is_empty()
        || !q(
            db,
            "SELECT 1 FROM storage_write_intents WHERE attempt_id=?1",
            vec![text(plan.attempt.as_str())],
        )?
        .is_empty()
    {
        return Ok(StorageAdmission::NotApplied);
    }
    if let StorageWriteTarget::Part { upload_id, .. }
    | StorageWriteTarget::Completion { upload_id, .. } = &plan.target
    {
        if let Some(row) = q(
            db,
            "SELECT bucket_name FROM multipart_uploads WHERE id=?1",
            vec![text(upload_id.as_str())],
        )?
        .first()
            && string(row, 0)? != bucket.as_str()
        {
            return Err(invalid("storage multipart routing mismatch"));
        }
    }
    for path in &plan.paths {
        if referenced(db, &path.path)? || !q(db,
            "SELECT 1 FROM storage_intent_paths WHERE storage_path=?1 UNION ALL SELECT 1 FROM storage_cleanups WHERE storage_path=?1 LIMIT 1",
            vec![text(path.path.as_str())])?.is_empty()
        {
            return Ok(StorageAdmission::NotApplied);
        }
    }
    let encoded =
        serde_json::to_string(&plan).map_err(|error| MetaError::Engine(error.to_string()))?;
    let (upload, reservation) = multipart_identity(&plan);
    x(
        db,
        "INSERT INTO storage_write_intents(attempt_id,generation,bucket_name,plan,created_at,upload_id,reservation_id) VALUES (?1,?2,?3,?4,?5,?6,?7)",
        vec![
            text(plan.attempt.as_str()),
            text(plan.generation.as_str()),
            text(bucket.as_str()),
            Cell::Text(encoded),
            Cell::Integer(now.0),
            upload.map_or(Cell::Null, text),
            reservation.map_or(Cell::Null, text),
        ],
    )?;
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
        )?;
    }
    Ok(StorageAdmission::Granted(Box::new(plan)))
}

fn load(
    db: &DB,
    bucket: &BucketName,
    attempt: &StorageToken,
    generation: &StorageToken,
) -> R<Option<StorageWritePlan>> {
    let rows = q(
        db,
        "SELECT plan,attempt_id,generation,bucket_name FROM storage_write_intents WHERE attempt_id=?1 AND generation=?2 AND bucket_name=?3",
        vec![
            text(attempt.as_str()),
            text(generation.as_str()),
            text(bucket.as_str()),
        ],
    )?;
    let plan = rows.first().map(decode_plan).transpose()?;
    if let Some(plan) = &plan {
        validate_index(db, plan)?;
    }
    Ok(plan)
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

fn referenced(db: &DB, path: &StoragePath) -> R<bool> {
    Ok(!q(db,
        "SELECT 1 FROM object_versions WHERE storage_path=?1 UNION ALL SELECT 1 FROM multipart_parts WHERE storage_path=?1 LIMIT 1",
        vec![text(path.as_str())])?.is_empty())
}

/// Enqueue aliases even when rename/unlink probably removed them: retirement still needs the
/// containing namespace barrier. Reference checks are Writer-serialized, never stale WAL reads.
fn resolve(db: &DB, plan: &StorageWritePlan) -> R<()> {
    validate_index(db, plan)?;
    let owner = matches!(plan.target, StorageWriteTarget::Part { .. })
        .then(|| plan.final_path())
        .transpose()?;
    let quota = if let StorageWriteTarget::Part {
        upload_id,
        reservation_id,
        ..
    } = &plan.target
    {
        if referenced(db, plan.final_path()?)? {
            None
        } else {
            let reservation = q(
                db,
                "SELECT r.reserved_bytes,r.created_at,u.bucket_name,COALESCE(u.initiated_by,u.owner_id) FROM multipart_part_reservations r JOIN multipart_uploads u ON u.id=r.upload_id WHERE r.attempt_id=?1 AND r.upload_id=?2",
                vec![text(reservation_id), text(upload_id.as_str())],
            )?;
            if let Some(row) = reservation.first() {
                if string(row, 2)? != plan.bucket.as_str() {
                    return Err(invalid("storage reservation bucket mismatch"));
                }
                let debt = create_quota_debt(
                    db,
                    upload_id.as_str(),
                    &plan.bucket,
                    string(row, 3)?,
                    plan.final_path()?,
                    integer(row, 0)?,
                    integer(row, 1)?,
                )?;
                x(
                    db,
                    "DELETE FROM multipart_part_reservations WHERE attempt_id=?1 AND upload_id=?2",
                    vec![text(reservation_id), text(upload_id.as_str())],
                )?;
                Some(debt)
            } else {
                let debts = q(
                    db,
                    "SELECT id FROM multipart_staging_cleanups WHERE storage_path=?1 AND storage_protocol=2",
                    vec![text(plan.final_path()?.as_str())],
                )?;
                if debts.len() != 1 {
                    return Err(invalid("missing exact multipart storage quota debt"));
                }
                Some(string(&debts[0], 0)?.to_owned())
            }
        }
    } else {
        None
    };
    for path in &plan.paths {
        if !referenced(db, &path.path)? {
            enqueue_owned(db, &plan.bucket, &path.path, quota.as_deref(), owner)?;
        }
    }
    discard_unacknowledged(db, plan)
}

/// Move a part/reservation's existing charge; do not increment staged-byte counters again.
fn create_quota_debt(
    db: &DB,
    upload: &str,
    bucket: &BucketName,
    principal: &str,
    path: &StoragePath,
    bytes: i64,
    created_at: i64,
) -> R<String> {
    if bytes < 0 {
        return Err(invalid("negative multipart storage charge"));
    }
    let id = format!("storage:{}", StorageToken::generate().as_str());
    x(
        db,
        "INSERT INTO multipart_staging_cleanups(id,upload_id,bucket_name,principal_id,bytes,storage_path,created_at,storage_protocol) VALUES (?1,?2,?3,?4,?5,?6,?7,2)",
        vec![
            text(&id),
            text(upload),
            text(bucket.as_str()),
            text(principal),
            Cell::Integer(bytes),
            text(path.as_str()),
            Cell::Integer(created_at),
        ],
    )?;
    link_quota(db, bucket, path, &id)?;
    Ok(id)
}

/// Keep temporary aliases charged when an active part attempt is cancelled by terminal upload
/// ownership. Intent membership continues to block reclamation until actual backend quiescence.
fn attach_reservation_intent(
    db: &DB,
    bucket: &BucketName,
    reservation: &str,
    path: &StoragePath,
    debt: &str,
) -> R<()> {
    let rows = q(
        db,
        "SELECT plan,attempt_id,generation,bucket_name FROM storage_write_intents WHERE reservation_id=?1",
        vec![text(reservation)],
    )?;
    if rows.len() > 1 {
        return Err(invalid("duplicate storage reservation ownership"));
    }
    for row in rows {
        let plan = decode_plan(&row)?;
        validate_index(db, &plan)?;
        if &plan.bucket != bucket || plan.final_path()? != path {
            return Err(invalid("storage reservation path mismatch"));
        }
        for alias in &plan.paths {
            enqueue_owned(db, bucket, &alias.path, Some(debt), Some(path))?;
        }
    }
    Ok(())
}

/// Terminal upload accounting is exact and memory-bounded even for the maximum part count.
/// The caller deletes the session only after this savepoint has preserved every physical charge.
pub fn retire_multipart(
    db: &DB,
    upload: &str,
    bucket: &BucketName,
    principal: &str,
    now: i64,
) -> R<()> {
    let mut cursor = 0;
    loop {
        let rows = q(
            db,
            "SELECT part_number,size,storage_path FROM multipart_parts WHERE upload_id=?1 AND part_number>?2 ORDER BY part_number LIMIT 128",
            vec![text(upload), Cell::Integer(cursor)],
        )?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            cursor = integer(&row, 0)?;
            create_quota_debt(
                db,
                upload,
                bucket,
                principal,
                &StoragePath::from_string(string(&row, 2)?.to_owned()),
                integer(&row, 1)?,
                now,
            )?;
        }
    }
    let mut cursor = 0;
    loop {
        let rows = q(
            db,
            "SELECT attempt_id,part_number,reserved_bytes,created_at FROM multipart_part_reservations WHERE upload_id=?1 AND part_number>?2 ORDER BY part_number LIMIT 128",
            vec![text(upload), Cell::Integer(cursor)],
        )?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let reservation = string(&row, 0)?;
            cursor = integer(&row, 1)?;
            let path = StoragePath::from_string(format!(
                ".staging/multipart/{upload}/{cursor:05}-{reservation}"
            ));
            let debt = create_quota_debt(
                db,
                upload,
                bucket,
                principal,
                &path,
                integer(&row, 2)?,
                integer(&row, 3)?,
            )?;
            attach_reservation_intent(db, bucket, reservation, &path, &debt)?;
        }
    }
    x(
        db,
        "UPDATE storage_write_intents SET cancelled=1 WHERE upload_id=?1",
        vec![text(upload)],
    )?;
    Ok(())
}

/// Retire the sole quota charge only after its last exact physical row has been retired. The
/// intent check is independent defense against forgiving a charge with an active backend owner.
fn retire_quota(db: &DB, debt: &str) -> R<()> {
    let rows = q(
        db,
        "SELECT bucket_name,principal_id,bytes FROM multipart_staging_cleanups AS d WHERE id=?1 AND storage_protocol=2 AND NOT EXISTS (SELECT 1 FROM storage_cleanups WHERE quota_debt_id=d.id) AND NOT EXISTS (SELECT 1 FROM storage_intent_paths WHERE storage_path=d.storage_path)",
        vec![text(debt)],
    )?;
    if let Some(row) = rows.first() {
        let bytes = integer(row, 2)?;
        if bytes < 0 {
            return Err(invalid("negative multipart storage charge"));
        }
        crate::apply::adjust_multipart_stats(db, string(row, 0)?, string(row, 1)?, 0, -bytes)?;
        x(
            db,
            "DELETE FROM multipart_staging_cleanups WHERE id=?1 AND storage_protocol=2",
            vec![text(debt)],
        )?;
    }
    Ok(())
}

pub fn enqueue(db: &DB, bucket: &BucketName, path: &StoragePath, quota: Option<&str>) -> R<()> {
    enqueue_owned(db, bucket, path, quota, None)
}

fn enqueue_owned(
    db: &DB,
    bucket: &BucketName,
    path: &StoragePath,
    quota: Option<&str>,
    owner: Option<&StoragePath>,
) -> R<()> {
    cairn_types::storage::validate_storage_path(bucket, path)?;
    if let Some(owner) = owner {
        cairn_types::storage::validate_storage_path(bucket, owner)?;
    }
    let existing = q(
        db,
        "SELECT bucket_name,quota_debt_id,quota_owner_path FROM storage_cleanups WHERE storage_path=?1",
        vec![text(path.as_str())],
    )?;
    if let Some(row) = existing.first() {
        if string(row, 0)? != bucket.as_str()
            || optional_string(row, 1)? != quota
            || optional_string(row, 2)? != owner.map(StoragePath::as_str)
        {
            return Err(invalid("conflicting exact storage cleanup ownership"));
        }
        return Ok(());
    }
    x(
        db,
        "INSERT INTO storage_cleanups(id,storage_path,bucket_name,quota_debt_id,quota_owner_path) VALUES (?1,?2,?3,?4,?5)",
        vec![
            text(StorageToken::generate().as_str()),
            text(path.as_str()),
            text(bucket.as_str()),
            quota.map_or(Cell::Null, text),
            owner.map_or(Cell::Null, |p| text(p.as_str())),
        ],
    )?;
    Ok(())
}

/// Transfer all unfinished aliases to the existing byte charge when a part reference disappears.
/// Changed ownership invalidates the old claim and immediately permits a fresh exact claim;
/// retaining its lease would delay quota retirement even after physical cleanup succeeds.
pub fn link_quota(db: &DB, bucket: &BucketName, path: &StoragePath, debt: &str) -> R<()> {
    let conflicts = q(
        db,
        "SELECT 1 FROM storage_cleanups WHERE quota_owner_path=?1 AND (bucket_name<>?2 OR (quota_debt_id IS NOT NULL AND quota_debt_id<>?3)) LIMIT 1",
        vec![text(path.as_str()), text(bucket.as_str()), text(debt)],
    )?;
    if !conflicts.is_empty() {
        return Err(invalid("conflicting multipart alias quota"));
    }
    x(
        db,
        "UPDATE storage_cleanups SET quota_debt_id=?2,claim_token=NULL,claim_generation=NULL,lease_until=NULL WHERE quota_owner_path=?1 AND quota_debt_id IS NOT ?2",
        vec![text(path.as_str()), text(debt)],
    )?;
    enqueue_owned(db, bucket, path, Some(debt), Some(path))
}

pub fn recover(db: &DB, generation: &StorageToken, limit: u32) -> R<MutationOutcome> {
    if !current(db, generation)? {
        return Ok(MutationOutcome::StorageIntentBatch(Vec::new()));
    }
    let plans = q(
        db,
        "SELECT plan,attempt_id,generation,bucket_name FROM storage_write_intents WHERE generation<>?1 ORDER BY generation,attempt_id LIMIT ?2",
        vec![
            text(generation.as_str()),
            Cell::Integer(i64::from(limit.clamp(1, 1000))),
        ],
    )?;
    let mut batch = Vec::with_capacity(plans.len());
    for row in &plans {
        let plan = decode_plan(row)?;
        validate_index(db, &plan)?;
        batch.push(plan);
    }
    Ok(MutationOutcome::StorageIntentBatch(batch))
}

pub fn claim(
    db: &DB,
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
    if !current(db, generation)? {
        return Ok(MutationOutcome::StorageCleanupBatch(Vec::new()));
    }
    let rows = q(
        db,
        "SELECT id,bucket_name,storage_path,quota_debt_id FROM storage_cleanups AS c WHERE (lease_until IS NULL OR lease_until<?1) AND NOT EXISTS (SELECT 1 FROM object_versions WHERE storage_path=c.storage_path) AND NOT EXISTS (SELECT 1 FROM multipart_parts WHERE storage_path=c.storage_path) AND NOT EXISTS (SELECT 1 FROM storage_intent_paths WHERE storage_path=c.storage_path) ORDER BY lease_until,id LIMIT ?2",
        vec![
            Cell::Integer(now.0),
            Cell::Integer(i64::from(limit.clamp(1, 1000))),
        ],
    )?;
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
        x(
            db,
            "UPDATE storage_cleanups SET claim_token=?2,claim_generation=?3,lease_until=?4 WHERE id=?1",
            vec![
                text(cleanup.id.as_str()),
                text(cleanup.claim_token.as_str()),
                text(generation.as_str()),
                Cell::Integer(until),
            ],
        )?;
        batch.push(cleanup);
    }
    Ok(MutationOutcome::StorageCleanupBatch(batch))
}

fn x(db: &DB, sql: &str, parameters: Vec<Cell>) -> R<u64> {
    db.execute(sql, rusqlite::params_from_iter(parameters))
        .map(|changed| changed as u64)
        .map_err(crate::model::engine_err)
}

fn q(db: &DB, sql: &str, parameters: Vec<Cell>) -> R<Vec<Row>> {
    let mut statement = db.prepare_cached(sql).map_err(crate::model::engine_err)?;
    let columns = statement.column_count();
    statement
        .query_map(rusqlite::params_from_iter(parameters), |row| {
            (0..columns).map(|index| row.get(index)).collect()
        })
        .map_err(crate::model::engine_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(crate::model::engine_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::storage::PlannedStorageWrite;
    use cairn_types::storage::io::StorageIoWatch;
    use std::sync::Arc;

    fn restore_invalidates_coverage_contract(db: &DB) {
        crate::schema::run_migrations(db).unwrap();
        let generation = StorageToken::generate();
        let restored = StorageToken::generate();
        let bucket = BucketName::parse("restore-coverage-contract").unwrap();
        let path = StoragePath::from_string(".staging/11111111111111111111111111111111.tmp".into());
        begin(db, &generation).unwrap();
        enqueue_owned(db, &bucket, &path, None, None).unwrap();
        let claims = claim(db, &generation, 100, Timestamp(0), 60).unwrap();
        assert!(matches!(&claims, MutationOutcome::StorageCleanupBatch(batch) if batch.len() == 1));
        // v37 startup still rejects complete coverage. Inject the future-shaped tuple only
        // after opening this fixture to prove invalidation without widening that preflight.
        x(db, "UPDATE storage_recovery_state SET coverage_state='complete',coverage_identity=?1,baseline_completed_at=42", vec![text(StorageToken::generate().as_str())]).unwrap();
        let protocol = q(db, "SELECT * FROM storage_protocol", vec![]).unwrap();
        let coverage = q(db, "SELECT * FROM storage_recovery_state", vec![]).unwrap();
        let cleanup = q(db, "SELECT * FROM storage_cleanups", vec![]).unwrap();
        assert!(prepare_restore(db, &generation).is_err());
        assert_eq!(
            q(db, "SELECT * FROM storage_recovery_state", vec![]).unwrap(),
            coverage
        );
        assert_eq!(
            q(db, "SELECT * FROM storage_cleanups", vec![]).unwrap(),
            cleanup
        );
        assert_eq!(
            prepare_restore(db, &restored).unwrap(),
            MutationOutcome::Ack
        );
        assert_eq!(q(db, "SELECT generation,coverage_state,coverage_identity,baseline_completed_at FROM storage_recovery_state", vec![]).unwrap(),
            vec![vec![text(restored.as_str()), text("incomplete"), Cell::Null, Cell::Null]]);
        assert_eq!(
            q(
                db,
                "SELECT claim_token,claim_generation,lease_until FROM storage_cleanups",
                vec![]
            )
            .unwrap(),
            vec![vec![Cell::Null, Cell::Null, Cell::Null]]
        );
        assert_eq!(
            q(db, "SELECT * FROM storage_protocol", vec![]).unwrap(),
            protocol
        );
        let MutationOutcome::StorageCleanupBatch(old) = claims else {
            unreachable!()
        };
        let MutationOutcome::StorageCleanupBatch(new) =
            claim(db, &restored, 100, Timestamp(1), 60).unwrap()
        else {
            unreachable!()
        };
        assert_eq!(new.len(), 1);
        assert_eq!(new[0].id, old[0].id);
        assert_eq!(new[0].path, old[0].path);
        assert_ne!(new[0].claim_token, old[0].claim_token);
        let cleanup = q(db, "SELECT * FROM storage_cleanups", vec![]).unwrap();
        x(db, "DELETE FROM storage_recovery_state", vec![]).unwrap();
        assert!(prepare_restore(db, &StorageToken::generate()).is_err());
        assert_eq!(
            q(db, "SELECT * FROM storage_cleanups", vec![]).unwrap(),
            cleanup
        );
    }

    #[test]
    fn sqlite_restore_invalidates_coverage_and_claims() {
        restore_invalidates_coverage_contract(&DB::open_in_memory().unwrap());
    }

    fn matching_quota_preserves_claim(db: &DB) {
        crate::schema::run_migrations(db).unwrap();
        let bucket = BucketName::parse("storage-relinked-claim").unwrap();
        let generation = StorageToken::generate();
        let owner = StoragePath::from_string(".staging/multipart/00000000000000000000000000000000/00001-11111111111111111111111111111111".into());
        let alias =
            StoragePath::from_string(".staging/22222222222222222222222222222222.index.tmp".into());
        begin(db, &generation).unwrap();
        enqueue_owned(db, &bucket, &alias, None, Some(&owner)).unwrap();
        link_quota(db, &bucket, &owner, "debt").unwrap();
        let MutationOutcome::StorageCleanupBatch(batch) =
            claim(db, &generation, 100, Timestamp(0), 60).unwrap()
        else {
            panic!("cleanup batch expected");
        };
        assert_eq!(batch.len(), 2);
        link_quota(db, &bucket, &owner, "debt").unwrap();
        assert_eq!(
            claim(db, &generation, 100, Timestamp(1), 60).unwrap(),
            MutationOutcome::StorageCleanupBatch(Vec::new())
        );
        for cleanup in batch {
            assert_eq!(
                apply(
                    db,
                    &bucket,
                    StorageMutation::FinishCleanup {
                        cleanup,
                        now: Timestamp(2)
                    }
                )
                .unwrap(),
                MutationOutcome::StorageUpdated { applied: true }
            );
        }
    }

    #[test]
    fn sqlite_matching_quota_preserves_cleanup_claim() {
        matching_quota_preserves_claim(&DB::open_in_memory().unwrap());
    }

    async fn corruption_contract(db: &DB) {
        crate::schema::run_migrations(db).unwrap();
        let bucket = BucketName::parse("storage-index-contract").unwrap();
        crate::apply::apply(
            db,
            cairn_types::Mutation::CreateBucket(Box::new(cairn_types::Bucket {
                name: bucket.clone(),
                owner_id: cairn_types::UserId::generate(),
                created_at: Timestamp(0),
                versioning: cairn_types::VersioningState::Unversioned,
                ownership_mode: cairn_types::OwnershipMode::BucketOwnerEnforced,
                region: "us-east-1".into(),
                compression: None,
            })),
        )
        .unwrap();
        let generation = StorageToken::generate();
        begin(db, &generation).unwrap();
        for change in [
            "UPDATE storage_intent_paths SET storage_path='.staging/00000000000000000000000000000000.tmp' WHERE attempt_id=?1 AND role='temporary'",
            "DELETE FROM storage_intent_paths WHERE attempt_id=?1 AND role='index_spool'",
            "UPDATE storage_write_intents SET upload_id='unrelated-upload' WHERE attempt_id=?1",
            "UPDATE storage_write_intents SET plan=json_set(plan,'$.paths[0].path','.staging/ffffffffffffffffffffffffffffffff.tmp') WHERE attempt_id=?1",
        ] {
            let planned = PlannedStorageWrite::new(
                bucket.clone(),
                generation.clone(),
                StorageWriteTarget::Object {
                    key: cairn_types::ObjectKey::parse("key").unwrap(),
                    version_id: cairn_types::VersionId::null(),
                    row_id: StorageToken::generate().as_str().to_owned(),
                },
            )
            .unwrap();
            let plan = planned.plan();
            reserve(db, &bucket, plan.clone(), Timestamp(0)).unwrap();
            assert!(owns_publication(db, plan).unwrap());
            x(db, change, vec![text(plan.attempt.as_str())]).unwrap();
            assert!(
                owns_publication(db, plan).is_err(),
                "accepted inconsistent journal: {change}"
            );
            let (mut watch, lease) =
                StorageIoWatch::new(plan.attempt.clone(), generation.clone(), Arc::new(()));
            drop(lease);
            assert!(
                apply(
                    db,
                    &bucket,
                    StorageMutation::Resolve {
                        quiescence: watch.quiescent().await
                    }
                )
                .is_err()
            );
            assert!(
                q(db, "SELECT id FROM storage_cleanups", vec![])
                    .unwrap()
                    .is_empty()
            );
        }
    }
    #[tokio::test]
    async fn sqlite_storage_index_mismatch_blocks_publication_and_cleanup() {
        let conn = DB::open_in_memory().unwrap();
        corruption_contract(&conn).await;
    }
}
