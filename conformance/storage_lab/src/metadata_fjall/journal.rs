//! Exact intent/alias/claim and multipart charge linkage, inside a staged writer member.
use super::{
    kv::{self, Overlay, View, get, key},
    model::*,
};
use cairn_types::{
    storage::{
        StorageAdmission, StorageCleanup, StorageMutation, StorageToken, StorageWritePlan,
        StorageWriteTarget,
    },
    *,
};

use cairn_types::meta::MultipartCleanup;

const QUOTA_PATH: u8 = 31;
fn generation(view: &dyn View) -> Result<Option<StorageToken>, MetaError> {
    get(view, &key(META, &["generation"]))
}
fn ready_key(debt: &Debt) -> Vec<u8> {
    time_key(
        DEBT_READY,
        debt.claim.as_ref().map_or(i64::MIN, |c| c.until.0),
        debt.id.as_str(),
    )
}
fn referenced(view: &dyn View, path: &StoragePath) -> Result<bool, MetaError> {
    Ok(view.get(&key(REFERENCE, &[path.as_str()]))?.is_some())
}
fn intent_path(view: &dyn View, path: &StoragePath) -> Result<bool, MetaError> {
    Ok(view.get(&key(INTENT_PATH, &[path.as_str()]))?.is_some())
}
fn refresh(view: &mut Overlay<'_>, debt: &Debt) -> Result<(), MetaError> {
    view.remove(ready_key(debt))?;
    if !referenced(view, &debt.path)? && !intent_path(view, &debt.path)? {
        view.put(ready_key(debt), &debt.id)?;
    }
    Ok(())
}
fn save(view: &mut Overlay<'_>, debt: &Debt) -> Result<(), MetaError> {
    view.put(key(DEBT, &[debt.id.as_str()]), debt)?;
    refresh(view, debt)
}
fn debt_by_path(view: &dyn View, path: &StoragePath) -> Result<Option<Debt>, MetaError> {
    let id: Option<StorageToken> = get(view, &key(DEBT_PATH, &[path.as_str()]))?;
    id.map(|id| get(view, &key(DEBT, &[id.as_str()]))?.ok_or(MetaError::Integrity))
        .transpose()
}
fn add_link(view: &mut Overlay<'_>, id: &str) -> Result<(), MetaError> {
    let mut quota: QuotaDebt = get(view, &key(QUOTA_DEBT, &[id]))?.ok_or(MetaError::Integrity)?;
    quota.links = quota.links.checked_add(1).ok_or(MetaError::Integrity)?;
    view.put(key(QUOTA_DEBT, &[id]), &quota)
}
pub fn enqueue(
    view: &mut Overlay<'_>,
    bucket: &BucketName,
    path: &StoragePath,
    quota: Option<&str>,
    owner: Option<&str>,
) -> Result<(), MetaError> {
    cairn_types::storage::validate_storage_path(bucket, path)?;
    if let Some(old) = debt_by_path(view, path)? {
        if old.bucket != *bucket
            || old.quota_id.as_deref() != quota
            || old.quota_owner_path.as_deref() != owner
        {
            return Err(kv::error("conflicting candidate cleanup ownership"));
        }
        return Ok(());
    }
    let debt = Debt {
        id: StorageToken::generate(),
        bucket: bucket.clone(),
        path: path.clone(),
        quota_id: quota.map(str::to_owned),
        quota_owner_path: owner.map(str::to_owned),
        claim: None,
    };
    view.put(key(DEBT_PATH, &[path.as_str()]), &debt.id)?;
    if let Some(owner) = owner {
        view.put(key(OWNER_ALIAS, &[owner, debt.id.as_str()]), &debt.id)?;
    }
    if let Some(id) = quota {
        add_link(view, id)?;
    }
    save(view, &debt)
}
#[allow(clippy::too_many_arguments)]
pub fn quota_debt(
    view: &mut Overlay<'_>,
    bucket: &BucketName,
    owner: &UserId,
    upload: &UploadId,
    path: &StoragePath,
    bytes: u64,
    now: Timestamp,
    explicit_id: Option<String>,
) -> Result<MultipartCleanup, MetaError> {
    if view.get(&key(QUOTA_PATH, &[path.as_str()]))?.is_some() {
        return Err(MetaError::Conflict);
    }
    let id =
        explicit_id.unwrap_or_else(|| format!("storage:{}", StorageToken::generate().as_str()));
    if view.get(&key(QUOTA_DEBT, &[&id]))?.is_some() {
        return Err(MetaError::Conflict);
    }
    let quota = QuotaDebt {
        id: id.clone(),
        bucket: bucket.clone(),
        principal: owner.clone(),
        upload: upload.clone(),
        path: path.as_str().to_owned(),
        bytes,
        links: 0,
        created_at: now,
    };
    view.put(key(QUOTA_DEBT, &[&id]), &quota)?;
    view.put(key(QUOTA_PATH, &[path.as_str()]), &id)?;
    let aliases = view.scan(&key(OWNER_ALIAS, &[path.as_str()]), None, kv::PAGE)?;
    if aliases.len() == kv::PAGE {
        return Err(kv::error("candidate part alias bound exceeded"));
    }
    for (_, value) in aliases {
        let token: StorageToken = kv::decode(&value)?;
        let mut debt: Debt =
            get(view, &key(DEBT, &[token.as_str()]))?.ok_or(MetaError::Integrity)?;
        if debt.bucket != *bucket || debt.quota_id.is_some() {
            return Err(MetaError::Integrity);
        }
        view.remove(ready_key(&debt))?;
        debt.quota_id = Some(id.clone());
        debt.claim = None;
        add_link(view, &id)?;
        save(view, &debt)?;
    }
    enqueue(view, bucket, path, Some(&id), Some(path.as_str()))?;
    Ok(MultipartCleanup {
        id,
        upload_id: upload.clone(),
        bucket: bucket.clone(),
        principal_id: owner.clone(),
        bytes,
        storage_path: Some(path.clone()),
        created_at: now,
    })
}
pub fn begin(view: &mut Overlay<'_>, token: StorageToken) -> Result<MutationOutcome, MetaError> {
    let mut after = None;
    loop {
        let rows = view.scan(&[DEBT], after.as_deref(), kv::PAGE)?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|(k, _)| k.clone());
        for (_, value) in rows {
            let mut debt: Debt = kv::decode(&value)?;
            view.remove(ready_key(&debt))?;
            debt.claim = None;
            save(view, &debt)?;
        }
    }
    view.put(key(META, &["generation"]), &token)?;
    Ok(MutationOutcome::Ack)
}
pub fn reserve(
    view: &mut Overlay<'_>,
    bucket: &BucketName,
    plan: StorageWritePlan,
) -> Result<MutationOutcome, MetaError> {
    plan.validate()?;
    require_bucket(view, bucket)?;
    if plan.bucket != *bucket || generation(view)?.as_ref() != Some(&plan.generation) {
        return Ok(MutationOutcome::StorageAdmission(
            StorageAdmission::NotApplied,
        ));
    }
    if let Some(old) = get::<Intent>(view, &key(INTENT, &[plan.attempt.as_str()]))? {
        if old.plan == plan && !old.cancelled {
            return Ok(MutationOutcome::StorageAdmission(
                StorageAdmission::Granted(Box::new(plan)),
            ));
        }
        return Ok(MutationOutcome::StorageAdmission(
            StorageAdmission::NotApplied,
        ));
    }
    for path in &plan.paths {
        if referenced(view, &path.path)?
            || intent_path(view, &path.path)?
            || debt_by_path(view, &path.path)?.is_some()
        {
            return Ok(MutationOutcome::StorageAdmission(
                StorageAdmission::NotApplied,
            ));
        }
    }
    for path in &plan.paths {
        view.put(key(INTENT_PATH, &[path.path.as_str()]), &plan.attempt)?;
    }
    view.put(
        key(INTENT, &[plan.attempt.as_str()]),
        &Intent {
            plan: plan.clone(),
            cancelled: false,
        },
    )?;
    Ok(MutationOutcome::StorageAdmission(
        StorageAdmission::Granted(Box::new(plan)),
    ))
}
pub fn publication_allowed(view: &dyn View, plan: &StorageWritePlan) -> Result<bool, MetaError> {
    if generation(view)?.as_ref() != Some(&plan.generation) {
        return Ok(false);
    }
    let Some(intent) = get::<Intent>(view, &key(INTENT, &[plan.attempt.as_str()]))? else {
        return Ok(false);
    };
    if intent.plan != *plan || intent.cancelled {
        return Ok(false);
    }
    for path in &plan.paths {
        if get::<StorageToken>(view, &key(INTENT_PATH, &[path.path.as_str()]))?.as_ref()
            != Some(&plan.attempt)
        {
            return Ok(false);
        }
    }
    Ok(true)
}
pub fn resolve(view: &mut Overlay<'_>, plan: &StorageWritePlan) -> Result<(), MetaError> {
    let owner = matches!(plan.target, StorageWriteTarget::Part { .. })
        .then(|| plan.final_path().map(|p| p.as_str().to_owned()))
        .transpose()?;
    let mut quota = None;
    if let StorageWriteTarget::Part {
        upload_id,
        reservation_id,
        ..
    } = &plan.target
        && !referenced(view, plan.final_path()?)?
    {
        if let Some(reservation) = get::<Reservation>(view, &key(RESERVATION, &[reservation_id]))? {
            if reservation.upload != *upload_id || reservation.bucket != plan.bucket {
                return Err(MetaError::Integrity);
            }
            let debt = quota_debt(
                view,
                &plan.bucket,
                &reservation.principal,
                upload_id,
                plan.final_path()?,
                reservation.bytes,
                reservation.created_at,
                None,
            )?;
            quota = Some(debt.id);
            super::multipart::remove_reservation(view, &reservation)?;
        } else {
            quota = get(view, &key(QUOTA_PATH, &[plan.final_path()?.as_str()]))?;
            if quota.is_none() {
                return Err(kv::error("candidate part resolution lost exact quota debt"));
            }
        }
    }
    for path in &plan.paths {
        if !referenced(view, &path.path)? {
            enqueue(
                view,
                &plan.bucket,
                &path.path,
                quota.as_deref(),
                owner.as_deref(),
            )?;
        }
    }
    view.remove(key(INTENT, &[plan.attempt.as_str()]))?;
    for path in &plan.paths {
        view.remove(key(INTENT_PATH, &[path.path.as_str()]))?;
        if let Some(debt) = debt_by_path(view, &path.path)? {
            refresh(view, &debt)?;
        }
    }
    Ok(())
}
pub fn claim(
    view: &mut Overlay<'_>,
    token: StorageToken,
    limit: u32,
    now: Timestamp,
    lease_secs: i64,
) -> Result<MutationOutcome, MetaError> {
    let until = claim_until(now, lease_secs)?;
    if generation(view)?.as_ref() != Some(&token) {
        return Ok(MutationOutcome::StorageCleanupBatch(Vec::new()));
    }
    if limit == 0 || limit as usize > kv::PAGE {
        return Err(kv::error("candidate cleanup claim exceeds bound"));
    }
    let rows = view.scan(&[DEBT_READY], None, limit as usize)?;
    let mut output = Vec::with_capacity(rows.len());
    for (_, value) in rows {
        let id: StorageToken = kv::decode(&value)?;
        let mut debt: Debt = get(view, &key(DEBT, &[id.as_str()]))?.ok_or(MetaError::Integrity)?;
        if debt.claim.as_ref().is_some_and(|claim| claim.until >= now) {
            break;
        }
        if referenced(view, &debt.path)? || intent_path(view, &debt.path)? {
            return Err(kv::error("candidate eligible cleanup index is corrupt"));
        }
        view.remove(ready_key(&debt))?;
        let claim = Claim {
            token: StorageToken::generate(),
            generation: token.clone(),
            until,
        };
        let result = StorageCleanup {
            id: id.clone(),
            bucket: debt.bucket.clone(),
            path: debt.path.clone(),
            quota_debt_id: debt.quota_id.clone(),
            claim_token: claim.token.clone(),
            generation: token.clone(),
            lease_until: until,
        };
        debt.claim = Some(claim);
        save(view, &debt)?;
        output.push(result);
    }
    Ok(MutationOutcome::StorageCleanupBatch(output))
}
fn finish(
    view: &mut Overlay<'_>,
    bucket: &BucketName,
    cleanup: StorageCleanup,
    now: Timestamp,
) -> Result<bool, MetaError> {
    let Some(debt) = get::<Debt>(view, &key(DEBT, &[cleanup.id.as_str()]))? else {
        return Ok(false);
    };
    let expected = Claim {
        token: cleanup.claim_token.clone(),
        generation: cleanup.generation.clone(),
        until: cleanup.lease_until,
    };
    if generation(view)?.as_ref() != Some(&cleanup.generation)
        || bucket != &cleanup.bucket
        || debt.bucket != *bucket
        || cleanup.lease_until < now
        || debt.path != cleanup.path
        || debt.quota_id != cleanup.quota_debt_id
        || debt.claim.as_ref() != Some(&expected)
        || referenced(view, &debt.path)?
        || intent_path(view, &debt.path)?
    {
        return Ok(false);
    }
    view.remove(ready_key(&debt))?;
    view.remove(key(DEBT, &[debt.id.as_str()]))?;
    view.remove(key(DEBT_PATH, &[debt.path.as_str()]))?;
    if let Some(owner) = &debt.quota_owner_path {
        view.remove(key(OWNER_ALIAS, &[owner, debt.id.as_str()]))?;
    }
    if let Some(id) = debt.quota_id {
        let mut quota: QuotaDebt =
            get(view, &key(QUOTA_DEBT, &[&id]))?.ok_or(MetaError::Integrity)?;
        quota.links = quota.links.checked_sub(1).ok_or(MetaError::Integrity)?;
        if quota.links == 0 && !intent_path(view, &StoragePath::from_string(quota.path.clone()))? {
            stage_delta(
                view,
                &quota.bucket,
                &quota.principal,
                -i128::from(quota.bytes),
            )?;
            view.remove(key(QUOTA_DEBT, &[&id]))?;
            view.remove(key(QUOTA_PATH, &[&quota.path]))?;
        } else {
            view.put(key(QUOTA_DEBT, &[&id]), &quota)?;
        }
    }
    Ok(true)
}
pub fn apply(
    view: &mut Overlay<'_>,
    bucket: &BucketName,
    operation: StorageMutation,
) -> Result<MutationOutcome, MetaError> {
    let applied = match operation {
        StorageMutation::Reserve { plan, .. } => return reserve(view, bucket, *plan),
        StorageMutation::Cancel {
            attempt,
            generation: expected,
        } => {
            let intent = get::<Intent>(view, &key(INTENT, &[attempt.as_str()]))?;
            if generation(view)?.as_ref() == Some(&expected)
                && let Some(mut intent) = intent
                && intent.plan.bucket == *bucket
                && intent.plan.generation == expected
            {
                intent.cancelled = true;
                view.put(key(INTENT, &[attempt.as_str()]), &intent)?;
                true
            } else {
                false
            }
        }
        StorageMutation::Resolve { quiescence } => {
            if generation(view)?.as_ref() == Some(quiescence.generation())
                && let Some(intent) =
                    get::<Intent>(view, &key(INTENT, &[quiescence.attempt().as_str()]))?
                && intent.plan.bucket == *bucket
                && &intent.plan.generation == quiescence.generation()
            {
                resolve(view, &intent.plan)?;
                true
            } else {
                false
            }
        }
        StorageMutation::ResolveRecovered {
            current_generation,
            quiescence,
        } => {
            if generation(view)?.as_ref() == Some(&current_generation)
                && &current_generation != quiescence.generation()
                && let Some(intent) =
                    get::<Intent>(view, &key(INTENT, &[quiescence.attempt().as_str()]))?
                && intent.plan.bucket == *bucket
                && &intent.plan.generation == quiescence.generation()
            {
                resolve(view, &intent.plan)?;
                true
            } else {
                false
            }
        }
        StorageMutation::FinishCleanup { cleanup, now } => finish(view, bucket, cleanup, now)?,
    };
    Ok(MutationOutcome::StorageUpdated { applied })
}
