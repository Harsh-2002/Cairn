//! In-memory journal semantics; only unfinished work occupies this map.

use super::State;
use crate::MetaError;
use crate::id::{BucketName, StoragePath};
use crate::meta::MutationOutcome;
use crate::storage::{
    StorageAdmission, StorageCleanup, StorageMutation, StorageToken, StorageWritePlan,
    StorageWriteTarget,
};
use crate::time::Timestamp;
use std::collections::{BTreeMap, BTreeSet};

type R<T> = Result<T, MetaError>;

#[derive(Clone, Default)]
pub(super) struct Journal {
    generation: Option<StorageToken>,
    intents: BTreeMap<StorageToken, (StorageWritePlan, bool)>,
    cleanups: BTreeMap<StorageToken, PendingCleanup>,
    exact_quota_debts: BTreeSet<String>,
}

#[derive(Clone)]
struct PendingCleanup {
    bucket: BucketName,
    path: StoragePath,
    quota: Option<String>,
    owner: Option<StoragePath>,
    claim: Option<StorageCleanup>,
}

fn invalid(message: &str) -> MetaError {
    MetaError::Engine(message.to_owned())
}

pub(super) fn begin(st: &mut State, generation: StorageToken) -> MutationOutcome {
    st.storage.generation = Some(generation);
    for row in st.storage.cleanups.values_mut() {
        row.claim = None;
    }
    MutationOutcome::Ack
}

pub(super) fn apply(
    st: &mut State,
    bucket: &BucketName,
    operation: StorageMutation,
) -> R<MutationOutcome> {
    // Savepoint isolation includes every pending path if a later conflict or validation fails.
    let mut journal = st.storage.clone();
    let outcome = match operation {
        StorageMutation::Reserve { plan, now: _ } => {
            if !matches!(plan.target, StorageWriteTarget::Object { .. }) {
                return Err(invalid(
                    "multipart storage admission requires its reserve or claim",
                ));
            }
            MutationOutcome::StorageAdmission(reserve(st, &mut journal, bucket, *plan)?)
        }
        StorageMutation::Cancel {
            attempt,
            generation,
        } => {
            let mut applied = false;
            if journal.generation.as_ref() == Some(&generation)
                && let Some((plan, cancelled)) = journal.intents.get_mut(&attempt)
                && &plan.bucket == bucket
                && plan.generation == generation
            {
                *cancelled = true;
                applied = true;
            }
            MutationOutcome::StorageUpdated { applied }
        }
        StorageMutation::Resolve { quiescence } => {
            let plan = journal
                .intents
                .get(quiescence.attempt())
                .filter(|(plan, _)| {
                    &plan.bucket == bucket && &plan.generation == quiescence.generation()
                })
                .map(|(plan, _)| plan.clone());
            let applied = if journal.generation.as_ref() == Some(quiescence.generation())
                && let Some(plan) = plan
            {
                resolve(st, &mut journal, &plan)?;
                true
            } else {
                false
            };
            MutationOutcome::StorageUpdated { applied }
        }
        StorageMutation::ResolveRecovered {
            current_generation,
            quiescence,
        } => {
            let plan = journal
                .intents
                .get(quiescence.attempt())
                .filter(|(plan, _)| {
                    &plan.bucket == bucket && &plan.generation == quiescence.generation()
                })
                .map(|(plan, _)| plan.clone());
            let applied = if journal.generation.as_ref() == Some(&current_generation)
                && &current_generation != quiescence.generation()
                && let Some(plan) = plan
            {
                resolve(st, &mut journal, &plan)?;
                true
            } else {
                false
            };
            MutationOutcome::StorageUpdated { applied }
        }
        StorageMutation::FinishCleanup { cleanup, now } => {
            let applied = journal.generation.as_ref() == Some(&cleanup.generation)
                && &cleanup.bucket == bucket
                && cleanup.lease_until >= now
                && journal.cleanups.get(&cleanup.id).is_some_and(|row| {
                    row.claim.as_ref() == Some(&cleanup) && row.quota == cleanup.quota_debt_id
                });
            if applied {
                journal.cleanups.remove(&cleanup.id);
                if let Some(debt) = &cleanup.quota_debt_id {
                    retire_quota(st, &mut journal, debt);
                }
            }
            MutationOutcome::StorageUpdated { applied }
        }
    };
    st.storage = journal;
    Ok(outcome)
}

pub(super) fn reserve_joint(
    st: &mut State,
    bucket: &BucketName,
    plan: StorageWritePlan,
    _now: Timestamp,
) -> R<StorageAdmission> {
    let mut journal = st.storage.clone();
    let admission = reserve(st, &mut journal, bucket, plan)?;
    st.storage = journal;
    Ok(admission)
}

pub(super) fn owns_publication(st: &State, plan: &StorageWritePlan) -> R<bool> {
    plan.validate()?;
    Ok(st.storage.generation.as_ref() == Some(&plan.generation)
        && st
            .storage
            .intents
            .get(&plan.attempt)
            .is_some_and(|(stored, cancelled)| stored == plan && !cancelled))
}

pub(super) fn discard_unacknowledged(st: &mut State, plan: &StorageWritePlan) {
    st.storage.intents.remove(&plan.attempt);
}

pub(super) fn consume_published(st: &mut State, plan: &StorageWritePlan) -> R<()> {
    let mut journal = st.storage.clone();
    resolve(st, &mut journal, plan)?;
    st.storage = journal;
    Ok(())
}

fn reserve(
    st: &State,
    journal: &mut Journal,
    bucket: &BucketName,
    plan: StorageWritePlan,
) -> R<StorageAdmission> {
    plan.validate()?;
    if &plan.bucket != bucket {
        return Err(invalid("storage admission routing mismatch"));
    }
    if journal.generation.as_ref() != Some(&plan.generation)
        || !st.buckets.contains_key(bucket.as_str())
        || journal.intents.contains_key(&plan.attempt)
    {
        return Ok(StorageAdmission::NotApplied);
    }
    if let StorageWriteTarget::Part { upload_id, .. }
    | StorageWriteTarget::Completion { upload_id, .. } = &plan.target
        && let Some(session) = st.multipart.get(upload_id.as_str())
        && &session.bucket != bucket
    {
        return Err(invalid("storage multipart routing mismatch"));
    }
    for path in &plan.paths {
        if referenced(st, &path.path)
            || journal
                .intents
                .values()
                .any(|(intent, _)| intent.paths.iter().any(|p| p.path == path.path))
            || journal.cleanups.values().any(|row| row.path == path.path)
        {
            return Ok(StorageAdmission::NotApplied);
        }
    }
    journal
        .intents
        .insert(plan.attempt.clone(), (plan.clone(), false));
    Ok(StorageAdmission::Granted(Box::new(plan)))
}

fn referenced(st: &State, path: &StoragePath) -> bool {
    st.versions
        .values()
        .any(|row| row.storage_path.as_ref() == Some(path))
        || st.parts.values().any(|row| &row.storage_path == path)
}

pub(super) fn enqueue(st: &mut State, bucket: &BucketName, path: &StoragePath) -> R<()> {
    enqueue_owned(&mut st.storage, bucket, path, None, None)
}

fn enqueue_owned(
    journal: &mut Journal,
    bucket: &BucketName,
    path: &StoragePath,
    quota: Option<String>,
    owner: Option<StoragePath>,
) -> R<()> {
    crate::storage::validate_storage_path(bucket, path)?;
    if let Some(owner) = &owner {
        crate::storage::validate_storage_path(bucket, owner)?;
    }
    if let Some(row) = journal.cleanups.values().find(|row| &row.path == path) {
        if &row.bucket != bucket || row.quota != quota || row.owner != owner {
            return Err(invalid("conflicting exact storage cleanup ownership"));
        }
        return Ok(());
    }
    journal.cleanups.insert(
        StorageToken::generate(),
        PendingCleanup {
            bucket: bucket.clone(),
            path: path.clone(),
            quota,
            owner,
            claim: None,
        },
    );
    Ok(())
}

fn link_quota_inner(
    journal: &mut Journal,
    bucket: &BucketName,
    path: &StoragePath,
    debt: &str,
) -> R<()> {
    for row in journal
        .cleanups
        .values_mut()
        .filter(|row| row.owner.as_ref() == Some(path))
    {
        if &row.bucket != bucket || row.quota.as_ref().is_some_and(|id| id != debt) {
            return Err(invalid("conflicting multipart alias quota"));
        }
        row.quota = Some(debt.to_owned());
    }
    journal.exact_quota_debts.insert(debt.to_owned());
    enqueue_owned(
        journal,
        bucket,
        path,
        Some(debt.to_owned()),
        Some(path.clone()),
    )
}

pub(super) fn link_quota(st: &mut State, debt: &crate::meta::MultipartCleanup) -> R<()> {
    let path = debt
        .storage_path
        .as_ref()
        .ok_or_else(|| invalid("missing superseded part path"))?;
    link_quota_inner(&mut st.storage, &debt.bucket, path, &debt.id)
}

fn create_quota_debt(
    st: &mut State,
    journal: &mut Journal,
    mut debt: crate::meta::MultipartCleanup,
) -> R<String> {
    debt.id = format!("storage:{}", StorageToken::generate().as_str());
    let path = debt
        .storage_path
        .as_ref()
        .ok_or_else(|| invalid("missing multipart charge path"))?;
    link_quota_inner(journal, &debt.bucket, path, &debt.id)?;
    let id = debt.id.clone();
    st.multipart_cleanups.insert(id.clone(), debt);
    Ok(id)
}

fn resolve(st: &mut State, journal: &mut Journal, plan: &StorageWritePlan) -> R<()> {
    let owner = matches!(plan.target, StorageWriteTarget::Part { .. })
        .then(|| plan.final_path().cloned())
        .transpose()?;
    let quota = if let StorageWriteTarget::Part {
        upload_id,
        reservation_id,
        ..
    } = &plan.target
    {
        if referenced(st, plan.final_path()?) {
            None
        } else if let Some(reservation) = st.multipart_reservations.get(reservation_id).cloned() {
            let session = st
                .multipart
                .get(upload_id.as_str())
                .cloned()
                .ok_or_else(|| invalid("missing storage reservation session"))?;
            if reservation.upload_id != *upload_id || session.bucket != plan.bucket {
                return Err(invalid("storage reservation bucket mismatch"));
            }
            let debt = create_quota_debt(
                st,
                journal,
                crate::meta::MultipartCleanup {
                    id: String::new(),
                    upload_id: upload_id.clone(),
                    bucket: plan.bucket.clone(),
                    principal_id: session.initiated_by,
                    bytes: reservation.reserved_bytes,
                    storage_path: Some(plan.final_path()?.clone()),
                    created_at: reservation.created_at,
                },
            )?;
            st.multipart_reservations.remove(reservation_id);
            Some(debt)
        } else {
            let debts: Vec<_> = st
                .multipart_cleanups
                .values()
                .filter(|debt| {
                    debt.storage_path.as_ref() == owner.as_ref()
                        && journal.exact_quota_debts.contains(&debt.id)
                })
                .collect();
            if debts.len() != 1 {
                return Err(invalid("missing exact multipart storage quota debt"));
            }
            Some(debts[0].id.clone())
        }
    } else {
        None
    };
    for path in &plan.paths {
        if !referenced(st, &path.path) {
            enqueue_owned(
                journal,
                &plan.bucket,
                &path.path,
                quota.clone(),
                owner.clone(),
            )?;
        }
    }
    journal.intents.remove(&plan.attempt);
    Ok(())
}

fn retire_quota(st: &mut State, journal: &mut Journal, debt: &str) {
    if journal.exact_quota_debts.contains(debt)
        && !journal
            .cleanups
            .values()
            .any(|row| row.quota.as_deref() == Some(debt))
        && st.multipart_cleanups.get(debt).is_some_and(|row| {
            !journal.intents.values().any(|(plan, _)| {
                plan.paths
                    .iter()
                    .any(|path| Some(&path.path) == row.storage_path.as_ref())
            })
        })
    {
        st.multipart_cleanups.remove(debt);
        journal.exact_quota_debts.remove(debt);
    }
}

pub(super) fn reservation_owned(st: &State, reservation: &str) -> bool {
    st.storage.intents.values().any(|(plan, _)| {
        matches!(&plan.target,
        StorageWriteTarget::Part { reservation_id, .. } if reservation_id == reservation)
    })
}

pub(super) fn exact_quota(st: &State, debt: &str) -> bool {
    st.storage.exact_quota_debts.contains(debt)
}

pub(super) fn cancel_bucket(st: &mut State, bucket: &BucketName) {
    for (plan, cancelled) in st.storage.intents.values_mut() {
        if &plan.bucket == bucket {
            *cancelled = true;
        }
    }
}

pub(super) fn retire_multipart(st: &mut State, session: &crate::meta::MultipartSession) -> R<()> {
    let mut journal = st.storage.clone();
    let parts: Vec<_> = st
        .parts
        .iter()
        .filter(|((upload, _), _)| upload == session.upload_id.as_str())
        .map(|(_, part)| part.clone())
        .collect();
    for part in parts {
        create_quota_debt(
            st,
            &mut journal,
            crate::meta::MultipartCleanup {
                id: String::new(),
                upload_id: session.upload_id.clone(),
                bucket: session.bucket.clone(),
                principal_id: session.initiated_by.clone(),
                bytes: part.size,
                storage_path: Some(part.storage_path),
                created_at: session.updated_at,
            },
        )?;
    }
    let reservations: Vec<_> = st
        .multipart_reservations
        .values()
        .filter(|row| row.upload_id == session.upload_id)
        .cloned()
        .collect();
    for reservation in reservations {
        let path = StoragePath::from_string(format!(
            ".staging/multipart/{}/{:05}-{}",
            session.upload_id, reservation.part_number, reservation.attempt_id
        ));
        let debt = create_quota_debt(
            st,
            &mut journal,
            crate::meta::MultipartCleanup {
                id: String::new(),
                upload_id: session.upload_id.clone(),
                bucket: session.bucket.clone(),
                principal_id: session.initiated_by.clone(),
                bytes: reservation.reserved_bytes,
                storage_path: Some(path.clone()),
                created_at: reservation.created_at,
            },
        )?;
        let plans: Vec<_> = journal.intents.values().filter(|(plan, _)| matches!(&plan.target,
            StorageWriteTarget::Part { reservation_id, .. } if reservation_id == &reservation.attempt_id)).map(|(plan, _)| plan.clone()).collect();
        if plans.len() > 1 {
            return Err(invalid("duplicate storage reservation ownership"));
        }
        for plan in plans {
            if plan.bucket != session.bucket || plan.final_path()? != &path {
                return Err(invalid("storage reservation path mismatch"));
            }
            for alias in plan.paths {
                enqueue_owned(
                    &mut journal,
                    &session.bucket,
                    &alias.path,
                    Some(debt.clone()),
                    Some(path.clone()),
                )?;
            }
        }
    }
    for (plan, cancelled) in journal.intents.values_mut() {
        if matches!(&plan.target, StorageWriteTarget::Part { upload_id, .. } | StorageWriteTarget::Completion { upload_id, .. } if upload_id == &session.upload_id)
        {
            *cancelled = true;
        }
    }
    st.storage = journal;
    Ok(())
}

pub(super) fn recover(st: &mut State, generation: &StorageToken, limit: u32) -> R<MutationOutcome> {
    if st.storage.generation.as_ref() != Some(generation) {
        return Ok(MutationOutcome::StorageIntentBatch(Vec::new()));
    }
    let journal = &st.storage;
    let mut plans: Vec<_> = journal
        .intents
        .values()
        .filter(|(plan, _)| &plan.generation != generation)
        .map(|(plan, _)| plan.clone())
        .collect();
    plans.sort_by(|a, b| (&a.generation, &a.attempt).cmp(&(&b.generation, &b.attempt)));
    plans.truncate(limit.clamp(1, 1000) as usize);
    Ok(MutationOutcome::StorageIntentBatch(plans))
}

pub(super) fn claim(
    st: &mut State,
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
    if st.storage.generation.as_ref() != Some(generation) {
        return Ok(MutationOutcome::StorageCleanupBatch(Vec::new()));
    }
    let journal = &st.storage;
    let mut due: Vec<_> = journal
        .cleanups
        .iter()
        .filter(|(_, row)| {
            row.claim
                .as_ref()
                .is_none_or(|claim| claim.lease_until < now)
                && !referenced(st, &row.path)
                && !journal
                    .intents
                    .values()
                    .any(|(plan, _)| plan.paths.iter().any(|path| path.path == row.path))
        })
        .map(|(id, row)| {
            (
                row.claim.as_ref().map(|claim| claim.lease_until),
                id.clone(),
            )
        })
        .collect();
    due.sort();
    due.truncate(limit.clamp(1, 1000) as usize);
    let mut batch = Vec::with_capacity(due.len());
    for (_, id) in due {
        let row = st.storage.cleanups.get_mut(&id).expect("selected cleanup");
        let cleanup = StorageCleanup {
            id,
            bucket: row.bucket.clone(),
            path: row.path.clone(),
            quota_debt_id: row.quota.clone(),
            claim_token: StorageToken::generate(),
            generation: generation.clone(),
            lease_until: Timestamp(until),
        };
        row.claim = Some(cleanup.clone());
        batch.push(cleanup);
    }
    Ok(MutationOutcome::StorageCleanupBatch(batch))
}
