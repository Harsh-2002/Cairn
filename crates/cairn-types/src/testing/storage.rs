//! In-memory journal semantics; only unfinished work occupies this map.

use super::State;
use crate::MetaError;
use crate::id::{BucketName, StoragePath};
use crate::meta::MutationOutcome;
use crate::storage::{
    StorageAdmission, StorageCleanup, StorageMutation, StorageToken, StorageWritePlan,
    StorageWriteTarget,
};
use crate::storage_baseline::*;
use crate::time::Timestamp;
use std::collections::{BTreeMap, BTreeSet};

type R<T> = Result<T, MetaError>;

#[derive(Clone, Default)]
pub(super) struct Journal {
    baseline: StorageBaselineState,
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
    st.storage.baseline.generation = Some(generation);
    st.storage.baseline.legacy_release_authorized = false;
    for row in st.storage.cleanups.values_mut() {
        row.claim = None;
    }
    MutationOutcome::Ack
}

pub(super) fn prepare_restore(st: &mut State, generation: StorageToken) -> R<MutationOutcome> {
    if st.storage.baseline.generation.as_ref() == Some(&generation) {
        return Err(invalid(
            "storage restore requires a fresh generation and recovery state",
        ));
    }
    st.storage.baseline.coverage_identity = None;
    st.storage.baseline.completed_at = None;
    Ok(begin(st, generation))
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
            if journal.baseline.generation.as_ref() == Some(&generation)
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
            let applied = if journal.baseline.generation.as_ref() == Some(quiescence.generation())
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
            let applied = if journal.baseline.generation.as_ref() == Some(&current_generation)
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
            let applied = journal.baseline.generation.as_ref() == Some(&cleanup.generation)
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
    Ok(
        st.storage.baseline.generation.as_ref() == Some(&plan.generation)
            && st
                .storage
                .intents
                .get(&plan.attempt)
                .is_some_and(|(stored, cancelled)| stored == plan && !cancelled),
    )
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
    if journal.baseline.legacy_accounting_hold {
        return Err(invalid("storage baseline accounting is held"));
    }

    plan.validate()?;
    if &plan.bucket != bucket {
        return Err(invalid("storage admission routing mismatch"));
    }
    if journal.baseline.generation.as_ref() != Some(&plan.generation)
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
        if row.quota.as_deref() != Some(debt) {
            row.quota = Some(debt.to_owned());
            row.claim = None;
        }
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
    if st.storage.baseline.generation.as_ref() != Some(generation) {
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
    if st.storage.baseline.generation.as_ref() != Some(generation) {
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

pub(super) fn baseline_state(st: &State) -> StorageBaselineState {
    st.storage.baseline.clone()
}
pub(super) fn baseline_pending(st: &State) -> StorageBaselinePending {
    StorageBaselinePending {
        intents: !st.storage.intents.is_empty(),
        intent_paths: !st.storage.intents.is_empty(),
        exact_debt: !st.storage.cleanups.is_empty(),
        native_quota_debt: st.multipart_cleanups.keys().any(|id| exact_quota(st, id)),
        legacy_reservations: !st.multipart_reservations.is_empty(),
        legacy_quota_debt: st.multipart_cleanups.keys().any(|id| !exact_quota(st, id)),
    }
}
fn baseline_updated(status: StorageBaselineTransition) -> MutationOutcome {
    MutationOutcome::StorageBaselineUpdated(status)
}
pub(super) fn baseline_begin(st: &mut State, token: StorageBaselineToken) -> R<MutationOutcome> {
    let state = &mut st.storage.baseline;
    if state.generation.as_ref() != Some(&token.generation) {
        return Ok(baseline_updated(StorageBaselineTransition::Stale));
    }
    if state.matches(&token) {
        return Ok(baseline_updated(if state.legacy_release_authorized {
            StorageBaselineTransition::Stale
        } else {
            StorageBaselineTransition::AlreadyApplied
        }));
    }
    state.coverage_identity = None;
    state.completed_at = None;
    state.baseline_id = Some(token.baseline_id);
    state.legacy_accounting_hold = true;
    state.legacy_release_authorized = false;
    Ok(baseline_updated(StorageBaselineTransition::Applied))
}

pub(super) fn baseline_owners(st: &State, paths: &[StoragePath]) -> R<Vec<StoragePathOwnership>> {
    if paths.len() > STORAGE_BASELINE_PAGE_LIMIT {
        return Err(invalid("storage ownership page exceeds bound"));
    }
    let mut result = Vec::with_capacity(paths.len());
    for path in paths {
        if path.as_str().len() > 256 {
            return Err(invalid("storage path exceeds bound"));
        }
        let mut owner = StoragePathOwnership {
            path: path.clone(),
            bucket: None,
            authoritative: false,
            intent: false,
            cleanup: false,
            legacy_debt: false,
        };
        let mut retain = |bucket: &BucketName, kind: u8| -> R<()> {
            crate::storage::validate_storage_path(bucket, path)?;
            if owner
                .bucket
                .as_ref()
                .is_some_and(|previous| previous != bucket)
            {
                return Err(invalid("conflicting retained storage buckets"));
            }
            owner.bucket = Some(bucket.clone());
            match kind {
                0 => owner.authoritative = true,
                1 => owner.intent = true,
                2 => owner.cleanup = true,
                3 => owner.legacy_debt = true,
                _ => {}
            }
            Ok(())
        };
        for row in st
            .versions
            .values()
            .filter(|row| row.storage_path.as_ref() == Some(path))
        {
            retain(&row.bucket, 0)?;
        }
        for ((upload, _), _) in st.parts.iter().filter(|(_, row)| &row.storage_path == path) {
            let session = st
                .multipart
                .get(upload)
                .ok_or_else(|| invalid("missing multipart authority parent"))?;
            retain(&session.bucket, 0)?;
        }
        for (plan, _) in st
            .storage
            .intents
            .values()
            .filter(|(plan, _)| plan.paths.iter().any(|alias| &alias.path == path))
        {
            retain(&plan.bucket, 1)?;
        }
        for row in st.storage.cleanups.values() {
            if &row.path == path {
                retain(&row.bucket, 2)?;
            }
            if row.owner.as_ref() == Some(path) {
                retain(&row.bucket, 4)?;
            }
        }
        for row in st
            .multipart_cleanups
            .values()
            .filter(|row| row.storage_path.as_ref() == Some(path))
        {
            retain(&row.bucket, 3)?;
        }
        if let Some(upload) = path
            .as_str()
            .strip_prefix(".staging/multipart/")
            .and_then(|tail| tail.split_once('/').map(|(upload, _)| upload))
        {
            if let Some(session) = st.multipart.get(upload) {
                retain(&session.bucket, 4)?;
            }
            for row in st
                .multipart_cleanups
                .values()
                .filter(|row| row.upload_id.as_str() == upload)
            {
                retain(&row.bucket, 4)?;
            }
            for (plan, _) in st.storage.intents.values() {
                if matches!(&plan.target, StorageWriteTarget::Part { upload_id, .. } | StorageWriteTarget::Completion { upload_id, .. } if upload_id.as_str() == upload)
                {
                    retain(&plan.bucket, 4)?;
                }
            }
        }
        result.push(owner);
    }
    Ok(result)
}

pub(super) fn baseline_classify(
    st: &mut State,
    bucket: BucketName,
    token: StorageBaselineToken,
    paths: Vec<StoragePath>,
) -> R<MutationOutcome> {
    if !st.storage.baseline.matches(&token) || st.storage.baseline.legacy_release_authorized {
        return Ok(baseline_updated(StorageBaselineTransition::Stale));
    }
    if paths.is_empty() || paths.len() > STORAGE_BASELINE_PAGE_LIMIT {
        return Err(invalid("invalid storage classification page size"));
    }
    let owners = baseline_owners(st, &paths)?;
    let mut dispositions = Vec::with_capacity(paths.len());
    for (path, owner) in paths.iter().zip(owners) {
        crate::storage::validate_storage_path(&bucket, path)?;
        if owner.bucket.as_ref().is_some_and(|owner| owner != &bucket) {
            return Err(invalid("storage classification routing mismatch"));
        }
        if owner.authoritative {
            dispositions.push(StorageBaselineDisposition::Authoritative);
        } else if owner.intent {
            dispositions.push(StorageBaselineDisposition::IntentOwned);
        } else {
            if !owner.cleanup {
                let mut quota = None;
                for row in st
                    .storage
                    .cleanups
                    .values()
                    .filter(|row| row.owner.as_ref() == Some(path))
                {
                    if let Some(debt) = &row.quota {
                        if quota.as_ref().is_some_and(|previous| previous != debt) {
                            return Err(invalid("conflicting native storage quota owners"));
                        }
                        quota = Some(debt.clone());
                    }
                }
                let owner = quota.as_ref().map(|_| path.clone());
                enqueue_owned(&mut st.storage, &bucket, path, quota, owner)?;
            }
            dispositions.push(StorageBaselineDisposition::CleanupRecorded);
        }
    }
    Ok(MutationOutcome::StorageBaselineClassified(dispositions))
}

pub(super) fn baseline_authorize(
    st: &mut State,
    token: &StorageBaselineToken,
) -> R<MutationOutcome> {
    if !st.storage.baseline.matches(token) {
        return Ok(baseline_updated(StorageBaselineTransition::Stale));
    }
    if baseline_pending(st).native_pending() {
        return Ok(baseline_updated(StorageBaselineTransition::Blocked));
    }
    if st.storage.baseline.legacy_release_authorized {
        return Ok(baseline_updated(StorageBaselineTransition::AlreadyApplied));
    }
    st.storage.baseline.legacy_release_authorized = true;
    Ok(baseline_updated(StorageBaselineTransition::Applied))
}

pub(super) fn baseline_finalize(
    st: &mut State,
    token: StorageBaselineToken,
    limit: u32,
) -> R<MutationOutcome> {
    if !st.storage.baseline.matches(&token) || !st.storage.baseline.legacy_release_authorized {
        return Ok(baseline_updated(StorageBaselineTransition::Stale));
    }
    if baseline_pending(st).native_pending() {
        return Ok(baseline_updated(StorageBaselineTransition::Blocked));
    }
    let limit = limit.clamp(1, 1000) as usize;
    // The double scans its maps but retains only one bounded page, matching SQL row ownership.
    let mut reservations = BTreeMap::new();
    for row in st.multipart_reservations.values() {
        reservations.insert((row.created_at, row.attempt_id.clone()), row.clone());
        if reservations.len() > limit {
            reservations.pop_last();
        }
    }
    let mut released = 0;
    for row in reservations.into_values() {
        if !st.multipart.contains_key(row.upload_id.as_str()) {
            return Err(invalid("missing legacy reservation parent"));
        }
        if st.multipart_reservations.remove(&row.attempt_id).is_some() {
            released += 1;
        }
    }
    let mut cleanups = BTreeMap::new();
    for row in st
        .multipart_cleanups
        .values()
        .filter(|row| !exact_quota(st, &row.id))
    {
        cleanups.insert((row.created_at, row.id.clone()), row.id.clone());
        if cleanups.len() > limit - released {
            cleanups.pop_last();
        }
    }
    for id in cleanups.into_values() {
        if st.multipart_cleanups.remove(&id).is_some() {
            released += 1;
        }
    }
    let pending = baseline_pending(st);
    Ok(MutationOutcome::StorageBaselineLegacyPage {
        released: released as u32,
        remaining: pending.legacy_reservations || pending.legacy_quota_debt,
    })
}

pub(super) fn baseline_complete(
    st: &mut State,
    token: StorageBaselineToken,
    completed_at: Timestamp,
) -> R<MutationOutcome> {
    let state = &st.storage.baseline;
    if !state.legacy_accounting_hold
        && state.generation.as_ref() == Some(&token.generation)
        && state.coverage_identity.as_ref() == Some(&token.baseline_id)
    {
        return Ok(baseline_updated(StorageBaselineTransition::AlreadyApplied));
    }
    if !state.matches(&token) || !state.legacy_release_authorized {
        return Ok(baseline_updated(StorageBaselineTransition::Stale));
    }
    if baseline_pending(st).any() {
        return Ok(baseline_updated(StorageBaselineTransition::Blocked));
    }
    if completed_at.0 < 0 {
        return Err(invalid("invalid storage baseline completion time"));
    }
    let state = &mut st.storage.baseline;
    state.coverage_identity = Some(token.baseline_id);
    state.completed_at = Some(completed_at);
    state.baseline_id = None;
    state.legacy_accounting_hold = false;
    state.legacy_release_authorized = false;
    Ok(baseline_updated(StorageBaselineTransition::Applied))
}

pub(super) fn baseline_authority(
    st: &State,
    cursor: Option<&StorageAuthorityCursor>,
    limit: u32,
) -> R<StorageAuthorityPage> {
    if st.versions.values().any(|row| row.id.is_empty()) {
        return Err(invalid("invalid storage authority row identity"));
    }
    let mut cursor = cursor.cloned().unwrap_or_default();
    if cursor.shard != 0
        || cursor.last_id.len() > 256
        || (cursor.kind == StorageAuthorityKind::Objects && cursor.last_part != 0)
    {
        return Err(invalid("invalid storage authority cursor"));
    }
    let limit = limit.clamp(1, STORAGE_BASELINE_PAGE_LIMIT as u32) as usize;
    let mut items = Vec::with_capacity(limit);
    if cursor.kind == StorageAuthorityKind::Objects {
        let mut rows = BTreeMap::new();
        for row in st.versions.values().filter(|row| row.id > cursor.last_id) {
            rows.insert(row.id.clone(), row);
            if rows.len() > limit + 1 {
                rows.pop_last();
            }
        }
        let more = rows.len() > limit;
        for row in rows.into_values().take(limit) {
            if let Some(path) = &row.storage_path {
                crate::storage::validate_storage_path(&row.bucket, path)?;
            }
            cursor.last_id = row.id.clone();
            items.push(StorageAuthority::Object(Box::new(row.clone())));
        }
        if more {
            return Ok(StorageAuthorityPage {
                items,
                next: Some(cursor),
            });
        }
        cursor.kind = StorageAuthorityKind::Parts;
        cursor.last_id.clear();
        cursor.last_part = 0;
    }
    let remaining = limit - items.len();
    let rows: Vec<_> = st
        .parts
        .range((
            std::ops::Bound::Excluded((cursor.last_id.clone(), cursor.last_part)),
            std::ops::Bound::Unbounded,
        ))
        .take(remaining + 1)
        .collect();
    let more = rows.len() > remaining;
    for ((upload, number), part) in rows.into_iter().take(remaining) {
        let session = st
            .multipart
            .get(upload)
            .ok_or_else(|| invalid("missing multipart authority parent"))?;
        crate::storage::validate_storage_path(&session.bucket, &part.storage_path)?;
        if !(1..=10000).contains(number) || *number != part.part_number {
            return Err(invalid("invalid multipart authority geometry"));
        }
        cursor.last_id = upload.clone();
        cursor.last_part = *number;
        items.push(StorageAuthority::Part {
            bucket: session.bucket.clone(),
            upload_id: session.upload_id.clone(),
            part: part.clone(),
        });
    }
    Ok(StorageAuthorityPage {
        items,
        next: more.then_some(cursor),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_quota_preserves_cleanup_claim() {
        let mut st = State::default();
        let bucket = BucketName::parse("storage-relinked-claim").unwrap();
        let generation = StorageToken::generate();
        let owner = StoragePath::from_string(".staging/multipart/00000000000000000000000000000000/00001-11111111111111111111111111111111".into());
        let alias =
            StoragePath::from_string(".staging/22222222222222222222222222222222.index.tmp".into());
        begin(&mut st, generation.clone());
        enqueue_owned(&mut st.storage, &bucket, &alias, None, Some(owner.clone())).unwrap();
        link_quota_inner(&mut st.storage, &bucket, &owner, "debt").unwrap();
        let MutationOutcome::StorageCleanupBatch(batch) =
            claim(&mut st, &generation, 100, Timestamp(0), 60).unwrap()
        else {
            panic!("cleanup batch expected");
        };
        assert_eq!(batch.len(), 2);
        link_quota_inner(&mut st.storage, &bucket, &owner, "debt").unwrap();
        assert_eq!(
            claim(&mut st, &generation, 100, Timestamp(1), 60).unwrap(),
            MutationOutcome::StorageCleanupBatch(Vec::new())
        );
        for cleanup in batch {
            assert_eq!(
                apply(
                    &mut st,
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
}
