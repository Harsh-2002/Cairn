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
use std::collections::BTreeMap;

type R<T> = Result<T, MetaError>;

#[derive(Clone, Default)]
pub(super) struct Journal {
    generation: Option<StorageToken>,
    intents: BTreeMap<StorageToken, (StorageWritePlan, bool)>,
    cleanups: BTreeMap<StorageToken, PendingCleanup>,
}

#[derive(Clone)]
struct PendingCleanup {
    bucket: BucketName,
    path: StoragePath,
    quota: Option<String>,
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
        StorageMutation::FinishCleanup { cleanup, now } => {
            let applied = journal.generation.as_ref() == Some(&cleanup.generation)
                && &cleanup.bucket == bucket
                && cleanup.lease_until >= now
                && journal
                    .cleanups
                    .get(&cleanup.id)
                    .is_some_and(|row| row.claim.as_ref() == Some(&cleanup));
            if applied {
                journal.cleanups.remove(&cleanup.id);
            }
            MutationOutcome::StorageUpdated { applied }
        }
    };
    st.storage = journal;
    Ok(outcome)
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

fn enqueue(
    journal: &mut Journal,
    bucket: &BucketName,
    path: &StoragePath,
    quota: Option<String>,
) -> R<()> {
    crate::storage::validate_storage_path(bucket, path)?;
    if let Some(row) = journal.cleanups.values().find(|row| &row.path == path) {
        if &row.bucket != bucket || row.quota != quota {
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
            claim: None,
        },
    );
    Ok(())
}

fn resolve(st: &State, journal: &mut Journal, plan: &StorageWritePlan) -> R<()> {
    for path in &plan.paths {
        if !referenced(st, &path.path) {
            enqueue(journal, &plan.bucket, &path.path, None)?;
        }
    }
    journal.intents.remove(&plan.attempt);
    Ok(())
}

pub(super) fn recover(st: &mut State, generation: &StorageToken, limit: u32) -> R<MutationOutcome> {
    if st.storage.generation.as_ref() != Some(generation) {
        return Ok(MutationOutcome::StorageRecovered(0));
    }
    let mut journal = st.storage.clone();
    let mut plans: Vec<_> = journal
        .intents
        .values()
        .filter(|(plan, _)| &plan.generation != generation)
        .map(|(plan, _)| plan.clone())
        .collect();
    plans.sort_by(|a, b| (&a.generation, &a.attempt).cmp(&(&b.generation, &b.attempt)));
    plans.truncate(limit.clamp(1, 1000) as usize);
    for plan in &plans {
        resolve(st, &mut journal, plan)?;
    }
    st.storage = journal;
    Ok(MutationOutcome::StorageRecovered(plans.len() as u32))
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
