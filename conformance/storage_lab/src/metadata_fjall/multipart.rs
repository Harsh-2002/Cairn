//! Bounded active-session, reservation and exact replacement accounting.
use super::{
    journal,
    kv::{self, Overlay, View, get, key},
    model::*,
};
use cairn_types::meta::MultipartLimits;
use cairn_types::*;
use serde::{Deserialize, Serialize};
const USAGE: u8 = 32;
const SLOT: u8 = 33;
const RESERVATION_PLAN: u8 = 34;
#[derive(Default, Serialize, Deserialize)]
struct Usage {
    distinct: u64,
    parts: u64,
    reservations: u64,
}
fn active(view: &dyn View, upload: &UploadId) -> Result<MultipartSession, MetaError> {
    let session = get::<Session>(view, &key(SESSION, &[upload.as_str()]))?
        .ok_or(MetaError::MultipartNotActive)?
        .0;
    if session.status != MultipartStatus::Active {
        return Err(MetaError::MultipartNotActive);
    }
    Ok(session)
}
fn slot(
    view: &mut Overlay<'_>,
    upload: &UploadId,
    number: u16,
    delta: i128,
    reservation: bool,
) -> Result<(), MetaError> {
    let address = key(SLOT, &[upload.as_str(), &format!("{number:05}")]);
    let before = get::<u64>(view, &address)?.unwrap_or(0);
    let after = change(before, delta)?;
    let mut usage: Usage = get(view, &key(USAGE, &[upload.as_str()]))?.unwrap_or_default();
    usage.distinct = change(
        usage.distinct,
        i128::from(after > 0) - i128::from(before > 0),
    )?;
    if reservation {
        usage.reservations = change(usage.reservations, delta)?;
    } else {
        usage.parts = change(usage.parts, delta)?;
    }
    if after == 0 {
        view.remove(address)?;
    } else {
        view.put(address, &after)?;
    }
    view.put(key(USAGE, &[upload.as_str()]), &usage)
}
pub fn part_key(upload: &UploadId, number: u16) -> Vec<u8> {
    key(PART, &[upload.as_str(), &format!("{number:05}")])
}
pub fn create(
    view: &mut Overlay<'_>,
    session: MultipartSession,
    limits: MultipartLimits,
) -> Result<MutationOutcome, MetaError> {
    require_bucket(view, &session.bucket)?;
    if session.status != MultipartStatus::Active
        || session.replica_intent.is_some()
        || limits.max_parts_per_upload > 2
        || limits.max_parts_per_upload == 0
        || limits.max_active_uploads_per_bucket > 256
        || limits.max_active_uploads_per_principal > 256
    {
        return Err(kv::error(
            "multipart request exceeds the candidate workload contract",
        ));
    }
    if view
        .get(&key(SESSION, &[session.upload_id.as_str()]))?
        .is_some()
    {
        return Err(MetaError::Conflict);
    }
    let mut stats = stats(view, &session.bucket)?;
    let mut principal = principal(view, &session.initiated_by)?;
    if stats.active >= u64::from(limits.max_active_uploads_per_bucket)
        || principal.active >= u64::from(limits.max_active_uploads_per_principal)
    {
        return Err(MetaError::QuotaExceeded);
    }
    stats.active += 1;
    principal.active += 1;
    view.put(key(STATS, &[session.bucket.as_str()]), &stats)?;
    view.put(key(PRINCIPAL, &[&session.initiated_by.0]), &principal)?;
    view.put(
        key(
            SESSION_BUCKET,
            &[
                session.bucket.as_str(),
                session.key.as_str(),
                session.upload_id.as_str(),
            ],
        ),
        &session.upload_id,
    )?;
    let id = session.upload_id.clone();
    view.put(key(SESSION, &[id.as_str()]), &Session(session))?;
    Ok(MutationOutcome::MultipartCreated(id))
}
#[allow(clippy::too_many_arguments)]
pub fn reserve(
    view: &mut Overlay<'_>,
    upload: UploadId,
    number: u16,
    attempt: String,
    bytes: u64,
    max_parts: u16,
    now: Timestamp,
) -> Result<MutationOutcome, MetaError> {
    let session = active(view, &upload)?;
    if number == 0 || number > 10_000 || max_parts > 2 || max_parts == 0 {
        return Err(kv::error("candidate part cardinality contract exceeded"));
    }
    if let Some(old) = get::<Reservation>(view, &key(RESERVATION, &[&attempt]))? {
        if old.upload == upload && old.part == number && old.bytes == bytes {
            return Ok(MutationOutcome::MultipartReserved);
        }
        return Err(MetaError::QuotaExceeded);
    }
    let usage: Usage = get(view, &key(USAGE, &[upload.as_str()]))?.unwrap_or_default();
    let count = get::<u64>(
        view,
        &key(SLOT, &[upload.as_str(), &format!("{number:05}")]),
    )?
    .unwrap_or(0);
    if (count == 0 && usage.distinct >= u64::from(max_parts)) || usage.reservations >= 128 {
        return Err(MetaError::QuotaExceeded);
    }
    stage_delta(
        view,
        &session.bucket,
        &session.initiated_by,
        i128::from(bytes),
    )?;
    slot(view, &upload, number, 1, true)?;
    let reservation = Reservation {
        attempt: attempt.clone(),
        upload: upload.clone(),
        part: number,
        bytes,
        bucket: session.bucket,
        principal: session.initiated_by,
        created_at: now,
    };
    view.put(
        key(RESERVATION_UPLOAD, &[upload.as_str(), &attempt]),
        &attempt,
    )?;
    view.put(key(RESERVATION, &[&attempt]), &reservation)?;
    Ok(MutationOutcome::MultipartReserved)
}
pub fn remember_plan(
    view: &mut Overlay<'_>,
    attempt: &str,
    plan: &cairn_types::storage::StorageWritePlan,
) -> Result<(), MetaError> {
    view.put(key(RESERVATION_PLAN, &[attempt]), plan)
}
pub fn remove_reservation(
    view: &mut Overlay<'_>,
    reservation: &Reservation,
) -> Result<(), MetaError> {
    view.remove(key(RESERVATION, &[&reservation.attempt]))?;
    view.remove(key(
        RESERVATION_UPLOAD,
        &[reservation.upload.as_str(), &reservation.attempt],
    ))?;
    view.remove(key(RESERVATION_PLAN, &[&reservation.attempt]))?;
    slot(view, &reservation.upload, reservation.part, -1, true)
}
pub fn record(
    view: &mut Overlay<'_>,
    upload: UploadId,
    attempt: String,
    part: PartRecord,
) -> Result<MutationOutcome, MetaError> {
    let session = active(view, &upload)?;
    let reservation: Reservation =
        get(view, &key(RESERVATION, &[&attempt]))?.ok_or(MetaError::MultipartNotActive)?;
    if reservation.upload != upload {
        return Err(MetaError::MultipartNotActive);
    }
    if reservation.part != part.part_number || reservation.bytes != part.size {
        return Err(MetaError::QuotaExceeded);
    }
    let address = part_key(&upload, part.part_number);
    let previous = get::<Part>(view, &address)?.map(|part| part.0);
    if view
        .get(&key(REFERENCE, &[part.storage_path.as_str()]))?
        .is_some()
    {
        return Err(MetaError::Conflict);
    }
    let cleanup = if let Some(old) = previous {
        view.remove(key(REFERENCE, &[old.storage_path.as_str()]))?;
        slot(view, &upload, old.part_number, -1, false)?;
        Some(journal::quota_debt(
            view,
            &session.bucket,
            &session.initiated_by,
            &upload,
            &old.storage_path,
            old.size,
            reservation.created_at,
            Some(format!("part:{attempt}")),
        )?)
    } else {
        None
    };
    remove_reservation(view, &reservation)?;
    slot(view, &upload, part.part_number, 1, false)?;
    view.put(key(REFERENCE, &[part.storage_path.as_str()]), &address)?;
    view.put(address, &Part(part))?;
    Ok(MutationOutcome::PartRecorded { cleanup })
}
pub fn abort(view: &mut Overlay<'_>, upload: UploadId) -> Result<MutationOutcome, MetaError> {
    let Some(Session(session)) = get(view, &key(SESSION, &[upload.as_str()]))? else {
        return Ok(MutationOutcome::MultipartTerminal(
            MultipartTerminalOutcome::NotOwner,
        ));
    };
    if session.status != MultipartStatus::Active {
        return Ok(MutationOutcome::MultipartTerminal(
            MultipartTerminalOutcome::NotOwner,
        ));
    }
    let parts = view.scan(&key(PART, &[upload.as_str()]), None, kv::PAGE)?;
    let reservations = view.scan(&key(RESERVATION_UPLOAD, &[upload.as_str()]), None, kv::PAGE)?;
    if parts.len() >= kv::PAGE || reservations.len() >= kv::PAGE {
        return Err(kv::error("candidate terminal mutation exceeds its bound"));
    }
    for (address, value) in parts {
        let part = kv::decode::<Part>(&value)?.0;
        view.remove(address)?;
        view.remove(key(REFERENCE, &[part.storage_path.as_str()]))?;
        slot(view, &upload, part.part_number, -1, false)?;
        journal::quota_debt(
            view,
            &session.bucket,
            &session.initiated_by,
            &upload,
            &part.storage_path,
            part.size,
            session.updated_at,
            None,
        )?;
    }
    for (_, value) in reservations {
        let attempt: String = kv::decode(&value)?;
        let reservation: Reservation =
            get(view, &key(RESERVATION, &[&attempt]))?.ok_or(MetaError::Integrity)?;
        let plan: cairn_types::storage::StorageWritePlan =
            get(view, &key(RESERVATION_PLAN, &[&attempt]))?.ok_or(MetaError::Integrity)?;
        journal::quota_debt(
            view,
            &session.bucket,
            &session.initiated_by,
            &upload,
            plan.final_path()?,
            reservation.bytes,
            reservation.created_at,
            None,
        )?;
        remove_reservation(view, &reservation)?;
    }
    let mut stats = stats(view, &session.bucket)?;
    let mut principal = principal(view, &session.initiated_by)?;
    stats.active = stats.active.checked_sub(1).ok_or(MetaError::Integrity)?;
    principal.active = principal
        .active
        .checked_sub(1)
        .ok_or(MetaError::Integrity)?;
    view.put(key(STATS, &[session.bucket.as_str()]), &stats)?;
    view.put(key(PRINCIPAL, &[&session.initiated_by.0]), &principal)?;
    view.remove(key(SESSION, &[upload.as_str()]))?;
    view.remove(key(
        SESSION_BUCKET,
        &[
            session.bucket.as_str(),
            session.key.as_str(),
            upload.as_str(),
        ],
    ))?;
    view.remove(key(USAGE, &[upload.as_str()]))?;
    Ok(MutationOutcome::MultipartTerminal(
        MultipartTerminalOutcome::Aborted,
    ))
}
