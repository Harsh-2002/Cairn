//! Atomic dispatch indexes and exact-token completion for the bounded candidate workload.
use super::{
    kv::{self, Overlay, View, get, key},
    model::*,
};
use cairn_types::id::ReplicationClaimToken;
use cairn_types::*;
fn due(entry: &OutboxEntry) -> Option<Vec<u8>> {
    match entry.status {
        ReplicationStatus::Pending => {
            Some(time_key(OUTBOX_DUE, entry.next_attempt_at.0, &entry.id))
        }
        ReplicationStatus::Claimed => entry
            .lease_until
            .map(|t| time_key(OUTBOX_DUE, t.0.max(entry.next_attempt_at.0), &entry.id)),
        _ => None,
    }
}
fn status_key(entry: &OutboxEntry) -> Result<Vec<u8>, MetaError> {
    let mut output = vec![OUTBOX_STATUS, status_index(entry.status)? as u8];
    output.extend_from_slice(&((entry.enqueued_at.0 as u64) ^ (1_u64 << 63)).to_be_bytes());
    kv::component(&mut output, entry.id.as_bytes());
    Ok(output)
}
fn save(
    view: &mut Overlay<'_>,
    previous: Option<&OutboxEntry>,
    entry: &OutboxEntry,
) -> Result<(), MetaError> {
    let mut stats = stats(view, &entry.bucket)?;
    if let Some(previous) = previous {
        stats.outbox[status_index(previous.status)?] = stats.outbox[status_index(previous.status)?]
            .checked_sub(1)
            .ok_or(MetaError::Integrity)?;
        if let Some(address) = due(previous) {
            view.remove(address)?;
        }
        view.remove(status_key(previous)?)?;
    }
    stats.outbox[status_index(entry.status)?] += 1;
    view.put(key(STATS, &[entry.bucket.as_str()]), &stats)?;
    if let Some(address) = due(entry) {
        view.put(address, &entry.id)?;
    }
    view.put(status_key(entry)?, &entry.id)?;
    view.put(
        key(
            OUTBOX_BUCKET_KEY,
            &[entry.bucket.as_str(), entry.key.as_str(), &entry.id],
        ),
        &entry.id,
    )?;
    view.put(key(OUTBOX, &[&entry.id]), &Outbox(entry.clone()))
}
pub fn enqueue(view: &mut Overlay<'_>, entries: &[OutboxEntry]) -> Result<(), MetaError> {
    if entries.len() > 16 {
        return Err(kv::error("candidate outbox fanout exceeds bound"));
    }
    for entry in entries {
        if entry.priority != 0
            || entry.status != ReplicationStatus::Pending
            || entry.claim_token.is_some()
            || entry.lease_until.is_some()
            || entry.target_arn.is_some()
        {
            return Err(kv::error(
                "outbox request exceeds candidate workload contract",
            ));
        }
        require_bucket(view, &entry.bucket)?;
        if view.get(&key(OUTBOX, &[&entry.id]))?.is_some() {
            return Err(MetaError::Conflict);
        }
        save(view, None, entry)?;
    }
    Ok(())
}
pub fn claim(
    view: &mut Overlay<'_>,
    limit: u32,
    now: Timestamp,
    lease_secs: i64,
) -> Result<MutationOutcome, MetaError> {
    if limit == 0 || limit as usize > kv::PAGE {
        return Err(kv::error("candidate replication claim exceeds bound"));
    }
    let until = claim_until(now, lease_secs)?;
    let rows = view.scan(&[OUTBOX_DUE], None, limit as usize)?;
    let mut entries = Vec::with_capacity(rows.len());
    for (_, value) in rows {
        let id: String = kv::decode(&value)?;
        let entry = get::<Outbox>(view, &key(OUTBOX, &[&id]))?
            .ok_or(MetaError::Integrity)?
            .0;
        if entry.next_attempt_at > now
            || (entry.status == ReplicationStatus::Claimed
                && entry.lease_until.is_none_or(|t| t >= now))
        {
            break;
        }
        if !matches!(
            entry.status,
            ReplicationStatus::Pending | ReplicationStatus::Claimed
        ) {
            return Err(MetaError::Integrity);
        }
        let mut claimed = entry.clone();
        claimed.status = ReplicationStatus::Claimed;
        claimed.claim_token = Some(ReplicationClaimToken::generate());
        claimed.lease_until = Some(until);
        save(view, Some(&entry), &claimed)?;
        entries.push(claimed);
    }
    Ok(MutationOutcome::ReplicationBatch(entries))
}
pub fn done(
    view: &mut Overlay<'_>,
    id: String,
    token: ReplicationClaimToken,
    now: Timestamp,
) -> Result<MutationOutcome, MetaError> {
    let previous = get::<Outbox>(view, &key(OUTBOX, &[&id]))?.map(|v| v.0);
    let applied = if let Some(previous) = previous
        && previous.status == ReplicationStatus::Claimed
        && previous.claim_token.as_ref() == Some(&token)
        && previous.lease_until.is_some_and(|t| t >= now)
    {
        if let Some(mut row) = version(view, &previous.bucket, &previous.key, &previous.version_id)?
            && row.replication_status != Some(ReplicationStatus::Replica)
        {
            row.replication_status = Some(ReplicationStatus::Completed);
            row.replicated_at = Some(now);
            save_row(view, row)?;
        }
        let mut completed = previous.clone();
        completed.status = ReplicationStatus::Completed;
        completed.claim_token = None;
        completed.lease_until = None;
        save(view, Some(&previous), &completed)?;
        true
    } else {
        false
    };
    Ok(MutationOutcome::ReplicationClaimUpdated { applied })
}
pub fn prune(view: &mut Overlay<'_>, before: i64) -> Result<MutationOutcome, MetaError> {
    for status in [2_u8, 3] {
        let prefix = [OUTBOX_STATUS, status];
        let mut after = None;
        'pages: loop {
            let rows = view.scan(&prefix, after.as_deref(), kv::PAGE)?;
            if rows.is_empty() {
                break;
            }
            after = rows.last().map(|(key, _)| key.clone());
            for (address, value) in rows {
                let id: String = kv::decode(&value)?;
                let entry = get::<Outbox>(view, &key(OUTBOX, &[&id]))?
                    .ok_or(MetaError::Integrity)?
                    .0;
                if entry.enqueued_at.0 >= before {
                    break 'pages;
                }
                let mut stats = stats(view, &entry.bucket)?;
                stats.outbox[usize::from(status)] = stats.outbox[usize::from(status)]
                    .checked_sub(1)
                    .ok_or(MetaError::Integrity)?;
                view.put(key(STATS, &[entry.bucket.as_str()]), &stats)?;
                view.remove(address)?;
                view.remove(key(OUTBOX, &[&id]))?;
                view.remove(key(
                    OUTBOX_BUCKET_KEY,
                    &[entry.bucket.as_str(), entry.key.as_str(), &id],
                ))?;
            }
        }
    }
    Ok(MutationOutcome::Ack)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase};

    #[test]
    fn retention_pages_all_completed_entries_and_preserves_exact_counters() {
        let root = tempfile::tempdir().unwrap();
        let db = SingleWriterTxDatabase::builder(root.path()).open().unwrap();
        let tree = db.keyspace("rows", KeyspaceCreateOptions::default).unwrap();
        let bucket = BucketName::parse("retention-bucket").unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        let native = kv::Native {
            transaction: &tx,
            tree: &tree,
        };
        let mut view = Overlay::new(&native);
        view.put(
            key(BUCKET, &[bucket.as_str()]),
            &Bucket {
                name: bucket.clone(),
                owner_id: UserId("owner".into()),
                created_at: Timestamp(0),
                versioning: VersioningState::Enabled,
                ownership_mode: OwnershipMode::BucketOwnerEnforced,
                region: "us-east-1".into(),
                compression: None,
            },
        )
        .unwrap();
        view.put(key(STATS, &[bucket.as_str()]), &Stats::default())
            .unwrap();
        let entries: Vec<_> = (0..260)
            .map(|i| OutboxEntry {
                id: format!("entry-{i:04}"),
                bucket: bucket.clone(),
                key: ObjectKey::parse("retained-key").unwrap(),
                version_id: VersionId::from_string("retained-version".into()),
                operation: ReplicationOp::ObjectCreate,
                rule_id: "test".into(),
                target_arn: None,
                attempts: 0,
                next_attempt_at: Timestamp(0),
                status: ReplicationStatus::Pending,
                last_error: None,
                priority: 0,
                lease_until: None,
                claim_token: None,
                enqueued_at: Timestamp(0),
            })
            .collect();
        for batch in entries.chunks(16) {
            enqueue(&mut view, batch).unwrap();
        }
        for _ in 0..2 {
            let MutationOutcome::ReplicationBatch(batch) =
                claim(&mut view, 256, Timestamp(0), 300).unwrap()
            else {
                panic!("claim outcome");
            };
            let counts = super::super::reads::replication_counts(&view, None).unwrap();
            assert_eq!(counts.claimed, batch.len() as u64);
            assert_eq!(counts.by_target.len(), 1);
            assert!(counts.by_target[0].target_arn.is_none());
            assert_eq!(counts.by_target[0].claimed, counts.claimed);
            assert_eq!(counts.by_target[0].pending, counts.pending);
            for entry in batch {
                done(
                    &mut view,
                    entry.id,
                    entry.claim_token.unwrap(),
                    Timestamp(0),
                )
                .unwrap();
            }
        }
        assert_eq!(stats(&view, &bucket).unwrap().outbox, [0, 0, 0, 260]);
        assert!(
            super::super::reads::replication_counts(&view, None)
                .unwrap()
                .by_target
                .is_empty()
        );
        for (key, value) in view.into_edits() {
            if let Some(value) = value {
                tx.insert(&tree, key, value);
            } else {
                tx.remove(&tree, key);
            }
        }
        tx.commit().unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        let native = kv::Native {
            transaction: &tx,
            tree: &tree,
        };
        let mut view = Overlay::new(&native);
        assert!(matches!(prune(&mut view, 1).unwrap(), MutationOutcome::Ack));
        for table in [OUTBOX, OUTBOX_DUE, OUTBOX_STATUS, OUTBOX_BUCKET_KEY] {
            assert!(!kv::exists(&view, &[table]).unwrap());
        }
        assert_eq!(stats(&view, &bucket).unwrap().outbox, [0; 4]);
        for (key, value) in view.into_edits() {
            if let Some(value) = value {
                tx.insert(&tree, key, value);
            } else {
                tx.remove(&tree, key);
            }
        }
        tx.commit().unwrap();
        let snapshot = db.read_tx();
        let native = kv::Native {
            transaction: &snapshot,
            tree: &tree,
        };
        assert!(!kv::exists(&native, &[OUTBOX]).unwrap());
        assert_eq!(stats(&native, &bucket).unwrap().outbox, [0; 4]);
    }
}
