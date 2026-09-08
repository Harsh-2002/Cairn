//! Indexed, snapshot-consistent reads for the candidate's populated workload.
use super::{
    kv::{self, View, get, key},
    model::*,
    multipart,
};
use cairn_types::meta::{ReplicationCounts, ReplicationTargetCounts};
use cairn_types::*;

fn limit(value: u32) -> Result<usize, MetaError> {
    if value as usize > kv::PAGE {
        return Err(kv::error("candidate external page exceeds bound"));
    }
    Ok(value.max(1) as usize)
}
fn before_key(table: u8, bucket: &BucketName, value: &str, versions: bool) -> Vec<u8> {
    let mut address = key(table, &[bucket.as_str(), value]);
    if !versions {
        address.pop();
    }
    address
}
fn later(cursor: &mut Option<Vec<u8>>, candidate: Vec<u8>) {
    if cursor.as_ref().is_none_or(|old| old < &candidate) {
        *cursor = Some(candidate);
    }
}
pub fn list(
    view: &dyn View,
    bucket: &BucketName,
    query: &ListQuery,
    versions: bool,
) -> Result<ListPage<ObjectSummary>, MetaError> {
    let limit = limit(query.limit)?;
    if versions && query.delimiter.as_ref().is_some_and(|d| !d.is_empty()) {
        return Err(kv::error(
            "candidate version delimiter listing is outside the fixed trace",
        ));
    }
    let table = if versions { VERSION_LIST } else { CURRENT_LIST };
    let prefix = query.prefix.as_deref().unwrap_or("");
    let address_prefix = kv::raw_prefix(table, &[bucket.as_str()], prefix);
    let delimiter = query.delimiter.as_deref().filter(|d| !d.is_empty());
    let mut after = None;
    if let Some(cursor) = &query.cursor {
        if versions && let Some(marker) = &query.version_id_marker {
            later(
                &mut after,
                version_index(
                    bucket,
                    &ObjectKey::parse(cursor).map_err(kv::error)?,
                    &VersionId::from_string(marker.clone()),
                ),
            );
        } else {
            later(&mut after, before_key(table, bucket, cursor, versions));
        }
    }
    if let Some(start) = &query.start_after {
        let common = delimiter.and_then(|delimiter| {
            start.strip_prefix(prefix).and_then(|rest| {
                rest.find(delimiter)
                    .map(|position| format!("{prefix}{}{delimiter}", &rest[..position]))
            })
        });
        if let Some(common) = common {
            let Some(next) = kv::successor(&kv::raw_prefix(table, &[bucket.as_str()], &common))
            else {
                return Ok(ListPage::default());
            };
            later(&mut after, next);
        } else {
            let address = key(table, &[bucket.as_str(), start]);
            if versions {
                let Some(next) = kv::successor(&address) else {
                    return Ok(ListPage::default());
                };
                later(&mut after, next);
            } else {
                later(&mut after, address);
            }
        }
    }
    let mut page: ListPage<ObjectSummary> = ListPage::default();
    'pages: loop {
        let rows = view.scan(&address_prefix, after.as_deref(), kv::PAGE)?;
        if rows.is_empty() {
            break;
        }
        let exhausted = rows.len() < kv::PAGE;
        for (address, bytes) in rows {
            let row = kv::decode::<Summary>(&bytes)?.into_summary();
            if page.items.len() + page.common_prefixes.len() == limit {
                page.truncated = true;
                if versions {
                    let last = page.items.last().ok_or(MetaError::Integrity)?;
                    page.next_cursor = Some(last.key.as_str().to_owned());
                    page.next_version_id_marker = Some(last.version_id.as_str().to_owned());
                } else {
                    page.next_cursor = Some(row.key.as_str().to_owned());
                }
                break 'pages;
            }
            if let Some(delimiter) = delimiter
                && let Some(rest) = row.key.as_str().strip_prefix(prefix)
                && let Some(position) = rest.find(delimiter)
            {
                let common = format!("{prefix}{}{delimiter}", &rest[..position]);
                let Some(next) = kv::successor(&kv::raw_prefix(table, &[bucket.as_str()], &common))
                else {
                    break 'pages;
                };
                page.common_prefixes.push(common);
                after = Some(next);
                continue 'pages;
            }
            after = Some(address);
            page.items.push(row);
        }
        if exhausted {
            break;
        }
    }
    Ok(page)
}
pub fn parts(
    view: &dyn View,
    upload: &UploadId,
    marker: u16,
    max: u32,
) -> Result<ListPage<PartRecord>, MetaError> {
    let limit = limit(max)?;
    let prefix = key(PART, &[upload.as_str()]);
    let rows = view.scan(&prefix, Some(&multipart::part_key(upload, marker)), limit)?;
    let truncated = rows.len() == limit
        && !view
            .scan(&prefix, rows.last().map(|(key, _)| key.as_slice()), 1)?
            .is_empty();
    let items = rows
        .into_iter()
        .map(|(_, data)| kv::decode::<Part>(&data).map(|part| part.0))
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor = truncated.then(|| {
        items
            .last()
            .expect("nonempty truncated page")
            .part_number
            .to_string()
    });
    Ok(ListPage {
        items,
        common_prefixes: vec![],
        next_cursor,
        next_version_id_marker: None,
        truncated,
    })
}
pub fn sessions(
    view: &dyn View,
    bucket: &BucketName,
    query: &ListQuery,
) -> Result<ListPage<MultipartSession>, MetaError> {
    let limit = limit(query.limit)?;
    if query.delimiter.as_ref().is_some_and(|d| !d.is_empty()) {
        return Err(kv::error(
            "candidate multipart delimiter is outside the fixed trace",
        ));
    }
    let prefix = kv::raw_prefix(
        SESSION_BUCKET,
        &[bucket.as_str()],
        query.prefix.as_deref().unwrap_or(""),
    );
    let marker = query.cursor.as_ref().or(query.start_after.as_ref());
    let after = marker.map(|marker| {
        if let Some(id) = &query.version_id_marker {
            key(SESSION_BUCKET, &[bucket.as_str(), marker, id])
        } else {
            kv::successor(&key(SESSION_BUCKET, &[bucket.as_str(), marker]))
                .expect("table prefix has successor")
        }
    });
    let rows = view.scan(&prefix, after.as_deref(), limit)?;
    let truncated = rows.len() == limit
        && !view
            .scan(&prefix, rows.last().map(|(key, _)| key.as_slice()), 1)?
            .is_empty();
    let items = rows
        .into_iter()
        .map(|(_, data)| {
            let id: UploadId = kv::decode(&data)?;
            get::<Session>(view, &key(SESSION, &[id.as_str()]))?
                .map(|s| s.0)
                .ok_or(MetaError::Integrity)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let last = items.last();
    Ok(ListPage {
        next_cursor: if truncated {
            last.map(|s| s.key.as_str().to_owned())
        } else {
            None
        },
        next_version_id_marker: if truncated {
            last.map(|s| s.upload_id.as_str().to_owned())
        } else {
            None
        },
        items,
        common_prefixes: vec![],
        truncated,
    })
}
pub fn bucket_counts(view: &dyn View) -> Result<Vec<BucketCounts>, MetaError> {
    let rows = view.scan(&[BUCKET], None, kv::PAGE)?;
    if rows.len() == kv::PAGE {
        return Err(kv::error(
            "candidate bucket population exceeds declared bound",
        ));
    }
    rows.into_iter()
        .map(|(_, value)| {
            let bucket: Bucket = kv::decode(&value)?;
            let stats = stats(view, &bucket.name)?;
            Ok(BucketCounts {
                bucket: bucket.name.as_str().to_owned(),
                objects: stats.objects,
                logical_bytes: stats.logical,
                physical_bytes: stats.physical,
            })
        })
        .collect()
}
pub fn pending(
    view: &dyn View,
) -> Result<cairn_types::storage_baseline::StorageBaselinePending, MetaError> {
    Ok(cairn_types::storage_baseline::StorageBaselinePending {
        intents: kv::exists(view, &[INTENT])?,
        intent_paths: kv::exists(view, &[INTENT_PATH])?,
        exact_debt: kv::exists(view, &[DEBT])?,
        native_quota_debt: kv::exists(view, &[QUOTA_DEBT])?,
        legacy_reservations: kv::exists(view, &[RESERVATION])?,
        legacy_quota_debt: false,
    })
}
pub fn replication_counts(
    view: &dyn View,
    bucket: Option<&BucketName>,
) -> Result<ReplicationCounts, MetaError> {
    if bucket.is_some() {
        return Err(kv::error(
            "candidate bucket-specific replication overview is outside the fixed trace",
        ));
    }
    let mut counts = ReplicationCounts::default();
    let rows = view.scan(&[STATS], None, kv::PAGE)?;
    if rows.len() == kv::PAGE {
        return Err(kv::error(
            "candidate bucket population exceeds declared bound",
        ));
    }
    for (_, value) in rows {
        let stats: Stats = kv::decode(&value)?;
        counts.pending += stats.outbox[0];
        counts.claimed += stats.outbox[1];
        counts.failed += stats.outbox[2];
        counts.completed += stats.outbox[3];
    }
    if let Some((_, value)) = view.scan(&[OUTBOX_STATUS, 0], None, 1)?.first() {
        let id: String = kv::decode(value)?;
        counts.oldest_pending_at_ms = get::<Outbox>(view, &key(OUTBOX, &[&id]))?
            .ok_or(MetaError::Integrity)?
            .0
            .enqueued_at
            .0;
    }
    if counts.pending + counts.failed > 0 {
        counts.by_target.push(ReplicationTargetCounts {
            target_arn: None,
            pending: counts.pending,
            failed: counts.failed,
        });
    }
    Ok(counts)
}
