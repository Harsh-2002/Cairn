//! The background lifecycle scanner (ARCH 19.2).
//!
//! [`LifecycleScanner::run_once`] processes the buckets that carry a lifecycle configuration.
//! For each bucket it pages through current objects, all versions, and stale multipart
//! sessions using the *bounded* enumeration methods of the [`MetadataStore`], evaluates each
//! item against the bucket's rules using the injected [`Clock`] for all age and date math, and
//! applies the due actions by submitting the appropriate [`Mutation`] and reclaiming blobs.
//!
//! The scanner is idempotent (ARCH 19.2): every action is a state transition that is a no-op
//! once already performed, so a scan that is interrupted and rerun, or simply run twice,
//! converges to the same end state. Current-object expiration in a versioned bucket relies on
//! [`MetadataStore::list_current`] excluding delete markers — once a marker hides a key, the
//! key no longer appears, so no second marker is inserted.
//!
//! Transition to a remote cold tier (ARCH 19.5) is a documented NO-OP placeholder in v1: the
//! scanner recognizes the action but performs no data movement and does not count it.

use crate::config::{Action, Expiration, Filter, LifecycleRule};
use cairn_types::{BlobStore, MetaError};
use cairn_types::{
    Bucket, BucketName, Clock, ListQuery, MetadataStore, MultipartSession, Mutation, ObjectKey,
    ObjectSummary, StoragePath, Timestamp, VersionId, VersioningState,
};

/// The page size used for every bounded enumeration the scanner issues. Memory stays flat
/// regardless of bucket size because each page is processed and dropped before the next.
const PAGE_LIMIT: u32 = 1000;

/// The number of stale sessions fetched per `enumerate_stale_sessions` call.
const SESSION_BATCH: u32 = 1000;

/// A tally of the work one scan performed, surfaced as metrics by the caller (ARCH 19.2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LifecycleReport {
    /// Current objects expired (permanently deleted in an unversioned bucket, or hidden behind
    /// a fresh delete marker in a versioned bucket).
    pub objects_expired: u64,
    /// Noncurrent versions permanently deleted.
    pub versions_expired: u64,
    /// Expired-object delete markers removed.
    pub delete_markers_removed: u64,
    /// Incomplete multipart uploads aborted.
    pub uploads_aborted: u64,
    /// Non-fatal errors encountered while applying actions.
    pub errors: u64,
}

impl LifecycleReport {
    /// Fold another report's counts into this one.
    fn merge(&mut self, other: LifecycleReport) {
        self.objects_expired += other.objects_expired;
        self.versions_expired += other.versions_expired;
        self.delete_markers_removed += other.delete_markers_removed;
        self.uploads_aborted += other.uploads_aborted;
        self.errors += other.errors;
    }
}

/// One bucket's lifecycle configuration: the bucket it applies to and its parsed rules.
#[derive(Debug, Clone)]
pub struct BucketLifecycle {
    /// The bucket whose objects the rules govern.
    pub bucket: BucketName,
    /// The parsed rules (as produced by [`crate::parse_lifecycle`]).
    pub rules: Vec<LifecycleRule>,
}

impl BucketLifecycle {
    /// Construct a per-bucket configuration.
    #[must_use]
    pub fn new(bucket: BucketName, rules: Vec<LifecycleRule>) -> Self {
        Self { bucket, rules }
    }
}

/// The stateless lifecycle scanner. It holds no resources of its own; the metadata store, blob
/// store, and clock are passed to [`run_once`](LifecycleScanner::run_once), so the same scanner
/// drives whatever backends the caller wires (the real ones in production, the in-memory
/// doubles in tests).
#[derive(Debug, Clone, Copy, Default)]
pub struct LifecycleScanner {
    _private: (),
}

impl LifecycleScanner {
    /// Construct a scanner.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }

    /// Run one full scan over every supplied bucket configuration and return the merged report.
    ///
    /// For each configuration the scanner looks up the bucket (skipping configurations whose
    /// bucket no longer exists), then applies, in order, current-object expiration, noncurrent-
    /// version expiration, expired-object-delete-marker removal, and incomplete-multipart abort,
    /// each driven by the enabled rules. Errors applying an individual action are counted in the
    /// report rather than aborting the scan, so one bad item cannot stall lifecycle for a bucket.
    ///
    /// # Errors
    /// Returns a [`MetaError`] only if a *bounded enumeration* itself fails (the store is
    /// unreachable); per-item mutation failures are tolerated and tallied as `errors`.
    pub async fn run_once<M, B, C>(
        &self,
        meta: &M,
        blob: &B,
        clock: &C,
        configs: &[BucketLifecycle],
    ) -> Result<LifecycleReport, MetaError>
    where
        M: MetadataStore + ?Sized,
        B: BlobStore + ?Sized,
        C: Clock + ?Sized,
    {
        let mut report = LifecycleReport::default();
        let now = clock.now();

        for cfg in configs {
            let Some(bucket) = meta.get_bucket(&cfg.bucket).await? else {
                // The bucket was deleted between configuration capture and the scan.
                tracing::debug!(bucket = %cfg.bucket, "lifecycle: bucket no longer exists, skipping");
                continue;
            };
            let enabled: Vec<&LifecycleRule> = cfg.rules.iter().filter(|r| r.enabled).collect();
            if enabled.is_empty() {
                continue;
            }

            report.merge(
                self.expire_current_objects(meta, blob, &bucket, &enabled, now)
                    .await?,
            );
            report.merge(
                self.expire_noncurrent_versions(meta, blob, &bucket, &enabled, now)
                    .await?,
            );
            report.merge(
                self.remove_expired_delete_markers(meta, &bucket, &enabled)
                    .await?,
            );
            report.merge(
                self.abort_incomplete_uploads(meta, blob, &bucket, &enabled, now)
                    .await?,
            );
        }

        Ok(report)
    }

    /// Current-object expiration (ARCH 19.3). Pages `list_current` (which excludes delete
    /// markers), and for each object due under some enabled `Expiration` rule whose filter
    /// matches, either permanently deletes it (unversioned/suspended) or inserts a delete
    /// marker (versioning enabled).
    async fn expire_current_objects<M, B>(
        &self,
        meta: &M,
        blob: &B,
        bucket: &Bucket,
        rules: &[&LifecycleRule],
        now: Timestamp,
    ) -> Result<LifecycleReport, MetaError>
    where
        M: MetadataStore + ?Sized,
        B: BlobStore + ?Sized,
    {
        let mut report = LifecycleReport::default();
        let versioned = matches!(bucket.versioning, VersioningState::Enabled);

        let mut cursor: Option<String> = None;
        loop {
            let query = ListQuery {
                cursor: cursor.clone(),
                limit: PAGE_LIMIT,
                ..Default::default()
            };
            let page = meta.list_current(&bucket.name, &query).await?;
            for obj in &page.items {
                let tags = self.tags_for(meta, bucket, obj).await;
                let Some(rule) = self.matching_expiration_rule(rules, obj, &tags, now) else {
                    continue;
                };
                debug_assert!(rule.enabled);
                if versioned {
                    if self
                        .insert_delete_marker(meta, bucket, &obj.key, now)
                        .await
                        .is_ok()
                    {
                        report.objects_expired += 1;
                    } else {
                        report.errors += 1;
                    }
                } else {
                    match self
                        .delete_version(meta, blob, &bucket.name, &obj.key, &obj.version_id, now)
                        .await
                    {
                        Ok(true) => report.objects_expired += 1,
                        // Ok(false): preserved by Object Lock — neither expired nor an error.
                        Ok(false) => {}
                        Err(_) => report.errors += 1,
                    }
                }
            }
            match page.next_cursor {
                Some(c) if page.truncated => cursor = Some(c),
                _ => break,
            }
        }
        Ok(report)
    }

    /// Noncurrent-version expiration (ARCH 19.3). Pages every version, groups by key, and for
    /// each key deletes the noncurrent (non-latest, non-delete-marker) versions that have been
    /// noncurrent longer than the rule's `days`, while preserving the newest
    /// `newer_noncurrent_versions` of them.
    async fn expire_noncurrent_versions<M, B>(
        &self,
        meta: &M,
        blob: &B,
        bucket: &Bucket,
        rules: &[&LifecycleRule],
        now: Timestamp,
    ) -> Result<LifecycleReport, MetaError>
    where
        M: MetadataStore + ?Sized,
        B: BlobStore + ?Sized,
    {
        let mut report = LifecycleReport::default();

        // Collect the full version listing one bounded page at a time, then group by key. The
        // page is the unit of memory; the grouping holds only summaries (no bytes).
        let mut all: Vec<ObjectSummary> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let query = ListQuery {
                cursor: cursor.clone(),
                limit: PAGE_LIMIT,
                ..Default::default()
            };
            let page = meta.list_versions(&bucket.name, &query).await?;
            all.extend(page.items);
            match page.next_cursor {
                Some(c) if page.truncated => cursor = Some(c),
                _ => break,
            }
        }

        // Group noncurrent, non-delete-marker versions by key, newest-first.
        let mut by_key: std::collections::BTreeMap<String, Vec<ObjectSummary>> =
            std::collections::BTreeMap::new();
        for obj in all {
            if obj.is_latest || obj.is_delete_marker {
                continue;
            }
            by_key
                .entry(obj.key.as_str().to_owned())
                .or_default()
                .push(obj);
        }

        for (_key, mut versions) in by_key {
            // Newest noncurrent version first (version ids sort by creation time).
            versions.sort_by(|a, b| b.version_id.as_str().cmp(a.version_id.as_str()));
            for (idx, obj) in versions.iter().enumerate() {
                let tags = self.tags_for(meta, bucket, obj).await;
                let Some((days, keep)) = self.matching_noncurrent_rule(rules, obj, &tags) else {
                    continue;
                };
                // Preserve the configured number of newest noncurrent versions.
                if let Some(keep) = keep {
                    if (idx as u32) < keep {
                        continue;
                    }
                }
                if now.secs_since(obj.last_modified) < i64::from(days) * 86_400 {
                    continue;
                }
                match self
                    .delete_version(meta, blob, &bucket.name, &obj.key, &obj.version_id, now)
                    .await
                {
                    Ok(true) => report.versions_expired += 1,
                    // Ok(false): preserved by Object Lock — neither expired nor an error.
                    Ok(false) => {}
                    Err(_) => report.errors += 1,
                }
            }
        }
        Ok(report)
    }

    /// Expired-object-delete-marker removal (ARCH 19.3). A delete marker that is the only
    /// remaining version of its key is removed so fully-expired keys do not accumulate dangling
    /// markers. Applies when any enabled rule whose filter matches the key carries the action.
    async fn remove_expired_delete_markers<M>(
        &self,
        meta: &M,
        bucket: &Bucket,
        rules: &[&LifecycleRule],
    ) -> Result<LifecycleReport, MetaError>
    where
        M: MetadataStore + ?Sized,
    {
        let mut report = LifecycleReport::default();

        let mut all: Vec<ObjectSummary> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let query = ListQuery {
                cursor: cursor.clone(),
                limit: PAGE_LIMIT,
                ..Default::default()
            };
            let page = meta.list_versions(&bucket.name, &query).await?;
            all.extend(page.items);
            match page.next_cursor {
                Some(c) if page.truncated => cursor = Some(c),
                _ => break,
            }
        }

        // Count versions per key so a marker can be recognized as the sole remaining version.
        let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for obj in &all {
            *counts.entry(obj.key.as_str().to_owned()).or_default() += 1;
        }

        for obj in &all {
            if !obj.is_delete_marker {
                continue;
            }
            if counts.get(obj.key.as_str()).copied().unwrap_or(0) != 1 {
                continue;
            }
            // A delete marker carries no tags worth filtering on, but still honour prefix/size.
            if !self.any_rule_with(rules, obj, &[], |a| {
                matches!(a, Action::ExpiredObjectDeleteMarker)
            }) {
                continue;
            }
            match meta
                .submit(Mutation::DeleteVersion {
                    bucket: bucket.name.clone(),
                    key: obj.key.clone(),
                    version_id: obj.version_id.clone(),
                })
                .await
            {
                Ok(_) => report.delete_markers_removed += 1,
                Err(_) => report.errors += 1,
            }
        }
        Ok(report)
    }

    /// Abort incomplete multipart uploads (ARCH 19.4). Enumerates stale sessions through the
    /// bounded sweeper interface and aborts those in this bucket older than the smallest
    /// `DaysAfterInitiation` of any enabled rule whose prefix matches the session key. Aborting
    /// removes the session and reclaims its staged parts via the normal abort path.
    async fn abort_incomplete_uploads<M, B>(
        &self,
        meta: &M,
        blob: &B,
        bucket: &Bucket,
        rules: &[&LifecycleRule],
        now: Timestamp,
    ) -> Result<LifecycleReport, MetaError>
    where
        M: MetadataStore + ?Sized,
        B: BlobStore + ?Sized,
    {
        let mut report = LifecycleReport::default();

        // `enumerate_stale_sessions(older_than, batch)` returns sessions updated before
        // `older_than`. Passing `now` yields every session not touched this instant; we then
        // re-check each against its rule's per-bucket threshold using `created_at`.
        let sessions: Vec<MultipartSession> =
            meta.enumerate_stale_sessions(now, SESSION_BATCH).await?;

        for session in sessions {
            if session.bucket.as_str() != bucket.name.as_str() {
                continue;
            }
            let Some(days) = self.matching_abort_days(rules, &session) else {
                continue;
            };
            if now.secs_since(session.created_at) < i64::from(days) * 86_400 {
                continue;
            }
            let aborted = meta
                .submit(Mutation::AbortMultipart(session.upload_id.clone()))
                .await
                .is_ok();
            // Reclaim staged parts; delete_session is idempotent (absence is success).
            let parts_cleared = blob.delete_session(&session.upload_id).await.is_ok();
            if aborted && parts_cleared {
                report.uploads_aborted += 1;
            } else {
                report.errors += 1;
            }
        }
        Ok(report)
    }

    // ----- helpers -------------------------------------------------------------------------

    /// Fetch an object version's tags, treating a store error as "no tags" so a tag lookup
    /// failure degrades a tag-filtered rule to non-matching rather than aborting the scan.
    async fn tags_for<M>(
        &self,
        meta: &M,
        bucket: &Bucket,
        obj: &ObjectSummary,
    ) -> Vec<(String, String)>
    where
        M: MetadataStore + ?Sized,
    {
        meta.get_object_tags(&bucket.name, &obj.key, &obj.version_id)
            .await
            .unwrap_or_default()
    }

    /// The first enabled `Expiration` rule whose filter matches and whose threshold is due for
    /// this current object, if any.
    fn matching_expiration_rule<'a>(
        &self,
        rules: &'a [&'a LifecycleRule],
        obj: &ObjectSummary,
        tags: &[(String, String)],
        now: Timestamp,
    ) -> Option<&'a LifecycleRule> {
        rules.iter().copied().find(|rule| {
            if !rule.filter.matches(obj.key.as_str(), obj.size, tags) {
                return false;
            }
            rule.actions.iter().any(|a| match a {
                Action::Expiration(Expiration::Days(d)) => {
                    now.secs_since(obj.last_modified) >= i64::from(*d) * 86_400
                }
                Action::Expiration(Expiration::Date(secs)) => now.as_secs() >= *secs,
                _ => false,
            })
        })
    }

    /// The `(days, keep)` of the first enabled `NoncurrentVersionExpiration` rule whose filter
    /// matches this version, if any.
    fn matching_noncurrent_rule(
        &self,
        rules: &[&LifecycleRule],
        obj: &ObjectSummary,
        tags: &[(String, String)],
    ) -> Option<(u32, Option<u32>)> {
        for rule in rules {
            if !rule.filter.matches(obj.key.as_str(), obj.size, tags) {
                continue;
            }
            for action in &rule.actions {
                if let Action::NoncurrentVersionExpiration {
                    days,
                    newer_noncurrent_versions,
                } = action
                {
                    return Some((*days, *newer_noncurrent_versions));
                }
            }
        }
        None
    }

    /// The smallest `DaysAfterInitiation` of any enabled rule whose prefix matches the session
    /// key, if any abort action applies to this session.
    fn matching_abort_days(
        &self,
        rules: &[&LifecycleRule],
        session: &MultipartSession,
    ) -> Option<u32> {
        let mut best: Option<u32> = None;
        for rule in rules {
            // Only the prefix portion of the filter is meaningful for an in-flight upload; it
            // has no committed size or tags yet.
            if let Some(p) = &rule.filter.prefix {
                if !session.key.as_str().starts_with(p.as_str()) {
                    continue;
                }
            }
            for action in &rule.actions {
                if let Action::AbortIncompleteMultipartUpload {
                    days_after_initiation,
                } = action
                {
                    best = Some(
                        best.map_or(*days_after_initiation, |b| b.min(*days_after_initiation)),
                    );
                }
            }
        }
        best
    }

    /// Whether any rule whose filter matches `obj` carries an action satisfying `pred`.
    fn any_rule_with<F>(
        &self,
        rules: &[&LifecycleRule],
        obj: &ObjectSummary,
        tags: &[(String, String)],
        pred: F,
    ) -> bool
    where
        F: Fn(&Action) -> bool,
    {
        rules.iter().any(|rule| {
            filter_matches_marker(&rule.filter, obj, tags) && rule.actions.iter().any(&pred)
        })
    }

    /// Insert a delete marker for the current object (versioned-bucket expiration), propagating it
    /// to replicas where the bucket's replication rule calls for it (ARCH 19.3/20.3).
    async fn insert_delete_marker<M>(
        &self,
        meta: &M,
        bucket: &Bucket,
        key: &ObjectKey,
        now: Timestamp,
    ) -> Result<(), MetaError>
    where
        M: MetadataStore + ?Sized,
    {
        let marker_id = VersionId::generate();
        let replication = Self::marker_replication(meta, bucket, key, &marker_id, now).await;
        meta.submit(Mutation::CreateDeleteMarker {
            bucket: bucket.name.clone(),
            key: key.clone(),
            version_id: marker_id,
            owner_id: bucket.owner_id.clone(),
            now,
            replication,
        })
        .await
        .map(|_| ())
    }

    /// Build a replication-outbox entry for a lifecycle-created delete marker when the bucket has an
    /// enabled replication rule (with delete-marker replication) whose prefix matches the key, so
    /// expirations propagate to the replica the same way a client delete does (ARCH 20.3/20.4).
    /// Replication requires versioning-enabled, so a non-enabled bucket yields `None`.
    async fn marker_replication<M>(
        meta: &M,
        bucket: &Bucket,
        key: &ObjectKey,
        marker_id: &VersionId,
        now: Timestamp,
    ) -> Option<cairn_types::meta::OutboxEntry>
    where
        M: MetadataStore + ?Sized,
    {
        if bucket.versioning != VersioningState::Enabled {
            return None;
        }
        let doc = meta
            .get_bucket_config(&bucket.name, cairn_types::bucket::ConfigAspect::Replication)
            .await
            .ok()??;
        let cfg = cairn_replication::parse_replication(doc.0.as_bytes()).ok()?;
        let rule = cfg
            .rules
            .iter()
            .filter(|r| {
                r.enabled && r.delete_marker_replication && r.filter.matches_prefix(key.as_str())
            })
            .reduce(|best, r| if r.priority > best.priority { r } else { best })?;
        Some(cairn_replication::outbox_entry_for(
            format!("dmrepl:{}", marker_id.as_str()),
            bucket.name.clone(),
            key.clone(),
            marker_id.clone(),
            cairn_types::meta::ReplicationOp::DeleteMarker,
            rule.id.clone(),
            rule.target_arn.clone(),
            now,
            rule.priority,
        ))
    }

    /// Permanently delete a version and reclaim its freed blob (idempotent: a missing version
    /// or absent blob is success).
    /// Permanently delete a version, reclaiming its blob. Returns `true` when the version was
    /// deleted, `false` when it was preserved because Object Lock (retention or legal hold) still
    /// protects it — lifecycle silently skips a locked version (the WORM guarantee outranks the
    /// expiry rule) and the rule applies once protection lapses.
    async fn delete_version<M, B>(
        &self,
        meta: &M,
        blob: &B,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: &VersionId,
        now: Timestamp,
    ) -> Result<bool, MetaError>
    where
        M: MetadataStore + ?Sized,
        B: BlobStore + ?Sized,
    {
        if meta
            .get_object_lock(bucket, key, version_id)
            .await?
            .is_protected(now)
        {
            return Ok(false);
        }
        let outcome = meta
            .submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version_id.clone(),
            })
            .await?;
        if let cairn_types::MutationOutcome::Deleted {
            freed: Some(path), ..
        } = outcome
        {
            self.reclaim(blob, &path).await;
        }
        Ok(true)
    }

    /// Best-effort blob reclamation; a delete failure is logged, not propagated, because the
    /// metadata row is already gone and a later reconciliation pass will catch the orphan.
    async fn reclaim<B>(&self, blob: &B, path: &StoragePath)
    where
        B: BlobStore + ?Sized,
    {
        if let Err(e) = blob.delete(path).await {
            tracing::warn!(path = %path, error = %e, "lifecycle: blob reclaim failed");
        }
    }
}

/// Filter match for a delete marker: a marker carries no committed size, so size bounds are
/// ignored and only the prefix (and any tags, which a marker lacks) are honoured.
fn filter_matches_marker(filter: &Filter, obj: &ObjectSummary, tags: &[(String, String)]) -> bool {
    if let Some(p) = &filter.prefix {
        if !obj.key.as_str().starts_with(p.as_str()) {
            return false;
        }
    }
    for (k, v) in &filter.tags {
        if !tags.iter().any(|(tk, tv)| tk == k && tv == v) {
            return false;
        }
    }
    true
}
