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
//! key no longer appears, so no second marker is inserted. Writer-atomic freshness guards also
//! make an enumeration-time decision a no-op when a new current version arrives, or when a sole
//! delete marker gains another version, before the mutation commits.
//!
//! Transition to a remote cold tier (ARCH 19.5) is a documented NO-OP placeholder in v1: the
//! scanner recognizes the action but performs no data movement and does not count it.

use crate::config::{Action, Expiration, Filter, LifecycleRule};
use cairn_types::{BlobStore, MetaError};
use cairn_types::{
    Bucket, BucketName, Clock, CurrentVersionGuard, GovernanceBypass, ListQuery, MetadataStore,
    MultipartSession, MultipartTerminalOutcome, Mutation, MutationOutcome, ObjectKey,
    ObjectSummary, StoragePath, Timestamp, VersionId, VersioningState,
};

/// The page size used for every bounded enumeration the scanner issues. Memory stays flat
/// regardless of bucket size because each page is processed and dropped before the next.
pub(crate) const PAGE_LIMIT: u32 = 1000;

/// The number of stale sessions fetched per `enumerate_stale_sessions` call.
const SESSION_BATCH: u32 = 1000;

/// An ordered, paged view of a bucket's version listing that yields one complete key group at a
/// time. A key may span arbitrarily many pages, so the pager retains that one group until the
/// listing advances to a different key; every other completed group is released immediately.
///
/// Peak memory is one metadata page plus the largest single key's version history, never the whole
/// bucket. Keeping a spanning key intact is also what makes noncurrent rank and sole-delete-marker
/// decisions correct at page boundaries.
struct VersionGroupPager<'a, M: MetadataStore + ?Sized> {
    meta: &'a M,
    bucket: &'a BucketName,
    cursor: Option<String>,
    version_id_marker: Option<String>,
    page: std::iter::Peekable<std::vec::IntoIter<ObjectSummary>>,
    finished: bool,
}

impl<'a, M: MetadataStore + ?Sized> VersionGroupPager<'a, M> {
    fn new(meta: &'a M, bucket: &'a BucketName) -> Self {
        Self {
            meta,
            bucket,
            cursor: None,
            version_id_marker: None,
            page: Vec::new().into_iter().peekable(),
            finished: false,
        }
    }

    /// Load the next bounded listing page and advance the paired `(key, version-id)` cursor.
    async fn load_page(&mut self) -> Result<(), MetaError> {
        let page = self
            .meta
            .list_versions(
                self.bucket,
                &ListQuery {
                    cursor: self.cursor.clone(),
                    version_id_marker: self.version_id_marker.clone(),
                    limit: PAGE_LIMIT,
                    ..Default::default()
                },
            )
            .await?;

        if page.truncated {
            let next_cursor = page.next_cursor.ok_or_else(|| {
                MetaError::Engine(
                    "truncated lifecycle version listing omitted its key cursor".to_owned(),
                )
            })?;
            let next_marker = page.next_version_id_marker;
            if self.cursor.as_deref() == Some(next_cursor.as_str())
                && self.version_id_marker.as_deref() == next_marker.as_deref()
            {
                return Err(MetaError::Engine(
                    "lifecycle version listing cursor made no progress".to_owned(),
                ));
            }
            self.cursor = Some(next_cursor);
            self.version_id_marker = next_marker;
        } else {
            self.finished = true;
        }
        self.page = page.items.into_iter().peekable();
        Ok(())
    }

    /// Return the next complete, key-homogeneous group in listing order.
    async fn next_group(&mut self) -> Result<Option<Vec<ObjectSummary>>, MetaError> {
        let mut group: Vec<ObjectSummary> = Vec::new();
        loop {
            if let Some(next) = self.page.peek() {
                if group
                    .first()
                    .is_some_and(|first| first.key.as_str() != next.key.as_str())
                {
                    return Ok(Some(group));
                }
                let Some(item) = self.page.next() else {
                    continue;
                };
                group.push(item);
                continue;
            }

            if self.finished {
                return Ok((!group.is_empty()).then_some(group));
            }
            self.load_page().await?;
        }
    }
}

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
                self.remove_expired_delete_markers(meta, &bucket, &enabled, now)
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
                    match self.insert_delete_marker(meta, bucket, obj, now).await {
                        Ok(true) => report.objects_expired += 1,
                        // Ok(false): the enumerated version was replaced after the scan, or Object
                        // Lock preserved it. Neither outcome is an expiration or an error.
                        Ok(false) => {}
                        Err(_) => report.errors += 1,
                    }
                } else {
                    match self
                        .delete_version(meta, blob, &bucket.name, obj, now)
                        .await
                    {
                        Ok(true) => report.objects_expired += 1,
                        // Ok(false): preserved by Object Lock or overwritten since the scan — neither
                        // expired nor an error.
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

    /// Noncurrent-version expiration (ARCH 19.3). Consumes the ordered version listing one complete
    /// key group at a time and deletes noncurrent (non-latest, non-delete-marker) versions that
    /// have been noncurrent longer than the rule's `days`, while preserving the newest
    /// `newer_noncurrent_versions` of them. A group that crosses page boundaries is carried until
    /// the next key arrives, so rank and supersession time remain exact without bucket-wide memory.
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
        let mut groups = VersionGroupPager::new(meta, &bucket.name);
        while let Some(versions) = groups.next_group().await? {
            report.merge(
                self.expire_noncurrent_group(meta, blob, bucket, rules, now, &versions)
                    .await,
            );
        }
        Ok(report)
    }

    /// Evaluate one complete newest-first key group for noncurrent-version expiration.
    async fn expire_noncurrent_group<M, B>(
        &self,
        meta: &M,
        blob: &B,
        bucket: &Bucket,
        rules: &[&LifecycleRule],
        now: Timestamp,
        versions: &[ObjectSummary],
    ) -> LifecycleReport
    where
        M: MetadataStore + ?Sized,
        B: BlobStore + ?Sized,
    {
        let mut report = LifecycleReport::default();
        // The listing contract is key ASC, version-id DESC: index 0 is the newest version or
        // delete marker. A version becomes noncurrent when the immediately-newer entry is created,
        // so retaining the entire key group preserves its exact supersession timestamp.
        let mut noncurrent_rank = 0u32;
        for (idx, obj) in versions.iter().enumerate() {
            if obj.is_latest || obj.is_delete_marker {
                continue;
            }
            let this_rank = noncurrent_rank;
            noncurrent_rank += 1;
            let tags = self.tags_for(meta, bucket, obj).await;
            let Some((days, keep)) = self.matching_noncurrent_rule(rules, obj, &tags) else {
                continue;
            };
            if keep.is_some_and(|keep| this_rank < keep) {
                continue;
            }
            let Some(newer) = idx.checked_sub(1).and_then(|newer| versions.get(newer)) else {
                // A non-latest entry at index zero violates the ordered-listing/latest invariant.
                // Treat corrupt metadata as a per-item error rather than panicking the scanner.
                report.errors += 1;
                continue;
            };
            if now.secs_since(newer.last_modified) < i64::from(days) * 86_400 {
                continue;
            }
            match self
                .delete_version(meta, blob, &bucket.name, obj, now)
                .await
            {
                Ok(true) => report.versions_expired += 1,
                // Ok(false): preserved by Object Lock or overwritten since the scan.
                Ok(false) => {}
                Err(_) => report.errors += 1,
            }
        }
        report
    }

    /// Expired-object-delete-marker removal (ARCH 19.3). The same key-group pager recognizes a
    /// sole marker as soon as its complete group ends, then releases that group before continuing.
    /// Applies when any enabled rule whose filter matches the key carries the action.
    async fn remove_expired_delete_markers<M>(
        &self,
        meta: &M,
        bucket: &Bucket,
        rules: &[&LifecycleRule],
        now: Timestamp,
    ) -> Result<LifecycleReport, MetaError>
    where
        M: MetadataStore + ?Sized,
    {
        let mut report = LifecycleReport::default();
        let mut groups = VersionGroupPager::new(meta, &bucket.name);
        while let Some(versions) = groups.next_group().await? {
            let [obj] = versions.as_slice() else {
                continue;
            };
            if !obj.is_delete_marker {
                continue;
            }
            // A delete marker carries no tags worth filtering on, but still honour prefix/size.
            if !self.any_rule_with(rules, obj, &[], |a| {
                matches!(a, Action::ExpiredObjectDeleteMarker)
            }) {
                continue;
            }
            match self.delete_expired_marker(meta, bucket, obj, now).await {
                Ok(true) => report.delete_markers_removed += 1,
                Ok(false) => {}
                Err(_) => report.errors += 1,
            }
        }
        Ok(report)
    }

    /// Abort incomplete multipart uploads (ARCH 19.4). Pages every active session in this bucket
    /// through the bounded `(key, upload-id)` listing and aborts those older than the smallest
    /// `DaysAfterInitiation` of any enabled rule whose prefix matches the session key. Per-bucket
    /// paging prevents another bucket (or one hot key) from monopolizing a global first batch.
    /// Aborting removes the session and reclaims its staged parts via the normal abort path.
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

        let mut key_marker = None;
        let mut upload_id_marker = None;
        loop {
            let page = meta
                .list_multipart_uploads(
                    &bucket.name,
                    &ListQuery {
                        cursor: key_marker.clone(),
                        version_id_marker: upload_id_marker.clone(),
                        limit: SESSION_BATCH,
                        ..Default::default()
                    },
                )
                .await?;

            for session in page.items {
                let Some(days) = self.matching_abort_days(rules, &session) else {
                    continue;
                };
                if now.secs_since(session.created_at) < i64::from(days) * 86_400 {
                    continue;
                }
                match meta
                    .submit(Mutation::AbortMultipart(session.upload_id.clone()))
                    .await
                {
                    Ok(MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::Aborted)) => {
                        // Only the terminal winner owns these bytes. A concurrent Complete that
                        // moved the session to `completing` returns NotOwner below and keeps parts.
                        if blob.delete_session(&session.upload_id).await.is_ok() {
                            match meta
                                .submit(Mutation::ReleaseMultipartUploadCleanups {
                                    upload_id: session.upload_id.clone(),
                                })
                                .await
                            {
                                Ok(MutationOutcome::Ack) => report.uploads_aborted += 1,
                                Ok(_) | Err(_) => report.errors += 1,
                            }
                        } else {
                            report.errors += 1;
                        }
                    }
                    Ok(MutationOutcome::MultipartTerminal(MultipartTerminalOutcome::NotOwner)) => {
                        // Expected race: completion owns the session. It reclaims its own parts.
                    }
                    Ok(_) | Err(_) => report.errors += 1,
                }
            }

            if !page.truncated {
                break;
            }
            match (page.next_cursor, page.next_version_id_marker) {
                (Some(next_key), Some(next_upload)) => {
                    key_marker = Some(next_key);
                    upload_id_marker = Some(next_upload);
                }
                _ => {
                    return Err(MetaError::Engine(
                        "truncated multipart lifecycle page had no tuple cursor".to_owned(),
                    ));
                }
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
    pub(crate) async fn insert_delete_marker<M>(
        &self,
        meta: &M,
        bucket: &Bucket,
        obj: &ObjectSummary,
        now: Timestamp,
    ) -> Result<bool, MetaError>
    where
        M: MetadataStore + ?Sized,
    {
        let marker_id = VersionId::generate();
        let replication = Self::marker_replication(meta, bucket, &obj.key, &marker_id, now).await;
        let outcome = meta
            .submit(Mutation::CreateDeleteMarker {
                bucket: bucket.name.clone(),
                key: obj.key.clone(),
                version_id: marker_id,
                owner_id: bucket.owner_id.clone(),
                now,
                bypass: GovernanceBypass::Denied,
                expected_current: Some(CurrentVersionGuard {
                    version_id: obj.version_id.clone(),
                    updated_at: obj.last_modified,
                }),
                replication,
            })
            .await?;
        match outcome {
            MutationOutcome::DeleteMarker { .. } => Ok(true),
            MutationOutcome::DeleteNotApplied | MutationOutcome::DeleteProtected => Ok(false),
            _ => Err(MetaError::Engine(
                "unexpected lifecycle delete-marker outcome".to_owned(),
            )),
        }
    }

    /// Delete a lifecycle-enumerated sole delete marker only while it is still the sole version.
    ///
    /// Replication may insert an older noncurrent version after enumeration. The writer guard keeps
    /// that arrival from being exposed by a stale sole-marker cleanup decision.
    pub(crate) async fn delete_expired_marker<M>(
        &self,
        meta: &M,
        bucket: &Bucket,
        obj: &ObjectSummary,
        now: Timestamp,
    ) -> Result<bool, MetaError>
    where
        M: MetadataStore + ?Sized,
    {
        let outcome = meta
            .submit(Mutation::DeleteVersion {
                bucket: bucket.name.clone(),
                key: obj.key.clone(),
                version_id: obj.version_id.clone(),
                expected_row_id: Some(obj.row_id.clone()),
                expected_updated_at: Some(obj.last_modified),
                require_sole_key_version: true,
                now,
                bypass: GovernanceBypass::Denied,
            })
            .await?;
        match outcome {
            MutationOutcome::Deleted { .. } => Ok(true),
            MutationOutcome::DeleteNotApplied | MutationOutcome::DeleteProtected => Ok(false),
            _ => Err(MetaError::Engine(
                "unexpected expired delete-marker outcome".to_owned(),
            )),
        }
    }

    /// Build the replication-outbox entries for a lifecycle-created delete marker — one per distinct
    /// destination target whose delete-marker-replication rule matches the key (1→N fan-out), so
    /// expirations propagate to every replica the same way a client delete does (ARCH 20.2/20.3/20.4).
    /// Rule matching mirrors the protocol's delete path (`matches` against an empty tag set).
    /// Replication requires versioning-enabled, so a non-enabled bucket yields an empty vec.
    async fn marker_replication<M>(
        meta: &M,
        bucket: &Bucket,
        key: &ObjectKey,
        marker_id: &VersionId,
        now: Timestamp,
    ) -> Vec<cairn_types::meta::OutboxEntry>
    where
        M: MetadataStore + ?Sized,
    {
        if bucket.versioning != VersioningState::Enabled {
            return Vec::new();
        }
        let Some(doc) = meta
            .get_bucket_config(&bucket.name, cairn_types::bucket::ConfigAspect::Replication)
            .await
            .ok()
            .flatten()
        else {
            return Vec::new();
        };
        let Ok(cfg) = cairn_replication::parse_replication(doc.0.as_bytes()) else {
            return Vec::new();
        };
        // The entry id is scoped by rule (`dmrepl:{rule}:{marker}`) so N targets never collide on
        // one primary key — a fan-out delete marker enqueues one durable row per target.
        cfg.matching_rules_for_all(key.as_str(), &[], true)
            .into_iter()
            .map(|rule| {
                cairn_replication::outbox_entry_for(
                    format!("dmrepl:{}:{}", rule.id, marker_id.as_str()),
                    bucket.name.clone(),
                    key.clone(),
                    marker_id.clone(),
                    cairn_types::meta::ReplicationOp::DeleteMarker,
                    rule.id.clone(),
                    rule.target_arn.clone(),
                    now,
                    rule.priority,
                )
            })
            .collect()
    }

    /// Permanently delete a version, reclaiming its blob. Returns `true` only when the writer
    /// confirms that the row was deleted, and `false` when Object Lock preserves it or a concurrent
    /// change made the compare-and-delete guard stale. Lifecycle silently skips both outcomes; a
    /// protected version becomes eligible once protection lapses, while a lost race is reconsidered
    /// from fresh metadata on a later scan.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn delete_version<M, B>(
        &self,
        meta: &M,
        blob: &B,
        bucket: &BucketName,
        obj: &ObjectSummary,
        now: Timestamp,
    ) -> Result<bool, MetaError>
    where
        M: MetadataStore + ?Sized,
        B: BlobStore + ?Sized,
    {
        // Compare-and-delete on the immutable row id captured at enumeration. Unversioned writes
        // reuse the null version sentinel and may share one timestamp tick, but every replacement
        // gets a new row id. `updated_at` remains a defense-in-depth freshness predicate.
        let outcome = meta
            .submit(Mutation::DeleteVersion {
                bucket: bucket.clone(),
                key: obj.key.clone(),
                version_id: obj.version_id.clone(),
                expected_row_id: Some(obj.row_id.clone()),
                expected_updated_at: Some(obj.last_modified),
                require_sole_key_version: false,
                now,
                bypass: GovernanceBypass::Denied,
            })
            .await?;
        match outcome {
            MutationOutcome::Deleted {
                freed: Some(path), ..
            } => {
                self.reclaim(blob, &path).await;
                Ok(true)
            }
            MutationOutcome::Deleted { freed: None, .. } => Ok(true),
            MutationOutcome::DeleteNotApplied => Ok(false),
            MutationOutcome::DeleteProtected => Ok(false),
            _ => Err(MetaError::Engine(
                "unexpected lifecycle delete outcome".to_owned(),
            )),
        }
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

#[cfg(test)]
mod outcome_tests {
    use super::*;
    use cairn_types::testing::{InMemoryBlobStore, InMemoryMetadataStore};
    use cairn_types::{ETag, StorageClass, UserId};

    #[tokio::test]
    async fn delete_not_applied_does_not_advance_lifecycle_counters() {
        let deleted = LifecycleScanner::new()
            .delete_version(
                &InMemoryMetadataStore::new(),
                &InMemoryBlobStore::new(),
                &BucketName::parse("missing-bucket").unwrap(),
                &ObjectSummary {
                    row_id: "missing-row".to_owned(),
                    key: ObjectKey::parse("missing-key").unwrap(),
                    version_id: VersionId::from_string("missing-version".to_owned()),
                    is_latest: true,
                    is_delete_marker: false,
                    etag: ETag::from_string("missing".to_owned()),
                    size: 0,
                    last_modified: Timestamp::EPOCH,
                    storage_class: StorageClass::Standard,
                    owner_id: UserId("missing-owner".to_owned()),
                },
                Timestamp::EPOCH,
            )
            .await
            .unwrap();

        assert!(
            !deleted,
            "a writer no-op must not be reported as a lifecycle deletion"
        );
    }
}
