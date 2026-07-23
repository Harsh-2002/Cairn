//! The replication **audit**: the source-side enumeration behind `cairn replication audit` and the
//! slow-cadence suspect gauges (ARCH 20.5, 26.4).
//!
//! ## Why this exists — and why it reads the version row, not the outbox
//!
//! Before release X the replication engine read source blobs with **no data key** (see
//! `docs/replication.md` 20.1). An SSE / `CAIRN_ENCRYPT_AT_REST` version was therefore shipped as
//! raw **ciphertext**. The destination's only integrity signal on the replication PUT is a re-emitted
//! flexible checksum (`cairn-replication/src/sink.rs`) — there is no `Content-MD5`, the source ETag
//! is never sent, and a composite multipart checksum is deliberately skipped — so:
//!
//! * with a supplementary checksum on the source object, the destination rejected the ciphertext
//!   with `400 BadDigest`, which is **terminal**. The version is stamped `failed` and is simply
//!   **absent** on the mirror. Harmless, but silent.
//! * without one (a multipart-completed object, a `curl`/presigned PUT, an older SDK), the
//!   destination **accepted** it. The replica exists, has exactly the right size, answers `200`, and
//!   is **garbage**. This is the dangerous population: anyone who failed over to the mirror in that
//!   window restored garbage and got a `200` while doing it.
//!
//! Two consequences shape this module:
//!
//! 1. **The outbox is not the ledger.** `CAIRN_REPLICATION_RETENTION_SECS` (default 86 400) prunes
//!    `completed` **and** `failed` outbox rows, so `GET /replication/failed` only ever shows the last
//!    24 h and is dangerously reassuring. The durable ledger is `object_versions.replication_status`,
//!    which is never pruned — so that is what this enumerates.
//! 2. **Remote comparison is meaningless.** The engine requested `[0, size_logical)` — the
//!    *plaintext* length — so a corrupt replica has exactly the right number of wrong bytes; and a
//!    multipart source replicates as a single-part PUT, so the ETag differs even for a **correct**
//!    replica. Neither size nor ETag separates good from bad. Enumeration is source-side, and the
//!    only honest remote check is a full byte comparison ([`ReplicaVerifier`], opt-in `--verify`).
//!
//! The suspect population is therefore: `sse_descriptor IS NOT NULL` **and** `replication_status IN
//! (completed, failed)` **and** the bucket has an enabled replication rule selecting the key **and
//! `created_at < cutoff` **and** `(replicated_at IS NULL OR replicated_at < cutoff)`. `failed` ⇒
//! almost certainly **absent** on the mirror; `completed` ⇒ **present and suspect**. The two are
//! reported separately because their remediation urgency is not remotely the same.
//!
//! ## 3. The cutoff is mandatory, because without it the signal cannot converge
//!
//! Only versions written by the **pre-fix binary** can be damaged. A version encrypted and
//! replicated *after* the fix is still `sse_descriptor IS NOT NULL` and still `completed` — it is
//! perfectly healthy, and an unbounded predicate counts it forever. That is not a conservative
//! over-report, it is a broken signal: the gauge would sit permanently non-zero on every healthy
//! node using SSE with replication, an alert on it would be meaningless, and a runbook telling the
//! operator to "watch it fall to 0" would be describing something that cannot happen.
//!
//! So [`audit_store`] takes a `created_before` cutoff and the operator supplies it: the moment they
//! deployed the fix. `--before` on the CLI, `CAIRN_REPLICATION_AUDIT_BEFORE` for the background
//! gauges — and that env knob is **unset by default**, so the loop does not run and no gauge is
//! emitted until an operator has decided what the cutoff is.
//!
//! ## 4. Convergence is `suspect == 0 AND repair_pending == 0`
//!
//! A forced requeue flips the damaged rows to `pending`, which the suspect predicate excludes. Read
//! naively, the suspect count therefore drops to zero the instant the repair is *queued* — before a
//! single byte has been re-shipped — and then climbs back as entries complete. Counting only
//! suspects would show the operator a success that has not happened yet, followed by an alarming
//! regression that is actually progress.
//!
//! [`BucketAudit::repair_pending`] closes that: an in-window encrypted version stamped
//! `pending`/`claimed` is repair **in flight**, not clean. It is published as its own gauge, and the
//! runbook's convergence condition is that *both* reach zero.
//!
//! ## 5. `replicated_at` is what lets a REPAIRED version leave the population
//!
//! The cutoff on `created_at` alone excludes versions written after the fix. It does **not** exclude
//! versions that were damaged and have since been *repaired* — and that is the difference between a
//! gauge and a decoration. A damaged version has `created_at = T0 < cutoff`; a forced requeue flips
//! it to `pending`, it re-ships correctly, and `MarkReplicationDone` stamps it `completed` again.
//! `created_at` is never rewritten. On a `created_at`-only predicate that version matches again,
//! **permanently**: the suspect count returns to its pre-repair value precisely because the repair
//! SUCCEEDED, and the operator watching the dashboard concludes it failed.
//!
//! Schema v23 therefore adds `object_versions.replicated_at`, stamped by `MarkReplicationDone` in
//! the same UPDATE as the status, and the predicate becomes
//! `created_at < cutoff AND (replicated_at IS NULL OR replicated_at < cutoff)`. A repaired version
//! gets `replicated_at > cutoff` and drops out; the gauge genuinely falls to zero and alerting on it
//! means something.
//!
//! `replicated_at IS NULL` stays **suspect** on purpose, and it has a cost. It is NULL for a version
//! that never shipped (correct: it is absent on the mirror, which is damage), but it is *also* NULL
//! for every row written before v23 — including ones that replicated perfectly well years ago. So a
//! node upgraded mid-incident over-reports healthy old versions as suspect on its first audit, and
//! that resolves itself as those versions are re-shipped and stamped. This is deliberate: an
//! over-report costs a wasted re-ship, an under-report leaves a garbage replica nobody ever looks
//! for. The runbook says so plainly (`docs/operations.md` 8.7).

use cairn_types::bucket::ConfigAspect;
use cairn_types::meta::{ListQuery, ReplicationStatus};
use cairn_types::object::ObjectVersionRow;
use cairn_types::sse::SseMode;
use cairn_types::time::Timestamp;
use cairn_types::traits::MetadataStore;

/// The page size used when walking versions; bounds memory per round. Matches `repair_dangling_rows`.
const AUDIT_PAGE_LIMIT: u32 = 1000;
/// Upper bound on paging iterations per bucket, so a hostile/corrupt cursor can never spin forever.
const AUDIT_MAX_PAGES: u32 = 100_000;

/// How a `--verify` byte comparison of one replica resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerifyOutcome {
    /// The replica's bytes hash to the source's plaintext MD5. Good.
    Matched,
    /// The replica exists and is the wrong bytes. **This is a confirmed corrupt replica.**
    Mismatched,
    /// The destination has no such object (the `failed`/BadDigest population).
    Absent,
    /// The check could not be completed (transport, credentials, a local key failure). Reported
    /// separately so an unreachable destination never reads as "all clean".
    Errored,
}

/// A destination-side byte check for one source version. Implemented in `main.rs`, where the blob
/// store, the master ring and the opened replication targets are all in hand; kept behind a trait so
/// the enumeration below is testable against the in-memory doubles with no network at all.
#[async_trait::async_trait]
pub(crate) trait ReplicaVerifier: Send + Sync {
    /// Fetch the replica of `row` from its destination and compare it against the **source
    /// plaintext** MD5 (never the stored ETag: a multipart source's ETag is composite and cannot be
    /// compared to a single-part replica).
    async fn verify(&self, row: &ObjectVersionRow) -> VerifyOutcome;
}

/// One suspect version, for the sample list an operator actually reads.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct SuspectVersion {
    /// The object key.
    pub key: String,
    /// The version id.
    pub version_id: String,
    /// `completed` (present and suspect) or `failed` (absent on the mirror).
    pub status: &'static str,
    /// Whether this is the bucket's current version — a resync backfill repairs **only** these.
    pub is_latest: bool,
    /// The logical size in bytes.
    pub size: u64,
    /// The SSE mode from the stored descriptor (`sse-s3` / `at-rest` / `kms`).
    pub mode: &'static str,
}

/// The audit result for one bucket that has at least one enabled replication rule.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct BucketAudit {
    /// The source bucket.
    pub bucket: String,
    /// Total versions walked (every version, not just the suspects).
    pub versions_scanned: u64,
    /// Encrypted versions stamped `completed`: **present on the mirror and suspect**. The dangerous
    /// population — the replica answers 200 and may be garbage.
    pub present_and_suspect: u64,
    /// Encrypted versions stamped `failed`: almost certainly **absent** on the mirror. Harmless
    /// (nothing to restore), but the object is unprotected.
    pub absent: u64,
    /// In-window encrypted versions stamped `pending`/`claimed`: **repair in flight**. A forced
    /// requeue moves damaged rows here, so they leave [`present_and_suspect`](Self::present_and_suspect)
    /// the moment the repair is *queued*, long before any byte re-ships. Convergence is
    /// `present_and_suspect == 0 AND repair_pending == 0` — never the first alone.
    pub repair_pending: u64,
    /// Of [`present_and_suspect`](Self::present_and_suspect), how many are **not** the current
    /// version. TRAP 2: a resync backfill enumerates current versions only, so these are **not
    /// repaired** by any command here.
    pub non_current_suspect: u64,
    /// Of the suspects, how many were encrypted because the **client** asked (`SseS3`/`Kms`) rather
    /// than by `CAIRN_ENCRYPT_AT_REST`. Only these are gated by the plaintext-over-http refusal.
    pub client_encrypted_suspect: u64,
    /// TRAP 1: whether some enabled rule sets `ExistingObjectReplication`. When `false`, a resync
    /// returns success and repairs **nothing**.
    pub existing_object_replication: bool,
    /// TRAP 3: destination endpoints that are `http://`. Repair re-ships **plaintext**, so the
    /// Stage-1 confidentiality gate refuses a client-encrypted object to these unless
    /// `CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP=true`.
    pub plaintext_http_endpoints: Vec<String>,
    /// Whether TRAP 3 actually bites for this bucket: there is a client-encrypted suspect, a
    /// plaintext endpoint, and the opt-in is off. A repair started in this state defers **forever**.
    pub repair_blocked_by_http_gate: bool,
    /// `--verify` only: replicas whose bytes matched the source plaintext.
    pub verified_matched: u64,
    /// `--verify` only: replicas that exist and are the **wrong bytes**. Confirmed corruption.
    pub verified_mismatched: u64,
    /// `--verify` only: replicas the destination does not have.
    pub verified_absent: u64,
    /// `--verify` only: checks that could not be completed.
    pub verify_errors: u64,
    /// `--verify` only: suspects **skipped** because they are not the current version.
    ///
    /// The verifier signs a plain `GET /{bucket}/{key}` with no `versionId`, so it can only ever
    /// fetch the destination's *current* object. Running it on a superseded source version compares
    /// two different objects and reports `Mismatched` — "confirmed corruption" — for a perfectly
    /// healthy mirror. Since TRAP 2 says non-current versions are unrepairable here anyway, that
    /// would be a pile of alarming numbers the operator cannot act on. They are skipped and counted.
    pub verify_skipped_non_current: u64,
    /// A bounded sample of the suspect versions, `completed` first.
    pub samples: Vec<SuspectVersion>,
}

/// Store-wide audit totals.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct AuditReport {
    /// Per-bucket detail; only buckets with an enabled replication rule appear.
    pub buckets: Vec<BucketAudit>,
    /// Sum of [`BucketAudit::present_and_suspect`].
    pub present_and_suspect: u64,
    /// Sum of [`BucketAudit::absent`].
    pub absent: u64,
    /// Sum of [`BucketAudit::repair_pending`] — repair in flight. Convergence needs this at zero
    /// too, not just [`present_and_suspect`](Self::present_and_suspect).
    pub repair_pending: u64,
    /// Sum of [`BucketAudit::non_current_suspect`] — the part of
    /// [`present_and_suspect`](Self::present_and_suspect) that **no command here can repair** (TRAP
    /// 2: the resync backfill enumerates current versions only).
    ///
    /// This is the FLOOR of the suspect count, and it is reported store-wide precisely so the
    /// runbook's done-state is checkable: the reachable end state is `repair_pending == 0 AND
    /// present_and_suspect == non_current_suspect`. Asserting `present_and_suspect == 0` where
    /// non-current suspects exist would be describing something that cannot happen without
    /// rebuilding the destination bucket.
    pub non_current_suspect: u64,
    /// The cutoff the audit ran with, as epoch seconds: only versions created strictly before this
    /// are in scope. Echoed so a JSON report is self-describing (the number is the whole meaning of
    /// the counts above).
    pub created_before: i64,
}

/// Parse an audit cutoff: either an RFC-3339 timestamp (`2026-07-23T10:00:00Z`, with an optional
/// fractional part and an optional `Z`/`±hh:mm` offset) or bare whole seconds since the Unix epoch.
///
/// Used by both `--before` and `CAIRN_REPLICATION_AUDIT_BEFORE`, so an operator writes the same
/// thing in both places, and by [`Config::validate`](crate::config::Config::validate) so an
/// unparseable value fails at startup rather than six hours later inside a background loop.
///
/// # Errors
/// A message naming both accepted forms — this value is typed by a human under incident pressure.
pub(crate) fn parse_cutoff(s: &str) -> Result<Timestamp, String> {
    let s = s.trim();
    if let Ok(secs) = s.parse::<i64>() {
        return Ok(Timestamp(secs));
    }
    parse_rfc3339_secs(s).map(Timestamp).ok_or_else(|| {
        format!(
            "cannot parse {s:?} as a timestamp: expected RFC-3339 \
             (e.g. 2026-07-23T10:00:00Z) or whole seconds since the Unix epoch (e.g. 1753264800)"
        )
    })
}

/// A minimal RFC-3339 parser yielding whole epoch seconds; `None` on any structural problem.
/// Accepts `YYYY-MM-DDThh:mm:ss` with an optional fractional part and an optional `Z` or `±hh:mm`
/// offset. Deliberately dependency-free and second-granular: the cutoff is a deployment moment, and
/// nothing here is worth a date-time crate.
fn parse_rfc3339_secs(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19 {
        return None;
    }
    let num = |r: std::ops::Range<usize>| -> Option<i64> { s.get(r)?.parse::<i64>().ok() };
    if b[4] != b'-' || b[7] != b'-' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    if b[10] != b'T' && b[10] != b't' && b[10] != b' ' {
        return None;
    }
    let (year, month, day) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (hour, minute, second) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    // An optional offset, after any fractional-seconds part.
    let rest = &s[19..];
    let tz = rest.trim_start_matches(|c: char| c == '.' || c.is_ascii_digit());
    let offset_secs = match tz.as_bytes().first() {
        None | Some(b'Z') | Some(b'z') => 0,
        Some(&sign @ b'+') | Some(&sign @ b'-') => {
            if tz.len() < 6 || tz.as_bytes()[3] != b':' {
                return None;
            }
            let mag =
                tz.get(1..3)?.parse::<i64>().ok()? * 3600 + tz.get(4..6)?.parse::<i64>().ok()? * 60;
            if sign == b'+' { mag } else { -mag }
        }
        _ => return None,
    };

    // Howard Hinnant's days-from-civil, proleptic Gregorian.
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + hour * 3_600 + minute * 60 + second - offset_secs)
}

/// Walk the store (or one bucket) and report the suspect population.
///
/// Buckets with no replication configuration, or none of whose rules are enabled, are skipped
/// entirely — they are not a mirror, so nothing about them is suspect. This is also what keeps the
/// background gauge pass near-free on the overwhelming majority of deployments, which replicate
/// nothing.
///
/// Rule selection is evaluated by **prefix only**: a tag-filtered rule may make this over-report,
/// which is the correct direction for an audit (a suspect that turns out never to have been selected
/// costs one wasted re-ship; a missed one costs a garbage replica nobody looks for).
///
/// `created_before` is the mandatory cutoff: only versions created strictly before it can have been
/// shipped by the pre-fix binary, and only those are suspect. See the module header — without it the
/// count includes every correctly-replicated encrypted version and can never reach zero.
///
/// `verifier` is the opt-in `--verify` byte check; `None` is the cheap default.
///
/// # Cost
/// The near-free half is real but partial: a bucket with **no enabled replication rule** is skipped
/// before a single version is read, so a store that does not replicate costs one `list_buckets` plus
/// one config read per bucket. A bucket that **does** replicate costs a full version listing *plus
/// one `get_version` point query per listed version* — `ObjectSummary` carries no `sse_descriptor`,
/// so there is no way to decide suspicion from the page alone. On a large replicated bucket that is
/// the dominant cost of the pass, and it is why this runs on a slow opt-in cadence and never per
/// metrics scrape.
///
/// # Errors
/// Any metadata-store failure, as a display string (this is CLI/loop surface, not a typed API).
pub(crate) async fn audit_store(
    meta: &dyn MetadataStore,
    bucket_filter: Option<&str>,
    created_before: Timestamp,
    sample_limit: usize,
    allow_plaintext_sse_over_http: bool,
    env_endpoint: Option<&str>,
    verifier: Option<&dyn ReplicaVerifier>,
) -> Result<AuditReport, String> {
    let buckets = meta.list_buckets(None).await.map_err(|e| e.to_string())?;
    let mut report = AuditReport {
        created_before: created_before.0,
        ..AuditReport::default()
    };

    for bucket in &buckets {
        if bucket_filter.is_some_and(|f| f != bucket.name.as_str()) {
            continue;
        }
        // The replication configuration decides whether this bucket is in scope at all.
        let doc = match meta
            .get_bucket_config(&bucket.name, ConfigAspect::Replication)
            .await
        {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(e) => return Err(e.to_string()),
        };
        let Ok(cfg) = cairn_replication::parse_replication(doc.0.as_bytes()) else {
            // An unparseable rule document means nothing replicates from this bucket; it is a
            // configuration fault, not a suspect population.
            continue;
        };
        if !cfg.rules.iter().any(|r| r.enabled) {
            continue;
        }

        // Endpoint scheme per enabled rule, for the http-gate flag (TRAP 3). Target endpoints are
        // stored in the clear (only the secret is sealed), so this needs no master key.
        let targets = match meta
            .get_bucket_config(&bucket.name, ConfigAspect::ReplicationTargets)
            .await
        {
            Ok(Some(d)) => cairn_replication::parse_targets(d.0.as_bytes()).unwrap_or_default(),
            Ok(None) => Vec::new(),
            Err(e) => return Err(e.to_string()),
        };
        let mut plaintext_http_endpoints: Vec<String> = Vec::new();
        for rule in cfg.rules.iter().filter(|r| r.enabled) {
            let endpoint = match &rule.target_arn {
                Some(arn) => cairn_replication::resolve_target(&targets, arn)
                    .map(|t| t.endpoint.clone())
                    .unwrap_or_default(),
                None => env_endpoint.unwrap_or_default().to_owned(),
            };
            if endpoint.starts_with("http://") && !plaintext_http_endpoints.contains(&endpoint) {
                plaintext_http_endpoints.push(endpoint);
            }
        }

        let mut audit = BucketAudit {
            bucket: bucket.name.as_str().to_owned(),
            existing_object_replication: cfg
                .rules
                .iter()
                .any(|r| r.enabled && r.existing_object_replication),
            plaintext_http_endpoints,
            ..BucketAudit::default()
        };

        let mut cursor: Option<String> = None;
        // A version page resumes on the (key, version-id) PAIR; feeding only the key half re-lists
        // a key with more versions than one page at every boundary (issue #7).
        let mut vmarker: Option<String> = None;
        'paging: for _ in 0..AUDIT_MAX_PAGES {
            let query = ListQuery {
                cursor: cursor.clone(),
                version_id_marker: vmarker.clone(),
                limit: AUDIT_PAGE_LIMIT,
                ..Default::default()
            };
            let page = meta
                .list_versions(&bucket.name, &query)
                .await
                .map_err(|e| e.to_string())?;
            if page.items.is_empty() {
                break;
            }
            for item in &page.items {
                audit.versions_scanned += 1;
                // A delete marker has no body and no descriptor; propagating one is unaffected.
                if item.is_delete_marker {
                    continue;
                }
                if cfg.matching_rule(item.key.as_str()).is_none() {
                    continue;
                }
                let row = match meta
                    .get_version(&bucket.name, &item.key, &item.version_id)
                    .await
                {
                    Ok(Some(r)) => r,
                    // Deleted between the listing and the read: nothing to audit.
                    Ok(None) => continue,
                    Err(e) => return Err(e.to_string()),
                };
                let Some(descriptor) = row.sse_descriptor.as_deref() else {
                    continue;
                };
                // THE CUTOFF, on BOTH clocks. A version written after the operator deployed the fix
                // was shipped through the DEK-aware path and is healthy, however encrypted and
                // however `completed` it reads. Counting it would make the gauge permanently
                // non-zero on a healthy node and destroy the only signal this module exists to
                // provide.
                //
                // `replicated_at` is the second half, and without it the gauge cannot CONVERGE: a
                // damaged version keeps `created_at = T0 < cutoff` forever, so once its repair
                // succeeds and re-stamps it `completed` it would match this predicate again,
                // permanently — the suspect count would return to its pre-repair value *because the
                // repair worked*. A repaired version has `replicated_at > cutoff` and drops out
                // here. `None` (never shipped, or a pre-v23 row) stays suspect on purpose; see the
                // module header for why that over-report is the right side to err on.
                if row.created_at.0 >= created_before.0 {
                    continue;
                }
                if row.replicated_at.is_some_and(|t| t.0 >= created_before.0) {
                    continue;
                }
                let status = match row.replication_status {
                    Some(ReplicationStatus::Completed) => "completed",
                    Some(ReplicationStatus::Failed) => "failed",
                    // An IN-WINDOW encrypted version sitting in `pending`/`claimed` is repair in
                    // flight: a forced requeue put it there. It is NOT clean, and reporting it as
                    // clean is what would make the convergence signal read backwards (suspects fall
                    // to zero when the repair is queued, then climb as it completes). Counted
                    // separately; the runbook's done condition is both at zero.
                    Some(ReplicationStatus::Pending) | Some(ReplicationStatus::Claimed) => {
                        audit.repair_pending += 1;
                        continue;
                    }
                    // `replica` is inbound and never shipped from here; an unstamped row was never
                    // enqueued at all.
                    _ => continue,
                };
                // The descriptor's `mode` decides whether the http gate applies: only encryption
                // the CLIENT asked for is a contract the sink must not break by putting the
                // decrypted body on an unauthenticated link. `AtRest` is an operator property.
                let mode = cairn_types::sse::parse_descriptor(descriptor)
                    .map(|d| d.mode)
                    .unwrap_or_default();
                let client_encrypted = matches!(mode, SseMode::SseS3 | SseMode::Kms);

                if status == "completed" {
                    audit.present_and_suspect += 1;
                    if !row.is_latest {
                        audit.non_current_suspect += 1;
                    }
                } else {
                    audit.absent += 1;
                }
                if client_encrypted {
                    audit.client_encrypted_suspect += 1;
                }

                // Verify CURRENT versions only. The verifier's GET carries no `versionId`, so for a
                // superseded source version it fetches the destination's *current* object and the
                // comparison is between two different objects — reported as `Mismatched`, i.e.
                // "confirmed corruption", against a mirror that may be entirely healthy. TRAP 2
                // already says these are unrepairable here; manufacturing false confirmations of
                // corruption on top of that is strictly worse than saying nothing.
                if let Some(v) = verifier {
                    if row.is_latest {
                        match v.verify(&row).await {
                            VerifyOutcome::Matched => audit.verified_matched += 1,
                            VerifyOutcome::Mismatched => audit.verified_mismatched += 1,
                            VerifyOutcome::Absent => audit.verified_absent += 1,
                            VerifyOutcome::Errored => audit.verify_errors += 1,
                        }
                    } else {
                        audit.verify_skipped_non_current += 1;
                    }
                }

                // Only the sample LIST is capped; the counts above are always exact. A zero budget
                // is the background-gauge path, which wants counts and nothing else.
                if audit.samples.len() < sample_limit {
                    audit.samples.push(SuspectVersion {
                        key: item.key.as_str().to_owned(),
                        version_id: item.version_id.as_str().to_owned(),
                        status,
                        is_latest: row.is_latest,
                        size: row.size_logical,
                        mode: mode_str(mode),
                    });
                }
            }
            match page.next_cursor {
                Some(next) => {
                    cursor = Some(next);
                    vmarker = page.next_version_id_marker;
                }
                None => break 'paging,
            }
        }

        // TRAP 3 bites only when all three hold at once.
        audit.repair_blocked_by_http_gate = audit.client_encrypted_suspect > 0
            && !audit.plaintext_http_endpoints.is_empty()
            && !allow_plaintext_sse_over_http;

        // `completed` first: an operator reads the dangerous population before the harmless one.
        audit
            .samples
            .sort_by(|a, b| a.status.cmp(b.status).then_with(|| a.key.cmp(&b.key)));

        report.present_and_suspect += audit.present_and_suspect;
        report.absent += audit.absent;
        report.repair_pending += audit.repair_pending;
        report.non_current_suspect += audit.non_current_suspect;
        report.buckets.push(audit);
    }

    Ok(report)
}

/// The wire token for an [`SseMode`], matching the descriptor's kebab-case serde representation.
fn mode_str(m: SseMode) -> &'static str {
    match m {
        SseMode::SseS3 => "sse-s3",
        SseMode::AtRest => "at-rest",
        SseMode::Kms => "kms",
    }
}

#[cfg(test)]
mod tests {
    use super::{AuditReport, ReplicaVerifier, VerifyOutcome, audit_store, parse_cutoff};
    use cairn_types::authz::OwnershipMode;
    use cairn_types::bucket::{Bucket, ConfigAspect, ConfigDoc, VersioningState};
    use cairn_types::id::{BucketName, ObjectKey, StoragePath, UserId, VersionId};
    use cairn_types::meta::{Mutation, Precondition, ReplicationStatus};
    use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
    use cairn_types::testing::InMemoryMetadataStore;
    use cairn_types::time::Timestamp;
    use cairn_types::traits::MetadataStore;

    /// An enabled, unfiltered rule with no `ExistingObjectReplication` — the default an operator
    /// actually has, and the one that makes a resync a silent no-op (TRAP 1).
    const RULE_XML: &str = r#"<ReplicationConfiguration><Role>r</Role><Rule><ID>r1</ID><Status>Enabled</Status><Prefix></Prefix><Destination><Bucket>arn:aws:s3:::mirror</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    /// A plain SSE-S3 descriptor (no `mode` field — the legacy/default form).
    const SSE_S3: &str = r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":""}"#;
    /// A transparent at-rest descriptor: encrypted, but never a client contract.
    const AT_REST: &str =
        r#"{"alg":"AES256-GCM","wrapped_dek_b64":"AAAA","nonce_b64":"","mode":"at-rest"}"#;

    async fn seed_bucket(meta: &InMemoryMetadataStore, name: &str, rules: Option<&str>) {
        meta.submit(Mutation::CreateBucket(Box::new(Bucket {
            name: BucketName::parse(name).unwrap(),
            owner_id: UserId("root".to_owned()),
            created_at: Timestamp(1),
            versioning: VersioningState::Enabled,
            ownership_mode: OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".to_owned(),
            compression: None,
        })))
        .await
        .unwrap();
        if let Some(xml) = rules {
            set_config(meta, name, ConfigAspect::Replication, xml).await;
        }
    }

    async fn set_config(
        meta: &InMemoryMetadataStore,
        bucket: &str,
        aspect: ConfigAspect,
        doc: &str,
    ) {
        meta.submit(Mutation::SetBucketConfig {
            bucket: BucketName::parse(bucket).unwrap(),
            aspect,
            doc: Some(ConfigDoc(doc.to_owned())),
        })
        .await
        .unwrap();
    }

    /// The audit cutoff every test runs with. Versions seeded at `created_at = 1` are *before* it
    /// (i.e. written by the pre-fix binary and therefore in scope); anything seeded at or after it
    /// is a post-fix write and must be invisible to the audit.
    const CUTOFF: Timestamp = Timestamp(1_000);

    async fn seed_version(
        meta: &InMemoryMetadataStore,
        bucket: &str,
        key: &str,
        version: &str,
        descriptor: Option<&str>,
        status: ReplicationStatus,
    ) {
        seed_version_at(
            meta,
            bucket,
            key,
            version,
            descriptor,
            status,
            Timestamp(1),
            None,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_version_at(
        meta: &InMemoryMetadataStore,
        bucket: &str,
        key: &str,
        version: &str,
        descriptor: Option<&str>,
        status: ReplicationStatus,
        created_at: Timestamp,
        // The replication-completion stamp (schema v23). `None` = never shipped, or a pre-v23 row.
        replicated_at: Option<Timestamp>,
    ) {
        let row = ObjectVersionRow {
            id: format!("{bucket}-{key}-{version}"),
            bucket: BucketName::parse(bucket).unwrap(),
            key: ObjectKey::parse(key).unwrap(),
            version_id: VersionId::from_string(version.to_owned()),
            is_latest: true,
            is_delete_marker: false,
            size_logical: 3,
            size_physical: 3,
            etag: ETag::from_string("e".to_owned()),
            content_type: "text/plain".to_owned(),
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            storage_path: Some(StoragePath::from_string(format!("{bucket}/{version}"))),
            compression: CompressionDescriptor::Uncompressed,
            storage_class: StorageClass::Standard,
            cold_locator: None,
            owner_id: UserId("root".to_owned()),
            user_metadata: Vec::new(),
            acl: None,
            checksums: Vec::new(),
            sse_descriptor: descriptor.map(ToOwned::to_owned),
            replication_status: Some(status),
            replicated_at,
            created_at,
            updated_at: created_at,
        };
        meta.submit(Mutation::PutObjectVersion {
            row: Box::new(row),
            precondition: Precondition::default(),
            replication: Vec::new(),
        })
        .await
        .unwrap();
    }

    async fn run(meta: &InMemoryMetadataStore) -> AuditReport {
        audit_store(meta, None, CUTOFF, 50, false, None, None)
            .await
            .unwrap()
    }

    /// The core split: an encrypted+`completed` version is PRESENT AND SUSPECT (on the mirror,
    /// possibly garbage), an encrypted+`failed` one is the absent class, and a plaintext version is
    /// not suspect at all. Without the DEK-aware filter these three are indistinguishable.
    #[tokio::test]
    async fn audit_separates_present_and_suspect_from_absent_and_ignores_plaintext() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(RULE_XML)).await;
        seed_version(
            &meta,
            "src",
            "enc-done",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;
        seed_version(
            &meta,
            "src",
            "enc-fail",
            "v2",
            Some(SSE_S3),
            ReplicationStatus::Failed,
        )
        .await;
        seed_version(
            &meta,
            "src",
            "plain",
            "v3",
            None,
            ReplicationStatus::Completed,
        )
        .await;

        let report = run(&meta).await;
        assert_eq!(report.present_and_suspect, 1, "the completed encrypted one");
        assert_eq!(report.absent, 1, "the failed encrypted one");
        assert_eq!(report.buckets.len(), 1);
        let b = &report.buckets[0];
        assert_eq!(b.samples.len(), 2, "the plaintext version is not suspect");
        assert!(b.samples.iter().all(|s| s.key != "plain"));
        assert_eq!(b.versions_scanned, 3);
    }

    /// A version that is still live work (`pending`) is not *suspect* — the fixed engine will ship
    /// it correctly — but it is not clean either: it is repair IN FLIGHT and is counted as such. A
    /// `replica` arrived inbound and is never shipped from here, so it is neither.
    #[tokio::test]
    async fn audit_ignores_pending_and_replica_versions() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(RULE_XML)).await;
        seed_version(
            &meta,
            "src",
            "a",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Pending,
        )
        .await;
        seed_version(
            &meta,
            "src",
            "b",
            "v2",
            Some(SSE_S3),
            ReplicationStatus::Replica,
        )
        .await;

        let report = run(&meta).await;
        assert_eq!(report.present_and_suspect, 0);
        assert_eq!(report.absent, 0);
        assert_eq!(
            report.repair_pending, 1,
            "the in-window pending version is repair in flight, not clean; the replica is neither"
        );
    }

    /// A bucket with no replication rule (or only disabled ones) is not a mirror source, so none of
    /// its encrypted versions are suspect — and it must not even appear in the report.
    #[tokio::test]
    async fn audit_ignores_buckets_without_an_enabled_rule() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "norule", None).await;
        seed_version(
            &meta,
            "norule",
            "k",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;

        let disabled = RULE_XML.replace("<Status>Enabled</Status>", "<Status>Disabled</Status>");
        seed_bucket(&meta, "off", Some(&disabled)).await;
        seed_version(
            &meta,
            "off",
            "k",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;

        let report = run(&meta).await;
        assert!(report.buckets.is_empty(), "no bucket is in scope");
        assert_eq!(report.present_and_suspect, 0);
    }

    /// TRAP 1: a rule without `ExistingObjectReplication` is flagged, because a resync against it
    /// returns success and repairs nothing.
    #[tokio::test]
    async fn audit_flags_a_rule_without_existing_object_replication() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(RULE_XML)).await;
        seed_version(
            &meta,
            "src",
            "k",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;
        let report = run(&meta).await;
        assert!(!report.buckets[0].existing_object_replication);

        let with_eor = RULE_XML.replace(
            "</Destination>",
            "</Destination><ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication>",
        );
        set_config(&meta, "src", ConfigAspect::Replication, &with_eor).await;
        let report = run(&meta).await;
        assert!(report.buckets[0].existing_object_replication);
    }

    /// TRAP 2: a non-current suspect is counted separately — a resync backfill enumerates CURRENT
    /// versions only and will never repair it.
    #[tokio::test]
    async fn audit_counts_non_current_suspects_separately() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(RULE_XML)).await;
        // Two versions of one key: the second put demotes the first from `is_latest`.
        seed_version(
            &meta,
            "src",
            "k",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;
        seed_version(
            &meta,
            "src",
            "k",
            "v2",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;

        let report = run(&meta).await;
        assert_eq!(report.buckets[0].present_and_suspect, 2);
        assert_eq!(
            report.buckets[0].non_current_suspect, 1,
            "the superseded version is not repairable by a resync backfill"
        );
    }

    /// TRAP 3: a client-encrypted suspect + an `http://` destination + the opt-in OFF means a repair
    /// would be refused and rescheduled forever. The audit must say so up front.
    #[tokio::test]
    async fn audit_flags_the_plaintext_http_gate() {
        let arn = "arn:cairn:replication:us-east-1:abc:mirror";
        let xml = RULE_XML.replace("arn:aws:s3:::mirror", arn);
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(&xml)).await;
        let targets = format!(
            r#"[{{"arn":"{arn}","endpoint":"http://mirror.internal:9000","region":"us-east-1","dest_bucket":"mirror","access_key_id":"AK","secret_ciphertext":[1],"nonce":[2]}}]"#
        );
        set_config(&meta, "src", ConfigAspect::ReplicationTargets, &targets).await;
        seed_version(
            &meta,
            "src",
            "k",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;

        let gated = audit_store(&meta, None, CUTOFF, 50, false, None, None)
            .await
            .unwrap();
        assert_eq!(
            gated.buckets[0].plaintext_http_endpoints,
            vec!["http://mirror.internal:9000".to_owned()]
        );
        assert!(
            gated.buckets[0].repair_blocked_by_http_gate,
            "client-encrypted suspects + an http:// endpoint + the opt-in off blocks repair"
        );

        // With CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP=true the endpoint is still reported,
        // but repair is no longer blocked.
        let opted_in = audit_store(&meta, None, CUTOFF, 50, true, None, None)
            .await
            .unwrap();
        assert!(!opted_in.buckets[0].repair_blocked_by_http_gate);
    }

    /// An `at-rest`-only bucket IS suspect (the mirror still holds ciphertext) but is NOT gated on a
    /// plaintext endpoint: transparent at-rest encryption is an operator storage property, never a
    /// client contract — exactly the distinction the Stage-1 sink gate draws.
    #[tokio::test]
    async fn audit_does_not_gate_at_rest_only_suspects() {
        let arn = "arn:cairn:replication:us-east-1:abc:mirror";
        let xml = RULE_XML.replace("arn:aws:s3:::mirror", arn);
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(&xml)).await;
        set_config(
            &meta,
            "src",
            ConfigAspect::ReplicationTargets,
            &format!(
                r#"[{{"arn":"{arn}","endpoint":"http://mirror.internal:9000","region":"us-east-1","dest_bucket":"mirror","access_key_id":"AK","secret_ciphertext":[1],"nonce":[2]}}]"#
            ),
        )
        .await;
        seed_version(
            &meta,
            "src",
            "k",
            "v1",
            Some(AT_REST),
            ReplicationStatus::Completed,
        )
        .await;

        let report = run(&meta).await;
        assert_eq!(report.present_and_suspect, 1, "still suspect on the mirror");
        assert_eq!(report.buckets[0].client_encrypted_suspect, 0);
        assert!(
            !report.buckets[0].repair_blocked_by_http_gate,
            "at-rest is not a client contract and is not gated"
        );
        assert_eq!(report.buckets[0].samples[0].mode, "at-rest");
    }

    /// `--bucket` scopes the audit; a suspect in another bucket must not leak into the report.
    #[tokio::test]
    async fn audit_honours_the_bucket_filter() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "one", Some(RULE_XML)).await;
        seed_bucket(&meta, "two", Some(RULE_XML)).await;
        seed_version(
            &meta,
            "one",
            "k",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;
        seed_version(
            &meta,
            "two",
            "k",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;

        let report = audit_store(&meta, Some("one"), CUTOFF, 50, false, None, None)
            .await
            .unwrap();
        assert_eq!(report.buckets.len(), 1);
        assert_eq!(report.buckets[0].bucket, "one");
        assert_eq!(report.present_and_suspect, 1);
    }

    /// A rule whose prefix does not select the key means the version was never replicated by it, so
    /// it is not suspect.
    #[tokio::test]
    async fn audit_respects_the_rule_prefix() {
        let meta = InMemoryMetadataStore::new();
        let scoped = RULE_XML.replace("<Prefix></Prefix>", "<Prefix>mirrored/</Prefix>");
        seed_bucket(&meta, "src", Some(&scoped)).await;
        seed_version(
            &meta,
            "src",
            "mirrored/a",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;
        seed_version(
            &meta,
            "src",
            "private/b",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;

        let report = run(&meta).await;
        assert_eq!(report.present_and_suspect, 1);
        assert_eq!(report.buckets[0].samples[0].key, "mirrored/a");
    }

    /// THE CONVERGENCE PROPERTY. A version encrypted and replicated AFTER the fix is
    /// indistinguishable from a damaged one by descriptor and status alone — it is encrypted and it
    /// is `completed`. Only the creation time separates them. Without the cutoff the gauge counts
    /// every healthy encrypted replica forever, so it can never reach zero, an alert on it is
    /// meaningless, and "watch it fall to 0" is not achievable advice.
    #[tokio::test]
    async fn audit_excludes_versions_created_at_or_after_the_cutoff() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(RULE_XML)).await;
        // Written by the PRE-fix binary: damaged.
        seed_version_at(
            &meta,
            "src",
            "old",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
            Timestamp(CUTOFF.0 - 1),
            None,
        )
        .await;
        // Written exactly AT the cutoff, and after it: shipped through the DEK-aware path, healthy.
        seed_version_at(
            &meta,
            "src",
            "at",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
            CUTOFF,
            None,
        )
        .await;
        seed_version_at(
            &meta,
            "src",
            "new",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
            Timestamp(CUTOFF.0 + 5_000),
            None,
        )
        .await;

        let report = run(&meta).await;
        assert_eq!(
            report.present_and_suspect, 1,
            "only the pre-cutoff version could have been shipped as ciphertext"
        );
        assert_eq!(report.buckets[0].samples.len(), 1);
        assert_eq!(report.buckets[0].samples[0].key, "old");
        assert_eq!(
            report.created_before, CUTOFF.0,
            "the report echoes its cutoff — the counts are meaningless without it"
        );

        // And the whole point: once every pre-cutoff version is repaired away, the signal is ZERO.
        // An unbounded predicate would still be reporting the two healthy post-fix versions here.
        let clean = InMemoryMetadataStore::new();
        seed_bucket(&clean, "src", Some(RULE_XML)).await;
        seed_version_at(
            &clean,
            "src",
            "new",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
            Timestamp(CUTOFF.0 + 1),
            None,
        )
        .await;
        let report = run(&clean).await;
        assert_eq!(
            (
                report.present_and_suspect,
                report.absent,
                report.repair_pending
            ),
            (0, 0, 0),
            "a node with only post-cutoff encrypted replicas must read completely clean"
        );
    }

    /// THE CONVERGENCE TEST (review 2, blocking 1). A `created_at`-only cutoff cannot ever reach
    /// zero on a node that HAD damage and repaired it: the damaged version keeps `created_at < cutoff`
    /// forever, so the instant the repair succeeds and re-stamps it `completed` it matches the
    /// predicate again — the gauge returns to its pre-repair value precisely BECAUSE the repair
    /// worked, and the runbook's "watch it fall to 0" describes a state that cannot be reached.
    ///
    /// `replicated_at` (schema v23) is what closes it: a repaired version carries a completion stamp
    /// AFTER the cutoff and drops out of the population.
    #[tokio::test]
    async fn audit_drops_a_repaired_version_so_the_gauge_can_reach_zero() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(RULE_XML)).await;
        // Damaged: created before the cutoff, and last shipped before it too (or never — a pre-v23
        // row). This is what the gauge must count.
        seed_version_at(
            &meta,
            "src",
            "damaged",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
            Timestamp(CUTOFF.0 - 10),
            Some(Timestamp(CUTOFF.0 - 5)),
        )
        .await;
        assert_eq!(
            run(&meta).await.present_and_suspect,
            1,
            "a version last shipped before the fix is suspect"
        );

        // Now REPAIR it: same row, same `created_at` (nothing rewrites it), re-shipped correctly
        // after the cutoff so `MarkReplicationDone` stamped a fresh `replicated_at`.
        let repaired = InMemoryMetadataStore::new();
        seed_bucket(&repaired, "src", Some(RULE_XML)).await;
        seed_version_at(
            &repaired,
            "src",
            "damaged",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
            Timestamp(CUTOFF.0 - 10),
            Some(Timestamp(CUTOFF.0 + 1)),
        )
        .await;
        let report = run(&repaired).await;
        assert_eq!(
            (
                report.present_and_suspect,
                report.absent,
                report.repair_pending
            ),
            (0, 0, 0),
            "a REPAIRED version must leave the suspect population — otherwise the gauge never \
             converges and the operator reads a successful repair as a failed one"
        );
    }

    /// A version that has NEVER been shipped (`replicated_at IS NULL`) stays suspect. That is
    /// correct for a genuinely unshipped version — it is absent on the mirror — and it is also what
    /// every pre-v23 row looks like after the migration, so an upgraded node over-reports healthy
    /// old versions on its first audit. Deliberate: an over-report costs one wasted re-ship, an
    /// under-report leaves a garbage replica nobody looks for (`docs/operations.md` 8.7).
    #[tokio::test]
    async fn audit_treats_a_null_replicated_at_as_suspect() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(RULE_XML)).await;
        seed_version_at(
            &meta,
            "src",
            "pre-v23",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
            Timestamp(CUTOFF.0 - 10),
            None,
        )
        .await;
        assert_eq!(
            run(&meta).await.present_and_suspect,
            1,
            "an unstamped pre-migration row is counted, not silently trusted"
        );
    }

    /// The convergence signal must not read BACKWARDS. A forced requeue flips damaged rows to
    /// `pending`, so suspects fall to zero the moment the repair is *queued* — before a byte moves.
    /// `repair_pending` is what stops that looking like success; the runbook's done condition is
    /// both counters at zero.
    #[tokio::test]
    async fn audit_counts_in_flight_repair_separately_from_clean() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(RULE_XML)).await;
        seed_version(
            &meta,
            "src",
            "queued",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Pending,
        )
        .await;
        seed_version(
            &meta,
            "src",
            "claimed",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Claimed,
        )
        .await;
        // A post-cutoff pending version is ordinary live work, NOT repair, and must not inflate the
        // repair gauge — otherwise the "done" condition never holds on a busy node.
        seed_version_at(
            &meta,
            "src",
            "live",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Pending,
            Timestamp(CUTOFF.0 + 100),
            None,
        )
        .await;

        let report = run(&meta).await;
        assert_eq!(report.present_and_suspect, 0, "nothing is stamped terminal");
        assert_eq!(
            report.repair_pending, 2,
            "both in-window pending/claimed versions are repair in flight"
        );
    }

    /// THE ALARM MUST BE ABLE TO STOP. `docs/operations.md` 8.7 tells the operator to alert on
    /// `repair_pending > 0` and to treat `present_and_suspect == 0 AND repair_pending == 0` as done,
    /// so that state has to be reachable.
    ///
    /// It was not. A forced requeue used to flip EVERY terminal version row of a paged key to
    /// `pending`, including non-current versions whose outbox row the retention sweep had already
    /// pruned — and nothing enqueues work for those, because the resync backfill enumerates current
    /// objects only. They sat `pending` forever, this gauge counted them forever, and the prescribed
    /// alert fired forever on a node whose repair had actually finished.
    ///
    /// This walks the whole arc on the double: damage, forced requeue, drain, convergence.
    #[tokio::test]
    async fn repair_pending_reaches_zero_after_a_forced_requeue_drains() {
        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(RULE_XML)).await;
        // One key, two encrypted versions, both `completed` and both damaged. Neither has an outbox
        // row: 24 h after the incident the retention sweep has taken them all, which is the case
        // the runbook is actually written for.
        for v in ["v1", "v2"] {
            seed_version(
                &meta,
                "src",
                "k",
                v,
                Some(SSE_S3),
                ReplicationStatus::Completed,
            )
            .await;
        }
        let before = run(&meta).await;
        assert_eq!(before.present_and_suspect, 2);
        assert_eq!(before.repair_pending, 0);
        assert_eq!(before.buckets[0].non_current_suspect, 1);

        meta.submit(Mutation::RequeueReplicationVersions {
            bucket: BucketName::parse("src").unwrap(),
            only_encrypted: true,
            after_key: None,
            now: Timestamp(2),
            limit: 1000,
        })
        .await
        .unwrap();

        let queued = run(&meta).await;
        assert_eq!(
            queued.repair_pending, 1,
            "only the CURRENT version is repair in flight — it is the only one the backfill will \
             re-enqueue"
        );
        assert_eq!(
            queued.buckets[0].non_current_suspect, 1,
            "the non-current version stays a suspect: it is still on the mirror and still wrong. \
             TRAP 2 says so; claiming it was queued would be a lie the gauge could never retract"
        );

        // The repair ships. `MarkReplicationDone` stamps `replicated_at` past the cutoff, so the
        // repaired version leaves the population entirely instead of returning to the suspect count.
        seed_version_at(
            &meta,
            "src",
            "k",
            "v2",
            Some(SSE_S3),
            ReplicationStatus::Completed,
            Timestamp(1),
            Some(Timestamp(CUTOFF.0 + 1)),
        )
        .await;

        let done = run(&meta).await;
        assert_eq!(
            done.repair_pending, 0,
            "THE DONE-STATE IS REACHABLE: nothing is left claiming to be queued"
        );
        assert_eq!(
            done.present_and_suspect, 1,
            "the residual floor is exactly the non-current versions, which the runbook says are \
             unrepairable without rebuilding the destination bucket"
        );
        assert_eq!(done.buckets[0].non_current_suspect, 1);
        assert_eq!(
            done.non_current_suspect, 1,
            "the floor is reported store-wide, so the runbook's done-state — repair_pending == 0 \
             AND present_and_suspect == non_current_suspect — is checkable without summing buckets"
        );
        assert_eq!(done.present_and_suspect, done.non_current_suspect);
    }

    /// `--verify` must SKIP non-current versions. The verifier signs a GET with no `versionId`, so
    /// it can only ever fetch the destination's CURRENT object; running it against a superseded
    /// source version compares two different objects and reports `Mismatched` — "confirmed
    /// corruption" — for a mirror that may be perfectly healthy.
    #[tokio::test]
    async fn verify_skips_non_current_versions_instead_of_reporting_false_mismatches() {
        /// A verifier that fails the test if it is ever handed a non-current version, and otherwise
        /// reports a match.
        struct CurrentOnly;
        #[async_trait::async_trait]
        impl ReplicaVerifier for CurrentOnly {
            async fn verify(&self, row: &ObjectVersionRow) -> VerifyOutcome {
                assert!(
                    row.is_latest,
                    "the verifier was handed the non-current version {:?}; its GET carries no \
                     versionId, so this comparison is against a different object entirely",
                    row.version_id.as_str()
                );
                VerifyOutcome::Matched
            }
        }

        let meta = InMemoryMetadataStore::new();
        seed_bucket(&meta, "src", Some(RULE_XML)).await;
        // Two versions of one key: the second put demotes the first from `is_latest`.
        seed_version(
            &meta,
            "src",
            "k",
            "v1",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;
        seed_version(
            &meta,
            "src",
            "k",
            "v2",
            Some(SSE_S3),
            ReplicationStatus::Completed,
        )
        .await;

        let report = audit_store(&meta, None, CUTOFF, 50, false, None, Some(&CurrentOnly))
            .await
            .unwrap();
        let b = &report.buckets[0];
        assert_eq!(b.present_and_suspect, 2);
        assert_eq!(b.verified_matched, 1, "only the current version is fetched");
        assert_eq!(
            b.verified_mismatched, 0,
            "a non-current version must never be reported as confirmed corruption"
        );
        assert_eq!(
            b.verify_skipped_non_current, 1,
            "the skipped version is counted, not silently dropped"
        );
    }

    /// The cutoff an operator types under incident pressure: both accepted forms parse, and an
    /// unparseable one fails loudly rather than defaulting to something that changes every count.
    #[test]
    fn cutoff_parses_rfc3339_and_epoch_seconds() {
        assert_eq!(
            parse_cutoff("1753264800").unwrap(),
            Timestamp(1_753_264_800)
        );
        assert_eq!(
            parse_cutoff("2026-07-23T10:00:00Z").unwrap(),
            Timestamp(1_784_800_800)
        );
        // Whitespace, a fractional part, and an explicit offset all round-trip to the same instant.
        assert_eq!(
            parse_cutoff("  2026-07-23T10:00:00.500Z  ").unwrap(),
            Timestamp(1_784_800_800)
        );
        assert_eq!(
            parse_cutoff("2026-07-23T12:00:00+02:00").unwrap(),
            Timestamp(1_784_800_800)
        );
        for bad in [
            "yesterday",
            "2026-07-23",
            "2026-13-01T00:00:00Z",
            "2026-07-23T25:00:00Z",
            "",
        ] {
            let err = parse_cutoff(bad).unwrap_err();
            assert!(
                err.contains("RFC-3339") && err.contains("epoch"),
                "the error must name both accepted forms, got {err:?}"
            );
        }
    }
}
