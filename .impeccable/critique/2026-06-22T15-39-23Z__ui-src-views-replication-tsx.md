---
target: Replication
total_score: 25
p0_count: 0
p1_count: 2
timestamp: 2026-06-22T15-39-23Z
slug: ui-src-views-replication-tsx
---
# Critique — Replication (`ui/src/views/replication.tsx` + the bucket-settings replication cards)

## Design Health Score

| # | Heuristic | Score | Key Issue |
|---|-----------|-------|-----------|
| 1 | Visibility of System Status | 2 | The "Replication" nav page shows **only the failed dead-letter queue** — no view of what's configured, what's healthy, lag, throughput, or which buckets replicate where. "All caught up" reads identically whether replication is healthy or not configured at all |
| 2 | Match System / Real World | 3 | "Target", "destination", "endpoint" are fine, but the nav item called "Replication" is a failures-only table (mismatch with the mental model of a replication overview), and it surfaces raw engine errors ("no replication target for source bucket") |
| 3 | User Control and Freedom | 2 | The bucket-settings card has Save/Remove/Retry/Resync, but the failed-tracker page has **no actions** — you see a failed object but can't requeue it or jump to its bucket; you must go hunt for the bucket's settings |
| 4 | Consistency and Standards | 3 | Shared DataTable/cards/StatusBadge are consistent; but replication config is split across two cards in *bucket settings* while the nav "Replication" page is elsewhere — one feature, three disconnected places. (Fixed this pass: the Key cell was missing its `data-label`.) |
| 5 | Error Prevention | 3 | **Was effectively 0** — the console let you create a rule that silently never replicated. Now fixed: the rule card forces selecting a *configured target* and disables Save (with guidance) until one exists, so a dangling rule can't be created. Still no "test connection" when adding a target |
| 6 | Recognition Rather Than Recall | 3 | Now you pick a target from a dropdown instead of recalling a bucket name that had to secretly match a target's dest_bucket. But the rule⇄target⇄nav-page relationship still spans three surfaces the operator must assemble mentally |
| 7 | Flexibility and Efficiency | 2 | Retry/Resync exist per bucket, but there's no node-wide replication view, no bulk retry, no filter on the failed tracker, and the failed tracker isn't actionable |
| 8 | Aesthetic and Minimalist Design | 3 | Clean, on-brand cards/table. The "Replication targets" add-form is always expanded (5 fields visible even when you only want to view targets), which weights the card down |
| 9 | Error Recovery | 2 | The failed tracker shows the error (full text on hover) — good diagnosis — but offers no recovery action on the page, the message is engine-phrased, and entries stamped under a broken rule can't be fixed by Retry (they need re-enqueue) with no hint of that |
| 10 | Help and Documentation | 2 | Card descriptions are decent (versioning requirement, sealed-secret note), but nothing explains the per-bucket target model, there's no test-connection, and the nav page never tells you replication is configured elsewhere |
| **Total** | | **25/40** | **Acceptable** — significant improvements needed |

## Anti-Patterns Verdict — PASS (visual), but a P0 *functional* break was found

`detect.mjs` over `replication.tsx` + `bucket-settings.tsx` → **0 findings, exit 0**. No slop tells; the visual craft matches the rest of the console.

The real problem wasn't visual — it was that **replication did not work at all through the console** (a P0). Standing up two real nodes and wiring bidirectional replication surfaced it immediately: every object failed with *"no replication target for source bucket."* Root cause: the engine routes each outbox entry to a registered remote target by the **target ARN** the rule names (ARCH 20.2 — "a rule references that target by its ARN"), but `s3.putReplication` wrote a plain S3 bucket ARN (`arn:aws:s3:::name`), so the rule linked to no target. The engine was correct; the UI wrote the wrong destination. **Fixed this pass** (commit `ab38ae4`) and validated end-to-end: a rule created entirely through the console now replicates to the peer node, both directions, with loop prevention holding.

## Overall Impression

This is the console's **weakest section, and the user's instinct was right.** Two things compound:
1. **It was broken.** The headline replication feature silently failed for anyone configuring it through the UI — objects piled into a failed queue with a developer-phrased error. That's now fixed and proven against a live two-node pair.
2. **It's fragmented.** Even working, "Replication" as a *section* is three disconnected places: a nav page that is only a failed-object dead-letter queue, and two cards buried in each bucket's Integrations tab (the rule, and the targets). An operator who clicks "Replication" expecting "is my replication healthy and what's flowing where" gets a table that's empty in the happy path.

The single biggest opportunity: **make the "Replication" nav page a real overview** — per-bucket rules with their target + live pending/failed/lag, healthy state included (not just failures), with the failed entries actionable (requeue inline, link to the bucket). The dead-letter queue is one tab of that, not the whole page.

## What's Working

1. **The per-bucket config cards are genuinely good now** — the rule card reads as "replicate to {target}", the targets card seals the secret server-side and says so, and Retry/Resync are present where the status is shown.
2. **Honest failure surface** — the failed tracker uses the shared table, a positive "All caught up" empty state, and a tooltip for the full error. The bones are right.
3. **Loop prevention is real** — validated: replicas are marked and not re-replicated, so the bidirectional pair converges instead of ping-ponging.

## Priority Issues

- **[P0 — FIXED this pass] UI-created rules never replicated.** `putReplication` wrote a bucket ARN, not the target ARN, so the rule linked to no target. **Fix shipped:** rule card now selects a registered target and writes its ARN; validated bidirectionally on two live nodes. → already done

- **[P1] The "Replication" nav page is only a dead-letter queue.** It shows failures and nothing else — no configured rules, no health, no throughput; "All caught up" can't distinguish healthy from unconfigured.
  - **Fix:** make it an overview: list each bucket with a replication rule, its target (bucket @ endpoint), and live pending/failed/lag from `replicationStatus`; keep failures as a section/filter within it, not the whole page.
  - **Suggested command:** `/impeccable craft` (a replication overview view)

- **[P1] Failed entries aren't actionable where you see them.** The tracker shows a failed object but offers no requeue and no link to its bucket; recovery means leaving the page to find bucket settings.
  - **Fix:** add a per-row "Requeue" (the `retryReplication` API exists) and link the bucket cell to its Integrations tab.
  - **Suggested command:** `/impeccable harden`

- **[P2] No "test connection" when adding a target.** You enter an endpoint + credentials and only learn they're wrong later, via failed replication.
  - **Fix:** a "Test" button that does a signed HEAD/list against the target before saving; surface the result inline.
  - **Suggested command:** `/impeccable craft`

- **[P2] The section is discoverability-fragmented.** Rule, targets, and the failed tracker live in three places with no cross-links.
  - **Fix:** cross-link them; from the nav overview, deep-link to a bucket's replication config.
  - **Suggested command:** `/impeccable layout`

## Persona Red Flags

**Alex (Power User):** No node-wide replication view; to audit replication health he opens each bucket's Integrations tab one at a time. No bulk retry. The failed tracker can't be filtered or acted on.

**Jordan (First-Timer):** Clicks "Replication", sees an empty "All caught up" table, and has no idea replication is configured per-bucket somewhere else. The target model (endpoint + region + dest bucket + access/secret, sealed) is a lot of fields with no test or example beyond a placeholder. Before the fix, a rule they set up would have silently never worked.

**Riley (Stress Tester):** Found the real bug — a rule that saves "successfully" but never replicates. Also: requeuing an entry stamped under a since-fixed rule does nothing (ARN is fixed at enqueue), with no UI hint; and "All caught up" hides a bucket that has a target but no rule (or a rule but no target).

## Minor Observations

- The failed-tracker "Next attempt" is an absolute locale timestamp (same as Activity pre-fix) — relative-with-hover would read better.
- The console logs benign 404s for unset `?publicAccessBlock` / `?tagging` on every bucket-settings load; harmless but noisy.
- The targets add-form is always expanded; collapsing it behind "Add target" until needed would calm the card.

## Questions to Consider

- What should a person see the instant they click "Replication" — a list of failures, or "here's what's replicating where, and is it healthy"?
- Should the failed objects be a tab of a replication overview rather than the entire page?
- Should adding a target verify it can actually be reached and written to before it's saved?
