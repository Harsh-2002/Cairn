---
target: Replication
total_score: 34
p0_count: 0
p1_count: 0
timestamp: 2026-06-22T15-46-36Z
slug: ui-src-views-replication-tsx
---
# Critique — Replication (`ui/src/views/replication.tsx` + the bucket-settings replication cards)

## Design Health Score — **34/40 · Good**  ·  trend **25 → 34 (+9)**

| # | Heuristic | Score | Δ | Key Issue |
|---|-----------|-------|---|-----------|
| 1 | Visibility of System Status | 4 | +2 | The nav page is now an **overview**: every bucket's rule, its target, a health badge, and live pending/failed — healthy state included, not just failures |
| 2 | Match System / Real World | 4 | +1 | "Replication" now answers "what replicates where, and is it healthy" instead of being a bare dead-letter queue |
| 3 | User Control and Freedom | 3 | +1 | Rules and failed rows link to the bucket's settings (where Retry/Resync live), so recovery is one click from the problem — but failed objects still can't be requeued inline |
| 4 | Consistency and Standards | 4 | +1 | The overview ties the formerly-disconnected surfaces together via links; shared components throughout; the failed-tracker Key cell now carries its `data-label` |
| 5 | Error Prevention | 3 | — | The rule card forces selecting a configured target (no dangling rules), but adding a target still has no "test connection" |
| 6 | Recognition Rather Than Recall | 4 | +1 | State is glanceable: rule → destination → health badge, all on one page; buckets are links, not names to recall |
| 7 | Flexibility and Efficiency | 3 | +1 | A node-wide view now exists (real efficiency for auditing), but no bulk retry, no inline requeue, no filter on the failed list |
| 8 | Aesthetic and Minimalist Design | 3 | — | Clean two-section overview; the "Replication targets" add-form in bucket settings is still always-expanded (5 fields visible when only viewing) |
| 9 | Error Recovery | 3 | +1 | Failed rows now link to where Retry lives; still no inline requeue, and entries stuck under a since-fixed rule can't be cleared (no UI hint) |
| 10 | Help and Documentation | 3 | +1 | The overview's empty state explains where to configure replication; still no test-connection or docs link |
| **Total** | | **34/40** | **+9** | **Good** |

## Anti-Patterns Verdict — PASS

`detect.mjs` over `replication.tsx` + `bucket-settings.tsx` → **0 findings, exit 0**.

## What changed since 25/40

- **P0 functional bug fixed (`ab38ae4`)** — a console-set rule wrote a plain bucket ARN, never linked to a target, and silently failed. The rule card now selects a registered target and writes its ARN; validated bidirectionally on two live nodes (a rule created entirely in the console replicates; loop prevention holds).
- **Replication overview (`7ea7ec0`)** — the nav page is now "Replication rules" (bucket → target → health badge → pending/failed) + "Failed objects" as a section beneath, with buckets linked to their settings. Verified live at 1440/390 against the running two-node pair.

## Remaining levers to Excellent

- **Inline requeue on failed rows** + a way to clear entries stuck under a since-fixed rule (lifts User Control + Error Recovery to 4).
- **Test-connection when adding a target** (Error Prevention → 4).
- **Bulk retry / a filter on the failed list** (Flexibility → 4).
- **Collapse the targets add-form** behind "Add target" until needed (Aesthetic → 4).
- **Replication lag / throughput** on the overview (needs the metrics endpoint, not just per-bucket status).

## Verdict

The section went from broken-and-bare to a genuine, working overview in the Good band. The path to Excellent is making the failed objects actionable in place and verifying targets before they're saved.
