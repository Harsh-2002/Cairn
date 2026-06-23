---
target: Activity
total_score: 32
p0_count: 0
p1_count: 2
timestamp: 2026-06-22T14-36-12Z
slug: ui-src-views-activity-tsx
---
# Critique — Activity (`ui/src/views/activity.tsx`)

## Design Health Score

| # | Heuristic | Score | Key Issue |
|---|-----------|-------|-----------|
| 1 | Visibility of System Status | 3 | Skeleton + refresh-busy + error alert are all present, but the view never shows a freshness signal, a result/event count, or that it's capped at the 500 most-recent rows |
| 2 | Match System / Real World | 2 | The Action column prints raw PascalCase enum codes (`SetBucketCompression`, `RevokeSessionCredential`) — developer identifiers, not the plain language the brand promises |
| 3 | User Control and Freedom | 3 | Filters set easily and bucket links navigate, but there's no one-click "Clear filters" when results exist, no date range, no pagination past the 500 cap, and filter state is lost on refresh (not in the URL) |
| 4 | Consistency and Standards | 4 | Pure reuse of the shared spine — `Page`/`PageHeader`, `DataTable`/`SkeletonRows`, `RefreshButton`, `EmptyState`, `TextLink`, mono identifiers, tabular timestamps. Indistinguishable in vocabulary from every other list view |
| 5 | Error Prevention | 4 | Read-only surface — no destructive action to guard; filters can't reach an invalid state |
| 6 | Recognition Rather Than Recall | 3 | The action dropdown enumerates choices (good), but the raw action codes force decoding, there's no relative time to orient ("2h ago"), and no day grouping for a log many rows of which share a timestamp |
| 7 | Flexibility and Efficiency | 2 | Two filters only. No export (CSV/JSON) for an audit trail, no column sort, no date range, no deep-linkable filtered view; a power user auditing beyond the last 500 events has no path |
| 8 | Aesthetic and Minimalist Design | 4 | Clean, spacious, on-brand; clear table hierarchy. Minor: stacked mobile cards repeat empty `—` fields, and the timestamp — the log's primary scan axis — is the most muted text in the row |
| 9 | Error Recovery | 4 | Load failure → `ErrorAlert` with a plain title, the server message, and a working "Try again" (`onRetry`); nothing to lose on a read-only fetch |
| 10 | Help and Documentation | 3 | Page description + a teaching empty state orient well, but nothing explains the 500-row cap, what each action code means, or what is/isn't audited |
| **Total** | | **32/40** | **Good** (upper band, near Excellent) |

**Trend: first run for this target, no trend yet.**

## Anti-Patterns Verdict — PASS

**LLM assessment:** No AI-slop tells. No gradient hero, no decorative color, no nested cards, no eyebrow kickers, no side-stripe borders. This reads as a competent operator's audit log, not a template. It passes the *product* slop test — a user fluent in Linear/Stripe would trust it on sight, because it borrows the same restrained table grammar the rest of the console uses.

**Deterministic scan:** `detect.mjs --json` over `activity.tsx` and the three components it composes (`data-table`, `empty-state`, `refresh-button`) → **0 findings, exit 0**. No false positives to flag.

**Visual evidence:** Desktop (1440px) and mobile (390px) screenshots inspected directly; no live detect.js overlay was injected, so there's no in-browser overlay to point at. The screenshots confirm the CLI verdict — clean execution, correct stacked-card responsiveness, real touch targets, no overflow.

## Overall Impression

This is a **structurally excellent page riding on borrowed strength** — it inherits the console's strong shared components and is therefore clean, consistent, responsive, and well-handled in its loading/empty/error states almost for free. Where it falls short of Excellent is on the things *specific to being an audit log*, which the shared components can't supply: the data it shows is still in the machine's voice, not the operator's; and it gives a power user auditing real history almost no leverage (no export, no range, no sort, capped silently at 500).

The single biggest opportunity: **translate the Action column into plain language.** A console whose stated north star is "make S3 concepts legible" prints `SetBucketNotifications` and `RevokeSessionCredential` straight from the wire. Humanizing that one column lifts three heuristics at once (Match, Recognition, Help) and is the difference between "a log of API calls" and "a record of what happened to your storage."

## What's Working

1. **Total vocabulary consistency.** Every primitive on this page is the same one used on Buckets, Users, Tags. The Action cell is sans, identifiers are mono, timestamps are tabular — the Mono-for-Truth rule is honored. There is zero "stitched from different products" feel; this is the heuristic the page nails outright (4).
2. **Honest, complete states.** Skeleton rows (not a spinner) on first load; a refresh button that disables on initial load and spins + `aria-busy` on re-fetch; an `ErrorAlert` with a real retry; and *two distinct* empty states — "No activity yet" (teaching) vs. "Nothing matches" (after filtering). That's the product register's "states, not nothing-here" done right.
3. **Bucket cross-links.** Each bucket cell is a `TextLink` straight to that bucket's browser — the log isn't a dead end; it's a jumping-off point to the object the event touched. Small, but exactly the "tool disappears into the task" move.

## Priority Issues

- **[P1] Raw machine action names.** The Action column renders enum codes verbatim (`CreateBucket`, `SetBucketCompression`, `RevokeSessionCredential`, `SetBucketNotifications`).
  - **Why it matters:** This is the console's headline promise broken on its own audit page. A first-time self-hoster reads `SetBucketNotifications` and has to mentally decode camelCase; a `DeleteBucket` looks identical in weight to a `CreateBucket`, so a destructive event doesn't read as one. It's the lowest score on the page (Match = 2) and drags Recognition and Help with it.
  - **Fix:** Map each action to a plain phrase ("Deleted bucket", "Minted session credential", "Enabled versioning", "Changed compression"). Keep the raw code available on hover (`title`) or as a muted mono secondary for experts who grep by it. Consider grouping the dropdown the same way. A *restrained* destructive cue (the existing destructive token on delete-class actions only) would let an operator spot the dangerous events while honoring the meaningful-color rule.
  - **Suggested command:** `/impeccable clarify`

- **[P1] No freshness, count, or cap visibility.** The view loads the 500 most-recent events and says nothing about it.
  - **Why it matters:** An audit log's first question is "is this everything, and how current is it?" Right now there's no event count, no "showing the 500 most recent," and no relative time — so an operator can't tell at a glance whether a row is from minutes or weeks ago, or whether older history exists beyond the window. Riley (stress tester) hits a silent truncation; the action-filter list also silently reflects only the loaded window.
  - **Fix:** Add a count line ("N events" / "Showing the 500 most recent — filtered to M"). Render time as `relTime()` with the absolute timestamp on hover (the codebase already has `relTime`). Optionally add lightweight day separators ("Today / Yesterday").
  - **Suggested command:** `/impeccable clarify` (copy + status), then `/impeccable layout` for day grouping

- **[P2] Thin power-user leverage.** Two filters, both client-side over the loaded page; no export, no sort, no date range, no URL-persisted filter state.
  - **Why it matters:** Flexibility is the page's other 2. For a compliance/audit surface, "export the last week as CSV" and "filter to a date range" are table-stakes power-user expectations (Alex). Filters living in component state means a refresh or a shared link loses them.
  - **Fix:** Persist `action`/`bucket` filters to the URL query so views are linkable and survive refresh; add an Export (CSV/JSON) action in the header; consider time-column sort and a date-range control feeding the API `limit`/range instead of the fixed 500.
  - **Suggested command:** `/impeccable harden` (edge cases + URL state), or a scoped `/impeccable craft` for export

- **[P2] No "Clear filters" when results exist.** The "Nothing matches → Clear the filters" copy only appears in the *empty* result state; when a filter is active but matching, there's no one-click reset.
  - **Why it matters:** Minor User-Control friction — the operator manually empties the input and resets the select.
  - **Fix:** Show a "Showing M of N · Clear" affordance beside the filter row whenever a filter is active.
  - **Suggested command:** `/impeccable clarify`

## Persona Red Flags

**Alex (Power User):** No export. No column sort. No date range — capped at the 500 most-recent with no pagination, so auditing last month is impossible. Filters aren't in the URL, so a refresh or a shared link drops them. (Mitigation: the global ⌘K palette exists, but it doesn't help inside the log.)

**Jordan (First-Timer):** `RevokeSessionCredential`, `SetBucketNotifications` are undecoded jargon with no tooltip or inline explanation. Nothing tells them the list is capped at 500 or what counts as "activity." The page description and empty state help, but the rows themselves don't speak their language.

**Riley (Stress Tester):** With >500 events the window truncates silently — no "more exist" signal. The action-filter dropdown only lists actions present in the loaded page, so filtering for an older action that's scrolled off is impossible and gives no hint why. Many rows share an identical timestamp (e.g. the seed `9:54:17 AM` batch); the secondary ordering isn't surfaced, so order looks arbitrary at the second granularity.

**Sam (Accessibility) — near-pass / strength:** Semantic `<table>`, both filters carry `aria-label`, bucket links are real anchors, the global focus ring and skip-link apply, and the route `status` region announces "Activity". Empty/error states aren't color-only. This persona is well served; the only nit is that relative-time and any future destructive cue must not be conveyed by color alone.

## Minor Observations

- Stacked mobile cards print every field including empty ones, so credential rows show `Bucket —` / `Key —` as dead weight; suppressing empty cells in stacked mode would tighten them (shared `DataTable` concern, not Activity-only).
- The timestamp is the log's primary scan axis yet is the most muted text in the row, while the (machine) Action name is bold — a mild hierarchy inversion that resolves itself once Action is humanized and time goes relative.
- `whenMs` uses `toLocaleString()`, so the format follows the viewer's locale — fine, but it means two operators may see different date orders; the relative-time primary with absolute-on-hover sidesteps the ambiguity.

## Questions to Consider

- What if the Action column read like a sentence an operator would say out loud — "Deleted bucket `photos`" — instead of a wire enum?
- Should this page own a date range and export, or is it deliberately a "recent glance" and the real audit trail lives elsewhere (logs/API)?
- If an operator is here, they're usually investigating *one* thing — does the page help them get from "something changed" to "here's exactly what and when," or just list calls?
