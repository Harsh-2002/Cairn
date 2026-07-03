---
target: Import section
total_score: 27
p0_count: 0
p1_count: 2
timestamp: 2026-07-03T16-49-57Z
slug: ui-src-views-imports-tsx
---
# Design critique — Import view (ui/src/views/imports.tsx)

## Design Health Score: 27/40 (Acceptable)

| # | Heuristic | Score | Key issue |
|---|-----------|-------|-----------|
| 1 | Visibility of system status | 3 | Live SSE table + badges + toasts, but progress bar shows 0% mid-run; no ETA/throughput. |
| 2 | Match system / real world | 3 | Plain language; `source:destination` jargon but explained inline. |
| 3 | User control & freedom | 3 | Per-row Cancel/Resume; secret cleared on submit. No confirm before bulk copy. |
| 4 | Consistency & standards | 3 | Mirrors replication.tsx; raw `title` vs Tooltip; running import painted amber. |
| 5 | Error prevention | 2 | CA-vs-skip-verify validated; empty Buckets = import all with no confirm; no collision guard. |
| 6 | Recognition vs recall | 2 | Blind Buckets textarea forces recall of exact source names + source:destination format. |
| 7 | Flexibility & efficiency | 3 | Workers override, bulk paste, fields retained. No profiles / test-connection. |
| 8 | Aesthetic & minimalist | 4 | Excellent — spacious Geist, hairline borders, restrained color. |
| 9 | Error recovery | 1 | Failed job shows only a red badge + Resume, no reason; list payload lacks last_error. |
| 10 | Help & documentation | 3 | Strong inline help; no deep-doc link. |

## Anti-Patterns Verdict: does NOT look AI-generated
detect.mjs on imports.tsx: 0 findings (clean). Agrees with LLM verdict (no slop tells). Detector is
blind to the runtime/logic issues (0% progress, missing error payload, toast-only validation) which
are the real work. Desktop + mobile render on-system and responsive (form -> single column, table ->
stacked cards).

## What's Working
1. Secret handling trustworthy end-to-end (type=password + autoComplete=off + wiped on submit + "sealed... never shown again" copy matches behavior).
2. Transport-security section respects a non-expert (CA vs skip-verify mutually exclusive + validated; honest skip-verify caveat; PEM tracking-wider craft).
3. Coherent reuse of the replication.tsx component vocabulary.

## Priority Issues
- [P1] Failed jobs are a diagnostic dead end — list payload carries no last_error; failed row shows only a badge + Resume. Fix: add last_error to the list DTO + surface inline (replication's CircleAlert+Tooltip). -> /impeccable harden
- [P1] Progress bar shows 0% for a healthy running job — pct() returns 0 when objects_total<=0 (the enumeration phase). Fix: indeterminate bar or bytes_done/bytes_total; show "X of Y". -> /impeccable harden
- [P2] "Import every bucket" is an unconfirmed footgun — empty Buckets copies all, immediate fire, collision risk. Fix: "Import all buckets" button label + confirm + collision warn. -> /impeccable harden
- [P2] Blind Buckets textarea instead of a picker — drives recognition + working-memory failures. Fix: Test-connection/fetch-buckets multi-select (needs the deferred POST /imports/source/buckets probe). -> /impeccable craft
- [P3] Empty Actions header + awkward mobile "Buckets" label wrap. Fix: visually-hidden actions header + reserved width; move Buckets help into a <p> beneath the label. -> /impeccable layout

## Persona Red Flags
- Alex (power): no saved profiles; no test-connection; no throughput/ETA/start-time; failed job = no error to act on.
- Jordan (first-timer): destination-is-local unclear; amber "Importing" reads as wrong; empty=import-all dangerous if caption unread.
- Sam (a11y): good label association + aria-busy, but validation is toast-only (no aria-invalid/aria-describedby, toast may auto-dismiss before AT); table has no aria-live; 0% aria-valuenow misleads.

## Minor Observations
- parseBuckets splits on ":" so a:b:c silently drops :c (low risk, zero feedback).
- bytes_total unused in the cell; "X of Y" would sidestep unknown-total.
- Full job id never copyable (only 8-char prefix).
- Source cell raw title vs replication's Tooltip.

## Questions
1. Where's the line between "a tool that explains itself" and "too quiet about the biggest action" (empty Buckets = copy everything)?
2. Why is a healthy running import amber?
3. Why a blind textarea instead of a fetched bucket picker for another-Cairn sources?
