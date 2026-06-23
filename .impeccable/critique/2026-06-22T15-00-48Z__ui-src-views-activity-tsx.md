---
target: Activity
total_score: 39
p0_count: 0
p1_count: 0
timestamp: 2026-06-22T15-00-48Z
slug: ui-src-views-activity-tsx
---
# Critique — Activity (`ui/src/views/activity.tsx`)

## Design Health Score — **39/40 · Excellent**  ·  trend **32 → 39 (+7)**

| # | Heuristic | Score | Δ | Key Issue |
|---|-----------|-------|---|-----------|
| 1 | Visibility of System Status | 4 | +1 | Event-count line names the 500-most-recent cap and shows "Showing M of N" when filtered; relative time gives at-a-glance freshness |
| 2 | Match System / Real World | 4 | +2 | Action column reads in plain English ("Deleted bucket", "Changed compression"); raw wire code on hover for experts |
| 3 | User Control and Freedom | 4 | +1 | One-click "Clear filters" (toolbar + empty state); filter state lives in the URL, so a view is linkable and survives refresh |
| 4 | Consistency and Standards | 4 | — | Shared spine throughout; the new `actionLabel` humanizer is reused by the Overview teaser so an event reads identically in both places |
| 5 | Error Prevention | 4 | — | Read-only surface — no destructive action to guard |
| 6 | Recognition Rather Than Recall | 4 | +1 | No enum decoding, no date parsing — plain labels + relative time remove the recall burden; dropdown shows humanized labels |
| 7 | Flexibility and Efficiency | 3 | +1 | Linkable/persisted filters are a real power-user win, but export, date range, and column sort were intentionally scoped out this round |
| 8 | Aesthetic and Minimalist Design | 4 | — | Destructive red stays rare and meaningful (delete-class only); count line is unobtrusive; relative time resolves the old hierarchy nit |
| 9 | Error Recovery | 4 | — | `ErrorAlert` with plain title, server message, and a working retry |
| 10 | Help and Documentation | 4 | +1 | Self-documenting: the cap is explained, action names are now plain English, the teaching empty state remains, raw code on hover |
| **Total** | | **39/40** | **+7** | **Excellent** |

## Anti-Patterns Verdict — PASS

`detect.mjs` over `activity.tsx` + `overview.tsx` + the shared components → **0 findings, exit 0**. No slop tells: the destructive cue is a single text colour on delete-class rows (the verb carries the meaning, so it's never colour-alone), not a stripe or badge. The humanizer is plain mapping, not decoration.

## What closed the gap (32 → 39)

- **Humanized actions (#2 2→4).** A shared `lib/activity.ts` `actionLabel()` maps the wire codes to operator phrasing, with a de-camelCase fallback so a future/unmapped action still reads as a phrase. The raw code is preserved on `title` hover. Applied to both the Activity table and the Overview teaser.
- **Restrained destructive cue.** `isDestructiveAction()` flags only bucket/object-destroying actions; those rows render in the destructive token. Red stays the rare, loud signal the brand reserves it as.
- **Freshness + scope (#1 3→4).** `relTime()` display with the absolute timestamp on hover; an event-count line that states the 500-row cap and a live "Showing M of N" when filtered (`aria-live`).
- **URL-persisted filters + Clear (#3 3→4, #7 2→3).** Filters moved from local `useState` to `useSearchParams`, so a filtered view is linkable and survives refresh; a one-click "Clear filters" appears in the toolbar and the empty state.

## The honest held-back point (#7 Flexibility = 3)

Export (CSV/JSON), a date range, and column sort were **deliberately scoped out** this round (the chosen power-user scope was URL-persisted filters + count/clear). They are the genuine remaining lever for an auditor working beyond the last 500 events; the score is held at 3 rather than inflated. Day-grouping separators were also deferred to keep the shared stacked `DataTable` intact — relative time already provides temporal orientation.

## Verdict

The page is now firmly in the **Excellent** band. The data finally speaks in the operator's voice, the window is honest about its ceiling, and a filtered view is a shareable URL. The only path to 40 is the audit-power tier (export + date range), which is a conscious, separately-scoped piece of work.
