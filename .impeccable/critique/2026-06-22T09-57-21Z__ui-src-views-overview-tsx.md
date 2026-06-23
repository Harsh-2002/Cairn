---
target: overview dashboard
total_score: 33
p0_count: 0
p1_count: 0
timestamp: 2026-06-22T09-57-21Z
slug: ui-src-views-overview-tsx
---
# Critique — Overview dashboard (`ui/src/views/overview.tsx`)

## Design Health Score

| # | Heuristic | Score | Key Issue |
|---|-----------|-------|-----------|
| 1 | Visibility of System Status | 4 | Skeletons, refresh spinner, disk/storage bars, activity teaser — strong |
| 2 | Match System / Real World | 3 | Compression bar fills to 17% (stored) under an "83% smaller" headline — bar ≠ headline |
| 3 | User Control and Freedom | 3 | Read-only; refresh + links + "Go to buckets" in empty. Fine for an overview |
| 4 | Consistency and Standards | 4 | StatCard/Card/UsageBar/StatusBadge used consistently with the rest of the app |
| 5 | Error Prevention | 3 | Read-only, low risk; ErrorAlert + retry |
| 6 | Recognition Rather Than Recall | 4 | Everything labelled + linked; good number context ("of X original", per-bucket share) |
| 7 | Flexibility and Efficiency | 3 | Refresh + ⌘K + deep links; no accelerators beyond that (fine here) |
| 8 | Aesthetic and Minimalist Design | 3 | ~250px void in the Compression card (grid-stretched to the Node card's height) |
| 9 | Error Recovery | 3 | ErrorAlert + retry on load failure |
| 10 | Help and Documentation | 3 | A one-line description on every card |
| **Total** | | **33/40** | **Good** |

## Anti-Patterns Verdict — PASS

`detect.mjs` on `overview.tsx` → **0 findings**. Manually: no gradient text, glassmorphism, side-stripes, eyebrows, or numbered scaffolding. The 4-tile KPI row is the closest thing to a "hero-metric" cliché, but each tile is a plain labelled figure with real context (e.g. "Stored 236 KiB of 1.37 MiB original"), not the gradient big-number template — so it passes. Calm, on-brand Geist execution.

## Overall Impression

A solid, trustworthy dashboard that arrives as a whole (one combined load, non-destructive refresh) and reads calmly. Two things hold it back from excellent, both in the **Node + Compression row**: the Compression card is forced to the Node card's height and ends in a large empty void, and its progress bar fills to the *stored* proportion (17%) while the headline shouts the *saved* proportion (83%) — so a nearly-empty bar sits under "83% smaller", which reads as a glitch. The single biggest opportunity is to rework that row so the right column isn't a mostly-empty box and the compression bar communicates "saved" at a glance.

## What's Working

- **It arrives as a whole.** One `Promise.all` load (overview + per-bucket + system + activity) with the activity teaser degrading independently, and refresh re-fetches without tearing the page down. No skeleton waterfall.
- **The per-bucket "Storage by bucket" reflow is genuinely well done** — on desktop it's a 3-column `name · bar · bytes`; on mobile the bar drops to its own full-width row under the name+bytes. That's real responsive craft, not a breakpoint afterthought.
- **Numbers carry context, not just digits** — "of 1.37 MiB original", each bucket's % of total, disk "11.75 GiB free of 98.22 GiB". Recognition over recall.

## Priority Issues

- **[P2] The Compression card is a mostly-empty box on desktop.** In the `lg:grid-cols-2` row the Node card is 384px tall (7 fact rows + disk) and the grid stretches Compression to match — but its content (bar + 3 rows) is ~130px, leaving ~250px of void. **Fix:** stop forcing equal height (`items-start` on the row), or give Compression real estate it earns — e.g. lead with the **5.95× ratio as a large figure** + the stored/saved/original breakdown, or pair Node with a taller neighbour. (Verified: both cards measure exactly 384px.) *Heuristic: Aesthetic/Minimalist.* → `/impeccable layout`
- **[P2] The compression bar visualises the wrong number.** It fills to `storedPct` = **17%** (physical/logical) while the card headline is "83% smaller". A 17%-filled bar under an 83% claim reads as dissonant/broken. **Fix:** fill the bar to the **saved** proportion (83%) with a label like "83% saved", or make it a two-segment bar (stored vs saved) so both numbers are legible, or drop the bar and let the big ratio carry it. *Heuristic: Match-real-world / Recognition.* → `/impeccable clarify` + `/impeccable layout`
- **[P2] "Storage by bucket" is an unbounded list.** It maps every bucket (`buckets.map`) with no cap — on a node with 100+ buckets this one card becomes an endless scroll (same class as the metrics "by operation" issue). **Fix:** cap to the top ~8 by size with a quiet "and N more →" link to /buckets. *Heuristic: Aesthetic / Flexibility.* → `/impeccable layout`
- **[P3] Disk usage at 88% is a neutral bar.** High disk is exactly when an operator wants a signal; the bar stays the same dark fill at 5% and 95%. **Fix:** semantic tone past thresholds (amber ≥85%, red ≥95%) on the disk bar — the design system reserves semantic colour for "when it means something", and a near-full disk qualifies. *Heuristic: Visibility of status.* → `/impeccable colorize`
- **[P3] Activity teaser uses absolute timestamps + no scope.** Every row repeats "6/22/2026, 9:54:17 AM"; relative time ("3m ago") would be friendlier and consistent with the "Updated 3m ago" cue just added to Metrics, and a "showing 6 of N" hint would set scope. *Heuristic: Help / Match-real-world.* → `/impeccable clarify`

## Persona Red Flags

**Sam (Accessibility):** Bars carry `aria-label`s and the heading outline is real (h1 → h2 card titles), good. But the compression bar's meaning lives only in adjacent text, and the bar value (17%) contradicts the "83%" headline — a screen-reader user hears "Stored 236 KiB of 1.37 MiB (83% saved)" which is clear, but a low-vision user scanning the bar sees a near-empty bar and an 83% claim. Reconcile them.

**Riley (Stress Tester):** With 0 buckets the empty state is handled well; with 100 buckets the unbounded "Storage by bucket" list balloons. The Node card's long values (data directory, addresses) already truncate with `title` — good.

**Alex (Power User):** Nothing to drill into on the numbers — the stat tiles aren't links (Buckets/Objects don't navigate). Minor: making "Buckets" tile click through to /buckets would reward the scan.

## Minor Observations

- The empty "Storage by bucket" state renders a dashed `EmptyState` box *inside* the card — a mild nested-box; fine, but the dashed border inside a bordered card is slightly redundant.
- Stat tiles are static `<p>`s, not links — see Alex above.
- "Versions 6 / Objects 5" with no hint of why versions exceed objects; standard S3, low concern.

## Questions to Consider

- Should the Node + Compression row exist at all, or is "instance identity" a different altitude than "storage efficiency"? What if Compression were a compact strip beside the Stored stat tile, and Node owned its own full-width row?
- What's the one number an operator opens this page for — and is it the most prominent thing? (Right now four equal tiles compete; nothing is the hero.)
- Should the stat tiles be navigational (click Buckets → /buckets)?
