---
target: tags section
total_score: 23
p0_count: 0
p1_count: 3
timestamp: 2026-06-21T11-48-13Z
slug: ui-src-views-tags-tsx
---
# Critique — Tags section (`ui/src/views/tags.tsx`)

## Design Health Score

| # | Heuristic | Score | Key Issue |
|---|-----------|-------|-----------|
| 1 | Visibility of System Status | 2 | Selected tag uses `bg-muted/60` — nearly invisible; can't tell what's active |
| 2 | Match System / Real World | 3 | Plain language, key=value chips read well |
| 3 | User Control and Freedom | 2 | No way to clear/deselect a tag once chosen |
| 4 | Consistency and Standards | 2 | Master list rendered as a full DataTable; nested cards break the app's own border system |
| 5 | Error Prevention | 3 | Read-only view, low risk |
| 6 | Recognition Rather Than Recall | 2 | The per-tag object **count is clipped out of view** — user must guess |
| 7 | Flexibility and Efficiency | 2 | No filter/search on the tag list; no keyboard accelerators |
| 8 | Aesthetic and Minimalist Design | 1 | Nested cards, ~60% dead space, redundant headings, three tag icons |
| 9 | Error Recovery | 3 | ErrorAlert + retry on both panes |
| 10 | Help and Documentation | 3 | Descriptions present on both panes |
| **Total** | | **23/40** | **Acceptable (low) — significant rework warranted** |

## Anti-Patterns Verdict

**Deterministic scan:** `detect.mjs` on `tags.tsx` → clean (0 findings). The detector misses these because they're structural/data-driven, not token-level.

**LLM assessment:** It doesn't scream "AI generated," but it commits the project's own cardinal sin — **nested cards**. Each pane is a `Card` whose body contains a `DataTable`, and `DataTable` draws its own `rounded-lg border`. The result is a bordered box inside a bordered box on *both* sides. DESIGN.md says depth comes from 1px borders, *not* stacked boxes; this is the opposite.

## Overall Impression

The data model is right (master list of `key=value` + count → detail of objects), but the execution undercuts it. The single most important number in the master list — **how many objects carry each tag — is invisible**, scrolled off the right edge because a 560px-min-width table is jammed into a ~300px card. The page also reads as unfinished: two cards float at the top of a mostly empty viewport. The biggest opportunity is to stop treating this as "two cards with tables inside" and build it as one real **master–detail panel** that fills the space, shows the count, and makes selection obvious.

## What's Working

- **The mental model is correct.** Distinct `key=value` tags on the left, objects-carrying-it on the right, each object linking into its bucket — that's exactly the right IA for "where is this tag used?"
- **The `TagChip`** (mono `key`=`value` with a muted `=`) is a clean, reusable identity for a tag and is used consistently in the list, the detail header, and the empty state.
- **States are handled** — loading skeletons, per-pane error + retry, and distinct empty states all exist.

## Priority Issues

- **[P1] The per-tag object count is clipped out of view.** The master list is a `DataTable` with `minWidth` 560 living inside a ~300px card, so `overflow-x-auto` scrolls the right-aligned "Objects" column off-screen. The one quantitative fact the master list exists to show is hidden on desktop (ironically it *does* show on mobile, where the P3 stacking labels it). **Fix:** drop the table; render each tag as a full-width row with the chip left and the count right, always visible.
- **[P1] Nested cards on both panes.** `Card` → `CardContent` → `DataTable(border)` is a box-in-a-box, against the project's "borders, not stacked boxes" rule. **Fix:** one bordered master–detail frame divided by a single hairline; no inner bordered tables.
- **[P1] Hollow, top-heavy layout.** Two cards sit at the top and ~60% of the screen is empty. It reads as unfinished rather than "spacious." **Fix:** a full-height (`100dvh`-based, min ~30rem) two-column panel that fills the column, list scrolling independently of the detail.
- **[P2] Selection is nearly invisible.** The active tag is `bg-muted/60` with no weight/accent change. **Fix:** `bg-accent` fill + medium weight + `aria-current`, so the active row is unmistakable.
- **[P2] Redundant headings & icons.** "Tags" appears in the nav, the H1, *and* the left card title; the tag icon appears three times. **Fix:** drop the duplicate card title; let the page H1 own the name and the panes carry only what's unique (a filter + count on the left, the selected chip on the right).
- **[P3] No filter on the tag list.** Fine at 4 tags, unusable at 80. **Fix:** a small filter input in the master header.

## Persona Red Flags

**Alex (Power User):** No filter/typeahead to jump to a tag in a long list. Can't tell which tag is selected at a glance. No keyboard hint that rows are selectable beyond tab-focus.

**Sam (Accessibility):** Selection conveyed by a near-imperceptible background tint alone — fails "don't rely on subtle color"; needs `aria-current` + a visible non-color cue. (Rows are real buttons, which is good.)

**Riley (Stress Tester):** With one bucket's tags this looks fine; at 50 tags the master list becomes an endless scroll with no filter, and the clipped count means you can't even sort by "most used" by eye.

## Minor Observations

- The detail repeats "demo-bucket" in its own column and again inside each object link; defensible (tags span buckets) but visually doubled when they don't.
- Two separate empty states ("No tags yet" / "Select a tag") each draw their own dashed inner box — more nested boxes.
- "4 distinct tags in use." is good microcopy; keep it, just relocate it into the master header.

## Questions to Consider

- What if the master list looked like a real navigator (file-tree / inbox), filling the height, instead of a short table floating in a card?
- Should the count be a sortable signal (most-used tags first) rather than alphabetical?
- Does the detail need a full table, or would a denser object row read better inside a narrower detail column?
