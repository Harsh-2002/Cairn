---
target: tags section (post-redesign)
total_score: 33
p0_count: 0
p1_count: 0
timestamp: 2026-06-21T12-38-44Z
slug: ui-src-views-tags-tsx
---
# Critique — Tags section (post-redesign)

## Design Health Score

| # | Heuristic | Score | Was | Note |
|---|-----------|-------|-----|------|
| 1 | Visibility of System Status | 3 | 2 | Selected row now unmistakable (bg-accent + aria-current); skeletons on both panes; "N objects" count |
| 2 | Match System / Real World | 3 | 3 | Plain language, key=value chips, "Objects tagged X" |
| 3 | User Control and Freedom | 3 | 2 | Free switching + filter; still no explicit "clear selection" (keeps it off 4) |
| 4 | Consistency and Standards | 4 | 2 | Uses the app border system correctly; no nested cards; standard table primitives |
| 5 | Error Prevention | 3 | 3 | Read-only view, low risk |
| 6 | Recognition Rather Than Recall | 4 | 2 | Per-tag count always visible; filter; active tag obvious |
| 7 | Flexibility and Efficiency | 3 | 2 | Filter/typeahead, keyboard-focusable rows; no jump-shortcut or sort |
| 8 | Aesthetic and Minimalist Design | 4 | 1 | Single frame fills the column; clear hierarchy; no dead space or clutter |
| 9 | Error Recovery | 3 | 3 | ErrorAlert + retry on both panes |
| 10 | Help and Documentation | 3 | 3 | Descriptions + inline empty-state guidance |
| **Total** | | **33/40** | **23** | **Good** |

## Summary

The three P1s are closed: the per-tag object count is now always visible (pill badge + detail
"N objects"), the nested-card structure is gone (one bordered frame split by a hairline), and the
hollow layout is replaced by a full-height navigator. The two P2s are closed too: selection is now
bg-accent + inverted badge + aria-current, and a filter input was added. Detector scan remains clean.

Remaining (P3): no explicit deselect/clear-selection control; no sort-by-count or keyboard jump.
None are blocking. Score moves 23 → 33 (Acceptable → Good).
