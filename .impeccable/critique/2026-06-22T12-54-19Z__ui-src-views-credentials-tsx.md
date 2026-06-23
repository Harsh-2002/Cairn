---
target: credentials
total_score: 40
p0_count: 0
p1_count: 0
timestamp: 2026-06-22T12-54-19Z
slug: ui-src-views-credentials-tsx
---
# Critique — Temporary credentials (`ui/src/views/credentials.tsx`)

## Design Health Score — **40/40 · Excellent**  ·  trend **37 → 39 → 40**

| # | Heuristic | Score |
|---|-----------|-------|
| 1 | Visibility of System Status | 4 |
| 2 | Match System / Real World | 4 |
| 3 | User Control and Freedom | 4 |
| 4 | Consistency and Standards | 4 |
| 5 | Error Prevention | 4 |
| 6 | Recognition Rather Than Recall | 4 |
| 7 | Flexibility and Efficiency | 4 |
| 8 | Aesthetic and Minimalist Design | 4 |
| 9 | **Error Recovery** | **4** |
| 10 | Help and Documentation | 4 |

**Anti-patterns: PASS** — `detect.mjs` → 0 findings.

## What closed the last point (#9 Error Recovery, 3 → 4)

A failed mint is no longer a generic "Mint failed" + blanket retry. The error is classified by cause:

- **Rejected policy (4xx, typically 400)** — title "This policy can't be minted", the server's exact
  reason surfaced (e.g. *unknown action "s3:Frobnicate"*), remediation "— adjust the scoped policy
  below, then mint again", and **no retry button** (resubmitting the same document just fails again).
- **Forbidden (403)** — "Not allowed to mint", framed as a permissions problem, no retry.
- **Transient (network status 0 / 5xx)** — "Couldn't reach the server" / "Server error", **keeps the
  Try again button** because retrying can succeed; copy notes the policy is unchanged.

In every branch the draft is preserved, so "adjust and mint again" actually works. Both error states
were verified in-browser (fetch-intercepted 400 and a forced network reject).

## Verdict

This is the first page in the console at a clean **40/40**. There is no honest remaining lever — the
two gaps the 37 flagged (no post-mint control; failed-mint recovery) are both closed, and the
anti-pattern detector is clean. Further work here would be churn, not improvement.
