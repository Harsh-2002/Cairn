---
target: credentials
total_score: 39
p0_count: 0
p1_count: 0
timestamp: 2026-06-22T10-55-21Z
slug: ui-src-views-credentials-tsx
---
# Critique — Temporary credentials (`ui/src/views/credentials.tsx`)

## Design Health Score

| # | Heuristic | Score | Δ | Key Issue |
|---|-----------|-------|---|-----------|
| 1 | Visibility of System Status | 4 | — | Minting state, success toast, one-time reveal — plus a live **"Will mint: …"** grant echo and a live Active-sessions count |
| 2 | Match System / Real World | 4 | — | Domain-true ("Lifetime", "Scoped policy", session-token header) and a plain-language "This lets the user…" outcome echo |
| 3 | User Control and Freedom | **4** | **+1** | **Active sessions can now be revoked** — minting is no longer one-way. Lifetime still preset-only (no custom value) |
| 4 | Consistency and Standards | 4 | — | Card + PermissionBuilder + CredentialsPanel + CopyField, same vocabulary as Users; new Sessions card reuses DataTable + StatusBadge |
| 5 | Error Prevention | 4 | — | "Shown only once" + save-confirm gate; the grant echo footer now shows the broad default *before* you commit |
| 6 | Recognition Rather Than Recall | 4 | — | The explainer says exactly what to do with the keys and when they die |
| 7 | Flexibility and Efficiency | 4 | — | Copy/reveal toggles, Builder/Split/Code policy modes, per-session revoke |
| 8 | Aesthetic and Minimalist Design | **4** | **+1** | The page is now a clean **two-card composition** (mint → live sessions); the explainer/option boxes read as form controls, not decorative nesting |
| 9 | Error Recovery | 3 | — | Failed-mint recovery is still just ErrorAlert + retry — no distinction between a transient error and a policy rejection, no remediation copy |
| 10 | Help and Documentation | 4 | — | Strong inline guidance (least-privilege framing, SDK config hint, expiry behaviour) |
| **Total** | | **39/40** | **+2** | **Excellent** |

**Trend: 37 → 39 (+2).**

## Anti-Patterns Verdict — PASS

`detect.mjs` over the view + its components (`credentials-panel`, `permission-builder`, `copy-field`) → **0 findings**. No slop tells. The one-time reveal and the new session list are both textbook trust patterns, not templates.

## Overall Impression

This was already the strongest page in the console; the round closed the two real gaps the
37/40 flagged. **User Control** lifts because minting is no longer a one-way door — every valid
session now appears in an **Active sessions** card with a confirm-gated **Revoke**, so an operator
who mints the wrong scope can immediately kill it. **Aesthetic** lifts because the page reads as a
deliberate two-card story (issue a credential → watch it live until it expires) with a proper empty
state, rather than a single dense form. And the prior "thoughtless Mint yields an all-buckets token"
worry is now defused at the source: the **"Will mint: Read-only · all buckets · 1 hour"** footer
echoes the exact grant the instant before you commit.

The one point still held back is honest: **Error Recovery (#9)** is unchanged. A failed mint shows a
generic alert and a retry; it doesn't tell the operator *why* (network vs. a rejected policy) or what
to change. That's the next — and only — real lever on this page.

## What changed since 37/40

- **Active sessions list + revoke** (`useResource(api.listSessions)` → `DataTable` with Access key /
  Scope / Created / Expires / Revoke). Backed end-to-end: `list_session_credentials` +
  `DeleteSessionCredential` mirrored across the SQLite store, the libSQL/Turso store, the in-memory
  double, and the cache/shard delegations; admin-gated; `record_activity("RevokeSessionCredential")`.
  Revoke routes through `ConfirmDialog`; the list refreshes after both mint and revoke.
- **Grant echo footer** — `Will mint: {summarizePolicy(doc)} · {durationLabel}` (new
  `summarizePolicy` in `lib/policy.ts` reusing `policyToPreset`/`LEVELS`), so the broad default is
  visible before the click.
- **Expiry echo on reveal** — "Valid for {durationLabel} — until {whenMs}".
- **Heading semantics** — `CredentialsPanel` title is now a real `h2` (`headingLevel` prop).

## The one thing left (optional, low priority)

**#9 Error Recovery — make a failed mint actionable.** Map the control-plane error to a cause:
a 4xx policy rejection should surface the offending statement ("This policy can't be minted: …"),
a network/5xx should read "Couldn't reach the server — retry". Keep the draft intact (already does).
This is the only move that takes the page to 40, and it's small — but it's genuinely optional; the
page is firmly in the Excellent band as-is.
