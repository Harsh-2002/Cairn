---
target: credentials
total_score: 37
p0_count: 0
p1_count: 0
timestamp: 2026-06-22T10-10-32Z
slug: ui-src-views-credentials-tsx
---
# Critique — Temporary credentials (`ui/src/views/credentials.tsx`)

## Design Health Score

| # | Heuristic | Score | Key Issue |
|---|-----------|-------|-----------|
| 1 | Visibility of System Status | 4 | "Minting…" state, success toast, error alert, and the one-time reveal — clear |
| 2 | Match System / Real World | 4 | Domain-true: "Lifetime", "Scoped policy", "Session token (X-Amz-Security-Token)" |
| 3 | User Control and Freedom | 3 | Lifetime capped at 12h with no custom value; reveal exits only via "Done" (correct) |
| 4 | Consistency and Standards | 4 | Card + PermissionBuilder + CredentialsPanel + CopyField, same vocabulary as Users |
| 5 | Error Prevention | 4 | "Shown only once" alert + save-confirm gate; Mint disabled until the policy is valid |
| 6 | Recognition Rather Than Recall | 4 | The explainer tells you exactly what to do with the keys + when they die |
| 7 | Flexibility and Efficiency | 4 | Copy buttons, reveal toggles, Builder/Split/Code policy modes |
| 8 | Aesthetic and Minimalist Design | 3 | A couple of bordered explainer/option boxes nested inside the card |
| 9 | Error Recovery | 3 | ErrorAlert + retry on a failed mint |
| 10 | Help and Documentation | 4 | Strong inline guidance (least-privilege framing, SDK config hint, expiry behaviour) |
| **Total** | | **37/40** | **Excellent** |

## Anti-Patterns Verdict — PASS

`detect.mjs` over the view + its three components (`credentials-panel`, `permission-builder`, `copy-field`) → **0 findings**. No slop tells. The one-time reveal is a textbook trust pattern, not a template.

## Overall Impression

This is the strongest page in the console — a calm, single-purpose form that hands off to an exemplary one-time-secret reveal. The reveal nails the hard part: focus jumps to the heading, a "Shown only once" alert sets the stakes, secrets are masked with reveal+copy, the exact expiry and what-happens-after are spelled out, and "Done" stays disabled until the operator confirms they saved them. The only thing genuinely at odds with itself is the **default policy scope**: the page sells "least-privilege" but the form defaults to the *broadest* grant (All buckets · Read-only), so a thoughtless Mint yields an all-buckets token.

## What's Working

- **The one-time reveal (`CredentialsPanel`) is exemplary.** Focus-to-heading on appear, a `role="alert"` "Shown only once" banner, masked secrets with per-field reveal + copy, an explainer that states the exact expiry *and* the deny-on-expiry behaviour, and a "I have saved these credentials" gate before "Done — mint another". This is how you reveal a secret once.
- **Trust copy is precise.** "The credential can do exactly what the policy below grants — nothing more — and expires automatically." Names the session-token header the SDK actually needs.
- **Progressive disclosure via the PermissionBuilder** — presets (Read-only / Read & write / Full access) → Specific buckets → Advanced actions → raw Code — is the right ladder from novice to expert, and it's shared with Users so it's learned once.
- **Responsive:** the dense CopyField rows (key + reveal + copy) fit cleanly at 390px without overflow.

## Priority Issues

- **[P2] The default scope contradicts the "least-privilege" pitch.** The PermissionBuilder opens on **All buckets · Read-only**, so clicking Mint without touching anything issues an all-buckets-read token — the widest read grant, on a page whose copy emphasises least-privilege. **Fix:** either default "Which buckets" to **Specific** (forcing a deliberate pick), or echo the resolved scope next to the action — e.g. "Mint a read-only token for all 3 buckets" / a one-line summary above the button — so it's never a blind click. *Heuristic: Error Prevention / Match-real-world.* → `/impeccable clarify` (+ a small `permission-builder` default change)
- **[P3] Expiry is an absolute timestamp only.** "valid until 6/22/2026, 11:08:02 AM" makes the operator compute the lifetime they just picked. **Fix:** echo the chosen duration — "valid for 1 hour (until 11:08 AM)" — and/or a relative "expires in 59m". *Heuristic: Match-real-world / Recognition.* → `/impeccable clarify`
- **[P3] No view of active sessions.** The page is mint-only — you can't see what temporary credentials are live or revoke one early. They auto-expire (low urgency), but an operator can't answer "what's outstanding?" **Fix (feature):** a small "Active sessions" list (count · expiry · revoke). *Heuristic: Visibility of status / User control.* → `/impeccable shape`
- **[P3] Heading skip in the reveal state.** Page is `h1`; the form Card title is `h2`; but the reveal renders `CredentialsPanel`'s hardcoded `h3` with no `h2` between (the reveal Card has no CardTitle). **Fix:** make `CredentialsPanel`'s heading level a prop (or render it as `h2` here). *Heuristic: a11y.* → `/impeccable harden`

## Persona Red Flags

**Jordan (First-Timer):** Mostly well-guided, but could mint an all-buckets token without realising the scope (see P2). The "X-Amz-Security-Token" label is jargon — correct for the SDK audience, but a first-timer won't know it's the env var; the explainer's "session token" wording carries them, so it's fine.

**Sam (Accessibility):** Excellent — focus moves to the reveal heading, the warning is `role="alert"`, fields are labelled, the save gate is a real checkbox. Only nit: the `h1 → h3` skip in the reveal.

**Riley (Stress Tester):** Mint, then refresh mid-reveal → the secret is gone (correct, by design, and the warning said so). With 0 buckets, "Specific buckets" should degrade gracefully (handled by `bucketsLoading`/empty in the builder — worth a glance).

## Minor Observations

- The PermissionBuilder's "This lets the user…" explainer and the reveal's SDK explainer are bordered boxes inside the card — mild nesting, acceptable.
- Lifetime options stop at 12h; fine for the use case, but no custom value if someone wants 36h.
- The masked fields show a fixed dot count (good — never leak secret length).

## Questions to Consider

- Should "Mint" restate the effective grant (scope + actions + lifetime) right before issuing, the way the delete dialogs restate what they'll destroy? It's the same "confirm what this does" moment, inverted.
- Is "All buckets" the right default for a least-privilege tool, or should the safe/empty default force a choice?
- Would operators benefit from seeing (and revoking) outstanding sessions, or is auto-expiry enough?
