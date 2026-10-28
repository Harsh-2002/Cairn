# Cairn merged-tree security review — sanitized summary

Date: 2026-10-28

Trees reviewed:
- `merge/all-in-contract` at `c2d1dd0` (combined #125–#134)
- `security/installer-secret-output` at `c110ab7` (installer fix branch)

Method: read-only source and specification review of the merged changes and the current authorization, read, audit, cleanup, and installer paths. No code, test, service, or Git write. No tests executed.

## Confirmed findings: 2

| Severity | Title |
|---|---|
| High | Version-selector authorization mismatch on non-version-scoped object writes (pre-existing; still present in the merged tree) |
| High (release-tree only) | Installer still emits the generated root secret on stdout in the merged tree; the fix exists on `security/installer-secret-output` but is not merged into `merge/all-in-contract` |

## Hypotheses: 2

| Severity | Title |
|---|---|
| Medium | Bucket delete/recreate race can let an in-flight write land in a same-name bucket owned by another user; needs a two-principal test |
| Low/Medium | Multi-shard activity co-commit is non-atomic; an acknowledged PUT can lose its audit row if the global-shard write fails (code calls it best-effort) |

## Verified without a new finding

- `#132` single authorized bucket snapshot; source/destination authorization separation; in-transaction conditional writes; Object Lock re-check in the writer.
- `#132` `Arc` identity-policy sharing; session scoping; no parent-policy inheritance; Allow/Deny evaluation unchanged.
- `#128` read admission before probe; anchored descriptor reuse; range bounds; permit/lease lifetime; documented zero-copy exception.
- `#126` single-shard activity co-commit; only duplicate ids ignored; engine/capacity faults fail publication.
- `#125`/`#133` cleanup claim/lease/barrier/prune sequence; path and cross-bucket checks; assembly chunking and size ceilings.
- Installer fix on `c110ab7` is correct (secret no longer printed there), but it is not in the stated merge tree.

## Release note

The stated review tree and the checked-out branch differ: the installer fix is not an ancestor of `merge/all-in-contract`. If the merge tree is the release candidate, the installer secret exposure remains open there; if the installer branch is the candidate, its fix is verified.

No exploit details, file paths, line numbers, or reproduction steps are included in this summary.
