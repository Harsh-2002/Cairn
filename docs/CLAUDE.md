# docs

The engineering specification — **the single source of truth** for Cairn. Split into
section-numbered reference documents (`## N.` headings, sections `0`–`34`, contiguous), plus operator
runbooks and design/product docs. Code comments cite sections as **"ARCH N"** (e.g. `ARCH 11.6`);
~460 such references across `crates/` resolve through this folder. Read the relevant doc *before* a
non-trivial change, and update it *with* any change to the behaviour it specifies.

## Layout — the section → document map

The spec (sections are **stable identifiers** — every `ARCH N` in the code points here):

| Doc | Sections | Covers |
|---|---|---|
| `overview.md` | 0–5 | how-to-read, exec summary, scope/non-goals, why-Rust, baseline arch, gap analysis |
| `data-plane.md` | 6–7 | node model, data/control plane; concurrency, runtime, I/O model |
| `storage-durability.md` | 8–10 | durability & crash consistency; on-disk layout; compression at rest |
| `metadata.md` | 11–12 | metadata store (writer/WAL/cache, schema); the 9-trait abstraction layer |
| `s3-api.md` | 13, 16–19, 21 | S3 protocol & op catalogue; versioning + Object Lock; tagging; CORS; lifecycle; request lifecycles |
| `auth.md` | 14–15 | authentication (SigV4/Bearer); authorization (policy/ACL/BPA/ownership) |
| `replication.md` | 20 | bucket replication (outbox engine, SigV4-signing sink) |
| `control-plane.md` | 22–24 | management API; web console; CLI |
| `security-errors.md` | 25, 27 | error model & S3 error mapping; security/threat model |
| `observability.md` | 26 | metrics, logging, audit |
| `configuration.md` | 28 | the full `CAIRN_*` reference |
| `testing-performance.md` | 29–30 | testing/conformance; performance targets, sharding |
| `delivery.md` | 31–34 | build/deploy/ops; roadmap; ADR log; **appendices** (34.1 schema, 34.3 API matrix, 34.4 actions, 34.5 condition keys) |

Operator runbooks (NOT spec — these number their own local `## 1/2/3` headings; never cite them as
"ARCH N"): `operations.md` (deploy + **master-key rotation runbook**), `upgrade-rollback.md`,
`scaling-limits.md`, `troubleshooting.md`, `deployment-kubernetes.md`, `backup-restore.md`,
`disaster-recovery.md`, `migration.md` (import from another S3 store), `s3-api-matrix.md`,
`benchmarks.md`. Design/product: `design.md` (UI visual system),
`product.md` (positioning/brand).

## Notes

- **Never renumber a section.** The `## N.` / `### N.M` numbers are an external contract — `ARCH N`
  citations in code, docs, and tests resolve to them positionally. Append new sections; don't reorder
  or reuse. Renumbering silently breaks every reference that points at the old number.
- **Spec and code move together.** This is the source of truth, so a behavioural change MUST land its
  doc edit in the same change — never let the spec describe something the code no longer does. When
  they disagree the spec is authoritative; reconcile, don't ignore.
- **One section → one document.** Sections `0`–`34` partition across the reference docs above with no
  gaps and no overlap. A new section goes in exactly one doc; keep this map and the root
  `../CLAUDE.md` doc table in sync when the partition changes.
- The appendices in `delivery.md` (34.x) are reference tables that **must track the code**: 34.1 the
  metadata schema (mirror with `cairn-meta/src/schema.rs` migrations), 34.3 the API matrix, 34.4 the
  policy-action catalogue, 34.5 the condition-key catalogue. Stale appendix tables are a common trap.
- These are reference documents, not a tutorial — terse, declarative, cross-referenced by number.
  Match that voice; don't add narrative prose or duplicate content between docs (point by section
  number instead — duplication goes stale).

## Pointers

- Per-folder agent briefs live in `crates/*/CLAUDE.md`; the workspace brief, gate, and conventions
  are the root `../CLAUDE.md` — start there. End-to-end verification harnesses: `../conformance/`.
- `CLAUDE.md` files (this one included) are agent briefs, **not** part of the numbered spec.
