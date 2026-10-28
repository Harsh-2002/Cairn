# Object metadata authority — execution plan (protocol 3)

**Status:** approved architecture, implementation in progress. Human decision recorded 2026-10-28:
object bytes and per-object metadata files become the durable authority for object data; SQLite is a
rebuildable index plus the control-plane authority. `CONTRACT.md` carries this ceiling.

## Why

Cairn currently pays a separate SQLite publication flush after the data-directory barrier. RustFS
writes object metadata as a file beside the data and commits both with one directory sync. Removing
the pre-write admission bought only 8% (`0.663x` → `0.717x` PUT vs RustFS 1.0.4), so the remaining
gap is the extra metadata publication barrier. File metadata removes it while SQLite keeps the 3.5x
LIST advantage.

## Authority and consistency

- **Protocol 3 ordinary object:** data file + metadata file staged, fsynced, renamed, one directory
  barrier, then ACK. The metadata file is the object durability authority.
- **SQLite:** index and control-plane authority. The object row is submitted to the Writer after the
  durable file barrier. It is not on the ACK path.
- **Read-after-write:** a bounded in-memory overlay holds index rows not yet folded. GET/HEAD/LIST
  merge the overlay with the index, so a fresh object is visible immediately. Overlay insertion is
  atomic with the ACK.
- **Crash:** SQLite may lag. On exclusive startup, the mandatory full scan reads object metadata
  files and rebuilds missing index rows before serving. Acknowledged objects are never lost.
- **Protocol 2 and legacy objects:** no metadata file. SQLite remains their authority. Readers and
  the rebuild path accept both; migration writes sidecars for old objects in the background only
  after the protocol-3 path is proven, never in place of SQLite truth.

## File format

Sidecar path: `<bucket>/<uuid>.meta` (same directory as `<bucket>/<uuid>`), a new
`StoragePathRole::ObjectMetadata` plan path, versioned and canonical.

Record (serde JSON for v1; bounded by the existing metadata limits):
`format_version=1`, bucket, key, version id, row id, delete-marker flag, logical/physical size, etag,
content type/encoding/cache-control/disposition/language/expires/expire_at, storage path,
compression, storage class, owner, user metadata, ACL, checksums, SSE descriptor, internal SHA-256,
created/updated timestamps, object tags, Object Lock state, and the replication intent identity.
Control-plane rows (bucket config, users, policies, quotas, activity, outbox) stay SQLite-only.

Writes: serialize before the data rename; write to the staging alias; `fsync` data and metadata;
rename data then metadata; one coalesced directory fsync; ACK. Metadata rename failure aborts before
ACK. The record carries no secret plaintext.

Reads before rebuild is complete: index first, then the metadata file for exact row recovery.
Corrupt/missing sidecars are incidents; never invent an object.

## Phases

### P1 — dual-write (no ACK change)
Add the metadata path role and record; write/fsync/rename/barrier the sidecar with the data; keep the
SQLite commit before ACK. No speed claim. Tests: byte-exact sidecar contents vs row, crash after
rename before SQLite (orphan data+meta reclaimed), sidecar-only orphan, cleanup removes both, backup
includes sidecars, old objects without sidecars still serve, encrypted/compressed metadata.
Acceptance: full gate green, no data-path regression beyond one small file sync, existing tests
unchanged.

### P2 — index off the ACK path

The hard requirement is that moving the index off the ACK path must not weaken conditional PUT,
quota, versioning, Object Lock, or bucket delete/recreate. File metadata alone cannot serialize two
concurrent `If-None-Match: *` writes, so P2 adds a single-process **object coordinator**:

- **Per-key striped locks.** Every object mutation (PUT, Copy, DELETE, tag/ACL/lock, multipart
  completion) acquires the key's stripe, then evaluates conditions, versioning and Object Lock
  against the SQLite index **plus the in-memory overlay**. The durable metadata file is written and
  the directory barrier completes while the stripe is held, then the overlay entry is inserted
  atomically with the ACK. A concurrent conditional writer waits, sees the overlay entry, and fails
  with 412 correctly.
- **Bucket generation fence.** Bucket delete/recreate takes the bucket's fence in the same
  coordinator; an object ACK re-checks the bucket generation after acquiring its stripe. Delete
  therefore waits for in-flight object commits or refuses, instead of allowing a same-name
  injection.
- **Quota and counters.** Quota is checked under the stripe against the durable roll-up plus overlay
  deltas; the winning write records its delta in the overlay before ACK. After a crash, rebuild
  restores the roll-up from metadata files before readiness, so a post-crash node is exact.
- **Version ordering.** Version ids are generated locally; the current-version decision is
  serialized by the stripe and represented in the overlay, so the index fold order cannot change
  which version a client saw as current at ACK.
- **Overlay and fold.** The overlay is bounded (entries and bytes), in-memory only, and holds the
  exact row plus superseded generation for LIST/GET merging. A background Writer fold submits rows
  in order; fold failure keeps the entry and retries, never loses an ACKed object. Shutdown drains
  the overlay before exit.
- **Rebuild.** Startup scans metadata files, validates data/meta identity, and inserts missing rows
  and counters before serving. A lagging index is repaired; a corrupted or missing sidecar is an
  incident, never an invented object.
- **LIST/GET.** LIST merges the overlay with the index, preserving read-after-write. GET may resolve
  the row from the metadata file when the index misses it.

Strictness limits to state explicitly: the coordinator is process-local, which is valid for the
single-node ceiling. A hard crash loses only the overlay; rebuild reconstructs it from durable
metadata files. Quota may be briefly stale only for writes that were ACKed after the crash boundary
and are already represented in their metadata files; readiness waits for rebuild, so no client sees
the stale state.

P2 acceptance requires a two-writer conditional race test, quota under concurrent PUTs, bucket
delete vs in-flight PUT, version ordering, overlay bound/shutdown drain, and crash-with-lagging-index
rebuild. No feature flag; protocol 3 gates it.

Overlay + async fold + startup rebuild. ACK after the file barrier; SQLite submission follows.
LIST/GET merge the overlay. Crash/restart rebuilds missing rows from sidecars before readiness.
Feature is protocol-gated, no runtime flag. Acceptance: fr38 PUT ≥ 0.95x RustFS; LIST stays ≥ 3x;
crash tests prove no acknowledged object is lost and no unacknowledged row appears.

### P3 — reads and migration
GET/HEAD can resolve from the metadata file when the index is stale; LIST overlay is bounded and
aged. Background sidecar migration for legacy objects after a verified snapshot; never deletes
SQLite truth until the migration is proven and the protocol floor is raised.

### P4 — GC, backup, restore, integrity
Cleanup owns both paths; scrub verifies data and metadata; backup/restore includes sidecars and
refuses mixed generations; full scan validates sidecar/row agreement.

## Crash and failure matrix

| Boundary | Expected after restart |
|---|---|
| After data fsync, before metadata write | data orphan + no meta; scan reclaims both |
| After metadata fsync, before either rename | staging aliases reclaimed |
| After data rename, before metadata rename | data + staging meta; scan reclaims, no row |
| After both renames, before dir fsync | neither name guaranteed; scan reclaims, no row |
| After dir fsync, before SQLite fold | data+meta survive; rebuild inserts the row |
| After SQLite fold, before ACK | row exists; response loss only |
| After ACK | row and files survive; rebuild is idempotent |

Additional: SIGKILL at each failpoint, power-loss model test, sidecar corruption, mismatched
data/meta identity, two-principal bucket recreate, quota, Object Lock, versioned overwrite, delete
marker, multipart (unchanged), backup/restore with a lagging index.

## Guardrails

- No second control-plane store, no external service, no runtime feature flag, no new tuning knob.
- Schema changes are append-only; protocol 3 is a compatibility floor, not a migration toggle.
- LIST must remain index-based; the overlay is bounded and in-memory only.
- Every PR carries its crash test and a fr38 benchmark. Stop and KEEP the prior phase if PUT does not
  improve or LIST regresses.
- No release tag; merges only after the full gate.

## Acceptance

P2 is adopted only if, on fr38 with strict durability and c16:

1. PUT 1 MiB ≥ 0.95x RustFS (target > 1.0x).
2. LIST 1,000 keys ≥ 3.0x RustFS.
3. GET does not regress from the current 0.79x.
4. All crash/rebuild tests pass; no acknowledged object lost; no unacknowledged row served.
5. Startup rebuild time is bounded and reported for 10k/100k sidecars.
