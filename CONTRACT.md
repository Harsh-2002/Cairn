# CONTRACT.md — the ceilings

**This file is edited by humans only.** If you are an AI agent and a change seems to require moving
one of these ceilings, **stop and ask**. Do not raise the ceiling, do not work around it, and do not
edit this file to permit the thing you want to build.

These are not conventions — conventions live in [`CLAUDE.md`](./CLAUDE.md), which tells you *how* to
work here. This file tells you what you may **not decide alone**. Each line is the most complexity
Cairn tolerates **right now**. "Right now" is deliberate: several of these will move, but only when a
human decides they should, never as a side effect of a task.

A pull request that crosses a ceiling without a human decision on record is rejected on that basis
alone, however good the code is.

---

**Topology — one node.** Cairn is a single-node object store. No clustering, no consensus, no
multi-node HA, no leader election, no shared-nothing sharding across machines. Durability across
machines is *asynchronous replication* and *backup/restore*, and that is the whole story. This is a
deliberate product position, not a gap waiting to be filled.

**Metadata — one embedded SQLite database, one writer, plus durable per-object metadata.**
No external database, no Redis, no message broker, no queue service. All index and control-plane
writes go through the single group-committing `Writer`; reads use the WAL pool. From storage
protocol 3, object bytes and their per-object metadata files are the durable authority for object
data: SQLite is a rebuildable index for object listings and the authority for control-plane state
(buckets, policies, users, locks, quotas, replication, activity). An object's durable file is the
per-object authority. A plaintext object is one file — its payload followed by a `CRNI` metadata
trailer; a compressed or encrypted object carries a `<uuid>.meta` sidecar because its container
trailer must stay last. That per-object metadata is part of the storage format, not a second
control-plane store. If something seems to need another datastore, it needs a human decision first.

**Configuration — environment variables only.** Every knob is a `CAIRN_*` env var parsed by strict
Figment. There is no config file, no TOML, no YAML, no CLI flag that sets configuration. Do not add
one, and do not add a "just for this" escape hatch that reads a file.

**Schema — append-only.** Migrations are append-only. Never edit a migration that has been applied.
There is no down-migration story and none is wanted.

**Durability — the ordering is fixed.** Stage → fsync file → rename → fsync dir → validate hashes →
commit the metadata transaction → reclaim superseded blobs. Do not reorder these, do not make any
step conditional, do not "optimise" one away. For a protocol-3 ordinary object write the commit is
the object's durable file — the data file with its trailer, or the data file and its `.meta`
sidecar — committed under the same file and directory barriers; a plaintext object has no second
file to stage, so one barrier covers both its payload and its metadata. The SQLite index fold is a
post-acknowledgement index update and is not the object durability authority. Visibility is not
deferred: an acknowledged row is served from a bounded in-memory overlay until its index fold
lands, and every index-dependent read drains that overlay first.

**Crypto — fails closed, one ring.** A missing key, a wrong key, or a tampered envelope returns an
error. Never plaintext, never zeros, never partial data, never a warning-and-continue. Every DEK is
sealed under the one master ring; `aws:kms` key ids are labels and a write-time allow-list, not
cryptographic isolation. Do not present them as isolation.

**The console — presentation only.** The web console holds no privileged logic; it is a client of
the same `/api/v1` and S3 surfaces everyone else uses. Object bytes must never become active content
in the console origin. Do not add a server-side rendering path, a console-only endpoint that skips
authorization, or a preview that executes what it renders.

**Scope of a change — no silent widening.** Do not add a dependency, a background thread, a cache
layer, or a new long-lived process to solve a problem inside one request path. Do not add a feature
flag to keep two designs alive at once.

**Releases — exactly one, cut by a human.** One release is ever active. Publishing is a deliberate
human act, gated on green CI for the exact commit. An agent never cuts, tags, or publishes a release,
and never deletes or retires an existing one, without being told to in that session.
