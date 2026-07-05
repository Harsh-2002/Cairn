# cairn-import

The S3 → Cairn import engine (ARCH 27): copies buckets and their objects from a remote
S3-compatible store (MinIO / Garage / R2 / AWS / another Cairn) **into this node**. Trait-generic
like `cairn-replication` — it holds no concrete backend, only `ImportOpts` — so the engine is
exercised entirely against in-memory doubles here and wired to real impls by `cairn-server`.

## Layout (`src/`)
- `lib.rs` — `ImportEngine` (`run`/`copy_bucket`/`copy_one`), the three seams (`SourceReader`,
  `DestWriter`, `ProgressSink`), `ImportError` (the `Retryable`/`Unavailable`/`Terminal` taxonomy,
  mirroring `cairn-replication`'s), `BucketPlan` (the per-bucket resume cursor + prior counters),
  `ImportOpts`/`ImportReport`, and `next_backoff` (deterministic exponential backoff).
- `source.rs` — `HttpS3Source`: the production `SourceReader`. SigV4-signed `GET`/`ListObjectsV2`/
  `ListBuckets` against a remote endpoint over http or https, dialing through the SSRF-guarded
  connector (`cairn-net`). Reuses `cairn-auth`'s signing primitives directly, so a signed request is
  byte-identical to what a real verifier expects.
- `tests.rs` — the engine unit suite (`#[path]`-included from `lib.rs`) against in-memory
  `SourceReader`/`DestWriter` doubles: paging, streaming, retry/defer classification, cursor resume,
  cancellation.
- `tests/source_http.rs` — `HttpS3Source` against a tiny in-process hyper server: proves a real
  signed round-trip, including the tag-fetch behavior (see below).

## Scale is bounded by construction
Enumeration streams (paged `ListObjectsV2`, **never** list-all-into-memory); each page's objects
copy under a per-bucket `object_workers` fan-out **and** a single global `Semaphore` whose permit
ceiling (`global_max_inflight`) caps total in-flight work across every bucket — held **below** the
blob-I/O permit pool (`DEFAULT_BLOB_IO_CONCURRENCY`) so a bulk import can never starve the node's
live GET/PUT traffic. Object bodies stream source→dest and are never buffered whole. Resume is one
cursor per `(job, bucket)`, carried in `BucketPlan` — a billion-object bucket never balloons memory
or the checkpoint.

## Fidelity (`SourceObject`)
Preserved on import: content-type, user metadata (`x-amz-meta-*`), the standard headers
(content-encoding/-disposition/-language, cache-control), and the object's **tag set**. Tags are
fetched via a second signed `GET ?tagging` in `HttpS3Source::get_object` — **unconditional**, not
gated on the source's `x-amz-tagging-count` response header: that hint is not something every
S3-compatible source is guaranteed to send (Cairn's own `GetObject` doesn't today), and skipping the
fetch on its absence would silently drop tags from exactly the sources — including Cairn itself —
that don't set it. A tag-fetch failure is **non-fatal**: it degrades to an empty tag set rather than
failing an otherwise-successful object copy (the body GET already succeeded). v1 imports the
**latest version only** — Object Lock / retention is skipped but surfaced in the job's notes, never
silently.

## Notes
- **Error taxonomy drives retry policy**, exactly like replication: `Retryable` backs off and burns
  an attempt (terminal after the budget); `Unavailable` (source down: transport error, 5xx, 429)
  reschedules without burning attempts, so an outage auto-resumes; `Terminal` (4xx, missing object)
  fails that object immediately — the job continues past it, never aborts the batch.
- **Progress is throttled, not per-object.** `ProgressSink::report` is called once per page (not
  once per object), avoiding a write-amplification trap on a bucket with millions of small objects.
  Returning `false` requests cooperative cancellation, honored at the next page boundary.
- The server's concrete wiring (`LocalDestWriter`, the claim/lease/resume loop, migration v20
  schema) lives in `cairn-server` (`import_dest.rs`/`import_run.rs`) and `cairn-control`
  (the `/imports` control-plane routes) — see those crates' briefs.
- Spec: `docs/migration.md` (the operator-facing import runbook). See the root `../../CLAUDE.md` for
  the gate and workspace-wide conventions.
