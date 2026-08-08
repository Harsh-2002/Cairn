# Migrating into Cairn from another S3 store

Cairn can import buckets and objects from any S3-compatible store — MinIO, Garage, Cloudflare R2,
AWS S3, or another Cairn node — directly **into this node**. The import runs server-side as a
resumable background job you configure through the management API (a `cairn import` CLI subcommand
and a console wizard drive the same API). This is an operator runbook; the spec is Section 27.7
(safety model) and Section 28 (configuration).

## 1. What it does

- Streams objects from the source through Cairn's **normal object-write path**, so server-side
  encryption (SSE-S3), compression, quota, versioning, and event notifications all apply to imported
  objects exactly as to a normal upload.
- Copies the **current version** of each object with its content type, user metadata (`x-amz-meta-*`),
  standard headers (content-encoding/-disposition/-language, cache-control), and tags preserved.
- Is **bounded and resumable**: it pages the source listing (never loading a whole bucket into
  memory), copies under a capped worker pool, and checkpoints a per-bucket cursor after every page so
  a restart or a cancel-and-resume continues rather than re-copying.

**Deferred in this version:** historical object versions and delete markers (current version only),
Object-Lock/retention/legal-hold state, and per-object ACLs (imported objects are owned by the root
administrator with the destination bucket's default ownership). Any-to-any (source and destination
both remote) and per-object checksum-verification ledgers are future work.

## 2. Prerequisites

- **Admin credentials for the source** (read access to the buckets you want) and Cairn's root
  administrator credentials for the management API.
- The source endpoint must be reachable from the Cairn node. If the source is on a **private
  network** (loopback, RFC1918, link-local), the SSRF guard blocks it by default — set
  `CAIRN_ALLOW_INTERNAL_ENDPOINTS=true` (Section 28) to allow it. Never leave that on in production
  unless you genuinely need it; it disables the guard for *all* outbound dialers.
- Tune the worker pool with `CAIRN_IMPORT_DEFAULT_WORKERS` / `CAIRN_IMPORT_MAX_WORKERS` and the
  node-protection ceiling `CAIRN_IMPORT_GLOBAL_MAX_INFLIGHT` (Section 28). The global cap is held
  below the blob-I/O pool so a bulk import cannot starve live traffic. Concurrent jobs share this
  one FIFO budget rather than each receiving a separate allowance.
- Source networking is bounded by `CAIRN_IMPORT_CONNECT_TIMEOUT_SECS`,
  `CAIRN_IMPORT_RESPONSE_HEAD_TIMEOUT_SECS`, and `CAIRN_IMPORT_BODY_IDLE_TIMEOUT_SECS`; one complete
  object copy is additionally bounded by `CAIRN_IMPORT_WHOLE_OBJECT_TIMEOUT_SECS`. Raise the
  whole-object value for very large objects over slow links rather than disabling the idle guard.

## 3. Run an import (management API)

The endpoints live on the console/API listener (`CAIRN_WEB_ADDR`, default `:7374`), under `/api/v1`,
and require an administrator Bearer token (`<access-key>.<secret>`).

Optionally, list the buckets a set of source credentials can see before committing to a job — this is
what the console's **Fetch buckets** step calls. The secret signs a single `ListBuckets` and is never
sealed, stored, logged, or echoed:

```sh
curl -s -X POST http://127.0.0.1:7374/api/v1/imports/source/buckets \
  -H 'Authorization: Bearer cairn.cairnadmin' \
  -H 'content-type: application/json' \
  -d '{
        "source_endpoint": "https://minio.internal:9000",
        "source_region":   "us-east-1",
        "access_key":      "SRC_ADMIN_KEY",
        "secret":          "SRC_ADMIN_SECRET"
      }'
# -> {"buckets":["media","backups","logs"]}
```

Create a job (an empty `buckets` list means "every bucket the source credentials can see"; an entry's
`dest` defaults to its `source` name):

```sh
curl -s -X POST http://127.0.0.1:7374/api/v1/imports \
  -H 'Authorization: Bearer cairn.cairnadmin' \
  -H 'content-type: application/json' \
  -d '{
        "source_endpoint": "https://minio.internal:9000",
        "source_region":   "us-east-1",
        "access_key":      "SRC_ADMIN_KEY",
        "secret":          "SRC_ADMIN_SECRET",
        "buckets":         [{"source": "media", "dest": "media"}],
        "workers":         16
      }'
# -> {"id":"<job-id>"}
```

The secret is **sealed under the master key immediately and never returned**. The response is the job
id only. For a self-signed-TLS source, pass `"ca_cert": "-----BEGIN CERTIFICATE-----\n…"` (or, for
testing only, `"insecure_skip_verify": true`).

Watch progress:

```sh
curl -s 'http://127.0.0.1:7374/api/v1/imports?limit=100' -H 'Authorization: Bearer cairn.cairnadmin' # list page
curl -s http://127.0.0.1:7374/api/v1/imports/<job>  -H 'Authorization: Bearer cairn.cairnadmin'   # detail
```

The detail shows the job state, aggregate `objects_done/objects_total` and byte counters, and a
per-bucket breakdown with each bucket's state and most-recent error. Neither the list nor the detail
ever contains the source secret. The list accepts `limit=1..1000` (default 1000) and an opaque
`cursor` copied verbatim from `next_cursor`; `next_cursor: null` is the final page. Invalid limits or
cursors return `400` instead of silently changing the requested page. Equal-millisecond jobs remain
stable because the cursor carries both creation time and job id.

Cancel or resume:

```sh
curl -s -X DELETE http://127.0.0.1:7374/api/v1/imports/<job>        -H 'Authorization: Bearer cairn.cairnadmin'
curl -s -X POST   http://127.0.0.1:7374/api/v1/imports/<job>/resume -H 'Authorization: Bearer cairn.cairnadmin'
```

A cancel stops cleanly at the next page boundary; a resume re-runs from the stored per-bucket cursors,
skipping what was already copied. If the node restarts mid-import, the job is reclaimed on startup and
resumes automatically from its cursors.

## 4. Failure handling

- A per-object failure (a transient source error past the retry budget, or a permanent 4xx) is
  **recorded and skipped** — one poison object never fails the whole job. Re-running (resume) retries
  from the cursor; a duplicate PUT of an already-copied object simply overwrites identical bytes.
- A whole-bucket failure (the source bucket is unreachable, or the destination cannot be created)
  marks that bucket failed with the error in the detail; other buckets still complete.
- A connection, response-head, body-idle, or whole-object timeout is retryable under the job's
  attempt policy. Its in-flight permit is released before backoff, so another queued object or job
  can progress while the failed attempt waits.
- Keys the destination cannot represent (an S3-illegal key the source allowed) are recorded as
  terminal for that object and skipped.
- On startup and each persistent import heartbeat, finished job rows older than
  `CAIRN_IMPORT_RETENTION_SECS` (default 7 days) are pruned in bounded batches. Pending/running work
  is never eligible, and a large upgrade backlog drains over later heartbeats without one
  history-sized writer transaction.

## 5. Verifying

After a job reports `completed`, list the destination buckets with any S3 client and spot-check a few
objects for byte-exactness and metadata. A large object is a good check that the streaming path
round-tripped it without truncation.
