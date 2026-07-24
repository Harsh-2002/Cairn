# Cairn Operations Guide

This guide covers configuring, deploying, and operating a Cairn node. Cairn is a single-node,
locally-backed, S3-compatible object store: one process, one data filesystem, one SQLite
metadata file. Cross-host redundancy comes from asynchronous bucket replication, not clustering.

## 1. The one-filesystem invariant

The SQLite database file, the staging directory, and the per-bucket blob directories **must
reside on the same filesystem**, because the commit protocol relies on atomic `rename(2)`,
which only works within a filesystem. The staging area lives inside the data root by design.
Violating this (e.g. mounting staging elsewhere) breaks the durability protocol.

Place the data filesystem on **redundant storage** — Cairn does not implement drive redundancy.
A checksumming, redundant filesystem (ZFS) gives redundancy *and* silent-corruption detection;
software/hardware RAID or a cloud block volume with provider redundancy are also fine.

## 2. Configuration

Configuration is **environment-only**: built-in defaults overlaid with `CAIRN_*` environment
variables. There is no configuration file and no `--config` flag — a Cairn host or container is
configured purely by env. It is validated on load; an invalid configuration fails fast. Validate
without starting: `cairn validate-config`.

| Setting | Env var | Default | Meaning |
|---|---|---|---|
| S3 API listener | `CAIRN_LISTEN_ADDR` | `0.0.0.0:7373` | S3 data plane, plus `/healthz`, `/readyz`, `/metrics`, and signed `/p/` share URLs. |
| Web console listener | `CAIRN_UI_ADDR` | `0.0.0.0:7374` | Management console (root path) + management API (`/api/v1`). Set `off`/`none`/empty to run headless. |
| Data directory | `CAIRN_DATA_DIR` | `./data` | Root of staging + per-bucket blobs. |
| Database path | `CAIRN_DB_PATH` | `./data/cairn.db` | SQLite metadata file (same FS as data). |
| Region | `CAIRN_REGION` | `us-east-1` | Location label + SigV4 scope. |
| Master key | `CAIRN_MASTER_KEY` | *(dev key)* | 32-byte hex; AEAD key for secrets at rest. **Set in production.** |
| TLS cert / key | `CAIRN_TLS_CERT_PATH` / `CAIRN_TLS_KEY_PATH` | unset | Enable built-in TLS when both set. |
| Max object size | `CAIRN_MAX_OBJECT_SIZE` | 5 TiB | Hard per-object ceiling. |
| Concurrency limit | `CAIRN_CONCURRENCY_LIMIT` | 1024 | Max in-flight requests. |
| Request timeout | `CAIRN_REQUEST_TIMEOUT_SECS` | 300 | Per-request timeout. |
| Log level / format | `CAIRN_LOG_LEVEL` / `CAIRN_LOG_FORMAT` | `info` / `text` | Verbosity; `text` or `json`. |
| Dev auth | `CAIRN_DEV_AUTH` | `false` | Loopback-only auth bypass (debug builds only). |
| Encrypt at rest | `CAIRN_ENCRYPT_AT_REST` | `false` | Transparently encrypt every stored object (an operator storage property; not advertised as SSE). |
| KMS key-id allow-list | `CAIRN_KMS_KEY_IDS` | *(unset = accept-all)* | Comma-separated `aws:kms` key ids accepted on writes; a label, not key isolation. Gates writes only. |
| STS wire surface | `CAIRN_STS_ENABLED` | `true` | Serve `AssumeRole`/`GetSessionToken` on the S3 port; set `false` to disable. |

> **Master key.** SigV4 secrets and replication credentials are envelope-encrypted under this
> key. Supply it out of band; **keep it out of the backup** that contains the database, so the
> backup alone cannot disclose secrets. Without it, a fixed insecure development key is used.

### Replication targets

The replication worker ships outbox entries to one or more S3-compatible destinations (20).

* **Single target** (node→node) — set `CAIRN_REPLICATION_ENDPOINT`, `CAIRN_REPLICATION_ACCESS_KEY`,
  and `CAIRN_REPLICATION_SECRET` (optionally `CAIRN_REPLICATION_DEST_BUCKET`,
  `CAIRN_REPLICATION_REGION`). Each source bucket's *destination bucket* is resolved from its own
  replication rule; the endpoint and credentials are shared.

* **Multiple targets** — set `CAIRN_REPLICATION_TARGETS` to a JSON array of named destinations,
  each with its own endpoint, credentials, and TLS trust. Each source bucket routes to the target
  whose `dest_bucket` (or `name`) matches its replication rule; a bucket matching no target falls
  back to the single-target keys above when present.

  ```json
  [
    { "name": "west", "endpoint": "https://s3.west.example", "region": "us-west-2",
      "dest_bucket": "mirror-west", "access_key": "AK...", "secret": "...",
      "ca_path": "/etc/cairn/west-ca.pem" },
    { "name": "lab", "endpoint": "https://s3.lab.internal", "region": "us-east-1",
      "dest_bucket": "mirror-lab", "access_key": "AK...", "secret": "...",
      "insecure_skip_verify": true }
  ]
  ```

  Per-target TLS trust for an `https://` endpoint: `ca_path` trusts a private CA's PEM bundle
  instead of the public webpki roots; `insecure_skip_verify` disables certificate verification
  entirely (**dangerous** — testing only, and logged loudly). The two are mutually exclusive.

## 3. Bootstrapping

Bootstrapping is automatic: every `serve` start ensures a single **root** administrator from
`CAIRN_ROOT_ACCESS_KEY` / `CAIRN_ROOT_SECRET_KEY` (default `cairn` / `cairnadmin`) exists, so a fresh
node is usable immediately. There is exactly one default admin; create further users from the console
or `cairn remote user create`.

`cairn bootstrap` is an optional convenience that ensures that same root admin and prints its
credentials:

```sh
cairn bootstrap        # ensures the root admin and prints its credentials
```

It is **idempotent** — it seeds the same `root` identity `serve` would, so running it before `serve`
(or repeatedly) never produces a second default admin. Set `CAIRN_ROOT_ACCESS_KEY` /
`CAIRN_ROOT_SECRET_KEY` before exposing a node; the stored form is only the Bearer hash and the
encrypted SigV4 secret.

A user can be permanently deleted (console, `cairn remote user rm <id>`, or `DELETE
/api/v1/users/{id}`), which revokes all of its access immediately. The root administrator, the last
administrator, the signed-in user, and any user that still owns buckets are refused. Deleting a user
leaves objects it had uploaded into other owners' buckets in place (their owner becomes a historical
id); only its credentials, sessions, and identity policy are removed.

## 4. Deployment shapes

Two first-class shapes:

1. **Standalone with built-in TLS.** Set `CAIRN_TLS_CERT_PATH`/`CAIRN_TLS_KEY_PATH`; Cairn
   terminates TLS itself using rustls (aws-lc-rs).
2. **Behind a terminating reverse proxy** on a trusted interface. The proxy must pass the
   authorization, range, conditional, and S3-specific headers through unchanged and must
   **stream** rather than buffer large bodies (otherwise Cairn's backpressure is defeated). Set
   `CAIRN_PUBLIC_BASE_URL` for correct generated URLs behind ingress.

Never expose the plaintext interface to an untrusted network.

Cairn binds **two listeners**: the S3 data plane (`CAIRN_LISTEN_ADDR`, default `:7373`) and the
web console + management API (`CAIRN_UI_ADDR`, default `:7374`). Expose the S3 port to clients;
keep the console/management port on a trusted interface, firewalled off, or disabled entirely with
`CAIRN_UI_ADDR=off`.

The release artifact is one fully static binary (`musl`) containing the server, the management
UI, and the CLI; it runs in a `scratch`/distroless container.

## 5. Day-two operations

Liveness `/healthz`, readiness `/readyz` (ready only after migrations + reconciliation),
Prometheus metrics `/metrics`. Signals to watch:

- **write-queue depth** — the single-writer ceiling; growth under load means small-object writes
  are the binding constraint (enlarge the group-commit linger, relax the synchronous setting if
  the durability trade is acceptable, or scale out with replication).
- **WAL size** — a log growing without bound indicates long-lived readers starving the
  checkpointer.
- **reconciliation counts** at startup — integrity.
- **out-of-space (507) rate** vs capacity.
- **replication lag and failures** — the health of redundancy.

On **shutdown** (SIGTERM), the server drains in-flight HTTP requests within the grace period; the
replication workers stop *claiming* new outbox work but do not block shutdown waiting for in-flight
transfers to finish. This is safe, not lossy: a claimed-but-unfinished entry is leased in the durable
outbox, and on restart the node releases its own stale claims back to pending and resumes them — so a
sudden stop loses no replication work (it ships when the node is back). Drain to a peer (watch
replication lag) before a planned stop if you want the peer fully current. See
[`upgrade-rollback.md`](./upgrade-rollback.md) for the upgrade/rollback procedure,
[`scaling-limits.md`](./scaling-limits.md) for capacity planning, and
[`troubleshooting.md`](./troubleshooting.md) for symptom→fix.

## 6. Durability guarantee

Cairn guarantees that after any crash it converges, with no manual intervention, to a state
where every visible metadata row references a present, durable blob and no orphan remains. The
commit sequence is: stream to staging → fsync the file → rename into place → **fsync the
directory** → validate hashes → commit the metadata transaction (the single linearization
point) → reclaim superseded blobs. A write is acknowledged only after its metadata commit is
durable. Drive-failure survival is delegated to the storage layer; host-failure survival comes
from bucket replication.

See [`backup-restore.md`](./backup-restore.md) for the backup procedure and its consistency
argument.

## 7. Master-key rotation

Secrets at rest — per-object data keys (SSE-S3, `aws:kms`, and transparent at-rest), users' SigV4
secrets, and replication-target secrets — are sealed under a 32-byte master key. A single key is fine for most deployments
(`CAIRN_MASTER_KEY`, 64 hex chars). To rotate the master key without re-encrypting object data,
use a **key ring** and follow this procedure. The whole flow is online; no downtime and no
re-upload of objects.

**Config.** A ring is a JSON array set in `CAIRN_MASTER_KEY_RING`, replacing `CAIRN_MASTER_KEY`:

```
CAIRN_MASTER_KEY_RING=[{"id":1,"key":"<64-hex old key>"},{"id":2,"key":"<64-hex new key>"}]
CAIRN_MASTER_KEY_ACTIVE_ID=2          # which id NEW seals use (default: the highest id)
CAIRN_KEY_REWRAP_INTERVAL_SECS=300    # background re-wrap cadence (0 disables); sqlite backend only
CAIRN_KEY_COUNTER_SYNC_SECS=60        # active-key seal-count flush cadence
```

Each `id` is a small integer (1–65535); keys are never logged. New seals always use the active
id; every other id in the ring stays available to **open** existing data, so nothing becomes
unreadable when you add a key.

**Procedure (rotate id 1 → id 2):**

1. **Add the new key, keep the old.** Deploy with the ring above (`id:1` + `id:2`, active `2`).
   New writes seal under id 2; all existing id-1 data still opens. (A single-key deployment that
   has never used a ring is just `[{"id":1,"key":"<current CAIRN_MASTER_KEY>"}]`.)
2. **Let re-wrap run.** A background worker re-seals existing secrets onto the active key,
   resumably and idempotently. It never deletes or rewrites data it cannot open; it only re-seals.
3. **Wait for completion.** Poll the admin endpoint:

   ```
   GET /api/v1/system/crypto-status        # Bearer admin token
   ```

   It reports the active id, the seal count vs the 75%/95% thresholds, per-key state (with a short
   non-reversible key-hash, never key material), per-stream re-wrap completion, and a
   `retire_eligible` flag per key. **Wait until the old key shows `retire_eligible: true`** (every
   stream re-wrapped onto the active key, on every shard, with no failures).
4. **Retire the old key.** Remove `id:1` from `CAIRN_MASTER_KEY_RING` and redeploy.

> **Do not remove a key before it is `retire_eligible`.** Startup enforces a **retire-gate**: if a
> removed key still has data sealed under it, the server **refuses to start** with a diagnostic
> naming the key id and shard, rather than booting into unreadable data. Restore the key to the
> ring, wait for re-wrap to finish, then retire it.

> **In-flight multipart uploads and retirement.** A multipart upload seals each part under the
> master key active when the part was uploaded, and those per-part keys are transient (not covered by
> the re-wrap stream — they are consumed and discarded at completion). If a key is retired while an
> upload started under it is still in flight, that upload's `CompleteMultipartUpload` fails closed
> (the session stays active and can be aborted + retried); no plaintext is exposed and no corrupt
> object is written. The exposure window is bounded by the multipart-session lifetime, so either drain
> in-flight uploads before retiring or accept that any spanning a retirement must be re-run.

**Seal-count bound.** Each key uses fresh random 96-bit GCM nonces; the active key's seal count is
tracked (and survives restarts). At 75% of the safe ceiling the server logs a "rotate soon"
warning; at 95% it refuses *new* seals (opens are never blocked) — rotate before then.

**Backend note.** Automatic re-wrap and the durable seal counter are implemented for the default
`sqlite` backend. The libSQL/Turso backends can rotate and read all data (old keys still open
everything), but do not auto-re-wrap or persist the counter, so the retire-gate and
`retire_eligible` are not available there — keep retired keys in the ring on those backends.

## 8. Server-side object encryption

Object data can be encrypted at rest in several ways; every mode uses a per-object AES-256-GCM data
key (DEK) sealed under the node master key / ring (§7), so all of them are covered by master-key
rotation.

- **Client SSE-S3.** A PUT with `x-amz-server-side-encryption: AES256` stores the object encrypted
  and advertises it back — nothing to configure.
- **Bucket default / mandatory.** Set a bucket's default encryption via the S3 `?encryption`
  subresource (`PutBucketEncryption`); with the management-plane `required` flag a plaintext client
  PUT is refused. Per bucket, not node-wide.
- **`aws:kms` requests.** A PUT with `x-amz-server-side-encryption: aws:kms` (optionally
  `-aws-kms-key-id`) is accepted for SDK compatibility. **The key id is a validated label, not
  distinct key material** — every DEK still seals under the node master key, so this is not
  cryptographic tenant isolation. Constrain which ids clients may name with `CAIRN_KMS_KEY_IDS`
  (comma-separated allow-list; unset accepts any id). The list gates writes only — removing an id
  never locks existing objects.
- **Transparent at-rest.** `CAIRN_ENCRYPT_AT_REST=true` encrypts every committed object even when the
  client requested no SSE, as an operator storage property (no `x-amz-server-side-encryption` is
  advertised). It is a confidentiality/throughput trade — an encrypted object engages neither the
  sendfile nor the small-object GET fast path — so it stays opt-in.

Multipart uploads are encrypted per part: each staged part is written encrypted and assembly
re-encrypts under the object DEK, so nothing plaintext is ever on disk for an SSE upload. An upload
pins its encryption intent at `CreateMultipartUpload`; see §7's retirement note for the fail-closed
window when the master key is retired mid-upload.

### 8.7 Repairing encrypted objects that replicated wrongly (the plaintext-seam incident)

**What happened.** Before release X the replication engine read source blobs with **no data key**
(`docs/replication.md` 20.1). Any version encrypted by SSE-S3, SSE-KMS, or `CAIRN_ENCRYPT_AT_REST`
was therefore shipped to the mirror as raw **ciphertext**. Two outcomes, and they need different
urgency:

- the destination rejected it with `400 BadDigest` **only** when the source object happened to carry
  a supplementary `x-amz-checksum-*` value. That is terminal and never retried, so the object is
  simply **absent** on the mirror. Unprotected, but not misleading;
- otherwise — a multipart-completed object, a `curl` or presigned PUT, an older SDK — the
  destination **accepted** it. The replica exists, is exactly the right size, answers `200`, and is
  **garbage**. Anyone who failed over to the mirror in that window restored garbage and got a `200`
  while doing it.

Upgrading fixes new writes. **It heals nothing already on the mirror.**

#### Detect

`GET /replication/failed` is **not** the ledger and will reassure you falsely: the outbox is pruned
at `CAIRN_REPLICATION_RETENTION_SECS` (default 24 h) and shows only recent failures. The durable
ledger is the version row (`object_versions.replication_status`), which is never pruned. Enumerate
it node-locally:

```sh
# --before is REQUIRED: the moment you deployed the fix on THIS node.
cairn replication audit --before 2026-07-23T10:00:00Z         # or bare epoch seconds
cairn replication audit --before 2026-07-23T10:00:00Z --bucket photos --json
cairn replication audit --before 2026-07-23T10:00:00Z --verify  # EXPENSIVE, see below
```

**Why the cutoff is mandatory.** Only versions written by the *pre-fix* binary can be damaged. A
version encrypted and replicated after the fix is equally encrypted and equally `completed`, and is
perfectly healthy — nothing about the row distinguishes it except when it was created. Audit without
a bound and you are counting your healthy encrypted replicas alongside the damaged ones: the number
never reaches zero, so it tells you nothing about whether the repair worked. Use the deployment
timestamp; err slightly late rather than early (an extra re-ship costs egress, a missed one leaves
garbage on the mirror).

The output separates three populations:

| Count | Meaning | Action |
|---|---|---|
| `present_and_suspect` | On the mirror, may be garbage. **The dangerous one.** | Repair. |
| `absent` | Rejected at the destination, never landed. Unprotected but honest. | Repair (or accept). |
| `repair_pending` | In-window and queued/claimed: **repair in flight**, bytes not yet re-shipped. | Wait. |

Do **not** try to narrow the set by comparing with the remote: the engine requested the *plaintext*
length, so a corrupt replica has exactly the right number of wrong bytes, and a multipart source
lands as a single-part PUT so the ETag differs even for a **correct** replica. `--verify` is the only
conclusive check and it transfers every suspect object in full. `--verify` compares against the
destination's **current** object (its GET carries no `versionId`), so it **skips non-current source
versions** and reports them as `skipped_non_current` — comparing a superseded version against the
destination's current object would report a mismatch for a perfectly healthy mirror. Those versions
are unrepairable here anyway (trap 2 below).

On a dashboard, watch `cairn_replication_encrypted_suspect_versions` **and**
`cairn_replication_encrypted_repair_pending_versions` (plus `…_encrypted_absent_versions` and
`…_encrypted_non_current_suspect_versions`, which is the *floor* of the suspect gauge — see
"Verifying convergence" below). These are
**opt-in**: they are emitted only when `CAIRN_REPLICATION_AUDIT_BEFORE` is set — same value as
`--before` — and the loop does not run at all otherwise. They are recomputed on a **6-hour
background cadence**, not per scrape: there is no index on `object_versions.replication_status`, and
adding one would be a schema migration this remediation deliberately does not take. A pass is
near-free on a store with **no enabled replication rule** (skipped before any version is read) but a
bucket that *does* replicate costs a full version listing **plus one point query per version**, since
the listing page carries no SSE descriptor — on a large replicated bucket that is the whole cost, and
it is the other reason the cadence is slow and the loop opt-in.

#### Decide — the three traps

Each of these silently wastes a repair pass. The audit prints all three; read them.

1. **Resync is gated on `ExistingObjectReplication`.** Most rules do not set it, and a resync against
   such a rule returns success and enqueues **nothing**. Edit the rule first (S3
   `PUT /{bucket}?replication` (PutBucketReplication), or the console's replication editor).
2. **The backfill enumerates CURRENT versions only.** A non-current version that replicated corrupt
   is **not repaired** by any command here — the audit counts these as `non_current_suspect`. Full
   version-history fidelity on the mirror requires rebuilding the destination bucket (delete and
   re-replicate, or re-import). This is stated plainly rather than papered over.
3. **Repair re-ships PLAINTEXT.** If the destination endpoint is `http://`, the confidentiality gate
   refuses every client-encrypted (SSE-S3/SSE-KMS) object and reschedules it *forever* without ever
   consuming the attempt budget — so the repair never fails, it just never happens. Move the endpoint
   to `https://`, or set `CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP=true` and accept sending
   decrypted bodies over an unauthenticated link. Transparent at-rest (`CAIRN_ENCRYPT_AT_REST`)
   objects are **not** gated — that is an operator storage property, not a client contract.

#### Repair — re-ship unconditionally, never diff

```sh
# 0. Record the moment you deploy — that timestamp is the audit cutoff for every step below.
CUTOFF=2026-07-23T10:00:00Z
# 1. Upgrade every node. Source first is fine; the wire format is unchanged, and an in-flight
#    outbox entry carries no encryption state, so it is read by the NEW code at drain time.
# 2. Look.
cairn replication audit --before "$CUTOFF"
# 3. Fix trap 1 (edit the rule) and trap 3 (https:// or the opt-in) if the audit flagged them.
# 4. Recover the recent 'failed' entries the outbox still remembers.
cairn replication retry photos
# 5. Re-ship the corrupt-but-'completed' population. --force is REQUIRED for any pass after the
#    first, and harmless on the first.
cairn replication resync photos --force
# 6. Verify — see below. Done is repair-pending 0 AND suspects down to the non-current floor;
#    neither number alone is a completion signal.
cairn replication audit --before "$CUTOFF"
# 7. Optional hard proof (current versions only).
cairn replication audit --before "$CUTOFF" --verify
```

##### Verifying convergence — read this before watching the numbers

The audit's suspect predicate runs on **two** clocks, and you need both to read the numbers:

* `created_at < cutoff` — only versions written by the pre-fix binary can be damaged;
* `replicated_at IS NULL OR replicated_at < cutoff` — `replicated_at` (schema **v23**) is stamped by
  the replication engine every time a version ships successfully. It is what makes a **repaired**
  version leave the population. Without it a damaged version would match forever: `created_at` is
  never rewritten, so once the repair re-stamped the row `completed` the count would return to its
  pre-repair value *because the repair worked*.

**Three consequences, all of which will surprise you if you have not read this.**

**1. The suspect count drops sharply the moment the repair is queued, before a single byte
re-ships.** `--force` flips the repairable damaged rows to `pending`, and `pending` is not a suspect
state. If you watch `present_and_suspect` alone you will see it fall immediately — which is not
success — and then you will see `repair_pending` carry the real progress.

So the sequence to expect is:

1. before the repair: `present_and_suspect` high, `repair_pending` 0;
2. immediately after `--force`: `present_and_suspect` down to `non_current_suspect` (see below),
   `repair_pending` high — nothing has shipped yet;
3. as the queue drains: `repair_pending` falls, and each entry that completes gets a fresh
   `replicated_at` *after* the cutoff — so it leaves the population entirely instead of returning to
   `present_and_suspect`. Both numbers therefore fall and stay down;
4. **done when `repair_pending == 0` AND `present_and_suspect == non_current_suspect`.** Neither
   number alone is a completion signal, and on a bucket with non-current suspects `0 / 0` is *not*
   the end state — see the next point. `repair_pending` really does reach 0: a version is only ever
   moved to `pending` when something can actually ship it. That is the difference `replicated_at`
   and the requeue's ledger scope make together.

**2. `present_and_suspect` floors at `non_current_suspect`, and that floor is correct.** A forced
requeue moves a version row to `pending` — i.e. into `repair_pending`, "repair in flight" — only when
something will genuinely ship it: the version is **current** (the resync backfill that follows
enumerates current objects, so it gets a fresh queue entry), or it still has an outbox row (the
requeue just put that row back to `pending`).

A **non-current** version whose outbox row the retention sweep already pruned — which, 24 h after the
incident, is essentially all of them — has neither. Nothing enqueues it, so if it were marked
`pending` it would sit there forever: the ledger would claim queued work that no queue holds,
`repair_pending` could never reach 0, and the alert below would fire permanently on a node whose
repair had in fact finished. So it stays counted in `present_and_suspect` and in
`non_current_suspect`, which is the truth — it is still on the mirror and still wrong, and **no
command in this runbook repairs it** (trap 2). Getting it off the mirror means rebuilding the
destination bucket: delete and re-replicate, or re-import.

`repair_pending` can also stall above zero for two *operator-fixable* reasons, and both are printed
by the audit: **trap 1** (no enabled rule sets `ExistingObjectReplication`, so the backfill enqueues
nothing for the current versions the requeue just marked `pending`) and **trap 3** (an `http://`
destination with client-encrypted objects, whose entries are rescheduled forever and never fail).
Fix the trap and re-run the resync; the number then drains.

**3. On a node upgraded mid-incident, the FIRST audit over-reports.** `replicated_at` is `NULL` for
every row that existed before the v23 migration — including versions that replicated perfectly well
long ago — and a `NULL` stamp is counted as suspect. So the first audit after the upgrade will size
the damage *too large*, and the excess resolves itself as those versions are re-shipped and stamped.

This is deliberate, and it is the right direction to be wrong in: an over-report costs a wasted
re-ship, an under-report leaves a garbage replica on the mirror that nobody ever goes looking for. Do
not try to "fix" the first number by narrowing the cutoff — you would be hiding real damage to make a
dashboard look tidy. Let it re-ship.

Alert on
`cairn_replication_encrypted_suspect_versions - cairn_replication_encrypted_non_current_suspect_versions > 0`
**or** `cairn_replication_encrypted_repair_pending_versions > 0`, and only once
`CAIRN_REPLICATION_AUDIT_BEFORE` is set — an unbounded audit cannot converge, so an alert on it would
fire forever on a healthy node.

Subtracting the non-current floor is not cosmetic: those versions are unrepairable by any command
here, so a bare `suspect > 0` alert on a bucket that has them is a permanently-firing alarm, exactly
what the cutoff exists to avoid. Track `…_non_current_suspect_versions` on its own — it should be
flat, and it going *up* means new damage, not leftover damage. If you have decided to rebuild the
destination bucket, it drops to 0 when you do and the subtraction becomes a no-op.

`--force` submits `RequeueReplicationVersions`, which flips the bucket's terminal (`completed` /
`failed`) outbox rows back to `pending` with the attempt budget reset, and resets the version-row
ledger stamps of everything that will actually be re-shipped. Without it, a **second** resync inside
the retention window is a silent no-op: the
backfill's enqueue is `INSERT OR IGNORE` on a deterministic `backfill:{rule}:{key}:{version}` id, and
that id still has a row. Add `--all-versions` to widen the requeue beyond encrypted keys (it requires
`--force`, and is rejected without it rather than silently ignored).

The requeue is scoped to the **key**, not the individual version: every terminal entry of any key
that has at least one encrypted version is re-queued, including that key's later plaintext versions
and delete markers. That is deliberate and load-bearing — re-shipping only the encrypted version
would PUT it *after* newer versions already at the destination, reverting the mirror's current object
to an old one, or resurrecting an object whose delete marker was never re-shipped.

The two halves have **deliberately different scopes**, and the asymmetry is the accurate rule rather
than an optimisation. The *outbox* half moves every terminal row of every key in scope. The *ledger*
half is narrower: a version row goes back to `pending` only if it is the **current** version (the
backfill will re-enqueue it) or it still has an outbox row (now requeued). `pending` in the ledger is
a claim that something will ship this version, and for a non-current version whose outbox row was
pruned nothing ever will — writing `pending` there would be the ledger lying, and the lie is not
harmless: it is what would leave `repair_pending` permanently above zero and the alarm permanently
firing. Those versions stay counted as suspects (trap 2), which is what they are.

The requeue is **paged by key**, not by row, and the page unit is a correctness property rather than
a tuning knob: every terminal row of a key is requeued in the same transaction as its siblings, and
each pass resumes strictly past the previous page's last key. A row-bounded page would serve rows in
index order — every `completed` row ahead of every `failed` one — and could requeue a key's newer
version pages before its older one, letting the 30-second heartbeat ship them out of order. The one
unbounded case is a single key with an enormous version history: its rows are never split, because
splitting them is the bug.

> **A forced resync on a very large bucket is write-heavy.** It rewrites every terminal outbox row
> and every terminal version-row stamp for the keys in scope, through the single metadata writer.
> The work is paged (a bounded number of keys per transaction, resuming forward until the bucket's
> keyspace is drained) precisely so it does not hold one long transaction and stall every other write
> on the node — but the total write volume is proportional to the bucket, the WAL will grow during
> the pass, and write latency will be elevated while it runs. Do it in a maintenance window on a
> bucket with millions of versions, one bucket at a time.
>
> If the requeue ever exhausts its batch budget it logs **`requeue DID NOT CONVERGE`** with a
> `resume_after` key and records a `ResyncReplicationTruncated` entry in the activity feed; if it
> errors part-way it logs **`requeue FAILED part-way`** and records `ResyncReplicationFailed`, with
> the same cursor. Either way the repair is INCOMPLETE — keys at or past that cursor were not
> requeued. Re-run the resync. (The HTTP response cannot carry this: the requeue runs after the
> request has already been answered `202 Accepted`, which is why both land in the activity feed
> rather than only in the log.)

Expect a **drain surge and destination egress** on the first pass: buckets that were silently not
replicating will start.

**Rolling back to a pre-fix binary silently resumes the corruption.** This upgrade is one-way in
that sense; nothing needs draining before it.

**Standing rule this incident bought** (see also `docs/testing-performance.md` 29): resolve a DEK
from the row you were handed, per read, and never cache one across passes — the master-key re-wrap
worker re-seals descriptors underneath a live consumer.

#### Release-note text (paste verbatim)

> **Encrypted objects did not replicate correctly before this release.** The replication worker read
> source blobs without their data key, so any object encrypted by SSE-S3, SSE-KMS, or
> `CAIRN_ENCRYPT_AT_REST` was shipped to the mirror as raw ciphertext. Where the source object
> carried a supplementary checksum the destination refused it and the object is simply **missing**
> on the mirror. Where it did not — every multipart-completed object, and any single-part object PUT
> without an `x-amz-checksum-*` header (`curl`, presigned PUT, older SDKs) — the destination
> **accepted** it: the replica exists, is exactly the right size, and answers `200` with garbage.
> **Anyone who failed over to a mirror in this window restored garbage for those objects, and got a
> 200 while doing it.**
>
> Upgrading fixes all new replication. It does **not** heal what is already on the mirror. Run
> `cairn replication audit --before <the moment you upgraded this node>` on each source node to size
> the damage (it reads the durable version-row ledger; the failed-entry API only covers the last
> `CAIRN_REPLICATION_RETENTION_SECS`; the cutoff is required because a version encrypted and
> replicated *after* the fix is indistinguishable from a damaged one by anything but its age), then follow
> the repair runbook in `docs/operations.md` 8.7. Three things will silently make a repair do
> nothing, and the audit flags all three: a rule without `ExistingObjectReplication`, a non-current
> version (the backfill only covers current versions), and an `http://` destination (repair ships
> plaintext and the confidentiality gate refuses it). Rolling back to a pre-fix binary resumes the
> corruption.
