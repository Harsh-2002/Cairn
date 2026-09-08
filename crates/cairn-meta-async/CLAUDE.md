# cairn-meta-async

An **async** `MetadataStore` (beta) over two embedded SQLite-compatible engines — **libSQL** and
the pure-Rust **Turso** — behind one driver seam. A parallel, additive backend that reproduces
`cairn-meta`'s behaviour **exactly**: same migrations, same `Mutation`->SQL `apply`, same listing
range-seek, same outcomes. `cairn-meta` is left untouched. Selected at runtime by
`CAIRN_META_BACKEND=libsql|turso` (default is the sqlite `cairn-meta`).

## Layout (`src/`)
- `driver.rs` — the `AsyncSqlDriver` seam: `Value`/`Row` cell model + parameterized
  `execute`/`query`/`execute_batch` and txn-control verbs. All apply/store/writer logic is written
  against this trait, engine-agnostic.
- `libsql_driver.rs` / `turso_driver.rs` — the two concrete drivers behind that seam.
- `apply.rs` — `Mutation` -> SQL. **One of the four mutation sites** (see below).
- `storage.rs` — protocol-2 Writer operations: exact admission/publication ownership, cancellation,
  quiescence resolution, bounded intent recovery and leased cleanup. Validate the normalized
  path/upload/reservation index against each persisted plan; mirror SQL and typed outcomes across
  both metadata crates and the in-memory double.
- `schema.rs` — the migration table. Must mirror `cairn-meta/src/schema.rs` (latest is v38 — the
  multipart SSE columns `sse_requested` v15, `encrypt_parts`/`part_dek` v21, `sse_kms_*` v22;
  `object_versions.replicated_at` + `idx_outbox_bucket_key` v23; bounded import
  scheduling/history/retention indexes v24; hash-only object-share capabilities and retryable
  legacy-token sanitation v25; bounded multipart reservations/cleanup accounting v26; multipart
  initial tags/Object Lock intent, the legacy-intent proof marker, and orphan-lock cleanup v27;
  lifecycle row identity in the partial current-listing covering index v28; exact multipart
  completion claim ownership tokens v29).
  Turso
  builders deliberately enable its experimental VACUUM support
  so a pending v25 sanitation marker can compact the local database before readers open.
- `store.rs` — `AsyncMetadataStore` (reads) + `AsyncReconcileOracle`; the read pool. Its
  `read_probe` executes a constant-row query through a checked-out driver connection, matching the
  default SQLite backend without enumerating application rows.
- `writer.rs` — the single async group-committing `Writer` task.
- `model.rs` — `Row`<->domain mappers + the `*_COLS` column lists; enum<->text strings.
- `range.rs` — listing range-seek helpers (`successor`, `prefix_upper_bound`).
- `lib.rs` — `open_libsql`/`open_turso` (+ `_in_memory`), `OpenOptions`, per-engine pragmas.

## Invariants & rules
- **Parity is the contract.** This crate must be byte-for-byte behaviour-identical to `cairn-meta`
  — same SQL, preconditions, savepoint semantics, JSON/enum encodings, list pagination, outcomes.
  Any divergence is a bug. `tests/contract.rs` (libSQL) and `tests/turso_contract.rs` (Turso) run
  both backends side-by-side against the rusqlite store and assert this; keep them green.
- **The 4(+1)-site rule.** A new `Mutation`/shared read lands in `cairn-meta/src/apply.rs` **and**
  here in `apply.rs` (plus the `cairn-types` in-memory double). This is the "+1": forget it and the
  parity tests fail.
- Object Lock is writer-authoritative in both async engines exactly as in `cairn-meta`: strict
  persisted-state parsing, immutable enablement/versioning, protected replacement/delete, retention
  non-weakening, and atomic object/tags/lock/outbox/session updates must not diverge.
- Lifecycle guard parity is exact: conditional `CreateDeleteMarker` checks the enumerated
  current version/timestamp before marker or outbox writes, and `DeleteVersion` checks the immutable
  listed row identity and timestamp and can require the target still to be the sole/latest delete
  marker. An absent or stale predicate returns `DeleteNotApplied`; `DeleteMarker`/`Deleted` are
  reserved for rows actually changed by the writer.
- Protocol-2 physical PUT, part and completion writes require `PublishStorageWrite` over their
  exact admitted plan. Ordinary admission precedes staging; multipart admission joins the original
  quota reserve or completion claim in one savepoint. Publication rechecks generation, intent,
  cancellation and target before the original preconditions/side effects. Reject bare physical
  publications; a missing owner returns `StoragePublicationNotApplied`.
- `ResolveObjectWrite` and `ResolveMultipartPartWrite` retain exact FIFO identity probes for
  acknowledgement ambiguity. A miss alone cannot authorize unlink or quota release. Actual
  quiescence must be proven before storage-intent resolution, and physical reclamation requires
  an exact leased Writer cleanup claim. Replacement/deletion records cleanup debt atomically with
  reference removal; a returned superseded path is not cleanup authority.
- Multipart terminal ownership is part of parity, not an implementation detail: Complete claims
  `active -> completing` under an exact persisted request token, Abort deletes only `active`, a
  failed claim releases only its own token, and final completion rechecks both `completing` and
  that token inside its savepoint before any object upsert.
- **Migrations are append-only and version-aligned.** Never edit an applied migration. Mirror new
  `cairn-meta` migrations here verbatim, **keeping the same version numbers** — versions **13 and
  14 are intentionally absent** (they are the #29 key-rotation schema the async backend does not
  implement); the runner applies any version > current max, so the v12->v15 gap is correct. Don't
  renumber to close it.
- **All writes go through the single `Writer`** task (group-commit, one savepoint per mutation, one
  commit = one durability barrier). A failing mutation rolls back its own savepoint; a failed
  savepoint operation aborts the entire batch because isolation is no longer established. Preserve
  typed `MetaError::OutOfSpace` through batch fan-out and secondary rollback errors. A capacity
  failure is not proof of nonpublication; retain the normal exact-identity ambiguity resolution.
  Never open an ad-hoc write connection.
- **Reads check out a connection exclusively.** A single libSQL/Turso connection cannot serve two
  concurrent reads — interleaved cursors return wrong/leaked rows (audit #8). The `ReadGuard` holds
  one pooled connection under its lock for the whole (possibly multi-query) read.
- **Positional columns, not by name.** The async drivers yield positional cells, so reads select a
  fixed `*_COLS` list and index it. Reorder/extend a `*_COLS` and you must update its mapper.

## Contract / dependencies
- Depends only on `cairn-types` (the trait spine). `cairn-meta` is a **dev-dependency only**, used
  by the parity tests — never depend on it at runtime.
- Compiled into the binary only under the server's `meta-async` cargo feature; `stack.rs` dispatches
  on `CAIRN_META_BACKEND` and errors if the feature is absent.

## Notes
- **glibc-only**: excluded from the static musl build (the bundled C deps SIGSEGV under static musl).
- **Turso is best-effort on pragmas.** The beta engine doesn't honour the full PRAGMA surface, so
  `apply_turso_pragmas` ignores individual failures. Turso self-manages its native WAL: no external
  checkpointer, and `wal_autocheckpoint`/`journal_size_limit` are deliberately unset (the W3
  guardrail doesn't apply). libSQL mirrors the rusqlite store: `wal_autocheckpoint=0` + a background
  checkpointer.
- Auto re-wrap, durable id→hash binding/retirement gate, and the durable seal counter (#29) are
  **not** implemented — rotate-and-read only. The SQLite six-stream registry must not be presented
  as an async-backend retirement guarantee; operators keep every historical key id in the ring.
- `OpenOptions` mirrors `cairn-meta`: WAL + `synchronous=NORMAL` by default, `FULL` opt-in.
- Spec: `docs/metadata.md` (11), concurrency model `docs/data-plane.md` (7.2/7.3). See the root
  `../../CLAUDE.md` for the workspace gate and conventions.

Replication outbox migration v30 adds exact attempt ownership. Done/fail/defer/renew validate the
claimed status, token, and lease inside the writer savepoint; every backend and shard fan-out must
preserve the typed applied result. Recovery invalidates tokens before new workers can claim.

Migration v31 adds nullable multipart `replica_intent` (source identity, response headers and expected
whole-object checksums). Decode it strictly and preserve it through claim/release/restart recovery;
ordinary and legacy uploads have no replica capability.

- The append-only v32 internal-integrity migration adds nullable `object_versions.internal_sha256`.
  Preserve it on every object read/write and snapshot; legacy NULL is intentionally not backfilled.
Schema v33 gives remote multipart uploads a separate durable journal with no bucket/outbox cascading foreign
keys. Persist before initiation and before parts; retain missing receipt incidents and accept late
receipts while rejecting further data I/O after ownership loss. Existing workers claim cleanup
independently using exact renewed leases; saved endpoint/bucket identity must match current routing.

Schema v34 adds writer-maintained current-visible `bucket_stats.objects`; overview counts read
these roll-ups without scanning object versions. Migration backfills existing databases once.

Schema v35 adds `storage_protocol` compatibility state (reader/writer 1, flat placement,
full-scan recovery). Startup validates schema/protocol support before PRAGMAs, migrations,
sanitation or Writer startup; direct migration calls repeat the guard. Older released binaries
without this check remain unsafe downgrade targets. Restore from a verified pre-upgrade snapshot.

Schema v36 adds exact storage intents/paths/cleanup and a process-generation/coverage singleton,
raises reader/writer protocol floors to 2, and preserves flat placement/full startup scans.
Schema v37 adds indexed upload/reservation identity and `quota_owner_path` so unfinished multipart
aliases follow their final part's sole v26 charge after replacement or terminal removal. All linked
cleanup rows must retire, and no intent may protect the charged path, before that charge is freed.
Attaching new quota ownership to a claimed alias invalidates its entire claim tuple immediately;
stale acknowledgements remain rejected without forcing retry to wait for the old lease. Repeating
the same quota attachment preserves its current claim. Mirror this in every implementation.
Legacy cleanup release mutations exclude protocol-2 debts; existing protocol-1 charges are not
forgiven by migration.

Cancellation never proves I/O quiescence. Cleanup claims exclude live objects/parts and outstanding
intents; settlement requires the exact id/path/bucket/token/generation/unexpired lease/debt identity
after durable physical absence. Per-bucket operations retain routing after bucket/session deletion;
generation changes reach every shard and global claims remain bounded. SQLite, libSQL, Turso and
the in-memory double preserve these savepoint and typed outcome semantics. Coverage remains
incomplete and startup retains full scans; phase gates remain tracked in
`docs/storage-evolution-plan.md`.

`PrepareStorageRestore` is an exclusive staged-image Writer operation. It requires
a fresh generation, atomically clears coverage identity/completion and cleanup claims, and
preserves old intent generations, authoritative references, cleanup debts and multipart charges.
Fan it out to every physical shard. It neither marks coverage complete nor enables journal
recovery. Restore clears release authorization and preserves a copied baseline HOLD and run id.

Schema v38 adds a global legacy-accounting HOLD with exact generation/run authorization. The
baseline first holds every physical shard; ordinary startup refuses any held shard before recovery.
The four legacy release helpers enforce HOLD internally, and shard routing checks every state
before its first release dispatch. Native exact cleanup remains valid; new storage admission does
not. Bounded classification records durable exact debt before deletion. Authorization checks all
intent/path/cleanup rows and all native quota debts with unconditional EXISTS; claimed or protected
work still blocks it. Only a completed blob proof authorizes bounded legacy release while HOLD
remains set. Completion requires all reservations and cleanup charges gone, while live part charges
remain intact. A generation change clears authorization; restore also clears coverage and claims.
Both preserve HOLD. Full startup scans and flat protocol-2 writes remain mandatory.
`storage_baseline_states`, pending work, exact path owners, and authority pages are uncached safety
reads. Authority pages cover every historical object row and live part, including orphan-parent
errors. Path-owner reads fan out across all shards and retain deleted bucket/upload attribution;
only genuinely ownerless staging paths use the stable `cairn-storage-orphans` route.
