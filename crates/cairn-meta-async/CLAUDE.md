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
- `schema.rs` — the migration table. Must mirror `cairn-meta/src/schema.rs`.
- `store.rs` — `AsyncMetadataStore` (reads) + `AsyncReconcileOracle`; the read pool.
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
- **Migrations are append-only and version-aligned.** Never edit an applied migration. Mirror new
  `cairn-meta` migrations here verbatim, **keeping the same version numbers** — versions **13 and
  14 are intentionally absent** (they are the #29 key-rotation schema the async backend does not
  implement); the runner applies any version > current max, so the v12->v15 gap is correct. Don't
  renumber to close it.
- **All writes go through the single `Writer`** task (group-commit, one savepoint per mutation, one
  commit = one durability barrier). A failing mutation rolls back only its own savepoint. Never
  open an ad-hoc write connection.
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
- Auto re-wrap and the durable seal counter (#29) are **not** implemented — rotate-and-read only.
- `OpenOptions` mirrors `cairn-meta`: WAL + `synchronous=NORMAL` by default, `FULL` opt-in.
- Spec: `docs/metadata.md` (11), concurrency model `docs/data-plane.md` (7.2/7.3). See the root
  `../../CLAUDE.md` for the workspace gate and conventions.
