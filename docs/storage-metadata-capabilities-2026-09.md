# Metadata engine capabilities and adapter gaps

Research date: 2026-09-09. This is the Phase 5B capability comparison, not an engine benchmark,
production replacement proposal, or evidence of a SQLite bottleneck. The workload and attribution
requirements remain in [the capacity evaluation](storage-metadata-capacity-2026-09.md) and
[Phase 5A–5C](storage-evolution-plan.md). The completed capacity result now qualifies the one-hot-bucket
trace for [the bounded transactional Fjall experiment](storage-metadata-alternative-2026-09.md).
Its isolated standalone-workspace dependency and adapter do not change the production backend.
RocksDB remains research-only.

Claims below distinguish upstream primitives from Cairn adapter work. A documented transaction
or fsync primitive does not establish this application's error handling, physical-write ordering,
backup completeness, or power-loss test coverage. Hardware and filesystem flush correctness are
necessary for every engine. The capability research itself installed no dependencies or engines.
The subsequently authorized, separate Fjall laboratory pins its own dependency and records its
correctness and measurement evidence in the conditional comparison report.

## Source and version identity

| Engine | Version examined | Relationship to Cairn |
|---|---|---|
| SQLite | Current production lock: `rusqlite 0.32.1`, `libsqlite3-sys 0.30.1`; the corresponding bundled [header identifies SQLite 3.46.0](https://raw.githubusercontent.com/rusqlite/rusqlite/v0.32.1/libsqlite3-sys/sqlite3/sqlite3.h). | Canonical production Writer/read-pool implementation; not a claim that 3.46.0 is current upstream SQLite. A measured binary must also record its runtime SQLite version/source ID. |
| libSQL | Cairn locks `0.10.0-pre.4`, `default-features=false`, `core`; upstream [version listing](https://docs.rs/crate/libsql/0.10.0-pre.4) also distinguishes this prerelease from stable `0.9.30`. | Existing optional embedded backend. libSQL is the SQLite fork, not the Rust rewrite. |
| Turso Database | Cairn locks `turso`/`turso_core 0.6.1`; current stable Rust API examined is [0.7.2](https://docs.rs/turso/0.7.2/turso/), with `0.8.0-pre.7` also listed [upstream](https://docs.rs/crate/turso/0.7.2). | Existing optional beta backend. Current documentation is not retrospective proof of every 0.6.1 guarantee. Cloud Turso/remote replicas are outside this comparison. |
| redb | [4.2.0 manifest](https://raw.githubusercontent.com/cberner/redb/v4.2.0/Cargo.toml). | Research only; no Cairn adapter or on-disk migration exists. |
| Fjall | **Exact conditional candidate: `fjall = "=3.1.10"`**, using `SingleWriterTxDatabase`; [versioned API](https://docs.rs/fjall/3.1.10/fjall/struct.SingleWriterTxDatabase.html) and [manifest](https://raw.githubusercontent.com/fjall-rs/fjall/3.1.10/Cargo.toml). | The Phase 5A hot-bucket predicate passed. The isolated adapter and pinned dependency live only in the standalone laboratory workspace. |
| RocksDB | [RocksDB 11.8.1](https://github.com/facebook/rocksdb/releases/tag/v11.8.1); [Rust wrapper 0.25.0](https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/v0.25.0/Cargo.toml) binds [librocksdb-sys 0.19.0+11.8.1](https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/v0.25.0/librocksdb-sys/Cargo.toml). | Research only, including its transactional APIs. |

Local versions and features come from [Cargo.lock](../Cargo.lock),
[the canonical crate](../crates/cairn-meta/Cargo.toml), and
[the optional backends](../crates/cairn-meta-async/Cargo.toml). Versioned sources are preferred;
unversioned official SQLite pages and RocksDB wiki pages are the documentation observed on the
research date, not frozen release specifications.

## Commit durability and conditional atomicity

| Engine | Required durable acknowledgment | Atomic condition + rows + accounting + outbox | Concurrency and important gaps |
|---|---|---|---|
| SQLite | WAL with verified `synchronous=FULL`; NORMAL can lose acknowledged transactions on power loss. [PRAGMA guarantees](https://www.sqlite.org/pragma.html#pragma_synchronous). | Cairn already evaluates conditions and updates all affected relations inside a Writer savepoint, within the group transaction. SQL [savepoints](https://www.sqlite.org/lang_savepoint.html) support mutation-local rollback; success replies still wait for outer COMMIT. | One writer, concurrent snapshot readers. Long readers can delay checkpoint completion. [WAL concurrency](https://www.sqlite.org/wal.html#concurrency). The capacity test must measure this actual Writer, not infer saturation from the engine model. |
| libSQL | Embedded local WAL/FULL must be applied and verified; no durability equivalence is claimed for remote or replica configurations. | Existing SQL adapter preserves the mutation model. The Rust interface exposes [explicit transaction behavior](https://docs.rs/libsql/0.9.30/libsql/struct.Connection.html#method.transaction_with_behavior); retain Cairn's exact SQL, savepoint boundaries, typed outcomes, and outer commit acknowledgment. | Upstream explicitly retains the single-writer limitation. Async calls do not remove that constraint. libSQL is maintained, while new feature development is concentrated in Turso. [Project distinction](https://github.com/tursodatabase/libsql#libsql). |
| Turso Database | Current docs specify `synchronous=FULL` as per-transaction fsync, with OFF/FULL support. [PRAGMA reference](https://docs.turso.tech/sql-reference/pragmas). Cairn 0.6.1 applies this setting best-effort; it is not a verified equivalent-durability baseline. | SQL compatibility reduces translation work but does not remove version-specific semantic testing. Statement cancellation, savepoint rollback, constraints and commit completion require the same fixture outcomes as SQLite. | Current 0.7.2 compatibility notes describe experimental MVCC sibling-statement completion/durability differences and partially supported PRAGMAs. [Pinned compatibility notes](https://raw.githubusercontent.com/tursodatabase/turso/v0.7.2/COMPAT.md). Concurrent-write claims cannot be transferred to Cairn's current single Writer or treated as proven parity. |
| redb | `Durability::Immediate` guarantees persistence when commit returns. [Durability API](https://docs.rs/redb/4.2.0/redb/enum.Durability.html). Its default durable path uses one fsync and checksummed commit slots; a two-fsync mode also exists. [Commit design](https://raw.githubusercontent.com/cberner/redb/v4.2.0/docs/design.md). | One write transaction can read conditions and update multiple tables atomically. Application code must maintain every secondary index and invariant. Savepoints cannot be created after opening tables in that transaction, so they are not replacements for Cairn's arbitrary per-mutation SQL savepoints. [Write transaction API](https://docs.rs/redb/4.2.0/redb/struct.WriteTransaction.html). | Single active writer; readers proceed concurrently. [Database API](https://docs.rs/redb/4.2.0/redb/struct.Database.html). Replacing SQL may change service demand, but does not itself add writers. The documented one-phase checksum threat assumptions need review before any production proposal. |
| Transactional Fjall | Explicit `write_tx().durability(Some(PersistMode::SyncAll))`, then successful `commit()`. `SyncAll` uses fsync; `Buffer` only reaches OS buffers. [Persistence modes](https://docs.rs/fjall/3.1.10/fjall/enum.PersistMode.html). The source defaults ordinary transactions to Buffer. [Transaction creation](https://raw.githubusercontent.com/fjall-rs/fjall/3.1.10/src/tx/single_writer/mod.rs). | `SingleWriterWriteTx` gives cross-keyspace reads/writes and whole-transaction rollback. Conditions must be read through that transaction, with all related keys committed together. [Transaction API](https://docs.rs/fjall/3.1.10/fjall/struct.SingleWriterWriteTx.html). A plain write batch cannot protect separately read preconditions. | The chosen path serializes writers and has no public nested-savepoint API in the inspected version. Fjall also exposes a distinct optimistic transaction API; selecting it would require conflict/retry/phantom analysis and a separate controlled comparison. [Available APIs](https://docs.rs/fjall/3.1.10/fjall/). |
| Transactional RocksDB | WAL enabled, `WriteOptions.sync=true`; asynchronous or WAL-disabled writes are not equivalent. The filesystem policy also determines whether `use_fsync=true` is needed. [Write durability](https://github.com/facebook/rocksdb/wiki/Basic-Operations#synchronous-writes). | `TransactionDB` or `OptimisticTransactionDB`, not plain `WriteBatch`. `GetForUpdate` establishes read preconditions; ordinary Get does not. Transactions support savepoints. [Transaction semantics](https://github.com/facebook/rocksdb/wiki/Transactions). | Concurrent callers are supported, but conflict keys, predicate protection, snapshot selection, retries, and background compaction remain adapter concerns. Ordered iteration alone does not establish a range predicate's conflict protection. No throughput prediction follows from concurrency support. |

All three KV choices can represent a jointly committed quota/outbox change. None implements
Cairn's quota, ownership, Object Lock, or outbox semantics automatically. In particular, an engine
commit error is not evidence that a retry is safe: uncertain outcomes must retain exact mutation
identity and be resolved without releasing physical ownership early.

The current Turso setting caveat is visible in
[`apply_turso_pragmas`](../crates/cairn-meta-async/src/lib.rs): synchronous errors are deliberately
ignored. This comparison does not change that adapter or claim a reproduced durability failure.

## Ordered reads, history and the adapter workload

| Surface | SQLite / embedded libSQL / Turso | redb / Fjall / RocksDB |
|---|---|---|
| Current GET and conditional publication | Existing SQL relations and current-row indexes; Turso still needs pinned-version query/constraint parity. | Separate current-key mapping and immutable logical version records, with expected-row checks inside the write transaction. |
| Prefix and delimiter LIST | Existing SQL prefix ranges and covering indexes; retain current filtering, delimiter grouping and bounded continuation rules. | Ordered range APIs provide the scan primitive: [redb range](https://docs.rs/redb/4.2.0/redb/trait.ReadableTable.html#tymethod.range), [Fjall range/prefix](https://docs.rs/fjall/3.1.10/fjall/), [RocksDB iteration](https://github.com/facebook/rocksdb/wiki/Basic-Operations#iteration). Byte ordering, prefix upper bounds, continuation seek, marker exclusion and delimiter skipping remain application work. |
| Version listing and retention | Explicit version rows and lock/tag side rows, independent of the engine's read snapshot lifetime. | Explicit historical records are still mandatory. Engine MVCC snapshots do not replace durable S3 history; compaction may discard obsolete engine versions once snapshots end. |
| Scheduled work and accounting | Existing outbox, cleanup, multipart, lifecycle and roll-up indexes. | Maintain due-time/status, bucket/key, upload/part, exact-token and reverse-reference indexes transactionally. No whole-keyspace filtering or heap inventory may replace bounded indexed pages. |
| Consistent multi-key reads | One SQL read transaction where the operation requires one view. | One engine read transaction/snapshot over every relevant table/keyspace/column family. Holding snapshots indefinitely can retain pages or versions and distort space and latency results. |

These are adapter requirements derived from [ARCH 11](metadata.md), rather than upstream engine
features. A candidate must use a documented, order-preserving encoding for bucket/key/version
tuples, including zero bytes, prefix boundaries, descending version order and stable tie-breakers.
Length prefixes that reorder keys are not automatically valid. Continuation tokens must preserve
the existing API's behavior; do not add a long-lived snapshot per client token.

A semantics-equivalent measurement includes conditional successes and rejections, retained
versions/markers, multipart admission and exact replacement, quota counters, publication/cleanup
journals, and atomically created and fenced outbox rows. The same bounded operation trace and
independent row/accounting expectations apply to both engines. Index maintenance, serialization,
transaction validation, commit sync, retries and compaction all count. Comparing a KV put against
that complete SQLite mutation would answer a different question.

Group commit is a material gap for redb and Fjall. A proposed adapter must show how a rejected or
partially failed member leaves no effects while successful members still receive the correct
durable outcomes. Precomputing a bounded mutation write set may help, but is not proof of
savepoint-equivalent behavior. If parity cannot be implemented and tested within scope, report
the alternative as unqualified instead of simplifying the SQLite workload.

## Snapshots, restore and format migration

| Engine | Upstream primitive | Cairn consequence / missing guarantee |
|---|---|---|
| SQLite | [Online Backup API](https://www.sqlite.org/backup.html) and [VACUUM INTO](https://www.sqlite.org/lang_vacuum.html#vacuum_with_an_into_clause) produce coherent database images. Interrupted output still requires validation. | Cairn's supported offline backup uses its node lock, VACUUM INTO, referenced blob verification and manifest-last publication. It supports only local SQLite with one shard. [Runbook](backup-restore.md). A database image alone is not an object-store backup. |
| libSQL | Upstream promises standard SQLite files when incompatible extensions are unused. [Compatibility policy](https://github.com/tursodatabase/libsql#compatibility-with-sqlite). | File compatibility is useful, but Cairn's native backup/restore explicitly refuses this backend. Exact checkpoint, image creation, journal state, generation reset and restore validation need integration before claiming parity. |
| Turso Database | The pinned compatibility document lists SQLite file compatibility and VACUUM INTO, with other SQL/PRAGMA limitations. [0.7.2 compatibility](https://raw.githubusercontent.com/tursodatabase/turso/v0.7.2/COMPAT.md). | This does not certify the locked 0.6.1 backend, mixed-engine opens, experimental MVCC state, or Cairn native backup. The supported runbook likewise refuses Turso. |
| redb | Read transactions/savepoints retain a consistent database state; automatic recovery is documented. [Database API](https://docs.rs/redb/4.2.0/redb/struct.Database.html), [savepoints](https://docs.rs/redb/4.2.0/redb/struct.WriteTransaction.html). | A savepoint resides in the database and is not an independent backup. This review found no directly equivalent public online backup/export API in the inspected Database interface. A checked quiescent copy or bounded logical export, independent image verification and fresh restore must be designed and tested. |
| Fjall | Cross-keyspace read snapshots exist. The engine recovers journals and checks its format marker; v2 images require manual migration to v3. [Versioned source](https://raw.githubusercontent.com/fjall-rs/fjall/3.1.10/src/db.rs). | No public manifest-producing physical backup/checkpoint API was found in the inspected database interface. A live directory copy during flush/compaction is unqualified. Any offline approach must explicitly persist, drain all owners/background work, copy the complete recoverable engine state, verify it, and test restore. A snapshot handle is not that protocol. |
| RocksDB | [Checkpoints](https://github.com/facebook/rocksdb/wiki/Checkpoints) produce a consistent directory and may hardlink immutable SSTs. [BackupEngine](https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB) includes required WALs; its `sync` setting controls backup flush durability. | Engine checkpoint hardlinks are not automatically compatible with Cairn's single-link owned namespace. An independent copy/manifest boundary, physical blob references, directory durability, encryption metadata, exact journals and fresh-generation restore still need a Cairn adapter. |

For every new engine, metadata migration is a separate, restartable protocol: a fresh destination,
bounded ordered extraction, schema/codec version identification, full logical row and index
verification, preserved sealed secrets and version identities, atomic completion marker, and an
unmodified verified rollback source. Reopening a different engine on the old directory is not
migration. A native engine snapshot does not supply Cairn's storage compatibility floors,
unpublished-intent recovery, cleanup claim fencing, or accounting reconstruction.

## Build compatibility and maintenance

| Engine | Rust / linkage evidence | Remaining qualification |
|---|---|---|
| Current SQLite | Bundled C via rusqlite; Cairn already builds/tests its default static musl binary. | Preserve actual runtime SQLite identity and dependency update discipline. The static build is an existing project capability, not an assumption that every SQLite build option is equivalent. |
| libSQL / Turso | The libSQL C fork and Turso Rust rewrite are different implementations. Cairn's combined optional crate is currently glibc-only and excluded from musl CI. [Local backend constraints](../crates/cairn-meta-async/CLAUDE.md), [CI matrix](../.github/workflows/ci.yml). | Do not label Turso intrinsically incapable of musl based on the combined crate. Nor does a pure-Rust core establish that Cairn's selected features/dependencies have a qualifying static build. No published MSRV was established from the inspected Turso 0.7.2 [workspace](https://raw.githubusercontent.com/tursodatabase/turso/v0.7.2/Cargo.toml) and [binding](https://raw.githubusercontent.com/tursodatabase/turso/v0.7.2/bindings/rust/Cargo.toml) manifests. |
| redb 4.2.0 | Pure Rust; declared MSRV 1.90 and Rust 2024. [Manifest](https://raw.githubusercontent.com/cberner/redb/v4.2.0/Cargo.toml). The project describes its format as stable and promises reasonable upgrade effort. [Release README](https://raw.githubusercontent.com/cberner/redb/v4.2.0/README.md). | A Cairn musl build was not performed. Format stability is not a SQLite migration path or a guarantee that future redb upgrades are automatic. |
| Fjall 3.1.10 | Rust 2021, MSRV 1.90.0; default compression is Rust `lz4_flex`. [Manifest](https://raw.githubusercontent.com/fjall-rs/fjall/3.1.10/Cargo.toml). Its [v3 changelog](https://github.com/fjall-rs/fjall/blob/3.1.10/CHANGELOG.md) records substantial format/storage changes. | Pure Rust makes it a plausible static candidate, not a tested Cairn build. Lock transitive versions/features; bound cache, memtables, journal and compaction resources. Monitor both Fjall and lsm-tree maintenance. |
| RocksDB 11.8.1 / wrapper 0.25.0 | Wrapper MSRV 1.88; native C++ engine and compression/build dependencies. The wrapper documents Clang/LLVM and a `bindgen-static` musllinux route. [Build instructions](https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/v0.25.0/README.md), [native manifest](https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/v0.25.0/librocksdb-sys/Cargo.toml). | Static linking is possible upstream, but a Cairn target build/runtime test is unperformed. Track wrapper and native-engine releases separately. Its background threads, caches and compactions must fit the same resource limits. |

Cairn declares Rust 1.85 in the workspace. The current laboratory/gate toolchain is Rust 1.97.1,
so the pinned Fjall candidate's MSRV is supported there. That does not establish production
MSRV parity or change the workspace promise. Isolated conditional laboratory dependencies are
within the approved experiment once its evidence condition is met; production adoption still
requires the architectural decision in [CONTRACT.md](../CONTRACT.md).

One upstream maintenance detail must not be confused with engine selection: SQLite documents a
WAL-reset race fixed in 3.51.3 (with selected older backports). Its trigger requires distinct
connections concurrently writing/checkpointing. [Upstream applicability](https://www.sqlite.org/wal.html#the_wal_reset_bug).
Cairn sends mutations and checkpoints through the same Writer connection, with query-only
readers; version 3.46.0 alone therefore does not establish exposure to that trigger. Track
version maintenance separately from the capacity verdict; this research did not reproduce a
failure or authorize a dependency change.

## Pinned conditional Fjall experiment contract

If Phase 5A proves limiting serialized Writer service demand with complete observations, the
first candidate is **Fjall 3.1.10, `SingleWriterTxDatabase`**, on the already used laboratory
toolchain. Keep the one-actor model for an interpretable initial comparison. The exact public
API sequence is `SingleWriterTxDatabase::builder(path).open()`, `keyspace(...)`, then
`write_tx().durability(Some(PersistMode::SyncAll))`, transaction-scoped `Readable` checks,
cross-keyspace updates and `commit()`. These names are from the
[versioned transaction API](https://docs.rs/fjall/3.1.10/fjall/struct.SingleWriterWriteTx.html), not
older Fjall examples using different type names or transaction feature flags. Version 3.1.10's
manifest does not require a `single_writer_tx` Cargo feature.

The pinned commit implementation persists its batch before publishing the sequence to readers,
and poisons the database on journal write/persist failure.
[Batch source](https://raw.githubusercontent.com/fjall-rs/fjall/3.1.10/src/batch/mod.rs).
This supports the intended acknowledgment boundary; it is not a substitute for I/O-error,
interrupted-commit and exact-reopen fixtures. A drop-time best-effort flush must never authorize
success or certify a backup.

Before admitting alternative timings, require all of the following:

1. The same populated logical dataset, outcome distribution, operation caps and invariant checks
   as SQLite, including histories, markers, conditions, multipart, journals and outbox.
2. A complete bounded key/index encoding and mutation adapter, with demonstrated failed-member
   rollback behavior. Successful replies follow durable commit; retries preserve exact identity.
3. Equal application durability and acknowledgment semantics, declared compression/features,
   committed lockfile/toolchain identity, and complete admission/queue/apply/commit observations.
4. Total resource accounting across the actor and engine workers: CPU, RSS, descriptors, caches,
   memtables, journal, live/obsolete SSTs, compaction output and post-drain disk space. Include
   sustained overwrite/delete and cleanup; do not compare only freshly buffered inserts.
5. Checked close/reopen with independently verified state, plus an explicit statement of which
   backup/migration/power-loss guarantees remain untested. Missing evidence cannot be promoted
   to parity by a fast throughput result.

No verified Writer bottleneck, an incomplete alternative adapter, or a nonqualifying gain means
**KEEP SQLite** for this phase. The comparison does not convert bucket distribution into writer
sharding, authorize RocksDB measurements, or waive complete migration/restore parity for a later
production decision.
