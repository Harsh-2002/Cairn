# cairn-meta

The default SQLite `MetadataStore` (the metadata trait's reference impl): one serialized,
**group-committing writer** + a pool of read-only WAL connections + a read-through cache. The
metadata commit is **the single linearization point of every mutation** (ARCH 11).

## Layout (`src/`)
- `lib.rs` — `open`/`open_in_memory` + `OpenOptions`; the per-connection PRAGMAs and the
  `SqliteReconcileOracle` (blob GC's membership oracle). The write conn runs `wal_autocheckpoint=0`
  (the background loop is the *sole* checkpointer) and `synchronous` from `OpenOptions`.
- `writer.rs` — the single `Writer` thread: group-commit, **one savepoint per mutation**, one
  `COMMIT` = one durability barrier. ALL writes go through it; never open an ad-hoc write connection.
  Carries `Control::{Checkpoint,Probe,Exec}` on the same queue — `run_exec` runs the master-key
  re-wrap closure (#29) serialized with mutations. `run_checkpoint` drops the conn's `busy_timeout`
  for the TRUNCATE so a reader-contended checkpoint returns `busy` **immediately** (the background
  loop retries next tick) instead of busy-waiting up to 5s and freezing every PUT/DELETE/CompleteMPU.
- `store.rs` — `SqliteMetadataStore`: the `MetadataStore` trait impl. Writes → `Writer::submit`;
  reads → `with_read` on the blocking pool. Listing is a half-open range seek (`range.rs`).
- `apply.rs` — `Mutation` → SQL. Preconditions are evaluated **here, inside the savepoint**, so
  check-and-upsert is atomic. **Mirror any change in `cairn-meta-async/src/apply.rs`** (4(+1)-site).
- `schema.rs` — migrations: **append-only**, monotonic `version` (latest is 23 — multipart SSE
  columns: `multipart_uploads.sse_requested` v15, `.encrypt_parts` + `multipart_parts.part_dek` v21,
  `.sse_kms_requested`/`sse_kms_key_id`/`sse_bucket_key_enabled` v22; `object_versions.replicated_at`
  + `idx_outbox_bucket_key` v23); never edit an applied
  migration, never reorder — add a new one.
- `model.rs` — SQL row ↔ domain-type conversions; complex fields (compression, ACL, checksums,
  user-metadata) are JSON columns. `engine_err` maps constraint violations → `MetaError::Conflict`.
- `range.rs` — `successor`/`prefix_upper_bound` for the listing range seek (UTF-8 byte order);
  unit-tested against empty/maximal/multibyte — their correctness *is* listing's correctness.
  The scan guards an **empty delimiter** to no-delimiter (`.filter(|d| !d.is_empty())`): `"".find`
  is `Some(0)`, so an unguarded empty delimiter would collapse every key into one common prefix and
  return zero objects — the bug that made warp/minio-go's recursive list (always `delimiter=`) fail.
  Normalise it at the S3 handler too; keep both backends' scans identical (async `contract.rs`).
- `cache.rs` — `CachedMetadataStore`, a decorator that memoises exactly three auth-path reads
  (`get_bucket`, `get_bucket_config`, `get_account_public_access_block`; F-10). Sharded, byte-budgeted,
  caches negatives; `submit` invalidates affected entries (when unsure, the whole bucket).
- `shard.rs` — `ShardedMetadataStore`: bucket-partitioned routing (`CAIRN_META_SHARDS`, default `1`
  = byte-for-byte pass-through). Account-global tables (users, activity, metrics, shares) live on
  shard 0; per-bucket tables on `shard_for_bucket`. Read the module header before touching it.

## Notes
- **Crypto fails closed, but this crate stores ciphertext only** — it never wraps/unwraps secrets;
  `sigv4_secret_ciphertext`/`_nonce` are opaque blobs sealed by `cairn-crypto`. Don't log row contents.
- The library `OpenOptions::default` is `synchronous=NORMAL` (benchmark/test posture); the **server
  overrides this to FULL** via `CAIRN_META_SYNCHRONOUS`. NORMAL never corrupts the DB — on power loss
  it loses at most the last uncheckpointed txn, which blob-first ordering downgrades to a GC'd orphan.
- `shard_for_bucket` uses a hand-rolled **FNV-1a**, not `std`'s `DefaultHasher` (whose internals may
  drift) — the mapping must be stable across processes/releases. Under sharding, user quota is
  **eventually-consistent** (it spans buckets on different shards) — a documented relaxation.
- The cache's generation re-check happens **inside the shard lock** to close the read-install TOCTOU;
  don't "optimize" it out of the lock.
- Spec: `docs/metadata.md` (11), durability `docs/storage-durability.md` (8), sharding `docs/testing-performance.md`
  (30). Tests: `tests/store.rs`, `tests/sharding.rs`, `tests/listing_oracle.rs`.
- See the root `../../CLAUDE.md` for the gate, env-only config, and the 4(+1)-site mutation rule.
