# cairn-meta

The default SQLite `MetadataStore`: one serialized, **group-committing writer** + a pool of
read-only WAL connections + a read-through cache. The metadata commit is the single linearization
point of every mutation.

## Layout (`src/`)
- `writer.rs` — the single `Writer` thread (group-commit, savepoint-per-mutation, `Control::Exec`).
  ALL writes go through it; never open an ad-hoc write connection.
- `store.rs` — reads on the WAL pool (`with_read`) + inherent helpers (e.g. the #29 re-wrap methods).
- `apply.rs` — `Mutation` -> SQL. **Mirror any change in `cairn-meta-async/src/apply.rs`.**
- `schema.rs` — migrations: **append-only**, keyed by version; never edit an applied migration.
- `cache.rs` — the read-through config/bucket cache (generation-guarded; see audit #5/#6).
- `shard.rs` — `ShardedMetadataStore` (bucket-partitioned routing, `CAIRN_META_SHARDS`).

## Notes
- Spec: `docs/metadata.md` (11), durability `docs/storage-durability.md` (8). Tests: `tests/`.
- See the root `../../CLAUDE.md` for the gate, the env-only config, and the 4(+1)-site mutation rule.
