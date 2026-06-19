# cairn-meta-async

An async `MetadataStore` over the embedded **libSQL** / **Turso** drivers (beta). Reproduces
`cairn-meta`'s behaviour exactly — same schema/migrations and the same `Mutation` -> SQL `apply`.

## Layout (`src/`)
- `apply.rs` — **must mirror `cairn-meta/src/apply.rs`** (the second of the two backend mutation sites).
- `schema.rs` — must mirror `cairn-meta/src/schema.rs` (same versions/migrations).
- `driver.rs` / `libsql_driver.rs` / `turso_driver.rs` — the backend drivers behind one interface.
- `store.rs` / `writer.rs` / `model.rs` / `range.rs`.

## Notes
- **glibc-only**: excluded from the static musl build (bundled C deps SIGSEGV under static musl).
- Selected by `CAIRN_META_BACKEND=libsql|turso`; the default is the sqlite `cairn-meta`.
- Auto re-wrap + the durable seal counter (#29) are NOT implemented here (rotate-and-read only).
- Spec: `docs/metadata.md` (11). See the root `../../CLAUDE.md`.
