# cairn-ui

Embeds the built React management SPA (`ui/dist`) into the server binary via `rust_embed`, so a
Cairn deployment is one binary with no separate UI service to deploy or version-match (ARCH 23.1).
This crate is the *embed + serve* surface only — the React/TypeScript source lives in **`ui/`** (a
separate npm project, excluded from the cargo workspace; see `../../ui/CLAUDE.md`).

## Layout (`src/`)
- `lib.rs` — the whole public surface. `Assets` (`#[folder = "../../ui/dist"]`) is the compile-time
  embed; `asset(path)` resolves an embedded file to `(content_type, bytes)` (empty path and
  `index.html` both map to the shell, leading slash tolerated, content-type guessed by extension);
  `spa_shell()` returns `index.html` for client-side routes (ARCH 23.3).
- `build.rs` — scaffolds a placeholder `ui/dist/index.html` only if one doesn't already exist, so
  the crate **compiles on a fresh checkout** (a missing `#[folder]` is a hard `RustEmbed` error).

## Notes
- **A real `npm run build` is the only thing that satisfies the gate.** The placeholder shell
  references NO `assets/` bundles by design, so `index_referenced_bundles_are_embedded` still FAILS
  on a placeholder — build the UI first (`cd ui && npm run build`) before relying on the binary.
- `spa_shell()` **panics** if no `index.html` is embedded — impossible after a successful build, but
  it means an empty `ui/dist` surfaces as a panic, not a compile error.
- Consumed by `cairn-server/src/adapter.rs`: on the UI listener only, `/` and embedded assets are
  served BEFORE S3 routing so a bucket named `assets` can't shadow `/assets/...`; any other path
  falls through to S3 routing. Don't add routing logic here — this crate only resolves bytes.
- Keep the surface tiny: no HTTP, no auth, no state. It maps a request path to embedded bytes; the
  server owns response building, the two listeners, and the `/web`→`/` back-compat redirect.
- Spec: `../../docs/control-plane.md` (ARCH 23). Visual system: `../../docs/design.md`,
  `../../docs/product.md`. See the root `../../CLAUDE.md` for the gate and workspace-wide rules.
