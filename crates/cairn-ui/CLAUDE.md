# cairn-ui

The Rust crate that **embeds** the built React management SPA into the binary (`rust_embed` over
`ui/dist`).

## Layout (`src/`)
- `lib.rs` — the embed + asset serving. `build.rs` scaffolds a placeholder `ui/dist` so the crate
  always compiles even before a real UI build.

## Notes
- The actual React/TypeScript source lives in **`ui/`** (a separate npm project, excluded from the
  cargo workspace) — see `../../ui/CLAUDE.md`.
- Build the UI first (`cd ui && npm run build`) or the binary embeds a placeholder that FAILS the
  `index_referenced_bundles_are_embedded` test.
- Spec: `docs/control-plane.md` (23). Visual system: `../../DESIGN.md`, `../../PRODUCT.md`.
