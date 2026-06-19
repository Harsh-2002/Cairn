# ui

The management console: a **React + TypeScript SPA** (Vite + shadcn/ui, Geist fonts). Built into
`ui/dist`, which the `cairn-ui` crate embeds into the binary and the server serves at the root of
the web-UI listener (`CAIRN_UI_ADDR`, :7374). Excluded from the cargo workspace.

## Layout (`src/`)
- `main.tsx` / `app.tsx` / `routes.tsx` — entry, shell, routing.
- `views/` — the pages (buckets, objects, users, metrics, ...). `components/` — shared UI
  (`permission-builder.tsx`, `data-table.tsx`, `share-dialog.tsx`, `json-editor.tsx`, ...).
- `lib/` — the API client + S3 helpers (`s3.ts`, `api.ts`). `hooks/` / `providers/`.

## Build & gate
- `npm run build` (`tsc -b && vite build`) — type-checks AND bundles into `dist/`; this is the gate
  for UI changes. `npm run dev` for local dev. The binary embeds whatever is in `dist/`.
- A real build is required: without it the crate embeds a placeholder that fails the embed test.

## Notes
- Follow the visual system in `../docs/design.md` and the product intent in `../docs/product.md`:
  Vercel/Geist minimalism, 1px borders not shadows, neutral primary, semantic colour only when it
  means something; AAA-where-it-helps accessibility, honour `prefers-reduced-motion`.
- The Rust embed side is `../crates/cairn-ui/`. Spec: `../docs/control-plane.md` (23).
