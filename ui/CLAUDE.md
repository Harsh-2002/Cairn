# ui

The management console: a **React 19 + TypeScript SPA** (Vite, Tailwind v4, shadcn-style
`radix-ui` components, self-hosted Geist fonts). Built into `ui/dist`, which the `cairn-ui` crate
embeds into the binary and the server serves at the root of the web-UI listener
(`CAIRN_UI_ADDR`, :7374). **Excluded from the cargo workspace** — its gate is `npm run build`, not
cargo (see the root `../CLAUDE.md`).

## Layout (`src/`)
- `main.tsx` / `app.tsx` / `routes.tsx` — entry, provider shell (`ThemeProvider` → `AuthProvider` →
  router), routing. Add a page here.
- `views/` — one file per route (`buckets`, `bucket-detail` + nested `bucket-browser`/`-settings`,
  `users`, `overview`, `metrics`, `replication`, `tags`, `activity`, `credentials`, `login`).
- `components/` — hand-written shared UI (`permission-builder.tsx`, `data-table.tsx`,
  `share-dialog.tsx`, the `*-card.tsx` settings panels, `app-shell.tsx`/`app-sidebar.tsx`).
  `components/ui/` — the generated shadcn/radix primitives; treat as vendored, regenerate via the
  shadcn CLI rather than hand-editing.
- `lib/` — `api.ts` (the `/api/v1` client + the error humanizer), `s3.ts` (object data plane),
  `live.ts` (SSE), `use-resource.ts` (data-fetch hook), `policy.ts` (policy↔builder codec),
  `types.ts`, `format.ts`, `activity.ts`, `utils.ts` (`cn`). `hooks/`, `providers/`.

## Invariants & rules
- **The UI is a pure presentation layer** over the control plane (ARCH 23) — it holds no privileged
  logic. Every server call goes through the `api` object in `lib/api.ts` (or `lib/s3.ts` for object
  bytes); **never** hand-roll a `fetch` to `/api/v1` in a view.
- **Auth is the server's httpOnly session cookie**, set by `POST /session` at sign-in. All requests
  use `credentials: "same-origin"` and send **no** `Authorization` header; the cookie is never
  readable from JS — **never** put a token in `localStorage`/`sessionStorage`. The same cookie
  authorizes the S3 data plane at root (`lib/s3.ts`), so the browser uploads/downloads bytes
  directly. (The "Bearer" copy in `users`/`user-detail`/`login` views is about the **end-user S3
  credentials the console mints**, not how the console authenticates — keep them distinct.)
- **Hash routing on purpose** (`createHashRouter`). The server serves the SPA shell only at `/`;
  every other path is the S3 data plane, so history-mode routes would collide with `/{bucket}/{key}`.
  Don't switch to a browser router.
- **`vite.config.ts` sets `base: "./"`** so assets are referenced relatively (`./assets/index-*`).
  `cairn-ui`'s `index_referenced_bundles_are_embedded` test depends on this shape — **don't change
  it**. A real build is required: without `dist/` the crate embeds a placeholder that fails that test.
- `@/` aliases `src/` (vite + tsconfig). `tsc -b` runs in strict mode with `noUnusedLocals`/
  `noUnusedParameters` — dead imports/vars fail the build, not just lint.

## Notes
- Fetch data with `useResource(load, deps)`: it keeps stale data on screen during a refresh
  (`refreshing` vs first-load `loading`) and discards out-of-order responses. Surface errors via
  `errorMessage(e, fallback)` from `lib/api.ts` — the humanizer maps S3/control `<Code>`s to
  operator-readable copy; don't render raw server strings.
- Live updates: subscribe a view with `useLiveTopic` (`lib/live.ts`), one multiplexed `EventSource`
  per tab. EventSource can't send headers, so it mints a single-use ticket (`POST /events/ticket`)
  and opens with `?ticket=`. It degrades silently to the per-view Refresh button.
- The Metrics view is lazy-loaded to code-split `recharts` out of the initial bundle (see the
  `Suspense` fallback in `routes.tsx`); keep heavy deps off the critical path the same way.
- Theme: light/dark/`system` via a `.dark` class on `<html>` + `color-scheme` (`theme-provider.tsx`);
  design tokens are oklch CSS vars in `globals.css`.
- Visual system: `../docs/design.md` (Vercel/Geist minimalism, 1px borders not shadows, neutral
  primary, semantic colour only when it means something, AAA-where-it-helps, honour
  `prefers-reduced-motion`); product intent in `../docs/product.md`.
- Rust embed side: `../crates/cairn-ui/`. Spec: `../docs/control-plane.md` (22–24).
