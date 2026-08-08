# web

The management console: a **React 19 + TypeScript SPA** (Vite, Tailwind v4, shadcn-style
`radix-ui` components, self-hosted Geist fonts). Built into `web/dist`, which the `cairn-web` crate
embeds into the binary and the server serves at the root of the web-console listener
(`CAIRN_WEB_ADDR`, :7374). **Excluded from the cargo workspace** — its gate is `npm run lint`
(ESLint 10 with `jsx-a11y-x`) + `npm run build` (strict `tsc` + vite), followed by
`npm audit --omit=dev --audit-level=moderate` and `npm audit --audit-level=high`; it is not covered
by cargo (see the root `../CLAUDE.md`).

## Layout (`src/`)
- `main.tsx` / `app.tsx` / `routes.tsx` — entry, provider shell (`ThemeProvider` → `AuthProvider` →
  router), routing. Add a page here.
- `views/` — one file per route (`buckets`, `bucket-detail` + nested `bucket-browser`/`-settings`,
  `users`, `overview`, `metrics`, `replication`, `tags`, `activity`, `credentials`, `login`).
- `components/` — hand-written shared web console (`permission-builder.tsx`, `data-table.tsx`,
  `share-dialog.tsx`, `object-preview.tsx` (the in-place object viewer), the `*-card.tsx` settings
  panels, `app-shell.tsx`/`app-sidebar.tsx`).
  `components/primitives/` — the generated shadcn/radix primitives; treat as vendored, regenerate via the
  shadcn CLI rather than hand-editing.
- `lib/` — `api.ts` (the `/api/v1` client + the error humanizer), `s3.ts` (object data plane,
  including `previewUrl`/`getObjectText` for the viewer), `preview.ts` (type detection + size caps),
  `markdown.tsx` (a safe, element-only Markdown subset),
  `live.ts` (SSE), `use-resource.ts` (data-fetch hook), `policy.ts` (policy↔builder codec),
  `types.ts`, `format.ts`, `activity.ts`, `utils.ts` (`cn`). `hooks/`, `providers/`.

## Invariants & rules
- **The web console is a pure presentation layer** over the control plane (ARCH 23) — it holds no privileged
  logic. Every server call goes through the `api` object in `lib/api.ts` (or `lib/s3.ts` for object
  bytes); **never** hand-roll a `fetch` to `/api/v1` in a view.
- **Persistent-share links are one-time output.** `share-dialog.tsx` may show/copy the URL returned
  by mint, but list/manage views receive only a stable non-secret share id and must never
  reconstruct `/share/{token}` or offer an existing-link copy action.
- **Auth is the server's httpOnly session cookie**, set by `POST /session` at sign-in. Management
  calls use `credentials: "same-origin"` and send **no** `Authorization` header; the cookie is never
  readable from JS — **never** put a token in `localStorage`/`sessionStorage`. The server honours
  that cookie only on the control listener. `lib/s3.ts` asks the management API for an exact
  data-origin SigV4 URL backed by a durable, bucket-scoped temporary session, then sends the raw S3
  request with `credentials: "omit"`. JavaScript retains only the public session handle in memory;
  the signing secret never leaves the server. (The "Bearer" copy in
  `users`/`user-detail`/`login` views is about end-user credentials, not console authentication.)
- **Browser dot segments fail closed.** WHATWG URL parsing removes literal and percent-encoded `.`
  / `..` path segments. `lib/s3.ts` and the server presign boundary reject these keys with an
  SDK/CLI fallback message; never normalize, retarget, or proxy them through the control origin.
- **Object bytes are never rendered as active content in the console origin.** `object-preview.tsx`
  renders only through inert paths (media elements; a PDF frame whose URL forces
  `response-content-type=application/pdf`, which `nosniff` then pins; text fetched and rendered as
  text — never `innerHTML`, never a same-origin navigation to the object). A stored `text/html` or
  `image/svg+xml` object would otherwise be stored XSS. Do not add a `sandbox` to the PDF frame: it
  breaks the built-in viewer, which is script-driven.
- **Hash routing on purpose** (`createHashRouter`). The server serves the SPA shell only at `/` and
  concrete embedded assets; every other control-listener path is a fail-closed 404. Don't switch to
  a browser router without defining an explicit, non-S3 server route family.
- **Recursive delete must converge.** A protected Object Lock version can make the control API
  return `more=true` indefinitely. Stop automatic retries after any zero-deletion pass, and
  de-duplicate failures by the exact `(key, version_id)` identity across partial passes.
- **`vite.config.ts` sets `base: "./"`** so assets are referenced relatively (`./assets/index-*`).
  `cairn-web`'s `index_referenced_bundles_are_embedded` test depends on this shape — **don't change
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
- Rust embed side: `../crates/cairn-web/`. Spec: `../docs/control-plane.md` (22–24).
