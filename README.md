# Cairn website

The source for the Cairn landing page — an **orphan branch** (no shared history with `main`),
deployed to GitHub Pages at **https://harsh-2002.github.io/Cairn/**.

- **Stack:** [Astro](https://astro.build) (static), self-hosted Geist Sans + Geist Mono. Monochrome
  light/dark design with a console screenshot gallery; small scripts power theme switching and copy buttons.
- **Content:** Cairn's own, drawn from the main branch's `README.md` and `docs/`.
- **Deploy:** `.github/workflows/pages.yml` builds on every push to `website` and deploys via the
  official GitHub Pages actions. Base path is `/Cairn/` (set in `astro.config.mjs`).

## Develop

```sh
npm install
npm run dev      # http://localhost:4321/Cairn/
npm run check    # Astro + TypeScript diagnostics
npm run build    # -> dist/
npm run preview  # serve the production build
npm audit --audit-level=moderate
```

Edit copy in `src/pages/index.astro`; the design system is `src/styles/tokens.css`; the shell +
theme toggle is `src/layouts/Base.astro`.

Keep deployment examples and endpoint names aligned with the current release on `main`. Console
screenshots are illustrative development captures. Performance claims must link to the documented
workload and measurements in `docs/benchmarks.md` on `main`.
