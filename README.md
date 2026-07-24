# Cairn website

The source for the Cairn landing page — an **orphan branch** (no shared history with `main`),
deployed to GitHub Pages at **https://harsh-2002.github.io/Cairn/**.

- **Stack:** [Astro](https://astro.build) (static, zero-JS by default), self-hosted Geist Sans + Geist
  Mono. Typography-only — no images or icon assets. Strictly monochrome, with light/dark mode.
- **Content:** Cairn's own, drawn from the main branch's `README.md` and `docs/`.
- **Deploy:** `.github/workflows/pages.yml` builds on every push to `website` and deploys via the
  official GitHub Pages actions. Base path is `/Cairn/` (set in `astro.config.mjs`).

## Develop

```sh
npm install
npm run dev      # http://localhost:4321/Cairn/
npm run build    # -> dist/
npm run preview  # serve the production build
```

Edit copy in `src/pages/index.astro`; the design system is `src/styles/tokens.css`; the shell +
theme toggle is `src/layouts/Base.astro`.
