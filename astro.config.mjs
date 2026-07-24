// @ts-check
import { defineConfig } from "astro/config";

// Served as a GitHub Pages PROJECT site at https://harsh-2002.github.io/Cairn/, so the whole app
// lives under the /Cairn/ base path. `site` builds correct absolute/canonical URLs; `base` prefixes
// every framework-emitted asset. NOTE: Astro does NOT auto-prefix `base` onto links/assets you write
// by hand — so hand-written references stay RELATIVE (./…) or use `import.meta.env.BASE_URL`.
export default defineConfig({
  site: "https://harsh-2002.github.io",
  base: "/Cairn",
  trailingSlash: "ignore",
  build: {
    // Emit /page/index.html so links work with or without a trailing slash under the base path.
    format: "directory",
  },
  devToolbar: { enabled: false },
});
