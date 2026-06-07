import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";

// The built assets are embedded into the Cairn server binary via rust-embed
// and served under the `/ui/` path. A relative base ("./") makes the generated
// asset URLs resolve correctly regardless of the mount prefix.
export default defineConfig({
  base: "./",
  plugins: [svelte()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
