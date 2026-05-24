import { defineConfig, devices } from "@playwright/test";

// Two run modes:
//
// 1) Against Vite dev (`npm run dev` in admin/, plus `cairn serve` for /api/*):
//    set CAIRN_BASE_URL=http://localhost:5173
//
// 2) Against the embedded SPA (`cairn serve` only — admin/dist is embedded
//    into the binary): set CAIRN_BASE_URL=http://localhost:8080
//
// Tests do not start either server themselves — operator runs them before
// `playwright test`. Auto-spawning would couple too tightly to the dev
// machine's git layout (`cairn serve` needs a real source repository).

const BASE_URL = process.env.CAIRN_BASE_URL ?? "http://localhost:5173";

export default defineConfig({
  testDir: "./tests-e2e",
  fullyParallel: true,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL: BASE_URL,
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
