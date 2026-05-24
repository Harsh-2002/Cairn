import { expect, test } from "@playwright/test";

// Smoke spec: the admin SPA loads, the header shows, and the post list
// area renders. Does not require the API to return a populated list —
// the empty / loading state is acceptable for this spec because the
// goal is "the bundle works at all in a real browser."

test("admin SPA renders the header", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Cairn admin" })).toBeVisible();
});

test("admin SPA mounts without console errors", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  page.on("console", (msg) => {
    if (msg.type() === "error") errors.push(msg.text());
  });
  await page.goto("/");
  // Wait for the editor mount cycle to settle.
  await page.waitForTimeout(500);
  // We expect API-related fetch errors when the backend isn't running
  // (CORS, 404, etc.); filter those out and fail only on JS exceptions.
  const realErrors = errors.filter(
    (e) =>
      !e.includes("listPosts:") &&
      !e.includes("Failed to load") &&
      !e.includes("net::ERR_"),
  );
  expect(realErrors, `unexpected JS errors:\n${realErrors.join("\n")}`).toEqual(
    [],
  );
});
