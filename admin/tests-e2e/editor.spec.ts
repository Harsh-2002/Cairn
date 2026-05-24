import { test, expect } from "@playwright/test";

// Editor-flow specs. Run against:
//   - `cairn serve <empty source>` on :8080 with --admin-secret test-secret
//   - `npm --prefix admin run dev` on :5173 (CAIRN_BASE_URL=http://localhost:5173)
//
// These specs create + edit + delete posts; the source repo will be
// left with the test artifacts. Use a throwaway directory.

const ADMIN_SECRET = process.env.CAIRN_ADMIN_SECRET ?? "test-secret";

async function setupSecret(page: import("@playwright/test").Page) {
  await page.addInitScript((secret) => {
    localStorage.setItem("cairn:admin_secret", secret);
  }, ADMIN_SECRET);
}

test.describe("editor flow", () => {
  test("creates a new post via the + New page button", async ({ page }) => {
    await setupSecret(page);
    await page.goto("/");

    const before = await page.locator(".post-item").count();
    await page.locator(".new-button").click();

    await expect(page.locator(".post-item")).toHaveCount(before + 1);
    await expect(page.locator(".post-row.active .post-title")).toHaveText(
      /Untitled/,
    );
    await expect(page.locator(".title")).toHaveValue("Untitled");
  });

  test("autosave fires after the debounce window", async ({ page }) => {
    await setupSecret(page);
    await page.goto("/");
    // Make sure we have a post.
    if ((await page.locator(".post-item").count()) === 0) {
      await page.locator(".new-button").click();
    }

    const title = page.locator(".title");
    await title.fill("Autosave Test");

    // Status pill becomes "Saved" within ~3s (1.5s debounce + commit time).
    await expect(page.locator('.status[data-status="saved"]')).toBeVisible({
      timeout: 5_000,
    });
  });

  test("publish writes a commit on main", async ({ page, request }) => {
    await setupSecret(page);
    await page.goto("/");
    if ((await page.locator(".post-item").count()) === 0) {
      await page.locator(".new-button").click();
    }
    await page.locator(".title").fill("Publish Test");
    await expect(page.locator('.status[data-status="saved"]')).toBeVisible({
      timeout: 5_000,
    });

    await page.locator(".publish").click();

    // After publish, the short commit hash appears in the top bar.
    await expect(page.locator(".hash")).toBeVisible({ timeout: 5_000 });
  });

  test("delete removes the post from the sidebar", async ({ page }) => {
    await setupSecret(page);
    await page.goto("/");
    // Ensure at least one post exists.
    if ((await page.locator(".post-item").count()) === 0) {
      await page.locator(".new-button").click();
    }
    const before = await page.locator(".post-item").count();

    page.once("dialog", (d) => d.accept());
    const firstRow = page.locator(".post-row").first();
    await firstRow.hover();
    await firstRow.locator(".post-delete").click();

    await expect(page.locator(".post-item")).toHaveCount(before - 1);
  });

  test("two tabs editing the same post get distinct session branches", async ({
    browser,
  }) => {
    const ctxA = await browser.newContext();
    const ctxB = await browser.newContext();
    const pageA = await ctxA.newPage();
    const pageB = await ctxB.newPage();

    for (const p of [pageA, pageB]) {
      await p.addInitScript((s) => {
        localStorage.setItem("cairn:admin_secret", s);
      }, ADMIN_SECRET);
    }
    await pageA.goto("/");
    await pageB.goto("/");

    // Both need an existing post; if there isn't one, create one in A.
    if ((await pageA.locator(".post-item").count()) === 0) {
      await pageA.locator(".new-button").click();
      await pageA.waitForTimeout(800);
      await pageB.reload();
    }

    await pageA.locator(".post-item").first().click();
    await pageB.locator(".post-item").first().click();

    await pageA.locator(".title").fill("A's edit");
    await pageB.locator(".title").fill("B's edit");

    // Wait for both autosaves to land.
    await expect(pageA.locator('.status[data-status="saved"]')).toBeVisible({
      timeout: 5_000,
    });
    await expect(pageB.locator('.status[data-status="saved"]')).toBeVisible({
      timeout: 5_000,
    });

    // Session UUIDs are minted per-tab; the hash shown is the commit SHA
    // returned from autosave/publish. Distinct sessions produce distinct
    // commits on distinct draft branches.
    const hashA = await pageA.locator(".hash").innerText().catch(() => "");
    const hashB = await pageB.locator(".hash").innerText().catch(() => "");
    expect(hashA).not.toEqual(hashB);

    await ctxA.close();
    await ctxB.close();
  });

  test("colliding new-post titles get a suffix on the slug", async ({ page }) => {
    await setupSecret(page);
    await page.goto("/");
    await page.locator(".new-button").click();
    await page.waitForTimeout(400);
    const firstSlug = await page.locator(".crumb.current").innerText();

    await page.locator(".new-button").click();
    await page.waitForTimeout(400);
    const secondSlug = await page.locator(".crumb.current").innerText();

    expect(secondSlug).not.toEqual(firstSlug);
    // The second untitled gets a timestamp suffix.
    expect(secondSlug).toMatch(/untitled-\d+/);
  });
});
