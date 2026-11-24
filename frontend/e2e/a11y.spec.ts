// axe-core (WCAG 2.0, 2.1 and 2.2, A and AA, including colour contrast) on the
// demo build: the sign-in page, every route as an admin with populated data,
// every tab on those pages, and every overlay, in all three themes.
import { expect, test } from "@playwright/test";
import { THEMES, expectNoViolations, open, prepare, resolveUrl, settled } from "./helpers";
import { OVERLAYS } from "./overlays";
import { POPULATED, ROUTES, pageUrls } from "./routes";

for (const theme of THEMES) {
  test.describe(`theme ${theme}`, () => {
    test("sign-in page", async ({ page }) => {
      await prepare(page, { theme, as: null });
      await open(page, "/login");
      await expect(page.getByRole("button", { name: /sign in/i })).toBeVisible();
      await expectNoViolations(page, `${theme} sign-in`);
    });

    for (const url of pageUrls()) {
      test(`page ${url} and its tabs`, async ({ page }) => {
        await prepare(page, { theme });
        const target = await resolveUrl(page, url);
        await open(page, target);
        // Populated, not just loaded: the page shows its fixture data.
        const want = POPULATED[url];
        if (want) await expect(page.locator("main")).toContainText(want);
        await expect(page.locator("main")).not.toBeEmpty();
        await expectNoViolations(page, `${theme} ${url}`);

        const tabs = page.locator("main [role=tab]");
        const count = await tabs.count();
        for (let i = 0; i < count; i++) {
          const tab = tabs.nth(i);
          if (!(await tab.isVisible()) || (await tab.getAttribute("aria-selected")) === "true") continue;
          const name = (await tab.innerText()).trim();
          await tab.click();
          await settled(page);
          await expectNoViolations(page, `${theme} ${url} tab "${name}"`);
        }
      });
    }

    for (const o of OVERLAYS) {
      test(`overlay: ${o.name}`, async ({ page }) => {
        await prepare(page, { theme });
        await o.open(page);
        await expect(o.surface(page)).toBeVisible();
        await settled(page);
        await expectNoViolations(page, `${theme} ${o.name}`);
      });
    }

    test("chart page: trend chart and its table view", async ({ page }) => {
      await prepare(page, { theme });
      await open(page, "/insights");
      await page.getByRole("tab", { name: /Trends/ }).click();
      await settled(page);
      await expect(page.locator(".insights-trend")).toBeVisible();
      await expectNoViolations(page, `${theme} trend chart`);
      await page.locator(".ant-segmented-item-label", { hasText: "Table" }).click();
      await expect(page.getByRole("table", { name: "Failures over recent runs" })).toBeVisible();
      await expectNoViolations(page, `${theme} trend table`);
    });
  });
}

test.describe("routes resolve", () => {
  for (const r of ROUTES.filter((x) => x.kind === "redirect")) {
    test(`${r.pattern} redirects to ${r.lands}`, async ({ page }) => {
      await prepare(page);
      await open(page, r.url);
      await expect.poll(() => new URL(page.url()).hash).toBe(`#${r.lands}`);
    });
  }
});
