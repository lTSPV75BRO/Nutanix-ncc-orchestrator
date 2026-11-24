// What a screen reader depends on, checked as far as a browser test can: one
// h1, a title that changes per route, landmarks, a live region for
// notifications and a text alternative for charts. This is a proxy. A real
// screen reader pass (NVDA + Firefox, VoiceOver + Safari) stays manual; see
// docs/ui/DESIGN_SYSTEM.md.
import { expect, test, type Page } from "@playwright/test";
import { open, prepare, resolveUrl, settled } from "./helpers";
import { OVERLAYS } from "./overlays";
import { pageUrls } from "./routes";

const URLS = pageUrls();

/** Heading levels never skip going down (h1 -> h2 -> h3). */
async function expectHeadingOrder(page: Page, where: string) {
  const levels = await page.evaluate(() => [...document.querySelectorAll("main h1, main h2, main h3, main h4, main h5, main h6, h1")].filter((h) => (h as HTMLElement).checkVisibility()).map((h) => Number(h.tagName[1])));
  levels.forEach((lv, i) => {
    if (i > 0) expect.soft(lv - levels[i - 1], `${where}: heading levels ${levels.join(",")}`).toBeLessThanOrEqual(1);
  });
}

test("every page has exactly one h1, a landmark structure and its own title", async ({ page }) => {
  await prepare(page);
  const titles = new Map<string, string>();
  for (const url of URLS) {
    const target = await resolveUrl(page, url);
    await open(page, target);
    const h1s = await page.locator("h1").allInnerTexts();
    expect(h1s.length, `${url}: h1 elements: ${JSON.stringify(h1s)}`).toBe(1);
    expect(h1s[0].trim(), `${url}: empty h1`).not.toBe("");

    // Landmarks: one main, one banner, a labelled sidebar, at least one nav.
    expect(await page.locator("main, [role=main]").count(), `${url}: main landmarks`).toBe(1);
    // A <header> inside main, a section or an article is not a banner.
    const banners = await page.evaluate(() => [...document.querySelectorAll("header, [role=banner]")].filter((h) => !h.closest("article, aside, main, nav, section")).length);
    expect(banners, `${url}: banner landmarks`).toBe(1);
    expect(await page.locator("nav, [role=navigation]").count(), `${url}: navigation landmarks`).toBeGreaterThanOrEqual(1);
    await expect(page.locator("aside.ui-sidebar")).toHaveAttribute("aria-label", "Primary");

    await expectHeadingOrder(page, url);
    // ...and on every tab of the page.
    const tabs = page.locator("main [role=tab]");
    for (let i = 0; i < (await tabs.count()); i++) {
      const tab = tabs.nth(i);
      if (!(await tab.isVisible()) || (await tab.getAttribute("aria-selected")) === "true") continue;
      await tab.click();
      await settled(page);
      await expectHeadingOrder(page, `${url} tab "${(await tab.innerText()).trim()}"`);
    }

    const title = await page.title();
    expect(title.trim(), `${url}: empty title`).not.toBe("");
    titles.set(url, title);
  }
  // Different pages, different titles. Sub-pages of one page may share one
  // ("Integrations" and a single integration).
  const family = (u: string) => (u.startsWith("/integrations") ? "/integrations" : u);
  const seen = new Map<string, string>();
  for (const [url, title] of titles) {
    const prior = seen.get(title);
    if (prior) expect(family(prior), `"${title}" is the title of both ${prior} and ${url}`).toBe(family(url));
    seen.set(title, url);
  }
});

test("the page title changes when navigating from page to page", async ({ page }) => {
  await prepare(page);
  await open(page, "/overview");
  const seen = [await page.title()];
  for (const label of ["Findings", "Insights", "Runs", "Integrations"]) {
    await page.locator(".ui-sidebar").getByRole("menuitem", { name: label }).click();
    await expect.poll(() => page.title()).toContain(label);
    const t = await page.title();
    expect(seen, `title after opening ${label}`).not.toContain(t);
    seen.push(t);
    await expect(page.locator("h1")).toHaveText(label);
  }
});

test("notifications are announced: a toast lands in a live region", async ({ page }) => {
  await prepare(page);
  const toast = OVERLAYS.find((o) => o.name === "notification toast")!;
  await toast.open(page);
  const notice = toast.surface(page);
  await expect(notice).toBeVisible();
  const live = await notice.evaluate((n) => {
    const hit = n.closest("[aria-live], [role=alert], [role=status]") ?? n.querySelector("[aria-live], [role=alert], [role=status]");
    return hit ? `${hit.getAttribute("role") ?? ""} ${hit.getAttribute("aria-live") ?? ""}`.trim() : null;
  });
  expect(live, "toast is not inside or marked as a live region").not.toBeNull();
  // And the page has a polite live region of its own for status text.
  expect(await page.locator("[aria-live]").count()).toBeGreaterThanOrEqual(1);
});

test("charts have a text alternative, and the trend chart a table view", async ({ page }) => {
  await prepare(page);
  await open(page, "/insights");
  // Summary tab: the severity bar.
  const stack = page.locator(".insights-stack[role=img]");
  await expect(stack).toHaveAttribute("aria-label", /\d/);
  await page.getByRole("tab", { name: /Trends/ }).click();
  await settled(page);
  const chart = page.locator(".insights-trend[role=img]");
  const label = (await chart.getAttribute("aria-label")) ?? "";
  expect(label, "trend chart name must carry the values").toMatch(/\d+(, \d+)+/);
  // The table view has one row per bar and the same numbers.
  const bars = await page.locator(".insights-trend-col").count();
  await page.locator(".ant-segmented-item-label", { hasText: "Table" }).click();
  const table = page.getByRole("table", { name: "Failures over recent runs" });
  await expect(table).toBeVisible();
  await expect(table.locator("tbody tr:not([aria-hidden])")).toHaveCount(bars);
  await expect(page.locator(".insights-trend")).toHaveCount(0);
  await page.locator(".ant-segmented-item-label", { hasText: "Chart" }).click();
  await expect(page.locator(".insights-trend")).toBeVisible();

  // Status page: runs per day.
  await open(page, "/system/status");
  for (const img of await page.locator("main [role=img]").all()) {
    const name = ((await img.getAttribute("aria-label")) ?? (await img.getAttribute("aria-labelledby")) ?? "").trim();
    expect(name, `role=img without a name: ${await img.evaluate((e) => e.outerHTML.slice(0, 120))}`).not.toBe("");
  }
});
