// Keyboard use: the skip link, a visible focus indicator on every tab stop of
// the Overview's first screen, and Escape closing overlays with focus going
// back to what opened them (WCAG 2.1.1, 2.4.1, 2.4.7, 2.4.3).
import { expect, test, type Page } from "@playwright/test";
import { THEMES, open, prepare } from "./helpers";
import { OVERLAYS } from "./overlays";

/** True if the focused element looks different focused than not (a pixel comparison, so it sees what a person sees). */
async function focusIsVisible(page: Page): Promise<{ ok: boolean; what: string }> {
  const handle = await page.evaluateHandle(() => document.activeElement as HTMLElement);
  const el = handle.asElement();
  if (!el) return { ok: false, what: "nothing focused" };
  const what = await el.evaluate((e) => `${e.tagName.toLowerCase()} "${(e.getAttribute("aria-label") || e.textContent || "").trim().slice(0, 40)}"`);
  const box = await el.boundingBox();
  if (!box) return { ok: false, what };
  const vp = page.viewportSize()!;
  const pad = 6;
  const x = Math.max(0, box.x - pad);
  const y = Math.max(0, box.y - pad);
  const clip = {
    x,
    y,
    width: Math.max(1, Math.min(vp.width - x, box.width + 2 * pad)),
    height: Math.max(1, Math.min(vp.height - y, box.height + 2 * pad)),
  };
  const focused = await page.screenshot({ clip });
  await el.evaluate((e) => e.blur());
  const plain = await page.screenshot({ clip });
  await el.focus();
  return { ok: !focused.equals(plain), what };
}

test.describe("skip link", () => {
  test("is the first tab stop and moves focus to the content without changing the page", async ({ page }) => {
    await prepare(page);
    await open(page, "/overview");
    await page.keyboard.press("Tab");
    const first = page.locator(":focus");
    await expect(first).toHaveText("Skip to content");
    await expect(first).toBeVisible();
    await page.keyboard.press("Enter");
    await expect(page.locator("#main-content")).toBeFocused();
    // With the hash router a bare "#main-content" link would navigate to a page that does not exist.
    expect(new URL(page.url()).hash).toBe("#/overview");
    await expect(page.locator("main h1, h1")).toHaveCount(1);
  });
});

for (const theme of THEMES) {
  test(`${theme}: every tab stop on the Overview's first screen shows focus`, async ({ page }) => {
    await prepare(page, { theme });
    await open(page, "/overview");
    const seen: string[] = [];
    const vp = page.viewportSize()!;
    for (let i = 0; i < 40; i++) {
      await page.keyboard.press("Tab");
      const onBody = await page.evaluate(() => document.activeElement === document.body || document.activeElement === document.documentElement);
      if (onBody) break;
      const box = await page.evaluate(() => {
        const r = document.activeElement!.getBoundingClientRect();
        return { top: r.top, bottom: r.bottom, w: r.width, h: r.height };
      });
      if (box.top >= vp.height || box.bottom <= 0 || box.w === 0 || box.h === 0) continue; // off the first screen
      const { ok, what } = await focusIsVisible(page);
      seen.push(what);
      expect(ok, `${theme}: no visible focus indicator on ${what}`).toBe(true);
    }
    // Skip link, brand, menu, collapse, jump, trigger, user menu at least.
    expect(seen.length, `tab stops checked: ${seen.join(" | ")}`).toBeGreaterThanOrEqual(7);
  });
}

for (const o of OVERLAYS.filter((x) => x.closesWithEscape)) {
  test(`Escape closes the ${o.name} and focus returns to what opened it`, async ({ page }) => {
    await prepare(page);
    const trigger = await o.open(page);
    await expect(o.surface(page)).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(o.surface(page)).toBeHidden();
    if (o.focusReturns) await expect(trigger).toBeFocused();
  });
}
