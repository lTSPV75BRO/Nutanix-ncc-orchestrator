// WCAG 1.4.10 Reflow: at 400% zoom a 1280 px window is 320 CSS px wide and 256
// CSS px tall. Content must not need horizontal scrolling, except inside data
// tables, code blocks and charts, which may scroll within themselves.
import { expect, test, type Page } from "@playwright/test";
import { THEMES, open, prepare, resolveUrl } from "./helpers";
import { OVERLAYS } from "./overlays";
import { pageUrls } from "./routes";

test.use({ viewport: { width: 320, height: 256 } });

/** Page-level overflow, and the elements responsible. */
async function overflow(page: Page) {
  return page.evaluate(() => {
    const doc = document.documentElement;
    const width = doc.clientWidth;
    const offenders: string[] = [];
    for (const el of document.querySelectorAll<HTMLElement>("body *")) {
      const r = el.getBoundingClientRect();
      if (r.width === 0 || r.height === 0 || r.right <= width + 1) continue;
      const cs = getComputedStyle(el);
      if (cs.position === "fixed" || cs.visibility === "hidden" || cs.display === "none") continue;
      // Sticking out is fine inside a container that fits the screen and
      // scrolls on its own (a data table, a code block, a chart), and inside
      // the tab bar, which collapses its overflow into a menu.
      let inside = false;
      for (let p = el.parentElement; p && p !== document.body; p = p.parentElement) {
        const scrolls = /(auto|scroll)/.test(getComputedStyle(p).overflowX) || p.matches(".ant-tabs-nav-wrap, .ant-switch, .ant-table-header, .ant-table-tbody-virtual, .ant-table-tbody-virtual-holder");
        if (scrolls && p.getBoundingClientRect().right <= width + 1) {
          inside = true;
          break;
        }
      }
      if (!inside) offenders.push(`${el.tagName.toLowerCase()}.${String(el.className).slice(0, 60)} right=${Math.round(r.right)} "${(el.textContent ?? "").slice(0, 40)}"`);
    }
    return { scrollWidth: Math.max(doc.scrollWidth, document.body.scrollWidth), width, offenders: offenders.slice(0, 8) };
  });
}

/** No page-level sideways scroll, and nothing sticking out past the edge outside a scroller (clipped content is a failure too). */
async function expectFits(page: Page, label: string) {
  // Fonts differ between systems; leave room for one that is a little wider.
  await page.addStyleTag({ content: "body * { letter-spacing: 0.06em; }" });
  const o = await overflow(page);
  const where = `\n  ${o.offenders.join("\n  ")}`;
  expect(o.scrollWidth, `${label} scrolls sideways (${o.scrollWidth} > ${o.width}):${where}`).toBeLessThanOrEqual(o.width);
  expect(o.offenders, `${label}: content past the right edge`).toEqual([]);
}

for (const theme of THEMES) {
  test(`${theme}: sign-in page reflows at 320 px`, async ({ page }) => {
    await prepare(page, { theme, as: null });
    await open(page, "/login");
    await expectFits(page, "sign-in");
  });

  for (const url of pageUrls()) {
    test(`${theme}: ${url} reflows at 320 px`, async ({ page }) => {
      await prepare(page, { theme });
      await open(page, await resolveUrl(page, url));
      await expectFits(page, url);
    });
  }
}

for (const o of OVERLAYS.filter((x) => x.name !== "notification toast")) {
  test(`overlay ${o.name} fits at 320 px`, async ({ page }) => {
    await prepare(page);
    await o.open(page);
    await expect(o.surface(page)).toBeVisible();
    const box = await o.surface(page).boundingBox();
    expect(box, "overlay has a box").not.toBeNull();
    expect(box!.x, "overlay starts off the left edge").toBeGreaterThanOrEqual(-1);
    expect(box!.x + box!.width, "overlay runs past the right edge").toBeLessThanOrEqual(321);
    await expectFits(page, `${o.name} open`);
  });
}
