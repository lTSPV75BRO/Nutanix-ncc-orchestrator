// WCAG 1.4.4 Resize text and 1.4.12 Text spacing (A11Y-03). At 1280 x 800:
//   - the root font size is doubled (200%), and
//   - the text-spacing bookmarklet styles are applied to every element.
// Neither may make a page scroll sideways, and headings, buttons and nav links
// must not be clipped. Tables and code blocks may scroll inside their own
// container; that is allowed, so only the elements listed below are checked.
import { expect, test, type Page } from "@playwright/test";
import { open, prepare, resolveUrl } from "./helpers";
import { pageUrls } from "./routes";

/** Headings, buttons and nav links: the text a user must be able to read in full. */
const TEXT_SELECTOR = "h1, h2, h3, h4, h5, h6, button, nav a, [role='tab'], [role='button']";

/** WCAG 1.4.12 bookmarklet: line height 1.5, letter spacing 0.12em, word spacing 0.16em, paragraph spacing 2em. */
const TEXT_SPACING_CSS = `
  * { line-height: 1.5 !important; letter-spacing: 0.12em !important; word-spacing: 0.16em !important; }
  p { margin-bottom: 2em !important; }
`;

/** Page-level sideways scroll, and clipped headings, buttons and nav links. */
async function layoutProblems(page: Page) {
  return page.evaluate((selector) => {
    const doc = document.scrollingElement!;
    const width = doc.clientWidth;
    const pageScroll = doc.scrollWidth - width;
    const clipped: string[] = [];
    for (const el of document.querySelectorAll<HTMLElement>(selector)) {
      const r = el.getBoundingClientRect();
      if (r.width === 0 || r.height === 0) continue;
      const cs = getComputedStyle(el);
      if (cs.visibility === "hidden" || cs.display === "none") continue;
      if (!(el.textContent ?? "").trim()) continue;
      // Ellipsis is a deliberate truncation, so it is not a clipping failure.
      if (cs.textOverflow === "ellipsis") continue;
      const hidesOverflow = /(hidden|clip)/.test(cs.overflowX) || /(hidden|clip)/.test(cs.overflowY);
      if (hidesOverflow && el.scrollWidth > el.clientWidth + 1) {
        clipped.push(`${el.tagName.toLowerCase()}.${String(el.className).slice(0, 50)} "${(el.textContent ?? "").trim().slice(0, 40)}" scrollWidth=${el.scrollWidth} clientWidth=${el.clientWidth}`);
      }
    }
    return { pageScroll, width, scrollWidth: doc.scrollWidth, clipped: clipped.slice(0, 8) };
  }, TEXT_SELECTOR);
}

async function expectLayout(page: Page, label: string) {
  const r = await layoutProblems(page);
  expect(r.scrollWidth, `${label} scrolls sideways (${r.scrollWidth} > ${r.width})`).toBeLessThanOrEqual(r.width + 1);
  expect(r.clipped, `${label}: clipped text:\n  ${r.clipped.join("\n  ")}`).toEqual([]);
}

for (const url of pageUrls()) {
  test(`resize text to 200%: ${url} keeps its layout`, async ({ page }) => {
    await prepare(page);
    await open(page, await resolveUrl(page, url));
    await page.evaluate(() => {
      document.documentElement.style.fontSize = "200%";
    });
    await expectLayout(page, `${url} at 200% root font size`);
  });

  test(`text spacing 1.4.12: ${url} keeps its layout`, async ({ page }) => {
    await prepare(page);
    await open(page, await resolveUrl(page, url));
    await page.addStyleTag({ content: TEXT_SPACING_CSS });
    await expectLayout(page, `${url} with text-spacing styles`);
  });
}
