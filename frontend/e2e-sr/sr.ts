// Helpers for the screen reader tests. NVDA and VoiceOver word things
// differently ("Overview, heading, level 1" against "Overview, heading level
// 1"), so every check is a tolerant pattern on the lower-cased phrase, never
// an exact string.
import { NVDAKeyCodeCommands, WindowsKeyCodes, WindowsModifiers } from "@guidepup/guidepup";
import { screenReaderTest } from "@guidepup/playwright";
import { expect, type Page } from "@playwright/test";
import { prepare, settled } from "../e2e/helpers";

export const test = screenReaderTest;
export { expect };

type ScreenReaderFixture = Parameters<Parameters<typeof screenReaderTest>[2]>[0]["screenReader"];
export type SR = ScreenReaderFixture;

/** What the screen reader said so far, lower-cased, one phrase per line. */
export async function heard(sr: SR): Promise<string> {
  return (await sr.spokenPhraseLog()).join("\n").toLowerCase();
}

/** Every pattern must match; on failure show what was said. */
export function expectSpoken(said: string, what: string, ...patterns: RegExp[]) {
  for (const p of patterns) expect(said, `${what}: expected the screen reader to say ${p}. It said:\n${said}`).toMatch(p);
}

/**
 * Press "next item" until the last phrase matches (or give up after `max`
 * steps). Returns the phrase that matched. The failure lists what was read.
 */
export async function reach(sr: SR, pattern: RegExp, max = 80): Promise<string> {
  for (let i = 0; i < max; i++) {
    const said = (await sr.lastSpokenPhrase()).toLowerCase();
    if (pattern.test(said)) return said;
    await sr.next();
  }
  const said = (await sr.lastSpokenPhrase()).toLowerCase();
  if (pattern.test(said)) return said;
  throw new Error(`never reached ${pattern} in ${max} steps. Heard:\n${(await sr.spokenPhraseLog()).join("\n")}`);
}

/** Same, with heading navigation. */
export async function reachHeading(sr: SR, pattern: RegExp, max = 12): Promise<string> {
  for (let i = 0; i < max; i++) {
    await sr.nextHeading();
    const said = (await sr.lastSpokenPhrase()).toLowerCase();
    if (pattern.test(said)) return said;
  }
  throw new Error(`no heading matching ${pattern} in ${max} headings. Heard:\n${(await sr.spokenPhraseLog()).join("\n")}`);
}

/** The workflow runs NVDA on Windows and VoiceOver on macOS. */
const onNVDA = process.platform === "win32";

/**
 * Guidepup's navigateToWebContent clicks the middle of the page to give it
 * focus. On Runs or Overview that click lands on a row and opens it.
 * VoiceOver: cover the page with an empty, hidden layer for the click, then
 * take it away. NVDA loses its place when that layer goes, so on NVDA this is
 * used only on the sign-in page, where the click is harmless; after that,
 * nvdaToTop moves NVDA without clicking.
 */
async function enterPage(page: Page, sr: SR) {
  if (onNVDA) {
    await sr.navigateToWebContent();
    return;
  }
  await page.evaluate(() => {
    const cover = document.createElement("div");
    cover.id = "sr-click-guard";
    cover.setAttribute("aria-hidden", "true");
    cover.style.cssText = "position:fixed;inset:0;z-index:2147483647;background:transparent";
    document.body.appendChild(cover);
  });
  try {
    await sr.navigateToWebContent();
  } finally {
    await page.evaluate(() => document.getElementById("sr-click-guard")?.remove());
  }
}

/**
 * Wait for the page to be ready, then put the screen reader at the top of it.
 * Focus is taken off any control first: on a sidebar menu item NVDA stays in
 * focus mode, where heading and item navigation keys go to the menu instead.
 */
export async function startAt(page: Page, sr: SR) {
  await settled(page);
  await page.getByRole("heading", { level: 1 }).first().waitFor();
  if (onNVDA) {
    await nvdaToTop(page, sr);
  } else {
    await page.evaluate(() => {
      (document.activeElement as HTMLElement | null)?.blur();
      window.scrollTo(0, 0);
    });
    await enterPage(page, sr);
  }
  await sr.clearSpokenPhraseLog();
}

/**
 * NVDA already has the browser (from signing in). Put focus on the main
 * region, then make sure NVDA is in browse mode: after a focus change it can
 * stay in focus mode, where heading keys are typed as letters ("h"). The
 * browse/focus key toggles, so probe with "next heading" and toggle only when
 * no heading is read. Then move to the top of the page.
 */
async function nvdaToTop(page: Page, sr: SR) {
  const toTop = { keyCode: [WindowsKeyCodes.Home], modifiers: [WindowsModifiers.Control] };
  await page.bringToFront();
  await page.evaluate(() => {
    (document.activeElement as HTMLElement | null)?.blur();
    document.getElementById("main-content")?.focus({ preventScroll: true });
    window.scrollTo(0, 0);
  });
  let browsing = false;
  for (let i = 0; i < 3 && !browsing; i++) {
    await sr.perform(toTop, { capture: false });
    await sr.nextHeading();
    browsing = /\bheading\b/i.test(await sr.lastSpokenPhrase());
    if (!browsing) {
      await sr.perform(NVDAKeyCodeCommands.toggleBetweenBrowseAndFocusMode, { capture: false });
      await page.waitForTimeout(300);
    }
  }
  if (!browsing) throw new Error(`NVDA did not enter browse mode. Heard:\n${(await sr.spokenPhraseLog()).join("\n")}`);
  await sr.perform(toTop, { capture: false });
  await page.waitForTimeout(200);
}

/**
 * Wait until the screen reader has said something matching `pattern`.
 * VoiceOver only logs what commands return, so the last phrase is read too:
 * that is where a live region announcement shows up.
 */
export async function waitToHear(sr: SR, pattern: RegExp, message: string, timeout = 15_000) {
  await expect
    .poll(async () => `${(await sr.lastSpokenPhrase()).toLowerCase()}\n${await heard(sr)}`, { timeout, message })
    .toMatch(pattern);
}

/** "Overview, heading, level 1" (NVDA) or "heading level 1 Overview" (VoiceOver). */
export function h1Pattern(name: string): RegExp {
  return new RegExp(`^(?=.*\\b${name}\\b)(?=.*level\\s*1\\b)`, "i");
}

/**
 * Open the demo signed out and sign in with the keyboard only: the username
 * field has focus on arrival; type, Tab, type, Enter (the demo's account is
 * demo / demo).
 */
export async function signInWithKeyboard(page: Page, sr: SR) {
  await prepare(page, { as: null });
  await page.goto("./#/overview", { waitUntil: "load" });
  const user = page.getByRole("textbox", { name: "Username" });
  await user.waitFor();
  await enterPage(page, sr);
  // Moving the screen reader into the page can move focus (VoiceOver does):
  // put it back on the field the page focuses on arrival.
  await user.focus();
  await expect(user).toBeFocused();
  await page.keyboard.type("demo");
  await expect(user).toHaveValue("demo");
  await page.keyboard.press("Tab");
  const password = page.getByLabel("Password", { exact: true });
  await expect(password).toBeFocused();
  await page.keyboard.type("demo");
  await expect(password).toHaveValue("demo");
  await page.keyboard.press("Enter");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Overview");
  await settled(page);
}

/** Go to a page through the sidebar, as a keyboard user would, and wait for its h1. */
export async function goTo(page: Page, label: string) {
  const item = page.locator(".ui-sidebar").getByRole("menuitem", { name: label });
  await item.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(label);
  await settled(page);
}
