// Helpers for the screen reader tests. NVDA and VoiceOver word things
// differently ("Overview, heading, level 1" against "Overview, heading level
// 1"), so every check is a tolerant pattern on the lower-cased phrase, never
// an exact string.
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

/** Wait for the page to be ready, then put the screen reader at the top of it. */
export async function startAt(page: Page, sr: SR) {
  await settled(page);
  await page.getByRole("heading", { level: 1 }).first().waitFor();
  await sr.navigateToWebContent();
  await sr.clearSpokenPhraseLog();
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
  await expect(user).toBeFocused();
  await sr.navigateToWebContent();
  await page.keyboard.type("demo");
  await page.keyboard.press("Tab");
  await page.keyboard.type("demo");
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
