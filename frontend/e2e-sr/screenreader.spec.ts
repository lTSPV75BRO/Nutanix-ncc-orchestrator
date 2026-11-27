// What a real screen reader says (A11Y-03). The browser suite in ../e2e checks
// the DOM; this checks the announcements, through NVDA (Windows) or VoiceOver
// (macOS) driven by Guidepup. Each test signs in with the keyboard first.
import { expect, expectSpoken, goTo, h1Pattern, heard, reach, reachHeading, signInWithKeyboard, startAt, test, waitToHear } from "./sr";

test.describe("screen reader on the demo build", () => {
  test("Overview, Findings and Runs: title, h1 and main landmark are announced", async ({ page, screenReader }) => {
    await signInWithKeyboard(page, screenReader);

    for (const name of ["Overview", "Findings", "Runs"]) {
      if (name !== "Overview") await goTo(page, name);
      // The window title changes with the page (it is what is read on switching windows).
      await expect.poll(() => page.title()).toContain(name);

      // Heading navigation finds the page's h1 and says it is a level 1 heading.
      await startAt(page, screenReader);
      const h1 = await reachHeading(screenReader, h1Pattern(name));
      expectSpoken(h1, `${name} h1`, /heading/);

      // Landmark navigation reaches the main region and announces it as one.
      await startAt(page, screenReader);
      const seen: string[] = [];
      for (let i = 0; i < 10 && !seen.some((s) => /\bmain\b/.test(s)); i++) {
        await screenReader.nextLandmark();
        seen.push((await screenReader.lastSpokenPhrase()).toLowerCase());
      }
      expect(seen.join("\n"), `${name}: no main landmark among ${JSON.stringify(seen)}`).toMatch(/\bmain\b/);
      // The labelled sidebar ("Primary") is announced on the way.
      expect(await heard(screenReader), `${name}: sidebar landmark`).toMatch(/primary|navigation/);
    }
  });

  test("severity filter buttons say their name, count and pressed state", async ({ page, screenReader }) => {
    await signInWithKeyboard(page, screenReader);
    await goTo(page, "Findings");
    await startAt(page, screenReader);

    // "Fail 7. Filter by Fail., toggle button, not pressed" (NVDA); VoiceOver
    // says "fail 7. filter by fail. toggle button" and names no state when off.
    const off = await reach(screenReader, /\bfail\b.*\d.*filter by fail/);
    expectSpoken(off, "Fail filter, off", /toggle|button/);
    expect(off, "Fail filter announced as pressed before it was pressed").not.toMatch(/(?<!not |un)pressed|(?<!not )selected|\bon\b/);
    // The next severity is a filter too.
    await screenReader.next();
    expectSpoken((await screenReader.lastSpokenPhrase()).toLowerCase(), "next severity filter", /filter by/, /toggle|button/);

    // Back to Fail, press it, read it again.
    await screenReader.previous();
    await reach(screenReader, /\bfail\b.*\d.*filter by fail/, 4);
    await screenReader.act();
    await expect(page.locator("button.severity-total-pill[aria-pressed=true]")).toHaveCount(1);
    await screenReader.next();
    await screenReader.previous();
    const on = (await screenReader.lastSpokenPhrase()).toLowerCase();
    expectSpoken(on, "Fail filter, on", /\bfail\b/, /\bpressed\b|\bon\b|\bselected\b/);
    expect(on, "still announced as not pressed").not.toMatch(/not pressed|unpressed|\boff\b|not selected/);
  });

  test("a table is announced as a table and its column headers are read", async ({ page, screenReader }) => {
    await signInWithKeyboard(page, screenReader);
    await goTo(page, "Runs");
    await startAt(page, screenReader);

    // "table with 8 columns and 20 rows" (NVDA), "table, 8 columns" (VoiceOver).
    // antd tables have no <caption>; if one is added it is spoken here too.
    const entry = await reach(screenReader, /\btable\b/, 120);
    expectSpoken(entry, "table entry", /\d/);
    // Walk the header row: each column header is read.
    for (let i = 0; i < 14; i++) await screenReader.next();
    expectSpoken(await heard(screenReader), "Runs table headers", /started/, /status/, /environment/, /duration/, /findings/);
  });

  test("a toast is announced and focus stays where it was", async ({ page, screenReader }) => {
    await signInWithKeyboard(page, screenReader);
    await goTo(page, "Findings");
    await startAt(page, screenReader);

    const button = page.getByRole("button", { name: "Copy link" });
    await button.focus();
    await screenReader.clearSpokenPhraseLog();
    await page.keyboard.press("Enter");
    await expect(page.locator(".ant-notification-notice").first()).toBeVisible();
    // "Link to these findings copied" (clipboard allowed) or "Copy this link" (not): both say "link".
    await waitToHear(screenReader, /link/, "toast text was never spoken");
    // The toast is a live region, not a focus move.
    await expect(button).toBeFocused();
  });
});
