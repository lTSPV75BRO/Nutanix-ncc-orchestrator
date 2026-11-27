// Real screen reader tests (A11Y-03): NVDA on Windows, VoiceOver on macOS,
// driven by Guidepup against the demo build. They only run where a screen
// reader can be started (the screenreader.yml CI workflow, or a machine you
// have set up yourself with `npx @guidepup/setup`). See docs/ui/DESIGN_SYSTEM.md.
//
//   npm run test:sr          builds the demo, then runs this config
//
// `npx playwright test --list -c e2e-sr/playwright.config.ts` only loads the
// tests and needs no screen reader.
import { devices } from "@playwright/test";
import { withGuidepup } from "@guidepup/playwright";

const PORT = Number(process.env.SR_PORT ?? 5231);
const mac = process.platform === "darwin";

export default withGuidepup({
  testDir: ".",
  testMatch: "**/*.spec.ts",
  outputDir: "../test-results-sr",
  reporter: process.env.CI ? [["list"], ["html", { open: "never", outputFolder: "../playwright-report-sr" }]] : [["list"]],
  reportSlowTests: null,
  // A screen reader session is slow: every announcement is a round trip.
  timeout: 5 * 60 * 1000,
  expect: { timeout: 15_000 },
  // One retry in CI for the hosted-runner screen reader start-up, no more.
  retries: process.env.CI ? 1 : 0,
  use: {
    baseURL: `http://127.0.0.1:${PORT}/`,
    reducedMotion: "reduce",
    trace: "retain-on-failure",
  },
  // NVDA is driven in Chromium, VoiceOver in WebKit (Safari's engine).
  projects: [mac ? { name: "voiceover-webkit", use: { ...devices["Desktop Safari"] } } : { name: "nvda-chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    // The demo build is made by `npm run test:sr`; this only serves it.
    command: `npx vite preview --config vite.demo.config.ts --host 127.0.0.1 --port ${PORT} --strictPort`,
    cwd: "..",
    url: `http://127.0.0.1:${PORT}/`,
    reuseExistingServer: false,
    timeout: 60_000,
  },
});
