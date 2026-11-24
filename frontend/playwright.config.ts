// Browser accessibility checks (A11Y-02). They run against the demo build
// (vite.demo.config.ts, fixture data, no backend). See docs/ui/DESIGN_SYSTEM.md.
//
//   npm run test:a11y                         build the demo, serve it, run everything
//   A11Y_CHANNEL=msedge npm run test:a11y     use the installed Edge, nothing to download
//
// CI installs Playwright's Chromium instead (`npx playwright install --with-deps chromium`).
import { defineConfig } from "@playwright/test";

const PORT = Number(process.env.A11Y_PORT ?? 5230);
const channel = process.env.A11Y_CHANNEL || undefined;

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  outputDir: "test-results",
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : [["list"]],
  fullyParallel: true,
  workers: process.env.CI ? 4 : undefined,
  retries: 0,
  timeout: 180_000, // CI runners are slower; a page test also visits every tab
  expect: { timeout: 10_000 },
  use: {
    baseURL: `http://127.0.0.1:${PORT}/`,
    channel,
    viewport: { width: 1280, height: 800 },
    // Animations (intro, drawer slides) would make contrast and focus checks race.
    reducedMotion: "reduce",
    trace: "retain-on-failure",
  },
  webServer: {
    // The demo build is made by `npm run test:a11y`; this only serves it.
    command: `npx vite preview --config vite.demo.config.ts --host 127.0.0.1 --port ${PORT} --strictPort`,
    url: `http://127.0.0.1:${PORT}/`,
    reuseExistingServer: false,
    timeout: 60_000,
  },
});
