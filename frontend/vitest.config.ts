import { configDefaults, defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  test: {
    // e2e/ holds the Playwright browser specs (npm run test:a11y), which Vitest must not load.
    exclude: [...configDefaults.exclude, "e2e/**"],
    environment: "jsdom",
    environmentOptions: {
      jsdom: {
        url: "http://localhost/",
      },
    },
    setupFiles: "./src/test/setup.ts",
    // Above the waitFor timeout in setup.ts, so a wait that does fail reports
    // its own error instead of the test's.
    testTimeout: 20_000,
  },
});
