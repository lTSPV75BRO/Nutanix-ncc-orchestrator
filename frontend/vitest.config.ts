import { configDefaults, defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  test: {
    // e2e/ and e2e-sr/ hold the Playwright browser specs (test:a11y, test:sr), which Vitest must not load.
    exclude: [...configDefaults.exclude, "e2e/**", "e2e-sr/**"],
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
