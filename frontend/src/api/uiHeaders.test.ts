import { describe, expect, it } from "vitest";
import { uiRequestHeaders } from "./client";

describe("dashboard request headers", () => {
  it("declare the dashboard version next to the dashboard marker", () => {
    const h = uiRequestHeaders();
    expect(h["X-Requested-With"]).toBe("ncc-ui");
    expect(h["X-NCC-UI-Version"]).toMatch(/^\d+\.\d+\.\d+/);
  });

  it("add the CSRF token only to changing requests", () => {
    document.cookie = "ncc_csrf=tok123; path=/";
    expect(uiRequestHeaders("GET")["X-CSRF-Token"]).toBeUndefined();
    expect(uiRequestHeaders("post")["X-CSRF-Token"]).toBe("tok123");
  });

  // The API refuses a request that claims to be the dashboard without a version
  // (anything built before 2.4.0 looks like that). A hand-written
  // X-Requested-With header elsewhere reintroduces the bug where the API
  // explorer and other pages were refused as "an old dashboard".
  it("is the only place that writes the dashboard marker header", () => {
    const files = import.meta.glob("/src/**/*.{ts,tsx}", { query: "?raw", import: "default", eager: true }) as Record<string, string>;
    const offenders = Object.entries(files)
      .filter(([path, text]) => !path.endsWith(".test.ts") && !path.endsWith(".test.tsx") && !path.endsWith("/api/client.ts"))
      .filter(([, text]) => text.includes("X-Requested-With"))
      .map(([path]) => path);
    expect(offenders).toEqual([]);
  });
});
