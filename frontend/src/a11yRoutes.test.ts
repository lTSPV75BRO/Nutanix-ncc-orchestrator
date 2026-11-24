// The browser accessibility suite (frontend/e2e, see docs/ui/DESIGN_SYSTEM.md)
// lists every route in e2e/routes.ts. This fails when a route is added to
// App.tsx, or a Settings/System section to navigation.tsx, without being added
// there, so a new page cannot slip past the accessibility scan.
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { ROUTES, SECTION_URLS, pageUrls } from "../e2e/routes";
import { OVERVIEW_PATH, PAGES, SECTIONS } from "./navigation";

const SRC = dirname(fileURLToPath(import.meta.url));
const appSource = readFileSync(join(SRC, "App.tsx"), "utf8");

/** The `path` of every <Route> in App.tsx, with OVERVIEW_PATH resolved. */
function appRoutes(): string[] {
  const out: string[] = [];
  for (const m of appSource.matchAll(/<Route\s+path=(?:"([^"]+)"|\{([A-Z_]+)\})/g)) {
    out.push(m[1] ?? (m[2] === "OVERVIEW_PATH" ? OVERVIEW_PATH : `UNRESOLVED:${m[2]}`));
  }
  return out;
}

describe("accessibility suite route list", () => {
  it("finds the routes in App.tsx", () => {
    expect(appRoutes().length).toBeGreaterThan(8);
  });

  it("covers every <Route> in App.tsx", () => {
    expect(ROUTES.map((r) => r.pattern).sort()).toEqual(appRoutes().sort());
  });

  it("opens every Settings/System section and every top-level page", () => {
    const urls = pageUrls();
    for (const s of SECTIONS) expect(SECTION_URLS, `section ${s.key}`).toContain(s.path);
    for (const p of PAGES) expect(urls, `page ${p.path}`).toContain(p.path);
  });

  it("gives every route a concrete address that matches its pattern", () => {
    for (const r of ROUTES) {
      if (r.pattern === "*") continue;
      const re = new RegExp(`^${r.pattern.replace(/:[a-z]+/g, "[^/]+")}$`);
      expect(r.url, r.pattern).toMatch(re);
      if (r.kind === "redirect") expect(r.lands, r.pattern).toBeTruthy();
    }
  });
});
