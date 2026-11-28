// Every route the browser accessibility suite covers. src/a11yRoutes.test.ts
// (Vitest) checks this list against the <Route path> entries in src/App.tsx and
// the section paths in src/navigation.tsx, so a new route cannot be added
// without being listed here.

export type RouteCase = {
  /** The pattern as written in App.tsx. */
  pattern: string;
  /** A concrete address to open. `:run` and `:cluster` are filled from the demo data. */
  url: string;
  /** `page` is scanned; `redirect` must end up at `lands`. */
  kind: "page" | "redirect";
  lands?: string;
};

export const ROUTES: RouteCase[] = [
  { pattern: "/", url: "/", kind: "redirect", lands: "/overview" },
  { pattern: "/overview", url: "/overview", kind: "page" },
  { pattern: "/findings", url: "/findings", kind: "page" },
  { pattern: "/login", url: "/login", kind: "redirect", lands: "/overview" },
  { pattern: "/insights", url: "/insights", kind: "page" },
  { pattern: "/integrations", url: "/integrations", kind: "page" },
  { pattern: "/integrations/:id", url: "/integrations/servicenow", kind: "page" },
  { pattern: "/runs", url: "/runs", kind: "page" },
  { pattern: "/runs/:id", url: "/runs/:run", kind: "page" },
  { pattern: "/clusters/:key", url: "/clusters/:cluster", kind: "page" },
  { pattern: "/settings", url: "/settings", kind: "redirect", lands: "/settings/config" },
  { pattern: "/settings/:section", url: "/settings/config", kind: "page" },
  { pattern: "/system/:section", url: "/system/status", kind: "page" },
  { pattern: "*", url: "/no-such-page", kind: "page" },
];

/** Every Settings/System section, opened through `/settings/:section` or `/system/:section`. */
export const SECTION_URLS = [
  "/runs",
  "/settings/config",
  "/settings/schedule",
  "/settings/notifications",
  "/settings/access",
  "/settings/features",
  "/system/status",
  "/system/health",
  "/system/updates",
  "/system/logs",
  "/system/audit",
  "/system/developer",
];

/** Text each data page must show before it is scanned, so a scan never runs on an empty or loading page. */
export const POPULATED: Record<string, RegExp> = {
  "/overview": /prod-/,
  "/findings": /vg_space_usage_check/,
  "/insights": /Health score/,
  "/integrations": /ServiceNow/,
  "/integrations/servicenow": /ServiceNow/,
  "/runs": /\d+ runs/,
  "/runs/:run": /prod-/,
  "/clusters/:cluster": /prod-/,
  "/settings/config": /prod-|Cluster/,
  "/settings/schedule": /Schedule/,
  "/settings/notifications": /Slack/,
  "/settings/access": /alex\.ops/,
  "/system/status": /Version|version/,
  "/system/health": /Check again/,
  "/system/updates": /Update/,
  "/system/logs": /Runner|runner/,
  "/system/audit": /auth\.login|Sign/i,
  "/no-such-page": /not found|doesn.t exist/i,
};

/** Pages scanned as admin: each `page` route plus every section. */
export function pageUrls(): string[] {
  const urls = ROUTES.filter((r) => r.kind === "page").map((r) => r.url);
  for (const s of SECTION_URLS) if (!urls.includes(s)) urls.push(s);
  return urls;
}
