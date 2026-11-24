import { createRequire } from "node:module";
import { expect, type Page } from "@playwright/test";
import type { AxeResults } from "axe-core";

const axeSource = createRequire(import.meta.url).resolve("axe-core/axe.min.js");

export const THEMES = ["light", "dark", "it-pro"] as const;
export type Theme = (typeof THEMES)[number];

export const ACCOUNTS = { admin: "demo", operator: "alex.ops", viewer: "sam.noc" } as const;
const ROLE_OF = { demo: "admin", "alex.ops": "operator", "sam.noc": "viewer" } as const;

/** WCAG 2.0, 2.1 and 2.2, levels A and AA. */
export const WCAG_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];

/**
 * Open the demo signed in (or signed out) with a theme. The demo keeps its
 * session in sessionStorage and the theme is the JSON string in localStorage
 * "app.theme"; both are set before the app starts.
 */
export async function prepare(page: Page, opts: { theme?: Theme; as?: keyof typeof ACCOUNTS | null } = {}) {
  const theme = opts.theme ?? "light";
  const as = opts.as === undefined ? "admin" : opts.as;
  const user = as ? { username: ACCOUNTS[as], role: ROLE_OF[ACCOUNTS[as]] } : null;
  await page.addInitScript(
    ([t, u]) => {
      try {
        localStorage.setItem("app.theme", JSON.stringify(t));
        // Skip first-visit overlays so they do not cover the page being scanned.
        sessionStorage.setItem("ncc-demo-state-v1", JSON.stringify({ user: u }));
      } catch {
        /* storage blocked: the test will fail on its own */
      }
    },
    [theme, user] as const,
  );
}

/** Anything that means "still loading": skeletons, spinners, busy regions, the route loader. */
const LOADING = ".ant-skeleton, .ant-spin-spinning, .ant-spin-dot, [aria-busy='true'], .ui-route-loading, .ui-loader, [data-loading='true']";

/**
 * Wait until the page has stopped loading: content is there and no skeleton,
 * spinner or busy region has been on screen for 500 ms. (A route first shows
 * its loader, then a skeleton while data arrives; the gap between the two must
 * not count as "done".)
 */
export async function settled(page: Page) {
  await page.waitForLoadState("load");
  await page.evaluate(() => {
    (window as unknown as { __quietSince?: number }).__quietSince = 0;
  });
  await page.waitForFunction(
    (loading) => {
      const w = window as unknown as { __quietSince?: number };
      const root = document.querySelector("main") ?? document.body;
      const busy = !!document.querySelector(loading) || !(root.textContent ?? "").trim();
      const now = performance.now();
      if (busy) {
        w.__quietSince = 0;
        return false;
      }
      if (!w.__quietSince) w.__quietSince = now;
      return now - w.__quietSince >= 500;
    },
    LOADING,
    { timeout: 20_000, polling: 100 },
  );
  await page.evaluate(() => document.fonts?.ready);
}

/** Open a hash route (the demo uses a hash router). */
export async function open(page: Page, path: string) {
  await page.goto(`./#${path}`);
  await settled(page);
}

/**
 * A real run id and cluster address from the demo data. Run ids come from the
 * time the demo was loaded, so ask the page that will be used (never cache).
 */
export async function demoIds(page: Page): Promise<{ run: string; cluster: string }> {
  if (!page.url().includes("#/")) {
    await page.goto("./#/overview");
    await settled(page);
  }
  return page.evaluate(async () => {
    const runs = (await (await fetch("/api/v1/runs")).json()).data as Array<{ id?: string; run_id?: string }>;
    const report = (await (await fetch("/api/v1/report/data")).json()).data as { ncc_cluster_summary?: Array<{ address?: string }> };
    const first = runs[0];
    return {
      run: String(first.id ?? first.run_id),
      cluster: String(report.ncc_cluster_summary?.[0]?.address),
    };
  });
}

export async function resolveUrl(page: Page, url: string): Promise<string> {
  if (!url.includes(":run") && !url.includes(":cluster")) return url;
  const d = await demoIds(page);
  return url.replace(":run", encodeURIComponent(d.run)).replace(":cluster", encodeURIComponent(d.cluster));
}

export type Exclusion = {
  rule: string;
  /** A CSS selector the offending element must match. */
  selector: string;
  /** Only on pages whose address matches. */
  page: RegExp;
  reason: string;
};

/**
 * Narrow, commented exclusions: one rule, one selector, one page. Never add a
 * rule without a selector. Each one is a known violation we cannot fix in this
 * change, so it needs a reason and a way out.
 */
export const EXCLUSIONS: Exclusion[] = [];

async function runAxe(page: Page, include?: string): Promise<AxeResults> {
  const loaded = await page.evaluate(() => "axe" in window);
  if (!loaded) await page.addScriptTag({ path: axeSource });
  return page.evaluate(
    ([tags, ctx]) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const axe = (window as any).axe;
      return axe.run(ctx ? { include: [ctx] } : document, { runOnly: { type: "tag", values: tags }, resultTypes: ["violations", "passes"] });
    },
    [WCAG_TAGS, include ?? null] as const,
  );
}

/** Scan the page (or one region) and fail with a readable list of violations. */
export async function expectNoViolations(page: Page, label: string, opts: { include?: string } = {}) {
  const results = await runAxe(page, opts.include);
  const url = page.url();
  const live: AxeResults["violations"] = [];
  for (const v of results.violations) {
    let nodes = v.nodes;
    for (const e of EXCLUSIONS) {
      if (e.rule !== v.id || !e.page.test(url)) continue;
      const excluded = await page.evaluate(
        ([targets, sel]) => targets.map((t) => document.querySelector(t)?.matches(sel) ?? false),
        [nodes.map((n) => String(n.target[0])), e.selector] as const,
      );
      nodes = nodes.filter((_, i) => !excluded[i]);
    }
    if (nodes.length > 0) live.push({ ...v, nodes });
  }
  const text = live
    .map((v) => `${v.id} (${v.impact}): ${v.help}\n` + v.nodes.slice(0, 5).map((n) => `    ${n.target.join(" ")}\n      ${(n.failureSummary ?? "").replace(/\n/g, " | ")}`).join("\n"))
    .join("\n");
  expect.soft(live.length, `${label}: ${live.length} axe violation(s)\n${text}`).toBe(0);
  // Guard against a scan that silently checked nothing.
  expect(results.passes.length, `${label}: axe ran no rules`).toBeGreaterThan(10);
  expect(results.passes.some((p) => p.id === "color-contrast"), `${label}: colour contrast was not evaluated`).toBe(true);
}
