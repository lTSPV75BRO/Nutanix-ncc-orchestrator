import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import axe from "axe-core";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AuthContext, type AuthValue } from "../auth/AuthContext";
import { OverviewPage } from "./OverviewPage";

vi.mock("../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

const admin: AuthValue = { me: null, role: "admin", isAdmin: true, canOperate: true, loginEnabled: true, authenticated: true };
const ok = (data: unknown) =>
  ({ ok: true, status: 200, statusText: "OK", headers: new Headers({ "content-type": "application/json" }), json: async () => ({ success: true, data }) }) as Response;

function renderOverview(report: unknown) {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/api/v1/report/data")) return ok(report);
    if (url.includes("/api/v1/runs/active")) return ok({ active: false });
    if (url.includes("/api/v1/runs")) return ok([]);
    if (url.includes("/api/v1/onboarding")) return ok({ steps: [], required_done: 0, required_total: 0, completed: true, dismissed: false });
    return ok({});
  }) as typeof fetch;
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <AuthContext.Provider value={admin}>
        <MemoryRouter>
          <OverviewPage />
        </MemoryRouter>
      </AuthContext.Provider>
    </QueryClientProvider>,
  );
}

async function violations(container: HTMLElement) {
  const r = await axe.run(container, {
    runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa", "best-practice"] },
    rules: { "color-contrast": { enabled: false }, region: { enabled: false } },
  });
  return r.violations.map((v) => `${v.id}: ${v.help} (${v.nodes.map((n) => n.target.join(" ")).join("; ")})`);
}

const recent = new Date(Date.now() - 30 * 60_000).toISOString();
const report = {
  run_summary: {
    timestamp: recent,
    duration_s: 125,
    clusters_ok: 2,
    clusters_failed: 1,
    clusters: [
      { address: "10.0.0.1", ok: true, warn_count: 2, health_score: 95 },
      { address: "10.0.0.2", ok: true, fail_count: 3, health_score: 60 },
      { address: "10.0.0.3", ok: false, error: "connection refused" },
    ],
  },
  ncc_summary_counts: { total_plugins: 100, pass: 90, fail: 3, error: 1, warn: 4, info: 2, unknown: 0 },
  checks_snapshot: [],
  regression_summary: { has_regression: true, delta_fail_total: 2 },
  policy_violations: [],
};

describe("OverviewPage accessibility (axe-core; colour contrast needs a real browser)", () => {
  afterEach(() => cleanup());
  it("empty state has no violations", async () => {
    const { container } = renderOverview({ run_summary: {}, ncc_summary_counts: {}, checks_snapshot: [] });
    await screen.findByText("No results yet");
    expect(await violations(container)).toEqual([]);
  });
  it("populated overview has no violations", async () => {
    const { container } = renderOverview(report);
    await screen.findAllByText(/10\.0\.0\.2/);
    expect(await violations(container)).toEqual([]);
  });
});
