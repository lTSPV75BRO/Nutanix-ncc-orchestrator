import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, within } from "@testing-library/react";
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

function ok(data: unknown) {
  return {
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({ success: true, data }),
  } as Response;
}

function renderOverview(report: unknown, runs: unknown[] = [], freshness: unknown = {}, auth: AuthValue = admin) {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/api/v1/freshness")) return ok(freshness);
    if (url.includes("/api/v1/report/data")) return ok(report);
    if (url.includes("/api/v1/runs/active")) return ok({ active: false });
    if (url.includes("/api/v1/runs")) return ok(runs);
    if (url.includes("/api/v1/onboarding")) return ok({ steps: [], required_done: 0, required_total: 0, completed: true, dismissed: false });
    return ok({});
  }) as typeof fetch;
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <AuthContext.Provider value={auth}>
        <MemoryRouter>
          <OverviewPage />
        </MemoryRouter>
      </AuthContext.Provider>
    </QueryClientProvider>,
  );
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

describe("OverviewPage", () => {
  afterEach(() => cleanup());

  it("says there are no results yet and offers the next steps", async () => {
    renderOverview({ run_summary: {}, ncc_summary_counts: {}, checks_snapshot: [] });
    expect(await screen.findByText("No results yet")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Start a run" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Configure clusters" })).toBeInTheDocument();
    expect(screen.queryByText("0%")).not.toBeInTheDocument();
  });

  it("summarises the latest run and links into Findings", async () => {
    renderOverview(report, [{ id: "r1", mod_time: recent, has_index: true, status: "partial", duration_s: 125, clusters_ok: 2, clusters_failed: 1 }]);
    expect(await screen.findByText("Health score")).toBeInTheDocument();
    // The Clusters tile and the matching recent run both read 2/3.
    expect(screen.getAllByText("2/3").length).toBeGreaterThan(0);
    expect(screen.getByText("1 could not be checked")).toBeInTheDocument();
    expect(screen.getByText("Regression since the previous run")).toBeInTheDocument();

    expect(screen.getByRole("link", { name: /Fail\s*3/ })).toHaveAttribute("href", "/findings?sev=FAIL");

    // Unreachable first, then the cluster with failures, then warnings.
    const clusters = screen.getAllByRole("link").filter((a) => a.getAttribute("href")?.startsWith("/findings?clusters="));
    expect(clusters.map((a) => decodeURIComponent(a.getAttribute("href")!.split("=")[1]))).toEqual([
      "10.0.0.3",
      "10.0.0.2",
      "10.0.0.1",
    ]);
    expect(within(clusters[0]).getByText("Not checked")).toBeInTheDocument();

    expect(await screen.findByText("Partial")).toBeInTheDocument();
  });

  it("shows how old each cluster's data is and adds clusters from other environments", async () => {
    const hoursAgo = (h: number) => new Date(Date.now() - h * 3600_000).toISOString();
    renderOverview(report, [], {
      generated_at: recent,
      schedules: [],
      schedule_overdue: false,
      clusters: [
        { address: "10.0.0.1", last_checked_at: recent, ok: true, last_success_at: recent, fail_count: 0, warn_count: 2, err_count: 0 },
        { address: "10.0.0.3", last_checked_at: recent, ok: false, error: "connection refused", last_success_at: hoursAgo(72), failed_runs: 4, fail_count: 0, warn_count: 0, err_count: 0 },
        { address: "dev-pe", environment: "dev", last_checked_at: hoursAgo(30), ok: true, last_success_at: hoursAgo(30), fail_count: 1, warn_count: 0, err_count: 0 },
      ],
    });
    const row = async (name: string) =>
      (await screen.findAllByRole("link")).find((a) => a.getAttribute("href") === `/findings?clusters=${encodeURIComponent(name)}`)!;

    const down = await row("10.0.0.3");
    expect(within(down).getByText("connection refused · data from 3d ago")).toBeInTheDocument();
    expect(down.querySelector(".overview-cluster-age")?.getAttribute("title")).toMatch(/The last 4 runs could not check it/);

    const dev = await row("dev-pe");
    expect(within(dev).getByText("1 fail")).toBeInTheDocument();
    const age = dev.querySelector(".overview-cluster-age");
    expect(age).toHaveTextContent("1d ago");
    expect(age).toHaveClass("is-old");
    expect(screen.getByText("4 clusters, each from its newest run")).toBeInTheDocument();
    expect(screen.queryByText("Scheduled runs have stopped")).not.toBeInTheDocument();
  });

  it("warns when a schedule has stopped", async () => {
    const freshness = {
      generated_at: recent,
      clusters: [],
      schedule_overdue: true,
      schedules: [
        { kind: "main", every: "4h", interval_s: 14400, last_run_at: new Date(Date.now() - 13 * 3600_000).toISOString(), overdue: true },
        { kind: "environment", environment: "prod", every: "1d", interval_s: 86400, overdue: false },
      ],
    };
    renderOverview(report, [], freshness);
    expect(await screen.findByText("Scheduled runs have stopped")).toBeInTheDocument();
    expect(screen.getByText(/The schedule runs every 4 hours, but the last run was 13h ago\./)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Check the schedule" })).toHaveAttribute("href", "/settings/schedule");
    cleanup();

    const viewer: AuthValue = { ...admin, role: "viewer", isAdmin: false, canOperate: false };
    renderOverview(report, [], freshness, viewer);
    expect(await screen.findByText("Scheduled runs have stopped")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Check the schedule" })).not.toBeInTheDocument();
  });
});
