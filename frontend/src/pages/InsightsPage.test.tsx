import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { InsightsPage } from "./InsightsPage";

vi.mock("../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

function ok(data: unknown) {
  return {
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({ success: true, data }),
  } as Response;
}

function Where() {
  const l = useLocation();
  return <div data-testid="where">{l.pathname + l.search}</div>;
}

function renderInsights(report: unknown, path = "/insights") {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/api/v1/features")) return ok({ insights: true });
    if (url.includes("/api/v1/report/trends")) return ok({ points: [] });
    if (url.includes("/api/v1/report/data")) return ok(report);
    if (url.includes("/api/v1/alerts/ncc-dispositions")) return ok({ items: [] });
    if (url.includes("/api/v1/alerts/ncc")) return ok({ items: [] });
    return ok({});
  }) as typeof fetch;
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={[path]}>
        <InsightsPage />
        <Where />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const report = {
  run_summary: { timestamp: new Date(Date.now() - 3600_000).toISOString(), duration_s: 300, clusters_ok: 2, clusters_failed: 0 },
  ncc_summary_counts: { total_plugins: 100, pass: 95, fail: 1, warn: 4 },
  ncc_cluster_summary: [
    { address: "10.0.0.1", total_plugins: 50, pass: 50 },
    { address: "10.0.0.2", total_plugins: 50, pass: 45, fail: 1, warn: 4 },
  ],
  agg_rows: [
    { cluster: "10.0.0.2", severity: "FAIL", check: "disk_check", detail: "Disk offline. See KB 1234" },
    { cluster: "10.0.0.2", severity: "WARN", check: "ntp_check", detail: "drift" },
  ],
  checks_snapshot: [],
  regression_summary: {},
};

describe("InsightsPage", () => {
  afterEach(() => cleanup());

  it("does not show a 0% score when there is nothing to analyse", async () => {
    renderInsights({ run_summary: {}, ncc_summary_counts: {}, agg_rows: [], checks_snapshot: [] });
    expect(await screen.findByText("Nothing to analyse yet")).toBeInTheDocument();
    expect(screen.queryByText(/0%/)).not.toBeInTheDocument();
  });

  it("shows the shared health score and keeps the tab in the URL", async () => {
    renderInsights(report);
    // 95% pass − 1%×8 − 4%×3.5 = 73.0%, the same formula as the Overview.
    expect(await screen.findByText("73.0%")).toBeInTheDocument();
    expect(screen.getByText("1/2")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("tab", { name: "Clusters" }));
    expect(screen.getByTestId("where")).toHaveTextContent("/insights?tab=clusters");
    expect(await screen.findByText("Cluster health")).toBeInTheDocument();
  });

  it("opens a check's clusters and output from Top failing checks", async () => {
    renderInsights(report);
    fireEvent.click(await screen.findByRole("cell", { name: /disk_check/ }));
    expect(await screen.findByText("Check output")).toBeInTheDocument();
    // The same text is also listed under "Review first"; check it in the drawer.
    expect(within(screen.getByRole("dialog")).getByText(/Disk offline/)).toBeInTheDocument();
  });

  it("opens the tab named in the URL", async () => {
    renderInsights(report, "/insights?tab=triage");
    expect(await screen.findByText("Triage queue")).toBeInTheDocument();
  });
});
