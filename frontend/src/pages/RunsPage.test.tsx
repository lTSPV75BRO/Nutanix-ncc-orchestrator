import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AuthContext, type AuthValue } from "../auth/AuthContext";
import { RunDetailPage } from "./RunDetailPage";
import { RunsPage } from "./RunsPage";

vi.mock("../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn(), loading: vi.fn(), close: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

const viewer: AuthValue = { me: null, role: "viewer", isAdmin: false, canOperate: false, loginEnabled: true, authenticated: true };
const operator: AuthValue = { ...viewer, role: "operator", canOperate: true };

const recent = new Date(Date.now() - 3600_000).toISOString();
const RUNS = [
  { id: "r-ok", source: "history", mod_time: recent, has_index: true, status: "success", duration_s: 200, clusters_ok: 3, clusters_failed: 0, fail_total: 0, warn_total: 1 },
  { id: "r-bad", source: "history", mod_time: recent, has_index: true, status: "failed", exit_code: 2, duration_s: 10, clusters_ok: 0, clusters_failed: 3 },
  { id: "latest", source: "summary", mod_time: recent, has_index: true, status: "success" },
];

function ok(data: unknown, status = 200) {
  return {
    ok: status < 400,
    status,
    statusText: status < 400 ? "OK" : "Not Found",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => (status < 400 ? { success: true, data } : { success: false, error: "run not found" }),
  } as Response;
}

function Where() {
  return <div data-testid="where">{useLocation().pathname}</div>;
}

function renderAt(path: string, auth: AuthValue, detail?: unknown) {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/api/v1/runs/active")) return ok({ active: false, runs: [] });
    if (url.includes("/api/v1/runs/")) return detail === undefined ? ok(null, 404) : ok(detail);
    if (url.includes("/api/v1/runs")) return ok(RUNS);
    if (url.includes("/api/v1/artifacts")) return ok([]);
    return ok({ status: "ok" });
  }) as typeof fetch;
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <AuthContext.Provider value={auth}>
        <MemoryRouter initialEntries={[path]}>
          <Routes>
            <Route path="/runs" element={<RunsPage />} />
            <Route path="/runs/:id" element={<><RunDetailPage /><Where /></>} />
          </Routes>
        </MemoryRouter>
      </AuthContext.Provider>
    </QueryClientProvider>,
  );
}

describe("RunsPage", () => {
  afterEach(() => cleanup());

  it("lets viewers browse history but not start runs", async () => {
    renderAt("/runs", viewer);
    expect(await screen.findByText("Failed · exit 2")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /New run/ })).not.toBeInTheDocument();
  });

  it("filters to runs with problems and opens an archived run", async () => {
    renderAt("/runs", operator, { run: RUNS[1], artifacts: {} });
    expect(await screen.findByRole("button", { name: /New run/ })).toBeInTheDocument();
    fireEvent.click(await screen.findByText(/With problems \(1\)/));
    expect(screen.queryByText("Success")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("row", { name: "Open run r-bad" }));
    expect(await screen.findByTestId("where")).toHaveTextContent("/runs/r-bad");
  });

  it("does not offer a detail page for the latest-summary row", async () => {
    renderAt("/runs", operator);
    await screen.findByText("Failed · exit 2");
    expect(screen.queryByRole("row", { name: "Open run latest" })).not.toBeInTheDocument();
  });
});

describe("RunDetailPage", () => {
  afterEach(() => cleanup());

  it("shows clusters worst first and the checks that did not pass", async () => {
    renderAt("/runs/r1", viewer, {
      run: { ...RUNS[0], id: "r1", status: "partial", clusters_ok: 2, clusters_failed: 1, fail_total: 1 },
      artifacts: {
        "run-summary.json": {
          clusters: [
            { address: "10.0.0.1", ok: true, warn_count: 1, checks_total: 100 },
            { address: "10.0.0.2", ok: false, error: "timeout", error_class: "network" },
          ],
          failure_classes: { network: 1 },
        },
        "checks-snapshot.json": {
          clusters: [
            {
              address: "10.0.0.1",
              checks: [
                { check_name: "ntp_check", severity: "WARN", detail: "drift" },
                { check_name: "disk_check", severity: "PASS" },
                { check_name: "cvm_mem", severity: "FAIL", detail: "high" },
              ],
            },
          ],
        },
      },
    });
    expect(await screen.findByText("Partial")).toBeInTheDocument();
    const rows = screen.getAllByRole("row").slice(1);
    expect(within(rows[0]).getByText("10.0.0.2")).toBeInTheDocument();
    expect(within(rows[0]).getByText("Not checked")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("tab", { name: /Checks \(2\)/ }));
    expect(await screen.findByText("cvm_mem")).toBeInTheDocument();
    expect(screen.queryByText("disk_check")).not.toBeInTheDocument();
  });

  it("explains a missing run", async () => {
    renderAt("/runs/gone", viewer);
    expect(await screen.findByText("Run not found")).toBeInTheDocument();
  });
});
