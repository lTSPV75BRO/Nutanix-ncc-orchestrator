import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import type { NCCAlert } from "../../api/types";
import { AuthContext, type AuthValue } from "../../auth/AuthContext";
import { LifecycleInsightsCard } from "./LifecycleInsightsCard";

vi.mock("../../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn(), loading: vi.fn(), close: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

const viewer: AuthValue = { me: null, role: "viewer", isAdmin: false, canOperate: false, loginEnabled: true, authenticated: true };
const operator: AuthValue = { ...viewer, role: "operator", canOperate: true };

const fixedAlert = {
  id: "a1",
  finding_id: "f1",
  correlation_key: "k1",
  source: "ncc",
  type: "check",
  cluster: "c1",
  check: "Disk usage",
  state: "open",
  detection: "not_detected",
  closed: false,
  first_seen_at: "2026-10-01T00:00:00Z",
  not_detected_since_run: "2026-10-30T00:00:00Z",
  created_at: "2026-10-01T00:00:00Z",
} as NCCAlert;

function renderAs(auth: AuthValue, alertHandling = true) {
  vi.spyOn(api, "nccAlertsAll").mockResolvedValue([fixedAlert]);
  vi.spyOn(api, "featureFlags").mockResolvedValue({ alert_handling: alertHandling } as Awaited<ReturnType<typeof api.featureFlags>>);
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <AuthContext.Provider value={auth}>
        <LifecycleInsightsCard enabled />
      </AuthContext.Provider>
    </QueryClientProvider>,
  );
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("LifecycleInsightsCard", () => {
  it("lets operators resolve alerts the runs no longer report", async () => {
    renderAs(operator);
    expect(await screen.findByText("Disk usage")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "Resolve" })).toBeInTheDocument();
  });

  it("shows viewers the list without a Resolve button", async () => {
    renderAs(viewer);
    expect(await screen.findByText("Disk usage")).toBeInTheDocument();
    expect(screen.getByText(/An operator can resolve them/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Resolve" })).not.toBeInTheDocument();
  });

  it("hides Resolve when alert handling is turned off", async () => {
    renderAs(operator, false);
    expect(await screen.findByText("Disk usage")).toBeInTheDocument();
    expect(await screen.findByText(/An operator can resolve them/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Resolve" })).not.toBeInTheDocument();
  });
});
