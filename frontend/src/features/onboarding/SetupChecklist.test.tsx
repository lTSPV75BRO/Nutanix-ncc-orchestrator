import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { OnboardingStatus } from "../../api/types";
import { AuthContext, type AuthValue } from "../../auth/AuthContext";
import { SetupChecklistCard } from "./SetupChecklist";

vi.mock("../../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

const baseStatus: OnboardingStatus = {
  steps: [
    { id: "clusters", title: "Connect your clusters", description: "d", done: true, detail: "2 cluster(s) configured.", tab: "config", action_label: "Open configuration", admin_only: true },
    { id: "first_run", title: "Run NCC for the first time", description: "d", done: false, detail: "No completed run yet.", tab: "runs", action_label: "Open runs" },
    { id: "backups", title: "Protect your setup with backups", description: "d", done: false, tab: "maintenance", action_label: "Open backups", admin_only: true },
    { id: "notifications", title: "Get notified", description: "d", done: false, optional: true, tab: "notifications", action_label: "Open notifications", admin_only: true },
  ],
  required_done: 1,
  required_total: 3,
  completed: false,
  dismissed: false,
};

function auth(isAdmin: boolean): AuthValue {
  return { me: null, role: isAdmin ? "admin" : "operator", isAdmin, canOperate: true, loginEnabled: true, authenticated: true };
}

function LocationProbe() {
  const loc = useLocation();
  return <div data-testid="location">{loc.pathname + loc.search}</div>;
}

function renderCard(status: OnboardingStatus, isAdmin = true) {
  globalThis.fetch = vi.fn(async () => ({
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({ success: true, data: status }),
  })) as unknown as typeof fetch;
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <AuthContext.Provider value={auth(isAdmin)}>
        <MemoryRouter initialEntries={["/"]}>
          <Routes>
            <Route path="*" element={<><SetupChecklistCard /><LocationProbe /></>} />
          </Routes>
        </MemoryRouter>
      </AuthContext.Provider>
    </QueryClientProvider>,
  );
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("SetupChecklistCard", () => {
  it("shows server-derived progress and links the next step into Settings", async () => {
    renderCard(baseStatus);
    expect(await screen.findByText("1 of 3 required steps done")).toBeInTheDocument();
    expect(screen.getByText("2 cluster(s) configured.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Open runs" }));
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent("/settings?tab=runs"));
  });

  it("tells operators to ask an admin for admin-only steps and hides the Hide button", async () => {
    renderCard(baseStatus, false);
    await screen.findByText("1 of 3 required steps done");
    expect(screen.getAllByText("Ask an administrator").length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: /Hide/ })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Open runs" })).toBeInTheDocument();
  });

  it("renders nothing once setup is complete or hidden by an admin", async () => {
    const { container } = renderCard({ ...baseStatus, completed: true });
    await waitFor(() => expect(globalThis.fetch).toHaveBeenCalled());
    expect(container.querySelector(".setup-card")).toBeNull();
    cleanup();
    const second = renderCard({ ...baseStatus, dismissed: true });
    await waitFor(() => expect(globalThis.fetch).toHaveBeenCalled());
    expect(second.container.querySelector(".setup-card")).toBeNull();
  });
});
