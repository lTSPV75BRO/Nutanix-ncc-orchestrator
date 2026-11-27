import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { OnboardingStatus, OnboardingStep } from "../../api/types";
import { AuthContext, type AuthValue } from "../../auth/AuthContext";
import { emptyCopy, setupGap, SetupEmptyState } from "./SetupEmptyState";

vi.mock("../../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

const viewer: AuthValue = { me: null, role: "viewer", isAdmin: false, canOperate: false, loginEnabled: true, authenticated: true };
const operator: AuthValue = { ...viewer, role: "operator", canOperate: true };
const admin: AuthValue = { ...viewer, role: "admin", isAdmin: true, canOperate: true };
const restricted: AuthValue = { ...operator, me: { authenticated: true, login_enabled: true, local_enabled: true, saml_enabled: false, cluster_access_unrestricted: false, allowed_clusters: [] } };

const step = (id: string, done: boolean, detail = ""): OnboardingStep => ({ id, title: id, description: "", done, detail, tab: id, action_label: "Open" });
const status = (...steps: OnboardingStep[]): OnboardingStatus => ({ steps, required_done: 0, required_total: steps.length, completed: false, dismissed: false });

const NO_CLUSTERS = status(step("clusters", false, "No clusters yet — the configuration still has the sample value."), step("first_run", false, "No completed run yet."), step("schedule", false));
const BAD_CONFIG = status(step("clusters", false, "The configuration file could not be read: bad yaml"));
const FIRST_RUN_FAILED = status(step("clusters", true), step("first_run", false, "The last run did not complete on any cluster (2 failed). Check addresses and credentials with a preflight check."), step("schedule", false));
const NO_RUN_NO_SCHEDULE = status(step("clusters", true), step("first_run", false, "No completed run yet."), step("schedule", false));

function ok(data: unknown) {
  return { ok: true, status: 200, statusText: "OK", headers: new Headers({ "content-type": "application/json" }), json: async () => ({ success: true, data }) } as Response;
}

function Where() {
  const loc = useLocation();
  return <div data-testid="where">{`${loc.pathname} ${JSON.stringify(loc.state)}`}</div>;
}

function renderEmpty(auth: AuthValue, onboarding: OnboardingStatus | null, active = false) {
  const onboardingCalls = vi.fn();
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/api/v1/onboarding")) {
      onboardingCalls();
      return ok(onboarding);
    }
    if (url.includes("/api/v1/runs/active")) return ok({ active });
    return ok({});
  }) as typeof fetch;
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={qc}>
      <AuthContext.Provider value={auth}>
        <MemoryRouter initialEntries={["/overview"]}>
          <Routes>
            <Route path="/overview" element={<SetupEmptyState />} />
            <Route path="*" element={<Where />} />
          </Routes>
        </MemoryRouter>
      </AuthContext.Provider>
    </QueryClientProvider>,
  );
  return onboardingCalls;
}

const buttons = () => screen.queryAllByRole("button").map((b) => b.textContent);

describe("setupGap", () => {
  it("puts the most basic missing step first", () => {
    expect(setupGap({ restricted: true, running: true, onboarding: NO_CLUSTERS }).kind).toBe("restricted");
    expect(setupGap({ restricted: false, running: true, onboarding: NO_CLUSTERS }).kind).toBe("running");
    expect(setupGap({ restricted: false, running: false, onboarding: BAD_CONFIG }).kind).toBe("no_config");
    expect(setupGap({ restricted: false, running: false, onboarding: NO_CLUSTERS }).kind).toBe("no_clusters");
    expect(setupGap({ restricted: false, running: false, onboarding: FIRST_RUN_FAILED }).kind).toBe("first_run_failed");
    expect(setupGap({ restricted: false, running: false, onboarding: NO_RUN_NO_SCHEDULE })).toEqual({ kind: "no_runs", scheduleMissing: true });
    // Viewers get no checklist.
    expect(setupGap({ restricted: false, running: false })).toEqual({ kind: "no_runs", scheduleMissing: false });
  });

  it("treats a skipped step as done", () => {
    const skipped = status({ ...step("clusters", false, "No clusters yet"), skipped: true });
    expect(setupGap({ restricted: false, running: false, onboarding: skipped }).kind).toBe("no_runs");
  });
});

describe("emptyCopy", () => {
  it("gives admin buttons to admins only", () => {
    const gap = setupGap({ restricted: false, running: false, onboarding: NO_CLUSTERS });
    expect(emptyCopy(gap, admin).actions.map((a) => a.label)).toEqual(["Add clusters"]);
    expect(emptyCopy(gap, operator).actions).toEqual([]);
    expect(emptyCopy(gap, operator).description).toMatch(/Ask an administrator/);
    expect(emptyCopy(gap, viewer).actions).toEqual([]);
  });

  it("offers a new run only to people who can start one", () => {
    const gap = setupGap({ restricted: false, running: false, onboarding: FIRST_RUN_FAILED });
    expect(emptyCopy(gap, admin).actions.map((a) => a.label)).toEqual(["Start a run again", "See the run", "Check Clusters & config"]);
    expect(emptyCopy(gap, operator).actions.map((a) => a.label)).toEqual(["Start a run again", "See the run"]);
    expect(emptyCopy(gap, viewer).actions.map((a) => a.label)).toEqual(["See the run"]);
  });
});

describe("SetupEmptyState", () => {
  afterEach(() => cleanup());

  it("tells a restricted account it sees no clusters, with no admin buttons", async () => {
    renderEmpty(restricted, NO_CLUSTERS);
    expect(await screen.findByText("Your account sees no clusters")).toBeInTheDocument();
    expect(screen.getByText("Ask an administrator to add you to a cluster group")).toBeInTheDocument();
    expect(buttons()).toEqual([]);
  });

  it("sends an admin to Clusters & config when no clusters are set up", async () => {
    renderEmpty(admin, NO_CLUSTERS);
    expect(await screen.findByText("No clusters are set up yet")).toBeInTheDocument();
    expect(buttons()).toEqual(["Add clusters"]);
    fireEvent.click(screen.getByRole("button", { name: "Add clusters" }));
    expect(screen.getByTestId("where")).toHaveTextContent("/settings/config");
  });

  it("asks an operator to get an administrator when no clusters are set up", async () => {
    renderEmpty(operator, NO_CLUSTERS);
    expect(await screen.findByText("No clusters are set up yet")).toBeInTheDocument();
    expect(screen.getByText(/Ask an administrator to add clusters/)).toBeInTheDocument();
    expect(buttons()).toEqual([]);
  });

  it("says the configuration can't be read", async () => {
    renderEmpty(admin, BAD_CONFIG);
    expect(await screen.findByText("The configuration can't be read")).toBeInTheDocument();
    expect(buttons()).toEqual(["Open Clusters & config"]);
  });

  it("explains a failed first run and lets an operator start another", async () => {
    renderEmpty(operator, FIRST_RUN_FAILED);
    expect(await screen.findByText("The first run didn't reach any cluster")).toBeInTheDocument();
    expect(screen.getByText(/did not complete on any cluster/)).toBeInTheDocument();
    expect(buttons()).toEqual(["Start a run again", "See the run"]);
    fireEvent.click(screen.getByRole("button", { name: "Start a run again" }));
    expect(screen.getByTestId("where")).toHaveTextContent('/runs {"newRun":true}');
  });

  it("offers a run and a schedule to an operator when nothing has run", async () => {
    renderEmpty(operator, NO_RUN_NO_SCHEDULE);
    expect(await screen.findByText("No results yet")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "Set a schedule" })).toBeInTheDocument();
    expect(buttons()).toEqual(["Start a run", "Set a schedule"]);
    fireEvent.click(screen.getByRole("button", { name: "Set a schedule" }));
    expect(screen.getByTestId("where")).toHaveTextContent("/settings/schedule");
  });

  it("gives a viewer no buttons and does not fetch the checklist", async () => {
    const calls = renderEmpty(viewer, NO_RUN_NO_SCHEDULE);
    expect(await screen.findByText("No results yet")).toBeInTheDocument();
    expect(screen.getByText(/after an operator starts a run/)).toBeInTheDocument();
    expect(buttons()).toEqual([]);
    expect(calls).not.toHaveBeenCalled();
  });
});
