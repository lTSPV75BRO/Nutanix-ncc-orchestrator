import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactElement } from "react";
import { MemoryRouter } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ScheduleSection } from "./ScheduleSection";

vi.mock("../../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn(), loading: vi.fn(), close: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

const renderWithQuery = (ui: ReactElement) =>
  render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><MemoryRouter>{ui}</MemoryRouter></QueryClientProvider>);

type Call = { url: string; method: string; body?: Record<string, unknown> };

/** A fake API holding one CronJob; PUTs replace its schedule. */
function fakeCronJobApi(initial: { cron: string; every?: string; action?: string }) {
  let state = { type: "auto", action: "create", config: "/data/config/config.yaml", task_name: "ncc-v2-runner", updated_at: "", ...initial };
  const calls: Call[] = [];
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method || "GET";
    const body = init?.body ? JSON.parse(String(init.body)) : undefined;
    calls.push({ url, method, body });
    let data: unknown = {};
    if (url.includes("/api/v1/schedule/health")) {
      data = { installed: true, configured: state.action === "create", suspended: state.action === "remove", task_name: "ncc-v2-runner", cron: state.cron, every: state.every };
    } else if (url.includes("/api/v1/schedule")) {
      if (method === "PUT") {
        const every = body.cron ? "" : body.every;
        state = { ...state, action: body.action, cron: body.cron || (every === "30m" ? "*/30 * * * *" : state.cron), every, updated_at: "2026-11-03T09:00:00Z" };
      }
      data = state;
    }
    return {
      ok: true,
      status: 200,
      statusText: "OK",
      headers: new Headers({ "content-type": "application/json" }),
      json: async () => ({ success: true, data }),
    };
  }) as unknown as typeof fetch;
  return calls;
}

const puts = (calls: Call[]) => calls.filter((c) => c.method === "PUT").map((c) => c.body!);

describe("ScheduleSection on Kubernetes", () => {
  afterEach(() => {
    cleanup();
    localStorage.clear();
  });

  it("keeps a custom CronJob schedule instead of replacing it with the interval", async () => {
    const calls = fakeCronJobApi({ cron: "15 */4 * * *" });
    renderWithQuery(<ScheduleSection backendConfigPath="config.yaml" onError={vi.fn()} isKubernetes />);
    const cronInput = await screen.findByLabelText("Custom cron expression");
    await waitFor(() => expect(cronInput).toHaveValue("15 */4 * * *"));
    expect(screen.queryByLabelText("Scheduler type")).toBeNull();
    expect(screen.queryByLabelText("Custom config path (optional)")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: /Save schedule/ }));
    await waitFor(() => expect(puts(calls)).toHaveLength(1));
    expect(puts(calls)[0]).toMatchObject({ action: "create", cron: "15 */4 * * *", apply: true, print_only: false });
  });

  it("pauses and resumes the CronJob", async () => {
    const calls = fakeCronJobApi({ cron: "0 */4 * * *", every: "4h" });
    renderWithQuery(<ScheduleSection backendConfigPath="config.yaml" onError={vi.fn()} isKubernetes />);
    const pause = await screen.findByRole("switch", { name: "Pause scheduled runs" });
    fireEvent.click(pause);
    fireEvent.click(screen.getAllByRole("button", { name: /Save schedule/ })[0]);
    await waitFor(() => expect(puts(calls)).toHaveLength(1));
    expect(puts(calls)[0]).toMatchObject({ action: "remove" });
    expect(await screen.findByText("Scheduled runs are paused")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole("switch", { name: "Pause scheduled runs" })).toBeChecked());
  });

  it("sends the interval alone in simple mode", async () => {
    const calls = fakeCronJobApi({ cron: "0 */4 * * *", every: "4h" });
    renderWithQuery(<ScheduleSection backendConfigPath="config.yaml" onError={vi.fn()} isKubernetes />);
    await waitFor(() => expect(screen.getByLabelText("Run every")).toHaveValue("4"));
    expect(screen.queryByLabelText("Custom cron expression")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Every 15m" }));
    fireEvent.click(screen.getAllByRole("button", { name: /Save schedule/ })[0]);
    await waitFor(() => expect(puts(calls)).toHaveLength(1));
    expect(puts(calls)[0]).toMatchObject({ action: "create", cron: "", every: "15m" });
  });
});

/** A host API: nothing installed yet; PUT echoes a preview or saves. */
function fakeHostApi() {
  const calls: Call[] = [];
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method || "GET";
    const body = init?.body ? JSON.parse(String(init.body)) : undefined;
    calls.push({ url, method, body });
    let data: unknown = {};
    if (url.includes("/api/v1/schedule/health")) {
      data = { installed: false, saved: false, detector: "crontab", type: "auto" };
    } else if (url.includes("/api/v1/runs/configs")) {
      data = { items: [{ path: "config.yaml", is_active: true }], default_path: "config.yaml" };
    } else if (url.includes("/api/v1/schedule")) {
      data = method === "PUT"
        ? { applied: true, preview: body.print_only, output: body.print_only ? "15 */4 * * * ncc-orchestrator --config config.yaml" : "" }
        : { type: "auto", action: "create", config: "config.yaml", print_only: false, updated_at: "" };
    }
    return {
      ok: true,
      status: 200,
      statusText: "OK",
      headers: new Headers({ "content-type": "application/json" }),
      json: async () => ({ success: true, data }),
    };
  }) as unknown as typeof fetch;
  return calls;
}

describe("ScheduleSection on a host", () => {
  afterEach(() => {
    cleanup();
    localStorage.clear();
  });

  it("installs on save and previews without saving", async () => {
    const calls = fakeHostApi();
    renderWithQuery(<ScheduleSection backendConfigPath="config.yaml" onError={vi.fn()} />);
    expect(await screen.findByText("No recurring runs yet")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Preview/ }));
    expect(await screen.findByText(/ncc-orchestrator --config config.yaml/)).toBeInTheDocument();
    expect(puts(calls)[0]).toMatchObject({ apply: true, print_only: true, action: "create" });

    fireEvent.click(screen.getAllByRole("button", { name: /Save schedule/ })[0]);
    await waitFor(() => expect(puts(calls)).toHaveLength(2));
    expect(puts(calls)[1]).toMatchObject({ apply: true, print_only: false, every: "4h", cron: "" });
  });

  it("explains a bad cron and blocks saving it", async () => {
    fakeHostApi();
    renderWithQuery(<ScheduleSection backendConfigPath="config.yaml" onError={vi.fn()} />);
    fireEvent.click(await screen.findByText("Custom cron"));
    fireEvent.change(screen.getByLabelText("Custom cron expression"), { target: { value: "*/5 * * *" } });
    expect(await screen.findByText(/Use five fields/)).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: /Save schedule/ })[0]).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "Weekdays 02:00" }));
    expect((await screen.findAllByText("Weekdays at 02:00")).length).toBeGreaterThan(0);
    expect(screen.getByText(/Next runs:/)).toBeInTheDocument();
  });
});
