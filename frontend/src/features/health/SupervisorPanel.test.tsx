import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import type { PlatformData, SupervisorData } from "../../api/types";
import { statusTabFrom } from "../status/StatusTabs";
import { PlatformPanel, SupervisorPanel, formatBytes, platformFindings, secondsSince } from "./SupervisorPanel";

vi.mock("../../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn(), loading: vi.fn(), close: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

function wrap(node: ReactNode) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>{node}</MemoryRouter>
    </QueryClientProvider>,
  );
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const now = new Date().toISOString();
const SUPERVISED: SupervisorData = {
  mode: "supervised",
  running: true,
  pid: 4242,
  state: {
    pid: 4242,
    started_at: now,
    updated_at: now,
    policy: { max_restarts: 5, window: "10m0s" },
    children: {
      api: { name: "api", state: "running", pid: 10, started_at: now, restarts_total: 1, restarts_in_window: 0, probe_failures: 0, last_probe_ok: true, last_probe_ms: 12, probed: true },
      ui: {
        name: "ui",
        state: "cooldown",
        restarts_total: 6,
        restarts_in_window: 6,
        probe_failures: 0,
        probed: true,
        next_start_at: now,
        last_exit_reason: "crashed",
        last_exit_detail: "exit status 2",
        last_exit_log_tail: ["panic: listener closed"],
      },
    },
    events: [{ at: now, child: "ui", event: "cooldown", detail: "5 restarts within 10m0s" }],
  },
};

const PLATFORM: PlatformData = {
  host: {
    os: "darwin",
    arch: { binary: "amd64", native: "arm64", emulated: true, known: true },
    cpus: 8,
    gomaxprocs: 8,
    go_version: "go1.27.3",
    memory: { total_bytes: 16 << 30 },
    cgroup: {},
    kubernetes: false,
    service_manager: "launchd",
  },
  api_process: {
    pid: 1,
    uptime_seconds: 3600,
    goroutines: 40,
    heap_alloc_bytes: 20 << 20,
    sys_bytes: 40 << 20,
    gc_cycles: 3,
    memory_limit: 0,
    tuning: { memory_limit_source: "none", gomaxprocs: 8 },
    file_limit_hard: 65536,
  },
};

describe("Supervisor and Platform", () => {
  it("are tabs of Status", () => {
    expect(statusTabFrom(new URLSearchParams("tab=supervisor"))).toBe("supervisor");
    expect(statusTabFrom(new URLSearchParams("tab=platform"))).toBe("platform");
    expect(statusTabFrom(new URLSearchParams(""))).toBe("overview");
  });

  it("formats sizes and ages", () => {
    expect(formatBytes(1536 << 20)).toBe("1.5 GiB");
    expect(formatBytes(0)).toBe("—");
    expect(secondsSince("2026-01-01T00:00:10Z", Date.parse("2026-01-01T00:01:10Z"))).toBe(60);
    expect(secondsSince(undefined)).toBeNull();
  });

  it("shows each server, the last crash with its log, and restarts on request", async () => {
    vi.spyOn(api, "supervisor").mockResolvedValue(SUPERVISED);
    const restart = vi.spyOn(api, "restartSupervisedChild").mockResolvedValue({ child: "ui" });
    wrap(<SupervisorPanel />);
    expect(await screen.findByText("API server")).toBeTruthy();
    expect(screen.getByText("Cooling down")).toBeTruthy();
    expect(screen.getByText(/Last stop: Crashed/)).toBeTruthy();
    fireEvent.click(screen.getByText("End of the server's log"));
    expect(await screen.findByText("panic: listener closed")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Start now/ }));
    fireEvent.click(await screen.findByRole("button", { name: "Restart" }));
    await waitFor(() => expect(restart).toHaveBeenCalledWith("ui"));
  });

  it("explains an unsupervised stack", async () => {
    vi.spyOn(api, "supervisor").mockResolvedValue({ mode: "manual", running: false, message: "No supervisor is running" });
    wrap(<SupervisorPanel />);
    expect(await screen.findByText("Not supervised")).toBeTruthy();
    expect(screen.getByText("No supervisor is running")).toBeTruthy();
  });

  it("flags CPU emulation on the platform tab", async () => {
    expect(platformFindings(PLATFORM)[0].text).toMatch(/emulation on an ARM64 machine/);
    vi.spyOn(api, "platform").mockResolvedValue(PLATFORM);
    wrap(<PlatformPanel />);
    expect(await screen.findByText("emulated")).toBeTruthy();
    expect(screen.getByText("launchd daemon")).toBeTruthy();
  });
});
