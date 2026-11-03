import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import type { UpdateStatus, UpgradeJob } from "../../api/types";
import { UpgradeProgress, failedStep, upgradeActive } from "./UpgradeProgress";

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

const at = "2026-11-03T10:00:00Z";
const JOB: UpgradeJob = {
  id: "20261103T100000Z",
  phase: "rolled_back",
  from_version: "2.3.0",
  target_version: "2.3.1",
  message: "Rolled back to v2.3.0",
  error: "api-server did not come back healthy",
  started_at: at,
  updated_at: at,
  steps: [
    { phase: "preflight", at, message: "Checking disk space" },
    { phase: "installing", at, message: "Installing v2.3.1" },
    { phase: "restarting", at, message: "Restarting the API server" },
    { phase: "rolling_back", at, message: "Putting v2.3.0 back" },
    { phase: "rolled_back", at, message: "Rolled back" },
  ],
};

function status(upgrade?: UpgradeJob): UpdateStatus {
  return { supervised_upgrades: true, upgrade } as UpdateStatus;
}

describe("UpgradeProgress", () => {
  it("knows active phases and where a job stopped", () => {
    expect(upgradeActive({ ...JOB, phase: "installing" })).toBe(true);
    expect(upgradeActive({ ...JOB, phase: "installing", stale: true })).toBe(false);
    expect(upgradeActive(JOB)).toBe(false);
    expect(upgradeActive(undefined)).toBe(false);
    expect(failedStep(JOB)).toBe(2);
  });

  it("explains supervised upgrades before the first one", async () => {
    vi.spyOn(api, "updateStatus").mockResolvedValue(status());
    wrap(<UpgradeProgress />);
    expect(await screen.findByText("Upgrades are checked and can roll back on their own")).toBeInTheDocument();
  });

  it("shows a rollback with its reason and steps", async () => {
    vi.spyOn(api, "updateStatus").mockResolvedValue(status(JOB));
    wrap(<UpgradeProgress />);
    expect(await screen.findByText("The update was rolled back; the previous version is running")).toBeInTheDocument();
    expect(screen.getByText("api-server did not come back healthy")).toBeInTheDocument();
    expect(screen.getByText("v2.3.0 → v2.3.1", { exact: false })).toBeInTheDocument();
    expect(screen.getByText(/Details \(5 steps\)/)).toBeInTheDocument();
  });

  it("warns when the supervisor is not running", async () => {
    vi.spyOn(api, "updateStatus").mockResolvedValue(status({ ...JOB, phase: "restarting", stale: true }));
    wrap(<UpgradeProgress />);
    expect(await screen.findByText("The supervisor is not running")).toBeInTheDocument();
  });
});
