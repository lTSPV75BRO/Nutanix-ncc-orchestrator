import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import type { UpdateStatus, UpgradeJob } from "../../api/types";
import { UpgradePlanSummary } from "../settings/access/UpdatesCard";
import { UpgradeProgress, canRollBack, failedStep, upgradeActive } from "./UpgradeProgress";

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

  const HELPER: UpgradeJob = {
    ...JOB,
    phase: "done",
    runner: "helper",
    action: "upgrade",
    error: undefined,
    to_version: "2.3.1",
    message: "Upgraded from v2.3.0 to v2.3.1; the supervisor, API and UI are healthy",
    migrations: ["systemd unit: replaced"],
    rollback_until: new Date(Date.now() + 3600_000).toISOString(),
  };

  it("follows a job of the update helper", async () => {
    expect(upgradeActive({ ...HELPER, phase: "stopping" })).toBe(true);
    expect(failedStep({ ...HELPER, phase: "failed", steps: [{ phase: "migrating", at, message: "" }, { phase: "restoring", at, message: "" }] })).toBe(2);
    vi.spyOn(api, "updateStatus").mockResolvedValue(status({ ...HELPER, phase: "swapping", message: "Installing v2.3.1" }));
    wrap(<UpgradeProgress />);
    expect(await screen.findByText("Upgrade in progress")).toBeInTheDocument();
    expect(screen.getByText("Update helper")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Roll back/ })).toBeNull();
  });

  it("offers the rollback only while the previous version is kept", async () => {
    expect(canRollBack(HELPER)).toBe(true);
    expect(canRollBack({ ...HELPER, rollback_until: "2020-01-01T00:00:00Z" })).toBe(false);
    expect(canRollBack({ ...HELPER, runner: undefined })).toBe(false);
    expect(canRollBack({ ...HELPER, action: "adopt" })).toBe(false);
    vi.spyOn(api, "updateStatus").mockResolvedValue(status(HELPER));
    const rollback = vi.spyOn(api, "rollbackUpdate").mockResolvedValue({ success: true, message: "Rolling back" });
    wrap(<UpgradeProgress />);
    expect(await screen.findByText("systemd unit: replaced")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /Roll back/ }));
    const dialog = await screen.findByRole("dialog");
    expect(dialog).toHaveTextContent("Roll back to v2.3.0?");
    expect(rollback).not.toHaveBeenCalled();
    fireEvent.click(within(dialog).getByRole("button", { name: "Roll back" }));
    await waitFor(() => expect(rollback).toHaveBeenCalledTimes(1));
  });

  it("shows a rollback asked for by hand as a success", async () => {
    vi.spyOn(api, "updateStatus").mockResolvedValue(status({ ...HELPER, phase: "rolled_back", action: "rollback", message: "Rolled back to v2.3.0" }));
    wrap(<UpgradeProgress />);
    expect(await screen.findByText("Last rollback")).toBeInTheDocument();
    expect(screen.queryByText(/The update was rolled back/)).toBeNull();
  });

  it("says how to continue a helper that went quiet", async () => {
    vi.spyOn(api, "updateStatus").mockResolvedValue(status({ ...HELPER, phase: "starting", stale: true }));
    wrap(<UpgradeProgress />);
    expect(await screen.findByText("The update helper has not reported for a while")).toBeInTheDocument();
  });

  it("lists what the plan changes and what would stop it", async () => {
    vi.spyOn(api, "updatePlan").mockResolvedValue({
      current_version: "2.4.0",
      helper: "ready",
      service: "systemd service ncc-orchestrator.service",
      definitions: ["service unit: merge (this host's MemoryMax kept in 50-local.conf)"],
      problems: ["not enough disk space"],
    });
    wrap(<UpgradePlanSummary />);
    expect(await screen.findByText(/MemoryMax kept/)).toBeInTheDocument();
    expect(screen.getByText("not enough disk space")).toBeInTheDocument();
  });

  it("warns when the supervisor is not running", async () => {
    vi.spyOn(api, "updateStatus").mockResolvedValue(status({ ...JOB, phase: "restarting", stale: true }));
    wrap(<UpgradeProgress />);
    expect(await screen.findByText("The supervisor is not running")).toBeInTheDocument();
  });
});
