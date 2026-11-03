import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import type { ConfigMigrationResult, HealHistoryData } from "../../api/types";
import { MigrationsPanel, plaintextSecrets } from "../updates/MigrationsPanel";
import { updatesTabFrom } from "../updates/UpdatesBackupsPage";
import { AutoHealPanel, HealHistoryPanel, healthTabFrom } from "./HealthRepairPage";

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

const MIXED: ConfigMigrationResult = {
  config_path: "config.yaml",
  layout: "mixed",
  changed: true,
  applied: false,
  moved: [{ from: "timeout", to: "runner.execution.timeout" }],
  duplicates: [
    { flat: "timeout", paths: ["timeout", "runner.execution.timeout"], winner: "timeout", value: "20m", shadowed_values: ["runner.execution.timeout = 15m"], differs: true },
  ],
  removed: ["runner.execution.timeout"],
  content: "schema-version: 1\nrunner:\n  execution:\n    timeout: 20m\n",
  message: "1 setting moved to the nested layout; 1 duplicate spelling removed",
};

describe("Health & repair", () => {
  it("picks the tab from the URL", () => {
    expect(healthTabFrom(new URLSearchParams("tab=history"))).toBe("history");
    expect(healthTabFrom(new URLSearchParams("tab=nope"))).toBe("checks");
    expect(updatesTabFrom(new URLSearchParams("tab=migrations"))).toBe("migrations");
    expect(updatesTabFrom(new URLSearchParams("focus=backup"))).toBe("backups");
    expect(updatesTabFrom(new URLSearchParams(""))).toBe("updates");
  });

  it("finds plain-text secrets in either layout", () => {
    expect(plaintextSecrets("password: hunter2\nsmtp-password: secret://SMTP\n")).toEqual(["password"]);
    expect(plaintextSecrets("schema-version: 1\nnotifications:\n  webhook:\n    secret: abc\n")).toEqual(["webhook-secret"]);
    expect(plaintextSecrets("password: ''\n")).toEqual([]);
  });

  it("previews a config migration, warns about conflicting duplicates and applies it", async () => {
    vi.spyOn(api, "previewConfigMigration").mockResolvedValue({ result: MIXED });
    vi.spyOn(api, "loadConfig").mockResolvedValue({ content: "password: secret://P\n", path: "config.yaml" } as Awaited<ReturnType<typeof api.loadConfig>>);
    const apply = vi.spyOn(api, "applyConfigMigration").mockResolvedValue({ result: { ...MIXED, applied: true, backup: "config.yaml.pre-migrate.bak" } });
    wrap(<MigrationsPanel />);
    expect(await screen.findByText("timeout is set twice with different values")).toBeInTheDocument();
    expect(screen.getByText(/The runner uses timeout = 20m; ignored: runner.execution.timeout = 15m/)).toBeInTheDocument();
    expect(screen.getByText("runner.execution.timeout")).toBeInTheDocument();
    expect(await screen.findByText("No plain-text secrets")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Migrate to nested layout/ }));
    fireEvent.click(await screen.findByRole("button", { name: "Migrate" }));
    await waitFor(() => expect(apply).toHaveBeenCalled());
  });

  it("says when there is nothing to migrate", async () => {
    vi.spyOn(api, "previewConfigMigration").mockResolvedValue({ result: { ...MIXED, layout: "nested", changed: false, moved: [], duplicates: [] } });
    vi.spyOn(api, "loadConfig").mockResolvedValue({ content: "", path: "config.yaml" } as Awaited<ReturnType<typeof api.loadConfig>>);
    wrap(<MigrationsPanel />);
    expect(await screen.findByText("Nothing to migrate")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Migrate to nested layout/ })).toBeNull();
  });

  it("flags repairs that keep coming back", async () => {
    const history: HealHistoryData = {
      entries: [{ at: "2026-11-03T07:00:00Z", id: "secrets-perms", title: "Secret file permissions", category: "encryption", fix_message: "chmod 0600 secrets.env", verified: false, status: "warn" }],
      total: 1,
      by_check: [{ id: "secrets-perms", title: "Secret file permissions", fixes_24h: 4, fixes_7d: 9, last: "2026-11-03T07:00:00Z", recurring: true }],
      unverified_7d: 1,
      recurring_after: 3,
    };
    vi.spyOn(api, "healHistory").mockResolvedValue(history);
    wrap(<HealHistoryPanel />);
    expect(await screen.findByText("Repairs that keep coming back")).toBeInTheDocument();
    expect(screen.getByText(/Secret file permissions \(4× in 24h\)/)).toBeInTheDocument();
    expect(screen.getByText("still failing")).toBeInTheDocument();
  });

  it("lists automatic healers with their state", async () => {
    vi.spyOn(api, "autoHeal").mockResolvedValue({
      healers: [
        { id: "selfheal-loop", title: "Background health checks", description: "d", state: "partial", detail: "Every 1h0m0s, report only", stats: { runs: 3, fixes: 0 } },
        { id: "run-auto-retry", title: "Run auto-retry", description: "d", state: "on", stats: { retries: 2 } },
      ],
    });
    wrap(<AutoHealPanel />);
    expect(await screen.findByText("Background health checks")).toBeInTheDocument();
    expect(screen.getByText("Report only")).toBeInTheDocument();
    expect(screen.getByText(/2 retries/)).toBeInTheDocument();
  });
});
