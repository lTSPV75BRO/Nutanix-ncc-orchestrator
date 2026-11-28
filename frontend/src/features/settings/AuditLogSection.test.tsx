import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuditLogSection } from "./AuditLogSection";

vi.mock("../../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

const entries = [
  {
    ts: new Date(Date.now() - 5 * 60_000).toISOString(),
    action: "settings.config.update",
    success: true,
    user: "alice",
    role: "admin",
    client: "10.0.0.5",
    method: "PUT",
    path: "/api/v1/settings/config",
    auth_mode: "hybrid",
    request_id: "req-123",
    client_kind: "ui",
    ui_version: "2.4.0",
  },
  {
    ts: new Date(Date.now() - 60 * 60_000).toISOString(),
    action: "auth.login",
    success: false,
    user: "bob",
    role: "viewer",
    client: "10.0.0.9",
    error: "invalid username or password",
  },
];

const requested: string[] = [];

function ok(data: unknown) {
  return {
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({ success: true, data }),
    clone() {
      return this;
    },
  } as unknown as Response;
}

function renderSection() {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    requested.push(url);
    return ok({ path: "audit.log", size: 100, mod_time: new Date().toISOString(), limit: 200, count: entries.length, max_bytes: 1000, entries, filters: {} });
  }) as typeof fetch;
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <AuditLogSection onError={vi.fn()} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  requested.length = 0;
});
afterEach(() => cleanup());

describe("AuditLogSection", () => {
  it("shows the compact columns and keeps the rest in the JSON entry", async () => {
    renderSection();
    await screen.findByText("settings.config.update");
    const headers = screen.getAllByRole("columnheader").map((h) => h.textContent?.trim());
    expect(headers).toEqual(expect.arrayContaining(["When", "Action", "Result", "User", "Client"]));
    // These used to be columns and made the table too busy; they are in the JSON.
    for (const gone of ["Endpoint", "Auth", "Failure"]) expect(headers).not.toContain(gone);
    expect(screen.queryByText("PUT /api/v1/settings/config")).not.toBeInTheDocument();
  });

  it("opens the full entry as JSON from the row's code button", async () => {
    renderSection();
    await screen.findByText("settings.config.update");
    fireEvent.click(screen.getByRole("button", { name: "View JSON for settings.config.update" }));
    const json = await screen.findByLabelText("Audit entry as JSON");
    expect(json.textContent).toContain('"request_id": "req-123"');
    expect(json.textContent).toContain('"path": "/api/v1/settings/config"');
    expect(json.textContent).not.toContain("__idx");
    expect(screen.getByRole("button", { name: /Copy JSON/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Copy request ID/ })).toBeInTheDocument();
  });

  it("explains a failed entry in the drawer", async () => {
    renderSection();
    await screen.findByText("auth.login");
    fireEvent.click(screen.getByRole("button", { name: "View JSON for auth.login" }));
    const drawer = await screen.findByRole("dialog");
    expect(within(drawer).getByText("invalid username or password")).toBeInTheDocument();
  });

  it("leaves out the automatic request records unless asked", async () => {
    renderSection();
    await screen.findByText("settings.config.update");
    expect(requested.some((u) => u.includes("exclude=http.request%2Cmcp.call"))).toBe(true);
    fireEvent.click(screen.getByRole("switch", { name: "Show automatic request records" }));
    await waitFor(() => expect(requested.some((u) => !u.includes("exclude="))).toBe(true));
  });
});
