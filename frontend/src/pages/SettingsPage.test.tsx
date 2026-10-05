import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SettingsPage } from "./SettingsPage";
import { AppThemeProvider } from "../theme";

vi.mock("../features/settings/ConfigSection", () => ({
  ConfigSection: () => <div>Lazy config loaded</div>,
}));
vi.mock("../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

function defaultFetchImpl(input: RequestInfo | URL) {
  const url = String(input);
  const data = url.includes("/runs") && !url.includes("/configs") ? [] : { status: "ok", items: [] };
  return Promise.resolve({
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({ success: true, data }),
  } as Response);
}

function renderSettings(opts?: { fetchImpl?: typeof fetch; isAdmin?: boolean; path?: string }) {
  globalThis.fetch = vi.fn(opts?.fetchImpl ?? defaultFetchImpl) as typeof fetch;
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <AppThemeProvider>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={[opts?.path ?? "/settings?tab=connection"]}>
          <SettingsPage isAdmin={opts?.isAdmin ?? true} />
        </MemoryRouter>
      </QueryClientProvider>
    </AppThemeProvider>,
  );
}

function fetchedPaths(): string[] {
  return vi.mocked(fetch).mock.calls.map(([input]) => {
    if (typeof input === "string") return input;
    if (input instanceof URL) return input.pathname + input.search;
    return String((input as Request).url);
  });
}

describe("SettingsPage", () => {
  afterEach(() => {
    cleanup();
    try {
      window.localStorage.clear();
    } catch {
      // jsdom may not expose localStorage
    }
  });

  it("loads a settings section only after its tab is selected", async () => {
    renderSettings();
    await screen.findByText("Connected");
    fireEvent.click(screen.getByRole("tab", { name: /Config/ }));
    await waitFor(() => expect(screen.getByText("Lazy config loaded")).toBeInTheDocument());
  });

  it("shows an API failure state when health cannot be loaded", async () => {
    renderSettings({
      fetchImpl: async () => {
        throw new Error("backend unavailable");
      },
    });
    expect(await screen.findByText("Unable to load settings health context")).toBeInTheDocument();
    expect(screen.getAllByRole("alert")[0]).toHaveTextContent("backend unavailable");
  });

  it("does not call admin-only APIs when an operator opens settings tabs", async () => {
    renderSettings({ isAdmin: false });
    await screen.findByText("Connected");
    expect(screen.queryByRole("tab", { name: /Config/ })).not.toBeInTheDocument();
    expect(fetchedPaths().some((p) => p.includes("/api/v1/settings/"))).toBe(false);

    fireEvent.click(screen.getByRole("tab", { name: /Schedule/ }));
    await screen.findByText("Scheduler Health");
    await waitFor(() => {
      expect(fetchedPaths().some((p) => p.includes("/api/v1/runs/configs"))).toBe(true);
    });
    expect(fetchedPaths().some((p) => p.includes("/api/v1/settings/"))).toBe(false);
  });
});
