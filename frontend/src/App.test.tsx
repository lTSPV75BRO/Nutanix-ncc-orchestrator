import { render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";
import { MemoryRouter, useLocation } from "react-router";
import App from "./App";
import { AppThemeProvider } from "./theme";

vi.mock("./notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

describe("App", () => {
  it("renders the accessible home link", async () => {
    globalThis.fetch = vi.fn(async () => ({
      ok: true,
      status: 200,
      statusText: "OK",
      headers: new Headers({ "content-type": "application/json" }),
      json: async () => ({ success: true, data: {} }),
    })) as unknown as typeof fetch;
    const queryClient = new QueryClient();
    render(
      <AppThemeProvider>
        <QueryClientProvider client={queryClient}>
          <MemoryRouter>
            <App />
          </MemoryRouter>
        </QueryClientProvider>
      </AppThemeProvider>,
    );
    expect(
      await screen.findByRole("link", { name: /NCC Orchestrator home/i }),
    ).toBeInTheDocument();
    // Without sign-in, About lives in the "More options" menu.
    expect(screen.getByRole("button", { name: "More options" })).toBeInTheDocument();
  });

  function renderAt(path: string, me: Record<string, unknown>) {
    globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => ({
      ok: true,
      status: 200,
      statusText: "OK",
      headers: new Headers({ "content-type": "application/json" }),
      json: async () => ({ success: true, data: String(input).includes("/auth/me") ? me : {} }),
    })) as unknown as typeof fetch;
    const seen: { path: string } = { path: "" };
    function Probe() {
      seen.path = useLocation().pathname;
      return null;
    }
    render(
      <AppThemeProvider>
        <QueryClientProvider client={new QueryClient()}>
          <MemoryRouter initialEntries={[path]}>
            <App />
            <Probe />
          </MemoryRouter>
        </QueryClientProvider>
      </AppThemeProvider>,
    );
    return seen;
  }

  it("sends / to /overview when no sign-in is needed", async () => {
    const seen = renderAt("/", { authenticated: false, login_enabled: false });
    await waitFor(() => expect(seen.path).toBe("/overview"));
  });

  it("sends / to /login when signed out", async () => {
    const seen = renderAt("/", { authenticated: false, login_enabled: true, local_enabled: true });
    await waitFor(() => expect(seen.path).toBe("/login"));
    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
  });

  it("keeps a deep link's address on the sign-in screen", async () => {
    const seen = renderAt("/findings", { authenticated: false, login_enabled: true, local_enabled: true });
    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
    expect(seen.path).toBe("/findings");
  });
});
