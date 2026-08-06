import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import App from "./App";
import { AppThemeProvider } from "./theme";

vi.mock("./notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

describe("App", () => {
  it("renders the accessible home link", () => {
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
      screen.getByRole("link", { name: /NCC Orchestrator home/i }),
    ).toBeInTheDocument();
  });
});
