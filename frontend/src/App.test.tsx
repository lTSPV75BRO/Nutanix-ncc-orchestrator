import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import App from "./App";
import { AppThemeProvider } from "./theme";

describe("App", () => {
  it("renders page title", () => {
    globalThis.fetch = vi.fn(async () => ({
      ok: true,
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
    expect(screen.getByText(/NCC Orchestrator v2/i)).toBeInTheDocument();
  });
});
