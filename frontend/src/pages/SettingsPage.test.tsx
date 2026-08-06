import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
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

function renderSettings(
  fetchImpl: typeof fetch = async (input) => {
    const data = String(input).includes("/runs") ? [] : { status: "ok" };
    return {
      ok: true,
      status: 200,
      statusText: "OK",
      headers: new Headers({ "content-type": "application/json" }),
      json: async () => ({ success: true, data }),
    } as Response;
  },
) {
  globalThis.fetch = vi.fn(fetchImpl) as typeof fetch;
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <AppThemeProvider>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <SettingsPage />
        </MemoryRouter>
      </QueryClientProvider>
    </AppThemeProvider>,
  );
}

describe("SettingsPage", () => {
  it("loads a settings section only after its tab is selected", async () => {
    renderSettings();
    await screen.findByText("Connected");
    fireEvent.click(screen.getByRole("tab", { name: /Config/ }));
    await waitFor(() => expect(screen.getByText("Lazy config loaded")).toBeInTheDocument());
  });

  it("shows an API failure state when health cannot be loaded", async () => {
    renderSettings(async () => {
      throw new Error("backend unavailable");
    });
    expect(await screen.findByText("Unable to load settings health context")).toBeInTheDocument();
    expect(screen.getAllByRole("alert")[0]).toHaveTextContent("backend unavailable");
  });
});
