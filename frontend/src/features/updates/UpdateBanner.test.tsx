import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { UpdateBanner } from "./UpdateBanner";

function serve(data: unknown) {
  const fetchMock = vi.fn(async () => ({
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({ success: true, data }),
  })) as unknown as typeof fetch;
  globalThis.fetch = fetchMock;
  return fetchMock;
}

function renderBanner() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <UpdateBanner />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

beforeEach(() => window.localStorage.clear());
afterEach(() => cleanup());

describe("UpdateBanner", () => {
  it("says which version is available and links to Updates", async () => {
    const fetchMock = serve({ current_version: "2.4.0", latest_version: "2.5.0", update_available: true, checked_at: "2026-11-28T08:00:00Z" });
    renderBanner();
    expect(await screen.findByText(/Version 2\.5\.0 is available\. You have 2\.4\.0\./)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Open Updates & backups" }).getAttribute("href")).toBe("/system/updates");
    expect(String(vi.mocked(fetchMock).mock.calls[0][0])).toContain("/api/v1/settings/update/notice");
  });

  it("stays hidden for that version once dismissed", async () => {
    serve({ current_version: "2.4.0", latest_version: "2.5.0", update_available: true, checked_at: "2026-11-28T08:00:00Z" });
    renderBanner();
    fireEvent.click(await screen.findByRole("button", { name: "Dismiss the notice for version 2.5.0" }));
    expect(screen.queryByText(/Version 2\.5\.0 is available/)).not.toBeInTheDocument();
    cleanup();
    renderBanner();
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByText(/is available/)).not.toBeInTheDocument();
  });

  it("comes back for a newer version", async () => {
    window.localStorage.setItem("updateBanner.dismissed", "2.5.0");
    serve({ current_version: "2.4.0", latest_version: "2.6.0", update_available: true, checked_at: "2026-11-28T08:00:00Z" });
    renderBanner();
    expect(await screen.findByText(/Version 2\.6\.0 is available/)).toBeInTheDocument();
  });

  it("shows nothing when up to date", async () => {
    serve({ current_version: "2.4.0", latest_version: "2.4.0", update_available: false });
    renderBanner();
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByText(/is available/)).not.toBeInTheDocument();
  });
});
