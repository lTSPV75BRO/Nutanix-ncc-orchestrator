import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AccessSection } from "./AccessSection";

vi.mock("../../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn(), loading: vi.fn(), close: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

function Where() {
  const l = useLocation();
  return <div data-testid="where">{l.search}</div>;
}

function renderAccess(search: string) {
  globalThis.fetch = vi.fn(async () => ({
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({ success: true, data: {} }),
  })) as unknown as typeof fetch;
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={[`/settings/access${search}`]}>
        <AccessSection />
        <Where />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const selected = () => document.querySelector(".access-tabs .ant-tabs-tab-active")?.textContent;

describe("AccessSection tabs", () => {
  afterEach(() => cleanup());

  it("opens on Users & roles", () => {
    renderAccess("");
    expect(selected()).toBe("Users & roles");
  });

  it("opens the tab that holds a ?focus= card (search shortcuts)", () => {
    renderAccess("?focus=tls");
    expect(selected()).toBe("HTTPS");
    cleanup();
    renderAccess("?focus=ldap");
    expect(selected()).toBe("Sign-in");
  });

  it("keeps the chosen tab in the URL", () => {
    renderAccess("?focus=users");
    fireEvent.click(screen.getByRole("tab", { name: "API tokens" }));
    expect(screen.getByTestId("where")).toHaveTextContent("?view=tokens");
    expect(selected()).toBe("API tokens");
  });
});
