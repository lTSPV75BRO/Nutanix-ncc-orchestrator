import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthContext, type AuthValue } from "../auth/AuthContext";
import { INTEGRATIONS, fillSnippet, parseApiAddress } from "../features/integrations/catalog";
import { IntegrationsPage } from "./IntegrationsPage";

const viewer: AuthValue = { me: null, role: "viewer", isAdmin: false, canOperate: false, loginEnabled: true, authenticated: true };
const admin: AuthValue = { ...viewer, role: "admin", isAdmin: true, canOperate: true };

function ok(data: unknown) {
  return {
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({ success: true, data }),
  } as Response;
}

function renderAt(path: string, auth: AuthValue = viewer) {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/api/v1/alerts/ncc")) return ok({ items: [], count: 3, next_cursor: "", last_run: "" });
    if (url.includes("/settings/notifications/status")) {
      return ok({ channels: [{ channel: "slack", enabled: true, state: "ok", dead_letters: 0 }], dead_letter_configured: false });
    }
    return ok({});
  }) as typeof fetch;
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <AuthContext.Provider value={auth}>
        <MemoryRouter initialEntries={[path]}>
          <Routes>
            <Route path="/integrations" element={<IntegrationsPage />} />
            <Route path="/integrations/:id" element={<IntegrationsPage />} />
          </Routes>
        </MemoryRouter>
      </AuthContext.Provider>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  window.localStorage.clear();
});

afterEach(() => {
  cleanup();
});

describe("IntegrationsPage", () => {
  it("lists every integration and filters by search", () => {
    renderAt("/integrations");
    for (const i of INTEGRATIONS) {
      expect(screen.getByRole("link", { name: new RegExp(`^${i.name.replace(/[()&]/g, ".")}`) })).toBeInTheDocument();
    }
    fireEvent.change(screen.getByLabelText("Search integrations"), { target: { value: "splunk" } });
    expect(screen.getByRole("link", { name: /^Splunk/ })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /^ServiceNow/ })).not.toBeInTheDocument();
  });

  it("fills the API address into a guide's snippets", () => {
    window.localStorage.setItem("integrations.apiAddress", JSON.stringify("ncc-api.example.com:8081/"));
    renderAt("/integrations/prometheus");
    expect(screen.getByRole("heading", { name: "Prometheus & Grafana" })).toBeInTheDocument();
    const scrape = screen.getByText("prometheus.yml", { selector: "figcaption span" }).closest("figure") as HTMLElement;
    expect(within(scrape).getByText(/targets: \["ncc-api\.example\.com:8081"\]/)).toBeInTheDocument();
    expect(within(scrape).getByText(/scheme: https/)).toBeInTheDocument();
  });

  it("remembers ticked steps and shows progress", () => {
    renderAt("/integrations/mcp");
    const boxes = screen.getAllByRole("checkbox", { name: "Done" });
    fireEvent.click(boxes[0]);
    expect(screen.getByText("1 of 4 steps done")).toBeInTheDocument();
    cleanup();
    renderAt("/integrations/mcp");
    expect(screen.getAllByRole("checkbox", { name: "Done" })[0]).toBeChecked();
  });

  it("says when every required step is done", () => {
    renderAt("/integrations/mcp");
    const required = INTEGRATIONS.find((i) => i.id === "mcp")!.steps.filter((s) => !s.optional).length;
    const boxes = screen.getAllByRole("checkbox", { name: "Done" });
    for (let n = 0; n < required; n++) fireEvent.click(boxes[n]);
    expect(screen.getByText(/All required steps are done/)).toBeInTheDocument();
  });

  it("sends viewers to an admin for admin-only steps", () => {
    renderAt("/integrations/servicenow");
    expect(screen.getAllByText("An admin does this step.").length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: /Open Access settings/ })).not.toBeInTheDocument();
  });

  it("checks NCC is answering from the browser", async () => {
    renderAt("/integrations/scripts");
    fireEvent.click(screen.getByRole("button", { name: /Check NCC is answering/ }));
    await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("NCC is answering: 3 open findings."));
  });

  it("shows admins their notification channels", async () => {
    renderAt("/integrations/notifications", admin);
    expect(await screen.findByText("Delivering")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Open Notifications/ })).toBeInTheDocument();
  });

  it("handles an unknown guide", () => {
    renderAt("/integrations/nope");
    expect(screen.getByText("No such integration")).toBeInTheDocument();
  });
});

describe("parseApiAddress", () => {
  it("adds https and drops a trailing slash", () => {
    expect(parseApiAddress("ncc.example.com:8081/")).toEqual({ api: "https://ncc.example.com:8081", host: "ncc.example.com:8081", scheme: "https" });
    expect(parseApiAddress("http://10.0.0.5:8081")).toEqual({ api: "http://10.0.0.5:8081", host: "10.0.0.5:8081", scheme: "http" });
  });

  it("fills every placeholder", () => {
    const addr = parseApiAddress("http://h:1");
    expect(fillSnippet("{{SCHEME}}://{{HOST}} {{API}}/x {{API}}", addr)).toBe("http://h:1 http://h:1/x http://h:1");
  });
});
