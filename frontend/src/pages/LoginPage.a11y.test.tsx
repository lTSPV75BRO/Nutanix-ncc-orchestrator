import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render } from "@testing-library/react";
import axe from "axe-core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LoginPage } from "./LoginPage";

vi.mock("../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

// jsdom has no layout engine, so colour contrast cannot be computed here; it
// needs a real browser pass. Every other WCAG 2.x A/AA rule runs.
async function violations(container: HTMLElement) {
  const result = await axe.run(container, {
    runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa", "best-practice"] },
    rules: { "color-contrast": { enabled: false }, region: { enabled: false } },
  });
  return result.violations.map((v) => `${v.id}: ${v.help} (${v.nodes.map((n) => n.target.join(" ")).join("; ")})`);
}

beforeEach(() => {
  globalThis.fetch = vi.fn(async () => ({
    ok: true,
    status: 200,
    statusText: "",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({ success: true, data: { status: "ok" } }),
    text: async () => "{}",
  })) as unknown as typeof fetch;
});
afterEach(() => cleanup());

function renderLogin(props: Partial<Parameters<typeof LoginPage>[0]> = {}) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <LoginPage localEnabled samlEnabled={false} bootstrapPending={false} onSuccess={vi.fn()} {...props} />
    </QueryClientProvider>,
  );
}

describe("LoginPage accessibility (axe-core)", () => {
  it("local sign-in form has no violations", async () => {
    const { container } = renderLogin();
    expect(await violations(container)).toEqual([]);
  });
  it("directory-only form has no violations", async () => {
    const { container } = renderLogin({ localEnabled: false, ldapEnabled: true });
    expect(await violations(container)).toEqual([]);
  });
  it("single sign-in options have no violations", async () => {
    const { container } = renderLogin({ oidcEnabled: true, oidcLabel: "Microsoft", samlEnabled: true });
    expect(await violations(container)).toEqual([]);
  });
});

describe("axe control", () => {
  // Proves the helper can fail: an input with no accessible name must be reported.
  it("reports an unlabelled input", async () => {
    const { container } = render(<input type="text" />);
    const found = await violations(container);
    expect(found.some((v) => v.startsWith("label:"))).toBe(true);
  });
});
