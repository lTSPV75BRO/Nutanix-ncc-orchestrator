import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { ConfigSection } from "./ConfigSection";

vi.mock("../../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn(), loading: vi.fn(), close: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));
vi.mock("./exampleConfig", () => ({ default: "clusters: x\n" }));

const EXAMPLE = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "..", "examples", "example_config.yaml"), "utf8");

function renderPage(content = EXAMPLE) {
  vi.spyOn(api, "loadConfig").mockResolvedValue({ content, path: "config.yaml" } as Awaited<ReturnType<typeof api.loadConfig>>);
  vi.spyOn(api, "listConfigs").mockResolvedValue({ items: [] } as unknown as Awaited<ReturnType<typeof api.listConfigs>>);
  vi.spyOn(api, "listConfigFiles").mockResolvedValue({ items: [] } as unknown as Awaited<ReturnType<typeof api.listConfigFiles>>);
  const save = vi.spyOn(api, "saveConfig").mockResolvedValue({} as Awaited<ReturnType<typeof api.saveConfig>>);
  render(<ConfigSection onError={vi.fn()} />);
  return { save };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("ConfigSection", () => {
  it("shows nested values and saves new settings under the nested layout", async () => {
    const { save } = renderPage();
    expect(await screen.findByText("Nested layout")).toBeInTheDocument();
    expect(screen.getByRole("switch", { name: /Keep run history/ })).toBeChecked();

    fireEvent.click(screen.getByRole("switch", { name: /Fix problems automatically/ }));
    fireEvent.change(screen.getByLabelText(/Per-cluster timeout/), { target: { value: "25m" } });
    expect(await screen.findByText("2 settings changed · Ctrl/⌘+S to save")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Save config" }));
    await waitFor(() => expect(save).toHaveBeenCalled());
    const saved = save.mock.calls[0][0];
    expect(saved).toMatch(/automation:\n\s+enabled: true/);
    expect(saved).toMatch(/execution:\n\s+timeout: 25m/);
    expect(saved).not.toMatch(/^(auto|timeout):/m);
  });

  it("blocks saving an invalid duration", async () => {
    const { save } = renderPage("timeout: 15m\n");
    await screen.findByText("Flat layout");
    fireEvent.change(screen.getByLabelText(/Per-cluster timeout/), { target: { value: "fifteen minutes" } });
    expect(await screen.findByText("Use a duration such as 30s, 5m or 1h30m.")).toBeInTheDocument();
    expect(screen.getByText("1 setting needs fixing before you can save.")).toBeInTheDocument();
    fireEvent.keyDown(window, { key: "s", ctrlKey: true });
    expect(save).not.toHaveBeenCalled();
  });

  it("finds settings by name or key and resets one to its default", async () => {
    renderPage("retry-max-attempts: 9\n");
    await screen.findByText("Flat layout");
    fireEvent.change(screen.getByRole("textbox", { name: "Find a setting" }), { target: { value: "webhook-headers" } });
    expect(screen.getByText("Extra headers")).toBeInTheDocument();
    expect(screen.queryByText("Per-cluster timeout")).not.toBeInTheDocument();

    fireEvent.change(screen.getByRole("textbox", { name: "Find a setting" }), { target: { value: "" } });
    const field = screen.getByText("Request retries").closest(".config-field") as HTMLElement;
    fireEvent.click(within(field).getByRole("button", { name: /Use default/ }));
    await waitFor(() => expect(within(field).getByText("Default: 6.")).toBeInTheDocument());
    expect(within(field).queryByRole("button", { name: /Use default/ })).toBeNull();
  });
});
