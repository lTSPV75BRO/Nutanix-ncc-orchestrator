import { describe, expect, it, vi } from "vitest";

vi.mock("@monaco-editor/react", () => ({
  default: () => null,
  loader: { config: vi.fn() },
}));

vi.mock("monaco-editor", () => ({
  editor: {
    defineTheme: vi.fn(),
    setTheme: vi.fn(),
  },
}));

describe("Monaco worker setup", () => {
  it("registers a same-origin worker factory", async () => {
    await import("./MonacoCodeEditor");
    const environment = (globalThis as typeof globalThis & {
      MonacoEnvironment?: { getWorker?: (id: string, label: string) => unknown };
    }).MonacoEnvironment;
    expect(environment?.getWorker).toEqual(expect.any(Function));
  });
});
