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
  // The first dynamic import transforms the module graph cold; under a full
  // parallel run on a loaded machine that alone exceeded vitest's 5s default
  // (it passes in ~2s in isolation), so give the import time to finish.
  it("registers a same-origin worker factory", { timeout: 30_000 }, async () => {
    await import("./MonacoCodeEditor");
    const environment = (globalThis as typeof globalThis & {
      MonacoEnvironment?: { getWorker?: (id: string, label: string) => unknown };
    }).MonacoEnvironment;
    expect(environment?.getWorker).toEqual(expect.any(Function));
  });
});
