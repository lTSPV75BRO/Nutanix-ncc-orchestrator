import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import type { RetentionPreview } from "../../api/types";
import { RetentionPreviewPanel } from "./RetentionPreviewPanel";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const PREVIEW: RetentionPreview = {
  dry_run: true,
  generated_at: "2026-11-17T10:00:00Z",
  total_count: 2,
  total_bytes: 3072,
  rules: [
    { id: "runs", title: "Run history", dir: "/x/runs", enabled: false, settings: { "retain-last": 0, "retain-days": 0 }, count: 0, bytes: 0, items: [] },
    {
      id: "raw-logs",
      title: "Raw NCC logs",
      dir: "/x/nccfiles",
      enabled: true,
      settings: { "raw-log-retain-days": 30 },
      count: 2,
      bytes: 3072,
      items: [
        { path: "/x/nccfiles/old-a.log", name: "old-a.log", bytes: 1024, modified: "2026-09-01T00:00:00Z" },
        { path: "/x/nccfiles/old-b.log", name: "old-b.log", bytes: 2048, modified: "2026-09-02T00:00:00Z" },
      ],
    },
  ],
};

describe("RetentionPreviewPanel", () => {
  it("loads the preview on demand and lists each rule", async () => {
    const spy = vi.spyOn(api, "retentionPreview").mockResolvedValue(PREVIEW);
    render(<RetentionPreviewPanel />);
    expect(spy).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: /Preview what would be removed/ }));
    expect(await screen.findByText("2 items would be removed (3.0 KiB).")).toBeTruthy();
    expect(screen.getByText("Off: keeps everything.")).toBeTruthy();
    expect(screen.getByText("/x/nccfiles/old-a.log")).toBeTruthy();
    expect(screen.getByText("2 items, 3.0 KiB")).toBeTruthy();
  });

  it("shows the error when the preview fails", async () => {
    vi.spyOn(api, "retentionPreview").mockRejectedValue(new Error("retention preview failed: boom"));
    render(<RetentionPreviewPanel />);
    fireEvent.click(screen.getByRole("button", { name: /Preview what would be removed/ }));
    expect(await screen.findByText("retention preview failed: boom")).toBeTruthy();
  });
});
