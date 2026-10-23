import { describe, expect, it } from "vitest";
import { runStatusDisplay } from "./runStatus";

describe("runStatusDisplay", () => {
  it("shows Success for an explicit success status", () => {
    expect(runStatusDisplay({ source: "history", status: "success", success: true, exit_code: 0 }).label).toBe("Success");
  });

  it("shows Success (not an em dash) for legacy summaries that omitted exit code 0", () => {
    const d = runStatusDisplay({ source: "history", status: "success", status_source: "legacy_summary" });
    expect(d.label).toBe("Success");
    expect(d.hint).toMatch(/older runner/);
  });

  it("falls back to success/exit_code from older API servers", () => {
    expect(runStatusDisplay({ source: "history", success: true }).label).toBe("Success");
    expect(runStatusDisplay({ source: "history", success: false, exit_code: 1 }).label).toBe("Failed · exit 1");
  });

  it("distinguishes partial and failed runs", () => {
    expect(runStatusDisplay({ source: "summary", status: "partial", exit_code: 3 })).toMatchObject({ label: "Partial · exit 3", color: "warning" });
    expect(runStatusDisplay({ source: "summary", status: "failed", exit_code: 1 })).toMatchObject({ label: "Failed · exit 1", color: "error" });
  });

  it("never treats missing data as success", () => {
    const d = runStatusDisplay({ source: "history" });
    expect(d.label).toBe("Unknown");
    expect(d.label).not.toBe("—");
    expect(runStatusDisplay({ source: "history", status: "unknown" }).label).toBe("Unknown");
  });

  it("maps trigger rows", () => {
    expect(runStatusDisplay({ source: "trigger", status: "triggered" }).label).toBe("Triggered");
    expect(runStatusDisplay({ source: "trigger", success: false }).label).toBe("Trigger failed");
  });
});
