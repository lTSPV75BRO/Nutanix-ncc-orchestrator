import { describe, expect, it } from "vitest";
import { everyLabel, mergeFreshness, overdueScheduleText, type OverviewClusterRow } from "./freshness";

const row = (address: string, ok = true): OverviewClusterRow => ({ name: address, address, ok, error: ok ? "" : "down", fail: 0, warn: 0, err: 0, score: null });

describe("everyLabel", () => {
  it("reads intervals as words", () => {
    expect(everyLabel("1d")).toBe("day");
    expect(everyLabel("4h")).toBe("4 hours");
    expect(everyLabel("15m")).toBe("15 minutes");
    expect(everyLabel("7d")).toBe("7 days");
    expect(everyLabel("weird")).toBe("weird");
  });
});

describe("overdueScheduleText", () => {
  it("names environment schedules and covers a schedule that never ran", () => {
    const since = new Date(Date.now() - 3 * 86400_000).toISOString();
    expect(overdueScheduleText({ kind: "environment", environment: "prod", every: "1d", interval_s: 86400, since, overdue: true })).toBe(
      "The prod schedule runs every day, but no run has finished since it was set up 3d ago.",
    );
  });
});

describe("mergeFreshness", () => {
  it("keeps rows untouched without history", () => {
    const rows = [row("a")];
    expect(mergeFreshness(rows, undefined, () => "")).toBe(rows);
    expect(mergeFreshness(rows, [], () => "")).toBe(rows);
  });

  it("matches addresses case-insensitively and appends unseen clusters with their own counts", () => {
    const out = mergeFreshness(
      [row("PE-A", false)],
      [
        { address: "pe-a", last_checked_at: "2026-11-15T10:00:00Z", ok: false, last_success_at: "2026-11-14T10:00:00Z", failed_runs: 2, fail_count: 9, warn_count: 9, err_count: 9 },
        { address: "pe-b", last_checked_at: "2026-11-15T09:00:00Z", ok: true, fail_count: 1, warn_count: 2, err_count: 3, health_score: 70 },
        { address: "pe-c", last_checked_at: "2026-11-15T09:00:00Z", ok: false, error: "401", fail_count: 5, warn_count: 0, err_count: 0 },
      ],
      (a) => (a === "pe-b" ? "Cluster B" : ""),
    );
    expect(out).toHaveLength(3);
    expect(out[0]).toMatchObject({ address: "PE-A", ok: false, lastDataAt: "2026-11-14T10:00:00Z", failedRuns: 2, fail: 0 });
    expect(out[1]).toMatchObject({ name: "Cluster B", ok: true, fail: 1, warn: 2, err: 3, score: 70 });
    // A cluster that failed has no current counts.
    expect(out[2]).toMatchObject({ name: "pe-c", ok: false, error: "401", fail: 0, score: null });
  });
});
