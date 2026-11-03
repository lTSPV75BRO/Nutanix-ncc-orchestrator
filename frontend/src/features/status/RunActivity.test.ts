import { describe, expect, it } from "vitest";
import type { RunInfo } from "../../api/types";
import { buildRunActivity } from "./RunActivity";

const at = (y: number, m: number, d: number, h = 12, min = 0) => new Date(y, m - 1, d, h, min).toISOString();

describe("buildRunActivity", () => {
  it("buckets runs per local day by outcome and ignores triggers and duplicates", () => {
    const now = new Date(2026, 10, 3, 15, 0);
    const runs: RunInfo[] = [
      { id: "a", mod_time: at(2026, 11, 3, 9), has_index: true, source: "history", status: "success" },
      { id: "a-summary", mod_time: at(2026, 11, 3, 9), has_index: false, source: "summary", status: "success" },
      { id: "b", mod_time: at(2026, 11, 2, 9), has_index: true, source: "history", status: "partial" },
      { id: "c", mod_time: at(2026, 11, 2, 18), has_index: true, source: "history", success: false },
      { id: "d", mod_time: at(2026, 10, 29, 8), has_index: false, source: "history", status: "unknown" },
      { id: "t", mod_time: at(2026, 11, 3, 10), has_index: false, source: "trigger", status: "triggered" },
      { id: "old", mod_time: at(2026, 10, 1, 8), has_index: true, source: "history", status: "success" },
    ];
    const a = buildRunActivity(runs, 7, now);
    expect(a.days).toHaveLength(7);
    expect(a.days[6]).toMatchObject({ isToday: true, ok: 1, total: 1 });
    expect(a.days[5]).toMatchObject({ problem: 2, total: 2 });
    expect(a.days[1]).toMatchObject({ unknown: 1, total: 1 });
    expect(a).toMatchObject({ total: 4, ok: 1, problem: 2 });
    expect(a.lastRun).toBe(at(2026, 11, 3, 9));
  });

  it("reports an empty week", () => {
    const a = buildRunActivity([], 7, new Date(2026, 10, 3));
    expect(a.total).toBe(0);
    expect(a.lastRun).toBeNull();
    expect(a.days.every((d) => d.total === 0)).toBe(true);
  });
});
