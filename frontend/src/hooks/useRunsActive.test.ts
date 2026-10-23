import { describe, expect, it } from "vitest";
import type { RunActiveData } from "../api/types";
import { runsActivePollInterval } from "./useRunsActive";

const cfg = { activeMs: 3000, idleMs: 30000, maxBackoffMs: 300000 };
const idle = { active: false, started_at: "", last_error: "", output_dir: "", config_path: "" } as RunActiveData;

describe("runsActivePollInterval", () => {
  it("polls slowly while idle and quickly while a run is active or queued", () => {
    expect(runsActivePollInterval(idle, {}, cfg)).toBe(30000);
    expect(runsActivePollInterval({ ...idle, active: true }, {}, cfg)).toBe(3000);
    expect(runsActivePollInterval({ ...idle, queued_count: 1 }, {}, cfg)).toBe(3000);
    expect(runsActivePollInterval(undefined, {}, cfg)).toBe(30000);
  });

  it("stops while the page is hidden", () => {
    expect(runsActivePollInterval({ ...idle, active: true }, { hidden: true }, cfg)).toBe(false);
  });

  it("backs off exponentially on consecutive errors, with a cap", () => {
    expect(runsActivePollInterval(idle, { consecutiveErrors: 1 }, cfg)).toBe(60000);
    expect(runsActivePollInterval(idle, { consecutiveErrors: 2 }, cfg)).toBe(120000);
    expect(runsActivePollInterval(idle, { consecutiveErrors: 20 }, cfg)).toBe(300000);
  });

  it("idle request rate is at most 2/min (was ~12+/min from overlapping pollers)", () => {
    const perMinute = 60000 / (runsActivePollInterval(idle, {}, cfg) as number);
    expect(perMinute).toBeLessThanOrEqual(2);
  });
});
