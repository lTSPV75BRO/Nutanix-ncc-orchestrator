import { describe, expect, it } from "vitest";
import type { NCCAlert, NCCAlertEvent } from "../api/types";
import {
  ageBucket,
  buildLifecycleInsights,
  formatSpan,
  looksFixed,
  severityChanges,
  summarizeFinding,
} from "./alertLifecycle";

const DAY = 86_400_000;
const NOW = Date.parse("2026-10-23T12:00:00Z");
const daysAgo = (n: number) => new Date(NOW - n * DAY).toISOString();

let seq = 0;
function alert(over: Partial<NCCAlert> = {}): NCCAlert {
  seq += 1;
  return {
    id: `00000000-0000-4000-8000-${String(seq).padStart(12, "0")}`,
    finding_id: "f_a",
    correlation_key: "ncc|10.0.0.1|cvm memory",
    source: "NCC",
    type: "ncc_check",
    cluster: "10.0.0.1",
    cluster_name: "prod-a",
    check: "CVM memory",
    state: "open",
    detection: "detected",
    closed: false,
    created_at: daysAgo(1),
    updated_at: daysAgo(1),
    revision: 1,
    ...over,
  };
}

describe("formatSpan and ageBucket", () => {
  it("formats short and long spans", () => {
    expect(formatSpan(30_000)).toBe("under 1 min");
    expect(formatSpan(45 * 60_000)).toBe("45 min");
    expect(formatSpan(5 * 3_600_000)).toBe("5 h");
    expect(formatSpan(12 * DAY)).toBe("12 d");
    expect(formatSpan(null)).toBe("—");
  });

  it("puts ages in the right bucket", () => {
    expect(ageBucket(DAY - 1)).toBe("day");
    expect(ageBucket(DAY)).toBe("week");
    expect(ageBucket(8 * DAY)).toBe("month");
    expect(ageBucket(31 * DAY)).toBe("older");
  });
});

describe("summarizeFinding", () => {
  it("counts recurrences and reports the latest resolution", () => {
    const first = alert({ closed: true, close_reason: "recurred", state: "resolved", first_seen_at: daysAgo(20), resolved_at: daysAgo(15), resolved_by: "jdoe", note: "restarted CVM" });
    const second = alert({ closed: true, close_reason: "recurred", state: "resolved", first_seen_at: daysAgo(10), resolved_at: daysAgo(8), resolved_by: "ann" });
    const current = alert({ first_seen_at: daysAgo(3), created_at: daysAgo(3), reopened: true });
    const s = summarizeFinding([first, second, current], NOW);
    expect(s.active?.id).toBe(current.id);
    expect(s.recurrences).toBe(2);
    expect(s.occurrences).toBe(3);
    expect(s.openForMs).toBe(3 * DAY);
    expect(s.firstEverMs).toBe(NOW - 20 * DAY);
    expect(s.lastResolution).toEqual({ by: "ann", at: daysAgo(8), note: undefined });
    expect(s.looksFixed).toBe(false);
  });

  it("flags an open alert the latest run no longer reports", () => {
    const a = alert({ state: "acknowledged", detection: "not_detected", not_detected_since_run: daysAgo(1) });
    expect(looksFixed(a)).toBe(true);
    expect(summarizeFinding([a], NOW).looksFixed).toBe(true);
    // A resolved alert waiting for confirmation is not "open", so not flagged.
    expect(looksFixed(alert({ state: "resolved", detection: "not_detected" }))).toBe(false);
  });

  it("has no open time for a resolved active instance", () => {
    expect(summarizeFinding([alert({ state: "resolved" })], NOW).openForMs).toBeNull();
  });
});

describe("severityChanges", () => {
  it("orders by seq and marks escalations", () => {
    const base = { id: "e", alert_id: "a", correlation_key: "k", actor: "NCC Orchestrator" };
    const events: NCCAlertEvent[] = [
      { ...base, seq: 5, type: "severity_changed", at: daysAgo(1), from_severity: "FAIL", to_severity: "WARN" },
      { ...base, seq: 2, type: "detected", at: daysAgo(3) },
      { ...base, seq: 3, type: "severity_changed", at: daysAgo(2), from_severity: "WARN", to_severity: "FAIL" },
    ];
    expect(severityChanges(events)).toEqual([
      { at: daysAgo(2), from: "WARN", to: "FAIL", escalated: true },
      { at: daysAgo(1), from: "FAIL", to: "WARN", escalated: false },
    ]);
  });
});

describe("buildLifecycleInsights", () => {
  it("buckets open alerts by age per cluster and skips closed or resolved ones", () => {
    const insights = buildLifecycleInsights(
      [
        alert({ finding_id: "f1", first_seen_at: daysAgo(0.5) }),
        alert({ finding_id: "f2", first_seen_at: daysAgo(3), state: "acknowledged" }),
        alert({ finding_id: "f3", first_seen_at: daysAgo(45), cluster: "10.0.0.2", cluster_name: "prod-b" }),
        alert({ finding_id: "f4", first_seen_at: daysAgo(45), state: "resolved" }),
        alert({ finding_id: "f5", first_seen_at: daysAgo(45), closed: true, state: "resolved" }),
      ],
      NOW,
    );
    expect(insights.open).toBe(3);
    expect(insights.buckets).toEqual({ day: 1, week: 1, month: 0, older: 1 });
    expect(insights.clusters.map((c) => c.name)).toEqual(["prod-b", "prod-a"]);
    expect(insights.clusters[0].oldestMs).toBe(45 * DAY);
  });

  it("computes median response times and ignores imported legacy marks", () => {
    const insights = buildLifecycleInsights(
      [
        alert({ finding_id: "f1", first_seen_at: daysAgo(10), acknowledged_at: daysAgo(9), resolved_at: daysAgo(6), state: "resolved" }),
        alert({ finding_id: "f2", first_seen_at: daysAgo(10), acknowledged_at: daysAgo(7) }),
        alert({ finding_id: "f3", first_seen_at: daysAgo(10), acknowledged_at: daysAgo(4) }),
        alert({ finding_id: "f4", first_seen_at: daysAgo(10), acknowledged_at: daysAgo(10), legacy_migrated: true }),
      ],
      NOW,
    );
    expect(insights.ackSamples).toBe(3);
    expect(insights.medianAckMs).toBe(3 * DAY);
    expect(insights.resolveSamples).toBe(1);
    expect(insights.medianResolveMs).toBe(4 * DAY);
  });

  it("ranks findings that came back and lists open alerts no longer reported", () => {
    const insights = buildLifecycleInsights(
      [
        alert({ finding_id: "once", closed: true, close_reason: "recurred", check: "Disk usage" }),
        alert({ finding_id: "once", check: "Disk usage" }),
        alert({ finding_id: "twice", closed: true, close_reason: "recurred", resolved_by: "jdoe", resolved_at: daysAgo(5) }),
        alert({ finding_id: "twice", closed: true, close_reason: "recurred" }),
        alert({ finding_id: "twice", state: "acknowledged", detection: "not_detected", not_detected_since_run: daysAgo(1) }),
        alert({ finding_id: "gone", closed: true, close_reason: "resolved_not_detected" }),
      ],
      NOW,
    );
    expect(insights.recurring.map((r) => [r.findingId, r.recurrences])).toEqual([
      ["twice", 2],
      ["once", 1],
    ]);
    expect(insights.recurring[0].lastResolution?.by).toBe("jdoe");
    expect(insights.recurring[0].openNow).toBe(true);
    expect(insights.looksFixed.map((a) => a.finding_id)).toEqual(["twice"]);
  });
});
