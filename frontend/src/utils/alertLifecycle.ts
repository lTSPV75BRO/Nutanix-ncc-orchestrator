import type { NCCAlert, NCCAlertEvent } from "../api/types";
import { parseInstant } from "./datetime";

/** Query-key prefix shared by every lifecycle query, so one invalidation refreshes them all. */
export const NCC_ALERTS_QUERY_KEY = "ncc-alerts";

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

function ms(value: string | undefined): number | null {
  const d = parseInstant(value);
  return d ? d.getTime() : null;
}

/** When an instance started: the first run that reported it, else when it was created. */
export function alertStartMs(a: NCCAlert): number | null {
  return ms(a.first_seen_at) ?? ms(a.created_at);
}

/** Open means not closed and not resolved; a resolved instance waits for a run to confirm it. */
export function isOpenAlert(a: NCCAlert): boolean {
  return !a.closed && a.state !== "resolved";
}

/** Still open, but the latest run that covered its cluster no longer reports it. */
export function looksFixed(a: NCCAlert): boolean {
  return isOpenAlert(a) && a.detection === "not_detected";
}

/** Short duration such as "45 min", "5 h", "12 d". */
export function formatSpan(spanMs: number | null | undefined): string {
  if (spanMs == null || !Number.isFinite(spanMs) || spanMs < 0) return "—";
  if (spanMs < MINUTE) return "under 1 min";
  if (spanMs < HOUR) return `${Math.round(spanMs / MINUTE)} min`;
  if (spanMs < DAY) return `${Math.round(spanMs / HOUR)} h`;
  return `${Math.round(spanMs / DAY)} d`;
}

export type FindingSummary = {
  /** The current (not closed) instance, if any. */
  active?: NCCAlert;
  /** How long the active instance has been open; null when there is none or it is resolved. */
  openForMs: number | null;
  /** Earliest start across every instance of the finding. */
  firstEverMs: number | null;
  occurrences: number;
  /** Times a resolved instance was found again by a later run. */
  recurrences: number;
  lastResolution?: { by: string; at: string; note?: string };
  looksFixed: boolean;
};

/** Summarise every instance (open and closed) of one finding. */
export function summarizeFinding(instances: NCCAlert[], now = Date.now()): FindingSummary {
  const active = [...instances].filter((a) => !a.closed).sort((a, b) => (ms(b.created_at) ?? 0) - (ms(a.created_at) ?? 0))[0];
  let firstEverMs: number | null = null;
  let recurrences = 0;
  let lastResolution: FindingSummary["lastResolution"];
  let lastResolvedMs = -Infinity;
  for (const a of instances) {
    const start = alertStartMs(a);
    if (start != null && (firstEverMs == null || start < firstEverMs)) firstEverMs = start;
    if (a.close_reason === "recurred") recurrences += 1;
    const resolved = ms(a.resolved_at);
    if (resolved != null && resolved > lastResolvedMs && a.resolved_at) {
      lastResolvedMs = resolved;
      lastResolution = { by: a.resolved_by || "unknown", at: a.resolved_at, note: a.note || undefined };
    }
  }
  const activeStart = active ? alertStartMs(active) : null;
  return {
    active,
    openForMs: active && isOpenAlert(active) && activeStart != null ? Math.max(0, now - activeStart) : null,
    firstEverMs,
    occurrences: instances.length,
    recurrences,
    lastResolution,
    looksFixed: active ? looksFixed(active) : false,
  };
}

const SEVERITY_RANK: Record<string, number> = { INFO: 1, WARN: 2, ERR: 3, FAIL: 4 };

export function severityRank(severity: string | undefined): number {
  return SEVERITY_RANK[(severity || "").trim().toUpperCase()] ?? 0;
}

export type SeverityChange = { at: string; from: string; to: string; escalated: boolean };

/** Severity changes recorded for an alert, oldest first. */
export function severityChanges(events: NCCAlertEvent[]): SeverityChange[] {
  return events
    .filter((e) => e.type === "severity_changed" && e.from_severity && e.to_severity)
    .sort((a, b) => a.seq - b.seq)
    .map((e) => ({
      at: e.at,
      from: e.from_severity as string,
      to: e.to_severity as string,
      escalated: severityRank(e.to_severity) > severityRank(e.from_severity),
    }));
}

export const AGE_BUCKETS = [
  { key: "day", label: "Under 1 day", maxMs: DAY },
  { key: "week", label: "1–7 days", maxMs: 7 * DAY },
  { key: "month", label: "7–30 days", maxMs: 30 * DAY },
  { key: "older", label: "Over 30 days", maxMs: Number.POSITIVE_INFINITY },
] as const;

export type AgeBucketKey = (typeof AGE_BUCKETS)[number]["key"];
export type AgeCounts = Record<AgeBucketKey, number>;

function emptyCounts(): AgeCounts {
  return { day: 0, week: 0, month: 0, older: 0 };
}

export function ageBucket(ageMs: number): AgeBucketKey {
  return (AGE_BUCKETS.find((b) => ageMs < b.maxMs) ?? AGE_BUCKETS[AGE_BUCKETS.length - 1]).key;
}

function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

export type RecurringFinding = {
  findingId: string;
  cluster: string;
  clusterName: string;
  check: string;
  recurrences: number;
  openNow: boolean;
  lastResolution?: { by: string; at: string; note?: string };
};

export type LifecycleInsights = {
  open: number;
  buckets: AgeCounts;
  clusters: Array<{ cluster: string; name: string; total: number; oldestMs: number } & AgeCounts>;
  medianAckMs: number | null;
  ackSamples: number;
  medianResolveMs: number | null;
  resolveSamples: number;
  recurring: RecurringFinding[];
  looksFixed: NCCAlert[];
};

/**
 * Aging, response times and recurrence across every alert instance (pass the
 * list fetched with include_closed=true so recurrences and resolve times count).
 */
export function buildLifecycleInsights(alerts: NCCAlert[], now = Date.now()): LifecycleInsights {
  const buckets = emptyCounts();
  const byCluster = new Map<string, LifecycleInsights["clusters"][number]>();
  const ack: number[] = [];
  const resolve: number[] = [];
  const findings = new Map<string, NCCAlert[]>();
  const fixed: NCCAlert[] = [];
  let open = 0;

  for (const a of alerts) {
    const list = findings.get(a.finding_id) ?? [];
    list.push(a);
    findings.set(a.finding_id, list);

    const start = alertStartMs(a);
    // Imported v2.2.0 marks have no real start time, so they would skew response times.
    if (start != null && !a.legacy_migrated) {
      const acked = ms(a.acknowledged_at);
      if (acked != null && acked >= start) ack.push(acked - start);
      const resolved = ms(a.resolved_at);
      if (resolved != null && resolved >= start) resolve.push(resolved - start);
    }
    if (!isOpenAlert(a)) continue;
    open += 1;
    if (looksFixed(a)) fixed.push(a);
    const age = start != null ? Math.max(0, now - start) : 0;
    const key = ageBucket(age);
    buckets[key] += 1;
    const slot = byCluster.get(a.cluster) ?? { cluster: a.cluster, name: a.cluster_name || a.cluster, total: 0, oldestMs: 0, ...emptyCounts() };
    slot.total += 1;
    slot[key] += 1;
    slot.oldestMs = Math.max(slot.oldestMs, age);
    byCluster.set(a.cluster, slot);
  }

  const recurring: RecurringFinding[] = [];
  for (const [findingId, list] of findings) {
    const summary = summarizeFinding(list, now);
    if (summary.recurrences === 0) continue;
    const sample = summary.active ?? list[list.length - 1];
    recurring.push({
      findingId,
      cluster: sample.cluster,
      clusterName: sample.cluster_name || sample.cluster,
      check: sample.check,
      recurrences: summary.recurrences,
      openNow: summary.active ? isOpenAlert(summary.active) : false,
      lastResolution: summary.lastResolution,
    });
  }
  recurring.sort((a, b) => b.recurrences - a.recurrences || a.clusterName.localeCompare(b.clusterName) || a.check.localeCompare(b.check));

  return {
    open,
    buckets,
    clusters: [...byCluster.values()].sort((a, b) => b.older - a.older || b.month - a.month || b.total - a.total || a.name.localeCompare(b.name)),
    medianAckMs: median(ack),
    ackSamples: ack.length,
    medianResolveMs: median(resolve),
    resolveSamples: resolve.length,
    recurring,
    looksFixed: fixed.sort((a, b) => (ms(a.not_detected_since_run) ?? 0) - (ms(b.not_detected_since_run) ?? 0)),
  };
}
