import type { ClusterFreshness, ScheduleFreshness } from "../api/types";
import { ageMs, formatDateTime, relativeTime } from "./datetime";

const DAY_MS = 24 * 3600_000;

/** How old a cluster's data is, worded as on the Overview. */
export type ClusterDataAge = {
  /** "data from 3h ago", "Not reached · data from 3d ago", "Not reached · no data yet". */
  text: string;
  /** The newest run that tried the cluster could not reach it. */
  notReached: boolean;
  /** Not reached, or data older than a day: shown in amber. */
  old: boolean;
  /** Tooltip with the exact times. */
  title: string;
};

export function clusterDataAge(f: ClusterFreshness): ClusterDataAge {
  const notReached = !f.ok;
  const data = f.last_success_at ? `data from ${relativeTime(f.last_success_at)}` : "no data yet";
  const title = [
    f.last_success_at ? `Data from ${formatDateTime(f.last_success_at)}.` : "No run has returned data for it yet.",
    notReached ? `The last run to try it (${formatDateTime(f.last_checked_at)}) could not reach it.` : "",
    notReached && (f.failed_runs ?? 0) > 1 ? `The last ${f.failed_runs} runs could not check it.` : "",
  ]
    .filter(Boolean)
    .join(" ");
  return {
    text: notReached ? `Not reached · ${data}` : data,
    notReached,
    old: notReached || !f.last_success_at || ageMs(f.last_success_at) > DAY_MS,
    title,
  };
}

/**
 * Data age per cluster, keyed by lower-cased address and display name, so
 * tables can look a row up without a request or a scan per row.
 */
export function clusterDataAges(fresh: ClusterFreshness[] | undefined, nameOf?: (address: string) => string): Map<string, ClusterDataAge> {
  const out = new Map<string, ClusterDataAge>();
  for (const f of fresh ?? []) {
    const age = clusterDataAge(f);
    const address = f.address.trim().toLowerCase();
    if (address) out.set(address, age);
    const name = nameOf?.(f.address)?.trim().toLowerCase();
    if (name && name !== "-" && !out.has(name)) out.set(name, age);
  }
  return out;
}

/** The age for the first key (address or name) that has one. */
export function lookupDataAge(ages: Map<string, ClusterDataAge> | undefined, ...keys: Array<string | undefined>): ClusterDataAge | undefined {
  if (!ages || ages.size === 0) return undefined;
  for (const k of keys) {
    const age = k ? ages.get(k.trim().toLowerCase()) : undefined;
    if (age) return age;
  }
  return undefined;
}

export type Freshness = { label: "Fresh" | "Recent" | "Aging" | "Stale" | "Unknown"; color: string; tone: "ok" | "info" | "warn" | "fail" | "neutral" };

/**
 * How current a run's results are: under 6 h fresh, under a day recent,
 * under three days aging, otherwise stale. `color` is the antd Tag colour.
 */
export function freshness(iso: string): Freshness {
  if (!iso) return { label: "Unknown", color: "default", tone: "neutral" };
  const age = ageMs(iso);
  if (!Number.isFinite(age)) return { label: "Unknown", color: "default", tone: "neutral" };
  if (age <= 6 * 3600_000) return { label: "Fresh", color: "success", tone: "ok" };
  if (age <= 24 * 3600_000) return { label: "Recent", color: "processing", tone: "info" };
  if (age <= 72 * 3600_000) return { label: "Aging", color: "warning", tone: "warn" };
  return { label: "Stale", color: "error", tone: "fail" };
}

/** "42s", "5m 3s", "1h 12m". */
export function formatDuration(seconds: number | undefined | null): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return "—";
  const s = Math.round(seconds);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
}

/** A row of the Overview cluster list. */
export type OverviewClusterRow = {
  name: string;
  address: string;
  ok: boolean;
  error: string;
  fail: number;
  warn: number;
  err: number;
  score: number | null;
  /** From the run history (GET /api/v1/freshness), when available. */
  checkedAt?: string;
  lastDataAt?: string;
  failedRuns?: number;
};

/** "1d" → "day", "4h" → "4 hours", "15m" → "15 minutes", "7d" → "7 days". */
export function everyLabel(every: string): string {
  const m = /^(\d+)([mhd])$/.exec(every.trim());
  if (!m) return every;
  const n = Number(m[1]);
  const unit = { m: "minute", h: "hour", d: "day" }[m[2] as "m" | "h" | "d"];
  return n === 1 ? unit : `${n} ${unit}s`;
}

/** One sentence per overdue schedule, for the banner. */
export function overdueScheduleText(s: ScheduleFreshness): string {
  const who = s.kind === "environment" ? `The ${s.environment} schedule` : "The schedule";
  const last = s.last_run_at
    ? `the last run was ${relativeTime(s.last_run_at)}`
    : `no run has finished since it was set up ${relativeTime(s.since)}`;
  return `${who} runs every ${everyLabel(s.every)}, but ${last}.`;
}

/**
 * Adds data age to the latest run's clusters, and adds clusters the latest
 * run did not include (other environments) from their own newest run.
 */
export function mergeFreshness(rows: OverviewClusterRow[], fresh: ClusterFreshness[] | undefined, nameOf: (address: string) => string): OverviewClusterRow[] {
  if (!fresh?.length) return rows;
  const byAddress = new Map(fresh.map((f) => [f.address.toLowerCase(), f]));
  const seen = new Set<string>();
  const out = rows.map((r) => {
    const key = r.address.toLowerCase();
    const f = byAddress.get(key);
    if (!f) return r;
    seen.add(key);
    return { ...r, checkedAt: f.last_checked_at, lastDataAt: f.last_success_at, failedRuns: f.failed_runs };
  });
  for (const f of fresh) {
    if (seen.has(f.address.toLowerCase())) continue;
    out.push({
      name: nameOf(f.address) || f.address,
      address: f.address,
      ok: f.ok,
      error: f.error ?? "",
      fail: f.ok ? f.fail_count : 0,
      warn: f.ok ? f.warn_count : 0,
      err: f.ok ? f.err_count : 0,
      score: f.ok && f.health_score != null ? f.health_score : null,
      checkedAt: f.last_checked_at,
      lastDataAt: f.last_success_at,
      failedRuns: f.failed_runs,
    });
  }
  return out;
}
