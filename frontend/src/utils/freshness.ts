import type { ClusterFreshness, ScheduleFreshness } from "../api/types";
import { ageMs, relativeTime } from "./datetime";

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
