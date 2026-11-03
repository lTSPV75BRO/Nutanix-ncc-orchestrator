import { useMemo } from "react";
import { Link } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Skeleton, Tooltip } from "antd";
import { ArrowRightOutlined } from "@ant-design/icons";
import { api } from "../../api/client";
import type { RunInfo } from "../../api/types";
import { localDateKey, relativeTime } from "../../utils/datetime";

type Outcome = "ok" | "problem" | "unknown";

export type DayBucket = {
  key: string;
  weekday: string;
  date: string;
  isToday: boolean;
  ok: number;
  problem: number;
  unknown: number;
  total: number;
};

export type RunActivity = {
  days: DayBucket[];
  total: number;
  ok: number;
  problem: number;
  lastRun: string | null;
};

function outcomeOf(run: RunInfo): Outcome {
  switch (run.status) {
    case "success":
      return "ok";
    case "failed":
    case "partial":
    case "cancelled":
    case "trigger_failed":
      return "problem";
  }
  if (run.success === true) return "ok";
  if (run.success === false) return "problem";
  return "unknown";
}

/**
 * Buckets runs into the last `days` local days. Only archived runs and the
 * latest summary count (trigger entries are requests, not runs); a summary and
 * its archived copy in the same minute count once.
 */
export function buildRunActivity(runs: RunInfo[], days: number, now = new Date()): RunActivity {
  const today = new Date(now.getTime());
  today.setHours(0, 0, 0, 0);
  const todayKey = localDateKey(today);
  const buckets: DayBucket[] = [];
  for (let i = days - 1; i >= 0; i -= 1) {
    const d = new Date(today.getFullYear(), today.getMonth(), today.getDate() - i);
    const key = localDateKey(d);
    buckets.push({
      key,
      weekday: d.toLocaleDateString(undefined, { weekday: "short" }),
      date: d.toLocaleDateString(undefined, { day: "numeric", month: "short" }),
      isToday: key === todayKey,
      ok: 0,
      problem: 0,
      unknown: 0,
      total: 0,
    });
  }
  const byKey = new Map(buckets.map((b) => [b.key, b]));
  const ordered = [...runs].sort((a, b) => {
    const rank: Record<string, number> = { history: 2, summary: 1 };
    return (rank[b.source ?? ""] ?? 0) - (rank[a.source ?? ""] ?? 0);
  });
  const seenMinute = new Set<string>();
  let lastRun: number | null = null;
  for (const r of ordered) {
    if (r.source !== "history" && r.source !== "summary") continue;
    const t = new Date(r.timestamp || r.mod_time).getTime();
    if (Number.isNaN(t)) continue;
    const minuteKey = new Date(t).toISOString().slice(0, 16);
    if (seenMinute.has(minuteKey)) continue;
    seenMinute.add(minuteKey);
    if (lastRun === null || t > lastRun) lastRun = t;
    const bucket = byKey.get(localDateKey(t));
    if (!bucket) continue;
    bucket[outcomeOf(r)] += 1;
    bucket.total += 1;
  }
  const sum = (k: "ok" | "problem" | "total") => buckets.reduce((acc, b) => acc + b[k], 0);
  return {
    days: buckets,
    total: sum("total"),
    ok: sum("ok"),
    problem: sum("problem"),
    lastRun: lastRun === null ? null : new Date(lastRun).toISOString(),
  };
}

function dayTooltip(b: DayBucket): string {
  if (b.total === 0) return `${b.weekday} ${b.date}: no runs`;
  const parts = [`${b.total} run${b.total === 1 ? "" : "s"}`];
  if (b.ok) parts.push(`${b.ok} succeeded`);
  if (b.problem) parts.push(`${b.problem} failed or partial`);
  if (b.unknown) parts.push(`${b.unknown} without an outcome`);
  return `${b.weekday} ${b.date}: ${parts.join(", ")}`;
}

/** Runs per day for the Status page: summary figures and a stacked bar per day. */
export function RunActivityChart({ days = 7 }: { days?: number }) {
  const runsQuery = useQuery({
    queryKey: ["runs", "activity", days],
    queryFn: () => api.runs({ limit: 200 }),
    staleTime: 30_000,
  });
  const activity = useMemo(() => buildRunActivity(runsQuery.data ?? [], days), [runsQuery.data, days]);
  const max = Math.max(1, ...activity.days.map((b) => b.total));

  if (runsQuery.isLoading) return <Skeleton active paragraph={{ rows: 3 }} />;
  if (runsQuery.isError) return <p className="run-activity-empty">Run history is not available from this server.</p>;

  return (
    <div className="run-activity">
      <dl className="run-activity-stats">
        <div>
          <dt>Runs</dt>
          <dd>{activity.total}</dd>
        </div>
        <div>
          <dt>Succeeded</dt>
          <dd className={activity.ok ? "is-ok" : undefined}>{activity.ok}</dd>
        </div>
        <div>
          <dt>Failed or partial</dt>
          <dd className={activity.problem ? "is-fail" : undefined}>{activity.problem}</dd>
        </div>
        <div>
          <dt>Last run</dt>
          <dd className="run-activity-last">
            {activity.lastRun ? (
              <Tooltip title={new Date(activity.lastRun).toLocaleString()}>
                <span>{relativeTime(activity.lastRun)}</span>
              </Tooltip>
            ) : (
              "Never"
            )}
          </dd>
        </div>
      </dl>

      <div
        className="run-activity-chart"
        role="img"
        aria-label={`Runs per day, last ${days} days: ${activity.days.map((b) => `${b.weekday} ${b.total}`).join(", ")}`}
        style={{ gridTemplateColumns: `repeat(${activity.days.length}, minmax(0, 1fr))` }}
      >
        {activity.days.map((b) => (
          <Tooltip key={b.key} title={dayTooltip(b)}>
            <div className={`run-activity-day${b.isToday ? " is-today" : ""}`}>
              <span className="run-activity-count">{b.total || ""}</span>
              <div className="run-activity-track">
                <div className="run-activity-bar" style={{ height: `${(b.total / max) * 100}%` }}>
                  {b.problem ? <span className="is-problem" style={{ flexGrow: b.problem }} /> : null}
                  {b.unknown ? <span className="is-unknown" style={{ flexGrow: b.unknown }} /> : null}
                  {b.ok ? <span className="is-ok" style={{ flexGrow: b.ok }} /> : null}
                </div>
              </div>
              <span className="run-activity-label">
                <span>{b.isToday ? "Today" : b.weekday}</span>
                <span className="run-activity-date">{b.date}</span>
              </span>
            </div>
          </Tooltip>
        ))}
      </div>

      <div className="run-activity-foot">
        <ul className="run-activity-legend">
          <li>
            <span className="is-ok" /> Succeeded
          </li>
          <li>
            <span className="is-problem" /> Failed or partial
          </li>
          <li>
            <span className="is-unknown" /> No outcome recorded
          </li>
        </ul>
        <Link to="/runs" className="run-activity-link">
          All runs <ArrowRightOutlined />
        </Link>
      </div>
    </div>
  );
}
