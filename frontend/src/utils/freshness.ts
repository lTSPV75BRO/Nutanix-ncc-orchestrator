import { ageMs } from "./datetime";

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
