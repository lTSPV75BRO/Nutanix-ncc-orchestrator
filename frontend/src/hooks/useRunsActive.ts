import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import type { RunActiveData } from "../api/types";

/**
 * Single source of truth for /api/v1/runs/active polling.
 *
 * Before v2.2.1 five independent pollers hit this endpoint: the app shell
 * (every 5 s, unconditionally), the header Run button (3 s / 15 s), the
 * dashboard (3 s / 30 s), System Health (10 s), and the Runs page, which ran
 * its own setInterval every 3 s outside React Query (so it was never deduped
 * and kept polling in hidden tabs). React Query runs one timer per observer,
 * so with several pages mounted the endpoint was requested roughly every
 * 1–2 s while idle.
 *
 * Now exactly one observer (useRunsActivePoller, mounted once in the app
 * shell) owns the interval; every other consumer (useRunsActive) only reads
 * the shared cache and never schedules its own timer.
 */
export const RUNS_ACTIVE_QUERY_KEY = ["runs-active"] as const;

function envMs(name: string, fallback: number): number {
  // import.meta.env is always defined under Vite/Vitest; guard for other runners.
  const env = (import.meta.env ?? {}) as Record<string, string | undefined>;
  const raw = env[name];
  const n = raw ? Number(raw) : NaN;
  return Number.isFinite(n) && n >= 1000 ? n : fallback;
}

export const RUNS_ACTIVE_POLL = {
  /** Interval while a run is queued or running (VITE_RUNS_ACTIVE_POLL_ACTIVE_MS). */
  activeMs: envMs("VITE_RUNS_ACTIVE_POLL_ACTIVE_MS", 3_000),
  /** Interval while idle (VITE_RUNS_ACTIVE_POLL_IDLE_MS). */
  idleMs: envMs("VITE_RUNS_ACTIVE_POLL_IDLE_MS", 30_000),
  /** Upper bound for the error backoff. */
  maxBackoffMs: 5 * 60_000,
};

/**
 * Pure polling policy (unit-tested): fast while work is in flight, slow when
 * idle, exponential backoff on consecutive errors, and `false` (stop) when
 * the page is hidden.
 */
export function runsActivePollInterval(
  data: RunActiveData | undefined,
  opts: { hidden?: boolean; consecutiveErrors?: number } = {},
  cfg = RUNS_ACTIVE_POLL,
): number | false {
  if (opts.hidden) return false;
  const busy = Boolean(data?.active) || (data?.running_count ?? 0) > 0 || (data?.queued_count ?? 0) > 0;
  const base = busy ? cfg.activeMs : cfg.idleMs;
  const errors = Math.max(0, opts.consecutiveErrors ?? 0);
  if (errors === 0) return base;
  return Math.min(base * 2 ** Math.min(errors, 10), cfg.maxBackoffMs);
}

function pageHidden(): boolean {
  return typeof document !== "undefined" && document.visibilityState === "hidden";
}

/** The one polling observer. Mount exactly once (app shell). */
export function useRunsActivePoller(enabled: boolean) {
  return useQuery({
    queryKey: RUNS_ACTIVE_QUERY_KEY,
    queryFn: ({ signal }) => api.runActive(signal),
    enabled,
    staleTime: 1_500,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: true, // catch up immediately when the tab becomes visible again
    refetchInterval: (q) =>
      runsActivePollInterval(q.state.data as RunActiveData | undefined, {
        hidden: pageHidden(),
        consecutiveErrors: q.state.fetchFailureCount,
      }),
  });
}

/** Read-only consumer: shares the poller's cache and never adds a timer. */
export function useRunsActive(enabled = true) {
  return useQuery({
    queryKey: RUNS_ACTIVE_QUERY_KEY,
    queryFn: ({ signal }) => api.runActive(signal),
    enabled,
    staleTime: 2_000,
    refetchInterval: false,
    refetchOnWindowFocus: false,
  });
}
