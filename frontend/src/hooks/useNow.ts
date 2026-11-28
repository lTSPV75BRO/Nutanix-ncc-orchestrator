import { useEffect, useState } from "react";

/** Current time (epoch ms), re-read every intervalMs while enabled. */
export function useNow(intervalMs = 1000, enabled = true): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!enabled) return;
    setNow(Date.now());
    const id = window.setInterval(() => setNow(Date.now()), intervalMs);
    return () => window.clearInterval(id);
  }, [intervalMs, enabled]);
  return now;
}

/**
 * Whole seconds left until `until` (epoch ms), ticking once a second; 0 once
 * it has passed or when there is no deadline.
 */
export function useSecondsUntil(until: number | null): number {
  useNow(1000, until !== null); // re-render once a second
  if (until === null) return 0;
  // Read the clock directly: the ticker's value can be a tick behind when the
  // deadline has just been set.
  return Math.max(0, Math.ceil((until - Date.now()) / 1000));
}
