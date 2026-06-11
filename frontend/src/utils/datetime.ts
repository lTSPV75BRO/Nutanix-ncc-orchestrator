// Centralized date/time handling for the UI.
//
// Contract with the backend: the API/orchestrator emit every timestamp in
// **UTC** (RFC3339, normally `Z`-suffixed). The UI's job is to render those
// instants in the **browser's local timezone**. All display formatting in the
// app should go through these helpers so the policy is enforced in one place.
//
// Robustness: if a backend value ever arrives without an explicit timezone
// designator (e.g. "2026-06-11T09:21:00" or "2026-06-11 09:21:00"), JS's
// `new Date()` would interpret it as *local* time — which is wrong, because the
// backend speaks UTC. `parseInstant` therefore appends a `Z` to timezone-less
// ISO-ish strings so they are always parsed as UTC before being rendered local.

/**
 * Parse a backend timestamp into a Date, treating timezone-less values as UTC.
 * Returns `null` for empty/unparseable input.
 */
export function parseInstant(value: string | number | Date | null | undefined): Date | null {
  if (value === null || value === undefined || value === "") return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;
  if (typeof value === "number") {
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  let s = value.trim();
  if (!s) return null;
  // Detect an explicit timezone: trailing Z, or a ±HH:MM / ±HHMM offset on the
  // time portion. If absent and the string looks like an ISO date-time, treat
  // it as UTC by appending Z. A space separator (SQL-style) is normalized to T.
  const hasTz = /[zZ]$/.test(s) || /[+-]\d{2}:?\d{2}$/.test(s);
  const looksDateTime = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}/.test(s);
  if (looksDateTime && !hasTz) {
    s = s.replace(" ", "T") + "Z";
  }
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

/** Full local date + time, e.g. "Jun 11, 2026, 3:48 PM". */
export function formatDateTime(value: string | number | Date | null | undefined): string {
  const d = parseInstant(value);
  if (!d) return typeof value === "string" && value ? value : "—";
  return d.toLocaleString();
}

/** Local date only, e.g. "Jun 11, 2026". */
export function formatDate(value: string | number | Date | null | undefined): string {
  const d = parseInstant(value);
  if (!d) return typeof value === "string" && value ? value : "—";
  return d.toLocaleDateString();
}

/** Local time only, e.g. "3:48 PM". */
export function formatTime(value: string | number | Date | null | undefined): string {
  const d = parseInstant(value);
  if (!d) return typeof value === "string" && value ? value : "—";
  return d.toLocaleTimeString();
}

/** Local YYYY-MM-DD (in the browser timezone, never UTC). */
export function localDateKey(value: string | number | Date | null | undefined): string {
  const d = parseInstant(value);
  if (!d) return "";
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

/**
 * Compact relative time vs. now, e.g. "5m ago" / "in 2h". Falls back to "—" for
 * empty input and echoes the raw value if it can't be parsed.
 */
export function relativeTime(value: string | number | Date | null | undefined): string {
  const d = parseInstant(value);
  if (!d) return typeof value === "string" && value ? value : "—";
  const diff = Date.now() - d.getTime();
  const abs = Math.abs(diff);
  const s = Math.floor(abs / 1000);
  if (s < 60) return diff >= 0 ? `${s}s ago` : `in ${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return diff >= 0 ? `${m}m ago` : `in ${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return diff >= 0 ? `${h}h ago` : `in ${h}h`;
  const day = Math.floor(h / 24);
  return diff >= 0 ? `${day}d ago` : `in ${day}d`;
}

/** Milliseconds since `value` (UTC-aware). NaN-safe: returns Infinity if unparseable. */
export function ageMs(value: string | number | Date | null | undefined): number {
  const d = parseInstant(value);
  if (!d) return Number.POSITIVE_INFINITY;
  return Date.now() - d.getTime();
}
