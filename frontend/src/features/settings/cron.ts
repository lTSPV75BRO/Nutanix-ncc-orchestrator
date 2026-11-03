// Small five-field cron helper for the Schedule page: validation, a plain
// English description, and the next run times for a preview. It covers what
// the server accepts (numbers, *, ranges, steps, lists and @-macros); names
// such as MON or JAN are rejected there too.

const MACROS: Record<string, string> = {
  "@yearly": "0 0 1 1 *",
  "@annually": "0 0 1 1 *",
  "@monthly": "0 0 1 * *",
  "@weekly": "0 0 * * 0",
  "@daily": "0 0 * * *",
  "@midnight": "0 0 * * *",
  "@hourly": "0 * * * *",
};

const FIELDS = [
  { name: "minute", min: 0, max: 59 },
  { name: "hour", min: 0, max: 23 },
  { name: "day of month", min: 1, max: 31 },
  { name: "month", min: 1, max: 12 },
  { name: "day of week", min: 0, max: 7 },
] as const;

const DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

export type ParsedCron = {
  fields: string[];
  sets: Set<number>[];
  /** Day-of-month / day-of-week restricted (cron ORs them when both are). */
  domRestricted: boolean;
  dowRestricted: boolean;
};

function parseField(src: string, min: number, max: number, name: string): Set<number> | string {
  const out = new Set<number>();
  for (const part of src.split(",")) {
    const m = part.match(/^(\*|\d+(?:-\d+)?)(?:\/(\d+))?$/);
    if (!m) return `invalid ${name} "${part}"`;
    let lo = min;
    let hi = max;
    if (m[1] !== "*") {
      const [a, b] = m[1].split("-").map(Number);
      lo = a;
      hi = b ?? (m[2] ? max : a);
    }
    const step = m[2] ? Number(m[2]) : 1;
    if (lo < min || hi > max || lo > hi) return `${name} must be between ${min} and ${max}`;
    if (step < 1) return `${name} step must be at least 1`;
    for (let v = lo; v <= hi; v += step) out.add(v);
  }
  return out;
}

/** Parses a cron expression; returns an error message string when invalid. */
export function parseCron(expr: string): ParsedCron | string {
  const raw = expr.trim();
  const text = MACROS[raw] ?? raw;
  const fields = text.split(/\s+/).filter(Boolean);
  if (fields.length !== 5) return "Use five fields: minute hour day-of-month month day-of-week (e.g. 15 */4 * * *).";
  const sets: Set<number>[] = [];
  for (let i = 0; i < 5; i++) {
    const r = parseField(fields[i], FIELDS[i].min, FIELDS[i].max, FIELDS[i].name);
    if (typeof r === "string") return r.charAt(0).toUpperCase() + r.slice(1) + ".";
    sets.push(r);
  }
  if (sets[4].has(7)) sets[4].add(0);
  return { fields, sets, domRestricted: fields[2] !== "*", dowRestricted: fields[4] !== "*" };
}

/** The next `count` run times after `from`, in local time or UTC. */
export function nextCronRuns(expr: string, from: Date, count: number, utc = false): Date[] {
  const p = parseCron(expr);
  if (typeof p === "string") return [];
  const [mins, hours, doms, months, dows] = p.sets;
  const sortedHours = [...hours].sort((a, b) => a - b);
  const sortedMins = [...mins].sort((a, b) => a - b);
  const out: Date[] = [];
  const day = new Date(from.getTime());
  if (utc) day.setUTCHours(0, 0, 0, 0);
  else day.setHours(0, 0, 0, 0);
  // Two years covers every expression that can fire at all (Feb 29 aside).
  for (let i = 0; i < 800 && out.length < count; i++) {
    const month = (utc ? day.getUTCMonth() : day.getMonth()) + 1;
    const dom = utc ? day.getUTCDate() : day.getDate();
    const dow = utc ? day.getUTCDay() : day.getDay();
    const domOk = doms.has(dom);
    const dowOk = dows.has(dow);
    const dayOk = p.domRestricted && p.dowRestricted ? domOk || dowOk : domOk && dowOk;
    if (months.has(month) && dayOk) {
      for (const h of sortedHours) {
        for (const m of sortedMins) {
          const t = new Date(day.getTime());
          if (utc) t.setUTCHours(h, m, 0, 0);
          else t.setHours(h, m, 0, 0);
          if (t.getTime() > from.getTime()) {
            out.push(t);
            if (out.length === count) return out;
          }
        }
      }
    }
    if (utc) day.setUTCDate(day.getUTCDate() + 1);
    else day.setDate(day.getDate() + 1);
  }
  return out;
}

const pad = (n: number) => String(n).padStart(2, "0");

function listDays(field: string): string | null {
  if (field === "1-5") return "weekdays";
  if (field === "0,6" || field === "6,0") return "weekends";
  const parts = field.split(",");
  if (parts.every((d) => /^[0-7]$/.test(d))) return parts.map((d) => DAY_NAMES[Number(d) % 7]).join(", ");
  return null;
}

/** Plain-English summary of common cron shapes; a generic label otherwise. */
export function describeCron(expr: string): string {
  const raw = expr.trim();
  const p = parseCron(raw);
  if (typeof p === "string") return "Invalid cron expression";
  const [mi, ho, dom, mon, dow] = p.fields;
  const num = (f: string) => /^\d+$/.test(f);
  const step = (f: string) => f.match(/^\*\/(\d+)$/)?.[1];
  if (raw === "@hourly" || (mi === "0" && ho === "*" && dom === "*" && mon === "*" && dow === "*")) return "Every hour, on the hour";
  if (step(mi) && ho === "*" && dom === "*" && mon === "*" && dow === "*") return `Every ${step(mi)} minutes`;
  if (mi === "*" && ho === "*" && dom === "*" && mon === "*" && dow === "*") return "Every minute";
  if (num(mi) && ho === "*" && dom === "*" && mon === "*" && dow === "*") return `Every hour at :${pad(Number(mi))}`;
  if (num(mi) && step(ho) && dom === "*" && mon === "*" && dow === "*") {
    return `Every ${step(ho)} hours${mi === "0" ? "" : ` at :${pad(Number(mi))} past`}`;
  }
  if (num(mi) && num(ho) && mon === "*") {
    const at = `at ${pad(Number(ho))}:${pad(Number(mi))}`;
    if (dom === "*" && dow === "*") return `Every day ${at}`;
    if (dom === "*") {
      const days = listDays(dow);
      if (days) return `Every ${days === "weekdays" || days === "weekends" ? days.slice(0, -1) : days} ${at}`.replace("Every weekday", "Weekdays").replace("Every weekend", "Weekends");
    }
    if (num(dom) && dow === "*") return `Monthly on day ${dom} ${at}`;
  }
  return "Custom schedule";
}

/**
 * The CronJob schedule the server derives from an interval on Kubernetes
 * (mirrors kubernetesCron in cmd/ncc-api-server/k8sschedule.go), or the
 * reason it refuses one.
 */
export function kubernetesCronFor(every: string): { cron: string } | { error: string } {
  const m = every.trim().match(/^([1-9]\d*)([mhd])$/);
  if (!m) return { error: "Use an interval like 15m, 4h or 1d." };
  const n = Number(m[1]);
  if (m[2] === "m") {
    if (n === 60) return { cron: "0 * * * *" };
    if (n > 59 || 60 % n !== 0) return { error: "Minute intervals must divide an hour evenly (1–6, 10, 12, 15, 20 or 30). Use a custom cron for other times." };
    return { cron: `*/${n} * * * *` };
  }
  if (m[2] === "h") {
    if (n === 24) return { cron: "0 0 * * *" };
    if (n > 23 || 24 % n !== 0) return { error: "Hour intervals must divide a day evenly (1, 2, 3, 4, 6, 8, 12 or 24). Use a custom cron for other times." };
    return { cron: n === 1 ? "0 * * * *" : `0 */${n} * * *` };
  }
  if (n === 1) return { cron: "0 0 * * *" };
  if (n === 7) return { cron: "0 0 * * 0" };
  return { error: "Day intervals must be 1 or 7 days on Kubernetes. Use a custom cron for other times." };
}

/** "Every 4 hours" for 4h. */
export function describeEvery(every: string): string {
  const m = every.trim().match(/^(\d+)([mhd])$/);
  if (!m) return "—";
  const n = Number(m[1]);
  const unit = m[2] === "m" ? "minute" : m[2] === "h" ? "hour" : "day";
  if (n === 1) return unit === "day" ? "Every day" : `Every ${unit}`;
  return `Every ${n} ${unit}s`;
}
