import type { ExtraArgOption } from "../api/types";

/** Form state for run options: comma-joined strings, or booleans for switches. */
export type RunOptionValues = Record<string, string | boolean | undefined>;

/** Splits the "Additional flags" free text into argv tokens. */
export function parseFreeTextArgs(raw: string): string[] {
  return raw.trim() ? raw.trim().split(/\s+/) : [];
}

/**
 * Builds extra_args for POST /runs/trigger from the structured run options
 * plus any free-text flags. Options the free text already sets are taken from
 * the free text (it is the more specific, advanced input), so a flag is never
 * sent twice. Admin-only options are dropped for restricted callers, who the
 * server would reject anyway.
 */
export function buildExtraArgs(
  options: ExtraArgOption[],
  values: RunOptionValues,
  freeText: string,
  opts: { restricted: boolean },
): string[] {
  const free = parseFreeTextArgs(freeText);
  const freeFlags = new Set(free.filter((t) => t.startsWith("--")));
  const out: string[] = [];
  for (const o of options) {
    if (freeFlags.has(o.flag)) continue;
    if (o.admin_only && opts.restricted) continue;
    const v = values[o.flag];
    if (!o.takes_value) {
      if (v === true) out.push(o.flag);
      continue;
    }
    const s = typeof v === "string" ? v.trim() : "";
    if (s) out.push(o.flag, s);
  }
  return [...out, ...free];
}

/** Human-readable preview of what will be sent, e.g. "--outputs csv,json --dry-run". */
export function describeExtraArgs(args: string[]): string {
  return args.length ? args.join(" ") : "none (configuration defaults)";
}
