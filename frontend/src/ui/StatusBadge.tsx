import type { ReactNode } from "react";

export type Tone = "ok" | "warn" | "fail" | "info" | "neutral";

/**
 * Map an NCC severity or status word to a tone, so every page colours
 * PASS/WARN/FAIL/ERR/INFO the same way.
 */
export function toneFor(value: string | undefined | null): Tone {
  const v = (value || "").trim().toUpperCase();
  if (["PASS", "OK", "SUCCESS", "SUCCEEDED", "COMPLETED", "HEALTHY", "DONE"].includes(v)) return "ok";
  if (["WARN", "WARNING", "DEGRADED", "PARTIAL", "RUNNING", "QUEUED"].includes(v)) return "warn";
  if (["FAIL", "FAILED", "ERR", "ERROR", "CRITICAL", "UNHEALTHY"].includes(v)) return "fail";
  if (["INFO"].includes(v)) return "info";
  return "neutral";
}

/** Small coloured status label with a dot. Pass `tone`, or let `toneFor(children)` pick it. */
export function StatusBadge({ tone, children }: { tone?: Tone; children: ReactNode }) {
  const t = tone ?? (typeof children === "string" ? toneFor(children) : "neutral");
  return (
    <span className={`ui-badge tone-${t}`}>
      <span className="ui-badge-dot" aria-hidden="true" />
      {children}
    </span>
  );
}
