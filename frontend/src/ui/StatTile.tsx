import type { ReactNode } from "react";
import type { Tone } from "./StatusBadge";

/** One headline number with its label and an optional hint underneath. */
export function StatTile({
  label,
  value,
  hint,
  tone,
}: {
  label: ReactNode;
  value: ReactNode;
  hint?: ReactNode;
  tone?: Exclude<Tone, "info" | "neutral">;
}) {
  return (
    <div className={`ui-stat${tone ? ` tone-${tone}` : ""}`}>
      <span className="ui-stat-label">{label}</span>
      <span className="ui-stat-value">{value}</span>
      {hint ? <span className="ui-stat-hint">{hint}</span> : null}
    </div>
  );
}
