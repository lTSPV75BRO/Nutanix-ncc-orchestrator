import type { RunInfo } from "../api/types";
import type { Tone } from "../ui/StatusBadge";

/** runStatusDisplay as a StatusBadge tone, for the redesigned pages. */
export function runStatusTone(row: Pick<RunInfo, "source" | "status" | "success" | "exit_code" | "status_source">): {
  tone: Tone;
  label: string;
  hint?: string;
} {
  const d = runStatusDisplay(row);
  const tone: Tone =
    d.color === "success" ? "ok" : d.color === "error" ? "fail" : d.color === "warning" ? "warn" : d.color === "processing" ? "info" : "neutral";
  return { tone, label: d.label, hint: d.hint };
}

export type RunStatusDisplay = {
  label: string;
  color: "success" | "error" | "warning" | "processing" | "default";
  /** Explains how the status was determined (shown as a tooltip). */
  hint?: string;
};

/**
 * Maps a run row onto an explicit, human-readable status.
 *
 * Before v2.2.1 the runner omitted `exit_code` when it was 0, so successful
 * runs had no `success` field and the Runs table rendered "—". The API now
 * returns an explicit `status`; this function prefers it, falls back to
 * `success`/`exit_code` for older API servers, and only shows "Unknown" when
 * the outcome genuinely cannot be determined (missing data is never shown as
 * success).
 */
export function runStatusDisplay(row: Pick<RunInfo, "source" | "status" | "success" | "exit_code" | "status_source">): RunStatusDisplay {
  const exit = typeof row.exit_code === "number" && row.exit_code !== 0 ? ` · exit ${row.exit_code}` : "";
  const legacyHint =
    row.status_source === "legacy_summary"
      ? "Recorded by an older runner that omitted exit code 0; the run completed normally."
      : undefined;
  switch (row.status) {
    case "success":
      return { label: "Success", color: "success", hint: legacyHint };
    case "partial":
      return { label: `Partial${exit}`, color: "warning", hint: "Some clusters could not be checked." };
    case "failed":
      return { label: `Failed${exit}`, color: "error" };
    case "running":
      return { label: "Running", color: "processing" };
    case "queued":
    case "pending":
      return { label: "Pending", color: "default" };
    case "cancelled":
      return { label: "Cancelled", color: "default" };
    case "triggered":
      return { label: "Triggered", color: "processing" };
    case "trigger_failed":
      return { label: "Trigger failed", color: "error" };
    default:
      break;
  }
  // Older API servers: derive from the trigger/success fields.
  if (row.source === "trigger") {
    return row.success === false
      ? { label: "Trigger failed", color: "error" }
      : { label: "Triggered", color: "processing" };
  }
  if (row.success === true) return { label: "Success", color: "success" };
  if (row.success === false) return { label: `Failed${exit}`, color: "error" };
  return {
    label: "Unknown",
    color: "default",
    hint: "The run did not record an outcome (for example, it was interrupted before writing its summary).",
  };
}
