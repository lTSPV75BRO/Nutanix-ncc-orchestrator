// Pure helpers for the Notifications settings page (tested in
// notificationsModel.test.ts).
import type { NotificationChannelStatus, NotificationDeliveryStatus, NotificationState } from "../../api/types";
import type { Tone } from "../../ui";

export type ChannelKey = "slack" | "webhook" | "email";

export type DeliveryView = {
  tone: "ok" | "fail" | "none";
  text: string;
  at?: string;
  error?: string;
};

/** The latest delivery outcome. A failure after an older success is a failure. */
export function deliveryView(d?: NotificationDeliveryStatus): DeliveryView {
  if (!d || (!d.last_attempt_at && !d.last_success_at)) return { tone: "none", text: "Nothing sent yet" };
  const attempt = d.last_attempt_at ? Date.parse(d.last_attempt_at) : NaN;
  const success = d.last_success_at ? Date.parse(d.last_success_at) : NaN;
  const failedLast = Boolean(d.last_error) && (!Number.isFinite(success) || attempt > success);
  if (failedLast) return { tone: "fail", text: "Last send failed", at: d.last_attempt_at, error: d.last_error };
  return { tone: "ok", text: "Last sent", at: d.last_success_at || d.last_attempt_at };
}

export type ChannelStatusView = {
  tone: Tone;
  /** Badge text. */
  label: string;
  /** "Last delivered" / "Never delivered" / "Nothing sent yet", followed by `at` when set. */
  text: string;
  at?: string;
  error?: string;
  details: { label: string; value: string }[];
};

const SOURCE_LABEL: Record<string, string> = {
  runner: "Run report from the runner",
  api: "Alert, digest or test from this server",
};

/**
 * The status row for one channel, from the shared delivery status (runner and
 * server). Falls back to the server-only `last_delivery` when the status
 * endpoint gave nothing (older server, or no permission).
 */
export function channelStatusView(row?: NotificationChannelStatus, legacy?: NotificationDeliveryStatus, deadLetterConfigured = false): ChannelStatusView {
  if (!row) {
    const d = deliveryView(legacy);
    if (d.tone === "none") return { tone: "neutral", label: "Not sent yet", text: "Nothing sent yet", details: [] };
    if (d.tone === "fail") {
      return {
        tone: "fail",
        label: "Failing",
        text: legacy?.last_success_at ? "Last delivered" : "Never delivered",
        at: legacy?.last_success_at,
        error: d.error,
        details: [],
      };
    }
    return { tone: "ok", label: "Delivering", text: "Last delivered", at: d.at, details: [] };
  }
  const details: { label: string; value: string }[] = [];
  if (row.state === "failing" && row.last_attempt_at) details.push({ label: "Last attempt", value: row.last_attempt_at });
  if (row.attempts && row.state !== "never") {
    details.push({ label: "Tries", value: row.attempts === 1 ? "1 try" : `${row.attempts} tries` });
  }
  if (row.source && SOURCE_LABEL[row.source]) details.push({ label: "Last message", value: SOURCE_LABEL[row.source] });
  if (deadLetterConfigured || row.dead_letters > 0) {
    details.push({ label: "Undelivered", value: row.dead_letters === 0 ? "None" : row.dead_letters === 1 ? "1 message saved" : `${row.dead_letters} messages saved` });
  }
  if (row.state === "never") return { tone: "neutral", label: "Not sent yet", text: "Nothing sent yet", details };
  if (row.state === "failing") {
    return {
      tone: "fail",
      label: "Failing",
      text: row.last_success_at ? "Last delivered" : "Never delivered",
      at: row.last_success_at,
      error: row.last_error,
      details,
    };
  }
  return {
    tone: row.dead_letters > 0 ? "warn" : "ok",
    label: row.dead_letters > 0 ? "Delivering, some undelivered" : "Delivering",
    text: "Last delivered",
    at: row.last_success_at || row.last_attempt_at,
    details,
  };
}

const HTTP_URL = /^https?:\/\/[^\s/?#]+[^\s]*$/i;
const EMAIL = /^[^\s@,<>]+@[^\s@,<>]+\.[^\s@,<>]+$/;

/** Field-level problems for one channel, keyed by field. Only checked when the channel is on. */
export function channelErrors(state: NotificationState, channel: ChannelKey): Record<string, string> {
  const out: Record<string, string> = {};
  if (channel === "slack" && state.slack?.enabled) {
    const url = state.slack.webhook_url?.trim() ?? "";
    if (!url) out.webhook_url = "Paste the Slack incoming-webhook URL.";
    else if (!HTTP_URL.test(url)) out.webhook_url = "Use an https:// URL.";
  }
  if (channel === "webhook" && state.webhook?.enabled) {
    const url = state.webhook.url?.trim() ?? "";
    if (!url) out.url = "Enter the URL to post events to.";
    else if (!HTTP_URL.test(url)) out.url = "Use an http:// or https:// URL.";
  }
  if (channel === "email" && state.email?.enabled) {
    const e = state.email;
    if (!e.smtp_host?.trim()) out.smtp_host = "Enter the mail server.";
    if (!e.smtp_port || e.smtp_port < 1 || e.smtp_port > 65535) out.smtp_port = "Enter a port (587, 465 or 25).";
    if (!e.from?.trim()) out.from = "Enter the sender address.";
    else if (!EMAIL.test(e.from.trim())) out.from = "Not an email address.";
    const to = (e.to ?? "").split(",").map((v) => v.trim()).filter(Boolean);
    if (to.length === 0) out.to = "Enter at least one recipient.";
    else if (to.some((v) => !EMAIL.test(v))) out.to = "Separate addresses with commas.";
  }
  return out;
}

/** The first problem that blocks saving, or null. */
export function validateNotifications(state: NotificationState): string | null {
  for (const ch of ["email", "slack", "webhook"] as const) {
    const errs = Object.values(channelErrors(state, ch));
    if (errs.length) return `${ch === "email" ? "Email" : ch === "slack" ? "Slack" : "Webhook"}: ${errs[0]}`;
  }
  const hhmm = /^([01]\d|2[0-3]):([0-5]\d)$/;
  if (state.quiet?.enabled) {
    if (!hhmm.test(state.quiet.start ?? "") || !hhmm.test(state.quiet.end ?? "")) return "Quiet hours need a start and end time.";
    if (state.quiet.start === state.quiet.end) return "Quiet hours start and end at the same time.";
  }
  for (const [i, w] of (state.maintenance ?? []).entries()) {
    const start = Date.parse(w.start);
    const end = Date.parse(w.end);
    if (!Number.isFinite(start) || !Number.isFinite(end)) return `Maintenance window ${i + 1} needs a start and end.`;
    if (end <= start) return `Maintenance window ${i + 1} ends before it starts.`;
  }
  if ((state.throttle?.dedup_window_sec ?? 0) < 0 || (state.throttle?.min_interval_sec ?? 0) < 0) {
    return "Throttle values cannot be negative.";
  }
  return null;
}

/** Fills defaults the server needs so turning something on never fails to save. */
export function withDefaults(state: NotificationState): NotificationState {
  const next = { ...state };
  if (next.digest?.enabled && !next.digest.every) next.digest = { ...next.digest, every: "24h" };
  if (next.quiet?.enabled && !next.quiet.timezone) next.quiet = { ...next.quiet, timezone: "UTC" };
  if (next.email && !next.email.smtp_port) next.email = { ...next.email, smtp_port: 587 };
  return next;
}

export function enabledChannelCount(state: NotificationState): number {
  return [state.slack?.enabled, state.webhook?.enabled, state.email?.enabled].filter(Boolean).length;
}

/** "Failures and policy violations" style summary of the alert toggles. */
export function alertSummary(state: NotificationState): string {
  const parts: string[] = [];
  if (state.events?.run_failure) parts.push("failures");
  if (state.events?.policy_violations) parts.push("policy violations");
  if (state.events?.run_success) parts.push("successful runs");
  if (parts.length === 0) return "No alerts";
  const text = parts.length === 1 ? parts[0] : `${parts.slice(0, -1).join(", ")} and ${parts[parts.length - 1]}`;
  return text.charAt(0).toUpperCase() + text.slice(1);
}

export const DIGEST_EVERY_OPTIONS = [
  { value: "6h", label: "Every 6 hours" },
  { value: "12h", label: "Every 12 hours" },
  { value: "24h", label: "Daily" },
  { value: "7d", label: "Weekly" },
];

export function digestLabel(every?: string): string {
  return DIGEST_EVERY_OPTIONS.find((o) => o.value === every)?.label ?? (every ? `Every ${every}` : "Daily");
}
