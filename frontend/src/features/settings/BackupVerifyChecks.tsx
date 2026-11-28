import { Typography } from "antd";
import { StatusBadge, type Tone } from "../../ui";
import type { BackupVerifyReport } from "../../api/types";

const TONE: Record<string, Tone> = { pass: "ok", warn: "warn", fail: "fail" };
const LABEL: Record<string, string> = { pass: "Pass", warn: "Warn", fail: "Fail" };

/**
 * The check list from `v2-restore --verify-only`: one row per check with a
 * StatusBadge, plus a one-line summary. Secrets are listed by name only.
 */
export function BackupVerifyChecks({ report }: { report: BackupVerifyReport }) {
  const fails = report.checks.filter((c) => c.status === "fail").length;
  const warns = report.checks.filter((c) => c.status === "warn").length;
  let summary = "Every check passed. Nothing was restored.";
  if (fails > 0) {
    summary = `${fails} ${fails === 1 ? "check" : "checks"} failed. Don't restore this backup.`;
  } else if (warns > 0) {
    summary = `Passed with ${warns} ${warns === 1 ? "warning" : "warnings"}. Nothing was restored.`;
  }
  return (
    <div className="backup-verify">
      <Typography.Paragraph type="secondary" style={{ marginBottom: 8 }}>
        {report.backup_version
          ? `Made by version ${report.backup_version}${report.created_at ? ` at ${report.created_at}` : ""}. This server runs ${report.running_version}.`
          : `This server runs ${report.running_version}.`}
      </Typography.Paragraph>
      <ul className="backup-verify-list" aria-label="Backup checks">
        {report.checks.map((c) => (
          <li key={c.name} className="backup-verify-row">
            <span className="backup-verify-badge">
              <StatusBadge tone={TONE[c.status] ?? "neutral"}>{LABEL[c.status] ?? c.status}</StatusBadge>
            </span>
            <span className="backup-verify-name">{c.name}</span>
            <span className="backup-verify-msg">{c.message}</span>
          </li>
        ))}
      </ul>
      <Typography.Paragraph strong style={{ marginTop: 12, marginBottom: 0 }}>
        {summary}
      </Typography.Paragraph>
    </div>
  );
}

/** Pull a verify report out of a successful response or a failed call's error data. */
export function backupVerifyReportFrom(data: unknown): BackupVerifyReport | null {
  if (!data || typeof data !== "object") return null;
  const report = (data as { report?: unknown }).report;
  if (!report || typeof report !== "object") return null;
  const checks = (report as { checks?: unknown }).checks;
  return Array.isArray(checks) ? (report as BackupVerifyReport) : null;
}
