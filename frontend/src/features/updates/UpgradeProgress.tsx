import { Alert, Button, Card, Collapse, Space, Steps, Tag, Timeline, Typography } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router";
import { api } from "../../api/client";
import type { UpgradeJob, UpgradePhase } from "../../api/types";
import { sectionHref } from "../../navigation";
import { formatTime } from "../../utils/datetime";

/** Query key shared with UpdatesCard, which refreshes it after "Back up and update". */
export const UPGRADE_QUERY_KEY = ["updates", "upgrade"] as const;

const ACTIVE: UpgradePhase[] = ["preflight", "snapshot", "installing", "restarting", "verifying", "rolling_back"];

export function upgradeActive(job: UpgradeJob | undefined): boolean {
  return Boolean(job && !job.stale && ACTIVE.includes(job.phase));
}

/** The supervisor's phases as the five steps shown to the user. */
const STEP_OF: Record<UpgradePhase, number> = {
  preflight: 0,
  snapshot: 0,
  installing: 1,
  restarting: 2,
  verifying: 3,
  done: 4,
  rolling_back: 2,
  rolled_back: 2,
  failed: 1,
};

/** Index of the step a failed job stopped at, from its last active phase. */
export function failedStep(job: UpgradeJob): number {
  const steps = job.steps ?? [];
  for (let i = steps.length - 1; i >= 0; i--) {
    const p = steps[i].phase;
    if (p !== "failed" && p !== "rolling_back" && p !== "rolled_back") return STEP_OF[p];
  }
  return 0;
}

const PHASE_TAG: Record<UpgradePhase, { color: string; label: string }> = {
  preflight: { color: "processing", label: "Preparing" },
  snapshot: { color: "processing", label: "Preparing" },
  installing: { color: "processing", label: "Installing" },
  restarting: { color: "processing", label: "Restarting servers" },
  verifying: { color: "processing", label: "Checking" },
  done: { color: "success", label: "Complete" },
  rolling_back: { color: "warning", label: "Rolling back" },
  rolled_back: { color: "warning", label: "Rolled back" },
  failed: { color: "error", label: "Failed" },
};

function v(s?: string) {
  return s ? `v${s.replace(/^v/, "")}` : "";
}

/**
 * Progress of the upgrade the supervisor carries out after the API has taken
 * the pre-update backup. It keeps polling through the API restart, so the
 * page follows the upgrade to the end, including an automatic rollback.
 */
export function UpgradeProgress() {
  const q = useQuery({
    queryKey: UPGRADE_QUERY_KEY,
    queryFn: () => api.updateStatus(),
    refetchInterval: (query) => (upgradeActive(query.state.data?.upgrade) || query.state.status === "error" ? 3000 : false),
    retry: false,
  });
  const job = q.data?.upgrade;
  const supervised = q.data?.supervised_upgrades;
  const active = upgradeActive(job);
  if (!job) {
    if (!supervised) return null;
    return (
      <Typography.Paragraph type="secondary" className="upgrade-note">
        <Typography.Text strong>Upgrades are checked and can roll back on their own</Typography.Text>
        {". The supervisor restarts the API, then the web UI, and checks each one. If one doesn't come back healthy, the previous version goes back in."}
      </Typography.Paragraph>
    );
  }
  const tag = PHASE_TAG[job.phase] ?? PHASE_TAG.failed;
  const current = job.phase === "failed" || job.phase === "rolled_back" ? failedStep(job) : STEP_OF[job.phase];
  const stepStatus = job.phase === "done" ? "finish" : job.phase === "failed" || job.phase === "rolled_back" ? "error" : "process";
  const from = v(job.from_version);
  const to = v(job.to_version || job.target_version);
  return (
    <Card
      size="small"
      className="page-card"
      data-upgrade-phase={job.phase}
      title={
        <Space size={8} wrap>
          <Typography.Text strong>{active ? "Upgrade in progress" : "Last upgrade"}</Typography.Text>
          <Tag color={tag.color}>{tag.label}</Tag>
          {from || to ? (
            <Typography.Text type="secondary">
              {from}
              {from && to ? " → " : ""}
              {to}
            </Typography.Text>
          ) : null}
        </Space>
      }
      extra={job.finished_at ? <Typography.Text type="secondary">{formatTime(job.finished_at)}</Typography.Text> : null}
    >
      <Space orientation="vertical" size={12} style={{ width: "100%" }}>
        <Steps
          size="small"
          current={current}
          status={stepStatus}
          items={[
            { title: "Prepare", content: "Backup and saved copy" },
            { title: "Install", content: "Checksum-verified" },
            { title: "Restart", content: "API, then web UI" },
            { title: "Check", content: "Health and versions" },
            { title: "Done" },
          ]}
        />
        {active && q.isError ? (
          <Alert type="info" showIcon title="Waiting for the API server to come back on the new version…" />
        ) : null}
        {job.stale ? (
          <Alert
            type="warning"
            showIcon
            title="The supervisor is not running"
            description="This upgrade continues when the supervisor starts again (for example when the service restarts)."
          />
        ) : null}
        {job.phase === "rolled_back" ? (
          <Alert type="warning" showIcon title="The update was rolled back; the previous version is running" description={job.error} />
        ) : null}
        {job.phase === "failed" ? (
          <Alert
            type="error"
            showIcon
            title="The update did not complete"
            description={
              <>
                {job.message}
                {job.backup_path ? (
                  <div style={{ marginTop: 4 }}>
                    The pre-update backup is on <Link to={sectionHref("updates", { tab: "backups" })}>Backups</Link>.
                  </div>
                ) : null}
              </>
            }
          />
        ) : null}
        {job.phase === "done" ? (
          <Alert
            type="success"
            showIcon
            title={job.message}
            description={
              <>
                {job.doctor?.fixed?.length ? <div>Fixed after the upgrade: {job.doctor.fixed.join(", ")}.</div> : null}
                {job.doctor?.failing?.length ? (
                  <div>
                    Still failing: {job.doctor.failing.join(", ")} (see{" "}
                    <Link to={sectionHref("health", { tab: "checks" })}>Health &amp; repair</Link>).
                  </div>
                ) : null}
                {job.supervisor_restart_pending ? (
                  <div>The supervisor itself switches to the new version at the next service restart.</div>
                ) : null}
              </>
            }
            action={
              <Button size="small" icon={<ReloadOutlined />} onClick={() => window.location.reload()}>
                Reload page
              </Button>
            }
          />
        ) : null}
        {active && !q.isError ? <Typography.Text type="secondary">{job.message}</Typography.Text> : null}
        <Collapse
          size="small"
          ghost
          items={[
            {
              key: "steps",
              label: `Details (${job.steps?.length ?? 0} steps${job.resumed ? ", resumed after a supervisor restart" : ""})`,
              children: (
                <Timeline
                  items={(job.steps ?? []).map((s) => ({
                    color: s.phase === "failed" ? "red" : s.phase === "rolling_back" || s.phase === "rolled_back" ? "orange" : s.phase === "done" ? "green" : "blue",
                    content: (
                      <>
                        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                          {formatTime(s.at)}
                        </Typography.Text>
                        <div>{s.message}</div>
                      </>
                    ),
                  }))}
                />
              ),
            },
          ]}
        />
      </Space>
    </Card>
  );
}
