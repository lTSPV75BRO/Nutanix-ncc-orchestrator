import { Alert, Button, Card, Collapse, Modal, Space, Steps, Tag, Timeline, Typography } from "antd";
import { ReloadOutlined, RollbackOutlined } from "@ant-design/icons";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router";
import { api } from "../../api/client";
import type { RollbackOffer, UpgradeJob, UpgradePhase } from "../../api/types";
import { sectionHref } from "../../navigation";
import { notify, notifyError } from "../../notify";
import { formatDateTime, formatTime } from "../../utils/datetime";

/** Query key shared with UpdatesCard, which refreshes it after "Back up and update". */
export const UPGRADE_QUERY_KEY = ["updates", "upgrade"] as const;

const FINISHED: UpgradePhase[] = ["done", "rolled_back", "failed"];

export function upgradeActive(job: UpgradeJob | undefined): boolean {
  return Boolean(job && !job.stale && !FINISHED.includes(job.phase));
}

/** The helper (and the preparation for it) carries this job out, not the supervisor of an older release. */
function helperJob(job: UpgradeJob): boolean {
  return job.runner === "helper" || job.runner === "stage";
}

/**
 * Whether the previous version can still be put back by hand: after an upgrade,
 * an adoption (service definitions) or `update` (programs), for 24 hours. The
 * server's offer decides; without one (older servers) the job does.
 */
export function canRollBack(job: UpgradeJob | undefined, offer?: RollbackOffer, now = Date.now()): boolean {
  if (upgradeActive(job)) return false;
  if (offer) return Date.parse(offer.until) > now;
  return Boolean(
    job?.phase === "done" &&
      job.runner === "helper" &&
      (job.action === "upgrade" || job.action === "adopt") &&
      job.rollback_until &&
      Date.parse(job.rollback_until) > now,
  );
}

/** Phases from which on the stack was changed. */
const CHANGING: UpgradePhase[] = ["stopping", "swapping", "migrating", "starting", "checking", "restoring", "restarting", "verifying", "rolling_back"];

/** A failed job that ended before anything of the stack was changed. */
export function nothingChanged(job: UpgradeJob): boolean {
  return job.phase === "failed" && !(job.steps ?? []).some((s) => CHANGING.includes(s.phase));
}

/** The phases as the five steps shown to the user. */
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
  downloading: 0,
  helper_install: 1,
  helper_check: 1,
  handover: 1,
  staging: 2,
  saving: 2,
  stopping: 2,
  swapping: 2,
  migrating: 2,
  starting: 3,
  checking: 3,
  restoring: 2,
};

const SUPERVISOR_STEPS = [
  { title: "Prepare", content: "Backup and saved copy" },
  { title: "Install", content: "Checksum-verified" },
  { title: "Restart", content: "API, then web UI" },
  { title: "Check", content: "Health and versions" },
  { title: "Done" },
];

const HELPER_STEPS = [
  { title: "Download", content: "Backed up and verified" },
  { title: "Update helper", content: "Installed and tested first" },
  { title: "Install", content: "Programs and service" },
  { title: "Check", content: "Supervisor, API and web UI" },
  { title: "Done" },
];

/** Index of the step a failed job stopped at, from its last active phase. */
export function failedStep(job: UpgradeJob): number {
  const steps = job.steps ?? [];
  for (let i = steps.length - 1; i >= 0; i--) {
    const p = steps[i].phase;
    if (p !== "failed" && p !== "rolling_back" && p !== "rolled_back" && p !== "restoring") return STEP_OF[p] ?? 0;
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
  downloading: { color: "processing", label: "Downloading" },
  helper_install: { color: "processing", label: "Updating the helper" },
  helper_check: { color: "processing", label: "Testing the helper" },
  handover: { color: "processing", label: "Starting the helper" },
  staging: { color: "processing", label: "Preparing" },
  saving: { color: "processing", label: "Saving the current version" },
  stopping: { color: "processing", label: "Stopping the service" },
  swapping: { color: "processing", label: "Installing" },
  migrating: { color: "processing", label: "Updating the service" },
  starting: { color: "processing", label: "Starting" },
  checking: { color: "processing", label: "Checking" },
  restoring: { color: "warning", label: "Rolling back" },
};

const TITLES: Record<string, [string, string]> = {
  adopt: ["Service update in progress", "Last service update"],
  rollback: ["Rollback in progress", "Last rollback"],
};

function v(s?: string) {
  return s ? `v${s.replace(/^v/, "")}` : "";
}

/**
 * Progress of the upgrade that goes on after the API has taken the pre-update
 * backup. It keeps polling through the API restart, so the page follows the
 * upgrade to the end, including an automatic rollback.
 */
export function UpgradeProgress() {
  const qc = useQueryClient();
  const q = useQuery({
    queryKey: UPGRADE_QUERY_KEY,
    queryFn: () => api.updateStatus(),
    refetchInterval: (query) => (upgradeActive(query.state.data?.upgrade) || query.state.status === "error" ? 3000 : false),
    retry: false,
  });
  const job = q.data?.upgrade;
  const offer = q.data?.rollback;
  const supervised = q.data?.supervised_upgrades;
  const active = upgradeActive(job);

  const confirmRollback = () => {
    const definitionsOnly = offer?.of === "adopt" || (!offer && job?.action === "adopt");
    const back = v(offer?.to) || (job?.action === "adopt" ? "" : v(job?.from_version));
    Modal.confirm({
      title: definitionsOnly ? "Put the service definitions back?" : `Roll back to ${back || "the previous version"}?`,
      icon: <RollbackOutlined style={{ color: "var(--ui-accent)" }} />,
      content: definitionsOnly
        ? "The service definitions from before this version's first start are put back, and the services restart. The programs stay as they are."
        : "The programs, the web UI, the service definitions and the configuration layout from before the upgrade are put back, and the services restart. Runs and settings made since the upgrade are kept.",
      okText: "Roll back",
      cancelText: "Cancel",
      onOk: async () => {
        try {
          const res = await api.rollbackUpdate();
          notify.info(res.message ?? "Rollback started.");
          void qc.invalidateQueries({ queryKey: UPGRADE_QUERY_KEY });
        } catch (e) {
          notifyError(e, "Couldn't start the rollback");
        }
      },
    });
  };
  const rollbackButton = (
    <Button size="small" icon={<RollbackOutlined />} onClick={confirmRollback}>
      Roll back
    </Button>
  );
  const offerText = offer
    ? offer.of === "adopt"
      ? `The service definitions from before v${offer.from} can be put back until ${formatDateTime(offer.until)}`
      : `${v(offer.to)} can be put back until ${formatDateTime(offer.until)}`
    : "";

  if (!job) {
    if (offer && canRollBack(undefined, offer)) {
      return <Alert type="info" showIcon title={offerText} action={rollbackButton} />;
    }
    if (!supervised) return null;
    return (
      <Typography.Paragraph type="secondary" className="upgrade-note">
        <Typography.Text strong>Upgrades are checked and can roll back on their own</Typography.Text>
        {". The new version's update helper goes in first and is tested. It then replaces the programs and the service definitions and checks the supervisor, the API and the web UI. If one doesn't come back healthy, the previous version goes back in."}
      </Typography.Paragraph>
    );
  }
  const tag = PHASE_TAG[job.phase] ?? PHASE_TAG.failed;
  const rollback = job.action === "rollback";
  const rolledBackByHand = rollback && job.phase === "rolled_back";
  const stoppedEarly = job.phase === "failed" || (job.phase === "rolled_back" && !rollback);
  const current = stoppedEarly ? failedStep(job) : rolledBackByHand ? 4 : (STEP_OF[job.phase] ?? 0);
  const stepStatus = job.phase === "done" || rolledBackByHand ? "finish" : stoppedEarly ? "error" : "process";
  const from = job.action === "adopt" ? "" : v(rollback ? undefined : job.from_version);
  const to = v(rollback ? job.from_version : job.to_version || job.target_version);
  const [activeTitle, lastTitle] = TITLES[job.action ?? ""] ?? ["Upgrade in progress", "Last upgrade"];
  const backupLink = job.backup_path ? (
    <div style={{ marginTop: 4 }}>
      The pre-update backup is on <Link to={sectionHref("updates", { tab: "backups" })}>Backups</Link>.
    </div>
  ) : null;

  const rollBack = canRollBack(job, offer);
  const unchanged = nothingChanged(job);

  return (
    <Card
      size="small"
      className="page-card"
      data-upgrade-phase={job.phase}
      title={
        <Space size={8} wrap>
          <Typography.Text strong>{active ? activeTitle : lastTitle}</Typography.Text>
          <Tag color={rolledBackByHand ? "success" : tag.color}>{tag.label}</Tag>
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
          items={helperJob(job) ? HELPER_STEPS : SUPERVISOR_STEPS}
        />
        {active && q.isError ? (
          <Alert type="info" showIcon title="Waiting for the API server to come back…" />
        ) : null}
        {job.stale ? (
          <Alert
            type="warning"
            showIcon
            title={helperJob(job) ? "The update helper has not reported for a while" : "The supervisor is not running"}
            description={
              helperJob(job)
                ? "It continues on its own when the host starts again. On the host, ncc-orchestrator upgrade --status shows where it is, and ncc-orchestrator upgrade-helper continues it."
                : "This upgrade continues when the supervisor starts again (for example when the service restarts)."
            }
          />
        ) : null}
        {rolledBackByHand ? (
          <Alert
            type="success"
            showIcon
            title={job.message}
            action={
              <Button size="small" icon={<ReloadOutlined />} onClick={() => window.location.reload()}>
                Reload page
              </Button>
            }
          />
        ) : null}
        {job.phase === "rolled_back" && !rollback ? (
          <Alert
            type="warning"
            showIcon
            title="Rolled back; the previous version is running"
            description={job.error ? `Why: ${job.error}` : job.message}
          />
        ) : null}
        {job.phase === "failed" && unchanged ? (
          <Alert
            type="info"
            showIcon
            title="Nothing was changed"
            description={
              <>
                {job.error ? `Why: ${job.error}` : job.message}
                {backupLink}
              </>
            }
          />
        ) : null}
        {job.phase === "failed" && !unchanged ? (
          <Alert
            type="error"
            showIcon
            title={rollback ? "The rollback did not complete" : "The update did not complete"}
            description={
              <>
                {job.message}
                {backupLink}
              </>
            }
          />
        ) : null}
        {job.phase !== "done" && rollBack && offer ? <Alert type="info" showIcon title={offerText} action={rollbackButton} /> : null}
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
                {job.migrations?.length ? (
                  <ul style={{ margin: "4px 0 0", paddingLeft: 18 }}>
                    {job.migrations.map((m) => (
                      <li key={m}>{m}</li>
                    ))}
                  </ul>
                ) : null}
                {rollBack ? (
                  <div style={{ marginTop: 4 }}>
                    {offerText ||
                      `${job.action === "adopt" ? "The service definitions from before" : from || "The previous version"} can be put back until ${formatDateTime(job.rollback_until)}`}
                  </div>
                ) : null}
              </>
            }
            action={
              <Space orientation="vertical" size={6}>
                <Button size="small" icon={<ReloadOutlined />} onClick={() => window.location.reload()}>
                  Reload page
                </Button>
                {rollBack ? rollbackButton : null}
              </Space>
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
              label: `Details (${job.steps?.length ?? 0} steps${job.resumed ? ", resumed after a restart" : ""})`,
              children: (
                <Timeline
                  items={(job.steps ?? []).map((s) => ({
                    color: s.phase === "failed" ? "red" : s.phase === "rolling_back" || s.phase === "restoring" || s.phase === "rolled_back" ? "orange" : s.phase === "done" ? "green" : "blue",
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
