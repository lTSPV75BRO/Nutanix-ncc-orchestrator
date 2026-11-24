import { useEffect, useRef, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Modal,
  Space,
  Typography,
} from "antd";
import { CloudDownloadOutlined, ReloadOutlined } from "@ant-design/icons";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { UPGRADE_QUERY_KEY, UpgradeProgress, upgradeActive } from "../../updates/UpgradeProgress";
import { api } from "../../../api/client";
import type {
  ComponentsData,
  UpdateStatus,
} from "../../../api/types";
import { notify, notifyError } from "../../../notify";
import { IconTitle, KeyValueList, StatusBadge } from "../../../ui";

const UPDATE_PHASE_LABEL: Record<string, string> = {
  idle: "Idle",
  backing_up: "Backing up",
  updating: "Downloading & installing",
  restarting: "Restarting",
  supervised: "Handed to the supervisor",
  done: "Done",
  error: "Failed",
};

// UpdatesCard lets an admin check for a newer release and apply it in place. The
// backend takes a pre-update backup, installs the new stack (orchestrator + api
// + ui + frontend, checksum-verified), then restarts the stack; this card polls
// the job phase and reconnects the page once the restarted stack is healthy.
export function UpdatesCard({ isKubernetes = false }: { isKubernetes?: boolean }) {
  const [status, setStatus] = useState<UpdateStatus | null>(null);
  const [components, setComponents] = useState<ComponentsData["components"] | null>(null);
  const [checking, setChecking] = useState(false);
  const [applying, setApplying] = useState(false);
  const [restarting, setRestarting] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const qc = useQueryClient();
  const upgrade = useQuery({ queryKey: UPGRADE_QUERY_KEY, queryFn: () => api.updateStatus(), retry: false });
  const supervisorBusy = upgradeActive(upgrade.data?.upgrade);
  const supervisedUpgrades = (status ?? upgrade.data)?.supervised_upgrades === true;

  const job = status?.job;
  const phase = job?.phase ?? "idle";
  const inProgress = job?.in_progress ?? false;
  const busy = checking || applying || inProgress || restarting || supervisorBusy;

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  // Poll the backend health endpoint until the restarted stack answers, then
  // reload so the app re-bootstraps on the freshly-installed version.
  const waitForRestartAndReload = async () => {
    setRestarting(true);
    const deadline = Date.now() + 180_000;
    await new Promise((r) => setTimeout(r, 4000));
    while (Date.now() < deadline) {
      try {
        const resp = await fetch("/api/v1/health", { cache: "no-store", credentials: "include" });
        if (resp.ok) break;
      } catch {
        // still down — keep waiting
      }
      await new Promise((r) => setTimeout(r, 2000));
    }
    window.location.reload();
  };

  const startPolling = () => {
    stopPolling();
    pollRef.current = setInterval(async () => {
      try {
        const s = await api.updateStatus();
        setStatus((prev) => ({ ...(prev ?? {}), ...s }) as UpdateStatus);
        const p = s.job?.phase;
        if (p === "supervised") {
          // The supervisor carries on; UpgradeProgress follows it.
          stopPolling();
          setApplying(false);
          void qc.invalidateQueries({ queryKey: UPGRADE_QUERY_KEY });
        } else if (p === "restarting") {
          stopPolling();
          void waitForRestartAndReload();
        } else if (p === "done") {
          stopPolling();
          setApplying(false);
          notify.success(s.job?.message ?? "Update complete.");
        } else if (p === "error") {
          stopPolling();
          setApplying(false);
          notify.error(s.job?.error ?? "Update failed.");
        }
      } catch {
        // A failed status poll while applying almost certainly means the
        // restart has begun and the api-server is going down — switch to
        // reconnect mode.
        stopPolling();
        void waitForRestartAndReload();
      }
    }, 3000);
  };

  useEffect(() => {
    let cancelled = false;
    void Promise.all([api.updateStatus(), api.components()])
      .then(([s, componentData]) => {
        if (cancelled) return;
        setStatus(s);
        setComponents(componentData.components);
        if (s.job?.in_progress) {
          setApplying(true);
          startPolling();
        }
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
      stopPolling();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleCheck = async () => {
    setChecking(true);
    try {
      const [s, componentData] = await Promise.all([api.checkUpdate(), api.components()]);
      setStatus(s);
      setComponents(componentData.components);
      if (!componentData.components.consistent) {
        notify.warning("NCC components are running different versions.");
      }
    } catch (e) {
      notifyError(e, "Couldn't check for updates");
    } finally {
      setChecking(false);
    }
  };

  const doApply = async () => {
    setApplying(true);
    try {
      const res = await api.applyUpdate();
      const jobData = (res.data ?? {}) as UpdateStatus["job"];
      setStatus((prev) => ({ ...(prev ?? {}), job: jobData }) as UpdateStatus);
      notify.info(res.message ?? "Update started.");
      startPolling();
    } catch (e) {
      setApplying(false);
      notifyError(e, "Couldn't start the update");
    }
  };

  const confirmApply = () => {
    Modal.confirm({
      title: "Confirm software update",
      icon: <CloudDownloadOutlined style={{ color: "var(--ui-accent)" }} />,
      width: 540,
      content: (
        <div>
          <Typography.Paragraph>
            This installation will be updated to{" "}
            <b>{status?.latest_version ? `v${status.latest_version}` : "the latest release"}</b>. The
            server will perform the following steps:
          </Typography.Paragraph>
          <ol style={{ marginTop: 0, paddingLeft: 20 }}>
            <li>
              create a <b>pre-update backup</b> of configuration and the current software version
              (saved under <Typography.Text code>backups/</Typography.Text> in the installation
              directory), so you can roll the update back later,
            </li>
            <li>
              install the new orchestrator, API, UI, and frontend components (checksum-verified),
              then
            </li>
            {supervisedUpgrades ? (
              <li>
                <b>restart the API and then the web UI</b>, checking that each comes back healthy. If one does
                not, the previous version is <b>put back automatically</b>.
              </li>
            ) : (
              <li>
                <b>restart the services automatically</b> to activate the new version.
              </li>
            )}
          </ol>
          <Typography.Paragraph style={{ marginBottom: 0 }}>
            This page will reconnect automatically once the restarted stack is healthy. You may be
            prompted to sign in again.
          </Typography.Paragraph>
        </div>
      ),
      okText: "Back up and update",
      cancelText: "Cancel",
      onOk: doApply,
    });
  };

  const supported = status?.supported !== false;
  const updateAvailable = status?.update_available === true;
  const checksOff = status?.disabled === true;

  return (
    <Card
      className="page-card"
      title={<IconTitle icon={<CloudDownloadOutlined />}>Software updates</IconTitle>}
      extra={
        <Button
          icon={<ReloadOutlined />}
          onClick={handleCheck}
          loading={checking}
          disabled={isKubernetes || checksOff || (busy && !checking)}
        >
          Check for updates
        </Button>
      }
    >
      {checksOff ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          title="Update checks are turned off"
          description="Nothing is looked up or installed from the release server. An administrator can turn it on under Settings → Features."
        />
      ) : null}
      {isKubernetes ? (
        <Alert
          type="info"
          showIcon
          title="Update with Helm or Kustomize"
          description="On Kubernetes, change the image tags and let the Deployments roll out. Updating in place is turned off."
        />
      ) : null}
      <Typography.Paragraph type="secondary">
        An update backs up first, checks the release checksums, installs every component together and
        restarts the services.
      </Typography.Paragraph>

      <Space orientation="vertical" size={12} style={{ width: "100%" }}>
        <KeyValueList
          items={[
            { label: "Installed", value: status?.current_version ? `v${status.current_version}` : "Unknown" },
            ...(status?.latest_version
              ? [
                  {
                    label: "Latest",
                    value: updateAvailable ? (
                      <StatusBadge tone="warn">{`v${status.latest_version} available`}</StatusBadge>
                    ) : (
                      `v${status.latest_version}`
                    ),
                  },
                ]
              : []),
            ...(components
              ? (["orchestrator", "api-server", "ui-server"] as const).map((name) => {
                  const component = components[name];
                  return {
                    label: name,
                    value:
                      component.status === "ok" ? (
                        `v${component.version}`
                      ) : (
                        <StatusBadge tone="fail">Not found</StatusBadge>
                      ),
                  };
                })
              : []),
          ]}
        />
        {components ? (
          <>
            {!components.consistent ? (
              <Alert
                type="warning"
                showIcon
                title="Installed components are out of sync"
                description={
                  [
                    components.message,
                    isKubernetes
                      ? "Roll out matching image tags with Helm or Kustomize. Run now is blocked while the runner is on a different minor version."
                      : "The orchestrator, API server and UI server must run the same version. Apply the update to bring them in line.",
                  ]
                    .filter(Boolean)
                    .join(" ")
                }
              />
            ) : null}
          </>
        ) : null}

        {!supported && !checksOff ? (
          <Alert
            type="info"
            showIcon
            title="In-app updates are not available"
            description="This build has no ncc-orchestrator binary to replace. Update from the release package instead."
          />
        ) : null}

        {supported && !inProgress && !restarting && status?.check_error ? (
          <Alert type="warning" showIcon title="Couldn't check for updates" description={status.check_error} />
        ) : null}

        {supported && !inProgress && !restarting && status?.update_available === false ? (
          <Alert type="success" showIcon title="You're on the latest release." />
        ) : null}

        {!isKubernetes && supported && !inProgress && !restarting && !status?.check_error && updateAvailable ? (
          <Alert
            type="warning"
            showIcon
            title={`Update available: v${status?.latest_version}`}
            description="Back up and update installs it and restarts the services."
            action={
              <Button type="primary" icon={<CloudDownloadOutlined />} onClick={confirmApply}>
                Back up and update
              </Button>
            }
          />
        ) : null}

        {(inProgress || restarting) ? (
          <Alert
            type="info"
            showIcon
            title={`Update in progress — ${UPDATE_PHASE_LABEL[restarting ? "restarting" : phase] ?? phase}`}
            description={
              restarting
                ? "The services are restarting. This page reconnects on its own."
                : (job?.message ?? "Processing…")
            }
          />
        ) : null}

        {!inProgress && !restarting && phase === "error" && job?.error ? (
          <Alert type="error" showIcon title="The last update failed" description={job.error} />
        ) : null}

        {!isKubernetes ? <UpgradeProgress /> : null}
      </Space>
    </Card>
  );
}
