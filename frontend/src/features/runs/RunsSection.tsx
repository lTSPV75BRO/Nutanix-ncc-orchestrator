import { useEffect, useMemo, useRef, useState } from "react";
import { Alert, Badge, Button, Card, Col, Collapse, Empty, Input, List, Row, Select, Space, Switch, Table, Tag, Tooltip, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import {
  ReloadOutlined,
  ThunderboltOutlined,
  FileSearchOutlined,
  DownloadOutlined,
  FileTextOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  PlayCircleOutlined,
  StopOutlined,
  ClockCircleOutlined,
} from "@ant-design/icons";
import { api } from "../../api/client";
import type { ActiveRunEntry, ArtifactInfo, ConfigListItem, RunActiveData, RunInfo, RunPreflightData } from "../../api/types";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { CodeEditor } from "../../components/CodeEditor";
import { notify } from "../../notify";
import { useAuth } from "../../auth/AuthContext";
import { formatDateTime } from "../../utils/datetime";

const RUN_TOAST_KEY = "ncc-run-active";

type Props = {
  backendConfigPath: string;
  onError: (e: unknown) => void;
};

function parseExtraArgs(raw: string): string[] {
  return raw.trim() ? raw.trim().split(/\s+/) : [];
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(value >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

const formatTime = (value: string): string => formatDateTime(value);

function formatElapsedSeconds(sec?: number): string {
  if (!Number.isFinite(sec) || (sec ?? 0) < 0) return "0s";
  const totalSec = Math.floor(sec ?? 0);
  const s = totalSec % 60;
  const m = Math.floor(totalSec / 60) % 60;
  const h = Math.floor(totalSec / 3600);
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

export function RunsSection({ backendConfigPath, onError }: Props) {
  const { me } = useAuth();
  // When the caller is confined to cluster groups, expose only their clusters
  // and let them optionally narrow the run to a subset of that allowed set.
  const clusterRestricted = me?.cluster_access_unrestricted === false;
  const allowedClusters = useMemo(() => me?.allowed_clusters ?? [], [me?.allowed_clusters]);
  const [selectedClusters, setSelectedClusters] = useState<string[]>([]);
  const [runConfigPath, setRunConfigPath] = useState("");
  const [runConfigOptions, setRunConfigOptions] = useState<ConfigListItem[]>([]);
  const [runPassword, setRunPassword] = useState("");
  const [extraArgs, setExtraArgs] = useLocalStorageState("runs.extraArgs", "");
  const [runs, setRuns] = useState<RunInfo[]>([]);
  const [artifacts, setArtifacts] = useState<ArtifactInfo[]>([]);
  const [active, setActive] = useState<RunActiveData | null>(null);
  const [preflight, setPreflight] = useState<RunPreflightData | null>(null);
  const [followTail, setFollowTail] = useLocalStorageState("runs.logs.followTail", true);
  const [jumpToLastSignal, setJumpToLastSignal] = useState(0);
  const [, setElapsedTick] = useState(0);

  const initialLoadCompletedRef = useRef(false);
  const prevRunningCountRef = useRef<number>(0);

  const activeRuns: ActiveRunEntry[] = useMemo(() => active?.runs ?? [], [active?.runs]);
  const runningRuns = useMemo(() => activeRuns.filter((r) => r.status === "running"), [activeRuns]);
  const queuedRuns = useMemo(() => activeRuns.filter((r) => r.status === "queued"), [activeRuns]);
  const runningCount = runningRuns.length;
  const maxConcurrent = active?.max_concurrent ?? 0;

  useEffect(() => {
    if (backendConfigPath && !runConfigPath) setRunConfigPath(backendConfigPath);
  }, [backendConfigPath]);

  const loadRunConfigOptions = async () => {
    try {
      const [cfgs, pref] = await Promise.all([api.runConfigs(), api.getRunConfigPreference()]);
      const items = cfgs.items ?? [];
      setRunConfigOptions(items);
      const fallbackPath = items.find((it) => it.is_active)?.path || backendConfigPath || "";
      const preferred = (pref.path || cfgs.default_path || me?.run_config_path || "").trim();
      const selected = preferred || fallbackPath;
      if (selected) setRunConfigPath(selected);
    } catch (e) {
      onError(e);
    }
  };

  const triggerPayload = useMemo(
    () => ({
      config_path: runConfigPath || undefined,
      password: runPassword || undefined,
      extra_args: parseExtraArgs(extraArgs),
      // Only send an explicit cluster subset; an empty list lets the server run
      // the caller's full allowed set (members) or every cluster (admins).
      clusters: clusterRestricted && selectedClusters.length > 0 ? selectedClusters : undefined,
    }),
    [runConfigPath, runPassword, extraArgs, clusterRestricted, selectedClusters],
  );

  const loadRunActive = async () => {
    try {
      setActive(await api.runActive());
    } catch (e) {
      onError(e);
    }
  };

  const refreshRuns = async () => {
    try {
      // Raw multi-cluster artifacts are admin-only; group-restricted members use
      // the filtered dashboard instead, so skip the (forbidden) artifacts call.
      if (clusterRestricted) {
        setRuns(await api.runs());
        setArtifacts([]);
        return;
      }
      const [r, a] = await Promise.all([api.runs(), api.artifacts()]);
      setRuns(r);
      setArtifacts(a);
    } catch (e) {
      onError(e);
    }
  };

  useEffect(() => {
    const bootstrap = async () => {
      await loadRunConfigOptions();
      await loadRunActive();
      await refreshRuns();
      initialLoadCompletedRef.current = true;
    };
    void bootstrap();
    const t = setInterval(() => {
      void loadRunActive();
    }, 3000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Refresh the runs/artifacts history whenever the number of running runs
  // drops (a run finished) so completed results populate without manual reload.
  useEffect(() => {
    if (!initialLoadCompletedRef.current) {
      prevRunningCountRef.current = runningCount;
      return;
    }
    if (runningCount < prevRunningCountRef.current) {
      void refreshRuns();
    }
    prevRunningCountRef.current = runningCount;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runningCount]);

  // Tick once a second so running-run elapsed counters advance smoothly.
  useEffect(() => {
    if (runningCount === 0) return;
    const id = window.setInterval(() => setElapsedTick((n) => n + 1), 1000);
    return () => window.clearInterval(id);
  }, [runningCount]);

  const triggerRun = async () => {
    notify.loading({
      key: RUN_TOAST_KEY,
      message: "Starting run…",
      description: "Submitting trigger request to the API server.",
    });
    try {
      const out = await api.runTrigger(triggerPayload);
      notify.close(RUN_TOAST_KEY);
      const skipped = out.skipped_clusters ?? [];
      const skippedNote =
        skipped.length > 0 ? (
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            {skipped.length} cluster(s) already running in another run were skipped: {skipped.join(", ")}.
          </Typography.Text>
        ) : null;

      if (out.queued) {
        notify.info({
          message: `Run queued (position ${out.queue_position ?? "?"})`,
          description: (
            <Space orientation="vertical" size={2}>
              <span>All concurrency slots are busy — this run will start automatically when one frees.</span>
              {skippedNote}
            </Space>
          ),
          duration: 8,
        });
      } else if (out.started === false) {
        // Nothing to run: every requested cluster is already being refreshed.
        notify.info({
          message: "Already running",
          description: (
            <Space orientation="vertical" size={2}>
              <span>All requested clusters are already being refreshed by an in-progress run. Their results will update when it finishes.</span>
              {skippedNote}
            </Space>
          ),
          duration: 8,
        });
      } else {
        const ran = out.clusters ?? [];
        notify.success({
          message: "Run triggered",
          description: (
            <Space orientation="vertical" size={2}>
              <span>
                {ran.length > 0 ? `Running ${ran.length} cluster(s).` : "Running all clusters."}
                {typeof out.running_count === "number" ? ` ${out.running_count} run(s) now active.` : ""}
              </span>
              {skippedNote}
            </Space>
          ),
          duration: 6,
        });
      }
      await loadRunActive();
    } catch (e) {
      notify.close(RUN_TOAST_KEY);
      onError(e);
    }
  };

  const cancelRun = async (id?: string) => {
    notify.loading({
      key: RUN_TOAST_KEY,
      message: "Cancelling run…",
      description: id ? `Signalling run ${id} to exit.` : "Signalling all active runs to exit.",
    });
    try {
      const out = await api.runCancel(id);
      notify.close(RUN_TOAST_KEY);
      notify.success({
        message: "Cancellation requested",
        description: id
          ? `Run ${out.run_id ?? id} will exit shortly.`
          : `${out.cancelled ?? 0} run(s) signalled to exit.`,
        duration: 5,
      });
      await loadRunActive();
    } catch (e) {
      notify.close(RUN_TOAST_KEY);
      onError(e);
    }
  };

  const runPreflight = async () => {
    notify.loading({
      key: "ncc-preflight",
      message: "Running preflight…",
      description: "Validating config and connectivity.",
    });
    try {
      const out = await api.runPreflight({ config_path: runConfigPath || undefined });
      setPreflight(out);
      notify.close("ncc-preflight");
      if (out.ok) {
        notify.success({
          message: "Preflight passed",
          description: `${out.checks.length} checks evaluated · ${out.warn} warning${out.warn === 1 ? "" : "s"}`,
        });
      } else {
        notify.warning({
          message: "Preflight has blocking failures",
          description: `${out.failed} failed · ${out.warn} warning${out.warn === 1 ? "" : "s"}. Review the Preflight Result card.`,
          duration: 8,
        });
      }
    } catch (e) {
      notify.close("ncc-preflight");
      onError(e);
    }
  };

  const onRunConfigChange = async (value: string) => {
    setRunConfigPath(value);
    try {
      await api.updateRunConfigPreference(value);
    } catch (e) {
      onError(e);
    }
  };

  // Format a duration in seconds as "5m 51s" / "1h 12m" / "42s".
  const formatDurationS = (s?: number): string => {
    if (typeof s !== "number" || !Number.isFinite(s) || s <= 0) return "—";
    const total = Math.round(s);
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    const sec = total % 60;
    if (h > 0) return `${h}h ${m}m`;
    if (m > 0) return `${m}m ${sec}s`;
    return `${sec}s`;
  };

  const renderIssueCounts = (row: RunInfo) => {
    const f = row.fail_total ?? 0;
    const w = row.warn_total ?? 0;
    const e = row.err_total ?? 0;
    if (row.source === "trigger" || (f === 0 && w === 0 && e === 0 && row.total_checks == null)) {
      return <Typography.Text type="secondary">—</Typography.Text>;
    }
    return (
      <Space size={6} wrap>
        {f > 0 ? <Tag color="error" style={{ marginInlineEnd: 0 }}>F {f}</Tag> : null}
        {w > 0 ? <Tag color="warning" style={{ marginInlineEnd: 0 }}>W {w}</Tag> : null}
        {e > 0 ? <Tag color="default" style={{ marginInlineEnd: 0 }}>E {e}</Tag> : null}
        {f === 0 && w === 0 && e === 0 ? (
          <Tag color="success" style={{ marginInlineEnd: 0 }}>clean</Tag>
        ) : null}
      </Space>
    );
  };

  const renderSource = (row: RunInfo) => {
    const scheduled = row.run_source === "scheduled";
    if (row.source === "history") {
      return scheduled ? (
        <Tooltip title="Launched automatically by the schedule (systemd timer / cron)">
          <Tag color="purple" style={{ marginInlineEnd: 0 }}>Scheduled</Tag>
        </Tooltip>
      ) : (
        <Tag color="blue" style={{ marginInlineEnd: 0 }}>Run</Tag>
      );
    }
    if (row.source === "summary") {
      return scheduled ? (
        <Tag color="purple" style={{ marginInlineEnd: 0 }}>Scheduled · latest</Tag>
      ) : (
        <Tag color="cyan" style={{ marginInlineEnd: 0 }}>Latest</Tag>
      );
    }
    if (row.source === "trigger") return <Tag style={{ marginInlineEnd: 0 }}>Trigger</Tag>;
    return <Tag style={{ marginInlineEnd: 0 }}>—</Tag>;
  };

  const renderStatus = (row: RunInfo) => {
    if (row.source === "trigger") {
      return row.success === false
        ? <Tag color="error" style={{ marginInlineEnd: 0 }}>Trigger failed</Tag>
        : <Tag color="processing" style={{ marginInlineEnd: 0 }}>Triggered</Tag>;
    }
    if (row.success === true) {
      return <Tag color="success" style={{ marginInlineEnd: 0 }}>Success</Tag>;
    }
    if (row.success === false) {
      const ec = typeof row.exit_code === "number" && row.exit_code !== 0 ? ` · exit ${row.exit_code}` : "";
      return <Tag color="error" style={{ marginInlineEnd: 0 }}>{`Failed${ec}`}</Tag>;
    }
    return <Tag style={{ marginInlineEnd: 0 }}>—</Tag>;
  };

  const runColumns: ColumnsType<RunInfo> = [
    {
      title: "Run ID",
      dataIndex: "id",
      key: "id",
      render: (v: string) => <Typography.Text code>{v}</Typography.Text>,
    },
    { title: "Started", dataIndex: "mod_time", key: "mod_time", width: 200, render: formatTime },
    { title: "Type", dataIndex: "source", key: "source", width: 120, render: (_v, row) => renderSource(row) },
    { title: "Status", key: "status", width: 150, render: (_v, row) => renderStatus(row) },
    {
      title: "Duration",
      dataIndex: "duration_s",
      key: "duration_s",
      width: 100,
      render: (v: number | undefined, row) =>
        row.source === "trigger" ? (
          <Typography.Text type="secondary">—</Typography.Text>
        ) : (
          <Typography.Text>{formatDurationS(v)}</Typography.Text>
        ),
    },
    {
      title: "Clusters",
      key: "clusters",
      width: 110,
      render: (_v, row) => {
        const ok = row.clusters_ok ?? 0;
        const failed = row.clusters_failed ?? 0;
        const total = ok + failed;
        if (row.source === "trigger" || total === 0) {
          return <Typography.Text type="secondary">—</Typography.Text>;
        }
        return (
          <Tooltip title={`${ok} reachable, ${failed} failed`}>
            <Typography.Text>{`${ok}/${total} OK`}</Typography.Text>
          </Tooltip>
        );
      },
    },
    { title: "Issues", key: "issues", render: (_v, row) => renderIssueCounts(row) },
  ];

  const artifactColumns: ColumnsType<ArtifactInfo> = [
    {
      title: "Artifact",
      dataIndex: "name",
      key: "name",
      render: (v: string) => (
        <a href={`/api/v1/artifacts/${encodeURIComponent(v)}`} target="_blank" rel="noreferrer">
          <FileTextOutlined style={{ marginRight: 6 }} />
          {v}
        </a>
      ),
    },
    { title: "Size", dataIndex: "size", key: "size", width: 120, render: (v: number) => formatBytes(v) },
    { title: "Modified", dataIndex: "mod_time", key: "mod_time", width: 200, render: formatTime },
    {
      title: " ",
      key: "actions",
      width: 130,
      render: (_, row) => (
        <Button size="small" icon={<DownloadOutlined />} href={`/api/v1/artifacts/${encodeURIComponent(row.name)}?download=1`}>
          Download
        </Button>
      ),
    },
  ];

  // Rough queue ETA: a run at position p waits for ~ceil(p / slots) batches of
  // the average run duration before a slot frees. Only shown once at least one
  // run has completed (so we have an average to project from).
  const avgRunDurationSec = active?.avg_run_duration_sec ?? 0;
  const estimateWaitSec = (position?: number): number | undefined => {
    if (!position || position < 1 || avgRunDurationSec <= 0) return undefined;
    const slots = Math.max(1, maxConcurrent || 1);
    return Math.ceil(position / slots) * avgRunDurationSec;
  };

  const renderRunEntry = (run: ActiveRunEntry) => {
    const clusters = run.clusters ?? [];
    const skipped = run.skipped ?? [];
    const waitSec = run.status === "queued" ? estimateWaitSec(run.queue_position) : undefined;
    const queuedLabel = run.queue_position ? `Queued · #${run.queue_position} in line` : "Queued";
    const isScheduled = run.source === "scheduled" || run.external === true;
    const header = (
      <Space size={8} wrap>
        <Badge status={run.status === "running" ? "processing" : "default"} />
        <Typography.Text code>{run.id}</Typography.Text>
        {run.group ? <Tag color="purple">{run.group}</Tag> : null}
        {isScheduled ? (
          <Tooltip title="Launched by the schedule (systemd timer / cron) outside this server">
            <Tag color="purple" icon={<ClockCircleOutlined />}>scheduled</Tag>
          </Tooltip>
        ) : null}
        {run.all_clusters ? <Tag color="geekblue">all clusters</Tag> : null}
        <Tag color={run.status === "running" ? "processing" : "default"}>
          {run.status === "running" ? `Running · ${formatElapsedSeconds(run.elapsed_sec)}` : queuedLabel}
        </Tag>
        {waitSec !== undefined ? (
          <Tooltip title="Estimated wait based on the average duration of recent runs">
            <Tag icon={<ClockCircleOutlined />} color="default">{`~${formatElapsedSeconds(waitSec)} ETA`}</Tag>
          </Tooltip>
        ) : null}
      </Space>
    );
    return {
      key: run.id,
      label: header,
      // A scheduled/external run isn't owned by this server's run manager, so it
      // can't be signalled here — show the log source instead of a Cancel button.
      extra: isScheduled ? (
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>scheduler log</Typography.Text>
      ) : (
        <Button
          danger
          size="small"
          icon={<StopOutlined />}
          onClick={(e) => {
            e.stopPropagation();
            void cancelRun(run.id);
          }}
        >
          Cancel
        </Button>
      ),
      children: (
        <Space orientation="vertical" size={10} style={{ width: "100%" }}>
          <div>
            <Typography.Text type="secondary" style={{ marginRight: 8 }}>Clusters:</Typography.Text>
            {clusters.length > 0 ? (
              clusters.map((c) => <Tag key={c}>{c}</Tag>)
            ) : run.all_clusters ? (
              <Tag color="geekblue">all configured clusters</Tag>
            ) : (
              <Typography.Text type="secondary">—</Typography.Text>
            )}
          </div>
          {skipped.length > 0 ? (
            <Alert
              type="info"
              showIcon
              title="Some clusters were skipped (already running in another run)"
              description={
                <Space orientation="vertical" size={2}>
                  {skipped.map((c) => (
                    <Typography.Text key={c}>
                      {c}
                      {run.skipped_owner?.[c] ? (
                        <Typography.Text type="secondary"> · owned by run {run.skipped_owner[c]}</Typography.Text>
                      ) : null}
                    </Typography.Text>
                  ))}
                </Space>
              }
            />
          ) : null}
          {run.status === "running" ? (
            <CodeEditor
              value={run.live_output || "Waiting for runner output…"}
              language="plaintext"
              readOnly
              height={260}
              autoRevealLastLine={followTail}
              jumpToLastSignal={jumpToLastSignal}
            />
          ) : (
            <Typography.Text type="secondary">
              {run.queue_position
                ? `Waiting for a free slot — #${run.queue_position} in line${
                    waitSec !== undefined ? ` · ~${formatElapsedSeconds(waitSec)} estimated` : ""
                  }.`
                : "Waiting for a free slot to start…"}
            </Typography.Text>
          )}
        </Space>
      ),
    };
  };

  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Trigger Run
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          Run NCC against the configured clusters. Multiple runs can execute at once — clusters already being refreshed by
          another active run are reused, so only the remaining clusters run. Use Preflight to validate before triggering.
        </Typography.Text>
        {clusterRestricted ? (
          <Alert
            type="info"
            showIcon
            style={{ marginTop: 12 }}
            title="Your access is limited to your cluster groups"
            description={
              allowedClusters.length > 0
                ? `You can run and view ${allowedClusters.length} cluster(s): ${allowedClusters.join(", ")}.`
                : "You are not a member of any cluster group. Ask an administrator to add you to a group before triggering a run."
            }
          />
        ) : null}
        <form
          onSubmit={(e) => {
            e.preventDefault();
            void triggerRun();
          }}
          autoComplete="off"
        >
          <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
            <Col xs={24} md={12}>
              <label htmlFor="run-config-path" style={{ display: "block", marginBottom: 4 }}>
                <Typography.Text type="secondary">Config file</Typography.Text>
              </label>
              <Select
                id="run-config-path"
                value={runConfigPath}
                onChange={(v) => void onRunConfigChange(v)}
                options={runConfigOptions.map((item) => ({
                  value: item.path,
                  label: item.path,
                }))}
                placeholder={backendConfigPath || "config.yaml"}
                style={{ width: "100%" }}
                showSearch
              />
            </Col>
            <Col xs={24} md={12}>
              <label htmlFor="run-password" style={{ display: "block", marginBottom: 4 }}>
                <Typography.Text type="secondary">One-off password (optional)</Typography.Text>
              </label>
              <Input.Password
                id="run-password"
                name="run-password"
                value={runPassword}
                onChange={(e) => setRunPassword(e.target.value)}
                placeholder="overrides config"
                autoComplete="new-password"
              />
            </Col>
            {clusterRestricted ? (
              <Col xs={24}>
                <label htmlFor="run-clusters" style={{ display: "block", marginBottom: 4 }}>
                  <Typography.Text type="secondary">Clusters (optional — defaults to all your clusters)</Typography.Text>
                </label>
                <Select
                  id="run-clusters"
                  mode="multiple"
                  allowClear
                  style={{ width: "100%" }}
                  placeholder="Leave empty to run all clusters in your groups"
                  value={selectedClusters}
                  onChange={setSelectedClusters}
                  options={allowedClusters.map((c) => ({ value: c, label: c }))}
                />
              </Col>
            ) : null}
            <Col xs={24}>
              <label htmlFor="run-extra-args" style={{ display: "block", marginBottom: 4 }}>
                <Typography.Text type="secondary">Additional flags (optional)</Typography.Text>
              </label>
              <Input
                id="run-extra-args"
                name="extra-args"
                value={extraArgs}
                onChange={(e) => setExtraArgs(e.target.value)}
                placeholder="e.g. --no-html --output-dir outputfiles"
                autoComplete="off"
              />
            </Col>
          </Row>
          <button type="submit" style={{ display: "none" }} aria-hidden="true" tabIndex={-1} />
        </form>
        <Space size={8} wrap style={{ marginTop: 12 }}>
          <Button type="primary" icon={<ThunderboltOutlined />} onClick={triggerRun}>
            Trigger Run
          </Button>
          <Button icon={<FileSearchOutlined />} onClick={runPreflight}>
            Run Preflight
          </Button>
          <Button icon={<ReloadOutlined />} onClick={() => { void loadRunActive(); void refreshRuns(); }}>
            Refresh
          </Button>
          {runningCount > 0 ? (
            <Tag icon={<PlayCircleOutlined spin />} color="processing" style={{ fontSize: 13, padding: "4px 10px" }}>
              {runningCount} running{maxConcurrent ? ` / ${maxConcurrent}` : ""}
            </Tag>
          ) : null}
          {queuedRuns.length > 0 ? (
            <Tag icon={<ClockCircleOutlined />} color="warning" style={{ fontSize: 13, padding: "4px 10px" }}>
              {queuedRuns.length} queued
            </Tag>
          ) : null}
        </Space>
      </Card>

      {preflight ? (
        <Card className="page-card">
          <Typography.Title level={4} className="section-title">
            Preflight Result
          </Typography.Title>
          <Alert
            type={preflight.ok ? "success" : "error"}
            showIcon
            title={preflight.ok ? "Preflight passed" : "Preflight has blocking failures"}
            description={`Config: ${preflight.config_path} · Failed: ${preflight.failed} · Warnings: ${preflight.warn}`}
          />
          <List
            style={{ marginTop: 12 }}
            bordered
            size="small"
            dataSource={preflight.checks}
            renderItem={(item) => (
              <List.Item>
                <Space orientation="vertical" size={2} style={{ width: "100%" }}>
                  <Space size={6} wrap>
                    <Typography.Text strong>{item.title}</Typography.Text>
                    <Tag color={item.status === "pass" ? "success" : item.status === "warn" ? "warning" : "error"}>
                      {item.status.toUpperCase()}
                    </Tag>
                    <Typography.Text code>{item.id}</Typography.Text>
                  </Space>
                  <Typography.Text>{item.message}</Typography.Text>
                  {item.hint ? <Typography.Text type="secondary">Hint: {item.hint}</Typography.Text> : null}
                </Space>
              </List.Item>
            )}
          />
        </Card>
      ) : null}

      <Card
        className="page-card"
        title={
          <Space size={8}>
            <Typography.Title level={4} className="section-title" style={{ margin: 0 }}>
              Active &amp; Queued Runs
            </Typography.Title>
            {runningCount > 0 ? (
              <Badge status="processing" text={<Typography.Text strong>{runningCount} running</Typography.Text>} />
            ) : active?.last_error ? (
              <Tag icon={<CloseCircleOutlined />} color="error">Last run failed</Tag>
            ) : (
              <Tag icon={<CheckCircleOutlined />} color="default">Idle</Tag>
            )}
          </Space>
        }
        extra={
          activeRuns.length > 0 ? (
            <Space size={8}>
              <Switch id="runs-follow-tail" aria-label="Follow tail" checked={followTail} onChange={setFollowTail} size="small" />
              <Typography.Text type="secondary">
                <label htmlFor="runs-follow-tail">Follow tail</label>
              </Typography.Text>
              <Button size="small" onClick={() => setJumpToLastSignal((n) => n + 1)}>Jump to latest</Button>
              <Button size="small" danger icon={<StopOutlined />} onClick={() => cancelRun()}>Cancel all</Button>
            </Space>
          ) : null
        }
      >
        {activeRuns.length > 0 ? (
          <Collapse
            defaultActiveKey={runningRuns.map((r) => r.id)}
            items={activeRuns.map(renderRunEntry)}
          />
        ) : active?.last_error ? (
          <Alert
            type="error"
            showIcon
            icon={<StopOutlined />}
            title="Last run failed"
            description={<Typography.Text>{active.last_error}</Typography.Text>}
          />
        ) : (
          <Typography.Text type="secondary">No runs currently in progress.</Typography.Text>
        )}
      </Card>

      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Runs
        </Typography.Title>
        {runs.length === 0 ? (
          <Empty description="No runs recorded yet" />
        ) : (
          <Table size="small" rowKey="id" columns={runColumns} dataSource={runs} pagination={{ pageSize: 10, showSizeChanger: false }} />
        )}
      </Card>

      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Artifacts
        </Typography.Title>
        {artifacts.length === 0 ? (
          <Empty description="No artifacts available" />
        ) : (
          <Table size="small" rowKey="name" columns={artifactColumns} dataSource={artifacts} pagination={{ pageSize: 10, showSizeChanger: false }} />
        )}
      </Card>
    </Space>
  );
}
