import { useEffect, useMemo, useRef, useState } from "react";
import { Alert, Badge, Button, Card, Col, Descriptions, Empty, Input, List, Row, Space, Switch, Table, Tag, Tooltip, Typography } from "antd";
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
} from "@ant-design/icons";
import { api, ApiError } from "../../api/client";
import type { ArtifactInfo, RunActiveData, RunConflictData, RunInfo, RunPreflightData } from "../../api/types";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { CodeEditor } from "../../components/CodeEditor";
import { notify } from "../../notify";

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

function formatTime(value: string): string {
  if (!value) return "-";
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString();
}

function formatElapsed(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return "0s";
  const totalSec = Math.floor(ms / 1000);
  const s = totalSec % 60;
  const totalMin = Math.floor(totalSec / 60);
  const m = totalMin % 60;
  const h = Math.floor(totalMin / 60);
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (totalMin > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function summariseError(raw: string | undefined, fallback = "Run terminated unexpectedly."): string {
  const trimmed = (raw || "").trim();
  if (!trimmed) return fallback;
  const firstLine = trimmed.split(/\r?\n/).find((l) => l.trim().length > 0) || trimmed;
  return firstLine.length > 220 ? `${firstLine.slice(0, 220)}…` : firstLine;
}

export function RunsSection({ backendConfigPath, onError }: Props) {
  const [runConfigPath, setRunConfigPath] = useState("");
  const [runPassword, setRunPassword] = useState("");
  const [extraArgs, setExtraArgs] = useLocalStorageState("runs.extraArgs", "");
  const [liveLogs, setLiveLogs] = useState("");
  const [runs, setRuns] = useState<RunInfo[]>([]);
  const [artifacts, setArtifacts] = useState<ArtifactInfo[]>([]);
  const [active, setActive] = useState<RunActiveData | null>(null);
  const [preflight, setPreflight] = useState<RunPreflightData | null>(null);
  const [followTail, setFollowTail] = useLocalStorageState("runs.logs.followTail", true);
  const [jumpToLastSignal, setJumpToLastSignal] = useState(0);
  const [elapsedTick, setElapsedTick] = useState(0);

  // Lifecycle tracking refs so we don't depend on stale closures.
  const prevActiveRef = useRef<boolean | null>(null);
  const runStartMsRef = useRef<number | null>(null);
  const initialLoadCompletedRef = useRef(false);
  const justTriggeredRef = useRef(false);

  useEffect(() => {
    if (backendConfigPath) setRunConfigPath(backendConfigPath);
  }, [backendConfigPath]);

  const triggerPayload = useMemo(
    () => ({
      config_path: runConfigPath || undefined,
      password: runPassword || undefined,
      extra_args: parseExtraArgs(extraArgs),
    }),
    [runConfigPath, runPassword, extraArgs],
  );

  const loadRunActive = async () => {
    try {
      const out = await api.runActive();
      setActive(out);
      try {
        const log = await api.runnerLogs();
        setLiveLogs(log.content || out.live_output || out.last_output || "");
      } catch {
        setLiveLogs(out.live_output || out.last_output || "");
      }
    } catch (e) {
      onError(e);
    }
  };

  const refreshRuns = async () => {
    try {
      const [r, a] = await Promise.all([api.runs(), api.artifacts()]);
      setRuns(r);
      setArtifacts(a);
    } catch (e) {
      onError(e);
    }
  };

  useEffect(() => {
    const bootstrap = async () => {
      await loadRunActive();
      await refreshRuns();
      initialLoadCompletedRef.current = true;
    };
    void bootstrap();
    const t = setInterval(() => {
      void loadRunActive();
    }, 3000);
    return () => clearInterval(t);
  }, []);

  // Run lifecycle: emit toasts on transitions between active/idle.
  useEffect(() => {
    if (!initialLoadCompletedRef.current && !justTriggeredRef.current) {
      // Skip on initial mount to avoid spurious toasts on page navigation.
      prevActiveRef.current = Boolean(active?.active);
      return;
    }
    const wasActive = prevActiveRef.current;
    const isActive = Boolean(active?.active);

    if (isActive && !wasActive) {
      const startedMs = active?.started_at ? new Date(active.started_at).getTime() : Date.now();
      runStartMsRef.current = Number.isFinite(startedMs) ? startedMs : Date.now();
      notify.loading({
        key: RUN_TOAST_KEY,
        message: "Run in progress",
        description: (
          <Space direction="vertical" size={2}>
            <span>Started {active?.started_at ? new Date(active.started_at).toLocaleTimeString() : "just now"}</span>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              Live logs are visible in the Active Run card below.
            </Typography.Text>
          </Space>
        ),
      });
      justTriggeredRef.current = false;
    }

    if (!isActive && wasActive) {
      const elapsedMs = runStartMsRef.current ? Date.now() - runStartMsRef.current : 0;
      runStartMsRef.current = null;
      notify.close(RUN_TOAST_KEY);
      const errorText = (active?.last_error || "").trim();
      if (errorText) {
        notify.error({
          message: "Run failed",
          description: (
            <Space direction="vertical" size={2}>
              <Typography.Text>{summariseError(errorText)}</Typography.Text>
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                Elapsed {formatElapsed(elapsedMs)} · See Active Run output for details.
              </Typography.Text>
            </Space>
          ),
          duration: 10,
        });
      } else {
        notify.success({
          message: "Run completed",
          description: `Finished in ${formatElapsed(elapsedMs)}. Refreshing artifacts…`,
          duration: 6,
        });
      }
      void refreshRuns();
    }

    prevActiveRef.current = isActive;
  }, [active?.active, active?.last_error, active?.started_at]);

  // Tick a state value every second while a run is active so the elapsed
  // counter rendered in the UI updates without needing to refetch.
  useEffect(() => {
    if (!active?.active) return;
    const id = window.setInterval(() => setElapsedTick((n) => n + 1), 1000);
    return () => window.clearInterval(id);
  }, [active?.active]);

  const triggerRun = async () => {
    if (active?.active) {
      notify.warning({
        message: "A run is already in progress",
        description: "Wait for the current run to finish before triggering another.",
      });
      return;
    }
    notify.loading({
      key: RUN_TOAST_KEY,
      message: "Starting run…",
      description: "Submitting trigger request to the API server.",
    });
    justTriggeredRef.current = true;
    try {
      const out = await api.runTrigger(triggerPayload);
      notify.loading({
        key: RUN_TOAST_KEY,
        message: "Run accepted",
        description: (
          <Space direction="vertical" size={2}>
            <span>
              PID <Typography.Text code>{out.pid}</Typography.Text> · Started{" "}
              {out.started_at ? new Date(out.started_at).toLocaleTimeString() : "just now"}
            </span>
            {out.config_path ? (
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                Config: {out.config_path}
              </Typography.Text>
            ) : null}
          </Space>
        ),
      });
      runStartMsRef.current = out.started_at ? new Date(out.started_at).getTime() : Date.now();
      await loadRunActive();
    } catch (e) {
      notify.close(RUN_TOAST_KEY);
      justTriggeredRef.current = false;
      // Backend rejected because another run is in flight — surface the
      // running run's metadata so the user can decide whether to wait or
      // investigate a stuck run.
      if (e instanceof ApiError && e.status === 409 && e.data && typeof e.data === "object") {
        const d = e.data as Partial<RunConflictData>;
        const startedAt = d.started_at ? new Date(d.started_at).toLocaleString() : "—";
        const elapsed = d.elapsed_human || (d.elapsed_seconds != null ? `${d.elapsed_seconds}s` : "—");
        notify.warning({
          message: "Cannot start another run — one is already in progress",
          description: (
            <Space direction="vertical" size={2}>
              <span>
                Started <Typography.Text>{startedAt}</Typography.Text> · Elapsed{" "}
                <Typography.Text code>{elapsed}</Typography.Text>
                {d.pid && d.pid > 0 ? (
                  <>
                    {" "}
                    · PID <Typography.Text code>{d.pid}</Typography.Text>
                  </>
                ) : null}
              </span>
              {d.overdue ? (
                <Typography.Text type="warning">
                  Active run has exceeded its expected duration — check the runner log.
                </Typography.Text>
              ) : null}
              {d.runner_log ? (
                <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                  Runner log: {d.runner_log}
                </Typography.Text>
              ) : null}
              {d.hint ? (
                <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                  {d.hint}
                </Typography.Text>
              ) : null}
            </Space>
          ),
          duration: 10,
        });
        await loadRunActive();
        return;
      }
      onError(e);
    }
  };

  const cancelRun = async () => {
    notify.loading({
      key: RUN_TOAST_KEY,
      message: "Cancelling run…",
      description: "Signalling the orchestrator process to exit.",
    });
    try {
      const out = await api.runCancel();
      notify.success({
        message: "Cancellation requested",
        description: `PID ${out.pid} signalled after ${out.elapsed_human}. The run will exit shortly.`,
        duration: 6,
      });
      await loadRunActive();
    } catch (e) {
      notify.close(RUN_TOAST_KEY);
      // 409 means no run is active anymore — surface a softer info toast.
      if (e instanceof ApiError && e.status === 409) {
        notify.info({
          message: "No run to cancel",
          description: "The run already finished before the cancel request arrived.",
        });
        await loadRunActive();
        return;
      }
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

  const elapsedDisplayMs = active?.active && runStartMsRef.current ? Date.now() - runStartMsRef.current : 0;
  // Reference elapsedTick so React picks up the second-by-second update.
  void elapsedTick;

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

  // Compact issue summary for runs that have summary data, e.g. "1F · 17W · 6E".
  // Empty string for trigger entries (which don't carry any counts) so the cell
  // gracefully renders as "—".
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

  // Source tag — communicates whether the row is an archived run, the latest
  // in-place summary, or just a trigger event from the audit log.
  const renderSource = (src?: RunInfo["source"]) => {
    if (src === "history") return <Tag color="blue" style={{ marginInlineEnd: 0 }}>Run</Tag>;
    if (src === "summary") return <Tag color="cyan" style={{ marginInlineEnd: 0 }}>Latest</Tag>;
    if (src === "trigger") return <Tag style={{ marginInlineEnd: 0 }}>Trigger</Tag>;
    return <Tag style={{ marginInlineEnd: 0 }}>—</Tag>;
  };

  // Status tag — for runs with a known success outcome, show pass/fail with
  // exit code; for triggers, indicate "Triggered" (with a soft warning tone if
  // the trigger itself failed at the API layer).
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
    {
      title: "Type",
      dataIndex: "source",
      key: "source",
      width: 100,
      render: (_v, row) => renderSource(row.source),
    },
    {
      title: "Status",
      key: "status",
      width: 150,
      render: (_v, row) => renderStatus(row),
    },
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
    {
      title: "Issues",
      key: "issues",
      render: (_v, row) => renderIssueCounts(row),
    },
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
        <Button
          size="small"
          icon={<DownloadOutlined />}
          href={`/api/v1/artifacts/${encodeURIComponent(row.name)}?download=1`}
        >
          Download
        </Button>
      ),
    },
  ];

  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Trigger Run
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          Run NCC against the configured clusters. Use Preflight to validate before triggering.
        </Typography.Text>
        {/*
          Wrap the trigger inputs in a real <form> so:
            1) Chrome stops emitting "Password field is not contained in a form"
               (it heuristically downgrades autofill/security if it's loose).
            2) Pressing Enter in any field triggers the run, matching user
               expectation for a one-action form.
          autoComplete="off" because these are session-scoped lab credentials,
          not user account credentials we want browsers to remember.
        */}
        <form
          onSubmit={(e) => {
            e.preventDefault();
            if (!active?.active) {
              void triggerRun();
            }
          }}
          autoComplete="off"
        >
          <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
            <Col xs={24} md={12}>
              <label htmlFor="run-config-path" style={{ display: "block", marginBottom: 4 }}>
                <Typography.Text type="secondary">Config file</Typography.Text>
              </label>
              <Input
                id="run-config-path"
                name="config-path"
                value={runConfigPath}
                onChange={(e) => setRunConfigPath(e.target.value)}
                placeholder={backendConfigPath || "config.yaml"}
                autoComplete="off"
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
          {/* Hidden submit button so Enter-key submission works on every browser. */}
          <button type="submit" style={{ display: "none" }} aria-hidden="true" tabIndex={-1} />
        </form>
        <Space size={8} wrap style={{ marginTop: 12 }}>
          <Button type="primary" icon={<ThunderboltOutlined />} onClick={triggerRun} disabled={Boolean(active?.active)}>
            {active?.active ? "Run in progress" : "Trigger Run"}
          </Button>
          {active?.active ? (
            <Button danger icon={<StopOutlined />} onClick={cancelRun}>
              Cancel Run
            </Button>
          ) : null}
          <Button icon={<FileSearchOutlined />} onClick={runPreflight}>
            Run Preflight
          </Button>
          <Button icon={<ReloadOutlined />} onClick={() => { void loadRunActive(); void refreshRuns(); }}>
            Refresh
          </Button>
          {active?.active ? (
            <Tag icon={<PlayCircleOutlined spin />} color="processing" style={{ fontSize: 13, padding: "4px 10px" }}>
              Running · {formatElapsed(elapsedDisplayMs)}
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
            message={preflight.ok ? "Preflight passed" : "Preflight has blocking failures"}
            description={`Config: ${preflight.config_path} · Failed: ${preflight.failed} · Warnings: ${preflight.warn}`}
          />
          <List
            style={{ marginTop: 12 }}
            bordered
            size="small"
            dataSource={preflight.checks}
            renderItem={(item) => (
              <List.Item>
                <Space direction="vertical" size={2} style={{ width: "100%" }}>
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
              Active Run
            </Typography.Title>
            {active?.active ? (
              <Badge status="processing" text={<Typography.Text strong>Running</Typography.Text>} />
            ) : active?.last_error ? (
              <Tag icon={<CloseCircleOutlined />} color="error">
                Last run failed
              </Tag>
            ) : (
              <Tag icon={<CheckCircleOutlined />} color="default">
                Idle
              </Tag>
            )}
          </Space>
        }
      >
        {active?.active ? (
          <Descriptions size="small" column={1} bordered>
            <Descriptions.Item label="Started">
              <Space size={6}>
                {formatTime(active.started_at)}
                <Typography.Text type="secondary">· elapsed {formatElapsed(elapsedDisplayMs)}</Typography.Text>
              </Space>
            </Descriptions.Item>
            <Descriptions.Item label="Config">{active.config_path || "-"}</Descriptions.Item>
            <Descriptions.Item label="Output dir">{active.output_dir || "-"}</Descriptions.Item>
            {active.last_error ? (
              <Descriptions.Item label="Last error">
                <Typography.Text type="danger">{active.last_error}</Typography.Text>
              </Descriptions.Item>
            ) : null}
          </Descriptions>
        ) : active?.last_error ? (
          <Alert
            type="error"
            showIcon
            icon={<StopOutlined />}
            message="Last run failed"
            description={<Typography.Text>{active.last_error}</Typography.Text>}
          />
        ) : (
          <Typography.Text type="secondary">No run currently in progress.</Typography.Text>
        )}
        <div style={{ marginTop: 16 }}>
          <Space size={8} style={{ marginBottom: 8 }}>
            <Typography.Text strong>Live Output</Typography.Text>
            <Switch id="runs-follow-tail" aria-label="Follow tail" checked={followTail} onChange={setFollowTail} size="small" />
            <Typography.Text type="secondary">
              <label htmlFor="runs-follow-tail">Follow tail</label>
            </Typography.Text>
            <Button size="small" onClick={() => setJumpToLastSignal((n) => n + 1)}>
              Jump to latest
            </Button>
          </Space>
          <CodeEditor
            value={liveLogs || "Waiting for runner output…"}
            language="plaintext"
            readOnly
            height={360}
            autoRevealLastLine={followTail}
            jumpToLastSignal={jumpToLastSignal}
          />
        </div>
      </Card>

      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Runs
        </Typography.Title>
        {runs.length === 0 ? (
          <Empty description="No runs recorded yet" />
        ) : (
          <Table
            size="small"
            rowKey="id"
            columns={runColumns}
            dataSource={runs}
            pagination={{ pageSize: 10, showSizeChanger: false }}
          />
        )}
      </Card>

      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Artifacts
        </Typography.Title>
        {artifacts.length === 0 ? (
          <Empty description="No artifacts available" />
        ) : (
          <Table
            size="small"
            rowKey="name"
            columns={artifactColumns}
            dataSource={artifacts}
            pagination={{ pageSize: 10, showSizeChanger: false }}
          />
        )}
      </Card>
    </Space>
  );
}
