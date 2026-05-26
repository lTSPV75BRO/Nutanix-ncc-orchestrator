import { useEffect, useMemo, useRef, useState } from "react";
import { Alert, Badge, Button, Card, Col, Descriptions, Empty, Input, List, Row, Space, Switch, Table, Tag, Typography } from "antd";
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
import { api } from "../../api/client";
import type { ArtifactInfo, RunActiveData, RunInfo, RunPreflightData } from "../../api/types";
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

  const runColumns: ColumnsType<RunInfo> = [
    { title: "Run ID", dataIndex: "id", key: "id", render: (v: string) => <Typography.Text code>{v}</Typography.Text> },
    { title: "Modified", dataIndex: "mod_time", key: "mod_time", width: 200, render: formatTime },
    {
      title: "Index",
      dataIndex: "has_index",
      key: "has_index",
      width: 100,
      render: (v: boolean) => (v ? <Tag color="success">available</Tag> : <Tag>missing</Tag>),
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
        <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
          <Col xs={24} md={12}>
            <Typography.Text type="secondary">Config file</Typography.Text>
            <Input
              value={runConfigPath}
              onChange={(e) => setRunConfigPath(e.target.value)}
              placeholder={backendConfigPath || "config.yaml"}
            />
          </Col>
          <Col xs={24} md={12}>
            <Typography.Text type="secondary">One-off password (optional)</Typography.Text>
            <Input.Password value={runPassword} onChange={(e) => setRunPassword(e.target.value)} placeholder="overrides config" />
          </Col>
          <Col xs={24}>
            <Typography.Text type="secondary">Additional flags (optional)</Typography.Text>
            <Input
              value={extraArgs}
              onChange={(e) => setExtraArgs(e.target.value)}
              placeholder="e.g. --no-html --output-dir outputfiles"
            />
          </Col>
        </Row>
        <Space size={8} wrap style={{ marginTop: 12 }}>
          <Button type="primary" icon={<ThunderboltOutlined />} onClick={triggerRun} disabled={Boolean(active?.active)}>
            {active?.active ? "Run in progress" : "Trigger Run"}
          </Button>
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
            <Switch checked={followTail} onChange={setFollowTail} size="small" />
            <Typography.Text type="secondary">Follow tail</Typography.Text>
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
