import { useEffect, useMemo, useState } from "react";
import { Alert, Button, Card, Col, Input, List, Row, Space, Tag, Typography } from "antd";
import { api } from "../../api/client";
import type { ArtifactInfo, RunActiveData, RunInfo, RunPreflightData } from "../../api/types";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { CodeEditor } from "../../components/CodeEditor";

type Props = {
  backendConfigPath: string;
  onError: (e: unknown) => void;
};

function parseExtraArgs(raw: string): string[] {
  return raw.trim() ? raw.trim().split(/\s+/) : [];
}

export function RunsSection({ backendConfigPath, onError }: Props) {
  const [runConfigPath, setRunConfigPath] = useState("");
  const [runPassword, setRunPassword] = useState("");
  const [extraArgs, setExtraArgs] = useLocalStorageState("runs.extraArgs", "");
  const [runsOut, setRunsOut] = useState("");
  const [liveLogs, setLiveLogs] = useState("");
  const [runs, setRuns] = useState<RunInfo[]>([]);
  const [artifacts, setArtifacts] = useState<ArtifactInfo[]>([]);
  const [active, setActive] = useState<RunActiveData | null>(null);
  const [preflight, setPreflight] = useState<RunPreflightData | null>(null);

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

  const triggerPayloadPreview = useMemo(
    () => ({
      config_path: runConfigPath || undefined,
      password: runPassword ? "***" : undefined,
      extra_args: parseExtraArgs(extraArgs),
    }),
    [runConfigPath, runPassword, extraArgs],
  );

  const loadRunActive = async () => {
    try {
      const out = await api.runActive();
      setActive(out);
      setRunsOut(JSON.stringify(out, null, 2));
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

  useEffect(() => {
    void loadRunActive();
    const t = setInterval(() => {
      void loadRunActive();
    }, 3000);
    return () => clearInterval(t);
  }, []);

  const triggerRun = async () => {
    try {
      if (active?.active) {
        setRunsOut("Run already active.");
        return;
      }
      const out = await api.runTrigger(triggerPayload);
      setRunsOut(JSON.stringify(out, null, 2));
      await loadRunActive();
    } catch (e) {
      onError(e);
    }
  };

  const runPreflight = async () => {
    try {
      const out = await api.runPreflight({ config_path: runConfigPath || undefined });
      setPreflight(out);
      setRunsOut(JSON.stringify(out, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  const loadRuns = async () => {
    try {
      const out = await api.runs();
      setRuns(out);
    } catch (e) {
      onError(e);
    }
  };

  const loadRunSummary = async () => {
    try {
      const out = await api.runSummary();
      setRunsOut(JSON.stringify(out, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  const loadArtifacts = async () => {
    try {
      const out = await api.artifacts();
      setArtifacts(out);
      setRunsOut(JSON.stringify(out, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  return (
    <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Runs and Artifacts
        </Typography.Title>
        <Space size={[8, 8]} wrap style={{ marginBottom: 12 }}>
          <Button onClick={loadRuns}>List Runs</Button>
          <Button onClick={loadRunSummary}>Load Run Summary</Button>
          <Button onClick={loadArtifacts}>List Artifacts</Button>
          <Button onClick={loadRunActive}>Active Run Status</Button>
          <Button onClick={runPreflight}>Preflight Check</Button>
        </Space>
        <Row gutter={8} style={{ marginBottom: 8 }}>
          <Col xs={24} md={12}>
            <Typography.Text type="secondary">Run config path</Typography.Text>
            <Input value={runConfigPath} onChange={(e) => setRunConfigPath(e.target.value)} />
          </Col>
          <Col xs={24} md={12}>
            <Typography.Text type="secondary">Run password one-off</Typography.Text>
            <Input.Password value={runPassword} onChange={(e) => setRunPassword(e.target.value)} />
          </Col>
        </Row>
        <Space size={8} style={{ width: "100%", marginBottom: 8 }}>
          <Input value={extraArgs} onChange={(e) => setExtraArgs(e.target.value)} placeholder="Trigger extra args" />
          <Button type="primary" onClick={triggerRun}>
            Trigger Run
          </Button>
        </Space>
        {preflight ? (
          <div style={{ marginBottom: 12 }}>
            <Alert
              type={preflight.ok ? "success" : "error"}
              title={preflight.ok ? "Preflight passed" : "Preflight has blocking failures"}
              description={`config: ${preflight.config_path} | failed: ${preflight.failed} | warnings: ${preflight.warn}`}
              showIcon
            />
            <List
              style={{ marginTop: 8 }}
              bordered
              size="small"
              dataSource={preflight.checks}
              renderItem={(item) => (
                <List.Item>
                  <Space direction="vertical" size={2} style={{ width: "100%" }}>
                    <Space size={6} wrap>
                      <Typography.Text strong>{item.title}</Typography.Text>
                      <Tag color={item.status === "pass" ? "green" : item.status === "warn" ? "orange" : "red"}>{item.status}</Tag>
                      <Typography.Text code>{item.id}</Typography.Text>
                    </Space>
                    <Typography.Text>{item.message}</Typography.Text>
                    {item.remediation_code ? (
                      <Typography.Text type="secondary">Remediation code: <Typography.Text code>{item.remediation_code}</Typography.Text></Typography.Text>
                    ) : null}
                    {item.hint ? <Typography.Text type="secondary">Hint: {item.hint}</Typography.Text> : null}
                  </Space>
                </List.Item>
              )}
            />
          </div>
        ) : null}
        <pre>{JSON.stringify(triggerPayloadPreview, null, 2)}</pre>
        <pre>{runsOut}</pre>
        <Typography.Title level={5}>Live Logs</Typography.Title>
        <CodeEditor value={liveLogs} language="plaintext" readOnly height={420} autoRevealLastLine />
        <Typography.Title level={5}>Runs</Typography.Title>
        <ul>
          {runs.map((r) => (
            <li key={r.id}>
              <span className="mono">{r.id}</span> {r.mod_time} {r.has_index ? "index" : ""}
            </li>
          ))}
        </ul>
        <Typography.Title level={5}>Artifacts</Typography.Title>
        <ul>
          {artifacts.map((a) => (
            <li key={a.name}>
              <a href={`/api/v1/artifacts/${encodeURIComponent(a.name)}`}>{a.name}</a> ({a.size} bytes){" "}
              <a href={`/api/v1/artifacts/${encodeURIComponent(a.name)}?download=1`}>download</a>
            </li>
          ))}
        </ul>
    </Card>
  );
}
