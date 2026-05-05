import { useState } from "react";
import { Alert, Button, Card, Space, Tabs, Typography } from "antd";
import { useQuery } from "@tanstack/react-query";
import { ConfigSection } from "../features/settings/ConfigSection";
import { ScheduleSection } from "../features/settings/ScheduleSection";
import { RunsSection } from "../features/runs/RunsSection";
import { LogsSection } from "../features/settings/LogsSection";
import { JsonOutputsSection } from "../features/settings/JsonOutputsSection";
import { RawOutputsSection } from "../features/settings/RawOutputsSection";
import { ApiExplorerSection } from "../features/settings/ApiExplorerSection";
import { api } from "../api/client";
import { useLocalStorageState } from "../hooks/useLocalStorageState";

export function SettingsPage() {
  const [tab, setTab] = useLocalStorageState("settings.activeTab", "connection");
  const [err, setErr] = useState("");
  const health = useQuery({ queryKey: ["health"], queryFn: api.health });
  const report = useQuery({ queryKey: ["report-data"], queryFn: api.reportData });
  const backendConfigPath = health.data?.config_path ?? "";

  return (
    <>
      {err ? <Alert type="error" style={{ marginBottom: 16 }} message={err} /> : null}
      <Tabs
        activeKey={tab}
        onChange={setTab}
        items={[
          {
            key: "connection",
            label: "Connection",
            children: (
              <Card>
                <Typography.Title level={4} className="section-title">
                  Connection + Report Refresh
                </Typography.Title>
                <Typography.Text type="secondary" className="section-subtitle">
                  Backend config path: {backendConfigPath || "unknown"}
                </Typography.Text>
                <Space size={8} style={{ marginTop: 8, marginBottom: 8, display: "flex" }}>
                  <Button onClick={() => void health.refetch()}>Refresh Health</Button>
                  <Button type="primary" onClick={() => void report.refetch()}>
                    Refresh Report Data
                  </Button>
                </Space>
                <pre>{JSON.stringify(health.data ?? {}, null, 2)}</pre>
              </Card>
            ),
          },
          { key: "config", label: "Config", children: <ConfigSection onError={(e) => setErr(String(e))} /> },
          { key: "schedule", label: "Schedule", children: <ScheduleSection backendConfigPath={backendConfigPath} onError={(e) => setErr(String(e))} /> },
          { key: "runs", label: "Runs", children: <RunsSection backendConfigPath={backendConfigPath} onError={(e) => setErr(String(e))} /> },
          { key: "api", label: "API Explorer", children: <ApiExplorerSection onError={(e) => setErr(String(e))} /> },
          { key: "logs", label: "Logs", children: <LogsSection onError={(e) => setErr(String(e))} /> },
          { key: "json", label: "JSON Outputs", children: <JsonOutputsSection onError={(e) => setErr(String(e))} /> },
          { key: "raw", label: "Raw Outputs", children: <RawOutputsSection onError={(e) => setErr(String(e))} /> },
        ]}
      />
    </>
  );
}
