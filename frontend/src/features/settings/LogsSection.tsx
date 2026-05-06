import { useEffect, useState } from "react";
import { Button, Card, Space, Switch, Typography } from "antd";
import { api } from "../../api/client";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { CodeEditor } from "../../components/CodeEditor";

type Props = {
  onError: (e: unknown) => void;
};

export function LogsSection({ onError }: Props) {
  const [content, setContent] = useState("");
  const [path, setPath] = useState("");
  const [auto, setAuto] = useLocalStorageState("settings.logs.autoRefresh", true);

  const load = async () => {
    try {
      const out = await api.runnerLogs();
      setContent(out.content || "");
      setPath(out.path || "");
    } catch (e) {
      onError(e);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  useEffect(() => {
    if (!auto) return;
    const timer = setInterval(() => void load(), 3000);
    return () => clearInterval(timer);
  }, [auto]);

  return (
    <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Runner Logs
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">{path || "logs/ncc-runner.log"}</Typography.Text>
        <Space size={8} style={{ marginTop: 8, marginBottom: 8, display: "flex" }}>
          <Switch checked={auto} onChange={setAuto} />
          <Typography.Text>Auto refresh every 3s</Typography.Text>
          <Button onClick={load}>Refresh</Button>
        </Space>
        <CodeEditor value={content} language="plaintext" readOnly height={420} autoRevealLastLine />
    </Card>
  );
}
