import { useEffect, useRef, useState } from "react";
import { Button, Card, Space, Switch, Typography } from "antd";
import { api } from "../../api/client";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";

type Props = {
  onError: (e: unknown) => void;
};

export function LogsSection({ onError }: Props) {
  const [content, setContent] = useState("");
  const [path, setPath] = useState("");
  const [auto, setAuto] = useLocalStorageState("settings.logs.autoRefresh", true);
  const logRef = useRef<HTMLPreElement | null>(null);

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

  useEffect(() => {
    const el = logRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [content]);

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
        <div>
          <pre ref={logRef} className="live-log">{content}</pre>
        </div>
    </Card>
  );
}
