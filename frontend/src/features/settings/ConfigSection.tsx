import { useState } from "react";
import { Button, Card, Input, Space, Typography } from "antd";
import { api } from "../../api/client";

type Props = {
  onError: (e: unknown) => void;
};

export function ConfigSection({ onError }: Props) {
  const [content, setContent] = useState("");
  const [out, setOut] = useState("");

  const load = async () => {
    try {
      const cfg = await api.loadConfig();
      setContent(cfg.content ?? "");
      setOut(JSON.stringify(cfg, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  const save = async () => {
    try {
      const resp = await api.saveConfig(content);
      setOut(JSON.stringify(resp, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  return (
    <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Application Config
        </Typography.Title>
        <Space size={8} style={{ marginBottom: 12 }}>
          <Button onClick={load}>Load Config</Button>
          <Button type="primary" onClick={save}>
            Save Config
          </Button>
        </Space>
        <Input.TextArea
          value={content}
          onChange={(e) => setContent(e.target.value)}
          rows={16}
        />
        <div style={{ marginTop: 12 }}>
          <pre>{out}</pre>
        </div>
    </Card>
  );
}
