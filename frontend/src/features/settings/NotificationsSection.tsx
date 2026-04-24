import { useState } from "react";
import { Button, Card, Checkbox, Divider, Input, InputNumber, Space, Switch, Tag, Typography } from "antd";
import { api } from "../../api/client";
import type { NotificationSettings } from "../../api/types";

type Props = {
  onError: (e: unknown) => void;
};

const EMPTY_SETTINGS: NotificationSettings = {
  enabled: true,
  events: {
    run_success: false,
    run_failure: true,
    policy_violations: true,
  },
  slack: {
    enabled: false,
    webhook_url: "",
    channel: "",
    username: "",
  },
  webhook: {
    enabled: false,
    url: "",
  },
  email: {
    enabled: false,
    smtp_host: "",
    smtp_port: 587,
    username: "",
    password: "",
    from: "",
    to: "",
  },
};

export function NotificationsSection({ onError }: Props) {
  const [settings, setSettings] = useState<NotificationSettings>(EMPTY_SETTINGS);
  const [out, setOut] = useState("");
  const [testing, setTesting] = useState<string>("");

  const load = async () => {
    try {
      const resp = await api.loadNotifications();
      setSettings({ ...EMPTY_SETTINGS, ...resp, events: { ...EMPTY_SETTINGS.events, ...resp.events } });
      setOut(JSON.stringify(resp, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  const save = async () => {
    try {
      const resp = await api.saveNotifications(settings);
      setSettings((prev) => ({ ...prev, ...resp, email: { ...prev.email, ...resp.email, password: "" } }));
      setOut(JSON.stringify(resp, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  const sendTest = async (channel: "all" | "slack" | "webhook" | "email") => {
    try {
      setTesting(channel);
      const resp = await api.testNotifications({ channel });
      setSettings((prev) => ({ ...prev, last_delivery: { ...(prev.last_delivery ?? {}), ...(resp.last_delivery ?? {}) } }));
      setOut(JSON.stringify(resp, null, 2));
    } catch (e) {
      onError(e);
    } finally {
      setTesting("");
    }
  };

  const channelStatus = (channel: "slack" | "webhook" | "email") => {
    const item = settings.last_delivery?.[channel];
    if (!item) {
      return <Typography.Text type="secondary">No test/delivery yet.</Typography.Text>;
    }
    return (
      <Space size={8} wrap>
        <Tag color={item.success ? "success" : "error"}>{item.success ? "Success" : "Failed"}</Tag>
        <Typography.Text type="secondary">Last attempt: {item.last_attempt_at || "-"}</Typography.Text>
        <Typography.Text type="secondary">Last success: {item.last_success_at || "-"}</Typography.Text>
        <Typography.Text type="secondary">Event: {item.last_event || "-"}</Typography.Text>
        <Typography.Text type="secondary">OK: {item.total_success ?? 0}</Typography.Text>
        <Typography.Text type="secondary">Fail: {item.total_failure ?? 0}</Typography.Text>
        {item.last_error ? <Typography.Text type="danger">Error: {item.last_error}</Typography.Text> : null}
      </Space>
    );
  };

  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Ops Notifications
      </Typography.Title>
      <Typography.Text type="secondary" className="section-subtitle">
        Optional alerts for run success/failure and policy violations.
      </Typography.Text>
      <Space size={8} style={{ marginTop: 12, marginBottom: 12 }}>
        <Button onClick={load}>Load Notifications</Button>
        <Button type="primary" onClick={save}>
          Save Notifications
        </Button>
        <Button loading={testing === "all"} onClick={() => void sendTest("all")}>
          Send Test (All)
        </Button>
      </Space>
      <div style={{ marginBottom: 12 }}>
        <Space>
          <Typography.Text strong>Enable notifications</Typography.Text>
          <Switch checked={settings.enabled} onChange={(checked) => setSettings((prev) => ({ ...prev, enabled: checked }))} />
        </Space>
      </div>

      <Divider>Events</Divider>
      <Space direction="vertical" size={8} style={{ width: "100%" }}>
        <Checkbox
          checked={settings.events.run_success}
          onChange={(e) =>
            setSettings((prev) => ({ ...prev, events: { ...prev.events, run_success: e.target.checked } }))
          }
        >
          Run success
        </Checkbox>
        <Checkbox
          checked={settings.events.run_failure}
          onChange={(e) =>
            setSettings((prev) => ({ ...prev, events: { ...prev.events, run_failure: e.target.checked } }))
          }
        >
          Run failure
        </Checkbox>
        <Checkbox
          checked={settings.events.policy_violations}
          onChange={(e) =>
            setSettings((prev) => ({ ...prev, events: { ...prev.events, policy_violations: e.target.checked } }))
          }
        >
          Policy violations
        </Checkbox>
      </Space>

      <Divider>Slack</Divider>
      <Space direction="vertical" size={8} style={{ width: "100%" }}>
        <Space>
          <Typography.Text strong>Enable Slack</Typography.Text>
          <Switch
            checked={settings.slack.enabled}
            onChange={(checked) => setSettings((prev) => ({ ...prev, slack: { ...prev.slack, enabled: checked } }))}
          />
        </Space>
        <Input
          placeholder="Slack Incoming Webhook URL"
          value={settings.slack.webhook_url}
          onChange={(e) => setSettings((prev) => ({ ...prev, slack: { ...prev.slack, webhook_url: e.target.value } }))}
        />
        <Input
          placeholder="Channel (optional)"
          value={settings.slack.channel}
          onChange={(e) => setSettings((prev) => ({ ...prev, slack: { ...prev.slack, channel: e.target.value } }))}
        />
        <Input
          placeholder="Username (optional)"
          value={settings.slack.username}
          onChange={(e) => setSettings((prev) => ({ ...prev, slack: { ...prev.slack, username: e.target.value } }))}
        />
        <Button loading={testing === "slack"} onClick={() => void sendTest("slack")}>
          Send Test Slack
        </Button>
        {channelStatus("slack")}
      </Space>

      <Divider>Webhook</Divider>
      <Space direction="vertical" size={8} style={{ width: "100%" }}>
        <Space>
          <Typography.Text strong>Enable webhook</Typography.Text>
          <Switch
            checked={settings.webhook.enabled}
            onChange={(checked) => setSettings((prev) => ({ ...prev, webhook: { ...prev.webhook, enabled: checked } }))}
          />
        </Space>
        <Input
          placeholder="Generic webhook URL"
          value={settings.webhook.url}
          onChange={(e) => setSettings((prev) => ({ ...prev, webhook: { ...prev.webhook, url: e.target.value } }))}
        />
        <Button loading={testing === "webhook"} onClick={() => void sendTest("webhook")}>
          Send Test Webhook
        </Button>
        {channelStatus("webhook")}
      </Space>

      <Divider>Email (SMTP)</Divider>
      <Space direction="vertical" size={8} style={{ width: "100%" }}>
        <Space>
          <Typography.Text strong>Enable email</Typography.Text>
          <Switch
            checked={settings.email.enabled}
            onChange={(checked) => setSettings((prev) => ({ ...prev, email: { ...prev.email, enabled: checked } }))}
          />
        </Space>
        <Input
          placeholder="SMTP Host"
          value={settings.email.smtp_host}
          onChange={(e) => setSettings((prev) => ({ ...prev, email: { ...prev.email, smtp_host: e.target.value } }))}
        />
        <InputNumber
          min={1}
          max={65535}
          style={{ width: 220 }}
          value={settings.email.smtp_port}
          onChange={(value) =>
            setSettings((prev) => ({ ...prev, email: { ...prev.email, smtp_port: Number(value || 587) } }))
          }
        />
        <Input
          placeholder="SMTP Username (optional)"
          value={settings.email.username}
          onChange={(e) => setSettings((prev) => ({ ...prev, email: { ...prev.email, username: e.target.value } }))}
        />
        <Input.Password
          placeholder="SMTP Password (optional; leave blank to keep existing)"
          value={settings.email.password}
          onChange={(e) => setSettings((prev) => ({ ...prev, email: { ...prev.email, password: e.target.value } }))}
        />
        <Input
          placeholder="From Email"
          value={settings.email.from}
          onChange={(e) => setSettings((prev) => ({ ...prev, email: { ...prev.email, from: e.target.value } }))}
        />
        <Input
          placeholder="To Email(s), comma separated"
          value={settings.email.to}
          onChange={(e) => setSettings((prev) => ({ ...prev, email: { ...prev.email, to: e.target.value } }))}
        />
        <Button loading={testing === "email"} onClick={() => void sendTest("email")}>
          Send Test Email
        </Button>
        {channelStatus("email")}
      </Space>
      <div style={{ marginTop: 12 }}>
        <pre>{out}</pre>
      </div>
    </Card>
  );
}
