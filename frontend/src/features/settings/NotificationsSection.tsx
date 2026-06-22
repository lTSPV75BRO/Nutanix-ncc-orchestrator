import { useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Divider,
  Form,
  Input,
  InputNumber,
  Select,
  Space,
  Switch,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  BellOutlined,
  ClockCircleOutlined,
  DeleteOutlined,
  PlusOutlined,
  SendOutlined,
  ToolOutlined,
} from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { MaintenanceWindow, NotificationState } from "../../api/types";
import { notify, notifyError } from "../../notify";
import { formatDateTime } from "../../utils/datetime";

const DIGEST_EVERY_OPTIONS = [
  { value: "6h", label: "Every 6 hours" },
  { value: "12h", label: "Every 12 hours" },
  { value: "24h", label: "Daily" },
  { value: "7d", label: "Weekly" },
];

// A representative subset of IANA zones; "Other" lets the user type one.
const TZ_OPTIONS = [
  "UTC",
  "America/Los_Angeles",
  "America/New_York",
  "Europe/London",
  "Europe/Berlin",
  "Asia/Kolkata",
  "Asia/Singapore",
  "Australia/Sydney",
].map((z) => ({ value: z, label: z }));

export function NotificationsSection() {
  const qc = useQueryClient();
  const query = useQuery({ queryKey: ["settings", "notifications"], queryFn: api.getNotifications });
  const [state, setState] = useState<NotificationState>({});
  const [baseline, setBaseline] = useState<NotificationState | null>(null);

  useEffect(() => {
    if (query.data) {
      setState(query.data);
      setBaseline(query.data);
    }
  }, [query.data]);

  const saveMut = useMutation({
    mutationFn: (next: NotificationState) => api.updateNotifications(next),
    onSuccess: () => {
      notify.success("Notification settings saved.");
      void qc.invalidateQueries({ queryKey: ["settings", "notifications"] });
    },
    onError: (e) => notifyError(e, "Failed to save notification settings"),
  });

  const [testing, setTesting] = useState<string | null>(null);
  const isDirty = JSON.stringify(state) !== JSON.stringify(baseline ?? {});
  const channelStatus = (channel: string) => state.last_delivery?.[channel];
  const fmtDelivery = (channel: string) => {
    const d = channelStatus(channel);
    if (!d) return "No delivery history yet.";
    if (d.last_success_at) return `Last success: ${formatDateTime(d.last_success_at)}`;
    if (d.last_attempt_at && d.last_error) return `Last failure: ${formatDateTime(d.last_attempt_at)} (${d.last_error})`;
    return "No delivery history yet.";
  };

  const validHHMM = (v?: string) => !v || /^([01]\d|2[0-3]):([0-5]\d)$/.test(v.trim());
  const validRFC3339 = (v: string) => {
    if (!v.trim()) return false;
    const t = Date.parse(v);
    return Number.isFinite(t);
  };

  const validateState = (next: NotificationState): string | null => {
    if (next.quiet?.enabled) {
      if (!validHHMM(next.quiet.start) || !validHHMM(next.quiet.end)) {
        return "Quiet hours must use HH:MM format (24-hour), e.g. 22:00.";
      }
      if (!next.quiet.timezone?.trim()) {
        return "Quiet-hours timezone is required when quiet hours are enabled.";
      }
    }
    for (const [i, w] of (next.maintenance ?? []).entries()) {
      if (!validRFC3339(w.start) || !validRFC3339(w.end)) {
        return `Maintenance window #${i + 1} must use RFC3339 timestamps.`;
      }
      if (Date.parse(w.start) >= Date.parse(w.end)) {
        return `Maintenance window #${i + 1} start must be before end.`;
      }
    }
    if ((next.throttle?.dedup_window_sec ?? 0) < 0 || (next.throttle?.min_interval_sec ?? 0) < 0) {
      return "Throttle values cannot be negative.";
    }
    return null;
  };

  const canTestSlack = Boolean(state.slack?.enabled && state.slack?.webhook_url?.trim());
  const canTestWebhook = Boolean(state.webhook?.enabled && state.webhook?.url?.trim());
  const canTestEmail = Boolean(state.email?.enabled && state.email?.smtp_host?.trim() && state.email?.to?.trim());

  const sendTest = async (channel: string) => {
    setTesting(channel);
    try {
      await api.testNotification(channel);
      notify.success(`Test notification sent (${channel}).`);
    } catch (e) {
      notifyError(e, "Test notification failed");
    } finally {
      setTesting(null);
    }
  };

  const onSave = () => {
    const err = validateState(state);
    if (err) {
      notify.warning(err);
      return;
    }
    saveMut.mutate(state);
  };

  // Shallow-merge helpers keep the controlled-state updates terse.
  const patch = (p: Partial<NotificationState>) => setState((s) => ({ ...s, ...p }));
  const patchEvents = (p: Partial<NonNullable<NotificationState["events"]>>) =>
    setState((s) => ({ ...s, events: { ...s.events, ...p } }));
  const patchSlack = (p: Partial<NonNullable<NotificationState["slack"]>>) =>
    setState((s) => ({ ...s, slack: { ...s.slack, ...p } }));
  const patchWebhook = (p: Partial<NonNullable<NotificationState["webhook"]>>) =>
    setState((s) => ({ ...s, webhook: { ...s.webhook, ...p } }));
  const patchEmail = (p: Partial<NonNullable<NotificationState["email"]>>) =>
    setState((s) => ({ ...s, email: { ...s.email, ...p } }));
  const patchQuiet = (p: Partial<NonNullable<NotificationState["quiet"]>>) =>
    setState((s) => ({ ...s, quiet: { ...s.quiet, ...p } }));
  const patchThrottle = (p: Partial<NonNullable<NotificationState["throttle"]>>) =>
    setState((s) => ({ ...s, throttle: { ...s.throttle, ...p } }));
  const patchDigest = (p: Partial<NonNullable<NotificationState["digest"]>>) =>
    setState((s) => ({ ...s, digest: { ...s.digest, ...p } }));

  const windows = state.maintenance ?? [];
  const setWindow = (i: number, p: Partial<MaintenanceWindow>) =>
    setState((s) => {
      const next = [...(s.maintenance ?? [])];
      next[i] = { ...next[i], ...p };
      return { ...s, maintenance: next };
    });
  const addWindow = () =>
    setState((s) => ({ ...s, maintenance: [...(s.maintenance ?? []), { start: "", end: "", note: "" }] }));
  const removeWindow = (i: number) =>
    setState((s) => ({ ...s, maintenance: (s.maintenance ?? []).filter((_, j) => j !== i) }));

  if (query.isLoading) {
    return (
      <Card className="page-card" loading title="Notifications" />
    );
  }

  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      <Card
        className="page-card"
        title={
          <Space>
            <BellOutlined />
            Notifications
          </Space>
        }
        extra={
          <Space>
            <Button
              disabled={!isDirty || saveMut.isPending}
              onClick={() => {
                if (baseline) setState(baseline);
              }}
            >
              Reset
            </Button>
            <Button type="primary" loading={saveMut.isPending} disabled={!isDirty} onClick={onSave}>
              Save
            </Button>
          </Space>
        }
      >
        <Typography.Paragraph type="secondary" style={{ marginTop: -4 }}>
          Alerts for run failures, backup failures, and self-heal events, plus a scheduled health
          digest — delivered to Slack, a generic webhook, and/or email. Failures reuse the{" "}
          <b>run failure</b> toggle.
        </Typography.Paragraph>

        <Form layout="vertical">
          <Form.Item label="Enable notifications" style={{ marginBottom: 12 }}>
            <Switch checked={Boolean(state.enabled)} onChange={(v) => patch({ enabled: v })} />
          </Form.Item>
          <Space size={24} wrap>
            <Form.Item label="On run failure" style={{ marginBottom: 8 }}>
              <Switch
                checked={Boolean(state.events?.run_failure)}
                onChange={(v) => patchEvents({ run_failure: v })}
              />
              <Tooltip title="Also covers backup failures and self-heal failing checks.">
                <Typography.Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>
                  incl. backup / self-heal
                </Typography.Text>
              </Tooltip>
            </Form.Item>
            <Form.Item label="On run success" style={{ marginBottom: 8 }}>
              <Switch
                checked={Boolean(state.events?.run_success)}
                onChange={(v) => patchEvents({ run_success: v })}
              />
            </Form.Item>
            <Form.Item label="On policy violations" style={{ marginBottom: 8 }}>
              <Switch
                checked={Boolean(state.events?.policy_violations)}
                onChange={(v) => patchEvents({ policy_violations: v })}
              />
            </Form.Item>
          </Space>
        </Form>
        {isDirty ? (
          <Alert
            style={{ marginTop: 12 }}
            type="warning"
            showIcon
            message="Unsaved changes"
            description="Save to apply notification changes."
          />
        ) : null}
      </Card>

      {/* Channels */}
      <Card className="page-card" title="Channels">
        <Form layout="vertical">
          <Divider titlePlacement="start" style={{ marginTop: 0 }}>
            Slack
            <Button
              size="small"
              icon={<SendOutlined />}
              style={{ marginLeft: 12 }}
              loading={testing === "slack"}
              disabled={!canTestSlack}
              onClick={() => sendTest("slack")}
            >
              Test
            </Button>
          </Divider>
          <Space size={16} wrap align="end">
            <Form.Item label="Enabled" style={{ marginBottom: 8 }}>
              <Switch checked={Boolean(state.slack?.enabled)} onChange={(v) => patchSlack({ enabled: v })} />
            </Form.Item>
            <Form.Item label="Webhook URL" style={{ marginBottom: 8 }}>
              <Input.Password
                style={{ width: 360 }}
                placeholder="https://hooks.slack.com/services/…"
                value={state.slack?.webhook_url ?? ""}
                onChange={(e) => patchSlack({ webhook_url: e.target.value })}
              />
            </Form.Item>
            <Form.Item label="Channel" style={{ marginBottom: 8 }}>
              <Input
                style={{ width: 160 }}
                placeholder="#sre-alerts"
                value={state.slack?.channel ?? ""}
                onChange={(e) => patchSlack({ channel: e.target.value })}
              />
            </Form.Item>
          </Space>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            {fmtDelivery("slack")}
          </Typography.Text>

          <Divider titlePlacement="start">
            Webhook
            <Button
              size="small"
              icon={<SendOutlined />}
              style={{ marginLeft: 12 }}
              loading={testing === "webhook"}
              disabled={!canTestWebhook}
              onClick={() => sendTest("webhook")}
            >
              Test
            </Button>
          </Divider>
          <Space size={16} wrap align="end">
            <Form.Item label="Enabled" style={{ marginBottom: 8 }}>
              <Switch checked={Boolean(state.webhook?.enabled)} onChange={(v) => patchWebhook({ enabled: v })} />
            </Form.Item>
            <Form.Item label="URL" style={{ marginBottom: 8 }}>
              <Input
                style={{ width: 420 }}
                placeholder="https://example.com/ncc-events"
                value={state.webhook?.url ?? ""}
                onChange={(e) => patchWebhook({ url: e.target.value })}
              />
            </Form.Item>
          </Space>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            {fmtDelivery("webhook")}
          </Typography.Text>

          <Divider titlePlacement="start">
            Email
            <Button
              size="small"
              icon={<SendOutlined />}
              style={{ marginLeft: 12 }}
              loading={testing === "email"}
              disabled={!canTestEmail}
              onClick={() => sendTest("email")}
            >
              Test
            </Button>
          </Divider>
          <Space size={16} wrap align="end">
            <Form.Item label="Enabled" style={{ marginBottom: 8 }}>
              <Switch checked={Boolean(state.email?.enabled)} onChange={(v) => patchEmail({ enabled: v })} />
            </Form.Item>
            <Form.Item label="SMTP host" style={{ marginBottom: 8 }}>
              <Input
                style={{ width: 220 }}
                placeholder="smtp.example.com"
                value={state.email?.smtp_host ?? ""}
                onChange={(e) => patchEmail({ smtp_host: e.target.value })}
              />
            </Form.Item>
            <Form.Item label="Port" style={{ marginBottom: 8 }}>
              <InputNumber
                min={1}
                max={65535}
                value={state.email?.smtp_port ?? 587}
                onChange={(v) => patchEmail({ smtp_port: typeof v === "number" ? v : undefined })}
              />
            </Form.Item>
          </Space>
          <Space size={16} wrap align="end">
            <Form.Item label="Username" style={{ marginBottom: 8 }}>
              <Input
                style={{ width: 220 }}
                autoComplete="off"
                value={state.email?.username ?? ""}
                onChange={(e) => patchEmail({ username: e.target.value })}
              />
            </Form.Item>
            <Form.Item label="Password" style={{ marginBottom: 8 }}>
              <Input.Password
                style={{ width: 220 }}
                placeholder="(unchanged)"
                value={state.email?.password ?? ""}
                onChange={(e) => patchEmail({ password: e.target.value })}
              />
            </Form.Item>
            <Form.Item label="From" style={{ marginBottom: 8 }}>
              <Input
                style={{ width: 220 }}
                placeholder="ncc@example.com"
                value={state.email?.from ?? ""}
                onChange={(e) => patchEmail({ from: e.target.value })}
              />
            </Form.Item>
            <Form.Item label="To" style={{ marginBottom: 8 }}>
              <Input
                style={{ width: 260 }}
                placeholder="sre@example.com"
                value={state.email?.to ?? ""}
                onChange={(e) => patchEmail({ to: e.target.value })}
              />
            </Form.Item>
          </Space>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            Leave the email password blank to keep the stored one. It is never returned to the
            browser.
          </Typography.Text>
          <br />
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            {fmtDelivery("email")}
          </Typography.Text>
        </Form>
      </Card>

      {/* Delivery controls */}
      <Card
        className="page-card"
        title={
          <Space>
            <ClockCircleOutlined />
            Delivery controls
          </Space>
        }
      >
        <Typography.Paragraph type="secondary" style={{ marginTop: -4 }}>
          Quiet hours and throttling prevent alert fatigue; maintenance windows mute everything
          during planned work.
        </Typography.Paragraph>
        <Form layout="vertical">
          <Divider titlePlacement="start" style={{ marginTop: 0 }}>
            Quiet hours
          </Divider>
          <Space size={8} wrap style={{ marginBottom: 8 }}>
            <Typography.Text type="secondary">Quick presets:</Typography.Text>
            <Button size="small" onClick={() => patchQuiet({ enabled: true, start: "22:00", end: "07:00" })}>Night (22:00-07:00)</Button>
            <Button size="small" onClick={() => patchQuiet({ enabled: true, start: "00:00", end: "06:00" })}>Midnight (00:00-06:00)</Button>
          </Space>
          <Space size={16} wrap align="end">
            <Form.Item label="Enabled" style={{ marginBottom: 8 }}>
              <Switch checked={Boolean(state.quiet?.enabled)} onChange={(v) => patchQuiet({ enabled: v })} />
            </Form.Item>
            <Form.Item label="Start (HH:MM)" style={{ marginBottom: 8 }}>
              <Input
                style={{ width: 110 }}
                placeholder="22:00"
                disabled={!state.quiet?.enabled}
                value={state.quiet?.start ?? ""}
                onChange={(e) => patchQuiet({ start: e.target.value })}
              />
            </Form.Item>
            <Form.Item label="End (HH:MM)" style={{ marginBottom: 8 }}>
              <Input
                style={{ width: 110 }}
                placeholder="07:00"
                disabled={!state.quiet?.enabled}
                value={state.quiet?.end ?? ""}
                onChange={(e) => patchQuiet({ end: e.target.value })}
              />
            </Form.Item>
            <Form.Item label="Timezone" style={{ marginBottom: 8 }}>
              <Select
                style={{ width: 200 }}
                showSearch
                disabled={!state.quiet?.enabled}
                value={state.quiet?.timezone || "UTC"}
                options={TZ_OPTIONS}
                onChange={(v) => patchQuiet({ timezone: v })}
              />
            </Form.Item>
            <Form.Item label="Failures bypass" style={{ marginBottom: 8 }}>
              <Tooltip title="When on, failure alerts are still delivered during quiet hours.">
                <Switch
                  checked={Boolean(state.quiet?.allow_failures)}
                  disabled={!state.quiet?.enabled}
                  onChange={(v) => patchQuiet({ allow_failures: v })}
                />
              </Tooltip>
            </Form.Item>
          </Space>

          <Divider titlePlacement="start">Throttle</Divider>
          <Space size={16} wrap align="end">
            <Form.Item
              label="Dedup window (sec)"
              style={{ marginBottom: 8 }}
              tooltip="Collapse repeats of the same event within this many seconds. 0 = off."
            >
              <InputNumber
                min={0}
                value={state.throttle?.dedup_window_sec ?? 0}
                onChange={(v) => patchThrottle({ dedup_window_sec: typeof v === "number" ? v : 0 })}
              />
            </Form.Item>
            <Form.Item
              label="Min interval (sec)"
              style={{ marginBottom: 8 }}
              tooltip="Minimum spacing between any two notifications. 0 = off."
            >
              <InputNumber
                min={0}
                value={state.throttle?.min_interval_sec ?? 0}
                onChange={(v) => patchThrottle({ min_interval_sec: typeof v === "number" ? v : 0 })}
              />
            </Form.Item>
          </Space>

          <Divider titlePlacement="start">
            <Space>
              <ToolOutlined />
              Maintenance windows
            </Space>
          </Divider>
          <Typography.Paragraph type="secondary" style={{ fontSize: 12, marginBottom: 8 }}>
            During an active window, all notifications (and the scheduled digest) are muted. Use
            RFC3339 timestamps, e.g. <code>2026-07-01T01:00:00Z</code>.
          </Typography.Paragraph>
          {windows.length === 0 ? (
            <Typography.Text type="secondary">No maintenance windows.</Typography.Text>
          ) : (
            <Space direction="vertical" size={8} style={{ width: "100%" }}>
              {windows.map((w, i) => (
                <Space key={i} size={8} wrap align="center">
                  <Input
                    style={{ width: 210 }}
                    placeholder="start (RFC3339)"
                    value={w.start}
                    onChange={(e) => setWindow(i, { start: e.target.value })}
                  />
                  <span>→</span>
                  <Input
                    style={{ width: 210 }}
                    placeholder="end (RFC3339)"
                    value={w.end}
                    onChange={(e) => setWindow(i, { end: e.target.value })}
                  />
                  <Input
                    style={{ width: 200 }}
                    placeholder="note (optional)"
                    value={w.note ?? ""}
                    onChange={(e) => setWindow(i, { note: e.target.value })}
                  />
                  <Button danger size="small" icon={<DeleteOutlined />} onClick={() => removeWindow(i)} />
                </Space>
              ))}
            </Space>
          )}
          <div style={{ marginTop: 8 }}>
            <Button size="small" icon={<PlusOutlined />} onClick={addWindow}>
              Add window
            </Button>
          </div>
        </Form>
      </Card>

      {/* Digest */}
      <Card className="page-card" title="Scheduled health digest">
        <Typography.Paragraph type="secondary" style={{ marginTop: -4 }}>
          A recurring summary of the latest run (cluster ok/failed counts, FAIL/WARN totals, top
          policy violations), delivered over the channels above.
        </Typography.Paragraph>
        <Form layout="vertical">
          <Space size={16} wrap align="end">
            <Form.Item label="Enabled" style={{ marginBottom: 8 }}>
              <Switch checked={Boolean(state.digest?.enabled)} onChange={(v) => patchDigest({ enabled: v })} />
            </Form.Item>
            <Form.Item label="Frequency" style={{ marginBottom: 8 }}>
              <Select
                style={{ width: 200 }}
                disabled={!state.digest?.enabled}
                value={state.digest?.every || "24h"}
                options={DIGEST_EVERY_OPTIONS}
                onChange={(v) => patchDigest({ every: v })}
              />
            </Form.Item>
          </Space>
          {state.digest?.last_sent_at ? (
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              Last digest sent: {formatDateTime(state.digest.last_sent_at)}
            </Typography.Text>
          ) : null}
        </Form>
      </Card>

      {!state.enabled ? (
        <Alert
          type="info"
          showIcon
          message="Notifications are disabled"
          description="Enable notifications above for channels, alerts, and the digest to be delivered."
        />
      ) : null}
      {state.updated_at ? (
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          <Tag>saved</Tag> last updated {formatDateTime(state.updated_at)}
        </Typography.Text>
      ) : null}
    </Space>
  );
}
