import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import {
  Alert,
  Button,
  Card,
  Collapse,
  DatePicker,
  Form,
  Input,
  InputNumber,
  Segmented,
  Select,
  Space,
  Switch,
  Tag,
  TimePicker,
  Tooltip,
  Typography,
} from "antd";
import { ApiOutlined, BellOutlined, CheckCircleFilled, ClockCircleOutlined, CloseCircleFilled, DeleteOutlined, FilterOutlined, MailOutlined, PlusOutlined, SendOutlined, SlackOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import dayjs, { type Dayjs } from "dayjs";
import { api } from "../../api/client";
import type { NotificationState } from "../../api/types";
import { notify, notifyError } from "../../notify";
import { formatDateTime, relativeTime } from "../../utils/datetime";
import { IconTitle, SaveBar, StatusBadge } from "../../ui";
import {
  alertSummary,
  channelErrors,
  deliveryView,
  DIGEST_EVERY_OPTIONS,
  digestLabel,
  enabledChannelCount,
  validateNotifications,
  withDefaults,
  type ChannelKey,
} from "./notificationsModel";

const BROWSER_TZ = (() => {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    return "UTC";
  }
})();

const TZ_OPTIONS = Array.from(
  new Set([
    "UTC",
    BROWSER_TZ,
    "America/Los_Angeles",
    "America/New_York",
    "Europe/London",
    "Europe/Berlin",
    "Asia/Kolkata",
    "Asia/Singapore",
    "Australia/Sydney",
  ]),
).map((z) => ({ value: z, label: z === BROWSER_TZ && z !== "UTC" ? `${z} (this browser)` : z }));

const toTime = (hhmm?: string): Dayjs | null => {
  const m = /^(\d{2}):(\d{2})$/.exec(hhmm ?? "");
  return m ? dayjs().hour(Number(m[1])).minute(Number(m[2])).second(0) : null;
};
// UTC RFC 3339 without milliseconds, as the server stores and matches it.
const toRFC3339 = (d: Dayjs) => d.second(0).millisecond(0).toISOString().replace(/\.\d{3}Z$/, "Z");

/** A toggle row: label and help on the left, the switch on the right. */
function ToggleRow({
  label,
  help,
  checked,
  disabled,
  onChange,
  children,
}: {
  label: ReactNode;
  help?: ReactNode;
  checked: boolean;
  disabled?: boolean;
  onChange: (v: boolean) => void;
  children?: ReactNode;
}) {
  return (
    <div className="notif-toggle">
      <div className="notif-toggle-text">
        <span className="notif-toggle-label">{label}</span>
        {help ? <span className="notif-toggle-help">{help}</span> : null}
        {children}
      </div>
      <Switch checked={checked} disabled={disabled} onChange={onChange} aria-label={typeof label === "string" ? label : undefined} />
    </div>
  );
}

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
    onError: (e) => notifyError(e, "Couldn't save notification settings"),
  });

  const testMut = useMutation({
    mutationFn: ({ channel, kind }: { channel: string; kind?: "test" | "digest" }) => api.testNotification(channel, kind),
    onSuccess: (_d, { channel, kind }) => {
      const where = channel === "all" ? "every channel that is on" : channel === "email" ? "email" : channel === "slack" ? "Slack" : "the webhook";
      notify.success(kind === "digest" ? `Health digest sent to ${where}.` : `Test message sent to ${where}.`);
    },
    onError: (e) => notifyError(e, "Sending failed"),
    // Refresh the delivery status either way.
    onSettled: () => void qc.invalidateQueries({ queryKey: ["settings", "notifications"] }),
  });

  const isDirty = JSON.stringify(state) !== JSON.stringify(baseline ?? {});
  const formError = useMemo(() => validateNotifications(withDefaults(state)), [state]);
  const savedChannels = baseline ? enabledChannelCount(baseline) : 0;

  const onSave = () => {
    const next = withDefaults(state);
    const err = validateNotifications(next);
    if (err) {
      notify.warning(err);
      return;
    }
    saveMut.mutate(next);
  };
  const discard = () => {
    if (baseline) setState(baseline);
  };

  const patch = (p: Partial<NotificationState>) => setState((s) => ({ ...s, ...p }));
  const patchIn = <K extends keyof NotificationState>(key: K, p: Partial<NonNullable<NotificationState[K]>>) =>
    setState((s) => ({ ...s, [key]: { ...(s[key] as object | undefined), ...p } }));

  // Tests use the saved settings, so they wait for a save.
  const testBlockedReason = (channel: ChannelKey | "all"): string | null => {
    if (isDirty) return "Save your changes first: tests use the saved settings.";
    if (channel === "all") return savedChannels === 0 ? "Turn on a channel first." : null;
    return baseline?.[channel]?.enabled ? null : "Turn this channel on and save first.";
  };
  const testButton = (channel: ChannelKey | "all", label = "Send test", kind: "test" | "digest" = "test") => {
    const reason = testBlockedReason(channel);
    const pending = testMut.isPending && testMut.variables?.channel === channel && (testMut.variables?.kind ?? "test") === kind;
    return (
      <Tooltip title={reason}>
        <Button
          icon={<SendOutlined />}
          disabled={Boolean(reason) || testMut.isPending}
          loading={pending}
          onClick={() => testMut.mutate({ channel, kind })}
        >
          {label}
        </Button>
      </Tooltip>
    );
  };

  if (query.isLoading) return <Card className="page-card" loading title={<IconTitle icon={<BellOutlined />}>Notifications</IconTitle>} />;
  if (query.isError) {
    return <Alert type="error" showIcon title="Couldn't load notification settings" description={String(query.error)} />;
  }

  const channelCard = (
    key: ChannelKey,
    title: string,
    icon: ReactNode,
    blurb: string,
    fields: (errors: Record<string, string>) => ReactNode,
  ) => {
    const on = Boolean(state[key]?.enabled);
    const errors = channelErrors(state, key);
    const delivery = deliveryView(state.last_delivery?.[key]);
    return (
      <section className={`notif-channel${on ? " is-on" : ""}`} aria-label={title}>
        <header className="notif-channel-head">
          <span className="notif-channel-icon" aria-hidden="true">
            {icon}
          </span>
          <div className="notif-channel-title">
            <span className="notif-channel-name">{title}</span>
            <span className="notif-channel-blurb">{blurb}</span>
          </div>
          <Switch
            checked={on}
            onChange={(v) => patchIn(key, { enabled: v })}
            aria-label={`Send to ${title}`}
          />
        </header>
        {on ? (
          <>
            <Form layout="vertical" className="notif-channel-form" component="div">
              {fields(errors)}
            </Form>
            <footer className="notif-channel-foot">
              <span className={`notif-delivery is-${delivery.tone}`}>
                {delivery.tone === "ok" ? <CheckCircleFilled /> : delivery.tone === "fail" ? <CloseCircleFilled /> : null}
                {delivery.text}
                {delivery.at ? (
                  <Tooltip title={formatDateTime(delivery.at)}>
                    <span> {relativeTime(delivery.at)}</span>
                  </Tooltip>
                ) : null}
                {delivery.error ? <span className="notif-delivery-error">— {delivery.error}</span> : null}
              </span>
              {testButton(key)}
            </footer>
          </>
        ) : null}
      </section>
    );
  };

  const field = (label: string, error: string | undefined, input: ReactNode, extra?: ReactNode) => (
    <Form.Item label={label} validateStatus={error ? "error" : undefined} help={error} extra={extra}>
      {input}
    </Form.Item>
  );

  const windows = state.maintenance ?? [];
  const setWindow = (i: number, p: Partial<NonNullable<NotificationState["maintenance"]>[number]>) =>
    setState((s) => {
      const next = [...(s.maintenance ?? [])];
      next[i] = { ...next[i], ...p };
      return { ...s, maintenance: next };
    });
  const now = Date.now();
  const activeWindow = (baseline?.maintenance ?? []).find((w) => Date.parse(w.start) <= now && now < Date.parse(w.end));

  const masterOn = Boolean(state.enabled);
  const channelsOn = enabledChannelCount(state);

  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }} className="notif-page">
      <div className="overview-runline" aria-label="Alert status">
        <div className="overview-runline-text">
          <StatusBadge tone={!masterOn ? "neutral" : channelsOn === 0 ? "warn" : "ok"}>
            {!masterOn ? "Paused" : channelsOn === 0 ? "No channel" : "Sending"}
          </StatusBadge>
          <span>
            {!masterOn
              ? "Nothing is sent from this server. Run reports from the runner still go out."
              : channelsOn === 0
                ? "Turn on a channel below to start sending."
                : `Sending to ${channelsOn} channel${channelsOn === 1 ? "" : "s"}`}
          </span>
          {masterOn && channelsOn > 0 ? (
            <Typography.Text type="secondary">
              {`${alertSummary(state)}${state.digest?.enabled ? ` · ${digestLabel(state.digest.every).toLowerCase()} digest` : ""}${state.quiet?.enabled ? ` · quiet ${state.quiet.start}–${state.quiet.end}` : ""}`}
            </Typography.Text>
          ) : null}
        </div>
        <div className="overview-runline-actions">
          {testButton("all", "Test all channels")}
          <Switch
            checked={masterOn}
            onChange={(v) => patch({ enabled: v })}
            checkedChildren="On"
            unCheckedChildren="Paused"
            aria-label="Send alerts"
          />
        </div>
      </div>
      {activeWindow ? (
        <Alert
          type="warning"
          showIcon
          title={`Muted by a maintenance window until ${formatDateTime(activeWindow.end)}`}
          description={activeWindow.note || undefined}
        />
      ) : null}

      <Card className="page-card" title={<IconTitle icon={<SendOutlined />}>Where to send</IconTitle>}>
        <div className="notif-channels">
          {channelCard("email", "Email", <MailOutlined />, "Through your SMTP server", (e) => (
            <div className="notif-grid">
              {field("Mail server", e.smtp_host, (
                <Input placeholder="smtp.example.com" value={state.email?.smtp_host ?? ""} onChange={(ev) => patchIn("email", { smtp_host: ev.target.value })} />
              ))}
              {field("Port", e.smtp_port, (
                <InputNumber
                  min={1}
                  max={65535}
                  style={{ width: "100%" }}
                  value={state.email?.smtp_port ?? 587}
                  onChange={(v) => patchIn("email", { smtp_port: typeof v === "number" ? v : undefined })}
                />
              ), "587 uses STARTTLS, 465 uses TLS.")}
              {field("Sender", e.from, (
                <Input placeholder="ncc@example.com" value={state.email?.from ?? ""} onChange={(ev) => patchIn("email", { from: ev.target.value })} />
              ))}
              {field("Recipients", e.to, (
                <Input placeholder="sre@example.com, oncall@example.com" value={state.email?.to ?? ""} onChange={(ev) => patchIn("email", { to: ev.target.value })} />
              ))}
              {field("Username", undefined, (
                <Input autoComplete="off" placeholder="Leave empty for no sign-in" value={state.email?.username ?? ""} onChange={(ev) => patchIn("email", { username: ev.target.value })} />
              ))}
              {field("Password", undefined, (
                <Input.Password
                  autoComplete="new-password"
                  placeholder="Unchanged"
                  value={state.email?.password ?? ""}
                  onChange={(ev) => patchIn("email", { password: ev.target.value })}
                />
              ), "Stored on the server and never shown again.")}
            </div>
          ))}
          {channelCard("slack", "Slack", <SlackOutlined />, "Posts to a channel through an incoming webhook", (e) => (
            <div className="notif-grid">
              {field("Webhook URL", e.webhook_url, (
                <Input.Password
                  placeholder="https://hooks.slack.com/services/…"
                  value={state.slack?.webhook_url ?? ""}
                  onChange={(ev) => patchIn("slack", { webhook_url: ev.target.value })}
                />
              ), "Treat it like a password: anyone with it can post.")}
              {field("Channel", undefined, (
                <Input placeholder="#sre-alerts" value={state.slack?.channel ?? ""} onChange={(ev) => patchIn("slack", { channel: ev.target.value })} />
              ), "Optional. Defaults to the webhook's channel.")}
            </div>
          ))}
          {channelCard("webhook", "Webhook", <ApiOutlined />, "Posts JSON events to your own endpoint", (e) => (
            <div className="notif-grid notif-grid-wide">
              {field("URL", e.url, (
                <Input placeholder="https://example.com/ncc-events" value={state.webhook?.url ?? ""} onChange={(ev) => patchIn("webhook", { url: ev.target.value })} />
              ), (
                <>
                  Requests are signed with <code>X-NCC-Signature</code> when <code>webhook-secret</code> is set in the config file.
                </>
              ))}
            </div>
          ))}
        </div>
      </Card>

      <Card className="page-card" title={<IconTitle icon={<FilterOutlined />}>What to send</IconTitle>}>
        <div className="notif-groups">
          <div className="notif-group">
            <Typography.Title level={5} className="notif-group-title">Alerts</Typography.Title>
            <Typography.Text type="secondary" className="notif-group-desc">
              Sent by this server when something happens.
            </Typography.Text>
            <ToggleRow
              label="Run failures"
              help="Also failed backups and failing automatic repairs."
              checked={Boolean(state.events?.run_failure)}
              disabled={!masterOn}
              onChange={(v) => patchIn("events", { run_failure: v })}
            />
            <ToggleRow
              label="Policy violations"
              help="When a run breaks a policy gate."
              checked={Boolean(state.events?.policy_violations)}
              disabled={!masterOn}
              onChange={(v) => patchIn("events", { policy_violations: v })}
            />
            <ToggleRow
              label="Successful runs"
              help="A message after every run that finishes. Can be noisy."
              checked={Boolean(state.events?.run_success)}
              disabled={!masterOn}
              onChange={(v) => patchIn("events", { run_success: v })}
            />
          </div>

          <div className="notif-group">
            <Typography.Title level={5} className="notif-group-title">Health digest</Typography.Title>
            <Typography.Text type="secondary" className="notif-group-desc">
              A summary of the latest run: clusters, FAIL and WARN totals, top policy violations.
            </Typography.Text>
            <ToggleRow
              label="Send a digest"
              checked={Boolean(state.digest?.enabled)}
              disabled={!masterOn}
              onChange={(v) => patchIn("digest", { enabled: v, every: state.digest?.every || "24h" })}
            >
              {state.digest?.last_sent_at ? (
                <span className="notif-toggle-help">Last sent {relativeTime(state.digest.last_sent_at)}</span>
              ) : null}
            </ToggleRow>
            <div className="notif-inline">
              <Select
                aria-label="Digest frequency"
                style={{ width: 180 }}
                disabled={!masterOn || !state.digest?.enabled}
                value={state.digest?.every || "24h"}
                options={DIGEST_EVERY_OPTIONS}
                onChange={(v) => patchIn("digest", { every: v })}
              />
              {testButton("all", "Send digest now", "digest")}
            </div>
          </div>

          <div className="notif-group">
            <Typography.Title level={5} className="notif-group-title">Run reports</Typography.Title>
            <Typography.Text type="secondary" className="notif-group-desc">
              Sent by the runner itself after each run, to every channel that is on (pausing alerts does not stop them).
            </Typography.Text>
            <div className="notif-inline">
              <Segmented
                aria-label="Run report grouping"
                value={state.run_reports?.per_run ? "run" : "cluster"}
                options={[
                  { value: "cluster", label: "One per cluster" },
                  { value: "run", label: "One per run" },
                ]}
                onChange={(v) => patchIn("run_reports", { per_run: v === "run" })}
              />
            </div>
            <ToggleRow
              label="Only when failures increase"
              help="Skip a cluster's report unless it has more FAILs than last time."
              checked={Boolean(state.run_reports?.only_on_regression)}
              onChange={(v) => patchIn("run_reports", { only_on_regression: v })}
            />
          </div>
        </div>
      </Card>

      <Card className="page-card" title={<IconTitle icon={<ClockCircleOutlined />}>Quiet times</IconTitle>}>
        <div className="notif-groups">
          <div className="notif-group">
            <Typography.Title level={5} className="notif-group-title">Quiet hours</Typography.Title>
            <Typography.Text type="secondary" className="notif-group-desc">Hold alerts every day during these hours.</Typography.Text>
            <ToggleRow
              label="Use quiet hours"
              checked={Boolean(state.quiet?.enabled)}
              onChange={(v) =>
                patchIn("quiet", {
                  enabled: v,
                  start: state.quiet?.start || "22:00",
                  end: state.quiet?.end || "07:00",
                  timezone: state.quiet?.timezone || BROWSER_TZ,
                })
              }
            />
            <div className="notif-inline">
              <TimePicker.RangePicker
                aria-label="Quiet hours"
                format="HH:mm"
                minuteStep={5}
                order={false}
                allowClear={false}
                disabled={!state.quiet?.enabled}
                allowEmpty={state.quiet?.enabled ? undefined : [true, true]}
                value={[toTime(state.quiet?.start), toTime(state.quiet?.end)]}
                onChange={(v) => {
                  if (v?.[0] && v?.[1]) patchIn("quiet", { start: v[0].format("HH:mm"), end: v[1].format("HH:mm") });
                }}
              />
              <Select
                aria-label="Quiet hours time zone"
                style={{ width: 230 }}
                showSearch
                disabled={!state.quiet?.enabled}
                value={state.quiet?.timezone || "UTC"}
                options={TZ_OPTIONS}
                onChange={(v) => patchIn("quiet", { timezone: v })}
              />
            </div>
            {state.quiet?.enabled ? (
              <Typography.Text type="secondary" className="notif-toggle-help">
                Run reports from the runner use the server's own clock, not this time zone.
              </Typography.Text>
            ) : null}
            <ToggleRow
              label="Let failures through"
              help="Failure alerts are still sent during quiet hours."
              checked={Boolean(state.quiet?.allow_failures)}
              disabled={!state.quiet?.enabled}
              onChange={(v) => patchIn("quiet", { allow_failures: v })}
            />
          </div>

          <div className="notif-group">
            <Typography.Title level={5} className="notif-group-title">Maintenance windows</Typography.Title>
            <Typography.Text type="secondary" className="notif-group-desc">
              Mute every alert and the digest during planned work. Times are in this browser's time zone.
            </Typography.Text>
            {windows.length === 0 ? <Typography.Text type="secondary">No maintenance windows.</Typography.Text> : null}
            {windows.map((w, i) => {
              const end = Date.parse(w.end);
              const start = Date.parse(w.start);
              const status = !Number.isFinite(end) ? null : end <= now ? "Ended" : start <= now ? "Active" : "Upcoming";
              return (
                <div className="notif-window" key={i}>
                  <DatePicker.RangePicker
                    aria-label={`Maintenance window ${i + 1}`}
                    showTime={{ format: "HH:mm", minuteStep: 5 }}
                    format="YYYY-MM-DD HH:mm"
                    value={[w.start ? dayjs(w.start) : null, w.end ? dayjs(w.end) : null]}
                    onChange={(v) => setWindow(i, { start: v?.[0] ? toRFC3339(v[0]) : "", end: v?.[1] ? toRFC3339(v[1]) : "" })}
                  />
                  <Input
                    aria-label={`Maintenance window ${i + 1} note`}
                    placeholder="Note, e.g. AOS upgrade"
                    value={w.note ?? ""}
                    onChange={(e) => setWindow(i, { note: e.target.value })}
                  />
                  {status ? <Tag color={status === "Active" ? "orange" : status === "Upcoming" ? "blue" : undefined}>{status}</Tag> : null}
                  <Button
                    aria-label={`Remove maintenance window ${i + 1}`}
                    icon={<DeleteOutlined />}
                    onClick={() => setState((s) => ({ ...s, maintenance: (s.maintenance ?? []).filter((_, j) => j !== i) }))}
                  />
                </div>
              );
            })}
            <div>
              <Button
                icon={<PlusOutlined />}
                onClick={() => setState((s) => ({ ...s, maintenance: [...(s.maintenance ?? []), { start: "", end: "", note: "" }] }))}
              >
                Add window
              </Button>
            </div>
          </div>
        </div>

        <Collapse
          ghost
          className="notif-advanced"
          items={[
            {
              key: "throttle",
              label: "Throttling",
              children: (
                <Form layout="vertical" component="div" className="notif-grid">
                  <Form.Item label="Ignore repeats of the same alert for" extra="0 turns it off.">
                    <Space.Compact style={{ width: "100%" }}>
                      <InputNumber
                        min={0}
                        style={{ width: "100%" }}
                        value={state.throttle?.dedup_window_sec ?? 0}
                        onChange={(v) => patchIn("throttle", { dedup_window_sec: typeof v === "number" ? v : 0 })}
                      />
                      <Space.Addon>seconds</Space.Addon>
                    </Space.Compact>
                  </Form.Item>
                  <Form.Item label="Leave at least this long between alerts" extra="0 turns it off.">
                    <Space.Compact style={{ width: "100%" }}>
                      <InputNumber
                        min={0}
                        style={{ width: "100%" }}
                        value={state.throttle?.min_interval_sec ?? 0}
                        onChange={(v) => patchIn("throttle", { min_interval_sec: typeof v === "number" ? v : 0 })}
                      />
                      <Space.Addon>seconds</Space.Addon>
                    </Space.Compact>
                  </Form.Item>
                </Form>
              ),
            },
          ]}
        />
      </Card>

      {state.updated_at ? (
        <Typography.Text type="secondary" className="notif-updated">
          Last saved {formatDateTime(state.updated_at)}
        </Typography.Text>
      ) : null}
      <SaveBar
        dirty={isDirty}
        saving={saveMut.isPending}
        onSave={onSave}
        onDiscard={discard}
        saveDisabled={Boolean(formError)}
        message={formError ?? "Notification settings have unsaved changes."}
      />
    </Space>
  );
}
