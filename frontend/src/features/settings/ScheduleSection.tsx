import { useEffect, useMemo, useState } from "react";
import { Alert, Button, Card, Col, Descriptions, Form, Input, InputNumber, Radio, Row, Select, Space, Switch, Tag, Tooltip, Typography } from "antd";
import { ReloadOutlined, SaveOutlined, HeartOutlined, InfoCircleOutlined } from "@ant-design/icons";
import { api } from "../../api/client";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { notify } from "../../notify";
import { formatDateTime as formatTime, formatDateTime, relativeTime } from "../../utils/datetime";

type Props = {
  backendConfigPath: string;
  onError: (e: unknown) => void;
};

type Mode = "simple" | "advanced";

type ScheduleHealth = {
  configured?: boolean;
  saved?: boolean;
  installed?: boolean;
  state_file_exists?: boolean;
  task_name?: string;
  type?: string;
  action?: string;
  with_lock?: boolean;
  config?: string;
  every?: string;
  cron?: string;
  log_path?: string;
  lock_path?: string;
  last_updated_at?: string;
  last_run?: string;
  last_success?: string;
  last_error?: string;
  log_exists?: boolean;
  log_size?: number;
  log_mod_time?: string;
  detector?: string;
  install_check_error?: string;
};

function formatBytes(bytes?: number): string {
  if (!bytes || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(value >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

/** Human-readable summary of the effective schedule (cron takes precedence). */
function describeSchedule(every?: string, cron?: string): string {
  const c = (cron || "").trim();
  if (c) return `Cron: ${c}`;
  const m = (every || "").trim().match(/^(\d+)([mhd])$/);
  if (m) {
    const n = Number(m[1]);
    const unit = m[2] === "m" ? "minute" : m[2] === "h" ? "hour" : "day";
    return `Every ${n} ${unit}${n === 1 ? "" : "s"}`;
  }
  return "—";
}

/** Friendly backend label for a saved scheduler type. */
function friendlyBackend(type?: string): string {
  switch ((type || "").toLowerCase()) {
    case "systemd":
      return "systemd timer";
    case "cron":
      return "cron";
    case "windows":
      return "Windows Scheduled Task";
    case "auto":
      return "auto-detect";
    default:
      return type || "—";
  }
}

/**
 * Approximate the next run for an interval schedule, anchored on the last time
 * the scheduler log was written (a good proxy for the last run). Returns null
 * for cron expressions (where slot math is non-trivial) — the UI labels the
 * result "approx" since systemd/cron wall-clock alignment may differ slightly.
 */
function approxNextRun(every?: string, cron?: string, lastModIso?: string): Date | null {
  if ((cron || "").trim()) return null;
  const m = (every || "").trim().match(/^(\d+)([mhd])$/);
  if (!m) return null;
  const n = Number(m[1]);
  const ms = n * (m[2] === "m" ? 60_000 : m[2] === "h" ? 3_600_000 : 86_400_000);
  if (ms <= 0) return null;
  const base = lastModIso ? new Date(lastModIso).getTime() : Date.now();
  if (Number.isNaN(base)) return null;
  const now = Date.now();
  const k = Math.max(1, Math.ceil((now - base) / ms));
  return new Date(base + k * ms);
}

export function ScheduleSection({ backendConfigPath, onError }: Props) {
  const [mode, setMode] = useLocalStorageState<Mode>("settings.schedule.mode", "simple");
  const [type, setType] = useLocalStorageState("settings.schedule.type", "auto");
  const [action, setAction] = useLocalStorageState("settings.schedule.action", "create");
  const [cron, setCron] = useLocalStorageState("settings.schedule.cron", "");
  const [everyValue, setEveryValue] = useLocalStorageState("settings.schedule.everyValue", 4);
  const [everyUnit, setEveryUnit] = useLocalStorageState("settings.schedule.everyUnit", "h");
  const [config, setConfig] = useLocalStorageState("settings.schedule.config", "config.yaml");
  const [taskName, setTaskName] = useLocalStorageState("settings.schedule.taskName", "ncc-orchestrator");
  const [logPath, setLogPath] = useLocalStorageState("settings.schedule.logPath", "logs/ncc-scheduler.log");
  const [withLock, setWithLock] = useLocalStorageState("settings.schedule.withLock", true);
  const [printOnly, setPrintOnly] = useLocalStorageState("settings.schedule.printOnly", true);
  const [apply, setApply] = useLocalStorageState("settings.schedule.apply", false);
  const [health, setHealth] = useState<ScheduleHealth | null>(null);

  // systemd timers gate overlapping activations themselves, so the file lock is
  // redundant (and the toggle is shown disabled/forced-on for that backend).
  const lockManagedBySystemd = mode === "advanced" && type === "systemd";

  const nextRun = useMemo(
    () => (health ? approxNextRun(health.every, health.cron, health.log_mod_time) : null),
    [health],
  );

  const payload = useMemo(() => {
    const every = everyValue > 0 ? `${everyValue}${everyUnit}` : "";
    return {
      type: mode === "advanced" ? type : "auto",
      action: mode === "advanced" ? action : "create",
      cron: mode === "advanced" ? cron.trim() : "",
      every,
      config: config.trim(),
      log_path: logPath.trim(),
      with_lock: withLock,
      task_name: taskName.trim(),
      print_only: printOnly,
      apply,
    };
  }, [mode, type, action, cron, everyValue, everyUnit, config, logPath, withLock, taskName, printOnly, apply]);

  const load = async () => {
    try {
      const s = await api.loadSchedule();
      setType(s.type || "auto");
      setAction(s.action || "create");
      setCron(s.cron || "");
      setConfig(s.config || backendConfigPath || "config.yaml");
      setLogPath(s.log_path || "logs/ncc-scheduler.log");
      setWithLock(Boolean(s.with_lock ?? true));
      setTaskName(s.task_name || "ncc-orchestrator");
      setPrintOnly(Boolean(s.print_only));
      const m = String(s.every || "").match(/^(\d+)([mhd])$/);
      if (m) {
        setEveryValue(Number(m[1]));
        setEveryUnit(m[2]);
      }
      setMode((s.type !== "auto" || s.action !== "create" || (s.cron || "").trim()) ? "advanced" : "simple");
      notify.success("Schedule loaded.");
    } catch (e) {
      onError(e);
    }
  };

  const loadHealth = async (silent = false) => {
    try {
      const h = await api.scheduleHealth();
      setHealth(h as ScheduleHealth);
      if (!silent) notify.success("Scheduler health refreshed.");
    } catch (e) {
      onError(e);
    }
  };

  const save = async () => {
    if (!payload.config) {
      notify.warning("Config path is required.");
      return;
    }
    try {
      await api.saveSchedule(payload);
      notify.success(apply ? "Schedule saved and applied." : "Schedule saved.");
    } catch (e) {
      onError(e);
    }
  };

  /**
   * One-click install: forces apply=true and print_only=false so the schedule
   * is actually installed on the host scheduler. Used by the "Not installed"
   * alert in the Scheduler Health card.
   */
  const installNow = async () => {
    if (!payload.config) {
      notify.warning("Config path is required.");
      return;
    }
    notify.loading({
      key: "ncc-schedule-install",
      message: "Installing schedule…",
      description: `Adding entry to ${health?.detector || "the host scheduler"}.`,
    });
    try {
      await api.saveSchedule({ ...payload, apply: true, print_only: false });
      // Persist apply=on / print_only=off so subsequent saves stay sticky.
      setApply(true);
      setPrintOnly(false);
      notify.close("ncc-schedule-install");
      notify.success({
        message: "Schedule installed",
        description: `Task '${payload.task_name}' is now scheduled to run every ${payload.every || "configured interval"}.`,
      });
      await loadHealth(true);
    } catch (e) {
      notify.close("ncc-schedule-install");
      onError(e);
    }
  };

  useEffect(() => {
    void loadHealth(true);
  }, []);

  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Schedule
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          Define a recurring NCC run. Use Simple mode for typical intervals, or Advanced for custom cron and platform options.
        </Typography.Text>

        <div style={{ marginTop: 16, marginBottom: 16 }}>
          <Radio.Group
            id="sched-mode"
            name="sched-mode"
            aria-label="Schedule editor mode"
            value={mode}
            onChange={(e) => setMode(e.target.value)}
          >
            <Radio.Button value="simple">Simple</Radio.Button>
            <Radio.Button value="advanced">Advanced</Radio.Button>
          </Radio.Group>
        </div>

        <Form layout="vertical" onSubmitCapture={(e) => e.preventDefault()}>
          <Row gutter={16}>
            <Col xs={12} md={6}>
              <Form.Item label="Run every" htmlFor="sched-every-value">
                <InputNumber id="sched-every-value" name="every-value" min={1} value={everyValue} onChange={(v) => setEveryValue(Number(v || 1))} style={{ width: "100%" }} />
              </Form.Item>
            </Col>
            <Col xs={12} md={6}>
              <Form.Item label="Unit" htmlFor="sched-every-unit">
                <Select
                  id="sched-every-unit"
                  value={everyUnit}
                  onChange={setEveryUnit}
                  options={[
                    { value: "m", label: "minutes" },
                    { value: "h", label: "hours" },
                    { value: "d", label: "days" },
                  ]}
                  style={{ width: "100%" }}
                />
              </Form.Item>
            </Col>
            <Col xs={24} md={12}>
              <Form.Item label="Config file" htmlFor="sched-config">
                <Input id="sched-config" name="config-path" value={config} onChange={(e) => setConfig(e.target.value)} placeholder="config.yaml" autoComplete="off" />
              </Form.Item>
            </Col>
          </Row>

          <Space size={8} wrap style={{ marginBottom: 12 }}>
            <Typography.Text type="secondary">Quick presets:</Typography.Text>
            <Button size="small" onClick={() => { setEveryValue(15); setEveryUnit("m"); }}>Every 15m</Button>
            <Button size="small" onClick={() => { setEveryValue(1); setEveryUnit("h"); }}>Hourly</Button>
            <Button size="small" onClick={() => { setEveryValue(4); setEveryUnit("h"); }}>Every 4h</Button>
            <Button size="small" onClick={() => { setEveryValue(24); setEveryUnit("h"); }}>Daily</Button>
          </Space>

          <div style={{ marginBottom: 12 }}>
            <Typography.Text type="secondary">
              Effective schedule:{" "}
              <Typography.Text strong>{describeSchedule(payload.every, payload.cron)}</Typography.Text>
            </Typography.Text>
          </div>

          {mode === "advanced" ? (
            <>
              <Row gutter={16}>
                <Col xs={24} md={8}>
                  <Form.Item label="Scheduler type" htmlFor="sched-type">
                    <Select
                      id="sched-type"
                      value={type}
                      onChange={setType}
                      options={[
                        { value: "auto", label: "Auto-detect" },
                        { value: "cron", label: "Cron (Linux/macOS)" },
                        { value: "systemd", label: "systemd timer (Linux)" },
                        { value: "windows", label: "Scheduled Task (Windows)" },
                      ]}
                      style={{ width: "100%" }}
                    />
                  </Form.Item>
                </Col>
                <Col xs={24} md={8}>
                  <Form.Item label="Action" htmlFor="sched-action">
                    <Select
                      id="sched-action"
                      value={action}
                      onChange={setAction}
                      options={[
                        { value: "create", label: "Create / update" },
                        { value: "list", label: "List existing" },
                        { value: "remove", label: "Remove" },
                        { value: "run-now", label: "Run now" },
                      ]}
                      style={{ width: "100%" }}
                    />
                  </Form.Item>
                </Col>
                <Col xs={24} md={8}>
                  <Form.Item label="Custom cron expression" tooltip="5-field cron, leave empty to derive from interval. Day-of-week is not supported for systemd timers." htmlFor="sched-cron">
                    <Input id="sched-cron" name="cron" value={cron} onChange={(e) => setCron(e.target.value)} placeholder="e.g. 15 */4 * * *" autoComplete="off" />
                  </Form.Item>
                </Col>
              </Row>
              {type === "systemd" ? (
                <Alert
                  type="info"
                  showIcon
                  style={{ marginBottom: 12 }}
                  message="systemd timer backend"
                  description="Installs a systemd .timer + oneshot .service under /etc/systemd/system (requires root). Overlapping runs are prevented automatically (the file lock is not needed), missed runs replay on boot (Persistent=true), and the run uses the config file's directory as its working directory. Day-of-week cron fields are not supported — use an interval or a cron without a DOW field."
                />
              ) : null}
              <Row gutter={16}>
                <Col xs={24} md={12}>
                  <Form.Item label="Task name" htmlFor="sched-task-name">
                    <Input id="sched-task-name" name="task-name" value={taskName} onChange={(e) => setTaskName(e.target.value)} autoComplete="off" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item label="Scheduler log file" htmlFor="sched-log-path">
                    <Input id="sched-log-path" name="log-path" value={logPath} onChange={(e) => setLogPath(e.target.value)} autoComplete="off" />
                  </Form.Item>
                </Col>
              </Row>
            </>
          ) : null}

          <Space size={24} wrap style={{ marginTop: 4, marginBottom: 12 }}>
            <Space size={8}>
              <Switch id="sched-print-only" aria-label="Preview only (no system changes)" checked={printOnly} onChange={setPrintOnly} />
              <Typography.Text>
                <label htmlFor="sched-print-only">Preview only (no system changes)</label>
              </Typography.Text>
            </Space>
            <Space size={8}>
              <Switch id="sched-apply" aria-label="Apply changes immediately" checked={apply} onChange={setApply} />
              <Typography.Text>
                <label htmlFor="sched-apply">Apply changes immediately</label>
              </Typography.Text>
            </Space>
            <Space size={8}>
              <Tooltip title={lockManagedBySystemd ? "systemd prevents overlapping runs natively — a second activation won't start while one is still running, so the file lock is not needed." : undefined}>
                <Switch
                  id="sched-with-lock"
                  aria-label="Prevent overlapping runs"
                  checked={lockManagedBySystemd ? true : withLock}
                  disabled={lockManagedBySystemd}
                  onChange={setWithLock}
                />
              </Tooltip>
              <Typography.Text>
                <label htmlFor="sched-with-lock">
                  Prevent overlapping runs {lockManagedBySystemd ? "(native via systemd)" : "(file lock)"}
                </label>
              </Typography.Text>
            </Space>
          </Space>

          <Space size={8} wrap>
            <Button icon={<ReloadOutlined />} onClick={load}>Load Current</Button>
            <Button type="primary" icon={<SaveOutlined />} onClick={save}>Save Schedule</Button>
            <Button icon={<HeartOutlined />} onClick={() => loadHealth(false)}>Refresh Health</Button>
          </Space>
        </Form>
      </Card>

      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Scheduler Health
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          Authoritative status from the host scheduler ({health?.detector || "crontab"}). Saved means the UI has stored
          a configuration; Installed means the host scheduler actually has a matching entry.
        </Typography.Text>
        {health ? (
          <>
            <Space size={[8, 8]} wrap style={{ marginTop: 12, marginBottom: 16 }}>
              <Tooltip title={health.installed ? `Detected via ${health.detector || "crontab"}` : "No matching entry found in host scheduler"}>
                <Tag color={health.installed ? "success" : "error"} style={{ fontSize: 13, padding: "2px 10px" }}>
                  {health.installed ? "Installed" : "Not installed"}
                </Tag>
              </Tooltip>
              <Tooltip title={health.saved ? "Schedule state has been saved at least once" : "Schedule state has never been saved"}>
                <Tag color={health.saved ? "processing" : "default"} style={{ fontSize: 13, padding: "2px 10px" }}>
                  {health.saved ? "Saved" : "Not saved"}
                </Tag>
              </Tooltip>
              {health.with_lock ? <Tag color="cyan">Overlap lock</Tag> : null}
              {typeof health.log_exists === "boolean" ? (
                <Tag color={health.log_exists ? "success" : "default"}>
                  {health.log_exists ? "Log present" : "No log yet"}
                </Tag>
              ) : null}
            </Space>

            {!health.installed && health.saved ? (
              <Alert
                type="warning"
                showIcon
                style={{ marginBottom: 12 }}
                message="Schedule saved but not installed"
                description={
                  <>
                    A schedule configuration is stored in the API state file but no matching entry was found in{" "}
                    <Typography.Text code>{health.detector || "the host scheduler"}</Typography.Text>. Click{" "}
                    <Typography.Text strong>Install now</Typography.Text> to add the cron entry on this host.
                  </>
                }
                action={
                  <Button size="small" type="primary" onClick={installNow}>
                    Install now
                  </Button>
                }
              />
            ) : null}

            {!health.installed && !health.saved ? (
              <Alert
                type="info"
                showIcon
                icon={<InfoCircleOutlined />}
                style={{ marginBottom: 12 }}
                message="No schedule configured"
                description={
                  <>
                    Pick an interval above (default <Typography.Text code>4h</Typography.Text> works well) and click{" "}
                    <Typography.Text strong>Install now</Typography.Text> to schedule it on this host.
                  </>
                }
                action={
                  <Button size="small" type="primary" onClick={installNow}>
                    Install now
                  </Button>
                }
              />
            ) : null}

            {health.last_error ? (
              <Alert
                type="error"
                showIcon
                style={{ marginBottom: 12 }}
                message="Last scheduled run failed"
                description={<Typography.Text code>{health.last_error}</Typography.Text>}
              />
            ) : null}

            <Descriptions size="small" column={1} bordered>
              <Descriptions.Item label="Task name">
                <Typography.Text code>{health.task_name || "—"}</Typography.Text>
              </Descriptions.Item>
              <Descriptions.Item label="Schedule">
                <Typography.Text strong>{describeSchedule(health.every, health.cron)}</Typography.Text>
              </Descriptions.Item>
              <Descriptions.Item label="Backend">
                <Space size={6}>
                  <Tag color={(health.type || "").toLowerCase() === "systemd" ? "geekblue" : "default"}>
                    {friendlyBackend(health.type)}
                  </Tag>
                  <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                    via {health.detector || "—"}
                  </Typography.Text>
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label="Config file">
                <Typography.Text className="mono" copyable={health.config ? { text: health.config } : false}>
                  {health.config || "—"}
                </Typography.Text>
              </Descriptions.Item>
              <Descriptions.Item label="Last run">
                {health.last_run ? <Tooltip title={formatDateTime(health.last_run)}>{relativeTime(health.last_run)}</Tooltip> : "—"}
              </Descriptions.Item>
              <Descriptions.Item label="Last success">
                {health.last_success ? <Tooltip title={formatDateTime(health.last_success)}>{relativeTime(health.last_success)}</Tooltip> : "—"}
              </Descriptions.Item>
              <Descriptions.Item label="Next run (approx)">
                {nextRun ? (
                  <Tooltip title={formatDateTime(nextRun.toISOString())}>
                    {relativeTime(nextRun.toISOString())}
                  </Tooltip>
                ) : (
                  <Typography.Text type="secondary">
                    {health.installed ? "—" : "not scheduled"}
                  </Typography.Text>
                )}
              </Descriptions.Item>
              <Descriptions.Item label="State updated">{formatTime(health.last_updated_at)}</Descriptions.Item>
              <Descriptions.Item label="Log file">
                <Space direction="vertical" size={2}>
                  <Typography.Text className="mono" copyable={{ text: health.log_path }}>
                    {health.log_path || "—"}
                  </Typography.Text>
                  {health.log_exists ? (
                    <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                      {formatBytes(health.log_size)} · modified {relativeTime(health.log_mod_time)}
                    </Typography.Text>
                  ) : null}
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label="Lock file">
                <Typography.Text className="mono" copyable={{ text: health.lock_path }}>
                  {health.lock_path || "—"}
                </Typography.Text>
              </Descriptions.Item>
            </Descriptions>
          </>
        ) : (
          <Typography.Text type="secondary">Click "Refresh Health" to load scheduler status.</Typography.Text>
        )}
      </Card>
    </Space>
  );
}
