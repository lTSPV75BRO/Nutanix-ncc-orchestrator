import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  AutoComplete,
  Button,
  Card,
  Col,
  Collapse,
  Descriptions,
  Form,
  Input,
  InputNumber,
  Modal,
  Popconfirm,
  Row,
  Segmented,
  Select,
  Space,
  Switch,
  Tooltip,
  Typography,
} from "antd";
import { EnvironmentSchedulesPanel } from "./EnvironmentSchedulesPanel";
import {
  DeleteOutlined,
  EyeOutlined,
  ReloadOutlined,
  SaveOutlined,
} from "@ant-design/icons";
import { api, ApiError } from "../../api/client";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { notify } from "../../notify";
import { formatDateTime, relativeTime } from "../../utils/datetime";
import type { ConfigListItem, ScheduleSaveResult, ScheduleState } from "../../api/types";
import { SaveBar, StatTile, StatusBadge, type Tone } from "../../ui";
import { LoadingStateCard } from "../../components/UxStates";
import { describeCron, describeEvery, kubernetesCronFor, nextCronRuns, parseCron } from "./cron";

type Props = {
  backendConfigPath: string;
  onError: (e: unknown) => void;
  isKubernetes?: boolean;
};

/** "simple" edits an interval, "advanced" a custom cron expression. */
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
  // Kubernetes CronJob status.
  suspended?: boolean;
  managed_by_ncc?: boolean;
  last_schedule_time?: string;
  last_successful_time?: string;
  time_zone?: string;
  active_jobs?: number;
};

type Outcome = "succeeded" | "running" | "failed" | null;

/** Outcome of the CronJob's latest Job, from its schedule/success times. */
function cronJobOutcome(h: ScheduleHealth): Outcome {
  if (!h.last_schedule_time) return null;
  if ((h.active_jobs ?? 0) > 0) return "running";
  const scheduled = Date.parse(h.last_schedule_time);
  const succeeded = h.last_successful_time ? Date.parse(h.last_successful_time) : NaN;
  return !Number.isNaN(succeeded) && succeeded >= scheduled ? "succeeded" : "failed";
}

function formatBytes(bytes?: number): string {
  if (!bytes || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(value >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
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
    case "kubernetes":
      return "Kubernetes CronJob";
    default:
      return type || "—";
  }
}

/** Plain-English summary of a saved schedule (cron takes precedence). */
function describeSchedule(every?: string, cron?: string): string {
  const c = (cron || "").trim();
  if (c) return describeCron(c);
  return (every || "").trim() ? describeEvery(every || "") : "—";
}

/**
 * Approximate the next run for a host interval schedule, anchored on the last
 * time the scheduler log was written (a good proxy for the last run).
 */
function approxNextRun(every?: string, lastModIso?: string): Date | null {
  const m = (every || "").trim().match(/^(\d+)([mhd])$/);
  if (!m) return null;
  const ms = Number(m[1]) * (m[2] === "m" ? 60_000 : m[2] === "h" ? 3_600_000 : 86_400_000);
  if (ms <= 0) return null;
  const base = lastModIso ? new Date(lastModIso).getTime() : Date.now();
  if (Number.isNaN(base)) return null;
  const k = Math.max(1, Math.ceil((Date.now() - base) / ms));
  return new Date(base + k * ms);
}

const isUTCZone = (tz?: string) => !tz || ["UTC", "Etc/UTC", "Etc/GMT", "GMT"].includes(tz);

type SchedulePayload = {
  type: string;
  action: string;
  cron: string;
  every: string;
  config: string;
  log_path: string;
  with_lock: boolean;
  task_name: string;
};

/**
 * What a save would change, for the unsaved-changes bar. The interval is
 * ignored when a cron expression is set, since cron wins.
 */
function scheduleKey(p: SchedulePayload): string {
  return JSON.stringify([p.type, p.action, p.cron, p.cron ? "" : p.every, p.config, p.log_path, p.with_lock, p.task_name]);
}

const INTERVAL_PRESETS = [
  { label: "Every 15m", value: 15, unit: "m" },
  { label: "Hourly", value: 1, unit: "h" },
  { label: "Every 4h", value: 4, unit: "h" },
  { label: "Every 12h", value: 12, unit: "h" },
  { label: "Daily", value: 24, unit: "h" },
];

const CRON_EXAMPLES = [
  { label: "Every 4h at :15", cron: "15 */4 * * *" },
  { label: "Weekdays 02:00", cron: "0 2 * * 1-5" },
  { label: "Daily 03:30", cron: "30 3 * * *" },
  { label: "Sundays 00:00", cron: "0 0 * * 0" },
];

function formatRun(d: Date, utc: boolean): string {
  return d.toLocaleString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    ...(utc ? { timeZone: "UTC" } : {}),
  });
}

const STATUS_TONE: Record<string, Tone> = { success: "ok", warning: "warn", error: "fail", default: "neutral" };

export function ScheduleSection({ backendConfigPath, onError, isKubernetes = false }: Props) {
  const [mode, setMode] = useLocalStorageState<Mode>("settings.schedule.mode", "simple");
  const [type, setType] = useLocalStorageState("settings.schedule.type", "auto");
  const [cron, setCron] = useLocalStorageState("settings.schedule.cron", "");
  const [everyValue, setEveryValue] = useLocalStorageState("settings.schedule.everyValue", 4);
  const [everyUnit, setEveryUnit] = useLocalStorageState("settings.schedule.everyUnit", "h");
  const [config, setConfig] = useLocalStorageState("settings.schedule.config", "config.yaml");
  const [taskName, setTaskName] = useLocalStorageState("settings.schedule.taskName", "ncc-orchestrator");
  const [logPath, setLogPath] = useLocalStorageState("settings.schedule.logPath", "logs/ncc-scheduler.log");
  const [withLock, setWithLock] = useLocalStorageState("settings.schedule.withLock", true);
  const [health, setHealth] = useState<ScheduleHealth | null>(null);
  const [healthLoading, setHealthLoading] = useState(false);
  /** Kubernetes: CronJob version the form was loaded from (conditional save). */
  const [resourceVersion, setResourceVersion] = useState<string | undefined>(undefined);
  // Kubernetes only: the CronJob's spec.suspend. Not kept in local storage;
  // the CronJob is the source of truth.
  const [paused, setPaused] = useState(false);
  const [configOptions, setConfigOptions] = useState<ConfigListItem[]>([]);
  const [preview, setPreview] = useState<ScheduleSaveResult | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [removing, setRemoving] = useState(false);

  // systemd timers gate overlapping activations themselves, so the file lock is
  // redundant (and the toggle is shown disabled/forced-on for that backend).
  const lockManagedBySystemd = !isKubernetes && type === "systemd";
  const every = everyValue > 0 ? `${everyValue}${everyUnit}` : "";
  const cronText = mode === "advanced" ? cron.trim() : "";

  const payload = useMemo(
    () => ({
      type: isKubernetes ? "auto" : type,
      action: isKubernetes && paused ? "remove" : "create",
      cron: cronText,
      every,
      config: config.trim(),
      log_path: logPath.trim(),
      with_lock: withLock,
      task_name: taskName.trim(),
    }),
    [isKubernetes, type, paused, cronText, every, config, logPath, withLock, taskName],
  );

  // Validation the server would otherwise answer with a 400.
  const cronError = cronText ? (typeof parseCron(cronText) === "string" ? (parseCron(cronText) as string) : null) : null;
  const k8sInterval = isKubernetes && !cronText ? kubernetesCronFor(every) : null;
  const intervalError = k8sInterval && "error" in k8sInterval ? k8sInterval.error : null;
  // schtasks repeats on an interval only; the engine refuses a cron there.
  const windowsHost = !isKubernetes && (type === "windows" || (type === "auto" && /schtasks/i.test(health?.detector || "")));
  const windowsCronError = windowsHost && cronText ? "Windows Scheduled Tasks run on an interval. Clear the cron expression and use Interval instead." : null;
  const formError = cronError || intervalError || windowsCronError;

  // What the form would schedule, as a cron when one is known, for the preview.
  const effectiveCron = cronText || (k8sInterval && "cron" in k8sInterval ? k8sInterval.cron : "");
  const previewUTC = isKubernetes && isUTCZone(health?.time_zone);
  const canPreviewTimes = Boolean(effectiveCron) && !formError && (!isKubernetes || previewUTC);
  const upcoming = useMemo(
    () => (canPreviewTimes ? nextCronRuns(effectiveCron, new Date(), 3, previewUTC) : []),
    [canPreviewTimes, effectiveCron, previewUTC],
  );

  // The saved schedule, as a scheduleKey; null until one is fetched (or none is saved).
  const [baseline, setBaseline] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const dirty = baseline !== null && scheduleKey(payload) !== baseline;

  /** The saved state with the same defaults the form applies. */
  const savedKey = (s: ScheduleState) =>
    scheduleKey({
      type: s.type || "auto",
      action: isKubernetes ? s.action || "create" : "create",
      // On Kubernetes an interval schedule comes back with its derived cron;
      // the form sends the interval alone for it.
      cron: isKubernetes && s.every ? "" : (s.cron || "").trim(),
      every: s.every || "",
      config: s.config || backendConfigPath || "config.yaml",
      log_path: s.log_path || "logs/ncc-scheduler.log",
      with_lock: Boolean(s.with_lock ?? true),
      task_name: s.task_name || "ncc-orchestrator",
    });

  // Without updated_at the server is returning defaults: nothing is saved yet.
  // A CronJob always has a live schedule, so on Kubernetes it is the baseline.
  const baselineFor = (s: ScheduleState) => (isKubernetes || s.updated_at ? savedKey(s) : null);

  const applySaved = (s: ScheduleState) => {
    setResourceVersion(s.resource_version);
    setType(s.type || "auto");
    setCron(s.cron || "");
    setConfig(s.config || backendConfigPath || "config.yaml");
    setLogPath(s.log_path || "logs/ncc-scheduler.log");
    setWithLock(Boolean(s.with_lock ?? true));
    setTaskName(s.task_name || "ncc-orchestrator");
    const m = String(s.every || "").match(/^(\d+)([mhd])$/);
    if (m) {
      setEveryValue(Number(m[1]));
      setEveryUnit(m[2]);
    }
    if (isKubernetes) {
      // A schedule set with kubectl or Helm that is not a plain interval
      // stays a custom cron instead of being replaced on the next save.
      setMode(s.every ? "simple" : "advanced");
      setType("auto");
      setPaused(s.action === "remove");
    } else {
      setMode((s.cron || "").trim() ? "advanced" : "simple");
    }
    setBaseline(baselineFor(s));
  };

  /** Discard: put the saved schedule back in the form. */
  const discard = async () => {
    try {
      applySaved(await api.loadSchedule());
    } catch (e) {
      onError(e);
    }
  };

  // The form keeps its draft across visits, so only the baseline is fetched
  // on mount. When nothing is saved yet there is no bar; the status card's
  // Install now covers it.
  const loadBaseline = async () => {
    try {
      setBaseline(baselineFor(await api.loadSchedule()));
    } catch {
      setBaseline(null);
    }
  };

  const loadConfigOptions = async (silent = true) => {
    try {
      // /api/v1/settings/configs is admin-only; the runs catalog is the same
      // file list and is allowed for operators who can edit the schedule.
      const resp = await api.runConfigs();
      const items = resp.items ?? [];
      setConfigOptions(items);
      if (!config.trim()) {
        setConfig(resp.default_path || items.find((it) => it.is_active)?.path || backendConfigPath || "config.yaml");
      }
      if (!silent) notify.success("Config file options refreshed.");
    } catch (e) {
      onError(e);
    }
  };

  const loadHealth = async (silent = true) => {
    setHealthLoading(true);
    try {
      setHealth((await api.scheduleHealth()) as ScheduleHealth);
      if (!silent) notify.success("Status refreshed.");
    } catch (e) {
      onError(e);
    } finally {
      setHealthLoading(false);
    }
  };

  /** Save, and on a host install it in the scheduler. */
  const save = async () => {
    if (formError) {
      notify.warning(formError);
      return;
    }
    if (!payload.config) {
      notify.warning("Choose the config file for scheduled runs.");
      return;
    }
    setSaving(true);
    try {
      await api.saveSchedule({ ...payload, print_only: false, apply: true, ...(isKubernetes && resourceVersion ? { resource_version: resourceVersion } : {}) });
      if (isKubernetes) {
        applySaved(await api.loadSchedule());
        notify.success(paused ? "Scheduled runs paused." : "Schedule applied to the CronJob.");
      } else {
        setBaseline(scheduleKey(payload));
        notify.success("Schedule saved and installed.");
      }
      void loadHealth(true);
    } catch (e) {
      if (isKubernetes && e instanceof ApiError && e.status === 409) {
        // Someone else changed the CronJob since this form loaded: show theirs.
        notify.warning("Someone else changed the schedule while you were editing. The form now shows the current schedule; make your change again.");
        try {
          applySaved(await api.loadSchedule());
        } catch (reloadErr) {
          onError(reloadErr);
        }
      } else {
        onError(e);
      }
    } finally {
      setSaving(false);
    }
  };

  /** Host only: show the scheduler command without saving or installing it. */
  const showPreview = async () => {
    if (formError) {
      notify.warning(formError);
      return;
    }
    setPreviewing(true);
    try {
      setPreview(await api.saveSchedule({ ...payload, print_only: true, apply: true }));
    } catch (e) {
      onError(e);
    } finally {
      setPreviewing(false);
    }
  };

  /** Host only: take the entry out of the scheduler. */
  const removeSchedule = async () => {
    setRemoving(true);
    try {
      await api.saveSchedule({ ...payload, action: "remove", print_only: false, apply: true });
      notify.success("Schedule removed from the host scheduler.");
      await loadBaseline();
      await loadHealth(true);
    } catch (e) {
      onError(e);
    } finally {
      setRemoving(false);
    }
  };

  useEffect(() => {
    void loadHealth(true);
    if (isKubernetes) {
      // The form shows the live CronJob, not a draft from an earlier visit.
      void discard();
    } else {
      void loadConfigOptions(true);
      void loadBaseline();
    }
  }, []);

  const configSelectOptions = useMemo(
    () =>
      configOptions.map((item) => ({
        value: item.path,
        label: item.is_active ? `${item.path} (active)` : item.path,
      })),
    [configOptions],
  );

  const presetActive = (p: (typeof INTERVAL_PRESETS)[number]) => everyValue === p.value && everyUnit === p.unit;

  // ---- Status summary --------------------------------------------------
  const status = (() => {
    if (!health) return null;
    if (isKubernetes) {
      return health.suspended
        ? { label: "Paused", tone: "warning" }
        : { label: "Active", tone: "success" };
    }
    if (health.installed) return { label: "Active", tone: "success" };
    if (health.saved) return { label: "Not installed", tone: "warning" };
    return { label: "Not scheduled", tone: "default" };
  })();

  const healthUTC = isKubernetes && isUTCZone(health?.time_zone);
  const healthNext = (() => {
    if (!health || (isKubernetes && health.suspended) || (!isKubernetes && !health.installed)) return null;
    const c = (health.cron || "").trim();
    if (c) {
      if (isKubernetes && !healthUTC) return null;
      return nextCronRuns(c, new Date(), 1, healthUTC)[0] ?? null;
    }
    return approxNextRun(health.every, health.log_mod_time);
  })();
  const lastRunAt = isKubernetes ? health?.last_schedule_time : health?.log_exists ? health?.log_mod_time : undefined;
  const outcome: Outcome = !health
    ? null
    : isKubernetes
      ? cronJobOutcome(health)
      : health.last_error
        ? "failed"
        : health.last_success
          ? "succeeded"
          : null;
  const outcomeLabel =
    outcome === "succeeded" ? "Succeeded" : outcome === "failed" ? "Failed" : outcome === "running" ? "Running" : "None yet";

  const details = health ? (
    <Descriptions size="small" column={1} bordered>
      <Descriptions.Item label={isKubernetes ? "CronJob" : "Task name"}>
        <Typography.Text code>{health.task_name || "—"}</Typography.Text>
      </Descriptions.Item>
      <Descriptions.Item label="Cron expression">
        <Typography.Text code>{health.cron || "—"}</Typography.Text>
      </Descriptions.Item>
      <Descriptions.Item label="Backend">
        {friendlyBackend(health.type)}
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          {" "}
          · via {health.detector || "—"}
        </Typography.Text>
      </Descriptions.Item>
      {isKubernetes ? (
        <Descriptions.Item label="Time zone">{health.time_zone || "Controller default (usually UTC)"}</Descriptions.Item>
      ) : null}
      <Descriptions.Item label="Config file">
        <Typography.Text className="mono" copyable={health.config ? { text: health.config } : false}>
          {health.config || "—"}
        </Typography.Text>
      </Descriptions.Item>
      <Descriptions.Item label={isKubernetes ? "Last changed from NCC" : "State updated"}>
        {health.last_updated_at ? formatDateTime(health.last_updated_at) : "—"}
      </Descriptions.Item>
      {!isKubernetes ? (
        <Descriptions.Item label="Log file">
          <Space orientation="vertical" size={2}>
            <Typography.Text className="mono" copyable={health.log_path ? { text: health.log_path } : false}>
              {health.log_path || "—"}
            </Typography.Text>
            {health.log_exists ? (
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                {formatBytes(health.log_size)} · modified {relativeTime(health.log_mod_time)}
              </Typography.Text>
            ) : null}
            {health.last_run ? (
              <Typography.Text type="secondary" style={{ fontSize: 12 }} ellipsis={{ tooltip: health.last_run }}>
                Last line: {health.last_run}
              </Typography.Text>
            ) : null}
          </Space>
        </Descriptions.Item>
      ) : null}
      {!isKubernetes ? (
        <Descriptions.Item label="Lock file">
          <Typography.Text className="mono" copyable={health.lock_path ? { text: health.lock_path } : false}>
            {health.lock_path || (health.with_lock ? "—" : "Off")}
          </Typography.Text>
        </Descriptions.Item>
      ) : null}
    </Descriptions>
  ) : null;

  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      {/* Status: what is scheduled right now, from the scheduler itself. */}
      <div className="overview-runline" aria-label="Schedule status">
        <div className="overview-runline-text">
          {status ? <StatusBadge tone={STATUS_TONE[status.tone] ?? "neutral"}>{status.label}</StatusBadge> : null}
          <span>
            {isKubernetes ? `Kubernetes CronJob ${health?.task_name || ""}`.trim() : `Host scheduler${health?.detector ? ` (${health.detector})` : ""}`}
          </span>
        </div>
        <div className="overview-runline-actions">
          {!isKubernetes && health?.installed ? (
            <Popconfirm
              title="Remove the schedule?"
              description="Scheduled runs stop until you save a schedule again."
              okText="Remove"
              okButtonProps={{ danger: true }}
              onConfirm={() => void removeSchedule()}
            >
              <Button danger icon={<DeleteOutlined />} loading={removing}>
                Remove
              </Button>
            </Popconfirm>
          ) : null}
          <Button icon={<ReloadOutlined />} loading={healthLoading} onClick={() => void loadHealth(false)} aria-label="Refresh status">
            Refresh
          </Button>
        </div>
      </div>

      {health ? (
        <div className="overview-stats">
          <StatTile
            label="Schedule"
            value={
              !isKubernetes && !health.installed && !health.saved
                ? "None"
                : describeSchedule(health.every, health.cron) === "—"
                  ? "Custom"
                  : describeSchedule(health.every, health.cron)
            }
          />
          <StatTile
            label="Next run"
            value={
              healthNext ? (
                <Tooltip title={formatDateTime(healthNext.toISOString())}>{relativeTime(healthNext.toISOString())}</Tooltip>
              ) : status?.label === "Active" ? (
                "Soon"
              ) : (
                "Not scheduled"
              )
            }
          />
          <StatTile
            label="Last run"
            value={lastRunAt ? <Tooltip title={formatDateTime(lastRunAt)}>{relativeTime(lastRunAt)}</Tooltip> : "Not yet"}
          />
          <StatTile
            label="Last outcome"
            value={outcomeLabel}
            tone={outcome === "succeeded" ? "ok" : outcome === "failed" ? "fail" : undefined}
          />
        </div>
      ) : (
        <LoadingStateCard rows={2} />
      )}

        {isKubernetes && health?.suspended ? (
          <Alert
            type="warning"
            showIcon
            className="schedule-status-alert"
            title="Scheduled runs are paused"
            description="The CronJob is suspended, so no runs start on schedule. Turn off Pause scheduled runs below and save to resume."
          />
        ) : null}
        {!isKubernetes && health && !health.installed ? (
          <Alert
            type={health.saved ? "warning" : "info"}
            showIcon
            className="schedule-status-alert"
            title={health.saved ? "Saved, but not installed on this host" : "No recurring runs yet"}
            description={
              health.saved
                ? `A schedule is saved, but ${health.detector || "the host scheduler"} has no matching entry. Save the schedule below to install it.`
                : "Pick when to run below (every 4 hours works well for most fleets) and click Save schedule."
            }
          />
        ) : null}
        {health?.install_check_error ? (
          <Alert type="warning" showIcon className="schedule-status-alert" title="Couldn't check the host scheduler" description={health.install_check_error} />
        ) : null}
        {!isKubernetes && health?.last_error ? (
          <Alert
            type="error"
            showIcon
            className="schedule-status-alert"
            title="Last scheduled run failed"
            description={<Typography.Text code>{health.last_error}</Typography.Text>}
          />
        ) : null}

        {details ? (
          <Collapse
            size="small"
            className="schedule-details"
            items={[{ key: "details", label: "Technical details", children: details }]}
          />
        ) : null}

      {/* Editor */}
      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          When to run
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          {isKubernetes
            ? `Changes go to the ${health?.task_name || "runner"} CronJob. Re-applying the Helm or Kustomize manifests resets it to their schedule.`
            : "Runs the main config. Pick an interval, or use a cron expression for exact times."}
        </Typography.Text>

        <Form layout="vertical" onSubmitCapture={(e) => e.preventDefault()} style={{ marginTop: 16 }}>
          <div style={{ marginBottom: 16 }}>
            <Segmented
              aria-label="Schedule editor mode"
              value={mode}
              onChange={(v) => setMode(v as Mode)}
              options={[
                { label: "Interval", value: "simple" },
                { label: "Custom cron", value: "advanced" },
              ]}
            />
          </div>

          {mode === "simple" ? (
            <>
              <Space size={8} wrap className="schedule-presets" aria-label="Interval presets">
                {INTERVAL_PRESETS.map((p) => (
                  <Button
                    key={p.label}
                    size="small"
                    type={presetActive(p) ? "primary" : "default"}
                    aria-pressed={presetActive(p)}
                    onClick={() => {
                      setEveryValue(p.value);
                      setEveryUnit(p.unit);
                    }}
                  >
                    {p.label}
                  </Button>
                ))}
              </Space>
              <Row gutter={12} align="bottom">
                <Col xs={12} sm={6}>
                  <Form.Item label="Run every" htmlFor="sched-every-value" style={{ marginBottom: 0 }}>
                    <InputNumber
                      id="sched-every-value"
                      name="every-value"
                      min={1}
                      value={everyValue}
                      onChange={(v) => setEveryValue(Number(v || 1))}
                      style={{ width: "100%" }}
                    />
                  </Form.Item>
                </Col>
                <Col xs={12} sm={6}>
                  <Form.Item label="Unit" htmlFor="sched-every-unit" style={{ marginBottom: 0 }}>
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
              </Row>
              {intervalError ? <Alert type="error" showIcon style={{ marginTop: 12 }} title={intervalError} /> : null}
            </>
          ) : (
            <>
              <Form.Item
                label="Custom cron expression"
                htmlFor="sched-cron"
                validateStatus={cronError || windowsCronError ? "error" : undefined}
                help={
                  cronError ||
                  windowsCronError ||
                  (cronText
                    ? undefined
                    :`Leave empty to use the interval (${describeEvery(every).toLowerCase()}). Five fields: minute hour day-of-month month day-of-week${isKubernetes ? ", or a macro such as @daily" : ""}.`)
                }
              >
                <Input
                  id="sched-cron"
                  name="cron"
                  value={cron}
                  onChange={(e) => setCron(e.target.value)}
                  placeholder="e.g. 15 */4 * * *"
                  autoComplete="off"
                  className="mono"
                />
              </Form.Item>
              <Space size={8} wrap className="schedule-presets" aria-label="Cron examples">
                <Typography.Text type="secondary">Examples:</Typography.Text>
                {CRON_EXAMPLES.map((ex) => (
                  <Button key={ex.cron} size="small" type={cronText === ex.cron ? "primary" : "default"} onClick={() => setCron(ex.cron)}>
                    {ex.label}
                  </Button>
                ))}
              </Space>
            </>
          )}

          <div className="schedule-preview" aria-live="polite">
            <Typography.Text strong>
              {formError ? "Fix the schedule above" : cronText ? describeCron(cronText) : describeEvery(every)}
            </Typography.Text>
            {upcoming.length > 0 ? (
              <Typography.Text type="secondary">
                Next runs{previewUTC ? " (UTC)" : ""}: {upcoming.map((d) => formatRun(d, previewUTC)).join(" · ")}
              </Typography.Text>
            ) : !formError && !isKubernetes && !cronText ? (
              <Typography.Text type="secondary">Counted from when the schedule is installed.</Typography.Text>
            ) : isKubernetes && !previewUTC && !formError ? (
              <Typography.Text type="secondary">Times follow the CronJob's time zone ({health?.time_zone}).</Typography.Text>
            ) : null}
          </div>

          {isKubernetes ? (
            <Space size={8} style={{ marginTop: 16 }}>
              <Switch id="sched-paused" aria-label="Pause scheduled runs" checked={paused} onChange={setPaused} />
              <Typography.Text>
                <label htmlFor="sched-paused">Pause scheduled runs (suspend the CronJob)</label>
              </Typography.Text>
            </Space>
          ) : (
            <>
              <Row gutter={16} style={{ marginTop: 16 }}>
                <Col xs={24} md={14}>
                  <Form.Item
                    label="Config file"
                    htmlFor="sched-config"
                    tooltip="The configuration scheduled runs use. Pick one from the list or type a path."
                    validateStatus={payload.config ? undefined : "error"}
                    help={payload.config ? undefined : "Required"}
                  >
                    <Space.Compact style={{ width: "100%" }}>
                      <AutoComplete
                        id="sched-config"
                        value={config}
                        options={configSelectOptions}
                        onChange={(v) => setConfig(String(v ?? ""))}
                        placeholder="config.yaml"
                        style={{ width: "100%" }}
                        filterOption={(input, option) => String(option?.value ?? "").toLowerCase().includes(input.toLowerCase())}
                      />
                      <Button icon={<ReloadOutlined />} onClick={() => void loadConfigOptions(false)} aria-label="Refresh config options" />
                    </Space.Compact>
                  </Form.Item>
                </Col>
                <Col xs={24} md={10}>
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
              </Row>
              {type === "systemd" ? (
                <Alert
                  type="info"
                  showIcon
                  style={{ marginBottom: 12 }}
                  title="systemd timer"
                  description="Installs a .timer and oneshot .service under /etc/systemd/system (needs root). Overlapping runs are prevented, missed runs replay on boot, and day-of-week cron fields are not supported."
                />
              ) : null}
              <Space size={8}>
                <Tooltip title={lockManagedBySystemd ? "systemd never starts a second run while one is still going, so the file lock is not needed." : undefined}>
                  <Switch
                    id="sched-with-lock"
                    aria-label="Prevent overlapping runs"
                    checked={lockManagedBySystemd ? true : withLock}
                    disabled={lockManagedBySystemd}
                    onChange={setWithLock}
                  />
                </Tooltip>
                <Typography.Text>
                  <label htmlFor="sched-with-lock">Skip a run if the previous one is still going{lockManagedBySystemd ? " (built into systemd)" : ""}</label>
                </Typography.Text>
              </Space>
              <Collapse
                ghost
                size="small"
                className="schedule-details"
                items={[
                  {
                    key: "more",
                    label: "More options",
                    children: (
                      <Row gutter={16}>
                        <Col xs={24} md={12}>
                          <Form.Item label="Task name" htmlFor="sched-task-name" tooltip="Name of the cron entry, timer or scheduled task.">
                            <Input id="sched-task-name" name="task-name" value={taskName} onChange={(e) => setTaskName(e.target.value)} autoComplete="off" />
                          </Form.Item>
                        </Col>
                        <Col xs={24} md={12}>
                          <Form.Item label="Scheduler log file" htmlFor="sched-log-path">
                            <Input id="sched-log-path" name="log-path" value={logPath} onChange={(e) => setLogPath(e.target.value)} autoComplete="off" />
                          </Form.Item>
                        </Col>
                      </Row>
                    ),
                  },
                ]}
              />
            </>
          )}

          <Space size={8} wrap style={{ marginTop: 16 }}>
            <Button type="primary" icon={<SaveOutlined />} onClick={() => void save()} loading={saving} disabled={Boolean(formError)}>
              Save schedule
            </Button>
            {!isKubernetes ? (
              <Tooltip title="Show the scheduler entry that would be installed, without changing anything.">
                <Button icon={<EyeOutlined />} onClick={() => void showPreview()} loading={previewing} disabled={Boolean(formError)}>
                  Preview
                </Button>
              </Tooltip>
            ) : null}
          </Space>
        </Form>
      </Card>

      <EnvironmentSchedulesPanel />

      <Modal
        title="Scheduler entry preview"
        open={preview !== null}
        onCancel={() => setPreview(null)}
        footer={<Button onClick={() => setPreview(null)}>Close</Button>}
        width={720}
      >
        <Typography.Paragraph type="secondary">Nothing was saved or installed. Save schedule installs this entry.</Typography.Paragraph>
        {preview?.apply_error ? <Alert type="error" showIcon title={preview.apply_error} style={{ marginBottom: 12 }} /> : null}
        <pre className="schedule-preview-output">{preview?.output || "(no output)"}</pre>
      </Modal>

      <SaveBar
        dirty={dirty}
        saving={saving}
        onSave={() => void save()}
        onDiscard={() => void discard()}
        message="The schedule differs from the saved one."
        saveLabel="Save schedule"
        saveDisabled={Boolean(formError)}
      />
    </Space>
  );
}
