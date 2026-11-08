import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router";
import { Button, Input, Select, Switch, Table, Tooltip } from "antd";
import { DeleteOutlined, DeploymentUnitOutlined, PlusOutlined, SaveOutlined } from "@ant-design/icons";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { EnvironmentSchedule } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { sectionHref } from "../../navigation";
import { notify, notifyError } from "../../notify";
import { EmptyState, Panel } from "../../ui";
import { formatDateTime, relativeTime } from "../../utils/datetime";

const EVERY = [
  { value: "1h", label: "Every hour" },
  { value: "4h", label: "Every 4 hours" },
  { value: "12h", label: "Every 12 hours" },
  { value: "1d", label: "Every day" },
  { value: "7d", label: "Every week" },
];

const TIME_RE = /^([01][0-9]|2[0-3]):[0-5][0-9]$/;

const isDays = (every: string) => /^[1-9][0-9]*d$/.test(every);

function rowError(r: EnvironmentSchedule): string {
  if (!r.environment) return "Pick an environment.";
  if (r.at && !TIME_RE.test(r.at)) return "Use a time like 02:00.";
  return "";
}

/**
 * Schedule → Environments: run each environment on its own timetable. These
 * run inside the API server through the normal run engine, so results merge
 * and are tagged like manual runs.
 */
export function EnvironmentSchedulesPanel() {
  const { canOperate } = useAuth();
  const qc = useQueryClient();
  const envs = useQuery({ queryKey: ["environments"], queryFn: api.listEnvironments });
  const schedules = useQuery({ queryKey: ["environment-schedules"], queryFn: api.environmentSchedules, refetchInterval: 60_000 });
  const [rows, setRows] = useState<EnvironmentSchedule[]>([]);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (schedules.data) setRows(schedules.data.items ?? []);
  }, [schedules.data]);

  const saved = JSON.stringify((schedules.data?.items ?? []).map(({ id, environment, enabled, every, at }) => ({ id, environment, enabled, every, at: at || "" })) ?? []);
  const current = JSON.stringify(rows.map(({ id, environment, enabled, every, at }) => ({ id, environment, enabled, every, at: at || "" })));
  const dirty = Boolean(schedules.data) && saved !== current;
  const firstError = rows.map(rowError).find(Boolean) ?? "";
  const envOptions = useMemo(() => (envs.data?.items ?? []).filter((e) => !e.error).map((e) => ({ value: e.name, label: e.name })), [envs.data]);
  const tz = schedules.data?.server_timezone;

  const update = (id: string, patch: Partial<EnvironmentSchedule>) => setRows((prev) => prev.map((r) => (r.id === id ? { ...r, ...patch } : r)));

  const add = () =>
    setRows((prev) => [
      ...prev,
      { id: `new-${Date.now()}`, environment: envOptions[0]?.value ?? "", enabled: true, every: "1d", at: "02:00" },
    ]);

  const save = async () => {
    setSaving(true);
    try {
      // New rows carry a local id; the server assigns the real one.
      await api.saveEnvironmentSchedules(rows.map((r) => ({ ...r, id: r.id.startsWith("new-") ? "" : r.id, at: isDays(r.every) ? r.at || "" : "" })));
      await qc.invalidateQueries({ queryKey: ["environment-schedules"] });
      notify.success("Environment schedules saved.");
    } catch (e) {
      notifyError(e, "Couldn't save the environment schedules");
    } finally {
      setSaving(false);
    }
  };

  const noEnvironments = envs.isSuccess && envOptions.length === 0;

  return (
    <Panel
      title="Environment schedules"
      icon={<DeploymentUnitOutlined />}
      description={`Run each environment on its own timetable, through the API server.${tz ? ` Times are server time (${tz}).` : ""}`}
      flush
      extra={
        canOperate && !noEnvironments ? (
          <>
            <Button icon={<PlusOutlined />} onClick={add}>
              Add
            </Button>
            <Tooltip title={firstError || undefined}>
              <Button type="primary" icon={<SaveOutlined />} onClick={() => void save()} disabled={!dirty || Boolean(firstError)} loading={saving}>
                Save
              </Button>
            </Tooltip>
          </>
        ) : null
      }
    >
      {noEnvironments ? (
        <EmptyState
          icon={<DeploymentUnitOutlined />}
          title="No environments yet"
          description="Environments (dev, staging, prod …) are added in Clusters & config. The schedule above runs the main config."
          actions={
            <Link to={sectionHref("config")}>
              <Button>Open Clusters & config</Button>
            </Link>
          }
        />
      ) : (
        <Table<EnvironmentSchedule>
          size="middle"
          rowKey="id"
          dataSource={rows}
          loading={schedules.isLoading}
          pagination={false}
          scroll={{ x: 760 }}
          locale={{ emptyText: <EmptyState title="No environment schedules" description="Add one to run an environment on a timetable, for example prod every day at 02:00." /> }}
          columns={[
            {
              title: "Environment",
              key: "environment",
              width: 170,
              render: (_: unknown, r) => (
                <Select
                  aria-label="Environment"
                  value={r.environment || undefined}
                  onChange={(v) => update(r.id, { environment: v })}
                  options={envOptions}
                  disabled={!canOperate}
                  style={{ width: "100%" }}
                  placeholder="Pick one"
                />
              ),
            },
            {
              title: "Runs",
              key: "every",
              width: 170,
              render: (_: unknown, r) => (
                <Select
                  aria-label="How often"
                  value={r.every}
                  onChange={(v) => update(r.id, { every: v, at: isDays(v) ? r.at || "02:00" : "" })}
                  options={EVERY.some((e) => e.value === r.every) ? EVERY : [...EVERY, { value: r.every, label: `Every ${r.every}` }]}
                  disabled={!canOperate}
                  style={{ width: "100%" }}
                />
              ),
            },
            {
              title: "At",
              key: "at",
              width: 100,
              render: (_: unknown, r) =>
                isDays(r.every) ? (
                  <Input
                    aria-label="Time of day"
                    value={r.at || ""}
                    onChange={(e) => update(r.id, { at: e.target.value })}
                    placeholder="02:00"
                    status={r.at && !TIME_RE.test(r.at) ? "error" : undefined}
                    disabled={!canOperate}
                  />
                ) : (
                  <span className="ui-field-help">—</span>
                ),
            },
            {
              title: "On",
              key: "enabled",
              width: 70,
              render: (_: unknown, r) => (
                <Switch size="small" aria-label="Enabled" checked={r.enabled} onChange={(v) => update(r.id, { enabled: v })} disabled={!canOperate} />
              ),
            },
            {
              title: "Next run",
              key: "next",
              render: (_: unknown, r) =>
                r.next_run_at && !dirty ? (
                  <Tooltip title={formatDateTime(r.next_run_at)}>{relativeTime(r.next_run_at)}</Tooltip>
                ) : (
                  <span className="ui-field-help">{r.enabled ? (dirty ? "After saving" : "—") : "Off"}</span>
                ),
            },
            {
              title: "Last run",
              key: "last",
              render: (_: unknown, r) =>
                r.last_run_at ? (
                  <span className="env-schedule-last">
                    <Tooltip title={formatDateTime(r.last_run_at)}>{relativeTime(r.last_run_at)}</Tooltip>
                    <span className="ui-field-help">
                      {r.last_run_id ? <Link to={`/runs/${encodeURIComponent(r.last_run_id)}`}>{r.last_result || "Run"}</Link> : r.last_result}
                    </span>
                  </span>
                ) : (
                  <span className="ui-field-help">Not yet</span>
                ),
            },
            ...(canOperate
              ? [
                  {
                    title: "",
                    key: "remove",
                    width: 56,
                    render: (_: unknown, r: EnvironmentSchedule) => (
                      <Button
                        type="text"
                        danger
                        icon={<DeleteOutlined />}
                        aria-label={`Remove the ${r.environment || "new"} schedule`}
                        onClick={() => setRows((prev) => prev.filter((x) => x.id !== r.id))}
                      />
                    ),
                  },
                ]
              : []),
          ]}
        />
      )}
    </Panel>
  );
}
