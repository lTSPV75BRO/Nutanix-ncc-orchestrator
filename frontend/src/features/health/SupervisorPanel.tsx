import { Alert, Button, Card, Collapse, Descriptions, Empty, Popconfirm, Skeleton, Space, Table, Tag, Typography } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router";
import { api } from "../../api/client";
import type { PlatformData, SupervisedChild, SupervisorEvent } from "../../api/types";
import { sectionHref } from "../../navigation";
import { notify, notifyError } from "../../notify";
import { formatTime } from "../../utils/datetime";
import { formatDuration } from "../../utils/freshness";

const CHILD_LABEL: Record<string, string> = { api: "API server", ui: "Web UI server" };

const STATE_TAG: Record<SupervisedChild["state"], { color: string; label: string }> = {
  running: { color: "success", label: "Running" },
  starting: { color: "processing", label: "Starting" },
  backoff: { color: "warning", label: "Restarting" },
  cooldown: { color: "error", label: "Cooling down" },
  stopped: { color: "default", label: "Stopped" },
};

const EXIT_LABEL: Record<string, string> = {
  crashed: "Crashed",
  unhealthy: "Stopped answering health probes",
  requested: "Restart requested",
  "port-in-use": "Port already in use",
  "start-failed": "Couldn't start",
};

const POLICY_LABEL: Record<string, string> = {
  max_restarts: "Restarts before a cooldown",
  window: "Restart window / cooldown",
  probe_interval: "Health probe every",
  unhealthy_threshold: "Failed probes before a restart",
  start_grace: "Start-up grace",
  stable_after: "Counts as stable after",
  backoff_cap: "Longest wait between restarts",
};

/** Policy rows in POLICY_LABEL order; unknown keys last. */
function policyRank(key: string): number {
  const i = Object.keys(POLICY_LABEL).indexOf(key);
  return i < 0 ? 99 : i;
}

/** Seconds since an RFC 3339 time, or null. */
export function secondsSince(at: string | undefined, now = Date.now()): number | null {
  if (!at) return null;
  const t = Date.parse(at);
  return Number.isFinite(t) ? Math.max(0, (now - t) / 1000) : null;
}

/** "1.5 GiB" style sizes. */
export function formatBytes(n: number | undefined | null): string {
  if (n == null || !Number.isFinite(n) || n <= 0) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v >= 10 || i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
}

function ChildCard({ child, onRestart, restarting }: { child: SupervisedChild; onRestart: () => void; restarting: boolean }) {
  const tag = STATE_TAG[child.state] ?? STATE_TAG.stopped;
  const up = child.state === "running" ? secondsSince(child.started_at) : null;
  const label = CHILD_LABEL[child.name] ?? child.name;
  const confirm =
    child.name === "api"
      ? "Restart the API server? Pages reconnect when it is back, usually within a few seconds."
      : "Restart the web UI server? Pages reload when it is back.";
  return (
    <Card
      size="small"
      className="page-card"
      data-child={child.name}
      title={
        <Space size={8}>
          <Typography.Text strong>{label}</Typography.Text>
          <Tag color={tag.color}>{tag.label}</Tag>
        </Space>
      }
      extra={
        <Popconfirm title={confirm} okText="Restart" onConfirm={onRestart}>
          <Button size="small" icon={<ReloadOutlined />} loading={restarting}>
            {child.state === "cooldown" || child.state === "backoff" ? "Start now" : "Restart"}
          </Button>
        </Popconfirm>
      }
    >
      <Descriptions size="small" column={{ xs: 1, sm: 2, lg: 3 }}>
        <Descriptions.Item label="Up for">{up == null ? "—" : formatDuration(up)}</Descriptions.Item>
        <Descriptions.Item label="PID">{child.pid || "—"}</Descriptions.Item>
        <Descriptions.Item label="Restarts">
          {child.restarts_total}
          {child.restarts_in_window > 0 ? ` (${child.restarts_in_window} recently)` : ""}
        </Descriptions.Item>
        {child.probed ? (
          <Descriptions.Item label="Health probe">
            {child.last_probe_ok === undefined
              ? "not yet"
              : `${child.last_probe_ok ? "ok" : "failed"}${child.last_probe_ms != null ? `, ${child.last_probe_ms} ms` : ""}`}
            {child.probe_failures > 0 ? ` · ${child.probe_failures} failed in a row` : ""}
          </Descriptions.Item>
        ) : null}
        {child.next_start_at && (child.state === "backoff" || child.state === "cooldown") ? (
          <Descriptions.Item label="Next start">{formatTime(child.next_start_at)}</Descriptions.Item>
        ) : null}
      </Descriptions>
      {child.last_exit_reason ? (
        <Alert
          style={{ marginTop: 8 }}
          type={child.last_exit_reason === "requested" ? "info" : "warning"}
          showIcon
          title={`Last stop: ${EXIT_LABEL[child.last_exit_reason] ?? child.last_exit_reason}${child.last_exit_at ? ` · ${formatTime(child.last_exit_at)}` : ""}`}
          description={
            <>
              {child.last_exit_detail ? <div>{child.last_exit_detail}</div> : null}
              {child.last_exit_log_tail?.length ? (
                <Collapse
                  size="small"
                  ghost
                  items={[
                    {
                      key: "log",
                      label: "End of the server's log",
                      children: <pre className="repair-preview">{child.last_exit_log_tail.join("\n")}</pre>,
                    },
                  ]}
                />
              ) : null}
            </>
          }
        />
      ) : null}
    </Card>
  );
}

export function SupervisorPanel() {
  const qc = useQueryClient();
  const q = useQuery({ queryKey: ["health", "supervisor"], queryFn: api.supervisor, refetchInterval: 10_000 });
  const restart = useMutation({
    mutationFn: (child: "api" | "ui") => api.restartSupervisedChild(child),
    onSuccess: (_, child) => {
      notify.success(`${CHILD_LABEL[child]} restart requested`);
      window.setTimeout(() => void qc.invalidateQueries({ queryKey: ["health", "supervisor"] }), 4000);
    },
    onError: (err) => notifyError(err, "Couldn't request the restart"),
  });
  if (q.isLoading) return <Card className="page-card"><Skeleton active /></Card>;
  if (q.isError) return <Alert type="error" showIcon title="Couldn't load the supervisor" description={String((q.error as Error)?.message ?? "")} />;
  const data = q.data;
  const st = data?.state;
  const intro = (
    <Typography.Paragraph type="secondary" style={{ margin: 0 }}>
      The supervisor keeps the API and web UI servers running: it restarts a server that crashes or stops answering
      health probes, waits longer after each quick failure, and pauses after too many in a row. The boot service
      (systemd, Windows Task Scheduler or launchd) keeps the supervisor itself running.
    </Typography.Paragraph>
  );
  if (data?.mode === "kubernetes") {
    return (
      <Space orientation="vertical" size={12} style={{ width: "100%" }}>
        <Alert type="info" showIcon title="Managed by Kubernetes" description={data.message} />
      </Space>
    );
  }
  if (!st) {
    return (
      <Space orientation="vertical" size={12} style={{ width: "100%" }}>
        {intro}
        <Alert type={data?.running ? "info" : "warning"} showIcon title={data?.running ? `Supervisor running (pid ${data.pid})` : "Not supervised"} description={data?.message} />
      </Space>
    );
  }
  const children = Object.values(st.children ?? {}).sort((a, b) => a.name.localeCompare(b.name));
  const events = [...(st.events ?? [])].reverse().map((e, i) => ({ ...e, key: `${e.at}-${e.child}-${i}` }));
  const supUp = secondsSince(st.started_at);
  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      {intro}
      {!data?.running ? <Alert type="warning" showIcon title="Supervisor not running" description={data?.message} /> : null}
      <Card size="small" className="page-card" title="Supervisor">
        <Descriptions size="small" column={{ xs: 1, sm: 2, lg: 3 }}>
          <Descriptions.Item label="PID">{st.pid}</Descriptions.Item>
          <Descriptions.Item label="Up for">{supUp == null ? "—" : formatDuration(supUp)}</Descriptions.Item>
          <Descriptions.Item label="Updated">{formatTime(st.updated_at)}</Descriptions.Item>
          {Object.entries(st.policy ?? {})
            .sort(([a], [b]) => policyRank(a) - policyRank(b))
            .map(([k, v]) => (
            <Descriptions.Item key={k} label={POLICY_LABEL[k] ?? k}>
              {String(v)}
            </Descriptions.Item>
            ))}
        </Descriptions>
      </Card>
      {children.map((c) => (
        <ChildCard
          key={c.name}
          child={c}
          restarting={restart.isPending && restart.variables === c.name}
          onRestart={() => restart.mutate(c.name as "api" | "ui")}
        />
      ))}
      <Card size="small" className="page-card" title="Recent events">
        {events.length ? (
          <Table<SupervisorEvent & { key: string }>
            size="small"
            rowKey="key"
            dataSource={events}
            pagination={{ pageSize: 10, size: "small", hideOnSinglePage: true }}
            scroll={{ x: 560 }}
            columns={[
              { title: "When", dataIndex: "at", width: 170, render: (v: string) => formatTime(v) },
              { title: "Server", dataIndex: "child", width: 120, render: (v: string) => CHILD_LABEL[v] ?? v },
              { title: "Event", dataIndex: "event", width: 150, render: (v: string) => EXIT_LABEL[v] ?? v.replace(/-/g, " ") },
              { title: "Detail", dataIndex: "detail" },
            ]}
          />
        ) : (
          <Empty description="No events yet" />
        )}
      </Card>
    </Space>
  );
}

const ARCH_LABEL: Record<string, string> = { amd64: "x86-64", arm64: "ARM64", "386": "x86 (32-bit)", arm: "ARM (32-bit)" };
const OS_LABEL: Record<string, string> = { linux: "Linux", windows: "Windows", darwin: "macOS" };
const MANAGER_LABEL: Record<string, string> = {
  systemd: "systemd service",
  "task-scheduler": "Windows Task Scheduler",
  launchd: "launchd daemon",
  supervisor: "Supervisor (no boot service found)",
  kubernetes: "Kubernetes",
  none: "None: started by hand",
};

/** Plain-language findings about the host, worst first. */
export function platformFindings(p: PlatformData): { type: "warning" | "info"; text: string }[] {
  const out: { type: "warning" | "info"; text: string }[] = [];
  const { arch, cgroup, memory } = p.host;
  if (arch.emulated && arch.native) {
    out.push({
      type: "warning",
      text: `This ${ARCH_LABEL[arch.binary] ?? arch.binary} build runs under emulation on an ${ARCH_LABEL[arch.native] ?? arch.native} machine. Install the ${p.host.os}/${arch.native} package for better speed and memory use.`,
    });
  }
  if (cgroup.oom_kills) out.push({ type: "warning", text: `The container killed processes for memory ${cgroup.oom_kills} time(s). Raise its memory limit.` });
  if (memory.total_bytes && memory.available_bytes && memory.available_bytes / memory.total_bytes < 0.1) {
    out.push({ type: "warning", text: "Less than 10% of memory is available." });
  }
  if (p.host.service_manager === "none") {
    out.push({ type: "warning", text: "Nothing restarts the services after a crash or reboot. Install the boot service: ncc-orchestrator v2-install-service --now." });
  }
  return out;
}

export function PlatformPanel() {
  const q = useQuery({ queryKey: ["health", "platform"], queryFn: api.platform, staleTime: 15_000 });
  if (q.isLoading) return <Card className="page-card"><Skeleton active /></Card>;
  if (q.isError || !q.data) return <Alert type="error" showIcon title="Couldn't load platform details" description={String((q.error as Error)?.message ?? "")} />;
  const p = q.data;
  const { host, api_process: proc } = p;
  const findings = platformFindings(p);
  const limitSource = { GOMEMLIMIT: "set by GOMEMLIMIT", cgroup: "90% of the container limit", none: "none" }[proc.tuning?.memory_limit_source ?? "none"];
  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      <Typography.Paragraph type="secondary" style={{ margin: 0 }}>
        The machine NCC runs on and how the servers adapt to it. The platform checks (CPU emulation, open-file limit,
        clock sync, inodes, memory, download quarantine, container limits, boot service) are on{" "}
        <Link to={sectionHref("health", { tab: "checks" })}>Checks</Link>.
      </Typography.Paragraph>
      {findings.map((f) => (
        <Alert key={f.text} type={f.type} showIcon title={f.text} />
      ))}
      <Card size="small" className="page-card" title="Host">
        <Descriptions size="small" column={{ xs: 1, sm: 2, lg: 3 }}>
          <Descriptions.Item label="Operating system">{OS_LABEL[host.os] ?? host.os}</Descriptions.Item>
          <Descriptions.Item label="Build">{ARCH_LABEL[host.arch.binary] ?? host.arch.binary}</Descriptions.Item>
          <Descriptions.Item label="Machine">
            {host.arch.known ? (
              <>
                {ARCH_LABEL[host.arch.native ?? ""] ?? host.arch.native}{" "}
                {host.arch.emulated ? <Tag color="warning">emulated</Tag> : <Tag color="success">native</Tag>}
              </>
            ) : (
              "not detectable"
            )}
          </Descriptions.Item>
          <Descriptions.Item label="CPUs">
            {host.cpus} (Go uses {host.gomaxprocs})
          </Descriptions.Item>
          <Descriptions.Item label="Memory">
            {host.memory.total_bytes ? `${formatBytes(host.memory.available_bytes)} free of ${formatBytes(host.memory.total_bytes)}` : "—"}
          </Descriptions.Item>
          <Descriptions.Item label="Kept running by">{MANAGER_LABEL[host.service_manager] ?? host.service_manager}</Descriptions.Item>
          {host.cgroup.cgroup_version ? (
            <Descriptions.Item label="Container limits">
              {[host.cgroup.memory_limit_bytes ? `memory ${formatBytes(host.cgroup.memory_limit_bytes)}` : null, host.cgroup.cpu_limit ? `${host.cgroup.cpu_limit} CPUs` : null]
                .filter(Boolean)
                .join(", ") || "none"}
            </Descriptions.Item>
          ) : null}
          <Descriptions.Item label="Go">{host.go_version}</Descriptions.Item>
        </Descriptions>
      </Card>
      <Card size="small" className="page-card" title="API server process">
        <Descriptions size="small" column={{ xs: 1, sm: 2, lg: 3 }}>
          <Descriptions.Item label="Up for">{formatDuration(proc.uptime_seconds)}</Descriptions.Item>
          <Descriptions.Item label="Goroutines">{proc.goroutines}</Descriptions.Item>
          <Descriptions.Item label="Heap">{formatBytes(proc.heap_alloc_bytes)}</Descriptions.Item>
          <Descriptions.Item label="Memory from OS">{formatBytes(proc.sys_bytes)}</Descriptions.Item>
          <Descriptions.Item label="Memory limit">
            {proc.tuning?.memory_limit_bytes ? `${formatBytes(proc.tuning.memory_limit_bytes)} (${limitSource})` : "none"}
          </Descriptions.Item>
          {proc.open_files != null ? (
            <Descriptions.Item label="Open files">
              {proc.open_files}
              {proc.file_limit_hard ? ` of ${proc.file_limit_hard}` : ""}
            </Descriptions.Item>
          ) : proc.file_limit_hard ? (
            <Descriptions.Item label="Open-file limit">{proc.file_limit_hard}</Descriptions.Item>
          ) : null}
        </Descriptions>
      </Card>
    </Space>
  );
}
