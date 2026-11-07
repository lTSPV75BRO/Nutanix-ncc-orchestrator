import { useMemo, useState } from "react";
import { useRunsActive } from "../../hooks/useRunsActive";
import { Alert, App as AntApp, Button, Dropdown, Input, Popconfirm, Segmented, Space, Tag, Tooltip, Typography } from "antd";
import type { MenuProps } from "antd";
import { DownOutlined, ReloadOutlined, SearchOutlined } from "@ant-design/icons";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router";
import { sectionHref } from "../../navigation";
import { api } from "../../api/client";
import type { DiagnosticCheck, DiagnosticsData } from "../../api/types";
import { notify, notifyError } from "../../notify";
import { formatTime } from "../../utils/datetime";
import { useFeatureFlags } from "../features/useFeatureFlags";
import { ItemList, Panel, StatTile, StatusBadge, toneFor } from "../../ui";
import { ErrorStateCard, LoadingStateCard } from "../../components/UxStates";

const STATUS_META: Record<DiagnosticCheck["status"], { label: string }> = {
  ok: { label: "Passing" },
  warn: { label: "Warning" },
  fail: { label: "Failing" },
};

function CategoryLabel({ value }: { value: string }) {
  const pretty: Record<string, string> = {
    config: "Configuration",
    storage: "Storage",
    encryption: "Secrets",
    backups: "Backups",
    runs: "Runs",
    directory: "Directory",
    sso: "Single sign-on",
    tls: "HTTPS",
    process: "Processes",
    performance: "Performance",
    platform: "Platform",
    runtime: "Runtime",
    kubernetes: "Kubernetes",
    security: "Security",
    schedule: "Schedule",
  };
  return <>{pretty[value] ?? value.charAt(0).toUpperCase() + value.slice(1)}</>;
}

export function SystemHealthSection() {
  const navigate = useNavigate();
  const { modal } = AntApp.useApp();
  // Lightweight local filter state without introducing additional persisted prefs.
  const [severityFilter, setSeverityFilter] = useState<"fail_warn" | "fail" | "all">("fail_warn");
  const [searchText, setSearchText] = useState("");
  const health = useQuery({
    queryKey: ["health"],
    queryFn: api.health,
    staleTime: 30_000,
  });
  const isKubernetes = Boolean(
    (health.data as { runtime?: { kubernetes?: boolean } } | undefined)?.runtime?.kubernetes,
  );
  // Settings → Features "Self-heal actions": off leaves this page report-only.
  const healOn = useFeatureFlags().data?.self_heal !== false;
  // Readiness cards heal in place only on hosts with self-heal on; otherwise
  // they link to the details.
  const healsInline = healOn && !isKubernetes;
  const schedule = useQuery({
    queryKey: ["schedule-health"],
    queryFn: api.scheduleHealth,
    staleTime: 30_000,
  });
  const activeRun = useRunsActive();
  const backups = useQuery({
    queryKey: ["settings", "backups"],
    queryFn: api.listBackups,
    staleTime: 30_000,
  });
  const diag = useQuery({
    queryKey: ["health", "diagnostics"],
    queryFn: api.diagnostics,
    staleTime: 30_000,
  });

  const heal = useMutation({
    mutationFn: (payload?: { check_ids?: string[]; verify_after_fix?: boolean; no_disruptive?: boolean; allow_disruptive?: boolean }) =>
      api.healDiagnostics(payload),
    onSuccess: (data: DiagnosticsData) => {
      const fixed = data.checks.filter((c) => c.fixed).length;
      diag.refetch();
      if (fixed > 0) notify.success(`Fixed ${fixed} check${fixed === 1 ? "" : "s"}.`);
      else notify.info("Nothing needed fixing.");
    },
    onError: (e) => notifyError(e, "The fix didn't complete"),
  });
  const bundle = useMutation({
    mutationFn: api.createDiagnosticsSupportBundle,
    onSuccess: (d) => notify.success(`Support bundle saved to ${d.path}`),
    onError: (e) => notifyError(e, "Couldn't create the support bundle"),
  });

  const data = diag.data;
  const overall = data?.overall ?? "ok";
  const diagErrorMessage =
    diag.error && typeof diag.error === "object" && "message" in diag.error
      ? String(diag.error.message || "The server didn't answer.")
      : "The server didn't answer.";

  const filteredChecks = useMemo(() => {
    const checks = data?.checks ?? [];
    const search = searchText.trim().toLowerCase();
    return checks.filter((c) => {
      if (severityFilter === "fail" && c.status !== "fail") return false;
      if (severityFilter === "fail_warn" && !(c.status === "fail" || c.status === "warn")) return false;
      if (!search) return true;
      const haystack = `${c.id} ${c.title} ${c.category} ${c.message} ${c.hint ?? ""}`.toLowerCase();
      return haystack.includes(search);
    });
  }, [data?.checks, severityFilter, searchText]);

  const actionableSummary = data?.actionable;

  const remediationTarget = (category: string): string => {
    if (category === "config") return sectionHref("config");
    if (category === "backups") return sectionHref("updates", { tab: "backups" });
    if (category === "performance") return sectionHref("config");
    if (category === "platform") return sectionHref("connection", { tab: "platform" });
    if (category === "runs") return sectionHref("runs");
    if (category === "process") return sectionHref("connection");
    if (category === "directory" || category === "sso") return sectionHref("access");
    return sectionHref("health");
  };

  const readinessFixLabel = (key: string): string => {
    if (!healsInline && (key === "api" || key === "supervisor" || key === "runs")) return "Open";
    if (key === "api" || key === "supervisor" || key === "runs") return "Fix";
    if (key === "schedule") return "Set up";
    if (key === "backups") return "Back up";
    return "Open";
  };

  const runReadinessFix = (key: string, path: string) => {
    if (!healsInline && (key === "api" || key === "supervisor" || key === "runs")) {
      navigate(path);
      return;
    }
    if (key === "api" || key === "supervisor" || key === "runs") {
      const ids =
        key === "api"
          ? ["runtime-mode-drift", "stale-pids", "config-output-routing"]
          : key === "supervisor"
            ? ["runtime-mode-drift", "stale-pids", "selinux-exec-context"]
            : ["stale-pids", "runtime-mode-drift"];
      heal.mutate({ check_ids: ids, verify_after_fix: true, no_disruptive: true });
      return;
    }
    navigate(path);
  };

  const healSingleCheck = (check: DiagnosticCheck, allowDisruptive = false) => {
    if (check.status === "ok") {
      notify.info("This check is already passing.");
      return;
    }
    if (check.source !== "orchestrator") {
      notify.info("This check can't be fixed automatically.");
      return;
    }
    heal.mutate({
      check_ids: [check.id],
      verify_after_fix: true,
      no_disruptive: !allowDisruptive,
      allow_disruptive: allowDisruptive,
    });
  };

  const readinessChecks = useMemo(() => {
    const hasBackup = (backups.data?.backups?.length || 0) > 0;
    const apiOk = (health.data?.status || "") === "ok";
    const scheduleOk = !schedule.data?.error && Boolean(schedule.data?.configured || schedule.data?.installed);
    const noActiveRun = !activeRun.data?.active;
    const processChecks = (data?.checks ?? []).filter((c) => c.category === "process");
    const hasProcessChecks = processChecks.length > 0;
    const processFailures = processChecks.filter((c) => c.status === "fail").length;
    const processWarns = processChecks.filter((c) => c.status === "warn").length;
    const supervisorGateOk = isKubernetes
      ? apiOk
      : hasProcessChecks && processFailures === 0 && processWarns === 0;
    const supervisorLabel = isKubernetes
      ? "Kubernetes manages the processes"
      : !hasProcessChecks
        ? "Supervisor status unknown"
        : processFailures > 0
          ? `Supervisor: ${processFailures} failing`
          : processWarns > 0
            ? `Supervisor: ${processWarns} warning${processWarns === 1 ? "" : "s"}`
            : "Supervisor and services running";
    const processSnapshot = processChecks.map((c) => ({
      title: c.title,
      status: c.status,
      message: c.message,
    }));
    return [
      { key: "api", label: "API server answering", ok: apiOk, fix: sectionHref("connection") },
      {
        key: "supervisor",
        label: supervisorLabel,
        ok: supervisorGateOk,
        fix: sectionHref("health"),
        details: processSnapshot,
      },
      { key: "schedule", label: "Runs are scheduled", ok: scheduleOk, fix: sectionHref("schedule") },
      { key: "backups", label: "A backup exists", ok: hasBackup, fix: sectionHref("updates", { tab: "backups" }) },
      { key: "runs", label: "No run stuck in progress", ok: noActiveRun, fix: sectionHref("runs") },
    ];
  }, [
    backups.data?.backups?.length,
    health.data?.status,
    schedule.data?.error,
    schedule.data?.configured,
    schedule.data?.installed,
    activeRun.data?.active,
    data?.checks,
    isKubernetes,
  ]);

  const healableCheckIDs = useMemo(
    () =>
      (data?.checks ?? [])
        .filter((c) => c.source === "orchestrator" && c.status !== "ok" && !c.disruptive)
        .map((c) => c.id),
    [data?.checks],
  );
  const disruptiveHealableCheckIDs = useMemo(
    () =>
      (data?.checks ?? [])
        .filter((c) => c.source === "orchestrator" && c.status !== "ok" && c.disruptive)
        .map((c) => c.id),
    [data?.checks],
  );

  if (diag.isLoading && !data) {
    return (
      <div id="settings-health">
        <LoadingStateCard rows={8} />
      </div>
    );
  }

  if (diag.isError && !data) {
    return (
      <div id="settings-health">
        <ErrorStateCard title="Couldn't load the health checks" error={diagErrorMessage} onRetry={() => diag.refetch()} />
      </div>
    );
  }

  const problems = (data?.summary.fail ?? 0) + (data?.summary.warn ?? 0);
  const overallTone = (data?.summary.fail ?? 0) > 0 || overall === "fail" ? "fail" : problems > 0 || overall === "warn" ? "warn" : "ok";
  const overallLabel = overallTone === "fail" ? "Failing" : overallTone === "warn" ? "Needs attention" : "Healthy";
  const totalChecks = (data?.summary.ok ?? 0) + (data?.summary.warn ?? 0) + (data?.summary.fail ?? 0);
  const healMenu: MenuProps["items"] = [
    {
      key: "actionable",
      label: "Fix everything that can be fixed safely",
      disabled: healableCheckIDs.length === 0,
      onClick: () => heal.mutate({ check_ids: healableCheckIDs, verify_after_fix: true, no_disruptive: true }),
    },
    {
      key: "disruptive",
      danger: true,
      label: "Fix with service restarts…",
      disabled: disruptiveHealableCheckIDs.length === 0,
      onClick: () =>
        modal.confirm({
          title: "Fix with service restarts?",
          content: "These fixes may restart NCC services. Use a maintenance window. They're blocked while a run is in progress.",
          okText: "Fix and restart",
          okButtonProps: { danger: true },
          onOk: () =>
            heal.mutate({
              check_ids: disruptiveHealableCheckIDs,
              verify_after_fix: true,
              no_disruptive: false,
              allow_disruptive: true,
            }),
        }),
    },
    { type: "divider" },
    { key: "bundle", label: "Collect support bundle", onClick: () => bundle.mutate() },
  ];

  return (
    <div id="settings-health" className="health-checks">
      <div className="overview-runline">
        <div className="overview-runline-text">
        <StatusBadge tone={overallTone}>{overallLabel}</StatusBadge>
        <span>
          {problems === 0
            ? "No problems found."
            : `${problems} problem${problems === 1 ? "" : "s"} · ${actionableSummary?.auto_fixable ?? 0} can be fixed automatically`}
          {data?.generated_at ? ` · checked ${formatTime(data.generated_at)}` : ""}
          {data?.auto_fix_loop ? " · automatic healing is on" : ""}
        </span>
        </div>
        <div className="overview-runline-actions">
          <Button icon={<ReloadOutlined />} onClick={() => diag.refetch()} loading={diag.isFetching}>
            Check again
          </Button>
          {healOn ? (
            <Space.Compact>
              <Popconfirm
                title="Apply safe fixes?"
                description="Only fixes that don't restart anything: missing folders, file permissions, config repairs, the self-signed certificate, log rotation, backups and leftover temporary files."
                okText="Fix"
                onConfirm={() => heal.mutate({ verify_after_fix: true, no_disruptive: true })}
              >
                <Button type="primary" loading={heal.isPending}>
                  Fix problems
                </Button>
              </Popconfirm>
              <Dropdown menu={{ items: healMenu }} trigger={["click"]}>
                <Button type="primary" icon={<DownOutlined />} aria-label="More fix options" />
              </Dropdown>
            </Space.Compact>
          ) : (
            <Button onClick={() => bundle.mutate()} loading={bundle.isPending}>
              Collect support bundle
            </Button>
          )}
        </div>
      </div>

      <div className="overview-stats">
        <StatTile label="Checks" value={totalChecks.toLocaleString()} />
        <StatTile label="Failing" value={data?.summary.fail ?? 0} tone={(data?.summary.fail ?? 0) > 0 ? "fail" : undefined} />
        <StatTile label="Warnings" value={data?.summary.warn ?? 0} tone={(data?.summary.warn ?? 0) > 0 ? "warn" : undefined} />
        <StatTile label="Passing" value={data?.summary.ok ?? 0} tone="ok" />
      </div>

      {data?.fix_history?.count ? (
        <Alert
          type={data.verified_stable ? "success" : "info"}
          showIcon
          title={`The last fix repaired ${data.fix_history.count} check${data.fix_history.count === 1 ? "" : "s"}${
            data.verified_stable ? " and they stayed fixed" : "; some still need attention"
          }`}
          description={
            <>
              {(data.fix_history.fixed_titles || data.fix_history.fixed_ids || []).join(", ")}
              {data.guardrails?.active_run_guard ? " Fixes that restart services waited because a run was in progress." : ""}
              {(data.actionable?.disruptive_skipped ?? 0) > 0
                ? ` ${data.actionable?.disruptive_skipped} fix${data.actionable?.disruptive_skipped === 1 ? "" : "es"} need a service restart; use "Fix with service restarts".`
                : ""}
            </>
          }
        />
      ) : null}
      {data?.orchestrator_error ? (
        <Alert type="warning" showIcon title="Some checks couldn't run" description={data.orchestrator_error} />
      ) : null}
      {heal.error ? (
        <Alert
          type="error"
          showIcon
          title="The fix didn't complete"
          description={heal.error instanceof Error ? heal.error.message : undefined}
        />
      ) : null}

      <Panel title="Readiness" description="What this installation needs before it can be relied on." flush>
        <ItemList
          items={readinessChecks}
          itemKey={(c) => c.key}
          render={(c) => (
            <Space size={12} align="center" wrap>
              <StatusBadge tone={c.ok ? "ok" : "fail"}>{c.ok ? "Ready" : "Not ready"}</StatusBadge>
              <Typography.Text>{c.label}</Typography.Text>
            </Space>
          )}
          actions={(c) =>
            c.ok ? null : (
              <Button
                size="small"
                loading={healsInline && (c.key === "api" || c.key === "supervisor" || c.key === "runs") ? heal.isPending : false}
                onClick={() => runReadinessFix(c.key, c.fix)}
              >
                {readinessFixLabel(c.key)}
              </Button>
            )
          }
        />
      </Panel>

      <Panel
        title="Checks"
        description={
          isKubernetes
            ? "Configuration, shared storage, secrets, backups, runs, sign-in and HTTPS. Kubernetes manages the processes."
            : "Configuration, storage, secrets, backups, runs, performance, sign-in, HTTPS and processes. Every fix is checked again."
        }
        extra={
          <Space wrap size={8}>
            <Segmented
              value={severityFilter}
              onChange={(v) => setSeverityFilter(v as "fail_warn" | "fail" | "all")}
              options={[
                { label: "Problems", value: "fail_warn" },
                { label: "Failing", value: "fail" },
                { label: "All", value: "all" },
              ]}
            />
            <Input
              allowClear
              placeholder="Search checks"
              prefix={<SearchOutlined />}
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              className="health-check-search"
            />
          </Space>
        }
        flush
      >
        <ItemList
          items={filteredChecks}
          itemKey={(c) => `${c.source}:${c.id}`}
          empty={severityFilter === "all" && !searchText ? "No checks reported." : "Nothing matches. Everything here is passing."}
          render={(c) => (
            <div className="health-check">
              <Space size={8} align="center" wrap>
                <StatusBadge tone={toneFor(c.status)}>{STATUS_META[c.status]?.label ?? c.status}</StatusBadge>
                <Typography.Text strong>{c.title}</Typography.Text>
                <Tag>
                  <CategoryLabel value={c.category} />
                </Tag>
                {c.disruptive ? <Tag color="warning">Restarts services</Tag> : null}
                {c.recurring ? (
                  <Tooltip title="Fixed several times in 24 hours: something keeps undoing it.">
                    <Tag color="error">Keeps coming back</Tag>
                  </Tooltip>
                ) : null}
                {c.fixed ? (
                  <Tag color={c.verified === false ? "warning" : "success"}>
                    {c.verified === false ? "Fixed, still failing" : "Fixed"}
                  </Tag>
                ) : null}
              </Space>
              <Typography.Paragraph type="secondary" className="health-check-text">
                {c.fixed && c.fix_message ? c.fix_message : c.message}
                {!c.fixed && c.hint ? ` ${c.hint}` : ""}
              </Typography.Paragraph>
            </div>
          )}
          actions={(c) =>
            c.status === "ok" ? null : (
              <Space size={8}>
                {healOn && c.source === "orchestrator" ? (
                  c.disruptive ? (
                    <Popconfirm
                      title="Fix with a service restart?"
                      description="This fix may restart NCC services. It's blocked while a run is in progress."
                      okText="Fix and restart"
                      okButtonProps={{ danger: true }}
                      onConfirm={() => healSingleCheck(c, true)}
                    >
                      <Button size="small" danger loading={heal.isPending}>
                        Fix
                      </Button>
                    </Popconfirm>
                  ) : (
                    <Button size="small" loading={heal.isPending} onClick={() => healSingleCheck(c, false)}>
                      Fix
                    </Button>
                  )
                ) : null}
                <Link to={remediationTarget(c.category)}>
                  <Button size="small">Open</Button>
                </Link>
              </Space>
            )
          }
        />
      </Panel>
    </div>
  );
}
