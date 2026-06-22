import { useMemo, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Empty,
  Popconfirm,
  Progress,
  Row,
  Skeleton,
  Segmented,
  Space,
  Statistic,
  Tag,
  Tooltip,
  Typography,
  Col,
} from "antd";
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  ExclamationCircleOutlined,
  ReloadOutlined,
  ThunderboltOutlined,
} from "@ant-design/icons";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { api } from "../../api/client";
import type { DiagnosticCheck, DiagnosticsData } from "../../api/types";
import { notify, notifyError } from "../../notify";
import { formatTime } from "../../utils/datetime";

const STATUS_META: Record<
  DiagnosticCheck["status"],
  { color: string; icon: React.ReactNode; label: string }
> = {
  ok: { color: "success", icon: <CheckCircleOutlined />, label: "OK" },
  warn: { color: "warning", icon: <ExclamationCircleOutlined />, label: "Warning" },
  fail: { color: "error", icon: <CloseCircleOutlined />, label: "Failing" },
};

function CategoryLabel({ value }: { value: string }) {
  const pretty: Record<string, string> = {
    config: "Configuration",
    storage: "Storage",
    encryption: "Secrets",
    backups: "Backups",
    runs: "Runs",
    directory: "Directory (AD/LDAP)",
    sso: "SSO (SAML)",
    tls: "TLS",
    process: "Processes",
  };
  return <>{pretty[value] ?? value}</>;
}

function CheckRow({ check }: { check: DiagnosticCheck }) {
  const meta = STATUS_META[check.status] ?? STATUS_META.warn;
  return (
    <div className={`health-check-row health-check-${check.status}`}>
      <div className="health-check-head">
        <Space size={8} align="center">
          <span className={`health-check-icon ${check.status}`}>{meta.icon}</span>
          <Typography.Text strong>{check.title}</Typography.Text>
          <Tag bordered={false}>
            <CategoryLabel value={check.category} />
          </Tag>
          <Tooltip title={check.source === "api" ? "Live probe by the API server" : "Checked by the orchestrator doctor"}>
            <Tag bordered={false} color={check.source === "api" ? "geekblue" : "default"}>
              {check.source}
            </Tag>
          </Tooltip>
          {check.fixed ? (
            <Tag color="success" bordered={false}>
              auto-fixed
            </Tag>
          ) : null}
        </Space>
        <Tag color={meta.color}>{meta.label}</Tag>
      </div>
      <Typography.Paragraph type="secondary" style={{ margin: "4px 0 0 28px" }}>
        {check.message}
      </Typography.Paragraph>
      {check.fixed && check.fix_message ? (
        <Typography.Paragraph style={{ margin: "0 0 0 28px", color: "var(--menu-selected-border, #1677ff)" }}>
          Fixed: {check.fix_message}
        </Typography.Paragraph>
      ) : null}
      {!check.fixed && check.hint ? (
        <Typography.Paragraph type="secondary" style={{ margin: "0 0 0 28px", fontStyle: "italic" }}>
          {check.hint}
        </Typography.Paragraph>
      ) : null}
    </div>
  );
}

export function SystemHealthSection() {
  const navigate = useNavigate();
  // Lightweight local filter state without introducing additional persisted prefs.
  const [severityFilter, setSeverityFilter] = useState<"fail_warn" | "fail" | "all">("all");
  const health = useQuery({
    queryKey: ["health"],
    queryFn: api.health,
    staleTime: 30_000,
  });
  const schedule = useQuery({
    queryKey: ["schedule-health"],
    queryFn: api.scheduleHealth,
    staleTime: 30_000,
  });
  const activeRun = useQuery({
    queryKey: ["runs-active"],
    queryFn: api.runActive,
    staleTime: 5_000,
    refetchInterval: 10_000,
  });
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
    mutationFn: (payload?: { check_ids?: string[]; verify_after_fix?: boolean; no_disruptive?: boolean }) =>
      api.healDiagnostics(payload),
    onSuccess: (data: DiagnosticsData) => {
      const fixed = data.checks.filter((c) => c.fixed).length;
      diag.refetch();
      if (fixed > 0) notify.success(`Applied ${fixed} safe remediation${fixed === 1 ? "" : "s"}.`);
      else notify.info("Self-heal ran; nothing needed fixing.");
    },
    onError: (e) => notifyError(e, "Self-heal failed"),
  });
  const bundle = useMutation({
    mutationFn: api.createDiagnosticsSupportBundle,
    onSuccess: (d) => notify.success(`Support bundle generated: ${d.path}`),
    onError: (e) => notifyError(e, "Support bundle generation failed"),
  });

  const data = diag.data;
  const overall = data?.overall ?? "ok";
  const overallMeta = STATUS_META[overall] ?? STATUS_META.ok;
  const diagErrorMessage =
    diag.error && typeof diag.error === "object" && "message" in diag.error
      ? String(diag.error.message || "Failed to load diagnostics")
      : "Failed to load diagnostics";

  const grouped = useMemo(() => {
    const out: Record<string, DiagnosticCheck[]> = {};
    for (const c of data?.checks ?? []) {
      (out[c.category] ??= []).push(c);
    }
    return out;
  }, [data]);

  const filteredChecks = useMemo(() => {
    const checks = data?.checks ?? [];
    if (severityFilter === "all") return checks;
    if (severityFilter === "fail") return checks.filter((c) => c.status === "fail");
    return checks.filter((c) => c.status === "fail" || c.status === "warn");
  }, [data?.checks, severityFilter]);

  const remediationTarget = (category: string): string => {
    if (category === "config") return "/settings?tab=config";
    if (category === "backups") return "/settings?tab=access";
    if (category === "runs") return "/settings?tab=runs";
    if (category === "process") return "/settings?tab=connection";
    if (category === "directory" || category === "sso") return "/settings?tab=access";
    return "/settings?tab=health";
  };

  const readinessFixLabel = (key: string): string => {
    if (key === "api" || key === "supervisor" || key === "runs") return "Heal now";
    if (key === "schedule") return "Open Schedule";
    if (key === "backups") return "Open Backups";
    return "Open Fix Area";
  };

  const runReadinessFix = (key: string, path: string) => {
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

  const healSingleCheck = (check: DiagnosticCheck) => {
    if (check.source !== "orchestrator") {
      notify.info("This check is probe-only and cannot be auto-fixed.");
      return;
    }
    heal.mutate({ check_ids: [check.id], verify_after_fix: true, no_disruptive: true });
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
    const supervisorGateOk = hasProcessChecks && processFailures === 0 && processWarns === 0;
    const supervisorLabel =
      !hasProcessChecks
        ? "Supervisor/service status unavailable"
        : processFailures > 0
          ? `Supervisor/process checks failing (${processFailures})`
          : processWarns > 0
            ? `Supervisor/process checks warning (${processWarns})`
            : "Supervisor and managed services healthy";
    const processSnapshot = processChecks.map((c) => ({
      title: c.title,
      status: c.status,
      message: c.message,
    }));
    return [
      { key: "api", label: "API health", ok: apiOk, fix: "/settings?tab=connection" },
      {
        key: "supervisor",
        label: supervisorLabel,
        ok: supervisorGateOk,
        fix: "/settings?tab=health",
        details: processSnapshot,
      },
      { key: "schedule", label: "Schedule configured", ok: scheduleOk, fix: "/settings?tab=schedule" },
      { key: "backups", label: "Backups available", ok: hasBackup, fix: "/settings?tab=access" },
      { key: "runs", label: "No active long-running run", ok: noActiveRun, fix: "/settings?tab=runs" },
    ];
  }, [
    backups.data?.backups?.length,
    health.data?.status,
    schedule.data?.error,
    schedule.data?.configured,
    schedule.data?.installed,
    activeRun.data?.active,
    data?.checks,
  ]);
  const readinessPass = readinessChecks.filter((c) => c.ok).length;
  const readinessPct = Math.round((readinessPass / Math.max(1, readinessChecks.length)) * 100);
  const confidenceScore = useMemo(() => {
    const checks = data?.checks ?? [];
    if (!checks.length) return 100;
    const w = (s: DiagnosticCheck["status"]) => (s === "ok" ? 1 : s === "warn" ? 0.5 : 0);
    const score = checks.reduce((acc, c) => acc + w(c.status), 0);
    return Math.round((score / checks.length) * 100);
  }, [data?.checks]);

  if (diag.isLoading && !data) {
    return (
      <Card className="page-card">
        <Skeleton active paragraph={{ rows: 8 }} />
      </Card>
    );
  }

  if (diag.isError && !data) {
    return (
      <Card className="page-card">
        <Space direction="vertical" size={12} style={{ width: "100%" }}>
          <Alert
            type="error"
            showIcon
            message="Unable to load system health diagnostics"
            description={diagErrorMessage}
          />
          <Space>
            <Button icon={<ReloadOutlined />} onClick={() => diag.refetch()} loading={diag.isFetching}>
              Retry
            </Button>
            <Button href="/login">Re-login</Button>
          </Space>
        </Space>
      </Card>
    );
  }

  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      <Card className="page-card">
        <div className="health-overview-head">
          <Space size={16} align="start">
            <div className={`connection-orb ${overall === "ok" ? "ok" : "err"}`}>{overallMeta.icon}</div>
            <div>
              <Typography.Title level={4} style={{ margin: 0 }}>
                System Health
              </Typography.Title>
              <Typography.Text type="secondary">
                Self-heal checks across configuration, storage, secrets, backups, runs, directory/SSO, TLS, and
                processes. Safe remediations can be applied with one click.
              </Typography.Text>
              <div style={{ marginTop: 8 }}>
                <Space size={[8, 8]} wrap>
                  <Tag color="success">{data?.summary.ok ?? 0} OK</Tag>
                  <Tag color="warning">{data?.summary.warn ?? 0} warnings</Tag>
                  <Tag color="error">{data?.summary.fail ?? 0} failing</Tag>
                  {data?.auto_fix_loop ? (
                    <Tooltip title="A background self-heal loop is applying safe fixes on a timer.">
                      <Tag color="processing">auto-heal loop on</Tag>
                    </Tooltip>
                  ) : null}
                  {data?.generated_at ? (
                    <Tag>checked {formatTime(data.generated_at)}</Tag>
                  ) : null}
                </Space>
              </div>
            </div>
          </Space>
          <Space size={8} wrap>
            <Button icon={<ReloadOutlined />} onClick={() => diag.refetch()} loading={diag.isFetching}>
              Re-scan
            </Button>
            <Popconfirm
              title="Apply safe remediations?"
              description="Runs non-disruptive self-heal fixers (no restart/stop actions): anchor relative output paths, create missing dirs, tighten secret-file perms, repair config, renew the self-signed TLS cert, rotate oversized logs, and verify/auto-take a backup."
              okText="Heal now"
              onConfirm={() => heal.mutate({ verify_after_fix: true, no_disruptive: true })}
            >
              <Button type="primary" icon={<ThunderboltOutlined />} loading={heal.isPending}>
                Heal now
              </Button>
            </Popconfirm>
            <Button onClick={() => bundle.mutate()} loading={bundle.isPending}>
              Collect support bundle
            </Button>
          </Space>
        </div>
        {data?.fix_history?.count ? (
          <Alert
            style={{ marginTop: 12 }}
            type={data.verified_stable ? "success" : "info"}
            showIcon
            message={`Last heal fixed ${data.fix_history.count} check(s)`}
            description={
              <Space direction="vertical" size={2}>
                <Typography.Text type="secondary">
                  {(data.fix_history.fixed_titles || data.fix_history.fixed_ids || []).join(", ")}
                </Typography.Text>
                <Typography.Text type="secondary">
                  Verification: {data.verification_runs ?? 0} re-scan(s), {data.verified_stable ? "stable" : "follow-up still needed"}
                </Typography.Text>
                {data.guardrails?.active_run_guard ? (
                  <Typography.Text type="warning">Disruptive fixes were deferred due to active runs.</Typography.Text>
                ) : null}
              </Space>
            }
          />
        ) : null}
        {data?.orchestrator_error ? (
          <Alert
            style={{ marginTop: 12 }}
            type="warning"
            showIcon
            message="Orchestrator self-heal checks unavailable"
            description={data.orchestrator_error}
          />
        ) : null}
      </Card>

      <Card className="page-card" size="small">
        <Space direction="vertical" size={12} style={{ width: "100%" }}>
          <Space align="center" style={{ justifyContent: "space-between", width: "100%" }}>
            <Typography.Title level={5} style={{ margin: 0 }}>
              Release Readiness Gates
            </Typography.Title>
            <Tag color={readinessPass === readinessChecks.length ? "success" : "warning"}>
              {readinessPass}/{readinessChecks.length} pass
            </Tag>
          </Space>
          <Progress percent={readinessPct} size="small" status={readinessPass === readinessChecks.length ? "success" : "active"} />
          <Row gutter={[12, 12]}>
            {readinessChecks.map((c) => (
              <Col xs={24} md={12} lg={8} key={c.key}>
                <Card size="small" className="health-gate-card">
                  <Space direction="vertical" size={8} style={{ width: "100%" }}>
                    <Space align="center" style={{ justifyContent: "space-between", width: "100%" }}>
                      <Typography.Text strong>{c.label}</Typography.Text>
                      <Tag color={c.ok ? "success" : "error"}>{c.ok ? "pass" : "fail"}</Tag>
                    </Space>
                    {c.key === "supervisor" && Array.isArray((c as { details?: Array<{ title: string; status: string; message: string }> }).details) ? (
                      <Space direction="vertical" size={4} style={{ width: "100%" }}>
                        {(c as { details?: Array<{ title: string; status: string; message: string }> }).details?.map((d) => (
                          <Space key={`${d.title}-${d.status}`} size={6} wrap>
                            <Tag color={d.status === "ok" ? "success" : d.status === "warn" ? "warning" : "error"}>
                              {d.title}
                            </Tag>
                            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                              {d.message}
                            </Typography.Text>
                          </Space>
                        ))}
                      </Space>
                    ) : null}
                    <Button
                      size="small"
                      icon={c.key === "api" || c.key === "supervisor" || c.key === "runs" ? <ThunderboltOutlined /> : undefined}
                      loading={(c.key === "api" || c.key === "supervisor" || c.key === "runs") ? heal.isPending : false}
                      onClick={() => runReadinessFix(c.key, c.fix)}
                    >
                      {readinessFixLabel(c.key)}
                    </Button>
                  </Space>
                </Card>
              </Col>
            ))}
          </Row>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            This section summarizes release gates; detailed root-cause diagnostics remain below.
          </Typography.Text>
        </Space>
      </Card>

      <Card className="page-card" size="small">
        <Space direction="vertical" size={12} style={{ width: "100%" }}>
          <Space align="center" style={{ justifyContent: "space-between", width: "100%" }} wrap>
            <Typography.Title level={5} style={{ margin: 0 }}>
              Guided Remediation
            </Typography.Title>
            <Space wrap>
              <Segmented
                size="small"
                value={severityFilter}
                onChange={(v) => setSeverityFilter(v as "fail_warn" | "fail" | "all")}
                options={[
                  { label: "Fail + Warn", value: "fail_warn" },
                  { label: "Fail only", value: "fail" },
                  { label: "All", value: "all" },
                ]}
              />
              <Button icon={<ReloadOutlined />} size="small" onClick={() => diag.refetch()} loading={diag.isFetching}>
                Refresh
              </Button>
            </Space>
          </Space>

          {(filteredChecks.length ?? 0) === 0 ? (
            <Alert type="success" showIcon message="No actionable issues. System is healthy." />
          ) : (
            <Space direction="vertical" size={10} style={{ width: "100%" }}>
              {filteredChecks.map((c) => (
                <Card key={`guide-${c.source}-${c.id}`} size="small" className="health-gate-card">
                  <Space direction="vertical" size={8} style={{ width: "100%" }}>
                    <Space wrap style={{ justifyContent: "space-between", width: "100%" }}>
                      <Space wrap>
                        <Tag color={c.status === "fail" ? "error" : c.status === "warn" ? "warning" : "success"}>
                          {c.status === "fail" ? "Failing" : c.status === "warn" ? "Warning" : "Healthy"}
                        </Tag>
                        <Tag bordered={false}>
                          <CategoryLabel value={c.category} />
                        </Tag>
                        <Typography.Text strong>{c.title}</Typography.Text>
                      </Space>
                    </Space>
                    <Typography.Text type="secondary">{c.message}</Typography.Text>
                    {c.hint ? <Typography.Text type="secondary">{c.hint}</Typography.Text> : null}
                    <Space wrap>
                      <Link to={remediationTarget(c.category)}>
                        <Button size="small">Open Fix Area</Button>
                      </Link>
                      <Button
                        size="small"
                        icon={<ThunderboltOutlined />}
                        loading={heal.isPending}
                        onClick={() => healSingleCheck(c)}
                        disabled={c.source !== "orchestrator"}
                      >
                        Heal this check
                      </Button>
                    </Space>
                  </Space>
                </Card>
              ))}
            </Space>
          )}
        </Space>
      </Card>

      <Card className="page-card" size="small">
        <Row gutter={[12, 12]}>
          <Col xs={12} md={6}>
            <Statistic title="Checks" value={(data?.checks?.length ?? 0).toLocaleString()} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title="Failing" value={data?.summary.fail ?? 0} valueStyle={{ color: "#f43f5e" }} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title="Warnings" value={data?.summary.warn ?? 0} valueStyle={{ color: "#f59e0b" }} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title="OK" value={data?.summary.ok ?? 0} valueStyle={{ color: "#22c55e" }} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic
              title="Confidence"
              value={confidenceScore}
              suffix="%"
              valueStyle={{ color: confidenceScore >= 90 ? "#22c55e" : confidenceScore >= 70 ? "#f59e0b" : "#f43f5e" }}
            />
          </Col>
        </Row>
      </Card>

      {(data?.checks?.length ?? 0) === 0 ? (
        <Card className="page-card">
          <Empty description="No diagnostics available" />
        </Card>
      ) : (
        Object.entries(grouped).map(([category, checks]) => (
          <Card key={category} className="page-card" size="small">
            <Typography.Title level={5} style={{ marginTop: 0 }}>
              <CategoryLabel value={category} />
            </Typography.Title>
            <Space direction="vertical" size={10} style={{ width: "100%" }}>
              {checks
                .filter((c) => filteredChecks.some((fc) => fc.id === c.id && fc.source === c.source))
                .map((c) => (
                <CheckRow key={`${c.source}:${c.id}`} check={c} />
                ))}
            </Space>
          </Card>
        ))
      )}
    </Space>
  );
}
