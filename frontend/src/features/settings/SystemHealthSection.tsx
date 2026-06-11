import { useMemo } from "react";
import {
  Alert,
  Button,
  Card,
  Empty,
  Popconfirm,
  Skeleton,
  Space,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  ExclamationCircleOutlined,
  ReloadOutlined,
  ThunderboltOutlined,
} from "@ant-design/icons";
import { useMutation, useQuery } from "@tanstack/react-query";
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
  const diag = useQuery({
    queryKey: ["health", "diagnostics"],
    queryFn: api.diagnostics,
    staleTime: 30_000,
  });

  const heal = useMutation({
    mutationFn: api.healDiagnostics,
    onSuccess: (data: DiagnosticsData) => {
      const fixed = data.checks.filter((c) => c.fixed).length;
      diag.refetch();
      if (fixed > 0) notify.success(`Applied ${fixed} safe remediation${fixed === 1 ? "" : "s"}.`);
      else notify.info("Self-heal ran; nothing needed fixing.");
    },
    onError: (e) => notifyError(e, "Self-heal failed"),
  });

  const data = diag.data;
  const overall = data?.overall ?? "ok";
  const overallMeta = STATUS_META[overall] ?? STATUS_META.ok;

  const grouped = useMemo(() => {
    const out: Record<string, DiagnosticCheck[]> = {};
    for (const c of data?.checks ?? []) {
      (out[c.category] ??= []).push(c);
    }
    return out;
  }, [data]);

  if (diag.isLoading && !data) {
    return (
      <Card className="page-card">
        <Skeleton active paragraph={{ rows: 8 }} />
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
              description="Runs the self-heal fixers: anchor relative output paths, create missing dirs, tighten secret-file perms, repair config, renew the self-signed TLS cert, rotate oversized logs, and verify/auto-take a backup. No destructive actions."
              okText="Heal now"
              onConfirm={() => heal.mutate()}
            >
              <Button type="primary" icon={<ThunderboltOutlined />} loading={heal.isPending}>
                Heal now
              </Button>
            </Popconfirm>
          </Space>
        </div>
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
              {checks.map((c) => (
                <CheckRow key={`${c.source}:${c.id}`} check={c} />
              ))}
            </Space>
          </Card>
        ))
      )}
    </Space>
  );
}
