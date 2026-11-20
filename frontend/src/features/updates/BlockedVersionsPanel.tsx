import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Button, Input, Select, Switch, Table, Tooltip } from "antd";
import type { ColumnsType } from "antd/es/table";
import { DeleteOutlined, PlusOutlined } from "@ant-design/icons";
import { api } from "../../api/client";
import type { BlockedUIVersion } from "../../api/types";
import { Panel, StatusBadge, type Tone } from "../../ui";

const SEVERITY_TONE: Record<string, Tone> = { high: "fail", medium: "warn", low: "neutral" };

const COMPONENT_LABEL: Record<string, string> = { ui: "Dashboard", "ui-server": "UI server", any: "Both" };

type Props = { onError: (e: unknown) => void };

/**
 * Dashboard versions the API refuses. The shipped part comes from scanning
 * every release for known vulnerabilities; the admin can add more. A refused
 * dashboard shows an error screen with code NCC_API_UI_VERSION_BLOCKED.
 */
export function BlockedVersionsPanel({ onError }: Props) {
  const [version, setVersion] = useState("");
  const [component, setComponent] = useState("ui");
  const [reason, setReason] = useState("");
  const policy = useQuery({ queryKey: ["ui-version-policy"], queryFn: () => api.uiVersionPolicy() });
  const save = useMutation({
    mutationFn: (payload: { block_unversioned: boolean; extra: { version: string; component?: string; reason: string }[] }) =>
      api.updateUIVersionPolicy(payload),
    onSuccess: () => {
      void policy.refetch();
      setVersion("");
      setReason("");
    },
    onError,
  });

  const data = policy.data;
  const extra = data?.extra ?? [];
  const blockUnversioned = data?.block_unversioned !== false;
  const persist = (nextExtra: BlockedUIVersion[], block = blockUnversioned) =>
    save.mutate({
      block_unversioned: block,
      extra: nextExtra.map((e) => ({ version: e.version, component: e.component, reason: e.reason })),
    });

  const rows: (BlockedUIVersion & { key: string })[] = [
    ...(data?.builtin ?? []).map((e) => ({ ...e, key: `b-${e.component}-${e.version}` })),
    ...extra.map((e) => ({ ...e, key: `x-${e.component}-${e.version}-${e.reason}` })),
  ];

  const columns: ColumnsType<BlockedUIVersion & { key: string }> = [
    { title: "Version", dataIndex: "version", width: 100, render: (v: string) => <span className="mono">{v}</span> },
    { title: "Applies to", key: "component", width: 110, render: (_, r) => COMPONENT_LABEL[r.component || "any"] ?? r.component },
    {
      title: "Severity",
      key: "severity",
      width: 100,
      render: (_, r) => (r.severity ? <StatusBadge tone={SEVERITY_TONE[r.severity] ?? "neutral"}>{r.severity}</StatusBadge> : <span className="audit-muted">—</span>),
    },
    {
      title: "Why",
      key: "why",
      render: (_, r) => {
        const adv = r.advisories ?? [];
        // Shipped entries carry advisory ids (the full text is in the tooltip);
        // entries an admin added carry their own reason.
        if (r.builtin && adv.length > 0) {
          return (
            <Tooltip title={r.reason}>
              <span className="blocked-adv mono">
                {adv.slice(0, 3).join(", ")}
                {adv.length > 3 ? ` +${adv.length - 3} more` : ""}
              </span>
            </Tooltip>
          );
        }
        return <span>{r.reason}</span>;
      },
    },
    {
      title: "Source",
      key: "source",
      width: 90,
      render: (_, r) => (r.builtin ? "Shipped" : "Added"),
    },
    {
      title: <span className="visually-hidden">Remove</span>,
      key: "remove",
      width: 52,
      render: (_, r) =>
        r.builtin ? null : (
          <Tooltip title="Remove from the list">
            <Button
              type="text"
              size="small"
              icon={<DeleteOutlined />}
              aria-label={`Remove ${r.version} from the blocked list`}
              disabled={save.isPending}
              onClick={() => persist(extra.filter((x) => !(x.version === r.version && x.component === r.component && x.reason === r.reason)))}
            />
          </Tooltip>
        ),
    },
  ];

  return (
    <Panel
      title="Blocked dashboard versions"
      description="Dashboards on this list are refused by the API and show an error screen with the code NCC_API_UI_VERSION_BLOCKED. The shipped part comes from scanning every release for known vulnerabilities."
    >
      <div className="blocked-policy">
        <Switch
          id="block-unversioned"
          size="small"
          checked={blockUnversioned}
          disabled={!data || save.isPending}
          onChange={(v) => persist(extra, v)}
          aria-label="Refuse dashboards that do not declare a version"
        />
        <label htmlFor="block-unversioned">
          Refuse dashboards that do not declare a version. Builds before 2.4.0 cannot, so this is what stops releases such as 2.2.1.
        </label>
      </div>

      <Table
        size="small"
        rowKey="key"
        columns={columns}
        dataSource={rows}
        loading={policy.isLoading}
        pagination={false}
        locale={{ emptyText: "No versions are blocked." }}
      />

      <div className="blocked-add">
        <Input placeholder="Version, e.g. 2.4.1" aria-label="Version to block" value={version} onChange={(e) => setVersion(e.target.value)} style={{ width: 150 }} />
        <Select
          aria-label="Applies to"
          value={component}
          onChange={setComponent}
          options={[
            { value: "ui", label: "Dashboard" },
            { value: "ui-server", label: "UI server" },
            { value: "any", label: "Both" },
          ]}
          style={{ width: 130 }}
        />
        <Input placeholder="Reason" aria-label="Reason" value={reason} onChange={(e) => setReason(e.target.value)} style={{ flex: 1, minWidth: 200 }} />
        <Button
          type="primary"
          icon={<PlusOutlined />}
          loading={save.isPending}
          disabled={!version.trim() || !reason.trim()}
          onClick={() => persist([...extra, { version: version.trim(), component, reason: reason.trim() }])}
        >
          Block version
        </Button>
      </div>
    </Panel>
  );
}
