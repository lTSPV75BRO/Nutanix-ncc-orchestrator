import { useMemo } from "react";
import { Alert, Button, Card, Collapse, Popconfirm, Skeleton, Space, Table, Tag, Typography } from "antd";
import { ArrowRightOutlined, SwapOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router";
import { parseDocument } from "yaml";
import { api } from "../../api/client";
import type { ConfigMigrationResult } from "../../api/types";
import { sectionHref } from "../../navigation";
import { notify, notifyError } from "../../notify";
import { readText } from "../settings/configDoc";

// Updates & backups → Migrations: config file layout and plain-text secrets.

const LAYOUT_LABEL: Record<ConfigMigrationResult["layout"], string> = {
  empty: "Empty file",
  flat: "Flat layout",
  nested: "Nested layout",
  mixed: "Mixed layout",
};

/** Secret settings that should be secret:// references rather than plain text. */
const SECRET_KEYS = ["password", "smtp-password", "webhook-secret", "slack-webhook-url"] as const;

export function plaintextSecrets(content: string): string[] {
  const doc = parseDocument(content || "");
  return SECRET_KEYS.filter((key) => {
    const v = readText(doc, key).trim();
    return v !== "" && !v.startsWith("secret://");
  });
}

function ConfigLayoutCard() {
  const qc = useQueryClient();
  const preview = useQuery({ queryKey: ["health", "migrations", "config"], queryFn: api.previewConfigMigration, staleTime: 15_000 });
  const apply = useMutation({
    mutationFn: api.applyConfigMigration,
    onSuccess: (d) => {
      notify.success(`${d.result.message}. The original is saved as ${d.result.backup ?? "a .bak file"}.`);
      void qc.invalidateQueries({ queryKey: ["health"] });
    },
    onError: (e) => notifyError(e, "Migration failed; nothing was changed"),
  });
  const res = preview.data?.result;
  const moved = res?.moved ?? [];
  const dups = res?.duplicates ?? [];
  return (
    <Card className="page-card" size="small" title={<span><SwapOutlined /> Config file layout</span>}>
      {preview.isLoading ? <Skeleton active paragraph={{ rows: 3 }} /> : null}
      {preview.isError ? (
        <Alert type="error" showIcon title="Couldn't check the config file" description={String((preview.error as Error)?.message ?? "")} />
      ) : null}
      {preview.data?.error ? <Alert type="error" showIcon title="Migration is not possible" description={preview.data.error} /> : null}
      {res ? (
        <Space orientation="vertical" size={10} style={{ width: "100%" }}>
          <Space wrap>
            <Tag>{LAYOUT_LABEL[res.layout] ?? res.layout}</Tag>
            <Typography.Text type="secondary" className="mono">{res.config_path}</Typography.Text>
          </Space>
          {!res.changed ? (
            <Alert type="success" showIcon title="Nothing to migrate" description="The config file already uses the nested schema-version: 1 layout and sets each setting once." />
          ) : (
            <>
              <Typography.Paragraph style={{ margin: 0 }}>
                Moves {moved.length} flat setting{moved.length === 1 ? "" : "s"} to the nested layout
                {dups.length ? ` and removes ${dups.length} duplicate spelling${dups.length === 1 ? "" : "s"}` : ""}.
                Runs behave exactly the same: the settings the runner reads are compared before and after, and nothing is
                written if any would change. The original file is kept as a .bak.
              </Typography.Paragraph>
              {dups.filter((d) => d.differs).map((d) => (
                <Alert
                  key={d.flat}
                  type="warning"
                  showIcon
                  title={`${d.flat} is set twice with different values`}
                  description={`The runner uses ${d.winner} = ${d.value || '""'}; ignored: ${(d.shadowed_values ?? []).join(", ")}. The ignored spelling is removed.`}
                />
              ))}
              {moved.length ? (
                <Table
                  className="repair-moves"
                  tableLayout="fixed"
                  size="small"
                  rowKey="from"
                  pagination={moved.length > 10 ? { pageSize: 10, size: "small" } : false}
                  dataSource={moved}
                  columns={[
                    { title: "Flat key", dataIndex: "from", render: (v: string) => <code>{v}</code> },
                    { title: "", width: 32, render: () => <ArrowRightOutlined /> },
                    { title: "Nested path", dataIndex: "to", render: (v: string) => <code>{v}</code> },
                  ]}
                />
              ) : null}
              {res.kept_flat?.length ? (
                <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                  Stay at the top level (no nested spelling): {res.kept_flat.join(", ")}
                </Typography.Text>
              ) : null}
              {res.content ? (
                <Collapse
                  size="small"
                  items={[{ key: "p", label: "Preview the new file", children: <pre className="repair-preview">{res.content}</pre> }]}
                />
              ) : null}
              <Space>
                <Popconfirm
                  title="Migrate config.yaml to the nested layout?"
                  description="The file is rewritten in place; the original is kept as a .bak next to it."
                  okText="Migrate"
                  onConfirm={() => apply.mutate()}
                >
                  <Button type="primary" icon={<SwapOutlined />} loading={apply.isPending}>
                    Migrate to nested layout
                  </Button>
                </Popconfirm>
                <Button onClick={() => void preview.refetch()} loading={preview.isFetching}>
                  Check again
                </Button>
              </Space>
            </>
          )}
        </Space>
      ) : null}
    </Card>
  );
}

function SecretsCard() {
  const cfg = useQuery({ queryKey: ["health", "migrations", "secrets"], queryFn: () => api.loadConfig(), staleTime: 15_000 });
  const plain = useMemo(() => plaintextSecrets(cfg.data?.content ?? ""), [cfg.data?.content]);
  return (
    <Card className="page-card" size="small" title="Secrets in the config file">
      {cfg.isLoading ? <Skeleton active paragraph={{ rows: 1 }} /> : null}
      {cfg.data ? (
        plain.length === 0 ? (
          <Alert type="success" showIcon title="No plain-text secrets" description="Passwords and webhook secrets are empty or secret:// references." />
        ) : (
          <Space orientation="vertical" size={8} style={{ width: "100%" }}>
            <Alert
              type="warning"
              showIcon
              title={`${plain.length} secret${plain.length === 1 ? " is" : "s are"} stored as plain text`}
              description={plain.map((k) => <code key={k} style={{ marginRight: 6 }}>{k}</code>)}
            />
            <Space wrap>
              {plain.includes("password") ? (
                <Link to={`${sectionHref("config")}?open=secrets`}>
                  <Button type="primary">Move the cluster password to a secret</Button>
                </Link>
              ) : null}
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                Others: set them to secret://NAME under Clusters & config → Notifications and add NAME to your secrets provider.
              </Typography.Text>
            </Space>
          </Space>
        )
      ) : null}
    </Card>
  );
}

export function MigrationsPanel() {
  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      <ConfigLayoutCard />
      <SecretsCard />
      <Card className="page-card" size="small" title="Upgrades and restores">
        <Typography.Paragraph type="secondary" style={{ margin: 0 }}>
          Software upgrades take a backup first and can be rolled back from <Link to={sectionHref("updates", { tab: "updates" })}>Updates</Link>.
          After a restore, output paths, schedule files and the HTTPS certificate are repaired automatically before the services restart.
        </Typography.Paragraph>
      </Card>
    </Space>
  );
}
