import {
  Button,
  Card,
  Popconfirm,
  Space,
  Table,
  Tag,
  Typography,
} from "antd";
import { DeleteOutlined, KeyOutlined, ReloadOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../../api/client";
import type {
  PersonalToken,
} from "../../../api/types";
import { notify, notifyError } from "../../../notify";
import { formatDateTime } from "../../../utils/datetime";
import { IconTitle } from "../../../ui";
import { roleColor } from "../../../auth/roleColor";

// TokensCard is the admin-wide personal-access-token inventory: every user's
// tokens with metadata, and a revoke action for any of them. Users mint and
// manage their own tokens from the header user menu; this card lets an admin
// audit and revoke org-wide.
export function TokensCard() {
  const qc = useQueryClient();
  const tokensQuery = useQuery({
    queryKey: ["settings", "tokens"],
    queryFn: api.listAllTokens,
  });
  const refresh = () => qc.invalidateQueries({ queryKey: ["settings", "tokens"] });

  const revokeMut = useMutation({
    mutationFn: (id: string) => api.adminRevokeToken(id),
    onSuccess: () => {
      notify.success("Token revoked.");
      void refresh();
    },
    onError: (e) => notifyError(e, "Could not revoke token"),
  });

  const tokens = (tokensQuery.data?.tokens ?? []) as PersonalToken[];
  const fmt = (v?: string) => formatDateTime(v);

  return (
    <Card
      className="page-card"
      title={<IconTitle icon={<KeyOutlined />}>Personal access tokens</IconTitle>}
      extra={
        <Space>
          <Tag color={tokens.length > 0 ? "blue" : "default"}>{tokens.length} active</Tag>
          <Button icon={<ReloadOutlined />} onClick={() => refresh()} loading={tokensQuery.isFetching}>
            Refresh
          </Button>
        </Space>
      }
    >
      <Typography.Paragraph type="secondary">
        Bearer tokens users created to call the API outside the browser. Each token carries its
        owner&apos;s role. Revoke any that are unused or compromised — the client stops working
        immediately.
      </Typography.Paragraph>
      <Table
        rowKey="id"
        size="small"
        loading={tokensQuery.isLoading}
        dataSource={tokens}
        pagination={false}
        locale={{ emptyText: "No personal access tokens." }}
        columns={[
          { title: "Owner", dataIndex: "owner", render: (v: string) => <Typography.Text strong>{v}</Typography.Text> },
          { title: "Name", dataIndex: "name" },
          { title: "Role", dataIndex: "role", render: (v: string) => <Tag color={roleColor(v)}>{v || "—"}</Tag> },
          { title: "Created", dataIndex: "created_at", render: fmt },
          {
            title: "Expires",
            dataIndex: "expires_at",
            render: (v?: string) => (v ? formatDateTime(v) : <Tag color="orange">Never</Tag>),
          },
          { title: "Last used", dataIndex: "last_used_at", render: fmt },
          {
            title: "Actions",
            key: "actions",
            render: (_: unknown, rec: PersonalToken) => (
              <Popconfirm
                title={`Revoke ${rec.owner}'s token "${rec.name}"?`}
                okText="Revoke"
                okButtonProps={{ danger: true }}
                onConfirm={() => revokeMut.mutate(rec.id)}
              >
                <Button size="small" danger icon={<DeleteOutlined />}>
                  Revoke
                </Button>
              </Popconfirm>
            ),
          },
        ]}
      />
    </Card>
  );
}
