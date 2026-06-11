import { useState } from "react";
import type { ReactNode } from "react";
import {
  Alert,
  Button,
  Form,
  Input,
  Modal,
  Popconfirm,
  Select,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import { CopyOutlined, DeleteOutlined, KeyOutlined, PlusOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { CreatedToken, PersonalToken } from "../../api/types";
import { notify, notifyError } from "../../notify";
import { formatDateTime } from "../../utils/datetime";

const { Text, Paragraph } = Typography;

const EXPIRY_OPTIONS = [
  { value: 7, label: "7 days" },
  { value: 30, label: "30 days" },
  { value: 90, label: "90 days" },
  { value: 180, label: "180 days" },
  { value: 365, label: "1 year" },
  { value: 0, label: "Never" },
];

const fmtDate = (s?: string): string => formatDateTime(s);

// An empty expiry means the token never expires (long-lived automation
// credential), so render it explicitly rather than as a missing value.
function fmtExpiry(s?: string): ReactNode {
  if (!s) return <Tag color="orange">Never</Tag>;
  return fmtDate(s);
}

function roleColor(role: string): string {
  return role === "admin" ? "gold" : role === "operator" ? "blue" : "default";
}

/**
 * Self-service personal access token manager. Any signed-in user can mint a
 * bearer token that inherits their own role, list their tokens, and revoke
 * them. The secret is shown exactly once, right after creation.
 */
export function PersonalTokensModal({
  open,
  onClose,
  role,
}: {
  open: boolean;
  onClose: () => void;
  role: string;
}) {
  const queryClient = useQueryClient();
  const [form] = Form.useForm();
  const [created, setCreated] = useState<CreatedToken | null>(null);

  const tokensQuery = useQuery({
    queryKey: ["personal-tokens"],
    queryFn: api.listTokens,
    enabled: open,
  });
  const tokens = tokensQuery.data?.tokens ?? [];

  const createMutation = useMutation({
    mutationFn: (values: { name: string; expires_in_days: number }) => api.createToken(values),
    onSuccess: (data) => {
      setCreated(data);
      form.resetFields();
      void queryClient.invalidateQueries({ queryKey: ["personal-tokens"] });
    },
    onError: (e) => notifyError(e, "Failed to create token"),
  });

  const revokeMutation = useMutation({
    mutationFn: (id: string) => api.revokeToken(id),
    onSuccess: () => {
      notify.success("Token revoked.");
      void queryClient.invalidateQueries({ queryKey: ["personal-tokens"] });
    },
    onError: (e) => notifyError(e, "Failed to revoke token"),
  });

  const copySecret = async (value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      notify.success("Token copied to clipboard.");
    } catch {
      notify.warning("Copy failed — select and copy the token manually.");
    }
  };

  const columns = [
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      render: (v: string) => <Text strong>{v}</Text>,
    },
    {
      title: "Role",
      dataIndex: "role",
      key: "role",
      render: (v: string) => <Tag color={roleColor(v)}>{v || "—"}</Tag>,
    },
    { title: "Created", dataIndex: "created_at", key: "created_at", render: fmtDate },
    { title: "Expires", dataIndex: "expires_at", key: "expires_at", render: fmtExpiry },
    { title: "Last used", dataIndex: "last_used_at", key: "last_used_at", render: fmtDate },
    {
      title: "",
      key: "actions",
      width: 90,
      render: (_: unknown, row: PersonalToken) => (
        <Popconfirm
          title="Revoke this token?"
          description="Any client using it will stop working immediately."
          okText="Revoke"
          okButtonProps={{ danger: true }}
          onConfirm={() => revokeMutation.mutate(row.id)}
        >
          <Button size="small" danger icon={<DeleteOutlined />} loading={revokeMutation.isPending}>
            Revoke
          </Button>
        </Popconfirm>
      ),
    },
  ];

  return (
    <Modal
      title={
        <Space>
          <KeyOutlined />
          Personal access tokens
        </Space>
      }
      open={open}
      onCancel={() => {
        setCreated(null);
        onClose();
      }}
      footer={null}
      width={720}
      destroyOnClose
    >
      <Paragraph type="secondary" style={{ marginTop: 0 }}>
        Generate a bearer token to call the API from scripts, <code>curl</code>, Postman, or CI. A
        token carries your current role (<Tag color={roleColor(role)} style={{ marginInlineEnd: 0 }}>{role || "—"}</Tag>)
        and can do anything you can. Send it as an <code>X-API-Token</code> header or{" "}
        <code>Authorization: Bearer &lt;token&gt;</code>.
      </Paragraph>

      {created ? (
        <Alert
          type="success"
          showIcon
          style={{ marginBottom: 16 }}
          message={`Token "${created.name}" created`}
          description={
            <div>
              <Paragraph style={{ marginBottom: 8 }}>
                Copy it now — for security it will <b>not</b> be shown again.
              </Paragraph>
              <Space.Compact style={{ width: "100%" }}>
                <Input readOnly value={created.token} onFocus={(e) => e.currentTarget.select()} />
                <Tooltip title="Copy">
                  <Button icon={<CopyOutlined />} onClick={() => copySecret(created.token)} />
                </Tooltip>
              </Space.Compact>
            </div>
          }
          closable
          onClose={() => setCreated(null)}
        />
      ) : null}

      <Form
        form={form}
        layout="inline"
        initialValues={{ expires_in_days: 90 }}
        onFinish={(values) => createMutation.mutate(values)}
        style={{ marginBottom: 16, rowGap: 8 }}
      >
        <Form.Item
          name="name"
          rules={[{ required: true, message: "Name your token" }]}
          style={{ flex: 1, minWidth: 200 }}
        >
          <Input placeholder="Token name (e.g. laptop-cli)" maxLength={80} />
        </Form.Item>
        <Form.Item name="expires_in_days">
          <Select options={EXPIRY_OPTIONS} style={{ width: 120 }} />
        </Form.Item>
        <Form.Item>
          <Button type="primary" htmlType="submit" icon={<PlusOutlined />} loading={createMutation.isPending}>
            Generate
          </Button>
        </Form.Item>
      </Form>

      <Table<PersonalToken>
        size="small"
        rowKey="id"
        columns={columns}
        dataSource={tokens}
        loading={tokensQuery.isLoading}
        pagination={false}
        locale={{ emptyText: "No tokens yet." }}
      />
    </Modal>
  );
}
