import { useState } from "react";
import {
  Button,
  Card,
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
import { DeleteOutlined, KeyOutlined, LogoutOutlined, PlusOutlined, ReloadOutlined, SearchOutlined, UnlockOutlined, UserOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../../api/client";
import type {
  PasswordResetRequest,
  UserAccount,
  UserRole,
} from "../../../api/types";
import { notify, notifyError } from "../../../notify";
import { formatDateTime } from "../../../utils/datetime";
import { IconTitle } from "../../../ui";
import { roleColor } from "../../../auth/roleColor";
import { ROLE_OPTIONS, isReservedAdmin, RESERVED_ADMIN_HINT } from "./shared";

export function UsersCard() {
  const qc = useQueryClient();
  const usersQuery = useQuery({ queryKey: ["settings", "users"], queryFn: api.listUsers });
  const [createOpen, setCreateOpen] = useState(false);
  const [resetUser, setResetUser] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [createForm] = Form.useForm();
  const [resetForm] = Form.useForm();

  const refresh = () => qc.invalidateQueries({ queryKey: ["settings", "users"] });

  const createMut = useMutation({
    mutationFn: (v: { username: string; password: string; role: UserRole }) => api.createUser(v),
    onSuccess: () => {
      notify.success("User created.");
      setCreateOpen(false);
      createForm.resetFields();
      void refresh();
    },
    onError: (e) => notifyError(e, "Could not create user"),
  });

  const roleMut = useMutation({
    mutationFn: (v: { username: string; role: UserRole }) => api.updateUser(v.username, { role: v.role }),
    onSuccess: () => {
      notify.success("Role updated.");
      void refresh();
    },
    onError: (e) => notifyError(e, "Could not update role"),
  });

  const resetMut = useMutation({
    mutationFn: (v: { username: string; password?: string; generate?: boolean }) =>
      api.updateUser(
        v.username,
        v.generate
          ? { generate_password: true }
          : { password: v.password, must_change_password: true },
      ),
    onSuccess: (res, vars) => {
      setResetUser(null);
      resetForm.resetFields();
      void refresh();
      if (vars.generate) {
        const pw = res?.temporary_password;
        if (pw) {
          Modal.success({
            title: "Temporary admin password generated",
            content: (
              <div>
                <Typography.Paragraph style={{ marginBottom: 8 }}>
                  Share this securely. The admin must change it on next login, and
                  all existing admin sessions have been signed out.
                </Typography.Paragraph>
                <Typography.Text code copyable={{ text: pw }} style={{ fontSize: 15 }}>
                  {pw}
                </Typography.Text>
              </div>
            ),
          });
        } else {
          notify.success(
            "Admin password reset. Retrieve the new password from the server logs or the .ncc-initial-admin-password file.",
          );
        }
      } else {
        notify.success("Password reset; the user must change it on next login.");
      }
    },
    onError: (e) => notifyError(e, "Could not reset password"),
  });

  const resetIsAdmin = resetUser !== null && isReservedAdmin(resetUser);

  const revokeMut = useMutation({
    mutationFn: (username: string) => api.updateUser(username, { revoke_sessions: true }),
    onSuccess: (_res, username) => {
      notify.success(`All sessions for ${username} have been signed out.`);
      void refresh();
    },
    onError: (e) => notifyError(e, "Could not sign out sessions"),
  });

  const deleteMut = useMutation({
    mutationFn: (username: string) => api.deleteUser(username),
    onSuccess: () => {
      notify.success("User deleted.");
      void refresh();
    },
    onError: (e) => notifyError(e, "Could not delete user"),
  });

  const users = (usersQuery.data?.users ?? []) as UserAccount[];
  const query = search.trim().toLowerCase();
  const filteredUsers = query
    ? users.filter(
        (u) =>
          u.username.toLowerCase().includes(query) || (u.role ?? "").toLowerCase().includes(query),
      )
    : users;

  return (
    <Card
      className="page-card"
      id="settings-users"
      title={<IconTitle icon={<UserOutlined />}>Local accounts</IconTitle>}
      extra={
        <Space>
          <Button icon={<ReloadOutlined />} onClick={() => refresh()} loading={usersQuery.isFetching}>
            Refresh
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateOpen(true)}>
            Add user
          </Button>
        </Space>
      }
    >
      <Typography.Paragraph type="secondary">
        Manage password accounts and roles. Roles: <Tag color={roleColor("admin")}>admin</Tag> full access,{" "}
        <Tag color="blue">operator</Tag> can trigger/cancel runs, <Tag>viewer</Tag> is read-only.
      </Typography.Paragraph>
      <Input
        allowClear
        prefix={<SearchOutlined />}
        placeholder="Search by username or role"
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        style={{ maxWidth: 320, marginBottom: 12 }}
      />
      <Table
        rowKey="username"
        size="small"
        loading={usersQuery.isLoading}
        dataSource={filteredUsers}
        locale={{
          emptyText: query && users.length > 0 ? `No accounts match “${search.trim()}”.` : undefined,
        }}
        pagination={false}
        columns={[
          { title: "Username", dataIndex: "username" },
          {
            title: "Role",
            dataIndex: "role",
            render: (_: unknown, rec: UserAccount) => {
              const locked = isReservedAdmin(rec.username);
              const select = (
                <Select
                  size="small"
                  aria-label={`Role for ${rec.username}`}
                  value={rec.role}
                  disabled={locked}
                  style={{ width: 130 }}
                  options={ROLE_OPTIONS.map((o) => ({ value: o.value, label: o.value }))}
                  onChange={(role) => roleMut.mutate({ username: rec.username, role: role as UserRole })}
                />
              );
              return locked ? <Tooltip title={RESERVED_ADMIN_HINT}>{select}</Tooltip> : select;
            },
          },
          {
            title: "Status",
            dataIndex: "must_change_password",
            render: (v: boolean, rec: UserAccount) => (
              <Space>
                <Tag color={roleColor(rec.role)}>{rec.role}</Tag>
                {v ? <Tag color="orange">must change password</Tag> : null}
              </Space>
            ),
          },
          {
            title: "Actions",
            key: "actions",
            render: (_: unknown, rec: UserAccount) => {
              const locked = isReservedAdmin(rec.username);
              return (
                <Space>
                  <Button size="small" icon={<KeyOutlined />} onClick={() => setResetUser(rec.username)}>
                    Reset password
                  </Button>
                  <Popconfirm
                    title={`Sign out all sessions for ${rec.username}?`}
                    description="Existing tokens for this user stop working immediately"
                    onConfirm={() => revokeMut.mutate(rec.username)}
                    okButtonProps={{ danger: true }}
                  >
                    <Tooltip title="Invalidate all active sessions for this user">
                      <Button size="small" icon={<LogoutOutlined />} loading={revokeMut.isPending}>
                        Sign out
                      </Button>
                    </Tooltip>
                  </Popconfirm>
                  {locked ? (
                    <Tooltip title={RESERVED_ADMIN_HINT}>
                      <Button size="small" danger icon={<DeleteOutlined />} disabled>
                        Delete
                      </Button>
                    </Tooltip>
                  ) : (
                    <Popconfirm
                      title={`Delete ${rec.username}?`}
                      onConfirm={() => deleteMut.mutate(rec.username)}
                      okButtonProps={{ danger: true }}
                    >
                      <Button size="small" danger icon={<DeleteOutlined />}>
                        Delete
                      </Button>
                    </Popconfirm>
                  )}
                </Space>
              );
            },
          },
        ]}
      />

      <Modal
        title="Add user"
        open={createOpen}
        onCancel={() => setCreateOpen(false)}
        onOk={() => createForm.submit()}
        confirmLoading={createMut.isPending}
        okText="Create"
      >
        <Form form={createForm} layout="vertical" onFinish={(v) => createMut.mutate(v)}>
          <Form.Item name="username" label="Username" rules={[{ required: true }]}>
            <Input autoComplete="off" />
          </Form.Item>
          <Form.Item name="role" label="Role" initialValue="viewer" rules={[{ required: true }]}>
            <Select options={ROLE_OPTIONS} />
          </Form.Item>
          <Form.Item
            name="password"
            label="Temporary password"
            rules={[
              { required: true },
              { min: 15, message: "Use at least 15 characters" },
            ]}
            extra="The user will be required to change this on first login."
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="Reset password"
        open={resetUser !== null}
        centered
        width={460}
        onCancel={() => {
          setResetUser(null);
          resetForm.resetFields();
        }}
        onOk={() =>
          resetIsAdmin
            ? resetUser && resetMut.mutate({ username: resetUser, generate: true })
            : resetForm.submit()
        }
        confirmLoading={resetMut.isPending}
        okText={resetIsAdmin ? "Generate new password" : "Reset password"}
      >
        {resetIsAdmin ? (
          <p className="dialog-intro" style={{ marginBottom: 0 }}>
            The built-in <strong>admin</strong> account gets a new random password, as on first start. It is shown here
            once and also written to the server log and the <code>.ncc-initial-admin-password</code> file. All admin
            sessions are signed out, and the new password must be changed at the next sign-in.
          </p>
        ) : (
          <>
            <p className="dialog-intro">
              Set a temporary password for <strong>{resetUser}</strong>. They must choose a new one at their next
              sign-in.
            </p>
            <Form
              form={resetForm}
              layout="vertical"
              requiredMark={false}
              onFinish={(v) => resetUser && resetMut.mutate({ username: resetUser, password: v.password })}
            >
              <Form.Item
                name="password"
                label="Temporary password"
                rules={[{ required: true, message: "Enter a temporary password" }, { min: 15, message: "Use at least 15 characters" }]}
                extra="At least 15 characters. Share it with the user securely."
                style={{ marginBottom: 0 }}
              >
                <Input.Password autoComplete="new-password" autoFocus />
              </Form.Item>
            </Form>
          </>
        )}
      </Modal>
    </Card>
  );
}

export function PasswordResetRequestsCard() {
  const qc = useQueryClient();
  const reqQuery = useQuery({
    queryKey: ["settings", "password-resets"],
    queryFn: api.listPasswordResets,
  });
  const [resetUser, setResetUser] = useState<string | null>(null);
  const [resetForm] = Form.useForm();

  const refresh = () => qc.invalidateQueries({ queryKey: ["settings", "password-resets"] });

  // Resetting the password resolves the request server-side (it auto-clears),
  // so this reuses the same reset call as the Local accounts card.
  const resetMut = useMutation({
    mutationFn: (v: { username: string; password: string }) =>
      api.updateUser(v.username, { password: v.password, must_change_password: true }),
    onSuccess: () => {
      notify.success("Password reset; the user must change it on next login.");
      setResetUser(null);
      resetForm.resetFields();
      void refresh();
      void qc.invalidateQueries({ queryKey: ["settings", "users"] });
    },
    onError: (e) => notifyError(e, "Could not reset password"),
  });

  const dismissMut = useMutation({
    mutationFn: (username: string) => api.dismissPasswordReset(username),
    onSuccess: () => {
      notify.success("Request dismissed.");
      void refresh();
    },
    onError: (e) => notifyError(e, "Could not dismiss request"),
  });

  const requests = (reqQuery.data?.requests ?? []) as PasswordResetRequest[];

  return (
    <Card
      className="page-card"
      title={<IconTitle icon={<UnlockOutlined />}>Password reset requests</IconTitle>}
      extra={
        <Space>
          <Tag color={requests.length > 0 ? "orange" : "default"}>{requests.length} pending</Tag>
          <Button icon={<ReloadOutlined />} onClick={() => refresh()} loading={reqQuery.isFetching}>
            Refresh
          </Button>
        </Space>
      }
    >
      <Typography.Paragraph type="secondary">
        Self-service “Forgot password” requests from the login page. Verify each request
        out-of-band, then reset the user’s password and share the temporary password securely.
        Resetting the password clears the request automatically.
      </Typography.Paragraph>
      {requests.length === 0 ? (
        <Typography.Text type="secondary">No pending requests.</Typography.Text>
      ) : (
        <Table
          rowKey="username"
          size="small"
          loading={reqQuery.isLoading}
          dataSource={requests}
          pagination={false}
          columns={[
            { title: "Username", dataIndex: "username" },
            {
              title: "Requested",
              dataIndex: "requested_at",
              render: (v: string) => formatDateTime(v),
            },
            {
              title: "From IP",
              dataIndex: "client_ip",
              render: (v?: string) => v || "—",
            },
            {
              title: "Actions",
              key: "actions",
              render: (_: unknown, rec: PasswordResetRequest) => (
                <Space>
                  <Button
                    size="small"
                    type="primary"
                    icon={<KeyOutlined />}
                    onClick={() => setResetUser(rec.username)}
                  >
                    Reset password
                  </Button>
                  <Popconfirm
                    title={`Dismiss the request from ${rec.username}?`}
                    onConfirm={() => dismissMut.mutate(rec.username)}
                  >
                    <Button size="small">Dismiss</Button>
                  </Popconfirm>
                </Space>
              ),
            },
          ]}
        />
      )}

      <Modal
        title="Reset password"
        open={resetUser !== null}
        centered
        width={460}
        onCancel={() => {
          setResetUser(null);
          resetForm.resetFields();
        }}
        onOk={() => resetForm.submit()}
        confirmLoading={resetMut.isPending}
        okText="Reset password"
      >
        <p className="dialog-intro">
          Set a temporary password for <strong>{resetUser}</strong>. They must choose a new one at their next sign-in,
          and the request is cleared.
        </p>
        <Form
          form={resetForm}
          layout="vertical"
          requiredMark={false}
          onFinish={(v) => resetUser && resetMut.mutate({ username: resetUser, password: v.password })}
        >
          <Form.Item
            name="password"
            label="Temporary password"
            rules={[{ required: true, message: "Enter a temporary password" }, { min: 15, message: "Use at least 15 characters" }]}
            extra="At least 15 characters. Share it with the user securely."
            style={{ marginBottom: 0 }}
          >
            <Input.Password autoComplete="new-password" autoFocus />
          </Form.Item>
        </Form>
      </Modal>
    </Card>
  );
}
