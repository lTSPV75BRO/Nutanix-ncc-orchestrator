import { useState } from "react";
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  InputNumber,
  Modal,
  Popconfirm,
  Select,
  Space,
  Switch,
  Table,
  Tag,
  Tooltip,
  Typography,
  Upload,
} from "antd";
import {
  ClockCircleOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  DownloadOutlined,
  KeyOutlined,
  LogoutOutlined,
  PlusOutlined,
  ReloadOutlined,
  SearchOutlined,
  UploadOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type {
  LDAPConfig,
  PasswordResetRequest,
  SessionPolicy,
  SSOConfig,
  UserAccount,
  UserRole,
} from "../../api/types";
import { notify, notifyError } from "../../notify";

const ROLE_OPTIONS = [
  { value: "admin", label: "admin — full access" },
  { value: "operator", label: "operator — trigger/cancel runs" },
  { value: "viewer", label: "viewer — read-only" },
];

function roleColor(role?: string): string {
  return role === "admin" ? "gold" : role === "operator" ? "blue" : "default";
}

// The built-in "admin" account's role is fixed and it cannot be deleted; the UI
// disables those controls to match the server-side enforcement.
function isReservedAdmin(username?: string): boolean {
  return (username ?? "").trim().toLowerCase() === "admin";
}

const RESERVED_ADMIN_HINT = "The built-in admin account's role is fixed and it cannot be deleted.";

function UsersCard() {
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
    onError: (e) => notifyError(e, "Failed to create user"),
  });

  const roleMut = useMutation({
    mutationFn: (v: { username: string; role: UserRole }) => api.updateUser(v.username, { role: v.role }),
    onSuccess: () => {
      notify.success("Role updated.");
      void refresh();
    },
    onError: (e) => notifyError(e, "Failed to update role"),
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
    onError: (e) => notifyError(e, "Failed to reset password"),
  });

  const resetIsAdmin = resetUser !== null && isReservedAdmin(resetUser);

  const revokeMut = useMutation({
    mutationFn: (username: string) => api.updateUser(username, { revoke_sessions: true }),
    onSuccess: (_res, username) => {
      notify.success(`All sessions for ${username} have been signed out.`);
      void refresh();
    },
    onError: (e) => notifyError(e, "Failed to sign out sessions"),
  });

  const deleteMut = useMutation({
    mutationFn: (username: string) => api.deleteUser(username),
    onSuccess: () => {
      notify.success("User deleted.");
      void refresh();
    },
    onError: (e) => notifyError(e, "Failed to delete user"),
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
      title="Local accounts"
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
        Manage password accounts and roles. Roles: <Tag color="gold">admin</Tag> full access,{" "}
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
                    description="Existing tokens for this user stop working immediately."
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
              { min: 8, message: "Use at least 8 characters" },
            ]}
            extra="The user will be required to change this on first login."
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={`Reset password — ${resetUser ?? ""}`}
        open={resetUser !== null}
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
        okText={resetIsAdmin ? "Generate & reset" : "Reset"}
      >
        {resetIsAdmin ? (
          <Typography.Paragraph type="secondary" style={{ fontSize: 13, marginBottom: 0 }}>
            The built-in <code>admin</code> password will be reset to a new random value, following
            the same workflow as first-run setup: the admin must change it on next login, all
            existing admin sessions are signed out, and the new password is also written to the
            server logs and the <code>.ncc-initial-admin-password</code> file. It will be shown once
            here so you can share it securely.
          </Typography.Paragraph>
        ) : (
          <Form
            form={resetForm}
            layout="vertical"
            onFinish={(v) => resetUser && resetMut.mutate({ username: resetUser, password: v.password })}
          >
            <Form.Item
              name="password"
              label="New temporary password"
              rules={[{ required: true }, { min: 8, message: "Use at least 8 characters" }]}
              extra="The user must change it on next login."
            >
              <Input.Password autoComplete="new-password" />
            </Form.Item>
          </Form>
        )}
      </Modal>
    </Card>
  );
}

function PasswordResetRequestsCard() {
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
    onError: (e) => notifyError(e, "Failed to reset password"),
  });

  const dismissMut = useMutation({
    mutationFn: (username: string) => api.dismissPasswordReset(username),
    onSuccess: () => {
      notify.success("Request dismissed.");
      void refresh();
    },
    onError: (e) => notifyError(e, "Failed to dismiss request"),
  });

  const requests = (reqQuery.data?.requests ?? []) as PasswordResetRequest[];

  return (
    <Card
      className="page-card"
      title="Password reset requests"
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
              render: (v: string) => (v ? new Date(v).toLocaleString() : "—"),
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
        title={`Reset password — ${resetUser ?? ""}`}
        open={resetUser !== null}
        onCancel={() => setResetUser(null)}
        onOk={() => resetForm.submit()}
        confirmLoading={resetMut.isPending}
        okText="Reset"
      >
        <Form
          form={resetForm}
          layout="vertical"
          onFinish={(v) => resetUser && resetMut.mutate({ username: resetUser, password: v.password })}
        >
          <Form.Item
            name="password"
            label="New temporary password"
            rules={[{ required: true }, { min: 8, message: "Use at least 8 characters" }]}
            extra="The user must change it on next login."
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
        </Form>
      </Modal>
    </Card>
  );
}

function SSOCard({ embedded }: { embedded?: boolean }) {
  const qc = useQueryClient();
  const ssoQuery = useQuery({ queryKey: ["settings", "sso"], queryFn: api.getSSO });
  const [form] = Form.useForm();
  const cfg = ssoQuery.data as SSOConfig | undefined;
  const managedByFlags = cfg?.managed_by === "flags";

  const saveMut = useMutation({
    mutationFn: (v: Record<string, unknown>) =>
      api.updateSSO({
        enabled: Boolean(v.enabled),
        root_url: String(v.root_url ?? ""),
        entity_id: String(v.entity_id ?? ""),
        idp_metadata_url: String(v.idp_metadata_url ?? ""),
        idp_metadata_xml: String(v.idp_metadata_xml ?? ""),
        role_attribute: String(v.role_attribute ?? ""),
        role_map: String(v.role_map ?? ""),
        default_role: String(v.default_role ?? "viewer"),
        username_attribute: String(v.username_attribute ?? ""),
        allow_idp_initiated: Boolean(v.allow_idp_initiated),
      }),
    onSuccess: () => {
      notify.success("SSO configuration saved.");
      void qc.invalidateQueries({ queryKey: ["settings", "sso"] });
      void qc.invalidateQueries({ queryKey: ["me"] });
    },
    onError: (e) => notifyError(e, "Failed to save SSO config"),
  });

  const body = (
    <>
      {managedByFlags ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          message="SAML is configured via startup flags and is read-only here."
        />
      ) : (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          message="The SP signing key is generated on the server. Publish the SP metadata URL below to your IdP — no private key leaves the server."
        />
      )}
      {cfg?.sp_metadata_url ? (
        <Typography.Paragraph>
          <Typography.Text strong>SP metadata URL: </Typography.Text>
          <Typography.Text copyable code>
            {cfg.sp_metadata_url}
          </Typography.Text>
        </Typography.Paragraph>
      ) : null}
      <Form
        form={form}
        layout="vertical"
        disabled={managedByFlags}
        initialValues={{
          enabled: cfg?.enabled ?? false,
          root_url: cfg?.root_url ?? "",
          entity_id: cfg?.entity_id ?? "",
          idp_metadata_url: cfg?.idp_metadata_url ?? "",
          idp_metadata_xml: "",
          role_attribute: cfg?.role_attribute ?? "Role",
          role_map: cfg?.role_map ?? "",
          default_role: cfg?.default_role ?? "viewer",
          username_attribute: cfg?.username_attribute ?? "",
          allow_idp_initiated: cfg?.allow_idp_initiated ?? false,
        }}
        onFinish={(v) => saveMut.mutate(v)}
      >
        <Form.Item name="enabled" label="Enable SAML SSO" valuePropName="checked">
          <Switch />
        </Form.Item>
        <Form.Item name="root_url" label="Server root URL" extra="External base URL, e.g. https://ncc.example.com" rules={[{ required: true }]}>
          <Input placeholder="https://ncc.example.com" />
        </Form.Item>
        <Form.Item name="idp_metadata_url" label="IdP metadata URL">
          <Input placeholder="https://idp.example.com/metadata" />
        </Form.Item>
        <Form.Item
          name="idp_metadata_xml"
          label="…or paste IdP metadata XML"
          extra={cfg?.has_idp_metadata_xml ? "XML is currently stored; leave blank to keep it." : undefined}
        >
          <Input.TextArea rows={4} placeholder="<EntityDescriptor ...>" />
        </Form.Item>
        <Form.Item name="role_attribute" label="Role/group attribute">
          <Input placeholder="Role" />
        </Form.Item>
        <Form.Item name="role_map" label="Role mapping" extra="idpValue=role, comma-separated, e.g. ncc-admins=admin,ncc-ops=operator">
          <Input placeholder="ncc-admins=admin,ncc-ops=operator" />
        </Form.Item>
        <Form.Item name="default_role" label="Default role (no mapping match)">
          <Select options={ROLE_OPTIONS} />
        </Form.Item>
        <Form.Item name="username_attribute" label="Username attribute (optional)">
          <Input placeholder="(defaults to NameID / common attrs)" />
        </Form.Item>
        <Form.Item name="entity_id" label="SP entity ID (optional)">
          <Input placeholder="(defaults to <root>/saml/metadata)" />
        </Form.Item>
        <Form.Item name="allow_idp_initiated" label="Allow IdP-initiated login" valuePropName="checked">
          <Switch />
        </Form.Item>
        {!managedByFlags ? (
          <Form.Item>
            <Button type="primary" htmlType="submit" loading={saveMut.isPending}>
              Save SSO configuration
            </Button>
          </Form.Item>
        ) : null}
      </Form>
    </>
  );
  if (embedded) return body;
  return (
    <Card
      className="page-card"
      title="Single sign-on (SAML)"
      extra={
        <Tag color={cfg?.enabled ? "green" : "default"}>{cfg?.enabled ? "enabled" : "disabled"}</Tag>
      }
    >
      {body}
    </Card>
  );
}

function SessionCard() {
  const qc = useQueryClient();
  const policyQuery = useQuery({ queryKey: ["settings", "session"], queryFn: api.getSessionPolicy });
  const [form] = Form.useForm();
  const cfg = policyQuery.data as SessionPolicy | undefined;

  const minMinutes = cfg ? Math.max(1, Math.ceil(cfg.min_sec / 60)) : 1;
  const maxMinutes = cfg ? Math.floor(cfg.max_sec / 60) : 1440;

  const saveMut = useMutation({
    mutationFn: (minutes: number) => api.updateSessionPolicy({ ttl_min: minutes }),
    onSuccess: () => {
      notify.success("Session duration updated. It applies the next time users sign in.");
      void qc.invalidateQueries({ queryKey: ["settings", "session"] });
      void qc.invalidateQueries({ queryKey: ["me"] });
    },
    onError: (e) => notifyError(e, "Failed to update session duration"),
  });

  const resetMut = useMutation({
    mutationFn: () => api.updateSessionPolicy({ ttl_sec: 0 }),
    onSuccess: () => {
      notify.success("Reverted to the server default session duration.");
      void qc.invalidateQueries({ queryKey: ["settings", "session"] });
      void qc.invalidateQueries({ queryKey: ["me"] });
    },
    onError: (e) => notifyError(e, "Failed to reset session duration"),
  });

  return (
    <Card
      className="page-card"
      title={
        <Space>
          <ClockCircleOutlined /> Session lifetime
        </Space>
      }
      extra={<Tag color={cfg?.source === "runtime" ? "blue" : "default"}>{cfg?.source === "runtime" ? "custom" : "server default"}</Tag>}
      loading={policyQuery.isLoading}
    >
      <Typography.Paragraph type="secondary">
        Choose how long a signed-in session stays active before users must log in again. The change
        takes effect on the next login; existing sessions keep their original expiry.
      </Typography.Paragraph>
      {cfg ? (
        <Form
          form={form}
          layout="inline"
          key={cfg.ttl_min}
          initialValues={{ ttl_min: cfg.ttl_min }}
          onFinish={(v) => saveMut.mutate(Number(v.ttl_min))}
        >
          <Form.Item
            name="ttl_min"
            label="Active for"
            rules={[
              { required: true, message: "Enter a duration" },
              {
                type: "number",
                min: minMinutes,
                max: maxMinutes,
                message: `Between ${minMinutes} and ${maxMinutes} minutes`,
              },
            ]}
            extra={`Allowed range: ${minMinutes}–${maxMinutes} minutes (max 24h).`}
          >
            <InputNumber min={minMinutes} max={maxMinutes} step={5} addonAfter="minutes" style={{ width: 200 }} />
          </Form.Item>
          <Form.Item>
            <Space>
              <Button type="primary" htmlType="submit" loading={saveMut.isPending}>
                Save
              </Button>
              <Button onClick={() => resetMut.mutate()} loading={resetMut.isPending} disabled={cfg.source !== "runtime"}>
                Use server default
              </Button>
            </Space>
          </Form.Item>
        </Form>
      ) : null}
    </Card>
  );
}

function BackupRestoreCard() {
  const [downloading, setDownloading] = useState(false);
  const [restoring, setRestoring] = useState(false);

  const handleDownload = async () => {
    setDownloading(true);
    try {
      const { blob, filename } = await api.downloadBackup();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      notify.success("Backup downloaded.");
    } catch (e) {
      notifyError(e, "Failed to create backup");
    } finally {
      setDownloading(false);
    }
  };

  // Poll the backend health endpoint until the restarted stack answers, then
  // reload so the app re-bootstraps against the restored config/session.
  const waitForRestartAndReload = async () => {
    const deadline = Date.now() + 120_000;
    // The old api/ui servers are being torn down; give them a head start.
    await new Promise((r) => setTimeout(r, 4000));
    while (Date.now() < deadline) {
      try {
        const resp = await fetch("/api/v1/health", { cache: "no-store" });
        if (resp.ok) break;
      } catch {
        // still down — keep waiting
      }
      await new Promise((r) => setTimeout(r, 2000));
    }
    window.location.reload();
  };

  const doRestore = async (file: File) => {
    setRestoring(true);
    try {
      const res = await api.restoreBackup(file);
      const data = (res.data ?? {}) as { restarting?: boolean };
      if (data.restarting) {
        Modal.success({
          title: "Backup restored — restarting",
          content:
            res.message ??
            "The stack is restarting to load the restored data. This page will reconnect automatically.",
          okText: "OK",
        });
        // Fire-and-forget: reconnect once the restarted stack is healthy.
        void waitForRestartAndReload();
      } else {
        Modal.success({
          title: "Backup restored",
          content:
            res.message ??
            "Restart the stack (v2-stop then v2-start) for the restored data to take effect.",
        });
      }
    } catch (e) {
      notifyError(e, "Restore failed");
    } finally {
      setRestoring(false);
    }
  };

  // beforeUpload intercepts the selected file, shows a destructive-action
  // confirmation, and returns false so antd never auto-uploads it.
  const confirmRestore = (file: File): boolean => {
    Modal.confirm({
      title: "Restore from backup?",
      icon: <WarningOutlined style={{ color: "#faad14" }} />,
      width: 520,
      content: (
        <div>
          <Typography.Paragraph>
            This overwrites the install directory with the contents of <b>{file.name}</b> —
            configuration and referenced files, local accounts and roles, the API token, and
            scheduler/notification state.
          </Typography.Paragraph>
          <Typography.Paragraph style={{ marginBottom: 0 }}>
            The stack will <b>restart automatically</b> afterward to load the restored data — this
            page will reconnect on its own, and you may be asked to sign in again if the API token
            or your account changed.
          </Typography.Paragraph>
        </div>
      ),
      okText: "Restore and overwrite",
      okButtonProps: { danger: true },
      cancelText: "Cancel",
      onOk: () => doRestore(file),
    });
    return false;
  };

  return (
    <Card
      className="page-card"
      title={
        <Space>
          <DatabaseOutlined /> Backup &amp; restore
        </Space>
      }
    >
      <Typography.Paragraph type="secondary">
        Download a full backup of this install — configuration and referenced files, local accounts
        and roles, the API token, scheduler/notification state, the start settings (CORS, listen
        addresses, session TTL), the audit log, and the latest run's report — as a single{" "}
        <Typography.Text code>.tar.gz</Typography.Text>. The archive contains secrets, so store it
        securely. Restoring overwrites the current install and then restarts the stack automatically
        — backups are portable across OS and version, so a Windows backup restores onto Linux/macOS.
      </Typography.Paragraph>
      <Space wrap>
        <Button icon={<DownloadOutlined />} onClick={handleDownload} loading={downloading}>
          Download backup
        </Button>
        <Upload
          accept=".gz,.tgz,.tar.gz,application/gzip"
          showUploadList={false}
          beforeUpload={(file) => confirmRestore(file as unknown as File)}
        >
          <Button icon={<UploadOutlined />} danger loading={restoring}>
            Restore from backup…
          </Button>
        </Upload>
      </Space>
    </Card>
  );
}

function LDAPCard({ embedded }: { embedded?: boolean }) {
  const qc = useQueryClient();
  const ldapQuery = useQuery({ queryKey: ["settings", "ldap"], queryFn: api.getLDAP });
  const [form] = Form.useForm();
  const [testForm] = Form.useForm();
  const [testOpen, setTestOpen] = useState(false);
  const [testing, setTesting] = useState(false);
  const cfg = ldapQuery.data as LDAPConfig | undefined;
  const managedByFlags = cfg?.managed_by === "flags";

  // Build the API payload from form values. The bind password and CA cert are
  // write-only: an empty field leaves the stored secret untouched.
  const toPayload = (v: Record<string, unknown>) => {
    const bind = String(v.bind_password ?? "").trim();
    const ca = String(v.ca_cert_pem ?? "").trim();
    return {
      enabled: Boolean(v.enabled),
      url: String(v.url ?? "").trim(),
      start_tls: Boolean(v.start_tls),
      insecure_skip_verify: Boolean(v.insecure_skip_verify),
      bind_dn: String(v.bind_dn ?? "").trim(),
      base_dn: String(v.base_dn ?? "").trim(),
      user_filter: String(v.user_filter ?? "").trim(),
      username_attribute: String(v.username_attribute ?? "").trim(),
      group_attribute: String(v.group_attribute ?? "").trim(),
      role_map: String(v.role_map ?? "").trim(),
      default_role: String(v.default_role ?? "viewer"),
      ...(bind ? { bind_password: bind } : {}),
      ...(ca ? { ca_cert_pem: ca } : {}),
    };
  };

  const saveMut = useMutation({
    mutationFn: (v: Record<string, unknown>) => api.updateLDAP(toPayload(v)),
    onSuccess: () => {
      notify.success("LDAP/AD configuration saved.");
      void qc.invalidateQueries({ queryKey: ["settings", "ldap"] });
      void qc.invalidateQueries({ queryKey: ["me"] });
    },
    onError: (e) => notifyError(e, "Failed to save LDAP config"),
  });

  const handleTest = async () => {
    const tv = await testForm.validateFields();
    const v = form.getFieldsValue(true);
    setTesting(true);
    try {
      const res = await api.testLDAP({
        ...toPayload(v),
        test_username: String(tv.test_username ?? "").trim(),
        test_password: String(tv.test_password ?? ""),
      });
      Modal.success({
        title: "LDAP authentication succeeded",
        content: (
          <span>
            Signed in as <b>{res.username}</b> with role <Tag color={roleColor(res.role)}>{res.role}</Tag>
          </span>
        ),
      });
      setTestOpen(false);
      testForm.resetFields();
    } catch (e) {
      notifyError(e, "LDAP test failed");
    } finally {
      setTesting(false);
    }
  };

  const body = (
    <>
      {managedByFlags ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          message="LDAP is configured via startup flags and is read-only here."
        />
      ) : (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          message="Users sign in on the normal login form with their AD credentials. Local accounts are tried first, then AD — so the built-in admin keeps working even if AD is unreachable."
        />
      )}
      <Form
        form={form}
        layout="vertical"
        disabled={managedByFlags}
        initialValues={{
          enabled: cfg?.enabled ?? false,
          url: cfg?.url ?? "",
          start_tls: cfg?.start_tls ?? false,
          insecure_skip_verify: cfg?.insecure_skip_verify ?? false,
          bind_dn: cfg?.bind_dn ?? "",
          bind_password: "",
          base_dn: cfg?.base_dn ?? "",
          user_filter: cfg?.user_filter ?? "",
          username_attribute: cfg?.username_attribute ?? "",
          group_attribute: cfg?.group_attribute ?? "",
          role_map: cfg?.role_map ?? "",
          default_role: cfg?.default_role ?? "viewer",
          ca_cert_pem: "",
        }}
        onFinish={(v) => saveMut.mutate(v)}
      >
        <Form.Item name="enabled" label="Enable LDAP/AD login" valuePropName="checked">
          <Switch />
        </Form.Item>
        <Form.Item
          name="url"
          label="Server URL(s)"
          extra="ldaps://dc1.corp.example.com:636 — comma-separate multiple servers for failover."
          rules={[{ required: true, message: "Enter at least one server URL" }]}
        >
          <Input placeholder="ldaps://dc1.corp.example.com:636" />
        </Form.Item>
        <Space size={24} wrap>
          <Form.Item name="start_tls" label="StartTLS (for ldap://)" valuePropName="checked">
            <Switch />
          </Form.Item>
          <Form.Item
            name="insecure_skip_verify"
            label="Skip TLS verification"
            valuePropName="checked"
            extra="Discouraged — only for testing."
          >
            <Switch />
          </Form.Item>
        </Space>
        <Form.Item
          name="ca_cert_pem"
          label="CA certificate (PEM)"
          extra={cfg?.has_ca_cert ? "A CA cert is stored; leave blank to keep it." : "Optional: paste the CA that signed the server certificate."}
        >
          <Input.TextArea rows={3} placeholder="-----BEGIN CERTIFICATE-----" />
        </Form.Item>
        <Form.Item
          name="bind_dn"
          label="Service account DN"
          extra="Read-only account used to search for users."
        >
          <Input placeholder="CN=ncc-svc,OU=Service Accounts,DC=corp,DC=example,DC=com" />
        </Form.Item>
        <Form.Item
          name="bind_password"
          label="Service account password"
          extra={cfg?.has_bind_password ? "A password is stored; leave blank to keep it." : undefined}
        >
          <Input.Password placeholder="••••••••" autoComplete="new-password" />
        </Form.Item>
        <Form.Item
          name="base_dn"
          label="Base DN"
          extra="Search base for users."
          rules={[{ required: true, message: "Enter the user search base DN" }]}
        >
          <Input placeholder="DC=corp,DC=example,DC=com" />
        </Form.Item>
        <Form.Item
          name="user_filter"
          label="User filter (optional)"
          extra="%s is the login name. Default: (&(objectClass=user)(sAMAccountName=%s))"
        >
          <Input placeholder="(&(objectClass=user)(sAMAccountName=%s))" />
        </Form.Item>
        <Space size={24} wrap>
          <Form.Item name="username_attribute" label="Username attribute (optional)">
            <Input placeholder="sAMAccountName" />
          </Form.Item>
          <Form.Item name="group_attribute" label="Group attribute (optional)">
            <Input placeholder="memberOf" />
          </Form.Item>
        </Space>
        <Form.Item
          name="role_map"
          label="Group → role mapping"
          extra="One per line (or semicolon-separated): group DN or CN = role. Highest matching role wins."
        >
          <Input.TextArea
            rows={3}
            placeholder={"CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin\nCN=NCC-Operators,OU=Groups,DC=corp,DC=example,DC=com=operator"}
          />
        </Form.Item>
        <Form.Item name="default_role" label="Default role (no mapping match)">
          <Select options={ROLE_OPTIONS} />
        </Form.Item>
        {!managedByFlags ? (
          <Form.Item>
            <Space>
              <Button type="primary" htmlType="submit" loading={saveMut.isPending}>
                Save LDAP configuration
              </Button>
              <Button onClick={() => setTestOpen(true)}>Test connection…</Button>
            </Space>
          </Form.Item>
        ) : null}
      </Form>

      <Modal
        title="Test LDAP/AD login"
        open={testOpen}
        onCancel={() => setTestOpen(false)}
        onOk={handleTest}
        okText="Run test"
        confirmLoading={testing}
        destroyOnClose
      >
        <Typography.Paragraph type="secondary">
          Uses the configuration entered above (not yet saved) to authenticate a real AD user, so you
          can verify connectivity and the resolved role before saving.
        </Typography.Paragraph>
        <Form form={testForm} layout="vertical">
          <Form.Item name="test_username" label="AD username" rules={[{ required: true }]}>
            <Input placeholder="jdoe" autoComplete="off" />
          </Form.Item>
          <Form.Item name="test_password" label="AD password" rules={[{ required: true }]}>
            <Input.Password autoComplete="off" />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
  if (embedded) return body;
  return (
    <Card
      className="page-card"
      title="LDAP / Active Directory"
      extra={<Tag color={cfg?.enabled ? "green" : "default"}>{cfg?.enabled ? "enabled" : "disabled"}</Tag>}
      loading={ldapQuery.isLoading}
    >
      {body}
    </Card>
  );
}

function ExternalAuthCard() {
  const ssoQuery = useQuery({ queryKey: ["settings", "sso"], queryFn: api.getSSO });
  const ldapQuery = useQuery({ queryKey: ["settings", "ldap"], queryFn: api.getLDAP });
  const samlOn = Boolean((ssoQuery.data as SSOConfig | undefined)?.enabled);
  const ldapOn = Boolean((ldapQuery.data as LDAPConfig | undefined)?.enabled);
  const [provider, setProvider] = useState<"saml" | "ldap">("saml");

  return (
    <Card className="page-card" title="External authentication">
      <Space direction="vertical" size={16} style={{ width: "100%" }}>
        <div>
          <Space align="center" wrap>
            <Typography.Text strong>Provider</Typography.Text>
            <Select
              value={provider}
              onChange={(v) => setProvider(v as "saml" | "ldap")}
              style={{ minWidth: 320 }}
              options={[
                { value: "saml", label: "SAML single sign-on" },
                { value: "ldap", label: "LDAP / Active Directory" },
              ]}
            />
            <Tag color={samlOn ? "green" : "default"}>SAML {samlOn ? "on" : "off"}</Tag>
            <Tag color={ldapOn ? "green" : "default"}>LDAP {ldapOn ? "on" : "off"}</Tag>
          </Space>
          <Typography.Paragraph type="secondary" style={{ marginTop: 8, marginBottom: 0 }}>
            Configure each provider independently — both can be enabled at the same time. The dropdown
            only changes which one you're editing; saving one does not disable the other. At login, local
            accounts are checked first, then any enabled directory provider.
          </Typography.Paragraph>
        </div>
        <div style={{ display: provider === "saml" ? "block" : "none" }}>
          <SSOCard embedded />
        </div>
        <div style={{ display: provider === "ldap" ? "block" : "none" }}>
          <LDAPCard embedded />
        </div>
      </Space>
    </Card>
  );
}

export function AccessSection() {
  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      <UsersCard />
      <PasswordResetRequestsCard />
      <SessionCard />
      <BackupRestoreCard />
      <ExternalAuthCard />
    </Space>
  );
}
