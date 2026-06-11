import { useEffect, useRef, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Divider,
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
  ApartmentOutlined,
  ClockCircleOutlined,
  CloudDownloadOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  DownloadOutlined,
  KeyOutlined,
  LogoutOutlined,
  LockOutlined,
  PlusOutlined,
  ReloadOutlined,
  RollbackOutlined,
  SafetyCertificateOutlined,
  SearchOutlined,
  UploadOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type {
  BackupEntry,
  ClusterGroup,
  DirectoryEntry,
  LDAPConfig,
  PasswordResetRequest,
  PersonalToken,
  SessionPolicy,
  SSOConfig,
  TLSApplyResult,
  TLSPolicy,
  UpdateStatus,
  UserAccount,
  UserRole,
} from "../../api/types";
import { notify, notifyError } from "../../notify";
import { formatDateTime } from "../../utils/datetime";

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

  // See LDAPCard: when embedded in the provider dropdown this form mounts before
  // the query resolves, so initialValues are blank. Sync loaded values in.
  // The write-only IdP metadata XML field is left untouched.
  useEffect(() => {
    if (!cfg) return;
    form.setFieldsValue({
      enabled: cfg.enabled ?? false,
      root_url: cfg.root_url ?? "",
      entity_id: cfg.entity_id ?? "",
      idp_metadata_url: cfg.idp_metadata_url ?? "",
      role_attribute: cfg.role_attribute ?? "Role",
      role_map: cfg.role_map ?? "",
      default_role: cfg.default_role ?? "viewer",
      username_attribute: cfg.username_attribute ?? "",
      allow_idp_initiated: cfg.allow_idp_initiated ?? false,
    });
  }, [cfg, form]);

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

// triggerBlobDownload saves a fetched Blob to disk via a transient anchor.
function triggerBlobDownload(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// Human-readable byte size for the backups table.
function humanSize(bytes?: number): string {
  if (!bytes || bytes <= 0) return "—";
  const units = ["B", "KB", "MB", "GB"];
  let v = bytes;
  let u = 0;
  while (v >= 1024 && u < units.length - 1) {
    v /= 1024;
    u += 1;
  }
  return `${v.toFixed(v < 10 && u > 0 ? 1 : 0)} ${units[u]}`;
}

const BACKUP_KIND_META: Record<string, { color: string; label: string }> = {
  "pre-update": { color: "gold", label: "pre-update" },
  manual: { color: "blue", label: "manual" },
  other: { color: "default", label: "other" },
};

function BackupRestoreCard() {
  const qc = useQueryClient();
  const backupsQuery = useQuery({ queryKey: ["settings", "backups"], queryFn: api.listBackups });
  const [downloading, setDownloading] = useState(false);
  const [restoring, setRestoring] = useState(false);
  const [busyName, setBusyName] = useState<string | null>(null);

  const backups = (backupsQuery.data?.backups ?? []) as BackupEntry[];
  const rollback = backups.find((b) => b.rollback_candidate);
  const refresh = () => qc.invalidateQueries({ queryKey: ["settings", "backups"] });

  const handleDownload = async () => {
    setDownloading(true);
    try {
      const { blob, filename } = await api.downloadBackup();
      triggerBlobDownload(blob, filename);
      notify.success("Backup downloaded.");
    } catch (e) {
      notifyError(e, "Failed to create backup");
    } finally {
      setDownloading(false);
    }
  };

  const createMut = useMutation({
    mutationFn: () => api.createBackup(),
    onSuccess: (res) => {
      notify.success(res.message ?? "Snapshot created.");
      void refresh();
    },
    onError: (e) => notifyError(e, "Failed to create snapshot"),
  });

  const deleteMut = useMutation({
    mutationFn: (name: string) => api.deleteBackup(name),
    onSuccess: () => {
      notify.success("Backup deleted.");
      void refresh();
    },
    onError: (e) => notifyError(e, "Failed to delete backup"),
  });

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

  const announceRestore = (restarting: boolean, message?: string) => {
    if (restarting) {
      Modal.success({
        title: "Backup restored — restarting",
        content:
          message ??
          "The stack is restarting to load the restored data. This page will reconnect automatically.",
        okText: "OK",
      });
      void waitForRestartAndReload();
    } else {
      Modal.success({
        title: "Backup restored",
        content:
          message ??
          "Restart the stack (v2-stop then v2-start) for the restored data to take effect.",
      });
    }
  };

  const doRestoreUpload = async (file: File) => {
    setRestoring(true);
    try {
      const res = await api.restoreBackup(file);
      const data = (res.data ?? {}) as { restarting?: boolean };
      announceRestore(Boolean(data.restarting), res.message);
    } catch (e) {
      notifyError(e, "Restore failed");
    } finally {
      setRestoring(false);
    }
  };

  const doRestoreNamed = async (name: string) => {
    setBusyName(name);
    try {
      const res = await api.restoreNamedBackup(name);
      const data = (res.data ?? {}) as { restarting?: boolean };
      announceRestore(Boolean(data.restarting), res.message);
    } catch (e) {
      notifyError(e, "Restore failed");
    } finally {
      setBusyName(null);
    }
  };

  const downloadNamed = async (name: string) => {
    setBusyName(name);
    try {
      const { blob, filename } = await api.downloadNamedBackup(name);
      triggerBlobDownload(blob, filename);
    } catch (e) {
      notifyError(e, "Failed to download backup");
    } finally {
      setBusyName(null);
    }
  };

  const restoreBody = (name: string, isRollback: boolean) => (
    <div>
      <Typography.Paragraph>
        This overwrites the install directory with the contents of <b>{name}</b> — configuration and
        referenced files, local accounts and roles, the API token, and scheduler/notification state.
      </Typography.Paragraph>
      <Typography.Paragraph style={{ marginBottom: 0 }}>
        {isRollback ? "The stack rolls back and " : "The stack "}
        <b>restarts automatically</b> afterward to load the restored data — this page reconnects on
        its own, and you may be asked to sign in again if the API token or your account changed.
      </Typography.Paragraph>
    </div>
  );

  const confirmRestoreNamed = (entry: BackupEntry) => {
    const isRollback = Boolean(entry.rollback_candidate);
    Modal.confirm({
      title: isRollback ? "Roll back to pre-update backup?" : "Restore this backup?",
      icon: <WarningOutlined style={{ color: "#faad14" }} />,
      width: 540,
      content: restoreBody(entry.name, isRollback),
      okText: isRollback ? "Roll back and restart" : "Restore and restart",
      okButtonProps: { danger: true },
      cancelText: "Cancel",
      onOk: () => doRestoreNamed(entry.name),
    });
  };

  // beforeUpload intercepts the selected file, shows a destructive-action
  // confirmation, and returns false so antd never auto-uploads it.
  const confirmRestoreUpload = (file: File): boolean => {
    Modal.confirm({
      title: "Restore from uploaded backup?",
      icon: <WarningOutlined style={{ color: "#faad14" }} />,
      width: 540,
      content: restoreBody(file.name, false),
      okText: "Restore and overwrite",
      okButtonProps: { danger: true },
      cancelText: "Cancel",
      onOk: () => doRestoreUpload(file),
    });
    return false;
  };

  const columns = [
    {
      title: "Name",
      dataIndex: "name",
      render: (n: string) => (
        <Typography.Text className="mono" style={{ fontSize: 12 }}>
          {n}
        </Typography.Text>
      ),
    },
    {
      title: "Type",
      dataIndex: "kind",
      render: (k: string, rec: BackupEntry) => {
        const meta = BACKUP_KIND_META[k] ?? BACKUP_KIND_META.other;
        return (
          <Space size={4}>
            <Tag color={meta.color}>{meta.label}</Tag>
            {rec.rollback_candidate ? <Tag color="volcano">latest rollback</Tag> : null}
          </Space>
        );
      },
    },
    { title: "Size", dataIndex: "size", render: (v: number) => humanSize(v) },
    {
      title: "Created",
      dataIndex: "mod_time",
      render: (v: string) => formatDateTime(v),
    },
    {
      title: "Actions",
      key: "actions",
      render: (_: unknown, rec: BackupEntry) => (
        <Space wrap>
          <Button
            size="small"
            danger
            icon={rec.rollback_candidate ? <RollbackOutlined /> : <UploadOutlined />}
            loading={busyName === rec.name}
            onClick={() => confirmRestoreNamed(rec)}
          >
            {rec.rollback_candidate ? "Roll back" : "Restore"}
          </Button>
          <Button
            size="small"
            icon={<DownloadOutlined />}
            loading={busyName === rec.name}
            onClick={() => downloadNamed(rec.name)}
          >
            Download
          </Button>
          <Popconfirm
            title={`Delete ${rec.name}?`}
            okText="Delete"
            okButtonProps={{ danger: true }}
            onConfirm={() => deleteMut.mutate(rec.name)}
          >
            <Button size="small" danger icon={<DeleteOutlined />}>
              Delete
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Card
      className="page-card"
      title={
        <Space>
          <DatabaseOutlined /> Backup &amp; restore
        </Space>
      }
      extra={
        <Space>
          {rollback ? (
            <Tooltip title={`Roll back to the most recent pre-update snapshot (${rollback.name})`}>
              <Button
                icon={<RollbackOutlined />}
                danger
                loading={busyName === rollback.name}
                onClick={() => confirmRestoreNamed(rollback)}
              >
                Roll back last update
              </Button>
            </Tooltip>
          ) : null}
          <Button
            icon={<ReloadOutlined />}
            onClick={() => refresh()}
            loading={backupsQuery.isFetching}
          >
            Refresh
          </Button>
        </Space>
      }
    >
      <Typography.Paragraph type="secondary">
        A backup is a single <Typography.Text code>.tar.gz</Typography.Text> of this installation —
        configuration and referenced files, local accounts and roles, the API token,
        scheduler/notification state, the start settings (CORS, listen addresses, session TTL), the
        audit log, and the latest run&apos;s report. Backups are portable across operating systems
        and versions, so a Windows backup restores onto Linux or macOS. Restoring overwrites the
        current installation and then restarts the stack automatically. Archives contain secrets —
        store them securely.
      </Typography.Paragraph>

      <Space wrap style={{ marginBottom: 16 }}>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => createMut.mutate()}
          loading={createMut.isPending}
        >
          Create snapshot
        </Button>
        <Button icon={<DownloadOutlined />} onClick={handleDownload} loading={downloading}>
          Download backup
        </Button>
        <Upload
          accept=".gz,.tgz,.tar.gz,application/gzip"
          showUploadList={false}
          beforeUpload={(file) => confirmRestoreUpload(file as unknown as File)}
        >
          <Button icon={<UploadOutlined />} danger loading={restoring}>
            Restore from file…
          </Button>
        </Upload>
      </Space>

      <Typography.Title level={5} style={{ marginTop: 0 }}>
        Server-side backups
      </Typography.Title>
      <Typography.Paragraph type="secondary" style={{ marginTop: -4 }}>
        Snapshots saved on the server, including automatic <b>pre-update</b> backups taken before
        each in-app update. Restore any of them without re-uploading, or roll back the most recent
        update with one click.
      </Typography.Paragraph>
      <Table
        rowKey="name"
        size="small"
        loading={backupsQuery.isLoading}
        dataSource={backups}
        columns={columns}
        pagination={false}
        locale={{ emptyText: "No server-side backups yet. Create a snapshot or run an update." }}
      />
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

  // initialValues only applies on first mount; in the provider dropdown this
  // card mounts (with display:none) before the config query resolves, so push
  // the loaded values into the form whenever they arrive/refresh. Write-only
  // secrets (bind password, CA cert) are intentionally left untouched.
  useEffect(() => {
    if (!cfg) return;
    form.setFieldsValue({
      enabled: cfg.enabled ?? false,
      url: cfg.url ?? "",
      start_tls: cfg.start_tls ?? false,
      insecure_skip_verify: cfg.insecure_skip_verify ?? false,
      bind_dn: cfg.bind_dn ?? "",
      base_dn: cfg.base_dn ?? "",
      user_filter: cfg.user_filter ?? "",
      username_attribute: cfg.username_attribute ?? "",
      group_attribute: cfg.group_attribute ?? "",
      role_map: cfg.role_map ?? "",
      default_role: cfg.default_role ?? "viewer",
    });
  }, [cfg, form]);

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
          extra="%s is the login name (each %s is substituted). Default matches sAMAccountName or userPrincipalName: (&(objectClass=user)(|(sAMAccountName=%s)(userPrincipalName=%s)))"
        >
          <Input placeholder="(&(objectClass=user)(|(sAMAccountName=%s)(userPrincipalName=%s)))" />
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

type DirectorySelectProps = {
  kind: "group" | "user";
  value?: string[];
  onChange?: (v: string[]) => void;
  placeholder?: string;
};

// DirectorySearchSelect is a tags-mode Select backed by live AD/LDAP type-ahead.
// As the admin types, it queries /settings/ldap/search and shows matching groups
// or users; selecting one stores the canonical identifier (group DN or user
// sAMAccountName). Manual entry is still allowed (tags mode) so it works even
// when LDAP is not reachable. Form.Item injects value/onChange.
function DirectorySearchSelect({ kind, value, onChange, placeholder }: DirectorySelectProps) {
  const [options, setOptions] = useState<{ value: string; label: string; title: string }[]>([]);
  const [fetching, setFetching] = useState(false);
  const [ldapDisabled, setLdapDisabled] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reqId = useRef(0);

  const runSearch = (term: string) => {
    const trimmed = term.trim();
    if (trimmed.length < 2) {
      setOptions([]);
      setFetching(false);
      return;
    }
    const myReq = ++reqId.current;
    setFetching(true);
    api
      .searchDirectory(trimmed, kind)
      .then((res) => {
        if (myReq !== reqId.current) return; // a newer keystroke superseded this
        setLdapDisabled(!res.ldap_enabled);
        setOptions(
          (res.results ?? []).map((e: DirectoryEntry) => ({
            value: e.value,
            label: e.upn ? `${e.name} — ${e.upn}` : `${e.name} (${e.value})`,
            title: e.dn || e.value,
          })),
        );
      })
      .catch(() => {
        if (myReq !== reqId.current) return;
        setOptions([]);
      })
      .finally(() => {
        if (myReq === reqId.current) setFetching(false);
      });
  };

  const onSearch = (term: string) => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => runSearch(term), 300);
  };

  return (
    <>
      <Select
        mode="tags"
        allowClear
        showSearch
        filterOption={false}
        value={value}
        onChange={(v) => onChange?.(v as string[])}
        onSearch={onSearch}
        notFoundContent={fetching ? "Searching directory…" : null}
        placeholder={placeholder}
        options={options}
        tokenSeparators={[",", "\n"]}
      />
      {ldapDisabled && (
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          LDAP/AD is not enabled, so live search is unavailable — type values manually.
        </Typography.Text>
      )}
    </>
  );
}

type PrismCentralFieldProps = {
  value?: string[];
  onChange?: (v: string[]) => void;
};

// PrismCentralField captures the Prism Central URLs/addresses whose registered
// clusters are folded into the group, with a "Discover clusters" action that
// previews how many clusters each PC currently manages (using the active run
// config's credentials server-side). Form.Item injects value/onChange.
function PrismCentralField({ value, onChange }: PrismCentralFieldProps) {
  const pcs = value ?? [];
  const [results, setResults] = useState<Record<string, { count?: number; clusters?: string[]; error?: string }>>({});
  const [loading, setLoading] = useState(false);

  const discover = async () => {
    if (pcs.length === 0) return;
    setLoading(true);
    const next: Record<string, { count?: number; clusters?: string[]; error?: string }> = {};
    await Promise.all(
      pcs.map(async (pc) => {
        try {
          const res = await api.discoverPCClusters(pc);
          next[pc] = { count: res.count, clusters: res.clusters.map((c) => c.name || c.address) };
        } catch (e) {
          next[pc] = { error: e instanceof Error ? e.message : String(e) };
        }
      }),
    );
    setResults(next);
    setLoading(false);
  };

  return (
    <Space direction="vertical" size={8} style={{ width: "100%" }}>
      <Space.Compact style={{ width: "100%" }}>
        <Select
          mode="tags"
          allowClear
          style={{ width: "100%" }}
          value={pcs}
          onChange={(v) => onChange?.(v as string[])}
          placeholder="https://pc.corp.example.com:9440 (type and press Enter)"
          tokenSeparators={[",", " ", "\n"]}
        />
        <Button onClick={discover} loading={loading} disabled={pcs.length === 0} icon={<SearchOutlined />}>
          Discover
        </Button>
      </Space.Compact>
      {pcs.map((pc) =>
        results[pc] ? (
          results[pc].error ? (
            <Alert key={pc} type="error" showIcon message={`${pc}: ${results[pc].error}`} />
          ) : (
            <Alert
              key={pc}
              type="success"
              showIcon
              message={`${pc}: ${results[pc].count} cluster(s)`}
              description={
                results[pc].clusters && results[pc].clusters!.length > 0 ? (
                  <Space size={[4, 4]} wrap>
                    {results[pc].clusters!.map((c) => (
                      <Tag key={c}>{c}</Tag>
                    ))}
                  </Space>
                ) : undefined
              }
            />
          )
        ) : null,
      )}
    </Space>
  );
}

function ClusterGroupsCard() {
  const qc = useQueryClient();
  const groupsQuery = useQuery({ queryKey: ["settings", "cluster-groups"], queryFn: api.getClusterGroups });
  const inventoryQuery = useQuery({ queryKey: ["settings", "clusters"], queryFn: api.getClusterInventory });
  const usersQuery = useQuery({ queryKey: ["settings", "users"], queryFn: api.listUsers });
  const [editing, setEditing] = useState<ClusterGroup | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [form] = Form.useForm();

  const groups = (groupsQuery.data?.groups ?? []) as ClusterGroup[];
  const inventory = (inventoryQuery.data?.clusters ?? []) as string[];
  const localAccounts = (usersQuery.data?.users ?? []).map((u) => u.username);
  const refresh = () => qc.invalidateQueries({ queryKey: ["settings", "cluster-groups"] });

  const saveMut = useMutation({
    mutationFn: (next: ClusterGroup[]) => api.updateClusterGroups(next),
    onSuccess: () => {
      notify.success("Cluster groups updated.");
      setModalOpen(false);
      setEditing(null);
      form.resetFields();
      void refresh();
    },
    onError: (e) => notifyError(e, "Failed to update cluster groups"),
  });

  const openCreate = () => {
    setEditing(null);
    form.resetFields();
    setModalOpen(true);
  };
  const openEdit = (g: ClusterGroup) => {
    setEditing(g);
    form.setFieldsValue({
      name: g.name,
      clusters: g.clusters ?? [],
      prism_centrals: g.prism_centrals ?? [],
      local_users: g.local_users ?? [],
      ad_groups: g.ad_groups ?? [],
      ad_users: g.ad_users ?? [],
    });
    setModalOpen(true);
  };

  const submit = () => {
    form
      .validateFields()
      .then(
        (v: {
          name: string;
          clusters?: string[];
          prism_centrals?: string[];
          local_users?: string[];
          ad_groups?: string[];
          ad_users?: string[];
        }) => {
        const entry: ClusterGroup = {
          name: v.name.trim(),
          clusters: v.clusters ?? [],
          prism_centrals: v.prism_centrals ?? [],
          local_users: v.local_users ?? [],
          ad_groups: v.ad_groups ?? [],
          ad_users: v.ad_users ?? [],
        };
        const others = groups.filter(
          (g) => g.name.toLowerCase() !== (editing?.name ?? entry.name).toLowerCase(),
        );
        saveMut.mutate([...others, entry]);
      })
      .catch(() => undefined);
  };

  const remove = (name: string) => {
    saveMut.mutate(groups.filter((g) => g.name.toLowerCase() !== name.toLowerCase()));
  };

  const columns = [
    { title: "Group", dataIndex: "name", key: "name", render: (n: string) => <Typography.Text strong>{n}</Typography.Text> },
    {
      title: "Clusters",
      key: "clusters",
      render: (_: unknown, g: ClusterGroup) => {
        const clusters = g.clusters ?? [];
        const pcs = g.prism_centrals ?? [];
        if (clusters.length === 0 && pcs.length === 0) {
          return <Typography.Text type="secondary">—</Typography.Text>;
        }
        return (
          <Space direction="vertical" size={2}>
            {clusters.length > 0 && (
              <Space size={[4, 4]} wrap>
                {clusters.map((x) => (
                  <Tag key={x}>{x}</Tag>
                ))}
              </Space>
            )}
            {pcs.length > 0 && (
              <span>
                <Typography.Text type="secondary">Prism Central: </Typography.Text>
                {pcs.map((x) => (
                  <Tag key={x} color="cyan" icon={<ApartmentOutlined />}>
                    {x}
                  </Tag>
                ))}
              </span>
            )}
          </Space>
        );
      },
    },
    {
      title: "Members",
      key: "members",
      render: (_: unknown, g: ClusterGroup) => (
        <Space direction="vertical" size={2}>
          {g.local_users && g.local_users.length > 0 && (
            <span>
              <Typography.Text type="secondary">Users: </Typography.Text>
              {g.local_users.map((u) => (
                <Tag key={u} color="blue">
                  {u}
                </Tag>
              ))}
            </span>
          )}
          {g.ad_groups && g.ad_groups.length > 0 && (
            <span>
              <Typography.Text type="secondary">AD groups: </Typography.Text>
              {g.ad_groups.map((u) => (
                <Tag key={u} color="geekblue">
                  {u}
                </Tag>
              ))}
            </span>
          )}
          {g.ad_users && g.ad_users.length > 0 && (
            <span>
              <Typography.Text type="secondary">AD users: </Typography.Text>
              {g.ad_users.map((u) => (
                <Tag key={u} color="purple">
                  {u}
                </Tag>
              ))}
            </span>
          )}
          {!(g.local_users?.length || g.ad_groups?.length || g.ad_users?.length) && (
            <Typography.Text type="secondary">no members</Typography.Text>
          )}
        </Space>
      ),
    },
    {
      title: "Actions",
      key: "actions",
      render: (_: unknown, g: ClusterGroup) => (
        <Space>
          <Button size="small" onClick={() => openEdit(g)}>
            Edit
          </Button>
          <Popconfirm title={`Delete group "${g.name}"?`} onConfirm={() => remove(g.name)} okText="Delete" okType="danger">
            <Button size="small" danger icon={<DeleteOutlined />}>
              Delete
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Card
      className="page-card"
      title="Cluster groups"
      extra={
        <Space>
          <Button icon={<ReloadOutlined />} onClick={() => refresh()} loading={groupsQuery.isFetching}>
            Refresh
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
            Add group
          </Button>
        </Space>
      }
    >
      <Typography.Paragraph type="secondary">
        Segregate clusters into groups and grant access to local accounts, Active Directory groups, and individual
        Active Directory users (live AD search as you type). You can also add a whole Prism Central — every cluster
        it manages is folded into the group automatically. Non-admin users only see, trigger, and act on clusters
        in the groups they belong to; a cluster may belong to multiple groups. Administrators always have access to
        every cluster, and clusters that are not in any group remain admin-only.
      </Typography.Paragraph>
      <Table
        rowKey="name"
        size="small"
        loading={groupsQuery.isLoading}
        dataSource={groups}
        columns={columns}
        pagination={false}
        locale={{ emptyText: "No cluster groups yet. Add one to confine members to a cluster subset." }}
      />
      <Modal
        title={editing ? `Edit group "${editing.name}"` : "Add cluster group"}
        open={modalOpen}
        onCancel={() => {
          setModalOpen(false);
          setEditing(null);
          form.resetFields();
        }}
        onOk={submit}
        okText="Save"
        confirmLoading={saveMut.isPending}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="name"
            label="Group name"
            rules={[{ required: true, message: "Group name is required" }]}
          >
            <Input placeholder="Platform" disabled={Boolean(editing)} />
          </Form.Item>
          <Form.Item
            name="clusters"
            label="Clusters"
            extra="Pick from the clusters in your active config, or type a cluster name/address."
          >
            <Select
              mode="tags"
              allowClear
              showSearch
              optionFilterProp="label"
              placeholder="Select or type cluster names"
              options={inventory.map((c) => ({ value: c, label: c }))}
            />
          </Form.Item>
          <Form.Item
            name="prism_centrals"
            label="Prism Centrals (optional)"
            extra="Every cluster registered under these Prism Centrals is granted to the group automatically (refreshed in the background). Use Discover to preview the clusters each PC manages."
          >
            <PrismCentralField />
          </Form.Item>
          <Form.Item
            name="local_users"
            label="Local accounts (optional)"
            extra="Start typing to pick from existing local accounts, or type a username."
          >
            <Select
              mode="tags"
              allowClear
              showSearch
              optionFilterProp="label"
              placeholder="Select or type a username"
              options={localAccounts.map((u) => ({ value: u, label: u }))}
            />
          </Form.Item>
          <Form.Item
            name="ad_groups"
            label="AD groups (optional)"
            extra="Search Active Directory as you type, or paste a group CN / full DN. All members of the group get access."
          >
            <DirectorySearchSelect kind="group" placeholder="Search AD groups (type to search)…" />
          </Form.Item>
          <Form.Item
            name="ad_users"
            label="AD users (optional)"
            extra="Search Active Directory for individual users to grant access directly (without adding their whole group)."
          >
            <DirectorySearchSelect kind="user" placeholder="Search AD users (type to search)…" />
          </Form.Item>
        </Form>
      </Modal>
    </Card>
  );
}

// TokensCard is the admin-wide personal-access-token inventory: every user's
// tokens with metadata, and a revoke action for any of them. Users mint and
// manage their own tokens from the header user menu; this card lets an admin
// audit and revoke org-wide.
function TokensCard() {
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
    onError: (e) => notifyError(e, "Failed to revoke token"),
  });

  const tokens = (tokensQuery.data?.tokens ?? []) as PersonalToken[];
  const fmt = (v?: string) => formatDateTime(v);

  return (
    <Card
      className="page-card"
      title={
        <Space>
          <KeyOutlined />
          Personal access tokens
        </Space>
      }
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

// TLSCard manages HTTPS for the browser-facing UI server: upload a PEM
// certificate + key to enable HTTPS (the stack restarts to bind TLS and session
// cookies become Secure), or remove it to fall back to plain HTTP. The private
// key is write-only — it is stored 0600 on the server and never returned.
function TLSCard() {
  const qc = useQueryClient();
  const tlsQuery = useQuery({ queryKey: ["settings", "tls"], queryFn: api.getTLS });
  const [form] = Form.useForm();
  const [submitting, setSubmitting] = useState(false);
  const cfg = tlsQuery.data as TLSPolicy | undefined;
  const enabled = Boolean(cfg?.https_enabled);
  // The stack may serve HTTPS-by-default (orchestrator self-signed) before any
  // certificate has been registered through this card, so trust the live scheme
  // too when labeling the current state.
  const servingHTTPS = enabled || (typeof window !== "undefined" && window.location.protocol === "https:");

  const fmt = (v?: string) => formatDateTime(v);

  // After enabling/disabling HTTPS the stack restarts and the scheme changes,
  // so the current origin stops answering. Guide the user to the new-scheme URL
  // (same host/port) and offer to navigate there once the restart settles.
  const announceSchemeSwitch = (result: TLSApplyResult, nextScheme: "https" | "http") => {
    const port = window.location.port ? `:${window.location.port}` : "";
    const target = `${nextScheme}://${window.location.hostname}${port}${window.location.pathname}`;
    if (result.restarting) {
      Modal.success({
        title: nextScheme === "https" ? "HTTPS enabled — restarting" : "HTTPS disabled — restarting",
        width: 540,
        content: (
          <div>
            <Typography.Paragraph>
              {result.tls.https_enabled
                ? "The stack is restarting to serve over TLS. Session cookies will be marked Secure."
                : "The stack is restarting to serve over plain HTTP."}{" "}
              Reconnect at:
            </Typography.Paragraph>
            <Typography.Text code copyable={{ text: target }}>
              {target}
            </Typography.Text>
            {nextScheme === "https" ? (
              <Typography.Paragraph type="secondary" style={{ marginTop: 8, marginBottom: 0 }}>
                If you uploaded a self-signed certificate, your browser will warn about it — that is
                expected; accept it to proceed.
              </Typography.Paragraph>
            ) : null}
          </div>
        ),
        okText: "Reconnect now",
        onOk: () => {
          window.location.href = target;
        },
      });
      // Auto-navigate after the restart has had time to come back up.
      window.setTimeout(() => {
        window.location.href = target;
      }, 8000);
    } else {
      Modal.warning({
        title: result.tls.https_enabled ? "Certificate installed" : "HTTPS disabled",
        content:
          "An automatic restart was unavailable. Restart the stack (v2-restart) for the change to take effect.",
      });
    }
  };

  const installMut = useMutation({
    mutationFn: (v: { cert: string; key: string }) => api.installTLS(v),
    onSuccess: (res) => {
      form.resetFields();
      void qc.invalidateQueries({ queryKey: ["settings", "tls"] });
      announceSchemeSwitch(res, "https");
    },
    onError: (e) => notifyError(e, "Failed to enable HTTPS"),
  });

  // Generate (or renew) a self-signed certificate. We pass the hostname the
  // admin is currently using so the certificate's SANs cover it; the backend
  // always adds localhost/loopback too.
  const generateMut = useMutation({
    mutationFn: () => api.generateTLS({ hosts: [window.location.hostname].filter(Boolean) }),
    onSuccess: (res) => {
      void qc.invalidateQueries({ queryKey: ["settings", "tls"] });
      announceSchemeSwitch(res, "https");
    },
    onError: (e) => notifyError(e, "Failed to generate self-signed certificate"),
  });

  const disableMut = useMutation({
    mutationFn: () => api.disableTLS(),
    onSuccess: (res) => {
      void qc.invalidateQueries({ queryKey: ["settings", "tls"] });
      announceSchemeSwitch(res, "http");
    },
    onError: (e) => notifyError(e, "Failed to disable HTTPS"),
  });

  const onFinish = async (v: { cert: string; key: string }) => {
    setSubmitting(true);
    try {
      await installMut.mutateAsync({ cert: v.cert, key: v.key });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Card
      className="page-card"
      title={
        <Space>
          <SafetyCertificateOutlined /> HTTPS / TLS
        </Space>
      }
      extra={
        <Tag color={servingHTTPS ? "green" : "default"}>{servingHTTPS ? "HTTPS enabled" : "HTTP only"}</Tag>
      }
      loading={tlsQuery.isLoading}
    >
      <Typography.Paragraph type="secondary">
        The UI is served over HTTPS by default with a self-signed certificate, and any plain-HTTP
        request is redirected to HTTPS. Generate or renew a self-signed certificate with one click,
        or upload your own PEM certificate and private key below. Applying a certificate restarts the
        stack to bind the browser-facing server to TLS and marks session cookies as <code>Secure</code>.
        The private key is stored with <code>0600</code> permissions on the server and is never shown
        again.
      </Typography.Paragraph>

      <div style={{ marginBottom: 16 }}>
        <Space wrap>
          <Popconfirm
            title={enabled ? "Renew the self-signed certificate?" : "Generate a self-signed certificate?"
            }
            description="The stack restarts to apply the new certificate. Browsers will show a one-time self-signed warning — accept it to continue."
            okText={enabled ? "Renew & restart" : "Generate & restart"}
            onConfirm={() => generateMut.mutate()}
          >
            <Button type="primary" icon={<SafetyCertificateOutlined />} loading={generateMut.isPending}>
              {enabled ? "Renew self-signed certificate" : "Generate self-signed certificate"}
            </Button>
          </Popconfirm>
          {enabled ? (
            <Popconfirm
              title="Disable HTTPS?"
              description="The stack restarts and falls back to plain HTTP. The stored certificate and key are removed. Note: if the stack is configured for HTTPS-by-default it will regenerate a self-signed certificate on the next start."
              okText="Disable HTTPS"
              okButtonProps={{ danger: true }}
              onConfirm={() => disableMut.mutate()}
            >
              <Button danger icon={<DeleteOutlined />} loading={disableMut.isPending}>
                Disable HTTPS
              </Button>
            </Popconfirm>
          ) : null}
        </Space>
      </div>

      {enabled ? (
        <Alert
          type="success"
          showIcon
          icon={<LockOutlined />}
          style={{ marginBottom: 16 }}
          message="HTTPS is enabled"
          description={
            <Space direction="vertical" size={2} style={{ width: "100%" }}>
              <span>
                <Typography.Text type="secondary">Subject: </Typography.Text>
                <Typography.Text code>{cfg?.subject || "—"}</Typography.Text>
              </span>
              <span>
                <Typography.Text type="secondary">Issuer: </Typography.Text>
                {cfg?.issuer || "—"}
              </span>
              <span>
                <Typography.Text type="secondary">Valid: </Typography.Text>
                {fmt(cfg?.not_before)} → {fmt(cfg?.not_after)}
              </span>
              {cfg?.dns_names && cfg.dns_names.length > 0 ? (
                <span>
                  <Typography.Text type="secondary">SANs: </Typography.Text>
                  {cfg.dns_names.map((d) => (
                    <Tag key={d}>{d}</Tag>
                  ))}
                </span>
              ) : null}
            </Space>
          }
        />
      ) : null}

      <Divider titlePlacement="start" plain>
        Advanced — bring your own certificate
      </Divider>
      <Typography.Paragraph type="secondary" style={{ marginTop: -8 }}>
        Replace the self-signed certificate with one issued by your own CA (e.g. an internal PKI or a
        publicly-trusted cert for a DNS name). Paste the full chain and the matching private key.
      </Typography.Paragraph>
      <Form form={form} layout="vertical" onFinish={onFinish}>
        <Form.Item
          name="cert"
          label="Certificate (PEM)"
          rules={[{ required: true, message: "Paste the PEM certificate (include any intermediates)" }]}
          extra="Paste the full certificate chain: leaf certificate first, then any intermediates."
        >
          <Input.TextArea rows={5} placeholder={"-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----"} />
        </Form.Item>
        <Form.Item
          name="key"
          label="Private key (PEM)"
          rules={[{ required: true, message: "Paste the matching PEM private key" }]}
          extra="The matching unencrypted private key. Stored 0600 on the server; never returned."
        >
          <Input.TextArea rows={4} placeholder={"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"} />
        </Form.Item>
        <Form.Item>
          <Button
            type="primary"
            htmlType="submit"
            icon={<LockOutlined />}
            loading={submitting || installMut.isPending}
          >
            {enabled ? "Replace certificate & restart" : "Enable HTTPS & restart"}
          </Button>
        </Form.Item>
      </Form>
    </Card>
  );
}

const UPDATE_PHASE_LABEL: Record<string, string> = {
  idle: "Idle",
  backing_up: "Backing up",
  updating: "Downloading & installing",
  restarting: "Restarting",
  done: "Done",
  error: "Failed",
};

// UpdatesCard lets an admin check for a newer release and apply it in place. The
// backend takes a pre-update backup, installs the new stack (orchestrator + api
// + ui + frontend, checksum-verified), then restarts the stack; this card polls
// the job phase and reconnects the page once the restarted stack is healthy.
function UpdatesCard() {
  const [status, setStatus] = useState<UpdateStatus | null>(null);
  const [checking, setChecking] = useState(false);
  const [applying, setApplying] = useState(false);
  const [restarting, setRestarting] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const job = status?.job;
  const phase = job?.phase ?? "idle";
  const inProgress = job?.in_progress ?? false;
  const busy = checking || applying || inProgress || restarting;

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  // Poll the backend health endpoint until the restarted stack answers, then
  // reload so the app re-bootstraps on the freshly-installed version.
  const waitForRestartAndReload = async () => {
    setRestarting(true);
    const deadline = Date.now() + 180_000;
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

  const startPolling = () => {
    stopPolling();
    pollRef.current = setInterval(async () => {
      try {
        const s = await api.updateStatus();
        setStatus((prev) => ({ ...(prev ?? {}), ...s }) as UpdateStatus);
        const p = s.job?.phase;
        if (p === "restarting") {
          stopPolling();
          void waitForRestartAndReload();
        } else if (p === "done") {
          stopPolling();
          setApplying(false);
          notify.success(s.job?.message ?? "Update complete.");
        } else if (p === "error") {
          stopPolling();
          setApplying(false);
          notify.error(s.job?.error ?? "Update failed.");
        }
      } catch {
        // A failed status poll while applying almost certainly means the
        // restart has begun and the api-server is going down — switch to
        // reconnect mode.
        stopPolling();
        void waitForRestartAndReload();
      }
    }, 3000);
  };

  useEffect(() => {
    let cancelled = false;
    void api
      .updateStatus()
      .then((s) => {
        if (cancelled) return;
        setStatus(s);
        if (s.job?.in_progress) {
          setApplying(true);
          startPolling();
        }
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
      stopPolling();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleCheck = async () => {
    setChecking(true);
    try {
      const s = await api.checkUpdate();
      setStatus(s);
    } catch (e) {
      notifyError(e, "Could not check for updates");
    } finally {
      setChecking(false);
    }
  };

  const doApply = async () => {
    setApplying(true);
    try {
      const res = await api.applyUpdate();
      const jobData = (res.data ?? {}) as UpdateStatus["job"];
      setStatus((prev) => ({ ...(prev ?? {}), job: jobData }) as UpdateStatus);
      notify.info(res.message ?? "Update started.");
      startPolling();
    } catch (e) {
      setApplying(false);
      notifyError(e, "Could not start the update");
    }
  };

  const confirmApply = () => {
    Modal.confirm({
      title: "Confirm software update",
      icon: <CloudDownloadOutlined style={{ color: "#1677ff" }} />,
      width: 540,
      content: (
        <div>
          <Typography.Paragraph>
            This installation will be updated to{" "}
            <b>{status?.latest_version ? `v${status.latest_version}` : "the latest release"}</b>. The
            server will perform the following steps:
          </Typography.Paragraph>
          <ol style={{ marginTop: 0, paddingLeft: 20 }}>
            <li>
              create a <b>pre-update backup</b> (saved under{" "}
              <Typography.Text code>backups/</Typography.Text> in the installation directory),
            </li>
            <li>
              install the new orchestrator, API, UI, and frontend components (checksum-verified),
              then
            </li>
            <li>
              <b>restart the stack automatically</b> to activate the new version.
            </li>
          </ol>
          <Typography.Paragraph style={{ marginBottom: 0 }}>
            This page will reconnect automatically once the restarted stack is healthy. You may be
            prompted to sign in again.
          </Typography.Paragraph>
        </div>
      ),
      okText: "Back up and update",
      cancelText: "Cancel",
      onOk: doApply,
    });
  };

  const supported = status?.supported !== false;
  const updateAvailable = status?.update_available === true;

  return (
    <Card
      className="page-card"
      title={
        <Space>
          <CloudDownloadOutlined /> Software updates
        </Space>
      }
      extra={
        <Button
          icon={<ReloadOutlined />}
          onClick={handleCheck}
          loading={checking}
          disabled={busy && !checking}
        >
          Check for updates
        </Button>
      }
    >
      <Typography.Paragraph type="secondary">
        Check for a newer release and apply it in place. The update creates a backup first, installs
        the latest orchestrator, API, UI, and frontend components (verified against the published
        release checksums), then restarts the stack automatically to activate the new version.
      </Typography.Paragraph>

      <Space direction="vertical" size={12} style={{ width: "100%" }}>
        <Space wrap>
          <Typography.Text type="secondary">Current version:</Typography.Text>
          <Tag>{status?.current_version ? `v${status.current_version}` : "unknown"}</Tag>
          {status?.latest_version ? (
            <>
              <Typography.Text type="secondary">Latest:</Typography.Text>
              <Tag color={updateAvailable ? "gold" : "green"}>v{status.latest_version}</Tag>
            </>
          ) : null}
        </Space>

        {!supported ? (
          <Alert
            type="info"
            showIcon
            message="In-app updates are not available"
            description="Updating from the UI requires a compiled ncc-orchestrator binary, which is not present in this development environment."
          />
        ) : null}

        {supported && !inProgress && !restarting && status?.check_error ? (
          <Alert type="warning" showIcon message="Unable to check for updates" description={status.check_error} />
        ) : null}

        {supported && !inProgress && !restarting && status?.update_available === false ? (
          <Alert type="success" showIcon message="This installation is running the latest available release." />
        ) : null}

        {supported && !inProgress && !restarting && updateAvailable ? (
          <Alert
            type="warning"
            showIcon
            message={`Update available: v${status?.latest_version}`}
            description="Select “Back up and update” to create a backup, install the update, and restart the stack."
            action={
              <Button type="primary" icon={<CloudDownloadOutlined />} onClick={confirmApply}>
                Back up and update
              </Button>
            }
          />
        ) : null}

        {(inProgress || restarting) ? (
          <Alert
            type="info"
            showIcon
            message={`Update in progress — ${UPDATE_PHASE_LABEL[restarting ? "restarting" : phase] ?? phase}`}
            description={
              restarting
                ? "The stack is restarting to load the new version. This page will reconnect automatically."
                : (job?.message ?? "Processing…")
            }
          />
        ) : null}

        {!inProgress && !restarting && phase === "error" && job?.error ? (
          <Alert type="error" showIcon message="Last update failed" description={job.error} />
        ) : null}
      </Space>
    </Card>
  );
}

export function AccessSection() {
  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      <UsersCard />
      <PasswordResetRequestsCard />
      <TokensCard />
      <ClusterGroupsCard />
      <SessionCard />
      <TLSCard />
      <ExternalAuthCard />
    </Space>
  );
}

// MaintenanceSection groups the lifecycle/operations tooling — in-place software
// updates and full backup/restore of the installation — into a dedicated tab,
// separate from access control.
export function MaintenanceSection() {
  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      <UpdatesCard />
      <BackupRestoreCard />
    </Space>
  );
}
