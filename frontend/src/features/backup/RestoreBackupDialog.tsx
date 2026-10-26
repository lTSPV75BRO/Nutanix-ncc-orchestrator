import { useState } from "react";
import { Alert, Button, Form, Input, Modal, Typography, Upload } from "antd";
import { CloudUploadOutlined, KeyOutlined, LockOutlined, UploadOutlined, UserOutlined } from "@ant-design/icons";
import { api, ApiError } from "../../api/client";

/**
 * Poll /api/v1/health until the restarted stack answers, then reload so the
 * app re-bootstraps against the restored deployment.
 */
export async function waitForRestartAndReload(): Promise<void> {
  const deadline = Date.now() + 120_000;
  await new Promise((r) => setTimeout(r, 4000));
  while (Date.now() < deadline) {
    try {
      const resp = await fetch("/api/v1/health", { cache: "no-store", credentials: "include" });
      if (resp.ok) break;
    } catch {
      // still down — keep waiting
    }
    await new Promise((r) => setTimeout(r, 2000));
  }
  window.location.reload();
}

/** Backups are sealed by default (".tar.gz.enc"); plaintext ones end in ".tar.gz". */
export function isEncryptedBackupName(name: string): boolean {
  return /\.enc$/i.test(name.trim());
}

function formatSize(bytes: number): string {
  if (bytes >= 1 << 20) return `${(bytes / (1 << 20)).toFixed(1)} MiB`;
  if (bytes >= 1 << 10) return `${Math.round(bytes / (1 << 10))} KiB`;
  return `${bytes} B`;
}

// Mirrors maxRestoreUploadBytes in cmd/ncc-api-server/maintenance.go.
const MAX_RESTORE_BYTES = 50 << 20;

type Props = {
  open: boolean;
  onClose: () => void;
  /**
   * Sign in with these admin credentials before uploading (the login page has
   * no session yet; the restore endpoint is the one action a bootstrap admin
   * may take before changing the initial password).
   */
  requireSignIn?: boolean;
};

type Values = { username?: string; password?: string; passphrase?: string };

/**
 * Restore an install from a backup archive. Shared by the login page (fresh
 * install, not signed in) and the forced first-login password page.
 *
 * Before: both entry points filtered the file picker to ".gz/.tgz/.tar.gz" and
 * had no passphrase field, but backups are encrypted by default
 * (".tar.gz.enc"), so the usual migration path could not be completed.
 */
export function RestoreBackupDialog({ open, onClose, requireSignIn }: Props) {
  const [form] = Form.useForm<Values>();
  const [file, setFile] = useState<File | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const encrypted = file ? isEncryptedBackupName(file.name) : false;

  const close = () => {
    setFile(null);
    setError(null);
    form.resetFields();
    onClose();
  };

  const submit = async (values: Values) => {
    if (!file) {
      setError("Choose a backup archive to restore.");
      return;
    }
    if (file.size > MAX_RESTORE_BYTES) {
      setError(`This archive is ${formatSize(file.size)}; the limit is ${formatSize(MAX_RESTORE_BYTES)}.`);
      return;
    }
    setSubmitting(true);
    setError(null);
    if (requireSignIn) {
      try {
        await api.login((values.username ?? "").trim(), values.password ?? "");
      } catch (e) {
        setSubmitting(false);
        setError(
          e instanceof ApiError
            ? `Sign-in failed: ${e.message}. Use the admin account of this new install.`
            : "Sign-in failed. Use the admin account of this new install.",
        );
        return;
      }
    }
    try {
      const res = await api.restoreBackup(file, values.passphrase);
      const data = (res.data ?? {}) as { restarting?: boolean };
      close();
      if (data.restarting) {
        Modal.success({
          title: "Backup restored — restarting",
          content:
            res.message ??
            "The stack is restarting with the restored deployment. This page reconnects by itself; then sign in with that deployment's accounts.",
          okText: "OK",
        });
        void waitForRestartAndReload();
      } else {
        Modal.success({
          title: "Backup restored",
          content:
            res.message ??
            "Restart the stack (v2-stop, then v2-start) to load the restored deployment, then sign in with its accounts.",
        });
      }
    } catch (e) {
      const msg = e instanceof ApiError ? e.message : "Restore failed";
      setError(
        /passphrase|decrypt|key/i.test(msg) && !values.passphrase
          ? `${msg} — enter the backup passphrase, or configure this install with the same backup key the original used.`
          : msg,
      );
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      title="Restore from a backup"
      open={open}
      onCancel={close}
      destroyOnHidden
      width={560}
      footer={[
        <Button key="cancel" onClick={close} disabled={submitting}>
          Cancel
        </Button>,
        <Button
          key="ok"
          type="primary"
          danger
          icon={<CloudUploadOutlined />}
          loading={submitting}
          onClick={() => form.submit()}
        >
          Restore and restart
        </Button>,
      ]}
    >
      <Typography.Paragraph type="secondary">
        This replaces this install&apos;s configuration, accounts, roles, API token, and saved state with
        the backup&apos;s. The stack restarts and this page reconnects on its own; afterwards you sign in
        with the <b>restored</b> deployment&apos;s accounts.
      </Typography.Paragraph>
      {error ? (
        <Alert type="error" showIcon title={error} style={{ marginBottom: 16 }} closable onClose={() => setError(null)} />
      ) : null}
      <Form form={form} layout="vertical" requiredMark={false} onFinish={submit} disabled={submitting}>
        {requireSignIn ? (
          <>
            <Form.Item
              name="username"
              label="Admin username (this install)"
              initialValue="admin"
              rules={[{ required: true, message: "Enter the admin username" }]}
            >
              <Input prefix={<UserOutlined />} autoComplete="username" />
            </Form.Item>
            <Form.Item
              name="password"
              label="Admin password (the initial password is fine)"
              rules={[{ required: true, message: "Enter the admin password" }]}
            >
              <Input.Password prefix={<LockOutlined />} autoComplete="current-password" />
            </Form.Item>
          </>
        ) : null}
        <Form.Item label="Backup archive" required>
          <Upload
            accept=".gz,.tgz,.enc,application/gzip,application/octet-stream"
            maxCount={1}
            showUploadList={false}
            beforeUpload={(f) => {
              setFile(f as unknown as File);
              setError(null);
              return false;
            }}
          >
            <Button icon={<UploadOutlined />}>{file ? "Choose a different file" : "Choose backup file"}</Button>
          </Upload>
          <div style={{ marginTop: 8 }}>
            {file ? (
              <Typography.Text>
                {file.name} <Typography.Text type="secondary">({formatSize(file.size)}{encrypted ? ", encrypted" : ""})</Typography.Text>
              </Typography.Text>
            ) : (
              <Typography.Text type="secondary">
                .tar.gz.enc (encrypted, the default) or .tar.gz
              </Typography.Text>
            )}
          </div>
        </Form.Item>
        <Form.Item
          name="passphrase"
          label="Backup passphrase"
          extra={
            encrypted
              ? "Required for passphrase-encrypted backups (downloads and manual snapshots). Scheduled backups are sealed with the server backup key instead: configure the same NCC_BACKUP_KEY on this install and leave this empty."
              : "Only needed for encrypted (.enc) backups."
          }
          style={{ marginBottom: 0 }}
        >
          <Input.Password prefix={<KeyOutlined />} autoComplete="off" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
