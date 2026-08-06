import { useState } from "react";
import { Alert, Button, Card, Divider, Form, Input, Modal, Typography, Upload } from "antd";
import { LockOutlined, UploadOutlined, WarningOutlined } from "@ant-design/icons";
import { api, ApiError } from "../api/client";
import { notifyError } from "../notify";

const { Title, Text } = Typography;

/**
 * Poll the backend health endpoint until the restarted stack answers, then
 * reload so the app re-bootstraps against the restored deployment (the restored
 * accounts/token replace the bootstrap admin, so the forced-change gate clears).
 */
async function waitForRestartAndReload() {
  const deadline = Date.now() + 120_000;
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
}

/**
 * Restore-from-backup control offered on the first-login gate. Lets the
 * bootstrap admin recover an existing deployment instead of setting a new
 * password: the restored user database carries the old admin credentials
 * (no forced change), so after the automatic restart the admin signs in exactly
 * as on the original deployment.
 */
function FirstLoginRestore() {
  const [restoring, setRestoring] = useState(false);

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
            "The stack is restarting to load the restored deployment. This page will reconnect automatically, then sign in with your original password.",
          okText: "OK",
        });
        void waitForRestartAndReload();
      } else {
        Modal.success({
          title: "Backup restored",
          content:
            res.message ??
            "Restart the stack (v2-stop then v2-start) for the restored deployment to take effect, then sign in with your original password.",
        });
      }
    } catch (e) {
      notifyError(e, "Restore failed");
    } finally {
      setRestoring(false);
    }
  };

  const confirmRestore = (file: File): boolean => {
    Modal.confirm({
      title: "Restore from backup?",
      icon: <WarningOutlined style={{ color: "#faad14" }} />,
      width: 520,
      content: (
        <div>
          <Typography.Paragraph>
            This overwrites this fresh install with the contents of <b>{file.name}</b> — the
            configuration, local accounts and roles, the API token, and all other backed-up state.
          </Typography.Paragraph>
          <Typography.Paragraph style={{ marginBottom: 0 }}>
            You will <b>not</b> need to set a new password: the restored deployment keeps its
            original admin account. The stack restarts automatically and this page reconnects on its
            own — then sign in with your <b>original</b> password.
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
    <>
      <Divider plain style={{ marginTop: 20, marginBottom: 12, fontSize: 12 }}>
        or
      </Divider>
      <Typography.Paragraph type="secondary" style={{ fontSize: 13, textAlign: "center", marginBottom: 12 }}>
        Restoring an existing deployment? Upload its backup to recover everything — you&apos;ll keep
        your original password instead of setting a new one.
      </Typography.Paragraph>
      <Upload
        accept=".gz,.tgz,.tar.gz,application/gzip"
        showUploadList={false}
        beforeUpload={(file) => confirmRestore(file as unknown as File)}
      >
        <Button icon={<UploadOutlined />} block loading={restoring}>
          Restore from backup…
        </Button>
      </Upload>
    </>
  );
}

type ChangePasswordFormProps = {
  submitLabel?: string;
  onSuccess: () => void;
};

/**
 * Shared current/new/confirm password form. Used by both the forced full-screen
 * gate and the self-service modal so the dialog is identical in both places.
 */
export function ChangePasswordForm({ submitLabel = "Change password", onSuccess }: ChangePasswordFormProps) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFinish = async (values: { current: string; next: string; confirm: string }) => {
    if (values.next !== values.confirm) {
      setError("New passwords do not match");
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      await api.changePassword(values.current, values.next);
      onSuccess();
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Failed to change password");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <>
      {error ? (
        <Alert type="error" showIcon title={error} style={{ marginBottom: 16 }} closable onClose={() => setError(null)} />
      ) : null}
      <Form layout="vertical" onFinish={handleFinish} requiredMark={false} disabled={submitting}>
        <Form.Item name="current" label="Current password" rules={[{ required: true, message: "Enter your current password" }]}>
          <Input.Password prefix={<LockOutlined />} autoComplete="current-password" autoFocus />
        </Form.Item>
        <Form.Item
          name="next"
          label="New password"
          rules={[
            { required: true, message: "Enter a new password" },
            { min: 8, message: "Use at least 8 characters" },
          ]}
        >
          <Input.Password prefix={<LockOutlined />} autoComplete="new-password" />
        </Form.Item>
        <Form.Item name="confirm" label="Confirm new password" rules={[{ required: true, message: "Re-enter the new password" }]}>
          <Input.Password prefix={<LockOutlined />} autoComplete="new-password" />
        </Form.Item>
        <Form.Item style={{ marginBottom: 0 }}>
          <Button type="primary" htmlType="submit" block loading={submitting}>
            {submitLabel}
          </Button>
        </Form.Item>
      </Form>
    </>
  );
}

type ChangePasswordPageProps = {
  username?: string;
  forced?: boolean;
  onSuccess: () => void;
};

/**
 * Full-screen forced password-change gate. Shown when the logged-in account is
 * flagged `must_change_password` (the bootstrap admin and admin-reset accounts).
 */
export function ChangePasswordPage({ username, forced, onSuccess }: ChangePasswordPageProps) {
  return (
    <div style={{ minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", padding: 24 }}>
      <Card style={{ width: 420, maxWidth: "100%" }}>
        <div style={{ textAlign: "center", marginBottom: 16 }}>
          <Title level={4} style={{ marginBottom: 0 }}>
            Set a new password
          </Title>
          <Text type="secondary">
            {forced
              ? `${username ? `${username}, you` : "You"} must change your password before continuing.`
              : "Update your account password."}
          </Text>
        </div>
        <ChangePasswordForm onSuccess={onSuccess} />
        {forced ? <FirstLoginRestore /> : null}
      </Card>
    </div>
  );
}

type ChangePasswordModalProps = {
  open: boolean;
  onClose: () => void;
  onSuccess: () => void;
};

/**
 * Self-service change-password dialog launched from the header user menu. Reuses
 * the same form as the forced gate; closes itself on success.
 */
export function ChangePasswordModal({ open, onClose, onSuccess }: ChangePasswordModalProps) {
  return (
    <Modal title="Change password" open={open} onCancel={onClose} footer={null} destroyOnHidden>
      <ChangePasswordForm
        onSuccess={() => {
          onSuccess();
          onClose();
        }}
      />
    </Modal>
  );
}
