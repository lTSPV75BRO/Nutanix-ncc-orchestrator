import { useState } from "react";
import { Alert, Button, Divider, Form, Input, Modal, Typography } from "antd";
import { CloudUploadOutlined, LockOutlined } from "@ant-design/icons";
import { api, ApiError } from "../api/client";
import { AuthShell } from "../components/AuthShell";
import { RestoreBackupDialog } from "../features/backup/RestoreBackupDialog";

const { Title, Text } = Typography;

// Mirrors minPasswordLen in cmd/ncc-api-server/usersapi.go.
const MIN_PASSWORD_LEN = 8;

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
    setSubmitting(true);
    setError(null);
    try {
      await api.changePassword(values.current, values.next);
      onSuccess();
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Could not change the password");
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
          extra={`At least ${MIN_PASSWORD_LEN} characters. A few unrelated words make a strong, memorable password.`}
          rules={[
            { required: true, message: "Enter a new password" },
            { min: MIN_PASSWORD_LEN, message: `Use at least ${MIN_PASSWORD_LEN} characters` },
            ({ getFieldValue }) => ({
              validator: (_, value) =>
                value && value === getFieldValue("current")
                  ? Promise.reject(new Error("Choose a password different from the current one"))
                  : Promise.resolve(),
            }),
          ]}
        >
          <Input.Password prefix={<LockOutlined />} autoComplete="new-password" />
        </Form.Item>
        <Form.Item
          name="confirm"
          label="Confirm new password"
          dependencies={["next"]}
          rules={[
            { required: true, message: "Re-enter the new password" },
            ({ getFieldValue }) => ({
              validator: (_, value) =>
                !value || value === getFieldValue("next")
                  ? Promise.resolve()
                  : Promise.reject(new Error("The passwords do not match")),
            }),
          ]}
        >
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
  /** Offer "restore from backup" — only for the bootstrap admin of a new install. */
  offerRestore?: boolean;
  onSuccess: () => void;
};

/**
 * Full-screen forced password-change gate. Shown when the logged-in account is
 * flagged `must_change_password` (the bootstrap admin and admin-reset accounts).
 */
export function ChangePasswordPage({ username, forced, offerRestore, onSuccess }: ChangePasswordPageProps) {
  const [restoreOpen, setRestoreOpen] = useState(false);
  return (
    <AuthShell>
      <div className="auth-heading">
        <Title level={2} className="auth-title">
          Choose a new password
        </Title>
        <Text type="secondary">
          {forced
            ? `${username ? `${username}, you` : "You"} signed in with a temporary password. Set your own to continue.`
            : "Update your account password."}
        </Text>
      </div>
      <ChangePasswordForm submitLabel="Save and continue" onSuccess={onSuccess} />
      {forced && offerRestore ? (
        <>
          <Divider plain>or</Divider>
          <Text type="secondary" style={{ display: "block", fontSize: 13, marginBottom: 8 }}>
            Moving an existing install here? Restore its backup instead — you&apos;ll keep its
            accounts and won&apos;t need a new password.
          </Text>
          <Button block icon={<CloudUploadOutlined />} onClick={() => setRestoreOpen(true)}>
            Restore from a backup
          </Button>
          <RestoreBackupDialog open={restoreOpen} onClose={() => setRestoreOpen(false)} />
        </>
      ) : null}
    </AuthShell>
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
