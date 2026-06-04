import { useState } from "react";
import { Alert, Button, Card, Form, Input, Modal, Typography } from "antd";
import { LockOutlined } from "@ant-design/icons";
import { api, ApiError } from "../api/client";

const { Title, Text } = Typography;

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
        <Alert type="error" showIcon message={error} style={{ marginBottom: 16 }} closable onClose={() => setError(null)} />
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
    <Modal title="Change password" open={open} onCancel={onClose} footer={null} destroyOnClose>
      <ChangePasswordForm
        onSuccess={() => {
          onSuccess();
          onClose();
        }}
      />
    </Modal>
  );
}
