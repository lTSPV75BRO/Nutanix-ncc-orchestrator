import { useState } from "react";
import { Alert, Button, Card, Divider, Form, Input, Modal, Typography, message } from "antd";
import {
  InfoCircleOutlined,
  LockOutlined,
  LoginOutlined,
  SafetyOutlined,
  UserOutlined,
} from "@ant-design/icons";
import { api, ApiError } from "../api/client";

const { Title, Text } = Typography;

type LoginPageProps = {
  localEnabled: boolean;
  samlEnabled: boolean;
  /** When true, the built-in admin still has its initial bootstrap password. */
  bootstrapPending: boolean;
  onSuccess: () => void;
};

/**
 * Full-screen login gate shown when the backend has interactive login enabled
 * and the current browser has no authenticated session. Offers local
 * username/password sign-in and/or a "Sign in with SSO" button (SAML).
 */
export function LoginPage({ localEnabled, samlEnabled, bootstrapPending, onSuccess }: LoginPageProps) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [forgotOpen, setForgotOpen] = useState(false);
  const [forgotSubmitting, setForgotSubmitting] = useState(false);
  const [forgotForm] = Form.useForm<{ username: string }>();

  const handleForgot = async (values: { username: string }) => {
    setForgotSubmitting(true);
    try {
      await api.forgotPassword(values.username.trim());
      // The backend never reveals whether the account exists; keep it generic.
      message.success(
        "If that account exists, an administrator has been notified to reset it.",
      );
      setForgotOpen(false);
      forgotForm.resetFields();
    } catch (e) {
      const msg = e instanceof ApiError ? e.message : "Request failed";
      message.error(msg);
    } finally {
      setForgotSubmitting(false);
    }
  };

  const handleFinish = async (values: { username: string; password: string }) => {
    setSubmitting(true);
    setError(null);
    try {
      await api.login(values.username.trim(), values.password);
      onSuccess();
    } catch (e) {
      const msg = e instanceof ApiError ? e.message : "Login failed";
      setError(msg);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 24,
      }}
    >
      <Card style={{ width: 380, maxWidth: "100%" }}>
        <div style={{ textAlign: "center", marginBottom: 16 }}>
          <img src="/logo.svg" alt="" style={{ height: 40, marginBottom: 8 }} />
          <Title level={4} style={{ marginBottom: 0 }}>
            NCC Orchestrator
          </Title>
          <Text type="secondary">Sign in to continue</Text>
        </div>

        {error ? (
          <Alert type="error" showIcon message={error} style={{ marginBottom: 16 }} closable onClose={() => setError(null)} />
        ) : null}

        {localEnabled ? (
          <Form layout="vertical" onFinish={handleFinish} requiredMark={false} disabled={submitting}>
            <Form.Item
              name="username"
              label="Username"
              rules={[{ required: true, message: "Enter your username" }]}
            >
              <Input name="username" prefix={<UserOutlined />} autoComplete="username" autoFocus />
            </Form.Item>
            <Form.Item
              name="password"
              label="Password"
              rules={[{ required: true, message: "Enter your password" }]}
            >
              <Input.Password name="password" prefix={<LockOutlined />} autoComplete="current-password" />
            </Form.Item>
            <Form.Item style={{ marginBottom: 0 }}>
              <Button type="primary" htmlType="submit" block loading={submitting} icon={<LoginOutlined />}>
                Sign in
              </Button>
            </Form.Item>
            <div style={{ textAlign: "center", marginTop: 8 }}>
              <Button type="link" size="small" onClick={() => setForgotOpen(true)} style={{ padding: 0 }}>
                Forgot password?
              </Button>
            </div>
          </Form>
        ) : null}

        {localEnabled && samlEnabled ? <Divider plain>or</Divider> : null}

        {samlEnabled ? (
          <Button
            block
            icon={<SafetyOutlined />}
            onClick={() => {
              window.location.href = "/saml/login";
            }}
          >
            Sign in with SSO
          </Button>
        ) : null}

        {!localEnabled && !samlEnabled ? (
          <Alert type="info" showIcon message="No login methods are configured on the server." />
        ) : null}

        {localEnabled && bootstrapPending ? (
          <>
            <Divider plain style={{ marginTop: 24 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                First time signing in?
              </Text>
            </Divider>
            <Alert
              type="info"
              showIcon
              icon={<InfoCircleOutlined />}
              message="Where's the admin password?"
              description={
                <Typography.Paragraph style={{ marginBottom: 0, fontSize: 12 }}>
                  On first launch the server creates an <Text code>admin</Text> account with a
                  random password and requires you to change it. Retrieve it from:
                  <ul style={{ margin: "8px 0 0", paddingLeft: 18 }}>
                    <li>
                      the <Text strong>api-server console / logs</Text> (the{" "}
                      <Text code>FIRST-RUN ADMIN CREATED</Text> banner), or the{" "}
                      <Text code>.ncc-initial-admin-password</Text> file beside the user database;
                    </li>
                    <li>
                      on <Text strong>Kubernetes</Text>, the{" "}
                      <Text code>initial-admin-password</Text> key of the user-database Secret, e.g.{" "}
                      <Text code copyable={{ text: "kubectl get secret ncc-v2-users -o jsonpath='{.data.initial-admin-password}' | base64 -d" }}>
                        kubectl get secret …
                      </Text>
                    </li>
                  </ul>
                </Typography.Paragraph>
              }
            />
          </>
        ) : null}
      </Card>

      <Modal
        title="Reset your password"
        open={forgotOpen}
        onCancel={() => {
          setForgotOpen(false);
          forgotForm.resetFields();
        }}
        okText="Send request"
        confirmLoading={forgotSubmitting}
        onOk={() => forgotForm.submit()}
        destroyOnClose
      >
        <Typography.Paragraph type="secondary" style={{ fontSize: 13 }}>
          Enter your username and an administrator will be asked to reset your
          password. They will give you a temporary password to change at next
          login. No email is sent.
        </Typography.Paragraph>
        <Form form={forgotForm} layout="vertical" requiredMark={false} onFinish={handleForgot}>
          <Form.Item
            name="username"
            label="Username"
            rules={[{ required: true, message: "Enter your username" }]}
          >
            <Input prefix={<UserOutlined />} autoComplete="username" autoFocus />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
