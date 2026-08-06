import { useState } from "react";
import { Alert, Button, Card, Divider, Form, Input, Modal, Tooltip, Typography, Upload, message } from "antd";
import {
  CloudUploadOutlined,
  InfoCircleOutlined,
  LockOutlined,
  LoginOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  SafetyOutlined,
  UploadOutlined,
  UserOutlined,
} from "@ant-design/icons";
import { api, ApiError } from "../api/client";
import { useLocalStorageState } from "../hooks/useLocalStorageState";

// Poll the backend health endpoint until the restarted stack answers, then
// reload so the SPA re-bootstraps against the restored deployment.
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

const { Title, Text } = Typography;

// Animated login backdrop: a few large, blurred gradient orbs that drift
// slowly behind the sign-in card. Rendered only when the user has the
// animation enabled (preference persisted in localStorage). Honors
// prefers-reduced-motion by freezing the orbs in place.
const LOGIN_BG_CSS = `
.ncc-login-bg {
  position: absolute;
  inset: 0;
  z-index: 0;
  overflow: hidden;
  pointer-events: none;
}
.ncc-login-orb {
  position: absolute;
  display: block;
  border-radius: 50%;
  filter: blur(70px);
  opacity: 0.55;
  will-change: transform;
}
.ncc-login-orb-1 {
  width: 460px; height: 460px;
  top: -120px; left: -100px;
  background: radial-gradient(circle at 30% 30%, #4f8cff, #2b5cff 60%, transparent 70%);
  animation: nccOrbA 18s ease-in-out infinite;
}
.ncc-login-orb-2 {
  width: 520px; height: 520px;
  bottom: -160px; right: -120px;
  background: radial-gradient(circle at 70% 70%, #8b5cf6, #d946ef 60%, transparent 72%);
  animation: nccOrbB 22s ease-in-out infinite;
}
.ncc-login-orb-3 {
  width: 360px; height: 360px;
  top: 40%; left: 55%;
  background: radial-gradient(circle at 50% 50%, #22d3ee, #0ea5e9 60%, transparent 72%);
  animation: nccOrbC 26s ease-in-out infinite;
}
@keyframes nccOrbA {
  0%   { transform: translate(0, 0) scale(1); }
  50%  { transform: translate(60px, 40px) scale(1.12); }
  100% { transform: translate(0, 0) scale(1); }
}
@keyframes nccOrbB {
  0%   { transform: translate(0, 0) scale(1); }
  50%  { transform: translate(-70px, -50px) scale(1.08); }
  100% { transform: translate(0, 0) scale(1); }
}
@keyframes nccOrbC {
  0%   { transform: translate(0, 0) scale(1); }
  50%  { transform: translate(-50px, 60px) scale(1.15); }
  100% { transform: translate(0, 0) scale(1); }
}
@media (prefers-reduced-motion: reduce) {
  .ncc-login-orb { animation: none !important; }
}
`;

type LoginPageProps = {
  localEnabled: boolean;
  samlEnabled: boolean;
  /** When true, the built-in admin still has its initial bootstrap password. */
  bootstrapPending: boolean;
  onSuccess: () => void;
  /** Called after a successful admin self-reset so the caller can refresh
   *  bootstrap_pending and show the retrieval hint immediately. */
  onBootstrapReset?: () => void;
};

/**
 * Full-screen login gate shown when the backend has interactive login enabled
 * and the current browser has no authenticated session. Offers local
 * username/password sign-in and/or a "Sign in with SSO" button (SAML).
 */
export function LoginPage({ localEnabled, samlEnabled, bootstrapPending, onSuccess, onBootstrapReset }: LoginPageProps) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Animated background preference persists across visits (per browser).
  const [animateBg, setAnimateBg] = useLocalStorageState<boolean>("ncc.login.animateBg", true);
  const [forgotOpen, setForgotOpen] = useState(false);
  const [forgotSubmitting, setForgotSubmitting] = useState(false);
  const [forgotForm] = Form.useForm<{ username: string }>();
  const [restoreOpen, setRestoreOpen] = useState(false);
  const [restoreSubmitting, setRestoreSubmitting] = useState(false);
  const [restoreFile, setRestoreFile] = useState<File | null>(null);
  const [restoreForm] = Form.useForm<{ username: string; password: string }>();

  // First-login restore: authenticate with the bootstrap admin credentials (the
  // restore endpoint is the one action allowed through the forced-change gate),
  // upload the archive, then let the orchestrator restart the stack. After the
  // reload the restored deployment governs: the old admin's stored state decides
  // whether a password change is still owed, so it behaves exactly as the
  // original deployment did (a still-pending admin is prompted to reset).
  const handleRestore = async (values: { username: string; password: string }) => {
    if (!restoreFile) {
      message.error("Choose a backup archive (.tar.gz) to restore.");
      return;
    }
    setRestoreSubmitting(true);
    try {
      await api.login(values.username.trim(), values.password);
    } catch (e) {
      setRestoreSubmitting(false);
      message.error(
        e instanceof ApiError
          ? `Sign-in failed: ${e.message}. Enter the current bootstrap admin password to restore.`
          : "Sign-in failed. Enter the current bootstrap admin password to restore.",
      );
      return;
    }
    try {
      const res = await api.restoreBackup(restoreFile);
      const data = (res.data ?? {}) as { restarting?: boolean };
      setRestoreOpen(false);
      if (data.restarting) {
        Modal.success({
          title: "Backup restored — restarting",
          content:
            res.message ??
            "The stack is restarting to load the restored deployment. This page will reconnect automatically — then sign in with the credentials from the restored deployment (you may be prompted to set a password if that deployment still required it).",
          okText: "OK",
        });
        void waitForRestartAndReload();
      } else {
        Modal.success({
          title: "Backup restored",
          content:
            res.message ??
            "Restart the stack (v2-stop then v2-start) for the restored deployment to take effect, then sign in with its credentials.",
        });
      }
    } catch (e) {
      message.error(e instanceof ApiError ? `Restore failed: ${e.message}` : "Restore failed");
    } finally {
      setRestoreSubmitting(false);
    }
  };

  const handleForgot = async (values: { username: string }) => {
    setForgotSubmitting(true);
    try {
      const res = await api.forgotPassword(values.username.trim());
      // For the built-in admin the server self-resets to a fresh random
      // password (first-run workflow) and returns tailored guidance; for every
      // other account it stays generic and never reveals whether it exists.
      if (res.data?.admin_reset) {
        message.success(
          res.message ??
            "A new temporary password for the admin account was generated. Retrieve it from the server logs or the .ncc-initial-admin-password file, then sign in and change it.",
          8,
        );
        // The admin now owes a forced change again — refresh so the login
        // screen re-shows the "where's the admin password?" retrieval hint.
        onBootstrapReset?.();
      } else {
        message.success(
          res.message ??
            "If that account exists, an administrator has been notified to reset it.",
        );
      }
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
        position: "relative",
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 24,
        overflow: "hidden",
      }}
    >
      <style>{LOGIN_BG_CSS}</style>
      {animateBg ? (
        <div className="ncc-login-bg" aria-hidden="true">
          <span className="ncc-login-orb ncc-login-orb-1" />
          <span className="ncc-login-orb ncc-login-orb-2" />
          <span className="ncc-login-orb ncc-login-orb-3" />
        </div>
      ) : null}

      <Tooltip title={animateBg ? "Stop background animation" : "Start background animation"}>
        <Button
          type="text"
          size="small"
          aria-label={animateBg ? "Stop background animation" : "Start background animation"}
          icon={animateBg ? <PauseCircleOutlined /> : <PlayCircleOutlined />}
          onClick={() => setAnimateBg((v) => !v)}
          style={{ position: "absolute", top: 16, right: 16, zIndex: 2 }}
        >
          {animateBg ? "Stop animation" : "Animate"}
        </Button>
      </Tooltip>

      <Card style={{ width: 380, maxWidth: "100%", position: "relative", zIndex: 1 }}>
        <div style={{ textAlign: "center", marginBottom: 16 }}>
          <img src="/logo.svg" alt="" style={{ height: 40, marginBottom: 8 }} />
          <Title level={4} style={{ marginBottom: 0 }}>
            NCC Orchestrator
          </Title>
          <Text type="secondary">Sign in to continue</Text>
        </div>

        {error ? (
          <Alert type="error" showIcon title={error} style={{ marginBottom: 16 }} closable onClose={() => setError(null)} />
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
          <Alert type="info" showIcon title="No login methods are configured on the server." />
        ) : null}

        {localEnabled && bootstrapPending ? (
          <>
            <Divider plain style={{ marginTop: 24 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                First-time sign-in
              </Text>
            </Divider>
            <Alert
              type="info"
              showIcon
              icon={<InfoCircleOutlined />}
              title="Initial administrator password"
              description={
                <Typography.Paragraph style={{ marginBottom: 0, fontSize: 12 }}>
                  On first launch, the server creates an <Text code>admin</Text> account with a
                  randomly generated password that must be changed at first sign-in. You can
                  retrieve it from:
                  <ul style={{ margin: "8px 0 0", paddingLeft: 18 }}>
                    <li>
                      the api-server console output (the{" "}
                      <Text code>FIRST-RUN ADMIN CREATED</Text> banner) or the{" "}
                      <Text code>.ncc-initial-admin-password</Text> file beside the user database; or
                    </li>
                    <li>
                      on Kubernetes, the <Text code>initial-admin-password</Text> key of the
                      user-database Secret:{" "}
                      <Text code copyable={{ text: "kubectl get secret ncc-v2-users -o jsonpath='{.data.initial-admin-password}' | base64 -d" }}>
                        kubectl get secret …
                      </Text>
                    </li>
                  </ul>
                </Typography.Paragraph>
              }
            />
            <Button
              block
              icon={<CloudUploadOutlined />}
              onClick={() => setRestoreOpen(true)}
              style={{ marginTop: 12 }}
            >
              Restore from a backup
            </Button>
            <Typography.Paragraph type="secondary" style={{ fontSize: 12, marginTop: 8, marginBottom: 0 }}>
              Migrating an existing deployment? Upload its backup to restore all accounts, roles,
              configuration, and the most recent run, then continue without further setup.
            </Typography.Paragraph>
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
        destroyOnHidden
      >
        <Typography.Paragraph type="secondary" style={{ fontSize: 13 }}>
          Enter your username and an administrator will be asked to reset your
          password. They will give you a temporary password to change at next
          login. No email is sent. If you are the built-in <code>admin</code> and
          are locked out, a fresh temporary password is generated and written to
          the server logs and the <code>.ncc-initial-admin-password</code> file.
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

      <Modal
        title="Restore from backup"
        open={restoreOpen}
        onCancel={() => {
          setRestoreOpen(false);
          setRestoreFile(null);
          restoreForm.resetFields();
        }}
        okText="Restore and restart"
        okButtonProps={{ danger: true, icon: <CloudUploadOutlined /> }}
        confirmLoading={restoreSubmitting}
        onOk={() => restoreForm.submit()}
        destroyOnHidden
      >
        <Typography.Paragraph type="secondary" style={{ fontSize: 13 }}>
          Recover an existing deployment onto this fresh install. Sign in with the current{" "}
          <code>admin</code> bootstrap password, choose its backup archive, and the stack will be
          restored and <b>restarted automatically</b>. Afterwards you&apos;ll sign in with the{" "}
          <b>restored</b> deployment&apos;s credentials — if that admin still owed a password change,
          you&apos;ll be prompted to set one, exactly as on the original deployment.
        </Typography.Paragraph>
        <Form
          form={restoreForm}
          layout="vertical"
          requiredMark={false}
          onFinish={handleRestore}
          disabled={restoreSubmitting}
        >
          <Form.Item
            name="username"
            label="Admin username"
            initialValue="admin"
            rules={[{ required: true, message: "Enter the admin username" }]}
          >
            <Input prefix={<UserOutlined />} autoComplete="username" />
          </Form.Item>
          <Form.Item
            name="password"
            label="Bootstrap admin password"
            rules={[{ required: true, message: "Enter the bootstrap admin password" }]}
          >
            <Input.Password prefix={<LockOutlined />} autoComplete="current-password" />
          </Form.Item>
          <Form.Item label="Backup archive" style={{ marginBottom: 0 }}>
            <Upload
              accept=".gz,.tgz,.tar.gz,application/gzip"
              maxCount={1}
              showUploadList={false}
              beforeUpload={(file) => {
                setRestoreFile(file as unknown as File);
                return false;
              }}
            >
              <Button icon={<UploadOutlined />}>Choose backup (.tar.gz)</Button>
            </Upload>
            {restoreFile ? (
              <Typography.Text style={{ marginLeft: 8 }}>{restoreFile.name}</Typography.Text>
            ) : (
              <Typography.Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>
                No file selected
              </Typography.Text>
            )}
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
