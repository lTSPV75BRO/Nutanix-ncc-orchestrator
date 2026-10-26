import { useState, type KeyboardEvent } from "react";
import { Alert, Button, Collapse, Divider, Form, Input, Modal, Space, Typography } from "antd";
import {
  CloudUploadOutlined,
  CopyOutlined,
  DownloadOutlined,
  LockOutlined,
  LoginOutlined,
  SafetyCertificateOutlined,
  SafetyOutlined,
  UserOutlined,
} from "@ant-design/icons";
import { useQuery } from "@tanstack/react-query";
import { api, ApiError } from "../api/client";
import type { PublicTLSInfo } from "../api/types";
import { AuthShell } from "../components/AuthShell";
import { RestoreBackupDialog } from "../features/backup/RestoreBackupDialog";
import { notify } from "../notify";

const { Title, Text, Paragraph } = Typography;

/** One-shot message for the next login screen (session expired, signed out). */
export type LoginNotice = { type: "info" | "warning"; text: string };

const LOGIN_NOTICE_KEY = "ncc.login.notice";

/** Leave a notice for the login screen that renders next (survives a reload). */
export function setLoginNotice(notice: LoginNotice): void {
  try {
    window.sessionStorage.setItem(LOGIN_NOTICE_KEY, JSON.stringify(notice));
  } catch {
    // storage unavailable — the notice is a nicety
  }
}

function takeLoginNotice(): LoginNotice | null {
  try {
    const raw = window.sessionStorage.getItem(LOGIN_NOTICE_KEY);
    window.sessionStorage.removeItem(LOGIN_NOTICE_KEY);
    if (!raw) return null;
    const n = JSON.parse(raw) as LoginNotice;
    return n && typeof n.text === "string" ? n : null;
  } catch {
    return null;
  }
}

type LoginPageProps = {
  localEnabled: boolean;
  /** Active Directory / LDAP sign-in is configured (works without local accounts). */
  ldapEnabled?: boolean;
  samlEnabled: boolean;
  /** When true, the built-in admin still has its initial bootstrap password. */
  bootstrapPending: boolean;
  onSuccess: () => void;
  /** Called after an admin self-reset so the caller can refresh bootstrap state. */
  onBootstrapReset?: () => void;
  /** /auth/me failed — offer Retry so a hung bootstrap is not a dead end. */
  apiUnreachable?: boolean;
  onRetryBootstrap?: () => void;
};

function isNonLocalHTTP(): boolean {
  if (typeof window === "undefined") return false;
  if (window.location.protocol !== "http:") return false;
  const h = window.location.hostname;
  return h !== "localhost" && h !== "127.0.0.1" && h !== "[::1]";
}

function httpsURL(): string {
  if (typeof window === "undefined") return "";
  const { hostname, port, pathname, search, hash } = window.location;
  const p = port && port !== "80" && port !== "443" ? `:${port}` : "";
  return `https://${hostname}${p}${pathname}${search}${hash}`;
}

function downloadCertPEM(pem: string) {
  const blob = new Blob([pem], { type: "application/x-x509-ca-cert" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "ncc-ui.crt";
  a.click();
  URL.revokeObjectURL(url);
}

async function copyText(text: string, ok: string) {
  try {
    await navigator.clipboard.writeText(text);
    notify.success(ok);
  } catch {
    notify.info(text);
  }
}

function TrustCertNotice({ tls }: { tls: PublicTLSInfo | undefined }) {
  if (!tls?.https_enabled && !tls?.cert_pem) return null;
  const fp = tls.fingerprint_sha256;
  const pem = tls.cert_pem;
  return (
    <Collapse
      className="auth-collapse"
      size="small"
      items={[
        {
          key: "cert",
          label: (
            <span>
              <SafetyCertificateOutlined /> Browser warns about the certificate?
            </span>
          ),
          children: (
            <Space orientation="vertical" size={8} style={{ width: "100%" }}>
              <Text type="secondary" style={{ fontSize: 13 }}>
                This install uses a self-signed certificate. Download it and trust it in your browser
                or OS, or compare the fingerprint below with the one your administrator gives you.
              </Text>
              {fp ? (
                <>
                  <Paragraph code style={{ marginBottom: 0, fontSize: 11, wordBreak: "break-all" }}>
                    {fp}
                  </Paragraph>
                  <Space wrap>
                    <Button size="small" icon={<CopyOutlined />} onClick={() => void copyText(fp, "Fingerprint copied")}>
                      Copy fingerprint
                    </Button>
                    {pem ? (
                      <Button size="small" icon={<DownloadOutlined />} onClick={() => downloadCertPEM(pem)}>
                        Download certificate
                      </Button>
                    ) : null}
                  </Space>
                </>
              ) : (
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {tls.message || "Certificate details are not available yet. Reload after HTTPS finishes starting."}
                </Text>
              )}
            </Space>
          ),
        },
      ]}
    />
  );
}

/**
 * Full-screen sign-in shown when the backend has interactive login enabled and
 * this browser has no session. Local and directory (AD/LDAP) accounts share
 * the username/password form; SAML SSO has its own button.
 */
export function LoginPage({
  localEnabled,
  ldapEnabled,
  samlEnabled,
  bootstrapPending,
  onSuccess,
  onBootstrapReset,
  apiUnreachable,
  onRetryBootstrap,
}: LoginPageProps) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<{ text: string; throttled: boolean } | null>(null);
  const [notice] = useState<LoginNotice | null>(() => takeLoginNotice());
  const [capsLock, setCapsLock] = useState(false);
  const [forgotOpen, setForgotOpen] = useState(false);
  const [forgotSubmitting, setForgotSubmitting] = useState(false);
  const [forgotForm] = Form.useForm<{ username: string }>();
  const [restoreOpen, setRestoreOpen] = useState(false);
  const httpInsecure = isNonLocalHTTP();
  const tlsQuery = useQuery({ queryKey: ["tls-public"], queryFn: api.tlsPublic, staleTime: 60_000, retry: 0 });
  const healthQuery = useQuery({ queryKey: ["health"], queryFn: api.health, staleTime: 15_000, retry: 0 });
  const cookieSecure = Boolean(healthQuery.data?.cookie_secure);
  // Before this change the form only appeared with local accounts, so an
  // install that signs in through Active Directory alone showed no form.
  const passwordLogin = localEnabled || Boolean(ldapEnabled);

  const trackCapsLock = (e: KeyboardEvent<HTMLInputElement>) => {
    setCapsLock(Boolean(e.getModifierState?.("CapsLock")));
  };

  const handleForgot = async (values: { username: string }) => {
    setForgotSubmitting(true);
    try {
      const res = await api.forgotPassword(values.username.trim());
      // With --allow-admin-self-reset the server self-resets the built-in
      // admin and returns tailored guidance; otherwise (the default, and for
      // every other account) the reply is generic and never reveals whether
      // the account exists.
      if (res.data?.admin_reset) {
        notify.success({
          message:
            res.message ??
            "A new temporary password for the admin account was generated. Retrieve it from the server, then sign in and change it.",
          duration: 8,
        });
        onBootstrapReset?.();
      } else {
        notify.success(res.message ?? "If that account exists, an administrator has been notified to reset it.");
      }
      setForgotOpen(false);
      forgotForm.resetFields();
    } catch (e) {
      notify.error(e instanceof ApiError ? e.message : "Request failed");
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
      if (e instanceof ApiError) {
        setError({ text: e.message, throttled: e.status === 429 });
      } else {
        setError({ text: "Sign-in failed. Check your connection and try again.", throttled: false });
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <AuthShell>
      <div className="auth-heading">
        <Title level={2} className="auth-title">
          Sign in
        </Title>
        <Text type="secondary">Use your NCC Orchestrator{ldapEnabled ? " or directory" : ""} account.</Text>
      </div>

      <div className="auth-alerts" aria-live="polite">
        {notice ? <Alert type={notice.type} showIcon title={notice.text} /> : null}
        {apiUnreachable ? (
          <Alert
            type="warning"
            showIcon
            title="Can't reach the server"
            description="The API did not answer. It may be restarting — retry in a moment."
            action={
              onRetryBootstrap ? (
                <Button size="small" onClick={onRetryBootstrap}>
                  Retry
                </Button>
              ) : undefined
            }
          />
        ) : null}
        {httpInsecure && cookieSecure ? (
          <Alert
            type="error"
            showIcon
            title="Switch to HTTPS to stay signed in"
            description={
              <span>
                This server only sends secure cookies, which browsers drop on plain HTTP. Open{" "}
                <Typography.Link href={httpsURL()}>{httpsURL()}</Typography.Link> instead.
              </span>
            }
          />
        ) : null}
        {error ? (
          <Alert
            type={error.throttled ? "warning" : "error"}
            showIcon
            title={error.throttled ? "Too many attempts" : "Couldn't sign you in"}
            description={error.text}
            closable
            onClose={() => setError(null)}
          />
        ) : null}
      </div>

      {passwordLogin ? (
        <Form layout="vertical" onFinish={handleFinish} requiredMark={false} disabled={submitting} className="auth-form">
          <Form.Item
            name="username"
            extra={ldapEnabled ? "Directory users: your usual Windows/AD username (e.g. jdoe)." : undefined}
            rules={[{ required: true, message: "Enter your username" }]}
          >
            <Input
              name="username"
              size="large"
              prefix={<UserOutlined />}
              placeholder="Username"
              aria-label="Username"
              autoComplete="username"
              autoFocus
            />
          </Form.Item>
          <Form.Item
            name="password"
            rules={[{ required: true, message: "Enter your password" }]}
            extra={capsLock ? <span className="auth-capslock">Caps Lock is on</span> : undefined}
          >
            <Input.Password
              name="password"
              size="large"
              prefix={<LockOutlined />}
              placeholder="Password"
              aria-label="Password"
              autoComplete="current-password"
              onKeyUp={trackCapsLock}
              onKeyDown={trackCapsLock}
            />
          </Form.Item>
          <Button type="primary" htmlType="submit" size="large" block loading={submitting} icon={<LoginOutlined />}>
            Sign in
          </Button>
          <div className="auth-form-links">
            <Button type="link" size="small" onClick={() => setForgotOpen(true)}>
              Forgot password?
            </Button>
          </div>
        </Form>
      ) : null}

      {passwordLogin && samlEnabled ? <Divider plain>or</Divider> : null}

      {samlEnabled ? (
        <Button
          block
          size="large"
          type={passwordLogin ? "default" : "primary"}
          icon={<SafetyOutlined />}
          onClick={() => {
            window.location.href = "/saml/login";
          }}
        >
          Sign in with single sign-on
        </Button>
      ) : null}

      {!passwordLogin && !samlEnabled ? (
        <Alert
          type="info"
          showIcon
          title="No sign-in method is configured"
          description="An administrator needs to enable local accounts, Active Directory, or SSO on the server."
        />
      ) : null}

      {localEnabled && bootstrapPending ? (
        <section className="auth-first-run" aria-labelledby="first-run-title">
          <Title level={5} id="first-run-title" className="auth-first-run-title">
            New install? Two ways to start
          </Title>
          <ol className="auth-first-run-options">
            <li>
              <Text strong>Sign in as admin</Text> with the initial password the server generated. You&apos;ll
              choose a new password next.
              <Collapse
                className="auth-collapse"
                size="small"
                items={[
                  {
                    key: "where",
                    label: "Where do I find the initial password?",
                    children: (
                      <ul className="auth-where-list">
                        <li>
                          In the <Text code>.ncc-initial-admin-password</Text> file next to the user database
                          on the server, or in the api-server console under{" "}
                          <Text code>FIRST-RUN ADMIN CREATED</Text>.
                        </li>
                        <li>
                          On Kubernetes, in the user-database Secret:
                          <Paragraph
                            code
                            copyable={{ text: "kubectl -n <namespace> get secret ncc-v2-users -o jsonpath='{.data.initial-admin-password}' | base64 -d" }}
                            style={{ margin: "4px 0 0", fontSize: 12 }}
                          >
                            kubectl -n &lt;namespace&gt; get secret ncc-v2-users -o jsonpath=&apos;&#123;.data.initial-admin-password&#125;&apos; | base64 -d
                          </Paragraph>
                        </li>
                      </ul>
                    ),
                  },
                ]}
              />
            </li>
            <li>
              <Text strong>Moving an existing install here?</Text> Restore its backup instead — accounts,
              configuration, and history come with it.
              <div style={{ marginTop: 8 }}>
                <Button icon={<CloudUploadOutlined />} onClick={() => setRestoreOpen(true)}>
                  Restore from a backup
                </Button>
              </div>
            </li>
          </ol>
        </section>
      ) : null}

      <TrustCertNotice tls={tlsQuery.data} />

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
        <Paragraph type="secondary" style={{ fontSize: 13 }}>
          An administrator will be asked to reset your password and give you a temporary one to
          change at next sign-in. No email is sent.
        </Paragraph>
        {ldapEnabled ? (
          <Paragraph type="secondary" style={{ fontSize: 13 }}>
            Signing in with a directory (Active Directory) account? Reset that password the way you
            normally do with your IT team; this form only covers local accounts.
          </Paragraph>
        ) : null}
        <Paragraph type="secondary" style={{ fontSize: 13 }}>
          Locked-out built-in <Text code>admin</Text>: reset it on the server with{" "}
          <Text code>ncc-orchestrator v2-reset-password --user admin</Text>.
        </Paragraph>
        <Form form={forgotForm} layout="vertical" requiredMark={false} onFinish={handleForgot}>
          <Form.Item name="username" label="Username" rules={[{ required: true, message: "Enter your username" }]}>
            <Input prefix={<UserOutlined />} autoComplete="username" autoFocus />
          </Form.Item>
        </Form>
      </Modal>

      <RestoreBackupDialog open={restoreOpen} onClose={() => setRestoreOpen(false)} requireSignIn />
    </AuthShell>
  );
}
