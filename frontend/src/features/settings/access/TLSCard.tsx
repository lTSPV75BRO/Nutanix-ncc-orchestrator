import { useState } from "react";
import {
  Alert,
  Button,
  Card,
  Divider,
  Form,
  Input,
  Modal,
  Popconfirm,
  Space,
  Tag,
  Typography,
} from "antd";
import { DeleteOutlined, DownloadOutlined, LockOutlined, SafetyCertificateOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../../api/client";
import type {
  TLSApplyResult,
  TLSPolicy,
} from "../../../api/types";
import { notify, notifyError } from "../../../notify";
import { formatDateTime } from "../../../utils/datetime";
import { IconTitle } from "../../../ui";

// TLSCard manages HTTPS for the browser-facing UI server: upload a PEM
// certificate + key to enable HTTPS (the stack restarts to bind TLS and session
// cookies become Secure), or remove it to fall back to plain HTTP. The private
// key is write-only — it is stored 0600 on the server and never returned.
export function TLSCard({ isKubernetes = false }: { isKubernetes?: boolean }) {
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
  const ingressManaged = cfg?.managed_by === "ingress";

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
      window.setTimeout(() => {
        window.location.href = target;
      }, 8000);
    } else if (!result.restart_required) {
      Modal.success({
        title: nextScheme === "http" ? "Reverted to self-signed HTTPS" : "Certificate updated",
        content:
          nextScheme === "https"
            ? "UI servers will use the new certificate within a few seconds. If it is self-signed, accept the browser warning. This page will reload."
            : "HTTPS stays enabled with the built-in self-signed certificate. This page will reload.",
        okText: "Reload now",
        onOk: () => {
          window.location.reload();
        },
      });
      window.setTimeout(() => {
        window.location.reload();
      }, 2500);
    } else {
      Modal.warning({
        title: result.tls.https_enabled ? "Certificate installed" : "HTTPS disabled",
        content:
          "An automatic restart was unavailable. Restart the services (v2-restart) for the change to take effect.",
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
    onError: (e) => notifyError(e, "Could not enable HTTPS"),
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
    onError: (e) => notifyError(e, "Could not generate self-signed certificate"),
  });

  const disableMut = useMutation({
    mutationFn: () => api.disableTLS(),
    onSuccess: (res) => {
      void qc.invalidateQueries({ queryKey: ["settings", "tls"] });
      announceSchemeSwitch(res, "http");
    },
    onError: (e) => notifyError(e, "Could not disable HTTPS"),
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
      id="settings-tls"
      title={<IconTitle icon={<SafetyCertificateOutlined />}>HTTPS / TLS</IconTitle>}
      extra={
        <Tag color={servingHTTPS || ingressManaged ? "green" : "default"}>
          {servingHTTPS || ingressManaged ? "HTTPS enabled" : "HTTP only"}
        </Tag>
      }
      loading={tlsQuery.isLoading}
    >
      {ingressManaged ? (
        <Space orientation="vertical" size={12} style={{ width: "100%" }}>
          <Alert
            type="success"
            showIcon
            icon={<LockOutlined />}
            title="HTTPS is terminated at the Kubernetes Ingress"
            description={
              cfg?.message ||
              "The UI is served over TLS by the cluster Ingress. Manage certificates on the Ingress TLS secret or with cert-manager. Session cookies are marked Secure."
            }
          />
          <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
            TLS secret: <Typography.Text code>{cfg?.secret_name || "ncc-v2-ui-tls"}</Typography.Text>.
            In-app certificate upload and self-signed generation are disabled on Kubernetes because the API
            pods are not the TLS terminator.
          </Typography.Paragraph>
        </Space>
      ) : (
        <>
      <Typography.Paragraph type="secondary">
        The UI is served over HTTPS by default with a self-signed certificate, and any plain-HTTP
        request is redirected to HTTPS. Generate or renew a self-signed certificate with one click,
        or upload your own PEM certificate and private key below.
        {isKubernetes
          ? " On Kubernetes the certificate lives on the shared volume; UI pods reload it without a stack restart."
          : " Applying a certificate restarts the services to bind the browser-facing server to TLS and marks session cookies as Secure."}{" "}
        The private key is stored with <code>0600</code> permissions on the server and is never shown
        again.
      </Typography.Paragraph>

      <div style={{ marginBottom: 16 }}>
        <Space wrap>
          <Popconfirm
            title={enabled ? "Renew the self-signed certificate?" : "Generate a self-signed certificate?"
            }
            description={
              isKubernetes
                ? "UI pods reload the new certificate from the shared volume within a few seconds. Browsers will show a one-time self-signed warning — accept it to continue."
                : "The stack restarts to apply the new certificate. Browsers will show a one-time self-signed warning — accept it to continue."
            }
            okText={enabled ? (isKubernetes ? "Renew" : "Renew & restart") : isKubernetes ? "Generate" : "Generate & restart"}
            onConfirm={() => generateMut.mutate()}
          >
            <Button type="primary" icon={<SafetyCertificateOutlined />} loading={generateMut.isPending}>
              {enabled ? "Renew self-signed certificate" : "Generate self-signed certificate"}
            </Button>
          </Popconfirm>
          {enabled ? (
            <Popconfirm
              title={isKubernetes ? "Revert to the self-signed certificate?" : "Disable HTTPS?"}
              description={
                isKubernetes
                  ? "The uploaded certificate is removed. HTTPS stays on with the built-in self-signed certificate."
                  : "The stack restarts and falls back to plain HTTP. The stored certificate and key are removed. Note: if the services are configured for HTTPS-by-default it will regenerate a self-signed certificate on the next start."
              }
              okText={isKubernetes ? "Revert" : "Disable HTTPS"}
              okButtonProps={{ danger: true }}
              onConfirm={() => disableMut.mutate()}
            >
              <Button danger icon={<DeleteOutlined />} loading={disableMut.isPending}>
                {isKubernetes ? "Revert to self-signed" : "Disable HTTPS"}
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
          title="HTTPS is enabled"
          description={
            <Space orientation="vertical" size={2} style={{ width: "100%" }}>
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
              {cfg?.fingerprint_sha256 ? (
                <span>
                  <Typography.Text type="secondary">SHA-256: </Typography.Text>
                  <Typography.Text code copyable={{ text: cfg.fingerprint_sha256 }}>
                    {cfg.fingerprint_sha256}
                  </Typography.Text>
                </span>
              ) : null}
              {cfg?.self_signed ? (
                <Button
                  size="small"
                  icon={<DownloadOutlined />}
                  onClick={async () => {
                    try {
                      const pub = await api.tlsPublic();
                      if (!pub.cert_pem) {
                        notify.warning("Certificate PEM is not available to download yet.");
                        return;
                      }
                      const blob = new Blob([pub.cert_pem], { type: "application/x-x509-ca-cert" });
                      const url = URL.createObjectURL(blob);
                      const a = document.createElement("a");
                      a.href = url;
                      a.download = "ncc-ui.crt";
                      a.click();
                      URL.revokeObjectURL(url);
                    } catch (e) {
                      notifyError(e, "Couldn't download the UI certificate");
                    }
                  }}
                >
                  Download certificate
                </Button>
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
            {enabled ? "Replace certificate" : isKubernetes ? "Install certificate" : "Enable HTTPS & restart"}
          </Button>
        </Form.Item>
      </Form>
        </>
      )}
    </Card>
  );
}
