import { useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  InputNumber,
  Select,
  Space,
  Switch,
  Tag,
  Typography,
} from "antd";
import { ClockCircleOutlined, GlobalOutlined, LoginOutlined, SafetyOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../../api/client";
import type {
  LDAPConfig,
  OIDCConfig,
  OIDCTestResult,
  SessionPolicy,
  SSOConfig,
} from "../../../api/types";
import { notify, notifyError } from "../../../notify";
import { IconTitle } from "../../../ui";
import { ROLE_OPTIONS } from "./shared";
import { LDAPCard } from "./LdapCard";

export function SSOCard({ embedded }: { embedded?: boolean }) {
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
    onError: (e) => notifyError(e, "Could not save SSO config"),
  });

  const body = (
    <>
      {managedByFlags ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          title="SAML is configured via startup flags and is read-only here."
        />
      ) : (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          title="The SP signing key is generated on the server. Publish the SP metadata URL below to your IdP — no private key leaves the server."
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
      title={<IconTitle icon={<SafetyOutlined />}>Single sign-on (SAML)</IconTitle>}
      extra={
        <Tag color={cfg?.enabled ? "green" : "default"}>{cfg?.enabled ? "enabled" : "disabled"}</Tag>
      }
    >
      {body}
    </Card>
  );
}

export function SessionCard() {
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
    onError: (e) => notifyError(e, "Could not update session duration"),
  });

  const resetMut = useMutation({
    mutationFn: () => api.updateSessionPolicy({ ttl_sec: 0 }),
    onSuccess: () => {
      notify.success("Reverted to the server default session duration.");
      void qc.invalidateQueries({ queryKey: ["settings", "session"] });
      void qc.invalidateQueries({ queryKey: ["me"] });
    },
    onError: (e) => notifyError(e, "Could not reset session duration"),
  });

  return (
    <Card
      className="page-card"
      title={<IconTitle icon={<ClockCircleOutlined />}>Session lifetime</IconTitle>}
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
            label="Active for"
            htmlFor="ttl_min"
            required
            extra={`Allowed range: ${minMinutes}–${maxMinutes} minutes (max 24h).`}
          >
            <Space.Compact style={{ width: 200 }}>
              <Form.Item
                name="ttl_min"
                noStyle
                rules={[
                  { required: true, message: "Enter a duration" },
                  {
                    type: "number",
                    min: minMinutes,
                    max: maxMinutes,
                    message: `Between ${minMinutes} and ${maxMinutes} minutes`,
                  },
                ]}
              >
                <InputNumber min={minMinutes} max={maxMinutes} step={5} style={{ width: "100%" }} />
              </Form.Item>
              <Space.Addon>minutes</Space.Addon>
            </Space.Compact>
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

const OIDC_DEFAULT_ROLE_OPTIONS = [
  ...ROLE_OPTIONS,
  { value: "none", label: "none — refuse users who match no mapping" },
];

/**
 * OpenID Connect sign-in (Entra ID, Okta, Google, Keycloak, …). The client
 * secret is write-only: the server reports has_client_secret and keeps the
 * stored value when the field is left blank.
 */
export function OIDCCard({ embedded = false }: { embedded?: boolean }) {
  const qc = useQueryClient();
  const oidcQuery = useQuery({ queryKey: ["settings", "oidc"], queryFn: api.getOIDC });
  const cfg = oidcQuery.data as OIDCConfig | undefined;
  const [form] = Form.useForm();
  const [testResult, setTestResult] = useState<OIDCTestResult | null>(null);
  const rootURL = Form.useWatch("root_url", form) as string | undefined;
  const redirectURL = rootURL
    ? `${rootURL.trim().replace(/\/+$/, "")}/api/v1/auth/oidc/callback`
    : cfg?.redirect_url ?? "";

  useEffect(() => {
    if (!cfg) return;
    form.setFieldsValue({
      enabled: cfg.enabled ?? false,
      issuer_url: cfg.issuer_url ?? "",
      client_id: cfg.client_id ?? "",
      root_url: cfg.root_url || window.location.origin,
      scopes: cfg.scopes ?? "",
      username_claim: cfg.username_claim ?? "",
      groups_claim: cfg.groups_claim || "groups",
      role_map: cfg.role_map ?? "",
      default_role: cfg.default_role || "viewer",
      button_label: cfg.button_label ?? "",
    });
  }, [cfg, form]);

  const saveMut = useMutation({
    mutationFn: (v: Record<string, unknown>) =>
      api.updateOIDC({
        enabled: Boolean(v.enabled),
        issuer_url: String(v.issuer_url ?? ""),
        client_id: String(v.client_id ?? ""),
        client_secret: String(v.client_secret ?? ""),
        root_url: String(v.root_url ?? ""),
        scopes: String(v.scopes ?? ""),
        username_claim: String(v.username_claim ?? ""),
        groups_claim: String(v.groups_claim ?? ""),
        role_map: String(v.role_map ?? ""),
        default_role: String(v.default_role ?? "viewer"),
        button_label: String(v.button_label ?? ""),
      }),
    onSuccess: () => {
      notify.success("OpenID Connect configuration saved.");
      form.setFieldValue("client_secret", "");
      void qc.invalidateQueries({ queryKey: ["settings", "oidc"] });
      void qc.invalidateQueries({ queryKey: ["me"] });
      void qc.invalidateQueries({ queryKey: ["health"] });
    },
    onError: (e) => notifyError(e, "Couldn't save the OpenID Connect settings"),
  });

  const testMut = useMutation({
    mutationFn: () => api.testOIDC(String(form.getFieldValue("issuer_url") ?? "")),
    onSuccess: (res) => setTestResult(res),
    onError: (e) => {
      setTestResult(null);
      notifyError(e, "Discovery failed");
    },
  });

  const body = (
    <>
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16 }}
        title="Register NCC Orchestrator as a web application with your identity provider"
        description={
          <>
            Use the authorization-code flow; PKCE is always used. Add this redirect URI:{" "}
            {redirectURL ? (
              <Typography.Text copyable code>
                {redirectURL}
              </Typography.Text>
            ) : (
              "set the dashboard URL below first"
            )}
            . For role mapping, have the provider include a groups claim in the ID token.
          </>
        }
      />
      <Form
        form={form}
        layout="vertical"
        initialValues={{ enabled: false, groups_claim: "groups", default_role: "viewer", root_url: window.location.origin }}
        onFinish={(v) => saveMut.mutate(v)}
      >
        <Form.Item name="enabled" label="Enable OpenID Connect sign-in" valuePropName="checked">
          <Switch />
        </Form.Item>
        <Form.Item
          label="Issuer URL"
          htmlFor="issuer_url"
          required
          extra="Entra ID: https://login.microsoftonline.com/<tenant-id>/v2.0 · Okta: https://<org>.okta.com · Google: https://accounts.google.com · Keycloak: https://<host>/realms/<realm>"
        >
          <Space.Compact style={{ width: "100%" }}>
            <Form.Item name="issuer_url" noStyle rules={[{ required: true, message: "Enter the issuer URL" }]}>
              <Input placeholder="https://login.microsoftonline.com/<tenant-id>/v2.0" />
            </Form.Item>
            <Button loading={testMut.isPending} onClick={() => testMut.mutate()}>
              Test discovery
            </Button>
          </Space.Compact>
        </Form.Item>
        {testResult ? (
          <Alert
            type={testResult.pkce_s256 ? "success" : "warning"}
            showIcon
            style={{ marginBottom: 16 }}
            title={testResult.pkce_s256 ? "Discovery succeeded" : "Discovery succeeded, but the provider does not advertise PKCE (S256)"}
            description={
              <>
                Authorization: <Typography.Text code>{testResult.authorization_endpoint}</Typography.Text>
                <br />
                Token: <Typography.Text code>{testResult.token_endpoint}</Typography.Text>
                {testResult.claims_supported?.length ? (
                  <>
                    <br />
                    Claims: {testResult.claims_supported.slice(0, 20).join(", ")}
                  </>
                ) : null}
              </>
            }
          />
        ) : null}
        <Form.Item name="client_id" label="Client ID" rules={[{ required: true }]}>
          <Input autoComplete="off" />
        </Form.Item>
        <Form.Item
          name="client_secret"
          label="Client secret"
          extra={
            cfg?.has_client_secret
              ? "A secret is stored; leave blank to keep it."
              : "Leave blank for a public client (PKCE only), if your provider allows it."
          }
        >
          <Input.Password autoComplete="new-password" />
        </Form.Item>
        <Form.Item
          name="root_url"
          label="Dashboard URL"
          extra="The address users open NCC Orchestrator at; the redirect URI is built from it."
          rules={[{ required: true }]}
        >
          <Input placeholder="https://ncc.example.com" />
        </Form.Item>
        <Form.Item name="groups_claim" label="Groups claim">
          <Input placeholder="groups" />
        </Form.Item>
        <Form.Item name="role_map" label="Role mapping" extra="group=role, comma-separated, e.g. ncc-admins=admin,ncc-ops=operator. Entra ID sends group object IDs.">
          <Input placeholder="ncc-admins=admin,ncc-ops=operator" />
        </Form.Item>
        <Form.Item name="default_role" label="Role when no group matches">
          <Select options={OIDC_DEFAULT_ROLE_OPTIONS} />
        </Form.Item>
        <Form.Item name="button_label" label="Sign-in button name" extra='Shown as "Sign in with …" on the sign-in page.'>
          <Input placeholder="Microsoft" />
        </Form.Item>
        <Form.Item name="username_claim" label="Username claim (optional)">
          <Input placeholder="(defaults to preferred_username, then email)" />
        </Form.Item>
        <Form.Item name="scopes" label="Extra scopes (optional)" extra="openid, profile and email are always requested.">
          <Input placeholder="groups" />
        </Form.Item>
        <Form.Item>
          <Button type="primary" htmlType="submit" loading={saveMut.isPending}>
            Save OpenID Connect configuration
          </Button>
        </Form.Item>
      </Form>
    </>
  );
  if (embedded) return body;
  return (
    <Card
      className="page-card"
      title={<IconTitle icon={<LoginOutlined />}>OpenID Connect</IconTitle>}
      extra={<Tag color={cfg?.enabled ? "green" : "default"}>{cfg?.enabled ? "enabled" : "disabled"}</Tag>}
      loading={oidcQuery.isLoading}
    >
      {body}
    </Card>
  );
}

export function ExternalAuthCard() {
  const ssoQuery = useQuery({ queryKey: ["settings", "sso"], queryFn: api.getSSO });
  const ldapQuery = useQuery({ queryKey: ["settings", "ldap"], queryFn: api.getLDAP });
  const oidcQuery = useQuery({ queryKey: ["settings", "oidc"], queryFn: api.getOIDC });
  const samlOn = Boolean((ssoQuery.data as SSOConfig | undefined)?.enabled);
  const ldapOn = Boolean((ldapQuery.data as LDAPConfig | undefined)?.enabled);
  const oidcOn = Boolean((oidcQuery.data as OIDCConfig | undefined)?.enabled);
  const [provider, setProvider] = useState<"oidc" | "saml" | "ldap">("oidc");

  return (
    <Card className="page-card" id="settings-ldap" title={<IconTitle icon={<GlobalOutlined />}>External authentication</IconTitle>}>
      <Space orientation="vertical" size={16} style={{ width: "100%" }}>
        <div>
          <Space align="center" wrap>
            <Typography.Text strong>Provider</Typography.Text>
            <Select
              aria-label="Sign-in provider"
              value={provider}
              onChange={(v) => setProvider(v as "oidc" | "saml" | "ldap")}
              style={{ minWidth: 320 }}
              options={[
                { value: "oidc", label: "OpenID Connect (Entra ID, Okta, Google, Keycloak)" },
                { value: "saml", label: "SAML single sign-on" },
                { value: "ldap", label: "LDAP / Active Directory" },
              ]}
            />
            <Tag color={oidcOn ? "green" : "default"}>OIDC {oidcOn ? "on" : "off"}</Tag>
            <Tag color={samlOn ? "green" : "default"}>SAML {samlOn ? "on" : "off"}</Tag>
            <Tag color={ldapOn ? "green" : "default"}>LDAP {ldapOn ? "on" : "off"}</Tag>
          </Space>
          <Typography.Paragraph type="secondary" style={{ marginTop: 8, marginBottom: 0 }}>
            Configure each provider independently — any of them can be enabled at the same time. The dropdown
            only changes which one you're editing; saving one does not disable the other. At login, local
            accounts are checked first, then any enabled directory provider.
          </Typography.Paragraph>
        </div>
        <div style={{ display: provider === "oidc" ? "block" : "none" }}>
          <OIDCCard embedded />
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
