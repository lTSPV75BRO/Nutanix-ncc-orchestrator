import { useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  Modal,
  Select,
  Space,
  Switch,
  Tag,
  Typography,
} from "antd";
import { DeleteOutlined, PlusOutlined, TeamOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../../api/client";
import type {
  LDAPConfig,
} from "../../../api/types";
import { notify, notifyError } from "../../../notify";
import { IconTitle } from "../../../ui";
import { roleColor } from "../../../auth/roleColor";
import { ldapFormValues, ldapProfilePayload, NEW_LDAP_PROFILE, textList, type LDAPProfileFormValues } from "../ldapProfiles";
import { ROLE_OPTIONS } from "./shared";

const PROFILE_NAME_RULE = {
  pattern: /^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$/,
  message: "Letters, digits, '.', '_' or '-' (up to 63 characters)",
};

// LDAPProfilesList edits the named directory profiles (Form.List "profiles")
// inside the LDAP form.
function LDAPProfilesList() {
  const form = Form.useFormInstance();
  const profiles = Form.useWatch("profiles", { form, preserve: true }) as LDAPProfileFormValues[] | undefined;
  return (
    <>
      <Typography.Title level={2} className="section-subheading">Other domains or forests</Typography.Title>
      <Typography.Paragraph type="secondary">
        Add a directory for each further domain or forest that has its own domain controllers.
        Signing in as <code>jdoe@&lt;suffix&gt;</code> or <code>&lt;NETBIOS&gt;\jdoe</code> asks only the
        directory that claims that domain. A plain <code>jdoe</code> is looked up in order: the
        directory above first, then these as listed, until one has the user; that directory alone
        checks the password. Users of these directories are stored under their UPN
        (<code>jdoe@emea.corp.example.com</code>), so the same short name in two forests stays two
        different people.
      </Typography.Paragraph>
      <Form.List name="profiles">
        {(fields, { add, remove }) => (
          <>
            {fields.map((field) => {
              const pv = profiles?.[field.name];
              return (
                <Card
                  key={field.key}
                  size="small"
                  style={{ marginBottom: 12 }}
                  title={pv?.name?.trim() || "New directory"}
                  extra={
                    <Button size="small" danger icon={<DeleteOutlined />} onClick={() => remove(field.name)}>
                      Remove
                    </Button>
                  }
                >
                  <Form.Item name={[field.name, "previous_name"]} hidden>
                    <Input />
                  </Form.Item>
                  <Space size={24} wrap align="start">
                    <Form.Item
                      name={[field.name, "name"]}
                      label="Name"
                      rules={[{ required: true, message: "Name this directory" }, PROFILE_NAME_RULE]}
                    >
                      <Input placeholder="emea" />
                    </Form.Item>
                    <Form.Item
                      name={[field.name, "upn_suffixes"]}
                      label="UPN suffixes"
                      extra="Every suffix its users' UPNs use."
                      rules={[{ required: true, type: "array", min: 1, message: "Add at least one UPN suffix" }]}
                    >
                      <Select mode="tags" open={false} tokenSeparators={[",", " "]} placeholder="emea.corp.example.com" style={{ minWidth: 280 }} />
                    </Form.Item>
                    <Form.Item name={[field.name, "netbios_names"]} label="NetBIOS domains (optional)">
                      <Select mode="tags" open={false} tokenSeparators={[",", " "]} placeholder="EMEA" style={{ minWidth: 160 }} />
                    </Form.Item>
                  </Space>
                  <Form.Item
                    name={[field.name, "url"]}
                    label="Server URL(s)"
                    extra="Domain controllers of this directory; comma-separate for failover."
                    rules={[{ required: true, message: "Enter at least one server URL" }]}
                  >
                    <Input placeholder="ldaps://dc1.emea.corp.example.com:636" />
                  </Form.Item>
                  <Space size={24} wrap>
                    <Form.Item name={[field.name, "start_tls"]} label="StartTLS (for ldap://)" valuePropName="checked">
                      <Switch />
                    </Form.Item>
                    <Form.Item name={[field.name, "insecure_skip_verify"]} label="Skip TLS verification" valuePropName="checked">
                      <Switch />
                    </Form.Item>
                  </Space>
                  <Form.Item
                    name={[field.name, "ca_cert_pem"]}
                    label="CA certificate (PEM)"
                    extra={pv?.has_ca_cert ? "A CA cert is stored; leave blank to keep it." : "Optional."}
                  >
                    <Input.TextArea rows={2} placeholder="-----BEGIN CERTIFICATE-----" />
                  </Form.Item>
                  <Space size={24} wrap align="start">
                    <Form.Item name={[field.name, "bind_dn"]} label="Service account DN">
                      <Input placeholder="CN=ncc-svc,OU=Service Accounts,DC=emea,DC=corp,DC=example,DC=com" style={{ minWidth: 320 }} />
                    </Form.Item>
                    <Form.Item
                      name={[field.name, "bind_password"]}
                      label="Service account password"
                      extra={pv?.has_bind_password ? "A password is stored; leave blank to keep it." : undefined}
                    >
                      <Input.Password placeholder="••••••••" autoComplete="new-password" />
                    </Form.Item>
                  </Space>
                  <Form.Item
                    name={[field.name, "base_dn"]}
                    label="Base DN"
                    rules={[{ required: true, message: "Enter the user search base DN" }]}
                  >
                    <Input placeholder="DC=emea,DC=corp,DC=example,DC=com" />
                  </Form.Item>
                  <Space size={24} wrap>
                    <Form.Item name={[field.name, "user_filter"]} label="User filter (optional)">
                      <Input placeholder="(&(objectClass=user)(|(sAMAccountName=%s)(userPrincipalName=%s)))" style={{ minWidth: 320 }} />
                    </Form.Item>
                    <Form.Item name={[field.name, "group_attribute"]} label="Group attribute (optional)">
                      <Input placeholder="memberOf" />
                    </Form.Item>
                  </Space>
                  <Form.Item
                    name={[field.name, "role_map"]}
                    label="Group → role mapping"
                    extra="This directory's own groups, one per line: group DN or CN = role."
                  >
                    <Input.TextArea rows={2} placeholder="CN=NCC-Admins,OU=Groups,DC=emea,DC=corp,DC=example,DC=com=admin" />
                  </Form.Item>
                  <Form.Item name={[field.name, "default_role"]} label="Default role (no mapping match)">
                    <Select options={ROLE_OPTIONS} />
                  </Form.Item>
                </Card>
              );
            })}
            <Form.Item>
              <Button icon={<PlusOutlined />} onClick={() => add({ ...NEW_LDAP_PROFILE })}>
                Add directory
              </Button>
            </Form.Item>
          </>
        )}
      </Form.List>
    </>
  );
}

export function LDAPCard({ embedded }: { embedded?: boolean }) {
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
    form.setFieldsValue(ldapFormValues(cfg));
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
      upn_suffixes: textList(v.upn_suffixes),
      netbios_names: textList(v.netbios_names),
      profiles: (Array.isArray(v.profiles) ? (v.profiles as LDAPProfileFormValues[]) : []).map(ldapProfilePayload),
    };
  };

  const saveMut = useMutation({
    mutationFn: (v: Record<string, unknown>) => api.updateLDAP(toPayload(v)),
    onSuccess: () => {
      notify.success("LDAP/AD configuration saved.");
      void qc.invalidateQueries({ queryKey: ["settings", "ldap"] });
      void qc.invalidateQueries({ queryKey: ["me"] });
    },
    onError: (e) => notifyError(e, "Could not save LDAP config"),
  });

  const handleTest = async () => {
    const tv = await testForm.validateFields();
    const v = form.getFieldsValue(true) as Record<string, unknown>;
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
            Signed in as <b>{res.username}</b>
            {res.directory ? <> through directory <b>{res.directory}</b></> : null} with role{" "}
            <Tag color={roleColor(res.role)}>{res.role}</Tag>
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
          title="LDAP is configured via startup flags and is read-only here."
        />
      ) : (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          title="Users sign in on the normal login form with their AD credentials. Local accounts are tried first, then AD — so the built-in admin keeps working even if AD is unreachable."
          description="With several domains or forests, the directory below is the default one; add the others under Other domains or forests"
        />
      )}
      <Form
        form={form}
        layout="vertical"
        disabled={managedByFlags}
        initialValues={{ ...ldapFormValues(cfg), bind_password: "", ca_cert_pem: "" }}
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
        <Space size={24} wrap align="start">
          <Form.Item
            name="upn_suffixes"
            label="UPN suffixes (optional)"
            extra="Domains this directory owns, e.g. corp.example.com. Only needed alongside other directories."
          >
            <Select mode="tags" open={false} tokenSeparators={[",", " "]} placeholder="corp.example.com" style={{ minWidth: 280 }} />
          </Form.Item>
          <Form.Item
            name="netbios_names"
            label="NetBIOS domains (optional)"
            extra={"Accept CORP\\jdoe as well as jdoe."}
          >
            <Select mode="tags" open={false} tokenSeparators={[",", " "]} placeholder="CORP" style={{ minWidth: 160 }} />
          </Form.Item>
        </Space>
        {!managedByFlags ? <LDAPProfilesList /> : null}
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
        destroyOnHidden
      >
        <Typography.Paragraph type="secondary">
          Uses the configuration entered above (not yet saved) to authenticate a real AD user, so you
          can verify connectivity and the resolved role before saving.
        </Typography.Paragraph>
        <Form form={testForm} layout="vertical">
          <Form.Item name="test_username" label="AD username" rules={[{ required: true }]}>
            <Input placeholder="jdoe, jdoe@emea.corp.example.com or EMEA\jdoe" autoComplete="off" />
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
      title={<IconTitle icon={<TeamOutlined />}>LDAP / Active Directory</IconTitle>}
      extra={<Tag color={cfg?.enabled ? "green" : "default"}>{cfg?.enabled ? "enabled" : "disabled"}</Tag>}
      loading={ldapQuery.isLoading}
    >
      {body}
    </Card>
  );
}
