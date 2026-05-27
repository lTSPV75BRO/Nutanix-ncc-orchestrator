import { useMemo, useState } from "react";
import {
  Alert,
  Button,
  Modal,
  Radio,
  Space,
  Steps,
  Tag,
  Typography,
} from "antd";
import { CheckCircleOutlined, KeyOutlined } from "@ant-design/icons";
import type { Document } from "yaml";
import { notify } from "../../notify";

type Mode = "env" | "file";

type Props = {
  open: boolean;
  /** Current plaintext password value (used to render the example commands). */
  passwordValue: string;
  /** Whether the doc currently has secrets-provider already set. */
  hasProvider: boolean;
  /** YAML doc accessor — used to mutate keys when the user confirms. */
  getDoc: () => Document;
  /** Called after the modal mutates the YAML doc so the parent can
   *  re-emit content + flag dirty state. */
  onApply: () => void;
  onClose: () => void;
};

const SECRET_NAME = "NCC_PASSWORD";
const FILE_KEY = "nutanix_password";

function shellSnippetForOs(secretName: string, value: string) {
  const safeValue = value.replace(/"/g, '\\"');
  return {
    bash: `# Linux/macOS (bash/zsh)
export ${secretName}="${safeValue}"

# Persist for future shells:
echo 'export ${secretName}="${safeValue}"' >> ~/.zshrc`,
    powershell: `# Windows PowerShell
$env:${secretName} = "${safeValue}"

# Persist for the user across sessions:
[Environment]::SetEnvironmentVariable("${secretName}", "${safeValue}", "User")`,
    systemd: `# systemd unit (drop-in)
[Service]
Environment="${secretName}=${safeValue}"`,
  };
}

function fileSnippet(value: string) {
  const safe = value.replace(/"/g, '\\"');
  return `# secrets.yaml (mode 0600, owner-only)
${FILE_KEY}: "${safe}"`;
}

export function SecretsMigrationModal({
  open,
  passwordValue,
  hasProvider,
  getDoc,
  onApply,
  onClose,
}: Props) {
  const [mode, setMode] = useState<Mode>("env");
  const [step, setStep] = useState(0);

  const snippets = useMemo(
    () => shellSnippetForOs(SECRET_NAME, passwordValue || "<your-password>"),
    [passwordValue],
  );
  const yamlSnippet = useMemo(
    () => fileSnippet(passwordValue || "<your-password>"),
    [passwordValue],
  );

  const handleConfirm = () => {
    const doc = getDoc();
    if (mode === "env") {
      doc.set("password", `secret://${SECRET_NAME}`);
      doc.set("secrets-provider", "env");
      // Make sure secrets-file isn't lying around half-set.
      if (doc.has("secrets-file") && String(doc.get("secrets-file") ?? "").trim() === "") {
        doc.delete("secrets-file");
      }
    } else {
      doc.set("password", `secret://${FILE_KEY}`);
      doc.set("secrets-provider", "file");
      const existing = String(doc.get("secrets-file") ?? "").trim();
      if (!existing) doc.set("secrets-file", "secrets.yaml");
    }
    onApply();
    notify.success({
      message: "Config rewritten to use secret references",
      description:
        mode === "env"
          ? `password is now secret://${SECRET_NAME}. Set the env var (snippet copied below) and click Save Config.`
          : `password is now secret://${FILE_KEY}. Create the secrets file (snippet copied below) with mode 0600, then click Save Config.`,
      duration: 8,
    });
    setStep(2);
  };

  const handleCopy = (text: string, label: string) => {
    void navigator.clipboard.writeText(text).then(() => notify.success(`${label} copied to clipboard.`));
  };

  return (
    <Modal
      title={
        <Space>
          <KeyOutlined />
          <span>Migrate plaintext password to a secret reference</span>
        </Space>
      }
      open={open}
      onCancel={() => {
        setStep(0);
        onClose();
      }}
      width={720}
      destroyOnClose
      footer={null}
    >
      <Steps
        current={step}
        size="small"
        style={{ marginBottom: 16 }}
        items={[
          { title: "Pick a provider" },
          { title: "Review snippet" },
          { title: "Apply" },
        ]}
      />

      {step === 0 ? (
        <Space direction="vertical" size={12} style={{ width: "100%" }}>
          <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
            Choose where the orchestrator should read the password from. Both options replace the
            plaintext value with a <Typography.Text code>secret://</Typography.Text> reference, and
            keep your config file safe to commit / share.
          </Typography.Paragraph>
          <Radio.Group
            id="secrets-migration-mode"
            name="secrets-mode"
            aria-label="Secrets storage mode"
            value={mode}
            onChange={(e) => setMode(e.target.value)}
            style={{ width: "100%" }}
          >
            <Space direction="vertical" size={10} style={{ width: "100%" }}>
              <Radio value="env" style={{ alignItems: "flex-start" }}>
                <div>
                  <Typography.Text strong>Environment variable</Typography.Text>{" "}
                  <Tag color="blue">recommended</Tag>
                  <div>
                    <Typography.Text type="secondary" style={{ fontSize: 13 }}>
                      Set <Typography.Text code>NCC_PASSWORD</Typography.Text> in your shell, systemd
                      unit, or container runtime. Nothing is written to disk.
                    </Typography.Text>
                  </div>
                </div>
              </Radio>
              <Radio value="file" style={{ alignItems: "flex-start" }}>
                <div>
                  <Typography.Text strong>Secrets file</Typography.Text>
                  <div>
                    <Typography.Text type="secondary" style={{ fontSize: 13 }}>
                      Store credentials in a separate <Typography.Text code>secrets.yaml</Typography.Text>{" "}
                      with mode <Typography.Text code>0600</Typography.Text>. Useful when env vars are
                      not available (e.g. ad-hoc runs).
                    </Typography.Text>
                  </div>
                </div>
              </Radio>
            </Space>
          </Radio.Group>
          {hasProvider ? (
            <Alert
              type="info"
              showIcon
              message="A secrets-provider is already configured"
              description="The wizard will overwrite the existing provider/file settings and the password key."
            />
          ) : null}
          <div style={{ display: "flex", justifyContent: "flex-end", gap: 8 }}>
            <Button onClick={onClose}>Cancel</Button>
            <Button type="primary" onClick={() => setStep(1)}>
              Next
            </Button>
          </div>
        </Space>
      ) : null}

      {step === 1 ? (
        <Space direction="vertical" size={12} style={{ width: "100%" }}>
          {mode === "env" ? (
            <>
              <Typography.Paragraph style={{ marginBottom: 0 }}>
                Set <Typography.Text code>{SECRET_NAME}</Typography.Text> in the environment that runs
                the orchestrator (UI, CLI, scheduler). Pick the snippet that matches your platform:
              </Typography.Paragraph>
              {(["bash", "powershell", "systemd"] as const).map((k) => (
                <div key={k} style={{ width: "100%" }}>
                  <Typography.Text type="secondary" style={{ fontSize: 12, textTransform: "uppercase" }}>
                    {k}
                  </Typography.Text>
                  <Typography.Paragraph
                    copyable={{ text: snippets[k], onCopy: () => handleCopy(snippets[k], `${k} snippet`) }}
                    style={{
                      background: "var(--surface-muted, rgba(0,0,0,0.04))",
                      padding: 10,
                      borderRadius: 6,
                      fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
                      whiteSpace: "pre",
                      marginBottom: 0,
                    }}
                  >
                    {snippets[k]}
                  </Typography.Paragraph>
                </div>
              ))}
            </>
          ) : (
            <>
              <Typography.Paragraph style={{ marginBottom: 0 }}>
                Create a YAML file with mode <Typography.Text code>0600</Typography.Text> next to your
                config (default: <Typography.Text code>secrets.yaml</Typography.Text>):
              </Typography.Paragraph>
              <Typography.Paragraph
                copyable={{ text: yamlSnippet, onCopy: () => handleCopy(yamlSnippet, "secrets.yaml snippet") }}
                style={{
                  background: "var(--surface-muted, rgba(0,0,0,0.04))",
                  padding: 10,
                  borderRadius: 6,
                  fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
                  whiteSpace: "pre",
                  marginBottom: 0,
                }}
              >
                {yamlSnippet}
              </Typography.Paragraph>
              <Alert
                type="warning"
                showIcon
                message="Restrict permissions"
                description={
                  <span style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" }}>
                    chmod 600 secrets.yaml
                  </span>
                }
              />
            </>
          )}
          <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
            <Button onClick={() => setStep(0)}>Back</Button>
            <Space>
              <Button onClick={onClose}>Cancel</Button>
              <Button type="primary" onClick={handleConfirm}>
                Apply to config
              </Button>
            </Space>
          </div>
        </Space>
      ) : null}

      {step === 2 ? (
        <Space direction="vertical" size={12} style={{ width: "100%" }}>
          <Alert
            type="success"
            showIcon
            icon={<CheckCircleOutlined />}
            message="Password replaced with a secret reference"
            description={
              <>
                Don't forget to click <Typography.Text strong>Save Config</Typography.Text> on the
                Config tab to persist the change.
              </>
            }
          />
          <div style={{ display: "flex", justifyContent: "flex-end" }}>
            <Button
              type="primary"
              onClick={() => {
                setStep(0);
                onClose();
              }}
            >
              Done
            </Button>
          </div>
        </Space>
      ) : null}
    </Modal>
  );
}
