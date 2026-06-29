import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Alert,
  Anchor,
  Badge,
  Button,
  Card,
  Col,
  Empty,
  Form,
  Input,
  InputNumber,
  List,
  Radio,
  Row,
  Select,
  Space,
  Switch,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  ApartmentOutlined,
  BellOutlined,
  CloudServerOutlined,
  EyeInvisibleOutlined,
  EyeOutlined,
  FileTextOutlined,
  KeyOutlined,
  ReloadOutlined,
  RocketOutlined,
  SafetyCertificateOutlined,
  SaveOutlined,
  SettingOutlined,
  ThunderboltOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import type { ReactNode } from "react";
import { Document, parseDocument } from "yaml";
import { api } from "../../api/client";
import { CodeEditor, inferEditorLanguage } from "../../components/CodeEditor";
import type { ConfigRelatedFileBatchOperation, ConfigRelatedFileInfo } from "../../api/types";
import { PolicyGateBuilderSection } from "./PolicyGateBuilderSection";
import { SecretsMigrationModal } from "./SecretsMigrationModal";
import { notify } from "../../notify";
import { SECTIONS, KNOWN_KEYS, type FieldDef, type SectionDef } from "./configSchema";

type Props = {
  onError: (e: unknown) => void;
};

type Mode = "form" | "yaml";

const SECTION_ICONS: Record<SectionDef["icon"], ReactNode> = {
  cluster: <CloudServerOutlined />,
  perf: <ThunderboltOutlined />,
  outputs: <ApartmentOutlined />,
  policy: <SafetyCertificateOutlined />,
  logs: <FileTextOutlined />,
  notify: <BellOutlined />,
  secrets: <KeyOutlined />,
};

function applyPolicyGatesToConfigContent(content: string, gatesCsv: string): string {
  const line = `policy-gates: "${gatesCsv}"`;
  const pattern = /^\s*policy-gates:\s*.*$/m;
  if (pattern.test(content)) {
    return content.replace(pattern, line);
  }
  const trimmed = content.trimEnd();
  if (!trimmed) return `${line}\n`;
  return `${trimmed}\n${line}\n`;
}

/** Tolerantly read a key from a YAML document as a string for form display. */
function readString(doc: Document, key: string): string {
  const v = doc.get(key);
  if (v === null || v === undefined) return "";
  if (typeof v === "boolean") return v ? "true" : "false";
  return String(v);
}

function readBoolean(doc: Document, key: string): boolean {
  const v = doc.get(key);
  if (typeof v === "boolean") return v;
  if (typeof v === "string") return v.trim().toLowerCase() === "true";
  return Boolean(v);
}

function readNumber(doc: Document, key: string): number | null {
  const v = doc.get(key);
  if (v === null || v === undefined || v === "") return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

function writeKey(doc: Document, key: string, value: unknown): void {
  if (value === null || value === undefined) {
    doc.delete(key);
    return;
  }
  doc.set(key, value);
}

function ensureString(value: unknown): string {
  if (value === null || value === undefined) return "";
  return String(value);
}

/** Parse YAML, returning a fresh empty Document on error. */
function safeParse(yaml: string): { doc: Document; error: string | null } {
  try {
    const doc = parseDocument(yaml || "");
    if (doc.errors && doc.errors.length > 0) {
      return { doc, error: doc.errors.map((e) => e.message).join("; ") };
    }
    return { doc, error: null };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { doc: new Document({}), error: msg };
  }
}

/** Friendly secret indicator for password-typed fields. */
function SecretInput({
  id,
  name,
  value,
  onChange,
  placeholder,
  isWeakPlaintext,
  onMigrate,
}: {
  id?: string;
  name?: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  isWeakPlaintext: boolean;
  onMigrate?: () => void;
}) {
  return (
    <Space direction="vertical" size={4} style={{ width: "100%" }}>
      <Input.Password
        id={id}
        name={name}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        iconRender={(visible) => (visible ? <EyeOutlined /> : <EyeInvisibleOutlined />)}
        autoComplete="new-password"
      />
      {isWeakPlaintext ? (
        <Space size={8} align="center" wrap style={{ marginTop: 2 }}>
          <Typography.Text type="warning" style={{ fontSize: 12 }}>
            <WarningOutlined /> Stored as plaintext in the config file.
          </Typography.Text>
          {onMigrate ? (
            <Button size="small" type="link" onClick={onMigrate} style={{ padding: 0 }}>
              Migrate to a secret reference →
            </Button>
          ) : null}
        </Space>
      ) : null}
    </Space>
  );
}

/** Stable DOM id for a config field — used to wire the Form.Item label's
 *  htmlFor to the actual input id and to give the input a name= attribute
 *  for browser autofill heuristics. */
function fieldDomId(key: string) {
  return `cfg-${key}`;
}

function FieldControl({
  field,
  doc,
  onChange,
  onMigrateSecret,
}: {
  field: FieldDef;
  doc: Document;
  onChange: () => void;
  onMigrateSecret?: () => void;
}) {
  // Stable id/name on every control so:
  //   1) The wrapping <label> can be linked via htmlFor (a11y + autofill UX).
  //   2) Browser DevTools stops reporting "form field has neither id nor
  //      name attribute".
  const id = fieldDomId(field.key);
  switch (field.type) {
    case "boolean": {
      const v = readBoolean(doc, field.key);
      return (
        <Switch
          id={id}
          checked={v}
          onChange={(checked) => {
            writeKey(doc, field.key, checked);
            onChange();
          }}
        />
      );
    }
    case "int": {
      const v = readNumber(doc, field.key);
      return (
        <InputNumber
          id={id}
          name={field.key}
          value={v ?? undefined}
          min={field.min}
          max={field.max}
          step={field.step ?? 1}
          placeholder={field.placeholder}
          style={{ width: "100%" }}
          onChange={(next) => {
            writeKey(doc, field.key, next ?? null);
            onChange();
          }}
        />
      );
    }
    case "select": {
      const v = readString(doc, field.key);
      return (
        <Select
          id={id}
          value={v}
          options={field.options ?? []}
          style={{ width: "100%" }}
          onChange={(next) => {
            writeKey(doc, field.key, next === "" ? "" : next);
            onChange();
          }}
        />
      );
    }
    case "password": {
      const v = readString(doc, field.key);
      const looksLikeSecretRef = v.trim().toLowerCase().startsWith("secret://");
      const isPlaintextPassword = field.key === "password" && v.trim().length > 0 && !looksLikeSecretRef;
      return (
        <SecretInput
          id={id}
          name={field.key}
          value={v}
          placeholder={field.placeholder}
          onChange={(next) => {
            writeKey(doc, field.key, next);
            onChange();
          }}
          isWeakPlaintext={isPlaintextPassword}
          onMigrate={isPlaintextPassword ? onMigrateSecret : undefined}
        />
      );
    }
    case "textarea": {
      const v = readString(doc, field.key);
      return (
        <Input.TextArea
          id={id}
          name={field.key}
          value={v}
          placeholder={field.placeholder}
          autoSize={{ minRows: 2, maxRows: 6 }}
          autoComplete={field.autoComplete ?? "off"}
          onChange={(e) => {
            writeKey(doc, field.key, e.target.value);
            onChange();
          }}
        />
      );
    }
    case "duration":
    case "string":
    case "list-csv":
    default: {
      const v = readString(doc, field.key);
      return (
        <Input
          id={id}
          name={field.key}
          value={v}
          placeholder={field.placeholder}
          // Default to "off" so browsers don't try to autofill emails/usernames
          // into config text inputs (e.g. "PC IP", "Webhook URL"). Explicit
          // `autoComplete` on the FieldDef wins (e.g. "username" for the
          // Username field).
          autoComplete={field.autoComplete ?? "off"}
          onChange={(e) => {
            writeKey(doc, field.key, e.target.value);
            onChange();
          }}
        />
      );
    }
  }
}

function SectionForm({
  section,
  doc,
  onChange,
  fieldStatuses,
  onMigrateSecret,
}: {
  section: SectionDef;
  doc: Document;
  onChange: () => void;
  fieldStatuses: Record<string, "warn" | "info" | undefined>;
  onMigrateSecret?: () => void;
}) {
  return (
    <Card
      id={`config-section-${section.id}`}
      className="page-card config-section-card"
      styles={{ body: { paddingTop: 18 } }}
    >
      <Space size={12} align="center" style={{ marginBottom: 8 }}>
        <span className="config-section-icon">{SECTION_ICONS[section.icon]}</span>
        <div>
          <Typography.Title level={4} style={{ margin: 0 }}>
            {section.title}
          </Typography.Title>
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            {section.description}
          </Typography.Text>
        </div>
      </Space>

      {/*
        Each section renders its own <form> on purpose so:
          1) The contained Input.Password fields have a form ancestor (Chrome
             warns "Password field is not contained in a form" otherwise and
             downgrades autofill/security UX).
          2) Browsers can scope autofill correctly per section.

        `onSubmitCapture` swallows accidental Enter-key submits — there is no
        explicit Save button per section; saving is handled by the page-level
        toolbar — but the form element itself still needs to exist for the
        password-autofill heuristics above. Chrome's softer "Multiple forms
        should be contained in their own form elements" recommendation is a
        hint, not an error, and is the correct trade-off here.
      */}
      <Form
        layout="vertical"
        colon={false}
        style={{ marginTop: 16 }}
        onSubmitCapture={(e) => e.preventDefault()}
      >
        <Row gutter={[20, 4]}>
          {section.fields.map((field) => {
            const help = field.help ? (
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                {field.help}
              </Typography.Text>
            ) : null;
            const isWide =
              field.type === "textarea" ||
              field.type === "list-csv" ||
              field.key === "policy-gates" ||
              field.key === "webhook-url" ||
              field.key === "smtp-server" ||
              field.key === "log-file" ||
              field.key === "clusters-file" ||
              field.key === "exclude-alert-titles-file" ||
              field.key === "secrets-file" ||
              field.key === "run-history-dir" ||
              field.key === "output-dir-filtered" ||
              field.key === "output-dir-logs" ||
              field.key === "email-to" ||
              field.key === "prism-central-url";
            const colProps = isWide ? { xs: 24 } : { xs: 24, sm: 12 };
            const status = fieldStatuses[field.key];
            return (
              <Col {...colProps} key={field.key}>
                <Form.Item
                  // htmlFor links the rendered <label> to the FieldControl's
                  // input id (set in fieldDomId(field.key)), so a11y tools and
                  // browser autofill can associate them. Without this, Chrome
                  // emits "No label associated with a form field" for every
                  // field on the page.
                  htmlFor={fieldDomId(field.key)}
                  label={
                    <Space size={6}>
                      <span>{field.label}</span>
                      <Typography.Text type="secondary" style={{ fontSize: 11 }}>
                        <Tooltip title={`YAML key: ${field.key}`}>
                          <code className="config-field-key">{field.key}</code>
                        </Tooltip>
                      </Typography.Text>
                      {field.required ? <Tag color="warning">required</Tag> : null}
                      {status === "warn" ? <Tag color="warning">attention</Tag> : null}
                      {status === "info" ? <Tag color="processing">change</Tag> : null}
                    </Space>
                  }
                  extra={help}
                  style={{ marginBottom: 12 }}
                >
                  <FieldControl
                    field={field}
                    doc={doc}
                    onChange={onChange}
                    onMigrateSecret={onMigrateSecret}
                  />
                </Form.Item>
              </Col>
            );
          })}
        </Row>
      </Form>
    </Card>
  );
}

export function ConfigSection({ onError }: Props) {
  const [mode, setMode] = useState<Mode>("form");
  const [content, setContent] = useState("");
  const [originalContent, setOriginalContent] = useState("");
  const [configPath, setConfigPath] = useState("");
  const [files, setFiles] = useState<ConfigRelatedFileInfo[]>([]);
  const [activeFile, setActiveFile] = useState<string>("");
  const [activeFileContent, setActiveFileContent] = useState("");
  const [bulkAddPaths, setBulkAddPaths] = useState("");
  const [bulkAddContent, setBulkAddContent] = useState("");
  const [bulkRemovePaths, setBulkRemovePaths] = useState<string[]>([]);
  const [parseError, setParseError] = useState<string | null>(null);
  const [secretsModalOpen, setSecretsModalOpen] = useState(false);
  const [, forceTick] = useState(0);
  const docRef = useRef<Document>(new Document({}));

  const dirty = content !== originalContent;
  const dirtyFile = false; // related file dirty state handled separately below

  // Parse content into a Document whenever the YAML text changes externally.
  // Form-mode edits mutate docRef and call refreshFromDoc to keep content in
  // sync, so we only re-parse when content changed externally.
  const lastSyncedContentRef = useRef("");
  useEffect(() => {
    if (content === lastSyncedContentRef.current) return;
    const { doc, error } = safeParse(content);
    docRef.current = doc;
    setParseError(error);
    lastSyncedContentRef.current = content;
    forceTick((n) => n + 1);
  }, [content]);

  /**
   * Called by FieldControl when the user mutates a field. Stringify the
   * underlying Document back into YAML text and update content. We update
   * lastSyncedContentRef so the parsing effect doesn't redundantly reparse
   * what we just emitted.
   */
  const refreshFromDoc = useCallback(() => {
    const next = docRef.current.toString();
    lastSyncedContentRef.current = next;
    setContent(next);
    setParseError(null);
    forceTick((n) => n + 1);
  }, []);

  const refreshConfigFiles = useCallback(async (preserveActivePath?: string) => {
    const resp = await api.listConfigFiles();
    const next = resp.items ?? [];
    setFiles(next);
    const selectedPath = preserveActivePath || activeFile;
    const exists = next.some((it) => it.path === selectedPath);
    if (selectedPath && exists) {
      setActiveFile(selectedPath);
      return next;
    }
    if (!selectedPath && next.length > 0) {
      setActiveFile(next[0].path);
      return next;
    }
    if (!exists) {
      setActiveFile("");
      setActiveFileContent("");
    }
    return next;
  }, [activeFile]);

  const load = useCallback(async (suppressError = false, silent = false) => {
    try {
      const cfg = await api.loadConfig();
      const text = cfg.content ?? "";
      setContent(text);
      setOriginalContent(text);
      setConfigPath(cfg.path ?? "");
      await refreshConfigFiles();
      if (!silent) notify.success("Config loaded.");
      return true;
    } catch (e) {
      if (!suppressError) onError(e);
      return false;
    }
  }, [onError, refreshConfigFiles]);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      for (let attempt = 0; attempt < 3; attempt += 1) {
        if (cancelled) return;
        const ok = await load(attempt < 2, true);
        if (ok) return;
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [load]);

  // Keyboard shortcuts: Cmd/Ctrl+S to save, Cmd/Ctrl+Shift+Z to revert.
  // We use refs in the closure to avoid re-binding on every render.
  const saveRef = useRef<() => void>(() => {});
  const revertRef = useRef<() => void>(() => {});
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const meta = e.metaKey || e.ctrlKey;
      if (!meta) return;
      const key = e.key.toLowerCase();
      if (key === "s" && !e.shiftKey) {
        e.preventDefault();
        saveRef.current();
      } else if (key === "z" && e.shiftKey) {
        e.preventDefault();
        revertRef.current();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, []);

  const save = async () => {
    if (parseError) {
      notify.warning({ message: "Cannot save", description: `YAML is invalid: ${parseError}` });
      return;
    }
    if (content === originalContent) {
      notify.info("No changes to save.");
      return;
    }
    try {
      await api.saveConfig(content);
      setOriginalContent(content);
      await refreshConfigFiles(activeFile);
      notify.success("Config saved.");
    } catch (e) {
      onError(e);
    }
  };

  const revert = () => {
    if (content === originalContent) {
      notify.info("Already in sync with last loaded config.");
      return;
    }
    setContent(originalContent);
    notify.info("Reverted to last loaded config.");
  };

  // Keep keyboard-shortcut refs current.
  saveRef.current = () => void save();
  revertRef.current = revert;

  const loadRelatedFile = async (path: string) => {
    try {
      const resp = await api.loadConfigFile(path);
      setActiveFile(path);
      setActiveFileContent(resp.content ?? "");
      notify.success(`Loaded ${path}.`);
    } catch (e) {
      onError(e);
    }
  };

  const saveRelatedFile = async () => {
    if (!activeFile) return;
    try {
      await api.saveConfigFile(activeFile, activeFileContent);
      await refreshConfigFiles(activeFile);
      notify.success(`Saved ${activeFile}.`);
    } catch (e) {
      onError(e);
    }
  };

  const runBatchFileOps = async (operations: ConfigRelatedFileBatchOperation[], successVerb: string) => {
    if (operations.length === 0) {
      notify.info("No file operations to apply.");
      return;
    }
    try {
      const resp = await api.batchConfigFiles(operations);
      await refreshConfigFiles(activeFile);
      const firstError = resp.results.find((r) => !r.ok)?.error;
      if (resp.failed > 0) {
        notify.warning({
          message: `${successVerb}: ${resp.ok}/${resp.total} succeeded`,
          description: firstError ? `First error: ${firstError}` : undefined,
        });
      } else {
        notify.success(`${successVerb}: ${resp.ok}/${resp.total} succeeded.`);
      }
    } catch (e) {
      onError(e);
    }
  };

  const bulkAddOrUpdateFiles = async () => {
    const paths = Array.from(
      new Set(
        bulkAddPaths
          .split("\n")
          .map((p) => p.trim())
          .filter(Boolean),
      ),
    );
    const operations = paths.map((path) => ({ action: "add" as const, path, content: bulkAddContent }));
    await runBatchFileOps(operations, "Bulk add/update complete");
  };

  const bulkRemoveFiles = async () => {
    const operations = bulkRemovePaths.map((path) => ({ action: "remove" as const, path }));
    await runBatchFileOps(operations, "Bulk remove complete");
  };

  const applyPolicyGates = (csv: string) => {
    setContent((prev) => applyPolicyGatesToConfigContent(prev, csv));
  };

  // Pre-compute per-field statuses based on whether they differ from the
  // original (info chip) or trigger a known-bad combination (warn chip).
  const fieldStatuses: Record<string, "warn" | "info" | undefined> = useMemo(() => {
    const map: Record<string, "warn" | "info" | undefined> = {};
    const { doc: origDoc } = safeParse(originalContent);
    const currDoc = docRef.current;
    for (const k of KNOWN_KEYS) {
      const a = ensureString(origDoc.get(k));
      const b = ensureString(currDoc.get(k));
      if (a !== b) map[k] = "info";
    }
    if (readString(currDoc, "password").trim()) {
      map["password"] = "warn";
    }
    if (readBoolean(currDoc, "insecure-skip-verify")) {
      map["insecure-skip-verify"] = "warn";
    }
    return map;
  }, [originalContent, content]);

  // Keys present in YAML but unknown to our schema — surfaced in "Other".
  const otherKeys: string[] = useMemo(() => {
    const out: string[] = [];
    const contents = docRef.current.contents;
    if (contents && typeof contents === "object" && "items" in contents) {
      const items = (contents as { items?: { key?: { value?: string } }[] }).items ?? [];
      for (const it of items) {
        const k = it.key?.value;
        if (typeof k === "string" && !KNOWN_KEYS.includes(k)) {
          out.push(k);
        }
      }
    }
    return out;
  }, [content]);

  const dirtyCount = useMemo(() => {
    if (!dirty) return 0;
    return Object.values(fieldStatuses).filter((v) => v === "info").length;
  }, [dirty, fieldStatuses]);

  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      {/* Toolbar */}
      <Card className="page-card config-toolbar-card">
        <Row gutter={[16, 12]} align="middle">
          <Col xs={24} md={12}>
            <Space size={10} align="center">
              <SettingOutlined className="section-header-icon lg" />
              <div>
                <Typography.Title level={4} style={{ margin: 0 }}>
                  Application Config
                </Typography.Title>
                <Typography.Text type="secondary" style={{ fontSize: 13 }}>
                  {configPath ? (
                    <>
                      Editing <Typography.Text code copyable={{ text: configPath }}>{configPath}</Typography.Text>
                    </>
                  ) : (
                    "Edit your YAML config below."
                  )}
                </Typography.Text>
              </div>
            </Space>
          </Col>
          <Col xs={24} md={12}>
            <Space size={8} wrap style={{ display: "flex", justifyContent: "flex-end" }}>
              <Radio.Group
                id="config-edit-mode"
                name="config-edit-mode"
                aria-label="Config editor mode"
                value={mode}
                onChange={(e) => setMode(e.target.value)}
                buttonStyle="solid"
              >
                <Radio.Button value="form">Form</Radio.Button>
                <Radio.Button value="yaml">YAML</Radio.Button>
              </Radio.Group>
              <Tooltip title="Reload config from disk">
                <Button icon={<ReloadOutlined />} onClick={() => void load()}>
                  Reload
                </Button>
              </Tooltip>
              <Tooltip title="Revert unsaved changes (⇧⌘Z / Ctrl+Shift+Z)">
                <Button icon={<RocketOutlined />} onClick={revert} disabled={!dirty}>
                  Revert
                </Button>
              </Tooltip>
              <Tooltip title="Save config (⌘S / Ctrl+S)">
                <Badge count={dirtyCount} offset={[-2, 4]} size="small">
                  <Button type="primary" icon={<SaveOutlined />} onClick={save} disabled={!dirty || Boolean(parseError)}>
                    Save Config
                  </Button>
                </Badge>
              </Tooltip>
            </Space>
          </Col>
        </Row>
        {parseError ? (
          <Alert
            type="error"
            showIcon
            style={{ marginTop: 12 }}
            message="YAML parse error"
            description={parseError}
          />
        ) : null}
        {dirty && !parseError ? (
          <Alert
            type="info"
            showIcon
            style={{ marginTop: 12 }}
            message="Unsaved changes"
            description={`${dirtyCount} field${dirtyCount === 1 ? "" : "s"} changed since last load. Click Save to persist.`}
          />
        ) : null}
      </Card>

      {/* Mode body */}
      {mode === "form" ? (
        <Row gutter={[16, 16]}>
          <Col xs={24} md={6} lg={5}>
            <Card className="page-card config-anchor-card">
              <Typography.Text strong style={{ display: "block", marginBottom: 8 }}>
                Sections
              </Typography.Text>
              <Anchor
                affix={false}
                getContainer={() => window}
                items={SECTIONS.map((s) => ({
                  key: s.id,
                  href: `#config-section-${s.id}`,
                  title: (
                    <Space size={6}>
                      <span>{SECTION_ICONS[s.icon]}</span>
                      <span>{s.title}</span>
                    </Space>
                  ),
                }))}
              />
            </Card>
          </Col>
          <Col xs={24} md={18} lg={19}>
            <Space direction="vertical" size={16} style={{ width: "100%" }}>
              {SECTIONS.map((section) => (
                <SectionForm
                  key={section.id}
                  section={section}
                  doc={docRef.current}
                  onChange={refreshFromDoc}
                  fieldStatuses={fieldStatuses}
                  onMigrateSecret={() => setSecretsModalOpen(true)}
                />
              ))}

              {otherKeys.length > 0 ? (
                <Card className="page-card" id="config-section-other">
                  <Typography.Title level={4} style={{ margin: 0 }}>
                    Other Settings
                  </Typography.Title>
                  <Typography.Text type="secondary" className="section-subtitle">
                    Keys present in your YAML that aren't covered by the form. Switch to YAML mode to edit.
                  </Typography.Text>
                  <List
                    style={{ marginTop: 10 }}
                    bordered
                    size="small"
                    dataSource={otherKeys}
                    renderItem={(k) => (
                      <List.Item>
                        <Typography.Text code>{k}</Typography.Text>
                        <Typography.Text type="secondary" style={{ marginLeft: 12 }}>
                          {ensureString(docRef.current.get(k))}
                        </Typography.Text>
                      </List.Item>
                    )}
                  />
                </Card>
              ) : null}
            </Space>
          </Col>
        </Row>
      ) : (
        <Card className="page-card">
          <CodeEditor value={content} onChange={setContent} language="yaml" height={520} />
        </Card>
      )}

      {/* Policy gates builder */}
      <Card className="page-card">
        <PolicyGateBuilderSection configContent={content} onApplyPolicyGates={applyPolicyGates} />
      </Card>

      {/* Linked files */}
      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Linked Files
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          Files referenced by the active config. Edit them in place without leaving Settings.
        </Typography.Text>
        <Space size={8} style={{ marginTop: 12, marginBottom: 12 }}>
          <Button size="small" icon={<ReloadOutlined />} onClick={() => refreshConfigFiles(activeFile)}>
            Refresh
          </Button>
        </Space>
        <Card size="small" style={{ marginBottom: 12 }}>
          <Space direction="vertical" size={10} style={{ width: "100%" }}>
            <Typography.Text strong>Bulk add/update files</Typography.Text>
            <Typography.Text type="secondary">
              Enter one referenced config file path per line and apply the same content to all.
            </Typography.Text>
            <Input.TextArea
              value={bulkAddPaths}
              onChange={(e) => setBulkAddPaths(e.target.value)}
              rows={3}
              placeholder={"clusters.txt\nexclude.txt\nsecrets.yaml"}
            />
            <CodeEditor
              value={bulkAddContent}
              onChange={setBulkAddContent}
              language={inferEditorLanguage((bulkAddPaths.split("\n")[0] || "").trim() || "txt")}
              height={160}
            />
            <Button type="primary" onClick={() => void bulkAddOrUpdateFiles()}>
              Add/Update Multiple Files
            </Button>
          </Space>
        </Card>
        <Card size="small" style={{ marginBottom: 12 }}>
          <Space direction="vertical" size={10} style={{ width: "100%" }}>
            <Typography.Text strong>Bulk remove files</Typography.Text>
            <Typography.Text type="secondary">
              Select one or more referenced files to delete from disk.
            </Typography.Text>
            <Select
              mode="multiple"
              allowClear
              style={{ width: "100%" }}
              value={bulkRemovePaths}
              onChange={(vals) => setBulkRemovePaths(vals)}
              options={files.map((f) => ({ label: `${f.key}: ${f.path}`, value: f.path }))}
              placeholder="Select files to remove"
            />
            <Button danger onClick={() => void bulkRemoveFiles()}>
              Remove Selected Files
            </Button>
          </Space>
        </Card>
        {files.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No related files detected in config yet" />
        ) : (
          <List
            bordered
            size="small"
            dataSource={files}
            renderItem={(item) => (
              <List.Item
                actions={[
                  <Button key="use" size="small" onClick={() => loadRelatedFile(item.path)}>
                    Open
                  </Button>,
                ]}
              >
                <Space direction="vertical" size={0}>
                  <Typography.Text code>{item.key}</Typography.Text>
                  <Typography.Text>{item.path}</Typography.Text>
                  {item.resolved_path ? (
                    <Typography.Text type="secondary">{item.resolved_path}</Typography.Text>
                  ) : (
                    <Typography.Text type="warning">Path escapes repo root</Typography.Text>
                  )}
                </Space>
                <Tag color={item.exists ? "success" : "default"}>{item.exists ? "exists" : "missing"}</Tag>
              </List.Item>
            )}
          />
        )}
        {activeFile ? (
          <div style={{ marginTop: 16 }}>
            <Space size={8} style={{ marginBottom: 8 }}>
              <Typography.Text strong>Editing: {activeFile}</Typography.Text>
              <Button size="small" icon={<ReloadOutlined />} onClick={() => loadRelatedFile(activeFile)}>
                Reload
              </Button>
              <Button size="small" type="primary" icon={<SaveOutlined />} onClick={saveRelatedFile} disabled={dirtyFile}>
                Save File
              </Button>
            </Space>
            <CodeEditor
              value={activeFileContent}
              onChange={setActiveFileContent}
              language={inferEditorLanguage(activeFile)}
              height={260}
            />
          </div>
        ) : null}
      </Card>
      <SecretsMigrationModal
        open={secretsModalOpen}
        passwordValue={readString(docRef.current, "password")}
        hasProvider={Boolean(readString(docRef.current, "secrets-provider").trim())}
        getDoc={() => docRef.current}
        onApply={() => {
          refreshFromDoc();
          setSecretsModalOpen(false);
        }}
        onClose={() => setSecretsModalOpen(false)}
      />
    </Space>
  );
}
