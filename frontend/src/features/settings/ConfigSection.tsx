import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Alert,
  Anchor,
  Badge,
  Button,
  Card,
  Col,
  Collapse,
  Empty,
  Form,
  Input,
  InputNumber,
  Radio,
  Row,
  Segmented,
  Select,
  Space,
  Switch,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  ApartmentOutlined,
  ApiOutlined,
  BellOutlined,
  CloudServerOutlined,
  DeleteOutlined,
  DeploymentUnitOutlined,
  EyeInvisibleOutlined,
  EyeOutlined,
  FileTextOutlined,
  KeyOutlined,
  LinkOutlined,
  PlusOutlined,
  ReloadOutlined,
  RocketOutlined,
  SafetyCertificateOutlined,
  SaveOutlined,
  SearchOutlined,
  ThunderboltOutlined,
  UndoOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import type { ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Document, parseDocument } from "yaml";
import { api } from "../../api/client";
import { CodeEditor, inferEditorLanguage } from "../../components/CodeEditor";
import type { ConfigRelatedFileBatchOperation, ConfigRelatedFileInfo } from "../../api/types";
import { EnvironmentsModal } from "./EnvironmentsModal";
import { PolicyGateBuilderSection } from "./PolicyGateBuilderSection";
import { SecretsMigrationModal } from "./SecretsMigrationModal";
import { notify } from "../../notify";
import { ItemList, SaveBar, TitleIcon, middleEllipsis } from "../../ui";
import {
  ALL_FIELDS,
  SECTIONS,
  fieldError,
  groupFields,
  matchesQuery,
  type FieldDef,
  type SectionDef,
  type SectionIcon,
} from "./configSchema";
import {
  changedKeys,
  differsFromDefault,
  isNested,
  locate,
  otherEntries,
  readBool,
  readList,
  readMap,
  readNumber,
  readText,
  writeValue,
} from "./configDoc";

type Props = {
  onError: (e: unknown) => void;
};

type Mode = "form" | "yaml";
type Show = "all" | "changed" | "set";

const SECTION_ICONS: Record<SectionIcon, ReactNode> = {
  cluster: <CloudServerOutlined />,
  connection: <ApiOutlined />,
  perf: <ThunderboltOutlined />,
  outputs: <ApartmentOutlined />,
  policy: <SafetyCertificateOutlined />,
  logs: <FileTextOutlined />,
  notify: <BellOutlined />,
  secrets: <KeyOutlined />,
};

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
    <Space orientation="vertical" size={4} style={{ width: "100%" }}>
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
            <WarningOutlined /> Stored as plain text in the config file.
          </Typography.Text>
          {onMigrate ? (
            <Button size="small" type="link" onClick={onMigrate} style={{ padding: 0 }}>
              Use a secret reference instead
            </Button>
          ) : null}
        </Space>
      ) : null}
    </Space>
  );
}

type KvRow = { id: number; name: string; value: string };
let rowSeq = 0;

/** Name/value rows for a string map. Rows without a name are not saved. */
function KeyValueEditor({ id, value, onChange }: { id: string; value: [string, string][]; onChange: (next: Record<string, string> | undefined) => void }) {
  const [rows, setRows] = useState<KvRow[]>(() => value.map(([name, v]) => ({ id: ++rowSeq, name, value: v })));
  const fromRows = (list: KvRow[]) => list.filter((r) => r.name.trim()).map((r) => [r.name.trim(), r.value] as [string, string]);
  // Pick up changes made elsewhere (YAML mode, reload) without fighting typing.
  useEffect(() => {
    if (JSON.stringify(fromRows(rows)) !== JSON.stringify(value)) {
      setRows(value.map(([name, v]) => ({ id: ++rowSeq, name, value: v })));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(value)]);
  const commit = (next: KvRow[]) => {
    setRows(next);
    const entries = fromRows(next);
    onChange(entries.length ? Object.fromEntries(entries) : undefined);
  };
  return (
    <div className="config-kv" id={id}>
      {rows.map((r, i) => (
        <Space.Compact key={r.id} style={{ width: "100%" }}>
          <Input
            aria-label={`Header ${i + 1} name`}
            placeholder="Header"
            value={r.name}
            style={{ width: "40%" }}
            autoComplete="off"
            onChange={(e) => commit(rows.map((x) => (x.id === r.id ? { ...x, name: e.target.value } : x)))}
          />
          <Input.Password
            aria-label={`Header ${i + 1} value`}
            placeholder="Value"
            value={r.value}
            autoComplete="new-password"
            onChange={(e) => commit(rows.map((x) => (x.id === r.id ? { ...x, value: e.target.value } : x)))}
          />
          <Button aria-label={`Remove header ${i + 1}`} icon={<DeleteOutlined />} onClick={() => commit(rows.filter((x) => x.id !== r.id))} />
        </Space.Compact>
      ))}
      <Button size="small" icon={<PlusOutlined />} onClick={() => setRows([...rows, { id: ++rowSeq, name: "", value: "" }])}>
        Add header
      </Button>
    </div>
  );
}

/** Stable DOM id for a config field, so its label can point at the input. */
function fieldDomId(key: string) {
  return `cfg-${key}`;
}

function defaultLabel(field: FieldDef): string | null {
  if (field.defaultValue === undefined) return null;
  if (field.type === "boolean") return field.defaultValue === "true" ? "On" : "Off";
  const opt = field.options?.find((o) => o.value === field.defaultValue);
  if (opt) return opt.label;
  if (field.type === "multiselect") return field.defaultValue.split(",").map((v) => field.options?.find((o) => o.value === v)?.label ?? v).join(", ");
  return field.defaultValue;
}

function FieldControl({
  field,
  doc,
  edit,
  error,
  onMigrateSecret,
}: {
  field: FieldDef;
  doc: Document;
  edit: (key: string, value: unknown) => void;
  error: string | null;
  onMigrateSecret?: () => void;
}) {
  const id = fieldDomId(field.key);
  const fallback = defaultLabel(field);
  const placeholder = field.placeholder ?? (fallback !== null ? fallback : undefined);
  switch (field.type) {
    case "boolean":
      return <Switch id={id} checked={readBool(doc, field.key)} onChange={(checked) => edit(field.key, checked)} />;
    case "int": {
      const v = readNumber(doc, field.key);
      return (
        <InputNumber
          id={id}
          name={field.key}
          value={v ?? undefined}
          min={field.min}
          max={field.max}
          placeholder={placeholder}
          style={{ width: "100%" }}
          onChange={(next) => edit(field.key, next ?? undefined)}
        />
      );
    }
    case "select": {
      const v = readText(doc, field.key);
      return (
        <Select
          id={id}
          value={v || undefined}
          placeholder={placeholder}
          allowClear
          options={field.options ?? []}
          style={{ width: "100%" }}
          onChange={(next?: string) => edit(field.key, next ?? undefined)}
        />
      );
    }
    case "multiselect":
    case "list": {
      const v = readList(doc, field.key);
      return (
        <Select
          id={id}
          mode={field.type === "list" ? "tags" : "multiple"}
          value={v}
          placeholder={placeholder ?? (field.type === "list" ? "Type a value and press Enter" : undefined)}
          options={field.options}
          tokenSeparators={[","]}
          notFoundContent={null}
          allowClear
          style={{ width: "100%" }}
          onChange={(next: string[]) => edit(field.key, next)}
        />
      );
    }
    case "keyvalue":
      return <KeyValueEditor id={id} value={readMap(doc, field.key)} onChange={(next) => edit(field.key, next)} />;
    case "password": {
      const v = readText(doc, field.key);
      const isPlaintextPassword = field.key === "password" && v.trim().length > 0 && !v.trim().toLowerCase().startsWith("secret://");
      return (
        <SecretInput
          id={id}
          name={field.key}
          value={v}
          placeholder={placeholder}
          onChange={(next) => edit(field.key, next)}
          isWeakPlaintext={isPlaintextPassword}
          onMigrate={isPlaintextPassword ? onMigrateSecret : undefined}
        />
      );
    }
    case "textarea":
      return (
        <Input.TextArea
          id={id}
          name={field.key}
          value={readText(doc, field.key)}
          placeholder={placeholder}
          autoSize={{ minRows: 2, maxRows: 8 }}
          autoComplete="off"
          className="config-mono"
          onChange={(e) => edit(field.key, e.target.value)}
        />
      );
    case "duration":
    case "string":
    default:
      return (
        <Input
          id={id}
          name={field.key}
          value={readText(doc, field.key)}
          placeholder={placeholder}
          status={error ? "error" : undefined}
          // "off" keeps browsers from filling an email into fields like PC IP.
          autoComplete={field.autoComplete ?? "off"}
          onChange={(e) => edit(field.key, e.target.value)}
        />
      );
  }
}

function FieldItem({
  field,
  doc,
  nested,
  edit,
  changed,
  warn,
  error,
  onMigrateSecret,
  inherit,
}: {
  field: FieldDef;
  doc: Document;
  nested: boolean;
  edit: (key: string, value: unknown) => void;
  changed: boolean;
  warn: boolean;
  error: string | null;
  onMigrateSecret?: () => void;
  /** Editing an environment: the main config, whose value applies when unset here. */
  inherit?: Document | null;
}) {
  const where = locate(doc, field);
  const keyText = (where ?? (nested ? field.path : [field.key])).join(".");
  const set = where !== null;
  const custom = differsFromDefault(doc, field.key);
  const inheritedRaw = inherit && !set && locate(inherit, field) !== null ? readText(inherit, field.key) : "";
  const inherited = inheritedRaw && field.type === "password" ? "set" : middleEllipsis(inheritedRaw, 60);
  const fallback = inherit && set && locate(inherit, field) !== null ? "the main config's value" : defaultLabel(field);
  const wide = field.wide || field.type === "textarea" || field.type === "keyvalue";
  return (
    <Col xs={24} md={wide ? 24 : 12}>
      <Form.Item
        htmlFor={fieldDomId(field.key)}
        className="config-field"
        validateStatus={error ? "error" : undefined}
        help={error ?? undefined}
        label={
          <div className="config-field-label">
            <span className="config-field-name">{field.label}</span>
            {field.required ? <Tag color="warning">Required</Tag> : null}
            {warn ? <Tag color="warning">Check</Tag> : null}
            {changed ? <Tag color="processing">Changed</Tag> : null}
            <Tooltip title={set ? "Key in this file" : "Key this setting is saved under"}>
              <code className="config-field-key">{keyText}</code>
            </Tooltip>
            {custom && fallback !== null ? (
              <Tooltip title={`Remove from the file; the runner then uses ${fallback}`}>
                <Button type="link" size="small" className="config-field-reset" icon={<UndoOutlined />} onClick={() => edit(field.key, undefined)}>
                  Use default
                </Button>
              </Tooltip>
            ) : null}
          </div>
        }
        extra={
          field.help || (!set && (fallback !== null || inherited)) ? (
            <span className="config-field-help">
              {field.help}
              {!set && inherited ? (
                <span className="config-field-default">
                  {field.help ? " " : ""}From the main config: {inherited}.
                </span>
              ) : !set && fallback !== null ? (
                <span className="config-field-default">
                  {field.help ? " " : ""}Default: {fallback}.
                </span>
              ) : null}
            </span>
          ) : null
        }
      >
        <FieldControl field={field} doc={doc} edit={edit} error={error} onMigrateSecret={onMigrateSecret} />
      </Form.Item>
    </Col>
  );
}

function SectionForm({
  section,
  fields,
  doc,
  nested,
  edit,
  changed,
  warnings,
  errors,
  onMigrateSecret,
  extra,
  inherit,
}: {
  section: SectionDef;
  fields: FieldDef[];
  doc: Document;
  nested: boolean;
  edit: (key: string, value: unknown) => void;
  changed: Set<string>;
  warnings: Set<string>;
  errors: Record<string, string>;
  onMigrateSecret?: () => void;
  extra?: ReactNode;
  inherit?: Document | null;
}) {
  return (
    <Card id={`config-section-${section.id}`} className="page-card config-section-card">
      <div className="config-section-head">
        <TitleIcon size="lg">{SECTION_ICONS[section.icon]}</TitleIcon>
        <div>
          <h3 className="ui-panel-title">{section.title}</h3>
          <p className="ui-panel-desc">{section.description}</p>
        </div>
      </div>
      {/* One <form> per section so password fields have a form ancestor;
          Enter is swallowed because saving happens from the page toolbar. */}
      <Form layout="vertical" colon={false} requiredMark={false} onSubmitCapture={(e) => e.preventDefault()}>
        {groupFields(fields).map(({ group, fields: groupItems }) => (
          <section key={group} className="config-group" aria-label={`${section.title}: ${group}`}>
            <Typography.Text className="config-group-title">{group}</Typography.Text>
            <Row gutter={[20, 0]}>
              {groupItems.map((field) => (
                <FieldItem
                  key={field.key}
                  field={field}
                  doc={doc}
                  nested={nested}
                  edit={edit}
                  changed={changed.has(field.key)}
                  warn={warnings.has(field.key)}
                  error={errors[field.key] ?? null}
                  onMigrateSecret={onMigrateSecret}
                  inherit={inherit}
                />
              ))}
            </Row>
          </section>
        ))}
      </Form>
      {extra}
    </Card>
  );
}

export function ConfigSection({ onError }: Props) {
  const [mode, setMode] = useState<Mode>("form");
  const [content, setContent] = useState("");
  const [originalContent, setOriginalContent] = useState("");
  const [loaded, setLoaded] = useState(false);
  const [configPath, setConfigPath] = useState("");
  // "" edits the main config; otherwise the environment of that name.
  const [target, setTarget] = useState("");
  const [mainContent, setMainContent] = useState("");
  const [envModalOpen, setEnvModalOpen] = useState(false);
  const envList = useQuery({ queryKey: ["environments"], queryFn: api.listEnvironments });
  const queryClient = useQueryClient();
  const [files, setFiles] = useState<ConfigRelatedFileInfo[]>([]);
  const [activeFile, setActiveFile] = useState<string>("");
  const [activeFileContent, setActiveFileContent] = useState("");
  const [bulkAddPaths, setBulkAddPaths] = useState("");
  const [bulkAddContent, setBulkAddContent] = useState("");
  const [bulkRemovePaths, setBulkRemovePaths] = useState<string[]>([]);
  // Updates & backups → Migrations links here with ?open=secrets.
  const [secretsModalOpen, setSecretsModalOpen] = useState(
    () => new URLSearchParams(window.location.search).get("open") === "secrets",
  );
  const [query, setQuery] = useState("");
  const [show, setShow] = useState<Show>("all");

  const dirty = content !== originalContent;

  // The form always renders from the current text, so YAML-mode edits, reloads
  // and form edits can never disagree.
  const parsed = useMemo(() => safeParse(content), [content]);
  const original = useMemo(() => safeParse(originalContent).doc, [originalContent]);
  const doc = parsed.doc;
  const parseError = parsed.error;
  const nested = isNested(doc) || Boolean(target);
  const mainDoc = useMemo(() => (target ? safeParse(mainContent).doc : null), [target, mainContent]);

  const edit = useCallback(
    (key: string, value: unknown) => {
      const next = doc.clone();
      writeValue(next, key, value);
      setContent(next.toString());
    },
    [doc],
  );

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

  const load = useCallback(async (suppressError = false, silent = false, requestedPath?: string, env = "") => {
    try {
      const cfg = await api.loadConfig(env ? undefined : requestedPath);
      setMainContent(cfg.content ?? "");
      let text = cfg.content ?? "";
      let path = cfg.path ?? "";
      if (env) {
        const e = await api.getEnvironment(env);
        text = e.content;
        path = e.path;
      }
      setContent(text);
      setOriginalContent(text);
      setConfigPath(path);
      setTarget(env);
      setLoaded(true);
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

  const changed = useMemo(() => new Set(changedKeys(original, doc)), [original, doc]);

  const errors = useMemo(() => {
    const out: Record<string, string> = {};
    if (parseError) return out;
    for (const f of ALL_FIELDS) {
      const msg = fieldError(f, readText(doc, f.key));
      if (msg) out[f.key] = msg;
    }
    return out;
  }, [doc, parseError]);
  const errorCount = Object.keys(errors).length;

  const warnings = useMemo(() => {
    const out = new Set<string>();
    const pw = readText(doc, "password").trim();
    if (pw && !pw.toLowerCase().startsWith("secret://")) out.add("password");
    if (readBool(doc, "insecure-skip-verify")) out.add("insecure-skip-verify");
    if (readBool(doc, "skip-preflight-check")) out.add("skip-preflight-check");
    if (readNumber(doc, "gen-test-agg")) out.add("gen-test-agg");
    if (readBool(doc, "replay")) out.add("replay");
    return out;
  }, [doc]);

  const visibleFields = useCallback(
    (section: SectionDef) =>
      section.fields.filter(
        (f) =>
          // An environment holds only runner settings.
          (!target || f.path[0] === "runner") &&
          matchesQuery(f, query) &&
          (show === "all" || (show === "changed" ? changed.has(f.key) : differsFromDefault(doc, f.key))),
      ),
    [query, show, changed, doc, target],
  );

  const others = useMemo(
    () => (parseError ? [] : otherEntries(doc).filter((o) => !(target && o.path === "description"))),
    [doc, parseError, target],
  );

  const save = async () => {
    if (parseError) {
      notify.warning({ message: "Cannot save", description: `YAML is invalid: ${parseError}` });
      return;
    }
    if (errorCount > 0) {
      notify.warning({ message: "Cannot save", description: `${errorCount} setting${errorCount === 1 ? " needs" : "s need"} fixing first.` });
      return;
    }
    if (content === originalContent) {
      notify.info("No changes to save.");
      return;
    }
    try {
      if (target) {
        await api.saveEnvironment(target, content);
        setOriginalContent(content);
        void queryClient.invalidateQueries({ queryKey: ["environments"] });
        notify.success(`Saved ${target}.`);
        return;
      }
      await api.saveConfig(content, configPath || undefined);
      setOriginalContent(content);
      setMainContent(content);
      await refreshConfigFiles(activeFile);
      notify.success("Config saved.");
    } catch (e) {
      onError(e);
    }
  };

  const switchTarget = async (next: string) => {
    if (next === target) return;
    if (dirty) {
      notify.warning({ message: "Unsaved changes", description: "Save or revert them before opening another file." });
      return;
    }
    await load(false, true, undefined, next);
  };

  const revert = () => {
    if (content === originalContent) {
      notify.info("Already in sync with last loaded config.");
      return;
    }
    setContent(originalContent);
    notify.info("Reverted to last loaded config.");
  };

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

  const dirtyCount = dirty ? changed.size : 0;
  const sectionsShown = SECTIONS.map((s) => ({ section: s, fields: visibleFields(s) })).filter((s) => s.fields.length > 0);

  return (
    // A plain flex column, not <Space>: Space wraps each child in its own box,
    // which stops the toolbar and section list from staying pinned.
    <div className="config-page">
      {/* Toolbar */}
      <Card className="page-card config-toolbar-card">
        <Row gutter={[16, 12]} align="middle">
          <Col xs={24} lg={11}>
            <Space size={10} align="center">
              <div>
                <Space size={8} wrap>
                  <Select
                    aria-label="Config to edit"
                    className="config-target-select"
                    value={target}
                    onChange={(v) => void switchTarget(v)}
                    popupMatchSelectWidth={false}
                    options={[
                      { value: "", label: "Main config" },
                      ...(envList.data?.items ?? []).map((e) => ({ value: e.name, label: `Environment: ${e.name}` })),
                    ]}
                  />
                  {loaded && !parseError && !target ? (
                    <Tooltip
                      title={
                        nested
                          ? "This file uses the nested layout (schema-version: 1). New settings are saved under runner:, storage:, notifications: and so on."
                          : "This file uses flat keys. New settings are saved as top-level keys."
                      }
                    >
                      <Tag>{nested ? "Nested layout" : "Flat layout"}</Tag>
                    </Tooltip>
                  ) : null}
                </Space>
                <Typography.Text type="secondary" className="config-toolbar-path">
                  {configPath ? (
                    <>
                      Editing{" "}
                      <Tooltip title={configPath}>
                        <Typography.Text code copyable={{ text: configPath }}>
                          {middleEllipsis(configPath, 48)}
                        </Typography.Text>
                      </Tooltip>
                    </>
                  ) : (
                    "Edit your YAML config below."
                  )}
                </Typography.Text>
              </div>
            </Space>
          </Col>
          <Col xs={24} lg={13}>
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
                <Button icon={<ReloadOutlined />} onClick={() => void load(false, false, undefined, target)}>
                  Reload
                </Button>
              </Tooltip>
              <Button icon={<DeploymentUnitOutlined />} onClick={() => setEnvModalOpen(true)}>
                Environments
              </Button>
              <Tooltip title="Revert unsaved changes (⇧⌘Z / Ctrl+Shift+Z)">
                <Button icon={<RocketOutlined />} onClick={revert} disabled={!dirty}>
                  Revert
                </Button>
              </Tooltip>
              <Tooltip title="Save config (⌘S / Ctrl+S)">
                <Badge count={dirtyCount} offset={[-2, 4]} size="small">
                  <Button type="primary" icon={<SaveOutlined />} onClick={save} disabled={!dirty || Boolean(parseError) || errorCount > 0}>
                    Save
                  </Button>
                </Badge>
              </Tooltip>
            </Space>
          </Col>
        </Row>
        {target ? (
          <Alert
            type="info"
            showIcon
            style={{ marginTop: 12 }}
            title={`Editing the ${target} environment`}
            description="Only runner settings live here. Anything not set comes from the main config, shown under each setting. Storage, notifications, logging and server settings are always the main config's."
          />
        ) : null}
        {parseError ? (
          <Alert
            type="error"
            showIcon
            style={{ marginTop: 12 }}
            title="YAML parse error"
            description={parseError}
          />
        ) : null}
      </Card>

      {mode === "form" ? (
        parseError ? (
          <Alert
            type="warning"
            showIcon
            title="The form is unavailable while the YAML has an error"
            description="Switch to YAML to fix it, or Revert to the saved file."
          />
        ) : (
          <Row gutter={[16, 16]}>
            <Col xs={24} md={8} lg={7} xl={6}>
              <Card className="page-card config-anchor-card">
                <Input
                  allowClear
                  prefix={<SearchOutlined />}
                  placeholder="Find a setting"
                  aria-label="Find a setting"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                />
                <Segmented<Show>
                  block
                  size="small"
                  className="config-show-filter"
                  value={show}
                  onChange={setShow}
                  options={[
                    { value: "all", label: "All" },
                    { value: "changed", label: "Changed" },
                    { value: "set", label: "Custom" },
                  ]}
                />
                <Anchor
                  affix={false}
                  targetOffset={184}
                  getContainer={() => window}
                  items={sectionsShown.map(({ section, fields }) => {
                    const n = section.fields.filter((f) => changed.has(f.key)).length;
                    const bad = section.fields.some((f) => errors[f.key]);
                    return {
                      key: section.id,
                      href: `#config-section-${section.id}`,
                      title: (
                        <span className="config-anchor-item">
                          <span className="config-anchor-icon">{SECTION_ICONS[section.icon]}</span>
                          <span className="config-anchor-text">{section.title}</span>
                          {bad ? (
                            <Tooltip title="Has a setting to fix">
                              <WarningOutlined className="config-anchor-warn" />
                            </Tooltip>
                          ) : n > 0 ? (
                            <Tooltip title={`${n} changed`}>
                              <span className="config-anchor-count">{n}</span>
                            </Tooltip>
                          ) : query || show !== "all" ? (
                            <span className="config-anchor-muted">{fields.length}</span>
                          ) : null}
                        </span>
                      ),
                    };
                  })}
                />
              </Card>
            </Col>
            <Col xs={24} md={16} lg={17} xl={18}>
              <Space orientation="vertical" size={16} style={{ width: "100%" }}>
                {sectionsShown.length === 0 ? (
                  <Card className="page-card">
                    <Empty
                      description={
                        show === "changed" && !query
                          ? "No unsaved changes."
                          : show === "set" && !query
                            ? "Every setting uses its default."
                            : "No settings match."
                      }
                    >
                      {query || show !== "all" ? (
                        <Button
                          onClick={() => {
                            setQuery("");
                            setShow("all");
                          }}
                        >
                          Show all settings
                        </Button>
                      ) : null}
                    </Empty>
                  </Card>
                ) : null}
                {sectionsShown.map(({ section, fields }) => (
                  <SectionForm
                    key={section.id}
                    section={section}
                    fields={fields}
                    doc={doc}
                    nested={nested}
                    edit={edit}
                    changed={changed}
                    warnings={warnings}
                    errors={errors}
                    onMigrateSecret={() => setSecretsModalOpen(true)}
                    inherit={mainDoc}
                    extra={
                      section.id === "filtering" && show === "all" && !query ? (
                        <Card size="small" className="config-subcard">
                          <PolicyGateBuilderSection
                            gates={readText(doc, "policy-gates")}
                            ready={loaded}
                            onApplyPolicyGates={(csv) => edit("policy-gates", csv)}
                          />
                        </Card>
                      ) : null
                    }
                  />
                ))}

                {others.length > 0 && show === "all" && !query ? (
                  <Card className="page-card" id="config-section-other">
                    <Typography.Title level={4} style={{ margin: 0 }}>
                      Other keys in this file
                    </Typography.Title>
                    <Typography.Text type="secondary" className="section-subtitle">
                      The form does not edit these. Server notes (api, ui, deployment) are read by the API and UI servers or are documentation only;
                      the runner ignores them. Use YAML mode to change them.
                    </Typography.Text>
                    <ItemList
                      style={{ marginTop: 10 }}
                      bordered
                      items={others}
                      render={(o) => (
                        <>
                          <Space size={8} wrap>
                            <Typography.Text code>{o.path}</Typography.Text>
                            {o.stack ? <Tag>Server note</Tag> : <Tag color="warning">Unknown to the runner</Tag>}
                          </Space>
                          <Typography.Text type="secondary" ellipsis style={{ maxWidth: "45%" }}>
                            {o.value || "—"}
                          </Typography.Text>
                        </>
                      )}
                    />
                  </Card>
                ) : null}
              </Space>
            </Col>
          </Row>
        )
      ) : (
        <Card className="page-card">
          <CodeEditor value={content} onChange={setContent} language="yaml" height={560} />
        </Card>
      )}

      {/* Linked files */}
      <Card className="page-card" hidden={Boolean(target)}>
        <Space style={{ width: "100%", justifyContent: "space-between" }} align="start" wrap>
          <div className="config-section-head">
            <TitleIcon size="lg">
              <LinkOutlined />
            </TitleIcon>
            <div>
            <Typography.Title level={4} className="section-title">
              Linked files
            </Typography.Title>
            <Typography.Text type="secondary" className="section-subtitle">
              Files this config points to, such as a clusters list or secrets file. Open one to edit it here.
            </Typography.Text>
            </div>
          </div>
          <Button size="small" icon={<ReloadOutlined />} onClick={() => refreshConfigFiles(activeFile)}>
            Refresh
          </Button>
        </Space>
        <div style={{ marginTop: 12 }}>
          {files.length === 0 ? (
            <Empty description="This config does not point to any files" />
          ) : (
            <ItemList
              bordered
              items={files}
              actions={(item) => (
                <Button size="small" onClick={() => loadRelatedFile(item.path)}>
                  Open
                </Button>
              )}
              render={(item) => (
                <Space orientation="vertical" size={0}>
                  <Space size={8}>
                    <Typography.Text code>{item.key}</Typography.Text>
                    <Tag color={item.exists ? "success" : "default"}>{item.exists ? "Exists" : "Missing"}</Tag>
                  </Space>
                  <Typography.Text>{item.path}</Typography.Text>
                  {item.resolved_path ? (
                    <Typography.Text type="secondary">{item.resolved_path}</Typography.Text>
                  ) : (
                    <Typography.Text type="warning">Path is outside the app folder</Typography.Text>
                  )}
                </Space>
              )}
            />
          )}
        </div>
        {activeFile ? (
          <div style={{ marginTop: 16 }}>
            <Space size={8} style={{ marginBottom: 8 }}>
              <Typography.Text strong>Editing: {activeFile}</Typography.Text>
              <Button size="small" icon={<ReloadOutlined />} onClick={() => loadRelatedFile(activeFile)}>
                Reload
              </Button>
              <Button size="small" type="primary" icon={<SaveOutlined />} onClick={saveRelatedFile}>
                Save file
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
        <Collapse
          ghost
          style={{ marginTop: 12 }}
          items={[
            {
              key: "bulk",
              label: "Bulk changes",
              children: (
                <Space orientation="vertical" size={16} style={{ width: "100%" }}>
                  <Space orientation="vertical" size={10} style={{ width: "100%" }}>
                    <Typography.Text strong>Add or update several files</Typography.Text>
                    <Typography.Text type="secondary">
                      One path per line; every file gets the same content.
                    </Typography.Text>
                    <Input.TextArea
                      aria-label="Paths to add or update"
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
                      Add or update files
                    </Button>
                  </Space>
                  <Space orientation="vertical" size={10} style={{ width: "100%" }}>
                    <Typography.Text strong>Remove files</Typography.Text>
                    <Typography.Text type="secondary">Deletes the selected files from disk.</Typography.Text>
                    <Select
                      mode="multiple"
                      allowClear
                      aria-label="Files to remove"
                      style={{ width: "100%" }}
                      value={bulkRemovePaths}
                      onChange={(vals) => setBulkRemovePaths(vals)}
                      options={files.map((f) => ({ label: `${f.key}: ${f.path}`, value: f.path }))}
                      placeholder="Select files to remove"
                    />
                    <Button danger disabled={bulkRemovePaths.length === 0} onClick={() => void bulkRemoveFiles()}>
                      Remove selected files
                    </Button>
                  </Space>
                </Space>
              ),
            },
          ]}
        />
      </Card>
      <SecretsMigrationModal
        open={secretsModalOpen}
        passwordValue={readText(doc, "password")}
        hasProvider={Boolean(readText(doc, "secrets-provider").trim())}
        readSetting={(key) => readText(doc, key)}
        writeSetting={(() => {
          // The modal sets several keys in a row; apply them to one copy.
          let working: Document | null = null;
          return (key: string, value: string | undefined) => {
            working = working ?? doc.clone();
            writeValue(working, key, value);
            setContent(working.toString());
          };
        })()}
        onApply={() => setSecretsModalOpen(false)}
        onClose={() => setSecretsModalOpen(false)}
      />
      <EnvironmentsModal
        open={envModalOpen}
        onClose={() => setEnvModalOpen(false)}
        mainContent={mainContent}
        editing={target}
        onEdit={(name) => void switchTarget(name)}
      />
      <SaveBar
        dirty={dirty}
        onSave={() => void save()}
        onDiscard={revert}
        saveDisabled={Boolean(parseError) || errorCount > 0}
        message={
          parseError
            ? "The YAML has an error; fix it before saving."
            : errorCount > 0
              ? `${errorCount} setting${errorCount === 1 ? " needs" : "s need"} fixing before you can save.`
              : `${dirtyCount > 0 ? `${dirtyCount} setting${dirtyCount === 1 ? "" : "s"} changed` : "Unsaved changes"} · Ctrl/⌘+S to save`
        }
        saveLabel="Save config"
      />
    </div>
  );
}
