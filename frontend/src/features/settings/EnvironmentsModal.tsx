import { useEffect, useMemo, useState } from "react";
import { Alert, Button, Input, Modal, Popconfirm, Select, Space, Typography } from "antd";
import { DeleteOutlined, EditOutlined, PlusOutlined } from "@ant-design/icons";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Document, parseDocument } from "yaml";
import { api } from "../../api/client";
import type { EnvironmentInfo } from "../../api/types";
import { notify, notifyError } from "../../notify";
import { ItemList, StatusBadge } from "../../ui";

const NAME_RE = /^[a-z0-9][a-z0-9-]{0,39}$/;

type StartFrom = { kind: "empty" } | { kind: "main" } | { kind: "copy"; name: string } | { kind: "import"; path: string };

function decodeStart(v: string): StartFrom {
  if (v.startsWith("copy:")) return { kind: "copy", name: v.slice(5) };
  if (v.startsWith("import:")) return { kind: "import", path: v.slice(7) };
  return v === "main" ? { kind: "main" } : { kind: "empty" };
}

/** What an environment points at, in a few words. */
export function environmentTargets(e: EnvironmentInfo): string {
  if (e.cluster_count > 0) return `${e.cluster_count.toLocaleString()} cluster${e.cluster_count === 1 ? "" : "s"}`;
  if (e.mode === "pc" || (e.prism_centrals?.length ?? 0) > 0) return "Clusters from Prism Central";
  return "Clusters from the main config";
}

function newEnvironmentYaml(description: string, runner?: unknown): string {
  const doc = new Document({ "schema-version": 1 });
  if (description.trim()) doc.set("description", description.trim());
  if (runner && typeof runner === "object" && Object.keys(runner).length > 0) doc.set("runner", runner);
  return doc.toString();
}

/**
 * Settings → Clusters & config → Environments: list, add (empty, from the main
 * config's runner settings, as a copy, or imported from an older config file)
 * and delete run environments.
 */
export function EnvironmentsModal({
  open,
  onClose,
  mainContent,
  onEdit,
  editing,
}: {
  open: boolean;
  onClose: () => void;
  /** The main config's YAML, for "start from the main config". */
  mainContent: string;
  /** Open an environment ("" for the main config) in the editor. */
  onEdit: (name: string) => void;
  editing: string;
}) {
  const qc = useQueryClient();
  const envs = useQuery({ queryKey: ["environments"], queryFn: api.listEnvironments, enabled: open });
  const importable = useQuery({ queryKey: ["environments", "importable"], queryFn: api.importableConfigFiles, enabled: open });
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [start, setStart] = useState("main");
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!open) {
      setName("");
      setDescription("");
      setStart("main");
    }
  }, [open]);

  const items = envs.data?.items ?? [];
  const nameError =
    name && !NAME_RE.test(name)
      ? "Lower-case letters, digits and dashes, e.g. prod or staging-eu."
      : items.some((e) => e.name === name)
        ? "An environment with this name exists."
        : "";

  const startOptions = useMemo(
    () => [
      { value: "main", label: "The main config's runner settings" },
      { value: "empty", label: "Nothing: inherit everything from the main config" },
      ...items.map((e) => ({ value: `copy:${e.name}`, label: `A copy of ${e.name}` })),
      ...(importable.data?.items ?? []).map((p) => ({ value: `import:${p}`, label: `Import ${p}` })),
    ],
    [items, importable.data],
  );

  const refresh = () => {
    void qc.invalidateQueries({ queryKey: ["environments"] });
  };

  const create = async () => {
    if (!name || nameError) return;
    setBusy(true);
    try {
      const from = decodeStart(start);
      if (from.kind === "import") {
        const out = await api.importEnvironment(from.path, name);
        if (description.trim()) {
          const d = await api.getEnvironment(name);
          const doc = parseDocument(d.content);
          doc.set("description", description.trim());
          await api.saveEnvironment(name, doc.toString());
        }
        notify.success({
          message: `Added ${name}`,
          description: out.dropped.length ? `Kept the runner settings; left out ${out.dropped.join(", ")}.` : undefined,
        });
      } else {
        let runner: unknown;
        if (from.kind === "main") runner = parseDocument(mainContent).toJS()?.runner;
        if (from.kind === "copy") runner = parseDocument((await api.getEnvironment(from.name)).content).toJS()?.runner;
        await api.saveEnvironment(name, newEnvironmentYaml(description, runner), true);
        notify.success(`Added ${name}.`);
      }
      refresh();
      onEdit(name);
      onClose();
    } catch (e) {
      notifyError(e, `Couldn't add ${name}`);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (env: string) => {
    try {
      await api.deleteEnvironment(env);
      notify.success(`Deleted ${env}.`);
      refresh();
      if (editing === env) onEdit("");
    } catch (e) {
      notifyError(e, `Couldn't delete ${env}`);
    }
  };

  return (
    <Modal title="Environments" open={open} onCancel={onClose} footer={null} width={720}>
      <Space orientation="vertical" size={16} style={{ width: "100%" }}>
        <Typography.Paragraph type="secondary" style={{ margin: 0 }}>
          An environment (dev, staging, prod …) holds only runner settings: which clusters, how to sign in to them and how
          runs behave. A run against it uses the main config with these settings on top, so results, notifications and
          server settings never change. Files live in <Typography.Text code>{envs.data?.dir ?? "configs/environments"}</Typography.Text>.
        </Typography.Paragraph>

        <ItemList
          bordered
          items={items}
          itemKey={(e) => e.name}
          empty={envs.isLoading ? "Loading…" : "No environments yet. Runs use the main config."}
          render={(e) => (
            <div className="env-row">
              <span className="env-row-name">
                <strong>{e.name}</strong>
                {e.error ? <StatusBadge tone="fail">Can't be used</StatusBadge> : null}
                {editing === e.name ? <StatusBadge tone="info">Open</StatusBadge> : null}
              </span>
              <span className="ui-field-help">{e.error || [e.description, environmentTargets(e)].filter(Boolean).join(" · ")}</span>
            </div>
          )}
          actions={(e) => (
            <>
              <Button
                size="small"
                icon={<EditOutlined />}
                onClick={() => {
                  onEdit(e.name);
                  onClose();
                }}
              >
                Edit
              </Button>
              <Popconfirm
                title={`Delete ${e.name}?`}
                description="Its schedules are removed too. Past runs keep their results."
                okText="Delete"
                okType="danger"
                onConfirm={() => void remove(e.name)}
              >
                <Button size="small" danger icon={<DeleteOutlined />} aria-label={`Delete ${e.name}`} />
              </Popconfirm>
            </>
          )}
        />

        <div className="env-create">
          <Typography.Text strong>Add an environment</Typography.Text>
          <div className="env-create-grid">
            <div className="ui-field">
              <label htmlFor="env-name" className="ui-field-label">
                Name
              </label>
              <Input
                id="env-name"
                value={name}
                onChange={(e) => setName(e.target.value.toLowerCase())}
                placeholder="prod"
                status={nameError ? "error" : undefined}
                autoComplete="off"
              />
              {nameError ? <span className="ui-field-help is-error">{nameError}</span> : null}
            </div>
            <div className="ui-field">
              <label htmlFor="env-description" className="ui-field-label">
                Description
              </label>
              <Input
                id="env-description"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="Production clusters"
                autoComplete="off"
              />
            </div>
          </div>
          <div className="ui-field">
            <label htmlFor="env-start" className="ui-field-label">
              Start from
            </label>
            <Select id="env-start" value={start} onChange={setStart} options={startOptions} />
          </div>
          {decodeStart(start).kind === "import" ? (
            <Alert
              type="info"
              showIcon
              title="Only the runner settings are imported"
              description="Storage, notifications, logging and server settings in that file are left out; the main config's apply. The file itself is not changed."
            />
          ) : null}
          <div>
            <Button type="primary" icon={<PlusOutlined />} onClick={() => void create()} disabled={!name || Boolean(nameError)} loading={busy}>
              Add environment
            </Button>
          </div>
        </div>
      </Space>
    </Modal>
  );
}

