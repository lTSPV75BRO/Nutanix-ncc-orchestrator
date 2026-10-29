import { useEffect, useMemo, useState } from "react";
import { Alert, Button, Col, Collapse, Drawer, Input, Row, Select, Space, Switch, Typography } from "antd";
import { FileSearchOutlined, ThunderboltOutlined } from "@ant-design/icons";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConfigListItem, RunPreflightData } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { RUNS_ACTIVE_QUERY_KEY } from "../../hooks/useRunsActive";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { notify, notifyError } from "../../notify";
import { StatusBadge } from "../../ui";
import { buildExtraArgs, describeExtraArgs, type RunOptionValues } from "../../utils/runOptions";

const RUN_TOAST_KEY = "ncc-run-active";

/**
 * Start a run: config, optional one-off password, cluster subset (group
 * members), run options from the server's allow-list, and a preflight check
 * shown in place. Closes once the server accepts the run.
 */
export function NewRunDrawer({ open, onClose, backendConfigPath }: { open: boolean; onClose: () => void; backendConfigPath: string }) {
  const { me } = useAuth();
  const queryClient = useQueryClient();
  // Group members may narrow the run to a subset of their allowed clusters.
  const clusterRestricted = me?.cluster_access_unrestricted === false;
  const allowedClusters = useMemo(() => me?.allowed_clusters ?? [], [me?.allowed_clusters]);
  const [selectedClusters, setSelectedClusters] = useState<string[]>([]);
  const [runConfigPath, setRunConfigPath] = useState("");
  const [runConfigOptions, setRunConfigOptions] = useState<ConfigListItem[]>([]);
  const [runPassword, setRunPassword] = useState("");
  const [extraArgs, setExtraArgs] = useLocalStorageState("runs.extraArgs", "");
  const [runOptionValues, setRunOptionValues] = useLocalStorageState<RunOptionValues>("runs.options", {});
  const [preflight, setPreflight] = useState<RunPreflightData | null>(null);
  const [busy, setBusy] = useState<"" | "start" | "preflight">("");
  const extraArgOptionsQuery = useQuery({ queryKey: ["runs", "extra-args"], queryFn: api.runExtraArgs, staleTime: 10 * 60_000, enabled: open });
  const extraArgOptions = useMemo(() => extraArgOptionsQuery.data?.options ?? [], [extraArgOptionsQuery.data]);
  const setRunOption = (flag: string, value: string | boolean | undefined) => setRunOptionValues((prev) => ({ ...prev, [flag]: value }));

  useEffect(() => {
    if (backendConfigPath && !runConfigPath) setRunConfigPath(backendConfigPath);
  }, [backendConfigPath, runConfigPath]);

  useEffect(() => {
    if (!open) return;
    let live = true;
    (async () => {
      try {
        // The per-user preference exists only for local accounts; on token-only
        // installs it 404s, so it must not fail the config list.
        const [cfgs, pref] = await Promise.all([
          api.runConfigs(),
          me?.is_local
            ? api.getRunConfigPreference().catch(() => ({ path: undefined }) as { path?: string })
            : Promise.resolve({ path: undefined } as { path?: string }),
        ]);
        if (!live) return;
        const items = cfgs.items ?? [];
        setRunConfigOptions(items);
        const fallbackPath = items.find((it) => it.is_active)?.path || backendConfigPath || "";
        const preferred = (pref.path || cfgs.default_path || me?.run_config_path || "").trim();
        const selected = preferred || fallbackPath;
        if (selected) setRunConfigPath(selected);
      } catch (e) {
        notifyError(e, "Could not load the config files");
      }
    })();
    return () => {
      live = false;
    };
    // Load once per opening.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const composedExtraArgs = useMemo(
    () => buildExtraArgs(extraArgOptions, runOptionValues, extraArgs, { restricted: clusterRestricted }),
    [extraArgOptions, runOptionValues, extraArgs, clusterRestricted],
  );

  const triggerRun = async () => {
    setBusy("start");
    notify.loading({ key: RUN_TOAST_KEY, message: "Starting run…", description: "Submitting the run to the API server." });
    try {
      const out = await api.runTrigger({
        config_path: runConfigPath || undefined,
        password: runPassword || undefined,
        extra_args: composedExtraArgs,
        // Only an explicit subset; empty runs the caller's full allowed set.
        clusters: clusterRestricted && selectedClusters.length > 0 ? selectedClusters : undefined,
      });
      notify.close(RUN_TOAST_KEY);
      const skipped = out.skipped_clusters ?? [];
      const skippedNote = skipped.length > 0 ? ` ${skipped.length} cluster(s) already running elsewhere were skipped: ${skipped.join(", ")}.` : "";
      if (out.queued) {
        notify.info({
          message: `Run queued (position ${out.queue_position ?? "?"})`,
          description: `All run slots are busy; it starts when one frees.${skippedNote}`,
          duration: 8,
        });
      } else if (out.started === false) {
        notify.info({
          message: "Already running",
          description: `Every requested cluster is already being checked by a run in progress.${skippedNote}`,
          duration: 8,
        });
      } else {
        const ran = out.clusters ?? [];
        notify.success({
          message: "Run started",
          description: `${ran.length > 0 ? `Checking ${ran.length} cluster(s).` : "Checking all clusters."}${skippedNote}`,
          duration: 6,
        });
      }
      setRunPassword("");
      await queryClient.invalidateQueries({ queryKey: RUNS_ACTIVE_QUERY_KEY });
      onClose();
    } catch (e) {
      notify.close(RUN_TOAST_KEY);
      notifyError(e, "Could not start the run");
    } finally {
      setBusy("");
    }
  };

  const runPreflight = async () => {
    setBusy("preflight");
    try {
      setPreflight(await api.runPreflight({ config_path: runConfigPath || undefined }));
    } catch (e) {
      notifyError(e, "Preflight failed to run");
    } finally {
      setBusy("");
    }
  };

  const onRunConfigChange = async (value: string) => {
    setRunConfigPath(value);
    setPreflight(null);
    try {
      await api.updateRunConfigPreference(value);
    } catch (e) {
      notifyError(e, "Could not remember the config choice");
    }
  };

  const field = (id: string, label: string, control: React.ReactNode, help?: string) => (
    <div className="ui-field">
      <label htmlFor={id} className="ui-field-label">
        {label}
      </label>
      {control}
      {help ? <span className="ui-field-help">{help}</span> : null}
    </div>
  );

  return (
    <Drawer
      title="New run"
      open={open}
      onClose={onClose}
      size={560}
      destroyOnHidden={false}
      footer={
        <div className="ui-drawer-footer">
          <Button icon={<FileSearchOutlined />} onClick={() => void runPreflight()} loading={busy === "preflight"}>
            Run preflight
          </Button>
          <Button type="primary" icon={<ThunderboltOutlined />} onClick={() => void triggerRun()} loading={busy === "start"}>
            Start run
          </Button>
        </div>
      }
    >
      <form
        className="new-run-form"
        onSubmit={(e) => {
          e.preventDefault();
          void triggerRun();
        }}
        autoComplete="off"
      >
        <Typography.Paragraph type="secondary">
          Runs NCC on the configured clusters. Clusters already being checked by another run are skipped. Preflight
          checks the config and connectivity without running NCC.
        </Typography.Paragraph>
        {clusterRestricted ? (
          <Alert
            type="info"
            showIcon
            title="Limited to your cluster groups"
            description={
              allowedClusters.length > 0
                ? `You can run ${allowedClusters.length} cluster(s): ${allowedClusters.join(", ")}.`
                : "You are not in any cluster group. Ask an administrator to add you before starting a run."
            }
          />
        ) : null}

        {field(
          "run-config-path",
          "Config file",
          <Select
            id="run-config-path"
            value={runConfigPath}
            onChange={(v) => void onRunConfigChange(v)}
            options={runConfigOptions.map((item) => ({ value: item.path, label: item.path }))}
            placeholder={backendConfigPath || "config.yaml"}
            showSearch
          />,
        )}
        {field(
          "run-password",
          "One-off password",
          <Input.Password
            id="run-password"
            name="run-password"
            value={runPassword}
            onChange={(e) => setRunPassword(e.target.value)}
            placeholder="Optional — overrides the config for this run"
            autoComplete="new-password"
          />,
        )}
        {clusterRestricted
          ? field(
              "run-clusters",
              "Clusters",
              <Select
                id="run-clusters"
                mode="multiple"
                allowClear
                placeholder="All clusters in your groups"
                value={selectedClusters}
                onChange={setSelectedClusters}
                options={allowedClusters.map((c) => ({ value: c, label: c }))}
              />,
            )
          : null}

        {extraArgOptions
          .filter((o) => !o.advanced)
          .map((o) =>
            o.takes_value ? (
              <div key={o.flag}>
                {field(
                  `run-opt-${o.flag}`,
                  o.label,
                  o.choices?.length ? (
                    <Select
                      id={`run-opt-${o.flag}`}
                      mode="multiple"
                      allowClear
                      placeholder="Configuration default"
                      value={String(runOptionValues[o.flag] ?? "")
                        .split(",")
                        .map((v) => v.trim())
                        .filter(Boolean)}
                      onChange={(v: string[]) => setRunOption(o.flag, v.join(",") || undefined)}
                      options={o.choices.map((c) => ({ value: c, label: c }))}
                    />
                  ) : (
                    <Input
                      id={`run-opt-${o.flag}`}
                      value={String(runOptionValues[o.flag] ?? "")}
                      onChange={(e) => setRunOption(o.flag, e.target.value)}
                      placeholder={o.example}
                      autoComplete="off"
                    />
                  ),
                  o.help,
                )}
              </div>
            ) : (
              <div className="ui-field ui-field-inline" key={o.flag}>
                <Switch
                  id={`run-opt-${o.flag}`}
                  size="small"
                  checked={runOptionValues[o.flag] === true}
                  onChange={(checked) => setRunOption(o.flag, checked)}
                  aria-label={o.label}
                  aria-describedby={`run-opt-${o.flag}-help`}
                />
                <label htmlFor={`run-opt-${o.flag}`}>
                  <span className="ui-field-label">{o.label}</span>
                  <span className="ui-field-help" id={`run-opt-${o.flag}-help`}>
                    {o.help}
                  </span>
                </label>
              </div>
            ),
          )}

        <Collapse
          size="small"
          items={[
            {
              key: "advanced",
              label: "Advanced options",
              children: (
                <Row gutter={[12, 12]}>
                  {extraArgOptions
                    .filter((o) => o.advanced && o.takes_value && !(o.admin_only && clusterRestricted))
                    .map((o) => (
                      <Col xs={24} key={o.flag}>
                        {field(
                          `run-opt-${o.flag}`,
                          o.label,
                          <Input
                            id={`run-opt-${o.flag}`}
                            value={String(runOptionValues[o.flag] ?? "")}
                            onChange={(e) => setRunOption(o.flag, e.target.value)}
                            placeholder={o.example}
                            autoComplete="off"
                          />,
                          o.help,
                        )}
                      </Col>
                    ))}
                  <Col xs={24}>
                    {field(
                      "run-extra-args",
                      "Additional flags",
                      <Input
                        id="run-extra-args"
                        name="extra-args"
                        value={extraArgs}
                        onChange={(e) => setExtraArgs(e.target.value)}
                        placeholder="Any allowed flag, e.g. --outputs csv,json"
                        autoComplete="off"
                      />,
                    )}
                  </Col>
                </Row>
              ),
            },
          ]}
        />

        <p className="ui-field-help">
          Options sent: <Typography.Text code>{describeExtraArgs(composedExtraArgs)}</Typography.Text>
        </p>
        <button type="submit" hidden aria-hidden="true" tabIndex={-1} />
      </form>

      {preflight ? (
        <section className="preflight-result" aria-label="Preflight result">
          <Alert
            type={preflight.ok ? "success" : "error"}
            showIcon
            title={preflight.ok ? "Preflight passed" : "Preflight found blocking problems"}
            description={`${preflight.failed} failed · ${preflight.warn} warning${preflight.warn === 1 ? "" : "s"} · ${preflight.config_path}`}
          />
          <ul className="preflight-checks">
            {preflight.checks.map((item) => (
              <li key={item.id}>
                <Space size={8} wrap>
                  <StatusBadge tone={item.status === "pass" ? "ok" : item.status === "warn" ? "warn" : "fail"}>
                    {item.status === "pass" ? "Pass" : item.status === "warn" ? "Warning" : "Fail"}
                  </StatusBadge>
                  <strong>{item.title}</strong>
                </Space>
                <p>{item.message}</p>
                {item.hint ? <p className="ui-field-help">Hint: {item.hint}</p> : null}
              </li>
            ))}
          </ul>
        </section>
      ) : null}
    </Drawer>
  );
}
