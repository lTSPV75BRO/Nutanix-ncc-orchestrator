import { useEffect, useMemo, useState } from "react";
import { Alert, Button, Col, Collapse, Drawer, Input, Row, Select, Space, Switch, Typography } from "antd";
import { FileSearchOutlined, ThunderboltOutlined } from "@ant-design/icons";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { EnvironmentInfo, RunPreflightData } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { RUNS_ACTIVE_QUERY_KEY } from "../../hooks/useRunsActive";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { notify, notifyError } from "../../notify";
import { StatusBadge } from "../../ui";
import { buildExtraArgs, describeExtraArgs, type RunOptionValues } from "../../utils/runOptions";

const RUN_TOAST_KEY = "ncc-run-active";

/**
 * Start a run: environment (or the main config), optional one-off password,
 * cluster subset, run options from the server's allow-list, and a preflight
 * check shown in place. Closes once the server accepts the run.
 *
 * An environment only changes runner settings (which clusters, credentials,
 * timeouts ...); results, notifications and server settings always come from
 * the main config.
 */
export function NewRunDrawer({ open, onClose, backendConfigPath }: { open: boolean; onClose: () => void; backendConfigPath: string }) {
  const { me, isAdmin } = useAuth();
  const queryClient = useQueryClient();
  // Group members may narrow the run to a subset of their allowed clusters.
  const clusterRestricted = me?.cluster_access_unrestricted === false;
  const allowedClusters = useMemo(() => me?.allowed_clusters ?? [], [me?.allowed_clusters]);
  const [selectedClusters, setSelectedClusters] = useState<string[]>([]);
  const [lastEnvironment, setLastEnvironment] = useLocalStorageState("runs.environment", "");
  const [environment, setEnvironment] = useState<string | null>(null);
  const [runPassword, setRunPassword] = useState("");
  const [extraArgs, setExtraArgs] = useLocalStorageState("runs.extraArgs", "");
  const [runOptionValues, setRunOptionValues] = useLocalStorageState<RunOptionValues>("runs.options", {});
  const [preflight, setPreflight] = useState<RunPreflightData | null>(null);
  const [busy, setBusy] = useState<"" | "start" | "preflight">("");
  const extraArgOptionsQuery = useQuery({ queryKey: ["runs", "extra-args"], queryFn: api.runExtraArgs, staleTime: 10 * 60_000, enabled: open });
  const extraArgOptions = useMemo(() => extraArgOptionsQuery.data?.options ?? [], [extraArgOptionsQuery.data]);
  const setRunOption = (flag: string, value: string | boolean | undefined) => setRunOptionValues((prev) => ({ ...prev, [flag]: value }));

  const envQuery = useQuery({ queryKey: ["environments"], queryFn: api.listEnvironments, enabled: open, staleTime: 30_000 });
  const environments = useMemo(() => envQuery.data?.items ?? [], [envQuery.data]);
  const mainConfig = envQuery.data?.main_config || backendConfigPath || "config.yaml";
  // Admins pick from the main config's clusters; others from their groups.
  const inventoryQuery = useQuery({
    queryKey: ["settings", "clusters"],
    queryFn: api.getClusterInventory,
    enabled: open && isAdmin,
    staleTime: 60_000,
  });

  // The saved choice: the server preference for local accounts, else this browser's.
  useEffect(() => {
    if (!open || environment !== null) return;
    let live = true;
    (async () => {
      let pick = lastEnvironment;
      if (me?.is_local) {
        const pref = await api.getRunConfigPreference().catch(() => ({}) as { environment?: string });
        if (pref.environment) pick = pref.environment;
      }
      if (live) setEnvironment(pick);
    })();
    return () => {
      live = false;
    };
  }, [open, environment, lastEnvironment, me?.is_local]);

  const chosen: EnvironmentInfo | undefined = environments.find((e) => e.name === environment);
  // A remembered environment that was deleted falls back to the main config.
  const envName = chosen && !chosen.error ? chosen.name : "";

  const clusterOptions = useMemo(() => {
    let list: string[];
    if (chosen && (chosen.clusters?.length ?? 0) > 0) list = chosen.clusters ?? [];
    else if (clusterRestricted) list = allowedClusters;
    else list = isAdmin ? ((inventoryQuery.data?.clusters ?? []) as string[]) : [];
    if (clusterRestricted && chosen) {
      const allowed = new Set(allowedClusters.map((c) => c.toLowerCase()));
      list = list.filter((c) => allowed.has(c.toLowerCase()));
    }
    return list.map((c) => ({ value: c, label: c }));
  }, [chosen, clusterRestricted, allowedClusters, isAdmin, inventoryQuery.data]);

  const composedExtraArgs = useMemo(
    () => buildExtraArgs(extraArgOptions, runOptionValues, extraArgs, { restricted: clusterRestricted }),
    [extraArgOptions, runOptionValues, extraArgs, clusterRestricted],
  );

  const triggerRun = async () => {
    setBusy("start");
    notify.loading({ key: RUN_TOAST_KEY, message: "Starting run…", description: "Submitting the run to the API server." });
    try {
      const out = await api.runTrigger({
        environment: envName || undefined,
        password: runPassword || undefined,
        extra_args: composedExtraArgs,
        // Only an explicit subset; empty runs every cluster the choice covers.
        clusters: selectedClusters.length > 0 ? selectedClusters : undefined,
      });
      notify.close(RUN_TOAST_KEY);
      const skipped = out.skipped_clusters ?? [];
      const skippedNote =
        skipped.length > 0
          ? ` ${skipped.length} cluster(s) already running elsewhere were skipped: ${skipped.slice(0, 5).join(", ")}${skipped.length > 5 ? ` and ${skipped.length - 5} more` : ""}.`
          : "";
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
      notifyError(e, "Couldn't start the run");
    } finally {
      setBusy("");
    }
  };

  const runPreflight = async () => {
    setBusy("preflight");
    try {
      setPreflight(await api.runPreflight({ environment: envName || undefined }));
    } catch (e) {
      notifyError(e, "Preflight failed to run");
    } finally {
      setBusy("");
    }
  };

  const onEnvironmentChange = async (value: string) => {
    setEnvironment(value);
    setLastEnvironment(value);
    setSelectedClusters([]);
    setPreflight(null);
    if (!me?.is_local) return;
    try {
      await api.updateRunConfigPreference(value ? { environment: value } : { path: "" });
    } catch (e) {
      notifyError(e, "Couldn't remember the environment");
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
                ? `You can run ${allowedClusters.length.toLocaleString()} cluster(s): ${allowedClusters.slice(0, 5).join(", ")}${allowedClusters.length > 5 ? ` and ${(allowedClusters.length - 5).toLocaleString()} more` : ""}.`
                : "You are not in any cluster group. Ask an administrator to add you before starting a run."
            }
          />
        ) : null}

        {field(
          "run-environment",
          "Environment",
          <Select
            id="run-environment"
            value={envName}
            onChange={(v) => void onEnvironmentChange(v)}
            loading={envQuery.isLoading}
            optionLabelProp="title"
            options={[
              { value: "", title: "Main config", label: <EnvOption name="Main config" detail={mainConfig} /> },
              ...environments.map((e) => ({
                value: e.name,
                title: e.name,
                disabled: Boolean(e.error),
                label: (
                  <EnvOption
                    name={e.name}
                    detail={e.error ? e.error : [e.description, envTargets(e)].filter(Boolean).join(" · ")}
                  />
                ),
              })),
            ]}
          />,
          envName
            ? `Runner settings from ${chosen?.path} on top of the main config. Results, notifications and server settings stay as set in the main config.`
            : environments.length
              ? "Runs with the main config's runner settings."
              : "Add environments (dev, staging, prod …) in Clusters & config to run them separately.",
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
        {clusterOptions.length > 0 || clusterRestricted
          ? field(
              "run-clusters",
              "Clusters",
              <Select
                id="run-clusters"
                mode="multiple"
                allowClear
                placeholder={
                  clusterRestricted ? "All clusters in your groups" : envName ? `All clusters in ${envName}` : "All configured clusters"
                }
                maxTagCount="responsive"
                value={selectedClusters}
                onChange={setSelectedClusters}
                options={clusterOptions}
              />,
              clusterOptions.length > 0 ? `${clusterOptions.length.toLocaleString()} to choose from. Leave empty to run them all.` : undefined,
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

function envTargets(e: EnvironmentInfo): string {
  if (e.cluster_count > 0) return `${e.cluster_count.toLocaleString()} cluster${e.cluster_count === 1 ? "" : "s"}`;
  if (e.mode === "pc" || (e.prism_centrals?.length ?? 0) > 0) return "clusters from Prism Central";
  return "";
}

function EnvOption({ name, detail }: { name: string; detail?: string }) {
  return (
    <span className="env-option">
      <span className="env-option-name">{name}</span>
      {detail ? <span className="env-option-detail">{detail}</span> : null}
    </span>
  );
}
