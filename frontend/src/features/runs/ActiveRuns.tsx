import { useEffect, useMemo, useState } from "react";
import { Alert, Button, Collapse, Space, Switch, Tag, Tooltip, Typography } from "antd";
import { ClockCircleOutlined, StopOutlined } from "@ant-design/icons";
import { useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ActiveRunEntry } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { CodeEditor } from "../../components/CodeEditor";
import { RUNS_ACTIVE_QUERY_KEY, useRunsActive } from "../../hooks/useRunsActive";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { notify, notifyError } from "../../notify";
import { Panel, StatusBadge } from "../../ui";
import { formatDuration } from "../../utils/freshness";

const RUN_TOAST_KEY = "ncc-run-active";

/**
 * Runs in progress and queued, with live runner output and Cancel. Shows the
 * last failure when nothing is running; renders nothing when idle and clean.
 */
export function ActiveRuns() {
  const { canOperate } = useAuth();
  const queryClient = useQueryClient();
  const activeQuery = useRunsActive();
  const active = activeQuery.data;
  const runs: ActiveRunEntry[] = useMemo(() => active?.runs ?? [], [active?.runs]);
  const running = runs.filter((r) => r.status === "running");
  const maxConcurrent = active?.max_concurrent ?? 0;
  const [followTail, setFollowTail] = useLocalStorageState("runs.logs.followTail", true);
  const [jumpToLastSignal, setJumpToLastSignal] = useState(0);
  const [, setTick] = useState(0);

  // Advance elapsed counters every second between the 3 s polls.
  useEffect(() => {
    if (running.length === 0) return;
    const id = window.setInterval(() => setTick((n) => n + 1), 1000);
    return () => window.clearInterval(id);
  }, [running.length]);

  const cancelRun = async (id?: string) => {
    notify.loading({ key: RUN_TOAST_KEY, message: "Cancelling…", description: id ? `Asking run ${id} to stop.` : "Asking every active run to stop." });
    try {
      const out = await api.runCancel(id);
      notify.close(RUN_TOAST_KEY);
      notify.success({
        message: "Cancellation requested",
        description: id ? `Run ${out.run_id ?? id} will stop shortly.` : `${out.cancelled ?? 0} run(s) asked to stop.`,
        duration: 5,
      });
      await queryClient.invalidateQueries({ queryKey: RUNS_ACTIVE_QUERY_KEY });
    } catch (e) {
      notify.close(RUN_TOAST_KEY);
      notifyError(e, "Could not cancel");
    }
  };

  // Queue ETA: position p waits ~ceil(p / slots) average runs.
  const avg = active?.avg_run_duration_sec ?? 0;
  const etaFor = (position?: number) =>
    !position || position < 1 || avg <= 0 ? undefined : Math.ceil(position / Math.max(1, maxConcurrent || 1)) * avg;

  if (runs.length === 0) {
    return active?.last_error ? (
      <Alert type="error" showIcon title="The last run failed" description={active.last_error} />
    ) : null;
  }

  const item = (run: ActiveRunEntry) => {
    const external = run.source === "scheduled" || run.external === true;
    const eta = run.status === "queued" ? etaFor(run.queue_position) : undefined;
    const clusters = run.clusters ?? [];
    const skipped = run.skipped ?? [];
    return {
      key: run.id,
      label: (
        <Space size={8} wrap>
          {run.status === "running" ? (
            <StatusBadge tone="info">Running · {formatDuration(run.elapsed_sec)}</StatusBadge>
          ) : (
            <StatusBadge tone="neutral">{run.queue_position ? `Queued · #${run.queue_position}` : "Queued"}</StatusBadge>
          )}
          <Typography.Text code>{run.id}</Typography.Text>
          {external ? (
            <Tooltip title="Started by the schedule (systemd timer / cron), outside this server">
              <Tag icon={<ClockCircleOutlined />}>Scheduled</Tag>
            </Tooltip>
          ) : null}
          {run.group ? <Tag>{run.group}</Tag> : null}
          {eta !== undefined ? <span className="ui-field-help">~{formatDuration(eta)} wait</span> : null}
        </Space>
      ),
      // A scheduled/external run is not owned by this server and cannot be signalled here.
      extra:
        canOperate && !external ? (
          <Button
            danger
            size="small"
            icon={<StopOutlined />}
            onClick={(e) => {
              e.stopPropagation();
              void cancelRun(run.id);
            }}
          >
            Cancel
          </Button>
        ) : null,
      children: (
        <Space orientation="vertical" size={10} style={{ width: "100%" }}>
          <div className="ui-field-help">
            Clusters:{" "}
            {clusters.length > 0 ? clusters.join(", ") : run.all_clusters ? "all configured clusters" : "—"}
          </div>
          {skipped.length > 0 ? (
            <Alert
              type="info"
              showIcon
              title="Skipped — already being checked by another run"
              description={skipped
                .map((c) => (run.skipped_owner?.[c] ? `${c} (run ${run.skipped_owner[c]})` : c))
                .join(", ")}
            />
          ) : null}
          {run.status === "running" ? (
            <CodeEditor
              value={run.live_output || "Waiting for runner output…"}
              language="plaintext"
              readOnly
              height={260}
              autoRevealLastLine={followTail}
              jumpToLastSignal={jumpToLastSignal}
            />
          ) : (
            <Typography.Text type="secondary">Waiting for a free slot to start.</Typography.Text>
          )}
        </Space>
      ),
    };
  };

  return (
    <Panel
      title="In progress"
      description={`${running.length} running${maxConcurrent ? ` of ${maxConcurrent} slots` : ""}${
        runs.length > running.length ? ` · ${runs.length - running.length} queued` : ""
      }`}
      extra={
        <Space size={8} wrap>
          <label className="ui-field-inline">
            <Switch size="small" checked={followTail} onChange={setFollowTail} aria-label="Follow output" />
            <span>Follow output</span>
          </label>
          <Button size="small" onClick={() => setJumpToLastSignal((n) => n + 1)}>
            Jump to latest
          </Button>
          {canOperate && runs.length > 1 ? (
            <Button size="small" danger icon={<StopOutlined />} onClick={() => void cancelRun()}>
              Cancel all
            </Button>
          ) : null}
        </Space>
      }
    >
      <Collapse defaultActiveKey={running.map((r) => r.id)} items={runs.map(item)} />
    </Panel>
  );
}
