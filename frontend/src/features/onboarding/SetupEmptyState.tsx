import type { ReactNode } from "react";
import { useLocation, useNavigate } from "react-router";
import { Button } from "antd";
import { ClusterOutlined, LockOutlined, ThunderboltOutlined, WarningOutlined } from "@ant-design/icons";
import type { MeData, OnboardingStatus } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { useRunsActive } from "../../hooks/useRunsActive";
import { sectionHref, type Viewer } from "../../navigation";
import { EmptyState } from "../../ui";
import { useOnboarding } from "./SetupChecklist";

// Operator-safe empty states: when a page has nothing to show because setup
// is not finished, say what is missing in one line and offer the next step
// as a button the viewer is allowed to use. The situation comes from the
// setup checklist (GET /api/v1/onboarding, operators and admins) and the
// caller's cluster scope (/auth/me); viewers get the parts they can act on.

export type SetupGap =
  /** A non-admin whose cluster groups contain no clusters. */
  | { kind: "restricted" }
  | { kind: "running" }
  /** The config file can't be read. */
  | { kind: "no_config"; detail: string }
  /** No clusters (or no sign-in for them) configured. */
  | { kind: "no_clusters"; detail: string }
  /** A run finished but reached no cluster. */
  | { kind: "first_run_failed"; detail: string }
  | { kind: "no_runs"; scheduleMissing: boolean };

/** Whether the caller is confined to cluster groups that hold no clusters. */
export function seesNoClusters(me: MeData | null, isAdmin: boolean): boolean {
  return !isAdmin && me?.cluster_access_unrestricted === false && (me.allowed_clusters ?? []).length === 0;
}

const open = (status: OnboardingStatus | undefined, id: string) => {
  const step = status?.steps.find((s) => s.id === id);
  return step && !step.done && !step.skipped ? step : undefined;
};

/** What is missing, most fundamental first. */
export function setupGap(input: { restricted: boolean; running: boolean; onboarding?: OnboardingStatus }): SetupGap {
  if (input.restricted) return { kind: "restricted" };
  if (input.running) return { kind: "running" };
  const clusters = open(input.onboarding, "clusters");
  if (clusters) {
    const detail = clusters.detail || "";
    if (/could not be read/i.test(detail)) return { kind: "no_config", detail };
    return { kind: "no_clusters", detail };
  }
  const firstRun = open(input.onboarding, "first_run");
  if (firstRun && firstRun.detail && !/^No completed run/i.test(firstRun.detail)) {
    return { kind: "first_run_failed", detail: firstRun.detail };
  }
  return { kind: "no_runs", scheduleMissing: Boolean(open(input.onboarding, "schedule")) };
}

export type EmptyAction = { label: string; to: string; primary?: boolean; startRun?: boolean };

export type EmptyCopy = { title: string; description: string; actions: EmptyAction[]; icon: "lock" | "cluster" | "run" | "warn" };

const RUNS = sectionHref("runs");
const CONFIG = sectionHref("config");
const SCHEDULE = sectionHref("schedule");

/** Text and the actions this viewer may take, for a gap. */
export function emptyCopy(gap: SetupGap, v: Pick<Viewer, "isAdmin" | "canOperate">): EmptyCopy {
  const operate = v.isAdmin || v.canOperate;
  const startRun: EmptyAction = { label: "Start a run", to: RUNS, primary: true, startRun: true };
  switch (gap.kind) {
    case "restricted":
      return {
        icon: "lock",
        title: "Your account sees no clusters",
        description: "Ask an administrator to add you to a cluster group",
        actions: [],
      };
    case "running":
      return {
        icon: "run",
        title: "The first run is in progress",
        description: "Results appear here as soon as it finishes",
        actions: [{ label: "Watch the run", to: RUNS }],
      };
    case "no_config":
      return {
        icon: "warn",
        title: "The configuration can't be read",
        description: v.isAdmin ? gap.detail : "Ask an administrator to fix it under Clusters & config.",
        actions: v.isAdmin ? [{ label: "Open Clusters & config", to: CONFIG, primary: true }] : [],
      };
    case "no_clusters":
      return {
        icon: "cluster",
        title: "No clusters are set up yet",
        description: v.isAdmin
          ? gap.detail || "Add the clusters to check, and how to sign in to them."
          : "Ask an administrator to add clusters under Clusters & config.",
        actions: v.isAdmin ? [{ label: "Add clusters", to: CONFIG, primary: true }] : [],
      };
    case "first_run_failed":
      return {
        icon: "warn",
        title: "The first run didn't reach any cluster",
        description: gap.detail,
        actions: [
          ...(operate ? [{ ...startRun, label: "Start a run again" }] : []),
          { label: "See the run", to: RUNS, primary: !operate },
          ...(v.isAdmin ? [{ label: "Check Clusters & config", to: CONFIG }] : []),
        ],
      };
    case "no_runs":
      return {
        icon: "run",
        title: "No results yet",
        description: operate
          ? "Run NCC across your clusters to see their health, findings and trends here."
          : "Results appear here after an operator starts a run or sets a schedule.",
        actions: operate
          ? [startRun, ...(gap.scheduleMissing ? [{ label: "Set a schedule", to: SCHEDULE }] : [])]
          : [],
      };
  }
}

const ICONS: Record<EmptyCopy["icon"], ReactNode> = {
  lock: <LockOutlined />,
  cluster: <ClusterOutlined />,
  run: <ThunderboltOutlined />,
  warn: <WarningOutlined />,
};

/** The setup situation for this viewer (onboarding is fetched for operators and admins only). */
export function useSetupGap(): SetupGap {
  const { me, isAdmin, canOperate } = useAuth();
  const onboarding = useOnboarding(isAdmin || canOperate);
  const active = useRunsActive();
  return setupGap({
    restricted: seesNoClusters(me, isAdmin),
    running: active.data?.active === true,
    onboarding: onboarding.data,
  });
}

/**
 * Empty state for a page with no results. `onStartRun` opens the new-run
 * drawer in place (the Runs page); elsewhere "Start a run" goes to Runs and
 * opens it there.
 */
export function SetupEmptyState({ onStartRun, gap: given }: { onStartRun?: () => void; gap?: SetupGap }) {
  const navigate = useNavigate();
  const { isAdmin, canOperate } = useAuth();
  const found = useSetupGap();
  const { pathname } = useLocation();
  const copy = emptyCopy(given ?? found, { isAdmin, canOperate });
  // A link to the page already open does nothing; leave it out.
  const actions = copy.actions.filter((a) => (a.startRun && onStartRun) || a.to !== pathname);
  const go = (a: EmptyAction) => {
    if (a.startRun && onStartRun) onStartRun();
    else navigate(a.to, a.startRun ? { state: { newRun: true } } : undefined);
  };
  return (
    <EmptyState
      icon={ICONS[copy.icon]}
      title={copy.title}
      description={copy.description}
      actions={
        actions.length ? (
          <>
            {actions.map((a) => (
              <Button key={a.label} type={a.primary ? "primary" : "default"} onClick={() => go(a)}>
                {a.label}
              </Button>
            ))}
          </>
        ) : undefined
      }
    />
  );
}
