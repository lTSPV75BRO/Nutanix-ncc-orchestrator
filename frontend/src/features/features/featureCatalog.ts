import type { FeatureFlags } from "../../api/types";

export type FeatureKey = keyof FeatureFlags;

export type { FeatureFlags };

export type FeatureGroup = "views" | "actions" | "signin";

export type FeatureSpec = {
  key: FeatureKey;
  title: string;
  saves: string;
  beta?: boolean;
  /** Where it is listed on Settings → Features; views when not set. */
  group?: FeatureGroup;
};

export const FEATURE_GROUPS: Array<{ key: FeatureGroup; title: string; description: string }> = [
  { key: "views", title: "Views", description: "Optional pages and panels. Health checks keep running." },
  { key: "actions", title: "Actions", description: "Things people can change from the app. Off makes them read-only." },
  { key: "signin", title: "Sign-in", description: "What everyone sees before they sign in." },
];

export const FEATURE_CATALOG: FeatureSpec[] = [
  {
    key: "insights",
    title: "Insights",
    saves: "Hides trends, comparisons, and the rest of Insights. Health checks keep running.",
  },
  {
    key: "pc_alerts",
    title: "Prism Central alerts",
    beta: true,
    saves: "Stops showing Prism Central alerts on the dashboard. The Beta switch stays in place.",
  },
  {
    key: "pc_discovery",
    title: "Prism Central cluster discovery",
    beta: true,
    saves: "Stops looking up clusters in Prism Central when you assign a cluster group.",
  },
  {
    key: "run_comparison",
    title: "Run-over-run comparison",
    saves: "Hides how this run compares with the previous one.",
  },
  {
    key: "flaky_checks",
    title: "Flaky check list",
    saves: "Hides checks that pass and fail across recent runs. The Flaky filter stays on the dashboard.",
  },
  {
    key: "slo",
    title: "SLO snapshot",
    saves: "Hides service-level health on Insights.",
  },
  {
    key: "ncc_log_index",
    title: "NCC log index",
    saves: "Hides the log list on each report. Individual logs stay available in Settings.",
  },
  {
    key: "alert_handling",
    title: "Alert handling",
    group: "actions",
    saves: "Removes Acknowledge, Resolve and Reopen from Findings. Earlier marks and their history stay visible.",
  },
  {
    key: "self_heal",
    title: "Self-heal actions",
    group: "actions",
    saves: "Removes the one-click fixes from System → Health. The checks still run and report problems.",
  },
  {
    key: "update_checks",
    title: "Update checks",
    group: "actions",
    saves: "Stops looking up new releases and installing them from Maintenance. Nothing is fetched from the release server.",
  },
  {
    key: "intro_title",
    title: "Opening title",
    group: "signin",
    saves: "Skips the animated NCC Orchestrator title shown before sign-in, for everyone.",
  },
];

export function featureSpec(key: FeatureKey): FeatureSpec {
  const found = FEATURE_CATALOG.find((item) => item.key === key);
  if (!found) {
    return { key, title: key, saves: "" };
  }
  return found;
}
