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
  { key: "views", title: "Views", description: "Optional pages and panels." },
  { key: "actions", title: "Actions", description: "Changes people can make from the app." },
  { key: "signin", title: "Sign-in", description: "What everyone sees before they sign in." },
];

export const FEATURE_CATALOG: FeatureSpec[] = [
  {
    key: "insights",
    title: "Insights",
    saves: "Hides Insights (trends and comparisons). Runs and checks carry on.",
  },
  {
    key: "pc_alerts",
    title: "Prism Central alerts",
    beta: true,
    saves: "Hides Prism Central alerts on Findings.",
  },
  {
    key: "pc_discovery",
    title: "Prism Central cluster discovery",
    beta: true,
    saves: "Stops looking up clusters in Prism Central for cluster groups.",
  },
  {
    key: "run_comparison",
    title: "Run-over-run comparison",
    saves: "Hides the comparison with the previous run.",
  },
  {
    key: "flaky_checks",
    title: "Flaky check list",
    saves: "Hides the list of checks that flip between pass and fail.",
  },
  {
    key: "slo",
    title: "SLO snapshot",
    saves: "Hides service-level health on Insights.",
  },
  {
    key: "ncc_log_index",
    title: "NCC log index",
    saves: "Hides the log list on each run. Logs stay available under System → Logs.",
  },
  {
    key: "alert_handling",
    title: "Alert handling",
    group: "actions",
    saves: "Removes Acknowledge, Resolve and Reopen from Findings. Earlier changes stay visible.",
  },
  {
    key: "self_heal",
    title: "Automatic healing",
    group: "actions",
    saves: "Removes the Fix buttons from Health & repair. Checks still run and report problems.",
  },
  {
    key: "update_checks",
    title: "Update checks",
    group: "actions",
    saves: "Stops checking for and installing new releases. Nothing is fetched from the release server.",
  },
  {
    key: "intro_title",
    title: "Opening title",
    group: "signin",
    saves: "Skips the animated title shown before sign-in.",
  },
];

export function featureSpec(key: FeatureKey): FeatureSpec {
  const found = FEATURE_CATALOG.find((item) => item.key === key);
  if (!found) {
    return { key, title: key, saves: "" };
  }
  return found;
}
