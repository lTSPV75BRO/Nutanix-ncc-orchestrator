import type { FeatureFlags } from "../../api/types";

export type FeatureKey = keyof FeatureFlags;

export type { FeatureFlags };

export type FeatureSpec = {
  key: FeatureKey;
  title: string;
  saves: string;
  beta?: boolean;
};

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
];

export function featureSpec(key: FeatureKey): FeatureSpec {
  const found = FEATURE_CATALOG.find((item) => item.key === key);
  if (!found) {
    return { key, title: key, saves: "" };
  }
  return found;
}
