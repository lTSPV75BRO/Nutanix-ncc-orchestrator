import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import type { ClusterFreshness } from "../api/types";
import { asArray, asRecord, buildClusterNameMap, resolveClusterName, toNumber } from "../utils/report";

/** The report preview the Overview and Findings already load (same cache entry). */
export const REPORT_PREVIEW_LIMIT = 1500;

export type ClusterEntry = {
  address: string;
  name: string;
  /** From the newest run's per-cluster result, when it included the cluster. */
  inLatestRun: boolean;
  ok: boolean;
  error: string;
  errorClass: string;
  fail: number;
  warn: number;
  err: number;
  info: number;
  checks: number;
  score: number | null;
  aosVersion: string;
  nccVersion: string;
  freshness?: ClusterFreshness;
};

/**
 * GET /api/v1/freshness (each cluster's data age, overdue schedules): one
 * cache entry shared by every page.
 */
export function useFreshness(enabled = true) {
  return useQuery({
    queryKey: ["freshness"],
    queryFn: api.freshness,
    enabled,
    staleTime: 30_000,
    // Older API servers have no freshness endpoint; pages work without it.
    retry: false,
  });
}

/**
 * Every known cluster, keyed by address, with its display name, latest
 * result, versions and data age. Built from the report preview and
 * GET /api/v1/freshness, which other pages already load.
 */
export function useClusterDirectory() {
  const report = useQuery({
    queryKey: ["report-data", "preview", REPORT_PREVIEW_LIMIT],
    queryFn: () => api.reportDataWithPagination({ limit: REPORT_PREVIEW_LIMIT }),
    staleTime: 30_000,
  });
  const freshness = useFreshness();

  const directory = useMemo(() => {
    const data = report.data;
    const aggRows = Array.isArray(data?.agg_rows) ? data.agg_rows : [];
    const nameMap = data
      ? buildClusterNameMap({
          runSummary: data.run_summary,
          checksSnapshot: data.checks_snapshot,
          aggRows,
          drilldownDiff: data.drilldown_diff,
          flakyChecks: data.flaky_checks,
          sloDashboard: data.slo_dashboard,
          regressionSummary: data.regression_summary,
        })
      : {};
    const nameOf = (address: string) => {
      const n = resolveClusterName(address, nameMap);
      return n && n !== "-" ? n : address;
    };

    const versions = new Map<string, { aos: string; ncc: string }>();
    for (const raw of aggRows) {
      const r = asRecord(raw);
      const key = String(r.cluster || r.address || "").toLowerCase();
      if (!key || versions.has(key)) continue;
      const aos = String(r.clusterVersion || r.cluster_version || "");
      const ncc = String(r.nccVersion || r.ncc_version || "");
      if (aos || ncc) versions.set(key, { aos, ncc });
    }

    const byAddress = new Map<string, ClusterEntry>();
    const blank = (address: string): ClusterEntry => {
      const v = versions.get(address.toLowerCase());
      return {
        address,
        name: nameOf(address),
        inLatestRun: false,
        ok: true,
        error: "",
        errorClass: "",
        fail: 0,
        warn: 0,
        err: 0,
        info: 0,
        checks: 0,
        score: null,
        aosVersion: v?.aos ?? "",
        nccVersion: v?.ncc ?? "",
      };
    };
    for (const raw of asArray(asRecord(data?.run_summary).clusters)) {
      const c = asRecord(raw);
      const address = String(c.address || "").trim();
      if (!address) continue;
      byAddress.set(address.toLowerCase(), {
        ...blank(address),
        inLatestRun: true,
        ok: c.ok === undefined ? true : Boolean(c.ok),
        error: String(c.error || ""),
        errorClass: String(c.error_class || ""),
        fail: toNumber(c.fail_count),
        warn: toNumber(c.warn_count),
        err: toNumber(c.err_count),
        info: toNumber(c.info_count),
        checks: toNumber(c.checks_total),
        score: c.health_score === undefined ? null : toNumber(c.health_score),
      });
    }
    for (const f of freshness.data?.clusters ?? []) {
      const key = f.address.toLowerCase();
      const existing = byAddress.get(key);
      if (existing) {
        existing.freshness = f;
        continue;
      }
      // Not in the newest run (another environment's cluster): its own newest run.
      byAddress.set(key, {
        ...blank(f.address),
        ok: f.ok,
        error: f.error ?? "",
        errorClass: f.error_class ?? "",
        fail: f.fail_count,
        warn: f.warn_count,
        err: f.err_count,
        score: f.health_score ?? null,
        freshness: f,
      });
    }

    /** The entry for an address or a display name, case-insensitively. */
    const find = (key: string): ClusterEntry | undefined => {
      const k = key.trim().toLowerCase();
      if (!k) return undefined;
      return byAddress.get(k) ?? [...byAddress.values()].find((c) => c.name.toLowerCase() === k);
    };
    return { nameMap, nameOf, find, clusters: [...byAddress.values()] };
  }, [report.data, freshness.data]);

  return {
    ...directory,
    report,
    freshness,
    isLoading: report.isLoading,
  };
}
