import { Tooltip } from "antd";
import type { ReportCacheInfo, ReportData } from "../../api/types";
import { StatusBadge, type Tone } from "../../ui";
import { formatDateTime, relativeTime } from "../../utils/datetime";

/** Poll every 5 s while a newer report is being built. */
export const REPORT_REFRESHING_POLL_MS = 5_000;
/** After a failed build, check again once a minute. */
export const REPORT_FAILED_POLL_MS = 60_000;

/**
 * refetchInterval for a report query: poll while the server builds a newer
 * report (refreshing or stale), slowly after a failed build, and stop once
 * the cache is warm.
 */
export function reportRefetchInterval(data: ReportData | undefined): number | false {
  const cache = data?.cache;
  if (!cache || cache.fallback) return false;
  if (cache.state === "refreshing" || cache.state === "stale") return REPORT_REFRESHING_POLL_MS;
  if (cache.state === "failed") return REPORT_FAILED_POLL_MS;
  return false;
}

type Notice = { tone: Tone; label: string; text: string };

function shownFrom(cache: ReportCacheInfo): string {
  return cache.built_at ? `the report built ${relativeTime(cache.built_at)}` : "the previous report";
}

/** What to tell the user about the cache, or null when there is nothing to say. */
export function reportCacheNotice(cache: ReportCacheInfo | undefined): Notice | null {
  if (!cache) return null;
  if (cache.fallback) {
    return {
      tone: "warn",
      label: "Saved report",
      text: "Couldn't load the report from the server; showing the saved report files.",
    };
  }
  switch (cache.state) {
    case "refreshing":
    case "stale":
      return {
        tone: "info",
        label: "Updating",
        text: `Showing ${shownFrom(cache)}; a newer one is being prepared.`,
      };
    case "failed": {
      const reason = (cache.last_error || "").trim();
      return {
        tone: "warn",
        label: "Out of date",
        text: `Couldn't build the newest report${reason ? `: ${reason}` : ""}. Showing ${shownFrom(cache)}.`,
      };
    }
    default:
      return null;
  }
}

/**
 * A quiet one-line notice near the page header when the report shown is not
 * the newest one (being rebuilt, failed to rebuild, or read from the saved
 * files). Renders nothing when the report is current.
 */
export function ReportCacheNotice({ cache }: { cache: ReportCacheInfo | undefined }) {
  const notice = reportCacheNotice(cache);
  if (!notice) return null;
  const builtAt = cache?.built_at ? formatDateTime(cache.built_at) : "";
  return (
    <div className="report-cache-notice" role="status">
      <StatusBadge tone={notice.tone}>{notice.label}</StatusBadge>
      {builtAt ? (
        <Tooltip title={`Built ${builtAt}`}>
          <span>{notice.text}</span>
        </Tooltip>
      ) : (
        <span>{notice.text}</span>
      )}
    </div>
  );
}
