import { useState } from "react";
import { Alert } from "antd";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router";
import { api } from "../../api/client";

export const UPDATE_NOTICE_QUERY_KEY = ["update-notice"] as const;
const DISMISSED_KEY = "updateBanner.dismissed";

function readDismissed(): string {
  try {
    return window.localStorage.getItem(DISMISSED_KEY) ?? "";
  } catch {
    return "";
  }
}

function writeDismissed(version: string) {
  try {
    window.localStorage.setItem(DISMISSED_KEY, version);
  } catch {
    // Storage blocked: the banner just comes back on the next load.
  }
}

/**
 * "Version X is available", for admins. The API server looks the release up
 * at most once a day and answers from that result, so polling here is cheap.
 * Dismissing hides it until a newer version comes out.
 */
export function UpdateBanner() {
  const [dismissed, setDismissed] = useState(readDismissed);
  const q = useQuery({
    queryKey: UPDATE_NOTICE_QUERY_KEY,
    queryFn: api.updateNotice,
    staleTime: 30 * 60_000,
    // Until the server has a result, ask again soon; then hourly.
    refetchInterval: (query) => (query.state.data?.checked_at ? 60 * 60_000 : 2 * 60_000),
    refetchOnWindowFocus: false,
    retry: false,
  });
  const latest = q.data?.update_available ? q.data.latest_version : undefined;
  if (!latest || latest === dismissed) return null;
  return (
    <Alert
      className="ui-update-banner"
      type="info"
      showIcon
      role="status"
      title={
        <span>
          Version {latest} is available. You have {q.data?.current_version}.{" "}
          <Link to="/system/updates">Open Updates &amp; backups</Link>
        </span>
      }
      closable={{ "aria-label": `Dismiss the notice for version ${latest}` }}
      onClose={() => {
        writeDismissed(latest);
        setDismissed(latest);
      }}
    />
  );
}
