import type { ReactNode } from "react";
import { Button } from "antd";
import { CopyOutlined, ReloadOutlined, StopOutlined } from "@ant-design/icons";
import { useBlocked, type BlockedInfo } from "../api/blocked";
import { notify } from "../notify";

function detailsText(info: BlockedInfo): string {
  const rows: [string, string | undefined][] = [
    ["Error code", info.code],
    ["Advisory", info.advisory],
    ["Severity", info.severity],
    ["Fixed in", info.fixedIn],
    ["Dashboard version", info.uiVersion],
    ["UI server version", info.uiServerVersion],
    ["Blocked entry", info.blockedVersion ? `${info.component || "ui"} ${info.blockedVersion}` : undefined],
    ["Request ID", info.requestId],
  ];
  return rows
    .filter(([, v]) => v)
    .map(([k, v]) => `${k}: ${v}`)
    .join("\n");
}

/**
 * Shown instead of the app when the API refuses this dashboard build because
 * the version is on its blocked list. Nothing else can work, so it says what
 * happened, which code to quote, and what to do.
 */
export function BlockedVersionScreen({ info }: { info: BlockedInfo }) {
  const rows: [string, string][] = [
    ["Error code", info.code],
    ...(info.advisory ? ([["Advisory", info.advisory]] as [string, string][]) : []),
    ...(info.severity ? ([["Severity", info.severity]] as [string, string][]) : []),
    ...(info.fixedIn ? ([["Fixed in", info.fixedIn]] as [string, string][]) : []),
    ["Dashboard version", info.uiVersion || "unknown"],
    ...(info.uiServerVersion ? ([["UI server version", info.uiServerVersion]] as [string, string][]) : []),
    ...(info.blockedVersion ? ([["Blocked entry", `${info.component || "ui"} ${info.blockedVersion}`]] as [string, string][]) : []),
    ...(info.requestId ? ([["Request ID", info.requestId]] as [string, string][]) : []),
  ];
  return (
    <main className="blocked-screen" role="alert" aria-labelledby="blocked-title">
      <div className="blocked-card">
        <span className="blocked-icon" aria-hidden="true">
          <StopOutlined />
        </span>
        <h1 id="blocked-title" className="blocked-title">
          This dashboard version is blocked
        </h1>
        <p className="blocked-reason">{info.message}</p>
        {info.remediation ? <p className="blocked-fix">{info.remediation}</p> : null}
        <dl className="blocked-facts">
          {rows.map(([k, v]) => (
            <div key={k}>
              <dt>{k}</dt>
              <dd className={k === "Error code" || k === "Request ID" ? "mono" : undefined}>{v}</dd>
            </div>
          ))}
        </dl>
        <div className="blocked-actions">
          <Button type="primary" icon={<ReloadOutlined />} onClick={() => window.location.reload()}>
            Reload
          </Button>
          <Button
            icon={<CopyOutlined />}
            onClick={() => {
              void navigator.clipboard.writeText(detailsText(info)).then(() => notify.success("Details copied."));
            }}
          >
            Copy details
          </Button>
        </div>
      </div>
    </main>
  );
}

/** Renders the blocked screen in place of the app once the API has refused this build. */
export function BlockedGate({ children }: { children: ReactNode }) {
  const info = useBlocked();
  if (info) return <BlockedVersionScreen info={info} />;
  return <>{children}</>;
}
