import type { ReactNode } from "react";
import { Typography } from "antd";
import { AlertOutlined, ClusterOutlined, ScheduleOutlined } from "@ant-design/icons";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";

const HIGHLIGHTS: Array<{ icon: ReactNode; title: string; text: string }> = [
  {
    icon: <ClusterOutlined />,
    title: "Every cluster, one run",
    text: "Run Nutanix Cluster Check across all your clusters in parallel.",
  },
  {
    icon: <AlertOutlined />,
    title: "Findings you can work through",
    text: "Triage, acknowledge, and track NCC results over time.",
  },
  {
    icon: <ScheduleOutlined />,
    title: "Hands-off operations",
    text: "Scheduled runs, notifications, and backups.",
  },
];

/**
 * Two-pane frame for the signed-out screens (sign in, forced password
 * change): product context on the left, the task on the right. Collapses to a
 * single column on narrow screens. Static background — no animation to opt
 * out of.
 */
export function AuthShell({ children }: { children: ReactNode }) {
  const health = useQuery({ queryKey: ["health"], queryFn: api.health, staleTime: 15_000, retry: 0 });
  const version = (health.data as { version?: string } | undefined)?.version;

  return (
    <div className="auth-shell">
      <aside className="auth-brand" aria-label="About NCC Orchestrator">
        <div className="auth-brand-head">
          <span className="auth-brand-logo">
            <img src="/logo.svg" alt="" />
          </span>
          <div>
            <div className="auth-brand-name">NCC Orchestrator</div>
            <div className="auth-brand-tagline">Cluster healthcheck platform</div>
          </div>
        </div>
        <ul className="auth-highlights">
          {HIGHLIGHTS.map((h) => (
            <li key={h.title}>
              <span className="auth-highlight-icon" aria-hidden="true">
                {h.icon}
              </span>
              <span>
                <span className="auth-highlight-title">{h.title}</span>
                <span className="auth-highlight-text">{h.text}</span>
              </span>
            </li>
          ))}
        </ul>
        {version ? (
          <Typography.Text className="auth-brand-version">Version {version}</Typography.Text>
        ) : null}
      </aside>
      <main className="auth-main">
        <div className="auth-panel">{children}</div>
      </main>
    </div>
  );
}
