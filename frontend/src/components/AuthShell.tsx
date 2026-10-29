import type { ReactNode } from "react";
import { ConfigProvider } from "antd";
import { BarChartOutlined, CarryOutOutlined, ThunderboltOutlined } from "@ant-design/icons";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { WaveRibbon } from "./WaveRibbon";

const FEATURES: Array<{ icon: ReactNode; title: string; text: string }> = [
  {
    icon: <ThunderboltOutlined />,
    title: "Parallel execution",
    text: "Run NCC checks across multiple clusters simultaneously.",
  },
  {
    icon: <BarChartOutlined />,
    title: "Actionable analysis",
    text: "Turn NCC results into findings you can triage.",
  },
  {
    icon: <CarryOutOutlined />,
    title: "Unified operations",
    text: "Track findings through to resolution in one place.",
  },
];

/**
 * Frame for the signed-out screens (sign in, forced password change): the
 * product summary on the left over a quiet flowing wave, the task on the
 * right. Colours come from the app theme tokens, so it follows the selected
 * theme like every other page. Collapses to one column on narrow screens.
 */
export function AuthShell({ children }: { children: ReactNode }) {
  const health = useQuery({ queryKey: ["health"], queryFn: api.health, staleTime: 15_000, retry: 0 });
  const version = (health.data as { version?: string } | undefined)?.version;

  return (
    // Only sizing here; colours are inherited from the app's theme.
    <ConfigProvider theme={{ token: { controlHeightLG: 46 } }}>
      <div className="auth-shell">
        <aside className="auth-brand" aria-label="About NCC Orchestrator">
          <WaveRibbon />
          <div className="auth-brand-head">
            <img className="auth-brand-logo" src="/logo.svg" alt="" />
            <span className="auth-brand-name">NCC Orchestrator</span>
          </div>
          <div className="auth-hero">
            <h1 className="auth-hero-title">
              Automate Nutanix cluster <span className="auth-hero-accent">health checks</span>
            </h1>
            <p className="auth-hero-sub">
              Run NCC checks in parallel, review the results, and resolve issues faster.
            </p>
            <ul className="auth-features">
              {FEATURES.map((f) => (
                <li key={f.title}>
                  <span className="auth-feature-icon" aria-hidden="true">
                    {f.icon}
                  </span>
                  <span>
                    <span className="auth-feature-title">{f.title}</span>
                    <span className="auth-feature-text">{f.text}</span>
                  </span>
                </li>
              ))}
            </ul>
          </div>
          <div className="auth-brand-foot">
            Powered by Nutanix APIs{version ? <span className="auth-brand-version">v{version}</span> : null}
          </div>
        </aside>
        <main className="auth-main">
          <div className="auth-panel">{children}</div>
        </main>
      </div>
    </ConfigProvider>
  );
}
