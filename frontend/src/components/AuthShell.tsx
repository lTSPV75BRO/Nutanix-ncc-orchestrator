import type { ReactNode } from "react";
import { ConfigProvider, theme as antdTheme } from "antd";
import { BarChartOutlined, CarryOutOutlined, ThunderboltOutlined } from "@ant-design/icons";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { WaveRibbon } from "./WaveRibbon";

const FEATURES: Array<{ icon: ReactNode; title: string; text: string }> = [
  {
    icon: <ThunderboltOutlined />,
    title: "Parallel Execution",
    text: "Run NCC checks across multiple clusters simultaneously.",
  },
  {
    icon: <BarChartOutlined />,
    title: "Intelligent Analysis",
    text: "Analyze NCC results and surface actionable findings.",
  },
  {
    icon: <CarryOutOutlined />,
    title: "Unified Operations",
    text: "Track findings and follow them to resolution in one place.",
  },
];

/**
 * Frame for the signed-out screens (sign in, forced password change): the
 * product story on the left over a flowing wave, the task on the right.
 * Always dark, whatever the app theme, so the antd controls inside get the
 * dark algorithm. Collapses to a single column on narrow screens.
 */
export function AuthShell({ children }: { children: ReactNode }) {
  const health = useQuery({ queryKey: ["health"], queryFn: api.health, staleTime: 15_000, retry: 0 });
  const version = (health.data as { version?: string } | undefined)?.version;

  return (
    <ConfigProvider
      theme={{
        algorithm: antdTheme.darkAlgorithm,
        token: {
          colorPrimary: "#8b5cf6",
          colorLink: "#38bdf8",
          colorBgContainer: "rgba(255, 255, 255, 0.03)",
          colorBorder: "rgba(167, 139, 250, 0.28)",
          borderRadiusLG: 12,
          controlHeightLG: 50,
        },
      }}
    >
      <div className="auth-shell">
        <aside className="auth-brand" aria-label="About NCC Orchestrator">
          <div className="auth-brand-dots" aria-hidden="true" />
          <WaveRibbon />
          <div className="auth-brand-head">
            <span className="auth-brand-logo">
              <img src="/logo.svg" alt="" />
            </span>
            <span className="auth-brand-name">NCC Orchestrator</span>
          </div>
          <div className="auth-hero">
            <h1 className="auth-hero-title">
              Automate Nutanix
              <br />
              Cluster <span className="auth-hero-accent">Health Checks</span>
            </h1>
            <p className="auth-hero-sub">
              Run NCC checks in parallel, collect and analyze results, and resolve issues faster.
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
            <span className="auth-pill">Powered by Nutanix APIs</span>
            {version ? <span className="auth-brand-version">v{version}</span> : null}
          </div>
        </aside>
        <main className="auth-main">
          <div className="auth-deco" aria-hidden="true">
            <span />
            <span />
          </div>
          <div className="auth-panel">{children}</div>
        </main>
      </div>
    </ConfigProvider>
  );
}
