import { BgColorsOutlined, BarChartOutlined, DashboardOutlined, SettingOutlined } from "@ant-design/icons";
import { Button, Dropdown, Layout, Menu, Typography } from "antd";
import { Link, Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { DashboardPage } from "./pages/DashboardPage";
import { SettingsPage } from "./pages/SettingsPage";
import { InsightsPage } from "./pages/InsightsPage";
import { THEME_OPTIONS, useAppTheme, type AppThemeSelection } from "./theme";

const { Header, Content } = Layout;

export default function App() {
  const location = useLocation();
  const navigate = useNavigate();
  const { selectedTheme, setTheme } = useAppTheme();
  const current = location.pathname.startsWith("/settings")
    ? "/settings"
    : location.pathname.startsWith("/insights")
      ? "/insights"
      : "/";

  return (
    <Layout className="app-shell">
      <Header className="app-header" style={{ position: "sticky", top: 0, zIndex: 10, width: "100%" }}>
        <div className="header-row">
          <Typography.Title level={4} style={{ margin: 0 }}>
            <Link to="/" className="brand-link">
              <img src="/logo.svg" alt="NCC logo" className="brand-logo" />
              <span>NCC Orchestrator v2</span>
            </Link>
          </Typography.Title>
          <Menu
            mode="horizontal"
            selectedKeys={[current]}
            onClick={({ key }) => navigate(key)}
            items={[
              { key: "/", icon: <DashboardOutlined />, label: "Dashboard" },
              { key: "/settings", icon: <SettingOutlined />, label: "Settings" },
              { key: "/insights", icon: <BarChartOutlined />, label: "Insights" },
            ]}
            style={{ flex: 1, minWidth: 380, background: "transparent" }}
          />
          <Dropdown
            placement="bottomRight"
            trigger={["click"]}
            menu={{
              selectedKeys: [selectedTheme],
              items: THEME_OPTIONS.map((opt) => ({ key: opt.value, label: opt.label })),
              onClick: ({ key }) => setTheme(key as AppThemeSelection),
            }}
          >
            <Button
              aria-label="Theme menu"
              icon={<BgColorsOutlined />}
              title="Theme"
            />
          </Dropdown>
        </div>
      </Header>
      <Content className="app-content" style={{ padding: "16px", maxWidth: 1400, margin: "0 auto", width: "100%" }}>
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="/insights" element={<InsightsPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Content>
    </Layout>
  );
}
