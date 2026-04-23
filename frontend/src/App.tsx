import { BarChartOutlined, DashboardOutlined, SettingOutlined } from "@ant-design/icons";
import { Layout, Menu, Space, Typography } from "antd";
import { Link, Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { DashboardPage } from "./pages/DashboardPage";
import { SettingsPage } from "./pages/SettingsPage";
import { InsightsPage } from "./pages/InsightsPage";

const { Header, Content } = Layout;

export default function App() {
  const location = useLocation();
  const navigate = useNavigate();
  const current = location.pathname.startsWith("/settings")
    ? "/settings"
    : location.pathname.startsWith("/insights")
      ? "/insights"
      : "/";

  return (
    <Layout className="app-shell">
      <Header className="app-header" style={{ position: "sticky", top: 0, zIndex: 10, width: "100%" }}>
        <Space size={24} style={{ width: "100%" }}>
          <Typography.Title level={4} style={{ margin: 0 }}>
            <Link to="/" style={{ color: "inherit" }}>
              NCC Orchestrator v2
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
        </Space>
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
