import { Button, Space, Typography } from "antd";
import { ArrowLeftOutlined, CompassOutlined, HomeOutlined } from "@ant-design/icons";
import { Link, useLocation, useNavigate } from "react-router-dom";

// Scoped styles for the 404 hero. Kept inline (like the login background) so the
// page is self-contained and reuses the app's brand gradient + theme tokens.
const NOT_FOUND_CSS = `
.ncc-404 {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  text-align: center;
  min-height: 60vh;
  padding: 32px 16px;
  gap: 4px;
}
.ncc-404-badge {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 14px;
  border-radius: 999px;
  font-size: 13px;
  font-weight: 600;
  letter-spacing: 0.3px;
  color: var(--brand-version-color, #c4b5fd);
  background: var(--brand-version-bg, rgba(139, 92, 246, 0.16));
  border: 1px solid var(--brand-version-border, rgba(139, 92, 246, 0.45));
  margin-bottom: 18px;
}
.ncc-404-code {
  font-weight: 800;
  font-size: clamp(96px, 22vw, 200px);
  line-height: 0.9;
  letter-spacing: -4px;
  background: var(--brand-name-grad, linear-gradient(90deg, #c4b5fd, #67e8f9));
  -webkit-background-clip: text;
  background-clip: text;
  color: transparent;
  filter: drop-shadow(0 12px 40px rgba(99, 102, 241, 0.25));
  animation: ncc404Float 6s ease-in-out infinite;
}
@keyframes ncc404Float {
  0%, 100% { transform: translateY(0); }
  50% { transform: translateY(-10px); }
}
.ncc-404-path {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 13px;
  padding: 4px 10px;
  border-radius: 8px;
  background: var(--pre-bg, rgba(0, 0, 0, 0.05));
  word-break: break-all;
  max-width: 90vw;
}
@media (prefers-reduced-motion: reduce) {
  .ncc-404-code { animation: none; }
}
`;

export function NotFoundPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const canGoBack = window.history.length > 1;

  return (
    <div className="ncc-404">
      <style>{NOT_FOUND_CSS}</style>
      <span className="ncc-404-badge">
        <CompassOutlined />
        Lost in the cluster
      </span>
      <div className="ncc-404-code" aria-hidden>
        404
      </div>
      <Typography.Title level={2} style={{ marginTop: 8, marginBottom: 4 }}>
        This page wandered off
      </Typography.Title>
      <Typography.Paragraph type="secondary" style={{ maxWidth: 460, fontSize: 15 }}>
        We couldn&apos;t find the page you were looking for. It may have been moved, renamed, or
        never existed. Let&apos;s get you back to a healthy endpoint.
      </Typography.Paragraph>
      <Space align="center" wrap style={{ marginTop: 4 }}>
        <Typography.Text type="secondary" style={{ fontSize: 13 }}>
          Requested:
        </Typography.Text>
        <span className="ncc-404-path">{location.pathname}</span>
      </Space>
      <Space size={12} wrap style={{ marginTop: 28 }}>
        <Link to="/">
          <Button type="primary" size="large" icon={<HomeOutlined />}>
            Back to dashboard
          </Button>
        </Link>
        <Button
          size="large"
          icon={<ArrowLeftOutlined />}
          onClick={() => (canGoBack ? navigate(-1) : navigate("/"))}
        >
          Go back
        </Button>
      </Space>
    </div>
  );
}
