import { Button, Space, Typography } from "antd";
import { ArrowLeftOutlined, CompassOutlined, HomeOutlined } from "@ant-design/icons";
import { Link, useLocation, useNavigate } from "react-router";

// Scoped styles for the 404 hero. Kept inline (like the login background) so the
// page is self-contained and reuses the app's brand gradient + theme tokens.
// The effect: drifting blurred orbs + a faint grid behind a glassy panel, with
// an animated gradient "404" that carries a subtle chromatic glitch.
const NOT_FOUND_CSS = `
.ncc-404 {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  text-align: center;
  min-height: 78vh;
  padding: 48px 16px;
  overflow: hidden;
}
.ncc-404-bg {
  position: absolute;
  inset: 0;
  z-index: 0;
  overflow: hidden;
  pointer-events: none;
}
.ncc-404-orb {
  position: absolute;
  border-radius: 50%;
  filter: blur(80px);
  opacity: 0.5;
  will-change: transform;
}
.ncc-404-orb-1 {
  width: 460px; height: 460px;
  top: -140px; left: -120px;
  background: radial-gradient(circle at 30% 30%, #4f8cff, #2b5cff 60%, transparent 70%);
  animation: ncc404OrbA 18s ease-in-out infinite;
}
.ncc-404-orb-2 {
  width: 520px; height: 520px;
  bottom: -180px; right: -140px;
  background: radial-gradient(circle at 70% 70%, #8b5cf6, #d946ef 60%, transparent 72%);
  animation: ncc404OrbB 22s ease-in-out infinite;
}
.ncc-404-orb-3 {
  width: 380px; height: 380px;
  top: 38%; left: 56%;
  background: radial-gradient(circle at 50% 50%, #22d3ee, #0ea5e9 60%, transparent 72%);
  animation: ncc404OrbC 26s ease-in-out infinite;
}
.ncc-404-grid {
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(to right, rgba(148, 163, 184, 0.22) 1px, transparent 1px),
    linear-gradient(to bottom, rgba(148, 163, 184, 0.22) 1px, transparent 1px);
  background-size: 44px 44px;
  -webkit-mask-image: radial-gradient(circle at 50% 45%, #000 0%, transparent 70%);
  mask-image: radial-gradient(circle at 50% 45%, #000 0%, transparent 70%);
  opacity: 0.5;
}
@keyframes ncc404OrbA {
  0%, 100% { transform: translate(0, 0) scale(1); }
  50% { transform: translate(60px, 40px) scale(1.12); }
}
@keyframes ncc404OrbB {
  0%, 100% { transform: translate(0, 0) scale(1); }
  50% { transform: translate(-70px, -50px) scale(1.08); }
}
@keyframes ncc404OrbC {
  0%, 100% { transform: translate(0, 0) scale(1); }
  50% { transform: translate(-50px, 60px) scale(1.15); }
}
.ncc-404-panel {
  position: relative;
  z-index: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 4px;
  padding: 40px clamp(20px, 5vw, 56px) 44px;
  border-radius: 28px;
  background: var(--card-bg, rgba(255, 255, 255, 0.06));
  border: 1px solid rgba(148, 163, 184, 0.22);
  box-shadow: 0 30px 80px -30px rgba(15, 23, 42, 0.55);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  max-width: 560px;
}
.ncc-404-badge {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 14px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 1.2px;
  text-transform: uppercase;
  color: var(--brand-version-color, #c4b5fd);
  background: var(--brand-version-bg, rgba(139, 92, 246, 0.16));
  border: 1px solid var(--brand-version-border, rgba(139, 92, 246, 0.45));
  margin-bottom: 14px;
}
.ncc-404-code {
  position: relative;
  font-weight: 900;
  font-size: clamp(120px, 28vw, 240px);
  line-height: 0.85;
  letter-spacing: -6px;
  background: linear-gradient(120deg, #c4b5fd, #67e8f9, #f0abfc, #c4b5fd);
  background-size: 300% 100%;
  -webkit-background-clip: text;
  background-clip: text;
  -webkit-text-fill-color: transparent;
  color: transparent;
  filter: drop-shadow(0 14px 44px rgba(99, 102, 241, 0.35));
  animation: ncc404Hue 9s linear infinite, ncc404Float 6s ease-in-out infinite;
}
/* Chromatic-glitch slices: two offset colored copies that flicker. */
.ncc-404-code::before,
.ncc-404-code::after {
  content: attr(data-text);
  position: absolute;
  left: 0;
  top: 0;
  width: 100%;
  background: none;
  mix-blend-mode: screen;
  pointer-events: none;
}
.ncc-404-code::before {
  -webkit-text-fill-color: #22d3ee;
  color: #22d3ee;
  animation: ncc404GlitchA 2.6s infinite steps(2, end);
}
.ncc-404-code::after {
  -webkit-text-fill-color: #f0abfc;
  color: #f0abfc;
  animation: ncc404GlitchB 3.1s infinite steps(2, end);
}
@keyframes ncc404Hue {
  to { background-position: 300% 0; }
}
@keyframes ncc404Float {
  0%, 100% { transform: translateY(0); }
  50% { transform: translateY(-12px); }
}
@keyframes ncc404GlitchA {
  0%, 100% { transform: translate(0, 0); clip-path: inset(0 0 0 0); opacity: 0; }
  6% { transform: translate(-4px, -2px); clip-path: inset(8% 0 62% 0); opacity: 0.7; }
  12% { transform: translate(3px, 2px); clip-path: inset(72% 0 8% 0); opacity: 0.7; }
  18% { transform: translate(0, 0); clip-path: inset(0 0 0 0); opacity: 0; }
}
@keyframes ncc404GlitchB {
  0%, 100% { transform: translate(0, 0); clip-path: inset(0 0 0 0); opacity: 0; }
  8% { transform: translate(4px, 2px); clip-path: inset(40% 0 38% 0); opacity: 0.7; }
  14% { transform: translate(-3px, -2px); clip-path: inset(20% 0 60% 0); opacity: 0.7; }
  22% { transform: translate(0, 0); clip-path: inset(0 0 0 0); opacity: 0; }
}
.ncc-404-path {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 13px;
  padding: 4px 10px;
  border-radius: 8px;
  background: var(--pre-bg, rgba(0, 0, 0, 0.06));
  border: 1px solid rgba(148, 163, 184, 0.25);
  word-break: break-all;
  max-width: 90vw;
}
@media (prefers-reduced-motion: reduce) {
  .ncc-404-orb,
  .ncc-404-code,
  .ncc-404-code::before,
  .ncc-404-code::after { animation: none !important; }
  .ncc-404-code::before,
  .ncc-404-code::after { opacity: 0; }
}
`;

export function NotFoundPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const canGoBack = window.history.length > 1;

  return (
    <div className="ncc-404">
      <style>{NOT_FOUND_CSS}</style>
      <div className="ncc-404-bg" aria-hidden="true">
        <span className="ncc-404-orb ncc-404-orb-1" />
        <span className="ncc-404-orb ncc-404-orb-2" />
        <span className="ncc-404-orb ncc-404-orb-3" />
        <div className="ncc-404-grid" />
      </div>

      <div className="ncc-404-panel">
        <span className="ncc-404-badge">
          <CompassOutlined />
          Error 404
        </span>
        <div className="ncc-404-code" data-text="404" aria-hidden>
          404
        </div>
        <Typography.Title level={2} style={{ marginTop: 12, marginBottom: 4 }}>
          Page not found
        </Typography.Title>
        <Typography.Paragraph type="secondary" style={{ maxWidth: 420, fontSize: 15 }}>
          The page you requested could not be found. It may have been moved, renamed, or no longer
          exists.
        </Typography.Paragraph>
        <Space align="center" wrap style={{ marginTop: 4 }}>
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            Requested path:
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
    </div>
  );
}
