import { CloudServerOutlined, DashboardOutlined } from "@ant-design/icons";

/**
 * Full-page loading indicator: three packets travel from a server to the
 * dashboard while data loads. Colours come from the theme tokens (accent,
 * border, tertiary text), so it follows dark, light and IT Pro. With reduced
 * motion the packets stay still at even spacing instead of moving.
 */
export function DataLoader({ label = "Loading" }: { label?: string }) {
  return (
    <div className="ui-data-loader" role="status" aria-live="polite">
      <CloudServerOutlined className="ui-data-loader-icon" aria-hidden />
      <span className="ui-data-loader-track" aria-hidden>
        <span className="ui-data-loader-packet" />
        <span className="ui-data-loader-packet" />
        <span className="ui-data-loader-packet" />
      </span>
      <DashboardOutlined className="ui-data-loader-icon" aria-hidden />
      <span className="sr-only">{label}</span>
    </div>
  );
}
