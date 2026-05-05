import { asRecord, toNumber } from "../../utils/report";
import { Card, Col, Row, Statistic, Typography } from "antd";

type Props = {
  runSummary: unknown;
  aggRows?: unknown[];
};

export function InsightsCards({ runSummary, aggRows }: Props) {
  const summary = asRecord(runSummary);
  const rows = Array.isArray(aggRows) ? aggRows.map((r) => asRecord(r)) : [];
  const checks = toNumber(summary.total_checks, rows.length);
  const fails = toNumber(summary.total_fail, rows.filter((r) => String(r.severity || "").toUpperCase() === "FAIL").length);
  const warnings = toNumber(summary.total_warn, rows.filter((r) => String(r.severity || "").toUpperCase() === "WARN").length);
  const errs = toNumber(summary.total_err, rows.filter((r) => String(r.severity || "").toUpperCase() === "ERR").length);
  const pass = toNumber(summary.total_pass);
  const unknown = toNumber(summary.total_unknown);
  const duration = toNumber(summary.duration_s);
  const cards = [
    { label: "Checks", value: checks },
    { label: "Fail", value: fails },
    { label: "Warn", value: warnings },
    { label: "Err", value: errs },
    { label: "Pass", value: pass },
    { label: "Unknown", value: unknown },
    { label: "Duration Sec", value: duration.toFixed(1) },
  ];

  const metricColor = (label: string): string => {
    switch (label) {
      case "Fail":
        return "#fb7185";
      case "Warn":
        return "#f59e0b";
      case "Err":
        return "#f97316";
      case "Pass":
        return "#34d399";
      case "Unknown":
        return "#a78bfa";
      case "Duration Sec":
        return "#38bdf8";
      default:
        return "#93c5fd";
    }
  };
  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Insights
      </Typography.Title>
      <Row gutter={[8, 8]}>
        {cards.map((c) => (
          <Col key={c.label} xs={12} md={6} lg={3}>
            <Card size="small" style={{ borderTop: `3px solid ${metricColor(c.label)}` }}>
              <Statistic title={c.label} value={c.value as string | number} valueStyle={{ color: metricColor(c.label), fontWeight: 700 }} />
            </Card>
          </Col>
        ))}
      </Row>
    </Card>
  );
}
