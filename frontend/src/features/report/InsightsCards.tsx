import { asRecord, toNumber } from "../../utils/report";
import { Card, Col, Row, Statistic, Typography } from "antd";

type Props = {
  runSummary: unknown;
  aggRows?: unknown[];
  nccSummaryCounts?: Record<string, number>;
  healthSource?: string;
};

export function InsightsCards({ runSummary, aggRows, nccSummaryCounts, healthSource }: Props) {
  const summary = asRecord(runSummary);
  const rows = Array.isArray(aggRows) ? aggRows.map((r) => asRecord(r)) : [];
  const nccCounts = nccSummaryCounts || {};
  const nccTotalPlugins = toNumber(nccCounts.total_plugins, 0);
  // Prefer plugin-level totals from NCC summary logs when present.
  const checks = nccTotalPlugins > 0 ? nccTotalPlugins : toNumber(summary.total_checks, rows.length);
  const fails = toNumber(summary.total_fail, toNumber(nccCounts.fail, rows.filter((r) => String(r.severity || "").toUpperCase() === "FAIL").length));
  const warnings = toNumber(summary.total_warn, rows.filter((r) => String(r.severity || "").toUpperCase() === "WARN").length);
  const errs = toNumber(summary.total_err, toNumber(nccCounts.error, rows.filter((r) => String(r.severity || "").toUpperCase() === "ERR").length));
  const unknown = toNumber(summary.total_unknown, toNumber(nccCounts.unknown, rows.filter((r) => String(r.severity || "").toUpperCase() === "UNKNOWN").length));
  const derivedPass = Math.max(0, checks - fails - warnings - errs - unknown);
  const pass = toNumber(summary.total_pass, toNumber(nccCounts.pass, derivedPass));
  const rawPassRate = checks > 0 ? (pass / checks) * 100 : 0;
  // Aggressive weighted penalty so low fail/error counts still visibly affect health.
  const failRate = checks > 0 ? (fails / checks) * 100 : 0;
  const errRate = checks > 0 ? (errs / checks) * 100 : 0;
  const warnRate = checks > 0 ? (warnings / checks) * 100 : 0;
  const unknownRate = checks > 0 ? (unknown / checks) * 100 : 0;
  const weightedPenalty = failRate * 8.0 + errRate * 5.5 + warnRate * 2.2 + unknownRate * 3.0;
  const healthRate = Math.max(0, rawPassRate - weightedPenalty);
  const duration = toNumber(summary.duration_s);
  const cards = [
    { label: "Checks", value: checks },
    { label: "Fail", value: fails },
    { label: "Warn", value: warnings },
    { label: "Err", value: errs },
    { label: "Pass", value: pass },
    { label: "Unknown", value: unknown },
    { label: "Health Rate", value: `${healthRate.toFixed(2)}%` },
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
      case "Health Rate":
        return "#22c55e";
      default:
        return "#93c5fd";
    }
  };
  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Insights
      </Typography.Title>
      <Typography.Text type="secondary" className="section-subtitle">
        Health source: {healthSource || "fallback"} (pass + weighted shown)
      </Typography.Text>
      <Row gutter={[8, 8]}>
        {cards.map((c) => (
          <Col key={c.label} xs={12} md={6} lg={3}>
            <Card size="small" style={{ borderTop: `3px solid ${metricColor(c.label)}` }}>
              <Statistic
                title={c.label}
                value={c.value as string | number}
                styles={{ content: { color: metricColor(c.label), fontWeight: 700 } }}
              />
            </Card>
          </Col>
        ))}
      </Row>
    </Card>
  );
}
