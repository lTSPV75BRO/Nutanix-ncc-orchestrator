import { Segmented } from "antd";
import { BarChartOutlined, TableOutlined } from "@ant-design/icons";

export type ChartView = "chart" | "table";

/**
 * Chart / Table switch for a Panel's `extra` slot. Every chart that shows more
 * than a label's worth of numbers offers its data as a table too (WCAG 1.1.1,
 * 1.4.1): the table is the text alternative, and some people simply read it
 * faster. `what` names the chart, e.g. "Failures over recent runs".
 */
export function ChartViewToggle({ value, onChange, what }: { value: ChartView; onChange: (v: ChartView) => void; what: string }) {
  return (
    <Segmented<ChartView>
      size="small"
      aria-label={`${what}: show as`}
      value={value}
      onChange={onChange}
      options={[
        { value: "chart", label: "Chart", icon: <BarChartOutlined aria-hidden /> },
        { value: "table", label: "Table", icon: <TableOutlined aria-hidden /> },
      ]}
    />
  );
}
