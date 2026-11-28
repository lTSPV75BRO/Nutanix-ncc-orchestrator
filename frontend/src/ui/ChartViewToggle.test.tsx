import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ChartViewToggle } from "./ChartViewToggle";

describe("ChartViewToggle", () => {
  it("names the chart and offers Chart and Table", () => {
    const onChange = vi.fn();
    render(<ChartViewToggle value="chart" onChange={onChange} what="Failures over recent runs" />);
    const group = screen.getByRole("radiogroup", { name: "Failures over recent runs: show as" });
    expect(group).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: /Chart/ })).toBeChecked();
    fireEvent.click(screen.getByRole("radio", { name: /Table/ }));
    expect(onChange).toHaveBeenCalledWith("table");
  });
});
