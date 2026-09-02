import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { ClusterTable } from "./ClusterTable";

const baseProps = {
  checksSnapshot: [],
  filterText: "",
  selectedClusters: [],
  clusterNameMap: {},
  severityFilters: [] as Array<"FAIL" | "WARN" | "ERR" | "INFO">,
  pcResolvedFilter: "No" as const,
  compareMode: "all" as const,
};

describe("ClusterTable alert sources", () => {
  afterEach(() => cleanup());

  it("renders the restored NCC table layout", () => {
    render(
      <ClusterTable
        {...baseProps}
        aggRows={[{ cluster: "ncc-a", check: "NCC finding", severity: "INFO" }]}
        pcAlerts={[{ cluster: "pc-a", title: "PC finding", severity: "FAIL", resolved: false }]}
        alertSource="NCC"
      />,
    );

    expect(screen.getByText("NCC finding")).toBeInTheDocument();
    expect(screen.queryByText("PC finding")).not.toBeInTheDocument();
    expect(screen.getByText("Alert")).toBeInTheDocument();
    expect(screen.getByText("Source")).toBeInTheDocument();
  });

  it("filters rows by the selected source", () => {
    render(
      <ClusterTable
        {...baseProps}
        aggRows={[{ cluster: "ncc-a", check: "NCC finding", severity: "INFO" }]}
        pcAlerts={[{ cluster: "pc-a", title: "PC finding", severity: "FAIL", resolved: false }]}
        alertSource="PC"
      />,
    );

    expect(screen.queryByText("NCC finding")).not.toBeInTheDocument();
    expect(screen.getByText("PC finding")).toBeInTheDocument();
    expect(screen.getByText("Title")).toBeInTheDocument();
    expect(screen.getByText("Entity Name")).toBeInTheDocument();
    expect(screen.getByText("Entity Type")).toBeInTheDocument();
    expect(screen.getByText("Last Occurred")).toBeInTheDocument();
    expect(screen.getByText("Status")).toBeInTheDocument();
    expect(screen.getByText("Impact Type")).toBeInTheDocument();
  });
});
