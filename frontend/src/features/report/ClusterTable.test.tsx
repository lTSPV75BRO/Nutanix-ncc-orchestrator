import { cleanup, fireEvent, render, screen } from "@testing-library/react";
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
  afterEach(() => {
    cleanup();
    try {
      window.localStorage?.clear();
    } catch {
      // jsdom in this environment does not always provide localStorage
    }
    document.documentElement.classList.remove("alerts-expanded");
  });

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
    expect(screen.queryByText("Source")).not.toBeInTheDocument();
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

  it("sizes the virtual alerts table from the host instead of a fixed 620px pane", () => {
    render(
      <ClusterTable
        {...baseProps}
        aggRows={[{ cluster: "ncc-a", check: "NCC finding", severity: "INFO" }]}
        alertSource="NCC"
      />,
    );

    const host = document.querySelector(".alerts-table-host");
    expect(host).toBeTruthy();
    const y = Number(host?.getAttribute("data-scroll-y"));
    expect(Number.isFinite(y)).toBe(true);
    expect(y).toBeGreaterThanOrEqual(240);
  });

  it("renders alert pagination outside the clipping table host", () => {
    render(
      <ClusterTable
        {...baseProps}
        aggRows={Array.from({ length: 120 }, (_, i) => ({
          cluster: `ncc-${i}`,
          check: `NCC finding ${i}`,
          severity: "INFO",
        }))}
        alertSource="NCC"
      />,
    );

    const pager = document.querySelector(".alerts-pagination");
    expect(pager).toBeTruthy();
    expect(pager?.closest(".alerts-table-host")).toBeNull();
    expect(screen.getByText(/1–100 of 120/)).toBeInTheDocument();
  });

  it("shows the next slice of alerts when the page changes", () => {
    render(
      <ClusterTable
        {...baseProps}
        aggRows={Array.from({ length: 120 }, (_, i) => ({
          cluster: `ncc-${i}`,
          check: `NCC finding ${i}`,
          severity: "INFO",
        }))}
        alertSource="NCC"
      />,
    );

    expect(screen.getByText("NCC finding 0")).toBeInTheDocument();
    expect(screen.queryByText("NCC finding 100")).not.toBeInTheDocument();

    fireEvent.click(screen.getByTitle("2"));

    expect(screen.queryByText("NCC finding 0")).not.toBeInTheDocument();
    expect(screen.getByText("NCC finding 100")).toBeInTheDocument();
    expect(screen.getByText(/101–120 of 120/)).toBeInTheDocument();
  });

  it("shows a richer inspector when a PC alert is expanded", () => {
    render(
      <ClusterTable
        {...baseProps}
        alertSource="PC"
        clusterNameMap={{ "10.1.2.3": "lab-a" }}
        pcAlerts={[
          {
            cluster: "lab-a",
            cluster_ip: "10.1.2.3",
            cluster_uuid: "aaaa-bbbb",
            title: "Disk almost full",
            severity: "FAIL",
            detail: "Capacity is high",
            root_cause: "CVM disk /home is 92% full",
            entity_name: "node-1",
            entity_type: "HOST",
            impact_type: "STORAGE",
            alert_type: "DiskUsageHigh",
            service_name: "Stargate",
            ext_id: "abc-123",
            created_at: "2026-09-01T10:00:00Z",
            last_occurred: "2026-09-01T11:00:00Z",
            resolved: false,
            acknowledged: false,
            kb_articles: ["https://portal.nutanix.com/kb/1234"],
          },
        ]}
      />,
    );

    fireEvent.click(screen.getByText("Disk almost full"));
    expect(screen.getByText("Alert message")).toBeInTheDocument();
    expect(screen.getByText("Capacity is high")).toBeInTheDocument();
    expect(screen.getByText("Root cause")).toBeInTheDocument();
    expect(screen.getByText("CVM disk /home is 92% full")).toBeInTheDocument();
    expect(screen.getByText("Timeline")).toBeInTheDocument();
    expect(screen.getByText("Stargate")).toBeInTheDocument();
    expect(screen.getAllByText("KB 1234").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Open Prism").length).toBeGreaterThan(0);
  });

  it("fills the page under the header and keeps the alerts toolbar", () => {
    render(
      <ClusterTable
        {...baseProps}
        aggRows={[{ cluster: "ncc-a", check: "NCC finding", severity: "FAIL" }]}
        alertSource="NCC"
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Expand alerts" }));
    expect(document.documentElement.classList.contains("alerts-expanded")).toBe(true);
    expect(screen.getByText("Alerts")).toBeInTheDocument();
    expect(screen.getByText("Needs attention")).toBeInTheDocument();
    expect(screen.getByText("NCC finding")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Exit full page" }));
    expect(document.documentElement.classList.contains("alerts-expanded")).toBe(false);
  });

  it("shows every matching alert in full page instead of a paged slice", () => {
    render(
      <ClusterTable
        {...baseProps}
        aggRows={Array.from({ length: 120 }, (_, i) => ({
          cluster: `ncc-${i}`,
          check: `NCC finding ${i}`,
          severity: "INFO",
        }))}
        alertSource="NCC"
      />,
    );

    expect(screen.queryByText("NCC finding 100")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Expand alerts" }));
    expect(screen.getByText(/Showing all 120 alerts/)).toBeInTheDocument();
    expect(document.querySelector(".alerts-pagination")).toBeNull();
  });

  it("filters NCC rows by handling and opens a tidy details panel", () => {
    render(
      <ClusterTable
        {...baseProps}
        alertSource="NCC"
        dispositions={[
          { cluster: "ncc-a", check: "Handled check", status: "resolved", by: "alice", at: "2026-10-05T10:00:00Z" },
        ]}
        aggRows={[
          { cluster: "ncc-a", check: "Handled check", severity: "FAIL", detail: "disk detail" },
          { cluster: "ncc-b", check: "Open check", severity: "ERR" },
        ]}
      />,
    );

    fireEvent.click(screen.getByRole("radio", { name: /Resolved/ }));
    expect(screen.getByText("Handled check")).toBeInTheDocument();
    expect(screen.queryByText("Open check")).not.toBeInTheDocument();

    fireEvent.click(screen.getByText("Handled check"));
    expect(screen.getByPlaceholderText("Optional note")).toBeInTheDocument();
    expect(screen.getAllByText("disk detail").length).toBeGreaterThan(1);
    expect(screen.getByRole("button", { name: "Resolve" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Resolved" })).not.toBeInTheDocument();
  });
});
