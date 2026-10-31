import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { AboutDialog } from "./AboutDialog";

describe("AboutDialog", () => {
  afterEach(cleanup);

  it("shows the build, the license and project links", () => {
    render(
      <AboutDialog
        open
        onClose={() => undefined}
        build={{ version: "2.3.0-abc123", buildDate: "2026-10-31", stream: "stable", goVersion: "go1.27.3", os: "linux", arch: "amd64" }}
      />,
    );
    expect(screen.getByRole("heading", { name: "NCC Orchestrator" })).toBeInTheDocument();
    expect(screen.getByText("v2.3.0")).toBeInTheDocument();
    expect(screen.getByText("2.3.0-abc123")).toBeInTheDocument();
    expect(screen.getByText("linux / amd64")).toBeInTheDocument();
    expect(screen.getByText(/MIT License\. Copyright \(c\) 2025 Prajwal Vernekar/)).toBeInTheDocument();
    for (const name of ["Project", "Releases", "Documentation"]) {
      const link = screen.getByRole("link", { name: new RegExp(name) });
      expect(link).toHaveAttribute("target", "_blank");
      expect(link.getAttribute("href")).toMatch(/^https:\/\/github\.com\/lTSPV75BRO\/Nutanix-ncc-orchestrator/);
    }
  });

  it("shows the full license text on request", () => {
    render(<AboutDialog open onClose={() => undefined} build={{}} />);
    expect(screen.queryByLabelText("MIT License text")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "View license" }));
    expect(screen.getByLabelText("MIT License text")).toHaveTextContent(/WITHOUT WARRANTY OF ANY KIND/);
    // Without health data, version and build date say so.
    expect(screen.getAllByText("Unknown")).toHaveLength(2);
  });
});
