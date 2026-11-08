import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { AboutDialog } from "./AboutDialog";

const REPO_LICENSE = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "LICENSE");

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
    expect(screen.getByText("About NCC Orchestrator")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "NCC Orchestrator" })).toBeInTheDocument();
    expect(screen.getByText("v2.3.0")).toBeInTheDocument();
    expect(screen.getByText("2.3.0-abc123")).toBeInTheDocument();
    expect(screen.getByText("linux / amd64")).toBeInTheDocument();
    expect(screen.getByText(/MIT License\. Copyright \(c\) 2025-2026 Prajwal Vernekar/)).toBeInTheDocument();
    for (const name of ["Project", "Releases", "Documentation"]) {
      const link = screen.getByRole("link", { name: new RegExp(name) });
      expect(link).toHaveAttribute("target", "_blank");
      expect(link.getAttribute("href")).toMatch(/^https:\/\/github\.com\/lTSPV75BRO\/Nutanix-ncc-orchestrator/);
    }
  });

  it("closes from the Close button", () => {
    let closed = 0;
    render(<AboutDialog open onClose={() => (closed += 1)} build={{}} />);
    // The header ✕ and the footer button are both named "Close"; use the footer one.
    const footerClose = screen.getAllByRole("button", { name: "Close" }).find((b) => !b.classList.contains("ant-modal-close"));
    expect(footerClose).toBeDefined();
    fireEvent.click(footerClose!);
    expect(closed).toBe(1);
  });

  it("shows the full license text on request, identical to LICENSE", () => {
    render(<AboutDialog open onClose={() => undefined} build={{}} />);
    expect(screen.queryByLabelText("MIT License text")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "View license" }));
    const shown = screen.getByLabelText("MIT License text").textContent ?? "";
    const file = readFileSync(REPO_LICENSE, "utf8").replace(/\r\n/g, "\n").trim();
    expect(shown.trim()).toBe(file);
    // Without health data, version and build date say so.
    expect(screen.getAllByText("Unknown")).toHaveLength(2);
  });
});
