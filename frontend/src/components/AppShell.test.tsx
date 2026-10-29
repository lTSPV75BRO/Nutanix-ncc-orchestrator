import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { AppShell } from "./AppShell";
import type { Viewer } from "../navigation";

function Where() {
  return <div data-testid="where">{useLocation().pathname}</div>;
}

function renderShell(viewer: Viewer, path = "/") {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <AppShell viewer={viewer} actions={<button type="button">Act</button>}>
        <Routes>
          <Route path="*" element={<Where />} />
        </Routes>
      </AppShell>
    </MemoryRouter>,
  );
}

const nav = () => screen.getByRole("complementary", { name: "Primary" });

describe("AppShell", () => {
  afterEach(() => {
    cleanup();
    window.localStorage.clear();
  });

  it("shows admins every group and titles the page", () => {
    renderShell({ isAdmin: true, canOperate: true, loginEnabled: true }, "/settings/config");
    expect(screen.getByRole("heading", { level: 1, name: "Clusters & config" })).toBeInTheDocument();
    for (const label of ["Overview", "Findings", "Insights", "Runs", "Clusters & config", "Access", "Audit log", "Developer"]) {
      expect(nav()).toHaveTextContent(label);
    }
    expect(screen.getByRole("button", { name: "Act" })).toBeInTheDocument();
  });

  it("shows viewers only the read-only pages", () => {
    renderShell({ isAdmin: false, canOperate: false, loginEnabled: true });
    expect(nav()).toHaveTextContent("Overview");
    expect(nav()).toHaveTextContent("Findings");
    expect(nav()).toHaveTextContent("Insights");
    expect(nav()).not.toHaveTextContent("Runs");
    expect(nav()).not.toHaveTextContent("Settings");
    expect(nav()).not.toHaveTextContent("System");
  });

  it("navigates from the sidebar", () => {
    renderShell({ isAdmin: false, canOperate: true, loginEnabled: true });
    fireEvent.click(screen.getByRole("menuitem", { name: /Audit log/ }));
    expect(screen.getByTestId("where")).toHaveTextContent("/system/audit");
  });

  it("toggles the sidebar and remembers the choice", () => {
    renderShell({ isAdmin: true, canOperate: true, loginEnabled: true });
    // jsdom reports a narrow screen, so it starts collapsed.
    expect(nav()).toHaveClass("is-collapsed");
    fireEvent.click(screen.getByRole("button", { name: "Expand sidebar" }));
    expect(nav()).not.toHaveClass("is-collapsed");
    expect(window.localStorage.getItem("ui.sidebar")).toContain("open");
    fireEvent.click(screen.getByRole("button", { name: "Collapse sidebar" }));
    expect(nav()).toHaveClass("is-collapsed");
  });
});
