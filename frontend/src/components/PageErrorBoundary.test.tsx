import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { PageErrorBoundary } from "./PageErrorBoundary";

function Broken(): never {
  throw new Error("report shape changed");
}

describe("PageErrorBoundary", () => {
  afterEach(cleanup);

  it("shows a recovery screen instead of a blank app", () => {
    const quiet = vi.spyOn(console, "error").mockImplementation(() => undefined);
    render(
      <PageErrorBoundary>
        <Broken />
      </PageErrorBoundary>,
    );
    expect(screen.getByRole("alert")).toHaveTextContent("This page could not be shown");
    expect(screen.getByText("report shape changed")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Reload" })).toBeInTheDocument();
    quiet.mockRestore();
  });

  it("renders the page when nothing fails", () => {
    render(
      <PageErrorBoundary>
        <p>All good</p>
      </PageErrorBoundary>,
    );
    expect(screen.getByText("All good")).toBeInTheDocument();
  });
});
