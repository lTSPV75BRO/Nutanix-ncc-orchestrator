import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ErrorStateCard, EmptyStateCard } from "./UxStates";

describe("UX state cards", () => {
  it("renders an empty state and optional action", () => {
    render(
      <EmptyStateCard
        title="No runs yet"
        description="Start a run to populate this view."
        cta={<button type="button">Start run</button>}
      />,
    );
    expect(screen.getByText("No runs yet")).toBeInTheDocument();
    expect(screen.getByText("Start run")).toBeInTheDocument();
  });

  it("renders API failures and retries", async () => {
    const onRetry = vi.fn();
    render(<ErrorStateCard title="API unavailable" error="Connection refused" onRetry={onRetry} />);
    expect(screen.getByText("API unavailable")).toBeInTheDocument();
    expect(screen.getByText("Connection refused")).toBeInTheDocument();
    screen.getByRole("button", { name: /Retry/ }).click();
    expect(onRetry).toHaveBeenCalledOnce();
  });
});
