import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { Panel } from "./Panel";

afterEach(() => cleanup());

// Regression: panel titles were <h3> directly under the page's <h1>, which
// skips a level (axe heading-order, WCAG 1.3.1). They are <h2> now.
describe("Panel", () => {
  it("renders its title as a level-2 heading", () => {
    render(<Panel title="Recent runs">content</Panel>);
    expect(screen.getByRole("heading", { level: 2, name: "Recent runs" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { level: 3 })).not.toBeInTheDocument();
  });
});
