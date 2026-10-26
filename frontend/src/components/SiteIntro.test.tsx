import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { INTRO_EXIT_MS, INTRO_HOLD_MS, SiteIntro } from "./SiteIntro";

describe("opening title", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    window.sessionStorage.clear();
    window.localStorage.clear();
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it("shows the name, then clears into the page and is not shown again this session", async () => {
    render(<SiteIntro />);
    await act(async () => {
      await vi.dynamicImportSettled();
    });
    expect(screen.getByRole("img", { name: "NCC Orchestrator" })).toBeInTheDocument();
    expect(screen.getByText("Cluster healthcheck platform")).toBeInTheDocument();

    act(() => {
      vi.advanceTimersByTime(INTRO_HOLD_MS);
    });
    expect(screen.getByTestId("site-intro")).toHaveClass("is-leaving");
    act(() => {
      vi.advanceTimersByTime(INTRO_EXIT_MS);
    });
    expect(screen.queryByTestId("site-intro")).toBeNull();
    expect(window.sessionStorage.getItem("ncc.intro.seen")).toBe("1");

    cleanup();
    render(<SiteIntro />);
    expect(screen.queryByTestId("site-intro")).toBeNull();
  });

  it("can be skipped with the button or Escape", () => {
    render(<SiteIntro />);
    fireEvent.click(screen.getByRole("button", { name: "Skip" }));
    expect(screen.getByTestId("site-intro")).toHaveClass("is-leaving");

    cleanup();
    window.sessionStorage.clear();
    render(<SiteIntro />);
    fireEvent.keyDown(window, { key: "Escape" });
    expect(screen.getByTestId("site-intro")).toHaveClass("is-leaving");
  });

  it("is not shown when the OS asks for reduced motion", () => {
    const spy = vi.spyOn(window, "matchMedia").mockImplementation(
      (query: string) => ({ matches: query.includes("reduce"), media: query }) as MediaQueryList,
    );
    render(<SiteIntro />);
    expect(screen.queryByTestId("site-intro")).toBeNull();
    spy.mockRestore();
  });
});
