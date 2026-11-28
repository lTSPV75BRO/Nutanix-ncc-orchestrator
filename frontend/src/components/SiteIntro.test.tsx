import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { INTRO_EXIT_MS, INTRO_HOLD_MS, ON_KEY, SiteIntro } from "./SiteIntro";

const health = vi.hoisted(() => vi.fn());
vi.mock("../api/client", () => ({ api: { health } }));

describe("opening title", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    window.sessionStorage.clear();
    window.localStorage.clear();
    // An administrator turned the title on, and this browser saw that before.
    window.localStorage.setItem(ON_KEY, "1");
    health.mockReset();
    health.mockResolvedValue({ status: "ok", intro_title: true });
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
    expect(screen.getByText("NCC health checks for every cluster")).toBeInTheDocument();

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

  it("closes when the server says the opening title is off, and stays off next time", async () => {
    health.mockResolvedValue({ status: "ok", intro_title: false });
    render(<SiteIntro />);
    expect(screen.getByTestId("site-intro")).toBeInTheDocument();
    await act(async () => {
      await Promise.resolve();
    });
    expect(screen.queryByTestId("site-intro")).toBeNull();
    expect(window.localStorage.getItem(ON_KEY)).toBeNull();

    cleanup();
    render(<SiteIntro />);
    expect(screen.queryByTestId("site-intro")).toBeNull();
  });

  it("is off by default: nothing shows unless the server says it is on", async () => {
    window.localStorage.removeItem(ON_KEY);
    health.mockResolvedValue({ status: "ok" });
    render(<SiteIntro />);
    expect(screen.queryByTestId("site-intro")).toBeNull();
    await act(async () => {
      await Promise.resolve();
    });
    expect(screen.queryByTestId("site-intro")).toBeNull();
    expect(window.localStorage.getItem(ON_KEY)).toBeNull();
  });

  it("plays once the switch is turned on, from the first frame next time", async () => {
    window.localStorage.removeItem(ON_KEY);
    render(<SiteIntro />);
    expect(screen.queryByTestId("site-intro")).toBeNull();
    await act(async () => {
      await Promise.resolve();
    });
    expect(screen.getByTestId("site-intro")).toBeInTheDocument();
    expect(window.localStorage.getItem(ON_KEY)).toBe("1");

    cleanup();
    render(<SiteIntro />);
    expect(screen.getByTestId("site-intro")).toBeInTheDocument();
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
