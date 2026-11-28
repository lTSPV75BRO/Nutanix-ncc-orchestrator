import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import type { ReportCacheInfo, ReportData } from "../../api/types";
import {
  REPORT_FAILED_POLL_MS,
  REPORT_REFRESHING_POLL_MS,
  ReportCacheNotice,
  reportRefetchInterval,
} from "./ReportCacheNotice";

afterEach(cleanup);

const builtAt = new Date(Date.now() - 2 * 60 * 60_000).toISOString();

function renderNotice(cache: ReportCacheInfo | undefined) {
  return render(<ReportCacheNotice cache={cache} />);
}

describe("ReportCacheNotice", () => {
  it("shows nothing when the report is current or the server predates the field", () => {
    const { container } = renderNotice({ state: "warm", built_at: builtAt });
    expect(container).toBeEmptyDOMElement();
    cleanup();
    expect(renderNotice(undefined).container).toBeEmptyDOMElement();
    cleanup();
    expect(renderNotice({ state: "cold" }).container).toBeEmptyDOMElement();
  });

  it("says a newer report is being prepared while refreshing or stale", () => {
    renderNotice({ state: "refreshing", built_at: builtAt, refresh_started_at: new Date().toISOString() });
    expect(screen.getByRole("status")).toHaveTextContent("Updating");
    expect(screen.getByText(/^Showing the report built 2h ago; a newer one is being prepared\.$/)).toBeInTheDocument();
    cleanup();
    renderNotice({ state: "stale", built_at: builtAt });
    expect(screen.getByText(/a newer one is being prepared/)).toBeInTheDocument();
  });

  it("gives the reason when the newest report could not be built", () => {
    renderNotice({ state: "failed", built_at: builtAt, last_error: "log directory unreadable" });
    expect(screen.getByRole("status")).toHaveTextContent("Out of date");
    expect(
      screen.getByText("Couldn't build the newest report: log directory unreadable. Showing the report built 2h ago."),
    ).toBeInTheDocument();
  });

  it("says when the report was read from the saved files", () => {
    renderNotice({ state: "failed", fallback: true });
    expect(screen.getByText(/showing the saved report files/)).toBeInTheDocument();
  });
});

describe("reportRefetchInterval", () => {
  const withCache = (cache?: ReportCacheInfo) => ({ cache }) as ReportData;

  it("polls while refreshing, slowly after a failure, and stops when warm", () => {
    expect(reportRefetchInterval(withCache({ state: "refreshing" }))).toBe(REPORT_REFRESHING_POLL_MS);
    expect(reportRefetchInterval(withCache({ state: "stale" }))).toBe(REPORT_REFRESHING_POLL_MS);
    expect(reportRefetchInterval(withCache({ state: "failed" }))).toBe(REPORT_FAILED_POLL_MS);
    expect(reportRefetchInterval(withCache({ state: "warm" }))).toBe(false);
    expect(reportRefetchInterval(withCache({ state: "failed", fallback: true }))).toBe(false);
    expect(reportRefetchInterval(withCache(undefined))).toBe(false);
    expect(reportRefetchInterval(undefined)).toBe(false);
  });
});
