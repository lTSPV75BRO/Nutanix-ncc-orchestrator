import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SessionIdleGuard, SessionTimeLeft } from "./SessionIdleGuard";

type Reply = { status: number; body: unknown };

function mockFetch(route: (url: string) => Reply | undefined) {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const r = route(String(input)) ?? { status: 200, body: { success: true, data: {} } };
    return {
      ok: r.status >= 200 && r.status < 300,
      status: r.status,
      statusText: "",
      headers: new Headers({ "content-type": "application/json" }),
      json: async () => r.body,
      text: async () => JSON.stringify(r.body),
    } as Response;
  }) as unknown as typeof fetch;
}

const MIN = 60_000;

// Fake only the clock and intervals: promises and testing-library's waitFor
// keep using real timeouts.
beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
  vi.setSystemTime(new Date("2026-11-17T10:00:00Z"));
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const advance = (ms: number) =>
  act(() => {
    vi.advanceTimersByTime(ms);
  });

describe("SessionTimeLeft", () => {
  it("shows the time left and keeps it current", () => {
    render(<SessionTimeLeft expiresAt={Date.now() + (7 * 60 + 40) * MIN + 30_000} />);
    expect(screen.getByText("Signed in for another 7 h 40 min")).toBeInTheDocument();
    advance(7 * 60 * MIN + 36 * MIN);
    expect(screen.getByText("Session ends in 4 min")).toBeInTheDocument();
  });
});

describe("SessionIdleGuard session-end notice", () => {
  it("warns a few minutes before the session ends and extends it on Stay signed in", async () => {
    let refreshed = 0;
    mockFetch((url) => {
      if (url.includes("/auth/refresh")) {
        refreshed++;
        return { status: 200, body: { success: true, data: { expires_at: "2026-11-17T18:00:00Z", ttl_sec: 28800 } } };
      }
      return undefined;
    });
    const onStay = vi.fn();
    const onLogout = vi.fn();
    render(<SessionIdleGuard onLogout={onLogout} onStayLoggedIn={onStay} expiresAt={Date.now() + 10 * MIN} />);
    expect(screen.queryByText("Your session is ending")).not.toBeInTheDocument();

    advance(5 * MIN + 1_000);
    expect(screen.getByText("Your session is ending")).toBeInTheDocument();
    expect(screen.getByRole("timer")).toHaveTextContent("4:59");
    advance(17_000);
    expect(screen.getByRole("timer")).toHaveTextContent("4:42");

    fireEvent.click(screen.getByRole("button", { name: /Stay signed in/ }));
    await waitFor(() => expect(onStay).toHaveBeenCalledTimes(1));
    expect(refreshed).toBe(1);
    expect(onLogout).not.toHaveBeenCalled();
  });

  it("offers Sign in again when the session cannot be extended", async () => {
    mockFetch((url) =>
      url.includes("/auth/refresh") ? { status: 400, body: { success: false, error: "no refreshable session" } } : undefined,
    );
    const onLogout = vi.fn();
    render(<SessionIdleGuard onLogout={onLogout} expiresAt={Date.now() + 2 * MIN} />);
    fireEvent.click(screen.getByRole("button", { name: /Stay signed in/ }));
    const again = await screen.findByRole("button", { name: /Sign in again/ });
    expect(screen.getByText(/can't be extended/)).toBeInTheDocument();
    fireEvent.click(again);
    expect(onLogout).toHaveBeenCalledTimes(1);
  });

  it("stays dismissed for the same session end and hides once it has passed", () => {
    mockFetch(() => undefined);
    const end = Date.now() + 3 * MIN;
    const { rerender } = render(<SessionIdleGuard onLogout={vi.fn()} expiresAt={end} />);
    fireEvent.click(screen.getByRole("button", { name: /close/i }));
    expect(screen.queryByText("Your session is ending")).not.toBeInTheDocument();
    // A later /auth/me poll re-measures the deadline a few seconds off.
    rerender(<SessionIdleGuard onLogout={vi.fn()} expiresAt={end + 3_000} />);
    expect(screen.queryByText("Your session is ending")).not.toBeInTheDocument();
    // A new session end (after a refresh elsewhere) warns again when it nears.
    rerender(<SessionIdleGuard onLogout={vi.fn()} expiresAt={Date.now() + 4.5 * MIN} />);
    expect(screen.getByText("Your session is ending")).toBeInTheDocument();
    advance(4.5 * MIN + 1_000);
    expect(screen.queryByText("Your session is ending")).not.toBeInTheDocument();
  });

  it("shows no notice without a known session end", () => {
    render(<SessionIdleGuard onLogout={vi.fn()} expiresAt={null} />);
    advance(10 * MIN);
    expect(screen.queryByText("Your session is ending")).not.toBeInTheDocument();
  });
});
