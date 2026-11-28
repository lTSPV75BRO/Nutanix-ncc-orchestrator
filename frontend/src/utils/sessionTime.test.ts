import { describe, expect, it } from "vitest";
import { formatCountdown, sessionDeadline, sessionRemainingLabel } from "./sessionTime";

describe("formatCountdown", () => {
  it("formats waits as a clock", () => {
    expect(formatCountdown(42)).toBe("0:42");
    expect(formatCountdown(0)).toBe("0:00");
    expect(formatCountdown(-5)).toBe("0:00");
    expect(formatCountdown(845)).toBe("14:05");
    expect(formatCountdown(3723)).toBe("1:02:03");
    expect(formatCountdown(41.2)).toBe("0:42");
  });
});

describe("sessionRemainingLabel", () => {
  it("names the time left in plain words", () => {
    expect(sessionRemainingLabel((7 * 60 + 40) * 60_000 + 5_000)).toBe("Signed in for another 7 h 40 min");
    expect(sessionRemainingLabel(2 * 3_600_000)).toBe("Signed in for another 2 h");
    expect(sessionRemainingLabel(4 * 60_000 + 30_000)).toBe("Session ends in 4 min");
    expect(sessionRemainingLabel(59 * 60_000)).toBe("Session ends in 59 min");
    expect(sessionRemainingLabel(30_000)).toBe("Session ends in less than a minute");
    expect(sessionRemainingLabel(0)).toBe("Session has ended");
  });
});

describe("sessionDeadline", () => {
  it("prefers expires_in_sec measured from when /auth/me answered", () => {
    expect(sessionDeadline({ expires_in_sec: 60, expires_at: "2000-01-01T00:00:00Z" }, 1_000_000)).toBe(1_060_000);
  });
  it("falls back to expires_at, and to null without either", () => {
    expect(sessionDeadline({ expires_at: "2026-11-17T12:00:00Z" }, 0)).toBe(Date.parse("2026-11-17T12:00:00Z"));
    expect(sessionDeadline({ expires_at: "not a date" }, 0)).toBeNull();
    expect(sessionDeadline({}, 1)).toBeNull();
    expect(sessionDeadline(null, 1)).toBeNull();
  });
});
