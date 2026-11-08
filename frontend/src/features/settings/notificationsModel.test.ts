import { describe, expect, it } from "vitest";
import { alertSummary, channelErrors, channelStatusView, deliveryView, validateNotifications, withDefaults } from "./notificationsModel";

describe("notifications model", () => {
  it("shows the latest delivery outcome, not an older success", () => {
    expect(deliveryView(undefined).tone).toBe("none");
    expect(deliveryView({ last_attempt_at: "2026-11-03T10:00:00Z", last_success_at: "2026-11-03T10:00:00Z" })).toMatchObject({
      tone: "ok",
      at: "2026-11-03T10:00:00Z",
    });
    // Succeeded yesterday, failed today: the page used to say "Last success".
    expect(
      deliveryView({ last_attempt_at: "2026-11-03T10:00:00Z", last_success_at: "2026-11-02T10:00:00Z", last_error: "webhook status: 500" }),
    ).toMatchObject({ tone: "fail", error: "webhook status: 500" });
    expect(deliveryView({ last_attempt_at: "2026-11-03T10:00:00Z", last_error: "dial tcp: refused" }).tone).toBe("fail");
  });

  it("checks only the channels that are on", () => {
    expect(channelErrors({ slack: { enabled: false } }, "slack")).toEqual({});
    expect(channelErrors({ slack: { enabled: true } }, "slack")).toHaveProperty("webhook_url");
    expect(channelErrors({ webhook: { enabled: true, url: "/hooks/ncc" } }, "webhook")).toHaveProperty("url");
    expect(channelErrors({ webhook: { enabled: true, url: "https://example.com/x" } }, "webhook")).toEqual({});
    const email = channelErrors({ email: { enabled: true, smtp_host: "smtp", smtp_port: 587, from: "ncc", to: "a@example.com; b@example.com" } }, "email");
    expect(Object.keys(email).sort()).toEqual(["from", "to"]);
  });

  it("validates quiet hours and maintenance windows", () => {
    expect(validateNotifications({})).toBeNull();
    expect(validateNotifications({ quiet: { enabled: true, start: "22:00", end: "22:00" } })).toMatch(/same time/);
    expect(validateNotifications({ maintenance: [{ start: "", end: "" }] })).toMatch(/needs a start and end/);
    expect(validateNotifications({ maintenance: [{ start: "2026-11-04T03:00:00Z", end: "2026-11-04T01:00:00Z" }] })).toMatch(/ends before/);
  });

  it("fills defaults the server requires", () => {
    // Turning the digest on without touching the frequency used to fail to save.
    expect(withDefaults({ digest: { enabled: true } }).digest?.every).toBe("24h");
    expect(withDefaults({ quiet: { enabled: true, start: "22:00", end: "07:00" } }).quiet?.timezone).toBe("UTC");
  });

  it("summarises the alert toggles", () => {
    expect(alertSummary({ events: { run_failure: true, policy_violations: true } })).toBe("Failures and policy violations");
    expect(alertSummary({ events: {} })).toBe("No alerts");
  });
});

describe("channelStatusView", () => {
  it("shows a failing channel with its last success, error, tries and dead letters", () => {
    const v = channelStatusView(
      { channel: "webhook", enabled: true, state: "failing", last_attempt_at: "2026-11-08T10:00:00Z", last_success_at: "2026-11-07T08:00:00Z", last_error: "webhook status 503", attempts: 3, source: "runner", dead_letters: 2 },
      undefined,
      true,
    );
    expect(v).toMatchObject({ tone: "fail", label: "Failing", text: "Last delivered", at: "2026-11-07T08:00:00Z", error: "webhook status 503" });
    expect(v.details).toEqual([
      { label: "Last attempt", value: "2026-11-08T10:00:00Z" },
      { label: "Tries", value: "3 tries" },
      { label: "Last message", value: "Run report from the runner" },
      { label: "Undelivered", value: "2 messages saved" },
    ]);
  });

  it("warns when deliveries work but messages wait in the dead-letter folder", () => {
    const v = channelStatusView({ channel: "slack", enabled: true, state: "ok", last_success_at: "2026-11-08T10:00:00Z", attempts: 1, dead_letters: 1 });
    expect(v.tone).toBe("warn");
    expect(v.details).toContainEqual({ label: "Undelivered", value: "1 message saved" });
  });

  it("says never delivered when a channel has only failed, and nothing sent for a fresh one", () => {
    expect(channelStatusView({ channel: "email", enabled: true, state: "failing", last_attempt_at: "2026-11-08T10:00:00Z", last_error: "dial tcp: refused", attempts: 3, dead_letters: 0 }).text).toBe("Never delivered");
    expect(channelStatusView({ channel: "email", enabled: true, state: "never", dead_letters: 0 })).toMatchObject({ tone: "neutral", label: "Not sent yet", details: [] });
  });

  it("falls back to the server-only last delivery", () => {
    expect(channelStatusView(undefined, { last_attempt_at: "2026-11-08T10:00:00Z", last_success_at: "2026-11-07T10:00:00Z", last_error: "boom" })).toMatchObject({ tone: "fail", error: "boom" });
    expect(channelStatusView(undefined, { last_success_at: "2026-11-08T10:00:00Z" })).toMatchObject({ tone: "ok", at: "2026-11-08T10:00:00Z" });
    expect(channelStatusView(undefined, undefined).tone).toBe("neutral");
  });
});
