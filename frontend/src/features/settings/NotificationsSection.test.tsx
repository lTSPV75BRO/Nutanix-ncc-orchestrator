import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import type { NotificationState, NotificationStatusResponse } from "../../api/types";
import { NotificationsSection } from "./NotificationsSection";

vi.mock("../../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn(), loading: vi.fn(), close: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

const saved: NotificationState = {
  enabled: true,
  events: { run_failure: true, policy_violations: true },
  webhook: { enabled: true, url: "https://example.com/hook" },
  slack: { enabled: false },
  email: { enabled: false, smtp_port: 587 },
  last_delivery: {
    webhook: { last_attempt_at: "2026-11-03T10:00:00Z", last_success_at: "2026-11-02T10:00:00Z", last_error: "webhook status: 500" },
  },
};

const sharedStatus: NotificationStatusResponse = {
  dead_letter_configured: true,
  channels: [
    { channel: "email", enabled: false, state: "never", dead_letters: 0 },
    {
      channel: "webhook",
      enabled: true,
      state: "failing",
      last_attempt_at: "2026-11-03T10:00:00Z",
      last_success_at: "2026-11-02T10:00:00Z",
      last_error: "webhook status 503",
      attempts: 3,
      source: "runner",
      dead_letters: 4,
    },
    { channel: "slack", enabled: false, state: "never", dead_letters: 0 },
  ],
};

function renderSection(status: NotificationStatusResponse | Error = sharedStatus) {
  vi.spyOn(api, "getNotifications").mockResolvedValue(saved);
  const statusSpy = vi.spyOn(api, "getNotificationStatus");
  if (status instanceof Error) statusSpy.mockRejectedValue(status);
  else statusSpy.mockResolvedValue(status);
  const test = vi.spyOn(api, "testNotification").mockResolvedValue({ channel: "all" } as Awaited<ReturnType<typeof api.testNotification>>);
  const update = vi.spyOn(api, "updateNotifications").mockImplementation(async (s) => s as Awaited<ReturnType<typeof api.updateNotifications>>);
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={qc}>
      <NotificationsSection />
    </QueryClientProvider>,
  );
  return { test, update, statusSpy };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("NotificationsSection", () => {
  it("summarises what is sent and shows the shared delivery status per channel", async () => {
    renderSection();
    expect(await screen.findByText("Sending to 1 channel")).toBeInTheDocument();
    expect(screen.getByText(/Failures and policy violations/)).toBeInTheDocument();
    const row = await screen.findByLabelText("Webhook delivery status");
    expect(await within(row).findByText("webhook status 503")).toBeInTheDocument();
    expect(within(row).getByText("Failing")).toBeInTheDocument();
    expect(within(row).getByText("3 tries")).toBeInTheDocument();
    expect(within(row).getByText("4 messages saved")).toBeInTheDocument();
    expect(within(row).getByText("Run report from the runner")).toBeInTheDocument();
    expect(within(row).getByText(/Last delivered/)).toBeInTheDocument();
  });

  it("falls back to the server's own last delivery when the shared status is unavailable", async () => {
    renderSection(new Error("forbidden"));
    const row = await screen.findByLabelText("Webhook delivery status");
    expect(await within(row).findByText("webhook status: 500")).toBeInTheDocument();
    expect(within(row).getByText("Failing")).toBeInTheDocument();
  });

  it("refreshes the delivery status after a test send", async () => {
    const { test, statusSpy } = renderSection();
    const first = await screen.findByLabelText("Webhook delivery status");
    await within(first).findByText("webhook status 503");
    statusSpy.mockResolvedValue({
      ...sharedStatus,
      channels: sharedStatus.channels.map((c) =>
        c.channel === "webhook"
          ? { ...c, state: "ok" as const, last_error: undefined, attempts: 1, source: "api", dead_letters: 0, last_success_at: new Date().toISOString() }
          : c,
      ),
    });
    fireEvent.click(screen.getByRole("button", { name: /Test all channels/ }));
    await waitFor(() => expect(test).toHaveBeenCalled());
    const row = screen.getByLabelText("Webhook delivery status");
    expect(await within(row).findByText("Delivering")).toBeInTheDocument();
    expect(within(row).queryByText("webhook status 503")).not.toBeInTheDocument();
    expect(statusSpy.mock.calls.length).toBeGreaterThan(1);
  });

  it("sends tests with the saved settings and blocks them while there are unsaved changes", async () => {
    const { test } = renderSection();
    await screen.findByText("Sending to 1 channel");
    fireEvent.click(screen.getByRole("button", { name: /Test all channels/ }));
    await waitFor(() => expect(test).toHaveBeenCalledWith("all", "test"));

    fireEvent.click(screen.getByRole("switch", { name: "Successful runs" }));
    expect(await screen.findByText("Notification settings have unsaved changes.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Test all channels/ })).toBeDisabled();
    expect(screen.getByRole("button", { name: /Send digest now/ })).toBeDisabled();
  });

  it("saves the digest with a default frequency", async () => {
    const { update } = renderSection();
    await screen.findByText("Sending to 1 channel");
    fireEvent.click(screen.getByRole("switch", { name: "Send a digest" }));
    fireEvent.click(await screen.findByRole("button", { name: "Save" }));
    await waitFor(() => expect(update).toHaveBeenCalled());
    expect(update.mock.calls[0][0].digest).toMatchObject({ enabled: true, every: "24h" });
  });

  it("blocks saving a channel that is missing its settings", async () => {
    renderSection();
    await screen.findByText("Sending to 1 channel");
    fireEvent.click(screen.getByRole("switch", { name: "Send to Slack" }));
    expect(await screen.findByText("Slack: Paste the Slack incoming-webhook URL.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();
  });
});
