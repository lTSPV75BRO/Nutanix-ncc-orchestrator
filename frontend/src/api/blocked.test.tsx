import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BlockedGate } from "../components/BlockedVersionScreen";
import { clearBlocked, inspectForBlock, UI_VERSION_BLOCKED_CODE } from "./blocked";

vi.mock("../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

function refusal(status: number, body: unknown): Response {
  return {
    status,
    headers: new Headers({ "x-request-id": "hdr-id" }),
    clone() {
      return { json: async () => body } as unknown as Response;
    },
  } as unknown as Response;
}

afterEach(() => {
  cleanup();
  clearBlocked();
});

describe("blocked dashboard version", () => {
  it("replaces the app with an error screen that names the code, advisory and fix", async () => {
    render(
      <BlockedGate>
        <p>the app</p>
      </BlockedGate>,
    );
    expect(screen.getByText("the app")).toBeInTheDocument();

    await act(async () => {
      await inspectForBlock(
        refusal(403, {
          success: false,
          error: "NCC 2.1.0 ships a known vulnerability; upgrade to 2.4.0 or later",
          error_code: UI_VERSION_BLOCKED_CODE,
          remediation: "Upgrade ncc-ui-server and frontend-dist.",
          request_id: "abc123",
          data: { ui_version: "2.1.0", blocked_version: "2.1.0", component: "any", advisory: "GO-2026-6603", severity: "medium", fixed_in: "2.4.0" },
        }),
      );
    });

    expect(screen.queryByText("the app")).not.toBeInTheDocument();
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "This dashboard version is blocked" })).toBeInTheDocument();
    expect(screen.getByText(UI_VERSION_BLOCKED_CODE)).toBeInTheDocument();
    expect(screen.getByText("GO-2026-6603")).toBeInTheDocument();
    expect(screen.getByText("abc123")).toBeInTheDocument();
    expect(screen.getByText("Upgrade ncc-ui-server and frontend-dist.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Reload/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Copy details/ })).toBeInTheDocument();
  });

  it("ignores ordinary 403s and other errors", async () => {
    render(
      <BlockedGate>
        <p>the app</p>
      </BlockedGate>,
    );
    await act(async () => {
      await inspectForBlock(refusal(403, { success: false, error: "forbidden", error_code: "NCC_API_FORBIDDEN" }));
      await inspectForBlock(refusal(500, { error_code: UI_VERSION_BLOCKED_CODE }));
    });
    expect(screen.getByText("the app")).toBeInTheDocument();
  });
});
