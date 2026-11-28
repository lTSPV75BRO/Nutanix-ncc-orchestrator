import { renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const error = vi.fn();
const success = vi.fn();
const close = vi.fn();
vi.mock("./notify", () => ({
  notify: { error: (...a: unknown[]) => error(...a), success: (...a: unknown[]) => success(...a), close: (...a: unknown[]) => close(...a) },
  notifyError: vi.fn(),
}));

import { useApiReachabilityNotice } from "./App";

describe("API reachability notice", () => {
  beforeEach(() => {
    error.mockClear();
    success.mockClear();
    close.mockClear();
  });

  it("stays quiet while the API answers", () => {
    renderHook(() => useApiReachabilityNotice(false, () => undefined));
    expect(error).not.toHaveBeenCalled();
    expect(success).not.toHaveBeenCalled();
  });

  it("raises one sticky notice when the API stops answering, and not again while it stays down", () => {
    const { rerender } = renderHook(({ down }) => useApiReachabilityNotice(down, () => undefined), { initialProps: { down: false } });
    rerender({ down: true });
    rerender({ down: true });
    expect(error).toHaveBeenCalledTimes(1);
    expect(error.mock.calls[0][0]).toMatchObject({ key: "ncc-api-unreachable", duration: 0, message: "API unreachable" });
  });

  it("notices an API that is down from the start, then confirms when it returns", () => {
    const { rerender } = renderHook(({ down }) => useApiReachabilityNotice(down, () => undefined), { initialProps: { down: true } });
    expect(error).toHaveBeenCalledTimes(1);
    rerender({ down: false });
    expect(close).toHaveBeenCalledWith("ncc-api-unreachable");
    expect(success).toHaveBeenCalledTimes(1);
    rerender({ down: false });
    expect(success).toHaveBeenCalledTimes(1);
  });

  it("announces again on a second outage", () => {
    const { rerender } = renderHook(({ down }) => useApiReachabilityNotice(down, () => undefined), { initialProps: { down: true } });
    rerender({ down: false });
    rerender({ down: true });
    expect(error).toHaveBeenCalledTimes(2);
  });
});
