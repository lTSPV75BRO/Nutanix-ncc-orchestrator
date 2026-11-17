import { renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { draftIsCurrent, storedDraftBase, useDraftBase } from "./useDraftBase";

const KEY = "form.hasDraft";

describe("useDraftBase", () => {
  afterEach(() => localStorage.clear());

  it("records the saved version a draft was made from", () => {
    const { rerender } = renderHook(({ dirty, base }: { dirty: boolean | null; base: string }) => useDraftBase(KEY, dirty, base), {
      initialProps: { dirty: null as boolean | null, base: "" },
    });
    expect(localStorage.getItem(KEY)).toBeNull();
    rerender({ dirty: false, base: "v1" });
    expect(storedDraftBase(KEY)).toBeNull();
    rerender({ dirty: true, base: "v1" });
    expect(storedDraftBase(KEY)).toBe("v1");
  });

  it("uses a draft only against the version it was made from", () => {
    localStorage.setItem(KEY, JSON.stringify({ base: "v1" }));
    expect(draftIsCurrent(KEY, "v1")).toBe(true);
    expect(draftIsCurrent(KEY, "v2")).toBe(false);
    expect(draftIsCurrent(KEY, "")).toBe(false);
  });

  it("does not trust a draft without a recorded version", () => {
    localStorage.setItem(KEY, "true");
    expect(storedDraftBase(KEY)).toBe("");
    expect(draftIsCurrent(KEY, "v1")).toBe(false);
    localStorage.setItem(KEY, "not json");
    expect(storedDraftBase(KEY)).toBeNull();
  });
});
