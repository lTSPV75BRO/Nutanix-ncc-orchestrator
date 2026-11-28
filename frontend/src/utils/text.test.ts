import { describe, expect, it } from "vitest";
import { plural } from "./text";

describe("plural", () => {
  it("picks the singular only for one", () => {
    expect(plural(1, "cluster")).toBe("1 cluster");
    expect(plural(0, "cluster")).toBe("0 clusters");
    expect(plural(2, "time")).toBe("2 times");
    expect(plural(2, "entry", "entries")).toBe("2 entries");
  });
});
