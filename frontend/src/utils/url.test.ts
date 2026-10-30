import { describe, expect, it } from "vitest";
import { nutanixKbUrl, safeExternalUrl } from "./url";

describe("safeExternalUrl", () => {
  it("keeps http and https links", () => {
    expect(safeExternalUrl("https://portal.nutanix.com/kb/2473")).toBe("https://portal.nutanix.com/kb/2473");
    expect(safeExternalUrl(" http://10.0.0.1:9440/ ")).toBe("http://10.0.0.1:9440/");
  });

  it("drops anything that is not a web link", () => {
    for (const bad of ["javascript:alert(1)", "JaVaScRiPt:alert(1)", "data:text/html,<b>x</b>", "vbscript:x", "/kb/1", "portal.nutanix.com/kb/1", "", null, undefined, 42]) {
      expect(safeExternalUrl(bad)).toBe("");
    }
  });

  it("builds portal KB links over https", () => {
    expect(nutanixKbUrl(2473)).toBe("https://portal.nutanix.com/kb/2473");
  });
});
