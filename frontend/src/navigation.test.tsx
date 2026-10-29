import { describe, expect, it } from "vitest";
import {
  findingsRedirect,
  legacySettingsTarget,
  pageTitle,
  sectionHref,
  settingsLanding,
  visibleSections,
  type Viewer,
} from "./navigation";

const admin: Viewer = { isAdmin: true, canOperate: true, loginEnabled: true };
const operator: Viewer = { isAdmin: false, canOperate: true, loginEnabled: true };
const viewer: Viewer = { isAdmin: false, canOperate: false, loginEnabled: true };

describe("navigation", () => {
  it("keeps the operator set of sections the old Settings tabs allowed", () => {
    expect(visibleSections(operator).map((s) => s.key).sort()).toEqual(
      ["audit", "connection", "logs", "runs", "schedule"].sort(),
    );
    expect(visibleSections(viewer)).toEqual([]);
    expect(visibleSections(admin).map((s) => s.key)).toContain("access");
  });

  it("hides Access when sign-in is off", () => {
    expect(visibleSections({ ...admin, loginEnabled: false }).map((s) => s.key)).not.toContain("access");
  });

  it("builds section links with query parameters", () => {
    expect(sectionHref("runs")).toBe("/runs");
    expect(sectionHref("maintenance", { focus: "backup" })).toBe("/system/maintenance?focus=backup");
    expect(sectionHref("audit", { action: undefined })).toBe("/system/audit");
  });

  it("redirects old /settings?tab= links and keeps other parameters", () => {
    expect(legacySettingsTarget("?tab=runs", admin)).toBe("/runs");
    expect(legacySettingsTarget("?tab=access&focus=tls", admin)).toBe("/settings/access?focus=tls");
    expect(legacySettingsTarget("?tab=developer&dev=json", admin)).toBe("/system/developer?dev=json");
    expect(legacySettingsTarget("", admin)).toBe("/settings/config");
  });

  it("sends people to a section they may open", () => {
    expect(legacySettingsTarget("?tab=config", operator)).toBe("/settings/schedule");
    expect(settingsLanding(operator)).toBe("/settings/schedule");
    expect(settingsLanding(viewer)).toBe("/");
  });

  it("sends old shared Dashboard links to Findings", () => {
    expect(findingsRedirect("?source=ncc&sev=FAIL")).toBe("/findings?source=ncc&sev=FAIL");
    expect(findingsRedirect("")).toBeNull();
    expect(findingsRedirect("?utm=x")).toBeNull();
  });

  it("names pages for the top bar", () => {
    expect(pageTitle("/findings")).toBe("Findings");
    expect(pageTitle("/")).toBe("Overview");
    expect(pageTitle("/system/status")).toBe("System status");
    expect(pageTitle("/settings/config/")).toBe("Clusters & config");
  });
});
