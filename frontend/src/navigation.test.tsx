import { describe, expect, it } from "vitest";
import {
  aliasRedirect,
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
    // Viewers read run history only.
    expect(visibleSections(viewer).map((s) => s.key)).toEqual(["runs"]);
    expect(visibleSections(admin).map((s) => s.key)).toContain("access");
  });

  it("hides Access when sign-in is off", () => {
    expect(visibleSections({ ...admin, loginEnabled: false }).map((s) => s.key)).not.toContain("access");
  });

  it("builds section links with query parameters", () => {
    expect(sectionHref("runs")).toBe("/runs");
    expect(sectionHref("health", { tab: "history" })).toBe("/system/health?tab=history");
    expect(sectionHref("audit", { action: undefined })).toBe("/system/audit");
  });

  it("sends Maintenance links to Updates & backups", () => {
    expect(visibleSections(admin).map((s) => s.key)).not.toContain("maintenance");
    expect(sectionHref("maintenance")).toBe("/system/updates?tab=updates");
    expect(sectionHref("maintenance", { focus: "backup" })).toBe("/system/updates?tab=backups");
    expect(aliasRedirect("/system/maintenance", "?focus=backup")).toBe("/system/updates?tab=backups");
    expect(aliasRedirect("/system/maintenance/", "")).toBe("/system/updates?tab=updates");
    expect(legacySettingsTarget("?tab=maintenance&focus=backup", admin)).toBe("/system/updates?tab=backups");
    expect(pageTitle("/system/updates")).toBe("Updates & backups");
  });

  it("redirects tabs that left Health & repair", () => {
    expect(aliasRedirect("/system/health", "")).toBeNull();
    expect(aliasRedirect("/system/health", "?tab=history")).toBeNull();
    expect(aliasRedirect("/system/health", "?tab=backups")).toBe("/system/updates?tab=backups");
    expect(aliasRedirect("/system/health", "?tab=migrations")).toBe("/system/updates?tab=migrations");
    expect(aliasRedirect("/system/health/", "?tab=updates")).toBe("/system/updates?tab=updates");
    expect(aliasRedirect("/system/health", "?focus=backup")).toBe("/system/updates?tab=backups");
    expect(aliasRedirect("/system/health", "?tab=supervisor")).toBe("/system/status?tab=supervisor");
    expect(aliasRedirect("/system/health", "?tab=platform")).toBe("/system/status?tab=platform");
    expect(sectionHref("health", { tab: "backups" })).toBe("/system/updates?tab=backups");
    expect(legacySettingsTarget("?tab=health&focus=backup", admin)).toBe("/system/updates?tab=backups");
    expect(pageTitle("/system/health")).toBe("Health & repair");
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
    expect(pageTitle("/runs/20261029T0940Z")).toBe("Run details");
    expect(pageTitle("/")).toBe("Overview");
    expect(pageTitle("/system/status")).toBe("System status");
    expect(pageTitle("/settings/config/")).toBe("Clusters & config");
  });
});
