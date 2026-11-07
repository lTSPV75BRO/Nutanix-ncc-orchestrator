import { describe, expect, it } from "vitest";
import type { Viewer } from "../navigation";
import { inCodeEditor, matchJumpItems, visibleJumpItems } from "./CommandJump";

const admin: Viewer = { isAdmin: true, canOperate: true, loginEnabled: true };
const operator: Viewer = { isAdmin: false, canOperate: true, loginEnabled: true };
const viewer: Viewer = { isAdmin: false, canOperate: false, loginEnabled: true };

describe("matchJumpItems", () => {
  it("matches TLS, users, backup, and health aliases", () => {
    expect(matchJumpItems("TLS").map((i) => i.id)).toContain("tls");
    expect(matchJumpItems("users").map((i) => i.id)).toContain("users");
    expect(matchJumpItems("backup").map((i) => i.id)).toContain("backup");
    expect(matchJumpItems("health").map((i) => i.id)).toContain("health");
    expect(matchJumpItems("ldap").map((i) => i.id)).toContain("ldap");
  });

  it("returns every destination for an empty query", () => {
    expect(matchJumpItems("").length).toBeGreaterThan(5);
  });
});

describe("visibleJumpItems", () => {
  const ids = (v?: Viewer) => visibleJumpItems(v).map((i) => i.id);

  it("shows only destinations the viewer may open", () => {
    expect(ids(admin)).toEqual(expect.arrayContaining(["tls", "backup", "supervisor", "schedule", "findings"]));
    expect(ids(operator)).toEqual(expect.arrayContaining(["schedule", "logs", "runs", "findings"]));
    expect(ids(operator)).not.toContain("backup");
    expect(ids(operator)).not.toContain("tls");
    expect(ids(viewer)).toEqual(expect.arrayContaining(["dashboard", "findings", "insights", "runs"]));
    expect(ids(viewer)).not.toContain("schedule");
  });

  it("hides Access destinations when sign-in is off", () => {
    expect(ids({ ...admin, loginEnabled: false })).not.toContain("users");
  });

  it("leaves Ctrl/⌘+K to a focused code editor", () => {
    const editor = document.createElement("div");
    editor.className = "monaco-editor";
    const textarea = document.createElement("textarea");
    editor.append(textarea);
    expect(inCodeEditor(textarea)).toBe(true);
    expect(inCodeEditor(document.body)).toBe(false);
    expect(inCodeEditor(null)).toBe(false);
  });
});
