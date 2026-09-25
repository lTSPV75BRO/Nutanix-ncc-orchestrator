import { describe, expect, it } from "vitest";
import { matchJumpItems } from "./CommandJump";

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
