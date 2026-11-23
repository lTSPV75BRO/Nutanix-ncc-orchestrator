import { describe, expect, it } from "vitest";
import { isClusterAddress, prismURLFor } from "./prism";

describe("prismURLFor", () => {
  it("links IPv4 addresses, with or without scheme and port", () => {
    expect(prismURLFor("10.90.0.11")).toBe("https://10.90.0.11:9440");
    expect(prismURLFor("https://10.90.0.11:9440/")).toBe("https://10.90.0.11:9440");
  });

  it("links fully qualified names", () => {
    expect(prismURLFor("prism.dc1.example.com")).toBe("https://prism.dc1.example.com:9440");
    expect(prismURLFor("PC.Example.COM")).toBe("https://PC.Example.COM:9440");
  });

  it("does not link display names, ids or junk", () => {
    for (const v of ["dev-lab-01", "", "  ", "0f3b2a14-7c1d-4e0a-9b6c-1a2b3c4d5e6f", "cluster one.example.com", "10.0.0", "a.b", "-x.example.com"]) {
      expect(prismURLFor(v), v).toBe("");
    }
    expect(isClusterAddress("10.0.0.1")).toBe(true);
    expect(isClusterAddress("dev-lab-01")).toBe(false);
  });
});
