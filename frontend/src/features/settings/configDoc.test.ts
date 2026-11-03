import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { parseDocument } from "yaml";
import {
  changedKeys,
  differsFromDefault,
  otherEntries,
  readBool,
  readList,
  readMap,
  readNumber,
  readText,
  writeValue,
} from "./configDoc";
import { ALL_FIELDS, KNOWN_KEYS, fieldError } from "./configSchema";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, "..", "..", "..", "..");
const EXAMPLE = readFileSync(join(REPO, "examples", "example_config.yaml"), "utf8");

describe("config document", () => {
  it("reads the nested layout the example config uses", () => {
    const doc = parseDocument(EXAMPLE);
    // These used to show the wrong value: the form only knew 37 nested paths.
    expect(readBool(doc, "run-history")).toBe(true);
    expect(readNumber(doc, "retain-last")).toBe(500);
    expect(readText(doc, "request-timeout")).toBe("20s");
    expect(readList(doc, "clusters")).toEqual(["REPLACE_WITH_CLUSTER_IP"]);
    expect(readList(doc, "outputs")).toEqual(["html", "csv"]);
    expect(readText(doc, "smtp-password")).toBe("secret://SMTP_PASSWORD");
    expect(readText(doc, "policy-gates")).toBe("");
    // Unset: the runner default.
    expect(readBool(doc, "auto")).toBe(false);
    expect(changedKeys(doc, parseDocument(EXAMPLE))).toEqual([]);
  });

  it("reads older nested spellings and lets a flat key win, like the runner", () => {
    const doc = parseDocument(
      "schema-version: 1\nnotifications:\n  smtp-port: 2525\nrunner:\n  filtering:\n    policy-gates: new-fails>0\n  execution:\n    timeout: 5m\ntimeout: 9m\n",
    );
    expect(readNumber(doc, "smtp-port")).toBe(2525);
    expect(readText(doc, "policy-gates")).toBe("new-fails>0");
    expect(readText(doc, "timeout")).toBe("9m");
  });

  it("writes new keys in the file's own layout", () => {
    const nested = parseDocument("schema-version: 1\nrunner:\n  targets:\n    clusters: [a]\n");
    writeValue(nested, "auto", true);
    writeValue(nested, "automation-level", "advisory");
    writeValue(nested, "clusters", ["a", "b"]);
    writeValue(nested, "webhook-headers", { "X-Auth-Token": "abc" });
    const text = nested.toString();
    expect(text).toContain("automation:\n    enabled: true\n    level: advisory");
    expect(text).toContain("clusters: [ a, b ]");
    expect(text).toContain("headers:\n      X-Auth-Token: abc");
    expect(text).not.toMatch(/^auto:/m);

    const flat = parseDocument("username: admin\npcs: [x]\n");
    writeValue(flat, "clusters", ["a", "b"]);
    writeValue(flat, "pcs", ["x", "y"]);
    writeValue(flat, "timeout", "20m");
    expect(flat.toString()).toBe("username: admin\npcs: [ x, y ]\nclusters: a,b\ntimeout: 20m\n");
  });

  it("updates a key where the file has it and removes every spelling on reset", () => {
    const doc = parseDocument("schema-version: 1\nnotifications:\n  webhook-url: https://a\npassword: hunter2\nrunner:\n  credentials:\n    password: secret://X\n");
    writeValue(doc, "webhook-url", "https://b");
    expect(doc.getIn(["notifications", "webhook-url"])).toBe("https://b");
    expect(doc.hasIn(["notifications", "webhook", "url"])).toBe(false);
    writeValue(doc, "password", undefined);
    expect(doc.has("password")).toBe(false);
    expect(doc.hasIn(["runner", "credentials", "password"])).toBe(false);
  });

  it("reads header maps", () => {
    const doc = parseDocument("webhook-headers:\n  X-A: one\n  X-B: two\n");
    expect(readMap(doc, "webhook-headers")).toEqual([
      ["X-A", "one"],
      ["X-B", "two"],
    ]);
  });

  it("knows which values differ from the runner default", () => {
    const doc = parseDocument("timeout: 15m\nmax-parallel: 0\nclusters-file: ''\nretry-max-attempts: 9\noutputs: html,csv\nemail-use-tls: false\n");
    expect(differsFromDefault(doc, "timeout")).toBe(false);
    expect(differsFromDefault(doc, "max-parallel")).toBe(false);
    expect(differsFromDefault(doc, "clusters-file")).toBe(false);
    expect(differsFromDefault(doc, "outputs")).toBe(false);
    expect(differsFromDefault(doc, "retry-max-attempts")).toBe(true);
    expect(differsFromDefault(doc, "email-use-tls")).toBe(true);
    expect(differsFromDefault(doc, "poll-interval")).toBe(false);
  });

  it("lists keys the form does not edit, marking server notes", () => {
    const doc = parseDocument(EXAMPLE + "\nrunner-typo: 1\n");
    const other = otherEntries(doc);
    expect(other.find((o) => o.path === "api.listen")).toMatchObject({ stack: true });
    expect(other.find((o) => o.path === "runner-typo")).toMatchObject({ stack: false });
    // Settings the form edits, including api.cache.pc-alerts-cache-ttl, are not listed.
    expect(other.some((o) => o.path.startsWith("runner.") || o.path === "api.cache.pc-alerts-cache-ttl")).toBe(false);
    expect(other.some((o) => o.path.startsWith("notifications."))).toBe(false);
  });

  it("checks durations, URLs and quiet hours", () => {
    const field = (key: string) => ALL_FIELDS.find((f) => f.key === key)!;
    expect(fieldError(field("timeout"), "1h30m")).toBeNull();
    expect(fieldError(field("timeout"), "0")).toBeNull();
    expect(fieldError(field("timeout"), "15 minutes")).toMatch(/duration/);
    expect(fieldError(field("webhook-url"), "hooks.example.com")).toMatch(/URL/);
    expect(fieldError(field("quiet-hours"), "22:00-06:00")).toBeNull();
    expect(fieldError(field("quiet-hours"), "10pm-6am")).toMatch(/HH:MM/);
  });
});

describe("config schema stays in step with the runner", () => {
  const go = readFileSync(join(REPO, "cmd", "ncc-orchestrator", "goNCC.go"), "utf8");

  it("has a field for every key config.yaml accepts", () => {
    const block = go.slice(go.indexOf("allowedTopKeys := map[string]bool{"), go.indexOf("for key := range viper.AllSettings()"));
    const keys = [...block.matchAll(/"([a-z0-9-]+)":\s*true/g)].map((m) => m[1]);
    const notSettings = new Set([
      "config",
      "update",
      "schema-version",
      "runner",
      "storage",
      "api",
      "ui",
      "deployment",
      "notifications",
      "logging",
      "prometheus",
      "secrets",
    ]);
    const missing = keys.filter((k) => !notSettings.has(k) && !KNOWN_KEYS.includes(k));
    expect(missing).toEqual([]);
  });

  it("uses the runner's nested paths", () => {
    const block = go.slice(go.indexOf("mappings := map[string]string{"));
    const pairs = [...block.slice(0, block.indexOf("\n\t}")).matchAll(/"([a-z0-9.-]+)":\s*"([a-z0-9-]+)"/g)];
    const wrong = pairs.filter(([, path, key]) => {
      const f = ALL_FIELDS.find((x) => x.key === key);
      return !f || ![f.path, ...(f.altPaths ?? [])].some((p) => p.join(".") === path);
    });
    expect(wrong.map(([, path, key]) => `${path} -> ${key}`)).toEqual([]);
  });
});
