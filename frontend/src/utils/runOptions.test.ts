import { describe, expect, it } from "vitest";
import type { ExtraArgOption } from "../api/types";
import { buildExtraArgs, describeExtraArgs } from "./runOptions";

const options: ExtraArgOption[] = [
  { flag: "--outputs", takes_value: true, label: "Report formats", help: "", choices: ["html", "csv", "json"], multi: true },
  { flag: "--dry-run", takes_value: false, label: "Dry run", help: "" },
  { flag: "--clusters", takes_value: true, label: "Clusters", help: "", admin_only: true, advanced: true },
];

describe("buildExtraArgs", () => {
  it("turns option values into flags in a stable order", () => {
    expect(
      buildExtraArgs(options, { "--outputs": "csv,json", "--dry-run": true, "--clusters": "10.0.0.1" }, "", { restricted: false }),
    ).toEqual(["--outputs", "csv,json", "--dry-run", "--clusters", "10.0.0.1"]);
  });

  it("omits empty values and switched-off flags", () => {
    expect(buildExtraArgs(options, { "--outputs": "  ", "--dry-run": false }, "", { restricted: false })).toEqual([]);
  });

  it("drops admin-only options for restricted callers", () => {
    expect(buildExtraArgs(options, { "--clusters": "10.0.0.1" }, "", { restricted: true })).toEqual([]);
  });

  it("lets free-text flags win instead of sending a flag twice", () => {
    expect(buildExtraArgs(options, { "--outputs": "csv" }, "--outputs json --quiet-hours 22:00-06:00", { restricted: false })).toEqual([
      "--outputs",
      "json",
      "--quiet-hours",
      "22:00-06:00",
    ]);
  });
});

describe("describeExtraArgs", () => {
  it("explains the empty case", () => {
    expect(describeExtraArgs([])).toMatch(/defaults/);
    expect(describeExtraArgs(["--dry-run"])).toBe("--dry-run");
  });
});
