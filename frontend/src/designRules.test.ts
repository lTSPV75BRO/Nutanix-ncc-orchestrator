// Guards the redesign's rules (docs/ui/redesign-plan.md) for the files built on
// the design system: colours come from theme tokens in styles.css, never hex
// values in components, and inline styles only carry data-driven sizes (bar
// widths, chart heights). Older screens are not covered yet.
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const SRC = dirname(fileURLToPath(import.meta.url));

const DIRS = ["ui", "features/runs"];
const FILES = [
  "navigation.tsx",
  "components/AppShell.tsx",
  "pages/OverviewPage.tsx",
  "pages/RunsPage.tsx",
  "pages/RunDetailPage.tsx",
  "pages/InsightsPage.tsx",
];

function covered(): string[] {
  const out = FILES.map((f) => join(SRC, f));
  for (const d of DIRS) {
    for (const f of readdirSync(join(SRC, d))) {
      if (/\.tsx?$/.test(f) && !/\.test\.tsx?$/.test(f)) out.push(join(SRC, d, f));
    }
  }
  return out;
}

/** Inline style keys allowed: sizes computed from data. */
const ALLOWED_STYLE_KEYS = new Set(["width", "height"]);

function stripComments(code: string): string {
  return code.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:])\/\/.*$/gm, "$1");
}

function hexColours(code: string): string[] {
  return code.match(/["'`]#[0-9a-fA-F]{3,8}\b/g) ?? [];
}

function badInlineStyles(code: string): string[] {
  const bad: string[] = [];
  for (const m of code.matchAll(/style=\{\{([^}]*)\}\}/g)) {
    const keys = [...m[1].matchAll(/(?:^|,)\s*(\[[^\]]+\]|[\w-]+)\s*:/g)].map((k) => k[1]);
    // CSS custom properties (["--x" as string]) are how components pass data to CSS.
    if (keys.some((k) => !ALLOWED_STYLE_KEYS.has(k) && !k.startsWith('["--'))) bad.push(m[0]);
  }
  return bad;
}

describe("design rules", () => {
  const files = covered();

  it("catches what it is meant to catch", () => {
    const sample = `<div style={{ width: "40%" }} /><p style={{ color: "#f43f5e", margin: 4 }} />
      <i style={{ ["--d" as string]: "1s" }} />// a comment "#123456" is fine`;
    expect(hexColours(stripComments(sample))).toEqual(['"#f43f5e']);
    expect(badInlineStyles(stripComments(sample))).toEqual(['style={{ color: "#f43f5e", margin: 4 }}']);
  });

  it("covers the redesigned files", () => {
    expect(files.length).toBeGreaterThan(10);
  });

  for (const file of files) {
    const name = relative(SRC, file).replace(/\\/g, "/");
    const code = stripComments(readFileSync(file, "utf8"));

    it(`${name}: no hex colours`, () => {
      expect(hexColours(code)).toEqual([]);
    });

    it(`${name}: inline styles only set data-driven sizes`, () => {
      expect(badInlineStyles(code)).toEqual([]);
    });
  }
});
