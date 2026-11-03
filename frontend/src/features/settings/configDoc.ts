// Reads and writes runner settings in a YAML document of either shape: flat
// keys (`timeout: 15m`) or the nested `schema-version: 1` layout
// (`runner.execution.timeout`). Like the runner, a flat key wins when both
// exist. Tested in configDoc.test.ts.
import { Document, isSeq } from "yaml";
import { ALL_FIELDS, KNOWN_KEYS, type FieldDef } from "./configSchema";

const FIELD_BY_KEY = new Map(ALL_FIELDS.map((f) => [f.key, f]));

export function fieldFor(key: string): FieldDef {
  const f = FIELD_BY_KEY.get(key);
  if (!f) throw new Error(`unknown config field ${key}`);
  return f;
}

export function isNested(doc: Document): boolean {
  const v = doc.get("schema-version");
  return v !== null && v !== undefined;
}

function plain(v: unknown): unknown {
  if (v && typeof v === "object" && "toJSON" in v && typeof (v as { toJSON: unknown }).toJSON === "function") {
    return (v as { toJSON: () => unknown }).toJSON();
  }
  return v;
}

function candidatePaths(field: FieldDef): string[][] {
  return [[field.key], field.path, ...(field.altPaths ?? [])];
}

/** Where the file sets this field (the one the runner uses), or null. */
export function locate(doc: Document, field: FieldDef): string[] | null {
  for (const p of candidatePaths(field)) {
    if (p.length === 1 ? doc.has(p[0]) : doc.hasIn(p)) return p;
  }
  return null;
}

/** The value as written in the file, as plain JS, or undefined when unset. */
export function readRaw(doc: Document, key: string): unknown {
  const loc = locate(doc, fieldFor(key));
  if (!loc) return undefined;
  const v = plain(doc.getIn(loc));
  return v === null ? undefined : v;
}

export function isSet(doc: Document, key: string): boolean {
  return locate(doc, fieldFor(key)) !== null;
}

/**
 * True when the file sets a value the runner would not use anyway: something
 * other than empty or the built-in default. Removing such a key changes a run.
 */
export function differsFromDefault(doc: Document, key: string): boolean {
  const field = fieldFor(key);
  const v = readRaw(doc, key);
  if (v === undefined) return false;
  if (field.type === "list" || field.type === "multiselect") {
    const items = toList(v);
    const def = toList(field.defaultValue ?? "");
    return items.length > 0 && items.join(",") !== def.join(",");
  }
  if (field.type === "keyvalue") return typeof v === "object" && v !== null && Object.keys(v).length > 0;
  const text = typeof v === "object" ? JSON.stringify(v) : String(v).trim();
  if (text === "") return false;
  if (field.defaultValue === undefined) return true;
  return text.toLowerCase() !== field.defaultValue.toLowerCase();
}

export function toList(v: unknown): string[] {
  if (v === undefined || v === null) return [];
  if (Array.isArray(v)) return v.map((x) => String(x).trim()).filter(Boolean);
  return String(v)
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

/** The value as text: lists joined with ", ", maps as k=v. */
export function readText(doc: Document, key: string): string {
  const v = readRaw(doc, key);
  if (v === undefined) return "";
  if (Array.isArray(v)) return toList(v).join(", ");
  if (typeof v === "object") return Object.entries(v as Record<string, unknown>).map(([k, x]) => `${k}=${String(x)}`).join(", ");
  return String(v);
}

export function readList(doc: Document, key: string): string[] {
  return toList(readRaw(doc, key));
}

/** The effective switch state: the file's value, else the runner default. */
export function readBool(doc: Document, key: string): boolean {
  const v = readRaw(doc, key);
  if (v === undefined) return fieldFor(key).defaultValue === "true";
  if (typeof v === "boolean") return v;
  return String(v).trim().toLowerCase() === "true";
}

export function readNumber(doc: Document, key: string): number | null {
  const v = readRaw(doc, key);
  if (v === undefined || v === "") return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

export function readMap(doc: Document, key: string): [string, string][] {
  const v = readRaw(doc, key);
  if (!v || typeof v !== "object" || Array.isArray(v)) return [];
  return Object.entries(v as Record<string, unknown>).map(([k, x]) => [k, x === null || x === undefined ? "" : String(x)]);
}

/**
 * Sets a field, keeping it where the file already has it. A new key goes to
 * the nested path in a nested file and to the flat key otherwise. `undefined`
 * removes every spelling of the key, so the runner default applies.
 */
export function writeValue(doc: Document, key: string, value: unknown): void {
  const field = fieldFor(key);
  if (value === undefined) {
    for (const p of candidatePaths(field)) {
      if (p.length === 1) doc.delete(p[0]);
      else if (doc.hasIn(p)) doc.deleteIn(p);
    }
    return;
  }
  const loc = locate(doc, field) ?? (isNested(doc) ? field.path : [field.key]);
  let next = value;
  if (field.type === "list" || field.type === "multiselect") {
    const items = Array.isArray(value) ? value.map(String) : toList(value);
    // Nested files and existing YAML lists keep a list; flat files use CSV.
    const asList = loc.length > 1 || isSeq(doc.getIn(loc, true));
    next = asList ? items : items.join(",");
  }
  const node = typeof next === "object" && next !== null ? doc.createNode(next, { flow: Array.isArray(next) }) : next;
  doc.setIn(loc, node);
}

/** Comparable form of a value, for change detection. */
function fingerprint(v: unknown): string {
  return v === undefined ? "∅" : JSON.stringify(v);
}

/** Keys whose value differs between two documents. */
export function changedKeys(before: Document, after: Document): string[] {
  return KNOWN_KEYS.filter((k) => fingerprint(readRaw(before, k)) !== fingerprint(readRaw(after, k)));
}

export type OtherEntry = { path: string; value: string; stack: boolean };

/**
 * Keys in the file that the form does not edit. The api, ui and deployment
 * sections are notes for the servers; the runner ignores them.
 */
export function otherEntries(doc: Document): OtherEntry[] {
  const known = new Set<string>(["schema-version"]);
  for (const f of ALL_FIELDS) {
    for (const p of candidatePaths(f)) known.add(p.join("."));
  }
  const out: OtherEntry[] = [];
  const walk = (value: unknown, path: string[]) => {
    const joined = path.join(".");
    if (known.has(joined)) return;
    if (value && typeof value === "object" && !Array.isArray(value) && Object.keys(value).length > 0) {
      for (const [k, v] of Object.entries(value as Record<string, unknown>)) walk(v, [...path, k]);
      return;
    }
    out.push({
      path: joined,
      value: value === undefined || value === null ? "" : typeof value === "object" ? JSON.stringify(value) : String(value),
      stack: ["api", "ui", "deployment"].includes(path[0]),
    });
  };
  const root = doc.toJS() as unknown;
  if (root && typeof root === "object" && !Array.isArray(root)) {
    for (const [k, v] of Object.entries(root as Record<string, unknown>)) walk(v, [k]);
  }
  return out;
}
