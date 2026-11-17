import { useEffect } from "react";

/**
 * Drafts that can't overwrite.
 *
 * A form that keeps its fields in local storage holds a draft across visits.
 * The draft is only worth keeping against the saved version it was made from:
 * once the server's version moves on (another admin saved), restoring it would
 * show old values as "unsaved changes", one click away from overwriting the
 * newer save. So the form stores, next to its fields, the version its draft
 * was made from, and on load uses the draft only when that version is still
 * the one the server returns.
 *
 * Stored under `key`: `"false"` when the form matches the saved version, or
 * `{"base":"<version>"}` while it holds unsaved changes.
 */

/** The saved version this browser's draft was made from, or null when it holds none. */
export function storedDraftBase(key: string): string | null {
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw || raw === "false") return null;
    const parsed: unknown = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && typeof (parsed as { base?: unknown }).base === "string") {
      return (parsed as { base: string }).base;
    }
    // A draft from before versions were recorded: its base is unknown.
    return "";
  } catch {
    return null;
  }
}

/** Whether the stored draft was made from `version`, the version the server returns now. */
export function draftIsCurrent(key: string, version: string): boolean {
  return version !== "" && storedDraftBase(key) === version;
}

/**
 * Keeps the draft marker up to date. `dirty` is null until the saved version
 * is known (nothing is written then); `base` is the saved version the form was
 * loaded from.
 */
export function useDraftBase(key: string, dirty: boolean | null, base: string) {
  useEffect(() => {
    if (dirty === null) return;
    try {
      window.localStorage.setItem(key, dirty ? JSON.stringify({ base }) : "false");
    } catch {
      // ignore storage errors
    }
  }, [key, dirty, base]);
}
