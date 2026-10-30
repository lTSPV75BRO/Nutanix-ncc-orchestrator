/**
 * Returns `value` when it is an absolute http(s) URL, otherwise "". Links built
 * from report data or Prism Central alerts go through this before they become
 * an href, so a `javascript:`, `data:` or relative value is never clickable.
 */
export function safeExternalUrl(value: unknown): string {
  const text = String(value ?? "").trim();
  if (!text) return "";
  try {
    const url = new URL(text);
    return url.protocol === "https:" || url.protocol === "http:" ? url.href : "";
  } catch {
    return "";
  }
}

/** Nutanix support portal article for a KB number. */
export function nutanixKbUrl(id: string | number): string {
  return `https://portal.nutanix.com/kb/${encodeURIComponent(String(id))}`;
}
