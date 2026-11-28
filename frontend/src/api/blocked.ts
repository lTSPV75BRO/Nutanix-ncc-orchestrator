import { useSyncExternalStore } from "react";

/**
 * The API refuses a dashboard build that is on its blocked-versions list
 * (known vulnerability) with HTTP 403 and this error code. Every request from
 * that build will fail the same way, so the app shows one explanatory screen
 * instead of a toast per request.
 */
export const UI_VERSION_BLOCKED_CODE = "NCC_API_UI_VERSION_BLOCKED";

export type BlockedInfo = {
  code: string;
  message: string;
  remediation?: string;
  requestId?: string;
  /** What this dashboard declared as its own version. */
  uiVersion?: string;
  uiServerVersion?: string;
  /** The entry on the blocked list that matched. */
  blockedVersion?: string;
  component?: string;
  advisory?: string;
  severity?: string;
  fixedIn?: string;
};

let current: BlockedInfo | null = null;
const listeners = new Set<() => void>();

function emit() {
  for (const l of listeners) l();
}

export function reportBlocked(info: BlockedInfo) {
  if (current?.code === info.code && current?.blockedVersion === info.blockedVersion) return;
  current = info;
  emit();
}

export function clearBlocked() {
  current = null;
  emit();
}

function subscribe(l: () => void) {
  listeners.add(l);
  return () => {
    listeners.delete(l);
  };
}

export function useBlocked(): BlockedInfo | null {
  return useSyncExternalStore(subscribe, () => current, () => null);
}

type BlockedPayload = {
  error?: string;
  error_code?: string;
  remediation?: string;
  request_id?: string;
  data?: Record<string, unknown>;
};

/** Reads a refused response (without consuming it) and reports a version block. */
export async function inspectForBlock(response: Response): Promise<void> {
  if (response.status !== 403) return;
  try {
    const payload = (await response.clone().json()) as BlockedPayload;
    if (payload?.error_code !== UI_VERSION_BLOCKED_CODE) return;
    const d = payload.data ?? {};
    const str = (k: string) => (typeof d[k] === "string" && d[k] ? (d[k] as string) : undefined);
    reportBlocked({
      code: UI_VERSION_BLOCKED_CODE,
      message: payload.error || "This dashboard version is blocked.",
      remediation: payload.remediation,
      requestId: payload.request_id || response.headers.get("X-Request-ID") || undefined,
      uiVersion: str("ui_version"),
      uiServerVersion: str("ui_server_version"),
      blockedVersion: str("blocked_version"),
      component: str("component"),
      advisory: str("advisory"),
      severity: str("severity"),
      fixedIn: str("fixed_in"),
    });
  } catch {
    // Not JSON: an ordinary 403.
  }
}
