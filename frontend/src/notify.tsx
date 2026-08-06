import type { ReactNode } from "react";
import { useEffect } from "react";
import { App, notification as staticNotification } from "antd";
import type { NotificationInstance } from "antd/es/notification/interface";
import { LoadingOutlined } from "@ant-design/icons";
import { ApiError } from "./api/client";

type ToastVariant = "success" | "info" | "warning" | "error";

type ToastInput =
  | string
  | {
      message: ReactNode;
      description?: ReactNode;
      duration?: number;
      key?: string;
      icon?: ReactNode;
      btn?: ReactNode;
      onClose?: () => void;
    };

let bound: NotificationInstance | null = null;

const COMMON_OPTS = {
  placement: "topRight" as const,
  duration: 4.5,
  showProgress: true,
};

export function setNotificationApi(api: NotificationInstance | null): void {
  bound = api;
}

function open(variant: ToastVariant, input: ToastInput): void {
  const payload =
    typeof input === "string"
      ? { title: input }
      : (({ message, ...rest }) => ({ title: message, ...rest }))(input);
  const args = {
    ...COMMON_OPTS,
    ...payload,
  };
  if (bound) {
    bound[variant](args);
  } else {
    staticNotification.config(COMMON_OPTS);
    staticNotification[variant](args);
  }
}

function destroy(key?: string): void {
  if (bound) {
    bound.destroy(key as string);
  } else {
    staticNotification.destroy(key as string);
  }
}

export const notify = {
  success: (input: ToastInput) => open("success", input),
  info: (input: ToastInput) => open("info", input),
  warning: (input: ToastInput) => open("warning", input),
  error: (input: ToastInput) => open("error", input),
  /**
   * Sticky info-style toast with a spinner icon and infinite duration unless
   * `duration` is explicitly set. Pass a stable `key` so subsequent calls
   * update the same toast in place. Use `notify.close(key)` to dismiss.
   */
  loading: (input: ToastInput) => {
    const payload = typeof input === "string" ? { message: input } : input;
    open("info", {
      ...payload,
      icon: payload.icon ?? <LoadingOutlined style={{ color: "#06b6d4" }} />,
      duration: payload.duration ?? 0,
    });
  },
  close: (key: string) => destroy(key),
};

// extractErrorDetail pulls the backend's verbose output (e.g. the exact
// `validate-config` errors carried in `data.output`) out of an ApiError so the
// toast can show the real problem instead of just "exit status 2".
function extractErrorDetail(error: unknown): string {
  if (!(error instanceof ApiError) || !error.data || typeof error.data !== "object") {
    return "";
  }
  const data = error.data as Record<string, unknown>;
  for (const key of ["output", "detail", "details", "stderr"]) {
    const v = data[key];
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return "";
}

export function notifyError(error: unknown, fallback = "Something went wrong"): void {
  const message =
    error instanceof Error
      ? error.message || fallback
      : typeof error === "string"
        ? error
        : fallback;
  const detail = extractErrorDetail(error);
  // When the backend includes detail, show it verbatim (monospace, scrollable)
  // and keep the toast up longer so the user can read the actual error.
  const description: ReactNode = detail ? (
    <div>
      <div>{message}</div>
      <pre
        style={{
          margin: "8px 0 0",
          padding: 8,
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
          maxHeight: 320,
          overflow: "auto",
          fontSize: 12,
          background: "rgba(127,127,127,0.12)",
          borderRadius: 4,
        }}
      >
        {detail}
      </pre>
    </div>
  ) : (
    message
  );
  notify.error({
    message: "Request failed",
    description,
    duration: detail ? 12 : COMMON_OPTS.duration,
  });
}

export function NotifyBridge() {
  const { notification } = App.useApp();
  useEffect(() => {
    setNotificationApi(notification);
    return () => setNotificationApi(null);
  }, [notification]);
  return null;
}
