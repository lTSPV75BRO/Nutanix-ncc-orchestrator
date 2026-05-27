import type { ReactNode } from "react";
import { useEffect } from "react";
import { App, notification as staticNotification } from "antd";
import type { NotificationInstance } from "antd/es/notification/interface";
import { LoadingOutlined } from "@ant-design/icons";

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
  const payload = typeof input === "string" ? { message: input } : input;
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

export function notifyError(error: unknown, fallback = "Something went wrong"): void {
  const message =
    error instanceof Error
      ? error.message || fallback
      : typeof error === "string"
        ? error
        : fallback;
  notify.error({ message: "Request failed", description: message });
}

export function NotifyBridge() {
  const { notification } = App.useApp();
  useEffect(() => {
    setNotificationApi(notification);
    return () => setNotificationApi(null);
  }, [notification]);
  return null;
}
