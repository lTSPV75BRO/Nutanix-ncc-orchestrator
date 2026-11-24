import { useEffect, useRef, useState } from "react";
import { Alert, Button, Modal, Progress } from "antd";
import { ClockCircleOutlined } from "@ant-design/icons";
import { api } from "../api/client";
import { useNow, useSecondsUntil } from "../hooks/useNow";
import { formatCountdown, sessionRemainingLabel } from "../utils/sessionTime";

/**
 * "Signed in for another 7 h 40 min" / "Session ends in 4 min", for the user
 * menu. Refreshes every 15 seconds.
 */
export function SessionTimeLeft({ expiresAt }: { expiresAt: number }) {
  useNow(15_000);
  return <span className="header-user-session">{sessionRemainingLabel(expiresAt - Date.now())}</span>;
}

// Inactivity threshold before the warning dialog, and the grace period the user
// has to respond before being signed out. Kept here so they are easy to tune.
const IDLE_LIMIT_MS = 60 * 60_000; // 60 minutes of no interaction
const COUNTDOWN_SEC = 60; // seconds to respond before auto-logout
/** How long before the session's own expiry the non-blocking notice shows. */
const EXPIRY_WARNING_SEC = 5 * 60;

type SessionIdleGuardProps = {
  onLogout: () => void;
  onStayLoggedIn?: () => void;
  /** When the session itself ends (epoch ms), from /auth/me; null if unknown. */
  expiresAt?: number | null;
};

/**
 * Non-blocking notice shown in the last EXPIRY_WARNING_SEC of a session. It
 * offers "Stay signed in" (POST /api/v1/auth/refresh); when the server will
 * not extend the session it offers "Sign in again" instead.
 */
function SessionExpiryNotice({
  expiresAt,
  onRefresh,
  onLogout,
}: {
  expiresAt: number;
  onRefresh: () => Promise<boolean>;
  onLogout: () => void;
}) {
  const secondsLeft = useSecondsUntil(expiresAt);
  const [dismissedFor, setDismissedFor] = useState<number | null>(null);
  const [failedFor, setFailedFor] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  // The deadline is re-measured on every /auth/me poll and moves by a few
  // seconds; a refresh moves it by the whole session length. Treat deadlines
  // within a minute as the same session end.
  const sameEnd = (t: number | null) => t !== null && Math.abs(t - expiresAt) < 60_000;
  if (secondsLeft <= 0 || secondsLeft > EXPIRY_WARNING_SEC || sameEnd(dismissedFor)) return null;
  const failed = sameEnd(failedFor);

  const stay = async () => {
    setBusy(true);
    const ok = await onRefresh();
    setBusy(false);
    if (!ok) setFailedFor(expiresAt);
  };

  return (
    <div className="session-expiry-notice">
      <Alert
        type="warning"
        showIcon
        icon={<ClockCircleOutlined />}
        title="Your session is ending"
        description={
          <span>
            {failed ? "This session can't be extended. Sign in again to keep working. " : null}
            Signing out in{" "}
            <strong role="timer" aria-live="off">
              {formatCountdown(secondsLeft)}
            </strong>
            .
          </span>
        }
        closable
        onClose={() => setDismissedFor(expiresAt)}
        action={
          failed ? (
            // Own key: a reused button still ignores clicks while its loading state ends.
            <Button key="again" size="small" type="primary" onClick={onLogout}>
              Sign in again
            </Button>
          ) : (
            <Button key="stay" size="small" type="primary" loading={busy} onClick={() => void stay()}>
              Stay signed in
            </Button>
          )
        }
      />
    </div>
  );
}

/**
 * Watches for user inactivity while signed in. After IDLE_LIMIT_MS with no
 * interaction it opens a "stay logged in?" dialog with a COUNTDOWN_SEC
 * countdown; staying refreshes the session (extending its expiry via
 * POST /api/v1/auth/refresh), while ignoring it or letting the countdown lapse
 * signs the user out.
 *
 * It also shows a non-blocking notice shortly before the session itself
 * expires (expiresAt), for users who are active but reach the session's end.
 * The idle dialog takes precedence; both use the same refresh call.
 */
export function SessionIdleGuard({ onLogout, onStayLoggedIn, expiresAt = null }: SessionIdleGuardProps) {
  const [warning, setWarning] = useState(false);
  const [secondsLeft, setSecondsLeft] = useState(COUNTDOWN_SEC);
  const [staying, setStaying] = useState(false);
  const lastActivityRef = useRef(Date.now());
  const warningRef = useRef(false);
  warningRef.current = warning;

  // Record user activity. Once the warning is showing, activity no longer
  // resets the timer — the user must make an explicit choice.
  useEffect(() => {
    const markActive = () => {
      if (!warningRef.current) lastActivityRef.current = Date.now();
    };
    const events: (keyof WindowEventMap)[] = [
      "mousemove",
      "mousedown",
      "keydown",
      "touchstart",
      "scroll",
    ];
    events.forEach((e) => window.addEventListener(e, markActive, { passive: true }));
    return () => events.forEach((e) => window.removeEventListener(e, markActive));
  }, []);

  // Drive the idle check and the countdown from a single 1s ticker.
  useEffect(() => {
    const id = window.setInterval(() => {
      if (warningRef.current) {
        setSecondsLeft((s) => Math.max(0, s - 1));
        return;
      }
      if (Date.now() - lastActivityRef.current >= IDLE_LIMIT_MS) {
        setSecondsLeft(COUNTDOWN_SEC);
        setWarning(true);
      }
    }, 1000);
    return () => window.clearInterval(id);
  }, []);

  // Sign out once the countdown elapses.
  useEffect(() => {
    if (warning && secondsLeft <= 0) onLogout();
  }, [warning, secondsLeft, onLogout]);

  const handleStay = async () => {
    setStaying(true);
    try {
      await api.refreshSession();
      lastActivityRef.current = Date.now();
      setWarning(false);
      onStayLoggedIn?.();
    } catch {
      // Refresh failed (session already gone) — sign out cleanly.
      onLogout();
    } finally {
      setStaying(false);
    }
  };

  // The expiry notice's "Stay signed in": same refresh as the idle dialog, but
  // a failure leaves the user signed in until the session ends, with "Sign in
  // again" offered instead.
  const refreshFromNotice = async (): Promise<boolean> => {
    try {
      await api.refreshSession();
      lastActivityRef.current = Date.now();
      onStayLoggedIn?.();
      return true;
    } catch {
      return false;
    }
  };

  return (
    <>
      {!warning && expiresAt !== null ? (
        <SessionExpiryNotice expiresAt={expiresAt} onRefresh={refreshFromNotice} onLogout={onLogout} />
      ) : null}
      <Modal
        open={warning}
        title="Are you still there?"
        okText="Stay signed in"
        cancelText="Sign out"
        onOk={handleStay}
        onCancel={onLogout}
        okButtonProps={{ autoFocus: true }}
        confirmLoading={staying}
        closable={false}
        mask={{ closable: false }}
        keyboard={false}
        centered
        width={420}
      >
        <div className="idle-dialog-body">
          <ClockCircleOutlined className="idle-dialog-icon" aria-hidden />
          <div style={{ flex: 1, minWidth: 0 }}>
            <p className="idle-dialog-text">
              You haven&apos;t used NCC Orchestrator for a while. For your security you&apos;ll be signed out in{" "}
              <strong className="idle-dialog-count">
                {secondsLeft} second{secondsLeft === 1 ? "" : "s"}
              </strong>
              .
            </p>
            <Progress
              percent={(secondsLeft / COUNTDOWN_SEC) * 100}
              showInfo={false}
              size="small"
              strokeColor={secondsLeft <= 10 ? "var(--ui-fail)" : "var(--ui-accent)"}
              aria-label="Time left before sign-out"
            />
          </div>
        </div>
      </Modal>
    </>
  );
}
