import { useEffect, useRef, useState } from "react";
import { Modal } from "antd";
import { api } from "../api/client";

// Inactivity threshold before the warning dialog, and the grace period the user
// has to respond before being signed out. Kept here so they are easy to tune.
const IDLE_LIMIT_MS = 60 * 60_000; // 60 minutes of no interaction
const COUNTDOWN_SEC = 60; // seconds to respond before auto-logout

type SessionIdleGuardProps = {
  onLogout: () => void;
  onStayLoggedIn?: () => void;
};

/**
 * Watches for user inactivity while signed in. After IDLE_LIMIT_MS with no
 * interaction it opens a "stay logged in?" dialog with a COUNTDOWN_SEC
 * countdown; staying refreshes the session (extending its expiry via
 * POST /api/v1/auth/refresh), while ignoring it or letting the countdown lapse
 * signs the user out.
 */
export function SessionIdleGuard({ onLogout, onStayLoggedIn }: SessionIdleGuardProps) {
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

  return (
    <Modal
      open={warning}
      title="Session about to expire"
      okText="Stay signed in"
      cancelText="Sign out"
      onOk={handleStay}
      onCancel={onLogout}
      confirmLoading={staying}
      closable={false}
      maskClosable={false}
      keyboard={false}
    >
      Your session has been inactive. For your security, you will be signed out in{" "}
      <strong>{secondsLeft}</strong> second{secondsLeft === 1 ? "" : "s"}.
    </Modal>
  );
}
