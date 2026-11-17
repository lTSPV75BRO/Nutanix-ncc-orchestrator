/** Formats a wait as a clock: "0:42", "14:05", "1:02:03". */
export function formatCountdown(totalSeconds: number): string {
  const s = Math.max(0, Math.ceil(totalSeconds));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  const ss = String(sec).padStart(2, "0");
  return h > 0 ? `${h}:${String(m).padStart(2, "0")}:${ss}` : `${m}:${ss}`;
}

/**
 * Plain label for the time left in a session, for the user menu:
 * "Signed in for another 7 h 40 min", "Session ends in 4 min",
 * "Session ends in less than a minute", "Session has ended".
 */
export function sessionRemainingLabel(msLeft: number): string {
  if (msLeft <= 0) return "Session has ended";
  const totalMin = Math.floor(msLeft / 60_000);
  if (totalMin < 1) return "Session ends in less than a minute";
  if (totalMin < 60) return `Session ends in ${totalMin} min`;
  const h = Math.floor(totalMin / 60);
  const m = totalMin % 60;
  return m > 0 ? `Signed in for another ${h} h ${m} min` : `Signed in for another ${h} h`;
}

/**
 * Session deadline (epoch ms) from /auth/me. expires_in_sec measured from when
 * the answer arrived is preferred over expires_at, so a skewed browser clock
 * does not move the deadline.
 */
export function sessionDeadline(
  me: { expires_at?: string; expires_in_sec?: number } | null | undefined,
  receivedAt: number,
): number | null {
  if (!me) return null;
  if (typeof me.expires_in_sec === "number" && me.expires_in_sec > 0 && receivedAt > 0) {
    return receivedAt + me.expires_in_sec * 1000;
  }
  if (me.expires_at) {
    const t = Date.parse(me.expires_at);
    return Number.isNaN(t) ? null : t;
  }
  return null;
}
