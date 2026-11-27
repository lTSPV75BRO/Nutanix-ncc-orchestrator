import { lazy, Suspense, useCallback, useEffect, useState } from "react";
import { api } from "../api/client";
import type { StrokeTextProps } from "./StrokeText/StrokeText";

/** Plain name if the StrokeText chunk cannot be loaded. */
function PlainName({ text }: StrokeTextProps) {
  return <span className="site-intro-plain">{text}</span>;
}

// gsap is only needed here, so StrokeText loads in its own chunk and only
// when the intro plays; the veil and logo show at once.
const StrokeText = lazy(() => import("./StrokeText/StrokeText").catch(() => ({ default: PlainName })));

/** Renders nothing; its effect fires once the lazy name has committed. */
function Ready({ onReady }: { onReady: () => void }) {
  useEffect(onReady, [onReady]);
  return null;
}

const SEEN_KEY = "ncc.intro.seen";
/** The server's "Opening title" switch (Settings → Features) was on last time. */
export const ON_KEY = "ncc.intro.on";
const NAME = "NCC Orchestrator";

// StrokeText timing (seconds). Outlines finish at DRAW + (len-1)*STAGGER; the
// wipe fill starts at DRAW + FILL_DELAY and lasts max(0.4, DRAW/2).
const DRAW = 1.4;
const STAGGER = 0.05;
const FILL_DELAY = 0.2;
const FILL_DONE_MS = (DRAW + FILL_DELAY + Math.max(0.4, DRAW / 2)) * 1000;
/** Name fully drawn and filled, plus a beat to read it. */
export const INTRO_HOLD_MS = Math.round(FILL_DONE_MS + 900);
/** Longest wait for the name to load before the intro moves on anyway. */
const LOAD_WAIT_MS = 4000;
/** Length of the frost clearing from the page underneath. */
export const INTRO_EXIT_MS = 1400;

function prefersReducedMotion(): boolean {
  return typeof window.matchMedia === "function" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

type Phase = "wait" | "show" | "leaving" | "done";

/**
 * The switch is off by default, so the title plays from the first frame only
 * when the server said "on" last time; otherwise it waits for the answer.
 */
function initialPhase(): Phase {
  if (prefersReducedMotion()) return "done";
  try {
    if (window.sessionStorage.getItem(SEEN_KEY)) return "done";
    return window.localStorage.getItem(ON_KEY) === "1" ? "show" : "wait";
  } catch {
    return "wait";
  }
}

function rememberSwitch(on: boolean) {
  try {
    if (on) window.localStorage.setItem(ON_KEY, "1");
    else window.localStorage.removeItem(ON_KEY);
  } catch {
    // not remembered; the next visit asks the server again
  }
}

function markSeen() {
  try {
    window.sessionStorage.setItem(SEEN_KEY, "1");
  } catch {
    // not remembered; it plays again on reload
  }
}

/**
 * Opening title shown once per browser tab when the site is first opened.
 * The page underneath (normally sign-in) is already rendered and shows
 * through frosted while the name draws itself (React Bits StrokeText); then
 * the name fades and the frost smoothly clears. Click, Escape, Enter or Space
 * skips it. Shown only when an administrator turned "Opening title" on (off
 * by default; read from the public /health and remembered, so later visits
 * play it from the first frame), and not when the OS asks for reduced motion.
 */
export function SiteIntro() {
  const [phase, setPhase] = useState<Phase>(initialPhase);
  const [ready, setReady] = useState(false);
  const onReady = useCallback(() => setReady(true), []);

  useEffect(() => {
    let live = true;
    api
      .health()
      .then((h) => {
        const on = h.intro_title === true;
        rememberSwitch(on);
        if (live) setPhase((p) => (!on ? "done" : p === "wait" ? "show" : p));
      })
      .catch(() => {
        // Unknown: keep a title that is already playing, start none.
        if (live) setPhase((p) => (p === "wait" ? "done" : p));
      });
    return () => {
      live = false;
    };
  }, []);

  const leave = useCallback(() => {
    markSeen();
    setPhase((p) => (p === "show" ? "leaving" : p));
  }, []);

  useEffect(() => {
    if (phase === "show") {
      // The hold counts from when the name can draw, not from page load.
      const t = window.setTimeout(leave, ready ? INTRO_HOLD_MS : LOAD_WAIT_MS);
      const onKey = (e: KeyboardEvent) => {
        if (e.key === "Escape" || e.key === "Enter" || e.key === " ") leave();
      };
      window.addEventListener("keydown", onKey);
      return () => {
        window.clearTimeout(t);
        window.removeEventListener("keydown", onKey);
      };
    }
    if (phase === "leaving") {
      const t = window.setTimeout(() => setPhase("done"), INTRO_EXIT_MS);
      return () => window.clearTimeout(t);
    }
    return undefined;
  }, [phase, ready, leave]);

  if (phase === "done" || phase === "wait") return null;

  return (
    <div
      className={`site-intro${phase === "leaving" ? " is-leaving" : ""}`}
      style={{ ["--intro-exit" as string]: `${INTRO_EXIT_MS}ms` }}
      onClick={leave}
      data-testid="site-intro"
    >
      <div className="site-intro-inner">
        <img className="site-intro-logo" src="/logo.svg" alt="" />
        <Suspense fallback={null}>
          <Ready onReady={onReady} />
          <StrokeText
            className="site-intro-name"
            text={NAME}
            strokeColor="#A78BFA"
            strokeStyle="solid"
            strokeWidth={1.4}
            dashLength={10}
            drawDuration={DRAW}
            fillDelay={FILL_DELAY}
            stagger={STAGGER}
            ease="power2.out"
            trigger="mount"
            fillMode="wipe"
            wipeFrom="left"
            fontSize={128}
            fontWeight={500}
            letterSpacing={-2}
            marching={false}
            hoverRedraw={false}
            shadow={false}
          />
        </Suspense>
        {ready ? <div className="site-intro-tagline">NCC health checks for every cluster</div> : null}
      </div>
      <button
        type="button"
        className="site-intro-skip"
        onClick={(e) => {
          e.stopPropagation();
          leave();
        }}
      >
        Skip
      </button>
    </div>
  );
}
