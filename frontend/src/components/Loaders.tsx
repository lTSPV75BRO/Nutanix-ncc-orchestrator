/**
 * Small loading indicators that match the full-page DataLoader. All are sized
 * in em, so they take the font size of where they sit (a button, a toast
 * icon, antd's Spin holder), and coloured from the theme tokens.
 *
 * - ArcSpinner: a thin two-colour arc, for buttons, toasts and inline status.
 * - BarsLoader: five bars rising in turn, for tables and panels being filled.
 * - GridLoader: a 3×3 grid lighting up in a wave, for an NCC run in progress.
 */

import type { CSSProperties } from "react";

type LoaderProps = { className?: string; style?: CSSProperties; label?: string };

function cx(...names: (string | undefined)[]) {
  return names.filter(Boolean).join(" ");
}

export function ArcSpinner({ className, style, label }: LoaderProps) {
  return (
    <span
      className={cx("anticon ui-arc-spinner", className)}
      style={style}
      role={label ? "status" : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
    />
  );
}

export function BarsLoader({ className, style, label }: LoaderProps) {
  return (
    <span
      className={cx("ui-bars-loader", className)}
      style={style}
      role={label ? "status" : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
    >
      <i />
      <i />
      <i />
      <i />
      <i />
    </span>
  );
}

export function GridLoader({ className, style, label }: LoaderProps) {
  return (
    <span
      className={cx("ui-grid-loader", className)}
      style={style}
      role={label ? "status" : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
    >
      {Array.from({ length: 9 }, (_, i) => (
        <i key={i} />
      ))}
    </span>
  );
}
