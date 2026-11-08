import type { ReactNode } from "react";

/** An accent-tinted icon tile, as beside panel, card and page titles. */
export function TitleIcon({ children, size }: { children: ReactNode; size?: "lg" }) {
  return (
    <span className={`ui-title-icon${size === "lg" ? " is-lg" : ""}`} aria-hidden="true">
      {children}
    </span>
  );
}

/** A card or section title with its icon tile in front. */
export function IconTitle({ icon, children }: { icon: ReactNode; children: ReactNode }) {
  return (
    <span className="ui-icon-title">
      <TitleIcon>{icon}</TitleIcon>
      <span className="ui-icon-title-text">{children}</span>
    </span>
  );
}
