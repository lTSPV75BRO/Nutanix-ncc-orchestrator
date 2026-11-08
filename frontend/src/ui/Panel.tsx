import type { ReactNode } from "react";
import { TitleIcon } from "./IconTitle";

/**
 * A titled surface for one block of content. Replaces ad-hoc antd Cards: a
 * hairline border, theme surface colour, no shadow or glow. Use `flush` for
 * content that brings its own padding (tables, lists). `icon` shows in an
 * accent tile beside the title.
 */
export function Panel({
  title,
  icon,
  description,
  extra,
  flush = false,
  className,
  children,
  id,
}: {
  title?: ReactNode;
  icon?: ReactNode;
  description?: ReactNode;
  extra?: ReactNode;
  flush?: boolean;
  className?: string;
  children?: ReactNode;
  id?: string;
}) {
  return (
    <section className={`ui-panel${className ? ` ${className}` : ""}`} id={id}>
      {title || extra ? (
        <div className="ui-panel-head">
          {icon && title ? <TitleIcon>{icon}</TitleIcon> : null}
          <div className="ui-panel-head-text">
            {title ? <h3 className="ui-panel-title">{title}</h3> : null}
            {description ? <p className="ui-panel-desc">{description}</p> : null}
          </div>
          {extra ? <div className="ui-panel-extra">{extra}</div> : null}
        </div>
      ) : null}
      <div className={`ui-panel-body${flush ? " is-flush" : ""}`}>{children}</div>
    </section>
  );
}
