import type { ReactNode } from "react";

/** Page title, one-line description and primary actions, at the top of a page. */
export function PageHeader({
  title,
  description,
  actions,
}: {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <header className="ui-page-header">
      <div className="ui-page-header-text">
        <h2 className="ui-page-header-title">{title}</h2>
        {description ? <p className="ui-page-header-desc">{description}</p> : null}
      </div>
      {actions ? <div className="ui-page-header-actions">{actions}</div> : null}
    </header>
  );
}
