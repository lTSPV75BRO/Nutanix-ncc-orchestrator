import type { ReactNode } from "react";
import { InboxOutlined } from "@ant-design/icons";

/** What to show when there is nothing yet — always with the next step to take. */
export function EmptyState({
  title,
  description,
  icon,
  actions,
}: {
  title: ReactNode;
  description?: ReactNode;
  icon?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div className="ui-empty">
      <span className="ui-empty-icon" aria-hidden="true">
        {icon ?? <InboxOutlined />}
      </span>
      <p className="ui-empty-title">{title}</p>
      {description ? <p className="ui-empty-desc">{description}</p> : null}
      {actions ? <div className="ui-empty-actions">{actions}</div> : null}
    </div>
  );
}
