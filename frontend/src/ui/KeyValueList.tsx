import type { ReactNode } from "react";
import { Typography } from "antd";

export type KeyValueItem = {
  label: ReactNode;
  value: ReactNode;
  /** Show a copy button for this text. */
  copy?: string;
  /** Render the value in a monospace font (paths, IDs). */
  mono?: boolean;
};

/**
 * Label/value rows (paths, IDs, settings). Long values are truncated on one
 * line, with the full text in a tooltip and a copy button when `copy` is set.
 */
export function KeyValueList({ items }: { items: KeyValueItem[] }) {
  return (
    <dl className="ui-kv">
      {items.map((it, i) => (
        <div key={i} className="ui-kv-row">
          <dt>{it.label}</dt>
          <dd>
            <span className="ui-kv-value">
              <span
                className={`ui-kv-text${it.mono ? " is-mono" : ""}`}
                title={typeof it.value === "string" ? it.value : undefined}
              >
                {it.value}
              </span>
              {it.copy ? <Typography.Text copyable={{ text: it.copy }} /> : null}
            </span>
          </dd>
        </div>
      ))}
    </dl>
  );
}
