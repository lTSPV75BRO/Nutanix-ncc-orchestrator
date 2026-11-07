import type { CSSProperties, Key, ReactNode } from "react";
import { Typography } from "antd";

type ItemListProps<T> = {
  items: T[];
  /** The row's content; laid out in a row that wraps, space between. */
  render: (item: T) => ReactNode;
  /** Buttons at the end of the row. */
  actions?: (item: T) => ReactNode;
  itemKey?: (item: T, index: number) => Key;
  bordered?: boolean;
  /** Shown instead of the list when `items` is empty. */
  empty?: ReactNode;
  style?: CSSProperties;
};

/**
 * A short, static list of rows with optional actions, in place of antd's
 * List (deprecated in antd 6.7; its replacement, Listy, is a virtual list
 * meant for long scrolling lists).
 */
export function ItemList<T>({ items, render, actions, itemKey, bordered, empty, style }: ItemListProps<T>) {
  if (!items.length) {
    return empty === undefined ? null : (
      <div className={`ui-item-list-empty${bordered ? " is-bordered" : ""}`} style={style}>
        {typeof empty === "string" ? <Typography.Text type="secondary">{empty}</Typography.Text> : empty}
      </div>
    );
  }
  return (
    <ul className={`ui-item-list${bordered ? " is-bordered" : ""}`} style={style}>
      {items.map((item, i) => (
        <li key={itemKey ? itemKey(item, i) : i} className="ui-item-list-row">
          <div className="ui-item-list-main">{render(item)}</div>
          {actions ? <div className="ui-item-list-actions">{actions(item)}</div> : null}
        </li>
      ))}
    </ul>
  );
}
