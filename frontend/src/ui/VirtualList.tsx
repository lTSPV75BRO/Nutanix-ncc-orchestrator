import { useEffect, useRef, useState, type Key, type ReactNode } from "react";

type VirtualListProps<T> = {
  items: T[];
  /** Fixed height of each row in px; rows must not grow past it. */
  rowHeight: number;
  /** Most the list grows to before it scrolls, in px. */
  maxHeight?: number;
  renderRow: (item: T, index: number) => ReactNode;
  itemKey: (item: T, index: number) => Key;
  /** Rows rendered above and below the visible window. */
  overscan?: number;
  className?: string;
  "aria-label"?: string;
};

/**
 * A scrolling list that renders only the rows in view, so lists of thousands
 * of clusters stay fast. Short lists render in full at their natural height.
 */
export function VirtualList<T>({
  items,
  rowHeight,
  maxHeight = 420,
  renderRow,
  itemKey,
  overscan = 8,
  className,
  "aria-label": ariaLabel,
}: VirtualListProps<T>) {
  const ref = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const total = items.length * rowHeight;
  const height = Math.min(total, maxHeight);
  const windowed = total > maxHeight;

  // A new filter can leave the scroll position past the end of the list.
  useEffect(() => {
    const el = ref.current;
    if (el && el.scrollTop > Math.max(0, total - height)) {
      el.scrollTop = 0;
      setScrollTop(0);
    }
  }, [total, height]);

  const first = windowed ? Math.max(0, Math.floor(scrollTop / rowHeight) - overscan) : 0;
  const last = windowed ? Math.min(items.length, Math.ceil((scrollTop + height) / rowHeight) + overscan) : items.length;

  return (
    <div
      ref={ref}
      className={`ui-virtual-list${windowed ? " is-windowed" : ""}${className ? ` ${className}` : ""}`}
      style={{ ["--vl-max" as string]: `${maxHeight}px` }}
      onScroll={windowed ? (e) => setScrollTop(e.currentTarget.scrollTop) : undefined}
      role="list"
      aria-label={ariaLabel}
      tabIndex={windowed ? 0 : undefined}
    >
      <div className="ui-virtual-list-track" style={{ height: windowed ? total : undefined }}>
        {items.slice(first, last).map((item, i) => {
          const index = first + i;
          return (
            <div
              key={itemKey(item, index)}
              role="listitem"
              className="ui-virtual-list-row"
              style={{ height: rowHeight, ["--vl-top" as string]: `${index * rowHeight}px` }}
            >
              {renderRow(item, index)}
            </div>
          );
        })}
      </div>
    </div>
  );
}
