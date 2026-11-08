import { useMemo, useState, type ReactNode } from "react";
import { Input, Popover, Tag } from "antd";
import { SearchOutlined } from "@ant-design/icons";
import { VirtualList } from "./VirtualList";
import { matchesSearch } from "./ListToolbar";

type TagListProps = {
  items: string[];
  /** Tags shown before the rest fold into "+N more". */
  max?: number;
  color?: string;
  icon?: ReactNode;
  /** What the items are, for the "+N more" popover ("clusters"). */
  noun?: string;
};

/**
 * A row of tags that stays short however many items there are: the first few
 * show, the rest open in a searchable, scrolling popover.
 */
export function TagList({ items, max = 6, color, icon, noun = "items" }: TagListProps) {
  const [search, setSearch] = useState("");
  const shown = useMemo(() => items.filter((x) => matchesSearch(search, x)), [items, search]);
  const head = items.slice(0, max);
  const rest = items.length - head.length;
  return (
    <span className="ui-tag-list">
      {head.map((x) => (
        <Tag key={x} color={color} icon={icon}>
          {x}
        </Tag>
      ))}
      {rest > 0 ? (
        <Popover
          trigger="click"
          title={`${items.length.toLocaleString()} ${noun}`}
          onOpenChange={(open) => !open && setSearch("")}
          content={
            <div className="ui-tag-list-popover">
              <Input
                size="small"
                allowClear
                prefix={<SearchOutlined />}
                placeholder={`Search ${noun}`}
                aria-label={`Search ${noun}`}
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
              <VirtualList
                items={shown}
                rowHeight={28}
                maxHeight={280}
                itemKey={(x) => x}
                aria-label={noun}
                renderRow={(x) => <span className="ui-tag-list-row">{x}</span>}
              />
              {shown.length === 0 ? <span className="ui-field-help">No matches</span> : null}
            </div>
          }
        >
          <Tag className="ui-tag-list-more" role="button" tabIndex={0}>
            +{rest.toLocaleString()} more
          </Tag>
        </Popover>
      ) : null}
    </span>
  );
}
