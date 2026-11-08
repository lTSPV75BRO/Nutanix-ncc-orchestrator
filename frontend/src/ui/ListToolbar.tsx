import { Input, Segmented } from "antd";
import { SearchOutlined } from "@ant-design/icons";

export type ListFilterOption = { value: string; label: string; count?: number };

type ListToolbarProps = {
  search: string;
  onSearch: (value: string) => void;
  placeholder?: string;
  filters?: ListFilterOption[];
  filter?: string;
  onFilter?: (value: string) => void;
  /** Rows after filtering, and before. */
  shown: number;
  total: number;
  noun?: string;
};

/** Search box, optional status filter and a "12 of 7,000" count for a long list. */
export function ListToolbar({ search, onSearch, placeholder, filters, filter, onFilter, shown, total, noun = "clusters" }: ListToolbarProps) {
  return (
    <div className="ui-list-toolbar">
      <Input
        allowClear
        size="small"
        prefix={<SearchOutlined />}
        placeholder={placeholder ?? `Search ${noun}`}
        value={search}
        onChange={(e) => onSearch(e.target.value)}
        aria-label={placeholder ?? `Search ${noun}`}
        className="ui-list-toolbar-search"
      />
      {filters && onFilter ? (
        <Segmented
          size="small"
          value={filter}
          onChange={(v) => onFilter(String(v))}
          options={filters.map((f) => ({
            value: f.value,
            label: f.count === undefined ? f.label : `${f.label} ${f.count.toLocaleString()}`,
          }))}
        />
      ) : null}
      <span className="ui-list-toolbar-count" aria-live="polite">
        {shown === total ? `${total.toLocaleString()} ${noun}` : `${shown.toLocaleString()} of ${total.toLocaleString()}`}
      </span>
    </div>
  );
}

/** Case-insensitive match of `query` against any of `fields`. */
export function matchesSearch(query: string, ...fields: Array<string | undefined>): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return fields.some((f) => (f ?? "").toLowerCase().includes(q));
}
