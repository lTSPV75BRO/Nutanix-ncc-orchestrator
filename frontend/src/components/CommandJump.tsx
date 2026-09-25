import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router";
import { Button, Input, Modal, Typography } from "antd";
import { SearchOutlined } from "@ant-design/icons";

export type JumpItem = {
  id: string;
  title: string;
  keywords: string;
  to: string;
  hint?: string;
};

export const JUMP_ITEMS: JumpItem[] = [
  { id: "dashboard", title: "Dashboard", keywords: "home alerts findings ncc pc", to: "/", hint: "Alerts" },
  { id: "insights", title: "Insights", keywords: "kb compare trends knowledge", to: "/insights", hint: "Reports" },
  { id: "tls", title: "HTTPS / TLS", keywords: "tls cert certificate https ssl trust fingerprint", to: "/settings?tab=access&focus=tls", hint: "Access" },
  { id: "users", title: "Users & roles", keywords: "users accounts rbac admin password access", to: "/settings?tab=access&focus=users", hint: "Access" },
  { id: "ldap", title: "LDAP / SSO", keywords: "ldap ad saml sso directory authentication access", to: "/settings?tab=access&focus=ldap", hint: "Access" },
  { id: "backup", title: "Backups", keywords: "backup restore snapshot maintenance", to: "/settings?tab=maintenance&focus=backup", hint: "Maintenance" },
  { id: "health", title: "System Health", keywords: "health diagnostics doctor", to: "/settings?tab=health&focus=health", hint: "Health" },
  { id: "config", title: "Config", keywords: "clusters prism pc yaml", to: "/settings?tab=config", hint: "Settings" },
  { id: "schedule", title: "Schedule", keywords: "cron job recurring", to: "/settings?tab=schedule", hint: "Settings" },
  { id: "runs", title: "Runs", keywords: "trigger history logs", to: "/settings?tab=runs", hint: "Settings" },
  { id: "notifications", title: "Notifications", keywords: "email slack webhook", to: "/settings?tab=notifications", hint: "Settings" },
  { id: "access", title: "Access", keywords: "login session cookies", to: "/settings?tab=access", hint: "Settings" },
  { id: "maintenance", title: "Maintenance", keywords: "update upgrade", to: "/settings?tab=maintenance", hint: "Settings" },
];

export function matchJumpItems(query: string, items: JumpItem[] = JUMP_ITEMS): JumpItem[] {
  const n = query.trim().toLowerCase();
  if (!n) return items;
  return items.filter((it) => `${it.title} ${it.keywords} ${it.hint ?? ""}`.toLowerCase().includes(n));
}

function shortcutLabel(): string {
  if (typeof navigator === "undefined") return "Ctrl+K";
  return /Mac|iPhone|iPad/i.test(navigator.platform || navigator.userAgent) ? "⌘K" : "Ctrl+K";
}

/**
 * Global jump palette (⌘K / Ctrl+K) for buried Settings tabs and the main
 * pages. Selecting an item navigates to `/settings?tab=&focus=` so the
 * destination can scroll the matching card into view.
 */
export function CommandJump() {
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const [active, setActive] = useState(0);
  const matches = useMemo(() => matchJumpItems(q), [q]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) {
        e.preventDefault();
        setOpen((v) => !v);
        setQ("");
        setActive(0);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    setActive(0);
  }, [q, open]);

  const go = (it: JumpItem) => {
    setOpen(false);
    setQ("");
    navigate(it.to);
  };

  return (
    <>
      <Button
        aria-label={`Jump to (${shortcutLabel()})`}
        title={`Jump to (${shortcutLabel()})`}
        icon={<SearchOutlined />}
        className="header-icon-btn"
        onClick={() => {
          setOpen(true);
          setQ("");
          setActive(0);
        }}
      />
      <Modal
        open={open}
        footer={null}
        onCancel={() => setOpen(false)}
        title="Jump to"
        destroyOnHidden
        width={480}
        className="command-jump-modal"
      >
        <Input
          autoFocus
          allowClear
          prefix={<SearchOutlined />}
          placeholder="TLS, users, backup, health…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "ArrowDown") {
              e.preventDefault();
              setActive((i) => Math.min(matches.length - 1, i + 1));
            } else if (e.key === "ArrowUp") {
              e.preventDefault();
              setActive((i) => Math.max(0, i - 1));
            } else if (e.key === "Enter" && matches[active]) {
              e.preventDefault();
              go(matches[active]);
            }
          }}
          aria-label="Jump to search"
        />
        <ul className="command-jump-list" role="listbox" aria-label="Jump destinations">
          {matches.length === 0 ? (
            <li className="command-jump-empty">
              <Typography.Text type="secondary">No matching settings</Typography.Text>
            </li>
          ) : (
            matches.map((it, i) => (
              <li key={it.id}>
                <button
                  type="button"
                  role="option"
                  aria-selected={i === active}
                  className={`command-jump-item${i === active ? " active" : ""}`}
                  onMouseEnter={() => setActive(i)}
                  onClick={() => go(it)}
                >
                  <span>{it.title}</span>
                  {it.hint ? <Typography.Text type="secondary">{it.hint}</Typography.Text> : null}
                </button>
              </li>
            ))
          )}
        </ul>
        <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0", fontSize: 12 }}>
          {shortcutLabel()} opens this from anywhere.
        </Typography.Paragraph>
      </Modal>
    </>
  );
}
