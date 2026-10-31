import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router";
import { Button, Input, Modal, Typography } from "antd";
import { SearchOutlined } from "@ant-design/icons";
import { sectionHref } from "../navigation";

export type JumpItem = {
  id: string;
  title: string;
  keywords: string;
  to: string;
  hint?: string;
};

export const JUMP_ITEMS: JumpItem[] = [
  { id: "dashboard", title: "Overview", keywords: "home dashboard summary health", to: "/", hint: "Overview" },
  { id: "findings", title: "Findings", keywords: "alerts findings ncc pc fail warn acknowledge", to: "/findings", hint: "Findings" },
  { id: "insights", title: "Insights", keywords: "kb compare trends knowledge", to: "/insights", hint: "Reports" },
  { id: "tls", title: "HTTPS / TLS", keywords: "tls cert certificate https ssl trust fingerprint", to: sectionHref("access", { focus: "tls" }), hint: "Access" },
  { id: "users", title: "Users & roles", keywords: "users accounts rbac admin password access", to: sectionHref("access", { focus: "users" }), hint: "Access" },
  { id: "ldap", title: "LDAP / SSO", keywords: "ldap ad saml oidc openid entra okta sso directory authentication access", to: sectionHref("access", { focus: "ldap" }), hint: "Access" },
  { id: "backup", title: "Backups", keywords: "backup restore snapshot maintenance", to: sectionHref("maintenance", { focus: "backup" }), hint: "Maintenance" },
  { id: "health", title: "System health", keywords: "health diagnostics doctor", to: sectionHref("health", { focus: "health" }), hint: "System" },
  { id: "config", title: "Clusters & config", keywords: "clusters prism pc yaml config", to: sectionHref("config"), hint: "Settings" },
  { id: "schedule", title: "Schedule", keywords: "cron job recurring", to: sectionHref("schedule"), hint: "Settings" },
  { id: "runs", title: "Runs", keywords: "trigger history run start", to: sectionHref("runs"), hint: "Runs" },
  { id: "logs", title: "Logs", keywords: "logs runner api output", to: sectionHref("logs"), hint: "System" },
  { id: "audit", title: "Audit log", keywords: "audit who changed history", to: sectionHref("audit"), hint: "System" },
  { id: "notifications", title: "Notifications", keywords: "email slack webhook", to: sectionHref("notifications"), hint: "Settings" },
  { id: "access", title: "Access", keywords: "login session cookies", to: sectionHref("access"), hint: "Settings" },
  { id: "maintenance", title: "Maintenance", keywords: "update upgrade", to: sectionHref("maintenance"), hint: "System" },
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
 * Global jump palette (⌘K / Ctrl+K) for the pages and Settings/System
 * sections. Items with `?focus=` let the destination scroll the matching card
 * into view.
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
        className="header-icon-btn command-jump-btn"
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
