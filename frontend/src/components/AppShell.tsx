import { useEffect, useState, type ReactNode } from "react";
import { Button, Drawer, Grid, Menu, type MenuProps } from "antd";
import { MenuFoldOutlined, MenuOutlined, MenuUnfoldOutlined } from "@ant-design/icons";
import { Link, useLocation, useNavigate } from "react-router";
import { useLocalStorageState } from "../hooks/useLocalStorageState";
import { PAGES, SECTIONS, canOpenSection, pageTitle, type Viewer } from "../navigation";

type MenuItem = Required<MenuProps>["items"][number];

function navItems(viewer: Viewer): MenuItem[] {
  const sections = SECTIONS.filter((s) => canOpenSection(s, viewer));
  const main: MenuItem[] = [
    ...PAGES.map((p) => ({ key: p.path, icon: p.icon, label: p.label })),
    ...sections.filter((s) => s.group === "main").map((s) => ({ key: s.path, icon: s.icon, label: s.label })),
  ];
  const groups: MenuItem[] = [];
  for (const [group, label] of [
    ["settings", "Settings"],
    ["system", "System"],
  ] as const) {
    const children = sections
      .filter((s) => s.group === group)
      .map((s) => ({ key: s.path, icon: s.icon, label: s.label }));
    if (children.length) groups.push({ type: "group", key: `g-${group}`, label, children });
  }
  return [...main, ...groups];
}

/** Menu key for the current location: the page/section path (a run's detail page belongs to Runs). */
function selectedKey(pathname: string): string {
  const p = pathname.replace(/\/+$/, "") || "/";
  return p.startsWith("/runs/") ? "/runs" : p;
}

function Brand({ brand, onNavigate }: { brand?: ReactNode; onNavigate?: () => void }) {
  return (
    <Link to="/" className="ui-sidebar-brand" aria-label="NCC Orchestrator home" onClick={onNavigate}>
      <img src="/logo.svg" alt="" />
      <span className="ui-sidebar-brand-name">NCC Orchestrator</span>
      {brand}
    </Link>
  );
}

/**
 * Signed-in frame: sidebar navigation (collapsible to icons, a drawer on
 * phones) and a slim top bar with the page title and global actions.
 * Destinations come from navigation.tsx and are filtered by role.
 */
export function AppShell({
  viewer,
  brand,
  actions,
  children,
}: {
  viewer: Viewer;
  /** Small element beside the product name (version tag). */
  brand?: ReactNode;
  /** Right side of the top bar. */
  actions?: ReactNode;
  children: ReactNode;
}) {
  const location = useLocation();
  const navigate = useNavigate();
  const screens = Grid.useBreakpoint();
  const isPhone = screens.md === false;
  // Below lg the sidebar starts collapsed; a choice the user makes sticks.
  const [pinned, setPinned] = useLocalStorageState<"open" | "collapsed" | "">("ui.sidebar", "");
  const collapsed = pinned ? pinned === "collapsed" : screens.lg === false;
  const [drawerOpen, setDrawerOpen] = useState(false);

  useEffect(() => {
    setDrawerOpen(false);
  }, [location.pathname]);

  const items = navItems(viewer);
  const key = selectedKey(location.pathname);
  const onSelect: MenuProps["onClick"] = ({ key: k }) => navigate(String(k));
  const menu = (inlineCollapsed: boolean) => (
    <Menu
      mode="inline"
      items={items}
      selectedKeys={[key]}
      onClick={onSelect}
      inlineCollapsed={inlineCollapsed}
    />
  );
  const title = pageTitle(location.pathname);

  return (
    <div className="ui-shell">
      {/* First stop for keyboard users: skip the sidebar and top bar. */}
      <a href="#main-content" className="ui-skip-link">
        Skip to content
      </a>
      <aside className={`ui-sidebar${collapsed ? " is-collapsed" : ""}`} aria-label="Primary">
        <Brand brand={brand} />
        <nav className="ui-sidebar-nav">{menu(collapsed)}</nav>
        <div className="ui-sidebar-foot">
          <Button
            type="text"
            icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
            onClick={() => setPinned(collapsed ? "open" : "collapsed")}
            aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {collapsed ? null : "Collapse"}
          </Button>
        </div>
      </aside>

      {isPhone ? (
        <Drawer
          className="ui-nav-drawer"
          placement="left"
          open={drawerOpen}
          onClose={() => setDrawerOpen(false)}
          size={264}
          closable={false}
          title={null}
        >
          <Brand brand={brand} onNavigate={() => setDrawerOpen(false)} />
          <nav className="ui-sidebar-nav" aria-label="Primary">
            {menu(false)}
          </nav>
        </Drawer>
      ) : null}

      <div className="ui-main">
        <header className="ui-topbar">
          <Button
            type="text"
            className="ui-topbar-menu-btn"
            icon={<MenuOutlined />}
            aria-label="Open navigation"
            onClick={() => setDrawerOpen(true)}
          />
          <h1 className="ui-topbar-title">{title}</h1>
          <div className="ui-topbar-actions">{actions}</div>
        </header>
        <main className="ui-content" id="main-content" tabIndex={-1}>
          {children}
        </main>
      </div>
    </div>
  );
}
