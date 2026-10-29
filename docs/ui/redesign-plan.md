# Internal pages: UI/UX redesign plan

Status: in progress — Phases 0 and 1 done (2026-10-29). Scope: every signed-in page of the web UI
(`frontend/src`). The API stays as it is; this is frontend-only work.

## Where we are

Reviewed live (token-mode test stack, dark theme, 1440×900) and in code.

### Navigation and information architecture

- Three top-level pages (Dashboard, Insights, Settings), but **Settings holds
  13 tabs** that mix day-to-day operations (Runs, Logs, Audit, System Health,
  Maintenance) with configuration (Config, Schedule, Notifications, Access,
  Features) and developer tools (API Explorer, saved reports, run files). The
  tab bar overflows into "…" at 1440 px.
- **Running NCC, the core task, lives in Settings → Runs.** The header has a
  Trigger button, but run history, live progress and run files are three
  different tabs.
- The header carries ten things: logo, version badge, tagline, three nav
  pills, search, Setup n/m, Trigger, API status, theme menu, user menu.

### Pages

- **Dashboard:** the setup checklist fills the whole first screen until every
  step is done. The "Operations Dashboard" summary repeats Insights' numbers,
  and the alerts table — the most useful thing — starts below the fold.
- **Insights:** a single column of about ten stacked cards; with no data it
  is a long list of empty states. The gauge shows a red **"0% Critical"**
  when there is no run yet (the Dashboard was fixed for this; Insights was
  not).
- **Settings:** Config is one long form beside a section list; Connection is
  diagnostics rather than a setting and prints full paths that wrap over
  two lines; save/revert buttons sit at the top, away from the fields.

### Consistency and code

| Measure | Count |
| --- | --- |
| Inline `style={{…}}` in components | 524 |
| Hard-coded hex colours in components (e.g. `#22c55e`) — these ignore the theme | 194 |
| `<Card>` usages (many nested cards) | 119 |
| `styles.css` | 2,655 lines |
| Largest components | `AccessSection` 3,005 lines, `ClusterTable` 1,422, `InsightsPage` 1,299, `ConfigSection` 1,181, `DashboardPage` 1,003 |

Visual language is mixed: purple glows and gradients, pill nav, several card
styles, ad-hoc font sizes. Responsive behaviour is limited (wide tables, tab
overflow), and the accessibility pass (audit item UX-10) is still open.

## Goals

1. **Task-first structure:** the things people do every day (see health, work
   findings, run checks) are one click away; configuration is out of the way.
2. **One design system:** shared tokens and components, every colour from the
   theme, so dark, light and IT Professional all look intentional.
3. **Calm, professional look:** flat surfaces, clear hierarchy, restrained
   accent colour — in line with the new sign-in screen.
4. **Production quality:** loading/empty/error states everywhere, URL-addressable
   state, keyboard access, WCAG 2.1 AA contrast in all three themes, works
   from 1280 px desktops down to tablets (phones: read-only use).

## Proposed structure

A **left sidebar** (collapsible to icons) replaces the header pills; the
header becomes a slim bar with the page title, global search, **Run NCC**,
API status and the user menu (theme moves into the user menu).

| Sidebar | Route | Contents (today's location) |
| --- | --- | --- |
| **Overview** | `/` | Health summary, last run, findings by severity, per-cluster status, recent runs, compact setup progress (Dashboard) |
| **Findings** | `/findings` | Full alerts table with filters, bulk acknowledge, detail drawer (Dashboard alerts table) |
| **Runs** | `/runs`, `/runs/:id` | New run (preflight → options → start), live progress, history, per-run detail with clusters, logs and files (Settings → Runs, Logs, Run files) |
| **Insights** | `/insights` | Trends, clusters, check reliability, SLOs, lifecycle — as sub-tabs (Insights) |
| *Settings* | | |
| Clusters & config | `/settings/config` | (Settings → Config) |
| Schedule | `/settings/schedule` | (Settings → Schedule) |
| Notifications | `/settings/notifications` | (Settings → Notifications) |
| Access | `/settings/access` | Users, SSO/LDAP, tokens, cluster groups (Settings → Access) |
| Features | `/settings/features` | (Settings → Features) |
| *System* (admin) | | |
| Health | `/system/health` | Self-heal, connection, paths (Settings → System Health, Connection) |
| Audit log | `/system/audit` | (Settings → Audit) |
| Maintenance | `/system/maintenance` | Backups, restore, updates (Settings → Maintenance) |
| Developer | `/system/developer` | API explorer, saved reports (Settings → Advanced) |

Old links keep working: `/settings?tab=runs` → `/runs`, and so on.
Viewers see Overview, Findings, Runs (read-only) and Insights; operators add
the Settings they can use today; admins see everything.

## Design system (foundation)

- **Tokens** in one place (`theme.tsx` → antd tokens + CSS variables, per
  theme): spacing 4/8/12/16/24/32; type 12/13/14/16/20/24/30; radius 6/8/12;
  surfaces (page, panel, raised), borders, text (primary/secondary/tertiary),
  accent, and **status colours** (pass/warn/fail/error/info) used everywhere.
- **Shared components** (`src/ui/`): `AppShell` (sidebar + top bar),
  `PageHeader` (title, description, actions), `Panel` (replaces ad-hoc
  cards), `StatTile`, `StatusBadge`, `EmptyState` (with a primary action),
  `DataTable` (sticky header, density, column chooser, loading/empty/error),
  `FilterBar`, `KeyValueList` (truncate + copy), `SaveBar` (sticky "unsaved
  changes" bar), `ConfirmAction`.
- **Rules:** no hex colours or layout inline styles in components (a lint
  check enforces it for touched files); no glows/gradients outside the brand
  mark; one accent per theme.

## Page designs (summary)

- **Overview:** top row of four stat tiles (health score — or "No runs yet"
  with a Run button —, failing checks, clusters at risk, last run); findings
  by severity that click through to Findings; a cluster grid (status per
  cluster); recent runs; setup progress as a slim banner until complete.
- **Findings:** full-height table, severity/cluster/check filters and saved
  views in the URL, bulk acknowledge, detail drawer (check text, KB link,
  history across runs, affected entities).
- **Runs:** "New run" drawer (preflight result, run options builder, confirm),
  live per-cluster progress, history table, run detail page with tabs
  (Summary · Clusters · Logs · Files).
- **Insights:** sub-tabs instead of one long page; charts share a style; no
  score without data.
- **Settings/System:** a settings sub-nav, one form per page, inline
  validation, sticky save bar at the bottom, Form/YAML toggle kept for config,
  long paths truncated with copy.

## Phases

Each phase lands as its own reviewable change with tests and screenshots in
all three themes.

| # | Phase | Delivers |
| --- | --- | --- |
| 0 | Foundation | Tokens, `src/ui` components, new app shell (sidebar + top bar), new routes with redirects; existing pages render inside it unchanged |
| 1 | Overview + Findings | New Overview; Findings page split out of the Dashboard |
| 2 | Runs | Runs page and run detail; moved out of Settings |
| 3 | Insights | Sub-tabs, shared chart style, empty-data fixes |
| 4 | Settings + System | Settings sub-nav and save bar; System pages; split `AccessSection` |
| 5 | Polish | Responsive pass, accessibility audit (UX-10), loading skeletons, per-route code splitting, final consistency sweep |

## Progress

### Phase 0 — done (2026-10-29)

- `src/navigation.tsx`: one table of sections (route, label, title, icon,
  group, minimum role). Sidebar, routes, legacy redirects and in-app links
  all use it (`sectionHref`). Role rules are unchanged from the old tabs.
- `src/components/AppShell.tsx`: sidebar (collapsible, remembered; collapsed
  by default below 992 px; drawer below 768 px) and top bar (page title,
  search, Setup, API status, Run, user menu with the theme picker).
- Routes: `/runs`, `/settings/{config,schedule,notifications,access,features}`,
  `/system/{status,health,logs,audit,maintenance,developer}`. `/settings?tab=…`
  redirects and keeps `focus`/`dev`/`action`. `SettingsPage` renders one
  section when given `section`.
- Tokens (`--ui-*`) per theme and `src/ui`: `PageHeader`, `Panel`,
  `StatTile`, `StatusBadge`/`toneFor`, `EmptyState`, `KeyValueList`.
- Fixed on the way: System Health sent "backups" problems to Access instead
  of Maintenance.
- Pages inside are unchanged, so some show their old card title under the
  new top-bar title (e.g. "Schedule" twice); each page's phase removes that.

Left for later phases: the old header/pill CSS (`.app-header`, `.nav-pill`,
`.brand-*`) and the unused `UxModernizationSection` are dead and go in Phase
5; `DataTable`, `FilterBar` and `SaveBar` are built with their first page
(Phases 1 and 4). Not checked live: the user menu's Theme submenu (the test
stack with sign-in has an unknown admin password); it is covered only by
type checks.

### Phase 1 — done (2026-10-29)

- **Overview** (`pages/OverviewPage.tsx`, `/`): run line (freshness, when,
  duration, Refresh, View findings); regression and policy-gate banners;
  four stat tiles (health score, need attention, clusters checked, last run);
  "Checks by result" (each severity links to `/findings?sev=…`); "Clusters"
  from the run summary's per-cluster results, worst first, each linking to
  `/findings?clusters=…`; "Recent runs" (last 5). No run yet → one empty
  state with Start a run / Configure clusters. Setup progress is a slim
  `SetupBanner` (next step, All steps drawer, Hide).
- **Findings** (`pages/FindingsPage.tsx`, `/findings`, renamed from
  `DashboardPage` with history kept): filters, sharing, acknowledge/resolve and
  the table are unchanged; the hero card is replaced by a summary bar whose
  severity pills count the whole run and act as filters. Old shared Dashboard
  links (`/?sev=…`, `/?source=…`) redirect to `/findings`.
- `weightedHealthScore` (utils/report) and `freshness`/`formatDuration`
  (utils/freshness) are shared.
- Found for Phase 3: `InsightsCards` weights warnings at 2.2 while the
  Overview/Insights page use 3.5, so the same run can show two health scores.

## Risks and constraints

- Large components (`AccessSection`, `ClusterTable`) need careful splitting;
  behaviour must stay identical (existing tests plus new ones per page).
- No npm on the current dev machine: no new dependencies unless added by hand
  (the gsap pattern); the plan uses antd and what is already installed.
- All work lands on the **`v2.3.0` branch** (decided 2026-10-29), so the
  v2.3.0 release notes and changelog grow with each phase, and each phase
  must leave the branch releasable (tests green, no half-moved pages).

## Decisions

1. Branch: **`v2.3.0`** — decided.
2. Sidebar navigation — decided (2026-10-29).
3. Findings and Runs as their own pages — decided (2026-10-29).
