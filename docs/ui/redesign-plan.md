# Internal pages: UI/UX redesign plan

Status: done — Phases 0–5 complete (2026-10-31). Scope: every signed-in page of the web UI
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

### Phase 2 — done (2026-10-29)

- **Runs** (`pages/RunsPage.tsx`, `/runs`): New run (operators) opens
  `features/runs/NewRunDrawer.tsx` — config, one-off password, cluster
  subset for group members, run options, advanced flags, preflight result in
  place; closes when the server accepts the run. `ActiveRuns.tsx` shows
  running/queued runs with live output, queue ETA and Cancel (not for
  scheduler-launched runs). History (`RunsTable.tsx`, shared with the
  Overview) with an All / With problems filter; archived rows open their
  detail page by click or Enter. Files from the latest run (unrestricted
  callers only).
- **Run detail** (`pages/RunDetailPage.tsx`, `/runs/:id`): status, duration,
  clusters, failing checks, average health; tabs Clusters (worst first, with
  errors), Checks (non-passing, severity filter), Details (IDs, why clusters
  failed, comparison with the previous run, the run's saved JSON files as
  downloads). Runner output is not stored per run, so it links to System →
  Logs.
- **Viewers** now see Runs (history and detail, read-only); the server
  already allowed it. Starting and cancelling remain operator-only.
- One status vocabulary everywhere (`runStatusTone` over `runStatusDisplay`).
- `RunsSection.tsx` is removed; Settings no longer has a Runs section.
- Left for Phase 5: on phones, tables scroll inside their panel instead of
  becoming cards.

### Phase 3 — done (2026-10-29)

- **Insights** (`pages/InsightsPage.tsx`): run line and four stat tiles
  (health score, checks, clusters at risk, duration), then tabs kept in the
  URL (`?tab=clusters|trends|triage`):
  - Summary — what the run is saying, results bar with legend, top failing
    checks (row opens the check drawer, also by keyboard), review first,
    about this report;
  - Clusters — health ranking, run reliability and versions, SLO panel;
  - Trends — failures over the last 12 runs (column chart), comparison with
    the previous run, new/resolved findings, flaky checks;
  - Triage — triage queue (link to Findings), alert lifecycle, the same
    check on several clusters, KB articles.
- **No data** shows "Nothing to analyse yet" with a link to Runs instead of
  a red "0% Critical".
- **One health formula:** the cluster ranking and `SloPanel` now use
  `weightedHealthScore` (SloPanel ignored warnings and weighted errors like
  failures); the unused `InsightsCards.tsx` (warnings at 2.2) is removed.
- The older report panels (SLO, diff, flaky, lifecycle) keep their markup but
  take the Panel look inside Insights (CSS bridge in styles.css); rebuilding
  them on `src/ui` is left for Phase 5.

### Phase 4 — done (2026-10-29)

- Every Settings/System section opens with a one-line description (from
  `navigation.tsx`) under the top-bar title, and its cards take the Panel
  look (CSS on `.section-page .page-card`).
- **SaveBar** (`src/ui/SaveBar.tsx`): fixed to the bottom of the content
  area (clear of the sidebar, full width on phones) while a form is dirty,
  with Discard and Save. Used by Notifications (replaces the in-card
  "Unsaved changes" alert) and Clusters & config (replaces its banner; Save,
  Revert and Ctrl/⌘+S stay; Save is blocked while the YAML is invalid).
- **System → Status** rebuilt: status line, tiles (API, version, auth,
  platform), server paths with middle-ellipsis (`middleEllipsis`), full path
  on hover, copy button; run activity.
- **Access** in tabs (Users & roles, Sign-in, Cluster groups, API tokens,
  HTTPS), kept in `?view=`; `?focus=users|ldap|sso|tls` (search shortcuts,
  health fixes) open the right tab.
- Duplicate titles: "Schedule" → "Recurring run", Notifications' first card
  → "When to notify".
- Not done here, moved to Phase 5: moving code out of `AccessSection.tsx`
  (3,000 lines; Backups and Updates live there but belong to Maintenance) —
  the tabs solve the overloaded page, the file move is a pure refactor; Schedule
  has no dirty tracking, so it keeps its own Save button; stat tiles stack
  one per row on phones.

### Phase 5 — done (2026-10-31)

- **Contrast (UX-10):** measured every text/background pair in the three
  themes. Badges failed AA in light (2.7–4.1:1), fail/neutral badges in dark
  and IT Pro, and light tertiary text. Fix: status tokens get text variants
  (`--ui-ok-text` … `--ui-neutral-text`) used for badge text, scores and
  "all clear" text, while `--ui-ok` etc. stay as fills and dots; tertiary
  text darkened. All pairs now ≥4.5:1 (large stat values ≥3:1).
- **Keyboard:** "Skip to content" link first in the shell (focuses
  `#main-content`); visible focus ring on links, `tabIndex=0` rows and
  non-antd buttons.
- **Phones:** Overview stat tiles two per row with tighter padding; no
  horizontal page scroll at 375px.
- **Schedule save bar:** the saved schedule is fetched on mount as a
  baseline (the form keeps its draft across visits, so it is not
  overwritten); the bar shows when a save would change it. No bar until a
  schedule has been saved (`updated_at` empty) — Install now covers that.
- **Dead code removed:** `UxModernizationSection`, `ClusterHealthWidget`,
  `Pagination`, `PerClusterReportsList`, `PolicyStatus` (none imported) and
  the old header/hero CSS (`.app-header`, `.nav-pill`, `.dashboard-hero`,
  `.finding-card`, …).
- **Guard test** (`src/designRules.test.ts`): no hex colours and only
  width/height/custom-property inline styles in `src/ui`, `features/runs`,
  the shell and the redesigned pages.
- **Deferred, not blocking:**
  - moving Backups/Updates out of `AccessSection.tsx` into a Maintenance
    file — a pure refactor with no user-visible change;
  - phone tables as cards (they scroll inside their panel);
  - rebuilding the older Insights panels (SLO, diff, flaky, lifecycle) on
    `src/ui` — they already take the Panel look through the CSS bridge;
  - extending the guard test to the older Settings sections.

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
