# Web UI design system

How the dashboard in [`frontend/`](../../frontend) is organised and the rules
new pages follow. The code is the source of truth; this page explains the
intent so changes stay consistent.

## Navigation

A collapsible left sidebar groups the pages by how often they are used:

| Group | Page | Route |
| --- | --- | --- |
| Daily work | Overview | `/overview` |
| | Findings | `/findings` |
| | Insights | `/insights` |
| | Integrations | `/integrations`, `/integrations/:id` |
| | Runs, run detail | `/runs`, `/runs/:id` |
| | Cluster | `/clusters/:key` |
| Settings | Clusters & config, Schedule, Notifications, Access, Features | `/settings/:section` |
| System (admin) | Status, Health & repair, Updates & backups, Logs | `/system/:section` |

Viewers see Overview, Findings, Runs (read-only) and Insights. Operators add
the settings they may change. Admins see everything. Old links such as
`/settings?tab=runs` redirect to the new routes.

## Tokens and themes

- Three themes: dark (default), light and IT Professional. They differ only in
  surfaces, text greys and status shades; accent, radii, type scale and
  component shapes are shared.
- All tokens live in [`src/theme.tsx`](../../frontend/src/theme.tsx) (Ant Design
  tokens, set once in `SHARED_TOKENS`) and CSS variables in
  [`src/styles.css`](../../frontend/src/styles.css).
- Spacing 4 / 8 / 12 / 16 / 24 / 32, type 12 / 13 / 14 / 16 / 20 / 24 / 30,
  weights 400 and 600, radii 6 / 8 / 12.
- One purple accent per theme (light `#7c3aed`, dark and IT Professional
  `#8b5cf6`). The accent is a fill colour. For accent-coloured **text**
  (selected menu item, active tab) use the per-theme text variant set in
  `theme.tsx` (`#6d28d9` light, `#c4b5fd` dark), which reaches the 4.5:1
  contrast ratio.
- Five status tones (pass, warn, fail, error, info) are used everywhere.
  Ant Design preset tag colours are mapped onto them in `styles.css`.

## Components

Shared building blocks live in [`src/ui/`](../../frontend/src/ui): `PageHeader`,
`Panel`, `StatTile`, `StatusBadge`, `EmptyState` (with a primary action),
`KeyValueList`, `ItemList`, `ListToolbar`, `SaveBar` (sticky "unsaved changes"
bar) and `VirtualList`. The app shell (sidebar and top bar) is in
`src/components/AppShell.tsx`.

Rules, enforced for touched files by `src/designRules.test.ts`:

- No hex colours in components (only in the theme file, Monaco and the two SVG
  intro components) and no layout inline styles.
- No glows or gradients outside the brand mark.
- Every page has loading, empty, error and partial-data states, and an empty
  state says what to do next. Never show a score without data.
- Wording: "Couldn't ..." for errors, "finding" for an NCC result, sentence case.

## Accessibility

The target is WCAG 2.2 AA in all three themes.

- Page title is the single `h1`; panel and section titles are `h2`. Do not skip
  heading levels.
- A control's accessible name must contain its visible text (WCAG 2.5.3). Put
  spaces between adjacent inline elements ("FAIL 0", not "FAIL0"); prefer the
  visible content as the name over a longer `aria-label`.
- Icon-only buttons need an `aria-label`. Pointer targets are at least 24 px
  (the password show/hide icon is enforced in `styles.css`).
- A `button` must not carry `role="status"`; announce changes from a separate
  visually hidden live region.
- Scrollable regions (code blocks) are focusable (`tabIndex={0}`).
- A "Skip to content" link is the first tab stop and every focus indicator is
  visible.

How it is checked: jsdom tests with axe-core for the Login and Overview pages
(`*.a11y.test.tsx`, colour contrast excluded because jsdom has no layout), and
a manual browser pass with axe-core that includes contrast, run on every route
in each theme. The browser pass is not yet automated; see the
[roadmap](../ROADMAP.md).

## Adding or changing a page

1. Build it from `src/ui` components and theme tokens.
2. Add loading, empty, error and partial-data states.
3. Add a Vitest test, and an axe test if the page is new.
4. Check it in all three themes at 1280 px and at tablet width.
5. Update the screenshot in `site/assets/screens` if the page is shown on the
   website.
