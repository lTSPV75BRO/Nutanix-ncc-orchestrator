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
- Spacing on a 4 px grid (4 / 8 / 12 / 16 / 24 / 32), type 12 / 13 / 14 / 16 /
  20 / 24 / 30, weights 400 and 500 (600 only for big stat figures), radii
  6 / 8 / 12. Labels are in sentence case; no upper-case tracked labels.
- Results and states are a status dot and a word (`StatusBadge`), never a
  tinted pill. The words are the same everywhere: Fail, Error, Warning, Info,
  Pass (`severityLabel`). Stat figures stay in the text colour; their tone is a
  dot beside the label. Title icons are a muted 16 px glyph, not an accent tile.
- No coloured shadows: buttons have none (`primaryShadow: "none"`).
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
- A chart that shows more than a label's worth of numbers also offers a table:
  put `ChartViewToggle` (`src/ui`) in the Panel's `extra` slot and render a
  `Table` for the "table" view. The chart itself keeps a `role="img"` with a
  label that carries the values.
- Dialogs, drawers and menus close with Escape and give focus back to what
  opened them. Opening one from a menu item: focus the menu button first
  (see the user menu in `App.tsx`), because the item is gone when the dialog
  closes.
- Reflow: at 320 CSS px (400% zoom) nothing scrolls sideways except inside a
  table, code block or chart. A flex or grid child that holds a table needs
  `min-width: 0`.
- Do not switch animations off with `animation: none` on antd popups: they wait
  for `animationend` and stay invisible. antd's `motion` token follows
  `prefers-reduced-motion` in `theme.tsx`.

### How it is checked

1. jsdom tests with axe-core for the Login and Overview pages
   (`*.a11y.test.tsx`, colour contrast excluded because jsdom has no layout).
2. A browser suite, `frontend/e2e` (Playwright, `npm run test:a11y`), on the
   **demo build** (fixture data, no backend). CI job `a11y` runs it on every
   push and pull request; it is about 3 minutes.

   | Spec | Checks |
   | --- | --- |
   | `a11y.spec.ts` | axe-core, WCAG 2.0/2.1/2.2 A and AA, including contrast: the sign-in page; every route in `App.tsx` and every section as an admin, and every tab on them; a findings drawer, the personal tokens dialog (lazy loaded), a confirm modal, the user menu, a toast and the trend chart (chart and table view). All three themes. |
   | `reflow.spec.ts` | 320x256 px (400% zoom): no page-level sideways scroll or clipped content outside tables, code and charts; overlays fit. |
   | `keyboard.spec.ts` | Skip link first and working; a visible focus indicator (pixel comparison) on every tab stop of the Overview's first screen; Escape closes drawers, dialogs and menus and focus returns to the opener. |
   | `screenreader.spec.ts` | One `h1` per page, no skipped heading levels on any tab, landmarks, a distinct page title per route, toasts in a live region, charts with a text alternative and a table view. |

   Every page is scanned only after it has shown its data (no skeleton for
   500 ms and a text from the fixture data), so a scan never passes on a
   loading page. `e2e/routes.ts` lists the routes; `src/a11yRoutes.test.ts`
   (Vitest) fails if `App.tsx` or `navigation.tsx` has a route or section that
   it does not list.

   Run it locally (Node 26; the demo build is made for you):

   ```bash
   cd frontend
   npm ci
   A11Y_CHANNEL=msedge npm run test:a11y        # installed Edge, nothing to download
   # or, once:  npx playwright install chromium && npm run test:a11y
   npx playwright test keyboard                 # one spec
   npx playwright test -g "dark /findings"      # one case
   ```

   `A11Y_PORT` changes the preview port (default 5230). Failures list the axe
   rule, the selector and why; `npx playwright show-report` (CI) or the trace in
   `test-results/` shows the page. Known violations we cannot fix yet are listed
   in `EXCLUSIONS` in `e2e/helpers.ts`: one rule, one selector, one page, with a
   reason. Never add a rule without a selector.

3. Still manual: a real screen reader pass (NVDA with Firefox, VoiceOver with
   Safari) on the Overview, Findings and a settings form; reading order and
   what is announced cannot be judged from the DOM. Also the keyboard path
   through every form, and zoom in a real browser at 200% text size.

## Adding or changing a page

1. Build it from `src/ui` components and theme tokens.
2. Add loading, empty, error and partial-data states.
3. Add a Vitest test, and an axe test if the page is new. A new route or
   section must also go into `frontend/e2e/routes.ts` (a Vitest test fails
   until it does); run `npm run test:a11y`.
4. Check it in all three themes at 1280 px and at tablet width.
5. Update the screenshot in `site/assets/screens` if the page is shown on the
   website.
