# UI/UX and data consumption

Method: code review of `frontend/src` plus a live walk-through of the
production build served by `ncc-ui-server` (token mode, fresh install, no run
history) at 1366×850 and 375×812. No user research or analytics were
available; recommendations are ranked by expected user value and risk. The UI
does not need a redesign: the information architecture (Dashboard → Insights
→ Settings sections) is coherent, and the fixes below are targeted.

## What works

* **Clear empty states with next actions:** "No alerts yet — Start a run from
  Settings → Runs. Add clusters or Prism Central under Config first" with
  *Open Config* / *Open Runs* buttons.
* **Freshness on the Dashboard:** run time as a relative timestamp with the
  absolute time in a tooltip, plus the last runtime error.
* **Shareable state:** filters live in the URL (`useUrlQueryState`, "Copy
  link") and preferences in local storage; one shared poller for active runs.
* **Performance hygiene:** route- and section-level code splitting; Monaco
  loads only in the config editor.
* **Accessibility basics:** reduced-motion handling on the login/404 pages,
  51 explicit `aria-label`/`role` usages, a dark theme with readable contrast in
  the reviewed screens, and a responsive layout (no horizontal page scroll at
  375 px in the reviewed views).
* **Errors from the API are surfaced verbatim** (`ApiError` keeps status and
  data), which pairs well with the new actionable 400 messages for run
  arguments.

## Implemented

| ID | Problem | Change | Verified |
| --- | --- | --- | --- |
| UX-01 | **Health pill showed a red "0.0%" with no data** (fresh install, or a run that produced no NCC results), which reads as "every check failed". Zero vs unavailable must not look the same. | `healthPillValue()` returns no value without completed checks; the pill shows a neutral "—", an `aria-label` "Health: no completed checks yet", and a tooltip. | Unit test; live: accessibility tree shows the new label, the pill renders "—" |
| UX-02 | **First-time setup steps broke words** ("Connectio/n", "Configuratio/n") at desktop width: four horizontal steps in a 760 px modal left ~75 px per title. | `titlePlacement="vertical"` on the Steps. | Live at 1366 px (every title one line, 24 px tall) and 375 px (vertical list, no overflow) |
| UX-03 | **Runs page and API Explorer suggested arguments that make every run fail** (`--no-html --output-dir outputfiles`). | Placeholder and samples use real flags; the API returns an actionable 400 for retired names. | Live trigger through the UI proxy |
| UX-04 | **False "timed out" errors** for preflight and backup/restore operations that were still running. | Client/proxy/server deadlines aligned. | Unit tests (Go and Vitest) |

## Recommendations (not implemented)

| ID | Recommendation | User value | Effort | Priority |
| --- | --- | --- | --- | --- |
| UX-05 | Replace the free-text "Additional flags" field with a small builder (chips for `--outputs`, `--severity-filter`, `--dry-run`, quiet hours) fed by a `GET /api/v1/runs/extra-args` list generated from `extraArgsAllowed`, keeping free text as an advanced option | Fewer failed runs, discoverability | M | P2 |
| UX-06 | Per-cluster freshness: in the cluster table, show "last successful data" and badge clusters whose latest run failed, so stale findings are not read as current | Trust in data | M | P2 |
| UX-07 | Stale-schedule banner on the Dashboard when `now − last success > 2 × schedule interval` | Detects silent schedule failure | S | P2 |
| UX-08 | The Runs page requests `/runs/config-preference`, which is 404 in token-only mode (console error each load). Skip the call when `local_enabled` is false | Clean console, fewer support questions | S | P3 |
| UX-09 | First-time setup appears on every visit until dismissed per browser; offer "Don't show again for this install" stored server-side | Less friction for multi-admin teams | S | P3 |
| UX-10 | Accessibility pass with axe-core in Vitest on Dashboard, Insights and Settings → Access (forms, tables, modals), and keyboard-only walkthrough of Trigger → Runs → Report | WCAG 2.2 AA confidence | M | P2 |
| UX-11 | Charts (trend/SLO): provide a table toggle or `aria-describedby` summary for each chart | Screen-reader access | S | P3 |

## WCAG 2.2 AA status

Not formally audited. Spot checks: semantic buttons/links from Ant Design,
visible focus rings (Ant Design defaults), labelled form fields in the reviewed
forms, status not conveyed by color alone on severity tags (text labels
present). Gaps to check in UX-10: chart alternatives, focus order in
multi-step modals, and contrast of muted secondary text in the dark theme.

## v2.4.0 re-audit (2026-11-19, HEAD ea76376c + uncommitted changes)

Phase 7. Evidence: 396 frontend tests and type-check pass; production build.

* No live browser walkthrough, responsive check or keyboard pass was done.
* **UX-10 blocked:** `axe-core` is not installed and cannot be added here (no npm). A regex scan for icon-only buttons without an accessible name produced false positives (buttons with text children) and is not evidence either way.
* To do with npm available: add `axe-core` (or `vitest-axe`), run it on each page test, then a keyboard walkthrough against WCAG 2.2 AA.

Update 2026-11-19: UX-10 partly done. `axe-core` 4.14.1 is now a dev dependency and the Login (three variants) and Overview (empty and populated) pages pass the WCAG A/AA and best-practice rules, excluding colour contrast. Not covered: Findings, Runs, Settings, Cluster, Integrations pages; colour contrast; keyboard walkthrough; screen-reader checks. The earlier regex scan for icon-only buttons is withdrawn (false positives).

### Live browser accessibility pass, 2026-11-19

Method: the built frontend served by the real UI server against a locally running API, signed in as a throwaway admin, axe-core 4.14.1 injected in the browser (WCAG 2.0 to 2.2 A and AA plus best-practice rules, **including colour contrast**, which the jsdom tests cannot check), scanning 16 routes in each of the three themes (dark, light, IT Professional) after each fix. The pages had no data (no clusters), so tables, charts and populated states were not scanned.

Found on the first scan (dark theme, all pages unless noted) and fixed:

* **Colour contrast (serious):** selected sidebar item text 2.4:1 (needs 4.5), active tab label 3.7:1, empty-state title 2.2:1, and in the light theme the gold/warning tag text 2.75:1 (including the role badge). Fixes in `theme.tsx` (menu and tab accent text per theme), `styles.css`.
* **Label in name (serious, WCAG 2.5.3):** the visible text of the brand link, the setup button and the severity filter buttons was glued together ("FAIL0", "Setup0/4"), so their accessible names did not contain it. Added the missing spaces and aligned the names; integration cards now take their name from their content.
* **Invalid ARIA:** the API health button carried `role="status"`; the live region is now a separate visually hidden element.
* **Heading order:** panel titles were `h3` straight under the page `h1`; now `h2` (also the Notifications groups, Schedule and Linked files headings); 404 page had an empty `h1`.
* **Target size (WCAG 2.2, 2.5.8):** the show/hide password icons were 14 px; now at least 24 px.
* **Keyboard:** the integration code blocks scrolled but were not focusable (WCAG 2.1.1); added `tabIndex`. The integration progress bar had no accessible name.

Result after fixes: 0 violations on all 16 routes in all three themes. A keyboard walkthrough of the first screen: a "Skip to content" link is the first stop, then the brand link, sidebar, header buttons and page actions in a sensible order, each with a visible focus outline. Not covered: screen-reader testing, populated-data pages, reflow/zoom at 400%, reduced-motion. Tests added/updated: `Panel.a11y.test.tsx`; existing tests adjusted to the new heading levels and link names.
