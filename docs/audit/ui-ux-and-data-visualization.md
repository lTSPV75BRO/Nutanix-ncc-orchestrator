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
