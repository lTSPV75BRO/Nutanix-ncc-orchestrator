# NCC Orchestrator — v2.3.0

**Release status:** Release candidate. Planned date: set when the `v2.3.0` tag is pushed (the release workflow publishes on tag push).

> **Affiliation:** This is an independent open-source project. It is not
> affiliated with or endorsed by Nutanix, Inc. The project is MIT licensed.

v2.3.0 follows a full application audit (findings and evidence in
[`docs/audit/`](../audit/executive-summary.md)). It replaces the first-time
setup modal with a setup checklist derived from the install's real state,
redesigns the sign-in screens, makes the user store safe with several API
replicas, makes restores roll back on failure, fixes a set of Windows-only
defects, and adds request IDs, per-route metrics, and a readiness endpoint.
It also adds dev / staging / prod environments with their own schedules,
detailed logs from every process with a Logs page, and OpenID Connect
sign-in.

**Before upgrading:** the anonymous admin self-reset from the login page is
now **off by default**. See [Upgrade](#upgrade).

## New

### Environments: dev / staging / prod

One main config, plus a runner file per environment in
`configs/environments/<name>.yaml`. An environment may hold only a
description and a `runner:` section (clusters, credential references,
execution and automation). It is laid over the main config at run time, so
the server, storage, sign-in and notification settings always come from the
main config and an environment can't break the stack.

- Manage environments in Settings → Clusters & config → Environments. Start
  from the main config's runner, an empty file, a copy of another
  environment, or an older full config (only its runner part is kept).
- New run: pick the main config or an environment, then all of its clusters
  or a subset. Any operator may run any environment; cluster groups still
  limit restricted users to their clusters.
- Every run is tagged with its environment. Runs, Findings and the Overview
  cluster list filter by it.
- A schedule per environment (for example prod daily at 02:00, dev weekly),
  run by the API server through the normal run engine. The OS-level schedule
  still runs the main config.
- API: `GET /api/v1/environments`, `GET/PUT/DELETE
  /api/v1/settings/environments/<name>`, `GET/PUT
  /api/v1/environment-schedules`; `environment` on `/runs/trigger` and
  `/runs/preflight`.

### Detailed logs, and a Logs page

Every process now says what it did, to what, for whom, and why it failed:

- **API server:** rotated JSON log `logs/ncc-api.log` with every request
  (request ID, user, status, duration, error), each run from accepted to
  finished (process ID, command, config, output tail), self-heal, schedules,
  backups, notification delivery, audit forwarding, user-store reloads, the
  audit trail and panics with their stack.
- **Runner:** one line per cluster step with run ID, environment and timings.
- **Notifications:** every attempt, retry and final failure.
- **UI server:** failed, slow and write requests, with the API's request ID.
- **MCP server:** each tool call and its outcome (never the arguments).

Passwords, tokens, API keys and webhook URL paths are removed before a line
is written. **System → Logs** shows each log newest first, with level
filters, search, per-line details ("all lines for this run / request /
cluster"), raw view, live refresh and download. Everyone sees the runner
log; admins see every log and can change the API server's log level until
the next restart. New settings: `--log-level`, `--log-format`,
`--server-log-path` (`NCC_LOG_LEVEL`, `NCC_LOG_FORMAT`,
`NCC_SERVER_LOG_PATH`).

Scrollbars now follow the theme, and role tags are back to admin gold,
operator blue, viewer plain everywhere.

### Day-to-day conveniences

- **Support bundle in the browser.** Health & repair → Collect support bundle
  now downloads the file, so no shell access to the server is needed on a
  VM, Windows, Docker or Kubernetes install. The bundle now carries every
  process log (API server, runner, scheduler, UI and supervisor) and the
  environment files, with secrets redacted.
- **Re-run failed clusters.** A run with clusters that couldn't be checked
  shows "Re-run N failed clusters" on its page. New run opens with the same
  environment and only those clusters.
- **Logs for this run.** A run's page links straight to System → Logs,
  filtered to that run.

### Setup checklist (replaces the first-time setup modal)

The old modal kept its progress in each browser, so every new browser saw
"New install detected" on established installs; it was shown to viewers who
cannot open Settings; its steps were manual "mark as done" boxes; and its
restore detection looked for the wrong audit action.

The checklist (`GET/PUT /api/v1/onboarding`) is computed on the server from
the install's real state and ticks itself off:

- Required: clusters configured (sample values do not count) with a username;
  a run that reached at least one cluster; a recurring schedule; scheduled
  backups.
- Optional: notifications; team access (more accounts, AD/LDAP, or SSO).

It shows as a Dashboard card and a **Setup n/m** header button with a drawer.
Each step links to its Settings tab; operators see "Ask an administrator" for
admin-only steps. An admin's **Hide** applies to the whole install. Admins can
**Skip** a step that does not apply (undoable) or **Finish setup** to retire
the checklist with steps left; reopen it from the user menu → *Setup
checklist*.

### Redesigned sign-in and first-password screens

- New, minimal look in the colours of the selected app theme (dark, light or
  IT Professional): a product pane on the left ("Automate Nutanix cluster
  health checks", parallel execution, analysis, and unified operations) with
  a quiet wave ribbon in the theme accent that flows slowly from left to
  right, and the sign-in form on the right. Responsive down to phone width;
  the wave stays still when the operating system asks for reduced motion.
- The sign-in form now appears on installs that sign in only through Active
  Directory/LDAP (it was hidden without local accounts).
- Caps Lock warning; lockouts shown as a warning with the server's retry time;
  a notice after sign-out or when a session ends.
- First-run guidance with a copyable Kubernetes command for the initial
  password.
- **Restore from a backup** (sign-in page and first-password page) now accepts
  the default encrypted `.tar.gz.enc` archives and has a passphrase field.
  Before, the file picker hid them and no passphrase could be given.
- The new-password form validates the confirmation as you type.

### New navigation

The header pills and the 13-tab Settings page are replaced by a sidebar:

- **Overview**, **Insights** and **Runs** at the top (Runs used to be a
  Settings tab).
- **Settings:** Clusters & config, Schedule, Notifications, Access, Features.
- **System:** Status, Health, Logs, Audit log, Maintenance, Developer.

The sidebar collapses to icons (the choice is remembered) and opens as a
drawer on phones. The top bar shows the page title, search (Ctrl+K / ⌘K),
setup progress, API status, **Trigger** and your user menu; the theme picker
is now in the user menu (or beside it when sign-in is off). Every section has
its own address, so it can be bookmarked; old `/settings?tab=…` links still
work. Each role sees the same sections as before. This is the first step of
a wider redesign of the signed-in pages
([`docs/ui/redesign-plan.md`](../ui/redesign-plan.md)).

### Overview and Findings

The Dashboard is now two pages:

- **Overview** (the home page) shows whether everything is healthy at a
  glance: health score, checks needing attention, clusters checked, and when
  the last run finished; checks by result; each cluster's status, worst
  first; and the last five runs. Regressions and policy-gate violations show
  as banners, and setup progress as a slim banner with the next step. Every
  number links to the matching findings.
- **Findings** is the alerts table, with the same filters, shareable links,
  and acknowledge/resolve as before. The severity counts above it now cover
  the whole run (they used to drop to 0 for severities you filtered out) and
  can be clicked to filter.

Links shared from the old Dashboard open the matching Findings view.

### Runs and run details

- **New run** opens a side panel with the config file, an optional one-off
  password, run options, and **Run preflight**, whose result appears right
  there. The panel closes once the run is accepted.
- **In progress** shows each running run's live output, queued runs with
  their place in line and an estimated wait, and Cancel.
- **History** lists every run with its status, trigger, duration, clusters
  and findings; switch to **With problems** to see only partial or failed
  runs.
- Click a run to open its **details**: per-cluster results (unreachable and
  failing clusters first, with the error), every check that did not pass,
  how it compares with the previous run, and the run's saved files to
  download.

Viewers can now browse runs and their details; starting and cancelling runs
still needs the operator role.

### Insights in tabs

Insights was one long page; it now opens with a summary (health score,
checks, clusters at risk, run duration) and four tabs:

- **Summary** — what the run is saying, a results breakdown, the most
  frequent failing checks (select one for its clusters and output), and the
  findings to review first.
- **Clusters** — clusters from worst to best health, why clusters could not
  be checked, NCC/AOS versions, and SLOs.
- **Trends** — failures across the last runs, the comparison with the
  previous run, new and resolved findings, and flaky checks.
- **Triage** — how critical alerts are being handled, alert lifecycle,
  checks failing on several clusters, and knowledge-base articles.

The tab is part of the address, so a view can be bookmarked or shared. Before
the first run, Insights says there is nothing to analyse yet instead of
showing "0% Critical". The health score is now calculated the same way on
every page (the SLO panel used a different formula).

### Settings and System

- Every section starts with a short line saying what it is for, and all
  sections share one look.
- **Unsaved changes** on Notifications and Clusters & config show a bar at
  the bottom of the screen with **Discard** and **Save**, so the Save button
  is always in reach. Config still saves with Ctrl/⌘+S and will not save
  YAML that has an error.
- **System → Status** shows API health, version, authentication and
  platform, and the server's paths with the start and the file name visible
  (full path on hover, copy button).
- **Access** is now in tabs: Users & roles, Sign-in (directory, SSO, session
  length), Cluster groups, API tokens and HTTPS. Searching for "TLS",
  "users" or "LDAP" opens the right tab.
- **Schedule** shows the same bar when the form differs from the saved
  schedule; Discard puts the saved values back.

### Accessibility and phones

- Status badges, scores and secondary text are readable in every theme
  (WCAG AA contrast, 4.5:1); the light theme's badges were too faint before.
- Keyboard users get a **Skip to content** link on the first Tab press and a
  visible focus ring on links and controls.
- On phones the Overview tiles sit two to a row instead of one long column.

### Ready for large fleets

Tested end to end with **7,000 clusters and 210,000 findings per run**
(details and sizing in [`docs/SCALING.md`](../SCALING.md)):

- Findings now appear for very large runs. Above 20,000 findings they were
  missing from Findings, Overview and Insights.
- Pages load in about 2 seconds at that size, and searching findings responds
  as you type (it used to freeze the page).
- The server answers report and alert requests in well under a second (up to
  32 seconds before), and records a run's alert history in about a second
  (37 seconds before).
- `max-parallel` can go up to 1,000 clusters at once (was 100), and now
  defaults to `0` (automatic: one per cluster, up to 500). With ~3-minute NCC
  runs, 7,000 clusters take about 45 minutes instead of about 90 hours at the
  old default of 4. If Prism pushes back (HTTP 429/503), the run slows down
  on its own.
- Writing reports and sending per-cluster notifications no longer hold up
  other clusters, and status checks notice a finished NCC run sooner.
- Clusters that time out or hit a network error are tried again at the end of
  the run (`retry-failed-clusters`, default 1).
- A long run that was interrupted can pick up where it stopped:
  `ncc-orchestrator --resume` reuses the clusters already finished.
- **Upgrade note:** if your config sets `max-parallel: 4` explicitly, that
  value still applies; set `0` (or remove it) to get the automatic size.
- Kubernetes: the api-server memory limit is now 2Gi. If you set your own
  limits, allow about 1 GiB for the api-server at 7,000 clusters.

### More switches in Settings → Features

Features is now grouped into **Views**, **Actions** and **Sign-in**, with
four new switches. All are on after upgrading, and the server refuses what
is switched off rather than only hiding it:

| Switch | When off |
| ------ | -------- |
| Alert handling | Findings is read-only: no Acknowledge, Resolve or Reopen. Earlier marks stay visible. |
| Automatic healing | System → Health & repair reports problems but has no Fix buttons. |
| Update checks | Nothing is fetched from the release server and in-app updates are refused. |
| Opening title | The animated title before sign-in is skipped for everyone. |

### Findings on laptop screens

- The alert table fits a 14" MacBook Pro (1512px) without scrolling
  sideways, and shows about twice as many rows: filters are one row, and
  severity is picked from the totals above them.
- **Full page** fills the window again and hides the sidebar.
- Labels across Settings, System and Insights use the same sentence case.

### Opening title

When the site is opened, the name "NCC Orchestrator" draws itself in outline
and fills in while the sign-in page behind it shows through frosted; the
frost then smoothly clears (about four seconds in all). It plays once per
browser tab, not on every reload. Click anywhere, press Escape, Enter or
Space, or use **Skip** to go straight to sign-in. It is not shown when the
operating system asks for reduced motion.

The title uses the StrokeText component from React Bits, which needs the new
`gsap` 3.15.0 dependency (GSAP's Standard "no charge" license, which allows
commercial use). GSAP loads only when the title plays, so the main bundle does
not grow.

### Run options builder

`GET /api/v1/runs/extra-args` publishes the allowed run options with labels
and allowed values. The Runs page turns them into controls (report formats,
severities, dry run, and advanced options) and shows exactly what will be sent.
Values outside a flag's choices (e.g. `--outputs pdf`) are rejected before
the run starts.

### Operability

- **Readiness:** `GET /api/v1/health/ready` (public; check names only) checks
  that the user store is readable, the runner binary is executable, and the
  output directory is writable. Kubernetes and Helm readiness probes use it;
  liveness stays on `/api/v1/health`.
- **Request IDs:** every API response carries `X-Request-ID` (the UI server
  mints one per request). It is recorded in audit entries, returned as
  `request_id` in error bodies, and shown in UI error toasts.
- **HTTP metrics:** `ncc_http_requests_total{route,method,code}` and
  `ncc_http_request_duration_seconds{route}` on `/metrics`.
- **Graceful shutdown:** the API and UI servers drain in-flight requests for
  up to 20 s on SIGTERM/SIGINT.

### Sign in with OpenID Connect

Settings → Access → Sign-in now offers **OpenID Connect** next to SAML and
LDAP: enter your provider's issuer URL, the client ID and secret, and register
the redirect URI it shows. Works with Microsoft Entra ID, Okta, Google
Workspace, Keycloak and other standard providers. Groups from the provider map
to roles, and you can refuse anyone outside the mapped groups. The sign-in page
gets a "Sign in with <provider>" button, and MFA or conditional access set at
the provider apply automatically. Setup notes per provider are in
`docs/SECURITY_AND_TRUST.md`.

## Bug fixes

- **User-store changes could be lost with several API replicas** (Helm default
  2): a write on one replica could undo a user deletion, demotion, token
  revocation, or sign-out-everywhere made on another. Every change now reloads
  the stored document first (file lock for the file store); the Kubernetes
  Secret store saves with a `resourceVersion` precondition and answers
  `409 NCC_API_STORE_CONFLICT` on a concurrent change.
- **Restore is transactional:** a failure part-way rolls every file back
  (including `frontend-dist`, previously deleted up front).
- **Deleting a local account now ends its sessions** (they stayed valid with
  their role until expiry); re-creating the name no longer revives old tokens.
- **Windows:** process liveness always reported "dead" (restore over a running
  stack, duplicate supervisors, forced kills on every stop, scheduled runs
  hidden from Active Runs); the file secrets provider rejected every file;
  post-restore path healing skipped Windows.
- **Run options:** 8 of the 10 previously allowed extra args were not runner
  flags, so runs started with them (including the UI/OpenAPI samples) failed
  with "unknown flag".
- **Long operations** (preflight, backup, restore, support bundle) were cut off
  by 15 s/60 s server timeouts and a 30 s client timeout while still running.
- **SMTP** sends (API and runner) had no timeout; the runner no longer tries
  AUTH against unauthenticated relays.
- Runs page on token-only installs: the config list stayed empty because a
  per-user preference request 404'd.
- Dashboard showed a red "0.0%" health score with no data.
- A malformed webhook URL could panic the notification path.
- **Two admins saving the Kubernetes schedule could overwrite each other.**
  A save from a form opened before someone else's change now gets 409
  (`NCC_API_SCHEDULE_CONFLICT`) and the page reloads the current schedule.
- A self-heal retry of an environment run fell back to the main config; it
  now keeps the run's environment and source.
- Error messages on the Access page read "Could notcreate user" and similar
  (missing space).

## Security

- **Passwords follow NIST SP 800-63B:** at least 15 characters (spaces
  welcome; a few unrelated words work well), and common, repeated, sequential
  or username-based passwords are refused. Existing passwords keep working
  until they are next changed.
- **Dependency vulnerabilities fixed:** vite, vitest, undici and
  source-map-js (build and test tooling; nothing in the shipped binaries or
  images was affected) and golang.org/x/net. Scanners report no open fixable
  vulnerabilities.
- **Compliance documentation:** NIST CSF 2.0 profile, SSDF (SP 800-218)
  mapping, risk register, incident response plan and a vulnerability
  reporting policy, in `docs/compliance/` and `.github/SECURITY.md`. NIST does
  not certify products; these are an evidence-backed self-assessment.
- Anonymous admin self-reset off by default (`--allow-admin-self-reset` to
  opt in).
- Sessions with an unknown or empty role are rejected (were treated as admin).
- Run `--prom-dir` values are confined under the install root.
- The UI server no longer routes API traffic (with the admin token in token
  mode) through `HTTP(S)_PROXY`.
- MCP server: Prism passwords only via `NCC_PASSWORD` (not on the command
  line); `create_schedule` refuses a custom command unless
  `NCC_MCP_ALLOW_SCHEDULE_COMMAND=true`.
- Runner email headers are encoded (no CR/LF header injection from run data).
- **SAML library updated to crewjam/saml v0.6.3 (GO-2026-6909).** The old
  version could crash on a signed assertion without a subject or conditions,
  and read the user name from the wrong XML namespace. A SAML login whose
  assertion names no user is now refused (it used to sign in as a shared
  `saml-user`).
- The first-run admin password is printed to the console only, never to a
  log file.

## CI and dependencies

- Windows `go vet` + `go test` job; advisory full `npm audit`; Dependabot for
  Go modules, npm and GitHub Actions.
- golang.org/x/crypto 0.58.0, x/sys 0.49.0, x/term 0.47.0, go-ldap 3.4.15.
- Latest Go modules: grpc 1.85.0, OpenTelemetry 1.47.0, go-jose 4.1.9,
  grpc-gateway 2.33.0, yaml 3.0.6, pflag 1.1.0, crewjam/saml 0.6.3.
- govulncheck (release build and CI) runs through `tools/vulngate`: it fails
  on any vulnerability the code calls, except advisories reviewed in
  `scripts/govulncheck-accepted.txt` with a reason and a review-by date.
  GO-2026-6909 is listed there because the Go vulnerability database has not
  yet recorded the upstream fix (v0.5.2).

## Upgrade

1. **Admin recovery:** a locked-out sole admin now resets the password on the
   host with `ncc-orchestrator v2-reset-password --user admin` (Kubernetes: run
   it in the API pod), unless you start the API with `--allow-admin-self-reset`
   / `NCC_ALLOW_ADMIN_SELF_RESET=true`.
2. **Kubernetes/Helm:** the API readiness probe moves to
   `/api/v1/health/ready`. The manifests and chart in this release already use
   it; custom manifests should be updated.
3. **Integrations sending run extra args:** only `--outputs`,
   `--severity-filter`, `--dry-run`, `--quiet-hours`, `--maintenance-windows`,
   `--clusters` and `--prom-dir` are accepted; the old names never worked and
   now return 400 with a hint.
4. The user store gains an optional `deleted_accounts` field; older versions
   ignore it.
5. **Password policy:** passwords set from now on need 15+ characters and
   must not be common or contain the username. Scripts that create users
   through the API with shorter passwords must be updated.
6. **Building from source:** the repository root was tidied. The runner is
   built with `go build ./cmd/ncc-orchestrator` (was `go build .`), the
   release build is `bash scripts/binaryGO.txt` (Windows:
   `scripts\binaryGO.cmd`), the reference config is
   `examples/example_config.yaml`, source-build Dockerfiles are in
   `build/docker/`, and release notes are in `docs/releases/`. Release assets,
   installed file names and the install layout do not change.
7. **SAML with encrypted assertions:** the updated SAML library no longer
   decrypts assertions that use RSA-1_5 key transport or TripleDES. Switch
   the identity provider to RSA-OAEP with AES before upgrading, or SAML
   sign-in fails. Assertions must also carry a NameID or the configured
   username attribute.

## Rollback

Roll back to v2.2.1 with the in-app rollback point or by redeploying v2.2.1
images/binaries. The user store and state files stay compatible.

## Known issues and residual risks

- Restored stack binaries are not yet checked against signed release
  checksums (SEC-07).
- Not exercised locally in this cycle: the Windows CI job on GitHub-hosted
  runners and a real multi-replica Kubernetes deployment. The repository
  includes Windows `go vet`/`go test` CI coverage and the Helm chart passes
  `helm lint`; these gates still require their target environments before a
  stable release.

## Readiness validation

The local v2.3.0 readiness pass completed successfully for:

- `go vet ./...`
- `go test -race -count=1 ./...`
- `go build ./...`
- frontend `npm test` (209 tests)
- frontend `npm run build`
- `npm audit` (0 advisories)
- `helm lint helm/ncc-orchestrator`

The release remains a release candidate until the Windows CI job and a
multi-replica Kubernetes smoke deployment are exercised. No tag or release
publication is implied by this document.
