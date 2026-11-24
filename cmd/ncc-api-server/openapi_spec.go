package main

import (
	"encoding/json"
	"net/http"
	"strings"
)

// routeRequiredRole returns the most restrictive minimum role across a route's
// methods (e.g. a GET+PUT settings route reports "admin" because the PUT is
// admin-only), as a display string ("viewer"/"operator"/"admin").
func routeRequiredRole(path string, methods []string) string {
	max := RoleViewer
	for _, m := range methods {
		isRead := m == http.MethodGet || m == http.MethodHead || m == http.MethodOptions
		if rr := routeMinRoleFor(path, isRead); rr > max {
			max = rr
		}
	}
	return max.String()
}

func (s *apiServer) handleMetaRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	routes := apiRouteCatalog()
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"routes": routes,
		"count":  len(routes),
	}})
}

// apiRouteCatalog is the single source of truth for the REST surface: it powers
// both /api/v1/meta/routes and (by gap-filling) the OpenAPI spec, so new routes
// can't silently go undocumented in either. Each entry's MinRole is computed
// from the same routeMinRole logic the auth middleware enforces.
func apiRouteCatalog() []routeMeta {
	routes := []routeMeta{
		{Path: "/api/v1/health", Methods: []string{http.MethodGet}, Description: "Backend health, version, and resolved paths"},
		{Path: "/api/v1/health/ready", Methods: []string{http.MethodGet}, Description: "Public readiness probe: 200 when the user store is readable, the runner binary is executable, and the output directory is writable; 503 otherwise (check names only)"},
		{Path: "/api/v1/tls/public", Methods: []string{http.MethodGet}, Description: "Public: UI certificate SHA-256 fingerprint and PEM (no private key) so the login page can offer download/trust for self-signed HTTPS"},
		{Path: "/api/v1/components", Methods: []string{http.MethodGet}, Description: "Viewer+: report orchestrator, API server, and UI server versions"},
		{Path: "/api/v1/alerts", Methods: []string{http.MethodGet}, Description: "Viewer+: fetch normalized Prism Central alerts with bounded caching and per-source errors"},
		{Path: "/api/v1/alerts/ncc-dispositions", Methods: []string{http.MethodGet, http.MethodPost}, Description: "GET viewer+: NCC acknowledge/resolve marks (v2.2.0-compatible view of the persistent finding lifecycle; items include id, finding_id, detection). POST operator+: acknowledge, resolve, or reopen one finding or a list (by cluster+check or id). A newer run that still finds a resolved check opens a new linked instance (status reopened).", SampleBody: "{\n  \"cluster\": \"prod-a\",\n  \"check\": \"CVM memory\",\n  \"action\": \"acknowledge\",\n  \"note\": \"Watching this run\"\n}"},
		{Path: "/api/v1/alerts/ncc", Methods: []string{http.MethodGet}, Description: "Viewer+: NCC finding instances with persistent lifecycle state (open/acknowledged/resolved), detection (detected/not_detected), unique UUIDv4 ids, stable finding_id, first/last seen and actor timestamps. Filters: state, detection, cluster, check, severity (FAIL,WARN,ERR,INFO, comma list), finding_id, updated_since, include_closed; cursor paging via limit + cursor (next_cursor). Cluster-group scoped."},
		{Path: "/api/v1/alerts/ncc/events", Methods: []string{http.MethodGet}, Description: "Viewer+: append-only NCC finding lifecycle events (detected, redetected, not_detected, severity_changed, acknowledged, resolved, reopened, recurred, closed) ordered by seq. Incremental sync: pass next_cursor back as cursor. Optional alert_id filter. Cluster-group scoped."},
		{Path: "/api/v1/alerts/ncc/{id}", Methods: []string{http.MethodGet}, Description: "Viewer+: one NCC finding instance with its full event history and the other instances (earlier/later episodes) of the same finding"},
		{Path: "/api/v1/alerts/ncc/{id}/{action}", Methods: []string{http.MethodPost}, Description: "Operator+: action is acknowledge, resolve, or reopen. Optional JSON body {\"note\": \"...\"} (max 500 chars). Idempotent: repeating the current state with the same note records nothing new.", SampleBody: "{\n  \"note\": \"Vendor case 12345 opened\"\n}"},
		{Path: "/api/v1/onboarding", Methods: []string{http.MethodGet, http.MethodPut}, Description: "GET operator+: the setup checklist, derived from the install's real state (clusters configured, first successful run, schedule, backups, notifications, team access). PUT admin-only: dismissed hides or shows the Overview banner, skip_step/unskip_step marks a step as not applicable (it counts as done), finished completes setup with steps left.", SampleBody: "{\n  \"skip_step\": \"notifications\"\n}"},
		{Path: "/api/v1/features", Methods: []string{http.MethodGet, http.MethodPut}, Description: "GET viewer+: which optional views are on. PUT admin-only: turn Insights, Prism Central alerts, discovery, comparison, flaky checks, SLO, or the log index on or off.", SampleBody: "{\n  \"insights\": false,\n  \"pc_alerts\": false\n}"},
		{Path: "/api/v1/audit", Methods: []string{http.MethodGet}, Description: "Read recent audit log entries (limit, action, failures filters)"},
		{Path: "/api/v1/metrics/rate-limit", Methods: []string{http.MethodGet}, Description: "Rate limiter configuration and counters"},
		{Path: "/metrics", Methods: []string{http.MethodGet}, Description: "Prometheus exposition (run/auth/update counters, build info, self-heal, backup inventory gauges, update-availability, audit-forward drops, report cache state)"},
		{Path: "/api/v1/health/diagnostics", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Admin-only: GET runs a read-only self-heal scan (doctor checks + live LDAP/SAML/clock probes) returning a ranked diagnostics list with an overall status; POST supports targeted fixes (check_ids), guardrails (no_disruptive), and post-fix verification."},
		{Path: "/api/v1/health/diagnostics/support-bundle", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Admin-only: POST generates a redacted support bundle (doctor report, every process log, redacted config and environment files) and returns its path and name; GET ?name=<name> downloads it."},
		{Path: "/api/v1/health/history", Methods: []string{http.MethodGet}, Description: "Admin-only: fixes applied by self-heal (background loop, Heal now, CLI doctor --fix), newest first, with per-check 24h/7d counts and recurring flags. ?limit=1..1000 (default 100)."},
		{Path: "/api/v1/health/autoheal", Methods: []string{http.MethodGet}, Description: "Admin-only: the automatic healing mechanisms (background checks, run auto-retry, runner automation, heal actions, repair after restore), whether each is on, and its counters."},
		{Path: "/api/v1/health/supervisor", Methods: []string{http.MethodGet}, Description: "Admin-only: the native supervisor's published state: each server's state, uptime, restarts, last exit reason with the end of its log, probe timings, policy and recent events; mode is supervised, manual or kubernetes."},
		{Path: "/api/v1/health/supervisor/restart", Methods: []string{http.MethodPost}, Description: "Admin-only: {\"child\":\"api\"|\"ui\"} asks the supervisor to restart that server gracefully, outside its restart budget (also ends a cooldown). 409 when no supervisor publishes state, or for api while a run is in progress."},
		{Path: "/api/v1/health/platform", Methods: []string{http.MethodGet}, Description: "Admin-only: host and runtime facts: OS, build and native CPU architecture (emulation), CPUs, GOMAXPROCS, memory, container limits, service manager, and this API process's goroutines, heap, memory limit and open files."},
		{Path: "/api/v1/health/migrations/config", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Admin-only: GET previews converting the active config.yaml to the nested schema-version: 1 layout (moves, duplicate spellings, new content); POST applies it, keeping the original as a .bak. Refused while a run is in progress. Runner settings are verified identical before anything is written."},
		{Path: "/api/v1/auth/session", Methods: []string{http.MethodPost}, Description: "Issue short-lived session token"},
		{Path: "/api/v1/auth/rotate", Methods: []string{http.MethodPost}, Description: "Rotate API token"},
		{Path: "/api/v1/auth/login", Methods: []string{http.MethodPost}, Description: "Local-account login; sets the httpOnly session + CSRF cookies and reports role/must-change", SampleBody: "{\n  \"username\": \"admin\",\n  \"password\": \"\"\n}"},
		{Path: "/api/v1/auth/logout", Methods: []string{http.MethodPost}, Description: "Clear the session and CSRF cookies"},
		{Path: "/api/v1/auth/me", Methods: []string{http.MethodGet}, Description: "Caller identity, role, session expiry, and which login methods are enabled (no auth required)"},
		{Path: "/api/v1/auth/change-password", Methods: []string{http.MethodPost}, Description: "Self-service password change for the logged-in session (requires CSRF on cookie sessions); bumps token generation so other sessions are signed out", SampleBody: "{\n  \"current_password\": \"\",\n  \"new_password\": \"\"\n}"},
		{Path: "/api/v1/auth/refresh", Methods: []string{http.MethodPost}, Description: "Re-issue the current session cookie with a fresh expiry (used by the UI's inactivity 'stay logged in' prompt); session auth only"},
		{Path: "/api/v1/auth/forgot-password", Methods: []string{http.MethodPost}, Description: "Public self-service: queue a password-reset request for an admin to action; always returns a generic 200 (no account enumeration)", SampleBody: "{\n  \"username\": \"alice\"\n}"},
		{Path: "/api/v1/auth/tokens", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Self-service personal access tokens (any signed-in user): GET lists your tokens (metadata only); POST mints a bearer token inheriting your role, returned once. Use it as 'X-API-Token: <token>' or 'Authorization: Bearer <token>'.", SampleBody: "{\n  \"name\": \"laptop-cli\",\n  \"expires_in_days\": 90\n}"},
		{Path: "/api/v1/auth/tokens/{id}", Methods: []string{http.MethodDelete}, Description: "Self-service: revoke one of your own personal access tokens by id"},
		{Path: "/api/v1/users/me/pats", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Alias of /api/v1/auth/tokens: mint or list hashed personal access tokens for the authenticated user. POST returns the plaintext PAT once (ncc_pat_...).", SampleBody: "{\n  \"name\": \"ci-runner\",\n  \"expires_in_days\": 90\n}"},
		{Path: "/api/v1/users/me/pats/{id}", Methods: []string{http.MethodDelete}, Description: "Alias of /api/v1/auth/tokens/{id}: revoke one of your own hashed personal access tokens by id"},
		{Path: "/api/v1/settings/password-resets", Methods: []string{http.MethodGet}, Description: "Admin-only: list pending self-service password-reset requests"},
		{Path: "/api/v1/settings/password-resets/{name}", Methods: []string{http.MethodDelete}, Description: "Admin-only: dismiss a pending password-reset request without resetting the password (resetting it clears the request automatically)"},
		{Path: "/api/v1/settings/users", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Admin-only: list local accounts / create one (last-admin + reserved-admin protection)", SampleBody: "{\n  \"username\": \"alice\",\n  \"password\": \"\",\n  \"role\": \"operator\",\n  \"must_change_password\": true\n}"},
		{Path: "/api/v1/settings/users/{name}", Methods: []string{http.MethodPut, http.MethodDelete}, Description: "Admin-only: update role / reset password / set must-change, or delete an account", SampleBody: "{\n  \"role\": \"viewer\",\n  \"password\": \"\",\n  \"must_change_password\": true\n}"},
		{Path: "/api/v1/settings/sso", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: read/update the runtime SAML SSO configuration (read-only when configured via startup flags)", SampleBody: "{\n  \"enabled\": true,\n  \"root_url\": \"https://ncc.example.com\",\n  \"idp_metadata_url\": \"https://idp.example.com/metadata\",\n  \"role_attribute\": \"groups\",\n  \"role_map\": \"ncc-admins=admin,ncc-ops=operator\",\n  \"default_role\": \"viewer\"\n}"},
		{Path: "/api/v1/settings/oidc", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: read/update the OpenID Connect sign-in configuration (client secret is write-only; discovery is checked before saving)", SampleBody: "{\n  \"enabled\": true,\n  \"issuer_url\": \"https://login.microsoftonline.com/<tenant-id>/v2.0\",\n  \"client_id\": \"<application-id>\",\n  \"client_secret\": \"<client-secret>\",\n  \"root_url\": \"https://ncc.example.com\",\n  \"groups_claim\": \"groups\",\n  \"role_map\": \"ncc-admins=admin,ncc-ops=operator\",\n  \"default_role\": \"viewer\",\n  \"button_label\": \"Microsoft\"\n}"},
		{Path: "/api/v1/settings/oidc/test", Methods: []string{http.MethodPost}, Description: "Admin-only: run OIDC discovery against an issuer without saving (returns endpoints, scopes, claims, PKCE support)", SampleBody: "{\n  \"issuer_url\": \"https://accounts.google.com\"\n}"},
		{Path: "/api/v1/settings/ldap", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: read/update the runtime LDAP/Active Directory configuration: the default directory plus optional named directory profiles (profiles[]) picked by UPN suffix or NetBIOS domain. Bind passwords and CA certs are write-only; omitting profiles keeps the stored ones. Read-only when configured via startup flags", SampleBody: "{\n  \"enabled\": true,\n  \"url\": \"ldaps://dc1.corp.example.com:636\",\n  \"bind_dn\": \"CN=ncc-svc,OU=Service Accounts,DC=corp,DC=example,DC=com\",\n  \"bind_password\": \"service-account-secret\",\n  \"base_dn\": \"DC=corp,DC=example,DC=com\",\n  \"role_map\": \"CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin\",\n  \"default_role\": \"viewer\"\n}"},
		{Path: "/api/v1/settings/ldap/test", Methods: []string{http.MethodPost}, Description: "Admin-only: validate an LDAP/AD configuration by authenticating sample credentials, without saving it", SampleBody: "{\n  \"url\": \"ldaps://dc1.corp.example.com:636\",\n  \"base_dn\": \"DC=corp,DC=example,DC=com\",\n  \"bind_dn\": \"CN=ncc-svc,DC=corp,DC=example,DC=com\",\n  \"bind_password\": \"service-account-secret\",\n  \"role_map\": \"CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin\",\n  \"test_username\": \"jdoe\",\n  \"test_password\": \"users-password\"\n}"},
		{Path: "/api/v1/settings/ldap/search", Methods: []string{http.MethodGet}, Description: "Admin-only: live AD/LDAP type-ahead search for groups and users (?q=<term>&type=group|user|all&limit=<n>) to assign to cluster groups"},
		{Path: "/api/v1/settings/session", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: read/set the session lifetime (ttl_sec or ttl_min; 0 restores the --session-ttl default)", SampleBody: "{\n  \"ttl_min\": 360\n}"},
		{Path: "/api/v1/settings/ui-versions", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: UI version blacklist. GET returns builtin compromised versions (2.2.1 node2shell) plus extra operator entries. PUT replaces extra entries and whether unversioned ncc-ui clients are refused.", SampleBody: "{\n  \"block_unversioned\": true,\n  \"extra\": [{\"version\":\"2.1.0\",\"component\":\"ui\",\"reason\":\"example\"}]\n}"},
		{Path: "/api/v1/settings/tls", Methods: []string{http.MethodGet, http.MethodPut, http.MethodDelete}, Description: "Admin-only: manage HTTPS for the UI server. GET returns the installed certificate metadata; PUT installs a PEM cert+key; DELETE on a host/VM disables HTTPS, and on Kubernetes reverts to the stack-managed self-signed cert (HTTPS stays on). Kubernetes UI pods hot-reload /data/tls; host/VM restarts the stack", SampleBody: "{\n  \"cert\": \"-----BEGIN CERTIFICATE-----\\n...\\n-----END CERTIFICATE-----\\n\",\n  \"key\": \"-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n\"\n}"},
		{Path: "/api/v1/settings/tls/generate", Methods: []string{http.MethodPost}, Description: "Admin-only: generate (or renew) a self-signed certificate for the UI server, enable HTTPS, and restart the stack. Body is optional: {hosts:[...], valid_days:N}; hosts default to the request host and always include localhost/loopback", SampleBody: "{\n  \"hosts\": [\"10.21.88.27\"],\n  \"valid_days\": 825\n}"},
		{Path: "/api/v1/settings/cluster-groups", Methods: []string{http.MethodGet, http.MethodPut}, Description: "GET operator+: read the cluster groups; PUT admin-only: replace the groups that confine non-admins to their clusters (members = local accounts + AD groups/users)", SampleBody: "{\n  \"groups\": [\n    {\n      \"name\": \"Platform\",\n      \"clusters\": [\"10.0.0.1\", \"pc-east\"],\n      \"local_users\": [\"alice\"],\n      \"ad_groups\": [\"CN=NCC-Platform,OU=Groups,DC=corp,DC=example,DC=com\"]\n    }\n  ]\n}"},
		{Path: "/api/v1/settings/tokens", Methods: []string{http.MethodGet}, Description: "Admin-only: inventory every user's personal access tokens (metadata only)"},
		{Path: "/api/v1/settings/tokens/{id}", Methods: []string{http.MethodDelete}, Description: "Admin-only: revoke any user's personal access token by id"},
		{Path: "/api/v1/settings/clusters", Methods: []string{http.MethodGet}, Description: "Operator+: list clusters known to the active config (for assigning to groups / scoping runs)"},
		{Path: "/api/v1/settings/pc-clusters", Methods: []string{http.MethodGet}, Description: "Admin-only: discover the clusters registered under a Prism Central (?pc=<url>) for assigning a PC to a cluster group; uses the active run config's credentials"},
		{Path: "/api/v1/settings/backup", Methods: []string{http.MethodGet}, Description: "Admin-only: download a .tar.gz backup of the install dir (config + referenced files, local user database, API token, scheduler/notifications state). Send an optional 'X-NCC-Backup-Passphrase' header to seal the download at rest with AES-256-GCM (served as .tar.gz.enc)."}, // gitleaks:allow (prose)
		{Path: "/api/v1/settings/restore", Methods: []string{http.MethodPost}, Description: "Admin-only: restore a backup archive uploaded as multipart/form-data (field 'archive'; optional 'passphrase' field decrypts an encrypted archive); overwrites install-dir files with --force. Restart the stack afterward for it to take effect."},
		{Path: "/api/v1/settings/backups", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Admin-only: GET lists server-side backups under <install>/backups (manual snapshots + pre-update rollback points; each entry reports whether it is encrypted); POST creates a new persistent snapshot. Pass an optional {\"passphrase\":\"...\"} to encrypt the snapshot at rest with AES-256-GCM (stored as .tar.gz.enc).", SampleBody: "{\n  \"passphrase\": \"\"\n}"},
		{Path: "/api/v1/settings/backups/restore", Methods: []string{http.MethodPost}, Description: "Admin-only: restore a server-side backup by name (no re-upload), then restart the stack. Also powers post-update rollback: a pre-update snapshot taken with --include-stack restores the previous software version (binaries + frontend) as well as configuration and accounts. Supply 'passphrase' to restore an encrypted (.tar.gz.enc) snapshot.", SampleBody: "{\n  \"name\": \"pre-update-20260610T120000Z.tar.gz\",\n  \"passphrase\": \"\"\n}"},
		{Path: "/api/v1/settings/backups/verify", Methods: []string{http.MethodPost}, Description: "Admin-only: check a server-side backup without restoring it or touching the install (v2-restore --verify-only). data.report.checks lists pass/warn/fail for decryption, archive integrity, safe paths, manifest and checksums, config parsing, secrets (names only), config-referenced files and reports, and version compatibility. Returns 400 when a check fails. For an encrypted (.tar.gz.enc) snapshot, supply 'passphrase'.", SampleBody: "{\n  \"name\": \"manual-20260610T120000Z.tar.gz\",\n  \"passphrase\": \"\"\n}"},
		{Path: "/api/v1/settings/retention/preview", Methods: []string{http.MethodGet}, Description: "Admin-only: what the retention rules would delete now (run history: retain-last/retain-days; report files: artifact-retain-days/artifact-retain-max-files; raw NCC logs: raw-log-retain-days), per rule with paths, sizes and totals. Read-only: nothing is deleted."},
		{Path: "/api/v1/settings/backups/schedule", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: get/set the in-process scheduled backup (interval, optional AES-256-GCM encryption using the API server's configured key, and retention of newest N scheduled archives). PUT with run_now=true takes one snapshot immediately.", SampleBody: "{\n  \"enabled\": true,\n  \"every\": \"24h\",\n  \"encrypt\": false,\n  \"retain\": 7,\n  \"run_now\": false\n}"},
		{Path: "/api/v1/settings/backups/delete", Methods: []string{http.MethodPost}, Description: "Admin-only: delete a server-side backup by name", SampleBody: "{\n  \"name\": \"manual-20260610T120000Z.tar.gz\"\n}"},
		{Path: "/api/v1/settings/backups/download", Methods: []string{http.MethodGet}, Description: "Admin-only: download a server-side backup by name (?name=...). Encrypted snapshots stream as the opaque .tar.gz.enc envelope (decrypt with v2-restore + the passphrase)."},
		{Path: "/api/v1/settings/update", Methods: []string{http.MethodGet}, Description: "Admin-only: check for a newer release (current/latest version, update_available) and report any in-progress in-app update job/phase"},
		{Path: "/api/v1/settings/update/apply", Methods: []string{http.MethodPost}, Description: "Admin-only: apply an in-app update — takes a pre-update backup of configuration and the current software version (binaries + frontend), installs the latest stack (checksum-verified against the release checksums.txt), then restarts the stack. Where a service manager runs the stack, the new version's update helper carries out the install from outside the service: it replaces the programs and the service definitions, checks that the supervisor, API and UI come back healthy on the new version, and puts everything back if not; progress is in the upgrade field of GET /api/v1/settings/update. Roll back last update restores that snapshot including the previous version. Runs in the background; poll GET /api/v1/settings/update for progress.", SampleBody: "{\n  \"target_version\": \"\"\n}"},
		{Path: "/api/v1/settings/update/plan", Methods: []string{http.MethodGet}, Description: "Admin-only: what an upgrade would do (versions, how the stack is run, service definitions that change or are kept, disk space, problems). Changes nothing. Optional ?target_version=."},
		{Path: "/api/v1/settings/update/rollback", Methods: []string{http.MethodPost}, Description: "Admin-only: put back the version before the last upgrade (programs, web UI and service definitions; kept for 24 hours). The update helper restarts the stack; poll GET /api/v1/settings/update for progress. Refused while a run or an upgrade is in progress."},
		{Path: "/api/v1/settings/config", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write runtime config", SampleBody: "{\n  \"content\": \"clusters: \\\"10.0.0.1\\\"\\nusername: \\\"admin\\\"\\n\"\n}"},
		{Path: "/api/v1/settings/config/batch", Methods: []string{http.MethodPost}, Description: "Bulk add/update/remove YAML config files", SampleBody: "{\n  \"operations\": [\n    {\"action\": \"add\", \"path\": \"configs/dev-a.yaml\", \"content\": \"clusters: \\\"10.0.0.1\\\"\\nusername: \\\"admin\\\"\\n\"},\n    {\"action\": \"remove\", \"path\": \"configs/old.yaml\"}\n  ]\n}"},
		{Path: "/api/v1/settings/config-files", Methods: []string{http.MethodGet}, Description: "List config-referenced files (clusters, exclusions, secrets)"},
		{Path: "/api/v1/settings/config-file", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write one config-referenced file", SampleBody: "{\n  \"path\": \"alerts-exclude.txt\",\n  \"content\": \"AHV_MemoryUsage\\n\"\n}"},
		{Path: "/api/v1/settings/config-files/batch", Methods: []string{http.MethodPost}, Description: "Bulk add/update/remove config-referenced files", SampleBody: "{\n  \"operations\": [\n    {\"action\": \"add\", \"path\": \"clusters.txt\", \"content\": \"10.0.0.1\\n\"},\n    {\"action\": \"remove\", \"path\": \"exclude.txt\"}\n  ]\n}"},
		{Path: "/api/v1/settings/notifications", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: read/write notification channels (slack/webhook/email), events, and delivery controls — quiet hours, maintenance windows, dedup/rate-limit throttle, the scheduled health digest, and run_reports (per_run, only_on_regression: how the runner reports each run, config.yaml notify-digest / notify-on-regression). Failures (run/backup/self-heal) reuse the run_failure toggle.", SampleBody: "{\n  \"enabled\": true,\n  \"events\": {\"run_failure\": true, \"run_success\": false, \"policy_violations\": true},\n  \"email\": {\"enabled\": true, \"smtp_host\": \"smtp.example.com\", \"smtp_port\": 587, \"from\": \"ncc@example.com\", \"to\": \"sre@example.com\"},\n  \"quiet\": {\"enabled\": true, \"start\": \"22:00\", \"end\": \"07:00\", \"timezone\": \"UTC\", \"allow_failures\": true},\n  \"maintenance\": [{\"start\": \"2026-07-01T01:00:00Z\", \"end\": \"2026-07-01T03:00:00Z\", \"note\": \"patching\"}],\n  \"throttle\": {\"dedup_window_sec\": 300, \"min_interval_sec\": 30},\n  \"digest\": {\"enabled\": true, \"every\": \"24h\"}\n}"},
		{Path: "/api/v1/settings/notifications/test", Methods: []string{http.MethodPost}, Description: "Operator+: send a test message to one channel, or to every enabled channel (all); kind digest sends the health digest now. Uses the saved settings; delivery errors are URL-redacted.", SampleBody: "{\n  \"channel\": \"all\"\n}"},
		{Path: "/api/v1/settings/notifications/status", Methods: []string{http.MethodGet}, Description: "Operator+: per-channel delivery status shared by the runner and this server: state (ok, failing, never), last attempt and last success times, the last URL-redacted error, attempts on the last delivery, and how many undelivered messages wait in notification-deadletter-dir."},
		{Path: "/api/v1/schedule", Methods: []string{http.MethodGet, http.MethodPut}, Description: "GET viewer+: read scheduler state; PUT operator+: create/update/apply a recurring run. GET returns resource_version; send it back on PUT and a save over a newer schedule answers 409 NCC_API_SCHEDULE_CONFLICT (omit it for an unconditional save)", SampleBody: "{\n  \"type\": \"cron\",\n  \"action\": \"create\",\n  \"cron\": \"15 */4 * * *\",\n  \"config\": \"config.yaml\",\n  \"print_only\": true,\n  \"apply\": false\n}"},
		{Path: "/api/v1/schedule/health", Methods: []string{http.MethodGet}, Description: "Scheduler health snapshot (last run/success/error hints)"},
		{Path: "/api/v1/freshness", Methods: []string{http.MethodGet}, Description: "Data freshness from the run history: per cluster, when it was last checked, when it last returned data and how many runs in a row failed; per active schedule (main and environment), whether two intervals passed without a run (schedule_overdue). Clusters and environment schedules are limited to the caller's cluster groups."},
		{Path: "/api/v1/artifacts", Methods: []string{http.MethodGet}, Description: "List available artifacts"},
		{Path: "/api/v1/artifacts/{name}", Methods: []string{http.MethodGet}, Description: "Read artifact by name"},
		{Path: "/api/v1/runs", Methods: []string{http.MethodGet}, Description: "List historical runs with metrics (limit, source, since filters; merges archived runs, latest summary, and audit triggers)"},
		{Path: "/api/v1/runs/{id}", Methods: []string{http.MethodGet}, Description: "Read one archived run's metadata + embedded artifacts"},
		{Path: "/api/v1/runs/summary", Methods: []string{http.MethodGet}, Description: "Read latest run summary (with on-read error-class healing)"},
		{Path: "/api/v1/runs/active", Methods: []string{http.MethodGet, http.MethodDelete}, Description: "GET: read all queued/running runs (concurrent engine) plus the legacy single-run summary. DELETE: cancel a run by ?id=<run-id>, or all queued/running runs when no id is given."},
		{Path: "/api/v1/runs/preflight", Methods: []string{http.MethodPost}, Description: "Run preflight checks (config/secrets/path permissions)", SampleBody: "{\n  \"config_path\": \"config.yaml\"\n}"},
		{Path: "/api/v1/runs/extra-args", Methods: []string{http.MethodGet}, Description: "Run options accepted in extra_args by POST /runs/trigger, with labels, allowed values and examples (generated from the server allowlist)."},
		{Path: "/api/v1/runs/trigger", Methods: []string{http.MethodPost}, Description: "Trigger an orchestrator run. Concurrent runs are allowed; clusters already being refreshed by another active run are skipped (reported in skipped_clusters) and the run executes only the remainder. When all concurrency slots are busy the run is queued and starts automatically.", SampleBody: "{\n  \"config_path\": \"config.yaml\",\n  \"password\": \"\",\n  \"group\": \"\",\n  \"clusters\": [],\n  \"extra_args\": [\"--outputs\", \"csv,json\"]\n}"},
		{Path: "/api/v1/jobs/{id}", Methods: []string{http.MethodGet, http.MethodDelete}, Description: "Read or cancel an asynchronous NCC job. GET returns phase, progress, result linkage, and stable error information; DELETE requests cancellation."},
		{Path: "/api/v1/report/data", Methods: []string{http.MethodGet}, Description: "Aggregated report payload (supports optional limit/offset pagination for large arrays; `cache` reports the report cache state)"},
		{Path: "/api/v1/report/trends", Methods: []string{http.MethodGet}, Description: "Historical trends from run summaries"},
		{Path: "/api/v1/logs/runner", Methods: []string{http.MethodGet}, Description: "Read tail of runner log"},
		{Path: "/api/v1/logs", Methods: []string{http.MethodGet}, Description: "Log sources the caller may read (runner for everyone; API server, UI server, supervisor and scheduler for admins) and the current server log level"},
		{Path: "/api/v1/logs/{source}", Methods: []string{http.MethodGet}, Description: "Tail of one log (?file= picks among a source's files, ?bytes= up to 4 MiB)"},
		{Path: "/api/v1/settings/logging", Methods: []string{http.MethodGet, http.MethodPut}, Description: "API server log level (debug, info, warn, error); changes apply at once and last until restart"},
		{Path: "/api/v1/environments", Methods: []string{http.MethodGet}, Description: "Run environments (dev/staging/prod runner files) with their clusters"},
		{Path: "/api/v1/environment-schedules", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Per-environment run schedules"},
		{Path: "/api/v1/settings/environments/{name}", Methods: []string{http.MethodGet, http.MethodPut, http.MethodDelete}, Description: "Read, create/update or delete one environment file"},
		{Path: "/api/v1/settings/environments/import", Methods: []string{http.MethodGet, http.MethodPost}, Description: "List config files that can become environments; import one's runner section"},
		{Path: "/api/v1/openapi.json", Methods: []string{http.MethodGet}, Description: "OpenAPI 3.0 specification"},
		{Path: "/api/v1/meta/routes", Methods: []string{http.MethodGet}, Description: "List available REST routes for API explorer"},
	}
	for i := range routes {
		routes[i].MinRole = routeRequiredRole(routes[i].Path, routes[i].Methods)
	}
	return routes
}

func (s *apiServer) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	spec := s.buildOpenAPISpec()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(spec)
}

// buildOpenAPISpec returns the hand-authored OpenAPI spec, then gap-fills it
// from the canonical route catalog so every registered route is present (with a
// summary, a request example where the catalog has one, and an x-required-role
// annotation) even if it wasn't hand-written. This keeps /docs/ui (Swagger) and
// the API explorer from drifting as routes are added.
func (s *apiServer) buildOpenAPISpec() map[string]interface{} {
	spec := s.buildOpenAPISpecBase()
	paths, ok := spec["paths"].(map[string]interface{})
	if !ok || paths == nil {
		paths = map[string]interface{}{}
		spec["paths"] = paths
	}
	addNCCAlertOpenAPI(spec, paths)
	for _, rt := range apiRouteCatalog() {
		item, exists := paths[rt.Path].(map[string]interface{})
		if !exists {
			item = openAPIItemFromRoute(rt)
			paths[rt.Path] = item
		}
		// Annotate the required role on each method operation (additive; never
		// clobbers a hand-authored field).
		role := routeRequiredRole(rt.Path, rt.Methods)
		for _, m := range rt.Methods {
			if op, okop := item[strings.ToLower(m)].(map[string]interface{}); okop {
				if _, has := op["x-required-role"]; !has {
					op["x-required-role"] = role
				}
			}
		}
	}
	return spec
}

// openAPIItemFromRoute builds a minimal OpenAPI path item from a catalog entry:
// a summary per method and, for mutating methods with a sample body, a JSON
// request example.
func openAPIItemFromRoute(rt routeMeta) map[string]interface{} {
	item := map[string]interface{}{}
	for _, m := range rt.Methods {
		op := map[string]interface{}{"summary": rt.Description}
		if rt.SampleBody != "" && (m == http.MethodPost || m == http.MethodPut) {
			var example interface{}
			if json.Unmarshal([]byte(rt.SampleBody), &example) == nil {
				op["requestBody"] = map[string]interface{}{
					"required": true,
					"content": map[string]interface{}{
						"application/json": map[string]interface{}{"example": example},
					},
				}
			}
		}
		item[strings.ToLower(m)] = op
	}
	return item
}

func (s *apiServer) buildOpenAPISpecBase() map[string]interface{} {
	return map[string]interface{}{
		"openapi": "3.0.3",
		"info": map[string]interface{}{
			"title":       "NCC Orchestrator API",
			"version":     "2.4.0",
			"description": "REST API for NCC orchestrator run control, artifacts, settings, and analytics.",
		},
		"servers": []map[string]interface{}{
			{"url": "/"},
		},
		"paths": map[string]interface{}{
			"/api/v1/health": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Backend health, version, build_date, and resolved paths"},
			},
			"/api/v1/tls/public": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Public UI certificate fingerprint and PEM (no private key) for login-page trust"},
			},
			"/api/v1/audit": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "Recent audit log entries (newest first).",
					"parameters": []map[string]interface{}{
						{"name": "limit", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 10000, "default": 100}},
						{"name": "action", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string", "description": "Filter to entries whose action starts with this string (e.g. \"settings\", \"runs.trigger\")"}},
						{"name": "failures", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string", "enum": []string{"1", "true"}, "description": "Return only failed entries"}},
					},
				},
			},
			"/api/v1/metrics/rate-limit": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Rate limiter configuration and counters"},
			},
			"/metrics": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Prometheus exposition format (run/notification counters, build info)"},
			},
			"/api/v1/auth/session": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Issue short-lived session token"},
			},
			"/api/v1/auth/rotate": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Rotate API token"},
			},
			"/api/v1/auth/login": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Local-account login. On success sets the httpOnly session cookie + CSRF cookie and returns the role, session expiry, and whether a password change is required.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"username": "admin",
									"password": "",
								},
							},
						},
					},
				},
			},
			"/api/v1/auth/logout": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Clear the session and CSRF cookies"},
			},
			"/api/v1/auth/refresh": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Re-issue the current session cookie with a fresh expiry (extends the session by the effective TTL). Session auth only (cookie sessions require X-CSRF-Token); used by the UI's inactivity 'stay logged in' prompt."},
			},
			"/api/v1/auth/me": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Caller identity, role, session expiry, and enabled login methods (local/SAML). Reachable unauthenticated so the UI can decide whether to show a login screen."},
			},
			"/api/v1/auth/change-password": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Change the logged-in account's password. Requires a session (cookie sessions also require the X-CSRF-Token header). Verifies the current password, enforces a 12-char minimum, and bumps the token generation so all other sessions are signed out.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"current_password": "",
									"new_password":     "",
								},
							},
						},
					},
				},
			},
			"/api/v1/auth/forgot-password": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Public self-service password recovery: queue a reset request for an admin to action out-of-band. Always returns a generic 200 regardless of whether the account exists (no enumeration); a request is recorded only for an existing local account. Rate-limited.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{"username": "alice"},
							},
						},
					},
				},
			},
			"/api/v1/settings/password-resets": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: list pending self-service password-reset requests (username, requested_at, client_ip)."},
			},
			"/api/v1/settings/password-resets/{name}": map[string]interface{}{
				"delete": map[string]interface{}{
					"summary": "Admin-only: dismiss a pending password-reset request without resetting the password (resetting the user's password via the users endpoint clears it automatically).",
					"parameters": []map[string]interface{}{
						{"name": "name", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string"}},
					},
				},
			},
			"/api/v1/settings/users": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: list local accounts (username, role, must-change, timestamps; password hashes are never returned)"},
				"post": map[string]interface{}{
					"summary": "Admin-only: create a local account. Role is admin|operator|viewer; new accounts default to must-change-password.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"username":             "alice",
									"password":             "",
									"role":                 "operator",
									"must_change_password": true,
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/users/{name}": map[string]interface{}{
				"put": map[string]interface{}{
					"summary": "Admin-only: update an account's role and/or reset its password and/or flip the must-change flag. The built-in admin cannot be demoted; the last admin cannot be demoted.",
					"parameters": []map[string]interface{}{
						{"name": "name", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string"}},
					},
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"role":                 "viewer",
									"password":             "",
									"must_change_password": true,
								},
							},
						},
					},
				},
				"delete": map[string]interface{}{
					"summary": "Admin-only: delete an account. The built-in admin and the last remaining admin cannot be deleted.",
					"parameters": []map[string]interface{}{
						{"name": "name", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string"}},
					},
				},
			},
			"/api/v1/settings/sso": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: read the runtime SAML SSO config (SP private key is never returned). Reports whether SSO is enabled and whether it is managed via startup flags or runtime."},
				"put": map[string]interface{}{
					"summary": "Admin-only: update the runtime SAML SSO config and hot-reload the SP. Returns 409 when SAML is managed via startup flags.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"enabled":          true,
									"root_url":         "https://ncc.example.com",
									"idp_metadata_url": "https://idp.example.com/metadata",
									"role_attribute":   "groups",
									"role_map":         "ncc-admins=admin,ncc-ops=operator",
									"default_role":     "viewer",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/ldap": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: read the runtime LDAP/Active Directory config (the bind password is never returned; has_bind_password is reported instead). Reports whether LDAP is enabled and whether it is managed via startup flags or runtime."},
				"put": map[string]interface{}{
					"summary": "Admin-only: update the runtime LDAP/AD config and hot-reload the provider. The flat fields are the default directory; profiles[] are further directories routed by upn_suffixes / netbios_names (omit profiles to keep the stored list, send [] to clear it). Bind passwords and CA certs are write-only (omit to keep the stored secret; a renamed profile names its old name in previous_name). Returns 409 when LDAP is managed via startup flags.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"enabled":       true,
									"url":           "ldaps://dc1.corp.example.com:636",
									"bind_dn":       "CN=ncc-svc,OU=Service Accounts,DC=corp,DC=example,DC=com",
									"bind_password": "service-account-secret",
									"base_dn":       "DC=corp,DC=example,DC=com",
									"role_map":      "CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin",
									"default_role":  "viewer",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/ldap/search": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: live AD/LDAP type-ahead search for groups and users (query params q, type=group|user|all, limit) used when assigning AD principals to cluster groups. Returns an empty result set when LDAP is not enabled."},
			},
			"/api/v1/settings/pc-clusters": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: discover the clusters registered under a Prism Central (query param pc=<url>) so it can be assigned to a cluster group. Uses the active run config's credentials; results are cached and refreshed in the background."},
			},
			"/api/v1/settings/cluster-groups": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: list the cluster groups. Membership (local accounts + AD groups + individual AD users) confines non-admins to the clusters in their groups (plus any clusters under an assigned Prism Central); ungrouped clusters are admin-only."},
				"put": map[string]interface{}{
					"summary": "Admin-only: replace the full set of cluster groups.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"groups": []map[string]interface{}{{
										"name":        "Platform",
										"clusters":    []string{"10.0.0.1", "pc-east"},
										"local_users": []string{"alice"},
										"ad_groups":   []string{"CN=NCC-Platform,OU=Groups,DC=corp,DC=example,DC=com"},
									}},
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/clusters": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: list the clusters known to the active config (inline clusters + clusters-file) for assigning to groups."},
			},
			"/api/v1/settings/session": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: read the effective session lifetime, its source (default|runtime), and the allowed bounds"},
				"put": map[string]interface{}{
					"summary": "Admin-only: set the session lifetime. Provide ttl_sec or ttl_min; 0 clears the override and restores the server's --session-ttl default. New value applies to sessions minted afterward.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"ttl_min": 360,
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/backup": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "Admin-only: download a .tar.gz backup of the install directory (config + referenced files, local user database, API token, scheduler/notifications state). The archive contains secrets — store it securely. Send an optional 'X-NCC-Backup-Passphrase' header to encrypt the download at rest with AES-256-GCM (served as .tar.gz.enc).", // gitleaks:allow (prose)
					"parameters": []map[string]interface{}{
						{"name": "X-NCC-Backup-Passphrase", "in": "header", "required": false, "schema": map[string]interface{}{"type": "string"}, "description": "When set, the archive is sealed with AES-256-GCM and downloaded as .tar.gz.enc; restore needs the same passphrase."},
					},
				},
			},
			"/api/v1/settings/restore": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Admin-only: restore a backup archive uploaded as multipart/form-data (field 'archive'; optional 'passphrase' field decrypts an encrypted archive). Overwrites install-dir files with --force and proceeds even while the stack is live; restart the stack afterward for the restored config/accounts/token to take effect."},
			},
			"/api/v1/settings/backups": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: list server-side backups under <install>/backups (manual snapshots + pre-update rollback points). Each entry reports name, kind, size, mod_time, and whether it is encrypted."},
				"post": map[string]interface{}{
					"summary": "Admin-only: create a persistent server-side snapshot. An optional passphrase seals it at rest with AES-256-GCM (stored as .tar.gz.enc).",
					"requestBody": map[string]interface{}{
						"required": false,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{"example": map[string]interface{}{"passphrase": ""}},
						},
					},
				},
			},
			"/api/v1/settings/backups/restore": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Admin-only: restore a server-side backup by name, then restart the stack. Supply 'passphrase' to restore an encrypted (.tar.gz.enc) snapshot.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{"example": map[string]interface{}{"name": "pre-update-20260610T120000Z.tar.gz", "passphrase": ""}},
						},
					},
				},
			},
			"/api/v1/settings/backups/delete": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Admin-only: delete a server-side backup by name.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{"example": map[string]interface{}{"name": "manual-20260610T120000Z.tar.gz"}},
						},
					},
				},
			},
			"/api/v1/settings/backups/download": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "Admin-only: download a server-side backup by name. Encrypted snapshots stream as the opaque .tar.gz.enc envelope.",
					"parameters": []map[string]interface{}{
						{"name": "name", "in": "query", "required": true, "schema": map[string]interface{}{"type": "string"}, "description": "Backup filename (…tar.gz or …tar.gz.enc)"},
					},
				},
			},
			"/api/v1/health/diagnostics": map[string]interface{}{
				"get":  map[string]interface{}{"summary": "Admin-only: read-only self-heal scan (doctor checks + live LDAP/SAML/clock probes) with a ranked diagnostics list and overall status."},
				"post": map[string]interface{}{"summary": "Admin-only: run self-heal with optional targeted checks (check_ids), disruptive-fix guardrails, and post-fix verification loops."},
			},
			"/api/v1/health/diagnostics/support-bundle": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Admin-only: generate a redacted support bundle (doctor report, every process log, redacted config and environment files); returns path, name and size."},
				"get":  map[string]interface{}{"summary": "Admin-only: download a bundle by name (?name=ncc-support-<UTC>.tar.gz)."},
			},
			"/api/v1/health/history": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: self-heal fix history, newest first, with per-check counts and recurring flags."},
			},
			"/api/v1/health/autoheal": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: automatic healing mechanisms and whether each is on."},
			},
			"/api/v1/health/supervisor": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: native supervisor state, restarts, last exits and events."},
			},
			"/api/v1/health/supervisor/restart": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Admin-only: ask the supervisor to restart the api or ui server."},
			},
			"/api/v1/health/platform": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: host, architecture, container limits and API process runtime facts."},
			},
			"/api/v1/health/migrations/config": map[string]interface{}{
				"get":  map[string]interface{}{"summary": "Admin-only: preview converting config.yaml to the nested layout."},
				"post": map[string]interface{}{"summary": "Admin-only: convert config.yaml to the nested layout (original kept as .bak)."},
			},
			"/api/v1/settings/config": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read runtime config"},
				"put": map[string]interface{}{
					"summary": "Write runtime config",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"content": "clusters: \"10.0.0.1\"\nusername: \"admin\"\n",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/config/batch": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Bulk add/update/remove YAML config files",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"operations": []map[string]interface{}{
										{"action": "add", "path": "configs/dev-a.yaml", "content": "clusters: \"10.0.0.1\"\nusername: \"admin\"\n"},
										{"action": "remove", "path": "configs/old.yaml"},
									},
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/config-files": map[string]interface{}{
				"get": map[string]interface{}{"summary": "List config-referenced files"},
			},
			"/api/v1/settings/config-file": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read one config-referenced file"},
				"put": map[string]interface{}{
					"summary": "Write one config-referenced file",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"path":    "alerts-exclude.txt",
									"content": "AHV_MemoryUsage\n",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/config-files/batch": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Bulk add/update/remove config-referenced files",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"operations": []map[string]interface{}{
										{"action": "add", "path": "clusters.txt", "content": "10.0.0.1\n"},
										{"action": "remove", "path": "exclude.txt"},
									},
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/notifications": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read notifications state"},
				"put": map[string]interface{}{
					"summary": "Write notifications state",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"enabled": true,
									"channel": "webhook",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/notifications/status": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Per-channel notification delivery status (operator+)"},
			},
			"/api/v1/settings/notifications/test": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Send test notifications",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"channel": "all",
								},
							},
						},
					},
				},
			},
			"/api/v1/schedule": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read scheduler state"},
				"put": map[string]interface{}{
					"summary": "Write scheduler state",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"type":       "cron",
									"action":     "create",
									"cron":       "15 */4 * * *",
									"config":     "config.yaml",
									"print_only": true,
									"apply":      false,
								},
							},
						},
					},
				},
			},
			"/api/v1/schedule/health": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read scheduler health snapshot"},
			},
			"/api/v1/freshness": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Per-cluster data age (last checked, last successful data, failed runs in a row) and overdue schedules"},
			},
			"/api/v1/artifacts": map[string]interface{}{
				"get": map[string]interface{}{"summary": "List available artifacts"},
			},
			"/api/v1/artifacts/{name}": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read artifact by name"},
			},
			"/api/v1/runs": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "List historical runs with metrics; merges archived runs, latest summary, and audit triggers.",
					"parameters": []map[string]interface{}{
						{"name": "limit", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 1000, "default": 200}},
						{"name": "source", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string", "enum": []string{"history", "summary", "trigger"}}},
						{"name": "since", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string", "description": "RFC3339 timestamp or YYYY-MM-DD date"}},
					},
				},
			},
			"/api/v1/runs/{id}": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "Read one archived run's metadata + embedded artifacts (run-summary.json, ncc-run-record.json, regression-summary.json, checks-snapshot.json, run-meta.json).",
					"parameters": []map[string]interface{}{
						{"name": "id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string"}},
					},
				},
			},
			"/api/v1/runs/summary": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read latest run summary (with on-read error_class healing for legacy entries)"},
			},
			"/api/v1/runs/active": map[string]interface{}{
				"get":    map[string]interface{}{"summary": "Read all queued/running runs (runs[], running_count, queued_count, max_concurrent) plus the legacy single-run summary"},
				"delete": map[string]interface{}{"summary": "Cancel a run by ?id=<run-id>, or all queued/running runs when no id is given. Returns 409 if nothing is active."},
			},
			"/api/v1/runs/preflight": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Run preflight checks before trigger-run",
					"requestBody": map[string]interface{}{
						"required": false,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"config_path": "config.yaml",
								},
							},
						},
					},
				},
			},
			"/api/v1/runs/trigger": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Trigger an orchestrator run. Concurrent runs are allowed; clusters already in flight elsewhere are skipped and only the remainder runs. Over the concurrency cap the run is queued.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"config_path": "config.yaml",
									"password":    "",
									"extra_args":  []string{"--outputs", "csv,json"},
								},
							},
						},
					},
				},
			},
			"/api/v1/report/data": map[string]interface{}{
				"get": map[string]interface{}{
					"summary":     "Aggregated report payload",
					"description": "The `cache` object reports the report cache state (cold, warm, refreshing, stale or failed), when the served report was built (built_at), refresh_started_at while a newer one is built, last_error and failed_at after a failed refresh, and the source run (source_run_id, source_run_time).",
					"parameters": []map[string]interface{}{
						{"name": "limit", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer", "minimum": 0, "maximum": 5000}},
						{"name": "offset", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer", "minimum": 0}},
						{"name": "cursor", "in": "query", "required": false, "description": "Opaque next_cursor returned in pagination metadata; cannot be combined with offset.", "schema": map[string]interface{}{"type": "string"}},
						{"name": "cluster", "in": "query", "required": false, "description": "Filter findings by cluster address or name.", "schema": map[string]interface{}{"type": "string"}},
						{"name": "check", "in": "query", "required": false, "description": "Case-insensitive substring filter for the check name.", "schema": map[string]interface{}{"type": "string"}},
						{"name": "severity", "in": "query", "required": false, "description": "Filter findings by severity.", "schema": map[string]interface{}{"type": "string", "enum": []string{"FAIL", "WARN", "ERR", "INFO"}}},
					},
				},
			},
			"/api/v1/jobs/{id}": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "Read asynchronous job state",
					"parameters": []map[string]interface{}{{
						"name": "id", "in": "path", "required": true,
						"schema": map[string]interface{}{"type": "string"},
					}},
					"responses": map[string]interface{}{
						"200": map[string]interface{}{"description": "Job state", "content": map[string]interface{}{"application/json": map[string]interface{}{"schema": map[string]interface{}{"$ref": "#/components/schemas/AIEnvelope"}}}},
					},
				},
				"delete": map[string]interface{}{
					"summary": "Request asynchronous job cancellation",
					"parameters": []map[string]interface{}{{
						"name": "id", "in": "path", "required": true,
						"schema": map[string]interface{}{"type": "string"},
					}},
				},
			},
			"/api/v1/report/trends": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Historical trends from run summaries"},
			},
			"/api/v1/logs/runner": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read tail of runner log"},
			},
			"/api/v1/openapi.json": map[string]interface{}{
				"get": map[string]interface{}{"summary": "OpenAPI 3.0 specification"},
			},
			"/api/v1/meta/routes": map[string]interface{}{
				"get": map[string]interface{}{"summary": "List available REST routes for API explorer"},
			},
		},
	}
}
