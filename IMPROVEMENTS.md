# Improvements and new feature suggestions

A project-wide list of improvements and new features for the NCC Orchestrator: application, scripts, Kubernetes, CI/CD, and documentation. Use as a backlog; prioritize by impact and effort.

For the latest completed milestone summary, see [docs/MILESTONE_v2.0.0.md](docs/MILESTONE_v2.0.0.md).

---

## Top of backlog (next)

**Extract `goNCC.go` into `internal/` packages.** `goNCC.go` is ~15.5k lines.
Splitting notifications, the Prometheus textfile writer, and the parser into
their own packages was attempted for v2.1.0 and **deferred**: those subsystems
depend on pervasive shared types — `Config`, `FS`, `ParsedBlock`,
`NotificationSummary`, `HTTPClient` — used throughout `goNCC.go` and the
`cmd/*` servers, so a naive move does not compile and a parameterized move
churns hundreds of call sites. Recommended sequencing for a dedicated release:

1. **`internal/model` first.** Move the foundational shared types (`Config`,
   `FS`, `ParsedBlock`, `Row`, `NotificationSummary`, `HTTPClient`, severity
   constants) into a small leaf package with no project imports. This is the
   enabling, highest-churn step — do it alone, behind a green `-race` suite.
2. **`internal/promtext`** (Prometheus textfile). Smallest, cleanest surface
   (`writePrometheusFile`, `sanitizeLabel`, `clusterHealthScore`); depends only
   on `FS` + `ParsedBlock` from `internal/model`.
3. **`internal/notify`** (email/webhook/slack + retry wrappers). Depends on
   `Config`, `HTTPClient`, `NotificationSummary`, and the shared retry helpers
   (`jitteredBackoff`, `isRetryableStatus`, `retryAfterDelay`) — move those
   helpers too.
4. **`internal/nccparse`** (parser → `ParsedBlock`) last, only if the surface
   stays clean once `internal/model` exists.

Each step must keep behavior identical, move the matching tests, and leave
`go test -race ./...` green before the next step.

---

## Shipped through v1.1.0 (not exhaustive)

The following backlog themes were partially or fully addressed by **v1.0.0** and **v1.1.0**; see [CHANGELOG.md](CHANGELOG.md), [RELEASE_NOTES_v1.0.0.md](RELEASE_NOTES_v1.0.0.md), and [RELEASE_NOTES_v1.1.0.md](RELEASE_NOTES_v1.1.0.md) for the canonical list.

| Theme | What shipped |
|--------|----------------|
| v4 APIs + PC | Default v4 cluster match, CVM `nodeIps`, Prism task polling |
| `--update` | Semver comparison (Masterminds/semver); `GITHUB_TOKEN` documented |
| Exit codes | **0 / 1 / 2 / 3** documented; **3** = partial success |
| run-summary.json + ncc-run-record.json | `clusters[]`, `exit_code`, per-cluster severity counts; versioned `ncc-run-record.json` |
| Viper / flags | Root `--insecure-skip-verify` no longer overwritten by discover subcommand binds |
| Rate limits | 429 logging + `Retry-After` cap; `X-RateLimit-*` headers logged |
| discover-clusters | `--format lines|table|json` |
| MCP / Cursor | `get_report` on `*.log` adds markdown KB links (`internal/kblinks`) |
| Tests | Golden CSV + per-cluster HTML, httptest 429 retry |
| Helm / Kustomize | `helm/ncc-orchestrator`, `kubectl apply -k k8s/` |
| Docs | [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) |

**Remaining** items in the sections below are still valid backlog (resume/checkpoint, cluster subset, templates, etc.).

### Also shipped in v2.0.x – v2.1.0

These rows in the sections below are now **done** and kept only for history:

| Theme | Shipped in |
|-------|-----------|
| Checksum verification for `update` (single-binary, stack, `--binary-url`) | v2.0.x |
| Checksum verification for `v2-bootstrap` + `--skip-checksum-verify` escape | **v2.1.0** |
| Download size cap + timeout for updates | v2.0.x |
| Windows-friendly self-update (`.new.exe`) → now an automated `apply-ncc-update.cmd` swap helper | v2.0.x → **v2.1.0** |
| Semver comparison + `GITHUB_TOKEN` for updates | v1.x / v2.0.x |
| Helm chart + Kustomize (`helm/ncc-orchestrator`, `k8s/`) | v1.1.0 / v2.0.x |
| CI test/vet job, multi-platform release asset upload, version-from-tag | v2.0.x |
| SHA-256 `checksums.txt`, CycloneDX SBOM, SLSA provenance, `release-attestation.json` | v2.0.2 |
| Windows VERSIONINFO + optional self-signed code-signing hooks | v2.0.2 |

---

## 1. Application (Go binary)

### 1.1 Configuration and input

| Suggestion | Description |
|------------|-------------|
| **Clusters from file** | Support `clusters-file: path/to/list.txt` (one cluster per line) in addition to `clusters: "ip1,ip2"` for large or dynamic cluster lists. |
| **Config schema / validation docs** | In README, mark which options are required always vs required when a feature is on (e.g. “Required if email-enabled”). Optional: ship a JSON Schema for config.yaml. |
| **Config file permissions check** | If config file is world-readable and contains `password` or `smtp-password`, log a warning or refuse to start to avoid leaking secrets on shared systems. |
| **Multiple config files** | Allow `--config a.yaml,b.yaml` or a `config-dir` that merges/overrides (e.g. base + env-specific). |

### 1.2 Run behaviour and reliability

| Suggestion | Description |
|------------|-------------|
| **Resume / checkpoint** | Option to skip clusters that already have a successful run in this “session” (e.g. same day) or to resume from a list of remaining clusters after a crash. |
| **Cluster subset / filter** | `--clusters-only 10.0.1.1,10.0.1.2` or `--exclude-clusters 10.0.1.3` to run a subset without editing config. |
| **Exit codes** | Document and use consistent exit codes: 0 = success, 1 = run/validation error, 2 = config not found / bad config. Use in code so scripts can branch. |
| **Run summary JSON file** | Write `outputfiles/run-summary.json` (or configurable path) with clusters_ok, clusters_failed, duration_s, index_html, timestamp so other tools can consume it without parsing logs. |
| **Stricter timeouts** | Optional max total run time (e.g. “abort all after 2h”) in addition to per-cluster timeout. |

### 1.3 Notifications and reporting

| Suggestion | Description |
|------------|-------------|
| **Notification failure metrics** | Prometheus counters for email_send_failures, webhook_send_failures, slack_send_failures so monitoring can alert on notification issues. |
| **Slack retries** | Apply the same retry wrapper used for email/webhook to Slack so transient failures are retried. |
| **Custom notification templates** | Allow override of email subject/body (and maybe webhook JSON) via template file or configurable format string. |
| **Notify only on regression** | Option to send notifications only when FAIL count increased vs last run (compare with previous run summary or stored baseline). |
| **Report retention** | Optional `--retain-last N` to keep only the last N runs’ output (or last N days) to avoid filling disk. |

### 1.4 Outputs and formats

| Suggestion | Description |
|------------|-------------|
| **Single-file report** | Option to emit one combined HTML (or PDF) for the whole run instead of only index + per-cluster pages. |
| **Diff vs previous run** | Optional “diff” output (e.g. new FAILs, resolved FAILs) compared to previous run’s stored summary. |
| **JSON run summary** | Machine-readable run result (clusters, status, counts, paths) in JSON for automation. |
| **Markdown report** | Optional Markdown output for easy paste into wikis or tickets. |

### 1.4.1 HTML report UX improvements (v1.1.0+)

| Suggestion | Description |
|------------|-------------|
| **Delta badges** | Show `+new FAIL` and `-resolved FAIL` per cluster/check using `drilldown-diff.json` so operators can focus on changes. |
| **Health score widget** | Display per-cluster health score (`0-100`) with trend sparkline from run history. |
| **Flaky check marker** | Highlight checks detected in `flaky-checks.json` to reduce noise and false urgency. |
| **Policy gate panel** | Add a summary card in report header showing gate pass/fail and violated rules (`policy-gates.txt`). |
| **Run comparison mode** | Add “current vs previous” view with filter for changed checks only. |
| **Cluster status heatmap** | Add an at-a-glance matrix (cluster x severity counts) before detailed tables. |
| **Artifact quick links** | Add in-report download links for `run-summary.json`, `drilldown-diff.json`, `flaky-checks.json`, and `slo-dashboard.json`. |
| **Advanced search tokens** | Support query tokens such as `sev:FAIL`, `cluster:<id>`, `changed:true`, `flaky:true`. |
| **Accessibility pass** | Improve contrast, keyboard navigation, and ARIA labels for filters, tables, and controls. |

### 1.5 Security and safety

| Suggestion | Description |
|------------|-------------|
| **Secrets from env only** | Option to ignore password/smtp-password from config file and require them from env only (e.g. `NCC_PASSWORD`), reducing risk of committing secrets. |
| **Redact more in log-http** | Extend redaction to other common secret headers (e.g. X-API-Key, custom auth headers) via config or pattern list. |
| **Download limit for --update** | Cap download size (e.g. 150 MiB) and enforce timeout when fetching the update binary to avoid abuse or malicious release assets. |
| **Checksum verification for --update** | Verify SHA256 of downloaded binary (from release checksums file or asset metadata) before replacing the executable. |

### 1.6 Update and versioning

| Suggestion | Description |
|------------|-------------|
| **GITHUB_TOKEN for --update** | Support `GITHUB_TOKEN` env for authenticated API calls (higher rate limit); on 403 rate limit, print clear message. |
| **Semver for --update** | Use proper semver comparison (e.g. Masterminds/semver) so 0.1.13 is correctly “newer” than 0.1.12. |
| **Windows-friendly replace** | On Windows, write to `ncc-orchestrator.new.exe` and instruct user to replace after exit, instead of failing replace-in-place. |

### 1.7 Code and structure

| Suggestion | Description |
|------------|-------------|
| **Split packages** | See **Top of backlog (next)** above — extract `internal/model`, then `internal/promtext`, `internal/notify`, `internal/nccparse`. This is the headline structural item. |
| **HTTP dump allocation** | In redactHTTPDump, reduce allocations when truncating (e.g. single pass or buffer reuse). |

---

## 2. Scripts (Bash)

### 2.1 Uninstall script (`uninstall-ncc-orchestrator.sh`)

| Suggestion | Description |
|------------|-------------|
| **Idempotent / no error on missing** | Already exits 0 when namespace does not exist; document that and ensure no noisy errors when resources are already gone. |
| **Backup before delete** | Optional `--backup-dir ./backup` to dump ConfigMap, Secret (redacted), and PVC list before deleting namespace. |
| **KUBECONFIG check** | Fail fast with a clear message if `kubectl cluster-info` fails (e.g. KUBECONFIG not set or invalid). |
| **Version / image** | Print image tag in use (e.g. from CronJob) before asking for confirmation so user knows what will be removed. |

### 2.2 Prune script (`prune-ncc-images-workers.sh`)

| Suggestion | Description |
|------------|-------------|
| **Image name from cluster** | Option to read image name from the running CronJob or Deployment in the namespace instead of hardcoding `NCC_IMAGE_NAME`. |
| **Parallel SSH** | Run SSH prune in parallel (e.g. xargs -P) to speed up when there are many nodes. |
| **Timeout per node** | Add a timeout per node so one stuck SSH does not block the whole script. |
| **Dry-run output** | In --dry-run, print the exact crictl/docker commands that would run on each node. |

### 2.3 New scripts

| Suggestion | Description |
|------------|-------------|
| **Install script** | `scripts/install-ncc-orchestrator.sh` that applies k8s manifests in order, prompts for secret, and optionally runs a one-off job to verify. |
| **Backup report script** | Script to tar/gzip outputfiles + nccfiles (and optionally run-summary) for archival or move to another host. |
| **Cluster list from Prism** | Optional script or subcommand that uses Prism Central API (if available) to discover clusters and output a list for `clusters` or clusters-file. |

---

## 3. Kubernetes

### 3.1 Manifests and deployment

| Suggestion | Description |
|------------|-------------|
| **Kustomize or Helm** | Offer a Kustomize overlay or a small Helm chart so users can override namespace, image tag, schedule, and resource limits without editing many YAML files. |
| **Resource limits** | Set requests/limits on CronJob and Deployment so the scheduler and cluster autoscaler behave predictably. |
| **PodDisruptionBudget** | If the nginx Deployment is critical, add a PDB (minAvailable: 1) so evictions are controlled. |
| **Multi-arch image** | Build and push amd64 and arm64 images (e.g. with buildx) and use image digest or multi-arch manifest so ARM nodes can run the CronJob. |
| **ConfigMap from file** | Document or script generating ConfigMap from a local config.yaml so users don’t paste large YAML into k8s/configmap.yaml. |

### 3.2 Operational improvements

| Suggestion | Description |
|------------|-------------|
| **Alerting example** | In k8s/README or Prometheus.md, add a sample PrometheusRule/Alertmanager config to alert on CronJob failure or NCC FAIL count. |
| **ScheduledJob backoff** | Document or tune CronJob backoffLimit and restartPolicy so repeated failures don’t spawn too many jobs. |
| **Readiness for nginx** | Add a readinessProbe for the nginx Deployment (e.g. GET /) so the Service doesn’t send traffic until the server is up. |
| **SecurityContext** | Set runAsNonRoot and readOnlyRootFilesystem where possible; document any required capabilities. |

### 3.3 Documentation

| Suggestion | Description |
|------------|-------------|
| **Runbook** | Already added; keep it updated with “what to do when report is empty”, “when to use job-debug”, and “how to change schedule”. |
| **Troubleshooting table** | Table mapping symptom → cause → action (e.g. “Permission denied” → NFS/fsGroup → adjust fsGroup or NFS export). |
| **Minimal install** | “Minimal install” path: single cluster, no MetalLB, use NodePort or port-forward for the report UI. |

---

## 4. CI/CD and release

### 4.1 GitHub Actions

| Suggestion | Description |
|------------|-------------|
| **Test job** | Job that runs `go test ./...` and `go vet ./...` on push/PR (and optionally staticcheck) so regressions are caught early. |
| **Upload release assets** | On release, upload Linux/Darwin/Windows binaries (amd64, and darwin/linux arm64) to the GitHub release so `ncc-orchestrator -u` can update all platforms. |
| **Version from tag** | Use the release tag (e.g. v0.1.12) as the single source of version for both the binary and the Docker image. |
| **Build matrix** | Build and test on Go 1.22 and 1.23 (or go.mod version) to catch compatibility issues. |

### 4.2 Release and versioning

| Suggestion | Description |
|------------|-------------|
| **Changelog enforcement** | In PR template or CI, remind to update CHANGELOG.md (or check that Unreleased section exists). |
| **Checksums file** | Generate and upload SHA256SUMS (or similar) with release assets so --update and users can verify downloads. |
| **Release checklist** | Keep a short RELEASE_CHECKLIST.md (tag, push, create release, verify image, verify -u). |

---

## 5. Documentation

### 5.1 User docs

| Suggestion | Description |
|------------|-------------|
| **Quick start** | One-page “Quick start” (install binary → minimal config → run once → view report) at the top of README or in QUICKSTART.md. |
| **Example configs** | Add example configs: minimal, with email, with webhook, digest-only, replay-only, and reference to dist/example_config.yaml. |
| **Architecture diagram** | Simple diagram: Prism clusters → NCC Orchestrator → outputs (files, email, webhook, Slack, Prometheus). |
| **Glossary** | Short glossary: NCC, Prism, severity (FAIL/WARN/ERR/INFO), replay, digest, etc. |

### 5.2 Operator and runbooks

| Suggestion | Description |
|------------|-------------|
| **Runbook index** | In k8s/README, list runbook scenarios with links (CronJob failed, report empty, permissions, prune images). |
| **Prometheus runbook** | In Prometheus.md, add “What to do when metrics disappear” (e.g. CronJob not running, textfile dir not updated). |
| **Notification runbook** | “Notifications not received” → check SMTP/webhook URL, log level, retries, firewall. |

### 5.3 Developer and contributing

| Suggestion | Description |
|------------|-------------|
| **Build from source** | In CONTRIBUTING or README, document exact go build command and ldflags for a versioned binary. |
| **Testing** | How to run tests, run a single test, and (if added) run tests in Docker. |
| **Code layout** | Short “Code layout” section in CONTRIBUTING or README: main flow, config, NCC client, notifications, outputs. |

---

## 6. New features (larger scope)

| Feature | Description |
|---------|-------------|
| **Prism Central integration** | If Prism Central is in use, discover clusters from PC and optionally run NCC per cluster from a single job. |
| **Slack / Teams interactive messages** | Rich message with “View report” button linking to index.html (e.g. via ingress URL). |
| **Web UI for history** | Optional minimal web UI (or static page generator) that lists past runs (from run-summary.json or directory listing) and links to index.html per run. |
| **NCC check blacklist/allowlist** | Config to run only certain checks or exclude certain checks by name/regex (if NCC API supports it). |
| **Scheduling without CronJob** | Optional built-in scheduler (e.g. run every N hours in a loop) for running outside Kubernetes. |
| **Audit log** | Optional audit log (who ran what, when, which clusters) for compliance. |
| **Multi-tenant / per-team config** | Support multiple config files or “profiles” (e.g. team-a.yaml, team-b.yaml) and run each with separate outputs. |

---

## 7. Summary by priority (suggested)

**High impact, lower effort**  
- Run tests in CI.  
- Upload release assets and document `-u`.  
- Run summary JSON file.  
- README: required vs optional config.  
- Exit codes documented and consistent.  

**High impact, medium effort**  
- Clusters from file.  
- Install script for k8s.  
- GITHUB_TOKEN + rate-limit message for --update.  
- Notification failure metrics (Prometheus).  
- Kustomize or Helm for k8s.  

**Medium impact**  
- Config file permissions check.  
- Slack retries.  
- Backup before uninstall (script).  
- Checksum verification for --update.  
- Alerting example for Prometheus.  

**Nice to have**  
- Resume/checkpoint, cluster subset filter.  
- Custom notification templates.  
- Multi-arch Docker image.  
- Quick start page, architecture diagram.  
- Prism Central discovery (new feature).  

---

*Last updated: 2026-06 (post v2.1.0 release prep). The headline open item is the `goNCC.go` package extraction — see "Top of backlog (next)". Revisit as the project evolves.*
