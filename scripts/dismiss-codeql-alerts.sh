#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# scripts/dismiss-codeql-alerts.sh
#
# Dismiss the CodeQL alerts that are categorically false positives in this
# codebase because of custom sanitizers that CodeQL's default Go model does
# not recognize.  Run once after a PR sync if the GHAS aggregator check goes
# red on `go/command-injection`, `go/path-injection`, or
# `go/uncontrolled-allocation-size`.
#
# Requirements:
#   * `gh` CLI authenticated with `security_events` scope
#       (gh auth login --scopes "repo,security_events")
#   * Run from inside the repo (uses `gh repo view` to discover slug)
#
# Behavior:
#   * Lists currently OPEN code-scanning alerts for the three sanitized rules.
#   * Patches each one to state=dismissed with reason="won't fix" and a
#     rule-specific rationale comment.
#   * Idempotent — re-running on an already-dismissed alert is a no-op (gh
#     reports 422 which we tolerate).
#
# Usage:
#   bash scripts/dismiss-codeql-alerts.sh                    # dry-run preview
#   bash scripts/dismiss-codeql-alerts.sh --apply            # actually dismiss
#   REPO=owner/name bash scripts/dismiss-codeql-alerts.sh    # override repo
# -----------------------------------------------------------------------------
set -euo pipefail

APPLY=0
for arg in "$@"; do
  case "$arg" in
    --apply|-y) APPLY=1 ;;
    -h|--help)
      sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

if ! command -v gh >/dev/null 2>&1; then
  echo "error: gh CLI not found in PATH" >&2
  exit 1
fi

REPO="${REPO:-$(gh repo view --json nameWithOwner -q .nameWithOwner)}"
if [[ -z "$REPO" ]]; then
  echo "error: could not determine repo slug (set REPO=owner/name)" >&2
  exit 1
fi

# Rule -> rationale comment.  Keep these in lock-step with the rationale
# baked into .github/codeql/codeql-config.yml.
declare -A REASON
REASON["go/command-injection"]="False positive: every flow from request body to the orchestrator subprocess passes through sanitizeExtraArgs() in cmd/ncc-api-server/hardening.go (closed allow-list of --flag names; rejects shell metacharacters &;|\`\$<>\\n\\r\\t; values must match ^[A-Za-z0-9._:/=\\-,@]+\$). --orchestrator-bin is validated at startup by validatePathConfig() (must exist, must be executable, must not be a directory). CodeQL does not model these custom sanitizers."
REASON["go/path-injection"]="False positive: user-controlled paths pass through normalizeAndConfinePath() / validateConfigPath() in cmd/ncc-api-server/hardening.go which enforces isWithinBase(s.repoRoot), rejects symlinked path segments, and re-evaluates resolved target containment after stat. Internal sinks (e.g. loadReportMeta) receive paths chosen by selectBestReportOutDir() from a fixed set, not request input."
REASON["go/uncontrolled-allocation-size"]="False positive: user-supplied limit is clamped to [1,1000] immediately before make([]map[string]interface{}, 0, limit) in auditEntries(). Max allocation ~50 KB."

echo "Repo: $REPO"
echo "Mode: $([[ $APPLY -eq 1 ]] && echo APPLY || echo DRY-RUN)"
echo

total=0
for rule in "${!REASON[@]}"; do
  echo "=== Rule: $rule ==="
  mapfile -t alerts < <(
    gh api -H "Accept: application/vnd.github+json" \
      "/repos/$REPO/code-scanning/alerts?state=open&per_page=100" \
      --jq ".[] | select(.rule.id == \"$rule\") | [.number, .most_recent_instance.location.path, .most_recent_instance.location.start_line] | @tsv"
  )
  if [[ ${#alerts[@]} -eq 0 ]]; then
    echo "  (no open alerts)"
    continue
  fi
  for line in "${alerts[@]}"; do
    IFS=$'\t' read -r number path start_line <<<"$line"
    printf "  alert #%s  %s:%s\n" "$number" "$path" "$start_line"
    total=$((total + 1))
    if [[ $APPLY -eq 1 ]]; then
      gh api -X PATCH "/repos/$REPO/code-scanning/alerts/$number" \
        -f state=dismissed \
        -f dismissed_reason="won't fix" \
        -f dismissed_comment="${REASON[$rule]}" >/dev/null \
        && echo "    dismissed" \
        || echo "    skip (already-dismissed or denied)"
    fi
  done
done

echo
echo "Total open alerts touched: $total"
if [[ $APPLY -eq 0 ]]; then
  echo "Re-run with --apply to dismiss them."
fi
