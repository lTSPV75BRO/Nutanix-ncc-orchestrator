#!/usr/bin/env bash
# release-state.sh TAG - print "none", "draft" or "published" for the GitHub
# release of TAG in $GH_REPO (or the current repository). Exits non-zero on any
# other error (network, auth), so a failed API call is never read as "none".
set -uo pipefail

tag="${1:?usage: release-state.sh TAG}"
if out="$(gh release view "$tag" --json isDraft --jq .isDraft 2>&1)"; then
  case "$out" in
    true) echo draft ;;
    false) echo published ;;
    *) echo "unexpected output from gh: $out" >&2; exit 1 ;;
  esac
elif printf '%s' "$out" | grep -qi 'release not found'; then
  echo none
else
  echo "cannot determine release state: $out" >&2
  exit 1
fi
