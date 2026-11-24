#!/usr/bin/env bash
# verify-published-release.sh - audit the assets of a GitHub release.
#
# Downloads every asset of release TAG (draft or published) and checks that:
#   1. checksums.txt exists, every line matches the downloaded file, and every
#      asset (except checksums.txt, *.sig, *.asc) is listed in it;
#   2. optionally (--expect-dir) the asset set and bytes are exactly what the
#      workflow built: no missing asset, no extra asset, identical SHA-256;
#   3. every asset has a GitHub build attestation produced by the release
#      workflow for this tag (`gh attestation verify --signer-workflow`), so a
#      file uploaded by hand or by another workflow fails;
#   4. Linux *.asc GPG signatures verify against the published
#      ncc-release-signing.gpg (skipped with a warning when gpg is missing).
#
# Usage:
#   scripts/verify-published-release.sh [-R OWNER/REPO] [--workflow PATH]
#                                       [--expect-dir DIR] [--keep DIR] TAG
#
# Env: VERIFY_ATTEMPTS=N retries each attestation check (5 s apart); default 1.
# Needs: gh (authenticated; GH_TOKEN works), sha256sum or shasum, bash >= 4.
# Exit status: 0 only if every check passed.
set -uo pipefail

repo="${GITHUB_REPOSITORY:-}"
workflow=".github/workflows/release.yml"
expect_dir=""
keep_dir=""
tag=""

usage() { sed -n '2,/^set -uo/p' "$0" | sed '$d;s/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    -R|--repo) repo="${2:-}"; shift 2 ;;
    --workflow) workflow="${2:-}"; shift 2 ;;
    --expect-dir) expect_dir="${2:-}"; shift 2 ;;
    --keep) keep_dir="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option: $1" >&2; exit 2 ;;
    *) tag="$1"; shift ;;
  esac
done
[ -n "$tag" ] || { usage >&2; exit 2; }
command -v gh >/dev/null 2>&1 || { echo "gh is required" >&2; exit 2; }
if [ -z "$repo" ]; then
  repo="$(gh repo view --json nameWithOwner --jq .nameWithOwner)" || exit 2
fi

if command -v sha256sum >/dev/null 2>&1; then
  sha256_of() { sha256sum "$1" | cut -d' ' -f1; }
else
  sha256_of() { shasum -a 256 "$1" | cut -d' ' -f1; }
fi

failures=0
fail() { echo "FAIL: $*"; failures=$((failures + 1)); }
ok() { echo "ok:   $*"; }

if [ -n "$keep_dir" ]; then
  work="$keep_dir"
  mkdir -p "$work"
else
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT
fi
dl="$work/assets"
mkdir -p "$dl"

info="$(gh release view "$tag" -R "$repo" --json isDraft,tagName --jq '"draft=\(.isDraft) tag=\(.tagName)"')" \
  || { echo "FAIL: release $tag not found in $repo"; exit 1; }
echo "release $repo $info"

mapfile -t names < <(gh release view "$tag" -R "$repo" --json assets --jq '.assets[].name' | tr -d '\r')
[ "${#names[@]}" -gt 0 ] || { echo "FAIL: release $tag has no assets"; exit 1; }
for n in "${names[@]}"; do
  case "$n" in
    ''|*[!A-Za-z0-9._+-]*|.*) fail "unexpected asset name '$n'"; exit 1 ;;
  esac
done
echo "assets: ${#names[@]}"

gh release download "$tag" -R "$repo" --dir "$dl" --clobber --pattern '*' >/dev/null \
  || { echo "FAIL: could not download assets"; exit 1; }

# 1. checksums.txt
if [ ! -f "$dl/checksums.txt" ]; then
  fail "checksums.txt is missing"
else
  declare -A listed=()
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    [ -n "$line" ] || continue
    want="${line%% *}"
    file="${line#"$want"}"
    file="${file# }"; file="${file#[ *]}"
    listed["$file"]=1
    if [ ! -f "$dl/$file" ]; then
      fail "checksums.txt lists $file, which is not a release asset"
    elif [ "$(sha256_of "$dl/$file")" != "$want" ]; then
      fail "sha256 mismatch for $file"
    fi
  done < "$dl/checksums.txt"
  for n in "${names[@]}"; do
    case "$n" in checksums.txt|*.sig|*.asc) continue ;; esac
    [ -n "${listed[$n]:-}" ] || fail "asset $n is not listed in checksums.txt"
  done
  ok "checksums.txt reviewed (${#listed[@]} entries)"
fi

# 2. exact match with what the workflow built
if [ -n "$expect_dir" ]; then
  built=()
  for f in "$expect_dir"/*; do
    [ -f "$f" ] && built+=("$(basename "$f")")
  done
  for b in "${built[@]}"; do
    if [ ! -f "$dl/$b" ]; then
      fail "built file $b was not published"
    elif [ "$(sha256_of "$dl/$b")" != "$(sha256_of "$expect_dir/$b")" ]; then
      fail "published $b differs from the built file"
    fi
  done
  for n in "${names[@]}"; do
    found=0
    for b in "${built[@]}"; do [ "$b" = "$n" ] && found=1; done
    [ "$found" -eq 1 ] || fail "asset $n was not built by this workflow run"
  done
  ok "asset set compared with $expect_dir (${#built[@]} files)"
fi

# 3. build attestations
digest="$(gh api "repos/$repo/commits/refs/tags/$tag" --jq .sha 2>/dev/null | tr -d '\r')"
if [ -z "$digest" ]; then
  fail "cannot resolve tag $tag to a commit in $repo"
else
  bad=0
  for n in "${names[@]}"; do
    attempt=1
    # A just-created attestation can take a few seconds to become queryable.
    while ! out="$(gh attestation verify "$dl/$n" --repo "$repo" \
        --signer-workflow "$repo/$workflow" \
        --source-ref "refs/tags/$tag" --source-digest "$digest" 2>&1)"; do
      if [ "$attempt" -lt "${VERIFY_ATTEMPTS:-1}" ]; then
        attempt=$((attempt + 1)); sleep 5; continue
      fi
      bad=$((bad + 1))
      fail "no valid attestation from $workflow for $n ($(printf '%s' "$out" | tail -n 1 | cut -c1-120))"
      break
    done
  done
  [ "$bad" -eq 0 ] && ok "all ${#names[@]} assets carry an attestation from $workflow at $digest"
fi

# 4. GPG detached signatures
if [ -f "$dl/ncc-release-signing.gpg" ] && ls "$dl"/*.asc >/dev/null 2>&1; then
  if command -v gpg >/dev/null 2>&1; then
    gh_home="$work/gnupg"
    install -d -m 700 "$gh_home"
    if gpg --batch --homedir "$gh_home" --import "$dl/ncc-release-signing.gpg" >/dev/null 2>&1; then
      gbad=0
      for sig in "$dl"/*.asc; do
        base="${sig%.asc}"
        [ -f "$base" ] || { fail "$(basename "$sig") has no matching asset"; gbad=1; continue; }
        gpg --batch --homedir "$gh_home" --verify "$sig" "$base" >/dev/null 2>&1 \
          || { fail "GPG signature does not verify for $(basename "$base")"; gbad=1; }
      done
      [ "$gbad" -eq 0 ] && ok "GPG signatures verify against ncc-release-signing.gpg"
    else
      fail "ncc-release-signing.gpg could not be imported"
    fi
  else
    echo "warn: gpg not installed; GPG signatures not checked"
  fi
fi

if [ "$failures" -gt 0 ]; then
  echo "RESULT: FAILED ($failures problem(s)) for $repo $tag"
  exit 1
fi
echo "RESULT: PASSED for $repo $tag"
