#!/usr/bin/env bash
# stage-release-assets.sh SRC_DIR DEST_DIR
#
# Copies exactly the files that a release publishes from the build output
# (SRC_DIR, normally dist/) into DEST_DIR (normally release-assets/). This
# directory is the single definition of "what is published": the same set is
# attested, uploaded as the workflow artifact, uploaded to the release, and
# compared against the release assets afterwards. Raw ncc-api-server-* and
# ncc-ui-server-* binaries are in the stacks and are intentionally not shipped.
set -euo pipefail

src="${1:?usage: stage-release-assets.sh SRC_DIR DEST_DIR}"
dest="${2:?usage: stage-release-assets.sh SRC_DIR DEST_DIR}"

[ -d "$src" ] || { echo "source directory $src not found" >&2; exit 1; }
rm -rf "$dest"
mkdir -p "$dest"
shopt -s nullglob

copy_required() {
  local f
  for f in "$@"; do
    [ -f "$src/$f" ] || { echo "required release file missing: $src/$f" >&2; exit 1; }
    cp -p "$src/$f" "$dest/"
  done
}
copy_glob() { # copy_glob PATTERN MIN_COUNT
  # shellcheck disable=SC2206 # the unquoted pattern is the point: it is a glob
  local -a m=("$src"/$1)
  [ "${#m[@]}" -ge "$2" ] || { echo "expected at least $2 files matching $1, found ${#m[@]}" >&2; exit 1; }
  cp -p "${m[@]}" "$dest/"
}

copy_required checksums.txt checksums.txt.sig ncc-release-signing.gpg \
  release-attestation.json example_config.yaml
copy_glob 'ncc-orchestrator-*' 6
copy_glob 'ncc-v2-stack-*' 6
copy_glob 'bom-*.cdx.json' 1
notes=("$src"/RELEASE_NOTES_*.md)
if [ "${#notes[@]}" -gt 0 ]; then
  cp -p "${notes[@]}" "$dest/"
else
  echo "::warning::no RELEASE_NOTES_*.md in $src; the release will use generated notes only"
fi

# A directory or odd file name would break upload and verification.
for f in "$dest"/*; do
  [ -f "$f" ] || { echo "unexpected non-file in $dest: $f" >&2; exit 1; }
done
echo "staged $(find "$dest" -maxdepth 1 -type f | wc -l | tr -d ' ') files in $dest"
