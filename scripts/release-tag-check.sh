#!/usr/bin/env bash
# release-tag-check.sh TAG - check that TAG is "v" plus the VERSION file, and
# print "prerelease=true" for a version with a suffix (2.4.0-rc.1), else
# "prerelease=false". Without this a tag v2.4.0-rc.1 on a commit whose VERSION
# is 2.4.0 would publish binaries that call themselves 2.4.0: they would never
# be offered the real 2.4.0.
set -euo pipefail

tag="${1:?usage: release-tag-check.sh TAG}"
cd "$(dirname "$0")/.."
version="$(tr -d '[:space:]' < VERSION)"
if [ "$tag" != "v$version" ]; then
  echo "tag $tag does not match VERSION ($version); stamp it with: bash scripts/binaryGO.txt --set-version ${tag#v}" >&2
  exit 1
fi
if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-(alpha|beta|rc)\.[0-9]+)?$ ]]; then
  echo "VERSION $version is neither X.Y.Z nor X.Y.Z-(alpha|beta|rc).N" >&2
  exit 1
fi
case "$version" in
  *-*) echo prerelease=true ;;
  *) echo prerelease=false ;;
esac
