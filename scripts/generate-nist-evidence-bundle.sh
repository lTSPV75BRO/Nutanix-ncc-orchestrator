#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Generate a NIST CSF evidence bundle from docs/NIST_CSF_EVIDENCE_MANIFEST.json.

Usage:
  scripts/generate-nist-evidence-bundle.sh [--manifest PATH] [--out-dir PATH] [--dry-run]

Options:
  --manifest PATH  Manifest JSON path (default: docs/NIST_CSF_EVIDENCE_MANIFEST.json)
  --out-dir PATH   Output directory (default: dist/compliance)
  --dry-run        Validate manifest and print evidence stats without writing bundle
EOF
}

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
MANIFEST="docs/NIST_CSF_EVIDENCE_MANIFEST.json"
OUT_DIR="dist/compliance"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest)
      MANIFEST="${2:-}"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

cd "$ROOT"
if [[ ! -f "$MANIFEST" ]]; then
  echo "Manifest not found: $MANIFEST" >&2
  exit 1
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
GIT_REV="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
BUNDLE_DIR="$OUT_DIR/nist-csf-evidence-$STAMP"
TARBALL="$BUNDLE_DIR.tar.gz"

TMP_LIST="$(mktemp)"
python3 - "$MANIFEST" "$ROOT" "$TMP_LIST" <<'PY'
import json
import pathlib
import sys

manifest_path = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2])
out_list = pathlib.Path(sys.argv[3])
obj = json.loads(manifest_path.read_text())
controls = obj.get("controls", [])
paths = set()
for c in controls:
    for ev in c.get("evidence", []):
        if not ev:
            continue
        p = pathlib.Path(ev)
        if p.is_absolute():
            continue
        paths.add(p.as_posix())
paths = sorted(paths)
out_list.write_text("\n".join(paths) + ("\n" if paths else ""))
existing = 0
for p in paths:
    if (root / p).exists():
        existing += 1
print(f"controls={len(controls)} evidence_paths={len(paths)} existing_paths={existing}")
PY

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "Dry-run complete. No bundle created."
  rm -f "$TMP_LIST"
  exit 0
fi

mkdir -p "$BUNDLE_DIR/evidence"

while IFS= read -r rel; do
  [[ -z "$rel" ]] && continue
  src="$ROOT/$rel"
  if [[ -e "$src" ]]; then
    dst="$BUNDLE_DIR/evidence/$rel"
    mkdir -p "$(dirname "$dst")"
    cp -a "$src" "$dst"
  fi
done < "$TMP_LIST"

cp -a "$MANIFEST" "$BUNDLE_DIR/manifest.json"
cp -a "docs/NIST_CSF_BASELINE.md" "$BUNDLE_DIR/NIST_CSF_BASELINE.md"

python3 - "$BUNDLE_DIR/metadata.json" "$STAMP" "$GIT_REV" "$MANIFEST" "$TMP_LIST" <<'PY'
import json
import pathlib
import sys

meta_path = pathlib.Path(sys.argv[1])
stamp = sys.argv[2]
git_rev = sys.argv[3]
manifest = sys.argv[4]
list_file = pathlib.Path(sys.argv[5])
paths = [ln.strip() for ln in list_file.read_text().splitlines() if ln.strip()]
meta = {
    "generated_at_utc": stamp,
    "git_revision": git_rev,
    "framework": "NIST CSF 2.0",
    "manifest_path": manifest,
    "evidence_paths_count": len(paths),
    "generator": "scripts/generate-nist-evidence-bundle.sh",
}
meta_path.write_text(json.dumps(meta, indent=2) + "\n")
PY

tar -czf "$TARBALL" -C "$OUT_DIR" "$(basename "$BUNDLE_DIR")"
rm -f "$TMP_LIST"

echo "Evidence bundle created:"
echo "  directory: $BUNDLE_DIR"
echo "  archive:   $TARBALL"
