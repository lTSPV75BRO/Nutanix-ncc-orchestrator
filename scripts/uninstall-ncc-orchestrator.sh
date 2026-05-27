#!/usr/bin/env bash
# Backward-compatible wrapper.
# Prefer using: ./scripts/uninstall-v2-clean.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_SCRIPT="$SCRIPT_DIR/uninstall-v2-clean.sh"

if [[ ! -x "$TARGET_SCRIPT" ]]; then
  chmod +x "$TARGET_SCRIPT" 2>/dev/null || true
fi

echo "Note: uninstall-ncc-orchestrator.sh is deprecated."
echo "Delegating to uninstall-v2-clean.sh ..."
exec "$TARGET_SCRIPT" "$@"
