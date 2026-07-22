#!/usr/bin/env bash
# Clean uninstaller for NCC Orchestrator v2 Kubernetes stack.
#
# Removes v2 Kubernetes resources, namespace, and optionally:
# - PVC data
# - local v2 runtime state files
# - NCC images on worker nodes (via prune script)
#
# Usage:
#   ./scripts/uninstall-v2-clean.sh [options]
#
# Options:
#   --namespace <name>         Namespace to uninstall (default: ncc-orchestrator-v2)
#   --k8s-dir <path>           Path to k8s manifests (default: <repo>/k8s)
#   --force, -f                Skip confirmation prompts
#   --dry-run                  Print actions only
#   --wait-timeout <seconds>   Namespace deletion wait timeout (default: 300)
#   --keep-pvc                 Keep PVC (default deletes ncc-v2-data PVC)
#   --remove-local-state       Remove local runtime files and install dir
#   --local-install-dir <dir>  Local v2 install dir to remove (default: .ncc-v2)
#   --prune-images             Run worker image prune after uninstall
#   --skip-prune-prompt        Do not prompt for prune-images when unset
#   --help, -h                 Show this help

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

NAMESPACE="${NAMESPACE:-ncc-orchestrator-v2}"
K8S_DIR="$REPO_ROOT/k8s"
FORCE=false
DRY_RUN=false
KEEP_PVC=false
REMOVE_LOCAL_STATE=false
LOCAL_INSTALL_DIR="${LOCAL_INSTALL_DIR:-.ncc-v2}"
PRUNE_IMAGES=false
SKIP_PRUNE_PROMPT=false
WAIT_TIMEOUT=300

usage() {
  sed -n '1,40p' "$0"
}

confirm() {
  local prompt="$1"
  if [[ "$FORCE" == "true" ]]; then
    return 0
  fi
  read -r -p "$prompt [y/N]: " ans
  case "$ans" in
    [yY]|[yY][eE][sS]) return 0 ;;
    *) return 1 ;;
  esac
}

run_cmd() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[dry-run] $*"
  else
    "$@"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)
      NAMESPACE="${2:-}"
      shift 2
      ;;
    --k8s-dir)
      K8S_DIR="${2:-}"
      shift 2
      ;;
    --force|-f)
      FORCE=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --wait-timeout)
      WAIT_TIMEOUT="${2:-300}"
      shift 2
      ;;
    --keep-pvc)
      KEEP_PVC=true
      shift
      ;;
    --remove-local-state)
      REMOVE_LOCAL_STATE=true
      shift
      ;;
    --local-install-dir)
      LOCAL_INSTALL_DIR="${2:-.ncc-v2}"
      shift 2
      ;;
    --prune-images)
      PRUNE_IMAGES=true
      shift
      ;;
    --skip-prune-prompt)
      SKIP_PRUNE_PROMPT=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found in PATH."
  exit 1
fi

echo "NCC v2 clean uninstall plan"
echo "  Namespace           : $NAMESPACE"
echo "  K8s manifests dir   : $K8S_DIR"
echo "  Delete PVC          : $([[ "$KEEP_PVC" == "true" ]] && echo "no" || echo "yes")"
echo "  Remove local state  : $REMOVE_LOCAL_STATE"
echo "  Prune worker images : $PRUNE_IMAGES"
echo "  Dry-run             : $DRY_RUN"
echo ""

if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "Current resources in namespace $NAMESPACE:"
  kubectl get all,configmap,secret,pvc,cronjob,job,networkpolicy -n "$NAMESPACE" 2>/dev/null || true
else
  echo "Namespace '$NAMESPACE' not found (will still attempt cleanup where applicable)."
fi
echo ""

if [[ "$DRY_RUN" != "true" ]]; then
  if ! confirm "Proceed with NCC v2 uninstall?"; then
    echo "Aborted."
    exit 1
  fi
fi

# 1) Delete kustomize resources first when manifests dir exists.
if [[ -d "$K8S_DIR" ]]; then
  echo "Deleting resources from kustomization: $K8S_DIR"
  run_cmd kubectl delete -k "$K8S_DIR" --ignore-not-found=true || true
else
  echo "K8s dir not found at '$K8S_DIR', skipping kubectl delete -k."
fi

# 2) Delete PVC unless kept.
if [[ "$KEEP_PVC" != "true" ]]; then
  echo "Deleting PVC ncc-v2-data in namespace $NAMESPACE (if present)."
  run_cmd kubectl delete pvc ncc-v2-data -n "$NAMESPACE" --ignore-not-found=true || true
else
  echo "Keeping PVC as requested (--keep-pvc)."
fi

# 3) Delete namespace.
echo "Deleting namespace $NAMESPACE."
run_cmd kubectl delete namespace "$NAMESPACE" --ignore-not-found=true || true

# 4) Wait for namespace removal.
if [[ "$DRY_RUN" != "true" ]]; then
  echo "Waiting up to ${WAIT_TIMEOUT}s for namespace deletion..."
  end=$((SECONDS + WAIT_TIMEOUT))
  while (( SECONDS < end )); do
    if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
      echo "Namespace '$NAMESPACE' removed."
      break
    fi
    sleep 2
  done
  if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "Namespace '$NAMESPACE' is still terminating. Check finalizers/events:"
    echo "  kubectl get namespace $NAMESPACE -o yaml"
    echo "  kubectl get events -n $NAMESPACE --sort-by=.lastTimestamp"
  fi
fi

# 5) Optional local state cleanup.
if [[ "$REMOVE_LOCAL_STATE" == "true" ]]; then
  echo "Removing local runtime state files."
  run_cmd rm -f "$REPO_ROOT/.ncc-api-token" "$REPO_ROOT/.ncc-api-schedule.json" "$REPO_ROOT/.ncc-api-notifications.json"
  run_cmd rm -rf "$REPO_ROOT/$LOCAL_INSTALL_DIR"
fi

# 6) Optional image prune step.
run_prune=false
if [[ "$PRUNE_IMAGES" == "true" ]]; then
  run_prune=true
elif [[ "$SKIP_PRUNE_PROMPT" != "true" && "$DRY_RUN" != "true" ]]; then
  if confirm "Also prune NCC images from worker nodes via SSH?"; then
    run_prune=true
  fi
fi

if [[ "$run_prune" == "true" ]]; then
  echo "Running worker image prune script..."
  run_cmd "$REPO_ROOT/scripts/prune-ncc-images-workers.sh"
fi

echo "Uninstall flow completed."
