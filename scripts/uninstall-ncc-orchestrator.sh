#!/usr/bin/env bash
# Uninstall NCC Orchestrator: delete the namespace and everything in it
# (CronJob, Jobs, Deployment, Service, ConfigMap, Secret, PVC, etc.).
# Optionally prune NCC container images from worker nodes (prompted or via --prune-images).
#
# Usage: ./scripts/uninstall-ncc-orchestrator.sh [--force] [--dry-run] [--prune-images]
#
# Optional env: KUBECONFIG (e.g. export KUBECONFIG=~/kubecon/wolverine.conf)
#
# Options:
#   --force         Skip confirmation prompt
#   --dry-run       Only print what would be deleted
#   --prune-images  After uninstall, also run prune-ncc-images-workers.sh (removes NCC images from nodes).
#                   If not set, you will be asked after the namespace is deleted. For prune, set SSH_KEY if needed.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="${NAMESPACE:-ncc-orchestrator}"
FORCE=false
DRY_RUN=false
PRUNE_IMAGES=false
for arg in "$@"; do
  case "$arg" in
    --force|-f)     FORCE=true ;;
    --dry-run)      DRY_RUN=true ;;
    --prune-images) PRUNE_IMAGES=true ;;
  esac
done

if ! kubectl get namespace "$NAMESPACE" &>/dev/null; then
  echo "Namespace '$NAMESPACE' does not exist. Nothing to do."
  exit 0
fi

echo "Uninstall NCC Orchestrator"
echo "  Namespace: $NAMESPACE"
echo "  This will delete the namespace and ALL resources in it:"
echo "    CronJob, Jobs, Deployment (nginx), Service, ConfigMap(s), Secret, PVC, etc."
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[dry-run] Would delete the following in namespace $NAMESPACE:"
  kubectl get all,configmap,secret,pvc,cronjob,job -n "$NAMESPACE" 2>/dev/null || true
  echo ""
  echo "[dry-run] Would run: kubectl delete namespace $NAMESPACE"
  echo "[dry-run] Optionally you would be asked: Also prune NCC images from worker nodes? (use --prune-images to do it without prompt)"
  exit 0
fi

if [[ "$FORCE" != "true" ]]; then
  echo "Proceed? [y/N]"
  read -r ans
  case "$ans" in
    [yY]|[yY][eE][sS]) ;;
    *) echo "Aborted."; exit 1 ;;
  esac
fi

echo "Deleting namespace $NAMESPACE..."
kubectl delete namespace "$NAMESPACE" --ignore-not-found=true --timeout=120s

echo "Waiting for namespace to be gone..."
for i in $(seq 1 30); do
  if ! kubectl get namespace "$NAMESPACE" &>/dev/null; then
    echo "Namespace '$NAMESPACE' removed."
    run_prune=false
    if [[ "$PRUNE_IMAGES" == "true" ]]; then
      run_prune=true
    else
      echo ""
      echo "Also prune NCC container images from worker nodes? (removes old/same-tag images; requires SSH access, set SSH_KEY if needed) [y/N]"
      read -r ans
      case "$ans" in
        [yY]|[yY][eE][sS]) run_prune=true ;;
      esac
    fi
    if [[ "$run_prune" == "true" ]]; then
      echo ""
      echo "Running prune script: $REPO_ROOT/scripts/prune-ncc-images-workers.sh"
      "$REPO_ROOT/scripts/prune-ncc-images-workers.sh" || echo "Prune script failed (e.g. SSH key). You can run it manually with SSH_KEY set."
    fi
    exit 0
  fi
  sleep 2
done

echo "Namespace may still be terminating. Check with: kubectl get namespace $NAMESPACE"
exit 0
