#!/usr/bin/env bash
# Remove NCC orchestrator container images from worker nodes via SSH.
# Removes ALL images for the NCC repo (same tag, old digest included) so the next
# pod pull gets the current image.
# Requires: kubectl (KUBECONFIG set), SSH key access (e.g. konvoy@<node-IP>).
#
# Usage: ./scripts/prune-ncc-images-workers.sh [options]
#
# Optional env (set before running):
#   SSH_KEY       Path to private key (e.g. ~/.ssh/konvoy_rsa). Use if default key fails.
#   SSH_OPTS      Extra ssh options (e.g. "-i ~/.ssh/konvoy_rsa").
#   SSH_USER      SSH user (default: konvoy).
#   NODE_IPS      Override node list (space-separated IPs). If set, skips kubectl node discovery.
#   NAMESPACE     Namespace for context display only (default: ncc-orchestrator-v2).
#   NCC_IMAGE_NAME Image match pattern (default: nutanix-ncc-orchestrator).

set -euo pipefail

DRY_RUN=false
NAMESPACE="${NAMESPACE:-ncc-orchestrator-v2}"
SSH_USER="${SSH_USER:-konvoy}"
SSH_OPTS="${SSH_OPTS:-}"
NODE_IPS_ARG=""
NCC_IMAGE_NAME="${NCC_IMAGE_NAME:-nutanix-ncc-orchestrator}"

usage() {
  cat <<'EOF'
Usage: ./scripts/prune-ncc-images-workers.sh [options]

Options:
  --dry-run                  Show actions only
  --node-ips "ip1 ip2"       Override node IP discovery
  --ssh-user <user>          SSH user (default: konvoy)
  --ssh-key <path>           SSH private key path
  --ssh-opts "<opts>"        Extra SSH options
  --image-match <pattern>    Image name/pattern to remove (default: nutanix-ncc-orchestrator)
  --namespace <name>         Namespace context for output only
  --help, -h                 Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --node-ips)
      NODE_IPS_ARG="${2:-}"
      shift 2
      ;;
    --ssh-user)
      SSH_USER="${2:-konvoy}"
      shift 2
      ;;
    --ssh-key)
      SSH_KEY="${2:-}"
      shift 2
      ;;
    --ssh-opts)
      SSH_OPTS="${2:-}"
      shift 2
      ;;
    --image-match)
      NCC_IMAGE_NAME="${2:-nutanix-ncc-orchestrator}"
      shift 2
      ;;
    --namespace)
      NAMESPACE="${2:-ncc-orchestrator-v2}"
      shift 2
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

if ! command -v ssh >/dev/null 2>&1; then
  echo "ssh not found in PATH."
  exit 1
fi

if [[ -n "${SSH_KEY:-}" ]]; then
  SSH_OPTS="-i ${SSH_KEY} ${SSH_OPTS}"
fi

# Node IPs: override with NODE_IPS, or discover workers (no control-plane role)
if [[ -n "$NODE_IPS_ARG" ]]; then
  WORKER_IPS="$NODE_IPS_ARG"
elif [[ -n "${NODE_IPS:-}" ]]; then
  WORKER_IPS="${NODE_IPS}"
else
  WORKER_IPS="$(
    kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.labels.node-role\.kubernetes\.io/control-plane}{"\t"}{.metadata.labels.node-role\.kubernetes\.io/master}{"\t"}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' \
      | awk -F'\t' '$1 != "true" && $2 != "true" {print $3}'
  )"
fi
if [[ -z "$WORKER_IPS" ]]; then
  echo "No node IPs found. Set NODE_IPS or check KUBECONFIG / kubectl get nodes."
  exit 1
fi

echo "NCC image prune"
echo "  Namespace context: $NAMESPACE"
echo "Node IPs (SSH ${SSH_USER}@<ip>): $WORKER_IPS"
echo "Removing all images matching: ${NCC_IMAGE_NAME}"
[[ -n "$SSH_OPTS" ]] && echo "SSH options: $SSH_OPTS"
echo ""

# On node: list image IDs for NCC repo, then rmi each (removes every digest with same tag)
for ip in $WORKER_IPS; do
  echo "--- $ip ---"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "  [dry-run] would run: list and remove all image IDs matching ${NCC_IMAGE_NAME}"
    continue
  fi
  ssh $SSH_OPTS -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "${SSH_USER}@${ip}" "set -e
    echo 'NCC images before:'
    sudo crictl images | grep -E '${NCC_IMAGE_NAME}|REPOSITORY' || true
    ids=\$(sudo crictl images --no-trunc | grep '${NCC_IMAGE_NAME}' | awk '{print \$3}' || true)
    if [ -n \"\$ids\" ]; then
      for id in \$ids; do echo \"  Removing \$id\"; sudo crictl rmi \"\$id\" || true; done
    else
      echo '  No NCC images found.'
    fi
    echo 'Done.'
  " || { echo "  SSH or crictl failed for $ip"; continue; }
done

echo ""
echo "Done. Next CronJob/Job run will pull the current image."
