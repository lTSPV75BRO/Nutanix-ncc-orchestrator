#!/usr/bin/env bash
# Remove NCC orchestrator container images from worker nodes via SSH.
# Removes ALL images for the NCC repo (same tag, old digest included) so the next
# pod pull gets the current image.
# Requires: kubectl (KUBECONFIG set), SSH key access (e.g. konvoy@<node-IP>).
#
# Usage: ./scripts/prune-ncc-images-workers.sh [--dry-run]
#
# Optional env (set before running):
#   SSH_KEY       Path to private key (e.g. ~/.ssh/konvoy_rsa). Use if default key fails.
#   SSH_OPTS      Extra ssh options (e.g. "-i ~/.ssh/konvoy_rsa").
#   SSH_USER      SSH user (default: konvoy).
#   NODE_IPS      Override node list (space-separated IPs). If set, skips kubectl node discovery.

set -e
DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

# Image name to match (same as in cronjob.yaml / job-debug.yaml)
NCC_IMAGE_NAME="${NCC_IMAGE_NAME:-nutanix-ncc-orchestrator}"

# Node IPs: override with NODE_IPS, or discover workers (no control-plane role)
if [[ -n "${NODE_IPS:-}" ]]; then
  WORKER_IPS="$NODE_IPS"
else
  WORKER_IPS=$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.labels.node-role\.kubernetes\.io/control-plane}{"\t"}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' | awk -F'\t' '$1 != "true" {print $2}')
fi
if [[ -z "$WORKER_IPS" ]]; then
  echo "No node IPs found. Set NODE_IPS or check KUBECONFIG / kubectl get nodes."
  exit 1
fi

SSH_USER="${SSH_USER:-konvoy}"
SSH_OPTS="${SSH_OPTS:-}"
[[ -n "${SSH_KEY:-}" ]] && SSH_OPTS="-i ${SSH_KEY} ${SSH_OPTS}"

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
