#!/usr/bin/env bash
# Installs the Helm chart on a throwaway kind cluster from images built here and
# checks that the stack works: pods ready, sign-in required, a CronJob run
# against a simulated cluster (tools/fakeprism), its report served by the API,
# the dashboard answering over HTTPS, and backup, verify, retention preview and
# restore. Everything runs with the chart's read-only root filesystem.
#
# Needs docker, kind, kubectl, helm and go. Used by CI (helm-smoke) and locally:
#
#   scripts/helm-smoke-test.sh           # leaves nothing behind
#   KEEP=1 scripts/helm-smoke-test.sh    # keep the cluster to look around
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash on Windows: leave /paths in arguments alone

cd "$(dirname "$0")/.."
CLUSTER="${CLUSTER:-ncc-smoke}"
NS=ncc-orchestrator-v2
TAG=smoke
REG=localhost/ncc
CTX="kind-$CLUSTER"
# A path relative to the checkout works for both Git Bash and Docker on Windows.
WORK="dist/.helm-smoke"
# The API's static token: random, for this throwaway cluster only.
TOKEN="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"

k() { kubectl --context "$CTX" "$@"; }
step() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    step "diagnostics"
    k -n "$NS" get pods,pvc,jobs -o wide || true
    k -n "$NS" get events --sort-by=.lastTimestamp | tail -30 || true
    for l in component=api component=ui component=runner; do
      k -n "$NS" logs -l "$l" --all-containers --prefix --tail=40 || true
    done
  fi
  if [ "${KEEP:-0}" != "1" ]; then
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
    rm -rf "$WORK"
  fi
  exit "$status"
}
trap cleanup EXIT

step "build images"
rm -rf "$WORK"
mkdir -p "$WORK/frontend-dist"
arch="$(go env GOHOSTARCH)"
for c in ncc-orchestrator ncc-api-server ncc-ui-server; do
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -ldflags "-s -w" -o "$WORK/$c-linux-$arch" "./cmd/$c"
done
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -o "$WORK/fakeprism" ./tools/fakeprism
# The dashboard build is tested elsewhere; a page is enough to start the server.
echo '<!doctype html><title>NCC smoke test</title>' > "$WORK/frontend-dist/index.html"
: > "$WORK/ncc-host-cas.pem"
printf 'FROM alpine:3.24\nCOPY fakeprism /fakeprism\nRUN chmod 0755 /fakeprism\nUSER 10001:10001\nENTRYPOINT ["/fakeprism"]\n' > "$WORK/Dockerfile.fakeprism"
docker build -q --build-arg TARGETARCH="$arch" -f deploy/docker/Dockerfile.runner -t "$REG/nutanix-ncc-orchestrator:$TAG" "$WORK"
docker build -q --build-arg TARGETARCH="$arch" -f deploy/docker/Dockerfile.api -t "$REG/ncc-api-server:$TAG" "$WORK"
docker build -q --build-arg TARGETARCH="$arch" -f deploy/docker/Dockerfile.ui -t "$REG/ncc-ui-server:$TAG" "$WORK"
docker build -q -f "$WORK/Dockerfile.fakeprism" -t "$REG/fakeprism:$TAG" "$WORK"

step "create the cluster"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" ${KIND_NODE_IMAGE:+--image "$KIND_NODE_IMAGE"} --wait 180s
kind load docker-image --name "$CLUSTER" \
  "$REG/nutanix-ncc-orchestrator:$TAG" "$REG/ncc-api-server:$TAG" "$REG/ncc-ui-server:$TAG" "$REG/fakeprism:$TAG"

step "simulated Prism Element"
k create namespace fakeprism
k -n fakeprism run fakeprism --image "$REG/fakeprism:$TAG" --image-pull-policy=Never --port 9440 --expose
k -n fakeprism wait --for=condition=Ready pod/fakeprism --timeout=120s

# kind's storage class only supports ReadWriteOnce; one node, so that is enough.
chart=(helm/ncc-orchestrator
  --set "namespace=$NS"
  --set "images.runner.repository=$REG/nutanix-ncc-orchestrator" --set "images.runner.tag=$TAG" --set images.runner.pullPolicy=Never
  --set "images.api.repository=$REG/ncc-api-server" --set "images.api.tag=$TAG" --set images.api.pullPolicy=Never
  --set "images.ui.repository=$REG/ncc-ui-server" --set "images.ui.tag=$TAG" --set images.ui.pullPolicy=Never
  --set persistence.accessMode=ReadWriteOnce
  --set ui.service.type=ClusterIP)

step "install the chart"
k create namespace "$NS"
k -n "$NS" create secret generic ncc-orchestrator-credentials \
  --from-literal=api-token="$TOKEN" \
  --from-literal=jwt-secret="$TOKEN$TOKEN" \
  --from-literal=password=smoke \
  --from-literal=backup-key="$(head -c 32 /dev/urandom | base64)"
helm --kube-context "$CTX" install ncc "${chart[@]}"
k -n "$NS" rollout status deployment/ncc-v2-api --timeout=240s
k -n "$NS" rollout status deployment/ncc-v2-ui --timeout=240s

api() { # api METHOD PATH [JSON]: call the API from inside its pod, as admin
  local method="$1" path="$2" body="${3:-}"
  local url="http://127.0.0.1:8081$path"
  if [ "$method" = GET ]; then
    k -n "$NS" exec deploy/ncc-v2-api -c api -- wget -q -T 150 -O- --header "X-API-Token: $TOKEN" "$url"
  else
    k -n "$NS" exec deploy/ncc-v2-api -c api -- wget -q -T 150 -O- --header "X-API-Token: $TOKEN" --header "Content-Type: application/json" --post-data "$body" "$url"
  fi
}
must() { # must TEXT DESCRIPTION: stdin has to contain TEXT
  local out
  out="$(cat)"
  case "$out" in *"$1"*) echo "ok: $2" ;; *) echo "$out" | head -c 2000; echo; fail "$2 (no '$1' in the answer)" ;; esac
}

step "root filesystem is read-only"
for d in ncc-v2-api:api ncc-v2-ui:ui; do
  # The mount itself: as uid 1000 a write would fail on file permissions anyway.
  k -n "$NS" exec "deploy/${d%%:*}" -c "${d##*:}" -- grep -E '^[^ ]+ / [^ ]+ ro[, ]' /proc/mounts >/dev/null     || fail "the root filesystem of ${d%%:*} is writable"
done
echo "ok: mounted read-only"

step "API"
api GET /api/v1/health/ready | must '"success":true' "ready"
if k -n "$NS" exec deploy/ncc-v2-api -c api -- wget -q -O- http://127.0.0.1:8081/api/v1/runs >/dev/null 2>&1; then
  fail "the API answers without credentials"
fi
echo "ok: credentials required"

step "dashboard"
# Run from the API pod: the UI's NetworkPolicy allows any peer on its port, and
# the API image has wget. The certificate is the self-signed one it generated.
k -n "$NS" exec deploy/ncc-v2-api -c api -- wget -q --no-check-certificate -O- https://ncc-v2-ui/ | must "NCC smoke test" "served over HTTPS"
k -n "$NS" exec deploy/ncc-v2-api -c api -- wget -q --no-check-certificate -O- https://ncc-v2-ui/api/v1/health | must '"success":true' "proxies to the API"

step "run"
k -n "$NS" exec -i deploy/ncc-v2-api -c api -- sh -c 'cat > /data/config/config.yaml' <<'EOF'
clusters: "fakeprism.fakeprism"
username: "admin"
password: "secret://NCC_PASSWORD"
ncc-api-version: Legacy
cluster-source-mode: clusters
insecure-skip-verify: true
skip-preflight-check: true
poll-interval: "1s"
poll-jitter: "200ms"
outputs: "html,csv,json"
EOF
k -n "$NS" create job --from=cronjob/ncc-orchestrator smoke-run
# Wait for the run, keeping its output as it goes: when the container fails
# twice the Job deletes its pod, and with it the only copy of the log.
run_log="$WORK/smoke-run.log"
run_state=""
for _ in $(seq 1 120); do
  {
    k -n "$NS" logs job/smoke-run --all-containers --previous 2>/dev/null | sed 's/^/[previous] /'
    k -n "$NS" logs job/smoke-run --all-containers 2>/dev/null
  } > "$run_log.new" || true
  [ -s "$run_log.new" ] && mv "$run_log.new" "$run_log"
  run_state="$(k -n "$NS" get job smoke-run -o 'jsonpath={.status.conditions[?(@.status=="True")].type}')"
  case "$run_state" in *Complete*|*Failed*) break ;; esac
  sleep 2
done
# Conditions read "SuccessCriteriaMet Complete" or "FailureTarget Failed".
if ! case "$run_state" in *Failed*) false ;; *Complete*) true ;; *) false ;; esac; then
  echo "the run did not complete (job conditions: ${run_state:-none}); its output:"
  tail -60 "$run_log" 2>/dev/null || echo "(no output was captured)"
  fail "the CronJob run"
fi
api GET /api/v1/freshness | must '"address":"fakeprism.fakeprism"' "the run reached the simulated cluster"
api GET /api/v1/report/data | must "fakeprism.fakeprism" "its report is served"

step "backup, verify, retention preview and restore"
created="$(api POST /api/v1/settings/backups '{}')"
name="$(printf '%s' "$created" | sed -n 's/.*"name":"\([^"]*\)".*/\1/p' | head -1)"
[ -n "$name" ] || { echo "$created"; fail "no backup was created"; }
echo "ok: created $name"
api POST /api/v1/settings/backups/verify "{\"name\":\"$name\"}" | must '"success":true' "verified"
api GET /api/v1/settings/retention/preview | must '"success":true' "retention preview"
api POST /api/v1/settings/backups/restore "{\"name\":\"$name\"}" | must '"success":true' "restored"
k -n "$NS" rollout status deployment/ncc-v2-api --timeout=240s
api GET /api/v1/health/ready | must '"success":true' "ready after the restore"

step "helm upgrade: image and version checks, rollback"
h() { helm --kube-context "$CTX" upgrade ncc "${chart[@]}" --timeout 300s "$@"; }
version="$(k -n "$NS" exec deploy/ncc-v2-api -c api -- wget -q -O- http://127.0.0.1:8081/api/v1/health | sed -n 's/.*"version":"\([^"]*\)".*/\1/p')"
[ -n "$version" ] || fail "the API does not say its version"
backups_before="$(api GET /api/v1/settings/backups | grep -o '"name":"[^"]*"' | wc -l)"
h --set "upgradeCheck.expectVersion=$version" --set api.replicas=1 || fail "an upgrade to the same version"
echo "ok: upgraded, v$version expected and found"
backups_after="$(api GET /api/v1/settings/backups | grep -o '"name":"[^"]*"' | wc -l)"
[ "$backups_after" -gt "$backups_before" ] || fail "the pre-upgrade hook took no backup ($backups_before -> $backups_after)"
echo "ok: the pre-upgrade hook took a backup ($backups_before -> $backups_after)"
if h --set images.ui.tag=missing --set upgradeCheck.pullTimeoutSeconds=20 2>&1; then fail "a missing image was not noticed"; fi
[ "$(k -n "$NS" get deploy ncc-v2-ui -o 'jsonpath={.spec.template.spec.containers[0].image}')" = "$REG/ncc-ui-server:$TAG" ] \
  || fail "the UI was changed before its image was checked"
echo "ok: a missing image stops the upgrade before anything changes"
if h --set upgradeCheck.expectVersion=9.9.9 --set upgradeCheck.timeout=10s --set api.replicas=2 --atomic 2>&1; then fail "the wrong version was accepted"; fi
[ "$(k -n "$NS" get deploy ncc-v2-api -o 'jsonpath={.spec.replicas}')" = 1 ] || fail "the failed upgrade was not rolled back"
k -n "$NS" rollout status deployment/ncc-v2-api --timeout=240s
echo "ok: the wrong version rolls the upgrade back"

# Each pre-upgrade check must stop the upgrade before anything changes, and
# --atomic must leave the old revision in place. api.replicas=2 would show up
# in the Deployment if anything had been applied.
precheck_fails() { # precheck_fails DESCRIPTION TEXT-IN-THE-HOOK-LOG ARGS...
  local what="$1" text="$2" rev out
  shift 2
  rev="$(helm --kube-context "$CTX" history ncc -o json | sed -n 's/.*"revision":\([0-9]*\).*/\1/p' | tail -1)"
  if h --set api.replicas=2 --atomic "$@" 2>&1; then fail "$what was not noticed"; fi
  out="$(k -n "$NS" logs job/ncc-v2-upgrade-preflight 2>&1 || true)"
  echo "$out" | tail -5
  case "$out" in *"$text"*) ;; *) fail "$what: the hook log does not say '$text'" ;; esac
  [ "$(k -n "$NS" get deploy ncc-v2-api -o 'jsonpath={.spec.replicas}')" = 1 ] || fail "$what: the failed upgrade was not rolled back"
  helm --kube-context "$CTX" status ncc -o json | grep -q '"status":"deployed"' || fail "$what: the release is not deployed again"
  k -n "$NS" rollout status deployment/ncc-v2-api --timeout=240s
  echo "ok: $what stops the upgrade and --atomic keeps the old revision (was $rev)"
}
precheck_fails "too little free space" "free space" --set upgradeCheck.preCheck.minFreeSpace=100Ti
precheck_fails "a version older than min_from" "upgrade to v9.0.0 first" --set upgradeCheck.preCheck.minFrom=9.0.0

printf '\nHELM_SMOKE_OK\n'
