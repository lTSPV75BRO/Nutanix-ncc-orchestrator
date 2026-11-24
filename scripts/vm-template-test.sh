#!/usr/bin/env bash
# Runs the VM provisioning templates (deploy/cloud-init, deploy/windows) on this
# host, with the releases that `scripts/upgrade-e2e.sh build OUT_DIR` made.
#
#   scripts/vm-template-test.sh OUT_DIR    # root/Administrator; Linux, or Git Bash on Windows
#
# It installs (and removes) a service named ncc-orchestrator and opens the
# template's firewall ports: use a throwaway host.
set -euo pipefail

out="$(cd "${1:?usage: vm-template-test.sh OUT_DIR}" && pwd)"
here="$(cd "$(dirname "$0")/.." && pwd)"
version=2.4.0 api=18081 ui=18080
# shellcheck disable=SC2016 # taken literally: nothing in it may be expanded
password='p"a\s$s wörd'
py=python3
command -v python3 >/dev/null 2>&1 || py=python

fail() { echo "FAIL: $*" >&2; exit 1; }
case "$(uname -s)" in
  Linux) os=linux dir=/opt/ncc-v2-test exe="" ;;
  *) os=windows dir=C:/ncc-v2-test exe=.exe ;;
esac
orch="$dir/bin/ncc-orchestrator$exe"
[ ! -e "$dir" ] || fail "$dir exists"
native="$dir"
[ "$os" = linux ] || native="$(cygpath -w "$dir")"

mirror="$(mktemp -d)"
"$py" - <<'EOF' "$mirror/port" "$out" &
import functools, http.server, os, sys
# Serves OUT_DIR/VERSION as /VERSION and /vVERSION.
class H(http.server.SimpleHTTPRequestHandler):
    def translate_path(self, path):
        return super().translate_path(path.replace("/v", "/", 1) if path.startswith("/v") else path)
    def log_message(self, *a):
        pass
s = http.server.ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(H, directory=sys.argv[2]))
with open(sys.argv[1] + ".new", "w") as f:
    f.write(str(s.server_address[1]))
os.replace(sys.argv[1] + ".new", sys.argv[1])
s.serve_forever()
EOF
server=$!
finish() {
  local status=$?
  [ "$status" = 0 ] || tail -n 30 "$dir/logs/v2-supervisor.log" 2>/dev/null || true
  "$orch" v2-uninstall-service --install-dir "$dir" >/dev/null 2>&1 || true
  kill "$server" 2>/dev/null || true
  rm -rf "$mirror"
  exit "$status"
}
trap finish EXIT
for ((i = 0; i < 50; i++)); do
  [ -s "$mirror/port" ] && break
  sleep 0.2
done
url="http://127.0.0.1:$(cat "$mirror/port")"

export NCC_VERSION="$version" NCC_RELEASE_BASE_URL="$url" NCC_VERIFY_ATTESTATION=off NCC_INSTALL_DIR="$native" \
  NCC_API_PORT="$api" NCC_UI_PORT="$ui" NCC_CLUSTERS='"10.0.0.1"' NCC_PCS="" NCC_USERNAME=admin NCC_PASSWORD="$password"
if [ "$os" = linux ]; then
  # The installer as cloud-init writes it, with other settings.
  "$py" - "$here/deploy/cloud-init/ncc-v2.yaml" "$mirror/install" <<'EOF'
import sys
lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
i = next(n for n, l in enumerate(lines) if "/usr/local/sbin/install-ncc-v2" in l)
i = next(n for n in range(i, len(lines)) if lines[n].strip() == "content: |") + 1
body = []
while i < len(lines) and (lines[i].startswith("      ") or not lines[i].strip()):
    body.append(lines[i][6:])
    i += 1
text = "\n".join(body).rstrip() + "\n"
assert ". /etc/ncc/ncc-install.env\n" in text
open(sys.argv[2], "w", encoding="utf-8").write(text.replace(". /etc/ncc/ncc-install.env\n", ""))
EOF
  sh "$mirror/install"
else
  powershell -NoProfile -ExecutionPolicy Bypass -File "$here/deploy/windows/setup-ncc.ps1"
fi

# The orchestrator reads some of them too, before its configuration file.
unset NCC_VERSION NCC_RELEASE_BASE_URL NCC_VERIFY_ATTESTATION NCC_INSTALL_DIR NCC_API_PORT NCC_UI_PORT NCC_CLUSTERS NCC_PCS NCC_USERNAME NCC_PASSWORD

health=""
for ((i = 0; i < 60; i++)); do
  health="$(curl -sk --max-time 5 "http://127.0.0.1:$api/api/v1/health" || true)"
  [[ "$health" == *"\"version\":\"$version\""* ]] && break
  sleep 2
done
[[ "$health" == *"\"version\":\"$version\""* ]] || fail "the API does not answer on port $api: $health"
page=""
for ((i = 0; i < 30; i++)); do # the UI starts after the API
  page="$(curl -sk --max-time 10 "https://127.0.0.1:$ui/" || true)"
  [[ "$page" == *"<title>NCC $version<"* ]] && break
  sleep 2
done
[[ "$page" == *"<title>NCC $version<"* ]] || fail "the UI does not answer on port $ui"
echo "ok: API and UI run on the chosen ports"

"$orch" validate-config --config "$dir/config.yaml" || fail "the configuration is not valid"
got="$("$py" -c 'import json, sys; sys.stdout.reconfigure(encoding="utf-8"); print(json.load(open(sys.argv[1], encoding="utf-8"))["NCC_PASSWORD"])' "$dir/secrets.json" | tr -d '\r')"
[ "$got" = "$password" ] || fail "the password was changed on the way: $got"
if [ "$os" = linux ]; then
  [ "$(stat -c %a "$dir/secrets.json")" = 600 ] || fail "others can read the password"
  systemctl is-enabled ncc-orchestrator.service ncc-log-maintenance.timer >/dev/null || fail "not started at boot"
else
  for task in ncc-orchestrator ncc-orchestrator-watchdog; do
    schtasks //Query //TN "$task" >/dev/null || fail "no task $task"
  done
fi
plan="$("$orch" upgrade --plan --offline --install-dir "$dir")"
[[ "$plan" == *"Update helper:      ready"* ]] || fail "the update helper is not ready: $plan"
echo "ok: configuration, password, service and update helper"
echo "VM_TEMPLATE_OK ($os)"
