#!/usr/bin/env bash
# Rehearses real upgrades with the update helper on this host: upgrade, adoption
# of an older install, rollback by hand, a supervisor that dies, a release that
# does not start, and a helper that is killed half way.
#
#   scripts/upgrade-e2e.sh build OUT_DIR           # builds the test releases (needs go)
#   scripts/upgrade-e2e.sh run OUT_DIR INSTALL_DIR # service manager of this OS; root/Administrator
#   scripts/upgrade-e2e.sh run OUT_DIR INSTALL_DIR manual API_PORT UI_PORT
#
# "run" installs (and removes) a service named ncc-orchestrator: use a throwaway
# host. On Windows run it in Git Bash. The releases are unsigned test builds.
set -euo pipefail

mode="${1:?usage: upgrade-e2e.sh build OUT_DIR | run OUT_DIR INSTALL_DIR [manual API_PORT UI_PORT]}"
out="${2:?OUT_DIR}"
here="$(cd "$(dirname "$0")/.." && pwd)"

case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) os=windows ;;
esac
exe=""
[ "$os" = windows ] && exe=.exe

step() { printf '\n== %s\n' "$*"; }
ok() { echo "ok: $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
sha() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1; }

A=2.4.0 B=2.4.1 BAD=2.4.2

if [ "$mode" = build ]; then
  goos="${GOOS:-$(go env GOHOSTOS)}"
  goarch="${GOARCH:-$(go env GOHOSTARCH)}"
  e=""
  [ "$goos" = windows ] && e=.exe
  mkdir -p "$out"
  out="$(cd "$out" && pwd)"
  cd "$here"
  for v in "$A" "$B" "$BAD"; do
    root="$out/$v/root"
    rm -rf "${out:?}/$v"
    mkdir -p "$root/bin" "$root/frontend-dist"
    for c in ncc-orchestrator ncc-api-server ncc-ui-server; do
      CGO_ENABLED=0 GOOS=$goos GOARCH=$goarch go build -trimpath -ldflags "-s -w -X main.Version=$v" -o "$root/bin/$c$e" "./cmd/$c"
    done
    if [ "$v" = "$BAD" ]; then
      # Says its version, but does not serve.
      mkdir -p "$out/$v/src"
      printf 'package main\nimport ("fmt"; "os")\nfunc main() {\n\tif len(os.Args) > 1 && os.Args[1] == "version" {\n\t\tfmt.Println("ncc-api-server")\n\t\tfmt.Println("  version: %s")\n\t\treturn\n\t}\n\tos.Exit(1)\n}\n' "$v" > "$out/$v/src/main.go"
      (cd "$out/$v/src" && CGO_ENABLED=0 GOOS=$goos GOARCH=$goarch GO111MODULE=off go build -o "$root/bin/ncc-api-server$e" main.go)
    fi
    echo "<!doctype html><title>NCC $v</title>" > "$root/frontend-dist/index.html"
    cp example_config.yaml "$root/" 2>/dev/null || true
    if [ "$goos" = windows ]; then
      pkg="ncc-v2-stack-$goos-$goarch.zip"
      if command -v zip >/dev/null 2>&1; then (cd "$root" && zip -qr "../$pkg" .)
      else (cd "$root" && powershell -NoProfile -Command "Compress-Archive -Path * -DestinationPath ../$pkg"); fi
    else
      pkg="ncc-v2-stack-$goos-$goarch.tar.gz"
      tar -C "$root" -czf "$out/$v/$pkg" bin frontend-dist
    fi
    printf '%s  %s\n' "$(sha "$out/$v/$pkg")" "$pkg" > "$out/$v/checksums.txt"
  done
  echo "built $A, $B and $BAD in $out"
  exit 0
fi

dir="${3:?INSTALL_DIR}"
manual=0 api=8081 ui=8080
if [ "${4:-}" = manual ]; then manual=1 api="${5:?API_PORT}" ui="${6:?UI_PORT}"; fi
out="$(cd "$out" && pwd)"
orch="$dir/bin/ncc-orchestrator$exe"
helper_unit=/etc/systemd/system/ncc-orchestrator-upgrade
bundle() { echo "$out/$1/"ncc-v2-stack-*.*; }

get() { curl -sk --max-time 10 "http://127.0.0.1:$api/api/v1/health" || curl -sk --max-time 10 "https://127.0.0.1:$api/api/v1/health" || true; }
version() { get | sed -n 's/.*"version":"\([^"]*\)".*/\1/p'; }
title() { curl -sk --max-time 10 "https://127.0.0.1:$ui/" | sed -n 's/.*<title>NCC \([^<]*\)<.*/\1/p'; }
wait_for() { # wait_for VERSION SECONDS
  local i v=""
  for ((i = 0; i < $2; i += 2)); do
    v="$(version)"
    [ "$v" = "$1" ] && return 0
    sleep 2
  done
  fail "the API runs '${v}', not v$1"
}
job_phase() { "$orch" upgrade --status --json --install-dir "$dir" | sed -n 's/^{"id":"[^"]*","phase":"\([^"]*\)".*/\1/p'; }
expect_job() { # expect_job PHASE
  local p
  p="$(job_phase)"
  [ "$p" = "$1" ] || { "$orch" upgrade --status --install-dir "$dir"; fail "the job ended as '$p', not '$1'"; }
}
check_version() { # check_version VERSION: programs, API and UI
  wait_for "$1" 90
  for c in ncc-orchestrator ncc-api-server ncc-ui-server; do
    "$dir/bin/$c$exe" version | grep -qi "version: *v\{0,1\}$1" || fail "$c is not v$1"
  done
  for ((i = 0; i < 20; i++)); do
    [ "$(title)" = "$1" ] && return 0
    sleep 1
  done
  fail "the UI serves '$(title)', not $1"
}
upgrade_to() { "$orch" upgrade --install-dir "$dir" --bundle "$(bundle "$1")" --skip-backup; }
helper_running() {
  if [ "$os" = windows ]; then tasklist //FI "IMAGENAME eq ncc-upgrade-helper.exe" | grep -q ncc-upgrade-helper
  else pgrep -f 'helper/ncc-upgrade-helper upgrade-helper' >/dev/null; fi
}
kill_helper() {
  if [ "$os" = windows ]; then taskkill //F //IM ncc-upgrade-helper.exe
  else pkill -9 -f 'helper/ncc-upgrade-helper upgrade-helper'; fi
}

diagnose() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    step "diagnostics"
    cat "$dir/run/v2-upgrade-state.json" 2>/dev/null || true
    tail -n 60 "$dir/logs/upgrade-helper.log" 2>/dev/null || true
    tail -n 40 "$dir/logs/v2-supervisor.log" 2>/dev/null || true
    tail -n 20 "$dir/logs/v2-api.log" 2>/dev/null | grep -v "password:" || true
    if [ "$os" = linux ] && [ "$manual" = 0 ]; then
      systemctl status 'ncc-orchestrator*' --no-pager -l 2>/dev/null | tail -n 80 || true
    fi
  fi
  cleanup >/dev/null 2>&1 || true
  exit "$status"
}
cleanup() {
  if [ "$manual" = 1 ]; then "$orch" v2-stop --install-dir "$dir" || true
  else "$orch" v2-uninstall-service --install-dir "$dir" || true; fi
}
trap diagnose EXIT

step "install v$A"
[ ! -e "$dir" ] || fail "$dir exists"
mkdir -p "$dir"
cp -R "$out/$A/root/." "$dir/"
chmod -R go-w "$dir"
cat > "$dir/config.yaml" <<'EOF'
clusters: "127.0.0.1"
username: "admin"
password: "secret://NCC_PASSWORD"
EOF
if [ "$manual" = 1 ]; then
  "$orch" v2-start --install-dir "$dir" --detach --api-listen "127.0.0.1:$api" --ui-listen "127.0.0.1:$ui" > "$dir.start.log" 2>&1 || { cat "$dir.start.log"; fail "v2-start"; }
  rm -f "$dir.start.log"
else
  "$orch" v2-install-service --install-dir "$dir" --orchestrator-bin "$orch" --now
fi
check_version "$A"
"$orch" upgrade --plan --offline --install-dir "$dir"
ok "installed and running"

if [ "$os" = linux ] && [ "$manual" = 0 ]; then
  unit=/etc/systemd/system/ncc-orchestrator.service
  step "a line of this host and an old definition"
  # As an older release wrote it, plus a limit the administrator added.
  sed -i -e '/^OOMScoreAdjust=/d' -e 's/^\[Service\]$/[Service]\nTasksMax=777/' "$unit"
  systemctl daemon-reload
  "$orch" upgrade --plan --offline --install-dir "$dir" | tee "$out/plan.txt"
  grep -q TasksMax "$out/plan.txt" || fail "the plan does not mention the host's line"
fi

step "upgrade to v$B"
upgrade_to "$B"
expect_job "done"
check_version "$B"
if [ "$os" = linux ] && [ "$manual" = 0 ]; then
  grep -q TasksMax "$unit" && fail "the host's line stayed in the unit"
  grep -q '^TasksMax=777' "$unit.d/50-local.conf" || fail "the host's line was lost"
  [ "$(systemctl show -p TasksMax --value ncc-orchestrator.service)" = 777 ] || fail "the host's limit does not apply"
  [ -e "$helper_unit.path" ] || fail "the helper has no path unit"
fi
helper_running && fail "the helper still runs"
if [ "$os" = windows ] && [ "$manual" = 0 ]; then
  task="$(schtasks //Query //TN ncc-orchestrator-upgrade //XML)" || fail "the helper has no task"
  [[ "$task" == *"<BootTrigger"* ]] || fail "the helper's task does not start at boot: $task"
  [[ "$task" != *"<Repetition>"* ]] || fail "the helper's task is still armed: $task"
fi
ok "upgraded; supervisor, API and UI run v$B"

step "roll back by hand"
"$orch" upgrade --rollback --install-dir "$dir"
expect_job rolled_back
check_version "$A"
if [ "$os" = linux ] && [ "$manual" = 0 ]; then
  grep -q '^TasksMax=777' "$unit" || fail "the unit from before is not back"
  [ ! -e "$unit.d/50-local.conf" ] || fail "the drop-in of the upgrade is still there"
fi
ok "v$A is back"

if [ "$manual" = 0 ]; then
  step "the supervisor dies"
  sup() { tr -cd '0-9' < "$dir/run/v2-supervisor.pid" 2>/dev/null || true; }
  old="$(sup)"
  [ -n "$old" ] || fail "no supervisor"
  if [ "$os" = windows ]; then taskkill //F //PID "$old"; else kill -9 "$old"; fi
  for ((i = 0; i < 180; i++)); do
    new="$(sup)"
    [ -n "$new" ] && [ "$new" != "$old" ] && break
    sleep 1
  done
  [ "$new" != "$old" ] || fail "the supervisor was not started again"
  check_version "$A"
  status="$("$orch" v2-status --install-dir "$dir" --json)"
  echo "ok: started again after ${i}s (process $old, now $new)"
  [[ "$status" != *port-in-use* ]] || fail "the servers of the dead supervisor hold the ports: $status"
  if [ "$os" = windows ]; then
    step "a stop by the administrator is respected"
    schtasks //End //TN ncc-orchestrator
    sleep 150
    [ -z "$(version)" ] || fail "the stack was started again after schtasks /End"
    schtasks //Run //TN ncc-orchestrator
    check_version "$A"
    "$orch" v2-stop --install-dir "$dir"
    sleep 150
    [ -z "$(version)" ] || fail "the stack was started again after v2-stop"
    schtasks //Run //TN ncc-orchestrator
    check_version "$A"
    ok "stays stopped after schtasks /End and v2-stop"
  fi
fi

step "a release that does not start (v$BAD)"
if upgrade_to "$BAD"; then fail "the upgrade claims to have worked"; fi
expect_job rolled_back
check_version "$A"
ok "rolled back on its own"

step "a package that was changed after it was signed for"
rm -rf "$dir.tampered"; mkdir -p "$dir.tampered" # not in $out, which may be another user's
cp "$(bundle "$B")" "$out/$B/checksums.txt" "$dir.tampered/"
printf 'x' >> "$(echo "$dir.tampered/"ncc-v2-stack-*.*)"
if "$orch" upgrade --install-dir "$dir" --bundle "$(echo "$dir.tampered/"ncc-v2-stack-*.*)" --skip-backup; then fail "a changed package was accepted"; fi
check_version "$A"
rm -rf "$dir.tampered"
ok "refused; nothing was changed"

interrupt() { # interrupt PHASE_PATTERN: upgrades to $B and kills the helper in such a phase
  local i p killed=""
  "$orch" upgrade --install-dir "$dir" --bundle "$(bundle "$B")" --skip-backup --no-wait
  for ((i = 0; i < 1200; i++)); do
    p="$(job_phase || true)"
    if [[ "$p" =~ ^($1)$ ]]; then
      kill_helper && killed="$p"
      break
    fi
    case "$p" in done | failed | rolled_back) fail "the upgrade ended as $p before it could be interrupted" ;; esac
    sleep 0.05
  done
  [ -n "$killed" ] || fail "no phase to interrupt was seen"
  echo "killed in phase $killed"
  if [ "$manual" = 1 ]; then
    # Nothing watches it; the next start finishes the job.
    sleep 2
    "$orch" v2-start --install-dir "$dir" --detach --api-listen "127.0.0.1:$api" --ui-listen "127.0.0.1:$ui"
  fi
  for ((i = 0; i < 300; i++)); do
    p="$(job_phase || true)"
    case "$p" in done | failed | rolled_back) return 0 ;; esac
    sleep 1
  done
  fail "the job was not continued"
}

step "the helper is killed while it installs"
interrupt 'stopping|swapping|migrating|starting|checking'
if [ "$(job_phase)" = "done" ]; then
  # It had everything in place already.
  check_version "$B"
  "$orch" upgrade --rollback --install-dir "$dir"
fi
expect_job rolled_back
check_version "$A"
ok "no mixed versions: v$A runs"

step "the helper is killed while it checks the new version"
interrupt checking
expect_job "done"
check_version "$B"
ok "finished after it was killed"

step "remove"
cleanup
if [ "$os" = linux ] && [ "$manual" = 0 ]; then
  for f in "$helper_unit.service" "$helper_unit.path"; do [ ! -e "$f" ] || fail "$f was left behind"; done
fi
if [ "$os" = windows ] && [ "$manual" = 0 ]; then
  for t in ncc-orchestrator ncc-orchestrator-upgrade ncc-orchestrator-watchdog; do
    if schtasks //Query //TN "$t" >/dev/null 2>&1; then fail "the task $t was left behind"; fi
  done
fi
[ -z "$(version)" ] || fail "the API still answers"
ok "removed"
trap - EXIT
printf '\nUPGRADE_E2E_OK (%s%s)\n' "$os" "$([ "$manual" = 1 ] && echo ', without a service manager' || true)"
