#!/usr/bin/env bash
# Sets up the NCC MCP server for an AI assistant in one command:
#   1. creates a least-privilege account (viewer by default) with a random password
#   2. signs in as it and mints a personal access token (the account's role only)
#   3. builds ncc-mcp-server from this repository (optional)
#   4. writes a ready-to-merge host config with the token, readable only by you
#
# The token and password are never printed. Run from a checkout of the repository.
#
#   NCC_ADMIN_TOKEN=<admin token or .ncc-api-token contents> \
#     scripts/ncc-mcp-setup.sh --api https://ncc.example.com:8081
#
# Options:
#   --api URL        API server base URL (default http://127.0.0.1:8081)
#   --role ROLE      viewer (default) or operator
#   --name NAME      account name to create (default svc-assistant)
#   --days N         token lifetime in days (default 90)
#   --out DIR        where to write the binary and config (default ./ncc-mcp)
#   --cacert FILE    CA bundle that signed the API certificate
#   --no-build       skip building ncc-mcp-server
#   --allow-http     allow plain HTTP to a non-loopback API (not recommended)
set -euo pipefail

API="http://127.0.0.1:8081"; ROLE="viewer"; NAME="svc-assistant"; DAYS=90
OUT="./ncc-mcp"; CACERT=""; BUILD=1; ALLOW_HTTP=0

die() { echo "error: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --api) API="${2:?}"; shift 2 ;;
    --role) ROLE="${2:?}"; shift 2 ;;
    --name) NAME="${2:?}"; shift 2 ;;
    --days) DAYS="${2:?}"; shift 2 ;;
    --out) OUT="${2:?}"; shift 2 ;;
    --cacert) CACERT="${2:?}"; shift 2 ;;
    --no-build) BUILD=0; shift ;;
    --allow-http) ALLOW_HTTP=1; shift ;;
    -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[ -n "${NCC_ADMIN_TOKEN:-}" ] || die "set NCC_ADMIN_TOKEN to an admin token (the contents of .ncc-api-token) in the environment"
case "$ROLE" in viewer|operator) ;; *) die "--role must be viewer or operator (admin tokens are not created for assistants)" ;; esac
[[ "$NAME" =~ ^[A-Za-z0-9._-]{3,64}$ ]] || die "--name may use letters, digits . _ - (3 to 64 characters)"
[[ "$DAYS" =~ ^[0-9]+$ ]] && [ "$DAYS" -ge 1 ] && [ "$DAYS" -le 365 ] || die "--days must be 1 to 365"
command -v curl >/dev/null || die "curl is required"
API="${API%/}"

host="${API#*://}"; host="${host%%/*}"; host="${host%%:*}"
case "$API" in
  https://*) ;;
  http://*)
    case "$host" in
      127.0.0.1|localhost|"[::1]") ;;
      *) [ "$ALLOW_HTTP" = 1 ] || die "plain HTTP to $host would send the token in clear text and ncc-mcp-server refuses it; use https:// (or tunnel to loopback)" ;;
    esac ;;
  *) die "--api must start with http:// or https://" ;;
esac

curl_opts=(-sS -m 30)
[ -n "$CACERT" ] && curl_opts+=(--cacert "$CACERT")

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
umask 077

# Random password: used once to sign in, then forgotten (reset it in the dashboard if you ever need it).
if command -v openssl >/dev/null; then pw="$(openssl rand -base64 24 | tr -d '\r\n')"
else pw="$(head -c 24 /dev/urandom | base64 | tr -d '\r\n')"; fi
pw="${pw}aA1!"

json_get() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p" | head -n1; }

echo "Checking the API at $API ..."
code="$(curl "${curl_opts[@]}" -o /dev/null -w '%{http_code}' "$API/api/v1/health" || true)"
[ "$code" = "200" ] || die "API health check returned '$code' (is the stack running, and is the URL and certificate trusted? try --cacert)"

echo "Creating $ROLE account '$NAME' ..."
body="$(printf '{"username":"%s","password":"%s","role":"%s","must_change_password":false}' "$NAME" "$pw" "$ROLE")"
code="$(curl "${curl_opts[@]}" -o "$tmp/user.out" -w '%{http_code}' -X POST "$API/api/v1/settings/users" \
  -H "Authorization: Bearer $NCC_ADMIN_TOKEN" -H 'Content-Type: application/json' -d "$body" || true)"
case "$code" in
  200|201) ;;
  401|403) die "the admin token was refused ($code)" ;;
  409) die "account '$NAME' already exists; pick another --name or remove it in Settings, Access" ;;
  *) die "could not create the account (HTTP $code): $(head -c 300 "$tmp/user.out")" ;;
esac

echo "Signing in and creating the token ..."
code="$(curl "${curl_opts[@]}" -o "$tmp/login.out" -w '%{http_code}' -c "$tmp/jar" -X POST "$API/api/v1/auth/login" \
  -H 'Content-Type: application/json' -d "$(printf '{"username":"%s","password":"%s"}' "$NAME" "$pw")" || true)"
[ "$code" = "200" ] || die "sign-in as '$NAME' failed (HTTP $code): $(head -c 300 "$tmp/login.out")"
csrf="$(awk 'tolower($6) ~ /csrf/ {print $7}' "$tmp/jar" | head -n1)"
[ -n "$csrf" ] || die "no CSRF cookie was issued"
code="$(curl "${curl_opts[@]}" -o "$tmp/pat.out" -w '%{http_code}' -b "$tmp/jar" -X POST "$API/api/v1/auth/tokens" \
  -H 'Content-Type: application/json' -H "X-CSRF-Token: $csrf" \
  -d "$(printf '{"name":"claude-mcp","expires_in_days":%s}' "$DAYS")" || true)"
[ "$code" = "200" ] || [ "$code" = "201" ] || die "token creation failed (HTTP $code): $(head -c 300 "$tmp/pat.out")"
token="$(json_get token < "$tmp/pat.out")"
got_role="$(json_get role < "$tmp/pat.out")"
[[ "$token" == ncc_pat_* ]] || die "the API did not return a token"
[ "$got_role" = "$ROLE" ] || die "the token has role '$got_role', expected '$ROLE'; not writing a config"

mkdir -p "$OUT"
exe="ncc-mcp-server"; case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) exe="ncc-mcp-server.exe" ;; esac
if [ "$BUILD" = 1 ]; then
  root="$(cd "$(dirname "$0")/.." && pwd)"
  [ -f "$root/cmd/ncc-mcp-server/main.go" ] || die "run this from the repository checkout (or pass --no-build and supply the binary)"
  command -v go >/dev/null || die "Go 1.27.4 or newer is required to build ncc-mcp-server (or pass --no-build)"
  echo "Building $exe ..."
  (cd "$root" && go build -trimpath -o "$(cd "$OUT" && pwd)/$exe" ./cmd/ncc-mcp-server)
fi
bindir="$(cd "$OUT" && { pwd -W 2>/dev/null || pwd; })"   # pwd -W: a Windows path under Git Bash
bin="$bindir/$exe"

cfg="$OUT/mcp-host-config.json"
{
  printf '{\n  "mcpServers": {\n    "ncc-orchestrator": {\n      "command": "%s",\n      "env": {\n' "$bin"
  printf '        "NCC_MCP_API_URL": "%s",\n        "NCC_MCP_TOKEN": "%s",\n        "NCC_MCP_PROFILE": "%s"\n' "$API" "$token" "$([ "$ROLE" = viewer ] && echo readonly || echo operator)"
  printf '      }\n    }\n  }\n}\n'
} > "$cfg"
chmod 600 "$cfg" 2>/dev/null || true

cat <<EOF

Done.
  Account:  $NAME ($ROLE), token valid $DAYS days
  Binary:   $bin$([ "$BUILD" = 1 ] || echo " (not built; supply it)")
  Config:   $cfg   (contains the token; readable only by you)

Next: merge that file's "ncc-orchestrator" block into your assistant's MCP settings
(Cursor: ~/.cursor/mcp.json, Claude Desktop: Settings > Developer > Edit Config),
restart the assistant, then ask: "Summarize the last NCC run."
Mutations stay off. To allow runs you need an operator account and
NCC_MCP_ALLOW_MUTATIONS=true; see docs/MCP_SERVER.md.
EOF
