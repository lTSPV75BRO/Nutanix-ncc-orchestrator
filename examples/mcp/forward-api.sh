#!/bin/sh
# Forward a remote ncc-api-server (port 8081) to loopback so ncc-mcp-server
# may use http://127.0.0.1:18091. Required when the API is only on HTTP
# (MCP refuses cleartext to a non-loopback address).
#
# Usage:
#   ./forward-api.sh user@api-host
# Then set NCC_MCP_API_URL=http://127.0.0.1:18091 in the MCP host config.

set -e
REMOTE="${1:?usage: forward-api.sh user@api-host}"
LOCAL_PORT="${NCC_MCP_FORWARD_PORT:-18091}"
REMOTE_PORT="${NCC_API_PORT:-8081}"
echo "127.0.0.1:${LOCAL_PORT} -> ${REMOTE}:127.0.0.1:${REMOTE_PORT}" >&2
exec ssh -N -L "${LOCAL_PORT}:127.0.0.1:${REMOTE_PORT}" "${REMOTE}"
