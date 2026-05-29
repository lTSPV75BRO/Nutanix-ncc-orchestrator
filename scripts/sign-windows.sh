#!/usr/bin/env bash
# Self-signed Authenticode signing for the Windows NCC Orchestrator
# binaries, runnable from a macOS/Linux build host.
#
# WHAT THIS DOES (and does NOT do):
#   - It produces a REAL Authenticode signature using a self-signed
#     code-signing certificate. `signtool verify`, sigcheck, and the
#     Properties -> Digital Signatures tab will show the publisher name
#     baked into the certificate subject.
#   - It does NOT make Windows SmartScreen trust the binary for the
#     general public. SmartScreen only trusts certificates that chain to
#     a CA in the Microsoft Trusted Root program (OV/EV code-signing
#     certs). A self-signed cert is trusted ONLY on machines that have
#     imported it into their "Trusted Publishers" / "Trusted Root"
#     stores -- which is exactly what locked-down enterprise fleets do
#     via GPO/Intune. See docs/SECURITY_AND_TRUST.md.
#
# For a public, no-warning experience you need a CA-issued certificate;
# point binaryGO.txt's osslsigncode hook at that PFX instead
# (NCC_WINDOWS_PFX_PATH=...). This script is the free, fleet-friendly
# alternative.
#
# Requirements: openssl + osslsigncode (brew install osslsigncode).
#
# Usage:
#   ./scripts/sign-windows.sh [options]
#
# Options:
#   --dist <dir>        Directory holding the *-windows-*.exe files (default: dist)
#   --pfx <path>        Use an existing PKCS#12 instead of generating one
#   --pfx-pass <pass>   Password for --pfx (or the generated one)
#   --subject <subj>    Cert subject (default: the project publisher CN)
#   --out-cert <path>   Where to write the public .cer for fleet import
#                       (default: <dist>/ncc-codesign-public.cer)
#   --timestamp <url>   RFC3161 timestamp URL (default: DigiCert; "" to skip)
#   --keep-key          Keep the generated key/cert/pfx (default: deleted)
#   -h, --help          Show this help
set -euo pipefail

DIST="dist"
PFX=""
PFX_PASS=""
SUBJECT="/CN=NCC Orchestrator (open-source project)/O=NCC Orchestrator project"
OUT_CERT=""
TS_URL="http://timestamp.digicert.com"
KEEP_KEY=0

while [ $# -gt 0 ]; do
  case "$1" in
    --dist) DIST="$2"; shift 2 ;;
    --pfx) PFX="$2"; shift 2 ;;
    --pfx-pass) PFX_PASS="$2"; shift 2 ;;
    --subject) SUBJECT="$2"; shift 2 ;;
    --out-cert) OUT_CERT="$2"; shift 2 ;;
    --timestamp) TS_URL="$2"; shift 2 ;;
    --keep-key) KEEP_KEY=1; shift ;;
    -h|--help) sed -n '1,40p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

command -v osslsigncode >/dev/null 2>&1 || { echo "error: osslsigncode not found (brew install osslsigncode)" >&2; exit 1; }
command -v openssl       >/dev/null 2>&1 || { echo "error: openssl not found" >&2; exit 1; }
[ -d "$DIST" ] || { echo "error: dist dir not found: $DIST" >&2; exit 1; }
[ -n "$OUT_CERT" ] || OUT_CERT="$DIST/ncc-codesign-public.cer"

WORK="$(mktemp -d)"
GENERATED_PFX=0
cleanup() { [ "$KEEP_KEY" -eq 1 ] || rm -rf "$WORK"; }
trap cleanup EXIT

if [ -z "$PFX" ]; then
  echo "==> generating self-signed code-signing certificate"
  echo "    subject: $SUBJECT"
  [ -n "$PFX_PASS" ] || PFX_PASS="$(openssl rand -hex 16)"
  # Code-signing EKU (1.3.6.1.5.5.7.3.3) is required for Authenticode.
  openssl req -x509 -newkey rsa:3072 -sha256 -days 1095 -nodes \
    -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
    -subj "$SUBJECT" \
    -addext "keyUsage=digitalSignature" \
    -addext "extendedKeyUsage=codeSigning" >/dev/null 2>&1
  openssl pkcs12 -export -out "$WORK/cert.pfx" \
    -inkey "$WORK/key.pem" -in "$WORK/cert.pem" \
    -passout "pass:$PFX_PASS" >/dev/null 2>&1
  PFX="$WORK/cert.pfx"
  GENERATED_PFX=1
  # Export the public certificate (DER) so fleet admins can import it
  # into LocalMachine\TrustedPublisher (and Root) to trust the binaries.
  openssl x509 -in "$WORK/cert.pem" -outform DER -out "$OUT_CERT"
  echo "==> wrote public certificate for fleet import: $OUT_CERT"
fi

ts_args=()
[ -n "$TS_URL" ] && ts_args=(-t "$TS_URL")

shopt -s nullglob
exes=("$DIST"/ncc-orchestrator-windows-*.exe "$DIST"/ncc-api-server-windows-*.exe "$DIST"/ncc-ui-server-windows-*.exe)
[ "${#exes[@]}" -gt 0 ] || { echo "error: no *-windows-*.exe files under $DIST" >&2; exit 1; }

for f in "${exes[@]}"; do
  echo "==> signing $f"
  osslsigncode sign -pkcs12 "$PFX" -pass "$PFX_PASS" \
    -h sha256 "${ts_args[@]}" \
    -n "NCC Orchestrator (open-source project)" \
    -i "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator" \
    -in "$f" -out "$f.signed"
  mv "$f.signed" "$f"
  osslsigncode verify "$f" 2>/dev/null | grep -Ei 'signature|subject|timestamp' || true
done

echo
echo "Done. Signed ${#exes[@]} binary(ies)."
if [ "$GENERATED_PFX" -eq 1 ]; then
  echo "Self-signed cert was used. For machines to trust these binaries, import:"
  echo "    $OUT_CERT"
  echo "into Trusted Publishers (and Trusted Root) -- see docs/SECURITY_AND_TRUST.md."
  if [ "$KEEP_KEY" -eq 1 ]; then
    echo "Private key/PFX kept under: $WORK (password: $PFX_PASS)"
  fi
fi
