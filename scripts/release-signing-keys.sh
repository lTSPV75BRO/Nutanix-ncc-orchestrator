#!/usr/bin/env bash
# release-signing-keys.sh DIR ENVFILE - write the release signing keys to files
# in DIR (mode 0600), check that they can sign a release, and append the settings
# for scripts/binaryGO.txt to ENVFILE (file names and public values only).
#
# Wrong keys stop the release here, in seconds, instead of at the end of the
# build. Input (environment, all required):
#   NCC_RELEASE_SIGN_KEY_BASE64       base64 of the Ed25519 private key file
#                                     (release-keygen file or PKCS#8 PEM), or the PEM text
#   NCC_RELEASE_PUBKEY                its public key (one line of base64, or PEM)
#   NCC_LINUX_GPG_PRIVATE_KEY_BASE64  base64 of the exported GPG secret key, or the armored text
#   NCC_LINUX_GPG_KEY_ID              its fingerprint
#   NCC_LINUX_GPG_PASSPHRASE          its passphrase
set -euo pipefail
umask 077

dir="${1:?usage: release-signing-keys.sh DIR ENVFILE}"
env_file="${2:?usage: release-signing-keys.sh DIR ENVFILE}"
cd "$(dirname "$0")/.."

fail() { echo "::error title=Release signing keys::$*" >&2; exit 1; }

for name in NCC_RELEASE_SIGN_KEY_BASE64 NCC_RELEASE_PUBKEY NCC_LINUX_GPG_PRIVATE_KEY_BASE64 NCC_LINUX_GPG_KEY_ID NCC_LINUX_GPG_PASSPHRASE; do
  [ -n "${!name:-}" ] || fail "$name is required for a release (environment 'release' secret/variable)"
done

# A fingerprint may be pasted with spaces or a line break.
key_id="$(printf '%s' "$NCC_LINUX_GPG_KEY_ID" | tr -d '[:space:]')"

# decode VALUE: base64 text, or a PEM/armored block as it is
decode() {
  case "$1" in
    *"-----BEGIN "*) printf '%s\n' "$1" ;;
    *) printf '%s' "$1" | tr -d '[:space:]' | base64 --decode 2>/dev/null ;;
  esac
}

work="$(mktemp -d "$dir/ncc-release-check.XXXXXX")"
trap 'rm -rf "$work"' EXIT
echo "release signing key check" > "$work/test"

# --- Ed25519 key for checksums.txt ---
release_key="$dir/ncc-release-signing.key"
decode "$NCC_RELEASE_SIGN_KEY_BASE64" > "$release_key" \
  || fail "NCC_RELEASE_SIGN_KEY_BASE64 is not base64"
chmod 600 "$release_key"
go build -o "$work/ncc" ./cmd/ncc-orchestrator
pubkey="$("$work/ncc" release-pubkey --key "$release_key" --public "$NCC_RELEASE_PUBKEY" 2> "$work/err")" \
  || fail "NCC_RELEASE_SIGN_KEY_BASE64 / NCC_RELEASE_PUBKEY: $(sed -n 's/^Error: //p' "$work/err" | head -n 1)"
"$work/ncc" release-sign --key "$release_key" --in "$work/test" --out "$work/test.sig" > /dev/null
echo "ok: the Ed25519 signing key belongs to the public key $pubkey"

# --- GPG key for the Linux files ---
gpg_home="$dir/ncc-release-gpg"
rm -rf "$gpg_home"
install -d -m 700 "$gpg_home"
decode "$NCC_LINUX_GPG_PRIVATE_KEY_BASE64" > "$work/gpg.key" \
  || fail "NCC_LINUX_GPG_PRIVATE_KEY_BASE64 is not base64"
gpg --batch --quiet --homedir "$gpg_home" --import "$work/gpg.key" 2> "$work/err" \
  || fail "NCC_LINUX_GPG_PRIVATE_KEY_BASE64 cannot be imported: $(tail -n 1 "$work/err")"
gpg --batch --homedir "$gpg_home" --list-secret-keys "$key_id" > /dev/null 2>&1 \
  || fail "NCC_LINUX_GPG_PRIVATE_KEY_BASE64 does not contain the secret key NCC_LINUX_GPG_KEY_ID"
# The passphrase goes to a 0600 file (gpg --passphrase-file), never to a command
# line or GITHUB_ENV.
pass_file="$dir/ncc-release-gpg.pass"
printf '%s\n' "$NCC_LINUX_GPG_PASSPHRASE" > "$pass_file"
chmod 600 "$pass_file"
gpg --batch --yes --quiet --homedir "$gpg_home" --local-user "$key_id" \
  --pinentry-mode loopback --passphrase-file "$pass_file" \
  --detach-sign --armor --output "$work/test.asc" "$work/test" 2> "$work/err" \
  || fail "the GPG key cannot sign (passphrase, expiry or key ID): $(tail -n 1 "$work/err")"
gpg --batch --quiet --homedir "$gpg_home" --verify "$work/test.asc" "$work/test" 2> /dev/null \
  || fail "the GPG test signature does not verify"
echo "ok: the GPG key signs"

{
  echo "NCC_RELEASE_SIGN_KEY=$release_key"
  echo "NCC_RELEASE_PUBKEY=$pubkey"
  echo "GNUPGHOME=$gpg_home"
  echo "NCC_LINUX_GPG_KEY_ID=$key_id"
  echo "NCC_LINUX_GPG_PASSPHRASE_FILE=$pass_file"
} >> "$env_file"
