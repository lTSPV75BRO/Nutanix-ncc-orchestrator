package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/scrypt"
)

// Encrypted backups. A backup archive is a secrets bundle — it carries the API
// token, bcrypt password hashes, the SAML SP private key, the LDAP bind
// password, and (commonly) a config.yaml with a plaintext Prism password. The
// on-disk file is 0600, but once it leaves the host (a download, a copy to
// another machine) those secrets are plaintext. `v2-backup --encrypt` wraps the
// whole .tar.gz with AES-256-GCM so the archive is unreadable without the key,
// and `v2-restore` transparently detects and decrypts it.
//
// Two key sources are supported, independent of the user-store master key
// (NCC_MASTER_KEY) so the two concerns stay separate:
//   - a passphrase (scrypt-derived; --passphrase / NCC_BACKUP_PASSPHRASE), or
//   - a raw 32-byte key (--key-file / NCC_BACKUP_KEY_FILE, or NCC_BACKUP_KEY).
//
// Envelope layout: magic || mode || [salt (passphrase mode only)] || nonce ||
// ciphertext+tag. The header (magic+mode+salt) is the GCM additional-
// authenticated-data, so tampering with the mode or salt is detected.
const backupEncMagic = "NCCBKP1\n"

const (
	backupModeKey  byte = 'K' // raw 32-byte key
	backupModePass byte = 'P' // scrypt-derived from a passphrase (salt in header)

	backupScryptN      = 1 << 15
	backupScryptR      = 8
	backupScryptP      = 1
	backupScryptKeyLen = 32
	backupSaltLen      = 16
)

// scryptBackupKey derives a 32-byte key from a passphrase + salt.
func scryptBackupKey(passphrase string, salt []byte) ([]byte, error) {
	return scrypt.Key([]byte(passphrase), salt, backupScryptN, backupScryptR, backupScryptP, backupScryptKeyLen)
}

// decodeBackupKey accepts a 32-byte key as base64 (std/raw/url), hex, or exactly
// 32 raw bytes (after trimming surrounding whitespace).
func decodeBackupKey(raw []byte) ([]byte, error) {
	s := strings.TrimSpace(string(raw))
	for _, dec := range []func(string) ([]byte, error){
		base64.StdEncoding.DecodeString,
		base64.RawStdEncoding.DecodeString,
		base64.URLEncoding.DecodeString,
		hex.DecodeString,
	} {
		if k, err := dec(s); err == nil && len(k) == backupScryptKeyLen {
			return k, nil
		}
	}
	if len(raw) == backupScryptKeyLen {
		return append([]byte(nil), raw...), nil
	}
	return nil, errors.New("backup key must decode to 32 bytes: provide a base64 or hex 32-byte key (e.g. `openssl rand -base64 32`)")
}

func newBackupGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != backupScryptKeyLen {
		return nil, fmt.Errorf("backup key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// resolveBackupEncKey resolves the key material for ENCRYPTING a backup.
// Precedence: passphrase (flag, then NCC_BACKUP_PASSPHRASE) -> raw key
// (--key-file, then NCC_BACKUP_KEY_FILE, then NCC_BACKUP_KEY). For passphrase
// mode a fresh random salt is generated and returned (stored in the envelope).
func resolveBackupEncKey(keyFile, passphrase string) (key []byte, mode byte, salt []byte, err error) {
	if pass := firstNonEmpty(passphrase, os.Getenv("NCC_BACKUP_PASSPHRASE")); strings.TrimSpace(pass) != "" {
		salt = make([]byte, backupSaltLen)
		if _, err = crand.Read(salt); err != nil {
			return nil, 0, nil, err
		}
		key, err = scryptBackupKey(strings.TrimSpace(pass), salt)
		if err != nil {
			return nil, 0, nil, err
		}
		return key, backupModePass, salt, nil
	}
	raw, rerr := readRawBackupKey(keyFile)
	if rerr != nil {
		return nil, 0, nil, rerr
	}
	if raw == nil {
		return nil, 0, nil, errors.New("encryption requested but no key provided: set --passphrase / NCC_BACKUP_PASSPHRASE, or --key-file / NCC_BACKUP_KEY_FILE / NCC_BACKUP_KEY")
	}
	key, err = decodeBackupKey(raw)
	if err != nil {
		return nil, 0, nil, err
	}
	return key, backupModeKey, nil, nil
}

// resolveBackupDecKey resolves the key for DECRYPTING a backup, given the
// envelope mode and (for passphrase mode) the stored salt.
func resolveBackupDecKey(mode byte, salt []byte, keyFile, passphrase string) ([]byte, error) {
	switch mode {
	case backupModePass:
		pass := strings.TrimSpace(firstNonEmpty(passphrase, os.Getenv("NCC_BACKUP_PASSPHRASE")))
		if pass == "" {
			return nil, errors.New("archive is passphrase-encrypted: provide --passphrase or NCC_BACKUP_PASSPHRASE")
		}
		return scryptBackupKey(pass, salt)
	case backupModeKey:
		raw, err := readRawBackupKey(keyFile)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, errors.New("archive is key-encrypted: provide --key-file / NCC_BACKUP_KEY_FILE / NCC_BACKUP_KEY")
		}
		return decodeBackupKey(raw)
	default:
		return nil, fmt.Errorf("unknown backup encryption mode %q", string(mode))
	}
}

// readRawBackupKey returns the raw key bytes from the key file (flag or
// NCC_BACKUP_KEY_FILE) or the inline NCC_BACKUP_KEY env, or (nil, nil) when none
// is configured.
func readRawBackupKey(keyFile string) ([]byte, error) {
	if kf := strings.TrimSpace(firstNonEmpty(keyFile, os.Getenv("NCC_BACKUP_KEY_FILE"))); kf != "" {
		b, err := os.ReadFile(kf)
		if err != nil {
			return nil, fmt.Errorf("read backup key file %q: %w", kf, err)
		}
		return b, nil
	}
	if v := strings.TrimSpace(os.Getenv("NCC_BACKUP_KEY")); v != "" {
		return []byte(v), nil
	}
	return nil, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// backupArchiveIsEncrypted reports whether the file at path carries the
// encrypted-backup envelope magic.
func backupArchiveIsEncrypted(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	hdr := make([]byte, len(backupEncMagic))
	n, err := f.Read(hdr)
	if err != nil && n == 0 {
		return false, err
	}
	return bytes.Equal(hdr[:n], []byte(backupEncMagic)), nil
}

// encryptBackupFile seals the file at path in place with AES-256-GCM. The
// plaintext archive is small (a single install's state), so reading it whole is
// fine; the inner tar is still streamed with caps on restore.
func encryptBackupFile(path string, key []byte, mode byte, salt []byte) error {
	plain, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	gcm, err := newBackupGCM(key)
	if err != nil {
		return err
	}
	header := append([]byte(backupEncMagic), mode)
	if mode == backupModePass {
		header = append(header, salt...)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := crand.Read(nonce); err != nil {
		return err
	}
	ct := gcm.Seal(nil, nonce, plain, header)
	out := make([]byte, 0, len(header)+len(nonce)+len(ct))
	out = append(out, header...)
	out = append(out, nonce...)
	out = append(out, ct...)
	return os.WriteFile(path, out, 0o600)
}

// decryptBackupArchive decrypts an encrypted backup at in into a fresh 0600
// temp .tar.gz and returns its path (the caller removes it). The encrypted blob
// is roughly the size of the gzip it wraps, and is size-capped before reading.
func decryptBackupArchive(in, keyFile, passphrase string) (string, error) {
	if st, err := os.Stat(in); err == nil && st.Size() > maxBackupTotalBytes+(16<<20) {
		return "", fmt.Errorf("encrypted backup is implausibly large (%d bytes); refusing to load", st.Size())
	}
	data, err := os.ReadFile(in)
	if err != nil {
		return "", err
	}
	if !bytes.HasPrefix(data, []byte(backupEncMagic)) {
		return "", errors.New("not an encrypted backup archive")
	}
	headerEnd := len(backupEncMagic) + 1 // magic + mode
	if len(data) < headerEnd {
		return "", errors.New("encrypted backup truncated (header)")
	}
	mode := data[len(backupEncMagic)]
	var salt []byte
	if mode == backupModePass {
		if len(data) < headerEnd+backupSaltLen {
			return "", errors.New("encrypted backup truncated (salt)")
		}
		salt = data[headerEnd : headerEnd+backupSaltLen]
		headerEnd += backupSaltLen
	}
	key, err := resolveBackupDecKey(mode, salt, keyFile, passphrase)
	if err != nil {
		return "", err
	}
	gcm, err := newBackupGCM(key)
	if err != nil {
		return "", err
	}
	ns := gcm.NonceSize()
	if len(data) < headerEnd+ns {
		return "", errors.New("encrypted backup truncated (nonce)")
	}
	header := data[:headerEnd]
	nonce := data[headerEnd : headerEnd+ns]
	ct := data[headerEnd+ns:]
	plain, err := gcm.Open(nil, nonce, ct, header)
	if err != nil {
		return "", fmt.Errorf("decrypt failed (wrong key/passphrase or corrupt archive): %w", err)
	}
	tmp, err := os.CreateTemp("", "ncc-restore-dec-*.tar.gz")
	if err != nil {
		return "", err
	}
	if _, err := tmp.Write(plain); err != nil {
		tmp.Close()
		_ = os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}
