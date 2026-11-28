package main

import (
	"os"

	"goncc/internal/winacl"
)

// Secret files are 0600 and their folders 0700. On Windows those modes protect
// nothing, so these helpers also close them to ordinary users there. A
// permission error never fails the write: v2-start and the doctor report it.

func writeSecretFile(path string, data []byte) error {
	return winacl.WriteFile(path, data, 0o600)
}

// renameSecretFile replaces dst and keeps its restricted permissions, which a
// plain rename would swap for the temp file's.
func renameSecretFile(tmp, dst string) error { return winacl.Rename(tmp, dst) }

// restrictNewSecret closes a file that was just created and is still empty.
func restrictNewSecret(path string) { _, _ = winacl.Restrict(path, winacl.Private) }

func mkdirPrivate(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	_, _ = winacl.Restrict(dir, winacl.Private)
	return nil
}
