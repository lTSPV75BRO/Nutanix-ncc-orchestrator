//go:build !unix

package selfsigned

func withDirLock(_ string, fn func() error) error {
	return fn()
}
