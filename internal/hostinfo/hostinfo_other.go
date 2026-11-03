//go:build !windows && !linux && !darwin

package hostinfo

func nativeArch() (string, bool, bool) { return "", false, false }
func systemMemory() Memory             { return Memory{} }
func quarantined(string) bool          { return false }
func clearQuarantine(string) error     { return nil }
