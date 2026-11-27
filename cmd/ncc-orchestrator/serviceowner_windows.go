//go:build windows

package main

import (
	"errors"
	"os"
	"time"
)

// The service user is a Linux (systemd) feature; Windows keeps SYSTEM and
// folder ACLs (WIN-02).

var errNoServiceUser = errors.New("a service user is only supported with systemd")

func applyServiceOwnership(string, int, int) error     { return errNoServiceUser }
func prepareOpsDirs(string, int) error                 { return errNoServiceUser }
func ownershipCurrent(string, int, int) bool           { return false }
func captureOwnership(string) (ownershipRecord, error) { return ownershipRecord{}, errNoServiceUser }
func restoreOwnership(string, ownershipRecord) error   { return errNoServiceUser }
func writableBy(string, int, int) bool                 { return true }

func openNoFollow(p string) (*os.File, error) { return os.Open(p) }
func fileOwnerUID(os.FileInfo) int            { return -1 }
func giveToService(string, string)            {}

func openLogNoFollow(p string) (*os.File, error) {
	return os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
}
func serviceUserRunner(string) func(time.Duration, string, ...string) (string, error) {
	return runUpgradeCommand
}
func unprivilegedInstall(string) bool { return false }
