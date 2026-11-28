package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"sync"
)

// Host (non-Kubernetes) schedules live in a JSON file. Its version is a hash
// of the file's bytes: the GET hands it out as resource_version, a save sends
// it back, and a save made from an older version is refused with 409 (the same
// conflict a Kubernetes CronJob gives through its resourceVersion).

// hostScheduleNone is the version of "nothing saved yet".
const hostScheduleNone = "none"

// hostScheduleMu makes the version check and the write one step.
var hostScheduleMu sync.Mutex

// errHostScheduleConflict: the saved schedule changed after the form loaded it.
var errHostScheduleConflict = errors.New("the schedule was changed by someone else after you opened it; reload to see the current schedule, then save again")

func hostScheduleVersion(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// currentHostScheduleVersion is the version of the schedule file on disk now.
func (s *apiServer) currentHostScheduleVersion() (string, error) {
	b, err := os.ReadFile(s.absPath(s.scheduleStatePath))
	if errors.Is(err, os.ErrNotExist) {
		return hostScheduleNone, nil
	}
	if err != nil {
		return "", err
	}
	return hostScheduleVersion(b), nil
}

// saveHostScheduleIfCurrent writes st when expected is empty (an older client
// that sends no version) or matches the file on disk, and returns the new
// version.
func (s *apiServer) saveHostScheduleIfCurrent(st scheduleState, expected string) (string, error) {
	hostScheduleMu.Lock()
	defer hostScheduleMu.Unlock()
	if expected != "" {
		current, err := s.currentHostScheduleVersion()
		if err != nil {
			return "", err
		}
		if current != expected {
			return "", errHostScheduleConflict
		}
	}
	st.ResourceVersion = "" // derived from the file, never stored in it
	if err := s.saveSchedule(st); err != nil {
		return "", err
	}
	return s.currentHostScheduleVersion()
}
