package notify

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// StatusFileName is the per-channel delivery status shared by the runner and
// the API server. It lives in the output directory, which both processes
// already share on every platform (the API reads run heartbeats there, and on
// Kubernetes it is the shared volume).
const StatusFileName = ".ncc-notification-status.json"

// statusLockTimeout bounds how long a sender waits for another process's
// status update; status is best-effort and must never hold up delivery.
const statusLockTimeout = 2 * time.Second

// ChannelStatus is the last known delivery outcome of one channel.
type ChannelStatus struct {
	LastAttemptAt string `json:"last_attempt_at,omitempty"`
	LastSuccessAt string `json:"last_success_at,omitempty"`
	// LastError is empty after a successful delivery. It is redacted with
	// RedactURLPaths before it is stored: webhook URLs carry their secret.
	LastError string `json:"last_error,omitempty"`
	// Attempts is how many tries the last delivery took (success or not).
	Attempts int  `json:"attempts,omitempty"`
	Success  bool `json:"success"`
	// Source is "runner" (result messages after a run) or "api" (alerts,
	// digests and test sends from the API server).
	Source string `json:"source,omitempty"`
	Event  string `json:"event,omitempty"`
	// DeadLetterDir is where the writer saved undelivered messages, as an
	// absolute path, when one is configured.
	DeadLetterDir string `json:"dead_letter_dir,omitempty"`
}

// StatusFile is the JSON document stored in StatusFileName.
type StatusFile struct {
	Channels  map[string]ChannelStatus `json:"channels"`
	UpdatedAt string                   `json:"updated_at,omitempty"`
}

// DeliveryOutcome describes one finished delivery (after retries).
type DeliveryOutcome struct {
	Channel       string
	Source        string
	Event         string
	Attempts      int
	Err           error
	DeadLetterDir string
}

// statusMu serializes writers inside one process; the file lock covers other
// processes (runner + API server, API replicas on a shared volume).
var statusMu sync.Mutex

var statusTmpSeq atomic.Uint64

// RecordDelivery updates the channel's entry in dir/StatusFileName under a
// file lock. It is best-effort: an empty dir is a no-op and errors are
// returned for the caller to log, never to fail a delivery.
func RecordDelivery(dir string, o DeliveryOutcome, now time.Time) error {
	dir = strings.TrimSpace(dir)
	if dir == "" || strings.TrimSpace(o.Channel) == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, StatusFileName)
	statusMu.Lock()
	defer statusMu.Unlock()
	unlock, err := lockStatusFile(path+".lock", statusLockTimeout)
	if err != nil {
		return err
	}
	defer unlock()

	st, err := LoadStatus(dir)
	if err != nil {
		// A corrupt file must not block status forever: start over.
		st = StatusFile{}
	}
	if st.Channels == nil {
		st.Channels = map[string]ChannelStatus{}
	}
	ts := now.UTC().Format(time.RFC3339)
	item := st.Channels[o.Channel]
	item.LastAttemptAt = ts
	item.Attempts = o.Attempts
	item.Source = o.Source
	item.Event = o.Event
	if d := strings.TrimSpace(o.DeadLetterDir); d != "" {
		if abs, aerr := filepath.Abs(d); aerr == nil {
			d = abs
		}
		item.DeadLetterDir = d
	} else {
		item.DeadLetterDir = ""
	}
	if o.Err != nil {
		item.Success = false
		item.LastError = RedactURLPaths(o.Err.Error())
	} else {
		item.Success = true
		item.LastError = ""
		item.LastSuccessAt = ts
	}
	st.Channels[o.Channel] = item
	st.UpdatedAt = ts
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return writeStatusAtomic(path, b)
}

// LoadStatus reads dir/StatusFileName. A missing file is an empty status.
func LoadStatus(dir string) (StatusFile, error) {
	st := StatusFile{Channels: map[string]ChannelStatus{}}
	b, err := os.ReadFile(filepath.Join(dir, StatusFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return st, nil
		}
		return st, err
	}
	if len(strings.TrimSpace(string(b))) == 0 {
		return st, nil
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return StatusFile{Channels: map[string]ChannelStatus{}}, err
	}
	if st.Channels == nil {
		st.Channels = map[string]ChannelStatus{}
	}
	return st, nil
}

// CountDeadLetters counts the undelivered messages writeDeadLetter left in dir
// for channel ("<channel>-<stamp>-<nanos>.json"). An empty or missing dir is 0.
func CountDeadLetters(dir, channel string) int {
	if strings.TrimSpace(dir) == "" {
		return 0
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		if strings.HasPrefix(name, channel+"-") {
			n++
		}
	}
	return n
}

// recordRunnerDelivery is the runner's hook: status goes next to the run's
// reports in the filtered output directory.
func recordRunnerDelivery(dir, channel string, attempts int, err error, deadLetterDir string) {
	if rerr := RecordDelivery(dir, DeliveryOutcome{
		Channel:       channel,
		Source:        "runner",
		Event:         "run_report",
		Attempts:      attempts,
		Err:           err,
		DeadLetterDir: deadLetterDir,
	}, time.Now()); rerr != nil {
		logStatusError(channel, rerr)
	}
}

func writeStatusAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp := filepath.Join(dir, "."+filepath.Base(path)+"."+strconv.Itoa(os.Getpid())+"."+
		strconv.FormatUint(statusTmpSeq.Add(1), 10)+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

var errStatusLockTimeout = errors.New("timed out waiting for the notification status lock")

func logStatusError(channel string, err error) {
	log.Warn().Str("channel", channel).Str("error", RedactURLPaths(err.Error())).Msg("notification delivery status could not be saved")
}
