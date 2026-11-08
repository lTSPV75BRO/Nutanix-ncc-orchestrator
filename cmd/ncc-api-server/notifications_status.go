package main

import (
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"goncc/internal/notify"
)

// notificationChannelStatus is one row of GET /api/v1/settings/notifications/status.
// It merges the shared delivery-status file (written by the runner after each
// run and by this server for alerts, digests and test sends) with whether the
// channel is turned on and how many undelivered messages wait in the
// dead-letter folder.
type notificationChannelStatus struct {
	Channel       string `json:"channel"`
	Enabled       bool   `json:"enabled"`
	State         string `json:"state"` // ok | failing | never
	LastAttemptAt string `json:"last_attempt_at,omitempty"`
	LastSuccessAt string `json:"last_success_at,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	Attempts      int    `json:"attempts,omitempty"`
	Source        string `json:"source,omitempty"`
	Event         string `json:"event,omitempty"`
	DeadLetters   int    `json:"dead_letters"`
}

type notificationStatusResponse struct {
	Channels             []notificationChannelStatus `json:"channels"`
	DeadLetterConfigured bool                        `json:"dead_letter_configured"`
	UpdatedAt            string                      `json:"updated_at,omitempty"`
}

// notificationStatusDir is where the shared status file lives: the output
// directory, which the runner writes on every platform.
// Empty when the server has no repo root or output dir (bare test servers),
// which turns recording into a no-op instead of writing into the cwd.
func (s *apiServer) notificationStatusDir() string {
	if s == nil || strings.TrimSpace(s.repoRoot) == "" || strings.TrimSpace(s.outputDir) == "" {
		return ""
	}
	return s.absPath(s.outputDir)
}

// recordSharedDelivery mirrors a delivery this server made into the shared
// status file. Best-effort: a failed write is logged, never returned.
func (s *apiServer) recordSharedDelivery(channel, event string, err error) {
	dir := s.notificationStatusDir()
	if dir == "" {
		return
	}
	if rerr := notify.RecordDelivery(dir, notify.DeliveryOutcome{
		Channel:       channel,
		Source:        "api",
		Event:         event,
		Attempts:      1,
		Err:           err,
		DeadLetterDir: s.notificationDeadLetterDir(),
	}, time.Now()); rerr != nil {
		serverLog.Warn("notification delivery status could not be saved", "channel", channel, "error", redactURLPaths(rerr.Error()))
	}
}

// notificationDeadLetterDir is config.yaml's notification-deadletter-dir,
// resolved like the runner resolves it (relative to the config file).
func (s *apiServer) notificationDeadLetterDir() string {
	cfg, err := s.loadRawConfigMap()
	if err != nil {
		return ""
	}
	dir := strings.TrimSpace(readMapString(cfg, "notification-deadletter-dir", ""))
	if dir == "" || filepath.IsAbs(dir) {
		return dir
	}
	if cfgPath, perr := s.validateConfigPath(s.configPath); perr == nil {
		return filepath.Join(filepath.Dir(cfgPath), dir)
	}
	return s.absPath(dir)
}

func (s *apiServer) handleNotificationsStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	st, err := s.loadNotifications()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	shared, err := notify.LoadStatus(s.notificationStatusDir())
	if err != nil {
		// A torn or corrupt file reads as "no history" rather than failing the page.
		serverLog.Warn("notification delivery status unreadable", "error", err.Error())
	}
	dl := s.notificationDeadLetterDir()
	enabled := enabledChannels(st)
	resp := notificationStatusResponse{DeadLetterConfigured: dl != "", UpdatedAt: shared.UpdatedAt}
	for _, ch := range notify.Channels {
		item := shared.Channels[ch]
		row := notificationChannelStatus{
			Channel:       ch,
			Enabled:       enabled[ch],
			LastAttemptAt: item.LastAttemptAt,
			LastSuccessAt: item.LastSuccessAt,
			// Stored redacted; redact again so a file written by an older
			// build can never leak a webhook path through this endpoint.
			LastError: notify.RedactURLPaths(item.LastError),
			Attempts:  item.Attempts,
			Source:    item.Source,
			Event:     item.Event,
		}
		switch {
		case item.LastAttemptAt == "":
			row.State = "never"
		case item.Success:
			row.State = "ok"
		default:
			row.State = "failing"
		}
		dir := dl
		if dir == "" {
			dir = item.DeadLetterDir
		}
		row.DeadLetters = notify.CountDeadLetters(dir, ch)
		resp.Channels = append(resp.Channels, row)
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: resp})
}
