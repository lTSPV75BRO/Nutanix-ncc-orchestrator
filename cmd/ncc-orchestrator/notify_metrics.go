package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"goncc/internal/notify"
	"goncc/internal/promtext"
)

func parseHHMM(s string) (int, int, error) {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected HH:MM")
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	if h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, 0, fmt.Errorf("time out of range")
	}
	return h, m, nil
}

func inQuietHours(now time.Time, quietHours string) (bool, error) {
	q := strings.TrimSpace(quietHours)
	if q == "" {
		return false, nil
	}
	parts := strings.Split(q, "-")
	if len(parts) != 2 {
		return false, fmt.Errorf("quiet-hours must be HH:MM-HH:MM")
	}
	sh, sm, err := parseHHMM(parts[0])
	if err != nil {
		return false, fmt.Errorf("quiet-hours start: %w", err)
	}
	eh, em, err := parseHHMM(parts[1])
	if err != nil {
		return false, fmt.Errorf("quiet-hours end: %w", err)
	}
	curMins := now.Hour()*60 + now.Minute()
	startMins := sh*60 + sm
	endMins := eh*60 + em
	if startMins == endMins {
		return true, nil // full-day quiet window
	}
	if startMins < endMins {
		return curMins >= startMins && curMins < endMins, nil
	}
	return curMins >= startMins || curMins < endMins, nil
}

func inMaintenanceWindow(now time.Time, windows []string) (bool, error) {
	for _, w := range windows {
		w = strings.TrimSpace(w)
		if w == "" {
			continue
		}
		parts := strings.Split(w, "/")
		if len(parts) != 2 {
			return false, fmt.Errorf("maintenance window %q must be start/end RFC3339", w)
		}
		start, err := time.Parse(time.RFC3339, strings.TrimSpace(parts[0]))
		if err != nil {
			return false, fmt.Errorf("maintenance window start %q: %w", parts[0], err)
		}
		end, err := time.Parse(time.RFC3339, strings.TrimSpace(parts[1]))
		if err != nil {
			return false, fmt.Errorf("maintenance window end %q: %w", parts[1], err)
		}
		if !end.After(start) {
			return false, fmt.Errorf("maintenance window %q end must be after start", w)
		}
		if (now.Equal(start) || now.After(start)) && now.Before(end) {
			return true, nil
		}
	}
	return false, nil
}

func notificationsSuppressedNow(cfg Config, now time.Time) (bool, string) {
	if ok, err := inQuietHours(now, cfg.QuietHours); err == nil && ok {
		return true, "quiet-hours"
	}
	if ok, err := inMaintenanceWindow(now, cfg.MaintenanceWindows); err == nil && ok {
		return true, "maintenance-window"
	}
	return false, ""
}

// ==================== Prometheus Metrics ====================
// The Prometheus textfile writers were extracted to goncc/internal/promtext.
// sanitizeLabel/writePrometheusFile are aliases so existing call sites in this
// package are unchanged.
var (
	sanitizeLabel       = promtext.SanitizeLabel
	writePrometheusFile = promtext.WritePrometheusFile
)

// writeNotificationMetricsFile snapshots the run's notification accumulator
// (owned by internal/notify) and delegates the textfile rendering to promtext.
func writeNotificationMetricsFile(fs FS, promDir string) error {
	attempts, failures := notify.SnapshotMetrics()
	return promtext.WriteNotificationMetricsFile(fs, promDir, notify.Channels, attempts, failures)
}

// ==================== Notifications ====================
// The email/webhook/slack senders, retry wrappers, text/template overrides,
// and the per-channel delivery-metrics accumulator were extracted to
// goncc/internal/notify. These aliases keep the existing call sites in this
// package unchanged; the run-level accumulator now lives in that package and
// is read via notify.ResetMetrics / notify.SnapshotMetrics.
var (
	sendEmailWithRetry   = notify.SendEmailWithRetry
	sendWebhookWithRetry = notify.SendWebhookWithRetry
	sendSlackWithRetry   = notify.SendSlackWithRetry
	applyEmailTemplates  = notify.ApplyEmailTemplates
)
