package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// This file adds a scheduled "digest" notification: a recurring summary of the
// latest NCC run's health, delivered over the configured channels (typically
// email). Like scheduled backups it runs on an in-process ticker so the
// always-on api-server owns the cadence; it reads the canonical run-summary.json
// the dashboard already serves, so no extra run is triggered. The digest is an
// informational, user-scheduled message, so it intentionally bypasses quiet
// hours / dedup (it still honors an active maintenance window).

var digestScheduleMu = make(chan struct{}, 1) // 1-slot mutex via channel

// startNotificationDigestLoop launches the digest ticker. It always runs and
// no-ops while the digest is disabled, so the cadence can be toggled at runtime.
func (s *apiServer) startNotificationDigestLoop(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(60 * time.Second):
		}
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			s.maybeSendDigest(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *apiServer) maybeSendDigest(ctx context.Context) {
	st, err := s.loadNotifications()
	if err != nil || !st.Enabled || !st.Digest.Enabled {
		return
	}
	if !digestDue(st.Digest, time.Now().UTC()) {
		return
	}
	// Honor an active maintenance window (defer the digest to the next tick).
	if inMaint, _ := inMaintenance(st.Maintenance, time.Now()); inMaint {
		return
	}
	select {
	case digestScheduleMu <- struct{}{}:
		defer func() { <-digestScheduleMu }()
	default:
		return // a send is already in progress
	}
	s.sendDigestOnce(st)
}

// digestDue reports whether the digest interval has elapsed since the last send.
func digestDue(d digestConfig, now time.Time) bool {
	if !d.Enabled {
		return false
	}
	every, err := parseEveryDuration(d.Every)
	if err != nil {
		return false
	}
	if strings.TrimSpace(d.LastSentAt) == "" {
		return true
	}
	last, err := time.Parse(time.RFC3339, d.LastSentAt)
	if err != nil {
		return true
	}
	return now.Sub(last) >= every
}

func (s *apiServer) sendDigestOnce(st notificationState) {
	title, body := s.buildDigest()
	details := map[string]interface{}{
		"summary":   body,
		"generated": time.Now().UTC().Format(time.RFC3339),
	}
	// Dispatch directly (bypasses event throttle/quiet-hours by design).
	_ = s.dispatchNotifications(&st, "digest", title, details, nil)

	// Persist last-sent without clobbering a concurrent config edit: reload,
	// set the timestamp, save.
	cur, err := s.loadNotifications()
	if err != nil {
		cur = st
	}
	cur.Digest.LastSentAt = time.Now().UTC().Format(time.RFC3339)
	if saveErr := s.saveNotifications(cur); saveErr != nil {
		// Non-fatal: a failed persist just means the next tick may re-send.
		return
	}
}

// buildDigest reads the latest run-summary.json and renders a compact,
// human-readable health summary (title + multi-line body). When no summary is
// available it still returns a sensible "no run yet" message.
func (s *apiServer) buildDigest() (title, body string) {
	path := filepath.Join(s.selectBestReportOutDir(), "run-summary.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return "NCC health digest", "No NCC run summary is available yet on this server."
	}
	var sum trendRunSummary
	if err := json.Unmarshal(b, &sum); err != nil {
		return "NCC health digest", "Could not parse the latest NCC run summary."
	}
	var fail, warn, errc int
	for _, c := range sum.Clusters {
		fail += c.FailCount
		warn += c.WarnCount
		errc += c.ErrCount
	}
	clusters := sum.ClustersOK + sum.ClustersFailed
	status := "healthy"
	if sum.ClustersFailed > 0 || fail > 0 {
		status = "attention needed"
	}
	title = fmt.Sprintf("NCC health digest — %s (%d fail, %d warn)", status, fail, warn)

	var lines []string
	lines = append(lines, fmt.Sprintf("NCC health digest (%s)", time.Now().UTC().Format("2006-01-02 15:04 UTC")))
	if strings.TrimSpace(sum.Timestamp) != "" {
		lines = append(lines, "Latest run: "+sum.Timestamp)
	}
	lines = append(lines, fmt.Sprintf("Clusters: %d total — %d ok, %d failed", clusters, sum.ClustersOK, sum.ClustersFailed))
	lines = append(lines, fmt.Sprintf("Checks: %d total — %d FAIL, %d WARN, %d ERR", sum.TotalChecks, fail, warn, errc))
	if sum.AvgHealthScore > 0 || sum.MinHealthScore > 0 {
		lines = append(lines, fmt.Sprintf("Health score: avg %d, min %d", sum.AvgHealthScore, sum.MinHealthScore))
	}
	// Top policy violations, if any, give the digest teeth.
	if viol := s.readPolicyViolations(); len(viol) > 0 {
		sort.Strings(viol)
		max := len(viol)
		if max > 10 {
			max = 10
		}
		lines = append(lines, "", fmt.Sprintf("Policy violations (%d):", len(viol)))
		for _, v := range viol[:max] {
			lines = append(lines, "  - "+v)
		}
		if len(viol) > max {
			lines = append(lines, fmt.Sprintf("  … and %d more", len(viol)-max))
		}
	}
	return title, strings.Join(lines, "\n")
}
