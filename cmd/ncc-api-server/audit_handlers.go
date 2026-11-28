package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// splitAuditPrefixes parses ?exclude=http.request,mcp.call into prefixes.
func splitAuditPrefixes(raw string) []string {
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" && len(p) <= 64 {
			out = append(out, p)
		}
	}
	return out
}

// handleAudit returns recent audit log entries (newest first) read from the
// JSONL file. Filters: ?limit=200, ?action=settings, ?exclude=http.request,
// ?channel=mcp, ?failures=1.
func (s *apiServer) handleAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	q := r.URL.Query()
	limit := 100
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid limit: must be a positive integer"})
			return
		}
		if n > 10000 {
			n = 10000
		}
		limit = n
	}
	filter := auditFilter{
		actionPrefix: strings.TrimSpace(q.Get("action")),
		user:         strings.TrimSpace(q.Get("user")),
		channel:      strings.TrimSpace(q.Get("channel")),
		exclude:      splitAuditPrefixes(q.Get("exclude")),
		onlyFailures: q.Get("failures") == "1" || strings.EqualFold(q.Get("failures"), "true"),
	}
	// since/until accept either an RFC3339 timestamp or a YYYY-MM-DD date.
	if raw := strings.TrimSpace(q.Get("since")); raw != "" {
		t, err := parseAuditTime(raw, false)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid since: use RFC3339 or YYYY-MM-DD"})
			return
		}
		filter.since = t
	}
	if raw := strings.TrimSpace(q.Get("until")); raw != "" {
		t, err := parseAuditTime(raw, true)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid until: use RFC3339 or YYYY-MM-DD"})
			return
		}
		filter.until = t
	}

	entries, err := s.auditEntries(limit, filter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "read audit log: " + err.Error()})
		return
	}

	// CSV export streams a downloadable file rather than the JSON envelope.
	if strings.EqualFold(strings.TrimSpace(q.Get("format")), "csv") {
		s.writeAuditCSV(w, entries)
		return
	}

	abs := s.absPath(s.auditLogPath)
	var size int64
	var modTime string
	if st, err := os.Stat(abs); err == nil {
		size = st.Size()
		modTime = st.ModTime().UTC().Format(time.RFC3339)
	}

	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"path":      abs,
		"size":      size,
		"mod_time":  modTime,
		"limit":     limit,
		"count":     len(entries),
		"max_bytes": s.auditLogMaxBytes,
		"entries":   entries,
		"filters": map[string]interface{}{
			"action":   filter.actionPrefix,
			"user":     filter.user,
			"channel":  filter.channel,
			"failures": filter.onlyFailures,
			"since":    q.Get("since"),
			"until":    q.Get("until"),
		},
	}})
}

// parseAuditTime parses an RFC3339 timestamp or a YYYY-MM-DD date (UTC). For a
// bare date used as an upper bound, endOfDay extends it to the end of that day
// so "until=2026-06-05" includes the whole day.
func parseAuditTime(raw string, endOfDay bool) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, nil
	}
	d, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, err
	}
	if endOfDay {
		return d.Add(24*time.Hour - time.Nanosecond), nil
	}
	return d, nil
}

// writeAuditCSV renders audit entries as a downloadable CSV with stable columns;
// any keys beyond the common ones are folded into a JSON "details" column.
func (s *apiServer) writeAuditCSV(w http.ResponseWriter, entries []map[string]interface{}) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"ncc-audit-%s.csv\"", time.Now().UTC().Format("20060102-150405")))
	cw := csv.NewWriter(w)
	cols := []string{"ts", "user", "role", "auth", "pat_id", "pat_name", "client_kind", "ui_version", "ui_server_version", "ui_compromised", "channel", "action", "success", "error", "error_code", "remediation", "client", "method", "path", "user_agent"}
	_ = cw.Write(append(append([]string{}, cols...), "details"))
	known := map[string]bool{}
	for _, c := range cols {
		known[c] = true
	}
	for _, e := range entries {
		row := make([]string, 0, len(cols)+1)
		for _, c := range cols {
			row = append(row, auditCSVCell(e[c]))
		}
		details := map[string]interface{}{}
		for k, v := range e {
			if !known[k] {
				details[k] = v
			}
		}
		if len(details) > 0 {
			b, _ := json.Marshal(details)
			row = append(row, string(b))
		} else {
			row = append(row, "")
		}
		_ = cw.Write(row)
	}
	cw.Flush()
}

func auditCSVCell(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", t)
	}
}
