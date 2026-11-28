package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"goncc/internal/ai"
)

// scrubSummaryPaths reduces absolute filesystem paths recorded by the runner in
// run artifacts (index_html, output/log locations) to their base names before
// they are returned to API clients.
func scrubSummaryPaths(m map[string]interface{}) {
	for _, k := range []string{"index_html", "output_dir", "log_file", "config", "config_path", "run_dir", "output_dir_filtered", "output_dir_logs"} {
		if v, ok := m[k].(string); ok && v != "" && (filepath.IsAbs(v) || strings.Contains(v, "\\") || strings.HasPrefix(v, "/")) {
			m[k] = filepath.Base(strings.ReplaceAll(v, "\\", "/"))
		}
	}
}

// pathForCaller returns an absolute server path for admins and a path relative
// to the stack root for everyone else (viewers/operators poll endpoints such as
// /runs/active that previously returned /data/logs/ncc-runner.log etc.).
func (s *apiServer) pathForCaller(r *http.Request, p string) string {
	if strings.TrimSpace(p) == "" {
		return ""
	}
	if pr, ok := principalFromContext(r.Context()); ok && pr.role >= RoleAdmin {
		return s.absPath(p)
	}
	return s.displayRelPath(p)
}

// displayRelPath renders a server path relative to the stack root (repo root)
// for non-admin callers, so the UI can still show and submit it without the
// response revealing the absolute filesystem layout.
func (s *apiServer) displayRelPath(p string) string {
	abs := s.absPath(p)
	root := s.absPath(s.repoRoot)
	if rel, err := filepath.Rel(root, abs); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel) {
		return filepath.ToSlash(rel)
	}
	return filepath.Base(abs)
}

func (s *apiServer) loadSchedule() (scheduleState, error) {
	if s.capabilities.Kubernetes {
		return s.loadKubernetesSchedule()
	}
	path := s.absPath(s.scheduleStatePath)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return scheduleState{
				Type:      "auto",
				Action:    "create",
				Config:    s.configPath,
				LogPath:   "logs/ncc-scheduler.log",
				WithLock:  true,
				PrintOnly: true,
				UpdatedAt: "",

				ResourceVersion: hostScheduleNone,
			}, nil
		}
		return scheduleState{}, err
	}
	var st scheduleState
	if err := json.Unmarshal(b, &st); err != nil {
		return scheduleState{}, err
	}
	if strings.TrimSpace(st.LogPath) == "" {
		st.LogPath = "logs/ncc-scheduler.log"
	}
	st.ResourceVersion = hostScheduleVersion(b)
	return st, nil
}

func (s *apiServer) saveSchedule(st scheduleState) error {
	path := s.absPath(s.scheduleStatePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return writeSecretFile(path, b)
}

func (s *apiServer) absPath(p string) string {
	key := s.repoRoot + "\x00" + p // the answer depends on the root too
	if v, ok := s.absCache.get(key); ok {
		return v
	}
	v := s.resolveAbsPath(p)
	s.absCache.put(key, v)
	return v
}

func (s *apiServer) resolveAbsPath(p string) string {
	if abs, err := s.normalizeAndConfinePath(p); err == nil {
		return abs
	}
	if filepath.IsAbs(p) {
		return filepath.Clean(p)
	}
	return filepath.Clean(filepath.Join(s.repoRoot, p))
}

func writeJSON(w http.ResponseWriter, status int, v envelope) {
	if v.SchemaVersion == "" {
		v.SchemaVersion = ai.SchemaVersion
	}
	if !v.Success && strings.TrimSpace(v.Error) != "" && strings.TrimSpace(v.ErrorCode) == "" {
		v.ErrorCode = defaultAPIErrorCode(status)
	}
	if !v.Success && v.RequestID == "" {
		v.RequestID = w.Header().Get(requestIDHeader)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func defaultAPIErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "NCC_API_BAD_REQUEST"
	case http.StatusUnauthorized:
		return "NCC_API_UNAUTHORIZED"
	case http.StatusForbidden:
		return "NCC_API_FORBIDDEN"
	case http.StatusNotFound:
		return "NCC_API_NOT_FOUND"
	case http.StatusMethodNotAllowed:
		return "NCC_API_METHOD_NOT_ALLOWED"
	case http.StatusConflict:
		return "NCC_API_CONFLICT"
	case http.StatusUnsupportedMediaType:
		return "NCC_API_UNSUPPORTED_MEDIA_TYPE"
	case http.StatusTooManyRequests:
		return "NCC_API_RATE_LIMITED"
	case http.StatusBadGateway:
		return "NCC_API_UPSTREAM_FAILURE"
	case http.StatusInternalServerError:
		return "NCC_API_INTERNAL"
	default:
		if status >= 400 && status < 500 {
			return "NCC_API_CLIENT_ERROR"
		}
		if status >= 500 {
			return "NCC_API_SERVER_ERROR"
		}
		return ""
	}
}

func defaultIfEmpty(v, def string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return def
	}
	return v
}
