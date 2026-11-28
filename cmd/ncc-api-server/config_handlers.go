package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"goncc/internal/plural"
)

func (s *apiServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		reqPath := strings.TrimSpace(r.URL.Query().Get("path"))
		targetPath := s.configPath
		if reqPath != "" {
			targetPath = reqPath
		}
		cfgPath, err := s.validateConfigPath(targetPath)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		b, err := os.ReadFile(cfgPath)
		if errors.Is(err, os.ErrNotExist) {
			// Bootstrap a dummy config through the root orchestrator flow.
			// validate-config currently requires an existing file, while root --config creates one when missing.
			out, runErr := s.runOrchestrator([]string{"--config", cfgPath}, 30*time.Second)
			b, err = os.ReadFile(cfgPath)
			if err == nil {
				s.audit(r, "settings.config.bootstrap", true, map[string]interface{}{"config_path": cfgPath})
			} else {
				data := map[string]interface{}{"output": tailString(redactSensitiveText(strings.TrimSpace(out)), 4000)}
				if runErr != nil {
					data["bootstrap_error"] = runErr.Error()
				}
				writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: fmt.Sprintf("config file not found: %s", cfgPath), Data: data})
				return
			}
		} else if err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"path":             cfgPath,
			"content":          string(b),
			"content_redacted": redactSensitiveText(string(b)),
		}})
	case http.MethodPut:
		// Configuration is intentionally writable in Kubernetes: the active
		// file lives on the shared PVC and is consumed by both the API and
		// controller-managed runner. Only host lifecycle mutations are blocked
		// by runtime capabilities.
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req configUpdateRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if strings.TrimSpace(req.Content) == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "content is required"})
			return
		}
		targetPath := s.configPath
		if strings.TrimSpace(req.Path) != "" {
			targetPath = strings.TrimSpace(req.Path)
		}
		cfgPath, err := s.validateConfigPath(targetPath)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		ext := filepath.Ext(cfgPath)
		base := strings.TrimSuffix(cfgPath, ext)
		if ext == "" {
			ext = ".yaml"
			base = cfgPath
		}
		// Keep temp file extension parseable by Viper during validate-config.
		tmpPath := base + ".tmp" + ext
		if err := writeSecretFile(tmpPath, []byte(req.Content)); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		defer os.Remove(tmpPath)
		cmd := s.makeOrchestratorCommand(context.TODO(), "validate-config", "--config", tmpPath)
		cmd.Dir = s.absPath(s.repoRoot)
		out, err := cmd.CombinedOutput()
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{
				Success: false,
				Error:   fmt.Sprintf("strict config validation failed: %v", err),
				Data:    map[string]string{"output": redactSensitiveText(string(out))},
			})
			return
		}
		if err := renameSecretFile(tmpPath, cfgPath); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "settings.config.update", true, map[string]interface{}{"config_path": cfgPath})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "config updated", Data: map[string]interface{}{
			"path":     cfgPath,
			"validate": true,
		}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) applyConfigBatchOperation(r *http.Request, op configBatchOperation) map[string]interface{} {
	action := strings.ToLower(strings.TrimSpace(op.Action))
	result := map[string]interface{}{
		"action": action,
		"path":   strings.TrimSpace(op.Path),
		"ok":     false,
	}
	cfgPath, err := s.validateConfigPath(op.Path)
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	result["resolved"] = cfgPath
	activeCfgPath, _ := s.validateConfigPath(s.configPath)
	switch action {
	case "add", "update":
		if strings.TrimSpace(op.Content) == "" {
			result["error"] = "content is required for add/update"
			return result
		}
		if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
			result["error"] = err.Error()
			return result
		}
		ext := filepath.Ext(cfgPath)
		base := strings.TrimSuffix(cfgPath, ext)
		if ext == "" {
			ext = ".yaml"
			base = cfgPath
		}
		tmpPath := base + ".tmp" + ext
		if err := writeSecretFile(tmpPath, []byte(op.Content)); err != nil {
			result["error"] = err.Error()
			return result
		}
		defer os.Remove(tmpPath)
		cmd := s.makeOrchestratorCommand(context.TODO(), "validate-config", "--config", tmpPath)
		cmd.Dir = s.absPath(s.repoRoot)
		out, err := cmd.CombinedOutput()
		if err != nil {
			result["error"] = fmt.Sprintf("strict config validation failed: %v", err)
			result["validate_output"] = tailString(redactSensitiveText(string(out)), 2000)
			return result
		}
		if err := renameSecretFile(tmpPath, cfgPath); err != nil {
			result["error"] = err.Error()
			return result
		}
		result["ok"] = true
		result["exists"] = true
		s.audit(r, "settings.config.batch_update", true, map[string]interface{}{"config_path": cfgPath, "action": action})
		return result
	case "remove", "delete":
		if activeCfgPath != "" && strings.EqualFold(cfgPath, activeCfgPath) {
			result["error"] = "cannot remove active config-path"
			return result
		}
		if err := os.Remove(cfgPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			result["error"] = err.Error()
			return result
		}
		result["ok"] = true
		result["exists"] = false
		s.audit(r, "settings.config.batch_delete", true, map[string]interface{}{"config_path": cfgPath, "action": action})
		return result
	default:
		result["error"] = "action must be one of: add, update, remove, delete"
		return result
	}
}

func (s *apiServer) handleConfigBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	var req configBatchRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if len(req.Operations) == 0 {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "operations must not be empty"})
		return
	}
	results := make([]map[string]interface{}, 0, len(req.Operations))
	okCount := 0
	failCount := 0
	for _, op := range req.Operations {
		res := s.applyConfigBatchOperation(r, op)
		if ok, _ := res["ok"].(bool); ok {
			okCount++
		} else {
			failCount++
		}
		results = append(results, res)
	}
	msg := fmt.Sprintf("processed %s: %d succeeded, %d failed", plural.Count(len(req.Operations), "operation"), okCount, failCount)
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: msg, Data: map[string]interface{}{
		"total":   len(req.Operations),
		"ok":      okCount,
		"failed":  failCount,
		"results": results,
	}})
}

func (s *apiServer) discoverAvailableConfigFiles() ([]availableConfigFile, error) {
	activeCfgPath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return nil, err
	}
	rootAbs, err := filepath.Abs(s.repoRoot)
	if err != nil {
		return nil, err
	}
	rootAbs = filepath.Clean(rootAbs)
	candidates := map[string]bool{activeCfgPath: true}

	// "configs/" is the canonical home for additional per-cluster config files.
	configsDir := filepath.Join(rootAbs, "configs")
	_ = filepath.WalkDir(configsDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}
		if abs, err := s.validateConfigPath(path); err == nil {
			candidates[abs] = true
		}
		return nil
	})

	// Also include any immediate sibling .ya?ml files near the active config.
	// Operators commonly create per-cluster files with non-"config*" names.
	activeDir := filepath.Dir(activeCfgPath)
	if entries, readErr := os.ReadDir(activeDir); readErr == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := strings.ToLower(entry.Name())
			ext := strings.ToLower(filepath.Ext(name))
			if ext != ".yaml" && ext != ".yml" {
				continue
			}
			if abs, err := s.validateConfigPath(filepath.Join(activeDir, entry.Name())); err == nil {
				candidates[abs] = true
			}
		}
	}

	items := make([]availableConfigFile, 0, len(candidates))
	for abs := range candidates {
		rel, relErr := filepath.Rel(rootAbs, abs)
		pathVal := abs
		if relErr == nil && rel != "" && !strings.HasPrefix(rel, "..") {
			pathVal = filepath.ToSlash(rel)
		}
		_, statErr := os.Stat(abs)
		items = append(items, availableConfigFile{
			Path:     pathVal,
			Resolved: abs,
			Exists:   statErr == nil,
			IsActive: strings.EqualFold(abs, activeCfgPath),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].IsActive != items[j].IsActive {
			return items[i].IsActive
		}
		return items[i].Path < items[j].Path
	})
	return items, nil
}

func (s *apiServer) handleConfigsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	items, err := s.discoverAvailableConfigFiles()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	defaultPath := ""
	if p, ok := principalFromContext(r.Context()); ok && s.users != nil {
		defaultPath = strings.TrimSpace(s.users.getRunConfigPath(p.subject))
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"items":        items,
		"default_path": defaultPath,
	}})
}

func (s *apiServer) handleRunConfigPreference(w http.ResponseWriter, r *http.Request) {
	if s.users == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "user preferences require a writable user database"})
		return
	}
	p, ok := principalFromContext(r.Context())
	if !ok || strings.TrimSpace(p.subject) == "" {
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		pref := strings.TrimSpace(s.users.getRunConfigPath(p.subject))
		data := map[string]interface{}{"path": pref}
		if strings.HasPrefix(pref, "env:") {
			data = map[string]interface{}{"environment": strings.TrimPrefix(pref, "env:")}
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req runConfigPreferenceRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		val := strings.TrimSpace(req.Path)
		if env := strings.TrimSpace(req.Environment); env != "" {
			if !s.environmentExists(env) {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("environment %q does not exist", env)})
				return
			}
			val = "env:" + env
		} else if val != "" {
			if _, err := s.resolveRunConfig(nil, "", val); err != nil {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
				return
			}
		}
		if err := s.users.setRunConfigPath(p.subject, val); err != nil {
			writeUserStoreError(w, err)
			return
		}
		s.audit(r, "runs.config_preference.update", true, map[string]interface{}{"path": val})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "run config preference updated", Data: map[string]interface{}{"path": val}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}
