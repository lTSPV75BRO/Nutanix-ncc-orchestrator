package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"goncc/internal/plural"

	yaml "go.yaml.in/yaml/v3"
)

func parseScalarConfigValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case fmt.Stringer:
		return strings.TrimSpace(t.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func isUnsetConfigPathLiteral(val string) bool {
	v := strings.ToLower(strings.TrimSpace(val))
	return v == "" || v == "null" || v == "nil" || v == "<nil>" || v == "~"
}

func extractRelatedFilePathFromConfig(content string, key string) string {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return ""
	}
	var ym map[string]interface{}
	if err := yaml.Unmarshal([]byte(content), &ym); err == nil {
		val := parseScalarConfigValue(ym[key])
		if isUnsetConfigPathLiteral(val) {
			return ""
		}
		return val
	}
	if strings.HasPrefix(trimmed, "{") {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(trimmed), &m); err == nil {
			val := parseScalarConfigValue(m[key])
			if isUnsetConfigPathLiteral(val) {
				return ""
			}
			return val
		}
	}
	pattern := fmt.Sprintf(`(?m)^\s*%s\s*:\s*(.+?)\s*$`, regexp.QuoteMeta(key))
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(content)
	if len(matches) < 2 {
		return ""
	}
	val := strings.TrimSpace(matches[1])
	val = strings.Trim(val, `"'`)
	if isUnsetConfigPathLiteral(val) {
		return ""
	}
	return val
}

func validateClusterAddressForConfigFile(cluster string) error {
	cluster = strings.TrimSpace(cluster)
	if cluster == "" {
		return errors.New("cluster address cannot be empty")
	}
	if len(cluster) > 255 {
		return errors.New("cluster address too long")
	}
	if net.ParseIP(cluster) != nil {
		return nil
	}
	if strings.Contains(cluster, "..") || strings.HasPrefix(cluster, ".") || strings.HasSuffix(cluster, ".") {
		return errors.New("invalid cluster hostname format")
	}
	for _, r := range cluster {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			continue
		}
		return fmt.Errorf("invalid character %q in cluster address", r)
	}
	return nil
}

func validateClustersFileContent(content string) error {
	for i, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		r := csv.NewReader(strings.NewReader(line))
		r.TrimLeadingSpace = true
		r.FieldsPerRecord = -1
		rec, err := r.Read()
		if err != nil {
			return fmt.Errorf("line %d: invalid CSV format: %w", i+1, err)
		}
		for idx := range rec {
			rec[idx] = strings.TrimSpace(rec[idx])
		}
		switch len(rec) {
		case 1:
			if err := validateClusterAddressForConfigFile(rec[0]); err != nil {
				return fmt.Errorf("line %d: %w", i+1, err)
			}
		case 2, 3:
			if err := validateClusterAddressForConfigFile(rec[0]); err != nil {
				return fmt.Errorf("line %d: %w", i+1, err)
			}
			if rec[1] == "" {
				return fmt.Errorf("line %d: username is empty", i+1)
			}
		default:
			return fmt.Errorf("line %d: expected cluster[,username[,password]]", i+1)
		}
	}
	return nil
}

func validateExcludeAlertTitlesFileContent(content string, matchMode string) error {
	mode := strings.ToLower(strings.TrimSpace(matchMode))
	if mode == "" {
		mode = "exact"
	}
	if mode != "exact" && mode != "contains" && mode != "regex" {
		return fmt.Errorf("unsupported exclude-alert-match-mode: %s", matchMode)
	}
	if mode != "regex" {
		return nil
	}
	for i, line := range strings.Split(content, "\n") {
		title := strings.TrimSpace(line)
		if title == "" || strings.HasPrefix(title, "#") {
			continue
		}
		if _, err := regexp.Compile(title); err != nil {
			return fmt.Errorf("line %d: invalid regex %q: %w", i+1, title, err)
		}
	}
	return nil
}

func validateSecretsFileContent(content string) error {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return nil
	}
	var jsonMap map[string]string
	if json.Unmarshal([]byte(trimmed), &jsonMap) == nil {
		return nil
	}
	var yamlMap map[string]interface{}
	if err := yaml.Unmarshal([]byte(trimmed), &yamlMap); err != nil {
		return fmt.Errorf("secrets file must be valid YAML/JSON map: %w", err)
	}
	for k, v := range yamlMap {
		if _, ok := v.(string); !ok {
			return fmt.Errorf("secret %q must be a string value", k)
		}
	}
	return nil
}

func (s *apiServer) configScalarValue(key string, def string) string {
	cfgPath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return def
	}
	content, err := os.ReadFile(cfgPath)
	if err != nil {
		return def
	}
	val := strings.TrimSpace(extractRelatedFilePathFromConfig(string(content), key))
	if val == "" {
		val = strings.TrimSpace(extractCanonicalConfigValue(string(content), key))
	}
	if val == "" {
		return def
	}
	return val
}

func extractCanonicalConfigValue(content, key string) string {
	paths := map[string][]string{
		"cluster-source-mode": {"runner", "targets", "mode"},
		"clusters":            {"runner", "targets", "clusters"},
		"pcs":                 {"runner", "targets", "pcs"},
		"pc-alerts-cache-ttl": {"api", "cache", "pc-alerts-cache-ttl"},
	}
	path, ok := paths[key]
	if !ok {
		return ""
	}
	var root map[string]interface{}
	if err := yaml.Unmarshal([]byte(content), &root); err != nil {
		return ""
	}
	var current interface{} = root
	for _, part := range path {
		values, ok := current.(map[string]interface{})
		if !ok {
			return ""
		}
		current = values[part]
	}
	switch values := current.(type) {
	case []interface{}:
		parts := make([]string, 0, len(values))
		for _, value := range values {
			if item := strings.TrimSpace(fmt.Sprint(value)); item != "" {
				parts = append(parts, item)
			}
		}
		return strings.Join(parts, ",")
	default:
		return fmt.Sprint(values)
	}
}

func (s *apiServer) validateConfigRelatedFileContent(ref *configRelatedFile, content string) error {
	switch ref.Key {
	case "clusters-file":
		return validateClustersFileContent(content)
	case "exclude-alert-titles-file":
		return validateExcludeAlertTitlesFileContent(content, s.configScalarValue("exclude-alert-match-mode", "exact"))
	case "secrets-file":
		return validateSecretsFileContent(content)
	default:
		return nil
	}
}

func (s *apiServer) discoverConfigRelatedFiles() ([]configRelatedFile, error) {
	cfgPath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}
	keys := []string{
		"clusters-file",
		"exclude-alert-titles-file",
		"secrets-file",
	}
	out := make([]configRelatedFile, 0, len(keys))
	for _, key := range keys {
		rawPath := extractRelatedFilePathFromConfig(string(content), key)
		if rawPath == "" {
			continue
		}
		resolved, err := s.normalizeAndConfinePath(rawPath)
		if err != nil {
			out = append(out, configRelatedFile{
				Key:          key,
				Path:         rawPath,
				ResolvedPath: "",
				Exists:       false,
			})
			continue
		}
		info, statErr := os.Stat(resolved)
		item := configRelatedFile{
			Key:          key,
			Path:         rawPath,
			ResolvedPath: resolved,
			Exists:       statErr == nil,
		}
		if statErr == nil {
			item.Size = info.Size()
		}
		out = append(out, item)
	}
	return out, nil
}

func (s *apiServer) relatedConfigFileByPath(rawPath string) (*configRelatedFile, error) {
	target := strings.TrimSpace(rawPath)
	if isUnsetConfigPathLiteral(target) {
		return nil, errors.New("path is required")
	}
	items, err := s.discoverConfigRelatedFiles()
	if err != nil {
		return nil, err
	}
	for i := range items {
		item := items[i]
		if strings.EqualFold(item.Path, target) || (item.ResolvedPath != "" && strings.EqualFold(item.ResolvedPath, target)) {
			return &item, nil
		}
	}
	return nil, fmt.Errorf("path is not referenced by config: %s", target)
}

func (s *apiServer) handleConfigFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	items, err := s.discoverConfigRelatedFiles()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"items": items,
	}})
}

func (s *apiServer) handleConfigFile(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		pathArg := strings.TrimSpace(r.URL.Query().Get("path"))
		ref, err := s.relatedConfigFileByPath(pathArg)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if ref.ResolvedPath == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "path cannot be resolved inside repo root"})
			return
		}
		content, err := os.ReadFile(ref.ResolvedPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
					"key":      ref.Key,
					"path":     ref.Path,
					"resolved": ref.ResolvedPath,
					"exists":   false,
					"content":  "",
				}})
				return
			}
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"key":      ref.Key,
			"path":     ref.Path,
			"resolved": ref.ResolvedPath,
			"exists":   true,
			"content":  string(content),
		}})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req configRelatedFileUpdateRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		ref, err := s.relatedConfigFileByPath(req.Path)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if ref.ResolvedPath == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "path cannot be resolved inside repo root"})
			return
		}
		if err := s.validateConfigRelatedFileContent(ref, req.Content); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("validation failed for %s: %v", ref.Key, err)})
			return
		}
		if err := os.MkdirAll(filepath.Dir(ref.ResolvedPath), 0o755); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := writeSecretFile(ref.ResolvedPath, []byte(req.Content)); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "settings.config_file.update", true, map[string]interface{}{"key": ref.Key, "path": ref.Path, "resolved": ref.ResolvedPath})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "config file updated", Data: map[string]interface{}{
			"key":      ref.Key,
			"path":     ref.Path,
			"resolved": ref.ResolvedPath,
			"exists":   true,
		}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) applyConfigFileOperation(r *http.Request, op configRelatedFileBatchOperation) map[string]interface{} {
	action := strings.ToLower(strings.TrimSpace(op.Action))
	result := map[string]interface{}{
		"action": action,
		"path":   strings.TrimSpace(op.Path),
		"ok":     false,
	}
	ref, err := s.relatedConfigFileByPath(op.Path)
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	result["key"] = ref.Key
	result["resolved"] = ref.ResolvedPath
	if ref.ResolvedPath == "" {
		result["error"] = "path cannot be resolved inside repo root"
		return result
	}
	switch action {
	case "add", "update":
		if err := s.validateConfigRelatedFileContent(ref, op.Content); err != nil {
			result["error"] = fmt.Sprintf("validation failed for %s: %v", ref.Key, err)
			return result
		}
		if err := os.MkdirAll(filepath.Dir(ref.ResolvedPath), 0o755); err != nil {
			result["error"] = err.Error()
			return result
		}
		if err := writeSecretFile(ref.ResolvedPath, []byte(op.Content)); err != nil {
			result["error"] = err.Error()
			return result
		}
		result["ok"] = true
		result["exists"] = true
		s.audit(r, "settings.config_file.update", true, map[string]interface{}{
			"key": ref.Key, "path": ref.Path, "resolved": ref.ResolvedPath, "action": action,
		})
		return result
	case "remove", "delete":
		if err := os.Remove(ref.ResolvedPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			result["error"] = err.Error()
			return result
		}
		result["ok"] = true
		result["exists"] = false
		s.audit(r, "settings.config_file.delete", true, map[string]interface{}{
			"key": ref.Key, "path": ref.Path, "resolved": ref.ResolvedPath, "action": action,
		})
		return result
	default:
		result["error"] = "action must be one of: add, update, remove, delete"
		return result
	}
}

func (s *apiServer) handleConfigFilesBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	var req configRelatedFileBatchRequest
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
		res := s.applyConfigFileOperation(r, op)
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
