package main

// Run environments (dev, staging, prod, ...).
//
// The main config file (s.configPath) owns everything the services depend on:
// storage, logging, notifications, the API and UI, secrets. An environment is a
// small file under configs/environments/<name>.yaml that holds only `runner:`
// settings (targets, credentials, connection, execution, filtering ...). A run
// against an environment uses the main config with the environment's runner
// settings laid over it, so choosing an environment can never move results,
// change notifications or reconfigure the API server.
//
// The merged file is written to configs/environments/.effective/<name>.yaml
// before each use (runs, preflight, scheduled runs) and is never edited by hand.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

const (
	environmentsRelDir   = "configs/environments"
	effectiveConfigDir   = ".effective"
	maxEnvironmentFile   = 1 << 20
	environmentListLimit = 200
)

var reEnvironmentName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)

// Top-level keys an environment file may contain.
var environmentTopLevelKeys = map[string]bool{"schema-version": true, "description": true, "runner": true}

type environmentInfo struct {
	Name          string   `json:"name"`
	Description   string   `json:"description,omitempty"`
	Path          string   `json:"path"`
	Mode          string   `json:"mode,omitempty"`
	Clusters      []string `json:"clusters,omitempty"`
	ClusterCount  int      `json:"cluster_count"`
	PrismCentrals []string `json:"prism_centrals,omitempty"`
	UpdatedAt     string   `json:"updated_at,omitempty"`
	Error         string   `json:"error,omitempty"`
}

func (s *apiServer) environmentsDir() string {
	return filepath.Join(s.absPath(s.repoRoot), filepath.FromSlash(environmentsRelDir))
}

func validEnvironmentName(name string) error {
	if !reEnvironmentName.MatchString(name) {
		return errors.New("environment names use lower-case letters, digits and dashes (up to 40), e.g. prod or staging-eu")
	}
	return nil
}

func (s *apiServer) environmentFile(name string) (string, error) {
	name = strings.TrimSpace(name)
	if err := validEnvironmentName(name); err != nil {
		return "", err
	}
	return filepath.Join(s.environmentsDir(), name+".yaml"), nil
}

// environmentNameForPath returns the environment a config path names, if the
// path is an environment file.
func (s *apiServer) environmentNameForPath(abs string) (string, bool) {
	// Compare resolved directories: callers pass paths built from the repo
	// root after EvalSymlinks (normalizeAndConfinePath), which differs from
	// the configured root under a symlink (macOS /tmp) or an 8.3 short name
	// (C:\Users\RUNNER~1 on Windows).
	dir := resolvedDir(s.environmentsDir())
	if !strings.EqualFold(resolvedDir(filepath.Dir(abs)), dir) {
		return "", false
	}
	base := filepath.Base(abs)
	ext := filepath.Ext(base)
	if !strings.EqualFold(ext, ".yaml") && !strings.EqualFold(ext, ".yml") {
		return "", false
	}
	name := strings.TrimSuffix(base, ext)
	if validEnvironmentName(name) != nil {
		return "", false
	}
	return name, true
}

// resolvedDir cleans dir and resolves symlinks and short names when it exists.
func resolvedDir(dir string) string {
	dir = filepath.Clean(dir)
	if real, err := filepath.EvalSymlinks(dir); err == nil {
		return filepath.Clean(real)
	}
	return dir
}

// parseEnvironment checks that content is a runner-only environment file and
// returns its parsed top level.
func parseEnvironment(content []byte) (map[string]interface{}, error) {
	if len(content) > maxEnvironmentFile {
		return nil, errors.New("environment file is too large")
	}
	doc := map[string]interface{}{}
	if err := yaml.Unmarshal(content, &doc); err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if doc == nil {
		doc = map[string]interface{}{}
	}
	extra := []string{}
	for k := range doc {
		if !environmentTopLevelKeys[k] {
			extra = append(extra, k)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return nil, fmt.Errorf("an environment holds only runner settings; move %s to the main config", strings.Join(extra, ", "))
	}
	if r, ok := doc["runner"]; ok && r != nil {
		if _, isMap := r.(map[string]interface{}); !isMap {
			return nil, errors.New("runner must be a mapping of settings")
		}
	}
	if d, ok := doc["description"]; ok && d != nil {
		if _, isStr := d.(string); !isStr {
			return nil, errors.New("description must be text")
		}
	}
	return doc, nil
}

func asStringMap(v interface{}) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return map[string]interface{}{}
}

// stringList reads a YAML list or comma-separated string.
func stringList(v interface{}) []string {
	out := []string{}
	switch t := v.(type) {
	case []interface{}:
		for _, x := range t {
			if s := strings.TrimSpace(fmt.Sprint(x)); s != "" && s != "<nil>" {
				out = append(out, s)
			}
		}
	case string:
		for _, x := range strings.Split(t, ",") {
			if s := strings.TrimSpace(x); s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

// readListFile reads one entry per line, skipping blanks and # comments.
func (s *apiServer) readListFile(p string) []string {
	p = strings.TrimSpace(p)
	if p == "" {
		return nil
	}
	if !filepath.IsAbs(p) {
		p = filepath.Join(s.absPath(s.repoRoot), p)
	}
	abs, err := s.normalizeAndConfinePath(p)
	if err != nil {
		return nil
	}
	f, err := os.Open(abs)
	if err != nil {
		return nil
	}
	defer f.Close()
	out := []string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if i := strings.Index(line, "#"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		for _, part := range strings.Split(line, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

// environmentTargets lists what an environment's runner settings point at.
func (s *apiServer) environmentTargets(runner map[string]interface{}) (mode string, clusters []string, pcs []string) {
	t := asStringMap(runner["targets"])
	mode = strings.TrimSpace(fmt.Sprint(t["mode"]))
	if mode == "<nil>" {
		mode = ""
	}
	seen := map[string]bool{}
	add := func(list []string) {
		for _, c := range list {
			if n := normClusterName(c); n != "" && !seen[n] {
				seen[n] = true
				clusters = append(clusters, c)
			}
		}
	}
	add(stringList(t["clusters"]))
	if f, ok := t["clusters-file"].(string); ok {
		add(s.readListFile(f))
	}
	pcs = stringList(t["pcs"])
	if f, ok := t["pcs-file"].(string); ok {
		pcs = append(pcs, s.readListFile(f)...)
	}
	if u, ok := t["prism-central-url"].(string); ok && strings.TrimSpace(u) != "" {
		pcs = append(pcs, strings.TrimSpace(u))
	}
	return mode, clusters, pcs
}

func (s *apiServer) environmentInfoFor(name, abs string) environmentInfo {
	info := environmentInfo{Name: name, Path: s.displayRelPath(abs)}
	st, err := os.Stat(abs)
	if err != nil {
		info.Error = "file not found"
		return info
	}
	info.UpdatedAt = st.ModTime().UTC().Format(time.RFC3339)
	b, err := os.ReadFile(abs)
	if err != nil {
		info.Error = err.Error()
		return info
	}
	doc, err := parseEnvironment(b)
	if err != nil {
		info.Error = err.Error()
		return info
	}
	if d, ok := doc["description"].(string); ok {
		info.Description = strings.TrimSpace(d)
	}
	mode, clusters, pcs := s.environmentTargets(asStringMap(doc["runner"]))
	info.Mode = mode
	info.ClusterCount = len(clusters)
	info.Clusters = clusters
	info.PrismCentrals = pcs
	return info
}

func (s *apiServer) listEnvironments() []environmentInfo {
	out := []environmentInfo{}
	entries, err := os.ReadDir(s.environmentsDir())
	if err != nil {
		return out
	}
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		abs := filepath.Join(s.environmentsDir(), e.Name())
		name, ok := s.environmentNameForPath(abs)
		if !ok {
			continue
		}
		out = append(out, s.environmentInfoFor(name, abs))
		if len(out) >= environmentListLimit {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *apiServer) environmentExists(name string) bool {
	p, err := s.environmentFile(name)
	if err != nil {
		return false
	}
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}

// deepMerge returns base with over laid on top: maps merge key by key, any
// other value (lists included) in over replaces the base value.
func deepMerge(base, over map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(base)+len(over))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range over {
		bm, bok := out[k].(map[string]interface{})
		om, ook := v.(map[string]interface{})
		if bok && ook {
			out[k] = deepMerge(bm, om)
			continue
		}
		out[k] = v
	}
	return out
}

// mergedEnvironmentConfig lays an environment's runner settings over the main
// config and returns the YAML to run with.
func (s *apiServer) mergedEnvironmentConfig(envContent []byte) ([]byte, error) {
	env, err := parseEnvironment(envContent)
	if err != nil {
		return nil, err
	}
	basePath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return nil, err
	}
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		return nil, fmt.Errorf("read main config: %w", err)
	}
	base := map[string]interface{}{}
	if err := yaml.Unmarshal(baseBytes, &base); err != nil {
		return nil, fmt.Errorf("main config is not valid YAML: %w", err)
	}
	if base == nil {
		base = map[string]interface{}{}
	}
	if _, nested := base["runner"]; !nested {
		if _, versioned := base["schema-version"]; !versioned && len(base) > 0 {
			return nil, errors.New("the main config uses the older flat layout; migrate it (Updates & backups → Migrations) before using environments")
		}
	}
	merged := deepMerge(base, map[string]interface{}{})
	merged["runner"] = deepMerge(asStringMap(base["runner"]), asStringMap(env["runner"]))
	if _, ok := merged["schema-version"]; !ok {
		merged["schema-version"] = 1
	}
	var buf bytes.Buffer
	buf.WriteString("# Generated by the NCC Orchestrator API server: the main config with an\n# environment's runner settings laid over it. Do not edit; it is rewritten\n# before every run.\n")
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(merged); err != nil {
		return nil, err
	}
	_ = enc.Close()
	return buf.Bytes(), nil
}

// materializeEnvironment writes the merged config for an environment and
// returns its absolute path.
func (s *apiServer) materializeEnvironment(name string) (string, error) {
	src, err := s.environmentFile(name)
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(src)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("environment %q does not exist", name)
		}
		return "", err
	}
	merged, err := s.mergedEnvironmentConfig(b)
	if err != nil {
		return "", fmt.Errorf("environment %q: %w", name, err)
	}
	dir := filepath.Join(s.environmentsDir(), effectiveConfigDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	target := filepath.Join(dir, name+".yaml")
	tmp, err := os.CreateTemp(dir, name+".*.tmp.yaml")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(merged); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return "", err
	}
	tmp.Close()
	_ = os.Chmod(tmpName, 0o600)
	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return "", err
	}
	return target, nil
}

// runConfigChoice is what a run (or preflight) executes with.
type runConfigChoice struct {
	path        string // absolute config path passed to the orchestrator
	environment string // "" for the main config
	clusters    []string
	mode        string
}

// resolveRunConfig picks the config for a run: a named environment, else an
// explicit config_path (the main config or an environment file), else the
// caller's saved preference, else the main config. Any other config file is
// refused: it could carry its own storage, notification or server settings.
func (s *apiServer) resolveRunConfig(r *http.Request, environment, configPath string) (runConfigChoice, error) {
	environment = strings.TrimSpace(environment)
	configPath = strings.TrimSpace(configPath)
	fromPreference := false
	if environment == "" && configPath == "" && r != nil {
		if p, ok := principalFromContext(r.Context()); ok && s.users != nil {
			configPath = strings.TrimSpace(s.users.getRunConfigPath(p.subject))
			if strings.HasPrefix(configPath, "env:") {
				environment, configPath = strings.TrimPrefix(configPath, "env:"), ""
			}
			fromPreference = environment != "" || configPath != ""
		}
	}
	if fromPreference {
		// A saved preference that no longer resolves (a file from before
		// environments, or a deleted environment) falls back to the main config.
		if choice, err := s.resolveRunConfig(nil, environment, configPath); err == nil {
			return choice, nil
		}
		return s.resolveRunConfig(nil, "", "")
	}
	if environment == "" && configPath != "" {
		abs, err := s.validateConfigPath(configPath)
		if err != nil {
			return runConfigChoice{}, err
		}
		mainAbs, _ := s.validateConfigPath(s.configPath)
		if strings.EqualFold(filepath.Clean(abs), filepath.Clean(mainAbs)) {
			return runConfigChoice{path: mainAbs}, nil
		}
		name, ok := s.environmentNameForPath(abs)
		if !ok {
			return runConfigChoice{}, fmt.Errorf("runs use the main config or an environment; %s is neither. Put its runner settings in %s/<name>.yaml", configPath, environmentsRelDir)
		}
		environment = name
	}
	if environment == "" {
		mainAbs, err := s.validateConfigPath(s.configPath)
		if err != nil {
			return runConfigChoice{}, err
		}
		return runConfigChoice{path: mainAbs}, nil
	}
	path, err := s.materializeEnvironment(environment)
	if err != nil {
		return runConfigChoice{}, err
	}
	src, _ := s.environmentFile(environment)
	info := s.environmentInfoFor(environment, src)
	return runConfigChoice{path: path, environment: environment, clusters: info.Clusters, mode: info.Mode}, nil
}

// confineToEnvironment narrows a restricted caller's clusters to those the
// environment lists. Environments that discover clusters from Prism Central
// have no fixed list, so nothing is narrowed.
func confineToEnvironment(scoped []string, choice runConfigChoice, unrestricted bool) ([]string, error) {
	if unrestricted || choice.environment == "" || len(choice.clusters) == 0 || len(scoped) == 0 {
		return scoped, nil
	}
	in := map[string]bool{}
	for _, c := range choice.clusters {
		in[canonicalClusterKey(c)] = true
	}
	out := []string{}
	for _, c := range scoped {
		if in[canonicalClusterKey(c)] {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("none of your clusters are in the %s environment", choice.environment)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// HTTP handlers.
// ---------------------------------------------------------------------------

// handleEnvironments lists environments (GET, any signed-in user). Content is
// admin-only, under /api/v1/settings/environments/.
func (s *apiServer) handleEnvironments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	items := s.listEnvironments()
	p, _ := principalFromContext(r.Context())
	access := s.allowedClusters(p)
	if !access.unrestricted {
		// Group members see only their own clusters in each environment.
		for i := range items {
			mine := []string{}
			for _, c := range items[i].Clusters {
				if access.permits(c) {
					mine = append(mine, c)
				}
			}
			items[i].Clusters = mine
			items[i].ClusterCount = len(mine)
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"items":       items,
		"main_config": s.displayRelPath(s.configPath),
		"dir":         environmentsRelDir,
	}})
}

type environmentSaveRequest struct {
	Content string `json:"content"`
	// Create refuses to overwrite an existing environment.
	Create bool `json:"create,omitempty"`
}

// handleEnvironmentSettings serves /api/v1/settings/environments/<name>
// (GET content, PUT save, DELETE) and /api/v1/settings/environments/import.
func (s *apiServer) handleEnvironmentSettings(w http.ResponseWriter, r *http.Request) {
	name := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/settings/environments"), "/")
	if name == "import" {
		s.handleEnvironmentImport(w, r)
		return
	}
	if name == "" {
		s.handleEnvironments(w, r)
		return
	}
	path, err := s.environmentFile(name)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	switch r.Method {
	case http.MethodGet:
		b, err := os.ReadFile(path)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, os.ErrNotExist) {
				status = http.StatusNotFound
				err = fmt.Errorf("environment %q does not exist", name)
			}
			writeJSON(w, status, envelope{Success: false, Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"name":    name,
			"path":    s.displayRelPath(path),
			"content": string(b),
			"info":    s.environmentInfoFor(name, path),
		}})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req environmentSaveRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if req.Create && s.environmentExists(name) {
			writeJSON(w, http.StatusConflict, envelope{Success: false, Error: fmt.Sprintf("environment %q already exists", name)})
			return
		}
		if out, err := s.saveEnvironment(name, []byte(req.Content)); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error(), Data: map[string]string{"output": out}})
			return
		}
		s.audit(r, "settings.environment.update", true, map[string]interface{}{"environment": name, "created": req.Create})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "environment saved", Data: map[string]interface{}{
			"info": s.environmentInfoFor(name, path),
		}})
	case http.MethodDelete:
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		_ = os.Remove(filepath.Join(s.environmentsDir(), effectiveConfigDir, name+".yaml"))
		s.removeEnvironmentSchedules(name)
		s.audit(r, "settings.environment.delete", true, map[string]interface{}{"environment": name})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "environment deleted"})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// saveEnvironment validates content (runner-only, and valid once laid over the
// main config) and writes it. It returns validator output on failure.
func (s *apiServer) saveEnvironment(name string, content []byte) (string, error) {
	path, err := s.environmentFile(name)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(content)) == "" {
		return "", errors.New("content is required")
	}
	merged, err := s.mergedEnvironmentConfig(content)
	if err != nil {
		return "", err
	}
	dir := s.environmentsDir()
	if err := os.MkdirAll(filepath.Join(dir, effectiveConfigDir), 0o700); err != nil {
		return "", err
	}
	check, err := os.CreateTemp(filepath.Join(dir, effectiveConfigDir), "validate-*.yaml")
	if err != nil {
		return "", err
	}
	checkName := check.Name()
	defer os.Remove(checkName)
	_, _ = check.Write(merged)
	check.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := s.makeOrchestratorCommand(ctx, "validate-config", "--config", checkName)
	cmd.Dir = s.absPath(s.repoRoot)
	if out, err := cmd.CombinedOutput(); err != nil {
		return redactSensitiveText(string(out)), fmt.Errorf("these settings don't validate with the main config: %v", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, content, 0o600); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return "", nil
}

// handleEnvironmentImport turns an older full config file into an environment
// by keeping only its runner settings: POST {"path": "...", "name": "..."}.
func (s *apiServer) handleEnvironmentImport(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"items": s.importableConfigFiles()}})
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	var req struct {
		Path string `json:"path"`
		Name string `json:"name"`
	}
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	name := strings.TrimSpace(req.Name)
	if err := validEnvironmentName(name); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if s.environmentExists(name) {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: fmt.Sprintf("environment %q already exists", name)})
		return
	}
	src, err := s.validateConfigPath(req.Path)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	b, err := os.ReadFile(src)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	full := map[string]interface{}{}
	if err := yaml.Unmarshal(b, &full); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "not valid YAML: " + err.Error()})
		return
	}
	runner, ok := full["runner"].(map[string]interface{})
	if !ok {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "this file has no runner section (older flat layout); migrate it first or create the environment by hand"})
		return
	}
	env := map[string]interface{}{
		"schema-version": 1,
		"description":    "Imported from " + s.displayRelPath(src),
		"runner":         runner,
	}
	content, err := yaml.Marshal(env)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	if out, err := s.saveEnvironment(name, content); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error(), Data: map[string]string{"output": out}})
		return
	}
	dropped := []string{}
	for k := range full {
		if !environmentTopLevelKeys[k] {
			dropped = append(dropped, k)
		}
	}
	sort.Strings(dropped)
	s.audit(r, "settings.environment.import", true, map[string]interface{}{"environment": name, "source": s.displayRelPath(src)})
	path, _ := s.environmentFile(name)
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "environment created", Data: map[string]interface{}{
		"info":    s.environmentInfoFor(name, path),
		"dropped": dropped,
	}})
}

// importableConfigFiles lists YAML files under configs/ (outside the
// environments folder) and beside the main config that have a runner section:
// the older per-cluster config files.
func (s *apiServer) importableConfigFiles() []string {
	mainAbs, _ := s.validateConfigPath(s.configPath)
	root := s.absPath(s.repoRoot)
	envDir := filepath.Clean(s.environmentsDir())
	seen := map[string]bool{}
	out := []string{}
	consider := func(p string) {
		abs, err := s.validateConfigPath(p)
		if err != nil || seen[abs] || strings.EqualFold(abs, mainAbs) {
			return
		}
		if strings.HasPrefix(strings.ToLower(abs), strings.ToLower(envDir)+string(filepath.Separator)) {
			return
		}
		seen[abs] = true
		b, err := os.ReadFile(abs)
		if err != nil || len(b) > maxEnvironmentFile {
			return
		}
		doc := map[string]interface{}{}
		if yaml.Unmarshal(b, &doc) != nil {
			return
		}
		if _, ok := doc["runner"].(map[string]interface{}); ok {
			out = append(out, s.displayRelPath(abs))
		}
	}
	_ = filepath.WalkDir(filepath.Join(root, "configs"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if strings.EqualFold(filepath.Clean(path), envDir) {
				return filepath.SkipDir
			}
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".yaml" || ext == ".yml" {
			consider(path)
		}
		return nil
	})
	if mainAbs != "" {
		if entries, err := os.ReadDir(filepath.Dir(mainAbs)); err == nil {
			for _, e := range entries {
				ext := strings.ToLower(filepath.Ext(e.Name()))
				if !e.IsDir() && (ext == ".yaml" || ext == ".yml") {
					consider(filepath.Join(filepath.Dir(mainAbs), e.Name()))
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// environmentForRunDir reads the environment recorded in an archived run.
func environmentForRunDir(runDir string) string {
	b, err := os.ReadFile(filepath.Join(runDir, "run-meta.json"))
	if err != nil {
		return ""
	}
	var meta struct {
		Environment string `json:"environment"`
	}
	if json.Unmarshal(b, &meta) != nil {
		return ""
	}
	return meta.Environment
}
