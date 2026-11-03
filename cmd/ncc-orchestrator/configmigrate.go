package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// This file converts a flat (legacy) config.yaml to the nested
// schema-version: 1 layout and tidies settings that are spelled twice. A
// migration never changes what the runner does: the settings the runner
// resolves from the file before and after must be identical, or nothing is
// written.

// preferredNestedPaths names the nested spelling to keep for the flat keys
// that have two (it matches examples/example_config.yaml and the UI).
var preferredNestedPaths = map[string]string{
	"flaky-lookback-runs":   "runner.policy.flaky-lookback-runs",
	"flaky-min-transitions": "runner.policy.flaky-min-transitions",
	"policy-gates":          "runner.policy.gates",
	"email-enabled":         "notifications.email.enabled",
	"smtp-server":           "notifications.email.smtp-server",
	"smtp-port":             "notifications.email.smtp-port",
	"webhook-url":           "notifications.webhook.url",
}

// configNestedPathsByFlat lists, for every flat key, its nested spellings with
// the preferred one first. When a file sets more than one, the first wins.
func configNestedPathsByFlat() map[string][]string {
	out := map[string][]string{}
	for nested, flat := range canonicalConfigMappings() {
		out[flat] = append(out[flat], nested)
	}
	for flat, paths := range out {
		pref := preferredNestedPaths[flat]
		sort.Slice(paths, func(i, j int) bool {
			if (paths[i] == pref) != (paths[j] == pref) {
				return paths[i] == pref
			}
			return paths[i] < paths[j]
		})
		out[flat] = paths
	}
	return out
}

// nestedConfigSections are the top-level keys of the nested layout. ui and
// deployment hold server notes the runner ignores.
func nestedConfigSections() map[string]bool {
	out := map[string]bool{"ui": true, "deployment": true}
	for nested := range canonicalConfigMappings() {
		out[strings.SplitN(nested, ".", 2)[0]] = true
	}
	return out
}

// configDuplicate is one setting the file spells more than once.
type configDuplicate struct {
	Flat     string   `json:"flat"`
	Paths    []string `json:"paths"`
	Winner   string   `json:"winner"`
	Value    string   `json:"value"`
	Shadowed []string `json:"shadowed_values,omitempty"`
	Differs  bool     `json:"differs"`
}

type configMigrationMove struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// configMigrationResult is what `migrate-config --json` prints and what the
// api-server shows under Updates & backups → Migrations.
type configMigrationResult struct {
	ConfigPath string                `json:"config_path"`
	Layout     string                `json:"layout"` // empty | flat | nested | mixed
	Changed    bool                  `json:"changed"`
	Applied    bool                  `json:"applied"`
	Moved      []configMigrationMove `json:"moved,omitempty"`
	Duplicates []configDuplicate     `json:"duplicates,omitempty"`
	Removed    []string              `json:"removed,omitempty"`
	KeptFlat   []string              `json:"kept_flat,omitempty"`
	Backup     string                `json:"backup,omitempty"`
	Content    string                `json:"content,omitempty"`
	Message    string                `json:"message"`
}

// settingString renders a YAML value the way the runner sees it: lists become
// comma-separated, as normalizeCanonicalConfig does.
func settingString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case []interface{}:
		items := make([]string, 0, len(t))
		for _, it := range t {
			items = append(items, fmt.Sprint(it))
		}
		return strings.Join(items, ",")
	case map[string]interface{}:
		b, _ := json.Marshal(t)
		return string(b)
	default:
		return fmt.Sprint(t)
	}
}

func lookupConfigPath(doc map[string]interface{}, path string) (interface{}, bool) {
	var cur interface{} = doc
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return nil, false
		}
		if cur, ok = m[seg]; !ok {
			return nil, false
		}
	}
	return cur, true
}

// effectiveConfigSettings is what the runner reads from a config file: each
// flat key (file value if present, else its first nested spelling), plus every
// other value in the file under its full path. Env vars and flags apply the
// same way to any layout, so they are left out.
func effectiveConfigSettings(raw []byte) (map[string]string, error) {
	var doc map[string]interface{}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	byFlat := configNestedPathsByFlat()
	mapped := map[string]bool{}
	for _, paths := range byFlat {
		for _, p := range paths {
			mapped[p] = true
		}
	}
	out := map[string]string{}
	sections := nestedConfigSections()
	var walk func(prefix string, v interface{})
	walk = func(prefix string, v interface{}) {
		if m, ok := v.(map[string]interface{}); ok && len(m) > 0 && !mapped[prefix] {
			for k, child := range m {
				walk(prefix+"."+k, child)
			}
			return
		}
		if !mapped[prefix] {
			out["@"+prefix] = settingString(v)
		}
	}
	for k, v := range doc {
		if k == "schema-version" {
			continue
		}
		if sections[k] {
			walk(k, v)
			continue
		}
		out[k] = settingString(v)
	}
	for flat, paths := range byFlat {
		if _, ok := doc[flat]; ok {
			continue
		}
		for _, p := range paths {
			if v, ok := lookupConfigPath(doc, p); ok {
				out[flat] = settingString(v)
				break
			}
		}
	}
	return out, nil
}

// findConfigDuplicates lists settings the file spells more than once and
// which spelling the runner uses.
func findConfigDuplicates(raw []byte) ([]configDuplicate, error) {
	var doc map[string]interface{}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	var out []configDuplicate
	for flat, paths := range configNestedPathsByFlat() {
		type spelled struct{ path, value string }
		var set []spelled
		if v, ok := doc[flat]; ok {
			set = append(set, spelled{flat, settingString(v)})
		}
		for _, p := range paths {
			if v, ok := lookupConfigPath(doc, p); ok {
				set = append(set, spelled{p, settingString(v)})
			}
		}
		if len(set) < 2 {
			continue
		}
		d := configDuplicate{Flat: flat, Winner: set[0].path, Value: set[0].value}
		for _, s := range set {
			d.Paths = append(d.Paths, s.path)
		}
		for _, s := range set[1:] {
			if s.value != set[0].value {
				d.Differs = true
				d.Shadowed = append(d.Shadowed, s.path+" = "+s.value)
			}
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Flat < out[j].Flat })
	return out, nil
}

func yamlMapGet(m *yaml.Node, key string) (int, *yaml.Node) {
	if m == nil || m.Kind != yaml.MappingNode {
		return -1, nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return i, m.Content[i+1]
		}
	}
	return -1, nil
}

func newYAMLMap() *yaml.Node { return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"} }

// yamlRemovePath deletes path under root; it also drops parent mappings left
// empty by the removal.
func yamlRemovePath(root *yaml.Node, segs []string) bool {
	i, child := yamlMapGet(root, segs[0])
	if child == nil {
		return false
	}
	if len(segs) == 1 {
		root.Content = append(root.Content[:i], root.Content[i+2:]...)
		return true
	}
	if !yamlRemovePath(child, segs[1:]) {
		return false
	}
	if child.Kind == yaml.MappingNode && len(child.Content) == 0 {
		root.Content = append(root.Content[:i], root.Content[i+2:]...)
	}
	return true
}

// yamlSetPath puts (key, val) at path under root, creating mappings on the
// way. key is the moved flat key node, renamed, so its comments travel too.
func yamlSetPath(root *yaml.Node, segs []string, key, val *yaml.Node) {
	cur := root
	for _, seg := range segs[:len(segs)-1] {
		_, child := yamlMapGet(cur, seg)
		if child == nil || child.Kind != yaml.MappingNode {
			child = newYAMLMap()
			cur.Content = append(cur.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: seg}, child)
		}
		cur = child
	}
	last := segs[len(segs)-1]
	key.Value = last
	if i, _ := yamlMapGet(cur, last); i >= 0 {
		cur.Content[i+1] = val
		return
	}
	cur.Content = append(cur.Content, key, val)
}

// migrateConfigToNested returns the nested-layout version of raw. Flat keys
// move to their preferred nested path, duplicate spellings collapse to the
// value the runner was using, and keys with no nested spelling stay at the top.
func migrateConfigToNested(raw []byte) ([]byte, configMigrationResult, error) {
	res := configMigrationResult{}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, res, fmt.Errorf("config is not valid YAML: %w", err)
	}
	if len(doc.Content) == 0 {
		res.Layout = "empty"
		res.Message = "the config file is empty"
		return raw, res, nil
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, res, errors.New("config must be a YAML mapping")
	}
	if _, v := yamlMapGet(root, "schema-version"); v != nil && strings.TrimSpace(v.Value) != "1" {
		return nil, res, fmt.Errorf("unsupported schema-version %q (supported: 1)", v.Value)
	}
	dups, err := findConfigDuplicates(raw)
	if err != nil {
		return nil, res, err
	}
	res.Duplicates = dups

	byFlat := configNestedPathsByFlat()
	sections := nestedConfigSections()
	hasFlat, hasNested := false, false
	for i := 0; i+1 < len(root.Content); i += 2 {
		k := root.Content[i].Value
		if _, ok := byFlat[k]; ok {
			hasFlat = true
		} else if sections[k] {
			hasNested = true
		} else if k != "schema-version" {
			res.KeptFlat = append(res.KeptFlat, k)
		}
	}
	switch {
	case hasFlat && hasNested:
		res.Layout = "mixed"
	case hasFlat:
		res.Layout = "flat"
	default:
		res.Layout = "nested"
	}

	// Collapse nested-only duplicates first: keep the spelling the runner uses.
	for _, d := range dups {
		if d.Winner == d.Flat {
			continue
		}
		for _, p := range d.Paths[1:] {
			if yamlRemovePath(root, strings.Split(p, ".")) {
				res.Removed = append(res.Removed, p)
			}
		}
	}

	// Move each flat key into place. A new section is inserted where the
	// first of its keys was, so the file keeps roughly its order.
	// The first key's head comment is usually the file's own header; it stays
	// at the top rather than moving with the key.
	header := ""
	if len(root.Content) > 0 {
		if _, isFlat := byFlat[root.Content[0].Value]; isFlat {
			header, root.Content[0].HeadComment = root.Content[0].HeadComment, ""
		}
	}
	built := make([]*yaml.Node, 0, len(root.Content))
	sectionNode := map[string]*yaml.Node{}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if sections[root.Content[i].Value] {
			sectionNode[root.Content[i].Value] = root.Content[i+1]
		}
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		k, v := root.Content[i], root.Content[i+1]
		paths, isFlat := byFlat[k.Value]
		if !isFlat {
			built = append(built, k, v)
			continue
		}
		flatName := k.Value
		// The flat value is the one in effect; other spellings were shadowed.
		for _, p := range paths {
			segs := strings.Split(p, ".")
			if sec := sectionNode[segs[0]]; sec != nil && len(segs) > 1 {
				if yamlRemovePath(sec, segs[1:]) {
					res.Removed = append(res.Removed, p)
				}
			}
		}
		target := strings.Split(paths[0], ".")
		sec := sectionNode[target[0]]
		if sec != nil && sec.Kind != yaml.MappingNode {
			if sec.Tag != "!!null" {
				return nil, res, fmt.Errorf("%s: must be a mapping to hold %s", target[0], paths[0])
			}
			sec.Kind, sec.Tag, sec.Value = yaml.MappingNode, "!!map", ""
		}
		if sec == nil {
			sec = newYAMLMap()
			sectionNode[target[0]] = sec
			built = append(built, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: target[0], HeadComment: k.HeadComment}, sec)
			k.HeadComment = ""
		}
		yamlSetPath(sec, target[1:], k, v)
		res.Moved = append(res.Moved, configMigrationMove{From: flatName, To: paths[0]})
	}
	// A section emptied by removing duplicates is dropped.
	root.Content = built[:0:0]
	for i := 0; i+1 < len(built); i += 2 {
		if sections[built[i].Value] && built[i+1].Kind == yaml.MappingNode && len(built[i+1].Content) == 0 {
			continue
		}
		root.Content = append(root.Content, built[i], built[i+1])
	}
	if i, _ := yamlMapGet(root, "schema-version"); i < 0 {
		root.Content = append([]*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "schema-version"},
			{Kind: yaml.ScalarNode, Tag: "!!int", Value: "1"},
		}, root.Content...)
		res.Changed = true
	}
	if header != "" && len(root.Content) > 0 {
		root.Content[0].HeadComment = strings.TrimSpace(header + "\n" + root.Content[0].HeadComment)
	}
	if len(res.Moved) > 0 || len(res.Removed) > 0 {
		res.Changed = true
	}
	if !res.Changed {
		res.Message = "already in the nested layout; nothing to change"
		return raw, res, nil
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return nil, res, err
	}
	_ = enc.Close()
	out := buf.Bytes()

	before, err := effectiveConfigSettings(raw)
	if err != nil {
		return nil, res, err
	}
	after, err := effectiveConfigSettings(out)
	if err != nil {
		return nil, res, err
	}
	if diff := diffSettings(before, after); diff != "" {
		return nil, res, fmt.Errorf("migration would change %s; nothing was written", diff)
	}
	res.Content = string(out)
	parts := []string{}
	if n := len(res.Moved); n > 0 {
		parts = append(parts, fmt.Sprintf("%d setting%s moved to the nested layout", n, plural(n)))
	}
	if n := len(res.Removed); n > 0 {
		parts = append(parts, fmt.Sprintf("%d duplicate spelling%s removed", n, plural(n)))
	}
	if len(parts) == 0 {
		parts = append(parts, "added schema-version: 1")
	}
	res.Message = strings.Join(parts, "; ")
	return out, res, nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func diffSettings(before, after map[string]string) string {
	keys := map[string]bool{}
	for k := range before {
		keys[k] = true
	}
	for k := range after {
		keys[k] = true
	}
	var changed []string
	for k := range keys {
		b, bok := before[k]
		a, aok := after[k]
		if bok != aok || a != b {
			changed = append(changed, fmt.Sprintf("%s (%q → %q)", strings.TrimPrefix(k, "@"), b, a))
		}
	}
	sort.Strings(changed)
	return strings.Join(changed, ", ")
}

// migrateConfigFile migrates path in place unless dryRun. The original is
// kept next to it as <name>.pre-migrate-<UTC>.bak.
func migrateConfigFile(path string, dryRun bool) (configMigrationResult, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return configMigrationResult{ConfigPath: path}, err
	}
	out, res, err := migrateConfigToNested(raw)
	res.ConfigPath = path
	if err != nil || !res.Changed || dryRun {
		return res, err
	}
	mode := os.FileMode(0o600)
	if st, serr := os.Stat(path); serr == nil {
		mode = st.Mode().Perm()
	}
	backup := fmt.Sprintf("%s.pre-migrate-%s.bak", path, time.Now().UTC().Format("20060102T150405Z"))
	if err := os.WriteFile(backup, raw, 0o600); err != nil {
		return res, fmt.Errorf("could not save a backup of the config: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".ncc-migrate-*")
	if err != nil {
		return res, err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(out); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return res, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return res, err
	}
	_ = os.Chmod(tmpName, mode)
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return res, err
	}
	res.Applied = true
	res.Backup = backup
	return res, nil
}
