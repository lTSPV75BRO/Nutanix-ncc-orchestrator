package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"
	"go.yaml.in/yaml/v3"

	"goncc/internal/plural"
)

// Isolated backup verification (`v2-restore --verify-only`). The archive is
// read as a stream and checked in memory: nothing is extracted and nothing is
// written into an install dir. An encrypted archive is decrypted to a private
// temp file (as restore does) that is always removed before returning.

// Check statuses.
const (
	checkPass = "pass"
	checkWarn = "warn"
	checkFail = "fail"
)

// backupCheck is one line of the verification report.
type backupCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // pass | warn | fail
	Message string `json:"message"`
}

// backupInspectReport is the full verification result (the --json output and
// what the API returns to the UI). It never carries secret values.
type backupInspectReport struct {
	Archive        string        `json:"archive"`
	Encrypted      bool          `json:"encrypted"`
	BackupVersion  string        `json:"backup_version,omitempty"`
	RunningVersion string        `json:"running_version"`
	CreatedAt      string        `json:"created_at,omitempty"`
	Files          int           `json:"files"`
	Bytes          int64         `json:"bytes"`
	Secrets        []string      `json:"secrets"` // names only
	Checks         []backupCheck `json:"checks"`
	OK             bool          `json:"ok"` // no check failed
}

func (r *backupInspectReport) add(name, status, format string, args ...interface{}) {
	r.Checks = append(r.Checks, backupCheck{Name: name, Status: status, Message: fmt.Sprintf(format, args...)})
}

// Files kept in memory for parsing: small config/state files and secret files.
const (
	maxInspectParseBytes = 4 << 20
	maxInspectHeldBytes  = 64 << 20
)

// inspectBackupArchive runs every verification check on the archive at in.
func inspectBackupArchive(in, keyFile, passphrase, runningVersion string) backupInspectReport {
	rep := backupInspectReport{Archive: filepath.Base(in), RunningVersion: runningVersion, Secrets: []string{}}

	// 1. Decryption.
	enc, err := backupArchiveIsEncrypted(in)
	if err != nil {
		rep.add("Archive", checkFail, "Cannot open the archive: %v", err)
		return rep.finish()
	}
	rep.Encrypted = enc
	tarPath := in
	if enc {
		dec, derr := decryptBackupArchive(in, keyFile, passphrase)
		if derr != nil {
			rep.add("Decryption", checkFail, "Cannot decrypt: %v", derr)
			return rep.finish()
		}
		defer os.Remove(dec)
		tarPath = dec
		rep.add("Decryption", checkPass, "Encrypted (AES-256-GCM) and decrypted with the provided key.")
	} else {
		rep.add("Decryption", checkPass, "Not encrypted. The archive holds secrets in plain text, so store it somewhere safe.")
	}

	// 2. Read every entry: integrity, layout, sizes, checksums.
	sc := scanBackupTar(tarPath)
	if sc.readErr != nil {
		rep.add("Archive", checkFail, "The archive is damaged: %v", sc.readErr)
		return rep.finish()
	}
	rep.Files = len(sc.dataOrder)
	rep.Bytes = sc.total
	rep.add("Archive", checkPass, "Every entry is readable: %s (%s uncompressed).", plural.Count(len(sc.dataOrder), "file"), humanBytes(uint64(sc.total)))

	if len(sc.pathIssues) > 0 {
		rep.add("Paths", checkFail, "%s", summarizeIssues(sc.pathIssues))
	} else if len(sc.dataOrder) == 0 {
		rep.add("Paths", checkFail, "The archive contains no data files to restore.")
	} else {
		rep.add("Paths", checkPass, "All paths stay inside the install dir (manifest.json plus data/).")
	}

	// 3. Manifest.
	if sc.manifestRaw == nil {
		rep.add("Manifest", checkFail, "The archive has no manifest.json (not an ncc backup?).")
		return rep.finish()
	}
	var m backupManifest
	if err := json.Unmarshal(sc.manifestRaw, &m); err != nil {
		rep.add("Manifest", checkFail, "manifest.json is not valid JSON: %v", err)
		return rep.finish()
	}
	rep.BackupVersion = m.Version
	rep.CreatedAt = m.CreatedAt
	if m.Tool != "" && m.Tool != "ncc-orchestrator" {
		rep.add("Manifest", checkFail, "manifest.json was written by %q, not ncc-orchestrator.", m.Tool)
	} else {
		rep.add("Manifest", checkPass, "Created %s by ncc-orchestrator %s.", defaultStr(m.CreatedAt, "at an unknown time"), defaultStr(m.Version, "(unknown version)"))
	}

	// 4. Contents match the manifest (file list and checksums).
	rep.addContentsCheck(m, sc)

	// 5. Config and state files parse.
	cfg, cfgRel := rep.addConfigCheck(sc)

	// 6. Secrets (names only).
	rep.addSecretsCheck(sc, cfg, m)

	// 7. Paths the config refers to.
	if cfg != nil {
		rep.addReferencesCheck(sc, cfg, cfgRel, m)
	}

	// 8. Version compatibility.
	rep.addVersionCheck(m.Version, runningVersion)
	return rep.finish()
}

func (r backupInspectReport) finish() backupInspectReport {
	r.OK = true
	for _, c := range r.Checks {
		if c.Status == checkFail {
			r.OK = false
		}
	}
	return r
}

func summarizeIssues(issues []string) string {
	const show = 3
	if len(issues) <= show {
		return strings.Join(issues, "; ")
	}
	return strings.Join(issues[:show], "; ") + fmt.Sprintf("; and %d more", len(issues)-show)
}

// backupTarScan is what one streaming pass over the tar learned.
type backupTarScan struct {
	readErr     error
	manifestRaw []byte
	dataOrder   []string          // archive-relative paths (slash form, below data/)
	sums        map[string]string // rel -> sha256 hex
	held        map[string][]byte // small files kept for parsing
	total       int64
	pathIssues  []string
}

func scanBackupTar(tarPath string) backupTarScan {
	sc := backupTarScan{sums: map[string]string{}, held: map[string][]byte{}}
	f, err := os.Open(tarPath)
	if err != nil {
		sc.readErr = err
		return sc
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		sc.readErr = fmt.Errorf("not a gzip archive: %w", err)
		return sc
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var held int64
	sep := string(filepath.Separator)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			sc.readErr = fmt.Errorf("corrupt tar stream: %w", err)
			return sc
		}
		name := hdr.Name
		if hdr.Typeflag == tar.TypeDir {
			if name != "data/" && !strings.HasPrefix(name, "data/") {
				sc.pathIssues = append(sc.pathIssues, fmt.Sprintf("unexpected entry %q outside data/", name))
			}
			continue
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != '\x00' {
			sc.pathIssues = append(sc.pathIssues, fmt.Sprintf("%q is not a regular file (links and devices are not allowed)", name))
			continue
		}
		if name == "manifest.json" {
			if sc.manifestRaw != nil {
				sc.pathIssues = append(sc.pathIssues, "manifest.json appears twice")
				continue
			}
			data, err := io.ReadAll(io.LimitReader(tr, 1<<20))
			if err != nil {
				sc.readErr = fmt.Errorf("read manifest.json: %w", err)
				return sc
			}
			sc.manifestRaw = data
			continue
		}
		if !strings.HasPrefix(name, "data/") {
			sc.pathIssues = append(sc.pathIssues, fmt.Sprintf("unexpected entry %q outside data/", name))
			continue
		}
		rel := strings.TrimPrefix(name, "data/")
		clean := filepath.Clean(filepath.FromSlash(rel))
		if rel == "" || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+sep) || filepath.IsAbs(clean) || strings.HasPrefix(rel, "/") || strings.Contains(rel, ":") {
			sc.pathIssues = append(sc.pathIssues, fmt.Sprintf("unsafe path %q", name))
			continue
		}
		rel = filepath.ToSlash(clean)
		if _, dup := sc.sums[rel]; dup {
			sc.pathIssues = append(sc.pathIssues, fmt.Sprintf("%q appears twice", name))
			continue
		}
		if len(sc.dataOrder) >= maxBackupFileCount {
			sc.readErr = fmt.Errorf("too many files (>%d)", maxBackupFileCount)
			return sc
		}
		h := sha256.New()
		keep := hdr.Size <= maxInspectParseBytes && held+hdr.Size <= maxInspectHeldBytes && inspectWantsBody(rel)
		var body strings.Builder
		var w io.Writer = h
		if keep {
			w = io.MultiWriter(h, &body)
		}
		n, err := io.Copy(w, io.LimitReader(tr, maxBackupFileBytes+1))
		if err != nil {
			sc.readErr = fmt.Errorf("read %s: %w", name, err)
			return sc
		}
		if n > maxBackupFileBytes {
			sc.readErr = fmt.Errorf("%q exceeds the per-file limit (%d bytes)", name, maxBackupFileBytes)
			return sc
		}
		sc.total += n
		if sc.total > maxBackupTotalBytes {
			sc.readErr = fmt.Errorf("the archive exceeds the total size limit (%d bytes)", maxBackupTotalBytes)
			return sc
		}
		sc.dataOrder = append(sc.dataOrder, rel)
		sc.sums[rel] = hex.EncodeToString(h.Sum(nil))
		if keep {
			sc.held[rel] = []byte(body.String())
			held += n
		}
	}
	return sc
}

// inspectWantsBody reports whether a data file is kept in memory for parsing:
// config and state files (YAML/JSON) and anything holding secrets.
func inspectWantsBody(rel string) bool {
	switch strings.ToLower(path.Ext(rel)) {
	case ".yaml", ".yml", ".json":
		return true
	}
	return sensitiveBackupName(rel)
}

func (r *backupInspectReport) addContentsCheck(m backupManifest, sc backupTarScan) {
	var issues []string
	if len(m.Files) > 0 {
		listed := make(map[string]bool, len(m.Files))
		for _, f := range m.Files {
			listed[path.Clean(filepath.ToSlash(f))] = true
		}
		for _, rel := range sc.dataOrder {
			if !listed[rel] {
				issues = append(issues, fmt.Sprintf("data/%s is not listed in the manifest", rel))
			}
		}
		for f := range listed {
			if _, ok := sc.sums[f]; !ok {
				issues = append(issues, fmt.Sprintf("data/%s is listed in the manifest but missing", f))
			}
		}
	}
	for rel, want := range m.Checksums {
		got, ok := sc.sums[path.Clean(filepath.ToSlash(rel))]
		if ok && !strings.EqualFold(got, want) {
			issues = append(issues, fmt.Sprintf("data/%s was changed after the backup was made (checksum mismatch)", rel))
		}
	}
	sort.Strings(issues)
	switch {
	case len(issues) > 0:
		r.add("Contents", checkFail, "%s", summarizeIssues(issues))
	case len(m.Checksums) == 0:
		r.add("Contents", checkWarn, "This backup has no checksums (made before they were added), so file contents were not compared.")
	default:
		r.add("Contents", checkPass, "Every file matches the manifest and its checksum.")
	}
}

// addConfigCheck parses every held YAML/JSON file and returns the parsed
// config.yaml (nil when absent or unparseable) with its archive path.
func (r *backupInspectReport) addConfigCheck(sc backupTarScan) (map[string]interface{}, string) {
	var issues []string
	parsed := 0
	rels := make([]string, 0, len(sc.held))
	for rel := range sc.held {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	for _, rel := range rels {
		body := sc.held[rel]
		switch strings.ToLower(path.Ext(rel)) {
		case ".yaml", ".yml":
			var v interface{}
			if err := yaml.Unmarshal(body, &v); err != nil {
				issues = append(issues, fmt.Sprintf("%s is not valid YAML: %s", rel, firstLine(err.Error())))
				continue
			}
			parsed++
		case ".json":
			if !json.Valid(body) {
				issues = append(issues, fmt.Sprintf("%s is not valid JSON", rel))
				continue
			}
			parsed++
		}
	}
	cfgRel := ""
	for _, c := range []string{"config.yaml", "config/config.yaml"} {
		if _, ok := sc.held[c]; ok {
			cfgRel = c
			break
		}
	}
	var cfg map[string]interface{}
	if cfgRel != "" {
		if err := yaml.Unmarshal(sc.held[cfgRel], &cfg); err != nil {
			cfg = nil
		}
	}
	switch {
	case len(issues) > 0:
		r.add("Config files", checkFail, "%s", summarizeIssues(issues))
	case cfgRel == "":
		r.add("Config files", checkWarn, "The backup has no config.yaml; a restore keeps the current config. %s parsed.", plural.Count(parsed, "file"))
	default:
		r.add("Config files", checkPass, "%s parsed, including %s.", plural.Count(parsed, "config and state file"), cfgRel)
	}
	return cfg, cfgRel
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func cfgString(cfg map[string]interface{}, key string) string {
	if cfg == nil {
		return ""
	}
	v, ok := cfg[key]
	if !ok || v == nil {
		return ""
	}
	s := strings.TrimSpace(fmt.Sprint(v))
	if s == "~" || strings.EqualFold(s, "null") {
		return ""
	}
	return s
}

// secretRefs collects the names behind every secret://name value in cfg.
func secretRefs(v interface{}, out map[string]bool) {
	switch t := v.(type) {
	case map[string]interface{}:
		for _, x := range t {
			secretRefs(x, out)
		}
	case []interface{}:
		for _, x := range t {
			secretRefs(x, out)
		}
	case string:
		if s := strings.TrimSpace(t); strings.HasPrefix(s, "secret://") {
			if n := strings.TrimSpace(strings.TrimPrefix(s, "secret://")); n != "" {
				out[n] = true
			}
		}
	}
}

// archiveRelForConfigPath maps a path written in config.yaml to its location
// in the archive: relative paths are relative to the install dir; absolute
// ones must sit under the backup's original install dir. ok is false when the
// path lies outside it (such files are never archived).
func archiveRelForConfigPath(raw, installDir string) (string, bool) {
	p := strings.ReplaceAll(strings.TrimSpace(raw), `\`, "/")
	abs := strings.HasPrefix(p, "/") || regexp.MustCompile(`^[A-Za-z]:/`).MatchString(p)
	if !abs {
		c := path.Clean(p)
		if c == ".." || strings.HasPrefix(c, "../") {
			return "", false
		}
		return c, true
	}
	base := strings.TrimRight(strings.ReplaceAll(strings.TrimSpace(installDir), `\`, "/"), "/")
	if base == "" || len(p) <= len(base)+1 || !strings.EqualFold(p[:len(base)+1], base+"/") {
		return "", false
	}
	c := path.Clean(p[len(base)+1:])
	if c == "." || c == ".." || strings.HasPrefix(c, "../") {
		return "", false
	}
	return c, true
}

func (r *backupInspectReport) addSecretsCheck(sc backupTarScan, cfg map[string]interface{}, m backupManifest) {
	var names []string
	var warns []string
	for _, rel := range sc.dataOrder {
		if sensitiveBackupName(rel) {
			names = append(names, rel)
		}
	}
	if body, ok := sc.held[".ncc-api-users.json"]; ok {
		if s := summarizeAuthProvidersBytes(body); s != nil {
			if s.SAMLHasSPKey {
				names = append(names, "SAML SP signing key")
			}
			if s.LDAPHasBindPassword {
				names = append(names, "LDAP bind password")
			}
			if s.SAMLEnabled && !s.SAMLHasSPKey {
				warns = append(warns, "SAML is on but no SP signing key is stored")
			}
		}
	}
	// Keys of a file-provider secrets file, and secret:// refs in the config.
	refs := map[string]bool{}
	secretRefs(cfg, refs)
	provider := strings.ToLower(cfgString(cfg, "secrets-provider"))
	var fileKeys map[string]bool
	if sf := cfgString(cfg, "secrets-file"); sf != "" {
		if rel, ok := archiveRelForConfigPath(sf, m.InstallDir); ok {
			if body, ok := sc.held[rel]; ok {
				fileKeys = map[string]bool{}
				var kv map[string]interface{}
				if err := json.Unmarshal(body, &kv); err != nil {
					kv = nil
					if yerr := yaml.Unmarshal(body, &kv); yerr != nil {
						warns = append(warns, fmt.Sprintf("the secrets file %s cannot be parsed", rel))
					}
				}
				for k := range kv {
					fileKeys[k] = true
					names = append(names, "secret "+k)
				}
			}
		}
	}
	if len(refs) > 0 && provider == "file" {
		var missing []string
		for n := range refs {
			if !fileKeys[n] {
				missing = append(missing, n)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			warns = append(warns, "config.yaml refers to secrets the backup's secrets file does not hold: "+strings.Join(missing, ", "))
		}
	} else if len(refs) > 0 && provider == "env" {
		warns = append(warns, fmt.Sprintf("config.yaml reads %s from environment variables, which a backup cannot carry", plural.Count(len(refs), "secret")))
	}
	sort.Strings(names)
	r.Secrets = append(r.Secrets[:0], names...)
	switch {
	case len(warns) > 0:
		r.add("Secrets", checkWarn, "%s.", strings.Join(warns, "; "))
	case len(names) == 0:
		r.add("Secrets", checkWarn, "No secrets found (no API token, user database or secrets file); sign-in may need to be set up again after a restore.")
	default:
		r.add("Secrets", checkPass, "Holds %s: %s.", plural.Count(len(names), "secret"), strings.Join(names, ", "))
	}
}

func (r *backupInspectReport) addReferencesCheck(sc backupTarScan, cfg map[string]interface{}, cfgRel string, m backupManifest) {
	var fails, warns []string
	checked := 0
	for _, key := range []string{"clusters-file", "exclude-alert-titles-file", "secrets-file", "pcs-file"} {
		raw := cfgString(cfg, key)
		if raw == "" {
			continue
		}
		checked++
		rel, ok := archiveRelForConfigPath(raw, m.InstallDir)
		if !ok {
			warns = append(warns, fmt.Sprintf("%s (%s) is outside the install dir, so it is not in the backup", key, raw))
			continue
		}
		if _, ok := sc.sums[rel]; !ok {
			fails = append(fails, fmt.Sprintf("%s points at %s, which is not in the backup", key, rel))
		}
	}
	outDir := cfgString(cfg, "output-dir-filtered")
	if outDir == "" {
		outDir = defaultOutputDirFiltered
	}
	reports := 0
	if rel, ok := archiveRelForConfigPath(outDir, m.InstallDir); ok {
		prefix := rel + "/"
		for _, f := range sc.dataOrder {
			if strings.HasPrefix(f, prefix) {
				reports++
			}
		}
		if reports == 0 {
			warns = append(warns, fmt.Sprintf("no report files under %s; the dashboard stays empty until the next run", rel))
		}
	} else {
		warns = append(warns, fmt.Sprintf("the report folder %s is outside the install dir, so no reports are in the backup", outDir))
	}
	switch {
	case len(fails) > 0:
		r.add("Report paths", checkFail, "%s.", summarizeIssues(append(fails, warns...)))
	case len(warns) > 0:
		r.add("Report paths", checkWarn, "%s.", summarizeIssues(warns))
	default:
		r.add("Report paths", checkPass, "%s from %s found, plus %s.", plural.Count(checked, "referenced file"), cfgRel, plural.Count(reports, "report file"))
	}
}

// addVersionCheck compares the backup's version with this binary's: the same
// major version is compatible, a newer minor warns, another major fails.
func (r *backupInspectReport) addVersionCheck(backupV, runningV string) {
	if strings.TrimSpace(backupV) == "" {
		r.add("Version", checkWarn, "The backup does not record which version made it.")
		return
	}
	parse := func(s string) *semver.Version {
		s = stripGoBuildGitSuffix(strings.TrimPrefix(strings.TrimSpace(s), "v"))
		v, err := semver.NewVersion(s)
		if err != nil {
			return nil
		}
		return v
	}
	b, cur := parse(backupV), parse(runningV)
	switch {
	case b == nil || cur == nil:
		r.add("Version", checkWarn, "Cannot compare versions (backup %s, this server %s).", backupV, defaultStr(runningV, "unknown"))
	case b.Major() != cur.Major():
		r.add("Version", checkFail, "The backup is from version %s and this server runs %s. Different major versions are not compatible.", backupV, runningV)
	case b.Minor() > cur.Minor():
		r.add("Version", checkWarn, "The backup is from a newer version (%s) than this server (%s); some settings may not load. Upgrade first if you can.", backupV, runningV)
	default:
		r.add("Version", checkPass, "The backup (%s) is compatible with this server (%s).", backupV, runningV)
	}
}

// printBackupInspectReport writes the human-readable report.
func printBackupInspectReport(w io.Writer, rep backupInspectReport) {
	fmt.Fprintf(w, "Backup check: %s\n", rep.Archive)
	if rep.BackupVersion != "" || rep.CreatedAt != "" {
		fmt.Fprintf(w, "  made by ncc-orchestrator %s at %s; this server runs %s\n\n", defaultStr(rep.BackupVersion, "?"), defaultStr(rep.CreatedAt, "?"), defaultStr(rep.RunningVersion, "?"))
	}
	warns, fails := 0, 0
	for _, c := range rep.Checks {
		switch c.Status {
		case checkWarn:
			warns++
		case checkFail:
			fails++
		}
		fmt.Fprintf(w, "  %-4s  %-13s %s\n", strings.ToUpper(c.Status), c.Name, c.Message)
	}
	fmt.Fprintln(w)
	switch {
	case fails > 0:
		fmt.Fprintf(w, "Result: FAILED (%s). Do not restore this backup.\n", plural.Count(fails, "failed check"))
	case warns > 0:
		fmt.Fprintf(w, "Result: OK with %s. Nothing was restored.\n", plural.Count(warns, "warning"))
	default:
		fmt.Fprintln(w, "Result: OK. Nothing was restored.")
	}
}

// runBackupVerify is `v2-restore --verify-only`: report every check, exit
// non-zero when one fails. It never touches an install dir.
func runBackupVerify(w io.Writer, in, keyFile, passphrase string, asJSON bool) error {
	rep := inspectBackupArchive(in, keyFile, passphrase, Version)
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			return err
		}
	} else {
		printBackupInspectReport(w, rep)
	}
	if !rep.OK {
		for _, c := range rep.Checks {
			if c.Status == checkFail {
				return fmt.Errorf("backup verification failed: %s: %s", c.Name, c.Message)
			}
		}
	}
	return nil
}
