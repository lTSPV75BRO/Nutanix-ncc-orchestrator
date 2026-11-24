package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"goncc/internal/plural"
	"goncc/internal/winacl"
)

// On Windows 0600/0700 modes protect nothing, so the start and the doctor set
// the install's permissions instead: secrets are closed to ordinary users, and
// program folders stay readable but not writable. Everything here is a no-op
// on other platforms.

// aclTarget is one path of an install and what ordinary users may do with it.
type aclTarget struct {
	path string
	mode winacl.Mode
	tree bool
	// reportOnly: a path outside the install dir that other software may use
	// too (a shared TLS key). It is checked, never changed.
	reportOnly bool
}

// aclInstall lists what belongs to one install.
type aclInstall struct {
	dir        string
	configPath string
	// secrets are files only this product reads, wherever they are.
	secrets []string
	// keys are TLS private keys: changed inside the install dir, only checked
	// outside it.
	keys []string
}

var (
	aclPrivateDirs = []string{"backups", "tls", "auth", "config", "configs", "secrets", "logs"}
	// run/ holds the files that ask the supervisor to restart or upgrade.
	aclProgramDirs = []string{"bin", "frontend-dist", "run"}
	// aclPrepareDirs are created before anyone else can create them.
	aclPrepareDirs = []string{"backups", "tls", "configs", "logs", "run"}
	// aclPrivateFiles are secret or security-relevant files in the install dir
	// itself (token, users, initial password, start settings, state, config and
	// its copies).
	aclPrivateFiles = []string{".ncc-*", "config*.y*ml*", "*.env", "secrets*"}
	// aclSkipFiles are empty locks and short-lived probe files.
	aclSkipFiles    = []string{"*.lock", ".ncc-ready-*", ".ncc-health-*", ".ncc-heal-probe-*", ".ncc-writecheck*"}
	aclProgramFiles = []string{"ncc-orchestrator*", "ncc-api-server*", "ncc-ui-server*", "ncc-mcp-server*"}
)

func (in aclInstall) targets() []aclTarget {
	var out []aclTarget
	seen := map[string]bool{}
	add := func(t aclTarget) {
		abs, err := filepath.Abs(t.path)
		key := strings.ToLower(abs)
		if err != nil || seen[key] {
			return
		}
		seen[key] = true
		t.path = abs
		out = append(out, t)
	}
	for _, name := range aclPrivateDirs {
		add(aclTarget{path: filepath.Join(in.dir, name), mode: winacl.Private, tree: true})
	}
	for _, name := range aclProgramDirs {
		add(aclTarget{path: filepath.Join(in.dir, name), mode: winacl.NoBroadWrite, tree: true})
	}
	addFiles := func(patterns []string, mode winacl.Mode) {
		entries, _ := os.ReadDir(in.dir)
		for _, e := range entries {
			// Directories are never matched: .ncc-v2 may be another install.
			if !e.IsDir() && !matchesAny(aclSkipFiles, e.Name()) && matchesAny(patterns, e.Name()) {
				add(aclTarget{path: filepath.Join(in.dir, e.Name()), mode: mode})
			}
		}
	}
	addFiles(aclPrivateFiles, winacl.Private)
	addFiles(aclProgramFiles, winacl.NoBroadWrite)
	// inTree: already covered by a folder above.
	inTree := func(path string) bool {
		for _, name := range append(append([]string(nil), aclPrivateDirs...), aclProgramDirs...) {
			if pathWithin(path, filepath.Join(in.dir, name)) {
				return true
			}
		}
		return false
	}
	for _, p := range append([]string{in.configPath}, in.secrets...) {
		if p = strings.TrimSpace(p); p != "" && !inTree(p) {
			add(aclTarget{path: p, mode: winacl.Private})
		}
	}
	for _, p := range in.keys {
		if p = strings.TrimSpace(p); p != "" && !inTree(p) {
			add(aclTarget{path: p, mode: winacl.Private, reportOnly: !pathWithin(p, in.dir)})
		}
	}
	return out
}

func matchesAny(patterns []string, name string) bool {
	for _, p := range patterns {
		if ok, _ := filepath.Match(p, strings.ToLower(name)); ok {
			return true
		}
	}
	return false
}

// pathWithin reports whether path is dir or inside it (Windows rules: case
// does not matter).
func pathWithin(path, dir string) bool {
	p, err1 := filepath.Abs(path)
	d, err2 := filepath.Abs(dir)
	if err1 != nil || err2 != nil {
		return false
	}
	p, d = strings.ToLower(p), strings.ToLower(d)
	return p == d || strings.HasPrefix(p, strings.TrimRight(d, `\/`)+string(filepath.Separator))
}

// aclOutcome is what a pass over the targets found.
type aclOutcome struct {
	changed []string
	broad   []string // still too open
	failed  []error
	// unsupported: the first path on a volume that keeps no permissions.
	unsupported string
}

func (o *aclOutcome) fail(path string, err error) {
	var pe *winacl.PathError
	if !errors.As(err, &pe) {
		err = &winacl.PathError{Path: path, Err: err}
	}
	if errors.Is(err, winacl.ErrUnsupported) {
		if o.unsupported == "" {
			o.unsupported = path
		}
		return
	}
	o.failed = append(o.failed, err)
}

func (in aclInstall) apply(fix bool) aclOutcome {
	var o aclOutcome
	for _, t := range in.targets() {
		switch {
		case t.tree && fix:
			changed, errs := winacl.RestrictTree(t.path, t.mode)
			o.changed = append(o.changed, changed...)
			for _, err := range errs {
				o.fail(t.path, err)
			}
		case t.tree:
			broad, errs := winacl.CheckTree(t.path, t.mode)
			o.broad = append(o.broad, broad...)
			for _, err := range errs {
				o.fail(t.path, err)
			}
		case fix && !t.reportOnly:
			changed, err := winacl.Restrict(t.path, t.mode)
			if changed {
				o.changed = append(o.changed, t.path)
			}
			if err != nil {
				o.fail(t.path, err)
			}
		default:
			broad, err := winacl.Check(t.path, t.mode)
			if len(broad) > 0 {
				o.broad = append(o.broad, t.path)
			}
			if err != nil {
				o.fail(t.path, err)
			}
		}
	}
	return o
}

// aclSummary lists a few items and counts the rest.
func aclSummary(items []string) string {
	const most = 3
	if len(items) <= most {
		return strings.Join(items, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(items[:most], ", "), len(items)-most)
}

func aclErrorSummary(errs []error) string {
	items := make([]string, 0, len(errs))
	for _, err := range errs {
		items = append(items, err.Error())
	}
	return aclSummary(items)
}

// aclConfigRef resolves a file named in the config against the install dir.
func aclConfigRef(installDir, path string) string {
	path = strings.TrimSpace(path)
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(installDir, path)
}

// aclRootHint is the command that closes the install dir itself. The product
// does not do that: the folder may be shared with other software.
func aclRootHint(dir string) string {
	return fmt.Sprintf(`icacls "%s" /inheritance:d, then icacls "%s" /remove:g *S-1-1-0 *S-1-5-11 *S-1-5-32-545`, dir, dir)
}

// secureInstallACLs restricts an install's secrets and program folders on
// Windows before the stack uses them. It only ever warns.
func secureInstallACLs(installDir string, opts v2StartOptions) {
	if !winacl.Supported || isVolumeRoot(installDir) {
		return
	}
	// Created here so that nobody else can create them first in a folder that
	// lets every user add subfolders. MkdirAll failures show up where the
	// folder is needed.
	for _, name := range aclPrepareDirs {
		_ = os.MkdirAll(filepath.Join(installDir, name), 0o700)
	}
	in := aclInstall{
		dir:        installDir,
		configPath: opts.ConfigPath,
		secrets: []string{
			opts.TokenFile,
			opts.UsersDB,
			filepath.Join(filepath.Dir(opts.UsersDB), ".ncc-initial-admin-password"),
			opts.APISessionSecretFile,
			os.Getenv("NCC_MASTER_KEY_FILE"),
			os.Getenv("NCC_BACKUP_KEY_FILE"),
		},
		keys: []string{opts.UITLSKeyFile, opts.APITLSKeyFile, opts.UIBackendClientKeyFile},
	}
	if cfg, err := loadConfigForValidation(opts.ConfigPath); err == nil {
		in.secrets = append(in.secrets, aclConfigRef(installDir, cfg.SecretsFile))
	}
	for _, line := range in.apply(true).startMessages(installDir) {
		fmt.Fprintln(os.Stderr, line)
	}
}

// startMessages are the lines a start prints: at most one per kind of problem.
func (o aclOutcome) startMessages(installDir string) []string {
	var out []string
	if len(o.changed) > 0 {
		out = append(out, fmt.Sprintf("[permissions] ordinary Windows users could reach %s; access is now limited to SYSTEM, Administrators and this account: %s",
			plural.Count(len(o.changed), "path"), aclSummary(o.changed)))
	}
	if len(o.failed) > 0 {
		out = append(out, "warning: could not restrict Windows permissions, other users of this computer may be able to read or change these: "+aclErrorSummary(o.failed))
	}
	if o.unsupported != "" {
		out = append(out, fmt.Sprintf("warning: could not restrict Windows permissions on %s: %v; keep the install on an NTFS or ReFS volume", o.unsupported, winacl.ErrUnsupported))
	}
	if len(o.broad) > 0 {
		out = append(out, "warning: other users of this computer can read these private keys outside the install folder: "+aclSummary(o.broad))
	}
	if w := aclRootWarning(installDir); w != "" {
		out = append(out, "warning: "+w)
	}
	return out
}

// aclRootWarning reports an install dir in which every user can add files.
func aclRootWarning(installDir string) string {
	broad, err := winacl.Check(installDir, winacl.NoBroadWrite)
	if err != nil || len(broad) == 0 {
		return ""
	}
	return fmt.Sprintf("%s can add files in the install folder %s; to close it, run as administrator: %s",
		strings.Join(broad, ", "), installDir, aclRootHint(installDir))
}

// refreshInstallACLs re-applies the permissions after files were replaced (an
// update or a restore): renamed files keep the permissions they were staged
// with. Silent; the next start and the doctor report problems.
func refreshInstallACLs(installDir string) {
	if winacl.Supported && !isVolumeRoot(installDir) {
		aclInstall{dir: installDir}.apply(true)
	}
}

// isVolumeRoot reports an install directly in C:\ or a share root, where a
// folder named logs or config most likely belongs to something else.
func isVolumeRoot(dir string) bool {
	abs, err := filepath.Abs(dir)
	return err == nil && filepath.Dir(abs) == abs
}

// writeSecretFile writes a 0600 file that is closed to ordinary users on
// Windows too.
func writeSecretFile(path string, data []byte) error {
	return winacl.WriteFile(path, data, 0o600)
}

// restrictNewSecret closes a file that was just created and is still empty.
func restrictNewSecret(path string) { _, _ = winacl.Restrict(path, winacl.Private) }

// renameSecretFile replaces a secret file without losing its restricted
// permissions on Windows.
func renameSecretFile(tmp, dst string) error { return winacl.Rename(tmp, dst) }

// checkSecretsPermsWindows is the Windows half of the secrets-perms check.
func checkSecretsPermsWindows(hc *healContext) healResult {
	res := healResult{ID: "secrets-perms", Title: "Secret file permissions", Category: "encryption"}
	in := aclInstall{
		dir:        hc.InstallDir,
		configPath: hc.ConfigPath,
		secrets:    []string{os.Getenv("NCC_MASTER_KEY_FILE"), os.Getenv("NCC_BACKUP_KEY_FILE")},
	}
	if hc.CfgLoaded {
		in.secrets = append(in.secrets, aclConfigRef(hc.InstallDir, hc.Cfg.SecretsFile))
	}
	if st, ok := loadV2StartState(hc.InstallDir); ok {
		in.keys = append(in.keys, st.UITLSKeyFile)
	}
	o := in.apply(hc.Fix && !isVolumeRoot(hc.InstallDir))
	root := aclRootWarning(hc.InstallDir)

	var problems []string
	if len(o.broad) > 0 {
		problems = append(problems, "ordinary Windows users can reach: "+aclSummary(o.broad))
	}
	if len(o.failed) > 0 {
		problems = append(problems, "could not check or restrict: "+aclErrorSummary(o.failed))
	}
	if o.unsupported != "" {
		problems = append(problems, fmt.Sprintf("%s: %v; keep the install on an NTFS or ReFS volume", o.unsupported, winacl.ErrUnsupported))
	}
	if root != "" {
		problems = append(problems, "the install folder itself is open to ordinary users")
	}
	if len(o.changed) > 0 {
		res.Fixed = true
		res.FixMsg = "limited to SYSTEM, Administrators and this account: " + aclSummary(o.changed)
	}
	if len(problems) > 0 {
		res.Status = healWarn
		res.Message = strings.Join(problems, "; ")
		var hints []string
		if !hc.Fix && len(o.broad) > 0 {
			hints = append(hints, "Re-run with --fix to limit the listed paths to SYSTEM, Administrators and this account.")
		}
		if root != "" {
			hints = append(hints, root)
		}
		res.Hint = strings.Join(hints, " ")
		return res
	}
	res.Status = healOK
	res.Message = "secret files and program folders are closed to ordinary Windows users"
	if res.Fixed {
		res.Message = "Windows permissions repaired"
	}
	return res
}
