package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// `ncc-orchestrator update` and the update helper. With a helper (or a service
// manager that can run one) `update` is `upgrade --to`: the whole stack,
// checked, and rolled back when it is not healthy. Without one it replaces the
// programs only, as before, and keeps the previous ones for a rollback by hand.

// updateProgramsOnlyEnv makes `update` replace the programs only. The API server
// sets it: it restarts the stack itself afterwards. (An environment variable,
// not a flag, so that an older ncc-orchestrator does not refuse it.)
const updateProgramsOnlyEnv = "NCC_UPDATE_PROGRAMS_ONLY"

func envTrue(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "on", "yes":
		return true
	}
	return false
}

// updateHelperCanTakeOver says whether `update` with these options can be
// handed to the update helper: options only the old path knows keep it.
func updateHelperCanTakeOver(opts updateOptions, programsOnly bool) bool {
	repo := strings.TrimSpace(opts.Repo)
	return !opts.CheckOnly && !programsOnly && !envTrue(updateProgramsOnlyEnv) &&
		strings.TrimSpace(opts.BinaryURL) == "" && !opts.SkipChecksumVerify && !opts.AllowMajorUpgrade &&
		(repo == "" || repo == defaultGitHubRepo)
}

// runUpdateCommand is `ncc-orchestrator update`.
func runUpdateCommand(ctx context.Context, opts updateOptions, programsOnly bool, out io.Writer) error {
	if updateHelperCanTakeOver(opts, programsOnly) {
		if svc, dir, why := updateHelperFor(); svc != nil {
			fmt.Fprintf(out, "The update helper upgrades the whole stack in %s (as `ncc-orchestrator upgrade`); --programs-only replaces the programs only.\n", dir)
			return runStackUpgrade(ctx, upgradeOptions{InstallDir: dir, To: opts.TargetVersion, Prerelease: opts.IncludePrerelease}, out)
		} else if why != "" {
			fmt.Fprintf(os.Stderr, "Replacing the programs only: %s. Restart the stack afterwards.\n", why)
		}
	}
	return runUpdate(opts)
}

// updateHelperFor returns the stack the helper can upgrade, or why not.
func updateHelperFor() (stackService, string, string) {
	if !upgradeHelperEnabled() {
		return nil, "", "the update helper is turned off (NCC_UPGRADE_HELPER=0)"
	}
	dir, err := resolveStackDir("")
	if err != nil || !existingDir(filepath.Join(dir, "bin")) {
		return nil, "", "" // not a stack install: a lone program
	}
	svc, err := stackServiceToUpgrade(dir)
	if err != nil {
		return nil, dir, "no service or update helper runs a stack from " + dir
	}
	if svc.kind() == kindManual && !fileExists(helperBinaryPath(dir)) {
		return nil, dir, "no update helper is installed for the stack in " + dir
	}
	return svc, dir, ""
}

// updateSnapshot is the previous version that `update` keeps, so that
// `upgrade --rollback` (or Roll back in the UI) can put it back for 24 hours.
type updateSnapshot struct {
	installDir, from, kind string
}

// saveUpdateSnapshot saves bin/, the web UI, the definitions and the
// configuration before `update` replaces them. It needs a stack the helper can
// later restart.
func saveUpdateSnapshot(installDir string, run func(time.Duration, string, ...string) (string, error)) (*updateSnapshot, error) {
	svc, err := stackServiceToUpgrade(installDir)
	if err != nil {
		return nil, err
	}
	if err := svc.privileged(); err != nil {
		return nil, err
	}
	if helperUnfinished(installDir) {
		return nil, fmt.Errorf("an upgrade is in progress")
	}
	if err := prepareHelperDir(installDir); err != nil {
		return nil, err
	}
	e := &helperEngine{installDir: installDir, svc: svc, logf: func(string, ...any) {},
		j: &helperJournal{helperRequest: helperRequest{Action: helperActionUpgrade}}}
	if err := e.snapshot(); err != nil {
		e.dropSnapshot()
		return nil, err
	}
	from := programVersion(run, svc.orchestratorBin())
	if from == "" {
		from = stripGoBuildGitSuffix(Version)
	}
	return &updateSnapshot{installDir: installDir, from: from, kind: svc.kind()}, nil
}

// keep records the snapshot as one a rollback can put back.
func (s *updateSnapshot) keep(to string) error {
	var added []string
	entries, _ := os.ReadDir(filepath.Join(s.installDir, "bin"))
	for _, en := range entries {
		if !en.IsDir() && !strings.Contains(en.Name(), updateAsideMarker) && !strings.HasPrefix(en.Name(), ".") &&
			!fileExists(filepath.Join(helperSnapshotDir(s.installDir), "bin", en.Name())) {
			added = append(added, en.Name())
		}
	}
	info := rollbackInfo{From: s.from, To: strings.TrimPrefix(to, "v"), Kind: s.kind, At: nowRFC3339(), Action: helperActionUpdate,
		NewFiles: added, Config: configSums(s.installDir)}
	if err := writeHelperJSON(helperRollbackPath(s.installDir), info); err != nil {
		s.drop()
		return err
	}
	return nil
}

func (s *updateSnapshot) drop() { dropHelperSnapshot(s.installDir) }

// dropUpdateSnapshot removes what `update` kept (only that).
func dropUpdateSnapshot(installDir string) {
	var info rollbackInfo
	if readHelperJSON(helperRollbackPath(installDir), &info) == nil && info.Action == helperActionUpdate {
		dropHelperSnapshot(installDir)
	}
}
