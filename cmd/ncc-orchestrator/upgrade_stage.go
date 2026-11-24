package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"goncc/internal/runtimecaps"
)

// Stage 1 of an upgrade: fetch and verify the release, put the new version's
// helper in place, prove that it works on this host, and hand the job over.
// Everything here leaves the running stack alone; until the hand-over a failure
// means "nothing was changed".

// helperProtocol is raised when the files in run/upgrade change meaning. A
// release that needs another one says so in its manifest.
const helperProtocol = 1

// upgradeManifest (upgrade-manifest.json) is an optional release asset, listed
// in checksums.txt like every other.
type upgradeManifest struct {
	Schema         int    `json:"schema"`
	Version        string `json:"version"`
	MinFrom        string `json:"min_from,omitempty"`
	HelperProtocol int    `json:"helper_protocol"`
}

const upgradeManifestName = "upgrade-manifest.json"

func (m upgradeManifest) allows(from string) error {
	if m.HelperProtocol > helperProtocol {
		return fmt.Errorf("v%s cannot be installed from v%s directly: upgrade to the newest release before it first", m.Version, from)
	}
	if m.MinFrom != "" && versionLess(from, m.MinFrom) {
		return fmt.Errorf("v%s needs v%s or later, and this is v%s: upgrade to v%s first", m.Version, m.MinFrom, from, m.MinFrom)
	}
	return nil
}

// upgradeEnabled: NCC_UPGRADE_HELPER=0 uses the in-supervisor upgrade instead.
func upgradeHelperEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("NCC_UPGRADE_HELPER"))) {
	case "0", "false", "off", "no":
		return false
	}
	return true
}

type stageOne struct {
	installDir string
	from       string // installed version
	svc        stackService
	log        *jobLog
	logf       func(string, ...any)
	runCmd     func(timeout time.Duration, name string, args ...string) (string, error)

	bundle     string // a local archive instead of a download
	prerelease bool

	probeTimeout, takeTimeout time.Duration
	poll                      time.Duration
}

func newStageOne(installDir string, svc stackService, log *jobLog, logf func(string, ...any)) *stageOne {
	// What a rollback brings back is what the service starts, which is not
	// always what runs right now.
	from := programVersion(runUpgradeCommand, svc.orchestratorBin())
	if from == "" {
		from = stripGoBuildGitSuffix(Version)
	}
	return &stageOne{
		installDir: installDir, svc: svc, log: log, logf: logf, from: from,
		runCmd: runUpgradeCommand, probeTimeout: 90 * time.Second, takeTimeout: 60 * time.Second, poll: 250 * time.Millisecond,
	}
}

// errUpToDate: there is nothing newer to install.
var errUpToDate = errors.New("already on the latest version")

// run prepares the upgrade and hands it over. The returned error is also in the
// job; nil means the helper has the job (or there is nothing to do).
func (s *stageOne) run(ctx context.Context, target string) error {
	unlock, err := s.lock()
	if err != nil {
		s.log.fail(upgFailed, err, "Not started: %v. Nothing was changed.", err)
		return err
	}
	defer unlock()
	stop := s.log.keepAlive(15 * time.Second)
	err = s.prepare(ctx, target)
	stop() // from here on the helper writes the progress
	if err == nil {
		err = s.handOver(ctx, helperActionUpgrade)
	}
	if errors.Is(err, errUpToDate) {
		s.cleanup()
		s.log.step(upgDone, "Already on the latest version; nothing to change")
		return nil
	}
	if err != nil {
		s.cleanup()
		s.log.fail(upgFailed, err, "Not started: %v. Nothing was changed.", err)
	}
	return err
}

func (s *stageOne) lock() (func(), error) {
	if err := runtimecaps.Detect().RejectHostOperation("in-place software updates"); err != nil {
		return nil, fmt.Errorf("%w; update the container image and roll out the Deployment", err)
	}
	if err := s.svc.privileged(); err != nil {
		return nil, err
	}
	if err := prepareHelperDir(s.installDir); err != nil {
		return nil, err
	}
	unlock, err := acquireSupervisorLock(helperFile(s.installDir, "stage.lock"))
	if err != nil {
		return nil, errors.New("another upgrade is being prepared")
	}
	if helperUnfinished(s.installDir) || fileExists(helperAcceptedPath(s.installDir)) || fileExists(helperRequestPath(s.installDir)) {
		unlock()
		return nil, errors.New("another upgrade is in progress")
	}
	return unlock, nil
}

func (s *stageOne) cleanup() {
	_ = os.RemoveAll(helperStageDir(s.installDir))
	_ = os.Remove(helperProbePath(s.installDir))
	if !helperUnfinished(s.installDir) {
		_ = os.Remove(helperWakePath(s.installDir))
	}
}

func (s *stageOne) prepare(ctx context.Context, target string) error {
	s.log.step(upgPreflight, "Checking this host")
	need := dirSize(filepath.Join(s.installDir, "bin")) + dirSize(filepath.Join(s.installDir, "frontend-dist"))
	need = need*4 + 250<<20
	if free, ok := diskFreeBytes(s.installDir); ok && free < need {
		return fmt.Errorf("only %s free on %s, need about %s", humanBytes(free), s.installDir, humanBytes(need))
	}
	st, err := s.fetch(target)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.New("the supervisor is stopping")
	}
	s.log.edit(func(j *upgradeJob) {
		j.TargetVersion, j.FromVersion, j.Runner, j.Action = st.Version, s.from, "stage", helperActionUpgrade
	})

	s.log.step(upgHelperInstall, "Installing the update helper of v%s", st.Version)
	body, err := os.ReadFile(filepath.Join(helperStageDir(s.installDir), st.Archive))
	if err != nil {
		return err
	}
	undo, err := s.installHelperFrom(body, st)
	if err != nil {
		return fmt.Errorf("the update helper could not be installed: %w", err)
	}
	s.log.step(upgHelperCheck, "Checking that the update helper works on this host")
	if err := s.probe(ctx, st.Version); err != nil {
		undo()
		return fmt.Errorf("the update helper does not work on this host: %w", err)
	}
	return nil
}

// fetch gets the release into run/upgrade/stage and verifies it.
func (s *stageOne) fetch(target string) (stagedRelease, error) {
	dir := helperStageDir(s.installDir)
	if err := os.RemoveAll(dir); err != nil {
		return stagedRelease{}, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return stagedRelease{}, err
	}
	var st stagedRelease
	var err error
	if s.bundle != "" {
		s.log.step(upgDownloading, "Reading %s", filepath.Base(s.bundle))
		st, err = s.fetchBundle(dir)
	} else {
		st, err = s.fetchRelease(dir, target)
	}
	if err != nil {
		return st, err
	}
	if err := writeHelperJSON(stageInfoPath(s.installDir), st); err != nil {
		return st, err
	}
	if _, _, err := verifyStaged(s.installDir); err != nil {
		return st, err
	}
	return st, nil
}

func (s *stageOne) fetchRelease(dir, target string) (stagedRelease, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	rel, err := findUpgradeRelease(client, s.from, target, s.prerelease)
	if err != nil {
		return stagedRelease{}, err
	}
	st := stagedRelease{Version: strings.TrimPrefix(strings.TrimSpace(rel.TagName), "v")}
	url, name := pickStackAssetForCurrentPlatform(*rel)
	if url == "" {
		return st, fmt.Errorf("release %s has no package for this platform", rel.TagName)
	}
	st.Archive = filepath.Base(name)
	s.log.step(upgDownloading, "Downloading v%s", st.Version)
	sums, sig, err := fetchReleaseChecksums(client, rel)
	if err != nil {
		return st, err
	}
	if err := checkReleaseManifest(client, rel, sums, s.from); err != nil {
		return st, err
	}
	big := &http.Client{Timeout: 15 * time.Minute}
	body, err := downloadBinaryURL(big, url)
	if err != nil {
		return st, err
	}
	if err := matchChecksum(sums, st.Archive, body); err != nil {
		return st, err
	}
	return st, saveStage(dir, st.Archive, body, sums, sig)
}

// fetchBundle takes a release archive from disk; checksums.txt (and its
// signature, for a signed build) lie beside it.
func (s *stageOne) fetchBundle(dir string) (stagedRelease, error) {
	st := stagedRelease{Archive: filepath.Base(s.bundle)}
	folder := filepath.Dir(s.bundle)
	body, err := os.ReadFile(s.bundle)
	if err != nil {
		return st, err
	}
	sums, err := os.ReadFile(filepath.Join(folder, stagedChecksums))
	if err != nil {
		return st, fmt.Errorf("checksums.txt of the release has to be in the same folder: %w", err)
	}
	sig, err := os.ReadFile(filepath.Join(folder, stagedSignature))
	if err != nil {
		sig = nil
	}
	if _, haveKey := embeddedReleasePublicKey(); haveKey {
		if sig == nil {
			return st, errors.New("checksums.txt.sig of the release has to be in the same folder")
		}
		if _, err := verifyReleaseSignature(sums, strings.TrimSpace(string(sig))); err != nil {
			return st, fmt.Errorf("checksums.txt: %w", err)
		}
	}
	if err := matchChecksum(sums, st.Archive, body); err != nil {
		return st, err
	}
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return st, err
	}
	defer os.RemoveAll(root)
	if err := extractArchiveByAssetName(body, st.Archive, root); err != nil {
		return st, fmt.Errorf("unpack %s: %w", st.Archive, err)
	}
	bin := existingBinaryInInstallDir(root, "ncc-orchestrator")
	if bin == "" {
		return st, fmt.Errorf("%s is not a release package for this platform", st.Archive)
	}
	_ = os.Chmod(bin, 0o755) // #nosec G302 -- a program
	if st.Version = programVersion(s.runCmd, bin); st.Version == "" {
		return st, fmt.Errorf("the programs in %s do not run on this host", st.Archive)
	}
	if err := checkUpgradePath(s.from, st.Version); err != nil {
		return st, err
	}
	return st, saveStage(dir, st.Archive, body, sums, sig)
}

func saveStage(dir, archive string, body, sums, sig []byte) error {
	if err := writeHelperFile(filepath.Join(dir, archive), body); err != nil {
		return err
	}
	if len(sig) > 0 {
		if err := writeHelperFile(filepath.Join(dir, stagedSignature), sig); err != nil {
			return err
		}
	}
	return writeHelperFile(filepath.Join(dir, stagedChecksums), sums)
}

// checkUpgradePath refuses a version that is not newer, or another major one.
func checkUpgradePath(from, to string) error {
	if !upgradeTargetPattern.MatchString(to) {
		return fmt.Errorf("invalid version %q", to)
	}
	if !versionLess(from, to) {
		if versionLess(to, from) {
			return fmt.Errorf("v%s is older than the installed v%s", to, from)
		}
		return errUpToDate
	}
	return enforceMajorUpgradePolicy(from, to, false)
}

// findUpgradeRelease picks the release to install: target, or the newest of the
// installed major version.
func findUpgradeRelease(client *http.Client, from, target string, prerelease bool) (*githubRelease, error) {
	releases, err := fetchGitHubReleases(defaultGitHubRepo, client)
	if err != nil {
		return nil, err
	}
	var rel *githubRelease
	if target = strings.TrimPrefix(strings.TrimSpace(target), "v"); target != "" {
		for i := range releases {
			if strings.EqualFold(strings.TrimPrefix(strings.TrimSpace(releases[i].TagName), "v"), target) {
				rel = &releases[i]
			}
		}
		if rel == nil {
			return nil, fmt.Errorf("version %s was not found", target)
		}
	} else {
		major, _ := parseVersionMajor(from)
		if rel = pickLatestSemverRelease(releases, major, prerelease || isPrereleaseVersion(from)); rel == nil {
			return nil, errUpToDate
		}
		if versionLess(strings.TrimPrefix(rel.TagName, "v"), from) {
			return nil, errUpToDate
		}
	}
	return rel, checkUpgradePath(from, strings.TrimPrefix(strings.TrimSpace(rel.TagName), "v"))
}

// fetchReleaseChecksums returns checksums.txt and its signature, verified when
// this build has a release key.
func fetchReleaseChecksums(client *http.Client, rel *githubRelease) (sums, sig []byte, err error) {
	asset, ok := findAsset(*rel, func(name string) bool { return name == stagedChecksums })
	if !ok {
		return nil, nil, fmt.Errorf("release %s has no checksums.txt", rel.TagName)
	}
	if sums, err = fetchURL(asset.BrowserDownloadURL, client); err != nil {
		return nil, nil, fmt.Errorf("fetch checksums.txt: %w", err)
	}
	sigText, present, err := fetchReleaseSignatureAsset(rel, client)
	if err != nil {
		return nil, nil, err
	}
	if _, haveKey := embeddedReleasePublicKey(); haveKey {
		if !present {
			return nil, nil, fmt.Errorf("release %s is not signed", rel.TagName)
		}
		if _, err := verifyReleaseSignature(sums, sigText); err != nil {
			return nil, nil, fmt.Errorf("checksums.txt of %s: %w", rel.TagName, err)
		}
	}
	if present {
		sig = []byte(sigText + "\n")
	}
	return sums, sig, nil
}

// checkReleaseManifest enforces the release's upgrade manifest, if it has one.
func checkReleaseManifest(client *http.Client, rel *githubRelease, sums []byte, from string) error {
	asset, ok := findAsset(*rel, func(name string) bool { return name == upgradeManifestName })
	if !ok {
		// Listed but not there: somebody removed it.
		if parseChecksumFile(sums, upgradeManifestName) != "" {
			return fmt.Errorf("release %s lists %s but does not have it", rel.TagName, upgradeManifestName)
		}
		return nil
	}
	body, err := fetchURL(asset.BrowserDownloadURL, client)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", upgradeManifestName, err)
	}
	if err := matchChecksum(sums, upgradeManifestName, body); err != nil {
		return err
	}
	var m upgradeManifest
	if err := decodeManifest(body, &m); err != nil {
		return fmt.Errorf("%s: %w", upgradeManifestName, err)
	}
	return m.allows(from)
}

func matchChecksum(sums []byte, name string, body []byte) error {
	want := parseChecksumFile(sums, name)
	if want == "" {
		return fmt.Errorf("%s is not listed in checksums.txt", name)
	}
	sum := sha256.Sum256(body)
	if got := hex.EncodeToString(sum[:]); !strings.EqualFold(got, want) {
		return fmt.Errorf("the checksum of %s is wrong (expected %s, got %s)", name, want, got)
	}
	return nil
}

// installHelperFrom installs the helper program out of a release archive. undo
// puts the previous helper back.
func (s *stageOne) installHelperFrom(archive []byte, st stagedRelease) (undo func(), err error) {
	tmp := filepath.Join(helperStageDir(s.installDir), "helper")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	if err := extractArchiveByAssetName(archive, st.Archive, tmp); err != nil {
		return nil, err
	}
	bin := existingBinaryInInstallDir(tmp, "ncc-orchestrator")
	if bin == "" {
		return nil, errors.New("the release has no ncc-orchestrator program")
	}
	body, err := os.ReadFile(bin)
	if err != nil {
		return nil, err
	}
	return installHelperProgram(s.installDir, s.svc, body, st.Version, s.runCmd)
}

// installHelperProgram puts a helper program and its definition in place.
func installHelperProgram(installDir string, svc stackService, body []byte, version string, run func(time.Duration, string, ...string) (string, error)) (undo func(), err error) {
	path := helperBinaryPath(installDir)
	prev := path + ".prev"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { // #nosec G301 -- programs are world-readable, like bin/
		return nil, err
	}
	staged, err := stageBytes(body, path, 0o755)
	if err != nil {
		return nil, err
	}
	defer os.Remove(staged)
	// Windows only starts programs whose name ends in .exe.
	trial := strings.TrimSuffix(path, exeSuffix()) + "-new" + exeSuffix()
	_ = removeWithRetry(trial)
	if err := renameWithRetry(staged, trial); err != nil {
		return nil, err
	}
	defer os.Remove(trial)
	relabelPrograms(filepath.Dir(path))
	if v := programVersion(run, trial); v != version {
		return nil, fmt.Errorf("it reports version %q instead of %s", v, version)
	}
	had := fileExists(path)
	if had {
		_ = removeWithRetry(prev)
		if err := renameWithRetry(path, prev); err != nil {
			return nil, err
		}
	}
	back := func() {
		if had {
			_ = removeWithRetry(path)
			_ = renameWithRetry(prev, path)
			return
		}
		svc.removeHelper()
		_ = removeWithRetry(path)
	}
	if err := renameWithRetry(trial, path); err != nil {
		back()
		return nil, err
	}
	refreshInstallACLs(installDir)
	if err := svc.installHelper(path); err != nil {
		back()
		return nil, err
	}
	return back, nil
}

// probe asks the installed helper to prove that it works.
func (s *stageOne) probe(ctx context.Context, version string) error {
	return probeInstalledHelper(ctx, s.installDir, s.svc, version, s.probeTimeout, s.poll)
}

func probeInstalledHelper(ctx context.Context, installDir string, svc stackService, version string, timeout, poll time.Duration) error {
	p := helperProbe{Nonce: newNonce(), Kind: svc.kind()}
	result := helperResultPath(installDir)
	_ = os.Remove(result)
	if err := writeHelperJSON(helperProbePath(installDir), p); err != nil {
		return err
	}
	defer os.Remove(helperProbePath(installDir))
	if err := wakeUpgradeHelper(installDir, svc); err != nil {
		return err
	}
	again := time.Now().Add(helperRewake)
	for deadline := time.Now().Add(timeout); ; {
		if time.Now().After(again) {
			_ = svc.wakeHelper()
			again = time.Now().Add(2 * helperRewake)
		}
		var res helperProbeResult
		if err := readHelperJSON(result, &res); err == nil && res.Nonce == p.Nonce {
			_ = os.Remove(result)
			switch {
			case !res.OK:
				return errors.New(res.Error)
			case res.Version != version:
				return fmt.Errorf("v%s answered instead of v%s", res.Version, version)
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("it did not answer within %s (see %s)", shortDuration(timeout), helperLogPath(installDir))
		}
		if !sleepCtx(ctx, poll) {
			return errors.New("the supervisor is stopping")
		}
	}
}

// helperRewake: when to start the helper once more while waiting for it.
const helperRewake = 8 * time.Second

// wakeUpgradeHelper starts the helper; the wake file makes the service manager
// start it again until it has finished.
func wakeUpgradeHelper(installDir string, svc stackService) error {
	if err := writeHelperFile(helperWakePath(installDir), []byte(nowRFC3339()+"\n")); err != nil {
		return err
	}
	return svc.wakeHelper()
}

// handOver gives the job to the helper and waits until it has taken it.
func (s *stageOne) handOver(ctx context.Context, action string) error {
	job := s.log.snapshot()
	s.log.step(upgHandover, "The update helper takes over")
	// The probe's helper may still be on its way out; it holds the lock.
	waitForHelperExit(ctx, s.installDir, 15*time.Second)
	req := helperRequest{
		ID: job.ID, Action: action, Kind: s.svc.kind(), From: job.FromVersion, Target: job.TargetVersion,
		BackupPath: job.BackupPath, RequestedBy: job.RequestedBy, RequestedAt: job.RequestedAt,
		StartedAt: job.StartedAt, Steps: s.log.snapshot().Steps,
	}
	if err := req.validate(); err != nil {
		return err
	}
	return giveToHelper(s.installDir, s.svc, req, s.takeTimeout, s.poll)
}

// waitForHelperExit waits until no helper holds the lock.
func waitForHelperExit(ctx context.Context, installDir string, d time.Duration) {
	for deadline := time.Now().Add(d); time.Now().Before(deadline); {
		if unlock, err := acquireSupervisorLock(helperLockPath(installDir)); err == nil {
			unlock()
			return
		}
		if !sleepCtx(ctx, 100*time.Millisecond) {
			return
		}
	}
}

// giveToHelper writes the request and waits for the helper to take it. Not
// interrupted by a shutdown: the request either is taken, or is withdrawn.
func giveToHelper(installDir string, svc stackService, req helperRequest, timeout, poll time.Duration) error {
	path := helperRequestPath(installDir)
	if err := writeHelperJSON(path, req); err != nil {
		return err
	}
	withdraw := func(why error) error {
		if err := os.Remove(path); err != nil {
			return nil // taken after all
		}
		return why
	}
	if err := wakeUpgradeHelper(installDir, svc); err != nil {
		return withdraw(err)
	}
	again := time.Now().Add(helperRewake)
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(poll) {
		if !fileExists(path) {
			return nil
		}
		if time.Now().After(again) {
			// A start can get lost while the previous run is just ending.
			_ = svc.wakeHelper()
			again = time.Now().Add(helperRewake)
		}
	}
	return withdraw(fmt.Errorf("the update helper did not start within %s (see %s)", shortDuration(timeout), helperLogPath(installDir)))
}
