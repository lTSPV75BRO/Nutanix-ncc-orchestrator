package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

// The service helper: a root oneshot unit, <service>-ops.service, started by a
// path unit while run/ops/req holds a request. The service (user ncc) asks it
// for what needs root:
//
//	schedule-install / schedule-remove   a systemd timer of its schedule
//	restart                              the stack, after a restore or a TLS change
//	upgrade / rollback / adopt           stage 1 of an upgrade (the update helper does the rest)
//
// A request is hostile input: one small JSON file of a fixed schema, unknown
// fields refused, names and paths checked; units are generated here, never
// taken from the request, and run as the service user. The answer goes to
// run/ops/res/<id>.json, which only root can write.

const (
	opScheduleInstall = "schedule-install"
	opScheduleRemove  = "schedule-remove"
	opRestart         = "restart"
	opUpgrade         = "upgrade"
	opRollback        = "rollback"
	opAdopt           = "adopt"
)

// maxOpsRequest is the largest request file read.
const maxOpsRequest = 16 << 10

func opsDir(installDir string) string        { return filepath.Join(installDir, "run", opsDirName) }
func opsRequestDir(installDir string) string { return filepath.Join(opsDir(installDir), "req") }
func opsResultDir(installDir string) string  { return filepath.Join(opsDir(installDir), "res") }

// opsServiceName names the service helper's units.
func opsServiceName(service string) string {
	if strings.TrimSpace(service) == "" {
		service = supervisorServiceName
	}
	return service + "-ops"
}

// opsRequest is what the service asks for.
type opsRequest struct {
	ID string `json:"id"`
	Op string `json:"op"`

	Task       string `json:"task,omitempty"`
	OnCalendar string `json:"on_calendar,omitempty"`
	Script     string `json:"script,omitempty"`
	WorkDir    string `json:"work_dir,omitempty"`

	Target      string `json:"target,omitempty"`
	JobID       string `json:"job_id,omitempty"`
	BackupPath  string `json:"backup_path,omitempty"`
	RequestedBy string `json:"requested_by,omitempty"`
	RequestedAt string `json:"requested_at,omitempty"`
}

// opsResult is the helper's answer.
type opsResult struct {
	ID     string `json:"id"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	Output string `json:"output,omitempty"`
	At     string `json:"at"`
}

var (
	opsIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
	// What OnCalendar= takes from cronToOnCalendar: weekdays, numbers, * , / - : and spaces.
	onCalendarPattern = regexp.MustCompile(`^[A-Za-z0-9*,/:. -]{1,200}$`)
	// A path that goes into a unit as it is: no quoting, no specifiers.
	unitPathPattern   = regexp.MustCompile(`^/[A-Za-z0-9._/@+-]{1,1000}$`)
	backupNamePattern = regexp.MustCompile(`^pre-update-[0-9]{8}T[0-9]{6}Z\.tar\.gz(\.enc)?$`)
)

func cleanUnitPath(p string) bool {
	return unitPathPattern.MatchString(p) && filepath.ToSlash(filepath.Clean(p)) == p && !strings.Contains(p, "/../")
}

// validate checks a request against its schema; installDir anchors paths.
func (r opsRequest) validate(installDir string) error {
	if !opsIDPattern.MatchString(r.ID) {
		return errors.New("invalid id")
	}
	empty := func(fields ...string) error {
		for _, f := range fields {
			if f != "" {
				return fmt.Errorf("%s takes no such field", r.Op)
			}
		}
		return nil
	}
	switch r.Op {
	case opScheduleInstall, opScheduleRemove:
		if err := validateScheduleTaskName(r.Task); err != nil || r.Task != strings.TrimSpace(r.Task) {
			return errors.New("invalid task name")
		}
		if err := empty(r.Target, r.JobID, r.BackupPath, r.RequestedBy, r.RequestedAt); err != nil {
			return err
		}
		if r.Op == opScheduleRemove {
			return empty(r.OnCalendar, r.Script, r.WorkDir)
		}
		if !onCalendarPattern.MatchString(r.OnCalendar) {
			return errors.New("invalid OnCalendar")
		}
		if !cleanUnitPath(r.Script) || filepath.Base(r.Script) != systemdUnitBase(r.Task)+".sh" {
			return errors.New("invalid runner script path")
		}
		if !cleanUnitPath(r.WorkDir) {
			return errors.New("invalid working directory")
		}
		return nil
	case opRestart, opAdopt:
		return empty(r.Task, r.OnCalendar, r.Script, r.WorkDir, r.Target, r.JobID, r.BackupPath, r.RequestedBy, r.RequestedAt)
	case opUpgrade, opRollback:
		if err := empty(r.Task, r.OnCalendar, r.Script, r.WorkDir); err != nil {
			return err
		}
		if r.JobID != "" && !helperIDPattern.MatchString(r.JobID) {
			return errors.New("invalid job id")
		}
		if len(r.RequestedBy) > 200 || strings.ContainsAny(r.RequestedBy, "\x00\r\n") {
			return errors.New("invalid requested_by")
		}
		if r.RequestedAt != "" {
			if _, err := time.Parse(time.RFC3339, r.RequestedAt); err != nil {
				return errors.New("invalid requested_at")
			}
		}
		if r.Op == opRollback {
			return empty(r.Target, r.BackupPath)
		}
		if r.Target != "" && !upgradeTargetPattern.MatchString(r.Target) {
			return errors.New("invalid target version")
		}
		if r.BackupPath != "" {
			if filepath.Dir(r.BackupPath) != filepath.Join(installDir, "backups") || !backupNamePattern.MatchString(filepath.Base(r.BackupPath)) {
				return errors.New("invalid backup path")
			}
		}
		return nil
	}
	return fmt.Errorf("unknown operation %q", r.Op)
}

// decodeOpsRequest reads a request strictly.
func decodeOpsRequest(b []byte) (opsRequest, error) {
	var r opsRequest
	if len(b) > maxOpsRequest {
		return r, errors.New("request too large")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return r, errors.New("trailing data")
	}
	return r, nil
}

// ---- the service's side ----

// opsAvailable returns the stack's service when this process is unprivileged
// and the service helper is installed for it.
func opsAvailable(installDir string) (*systemdService, bool) {
	if runtime.GOOS != "linux" || os.Geteuid() == 0 {
		return nil, false
	}
	svc, ok := findSystemdService(installDir, systemdUnitDir).(*systemdService)
	if !ok || svc == nil || !fileExists(svc.unit(opsServiceName(svc.o.ServiceName)+".path")) || !existingDir(opsRequestDir(svc.o.InstallDir)) {
		return nil, false
	}
	return svc, true
}

// submitOps hands a request to the service helper. It waits until the helper
// has taken it (pick), then for its answer (wait; 0 = do not wait).
func submitOps(installDir string, r opsRequest, pick, wait time.Duration) (opsResult, error) {
	if r.ID == "" {
		r.ID = newNonce()
	}
	if err := r.validate(installDir); err != nil {
		return opsResult{}, err
	}
	b, err := json.Marshal(r)
	if err != nil {
		return opsResult{}, err
	}
	dir := opsRequestDir(installDir)
	tmp := filepath.Join(dir, "."+r.ID+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return opsResult{}, fmt.Errorf("ask the service helper: %w", err)
	}
	_, werr := f.Write(b)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	req := filepath.Join(dir, r.ID+".json")
	if werr == nil {
		werr = os.Rename(tmp, req)
	}
	if werr != nil {
		_ = os.Remove(tmp)
		return opsResult{}, fmt.Errorf("ask the service helper: %w", werr)
	}
	res := filepath.Join(opsResultDir(installDir), r.ID+".json")
	taken := waitUntil(pick, func() bool { return !fileExists(req) })
	if !taken {
		if os.Remove(req) == nil {
			return opsResult{}, errors.New("the service helper (" + opsServiceName(supervisorServiceName) + ".path, root) did not take the request; check `systemctl status " + opsServiceName(supervisorServiceName) + ".path`")
		}
	}
	if wait <= 0 {
		return opsResult{ID: r.ID, OK: true}, nil
	}
	var out opsResult
	if !waitUntil(wait, func() bool { return readOpsResult(res, &out) == nil }) {
		return opsResult{}, fmt.Errorf("the service helper did not answer within %s", shortDuration(wait))
	}
	if !out.OK {
		return out, errors.New(out.Error)
	}
	return out, nil
}

func readOpsResult(path string, v *opsResult) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// ---- the root side ----

// runServiceHelper takes the requests in run/ops/req, one after the other,
// until there are none. Started by the path unit, as root.
func runServiceHelper(installDir string) error {
	if os.Geteuid() != 0 {
		return errors.New("the service helper runs as root")
	}
	installDir, err := filepath.Abs(installDir)
	if err != nil {
		return err
	}
	svc, ok := findSystemdService(installDir, systemdUnitDir).(*systemdService)
	if !ok || svc == nil {
		return fmt.Errorf("no service runs the stack in %s", installDir)
	}
	uid, gid, okIDs := lookupServiceIDs(svc.o.ServiceUser)
	if rootService(svc.o.ServiceUser) || !okIDs {
		return fmt.Errorf("the service runs as %q; it needs no service helper", svc.o.ServiceUser)
	}
	if err := prepareOpsDirs(installDir, gid); err != nil {
		return err
	}
	unlock, err := acquireSupervisorLock(filepath.Join(opsDir(installDir), "lock"))
	if err != nil {
		return nil // another one is at work
	}
	defer unlock()
	logf, closeLog := helperLogger(installDir)
	defer closeLog()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	pruneOpsResults(installDir)
	for round := 0; round < 64 && ctx.Err() == nil; round++ {
		names, junk := pendingOpsRequests(installDir)
		for _, j := range junk {
			_ = os.Remove(j)
		}
		if len(names) == 0 {
			return nil
		}
		for _, name := range names {
			r, err := takeOpsRequest(installDir, name, uid)
			res := opsResult{ID: strings.TrimSuffix(filepath.Base(name), ".json")}
			if err != nil {
				logf("service helper: refused %s: %v", filepath.Base(name), err)
				res.Error = "refused: " + err.Error()
			} else {
				logf("service helper: %s %s", r.Op, r.Task)
				out, err := runOpsRequest(ctx, svc, r)
				res.Output = tailString(out, 4000)
				if err != nil {
					logf("service helper: %s failed: %v", r.Op, err)
					res.Error = err.Error()
				}
				res.OK = err == nil
			}
			res.At = nowRFC3339()
			if opsIDPattern.MatchString(res.ID) {
				if err := writeOpsResult(installDir, res, gid); err != nil {
					logf("service helper: answer %s: %v", res.ID, err)
				}
			}
		}
	}
	return nil
}

func tailString(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

// pendingOpsRequests lists the requests (oldest first), and leftovers to delete.
func pendingOpsRequests(installDir string) (names, junk []string) {
	dir := opsRequestDir(installDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil
	}
	type item struct {
		name string
		at   time.Time
	}
	var items []item
	for _, e := range entries {
		p := filepath.Join(dir, e.Name())
		info, err := e.Info()
		if err != nil {
			continue
		}
		id, isReq := strings.CutSuffix(e.Name(), ".json")
		switch {
		case isReq && opsIDPattern.MatchString(id):
			items = append(items, item{p, info.ModTime()})
		case strings.HasPrefix(e.Name(), ".") && strings.HasSuffix(e.Name(), ".tmp") && time.Since(info.ModTime()) < time.Minute:
			// being written
		default:
			junk = append(junk, p)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].at.Before(items[j].at) })
	for _, it := range items {
		names = append(names, it.name)
	}
	return names, junk
}

// takeOpsRequest reads and removes one request: a regular file of the service
// user (or root), small, never through a symbolic link.
func takeOpsRequest(installDir, path string, uid int) (opsRequest, error) {
	defer os.Remove(path)
	f, err := openNoFollow(path)
	if err != nil {
		return opsRequest{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return opsRequest{}, err
	}
	if !fi.Mode().IsRegular() {
		return opsRequest{}, errors.New("not a regular file")
	}
	if owner := fileOwnerUID(fi); owner != uid && owner != 0 {
		return opsRequest{}, fmt.Errorf("written by user %d, not by the service", owner)
	}
	b, err := readBounded(f, maxOpsRequest+1)
	if err != nil {
		return opsRequest{}, err
	}
	r, err := decodeOpsRequest(b)
	if err != nil {
		return r, err
	}
	if r.ID != strings.TrimSuffix(filepath.Base(path), ".json") {
		return r, errors.New("the id does not match the file name")
	}
	return r, r.validate(installDir)
}

func writeOpsResult(installDir string, res opsResult, gid int) error {
	b, err := json.Marshal(res)
	if err != nil {
		return err
	}
	dst := filepath.Join(opsResultDir(installDir), res.ID+".json")
	staged, err := stageBytes(b, dst, 0o640)
	if err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		_ = os.Lchown(staged, 0, gid)
	}
	if err := renameWithRetry(staged, dst); err != nil {
		_ = os.Remove(staged)
		return err
	}
	return nil
}

// pruneOpsResults removes answers nobody read within an hour.
func pruneOpsResults(installDir string) {
	entries, err := os.ReadDir(opsResultDir(installDir))
	if err != nil {
		return
	}
	for _, e := range entries {
		if info, err := e.Info(); err == nil && time.Since(info.ModTime()) > time.Hour {
			_ = os.Remove(filepath.Join(opsResultDir(installDir), e.Name()))
		}
	}
}

// runOpsRequest carries a validated request out, as root.
func runOpsRequest(ctx context.Context, svc *systemdService, r opsRequest) (string, error) {
	installDir := svc.o.InstallDir
	var out bytes.Buffer
	switch r.Op {
	case opScheduleInstall:
		return "", installScheduleUnits(svc, r)
	case opScheduleRemove:
		return "", removeScheduleUnits(svc, r.Task)
	case opRestart:
		return "", svc.start()
	case opUpgrade:
		err := runStackUpgrade(ctx, upgradeOptions{
			InstallDir: installDir, To: r.Target, NoWait: true, SkipBackup: true,
			ID: r.JobID, BackupPath: r.BackupPath, RequestedBy: defaultIfBlank(r.RequestedBy, "the service"),
		}, &out)
		return out.String(), err
	case opRollback:
		err := runStackRollback(ctx, installDir, r.JobID, defaultIfBlank(r.RequestedBy, "the service"), false, &out)
		return out.String(), err
	case opAdopt:
		err := runUpgradeAdopt(ctx, installDir, true, false, &out)
		return out.String(), err
	}
	return "", fmt.Errorf("unknown operation %q", r.Op)
}

func defaultIfBlank(s, def string) string {
	if strings.TrimSpace(s) == "" {
		return def
	}
	return s
}

// scheduleUnitOurs reports whether a schedule unit is absent or ours.
func scheduleUnitOurs(path, task string) bool {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return true
	}
	return err == nil && strings.Contains(string(b), "# "+scheduleMarker(task)+"\n")
}

// installScheduleUnits writes a schedule's units for the service user.
func installScheduleUnits(svc *systemdService, r opsRequest) error {
	st, err := os.Lstat(r.Script)
	if err != nil || !st.Mode().IsRegular() {
		return fmt.Errorf("the runner script %s is not a file", r.Script)
	}
	if !existingDir(r.WorkDir) {
		return fmt.Errorf("the working directory %s does not exist", r.WorkDir)
	}
	if _, err := svcCommand("systemd-analyze", "calendar", r.OnCalendar); err != nil && !strings.Contains(err.Error(), "executable file not found") {
		return fmt.Errorf("OnCalendar=%s is not valid", r.OnCalendar)
	}
	base := systemdUnitBase(r.Task)
	servicePath, timerPath := svc.unit(base+".service"), svc.unit(base+".timer")
	for _, p := range []string{servicePath, timerPath} {
		if !scheduleUnitOurs(p, r.Task) {
			return fmt.Errorf("%s exists and was not written by ncc-orchestrator", p)
		}
	}
	service, timer := buildSystemdScheduleUnitsFor(r.Task, r.OnCalendar, r.Script, r.WorkDir, scheduleMarker(r.Task), svc.o.ServiceUser)
	if err := writeDefinitionFile(servicePath, service); err != nil {
		return err
	}
	if err := writeDefinitionFile(timerPath, timer); err != nil {
		return err
	}
	if _, err := svcCommand("systemctl", "daemon-reload"); err != nil {
		return err
	}
	_, err = svcCommand("systemctl", "enable", "--now", base+".timer")
	return err
}

// removeScheduleUnits removes a schedule's units (ours only).
func removeScheduleUnits(svc *systemdService, task string) error {
	base := systemdUnitBase(task)
	servicePath, timerPath := svc.unit(base+".service"), svc.unit(base+".timer")
	for _, p := range []string{servicePath, timerPath} {
		if !scheduleUnitOurs(p, task) {
			return fmt.Errorf("%s was not written by ncc-orchestrator; it is left alone", p)
		}
	}
	_, _ = svcCommand("systemctl", "disable", "--now", base+".timer")
	_, _ = svcCommand("systemctl", "stop", base+".service")
	var errs []error
	for _, p := range []string{timerPath, servicePath} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	_, err := svcCommand("systemctl", "daemon-reload")
	return errors.Join(append(errs, err)...)
}

// ---- units ----

// opsUnit is the service helper's service: root, process hardening only (it
// writes units, calls systemctl and installs the update helper).
func (s *systemdService) opsUnit() string {
	lines := []string{
		"[Unit]",
		"Description=NCC Orchestrator service helper (applies schedule, restart and upgrade requests of the service as root)",
		"Documentation=https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator",
		"StartLimitIntervalSec=0",
		"",
		"[Service]",
		"Type=oneshot",
		fmt.Sprintf("ExecStart=%s service-helper --install-dir %s", systemdArg(s.o.OrchestratorBin), systemdArg(s.o.InstallDir)),
		"TimeoutStartSec=30min",
		"TimeoutStopSec=60",
	}
	lines = append(lines, systemdProcessHardening()...)
	return strings.Join(append(lines, ""), "\n")
}

// opsPathUnit starts the service helper while a request waits.
func (s *systemdService) opsPathUnit() string {
	return strings.Join([]string{
		"[Unit]",
		"Description=Start the NCC Orchestrator service helper when the service asks for something",
		"Documentation=https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator",
		"",
		"[Path]",
		"DirectoryNotEmpty=" + strings.ReplaceAll(filepath.ToSlash(opsRequestDir(s.o.InstallDir)), "%", "%%"),
		"Unit=" + opsServiceName(s.o.ServiceName) + ".service",
		"",
		"[Install]",
		"WantedBy=multi-user.target",
		"",
	}, "\n")
}

// stackInstallDir is the install this program belongs to: <install>/bin/ncc-orchestrator.
func stackInstallDir() string {
	if self, err := os.Executable(); err == nil {
		if real, err := filepath.EvalSymlinks(self); err == nil {
			self = real
		}
		if bin := filepath.Dir(self); filepath.Base(bin) == "bin" {
			return filepath.Dir(bin)
		}
	}
	dir, _ := filepath.Abs(defaultV2InstallDir())
	return dir
}

// stackServiceUser is the user the stack's service runs as ("" when there is
// no such service, or it is from before LIN-02).
func stackServiceUser(installDir string) string {
	if runtime.GOOS != "linux" {
		return ""
	}
	if svc, ok := findSystemdService(installDir, systemdUnitDir).(*systemdService); ok && svc != nil {
		return svc.o.ServiceUser
	}
	return ""
}

// ---- commands ----

func newServiceOpsCommands() []*cobra.Command {
	helper := &cobra.Command{
		Use: "service-helper", Hidden: true, SilenceUsage: true, Args: cobra.NoArgs,
		Short: "Apply the service's requests as root (started by the service manager)",
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _ := cmd.Flags().GetString("install-dir")
			if strings.TrimSpace(dir) == "" {
				return errors.New("--install-dir is required")
			}
			return runServiceHelper(dir)
		},
	}
	helper.Flags().String("install-dir", "", "Install folder")

	request := &cobra.Command{
		Use: "service-request", Hidden: true, SilenceUsage: true, Args: cobra.ExactArgs(1),
		Short: "Ask the service helper to restart the stack (used by the API server)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if args[0] != opRestart {
				return fmt.Errorf("unknown request %q", args[0])
			}
			flag, _ := cmd.Flags().GetString("install-dir")
			dir, err := resolveStackDir(flag)
			if err != nil {
				return err
			}
			if _, ok := opsAvailable(dir); !ok {
				return errors.New("no service helper here (the service runs as root, or not under systemd)")
			}
			_, err = submitOps(dir, opsRequest{Op: opRestart}, 30*time.Second, 0)
			return err
		},
	}
	request.Flags().String("install-dir", "", "Install folder")
	return []*cobra.Command{helper, request}
}
