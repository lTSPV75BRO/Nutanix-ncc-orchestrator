package main

// Checks around a Kubernetes upgrade (Helm hooks, or the Jobs in k8s/):
//
//	upgrade-preflight   before the rollout: version skew, write permission on
//	                    the data volume, free space, and a verified backup
//	                    taken through the API. The first failure of the first
//	                    three stops before a backup is made.
//	upgrade-verify      after the rollout: the API, the UI and this runner
//	                    image all report the target version, five answers in a
//	                    row.
//
// Both only read or write through the same interfaces an operator has: the
// API's HTTP endpoints and the mounted data volume. Nothing here changes the
// running stack except the backup it asks for.

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/cobra"
)

// preflightOptions are the settings of upgrade-preflight.
type preflightOptions struct {
	API            string        // base URL of the running API
	Token          string        // its API token (the backup needs it)
	DataDir        string        // where the data volume is mounted
	Target         string        // version about to be rolled out; "" = this binary's
	Expect         string        // optional: must agree with Target
	MinFrom        string        // oldest version the target can be installed over
	MinFreeBytes   int64         // 0 = skip the space check
	Backup         bool          // take and verify a backup
	Skew           bool          // check the version change
	Permissions    bool          // check that the volume can be written
	AllowDowngrade bool          // do not refuse a lower target
	Timeout        time.Duration // how long to wait for the API
}

// preflightClient talks to the in-cluster API over plain HTTP.
func preflightClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: &http.Transport{DisableKeepAlives: true}}
}

// runPreflight runs the selected checks and prints one line for each. It
// returns an error that names every failed check.
func runPreflight(ctx context.Context, o preflightOptions, out io.Writer) error {
	target := stripGoBuildGitSuffix(strings.TrimPrefix(strings.TrimSpace(o.Target), "v"))
	if target == "" {
		target = stripGoBuildGitSuffix(strings.TrimPrefix(Version, "v"))
	}
	var failed []string
	fail := func(name string, err error) {
		fmt.Fprintf(out, "FAIL  %s: %v\n", name, err)
		failed = append(failed, name)
	}
	pass := func(format string, a ...any) { fmt.Fprintf(out, "ok    "+format+"\n", a...) }

	fmt.Fprintf(out, "Pre-upgrade checks for v%s\n", target)
	if want := strings.TrimPrefix(strings.TrimSpace(o.Expect), "v"); want != "" && want != target {
		fail("target", fmt.Errorf("the chart's images are v%s but the version to expect is v%s: change upgradeCheck.expectVersion or the image tags", target, want))
	}

	client := preflightClient(10 * time.Second)
	current := ""
	if o.Skew || o.Backup {
		v, err := waitHealthVersion(ctx, client, o.API, o.Timeout)
		if err != nil {
			if o.Backup {
				fail("api", fmt.Errorf("the running API does not answer (%v): a backup needs it. Start it, or set upgradeCheck.preCheck.backup=false", err))
			} else {
				fmt.Fprintf(out, "skip  version skew: the running API does not answer (%v)\n", err)
			}
		} else {
			current = v
			pass("running version is v%s", current)
		}
	}
	if o.Skew && current != "" {
		if msg, err := checkVersionSkew(current, target, o.MinFrom, o.AllowDowngrade); err != nil {
			fail("version skew", err)
		} else {
			pass("version skew: %s", msg)
		}
	}
	if o.Permissions {
		if msg, err := checkDataWritable(o.DataDir); err != nil {
			fail("permissions", err)
		} else {
			pass("permissions: %s", msg)
		}
	}
	if o.MinFreeBytes > 0 {
		if msg, err := checkFreeSpace(o.DataDir, o.MinFreeBytes); err != nil {
			fail("free space", err)
		} else {
			pass("free space: %s", msg)
		}
	}
	if len(failed) > 0 {
		fmt.Fprintln(out, "no backup was taken and nothing was changed")
		return preflightError(failed)
	}
	if o.Backup {
		name, err := takeVerifiedBackup(ctx, o.API, o.Token, out)
		if err != nil {
			fail("backup", err)
			return preflightError(failed)
		}
		pass("backup %s taken and verified", name)
		if o.MinFreeBytes > 0 { // the backup lives on the same volume
			if msg, err := checkFreeSpace(o.DataDir, o.MinFreeBytes); err != nil {
				fail("free space after the backup", err)
				return preflightError(failed)
			} else {
				pass("free space after the backup: %s", msg)
			}
		}
	}
	fmt.Fprintln(out, "all pre-upgrade checks passed")
	return nil
}

func preflightError(failed []string) error {
	return fmt.Errorf("pre-upgrade check failed (%s): the upgrade was stopped before anything changed", strings.Join(failed, ", "))
}

// waitHealthVersion reads the running version, retrying while the API starts.
func waitHealthVersion(ctx context.Context, client *http.Client, api string, wait time.Duration) (string, error) {
	if strings.TrimSpace(api) == "" {
		return "", errors.New("no API address")
	}
	if wait <= 0 {
		wait = 30 * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		v, err := healthVersion(ctx, client, strings.TrimRight(api, "/")+"/api/v1/health")
		if err == nil {
			return v, nil
		}
		if time.Now().After(deadline) {
			return "", err
		}
		if !sleepCtx(ctx, upgradeCheckEvery) {
			return "", ctx.Err()
		}
	}
}

// checkVersionSkew decides whether current -> target is an upgrade the target
// accepts: not a downgrade, and not older than min_from (which is how a
// release says "go through vX first", see upgradeManifest.allows).
func checkVersionSkew(current, target, minFrom string, allowDowngrade bool) (string, error) {
	cur, errC := semver.NewVersion(stripGoBuildGitSuffix(strings.TrimPrefix(strings.TrimSpace(current), "v")))
	tgt, errT := semver.NewVersion(stripGoBuildGitSuffix(strings.TrimPrefix(strings.TrimSpace(target), "v")))
	if errC != nil || errT != nil {
		// Images tagged with something else than a version (a branch build).
		return fmt.Sprintf("v%s -> v%s are not both plain versions, not compared", current, target), nil
	}
	if tgt.LessThan(cur) && !allowDowngrade {
		return "", fmt.Errorf("v%s is older than the running v%s: a downgrade is refused (upgradeCheck.preCheck.allowDowngrade=true allows it; restore a backup to go back)", tgt, cur)
	}
	m := upgradeManifest{Version: tgt.String(), MinFrom: strings.TrimSpace(minFrom)}
	if err := m.allows(cur.String()); err != nil {
		return "", err
	}
	if tgt.LessThan(cur) {
		return fmt.Sprintf("v%s -> v%s, a downgrade, allowed by setting", cur, tgt), nil
	}
	return fmt.Sprintf("v%s -> v%s", cur, tgt), nil
}

// checkDataWritable writes and removes a file in the data volume (and in the
// folders the stack keeps there) as the user this process runs as, which the
// chart sets to the API's runAsUser and fsGroup.
func checkDataWritable(dir string) (string, error) {
	st, err := os.Stat(dir)
	if err != nil {
		return "", fmt.Errorf("the data volume %s is not there: %v", dir, err)
	}
	if !st.IsDir() {
		return "", fmt.Errorf("%s is not a folder", dir)
	}
	who := fmt.Sprintf("uid %d gid %d", os.Getuid(), os.Getgid())
	targets := []string{dir}
	for _, sub := range []string{"config", "outputfiles", "logs", "auth", "backups", "nccfiles"} {
		p := filepath.Join(dir, sub)
		if s, err := os.Stat(p); err == nil && s.IsDir() {
			targets = append(targets, p)
		}
	}
	for _, d := range targets {
		f, err := os.CreateTemp(d, ".ncc-upgrade-probe-*")
		if err != nil {
			return "", fmt.Errorf("%s cannot write to %s (%v): check the volume's ownership, fsGroup or runAsUser", who, d, err)
		}
		name := f.Name()
		_, werr := f.WriteString("probe")
		serr := f.Sync()
		cerr := f.Close()
		rerr := os.Remove(name)
		for _, e := range []error{werr, serr, cerr, rerr} {
			if e != nil {
				return "", fmt.Errorf("%s wrote %s but the file could not be saved or removed: %v", who, d, e)
			}
		}
	}
	return fmt.Sprintf("%s can write %d folder(s) of %s", who, len(targets), dir), nil
}

// checkFreeSpace refuses when less than min bytes are free for the stack.
func checkFreeSpace(dir string, min int64) (string, error) {
	free, err := freeBytes(dir)
	if err != nil {
		return "", fmt.Errorf("cannot read the free space of %s: %v", dir, err)
	}
	if free < uint64(min) {
		return "", fmt.Errorf("%s has %s free, less than the %s required (upgradeCheck.preCheck.minFreeSpace): free space or enlarge the volume", dir, k8sBytes(free), k8sBytes(uint64(min)))
	}
	return fmt.Sprintf("%s free on %s, %s required", k8sBytes(free), dir, k8sBytes(uint64(min))), nil
}

func k8sBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit && exp < 4; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTP"[exp])
}

// parseByteQuantity reads "512Mi", "2Gi", "500M", "1048576" (Kubernetes
// style: Ki Mi Gi Ti are binary, K M G T decimal). "" and "0" mean none.
func parseByteQuantity(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	units := []struct {
		suffix string
		mult   float64
	}{
		{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40}, {"Pi", 1 << 50},
		{"K", 1e3}, {"k", 1e3}, {"M", 1e6}, {"G", 1e9}, {"T", 1e12}, {"P", 1e15},
	}
	mult := 1.0
	num := s
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			mult, num = u.mult, strings.TrimSuffix(s, u.suffix)
			break
		}
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("%q is not a size such as 512Mi or 2Gi", s)
	}
	if f*mult > 9e18 {
		return 0, fmt.Errorf("%q is too large", s)
	}
	return int64(f * mult), nil
}

// takeVerifiedBackup asks the running API for a backup and has it verified.
func takeVerifiedBackup(ctx context.Context, api, token string, out io.Writer) (string, error) {
	if strings.TrimSpace(token) == "" {
		return "", errors.New("no API token (NCC_API_TOKEN) to ask for a backup with: the Secret needs an api-token key")
	}
	client := preflightClient(5 * time.Minute)
	base := strings.TrimRight(api, "/")
	created, err := apiCall(ctx, client, http.MethodPost, base+"/api/v1/settings/backups", token, []byte("{}"))
	if err != nil {
		return "", fmt.Errorf("the API could not take a backup: %w", err)
	}
	var c struct {
		Data struct {
			Backup struct {
				Name string `json:"name"`
			} `json:"backup"`
		} `json:"data"`
	}
	if err := json.Unmarshal(created, &c); err != nil || c.Data.Backup.Name == "" {
		return "", errors.New("the API answered without the name of the backup")
	}
	name := c.Data.Backup.Name
	fmt.Fprintf(out, "      backup %s created, verifying\n", name)
	body, _ := json.Marshal(map[string]string{"name": name})
	if _, err := apiCall(ctx, client, http.MethodPost, base+"/api/v1/settings/backups/verify", token, body); err != nil {
		return "", fmt.Errorf("backup %s was taken but does not verify: %w", name, err)
	}
	return name, nil
}

// apiCall sends one authenticated request and returns the body of a success.
func apiCall(ctx context.Context, client *http.Client, method, url, token string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Token", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := readBounded(resp.Body, 1<<20)
	if err != nil {
		return nil, err
	}
	var env struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	_ = json.Unmarshal(raw, &env)
	if resp.StatusCode != http.StatusOK || !env.Success {
		msg := strings.TrimSpace(env.Error)
		if msg == "" {
			msg = "HTTP " + strconv.Itoa(resp.StatusCode)
		} else {
			msg = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, msg)
		}
		return nil, errors.New(msg)
	}
	return raw, nil
}

// ---- after the rollout ----

// rolloutOptions are the settings of upgrade-verify.
type rolloutOptions struct {
	API, UI string
	Expect  string
	Runner  string // version of the runner image; "" = this binary's
	Timeout time.Duration
}

// runUpgradeVerify waits until the API, the UI and the runner image all
// report the same (expected) version, five times in a row.
func runUpgradeVerify(ctx context.Context, o rolloutOptions, out io.Writer) error {
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}, // #nosec G402 -- only a version is read; the UI's certificate is often self-signed
	}}
	runner := stripGoBuildGitSuffix(strings.TrimPrefix(strings.TrimSpace(o.Runner), "v"))
	if runner == "" {
		runner = stripGoBuildGitSuffix(strings.TrimPrefix(Version, "v"))
	}
	for deadline, good := time.Now().Add(o.Timeout), 0; ; {
		err := checkRolloutVersions(ctx, client, o.API, o.UI, o.Expect, runner, out)
		if err == nil {
			if good++; good == upgradeCheckInARow {
				return nil
			}
		} else {
			good = 0
			if time.Now().After(deadline) {
				return err
			}
			fmt.Fprintln(out, "waiting:", err)
		}
		if !sleepCtx(ctx, upgradeCheckEvery) {
			return ctx.Err()
		}
	}
}

func checkRolloutVersions(ctx context.Context, client *http.Client, api, ui, expect, runner string, out io.Writer) error {
	// API (and the UI's proxy to it) through the existing check.
	if err := checkStackVersions(ctx, client, api, ui, expect, out); err != nil {
		return err
	}
	apiVersion, err := healthVersion(ctx, client, strings.TrimRight(api, "/")+"/api/v1/health")
	if err != nil {
		return fmt.Errorf("API: %w", err)
	}
	want := strings.TrimPrefix(strings.TrimSpace(expect), "v")
	if want == "" {
		want = apiVersion
	}
	if ui != "" {
		got, err := uiOwnVersion(ctx, client, ui)
		if err != nil {
			return fmt.Errorf("UI: %w", err)
		}
		if got != want {
			return fmt.Errorf("the UI server runs v%s, not v%s", got, want)
		}
		fmt.Fprintf(out, "UI server: v%s\n", got)
	}
	if runner != want {
		return fmt.Errorf("the runner image is v%s, not v%s: the CronJob would run the old version (check images.runner.tag)", runner, want)
	}
	fmt.Fprintf(out, "runner: v%s\n", runner)
	return nil
}

// uiOwnVersion reads the version header the UI server puts on its pages.
func uiOwnVersion(ctx context.Context, client *http.Client, ui string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(ui, "/")+"/", nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	v := strings.TrimSpace(resp.Header.Get("X-NCC-UI-Version"))
	if v == "" {
		return "", fmt.Errorf("HTTP %d without a version: this is an older UI image", resp.StatusCode)
	}
	return stripGoBuildGitSuffix(strings.TrimPrefix(v, "v")), nil
}

// ---- commands ----

func newK8sCheckCommands() []*cobra.Command {
	pre := &cobra.Command{
		Use: "upgrade-preflight", Hidden: true, SilenceUsage: true, Args: cobra.NoArgs,
		Short: "Checks before a Kubernetes upgrade: version skew, permissions, free space, backup (Helm pre-upgrade hook)",
		RunE: func(cmd *cobra.Command, args []string) error {
			get := func(n string) string { v, _ := cmd.Flags().GetString(n); return strings.TrimSpace(v) }
			flag := func(n string) bool { v, _ := cmd.Flags().GetBool(n); return v }
			min, err := parseByteQuantity(get("min-free"))
			if err != nil {
				return fmt.Errorf("--min-free: %w", err)
			}
			timeout, _ := cmd.Flags().GetDuration("timeout")
			return runPreflight(cmd.Context(), preflightOptions{
				API: get("api"), Token: os.Getenv("NCC_API_TOKEN"), DataDir: get("data-dir"),
				Target: get("target"), Expect: get("expect"), MinFrom: get("min-from"), MinFreeBytes: min,
				Backup: flag("backup"), Skew: flag("skew"), Permissions: flag("permissions"),
				AllowDowngrade: flag("allow-downgrade"), Timeout: timeout,
			}, cmd.OutOrStdout())
		},
	}
	f := pre.Flags()
	f.String("api", "", "Base URL of the running API")
	f.String("data-dir", "/data", "Where the data volume is mounted")
	f.String("target", "", "Version being rolled out (default: this binary's)")
	f.String("expect", "", "Version the chart is expected to roll out (optional)")
	f.String("min-from", "", "Oldest version the target can be installed over (optional)")
	f.String("min-free", "512Mi", "Free space required on the data volume (0 = skip)")
	f.Bool("backup", true, "Take and verify a backup through the API")
	f.Bool("skew", true, "Check the version change")
	f.Bool("permissions", true, "Check that the data volume can be written")
	f.Bool("allow-downgrade", false, "Do not refuse a target older than the running version")
	f.Duration("timeout", 2*time.Minute, "How long to wait for the running API")

	verify := &cobra.Command{
		Use: "upgrade-verify", Hidden: true, SilenceUsage: true, Args: cobra.NoArgs,
		Short: "After a Kubernetes upgrade: the API, UI and runner all run the target version (Helm post-upgrade hook)",
		RunE: func(cmd *cobra.Command, args []string) error {
			get := func(n string) string { v, _ := cmd.Flags().GetString(n); return strings.TrimSpace(v) }
			if get("api") == "" {
				return errors.New("--api is required")
			}
			timeout, _ := cmd.Flags().GetDuration("timeout")
			return runUpgradeVerify(cmd.Context(), rolloutOptions{API: get("api"), UI: get("ui"), Expect: get("expect"), Timeout: timeout}, cmd.OutOrStdout())
		},
	}
	verify.Flags().String("api", "", "Base URL of the API")
	verify.Flags().String("ui", "", "Base URL of the UI (optional)")
	verify.Flags().String("expect", "", "Expected version (default: whatever the API reports)")
	verify.Flags().Duration("timeout", 3*time.Minute, "How long to wait")
	return []*cobra.Command{pre, verify}
}
