package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Engine helpers for the self-heal registry in selfheal.go: crash isolation,
// timing, the re-check after a fix, and the fix history.

// recurringFixThreshold is how many fixes of one check in 24h mark it as
// recurring: something outside self-heal keeps undoing the repair.
const recurringFixThreshold = 3

// runHealCheckSafely runs one check and turns a panic into a failing result,
// so one broken check cannot take down the whole doctor run.
func runHealCheckSafely(c healCheck, hc *healContext) (res healResult) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			res = healResult{
				ID: c.ID, Title: c.Title, Category: c.Category, Status: healFail,
				Message: fmt.Sprintf("check crashed: %v", r),
				Hint:    "This is a bug in the check, not in your install. Other checks still ran; include this message in a support bundle.",
			}
		}
		res.DurationMS = time.Since(start).Milliseconds()
	}()
	return c.Run(hc)
}

// verifyHealFix re-runs a check read-only after it reported a fix. A fix only
// counts when the check now passes; otherwise the result shows what is left.
func verifyHealFix(c healCheck, hc *healContext, res healResult) healResult {
	if !res.Fixed || c.Disruptive {
		// Disruptive fixes restart services; the api-server re-checks those
		// after a delay instead.
		return res
	}
	readOnly := *hc
	readOnly.Fix = false
	again := runHealCheckSafely(c, &readOnly)
	ok := again.Status == healOK
	res.Verified = &ok
	if !ok {
		res.Status = again.Status
		res.Message = again.Message + " (a fix was applied, but the check still does not pass)"
		if again.Hint != "" {
			res.Hint = again.Hint
		}
	}
	return res
}

type healHistoryEntry struct {
	At       string `json:"at"`
	ID       string `json:"id"`
	Title    string `json:"title"`
	Category string `json:"category"`
	FixMsg   string `json:"fix_message"`
	Verified *bool  `json:"verified,omitempty"`
	Status   string `json:"status"`
}

func healHistoryPath(installDir string) string {
	return filepath.Join(installDir, "logs", "selfheal-history.jsonl")
}

// readHealHistory returns entries newer than since (all when since is zero),
// oldest first. A missing or partly corrupt file yields what can be read.
func readHealHistory(installDir string, since time.Time) []healHistoryEntry {
	f, err := os.Open(healHistoryPath(installDir))
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []healHistoryEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		var e healHistoryEntry
		if json.Unmarshal(sc.Bytes(), &e) != nil {
			continue
		}
		if !since.IsZero() {
			at, err := time.Parse(time.RFC3339, e.At)
			if err != nil || at.Before(since) {
				continue
			}
		}
		out = append(out, e)
	}
	return out
}

// appendHealHistory records applied fixes. The file is trimmed to its newest
// 2000 entries once it passes 1 MiB.
func appendHealHistory(installDir string, entries []healHistoryEntry) {
	if len(entries) == 0 {
		return
	}
	path := healHistoryPath(installDir)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return
	}
	var buf bytes.Buffer
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			continue
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = f.Write(buf.Bytes())
	_ = f.Close()
	if st, err := os.Stat(path); err == nil && st.Size() > 1<<20 {
		trimHealHistory(path, 2000)
	}
}

func trimHealHistory(path string, keep int) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) <= keep {
		return
	}
	tmp := path + ".tmp"
	if os.WriteFile(tmp, []byte(strings.Join(lines[len(lines)-keep:], "\n")+"\n"), 0o600) == nil {
		_ = os.Rename(tmp, path)
	}
}

// markRecurringFixes sets Fixes24h/Recurring on fixed results from the
// history (before this run's fixes are appended).
func markRecurringFixes(installDir string, results []healResult) {
	recent := readHealHistory(installDir, time.Now().Add(-24*time.Hour))
	counts := map[string]int{}
	for _, e := range recent {
		counts[e.ID]++
	}
	for i := range results {
		if !results[i].Fixed {
			continue
		}
		n := counts[results[i].ID] + 1
		results[i].Fixes24h = n
		if n >= recurringFixThreshold {
			results[i].Recurring = true
			results[i].Hint = strings.TrimSpace(fmt.Sprintf("Fixed %d times in 24 hours: something keeps undoing this repair. %s", n, results[i].Hint))
		}
	}
}

func historyEntriesFor(results []healResult) []healHistoryEntry {
	now := time.Now().UTC().Format(time.RFC3339)
	var out []healHistoryEntry
	for _, r := range results {
		if !r.Fixed {
			continue
		}
		out = append(out, healHistoryEntry{At: now, ID: r.ID, Title: r.Title, Category: r.Category, FixMsg: r.FixMsg, Verified: r.Verified, Status: string(r.Status)})
	}
	return out
}
