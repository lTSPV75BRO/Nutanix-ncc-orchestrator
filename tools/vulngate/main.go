// Command vulngate fails a build on govulncheck findings, except advisories
// that were reviewed and listed in an accepted file because the Go
// vulnerability database is wrong about them (for example, the fix is
// released upstream but the record still says "Fixed in: N/A").
//
//	govulncheck -format json ./... | go run ./tools/vulngate scripts/govulncheck-accepted.txt
//
// Accepted-file lines: "<OSV id> <module> <review-by YYYY-MM-DD>  # reason".
// The gate fails when:
//   - code calls a vulnerable symbol whose ID isn't listed, or is listed for
//     another module;
//   - a listed entry is past its review-by date (re-check the advisory, then
//     move the date or drop the entry);
//   - the input has no govulncheck output (so a broken scan can't pass).
//
// Entries govulncheck no longer reports are printed as stale, to be removed.
// Findings in imported packages or required modules that the code doesn't
// call are listed for information, the same as govulncheck's own summary.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

type accepted struct {
	module   string
	reviewBy time.Time
	reason   string
	line     int
}

type frame struct {
	Module   string `json:"module"`
	Version  string `json:"version"`
	Package  string `json:"package"`
	Function string `json:"function"`
	Receiver string `json:"receiver"`
	Position *struct {
		Filename string `json:"filename"`
		Line     int    `json:"line"`
	} `json:"position"`
}

type message struct {
	Config *struct {
		ScannerVersion string `json:"scanner_version"`
		DB             string `json:"db"`
	} `json:"config"`
	OSV *struct {
		ID      string `json:"id"`
		Summary string `json:"summary"`
	} `json:"osv"`
	Finding *struct {
		OSV          string  `json:"osv"`
		FixedVersion string  `json:"fixed_version"`
		Trace        []frame `json:"trace"`
	} `json:"finding"`
}

type hit struct {
	id, module, version, fixed, where string
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: govulncheck -format json ./... | vulngate <accepted-file>")
		os.Exit(2)
	}
	acc, err := readAccepted(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "vulngate:", err)
		os.Exit(2)
	}
	code, err := gate(os.Stdin, os.Stdout, acc, time.Now())
	if err != nil {
		fmt.Fprintln(os.Stderr, "vulngate:", err)
		os.Exit(2)
	}
	os.Exit(code)
}

func readAccepted(path string) (map[string]accepted, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]accepted{}
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		n++
		line := sc.Text()
		reason := ""
		if i := strings.Index(line, "#"); i >= 0 {
			reason = strings.TrimSpace(line[i+1:])
			line = line[:i]
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 3 {
			return nil, fmt.Errorf("%s:%d: want \"<id> <module> <review-by YYYY-MM-DD>  # reason\"", path, n)
		}
		by, err := time.Parse("2006-01-02", fields[2])
		if err != nil {
			return nil, fmt.Errorf("%s:%d: review-by date: %v", path, n, err)
		}
		if reason == "" {
			return nil, fmt.Errorf("%s:%d: say why it is accepted after #", path, n)
		}
		out[fields[0]] = accepted{module: fields[1], reviewBy: by, reason: reason, line: n}
	}
	return out, sc.Err()
}

func gate(r io.Reader, w io.Writer, acc map[string]accepted, now time.Time) (int, error) {
	dec := json.NewDecoder(r)
	summaries := map[string]string{}
	called := map[string]hit{}
	notCalled := map[string]hit{}
	sawConfig := false
	for {
		var m message
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, fmt.Errorf("reading govulncheck JSON: %w", err)
		}
		if m.Config != nil {
			sawConfig = true
			fmt.Fprintf(w, "govulncheck %s, database %s\n", m.Config.ScannerVersion, m.Config.DB)
		}
		if m.OSV != nil {
			summaries[m.OSV.ID] = m.OSV.Summary
		}
		if m.Finding == nil || len(m.Finding.Trace) == 0 {
			continue
		}
		f := m.Finding
		top := f.Trace[0]
		h := hit{id: f.OSV, module: top.Module, version: top.Version, fixed: f.FixedVersion}
		if top.Function != "" {
			// Symbol-level finding: the code reaches the vulnerable function.
			last := f.Trace[len(f.Trace)-1]
			if last.Position != nil {
				h.where = fmt.Sprintf("%s:%d", last.Position.Filename, last.Position.Line)
			}
			if _, ok := called[f.OSV]; !ok {
				called[f.OSV] = h
			}
			delete(notCalled, f.OSV)
		} else if _, ok := called[f.OSV]; !ok {
			notCalled[f.OSV] = h
		}
	}
	if !sawConfig {
		return 0, errors.New("no govulncheck output (did the scan run?)")
	}

	failed := false
	for _, id := range sortedKeys(called) {
		h := called[id]
		fixed := h.fixed
		if fixed == "" {
			fixed = "N/A"
		}
		a, ok := acc[id]
		switch {
		case !ok:
			failed = true
			fmt.Fprintf(w, "FAIL   %s %s@%s (fixed in %s): %s\n       called from %s\n", id, h.module, h.version, fixed, summaries[id], h.where)
		case a.module != h.module:
			failed = true
			fmt.Fprintf(w, "FAIL   %s is accepted for %s but found in %s@%s\n", id, a.module, h.module, h.version)
		case now.After(a.reviewBy.Add(24 * time.Hour)):
			failed = true
			fmt.Fprintf(w, "FAIL   %s %s@%s: accepted entry expired on %s; re-check the advisory, then move the date or drop the entry\n", id, h.module, h.version, a.reviewBy.Format("2006-01-02"))
		default:
			fmt.Fprintf(w, "ACCEPT %s %s@%s (database says fixed in %s; review by %s): %s\n", id, h.module, h.version, fixed, a.reviewBy.Format("2006-01-02"), a.reason)
		}
	}
	for _, id := range sortedKeys(notCalled) {
		h := notCalled[id]
		fmt.Fprintf(w, "info   %s %s@%s is required but not called\n", id, h.module, h.version)
	}
	for _, id := range sortedKeys(acc) {
		if _, ok := called[id]; !ok {
			fmt.Fprintf(w, "stale  %s is no longer reported; remove it from the accepted file (line %d)\n", id, acc[id].line)
		}
	}
	if failed {
		fmt.Fprintln(w, "vulnerabilities found: upgrade the module, or (only if the advisory is wrong) review and list it in the accepted file")
		return 3, nil
	}
	fmt.Fprintf(w, "no unaccepted vulnerabilities in called code (%d accepted)\n", len(called))
	return 0, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
