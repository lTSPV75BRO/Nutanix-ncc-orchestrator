package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Service definitions that an upgrade migrates: which ones changed, which were
// edited on this host, and how to put them back.

// savedDefinition is a definition as it was before a migration.
type savedDefinition struct {
	ID      string `json:"id"` // "file:/etc/..." or "task:name"
	Body    string `json:"body,omitempty"`
	Missing bool   `json:"missing,omitempty"`
}

// Actions of a defChange.
const (
	defUnchanged = "unchanged"
	defReplace   = "replace" // as we generated it; replaced by the new form
	defMerge     = "merge"   // replaced, with this host's own lines kept in a drop-in
	defKeep      = "keep"    // edited in a way that cannot be merged; left alone
	defCreate    = "create"
)

// defChange is one definition and what a migration does with it.
type defChange struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Action string `json:"action"`
	Detail string `json:"detail,omitempty"`

	body   string // new content
	dropIn string // content of the drop-in (merge)
}

func (c defChange) pending() bool {
	return c.Action == defReplace || c.Action == defMerge || c.Action == defCreate
}

func (c defChange) String() string {
	s := c.Title + ": " + c.Action
	if c.Detail != "" {
		s += " (" + c.Detail + ")"
	}
	return s
}

func pendingChanges(changes []defChange) (n int) {
	for _, c := range changes {
		if c.pending() {
			n++
		}
	}
	return n
}

const (
	fileDefPrefix = "file:"
	taskDefPrefix = "task:"
	// unitDropInName holds lines of a unit that somebody added by hand.
	unitDropInName = "50-local.conf"
)

func unitDropInPath(unitPath string) string { return filepath.Join(unitPath+".d", unitDropInName) }

// saveFileDefinition reads a definition file for a later restore.
func saveFileDefinition(path string) (savedDefinition, error) {
	d := savedDefinition{ID: fileDefPrefix + path}
	b, err := os.ReadFile(path)
	switch {
	case os.IsNotExist(err):
		d.Missing = true
	case err != nil:
		return d, err
	default:
		d.Body = string(b)
	}
	return d, nil
}

// restoreFileDefinition puts a saved definition file back.
func restoreFileDefinition(d savedDefinition) error {
	path := strings.TrimPrefix(d.ID, fileDefPrefix)
	if d.Missing {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		if strings.HasSuffix(filepath.Dir(path), ".d") {
			_ = os.Remove(filepath.Dir(path)) // our drop-in folder, if now empty
		}
		return nil
	}
	if b, err := os.ReadFile(path); err == nil && string(b) == d.Body {
		return nil
	}
	return writeDefinitionFile(path, d.Body)
}

// writeDefinitionFile replaces a definition atomically.
func writeDefinitionFile(path, body string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { // #nosec G301 -- systemd/launchd configuration is world-readable
		return err
	}
	staged, err := stageBytes([]byte(body), path, 0o644)
	if err != nil {
		return err
	}
	if err := renameWithRetry(staged, path); err != nil {
		_ = os.Remove(staged)
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// unitLine is a directive with the section it is in.
type unitLine struct{ section, text string }

func (l unitLine) key() string {
	k, _, _ := strings.Cut(l.text, "=")
	return strings.TrimSpace(k)
}

// unitLines lists the directives of a systemd unit; comments and white space
// do not matter, and continued lines count as one.
func unitLines(unit string) []unitLine {
	var out []unitLine
	section, pending := "", ""
	for _, raw := range strings.Split(strings.ReplaceAll(unit, "\r\n", "\n"), "\n") {
		l := strings.TrimSpace(raw)
		if l != "" && (l[0] == '#' || l[0] == ';') {
			continue // systemd skips comments inside a continuation too
		}
		if l == "" && pending == "" {
			continue
		}
		if rest, more := strings.CutSuffix(l, "\\"); more {
			pending += strings.TrimSpace(rest) + " "
			continue
		}
		l = strings.TrimSpace(pending + l)
		pending = ""
		if strings.HasPrefix(l, "[") && strings.HasSuffix(l, "]") {
			section = l
			continue
		}
		out = append(out, unitLine{section, l})
	}
	return out
}

// unitIdentityKeys decide what a unit runs and as whom. A host that changed one
// of them runs something of its own, which must not be rewritten.
var unitIdentityKeys = map[string]bool{
	"ExecStart": true, "Type": true, "User": true, "Group": true, "DynamicUser": true,
	"RootDirectory": true, "RootImage": true, "WorkingDirectory": true,
}

// planUnit decides how an installed unit becomes the desired one. Lines that
// this program never wrote move to a drop-in, which wins over the unit.
func planUnit(title, path, installed, desired string) defChange {
	c := defChange{ID: fileDefPrefix + path, Title: title, body: desired}
	if installed == "" {
		c.Action = defCreate
		return c
	}
	want := map[unitLine]bool{}
	for _, l := range unitLines(desired) {
		want[l] = true
	}
	var extra []unitLine
	have := map[unitLine]bool{}
	for _, l := range unitLines(installed) {
		if want[l] {
			have[l] = true
		} else if !retiredUnitLines[l.text] {
			extra = append(extra, l)
		}
	}
	if len(extra) == 0 {
		c.Action = defReplace
		if strings.ReplaceAll(installed, "\r\n", "\n") == desired {
			c.Action = defUnchanged
		} else if len(have) == len(want) {
			c.Action = defUnchanged
		}
		return c
	}
	var names []string
	for _, l := range extra {
		names = append(names, l.key())
		if unitIdentityKeys[l.key()] || (l.section != "[Service]" && l.section != "[Unit]") {
			c.Action = defKeep
		}
	}
	sort.Strings(names)
	list := strings.Join(names, ", ")
	if c.Action == defKeep {
		c.Detail = "changed on this host: " + list
		return c
	}
	c.Action = defMerge
	c.Detail = "this host's " + list + " kept in " + unitDropInName
	var b strings.Builder
	b.WriteString("# Kept from " + filepath.Base(path) + " by an ncc-orchestrator upgrade: lines added on this host.\n")
	for _, section := range []string{"[Unit]", "[Service]"} {
		header := false
		for _, l := range extra {
			if l.section != section {
				continue
			}
			if !header {
				b.WriteString(section + "\n")
				header = true
			}
			b.WriteString(l.text + "\n")
		}
	}
	c.dropIn = b.String()
	return c
}

// retiredUnitLines were written by an earlier release and are dropped without
// asking. (Every line of 2.2.1 and 2.3.0 is still written today.)
var retiredUnitLines = map[string]bool{}

// applyUnit writes what planUnit decided.
func applyUnit(c defChange) error {
	if !c.pending() {
		return nil
	}
	path := strings.TrimPrefix(c.ID, fileDefPrefix)
	if c.Action == defMerge {
		dropIn := unitDropInPath(path)
		if b, err := os.ReadFile(dropIn); err == nil {
			// Lines of an earlier merge (or of this host) come first.
			c.dropIn = strings.TrimRight(string(b), "\n") + "\n\n" + c.dropIn
		}
		if err := writeDefinitionFile(dropIn, c.dropIn); err != nil {
			return fmt.Errorf("%s: %w", dropIn, err)
		}
	}
	if err := writeDefinitionFile(path, c.body); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}
