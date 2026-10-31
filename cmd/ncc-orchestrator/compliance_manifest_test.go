package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The NIST CSF evidence manifest is audit evidence: every path it cites must
// exist, every ID must be a CSF 2.0 subcategory, and an "Implemented" control
// must point at something. Files moved in a refactor used to drop out of the
// evidence bundle silently; this keeps the manifest true.
func TestNISTEvidenceManifestIsValid(t *testing.T) {
	root := filepath.Join("..", "..")
	b, err := os.ReadFile(filepath.Join(root, "docs", "compliance", "NIST_CSF_EVIDENCE_MANIFEST.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Framework  string   `json:"framework"`
		ProfileDoc string   `json:"profile_doc"`
		Related    []string `json:"related"`
		Controls   []struct {
			ID       string   `json:"id"`
			Function string   `json:"function"`
			Status   string   `json:"status"`
			Evidence []string `json:"evidence"`
		} `json:"controls"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("manifest is not valid JSON: %v", err)
	}
	if m.Framework != "NIST CSF 2.0" {
		t.Errorf("framework = %q, want NIST CSF 2.0", m.Framework)
	}
	exists := func(rel string) bool {
		_, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
		return err == nil
	}
	for _, p := range append([]string{m.ProfileDoc}, m.Related...) {
		if !exists(p) {
			t.Errorf("manifest references missing document %s", p)
		}
	}
	// CSF 2.0 categories; 1.1-only codes such as PR.PT or RS.RP are rejected.
	idRe := regexp.MustCompile(`^(GV\.(OC|RM|RR|PO|OV|SC)|ID\.(AM|RA|IM)|PR\.(AA|AT|DS|PS|IR)|DE\.(CM|AE)|RS\.(MA|AN|CO|MI)|RC\.(RP|CO))-\d{2}$`)
	seen := map[string]bool{}
	for _, c := range m.Controls {
		if !idRe.MatchString(c.ID) {
			t.Errorf("%s is not a NIST CSF 2.0 subcategory ID", c.ID)
		}
		if seen[c.ID] {
			t.Errorf("%s listed twice", c.ID)
		}
		seen[c.ID] = true
		if !strings.HasPrefix(c.ID, c.Function+".") {
			t.Errorf("%s: function %q does not match the ID", c.ID, c.Function)
		}
		switch c.Status {
		case "Implemented", "Partial", "Planned":
		default:
			t.Errorf("%s: status %q, want Implemented, Partial or Planned", c.ID, c.Status)
		}
		if c.Status == "Implemented" && len(c.Evidence) == 0 {
			t.Errorf("%s is Implemented but cites no evidence", c.ID)
		}
		for _, ev := range c.Evidence {
			if !exists(ev) {
				t.Errorf("%s: evidence %s does not exist", c.ID, ev)
			}
		}
	}
	for _, fn := range []string{"GV", "ID", "PR", "DE", "RS", "RC"} {
		found := false
		for id := range seen {
			if strings.HasPrefix(id, fn+".") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no control covers CSF function %s", fn)
		}
	}
}
