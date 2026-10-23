package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestNCCAlertSchemasMatchStructs fails when a field is added to nccAlert or
// nccAlertEvent without documenting it, or when required no longer matches
// which fields are always serialized.
func TestNCCAlertSchemasMatchStructs(t *testing.T) {
	schemas := nccAlertOpenAPISchemas()
	for name, typ := range map[string]reflect.Type{
		"NCCAlert":      reflect.TypeOf(nccAlert{}),
		"NCCAlertEvent": reflect.TypeOf(nccAlertEvent{}),
	} {
		schema := schemas[name].(oaObj)
		props := schema["properties"].(oaObj)
		var fields, always []string
		for i := 0; i < typ.NumField(); i++ {
			tag := typ.Field(i).Tag.Get("json")
			parts := strings.Split(tag, ",")
			fields = append(fields, parts[0])
			if len(parts) == 1 {
				always = append(always, parts[0])
			}
		}
		var documented []string
		for k := range props {
			documented = append(documented, k)
		}
		required := append([]string(nil), schema["required"].([]string)...)
		sort.Strings(fields)
		sort.Strings(always)
		sort.Strings(documented)
		sort.Strings(required)
		if !reflect.DeepEqual(fields, documented) {
			t.Errorf("%s properties %v do not match struct fields %v", name, documented, fields)
		}
		if !reflect.DeepEqual(always, required) {
			t.Errorf("%s required %v do not match non-omitempty fields %v", name, required, always)
		}
	}
}

func TestOpenAPISpecHasTypedNCCAlertPaths(t *testing.T) {
	spec := (&apiServer{}).buildOpenAPISpec()
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("spec must marshal: %v", err)
	}
	var doc map[string]interface{}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	schemas := doc["components"].(map[string]interface{})["schemas"].(map[string]interface{})
	// Every $ref in the spec must resolve.
	for _, m := range regexp.MustCompile(`"#/components/schemas/([A-Za-z]+)"`).FindAllStringSubmatch(string(raw), -1) {
		if _, ok := schemas[m[1]]; !ok {
			t.Errorf("unresolved $ref %s", m[1])
		}
	}
	paths := doc["paths"].(map[string]interface{})
	for p, want := range map[string]string{
		"/api/v1/alerts/ncc":        "NCCAlertList",
		"/api/v1/alerts/ncc/events": "NCCAlertEventPage",
		"/api/v1/alerts/ncc/{id}":   "NCCAlertDetail",
	} {
		get := paths[p].(map[string]interface{})["get"].(map[string]interface{})
		ok200 := get["responses"].(map[string]interface{})["200"]
		b, _ := json.Marshal(ok200)
		if !strings.Contains(string(b), `"#/components/schemas/`+want+`"`) {
			t.Errorf("%s 200 response should reference %s: %s", p, want, b)
		}
		if get["x-required-role"] != "viewer" {
			t.Errorf("%s: x-required-role = %v, want viewer", p, get["x-required-role"])
		}
	}
}

// TestNCCAlertResponsesConformToSchemas runs the real handlers through a
// resolve → recur lifecycle (so optional fields such as previous_id, next_id,
// close_reason and resolved_* are populated) and validates every response
// against the published schemas, rejecting undeclared fields.
func TestNCCAlertResponsesConformToSchemas(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "outputfiles")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	writeRun := func(ts string) {
		t.Helper()
		sum := fmt.Sprintf(`{"timestamp":%q,"clusters":[{"address":"10.0.0.1","ok":true}]}`, ts)
		rows := `[{"cluster":"10.0.0.1","clusterName":"prod-a","check":"CVM memory","severity":"FAIL"}]`
		if err := os.WriteFile(filepath.Join(out, "run-summary.json"), []byte(sum), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, "index.html"), []byte("const AGG = "+rows+";\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s := &apiServer{repoRoot: dir, outputDir: out}
	do := func(method, path, body string) map[string]interface{} {
		t.Helper()
		var req *http.Request
		if body != "" {
			req = httptest.NewRequest(method, path, bytes.NewBufferString(body))
			req.Header.Set("Content-Type", "application/json")
		} else {
			req = httptest.NewRequest(method, path, nil)
		}
		req = withPrincipal(req, principal{subject: "op", role: RoleOperator})
		rec := httptest.NewRecorder()
		s.handleNCCAlerts(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s %s: %d %s", method, path, rec.Code, rec.Body.String())
		}
		var got map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	writeRun(run1)
	list := do(http.MethodGet, "/api/v1/alerts/ncc", "")
	first := list["data"].(map[string]interface{})["items"].([]interface{})[0].(map[string]interface{})["id"].(string)
	do(http.MethodPost, "/api/v1/alerts/ncc/"+first+"/resolve", `{"note":"fixed"}`)
	writeRun(run2) // still detected → resolved instance closes as recurred, a new one opens

	schemas := nccAlertOpenAPISchemas()
	check := func(name string, body map[string]interface{}) {
		t.Helper()
		if errs := validateOA(oaEnvelope(oaRef(name)), body, schemas, "$"); len(errs) > 0 {
			t.Errorf("%s does not match schema:\n  %s", name, strings.Join(errs, "\n  "))
		}
	}
	check("NCCAlertList", do(http.MethodGet, "/api/v1/alerts/ncc?include_closed=true", ""))
	check("NCCAlertEventPage", do(http.MethodGet, "/api/v1/alerts/ncc/events", ""))
	check("NCCAlertDetail", do(http.MethodGet, "/api/v1/alerts/ncc/"+first, ""))
}

var oaUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// validateOA is a small validator for the subset of OpenAPI used above. It
// also rejects properties the schema does not declare, so the docs cannot
// fall behind the handlers.
func validateOA(schema oaObj, v interface{}, schemas oaObj, at string) []string {
	if ref, ok := schema["$ref"].(string); ok {
		return validateOA(schemas[strings.TrimPrefix(ref, "#/components/schemas/")].(oaObj), v, schemas, at)
	}
	var errs []string
	switch schema["type"] {
	case "object":
		m, ok := v.(map[string]interface{})
		if !ok {
			return []string{at + ": want object"}
		}
		props, _ := schema["properties"].(oaObj)
		req, _ := schema["required"].([]string)
		for _, k := range req {
			if _, ok := m[k]; !ok {
				errs = append(errs, at+"."+k+": required but missing")
			}
		}
		for k, val := range m {
			ps, ok := props[k].(oaObj)
			if !ok {
				errs = append(errs, at+"."+k+": not declared in schema")
				continue
			}
			errs = append(errs, validateOA(ps, val, schemas, at+"."+k)...)
		}
	case "array":
		arr, ok := v.([]interface{})
		if !ok {
			return []string{at + ": want array"}
		}
		for i, el := range arr {
			errs = append(errs, validateOA(schema["items"].(oaObj), el, schemas, fmt.Sprintf("%s[%d]", at, i))...)
		}
	case "integer":
		if f, ok := v.(float64); !ok || f != float64(int64(f)) {
			errs = append(errs, at+": want integer")
		}
	case "boolean":
		b, ok := v.(bool)
		if !ok {
			return []string{at + ": want boolean"}
		}
		if enum, ok := schema["enum"].([]bool); ok && !(len(enum) == 1 && enum[0] == b) {
			errs = append(errs, at+": boolean not in enum")
		}
	case "string":
		s, ok := v.(string)
		if !ok {
			return []string{at + ": want string"}
		}
		if enum, ok := schema["enum"].([]string); ok {
			found := false
			for _, e := range enum {
				found = found || e == s
			}
			if !found {
				errs = append(errs, fmt.Sprintf("%s: %q not in enum %v", at, s, enum))
			}
		}
		switch schema["format"] {
		case "uuid":
			if !oaUUID.MatchString(s) {
				errs = append(errs, fmt.Sprintf("%s: %q is not a uuid", at, s))
			}
		case "date-time":
			if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
				errs = append(errs, fmt.Sprintf("%s: %q is not RFC 3339", at, s))
			}
		}
	}
	return errs
}
