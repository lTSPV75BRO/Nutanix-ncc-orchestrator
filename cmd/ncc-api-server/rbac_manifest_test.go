package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// manifestDocs decodes every YAML document in data.
func manifestDocs(t *testing.T, name string, data []byte) []map[string]interface{} {
	t.Helper()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var docs []map[string]interface{}
	for {
		var doc map[string]interface{}
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if doc != nil {
			docs = append(docs, doc)
		}
	}
	return docs
}

// Every plain manifest in k8s/ must parse.
func TestK8sManifestsParse(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "k8s", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no manifests found: %v", err)
	}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, doc := range manifestDocs(t, f, data) {
			if doc["kind"] == nil {
				t.Errorf("%s document %d has no kind", f, i)
			}
		}
	}
}

type rbacRule struct {
	Resource      string
	ResourceNames string
	Verbs         string
}

// apiRoleRules flattens the Roles in an RBAC manifest. The runner CronJob is
// named differently by Helm and Kustomize, so its name is normalised.
func apiRoleRules(t *testing.T, name string, data []byte) map[string][]rbacRule {
	t.Helper()
	out := map[string][]rbacRule{}
	for _, doc := range manifestDocs(t, name, data) {
		if doc["kind"] != "Role" {
			continue
		}
		role := doc["metadata"].(map[string]interface{})["name"].(string)
		for _, r := range doc["rules"].([]interface{}) {
			rule := r.(map[string]interface{})
			join := func(key string) string {
				s := asList(rule[key])
				sort.Strings(s)
				return strings.Join(s, ",")
			}
			names := join("resourceNames")
			if names == "ncc-orchestrator" || names == "ncc-v2-runner" {
				names = "<runner-cronjob>"
			}
			out[role] = append(out[role], rbacRule{Resource: join("resources"), ResourceNames: names, Verbs: join("verbs")})
		}
	}
	return out
}

func asList(v interface{}) []string {
	var s []string
	if l, ok := v.([]interface{}); ok {
		for _, x := range l {
			s = append(s, x.(string))
		}
	}
	return s
}

// The Helm chart and k8s/ grant the API server the same narrow rules, and
// only what its Kubernetes calls need (k8ssecret.go, k8sschedule.go).
func TestAPIServerRBACIsNarrowAndConsistent(t *testing.T) {
	plain, err := os.ReadFile(filepath.Join("..", "..", "k8s", "rbac.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	chart, err := os.ReadFile(filepath.Join("..", "..", "helm", "ncc-orchestrator", "templates", "rbac.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	chart = bytes.ReplaceAll(chart, []byte("{{ .Values.namespace }}"), []byte("ncc"))
	want := map[string][]rbacRule{
		"ncc-v2-users-secret": {
			{Resource: "secrets", ResourceNames: "ncc-v2-users", Verbs: "get,patch"},
			{Resource: "secrets", ResourceNames: "", Verbs: "create"},
		},
		"ncc-v2-runner-cronjob": {
			{Resource: "cronjobs", ResourceNames: "<runner-cronjob>", Verbs: "get,patch"},
		},
	}
	for name, data := range map[string][]byte{"k8s/rbac.yaml": plain, "helm rbac.yaml": chart} {
		if got := apiRoleRules(t, name, data); !reflect.DeepEqual(got, want) {
			t.Errorf("%s rules = %+v, want %+v", name, got, want)
		}
	}
}
