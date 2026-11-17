// Package readguard holds a source check: every io.ReadAll must read through
// a size limit, so a large or hostile response (Prism Central, LDAP, update
// server, webhook) cannot exhaust memory. It has no runtime code.
package readguard

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Directories scanned, relative to this package.
var roots = []string{"../../cmd", "../../internal", "../../tools"}

// optOut lets a call through when its line, or the line above it, says why
// the read is already bounded, e.g. "// bounded-read: small fixed-size file".
const optOut = "bounded-read:"

// TestReadAllIsBounded fails on io.ReadAll / ioutil.ReadAll calls whose
// argument is not wrapped in io.LimitReader or http.MaxBytesReader.
func TestReadAllIsBounded(t *testing.T) {
	var problems []string
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if name := d.Name(); name == "testdata" || name == "node_modules" || strings.HasPrefix(name, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			problems = append(problems, unboundedReads(t, path)...)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	for _, p := range problems {
		t.Errorf("%s: io.ReadAll without a size limit; wrap the reader in io.LimitReader (or http.MaxBytesReader), or add a %q comment saying why it is bounded", p, optOut)
	}
}

func unboundedReads(t *testing.T, path string) []string {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	lines := strings.Split(string(src), "\n")
	var out []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isCall(call.Fun, "io", "ReadAll") && !isCall(call.Fun, "ioutil", "ReadAll") || len(call.Args) != 1 {
			return true
		}
		if limited(call.Args[0]) {
			return true
		}
		line := fset.Position(call.Pos()).Line
		if hasOptOut(lines, line) {
			return true
		}
		out = append(out, fset.Position(call.Pos()).String())
		return true
	})
	return out
}

// limited reports whether the reader expression is a size-limiting wrapper.
func limited(e ast.Expr) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	return isCall(call.Fun, "io", "LimitReader") || isCall(call.Fun, "http", "MaxBytesReader")
}

func isCall(fun ast.Expr, pkg, name string) bool {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg
}

func hasOptOut(lines []string, line int) bool {
	for _, l := range []int{line - 1, line - 2} { // lines are 1-based
		if l >= 0 && l < len(lines) && strings.Contains(lines[l], optOut) {
			return true
		}
	}
	return false
}

// TestGuardCatchesUnboundedRead keeps the check honest.
func TestGuardCatchesUnboundedRead(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.go")
	src := "package x\n\nimport (\n\t\"io\"\n\t\"net/http\"\n)\n\nfunc f(r *http.Response) {\n\t_, _ = io.ReadAll(r.Body)\n\t_, _ = io.ReadAll(io.LimitReader(r.Body, 10))\n\t// bounded-read: test\n\t_, _ = io.ReadAll(r.Body)\n}\n"
	if err := os.WriteFile(bad, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	got := unboundedReads(t, bad)
	if len(got) != 1 || !strings.HasSuffix(got[0], ":9:9") {
		t.Fatalf("want one finding at line 9, got %v", got)
	}
}
