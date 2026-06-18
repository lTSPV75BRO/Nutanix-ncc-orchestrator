package main

import "testing"

func TestNormalizeChecksSnapshot_FlattensClusterShape(t *testing.T) {
	in := map[string]interface{}{
		"timestamp": "2026-06-18T19:24:52Z",
		"clusters": []interface{}{
			map[string]interface{}{
				"address": "10.21.10.206",
				"checks": []interface{}{
					map[string]interface{}{"check_name": "a", "severity": "FAIL"},
					map[string]interface{}{"check_name": "b", "severity": "WARN"},
				},
			},
			map[string]interface{}{
				"address": "10.21.10.208",
				"checks": []interface{}{
					map[string]interface{}{"check_name": "c", "severity": "INFO"},
				},
			},
		},
	}

	got := normalizeChecksSnapshot(in)
	rows, ok := got.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", got)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 flattened rows, got %d", len(rows))
	}
	first, ok := rows[0].(map[string]interface{})
	if !ok {
		t.Fatalf("first row type = %T", rows[0])
	}
	if first["cluster"] != "10.21.10.206" {
		t.Fatalf("expected propagated cluster, got %#v", first["cluster"])
	}
	if first["check_name"] != "a" {
		t.Fatalf("expected first check_name 'a', got %#v", first["check_name"])
	}
}

func TestNormalizeChecksSnapshot_PassThroughFlatList(t *testing.T) {
	in := []interface{}{map[string]interface{}{"cluster": "x", "check_name": "y", "severity": "FAIL"}}
	got := normalizeChecksSnapshot(in)
	rows, ok := got.([]interface{})
	if !ok || len(rows) != 1 {
		t.Fatalf("expected unchanged flat list, got %#v", got)
	}
}
