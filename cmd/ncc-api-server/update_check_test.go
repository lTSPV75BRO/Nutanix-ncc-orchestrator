package main

import "testing"

func TestParseUpdateCheckJSONPreservesDegradedCheck(t *testing.T) {
	data := map[string]interface{}{}
	ok := parseUpdateCheckJSON(`NCC_UPDATE_JSON {"current_version":"2.2.1","latest_version":"2.3.0","latest_overall":"2.3.0","update_available":true,"has_package":false,"check_degraded":true,"check_error":"GitHub API rate limited"}`, data)
	if !ok {
		t.Fatal("parseUpdateCheckJSON did not recognize the sentinel")
	}
	if got, _ := data["latest_version"].(string); got != "2.3.0" {
		t.Fatalf("latest_version = %q, want 2.3.0", got)
	}
	if got, _ := data["check_degraded"].(bool); !got {
		t.Fatal("check_degraded was not preserved")
	}
	if got, _ := data["check_error"].(string); got == "" {
		t.Fatal("check_error was not preserved")
	}
}
