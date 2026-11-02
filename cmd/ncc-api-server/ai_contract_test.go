package main

import (
	"net/http/httptest"
	"testing"
)

func TestParseReportFindingFilters(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/report/data?cluster=prod-a&check=memory&severity=fail", nil)
	got, err := parseReportFindingFilters(req)
	if err != nil {
		t.Fatalf("parse filters: %v", err)
	}
	if got.Cluster != "prod-a" || got.Check != "memory" || got.Severity != "FAIL" {
		t.Fatalf("unexpected filters: %+v", got)
	}
}

func TestFilterReportRows(t *testing.T) {
	rows := []interface{}{
		map[string]interface{}{"cluster": "prod-a", "check_name": "Memory", "severity": "FAIL"},
		map[string]interface{}{"cluster": "prod-b", "check_name": "Memory", "severity": "WARN"},
	}
	got := filterReportRows(rows, reportFindingFilters{Cluster: "prod-a", Severity: "FAIL"})
	filtered := got.([]interface{})
	if len(filtered) != 1 {
		t.Fatalf("expected one filtered row, got %#v", got)
	}
}

func TestParseReportFindingFiltersRejectsUnknownSeverity(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/report/data?severity=critical", nil)
	if _, err := parseReportFindingFilters(req); err == nil {
		t.Fatal("expected invalid severity error")
	}
}

func TestReportPaginationSupportsCursor(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/report/data?cursor=10&limit=2", nil)
	p, err := parseReportDataPagination(req)
	if err != nil || !p.HasCursor || p.CursorOffset != 10 {
		t.Fatalf("unexpected cursor pagination: %+v, %v", p, err)
	}
	rows, meta := paginateAnySlice([]interface{}{1, 2, 3, 4}, p)
	if len(rows) != 0 || meta["next_cursor"] != nil {
		t.Fatalf("cursor beyond test rows should be empty: rows=%v meta=%v", rows, meta)
	}
}
