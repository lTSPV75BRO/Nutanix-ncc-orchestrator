package ai

import (
	"encoding/json"
	"testing"
)

func TestResultUsesVersionedEnvelope(t *testing.T) {
	raw, err := MarshalResult("success", "one finding", Page[Finding]{
		Items: []Finding{{ID: "finding-1", Cluster: "10.0.0.1", Check: "Storage", Severity: "FAIL"}},
	})
	if err != nil {
		t.Fatalf("MarshalResult() error = %v", err)
	}

	var got Result[Page[Finding]]
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if got.SchemaVersion != SchemaVersion || got.Status != "success" {
		t.Fatalf("unexpected envelope: %+v", got)
	}
	if len(got.Data.Items) != 1 || got.Data.Items[0].ID != "finding-1" {
		t.Fatalf("unexpected data: %+v", got.Data)
	}
}

func TestErrorValidation(t *testing.T) {
	err := NewError(CodeConfigInvalid, "validation", "config is invalid").
		WithRemediation("run preflight_check").
		WithRetryable(false)
	if validationErr := err.Validate(); validationErr != nil {
		t.Fatalf("Validate() error = %v", validationErr)
	}
	if err.SchemaVersion != SchemaVersion || err.Code != CodeConfigInvalid {
		t.Fatalf("unexpected error: %+v", err)
	}
}
