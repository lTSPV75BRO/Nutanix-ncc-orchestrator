package main

// Typed OpenAPI schemas for the NCC alert lifecycle API (/api/v1/alerts/ncc,
// /events and /{id}). These endpoints are the integration surface for
// third-party tools (ServiceNow, SIEMs, client generators), so unlike most
// routes — which only get a catalog summary — they carry full response
// schemas. openapi_alerts_test.go checks the schemas against the nccAlert and
// nccAlertEvent structs so a new field cannot silently go undocumented.

type oaObj = map[string]interface{}

func oaRef(name string) oaObj { return oaObj{"$ref": "#/components/schemas/" + name} }

func oaStr(desc string) oaObj { return oaObj{"type": "string", "description": desc} }

func oaTime(desc string) oaObj {
	return oaObj{"type": "string", "format": "date-time", "description": desc}
}

func oaEnum(desc string, values ...string) oaObj {
	return oaObj{"type": "string", "enum": values, "description": desc}
}

// oaEnvelope wraps a data schema in the standard {success, data} envelope.
func oaEnvelope(data oaObj) oaObj {
	return oaObj{
		"type":     "object",
		"required": []string{"success", "data"},
		"properties": oaObj{
			"success": oaObj{"type": "boolean", "enum": []bool{true}},
			"data":    data,
		},
	}
}

func oaJSON(desc string, schema oaObj) oaObj {
	return oaObj{"description": desc, "content": oaObj{"application/json": oaObj{"schema": schema}}}
}

func oaQuery(name, desc string, schema oaObj) oaObj {
	return oaObj{"name": name, "in": "query", "required": false, "description": desc, "schema": schema}
}

func nccAlertOpenAPISchemas() oaObj {
	return oaObj{
		"Error": oaObj{
			"type":     "object",
			"required": []string{"success", "error"},
			"properties": oaObj{
				"success":    oaObj{"type": "boolean", "enum": []bool{false}},
				"error":      oaStr("Human-readable error message."),
				"error_code": oaStr("Stable machine-readable code, when one applies (e.g. NCC_API_RATE_LIMITED)."),
			},
		},
		"NCCAlert": oaObj{
			"type":        "object",
			"description": "One NCC alert instance: an episode of a finding. Optional fields are omitted when empty.",
			"required": []string{
				"id", "finding_id", "correlation_key", "source", "type", "cluster", "check",
				"state", "detection", "closed", "created_at", "updated_at", "revision",
			},
			"properties": oaObj{
				"id":                     oaObj{"type": "string", "format": "uuid", "description": "Server-generated UUIDv4 for this instance; never reused."},
				"finding_id":             oaStr("Stable identity of the finding (cluster + normalized check), shared by every instance of it across runs."),
				"correlation_key":        oaStr("Human-readable form of the finding identity."),
				"source":                 oaEnum("Alert source.", "NCC"),
				"type":                   oaEnum("Alert type.", "ncc_check"),
				"cluster":                oaStr("Cluster address as used by the runner."),
				"cluster_name":           oaStr("Cluster display name, when known."),
				"check":                  oaStr("NCC check title."),
				"severity":               oaStr("Severity reported by the latest run that detected it (e.g. FAIL, WARN, ERR, INFO)."),
				"state":                  oaEnum("Operator state.", alertStateOpen, alertStateAcknowledged, alertStateResolved),
				"detection":              oaEnum("Whether the latest run still reports the finding; independent of state.", alertDetected, alertNotDetected, alertUnknown),
				"closed":                 oaObj{"type": "boolean", "description": "True for historical instances; closed instances are only listed with include_closed=true."},
				"closed_at":              oaTime("When the instance was closed."),
				"close_reason":           oaEnum("Why it closed: resolved and confirmed gone, or detected again (see next_id).", "resolved_not_detected", "recurred"),
				"first_seen_at":          oaTime("First time a run reported this instance."),
				"first_seen_run":         oaTime("Timestamp of that run."),
				"last_seen_at":           oaTime("Most recent time a run reported it."),
				"last_seen_run":          oaTime("Timestamp of that run."),
				"not_detected_since_run": oaTime("First run that no longer reported it."),
				"acknowledged_at":        oaTime("When it was acknowledged."),
				"acknowledged_by":        oaStr("Who acknowledged it."),
				"resolved_at":            oaTime("When it was resolved."),
				"resolved_by":            oaStr("Who resolved it."),
				"resolved_run":           oaTime("Latest run at the time it was resolved."),
				"note":                   oaObj{"type": "string", "maxLength": nccDispositionNoteMax, "description": "Operator note from the latest acknowledge/resolve/reopen."},
				"reopened":               oaObj{"type": "boolean", "description": "True when this instance was opened because a resolved instance was detected again."},
				"reopen_reason":          oaStr("Why it was reopened."),
				"previous_id":            oaObj{"type": "string", "format": "uuid", "description": "Earlier instance of the same finding."},
				"next_id":                oaObj{"type": "string", "format": "uuid", "description": "Later instance of the same finding."},
				"created_at":             oaTime("When the instance was created."),
				"updated_at":             oaTime("Last change; also the sort key for paging."),
				"revision":               oaObj{"type": "integer", "minimum": 0, "description": "Increments on every change; use it to detect updates."},
				"legacy_migrated":        oaObj{"type": "boolean", "description": "Imported from the v2.2.0 dispositions file."},
				"last_actor":             oaStr("Who made the latest change (\"NCC Orchestrator\" for run-driven changes)."),
				"last_state_change_at":   oaTime("When state last changed."),
			},
		},
		"NCCAlertEvent": oaObj{
			"type":        "object",
			"description": "One immutable lifecycle event. Events are append-only and ordered by seq.",
			"required":    []string{"id", "alert_id", "correlation_key", "type", "at", "actor", "seq"},
			"properties": oaObj{
				"id":              oaObj{"type": "string", "format": "uuid", "description": "Event UUIDv4."},
				"alert_id":        oaObj{"type": "string", "format": "uuid", "description": "The NCCAlert this event belongs to."},
				"correlation_key": oaStr("Finding identity of that alert."),
				"type": oaEnum("Event type.",
					alertEvtDetected, alertEvtRedetected, alertEvtNotDetected, alertEvtSeverityChanged,
					alertEvtAcknowledged, alertEvtResolved, alertEvtReopened, alertEvtRecurred,
					alertEvtClosed, alertEvtMigrated, alertEvtNoteUpdated),
				"at":            oaTime("When the event was recorded."),
				"actor":         oaStr("User or token subject, or \"NCC Orchestrator\" for run-driven events."),
				"run_at":        oaTime("Run that caused the event, when run-driven."),
				"from_state":    oaEnum("State before the event.", alertStateOpen, alertStateAcknowledged, alertStateResolved),
				"to_state":      oaEnum("State after the event.", alertStateOpen, alertStateAcknowledged, alertStateResolved),
				"from_severity": oaStr("Severity before a severity_changed event."),
				"to_severity":   oaStr("Severity after the event."),
				"note":          oaStr("Operator note attached to the event."),
				"reason":        oaStr("System reason, e.g. for closed or reopened events."),
				"seq":           oaObj{"type": "integer", "format": "int64", "minimum": 1, "description": "Global, strictly increasing sequence number; the events cursor."},
			},
		},
		"NCCAlertInstanceLink": oaObj{
			"type":        "object",
			"description": "Another instance of the same finding. Empty strings mean not set.",
			"required":    []string{"id", "state", "created_at", "closed_at", "close_reason"},
			"properties": oaObj{
				"id":           oaObj{"type": "string", "format": "uuid"},
				"state":        oaEnum("Operator state.", alertStateOpen, alertStateAcknowledged, alertStateResolved),
				"created_at":   oaStr("RFC 3339 timestamp."),
				"closed_at":    oaStr("RFC 3339 timestamp, or empty while open."),
				"close_reason": oaStr("resolved_not_detected, recurred, or empty."),
			},
		},
		"NCCAlertList": oaObj{
			"type":     "object",
			"required": []string{"items", "count", "next_cursor", "last_run"},
			"properties": oaObj{
				"items":       oaObj{"type": "array", "items": oaRef("NCCAlert")},
				"count":       oaObj{"type": "integer", "minimum": 0},
				"next_cursor": oaStr("Pass back as cursor for the next page; empty when there are no more."),
				"last_run":    oaStr("Timestamp of the latest run ingested into the store (RFC 3339, or empty)."),
			},
		},
		"NCCAlertEventPage": oaObj{
			"type":     "object",
			"required": []string{"items", "count", "next_cursor", "has_more"},
			"properties": oaObj{
				"items":       oaObj{"type": "array", "items": oaRef("NCCAlertEvent")},
				"count":       oaObj{"type": "integer", "minimum": 0},
				"next_cursor": oaStr("Highest seq returned (or the cursor you sent); pass it back as cursor to get only newer events."),
				"has_more":    oaObj{"type": "boolean", "description": "True when more events are available right now."},
			},
		},
		"NCCAlertDetail": oaObj{
			"type":     "object",
			"required": []string{"alert", "events", "instances"},
			"properties": oaObj{
				"alert":     oaRef("NCCAlert"),
				"events":    oaObj{"type": "array", "items": oaRef("NCCAlertEvent")},
				"instances": oaObj{"type": "array", "items": oaRef("NCCAlertInstanceLink")},
			},
		},
	}
}

func nccAlertOpenAPIPaths() oaObj {
	errResp := func(desc string) oaObj { return oaJSON(desc, oaRef("Error")) }
	common := oaObj{
		"401": errResp("Missing or invalid credentials."),
		"429": errResp("Rate limited."),
		"500": errResp("The alert store could not be read or updated."),
	}
	with := func(extra oaObj) oaObj {
		out := oaObj{}
		for k, v := range common {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	stateEnum := oaObj{"type": "string", "enum": []string{alertStateOpen, alertStateAcknowledged, alertStateResolved}}
	return oaObj{
		"/api/v1/alerts/ncc": oaObj{"get": oaObj{
			"operationId": "listNCCAlerts",
			"tags":        []string{"NCC alerts"},
			"summary":     "List NCC alert instances",
			"description": "Ordered by (updated_at, id) ascending, so cursor paging is stable while alerts change. Cluster-group scoped.",
			"parameters": []oaObj{
				oaQuery("state", "Filter by operator state.", stateEnum),
				oaQuery("detection", "Filter by detection.", oaObj{"type": "string", "enum": []string{alertDetected, alertNotDetected, alertUnknown}}),
				oaQuery("cluster", "Cluster address or name (case-insensitive).", oaObj{"type": "string"}),
				oaQuery("check", "NCC check title.", oaObj{"type": "string"}),
				oaQuery("finding_id", "Stable finding identity.", oaObj{"type": "string"}),
				oaQuery("updated_since", "Only instances updated at or after this time.", oaObj{"type": "string", "format": "date-time"}),
				oaQuery("include_closed", "Include closed (historical) instances.", oaObj{"type": "boolean", "default": false}),
				oaQuery("limit", "Page size.", oaObj{"type": "integer", "minimum": 1, "maximum": 1000, "default": 100}),
				oaQuery("cursor", "next_cursor from the previous page.", oaObj{"type": "string"}),
			},
			"responses": with(oaObj{
				"200": oaJSON("A page of alert instances.", oaEnvelope(oaRef("NCCAlertList"))),
				"400": errResp("Invalid filter, limit or cursor."),
			}),
		}},
		"/api/v1/alerts/ncc/events": oaObj{"get": oaObj{
			"operationId": "listNCCAlertEvents",
			"tags":        []string{"NCC alerts"},
			"summary":     "Read the append-only NCC alert event feed",
			"description": "Events with seq greater than cursor, in seq order. Store next_cursor and send it back to receive only new events. Cluster-group scoped.",
			"parameters": []oaObj{
				oaQuery("cursor", "next_cursor from the previous response (a seq number); omit to start from the beginning.", oaObj{"type": "string", "pattern": "^[0-9]+$"}),
				oaQuery("alert_id", "Only events for this alert instance.", oaObj{"type": "string", "format": "uuid"}),
				oaQuery("limit", "Page size.", oaObj{"type": "integer", "minimum": 1, "maximum": 5000, "default": 500}),
			},
			"responses": with(oaObj{
				"200": oaJSON("A page of events.", oaEnvelope(oaRef("NCCAlertEventPage"))),
				"400": errResp("Invalid limit or cursor."),
			}),
		}},
		"/api/v1/alerts/ncc/{id}": oaObj{"get": oaObj{
			"operationId": "getNCCAlert",
			"tags":        []string{"NCC alerts"},
			"summary":     "Get one NCC alert instance with its history",
			"parameters": []oaObj{{
				"name": "id", "in": "path", "required": true, "description": "Alert instance UUIDv4.",
				"schema": oaObj{"type": "string", "format": "uuid"},
			}},
			"responses": with(oaObj{
				"200": oaJSON("The instance, its events, and the other instances of the same finding.", oaEnvelope(oaRef("NCCAlertDetail"))),
				"404": errResp("No such alert, or it is outside the caller's cluster groups."),
			}),
		}},
	}
}

// addNCCAlertOpenAPI merges the typed alert paths and their component schemas
// into spec. Call it before the route-catalog gap-fill so these hand-authored
// items win over the generic ones.
func addNCCAlertOpenAPI(spec oaObj, paths oaObj) {
	for p, item := range nccAlertOpenAPIPaths() {
		paths[p] = item
	}
	components, _ := spec["components"].(oaObj)
	if components == nil {
		components = oaObj{}
		spec["components"] = components
	}
	schemas, _ := components["schemas"].(oaObj)
	if schemas == nil {
		schemas = oaObj{}
		components["schemas"] = schemas
	}
	for name, s := range nccAlertOpenAPISchemas() {
		schemas[name] = s
	}
}
