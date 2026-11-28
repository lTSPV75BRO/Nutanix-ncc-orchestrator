package main

import (
	"bufio"
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"goncc/internal/nccparse"

	"github.com/rs/zerolog/log"
)

// ==================== API Types ====================

type TaskStatus struct {
	PercentageComplete int    `json:"percentage_complete"`
	ProgressStatus     string `json:"progress_status"`
}

type NCCSummary struct {
	RunSummary string `json:"runSummary"`
}

// ==================== Parser ====================
// The NCC summary parser was extracted to goncc/internal/nccparse. These
// aliases keep the existing call sites in this package unchanged. splitLines is
// also used by non-parser code here, so it is aliased too.
var (
	splitLines                               = nccparse.SplitLines
	ParseSummary                             = nccparse.ParseSummary
	validateParsedAlertsAgainstPluginResults = nccparse.ValidateParsedAlertsAgainstPluginResults
)

type Row struct {
	Severity  string
	CheckName string
	Detail    template.HTML
}

func parseNCCHeader(path string) (HTMLMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return HTMLMeta{}, err
	}
	defer f.Close()

	var meta HTMLMeta
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		applyNCCHeaderLine(&meta, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return HTMLMeta{}, err
	}
	return meta, nil
}

// parseNCCHeaderText is parseNCCHeader for a summary already in memory.
func parseNCCHeaderText(text string) HTMLMeta {
	var meta HTMLMeta
	for _, line := range strings.Split(text, "\n") {
		applyNCCHeaderLine(&meta, line)
	}
	return meta
}

func applyNCCHeaderLine(meta *HTMLMeta, raw string) {
	line := strings.TrimSpace(raw)
	switch {
	case strings.HasPrefix(line, "Cluster Name:"):
		meta.ClusterName = strings.TrimSpace(strings.TrimPrefix(line, "Cluster Name:"))
	case strings.HasPrefix(line, "Cluster Version:"):
		meta.ClusterVersion = strings.TrimSpace(strings.TrimPrefix(line, "Cluster Version:"))
	case strings.HasPrefix(line, "NCC Version:"):
		meta.NCCVersion = strings.TrimSpace(strings.TrimPrefix(line, "NCC Version:"))
	}
}

// ==================== NCC Client ====================

type NCCClient struct {
	baseURL    string
	cluster    string // host:port or IP for building v4 API URL
	user       string
	pass       string
	http       HTTPClient
	cfg        Config
	apiVersion string // "v1" or "v4"
	// taskExtID is the full Nutanix extId from start-checks v4 (e.g. "base:uuid"); used for GET /api/prism/{ver}/config/tasks/{extId}.
	taskExtID string
}

func NewNCCClient(cluster, user, pass string, httpc HTTPClient, cfg Config) *NCCClient {
	apiVer := strings.ToLower(strings.TrimSpace(cfg.NCCAPIVersion))
	if apiVer == "" {
		apiVer = "v4"
	}
	return &NCCClient{
		baseURL:    fmt.Sprintf("https://%s:9440/PrismGateway/services/rest", cluster),
		cluster:    cluster,
		user:       user,
		pass:       pass,
		http:       httpc,
		cfg:        cfg,
		apiVersion: apiVer,
	}
}

// generateRequestID returns a UUID v4-style string for NTNX-Request-Id header.
func generateRequestID() (string, error) {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// resolveClusterV4ForNCC finds the clustermgmt entity matching c.cluster (IP, name, extId, or CVM IP),
// returns its extId and CVM IPv4s for run-system-defined-checks. Prism Central often registers multiple
// clusters (e.g. AOS + PC); using only data[0] paired with nodeIps={c.cluster} caused NCC-40023 when
// the first cluster was not the one the user addressed.
func (c *NCCClient) resolveClusterV4ForNCC(ctx context.Context) (clusterExtID string, nodeIPv4s []string, err error) {
	ref := strings.TrimSpace(c.cluster)
	if ref == "" {
		return "", nil, errors.New("cluster address is empty")
	}
	ver := nutanixV4PathSegment(c.cfg.NutanixV4APIVersion)
	base := fmt.Sprintf("https://%s:9440/api/clustermgmt/%s/config/clusters", c.cluster, ver)
	const pageSize = 100
	var scanned int

	for page := 0; page < 1000; page++ {
		u, err := url.Parse(base)
		if err != nil {
			return "", nil, fmt.Errorf("parse URL: %w", err)
		}
		q := url.Values{}
		q.Set("$limit", strconv.Itoa(pageSize))
		q.Set("$page", strconv.Itoa(page))
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return "", nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(c.user, c.pass)
		resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get cluster uuid v4")
		if err != nil {
			log.Error().Err(err).Str("url", u.String()).Msg("get cluster uuid v4 failed")
			return "", nil, fmt.Errorf("get cluster uuid v4: %w", err)
		}
		_ = resp

		var payload struct {
			Metadata struct {
				TotalAvailableResults int `json:"totalAvailableResults"`
			} `json:"metadata"`
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return "", nil, fmt.Errorf("parse cluster list response: %w", err)
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, entity := range payload.Data {
			scanned++
			if entity == nil {
				continue
			}
			if !clusterEntityMatchesUserRef(ref, entity) {
				continue
			}
			extID := extractClusterExtIDV4(entity)
			if extID == "" {
				return "", nil, errors.New("matched cluster entity missing extId")
			}
			nodeIPs := extractCVMIPv4sFromClusterEntity(entity)
			if len(nodeIPs) == 0 {
				if a := extractClusterAddressV4(entity); a != "" {
					nodeIPs = []string{a}
				}
			}
			return extID, dedupeStringsKeepOrder(nodeIPs), nil
		}
		total := payload.Metadata.TotalAvailableResults
		if total > 0 && scanned >= total {
			break
		}
	}

	return "", nil, fmt.Errorf("no cluster registered in Prism matches %q (use an IP, name, or extId from discover-clusters)", ref)
}

func (c *NCCClient) StartChecks(ctx context.Context) (string, []byte, error) {
	if c.apiVersion == "v4" {
		taskID, body, err := c.startChecksV4(ctx)
		if err != nil && strings.Contains(err.Error(), "404") {
			log.Info().Str("cluster", c.cluster).Msg("v4 endpoint not available (404), falling back to v1")
			return c.startChecksV1(ctx)
		}
		if err != nil {
			return "", body, err
		}
		return taskID, body, nil
	}
	return c.startChecksV1(ctx)
}

func (c *NCCClient) startChecksV1(ctx context.Context) (string, []byte, error) {
	url := c.baseURL + "/v1/ncc/checks"
	payload := []byte(`{"sendEmail":false}`)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "start checks")
	if err != nil {
		log.Error().Err(err).Str("url", url).Str("method", "POST").Msg("http do error")
		return "", body, fmt.Errorf("start checks: %w", err)
	}
	_ = resp
	log.Debug().Str("url", url).RawJSON("body", body).Msg("start checks response")

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", body, err
	}
	uuid, _ := data["taskUuid"].(string)
	if uuid == "" {
		if alt, ok := data["task_uuid"].(string); ok && alt != "" {
			uuid = alt
		}
	}
	if uuid == "" {
		return "", body, errors.New("missing taskUuid in response")
	}
	return uuid, body, nil
}

// v4RunChecksRequest is the body for monitoring v4 run-system-defined-checks.
type v4RunChecksRequest struct {
	ShouldAnonymize                        bool       `json:"shouldAnonymize"`
	ShouldSendReportToConfiguredRecipients bool       `json:"shouldSendReportToConfiguredRecipients"`
	AdditionalRecipients                   []string   `json:"additionalRecipients"`
	NodeIps                                []v4NodeIP `json:"nodeIps"`
	ShouldRunAllChecks                     bool       `json:"shouldRunAllChecks"`
}

type v4NodeIP struct {
	Value        string `json:"value"`
	PrefixLength int    `json:"prefixLength"`
}

func (c *NCCClient) startChecksV4(ctx context.Context) (string, []byte, error) {
	clusterUUID, nodeIPs, err := c.resolveClusterV4ForNCC(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("get cluster uuid for v4: %w", err)
	}
	if len(nodeIPs) == 0 {
		nodeIPs = []string{c.cluster}
	}
	var nodeIpsReq []v4NodeIP
	for _, ip := range nodeIPs {
		nodeIpsReq = append(nodeIpsReq, v4NodeIP{Value: ip, PrefixLength: 32})
	}
	requestID, err := generateRequestID()
	if err != nil {
		return "", nil, fmt.Errorf("generate request id: %w", err)
	}
	ver := nutanixV4PathSegment(c.cfg.NutanixV4APIVersion)
	v4Base := fmt.Sprintf("https://%s:9440/api/monitoring/%s/serviceability/clusters/%s/$actions/run-system-defined-checks", c.cluster, ver, clusterUUID)
	// Prism Central v4 requires additionalRecipients to have at least one element matching email regex (user@domain.tld); use placeholder when not sending report
	additionalRecipients := []string{"noreply@example.com"}
	body := v4RunChecksRequest{
		ShouldAnonymize:                        false,
		ShouldSendReportToConfiguredRecipients: false,
		AdditionalRecipients:                   additionalRecipients,
		NodeIps:                                nodeIpsReq,
		ShouldRunAllChecks:                     true,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", v4Base, bytes.NewReader(payload))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("NTNX-Request-Id", requestID)
	req.SetBasicAuth(c.user, c.pass)

	resp, respBody, err := doWithRetry(ctx, c.http, req, c.cfg, "start checks v4")
	if err != nil {
		log.Error().Err(err).Str("url", v4Base).Str("method", "POST").Msg("http do error")
		return "", respBody, fmt.Errorf("start checks v4: %w", err)
	}
	_ = resp
	log.Debug().Str("url", v4Base).RawJSON("body", respBody).Msg("start checks v4 response")

	var data map[string]interface{}
	if err := json.Unmarshal(respBody, &data); err != nil {
		return "", respBody, err
	}
	c.taskExtID = ""
	var taskID string
	if dataObj, _ := data["data"].(map[string]interface{}); dataObj != nil {
		if extID, _ := dataObj["extId"].(string); extID != "" {
			c.taskExtID = extID
			if idx := strings.LastIndex(extID, ":"); idx >= 0 && idx+1 < len(extID) {
				taskID = extID[idx+1:]
			} else {
				taskID = extID
			}
		}
	}
	if taskID == "" {
		taskID, _ = data["taskUuid"].(string)
	}
	if taskID == "" {
		if alt, ok := data["task_uuid"].(string); ok && alt != "" {
			taskID = alt
		}
	}
	if taskID == "" {
		if extID, ok := data["executionId"].(string); ok && extID != "" {
			taskID = extID
		}
	}
	if taskID == "" {
		return "", respBody, errors.New("v4 response missing taskUuid or executionId")
	}
	return taskID, respBody, nil
}

// mapPrismTaskJSONToTaskStatus maps Prism v4 GET .../api/prism/{ver}/config/tasks/{extId} JSON to legacy TaskStatus for the poll loop.
func mapPrismTaskJSONToTaskStatus(raw []byte) (TaskStatus, error) {
	var wrap struct {
		Data struct {
			Status             string `json:"status"`
			ProgressPercentage int    `json:"progressPercentage"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return TaskStatus{}, err
	}
	st := strings.ToUpper(strings.TrimSpace(wrap.Data.Status))
	pct := wrap.Data.ProgressPercentage
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	var ps string
	switch st {
	case "SUCCEEDED", "SUCCESS", "COMPLETED", "DONE", "COMPLETE", "SUCCEED":
		ps = "Succeeded"
		if pct < 100 {
			pct = 100
		}
	case "FAILED", "FAILURE", "ABORTED", "CANCELED", "CANCELLED", "ERROR":
		ps = "Failed"
	default:
		ps = "Running"
	}
	return TaskStatus{PercentageComplete: pct, ProgressStatus: ps}, nil
}

func (c *NCCClient) getTaskPrismV4(ctx context.Context, extID string) (TaskStatus, []byte, error) {
	ver := nutanixV4PathSegment(c.cfg.NutanixV4APIVersion)
	u := fmt.Sprintf("https://%s:9440/api/prism/%s/config/tasks/%s", c.cluster, ver, url.PathEscape(extID))
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return TaskStatus{}, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get task prism v4")
	if err != nil {
		log.Error().Err(err).Str("url", u).Msg("http do error")
		return TaskStatus{}, body, fmt.Errorf("get task prism v4: %w", err)
	}
	_ = resp
	log.Debug().Str("url", u).RawJSON("body", body).Msg("get task prism v4 response")

	st, err := mapPrismTaskJSONToTaskStatus(body)
	if err != nil {
		return TaskStatus{}, body, fmt.Errorf("parse prism task response: %w", err)
	}
	return st, body, nil
}

func (c *NCCClient) getTaskV2Legacy(ctx context.Context, taskID string) (TaskStatus, []byte, error) {
	url := c.baseURL + "/v2.0/tasks/" + taskID
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return TaskStatus{}, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get task")
	if err != nil {
		log.Error().Err(err).Str("url", url).Msg("http do error")
		return TaskStatus{}, body, fmt.Errorf("get task: %w", err)
	}
	_ = resp
	log.Debug().Str("url", url).RawJSON("body", body).Msg("get task response")

	var status TaskStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return TaskStatus{}, body, err
	}
	return status, body, nil
}

func (c *NCCClient) GetTask(ctx context.Context, taskID string) (TaskStatus, []byte, error) {
	if c.apiVersion == "v4" && c.taskExtID != "" {
		st, body, err := c.getTaskPrismV4(ctx, c.taskExtID)
		if err == nil {
			return st, body, nil
		}
		if strings.Contains(err.Error(), "404") {
			log.Info().Str("cluster", c.cluster).Msg("prism v4 task API unavailable (404), falling back to Prism Gateway v2.0/tasks")
			return c.getTaskV2Legacy(ctx, taskID)
		}
		return TaskStatus{}, body, err
	}
	return c.getTaskV2Legacy(ctx, taskID)
}

func (c *NCCClient) GetRunSummary(ctx context.Context, taskID string) (NCCSummary, []byte, error) {
	url := c.baseURL + "/v1/ncc/" + taskID
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return NCCSummary{}, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get summary")
	if err != nil {
		log.Error().Err(err).Str("url", url).Msg("http do error")
		return NCCSummary{}, body, fmt.Errorf("get summary: %w", err)
	}
	_ = resp
	log.Debug().Str("url", url).RawJSON("body", body).Msg("get summary response")

	var summary NCCSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return NCCSummary{}, body, err
	}
	return summary, body, nil
}
