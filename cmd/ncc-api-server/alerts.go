package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"goncc/internal/httpclient"
	"goncc/internal/model"
)

const defaultPCAlertsCacheTTL = 5 * time.Minute

type pcAlertsCacheEntry struct {
	alerts         []map[string]interface{}
	errors         []string
	fetchedAt      time.Time
	resolvedFilter string
}

type pcAlertResponse struct {
	Metadata struct {
		TotalAvailableResults int `json:"totalAvailableResults"`
	} `json:"metadata"`
	Data []map[string]interface{} `json:"data"`
}

func (s *apiServer) handleAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}

	cfg, err := s.loadRawConfigMap()
	cacheTTL := s.pcAlertsCacheTTL
	if cacheTTL <= 0 {
		cacheTTL = defaultPCAlertsCacheTTL
	}
	if err == nil {
		cacheTTL = rawDuration(cfg, "pc-alerts-cache-ttl", cacheTTL)
	}
	forceRefresh := r.URL.Query().Get("refresh") == "1" || strings.EqualFold(r.URL.Query().Get("refresh"), "true")
	resolvedFilter := normalizeResolvedFilter(r.URL.Query().Get("resolved"))

	s.pcAlertsMu.Lock()
	if !forceRefresh && cacheTTL > 0 && s.pcAlertsCache != nil &&
		s.pcAlertsCache.resolvedFilter == resolvedFilter &&
		time.Since(s.pcAlertsCache.fetchedAt) < cacheTTL {
		cached := *s.pcAlertsCache
		s.pcAlertsMu.Unlock()
		s.writeAlertsResponse(w, r, cached.alerts, cached.errors, cached.fetchedAt, true, cacheTTL)
		return
	}
	s.pcAlertsMu.Unlock()

	if err != nil {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"alerts": []map[string]interface{}{},
			"errors": []string{fmt.Sprintf("load config: %v", err)},
		}})
		return
	}
	targets := pcAlertTargets(cfg)
	if len(targets) == 0 {
		now := time.Now().UTC()
		s.writeAlertsResponse(w, r, []map[string]interface{}{}, []string{"no Prism Central targets configured"}, now, false, cacheTTL)
		return
	}

	clientCfg := model.Config{
		Username:           rawString(cfg, "username"),
		Password:           resolveAlertSecret(rawString(cfg, "password")),
		InsecureSkipVerify: rawBool(cfg, "insecure-skip-verify"),
		CABundle:           rawString(cfg, "ca-bundle"),
		RequestTimeout:     rawDuration(cfg, "request-timeout", 30*time.Second),
		LogHTTP:            rawBool(cfg, "log-http"),
	}
	client := httpclient.New(clientCfg)
	alerts := make([]map[string]interface{}, 0)
	fetchErrors := make([]string, 0)
	apiVersion := rawString(cfg, "nutanix-v4-api-version")
	if apiVersion == "" {
		apiVersion = "v4.2"
	}
	type targetResult struct {
		target string
		rows   []map[string]interface{}
		err    error
	}
	results := make(chan targetResult, len(targets))
	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			rows, fetchErr := fetchPCAlerts(client, target, apiVersion, clientCfg.Username, clientCfg.Password, resolvedFilter)
			results <- targetResult{target: target, rows: rows, err: fetchErr}
		}(target)
	}
	wg.Wait()
	close(results)
	resultByTarget := make(map[string]targetResult, len(targets))
	for result := range results {
		resultByTarget[result.target] = result
	}
	for _, target := range targets {
		result := resultByTarget[target]
		if result.err != nil {
			fetchErrors = append(fetchErrors, fmt.Sprintf("%s: %v", target, result.err))
			continue
		}
		alerts = append(alerts, result.rows...)
	}

	now := time.Now().UTC()
	s.pcAlertsMu.Lock()
	s.pcAlertsCache = &pcAlertsCacheEntry{
		alerts:         alerts,
		errors:         fetchErrors,
		fetchedAt:      now,
		resolvedFilter: resolvedFilter,
	}
	s.pcAlertsMu.Unlock()
	s.writeAlertsResponse(w, r, alerts, fetchErrors, now, false, cacheTTL)
}

func (s *apiServer) writeAlertsResponse(w http.ResponseWriter, r *http.Request, alerts []map[string]interface{}, fetchErrors []string, fetchedAt time.Time, cacheHit bool, cacheTTL time.Duration) {
	p, _ := principalFromContext(r.Context())
	access := s.allowedClusters(p)
	filtered := make([]map[string]interface{}, 0, len(alerts))
	for _, alert := range alerts {
		cluster := strings.TrimSpace(fmt.Sprint(alert["cluster"]))
		if !access.unrestricted && cluster != "" && !access.permits(cluster) {
			continue
		}
		filtered = append(filtered, alert)
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"alerts":      filtered,
		"source":      "PC",
		"fetched_at":  fetchedAt.Format(time.RFC3339),
		"cache_hit":   cacheHit,
		"cache_ttl_s": int(cacheTTL / time.Second),
		"errors":      fetchErrors,
		"configured":  len(alerts) > 0 || len(fetchErrors) == 0,
	}})
}

func normalizeResolvedFilter(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "yes", "true":
		return "Yes"
	case "all":
		return "all"
	default:
		return "No"
	}
}

func fetchPCAlerts(client *http.Client, target, apiVersion, username, password, resolvedFilter string) ([]map[string]interface{}, error) {
	base, err := pcAlertsURL(target, apiVersion)
	if err != nil {
		return nil, err
	}
	all := make([]map[string]interface{}, 0)
	for page := 0; page < 100; page++ {
		u, err := url.Parse(base)
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("$page", strconv.Itoa(page))
		q.Set("$limit", "100")
		q.Set("$orderby", "lastUpdatedTime desc")
		switch resolvedFilter {
		case "Yes":
			q.Set("$filter", "isResolved eq true")
		case "No":
			q.Set("$filter", "isResolved eq false")
		}
		u.RawQuery = q.Encode()

		var response pcAlertResponse
		if err := doPCAlertRequest(client, u.String(), username, password, &response); err != nil {
			return nil, err
		}
		for _, raw := range response.Data {
			all = append(all, normalizePCAlert(raw))
		}
		if len(response.Data) < 100 && len(all) >= response.Metadata.TotalAvailableResults {
			break
		}
	}
	return all, nil
}

func doPCAlertRequest(client *http.Client, endpoint, username, password string, out *pcAlertResponse) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(username, password)
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
		} else {
			bodyErr := json.NewDecoder(resp.Body).Decode(out)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 && bodyErr == nil {
				return nil
			}
			if bodyErr != nil {
				lastErr = bodyErr
			} else {
				lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			}
			if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
				return lastErr
			}
		}
		if attempt < 2 {
			time.Sleep(time.Duration(1<<attempt) * 100 * time.Millisecond)
		}
	}
	return lastErr
}

func pcAlertsURL(raw, apiVersion string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("empty Prism Central URL")
	}
	apiVersion = strings.TrimSpace(apiVersion)
	if apiVersion == "" {
		apiVersion = "v4.2"
	}
	if !strings.HasPrefix(apiVersion, "v") || strings.ContainsAny(apiVersion, "/?#") {
		return "", fmt.Errorf("invalid Nutanix v4 API version %q", apiVersion)
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("invalid Prism Central URL %q", raw)
	}
	if u.Port() == "" {
		u.Host = net.JoinHostPort(u.Hostname(), "9440")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/api/monitoring/" + apiVersion + "/serviceability/alerts"
	u.RawQuery = ""
	return u.String(), nil
}

func normalizePCAlert(raw map[string]interface{}) map[string]interface{} {
	severity := strings.ToUpper(strings.TrimSpace(fmt.Sprint(raw["severity"])))
	switch severity {
	case "CRITICAL":
		severity = "FAIL"
	case "WARNING":
		severity = "WARN"
	case "INFO":
	default:
		severity = "UNKNOWN"
	}
	entityName := firstAlertString(raw, "entityName", "entity_name", "sourceEntityName")
	entityType := firstAlertString(raw, "entityType", "entity_type")
	if source, ok := raw["sourceEntity"].(map[string]interface{}); ok {
		if entityName == "" {
			entityName = firstAlertString(source, "name", "extId")
		}
		if entityType == "" {
			entityType = firstAlertString(source, "type", "entityType")
		}
	}
	cluster := firstAlertString(raw, "clusterName", "clusterUUID")
	if cluster == "" {
		if source, ok := raw["sourceEntity"].(map[string]interface{}); ok {
			cluster = firstAlertString(source, "name", "extId")
		}
	}
	title := firstAlertString(raw, "title", "name", "alertType")
	detail := firstAlertString(raw, "message", "rootCauseAnalysis")
	return map[string]interface{}{
		"source":        "PC",
		"cluster":       cluster,
		"cluster_name":  cluster,
		"check":         title,
		"check_name":    title,
		"alert":         title,
		"entity_name":   entityName,
		"entity_type":   entityType,
		"severity":      severity,
		"detail":        detail,
		"alert_type":    firstAlertString(raw, "alertType"),
		"status":        firstAlertString(raw, "status"),
		"impact_type":   firstAlertString(raw, "primaryImpactType", "impactType", "impact_type"),
		"created_at":    firstAlertString(raw, "creationTime"),
		"last_occurred": firstAlertString(raw, "lastOccurredTime", "lastUpdatedTime", "creationTime"),
		"updated_at":    firstAlertString(raw, "lastUpdatedTime"),
		"resolved_at":   firstAlertString(raw, "resolvedTime"),
		"auto_resolved": raw["isAutoResolved"],
		"acknowledged":  raw["isAcknowledged"],
		"resolved":      raw["isResolved"],
		"kb_articles":   raw["kbArticles"],
	}
}

func firstAlertString(raw map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := raw[key]; ok {
			if text := strings.TrimSpace(fmt.Sprint(value)); text != "" && text != "<nil>" {
				return text
			}
		}
	}
	return ""
}

func pcAlertTargets(cfg map[string]interface{}) []string {
	targets := make([]string, 0)
	if value, ok := cfg["pcs"]; ok {
		switch values := value.(type) {
		case []interface{}:
			for _, item := range values {
				if text := strings.TrimSpace(fmt.Sprint(item)); text != "" {
					targets = append(targets, text)
				}
			}
		case string:
			for _, item := range strings.Split(values, ",") {
				if text := strings.TrimSpace(item); text != "" {
					targets = append(targets, text)
				}
			}
		}
	}
	if len(targets) == 0 {
		if target := rawString(cfg, "prism-central-url"); target != "" {
			targets = append(targets, target)
		}
	}
	return targets
}

func rawString(cfg map[string]interface{}, key string) string {
	return strings.TrimSpace(fmt.Sprint(cfg[key]))
}

func rawBool(cfg map[string]interface{}, key string) bool {
	value, _ := strconv.ParseBool(rawString(cfg, key))
	return value
}

func rawDuration(cfg map[string]interface{}, key string, fallback time.Duration) time.Duration {
	raw := rawString(cfg, key)
	if raw == "" {
		return fallback
	}
	if value, err := time.ParseDuration(raw); err == nil {
		return value
	}
	return fallback
}

func resolveAlertSecret(value string) string {
	if !strings.HasPrefix(value, "secret://") {
		return value
	}
	name := strings.TrimPrefix(value, "secret://")
	return os.Getenv(name)
}
