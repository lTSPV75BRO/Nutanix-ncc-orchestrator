package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"goncc/internal/plural"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// pcListClustersResponse represents a minimal Prism Central v3 clusters/list response.
type pcListClustersResponse struct {
	Entities []map[string]interface{} `json:"entities"`
}

// errDiscoverV4Unavailable signals that the v4 clustermgmt API is not available (e.g. HTTP 404).
type errDiscoverV4Unavailable struct {
	status int
	body   string
}

func (e errDiscoverV4Unavailable) Error() string {
	return fmt.Sprintf("Prism Central clustermgmt v4 returned HTTP %d", e.status)
}

func discoverHTTPClient(insecure bool) *http.Client {
	return &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
	}
}

func normalizePCBaseURL(raw string, insecureSkipVerify bool) (string, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return "", errors.New("pc target is empty")
	}
	if !strings.HasPrefix(target, "https://") && !strings.HasPrefix(target, "http://") {
		target = "https://" + target
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("parse PC target %q: %w", raw, err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return "", fmt.Errorf("PC target must use http or https scheme, got %q", u.Scheme)
	}
	if u.Scheme == "http" && !insecureSkipVerify {
		return "", errors.New("PC target with http:// requires --insecure-skip-verify")
	}
	if strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("PC target %q has no host", raw)
	}
	hostname := u.Hostname()
	if err := validateClusterAddress(hostname); err != nil {
		return "", fmt.Errorf("PC host %q: %w", hostname, err)
	}
	port := u.Port()
	if strings.TrimSpace(port) == "" {
		port = strconv.Itoa(prismGatewayPort)
	}
	clean := url.URL{
		Scheme: u.Scheme,
		Host:   net.JoinHostPort(hostname, port),
	}
	return strings.TrimSuffix(clean.String(), "/"), nil
}

func discoverClustersFromPCTargets(cfg Config) ([]string, error) {
	apiVersion := strings.ToLower(strings.TrimSpace(cfg.DiscoverAPIVersion))
	if apiVersion == "" {
		apiVersion = defaultDiscoverAPIVersion
	}
	if apiVersion != "v3" && apiVersion != "v4" {
		return nil, fmt.Errorf("discover-api-version must be v3 or v4, got %q", apiVersion)
	}

	seen := make(map[string]bool)
	out := make([]string, 0)
	for _, pc := range cfg.PCs {
		pcURL, err := normalizePCBaseURL(pc, cfg.InsecureSkipVerify)
		if err != nil {
			return nil, err
		}
		var discovered []string
		if apiVersion == "v4" {
			discovered, err = fetchPCClustersV4(pcURL, cfg.Username, cfg.Password, cfg.InsecureSkipVerify, cfg.NutanixV4APIVersion)
			if err != nil {
				var v4Unavailable errDiscoverV4Unavailable
				if errors.As(err, &v4Unavailable) {
					discovered, err = fetchPCClustersV3(pcURL, cfg.Username, cfg.Password, cfg.InsecureSkipVerify)
				}
			}
		} else {
			discovered, err = fetchPCClustersV3(pcURL, cfg.Username, cfg.Password, cfg.InsecureSkipVerify)
		}
		if err != nil {
			return nil, fmt.Errorf("discover clusters from PC %s: %w", pcURL, err)
		}
		for _, addr := range discovered {
			addr = strings.TrimSpace(addr)
			if addr == "" || seen[addr] {
				continue
			}
			seen[addr] = true
			out = append(out, addr)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no clusters discovered from provided PCs")
	}
	return out, nil
}

// fetchPCClustersV3 lists clusters via legacy POST /api/nutanix/v3/clusters/list.
func fetchPCClustersV3(pcURL, username, password string, insecure bool) ([]string, error) {
	u := pcURL + "/api/nutanix/v3/clusters/list"
	body := []byte(`{}`)
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(username, password)

	client := discoverHTTPClient(insecure)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to Prism Central: %w", err)
	}
	defer resp.Body.Close()
	data, err := readBounded(resp.Body, maxDiscoveryResponseBytes)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP %d from Prism Central: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
	}

	var list pcListClustersResponse
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	var addresses []string
	for _, e := range list.Entities {
		addr := extractClusterAddressV3(e)
		if addr != "" {
			addresses = append(addresses, addr)
		}
	}
	return addresses, nil
}

// fetchPCClustersV4 lists clusters via GET /api/clustermgmt/{ver}/config/clusters with $page / $limit pagination.
func fetchPCClustersV4(pcURL, username, password string, insecure bool, v4APIVer string) ([]string, error) {
	ver := nutanixV4PathSegment(v4APIVer)
	base := strings.TrimSuffix(pcURL, "/") + "/api/clustermgmt/" + ver + "/config/clusters"
	client := discoverHTTPClient(insecure)
	const pageSize = 100
	seen := make(map[string]bool)
	var out []string

	for page := 0; page < 1000; page++ {
		u, err := url.Parse(base)
		if err != nil {
			return nil, fmt.Errorf("parse URL: %w", err)
		}
		q := url.Values{}
		q.Set("$limit", strconv.Itoa(pageSize))
		q.Set("$page", strconv.Itoa(page))
		u.RawQuery = q.Encode()

		req, err := http.NewRequest(http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(username, password)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request to Prism Central: %w", err)
		}
		data, err := readBounded(resp.Body, maxDiscoveryResponseBytes)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil, errDiscoverV4Unavailable{status: resp.StatusCode, body: string(data)}
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected HTTP %d from Prism Central: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
		}

		var payload struct {
			Metadata struct {
				TotalAvailableResults int `json:"totalAvailableResults"`
			} `json:"metadata"`
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("parse response: %w", err)
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, e := range payload.Data {
			addr := extractClusterAddressV4(e)
			if addr != "" && !seen[addr] {
				seen[addr] = true
				out = append(out, addr)
			}
		}
		total := payload.Metadata.TotalAvailableResults
		if total > 0 && len(out) >= total {
			break
		}
		// Next page until empty; do not stop on len(data) < $limit (server may cap below our limit).
	}
	return out, nil
}

// DiscoverClusterRow is one registered cluster from discover-clusters (table/json output).
type DiscoverClusterRow struct {
	Name    string `json:"name"`
	ExtID   string `json:"ext_id"`
	Address string `json:"address"`
	API     string `json:"api"`
}

func discoverRowFromV4Entity(e map[string]interface{}) DiscoverClusterRow {
	if e == nil {
		return DiscoverClusterRow{}
	}
	name, _ := e["name"].(string)
	return DiscoverClusterRow{
		Name:    strings.TrimSpace(name),
		ExtID:   extractClusterExtIDV4(e),
		Address: extractClusterAddressV4(e),
		API:     "v4",
	}
}

func extractClusterExtIDV3(e map[string]interface{}) string {
	if m, _ := e["metadata"].(map[string]interface{}); m != nil {
		if u, _ := m["uuid"].(string); strings.TrimSpace(u) != "" {
			return strings.TrimSpace(u)
		}
	}
	return ""
}

func discoverRowFromV3Entity(e map[string]interface{}) DiscoverClusterRow {
	if e == nil {
		return DiscoverClusterRow{}
	}
	name := ""
	if m, _ := e["metadata"].(map[string]interface{}); m != nil {
		if n, _ := m["name"].(string); strings.TrimSpace(n) != "" {
			name = strings.TrimSpace(n)
		}
	}
	if name == "" {
		if spec, _ := e["spec"].(map[string]interface{}); spec != nil {
			if n, _ := spec["name"].(string); strings.TrimSpace(n) != "" {
				name = strings.TrimSpace(n)
			}
		}
	}
	return DiscoverClusterRow{
		Name:    name,
		ExtID:   extractClusterExtIDV3(e),
		Address: extractClusterAddressV3(e),
		API:     "v3",
	}
}

// fetchDiscoverClusterRowsV4 returns cluster rows from GET clustermgmt v4 (paginated).
func fetchDiscoverClusterRowsV4(pcURL, username, password string, insecure bool, v4APIVer string) ([]DiscoverClusterRow, error) {
	ver := nutanixV4PathSegment(v4APIVer)
	base := strings.TrimSuffix(pcURL, "/") + "/api/clustermgmt/" + ver + "/config/clusters"
	client := discoverHTTPClient(insecure)
	const pageSize = 100
	seen := make(map[string]bool)
	var out []DiscoverClusterRow

	for page := 0; page < 1000; page++ {
		u, err := url.Parse(base)
		if err != nil {
			return nil, fmt.Errorf("parse URL: %w", err)
		}
		q := url.Values{}
		q.Set("$limit", strconv.Itoa(pageSize))
		q.Set("$page", strconv.Itoa(page))
		u.RawQuery = q.Encode()

		req, err := http.NewRequest(http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(username, password)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request to Prism Central: %w", err)
		}
		data, err := readBounded(resp.Body, maxDiscoveryResponseBytes)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil, errDiscoverV4Unavailable{status: resp.StatusCode, body: string(data)}
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected HTTP %d from Prism Central: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
		}

		var payload struct {
			Metadata struct {
				TotalAvailableResults int `json:"totalAvailableResults"`
			} `json:"metadata"`
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("parse response: %w", err)
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, e := range payload.Data {
			row := discoverRowFromV4Entity(e)
			key := row.ExtID
			if key == "" {
				key = row.Address + row.Name
			}
			if key != "" && !seen[key] {
				seen[key] = true
				out = append(out, row)
			}
		}
		total := payload.Metadata.TotalAvailableResults
		if total > 0 && len(out) >= total {
			break
		}
	}
	return out, nil
}

// fetchDiscoverClusterRowsV3 returns cluster rows from POST v3 clusters/list.
func fetchDiscoverClusterRowsV3(pcURL, username, password string, insecure bool) ([]DiscoverClusterRow, error) {
	u := pcURL + "/api/nutanix/v3/clusters/list"
	body := []byte(`{}`)
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(username, password)

	client := discoverHTTPClient(insecure)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to Prism Central: %w", err)
	}
	defer resp.Body.Close()
	data, err := readBounded(resp.Body, maxDiscoveryResponseBytes)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP %d from Prism Central: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
	}

	var list pcListClustersResponse
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	var out []DiscoverClusterRow
	for _, e := range list.Entities {
		out = append(out, discoverRowFromV3Entity(e))
	}
	return out, nil
}

// runDiscoverClusters lists clusters from Prism Central (default v4 GET clustermgmt; optional v3 or v4→v3 fallback).
func runDiscoverClusters(cmd *cobra.Command, args []string) error {
	// Load config file if set (so prism-central-url, username, etc. can come from config)
	if cfgFile := viper.GetString("config"); cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		_ = viper.ReadInConfig()
	}
	pcURL := strings.TrimSuffix(strings.TrimSpace(viper.GetString("prism-central-url")), "/")
	if pcURL == "" {
		return exitConfig(errors.New("prism-central-url is required (flag or config)"))
	}
	if !strings.HasPrefix(pcURL, "https://") && !strings.HasPrefix(pcURL, "http://") {
		pcURL = "https://" + pcURL
	}
	// Discover defines its own username/password/insecure flags; those must NOT be
	// viper.BindPFlag'd to the same keys as the root command (the last bind wins and
	// breaks the main ncc-orchestrator run). Merge: flag when set, else viper/config.
	username := viper.GetString("username")
	if cmd.Flags().Changed("username") {
		username, _ = cmd.Flags().GetString("username")
	}
	if strings.TrimSpace(username) == "" {
		username = "admin"
	}
	password := viper.GetString("password")
	if cmd.Flags().Changed("password") {
		password, _ = cmd.Flags().GetString("password")
	}
	if password == "" {
		password = os.Getenv("NCC_PASSWORD")
	}
	if password == "" {
		var err error
		password, err = promptPasswordIfEmpty("", username)
		if err != nil {
			return err
		}
	}
	insecure := viper.GetBool("insecure-skip-verify")
	if cmd.Flags().Changed("insecure-skip-verify") {
		insecure, _ = cmd.Flags().GetBool("insecure-skip-verify")
	}

	discoverVer, _ := cmd.Flags().GetString("discover-api-version")
	discoverVer = strings.ToLower(strings.TrimSpace(discoverVer))
	if discoverVer == "" {
		discoverVer = strings.ToLower(strings.TrimSpace(viper.GetString("discover-api-version")))
	}
	if discoverVer == "" {
		discoverVer = "v4"
	}
	if discoverVer != "v3" && discoverVer != "v4" {
		return exitConfig(fmt.Errorf("discover-api-version must be v3 or v4, got %q", discoverVer))
	}
	if discoverVer == "v4" {
		if err := validateNutanixV4APIVersion(viper.GetString("nutanix-v4-api-version")); err != nil {
			return exitConfig(fmt.Errorf("nutanix-v4-api-version: %w", err))
		}
	}

	format, _ := cmd.Flags().GetString("format")
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "lines"
	}
	switch format {
	case "lines", "table", "json":
	default:
		return exitConfig(fmt.Errorf("format must be lines, table, or json, got %q", format))
	}

	var rows []DiscoverClusterRow
	var err error
	if discoverVer == "v4" {
		rows, err = fetchDiscoverClusterRowsV4(pcURL, username, password, insecure, viper.GetString("nutanix-v4-api-version"))
		if err != nil {
			var v4un errDiscoverV4Unavailable
			if errors.As(err, &v4un) && v4un.status == http.StatusNotFound {
				fmt.Fprintf(os.Stderr, "discover-clusters: v4 API not found (%d), falling back to v3\n", v4un.status)
				rows, err = fetchDiscoverClusterRowsV3(pcURL, username, password, insecure)
			}
		}
	} else {
		rows, err = fetchDiscoverClusterRowsV3(pcURL, username, password, insecure)
	}
	if err != nil {
		return err
	}

	outPath, _ := cmd.Flags().GetString("output")
	writeOut := func(content string) error {
		if outPath != "" {
			if err := os.WriteFile(outPath, []byte(content), 0644); err != nil {
				return fmt.Errorf("write %s: %w", outPath, err)
			}
			fmt.Fprintf(os.Stderr, "Wrote %s to %s\n", plural.Count(len(rows), "cluster"), outPath)
			return nil
		}
		fmt.Print(content)
		return nil
	}

	switch format {
	case "json":
		b, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		return writeOut(string(b) + "\n")
	case "table":
		var buf strings.Builder
		tw := tabwriter.NewWriter(&buf, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "NAME\tEXT_ID\tADDRESS\tAPI")
		for _, r := range rows {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.Name, r.ExtID, r.Address, r.API)
		}
		_ = tw.Flush()
		return writeOut(buf.String())
	default: // lines
		var lines []string
		for _, r := range rows {
			if r.Address != "" {
				lines = append(lines, r.Address)
			} else if r.Name != "" {
				lines = append(lines, r.Name)
			}
		}
		return writeOut(strings.Join(lines, "\n") + "\n")
	}
}

// extractClusterAddressV4 returns a reachable cluster address from clustermgmt v4 config cluster JSON.
func extractClusterAddressV4(entity map[string]interface{}) string {
	if netw, _ := entity["network"].(map[string]interface{}); netw != nil {
		if ext, _ := netw["externalAddress"].(map[string]interface{}); ext != nil {
			if ipv4, _ := ext["ipv4"].(map[string]interface{}); ipv4 != nil {
				if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
			if ipv6, _ := ext["ipv6"].(map[string]interface{}); ipv6 != nil {
				if v, _ := ipv6["value"].(string); strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
		}
	}
	if nodes, _ := entity["nodes"].(map[string]interface{}); nodes != nil {
		if list, _ := nodes["nodeList"].([]interface{}); len(list) > 0 {
			if first, _ := list[0].(map[string]interface{}); first != nil {
				if cvm, _ := first["controllerVmIp"].(map[string]interface{}); cvm != nil {
					if ipv4, _ := cvm["ipv4"].(map[string]interface{}); ipv4 != nil {
						if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
							return strings.TrimSpace(v)
						}
					}
				}
				if host, _ := first["hostIp"].(map[string]interface{}); host != nil {
					if ipv4, _ := host["ipv4"].(map[string]interface{}); ipv4 != nil {
						if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
							return strings.TrimSpace(v)
						}
					}
				}
			}
		}
	}
	if name, _ := entity["name"].(string); strings.TrimSpace(name) != "" {
		return strings.TrimSpace(name)
	}
	return ""
}

// extractCVMIPv4sFromClusterEntity returns controller VM IPv4 addresses from a clustermgmt v4 cluster entity.
func extractCVMIPv4sFromClusterEntity(entity map[string]interface{}) []string {
	nodes, _ := entity["nodes"].(map[string]interface{})
	if nodes == nil {
		return nil
	}
	list, _ := nodes["nodeList"].([]interface{})
	var out []string
	for _, raw := range list {
		node, _ := raw.(map[string]interface{})
		if node == nil {
			continue
		}
		cvm, _ := node["controllerVmIp"].(map[string]interface{})
		if cvm == nil {
			continue
		}
		if ipv4, _ := cvm["ipv4"].(map[string]interface{}); ipv4 != nil {
			if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
				out = append(out, strings.TrimSpace(v))
			}
		}
	}
	return out
}

func extractClusterExtIDV4(entity map[string]interface{}) string {
	if extID, _ := entity["extId"].(string); strings.TrimSpace(extID) != "" {
		return strings.TrimSpace(extID)
	}
	return ""
}

func dedupeStringsKeepOrder(in []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// clusterEntityMatchesUserRef reports whether the user's --clusters value refers to this
// registered cluster (name, extId, external address, or any CVM IP). Used so Prism Central
// does not always use data[0] (wrong cluster when multiple are registered).
func clusterEntityMatchesUserRef(ref string, entity map[string]interface{}) bool {
	want := canonicalClusterRef(ref)
	if want == "" || entity == nil {
		return false
	}
	candidates := []string{}
	if name, _ := entity["name"].(string); strings.TrimSpace(name) != "" {
		candidates = append(candidates, name)
	}
	if extID, _ := entity["extId"].(string); strings.TrimSpace(extID) != "" {
		candidates = append(candidates, extID)
	}
	if uuid, _ := entity["uuid"].(string); strings.TrimSpace(uuid) != "" {
		candidates = append(candidates, uuid)
	}
	if netw, _ := entity["network"].(map[string]interface{}); netw != nil {
		if fqdn, _ := netw["fqdn"].(string); strings.TrimSpace(fqdn) != "" {
			candidates = append(candidates, fqdn)
		}
	}
	if a := extractClusterAddressV4(entity); a != "" {
		candidates = append(candidates, a)
	}
	candidates = append(candidates, extractCVMIPv4sFromClusterEntity(entity)...)
	for _, c := range candidates {
		if canonicalClusterRef(c) == want {
			return true
		}
	}
	return false
}

// canonicalClusterRef folds a URL, host:port, urn:uuid, or braced UUID into the
// host or id operators actually type. Some Prism clusters are addressed as
// https://name:9440 while the API record only has the IP, name, or extId.
func canonicalClusterRef(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	lower := strings.ToLower(s)
	lower = strings.TrimPrefix(lower, "urn:uuid:")
	lower = strings.Trim(lower, "{}")
	if strings.Contains(lower, "://") {
		if u, err := url.Parse(lower); err == nil {
			if h := strings.TrimSpace(u.Hostname()); h != "" {
				lower = h
			}
		}
	}
	if host, _, err := net.SplitHostPort(lower); err == nil && host != "" {
		lower = strings.Trim(host, "[]")
	} else if i := strings.LastIndex(lower, ":"); i > 0 && strings.Count(lower, ":") == 1 {
		if _, err := strconv.Atoi(lower[i+1:]); err == nil {
			lower = lower[:i]
		}
	}
	return strings.Trim(strings.TrimSpace(lower), "[]")
}

// extractClusterAddressV3 extracts external IP or name from a Prism Central cluster entity (v3).
func extractClusterAddressV3(entity map[string]interface{}) string {
	// spec.resources.network.external_ip or external_ip_address
	if spec, _ := entity["spec"].(map[string]interface{}); spec != nil {
		if res, _ := spec["resources"].(map[string]interface{}); res != nil {
			if netw, _ := res["network"].(map[string]interface{}); netw != nil {
				if ip, _ := netw["external_ip"].(string); ip != "" {
					return ip
				}
				if ip, _ := netw["external_ip_address"].(string); ip != "" {
					return ip
				}
			}
		}
	}
	// status.resources.network.external_ip
	if status, _ := entity["status"].(map[string]interface{}); status != nil {
		if res, _ := status["resources"].(map[string]interface{}); res != nil {
			if netw, _ := res["network"].(map[string]interface{}); netw != nil {
				if ip, _ := netw["external_ip"].(string); ip != "" {
					return ip
				}
			}
		}
		if name, _ := status["name"].(string); name != "" {
			return name
		}
	}
	return ""
}
