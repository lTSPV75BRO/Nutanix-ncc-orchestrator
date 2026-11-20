package main

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"unicode"
)

func joinAPIURL(base *url.URL, apiPath string) (*url.URL, error) {
	if base == nil {
		return nil, fmt.Errorf("API URL is not set")
	}
	apiPath = strings.TrimSpace(apiPath)
	if !strings.HasPrefix(apiPath, "/api/") {
		return nil, fmt.Errorf("MCP may only call /api/ routes")
	}
	if strings.Contains(apiPath, "://") || strings.Contains(apiPath, "..") || strings.Contains(apiPath, `\`) {
		return nil, fmt.Errorf("invalid API path")
	}
	for _, r := range apiPath {
		if r < 32 || r == 127 || unicode.IsSpace(r) {
			return nil, fmt.Errorf("invalid API path")
		}
	}
	u, err := url.Parse(strings.TrimRight(base.String(), "/") + apiPath)
	if err != nil {
		return nil, err
	}
	if u.Scheme != base.Scheme || u.Host != base.Host || u.User != nil {
		return nil, fmt.Errorf("API path must stay on the configured API host")
	}
	return u, nil
}

func safeArtifactName(file string) (string, error) {
	file = strings.TrimSpace(file)
	if file == "" || strings.EqualFold(file, "index") {
		return "index.html", nil
	}
	if filepath.Base(file) != file || strings.ContainsAny(file, `/\`) || strings.Contains(file, "..") {
		return "", fmt.Errorf("file must be a basename inside the API artifacts directory")
	}
	for _, r := range file {
		if r < 32 || r == 127 || unicode.IsSpace(r) {
			return "", fmt.Errorf("file must be a basename inside the API artifacts directory")
		}
	}
	return file, nil
}

func safeRunID(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" || len(id) > 128 || strings.Contains(id, "..") || strings.ContainsAny(id, `/\`) {
		return "", fmt.Errorf("invalid run id")
	}
	for _, r := range id {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '_' || r == '-' || r == ':') {
			return "", fmt.Errorf("invalid run id")
		}
	}
	return id, nil
}

func safeHTTPSEndpoint(raw, field string) (string, error) {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("%s must be an absolute https URL", field)
	}
	if !strings.EqualFold(u.Scheme, "https") || u.User != nil || u.Fragment != "" {
		return "", fmt.Errorf("%s must be https without credentials or fragment", field)
	}
	return u.String(), nil
}
