package main

import (
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
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

var backupNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}\.tar\.gz(\.enc)?$`)

// safeBackupName accepts only the name of a snapshot kept on the server
// (for example manual-20261120T101500Z.tar.gz.enc), never a path.
func safeBackupName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if !backupNameRe.MatchString(name) || strings.Contains(name, "..") {
		return "", fmt.Errorf("input_file must be the name of a server-side backup (for example manual-<time>.tar.gz.enc), not a path")
	}
	return name, nil
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
