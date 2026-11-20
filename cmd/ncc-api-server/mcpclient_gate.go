package main

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"goncc/internal/mcpclient"
)

// rejectInvalidMCPClient writes 403 when the caller identified as
// ncc-mcp-server but is too old or failed the HMAC proof. Returns true when
// the request must not continue. Browsers and ordinary API clients are left
// alone.
func isMCPHTTPRequest(r *http.Request) bool {
	if r == nil {
		return false
	}
	if mcpclient.SanitizeToolName(r.Header.Get(mcpclient.ToolHeader)) != "" {
		return true
	}
	if strings.TrimSpace(r.Header.Get(mcpclient.ProofHeader)) != "" {
		return true
	}
	_, isMCP := mcpclient.ParseRequestVersion(r.Header.Get(mcpclient.VersionHeader), r.Header.Get("User-Agent"))
	return isMCP
}

func rejectInvalidMCPClient(w http.ResponseWriter, r *http.Request) bool {
	if r == nil {
		return false
	}
	version, isMCP := mcpclient.ParseRequestVersion(r.Header.Get(mcpclient.VersionHeader), r.Header.Get("User-Agent"))
	proof := r.Header.Get(mcpclient.ProofHeader)
	tool := mcpclient.SanitizeToolName(r.Header.Get(mcpclient.ToolHeader))
	if !isMCP && proof == "" && tool == "" {
		return false
	}
	token := extractAPIToken(r)
	err := mcpclient.VerifyProof(token, version, proof, time.Now().UTC())
	if err == nil {
		return false
	}
	code := mcpclient.ProofErrorCode
	msg := mcpclient.InvalidProofMessage()
	if errors.Is(err, mcpclient.ErrUpgrade) || errors.Is(err, mcpclient.ErrMissingToken) {
		code = mcpclient.ErrorCode
		msg = mcpclient.UpgradeMessage()
	}
	writeJSON(w, http.StatusForbidden, envelope{
		Success:     false,
		Error:       msg,
		ErrorCode:   code,
		Remediation: mcpclient.Remediation(),
	})
	return true
}
