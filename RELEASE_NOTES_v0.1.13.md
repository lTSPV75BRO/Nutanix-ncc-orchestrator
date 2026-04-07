# Release notes – v0.1.13

**Release date:** 2026-03-02

This release extends the **NCC MCP server** with new tools and read-only resources so AI assistants (Cursor, Claude Desktop, etc.) can list run artifacts, read report content, and access the latest run summary and report via MCP resources.

---

## New features

### MCP: New tools

- **`list_run_artifacts`**  
  List files in an NCC run output directory (e.g. `run-summary.json`, `index.html`, per-cluster `.log`/`.html`/`.csv`). Optional `output_dir` (default `outputfiles`). Use to discover what reports exist from a previous run.

- **`get_report`**  
  Read the aggregated `index.html` or a specific cluster report file from an output directory. Optional `output_dir`, and `file` (e.g. `index` for `index.html` or `10.0.0.1.html`). Very large reports are truncated (150k chars) to stay within context limits.

### MCP: Resources (read-only)

The MCP server now exposes two **resources** that clients can list and read via `resources/list` and `resources/read`:

| URI | Description |
|-----|-------------|
| **ncc://run-summary** | Latest `run-summary.json` from the default output directory (`outputfiles`). MIME: `application/json`. |
| **ncc://report** | Latest aggregated NCC report (`index.html`) from the default output directory. MIME: `text/html`. Large content may be truncated. |

This allows the AI to pull the latest run summary or report as context without calling a tool.

---

## Documentation

- **docs/MCP_SERVER.md:** Updated with the new tools (`list_run_artifacts`, `get_report`) and the resources table. Added a “Possible future additions” section (e.g. `validate_config`, `get_sample_config`, resource template for arbitrary `output_dir`).
- **MCP tool descriptors:** Added `list_run_artifacts.json` and `get_report.json` for client schema discovery where applicable.

---

## Other changes

- No breaking changes. New MCP tools and resources are additive.
- Orchestrator behavior and CLI are unchanged from v0.1.12.

---

## Upgrade

- **Orchestrator:** No config or flag changes required. Optional: rebuild or pull `ncc-orchestrator:0.1.13` for consistency.
- **MCP server:** Rebuild `ncc-mcp-server` and reload MCP in your client to use the new tools and resources. Existing tools (`run_ncc`, `discover_clusters`, `get_run_summary`, `replay_reports`) are unchanged.

---

## Version / Docker / Checksums

- **Version:** 0.1.13 (from [VERSION](VERSION); default in code when not set via ldflags).
- **Docker:** `prajwalnutant/nutanix-ncc-orchestrator:0.1.13` (and `:latest` when built from main).
- **Kubernetes:** `k8s/cronjob.yaml` and `k8s/job-debug.yaml` reference image tag `0.1.13`.
- Build and test: `go build ./...` and `go test ./...` pass. MCP server builds with `go build -o ncc-mcp-server ./cmd/ncc-mcp-server/`.
- Release maintainers: see [docs/RELEASE_CHECKSUMS.md](docs/RELEASE_CHECKSUMS.md) for checksum generation so `ncc-orchestrator -u` can verify downloads.
