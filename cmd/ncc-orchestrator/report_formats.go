package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"path/filepath"
	"strings"
	"time"
)

// ==================== Report Renderers ====================

// func generateHTMLNoMeta(fs FS, rows []Row, filename string) error {
// 	return generateHTML(fs, rows, filename, HTMLMeta{})
// }

// htmlNowForReport supplies the "Generated" timestamp for per-cluster HTML reports.
// Tests may replace it with a fixed clock for golden-file stability.
var htmlNowForReport = func() time.Time { return time.Now() }

func generateHTML(fs FS, rows []Row, filename string, meta HTMLMeta) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	const tmpl = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NCC Report - {{.Meta.ClusterName}}</title>
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMzIiIGhlaWdodD0iMzIiIHZpZXdCb3g9IjAgMCAzMiAzMiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3QgeD0iMiIgeT0iMiIgd2lkdGg9IjI4IiBoZWlnaHQ9IjI4IiByeD0iOCIgZmlsbD0iIzBCMTIyMCIvPgo8cGF0aCBkPSJNOSAyM1Y5SDEyTDIwIDE5VjlIMjNWMjNIMjBMMTIgMTNWMjNIOVoiIGZpbGw9IiMyMkM1NUUiLz4KPGNpcmNsZSBjeD0iMjQiIGN5PSI4IiByPSIyIiBmaWxsPSIjMzhCREY4Ii8+CjxjaXJjbGUgY3g9IjgiIGN5PSIyNCIgcj0iMiIgZmlsbD0iI0Y1OUUwQiIvPgo8L3N2Zz4=">
  <style>
    :root {
      --fail: #ef4444;
      --warn: #f59e0b;
      --info: #3b82f6;
      --err:  #374151;
      --border: #d1d5db;
      --thead: #f3f4f6;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      color: #111827;
      background: #f9fafb;
    }
    .page { max-width: 1100px; margin: 24px auto; padding: 0 16px 24px; }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 16px;
      margin-bottom: 16px;
      flex-wrap: wrap;
    }
    h1 { margin: 0; font-size: 22px; }
    .subtitle { margin-top: 4px; font-size: 13px; color: #6b7280; }
    .summary-line { font-size: 13px; color: #6b7280; margin-top: 6px; }
    .summary-line strong { color: #111827; }
    .tags { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }
    .tag {
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 11px;
      display: inline-flex;
      gap: 4px;
    }
    .tag .label { color: #6b7280; }
    .tag .value { font-weight: 600; color: #111827; }
    table { border-collapse: collapse; width: 100%; border: 1px solid var(--border); }
    thead th {
      position: sticky; top: 0; background: var(--thead);
      border-bottom: 1px solid var(--border);
      padding: 10px; text-align: left; font-size: 13px;
    }
    tbody td { border-bottom: 1px solid var(--border); padding: 10px; vertical-align: top; }
    tbody tr:nth-child(odd) { background: #fafafa; }
    .sev { display: inline-block; padding: 2px 8px; border-radius: 999px; font-weight: 600; font-size: 12px; }
    .sev.FAIL { color: #fff; background: var(--fail); }
    .sev.WARN { color: #111827; background: #fde68a; }
    .sev.INFO { color: #fff; background: var(--info); }
    .sev.ERR  { color: #111827; background: #e5e7eb; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; white-space: pre-wrap; word-break: break-word; }
  </style>
</head>
<body>
  <div class="page">
    <header class="header">
      <div>
        <h1>NCC Report</h1>
        <div class="subtitle">{{.Meta.ClusterName}}</div>
        <div class="summary-line">
          {{.Summary.FAIL}} FAIL, {{.Summary.WARN}} WARN, {{.Summary.ERR}} ERR, {{.Summary.INFO}} INFO
        </div>
      </div>
      <div class="tags">
        <div class="tag"><span class="label">Cluster</span><span class="value">{{.Meta.ClusterName}}</span></div>
        <div class="tag"><span class="label">Cluster Version</span><span class="value">{{.Meta.ClusterVersion}}</span></div>
        <div class="tag"><span class="label">NCC Version</span><span class="value">{{.Meta.NCCVersion}}</span></div>
        <div class="tag"><span class="label">Generated</span><span class="value">{{.Now}}</span></div>
      </div>
    </header>
    <main>
      <table>
        <thead>
          <tr>
            <th style="width:110px">Severity</th>
            <th style="width:320px">NCC Check Name</th>
            <th>Detail Information</th>
          </tr>
        </thead>
        <tbody>
        {{range .Rows}}
          <tr>
            <td><span class="sev {{.Severity}}">{{.Severity}}</span></td>
            <td class="mono">{{.CheckName}}</td>
            <td class="mono">{{.Detail}}</td>
          </tr>
        {{end}}
        </tbody>
      </table>
    </main>
  </div>
</body>
</html>`
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	sum := SummaryCounts{}
	for _, r := range rows {
		switch r.Severity {
		case "FAIL":
			sum.FAIL++
		case "WARN":
			sum.WARN++
		case "ERR":
			sum.ERR++
		default:
			sum.INFO++
		}
	}
	data := HTMLData{
		Rows:    rows,
		Now:     htmlNowForReport().UTC().Format(time.RFC3339),
		Meta:    meta,
		Summary: sum,
	}
	t := template.Must(template.New("table").Parse(tmpl))
	return t.Execute(f, data)
}

func generateCSV(fs FS, blocks []ParsedBlock, filename string) error {
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"Severity", "CheckName", "Detail"}); err != nil {
		return err
	}
	for _, b := range blocks {
		if err := w.Write([]string{b.Severity, b.CheckName, b.DetailRaw}); err != nil {
			return err
		}
	}
	return w.Error()
}

func generateMarkdown(fs FS, blocks []ParsedBlock, filename string, meta HTMLMeta) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("# NCC Report\n\n")
	if meta.ClusterName != "" || meta.ClusterVersion != "" || meta.NCCVersion != "" {
		b.WriteString("| Field | Value |\n|-------|-------|\n")
		if meta.ClusterName != "" {
			b.WriteString(fmt.Sprintf("| Cluster | %s |\n", escapeMarkdown(meta.ClusterName)))
		}
		if meta.ClusterVersion != "" {
			b.WriteString(fmt.Sprintf("| Version | %s |\n", escapeMarkdown(meta.ClusterVersion)))
		}
		if meta.NCCVersion != "" {
			b.WriteString(fmt.Sprintf("| NCC Version | %s |\n", escapeMarkdown(meta.NCCVersion)))
		}
		b.WriteString("\n")
	}
	b.WriteString("| Severity | Check | Detail |\n|----------|-------|--------|\n")
	for _, block := range blocks {
		sev := block.Severity
		if sev == "" {
			sev = "INFO"
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s |\n",
			escapeMarkdown(sev),
			escapeMarkdown(block.CheckName),
			escapeMarkdown(strings.ReplaceAll(block.DetailRaw, "\n", " "))))
	}
	return fs.WriteFile(filename, []byte(b.String()), 0644)
}

func escapeMarkdown(s string) string {
	return strings.NewReplacer("|", "\\|", "\n", " ").Replace(s)
}

func generateJSON(fs FS, blocks []ParsedBlock, filename string, meta HTMLMeta) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	checks := make([]JSONCheck, 0, len(blocks))
	for _, b := range blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		counts[sev]++
		checks = append(checks, JSONCheck{
			Severity:  sev,
			CheckName: b.CheckName,
			Detail:    b.DetailRaw,
		})
	}

	output := JSONOutput{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Checks:      checks,
		Summary: JSONSummary{
			Total: len(blocks),
			Count: counts,
		},
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}

type sarifLog struct {
	Version string     `json:"version"`
	Schema  string     `json:"$schema"`
	Runs    []sarifRun `json:"runs"`
}
type sarifRun struct {
	Tool    sarifTool     `json:"tool"`
	Results []sarifResult `json:"results"`
}
type sarifTool struct {
	Driver sarifDriver `json:"driver"`
}
type sarifDriver struct {
	Name           string      `json:"name"`
	Version        string      `json:"version,omitempty"`
	InformationURI string      `json:"informationUri,omitempty"`
	Rules          []sarifRule `json:"rules,omitempty"`
}
type sarifRule struct {
	ID               string `json:"id"`
	ShortDescription struct {
		Text string `json:"text"`
	} `json:"shortDescription"`
}
type sarifResult struct {
	RuleID  string `json:"ruleId"`
	Level   string `json:"level"`
	Message struct {
		Text string `json:"text"`
	} `json:"message"`
}

func sarifLevelForSeverity(sev string) string {
	switch strings.ToUpper(strings.TrimSpace(sev)) {
	case "FAIL":
		return "error"
	case "WARN":
		return "warning"
	case "ERR":
		return "error"
	default:
		return "note"
	}
}

func generateSARIF(fs FS, blocks []ParsedBlock, filename string) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	ruleMap := map[string]bool{}
	rules := make([]sarifRule, 0, len(blocks))
	results := make([]sarifResult, 0, len(blocks))
	for _, b := range blocks {
		ruleID := strings.TrimSpace(b.CheckName)
		if ruleID == "" {
			ruleID = "NCC_CHECK"
		}
		if !ruleMap[ruleID] {
			ruleMap[ruleID] = true
			r := sarifRule{ID: ruleID}
			r.ShortDescription.Text = ruleID
			rules = append(rules, r)
		}
		res := sarifResult{
			RuleID: ruleID,
			Level:  sarifLevelForSeverity(b.Severity),
		}
		res.Message.Text = strings.TrimSpace(b.DetailRaw)
		if res.Message.Text == "" {
			res.Message.Text = "NCC finding"
		}
		results = append(results, res)
	}

	payload := sarifLog{
		Version: "2.1.0",
		Schema:  "https://json.schemastore.org/sarif-2.1.0.json",
		Runs: []sarifRun{{
			Tool: sarifTool{
				Driver: sarifDriver{
					Name:           "ncc-orchestrator",
					Version:        Version,
					InformationURI: "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator",
					Rules:          rules,
				},
			},
			Results: results,
		}},
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(filename, data, 0644)
}
