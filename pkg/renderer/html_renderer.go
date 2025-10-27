package renderer

import (
	"encoding/csv"
	"encoding/json"
	"html/template"
	"path/filepath"
	"time"

	"goncc/pkg/errors"
	"goncc/pkg/types"
)

// HTMLRenderer handles HTML report generation
type HTMLRenderer struct {
	fs types.FS
}

// NewHTMLRenderer creates a new HTML renderer
func NewHTMLRenderer(fs types.FS) *HTMLRenderer {
	return &HTMLRenderer{fs: fs}
}

// GenerateHTML generates an HTML report for a single cluster
func (r *HTMLRenderer) GenerateHTML(rows []types.Row, filename string) error {
	const tmpl = `
<html>
<head>
  <meta charset="utf-8">
  <title>NCC Report</title>
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
    body { margin: 16px; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; color: #111827; }
    h1 { margin: 0 0 8px 0; font-size: 20px; }
    .meta { color: #6b7280; font-size: 12px; margin-bottom: 12px; }
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
  <h1>NCC Report</h1>
  <div class="meta">Generated at {{.Now}}</div>
  <table>
    <thead>
      <tr>
        <th style="width:120px">Severity</th>
        <th style="width:360px">NCC Check Name</th>
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
</body>
</html>`

	f, err := r.fs.Create(filename)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to create HTML file")
	}
	defer f.Close()

	data := struct {
		Rows []types.Row
		Now  string
	}{
		Rows: rows,
		Now:  time.Now().Format(time.RFC3339),
	}
	t := template.Must(template.New("table").Parse(tmpl))
	return t.Execute(f, data)
}

// GenerateCSV generates a CSV report
func (r *HTMLRenderer) GenerateCSV(blocks []types.ParsedBlock, filename string) error {
	f, err := r.fs.Create(filename)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to create CSV file")
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"Severity", "CheckName", "Detail"}); err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to write CSV header")
	}

	for _, b := range blocks {
		if err := w.Write([]string{b.Severity, b.CheckName, b.DetailRaw}); err != nil {
			return errors.Wrap(err, errors.ErrorTypeFile, "failed to write CSV row")
		}
	}
	return w.Error()
}

// GenerateJSON generates a JSON report for a single cluster
func (r *HTMLRenderer) GenerateJSON(blocks []types.ParsedBlock, filename string) error {
	f, err := r.fs.Create(filename)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to create JSON file")
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")

	report := struct {
		GeneratedAt string              `json:"generated_at"`
		Cluster     string              `json:"cluster"`
		Results     []types.ParsedBlock `json:"results"`
		Summary     struct {
			Total int `json:"total"`
			Fail  int `json:"fail"`
			Warn  int `json:"warn"`
			Info  int `json:"info"`
			Err   int `json:"err"`
		} `json:"summary"`
	}{
		GeneratedAt: time.Now().Format(time.RFC3339),
		Results:     blocks,
	}

	// Count severities
	for _, block := range blocks {
		report.Summary.Total++
		switch block.Severity {
		case "FAIL":
			report.Summary.Fail++
		case "WARN":
			report.Summary.Warn++
		case "INFO":
			report.Summary.Info++
		case "ERR":
			report.Summary.Err++
		}
	}

	return encoder.Encode(report)
}

// WriteAggregatedHTML generates the aggregated HTML report
func (r *HTMLRenderer) WriteAggregatedHTML(outDir string, rows []types.AggBlock, perCluster []struct{ Cluster, HTML, CSV string }) error {
	if err := r.fs.MkdirAll(outDir, 0755); err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to create output directory")
	}

	path := filepath.Join(outDir, "index.html")

	// This is a simplified version - the full template would be much longer
	const tmpl = `
<html>
<head>
  <meta charset="utf-8">
  <title>NCC Aggregated Report</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 20px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    th { background-color: #f2f2f2; }
    .FAIL { background-color: #ffebee; }
    .WARN { background-color: #fff3e0; }
    .INFO { background-color: #e3f2fd; }
    .ERR { background-color: #f5f5f5; }
  </style>
</head>
<body>
  <h1>NCC Aggregated Report</h1>
  <div>Generated at {{.GeneratedAt}}</div>
  <table>
    <thead>
      <tr>
        <th>Cluster</th>
        <th>Severity</th>
        <th>Check</th>
        <th>Detail</th>
      </tr>
    </thead>
    <tbody>
      {{range .Rows}}
      <tr class="{{.Severity}}">
        <td>{{.Cluster}}</td>
        <td>{{.Severity}}</td>
        <td>{{.Check}}</td>
        <td>{{.Detail}}</td>
      </tr>
      {{end}}
    </tbody>
  </table>
</body>
</html>`

	data := struct {
		Rows        []types.AggBlock
		GeneratedAt string
	}{
		Rows:        rows,
		GeneratedAt: time.Now().Format(time.RFC3339),
	}

	f, err := r.fs.Create(path)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to create aggregated HTML file")
	}
	defer f.Close()

	t := template.Must(template.New("index").Parse(tmpl))
	return t.Execute(f, data)
}
