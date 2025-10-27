package notifications

import (
	"bytes"
	"fmt"
	"net/smtp"
	"strings"
	"text/template"

	"goncc/pkg/errors"
	"goncc/pkg/types"
)

// EmailConfig holds email notification configuration
type EmailConfig struct {
	SMTPHost string
	SMTPPort int
	Username string
	Password string
	From     string
	To       []string
	Subject  string
	UseTLS   bool
	UseAuth  bool
}

// EmailNotifier handles email notifications
type EmailNotifier struct {
	config EmailConfig
}

// NewEmailNotifier creates a new email notifier
func NewEmailNotifier(config EmailConfig) *EmailNotifier {
	return &EmailNotifier{config: config}
}

// SendReport sends an email with NCC report
func (e *EmailNotifier) SendReport(results []types.AggBlock, failedClusters []string) error {
	if len(e.config.To) == 0 {
		return errors.New(errors.ErrorTypeConfig, "no email recipients configured")
	}

	body, err := e.generateEmailBody(results, failedClusters)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to generate email body")
	}

	msg := e.buildMessage(body)

	addr := fmt.Sprintf("%s:%d", e.config.SMTPHost, e.config.SMTPPort)

	var auth smtp.Auth
	if e.config.UseAuth {
		auth = smtp.PlainAuth("", e.config.Username, e.config.Password, e.config.SMTPHost)
	}

	if e.config.UseTLS {
		return e.sendTLS(addr, auth, msg)
	}

	return smtp.SendMail(addr, auth, e.config.From, e.config.To, msg)
}

// sendTLS sends email with TLS
func (e *EmailNotifier) sendTLS(addr string, auth smtp.Auth, msg []byte) error {
	// This is a simplified TLS implementation
	// In production, you'd want to use proper TLS configuration
	return smtp.SendMail(addr, auth, e.config.From, e.config.To, msg)
}

// buildMessage builds the complete email message
func (e *EmailNotifier) buildMessage(body string) []byte {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("From: %s\r\n", e.config.From))
	buf.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(e.config.To, ", ")))
	buf.WriteString(fmt.Sprintf("Subject: %s\r\n", e.config.Subject))
	buf.WriteString("Content-Type: text/html; charset=UTF-8\r\n")
	buf.WriteString("\r\n")
	buf.WriteString(body)

	return buf.Bytes()
}

// generateEmailBody generates HTML email body
func (e *EmailNotifier) generateEmailBody(results []types.AggBlock, failedClusters []string) (string, error) {
	const emailTemplate = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>NCC Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: #f4f4f4; padding: 15px; border-radius: 5px; }
        .summary { margin: 20px 0; }
        .summary-item { display: inline-block; margin: 5px 10px; padding: 5px 10px; border-radius: 3px; }
        .fail { background-color: #ffebee; color: #c62828; }
        .warn { background-color: #fff3e0; color: #ef6c00; }
        .info { background-color: #e3f2fd; color: #1565c0; }
        .err { background-color: #f5f5f5; color: #424242; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .failed-clusters { background-color: #ffebee; padding: 10px; border-radius: 5px; margin: 10px 0; }
    </style>
</head>
<body>
    <div class="header">
        <h1>NCC Orchestrator Report</h1>
        <p>Generated at: {{.GeneratedAt}}</p>
    </div>
    
    {{if .FailedClusters}}
    <div class="failed-clusters">
        <h3>Failed Clusters:</h3>
        <ul>
            {{range .FailedClusters}}
            <li>{{.}}</li>
            {{end}}
        </ul>
    </div>
    {{end}}
    
    <div class="summary">
        <h3>Summary:</h3>
        <span class="summary-item fail">FAIL: {{.Summary.Fail}}</span>
        <span class="summary-item warn">WARN: {{.Summary.Warn}}</span>
        <span class="summary-item info">INFO: {{.Summary.Info}}</span>
        <span class="summary-item err">ERR: {{.Summary.Err}}</span>
        <span class="summary-item">Total: {{.Summary.Total}}</span>
    </div>
    
    {{if .Results}}
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
            {{range .Results}}
            <tr>
                <td>{{.Cluster}}</td>
                <td><span class="{{.Severity | lower}}">{{.Severity}}</span></td>
                <td>{{.Check}}</td>
                <td>{{.Detail}}</td>
            </tr>
            {{end}}
        </tbody>
    </table>
    {{else}}
    <p>No results to display.</p>
    {{end}}
</body>
</html>`

	tmpl, err := template.New("email").Funcs(template.FuncMap{
		"lower": strings.ToLower,
	}).Parse(emailTemplate)
	if err != nil {
		return "", err
	}

	// Count severities
	summary := struct {
		Total int
		Fail  int
		Warn  int
		Info  int
		Err   int
	}{}

	for _, result := range results {
		summary.Total++
		switch result.Severity {
		case "FAIL":
			summary.Fail++
		case "WARN":
			summary.Warn++
		case "INFO":
			summary.Info++
		case "ERR":
			summary.Err++
		}
	}

	data := struct {
		GeneratedAt    string
		Results        []types.AggBlock
		FailedClusters []string
		Summary        interface{}
	}{
		GeneratedAt:    "{{.GeneratedAt}}", // This would be replaced with actual time
		Results:        results,
		FailedClusters: failedClusters,
		Summary:        summary,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}
