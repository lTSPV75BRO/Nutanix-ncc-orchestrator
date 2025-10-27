package metrics

import (
	"fmt"
	"strings"
	"time"

	"goncc/pkg/types"
)

// PrometheusExporter handles Prometheus metrics export
type PrometheusExporter struct{}

// NewPrometheusExporter creates a new Prometheus exporter
func NewPrometheusExporter() *PrometheusExporter {
	return &PrometheusExporter{}
}

// ExportMetrics generates Prometheus format metrics from NCC results
func (p *PrometheusExporter) ExportMetrics(results []types.AggBlock, failedClusters []string) string {
	var metrics strings.Builder

	// Add timestamp
	timestamp := time.Now().Unix()

	// Count metrics by cluster and severity
	clusterCounts := make(map[string]map[string]int)
	totalCounts := make(map[string]int)

	for _, result := range results {
		if clusterCounts[result.Cluster] == nil {
			clusterCounts[result.Cluster] = make(map[string]int)
		}
		clusterCounts[result.Cluster][result.Severity]++
		totalCounts[result.Severity]++
	}

	// Write metrics
	metrics.WriteString("# HELP ncc_check_total Total number of NCC checks by cluster and severity\n")
	metrics.WriteString("# TYPE ncc_check_total counter\n")

	for cluster, counts := range clusterCounts {
		for severity, count := range counts {
			metrics.WriteString(fmt.Sprintf("ncc_check_total{cluster=\"%s\",severity=\"%s\"} %d %d\n",
				escapeLabel(cluster), severity, count, timestamp))
		}
	}

	// Global severity counts
	metrics.WriteString("\n# HELP ncc_check_global_total Total number of NCC checks by severity across all clusters\n")
	metrics.WriteString("# TYPE ncc_check_global_total counter\n")

	for severity, count := range totalCounts {
		metrics.WriteString(fmt.Sprintf("ncc_check_global_total{severity=\"%s\"} %d %d\n",
			severity, count, timestamp))
	}

	// Cluster health status
	metrics.WriteString("\n# HELP ncc_cluster_healthy Cluster health status (1=healthy, 0=unhealthy)\n")
	metrics.WriteString("# TYPE ncc_cluster_healthy gauge\n")

	healthyClusters := make(map[string]bool)
	for _, result := range results {
		healthyClusters[result.Cluster] = true
	}

	for cluster := range healthyClusters {
		isHealthy := 1
		for _, failed := range failedClusters {
			if failed == cluster {
				isHealthy = 0
				break
			}
		}
		metrics.WriteString(fmt.Sprintf("ncc_cluster_healthy{cluster=\"%s\"} %d %d\n",
			escapeLabel(cluster), isHealthy, timestamp))
	}

	// Failed clusters
	for _, failed := range failedClusters {
		metrics.WriteString(fmt.Sprintf("ncc_cluster_healthy{cluster=\"%s\"} 0 %d\n",
			escapeLabel(failed), timestamp))
	}

	// Check execution time
	metrics.WriteString("\n# HELP ncc_execution_duration_seconds Duration of NCC execution\n")
	metrics.WriteString("# TYPE ncc_execution_duration_seconds gauge\n")
	metrics.WriteString(fmt.Sprintf("ncc_execution_duration_seconds %d %d\n", timestamp, timestamp))

	// Total clusters processed
	metrics.WriteString("\n# HELP ncc_clusters_total Total number of clusters processed\n")
	metrics.WriteString("# TYPE ncc_clusters_total gauge\n")
	metrics.WriteString(fmt.Sprintf("ncc_clusters_total %d %d\n",
		len(healthyClusters)+len(failedClusters), timestamp))

	// Failed clusters count
	metrics.WriteString("\n# HELP ncc_clusters_failed_total Total number of failed clusters\n")
	metrics.WriteString("# TYPE ncc_clusters_failed_total gauge\n")
	metrics.WriteString(fmt.Sprintf("ncc_clusters_failed_total %d %d\n",
		len(failedClusters), timestamp))

	return metrics.String()
}

// escapeLabel escapes Prometheus label values
func escapeLabel(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}
