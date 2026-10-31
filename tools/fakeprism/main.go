// Command fakeprism is a load-test stand-in for Prism Element: it answers the
// three calls the orchestrator makes per cluster (start NCC checks, task
// status, run summary) for ANY host name, so the real engine, api-server and
// UI can be exercised against thousands of simulated clusters on one machine.
// See docs/SCALING.md for the full procedure and measured results.
//
// Every address in 127.0.0.0/8 reaches a listener on 0.0.0.0 on Linux and
// Windows, so -make-config lists clusters as 127.0.x.y (macOS only answers on
// 127.0.0.1 unless aliases are added). Each simulated cluster returns a
// deterministic NCC summary: a header, a "Plugin results" table, PASS lines
// and -findings non-PASS findings.
//
//	go run ./tools/fakeprism -make-config /tmp/ncc-scale -clusters 7000 -parallel 500
//	go run ./tools/fakeprism &                       # listens on :9440
//	cd /tmp/ncc-scale && NCC_PASSWORD=x ncc-orchestrator --config config.yaml
//
// Test tool only: it serves a throwaway self-signed certificate, so the
// generated config sets insecure-skip-verify.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var checks = []string{
	"cvm_memory_usage_check", "disk_usage_check", "ntp_server_check", "pc_vm_resource_check",
	"dimm_temperature_check", "cluster_services_down_check", "stale_snapshot_check", "nic_link_down_check",
	"lcm_version_check", "zkalias_check_plugin", "cassandra_status_check", "stargate_latency_check",
	"hypervisor_time_check", "dns_server_check", "power_supply_check", "fan_speed_check",
	"ipmi_sel_check", "cvm_reboot_check", "vm_checks", "protection_domain_check",
}

// summary builds the NCC run summary text for host.
func summary(host string, findings, passes int) string {
	f := fnv.New32a()
	_, _ = f.Write([]byte(host))
	seed := int(f.Sum32())
	var b strings.Builder
	fmt.Fprintf(&b, "Running : health_checks run_all\nCluster Name: cl-%s\nCluster Version: 6.%d.%d\nNCC Version: 5.0.%d\n\n",
		strings.ReplaceAll(host, ".", "-"), 5+seed%4, seed%3, seed%5)
	for i := 0; i < passes; i++ {
		fmt.Fprintf(&b, "/health_checks/system_checks/check_%d [ PASS ]\n", i)
	}
	sevs := []string{"FAIL", "WARN", "WARN", "INFO", "ERR", "FAIL", "INFO", "WARN"}
	counts := map[string]int{}
	for i := 0; i < findings; i++ {
		counts[sevs[(seed+i)%len(sevs)]]++
	}
	fmt.Fprintf(&b, "+-----------------------+\n| State         | Count |\n+-----------------------+\n")
	fmt.Fprintf(&b, "| Pass          | %d |\n| Fail          | %d |\n| Warning       | %d |\n| Error         | %d |\n| Info          | %d |\n| Total Plugins | %d |\n+-----------------------+\n\n",
		passes, counts["FAIL"], counts["WARN"], counts["ERR"], counts["INFO"], passes+findings)
	for i := 0; i < findings; i++ {
		sev := sevs[(seed+i)%len(sevs)]
		chk := checks[(seed+i*7)%len(checks)]
		kb := 2000 + (seed+i)%5000
		fmt.Fprintf(&b, "Detailed information for %s:\n", chk)
		fmt.Fprintf(&b, "Node 10.%d.%d.%d:\n%s: %s on host %s is above the threshold (%d%%).\n",
			seed%250, i%250, (seed+i)%250, sev, chk, host, 70+(seed+i)%30)
		fmt.Fprintf(&b, "Refer to KB %d (https://portal.nutanix.com/kb/%d) for details on %s or Recheck with: ncc health_checks run_all\n", kb, kb, chk)
	}
	return b.String()
}

// makeConfig writes config.yaml and clusters.txt for n simulated clusters.
func makeConfig(dir string, n, parallel int) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var list strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&list, "127.0.%d.%d\n", i/250+1, i%250+1)
	}
	if err := os.WriteFile(filepath.Join(dir, "clusters.txt"), []byte(list.String()), 0o644); err != nil {
		return err
	}
	cfg := fmt.Sprintf(`# Load test against tools/fakeprism (see docs/SCALING.md). Not for real clusters.
clusters: ""
clusters-file: "clusters.txt"
username: "admin"
ncc-api-version: Legacy
cluster-source-mode: clusters
insecure-skip-verify: true
skip-preflight-check: true
timeout: "15m"
request-timeout: "60s"
poll-interval: "1s"
poll-jitter: "500ms"
max-parallel: %d
outputs: "html,csv"
output-dir-logs: "nccfiles"
output-dir-filtered: "outputfiles"
run-history: true
run-history-dir: "outputfiles/runs"
`, parallel)
	return os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(cfg), 0o644)
}

func main() {
	addr := flag.String("addr", ":9440", "listen address")
	findings := flag.Int("findings", 30, "non-PASS findings per cluster")
	passes := flag.Int("passes", 300, "PASS lines per cluster")
	delay := flag.Duration("delay", 0, "extra latency per request (simulate slow clusters)")
	taskDuration := flag.Duration("task-duration", 0, "how long each NCC run takes before its task reports 100% (real runs: ~3m)")
	hookDelay := flag.Duration("hook-delay", 2*time.Second, "how long POST /hook (a stand-in webhook receiver) takes to answer")
	configDir := flag.String("make-config", "", "write config.yaml + clusters.txt into this directory and exit")
	clusters := flag.Int("clusters", 7000, "clusters for -make-config")
	parallel := flag.Int("parallel", 500, "max-parallel for -make-config")
	flag.Parse()

	if *configDir != "" {
		if err := makeConfig(*configDir, *clusters, *parallel); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("wrote %d clusters to %s\n", *clusters, *configDir)
		return
	}

	var started atomic.Int64
	// Each host's NCC task "runs" for -task-duration from its start call;
	// status polls report progress until then (0 = finished immediately).
	var taskStart sync.Map // host -> time.Time
	progress := func(h string) int {
		if *taskDuration <= 0 {
			return 100
		}
		v, ok := taskStart.Load(h)
		if !ok {
			return 100
		}
		pct := int(time.Since(v.(time.Time)) * 100 / *taskDuration)
		if pct > 100 {
			pct = 100
		}
		return pct
	}
	pause := func() {
		if *delay > 0 {
			time.Sleep(*delay)
		}
	}
	host := func(r *http.Request) string {
		h, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			return r.Host
		}
		return h
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/PrismGateway/services/rest/v1/ncc/checks", func(w http.ResponseWriter, r *http.Request) {
		pause()
		if n := started.Add(1); n%500 == 0 {
			log.Printf("started %d runs", n)
		}
		taskStart.Store(host(r), time.Now())
		_ = json.NewEncoder(w).Encode(map[string]string{"taskUuid": "t-" + host(r)})
	})
	// A slow webhook receiver, to see how per-cluster notifications affect a run
	// (webhook-url: https://127.0.0.1:9440/hook).
	mux.HandleFunc("/hook", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(*hookDelay)
		w.WriteHeader(http.StatusNoContent)
	})
	var polls atomic.Int64
	mux.HandleFunc("/PrismGateway/services/rest/v2.0/tasks/", func(w http.ResponseWriter, r *http.Request) {
		pause()
		if n := polls.Add(1); n%10000 == 0 {
			log.Printf("answered %d status polls", n)
		}
		pct := progress(host(r))
		status := "Running"
		if pct >= 100 {
			status = "Succeeded"
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"percentage_complete": pct, "progress_status": status})
	})
	mux.HandleFunc("/PrismGateway/services/rest/v1/ncc/", func(w http.ResponseWriter, r *http.Request) {
		pause()
		_ = json.NewEncoder(w).Encode(map[string]string{"runSummary": summary(host(r), *findings, *passes)})
	})

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fakeprism"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12},
	}
	log.Printf("fakeprism on %s (findings=%d passes=%d delay=%s task-duration=%s)", *addr, *findings, *passes, *delay, *taskDuration)
	log.Fatal(srv.ListenAndServeTLS("", ""))
}
